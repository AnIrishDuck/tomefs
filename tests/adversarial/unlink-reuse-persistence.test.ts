/**
 * Adversarial tests: path reuse after unlink with persistence and orphan cleanup.
 *
 * When a file is unlinked while it has open fds, tomefs moves its pages to a
 * /__deleted_* temporary path. If a new file is then created at the same
 * original path, two independent storage paths coexist:
 *   - The new file at its computed storagePath (normal path)
 *   - The old file at /__deleted_* (accessible via open fd only)
 *
 * syncfs must:
 *   1. Persist the new file's data and metadata
 *   2. NOT delete /__deleted_* pages while old fds are open (orphan protection)
 *   3. Clean up /__deleted_* pages after the last old fd closes
 *   4. Survive remount with only the new file visible
 *
 * This exercises the full tree walk path in syncfs (because unlink sets
 * needsOrphanCleanup = true), specifically the orphan protection at
 * tomefs.ts:1255-1263 that preserves /__deleted_* paths for unlinked nodes.
 *
 * Ethos §9: "Write tests designed to break tomefs specifically — target the seams"
 */
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { describe, it, expect, beforeEach } from "vitest";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { createTomeFS } from "../../src/tomefs.js";
import { PAGE_SIZE } from "../../src/types.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

const O = {
  RDONLY: 0,
  WRONLY: 1,
  RDWR: 2,
  CREAT: 64,
  TRUNC: 512,
} as const;

const MOUNT = "/tome";

function encode(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

function decode(buf: Uint8Array, length?: number): string {
  return new TextDecoder().decode(
    length !== undefined ? buf.subarray(0, length) : buf,
  );
}

async function mountTome(backend: SyncMemoryBackend, maxPages?: number) {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  const Module = await createModule();
  const FS = Module.FS;
  const tomefs = createTomeFS(FS, { backend, maxPages });
  FS.mkdir(MOUNT);
  FS.mount(tomefs, {}, MOUNT);
  return { FS, tomefs, Module };
}

function syncfs(FS: any, tomefs: any) {
  tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
    if (err) throw err;
  });
}

function syncAndUnmount(FS: any, tomefs: any) {
  syncfs(FS, tomefs);
  FS.unmount(MOUNT);
}

describe("adversarial: path reuse after unlink with persistence", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  // ------------------------------------------------------------------
  // Basic: unlink + reuse + syncfs + remount
  // ------------------------------------------------------------------

  it("new file at reused path persists correctly after unlink @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create original file and keep fd open
    const oldFd = FS.open(`${MOUNT}/reuse`, O.RDWR | O.CREAT, 0o666);
    const oldData = encode("old-content-must-not-leak");
    FS.write(oldFd, oldData, 0, oldData.length, 0);

    // Unlink while fd is open — pages move to /__deleted_*
    FS.unlink(`${MOUNT}/reuse`);

    // Create new file at the same path
    const newFd = FS.open(`${MOUNT}/reuse`, O.RDWR | O.CREAT, 0o666);
    const newData = encode("new-content-must-persist");
    FS.write(newFd, newData, 0, newData.length, 0);
    FS.close(newFd);

    // Old fd still readable
    const buf = new Uint8Array(oldData.length);
    FS.llseek(oldFd, 0, 0);
    const n = FS.read(oldFd, buf, 0, oldData.length);
    expect(decode(buf, n)).toBe("old-content-must-not-leak");

    // Close old fd — orphan becomes eligible for cleanup
    FS.close(oldFd);

    // Sync and unmount
    syncAndUnmount(FS, tomefs);

    // Remount — only new file should exist with correct content
    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/reuse`);
    expect(stat.size).toBe(newData.length);
    const readBuf = new Uint8Array(newData.length);
    const s = FS2.open(`${MOUNT}/reuse`, O.RDONLY);
    const read = FS2.read(s, readBuf, 0, newData.length);
    FS2.close(s);
    expect(read).toBe(newData.length);
    expect(decode(readBuf, read)).toBe("new-content-must-persist");
  });

  // ------------------------------------------------------------------
  // Orphan protection: syncfs while old fd still open
  // ------------------------------------------------------------------

  it("syncfs preserves old fd data while new file exists at same path", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create and sync original
    const oldFd = FS.open(`${MOUNT}/protected`, O.RDWR | O.CREAT, 0o666);
    const oldData = encode("protected-data");
    FS.write(oldFd, oldData, 0, oldData.length, 0);
    syncfs(FS, tomefs);

    // Unlink while fd open
    FS.unlink(`${MOUNT}/protected`);

    // Create replacement at same path
    const newFd = FS.open(`${MOUNT}/protected`, O.RDWR | O.CREAT, 0o666);
    const newData = encode("replacement-data");
    FS.write(newFd, newData, 0, newData.length, 0);
    FS.close(newFd);

    // Sync with old fd still open — orphan cleanup runs (full tree walk)
    // but must NOT delete /__deleted_* pages
    syncfs(FS, tomefs);

    // Old fd must still be readable after syncfs
    const buf = new Uint8Array(oldData.length);
    FS.llseek(oldFd, 0, 0);
    const n = FS.read(oldFd, buf, 0, oldData.length);
    expect(n).toBe(oldData.length);
    expect(decode(buf, n)).toBe("protected-data");

    FS.close(oldFd);
  });

  // ------------------------------------------------------------------
  // Multiple syncs: orphan cleaned up only after fd close
  // ------------------------------------------------------------------

  it("orphan persists across multiple syncfs calls until fd closes", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create file, keep fd
    const fd = FS.open(`${MOUNT}/multi`, O.RDWR | O.CREAT, 0o666);
    const data = encode("orphan-data-across-syncs");
    FS.write(fd, data, 0, data.length, 0);
    syncfs(FS, tomefs);

    // Unlink
    FS.unlink(`${MOUNT}/multi`);

    // Create replacement
    const fd2 = FS.open(`${MOUNT}/multi`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd2, encode("v2"), 0, 2, 0);
    FS.close(fd2);

    // Multiple syncfs calls — old fd data must survive each one
    for (let i = 0; i < 3; i++) {
      syncfs(FS, tomefs);
      const buf = new Uint8Array(data.length);
      FS.llseek(fd, 0, 0);
      const n = FS.read(fd, buf, 0, data.length);
      expect(n).toBe(data.length);
      expect(decode(buf, n)).toBe("orphan-data-across-syncs");
    }

    // Close fd — now orphan should be cleaned
    FS.close(fd);
    syncAndUnmount(FS, tomefs);

    // Remount: only replacement visible
    const { FS: FS2 } = await mountTome(backend);
    const s = FS2.open(`${MOUNT}/multi`, O.RDONLY);
    const buf = new Uint8Array(10);
    const n = FS2.read(s, buf, 0, 10);
    FS2.close(s);
    expect(decode(buf, n)).toBe("v2");
  });

  // ------------------------------------------------------------------
  // Cache pressure: 4-page cache with multi-page files
  // ------------------------------------------------------------------

  it("path reuse under cache pressure preserves both old and new data", async () => {
    // 4-page cache = extreme eviction pressure
    const { FS, tomefs } = await mountTome(backend, 4);

    // Create a multi-page file (3 pages = 24 KB)
    const oldData = new Uint8Array(PAGE_SIZE * 3);
    for (let i = 0; i < oldData.length; i++) oldData[i] = (i * 7 + 13) & 0xff;

    const fd = FS.open(`${MOUNT}/large`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, oldData, 0, oldData.length, 0);
    syncfs(FS, tomefs);

    // Unlink with open fd
    FS.unlink(`${MOUNT}/large`);

    // Create new multi-page file at same path (2 pages)
    const newData = new Uint8Array(PAGE_SIZE * 2);
    for (let i = 0; i < newData.length; i++) newData[i] = (i * 11 + 37) & 0xff;

    const fd2 = FS.open(`${MOUNT}/large`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd2, newData, 0, newData.length, 0);
    FS.close(fd2);

    // syncfs under cache pressure — eviction will cycle pages from both
    // the old /__deleted_* file and the new file
    syncfs(FS, tomefs);

    // Old fd still reads correct data (must re-load from backend after eviction)
    const readOld = new Uint8Array(oldData.length);
    FS.llseek(fd, 0, 0);
    const n = FS.read(fd, readOld, 0, oldData.length);
    expect(n).toBe(oldData.length);
    expect(readOld).toEqual(oldData);

    // New file reads correct data
    const fd3 = FS.open(`${MOUNT}/large`, O.RDONLY);
    const readNew = new Uint8Array(newData.length);
    const n2 = FS.read(fd3, readNew, 0, newData.length);
    FS.close(fd3);
    expect(n2).toBe(newData.length);
    expect(readNew).toEqual(newData);

    // Close old fd, sync, remount
    FS.close(fd);
    syncAndUnmount(FS, tomefs);

    // After remount: new file persists with correct size and content
    const { FS: FS2 } = await mountTome(backend, 4);
    const stat = FS2.stat(`${MOUNT}/large`);
    expect(stat.size).toBe(newData.length);

    const s = FS2.open(`${MOUNT}/large`, O.RDONLY);
    const verify = new Uint8Array(newData.length);
    const n3 = FS2.read(s, verify, 0, newData.length);
    FS2.close(s);
    expect(n3).toBe(newData.length);
    expect(verify).toEqual(newData);
  });

  // ------------------------------------------------------------------
  // Write to old fd between syncs: data preserved across evictions
  // ------------------------------------------------------------------

  it("writes to unlinked fd between syncfs calls survive cache eviction", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    const fd = FS.open(`${MOUNT}/writeback`, O.RDWR | O.CREAT, 0o666);
    const initial = encode("initial");
    FS.write(fd, initial, 0, initial.length, 0);
    syncfs(FS, tomefs);

    FS.unlink(`${MOUNT}/writeback`);

    // Write more data through the unlinked fd
    const extra = encode("-plus-extra-data");
    FS.write(fd, extra, 0, extra.length, initial.length);

    // Create replacement file at the same path (causes cache pressure)
    const fd2 = FS.open(`${MOUNT}/writeback`, O.RDWR | O.CREAT, 0o666);
    const replacement = new Uint8Array(PAGE_SIZE * 3);
    replacement.fill(0xab);
    FS.write(fd2, replacement, 0, replacement.length, 0);
    FS.close(fd2);

    // syncfs — must persist new file AND preserve old fd's pages in backend
    syncfs(FS, tomefs);

    // Read back through old fd — should get "initial-plus-extra-data"
    const expected = "initial-plus-extra-data";
    const buf = new Uint8Array(expected.length);
    FS.llseek(fd, 0, 0);
    const n = FS.read(fd, buf, 0, expected.length);
    expect(n).toBe(expected.length);
    expect(decode(buf, n)).toBe(expected);

    FS.close(fd);
  });

  // ------------------------------------------------------------------
  // Multiple files unlinked + reused simultaneously
  // ------------------------------------------------------------------

  it("multiple simultaneous unlink-reuse cycles persist correctly", async () => {
    const { FS, tomefs } = await mountTome(backend, 8);

    // Create 3 files, keep fds open
    const fds: number[] = [];
    const origData = ["alpha-original", "beta-original", "gamma-original"];
    for (let i = 0; i < 3; i++) {
      const fd = FS.open(`${MOUNT}/file${i}`, O.RDWR | O.CREAT, 0o666);
      const d = encode(origData[i]);
      FS.write(fd, d, 0, d.length, 0);
      fds.push(fd);
    }
    syncfs(FS, tomefs);

    // Unlink all 3
    for (let i = 0; i < 3; i++) {
      FS.unlink(`${MOUNT}/file${i}`);
    }

    // Create replacements
    const newData = ["alpha-new", "beta-new", "gamma-new"];
    for (let i = 0; i < 3; i++) {
      const fd = FS.open(`${MOUNT}/file${i}`, O.RDWR | O.CREAT, 0o666);
      const d = encode(newData[i]);
      FS.write(fd, d, 0, d.length, 0);
      FS.close(fd);
    }

    // syncfs with all old fds still open
    syncfs(FS, tomefs);

    // Verify old fds still readable
    for (let i = 0; i < 3; i++) {
      const buf = new Uint8Array(origData[i].length);
      FS.llseek(fds[i], 0, 0);
      const n = FS.read(fds[i], buf, 0, origData[i].length);
      expect(decode(buf, n)).toBe(origData[i]);
    }

    // Close old fds one at a time, syncing between each
    for (let i = 0; i < 3; i++) {
      FS.close(fds[i]);
      syncfs(FS, tomefs);
    }

    syncAndUnmount(FS, tomefs);

    // Remount — only new versions exist
    const { FS: FS2 } = await mountTome(backend, 8);
    for (let i = 0; i < 3; i++) {
      const s = FS2.open(`${MOUNT}/file${i}`, O.RDONLY);
      const buf = new Uint8Array(50);
      const n = FS2.read(s, buf, 0, 50);
      FS2.close(s);
      expect(decode(buf, n)).toBe(newData[i]);
    }
  });

  // ------------------------------------------------------------------
  // Orphan cleanup verification: /__deleted_* entries removed from backend
  // ------------------------------------------------------------------

  it("backend has no /__deleted_* entries after clean shutdown @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create, unlink with open fd, create replacement
    const fd = FS.open(`${MOUNT}/clean`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, encode("temp"), 0, 4, 0);
    syncfs(FS, tomefs);
    FS.unlink(`${MOUNT}/clean`);

    const fd2 = FS.open(`${MOUNT}/clean`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd2, encode("final"), 0, 5, 0);
    FS.close(fd2);

    // syncfs while old fd open — /__deleted_* should be in backend
    syncfs(FS, tomefs);
    const filesBeforeClose = backend.listFiles();
    const hasDeleted = filesBeforeClose.some((f) => f.startsWith("/__deleted_"));
    expect(hasDeleted).toBe(true);

    // Close old fd and sync — orphan cleanup should remove /__deleted_*
    FS.close(fd);
    syncAndUnmount(FS, tomefs);

    const filesAfterClose = backend.listFiles();
    const hasDeletedAfter = filesAfterClose.some((f) =>
      f.startsWith("/__deleted_"),
    );
    expect(hasDeletedAfter).toBe(false);
  });

  // ------------------------------------------------------------------
  // Crash recovery: remount after crash with /__deleted_* orphans
  // ------------------------------------------------------------------

  it("remount after crash recovers without /__deleted_* data leaking @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create, sync, unlink with open fd
    const fd = FS.open(`${MOUNT}/crash`, O.RDWR | O.CREAT, 0o666);
    const data = encode("crash-data");
    FS.write(fd, data, 0, data.length, 0);
    syncfs(FS, tomefs);

    FS.unlink(`${MOUNT}/crash`);

    // Create replacement and sync — this persists both the new file
    // and the /__deleted_* marker for the old one
    const fd2 = FS.open(`${MOUNT}/crash`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd2, encode("recovered"), 0, 9, 0);
    FS.close(fd2);
    syncfs(FS, tomefs);

    // Simulate crash: don't close old fd, don't do final syncfs
    // The backend still has /__deleted_* entries

    // "Crash" — remount from same backend without clean shutdown
    // First, remove the clean-shutdown marker to simulate crash
    backend.deleteMeta("/__tomefs_clean");

    const { FS: FS2 } = await mountTome(backend);

    // The new file should be visible and correct
    const s = FS2.open(`${MOUNT}/crash`, O.RDONLY);
    const buf = new Uint8Array(20);
    const n = FS2.read(s, buf, 0, 20);
    FS2.close(s);
    expect(decode(buf, n)).toBe("recovered");

    // /__deleted_* entries should exist in backend (orphans from crash)
    const files = backend.listFiles();
    const orphans = files.filter((f) => f.startsWith("/__deleted_"));
    expect(orphans.length).toBeGreaterThan(0);

    // First syncfs after crash remount should clean up orphans
    const tomefs2 = FS2.lookupPath(`${MOUNT}`).node.mount.type;
    tomefs2.syncfs(FS2.lookupPath(`${MOUNT}`).node.mount, false, (err: any) => {
      if (err) throw err;
    });

    // Now orphans should be cleaned
    const filesAfterSync = backend.listFiles();
    const orphansAfter = filesAfterSync.filter((f) =>
      f.startsWith("/__deleted_"),
    );
    expect(orphansAfter.length).toBe(0);
  });

  // ------------------------------------------------------------------
  // Directory + file reuse: unlink file, create dir at same name
  // ------------------------------------------------------------------

  it("creating directory at unlinked file path doesn't corrupt open fd", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create file, keep fd
    const fd = FS.open(`${MOUNT}/morph`, O.RDWR | O.CREAT, 0o666);
    const data = encode("file-content");
    FS.write(fd, data, 0, data.length, 0);
    syncfs(FS, tomefs);

    // Unlink file
    FS.unlink(`${MOUNT}/morph`);

    // Create a DIRECTORY at the same name
    FS.mkdir(`${MOUNT}/morph`);

    // Create a file inside the new directory
    const fd2 = FS.open(`${MOUNT}/morph/child`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd2, encode("child-data"), 0, 10, 0);
    FS.close(fd2);

    // syncfs with old fd still open
    syncfs(FS, tomefs);

    // Old fd still works
    const buf = new Uint8Array(data.length);
    FS.llseek(fd, 0, 0);
    const n = FS.read(fd, buf, 0, data.length);
    expect(decode(buf, n)).toBe("file-content");
    FS.close(fd);

    // Final sync and remount
    syncAndUnmount(FS, tomefs);
    const { FS: FS2 } = await mountTome(backend);

    // Directory and child should exist
    const stat = FS2.stat(`${MOUNT}/morph`);
    expect(stat.mode & 0o170000).toBe(0o040000); // S_IFDIR

    const s = FS2.open(`${MOUNT}/morph/child`, O.RDONLY);
    const childBuf = new Uint8Array(10);
    const n2 = FS2.read(s, childBuf, 0, 10);
    FS2.close(s);
    expect(decode(childBuf, n2)).toBe("child-data");
  });
});
