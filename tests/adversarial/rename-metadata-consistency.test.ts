/**
 * Adversarial tests: Rename metadata consistency.
 *
 * Verifies that file metadata is moved to the new path during rename
 * operations, matching how symlinks and directories are handled. Without
 * this, a crash between rename and syncfs could lose file metadata —
 * pages exist under the new path but metadata is gone from both old and
 * new paths, causing data loss on restore.
 *
 * These tests inspect backend metadata state directly after rename
 * (without calling syncfs) to verify the intermediate state is consistent.
 * They also verify that persist/restore round-trips work correctly when
 * syncfs is NOT called between rename and crash (simulated by remounting
 * from the backend without calling syncfs first).
 *
 * Ethos §9: "Write tests designed to break tomefs specifically — things
 * that pass against MEMFS but expose real bugs in the page cache layer."
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

// ---------------------------------------------------------------------------
// File rename metadata consistency
// ---------------------------------------------------------------------------

describe("adversarial: rename metadata consistency", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("file rename moves metadata to new path in backend @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create file and sync to populate backend metadata
    const s = FS.open(`${MOUNT}/old`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("content"), 0, 7);
    FS.close(s);
    syncfs(FS, tomefs);

    // Verify metadata exists at old path
    expect(backend.readMeta("/old")).not.toBeNull();

    // Rename without syncfs
    FS.rename(`${MOUNT}/old`, `${MOUNT}/new`);

    // Metadata should now be at new path, not old path
    expect(backend.readMeta("/old")).toBeNull();
    expect(backend.readMeta("/new")).not.toBeNull();
    expect(backend.readMeta("/new")!.size).toBe(7);

    FS.unmount(MOUNT);
  });

  it("directory rename moves child file metadata to new paths @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/dir`);
    const s = FS.open(`${MOUNT}/dir/file`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("inside dir"), 0, 10);
    FS.close(s);
    syncfs(FS, tomefs);

    // Both directory and file metadata should exist
    expect(backend.readMeta("/dir")).not.toBeNull();
    expect(backend.readMeta("/dir/file")).not.toBeNull();

    // Rename directory without syncfs
    FS.rename(`${MOUNT}/dir`, `${MOUNT}/moved`);

    // Old metadata should be gone, new metadata should exist
    expect(backend.readMeta("/dir")).toBeNull();
    expect(backend.readMeta("/dir/file")).toBeNull();
    expect(backend.readMeta("/moved")).not.toBeNull();
    expect(backend.readMeta("/moved/file")).not.toBeNull();
    expect(backend.readMeta("/moved/file")!.size).toBe(10);

    FS.unmount(MOUNT);
  });

  it("file rename preserves metadata fields (mode, timestamps)", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const s = FS.open(`${MOUNT}/src`, O.RDWR | O.CREAT, 0o644);
    FS.write(s, encode("data"), 0, 4);
    FS.close(s);
    syncfs(FS, tomefs);

    const oldMeta = backend.readMeta("/src")!;
    expect(oldMeta).not.toBeNull();

    FS.rename(`${MOUNT}/src`, `${MOUNT}/dst`);

    const newMeta = backend.readMeta("/dst")!;
    expect(newMeta).not.toBeNull();
    expect(newMeta.mode).toBe(oldMeta.mode);
    expect(newMeta.size).toBe(oldMeta.size);
    expect(newMeta.ctime).toBe(oldMeta.ctime);
    expect(newMeta.mtime).toBe(oldMeta.mtime);

    FS.unmount(MOUNT);
  });

  // ---------------------------------------------------------------------------
  // Crash recovery: rename without syncfs, then restore
  // ---------------------------------------------------------------------------

  it("file survives crash after rename (no syncfs between rename and crash)", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create and sync to establish metadata + pages in backend
    const data = new Uint8Array(PAGE_SIZE + 100);
    for (let i = 0; i < data.length; i++) data[i] = (i * 37) & 0xff;
    const s = FS.open(`${MOUNT}/before`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    FS.close(s);
    syncfs(FS, tomefs);

    // Rename without syncing — simulates crash before next syncfs
    FS.rename(`${MOUNT}/before`, `${MOUNT}/after`);

    // "Crash" — remount from backend without syncfs
    // (don't call syncfs or unmount — just create a new mount)
    const { FS: FS2 } = await mountTome(backend);

    // File should be accessible at new path
    const stat = FS2.stat(`${MOUNT}/after`);
    expect(stat.size).toBe(data.length);

    // Verify data integrity
    const buf = new Uint8Array(data.length);
    const s2 = FS2.open(`${MOUNT}/after`, O.RDONLY);
    FS2.read(s2, buf, 0, data.length, 0);
    FS2.close(s2);
    for (let i = 0; i < data.length; i++) {
      if (buf[i] !== data[i]) {
        throw new Error(
          `Data mismatch at byte ${i}: expected ${data[i]}, got ${buf[i]}`,
        );
      }
    }

    // Old path should not exist
    expect(() => FS2.stat(`${MOUNT}/before`)).toThrow();
  });

  it("directory rename files survive crash (no syncfs)", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/orig`);
    const s1 = FS.open(`${MOUNT}/orig/a`, O.RDWR | O.CREAT, 0o666);
    FS.write(s1, encode("file-a-data"), 0, 11);
    FS.close(s1);
    const s2 = FS.open(`${MOUNT}/orig/b`, O.RDWR | O.CREAT, 0o666);
    FS.write(s2, encode("file-b-data"), 0, 11);
    FS.close(s2);
    syncfs(FS, tomefs);

    // Rename directory without syncfs
    FS.rename(`${MOUNT}/orig`, `${MOUNT}/renamed`);

    // "Crash" + remount
    const { FS: FS2 } = await mountTome(backend);

    // Both files should be at new paths
    const buf = new Uint8Array(20);
    const sa = FS2.open(`${MOUNT}/renamed/a`, O.RDONLY);
    const na = FS2.read(sa, buf, 0, 20);
    FS2.close(sa);
    expect(decode(buf, na)).toBe("file-a-data");

    const sb = FS2.open(`${MOUNT}/renamed/b`, O.RDONLY);
    const nb = FS2.read(sb, buf, 0, 20);
    FS2.close(sb);
    expect(decode(buf, nb)).toBe("file-b-data");

    // Old paths should not exist
    expect(() => FS2.stat(`${MOUNT}/orig`)).toThrow();
  });

  it("nested directory rename preserves deeply nested file metadata", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/a`);
    FS.mkdir(`${MOUNT}/a/b`);
    FS.mkdir(`${MOUNT}/a/b/c`);
    const s = FS.open(`${MOUNT}/a/b/c/deep`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("deep-data"), 0, 9);
    FS.close(s);
    FS.symlink("deep", `${MOUNT}/a/b/c/link`);
    syncfs(FS, tomefs);

    // Verify all metadata exists
    expect(backend.readMeta("/a")).not.toBeNull();
    expect(backend.readMeta("/a/b")).not.toBeNull();
    expect(backend.readMeta("/a/b/c")).not.toBeNull();
    expect(backend.readMeta("/a/b/c/deep")).not.toBeNull();
    expect(backend.readMeta("/a/b/c/link")).not.toBeNull();

    // Rename top-level directory
    FS.rename(`${MOUNT}/a`, `${MOUNT}/x`);

    // All metadata should be at new paths
    expect(backend.readMeta("/a")).toBeNull();
    expect(backend.readMeta("/a/b")).toBeNull();
    expect(backend.readMeta("/a/b/c")).toBeNull();
    expect(backend.readMeta("/a/b/c/deep")).toBeNull();
    expect(backend.readMeta("/a/b/c/link")).toBeNull();

    expect(backend.readMeta("/x")).not.toBeNull();
    expect(backend.readMeta("/x/b")).not.toBeNull();
    expect(backend.readMeta("/x/b/c")).not.toBeNull();
    expect(backend.readMeta("/x/b/c/deep")).not.toBeNull();
    expect(backend.readMeta("/x/b/c/link")).not.toBeNull();

    FS.unmount(MOUNT);
  });

  // ---------------------------------------------------------------------------
  // Rename overwrite: target file metadata should be replaced
  // ---------------------------------------------------------------------------

  it("rename overwriting existing file replaces target metadata", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create two files
    const s1 = FS.open(`${MOUNT}/src`, O.RDWR | O.CREAT, 0o666);
    FS.write(s1, encode("new-content"), 0, 11);
    FS.close(s1);
    const s2 = FS.open(`${MOUNT}/dst`, O.RDWR | O.CREAT, 0o666);
    FS.write(s2, encode("old"), 0, 3);
    FS.close(s2);
    syncfs(FS, tomefs);

    expect(backend.readMeta("/src")!.size).toBe(11);
    expect(backend.readMeta("/dst")!.size).toBe(3);

    // Rename src over dst
    FS.rename(`${MOUNT}/src`, `${MOUNT}/dst`);

    // dst metadata should reflect src's data
    expect(backend.readMeta("/src")).toBeNull();
    const dstMeta = backend.readMeta("/dst");
    expect(dstMeta).not.toBeNull();
    expect(dstMeta!.size).toBe(11);

    FS.unmount(MOUNT);
  });

  // ---------------------------------------------------------------------------
  // Cache pressure: rename under eviction
  // ---------------------------------------------------------------------------

  it("rename under cache pressure preserves metadata and data", async () => {
    // 4-page cache to force eviction
    const { FS, tomefs } = await mountTome(backend, 4);

    // Create a multi-page file (3 pages)
    const bigData = new Uint8Array(PAGE_SIZE * 3);
    for (let i = 0; i < bigData.length; i++) bigData[i] = (i * 41) & 0xff;
    const s = FS.open(`${MOUNT}/big`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, bigData, 0, bigData.length);
    FS.close(s);
    syncfs(FS, tomefs);

    // Create another file to pressure the cache
    const extra = new Uint8Array(PAGE_SIZE * 2);
    const s2 = FS.open(`${MOUNT}/extra`, O.RDWR | O.CREAT, 0o666);
    FS.write(s2, extra, 0, extra.length);
    FS.close(s2);

    // Rename the big file (some pages may be evicted)
    FS.rename(`${MOUNT}/big`, `${MOUNT}/moved`);

    // Metadata should be at new path
    expect(backend.readMeta("/big")).toBeNull();
    expect(backend.readMeta("/moved")).not.toBeNull();
    expect(backend.readMeta("/moved")!.size).toBe(bigData.length);

    // "Crash" + remount — verify data survives
    syncfs(FS, tomefs);
    const { FS: FS2 } = await mountTome(backend, 4);

    const buf = new Uint8Array(bigData.length);
    const s3 = FS2.open(`${MOUNT}/moved`, O.RDONLY);
    FS2.read(s3, buf, 0, bigData.length, 0);
    FS2.close(s3);
    for (let i = 0; i < bigData.length; i++) {
      if (buf[i] !== bigData[i]) {
        throw new Error(
          `Data mismatch at byte ${i}: expected ${bigData[i]}, got ${buf[i]}`,
        );
      }
    }
  });

  // ---------------------------------------------------------------------------
  // Sequential renames: A→B→C
  // ---------------------------------------------------------------------------

  it("chain rename A→B→C moves metadata correctly at each step", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const s = FS.open(`${MOUNT}/a`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("chain"), 0, 5);
    FS.close(s);
    syncfs(FS, tomefs);

    expect(backend.readMeta("/a")).not.toBeNull();

    // A → B
    FS.rename(`${MOUNT}/a`, `${MOUNT}/b`);
    expect(backend.readMeta("/a")).toBeNull();
    expect(backend.readMeta("/b")).not.toBeNull();

    // B → C
    FS.rename(`${MOUNT}/b`, `${MOUNT}/c`);
    expect(backend.readMeta("/b")).toBeNull();
    expect(backend.readMeta("/c")).not.toBeNull();
    expect(backend.readMeta("/c")!.size).toBe(5);

    // Verify data after remount
    syncAndUnmount(FS, tomefs);
    const { FS: FS2 } = await mountTome(backend);
    const buf = new Uint8Array(10);
    const s2 = FS2.open(`${MOUNT}/c`, O.RDONLY);
    const n = FS2.read(s2, buf, 0, 10);
    FS2.close(s2);
    expect(decode(buf, n)).toBe("chain");
  });

  // ---------------------------------------------------------------------------
  // Rename file that was never synced (no metadata in backend)
  // ---------------------------------------------------------------------------

  it("rename of never-synced file does not crash (no metadata to move)", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create file but don't sync — no metadata in backend
    const s = FS.open(`${MOUNT}/unsynced`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("new"), 0, 3);
    FS.close(s);

    expect(backend.readMeta("/unsynced")).toBeNull();

    // Rename should work without crashing
    FS.rename(`${MOUNT}/unsynced`, `${MOUNT}/moved`);

    // No metadata at either path (never synced)
    expect(backend.readMeta("/unsynced")).toBeNull();
    expect(backend.readMeta("/moved")).toBeNull();

    // Data should still be accessible
    const buf = new Uint8Array(10);
    const s2 = FS.open(`${MOUNT}/moved`, O.RDONLY);
    const n = FS.read(s2, buf, 0, 10);
    FS.close(s2);
    expect(decode(buf, n)).toBe("new");

    FS.unmount(MOUNT);
  });

  // ---------------------------------------------------------------------------
  // Mid-level directory rename with mixed node types
  // ---------------------------------------------------------------------------

  it("mid-level directory rename moves file + symlink + subdir metadata", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/top`);
    FS.mkdir(`${MOUNT}/top/mid`);
    FS.mkdir(`${MOUNT}/top/mid/sub`);

    const s1 = FS.open(`${MOUNT}/top/mid/file1`, O.RDWR | O.CREAT, 0o666);
    FS.write(s1, encode("f1"), 0, 2);
    FS.close(s1);
    const s2 = FS.open(`${MOUNT}/top/mid/sub/file2`, O.RDWR | O.CREAT, 0o666);
    FS.write(s2, encode("f2"), 0, 2);
    FS.close(s2);
    FS.symlink("file1", `${MOUNT}/top/mid/lnk`);

    syncfs(FS, tomefs);

    // Rename mid-level directory
    FS.rename(`${MOUNT}/top/mid`, `${MOUNT}/top/moved`);

    // Old paths: all gone
    expect(backend.readMeta("/top/mid")).toBeNull();
    expect(backend.readMeta("/top/mid/file1")).toBeNull();
    expect(backend.readMeta("/top/mid/sub")).toBeNull();
    expect(backend.readMeta("/top/mid/sub/file2")).toBeNull();
    expect(backend.readMeta("/top/mid/lnk")).toBeNull();

    // New paths: all present
    expect(backend.readMeta("/top/moved")).not.toBeNull();
    expect(backend.readMeta("/top/moved/file1")).not.toBeNull();
    expect(backend.readMeta("/top/moved/sub")).not.toBeNull();
    expect(backend.readMeta("/top/moved/sub/file2")).not.toBeNull();
    expect(backend.readMeta("/top/moved/lnk")).not.toBeNull();

    // Crash + remount
    const { FS: FS2 } = await mountTome(backend);

    const buf = new Uint8Array(10);
    const sa = FS2.open(`${MOUNT}/top/moved/file1`, O.RDONLY);
    const na = FS2.read(sa, buf, 0, 10);
    FS2.close(sa);
    expect(decode(buf, na)).toBe("f1");

    const sb = FS2.open(`${MOUNT}/top/moved/sub/file2`, O.RDONLY);
    const nb = FS2.read(sb, buf, 0, 10);
    FS2.close(sb);
    expect(decode(buf, nb)).toBe("f2");

    expect(FS2.readlink(`${MOUNT}/top/moved/lnk`)).toBe("file1");
  });
});
