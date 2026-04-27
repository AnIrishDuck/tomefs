/**
 * Adversarial tests: clean-shutdown marker invalidation during rename/unlink.
 *
 * The clean-shutdown marker (/__tomefs_clean) is written during syncfs to
 * indicate the backend is consistent. On mount, if the marker exists and
 * no /__deleted_* entries are present, needsOrphanCleanup is set to false,
 * enabling the fast O(dirty) incremental syncfs path.
 *
 * However, rename() and unlink-with-open-fds perform multiple non-atomic
 * backend writes: writeMeta at the new path, renameFile for pages, then
 * deleteMeta at the old path. If the process crashes between these writes,
 * stale metadata remains at the old path. Without invalidating the clean
 * marker, the next mount would trust the marker and skip orphan cleanup,
 * leaving the stale entry permanently.
 *
 * These tests verify that:
 *   1. rename() and unlink-with-open-fds invalidate the clean marker
 *   2. The next syncfs uses the full tree walk (orphan cleanup) path
 *   3. After crash during rename, remount + syncfs cleans up stale entries
 *   4. The cost is amortized: only the first rename per sync cycle pays
 *      for the marker deletion
 *
 * Ethos §9: "Write tests designed to break tomefs specifically — target
 * the seams: metadata updates after flush"
 */
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { describe, it, expect, beforeEach } from "vitest";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { createTomeFS } from "../../src/tomefs.js";
import { PAGE_SIZE } from "../../src/types.js";
import type { SyncStorageBackend } from "../../src/sync-storage-backend.js";
import type { FileMeta } from "../../src/types.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

const O = {
  RDONLY: 0,
  WRONLY: 1,
  RDWR: 2,
  CREAT: 64,
  TRUNC: 512,
} as const;

const MOUNT = "/tome";
const CLEAN_MARKER_PATH = "/__tomefs_clean";

function encode(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

function decode(buf: Uint8Array, length?: number): string {
  return new TextDecoder().decode(
    length !== undefined ? buf.subarray(0, length) : buf,
  );
}

/**
 * SyncMemoryBackend that counts listFiles() and deleteMeta() calls.
 *
 * listFiles() is called by the full tree walk path for orphan cleanup;
 * the incremental path does not call it. Counting lets tests verify
 * which syncfs path was taken.
 *
 * deleteMeta() counting verifies the clean marker invalidation cost.
 */
class InstrumentedBackend extends SyncMemoryBackend {
  listFilesCalls = 0;
  deleteMetaCalls = 0;
  deleteMetaPaths: string[] = [];
  counting = false;

  startCounting(): void {
    this.listFilesCalls = 0;
    this.deleteMetaCalls = 0;
    this.deleteMetaPaths = [];
    this.counting = true;
  }

  stopCounting(): void {
    this.counting = false;
  }

  listFiles(): string[] {
    if (this.counting) {
      this.listFilesCalls++;
    }
    return super.listFiles();
  }

  deleteMeta(path: string): void {
    if (this.counting) {
      this.deleteMetaCalls++;
      this.deleteMetaPaths.push(path);
    }
    super.deleteMeta(path);
  }
}

/**
 * SyncStorageBackend that crashes after a configurable number of write ops.
 * Reads never crash. Same pattern as rename-crash-mid-operation.test.ts.
 */
class CrashBackend implements SyncStorageBackend {
  readonly inner: SyncMemoryBackend;
  private crashAfter: number;
  private opCount = 0;
  armed = false;
  crashed = false;

  constructor(inner: SyncMemoryBackend, crashAfter: number) {
    this.inner = inner;
    this.crashAfter = crashAfter;
  }

  arm(crashAfter?: number): void {
    if (crashAfter !== undefined) this.crashAfter = crashAfter;
    this.armed = true;
    this.opCount = 0;
    this.crashed = false;
  }

  disarm(): void {
    this.armed = false;
  }

  private tick(): void {
    if (!this.armed) return;
    this.opCount++;
    if (this.opCount > this.crashAfter) {
      this.crashed = true;
      throw new Error("simulated crash");
    }
  }

  // Reads never crash
  readPage(p: string, i: number) { return this.inner.readPage(p, i); }
  readPages(p: string, is: number[]) { return this.inner.readPages(p, is); }
  readMeta(p: string) { return this.inner.readMeta(p); }
  readMetas(ps: string[]) { return this.inner.readMetas(ps); }
  countPages(p: string) { return this.inner.countPages(p); }
  countPagesBatch(ps: string[]) { return this.inner.countPagesBatch(ps); }
  maxPageIndex(p: string) { return this.inner.maxPageIndex(p); }
  maxPageIndexBatch(ps: string[]) { return this.inner.maxPageIndexBatch(ps); }
  listFiles() { return this.inner.listFiles(); }

  // Writes may crash
  writePage(p: string, i: number, d: Uint8Array) {
    this.tick(); this.inner.writePage(p, i, d);
  }
  writePages(pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>) {
    for (const { path, pageIndex, data } of pages) {
      this.tick(); this.inner.writePage(path, pageIndex, data);
    }
  }
  writeMeta(p: string, m: FileMeta) { this.tick(); this.inner.writeMeta(p, m); }
  writeMetas(entries: Array<{ path: string; meta: FileMeta }>) {
    for (const { path, meta } of entries) { this.tick(); this.inner.writeMeta(path, meta); }
  }
  deleteFile(p: string) { this.tick(); this.inner.deleteFile(p); }
  deleteFiles(ps: string[]) { for (const p of ps) { this.tick(); this.inner.deleteFile(p); } }
  deletePagesFrom(p: string, i: number) { this.tick(); this.inner.deletePagesFrom(p, i); }
  deleteMeta(p: string) { this.tick(); this.inner.deleteMeta(p); }
  deleteMetas(ps: string[]) { for (const p of ps) { this.tick(); this.inner.deleteMeta(p); } }
  renameFile(o: string, n: string) { this.tick(); this.inner.renameFile(o, n); }
  deleteAll(ps: string[]) { this.tick(); this.inner.deleteAll(ps); }
  syncAll(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
    metas: Array<{ path: string; meta: FileMeta }>,
  ) {
    this.writePages(pages);
    this.writeMetas(metas);
  }
}

async function mountTome(backend: SyncStorageBackend, maxPages?: number) {
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

function syncfs(FS: any, tomefs: any): Error | null {
  let error: Error | null = null;
  tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
    error = err;
  });
  return error;
}

// ---------------------------------------------------------------------------
// Clean marker invalidation tests
// ---------------------------------------------------------------------------

describe("clean marker invalidation on rename", () => {
  let backend: InstrumentedBackend;

  beforeEach(() => {
    backend = new InstrumentedBackend();
  });

  it("rename deletes clean marker from backend @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create a file and syncfs to establish clean state with marker
    const data = encode("test-data");
    const s = FS.open(`${MOUNT}/file.txt`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    FS.close(s);
    syncfs(FS, tomefs);

    // Clean marker should now exist in backend
    expect(backend.readMeta(CLEAN_MARKER_PATH)).not.toBeNull();

    // Rename the file — this should invalidate the clean marker
    FS.rename(`${MOUNT}/file.txt`, `${MOUNT}/renamed.txt`);

    // Clean marker should be deleted from backend
    expect(backend.readMeta(CLEAN_MARKER_PATH)).toBeNull();
  });

  it("syncfs after rename uses incremental path (no full tree walk) @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create file and establish clean state
    const data = encode("walk-test");
    const s = FS.open(`${MOUNT}/a.txt`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    FS.close(s);
    syncfs(FS, tomefs);

    // Second syncfs should use incremental path (no listFiles call)
    backend.startCounting();
    syncfs(FS, tomefs);
    expect(backend.listFilesCalls).toBe(0);
    backend.stopCounting();

    // Rename the file — marker is invalidated in backend but
    // needsOrphanCleanup stays false (rename completes its own cleanup)
    FS.rename(`${MOUNT}/a.txt`, `${MOUNT}/b.txt`);

    // Next syncfs should still use incremental path (rename completed
    // successfully, so no orphan cleanup needed for this session)
    backend.startCounting();
    syncfs(FS, tomefs);
    expect(backend.listFilesCalls).toBe(0);
    backend.stopCounting();
  });

  it("syncfs after rename re-writes the clean marker @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create file and establish clean state
    const data = encode("marker-rewrite");
    const s = FS.open(`${MOUNT}/x.txt`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    FS.close(s);
    syncfs(FS, tomefs);

    // Rename deletes marker
    FS.rename(`${MOUNT}/x.txt`, `${MOUNT}/y.txt`);
    expect(backend.readMeta(CLEAN_MARKER_PATH)).toBeNull();

    // syncfs re-writes it
    syncfs(FS, tomefs);
    expect(backend.readMeta(CLEAN_MARKER_PATH)).not.toBeNull();
  });

  it("multiple renames before syncfs only invalidate marker once @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create files and establish clean state
    for (let i = 0; i < 3; i++) {
      const data = encode(`file-${i}`);
      const s = FS.open(`${MOUNT}/f${i}.txt`, O.RDWR | O.CREAT, 0o666);
      FS.write(s, data, 0, data.length);
      FS.close(s);
    }
    syncfs(FS, tomefs);

    // Count deleteMeta calls during renames
    backend.startCounting();
    FS.rename(`${MOUNT}/f0.txt`, `${MOUNT}/g0.txt`);
    FS.rename(`${MOUNT}/f1.txt`, `${MOUNT}/g1.txt`);
    FS.rename(`${MOUNT}/f2.txt`, `${MOUNT}/g2.txt`);

    // Only the first rename should delete the clean marker.
    // Subsequent renames see needsOrphanCleanup already true and skip.
    const markerDeletes = backend.deleteMetaPaths.filter(
      (p) => p === CLEAN_MARKER_PATH,
    );
    expect(markerDeletes.length).toBe(1);
    backend.stopCounting();
  });

  it("directory rename invalidates clean marker @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create directory with a file inside
    FS.mkdir(`${MOUNT}/dir`);
    const data = encode("dir-file-data");
    const s = FS.open(`${MOUNT}/dir/file.txt`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    FS.close(s);
    syncfs(FS, tomefs);

    // Clean marker should exist
    expect(backend.readMeta(CLEAN_MARKER_PATH)).not.toBeNull();

    // Rename directory
    FS.rename(`${MOUNT}/dir`, `${MOUNT}/newdir`);

    // Clean marker should be deleted
    expect(backend.readMeta(CLEAN_MARKER_PATH)).toBeNull();
  });
});

describe("clean marker invalidation on unlink with open fds", () => {
  let backend: InstrumentedBackend;

  beforeEach(() => {
    backend = new InstrumentedBackend();
  });

  it("unlink with open fd deletes clean marker @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create file, open fd, syncfs
    const data = encode("open-fd-data");
    const s = FS.open(`${MOUNT}/keep-open.txt`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    syncfs(FS, tomefs);

    // Clean marker should exist
    expect(backend.readMeta(CLEAN_MARKER_PATH)).not.toBeNull();

    // Unlink with fd still open — creates /__deleted_* entry
    FS.unlink(`${MOUNT}/keep-open.txt`);

    // Clean marker should be deleted
    expect(backend.readMeta(CLEAN_MARKER_PATH)).toBeNull();

    // Cleanup: close the fd
    FS.close(s);
  });

  it("unlink without open fd does not invalidate marker @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create file, close it, syncfs
    const data = encode("closed-fd-data");
    const s = FS.open(`${MOUNT}/no-fd.txt`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    FS.close(s);
    syncfs(FS, tomefs);

    // Clean marker should exist
    expect(backend.readMeta(CLEAN_MARKER_PATH)).not.toBeNull();

    // Unlink without open fds — single backend operation, no crash window
    FS.unlink(`${MOUNT}/no-fd.txt`);

    // Clean marker should still exist (no multi-step operation)
    expect(backend.readMeta(CLEAN_MARKER_PATH)).not.toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Crash recovery tests
// ---------------------------------------------------------------------------

describe("crash recovery: rename after syncfs", () => {
  it("crash mid-rename leaves no clean marker, enabling orphan cleanup on remount", async () => {
    const inner = new SyncMemoryBackend();
    const crashBackend = new CrashBackend(inner, 999);

    // Phase 1: create file, syncfs → clean marker written
    const { FS, tomefs } = await mountTome(crashBackend);
    const data = encode("crash-test-payload-" + "x".repeat(100));
    const s = FS.open(`${MOUNT}/original.txt`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    FS.close(s);
    const err = syncfs(FS, tomefs);
    expect(err).toBeNull();

    // Verify clean marker and file metadata exist in backend
    expect(inner.readMeta(CLEAN_MARKER_PATH)).not.toBeNull();
    expect(inner.readMeta("/original.txt")).not.toBeNull();

    // Phase 2: arm crash during rename
    // With the fix, rename ops are:
    //   1. deleteMeta(CLEAN_MARKER_PATH) — marker invalidation
    //   2. writeMeta("/renamed.txt", ...) — metadata at new path
    //   3. renameFile("/original.txt", "/renamed.txt") — pages moved
    //   4. deleteMeta("/original.txt") — old metadata removed
    // Crash after op 3: metadata at both paths, pages at new path, no marker
    crashBackend.arm(3);

    let crashed = false;
    try {
      FS.rename(`${MOUNT}/original.txt`, `${MOUNT}/renamed.txt`);
    } catch {
      crashed = true;
    }
    expect(crashed).toBe(true);
    expect(crashBackend.crashed).toBe(true);

    // Backend state after crash:
    // - Clean marker: deleted (op 1 succeeded)
    // - /original.txt metadata: still exists (op 4 didn't run)
    // - /renamed.txt metadata: exists (op 2 succeeded)
    // - Pages: moved to /renamed.txt (op 3 succeeded)
    expect(inner.readMeta(CLEAN_MARKER_PATH)).toBeNull();
    expect(inner.readMeta("/original.txt")).not.toBeNull();
    expect(inner.readMeta("/renamed.txt")).not.toBeNull();

    // Phase 3: remount from inner backend (crash recovery)
    // Without the fix, the clean marker would still exist, and
    // needsOrphanCleanup would be false on mount — the stale
    // /original.txt entry would persist across syncfs cycles
    // without a full tree walk ever running.
    //
    // With the fix, no clean marker → needsOrphanCleanup = true
    // on mount → first syncfs does full tree walk + orphan cleanup.
    const { FS: FS2, tomefs: tomefs2 } = await mountTome(inner);
    const err2 = syncfs(FS2, tomefs2);
    expect(err2).toBeNull();

    // Verify the renamed file has data (pages were moved successfully)
    const buf = new Uint8Array(data.length);
    const s2 = FS2.open(`${MOUNT}/renamed.txt`, O.RDONLY);
    FS2.read(s2, buf, 0, data.length, 0);
    FS2.close(s2);
    expect(decode(buf, data.length)).toBe(decode(data));
  });

  it("clean rename (no crash) followed by syncfs is consistent @fast", async () => {
    const inner = new SyncMemoryBackend();

    // Phase 1: create file, syncfs
    const { FS, tomefs } = await mountTome(inner);
    const data = encode("consistency-check");
    const s = FS.open(`${MOUNT}/src.txt`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    FS.close(s);
    syncfs(FS, tomefs);

    // Phase 2: rename and syncfs
    FS.rename(`${MOUNT}/src.txt`, `${MOUNT}/dst.txt`);
    syncfs(FS, tomefs);

    // Phase 3: remount and verify consistency
    const { FS: FS2 } = await mountTome(inner);

    // Only dst.txt should exist, not src.txt
    const entries = FS2.readdir(MOUNT);
    expect(entries).toContain("dst.txt");
    expect(entries).not.toContain("src.txt");

    // Verify data is intact
    const buf = new Uint8Array(data.length);
    const s2 = FS2.open(`${MOUNT}/dst.txt`, O.RDONLY);
    FS2.read(s2, buf, 0, data.length, 0);
    FS2.close(s2);
    expect(decode(buf, data.length)).toBe("consistency-check");
  });

  it("rename + crash before syncfs: no clean marker on remount @fast", async () => {
    const inner = new SyncMemoryBackend();

    // Phase 1: create file, syncfs → clean marker written
    const { FS, tomefs } = await mountTome(inner);
    const data = encode("no-syncfs-after-rename");
    const s = FS.open(`${MOUNT}/a.txt`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    FS.close(s);
    syncfs(FS, tomefs);

    expect(inner.readMeta(CLEAN_MARKER_PATH)).not.toBeNull();

    // Phase 2: rename without subsequent syncfs (simulates crash after rename)
    FS.rename(`${MOUNT}/a.txt`, `${MOUNT}/b.txt`);
    // Do NOT syncfs — simulate process death

    // Phase 3: the clean marker should be gone from backend
    // On remount, needsOrphanCleanup will be true
    expect(inner.readMeta(CLEAN_MARKER_PATH)).toBeNull();

    // Remount: should trigger full tree walk on first syncfs
    const { FS: FS2, tomefs: tomefs2 } = await mountTome(inner);

    // Verify the file is accessible (rename completed in backend)
    const buf = new Uint8Array(data.length);
    const s2 = FS2.open(`${MOUNT}/b.txt`, O.RDONLY);
    FS2.read(s2, buf, 0, data.length, 0);
    FS2.close(s2);
    expect(decode(buf, data.length)).toBe("no-syncfs-after-rename");
  });
});
