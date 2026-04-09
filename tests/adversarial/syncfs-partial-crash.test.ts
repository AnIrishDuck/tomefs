/**
 * Adversarial tests for partial syncfs completion + restoreTree recovery.
 *
 * Simulates the process crashing at various points during a syncfs call:
 *   - During page writes (partial pages persisted, no metadata)
 *   - After page writes but before metadata batch write
 *   - During the metadata batch write (partial metadata persisted)
 *   - After metadata write but before orphan cleanup
 *
 * Verifies that remounting from the partially-written backend state always
 * produces a usable filesystem: files created before the last successful
 * sync are intact, and files modified since may or may not reflect the
 * latest writes depending on crash timing — but no corruption occurs.
 *
 * Uses a CrashAfterNOpsSyncBackend fake (not a mock) that wraps
 * SyncMemoryBackend and throws after a configurable number of mutating
 * operations.
 *
 * Ethos §9: "Write tests designed to break tomefs specifically — target
 * the seams: metadata updates after flush, dirty flush ordering"
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

function encode(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

function decode(buf: Uint8Array, length?: number): string {
  return new TextDecoder().decode(
    length !== undefined ? buf.subarray(0, length) : buf,
  );
}

/**
 * SyncStorageBackend fake that wraps SyncMemoryBackend and throws
 * after a configurable number of mutating operations.
 *
 * "Mutating operations" are: writePage, writePages (counted per page),
 * writeMeta, writeMetas (counted per entry), deleteFile, deleteFiles
 * (counted per path), deleteMeta, deleteMetas (counted per path),
 * deletePagesFrom, renameFile.
 *
 * Read operations never throw (they represent the crash-surviving state).
 */
class CrashAfterNOpsSyncBackend implements SyncStorageBackend {
  readonly inner: SyncMemoryBackend;
  private crashAfter: number;
  private opCount = 0;
  armed = false;
  crashed = false;

  constructor(inner: SyncMemoryBackend, crashAfter: number) {
    this.inner = inner;
    this.crashAfter = crashAfter;
  }

  /** Arm the crash trigger. No-op until armed. */
  arm(): void {
    this.armed = true;
    this.opCount = 0;
    this.crashed = false;
  }

  /** Disarm — allow all operations to proceed. */
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

  // --- Reads (never crash) ---

  readPage(path: string, pageIndex: number): Uint8Array | null {
    return this.inner.readPage(path, pageIndex);
  }
  readPages(path: string, pageIndices: number[]): Array<Uint8Array | null> {
    return this.inner.readPages(path, pageIndices);
  }
  readMeta(path: string): FileMeta | null {
    return this.inner.readMeta(path);
  }
  readMetas(paths: string[]): Array<FileMeta | null> {
    return this.inner.readMetas(paths);
  }
  countPages(path: string): number {
    return this.inner.countPages(path);
  }
  countPagesBatch(paths: string[]): number[] {
    return this.inner.countPagesBatch(paths);
  }
  maxPageIndex(path: string): number {
    return this.inner.maxPageIndex(path);
  }
  maxPageIndexBatch(paths: string[]): number[] {
    return this.inner.maxPageIndexBatch(paths);
  }
  listFiles(): string[] {
    return this.inner.listFiles();
  }

  // --- Writes (may crash) ---

  writePage(path: string, pageIndex: number, data: Uint8Array): void {
    this.tick();
    this.inner.writePage(path, pageIndex, data);
  }

  writePages(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
  ): void {
    for (const { path, pageIndex, data } of pages) {
      this.tick();
      this.inner.writePage(path, pageIndex, data);
    }
  }

  writeMeta(path: string, meta: FileMeta): void {
    this.tick();
    this.inner.writeMeta(path, meta);
  }

  writeMetas(entries: Array<{ path: string; meta: FileMeta }>): void {
    for (const { path, meta } of entries) {
      this.tick();
      this.inner.writeMeta(path, meta);
    }
  }

  deleteFile(path: string): void {
    this.tick();
    this.inner.deleteFile(path);
  }

  deleteFiles(paths: string[]): void {
    for (const path of paths) {
      this.tick();
      this.inner.deleteFile(path);
    }
  }

  deletePagesFrom(path: string, fromPageIndex: number): void {
    this.tick();
    this.inner.deletePagesFrom(path, fromPageIndex);
  }

  deleteMeta(path: string): void {
    this.tick();
    this.inner.deleteMeta(path);
  }

  deleteMetas(paths: string[]): void {
    for (const path of paths) {
      this.tick();
      this.inner.deleteMeta(path);
    }
  }

  renameFile(oldPath: string, newPath: string): void {
    this.tick();
    this.inner.renameFile(oldPath, newPath);
  }

  syncAll(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
    metas: Array<{ path: string; meta: FileMeta }>,
  ): void {
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

function syncAndUnmount(FS: any, tomefs: any) {
  const err = syncfs(FS, tomefs);
  if (err) throw err;
  FS.unmount(MOUNT);
}

/**
 * Mount fresh with the inner backend (bypassing crash wrapper) to verify
 * that the backend state is self-consistent after a partial sync.
 */
async function remountAndVerify(
  inner: SyncMemoryBackend,
  maxPages?: number,
) {
  const { FS, tomefs } = await mountTome(inner, maxPages);
  return { FS, tomefs };
}

// ---------------------------------------------------------------------------
// Test: crash during page flush (flushAll)
// ---------------------------------------------------------------------------

describe("partial syncfs: crash during page flush", () => {
  let inner: SyncMemoryBackend;

  beforeEach(() => {
    inner = new SyncMemoryBackend();
  });

  it("page data written by syncfs survives crash during metadata write @fast", async () => {
    // Phase 1: establish baseline
    const { FS, tomefs } = await mountTome(inner);
    const s = FS.open(`${MOUNT}/base`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("baseline"), 0, 8);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // Phase 2: modify file. Dirty pages stay in cache until syncfs.
    // syncfs writes pages first (via syncAll), then metadata.
    // crashAfter=1 allows the page write but crashes on first metadata entry.
    const crashBackend = new CrashAfterNOpsSyncBackend(inner, 1);
    const { FS: FS2, tomefs: t2 } = await mountTome(crashBackend);
    const s2 = FS2.open(`${MOUNT}/base`, O.RDWR);
    FS2.write(s2, encode("MODIFIED"), 0, 8);
    FS2.close(s2);

    // Crash after page write succeeds but before metadata write completes
    crashBackend.arm();
    const err = syncfs(FS2, t2);
    expect(err).not.toBeNull();

    // Phase 3: remount — page data reflects the write (written by syncfs
    // before the crash). Since the file size didn't change (still 8 bytes),
    // restoreTree trusts the stale metadata and the page data has the
    // latest content.
    const { FS: FS3 } = await remountAndVerify(inner);
    const buf = new Uint8Array(20);
    const s3 = FS3.open(`${MOUNT}/base`, O.RDONLY);
    const n = FS3.read(s3, buf, 0, 20);
    FS3.close(s3);
    // Page was written by syncfs before crash, so new content is in the backend
    expect(decode(buf, n)).toBe("MODIFIED");
  });

  it("file extension: pages written by syncfs but stale metadata size is detected @fast", async () => {
    // Phase 1: create a small file
    const { FS, tomefs } = await mountTome(inner);
    const s = FS.open(`${MOUNT}/grow`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("small"), 0, 5);
    FS.close(s);
    syncAndUnmount(FS, tomefs);
    expect(inner.readMeta("/grow")!.size).toBe(5);

    // Phase 2: extend file to multiple pages. Dirty pages stay in cache
    // until syncfs. syncfs writes pages first (via syncAll), then metadata.
    // crashAfter=2 allows both page writes but crashes on first metadata entry.
    const crashBackend = new CrashAfterNOpsSyncBackend(inner, 2);
    const { FS: FS2, tomefs: t2 } = await mountTome(crashBackend);
    const s2 = FS2.open(`${MOUNT}/grow`, O.RDWR);
    const bigData = new Uint8Array(PAGE_SIZE * 2);
    for (let i = 0; i < bigData.length; i++) bigData[i] = (i * 7) & 0xff;
    FS2.write(s2, bigData, 0, bigData.length);
    FS2.close(s2);

    crashBackend.arm();
    syncfs(FS2, t2); // crashes after page writes, before metadata

    // Backend has pages 0+1 (written by syncfs), but metadata still says size=5.
    // restoreTree should detect extra pages and expand file size.
    expect(inner.readMeta("/grow")!.size).toBe(5); // stale metadata
    expect(inner.countPages("/grow")).toBe(2); // pages written by syncfs

    // Phase 3: remount — restoreTree detects page count mismatch
    const { FS: FS3 } = await remountAndVerify(inner);
    const stat = FS3.stat(`${MOUNT}/grow`);
    // File should be at least 2 pages (restoreTree adjusts up)
    expect(stat.size).toBe(PAGE_SIZE * 2);

    // Data should be readable
    const buf = new Uint8Array(PAGE_SIZE * 2);
    const s3 = FS3.open(`${MOUNT}/grow`, O.RDONLY);
    FS3.read(s3, buf, 0, PAGE_SIZE * 2);
    FS3.close(s3);
    expect(buf).toEqual(bigData);
  });
});

// ---------------------------------------------------------------------------
// Test: crash during metadata batch write (partial metadata persisted)
// ---------------------------------------------------------------------------

describe("partial syncfs: crash during metadata write", () => {
  let inner: SyncMemoryBackend;

  beforeEach(() => {
    inner = new SyncMemoryBackend();
  });

  it("file extension with stale metadata is recovered by restoreTree @fast", async () => {
    // Phase 1: create a file and sync successfully
    const { FS, tomefs } = await mountTome(inner);
    const s = FS.open(`${MOUNT}/data`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("v1"), 0, 2);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // Phase 2: extend the file significantly (new pages). Dirty pages
    // stay in cache until syncfs. syncfs writes pages first via syncAll.
    // crashAfter=3 allows all 3 page writes but crashes on first metadata entry.
    const crashBackend = new CrashAfterNOpsSyncBackend(inner, 3);
    const { FS: FS2, tomefs: t2 } = await mountTome(crashBackend);
    const s2 = FS2.open(`${MOUNT}/data`, O.RDWR);
    const bigData = new Uint8Array(PAGE_SIZE * 3);
    for (let i = 0; i < bigData.length; i++) bigData[i] = (i * 7) & 0xff;
    FS2.write(s2, bigData, 0, bigData.length);
    FS2.close(s2);

    crashBackend.arm();
    const err = syncfs(FS2, t2);
    expect(err).not.toBeNull();

    // Metadata is stale (size=2), but pages 0-2 are in the backend (written by syncfs)
    expect(inner.readMeta("/data")!.size).toBe(2);
    expect(inner.countPages("/data")).toBe(3);

    // Phase 3: remount — restoreTree detects extra pages
    const { FS: FS3 } = await remountAndVerify(inner);
    const stat = FS3.stat(`${MOUNT}/data`);
    expect(stat.size).toBe(PAGE_SIZE * 3);

    // The first page should contain our written data
    const readBuf = new Uint8Array(PAGE_SIZE);
    const s3 = FS3.open(`${MOUNT}/data`, O.RDONLY);
    FS3.read(s3, readBuf, 0, PAGE_SIZE);
    FS3.close(s3);
    expect(readBuf).toEqual(bigData.subarray(0, PAGE_SIZE));
  });

  it("multiple files: partial metadata write leaves filesystem recoverable", async () => {
    // Phase 1: create multiple files
    const { FS, tomefs } = await mountTome(inner);
    for (const name of ["alpha", "beta", "gamma"]) {
      const s = FS.open(`${MOUNT}/${name}`, O.RDWR | O.CREAT, 0o666);
      FS.write(s, encode(`v1-${name}`), 0, `v1-${name}`.length);
      FS.close(s);
    }
    syncAndUnmount(FS, tomefs);

    // Phase 2: modify all files (same size, so no page count mismatch).
    // Dirty pages stay in cache until syncfs. syncfs writes pages first
    // via syncAll (3 pages = ops 1-3), then metadata (3 files + clean
    // marker = ops 4-7). crashAfter=5 allows all 3 page writes + 2
    // metadata entries, crashing on the 3rd metadata entry.
    const crashBackend = new CrashAfterNOpsSyncBackend(inner, 5);
    const { FS: FS2, tomefs: t2 } = await mountTome(crashBackend);
    for (const name of ["alpha", "beta", "gamma"]) {
      const s = FS2.open(`${MOUNT}/${name}`, O.RDWR);
      FS2.write(s, encode(`v2-${name}`), 0, `v2-${name}`.length);
      FS2.close(s);
    }

    crashBackend.arm();
    const err = syncfs(FS2, t2);
    expect(err).not.toBeNull();

    // Phase 3: remount — all files should be readable (no corruption)
    // Page data was written by syncfs (all 3 pages before crash).
    // Metadata may be v1 or v2 depending on crash point — but since sizes
    // match (v1 and v2 have same length), restoreTree trusts metadata.
    const { FS: FS3 } = await remountAndVerify(inner);
    for (const name of ["alpha", "beta", "gamma"]) {
      const s = FS3.open(`${MOUNT}/${name}`, O.RDONLY);
      const buf = new Uint8Array(50);
      const n = FS3.read(s, buf, 0, 50);
      FS3.close(s);
      const content = decode(buf, n);
      // Page data is v2 (written by syncfs before crash), regardless of metadata state
      expect(content).toBe(`v2-${name}`);
    }
  });

  it("partially written metadata + subsequent clean sync produces consistent state @fast", async () => {
    // Phase 1: baseline
    const { FS, tomefs } = await mountTome(inner);
    const s = FS.open(`${MOUNT}/file`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("original"), 0, 8);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // Phase 2: modify + crash during sync
    const crashBackend = new CrashAfterNOpsSyncBackend(inner, 2);
    const { FS: FS2, tomefs: t2 } = await mountTome(crashBackend);
    const s2 = FS2.open(`${MOUNT}/file`, O.RDWR);
    FS2.write(s2, encode("changed!"), 0, 8);
    FS2.close(s2);
    crashBackend.arm();
    syncfs(FS2, t2); // may partially succeed

    // Phase 3: remount + clean sync — should reach fully consistent state
    const { FS: FS3, tomefs: t3 } = await remountAndVerify(inner);
    const s3 = FS3.open(`${MOUNT}/file`, O.RDONLY);
    const buf = new Uint8Array(20);
    const n = FS3.read(s3, buf, 0, 20);
    FS3.close(s3);
    const content = decode(buf, n);
    // Content is "original" or "changed!" — depends on crash timing
    expect(["original", "changed!"]).toContain(content);

    // Now do a clean sync + remount to verify full consistency
    syncAndUnmount(FS3, t3);
    const { FS: FS4 } = await remountAndVerify(inner);
    const s4 = FS4.open(`${MOUNT}/file`, O.RDONLY);
    const buf2 = new Uint8Array(20);
    const n2 = FS4.read(s4, buf2, 0, 20);
    FS4.close(s4);
    // After a clean sync, content should match what was read before sync
    expect(decode(buf2, n2)).toBe(content);
  });
});

// ---------------------------------------------------------------------------
// Test: crash during orphan cleanup
// ---------------------------------------------------------------------------

describe("partial syncfs: crash during orphan cleanup", () => {
  let inner: SyncMemoryBackend;

  beforeEach(() => {
    inner = new SyncMemoryBackend();
  });

  it("orphans survive partial cleanup and are cleaned on next sync @fast", async () => {
    // Phase 1: create files and sync
    const { FS, tomefs } = await mountTome(inner);
    for (const name of ["keep", "remove"]) {
      const s = FS.open(`${MOUNT}/${name}`, O.RDWR | O.CREAT, 0o666);
      FS.write(s, encode(`data-${name}`), 0, `data-${name}`.length);
      FS.close(s);
    }
    syncAndUnmount(FS, tomefs);

    // Phase 2: delete a file, then crash during syncfs orphan cleanup
    // Use a high crash count so pages + metadata write succeeds,
    // but orphan cleanup (listFiles + deleteFiles + deleteMetas) crashes.
    const crashBackend = new CrashAfterNOpsSyncBackend(inner, 100);
    const { FS: FS2, tomefs: t2 } = await mountTome(crashBackend);
    FS2.unlink(`${MOUNT}/remove`);

    // Inject stale metadata back to simulate orphan (unlink already cleaned
    // the backend; we re-add it to simulate a prior crash leaving orphans)
    inner.writeMeta("/remove", {
      size: 11,
      mode: 0o100666,
      ctime: Date.now(),
      mtime: Date.now(),
    });
    inner.writePage("/remove", 0, new Uint8Array(PAGE_SIZE));

    // Make crash happen during orphan cleanup phase.
    // Count: writeMetas for /keep = 1, then orphan cleanup starts:
    // listFiles doesn't crash (read), deleteFiles for /remove crashes.
    // Set crash count high enough to let metadata write succeed but crash
    // during deleteFiles.
    crashBackend.disarm();

    // Re-mount fresh for a controlled test
    const crashBackend2 = new CrashAfterNOpsSyncBackend(inner, 3);
    const { FS: FS3, tomefs: t3 } = await mountTome(crashBackend2);
    FS3.unlink(`${MOUNT}/remove`);

    // Re-inject orphan
    inner.writeMeta("/remove", {
      size: 11,
      mode: 0o100666,
      ctime: Date.now(),
      mtime: Date.now(),
    });
    inner.writePage("/remove", 0, new Uint8Array(PAGE_SIZE));

    crashBackend2.arm();
    const err = syncfs(FS3, t3);
    // May or may not crash depending on exact op count

    // Phase 3: remount and verify — orphan may or may not be present
    const { FS: FS4, tomefs: t4 } = await remountAndVerify(inner);

    // "keep" must always be readable
    const s4 = FS4.open(`${MOUNT}/keep`, O.RDONLY);
    const buf = new Uint8Array(20);
    const n = FS4.read(s4, buf, 0, 20);
    FS4.close(s4);
    expect(decode(buf, n)).toBe("data-keep");

    // Clean sync should eliminate any orphans
    syncAndUnmount(FS4, t4);
    const { FS: FS5, tomefs: t5 } = await remountAndVerify(inner);

    // Only "keep" should exist
    const listing = FS5.readdir(`${MOUNT}`);
    expect(listing).toContain("keep");
    // "remove" might still be there as an orphan from the crash, or cleaned up.
    // After a second clean sync, it must be gone.
    syncAndUnmount(FS5, t5);
    const { FS: FS6 } = await remountAndVerify(inner);
    const listing2 = FS6.readdir(`${MOUNT}`);
    const filesOnly = listing2.filter(
      (f: string) => f !== "." && f !== "..",
    );
    expect(filesOnly).toEqual(["keep"]);
  });
});

// ---------------------------------------------------------------------------
// Test: crash at every possible point during syncfs
// ---------------------------------------------------------------------------

describe("partial syncfs: exhaustive crash-point sweep", () => {
  let inner: SyncMemoryBackend;

  beforeEach(() => {
    inner = new SyncMemoryBackend();
  });

  it("filesystem is always recoverable regardless of crash point @fast", async () => {
    // Phase 1: establish baseline with several files + directory
    const { FS, tomefs } = await mountTome(inner);
    FS.mkdir(`${MOUNT}/dir`);
    for (const name of ["a", "b", "dir/c"]) {
      const s = FS.open(`${MOUNT}/${name}`, O.RDWR | O.CREAT, 0o666);
      const data = new Uint8Array(PAGE_SIZE + 100);
      for (let i = 0; i < data.length; i++) data[i] = (name.charCodeAt(0) + i) & 0xff;
      FS.write(s, data, 0, data.length);
      FS.close(s);
    }
    syncAndUnmount(FS, tomefs);

    // Take a snapshot of the baseline backend state
    const baselineMeta = new Map<string, FileMeta>();
    for (const path of inner.listFiles()) {
      const m = inner.readMeta(path);
      if (m) baselineMeta.set(path, { ...m });
    }

    // Phase 2: for each crash point (0 through 20), modify files, crash,
    // and verify recovery
    for (let crashPoint = 0; crashPoint <= 20; crashPoint++) {
      // Reset backend to baseline
      // (Re-write baseline metadata and pages)
      const freshInner = new SyncMemoryBackend();
      for (const [path, meta] of baselineMeta) {
        freshInner.writeMeta(path, meta);
      }
      // Re-write pages for baseline files
      for (const path of ["a", "b", "dir/c"].map((n) => `/${n}`)) {
        const meta = baselineMeta.get(path);
        if (!meta) continue;
        const pageCount = Math.ceil(meta.size / PAGE_SIZE);
        for (let p = 0; p < pageCount; p++) {
          const page = inner.readPage(path, p);
          if (page) freshInner.writePage(path, p, page);
        }
      }
      // Copy directory metadata
      const dirMeta = baselineMeta.get("/dir");
      if (dirMeta) freshInner.writeMeta("/dir", dirMeta);

      const crashBackend = new CrashAfterNOpsSyncBackend(
        freshInner,
        crashPoint,
      );
      const { FS: FS2, tomefs: t2 } = await mountTome(crashBackend);

      // Modify all files
      for (const name of ["a", "b", "dir/c"]) {
        const s = FS2.open(`${MOUNT}/${name}`, O.RDWR);
        FS2.write(s, encode(`modified-${crashPoint}`), 0, `modified-${crashPoint}`.length);
        FS2.close(s);
      }

      // Delete one file to exercise orphan cleanup
      FS2.unlink(`${MOUNT}/b`);

      crashBackend.arm();
      syncfs(FS2, t2); // may or may not crash

      // Phase 3: remount from crash backend's inner state
      // This must ALWAYS succeed — no exception, no corruption
      let recoveredFS: any;
      try {
        const result = await remountAndVerify(freshInner);
        recoveredFS = result.FS;
      } catch (e) {
        throw new Error(
          `Crash at point ${crashPoint}: remount failed: ${e}`,
        );
      }

      // Files "a" and "dir/c" must exist and be readable
      for (const name of ["a", "dir/c"]) {
        try {
          const s = recoveredFS.open(`${MOUNT}/${name}`, O.RDONLY);
          const buf = new Uint8Array(PAGE_SIZE * 2);
          const n = recoveredFS.read(s, buf, 0, buf.length);
          recoveredFS.close(s);
          expect(n).toBeGreaterThan(0);
        } catch (e) {
          throw new Error(
            `Crash at point ${crashPoint}: file ${name} not readable: ${e}`,
          );
        }
      }

      // File "b" may or may not exist depending on whether unlink
      // cleanup was persisted before the crash. Either way, the
      // filesystem should be consistent — no exception on readdir.
      const listing = recoveredFS.readdir(`${MOUNT}`);
      expect(listing).toContain(".");
      expect(listing).toContain("..");
      expect(listing).toContain("a");
      expect(listing).toContain("dir");
    }
  });
});

// ---------------------------------------------------------------------------
// Test: crash with cache pressure (dirty eviction during flushAll)
// ---------------------------------------------------------------------------

describe("partial syncfs: crash under cache pressure", () => {
  let inner: SyncMemoryBackend;

  beforeEach(() => {
    inner = new SyncMemoryBackend();
  });

  it("files survive crash when dirty pages were evicted before sync", async () => {
    // Phase 1: baseline with a tiny cache
    const { FS, tomefs } = await mountTome(inner, 4);
    const s = FS.open(`${MOUNT}/large`, O.RDWR | O.CREAT, 0o666);
    const data = new Uint8Array(PAGE_SIZE * 8); // 8 pages, 4-page cache
    for (let i = 0; i < data.length; i++) data[i] = (i * 11) & 0xff;
    FS.write(s, data, 0, data.length);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // Phase 2: modify file, let some dirty pages evict naturally, then crash
    const crashBackend = new CrashAfterNOpsSyncBackend(inner, 3);
    const { FS: FS2, tomefs: t2 } = await mountTome(crashBackend, 4);
    const s2 = FS2.open(`${MOUNT}/large`, O.RDWR);
    // Write to all 8 pages — with 4-page cache, dirty pages get evicted
    // to backend during writes (before syncfs even starts)
    for (let p = 0; p < 8; p++) {
      const pageData = new Uint8Array(PAGE_SIZE);
      pageData.fill(0x42 + p);
      FS2.write(s2, pageData, 0, PAGE_SIZE, p * PAGE_SIZE);
    }
    FS2.close(s2);

    // Some pages were already evicted to backend. syncfs will crash early.
    crashBackend.arm();
    syncfs(FS2, t2);

    // Phase 3: remount — should recover. Some pages may have new data
    // (from eviction), some may have old data (from baseline). But the
    // file should be readable with no corruption.
    const { FS: FS3 } = await remountAndVerify(inner, 4);
    const stat = FS3.stat(`${MOUNT}/large`);
    expect(stat.size).toBeGreaterThanOrEqual(PAGE_SIZE * 8);

    // All pages should be readable
    const buf = new Uint8Array(PAGE_SIZE);
    const s3 = FS3.open(`${MOUNT}/large`, O.RDONLY);
    for (let p = 0; p < 8; p++) {
      const n = FS3.read(s3, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expect(n).toBe(PAGE_SIZE);
      // Each page is either old data or new data — both are valid
      const isOld = buf[0] === ((p * PAGE_SIZE * 11) & 0xff);
      const isNew = buf[0] === 0x42 + p;
      expect(isOld || isNew).toBe(true);
    }
    FS3.close(s3);
  });

  it("new files created between syncs are discoverable after crash", async () => {
    // Phase 1: baseline
    const { FS, tomefs } = await mountTome(inner, 4);
    const s = FS.open(`${MOUNT}/existing`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("existing"), 0, 8);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // Phase 2: create a new file, then crash during sync
    // Pages for the new file get written (flushAll), but metadata may not
    const crashBackend = new CrashAfterNOpsSyncBackend(inner, 2);
    const { FS: FS2, tomefs: t2 } = await mountTome(crashBackend, 4);
    const s2 = FS2.open(`${MOUNT}/new_file`, O.RDWR | O.CREAT, 0o666);
    FS2.write(s2, encode("brand new"), 0, 9);
    FS2.close(s2);

    crashBackend.arm();
    syncfs(FS2, t2);

    // Phase 3: remount — existing file must be present.
    // New file may or may not be present depending on whether its
    // metadata was written before the crash.
    const { FS: FS3, tomefs: t3 } = await remountAndVerify(inner, 4);
    const s3 = FS3.open(`${MOUNT}/existing`, O.RDONLY);
    const buf = new Uint8Array(20);
    const n = FS3.read(s3, buf, 0, 20);
    FS3.close(s3);
    expect(decode(buf, n)).toBe("existing");

    // Clean sync to stabilize
    syncAndUnmount(FS3, t3);
    const { FS: FS4 } = await remountAndVerify(inner, 4);
    // After clean sync, existing file is definitely present
    const s4 = FS4.open(`${MOUNT}/existing`, O.RDONLY);
    const buf2 = new Uint8Array(20);
    const n2 = FS4.read(s4, buf2, 0, 20);
    FS4.close(s4);
    expect(decode(buf2, n2)).toBe("existing");
  });
});

// ---------------------------------------------------------------------------
// Test: directory rename + crash (complex metadata operation)
// ---------------------------------------------------------------------------

describe("partial syncfs: crash after directory rename", () => {
  let inner: SyncMemoryBackend;

  beforeEach(() => {
    inner = new SyncMemoryBackend();
  });

  it("directory rename metadata is recoverable after partial sync", async () => {
    // Phase 1: create directory tree
    const { FS, tomefs } = await mountTome(inner);
    FS.mkdir(`${MOUNT}/src`);
    const s = FS.open(`${MOUNT}/src/data`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("important"), 0, 9);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // Phase 2: rename directory, then crash during sync
    // Directory rename eagerly writes metadata to the backend (not deferred
    // to syncfs), so the rename itself should survive. But syncfs cleanup
    // (deleting old metadata at /src) may not complete.
    const crashBackend = new CrashAfterNOpsSyncBackend(inner, 3);
    const { FS: FS2, tomefs: t2 } = await mountTome(crashBackend);
    FS2.rename(`${MOUNT}/src`, `${MOUNT}/dst`);

    crashBackend.arm();
    syncfs(FS2, t2);

    // Phase 3: remount — data should be at new path (or both paths)
    const { FS: FS3, tomefs: t3 } = await remountAndVerify(inner);
    const listing = FS3.readdir(`${MOUNT}`);

    // Data must be accessible at dst (rename was eager)
    expect(listing).toContain("dst");
    const subListing = FS3.readdir(`${MOUNT}/dst`);
    expect(subListing).toContain("data");

    const s3 = FS3.open(`${MOUNT}/dst/data`, O.RDONLY);
    const buf = new Uint8Array(20);
    const n = FS3.read(s3, buf, 0, 20);
    FS3.close(s3);
    expect(decode(buf, n)).toBe("important");

    // Clean sync should remove any stale /src entries
    syncAndUnmount(FS3, t3);
    const { FS: FS4 } = await remountAndVerify(inner);
    const finalListing = FS4.readdir(`${MOUNT}`).filter(
      (f: string) => f !== "." && f !== "..",
    );
    expect(finalListing).toEqual(["dst"]);
  });
});
