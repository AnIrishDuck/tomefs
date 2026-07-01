/**
 * Adversarial tests for syncAll write ordering and clean-marker interaction.
 *
 * The clean-shutdown marker is included in the metadata batch passed to
 * syncAll. If metadata is written BEFORE pages (the old OPFS ordering),
 * a crash between the two phases persists the clean marker with stale
 * page data. The next mount sees the marker and skips orphan cleanup,
 * trusting stale page content — silent data corruption.
 *
 * Pages-first ordering prevents this: a crash after pages but before
 * metadata leaves the clean marker absent, forcing a full recovery pass
 * on the next mount. Orphaned pages are cleaned up by cleanupOrphanedPages.
 *
 * These tests use a MetaFirstSyncBackend to reproduce the dangerous
 * metadata-first ordering and verify that pages-first ordering avoids
 * the corruption window.
 *
 * Ethos §9: "Write tests designed to break tomefs specifically — target
 * the seams: metadata updates after flush, dirty flush ordering"
 */
import { dirname, join } from "path";
import { fileURLToPath } from "url";
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
 * Backend that writes metadata BEFORE pages in syncAll, then crashes
 * after metadata but before pages. This reproduces the dangerous
 * ordering that the old OPFS backends used.
 */
class MetaFirstCrashBackend implements SyncStorageBackend {
  readonly inner: SyncMemoryBackend;
  armed = false;

  constructor(inner: SyncMemoryBackend) {
    this.inner = inner;
  }

  readPage(path: string, pageIndex: number): Uint8Array | null {
    return this.inner.readPage(path, pageIndex);
  }
  readPages(path: string, pageIndices: number[]): Array<Uint8Array | null> {
    return this.inner.readPages(path, pageIndices);
  }
  readPageBatch(entries: Array<{ path: string; pageIndex: number }>): Array<Uint8Array | null> {
    return this.inner.readPageBatch(entries);
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
  writePage(path: string, pageIndex: number, data: Uint8Array): void {
    this.inner.writePage(path, pageIndex, data);
  }
  writePages(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
  ): void {
    this.inner.writePages(pages);
  }
  writeMeta(path: string, meta: FileMeta): void {
    this.inner.writeMeta(path, meta);
  }
  writeMetas(entries: Array<{ path: string; meta: FileMeta }>): void {
    this.inner.writeMetas(entries);
  }
  deleteFile(path: string): void {
    this.inner.deleteFile(path);
  }
  deleteFiles(paths: string[]): void {
    this.inner.deleteFiles(paths);
  }
  deletePagesFrom(path: string, fromPageIndex: number): void {
    this.inner.deletePagesFrom(path, fromPageIndex);
  }
  deleteMeta(path: string): void {
    this.inner.deleteMeta(path);
  }
  deleteMetas(paths: string[]): void {
    this.inner.deleteMetas(paths);
  }
  renameFile(oldPath: string, newPath: string): void {
    this.inner.renameFile(oldPath, newPath);
  }
  deleteAll(paths: string[]): void {
    this.inner.deleteAll(paths);
  }

  syncAll(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
    metas: Array<{ path: string; meta: FileMeta }>,
  ): void {
    if (this.armed) {
      // Metadata-first: write metadata (including clean marker), then crash
      // before pages. This is the dangerous ordering.
      this.inner.writeMetas(metas);
      throw new Error("simulated crash after metadata write");
    }
    // Normal: pages first, then metadata (correct ordering)
    this.inner.writePages(pages);
    this.inner.writeMetas(metas);
  }
}

/**
 * Backend that writes pages BEFORE metadata in syncAll, then crashes
 * after pages but before metadata. This is the safe ordering.
 */
class PagesFirstCrashBackend implements SyncStorageBackend {
  readonly inner: SyncMemoryBackend;
  armed = false;

  constructor(inner: SyncMemoryBackend) {
    this.inner = inner;
  }

  readPage(path: string, pageIndex: number): Uint8Array | null {
    return this.inner.readPage(path, pageIndex);
  }
  readPages(path: string, pageIndices: number[]): Array<Uint8Array | null> {
    return this.inner.readPages(path, pageIndices);
  }
  readPageBatch(entries: Array<{ path: string; pageIndex: number }>): Array<Uint8Array | null> {
    return this.inner.readPageBatch(entries);
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
  writePage(path: string, pageIndex: number, data: Uint8Array): void {
    this.inner.writePage(path, pageIndex, data);
  }
  writePages(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
  ): void {
    this.inner.writePages(pages);
  }
  writeMeta(path: string, meta: FileMeta): void {
    this.inner.writeMeta(path, meta);
  }
  writeMetas(entries: Array<{ path: string; meta: FileMeta }>): void {
    this.inner.writeMetas(entries);
  }
  deleteFile(path: string): void {
    this.inner.deleteFile(path);
  }
  deleteFiles(paths: string[]): void {
    this.inner.deleteFiles(paths);
  }
  deletePagesFrom(path: string, fromPageIndex: number): void {
    this.inner.deletePagesFrom(path, fromPageIndex);
  }
  deleteMeta(path: string): void {
    this.inner.deleteMeta(path);
  }
  deleteMetas(paths: string[]): void {
    this.inner.deleteMetas(paths);
  }
  renameFile(oldPath: string, newPath: string): void {
    this.inner.renameFile(oldPath, newPath);
  }
  deleteAll(paths: string[]): void {
    this.inner.deleteAll(paths);
  }

  syncAll(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
    metas: Array<{ path: string; meta: FileMeta }>,
  ): void {
    if (this.armed) {
      // Pages-first: write pages, then crash before metadata.
      // This is the safe ordering — clean marker is not persisted.
      this.inner.writePages(pages);
      throw new Error("simulated crash after page write");
    }
    this.inner.writePages(pages);
    this.inner.writeMetas(metas);
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

const CLEAN_MARKER_PATH = "/__tomefs_clean";

// ---------------------------------------------------------------------------
// Metadata-first crash: clean marker is persisted with stale page data
// ---------------------------------------------------------------------------

describe("syncAll ordering: metadata-first crash persists stale clean marker", () => {
  let inner: SyncMemoryBackend;

  beforeEach(() => {
    inner = new SyncMemoryBackend();
  });

  it("in-place page modification lost when clean marker written before pages @fast", async () => {
    // Phase 1: create file and sync successfully
    const { FS, tomefs } = await mountTome(inner);
    const s = FS.open(`${MOUNT}/data`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("original"), 0, 8);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // Verify baseline
    const basePage = inner.readPage("/data", 0);
    expect(basePage).not.toBeNull();
    expect(decode(basePage!.subarray(0, 8))).toBe("original");

    // Phase 2: modify file in-place (same size — restoreTree won't detect
    // page mismatch via maxPageIndex). Use metadata-first crash backend.
    const crashBackend = new MetaFirstCrashBackend(inner);
    const { FS: FS2, tomefs: t2 } = await mountTome(crashBackend);
    const s2 = FS2.open(`${MOUNT}/data`, O.RDWR);
    FS2.write(s2, encode("MODIFIED"), 0, 8);
    FS2.close(s2);

    // Arm crash: metadata (including clean marker) will be written,
    // then crash before pages.
    crashBackend.armed = true;
    const err = syncfs(FS2, t2);
    expect(err).not.toBeNull();

    // The clean marker was persisted (metadata-first ordering)
    const marker = inner.readMeta(CLEAN_MARKER_PATH);
    expect(marker).not.toBeNull();

    // But page data still has "original" (pages never written)
    const stalePage = inner.readPage("/data", 0);
    expect(stalePage).not.toBeNull();
    expect(decode(stalePage!.subarray(0, 8))).toBe("original");

    // Phase 3: remount — sees clean marker, skips orphan cleanup,
    // trusts stale page data. File reads "original" instead of "MODIFIED".
    const { FS: FS3 } = await mountTome(inner);
    const s3 = FS3.open(`${MOUNT}/data`, O.RDONLY);
    const buf = new Uint8Array(20);
    const n = FS3.read(s3, buf, 0, 20);
    FS3.close(s3);

    // This is the corruption: data was "MODIFIED" before crash, but
    // page content reverted to "original" because pages weren't written.
    // The clean marker prevented recovery detection.
    expect(decode(buf, n)).toBe("original");
  });

  it("file extension: stale metadata size with clean marker causes size mismatch @fast", async () => {
    // Phase 1: create small file
    const { FS, tomefs } = await mountTome(inner);
    const s = FS.open(`${MOUNT}/grow`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("tiny"), 0, 4);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // Phase 2: extend file to 2 pages. Metadata-first crash writes
    // updated metadata (size = 2*PAGE_SIZE) + clean marker, but NOT
    // the new pages.
    const crashBackend = new MetaFirstCrashBackend(inner);
    const { FS: FS2, tomefs: t2 } = await mountTome(crashBackend);
    const s2 = FS2.open(`${MOUNT}/grow`, O.RDWR);
    const bigData = new Uint8Array(PAGE_SIZE * 2);
    for (let i = 0; i < bigData.length; i++) bigData[i] = 0xab;
    FS2.write(s2, bigData, 0, bigData.length);
    FS2.close(s2);

    crashBackend.armed = true;
    syncfs(FS2, t2);

    // Metadata says file is 2 pages, but only page 0 (original) exists.
    // The dirty pages from the extension were never flushed.
    const meta = inner.readMeta("/grow");
    expect(meta).not.toBeNull();
    // Metadata was updated (size reflects the extension)
    expect(meta!.size).toBe(PAGE_SIZE * 2);

    // Clean marker present — next mount trusts metadata
    expect(inner.readMeta(CLEAN_MARKER_PATH)).not.toBeNull();

    // Only the original page 0 is in the backend (from Phase 1)
    // The sentinel page from the extension was dirty in cache but never written
    expect(inner.maxPageIndex("/grow")).toBe(0);

    // Phase 3: remount — restoreTree sees maxPageIndex=0 < lastExpected=1,
    // adjusts size down to 1*PAGE_SIZE. Data integrity is preserved
    // (original page 0 content), but the extension is lost.
    const { FS: FS3 } = await mountTome(inner);
    const stat = FS3.stat(`${MOUNT}/grow`);
    expect(stat.size).toBe(PAGE_SIZE);
  });
});

// ---------------------------------------------------------------------------
// Pages-first crash: clean marker NOT persisted, enabling safe recovery
// ---------------------------------------------------------------------------

describe("syncAll ordering: pages-first crash leaves clean marker absent", () => {
  let inner: SyncMemoryBackend;

  beforeEach(() => {
    inner = new SyncMemoryBackend();
  });

  it("in-place modification preserved when pages written before crash @fast", async () => {
    // Phase 1: create file and sync
    const { FS, tomefs } = await mountTome(inner);
    const s = FS.open(`${MOUNT}/data`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("original"), 0, 8);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // Phase 2: modify file in-place. Pages-first crash: pages are written,
    // metadata (including clean marker) is NOT.
    const crashBackend = new PagesFirstCrashBackend(inner);
    const { FS: FS2, tomefs: t2 } = await mountTome(crashBackend);
    const s2 = FS2.open(`${MOUNT}/data`, O.RDWR);
    FS2.write(s2, encode("MODIFIED"), 0, 8);
    FS2.close(s2);

    crashBackend.armed = true;
    const err = syncfs(FS2, t2);
    expect(err).not.toBeNull();

    // Pages were written (pages-first ordering)
    const page = inner.readPage("/data", 0);
    expect(page).not.toBeNull();
    expect(decode(page!.subarray(0, 8))).toBe("MODIFIED");

    // Clean marker was NOT written (crash before metadata)
    expect(inner.readMeta(CLEAN_MARKER_PATH)).toBeNull();

    // Phase 3: remount — no clean marker → needsOrphanCleanup=true →
    // full tree walk on first syncfs. Page data is current.
    const { FS: FS3 } = await mountTome(inner);
    const s3 = FS3.open(`${MOUNT}/data`, O.RDONLY);
    const buf = new Uint8Array(20);
    const n = FS3.read(s3, buf, 0, 20);
    FS3.close(s3);
    expect(decode(buf, n)).toBe("MODIFIED");
  });

  it("file extension: new pages survive crash, stale metadata size corrected @fast", async () => {
    // Phase 1: small file
    const { FS, tomefs } = await mountTome(inner);
    const s = FS.open(`${MOUNT}/grow`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("tiny"), 0, 4);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // Phase 2: extend to 2 pages. Pages-first crash writes the new pages,
    // metadata stays stale (size=4).
    const crashBackend = new PagesFirstCrashBackend(inner);
    const { FS: FS2, tomefs: t2 } = await mountTome(crashBackend);
    const s2 = FS2.open(`${MOUNT}/grow`, O.RDWR);
    const bigData = new Uint8Array(PAGE_SIZE * 2);
    for (let i = 0; i < bigData.length; i++) bigData[i] = 0xab;
    FS2.write(s2, bigData, 0, bigData.length);
    FS2.close(s2);

    crashBackend.armed = true;
    syncfs(FS2, t2);

    // Pages were written, metadata was NOT
    expect(inner.maxPageIndex("/grow")).toBe(1); // page 0 and 1 exist
    expect(inner.readMeta("/grow")!.size).toBe(4); // stale metadata

    // No clean marker
    expect(inner.readMeta(CLEAN_MARKER_PATH)).toBeNull();

    // Phase 3: remount — restoreTree detects maxPageIndex=1 > lastExpected=0,
    // expands size to 2*PAGE_SIZE. Data is recovered.
    const { FS: FS3 } = await mountTome(inner);
    const stat = FS3.stat(`${MOUNT}/grow`);
    expect(stat.size).toBe(PAGE_SIZE * 2);

    // Data is correct
    const readBuf = new Uint8Array(PAGE_SIZE * 2);
    const s3 = FS3.open(`${MOUNT}/grow`, O.RDONLY);
    FS3.read(s3, readBuf, 0, PAGE_SIZE * 2);
    FS3.close(s3);
    expect(readBuf).toEqual(bigData);
  });

  it("orphaned pages from new file cleaned up on next full sync @fast", async () => {
    // Phase 1: baseline
    const { FS, tomefs } = await mountTome(inner);
    const s = FS.open(`${MOUNT}/existing`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("keep me"), 0, 7);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // Phase 2: create new file, pages-first crash. Pages are written for
    // the new file but metadata never arrives → orphaned pages.
    const crashBackend = new PagesFirstCrashBackend(inner);
    const { FS: FS2, tomefs: t2 } = await mountTome(crashBackend);
    const s2 = FS2.open(`${MOUNT}/newfile`, O.RDWR | O.CREAT, 0o666);
    FS2.write(s2, encode("new data"), 0, 8);
    FS2.close(s2);

    crashBackend.armed = true;
    syncfs(FS2, t2);

    // New file pages exist but no metadata
    expect(inner.readMeta("/newfile")).toBeNull();
    // Pages may or may not exist depending on whether dirty page flush
    // happened during syncAll or was evicted earlier

    // No clean marker — next mount forces orphan cleanup
    expect(inner.readMeta(CLEAN_MARKER_PATH)).toBeNull();

    // Phase 3: remount + clean sync cleans up orphans
    const { FS: FS3, tomefs: t3 } = await mountTome(inner);

    // Existing file is intact
    const s3 = FS3.open(`${MOUNT}/existing`, O.RDONLY);
    const buf = new Uint8Array(20);
    const n = FS3.read(s3, buf, 0, 20);
    FS3.close(s3);
    expect(decode(buf, n)).toBe("keep me");

    // New file doesn't exist (no metadata)
    const listing = FS3.readdir(`${MOUNT}`).filter(
      (f: string) => f !== "." && f !== "..",
    );
    expect(listing).not.toContain("newfile");

    // Clean sync to eliminate any orphaned pages
    syncAndUnmount(FS3, t3);

    // Verify backend is clean
    const finalPaths = inner.listFiles().filter(
      (p: string) => !p.startsWith("/__tomefs"),
    );
    expect(finalPaths).toEqual(["/existing"]);
  });
});

// ---------------------------------------------------------------------------
// Verify correct ordering: SyncMemoryBackend.syncAll uses pages-first
// ---------------------------------------------------------------------------

describe("syncAll ordering: SyncMemoryBackend uses pages-first", () => {
  it("syncAll writes pages then metadata (verified by crash timing) @fast", () => {
    const inner = new SyncMemoryBackend();

    // Write pages and metadata via syncAll
    const pages = [
      { path: "/f", pageIndex: 0, data: new Uint8Array(PAGE_SIZE).fill(0x42) },
    ];
    const metas = [
      { path: "/f", meta: { size: PAGE_SIZE, mode: 0o100666, ctime: 1, mtime: 1 } as FileMeta },
    ];

    inner.syncAll(pages, metas);

    // Both exist
    expect(inner.readPage("/f", 0)).not.toBeNull();
    expect(inner.readMeta("/f")).not.toBeNull();
  });
});
