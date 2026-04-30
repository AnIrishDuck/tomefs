/**
 * Adversarial tests: PreloadBackend init() failure and retry.
 *
 * PreloadBackend.init() loads all metadata and pages from a remote async
 * backend into memory. If the remote fails mid-init (transient IDB error,
 * quota exceeded, tab close during load), init() rejects and clears the
 * cached promise so the caller can retry.
 *
 * Before the fix, doInit() did not clear in-memory state before retrying.
 * A partial first attempt left stale pages and secondary indices for files
 * that loaded successfully. If the remote state changed between attempts
 * (e.g., another tab deleted a file, or storage pressure evicted entries),
 * the stale data persisted — invisible to listFiles() (no metadata) but
 * occupying memory and polluting secondary indices.
 *
 * These tests verify:
 *   1. Retry after failure produces clean state (no stale leftovers)
 *   2. Remote state changes between attempts are respected
 *   3. Concurrent init() calls share the same promise
 *   4. Successful init after failure is fully functional
 *
 * Ethos §9 (adversarial — target failure recovery seams)
 * Ethos §10 (graceful degradation — PreloadBackend is the no-SAB path)
 */

import { describe, it, expect } from "vitest";
import { PreloadBackend } from "../../src/preload-backend.js";
import { PAGE_SIZE, pageKeyStr } from "../../src/types.js";
import type { StorageBackend } from "../../src/storage-backend.js";
import type { FileMeta } from "../../src/types.js";
import { MemoryBackend } from "../../src/memory-backend.js";

// ---------------------------------------------------------------
// Failing backend: wraps MemoryBackend, injects transient failures
// ---------------------------------------------------------------

/**
 * StorageBackend that fails on readPages for a specific file path.
 * After disarming, operations succeed normally. This models transient
 * IDB/OPFS errors (quota pressure, lock contention, network hiccup).
 */
class TransientFailureBackend implements StorageBackend {
  readonly inner: MemoryBackend;
  private failPath: string | null = null;
  private failOp: string | null = null;

  constructor(inner: MemoryBackend) {
    this.inner = inner;
  }

  /** Arm: readPages calls for `path` will throw. */
  failReadPagesFor(path: string): void {
    this.failPath = path;
    this.failOp = "readPages";
  }

  /** Arm: listFiles() will throw. */
  failListFiles(): void {
    this.failOp = "listFiles";
  }

  /** Arm: readMetas() will throw. */
  failReadMetas(): void {
    this.failOp = "readMetas";
  }

  /** Disarm all failures. */
  disarm(): void {
    this.failPath = null;
    this.failOp = null;
  }

  async listFiles(): Promise<string[]> {
    if (this.failOp === "listFiles") {
      throw new Error("TransientFailure: listFiles");
    }
    return this.inner.listFiles();
  }

  async readPage(path: string, pageIndex: number) {
    return this.inner.readPage(path, pageIndex);
  }

  async readPages(path: string, pageIndices: number[]) {
    if (this.failOp === "readPages" && path === this.failPath) {
      throw new Error(`TransientFailure: readPages for ${path}`);
    }
    return this.inner.readPages(path, pageIndices);
  }

  async readMeta(path: string) { return this.inner.readMeta(path); }
  async readMetas(paths: string[]) {
    if (this.failOp === "readMetas") {
      throw new Error("TransientFailure: readMetas");
    }
    return this.inner.readMetas(paths);
  }

  async writePage(path: string, pageIndex: number, data: Uint8Array) {
    return this.inner.writePage(path, pageIndex, data);
  }
  async writePages(pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>) {
    return this.inner.writePages(pages);
  }
  async deleteFile(path: string) { return this.inner.deleteFile(path); }
  async deleteFiles(paths: string[]) { return this.inner.deleteFiles(paths); }
  async deletePagesFrom(path: string, fromPageIndex: number) {
    return this.inner.deletePagesFrom(path, fromPageIndex);
  }
  async renameFile(oldPath: string, newPath: string) {
    return this.inner.renameFile(oldPath, newPath);
  }
  async countPages(path: string) { return this.inner.countPages(path); }
  async countPagesBatch(paths: string[]) { return this.inner.countPagesBatch(paths); }
  async maxPageIndex(path: string) { return this.inner.maxPageIndex(path); }
  async maxPageIndexBatch(paths: string[]) { return this.inner.maxPageIndexBatch(paths); }
  async writeMeta(path: string, meta: FileMeta) { return this.inner.writeMeta(path, meta); }
  async writeMetas(entries: Array<{ path: string; meta: FileMeta }>) {
    return this.inner.writeMetas(entries);
  }
  async deleteMeta(path: string) { return this.inner.deleteMeta(path); }
  async deleteMetas(paths: string[]) { return this.inner.deleteMetas(paths); }
  async deleteAll(paths: string[]) { return this.inner.deleteAll(paths); }
  async syncAll(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
    metas: Array<{ path: string; meta: FileMeta }>,
  ) {
    return this.inner.syncAll(pages, metas);
  }
}

// ---------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------

function makePage(fill: number): Uint8Array {
  const data = new Uint8Array(PAGE_SIZE);
  data.fill(fill);
  return data;
}

const META: FileMeta = {
  size: PAGE_SIZE,
  mode: 0o100644,
  ctime: 1000,
  mtime: 1000,
};

async function seedRemote(
  remote: MemoryBackend,
  files: Array<{ path: string; fill: number; pages?: number }>,
): Promise<void> {
  for (const { path, fill, pages: pageCount = 1 } of files) {
    for (let i = 0; i < pageCount; i++) {
      await remote.writePage(path, i, makePage(fill + i));
    }
    await remote.writeMeta(path, {
      ...META,
      size: PAGE_SIZE * pageCount,
    });
  }
}

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

describe("PreloadBackend init() recovery", () => {
  it("@fast retry after readPages failure produces clean state", async () => {
    const inner = new MemoryBackend();
    await seedRemote(inner, [
      { path: "/a", fill: 0xaa },
      { path: "/b", fill: 0xbb },
      { path: "/c", fill: 0xcc },
    ]);
    const failingBackend = new TransientFailureBackend(inner);

    // First attempt: fail on /b's pages
    failingBackend.failReadPagesFor("/b");
    const preload = new PreloadBackend(failingBackend);
    await expect(preload.init()).rejects.toThrow("TransientFailure");

    // Disarm and retry
    failingBackend.disarm();
    await preload.init();

    // All three files should be loaded correctly
    expect(preload.listFiles().sort()).toEqual(["/a", "/b", "/c"]);
    expect(preload.readPage("/a", 0)![0]).toBe(0xaa);
    expect(preload.readPage("/b", 0)![0]).toBe(0xbb);
    expect(preload.readPage("/c", 0)![0]).toBe(0xcc);
  });

  it("@fast retry clears stale pages from first attempt", async () => {
    const inner = new MemoryBackend();
    await seedRemote(inner, [
      { path: "/a", fill: 0xaa },
      { path: "/b", fill: 0xbb },
      { path: "/c", fill: 0xcc },
    ]);
    const failingBackend = new TransientFailureBackend(inner);

    // First attempt: /a and /b load successfully, /c fails
    // (loadFilePages runs in parallel via Promise.all, but /c's failure
    // rejects the whole batch — /a and /b's pages are already in memory)
    failingBackend.failReadPagesFor("/c");
    const preload = new PreloadBackend(failingBackend);
    await expect(preload.init()).rejects.toThrow("TransientFailure");

    // Between attempts: delete /b from the remote
    await inner.deleteFile("/b");
    await inner.deleteMeta("/b");

    // Retry
    failingBackend.disarm();
    await preload.init();

    // /b should NOT be present (deleted from remote between attempts)
    expect(preload.listFiles().sort()).toEqual(["/a", "/c"]);
    expect(preload.readPage("/b", 0)).toBeNull();

    // /a and /c should have correct data
    expect(preload.readPage("/a", 0)![0]).toBe(0xaa);
    expect(preload.readPage("/c", 0)![0]).toBe(0xcc);
  });

  it("@fast retry after listFiles failure works", async () => {
    const inner = new MemoryBackend();
    await seedRemote(inner, [{ path: "/x", fill: 0x42 }]);
    const failingBackend = new TransientFailureBackend(inner);

    failingBackend.failListFiles();
    const preload = new PreloadBackend(failingBackend);
    await expect(preload.init()).rejects.toThrow("TransientFailure");

    failingBackend.disarm();
    await preload.init();

    expect(preload.listFiles()).toEqual(["/x"]);
    expect(preload.readPage("/x", 0)![0]).toBe(0x42);
  });

  it("@fast retry after readMetas failure works", async () => {
    const inner = new MemoryBackend();
    await seedRemote(inner, [
      { path: "/p", fill: 0x11 },
      { path: "/q", fill: 0x22 },
    ]);
    const failingBackend = new TransientFailureBackend(inner);

    failingBackend.failReadMetas();
    const preload = new PreloadBackend(failingBackend);
    await expect(preload.init()).rejects.toThrow("TransientFailure");

    failingBackend.disarm();
    await preload.init();

    expect(preload.listFiles().sort()).toEqual(["/p", "/q"]);
    const meta = preload.readMeta("/p");
    expect(meta).not.toBeNull();
    expect(meta!.size).toBe(PAGE_SIZE);
  });

  it("concurrent init() calls share the same promise @fast", async () => {
    const inner = new MemoryBackend();
    await seedRemote(inner, [{ path: "/f", fill: 0x99 }]);
    const preload = new PreloadBackend(inner);

    // Launch three concurrent init() calls
    const p1 = preload.init();
    const p2 = preload.init();
    const p3 = preload.init();

    // All should resolve (same underlying promise)
    await Promise.all([p1, p2, p3]);

    expect(preload.listFiles()).toEqual(["/f"]);
  });

  it("concurrent init() calls all reject on failure @fast", async () => {
    const inner = new MemoryBackend();
    await seedRemote(inner, [{ path: "/f", fill: 0x99 }]);
    const failingBackend = new TransientFailureBackend(inner);

    failingBackend.failReadPagesFor("/f");
    const preload = new PreloadBackend(failingBackend);

    const p1 = preload.init();
    const p2 = preload.init();

    await expect(p1).rejects.toThrow("TransientFailure");
    await expect(p2).rejects.toThrow("TransientFailure");

    // Retry should work
    failingBackend.disarm();
    await preload.init();
    expect(preload.readPage("/f", 0)![0]).toBe(0x99);
  });

  it("retry does not leak secondary indices from first attempt @fast", async () => {
    const inner = new MemoryBackend();
    await seedRemote(inner, [
      { path: "/keep", fill: 0x11, pages: 3 },
      { path: "/remove", fill: 0x22, pages: 2 },
      { path: "/fail", fill: 0x33 },
    ]);
    const failingBackend = new TransientFailureBackend(inner);

    // Fail on /fail — /keep and /remove pages are loaded
    failingBackend.failReadPagesFor("/fail");
    const preload = new PreloadBackend(failingBackend);
    await expect(preload.init()).rejects.toThrow("TransientFailure");

    // Delete /remove from remote
    await inner.deleteFile("/remove");
    await inner.deleteMeta("/remove");

    // Retry
    failingBackend.disarm();
    await preload.init();

    // countPages and maxPageIndex should reflect clean state
    expect(preload.countPages("/keep")).toBe(3);
    expect(preload.countPages("/remove")).toBe(0);
    expect(preload.countPages("/fail")).toBe(1);

    expect(preload.maxPageIndex("/keep")).toBe(2);
    expect(preload.maxPageIndex("/remove")).toBe(-1);
    expect(preload.maxPageIndex("/fail")).toBe(0);
  });

  it("retry after failure with modified remote data loads fresh data", async () => {
    const inner = new MemoryBackend();
    await seedRemote(inner, [
      { path: "/data", fill: 0xaa, pages: 2 },
      { path: "/crash", fill: 0xff },
    ]);
    const failingBackend = new TransientFailureBackend(inner);

    // First attempt: /data loads (with 0xaa), /crash fails
    failingBackend.failReadPagesFor("/crash");
    const preload = new PreloadBackend(failingBackend);
    await expect(preload.init()).rejects.toThrow("TransientFailure");

    // Between attempts: overwrite /data with different content
    await inner.writePage("/data", 0, makePage(0xdd));
    await inner.writePage("/data", 1, makePage(0xee));

    // Retry — should load the NEW data, not stale data from first attempt
    failingBackend.disarm();
    await preload.init();

    expect(preload.readPage("/data", 0)![0]).toBe(0xdd);
    expect(preload.readPage("/data", 1)![0]).toBe(0xee);
    expect(preload.readPage("/crash", 0)![0]).toBe(0xff);
  });

  it("successful init is fully functional for read/write/flush @fast", async () => {
    const inner = new MemoryBackend();
    await seedRemote(inner, [{ path: "/file", fill: 0x42, pages: 2 }]);
    const failingBackend = new TransientFailureBackend(inner);

    // Fail, then succeed
    failingBackend.failReadPagesFor("/file");
    const preload = new PreloadBackend(failingBackend);
    await expect(preload.init()).rejects.toThrow("TransientFailure");

    failingBackend.disarm();
    await preload.init();

    // Read existing data
    expect(preload.readPage("/file", 0)![0]).toBe(0x42);
    expect(preload.readPage("/file", 1)![0]).toBe(0x43);

    // Write new data
    preload.writePage("/file", 2, makePage(0x99));
    expect(preload.readPage("/file", 2)![0]).toBe(0x99);
    expect(preload.isDirty).toBe(true);

    // Flush to remote
    await preload.flush();
    expect(preload.isDirty).toBe(false);

    // Verify flush persisted to remote
    const remotePage = await inner.readPage("/file", 2);
    expect(remotePage).not.toBeNull();
    expect(remotePage![0]).toBe(0x99);
  });

  it("double init after success is idempotent @fast", async () => {
    const inner = new MemoryBackend();
    await seedRemote(inner, [{ path: "/f", fill: 0x11 }]);
    const preload = new PreloadBackend(inner);

    await preload.init();
    preload.writePage("/f", 1, makePage(0x22));

    // Second init() should be a no-op (already initialized)
    await preload.init();

    // Written data should still be there
    expect(preload.readPage("/f", 1)![0]).toBe(0x22);
  });

  it("retry clears dirty tracking from failed attempt", async () => {
    const inner = new MemoryBackend();
    await seedRemote(inner, [
      { path: "/a", fill: 0xaa },
      { path: "/b", fill: 0xbb },
    ]);
    const failingBackend = new TransientFailureBackend(inner);

    // Fail on first attempt
    failingBackend.failReadPagesFor("/b");
    const preload = new PreloadBackend(failingBackend);
    await expect(preload.init()).rejects.toThrow("TransientFailure");

    // Succeed on retry
    failingBackend.disarm();
    await preload.init();

    // Backend should report no dirty state (init doesn't create dirty entries)
    expect(preload.isDirty).toBe(false);
    expect(preload.dirtyPageCount).toBe(0);
    expect(preload.dirtyMetaCount).toBe(0);
  });

  it("new file added to remote between attempts is loaded on retry", async () => {
    const inner = new MemoryBackend();
    await seedRemote(inner, [
      { path: "/existing", fill: 0x11 },
      { path: "/fail", fill: 0xff },
    ]);
    const failingBackend = new TransientFailureBackend(inner);

    // First attempt fails
    failingBackend.failReadPagesFor("/fail");
    const preload = new PreloadBackend(failingBackend);
    await expect(preload.init()).rejects.toThrow("TransientFailure");

    // Add a new file to remote between attempts
    await inner.writePage("/new-file", 0, makePage(0x77));
    await inner.writeMeta("/new-file", { ...META, size: PAGE_SIZE });

    // Retry
    failingBackend.disarm();
    await preload.init();

    // New file should be loaded
    expect(preload.listFiles().sort()).toEqual([
      "/existing",
      "/fail",
      "/new-file",
    ]);
    expect(preload.readPage("/new-file", 0)![0]).toBe(0x77);
  });
});
