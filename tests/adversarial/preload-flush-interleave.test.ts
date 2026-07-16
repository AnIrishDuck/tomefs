/**
 * Adversarial tests for PreloadBackend flush interleaving.
 *
 * Verifies that sync mutations (writePage, deleteFile, deletePagesFrom,
 * renameFile, etc.) that occur DURING an async flush() are not lost.
 *
 * The flush() method yields to the event loop at each await point. If
 * sync methods are called between those yields (e.g., from a microtask
 * or in a concurrent flush scenario), their tracking entries (dirty pages,
 * truncations, deletedFiles, deletedMeta) must survive into the next
 * flush cycle — not be silently cleared by the in-progress flush.
 *
 * Previously, flush() used .clear() on truncations/deletedFiles/deletedMeta
 * after processing, which would wipe entries added during the async gap.
 * The fix snapshots these sets before any async work and only removes
 * the snapshotted entries afterward.
 */

import { describe, it, expect } from "vitest";
import { PreloadBackend } from "../../src/preload-backend.js";
import { PAGE_SIZE } from "../../src/types.js";
import type { StorageBackend } from "../../src/storage-backend.js";
import type { FileMeta } from "../../src/types.js";

/**
 * A StorageBackend wrapper that calls hooks during async operations,
 * allowing us to inject sync mutations while an async flush is in
 * progress.
 */
class InterceptingBackend implements StorageBackend {
  private inner: StorageBackend;
  onDeletePagesFrom: (() => void) | null = null;
  onSyncAll: (() => void) | null = null;

  constructor(inner: StorageBackend) {
    this.inner = inner;
  }

  async readPage(path: string, pageIndex: number) {
    return this.inner.readPage(path, pageIndex);
  }
  async readPages(path: string, pageIndices: number[]) {
    return this.inner.readPages(path, pageIndices);
  }
  async readPageBatch(entries: Array<{ path: string; pageIndex: number }>): Promise<Array<Uint8Array | null>> {
    return this.inner.readPageBatch(entries);
  }
  async writePage(path: string, pageIndex: number, data: Uint8Array) {
    return this.inner.writePage(path, pageIndex, data);
  }
  async writePages(pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>) {
    return this.inner.writePages(pages);
  }
  async deleteFile(path: string) {
    return this.inner.deleteFile(path);
  }
  async deleteFiles(paths: string[]) {
    return this.inner.deleteFiles(paths);
  }
  async deletePagesFrom(path: string, fromPageIndex: number) {
    this.onDeletePagesFrom?.();
    return this.inner.deletePagesFrom(path, fromPageIndex);
  }
  async renameFile(oldPath: string, newPath: string) {
    return this.inner.renameFile(oldPath, newPath);
  }
  async countPages(path: string) {
    return this.inner.countPages(path);
  }
  async countPagesBatch(paths: string[]) {
    return this.inner.countPagesBatch(paths);
  }
  async maxPageIndex(path: string) {
    return this.inner.maxPageIndex(path);
  }
  async maxPageIndexBatch(paths: string[]) {
    return this.inner.maxPageIndexBatch(paths);
  }
  async readMeta(path: string) {
    return this.inner.readMeta(path);
  }
  async readMetas(paths: string[]) {
    return this.inner.readMetas(paths);
  }
  async writeMeta(path: string, meta: FileMeta) {
    return this.inner.writeMeta(path, meta);
  }
  async writeMetas(entries: Array<{ path: string; meta: FileMeta }>) {
    return this.inner.writeMetas(entries);
  }
  async deleteMeta(path: string) {
    return this.inner.deleteMeta(path);
  }
  async deleteMetas(paths: string[]) {
    return this.inner.deleteMetas(paths);
  }
  async listFiles() {
    return this.inner.listFiles();
  }
  async syncAll(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
    metas: Array<{ path: string; meta: FileMeta }>,
  ) {
    this.onSyncAll?.();
    return this.inner.syncAll(pages, metas);
  }
  async deleteAll(paths: string[]) {
    return this.inner.deleteAll(paths);
  }
  async cleanupOrphanedPages(): Promise<number> {
    return (this.inner as any).cleanupOrphanedPages();
  }
}

function testPage(value: number): Uint8Array {
  const buf = new Uint8Array(PAGE_SIZE);
  buf.fill(value);
  return buf;
}

function testMeta(size: number): FileMeta {
  return { size, mode: 0o100644, ctime: 1000, mtime: 2000 };
}

describe("PreloadBackend flush interleave safety", () => {
  it("truncation added during flush is preserved @fast", async () => {
    const { MemoryBackend } = await import("../../src/memory-backend.js");
    const interceptor = new InterceptingBackend(new MemoryBackend());
    const preload = new PreloadBackend(interceptor);
    await preload.init();

    // Write pages 0-3 for /a, with metadata and a pending truncation
    for (let i = 0; i < 4; i++) {
      preload.writePage("/a", i, testPage(i + 1));
    }
    preload.writeMeta("/a", testMeta(4 * PAGE_SIZE));
    preload.deletePagesFrom("/a", 2); // truncation at page 2

    // Also delete a file so flush takes the complex path
    preload.writePage("/b", 0, testPage(10));
    preload.writeMeta("/b", testMeta(PAGE_SIZE));
    preload.deleteFile("/b");

    // During the flush's deletePagesFrom call for /a, inject a NEW
    // truncation for /c
    let injected = false;
    interceptor.onDeletePagesFrom = () => {
      if (!injected) {
        injected = true;
        preload.writePage("/c", 0, testPage(20));
        preload.writePage("/c", 1, testPage(21));
        preload.writePage("/c", 2, testPage(22));
        preload.writeMeta("/c", testMeta(3 * PAGE_SIZE));
        preload.deletePagesFrom("/c", 1);
      }
    };

    await preload.flush();

    // The /c truncation should still be pending
    expect(preload.isDirty).toBe(true);

    // Second flush processes /c's data + truncation
    interceptor.onDeletePagesFrom = null;
    await preload.flush();

    // Verify: reload from remote and check /c has only page 0
    const fresh = new PreloadBackend(interceptor);
    await fresh.init();
    expect(fresh.readPage("/c", 0)).not.toBeNull();
    expect(fresh.readPage("/c", 1)).toBeNull();
    expect(fresh.readPage("/c", 2)).toBeNull();
  });

  it("deleteAll added during flush is preserved @fast", async () => {
    const { MemoryBackend } = await import("../../src/memory-backend.js");
    const interceptor = new InterceptingBackend(new MemoryBackend());
    const preload = new PreloadBackend(interceptor);
    await preload.init();

    // Pre-populate /d in remote
    preload.writePage("/d", 0, testPage(40));
    preload.writeMeta("/d", testMeta(PAGE_SIZE));
    await preload.flush();

    // Set up a complex flush: create /a and delete it
    preload.writePage("/a", 0, testPage(2));
    preload.writeMeta("/a", testMeta(PAGE_SIZE));
    preload.deleteFile("/a");

    // During the early syncAll, inject a deleteAll for /d
    let injected = false;
    interceptor.onSyncAll = () => {
      if (!injected) {
        injected = true;
        preload.deleteAll(["/d"]);
      }
    };

    await preload.flush();

    // /d should still be pending deletion (both pages and metadata)
    expect(preload.isDirty).toBe(true);

    interceptor.onSyncAll = null;
    await preload.flush();

    // Verify: /d should be fully gone from remote
    const fresh = new PreloadBackend(interceptor);
    await fresh.init();
    expect(fresh.readPage("/d", 0)).toBeNull();
    expect(fresh.listFiles()).not.toContain("/d");
  });

  it("dirty pages added during flush are preserved @fast", async () => {
    const { MemoryBackend } = await import("../../src/memory-backend.js");
    const interceptor = new InterceptingBackend(new MemoryBackend());
    const preload = new PreloadBackend(interceptor);
    await preload.init();

    // Trigger complex path with a deletion
    preload.writePage("/g", 0, testPage(70));
    preload.writeMeta("/g", testMeta(PAGE_SIZE));
    preload.deleteFile("/g");

    // During flush's syncAll, inject new dirty writes
    let injected = false;
    interceptor.onSyncAll = () => {
      if (!injected) {
        injected = true;
        preload.writePage("/h", 0, testPage(80));
        preload.writeMeta("/h", testMeta(PAGE_SIZE));
      }
    };

    await preload.flush();

    // /h should still be dirty
    expect(preload.isDirty).toBe(true);

    interceptor.onSyncAll = null;
    await preload.flush();

    // Verify: /h should exist in remote
    const fresh = new PreloadBackend(interceptor);
    await fresh.init();
    const page = fresh.readPage("/h", 0);
    expect(page).not.toBeNull();
    expect(page![0]).toBe(80);
  });

  it("rename during flush does not lose tracking @fast", async () => {
    const { MemoryBackend } = await import("../../src/memory-backend.js");
    const interceptor = new InterceptingBackend(new MemoryBackend());
    const preload = new PreloadBackend(interceptor);
    await preload.init();

    // Pre-populate /src in remote
    preload.writePage("/src", 0, testPage(50));
    preload.writePage("/src", 1, testPage(51));
    preload.writeMeta("/src", testMeta(2 * PAGE_SIZE));
    await preload.flush();

    // Trigger complex path
    preload.writePage("/tmp", 0, testPage(99));
    preload.writeMeta("/tmp", testMeta(PAGE_SIZE));
    preload.deleteFile("/tmp");

    // During flush, rename /src → /dst (including metadata update,
    // which tomefs would normally handle)
    let injected = false;
    interceptor.onSyncAll = () => {
      if (!injected) {
        injected = true;
        preload.renameFile("/src", "/dst");
        preload.deleteMeta("/src");
        preload.writeMeta("/dst", testMeta(2 * PAGE_SIZE));
      }
    };

    await preload.flush();

    // The rename should have created pending deletedFiles + dirty pages
    expect(preload.isDirty).toBe(true);

    interceptor.onSyncAll = null;
    await preload.flush();

    // Verify round-trip
    const fresh = new PreloadBackend(interceptor);
    await fresh.init();
    expect(fresh.readPage("/src", 0)).toBeNull();
    expect(fresh.readPage("/dst", 0)).not.toBeNull();
    expect(fresh.readPage("/dst", 0)![0]).toBe(50);
    expect(fresh.readPage("/dst", 1)).not.toBeNull();
    expect(fresh.readPage("/dst", 1)![0]).toBe(51);
  });

  it("mixed mutations during flush all survive @fast", async () => {
    const { MemoryBackend } = await import("../../src/memory-backend.js");
    const interceptor = new InterceptingBackend(new MemoryBackend());
    const preload = new PreloadBackend(interceptor);
    await preload.init();

    // Pre-populate several files
    for (let i = 0; i < 5; i++) {
      preload.writePage(`/f${i}`, 0, testPage(i));
      preload.writePage(`/f${i}`, 1, testPage(i + 10));
      preload.writeMeta(`/f${i}`, testMeta(2 * PAGE_SIZE));
    }
    await preload.flush();

    // Trigger complex path
    preload.deleteFile("/f0");
    preload.deletePagesFrom("/f1", 1);

    // During flush, inject a mix of operations
    let injected = false;
    interceptor.onDeletePagesFrom = () => {
      if (!injected) {
        injected = true;
        preload.renameFile("/f2", "/f2_moved");
        preload.deleteMeta("/f2");
        preload.writeMeta("/f2_moved", testMeta(2 * PAGE_SIZE));
        preload.writePage("/new", 0, testPage(99));
        preload.writeMeta("/new", testMeta(PAGE_SIZE));
        preload.deletePagesFrom("/f3", 1);
      }
    };

    await preload.flush();
    preload.assertInvariants();

    // Flush remaining interleaved mutations
    interceptor.onDeletePagesFrom = null;
    await preload.flush();
    preload.assertInvariants();

    // Final verification via round-trip
    const fresh = new PreloadBackend(interceptor);
    await fresh.init();
    fresh.assertInvariants();

    // /f0 pages deleted (metadata survives since deleteFile doesn't
    // track meta deletion — only deleteAll does)
    expect(fresh.readPage("/f0", 0)).toBeNull();

    // /f1 truncated at page 1
    expect(fresh.readPage("/f1", 0)).not.toBeNull();
    expect(fresh.readPage("/f1", 1)).toBeNull();

    // /f2 renamed to /f2_moved
    expect(fresh.readPage("/f2", 0)).toBeNull();
    expect(fresh.readPage("/f2_moved", 0)).not.toBeNull();

    // new file created
    expect(fresh.readPage("/new", 0)).not.toBeNull();
    expect(fresh.readPage("/new", 0)![0]).toBe(99);

    // /f3 truncated at page 1
    expect(fresh.readPage("/f3", 0)).not.toBeNull();
    expect(fresh.readPage("/f3", 1)).toBeNull();

    // /f4 untouched
    expect(fresh.readPage("/f4", 0)).not.toBeNull();
    expect(fresh.readPage("/f4", 1)).not.toBeNull();
  });
});
