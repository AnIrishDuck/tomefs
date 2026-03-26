/**
 * Tests for error handling in page cache eviction, rename, and SAB bridge.
 *
 * Uses a FailingSyncBackend fake that can inject errors at specific points
 * to verify the cache preserves data integrity under backend failures.
 */

import { describe, it, expect, beforeEach } from "vitest";
import { SyncPageCache } from "../../src/sync-page-cache.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { PageCache } from "../../src/page-cache.js";
import { MemoryBackend } from "../../src/memory-backend.js";
import { PAGE_SIZE } from "../../src/types.js";
import type { SyncStorageBackend } from "../../src/sync-storage-backend.js";
import type { StorageBackend } from "../../src/storage-backend.js";
import type { FileMeta } from "../../src/types.js";

/**
 * Sync backend fake that wraps SyncMemoryBackend and can inject write failures.
 */
class FailingSyncBackend implements SyncStorageBackend {
  private inner = new SyncMemoryBackend();
  writePageFails = false;
  writePagesFails = false;
  renameFileFails = false;
  writeFailCount = 0;

  readPage(path: string, pageIndex: number): Uint8Array | null {
    return this.inner.readPage(path, pageIndex);
  }

  readPages(path: string, pageIndices: number[]): Array<Uint8Array | null> {
    return this.inner.readPages(path, pageIndices);
  }

  writePage(path: string, pageIndex: number, data: Uint8Array): void {
    if (this.writePageFails) {
      this.writeFailCount++;
      throw new Error("injected writePage failure");
    }
    this.inner.writePage(path, pageIndex, data);
  }

  writePages(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
  ): void {
    if (this.writePagesFails) {
      this.writeFailCount++;
      throw new Error("injected writePages failure");
    }
    this.inner.writePages(pages);
  }

  deleteFile(path: string): void {
    this.inner.deleteFile(path);
  }

  deletePagesFrom(path: string, fromPageIndex: number): void {
    this.inner.deletePagesFrom(path, fromPageIndex);
  }

  renameFile(oldPath: string, newPath: string): void {
    if (this.renameFileFails) {
      this.writeFailCount++;
      throw new Error("injected renameFile failure");
    }
    this.inner.renameFile(oldPath, newPath);
  }

  readMeta(path: string): FileMeta | null {
    return this.inner.readMeta(path);
  }

  writeMeta(path: string, meta: FileMeta): void {
    this.inner.writeMeta(path, meta);
  }

  writeMetas(entries: Array<{ path: string; meta: FileMeta }>): void {
    this.inner.writeMetas(entries);
  }

  deleteMeta(path: string): void {
    this.inner.deleteMeta(path);
  }

  deleteMetas(paths: string[]): void {
    this.inner.deleteMetas(paths);
  }

  listFiles(): string[] {
    return this.inner.listFiles();
  }
}

/**
 * Async backend fake that wraps MemoryBackend and can inject write failures.
 */
class FailingAsyncBackend implements StorageBackend {
  private inner = new MemoryBackend();
  writePageFails = false;
  writeFailCount = 0;

  async readPage(path: string, pageIndex: number): Promise<Uint8Array | null> {
    return this.inner.readPage(path, pageIndex);
  }

  async readPages(
    path: string,
    pageIndices: number[],
  ): Promise<Array<Uint8Array | null>> {
    return this.inner.readPages(path, pageIndices);
  }

  async writePage(
    path: string,
    pageIndex: number,
    data: Uint8Array,
  ): Promise<void> {
    if (this.writePageFails) {
      this.writeFailCount++;
      throw new Error("injected writePage failure");
    }
    return this.inner.writePage(path, pageIndex, data);
  }

  async writePages(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
  ): Promise<void> {
    return this.inner.writePages(pages);
  }

  async deleteFile(path: string): Promise<void> {
    return this.inner.deleteFile(path);
  }

  async renameFile(oldPath: string, newPath: string): Promise<void> {
    return this.inner.renameFile(oldPath, newPath);
  }

  async deletePagesFrom(
    path: string,
    fromPageIndex: number,
  ): Promise<void> {
    return this.inner.deletePagesFrom(path, fromPageIndex);
  }

  async readMeta(path: string): Promise<FileMeta | null> {
    return this.inner.readMeta(path);
  }

  async writeMeta(path: string, meta: FileMeta): Promise<void> {
    return this.inner.writeMeta(path, meta);
  }

  async writeMetas(
    entries: Array<{ path: string; meta: FileMeta }>,
  ): Promise<void> {
    return this.inner.writeMetas(entries);
  }

  async deleteMeta(path: string): Promise<void> {
    return this.inner.deleteMeta(path);
  }

  async deleteMetas(paths: string[]): Promise<void> {
    return this.inner.deleteMetas(paths);
  }

  async listFiles(): Promise<string[]> {
    return this.inner.listFiles();
  }
}

describe("SyncPageCache error handling", () => {
  describe("ensureCapacity on backend write failure", () => {
    it("propagates error when evicting dirty page and backend fails", () => {
      const backend = new FailingSyncBackend();
      const cache = new SyncPageCache(backend, 1);

      // Write to page 0 (dirty, cached)
      cache.write("/file", new Uint8Array([42]), 0, 1, 0, 0);
      expect(cache.isDirty("/file", 0)).toBe(true);

      // Make backend fail on writePage
      backend.writePageFails = true;

      // Loading a new page should try to evict the dirty page, fail, and propagate
      expect(() => cache.getPage("/other", 0)).toThrow(
        "injected writePage failure",
      );

      // The dirty page should still be in the cache (not lost)
      expect(cache.has("/file", 0)).toBe(true);
      expect(cache.isDirty("/file", 0)).toBe(true);

      // Recovery: disable failures and retry — should succeed now
      backend.writePageFails = false;
      const page = cache.getPage("/other", 0);
      expect(page).toBeDefined();
      // Original data should have been flushed to backend during successful eviction
      const stored = backend.readPage("/file", 0);
      expect(stored).not.toBeNull();
      expect(stored![0]).toBe(42);
    });

    it("evicts clean pages without calling backend", () => {
      const backend = new FailingSyncBackend();
      const cache = new SyncPageCache(backend, 1);

      // Load a clean page
      cache.getPage("/file", 0);
      expect(cache.isDirty("/file", 0)).toBe(false);

      // Even with write failures, evicting a clean page shouldn't fail
      backend.writePageFails = true;
      const page = cache.getPage("/other", 0);
      expect(page).toBeDefined();
      expect(cache.has("/file", 0)).toBe(false);
      expect(backend.writeFailCount).toBe(0);
    });
  });

  describe("renameFile error handling", () => {
    it("preserves old data in backend if renameFile fails", () => {
      const backend = new FailingSyncBackend();
      const cache = new SyncPageCache(backend, 4);

      // Write data to /old and flush to backend
      cache.write("/old", new Uint8Array([1, 2, 3]), 0, 3, 0, 0);
      cache.flushFile("/old");

      // Make renameFile fail
      backend.renameFileFails = true;

      expect(() => cache.renameFile("/old", "/new")).toThrow(
        "injected renameFile failure",
      );

      // Old data should still be in the backend (rename failed, no delete)
      const stored = backend.readPage("/old", 0);
      expect(stored).not.toBeNull();
      expect(stored![0]).toBe(1);
      expect(stored![1]).toBe(2);
      expect(stored![2]).toBe(3);
    });
  });
});

describe("PageCache (async) error handling", () => {
  describe("ensureCapacity on backend write failure", () => {
    it("propagates error when evicting dirty page and backend fails", async () => {
      const backend = new FailingAsyncBackend();
      const cache = new PageCache(backend, 1);

      // Write to page 0 (dirty, cached)
      await cache.write("/file", new Uint8Array([42]), 0, 1, 0, 0);
      expect(cache.isDirty("/file", 0)).toBe(true);

      // Make backend fail on writePage
      backend.writePageFails = true;

      // Loading a new page should try to evict the dirty page, fail, and propagate
      await expect(cache.getPage("/other", 0)).rejects.toThrow(
        "injected writePage failure",
      );

      // The dirty page should still be in the cache (not lost)
      expect(cache.has("/file", 0)).toBe(true);
      expect(cache.isDirty("/file", 0)).toBe(true);

      // Recovery: disable failures and retry
      backend.writePageFails = false;
      const page = await cache.getPage("/other", 0);
      expect(page).toBeDefined();
      const stored = await backend.readPage("/file", 0);
      expect(stored).not.toBeNull();
      expect(stored![0]).toBe(42);
    });
  });
});
