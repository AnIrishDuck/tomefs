/**
 * Unit tests for SyncPageCache — synchronous LRU page cache.
 *
 * Validates bounded-memory operation: LRU eviction, dirty flush on eviction,
 * multi-page I/O, and correct behavior under eviction pressure.
 */

import { describe, it, expect, beforeEach } from "vitest";
import { SyncPageCache } from "../../src/sync-page-cache.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { PAGE_SIZE } from "../../src/types.js";

describe("SyncPageCache", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  describe("basic operations", () => {
    it("@fast reads zeros from non-existent page", () => {
      const cache = new SyncPageCache(backend, 4);
      const page = cache.getPage("/file", 0);
      expect(page.data).toEqual(new Uint8Array(PAGE_SIZE));
      expect(page.dirty).toBe(false);
    });

    it("write and read back data within a single page", () => {
      const cache = new SyncPageCache(backend, 4);
      const data = new Uint8Array([1, 2, 3, 4, 5]);
      const result = cache.write("/file", data, 0, 5, 0, 0);
      expect(result.bytesWritten).toBe(5);
      expect(result.newFileSize).toBe(5);

      const buf = new Uint8Array(5);
      const n = cache.read("/file", buf, 0, 5, 0, 5);
      expect(n).toBe(5);
      expect(buf).toEqual(data);
    });

    it("write and read spanning multiple pages", () => {
      const cache = new SyncPageCache(backend, 8);
      const size = PAGE_SIZE * 2 + 100;
      const data = new Uint8Array(size);
      for (let i = 0; i < size; i++) data[i] = i & 0xff;

      cache.write("/file", data, 0, size, 0, 0);

      const buf = new Uint8Array(size);
      cache.read("/file", buf, 0, size, 0, size);
      expect(buf).toEqual(data);
    });

    it("read clamps to file size", () => {
      const cache = new SyncPageCache(backend, 4);
      const data = new Uint8Array([10, 20, 30]);
      cache.write("/file", data, 0, 3, 0, 0);

      const buf = new Uint8Array(10);
      const n = cache.read("/file", buf, 0, 10, 0, 3);
      expect(n).toBe(3);
      expect(buf.subarray(0, 3)).toEqual(data);
    });

    it("zero-length write returns zero", () => {
      const cache = new SyncPageCache(backend, 4);
      const result = cache.write("/file", new Uint8Array(0), 0, 0, 0, 0);
      expect(result.bytesWritten).toBe(0);
      expect(result.newFileSize).toBe(0);
    });
  });

  describe("LRU eviction", () => {
    it("@fast evicts LRU page when cache is full", () => {
      const cache = new SyncPageCache(backend, 2);

      // Load 3 pages into a 2-page cache
      cache.getPage("/file", 0);
      cache.getPage("/file", 1);
      expect(cache.size).toBe(2);

      cache.getPage("/file", 2);
      expect(cache.size).toBe(2);
      // Page 0 should be evicted (LRU)
      expect(cache.has("/file", 0)).toBe(false);
      expect(cache.has("/file", 1)).toBe(true);
      expect(cache.has("/file", 2)).toBe(true);
    });

    it("accessing a page refreshes its LRU position", () => {
      const cache = new SyncPageCache(backend, 2);

      cache.getPage("/file", 0);
      cache.getPage("/file", 1);
      // Touch page 0 to refresh it
      cache.getPage("/file", 0);
      // Now page 1 is LRU
      cache.getPage("/file", 2);
      expect(cache.has("/file", 0)).toBe(true);
      expect(cache.has("/file", 1)).toBe(false);
      expect(cache.has("/file", 2)).toBe(true);
    });

    it("dirty pages are flushed to backend before eviction", () => {
      const cache = new SyncPageCache(backend, 1);
      const data = new Uint8Array([42]);

      // Write to page 0 (marks dirty)
      cache.write("/file", data, 0, 1, 0, 0);
      expect(cache.isDirty("/file", 0)).toBe(true);

      // Load page 1, evicting page 0 (flush first)
      cache.getPage("/file", 1);

      // Page 0 should be in backend
      const stored = backend.readPage("/file", 0);
      expect(stored).not.toBeNull();
      expect(stored![0]).toBe(42);
    });

    it("@fast data survives eviction + re-read from backend", () => {
      const cache = new SyncPageCache(backend, 1);

      // Write 100 bytes to page 0
      const data = new Uint8Array(100);
      for (let i = 0; i < 100; i++) data[i] = i;
      cache.write("/file", data, 0, 100, 0, 0);

      // Evict page 0 by loading page 1
      cache.getPage("/other", 0);
      expect(cache.has("/file", 0)).toBe(false);

      // Re-read: should come from backend
      const buf = new Uint8Array(100);
      cache.read("/file", buf, 0, 100, 0, 100);
      expect(buf).toEqual(data);
    });

    it("multi-page write under eviction pressure", () => {
      // 4-page cache, write 8 pages
      const cache = new SyncPageCache(backend, 4);
      const size = PAGE_SIZE * 8;
      const data = new Uint8Array(size);
      for (let i = 0; i < size; i++) data[i] = (i * 7) & 0xff;

      cache.write("/file", data, 0, size, 0, 0);

      // Read it all back — earlier pages must come from backend
      const buf = new Uint8Array(size);
      cache.read("/file", buf, 0, size, 0, size);
      expect(buf).toEqual(data);
    });
  });

  describe("flush", () => {
    it("flushFile writes dirty pages to backend", () => {
      const cache = new SyncPageCache(backend, 4);
      cache.write("/a", new Uint8Array([1]), 0, 1, 0, 0);
      cache.write("/b", new Uint8Array([2]), 0, 1, 0, 0);

      cache.flushFile("/a");
      expect(cache.isDirty("/a", 0)).toBe(false);
      expect(cache.isDirty("/b", 0)).toBe(true);

      const stored = backend.readPage("/a", 0);
      expect(stored![0]).toBe(1);
    });

    it("flushAll writes all dirty pages", () => {
      const cache = new SyncPageCache(backend, 4);
      cache.write("/a", new Uint8Array([1]), 0, 1, 0, 0);
      cache.write("/b", new Uint8Array([2]), 0, 1, 0, 0);

      const count = cache.flushAll();
      expect(count).toBe(2);
      expect(cache.dirtyCount).toBe(0);
    });
  });

  describe("truncation", () => {
    it("invalidatePagesFrom removes pages beyond threshold", () => {
      const cache = new SyncPageCache(backend, 8);
      // Write 3 pages
      for (let i = 0; i < 3; i++) {
        cache.getPage("/file", i);
      }
      expect(cache.has("/file", 2)).toBe(true);

      cache.invalidatePagesFrom("/file", 1);
      expect(cache.has("/file", 0)).toBe(true);
      expect(cache.has("/file", 1)).toBe(false);
      expect(cache.has("/file", 2)).toBe(false);
    });

    it("zeroTailAfterTruncate zeros tail of last page", () => {
      const cache = new SyncPageCache(backend, 4);
      const data = new Uint8Array(PAGE_SIZE);
      data.fill(0xff);
      cache.write("/file", data, 0, PAGE_SIZE, 0, 0);

      // Truncate to 100 bytes
      cache.zeroTailAfterTruncate("/file", 100);
      const page = cache.getPage("/file", 0);
      // First 100 bytes should be 0xff, rest should be 0
      for (let i = 0; i < 100; i++) {
        expect(page.data[i]).toBe(0xff);
      }
      for (let i = 100; i < PAGE_SIZE; i++) {
        expect(page.data[i]).toBe(0);
      }
    });
  });

  describe("deleteFile", () => {
    it("removes pages from cache and backend", () => {
      const cache = new SyncPageCache(backend, 4);
      cache.write("/file", new Uint8Array([1, 2, 3]), 0, 3, 0, 0);
      cache.flushFile("/file");

      cache.deleteFile("/file");
      expect(cache.has("/file", 0)).toBe(false);
      expect(backend.readPage("/file", 0)).toBeNull();
    });
  });

  describe("renameFile", () => {
    it("@fast moves pages from old path to new path", () => {
      const cache = new SyncPageCache(backend, 4);
      const data = new Uint8Array([10, 20, 30]);
      cache.write("/old", data, 0, 3, 0, 0);

      cache.renameFile("/old", "/new");

      // Old path should be gone
      expect(cache.has("/old", 0)).toBe(false);
      expect(backend.readPage("/old", 0)).toBeNull();

      // New path should have the data
      const buf = new Uint8Array(3);
      cache.read("/new", buf, 0, 3, 0, 3);
      expect(buf).toEqual(data);
    });

    it("rename preserves data that was evicted to backend", () => {
      const cache = new SyncPageCache(backend, 1);
      // Write page 0, then evict it
      const data = new Uint8Array([42]);
      cache.write("/old", data, 0, 1, 0, 0);
      cache.getPage("/other", 0); // evict /old page 0

      cache.renameFile("/old", "/new");

      const buf = new Uint8Array(1);
      cache.read("/new", buf, 0, 1, 0, 1);
      expect(buf[0]).toBe(42);
    });
  });

  describe("batch readPages optimization", () => {
    it("uses readPages for multi-page reads with multiple cache misses", () => {
      // Write 4 pages to backend directly
      for (let i = 0; i < 4; i++) {
        const data = new Uint8Array(PAGE_SIZE);
        data[0] = i + 1;
        backend.writePage("/file", i, data);
      }

      // Track readPages calls
      let readPagesCallCount = 0;
      const origReadPages = backend.readPages.bind(backend);
      backend.readPages = (path: string, pageIndices: number[]) => {
        readPagesCallCount++;
        return origReadPages(path, pageIndices);
      };

      const cache = new SyncPageCache(backend, 16);
      const buf = new Uint8Array(PAGE_SIZE * 4);
      cache.read("/file", buf, 0, PAGE_SIZE * 4, 0, PAGE_SIZE * 4);

      // Should have used readPages (1 batch call instead of 4 individual reads)
      expect(readPagesCallCount).toBe(1);
      // Verify data integrity
      expect(buf[0]).toBe(1);
      expect(buf[PAGE_SIZE]).toBe(2);
      expect(buf[PAGE_SIZE * 2]).toBe(3);
      expect(buf[PAGE_SIZE * 3]).toBe(4);
    });

    it("skips batch for single-page reads", () => {
      const data = new Uint8Array(PAGE_SIZE);
      data[0] = 42;
      backend.writePage("/file", 0, data);

      let readPagesCallCount = 0;
      const origReadPages = backend.readPages.bind(backend);
      backend.readPages = (path: string, pageIndices: number[]) => {
        readPagesCallCount++;
        return origReadPages(path, pageIndices);
      };

      const cache = new SyncPageCache(backend, 16);
      const buf = new Uint8Array(100);
      cache.read("/file", buf, 0, 100, 0, PAGE_SIZE);

      // Single-page read should NOT use readPages
      expect(readPagesCallCount).toBe(0);
      expect(buf[0]).toBe(42);
    });

    it("skips batch when only one page is a cache miss", () => {
      // Write 2 pages
      for (let i = 0; i < 2; i++) {
        const data = new Uint8Array(PAGE_SIZE);
        data[0] = i + 1;
        backend.writePage("/file", i, data);
      }

      const cache = new SyncPageCache(backend, 16);

      // Pre-load page 0 into cache
      cache.getPage("/file", 0);

      let readPagesCallCount = 0;
      const origReadPages = backend.readPages.bind(backend);
      backend.readPages = (path: string, pageIndices: number[]) => {
        readPagesCallCount++;
        return origReadPages(path, pageIndices);
      };

      // Read spanning pages 0-1; only page 1 is a miss
      const buf = new Uint8Array(PAGE_SIZE * 2);
      cache.read("/file", buf, 0, PAGE_SIZE * 2, 0, PAGE_SIZE * 2);

      // Only 1 miss — should fall through to getPage, not readPages
      expect(readPagesCallCount).toBe(0);
      expect(buf[0]).toBe(1);
      expect(buf[PAGE_SIZE]).toBe(2);
    });

    it("handles mix of existing and non-existing pages in batch", () => {
      // Only write pages 0 and 2; page 1 doesn't exist in backend
      const d0 = new Uint8Array(PAGE_SIZE);
      d0[0] = 0xaa;
      backend.writePage("/file", 0, d0);
      const d2 = new Uint8Array(PAGE_SIZE);
      d2[0] = 0xcc;
      backend.writePage("/file", 2, d2);

      const cache = new SyncPageCache(backend, 16);
      const buf = new Uint8Array(PAGE_SIZE * 3);
      cache.read("/file", buf, 0, PAGE_SIZE * 3, 0, PAGE_SIZE * 3);

      expect(buf[0]).toBe(0xaa);
      expect(buf[PAGE_SIZE]).toBe(0); // non-existent page → zeros
      expect(buf[PAGE_SIZE * 2]).toBe(0xcc);
    });
  });

  describe("constructor validation", () => {
    it("rejects maxPages < 1", () => {
      expect(() => new SyncPageCache(backend, 0)).toThrow(
        "maxPages must be at least 1",
      );
    });
  });
});
