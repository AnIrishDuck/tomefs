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

    it("zeroTailAfterTruncate zeros tail of evicted page in backend", () => {
      // Use a 1-page cache so the target page gets evicted to backend
      const cache = new SyncPageCache(backend, 1);

      // Write a full page of 0xFF
      const data = new Uint8Array(PAGE_SIZE);
      data.fill(0xff);
      cache.write("/file", data, 0, PAGE_SIZE, 0, 0);

      // Evict the page by touching a different file
      const other = new Uint8Array(PAGE_SIZE);
      cache.write("/other", other, 0, PAGE_SIZE, 0, 0);

      // Page 0 of /file is now only in the backend, not in cache
      expect(cache.has("/file", 0)).toBe(false);

      // Truncate to 100 bytes — must zero tail even though page is not cached
      cache.zeroTailAfterTruncate("/file", 100);

      // Load the page back from backend
      const page = cache.getPage("/file", 0);
      // First 100 bytes should be 0xff
      for (let i = 0; i < 100; i++) {
        expect(page.data[i]).toBe(0xff);
      }
      // Tail should be zeroed, not stale 0xff
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

    it("evicts cached destination pages before rename @fast", () => {
      const cache = new SyncPageCache(backend, 8);
      // Write 3 pages to destination
      const destData = new Uint8Array(PAGE_SIZE * 3);
      destData.fill(0xdd);
      cache.write("/dest", destData, 0, PAGE_SIZE * 3, 0, 0);
      cache.flushFile("/dest");

      // Write 1 page to source
      const srcData = new Uint8Array(PAGE_SIZE);
      srcData.fill(0xaa);
      cache.write("/src", srcData, 0, PAGE_SIZE, 0, 0);

      cache.renameFile("/src", "/dest");

      // Destination page 0 should be source data
      const buf0 = new Uint8Array(PAGE_SIZE);
      cache.read("/dest", buf0, 0, PAGE_SIZE, 0, PAGE_SIZE);
      expect(buf0[0]).toBe(0xaa);

      // Stale destination pages should not be cached
      expect(cache.has("/dest", 1)).toBe(false);
      expect(cache.has("/dest", 2)).toBe(false);

      // Backend should also have no orphan pages
      expect(backend.readPage("/dest", 1)).toBeNull();
      expect(backend.readPage("/dest", 2)).toBeNull();
    });

    it("evicts dirty destination pages without flushing them", () => {
      const cache = new SyncPageCache(backend, 8);
      // Write dirty page to destination (not flushed)
      const destData = new Uint8Array(PAGE_SIZE);
      destData.fill(0xdd);
      cache.write("/dest", destData, 0, PAGE_SIZE, 0, 0);
      expect(cache.isDirty("/dest", 0)).toBe(true);

      // Write source page
      const srcData = new Uint8Array(PAGE_SIZE);
      srcData.fill(0xaa);
      cache.write("/src", srcData, 0, PAGE_SIZE, 0, 0);

      cache.renameFile("/src", "/dest");

      // Destination should have source data, not the dirty dest data
      const buf = new Uint8Array(PAGE_SIZE);
      cache.read("/dest", buf, 0, PAGE_SIZE, 0, PAGE_SIZE);
      expect(buf[0]).toBe(0xaa);
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

  describe("batch eviction optimization", () => {
    /**
     * Counting wrapper around SyncMemoryBackend to verify that multi-page
     * operations use batch backend calls instead of per-page calls.
     */
    function createCountingBackend() {
      const inner = new SyncMemoryBackend();
      const counts = {
        readPage: 0,
        readPages: 0,
        writePage: 0,
        writePages: 0,
        reset() {
          this.readPage = this.readPages = this.writePage = this.writePages = 0;
        },
      };

      const counting: SyncMemoryBackend & { counts: typeof counts } =
        Object.create(inner);
      counting.counts = counts;
      counting.readPage = (...args: Parameters<typeof inner.readPage>) => {
        counts.readPage++;
        return inner.readPage(...args);
      };
      counting.readPages = (...args: Parameters<typeof inner.readPages>) => {
        counts.readPages++;
        return inner.readPages(...args);
      };
      counting.writePage = (...args: Parameters<typeof inner.writePage>) => {
        counts.writePage++;
        return inner.writePage(...args);
      };
      counting.writePages = (...args: Parameters<typeof inner.writePages>) => {
        counts.writePages++;
        return inner.writePages(...args);
      };
      return counting;
    }

    it("batches dirty eviction flushes during multi-page write", () => {
      const cb = createCountingBackend();
      const cache = new SyncPageCache(cb, 4);

      // Fill cache with 4 dirty pages for file A
      const fillData = new Uint8Array(PAGE_SIZE * 4);
      for (let i = 0; i < fillData.length; i++) fillData[i] = (i * 3) & 0xff;
      cache.write("/a", fillData, 0, PAGE_SIZE * 4, 0, 0);

      cb.counts.reset();

      // Write 4 pages to file B — requires evicting all 4 dirty pages from A
      const writeData = new Uint8Array(PAGE_SIZE * 4);
      for (let i = 0; i < writeData.length; i++)
        writeData[i] = (i * 7) & 0xff;
      cache.write("/b", writeData, 0, PAGE_SIZE * 4, 0, 0);

      // Should batch the dirty eviction flush (1 writePages, not 4 writePage)
      expect(cb.counts.writePage).toBe(0);
      expect(cb.counts.writePages).toBe(1);

      // File B is new (currentFileSize=0), so all pages are beyond the file
      // extent — no backend reads needed (skip-read optimization).
      expect(cb.counts.readPages).toBe(0);
      expect(cb.counts.readPage).toBe(0);

      // Verify data integrity: read back file B
      const buf = new Uint8Array(PAGE_SIZE * 4);
      cache.read("/b", buf, 0, PAGE_SIZE * 4, 0, PAGE_SIZE * 4);
      expect(buf).toEqual(writeData);

      // Verify evicted file A data is in backend
      const evicted = cb.readPage("/a", 0);
      expect(evicted).not.toBeNull();
      expect(evicted![0]).toBe(fillData[0]);
    });

    it("batches dirty eviction flushes during multi-page read", () => {
      const cb = createCountingBackend();
      const cache = new SyncPageCache(cb, 4);

      // Fill cache with 4 dirty pages
      const fillData = new Uint8Array(PAGE_SIZE * 4);
      fillData.fill(0xab);
      cache.write("/a", fillData, 0, PAGE_SIZE * 4, 0, 0);

      // Write 4 pages of file B directly to backend (bypass cache)
      for (let i = 0; i < 4; i++) {
        const d = new Uint8Array(PAGE_SIZE);
        d[0] = i + 1;
        cb.writePage("/b", i, d);
      }

      cb.counts.reset();

      // Read all 4 pages of file B — evicts all of A's dirty pages
      const buf = new Uint8Array(PAGE_SIZE * 4);
      cache.read("/b", buf, 0, PAGE_SIZE * 4, 0, PAGE_SIZE * 4);

      // Eviction should be batched
      expect(cb.counts.writePage).toBe(0);
      expect(cb.counts.writePages).toBe(1);

      // Verify read data
      expect(buf[0]).toBe(1);
      expect(buf[PAGE_SIZE]).toBe(2);
      expect(buf[PAGE_SIZE * 2]).toBe(3);
      expect(buf[PAGE_SIZE * 3]).toBe(4);
    });

    it("uses single writePage for single dirty eviction", () => {
      const cb = createCountingBackend();
      const cache = new SyncPageCache(cb, 2);

      // Fill cache with 2 dirty pages
      cache.write("/a", new Uint8Array([1]), 0, 1, 0, 0);
      cache.write("/b", new Uint8Array([2]), 0, 1, 0, 0);

      cb.counts.reset();

      // Single page write that triggers 1 eviction
      cache.write("/c", new Uint8Array([3]), 0, 1, 0, 0);

      // Single eviction should use writePage, not writePages
      expect(cb.counts.writePage).toBe(1);
      expect(cb.counts.writePages).toBe(0);
    });

    it("data survives batch eviction and re-read from backend", () => {
      const cb = createCountingBackend();
      const cache = new SyncPageCache(cb, 4);

      // Write distinctive data to 8 pages across 2 files
      const dataA = new Uint8Array(PAGE_SIZE * 4);
      for (let i = 0; i < dataA.length; i++) dataA[i] = (i * 11) & 0xff;
      cache.write("/a", dataA, 0, PAGE_SIZE * 4, 0, 0);

      const dataB = new Uint8Array(PAGE_SIZE * 4);
      for (let i = 0; i < dataB.length; i++) dataB[i] = (i * 13) & 0xff;
      cache.write("/b", dataB, 0, PAGE_SIZE * 4, 0, 0);

      // File A was evicted when B was written. Read it back from backend.
      const bufA = new Uint8Array(PAGE_SIZE * 4);
      cache.read("/a", bufA, 0, PAGE_SIZE * 4, 0, PAGE_SIZE * 4);
      expect(bufA).toEqual(dataA);

      // File B was evicted when A was re-read. Read it back too.
      const bufB = new Uint8Array(PAGE_SIZE * 4);
      cache.read("/b", bufB, 0, PAGE_SIZE * 4, 0, PAGE_SIZE * 4);
      expect(bufB).toEqual(dataB);
    });

    it("does not over-evict when some pages are already cached", () => {
      const cb = createCountingBackend();
      const cache = new SyncPageCache(cb, 4);

      // Load 2 dirty pages for /other first (these will be LRU)
      cache.write("/other", new Uint8Array([0xaa]), 0, 1, 0, 0);
      cache.write("/other", new Uint8Array([0xbb]), 0, 1, PAGE_SIZE, 0);

      // Load pages 0 and 1 for /file (clean, from backend — MRU end)
      for (let i = 0; i < 2; i++) {
        const d = new Uint8Array(PAGE_SIZE);
        d[0] = i + 1;
        cb.writePage("/file", i, d);
      }
      cache.getPage("/file", 0);
      cache.getPage("/file", 1);

      // Cache is now full (4 pages). Write spanning pages 0-3 of /file.
      // Pages 0-1 are cached, 2-3 are misses. Only need to evict 2.
      cb.counts.reset();

      const writeData = new Uint8Array(PAGE_SIZE * 4);
      writeData.fill(0xcc);
      cache.write("/file", writeData, 0, PAGE_SIZE * 4, 0, 0);

      // Should only evict 2 pages (the /other pages), not 4
      // The 2 dirty /other pages should be batch-flushed
      expect(cb.counts.writePages).toBe(1);
      expect(cb.counts.writePage).toBe(0);

      // Verify evicted data is in backend
      const evicted = cb.readPage("/other", 0);
      expect(evicted).not.toBeNull();
      expect(evicted![0]).toBe(0xaa);
    });
  });

  describe("constructor validation", () => {
    it("rejects maxPages < 1", () => {
      expect(() => new SyncPageCache(backend, 0)).toThrow(
        "maxPages must be at least 1",
      );
    });
  });

  describe("getStats", () => {
    it("@fast starts at zero", () => {
      const cache = new SyncPageCache(backend, 4);
      const stats = cache.getStats();
      expect(stats).toEqual({ hits: 0, misses: 0, evictions: 0, flushes: 0 });
    });

    it("@fast counts cache miss on first getPage", () => {
      const cache = new SyncPageCache(backend, 4);
      cache.getPage("/file", 0);
      expect(cache.getStats().misses).toBe(1);
      expect(cache.getStats().hits).toBe(0);
    });

    it("@fast counts cache hit on repeated getPage", () => {
      const cache = new SyncPageCache(backend, 4);
      cache.getPage("/file", 0);
      cache.getPage("/file", 0);
      expect(cache.getStats().misses).toBe(1);
      expect(cache.getStats().hits).toBe(1);
    });

    it("counts MRU fast-path as a hit", () => {
      const cache = new SyncPageCache(backend, 4);
      cache.getPage("/file", 0);
      // Second call hits MRU fast path
      cache.getPage("/file", 0);
      // Third call also hits MRU fast path
      cache.getPage("/file", 0);
      expect(cache.getStats().hits).toBe(2);
    });

    it("counts eviction when cache is full", () => {
      const cache = new SyncPageCache(backend, 2);
      cache.getPage("/file", 0);
      cache.getPage("/file", 1);
      // Third page triggers eviction
      cache.getPage("/file", 2);
      expect(cache.getStats().evictions).toBe(1);
    });

    it("counts dirty flush on eviction", () => {
      const cache = new SyncPageCache(backend, 2);
      const data = new Uint8Array([1]);
      cache.write("/file", data, 0, 1, 0, 0);
      cache.getPage("/file", 1);
      // Third page evicts dirty page 0
      cache.getPage("/file", 2);
      expect(cache.getStats().flushes).toBe(1);
      expect(cache.getStats().evictions).toBe(1);
    });

    it("counts flushFile", () => {
      const cache = new SyncPageCache(backend, 4);
      const data = new Uint8Array([1, 2, 3]);
      cache.write("/file", data, 0, 3, 0, 0);
      cache.write("/file", data, 0, 3, PAGE_SIZE, 3);
      cache.flushFile("/file");
      expect(cache.getStats().flushes).toBe(2);
    });

    it("counts flushAll", () => {
      const cache = new SyncPageCache(backend, 4);
      const data = new Uint8Array([1]);
      cache.write("/a", data, 0, 1, 0, 0);
      cache.write("/b", data, 0, 1, 0, 0);
      cache.flushAll();
      expect(cache.getStats().flushes).toBe(2);
    });

    it("resetStats clears all counters", () => {
      const cache = new SyncPageCache(backend, 2);
      const data = new Uint8Array([1]);
      cache.write("/file", data, 0, 1, 0, 0);
      cache.getPage("/file", 0);
      cache.getPage("/file", 1);
      cache.getPage("/file", 2); // eviction
      cache.flushAll();
      // Verify counters are non-zero
      const before = cache.getStats();
      expect(before.hits + before.misses + before.evictions + before.flushes).toBeGreaterThan(0);

      cache.resetStats();
      expect(cache.getStats()).toEqual({ hits: 0, misses: 0, evictions: 0, flushes: 0 });
    });

    it("getStats returns a snapshot (not a live reference)", () => {
      const cache = new SyncPageCache(backend, 4);
      const snap = cache.getStats();
      cache.getPage("/file", 0);
      // Snapshot should not change
      expect(snap.misses).toBe(0);
      expect(cache.getStats().misses).toBe(1);
    });

    it("counts multi-page read misses correctly", () => {
      const cache = new SyncPageCache(backend, 8);
      // Write 3 pages of data so read spans multiple pages
      const size = PAGE_SIZE * 3;
      const data = new Uint8Array(size);
      cache.write("/file", data, 0, size, 0, 0);
      cache.flushAll();

      // Evict and re-read to force cache misses
      cache.evictFile("/file");
      cache.resetStats();

      const buf = new Uint8Array(size);
      cache.read("/file", buf, 0, size, 0, size);

      // 3 pages should be loaded (misses)
      expect(cache.getStats().misses).toBe(3);
    });

    it("multi-page read does not double-count batch-loaded pages as hits", () => {
      const cache = new SyncPageCache(backend, 8);
      // Write 3 pages of data
      const size = PAGE_SIZE * 3;
      const data = new Uint8Array(size);
      cache.write("/file", data, 0, size, 0, 0);
      cache.flushAll();

      // Evict all, then re-read all 3 pages in one call
      cache.evictFile("/file");
      cache.resetStats();

      const buf = new Uint8Array(size);
      cache.read("/file", buf, 0, size, 0, size);

      const stats = cache.getStats();
      // All 3 pages were cache misses (loaded from backend)
      expect(stats.misses).toBe(3);
      // No pages should be counted as hits — they were all loaded fresh
      expect(stats.hits).toBe(0);
    });

    it("multi-page read counts hits correctly for partially cached pages", () => {
      const cache = new SyncPageCache(backend, 8);
      // Write 3 pages of data
      const size = PAGE_SIZE * 3;
      const data = new Uint8Array(size);
      cache.write("/file", data, 0, size, 0, 0);
      cache.flushAll();

      // Evict all, then load page 1 back into cache
      cache.evictFile("/file");
      cache.getPage("/file", 1);
      cache.resetStats();

      // Read all 3 pages — page 1 is cached, pages 0 and 2 are not
      const buf = new Uint8Array(size);
      cache.read("/file", buf, 0, size, 0, size);

      const stats = cache.getStats();
      // Pages 0 and 2 are misses (batch-loaded from backend)
      expect(stats.misses).toBe(2);
      // Page 1 is a legitimate hit (was already in cache)
      expect(stats.hits).toBe(1);
    });

    it("multi-page write does not double-count batch-loaded pages as hits", () => {
      const cache = new SyncPageCache(backend, 8);
      // Pre-populate 3 pages in backend
      const size = PAGE_SIZE * 3;
      const data = new Uint8Array(size);
      cache.write("/file", data, 0, size, 0, 0);
      cache.flushAll();

      // Evict all, then write across all 3 pages
      cache.evictFile("/file");
      cache.resetStats();

      const writeData = new Uint8Array(size);
      writeData.fill(0x42);
      cache.write("/file", writeData, 0, size, 0, size);

      const stats = cache.getStats();
      // All 3 pages were cache misses (loaded from backend for read-modify-write)
      expect(stats.misses).toBe(3);
      // No pages should be counted as hits
      expect(stats.hits).toBe(0);
    });
  });

  describe("skip backend reads for pages beyond file extent", () => {
    /**
     * Counting wrapper around SyncMemoryBackend that tracks readPage
     * and readPages calls to verify the optimization.
     */
    function createCountingBackend() {
      const inner = new SyncMemoryBackend();
      let readPageCalls = 0;
      let readPagesCalls = 0;
      let readPagesIndicesTotal = 0;

      const counting: SyncMemoryBackend & {
        readPageCalls: number;
        readPagesCalls: number;
        readPagesIndicesTotal: number;
        resetCounts(): void;
      } = Object.create(inner);

      Object.defineProperty(counting, "readPageCalls", {
        get: () => readPageCalls,
      });
      Object.defineProperty(counting, "readPagesCalls", {
        get: () => readPagesCalls,
      });
      Object.defineProperty(counting, "readPagesIndicesTotal", {
        get: () => readPagesIndicesTotal,
      });
      counting.resetCounts = () => {
        readPageCalls = 0;
        readPagesCalls = 0;
        readPagesIndicesTotal = 0;
      };
      counting.readPage = (path: string, pageIndex: number) => {
        readPageCalls++;
        return inner.readPage(path, pageIndex);
      };
      counting.readPages = (path: string, pageIndices: number[]) => {
        readPagesCalls++;
        readPagesIndicesTotal += pageIndices.length;
        return inner.readPages(path, pageIndices);
      };
      return counting;
    }

    it("single-page write to new file skips backend read", () => {
      const cb = createCountingBackend();
      const cache = new SyncPageCache(cb, 8);

      // Write to a brand-new file (currentFileSize=0)
      const data = new Uint8Array([1, 2, 3]);
      cb.resetCounts();
      cache.write("/file", data, 0, 3, 0, 0);

      // No backend reads — file has no existing pages
      expect(cb.readPageCalls).toBe(0);
      expect(cb.readPagesCalls).toBe(0);
    });

    it("multi-page extending write skips backend reads for new pages", () => {
      const cb = createCountingBackend();
      const cache = new SyncPageCache(cb, 16);

      // Write initial page
      const init = new Uint8Array(PAGE_SIZE);
      init.fill(0xaa);
      cache.write("/file", init, 0, PAGE_SIZE, 0, 0);
      cache.flushAll();
      cache.evictFile("/file");

      // Now write 4 pages starting at position 0 (1 existing + 3 new)
      const data = new Uint8Array(PAGE_SIZE * 4);
      for (let i = 0; i < data.length; i++) data[i] = (i * 7) & 0xff;

      cb.resetCounts();
      cache.write("/file", data, 0, data.length, 0, PAGE_SIZE);

      // Page 0 exists but is fully overwritten (page-aligned write covers
      // entire page) → read skipped. Pages 1-3 beyond file extent → also
      // no read.
      expect(cb.readPageCalls).toBe(0);
      expect(cb.readPagesCalls).toBe(0);

      // Verify data integrity
      const buf = new Uint8Array(data.length);
      cache.read("/file", buf, 0, data.length, 0, PAGE_SIZE * 4);
      expect(buf).toEqual(data);
    });

    it("batch read only includes pages within file extent", () => {
      const cb = createCountingBackend();
      const cache = new SyncPageCache(cb, 16);

      // Write 3 pages to establish file extent
      const init = new Uint8Array(PAGE_SIZE * 3);
      init.fill(0xbb);
      cache.write("/file", init, 0, init.length, 0, 0);
      cache.flushAll();
      cache.evictFile("/file");

      // Write 5 pages starting at page 1 (2 existing + 3 new)
      const data = new Uint8Array(PAGE_SIZE * 5);
      for (let i = 0; i < data.length; i++) data[i] = (i * 11) & 0xff;

      cb.resetCounts();
      cache.write("/file", data, 0, data.length, PAGE_SIZE, PAGE_SIZE * 3);

      // Pages 1-2 exist in backend but are fully overwritten (page-aligned
      // write covers entire pages) → reads skipped by full-page-overwrite
      // optimization. Pages 3-5 are beyond file extent → also no read.
      expect(cb.readPagesIndicesTotal).toBe(0);

      // Verify data integrity: page 0 still has original data
      const fullBuf = new Uint8Array(PAGE_SIZE * 6);
      cache.read("/file", fullBuf, 0, PAGE_SIZE * 6, 0, PAGE_SIZE * 6);
      // Page 0: original 0xbb fill
      for (let i = 0; i < PAGE_SIZE; i++) {
        expect(fullBuf[i]).toBe(0xbb);
      }
      // Pages 1-5: new data
      for (let i = 0; i < data.length; i++) {
        expect(fullBuf[PAGE_SIZE + i]).toBe(data[i]);
      }
    });

    it("appending write to existing file skips backend read", () => {
      const cb = createCountingBackend();
      const cache = new SyncPageCache(cb, 8);

      // Write some initial data
      const init = new Uint8Array(100);
      init.fill(0xcc);
      cache.write("/file", init, 0, 100, 0, 0);
      cache.flushAll();
      cache.evictFile("/file");

      // Append at the end (next page boundary)
      const append = new Uint8Array(PAGE_SIZE);
      append.fill(0xdd);

      cb.resetCounts();
      cache.write("/file", append, 0, PAGE_SIZE, PAGE_SIZE, 100);

      // Page 1 is beyond the file extent (100 bytes = 1 page).
      // ceil(100/8192) = 1, so page index 1 >= firstNewPage.
      // No backend reads needed.
      expect(cb.readPageCalls).toBe(0);
      expect(cb.readPagesCalls).toBe(0);
    });

    it("write within existing extent still reads from backend", () => {
      const cb = createCountingBackend();
      const cache = new SyncPageCache(cb, 8);

      // Write 2 pages
      const init = new Uint8Array(PAGE_SIZE * 2);
      init.fill(0xee);
      cache.write("/file", init, 0, init.length, 0, 0);
      cache.flushAll();
      cache.evictFile("/file");

      // Overwrite page 0 (full-page write within existing extent)
      const overwrite = new Uint8Array(PAGE_SIZE);
      overwrite.fill(0xff);

      cb.resetCounts();
      cache.write("/file", overwrite, 0, PAGE_SIZE, 0, PAGE_SIZE * 2);

      // Page 0 exists in backend, but the write covers the entire page
      // → read skipped by full-page-overwrite optimization
      expect(cb.readPageCalls).toBe(0);
    });

    it("extending write preserves data integrity under cache pressure", () => {
      const cb = createCountingBackend();
      const cache = new SyncPageCache(cb, 4); // Tiny cache

      // Write 2 pages
      const init = new Uint8Array(PAGE_SIZE * 2);
      for (let i = 0; i < init.length; i++) init[i] = (i * 3) & 0xff;
      cache.write("/file", init, 0, init.length, 0, 0);
      cache.flushAll();

      // Extend by 8 more pages (far exceeds cache)
      const extend = new Uint8Array(PAGE_SIZE * 8);
      for (let i = 0; i < extend.length; i++)
        extend[i] = ((i + 1000) * 7) & 0xff;

      cb.resetCounts();
      cache.write("/file", extend, 0, extend.length, PAGE_SIZE * 2, PAGE_SIZE * 2);

      // Pages 2-9 are all new → no backend reads for them
      expect(cb.readPagesIndicesTotal).toBe(0);

      // Verify all data survives (original + extension)
      const total = PAGE_SIZE * 10;
      const buf = new Uint8Array(total);
      cache.read("/file", buf, 0, total, 0, total);

      // Pages 0-1: original data
      for (let i = 0; i < PAGE_SIZE * 2; i++) {
        expect(buf[i]).toBe((i * 3) & 0xff);
      }
      // Pages 2-9: extension data
      for (let i = 0; i < PAGE_SIZE * 8; i++) {
        expect(buf[PAGE_SIZE * 2 + i]).toBe(((i + 1000) * 7) & 0xff);
      }
    });
  });

  describe("getPageNoRead", () => {
    /**
     * Counting wrapper (same as above) for verifying backend reads are skipped.
     */
    function createCountingBackend() {
      const inner = new SyncMemoryBackend();
      let readPageCalls = 0;

      const counting: SyncMemoryBackend & {
        readPageCalls: number;
        resetCounts(): void;
      } = Object.create(inner);

      Object.defineProperty(counting, "readPageCalls", {
        get: () => readPageCalls,
      });
      counting.resetCounts = () => { readPageCalls = 0; };
      counting.readPage = (path: string, pageIndex: number) => {
        readPageCalls++;
        return inner.readPage(path, pageIndex);
      };
      counting.readPages = (path: string, pageIndices: number[]) => {
        readPageCalls += pageIndices.length;
        return inner.readPages(path, pageIndices);
      };
      return counting;
    }

    it("@fast creates zero-filled page without reading backend", () => {
      const cb = createCountingBackend();
      const cache = new SyncPageCache(cb, 8);

      cb.resetCounts();
      const page = cache.getPageNoRead("/file", 0);

      expect(cb.readPageCalls).toBe(0);
      expect(page.data).toEqual(new Uint8Array(PAGE_SIZE));
      expect(page.dirty).toBe(false);
    });

    it("returns cached page if already present (no backend read)", () => {
      const cb = createCountingBackend();
      const cache = new SyncPageCache(cb, 8);

      // Load page into cache via normal getPage
      const page1 = cache.getPage("/file", 0);
      page1.data[0] = 0x42;

      cb.resetCounts();
      const page2 = cache.getPageNoRead("/file", 0);

      // Should return same cached page, no backend read
      expect(cb.readPageCalls).toBe(0);
      expect(page2.data[0]).toBe(0x42);
    });

    it("page created by getPageNoRead is writable and flushable", () => {
      const cb = createCountingBackend();
      const cache = new SyncPageCache(cb, 8);

      const page = cache.getPageNoRead("/file", 0);
      page.data.set(new Uint8Array([1, 2, 3, 4, 5]));
      page.dirty = true;
      cache.addDirtyKey(page.key, "/file");

      cache.flushAll();

      // Verify data made it to backend
      const stored = cb.readPage("/file", 0);
      expect(stored).not.toBeNull();
      expect(stored![0]).toBe(1);
      expect(stored![4]).toBe(5);
    });
  });

  describe("full-page overwrite skip", () => {
    /**
     * Counting wrapper for verifying backend reads are skipped on
     * full-page overwrites.
     */
    function createCountingBackend() {
      const inner = new SyncMemoryBackend();
      let readPageCalls = 0;

      const counting: SyncMemoryBackend & {
        readPageCalls: number;
        resetCounts(): void;
      } = Object.create(inner);

      Object.defineProperty(counting, "readPageCalls", {
        get: () => readPageCalls,
      });
      counting.resetCounts = () => { readPageCalls = 0; };
      counting.readPage = (path: string, pageIndex: number) => {
        readPageCalls++;
        return inner.readPage(path, pageIndex);
      };
      counting.readPages = (path: string, pageIndices: number[]) => {
        readPageCalls += pageIndices.length;
        return inner.readPages(path, pageIndices);
      };
      return counting;
    }

    it("@fast single-page full overwrite skips backend read", () => {
      const cb = createCountingBackend();
      const cache = new SyncPageCache(cb, 8);

      // Write initial data to establish a page in the backend
      const initial = new Uint8Array(PAGE_SIZE);
      initial.fill(0xaa);
      cache.write("/file", initial, 0, PAGE_SIZE, 0, 0);
      cache.flushAll();
      cache.evictFile("/file");
      cb.resetCounts();

      // Full-page overwrite: position 0, length PAGE_SIZE
      const overwrite = new Uint8Array(PAGE_SIZE);
      overwrite.fill(0xbb);
      cache.write("/file", overwrite, 0, PAGE_SIZE, 0, PAGE_SIZE);

      // Should NOT have read from backend — entire page is overwritten
      expect(cb.readPageCalls).toBe(0);

      // Verify correct data
      const buf = new Uint8Array(PAGE_SIZE);
      cache.read("/file", buf, 0, PAGE_SIZE, 0, PAGE_SIZE);
      expect(buf[0]).toBe(0xbb);
      expect(buf[PAGE_SIZE - 1]).toBe(0xbb);
    });

    it("partial write within existing page still reads from backend", () => {
      const cb = createCountingBackend();
      const cache = new SyncPageCache(cb, 8);

      // Write initial data
      const initial = new Uint8Array(PAGE_SIZE);
      initial.fill(0xaa);
      cache.write("/file", initial, 0, PAGE_SIZE, 0, 0);
      cache.flushAll();
      cache.evictFile("/file");
      cb.resetCounts();

      // Partial write: only 100 bytes at offset 0
      const partial = new Uint8Array(100);
      partial.fill(0xcc);
      cache.write("/file", partial, 0, 100, 0, PAGE_SIZE);

      // SHOULD have read from backend — partial overwrite needs existing data
      expect(cb.readPageCalls).toBe(1);

      // Verify: first 100 bytes overwritten, rest preserved
      const buf = new Uint8Array(PAGE_SIZE);
      cache.read("/file", buf, 0, PAGE_SIZE, 0, PAGE_SIZE);
      expect(buf[0]).toBe(0xcc);
      expect(buf[99]).toBe(0xcc);
      expect(buf[100]).toBe(0xaa);
    });

    it("multi-page write skips reads for fully-overwritten middle pages", () => {
      const cb = createCountingBackend();
      const cache = new SyncPageCache(cb, 16);

      // Write 4 pages of initial data
      const initial = new Uint8Array(PAGE_SIZE * 4);
      initial.fill(0xaa);
      cache.write("/file", initial, 0, initial.length, 0, 0);
      cache.flushAll();
      cache.evictFile("/file");
      cb.resetCounts();

      // Write spanning pages 0-3, starting at offset 10 in page 0
      // Page 0: partial (starts at offset 10) — needs read
      // Pages 1-2: fully overwritten (middle pages) — skip read
      // Page 3: partial (doesn't fill to end) — needs read
      const writeSize = PAGE_SIZE * 4 - 20; // 10 bytes short on each end
      const overwrite = new Uint8Array(writeSize);
      overwrite.fill(0xdd);
      cache.write("/file", overwrite, 0, writeSize, 10, PAGE_SIZE * 4);

      // Only pages 0 and 3 should be read (partial overwrites)
      expect(cb.readPageCalls).toBe(2);
    });

    it("@fast page-aligned multi-page write skips all backend reads", () => {
      const cb = createCountingBackend();
      const cache = new SyncPageCache(cb, 16);

      // Write 3 pages of initial data
      const initial = new Uint8Array(PAGE_SIZE * 3);
      initial.fill(0xaa);
      cache.write("/file", initial, 0, initial.length, 0, 0);
      cache.flushAll();
      cache.evictFile("/file");
      cb.resetCounts();

      // Page-aligned overwrite of all 3 pages
      const overwrite = new Uint8Array(PAGE_SIZE * 3);
      overwrite.fill(0xee);
      cache.write("/file", overwrite, 0, overwrite.length, 0, PAGE_SIZE * 3);

      // No backend reads needed — all pages fully overwritten
      expect(cb.readPageCalls).toBe(0);

      // Verify data integrity
      const buf = new Uint8Array(PAGE_SIZE * 3);
      cache.read("/file", buf, 0, buf.length, 0, PAGE_SIZE * 3);
      for (let i = 0; i < buf.length; i++) {
        expect(buf[i]).toBe(0xee);
      }
    });

    it("full-page overwrite data persists through flush and re-read", () => {
      const cb = createCountingBackend();
      const cache = new SyncPageCache(cb, 8);

      // Establish a file with known data
      const initial = new Uint8Array(PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) initial[i] = i & 0xff;
      cache.write("/file", initial, 0, PAGE_SIZE, 0, 0);
      cache.flushAll();
      cache.evictFile("/file");

      // Full-page overwrite (skips read)
      const overwrite = new Uint8Array(PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) overwrite[i] = (i * 3) & 0xff;
      cache.write("/file", overwrite, 0, PAGE_SIZE, 0, PAGE_SIZE);

      // Flush and evict to force round-trip through backend
      cache.flushAll();
      cache.evictFile("/file");

      // Re-read and verify
      const buf = new Uint8Array(PAGE_SIZE);
      cache.read("/file", buf, 0, PAGE_SIZE, 0, PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) {
        expect(buf[i]).toBe((i * 3) & 0xff);
      }
    });
  });
});
