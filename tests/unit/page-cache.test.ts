/**
 * Unit tests for PageCache.
 *
 * Tests LRU eviction, dirty tracking, read/write spanning pages,
 * flush, and invalidation using MemoryBackend.
 */
import { describe, it, expect, beforeEach } from "vitest";
import { PageCache } from "../../src/page-cache.js";
import { MemoryBackend } from "../../src/memory-backend.js";
import { PAGE_SIZE } from "../../src/types.js";

function fillBuf(length: number, value: number): Uint8Array {
  const buf = new Uint8Array(length);
  buf.fill(value);
  return buf;
}

describe("PageCache", () => {
  let backend: MemoryBackend;
  let cache: PageCache;

  beforeEach(() => {
    backend = new MemoryBackend();
    cache = new PageCache(backend);
  });

  describe("constructor", () => {
    it("rejects maxPages < 1", () => {
      expect(() => new PageCache(backend, 0)).toThrow("maxPages must be at least 1");
    });

    it("reports capacity", () => {
      const c = new PageCache(backend, 16);
      expect(c.capacity).toBe(16);
    });
  });

  describe("getPage", () => {
    it("@fast loads a zero-filled page for new file", async () => {
      const page = await cache.getPage("/test", 0);
      expect(page.data.length).toBe(PAGE_SIZE);
      expect(page.data.every((b) => b === 0)).toBe(true);
      expect(page.dirty).toBe(false);
    });

    it("@fast loads page from backend on cache miss", async () => {
      const data = new Uint8Array(PAGE_SIZE);
      data[0] = 0xab;
      await backend.writePage("/test", 0, data);

      const page = await cache.getPage("/test", 0);
      expect(page.data[0]).toBe(0xab);
      expect(page.dirty).toBe(false);
    });

    it("returns cached page on hit without backend read", async () => {
      const data = new Uint8Array(PAGE_SIZE);
      data[0] = 1;
      await backend.writePage("/test", 0, data);

      const page1 = await cache.getPage("/test", 0);
      // Modify backend — cache should not re-read
      const data2 = new Uint8Array(PAGE_SIZE);
      data2[0] = 99;
      await backend.writePage("/test", 0, data2);

      const page2 = await cache.getPage("/test", 0);
      expect(page2.data[0]).toBe(1); // still cached value
    });

    it("tracks cache size", async () => {
      expect(cache.size).toBe(0);
      await cache.getPage("/a", 0);
      expect(cache.size).toBe(1);
      await cache.getPage("/a", 1);
      expect(cache.size).toBe(2);
      // Re-access should not increase size
      await cache.getPage("/a", 0);
      expect(cache.size).toBe(2);
    });
  });

  describe("LRU eviction", () => {
    it("@fast evicts LRU page when at capacity", async () => {
      const small = new PageCache(backend, 2);

      await small.getPage("/a", 0);
      await small.getPage("/a", 1);
      expect(small.size).toBe(2);

      // Access page 2 — should evict page 0
      await small.getPage("/a", 2);
      expect(small.size).toBe(2);
      expect(small.has("/a", 0)).toBe(false);
      expect(small.has("/a", 1)).toBe(true);
      expect(small.has("/a", 2)).toBe(true);
    });

    it("access refreshes LRU order", async () => {
      const small = new PageCache(backend, 2);

      await small.getPage("/a", 0);
      await small.getPage("/a", 1);

      // Re-access page 0 — now page 1 is LRU
      await small.getPage("/a", 0);

      // Eviction should remove page 1
      await small.getPage("/a", 2);
      expect(small.has("/a", 0)).toBe(true);
      expect(small.has("/a", 1)).toBe(false);
      expect(small.has("/a", 2)).toBe(true);
    });

    it("accesses below capacity preserve LRU order for later eviction @fast", async () => {
      const cache = new PageCache(backend, 4);

      // Fill cache to 3/4 capacity (below maxPages)
      await cache.getPage("/a", 0);
      await cache.getPage("/a", 1);
      await cache.getPage("/a", 2);

      // Re-access page 0 while below capacity — must update LRU position
      await cache.getPage("/a", 0);

      // Fill to capacity
      await cache.getPage("/a", 3);
      expect(cache.size).toBe(4);

      // Trigger eviction — page 1 should be evicted (true LRU), not page 0
      await cache.getPage("/a", 4);
      expect(cache.has("/a", 0)).toBe(true); // accessed after page 1
      expect(cache.has("/a", 1)).toBe(false); // oldest access = LRU victim
      expect(cache.has("/a", 2)).toBe(true);
      expect(cache.has("/a", 3)).toBe(true);
      expect(cache.has("/a", 4)).toBe(true);
    });

    it("flushes dirty page before eviction", async () => {
      const small = new PageCache(backend, 1);

      // Write to page, making it dirty
      const buf = fillBuf(5, 0xaa);
      await small.write("/test", buf, 0, 5, 0, 0);
      expect(small.isDirty("/test", 0)).toBe(true);

      // Force eviction by loading a different page
      await small.getPage("/other", 0);

      // Dirty page should have been flushed to backend
      const stored = await backend.readPage("/test", 0);
      expect(stored).not.toBeNull();
      expect(stored![0]).toBe(0xaa);
      expect(stored![4]).toBe(0xaa);
    });
  });

  describe("read", () => {
    it("@fast reads within a single page", async () => {
      // Pre-populate backend
      const pageData = new Uint8Array(PAGE_SIZE);
      pageData.set([72, 101, 108, 108, 111]); // "Hello"
      await backend.writePage("/test", 0, pageData);

      const buf = new Uint8Array(5);
      const n = await cache.read("/test", buf, 0, 5, 0, 5);
      expect(n).toBe(5);
      expect(Array.from(buf)).toEqual([72, 101, 108, 108, 111]);
    });

    it("clamps read to file size", async () => {
      const pageData = new Uint8Array(PAGE_SIZE);
      pageData.set([1, 2, 3]);
      await backend.writePage("/test", 0, pageData);

      const buf = new Uint8Array(10);
      const n = await cache.read("/test", buf, 0, 10, 0, 3);
      expect(n).toBe(3);
      expect(Array.from(buf.subarray(0, 3))).toEqual([1, 2, 3]);
    });

    it("returns 0 when reading past end of file", async () => {
      const buf = new Uint8Array(10);
      const n = await cache.read("/test", buf, 0, 10, 100, 50);
      expect(n).toBe(0);
    });

    it("reads across page boundaries", async () => {
      // Write data spanning pages 0 and 1
      const page0 = new Uint8Array(PAGE_SIZE);
      page0[PAGE_SIZE - 2] = 0xaa;
      page0[PAGE_SIZE - 1] = 0xbb;
      const page1 = new Uint8Array(PAGE_SIZE);
      page1[0] = 0xcc;
      page1[1] = 0xdd;

      await backend.writePage("/test", 0, page0);
      await backend.writePage("/test", 1, page1);

      const buf = new Uint8Array(4);
      const n = await cache.read(
        "/test",
        buf,
        0,
        4,
        PAGE_SIZE - 2,
        PAGE_SIZE + 2,
      );
      expect(n).toBe(4);
      expect(Array.from(buf)).toEqual([0xaa, 0xbb, 0xcc, 0xdd]);
    });

    it("reads with buffer offset", async () => {
      const pageData = new Uint8Array(PAGE_SIZE);
      pageData.set([1, 2, 3]);
      await backend.writePage("/test", 0, pageData);

      const buf = new Uint8Array(10);
      buf.fill(0xff);
      const n = await cache.read("/test", buf, 5, 3, 0, 3);
      expect(n).toBe(3);
      expect(buf[4]).toBe(0xff);
      expect(buf[5]).toBe(1);
      expect(buf[6]).toBe(2);
      expect(buf[7]).toBe(3);
      expect(buf[8]).toBe(0xff);
    });
  });

  describe("write", () => {
    it("@fast writes within a single page", async () => {
      const buf = new Uint8Array([72, 101, 108, 108, 111]);
      const result = await cache.write("/test", buf, 0, 5, 0, 0);

      expect(result.bytesWritten).toBe(5);
      expect(result.newFileSize).toBe(5);

      // Verify data in cache
      const page = await cache.getPage("/test", 0);
      expect(Array.from(page.data.subarray(0, 5))).toEqual([
        72, 101, 108, 108, 111,
      ]);
      expect(page.dirty).toBe(true);
    });

    it("writes across page boundaries", async () => {
      const pos = PAGE_SIZE - 2;
      const buf = new Uint8Array([0xaa, 0xbb, 0xcc, 0xdd]);
      const result = await cache.write("/test", buf, 0, 4, pos, 0);

      expect(result.bytesWritten).toBe(4);
      expect(result.newFileSize).toBe(pos + 4);

      const page0 = await cache.getPage("/test", 0);
      expect(page0.data[PAGE_SIZE - 2]).toBe(0xaa);
      expect(page0.data[PAGE_SIZE - 1]).toBe(0xbb);

      const page1 = await cache.getPage("/test", 1);
      expect(page1.data[0]).toBe(0xcc);
      expect(page1.data[1]).toBe(0xdd);
    });

    it("extends file size", async () => {
      const buf = fillBuf(10, 0x42);
      const r1 = await cache.write("/test", buf, 0, 10, 0, 0);
      expect(r1.newFileSize).toBe(10);

      const r2 = await cache.write("/test", buf, 0, 10, 100, r1.newFileSize);
      expect(r2.newFileSize).toBe(110);
    });

    it("does not shrink file size", async () => {
      const buf = fillBuf(10, 0x42);
      await cache.write("/test", buf, 0, 10, 100, 0);

      // Write at position 0 should not shrink file
      const r = await cache.write("/test", buf, 0, 5, 0, 110);
      expect(r.newFileSize).toBe(110);
    });

    it("zero-length write is a no-op", async () => {
      const buf = new Uint8Array(0);
      const r = await cache.write("/test", buf, 0, 0, 0, 50);
      expect(r.bytesWritten).toBe(0);
      expect(r.newFileSize).toBe(50);
      expect(cache.size).toBe(0);
    });

    it("writes with buffer offset", async () => {
      const buf = new Uint8Array([0, 0, 1, 2, 3, 0, 0]);
      await cache.write("/test", buf, 2, 3, 0, 0);

      const page = await cache.getPage("/test", 0);
      expect(Array.from(page.data.subarray(0, 3))).toEqual([1, 2, 3]);
    });
  });

  describe("flush", () => {
    it("@fast flushes dirty pages to backend", async () => {
      await cache.write("/test", fillBuf(5, 0x42), 0, 5, 0, 0);
      expect(cache.isDirty("/test", 0)).toBe(true);

      const count = await cache.flushFile("/test");
      expect(count).toBe(1);
      expect(cache.isDirty("/test", 0)).toBe(false);

      // Verify backend has the data
      const stored = await backend.readPage("/test", 0);
      expect(stored![0]).toBe(0x42);
    });

    it("flushAll flushes all files", async () => {
      await cache.write("/a", fillBuf(5, 1), 0, 5, 0, 0);
      await cache.write("/b", fillBuf(5, 2), 0, 5, 0, 0);
      expect(cache.dirtyCount).toBe(2);

      const count = await cache.flushAll();
      expect(count).toBe(2);
      expect(cache.dirtyCount).toBe(0);

      expect((await backend.readPage("/a", 0))![0]).toBe(1);
      expect((await backend.readPage("/b", 0))![0]).toBe(2);
    });

    it("flush is idempotent", async () => {
      await cache.write("/test", fillBuf(5, 0x42), 0, 5, 0, 0);
      await cache.flushFile("/test");
      const count = await cache.flushFile("/test");
      expect(count).toBe(0);
    });

    it("only flushes pages for the specified file", async () => {
      await cache.write("/a", fillBuf(5, 1), 0, 5, 0, 0);
      await cache.write("/b", fillBuf(5, 2), 0, 5, 0, 0);

      await cache.flushFile("/a");
      expect(cache.isDirty("/a", 0)).toBe(false);
      expect(cache.isDirty("/b", 0)).toBe(true);
    });

    it("addDirtyKey registers a page as dirty without page lookup", async () => {
      // Load page into cache (clean)
      const page = await cache.getPage("/test", 0);
      expect(page.dirty).toBe(false);
      expect(cache.isDirty("/test", 0)).toBe(false);

      // Simulate external mutation: mark dirty on the page object,
      // then register via addDirtyKey
      page.dirty = true;
      cache.addDirtyKey(page.key, "/test");
      expect(cache.isDirty("/test", 0)).toBe(true);
      expect(cache.dirtyCount).toBe(1);

      // flushFile should write it to the backend
      await cache.flushFile("/test");
      expect(cache.dirtyCount).toBe(0);
    });
  });

  describe("evictFile", () => {
    it("removes all cached pages for a file", async () => {
      await cache.getPage("/a", 0);
      await cache.getPage("/a", 1);
      await cache.getPage("/b", 0);
      expect(cache.size).toBe(3);

      await cache.evictFile("/a");
      expect(cache.size).toBe(1);
      expect(cache.has("/a", 0)).toBe(false);
      expect(cache.has("/a", 1)).toBe(false);
      expect(cache.has("/b", 0)).toBe(true);
    });

    it("flushes dirty pages before evicting", async () => {
      await cache.write("/test", fillBuf(5, 0xab), 0, 5, 0, 0);
      await cache.evictFile("/test");

      expect(cache.has("/test", 0)).toBe(false);
      const stored = await backend.readPage("/test", 0);
      expect(stored![0]).toBe(0xab);
    });
  });

  describe("invalidatePagesFrom", () => {
    it("removes pages at and beyond index from cache", async () => {
      await cache.getPage("/test", 0);
      await cache.getPage("/test", 1);
      await cache.getPage("/test", 2);
      await cache.getPage("/test", 3);

      cache.invalidatePagesFrom("/test", 2);
      expect(cache.has("/test", 0)).toBe(true);
      expect(cache.has("/test", 1)).toBe(true);
      expect(cache.has("/test", 2)).toBe(false);
      expect(cache.has("/test", 3)).toBe(false);
    });

    it("does not affect other files", async () => {
      await cache.getPage("/a", 0);
      await cache.getPage("/b", 0);

      cache.invalidatePagesFrom("/a", 0);
      expect(cache.has("/a", 0)).toBe(false);
      expect(cache.has("/b", 0)).toBe(true);
    });
  });

  describe("zeroTailAfterTruncate", () => {
    it("zeros bytes after new size within last page", async () => {
      await cache.write("/test", fillBuf(PAGE_SIZE, 0xff), 0, PAGE_SIZE, 0, 0);

      await cache.zeroTailAfterTruncate("/test", 10);

      const page = await cache.getPage("/test", 0);
      // First 10 bytes unchanged
      expect(page.data[9]).toBe(0xff);
      // Everything after is zeroed
      expect(page.data[10]).toBe(0);
      expect(page.data[PAGE_SIZE - 1]).toBe(0);
      expect(page.dirty).toBe(true);
    });

    it("no-op when new size is page-aligned", async () => {
      await cache.write("/test", fillBuf(PAGE_SIZE, 0xff), 0, PAGE_SIZE, 0, 0);
      await cache.flushFile("/test");

      await cache.zeroTailAfterTruncate("/test", PAGE_SIZE);
      // Page should not be re-dirtied since there's no tail to zero
      // (dirty was cleared by flush, and PAGE_SIZE % PAGE_SIZE === 0 exits early)
      expect(cache.isDirty("/test", 0)).toBe(false);
    });

    it("loads zero-filled page into cache when page not in backend", async () => {
      // No pages loaded or stored — should load a zero-filled page into cache
      await cache.zeroTailAfterTruncate("/test", 100);
      expect(cache.size).toBe(1);
      // Page should be dirty (tail was zeroed)
      expect(cache.isDirty("/test", 0)).toBe(true);
    });

    it("zeros tail of evicted page in backend", async () => {
      // Write a full page, flush, and evict it
      await cache.write("/file", fillBuf(PAGE_SIZE, 0xaa), 0, PAGE_SIZE, 0, 0);
      await cache.flushFile("/file");
      await cache.evictFile("/file");
      expect(cache.has("/file", 0)).toBe(false);

      // Truncate to 100 bytes — should zero tail in backend
      await cache.zeroTailAfterTruncate("/file", 100);

      // Reload from backend and verify
      const page = await cache.getPage("/file", 0);
      expect(page.data[99]).toBe(0xaa);
      expect(page.data[100]).toBe(0);
      expect(page.data[PAGE_SIZE - 1]).toBe(0);
    });
  });

  describe("deleteFile", () => {
    it("removes all cached pages and deletes from backend", async () => {
      await cache.write("/file", fillBuf(PAGE_SIZE * 2, 0xbb), 0, PAGE_SIZE * 2, 0, 0);
      await cache.flushFile("/file");
      expect(cache.has("/file", 0)).toBe(true);
      expect(cache.has("/file", 1)).toBe(true);

      await cache.deleteFile("/file");
      expect(cache.has("/file", 0)).toBe(false);
      expect(cache.has("/file", 1)).toBe(false);
      expect(cache.size).toBe(0);

      // Backend should also be empty
      const data = await backend.readPage("/file", 0);
      expect(data).toBeNull();
    });

    it("does not flush dirty pages before deletion", async () => {
      await cache.write("/file", fillBuf(10, 0xcc), 0, 10, 0, 0);
      expect(cache.dirtyCount).toBe(1);

      await cache.deleteFile("/file");
      expect(cache.dirtyCount).toBe(0);

      // Backend should have no data (dirty page was discarded, not flushed)
      const data = await backend.readPage("/file", 0);
      expect(data).toBeNull();
    });

    it("does not affect other files", async () => {
      await cache.write("/a", fillBuf(10, 0x11), 0, 10, 0, 0);
      await cache.write("/b", fillBuf(10, 0x22), 0, 10, 0, 0);
      await cache.flushAll();

      await cache.deleteFile("/a");
      expect(cache.has("/b", 0)).toBe(true);
      const data = await backend.readPage("/b", 0);
      expect(data).not.toBeNull();
    });
  });

  describe("renameFile", () => {
    it("moves pages from old path to new path", async () => {
      await cache.write("/old", fillBuf(PAGE_SIZE + 10, 0xdd), 0, PAGE_SIZE + 10, 0, 0);

      await cache.renameFile("/old", "/new");

      // Old path should be gone
      expect(cache.has("/old", 0)).toBe(false);
      expect(cache.has("/old", 1)).toBe(false);

      // New path should have the data
      expect(cache.has("/new", 0)).toBe(true);
      expect(cache.has("/new", 1)).toBe(true);

      const buf = new Uint8Array(PAGE_SIZE + 10);
      const n = await cache.read("/new", buf, 0, PAGE_SIZE + 10, 0, PAGE_SIZE + 10);
      expect(n).toBe(PAGE_SIZE + 10);
      expect(buf[0]).toBe(0xdd);
      expect(buf[PAGE_SIZE + 9]).toBe(0xdd);
    });

    it("flushes dirty pages before rename", async () => {
      await cache.write("/old", fillBuf(10, 0xee), 0, 10, 0, 0);
      expect(cache.isDirty("/old", 0)).toBe(true);

      await cache.renameFile("/old", "/new");

      // Backend should have the data under new path
      const data = await backend.readPage("/new", 0);
      expect(data).not.toBeNull();
      expect(data![0]).toBe(0xee);
    });

    it("survives eviction and re-read after rename", async () => {
      await cache.write("/old", fillBuf(10, 0xff), 0, 10, 0, 0);
      await cache.renameFile("/old", "/new");
      await cache.evictFile("/new");

      const buf = new Uint8Array(10);
      const n = await cache.read("/new", buf, 0, 10, 0, 10);
      expect(n).toBe(10);
      expect(buf[0]).toBe(0xff);
    });

    it("evicts cached destination pages before rename", async () => {
      // Write 3 pages to destination
      await cache.write("/dest", fillBuf(PAGE_SIZE * 3, 0xdd), 0, PAGE_SIZE * 3, 0, 0);
      await cache.flushFile("/dest");

      // Write 1 page to source
      await cache.write("/src", fillBuf(PAGE_SIZE, 0xaa), 0, PAGE_SIZE, 0, 0);

      await cache.renameFile("/src", "/dest");

      // Destination page 0 should be source data
      const buf0 = new Uint8Array(PAGE_SIZE);
      await cache.read("/dest", buf0, 0, PAGE_SIZE, 0, PAGE_SIZE);
      expect(buf0[0]).toBe(0xaa);

      // Stale destination pages should not be cached
      expect(cache.has("/dest", 1)).toBe(false);
      expect(cache.has("/dest", 2)).toBe(false);

      // Backend should have no orphan pages
      expect(await backend.readPage("/dest", 1)).toBeNull();
      expect(await backend.readPage("/dest", 2)).toBeNull();
    });

    it("evicts dirty destination pages without flushing them", async () => {
      // Write dirty page to destination (not flushed)
      await cache.write("/dest", fillBuf(PAGE_SIZE, 0xdd), 0, PAGE_SIZE, 0, 0);
      expect(cache.isDirty("/dest", 0)).toBe(true);

      // Write source page
      await cache.write("/src", fillBuf(PAGE_SIZE, 0xaa), 0, PAGE_SIZE, 0, 0);

      await cache.renameFile("/src", "/dest");

      // Destination should have source data
      const buf = new Uint8Array(PAGE_SIZE);
      await cache.read("/dest", buf, 0, PAGE_SIZE, 0, PAGE_SIZE);
      expect(buf[0]).toBe(0xaa);
    });
  });

  describe("batch read pre-loading", () => {
    it("batch-loads missing pages for multi-page reads", async () => {
      // Pre-populate 3 pages in backend
      for (let i = 0; i < 3; i++) {
        const data = new Uint8Array(PAGE_SIZE);
        data.fill(i + 1);
        await backend.writePage("/test", i, data);
      }

      // Track readPages calls
      let readPagesCalls = 0;
      const origReadPages = backend.readPages.bind(backend);
      backend.readPages = async (path: string, indices: number[]) => {
        readPagesCalls++;
        return origReadPages(path, indices);
      };

      const buf = new Uint8Array(PAGE_SIZE * 3);
      const n = await cache.read("/test", buf, 0, PAGE_SIZE * 3, 0, PAGE_SIZE * 3);
      expect(n).toBe(PAGE_SIZE * 3);
      // Should have used batch readPages (one call for the 3 missing pages)
      expect(readPagesCalls).toBe(1);
      // Verify data integrity
      expect(buf[0]).toBe(1);
      expect(buf[PAGE_SIZE]).toBe(2);
      expect(buf[PAGE_SIZE * 2]).toBe(3);
    });

    it("skips batching when all pages are cached", async () => {
      // Pre-load pages into cache
      for (let i = 0; i < 3; i++) {
        const data = new Uint8Array(PAGE_SIZE);
        data.fill(i + 1);
        await backend.writePage("/test", i, data);
        await cache.getPage("/test", i);
      }

      let readPagesCalls = 0;
      const origReadPages = backend.readPages.bind(backend);
      backend.readPages = async (path: string, indices: number[]) => {
        readPagesCalls++;
        return origReadPages(path, indices);
      };

      const buf = new Uint8Array(PAGE_SIZE * 3);
      await cache.read("/test", buf, 0, PAGE_SIZE * 3, 0, PAGE_SIZE * 3);
      // No batch read needed — all pages already cached
      expect(readPagesCalls).toBe(0);
    });

    it("only batch-loads uncached pages, skips cached ones", async () => {
      // Pre-populate 3 pages in backend
      for (let i = 0; i < 3; i++) {
        const data = new Uint8Array(PAGE_SIZE);
        data.fill(i + 1);
        await backend.writePage("/test", i, data);
      }
      // Cache page 1 (middle page)
      await cache.getPage("/test", 1);

      let batchedIndices: number[] = [];
      const origReadPages = backend.readPages.bind(backend);
      backend.readPages = async (path: string, indices: number[]) => {
        batchedIndices = indices;
        return origReadPages(path, indices);
      };

      const buf = new Uint8Array(PAGE_SIZE * 3);
      await cache.read("/test", buf, 0, PAGE_SIZE * 3, 0, PAGE_SIZE * 3);
      // Should only request pages 0 and 2 (page 1 is cached)
      expect(batchedIndices).toEqual([0, 2]);
    });

    it("falls back to getPage for single-page reads", async () => {
      const data = new Uint8Array(PAGE_SIZE);
      data.fill(0xab);
      await backend.writePage("/test", 0, data);

      let readPagesCalls = 0;
      const origReadPages = backend.readPages.bind(backend);
      backend.readPages = async (path: string, indices: number[]) => {
        readPagesCalls++;
        return origReadPages(path, indices);
      };

      const buf = new Uint8Array(100);
      await cache.read("/test", buf, 0, 100, 0, PAGE_SIZE);
      // Single-page read should not use batch readPages
      expect(readPagesCalls).toBe(0);
      expect(buf[0]).toBe(0xab);
    });
  });

  describe("batch write pre-loading", () => {
    it("batch-loads missing pages for multi-page writes", async () => {
      // Pre-populate 3 pages with existing data
      for (let i = 0; i < 3; i++) {
        const data = new Uint8Array(PAGE_SIZE);
        data.fill(0x10 + i);
        await backend.writePage("/test", i, data);
      }

      let readPagesCalls = 0;
      const origReadPages = backend.readPages.bind(backend);
      backend.readPages = async (path: string, indices: number[]) => {
        readPagesCalls++;
        return origReadPages(path, indices);
      };

      // Write spanning 3 pages (page-aligned, fully overwrites all pages)
      const buf = new Uint8Array(PAGE_SIZE * 3);
      buf.fill(0xff);
      await cache.write("/test", buf, 0, PAGE_SIZE * 3, 0, PAGE_SIZE * 3);
      // All 3 pages fully overwritten → no batch reads needed
      expect(readPagesCalls).toBe(0);
    });
  });

  describe("batch eviction", () => {
    it("batch-flushes dirty pages during eviction", async () => {
      const small = new PageCache(backend, 4);

      // Fill cache with dirty pages
      for (let i = 0; i < 4; i++) {
        await small.write("/a", fillBuf(PAGE_SIZE, i + 1), 0, PAGE_SIZE, i * PAGE_SIZE, i * PAGE_SIZE);
      }
      expect(small.size).toBe(4);

      let writePagesCalls = 0;
      const origWritePages = backend.writePages.bind(backend);
      backend.writePages = async (pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>) => {
        writePagesCalls++;
        return origWritePages(pages);
      };

      // Read 3 new pages — triggers batch eviction of 3 dirty pages
      for (let i = 0; i < 3; i++) {
        const data = new Uint8Array(PAGE_SIZE);
        data.fill(0xbb);
        await backend.writePage("/b", i, data);
      }

      const buf = new Uint8Array(PAGE_SIZE * 3);
      await small.read("/b", buf, 0, PAGE_SIZE * 3, 0, PAGE_SIZE * 3);

      // Dirty pages should have been flushed to backend
      for (let i = 0; i < 3; i++) {
        const stored = await backend.readPage("/a", i);
        expect(stored).not.toBeNull();
        expect(stored![0]).toBe(i + 1);
      }
    });

    it("preserves data integrity under eviction pressure during multi-page write", async () => {
      // Cache holds 4 pages, write spans 8 pages with batch optimizations
      const small = new PageCache(backend, 4);
      const size = PAGE_SIZE * 8;
      const data = new Uint8Array(size);
      for (let i = 0; i < size; i++) data[i] = i % 251;

      await small.write("/test", data, 0, size, 0, 0);
      await small.flushAll();

      // Verify all data round-trips correctly
      const buf = new Uint8Array(size);
      const n = await small.read("/test", buf, 0, size, 0, size);
      expect(n).toBe(size);
      expect(Array.from(buf)).toEqual(Array.from(data));
    });
  });

  describe("read-write round-trip", () => {
    it("@fast writes then reads same data", async () => {
      const data = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
      await cache.write("/test", data, 0, 10, 0, 0);

      const buf = new Uint8Array(10);
      const n = await cache.read("/test", buf, 0, 10, 0, 10);
      expect(n).toBe(10);
      expect(Array.from(buf)).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    });

    it("round-trips data across page boundaries", async () => {
      const size = PAGE_SIZE * 2 + 100;
      const data = new Uint8Array(size);
      for (let i = 0; i < size; i++) data[i] = i % 256;

      await cache.write("/test", data, 0, size, 0, 0);

      const buf = new Uint8Array(size);
      const n = await cache.read("/test", buf, 0, size, 0, size);
      expect(n).toBe(size);
      expect(Array.from(buf)).toEqual(Array.from(data));
    });

    it("survives flush + evict + re-read", async () => {
      const data = new Uint8Array([0xde, 0xad, 0xbe, 0xef]);
      await cache.write("/test", data, 0, 4, 0, 0);

      await cache.evictFile("/test");
      expect(cache.has("/test", 0)).toBe(false);

      // Re-read from backend
      const buf = new Uint8Array(4);
      const n = await cache.read("/test", buf, 0, 4, 0, 4);
      expect(n).toBe(4);
      expect(Array.from(buf)).toEqual([0xde, 0xad, 0xbe, 0xef]);
    });

    it("large multi-page write survives eviction pressure", async () => {
      // Cache only holds 4 pages, but write spans 8 pages
      const small = new PageCache(backend, 4);
      const size = PAGE_SIZE * 8;
      const data = new Uint8Array(size);
      for (let i = 0; i < size; i++) data[i] = i % 251; // prime to avoid patterns

      await small.write("/test", data, 0, size, 0, 0);

      // Flush everything before reading (some pages may have been evicted dirty)
      await small.flushAll();

      // Read back (will re-load from backend as pages get evicted)
      const buf = new Uint8Array(size);
      const n = await small.read("/test", buf, 0, size, 0, size);
      expect(n).toBe(size);
      expect(Array.from(buf)).toEqual(Array.from(data));
    });
  });

  describe("getStats", () => {
    it("@fast starts at zero", () => {
      const stats = cache.getStats();
      expect(stats).toEqual({ hits: 0, misses: 0, evictions: 0, flushes: 0 });
    });

    it("@fast counts cache miss on first getPage", async () => {
      await cache.getPage("/test", 0);
      expect(cache.getStats().misses).toBe(1);
      expect(cache.getStats().hits).toBe(0);
    });

    it("@fast counts cache hit on repeated getPage", async () => {
      await cache.getPage("/test", 0);
      await cache.getPage("/test", 0);
      expect(cache.getStats().misses).toBe(1);
      expect(cache.getStats().hits).toBe(1);
    });

    it("counts MRU fast-path as a hit", async () => {
      await cache.getPage("/test", 0);
      await cache.getPage("/test", 0);
      await cache.getPage("/test", 0);
      expect(cache.getStats().hits).toBe(2);
    });

    it("counts eviction when cache is full", async () => {
      const small = new PageCache(backend, 2);
      await small.getPage("/test", 0);
      await small.getPage("/test", 1);
      await small.getPage("/test", 2);
      expect(small.getStats().evictions).toBe(1);
    });

    it("counts dirty flush on eviction", async () => {
      const small = new PageCache(backend, 2);
      const data = new Uint8Array([1]);
      await small.write("/test", data, 0, 1, 0, 0);
      await small.getPage("/test", 1);
      // Third page evicts dirty page 0
      await small.getPage("/test", 2);
      expect(small.getStats().flushes).toBe(1);
      expect(small.getStats().evictions).toBe(1);
    });

    it("counts flushFile", async () => {
      const data = new Uint8Array([1, 2, 3]);
      await cache.write("/test", data, 0, 3, 0, 0);
      await cache.write("/test", data, 0, 3, PAGE_SIZE, 3);
      await cache.flushFile("/test");
      expect(cache.getStats().flushes).toBe(2);
    });

    it("counts flushAll", async () => {
      const data = new Uint8Array([1]);
      await cache.write("/a", data, 0, 1, 0, 0);
      await cache.write("/b", data, 0, 1, 0, 0);
      await cache.flushAll();
      expect(cache.getStats().flushes).toBe(2);
    });

    it("resetStats clears all counters", async () => {
      const small = new PageCache(backend, 2);
      const data = new Uint8Array([1]);
      await small.write("/test", data, 0, 1, 0, 0);
      await small.getPage("/test", 0);
      await small.getPage("/test", 1);
      await small.getPage("/test", 2); // eviction
      await small.flushAll();

      const before = small.getStats();
      expect(before.hits + before.misses + before.evictions + before.flushes).toBeGreaterThan(0);

      small.resetStats();
      expect(small.getStats()).toEqual({ hits: 0, misses: 0, evictions: 0, flushes: 0 });
    });

    it("getStats returns a snapshot (not a live reference)", async () => {
      const snap = cache.getStats();
      await cache.getPage("/test", 0);
      expect(snap.misses).toBe(0);
      expect(cache.getStats().misses).toBe(1);
    });

    it("counts multi-page read misses correctly", async () => {
      const size = PAGE_SIZE * 3;
      const data = new Uint8Array(size);
      await cache.write("/test", data, 0, size, 0, 0);
      await cache.flushAll();
      await cache.evictFile("/test");
      cache.resetStats();

      const buf = new Uint8Array(size);
      await cache.read("/test", buf, 0, size, 0, size);

      expect(cache.getStats().misses).toBe(3);
    });

    it("multi-page read does not double-count batch-loaded pages as hits", async () => {
      const size = PAGE_SIZE * 3;
      const data = new Uint8Array(size);
      await cache.write("/test", data, 0, size, 0, 0);
      await cache.flushAll();
      await cache.evictFile("/test");
      cache.resetStats();

      const buf = new Uint8Array(size);
      await cache.read("/test", buf, 0, size, 0, size);

      const stats = cache.getStats();
      expect(stats.misses).toBe(3);
      // No pages should be counted as hits — they were all loaded fresh
      expect(stats.hits).toBe(0);
    });

    it("multi-page read counts hits correctly for partially cached pages", async () => {
      const size = PAGE_SIZE * 3;
      const data = new Uint8Array(size);
      await cache.write("/test", data, 0, size, 0, 0);
      await cache.flushAll();
      await cache.evictFile("/test");
      // Load page 1 back into cache
      await cache.getPage("/test", 1);
      cache.resetStats();

      const buf = new Uint8Array(size);
      await cache.read("/test", buf, 0, size, 0, size);

      const stats = cache.getStats();
      // Pages 0 and 2 are misses
      expect(stats.misses).toBe(2);
      // Page 1 is a legitimate hit
      expect(stats.hits).toBe(1);
    });

    it("multi-page write does not double-count batch-loaded pages as hits", async () => {
      const size = PAGE_SIZE * 3;
      const data = new Uint8Array(size);
      await cache.write("/test", data, 0, size, 0, 0);
      await cache.flushAll();
      await cache.evictFile("/test");
      cache.resetStats();

      const writeData = new Uint8Array(size);
      writeData.fill(0x42);
      await cache.write("/test", writeData, 0, size, 0, size);

      const stats = cache.getStats();
      expect(stats.misses).toBe(3);
      expect(stats.hits).toBe(0);
    });
  });

  describe("skip backend reads for pages beyond file extent", () => {
    it("extending write skips backend reads for new pages", async () => {
      // Write 1 page to establish file extent
      const init = new Uint8Array(PAGE_SIZE);
      init.fill(0xaa);
      await cache.write("/file", init, 0, PAGE_SIZE, 0, 0);
      await cache.flushAll();
      await cache.evictFile("/file");

      // Spy on readPages
      let readPagesIndicesTotal = 0;
      const origReadPages = backend.readPages.bind(backend);
      backend.readPages = async (path: string, indices: number[]) => {
        readPagesIndicesTotal += indices.length;
        return origReadPages(path, indices);
      };
      let readPageCalls = 0;
      const origReadPage = backend.readPage.bind(backend);
      backend.readPage = async (path: string, pageIndex: number) => {
        readPageCalls++;
        return origReadPage(path, pageIndex);
      };

      // Write 4 pages starting at page 0 (1 existing + 3 new)
      const data = new Uint8Array(PAGE_SIZE * 4);
      for (let i = 0; i < data.length; i++) data[i] = (i * 7) & 0xff;
      await cache.write("/file", data, 0, data.length, 0, PAGE_SIZE);

      // Page 0 exists but is fully overwritten (page-aligned write covers
      // entire page) → read skipped. Pages 1-3 beyond file extent → also
      // no read.
      expect(readPageCalls).toBe(0);
      expect(readPagesIndicesTotal).toBe(0);

      // Verify data integrity
      const buf = new Uint8Array(data.length);
      await cache.read("/file", buf, 0, data.length, 0, PAGE_SIZE * 4);
      expect(buf).toEqual(data);
    });

    it("MRU page reference is cleared on eviction", async () => {
      const small = new PageCache(backend, 2);
      // Access page 0 — becomes MRU
      await small.getPage("/file", 0);
      // Access page 1
      await small.getPage("/file", 1);
      // Access page 2 — evicts page 0 (LRU)
      await small.getPage("/file", 2);
      // Now access page 0 again — must be a miss (reloaded from backend),
      // proving the MRU reference was cleared when page 0 was evicted
      const stats = small.getStats();
      const missesBefore = stats.misses;
      await small.getPage("/file", 0);
      expect(small.getStats().misses).toBe(missesBefore + 1);
    });

    it("MRU page sets evicted flag when evicted", async () => {
      const small = new PageCache(backend, 1);
      const page = await small.getPage("/file", 0);
      expect(page.evicted).toBe(false);
      // Access another page — evicts the MRU page
      await small.getPage("/file", 1);
      expect(page.evicted).toBe(true);
    });

    it("write to new file skips all backend reads", async () => {
      let readPageCalls = 0;
      const origReadPage = backend.readPage.bind(backend);
      backend.readPage = async (path: string, pageIndex: number) => {
        readPageCalls++;
        return origReadPage(path, pageIndex);
      };

      // Write 3 pages to a brand-new file (currentFileSize=0)
      const data = new Uint8Array(PAGE_SIZE * 3);
      data.fill(0xbb);
      await cache.write("/new", data, 0, data.length, 0, 0);

      // No backend reads — all pages are new
      expect(readPageCalls).toBe(0);

      const buf = new Uint8Array(data.length);
      await cache.read("/new", buf, 0, data.length, 0, PAGE_SIZE * 3);
      expect(buf).toEqual(data);
    });
  });

  describe("full-page overwrite skip", () => {
    function createCountingBackend() {
      const inner = new MemoryBackend();
      let readPageCalls = 0;

      return {
        backend: inner,
        get readPageCalls() { return readPageCalls; },
        resetCounts() { readPageCalls = 0; },
        wrap(): MemoryBackend {
          const wrapped = Object.create(inner) as MemoryBackend;
          wrapped.readPage = async (path: string, pageIndex: number) => {
            readPageCalls++;
            return inner.readPage(path, pageIndex);
          };
          wrapped.readPages = async (path: string, pageIndices: number[]) => {
            readPageCalls += pageIndices.length;
            return inner.readPages(path, pageIndices);
          };
          return wrapped;
        },
      };
    }

    it("@fast single-page full overwrite skips backend read", async () => {
      const cb = createCountingBackend();
      const wrapped = cb.wrap();
      const cache = new PageCache(wrapped, 8);

      // Write initial data
      const initial = new Uint8Array(PAGE_SIZE);
      initial.fill(0xaa);
      await cache.write("/file", initial, 0, PAGE_SIZE, 0, 0);
      await cache.flushAll();
      await cache.evictFile("/file");
      cb.resetCounts();

      // Full-page overwrite
      const overwrite = new Uint8Array(PAGE_SIZE);
      overwrite.fill(0xbb);
      await cache.write("/file", overwrite, 0, PAGE_SIZE, 0, PAGE_SIZE);

      expect(cb.readPageCalls).toBe(0);

      const buf = new Uint8Array(PAGE_SIZE);
      await cache.read("/file", buf, 0, PAGE_SIZE, 0, PAGE_SIZE);
      expect(buf[0]).toBe(0xbb);
      expect(buf[PAGE_SIZE - 1]).toBe(0xbb);
    });

    it("partial write within existing page still reads from backend", async () => {
      const cb = createCountingBackend();
      const wrapped = cb.wrap();
      const cache = new PageCache(wrapped, 8);

      const initial = new Uint8Array(PAGE_SIZE);
      initial.fill(0xaa);
      await cache.write("/file", initial, 0, PAGE_SIZE, 0, 0);
      await cache.flushAll();
      await cache.evictFile("/file");
      cb.resetCounts();

      const partial = new Uint8Array(100);
      partial.fill(0xcc);
      await cache.write("/file", partial, 0, 100, 0, PAGE_SIZE);

      expect(cb.readPageCalls).toBe(1);

      const buf = new Uint8Array(PAGE_SIZE);
      await cache.read("/file", buf, 0, PAGE_SIZE, 0, PAGE_SIZE);
      expect(buf[0]).toBe(0xcc);
      expect(buf[99]).toBe(0xcc);
      expect(buf[100]).toBe(0xaa);
    });

    it("multi-page write skips reads for fully-overwritten middle pages", async () => {
      const cb = createCountingBackend();
      const wrapped = cb.wrap();
      const cache = new PageCache(wrapped, 16);

      const initial = new Uint8Array(PAGE_SIZE * 4);
      initial.fill(0xaa);
      await cache.write("/file", initial, 0, initial.length, 0, 0);
      await cache.flushAll();
      await cache.evictFile("/file");
      cb.resetCounts();

      // Write spanning pages 0-3, offset 10 in page 0
      // Page 0: partial — needs read
      // Pages 1-2: fully overwritten — skip read
      // Page 3: partial — needs read
      const writeSize = PAGE_SIZE * 4 - 20;
      const overwrite = new Uint8Array(writeSize);
      overwrite.fill(0xdd);
      await cache.write("/file", overwrite, 0, writeSize, 10, PAGE_SIZE * 4);

      expect(cb.readPageCalls).toBe(2);
    });

    it("@fast page-aligned multi-page write skips all backend reads", async () => {
      const cb = createCountingBackend();
      const wrapped = cb.wrap();
      const cache = new PageCache(wrapped, 16);

      const initial = new Uint8Array(PAGE_SIZE * 3);
      initial.fill(0xaa);
      await cache.write("/file", initial, 0, initial.length, 0, 0);
      await cache.flushAll();
      await cache.evictFile("/file");
      cb.resetCounts();

      const overwrite = new Uint8Array(PAGE_SIZE * 3);
      overwrite.fill(0xee);
      await cache.write("/file", overwrite, 0, overwrite.length, 0, PAGE_SIZE * 3);

      expect(cb.readPageCalls).toBe(0);

      const buf = new Uint8Array(PAGE_SIZE * 3);
      await cache.read("/file", buf, 0, buf.length, 0, PAGE_SIZE * 3);
      for (let i = 0; i < buf.length; i++) {
        expect(buf[i]).toBe(0xee);
      }
    });

    it("full-page overwrite data persists through flush and re-read", async () => {
      const cb = createCountingBackend();
      const wrapped = cb.wrap();
      const cache = new PageCache(wrapped, 8);

      const initial = new Uint8Array(PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) initial[i] = i & 0xff;
      await cache.write("/file", initial, 0, PAGE_SIZE, 0, 0);
      await cache.flushAll();
      await cache.evictFile("/file");

      const overwrite = new Uint8Array(PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) overwrite[i] = (i * 3) & 0xff;
      await cache.write("/file", overwrite, 0, PAGE_SIZE, 0, PAGE_SIZE);

      await cache.flushAll();
      await cache.evictFile("/file");

      const buf = new Uint8Array(PAGE_SIZE);
      await cache.read("/file", buf, 0, PAGE_SIZE, 0, PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) {
        expect(buf[i]).toBe((i * 3) & 0xff);
      }
    });
  });

  describe("collectDirtyPages", () => {
    it("returns empty array when no dirty pages", () => {
      expect(cache.collectDirtyPages()).toEqual([]);
    });

    it("returns all dirty pages without clearing dirty flags", async () => {
      const buf1 = fillBuf(PAGE_SIZE, 0xaa);
      const buf2 = fillBuf(PAGE_SIZE, 0xbb);
      await cache.write("/a", buf1, 0, PAGE_SIZE, 0, 0);
      await cache.write("/b", buf2, 0, PAGE_SIZE, 0, 0);

      const collected = cache.collectDirtyPages();
      expect(collected.length).toBe(2);

      // Pages should still be dirty after collection
      expect(cache.dirtyCount).toBe(2);
      expect(cache.isDirty("/a", 0)).toBe(true);
      expect(cache.isDirty("/b", 0)).toBe(true);

      // Verify collected data
      const paths = collected.map((p) => p.path).sort();
      expect(paths).toEqual(["/a", "/b"]);
    });

    it("can be called multiple times without side effects", async () => {
      await cache.write("/f", fillBuf(PAGE_SIZE, 1), 0, PAGE_SIZE, 0, 0);

      const first = cache.collectDirtyPages();
      const second = cache.collectDirtyPages();
      expect(first.length).toBe(1);
      expect(second.length).toBe(1);
      expect(cache.dirtyCount).toBe(1);
    });
  });

  describe("commitDirtyPages", () => {
    it("clears dirty flags for committed pages", async () => {
      await cache.write("/a", fillBuf(PAGE_SIZE, 0xaa), 0, PAGE_SIZE, 0, 0);
      await cache.write("/b", fillBuf(PAGE_SIZE, 0xbb), 0, PAGE_SIZE, 0, 0);

      const collected = cache.collectDirtyPages();
      expect(cache.dirtyCount).toBe(2);

      cache.commitDirtyPages(collected);
      expect(cache.dirtyCount).toBe(0);
      expect(cache.isDirty("/a", 0)).toBe(false);
      expect(cache.isDirty("/b", 0)).toBe(false);
    });

    it("preserves pages dirtied between collect and commit", async () => {
      await cache.write("/a", fillBuf(PAGE_SIZE, 0xaa), 0, PAGE_SIZE, 0, 0);
      const collected = cache.collectDirtyPages();

      // Dirty a new page after collection
      await cache.write("/b", fillBuf(PAGE_SIZE, 0xbb), 0, PAGE_SIZE, 0, 0);
      expect(cache.dirtyCount).toBe(2);

      // Commit only the originally collected pages
      cache.commitDirtyPages(collected);
      expect(cache.dirtyCount).toBe(1);
      expect(cache.isDirty("/a", 0)).toBe(false);
      expect(cache.isDirty("/b", 0)).toBe(true);
    });

    it("handles pages evicted between collect and commit", async () => {
      const smallCache = new PageCache(backend, 2);
      await smallCache.write("/a", fillBuf(PAGE_SIZE, 1), 0, PAGE_SIZE, 0, 0);
      const collected = smallCache.collectDirtyPages();

      // Fill cache to force eviction of /a
      await smallCache.getPage("/b", 0);
      await smallCache.getPage("/c", 0);

      // Commit should be a no-op for the evicted page (it was flushed on eviction)
      smallCache.commitDirtyPages(collected);
      // No error, no crash
      expect(smallCache.dirtyCount).toBe(0);
    });

    it("handles pages re-dirtied between collect and commit", async () => {
      await cache.write("/a", fillBuf(PAGE_SIZE, 0xaa), 0, PAGE_SIZE, 0, 0);
      const collected = cache.collectDirtyPages();

      // Re-dirty the same page with new data
      await cache.write("/a", fillBuf(PAGE_SIZE, 0xbb), 0, PAGE_SIZE, 0, PAGE_SIZE);

      // Commit clears dirty flag — the re-dirtied page's flag is the same flag
      // (page is still in cache at the same key), so commit clears it.
      // This matches the sync variant's behavior: collectDirtyPages snapshots
      // dirty page references, and commitDirtyPages clears each one.
      cache.commitDirtyPages(collected);
      expect(cache.isDirty("/a", 0)).toBe(false);
    });

    it("increments flush counter", async () => {
      await cache.write("/a", fillBuf(PAGE_SIZE, 1), 0, PAGE_SIZE, 0, 0);
      await cache.write("/b", fillBuf(PAGE_SIZE, 2), 0, PAGE_SIZE, 0, 0);

      const before = cache.getStats().flushes;
      cache.commitDirtyPages(cache.collectDirtyPages());
      expect(cache.getStats().flushes).toBe(before + 2);
    });

    it("two-phase commit round-trip: collect → backend write → commit", async () => {
      // Write dirty pages
      await cache.write("/f", fillBuf(PAGE_SIZE, 0x42), 0, PAGE_SIZE, 0, 0);

      // Phase 1: collect
      const dirty = cache.collectDirtyPages();
      expect(dirty.length).toBe(1);
      expect(cache.dirtyCount).toBe(1); // still dirty

      // Phase 2: write to backend
      await backend.writePages(dirty);

      // Phase 3: commit
      cache.commitDirtyPages(dirty);
      expect(cache.dirtyCount).toBe(0);

      // Verify data persisted correctly
      const stored = await backend.readPage("/f", 0);
      expect(stored).not.toBeNull();
      expect(stored![0]).toBe(0x42);
    });
  });

  describe("markPageDirtyNoRead", () => {
    it("@fast marks a page as dirty without reading from backend", async () => {
      let readPageCalls = 0;
      const origReadPage = backend.readPage.bind(backend);
      backend.readPage = async (path: string, pageIndex: number) => {
        readPageCalls++;
        return origReadPage(path, pageIndex);
      };

      await cache.markPageDirtyNoRead("/file", 5);

      expect(readPageCalls).toBe(0);
      expect(cache.isDirty("/file", 5)).toBe(true);
      expect(cache.has("/file", 5)).toBe(true);
    });

    it("creates a zero-filled page on cache miss", async () => {
      await cache.markPageDirtyNoRead("/file", 3);

      const buf = new Uint8Array(PAGE_SIZE);
      await cache.read("/file", buf, 0, PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) {
        expect(buf[i]).toBe(0);
      }
    });

    it("reuses existing cached page when already loaded", async () => {
      await cache.write("/file", fillBuf(PAGE_SIZE, 0xAB), 0, PAGE_SIZE, 0, 0);
      await cache.flushFile("/file");

      expect(cache.isDirty("/file", 0)).toBe(false);
      await cache.markPageDirtyNoRead("/file", 0);
      expect(cache.isDirty("/file", 0)).toBe(true);

      const buf = new Uint8Array(PAGE_SIZE);
      await cache.read("/file", buf, 0, PAGE_SIZE, 0, PAGE_SIZE);
      expect(buf[0]).toBe(0xAB);
    });

    it("sentinel page survives flush + eviction round-trip", async () => {
      await cache.markPageDirtyNoRead("/file", 10);

      await cache.flushAll();
      expect(cache.isDirty("/file", 10)).toBe(false);

      await cache.evictFile("/file");
      expect(cache.has("/file", 10)).toBe(false);

      const stored = await backend.readPage("/file", 10);
      expect(stored).not.toBeNull();
      expect(stored!.every((b: number) => b === 0)).toBe(true);
    });
  });

  describe("markPageDirty", () => {
    it("loads existing data from backend when page is not cached", async () => {
      await backend.writePage("/file", 2, fillBuf(PAGE_SIZE, 0xCD));

      await cache.markPageDirty("/file", 2);

      expect(cache.isDirty("/file", 2)).toBe(true);
      const buf = new Uint8Array(PAGE_SIZE);
      await cache.read("/file", buf, 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE);
      expect(buf[0]).toBe(0xCD);
    });
  });
});
