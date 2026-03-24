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

      cache.zeroTailAfterTruncate("/test", 10);

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

      cache.zeroTailAfterTruncate("/test", PAGE_SIZE);
      // Page should not be re-dirtied since there's no tail to zero
      // (dirty was cleared by flush, and PAGE_SIZE % PAGE_SIZE === 0 exits early)
      expect(cache.isDirty("/test", 0)).toBe(false);
    });

    it("no-op when page not in cache", async () => {
      // No pages loaded — should not throw
      cache.zeroTailAfterTruncate("/test", 100);
      expect(cache.size).toBe(0);
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
});
