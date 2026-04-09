/**
 * Parity tests: PageCache (async) vs SyncPageCache (sync).
 *
 * These two classes are independently maintained (~920 LOC each) with
 * near-identical logic differing only in async/await. This test suite
 * runs the same operation sequences against both and verifies identical
 * outcomes: same data, same dirty state, same eviction behavior, same
 * stats. Any divergence here indicates a porting bug.
 */
import { describe, it, expect, beforeEach } from "vitest";
import { PageCache } from "../../src/page-cache.js";
import { SyncPageCache } from "../../src/sync-page-cache.js";
import { MemoryBackend } from "../../src/memory-backend.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { PAGE_SIZE } from "../../src/types.js";

describe("PageCache / SyncPageCache parity", () => {
  let asyncBackend: MemoryBackend;
  let syncBackend: SyncMemoryBackend;
  let asyncCache: PageCache;
  let syncCache: SyncPageCache;

  /** Run tests at a small cache size to exercise eviction paths. */
  const MAX_PAGES = 4;

  beforeEach(() => {
    asyncBackend = new MemoryBackend();
    syncBackend = new SyncMemoryBackend();
    asyncCache = new PageCache(asyncBackend, MAX_PAGES);
    syncCache = new SyncPageCache(syncBackend, MAX_PAGES);
  });

  /** Seed both backends with the same page data. */
  async function seedPage(
    path: string,
    pageIndex: number,
    fillByte: number,
  ): Promise<void> {
    const data = new Uint8Array(PAGE_SIZE);
    data.fill(fillByte);
    await asyncBackend.writePage(path, pageIndex, data);
    syncBackend.writePage(path, pageIndex, data);
  }

  /** Seed both backends with the same metadata. */
  async function seedMeta(
    path: string,
    size: number,
  ): Promise<void> {
    const meta = { size, mode: 0o644, ctime: 1000, mtime: 2000 };
    await asyncBackend.writeMeta(path, meta);
    syncBackend.writeMeta(path, meta);
  }

  describe("@fast getPage", () => {
    it("returns identical zero-filled pages for new files", async () => {
      const asyncPage = await asyncCache.getPage("/f", 0);
      const syncPage = syncCache.getPage("/f", 0);

      expect(asyncPage.data).toEqual(syncPage.data);
      expect(asyncPage.dirty).toBe(syncPage.dirty);
      expect(asyncPage.path).toBe(syncPage.path);
      expect(asyncPage.pageIndex).toBe(syncPage.pageIndex);
    });

    it("returns identical data from backend", async () => {
      await seedPage("/f", 0, 0xab);

      const asyncPage = await asyncCache.getPage("/f", 0);
      const syncPage = syncCache.getPage("/f", 0);

      expect(asyncPage.data).toEqual(syncPage.data);
      expect(asyncPage.dirty).toBe(false);
      expect(syncPage.dirty).toBe(false);
    });

    it("cache hit stats match", async () => {
      await seedPage("/f", 0, 0x01);

      // First access = miss, second = hit
      await asyncCache.getPage("/f", 0);
      await asyncCache.getPage("/f", 0);
      syncCache.getPage("/f", 0);
      syncCache.getPage("/f", 0);

      expect(asyncCache.getStats()).toEqual(syncCache.getStats());
    });
  });

  describe("@fast getPageNoRead", () => {
    it("returns identical zero-filled pages", async () => {
      const asyncPage = await asyncCache.getPageNoRead("/f", 5);
      const syncPage = syncCache.getPageNoRead("/f", 5);

      expect(asyncPage.data).toEqual(syncPage.data);
      expect(asyncPage.data.every((b) => b === 0)).toBe(true);
    });
  });

  describe("@fast single-page write and read", () => {
    it("produces identical results", async () => {
      const data = new Uint8Array([10, 20, 30, 40, 50]);

      const asyncResult = await asyncCache.write("/f", data, 0, 5, 0, 0);
      const syncResult = syncCache.write("/f", data, 0, 5, 0, 0);

      expect(asyncResult).toEqual(syncResult);

      const asyncBuf = new Uint8Array(5);
      const syncBuf = new Uint8Array(5);
      const asyncRead = await asyncCache.read("/f", asyncBuf, 0, 5, 0, 5);
      const syncRead = syncCache.read("/f", syncBuf, 0, 5, 0, 5);

      expect(asyncRead).toBe(syncRead);
      expect(asyncBuf).toEqual(syncBuf);
    });
  });

  describe("@fast multi-page write and read", () => {
    it("produces identical results spanning 3 pages", async () => {
      const size = PAGE_SIZE * 2 + 100;
      const data = new Uint8Array(size);
      for (let i = 0; i < size; i++) data[i] = i & 0xff;

      const asyncResult = await asyncCache.write("/f", data, 0, size, 0, 0);
      const syncResult = syncCache.write("/f", data, 0, size, 0, 0);

      expect(asyncResult).toEqual(syncResult);

      const asyncBuf = new Uint8Array(size);
      const syncBuf = new Uint8Array(size);
      await asyncCache.read("/f", asyncBuf, 0, size, 0, size);
      syncCache.read("/f", syncBuf, 0, size, 0, size);

      expect(asyncBuf).toEqual(syncBuf);
    });

    it("multi-page read with pre-seeded backend data", async () => {
      // Seed 3 pages in both backends
      for (let i = 0; i < 3; i++) {
        await seedPage("/f", i, 0x10 + i);
      }
      const fileSize = PAGE_SIZE * 3;

      const asyncBuf = new Uint8Array(fileSize);
      const syncBuf = new Uint8Array(fileSize);
      const asyncRead = await asyncCache.read(
        "/f", asyncBuf, 0, fileSize, 0, fileSize,
      );
      const syncRead = syncCache.read(
        "/f", syncBuf, 0, fileSize, 0, fileSize,
      );

      expect(asyncRead).toBe(syncRead);
      expect(asyncBuf).toEqual(syncBuf);
    });
  });

  describe("@fast read clamping to file size", () => {
    it("both clamp identically", async () => {
      const data = new Uint8Array([1, 2, 3]);
      await asyncCache.write("/f", data, 0, 3, 0, 0);
      syncCache.write("/f", data, 0, 3, 0, 0);

      const asyncBuf = new Uint8Array(10);
      const syncBuf = new Uint8Array(10);
      const asyncRead = await asyncCache.read("/f", asyncBuf, 0, 10, 0, 3);
      const syncRead = syncCache.read("/f", syncBuf, 0, 10, 0, 3);

      expect(asyncRead).toBe(syncRead);
      expect(asyncRead).toBe(3);
      expect(asyncBuf).toEqual(syncBuf);
    });
  });

  describe("@fast zero-length operations", () => {
    it("zero-length write returns same result", async () => {
      const data = new Uint8Array(0);
      const asyncResult = await asyncCache.write("/f", data, 0, 0, 0, 100);
      const syncResult = syncCache.write("/f", data, 0, 0, 0, 100);
      expect(asyncResult).toEqual(syncResult);
    });

    it("zero-length read returns 0", async () => {
      const asyncRead = await asyncCache.read(
        "/f", new Uint8Array(0), 0, 0, 0, 100,
      );
      const syncRead = syncCache.read(
        "/f", new Uint8Array(0), 0, 0, 0, 100,
      );
      expect(asyncRead).toBe(0);
      expect(syncRead).toBe(0);
    });
  });

  describe("dirty tracking parity", () => {
    it("@fast single write marks same dirty state", async () => {
      const data = new Uint8Array([42]);
      await asyncCache.write("/f", data, 0, 1, 0, 0);
      syncCache.write("/f", data, 0, 1, 0, 0);

      expect(asyncCache.dirtyCount).toBe(syncCache.dirtyCount);
      expect(asyncCache.isDirty("/f", 0)).toBe(syncCache.isDirty("/f", 0));
    });

    it("@fast multi-page write marks same dirty count", async () => {
      const size = PAGE_SIZE * 3;
      const data = new Uint8Array(size);
      data.fill(0xaa);

      await asyncCache.write("/f", data, 0, size, 0, 0);
      syncCache.write("/f", data, 0, size, 0, 0);

      expect(asyncCache.dirtyCount).toBe(syncCache.dirtyCount);
      expect(asyncCache.dirtyCount).toBe(3);
    });

    it("flushFile clears dirty state identically", async () => {
      const data = new Uint8Array(PAGE_SIZE);
      data.fill(0xbb);
      await asyncCache.write("/f", data, 0, PAGE_SIZE, 0, 0);
      syncCache.write("/f", data, 0, PAGE_SIZE, 0, 0);

      await asyncCache.flushFile("/f");
      syncCache.flushFile("/f");

      expect(asyncCache.dirtyCount).toBe(syncCache.dirtyCount);
      expect(asyncCache.dirtyCount).toBe(0);

      // Verify data reached both backends
      const asyncBack = await asyncBackend.readPage("/f", 0);
      const syncBack = syncBackend.readPage("/f", 0);
      expect(asyncBack).toEqual(syncBack);
    });

    it("flushAll clears all dirty state identically", async () => {
      const data = new Uint8Array(PAGE_SIZE);
      for (let i = 0; i < 3; i++) {
        data.fill(0x10 + i);
        await asyncCache.write(`/f${i}`, data, 0, PAGE_SIZE, 0, 0);
        syncCache.write(`/f${i}`, data, 0, PAGE_SIZE, 0, 0);
      }

      await asyncCache.flushAll();
      syncCache.flushAll();

      expect(asyncCache.dirtyCount).toBe(0);
      expect(syncCache.dirtyCount).toBe(0);
    });
  });

  describe("two-phase dirty commit parity", () => {
    it("@fast collectDirtyPages returns same entries", async () => {
      const data = new Uint8Array(PAGE_SIZE);
      data.fill(0xcc);
      await asyncCache.write("/f", data, 0, PAGE_SIZE, 0, 0);
      syncCache.write("/f", data, 0, PAGE_SIZE, 0, 0);

      data.fill(0xdd);
      await asyncCache.write("/g", data, 0, PAGE_SIZE, 0, 0);
      syncCache.write("/g", data, 0, PAGE_SIZE, 0, 0);

      const asyncDirty = await asyncCache.collectDirtyPages();
      const syncDirty = syncCache.collectDirtyPages();

      // Sort by path+pageIndex for stable comparison
      const sortKey = (p: { path: string; pageIndex: number }) =>
        `${p.path}:${p.pageIndex}`;
      asyncDirty.sort((a, b) => sortKey(a).localeCompare(sortKey(b)));
      syncDirty.sort((a, b) => sortKey(a).localeCompare(sortKey(b)));

      expect(asyncDirty.length).toBe(syncDirty.length);
      for (let i = 0; i < asyncDirty.length; i++) {
        expect(asyncDirty[i].path).toBe(syncDirty[i].path);
        expect(asyncDirty[i].pageIndex).toBe(syncDirty[i].pageIndex);
        expect(asyncDirty[i].data).toEqual(syncDirty[i].data);
      }
    });

    it("commitDirtyPages clears same dirty state", async () => {
      const data = new Uint8Array(PAGE_SIZE);
      data.fill(0xee);
      await asyncCache.write("/f", data, 0, PAGE_SIZE, 0, 0);
      syncCache.write("/f", data, 0, PAGE_SIZE, 0, 0);

      const asyncDirty = await asyncCache.collectDirtyPages();
      const syncDirty = syncCache.collectDirtyPages();

      asyncCache.commitDirtyPages(asyncDirty);
      syncCache.commitDirtyPages(syncDirty);

      expect(asyncCache.dirtyCount).toBe(syncCache.dirtyCount);
      expect(asyncCache.dirtyCount).toBe(0);
    });
  });

  describe("eviction parity", () => {
    it("@fast evicts LRU pages identically under pressure", async () => {
      // Fill cache to capacity (4 pages), then add one more to trigger eviction
      for (let i = 0; i < MAX_PAGES; i++) {
        await seedPage("/f", i, 0x10 + i);
      }
      await seedPage("/f", MAX_PAGES, 0x50);

      // Access pages 0..3 in order, then access page 4 to evict page 0
      for (let i = 0; i < MAX_PAGES; i++) {
        await asyncCache.getPage("/f", i);
        syncCache.getPage("/f", i);
      }
      await asyncCache.getPage("/f", MAX_PAGES);
      syncCache.getPage("/f", MAX_PAGES);

      expect(asyncCache.getStats()).toEqual(syncCache.getStats());
      expect(asyncCache.size).toBe(syncCache.size);

      // Page 0 should be evicted in both
      expect(asyncCache.has("/f", 0)).toBe(syncCache.has("/f", 0));
      expect(asyncCache.has("/f", 0)).toBe(false);
    });

    it("dirty eviction flushes to backend identically", async () => {
      // Write 5 pages (exceeds cache of 4), forcing dirty eviction
      const size = PAGE_SIZE * 5;
      const data = new Uint8Array(size);
      for (let i = 0; i < size; i++) data[i] = i & 0xff;

      await asyncCache.write("/f", data, 0, size, 0, 0);
      syncCache.write("/f", data, 0, size, 0, 0);

      expect(asyncCache.getStats().evictions).toBe(
        syncCache.getStats().evictions,
      );

      // Verify evicted pages were flushed to both backends
      for (let i = 0; i < 5; i++) {
        const asyncPage = await asyncBackend.readPage("/f", i);
        const syncPage = syncBackend.readPage("/f", i);
        if (asyncPage !== null || syncPage !== null) {
          // Both should either have the page or not
          expect(asyncPage !== null).toBe(syncPage !== null);
          if (asyncPage && syncPage) {
            expect(asyncPage).toEqual(syncPage);
          }
        }
      }
    });

    it("evictFile produces same cache state", async () => {
      const data = new Uint8Array(PAGE_SIZE * 2);
      data.fill(0xaa);
      await asyncCache.write("/f", data, 0, data.length, 0, 0);
      syncCache.write("/f", data, 0, data.length, 0, 0);

      await asyncCache.evictFile("/f");
      syncCache.evictFile("/f");

      expect(asyncCache.size).toBe(syncCache.size);
      expect(asyncCache.size).toBe(0);
      expect(asyncCache.dirtyCount).toBe(syncCache.dirtyCount);

      // Data should have been flushed to both backends
      const asyncP0 = await asyncBackend.readPage("/f", 0);
      const syncP0 = syncBackend.readPage("/f", 0);
      expect(asyncP0).toEqual(syncP0);
    });
  });

  describe("invalidatePagesFrom parity", () => {
    it("@fast invalidates same pages", async () => {
      const data = new Uint8Array(PAGE_SIZE * 3);
      data.fill(0xaa);
      await asyncCache.write("/f", data, 0, data.length, 0, 0);
      syncCache.write("/f", data, 0, data.length, 0, 0);

      asyncCache.invalidatePagesFrom("/f", 1);
      syncCache.invalidatePagesFrom("/f", 1);

      expect(asyncCache.size).toBe(syncCache.size);
      expect(asyncCache.has("/f", 0)).toBe(syncCache.has("/f", 0));
      expect(asyncCache.has("/f", 1)).toBe(syncCache.has("/f", 1));
      expect(asyncCache.has("/f", 2)).toBe(syncCache.has("/f", 2));
      expect(asyncCache.has("/f", 0)).toBe(true);
      expect(asyncCache.has("/f", 1)).toBe(false);
    });
  });

  describe("@fast zeroTailAfterTruncate parity", () => {
    it("zeros tail bytes identically", async () => {
      // Write a full page
      const data = new Uint8Array(PAGE_SIZE);
      data.fill(0xff);
      await asyncCache.write("/f", data, 0, PAGE_SIZE, 0, 0);
      syncCache.write("/f", data, 0, PAGE_SIZE, 0, 0);

      // Truncate to half-page
      const newSize = PAGE_SIZE / 2;
      await asyncCache.zeroTailAfterTruncate("/f", newSize);
      syncCache.zeroTailAfterTruncate("/f", newSize);

      // Read back and compare
      const asyncBuf = new Uint8Array(PAGE_SIZE);
      const syncBuf = new Uint8Array(PAGE_SIZE);
      await asyncCache.read("/f", asyncBuf, 0, PAGE_SIZE, 0, PAGE_SIZE);
      syncCache.read("/f", syncBuf, 0, PAGE_SIZE, 0, PAGE_SIZE);

      expect(asyncBuf).toEqual(syncBuf);
      // First half should be 0xff, second half should be 0x00
      expect(asyncBuf[newSize - 1]).toBe(0xff);
      expect(asyncBuf[newSize]).toBe(0x00);
    });
  });

  describe("deleteFile parity", () => {
    it("@fast removes pages from cache and backend identically", async () => {
      const data = new Uint8Array(PAGE_SIZE * 2);
      data.fill(0x33);
      await asyncCache.write("/f", data, 0, data.length, 0, 0);
      syncCache.write("/f", data, 0, data.length, 0, 0);

      await asyncCache.deleteFile("/f");
      syncCache.deleteFile("/f");

      expect(asyncCache.size).toBe(syncCache.size);
      expect(asyncCache.dirtyCount).toBe(syncCache.dirtyCount);
      expect(asyncCache.has("/f", 0)).toBe(false);
      expect(syncCache.has("/f", 0)).toBe(false);

      expect(await asyncBackend.readPage("/f", 0)).toBeNull();
      expect(syncBackend.readPage("/f", 0)).toBeNull();
    });
  });

  describe("renameFile parity", () => {
    it("@fast moves cache entries identically", async () => {
      const data = new Uint8Array(PAGE_SIZE);
      data.fill(0x55);
      await asyncCache.write("/old", data, 0, PAGE_SIZE, 0, 0);
      syncCache.write("/old", data, 0, PAGE_SIZE, 0, 0);

      await asyncCache.renameFile("/old", "/new");
      syncCache.renameFile("/old", "/new");

      expect(asyncCache.has("/old", 0)).toBe(false);
      expect(syncCache.has("/old", 0)).toBe(false);
      expect(asyncCache.has("/new", 0)).toBe(true);
      expect(syncCache.has("/new", 0)).toBe(true);

      const asyncBuf = new Uint8Array(PAGE_SIZE);
      const syncBuf = new Uint8Array(PAGE_SIZE);
      await asyncCache.read("/new", asyncBuf, 0, PAGE_SIZE, 0, PAGE_SIZE);
      syncCache.read("/new", syncBuf, 0, PAGE_SIZE, 0, PAGE_SIZE);
      expect(asyncBuf).toEqual(syncBuf);
    });

    it("rename overwrites destination identically", async () => {
      // Write different data to src and dest
      const srcData = new Uint8Array(PAGE_SIZE);
      srcData.fill(0xaa);
      const dstData = new Uint8Array(PAGE_SIZE);
      dstData.fill(0xbb);

      await asyncCache.write("/src", srcData, 0, PAGE_SIZE, 0, 0);
      syncCache.write("/src", srcData, 0, PAGE_SIZE, 0, 0);
      await asyncCache.write("/dst", dstData, 0, PAGE_SIZE, 0, 0);
      syncCache.write("/dst", dstData, 0, PAGE_SIZE, 0, 0);

      await asyncCache.renameFile("/src", "/dst");
      syncCache.renameFile("/src", "/dst");

      const asyncBuf = new Uint8Array(PAGE_SIZE);
      const syncBuf = new Uint8Array(PAGE_SIZE);
      await asyncCache.read("/dst", asyncBuf, 0, PAGE_SIZE, 0, PAGE_SIZE);
      syncCache.read("/dst", syncBuf, 0, PAGE_SIZE, 0, PAGE_SIZE);

      expect(asyncBuf).toEqual(syncBuf);
      expect(asyncBuf[0]).toBe(0xaa); // src data, not dst
    });

    it("rename no-op for same path", async () => {
      const data = new Uint8Array(PAGE_SIZE);
      data.fill(0x77);
      await asyncCache.write("/f", data, 0, PAGE_SIZE, 0, 0);
      syncCache.write("/f", data, 0, PAGE_SIZE, 0, 0);

      await asyncCache.renameFile("/f", "/f");
      syncCache.renameFile("/f", "/f");

      expect(asyncCache.has("/f", 0)).toBe(true);
      expect(syncCache.has("/f", 0)).toBe(true);
    });
  });

  describe("@fast markPageDirty parity", () => {
    it("marks identical dirty state", async () => {
      await asyncCache.markPageDirty("/f", 0);
      syncCache.markPageDirty("/f", 0);

      expect(asyncCache.isDirty("/f", 0)).toBe(syncCache.isDirty("/f", 0));
      expect(asyncCache.dirtyCount).toBe(syncCache.dirtyCount);
    });
  });

  describe("stats parity", () => {
    it("@fast identical stats after mixed operations", async () => {
      // Seed backend
      for (let i = 0; i < 6; i++) {
        await seedPage("/f", i, 0x10 + i);
      }

      // Access pattern: sequential read causing evictions
      for (let i = 0; i < 6; i++) {
        await asyncCache.getPage("/f", i);
        syncCache.getPage("/f", i);
      }
      // Re-access early pages (cache misses since evicted)
      for (let i = 0; i < 2; i++) {
        await asyncCache.getPage("/f", i);
        syncCache.getPage("/f", i);
      }

      expect(asyncCache.getStats()).toEqual(syncCache.getStats());
    });

    it("resetStats works identically", async () => {
      await asyncCache.getPage("/f", 0);
      syncCache.getPage("/f", 0);

      asyncCache.resetStats();
      syncCache.resetStats();

      expect(asyncCache.getStats()).toEqual(syncCache.getStats());
      expect(asyncCache.getStats().hits).toBe(0);
    });
  });

  describe("write skip-backend-read parity", () => {
    it("@fast skips read for pages beyond file extent identically", async () => {
      // Write beyond current file extent — both should skip backend read
      const data = new Uint8Array(PAGE_SIZE);
      data.fill(0xcc);

      const asyncResult = await asyncCache.write(
        "/f", data, 0, PAGE_SIZE, PAGE_SIZE * 2, 0,
      );
      const syncResult = syncCache.write(
        "/f", data, 0, PAGE_SIZE, PAGE_SIZE * 2, 0,
      );

      expect(asyncResult).toEqual(syncResult);
      expect(asyncCache.getStats()).toEqual(syncCache.getStats());
    });

    it("skips read for fully overwritten page identically", async () => {
      // Seed a page in both backends
      await seedPage("/f", 0, 0x11);

      // Overwrite entire page — should skip the backend read
      const data = new Uint8Array(PAGE_SIZE);
      data.fill(0x22);

      await asyncCache.write("/f", data, 0, PAGE_SIZE, 0, PAGE_SIZE);
      syncCache.write("/f", data, 0, PAGE_SIZE, 0, PAGE_SIZE);

      expect(asyncCache.getStats()).toEqual(syncCache.getStats());
      // Cache miss is counted (page wasn't in cache) but backend read
      // is skipped because the entire page is overwritten. The key
      // assertion is that both caches report the same stats.
      expect(asyncCache.getStats().misses).toBe(1);
    });
  });

  describe("complex operation sequence parity", () => {
    it("write-read-truncate-extend-rename sequence", async () => {
      // 1. Write 3 pages
      const data = new Uint8Array(PAGE_SIZE * 3);
      for (let i = 0; i < data.length; i++) data[i] = i & 0xff;
      await asyncCache.write("/f", data, 0, data.length, 0, 0);
      syncCache.write("/f", data, 0, data.length, 0, 0);

      // 2. Truncate to 1 page
      asyncCache.invalidatePagesFrom("/f", 1);
      syncCache.invalidatePagesFrom("/f", 1);
      await asyncCache.zeroTailAfterTruncate("/f", PAGE_SIZE);
      syncCache.zeroTailAfterTruncate("/f", PAGE_SIZE);

      // 3. Write beyond truncation point (extend)
      const ext = new Uint8Array(100);
      ext.fill(0xdd);
      await asyncCache.write("/f", ext, 0, 100, PAGE_SIZE * 2, PAGE_SIZE);
      syncCache.write("/f", ext, 0, 100, PAGE_SIZE * 2, PAGE_SIZE);

      // 4. Rename
      await asyncCache.renameFile("/f", "/g");
      syncCache.renameFile("/f", "/g");

      // 5. Read back and verify parity
      const asyncBuf = new Uint8Array(PAGE_SIZE * 3);
      const syncBuf = new Uint8Array(PAGE_SIZE * 3);
      await asyncCache.read("/g", asyncBuf, 0, asyncBuf.length, 0, PAGE_SIZE * 3);
      syncCache.read("/g", syncBuf, 0, syncBuf.length, 0, PAGE_SIZE * 3);

      expect(asyncBuf).toEqual(syncBuf);
      expect(asyncCache.getStats()).toEqual(syncCache.getStats());
    });

    it("interleaved multi-file operations", async () => {
      const buf = new Uint8Array(PAGE_SIZE);

      // Interleave writes to multiple files
      for (let i = 0; i < 3; i++) {
        buf.fill(0x10 * (i + 1));
        await asyncCache.write(`/f${i}`, buf, 0, PAGE_SIZE, 0, 0);
        syncCache.write(`/f${i}`, buf, 0, PAGE_SIZE, 0, 0);
      }

      // Flush one file
      await asyncCache.flushFile("/f1");
      syncCache.flushFile("/f1");

      // Delete another
      await asyncCache.deleteFile("/f2");
      syncCache.deleteFile("/f2");

      // Read remaining
      const asyncBuf = new Uint8Array(PAGE_SIZE);
      const syncBuf = new Uint8Array(PAGE_SIZE);
      await asyncCache.read("/f0", asyncBuf, 0, PAGE_SIZE, 0, PAGE_SIZE);
      syncCache.read("/f0", syncBuf, 0, PAGE_SIZE, 0, PAGE_SIZE);

      expect(asyncBuf).toEqual(syncBuf);
      expect(asyncCache.size).toBe(syncCache.size);
      expect(asyncCache.dirtyCount).toBe(syncCache.dirtyCount);
      expect(asyncCache.getStats()).toEqual(syncCache.getStats());
    });
  });

  describe("addDirtyKey parity", () => {
    it("@fast registers dirty state identically", async () => {
      // Get a page in both caches
      const asyncPage = await asyncCache.getPage("/f", 0);
      const syncPage = syncCache.getPage("/f", 0);

      // Manually mark dirty and register via addDirtyKey
      asyncPage.dirty = true;
      asyncCache.addDirtyKey(asyncPage.key, asyncPage.path);
      syncPage.dirty = true;
      syncCache.addDirtyKey(syncPage.key, syncPage.path);

      expect(asyncCache.dirtyCount).toBe(syncCache.dirtyCount);
      expect(asyncCache.isDirty("/f", 0)).toBe(syncCache.isDirty("/f", 0));
    });
  });

  describe("capacity / size parity", () => {
    it("@fast reports same capacity and size", async () => {
      expect(asyncCache.capacity).toBe(syncCache.capacity);
      expect(asyncCache.size).toBe(syncCache.size);

      await asyncCache.getPage("/f", 0);
      syncCache.getPage("/f", 0);

      expect(asyncCache.size).toBe(syncCache.size);
    });
  });
});
