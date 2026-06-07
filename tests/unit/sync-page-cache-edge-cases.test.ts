/**
 * Edge case tests for SyncPageCache — targeting specific code paths
 * that the main sync-page-cache.test.ts doesn't exercise directly.
 *
 * Covers:
 * - Two-page write specialization (lines 375-402 of sync-page-cache.ts)
 * - MRU invalidation and recovery during eviction
 * - Buffer pool data isolation (zeroing on reuse)
 * - Self-rename no-op
 * - deleteFile discards dirty pages without flushing
 * - zeroTailAfterTruncate page-boundary edge case
 * - Stats accuracy for two-page write false-hit compensation
 */

import { describe, it, expect, beforeEach } from "vitest";
import { SyncPageCache } from "../../src/sync-page-cache.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { PAGE_SIZE } from "../../src/types.js";

describe("SyncPageCache edge cases", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  describe("two-page write specialization", () => {
    it("@fast write spanning exactly two pages produces correct data", () => {
      const cache = new SyncPageCache(backend, 8);
      const writeSize = 256;
      const position = PAGE_SIZE - 128; // straddles page 0→1 boundary
      const data = new Uint8Array(writeSize);
      for (let i = 0; i < writeSize; i++) data[i] = (i * 13) & 0xff;

      const result = cache.write("/file", data, 0, writeSize, position, 0);
      expect(result.bytesWritten).toBe(writeSize);
      expect(result.newFileSize).toBe(position + writeSize);

      const buf = new Uint8Array(writeSize);
      cache.read("/file", buf, 0, writeSize, position, result.newFileSize);
      expect(buf).toEqual(data);
    });

    it("two-page write marks both pages dirty", () => {
      const cache = new SyncPageCache(backend, 8);
      const data = new Uint8Array(256);
      data.fill(0xab);
      cache.write("/file", data, 0, 256, PAGE_SIZE - 128, 0);

      expect(cache.isDirty("/file", 0)).toBe(true);
      expect(cache.isDirty("/file", 1)).toBe(true);
      expect(cache.dirtyCount).toBe(2);
    });

    it("two-page write with existing data preserves unwritten regions", () => {
      const cache = new SyncPageCache(backend, 8);

      // Fill two pages with known data
      const page0 = new Uint8Array(PAGE_SIZE).fill(0xaa);
      const page1 = new Uint8Array(PAGE_SIZE).fill(0xbb);
      cache.write("/file", page0, 0, PAGE_SIZE, 0, 0);
      cache.write("/file", page1, 0, PAGE_SIZE, PAGE_SIZE, PAGE_SIZE);
      cache.flushAll();

      // Write 64 bytes straddling the boundary: last 32 of page 0, first 32 of page 1
      const crossData = new Uint8Array(64).fill(0xcc);
      cache.write("/file", crossData, 0, 64, PAGE_SIZE - 32, PAGE_SIZE * 2);

      // Verify page 0: first (PAGE_SIZE-32) bytes = 0xaa, last 32 = 0xcc
      const buf0 = new Uint8Array(PAGE_SIZE);
      cache.read("/file", buf0, 0, PAGE_SIZE, 0, PAGE_SIZE * 2);
      for (let i = 0; i < PAGE_SIZE - 32; i++) {
        expect(buf0[i]).toBe(0xaa);
      }
      for (let i = PAGE_SIZE - 32; i < PAGE_SIZE; i++) {
        expect(buf0[i]).toBe(0xcc);
      }

      // Verify page 1: first 32 bytes = 0xcc, rest = 0xbb
      const buf1 = new Uint8Array(PAGE_SIZE);
      cache.read("/file", buf1, 0, PAGE_SIZE, PAGE_SIZE, PAGE_SIZE * 2);
      for (let i = 0; i < 32; i++) {
        expect(buf1[i]).toBe(0xcc);
      }
      for (let i = 32; i < PAGE_SIZE; i++) {
        expect(buf1[i]).toBe(0xbb);
      }
    });

    it("two-page write evicts and reloads correctly from backend @fast", () => {
      const cache = new SyncPageCache(backend, 8);

      // Write two pages and flush+evict to force backend round-trip
      const page0 = new Uint8Array(PAGE_SIZE).fill(0xdd);
      const page1 = new Uint8Array(PAGE_SIZE).fill(0xee);
      cache.write("/file", page0, 0, PAGE_SIZE, 0, 0);
      cache.write("/file", page1, 0, PAGE_SIZE, PAGE_SIZE, PAGE_SIZE);
      cache.flushAll();
      cache.evictFile("/file");

      // Two-page write spanning boundary (pages not in cache → reloaded)
      const crossData = new Uint8Array(100).fill(0xff);
      cache.write("/file", crossData, 0, 100, PAGE_SIZE - 50, PAGE_SIZE * 2);

      // Flush and evict again
      cache.flushAll();
      cache.evictFile("/file");

      // Re-read from backend and verify
      const fullBuf = new Uint8Array(PAGE_SIZE * 2);
      cache.read("/file", fullBuf, 0, PAGE_SIZE * 2, 0, PAGE_SIZE * 2);

      // Page 0: 0xdd except last 50 bytes = 0xff
      for (let i = 0; i < PAGE_SIZE - 50; i++) {
        expect(fullBuf[i]).toBe(0xdd);
      }
      for (let i = PAGE_SIZE - 50; i < PAGE_SIZE; i++) {
        expect(fullBuf[i]).toBe(0xff);
      }
      // Page 1: first 50 bytes = 0xff, rest = 0xee
      for (let i = 0; i < 50; i++) {
        expect(fullBuf[PAGE_SIZE + i]).toBe(0xff);
      }
      for (let i = 50; i < PAGE_SIZE; i++) {
        expect(fullBuf[PAGE_SIZE + i]).toBe(0xee);
      }
    });

    it("two-page write stats: no false hits when both pages are cache misses", () => {
      const cache = new SyncPageCache(backend, 8);

      // Put 2 pages in backend
      const p0 = new Uint8Array(PAGE_SIZE).fill(0x11);
      const p1 = new Uint8Array(PAGE_SIZE).fill(0x22);
      backend.writePage("/file", 0, p0);
      backend.writePage("/file", 1, p1);

      cache.resetStats();

      // Two-page write: both pages are cache misses and need backend read
      const crossData = new Uint8Array(200).fill(0x33);
      cache.write("/file", crossData, 0, 200, PAGE_SIZE - 100, PAGE_SIZE * 2);

      const stats = cache.getStats();
      // Both pages were loaded from backend (2 misses from batch + 0 from two-page path)
      // Batch path counts 2 misses, two-page path finds them cached (2 hits),
      // then compensation subtracts 2 false hits
      expect(stats.hits).toBe(0);
      expect(stats.misses).toBe(2);
    });

    it("two-page write extending file skips backend read for new page", () => {
      let readPageCalls = 0;
      const origReadPage = backend.readPage.bind(backend);
      backend.readPage = (path: string, pageIndex: number) => {
        readPageCalls++;
        return origReadPage(path, pageIndex);
      };

      const cache = new SyncPageCache(backend, 8);

      // Write 100 bytes to establish file extent (within page 0)
      cache.write("/file", new Uint8Array(100).fill(0xaa), 0, 100, 0, 0);
      cache.flushAll();
      cache.evictFile("/file");
      readPageCalls = 0;

      // Two-page write: page 0 has data, page 1 is new (beyond extent)
      const crossData = new Uint8Array(200).fill(0xbb);
      cache.write("/file", crossData, 0, 200, PAGE_SIZE - 100, 100);

      // Page 0 needs read (partial overwrite of existing page)
      // Page 1 should NOT be read (beyond file extent)
      expect(readPageCalls).toBe(1);
    });
  });

  describe("MRU invalidation and recovery", () => {
    it("@fast MRU is cleared on eviction and rebuilt on next access", () => {
      const cache = new SyncPageCache(backend, 2);

      // Access page 0, then page 1 — page 1 becomes MRU
      cache.getPage("/file", 0);
      cache.getPage("/file", 1);

      // Access page 0 again — it's an LRU reorder (not MRU fast path)
      // Then access page 2 — evicts page 1 (the previous MRU)
      cache.getPage("/file", 0);
      cache.getPage("/file", 2);

      // MRU was cleared when page 1 was evicted. Now page 2 should be MRU.
      // Accessing page 2 again should hit the MRU fast path (no key construction)
      cache.resetStats();
      cache.getPage("/file", 2);
      expect(cache.getStats().hits).toBe(1);
    });

    it("MRU cleared by deleteFile, rebuilt on next getPage", () => {
      const cache = new SyncPageCache(backend, 4);

      cache.getPage("/file", 0);
      // MRU is now /file:0
      cache.deleteFile("/file");

      // MRU should be null. Next access creates new MRU.
      cache.getPage("/other", 0);
      cache.resetStats();
      cache.getPage("/other", 0);
      expect(cache.getStats().hits).toBe(1);
    });

    it("MRU cleared by evictFile, rebuilt on next getPage", () => {
      const cache = new SyncPageCache(backend, 4);

      cache.getPage("/file", 0);
      cache.evictFile("/file");

      cache.getPage("/other", 0);
      cache.resetStats();
      cache.getPage("/other", 0);
      expect(cache.getStats().hits).toBe(1);
    });

    it("MRU cleared by invalidatePagesFrom when MRU page is invalidated", () => {
      const cache = new SyncPageCache(backend, 8);

      cache.getPage("/file", 0);
      cache.getPage("/file", 1);
      cache.getPage("/file", 2);
      // MRU is now /file:2

      // Invalidate from page 1 (removes pages 1 and 2, including MRU)
      cache.invalidatePagesFrom("/file", 1);

      // Page 0 should still be accessible
      cache.resetStats();
      cache.getPage("/file", 0);
      // This should be a regular cache hit (not MRU fast path, since MRU was cleared)
      expect(cache.getStats().hits).toBe(1);
    });

    it("@fast MRU survives invalidation when MRU page is not invalidated", () => {
      const cache = new SyncPageCache(backend, 8);

      cache.getPage("/file", 0);
      cache.getPage("/file", 1);
      cache.getPage("/file", 2);
      // MRU is /file:2

      // Invalidate from page 3 (doesn't touch MRU at page 2)
      cache.invalidatePagesFrom("/file", 3);

      // MRU fast path should still work for page 2
      cache.resetStats();
      cache.getPage("/file", 2);
      expect(cache.getStats().hits).toBe(1);
    });
  });

  describe("buffer pool data isolation", () => {
    it("@fast evicted buffer data does not leak into new page via pool", () => {
      const cache = new SyncPageCache(backend, 1);

      // Write distinctive data to a page
      const sensitiveData = new Uint8Array(PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) sensitiveData[i] = 0xde;
      cache.write("/secret", sensitiveData, 0, PAGE_SIZE, 0, 0);

      // Evict the page (buffer goes back to pool)
      cache.getPage("/other", 0);

      // Create a new page using getPageNoRead (acquires from pool)
      const newPage = cache.getPageNoRead("/new", 0);

      // New page must be zero-filled, not contain stale data
      for (let i = 0; i < PAGE_SIZE; i++) {
        expect(newPage.data[i]).toBe(0);
      }
    });

    it("buffer pool isolation holds across multiple eviction cycles", () => {
      const cache = new SyncPageCache(backend, 1);

      for (let cycle = 0; cycle < 5; cycle++) {
        // Write pattern to page
        const data = new Uint8Array(PAGE_SIZE);
        data.fill(0xa0 + cycle);
        cache.write(`/cycle${cycle}`, data, 0, PAGE_SIZE, 0, 0);

        // Evict by loading new file
        const nextPage = cache.getPageNoRead(`/cycle${cycle + 1}`, 0);

        // Verify new page is clean
        for (let i = 0; i < PAGE_SIZE; i++) {
          expect(nextPage.data[i]).toBe(0);
        }
      }
    });

    it("deleted file buffer does not leak into subsequent getPage", () => {
      const cache = new SyncPageCache(backend, 2);

      // Write data and delete the file (buffer goes to pool)
      const data = new Uint8Array(PAGE_SIZE);
      data.fill(0xba);
      cache.write("/deleted", data, 0, PAGE_SIZE, 0, 0);
      cache.deleteFile("/deleted");

      // Acquire a new page — should get zeroed buffer from pool
      const newPage = cache.getPage("/fresh", 0);
      for (let i = 0; i < PAGE_SIZE; i++) {
        expect(newPage.data[i]).toBe(0);
      }
    });
  });

  describe("renameFile edge cases", () => {
    it("@fast self-rename is a no-op", () => {
      const cache = new SyncPageCache(backend, 8);
      const data = new Uint8Array([1, 2, 3, 4, 5]);
      cache.write("/file", data, 0, 5, 0, 0);

      cache.renameFile("/file", "/file");

      // Data should be intact
      expect(cache.has("/file", 0)).toBe(true);
      const buf = new Uint8Array(5);
      cache.read("/file", buf, 0, 5, 0, 5);
      expect(buf).toEqual(data);
    });

    it("rename flushes dirty pages before re-keying @fast", () => {
      const cache = new SyncPageCache(backend, 8);
      const data = new Uint8Array(PAGE_SIZE).fill(0x42);
      cache.write("/old", data, 0, PAGE_SIZE, 0, 0);
      expect(cache.isDirty("/old", 0)).toBe(true);

      cache.renameFile("/old", "/new");

      // renameFile flushes dirty pages before re-keying, so page is clean
      expect(cache.isDirty("/old", 0)).toBe(false);
      expect(cache.isDirty("/new", 0)).toBe(false);
      expect(cache.dirtyCount).toBe(0);

      // Backend should have data under new path (flushed + re-keyed)
      const stored = backend.readPage("/new", 0);
      expect(stored).not.toBeNull();
      expect(stored![0]).toBe(0x42);

      // Data is readable from cache under new path
      const buf = new Uint8Array(PAGE_SIZE);
      cache.read("/new", buf, 0, PAGE_SIZE, 0, PAGE_SIZE);
      expect(buf[0]).toBe(0x42);
    });

    it("rename clears MRU when source page was MRU", () => {
      const cache = new SyncPageCache(backend, 8);
      cache.getPage("/old", 0);
      // MRU is now /old:0

      cache.renameFile("/old", "/new");

      // Accessing /new:0 should work (it was re-keyed in the cache)
      const page = cache.getPage("/new", 0);
      expect(page).toBeDefined();
      expect(cache.has("/new", 0)).toBe(true);
    });
  });

  describe("deleteFile dirty page handling", () => {
    it("@fast deleteFile discards dirty pages without flushing to backend", () => {
      const cache = new SyncPageCache(backend, 8);

      // Write dirty data
      const data = new Uint8Array(PAGE_SIZE).fill(0xfe);
      cache.write("/file", data, 0, PAGE_SIZE, 0, 0);
      expect(cache.isDirty("/file", 0)).toBe(true);

      // Delete file — dirty data should be discarded, not flushed
      cache.deleteFile("/file");

      // Backend should have no page data for this file
      // (deleteFile calls backend.deleteFile which deletes everything,
      // but the dirty page should NOT have been flushed first)
      expect(cache.has("/file", 0)).toBe(false);
      expect(cache.dirtyCount).toBe(0);
    });

    it("deleteFile with multiple dirty pages clears all dirty tracking", () => {
      const cache = new SyncPageCache(backend, 8);

      // Write 3 dirty pages
      for (let i = 0; i < 3; i++) {
        const data = new Uint8Array(PAGE_SIZE).fill(i + 1);
        cache.write("/file", data, 0, PAGE_SIZE, i * PAGE_SIZE, i * PAGE_SIZE);
      }
      expect(cache.dirtyCount).toBe(3);

      cache.deleteFile("/file");

      expect(cache.dirtyCount).toBe(0);
      expect(cache.size).toBe(0);
    });

    it("deleteFile does not affect other files' dirty state", () => {
      const cache = new SyncPageCache(backend, 8);

      cache.write("/keep", new Uint8Array(PAGE_SIZE).fill(0xaa), 0, PAGE_SIZE, 0, 0);
      cache.write("/delete", new Uint8Array(PAGE_SIZE).fill(0xbb), 0, PAGE_SIZE, 0, 0);

      cache.deleteFile("/delete");

      expect(cache.isDirty("/keep", 0)).toBe(true);
      expect(cache.dirtyCount).toBe(1);
    });
  });

  describe("zeroTailAfterTruncate edge cases", () => {
    it("@fast page-aligned truncate is a no-op (tailOffset === 0)", () => {
      const cache = new SyncPageCache(backend, 4);
      const data = new Uint8Array(PAGE_SIZE).fill(0xff);
      cache.write("/file", data, 0, PAGE_SIZE, 0, 0);
      cache.flushAll();

      // Truncate to exactly PAGE_SIZE — no tail to zero
      cache.zeroTailAfterTruncate("/file", PAGE_SIZE);

      // Page should be unchanged
      const page = cache.getPage("/file", 0);
      for (let i = 0; i < PAGE_SIZE; i++) {
        expect(page.data[i]).toBe(0xff);
      }
    });

    it("truncate to 0 is a no-op (tailOffset === 0)", () => {
      const cache = new SyncPageCache(backend, 4);
      const data = new Uint8Array(PAGE_SIZE).fill(0xff);
      cache.write("/file", data, 0, PAGE_SIZE, 0, 0);
      cache.flushAll();

      cache.zeroTailAfterTruncate("/file", 0);

      // Should not have modified any page
      const page = cache.getPage("/file", 0);
      expect(page.data[0]).toBe(0xff);
    });

    it("truncate to 1 byte zeros rest of first page", () => {
      const cache = new SyncPageCache(backend, 4);
      const data = new Uint8Array(PAGE_SIZE).fill(0xff);
      cache.write("/file", data, 0, PAGE_SIZE, 0, 0);

      cache.zeroTailAfterTruncate("/file", 1);

      const page = cache.getPage("/file", 0);
      expect(page.data[0]).toBe(0xff);
      for (let i = 1; i < PAGE_SIZE; i++) {
        expect(page.data[i]).toBe(0);
      }
      expect(page.dirty).toBe(true);
    });

    it("truncate mid-second-page zeros tail of that page only", () => {
      const cache = new SyncPageCache(backend, 4);
      const data = new Uint8Array(PAGE_SIZE * 2).fill(0xff);
      cache.write("/file", data, 0, PAGE_SIZE * 2, 0, 0);

      const truncSize = PAGE_SIZE + 500;
      cache.zeroTailAfterTruncate("/file", truncSize);

      // Page 0 should be untouched (truncate targets page 1)
      const page0 = cache.getPage("/file", 0);
      for (let i = 0; i < PAGE_SIZE; i++) {
        expect(page0.data[i]).toBe(0xff);
      }

      // Page 1: first 500 bytes = 0xff, rest = 0x00
      const page1 = cache.getPage("/file", 1);
      for (let i = 0; i < 500; i++) {
        expect(page1.data[i]).toBe(0xff);
      }
      for (let i = 500; i < PAGE_SIZE; i++) {
        expect(page1.data[i]).toBe(0);
      }
    });
  });

  describe("assertInvariants under edge conditions", () => {
    it("invariants hold after interleaved operations on multiple files @fast", () => {
      const cache = new SyncPageCache(backend, 4);

      // Write to multiple files
      cache.write("/a", new Uint8Array(PAGE_SIZE).fill(1), 0, PAGE_SIZE, 0, 0);
      cache.write("/b", new Uint8Array(PAGE_SIZE).fill(2), 0, PAGE_SIZE, 0, 0);
      cache.write("/c", new Uint8Array(PAGE_SIZE).fill(3), 0, PAGE_SIZE, 0, 0);
      cache.assertInvariants();

      // Flush one, delete another, rename the third
      cache.flushFile("/a");
      cache.assertInvariants();
      cache.deleteFile("/b");
      cache.assertInvariants();
      cache.renameFile("/c", "/d");
      cache.assertInvariants();

      // Write more, trigger eviction
      cache.write("/e", new Uint8Array(PAGE_SIZE).fill(4), 0, PAGE_SIZE, 0, 0);
      cache.write("/f", new Uint8Array(PAGE_SIZE).fill(5), 0, PAGE_SIZE, 0, 0);
      cache.write("/g", new Uint8Array(PAGE_SIZE).fill(6), 0, PAGE_SIZE, 0, 0);
      cache.write("/h", new Uint8Array(PAGE_SIZE).fill(7), 0, PAGE_SIZE, 0, 0);
      cache.assertInvariants();
    });

    it("invariants hold after truncation + re-extension cycle", () => {
      const cache = new SyncPageCache(backend, 8);

      // Write 4 pages
      for (let i = 0; i < 4; i++) {
        cache.write(
          "/file",
          new Uint8Array(PAGE_SIZE).fill(i + 1),
          0,
          PAGE_SIZE,
          i * PAGE_SIZE,
          i * PAGE_SIZE,
        );
      }
      cache.assertInvariants();

      // Truncate to 1 page
      cache.invalidatePagesFrom("/file", 1);
      cache.zeroTailAfterTruncate("/file", 100);
      cache.assertInvariants();

      // Re-extend with new data
      cache.write(
        "/file",
        new Uint8Array(PAGE_SIZE * 3).fill(0xfe),
        0,
        PAGE_SIZE * 3,
        PAGE_SIZE,
        100,
      );
      cache.assertInvariants();
    });

    it("invariants hold after collect→commit cycle", () => {
      const cache = new SyncPageCache(backend, 8);

      cache.write("/a", new Uint8Array(PAGE_SIZE).fill(1), 0, PAGE_SIZE, 0, 0);
      cache.write("/b", new Uint8Array(PAGE_SIZE).fill(2), 0, PAGE_SIZE, 0, 0);

      const collected = cache.collectDirtyPages();
      cache.assertInvariants();

      backend.writePages(collected);
      cache.commitDirtyPages(collected);
      cache.assertInvariants();

      expect(cache.dirtyCount).toBe(0);
    });
  });

  describe("multi-page write with batch pre-load and eviction pressure", () => {
    it("3-page write into tiny cache preserves all data", () => {
      const cache = new SyncPageCache(backend, 2);

      // Write 3 pages into a 2-page cache
      const size = PAGE_SIZE * 3;
      const data = new Uint8Array(size);
      for (let i = 0; i < size; i++) data[i] = (i * 17) & 0xff;

      cache.write("/file", data, 0, size, 0, 0);

      // Read back — earlier pages must come from backend
      const buf = new Uint8Array(size);
      cache.read("/file", buf, 0, size, 0, size);
      expect(buf).toEqual(data);
    });

    it("multi-page write skips reads for fully-overwritten pages in batch", () => {
      let readPagesIndices: number[] = [];
      const origReadPages = backend.readPages.bind(backend);
      backend.readPages = (path: string, pageIndices: number[]) => {
        readPagesIndices = [...pageIndices];
        return origReadPages(path, pageIndices);
      };

      const cache = new SyncPageCache(backend, 16);

      // Establish 4 pages of data
      const init = new Uint8Array(PAGE_SIZE * 4);
      init.fill(0xaa);
      cache.write("/file", init, 0, init.length, 0, 0);
      cache.flushAll();
      cache.evictFile("/file");
      readPagesIndices = [];

      // Page-aligned overwrite of all 4 pages — none need backend reads
      const overwrite = new Uint8Array(PAGE_SIZE * 4);
      overwrite.fill(0xbb);
      cache.write("/file", overwrite, 0, overwrite.length, 0, PAGE_SIZE * 4);

      // No pages should have been batch-loaded (all fully overwritten)
      expect(readPagesIndices).toEqual([]);
    });
  });

  describe("addDirtyKey", () => {
    it("@fast registers page as dirty without page lookup", () => {
      const cache = new SyncPageCache(backend, 8);
      const page = cache.getPage("/file", 0);

      page.dirty = true;
      cache.addDirtyKey(page.key, "/file");

      expect(cache.isDirty("/file", 0)).toBe(true);
      expect(cache.dirtyCount).toBe(1);
    });

    it("dirty state persists through flush cycle after addDirtyKey", () => {
      const cache = new SyncPageCache(backend, 8);
      const page = cache.getPage("/file", 0);
      page.data.fill(0x99);
      page.dirty = true;
      cache.addDirtyKey(page.key, "/file");

      cache.flushFile("/file");
      expect(cache.isDirty("/file", 0)).toBe(false);

      const stored = backend.readPage("/file", 0);
      expect(stored).not.toBeNull();
      expect(stored![0]).toBe(0x99);
    });
  });
});
