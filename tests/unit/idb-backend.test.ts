/**
 * IdbBackend tests — exercises the StorageBackend interface over IndexedDB.
 *
 * Uses fake-indexeddb to provide a standards-compliant IDB implementation
 * in Node.js. This is a fake (not a mock) per project conventions.
 */

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import "fake-indexeddb/auto";
import { IdbBackend } from "../../src/idb-backend.js";
import { PAGE_SIZE } from "../../src/types.js";

/** Create a page filled with a repeating byte value. */
function filledPage(value: number): Uint8Array {
  const page = new Uint8Array(PAGE_SIZE);
  page.fill(value);
  return page;
}

/** Create a small buffer with sequential byte values. */
function testData(length: number, start = 0): Uint8Array {
  const buf = new Uint8Array(length);
  for (let i = 0; i < length; i++) {
    buf[i] = (start + i) & 0xff;
  }
  return buf;
}

describe("IdbBackend", () => {
  let backend: IdbBackend;
  let dbCounter = 0;

  beforeEach(() => {
    // Use a unique db name per test to avoid cross-test contamination
    backend = new IdbBackend({ dbName: `tomefs-test-${dbCounter++}` });
  });

  afterEach(async () => {
    await backend.destroy();
  });

  // -------------------------------------------------------------------
  // Page read/write
  // -------------------------------------------------------------------

  describe("page operations", () => {
    it("readPage returns null for non-existent page", async () => {
      const result = await backend.readPage("/test", 0);
      expect(result).toBeNull();
    });

    it("writePage then readPage round-trips data", async () => {
      const data = filledPage(0xab);
      await backend.writePage("/file1", 0, data);

      const result = await backend.readPage("/file1", 0);
      expect(result).toEqual(data);
    });

    it("writePage stores a copy, not a reference", async () => {
      const data = filledPage(0x01);
      await backend.writePage("/file1", 0, data);

      // Mutate original
      data.fill(0xff);

      const result = await backend.readPage("/file1", 0);
      expect(result![0]).toBe(0x01);
    });

    it("readPage returns a copy, not a reference", async () => {
      await backend.writePage("/file1", 0, filledPage(0x42));

      const a = await backend.readPage("/file1", 0);
      const b = await backend.readPage("/file1", 0);
      a![0] = 0xff;

      expect(b![0]).toBe(0x42);
    });

    it("handles multiple pages for the same file", async () => {
      await backend.writePage("/file1", 0, filledPage(0x01));
      await backend.writePage("/file1", 1, filledPage(0x02));
      await backend.writePage("/file1", 2, filledPage(0x03));

      expect(await backend.readPage("/file1", 0)).toEqual(filledPage(0x01));
      expect(await backend.readPage("/file1", 1)).toEqual(filledPage(0x02));
      expect(await backend.readPage("/file1", 2)).toEqual(filledPage(0x03));
    });

    it("handles pages for different files independently", async () => {
      await backend.writePage("/a", 0, filledPage(0xaa));
      await backend.writePage("/b", 0, filledPage(0xbb));

      expect(await backend.readPage("/a", 0)).toEqual(filledPage(0xaa));
      expect(await backend.readPage("/b", 0)).toEqual(filledPage(0xbb));
    });

    it("overwrites existing page data", async () => {
      await backend.writePage("/file1", 0, filledPage(0x01));
      await backend.writePage("/file1", 0, filledPage(0x02));

      expect(await backend.readPage("/file1", 0)).toEqual(filledPage(0x02));
    });

    it("handles sub-page-size data", async () => {
      const small = testData(100);
      await backend.writePage("/file1", 0, small);

      const result = await backend.readPage("/file1", 0);
      expect(result).toEqual(small);
    });
  });

  // -------------------------------------------------------------------
  // Batch reads
  // -------------------------------------------------------------------

  describe("readPages (batch)", () => {
    it("reads multiple pages in one transaction", async () => {
      await backend.writePage("/test", 0, filledPage(0xaa));
      await backend.writePage("/test", 1, filledPage(0xbb));
      await backend.writePage("/test", 3, filledPage(0xdd));

      const results = await backend.readPages("/test", [0, 1, 2, 3]);
      expect(results).toHaveLength(4);
      expect(results[0]![0]).toBe(0xaa);
      expect(results[1]![0]).toBe(0xbb);
      expect(results[2]).toBeNull();
      expect(results[3]![0]).toBe(0xdd);
    });

    it("returns empty array for empty indices", async () => {
      const results = await backend.readPages("/test", []);
      expect(results).toEqual([]);
    });

    it("returns all nulls for non-existent file", async () => {
      const results = await backend.readPages("/missing", [0, 1, 2]);
      expect(results).toEqual([null, null, null]);
    });
  });

  // -------------------------------------------------------------------
  // Batch writes
  // -------------------------------------------------------------------

  describe("writePages (batch)", () => {
    it("writes zero pages without error", async () => {
      await backend.writePages([]);
    });

    it("writes multiple pages atomically", async () => {
      await backend.writePages([
        { path: "/file1", pageIndex: 0, data: filledPage(0x01) },
        { path: "/file1", pageIndex: 1, data: filledPage(0x02) },
        { path: "/file2", pageIndex: 0, data: filledPage(0x03) },
      ]);

      expect(await backend.readPage("/file1", 0)).toEqual(filledPage(0x01));
      expect(await backend.readPage("/file1", 1)).toEqual(filledPage(0x02));
      expect(await backend.readPage("/file2", 0)).toEqual(filledPage(0x03));
    });

    it("batch overwrite replaces existing pages", async () => {
      await backend.writePage("/file1", 0, filledPage(0xaa));

      await backend.writePages([
        { path: "/file1", pageIndex: 0, data: filledPage(0xbb) },
      ]);

      expect(await backend.readPage("/file1", 0)).toEqual(filledPage(0xbb));
    });
  });

  // -------------------------------------------------------------------
  // Delete operations
  // -------------------------------------------------------------------

  describe("deleteFile", () => {
    it("deletes all pages for a file", async () => {
      await backend.writePage("/file1", 0, filledPage(0x01));
      await backend.writePage("/file1", 1, filledPage(0x02));
      await backend.writePage("/file1", 2, filledPage(0x03));

      await backend.deleteFile("/file1");

      expect(await backend.readPage("/file1", 0)).toBeNull();
      expect(await backend.readPage("/file1", 1)).toBeNull();
      expect(await backend.readPage("/file1", 2)).toBeNull();
    });

    it("does not affect other files", async () => {
      await backend.writePage("/file1", 0, filledPage(0x01));
      await backend.writePage("/file2", 0, filledPage(0x02));

      await backend.deleteFile("/file1");

      expect(await backend.readPage("/file1", 0)).toBeNull();
      expect(await backend.readPage("/file2", 0)).toEqual(filledPage(0x02));
    });

    it("deleting non-existent file is a no-op", async () => {
      await backend.deleteFile("/nonexistent");
      // No error thrown
    });

    it("does not delete file with prefix match", async () => {
      await backend.writePage("/file1", 0, filledPage(0x01));
      await backend.writePage("/file10", 0, filledPage(0x10));

      await backend.deleteFile("/file1");

      expect(await backend.readPage("/file1", 0)).toBeNull();
      expect(await backend.readPage("/file10", 0)).toEqual(filledPage(0x10));
    });
  });

  describe("deletePagesFrom", () => {
    it("deletes pages at and beyond the given index", async () => {
      await backend.writePage("/file1", 0, filledPage(0x01));
      await backend.writePage("/file1", 1, filledPage(0x02));
      await backend.writePage("/file1", 2, filledPage(0x03));
      await backend.writePage("/file1", 3, filledPage(0x04));

      await backend.deletePagesFrom("/file1", 2);

      expect(await backend.readPage("/file1", 0)).toEqual(filledPage(0x01));
      expect(await backend.readPage("/file1", 1)).toEqual(filledPage(0x02));
      expect(await backend.readPage("/file1", 2)).toBeNull();
      expect(await backend.readPage("/file1", 3)).toBeNull();
    });

    it("deletePagesFrom(0) deletes all pages", async () => {
      await backend.writePage("/file1", 0, filledPage(0x01));
      await backend.writePage("/file1", 1, filledPage(0x02));

      await backend.deletePagesFrom("/file1", 0);

      expect(await backend.readPage("/file1", 0)).toBeNull();
      expect(await backend.readPage("/file1", 1)).toBeNull();
    });

    it("does not affect other files", async () => {
      await backend.writePage("/file1", 0, filledPage(0x01));
      await backend.writePage("/file1", 1, filledPage(0x02));
      await backend.writePage("/file2", 0, filledPage(0xaa));

      await backend.deletePagesFrom("/file1", 1);

      expect(await backend.readPage("/file2", 0)).toEqual(filledPage(0xaa));
    });
  });

  // -------------------------------------------------------------------
  // renameFile
  // -------------------------------------------------------------------

  describe("renameFile", () => {
    it("moves all pages from old path to new path", async () => {
      const d0 = new Uint8Array(PAGE_SIZE);
      d0[0] = 0xaa;
      const d1 = new Uint8Array(PAGE_SIZE);
      d1[0] = 0xbb;
      await backend.writePage("/old", 0, d0);
      await backend.writePage("/old", 1, d1);
      await backend.writePage("/other", 0, new Uint8Array(PAGE_SIZE));

      await backend.renameFile("/old", "/new");

      expect(await backend.readPage("/old", 0)).toBeNull();
      expect(await backend.readPage("/old", 1)).toBeNull();
      expect((await backend.readPage("/new", 0))![0]).toBe(0xaa);
      expect((await backend.readPage("/new", 1))![0]).toBe(0xbb);
      expect(await backend.readPage("/other", 0)).not.toBeNull();
    });

    it("is a no-op when old path has no pages", async () => {
      await backend.renameFile("/nonexistent", "/new");
      expect(await backend.readPage("/new", 0)).toBeNull();
    });

    it("overwrites existing pages at new path", async () => {
      const d0 = new Uint8Array(PAGE_SIZE);
      d0[0] = 0x11;
      await backend.writePage("/new", 0, d0);

      const d1 = new Uint8Array(PAGE_SIZE);
      d1[0] = 0x22;
      await backend.writePage("/old", 0, d1);

      await backend.renameFile("/old", "/new");

      expect((await backend.readPage("/new", 0))![0]).toBe(0x22);
      expect(await backend.readPage("/old", 0)).toBeNull();
    });

    it("cleans up extra destination pages when source has fewer pages", async () => {
      // Destination has 4 pages, source has 2 — extra pages must not survive.
      for (let i = 0; i < 4; i++) {
        await backend.writePage("/dest", i, filledPage(0xdd));
      }
      await backend.writePage("/src", 0, filledPage(0xaa));
      await backend.writePage("/src", 1, filledPage(0xbb));

      await backend.renameFile("/src", "/dest");

      expect(await backend.readPage("/dest", 0)).toEqual(filledPage(0xaa));
      expect(await backend.readPage("/dest", 1)).toEqual(filledPage(0xbb));
      // Orphan pages from old destination must be gone
      expect(await backend.readPage("/dest", 2)).toBeNull();
      expect(await backend.readPage("/dest", 3)).toBeNull();
      // Source is gone
      expect(await backend.readPage("/src", 0)).toBeNull();
      expect(await backend.readPage("/src", 1)).toBeNull();
    });
  });

  // -------------------------------------------------------------------
  // Metadata operations
  // -------------------------------------------------------------------

  describe("metadata", () => {
    const meta = { size: 8192, mode: 0o100644, ctime: 1000, mtime: 2000 };

    it("readMeta returns null for non-existent file", async () => {
      const result = await backend.readMeta("/test");
      expect(result).toBeNull();
    });

    it("writeMeta then readMeta round-trips", async () => {
      await backend.writeMeta("/file1", meta);
      const result = await backend.readMeta("/file1");
      expect(result).toEqual(meta);
    });

    it("writeMeta stores a copy", async () => {
      const m = { ...meta };
      await backend.writeMeta("/file1", m);
      m.size = 99999;

      const result = await backend.readMeta("/file1");
      expect(result!.size).toBe(8192);
    });

    it("overwrites existing metadata", async () => {
      await backend.writeMeta("/file1", meta);
      const updated = { ...meta, size: 16384, mtime: 3000 };
      await backend.writeMeta("/file1", updated);

      const result = await backend.readMeta("/file1");
      expect(result).toEqual(updated);
    });

    it("deleteMeta removes metadata", async () => {
      await backend.writeMeta("/file1", meta);
      await backend.deleteMeta("/file1");

      expect(await backend.readMeta("/file1")).toBeNull();
    });

    it("deleteMeta on non-existent file is a no-op", async () => {
      await backend.deleteMeta("/nonexistent");
    });

    it("writeMetas batch writes multiple metadata entries", async () => {
      const meta1 = { size: 100, mode: 0o100644, ctime: 1000, mtime: 2000 };
      const meta2 = { size: 200, mode: 0o100755, ctime: 3000, mtime: 4000 };

      await backend.writeMetas([
        { path: "/a", meta: meta1 },
        { path: "/b", meta: meta2 },
      ]);

      expect(await backend.readMeta("/a")).toEqual(meta1);
      expect(await backend.readMeta("/b")).toEqual(meta2);
    });

    it("writeMetas with empty array is a no-op", async () => {
      await backend.writeMetas([]);
    });

    it("writeMetas overwrites existing metadata", async () => {
      await backend.writeMeta("/a", meta);
      const updated = { ...meta, size: 99999, mtime: 5000 };
      await backend.writeMetas([{ path: "/a", meta: updated }]);

      expect(await backend.readMeta("/a")).toEqual(updated);
    });

    it("deleteMetas removes multiple metadata entries", async () => {
      await backend.writeMeta("/a", meta);
      await backend.writeMeta("/b", meta);
      await backend.writeMeta("/c", meta);

      await backend.deleteMetas(["/a", "/b"]);

      expect(await backend.readMeta("/a")).toBeNull();
      expect(await backend.readMeta("/b")).toBeNull();
      expect(await backend.readMeta("/c")).toEqual(meta);
    });

    it("deleteMetas with empty array is a no-op", async () => {
      await backend.deleteMetas([]);
    });

    it("deleteMetas on non-existent paths is a no-op", async () => {
      await backend.deleteMetas(["/nonexistent1", "/nonexistent2"]);
    });

    it("metadata and pages are independent", async () => {
      await backend.writeMeta("/file1", meta);
      await backend.writePage("/file1", 0, filledPage(0x42));

      await backend.deleteMeta("/file1");

      // Pages still exist after meta deletion
      expect(await backend.readPage("/file1", 0)).toEqual(filledPage(0x42));
    });
  });

  // -------------------------------------------------------------------
  // listFiles
  // -------------------------------------------------------------------

  describe("listFiles", () => {
    it("returns empty array when no files exist", async () => {
      const files = await backend.listFiles();
      expect(files).toEqual([]);
    });

    it("returns paths of files with metadata", async () => {
      await backend.writeMeta("/a", {
        size: 0,
        mode: 0o100644,
        ctime: 0,
        mtime: 0,
      });
      await backend.writeMeta("/b", {
        size: 0,
        mode: 0o100644,
        ctime: 0,
        mtime: 0,
      });

      const files = await backend.listFiles();
      expect(files.sort()).toEqual(["/a", "/b"]);
    });

    it("does not include files that only have pages (no meta)", async () => {
      await backend.writePage("/orphan", 0, filledPage(0x01));

      const files = await backend.listFiles();
      expect(files).toEqual([]);
    });

    it("reflects deletions", async () => {
      await backend.writeMeta("/a", {
        size: 0,
        mode: 0o100644,
        ctime: 0,
        mtime: 0,
      });
      await backend.writeMeta("/b", {
        size: 0,
        mode: 0o100644,
        ctime: 0,
        mtime: 0,
      });
      await backend.deleteMeta("/a");

      const files = await backend.listFiles();
      expect(files).toEqual(["/b"]);
    });
  });

  // -------------------------------------------------------------------
  // Integration: PageCache + IdbBackend
  // -------------------------------------------------------------------

  describe("integration with PageCache", () => {
    it("PageCache reads and writes through IdbBackend", async () => {
      // Lazy import to avoid circular deps at module level
      const { PageCache } = await import("../../src/page-cache.js");

      const cache = new PageCache(backend, 4);

      // Write through cache
      const data = new TextEncoder().encode("Hello, IDB!");
      await cache.write("/test", data, 0, data.length, 0, 0);

      // Flush to IDB
      await cache.flushFile("/test");

      // Verify data reached IDB
      const page = await backend.readPage("/test", 0);
      expect(page).not.toBeNull();
      expect(new TextDecoder().decode(page!.subarray(0, data.length))).toBe(
        "Hello, IDB!",
      );
    });

    it("PageCache survives eviction with IdbBackend", async () => {
      const { PageCache } = await import("../../src/page-cache.js");

      // Very small cache — forces eviction
      const cache = new PageCache(backend, 2);

      // Write 4 pages (2 will be evicted)
      for (let i = 0; i < 4; i++) {
        const page = filledPage(i + 1);
        await cache.write("/bigfile", page, 0, PAGE_SIZE, i * PAGE_SIZE, i * PAGE_SIZE);
      }

      // Read all pages back — evicted ones should come from IDB
      for (let i = 0; i < 4; i++) {
        const buf = new Uint8Array(PAGE_SIZE);
        await cache.read("/bigfile", buf, 0, PAGE_SIZE, i * PAGE_SIZE, 4 * PAGE_SIZE);
        expect(buf[0]).toBe(i + 1);
        expect(buf[PAGE_SIZE - 1]).toBe(i + 1);
      }
    });
  });

  // -------------------------------------------------------------------
  // Database lifecycle
  // -------------------------------------------------------------------

  describe("lifecycle", () => {
    it("close then re-open preserves data", async () => {
      const dbName = `tomefs-lifecycle-${dbCounter++}`;
      const backend1 = new IdbBackend({ dbName });

      await backend1.writePage("/file1", 0, filledPage(0xab));
      await backend1.writeMeta("/file1", {
        size: PAGE_SIZE,
        mode: 0o100644,
        ctime: 1000,
        mtime: 2000,
      });

      backend1.close();

      // Re-open same database
      const backend2 = new IdbBackend({ dbName });
      const page = await backend2.readPage("/file1", 0);
      expect(page).toEqual(filledPage(0xab));

      const meta = await backend2.readMeta("/file1");
      expect(meta).toEqual({
        size: PAGE_SIZE,
        mode: 0o100644,
        ctime: 1000,
        mtime: 2000,
      });

      await backend2.destroy();
    });

    it("destroy removes all data", async () => {
      const dbName = `tomefs-destroy-${dbCounter++}`;
      const b1 = new IdbBackend({ dbName });
      await b1.writePage("/file1", 0, filledPage(0x01));
      await b1.destroy();

      const b2 = new IdbBackend({ dbName });
      const page = await b2.readPage("/file1", 0);
      expect(page).toBeNull();
      await b2.destroy();
    });
  });

  // -------------------------------------------------------------------
  // Edge cases
  // -------------------------------------------------------------------

  describe("edge cases", () => {
    it("handles paths with special characters", async () => {
      const path = "/base/16384/12345";
      await backend.writePage(path, 0, filledPage(0xcc));
      expect(await backend.readPage(path, 0)).toEqual(filledPage(0xcc));
    });

    it("handles high page indices", async () => {
      await backend.writePage("/file1", 10000, filledPage(0xdd));
      expect(await backend.readPage("/file1", 10000)).toEqual(
        filledPage(0xdd),
      );
    });

    it("handles empty page (all zeros)", async () => {
      const empty = new Uint8Array(PAGE_SIZE);
      await backend.writePage("/file1", 0, empty);

      const result = await backend.readPage("/file1", 0);
      expect(result).toEqual(empty);
    });

    it("concurrent writes to different files do not interfere", async () => {
      const writes = Array.from({ length: 10 }, (_, i) =>
        backend.writePage(`/file${i}`, 0, filledPage(i)),
      );
      await Promise.all(writes);

      for (let i = 0; i < 10; i++) {
        const page = await backend.readPage(`/file${i}`, 0);
        expect(page).toEqual(filledPage(i));
      }
    });

    it("deleteFile with many pages only deletes the target file", async () => {
      // Write 20 pages across two files with overlapping page indices
      for (let i = 0; i < 10; i++) {
        await backend.writePage("/target", i, filledPage(i));
        await backend.writePage("/keep", i, filledPage(0xff - i));
      }

      await backend.deleteFile("/target");

      // All target pages gone
      for (let i = 0; i < 10; i++) {
        expect(await backend.readPage("/target", i)).toBeNull();
      }
      // All kept pages intact
      for (let i = 0; i < 10; i++) {
        expect(await backend.readPage("/keep", i)).toEqual(
          filledPage(0xff - i),
        );
      }
    });

    it("deletePagesFrom preserves pages below the threshold", async () => {
      for (let i = 0; i < 10; i++) {
        await backend.writePage("/file", i, filledPage(i));
      }

      await backend.deletePagesFrom("/file", 5);

      // Pages 0-4 survive
      for (let i = 0; i < 5; i++) {
        expect(await backend.readPage("/file", i)).toEqual(filledPage(i));
      }
      // Pages 5-9 deleted
      for (let i = 5; i < 10; i++) {
        expect(await backend.readPage("/file", i)).toBeNull();
      }
    });
  });
});
