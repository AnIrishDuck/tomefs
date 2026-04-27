/**
 * Unit tests for SyncMemoryBackend.
 *
 * Validates the SyncStorageBackend interface contract using the synchronous
 * in-memory implementation. SyncMemoryBackend is the foundational test fixture
 * used by virtually every test in the suite — bugs here silently corrupt all
 * test results. These tests verify it independently.
 *
 * Mirrors memory-backend.test.ts (async variant) with additional tests for
 * copy safety and prefix isolation, which are critical properties for a
 * backend used as a test fixture.
 */
import { describe, it, expect, beforeEach } from "vitest";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { PAGE_SIZE } from "../../src/types.js";

describe("SyncMemoryBackend", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  describe("page operations", () => {
    it("@fast returns null for non-existent page", () => {
      const page = backend.readPage("/test", 0);
      expect(page).toBeNull();
    });

    it("@fast writes and reads a page", () => {
      const data = new Uint8Array(PAGE_SIZE);
      data[0] = 0xde;
      data[1] = 0xad;
      data[PAGE_SIZE - 1] = 0xff;

      backend.writePage("/test", 0, data);
      const read = backend.readPage("/test", 0);

      expect(read).not.toBeNull();
      expect(read![0]).toBe(0xde);
      expect(read![1]).toBe(0xad);
      expect(read![PAGE_SIZE - 1]).toBe(0xff);
    });

    it("readPage returns a copy, not a reference", () => {
      const data = new Uint8Array(PAGE_SIZE);
      data[0] = 42;
      backend.writePage("/test", 0, data);

      const read1 = backend.readPage("/test", 0);
      read1![0] = 99;

      const read2 = backend.readPage("/test", 0);
      expect(read2![0]).toBe(42);
    });

    it("writePage stores a copy, not a reference", () => {
      const data = new Uint8Array(PAGE_SIZE);
      data[0] = 42;
      backend.writePage("/test", 0, data);

      // Mutating the original should not affect stored data
      data[0] = 99;

      const read = backend.readPage("/test", 0);
      expect(read![0]).toBe(42);
    });

    it("stores pages independently by path and index", () => {
      const data1 = new Uint8Array(PAGE_SIZE);
      data1[0] = 1;
      const data2 = new Uint8Array(PAGE_SIZE);
      data2[0] = 2;
      const data3 = new Uint8Array(PAGE_SIZE);
      data3[0] = 3;

      backend.writePage("/a", 0, data1);
      backend.writePage("/a", 1, data2);
      backend.writePage("/b", 0, data3);

      expect(backend.readPage("/a", 0)![0]).toBe(1);
      expect(backend.readPage("/a", 1)![0]).toBe(2);
      expect(backend.readPage("/b", 0)![0]).toBe(3);
    });

    it("overwrites existing page data", () => {
      const data1 = new Uint8Array(PAGE_SIZE);
      data1[0] = 1;
      backend.writePage("/test", 0, data1);

      const data2 = new Uint8Array(PAGE_SIZE);
      data2[0] = 2;
      backend.writePage("/test", 0, data2);

      expect(backend.readPage("/test", 0)![0]).toBe(2);
    });
  });

  describe("batch read", () => {
    it("reads multiple pages in one call", () => {
      const d0 = new Uint8Array(PAGE_SIZE);
      d0[0] = 0xaa;
      const d1 = new Uint8Array(PAGE_SIZE);
      d1[0] = 0xbb;
      backend.writePage("/test", 0, d0);
      backend.writePage("/test", 1, d1);

      const results = backend.readPages("/test", [0, 1, 2]);
      expect(results).toHaveLength(3);
      expect(results[0]![0]).toBe(0xaa);
      expect(results[1]![0]).toBe(0xbb);
      expect(results[2]).toBeNull();
    });

    it("returns empty array for empty indices", () => {
      const results = backend.readPages("/test", []);
      expect(results).toEqual([]);
    });

    it("returns independent copies for each page", () => {
      const data = new Uint8Array(PAGE_SIZE);
      data[0] = 42;
      backend.writePage("/test", 0, data);

      const results = backend.readPages("/test", [0, 0]);
      results[0]![0] = 99;
      expect(results[1]![0]).toBe(42);
    });
  });

  describe("batch write", () => {
    it("writes multiple pages in one call", () => {
      const pages = [
        { path: "/a", pageIndex: 0, data: new Uint8Array(PAGE_SIZE) },
        { path: "/a", pageIndex: 1, data: new Uint8Array(PAGE_SIZE) },
        { path: "/b", pageIndex: 0, data: new Uint8Array(PAGE_SIZE) },
      ];
      pages[0].data[0] = 10;
      pages[1].data[0] = 20;
      pages[2].data[0] = 30;

      backend.writePages(pages);

      expect(backend.readPage("/a", 0)![0]).toBe(10);
      expect(backend.readPage("/a", 1)![0]).toBe(20);
      expect(backend.readPage("/b", 0)![0]).toBe(30);
    });

    it("is a no-op for empty array", () => {
      backend.writePages([]);
      // No error thrown
    });
  });

  describe("deleteFile", () => {
    it("@fast removes all pages for a file", () => {
      backend.writePage("/a", 0, new Uint8Array(PAGE_SIZE));
      backend.writePage("/a", 1, new Uint8Array(PAGE_SIZE));
      backend.writePage("/b", 0, new Uint8Array(PAGE_SIZE));

      backend.deleteFile("/a");

      expect(backend.readPage("/a", 0)).toBeNull();
      expect(backend.readPage("/a", 1)).toBeNull();
      expect(backend.readPage("/b", 0)).not.toBeNull();
    });

    it("is a no-op for non-existent file", () => {
      backend.deleteFile("/nonexistent");
      // No error thrown
    });

    it("does not delete pages from path with shared prefix", () => {
      backend.writePage("/abc", 0, new Uint8Array(PAGE_SIZE));
      backend.writePage("/abcdef", 0, new Uint8Array(PAGE_SIZE));

      backend.deleteFile("/abc");

      expect(backend.readPage("/abc", 0)).toBeNull();
      expect(backend.readPage("/abcdef", 0)).not.toBeNull();
    });
  });

  describe("deletePagesFrom", () => {
    it("removes pages at and beyond the given index", () => {
      backend.writePage("/a", 0, new Uint8Array(PAGE_SIZE));
      backend.writePage("/a", 1, new Uint8Array(PAGE_SIZE));
      backend.writePage("/a", 2, new Uint8Array(PAGE_SIZE));
      backend.writePage("/a", 3, new Uint8Array(PAGE_SIZE));

      backend.deletePagesFrom("/a", 2);

      expect(backend.readPage("/a", 0)).not.toBeNull();
      expect(backend.readPage("/a", 1)).not.toBeNull();
      expect(backend.readPage("/a", 2)).toBeNull();
      expect(backend.readPage("/a", 3)).toBeNull();
    });

    it("deletes all pages when fromIndex is 0", () => {
      backend.writePage("/a", 0, new Uint8Array(PAGE_SIZE));
      backend.writePage("/a", 1, new Uint8Array(PAGE_SIZE));

      backend.deletePagesFrom("/a", 0);

      expect(backend.readPage("/a", 0)).toBeNull();
      expect(backend.readPage("/a", 1)).toBeNull();
    });

    it("does not affect other files", () => {
      backend.writePage("/a", 0, new Uint8Array(PAGE_SIZE));
      backend.writePage("/b", 0, new Uint8Array(PAGE_SIZE));

      backend.deletePagesFrom("/a", 0);

      expect(backend.readPage("/b", 0)).not.toBeNull();
    });
  });

  describe("renameFile", () => {
    it("moves all pages from old path to new path", () => {
      const d0 = new Uint8Array(PAGE_SIZE);
      d0[0] = 0xaa;
      const d1 = new Uint8Array(PAGE_SIZE);
      d1[0] = 0xbb;
      backend.writePage("/old", 0, d0);
      backend.writePage("/old", 1, d1);
      backend.writePage("/other", 0, new Uint8Array(PAGE_SIZE));

      backend.renameFile("/old", "/new");

      expect(backend.readPage("/old", 0)).toBeNull();
      expect(backend.readPage("/old", 1)).toBeNull();
      expect(backend.readPage("/new", 0)![0]).toBe(0xaa);
      expect(backend.readPage("/new", 1)![0]).toBe(0xbb);
      // Unrelated file untouched
      expect(backend.readPage("/other", 0)).not.toBeNull();
    });

    it("is a no-op when old path has no pages", () => {
      backend.renameFile("/nonexistent", "/new");
      expect(backend.readPage("/new", 0)).toBeNull();
    });

    it("overwrites pages at the target path", () => {
      const d0 = new Uint8Array(PAGE_SIZE);
      d0[0] = 0xaa;
      backend.writePage("/old", 0, d0);

      const existing = new Uint8Array(PAGE_SIZE);
      existing[0] = 0xff;
      backend.writePage("/new", 0, existing);

      backend.renameFile("/old", "/new");

      expect(backend.readPage("/new", 0)![0]).toBe(0xaa);
    });

    it("does not rename pages from path with shared prefix", () => {
      backend.writePage("/abc", 0, new Uint8Array(PAGE_SIZE));
      const d = new Uint8Array(PAGE_SIZE);
      d[0] = 0x42;
      backend.writePage("/abcdef", 0, d);

      backend.renameFile("/abc", "/xyz");

      expect(backend.readPage("/abc", 0)).toBeNull();
      expect(backend.readPage("/xyz", 0)).not.toBeNull();
      // /abcdef should be untouched
      expect(backend.readPage("/abcdef", 0)![0]).toBe(0x42);
    });
  });

  describe("metadata operations", () => {
    it("@fast returns null for non-existent metadata", () => {
      expect(backend.readMeta("/test")).toBeNull();
    });

    it("@fast writes and reads metadata", () => {
      const meta = { size: 1024, mode: 0o644, ctime: 1000, mtime: 2000 };
      backend.writeMeta("/test", meta);

      const read = backend.readMeta("/test");
      expect(read).toEqual(meta);
    });

    it("readMeta returns a copy of metadata", () => {
      const meta = { size: 1024, mode: 0o644, ctime: 1000, mtime: 2000 };
      backend.writeMeta("/test", meta);

      const read = backend.readMeta("/test");
      read!.size = 9999;

      const read2 = backend.readMeta("/test");
      expect(read2!.size).toBe(1024);
    });

    it("writeMeta stores a copy of metadata", () => {
      const meta = { size: 1024, mode: 0o644, ctime: 1000, mtime: 2000 };
      backend.writeMeta("/test", meta);

      // Mutating the original should not affect stored data
      meta.size = 9999;

      const read = backend.readMeta("/test");
      expect(read!.size).toBe(1024);
    });

    it("preserves optional atime field", () => {
      const meta = { size: 0, mode: 0o644, ctime: 1, mtime: 2, atime: 3 };
      backend.writeMeta("/test", meta);
      expect(backend.readMeta("/test")!.atime).toBe(3);
    });

    it("preserves optional link field", () => {
      const meta = {
        size: 0,
        mode: 0o120777,
        ctime: 1,
        mtime: 2,
        link: "/target",
      };
      backend.writeMeta("/test", meta);
      expect(backend.readMeta("/test")!.link).toBe("/target");
    });

    it("deleteMeta removes metadata", () => {
      backend.writeMeta("/test", {
        size: 0,
        mode: 0o644,
        ctime: 0,
        mtime: 0,
      });
      backend.deleteMeta("/test");
      expect(backend.readMeta("/test")).toBeNull();
    });

    it("deleteMeta is a no-op for non-existent path", () => {
      backend.deleteMeta("/nonexistent");
      // No error thrown
    });
  });

  describe("batch metadata write", () => {
    it("@fast writes multiple metadata entries in one call", () => {
      backend.writeMetas([
        { path: "/a", meta: { size: 100, mode: 0o644, ctime: 1, mtime: 2 } },
        { path: "/b", meta: { size: 200, mode: 0o755, ctime: 3, mtime: 4 } },
        {
          path: "/c/d",
          meta: { size: 0, mode: 0o40755, ctime: 5, mtime: 6 },
        },
      ]);

      const a = backend.readMeta("/a");
      const b = backend.readMeta("/b");
      const cd = backend.readMeta("/c/d");
      expect(a).toEqual({ size: 100, mode: 0o644, ctime: 1, mtime: 2 });
      expect(b).toEqual({ size: 200, mode: 0o755, ctime: 3, mtime: 4 });
      expect(cd).toEqual({ size: 0, mode: 0o40755, ctime: 5, mtime: 6 });
    });

    it("overwrites existing metadata", () => {
      backend.writeMeta("/a", {
        size: 100,
        mode: 0o644,
        ctime: 1,
        mtime: 2,
      });
      backend.writeMetas([
        { path: "/a", meta: { size: 999, mode: 0o755, ctime: 10, mtime: 20 } },
      ]);
      expect(backend.readMeta("/a")!.size).toBe(999);
    });

    it("is a no-op for empty array", () => {
      backend.writeMetas([]);
      expect(backend.listFiles()).toEqual([]);
    });

    it("stores copies of metadata", () => {
      const meta = { size: 100, mode: 0o644, ctime: 1, mtime: 2 };
      backend.writeMetas([{ path: "/a", meta }]);

      meta.size = 9999;
      expect(backend.readMeta("/a")!.size).toBe(100);
    });
  });

  describe("batch metadata delete", () => {
    it("@fast deletes multiple metadata entries in one call", () => {
      const meta = { size: 0, mode: 0o644, ctime: 0, mtime: 0 };
      backend.writeMeta("/a", meta);
      backend.writeMeta("/b", meta);
      backend.writeMeta("/c", meta);

      backend.deleteMetas(["/a", "/c"]);

      expect(backend.readMeta("/a")).toBeNull();
      expect(backend.readMeta("/b")).not.toBeNull();
      expect(backend.readMeta("/c")).toBeNull();
    });

    it("is a no-op for empty array", () => {
      backend.writeMeta("/a", {
        size: 0,
        mode: 0o644,
        ctime: 0,
        mtime: 0,
      });
      backend.deleteMetas([]);
      expect(backend.readMeta("/a")).not.toBeNull();
    });

    it("silently ignores non-existent paths", () => {
      backend.deleteMetas(["/nonexistent"]);
      // No error thrown
    });
  });

  describe("listFiles", () => {
    it("@fast returns empty array when no files exist", () => {
      expect(backend.listFiles()).toEqual([]);
    });

    it("lists all files with metadata", () => {
      const meta = { size: 0, mode: 0o644, ctime: 0, mtime: 0 };
      backend.writeMeta("/a", meta);
      backend.writeMeta("/b", meta);
      backend.writeMeta("/c/d", meta);

      const files = backend.listFiles();
      expect(files.sort()).toEqual(["/a", "/b", "/c/d"]);
    });

    it("does not list deleted files", () => {
      const meta = { size: 0, mode: 0o644, ctime: 0, mtime: 0 };
      backend.writeMeta("/a", meta);
      backend.writeMeta("/b", meta);
      backend.deleteMeta("/a");

      expect(backend.listFiles()).toEqual(["/b"]);
    });

    it("reflects batch metadata operations", () => {
      backend.writeMetas([
        { path: "/x", meta: { size: 0, mode: 0o644, ctime: 0, mtime: 0 } },
        { path: "/y", meta: { size: 0, mode: 0o644, ctime: 0, mtime: 0 } },
      ]);
      backend.deleteMetas(["/x"]);

      expect(backend.listFiles()).toEqual(["/y"]);
    });
  });

  describe("readMetas", () => {
    it("reads multiple metadata entries in one call", () => {
      const metaA = { size: 100, mode: 0o644, ctime: 1, mtime: 2 };
      const metaB = { size: 200, mode: 0o755, ctime: 3, mtime: 4 };
      backend.writeMeta("/a", metaA);
      backend.writeMeta("/b", metaB);

      const results = backend.readMetas(["/a", "/b", "/missing"]);

      expect(results).toHaveLength(3);
      expect(results[0]).toEqual(metaA);
      expect(results[1]).toEqual(metaB);
      expect(results[2]).toBeNull();
    });

    it("returns empty array for empty input", () => {
      expect(backend.readMetas([])).toEqual([]);
    });

    it("returns independent copies", () => {
      backend.writeMeta("/a", { size: 100, mode: 0o644, ctime: 1, mtime: 2 });

      const results = backend.readMetas(["/a", "/a"]);
      results[0]!.size = 9999;

      expect(results[1]!.size).toBe(100);
    });
  });

  describe("deleteFiles", () => {
    it("deletes pages for multiple files in one call", () => {
      backend.writePage("/a", 0, new Uint8Array(PAGE_SIZE));
      backend.writePage("/a", 1, new Uint8Array(PAGE_SIZE));
      backend.writePage("/b", 0, new Uint8Array(PAGE_SIZE));
      backend.writePage("/keep", 0, new Uint8Array(PAGE_SIZE));

      backend.deleteFiles(["/a", "/b"]);

      expect(backend.readPage("/a", 0)).toBeNull();
      expect(backend.readPage("/a", 1)).toBeNull();
      expect(backend.readPage("/b", 0)).toBeNull();
      expect(backend.readPage("/keep", 0)).not.toBeNull();
    });

    it("is a no-op for empty array", () => {
      backend.writePage("/a", 0, new Uint8Array(PAGE_SIZE));
      backend.deleteFiles([]);
      expect(backend.readPage("/a", 0)).not.toBeNull();
    });
  });

  describe("countPages", () => {
    it("returns 0 for non-existent file", () => {
      expect(backend.countPages("/missing")).toBe(0);
    });

    it("counts pages for a file", () => {
      backend.writePage("/a", 0, new Uint8Array(PAGE_SIZE));
      backend.writePage("/a", 3, new Uint8Array(PAGE_SIZE));
      backend.writePage("/a", 7, new Uint8Array(PAGE_SIZE));

      expect(backend.countPages("/a")).toBe(3);
    });

    it("reflects deletions", () => {
      backend.writePage("/a", 0, new Uint8Array(PAGE_SIZE));
      backend.writePage("/a", 1, new Uint8Array(PAGE_SIZE));
      backend.deletePagesFrom("/a", 1);

      expect(backend.countPages("/a")).toBe(1);
    });
  });

  describe("countPagesBatch", () => {
    it("counts pages for multiple files in one call", () => {
      backend.writePage("/a", 0, new Uint8Array(PAGE_SIZE));
      backend.writePage("/a", 1, new Uint8Array(PAGE_SIZE));
      backend.writePage("/b", 0, new Uint8Array(PAGE_SIZE));

      expect(backend.countPagesBatch(["/a", "/b", "/missing"])).toEqual([2, 1, 0]);
    });

    it("returns empty array for empty input", () => {
      expect(backend.countPagesBatch([])).toEqual([]);
    });
  });

  describe("maxPageIndex", () => {
    it("returns -1 for non-existent file", () => {
      expect(backend.maxPageIndex("/missing")).toBe(-1);
    });

    it("returns highest page index", () => {
      backend.writePage("/a", 0, new Uint8Array(PAGE_SIZE));
      backend.writePage("/a", 5, new Uint8Array(PAGE_SIZE));
      backend.writePage("/a", 2, new Uint8Array(PAGE_SIZE));

      expect(backend.maxPageIndex("/a")).toBe(5);
    });

    it("reflects deletions", () => {
      backend.writePage("/a", 0, new Uint8Array(PAGE_SIZE));
      backend.writePage("/a", 3, new Uint8Array(PAGE_SIZE));
      backend.deletePagesFrom("/a", 2);

      expect(backend.maxPageIndex("/a")).toBe(0);
    });

    it("returns -1 after all pages deleted", () => {
      backend.writePage("/a", 0, new Uint8Array(PAGE_SIZE));
      backend.deleteFile("/a");

      expect(backend.maxPageIndex("/a")).toBe(-1);
    });
  });

  describe("maxPageIndexBatch", () => {
    it("returns max page index for multiple files", () => {
      backend.writePage("/a", 0, new Uint8Array(PAGE_SIZE));
      backend.writePage("/a", 4, new Uint8Array(PAGE_SIZE));
      backend.writePage("/b", 2, new Uint8Array(PAGE_SIZE));

      expect(backend.maxPageIndexBatch(["/a", "/b", "/missing"])).toEqual([4, 2, -1]);
    });

    it("returns empty array for empty input", () => {
      expect(backend.maxPageIndexBatch([])).toEqual([]);
    });
  });

  describe("syncAll", () => {
    it("writes pages and metadata atomically", () => {
      const page = new Uint8Array(PAGE_SIZE);
      page[0] = 0xab;

      backend.syncAll(
        [{ path: "/a", pageIndex: 0, data: page }],
        [{ path: "/a", meta: { size: PAGE_SIZE, mode: 0o644, ctime: 1, mtime: 2 } }],
      );

      expect(backend.readPage("/a", 0)![0]).toBe(0xab);
      expect(backend.readMeta("/a")).toEqual({
        size: PAGE_SIZE, mode: 0o644, ctime: 1, mtime: 2,
      });
    });

    it("handles empty pages with metadata", () => {
      backend.syncAll(
        [],
        [{ path: "/a", meta: { size: 0, mode: 0o644, ctime: 1, mtime: 2 } }],
      );

      expect(backend.readMeta("/a")).not.toBeNull();
      expect(backend.countPages("/a")).toBe(0);
    });

    it("handles pages with empty metadata", () => {
      const page = new Uint8Array(PAGE_SIZE);
      page[0] = 0xcd;

      backend.syncAll(
        [{ path: "/a", pageIndex: 0, data: page }],
        [],
      );

      expect(backend.readPage("/a", 0)![0]).toBe(0xcd);
      expect(backend.readMeta("/a")).toBeNull();
    });

    it("is a no-op with both arrays empty", () => {
      backend.syncAll([], []);
      expect(backend.listFiles()).toEqual([]);
    });

    it("overwrites existing data", () => {
      backend.writePage("/a", 0, new Uint8Array(PAGE_SIZE));
      backend.writeMeta("/a", { size: 100, mode: 0o644, ctime: 0, mtime: 0 });

      const page = new Uint8Array(PAGE_SIZE);
      page[0] = 0xff;
      backend.syncAll(
        [{ path: "/a", pageIndex: 0, data: page }],
        [{ path: "/a", meta: { size: PAGE_SIZE, mode: 0o755, ctime: 5, mtime: 6 } }],
      );

      expect(backend.readPage("/a", 0)![0]).toBe(0xff);
      expect(backend.readMeta("/a")!.mode).toBe(0o755);
    });
  });

  describe("deleteAll", () => {
    it("deletes both pages and metadata for given paths", () => {
      backend.writePage("/a", 0, new Uint8Array(PAGE_SIZE));
      backend.writePage("/a", 1, new Uint8Array(PAGE_SIZE));
      backend.writeMeta("/a", { size: PAGE_SIZE * 2, mode: 0o644, ctime: 1, mtime: 2 });
      backend.writePage("/b", 0, new Uint8Array(PAGE_SIZE));
      backend.writeMeta("/b", { size: PAGE_SIZE, mode: 0o644, ctime: 1, mtime: 2 });

      backend.deleteAll(["/a", "/b"]);

      expect(backend.readPage("/a", 0)).toBeNull();
      expect(backend.readPage("/a", 1)).toBeNull();
      expect(backend.readMeta("/a")).toBeNull();
      expect(backend.readPage("/b", 0)).toBeNull();
      expect(backend.readMeta("/b")).toBeNull();
      expect(backend.listFiles()).toEqual([]);
    });

    it("is a no-op for empty array", () => {
      backend.writePage("/a", 0, new Uint8Array(PAGE_SIZE));
      backend.writeMeta("/a", { size: PAGE_SIZE, mode: 0o644, ctime: 0, mtime: 0 });

      backend.deleteAll([]);

      expect(backend.readPage("/a", 0)).not.toBeNull();
      expect(backend.readMeta("/a")).not.toBeNull();
    });

    it("does not affect unrelated files", () => {
      const page = new Uint8Array(PAGE_SIZE);
      page[0] = 0x42;
      backend.writePage("/keep", 0, page);
      backend.writeMeta("/keep", { size: PAGE_SIZE, mode: 0o644, ctime: 0, mtime: 0 });
      backend.writePage("/remove", 0, new Uint8Array(PAGE_SIZE));
      backend.writeMeta("/remove", { size: PAGE_SIZE, mode: 0o644, ctime: 0, mtime: 0 });

      backend.deleteAll(["/remove"]);

      expect(backend.readPage("/keep", 0)![0]).toBe(0x42);
      expect(backend.readMeta("/keep")).not.toBeNull();
    });

    it("handles paths with pages but no metadata", () => {
      backend.writePage("/orphan", 0, new Uint8Array(PAGE_SIZE));

      backend.deleteAll(["/orphan"]);

      expect(backend.readPage("/orphan", 0)).toBeNull();
    });

    it("handles paths with metadata but no pages", () => {
      backend.writeMeta("/meta-only", { size: 0, mode: 0o644, ctime: 0, mtime: 0 });

      backend.deleteAll(["/meta-only"]);

      expect(backend.readMeta("/meta-only")).toBeNull();
    });

    it("handles non-existent paths", () => {
      backend.deleteAll(["/nonexistent"]);
    });

    it("handles duplicate paths", () => {
      backend.writePage("/a", 0, new Uint8Array(PAGE_SIZE));
      backend.writeMeta("/a", { size: PAGE_SIZE, mode: 0o644, ctime: 0, mtime: 0 });

      backend.deleteAll(["/a", "/a"]);

      expect(backend.readPage("/a", 0)).toBeNull();
      expect(backend.readMeta("/a")).toBeNull();
    });

    it("updates countPages and maxPageIndex", () => {
      backend.writePage("/a", 0, new Uint8Array(PAGE_SIZE));
      backend.writePage("/a", 3, new Uint8Array(PAGE_SIZE));
      backend.writeMeta("/a", { size: PAGE_SIZE * 4, mode: 0o644, ctime: 0, mtime: 0 });

      backend.deleteAll(["/a"]);

      expect(backend.countPages("/a")).toBe(0);
      expect(backend.maxPageIndex("/a")).toBe(-1);
    });

    it("does not delete files with a prefix match", () => {
      const page10 = new Uint8Array(PAGE_SIZE);
      page10[0] = 0x10;
      backend.writePage("/file1", 0, new Uint8Array(PAGE_SIZE));
      backend.writeMeta("/file1", { size: PAGE_SIZE, mode: 0o644, ctime: 0, mtime: 0 });
      backend.writePage("/file10", 0, page10);
      backend.writeMeta("/file10", { size: PAGE_SIZE, mode: 0o644, ctime: 0, mtime: 0 });

      backend.deleteAll(["/file1"]);

      expect(backend.readPage("/file1", 0)).toBeNull();
      expect(backend.readMeta("/file1")).toBeNull();
      expect(backend.readPage("/file10", 0)![0]).toBe(0x10);
      expect(backend.readMeta("/file10")).not.toBeNull();
    });
  });

  describe("page and metadata independence", () => {
    it("deleteFile does not remove metadata", () => {
      backend.writeMeta("/a", {
        size: 8192,
        mode: 0o644,
        ctime: 0,
        mtime: 0,
      });
      backend.writePage("/a", 0, new Uint8Array(PAGE_SIZE));

      backend.deleteFile("/a");

      expect(backend.readPage("/a", 0)).toBeNull();
      expect(backend.readMeta("/a")).not.toBeNull();
    });

    it("deleteMeta does not remove pages", () => {
      backend.writeMeta("/a", {
        size: 8192,
        mode: 0o644,
        ctime: 0,
        mtime: 0,
      });
      const data = new Uint8Array(PAGE_SIZE);
      data[0] = 0x42;
      backend.writePage("/a", 0, data);

      backend.deleteMeta("/a");

      expect(backend.readMeta("/a")).toBeNull();
      expect(backend.readPage("/a", 0)![0]).toBe(0x42);
    });

    it("renameFile does not move metadata", () => {
      backend.writeMeta("/old", {
        size: 8192,
        mode: 0o644,
        ctime: 0,
        mtime: 0,
      });
      backend.writePage("/old", 0, new Uint8Array(PAGE_SIZE));

      backend.renameFile("/old", "/new");

      // Pages moved, but metadata stays at old path
      expect(backend.readPage("/new", 0)).not.toBeNull();
      expect(backend.readMeta("/old")).not.toBeNull();
      expect(backend.readMeta("/new")).toBeNull();
    });
  });
});
