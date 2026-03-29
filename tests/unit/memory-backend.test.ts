/**
 * Unit tests for MemoryBackend.
 *
 * Validates the StorageBackend interface contract using the in-memory
 * implementation. These tests also serve as the specification for any
 * future backend (IDB, OPFS).
 */
import { describe, it, expect, beforeEach } from "vitest";
import { MemoryBackend } from "../../src/memory-backend.js";
import { PAGE_SIZE } from "../../src/types.js";

describe("MemoryBackend", () => {
  let backend: MemoryBackend;

  beforeEach(() => {
    backend = new MemoryBackend();
  });

  describe("page operations", () => {
    it("@fast returns null for non-existent page", async () => {
      const page = await backend.readPage("/test", 0);
      expect(page).toBeNull();
    });

    it("@fast writes and reads a page", async () => {
      const data = new Uint8Array(PAGE_SIZE);
      data[0] = 0xde;
      data[1] = 0xad;
      data[PAGE_SIZE - 1] = 0xff;

      await backend.writePage("/test", 0, data);
      const read = await backend.readPage("/test", 0);

      expect(read).not.toBeNull();
      expect(read![0]).toBe(0xde);
      expect(read![1]).toBe(0xad);
      expect(read![PAGE_SIZE - 1]).toBe(0xff);
    });

    it("returns a copy, not a reference", async () => {
      const data = new Uint8Array(PAGE_SIZE);
      data[0] = 42;
      await backend.writePage("/test", 0, data);

      const read1 = await backend.readPage("/test", 0);
      read1![0] = 99;

      const read2 = await backend.readPage("/test", 0);
      expect(read2![0]).toBe(42);
    });

    it("stores pages independently by path and index", async () => {
      const data1 = new Uint8Array(PAGE_SIZE);
      data1[0] = 1;
      const data2 = new Uint8Array(PAGE_SIZE);
      data2[0] = 2;
      const data3 = new Uint8Array(PAGE_SIZE);
      data3[0] = 3;

      await backend.writePage("/a", 0, data1);
      await backend.writePage("/a", 1, data2);
      await backend.writePage("/b", 0, data3);

      expect((await backend.readPage("/a", 0))![0]).toBe(1);
      expect((await backend.readPage("/a", 1))![0]).toBe(2);
      expect((await backend.readPage("/b", 0))![0]).toBe(3);
    });

    it("overwrites existing page data", async () => {
      const data1 = new Uint8Array(PAGE_SIZE);
      data1[0] = 1;
      await backend.writePage("/test", 0, data1);

      const data2 = new Uint8Array(PAGE_SIZE);
      data2[0] = 2;
      await backend.writePage("/test", 0, data2);

      expect((await backend.readPage("/test", 0))![0]).toBe(2);
    });
  });

  describe("batch read", () => {
    it("reads multiple pages in one call", async () => {
      const d0 = new Uint8Array(PAGE_SIZE);
      d0[0] = 0xaa;
      const d1 = new Uint8Array(PAGE_SIZE);
      d1[0] = 0xbb;
      await backend.writePage("/test", 0, d0);
      await backend.writePage("/test", 1, d1);

      const results = await backend.readPages("/test", [0, 1, 2]);
      expect(results).toHaveLength(3);
      expect(results[0]![0]).toBe(0xaa);
      expect(results[1]![0]).toBe(0xbb);
      expect(results[2]).toBeNull();
    });

    it("returns empty array for empty indices", async () => {
      const results = await backend.readPages("/test", []);
      expect(results).toEqual([]);
    });
  });

  describe("batch write", () => {
    it("writes multiple pages atomically", async () => {
      const pages = [
        { path: "/a", pageIndex: 0, data: new Uint8Array(PAGE_SIZE) },
        { path: "/a", pageIndex: 1, data: new Uint8Array(PAGE_SIZE) },
        { path: "/b", pageIndex: 0, data: new Uint8Array(PAGE_SIZE) },
      ];
      pages[0].data[0] = 10;
      pages[1].data[0] = 20;
      pages[2].data[0] = 30;

      await backend.writePages(pages);

      expect((await backend.readPage("/a", 0))![0]).toBe(10);
      expect((await backend.readPage("/a", 1))![0]).toBe(20);
      expect((await backend.readPage("/b", 0))![0]).toBe(30);
    });
  });

  describe("deleteFile", () => {
    it("removes all pages for a file", async () => {
      await backend.writePage("/a", 0, new Uint8Array(PAGE_SIZE));
      await backend.writePage("/a", 1, new Uint8Array(PAGE_SIZE));
      await backend.writePage("/b", 0, new Uint8Array(PAGE_SIZE));

      await backend.deleteFile("/a");

      expect(await backend.readPage("/a", 0)).toBeNull();
      expect(await backend.readPage("/a", 1)).toBeNull();
      expect(await backend.readPage("/b", 0)).not.toBeNull();
    });
  });

  describe("deleteFiles", () => {
    it("@fast removes all pages for multiple files in one call", async () => {
      await backend.writePage("/a", 0, new Uint8Array(PAGE_SIZE));
      await backend.writePage("/a", 1, new Uint8Array(PAGE_SIZE));
      await backend.writePage("/b", 0, new Uint8Array(PAGE_SIZE));
      await backend.writePage("/c", 0, new Uint8Array(PAGE_SIZE));

      await backend.deleteFiles(["/a", "/b"]);

      expect(await backend.readPage("/a", 0)).toBeNull();
      expect(await backend.readPage("/a", 1)).toBeNull();
      expect(await backend.readPage("/b", 0)).toBeNull();
      expect(await backend.readPage("/c", 0)).not.toBeNull();
    });

    it("is a no-op for empty array", async () => {
      await backend.writePage("/a", 0, new Uint8Array(PAGE_SIZE));
      await backend.deleteFiles([]);
      expect(await backend.readPage("/a", 0)).not.toBeNull();
    });

    it("handles non-existent paths gracefully", async () => {
      await backend.writePage("/a", 0, new Uint8Array(PAGE_SIZE));
      await backend.deleteFiles(["/nonexistent", "/also-missing"]);
      expect(await backend.readPage("/a", 0)).not.toBeNull();
    });

    it("does not delete pages from prefix-matching paths", async () => {
      await backend.writePage("/abc", 0, new Uint8Array(PAGE_SIZE));
      await backend.writePage("/abcdef", 0, new Uint8Array(PAGE_SIZE));

      await backend.deleteFiles(["/abc"]);

      expect(await backend.readPage("/abc", 0)).toBeNull();
      expect(await backend.readPage("/abcdef", 0)).not.toBeNull();
    });
  });

  describe("deletePagesFrom", () => {
    it("removes pages at and beyond the given index", async () => {
      await backend.writePage("/a", 0, new Uint8Array(PAGE_SIZE));
      await backend.writePage("/a", 1, new Uint8Array(PAGE_SIZE));
      await backend.writePage("/a", 2, new Uint8Array(PAGE_SIZE));
      await backend.writePage("/a", 3, new Uint8Array(PAGE_SIZE));

      await backend.deletePagesFrom("/a", 2);

      expect(await backend.readPage("/a", 0)).not.toBeNull();
      expect(await backend.readPage("/a", 1)).not.toBeNull();
      expect(await backend.readPage("/a", 2)).toBeNull();
      expect(await backend.readPage("/a", 3)).toBeNull();
    });
  });

  describe("countPages", () => {
    it("@fast returns 0 for non-existent file", async () => {
      expect(await backend.countPages("/nonexistent")).toBe(0);
    });

    it("@fast counts pages for a file", async () => {
      await backend.writePage("/test", 0, new Uint8Array(PAGE_SIZE));
      await backend.writePage("/test", 1, new Uint8Array(PAGE_SIZE));
      await backend.writePage("/test", 2, new Uint8Array(PAGE_SIZE));
      expect(await backend.countPages("/test")).toBe(3);
    });

    it("does not count pages from other files", async () => {
      await backend.writePage("/a", 0, new Uint8Array(PAGE_SIZE));
      await backend.writePage("/a", 1, new Uint8Array(PAGE_SIZE));
      await backend.writePage("/b", 0, new Uint8Array(PAGE_SIZE));
      expect(await backend.countPages("/a")).toBe(2);
      expect(await backend.countPages("/b")).toBe(1);
    });

    it("returns 0 after deleteFile", async () => {
      await backend.writePage("/test", 0, new Uint8Array(PAGE_SIZE));
      await backend.writePage("/test", 1, new Uint8Array(PAGE_SIZE));
      await backend.deleteFile("/test");
      expect(await backend.countPages("/test")).toBe(0);
    });

    it("reflects deletePagesFrom", async () => {
      await backend.writePage("/test", 0, new Uint8Array(PAGE_SIZE));
      await backend.writePage("/test", 1, new Uint8Array(PAGE_SIZE));
      await backend.writePage("/test", 2, new Uint8Array(PAGE_SIZE));
      await backend.deletePagesFrom("/test", 1);
      expect(await backend.countPages("/test")).toBe(1);
    });
  });

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
      // Unrelated file untouched
      expect(await backend.readPage("/other", 0)).not.toBeNull();
    });

    it("is a no-op when old path has no pages", async () => {
      await backend.renameFile("/nonexistent", "/new");
      expect(await backend.readPage("/new", 0)).toBeNull();
    });

    it("overwrites existing pages at destination", async () => {
      const d0 = new Uint8Array(PAGE_SIZE);
      d0[0] = 0x11;
      await backend.writePage("/dest", 0, d0);

      const d1 = new Uint8Array(PAGE_SIZE);
      d1[0] = 0x22;
      await backend.writePage("/src", 0, d1);

      await backend.renameFile("/src", "/dest");

      expect((await backend.readPage("/dest", 0))![0]).toBe(0x22);
      expect(await backend.readPage("/src", 0)).toBeNull();
    });

    it("cleans up extra destination pages when source has fewer pages", async () => {
      // Destination has 4 pages, source has 2 — extra pages must not survive.
      for (let i = 0; i < 4; i++) {
        const d = new Uint8Array(PAGE_SIZE);
        d.fill(0xdd);
        await backend.writePage("/dest", i, d);
      }
      const s0 = new Uint8Array(PAGE_SIZE);
      s0.fill(0xaa);
      const s1 = new Uint8Array(PAGE_SIZE);
      s1.fill(0xbb);
      await backend.writePage("/src", 0, s0);
      await backend.writePage("/src", 1, s1);

      await backend.renameFile("/src", "/dest");

      expect((await backend.readPage("/dest", 0))![0]).toBe(0xaa);
      expect((await backend.readPage("/dest", 1))![0]).toBe(0xbb);
      // Orphan pages from old destination must be gone
      expect(await backend.readPage("/dest", 2)).toBeNull();
      expect(await backend.readPage("/dest", 3)).toBeNull();
      // Source is gone
      expect(await backend.readPage("/src", 0)).toBeNull();
      expect(await backend.readPage("/src", 1)).toBeNull();
    });
  });

  describe("metadata operations", () => {
    it("@fast returns null for non-existent metadata", async () => {
      expect(await backend.readMeta("/test")).toBeNull();
    });

    it("writes and reads metadata", async () => {
      const meta = { size: 1024, mode: 0o644, ctime: 1000, mtime: 2000 };
      await backend.writeMeta("/test", meta);

      const read = await backend.readMeta("/test");
      expect(read).toEqual(meta);
    });

    it("returns a copy of metadata", async () => {
      const meta = { size: 1024, mode: 0o644, ctime: 1000, mtime: 2000 };
      await backend.writeMeta("/test", meta);

      const read = await backend.readMeta("/test");
      read!.size = 9999;

      const read2 = await backend.readMeta("/test");
      expect(read2!.size).toBe(1024);
    });

    it("deleteMeta removes metadata", async () => {
      await backend.writeMeta("/test", {
        size: 0,
        mode: 0o644,
        ctime: 0,
        mtime: 0,
      });
      await backend.deleteMeta("/test");
      expect(await backend.readMeta("/test")).toBeNull();
    });
  });

  describe("batch metadata write", () => {
    it("@fast writes multiple metadata entries in one call", async () => {
      await backend.writeMetas([
        { path: "/a", meta: { size: 100, mode: 0o644, ctime: 1, mtime: 2 } },
        { path: "/b", meta: { size: 200, mode: 0o755, ctime: 3, mtime: 4 } },
        { path: "/c/d", meta: { size: 0, mode: 0o40755, ctime: 5, mtime: 6 } },
      ]);

      const a = await backend.readMeta("/a");
      const b = await backend.readMeta("/b");
      const cd = await backend.readMeta("/c/d");
      expect(a).toEqual({ size: 100, mode: 0o644, ctime: 1, mtime: 2 });
      expect(b).toEqual({ size: 200, mode: 0o755, ctime: 3, mtime: 4 });
      expect(cd).toEqual({ size: 0, mode: 0o40755, ctime: 5, mtime: 6 });
    });

    it("overwrites existing metadata", async () => {
      await backend.writeMeta("/a", { size: 100, mode: 0o644, ctime: 1, mtime: 2 });
      await backend.writeMetas([
        { path: "/a", meta: { size: 999, mode: 0o755, ctime: 10, mtime: 20 } },
      ]);
      expect((await backend.readMeta("/a"))!.size).toBe(999);
    });

    it("is a no-op for empty array", async () => {
      await backend.writeMetas([]);
      expect(await backend.listFiles()).toEqual([]);
    });
  });

  describe("batch metadata read", () => {
    it("@fast reads multiple metadata entries in one call", async () => {
      const metaA = { size: 100, mode: 0o644, ctime: 1, mtime: 2 };
      const metaB = { size: 200, mode: 0o755, ctime: 3, mtime: 4 };
      await backend.writeMeta("/a", metaA);
      await backend.writeMeta("/b", metaB);

      const results = await backend.readMetas(["/a", "/b", "/nonexistent"]);
      expect(results).toHaveLength(3);
      expect(results[0]).toEqual(metaA);
      expect(results[1]).toEqual(metaB);
      expect(results[2]).toBeNull();
    });

    it("returns empty array for empty input", async () => {
      const results = await backend.readMetas([]);
      expect(results).toEqual([]);
    });

    it("returns all nulls for non-existent paths", async () => {
      const results = await backend.readMetas(["/x", "/y"]);
      expect(results).toEqual([null, null]);
    });

    it("returns independent copies", async () => {
      const meta = { size: 100, mode: 0o644, ctime: 1, mtime: 2 };
      await backend.writeMeta("/a", meta);

      const results = await backend.readMetas(["/a", "/a"]);
      expect(results[0]).toEqual(results[1]);
      results[0]!.size = 999;
      expect(results[1]!.size).toBe(100);
    });
  });

  describe("batch metadata delete", () => {
    it("@fast deletes multiple metadata entries in one call", async () => {
      const meta = { size: 0, mode: 0o644, ctime: 0, mtime: 0 };
      await backend.writeMeta("/a", meta);
      await backend.writeMeta("/b", meta);
      await backend.writeMeta("/c", meta);

      await backend.deleteMetas(["/a", "/c"]);

      expect(await backend.readMeta("/a")).toBeNull();
      expect(await backend.readMeta("/b")).not.toBeNull();
      expect(await backend.readMeta("/c")).toBeNull();
    });

    it("is a no-op for empty array", async () => {
      await backend.writeMeta("/a", { size: 0, mode: 0o644, ctime: 0, mtime: 0 });
      await backend.deleteMetas([]);
      expect(await backend.readMeta("/a")).not.toBeNull();
    });

    it("silently ignores non-existent paths", async () => {
      await backend.deleteMetas(["/nonexistent"]);
      // No error thrown
    });
  });

  describe("listFiles", () => {
    it("returns empty array when no files exist", async () => {
      expect(await backend.listFiles()).toEqual([]);
    });

    it("lists all files with metadata", async () => {
      const meta = { size: 0, mode: 0o644, ctime: 0, mtime: 0 };
      await backend.writeMeta("/a", meta);
      await backend.writeMeta("/b", meta);
      await backend.writeMeta("/c/d", meta);

      const files = await backend.listFiles();
      expect(files.sort()).toEqual(["/a", "/b", "/c/d"]);
    });

    it("does not list deleted files", async () => {
      const meta = { size: 0, mode: 0o644, ctime: 0, mtime: 0 };
      await backend.writeMeta("/a", meta);
      await backend.writeMeta("/b", meta);
      await backend.deleteMeta("/a");

      expect(await backend.listFiles()).toEqual(["/b"]);
    });
  });
});
