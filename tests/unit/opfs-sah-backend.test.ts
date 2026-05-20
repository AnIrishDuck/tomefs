/**
 * OpfsSahBackend tests — exercises the StorageBackend interface using
 * OPFS sync access handles.
 *
 * Uses the fake OPFS implementation (tests/harness/fake-opfs.ts) which
 * provides createSyncAccessHandle() support for Node.js testing.
 */

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { OpfsSahBackend } from "../../src/opfs-sah-backend.js";
import { PAGE_SIZE } from "../../src/types.js";
import { createFakeOpfsRoot } from "../harness/fake-opfs.js";

function filledPage(value: number): Uint8Array {
  const page = new Uint8Array(PAGE_SIZE);
  page.fill(value);
  return page;
}

function testData(length: number, start = 0): Uint8Array {
  const buf = new Uint8Array(length);
  for (let i = 0; i < length; i++) {
    buf[i] = (start + i) & 0xff;
  }
  return buf;
}

describe("OpfsSahBackend", () => {
  let backend: OpfsSahBackend;

  beforeEach(() => {
    const root = createFakeOpfsRoot();
    backend = new OpfsSahBackend({ root: root as any });
  });

  afterEach(async () => {
    await backend.destroy();
  });

  describe("page operations", () => {
    it("readPage returns null for non-existent file @fast", async () => {
      const result = await backend.readPage("/test", 0);
      expect(result).toBeNull();
    });

    it("writePage then readPage round-trips data @fast", async () => {
      const data = filledPage(0xab);
      await backend.writePage("/file1", 0, data);
      const result = await backend.readPage("/file1", 0);
      expect(result).toEqual(data);
    });

    it("writePage stores a copy, not a reference @fast", async () => {
      const data = filledPage(0x01);
      await backend.writePage("/file1", 0, data);
      data.fill(0xff);
      const result = await backend.readPage("/file1", 0);
      expect(result![0]).toBe(0x01);
    });

    it("handles multiple pages for same file @fast", async () => {
      await backend.writePage("/file1", 0, filledPage(0x01));
      await backend.writePage("/file1", 1, filledPage(0x02));
      await backend.writePage("/file1", 2, filledPage(0x03));

      expect(await backend.readPage("/file1", 0)).toEqual(filledPage(0x01));
      expect(await backend.readPage("/file1", 1)).toEqual(filledPage(0x02));
      expect(await backend.readPage("/file1", 2)).toEqual(filledPage(0x03));
    });

    it("handles pages for different files independently @fast", async () => {
      await backend.writePage("/a", 0, filledPage(0xaa));
      await backend.writePage("/b", 0, filledPage(0xbb));

      expect(await backend.readPage("/a", 0)).toEqual(filledPage(0xaa));
      expect(await backend.readPage("/b", 0)).toEqual(filledPage(0xbb));
    });

    it("overwrites existing page data @fast", async () => {
      await backend.writePage("/file1", 0, filledPage(0x01));
      await backend.writePage("/file1", 0, filledPage(0x02));
      expect(await backend.readPage("/file1", 0)).toEqual(filledPage(0x02));
    });

    it("readPage returns null for page beyond file size @fast", async () => {
      await backend.writePage("/file1", 0, filledPage(0x01));
      const result = await backend.readPage("/file1", 5);
      expect(result).toBeNull();
    });

    it("sparse writes create zero-filled gaps @fast", async () => {
      await backend.writePage("/file1", 0, filledPage(0xaa));
      await backend.writePage("/file1", 3, filledPage(0xbb));

      const page1 = await backend.readPage("/file1", 1);
      expect(page1).toEqual(new Uint8Array(PAGE_SIZE));

      const page2 = await backend.readPage("/file1", 2);
      expect(page2).toEqual(new Uint8Array(PAGE_SIZE));
    });
  });

  describe("batch page operations", () => {
    it("readPages returns array of results @fast", async () => {
      await backend.writePage("/file1", 0, filledPage(0x01));
      await backend.writePage("/file1", 2, filledPage(0x03));

      const results = await backend.readPages("/file1", [0, 1, 2]);
      expect(results[0]).toEqual(filledPage(0x01));
      expect(results[1]).toEqual(new Uint8Array(PAGE_SIZE));
      expect(results[2]).toEqual(filledPage(0x03));
    });

    it("readPages returns nulls for non-existent file @fast", async () => {
      const results = await backend.readPages("/nope", [0, 1, 2]);
      expect(results).toEqual([null, null, null]);
    });

    it("writePages writes multiple pages atomically @fast", async () => {
      await backend.writePages([
        { path: "/file1", pageIndex: 0, data: filledPage(0x10) },
        { path: "/file1", pageIndex: 1, data: filledPage(0x20) },
        { path: "/file2", pageIndex: 0, data: filledPage(0x30) },
      ]);

      expect(await backend.readPage("/file1", 0)).toEqual(filledPage(0x10));
      expect(await backend.readPage("/file1", 1)).toEqual(filledPage(0x20));
      expect(await backend.readPage("/file2", 0)).toEqual(filledPage(0x30));
    });

    it("writePages handles empty array @fast", async () => {
      await backend.writePages([]);
    });
  });

  describe("deleteFile", () => {
    it("removes all pages for a file @fast", async () => {
      await backend.writePage("/file1", 0, filledPage(0x01));
      await backend.writePage("/file1", 1, filledPage(0x02));
      await backend.deleteFile("/file1");

      expect(await backend.readPage("/file1", 0)).toBeNull();
      expect(await backend.readPage("/file1", 1)).toBeNull();
    });

    it("does not affect other files @fast", async () => {
      await backend.writePage("/a", 0, filledPage(0xaa));
      await backend.writePage("/b", 0, filledPage(0xbb));
      await backend.deleteFile("/a");

      expect(await backend.readPage("/b", 0)).toEqual(filledPage(0xbb));
    });

    it("is idempotent for non-existent files @fast", async () => {
      await backend.deleteFile("/nonexistent");
    });
  });

  describe("deleteFiles", () => {
    it("removes multiple files @fast", async () => {
      await backend.writePage("/a", 0, filledPage(0xaa));
      await backend.writePage("/b", 0, filledPage(0xbb));
      await backend.deleteFiles(["/a", "/b"]);

      expect(await backend.readPage("/a", 0)).toBeNull();
      expect(await backend.readPage("/b", 0)).toBeNull();
    });
  });

  describe("deletePagesFrom", () => {
    it("truncates pages at and beyond the given index @fast", async () => {
      await backend.writePage("/file1", 0, filledPage(0x01));
      await backend.writePage("/file1", 1, filledPage(0x02));
      await backend.writePage("/file1", 2, filledPage(0x03));

      await backend.deletePagesFrom("/file1", 1);

      expect(await backend.readPage("/file1", 0)).toEqual(filledPage(0x01));
      expect(await backend.readPage("/file1", 1)).toBeNull();
      expect(await backend.readPage("/file1", 2)).toBeNull();
    });

    it("no-op for non-existent file @fast", async () => {
      await backend.deletePagesFrom("/nonexistent", 0);
    });

    it("truncate at 0 removes all pages @fast", async () => {
      await backend.writePage("/file1", 0, filledPage(0x01));
      await backend.writePage("/file1", 1, filledPage(0x02));
      await backend.deletePagesFrom("/file1", 0);

      expect(await backend.readPage("/file1", 0)).toBeNull();
    });
  });

  describe("renameFile", () => {
    it("moves pages from old path to new path @fast", async () => {
      await backend.writePage("/old", 0, filledPage(0xaa));
      await backend.writePage("/old", 1, filledPage(0xbb));
      await backend.renameFile("/old", "/new");

      expect(await backend.readPage("/old", 0)).toBeNull();
      expect(await backend.readPage("/new", 0)).toEqual(filledPage(0xaa));
      expect(await backend.readPage("/new", 1)).toEqual(filledPage(0xbb));
    });

    it("overwrites destination if it exists @fast", async () => {
      await backend.writePage("/src", 0, filledPage(0x11));
      await backend.writePage("/dst", 0, filledPage(0x22));
      await backend.renameFile("/src", "/dst");

      expect(await backend.readPage("/dst", 0)).toEqual(filledPage(0x11));
      expect(await backend.readPage("/src", 0)).toBeNull();
    });

    it("no-op when old and new are the same @fast", async () => {
      await backend.writePage("/same", 0, filledPage(0x33));
      await backend.renameFile("/same", "/same");
      expect(await backend.readPage("/same", 0)).toEqual(filledPage(0x33));
    });

    it("no-op when source does not exist @fast", async () => {
      await backend.renameFile("/nonexistent", "/target");
    });
  });

  describe("countPages", () => {
    it("returns 0 for non-existent file @fast", async () => {
      expect(await backend.countPages("/nope")).toBe(0);
    });

    it("returns page count for existing file @fast", async () => {
      await backend.writePage("/file1", 0, filledPage(0x01));
      await backend.writePage("/file1", 1, filledPage(0x02));
      expect(await backend.countPages("/file1")).toBe(2);
    });

    it("accounts for sparse pages @fast", async () => {
      await backend.writePage("/file1", 0, filledPage(0x01));
      await backend.writePage("/file1", 4, filledPage(0x05));
      expect(await backend.countPages("/file1")).toBe(5);
    });
  });

  describe("countPagesBatch", () => {
    it("returns counts for multiple files @fast", async () => {
      await backend.writePage("/a", 0, filledPage(0x01));
      await backend.writePage("/a", 1, filledPage(0x02));
      await backend.writePage("/b", 0, filledPage(0x03));

      const counts = await backend.countPagesBatch(["/a", "/b", "/c"]);
      expect(counts).toEqual([2, 1, 0]);
    });
  });

  describe("maxPageIndex", () => {
    it("returns -1 for non-existent file @fast", async () => {
      expect(await backend.maxPageIndex("/nope")).toBe(-1);
    });

    it("returns highest page index @fast", async () => {
      await backend.writePage("/file1", 0, filledPage(0x01));
      await backend.writePage("/file1", 3, filledPage(0x04));
      expect(await backend.maxPageIndex("/file1")).toBe(3);
    });
  });

  describe("maxPageIndexBatch", () => {
    it("returns indices for multiple files @fast", async () => {
      await backend.writePage("/a", 0, filledPage(0x01));
      await backend.writePage("/a", 2, filledPage(0x03));
      await backend.writePage("/b", 0, filledPage(0x01));

      const indices = await backend.maxPageIndexBatch(["/a", "/b", "/c"]);
      expect(indices).toEqual([2, 0, -1]);
    });
  });

  describe("metadata operations", () => {
    const meta = { size: 1024, mode: 0o644, ctime: 1000, mtime: 2000 };

    it("readMeta returns null for non-existent file @fast", async () => {
      expect(await backend.readMeta("/test")).toBeNull();
    });

    it("writeMeta then readMeta round-trips @fast", async () => {
      await backend.writeMeta("/file1", meta);
      expect(await backend.readMeta("/file1")).toEqual(meta);
    });

    it("writeMeta overwrites existing metadata @fast", async () => {
      await backend.writeMeta("/file1", meta);
      const updated = { ...meta, size: 2048, mtime: 3000 };
      await backend.writeMeta("/file1", updated);
      expect(await backend.readMeta("/file1")).toEqual(updated);
    });

    it("deleteMeta removes metadata @fast", async () => {
      await backend.writeMeta("/file1", meta);
      await backend.deleteMeta("/file1");
      expect(await backend.readMeta("/file1")).toBeNull();
    });

    it("deleteMeta is idempotent @fast", async () => {
      await backend.deleteMeta("/nonexistent");
    });

    it("readMetas batch @fast", async () => {
      await backend.writeMeta("/a", meta);
      const results = await backend.readMetas(["/a", "/b"]);
      expect(results[0]).toEqual(meta);
      expect(results[1]).toBeNull();
    });

    it("writeMetas batch @fast", async () => {
      const meta2 = { ...meta, size: 512 };
      await backend.writeMetas([
        { path: "/a", meta },
        { path: "/b", meta: meta2 },
      ]);
      expect(await backend.readMeta("/a")).toEqual(meta);
      expect(await backend.readMeta("/b")).toEqual(meta2);
    });

    it("deleteMetas batch @fast", async () => {
      await backend.writeMeta("/a", meta);
      await backend.writeMeta("/b", meta);
      await backend.deleteMetas(["/a", "/b"]);
      expect(await backend.readMeta("/a")).toBeNull();
      expect(await backend.readMeta("/b")).toBeNull();
    });
  });

  describe("listFiles", () => {
    it("returns empty array when no files exist @fast", async () => {
      expect(await backend.listFiles()).toEqual([]);
    });

    it("returns paths with metadata @fast", async () => {
      const meta = { size: 0, mode: 0o644, ctime: 1000, mtime: 1000 };
      await backend.writeMeta("/a", meta);
      await backend.writeMeta("/b", meta);
      const files = await backend.listFiles();
      expect(files.sort()).toEqual(["/a", "/b"]);
    });
  });

  describe("syncAll", () => {
    it("writes pages and metadata together @fast", async () => {
      const meta = { size: PAGE_SIZE, mode: 0o644, ctime: 1000, mtime: 1000 };
      await backend.syncAll(
        [{ path: "/file1", pageIndex: 0, data: filledPage(0xab) }],
        [{ path: "/file1", meta }],
      );

      expect(await backend.readPage("/file1", 0)).toEqual(filledPage(0xab));
      expect(await backend.readMeta("/file1")).toEqual(meta);
    });
  });

  describe("deleteAll", () => {
    it("removes pages and metadata @fast", async () => {
      const meta = { size: PAGE_SIZE, mode: 0o644, ctime: 1000, mtime: 1000 };
      await backend.writePage("/file1", 0, filledPage(0x01));
      await backend.writeMeta("/file1", meta);

      await backend.deleteAll(["/file1"]);

      expect(await backend.readPage("/file1", 0)).toBeNull();
      expect(await backend.readMeta("/file1")).toBeNull();
    });
  });

  describe("handle cache", () => {
    it("evicts handles when maxOpenHandles exceeded @fast", async () => {
      const smallBackend = new OpfsSahBackend({
        root: createFakeOpfsRoot() as any,
        maxOpenHandles: 3,
      });

      for (let i = 0; i < 5; i++) {
        await smallBackend.writePage(`/file${i}`, 0, filledPage(i));
      }

      for (let i = 0; i < 5; i++) {
        expect(await smallBackend.readPage(`/file${i}`, 0)).toEqual(
          filledPage(i),
        );
      }

      await smallBackend.destroy();
    });

    it("reuses cached handles for same path @fast", async () => {
      await backend.writePage("/file1", 0, filledPage(0x01));
      await backend.writePage("/file1", 1, filledPage(0x02));
      await backend.readPage("/file1", 0);
      await backend.readPage("/file1", 1);

      expect(await backend.readPage("/file1", 0)).toEqual(filledPage(0x01));
      expect(await backend.readPage("/file1", 1)).toEqual(filledPage(0x02));
    });
  });

  describe("cleanupOrphanedPages", () => {
    it("removes page files with no metadata @fast", async () => {
      await backend.writePage("/file1", 0, filledPage(0x01));
      const meta = { size: PAGE_SIZE, mode: 0o644, ctime: 1000, mtime: 1000 };
      await backend.writeMeta("/file2", meta);
      await backend.writePage("/file2", 0, filledPage(0x02));

      const removed = await backend.cleanupOrphanedPages();
      expect(removed).toBe(1);
      expect(await backend.readPage("/file1", 0)).toBeNull();
      expect(await backend.readPage("/file2", 0)).toEqual(filledPage(0x02));
    });

    it("returns 0 when no orphans exist @fast", async () => {
      const meta = { size: PAGE_SIZE, mode: 0o644, ctime: 1000, mtime: 1000 };
      await backend.writeMeta("/file1", meta);
      await backend.writePage("/file1", 0, filledPage(0x01));

      const removed = await backend.cleanupOrphanedPages();
      expect(removed).toBe(0);
    });
  });

  describe("destroy", () => {
    it("removes all data @fast", async () => {
      const meta = { size: PAGE_SIZE, mode: 0o644, ctime: 1000, mtime: 1000 };
      await backend.writePage("/file1", 0, filledPage(0x01));
      await backend.writeMeta("/file1", meta);
      await backend.destroy();

      const fresh = new OpfsSahBackend({
        root: createFakeOpfsRoot() as any,
      });
      expect(await fresh.readPage("/file1", 0)).toBeNull();
      expect(await fresh.readMeta("/file1")).toBeNull();
      await fresh.destroy();
    });
  });
});
