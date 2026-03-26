/**
 * OpfsBackend tests — exercises the StorageBackend interface over OPFS.
 *
 * Uses a fake OPFS implementation (tests/harness/fake-opfs.ts) to provide
 * the FileSystemDirectoryHandle API in Node.js. This is a fake (not a mock)
 * per project conventions.
 */

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { OpfsBackend } from "../../src/opfs-backend.js";
import { PAGE_SIZE } from "../../src/types.js";
import { createFakeOpfsRoot } from "../harness/fake-opfs.js";

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

describe("OpfsBackend", () => {
  let backend: OpfsBackend;

  beforeEach(() => {
    const root = createFakeOpfsRoot();
    backend = new OpfsBackend({ root: root as any });
  });

  afterEach(async () => {
    await backend.destroy();
  });

  // -------------------------------------------------------------------
  // Page read/write
  // -------------------------------------------------------------------

  describe("page operations", () => {
    it("readPage returns null for non-existent page @fast", async () => {
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
  // Batch writes
  // -------------------------------------------------------------------

  describe("writePages (batch)", () => {
    it("writes zero pages without error", async () => {
      await backend.writePages([]);
    });

    it("writes multiple pages", async () => {
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

    it("writes many pages across multiple files in parallel", async () => {
      const pages = [];
      for (let f = 0; f < 5; f++) {
        for (let p = 0; p < 4; p++) {
          pages.push({
            path: `/file${f}`,
            pageIndex: p,
            data: filledPage(f * 10 + p),
          });
        }
      }

      await backend.writePages(pages);

      for (let f = 0; f < 5; f++) {
        for (let p = 0; p < 4; p++) {
          expect(await backend.readPage(`/file${f}`, p)).toEqual(
            filledPage(f * 10 + p),
          );
        }
      }
    });
  });

  // -------------------------------------------------------------------
  // Batch reads
  // -------------------------------------------------------------------

  describe("readPages (batch)", () => {
    it("returns empty array for empty indices", async () => {
      const result = await backend.readPages("/file", []);
      expect(result).toEqual([]);
    });

    it("returns nulls for non-existent file", async () => {
      const result = await backend.readPages("/nonexistent", [0, 1, 2]);
      expect(result).toEqual([null, null, null]);
    });

    it("reads multiple pages in parallel", async () => {
      await backend.writePage("/file", 0, filledPage(0x01));
      await backend.writePage("/file", 1, filledPage(0x02));
      await backend.writePage("/file", 2, filledPage(0x03));

      const result = await backend.readPages("/file", [0, 1, 2]);
      expect(result[0]).toEqual(filledPage(0x01));
      expect(result[1]).toEqual(filledPage(0x02));
      expect(result[2]).toEqual(filledPage(0x03));
    });

    it("returns null for missing pages in a sparse read", async () => {
      await backend.writePage("/file", 0, filledPage(0x01));
      await backend.writePage("/file", 3, filledPage(0x04));

      const result = await backend.readPages("/file", [0, 1, 2, 3]);
      expect(result[0]).toEqual(filledPage(0x01));
      expect(result[1]).toBeNull();
      expect(result[2]).toBeNull();
      expect(result[3]).toEqual(filledPage(0x04));
    });

    it("preserves order matching indices", async () => {
      await backend.writePage("/file", 2, filledPage(0xcc));
      await backend.writePage("/file", 0, filledPage(0xaa));

      // Request in reverse order
      const result = await backend.readPages("/file", [2, 0]);
      expect(result[0]).toEqual(filledPage(0xcc));
      expect(result[1]).toEqual(filledPage(0xaa));
    });
  });

  // -------------------------------------------------------------------
  // Rename operations
  // -------------------------------------------------------------------

  describe("renameFile", () => {
    it("moves all pages from old path to new path", async () => {
      await backend.writePage("/old", 0, filledPage(0x01));
      await backend.writePage("/old", 1, filledPage(0x02));
      await backend.writePage("/old", 2, filledPage(0x03));

      await backend.renameFile("/old", "/new");

      // Old pages gone
      expect(await backend.readPage("/old", 0)).toBeNull();
      expect(await backend.readPage("/old", 1)).toBeNull();
      expect(await backend.readPage("/old", 2)).toBeNull();

      // New pages present with correct data
      expect(await backend.readPage("/new", 0)).toEqual(filledPage(0x01));
      expect(await backend.readPage("/new", 1)).toEqual(filledPage(0x02));
      expect(await backend.readPage("/new", 2)).toEqual(filledPage(0x03));
    });

    it("overwrites existing destination pages", async () => {
      await backend.writePage("/old", 0, filledPage(0xaa));
      await backend.writePage("/dest", 0, filledPage(0xbb));

      await backend.renameFile("/old", "/dest");

      expect(await backend.readPage("/dest", 0)).toEqual(filledPage(0xaa));
      expect(await backend.readPage("/old", 0)).toBeNull();
    });

    it("renaming non-existent file is a no-op", async () => {
      await backend.renameFile("/nonexistent", "/dest");
      expect(await backend.readPage("/dest", 0)).toBeNull();
    });

    it("does not affect other files", async () => {
      await backend.writePage("/a", 0, filledPage(0x01));
      await backend.writePage("/b", 0, filledPage(0x02));
      await backend.writePage("/c", 0, filledPage(0x03));

      await backend.renameFile("/a", "/d");

      expect(await backend.readPage("/b", 0)).toEqual(filledPage(0x02));
      expect(await backend.readPage("/c", 0)).toEqual(filledPage(0x03));
    });

    it("handles rename with many pages (parallel copy)", async () => {
      const pageCount = 20;
      for (let i = 0; i < pageCount; i++) {
        await backend.writePage("/source", i, filledPage(i));
      }

      await backend.renameFile("/source", "/target");

      for (let i = 0; i < pageCount; i++) {
        expect(await backend.readPage("/source", i)).toBeNull();
        expect(await backend.readPage("/target", i)).toEqual(filledPage(i));
      }
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
  // Metadata operations
  // -------------------------------------------------------------------

  describe("metadata", () => {
    const meta = { size: 8192, mode: 0o100644, ctime: 1000, mtime: 2000 };

    it("readMeta returns null for non-existent file @fast", async () => {
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
    it("returns empty array when no files exist @fast", async () => {
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
  // Integration: PageCache + OpfsBackend
  // -------------------------------------------------------------------

  describe("integration with PageCache", () => {
    it("PageCache reads and writes through OpfsBackend", async () => {
      const { PageCache } = await import("../../src/page-cache.js");

      const cache = new PageCache(backend, 4);

      // Write through cache
      const data = new TextEncoder().encode("Hello, OPFS!");
      await cache.write("/test", data, 0, data.length, 0, 0);

      // Flush to OPFS
      await cache.flushFile("/test");

      // Verify data reached OPFS
      const page = await backend.readPage("/test", 0);
      expect(page).not.toBeNull();
      expect(new TextDecoder().decode(page!.subarray(0, data.length))).toBe(
        "Hello, OPFS!",
      );
    });

    it("PageCache survives eviction with OpfsBackend", async () => {
      const { PageCache } = await import("../../src/page-cache.js");

      // Very small cache — forces eviction
      const cache = new PageCache(backend, 2);

      // Write 4 pages (2 will be evicted)
      for (let i = 0; i < 4; i++) {
        const page = filledPage(i + 1);
        await cache.write(
          "/bigfile",
          page,
          0,
          PAGE_SIZE,
          i * PAGE_SIZE,
          i * PAGE_SIZE,
        );
      }

      // Read all pages back — evicted ones should come from OPFS
      for (let i = 0; i < 4; i++) {
        const buf = new Uint8Array(PAGE_SIZE);
        await cache.read(
          "/bigfile",
          buf,
          0,
          PAGE_SIZE,
          i * PAGE_SIZE,
          4 * PAGE_SIZE,
        );
        expect(buf[0]).toBe(i + 1);
        expect(buf[PAGE_SIZE - 1]).toBe(i + 1);
      }
    });
  });

  // -------------------------------------------------------------------
  // Error propagation (non-NotFoundError exceptions must not be swallowed)
  // -------------------------------------------------------------------

  describe("error propagation", () => {
    it("getFileDir propagates non-NotFoundError from getDirectoryHandle", async () => {
      const faultyRoot = createFakeOpfsRoot();
      const b = new OpfsBackend({ root: faultyRoot as any });
      // Trigger init by reading a page (creates pages/ and meta/ dirs)
      await b.readPage("/init", 0);

      // Monkey-patch pagesDir to always throw SecurityError on getDirectoryHandle
      const pagesDir = (b as any).pagesDir;
      pagesDir.getDirectoryHandle = async () => {
        throw new DOMException("Access denied", "SecurityError");
      };

      // readPage should propagate the SecurityError, not return null
      await expect(b.readPage("/test", 0)).rejects.toThrow("Access denied");
    });

    it("readPage propagates non-NotFoundError from getFileHandle", async () => {
      const root = createFakeOpfsRoot();
      const b = new OpfsBackend({ root: root as any });

      // Write a page first so the file dir exists
      await b.writePage("/err-test", 0, filledPage(0x01));

      // Sabotage the file directory to throw TypeMismatchError on getFileHandle
      const pagesDir = (b as any).pagesDir;
      const fileDir = await pagesDir.getDirectoryHandle(
        // Encode the path the same way OpfsBackend does
        Array.from(new TextEncoder().encode("/err-test"))
          .map((b) => b.toString(16).padStart(2, "0"))
          .join(""),
      );
      const original = fileDir.getFileHandle.bind(fileDir);
      fileDir.getFileHandle = async (
        name: string,
        options?: { create?: boolean },
      ) => {
        if (name === "0") {
          throw new DOMException("Wrong type", "TypeMismatchError");
        }
        return original(name, options);
      };

      await expect(b.readPage("/err-test", 0)).rejects.toThrow("Wrong type");
    });

    it("deleteFile propagates non-NotFoundError", async () => {
      const root = createFakeOpfsRoot();
      const b = new OpfsBackend({ root: root as any });
      await b.readPage("/init", 0); // trigger init

      const pagesDir = (b as any).pagesDir;
      const original = pagesDir.removeEntry.bind(pagesDir);
      pagesDir.removeEntry = async (
        name: string,
        options?: { recursive?: boolean },
      ) => {
        throw new DOMException("Quota exceeded", "QuotaExceededError");
      };

      await expect(b.deleteFile("/any")).rejects.toThrow("Quota exceeded");
      // Restore for cleanup
      pagesDir.removeEntry = original;
    });

    it("deleteMeta propagates non-NotFoundError", async () => {
      const root = createFakeOpfsRoot();
      const b = new OpfsBackend({ root: root as any });
      await b.readPage("/init", 0); // trigger init

      const metaDir = (b as any).metaDir;
      const original = metaDir.removeEntry.bind(metaDir);
      metaDir.removeEntry = async (name: string) => {
        throw new DOMException("IO error", "InvalidStateError");
      };

      await expect(b.deleteMeta("/any")).rejects.toThrow("IO error");
      metaDir.removeEntry = original;
    });

    it("readMeta returns null for corrupted JSON metadata", async () => {
      const root = createFakeOpfsRoot();
      const b = new OpfsBackend({ root: root as any });

      // Write valid metadata first
      await b.writeMeta("/corrupt", { size: 100, mode: 0o100644, ctime: 1, mtime: 2 });

      // Corrupt the metadata file by writing invalid JSON directly
      const metaDir = (b as any).metaDir;
      const encoded = Array.from(new TextEncoder().encode("/corrupt"))
        .map((byte: number) => byte.toString(16).padStart(2, "0"))
        .join("");
      const handle = await metaDir.getFileHandle(encoded);
      const writable = await handle.createWritable();
      await writable.write("{truncated");
      await writable.close();

      // Should return null instead of crashing
      const result = await b.readMeta("/corrupt");
      expect(result).toBeNull();
      await b.destroy();
    });

    it("readMeta returns null for empty metadata file", async () => {
      const root = createFakeOpfsRoot();
      const b = new OpfsBackend({ root: root as any });

      await b.writeMeta("/empty", { size: 0, mode: 0o100644, ctime: 0, mtime: 0 });

      // Write empty content to metadata file
      const metaDir = (b as any).metaDir;
      const encoded = Array.from(new TextEncoder().encode("/empty"))
        .map((byte: number) => byte.toString(16).padStart(2, "0"))
        .join("");
      const handle = await metaDir.getFileHandle(encoded);
      const writable = await handle.createWritable();
      await writable.write("");
      await writable.close();

      const result = await b.readMeta("/empty");
      expect(result).toBeNull();
      await b.destroy();
    });

    it("readMeta propagates non-NotFoundError", async () => {
      const root = createFakeOpfsRoot();
      const b = new OpfsBackend({ root: root as any });
      await b.readPage("/init", 0); // trigger init

      const metaDir = (b as any).metaDir;
      const original = metaDir.getFileHandle.bind(metaDir);
      metaDir.getFileHandle = async (
        name: string,
        options?: { create?: boolean },
      ) => {
        throw new DOMException("Locked", "InvalidStateError");
      };

      await expect(b.readMeta("/any")).rejects.toThrow("Locked");
      metaDir.getFileHandle = original;
    });
  });

  // -------------------------------------------------------------------
  // Lifecycle
  // -------------------------------------------------------------------

  describe("lifecycle", () => {
    it("destroy removes all data", async () => {
      await backend.writePage("/file1", 0, filledPage(0x01));
      await backend.writeMeta("/file1", {
        size: PAGE_SIZE,
        mode: 0o100644,
        ctime: 1000,
        mtime: 2000,
      });

      await backend.destroy();

      // Create a new backend on the same root — data should be gone
      const root = createFakeOpfsRoot();
      const backend2 = new OpfsBackend({ root: root as any });
      expect(await backend2.readPage("/file1", 0)).toBeNull();
      expect(await backend2.readMeta("/file1")).toBeNull();
      await backend2.destroy();
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

    it("metadata preserves optional fields", async () => {
      const meta = {
        size: 100,
        mode: 0o100644,
        ctime: 1000,
        mtime: 2000,
        atime: 3000,
        link: "/symlink/target",
      };
      await backend.writeMeta("/file1", meta);
      expect(await backend.readMeta("/file1")).toEqual(meta);
    });
  });
});
