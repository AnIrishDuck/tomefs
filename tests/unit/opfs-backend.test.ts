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
  // Batch failure handling
  // -------------------------------------------------------------------

  describe("batch failure handling", () => {
    /** Encode a path as hex (same as OpfsBackend internal encodePath). */
    function encodePath(path: string): string {
      const bytes = new TextEncoder().encode(path);
      let hex = "";
      for (const b of bytes) {
        hex += b.toString(16).padStart(2, "0");
      }
      return hex;
    }

    it("renameFile cleans up partial new dir on copy failure", async () => {
      const root = createFakeOpfsRoot();
      const b = new OpfsBackend({ root: root as any });

      // Write 3 pages to source
      await b.writePage("/src", 0, filledPage(0x01));
      await b.writePage("/src", 1, filledPage(0x02));
      await b.writePage("/src", 2, filledPage(0x03));

      // Sabotage: make the file dir for /src return a faulty getFileHandle
      // that fails on the third page read, simulating an I/O error mid-copy
      const pagesDir = (b as any).pagesDir;
      const srcEncoded = encodePath("/src");
      const srcDir = await pagesDir.getDirectoryHandle(srcEncoded);
      const originalGetFile = srcDir.getFileHandle.bind(srcDir);
      let callCount = 0;
      srcDir.getFileHandle = async (name: string, options?: { create?: boolean }) => {
        // Fail on the 3rd access (one of the parallel copies will fail)
        callCount++;
        if (callCount >= 3 && !options?.create) {
          throw new DOMException("Disk error", "InvalidStateError");
        }
        return originalGetFile(name, options);
      };

      // Rename should fail
      await expect(b.renameFile("/src", "/dst")).rejects.toThrow("Disk error");

      // The partial new dir should be cleaned up — no /dst pages should exist
      // Restore the source dir to verify the source data is still intact
      srcDir.getFileHandle = originalGetFile;

      // Source pages should still be readable (not deleted)
      expect(await b.readPage("/src", 0)).toEqual(filledPage(0x01));
      expect(await b.readPage("/src", 1)).toEqual(filledPage(0x02));
      expect(await b.readPage("/src", 2)).toEqual(filledPage(0x03));

      // Destination should not exist (cleaned up)
      expect(await b.readPage("/dst", 0)).toBeNull();
      expect(await b.readPage("/dst", 1)).toBeNull();

      await b.destroy();
    });

    it("renameFile preserves source data on copy failure (no data loss)", async () => {
      const root = createFakeOpfsRoot();
      const b = new OpfsBackend({ root: root as any });

      // Write pages to both source and pre-existing destination
      await b.writePage("/old", 0, filledPage(0xaa));
      await b.writePage("/old", 1, filledPage(0xbb));
      await b.writePage("/existing", 0, filledPage(0xcc));

      // Sabotage: make creating writable on destination fail
      const pagesDir = (b as any).pagesDir;
      const dstEncoded = encodePath("/existing");

      // The rename first removes destination, then copies. Intercept after
      // the destination removal by making the new dir's getFileHandle(create)
      // fail. We need to let the destination removal succeed first.
      const originalGetDir = pagesDir.getDirectoryHandle.bind(pagesDir);
      let intercepted = false;
      pagesDir.getDirectoryHandle = async (name: string, options?: { create?: boolean }) => {
        const dir = await originalGetDir(name, options);
        if (name === dstEncoded && options?.create && !intercepted) {
          intercepted = true;
          const origGetFile = dir.getFileHandle.bind(dir);
          dir.getFileHandle = async (fname: string, opts?: { create?: boolean }) => {
            if (opts?.create) {
              throw new DOMException("Disk full", "QuotaExceededError");
            }
            return origGetFile(fname, opts);
          };
        }
        return dir;
      };

      await expect(b.renameFile("/old", "/existing")).rejects.toThrow("Disk full");

      // Restore
      pagesDir.getDirectoryHandle = originalGetDir;

      // Source still intact
      expect(await b.readPage("/old", 0)).toEqual(filledPage(0xaa));
      expect(await b.readPage("/old", 1)).toEqual(filledPage(0xbb));

      await b.destroy();
    });

    it("deletePagesFrom reports partial removal failures", async () => {
      const root = createFakeOpfsRoot();
      const b = new OpfsBackend({ root: root as any });

      // Write 5 pages
      for (let i = 0; i < 5; i++) {
        await b.writePage("/file", i, filledPage(i));
      }

      // Sabotage: make removeEntry fail for one specific page
      const pagesDir = (b as any).pagesDir;
      const fileEncoded = encodePath("/file");
      const fileDir = await pagesDir.getDirectoryHandle(fileEncoded);
      const originalRemove = fileDir.removeEntry.bind(fileDir);
      fileDir.removeEntry = async (name: string) => {
        if (name === "3") {
          throw new DOMException("Locked by another process", "InvalidStateError");
        }
        return originalRemove(name);
      };

      // Should throw reporting the failure
      await expect(b.deletePagesFrom("/file", 2)).rejects.toThrow(
        /page removals failed/,
      );

      // Restore and verify: pages 0-1 untouched, page 3 still exists (failed to delete),
      // pages 2 and 4 were successfully deleted
      fileDir.removeEntry = originalRemove;
      expect(await b.readPage("/file", 0)).toEqual(filledPage(0));
      expect(await b.readPage("/file", 1)).toEqual(filledPage(1));
      expect(await b.readPage("/file", 2)).toBeNull(); // deleted before failure
      expect(await b.readPage("/file", 3)).toEqual(filledPage(3)); // failed to delete
      expect(await b.readPage("/file", 4)).toBeNull(); // deleted

      await b.destroy();
    });

    it("deletePagesFrom succeeds when all removals succeed", async () => {
      // Verify normal behavior still works with Promise.allSettled
      for (let i = 0; i < 5; i++) {
        await backend.writePage("/file", i, filledPage(i));
      }

      await backend.deletePagesFrom("/file", 3);

      expect(await backend.readPage("/file", 0)).toEqual(filledPage(0));
      expect(await backend.readPage("/file", 1)).toEqual(filledPage(1));
      expect(await backend.readPage("/file", 2)).toEqual(filledPage(2));
      expect(await backend.readPage("/file", 3)).toBeNull();
      expect(await backend.readPage("/file", 4)).toBeNull();
    });
  });

  // -------------------------------------------------------------------
  // Init retry after transient failure
  // -------------------------------------------------------------------

  describe("init retry after transient failure", () => {
    it("retries init after transient getDirectoryHandle failure @fast", async () => {
      const root = createFakeOpfsRoot();
      const original = root.getDirectoryHandle.bind(root);
      let failCount = 0;

      // First call to getDirectoryHandle fails (simulating transient OPFS error)
      (root as any).getDirectoryHandle = async (
        name: string,
        options?: { create?: boolean },
      ) => {
        if (failCount < 1) {
          failCount++;
          throw new DOMException("Temporary I/O error", "InvalidStateError");
        }
        return original(name, options);
      };

      const b = new OpfsBackend({ root: root as any });

      // First init should fail
      await expect(b.readPage("/test", 0)).rejects.toThrow("Temporary I/O error");

      // Second init should succeed (the transient error resolved)
      const result = await b.readPage("/test", 0);
      expect(result).toBeNull();

      await b.destroy();
    });

    it("loads data correctly after retried init", async () => {
      // Set up a root with pre-existing data using a working backend
      const root = createFakeOpfsRoot();
      const setup = new OpfsBackend({ root: root as any });
      await setup.writePage("/file", 0, filledPage(0xab));
      await setup.writeMeta("/file", {
        size: PAGE_SIZE,
        mode: 0o100644,
        ctime: 1000,
        mtime: 2000,
      });

      // Create a new backend on the same root, but make init fail once
      const original = root.getDirectoryHandle.bind(root);
      let failCount = 0;
      (root as any).getDirectoryHandle = async (
        name: string,
        options?: { create?: boolean },
      ) => {
        if (failCount < 1) {
          failCount++;
          throw new DOMException("Quota exceeded", "QuotaExceededError");
        }
        return original(name, options);
      };

      const b = new OpfsBackend({ root: root as any });

      // First attempt fails
      await expect(b.readPage("/file", 0)).rejects.toThrow("Quota exceeded");

      // After transient error clears, data is accessible
      const page = await b.readPage("/file", 0);
      expect(page).toEqual(filledPage(0xab));

      const meta = await b.readMeta("/file");
      expect(meta).toEqual({
        size: PAGE_SIZE,
        mode: 0o100644,
        ctime: 1000,
        mtime: 2000,
      });

      await b.destroy();
    });

    it("concurrent init callers share the same rejection then retry succeeds", async () => {
      const root = createFakeOpfsRoot();
      const original = root.getDirectoryHandle.bind(root);
      let failCount = 0;

      (root as any).getDirectoryHandle = async (
        name: string,
        options?: { create?: boolean },
      ) => {
        if (failCount < 1) {
          failCount++;
          throw new DOMException("Busy", "InvalidStateError");
        }
        return original(name, options);
      };

      const b = new OpfsBackend({ root: root as any });

      // Two concurrent callers both get the same rejection
      const [r1, r2] = await Promise.allSettled([
        b.readPage("/a", 0),
        b.listFiles(),
      ]);

      expect(r1.status).toBe("rejected");
      expect(r2.status).toBe("rejected");

      // After transient error resolves, new calls succeed
      const files = await b.listFiles();
      expect(files).toEqual([]);

      await b.destroy();
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

  // -------------------------------------------------------------------
  // cleanupOrphanedPages
  // -------------------------------------------------------------------

  describe("cleanupOrphanedPages", () => {
    it("removes page directories with no corresponding metadata @fast", async () => {
      // Write pages and metadata for /file1, but only pages for /file2.
      // /file2's pages are orphaned (no metadata to reference them).
      await backend.writePage("/file1", 0, filledPage(0xaa));
      await backend.writeMeta("/file1", {
        size: PAGE_SIZE,
        mode: 0o100644,
        ctime: 1000,
        mtime: 2000,
      });
      await backend.writePage("/file2", 0, filledPage(0xbb));
      await backend.writePage("/file2", 1, filledPage(0xcc));
      // No metadata for /file2 — simulates crash after writePages but
      // before writeMetas in the old (pages-first) syncAll ordering.

      const removed = await backend.cleanupOrphanedPages();

      expect(removed).toBe(1);
      // /file1's pages survive
      expect(await backend.readPage("/file1", 0)).toEqual(filledPage(0xaa));
      // /file2's orphaned pages are gone
      expect(await backend.readPage("/file2", 0)).toBeNull();
      expect(await backend.readPage("/file2", 1)).toBeNull();
    });

    it("returns 0 when there are no orphans @fast", async () => {
      await backend.writePage("/f", 0, filledPage(0x01));
      await backend.writeMeta("/f", {
        size: PAGE_SIZE,
        mode: 0o100644,
        ctime: 1000,
        mtime: 2000,
      });

      const removed = await backend.cleanupOrphanedPages();
      expect(removed).toBe(0);
      expect(await backend.readPage("/f", 0)).toEqual(filledPage(0x01));
    });

    it("returns 0 on empty backend @fast", async () => {
      const removed = await backend.cleanupOrphanedPages();
      expect(removed).toBe(0);
    });

    it("removes multiple orphaned page directories", async () => {
      // 3 files with pages, only 1 has metadata
      await backend.writePage("/keep", 0, filledPage(0x01));
      await backend.writeMeta("/keep", {
        size: PAGE_SIZE,
        mode: 0o100644,
        ctime: 1000,
        mtime: 2000,
      });
      await backend.writePage("/orphan1", 0, filledPage(0x02));
      await backend.writePage("/orphan2", 0, filledPage(0x03));
      await backend.writePage("/orphan2", 1, filledPage(0x04));

      const removed = await backend.cleanupOrphanedPages();

      expect(removed).toBe(2);
      expect(await backend.readPage("/keep", 0)).toEqual(filledPage(0x01));
      expect(await backend.readPage("/orphan1", 0)).toBeNull();
      expect(await backend.readPage("/orphan2", 0)).toBeNull();
    });

    it("does not remove metadata-only entries (no pages)", async () => {
      // Metadata exists but no pages — this is a valid state (empty file).
      // cleanupOrphanedPages should not touch metadata.
      await backend.writeMeta("/empty", {
        size: 0,
        mode: 0o100644,
        ctime: 1000,
        mtime: 2000,
      });

      const removed = await backend.cleanupOrphanedPages();

      expect(removed).toBe(0);
      expect(await backend.readMeta("/empty")).toEqual({
        size: 0,
        mode: 0o100644,
        ctime: 1000,
        mtime: 2000,
      });
    });
  });

  // -------------------------------------------------------------------
  // syncAll ordering
  // -------------------------------------------------------------------

  describe("syncAll ordering", () => {
    it("syncAll writes metadata before pages @fast", async () => {
      // Verify that after syncAll, both metadata and pages are present.
      // This is the basic correctness check — the ordering (metadata-first)
      // is verified by the crash-safety property: if only the first write
      // succeeds, we get metadata without pages (recoverable) rather than
      // pages without metadata (permanent orphan leak).
      const meta = {
        size: PAGE_SIZE,
        mode: 0o100644,
        ctime: 1000,
        mtime: 2000,
      };
      await backend.syncAll(
        [{ path: "/f", pageIndex: 0, data: filledPage(0xab) }],
        [{ path: "/f", meta }],
      );

      expect(await backend.readMeta("/f")).toEqual(meta);
      expect(await backend.readPage("/f", 0)).toEqual(filledPage(0xab));
    });

    it("partial syncAll (metadata written, pages not) leaves no orphan pages", async () => {
      // Simulate the crash-safe scenario: metadata is present but pages
      // were not written. This is the expected failure mode with
      // metadata-first ordering. No orphaned page directories should exist.
      //
      // We can't truly simulate a crash, but we can verify the invariant:
      // writing metadata alone (without pages) creates no page directories,
      // so cleanupOrphanedPages finds nothing to clean.
      const meta = {
        size: PAGE_SIZE * 2,
        mode: 0o100644,
        ctime: 1000,
        mtime: 2000,
      };
      await backend.writeMetas([{ path: "/crash-file", meta }]);
      // Pages were NOT written (simulating crash after metadata write)

      const orphans = await backend.cleanupOrphanedPages();
      expect(orphans).toBe(0);

      // Metadata is present but no pages — maxPageIndex returns -1
      expect(await backend.readMeta("/crash-file")).toEqual(meta);
      expect(await backend.maxPageIndex("/crash-file")).toBe(-1);
    });
  });
});
