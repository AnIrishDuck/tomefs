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

    it("destroy rejects when another connection blocks deletion @fast", async () => {
      const dbName = `tomefs-destroy-blocked-${dbCounter++}`;
      const b = new IdbBackend({ dbName });
      await b.writePage("/test", 0, filledPage(0x01));

      const holdingConn = await new Promise<IDBDatabase>((resolve, reject) => {
        const req = indexedDB.open(dbName, 1);
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error);
      });

      try {
        await expect(b.destroy()).rejects.toThrow(/blocked/i);
      } finally {
        holdingConn.close();
        await new Promise<void>((resolve, reject) => {
          const req = indexedDB.deleteDatabase(dbName);
          req.onsuccess = () => resolve();
          req.onerror = () => reject(req.error);
        });
      }
    });

    it("destroy succeeds after blocking connection closes @fast", async () => {
      const dbName = `tomefs-destroy-unblocked-${dbCounter++}`;
      const b = new IdbBackend({ dbName });
      await b.writePage("/test", 0, filledPage(0x01));

      const b2 = new IdbBackend({ dbName });
      await b2.readPage("/test", 0);
      b2.close();

      await b.destroy();

      const b3 = new IdbBackend({ dbName });
      expect(await b3.readPage("/test", 0)).toBeNull();
      await b3.destroy();
    });
  });

  // -------------------------------------------------------------------
  // Edge cases
  // -------------------------------------------------------------------

  // -------------------------------------------------------------------
  // Transaction abort handling
  // -------------------------------------------------------------------

  describe("transaction abort handling", () => {
    async function openTestDb(name: string): Promise<IDBDatabase> {
      return new Promise((resolve, reject) => {
        const req = indexedDB.open(name, 1);
        req.onupgradeneeded = () => {
          const db = req.result;
          if (!db.objectStoreNames.contains("pages"))
            db.createObjectStore("pages");
          if (!db.objectStoreNames.contains("file_meta"))
            db.createObjectStore("file_meta");
        };
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error);
      });
    }

    async function destroyTestDb(name: string, db: IDBDatabase): Promise<void> {
      db.close();
      return new Promise((resolve, reject) => {
        const req = indexedDB.deleteDatabase(name);
        req.onsuccess = () => resolve();
        req.onerror = () => reject(req.error);
      });
    }

    it("onabort rejects when abort fires after request completes @fast", async () => {
      const dbName = `tomefs-abort-${dbCounter++}`;
      const db = await openTestDb(dbName);

      const promise = new Promise<void>((resolve, reject) => {
        const tx = db.transaction("pages", "readwrite");
        const store = tx.objectStore("pages");
        const req = store.put(new Uint8Array(8), ["test", 0]);

        req.onsuccess = () => {
          tx.abort();
        };

        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(tx.error);
        tx.onabort = () =>
          reject(tx.error || new Error("IDB transaction aborted"));
      });

      await expect(promise).rejects.toThrow();

      await destroyTestDb(dbName, db);
    });

    it("without onabort the promise would not settle on abort @fast", async () => {
      const dbName = `tomefs-abort-nohandler-${dbCounter++}`;
      const db = await openTestDb(dbName);

      const events: string[] = [];
      new Promise<void>((resolve, reject) => {
        const tx = db.transaction("pages", "readwrite");
        const store = tx.objectStore("pages");
        const req = store.put(new Uint8Array(8), ["test", 0]);

        req.onsuccess = () => {
          tx.abort();
        };

        tx.oncomplete = () => {
          events.push("complete");
          resolve();
        };
        tx.onerror = () => {
          events.push("error");
          reject(tx.error);
        };
        // Deliberately NO onabort — simulates the old code
        tx.addEventListener("abort", () => {
          events.push("abort");
        });
      });

      // Let microtasks settle
      await new Promise((r) => setTimeout(r, 50));

      // The abort event fired but neither complete nor error did —
      // without onabort, the Promise hangs forever.
      expect(events).toContain("abort");
      expect(events).not.toContain("complete");
      expect(events).not.toContain("error");

      await destroyTestDb(dbName, db);
    });

    it("IdbBackend writePage rejects when db connection is closed @fast", async () => {
      const dbName = `tomefs-close-abort-${dbCounter++}`;
      const db = await openTestDb(dbName);
      const b = new IdbBackend({ db });

      await b.writePage("/file", 0, filledPage(0x01));

      db.close();

      await expect(
        b.writePage("/file", 1, filledPage(0x02)),
      ).rejects.toThrow();

      await new Promise<void>((resolve, reject) => {
        const req = indexedDB.deleteDatabase(dbName);
        req.onsuccess = () => resolve();
        req.onerror = () => reject(req.error);
      });
    });

    it("IdbBackend syncAll rejects when db connection is closed @fast", async () => {
      const dbName = `tomefs-close-syncall-${dbCounter++}`;
      const db = await openTestDb(dbName);
      const b = new IdbBackend({ db });

      await b.writePage("/file", 0, filledPage(0x01));

      db.close();

      await expect(
        b.syncAll(
          [{ path: "/file", pageIndex: 1, data: filledPage(0x02) }],
          [{ path: "/file", meta: { size: PAGE_SIZE, mode: 0o100644, ctime: 0, mtime: 0 } }],
        ),
      ).rejects.toThrow();

      await new Promise<void>((resolve, reject) => {
        const req = indexedDB.deleteDatabase(dbName);
        req.onsuccess = () => resolve();
        req.onerror = () => reject(req.error);
      });
    });

    it("IdbBackend readMeta rejects when db connection is closed @fast", async () => {
      const dbName = `tomefs-close-readmeta-${dbCounter++}`;
      const db = await openTestDb(dbName);
      const b = new IdbBackend({ db });

      await b.writeMeta("/file", { size: 0, mode: 0o100644, ctime: 0, mtime: 0 });

      db.close();

      await expect(b.readMeta("/file")).rejects.toThrow();

      await new Promise<void>((resolve, reject) => {
        const req = indexedDB.deleteDatabase(dbName);
        req.onsuccess = () => resolve();
        req.onerror = () => reject(req.error);
      });
    });

    it("IdbBackend listFiles rejects when db connection is closed @fast", async () => {
      const dbName = `tomefs-close-listfiles-${dbCounter++}`;
      const db = await openTestDb(dbName);
      const b = new IdbBackend({ db });

      await b.writeMeta("/file", { size: 0, mode: 0o100644, ctime: 0, mtime: 0 });

      db.close();

      await expect(b.listFiles()).rejects.toThrow();

      await new Promise<void>((resolve, reject) => {
        const req = indexedDB.deleteDatabase(dbName);
        req.onsuccess = () => resolve();
        req.onerror = () => reject(req.error);
      });
    });

    it("IdbBackend readPage rejects when db connection is closed @fast", async () => {
      const dbName = `tomefs-close-readpage-${dbCounter++}`;
      const db = await openTestDb(dbName);
      const b = new IdbBackend({ db });

      await b.writePage("/file", 0, filledPage(0x01));

      db.close();

      await expect(b.readPage("/file", 0)).rejects.toThrow();

      await new Promise<void>((resolve, reject) => {
        const req = indexedDB.deleteDatabase(dbName);
        req.onsuccess = () => resolve();
        req.onerror = () => reject(req.error);
      });
    });

    it("IdbBackend countPages rejects when db connection is closed @fast", async () => {
      const dbName = `tomefs-close-countpages-${dbCounter++}`;
      const db = await openTestDb(dbName);
      const b = new IdbBackend({ db });

      await b.writePage("/file", 0, filledPage(0x01));

      db.close();

      await expect(b.countPages("/file")).rejects.toThrow();

      await new Promise<void>((resolve, reject) => {
        const req = indexedDB.deleteDatabase(dbName);
        req.onsuccess = () => resolve();
        req.onerror = () => reject(req.error);
      });
    });

    it("IdbBackend maxPageIndex rejects when db connection is closed @fast", async () => {
      const dbName = `tomefs-close-maxpageidx-${dbCounter++}`;
      const db = await openTestDb(dbName);
      const b = new IdbBackend({ db });

      await b.writePage("/file", 0, filledPage(0x01));

      db.close();

      await expect(b.maxPageIndex("/file")).rejects.toThrow();

      await new Promise<void>((resolve, reject) => {
        const req = indexedDB.deleteDatabase(dbName);
        req.onsuccess = () => resolve();
        req.onerror = () => reject(req.error);
      });
    });
  });

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

  // -------------------------------------------------------------------
  // Durability option
  // -------------------------------------------------------------------

  describe("durability option", () => {
    it("works with durability: strict", async () => {
      const strictBackend = new IdbBackend({
        dbName: `tomefs-strict-${dbCounter++}`,
        durability: "strict",
      });
      try {
        const data = filledPage(0xcc);
        await strictBackend.writePage("/test", 0, data);
        const result = await strictBackend.readPage("/test", 0);
        expect(result).toEqual(data);
      } finally {
        await strictBackend.destroy();
      }
    });

    it("works with durability: relaxed", async () => {
      const relaxedBackend = new IdbBackend({
        dbName: `tomefs-relaxed-${dbCounter++}`,
        durability: "relaxed",
      });
      try {
        const data = filledPage(0xdd);
        await relaxedBackend.writePage("/test", 0, data);
        const result = await relaxedBackend.readPage("/test", 0);
        expect(result).toEqual(data);
      } finally {
        await relaxedBackend.destroy();
      }
    });

    it("syncAll respects durability option", async () => {
      const strictBackend = new IdbBackend({
        dbName: `tomefs-sync-strict-${dbCounter++}`,
        durability: "strict",
      });
      try {
        await strictBackend.syncAll(
          [{ path: "/f", pageIndex: 0, data: filledPage(0xab) }],
          [{ path: "/f", meta: { size: PAGE_SIZE, mode: 0o100644, ctime: 1, mtime: 2 } }],
        );
        const page = await strictBackend.readPage("/f", 0);
        expect(page).toEqual(filledPage(0xab));
        const meta = await strictBackend.readMeta("/f");
        expect(meta?.size).toBe(PAGE_SIZE);
      } finally {
        await strictBackend.destroy();
      }
    });

    it("deleteAll respects durability option", async () => {
      const strictBackend = new IdbBackend({
        dbName: `tomefs-del-strict-${dbCounter++}`,
        durability: "strict",
      });
      try {
        await strictBackend.writePage("/a", 0, filledPage(0x11));
        await strictBackend.writeMeta("/a", { size: PAGE_SIZE, mode: 0o100644, ctime: 1, mtime: 2 });
        await strictBackend.deleteAll(["/a"]);
        expect(await strictBackend.readPage("/a", 0)).toBeNull();
        expect(await strictBackend.readMeta("/a")).toBeNull();
      } finally {
        await strictBackend.destroy();
      }
    });

    it("writePages batch respects durability option", async () => {
      const strictBackend = new IdbBackend({
        dbName: `tomefs-batch-strict-${dbCounter++}`,
        durability: "strict",
      });
      try {
        await strictBackend.writePages([
          { path: "/x", pageIndex: 0, data: filledPage(0x01) },
          { path: "/x", pageIndex: 1, data: filledPage(0x02) },
        ]);
        expect(await strictBackend.readPage("/x", 0)).toEqual(filledPage(0x01));
        expect(await strictBackend.readPage("/x", 1)).toEqual(filledPage(0x02));
      } finally {
        await strictBackend.destroy();
      }
    });
  });

  // -------------------------------------------------------------------
  // cleanupOrphanedPages
  // -------------------------------------------------------------------

  describe("cleanupOrphanedPages", () => {
    it("removes pages with no corresponding metadata @fast", async () => {
      await backend.writePage("/file1", 0, filledPage(0xaa));
      await backend.writeMeta("/file1", {
        size: PAGE_SIZE,
        mode: 0o100644,
        ctime: 1000,
        mtime: 2000,
      });
      await backend.writePage("/file2", 0, filledPage(0xbb));
      await backend.writePage("/file2", 1, filledPage(0xcc));

      const removed = await backend.cleanupOrphanedPages();

      expect(removed).toBe(1);
      expect(await backend.readPage("/file1", 0)).toEqual(filledPage(0xaa));
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

    it("removes multiple orphaned paths", async () => {
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

    it("idempotent: second call returns 0", async () => {
      await backend.writePage("/orphan", 0, filledPage(0xff));

      expect(await backend.cleanupOrphanedPages()).toBe(1);
      expect(await backend.cleanupOrphanedPages()).toBe(0);
    });

    it("handles mixed: some files have metadata, some do not", async () => {
      await backend.writePage("/a", 0, filledPage(0x01));
      await backend.writeMeta("/a", { size: PAGE_SIZE, mode: 0o100644, ctime: 1, mtime: 2 });
      await backend.writePage("/b", 0, filledPage(0x02));
      await backend.writePage("/c", 0, filledPage(0x03));
      await backend.writeMeta("/c", { size: PAGE_SIZE, mode: 0o100644, ctime: 1, mtime: 2 });
      await backend.writePage("/d", 0, filledPage(0x04));
      await backend.writePage("/d", 1, filledPage(0x05));
      await backend.writePage("/d", 2, filledPage(0x06));

      const removed = await backend.cleanupOrphanedPages();

      expect(removed).toBe(2);
      expect(await backend.readPage("/a", 0)).toEqual(filledPage(0x01));
      expect(await backend.readPage("/b", 0)).toBeNull();
      expect(await backend.readPage("/c", 0)).toEqual(filledPage(0x03));
      expect(await backend.readPage("/d", 0)).toBeNull();
      expect(await backend.countPages("/d")).toBe(0);
    });

    it("orphan cleanup after deleteFile + crash (metadata deleted, pages remain)", async () => {
      await backend.writePage("/victim", 0, filledPage(0x11));
      await backend.writePage("/victim", 1, filledPage(0x22));
      await backend.writeMeta("/victim", { size: PAGE_SIZE * 2, mode: 0o100644, ctime: 1, mtime: 2 });

      // Simulate: deleteMeta succeeded but deleteFile (pages) did not
      await backend.deleteMeta("/victim");

      expect(await backend.readPage("/victim", 0)).toEqual(filledPage(0x11));
      const removed = await backend.cleanupOrphanedPages();
      expect(removed).toBe(1);
      expect(await backend.readPage("/victim", 0)).toBeNull();
      expect(await backend.readPage("/victim", 1)).toBeNull();
    });

    it("correctly identifies orphans among files with many pages per file", async () => {
      const meta = { size: PAGE_SIZE * 20, mode: 0o100644, ctime: 1, mtime: 2 };

      for (let i = 0; i < 20; i++) {
        await backend.writePage("/keep", i, filledPage(0x01));
      }
      await backend.writeMeta("/keep", meta);

      for (let i = 0; i < 15; i++) {
        await backend.writePage("/orphan", i, filledPage(0x02));
      }

      for (let i = 0; i < 10; i++) {
        await backend.writePage("/also-keep", i, filledPage(0x03));
      }
      await backend.writeMeta("/also-keep", { ...meta, size: PAGE_SIZE * 10 });

      const removed = await backend.cleanupOrphanedPages();

      expect(removed).toBe(1);
      expect(await backend.countPages("/keep")).toBe(20);
      expect(await backend.countPages("/orphan")).toBe(0);
      expect(await backend.countPages("/also-keep")).toBe(10);
    });
  });
});
