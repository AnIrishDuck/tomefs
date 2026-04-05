/**
 * Shared StorageBackend behavioral contract tests.
 *
 * Runs the same test suite against every backend implementation to ensure
 * they all satisfy the same contract. Motivated by the rename-overwrite
 * orphan page bug that was independently found in three backends (IDB #96,
 * MemoryBackend #117, PreloadBackend #120).
 *
 * Tests the async StorageBackend interface (MemoryBackend, IdbBackend,
 * OpfsBackend) and the sync SyncStorageBackend interface (SyncMemoryBackend)
 * through a unified async adapter.
 *
 * Ethos §5: "we never use mocks — we use fakes that implement the real
 * interface"
 * Ethos §9: systematic testing of interface contracts
 */

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import "fake-indexeddb/auto";
import { MemoryBackend } from "../../src/memory-backend.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { IdbBackend } from "../../src/idb-backend.js";
import { OpfsBackend } from "../../src/opfs-backend.js";
import { PAGE_SIZE } from "../../src/types.js";
import type { StorageBackend } from "../../src/storage-backend.js";
import type { SyncStorageBackend } from "../../src/sync-storage-backend.js";
import { createFakeOpfsRoot } from "../harness/fake-opfs.js";

/** Create a page filled with a repeating byte value. */
function filledPage(value: number): Uint8Array {
  const page = new Uint8Array(PAGE_SIZE);
  page.fill(value);
  return page;
}

const meta = { size: 8192, mode: 0o100644, ctime: 1000, mtime: 2000 };

/**
 * Wrap a SyncStorageBackend as an async StorageBackend so both can be
 * tested through the same contract suite.
 */
function syncToAsync(sync: SyncStorageBackend): StorageBackend {
  return {
    readPage: async (p, i) => sync.readPage(p, i),
    readPages: async (p, is) => sync.readPages(p, is),
    writePage: async (p, i, d) => sync.writePage(p, i, d),
    writePages: async (ps) => sync.writePages(ps),
    deleteFile: async (p) => sync.deleteFile(p),
    deleteFiles: async (ps) => sync.deleteFiles(ps),
    deletePagesFrom: async (p, i) => sync.deletePagesFrom(p, i),
    renameFile: async (o, n) => sync.renameFile(o, n),
    readMeta: async (p) => sync.readMeta(p),
    readMetas: async (ps) => sync.readMetas(ps),
    writeMeta: async (p, m) => sync.writeMeta(p, m),
    writeMetas: async (e) => sync.writeMetas(e),
    deleteMeta: async (p) => sync.deleteMeta(p),
    deleteMetas: async (ps) => sync.deleteMetas(ps),
    listFiles: async () => sync.listFiles(),
    countPages: async (p) => sync.countPages(p),
    countPagesBatch: async (ps) => sync.countPagesBatch(ps),
    maxPageIndex: async (p) => sync.maxPageIndex(p),
    maxPageIndexBatch: async (ps) => sync.maxPageIndexBatch(ps),
    syncAll: async (pages, metas) => sync.syncAll(pages, metas),
  };
}

// -------------------------------------------------------------------
// Backend factories
// -------------------------------------------------------------------

interface BackendFactory {
  name: string;
  create: () => StorageBackend;
  destroy?: () => Promise<void>;
}

let idbCounter = 0;

const factories: BackendFactory[] = [
  {
    name: "MemoryBackend",
    create: () => new MemoryBackend(),
  },
  {
    name: "SyncMemoryBackend",
    create: () => syncToAsync(new SyncMemoryBackend()),
  },
  {
    name: "IdbBackend",
    create: () => {
      const b = new IdbBackend({ dbName: `contract-test-${idbCounter++}` });
      factories[2].destroy = () => b.destroy();
      return b;
    },
  },
  {
    name: "OpfsBackend",
    create: () => {
      const root = createFakeOpfsRoot();
      const b = new OpfsBackend({ root: root as any });
      factories[3].destroy = () => b.destroy();
      return b;
    },
  },
];

// -------------------------------------------------------------------
// Contract test suite — runs against each backend
// -------------------------------------------------------------------

for (const factory of factories) {
  describe(`StorageBackend contract: ${factory.name}`, () => {
    let backend: StorageBackend;

    beforeEach(() => {
      backend = factory.create();
    });

    afterEach(async () => {
      if (factory.destroy) await factory.destroy();
    });

    // ---------------------------------------------------------------
    // renameFile: destination cleanup (the bug found 3 times)
    // ---------------------------------------------------------------

    describe("renameFile destination cleanup @fast", () => {
      it("cleans up extra destination pages when source has fewer pages", async () => {
        // Destination has 4 pages, source has 2 — orphan pages must not survive.
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

      it("overwrites destination pages at matching indices", async () => {
        await backend.writePage("/dest", 0, filledPage(0xdd));
        await backend.writePage("/src", 0, filledPage(0xaa));

        await backend.renameFile("/src", "/dest");

        expect(await backend.readPage("/dest", 0)).toEqual(filledPage(0xaa));
      });
    });

    // ---------------------------------------------------------------
    // renameFile: basic behavior
    // ---------------------------------------------------------------

    describe("renameFile self-rename @fast", () => {
      it("self-rename preserves all pages", async () => {
        await backend.writePage("/f", 0, filledPage(0xaa));
        await backend.writePage("/f", 1, filledPage(0xbb));

        await backend.renameFile("/f", "/f");

        expect(await backend.readPage("/f", 0)).toEqual(filledPage(0xaa));
        expect(await backend.readPage("/f", 1)).toEqual(filledPage(0xbb));
        expect(await backend.countPages("/f")).toBe(2);
      });

      it("self-rename preserves sparse pages", async () => {
        await backend.writePage("/f", 0, filledPage(0x01));
        await backend.writePage("/f", 5, filledPage(0x05));
        await backend.writePage("/f", 10, filledPage(0x0a));

        await backend.renameFile("/f", "/f");

        expect(await backend.readPage("/f", 0)).toEqual(filledPage(0x01));
        expect(await backend.readPage("/f", 5)).toEqual(filledPage(0x05));
        expect(await backend.readPage("/f", 10)).toEqual(filledPage(0x0a));
        expect(await backend.countPages("/f")).toBe(3);
        expect(await backend.maxPageIndex("/f")).toBe(10);
      });

      it("self-rename of non-existent file is a no-op", async () => {
        await backend.renameFile("/missing", "/missing");
        expect(await backend.readPage("/missing", 0)).toBeNull();
      });
    });

    describe("renameFile basics", () => {
      it("moves all pages from old to new path", async () => {
        await backend.writePage("/old", 0, filledPage(0x01));
        await backend.writePage("/old", 1, filledPage(0x02));

        await backend.renameFile("/old", "/new");

        expect(await backend.readPage("/old", 0)).toBeNull();
        expect(await backend.readPage("/old", 1)).toBeNull();
        expect(await backend.readPage("/new", 0)).toEqual(filledPage(0x01));
        expect(await backend.readPage("/new", 1)).toEqual(filledPage(0x02));
      });

      it("renaming non-existent file is a no-op", async () => {
        await backend.renameFile("/nonexistent", "/dest");
        expect(await backend.readPage("/dest", 0)).toBeNull();
      });

      it("does not affect other files", async () => {
        await backend.writePage("/a", 0, filledPage(0x01));
        await backend.writePage("/b", 0, filledPage(0x02));

        await backend.renameFile("/a", "/c");

        expect(await backend.readPage("/b", 0)).toEqual(filledPage(0x02));
      });

      it("does not affect files with a prefix match", async () => {
        await backend.writePage("/file1", 0, filledPage(0x01));
        await backend.writePage("/file10", 0, filledPage(0x10));
        await backend.writePage("/file10", 1, filledPage(0x11));

        await backend.renameFile("/file1", "/renamed");

        expect(await backend.readPage("/renamed", 0)).toEqual(filledPage(0x01));
        expect(await backend.readPage("/file1", 0)).toBeNull();
        expect(await backend.readPage("/file10", 0)).toEqual(filledPage(0x10));
        expect(await backend.readPage("/file10", 1)).toEqual(filledPage(0x11));
      });

      it("preserves non-contiguous (sparse) pages during rename", async () => {
        await backend.writePage("/f", 0, filledPage(0x01));
        await backend.writePage("/f", 3, filledPage(0x03));
        await backend.writePage("/f", 7, filledPage(0x07));

        await backend.renameFile("/f", "/g");

        expect(await backend.readPage("/g", 0)).toEqual(filledPage(0x01));
        expect(await backend.readPage("/g", 1)).toBeNull();
        expect(await backend.readPage("/g", 2)).toBeNull();
        expect(await backend.readPage("/g", 3)).toEqual(filledPage(0x03));
        expect(await backend.readPage("/g", 7)).toEqual(filledPage(0x07));
        expect(await backend.countPages("/g")).toBe(3);
        expect(await backend.maxPageIndex("/g")).toBe(7);
      });
    });

    // ---------------------------------------------------------------
    // deletePagesFrom with sparse pages
    // ---------------------------------------------------------------

    describe("deletePagesFrom with sparse pages", () => {
      it("deletes only sparse pages at or beyond the threshold", async () => {
        await backend.writePage("/f", 0, filledPage(0x01));
        await backend.writePage("/f", 3, filledPage(0x03));
        await backend.writePage("/f", 7, filledPage(0x07));
        await backend.writePage("/f", 10, filledPage(0x0a));

        await backend.deletePagesFrom("/f", 5);

        expect(await backend.readPage("/f", 0)).toEqual(filledPage(0x01));
        expect(await backend.readPage("/f", 3)).toEqual(filledPage(0x03));
        expect(await backend.readPage("/f", 7)).toBeNull();
        expect(await backend.readPage("/f", 10)).toBeNull();
        expect(await backend.countPages("/f")).toBe(2);
        expect(await backend.maxPageIndex("/f")).toBe(3);
      });

      it("threshold between two sparse pages deletes only the later one", async () => {
        await backend.writePage("/f", 2, filledPage(0x02));
        await backend.writePage("/f", 8, filledPage(0x08));

        await backend.deletePagesFrom("/f", 3);

        expect(await backend.readPage("/f", 2)).toEqual(filledPage(0x02));
        expect(await backend.readPage("/f", 8)).toBeNull();
        expect(await backend.countPages("/f")).toBe(1);
      });

      it("threshold at exact sparse page index deletes that page", async () => {
        await backend.writePage("/f", 0, filledPage(0x01));
        await backend.writePage("/f", 5, filledPage(0x05));

        await backend.deletePagesFrom("/f", 5);

        expect(await backend.readPage("/f", 0)).toEqual(filledPage(0x01));
        expect(await backend.readPage("/f", 5)).toBeNull();
        expect(await backend.countPages("/f")).toBe(1);
      });
    });

    // ---------------------------------------------------------------
    // Page operations
    // ---------------------------------------------------------------

    describe("page operations", () => {
      it("readPage returns null for non-existent page @fast", async () => {
        expect(await backend.readPage("/missing", 0)).toBeNull();
      });

      it("writePage then readPage round-trips data @fast", async () => {
        const data = filledPage(0xab);
        await backend.writePage("/f", 0, data);
        expect(await backend.readPage("/f", 0)).toEqual(data);
      });

      it("writePage stores a copy, not a reference", async () => {
        const data = filledPage(0x01);
        await backend.writePage("/f", 0, data);
        data.fill(0xff);
        expect((await backend.readPage("/f", 0))![0]).toBe(0x01);
      });

      it("readPage returns a copy, not a reference", async () => {
        await backend.writePage("/f", 0, filledPage(0x42));
        const a = await backend.readPage("/f", 0);
        a![0] = 0xff;
        expect((await backend.readPage("/f", 0))![0]).toBe(0x42);
      });

      it("handles multiple pages per file", async () => {
        await backend.writePage("/f", 0, filledPage(0x01));
        await backend.writePage("/f", 1, filledPage(0x02));
        expect(await backend.readPage("/f", 0)).toEqual(filledPage(0x01));
        expect(await backend.readPage("/f", 1)).toEqual(filledPage(0x02));
      });

      it("handles pages across different files independently", async () => {
        await backend.writePage("/a", 0, filledPage(0xaa));
        await backend.writePage("/b", 0, filledPage(0xbb));
        expect(await backend.readPage("/a", 0)).toEqual(filledPage(0xaa));
        expect(await backend.readPage("/b", 0)).toEqual(filledPage(0xbb));
      });

      it("overwriting a page replaces its data", async () => {
        await backend.writePage("/f", 0, filledPage(0x01));
        await backend.writePage("/f", 0, filledPage(0x02));
        expect(await backend.readPage("/f", 0)).toEqual(filledPage(0x02));
      });
    });

    // ---------------------------------------------------------------
    // Batch page operations
    // ---------------------------------------------------------------

    describe("readPages batch", () => {
      it("reads multiple pages in one call", async () => {
        await backend.writePage("/f", 0, filledPage(0xaa));
        await backend.writePage("/f", 2, filledPage(0xcc));

        const results = await backend.readPages("/f", [0, 1, 2]);
        expect(results[0]).toEqual(filledPage(0xaa));
        expect(results[1]).toBeNull();
        expect(results[2]).toEqual(filledPage(0xcc));
      });

      it("returns empty array for empty indices", async () => {
        expect(await backend.readPages("/f", [])).toEqual([]);
      });

      it("returns all nulls for non-existent file", async () => {
        const results = await backend.readPages("/missing", [0, 1]);
        expect(results).toEqual([null, null]);
      });
    });

    describe("writePages batch", () => {
      it("writes zero pages without error", async () => {
        await backend.writePages([]);
      });

      it("writes multiple pages across files", async () => {
        await backend.writePages([
          { path: "/a", pageIndex: 0, data: filledPage(0x01) },
          { path: "/a", pageIndex: 1, data: filledPage(0x02) },
          { path: "/b", pageIndex: 0, data: filledPage(0x03) },
        ]);

        expect(await backend.readPage("/a", 0)).toEqual(filledPage(0x01));
        expect(await backend.readPage("/a", 1)).toEqual(filledPage(0x02));
        expect(await backend.readPage("/b", 0)).toEqual(filledPage(0x03));
      });
    });

    // ---------------------------------------------------------------
    // Delete operations
    // ---------------------------------------------------------------

    describe("deleteFile", () => {
      it("deletes all pages for a file", async () => {
        await backend.writePage("/f", 0, filledPage(0x01));
        await backend.writePage("/f", 1, filledPage(0x02));

        await backend.deleteFile("/f");

        expect(await backend.readPage("/f", 0)).toBeNull();
        expect(await backend.readPage("/f", 1)).toBeNull();
      });

      it("does not affect other files", async () => {
        await backend.writePage("/a", 0, filledPage(0x01));
        await backend.writePage("/b", 0, filledPage(0x02));

        await backend.deleteFile("/a");
        expect(await backend.readPage("/b", 0)).toEqual(filledPage(0x02));
      });

      it("deleting non-existent file is a no-op", async () => {
        await backend.deleteFile("/nonexistent");
      });

      it("does not delete files with a prefix match", async () => {
        await backend.writePage("/file1", 0, filledPage(0x01));
        await backend.writePage("/file10", 0, filledPage(0x10));

        await backend.deleteFile("/file1");

        expect(await backend.readPage("/file1", 0)).toBeNull();
        expect(await backend.readPage("/file10", 0)).toEqual(filledPage(0x10));
      });
    });

    describe("deletePagesFrom", () => {
      it("deletes pages at and beyond the given index", async () => {
        await backend.writePage("/f", 0, filledPage(0x01));
        await backend.writePage("/f", 1, filledPage(0x02));
        await backend.writePage("/f", 2, filledPage(0x03));

        await backend.deletePagesFrom("/f", 1);

        expect(await backend.readPage("/f", 0)).toEqual(filledPage(0x01));
        expect(await backend.readPage("/f", 1)).toBeNull();
        expect(await backend.readPage("/f", 2)).toBeNull();
      });

      it("deletePagesFrom(0) deletes all pages", async () => {
        await backend.writePage("/f", 0, filledPage(0x01));
        await backend.writePage("/f", 1, filledPage(0x02));

        await backend.deletePagesFrom("/f", 0);

        expect(await backend.readPage("/f", 0)).toBeNull();
        expect(await backend.readPage("/f", 1)).toBeNull();
      });

      it("does not affect other files", async () => {
        await backend.writePage("/a", 0, filledPage(0x01));
        await backend.writePage("/b", 0, filledPage(0xbb));

        await backend.deletePagesFrom("/a", 0);
        expect(await backend.readPage("/b", 0)).toEqual(filledPage(0xbb));
      });

      it("does not affect files with a prefix match", async () => {
        await backend.writePage("/file1", 0, filledPage(0x01));
        await backend.writePage("/file1", 1, filledPage(0x02));
        await backend.writePage("/file10", 0, filledPage(0x10));
        await backend.writePage("/file10", 1, filledPage(0x11));

        await backend.deletePagesFrom("/file1", 1);

        expect(await backend.readPage("/file1", 0)).toEqual(filledPage(0x01));
        expect(await backend.readPage("/file1", 1)).toBeNull();
        expect(await backend.readPage("/file10", 0)).toEqual(filledPage(0x10));
        expect(await backend.readPage("/file10", 1)).toEqual(filledPage(0x11));
      });
    });

    // ---------------------------------------------------------------
    // deleteFiles batch
    // ---------------------------------------------------------------

    describe("deleteFiles batch", () => {
      it("deletes all pages for multiple files @fast", async () => {
        await backend.writePage("/a", 0, filledPage(0x01));
        await backend.writePage("/a", 1, filledPage(0x02));
        await backend.writePage("/b", 0, filledPage(0x03));

        await backend.deleteFiles(["/a", "/b"]);

        expect(await backend.readPage("/a", 0)).toBeNull();
        expect(await backend.readPage("/a", 1)).toBeNull();
        expect(await backend.readPage("/b", 0)).toBeNull();
      });

      it("empty array is a no-op", async () => {
        await backend.writePage("/f", 0, filledPage(0x01));
        await backend.deleteFiles([]);
        expect(await backend.readPage("/f", 0)).toEqual(filledPage(0x01));
      });

      it("non-existent files are a no-op", async () => {
        await backend.deleteFiles(["/nonexistent1", "/nonexistent2"]);
      });

      it("does not affect other files", async () => {
        await backend.writePage("/a", 0, filledPage(0xaa));
        await backend.writePage("/b", 0, filledPage(0xbb));
        await backend.writePage("/c", 0, filledPage(0xcc));

        await backend.deleteFiles(["/a", "/b"]);

        expect(await backend.readPage("/c", 0)).toEqual(filledPage(0xcc));
      });

      it("does not delete files with a prefix match", async () => {
        await backend.writePage("/file1", 0, filledPage(0x01));
        await backend.writePage("/file10", 0, filledPage(0x10));
        await backend.writePage("/file100", 0, filledPage(0xff));

        await backend.deleteFiles(["/file1"]);

        expect(await backend.readPage("/file1", 0)).toBeNull();
        expect(await backend.readPage("/file10", 0)).toEqual(filledPage(0x10));
        expect(await backend.readPage("/file100", 0)).toEqual(filledPage(0xff));
      });

      it("countPages returns 0 after batch delete", async () => {
        await backend.writePage("/a", 0, filledPage(0x01));
        await backend.writePage("/a", 1, filledPage(0x02));
        await backend.writePage("/b", 0, filledPage(0x03));

        await backend.deleteFiles(["/a", "/b"]);

        expect(await backend.countPages("/a")).toBe(0);
        expect(await backend.countPages("/b")).toBe(0);
      });

      it("handles duplicate paths in batch", async () => {
        await backend.writePage("/f", 0, filledPage(0x42));

        await backend.deleteFiles(["/f", "/f"]);

        expect(await backend.readPage("/f", 0)).toBeNull();
      });

      it("mixed existing and non-existent files", async () => {
        await backend.writePage("/exists", 0, filledPage(0xaa));

        await backend.deleteFiles(["/exists", "/missing1", "/missing2"]);

        expect(await backend.readPage("/exists", 0)).toBeNull();
      });
    });

    // ---------------------------------------------------------------
    // Metadata operations
    // ---------------------------------------------------------------

    describe("metadata", () => {
      it("readMeta returns null for non-existent file @fast", async () => {
        expect(await backend.readMeta("/missing")).toBeNull();
      });

      it("writeMeta then readMeta round-trips @fast", async () => {
        await backend.writeMeta("/f", meta);
        expect(await backend.readMeta("/f")).toEqual(meta);
      });

      it("overwrites existing metadata", async () => {
        await backend.writeMeta("/f", meta);
        const updated = { ...meta, size: 16384, mtime: 3000 };
        await backend.writeMeta("/f", updated);
        expect(await backend.readMeta("/f")).toEqual(updated);
      });

      it("deleteMeta removes metadata", async () => {
        await backend.writeMeta("/f", meta);
        await backend.deleteMeta("/f");
        expect(await backend.readMeta("/f")).toBeNull();
      });

      it("deleteMeta on non-existent file is a no-op", async () => {
        await backend.deleteMeta("/nonexistent");
      });

      it("metadata and pages are independent", async () => {
        await backend.writeMeta("/f", meta);
        await backend.writePage("/f", 0, filledPage(0x42));

        await backend.deleteMeta("/f");
        expect(await backend.readPage("/f", 0)).toEqual(filledPage(0x42));
      });

      it("preserves optional atime field", async () => {
        const withAtime = { ...meta, atime: 500 };
        await backend.writeMeta("/f", withAtime);
        const result = await backend.readMeta("/f");
        expect(result!.atime).toBe(500);
      });

      it("preserves optional link field for symlinks", async () => {
        const symMeta = { ...meta, mode: 0o120777, link: "/target/path" };
        await backend.writeMeta("/link", symMeta);
        const result = await backend.readMeta("/link");
        expect(result!.link).toBe("/target/path");
      });
    });

    // ---------------------------------------------------------------
    // Batch metadata operations
    // ---------------------------------------------------------------

    describe("writeMetas batch", () => {
      it("writes multiple entries", async () => {
        const m1 = { ...meta, size: 100 };
        const m2 = { ...meta, size: 200 };

        await backend.writeMetas([
          { path: "/a", meta: m1 },
          { path: "/b", meta: m2 },
        ]);

        expect(await backend.readMeta("/a")).toEqual(m1);
        expect(await backend.readMeta("/b")).toEqual(m2);
      });

      it("empty array is a no-op", async () => {
        await backend.writeMetas([]);
      });

      it("overwrites existing metadata", async () => {
        await backend.writeMeta("/f", meta);
        const updated = { ...meta, size: 99999 };
        await backend.writeMetas([{ path: "/f", meta: updated }]);
        expect(await backend.readMeta("/f")).toEqual(updated);
      });
    });

    describe("deleteMetas batch", () => {
      it("removes multiple entries", async () => {
        await backend.writeMeta("/a", meta);
        await backend.writeMeta("/b", meta);
        await backend.writeMeta("/c", meta);

        await backend.deleteMetas(["/a", "/b"]);

        expect(await backend.readMeta("/a")).toBeNull();
        expect(await backend.readMeta("/b")).toBeNull();
        expect(await backend.readMeta("/c")).toEqual(meta);
      });

      it("empty array is a no-op", async () => {
        await backend.deleteMetas([]);
      });

      it("non-existent paths are a no-op", async () => {
        await backend.deleteMetas(["/nonexistent1", "/nonexistent2"]);
      });
    });

    // ---------------------------------------------------------------
    // listFiles
    // ---------------------------------------------------------------

    describe("listFiles", () => {
      it("returns empty array initially @fast", async () => {
        expect(await backend.listFiles()).toEqual([]);
      });

      it("returns paths with metadata", async () => {
        await backend.writeMeta("/a", meta);
        await backend.writeMeta("/b", meta);

        const files = await backend.listFiles();
        expect(files.sort()).toEqual(["/a", "/b"]);
      });

      it("does not include files that only have pages", async () => {
        await backend.writePage("/orphan", 0, filledPage(0x01));
        expect(await backend.listFiles()).toEqual([]);
      });

      it("reflects deletions", async () => {
        await backend.writeMeta("/a", meta);
        await backend.writeMeta("/b", meta);

        await backend.deleteMeta("/a");

        const files = await backend.listFiles();
        expect(files).toEqual(["/b"]);
      });
    });

    // ---------------------------------------------------------------
    // Cross-operation consistency
    // ---------------------------------------------------------------

    describe("cross-operation consistency", () => {
      it("rename + delete cycle leaves no traces", async () => {
        await backend.writePage("/f", 0, filledPage(0x01));
        await backend.writeMeta("/f", meta);

        await backend.renameFile("/f", "/g");
        // Metadata is NOT moved by renameFile — only pages
        await backend.writeMeta("/g", meta);
        await backend.deleteMeta("/f");

        await backend.deleteFile("/g");
        await backend.deleteMeta("/g");

        expect(await backend.readPage("/f", 0)).toBeNull();
        expect(await backend.readPage("/g", 0)).toBeNull();
        expect(await backend.readMeta("/f")).toBeNull();
        expect(await backend.readMeta("/g")).toBeNull();
        expect(await backend.listFiles()).toEqual([]);
      });

      it("sequential renames preserve data", async () => {
        await backend.writePage("/a", 0, filledPage(0xaa));
        await backend.renameFile("/a", "/b");
        await backend.renameFile("/b", "/c");

        expect(await backend.readPage("/a", 0)).toBeNull();
        expect(await backend.readPage("/b", 0)).toBeNull();
        expect(await backend.readPage("/c", 0)).toEqual(filledPage(0xaa));
      });

      it("deletePagesFrom + writePage re-creates truncated pages", async () => {
        await backend.writePage("/f", 0, filledPage(0x01));
        await backend.writePage("/f", 1, filledPage(0x02));

        await backend.deletePagesFrom("/f", 1);
        await backend.writePage("/f", 1, filledPage(0x03));

        expect(await backend.readPage("/f", 0)).toEqual(filledPage(0x01));
        expect(await backend.readPage("/f", 1)).toEqual(filledPage(0x03));
      });
    });

    // ---------------------------------------------------------------
    // countPages
    // ---------------------------------------------------------------

    describe("countPages", () => {
      it("returns 0 for non-existent file @fast", async () => {
        expect(await backend.countPages("/missing")).toBe(0);
      });

      it("returns correct count after writes @fast", async () => {
        await backend.writePage("/f", 0, filledPage(0x01));
        await backend.writePage("/f", 1, filledPage(0x02));
        await backend.writePage("/f", 2, filledPage(0x03));

        expect(await backend.countPages("/f")).toBe(3);
      });

      it("returns correct count for sparse pages", async () => {
        await backend.writePage("/f", 0, filledPage(0x01));
        await backend.writePage("/f", 5, filledPage(0x05));

        expect(await backend.countPages("/f")).toBe(2);
      });

      it("reflects deletePagesFrom", async () => {
        await backend.writePage("/f", 0, filledPage(0x01));
        await backend.writePage("/f", 1, filledPage(0x02));
        await backend.writePage("/f", 2, filledPage(0x03));

        await backend.deletePagesFrom("/f", 1);

        expect(await backend.countPages("/f")).toBe(1);
      });

      it("returns 0 after deleteFile", async () => {
        await backend.writePage("/f", 0, filledPage(0x01));
        await backend.writePage("/f", 1, filledPage(0x02));

        await backend.deleteFile("/f");

        expect(await backend.countPages("/f")).toBe(0);
      });

      it("counts pages independently per file", async () => {
        await backend.writePage("/a", 0, filledPage(0x01));
        await backend.writePage("/b", 0, filledPage(0x02));
        await backend.writePage("/b", 1, filledPage(0x03));

        expect(await backend.countPages("/a")).toBe(1);
        expect(await backend.countPages("/b")).toBe(2);
      });

      it("reflects renameFile (source becomes 0, dest gets count)", async () => {
        await backend.writePage("/src", 0, filledPage(0x01));
        await backend.writePage("/src", 1, filledPage(0x02));

        await backend.renameFile("/src", "/dest");

        expect(await backend.countPages("/src")).toBe(0);
        expect(await backend.countPages("/dest")).toBe(2);
      });

      it("does not count pages from prefix-matching files", async () => {
        await backend.writePage("/file1", 0, filledPage(0x01));
        await backend.writePage("/file10", 0, filledPage(0x02));
        await backend.writePage("/file10", 1, filledPage(0x03));

        expect(await backend.countPages("/file1")).toBe(1);
        expect(await backend.countPages("/file10")).toBe(2);
      });
    });

    // ---------------------------------------------------------------
    // countPagesBatch
    // ---------------------------------------------------------------

    describe("countPagesBatch", () => {
      it("returns empty array for empty input @fast", async () => {
        expect(await backend.countPagesBatch([])).toEqual([]);
      });

      it("returns counts parallel to input paths @fast", async () => {
        await backend.writePage("/a", 0, filledPage(0x01));
        await backend.writePage("/b", 0, filledPage(0x02));
        await backend.writePage("/b", 1, filledPage(0x03));
        await backend.writePage("/c", 0, filledPage(0x04));
        await backend.writePage("/c", 1, filledPage(0x05));
        await backend.writePage("/c", 2, filledPage(0x06));

        const counts = await backend.countPagesBatch(["/a", "/b", "/c"]);
        expect(counts).toEqual([1, 2, 3]);
      });

      it("returns 0 for non-existent files in batch", async () => {
        await backend.writePage("/exists", 0, filledPage(0x01));

        const counts = await backend.countPagesBatch([
          "/missing1",
          "/exists",
          "/missing2",
        ]);
        expect(counts).toEqual([0, 1, 0]);
      });

      it("handles single-element batch", async () => {
        await backend.writePage("/f", 0, filledPage(0x01));
        await backend.writePage("/f", 1, filledPage(0x02));

        const counts = await backend.countPagesBatch(["/f"]);
        expect(counts).toEqual([2]);
      });

      it("handles sparse pages in batch", async () => {
        await backend.writePage("/sparse", 0, filledPage(0x01));
        await backend.writePage("/sparse", 5, filledPage(0x02));

        const counts = await backend.countPagesBatch(["/sparse"]);
        expect(counts).toEqual([2]);
      });
    });

    // ---------------------------------------------------------------
    // maxPageIndex
    // ---------------------------------------------------------------

    describe("maxPageIndex", () => {
      it("returns -1 for non-existent file @fast", async () => {
        expect(await backend.maxPageIndex("/missing")).toBe(-1);
      });

      it("returns highest index for contiguous pages @fast", async () => {
        await backend.writePage("/f", 0, filledPage(0x01));
        await backend.writePage("/f", 1, filledPage(0x02));
        await backend.writePage("/f", 2, filledPage(0x03));

        expect(await backend.maxPageIndex("/f")).toBe(2);
      });

      it("returns highest index for sparse (non-contiguous) pages", async () => {
        await backend.writePage("/f", 0, filledPage(0x01));
        await backend.writePage("/f", 5, filledPage(0x05));

        expect(await backend.maxPageIndex("/f")).toBe(5);
      });

      it("reflects deletePagesFrom", async () => {
        await backend.writePage("/f", 0, filledPage(0x01));
        await backend.writePage("/f", 3, filledPage(0x03));
        await backend.writePage("/f", 7, filledPage(0x07));

        await backend.deletePagesFrom("/f", 4);

        expect(await backend.maxPageIndex("/f")).toBe(3);
      });

      it("returns -1 after deleteFile", async () => {
        await backend.writePage("/f", 0, filledPage(0x01));
        await backend.writePage("/f", 5, filledPage(0x05));

        await backend.deleteFile("/f");

        expect(await backend.maxPageIndex("/f")).toBe(-1);
      });

      it("tracks files independently", async () => {
        await backend.writePage("/a", 0, filledPage(0x01));
        await backend.writePage("/a", 10, filledPage(0x0a));
        await backend.writePage("/b", 0, filledPage(0x01));
        await backend.writePage("/b", 3, filledPage(0x03));

        expect(await backend.maxPageIndex("/a")).toBe(10);
        expect(await backend.maxPageIndex("/b")).toBe(3);
      });

      it("reflects renameFile", async () => {
        await backend.writePage("/src", 0, filledPage(0x01));
        await backend.writePage("/src", 8, filledPage(0x08));

        await backend.renameFile("/src", "/dest");

        expect(await backend.maxPageIndex("/src")).toBe(-1);
        expect(await backend.maxPageIndex("/dest")).toBe(8);
      });

      it("returns 0 for a single page at index 0", async () => {
        await backend.writePage("/f", 0, filledPage(0x01));
        expect(await backend.maxPageIndex("/f")).toBe(0);
      });
    });

    // ---------------------------------------------------------------
    // maxPageIndexBatch
    // ---------------------------------------------------------------

    describe("maxPageIndexBatch", () => {
      it("returns empty array for empty input @fast", async () => {
        expect(await backend.maxPageIndexBatch([])).toEqual([]);
      });

      it("returns max indices parallel to input paths @fast", async () => {
        await backend.writePage("/a", 0, filledPage(0x01));
        await backend.writePage("/b", 0, filledPage(0x02));
        await backend.writePage("/b", 3, filledPage(0x03));
        await backend.writePage("/c", 0, filledPage(0x04));
        await backend.writePage("/c", 5, filledPage(0x05));

        const indices = await backend.maxPageIndexBatch(["/a", "/b", "/c"]);
        expect(indices).toEqual([0, 3, 5]);
      });

      it("returns -1 for non-existent files in batch @fast", async () => {
        await backend.writePage("/exists", 0, filledPage(0x01));

        const indices = await backend.maxPageIndexBatch([
          "/missing1",
          "/exists",
          "/missing2",
        ]);
        expect(indices).toEqual([-1, 0, -1]);
      });

      it("handles single-element batch", async () => {
        await backend.writePage("/f", 0, filledPage(0x01));
        await backend.writePage("/f", 7, filledPage(0x02));

        const indices = await backend.maxPageIndexBatch(["/f"]);
        expect(indices).toEqual([7]);
      });

      it("handles sparse pages in batch", async () => {
        await backend.writePage("/sparse", 0, filledPage(0x01));
        await backend.writePage("/sparse", 10, filledPage(0x02));

        const indices = await backend.maxPageIndexBatch(["/sparse"]);
        expect(indices).toEqual([10]);
      });
    });

    // ---------------------------------------------------------------
    // readMetas batch
    // ---------------------------------------------------------------

    describe("readMetas batch", () => {
      it("returns empty array for empty input @fast", async () => {
        expect(await backend.readMetas([])).toEqual([]);
      });

      it("reads multiple entries in parallel order @fast", async () => {
        const m1 = { ...meta, size: 100 };
        const m2 = { ...meta, size: 200 };
        await backend.writeMeta("/a", m1);
        await backend.writeMeta("/b", m2);

        const results = await backend.readMetas(["/a", "/b"]);
        expect(results).toEqual([m1, m2]);
      });

      it("returns null for non-existent paths", async () => {
        await backend.writeMeta("/exists", meta);

        const results = await backend.readMetas(["/exists", "/missing", "/also-missing"]);
        expect(results[0]).toEqual(meta);
        expect(results[1]).toBeNull();
        expect(results[2]).toBeNull();
      });

      it("returns all nulls when no files exist", async () => {
        const results = await backend.readMetas(["/a", "/b"]);
        expect(results).toEqual([null, null]);
      });

      it("returns results in same order as input paths", async () => {
        const m1 = { ...meta, size: 111 };
        const m2 = { ...meta, size: 222 };
        const m3 = { ...meta, size: 333 };
        await backend.writeMeta("/x", m1);
        await backend.writeMeta("/y", m2);
        await backend.writeMeta("/z", m3);

        // Read in reverse order
        const results = await backend.readMetas(["/z", "/x", "/y"]);
        expect(results).toEqual([m3, m1, m2]);
      });

      it("reflects deleteMeta changes", async () => {
        await backend.writeMeta("/a", meta);
        await backend.writeMeta("/b", meta);
        await backend.deleteMeta("/a");

        const results = await backend.readMetas(["/a", "/b"]);
        expect(results[0]).toBeNull();
        expect(results[1]).toEqual(meta);
      });

      it("handles duplicate paths in input", async () => {
        await backend.writeMeta("/f", meta);

        const results = await backend.readMetas(["/f", "/f"]);
        expect(results).toEqual([meta, meta]);
      });
    });
  });
}
