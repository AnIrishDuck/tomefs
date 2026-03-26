/**
 * Unit tests for PreloadBackend.
 *
 * Validates the graceful degradation path: wrapping an async StorageBackend
 * to provide synchronous access after preloading, with dirty tracking and
 * async flush.
 */
import { describe, it, expect, beforeEach } from "vitest";
import { MemoryBackend } from "../../src/memory-backend.js";
import { PreloadBackend } from "../../src/preload-backend.js";
import { PAGE_SIZE } from "../../src/types.js";

describe("PreloadBackend", () => {
  let remote: MemoryBackend;

  beforeEach(() => {
    remote = new MemoryBackend();
  });

  describe("init", () => {
    it("@fast loads metadata and pages from remote", async () => {
      // Seed the remote with data
      const data = new Uint8Array(PAGE_SIZE);
      data[0] = 0xab;
      await remote.writePage("/file", 0, data);
      await remote.writeMeta("/file", {
        size: PAGE_SIZE,
        mode: 0o100644,
        ctime: 1000,
        mtime: 1000,
      });

      const backend = new PreloadBackend(remote);
      await backend.init();

      const read = backend.readPage("/file", 0);
      expect(read).not.toBeNull();
      expect(read![0]).toBe(0xab);

      const meta = backend.readMeta("/file");
      expect(meta).not.toBeNull();
      expect(meta!.size).toBe(PAGE_SIZE);
    });

    it("loads multi-page files", async () => {
      const page0 = new Uint8Array(PAGE_SIZE);
      page0[0] = 1;
      const page1 = new Uint8Array(PAGE_SIZE);
      page1[0] = 2;
      await remote.writePage("/big", 0, page0);
      await remote.writePage("/big", 1, page1);
      await remote.writeMeta("/big", {
        size: PAGE_SIZE * 2,
        mode: 0o100644,
        ctime: 1000,
        mtime: 1000,
      });

      const backend = new PreloadBackend(remote);
      await backend.init();

      expect(backend.readPage("/big", 0)![0]).toBe(1);
      expect(backend.readPage("/big", 1)![0]).toBe(2);
    });

    it("handles empty remote", async () => {
      const backend = new PreloadBackend(remote);
      await backend.init();

      expect(backend.listFiles()).toEqual([]);
      expect(backend.readPage("/nope", 0)).toBeNull();
    });

    it("throws if used before init", () => {
      const backend = new PreloadBackend(remote);
      expect(() => backend.readPage("/x", 0)).toThrow("init()");
    });
  });

  describe("sync operations after init", () => {
    let backend: PreloadBackend;

    beforeEach(async () => {
      backend = new PreloadBackend(remote);
      await backend.init();
    });

    it("@fast readPage returns null for non-existent page", () => {
      expect(backend.readPage("/nope", 0)).toBeNull();
    });

    it("@fast writePage and readPage round-trip", () => {
      const data = new Uint8Array(PAGE_SIZE);
      data[0] = 0xde;
      data[PAGE_SIZE - 1] = 0xff;

      backend.writePage("/test", 0, data);
      const read = backend.readPage("/test", 0);

      expect(read![0]).toBe(0xde);
      expect(read![PAGE_SIZE - 1]).toBe(0xff);
    });

    it("readPage returns a copy", () => {
      const data = new Uint8Array(PAGE_SIZE);
      data[0] = 42;
      backend.writePage("/test", 0, data);

      const read1 = backend.readPage("/test", 0);
      read1![0] = 99;

      const read2 = backend.readPage("/test", 0);
      expect(read2![0]).toBe(42);
    });

    it("writePages writes multiple pages", () => {
      const d1 = new Uint8Array(PAGE_SIZE);
      d1[0] = 1;
      const d2 = new Uint8Array(PAGE_SIZE);
      d2[0] = 2;

      backend.writePages([
        { path: "/a", pageIndex: 0, data: d1 },
        { path: "/b", pageIndex: 0, data: d2 },
      ]);

      expect(backend.readPage("/a", 0)![0]).toBe(1);
      expect(backend.readPage("/b", 0)![0]).toBe(2);
    });

    it("deleteFile removes all pages for a file", () => {
      const data = new Uint8Array(PAGE_SIZE);
      backend.writePage("/f", 0, data);
      backend.writePage("/f", 1, data);
      backend.writePage("/other", 0, data);

      backend.deleteFile("/f");

      expect(backend.readPage("/f", 0)).toBeNull();
      expect(backend.readPage("/f", 1)).toBeNull();
      expect(backend.readPage("/other", 0)).not.toBeNull();
    });

    it("deletePagesFrom removes pages at and beyond index", () => {
      const data = new Uint8Array(PAGE_SIZE);
      backend.writePage("/f", 0, data);
      backend.writePage("/f", 1, data);
      backend.writePage("/f", 2, data);

      backend.deletePagesFrom("/f", 1);

      expect(backend.readPage("/f", 0)).not.toBeNull();
      expect(backend.readPage("/f", 1)).toBeNull();
      expect(backend.readPage("/f", 2)).toBeNull();
    });

    it("metadata CRUD", () => {
      expect(backend.readMeta("/f")).toBeNull();

      backend.writeMeta("/f", {
        size: 100,
        mode: 0o100644,
        ctime: 1000,
        mtime: 2000,
      });

      const meta = backend.readMeta("/f");
      expect(meta!.size).toBe(100);
      expect(meta!.mtime).toBe(2000);

      expect(backend.listFiles()).toContain("/f");

      backend.deleteMeta("/f");
      expect(backend.readMeta("/f")).toBeNull();
      expect(backend.listFiles()).not.toContain("/f");
    });

    it("readMeta returns a copy", () => {
      backend.writeMeta("/f", {
        size: 100,
        mode: 0o100644,
        ctime: 1000,
        mtime: 2000,
      });

      const m1 = backend.readMeta("/f")!;
      m1.size = 999;

      const m2 = backend.readMeta("/f")!;
      expect(m2.size).toBe(100);
    });
  });

  describe("dirty tracking", () => {
    let backend: PreloadBackend;

    beforeEach(async () => {
      backend = new PreloadBackend(remote);
      await backend.init();
    });

    it("@fast starts clean", () => {
      expect(backend.isDirty).toBe(false);
      expect(backend.dirtyPageCount).toBe(0);
      expect(backend.dirtyMetaCount).toBe(0);
    });

    it("writePage marks dirty", () => {
      backend.writePage("/f", 0, new Uint8Array(PAGE_SIZE));
      expect(backend.isDirty).toBe(true);
      expect(backend.dirtyPageCount).toBe(1);
    });

    it("writeMeta marks dirty", () => {
      backend.writeMeta("/f", {
        size: 0,
        mode: 0o100644,
        ctime: 1000,
        mtime: 1000,
      });
      expect(backend.isDirty).toBe(true);
      expect(backend.dirtyMetaCount).toBe(1);
    });

    it("deleteFile marks dirty", () => {
      backend.writePage("/f", 0, new Uint8Array(PAGE_SIZE));
      backend.deleteFile("/f");
      expect(backend.isDirty).toBe(true);
      // dirty pages for that file should be cleared
      expect(backend.dirtyPageCount).toBe(0);
    });

    it("deletePagesFrom marks dirty", () => {
      backend.writePage("/f", 0, new Uint8Array(PAGE_SIZE));
      backend.writePage("/f", 1, new Uint8Array(PAGE_SIZE));
      backend.deletePagesFrom("/f", 1);
      expect(backend.isDirty).toBe(true);
      // page 1 dirty entry cleared, page 0 still dirty
      expect(backend.dirtyPageCount).toBe(1);
    });

    it("deleteMeta marks dirty", () => {
      backend.writeMeta("/f", {
        size: 0,
        mode: 0o100644,
        ctime: 1000,
        mtime: 1000,
      });
      backend.deleteMeta("/f");
      expect(backend.isDirty).toBe(true);
      // writeMeta dirty cleared, deleteMeta tracked separately
      expect(backend.dirtyMetaCount).toBe(0);
    });
  });

  describe("renameFile", () => {
    it("moves pages from old path to new path in memory", async () => {
      await remote.writePage("/old", 0, (() => { const d = new Uint8Array(PAGE_SIZE); d[0] = 0xaa; return d; })());
      await remote.writeMeta("/old", { size: PAGE_SIZE, mode: 0o100644, ctime: 0, mtime: 0 });
      const backend = new PreloadBackend(remote);
      await backend.init();

      backend.renameFile("/old", "/new");

      expect(backend.readPage("/old", 0)).toBeNull();
      expect(backend.readPage("/new", 0)![0]).toBe(0xaa);
    });

    it("flush after renameFile deletes old and writes new to remote", async () => {
      await remote.writePage("/old", 0, (() => { const d = new Uint8Array(PAGE_SIZE); d[0] = 0xbb; return d; })());
      await remote.writeMeta("/old", { size: PAGE_SIZE, mode: 0o100644, ctime: 0, mtime: 0 });
      const backend = new PreloadBackend(remote);
      await backend.init();

      backend.renameFile("/old", "/new");
      await backend.flush();

      expect(await remote.readPage("/old", 0)).toBeNull();
      expect((await remote.readPage("/new", 0))![0]).toBe(0xbb);
    });
  });

  describe("flush", () => {
    it("@fast writes dirty pages to remote", async () => {
      const backend = new PreloadBackend(remote);
      await backend.init();

      const data = new Uint8Array(PAGE_SIZE);
      data[0] = 0xca;
      backend.writePage("/f", 0, data);

      // Not in remote yet
      expect(await remote.readPage("/f", 0)).toBeNull();

      await backend.flush();

      // Now in remote
      const read = await remote.readPage("/f", 0);
      expect(read![0]).toBe(0xca);
      expect(backend.isDirty).toBe(false);
    });

    it("writes dirty metadata to remote", async () => {
      const backend = new PreloadBackend(remote);
      await backend.init();

      backend.writeMeta("/f", {
        size: 100,
        mode: 0o100644,
        ctime: 1000,
        mtime: 2000,
      });

      expect(await remote.readMeta("/f")).toBeNull();

      await backend.flush();

      const meta = await remote.readMeta("/f");
      expect(meta!.size).toBe(100);
    });

    it("applies file deletions to remote", async () => {
      // Seed remote
      await remote.writePage("/f", 0, new Uint8Array(PAGE_SIZE));
      await remote.writeMeta("/f", {
        size: PAGE_SIZE,
        mode: 0o100644,
        ctime: 1000,
        mtime: 1000,
      });

      const backend = new PreloadBackend(remote);
      await backend.init();

      backend.deleteFile("/f");
      await backend.flush();

      expect(await remote.readPage("/f", 0)).toBeNull();
    });

    it("applies truncations to remote", async () => {
      // Seed remote with 3 pages
      for (let i = 0; i < 3; i++) {
        const d = new Uint8Array(PAGE_SIZE);
        d[0] = i;
        await remote.writePage("/f", i, d);
      }
      await remote.writeMeta("/f", {
        size: PAGE_SIZE * 3,
        mode: 0o100644,
        ctime: 1000,
        mtime: 1000,
      });

      const backend = new PreloadBackend(remote);
      await backend.init();

      backend.deletePagesFrom("/f", 1);
      await backend.flush();

      expect(await remote.readPage("/f", 0)).not.toBeNull();
      expect(await remote.readPage("/f", 1)).toBeNull();
      expect(await remote.readPage("/f", 2)).toBeNull();
    });

    it("applies metadata deletions to remote", async () => {
      await remote.writeMeta("/f", {
        size: 0,
        mode: 0o100644,
        ctime: 1000,
        mtime: 1000,
      });

      const backend = new PreloadBackend(remote);
      await backend.init();

      backend.deleteMeta("/f");
      await backend.flush();

      expect(await remote.readMeta("/f")).toBeNull();
    });

    it("no-op when nothing is dirty", async () => {
      const backend = new PreloadBackend(remote);
      await backend.init();

      // Should not throw
      await backend.flush();
      expect(backend.isDirty).toBe(false);
    });

    it("flush is idempotent", async () => {
      const backend = new PreloadBackend(remote);
      await backend.init();

      backend.writePage("/f", 0, new Uint8Array(PAGE_SIZE));
      await backend.flush();
      await backend.flush(); // second flush is a no-op

      expect(backend.isDirty).toBe(false);
    });

    it("@fast flush + re-init roundtrip preserves data", async () => {
      // Write data through preload backend
      const backend1 = new PreloadBackend(remote);
      await backend1.init();

      const data = new Uint8Array(PAGE_SIZE);
      data[0] = 0xbe;
      data[4095] = 0xef;
      backend1.writePage("/roundtrip", 0, data);
      backend1.writeMeta("/roundtrip", {
        size: PAGE_SIZE,
        mode: 0o100644,
        ctime: 1000,
        mtime: 2000,
      });
      await backend1.flush();

      // Create a fresh PreloadBackend on the same remote
      const backend2 = new PreloadBackend(remote);
      await backend2.init();

      const read = backend2.readPage("/roundtrip", 0);
      expect(read![0]).toBe(0xbe);
      expect(read![4095]).toBe(0xef);

      const meta = backend2.readMeta("/roundtrip");
      expect(meta!.size).toBe(PAGE_SIZE);
      expect(meta!.mtime).toBe(2000);
    });
  });

  describe("flush ordering edge cases", () => {
    it("deleteFile then write new pages flushes correctly", async () => {
      // Seed remote
      await remote.writePage("/f", 0, new Uint8Array(PAGE_SIZE));
      await remote.writeMeta("/f", {
        size: PAGE_SIZE,
        mode: 0o100644,
        ctime: 1000,
        mtime: 1000,
      });

      const backend = new PreloadBackend(remote);
      await backend.init();

      // Delete then recreate
      backend.deleteFile("/f");
      const newData = new Uint8Array(PAGE_SIZE);
      newData[0] = 0xff;
      backend.writePage("/f", 0, newData);

      await backend.flush();

      const read = await remote.readPage("/f", 0);
      expect(read![0]).toBe(0xff);
    });

    it("truncation keeps lowest fromIndex", async () => {
      const backend = new PreloadBackend(remote);
      await backend.init();

      // Write 5 pages
      for (let i = 0; i < 5; i++) {
        backend.writePage("/f", i, new Uint8Array(PAGE_SIZE));
      }
      await backend.flush();

      // Truncate to 3, then to 1
      backend.deletePagesFrom("/f", 3);
      backend.deletePagesFrom("/f", 1);

      await backend.flush();

      expect(await remote.readPage("/f", 0)).not.toBeNull();
      expect(await remote.readPage("/f", 1)).toBeNull();
      expect(await remote.readPage("/f", 3)).toBeNull();
    });
  });

  describe("crash recovery: pages beyond meta.size", () => {
    it("@fast init loads pages that exist beyond meta.size", async () => {
      // Simulate crash: pages were written to remote but metadata wasn't
      // updated. meta.size says 1 page, but 3 pages exist.
      const d0 = new Uint8Array(PAGE_SIZE); d0[0] = 0x10;
      const d1 = new Uint8Array(PAGE_SIZE); d1[0] = 0x20;
      const d2 = new Uint8Array(PAGE_SIZE); d2[0] = 0x30;
      await remote.writePage("/f", 0, d0);
      await remote.writePage("/f", 1, d1);
      await remote.writePage("/f", 2, d2);
      await remote.writeMeta("/f", {
        size: PAGE_SIZE, // stale: only accounts for 1 page
        mode: 0o100644,
        ctime: 1000,
        mtime: 1000,
      });

      const backend = new PreloadBackend(remote);
      await backend.init();

      // All 3 pages should be loaded, not just page 0
      expect(backend.readPage("/f", 0)![0]).toBe(0x10);
      expect(backend.readPage("/f", 1)![0]).toBe(0x20);
      expect(backend.readPage("/f", 2)![0]).toBe(0x30);
    });

    it("init loads pages when meta.size is zero", async () => {
      // meta.size is 0 but pages exist (e.g., file created and written
      // but metadata never synced before crash)
      const d0 = new Uint8Array(PAGE_SIZE); d0[0] = 0xaa;
      await remote.writePage("/f", 0, d0);
      await remote.writeMeta("/f", {
        size: 0,
        mode: 0o100644,
        ctime: 1000,
        mtime: 1000,
      });

      const backend = new PreloadBackend(remote);
      await backend.init();

      expect(backend.readPage("/f", 0)![0]).toBe(0xaa);
    });

    it("init loads many pages beyond meta.size", async () => {
      // meta.size says 1 page, but 20 pages exist — tests exponential probe
      for (let i = 0; i < 20; i++) {
        const d = new Uint8Array(PAGE_SIZE);
        d[0] = i;
        await remote.writePage("/f", i, d);
      }
      await remote.writeMeta("/f", {
        size: PAGE_SIZE,
        mode: 0o100644,
        ctime: 1000,
        mtime: 1000,
      });

      const backend = new PreloadBackend(remote);
      await backend.init();

      for (let i = 0; i < 20; i++) {
        const page = backend.readPage("/f", i);
        expect(page).not.toBeNull();
        expect(page![0]).toBe(i);
      }
      // Page 20 should not exist
      expect(backend.readPage("/f", 20)).toBeNull();
    });

    it("init does not load extra pages when meta.size matches actual pages", async () => {
      // No extra pages — init should work as before without loading extras
      const d0 = new Uint8Array(PAGE_SIZE); d0[0] = 0x42;
      await remote.writePage("/f", 0, d0);
      await remote.writeMeta("/f", {
        size: PAGE_SIZE,
        mode: 0o100644,
        ctime: 1000,
        mtime: 1000,
      });

      const backend = new PreloadBackend(remote);
      await backend.init();

      expect(backend.readPage("/f", 0)![0]).toBe(0x42);
      expect(backend.readPage("/f", 1)).toBeNull();
    });
  });

  describe("flush crash safety: write-before-delete ordering", () => {
    it("rename flush writes new pages before deleting old", async () => {
      // Seed remote with data at old path
      const d = new Uint8Array(PAGE_SIZE); d[0] = 0xcc;
      await remote.writePage("/old", 0, d);
      await remote.writeMeta("/old", { size: PAGE_SIZE, mode: 0o100644, ctime: 0, mtime: 0 });

      const backend = new PreloadBackend(remote);
      await backend.init();

      backend.renameFile("/old", "/new");
      await backend.flush();

      // New path should have the data
      expect((await remote.readPage("/new", 0))![0]).toBe(0xcc);
      // Old path should be cleaned up
      expect(await remote.readPage("/old", 0)).toBeNull();
    });

    it("delete then recreate at same path preserves new data", async () => {
      // Seed remote
      const old = new Uint8Array(PAGE_SIZE); old[0] = 0x11;
      await remote.writePage("/f", 0, old);
      await remote.writeMeta("/f", { size: PAGE_SIZE, mode: 0o100644, ctime: 0, mtime: 0 });

      const backend = new PreloadBackend(remote);
      await backend.init();

      // Delete then recreate with different data
      backend.deleteFile("/f");
      const fresh = new Uint8Array(PAGE_SIZE); fresh[0] = 0x99;
      backend.writePage("/f", 0, fresh);

      await backend.flush();

      // Remote should have the NEW data, not old
      const page = await remote.readPage("/f", 0);
      expect(page).not.toBeNull();
      expect(page![0]).toBe(0x99);
    });

    it("flush + re-init roundtrip after rename", async () => {
      // Write data, flush, rename, flush, re-init — data should survive
      const backend1 = new PreloadBackend(remote);
      await backend1.init();

      const d = new Uint8Array(PAGE_SIZE);
      d[0] = 0xdd; d[PAGE_SIZE - 1] = 0xee;
      backend1.writePage("/src", 0, d);
      backend1.writeMeta("/src", { size: PAGE_SIZE, mode: 0o100644, ctime: 0, mtime: 0 });
      await backend1.flush();

      // Rename in a new PreloadBackend session
      const backend2 = new PreloadBackend(remote);
      await backend2.init();
      backend2.renameFile("/src", "/dst");
      backend2.writeMeta("/dst", { size: PAGE_SIZE, mode: 0o100644, ctime: 0, mtime: 0 });
      backend2.deleteMeta("/src");
      await backend2.flush();

      // Re-init and verify
      const backend3 = new PreloadBackend(remote);
      await backend3.init();
      expect(backend3.readPage("/src", 0)).toBeNull();
      const page = backend3.readPage("/dst", 0);
      expect(page).not.toBeNull();
      expect(page![0]).toBe(0xdd);
      expect(page![PAGE_SIZE - 1]).toBe(0xee);
    });
  });
});
