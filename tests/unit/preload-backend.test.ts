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
import type { StorageBackend } from "../../src/storage-backend.js";
import type { FileMeta } from "../../src/types.js";

/**
 * Wrapper around MemoryBackend that counts calls to each method.
 * Used to verify that flush() uses batch operations instead of
 * individual calls (reducing SAB bridge round-trips).
 */
class CountingBackend implements StorageBackend {
  private inner: MemoryBackend;
  calls: Record<string, number> = {};

  constructor(inner: MemoryBackend) {
    this.inner = inner;
  }

  private count(method: string): void {
    this.calls[method] = (this.calls[method] ?? 0) + 1;
  }

  resetCounts(): void {
    this.calls = {};
  }

  async readPage(path: string, pageIndex: number) {
    this.count("readPage");
    return this.inner.readPage(path, pageIndex);
  }
  async readPages(path: string, pageIndices: number[]) {
    this.count("readPages");
    return this.inner.readPages(path, pageIndices);
  }
  async writePage(path: string, pageIndex: number, data: Uint8Array) {
    this.count("writePage");
    return this.inner.writePage(path, pageIndex, data);
  }
  async writePages(pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>) {
    this.count("writePages");
    return this.inner.writePages(pages);
  }
  async deleteFile(path: string) {
    this.count("deleteFile");
    return this.inner.deleteFile(path);
  }
  async deleteFiles(paths: string[]) {
    this.count("deleteFiles");
    return this.inner.deleteFiles(paths);
  }
  async deletePagesFrom(path: string, fromPageIndex: number) {
    this.count("deletePagesFrom");
    return this.inner.deletePagesFrom(path, fromPageIndex);
  }
  async renameFile(oldPath: string, newPath: string) {
    this.count("renameFile");
    return this.inner.renameFile(oldPath, newPath);
  }
  async readMeta(path: string) {
    this.count("readMeta");
    return this.inner.readMeta(path);
  }
  async readMetas(paths: string[]) {
    this.count("readMetas");
    return this.inner.readMetas(paths);
  }
  async writeMeta(path: string, meta: FileMeta) {
    this.count("writeMeta");
    return this.inner.writeMeta(path, meta);
  }
  async writeMetas(entries: Array<{ path: string; meta: FileMeta }>) {
    this.count("writeMetas");
    return this.inner.writeMetas(entries);
  }
  async deleteMeta(path: string) {
    this.count("deleteMeta");
    return this.inner.deleteMeta(path);
  }
  async deleteMetas(paths: string[]) {
    this.count("deleteMetas");
    return this.inner.deleteMetas(paths);
  }
  async countPages(path: string) {
    this.count("countPages");
    return this.inner.countPages(path);
  }
  async maxPageIndex(path: string) {
    this.count("maxPageIndex");
    return this.inner.maxPageIndex(path);
  }
  async listFiles() {
    this.count("listFiles");
    return this.inner.listFiles();
  }
}

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

    it("concurrent init() calls are safe (idempotent)", async () => {
      const data = new Uint8Array(PAGE_SIZE);
      data[0] = 0xcc;
      await remote.writePage("/f", 0, data);
      await remote.writeMeta("/f", {
        size: PAGE_SIZE,
        mode: 0o100644,
        ctime: 1000,
        mtime: 1000,
      });

      const backend = new PreloadBackend(remote);
      // Call init() multiple times concurrently
      await Promise.all([backend.init(), backend.init(), backend.init()]);

      const read = backend.readPage("/f", 0);
      expect(read).not.toBeNull();
      expect(read![0]).toBe(0xcc);
    });

    it("repeated init() after completion is a no-op", async () => {
      const inner = new MemoryBackend();
      const counting = new CountingBackend(inner);
      const backend = new PreloadBackend(counting);

      await backend.init();
      const firstCallCount = counting.calls["listFiles"] ?? 0;

      counting.resetCounts();
      await backend.init();

      // Second init should not call listFiles again
      expect(counting.calls["listFiles"] ?? 0).toBe(0);
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

    it("cleans up extra destination pages when source has fewer pages", async () => {
      // Destination has 4 pages, source has 2 — extra pages must not survive.
      const filledPage = (v: number) => { const d = new Uint8Array(PAGE_SIZE); d.fill(v); return d; };
      for (let i = 0; i < 4; i++) {
        await remote.writePage("/dest", i, filledPage(0xdd));
      }
      await remote.writeMeta("/dest", { size: 4 * PAGE_SIZE, mode: 0o100644, ctime: 0, mtime: 0 });
      await remote.writePage("/src", 0, filledPage(0xaa));
      await remote.writePage("/src", 1, filledPage(0xbb));
      await remote.writeMeta("/src", { size: 2 * PAGE_SIZE, mode: 0o100644, ctime: 0, mtime: 0 });

      const backend = new PreloadBackend(remote);
      await backend.init();

      backend.renameFile("/src", "/dest");

      // Source pages moved to destination
      expect(backend.readPage("/dest", 0)).toEqual(filledPage(0xaa));
      expect(backend.readPage("/dest", 1)).toEqual(filledPage(0xbb));
      // Orphan pages from old destination must be gone
      expect(backend.readPage("/dest", 2)).toBeNull();
      expect(backend.readPage("/dest", 3)).toBeNull();
      // Source is gone
      expect(backend.readPage("/src", 0)).toBeNull();

      // After flush, remote should also be clean
      await backend.flush();
      expect(await remote.readPage("/dest", 0)).toEqual(filledPage(0xaa));
      expect(await remote.readPage("/dest", 1)).toEqual(filledPage(0xbb));
      expect(await remote.readPage("/dest", 2)).toBeNull();
      expect(await remote.readPage("/dest", 3)).toBeNull();
      expect(await remote.readPage("/src", 0)).toBeNull();
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

  describe("init batching", () => {
    it("@fast batches metadata reads into a single readMetas call during init", async () => {
      const inner = new MemoryBackend();
      const counting = new CountingBackend(inner);

      // Seed 5 files with metadata
      for (let i = 0; i < 5; i++) {
        await inner.writeMeta(`/file${i}`, {
          size: 0,
          mode: 0o100644,
          ctime: 1000,
          mtime: 1000,
        });
      }

      counting.resetCounts();
      const backend = new PreloadBackend(counting);
      await backend.init();

      // Should use 1 readMetas call, not 5 individual readMeta calls
      expect(counting.calls["readMetas"]).toBe(1);
      expect(counting.calls["readMeta"] ?? 0).toBe(0);

      // Verify all metadata was loaded
      for (let i = 0; i < 5; i++) {
        const meta = backend.readMeta(`/file${i}`);
        expect(meta).not.toBeNull();
        expect(meta!.mode).toBe(0o100644);
      }
    });
  });

  describe("flush batching", () => {
    it("@fast batches multiple metadata writes into a single writeMetas call", async () => {
      const inner = new MemoryBackend();
      const counting = new CountingBackend(inner);
      const backend = new PreloadBackend(counting);
      await backend.init();

      // Write 5 metadata entries
      for (let i = 0; i < 5; i++) {
        backend.writeMeta(`/file${i}`, {
          size: 0,
          mode: 0o100644,
          ctime: 1000,
          mtime: 1000,
        });
      }

      counting.resetCounts();
      await backend.flush();

      // Should use 1 writeMetas call, not 5 individual writeMeta calls
      expect(counting.calls["writeMetas"]).toBe(1);
      expect(counting.calls["writeMeta"] ?? 0).toBe(0);
    });

    it("batches multiple metadata deletes into a single deleteMetas call", async () => {
      const inner = new MemoryBackend();
      const counting = new CountingBackend(inner);
      const backend = new PreloadBackend(counting);

      // Seed with metadata
      for (let i = 0; i < 5; i++) {
        await inner.writeMeta(`/file${i}`, {
          size: 0,
          mode: 0o100644,
          ctime: 1000,
          mtime: 1000,
        });
      }
      await backend.init();

      // Delete all metadata entries
      for (let i = 0; i < 5; i++) {
        backend.deleteMeta(`/file${i}`);
      }

      counting.resetCounts();
      await backend.flush();

      // Should use 1 deleteMetas call, not 5 individual deleteMeta calls
      expect(counting.calls["deleteMetas"]).toBe(1);
      expect(counting.calls["deleteMeta"] ?? 0).toBe(0);
    });

    it("batches multiple file deletions into a single deleteFiles call", async () => {
      const inner = new MemoryBackend();
      const counting = new CountingBackend(inner);
      const backend = new PreloadBackend(counting);

      // Seed with files
      for (let i = 0; i < 3; i++) {
        await inner.writePage(`/file${i}`, 0, new Uint8Array(PAGE_SIZE));
        await inner.writeMeta(`/file${i}`, {
          size: PAGE_SIZE,
          mode: 0o100644,
          ctime: 1000,
          mtime: 1000,
        });
      }
      await backend.init();

      // Delete all files
      for (let i = 0; i < 3; i++) {
        backend.deleteFile(`/file${i}`);
      }

      counting.resetCounts();
      await backend.flush();

      // Should use 1 deleteFiles call, not 3 individual deleteFile calls
      expect(counting.calls["deleteFiles"]).toBe(1);
      expect(counting.calls["deleteFile"] ?? 0).toBe(0);
    });

    it("parallelizes multiple truncations", async () => {
      const inner = new MemoryBackend();
      const counting = new CountingBackend(inner);
      const backend = new PreloadBackend(counting);
      await backend.init();

      // Write pages for 3 files, then truncate each
      for (let i = 0; i < 3; i++) {
        for (let p = 0; p < 5; p++) {
          backend.writePage(`/file${i}`, p, new Uint8Array(PAGE_SIZE));
        }
      }
      await backend.flush();

      for (let i = 0; i < 3; i++) {
        backend.deletePagesFrom(`/file${i}`, 1);
      }

      counting.resetCounts();
      await backend.flush();

      // All 3 truncation calls should happen (in parallel)
      expect(counting.calls["deletePagesFrom"]).toBe(3);
    });

    it("batches late metadata writes for delete-then-recreate paths", async () => {
      const inner = new MemoryBackend();
      const counting = new CountingBackend(inner);
      const backend = new PreloadBackend(counting);

      // Seed remote
      for (let i = 0; i < 3; i++) {
        await inner.writePage(`/file${i}`, 0, new Uint8Array(PAGE_SIZE));
        await inner.writeMeta(`/file${i}`, {
          size: PAGE_SIZE,
          mode: 0o100644,
          ctime: 1000,
          mtime: 1000,
        });
      }
      await backend.init();

      // Delete and recreate all 3 files with new metadata
      for (let i = 0; i < 3; i++) {
        backend.deleteFile(`/file${i}`);
        backend.writePage(`/file${i}`, 0, new Uint8Array(PAGE_SIZE));
        backend.writeMeta(`/file${i}`, {
          size: PAGE_SIZE,
          mode: 0o100644,
          ctime: 2000,
          mtime: 2000,
        });
      }

      counting.resetCounts();
      await backend.flush();

      // Late metadata should be batched into a single writeMetas call
      expect(counting.calls["writeMetas"]).toBe(1);
      expect(counting.calls["writeMeta"] ?? 0).toBe(0);

      // Verify data integrity
      for (let i = 0; i < 3; i++) {
        const meta = await inner.readMeta(`/file${i}`);
        expect(meta!.mtime).toBe(2000);
      }
    });

    it("no remote calls when nothing is dirty", async () => {
      const inner = new MemoryBackend();
      const counting = new CountingBackend(inner);
      const backend = new PreloadBackend(counting);
      await backend.init();

      counting.resetCounts();
      await backend.flush();

      // No remote calls should happen at all
      const totalCalls = Object.values(counting.calls).reduce((a, b) => a + b, 0);
      expect(totalCalls).toBe(0);
    });
  });

  describe("flush error recovery", () => {
    /**
     * Backend fake that fails writePages on demand.
     * Not a mock — delegates to a real MemoryBackend for all operations.
     */
    class FailingRemote implements StorageBackend {
      private inner = new MemoryBackend();
      writePagesFailCount = 0;

      async readPage(path: string, pageIndex: number) { return this.inner.readPage(path, pageIndex); }
      async readPages(path: string, pageIndices: number[]) { return this.inner.readPages(path, pageIndices); }
      async writePage(path: string, pageIndex: number, data: Uint8Array) { return this.inner.writePage(path, pageIndex, data); }
      async writePages(pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>) {
        if (this.writePagesFailCount > 0) {
          this.writePagesFailCount--;
          throw new Error("injected writePages failure");
        }
        return this.inner.writePages(pages);
      }
      async deleteFile(path: string) { return this.inner.deleteFile(path); }
      async deleteFiles(paths: string[]) { return this.inner.deleteFiles(paths); }
      async deletePagesFrom(path: string, fromPageIndex: number) { return this.inner.deletePagesFrom(path, fromPageIndex); }
      async renameFile(oldPath: string, newPath: string) { return this.inner.renameFile(oldPath, newPath); }
      async readMeta(path: string) { return this.inner.readMeta(path); }
      async readMetas(paths: string[]) { return this.inner.readMetas(paths); }
      async writeMeta(path: string, meta: FileMeta) { return this.inner.writeMeta(path, meta); }
      async writeMetas(entries: Array<{ path: string; meta: FileMeta }>) { return this.inner.writeMetas(entries); }
      async deleteMeta(path: string) { return this.inner.deleteMeta(path); }
      async deleteMetas(paths: string[]) { return this.inner.deleteMetas(paths); }
      async countPages(path: string) { return this.inner.countPages(path); }
      async maxPageIndex(path: string) { return this.inner.maxPageIndex(path); }
      async listFiles() { return this.inner.listFiles(); }
    }

    it("retains dirty tracking when writePages fails so retry succeeds", async () => {
      const failing = new FailingRemote();
      const backend = new PreloadBackend(failing);
      await backend.init();

      // Write a page and metadata
      const data = new Uint8Array(PAGE_SIZE);
      data[0] = 0xaa;
      data[1] = 0xbb;
      backend.writePage("/file", 0, data);
      backend.writeMeta("/file", { size: PAGE_SIZE, mode: 0o100644, ctime: 1000, mtime: 1000 });

      expect(backend.dirtyPageCount).toBe(1);
      expect(backend.dirtyMetaCount).toBe(1);

      // First flush fails
      failing.writePagesFailCount = 1;
      await expect(backend.flush()).rejects.toThrow("injected writePages failure");

      // Dirty tracking must be preserved for retry
      expect(backend.dirtyPageCount).toBe(1);
      expect(backend.dirtyMetaCount).toBe(1);

      // Retry succeeds
      await backend.flush();
      expect(backend.dirtyPageCount).toBe(0);
      expect(backend.dirtyMetaCount).toBe(0);

      // Verify data reached the remote backend
      const stored = await failing.readPage("/file", 0);
      expect(stored).not.toBeNull();
      expect(stored![0]).toBe(0xaa);
      expect(stored![1]).toBe(0xbb);
    });

    it("preserves new writes made during a failed flush", async () => {
      const failing = new FailingRemote();
      const backend = new PreloadBackend(failing);
      await backend.init();

      // Write initial data
      const data1 = new Uint8Array(PAGE_SIZE);
      data1[0] = 0x11;
      backend.writePage("/a", 0, data1);
      backend.writeMeta("/a", { size: PAGE_SIZE, mode: 0o100644, ctime: 1000, mtime: 1000 });

      // Fail the first flush
      failing.writePagesFailCount = 1;
      await expect(backend.flush()).rejects.toThrow("injected writePages failure");

      // Write new data while first flush's dirty entries are still pending
      const data2 = new Uint8Array(PAGE_SIZE);
      data2[0] = 0x22;
      backend.writePage("/b", 0, data2);
      backend.writeMeta("/b", { size: PAGE_SIZE, mode: 0o100644, ctime: 2000, mtime: 2000 });

      // Both original and new entries should be dirty
      expect(backend.dirtyPageCount).toBe(2);
      expect(backend.dirtyMetaCount).toBe(2);

      // Retry succeeds — both files flushed
      await backend.flush();
      expect(backend.dirtyPageCount).toBe(0);
      expect(backend.dirtyMetaCount).toBe(0);

      const storedA = await failing.readPage("/a", 0);
      expect(storedA![0]).toBe(0x11);
      const storedB = await failing.readPage("/b", 0);
      expect(storedB![0]).toBe(0x22);
    });
  });

  describe("file page index correctness", () => {
    it("@fast deleteFile only removes target file pages, not others", async () => {
      const backend = new PreloadBackend(remote);
      await backend.init();

      // Write pages for two files
      for (let i = 0; i < 10; i++) {
        const page = new Uint8Array(PAGE_SIZE);
        page[0] = i;
        backend.writePage("/fileA", i, page);
        backend.writePage("/fileB", i, page);
      }

      // Delete fileA
      backend.deleteFile("/fileA");

      // fileA pages gone
      for (let i = 0; i < 10; i++) {
        expect(backend.readPage("/fileA", i)).toBeNull();
      }
      // fileB pages intact
      for (let i = 0; i < 10; i++) {
        const page = backend.readPage("/fileB", i);
        expect(page).not.toBeNull();
        expect(page![0]).toBe(i);
      }
    });

    it("@fast renameFile moves pages without affecting other files", async () => {
      const backend = new PreloadBackend(remote);
      await backend.init();

      for (let i = 0; i < 5; i++) {
        const page = new Uint8Array(PAGE_SIZE);
        page[0] = i + 1;
        backend.writePage("/src", i, page);
        backend.writePage("/other", i, new Uint8Array(PAGE_SIZE));
      }

      backend.renameFile("/src", "/dst");

      // src pages gone
      for (let i = 0; i < 5; i++) {
        expect(backend.readPage("/src", i)).toBeNull();
      }
      // dst pages have correct data
      for (let i = 0; i < 5; i++) {
        const page = backend.readPage("/dst", i);
        expect(page).not.toBeNull();
        expect(page![0]).toBe(i + 1);
      }
      // other file untouched
      for (let i = 0; i < 5; i++) {
        expect(backend.readPage("/other", i)).not.toBeNull();
      }
    });

    it("@fast deletePagesFrom only removes pages at or beyond index", async () => {
      const backend = new PreloadBackend(remote);
      await backend.init();

      for (let i = 0; i < 8; i++) {
        const page = new Uint8Array(PAGE_SIZE);
        page[0] = i;
        backend.writePage("/file", i, page);
      }

      backend.deletePagesFrom("/file", 4);

      // Pages 0-3 intact
      for (let i = 0; i < 4; i++) {
        const page = backend.readPage("/file", i);
        expect(page).not.toBeNull();
        expect(page![0]).toBe(i);
      }
      // Pages 4-7 gone
      for (let i = 4; i < 8; i++) {
        expect(backend.readPage("/file", i)).toBeNull();
      }
    });

    it("@fast interleaved operations maintain index consistency", async () => {
      const backend = new PreloadBackend(remote);
      await backend.init();

      // Create 5 files with 3 pages each
      for (let f = 0; f < 5; f++) {
        for (let p = 0; p < 3; p++) {
          const page = new Uint8Array(PAGE_SIZE);
          page[0] = f * 10 + p;
          backend.writePage(`/f${f}`, p, page);
        }
      }

      // Delete f1, rename f2→f1, truncate f3 from page 1
      backend.deleteFile("/f1");
      backend.renameFile("/f2", "/f1");
      backend.deletePagesFrom("/f3", 1);

      // f0 untouched
      for (let p = 0; p < 3; p++) {
        expect(backend.readPage("/f0", p)![0]).toBe(p);
      }
      // f1 now has f2's data
      for (let p = 0; p < 3; p++) {
        expect(backend.readPage("/f1", p)![0]).toBe(20 + p);
      }
      // f2 gone
      for (let p = 0; p < 3; p++) {
        expect(backend.readPage("/f2", p)).toBeNull();
      }
      // f3 only has page 0
      expect(backend.readPage("/f3", 0)![0]).toBe(30);
      expect(backend.readPage("/f3", 1)).toBeNull();
      // f4 untouched
      for (let p = 0; p < 3; p++) {
        expect(backend.readPage("/f4", p)![0]).toBe(40 + p);
      }

      // Flush and verify remote state matches
      backend.writeMeta("/f0", { size: PAGE_SIZE * 3, mode: 0o100644, ctime: 1, mtime: 1 });
      backend.writeMeta("/f1", { size: PAGE_SIZE * 3, mode: 0o100644, ctime: 1, mtime: 1 });
      backend.writeMeta("/f3", { size: PAGE_SIZE, mode: 0o100644, ctime: 1, mtime: 1 });
      backend.writeMeta("/f4", { size: PAGE_SIZE * 3, mode: 0o100644, ctime: 1, mtime: 1 });
      await backend.flush();

      // Verify remote has correct data
      const remoteF1P0 = await remote.readPage("/f1", 0);
      expect(remoteF1P0![0]).toBe(20);
    });
  });
});
