/**
 * Adversarial tests: PreloadBackend sync writes during async flush().
 *
 * flush() snapshots dirty tracking sets before performing async backend
 * operations, then clears only the snapshotted entries on success (PR #332).
 * These tests validate that sync writes occurring during the async gap
 * (while flush's awaits yield to the event loop) are preserved for the
 * next flush cycle.
 *
 * The HookableBackend injects callbacks during async operations, simulating
 * the scenario where Emscripten FS operations (which are synchronous) write
 * to the PreloadBackend while a flush is in progress.
 *
 * Attack surfaces:
 * - New file writes during flush → dirty after flush completes
 * - Overwrite of a page being flushed → latest data wins on next flush
 * - Delete of a file being flushed → must not resurrect on reinit
 * - New truncation during flush → persists in next flush
 * - Metadata write during flush → persists in next flush
 * - Mixed delete-then-recreate arriving during ongoing flush
 *
 * Ethos §9 (adversarial — target async concurrency seams)
 * Ethos §10 (graceful degradation — PreloadBackend is the no-SAB path)
 */
import { describe, it, expect } from "vitest";
import { PreloadBackend } from "../../src/preload-backend.js";
import { MemoryBackend } from "../../src/memory-backend.js";
import { PAGE_SIZE } from "../../src/types.js";
import type { StorageBackend } from "../../src/storage-backend.js";
import type { FileMeta } from "../../src/types.js";

// ---------------------------------------------------------------
// HookableBackend: injects callbacks during async operations
// ---------------------------------------------------------------

type HookPoint = "syncAll" | "deleteFiles" | "deleteMetas" | "deletePagesFrom";

class HookableBackend implements StorageBackend {
  private inner = new MemoryBackend();
  private hooks = new Map<HookPoint, () => void>();

  onBefore(point: HookPoint, fn: () => void): void {
    this.hooks.set(point, fn);
  }

  clearHooks(): void {
    this.hooks.clear();
  }

  private runHook(point: HookPoint): void {
    const fn = this.hooks.get(point);
    if (fn) fn();
  }

  async readPage(path: string, pageIndex: number) {
    return this.inner.readPage(path, pageIndex);
  }
  async readPages(path: string, pageIndices: number[]) {
    return this.inner.readPages(path, pageIndices);
  }
  async readPageBatch(entries: Array<{ path: string; pageIndex: number }>) {
    return this.inner.readPageBatch(entries);
  }
  async writePage(path: string, pageIndex: number, data: Uint8Array) {
    return this.inner.writePage(path, pageIndex, data);
  }
  async writePages(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
  ) {
    return this.inner.writePages(pages);
  }
  async deleteFile(path: string) {
    return this.inner.deleteFile(path);
  }
  async deleteFiles(paths: string[]) {
    this.runHook("deleteFiles");
    return this.inner.deleteFiles(paths);
  }
  async deletePagesFrom(path: string, fromPageIndex: number) {
    this.runHook("deletePagesFrom");
    return this.inner.deletePagesFrom(path, fromPageIndex);
  }
  async renameFile(oldPath: string, newPath: string) {
    return this.inner.renameFile(oldPath, newPath);
  }
  async countPages(path: string) {
    return this.inner.countPages(path);
  }
  async countPagesBatch(paths: string[]) {
    return this.inner.countPagesBatch(paths);
  }
  async maxPageIndex(path: string) {
    return this.inner.maxPageIndex(path);
  }
  async maxPageIndexBatch(paths: string[]) {
    return this.inner.maxPageIndexBatch(paths);
  }
  async readMeta(path: string) {
    return this.inner.readMeta(path);
  }
  async readMetas(paths: string[]) {
    return this.inner.readMetas(paths);
  }
  async writeMeta(path: string, meta: FileMeta) {
    return this.inner.writeMeta(path, meta);
  }
  async writeMetas(entries: Array<{ path: string; meta: FileMeta }>) {
    return this.inner.writeMetas(entries);
  }
  async deleteMeta(path: string) {
    return this.inner.deleteMeta(path);
  }
  async deleteMetas(paths: string[]) {
    this.runHook("deleteMetas");
    return this.inner.deleteMetas(paths);
  }
  async listFiles() {
    return this.inner.listFiles();
  }
  async syncAll(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
    metas: Array<{ path: string; meta: FileMeta }>,
  ) {
    this.runHook("syncAll");
    return this.inner.syncAll(pages, metas);
  }
  async deleteAll(paths: string[]) {
    return this.inner.deleteAll(paths);
  }
}

// ---------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------

function makePage(fill: number): Uint8Array {
  const buf = new Uint8Array(PAGE_SIZE);
  buf.fill(fill);
  return buf;
}

const baseMeta: FileMeta = {
  size: PAGE_SIZE,
  mode: 0o100644,
  ctime: 1000,
  mtime: 1000,
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

describe("PreloadBackend: sync writes during async flush()", () => {
  describe("fast path (no deletes/truncations)", () => {
    it("@fast new file written during syncAll appears dirty after flush", async () => {
      const remote = new HookableBackend();
      const preload = new PreloadBackend(remote);
      await preload.init();

      preload.writePage("/existing", 0, makePage(0x11));
      preload.writeMeta("/existing", baseMeta);

      remote.onBefore("syncAll", () => {
        preload.writePage("/new-during-flush", 0, makePage(0x22));
        preload.writeMeta("/new-during-flush", baseMeta);
      });

      await preload.flush();
      remote.clearHooks();

      // /existing should be flushed (no longer dirty)
      // /new-during-flush was written during flush — must still be dirty
      expect(preload.isDirty).toBe(true);
      expect(preload.dirtyPageCount).toBeGreaterThan(0);

      // Second flush persists the concurrent write
      await preload.flush();
      expect(preload.isDirty).toBe(false);

      // Verify via reinit
      const fresh = new PreloadBackend(remote);
      await fresh.init();
      expect(fresh.readPage("/existing", 0)![0]).toBe(0x11);
      expect(fresh.readPage("/new-during-flush", 0)![0]).toBe(0x22);
    });

    it("@fast overwrite of flushing page during syncAll preserves latest data", async () => {
      const remote = new HookableBackend();
      const preload = new PreloadBackend(remote);
      await preload.init();

      preload.writePage("/file", 0, makePage(0x11));
      preload.writeMeta("/file", baseMeta);

      remote.onBefore("syncAll", () => {
        // Overwrite the same page being flushed
        preload.writePage("/file", 0, makePage(0x99));
      });

      await preload.flush();
      remote.clearHooks();

      // The page was re-dirtied during flush
      expect(preload.isDirty).toBe(true);

      // Second flush writes the latest data (0x99)
      await preload.flush();
      expect(preload.isDirty).toBe(false);

      const fresh = new PreloadBackend(remote);
      await fresh.init();
      expect(fresh.readPage("/file", 0)![0]).toBe(0x99);
    });

    it("metadata update during syncAll preserved for next flush", async () => {
      const remote = new HookableBackend();
      const preload = new PreloadBackend(remote);
      await preload.init();

      preload.writePage("/file", 0, makePage(0x11));
      preload.writeMeta("/file", baseMeta);

      remote.onBefore("syncAll", () => {
        preload.writeMeta("/file", { ...baseMeta, mtime: 9999 });
      });

      await preload.flush();
      remote.clearHooks();

      expect(preload.isDirty).toBe(true);
      expect(preload.dirtyMetaCount).toBeGreaterThan(0);

      await preload.flush();

      const fresh = new PreloadBackend(remote);
      await fresh.init();
      expect(fresh.readMeta("/file")!.mtime).toBe(9999);
    });

    it("multiple pages written to new file during flush all persist", async () => {
      const remote = new HookableBackend();
      const preload = new PreloadBackend(remote);
      await preload.init();

      preload.writePage("/a", 0, makePage(0x01));
      preload.writeMeta("/a", baseMeta);

      remote.onBefore("syncAll", () => {
        for (let i = 0; i < 4; i++) {
          preload.writePage("/big", i, makePage(0x10 + i));
        }
        preload.writeMeta("/big", { ...baseMeta, size: PAGE_SIZE * 4 });
      });

      await preload.flush();
      remote.clearHooks();

      expect(preload.isDirty).toBe(true);
      await preload.flush();

      const fresh = new PreloadBackend(remote);
      await fresh.init();
      expect(fresh.readPage("/a", 0)![0]).toBe(0x01);
      for (let i = 0; i < 4; i++) {
        expect(fresh.readPage("/big", i)![0]).toBe(0x10 + i);
      }
      expect(fresh.readMeta("/big")!.size).toBe(PAGE_SIZE * 4);
    });
  });

  describe("complex path (with deletes/truncations)", () => {
    it("@fast new write during deleteFiles step preserved", async () => {
      const remote = new HookableBackend();
      const preload = new PreloadBackend(remote);
      await preload.init();

      // Set up: one file to delete, one to keep
      preload.writePage("/keep", 0, makePage(0x11));
      preload.writeMeta("/keep", baseMeta);
      preload.writePage("/victim", 0, makePage(0x22));
      preload.writeMeta("/victim", baseMeta);
      await preload.flush();

      // Delete /victim — triggers complex flush path
      preload.deleteFile("/victim");
      preload.deleteMeta("/victim");

      // Modify /keep so early batch is non-empty
      preload.writePage("/keep", 0, makePage(0x33));

      remote.onBefore("deleteFiles", () => {
        preload.writePage("/born-during-delete", 0, makePage(0x44));
        preload.writeMeta("/born-during-delete", baseMeta);
      });

      await preload.flush();
      remote.clearHooks();

      // The concurrent write must survive
      expect(preload.isDirty).toBe(true);
      await preload.flush();
      expect(preload.isDirty).toBe(false);

      const fresh = new PreloadBackend(remote);
      await fresh.init();
      expect(fresh.readPage("/keep", 0)![0]).toBe(0x33);
      expect(fresh.readPage("/born-during-delete", 0)![0]).toBe(0x44);
      expect(fresh.readPage("/victim", 0)).toBeNull();
    });

    it("@fast new truncation during syncAll preserved for next flush", async () => {
      const remote = new HookableBackend();
      const preload = new PreloadBackend(remote);
      await preload.init();

      // Set up multi-page files
      for (let i = 0; i < 4; i++) {
        preload.writePage("/data", i, makePage(0x50 + i));
      }
      preload.writeMeta("/data", { ...baseMeta, size: PAGE_SIZE * 4 });

      // Also create a file to trigger a deletion (complex path)
      preload.writePage("/todelete", 0, makePage(0xff));
      preload.writeMeta("/todelete", baseMeta);
      await preload.flush();

      // Delete /todelete to trigger complex path
      preload.deleteFile("/todelete");
      preload.deleteMeta("/todelete");

      // Also dirty /data so early batch is non-empty
      preload.writePage("/data", 0, makePage(0x60));

      remote.onBefore("syncAll", () => {
        // Truncate /data during the flush
        preload.deletePagesFrom("/data", 2);
        preload.writeMeta("/data", { ...baseMeta, size: PAGE_SIZE * 2 });
      });

      await preload.flush();
      remote.clearHooks();

      // Truncation happened during flush — must be dirty
      expect(preload.isDirty).toBe(true);
      await preload.flush();

      const fresh = new PreloadBackend(remote);
      await fresh.init();
      expect(fresh.readPage("/data", 0)![0]).toBe(0x60);
      expect(fresh.readPage("/data", 1)![0]).toBe(0x51);
      expect(fresh.readPage("/data", 2)).toBeNull();
      expect(fresh.readPage("/data", 3)).toBeNull();
      expect(fresh.readMeta("/data")!.size).toBe(PAGE_SIZE * 2);
    });

    it("delete of different file during deleteFiles step preserved", async () => {
      const remote = new HookableBackend();
      const preload = new PreloadBackend(remote);
      await preload.init();

      preload.writePage("/a", 0, makePage(0x0a));
      preload.writeMeta("/a", baseMeta);
      preload.writePage("/b", 0, makePage(0x0b));
      preload.writeMeta("/b", baseMeta);
      preload.writePage("/c", 0, makePage(0x0c));
      preload.writeMeta("/c", baseMeta);
      await preload.flush();

      // Delete /a (triggers complex path)
      preload.deleteFile("/a");
      preload.deleteMeta("/a");
      // Dirty /c so early batch is non-empty
      preload.writePage("/c", 0, makePage(0x1c));

      remote.onBefore("deleteFiles", () => {
        // Delete /b during the deleteFiles call for /a
        preload.deleteFile("/b");
        preload.deleteMeta("/b");
      });

      await preload.flush();
      remote.clearHooks();

      // /b deletion happened during flush — must be pending
      expect(preload.isDirty).toBe(true);
      await preload.flush();

      const fresh = new PreloadBackend(remote);
      await fresh.init();
      expect(fresh.readPage("/a", 0)).toBeNull();
      expect(fresh.readPage("/b", 0)).toBeNull();
      expect(fresh.readPage("/c", 0)![0]).toBe(0x1c);
      expect(fresh.listFiles()).toEqual(["/c"]);
    });

    it("rename during syncAll preserved for next flush", async () => {
      const remote = new HookableBackend();
      const preload = new PreloadBackend(remote);
      await preload.init();

      preload.writePage("/src", 0, makePage(0xaa));
      preload.writeMeta("/src", baseMeta);
      preload.writePage("/other", 0, makePage(0xbb));
      preload.writeMeta("/other", baseMeta);

      // Delete /other to trigger complex path
      preload.writePage("/todelete", 0, makePage(0xff));
      preload.writeMeta("/todelete", baseMeta);
      await preload.flush();

      preload.deleteFile("/todelete");
      preload.deleteMeta("/todelete");
      preload.writePage("/src", 0, makePage(0xcc));

      remote.onBefore("syncAll", () => {
        preload.renameFile("/src", "/dst");
        preload.deleteMeta("/src");
        preload.writeMeta("/dst", baseMeta);
      });

      await preload.flush();
      remote.clearHooks();

      expect(preload.isDirty).toBe(true);
      await preload.flush();

      const fresh = new PreloadBackend(remote);
      await fresh.init();
      expect(fresh.readPage("/src", 0)).toBeNull();
      expect(fresh.readPage("/dst", 0)![0]).toBe(0xcc);
      expect(fresh.readPage("/todelete", 0)).toBeNull();
    });
  });

  describe("delete-then-recreate during flush", () => {
    it("@fast delete-then-recreate at flushing path during syncAll", async () => {
      const remote = new HookableBackend();
      const preload = new PreloadBackend(remote);
      await preload.init();

      preload.writePage("/file", 0, makePage(0x11));
      preload.writeMeta("/file", baseMeta);
      await preload.flush();

      // Modify the file (triggers a simple flush)
      preload.writePage("/file", 0, makePage(0x22));

      remote.onBefore("syncAll", () => {
        // Delete and recreate with different data during flush
        preload.deleteFile("/file");
        preload.writePage("/file", 0, makePage(0x33));
        preload.writeMeta("/file", { ...baseMeta, mtime: 5555 });
      });

      await preload.flush();
      remote.clearHooks();

      // The delete-then-recreate during flush must survive
      expect(preload.isDirty).toBe(true);
      await preload.flush();

      const fresh = new PreloadBackend(remote);
      await fresh.init();
      expect(fresh.readPage("/file", 0)![0]).toBe(0x33);
      expect(fresh.readMeta("/file")!.mtime).toBe(5555);
    });

    it("delete-then-recreate of non-flushed file during syncAll", async () => {
      const remote = new HookableBackend();
      const preload = new PreloadBackend(remote);
      await preload.init();

      preload.writePage("/flushing", 0, makePage(0x11));
      preload.writeMeta("/flushing", baseMeta);
      preload.writePage("/bystander", 0, makePage(0x22));
      preload.writeMeta("/bystander", baseMeta);
      await preload.flush();

      preload.writePage("/flushing", 0, makePage(0x33));

      remote.onBefore("syncAll", () => {
        preload.deleteFile("/bystander");
        preload.writePage("/bystander", 0, makePage(0x44));
        preload.writeMeta("/bystander", { ...baseMeta, mtime: 7777 });
      });

      await preload.flush();
      remote.clearHooks();

      expect(preload.isDirty).toBe(true);
      await preload.flush();

      const fresh = new PreloadBackend(remote);
      await fresh.init();
      expect(fresh.readPage("/flushing", 0)![0]).toBe(0x33);
      expect(fresh.readPage("/bystander", 0)![0]).toBe(0x44);
      expect(fresh.readMeta("/bystander")!.mtime).toBe(7777);
    });
  });

  describe("metadata-only mutations during flush", () => {
    it("@fast metadata delete during deleteMetas step preserved", async () => {
      const remote = new HookableBackend();
      const preload = new PreloadBackend(remote);
      await preload.init();

      preload.writePage("/a", 0, makePage(0x0a));
      preload.writeMeta("/a", baseMeta);
      preload.writePage("/b", 0, makePage(0x0b));
      preload.writeMeta("/b", baseMeta);
      preload.writePage("/c", 0, makePage(0x0c));
      preload.writeMeta("/c", baseMeta);
      await preload.flush();

      // Delete /a to trigger complex path
      preload.deleteFile("/a");
      preload.deleteMeta("/a");
      // Dirty /b
      preload.writePage("/b", 0, makePage(0x1b));

      remote.onBefore("deleteMetas", () => {
        // Delete /c's metadata during the deleteMetas call
        preload.deleteMeta("/c");
      });

      await preload.flush();
      remote.clearHooks();

      expect(preload.isDirty).toBe(true);
      await preload.flush();

      const fresh = new PreloadBackend(remote);
      await fresh.init();
      expect(fresh.readMeta("/a")).toBeNull();
      expect(fresh.readMeta("/c")).toBeNull();
      expect(fresh.readPage("/b", 0)![0]).toBe(0x1b);
    });

    it("new metadata write for existing file during syncAll", async () => {
      const remote = new HookableBackend();
      const preload = new PreloadBackend(remote);
      await preload.init();

      preload.writePage("/file", 0, makePage(0x11));
      preload.writeMeta("/file", { ...baseMeta, mode: 0o100644 });

      // Add a deletion to trigger complex path
      preload.writePage("/del", 0, makePage(0xff));
      preload.writeMeta("/del", baseMeta);
      await preload.flush();

      preload.deleteFile("/del");
      preload.deleteMeta("/del");
      preload.writePage("/file", 0, makePage(0x22));

      remote.onBefore("syncAll", () => {
        // chmod during flush
        preload.writeMeta("/file", { ...baseMeta, mode: 0o100755, mtime: 8888 });
      });

      await preload.flush();
      remote.clearHooks();

      expect(preload.isDirty).toBe(true);
      await preload.flush();

      const fresh = new PreloadBackend(remote);
      await fresh.init();
      expect(fresh.readMeta("/file")!.mode).toBe(0o100755);
      expect(fresh.readMeta("/file")!.mtime).toBe(8888);
    });
  });

  describe("invariant checks after concurrent mutations", () => {
    it("assertInvariants passes after concurrent write + flush + flush", async () => {
      const remote = new HookableBackend();
      const preload = new PreloadBackend(remote);
      await preload.init();

      preload.writePage("/a", 0, makePage(0x01));
      preload.writeMeta("/a", baseMeta);

      remote.onBefore("syncAll", () => {
        preload.writePage("/b", 0, makePage(0x02));
        preload.writeMeta("/b", baseMeta);
        preload.writePage("/a", 1, makePage(0x03));
      });

      await preload.flush();
      remote.clearHooks();
      preload.assertInvariants();

      await preload.flush();
      preload.assertInvariants();

      expect(preload.isDirty).toBe(false);
    });

    it("assertInvariants passes after concurrent delete + flush + flush", async () => {
      const remote = new HookableBackend();
      const preload = new PreloadBackend(remote);
      await preload.init();

      preload.writePage("/x", 0, makePage(0x10));
      preload.writeMeta("/x", baseMeta);
      preload.writePage("/y", 0, makePage(0x20));
      preload.writeMeta("/y", baseMeta);
      await preload.flush();

      preload.writePage("/x", 0, makePage(0x11));

      remote.onBefore("syncAll", () => {
        preload.deleteFile("/y");
        preload.deleteMeta("/y");
      });

      await preload.flush();
      remote.clearHooks();
      preload.assertInvariants();

      await preload.flush();
      preload.assertInvariants();

      const fresh = new PreloadBackend(remote);
      await fresh.init();
      expect(fresh.readPage("/x", 0)![0]).toBe(0x11);
      expect(fresh.readPage("/y", 0)).toBeNull();
      expect(fresh.listFiles()).toEqual(["/x"]);
    });

    it("assertInvariants passes after concurrent truncation + flush + flush", async () => {
      const remote = new HookableBackend();
      const preload = new PreloadBackend(remote);
      await preload.init();

      for (let i = 0; i < 5; i++) {
        preload.writePage("/big", i, makePage(0x30 + i));
      }
      preload.writeMeta("/big", { ...baseMeta, size: PAGE_SIZE * 5 });
      // Need a delete to trigger complex path
      preload.writePage("/temp", 0, makePage(0xff));
      preload.writeMeta("/temp", baseMeta);
      await preload.flush();

      preload.deleteFile("/temp");
      preload.deleteMeta("/temp");
      preload.writePage("/big", 0, makePage(0x40));

      remote.onBefore("syncAll", () => {
        preload.deletePagesFrom("/big", 3);
        preload.writeMeta("/big", { ...baseMeta, size: PAGE_SIZE * 3 });
      });

      await preload.flush();
      remote.clearHooks();
      preload.assertInvariants();

      await preload.flush();
      preload.assertInvariants();

      const fresh = new PreloadBackend(remote);
      await fresh.init();
      expect(fresh.readPage("/big", 0)![0]).toBe(0x40);
      expect(fresh.readPage("/big", 1)![0]).toBe(0x31);
      expect(fresh.readPage("/big", 2)![0]).toBe(0x32);
      expect(fresh.readPage("/big", 3)).toBeNull();
      expect(fresh.readMeta("/big")!.size).toBe(PAGE_SIZE * 3);
    });
  });

  describe("stress: high-volume concurrent mutations", () => {
    it("20 files written during flush of 10 files all persist", async () => {
      const remote = new HookableBackend();
      const preload = new PreloadBackend(remote);
      await preload.init();

      // Write 10 files
      for (let i = 0; i < 10; i++) {
        preload.writePage(`/batch1-${i}`, 0, makePage(i));
        preload.writeMeta(`/batch1-${i}`, baseMeta);
      }

      remote.onBefore("syncAll", () => {
        // Write 20 more files during the flush
        for (let i = 0; i < 20; i++) {
          preload.writePage(`/batch2-${i}`, 0, makePage(0x80 + i));
          preload.writeMeta(`/batch2-${i}`, baseMeta);
        }
      });

      await preload.flush();
      remote.clearHooks();

      expect(preload.isDirty).toBe(true);
      expect(preload.dirtyPageCount).toBe(20);
      expect(preload.dirtyMetaCount).toBe(20);

      await preload.flush();
      expect(preload.isDirty).toBe(false);

      const fresh = new PreloadBackend(remote);
      await fresh.init();
      for (let i = 0; i < 10; i++) {
        expect(fresh.readPage(`/batch1-${i}`, 0)![0]).toBe(i);
      }
      for (let i = 0; i < 20; i++) {
        expect(fresh.readPage(`/batch2-${i}`, 0)![0]).toBe(0x80 + i);
      }
      expect(fresh.listFiles().length).toBe(30);
    });

    it("mixed write/delete/truncate during multi-phase flush", async () => {
      const remote = new HookableBackend();
      const preload = new PreloadBackend(remote);
      await preload.init();

      // Create initial state: 5 files
      for (let i = 0; i < 5; i++) {
        for (let p = 0; p < 3; p++) {
          preload.writePage(`/f${i}`, p, makePage(i * 16 + p));
        }
        preload.writeMeta(`/f${i}`, { ...baseMeta, size: PAGE_SIZE * 3 });
      }
      await preload.flush();

      // Trigger complex path: delete /f0, modify /f1
      preload.deleteFile("/f0");
      preload.deleteMeta("/f0");
      preload.writePage("/f1", 0, makePage(0xf1));

      let hookPhase = 0;
      remote.onBefore("syncAll", () => {
        hookPhase++;
        if (hookPhase === 1) {
          // During early syncAll: truncate /f2, write new /f5
          preload.deletePagesFrom("/f2", 1);
          preload.writeMeta("/f2", { ...baseMeta, size: PAGE_SIZE });
          preload.writePage("/f5", 0, makePage(0xf5));
          preload.writeMeta("/f5", baseMeta);
        }
      });

      remote.onBefore("deleteFiles", () => {
        // During deleteFiles: modify /f3
        preload.writePage("/f3", 0, makePage(0xf3));
      });

      await preload.flush();
      remote.clearHooks();

      // Multiple concurrent mutations should be pending
      expect(preload.isDirty).toBe(true);
      preload.assertInvariants();

      await preload.flush();
      preload.assertInvariants();
      expect(preload.isDirty).toBe(false);

      const fresh = new PreloadBackend(remote);
      await fresh.init();

      // /f0 deleted
      expect(fresh.readPage("/f0", 0)).toBeNull();
      expect(fresh.readMeta("/f0")).toBeNull();

      // /f1 modified
      expect(fresh.readPage("/f1", 0)![0]).toBe(0xf1);

      // /f2 truncated to 1 page
      expect(fresh.readPage("/f2", 0)![0]).toBe(2 * 16);
      expect(fresh.readPage("/f2", 1)).toBeNull();
      expect(fresh.readMeta("/f2")!.size).toBe(PAGE_SIZE);

      // /f3 modified
      expect(fresh.readPage("/f3", 0)![0]).toBe(0xf3);

      // /f4 unchanged
      expect(fresh.readPage("/f4", 0)![0]).toBe(4 * 16);

      // /f5 created during flush
      expect(fresh.readPage("/f5", 0)![0]).toBe(0xf5);
    });
  });
});
