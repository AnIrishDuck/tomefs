/**
 * Adversarial tests: PreloadBackend flush() partial failure and retry.
 *
 * The flush() method has 6 ordered phases, each of which clears its own
 * tracking state on success. When an intermediate phase fails, earlier
 * phases have already committed changes to the remote AND cleared their
 * tracking. The retry must produce correct results despite this partial
 * state.
 *
 * Phases:
 *   1. deletePagesFrom (truncations)  → truncations.clear()
 *   2. syncAll (early batch)          → (no clear, dirty stays)
 *   3. deleteFiles                    → deletedFiles.clear()
 *   4. deleteMetas                    → deletedMeta.clear()
 *   5. syncAll (late batch)           → (no clear, dirty stays)
 *   6. clear dirtyPages/dirtyMeta for flushed keys
 *
 * Key interactions tested:
 * - Truncations applied then syncAll fails: retry doesn't re-truncate,
 *   dirty pages at truncation points are correctly re-applied
 * - DeleteFiles succeeds then deleteMetas fails: retry handles the
 *   reclassification of late-batch pages as early-batch
 * - Delete-then-recreate: phase 3 failure leaves deletedFiles intact,
 *   retry correctly defers recreated data to late batch
 * - Phase 5 failure: late-batch data (delete-then-recreate) is preserved
 *   for retry
 *
 * Ethos §9 (adversarial — target partial failure seams)
 * Ethos §10 (graceful degradation — PreloadBackend is the no-SAB path)
 */

import { describe, it, expect } from "vitest";
import { PreloadBackend } from "../../src/preload-backend.js";
import { MemoryBackend } from "../../src/memory-backend.js";
import { PAGE_SIZE } from "../../src/types.js";
import type { StorageBackend } from "../../src/storage-backend.js";
import type { FileMeta } from "../../src/types.js";

// ---------------------------------------------------------------
// Phase-targeted failing backend
// ---------------------------------------------------------------

type FailPhase =
  | "deletePagesFrom"
  | "syncAll"
  | "deleteFiles"
  | "deleteMetas";

/**
 * StorageBackend that can inject failures at specific flush phases.
 * Tracks which operations have been called to verify retry behavior.
 */
class PhaseFailingBackend implements StorageBackend {
  private inner = new MemoryBackend();
  private failPhase: FailPhase | null = null;
  private failCount = 0;
  private syncAllCallCount = 0;
  private failOnNthSyncAll = 0;

  /** Arm: next call to the specified phase will throw. */
  failNext(phase: FailPhase, count = 1): void {
    this.failPhase = phase;
    this.failCount = count;
  }

  /** Arm: fail on the Nth syncAll call (1-indexed). */
  failSyncAllOnCall(n: number): void {
    this.failOnNthSyncAll = n;
    this.syncAllCallCount = 0;
  }

  disarm(): void {
    this.failPhase = null;
    this.failCount = 0;
    this.failOnNthSyncAll = 0;
  }

  resetSyncAllCounter(): void {
    this.syncAllCallCount = 0;
  }

  async readPage(path: string, pageIndex: number) {
    return this.inner.readPage(path, pageIndex);
  }
  async readPages(path: string, pageIndices: number[]) {
    return this.inner.readPages(path, pageIndices);
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
    if (this.failPhase === "deleteFiles" && this.failCount > 0) {
      this.failCount--;
      if (this.failCount === 0) this.failPhase = null;
      throw new Error("injected deleteFiles failure");
    }
    return this.inner.deleteFiles(paths);
  }
  async deletePagesFrom(path: string, fromPageIndex: number) {
    if (this.failPhase === "deletePagesFrom" && this.failCount > 0) {
      this.failCount--;
      if (this.failCount === 0) this.failPhase = null;
      throw new Error("injected deletePagesFrom failure");
    }
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
    if (this.failPhase === "deleteMetas" && this.failCount > 0) {
      this.failCount--;
      if (this.failCount === 0) this.failPhase = null;
      throw new Error("injected deleteMetas failure");
    }
    return this.inner.deleteMetas(paths);
  }
  async listFiles() {
    return this.inner.listFiles();
  }
  async syncAll(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
    metas: Array<{ path: string; meta: FileMeta }>,
  ) {
    this.syncAllCallCount++;
    if (this.failPhase === "syncAll" && this.failCount > 0) {
      this.failCount--;
      if (this.failCount === 0) this.failPhase = null;
      throw new Error("injected syncAll failure");
    }
    if (
      this.failOnNthSyncAll > 0 &&
      this.syncAllCallCount === this.failOnNthSyncAll
    ) {
      this.failOnNthSyncAll = 0;
      throw new Error("injected syncAll failure (Nth call)");
    }
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

describe("PreloadBackend flush() partial failure recovery", () => {
  describe("phase 2 failure (early syncAll) after phase 1 (truncations)", () => {
    it("@fast truncation applied, syncAll fails: retry writes pages without re-truncating", async () => {
      const remote = new PhaseFailingBackend();
      const preload = new PreloadBackend(remote);
      await preload.init();

      // Write 4 pages to /file
      for (let i = 0; i < 4; i++) {
        preload.writePage("/file", i, makePage(0x10 + i));
      }
      preload.writeMeta("/file", { ...baseMeta, size: PAGE_SIZE * 4 });

      // Flush to establish baseline in remote
      await preload.flush();

      // Truncate to 2 pages, then write a new page at index 2
      preload.deletePagesFrom("/file", 2);
      preload.writePage("/file", 2, makePage(0xaa));
      preload.writeMeta("/file", { ...baseMeta, size: PAGE_SIZE * 3 });

      // Fail on syncAll (phase 2) — truncation (phase 1) already applied
      remote.failNext("syncAll");
      await expect(preload.flush()).rejects.toThrow("injected syncAll failure");

      // Verify: truncation was applied to remote (pages 2,3 gone)
      const p2before = await remote.readPage("/file", 2);
      expect(p2before).toBeNull();
      const p3 = await remote.readPage("/file", 3);
      expect(p3).toBeNull();

      // Dirty tracking preserved for retry
      expect(preload.dirtyPageCount).toBeGreaterThan(0);

      // Retry succeeds: new page 2 is written
      await preload.flush();
      expect(preload.isDirty).toBe(false);

      // Verify final state via re-init
      const fresh = new PreloadBackend(remote);
      await fresh.init();
      expect(fresh.readPage("/file", 0)![0]).toBe(0x10);
      expect(fresh.readPage("/file", 1)![0]).toBe(0x11);
      expect(fresh.readPage("/file", 2)![0]).toBe(0xaa);
      expect(fresh.readPage("/file", 3)).toBeNull();
    });

    it("multiple truncations: all applied before syncAll failure, none re-applied on retry", async () => {
      const remote = new PhaseFailingBackend();
      const preload = new PreloadBackend(remote);
      await preload.init();

      // Set up two multi-page files
      for (let i = 0; i < 5; i++) {
        preload.writePage("/a", i, makePage(0xa0 + i));
        preload.writePage("/b", i, makePage(0xb0 + i));
      }
      preload.writeMeta("/a", { ...baseMeta, size: PAGE_SIZE * 5 });
      preload.writeMeta("/b", { ...baseMeta, size: PAGE_SIZE * 5 });
      await preload.flush();

      // Truncate both files
      preload.deletePagesFrom("/a", 2);
      preload.deletePagesFrom("/b", 3);
      preload.writeMeta("/a", { ...baseMeta, size: PAGE_SIZE * 2 });
      preload.writeMeta("/b", { ...baseMeta, size: PAGE_SIZE * 3 });

      // Fail syncAll
      remote.failNext("syncAll");
      await expect(preload.flush()).rejects.toThrow("injected syncAll failure");

      // Both truncations applied to remote
      expect(await remote.maxPageIndex("/a")).toBe(1);
      expect(await remote.maxPageIndex("/b")).toBe(2);

      // Retry
      await preload.flush();
      expect(preload.isDirty).toBe(false);

      // Verify metadata reached remote
      const fresh = new PreloadBackend(remote);
      await fresh.init();
      expect(fresh.readMeta("/a")!.size).toBe(PAGE_SIZE * 2);
      expect(fresh.readMeta("/b")!.size).toBe(PAGE_SIZE * 3);
    });
  });

  describe("phase 3 failure (deleteFiles) after phase 2 (early syncAll)", () => {
    it("@fast early batch written, deleteFiles fails: retry re-applies deletion", async () => {
      const remote = new PhaseFailingBackend();
      const preload = new PreloadBackend(remote);
      await preload.init();

      // Write files: /keep (early batch) and /delete (to be deleted)
      preload.writePage("/keep", 0, makePage(0x11));
      preload.writeMeta("/keep", baseMeta);
      preload.writePage("/delete", 0, makePage(0x22));
      preload.writeMeta("/delete", baseMeta);
      await preload.flush();

      // Modify /keep (dirty, goes to early batch) and delete /delete
      preload.writePage("/keep", 0, makePage(0x33));
      preload.writeMeta("/keep", { ...baseMeta, mtime: 2000 });
      preload.deleteFile("/delete");
      preload.deleteMeta("/delete");

      // Fail deleteFiles (phase 3) — early syncAll (phase 2) already wrote /keep
      remote.failNext("deleteFiles");
      await expect(preload.flush()).rejects.toThrow(
        "injected deleteFiles failure",
      );

      // /keep's data was written (phase 2 succeeded)
      const keepPage = await remote.readPage("/keep", 0);
      expect(keepPage![0]).toBe(0x33);

      // /delete still exists in remote (phase 3 failed)
      const deletePage = await remote.readPage("/delete", 0);
      expect(deletePage).not.toBeNull();

      // Retry succeeds
      await preload.flush();
      expect(preload.isDirty).toBe(false);

      // /delete gone from remote
      const fresh = new PreloadBackend(remote);
      await fresh.init();
      expect(fresh.listFiles()).toEqual(["/keep"]);
      expect(fresh.readPage("/keep", 0)![0]).toBe(0x33);
    });
  });

  describe("phase 4 failure (deleteMetas) after phase 3 (deleteFiles)", () => {
    it("@fast pages deleted, metadata delete fails: retry cleans up metadata", async () => {
      const remote = new PhaseFailingBackend();
      const preload = new PreloadBackend(remote);
      await preload.init();

      // Create a file and flush
      preload.writePage("/victim", 0, makePage(0x55));
      preload.writeMeta("/victim", baseMeta);
      await preload.flush();

      // Delete the file (both pages and meta tracked)
      preload.deleteFile("/victim");
      preload.deleteMeta("/victim");

      // Fail deleteMetas (phase 4) — deleteFiles (phase 3) already removed pages
      remote.failNext("deleteMetas");
      await expect(preload.flush()).rejects.toThrow(
        "injected deleteMetas failure",
      );

      // Pages gone (phase 3 succeeded)
      expect(await remote.readPage("/victim", 0)).toBeNull();
      // Metadata still present (phase 4 failed)
      expect(await remote.readMeta("/victim")).not.toBeNull();

      // Retry removes the orphaned metadata
      await preload.flush();
      expect(preload.isDirty).toBe(false);

      const fresh = new PreloadBackend(remote);
      await fresh.init();
      expect(fresh.listFiles()).toEqual([]);
    });

    it("deleteFiles clears then deleteMetas fails: late-batch reclassification on retry", async () => {
      const remote = new PhaseFailingBackend();
      const preload = new PreloadBackend(remote);
      await preload.init();

      // Create /old (will be delete-then-recreate) and /orphan (will just be deleted)
      preload.writePage("/old", 0, makePage(0xaa));
      preload.writeMeta("/old", baseMeta);
      preload.writePage("/orphan", 0, makePage(0x55));
      preload.writeMeta("/orphan", baseMeta);
      await preload.flush();

      // Delete /old then recreate at same path (delete-then-recreate)
      preload.deleteFile("/old");
      preload.writePage("/old", 0, makePage(0xbb));
      preload.writeMeta("/old", { ...baseMeta, mtime: 9999 });

      // Delete /orphan entirely (meta deletion stays in deletedMeta)
      preload.deleteFile("/orphan");
      preload.deleteMeta("/orphan");

      // Fail deleteMetas — deleteFiles succeeds (clears deletedFiles)
      remote.failNext("deleteMetas");
      await expect(preload.flush()).rejects.toThrow(
        "injected deleteMetas failure",
      );

      // deleteFiles was cleared (phase 3 succeeded): both /old and /orphan pages gone
      expect(await remote.readPage("/old", 0)).toBeNull();
      expect(await remote.readPage("/orphan", 0)).toBeNull();
      // /orphan's metadata still present (phase 4 failed)
      expect(await remote.readMeta("/orphan")).not.toBeNull();

      // On retry: /old is no longer in deletedFiles → goes to early batch.
      // /orphan's metadata deletion retried in phase 4.
      await preload.flush();
      expect(preload.isDirty).toBe(false);

      const fresh = new PreloadBackend(remote);
      await fresh.init();
      expect(fresh.readPage("/old", 0)![0]).toBe(0xbb);
      expect(fresh.readMeta("/old")!.mtime).toBe(9999);
      expect(fresh.readMeta("/orphan")).toBeNull();
      expect(fresh.listFiles().sort()).toEqual(["/old"]);
    });
  });

  describe("phase 5 failure (late syncAll) after delete-then-recreate", () => {
    it("@fast delete-then-recreate: phases 3-4 succeed, late syncAll fails, retry works", async () => {
      const remote = new PhaseFailingBackend();
      const preload = new PreloadBackend(remote);
      await preload.init();

      // Create /file with original data, plus /other (early batch data)
      preload.writePage("/file", 0, makePage(0x11));
      preload.writeMeta("/file", baseMeta);
      preload.writePage("/other", 0, makePage(0x77));
      preload.writeMeta("/other", baseMeta);
      await preload.flush();

      // Delete and recreate /file at same path
      preload.deleteFile("/file");
      preload.writePage("/file", 0, makePage(0x99));
      preload.writeMeta("/file", { ...baseMeta, size: PAGE_SIZE, mtime: 5000 });

      // Also modify /other so early batch is non-empty (ensuring two syncAll calls)
      preload.writePage("/other", 0, makePage(0x88));
      preload.writeMeta("/other", { ...baseMeta, mtime: 6000 });

      // Fail the second syncAll call (the late batch, phase 5)
      // Phase 2 (early syncAll) writes /other since it's not in deletedFiles
      remote.failSyncAllOnCall(2);
      await expect(preload.flush()).rejects.toThrow(
        "injected syncAll failure (Nth call)",
      );

      // /other was written in early batch (phase 2 succeeded)
      expect((await remote.readPage("/other", 0))![0]).toBe(0x88);

      // Pages for /file were deleted (phase 3 succeeded)
      expect(await remote.readPage("/file", 0)).toBeNull();

      // Retry: now deletedFiles is clear, so /file goes to early batch
      await preload.flush();
      expect(preload.isDirty).toBe(false);

      const fresh = new PreloadBackend(remote);
      await fresh.init();
      expect(fresh.readPage("/file", 0)![0]).toBe(0x99);
      expect(fresh.readMeta("/file")!.mtime).toBe(5000);
    });

    it("multiple delete-then-recreate paths with late syncAll failure", async () => {
      const remote = new PhaseFailingBackend();
      const preload = new PreloadBackend(remote);
      await preload.init();

      // Create /x, /y, and /z
      preload.writePage("/x", 0, makePage(0x10));
      preload.writePage("/y", 0, makePage(0x20));
      preload.writePage("/z", 0, makePage(0x30));
      preload.writeMeta("/x", baseMeta);
      preload.writeMeta("/y", baseMeta);
      preload.writeMeta("/z", baseMeta);
      await preload.flush();

      // Delete and recreate /x and /y
      preload.deleteFile("/x");
      preload.writePage("/x", 0, makePage(0x11));
      preload.writeMeta("/x", { ...baseMeta, mtime: 1111 });

      preload.deleteFile("/y");
      preload.writePage("/y", 0, makePage(0x22));
      preload.writeMeta("/y", { ...baseMeta, mtime: 2222 });

      // Modify /z (NOT delete-then-recreate — goes to early batch)
      preload.writePage("/z", 0, makePage(0x33));
      preload.writeMeta("/z", { ...baseMeta, mtime: 3333 });

      // Fail late syncAll (phase 5) — /z is in early batch so two syncAll calls
      remote.failSyncAllOnCall(2);
      await expect(preload.flush()).rejects.toThrow(
        "injected syncAll failure (Nth call)",
      );

      // /z was written in early batch (phase 2 succeeded)
      expect((await remote.readPage("/z", 0))![0]).toBe(0x33);

      // /x and /y pages deleted (phase 3), but new data not yet written
      expect(await remote.readPage("/x", 0)).toBeNull();
      expect(await remote.readPage("/y", 0)).toBeNull();

      // Retry
      await preload.flush();
      expect(preload.isDirty).toBe(false);

      const fresh = new PreloadBackend(remote);
      await fresh.init();
      expect(fresh.readPage("/x", 0)![0]).toBe(0x11);
      expect(fresh.readPage("/y", 0)![0]).toBe(0x22);
      expect(fresh.readPage("/z", 0)![0]).toBe(0x33);
    });
  });

  describe("truncation + re-extension + failure + retry", () => {
    it("@fast truncate then extend past truncation point: flush fails, retry succeeds", async () => {
      const remote = new PhaseFailingBackend();
      const preload = new PreloadBackend(remote);
      await preload.init();

      // Create 5-page file
      for (let i = 0; i < 5; i++) {
        preload.writePage("/data", i, makePage(0x50 + i));
      }
      preload.writeMeta("/data", { ...baseMeta, size: PAGE_SIZE * 5 });
      await preload.flush();

      // Truncate to 1 page, then extend with new data at pages 1-3
      preload.deletePagesFrom("/data", 1);
      preload.writePage("/data", 1, makePage(0xa1));
      preload.writePage("/data", 2, makePage(0xa2));
      preload.writePage("/data", 3, makePage(0xa3));
      preload.writeMeta("/data", { ...baseMeta, size: PAGE_SIZE * 4 });

      // Phase 1 (truncation) succeeds, phase 2 (syncAll) fails
      remote.failNext("syncAll");
      await expect(preload.flush()).rejects.toThrow("injected syncAll failure");

      // Remote state: pages 1-4 deleted by truncation, but new 1-3 not yet written
      expect(await remote.readPage("/data", 0)).not.toBeNull();
      expect(await remote.readPage("/data", 1)).toBeNull();
      expect(await remote.readPage("/data", 4)).toBeNull();

      // Retry: truncations already cleared (no re-truncation), pages written
      await preload.flush();
      expect(preload.isDirty).toBe(false);

      const fresh = new PreloadBackend(remote);
      await fresh.init();
      expect(fresh.readPage("/data", 0)![0]).toBe(0x50);
      expect(fresh.readPage("/data", 1)![0]).toBe(0xa1);
      expect(fresh.readPage("/data", 2)![0]).toBe(0xa2);
      expect(fresh.readPage("/data", 3)![0]).toBe(0xa3);
      expect(fresh.readPage("/data", 4)).toBeNull();
    });
  });

  describe("writes during failed flush are preserved", () => {
    it("@fast new writes added after flush snapshot are included in retry", async () => {
      const remote = new PhaseFailingBackend();
      const preload = new PreloadBackend(remote);
      await preload.init();

      // Initial write
      preload.writePage("/f", 0, makePage(0x11));
      preload.writeMeta("/f", baseMeta);

      // Fail flush
      remote.failNext("syncAll");
      await expect(preload.flush()).rejects.toThrow("injected syncAll failure");

      // Write more data while original is still pending
      preload.writePage("/g", 0, makePage(0x22));
      preload.writeMeta("/g", { ...baseMeta, mtime: 3000 });

      // Retry: both /f and /g should be flushed
      await preload.flush();
      expect(preload.isDirty).toBe(false);

      const fresh = new PreloadBackend(remote);
      await fresh.init();
      expect(fresh.readPage("/f", 0)![0]).toBe(0x11);
      expect(fresh.readPage("/g", 0)![0]).toBe(0x22);
    });

    it("overwrite of pending dirty page during failed flush uses latest data", async () => {
      const remote = new PhaseFailingBackend();
      const preload = new PreloadBackend(remote);
      await preload.init();

      // Write initial data
      preload.writePage("/f", 0, makePage(0x11));
      preload.writeMeta("/f", baseMeta);

      // Fail flush
      remote.failNext("syncAll");
      await expect(preload.flush()).rejects.toThrow("injected syncAll failure");

      // Overwrite with new data
      preload.writePage("/f", 0, makePage(0x99));

      // Retry: should write 0x99, not 0x11
      await preload.flush();

      const fresh = new PreloadBackend(remote);
      await fresh.init();
      expect(fresh.readPage("/f", 0)![0]).toBe(0x99);
    });
  });

  describe("complex multi-file interactions", () => {
    it("mixed operations across files with phase 2 failure", async () => {
      const remote = new PhaseFailingBackend();
      const preload = new PreloadBackend(remote);
      await preload.init();

      // Set up initial state
      preload.writePage("/keep", 0, makePage(0x01));
      preload.writePage("/rename-src", 0, makePage(0x02));
      preload.writePage("/trunc", 0, makePage(0x03));
      preload.writePage("/trunc", 1, makePage(0x04));
      preload.writeMeta("/keep", baseMeta);
      preload.writeMeta("/rename-src", baseMeta);
      preload.writeMeta("/trunc", { ...baseMeta, size: PAGE_SIZE * 2 });
      await preload.flush();

      // Mixed operations
      preload.writePage("/keep", 0, makePage(0x11)); // modify
      preload.writeMeta("/keep", { ...baseMeta, mtime: 2000 });
      preload.renameFile("/rename-src", "/rename-dst"); // rename pages
      preload.deleteMeta("/rename-src"); // remove old metadata
      preload.writeMeta("/rename-dst", baseMeta); // add new metadata
      preload.deletePagesFrom("/trunc", 1); // truncate
      preload.writeMeta("/trunc", { ...baseMeta, size: PAGE_SIZE });
      preload.writePage("/new", 0, makePage(0x44)); // new file
      preload.writeMeta("/new", baseMeta);

      // Fail early syncAll (after truncation applied)
      remote.failNext("syncAll");
      await expect(preload.flush()).rejects.toThrow("injected syncAll failure");

      // Truncation of /trunc was applied to remote
      expect(await remote.maxPageIndex("/trunc")).toBe(0);

      // Retry
      await preload.flush();
      expect(preload.isDirty).toBe(false);

      const fresh = new PreloadBackend(remote);
      await fresh.init();
      expect(fresh.readPage("/keep", 0)![0]).toBe(0x11);
      expect(fresh.readPage("/rename-src", 0)).toBeNull();
      expect(fresh.readPage("/rename-dst", 0)![0]).toBe(0x02);
      expect(fresh.maxPageIndex("/trunc")).toBe(0);
      expect(fresh.readPage("/new", 0)![0]).toBe(0x44);
    });

    it("double failure then success: all data eventually reaches remote", async () => {
      const remote = new PhaseFailingBackend();
      const preload = new PreloadBackend(remote);
      await preload.init();

      preload.writePage("/a", 0, makePage(0xaa));
      preload.writeMeta("/a", baseMeta);

      // First failure
      remote.failNext("syncAll");
      await expect(preload.flush()).rejects.toThrow("injected syncAll failure");
      expect(preload.dirtyPageCount).toBe(1);

      // Add more data
      preload.writePage("/b", 0, makePage(0xbb));
      preload.writeMeta("/b", baseMeta);

      // Second failure
      remote.failNext("syncAll");
      await expect(preload.flush()).rejects.toThrow("injected syncAll failure");
      expect(preload.dirtyPageCount).toBe(2);

      // Add even more data
      preload.writePage("/c", 0, makePage(0xcc));
      preload.writeMeta("/c", baseMeta);

      // Success
      await preload.flush();
      expect(preload.isDirty).toBe(false);

      const fresh = new PreloadBackend(remote);
      await fresh.init();
      expect(fresh.readPage("/a", 0)![0]).toBe(0xaa);
      expect(fresh.readPage("/b", 0)![0]).toBe(0xbb);
      expect(fresh.readPage("/c", 0)![0]).toBe(0xcc);
    });
  });

  describe("phase 1 failure (truncation)", () => {
    it("@fast truncation fails: all state preserved for retry", async () => {
      const remote = new PhaseFailingBackend();
      const preload = new PreloadBackend(remote);
      await preload.init();

      // Create multi-page file
      for (let i = 0; i < 4; i++) {
        preload.writePage("/data", i, makePage(0x40 + i));
      }
      preload.writeMeta("/data", { ...baseMeta, size: PAGE_SIZE * 4 });
      await preload.flush();

      // Truncate and add new data
      preload.deletePagesFrom("/data", 2);
      preload.writePage("/data", 2, makePage(0xee));
      preload.writeMeta("/data", { ...baseMeta, size: PAGE_SIZE * 3 });

      // Fail truncation (phase 1)
      remote.failNext("deletePagesFrom");
      await expect(preload.flush()).rejects.toThrow(
        "injected deletePagesFrom failure",
      );

      // Nothing should have been applied to remote
      // (truncation is the first phase and it failed)
      expect(await remote.maxPageIndex("/data")).toBe(3); // still has 4 pages

      // Retry succeeds
      await preload.flush();
      expect(preload.isDirty).toBe(false);

      const fresh = new PreloadBackend(remote);
      await fresh.init();
      expect(fresh.readPage("/data", 0)![0]).toBe(0x40);
      expect(fresh.readPage("/data", 1)![0]).toBe(0x41);
      expect(fresh.readPage("/data", 2)![0]).toBe(0xee);
      expect(fresh.readPage("/data", 3)).toBeNull();
    });
  });

  describe("idempotency: successful flush then immediate retry is no-op", () => {
    it("@fast double flush after mutations: second is no-op", async () => {
      const remote = new PhaseFailingBackend();
      const preload = new PreloadBackend(remote);
      await preload.init();

      preload.writePage("/x", 0, makePage(0x42));
      preload.writeMeta("/x", baseMeta);

      await preload.flush();
      expect(preload.isDirty).toBe(false);

      // Second flush: no dirty state, no-op
      await preload.flush();
      expect(preload.isDirty).toBe(false);

      const fresh = new PreloadBackend(remote);
      await fresh.init();
      expect(fresh.readPage("/x", 0)![0]).toBe(0x42);
    });
  });
});
