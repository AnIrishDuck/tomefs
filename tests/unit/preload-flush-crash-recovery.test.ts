/**
 * Crash-at-each-step tests for PreloadBackend.flush().
 *
 * Validates that when flush() fails partway through its 6-step sequence,
 * dirty tracking is preserved and a subsequent retry produces the correct
 * final state. This exercises the recovery guarantee: every mutating
 * backend operation in flush() is idempotent, so replaying the entire
 * flush after a partial failure is safe.
 *
 * Flush steps (from preload-backend.ts):
 *   1. Apply truncations (deletePagesFrom)
 *   2. Early syncAll (pages + metadata for non-deleted paths)
 *   3. Batch-delete files (deleteFiles)
 *   4. Batch-delete metadata (deleteMetas)
 *   5. Late syncAll (pages + metadata for delete-then-recreate paths)
 *   6. Clear dirty tracking (only on full success)
 *
 * Ethos §9 (adversarial), §10 (graceful degradation).
 */
import { describe, it, expect, beforeEach } from "vitest";
import { MemoryBackend } from "../../src/memory-backend.js";
import { PreloadBackend } from "../../src/preload-backend.js";
import { PAGE_SIZE } from "../../src/types.js";
import type { StorageBackend } from "../../src/storage-backend.js";
import type { FileMeta } from "../../src/types.js";

/** Helper: create a PAGE_SIZE buffer with a marker byte at index 0. */
function pageWith(marker: number): Uint8Array {
  const buf = new Uint8Array(PAGE_SIZE);
  buf[0] = marker;
  return buf;
}

/** Helper: standard metadata for a file of a given size. */
function metaOf(size: number, mtime = 1000): FileMeta {
  return { size, mode: 0o100644, ctime: 1000, mtime };
}

/**
 * Backend wrapper with controllable crash injection.
 *
 * When armed, throws SIMULATED_CRASH after the Nth mutating operation
 * (0-indexed: arm(0) crashes on the first op). Operations that throw
 * do NOT modify the inner backend (per-op atomicity, modeling IDB
 * transaction rollback semantics).
 *
 * When disarmed (default), all operations pass through to the inner backend.
 */
class ControllableBackend implements StorageBackend {
  readonly inner: MemoryBackend;
  private opsRemaining = Infinity;

  constructor(inner: MemoryBackend) {
    this.inner = inner;
  }

  /** Arm: crash after the next `n` mutating operations succeed.
   *  arm(0) crashes on the first mutating op. */
  arm(n: number): void {
    this.opsRemaining = n;
  }

  /** Disarm: stop crashing. */
  disarm(): void {
    this.opsRemaining = Infinity;
  }

  private tick(): void {
    if (this.opsRemaining <= 0) {
      throw new Error("SIMULATED_CRASH");
    }
    this.opsRemaining--;
  }

  // --- Read-only operations (never crash) ---

  async readPage(path: string, pageIndex: number) {
    return this.inner.readPage(path, pageIndex);
  }
  async readPages(path: string, pageIndices: number[]) {
    return this.inner.readPages(path, pageIndices);
  }
  async readMeta(path: string) {
    return this.inner.readMeta(path);
  }
  async readMetas(paths: string[]) {
    return this.inner.readMetas(paths);
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
  async listFiles() {
    return this.inner.listFiles();
  }

  // --- Mutating operations (crash when armed) ---

  async writePage(path: string, pageIndex: number, data: Uint8Array) {
    this.tick();
    return this.inner.writePage(path, pageIndex, data);
  }
  async writePages(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
  ) {
    this.tick();
    return this.inner.writePages(pages);
  }
  async deleteFile(path: string) {
    this.tick();
    return this.inner.deleteFile(path);
  }
  async deleteFiles(paths: string[]) {
    this.tick();
    return this.inner.deleteFiles(paths);
  }
  async deletePagesFrom(path: string, fromPageIndex: number) {
    this.tick();
    return this.inner.deletePagesFrom(path, fromPageIndex);
  }
  async renameFile(oldPath: string, newPath: string) {
    this.tick();
    return this.inner.renameFile(oldPath, newPath);
  }
  async writeMeta(path: string, meta: FileMeta) {
    this.tick();
    return this.inner.writeMeta(path, meta);
  }
  async writeMetas(entries: Array<{ path: string; meta: FileMeta }>) {
    this.tick();
    return this.inner.writeMetas(entries);
  }
  async deleteMeta(path: string) {
    this.tick();
    return this.inner.deleteMeta(path);
  }
  async deleteMetas(paths: string[]) {
    this.tick();
    return this.inner.deleteMetas(paths);
  }
  async syncAll(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
    metas: Array<{ path: string; meta: FileMeta }>,
  ) {
    this.tick();
    return this.inner.syncAll(pages, metas);
  }
}

describe("PreloadBackend flush crash recovery", () => {
  let inner: MemoryBackend;
  let ctrl: ControllableBackend;

  beforeEach(() => {
    inner = new MemoryBackend();
    ctrl = new ControllableBackend(inner);
  });

  // ---------------------------------------------------------------
  // Scenario 1: Simple write (no deletes, no truncations)
  // flush() → step 2 only (early syncAll)
  // ---------------------------------------------------------------

  describe("simple write (early syncAll only)", () => {
    it("@fast crash at step 2 (syncAll) preserves dirty tracking and retry succeeds", async () => {
      const backend = new PreloadBackend(ctrl);
      await backend.init();

      backend.writePage("/a", 0, pageWith(0x42));
      backend.writeMeta("/a", metaOf(PAGE_SIZE));

      // Crash at first mutating op (step 2: early syncAll)
      ctrl.arm(0);
      await expect(backend.flush()).rejects.toThrow("SIMULATED_CRASH");

      // Dirty tracking preserved
      expect(backend.isDirty).toBe(true);
      expect(backend.dirtyPageCount).toBeGreaterThan(0);
      expect(backend.dirtyMetaCount).toBeGreaterThan(0);

      // Backend unchanged (syncAll was atomic — it threw before committing)
      expect(await inner.readPage("/a", 0)).toBeNull();
      expect(await inner.readMeta("/a")).toBeNull();

      // Retry succeeds
      ctrl.disarm();
      await backend.flush();

      expect(backend.isDirty).toBe(false);
      const page = await inner.readPage("/a", 0);
      expect(page).not.toBeNull();
      expect(page![0]).toBe(0x42);
      expect(await inner.readMeta("/a")).not.toBeNull();
    });
  });

  // ---------------------------------------------------------------
  // Scenario 2: Truncation + write
  // flush() → step 1 (deletePagesFrom) + step 2 (early syncAll)
  // ---------------------------------------------------------------

  describe("truncation + write", () => {
    it("crash at step 1 (truncation) preserves all dirty state", async () => {
      // Seed: /a has 3 pages
      await inner.writePage("/a", 0, pageWith(0xaa));
      await inner.writePage("/a", 1, pageWith(0xbb));
      await inner.writePage("/a", 2, pageWith(0xcc));
      await inner.writeMeta("/a", metaOf(3 * PAGE_SIZE));

      const backend = new PreloadBackend(ctrl);
      await backend.init();

      // Truncate to 1 page and write new data
      backend.deletePagesFrom("/a", 1);
      backend.writePage("/a", 0, pageWith(0xff));
      backend.writeMeta("/a", metaOf(PAGE_SIZE, 2000));

      // Crash at step 1 (deletePagesFrom)
      ctrl.arm(0);
      await expect(backend.flush()).rejects.toThrow("SIMULATED_CRASH");

      // All dirty tracking preserved (truncations, pages, meta)
      expect(backend.isDirty).toBe(true);

      // Backend unchanged: old pages still present
      expect(await inner.countPages("/a")).toBe(3);

      // Retry succeeds
      ctrl.disarm();
      await backend.flush();

      expect(backend.isDirty).toBe(false);
      // Truncation applied: only page 0 remains
      expect(await inner.countPages("/a")).toBe(1);
      const page = await inner.readPage("/a", 0);
      expect(page![0]).toBe(0xff);
      expect((await inner.readMeta("/a"))!.mtime).toBe(2000);
    });

    it("crash at step 2 (syncAll after successful truncation) — retry still writes pages", async () => {
      // Seed: /a has 3 pages
      await inner.writePage("/a", 0, pageWith(0xaa));
      await inner.writePage("/a", 1, pageWith(0xbb));
      await inner.writePage("/a", 2, pageWith(0xcc));
      await inner.writeMeta("/a", metaOf(3 * PAGE_SIZE));

      const backend = new PreloadBackend(ctrl);
      await backend.init();

      // Truncate to 1 page and write new page 0
      backend.deletePagesFrom("/a", 1);
      backend.writePage("/a", 0, pageWith(0xff));
      backend.writeMeta("/a", metaOf(PAGE_SIZE, 2000));

      // arm(1): step 1 (deletePagesFrom) succeeds, step 2 (syncAll) crashes
      ctrl.arm(1);
      await expect(backend.flush()).rejects.toThrow("SIMULATED_CRASH");

      // Dirty tracking: pages and meta preserved, truncations cleared
      // (step 1 completed and truncations.clear() was reached)
      expect(backend.isDirty).toBe(true);
      expect(backend.dirtyPageCount).toBeGreaterThan(0);

      // Backend state after partial flush:
      // - Truncation committed: pages 1 and 2 deleted
      // - SyncAll failed: new page 0 NOT written, metadata NOT updated
      expect(await inner.countPages("/a")).toBe(1);
      expect((await inner.readPage("/a", 0))![0]).toBe(0xaa); // still old data
      expect((await inner.readMeta("/a"))!.size).toBe(3 * PAGE_SIZE); // still old meta

      // Retry: step 1 skipped (truncations cleared), step 2 writes page + meta
      ctrl.disarm();
      await backend.flush();

      expect(backend.isDirty).toBe(false);
      expect((await inner.readPage("/a", 0))![0]).toBe(0xff);
      expect((await inner.readMeta("/a"))!.mtime).toBe(2000);
    });
  });

  // ---------------------------------------------------------------
  // Scenario 3: Delete-then-recreate at same path
  // flush() → step 3 (deleteFiles) + step 5 (late syncAll)
  // (steps 1, 2, 4 skipped: no truncations, no early batch, no deletedMeta)
  // ---------------------------------------------------------------

  describe("delete-then-recreate at same path", () => {
    it("crash at step 3 (deleteFiles) preserves all state", async () => {
      // Seed: /a has old data
      await inner.writePage("/a", 0, pageWith(0xaa));
      await inner.writeMeta("/a", metaOf(PAGE_SIZE));

      const backend = new PreloadBackend(ctrl);
      await backend.init();

      // Delete old /a, recreate with new data
      backend.deleteFile("/a");
      backend.deleteMeta("/a");
      backend.writePage("/a", 0, pageWith(0xbb));
      backend.writeMeta("/a", metaOf(PAGE_SIZE, 2000));

      // Crash at step 3 (deleteFiles — first mutating op since no truncations/early batch)
      ctrl.arm(0);
      await expect(backend.flush()).rejects.toThrow("SIMULATED_CRASH");

      // Dirty tracking preserved
      expect(backend.isDirty).toBe(true);

      // Backend unchanged: old data still present (deleteFiles threw before committing)
      expect((await inner.readPage("/a", 0))![0]).toBe(0xaa);

      // Retry
      ctrl.disarm();
      await backend.flush();

      expect(backend.isDirty).toBe(false);
      expect((await inner.readPage("/a", 0))![0]).toBe(0xbb);
      expect((await inner.readMeta("/a"))!.mtime).toBe(2000);
    });

    it("crash at step 5 (late syncAll after successful delete) — retry reclassifies as early", async () => {
      // Seed: /a has old data
      await inner.writePage("/a", 0, pageWith(0xaa));
      await inner.writeMeta("/a", metaOf(PAGE_SIZE));

      const backend = new PreloadBackend(ctrl);
      await backend.init();

      // Delete old /a, recreate with new data
      backend.deleteFile("/a");
      backend.deleteMeta("/a");
      backend.writePage("/a", 0, pageWith(0xbb));
      backend.writeMeta("/a", metaOf(PAGE_SIZE, 2000));

      // arm(1): step 3 (deleteFiles) succeeds, step 5 (late syncAll) crashes
      ctrl.arm(1);
      await expect(backend.flush()).rejects.toThrow("SIMULATED_CRASH");

      // After crash: deletedFiles cleared (step 3 succeeded), dirty pages preserved
      expect(backend.isDirty).toBe(true);

      // Backend state: old /a PAGES deleted, but old METADATA still present.
      // deleteFiles only deletes pages, not metadata. The old metadata persists
      // because deletedMeta was cleared by writeMeta() (delete-then-recreate),
      // so step 4 (deleteMetas) was a no-op.
      expect(await inner.readPage("/a", 0)).toBeNull();
      expect(await inner.readMeta("/a")).not.toBeNull(); // old metadata still present

      // Retry: deletedFiles is empty, so late batch pages reclassify as early.
      // Step 2 (early syncAll) writes the new /a data.
      ctrl.disarm();
      await backend.flush();

      expect(backend.isDirty).toBe(false);
      expect((await inner.readPage("/a", 0))![0]).toBe(0xbb);
      expect((await inner.readMeta("/a"))!.mtime).toBe(2000);
    });
  });

  // ---------------------------------------------------------------
  // Scenario 4: Mixed — early writes + delete-then-recreate
  // flush() → step 2 (early syncAll) + step 3 (deleteFiles) + step 5 (late syncAll)
  // ---------------------------------------------------------------

  describe("mixed: early writes + delete-then-recreate", () => {
    it("crash at step 2 (early syncAll) — nothing committed, full retry", async () => {
      // Seed: /old has data
      await inner.writePage("/old", 0, pageWith(0xaa));
      await inner.writeMeta("/old", metaOf(PAGE_SIZE));

      const backend = new PreloadBackend(ctrl);
      await backend.init();

      // Write new file /fresh (early batch)
      backend.writePage("/fresh", 0, pageWith(0x11));
      backend.writeMeta("/fresh", metaOf(PAGE_SIZE));

      // Delete-then-recreate /old (late batch)
      backend.deleteFile("/old");
      backend.deleteMeta("/old");
      backend.writePage("/old", 0, pageWith(0xbb));
      backend.writeMeta("/old", metaOf(PAGE_SIZE, 2000));

      // Crash at step 2 (early syncAll for /fresh)
      ctrl.arm(0);
      await expect(backend.flush()).rejects.toThrow("SIMULATED_CRASH");

      expect(backend.isDirty).toBe(true);

      // Nothing committed to backend
      expect(await inner.readPage("/fresh", 0)).toBeNull();
      expect((await inner.readPage("/old", 0))![0]).toBe(0xaa); // old data intact

      // Retry: full flush succeeds
      ctrl.disarm();
      await backend.flush();

      expect(backend.isDirty).toBe(false);
      expect((await inner.readPage("/fresh", 0))![0]).toBe(0x11);
      expect((await inner.readPage("/old", 0))![0]).toBe(0xbb);
      expect((await inner.readMeta("/old"))!.mtime).toBe(2000);
    });

    it("crash at step 3 (deleteFiles after early syncAll) — early data committed", async () => {
      // Seed: /old has data
      await inner.writePage("/old", 0, pageWith(0xaa));
      await inner.writeMeta("/old", metaOf(PAGE_SIZE));

      const backend = new PreloadBackend(ctrl);
      await backend.init();

      // Write new file /fresh (early batch)
      backend.writePage("/fresh", 0, pageWith(0x11));
      backend.writeMeta("/fresh", metaOf(PAGE_SIZE));

      // Delete-then-recreate /old (late batch)
      backend.deleteFile("/old");
      backend.deleteMeta("/old");
      backend.writePage("/old", 0, pageWith(0xbb));
      backend.writeMeta("/old", metaOf(PAGE_SIZE, 2000));

      // arm(1): step 2 (early syncAll for /fresh) succeeds, step 3 crashes
      ctrl.arm(1);
      await expect(backend.flush()).rejects.toThrow("SIMULATED_CRASH");

      // Early data committed, delete not done, late data not written
      expect((await inner.readPage("/fresh", 0))![0]).toBe(0x11);
      expect((await inner.readPage("/old", 0))![0]).toBe(0xaa); // old data intact

      // Retry
      ctrl.disarm();
      await backend.flush();

      expect(backend.isDirty).toBe(false);
      expect((await inner.readPage("/fresh", 0))![0]).toBe(0x11);
      expect((await inner.readPage("/old", 0))![0]).toBe(0xbb);
    });

    it("crash at step 5 (late syncAll after delete) — early + delete committed", async () => {
      // Seed: /old has data
      await inner.writePage("/old", 0, pageWith(0xaa));
      await inner.writeMeta("/old", metaOf(PAGE_SIZE));

      const backend = new PreloadBackend(ctrl);
      await backend.init();

      // Write new file /fresh (early batch)
      backend.writePage("/fresh", 0, pageWith(0x11));
      backend.writeMeta("/fresh", metaOf(PAGE_SIZE));

      // Delete-then-recreate /old (late batch)
      backend.deleteFile("/old");
      backend.deleteMeta("/old");
      backend.writePage("/old", 0, pageWith(0xbb));
      backend.writeMeta("/old", metaOf(PAGE_SIZE, 2000));

      // arm(2): step 2 + step 3 succeed, step 5 (late syncAll) crashes
      ctrl.arm(2);
      await expect(backend.flush()).rejects.toThrow("SIMULATED_CRASH");

      // Early data committed, old /old PAGES deleted, new /old NOT written.
      // Old metadata persists (deleteMetas was empty — writeMeta cleared deletedMeta).
      expect((await inner.readPage("/fresh", 0))![0]).toBe(0x11);
      expect(await inner.readPage("/old", 0)).toBeNull(); // pages deleted
      expect(await inner.readMeta("/old")).not.toBeNull(); // old metadata still present

      // Retry: /old reclassified as early (deletedFiles cleared), written in step 2
      ctrl.disarm();
      await backend.flush();

      expect(backend.isDirty).toBe(false);
      expect((await inner.readPage("/old", 0))![0]).toBe(0xbb);
      expect((await inner.readMeta("/old"))!.mtime).toBe(2000);
    });
  });

  // ---------------------------------------------------------------
  // Scenario 5: Full scenario — truncation + early + delete + late
  // All 5 steps fire
  // ---------------------------------------------------------------

  describe("full scenario: truncation + early + delete-then-recreate", () => {
    it("@fast crash at each step boundary and recover", async () => {
      // This test exercises crash at every possible step, verifying
      // that retry always produces the correct final state.
      //
      // Setup:
      //   /trunc: 3 pages → truncate to 1 page + rewrite page 0 (early)
      //   /clean: new file, 1 page (early)
      //   /recycled: old data → delete + recreate (late)
      //   /removed: old data → delete (pure deletion, metadata only)

      async function seedAndInit(): Promise<{
        backend: PreloadBackend;
        ctrl: ControllableBackend;
        inner: MemoryBackend;
      }> {
        const inner = new MemoryBackend();
        await inner.writePage("/trunc", 0, pageWith(0x01));
        await inner.writePage("/trunc", 1, pageWith(0x02));
        await inner.writePage("/trunc", 2, pageWith(0x03));
        await inner.writeMeta("/trunc", metaOf(3 * PAGE_SIZE));

        await inner.writePage("/recycled", 0, pageWith(0xaa));
        await inner.writeMeta("/recycled", metaOf(PAGE_SIZE));

        await inner.writeMeta("/removed", metaOf(0));

        const ctrl = new ControllableBackend(inner);
        const backend = new PreloadBackend(ctrl);
        await backend.init();

        // Apply all mutations
        backend.deletePagesFrom("/trunc", 1);
        backend.writePage("/trunc", 0, pageWith(0xff));
        backend.writeMeta("/trunc", metaOf(PAGE_SIZE, 2000));

        backend.writePage("/clean", 0, pageWith(0x22));
        backend.writeMeta("/clean", metaOf(PAGE_SIZE));

        backend.deleteFile("/recycled");
        backend.deleteMeta("/recycled");
        backend.writePage("/recycled", 0, pageWith(0xbb));
        backend.writeMeta("/recycled", metaOf(PAGE_SIZE, 3000));

        backend.deleteMeta("/removed");

        return { backend, ctrl, inner };
      }

      /** Verify the final expected state in the backend. */
      async function verifyFinalState(inner: MemoryBackend): Promise<void> {
        // /trunc: truncated to 1 page, new data
        expect(await inner.countPages("/trunc")).toBe(1);
        expect((await inner.readPage("/trunc", 0))![0]).toBe(0xff);
        expect((await inner.readMeta("/trunc"))!.mtime).toBe(2000);

        // /clean: new file
        expect((await inner.readPage("/clean", 0))![0]).toBe(0x22);
        expect(await inner.readMeta("/clean")).not.toBeNull();

        // /recycled: new data
        expect((await inner.readPage("/recycled", 0))![0]).toBe(0xbb);
        expect((await inner.readMeta("/recycled"))!.mtime).toBe(3000);

        // /removed: metadata deleted
        expect(await inner.readMeta("/removed")).toBeNull();
      }

      // Flush has these operations:
      // Op 0: deletePagesFrom("/trunc", 1) — step 1
      // Op 1: syncAll(early: /trunc + /clean pages + metas) — step 2
      // Op 2: deleteFiles(["/recycled"]) — step 3
      // Op 3: deleteMetas(["/removed"]) — step 4
      // Op 4: syncAll(late: /recycled pages + metas) — step 5

      // Test crash at each operation index (0–4) and verify retry
      for (let crashAt = 0; crashAt <= 4; crashAt++) {
        const { backend, ctrl, inner } = await seedAndInit();

        ctrl.arm(crashAt);
        await expect(backend.flush()).rejects.toThrow("SIMULATED_CRASH");
        expect(backend.isDirty).toBe(true);

        ctrl.disarm();
        await backend.flush();

        expect(backend.isDirty).toBe(false);
        await verifyFinalState(inner);
      }
    });
  });

  // ---------------------------------------------------------------
  // Scenario 6: Multiple truncations in parallel
  // Step 1 uses Promise.all — partial truncation success
  // ---------------------------------------------------------------

  describe("multiple truncations with partial failure", () => {
    it("crash during second truncation — first truncation committed, retry completes both", async () => {
      // Seed: /a and /b both have 3 pages
      await inner.writePage("/a", 0, pageWith(0x01));
      await inner.writePage("/a", 1, pageWith(0x02));
      await inner.writePage("/a", 2, pageWith(0x03));
      await inner.writeMeta("/a", metaOf(3 * PAGE_SIZE));

      await inner.writePage("/b", 0, pageWith(0x04));
      await inner.writePage("/b", 1, pageWith(0x05));
      await inner.writePage("/b", 2, pageWith(0x06));
      await inner.writeMeta("/b", metaOf(3 * PAGE_SIZE));

      const backend = new PreloadBackend(ctrl);
      await backend.init();

      // Truncate both files
      backend.deletePagesFrom("/a", 1);
      backend.deletePagesFrom("/b", 1);
      backend.writeMeta("/a", metaOf(PAGE_SIZE, 2000));
      backend.writeMeta("/b", metaOf(PAGE_SIZE, 2000));

      // arm(1): first truncation succeeds, second crashes
      // (Promise.all fires both concurrently — one succeeds, one fails)
      ctrl.arm(1);
      await expect(backend.flush()).rejects.toThrow("SIMULATED_CRASH");

      // One truncation may have committed, but truncations set is preserved
      // because truncations.clear() is after the await Promise.all
      expect(backend.isDirty).toBe(true);

      // Retry: both truncations are re-attempted (idempotent for the one
      // that already succeeded), then syncAll writes metadata
      ctrl.disarm();
      await backend.flush();

      expect(backend.isDirty).toBe(false);
      expect(await inner.countPages("/a")).toBe(1);
      expect(await inner.countPages("/b")).toBe(1);
      expect((await inner.readMeta("/a"))!.mtime).toBe(2000);
      expect((await inner.readMeta("/b"))!.mtime).toBe(2000);
    });
  });

  // ---------------------------------------------------------------
  // Scenario 7: Writes during flush are preserved across crash
  // ---------------------------------------------------------------

  describe("writes during flush retry cycle", () => {
    it("new writes between failed flush and retry are included", async () => {
      const backend = new PreloadBackend(ctrl);
      await backend.init();

      backend.writePage("/a", 0, pageWith(0x11));
      backend.writeMeta("/a", metaOf(PAGE_SIZE));

      // Crash during flush
      ctrl.arm(0);
      await expect(backend.flush()).rejects.toThrow("SIMULATED_CRASH");

      // Write additional data AFTER the failed flush
      backend.writePage("/b", 0, pageWith(0x22));
      backend.writeMeta("/b", metaOf(PAGE_SIZE));

      // Retry: both /a and /b should be flushed
      ctrl.disarm();
      await backend.flush();

      expect(backend.isDirty).toBe(false);
      expect((await inner.readPage("/a", 0))![0]).toBe(0x11);
      expect((await inner.readPage("/b", 0))![0]).toBe(0x22);
    });
  });

  // ---------------------------------------------------------------
  // Scenario 8: Double crash — flush fails twice, third attempt succeeds
  // ---------------------------------------------------------------

  describe("double crash recovery", () => {
    it("@fast dirty tracking survives multiple consecutive failures", async () => {
      const backend = new PreloadBackend(ctrl);
      await backend.init();

      backend.writePage("/a", 0, pageWith(0x42));
      backend.writeMeta("/a", metaOf(PAGE_SIZE));

      // First crash
      ctrl.arm(0);
      await expect(backend.flush()).rejects.toThrow("SIMULATED_CRASH");
      expect(backend.isDirty).toBe(true);

      // Second crash
      ctrl.arm(0);
      await expect(backend.flush()).rejects.toThrow("SIMULATED_CRASH");
      expect(backend.isDirty).toBe(true);

      // Third attempt succeeds
      ctrl.disarm();
      await backend.flush();

      expect(backend.isDirty).toBe(false);
      expect((await inner.readPage("/a", 0))![0]).toBe(0x42);
    });
  });

  // ---------------------------------------------------------------
  // Scenario 9: Delete with separate metadata deletion (deletedMeta path)
  // flush() → step 3 (deleteFiles) + step 4 (deleteMetas)
  // ---------------------------------------------------------------

  describe("file delete + metadata delete (no recreate)", () => {
    it("crash at step 4 (deleteMetas after deleteFiles) — files deleted, meta retained", async () => {
      // Seed: /gone has pages and metadata
      await inner.writePage("/gone", 0, pageWith(0xdd));
      await inner.writeMeta("/gone", metaOf(PAGE_SIZE));

      const backend = new PreloadBackend(ctrl);
      await backend.init();

      // Delete everything for /gone
      backend.deleteFile("/gone");
      backend.deleteMeta("/gone");

      // Step 3 (deleteFiles) succeeds, step 4 (deleteMetas) crashes
      ctrl.arm(1);
      await expect(backend.flush()).rejects.toThrow("SIMULATED_CRASH");

      // Pages deleted, metadata still present
      expect(await inner.readPage("/gone", 0)).toBeNull();
      expect(await inner.readMeta("/gone")).not.toBeNull();

      // Retry: deleteFiles is idempotent (already done), deleteMetas succeeds
      ctrl.disarm();
      await backend.flush();

      expect(backend.isDirty).toBe(false);
      expect(await inner.readMeta("/gone")).toBeNull();
    });
  });

  // ---------------------------------------------------------------
  // Scenario 10: Verify flush is clean after successful retry
  // (no stale dirty entries cause duplicate work)
  // ---------------------------------------------------------------

  describe("post-recovery cleanliness", () => {
    it("second flush after recovery is a no-op", async () => {
      const backend = new PreloadBackend(ctrl);
      await backend.init();

      backend.writePage("/a", 0, pageWith(0x42));
      backend.writeMeta("/a", metaOf(PAGE_SIZE));

      // Crash + retry
      ctrl.arm(0);
      await expect(backend.flush()).rejects.toThrow("SIMULATED_CRASH");
      ctrl.disarm();
      await backend.flush();

      expect(backend.isDirty).toBe(false);
      expect(backend.dirtyPageCount).toBe(0);
      expect(backend.dirtyMetaCount).toBe(0);

      // A subsequent flush should be a no-op (nothing dirty)
      await backend.flush();
      expect(backend.isDirty).toBe(false);
    });
  });
});
