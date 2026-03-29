/**
 * Crash-recovery probe edge-case tests for PreloadBackend.doInit().
 *
 * The PreloadBackend uses an exponential probe + binary search algorithm
 * to discover pages that exist beyond what metadata accounts for. This
 * happens when a crash occurs after pages were written to the backend
 * but before metadata was synced.
 *
 * These tests exercise boundary conditions of the probe algorithm:
 * - Power-of-2 page counts (where exponential doubling aligns exactly)
 * - Sub-page metadata sizes (meta.size not page-aligned)
 * - Multiple files with different orphan extents
 * - File with metadata but zero actual pages
 * - Exact boundary between exponential probe and binary search
 * - Single extra page (simplest probe case)
 */
import { describe, it, expect } from "vitest";
import { MemoryBackend } from "../../src/memory-backend.js";
import { PreloadBackend } from "../../src/preload-backend.js";
import { PAGE_SIZE } from "../../src/types.js";
import type { StorageBackend } from "../../src/storage-backend.js";
import type { FileMeta } from "../../src/types.js";

/** Helper: write N pages to the remote backend with distinguishable content. */
async function seedPages(
  remote: MemoryBackend,
  path: string,
  count: number,
): Promise<void> {
  for (let i = 0; i < count; i++) {
    const d = new Uint8Array(PAGE_SIZE);
    // Store page index in first two bytes (big-endian) for verification
    d[0] = (i >> 8) & 0xff;
    d[1] = i & 0xff;
    await remote.writePage(path, i, d);
  }
}

/** Helper: verify page i has the expected marker bytes. */
function verifyPage(
  backend: PreloadBackend,
  path: string,
  pageIndex: number,
): void {
  const page = backend.readPage(path, pageIndex);
  expect(page, `page ${pageIndex} should exist`).not.toBeNull();
  const expected0 = (pageIndex >> 8) & 0xff;
  const expected1 = pageIndex & 0xff;
  expect(page![0]).toBe(expected0);
  expect(page![1]).toBe(expected1);
}

const baseMeta: FileMeta = {
  size: 0,
  mode: 0o100644,
  ctime: 1000,
  mtime: 1000,
};

/**
 * Wrapper that counts readPage calls to verify probe efficiency.
 */
class ReadCountingBackend implements StorageBackend {
  private inner: MemoryBackend;
  readPageCount = 0;

  constructor(inner: MemoryBackend) {
    this.inner = inner;
  }

  resetCount(): void {
    this.readPageCount = 0;
  }

  async readPage(path: string, pageIndex: number) {
    this.readPageCount++;
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
    return this.inner.deleteFiles(paths);
  }
  async deletePagesFrom(path: string, fromPageIndex: number) {
    return this.inner.deletePagesFrom(path, fromPageIndex);
  }
  async renameFile(oldPath: string, newPath: string) {
    return this.inner.renameFile(oldPath, newPath);
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
    return this.inner.deleteMetas(paths);
  }
  async countPages(path: string) {
    return this.inner.countPages(path);
  }
  async maxPageIndex(path: string) {
    return this.inner.maxPageIndex(path);
  }
  async listFiles() {
    return this.inner.listFiles();
  }
}

describe("PreloadBackend crash-recovery probe", () => {
  describe("power-of-2 boundary conditions", () => {
    it("@fast exactly 1 extra page beyond metadata", async () => {
      // meta says 2 pages, but 3 exist. Simplest probe: check page 2,
      // found → lo=2, hi=3, check page 3 → missing. Binary search skips
      // (hi - lo = 1). Load batch: just page 2 (already stored from initial check).
      const remote = new MemoryBackend();
      await seedPages(remote, "/f", 3);
      await remote.writeMeta("/f", { ...baseMeta, size: PAGE_SIZE * 2 });

      const backend = new PreloadBackend(remote);
      await backend.init();

      for (let i = 0; i < 3; i++) verifyPage(backend, "/f", i);
      expect(backend.readPage("/f", 3)).toBeNull();
    });

    it("exactly 2 extra pages (triggers one probe doubling)", async () => {
      // meta says 1 page, but 3 exist (pages 0, 1, 2).
      // Probe: check page 1 → found (lo=1, hi=2). Check page 2 → found
      // (lo=2, hi=4). Check page 4 → missing. Binary: lo=2, hi=4, mid=3
      // → missing → hi=3. hi-lo=1 → done. lo=2.
      const remote = new MemoryBackend();
      await seedPages(remote, "/f", 3);
      await remote.writeMeta("/f", { ...baseMeta, size: PAGE_SIZE });

      const backend = new PreloadBackend(remote);
      await backend.init();

      for (let i = 0; i < 3; i++) verifyPage(backend, "/f", i);
      expect(backend.readPage("/f", 3)).toBeNull();
    });

    it("4 extra pages at power-of-2 boundary", async () => {
      // meta says 0 pages, but 4 exist (0-3). pageCount=0.
      // Probe: page 0 exists → stored. lo=0, hi=1.
      // page 1 exists → lo=1, hi=2. page 2 exists → lo=2, hi=4.
      // page 4 missing → exit. Binary: lo=2, hi=4, mid=3 → exists → lo=3.
      // hi-lo=1 → done. Load pages 1 through 3.
      const remote = new MemoryBackend();
      await seedPages(remote, "/f", 4);
      await remote.writeMeta("/f", { ...baseMeta, size: 0 });

      const backend = new PreloadBackend(remote);
      await backend.init();

      for (let i = 0; i < 4; i++) verifyPage(backend, "/f", i);
      expect(backend.readPage("/f", 4)).toBeNull();
    });

    it("8 extra pages — multiple probe doublings", async () => {
      // meta says 1 page, 9 pages exist (0-8). pageCount=1.
      // Probe page 1 → exists. lo=1, hi=2.
      // page 2 → exists. lo=2, hi=4.
      // page 4 → exists. lo=4, hi=8.
      // page 8 → exists. lo=8, hi=16.
      // page 16 → missing. Binary search narrows 8-16.
      const remote = new MemoryBackend();
      await seedPages(remote, "/f", 9);
      await remote.writeMeta("/f", { ...baseMeta, size: PAGE_SIZE });

      const backend = new PreloadBackend(remote);
      await backend.init();

      for (let i = 0; i < 9; i++) verifyPage(backend, "/f", i);
      expect(backend.readPage("/f", 9)).toBeNull();
    });

    it("16 extra pages — probe lands exactly on last page", async () => {
      // meta says 0 pages, 16 pages exist (0-15). pageCount=0.
      // The exponential probe should find hi=16 and page 16 is missing,
      // then binary search between lo (some value) and 16 finds page 15.
      const remote = new MemoryBackend();
      await seedPages(remote, "/f", 16);
      await remote.writeMeta("/f", { ...baseMeta, size: 0 });

      const backend = new PreloadBackend(remote);
      await backend.init();

      for (let i = 0; i < 16; i++) verifyPage(backend, "/f", i);
      expect(backend.readPage("/f", 16)).toBeNull();
    });
  });

  describe("sub-page metadata sizes", () => {
    it("@fast meta.size is sub-page (partial first page) with extra pages", async () => {
      // meta.size = 100 (sub-page), pageCount = ceil(100/8192) = 1.
      // But 3 pages actually exist (0, 1, 2). Probe starts at page 1.
      const remote = new MemoryBackend();
      await seedPages(remote, "/f", 3);
      await remote.writeMeta("/f", { ...baseMeta, size: 100 });

      const backend = new PreloadBackend(remote);
      await backend.init();

      for (let i = 0; i < 3; i++) verifyPage(backend, "/f", i);
      expect(backend.readPage("/f", 3)).toBeNull();
    });

    it("meta.size is exactly PAGE_SIZE (no partial page)", async () => {
      // meta.size = PAGE_SIZE, pageCount = 1. Pages 0-4 exist.
      const remote = new MemoryBackend();
      await seedPages(remote, "/f", 5);
      await remote.writeMeta("/f", { ...baseMeta, size: PAGE_SIZE });

      const backend = new PreloadBackend(remote);
      await backend.init();

      for (let i = 0; i < 5; i++) verifyPage(backend, "/f", i);
      expect(backend.readPage("/f", 5)).toBeNull();
    });

    it("meta.size is PAGE_SIZE + 1 (just over boundary)", async () => {
      // meta.size = PAGE_SIZE + 1, pageCount = 2. Pages 0-5 exist.
      const remote = new MemoryBackend();
      await seedPages(remote, "/f", 6);
      await remote.writeMeta("/f", { ...baseMeta, size: PAGE_SIZE + 1 });

      const backend = new PreloadBackend(remote);
      await backend.init();

      for (let i = 0; i < 6; i++) verifyPage(backend, "/f", i);
      expect(backend.readPage("/f", 6)).toBeNull();
    });

    it("meta.size is PAGE_SIZE - 1 (just under boundary)", async () => {
      // meta.size = PAGE_SIZE - 1, pageCount = 1. Pages 0-3 exist.
      const remote = new MemoryBackend();
      await seedPages(remote, "/f", 4);
      await remote.writeMeta("/f", { ...baseMeta, size: PAGE_SIZE - 1 });

      const backend = new PreloadBackend(remote);
      await backend.init();

      for (let i = 0; i < 4; i++) verifyPage(backend, "/f", i);
      expect(backend.readPage("/f", 4)).toBeNull();
    });
  });

  describe("multiple files with different orphan extents", () => {
    it("three files with 0, 3, and 10 extra pages respectively", async () => {
      const remote = new MemoryBackend();

      // File A: metadata matches actual pages (no orphans)
      await seedPages(remote, "/a", 2);
      await remote.writeMeta("/a", { ...baseMeta, size: PAGE_SIZE * 2 });

      // File B: 3 extra pages (meta says 1, actually 4)
      await seedPages(remote, "/b", 4);
      await remote.writeMeta("/b", { ...baseMeta, size: PAGE_SIZE });

      // File C: 10 extra pages (meta says 2, actually 12)
      await seedPages(remote, "/c", 12);
      await remote.writeMeta("/c", { ...baseMeta, size: PAGE_SIZE * 2 });

      const backend = new PreloadBackend(remote);
      await backend.init();

      // Verify file A: exactly 2 pages
      for (let i = 0; i < 2; i++) verifyPage(backend, "/a", i);
      expect(backend.readPage("/a", 2)).toBeNull();

      // Verify file B: all 4 pages recovered
      for (let i = 0; i < 4; i++) verifyPage(backend, "/b", i);
      expect(backend.readPage("/b", 4)).toBeNull();

      // Verify file C: all 12 pages recovered
      for (let i = 0; i < 12; i++) verifyPage(backend, "/c", i);
      expect(backend.readPage("/c", 12)).toBeNull();
    });

    it("file with orphans alongside file with no pages", async () => {
      const remote = new MemoryBackend();

      // File A: metadata exists but size is 0, no pages
      await remote.writeMeta("/empty", { ...baseMeta, size: 0 });

      // File B: 5 extra pages beyond metadata
      await seedPages(remote, "/data", 7);
      await remote.writeMeta("/data", { ...baseMeta, size: PAGE_SIZE * 2 });

      const backend = new PreloadBackend(remote);
      await backend.init();

      expect(backend.readPage("/empty", 0)).toBeNull();
      expect(backend.listFiles()).toContain("/empty");

      for (let i = 0; i < 7; i++) verifyPage(backend, "/data", i);
      expect(backend.readPage("/data", 7)).toBeNull();
    });
  });

  describe("edge cases", () => {
    it("metadata exists with size > 0 but no pages in backend", async () => {
      // Simulates a crash where metadata was written but pages weren't.
      // meta says 2 pages, but no pages exist at all.
      const remote = new MemoryBackend();
      await remote.writeMeta("/f", { ...baseMeta, size: PAGE_SIZE * 2 });

      const backend = new PreloadBackend(remote);
      await backend.init();

      // No pages should be loaded (readPages returns all null)
      expect(backend.readPage("/f", 0)).toBeNull();
      expect(backend.readPage("/f", 1)).toBeNull();
      // Metadata should still be present
      expect(backend.readMeta("/f")).not.toBeNull();
    });

    it("metadata with size > 0, some pages missing within expected range", async () => {
      // meta says 3 pages, but only pages 0 and 2 exist (page 1 missing).
      // This simulates partial page loss. The probe for beyond-meta pages
      // should still work correctly.
      const remote = new MemoryBackend();
      const d0 = new Uint8Array(PAGE_SIZE);
      d0[0] = 0xaa;
      await remote.writePage("/f", 0, d0);
      // Skip page 1
      const d2 = new Uint8Array(PAGE_SIZE);
      d2[0] = 0xcc;
      await remote.writePage("/f", 2, d2);
      await remote.writeMeta("/f", { ...baseMeta, size: PAGE_SIZE * 3 });

      const backend = new PreloadBackend(remote);
      await backend.init();

      // Page 0 loaded, page 1 missing (null from readPages), page 2 loaded
      expect(backend.readPage("/f", 0)![0]).toBe(0xaa);
      expect(backend.readPage("/f", 1)).toBeNull();
      expect(backend.readPage("/f", 2)![0]).toBe(0xcc);
    });

    it("pages exist beyond expected range at non-contiguous indices", async () => {
      // meta says 1 page, pages 0 and 1 exist, but page 2 is missing
      // and page 3 exists. The probe checks page 1 (pageCount=1),
      // finds it, then checks page 2 — missing. So it stops at lo=1.
      // Page 3 is NOT discovered (known limitation of contiguous probe).
      const remote = new MemoryBackend();
      const d0 = new Uint8Array(PAGE_SIZE);
      d0[0] = 0x10;
      const d1 = new Uint8Array(PAGE_SIZE);
      d1[0] = 0x20;
      const d3 = new Uint8Array(PAGE_SIZE);
      d3[0] = 0x40;
      await remote.writePage("/f", 0, d0);
      await remote.writePage("/f", 1, d1);
      // Skip page 2
      await remote.writePage("/f", 3, d3);
      await remote.writeMeta("/f", { ...baseMeta, size: PAGE_SIZE });

      const backend = new PreloadBackend(remote);
      await backend.init();

      // Pages 0 and 1 should be loaded
      expect(backend.readPage("/f", 0)![0]).toBe(0x10);
      expect(backend.readPage("/f", 1)![0]).toBe(0x20);
      // Page 3 is NOT loaded (gap at page 2 stops the probe)
      expect(backend.readPage("/f", 3)).toBeNull();
    });

    it("large orphan count (50 pages beyond metadata)", async () => {
      // Tests multiple exponential doublings + binary search convergence
      const remote = new MemoryBackend();
      const totalPages = 52;
      await seedPages(remote, "/f", totalPages);
      await remote.writeMeta("/f", { ...baseMeta, size: PAGE_SIZE * 2 });

      const backend = new PreloadBackend(remote);
      await backend.init();

      for (let i = 0; i < totalPages; i++) verifyPage(backend, "/f", i);
      expect(backend.readPage("/f", totalPages)).toBeNull();
    });

    it("all pages beyond metadata (meta.size = 0, many pages)", async () => {
      // Every page is an "orphan" — metadata never synced after creation.
      const remote = new MemoryBackend();
      await seedPages(remote, "/f", 33);
      await remote.writeMeta("/f", { ...baseMeta, size: 0 });

      const backend = new PreloadBackend(remote);
      await backend.init();

      for (let i = 0; i < 33; i++) verifyPage(backend, "/f", i);
      expect(backend.readPage("/f", 33)).toBeNull();
    });
  });

  describe("probe efficiency", () => {
    it("probe uses O(log n) readPage calls, not O(n)", async () => {
      // 100 pages beyond metadata — should use ~14 readPage probes
      // (log2(100) ≈ 7 for exponential + ~7 for binary search),
      // not 100 linear scans.
      const inner = new MemoryBackend();
      await seedPages(inner, "/f", 100);
      await inner.writeMeta("/f", { ...baseMeta, size: 0 });

      const counting = new ReadCountingBackend(inner);
      const backend = new PreloadBackend(counting);
      counting.resetCount();
      await backend.init();

      // Verify all pages loaded correctly
      for (let i = 0; i < 100; i++) {
        expect(backend.readPage("/f", i)).not.toBeNull();
      }
      expect(backend.readPage("/f", 100)).toBeNull();

      // The probe should use far fewer than 100 readPage calls.
      // Initial check (1) + exponential probe (~7) + binary search (~7) = ~15.
      // Plus 1 readPage for the file's normal page loading is via readPages (not counted).
      // Allow generous margin but confirm it's logarithmic, not linear.
      expect(counting.readPageCount).toBeLessThan(30);
    });

    it("no extra readPage calls when metadata matches actual extent", async () => {
      // When no orphan pages exist, the probe should make exactly 1 extra
      // readPage call (checking the page at pageCount, which returns null).
      const inner = new MemoryBackend();
      await seedPages(inner, "/f", 5);
      await inner.writeMeta("/f", { ...baseMeta, size: PAGE_SIZE * 5 });

      const counting = new ReadCountingBackend(inner);
      const backend = new PreloadBackend(counting);
      counting.resetCount();
      await backend.init();

      // 1 readPage call to check page 5 (doesn't exist)
      expect(counting.readPageCount).toBe(1);
    });
  });

  describe("flush after probe-loaded pages", () => {
    it("@fast probe-loaded pages are not dirty (no unnecessary flush)", async () => {
      // Pages loaded during init via the probe should not be marked dirty.
      // They're already in the backend — writing them back wastes I/O.
      const remote = new MemoryBackend();
      await seedPages(remote, "/f", 5);
      await remote.writeMeta("/f", { ...baseMeta, size: PAGE_SIZE });

      const backend = new PreloadBackend(remote);
      await backend.init();

      // No writes occurred locally, so nothing should be dirty
      expect(backend.isDirty).toBe(false);
      expect(backend.dirtyPageCount).toBe(0);
    });

    it("writing to a probe-loaded page marks it dirty correctly", async () => {
      const remote = new MemoryBackend();
      await seedPages(remote, "/f", 3);
      await remote.writeMeta("/f", { ...baseMeta, size: PAGE_SIZE });

      const backend = new PreloadBackend(remote);
      await backend.init();

      // Modify a probe-loaded page
      const modified = new Uint8Array(PAGE_SIZE);
      modified[0] = 0xff;
      backend.writePage("/f", 2, modified);

      expect(backend.isDirty).toBe(true);
      expect(backend.dirtyPageCount).toBe(1);

      await backend.flush();

      // Verify the modification reached the remote
      const page = await remote.readPage("/f", 2);
      expect(page![0]).toBe(0xff);
    });

    it("flush + re-init round-trip preserves probe-loaded pages", async () => {
      // Write pages beyond metadata, init (probe loads them),
      // write new data, flush, then re-init and verify everything survived.
      const remote = new MemoryBackend();
      await seedPages(remote, "/f", 5);
      await remote.writeMeta("/f", { ...baseMeta, size: PAGE_SIZE });

      const backend1 = new PreloadBackend(remote);
      await backend1.init();

      // Write updated metadata to match actual extent
      backend1.writeMeta("/f", { ...baseMeta, size: PAGE_SIZE * 5 });
      await backend1.flush();

      // Re-init: should load all 5 pages without probe (metadata now correct)
      const backend2 = new PreloadBackend(remote);
      await backend2.init();

      for (let i = 0; i < 5; i++) verifyPage(backend2, "/f", i);
      expect(backend2.readPage("/f", 5)).toBeNull();
    });
  });
});
