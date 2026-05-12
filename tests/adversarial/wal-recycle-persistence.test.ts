/**
 * Adversarial tests: WAL segment recycling patterns under cache pressure.
 *
 * Postgres recycles WAL segments by truncating them to zero and re-allocating,
 * or by renaming them to new segment names. This creates a specific pattern:
 *   allocate → sequential write → syncfs → truncate-to-zero → re-allocate →
 *   write new data → syncfs → remount
 *
 * Under a tiny page cache, this pattern exercises:
 * - allocate's sentinel page creation (markPageDirtyNoRead)
 * - truncate-to-zero's full page cache + backend cleanup (deleteFile)
 * - Re-allocation after deletion on the same storagePath
 * - Dirty tracking reset after truncate-to-zero (no stale dirty keys)
 * - Cross-file cache eviction during interleaved recycling
 * - Persistence correctness through multiple syncfs → remount cycles
 *
 * The critical seam: when a file is truncated to zero and immediately
 * re-allocated, the page cache's filePages and dirtyFileKeys indexes
 * must be fully cleaned up. Any stale entries cause data corruption on
 * the next syncfs (flushing deleted pages) or memory leaks (zombie
 * entries in the secondary indexes).
 *
 * Ethos §9: "Target the seams: reads that span page boundaries, writes
 * during eviction, metadata updates after flush, large sequential scans
 * that rotate the entire cache."
 */
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { describe, it, expect, beforeEach } from "vitest";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { createTomeFS } from "../../src/tomefs.js";
import { PAGE_SIZE } from "../../src/types.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

const O = {
  RDONLY: 0,
  WRONLY: 1,
  RDWR: 2,
  CREAT: 64,
  TRUNC: 512,
} as const;

const MOUNT = "/tome";

function fillPattern(size: number, seed: number): Uint8Array {
  const buf = new Uint8Array(size);
  for (let i = 0; i < size; i++) {
    buf[i] = (seed + i * 31) & 0xff;
  }
  return buf;
}

function verifyPattern(buf: Uint8Array, size: number, seed: number): void {
  for (let i = 0; i < size; i++) {
    if (buf[i] !== ((seed + i * 31) & 0xff)) {
      throw new Error(
        `Pattern mismatch at offset ${i}: expected ${(seed + i * 31) & 0xff}, got ${buf[i]}`,
      );
    }
  }
}

async function mountTome(backend: SyncMemoryBackend, maxPages = 4) {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  const Module = await createModule();
  const FS = Module.FS;
  const tomefs = createTomeFS(FS, { backend, maxPages });
  FS.mkdir(MOUNT);
  FS.mount(tomefs, {}, MOUNT);
  return { FS, tomefs };
}

function syncfs(FS: any, tomefs: any) {
  tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
    if (err) throw err;
  });
}

function writeSequential(
  FS: any,
  path: string,
  pages: number,
  seed: number,
): void {
  const fd = FS.open(path, O.RDWR | O.CREAT, 0o666);
  for (let p = 0; p < pages; p++) {
    const data = fillPattern(PAGE_SIZE, seed + p);
    FS.write(fd, data, 0, PAGE_SIZE, p * PAGE_SIZE);
  }
  FS.close(fd);
}

function verifySequential(
  FS: any,
  path: string,
  pages: number,
  seed: number,
): void {
  const fd = FS.open(path, O.RDONLY);
  const buf = new Uint8Array(PAGE_SIZE);
  for (let p = 0; p < pages; p++) {
    FS.read(fd, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
    verifyPattern(buf, PAGE_SIZE, seed + p);
  }
  FS.close(fd);
}

describe("adversarial: WAL segment recycling + persistence", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("single segment recycle: allocate → write → sync → truncate-to-zero → re-allocate → rewrite → sync → remount @fast", async () => {
    {
      const { FS, tomefs } = await mountTome(backend);

      // Phase 1: Create and fill WAL segment
      const fd = FS.open(`${MOUNT}/wal_000`, O.RDWR | O.CREAT, 0o666);
      fd.stream_ops.allocate(fd, 0, 3 * PAGE_SIZE);
      for (let p = 0; p < 3; p++) {
        FS.write(fd, fillPattern(PAGE_SIZE, 0x10 + p), 0, PAGE_SIZE, p * PAGE_SIZE);
      }
      FS.close(fd);
      syncfs(FS, tomefs);

      // Phase 2: Recycle — truncate to zero, re-allocate, write new data
      FS.truncate(`${MOUNT}/wal_000`, 0);
      const fd2 = FS.open(`${MOUNT}/wal_000`, O.RDWR, 0o666);
      fd2.stream_ops.allocate(fd2, 0, 3 * PAGE_SIZE);
      for (let p = 0; p < 3; p++) {
        FS.write(fd2, fillPattern(PAGE_SIZE, 0x50 + p), 0, PAGE_SIZE, p * PAGE_SIZE);
      }
      FS.close(fd2);
      syncfs(FS, tomefs);
    }

    // Remount and verify only the recycled (new) data exists
    {
      const { FS } = await mountTome(backend);
      const stat = FS.stat(`${MOUNT}/wal_000`);
      expect(stat.size).toBe(3 * PAGE_SIZE);
      verifySequential(FS, `${MOUNT}/wal_000`, 3, 0x50);
    }
  });

  it("multiple recycle cycles without remount @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);
    const path = `${MOUNT}/wal`;

    for (let cycle = 0; cycle < 5; cycle++) {
      const seed = (cycle + 1) * 0x20;

      // Truncate to zero (skip on first cycle — file doesn't exist yet)
      if (cycle > 0) {
        FS.truncate(path, 0);
      }

      // Allocate and write
      const fd = FS.open(path, O.RDWR | O.CREAT, 0o666);
      fd.stream_ops.allocate(fd, 0, 3 * PAGE_SIZE);
      for (let p = 0; p < 3; p++) {
        FS.write(fd, fillPattern(PAGE_SIZE, seed + p), 0, PAGE_SIZE, p * PAGE_SIZE);
      }
      FS.close(fd);
      syncfs(FS, tomefs);

      // Verify current cycle's data before next recycle
      verifySequential(FS, path, 3, seed);
    }

    // Final remount verification
    syncfs(FS, tomefs);
    const { FS: FS2 } = await mountTome(backend);
    const lastSeed = 5 * 0x20;
    verifySequential(FS2, `${MOUNT}/wal`, 3, lastSeed);
  });

  it("concurrent segment recycling: 3 WAL segments competing for 4-page cache @fast", async () => {
    const SEGMENTS = 3;
    const PAGES_PER_SEG = 2;

    {
      const { FS, tomefs } = await mountTome(backend, 4);

      // Create all segments
      for (let s = 0; s < SEGMENTS; s++) {
        writeSequential(FS, `${MOUNT}/wal_${s}`, PAGES_PER_SEG, s * 0x10);
      }
      syncfs(FS, tomefs);

      // Recycle all segments: truncate-to-zero and rewrite with new data
      for (let s = 0; s < SEGMENTS; s++) {
        FS.truncate(`${MOUNT}/wal_${s}`, 0);
        writeSequential(FS, `${MOUNT}/wal_${s}`, PAGES_PER_SEG, 0x80 + s * 0x10);
      }
      syncfs(FS, tomefs);
    }

    // Remount and verify all segments have recycled data
    {
      const { FS } = await mountTome(backend, 4);
      for (let s = 0; s < SEGMENTS; s++) {
        const stat = FS.stat(`${MOUNT}/wal_${s}`);
        expect(stat.size).toBe(PAGES_PER_SEG * PAGE_SIZE);
        verifySequential(FS, `${MOUNT}/wal_${s}`, PAGES_PER_SEG, 0x80 + s * 0x10);
      }
    }
  });

  it("recycle with different allocation sizes @fast", async () => {
    {
      const { FS, tomefs } = await mountTome(backend);

      // Cycle 1: 4 pages
      const fd1 = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);
      fd1.stream_ops.allocate(fd1, 0, 4 * PAGE_SIZE);
      FS.write(fd1, fillPattern(PAGE_SIZE, 0xAA), 0, PAGE_SIZE, 0);
      FS.write(fd1, fillPattern(PAGE_SIZE, 0xAB), 0, PAGE_SIZE, 3 * PAGE_SIZE);
      FS.close(fd1);
      syncfs(FS, tomefs);

      // Recycle to smaller size: 2 pages
      FS.truncate(`${MOUNT}/wal`, 0);
      const fd2 = FS.open(`${MOUNT}/wal`, O.RDWR, 0o666);
      fd2.stream_ops.allocate(fd2, 0, 2 * PAGE_SIZE);
      FS.write(fd2, fillPattern(PAGE_SIZE, 0xCC), 0, PAGE_SIZE, 0);
      FS.write(fd2, fillPattern(PAGE_SIZE, 0xCD), 0, PAGE_SIZE, PAGE_SIZE);
      FS.close(fd2);
      syncfs(FS, tomefs);
    }

    {
      const { FS } = await mountTome(backend);
      const stat = FS.stat(`${MOUNT}/wal`);
      expect(stat.size).toBe(2 * PAGE_SIZE);
      verifySequential(FS, `${MOUNT}/wal`, 2, 0xCC);

      // Verify no ghost data from the old 4-page allocation
      // (backend should have at most 2 pages)
      expect(backend.maxPageIndex("/wal")).toBeLessThanOrEqual(1);
    }
  });

  it("recycle to larger size: old pages fully replaced @fast", async () => {
    {
      const { FS, tomefs } = await mountTome(backend);

      // Start with 2 pages
      writeSequential(FS, `${MOUNT}/wal`, 2, 0x10);
      syncfs(FS, tomefs);

      // Recycle to 5 pages (larger than original)
      FS.truncate(`${MOUNT}/wal`, 0);
      const fd = FS.open(`${MOUNT}/wal`, O.RDWR, 0o666);
      fd.stream_ops.allocate(fd, 0, 5 * PAGE_SIZE);
      for (let p = 0; p < 5; p++) {
        FS.write(fd, fillPattern(PAGE_SIZE, 0x60 + p), 0, PAGE_SIZE, p * PAGE_SIZE);
      }
      FS.close(fd);
      syncfs(FS, tomefs);
    }

    {
      const { FS } = await mountTome(backend);
      expect(FS.stat(`${MOUNT}/wal`).size).toBe(5 * PAGE_SIZE);
      verifySequential(FS, `${MOUNT}/wal`, 5, 0x60);
    }
  });

  it("interleaved recycling: recycle A while B has open fd reading @fast", async () => {
    {
      const { FS, tomefs } = await mountTome(backend, 4);

      // Create two segments
      writeSequential(FS, `${MOUNT}/a`, 2, 0x10);
      writeSequential(FS, `${MOUNT}/b`, 2, 0x20);
      syncfs(FS, tomefs);

      // Open B for reading (holds pages in cache)
      const fdB = FS.open(`${MOUNT}/b`, O.RDONLY);
      const bufB = new Uint8Array(PAGE_SIZE);
      FS.read(fdB, bufB, 0, PAGE_SIZE, 0);
      verifyPattern(bufB, PAGE_SIZE, 0x20);

      // Recycle A while B is open — A's truncate + rewrite competes
      // with B's cached pages
      FS.truncate(`${MOUNT}/a`, 0);
      writeSequential(FS, `${MOUNT}/a`, 3, 0x40);

      // Read B's second page — may need reload from backend due to
      // A's rewrite evicting B's pages
      FS.read(fdB, bufB, 0, PAGE_SIZE, PAGE_SIZE);
      verifyPattern(bufB, PAGE_SIZE, 0x21);
      FS.close(fdB);

      syncfs(FS, tomefs);
    }

    {
      const { FS } = await mountTome(backend, 4);
      expect(FS.stat(`${MOUNT}/a`).size).toBe(3 * PAGE_SIZE);
      verifySequential(FS, `${MOUNT}/a`, 3, 0x40);
      expect(FS.stat(`${MOUNT}/b`).size).toBe(2 * PAGE_SIZE);
      verifySequential(FS, `${MOUNT}/b`, 2, 0x20);
    }
  });

  it("recycle via O_TRUNC open instead of explicit truncate @fast", async () => {
    {
      const { FS, tomefs } = await mountTome(backend);

      writeSequential(FS, `${MOUNT}/wal`, 3, 0x10);
      syncfs(FS, tomefs);

      // Recycle using O_TRUNC (Postgres sometimes does this)
      const fd = FS.open(`${MOUNT}/wal`, O.RDWR | O.TRUNC, 0o666);
      expect(FS.fstat(fd.fd).size).toBe(0);

      fd.stream_ops.allocate(fd, 0, 3 * PAGE_SIZE);
      for (let p = 0; p < 3; p++) {
        FS.write(fd, fillPattern(PAGE_SIZE, 0x70 + p), 0, PAGE_SIZE, p * PAGE_SIZE);
      }
      FS.close(fd);
      syncfs(FS, tomefs);
    }

    {
      const { FS } = await mountTome(backend);
      expect(FS.stat(`${MOUNT}/wal`).size).toBe(3 * PAGE_SIZE);
      verifySequential(FS, `${MOUNT}/wal`, 3, 0x70);
    }
  });

  it("recycle with partial write: only first page written after re-allocate @fast", async () => {
    {
      const { FS, tomefs } = await mountTome(backend);

      // Full write
      writeSequential(FS, `${MOUNT}/wal`, 4, 0x10);
      syncfs(FS, tomefs);

      // Recycle: truncate-to-zero, allocate 4 pages, write only page 0
      FS.truncate(`${MOUNT}/wal`, 0);
      const fd = FS.open(`${MOUNT}/wal`, O.RDWR, 0o666);
      fd.stream_ops.allocate(fd, 0, 4 * PAGE_SIZE);
      FS.write(fd, fillPattern(PAGE_SIZE, 0xBB), 0, PAGE_SIZE, 0);
      FS.close(fd);
      syncfs(FS, tomefs);
    }

    {
      const { FS } = await mountTome(backend);
      expect(FS.stat(`${MOUNT}/wal`).size).toBe(4 * PAGE_SIZE);

      // Page 0: new data
      const fd = FS.open(`${MOUNT}/wal`, O.RDONLY);
      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(fd, buf, 0, PAGE_SIZE, 0);
      verifyPattern(buf, PAGE_SIZE, 0xBB);

      // Pages 1-3: must be zero (NOT old data from before recycle)
      for (let p = 1; p < 4; p++) {
        FS.read(fd, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
        for (let i = 0; i < PAGE_SIZE; i++) {
          if (buf[i] !== 0) {
            expect.fail(
              `Stale data at page ${p} offset ${i}: got ${buf[i]}, expected 0`,
            );
          }
        }
      }
      FS.close(fd);
    }
  });

  it("rapid recycle storm: 10 cycles back-to-back on 4-page cache @fast", async () => {
    const CYCLES = 10;
    const PAGES = 3;

    {
      const { FS, tomefs } = await mountTome(backend, 4);

      for (let c = 0; c < CYCLES; c++) {
        const seed = c * 0x10;
        if (c > 0) FS.truncate(`${MOUNT}/wal`, 0);
        writeSequential(FS, `${MOUNT}/wal`, PAGES, seed);
        syncfs(FS, tomefs);
      }
    }

    // Only the last cycle's data should survive
    {
      const { FS } = await mountTome(backend, 4);
      const lastSeed = (CYCLES - 1) * 0x10;
      expect(FS.stat(`${MOUNT}/wal`).size).toBe(PAGES * PAGE_SIZE);
      verifySequential(FS, `${MOUNT}/wal`, PAGES, lastSeed);
    }
  });

  it("recycle with non-page-aligned intermediate size @fast", async () => {
    {
      const { FS, tomefs } = await mountTome(backend);

      // Write 3 pages
      writeSequential(FS, `${MOUNT}/wal`, 3, 0x10);
      syncfs(FS, tomefs);

      // Truncate to mid-page (not zero — partial recycle)
      FS.truncate(`${MOUNT}/wal`, PAGE_SIZE + 100);

      // Then truncate to zero (full recycle)
      FS.truncate(`${MOUNT}/wal`, 0);

      // Re-allocate and rewrite
      const fd = FS.open(`${MOUNT}/wal`, O.RDWR, 0o666);
      fd.stream_ops.allocate(fd, 0, 2 * PAGE_SIZE);
      FS.write(fd, fillPattern(PAGE_SIZE, 0xDD), 0, PAGE_SIZE, 0);
      FS.write(fd, fillPattern(PAGE_SIZE, 0xDE), 0, PAGE_SIZE, PAGE_SIZE);
      FS.close(fd);
      syncfs(FS, tomefs);
    }

    {
      const { FS } = await mountTome(backend);
      expect(FS.stat(`${MOUNT}/wal`).size).toBe(2 * PAGE_SIZE);
      verifySequential(FS, `${MOUNT}/wal`, 2, 0xDD);
    }
  });

  it("page cache assertInvariants holds through recycle cycle @fast", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    writeSequential(FS, `${MOUNT}/wal`, 3, 0x10);
    tomefs.pageCache.assertInvariants();

    syncfs(FS, tomefs);
    tomefs.pageCache.assertInvariants();

    FS.truncate(`${MOUNT}/wal`, 0);
    tomefs.pageCache.assertInvariants();

    writeSequential(FS, `${MOUNT}/wal`, 2, 0x30);
    tomefs.pageCache.assertInvariants();

    syncfs(FS, tomefs);
    tomefs.pageCache.assertInvariants();

    // Second recycle with competing file
    writeSequential(FS, `${MOUNT}/other`, 2, 0x50);
    tomefs.pageCache.assertInvariants();

    FS.truncate(`${MOUNT}/wal`, 0);
    tomefs.pageCache.assertInvariants();

    writeSequential(FS, `${MOUNT}/wal`, 3, 0x70);
    tomefs.pageCache.assertInvariants();

    syncfs(FS, tomefs);
    tomefs.pageCache.assertInvariants();
  });

  it("rename-based recycling: rename wal_old → wal_new, write new data @fast", async () => {
    {
      const { FS, tomefs } = await mountTome(backend, 4);

      // Create original segment
      writeSequential(FS, `${MOUNT}/wal_000`, 2, 0x10);
      syncfs(FS, tomefs);

      // Recycle by rename (Postgres renames old WAL to new segment name)
      FS.rename(`${MOUNT}/wal_000`, `${MOUNT}/wal_001`);

      // Truncate recycled segment and rewrite
      FS.truncate(`${MOUNT}/wal_001`, 0);
      writeSequential(FS, `${MOUNT}/wal_001`, 2, 0x30);

      // Create new segment at old name
      writeSequential(FS, `${MOUNT}/wal_000`, 2, 0x50);

      syncfs(FS, tomefs);
    }

    {
      const { FS } = await mountTome(backend, 4);
      // wal_000 has new data
      verifySequential(FS, `${MOUNT}/wal_000`, 2, 0x50);
      // wal_001 has recycled data
      verifySequential(FS, `${MOUNT}/wal_001`, 2, 0x30);
    }
  });

  it("recycle allocate-only segment (no writes before recycle) @fast", async () => {
    {
      const { FS, tomefs } = await mountTome(backend);

      // Allocate without writing (Postgres pre-allocates WAL segments)
      const fd = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);
      fd.stream_ops.allocate(fd, 0, 4 * PAGE_SIZE);
      FS.close(fd);
      syncfs(FS, tomefs);

      // Recycle the unwritten segment
      FS.truncate(`${MOUNT}/wal`, 0);
      const fd2 = FS.open(`${MOUNT}/wal`, O.RDWR, 0o666);
      fd2.stream_ops.allocate(fd2, 0, 4 * PAGE_SIZE);
      FS.write(fd2, fillPattern(PAGE_SIZE, 0xEE), 0, PAGE_SIZE, 0);
      FS.close(fd2);
      syncfs(FS, tomefs);
    }

    {
      const { FS } = await mountTome(backend);
      expect(FS.stat(`${MOUNT}/wal`).size).toBe(4 * PAGE_SIZE);

      const fd = FS.open(`${MOUNT}/wal`, O.RDONLY);
      const buf = new Uint8Array(PAGE_SIZE);

      // Page 0: written data
      FS.read(fd, buf, 0, PAGE_SIZE, 0);
      verifyPattern(buf, PAGE_SIZE, 0xEE);

      // Pages 1-3: zeros (allocated but never written)
      for (let p = 1; p < 4; p++) {
        FS.read(fd, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
        for (let i = 0; i < PAGE_SIZE; i++) {
          if (buf[i] !== 0) {
            expect.fail(`Page ${p} offset ${i}: expected 0, got ${buf[i]}`);
          }
        }
      }
      FS.close(fd);
    }
  });

  it("recycle beyond cache capacity: 8-page segment on 4-page cache @fast", async () => {
    const PAGES = 8;

    {
      const { FS, tomefs } = await mountTome(backend, 4);

      // Fill large segment
      writeSequential(FS, `${MOUNT}/wal`, PAGES, 0x10);
      syncfs(FS, tomefs);

      // Recycle: truncate-to-zero + refill with new data
      FS.truncate(`${MOUNT}/wal`, 0);
      writeSequential(FS, `${MOUNT}/wal`, PAGES, 0x80);
      syncfs(FS, tomefs);
    }

    {
      const { FS } = await mountTome(backend, 4);
      expect(FS.stat(`${MOUNT}/wal`).size).toBe(PAGES * PAGE_SIZE);
      verifySequential(FS, `${MOUNT}/wal`, PAGES, 0x80);
    }
  });

  it("recycle interleaved with reads: stale data never leaks through cache @fast", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    // Write initial data
    writeSequential(FS, `${MOUNT}/wal`, 3, 0x10);
    syncfs(FS, tomefs);

    // Truncate to zero
    FS.truncate(`${MOUNT}/wal`, 0);

    // Verify size is 0
    expect(FS.stat(`${MOUNT}/wal`).size).toBe(0);

    // Re-allocate and partially write
    const fd = FS.open(`${MOUNT}/wal`, O.RDWR, 0o666);
    fd.stream_ops.allocate(fd, 0, 3 * PAGE_SIZE);

    // Write only page 0 with new data
    FS.write(fd, fillPattern(PAGE_SIZE, 0xCC), 0, PAGE_SIZE, 0);

    // Read page 1 — must be zero, NOT old data
    const buf = new Uint8Array(PAGE_SIZE);
    FS.read(fd, buf, 0, PAGE_SIZE, PAGE_SIZE);
    for (let i = 0; i < PAGE_SIZE; i++) {
      if (buf[i] !== 0) {
        FS.close(fd);
        expect.fail(
          `Stale data leaked through cache at page 1 offset ${i}: got ${buf[i]}`,
        );
      }
    }

    // Read page 2 — must also be zero
    FS.read(fd, buf, 0, PAGE_SIZE, 2 * PAGE_SIZE);
    for (let i = 0; i < PAGE_SIZE; i++) {
      if (buf[i] !== 0) {
        FS.close(fd);
        expect.fail(
          `Stale data leaked through cache at page 2 offset ${i}: got ${buf[i]}`,
        );
      }
    }

    FS.close(fd);
  });
});
