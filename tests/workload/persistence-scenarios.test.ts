/**
 * Workload persistence scenario tests for tomefs.
 *
 * These bridge the gap between workload scenarios (realistic Postgres-like
 * patterns, no persistence) and adversarial persistence tests (specific edge
 * cases with syncfs→remount). Each scenario runs a holistic multi-operation
 * workload with periodic "checkpoints" (syncfs→remount), verifying data
 * integrity across persistence boundaries at multiple cache pressure levels.
 *
 * Ethos §6 (performance parity), §8 (workload scenarios), §9 (adversarial).
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
  APPEND: 1024,
} as const;

const SEEK_SET = 0;
const SEEK_END = 2;

const MOUNT = "/tome";

/** Cache sizes that force different eviction behaviors. */
const CACHE_CONFIGS = {
  tiny: 4,     // 32 KB — extreme eviction pressure
  small: 16,   // 128 KB — moderate eviction
  large: 4096, // 32 MB — working set fits
} as const;

type CacheSize = keyof typeof CACHE_CONFIGS;

function encode(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

function decode(buf: Uint8Array, length?: number): string {
  return new TextDecoder().decode(
    length !== undefined ? buf.subarray(0, length) : buf,
  );
}

/** Fill a buffer with a deterministic pattern based on a seed byte. */
function fillPattern(size: number, seed: number): Uint8Array {
  const buf = new Uint8Array(size);
  for (let i = 0; i < size; i++) {
    buf[i] = (seed + i * 31) & 0xff;
  }
  return buf;
}

/** Verify a buffer matches the expected deterministic pattern. */
function expectPattern(buf: Uint8Array, size: number, seed: number): void {
  for (let i = 0; i < size; i++) {
    if (buf[i] !== ((seed + i * 31) & 0xff)) {
      throw new Error(
        `Pattern mismatch at byte ${i}: expected ${(seed + i * 31) & 0xff}, got ${buf[i]} (seed=${seed})`,
      );
    }
  }
}

async function mountTome(backend: SyncMemoryBackend, maxPages: number) {
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

function syncAndUnmount(FS: any, tomefs: any) {
  syncfs(FS, tomefs);
  FS.unmount(MOUNT);
}

/** Helper: read an entire file into a buffer. */
function readFile(FS: any, path: string, size: number): Uint8Array {
  const buf = new Uint8Array(size);
  const s = FS.open(path, O.RDONLY);
  FS.read(s, buf, 0, size);
  FS.close(s);
  return buf;
}

/** Helper: write a buffer to a file (truncating). */
function writeFile(FS: any, path: string, data: Uint8Array): void {
  const s = FS.open(path, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
  FS.write(s, data, 0, data.length);
  FS.close(s);
}

/** Helper: append data to a file. */
function appendFile(FS: any, path: string, data: Uint8Array): void {
  const s = FS.open(path, O.WRONLY | O.CREAT | O.APPEND, 0o666);
  FS.write(s, data, 0, data.length);
  FS.close(s);
}

// ---------------------------------------------------------------------------
// Test runner: runs each scenario at multiple cache pressure levels
// ---------------------------------------------------------------------------

function describeWithPersistence(
  name: string,
  scenarioFn: (backend: SyncMemoryBackend, maxPages: number) => Promise<void>,
) {
  describe(name, () => {
    for (const [sizeName, pages] of Object.entries(CACHE_CONFIGS)) {
      it(`cache=${sizeName} (${pages} pages)`, async () => {
        const backend = new SyncMemoryBackend();
        await scenarioFn(backend, pages);
      });
    }
  });
}

// ---------------------------------------------------------------------------
// Scenario 1: WAL Rotation + Checkpoint
//
// Postgres writes sequentially to a WAL segment. When it fills up, the old
// segment is renamed (archived) and a new segment is created. A checkpoint
// (syncfs) persists state. After restart (remount), both the archived and
// active WAL must be intact.
// ---------------------------------------------------------------------------

describeWithPersistence(
  "Persistence Scenario 1: WAL Rotation + Checkpoint @fast",
  async (backend, maxPages) => {
    const walDir = `${MOUNT}/pg_wal`;
    const segmentSize = PAGE_SIZE * 3; // 24 KB per WAL segment

    // --- Cycle 1: write first WAL segment, rotate, start second ---
    const { FS: FS1, tomefs: t1 } = await mountTome(backend, maxPages);
    FS1.mkdir(walDir);

    // Write WAL segment 000
    const wal0Data = fillPattern(segmentSize, 0x10);
    writeFile(FS1, `${walDir}/000000010000000000000000`, wal0Data);

    // Rotate: archive the full segment, create new one
    FS1.rename(
      `${walDir}/000000010000000000000000`,
      `${walDir}/000000010000000000000000.done`,
    );
    const wal1Partial = fillPattern(PAGE_SIZE, 0x20);
    writeFile(FS1, `${walDir}/000000010000000000000001`, wal1Partial);

    // Checkpoint
    syncAndUnmount(FS1, t1);

    // --- Cycle 2: continue writing to active WAL, rotate again ---
    const { FS: FS2, tomefs: t2 } = await mountTome(backend, maxPages);

    // Verify archived WAL survived
    const archived = readFile(
      FS2,
      `${walDir}/000000010000000000000000.done`,
      segmentSize,
    );
    expectPattern(archived, segmentSize, 0x10);

    // Verify partial active WAL survived
    const active1 = readFile(
      FS2,
      `${walDir}/000000010000000000000001`,
      PAGE_SIZE,
    );
    expectPattern(active1, PAGE_SIZE, 0x20);

    // Extend active WAL to full size
    const wal1Extend = fillPattern(PAGE_SIZE * 2, 0x21);
    const s = FS2.open(`${walDir}/000000010000000000000001`, O.WRONLY);
    FS2.llseek(s, 0, SEEK_END);
    FS2.write(s, wal1Extend, 0, wal1Extend.length);
    FS2.close(s);

    // Rotate and start segment 002
    FS2.rename(
      `${walDir}/000000010000000000000001`,
      `${walDir}/000000010000000000000001.done`,
    );
    const wal2Data = fillPattern(PAGE_SIZE * 2, 0x30);
    writeFile(FS2, `${walDir}/000000010000000000000002`, wal2Data);

    syncAndUnmount(FS2, t2);

    // --- Cycle 3: verify all segments survived ---
    const { FS: FS3 } = await mountTome(backend, maxPages);

    // Archived segment 000
    const seg0 = readFile(
      FS3,
      `${walDir}/000000010000000000000000.done`,
      segmentSize,
    );
    expectPattern(seg0, segmentSize, 0x10);

    // Archived segment 001: first page from cycle 1, next 2 pages from cycle 2
    const seg1Stat = FS3.stat(`${walDir}/000000010000000000000001.done`);
    expect(seg1Stat.size).toBe(segmentSize);
    const seg1 = readFile(
      FS3,
      `${walDir}/000000010000000000000001.done`,
      segmentSize,
    );
    expectPattern(seg1.subarray(0, PAGE_SIZE), PAGE_SIZE, 0x20);
    expectPattern(seg1.subarray(PAGE_SIZE), PAGE_SIZE * 2, 0x21);

    // Active segment 002
    const seg2 = readFile(
      FS3,
      `${walDir}/000000010000000000000002`,
      PAGE_SIZE * 2,
    );
    expectPattern(seg2, PAGE_SIZE * 2, 0x30);

    // Directory listing
    const entries = FS3.readdir(walDir).filter(
      (e: string) => e !== "." && e !== "..",
    );
    expect(entries.sort()).toEqual([
      "000000010000000000000000.done",
      "000000010000000000000001.done",
      "000000010000000000000002",
    ]);
  },
);

// ---------------------------------------------------------------------------
// Scenario 2: Vacuum-like Rewrite
//
// Postgres VACUUM rewrites relation pages to reclaim dead tuple space.
// It reads pages sequentially, rewrites a subset, then truncates the file
// if the tail is empty. This tests the read-scan + selective-rewrite +
// truncate pattern across persistence boundaries.
// ---------------------------------------------------------------------------

describeWithPersistence(
  "Persistence Scenario 2: Vacuum-like Rewrite",
  async (backend, maxPages) => {
    const tablePath = `${MOUNT}/base/16384/16385`;
    const totalPages = 8;
    const fileSize = totalPages * PAGE_SIZE;

    // --- Cycle 1: create a table file with 8 pages ---
    const { FS: FS1, tomefs: t1 } = await mountTome(backend, maxPages);
    FS1.mkdir(`${MOUNT}/base`);
    FS1.mkdir(`${MOUNT}/base/16384`);

    const s1 = FS1.open(tablePath, O.RDWR | O.CREAT, 0o666);
    for (let p = 0; p < totalPages; p++) {
      const data = fillPattern(PAGE_SIZE, p);
      FS1.write(s1, data, 0, PAGE_SIZE);
    }
    FS1.close(s1);
    syncAndUnmount(FS1, t1);

    // --- Cycle 2: vacuum — scan all pages, rewrite even-numbered pages,
    //     truncate last 2 pages (simulating dead space reclaim) ---
    const { FS: FS2, tomefs: t2 } = await mountTome(backend, maxPages);

    const s2 = FS2.open(tablePath, O.RDWR);
    const readBuf = new Uint8Array(PAGE_SIZE);

    // Sequential scan: read each page, rewrite even ones with new content
    for (let p = 0; p < totalPages; p++) {
      FS2.read(s2, readBuf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expectPattern(readBuf, PAGE_SIZE, p);

      if (p % 2 === 0) {
        // Rewrite this page (vacuum compaction)
        const newData = fillPattern(PAGE_SIZE, p + 100);
        FS2.write(s2, newData, 0, PAGE_SIZE, p * PAGE_SIZE);
      }
    }
    FS2.close(s2);

    // Truncate last 2 pages (dead tail reclaimed)
    const newSize = (totalPages - 2) * PAGE_SIZE;
    FS2.truncate(tablePath, newSize);

    syncAndUnmount(FS2, t2);

    // --- Cycle 3: verify vacuum results ---
    const { FS: FS3 } = await mountTome(backend, maxPages);

    const stat = FS3.stat(tablePath);
    expect(stat.size).toBe(newSize);

    const s3 = FS3.open(tablePath, O.RDONLY);
    for (let p = 0; p < totalPages - 2; p++) {
      FS3.read(s3, readBuf, 0, PAGE_SIZE, p * PAGE_SIZE);
      if (p % 2 === 0) {
        // Rewritten pages
        expectPattern(readBuf, PAGE_SIZE, p + 100);
      } else {
        // Untouched pages
        expectPattern(readBuf, PAGE_SIZE, p);
      }
    }
    FS3.close(s3);
  },
);

// ---------------------------------------------------------------------------
// Scenario 3: Multi-file Table Extension
//
// Postgres extends relation files one page at a time. With multiple tables
// being written concurrently (e.g., INSERT into one while SELECT creates
// temp results in another), the page cache faces interleaved extensions
// across files. Tests that page-at-a-time growth across multiple files
// persists correctly through checkpoints.
// ---------------------------------------------------------------------------

describeWithPersistence(
  "Persistence Scenario 3: Multi-file Table Extension @fast",
  async (backend, maxPages) => {
    const fileCount = 4;
    const pagesPerFile = 5;
    const files = Array.from(
      { length: fileCount },
      (_, i) => `${MOUNT}/rel_${i}`,
    );

    // --- Cycle 1: extend files one page at a time, interleaved ---
    const { FS: FS1, tomefs: t1 } = await mountTome(backend, maxPages);

    // Open all files
    const streams = files.map((f) =>
      FS1.open(f, O.RDWR | O.CREAT, 0o666),
    );

    // Interleaved page-at-a-time extension: round-robin across files
    for (let page = 0; page < pagesPerFile; page++) {
      for (let f = 0; f < fileCount; f++) {
        const seed = f * 100 + page;
        const data = fillPattern(PAGE_SIZE, seed);
        FS1.write(streams[f], data, 0, PAGE_SIZE);
      }
    }
    for (const s of streams) FS1.close(s);

    syncAndUnmount(FS1, t1);

    // --- Cycle 2: extend each file by 2 more pages, then checkpoint ---
    const { FS: FS2, tomefs: t2 } = await mountTome(backend, maxPages);

    for (let f = 0; f < fileCount; f++) {
      const s = FS2.open(files[f], O.WRONLY | O.APPEND);
      for (let page = pagesPerFile; page < pagesPerFile + 2; page++) {
        const seed = f * 100 + page;
        const data = fillPattern(PAGE_SIZE, seed);
        FS2.write(s, data, 0, PAGE_SIZE);
      }
      FS2.close(s);
    }

    syncAndUnmount(FS2, t2);

    // --- Cycle 3: verify all data across all files ---
    const { FS: FS3 } = await mountTome(backend, maxPages);
    const totalPages = pagesPerFile + 2;

    for (let f = 0; f < fileCount; f++) {
      const stat = FS3.stat(files[f]);
      expect(stat.size).toBe(totalPages * PAGE_SIZE);

      const s = FS3.open(files[f], O.RDONLY);
      const readBuf = new Uint8Array(PAGE_SIZE);
      for (let page = 0; page < totalPages; page++) {
        const n = FS3.read(s, readBuf, 0, PAGE_SIZE);
        expect(n).toBe(PAGE_SIZE);
        const seed = f * 100 + page;
        expectPattern(readBuf, PAGE_SIZE, seed);
      }
      FS3.close(s);
    }
  },
);

// ---------------------------------------------------------------------------
// Scenario 4: Mixed Checkpoint Workload (WAL + Heap + Temp)
//
// Realistic Postgres session: writes to a WAL file, updates heap pages
// (random write), creates and destroys temp files, all with periodic
// checkpoints. This is the most holistic scenario — combining sequential
// writes, random writes, file lifecycle, and persistence into one test.
// ---------------------------------------------------------------------------

describeWithPersistence(
  "Persistence Scenario 4: Mixed Checkpoint Workload",
  async (backend, maxPages) => {
    const walPath = `${MOUNT}/pg_wal/wal_000`;
    const heapPath = `${MOUNT}/base/heap_001`;
    const heapPages = 6;

    // --- Cycle 1: set up WAL + heap, first checkpoint ---
    const { FS: FS1, tomefs: t1 } = await mountTome(backend, maxPages);
    FS1.mkdir(`${MOUNT}/pg_wal`);
    FS1.mkdir(`${MOUNT}/base`);

    // Create heap file with 6 pages
    const heapS = FS1.open(heapPath, O.RDWR | O.CREAT, 0o666);
    for (let p = 0; p < heapPages; p++) {
      FS1.write(heapS, fillPattern(PAGE_SIZE, p), 0, PAGE_SIZE);
    }
    FS1.close(heapS);

    // Start WAL with 2 pages of records
    writeFile(FS1, walPath, fillPattern(PAGE_SIZE * 2, 0xA0));

    // Create a temp file (query result spool)
    const tmpPath = `${MOUNT}/base/t_sort_001`;
    writeFile(FS1, tmpPath, fillPattern(PAGE_SIZE, 0xF0));

    syncAndUnmount(FS1, t1);

    // --- Cycle 2: random heap updates + WAL growth + temp file lifecycle ---
    const { FS: FS2, tomefs: t2 } = await mountTome(backend, maxPages);

    // Random heap page updates (simulate UPDATE statements)
    const updatedPages = [1, 3, 5]; // specific pages to modify
    const hs = FS2.open(heapPath, O.RDWR);
    for (const p of updatedPages) {
      const newData = fillPattern(PAGE_SIZE, p + 50);
      FS2.write(hs, newData, 0, PAGE_SIZE, p * PAGE_SIZE);
    }
    FS2.close(hs);

    // Extend WAL (more records)
    appendFile(FS2, walPath, fillPattern(PAGE_SIZE, 0xA1));

    // Destroy temp file from previous cycle
    FS2.unlink(tmpPath);

    // Create new temp file
    const tmpPath2 = `${MOUNT}/base/t_hash_002`;
    writeFile(FS2, tmpPath2, fillPattern(PAGE_SIZE * 2, 0xF1));

    syncAndUnmount(FS2, t2);

    // --- Cycle 3: WAL rotation + more heap updates ---
    const { FS: FS3, tomefs: t3 } = await mountTome(backend, maxPages);

    // Rotate WAL
    FS3.rename(walPath, `${walPath}.done`);
    writeFile(FS3, walPath, fillPattern(PAGE_SIZE, 0xB0));

    // Heap: extend by 1 page + update page 0
    const hs3 = FS3.open(heapPath, O.RDWR);
    FS3.write(hs3, fillPattern(PAGE_SIZE, 200), 0, PAGE_SIZE, 0);
    FS3.llseek(hs3, 0, SEEK_END);
    FS3.write(hs3, fillPattern(PAGE_SIZE, 206), 0, PAGE_SIZE);
    FS3.close(hs3);

    // Delete temp file from cycle 2
    FS3.unlink(tmpPath2);

    syncAndUnmount(FS3, t3);

    // --- Cycle 4: verify all state ---
    const { FS: FS4 } = await mountTome(backend, maxPages);

    // Heap: 7 pages total (6 original + 1 extended)
    const heapStat = FS4.stat(heapPath);
    expect(heapStat.size).toBe((heapPages + 1) * PAGE_SIZE);

    const hs4 = FS4.open(heapPath, O.RDONLY);
    const readBuf = new Uint8Array(PAGE_SIZE);
    for (let p = 0; p <= heapPages; p++) {
      FS4.read(hs4, readBuf, 0, PAGE_SIZE, p * PAGE_SIZE);
      let expectedSeed: number;
      if (p === 0) {
        expectedSeed = 200; // Updated in cycle 3
      } else if (updatedPages.includes(p)) {
        expectedSeed = p + 50; // Updated in cycle 2
      } else if (p === heapPages) {
        expectedSeed = 206; // Extended in cycle 3
      } else {
        expectedSeed = p; // Original from cycle 1
      }
      expectPattern(readBuf, PAGE_SIZE, expectedSeed);
    }
    FS4.close(hs4);

    // Archived WAL: 3 pages from cycles 1+2
    const archivedWal = readFile(FS4, `${walPath}.done`, PAGE_SIZE * 3);
    expectPattern(archivedWal.subarray(0, PAGE_SIZE * 2), PAGE_SIZE * 2, 0xA0);
    expectPattern(
      archivedWal.subarray(PAGE_SIZE * 2),
      PAGE_SIZE,
      0xA1,
    );

    // Active WAL: 1 page from cycle 3
    const activeWal = readFile(FS4, walPath, PAGE_SIZE);
    expectPattern(activeWal, PAGE_SIZE, 0xB0);

    // Temp files should be gone
    expect(() => FS4.stat(tmpPath)).toThrow();
    expect(() => FS4.stat(tmpPath2)).toThrow();

    // Only expected files remain
    const baseEntries = FS4.readdir(`${MOUNT}/base`).filter(
      (e: string) => e !== "." && e !== "..",
    );
    expect(baseEntries.sort()).toEqual(["heap_001"]);
  },
);

// ---------------------------------------------------------------------------
// Scenario 5: Index Rebuild with Persistence
//
// Postgres REINDEX reads all heap pages to extract index keys, builds the
// new index in bulk (sequential writes), then drops the old index file.
// This tests the full-scan-read + bulk-write + delete-old pattern.
// ---------------------------------------------------------------------------

describeWithPersistence(
  "Persistence Scenario 5: Index Rebuild",
  async (backend, maxPages) => {
    const heapPath = `${MOUNT}/heap`;
    const oldIdxPath = `${MOUNT}/idx_old`;
    const newIdxPath = `${MOUNT}/idx_new`;
    const heapPages = 6;

    // --- Cycle 1: create heap + old index ---
    const { FS: FS1, tomefs: t1 } = await mountTome(backend, maxPages);

    const hs = FS1.open(heapPath, O.RDWR | O.CREAT, 0o666);
    for (let p = 0; p < heapPages; p++) {
      FS1.write(hs, fillPattern(PAGE_SIZE, p), 0, PAGE_SIZE);
    }
    FS1.close(hs);

    // Old index: 3 pages
    writeFile(FS1, oldIdxPath, fillPattern(PAGE_SIZE * 3, 0x80));

    syncAndUnmount(FS1, t1);

    // --- Cycle 2: REINDEX — scan heap, build new index, drop old ---
    const { FS: FS2, tomefs: t2 } = await mountTome(backend, maxPages);

    // Full heap scan (read all pages sequentially)
    const scanStream = FS2.open(heapPath, O.RDONLY);
    const scanBuf = new Uint8Array(PAGE_SIZE);
    for (let p = 0; p < heapPages; p++) {
      const n = FS2.read(scanStream, scanBuf, 0, PAGE_SIZE);
      expect(n).toBe(PAGE_SIZE);
      expectPattern(scanBuf, PAGE_SIZE, p);
    }
    FS2.close(scanStream);

    // Build new index from scan results (4 pages — different from old)
    const newIdxStream = FS2.open(newIdxPath, O.RDWR | O.CREAT, 0o666);
    for (let p = 0; p < 4; p++) {
      FS2.write(newIdxStream, fillPattern(PAGE_SIZE, 0x90 + p), 0, PAGE_SIZE);
    }
    FS2.close(newIdxStream);

    // Drop old index
    FS2.unlink(oldIdxPath);

    syncAndUnmount(FS2, t2);

    // --- Cycle 3: verify ---
    const { FS: FS3 } = await mountTome(backend, maxPages);

    // Heap unchanged
    const verifyStream = FS3.open(heapPath, O.RDONLY);
    for (let p = 0; p < heapPages; p++) {
      FS3.read(verifyStream, scanBuf, 0, PAGE_SIZE);
      expectPattern(scanBuf, PAGE_SIZE, p);
    }
    FS3.close(verifyStream);

    // New index present with correct data
    const idxStat = FS3.stat(newIdxPath);
    expect(idxStat.size).toBe(PAGE_SIZE * 4);
    const idxStream = FS3.open(newIdxPath, O.RDONLY);
    for (let p = 0; p < 4; p++) {
      FS3.read(idxStream, scanBuf, 0, PAGE_SIZE);
      expectPattern(scanBuf, PAGE_SIZE, 0x90 + p);
    }
    FS3.close(idxStream);

    // Old index gone
    expect(() => FS3.stat(oldIdxPath)).toThrow();
  },
);

// ---------------------------------------------------------------------------
// Scenario 6: Multi-cycle Incremental Updates
//
// Simulates a long-running application that accumulates state across many
// checkpoint cycles. Each cycle modifies a subset of pages and adds new
// files. Tests that incremental changes compose correctly across 5+ cycles.
// ---------------------------------------------------------------------------

describeWithPersistence(
  "Persistence Scenario 6: Multi-cycle Incremental Updates @fast",
  async (backend, maxPages) => {
    const cycles = 5;
    const basePath = `${MOUNT}/data`;
    // Track expected state: file -> { size, seeds: pageIndex -> seed }
    const expectedState = new Map<
      string,
      { size: number; seeds: Map<number, number> }
    >();

    for (let cycle = 0; cycle < cycles; cycle++) {
      const { FS, tomefs } = await mountTome(backend, maxPages);

      if (cycle === 0) {
        FS.mkdir(basePath);
      }

      // Each cycle: create one new file and modify one page in each existing file
      const newFile = `${basePath}/file_${cycle}`;
      const newFilePages = 3;
      const ns = FS.open(newFile, O.RDWR | O.CREAT, 0o666);
      const seeds = new Map<number, number>();
      for (let p = 0; p < newFilePages; p++) {
        const seed = cycle * 1000 + p;
        FS.write(ns, fillPattern(PAGE_SIZE, seed), 0, PAGE_SIZE);
        seeds.set(p, seed);
      }
      FS.close(ns);
      expectedState.set(newFile, {
        size: newFilePages * PAGE_SIZE,
        seeds,
      });

      // Modify page 1 of each existing file (except the one just created)
      for (const [path, state] of expectedState) {
        if (path === newFile) continue;
        const modSeed = cycle * 1000 + 900 + expectedState.size;
        const ms = FS.open(path, O.RDWR);
        FS.write(ms, fillPattern(PAGE_SIZE, modSeed), 0, PAGE_SIZE, PAGE_SIZE);
        FS.close(ms);
        state.seeds.set(1, modSeed);
      }

      syncAndUnmount(FS, tomefs);
    }

    // Final verification cycle
    const { FS } = await mountTome(backend, maxPages);
    const readBuf = new Uint8Array(PAGE_SIZE);

    for (const [path, state] of expectedState) {
      const stat = FS.stat(path);
      expect(stat.size).toBe(state.size);

      const s = FS.open(path, O.RDONLY);
      const pageCount = state.size / PAGE_SIZE;
      for (let p = 0; p < pageCount; p++) {
        FS.read(s, readBuf, 0, PAGE_SIZE, p * PAGE_SIZE);
        expectPattern(readBuf, PAGE_SIZE, state.seeds.get(p)!);
      }
      FS.close(s);
    }
  },
);

// ---------------------------------------------------------------------------
// Scenario 7: B-tree Index Random Updates with Persistence
//
// Simulates a Postgres B-tree index across multiple checkpoint cycles.
// Each cycle performs random read-modify-write operations on index pages
// (simulating INSERT/UPDATE/DELETE modifying leaf pages), then checkpoints.
// The random scattered dirty pages stress the cache eviction and flush
// path differently from sequential patterns — dirty pages are spread
// across the LRU chain rather than clustered at the tail.
// ---------------------------------------------------------------------------

describeWithPersistence(
  "Persistence Scenario 7: B-tree Index Random Updates",
  async (backend, maxPages) => {
    const indexPages = 20;
    const indexSize = indexPages * PAGE_SIZE;
    const indexPath = `${MOUNT}/idx_btree`;
    const cycles = 4;
    const updatesPerCycle = 15;

    // Track expected page headers (first 64 bytes of each page)
    // null = unmodified (original pattern), number = modification seed
    const expectedHeaders: (number | null)[] = new Array(indexPages).fill(null);

    // --- Cycle 0: create the index file ---
    {
      const { FS, tomefs } = await mountTome(backend, maxPages);
      const s = FS.open(indexPath, O.RDWR | O.CREAT, 0o666);
      for (let p = 0; p < indexPages; p++) {
        FS.write(s, fillPattern(PAGE_SIZE, p), 0, PAGE_SIZE);
      }
      FS.close(s);
      syncAndUnmount(FS, tomefs);
    }

    // --- Cycles 1-N: random read-modify-write + checkpoint ---
    for (let cycle = 1; cycle <= cycles; cycle++) {
      const { FS, tomefs } = await mountTome(backend, maxPages);

      const s = FS.open(indexPath, O.RDWR);
      const buf = new Uint8Array(PAGE_SIZE);

      for (let u = 0; u < updatesPerCycle; u++) {
        // Deterministic random page selection (root→internal→leaf traversal)
        const root = 0;
        const leaf = 1 + ((cycle * 37 + u * 13 + 7) % (indexPages - 1));

        // Read root (access pattern, no modify)
        FS.read(s, buf, 0, PAGE_SIZE, root * PAGE_SIZE);

        // Read leaf page
        FS.read(s, buf, 0, PAGE_SIZE, leaf * PAGE_SIZE);

        // Verify leaf has expected content
        const prevHeader = expectedHeaders[leaf];
        if (prevHeader !== null) {
          for (let i = 0; i < 64; i++) {
            if (buf[i] !== ((prevHeader + i * 37) & 0xff)) {
              throw new Error(
                `Cycle ${cycle} update ${u}: leaf ${leaf} header mismatch at byte ${i}`,
              );
            }
          }
        } else {
          const orig = fillPattern(PAGE_SIZE, leaf);
          for (let i = 0; i < 64; i++) {
            if (buf[i] !== orig[i]) {
              throw new Error(
                `Cycle ${cycle} update ${u}: leaf ${leaf} original data mismatch at byte ${i}`,
              );
            }
          }
        }

        // Modify leaf header (simulating index tuple insert)
        const modSeed = cycle * 10000 + u * 100 + leaf;
        for (let i = 0; i < 64; i++) {
          buf[i] = (modSeed + i * 37) & 0xff;
        }
        FS.write(s, buf, 0, PAGE_SIZE, leaf * PAGE_SIZE);
        expectedHeaders[leaf] = modSeed;
      }

      FS.close(s);
      syncAndUnmount(FS, tomefs);
    }

    // --- Final verification: all pages must match expected state ---
    const { FS: FSv } = await mountTome(backend, maxPages);
    const stat = FSv.stat(indexPath);
    expect(stat.size).toBe(indexSize);

    const vs = FSv.open(indexPath, O.RDONLY);
    const vbuf = new Uint8Array(PAGE_SIZE);

    for (let p = 0; p < indexPages; p++) {
      FSv.read(vs, vbuf, 0, PAGE_SIZE, p * PAGE_SIZE);
      const orig = fillPattern(PAGE_SIZE, p);

      const modSeed = expectedHeaders[p];
      if (modSeed !== null) {
        // Header was modified — check modification
        for (let i = 0; i < 64; i++) {
          if (vbuf[i] !== ((modSeed + i * 37) & 0xff)) {
            throw new Error(
              `Final verify page ${p} header byte ${i}: ` +
              `expected ${(modSeed + i * 37) & 0xff}, got ${vbuf[i]}`,
            );
          }
        }
        // Rest of page should be original
        for (let i = 64; i < PAGE_SIZE; i++) {
          if (vbuf[i] !== orig[i]) {
            throw new Error(
              `Final verify page ${p} body byte ${i}: ` +
              `expected ${orig[i]}, got ${vbuf[i]}`,
            );
          }
        }
      } else {
        // Unmodified page — check full original
        for (let i = 0; i < PAGE_SIZE; i++) {
          if (vbuf[i] !== orig[i]) {
            throw new Error(
              `Final verify page ${p} byte ${i}: expected ${orig[i]}, got ${vbuf[i]}`,
            );
          }
        }
      }
    }
    FSv.close(vs);
  },
);

// ---------------------------------------------------------------------------
// Scenario 8: Heap + Index Concurrent Modification with Persistence
//
// Postgres UPDATEs modify both heap pages (tuple data) and index pages
// (index entries) in the same transaction. When both files compete for
// cache slots under pressure, dirty pages from one file may be evicted
// to make room for the other. This tests that interleaved modifications
// to two files persist correctly through checkpoint cycles.
// ---------------------------------------------------------------------------

describeWithPersistence(
  "Persistence Scenario 8: Heap + Index Concurrent Modification @fast",
  async (backend, maxPages) => {
    const heapPath = `${MOUNT}/heap`;
    const idxPath = `${MOUNT}/idx`;
    const heapPages = 8;
    const idxPages = 6;

    // Track expected state per page per file
    const heapSeeds = new Map<number, number>();
    const idxSeeds = new Map<number, number>();

    // --- Cycle 0: create both files ---
    {
      const { FS, tomefs } = await mountTome(backend, maxPages);

      const hs = FS.open(heapPath, O.RDWR | O.CREAT, 0o666);
      for (let p = 0; p < heapPages; p++) {
        FS.write(hs, fillPattern(PAGE_SIZE, p), 0, PAGE_SIZE);
        heapSeeds.set(p, p);
      }
      FS.close(hs);

      const is = FS.open(idxPath, O.RDWR | O.CREAT, 0o666);
      for (let p = 0; p < idxPages; p++) {
        const seed = p + 100;
        FS.write(is, fillPattern(PAGE_SIZE, seed), 0, PAGE_SIZE);
        idxSeeds.set(p, seed);
      }
      FS.close(is);

      syncAndUnmount(FS, tomefs);
    }

    // --- Cycles 1-3: interleaved heap + index modifications ---
    for (let cycle = 1; cycle <= 3; cycle++) {
      const { FS, tomefs } = await mountTome(backend, maxPages);

      const hs = FS.open(heapPath, O.RDWR);
      const is = FS.open(idxPath, O.RDWR);

      // Simulate 5 UPDATE operations per cycle
      for (let op = 0; op < 5; op++) {
        // Modify a heap page
        const hp = (cycle * 3 + op * 2) % heapPages;
        const hSeed = cycle * 1000 + op * 10;
        FS.write(hs, fillPattern(PAGE_SIZE, hSeed), 0, PAGE_SIZE, hp * PAGE_SIZE);
        heapSeeds.set(hp, hSeed);

        // Modify corresponding index page
        const ip = (cycle * 2 + op) % idxPages;
        const iSeed = cycle * 1000 + op * 10 + 500;
        FS.write(is, fillPattern(PAGE_SIZE, iSeed), 0, PAGE_SIZE, ip * PAGE_SIZE);
        idxSeeds.set(ip, iSeed);
      }

      FS.close(hs);
      FS.close(is);
      syncAndUnmount(FS, tomefs);
    }

    // --- Final verification ---
    const { FS: FSv } = await mountTome(backend, maxPages);
    const readBuf = new Uint8Array(PAGE_SIZE);

    // Verify heap
    const hvs = FSv.open(heapPath, O.RDONLY);
    for (let p = 0; p < heapPages; p++) {
      FSv.read(hvs, readBuf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expectPattern(readBuf, PAGE_SIZE, heapSeeds.get(p)!);
    }
    FSv.close(hvs);

    // Verify index
    const ivs = FSv.open(idxPath, O.RDONLY);
    for (let p = 0; p < idxPages; p++) {
      FSv.read(ivs, readBuf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expectPattern(readBuf, PAGE_SIZE, idxSeeds.get(p)!);
    }
    FSv.close(ivs);
  },
);

// ---------------------------------------------------------------------------
// Scenario 9: WAL Segment Recycling
//
// Postgres reuses WAL segment files instead of creating new ones. After a
// checkpoint consumes a WAL segment, Postgres renames the archived segment
// to a new segment number (recycling), truncates it, and writes fresh data.
// This differs from WAL rotation (Scenario 1): rotation archives segments
// with a .done suffix; recycling reuses the FILE itself for a new segment.
//
// The recycling pattern exercises a critical interaction chain:
//   1. rename(old_segment, new_segment) — page cache moves pages to new path
//   2. truncate(new_segment, 0) — page cache deletes ALL old pages
//   3. write(new_segment, new_data) — page cache adds new pages at same path
//   4. syncfs — persist new data under the new segment name
//   5. remount — verify new data survives, old data is gone
//
// A bug in any step can cause data corruption: stale pages from the old
// segment surviving the rename+truncate, or new pages being keyed under
// the wrong storage path after rename.
// ---------------------------------------------------------------------------

describeWithPersistence(
  "Persistence Scenario 9: WAL Segment Recycling @fast",
  async (backend, maxPages) => {
    const walDir = `${MOUNT}/pg_wal`;
    const segmentSize = PAGE_SIZE * 4; // 32 KB per WAL segment

    // --- Cycle 1: create 3 WAL segments and fill them ---
    const { FS: FS1, tomefs: t1 } = await mountTome(backend, maxPages);
    FS1.mkdir(walDir);

    for (let seg = 0; seg < 3; seg++) {
      writeFile(FS1, `${walDir}/seg_${seg}`, fillPattern(segmentSize, seg + 1));
    }
    syncAndUnmount(FS1, t1);

    // --- Cycle 2: "checkpoint" consumes seg_0 and seg_1; recycle them ---
    const { FS: FS2, tomefs: t2 } = await mountTome(backend, maxPages);

    // Verify all 3 segments survived remount
    for (let seg = 0; seg < 3; seg++) {
      const data = readFile(FS2, `${walDir}/seg_${seg}`, segmentSize);
      expectPattern(data, segmentSize, seg + 1);
    }

    // Recycle seg_0 → seg_3: rename, truncate, write new data
    FS2.rename(`${walDir}/seg_0`, `${walDir}/seg_3`);
    FS2.truncate(`${walDir}/seg_3`, 0);
    writeFile(FS2, `${walDir}/seg_3`, fillPattern(segmentSize, 0x30));

    // Recycle seg_1 → seg_4: rename, truncate, write new data
    FS2.rename(`${walDir}/seg_1`, `${walDir}/seg_4`);
    FS2.truncate(`${walDir}/seg_4`, 0);
    writeFile(FS2, `${walDir}/seg_4`, fillPattern(segmentSize, 0x40));

    // seg_2 remains active (current WAL segment), extend it
    appendFile(FS2, `${walDir}/seg_2`, fillPattern(PAGE_SIZE, 0x2E));

    syncAndUnmount(FS2, t2);

    // --- Cycle 3: verify recycled segments have NEW data ---
    const { FS: FS3, tomefs: t3 } = await mountTome(backend, maxPages);

    // seg_0 and seg_1 should be GONE (recycled into seg_3 and seg_4)
    expect(() => FS3.stat(`${walDir}/seg_0`)).toThrow();
    expect(() => FS3.stat(`${walDir}/seg_1`)).toThrow();

    // seg_2 should be extended (original 4 pages + 1 appended)
    const seg2 = readFile(FS3, `${walDir}/seg_2`, segmentSize + PAGE_SIZE);
    expectPattern(seg2.subarray(0, segmentSize), segmentSize, 3);
    expectPattern(seg2.subarray(segmentSize), PAGE_SIZE, 0x2E);

    // seg_3 should have NEW data from cycle 2, NOT old seg_0 data
    const seg3 = readFile(FS3, `${walDir}/seg_3`, segmentSize);
    expectPattern(seg3, segmentSize, 0x30);

    // seg_4 should have NEW data from cycle 2, NOT old seg_1 data
    const seg4 = readFile(FS3, `${walDir}/seg_4`, segmentSize);
    expectPattern(seg4, segmentSize, 0x40);

    // --- Cycle 3 continued: recycle seg_2 → seg_5, create seg_6 ---
    FS3.rename(`${walDir}/seg_2`, `${walDir}/seg_5`);
    FS3.truncate(`${walDir}/seg_5`, 0);
    writeFile(FS3, `${walDir}/seg_5`, fillPattern(PAGE_SIZE * 2, 0x50));

    // Create a brand new segment (not recycled)
    writeFile(FS3, `${walDir}/seg_6`, fillPattern(PAGE_SIZE * 3, 0x60));

    syncAndUnmount(FS3, t3);

    // --- Cycle 4: final verification across all surviving segments ---
    const { FS: FS4 } = await mountTome(backend, maxPages);

    const entries = FS4.readdir(walDir)
      .filter((e: string) => e !== "." && e !== "..")
      .sort();
    expect(entries).toEqual(["seg_3", "seg_4", "seg_5", "seg_6"]);

    // Verify each segment has correct data
    const seg3v = readFile(FS4, `${walDir}/seg_3`, segmentSize);
    expectPattern(seg3v, segmentSize, 0x30);

    const seg4v = readFile(FS4, `${walDir}/seg_4`, segmentSize);
    expectPattern(seg4v, segmentSize, 0x40);

    const seg5v = readFile(FS4, `${walDir}/seg_5`, PAGE_SIZE * 2);
    expectPattern(seg5v, PAGE_SIZE * 2, 0x50);

    const seg6v = readFile(FS4, `${walDir}/seg_6`, PAGE_SIZE * 3);
    expectPattern(seg6v, PAGE_SIZE * 3, 0x60);
  },
);

// ---------------------------------------------------------------------------
// Scenario 10: WAL Pre-allocation + Recycling Under Cache Pressure
//
// Postgres pre-allocates WAL segments via posix_fallocate() to avoid
// filesystem allocation latency during critical WAL writes. The pre-
// allocated segment is a sparse file filled with zeros. After filling,
// the segment is checkpointed and recycled. Under small cache sizes,
// the pre-allocation can thrash the cache if it materializes too many
// pages (the allocate() optimization in tomefs only materializes the
// LAST page as a sentinel — see resizeFileStorage comments).
//
// This test verifies:
//   1. allocate() correctly extends file size without data corruption
//   2. Pre-allocated regions read as zeros (POSIX guarantee)
//   3. Writes into pre-allocated space work correctly
//   4. Recycling a pre-allocated+filled segment preserves new data
//   5. All of the above survives syncfs + remount at each cache size
// ---------------------------------------------------------------------------

describeWithPersistence(
  "Persistence Scenario 10: WAL Pre-allocation + Recycling",
  async (backend, maxPages) => {
    const walDir = `${MOUNT}/pg_wal`;
    const preAllocSize = PAGE_SIZE * 6; // 48 KB pre-allocated segment

    // --- Cycle 1: pre-allocate, then fill partially ---
    const { FS: FS1, tomefs: t1 } = await mountTome(backend, maxPages);
    FS1.mkdir(walDir);

    // Pre-allocate a WAL segment (posix_fallocate equivalent)
    const stream1 = FS1.open(
      `${walDir}/seg_000`,
      O.RDWR | O.CREAT,
      0o666,
    );
    stream1.stream_ops.allocate(stream1, 0, preAllocSize);

    // Verify size is extended
    const stat1 = FS1.fstat(stream1.fd);
    expect(stat1.size).toBe(preAllocSize);

    // Verify pre-allocated region reads as zeros
    const zeroBuf = new Uint8Array(PAGE_SIZE);
    for (let p = 0; p < 6; p++) {
      const n = FS1.read(stream1, zeroBuf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expect(n).toBe(PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) {
        if (zeroBuf[i] !== 0) {
          throw new Error(
            `Pre-allocated page ${p} byte ${i} is ${zeroBuf[i]}, expected 0`,
          );
        }
      }
    }

    // Write WAL records into first 3 pages (partially filling the segment)
    for (let p = 0; p < 3; p++) {
      FS1.write(
        stream1,
        fillPattern(PAGE_SIZE, 0xA0 + p),
        0,
        PAGE_SIZE,
        p * PAGE_SIZE,
      );
    }
    FS1.close(stream1);

    syncAndUnmount(FS1, t1);

    // --- Cycle 2: verify partial fill + continue filling + recycle ---
    const { FS: FS2, tomefs: t2 } = await mountTome(backend, maxPages);

    // Verify the segment size and partial fill survived remount
    const stat2 = FS2.stat(`${walDir}/seg_000`);
    expect(stat2.size).toBe(preAllocSize);

    const stream2 = FS2.open(`${walDir}/seg_000`, O.RDWR);
    const readBuf = new Uint8Array(PAGE_SIZE);

    // First 3 pages have written data
    for (let p = 0; p < 3; p++) {
      FS2.read(stream2, readBuf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expectPattern(readBuf, PAGE_SIZE, 0xA0 + p);
    }

    // Last 3 pages should still be zero (pre-allocated but unwritten)
    for (let p = 3; p < 6; p++) {
      FS2.read(stream2, readBuf, 0, PAGE_SIZE, p * PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) {
        if (readBuf[i] !== 0) {
          throw new Error(
            `Post-remount pre-allocated page ${p} byte ${i} is ${readBuf[i]}, expected 0`,
          );
        }
      }
    }

    // Fill remaining 3 pages
    for (let p = 3; p < 6; p++) {
      FS2.write(
        stream2,
        fillPattern(PAGE_SIZE, 0xB0 + p),
        0,
        PAGE_SIZE,
        p * PAGE_SIZE,
      );
    }
    FS2.close(stream2);

    // Now recycle seg_000 → seg_001: rename, truncate, pre-allocate, fill
    FS2.rename(`${walDir}/seg_000`, `${walDir}/seg_001`);
    FS2.truncate(`${walDir}/seg_001`, 0);

    // Pre-allocate the recycled segment (same size)
    const stream2b = FS2.open(`${walDir}/seg_001`, O.RDWR);
    stream2b.stream_ops.allocate(stream2b, 0, preAllocSize);

    // Partially fill with different data
    for (let p = 0; p < 2; p++) {
      FS2.write(
        stream2b,
        fillPattern(PAGE_SIZE, 0xC0 + p),
        0,
        PAGE_SIZE,
        p * PAGE_SIZE,
      );
    }
    FS2.close(stream2b);

    syncAndUnmount(FS2, t2);

    // --- Cycle 3: verify recycled+pre-allocated segment ---
    const { FS: FS3 } = await mountTome(backend, maxPages);

    // seg_000 should be gone
    expect(() => FS3.stat(`${walDir}/seg_000`)).toThrow();

    // seg_001 should exist with pre-allocated size
    const stat3 = FS3.stat(`${walDir}/seg_001`);
    expect(stat3.size).toBe(preAllocSize);

    const stream3 = FS3.open(`${walDir}/seg_001`, O.RDONLY);

    // First 2 pages have new data (from cycle 2 after recycling)
    for (let p = 0; p < 2; p++) {
      FS3.read(stream3, readBuf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expectPattern(readBuf, PAGE_SIZE, 0xC0 + p);
    }

    // Pages 2-5 should be zeros (pre-allocated but unwritten after recycle)
    for (let p = 2; p < 6; p++) {
      FS3.read(stream3, readBuf, 0, PAGE_SIZE, p * PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) {
        if (readBuf[i] !== 0) {
          throw new Error(
            `Final verify recycled page ${p} byte ${i} is ${readBuf[i]}, expected 0`,
          );
        }
      }
    }
    FS3.close(stream3);

    // Directory should only contain seg_001
    const entries = FS3.readdir(walDir)
      .filter((e: string) => e !== "." && e !== "..")
      .sort();
    expect(entries).toEqual(["seg_001"]);
  },
);

// ---------------------------------------------------------------------------
// Scenario 11: VACUUM FULL — Complete Heap Rewrite via Temp File
//
// Unlike regular VACUUM (Scenario 2) which rewrites pages in-place,
// VACUUM FULL creates a new heap file, copies all live tuples to it in a
// compacted layout, then renames the new file over the original. This
// exercises the critical rename-over-existing path with multi-page files
// under cache pressure: both old and new files compete for cache slots
// during the copy phase, and the rename must correctly re-key all pages.
//
// The pattern: open old → create temp → sequential copy with compaction →
// close both → rename temp over original → syncfs → remount → verify.
//
// This targets bugs in:
// - pageCache.renameFile with destination eviction under cache pressure
// - Backend page re-keying when both source and destination have pages
// - restoreTree size recovery after rename + persist
// - Dirty page tracking across rename boundaries
// ---------------------------------------------------------------------------

describeWithPersistence(
  "Persistence Scenario 11: VACUUM FULL @fast",
  async (backend, maxPages) => {
    const basePath = `${MOUNT}/base/16384`;
    const heapPath = `${basePath}/16400`;
    const toastPath = `${basePath}/16400_toast`;
    const tempPath = `${basePath}/16400_vm_tmp`;
    const totalPages = 12;
    const readBuf = new Uint8Array(PAGE_SIZE);

    // --- Cycle 1: create a heap with 12 pages of known data + TOAST ---
    const { FS: FS1, tomefs: t1 } = await mountTome(backend, maxPages);
    FS1.mkdir(`${MOUNT}/base`);
    FS1.mkdir(basePath);

    const s1 = FS1.open(heapPath, O.RDWR | O.CREAT, 0o666);
    for (let p = 0; p < totalPages; p++) {
      FS1.write(s1, fillPattern(PAGE_SIZE, p), 0, PAGE_SIZE);
    }
    FS1.close(s1);

    // TOAST table with 4 pages
    const toast1 = FS1.open(toastPath, O.RDWR | O.CREAT, 0o666);
    for (let p = 0; p < 4; p++) {
      FS1.write(toast1, fillPattern(PAGE_SIZE, 0xA0 + p), 0, PAGE_SIZE);
    }
    FS1.close(toast1);

    syncAndUnmount(FS1, t1);

    // --- Cycle 2: VACUUM FULL — rebuild the heap into a temp file ---
    const { FS: FS2, tomefs: t2 } = await mountTome(backend, maxPages);

    // Read back old heap to verify persistence from cycle 1
    const oldHeap = FS2.open(heapPath, O.RDONLY);
    for (let p = 0; p < totalPages; p++) {
      FS2.read(oldHeap, readBuf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expectPattern(readBuf, PAGE_SIZE, p);
    }

    // Create temp file and copy live tuples (skip pages 3, 7, 11 = dead)
    const tempFile = FS2.open(tempPath, O.RDWR | O.CREAT, 0o666);
    let outPage = 0;
    for (let p = 0; p < totalPages; p++) {
      if (p === 3 || p === 7 || p === 11) continue; // dead tuples
      FS2.read(oldHeap, readBuf, 0, PAGE_SIZE, p * PAGE_SIZE);
      // Write with compacted seed to distinguish from original
      const compacted = fillPattern(PAGE_SIZE, p + 0x40);
      FS2.write(tempFile, compacted, 0, PAGE_SIZE, outPage * PAGE_SIZE);
      outPage++;
    }
    FS2.close(oldHeap);
    FS2.close(tempFile);

    // Rename temp over original (the VACUUM FULL atomic swap)
    FS2.rename(tempPath, heapPath);

    // Verify TOAST is untouched after heap rename
    const toast2 = FS2.open(toastPath, O.RDONLY);
    for (let p = 0; p < 4; p++) {
      FS2.read(toast2, readBuf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expectPattern(readBuf, PAGE_SIZE, 0xA0 + p);
    }
    FS2.close(toast2);

    syncAndUnmount(FS2, t2);

    // --- Cycle 3: verify VACUUM FULL results survived persistence ---
    const { FS: FS3 } = await mountTome(backend, maxPages);

    // Heap should now have 9 compacted pages (12 - 3 dead)
    const stat = FS3.stat(heapPath);
    expect(stat.size).toBe(9 * PAGE_SIZE);

    const s3 = FS3.open(heapPath, O.RDONLY);
    let expectedPage = 0;
    for (let origPage = 0; origPage < totalPages; origPage++) {
      if (origPage === 3 || origPage === 7 || origPage === 11) continue;
      FS3.read(s3, readBuf, 0, PAGE_SIZE, expectedPage * PAGE_SIZE);
      expectPattern(readBuf, PAGE_SIZE, origPage + 0x40);
      expectedPage++;
    }
    FS3.close(s3);

    // TOAST should still be intact
    const toast3 = FS3.open(toastPath, O.RDONLY);
    for (let p = 0; p < 4; p++) {
      FS3.read(toast3, readBuf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expectPattern(readBuf, PAGE_SIZE, 0xA0 + p);
    }
    FS3.close(toast3);

    // Temp file should be gone
    expect(() => FS3.stat(tempPath)).toThrow();
  },
);

// ---------------------------------------------------------------------------
// Scenario 12: COPY Bulk Load — Sequential Write Exceeding Cache
//
// Postgres COPY writes tuples sequentially into heap pages, extending the
// file one page at a time. For large imports, the total data far exceeds
// the page cache capacity, forcing eviction of earlier pages while later
// pages are still being written. After the COPY, an index is built by
// scanning the heap (re-reading evicted pages) and writing index pages.
//
// This exercises:
// - Sequential writes past cache capacity (eviction during write)
// - Re-reading evicted pages (cache miss → backend read after eviction)
// - Two files (heap + index) competing for cache slots
// - Persistence of both files after the combined operation
// ---------------------------------------------------------------------------

describeWithPersistence(
  "Persistence Scenario 12: COPY Bulk Load + Index Build",
  async (backend, maxPages) => {
    const basePath = `${MOUNT}/base/16384`;
    const heapPath = `${basePath}/16390`;
    const indexPath = `${basePath}/16390_idx`;
    // 24 pages = 192 KB. With tiny cache (4 pages), this forces 6x cache
    // rotations during the sequential write. With small cache (16 pages),
    // still forces 1.5x rotation.
    const heapPages = 24;
    const readBuf = new Uint8Array(PAGE_SIZE);

    // --- Cycle 1: COPY bulk load (sequential write) + index build ---
    const { FS: FS1, tomefs: t1 } = await mountTome(backend, maxPages);
    FS1.mkdir(`${MOUNT}/base`);
    FS1.mkdir(basePath);

    // Simulate COPY: write pages sequentially
    const heap1 = FS1.open(heapPath, O.RDWR | O.CREAT, 0o666);
    for (let p = 0; p < heapPages; p++) {
      FS1.write(heap1, fillPattern(PAGE_SIZE, p), 0, PAGE_SIZE);
    }
    FS1.close(heap1);

    // Build index: scan heap (forces re-read of evicted pages) and
    // write index entries. Each index page references 4 heap pages.
    const indexPages = Math.ceil(heapPages / 4);
    const idx1 = FS1.open(indexPath, O.RDWR | O.CREAT, 0o666);
    const heapScan = FS1.open(heapPath, O.RDONLY);

    for (let ip = 0; ip < indexPages; ip++) {
      // Read 4 heap pages to "scan" them
      for (let h = 0; h < 4 && (ip * 4 + h) < heapPages; h++) {
        const heapPageIdx = ip * 4 + h;
        FS1.read(heapScan, readBuf, 0, PAGE_SIZE, heapPageIdx * PAGE_SIZE);
        expectPattern(readBuf, PAGE_SIZE, heapPageIdx);
      }
      // Write one index page (derived from heap data)
      FS1.write(idx1, fillPattern(PAGE_SIZE, 0xE0 + ip), 0, PAGE_SIZE);
    }
    FS1.close(heapScan);
    FS1.close(idx1);

    syncAndUnmount(FS1, t1);

    // --- Cycle 2: verify bulk load survived persistence ---
    const { FS: FS2, tomefs: t2 } = await mountTome(backend, maxPages);

    // Verify heap
    const heapStat = FS2.stat(heapPath);
    expect(heapStat.size).toBe(heapPages * PAGE_SIZE);

    const heap2 = FS2.open(heapPath, O.RDONLY);
    for (let p = 0; p < heapPages; p++) {
      FS2.read(heap2, readBuf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expectPattern(readBuf, PAGE_SIZE, p);
    }
    FS2.close(heap2);

    // Verify index
    const idxStat = FS2.stat(indexPath);
    expect(idxStat.size).toBe(indexPages * PAGE_SIZE);

    const idx2 = FS2.open(indexPath, O.RDONLY);
    for (let ip = 0; ip < indexPages; ip++) {
      FS2.read(idx2, readBuf, 0, PAGE_SIZE, ip * PAGE_SIZE);
      expectPattern(readBuf, PAGE_SIZE, 0xE0 + ip);
    }
    FS2.close(idx2);

    // --- Update via positioned writes (simulating UPDATEs after COPY) ---
    // Modify pages 5 and 15 (scattered within the heap)
    const heap2w = FS2.open(heapPath, O.RDWR);
    FS2.write(heap2w, fillPattern(PAGE_SIZE, 0xF5), 0, PAGE_SIZE, 5 * PAGE_SIZE);
    FS2.write(heap2w, fillPattern(PAGE_SIZE, 0xFF), 0, PAGE_SIZE, 15 * PAGE_SIZE);
    FS2.close(heap2w);

    syncAndUnmount(FS2, t2);

    // --- Cycle 3: verify updates persisted correctly ---
    const { FS: FS3 } = await mountTome(backend, maxPages);

    const heap3 = FS3.open(heapPath, O.RDONLY);
    for (let p = 0; p < heapPages; p++) {
      FS3.read(heap3, readBuf, 0, PAGE_SIZE, p * PAGE_SIZE);
      if (p === 5) {
        expectPattern(readBuf, PAGE_SIZE, 0xF5);
      } else if (p === 15) {
        expectPattern(readBuf, PAGE_SIZE, 0xFF);
      } else {
        expectPattern(readBuf, PAGE_SIZE, p);
      }
    }
    FS3.close(heap3);

    // Index should be unchanged
    const idx3 = FS3.open(indexPath, O.RDONLY);
    for (let ip = 0; ip < indexPages; ip++) {
      FS3.read(idx3, readBuf, 0, PAGE_SIZE, ip * PAGE_SIZE);
      expectPattern(readBuf, PAGE_SIZE, 0xE0 + ip);
    }
    FS3.close(idx3);
  },
);

// ---------------------------------------------------------------------------
// Scenario 13: CREATE INDEX CONCURRENTLY — Multi-phase Index Build
//
// CREATE INDEX CONCURRENTLY builds the index in multiple phases:
// 1. Sequential heap scan → write sorted entries to temp file
// 2. Merge temp file into final index structure
// 3. Rename temp to final index path
// Each phase involves different I/O patterns competing for cache slots.
// With a tiny cache, the heap scan evicts index pages and vice versa.
//
// This exercises:
// - Three files (heap, temp, final index) competing for cache
// - Read + write interleaving across files under extreme pressure
// - Rename of a multi-page file to a new path (not over existing)
// - Persistence after a complex multi-file operation
// ---------------------------------------------------------------------------

describeWithPersistence(
  "Persistence Scenario 13: CREATE INDEX CONCURRENTLY @fast",
  async (backend, maxPages) => {
    const basePath = `${MOUNT}/base/16384`;
    const heapPath = `${basePath}/16395`;
    const tempIdxPath = `${basePath}/16395_idx_tmp`;
    const finalIdxPath = `${basePath}/16395_pkey`;
    const heapPages = 16;
    const readBuf = new Uint8Array(PAGE_SIZE);

    // --- Cycle 1: create heap table ---
    const { FS: FS1, tomefs: t1 } = await mountTome(backend, maxPages);
    FS1.mkdir(`${MOUNT}/base`);
    FS1.mkdir(basePath);

    const s1 = FS1.open(heapPath, O.RDWR | O.CREAT, 0o666);
    for (let p = 0; p < heapPages; p++) {
      FS1.write(s1, fillPattern(PAGE_SIZE, p), 0, PAGE_SIZE);
    }
    FS1.close(s1);

    syncAndUnmount(FS1, t1);

    // --- Cycle 2: build index concurrently ---
    const { FS: FS2, tomefs: t2 } = await mountTome(backend, maxPages);

    // Phase 1: scan heap → write sorted entries to temp index file
    const heapScan = FS2.open(heapPath, O.RDONLY);
    const tempIdx = FS2.open(tempIdxPath, O.RDWR | O.CREAT, 0o666);

    // Each heap page produces half a page of index entries (compressed)
    // So 16 heap pages → 8 index pages
    const indexPages = heapPages / 2;
    for (let ip = 0; ip < indexPages; ip++) {
      // Read 2 heap pages per index page
      for (let h = 0; h < 2; h++) {
        const hp = ip * 2 + h;
        FS2.read(heapScan, readBuf, 0, PAGE_SIZE, hp * PAGE_SIZE);
        expectPattern(readBuf, PAGE_SIZE, hp);
      }
      // Write index page (derived from heap data)
      FS2.write(tempIdx, fillPattern(PAGE_SIZE, 0xD0 + ip), 0, PAGE_SIZE);
    }
    FS2.close(heapScan);
    FS2.close(tempIdx);

    // Phase 2: rename temp index to final location
    FS2.rename(tempIdxPath, finalIdxPath);

    // Phase 3: verify heap is still readable after index build
    // (ensure index build didn't corrupt heap pages in cache)
    const heapVerify = FS2.open(heapPath, O.RDONLY);
    for (let p = 0; p < heapPages; p++) {
      FS2.read(heapVerify, readBuf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expectPattern(readBuf, PAGE_SIZE, p);
    }
    FS2.close(heapVerify);

    syncAndUnmount(FS2, t2);

    // --- Cycle 3: verify index build results survived persistence ---
    const { FS: FS3 } = await mountTome(backend, maxPages);

    // Temp index should be gone
    expect(() => FS3.stat(tempIdxPath)).toThrow();

    // Final index should exist
    const idxStat = FS3.stat(finalIdxPath);
    expect(idxStat.size).toBe(indexPages * PAGE_SIZE);

    const idx3 = FS3.open(finalIdxPath, O.RDONLY);
    for (let ip = 0; ip < indexPages; ip++) {
      FS3.read(idx3, readBuf, 0, PAGE_SIZE, ip * PAGE_SIZE);
      expectPattern(readBuf, PAGE_SIZE, 0xD0 + ip);
    }
    FS3.close(idx3);

    // Heap should still be intact
    const heap3 = FS3.open(heapPath, O.RDONLY);
    for (let p = 0; p < heapPages; p++) {
      FS3.read(heap3, readBuf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expectPattern(readBuf, PAGE_SIZE, p);
    }
    FS3.close(heap3);
  },
);
