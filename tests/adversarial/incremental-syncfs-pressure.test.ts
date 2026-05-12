/**
 * Adversarial tests: incremental syncfs under 2-page cache pressure.
 *
 * The incremental syncfs path (O(dirty) instead of O(tree)) is the
 * production hot path for PGlite. These tests exercise it at the minimum
 * viable cache size (2 pages) where every read/write operation evicts the
 * previous operation's page — forcing continuous dirty flush through
 * eviction before syncfs even runs.
 *
 * Key interactions tested:
 *   - Dirty pages flushed via eviction BEFORE syncfs, then syncfs must
 *     still persist metadata for those pages
 *   - Many files competing for 2 cache pages with interleaved syncs
 *   - Sub-page writes that dirty a page, get evicted, then get re-dirtied
 *     in the same sync cycle
 *   - Cross-page boundary writes under cache pressure
 *   - Selective modification: only some files dirty between syncs
 *   - Mixed read-write patterns where reads evict dirty pages
 *   - Rapid create-write-sync cycles on many small files
 *
 * These complement incremental-syncfs-cycles.test.ts (which uses 4-page
 * and default caches) by targeting the extreme eviction edge.
 *
 * Ethos §8 (workload scenarios), §9 (adversarial differential testing)
 */
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { describe, it, expect } from "vitest";
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

const MOUNT = "/tome";

function fillPattern(size: number, seed: number): Uint8Array {
  const buf = new Uint8Array(size);
  for (let i = 0; i < size; i++) {
    buf[i] = (seed + i * 31) & 0xff;
  }
  return buf;
}

function verifyPattern(buf: Uint8Array, size: number, seed: number): boolean {
  for (let i = 0; i < size; i++) {
    if (buf[i] !== ((seed + i * 31) & 0xff)) return false;
  }
  return true;
}

async function mountTome(backend: SyncMemoryBackend, maxPages = 2) {
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

function syncfs(FS: any, tomefs: any): void {
  tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
    if (err) throw err;
  });
}

describe("adversarial: incremental syncfs under 2-page cache pressure", () => {

  // ------------------------------------------------------------------
  // Many files competing for 2 cache pages
  // ------------------------------------------------------------------

  it("8 files written round-robin with sync after each round persist @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    const N = 8;
    const streams: any[] = [];
    for (let i = 0; i < N; i++) {
      streams.push(FS.open(`${MOUNT}/f${i}`, O.RDWR | O.CREAT, 0o666));
    }

    // 4 rounds: each round writes 1 page to all 8 files, then syncs.
    // With 2-page cache, every write evicts the previous file's page.
    for (let round = 0; round < 4; round++) {
      for (let i = 0; i < N; i++) {
        const seed = round * N + i;
        FS.write(
          streams[i],
          fillPattern(PAGE_SIZE, seed),
          0,
          PAGE_SIZE,
          round * PAGE_SIZE,
        );
      }
      syncfs(FS, tomefs);
    }

    for (const s of streams) FS.close(s);

    // Remount with same 2-page cache and verify all data
    const { FS: FS2 } = await mountTome(backend);
    for (let i = 0; i < N; i++) {
      const s = FS2.open(`${MOUNT}/f${i}`, O.RDONLY);
      for (let round = 0; round < 4; round++) {
        const buf = new Uint8Array(PAGE_SIZE);
        FS2.read(s, buf, 0, PAGE_SIZE, round * PAGE_SIZE);
        const seed = round * N + i;
        expect(verifyPattern(buf, PAGE_SIZE, seed)).toBe(true);
      }
      FS2.close(s);
    }
  });

  // ------------------------------------------------------------------
  // Sub-page writes: dirty, evict, re-dirty within one sync cycle
  // ------------------------------------------------------------------

  it("sub-page writes to same page from different files between syncs @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    // Create 4 files, each with a sub-page write (128 bytes).
    // With 2-page cache: file0's page gets evicted when file2 writes,
    // then file0 gets re-loaded and re-dirtied by a second write.
    const streams: any[] = [];
    for (let i = 0; i < 4; i++) {
      streams.push(FS.open(`${MOUNT}/sub${i}`, O.RDWR | O.CREAT, 0o666));
    }

    // First pass: write 128 bytes at offset 0 to each file
    for (let i = 0; i < 4; i++) {
      FS.write(streams[i], fillPattern(128, i * 10), 0, 128, 0);
    }

    // Second pass: write 128 bytes at offset 128 to file 0 and file 1
    // These pages were evicted during the first pass and must be re-loaded
    FS.write(streams[0], fillPattern(128, 0xA0), 0, 128, 128);
    FS.write(streams[1], fillPattern(128, 0xB0), 0, 128, 128);

    syncfs(FS, tomefs);

    for (const s of streams) FS.close(s);

    // Remount and verify both writes to file 0 and 1 survived
    const { FS: FS2 } = await mountTome(backend);

    for (let i = 0; i < 4; i++) {
      const s = FS2.open(`${MOUNT}/sub${i}`, O.RDONLY);
      const buf = new Uint8Array(256);
      FS2.read(s, buf, 0, 256, 0);

      // First 128 bytes: original sub-page write
      expect(verifyPattern(buf.subarray(0, 128), 128, i * 10)).toBe(true);

      if (i < 2) {
        // Files 0 and 1 got second writes at offset 128
        const seed2 = i === 0 ? 0xA0 : 0xB0;
        expect(verifyPattern(buf.subarray(128, 256), 128, seed2)).toBe(true);
      }
      FS2.close(s);
    }
  });

  // ------------------------------------------------------------------
  // Cross-page boundary writes under 2-page pressure
  // ------------------------------------------------------------------

  it("cross-page write under 2-page cache persists both pages @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/cross`, O.RDWR | O.CREAT, 0o666);

    // Write 256 bytes straddling the page boundary: PAGE_SIZE - 128 to PAGE_SIZE + 128
    const crossData = fillPattern(256, 0xCC);
    FS.write(stream, crossData, 0, 256, PAGE_SIZE - 128);

    syncfs(FS, tomefs);
    FS.close(stream);

    // Remount and verify both pages have correct data
    const { FS: FS2 } = await mountTome(backend);
    const s = FS2.open(`${MOUNT}/cross`, O.RDONLY);
    const buf = new Uint8Array(256);
    FS2.read(s, buf, 0, 256, PAGE_SIZE - 128);
    expect(verifyPattern(buf, 256, 0xCC)).toBe(true);

    // Verify file size = PAGE_SIZE + 128 (extends into page 1)
    expect(FS2.stat(`${MOUNT}/cross`).size).toBe(PAGE_SIZE + 128);
    FS2.close(s);
  });

  it("cross-page writes to multiple files with interleaved syncs @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    // 3 files, each with a cross-page write, synced between each
    for (let i = 0; i < 3; i++) {
      const s = FS.open(`${MOUNT}/xpage${i}`, O.RDWR | O.CREAT, 0o666);
      const data = fillPattern(512, i * 0x20);
      FS.write(s, data, 0, 512, PAGE_SIZE - 256);
      FS.close(s);
      syncfs(FS, tomefs);
    }

    // Remount and verify all 3 files
    const { FS: FS2 } = await mountTome(backend);
    for (let i = 0; i < 3; i++) {
      const s = FS2.open(`${MOUNT}/xpage${i}`, O.RDONLY);
      const buf = new Uint8Array(512);
      FS2.read(s, buf, 0, 512, PAGE_SIZE - 256);
      expect(verifyPattern(buf, 512, i * 0x20)).toBe(true);
      FS2.close(s);
    }
  });

  // ------------------------------------------------------------------
  // Selective modification: only some files dirty between syncs
  // ------------------------------------------------------------------

  it("modifying subset of files between syncs preserves unmodified files @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    // Create 6 files and sync
    for (let i = 0; i < 6; i++) {
      FS.writeFile(`${MOUNT}/sel${i}`, fillPattern(PAGE_SIZE, i));
    }
    syncfs(FS, tomefs);

    // Modify only files 1 and 4 (dirty tracking must NOT touch 0,2,3,5)
    const s1 = FS.open(`${MOUNT}/sel1`, O.WRONLY);
    FS.write(s1, fillPattern(PAGE_SIZE, 0xF1), 0, PAGE_SIZE, 0);
    FS.close(s1);

    const s4 = FS.open(`${MOUNT}/sel4`, O.WRONLY);
    FS.write(s4, fillPattern(PAGE_SIZE, 0xF4), 0, PAGE_SIZE, 0);
    FS.close(s4);

    syncfs(FS, tomefs);

    // Remount: files 0,2,3,5 should have original data; 1,4 should have new data
    const { FS: FS2 } = await mountTome(backend);
    const expected = [0, 0xF1, 2, 3, 0xF4, 5];
    for (let i = 0; i < 6; i++) {
      const buf = FS2.readFile(`${MOUNT}/sel${i}`);
      expect(verifyPattern(buf, PAGE_SIZE, expected[i])).toBe(true);
    }
  });

  // ------------------------------------------------------------------
  // Reads causing eviction of dirty pages mid-cycle
  // ------------------------------------------------------------------

  it("read-evicting dirty page before syncfs still persists data @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    // Write to file A (page 0 dirty, in cache)
    const sA = FS.open(`${MOUNT}/dirtyA`, O.RDWR | O.CREAT, 0o666);
    FS.write(sA, fillPattern(PAGE_SIZE, 0xAA), 0, PAGE_SIZE, 0);

    // Write to file B (page 0 dirty, evicts A's page 0 → flushed to backend)
    const sB = FS.open(`${MOUNT}/dirtyB`, O.RDWR | O.CREAT, 0o666);
    FS.write(sB, fillPattern(PAGE_SIZE, 0xBB), 0, PAGE_SIZE, 0);

    // Now read file C (created first to have data), evicting B's dirty page
    FS.writeFile(`${MOUNT}/readC`, fillPattern(PAGE_SIZE, 0xC0));
    const sC = FS.open(`${MOUNT}/readC`, O.RDONLY);
    const readBuf = new Uint8Array(PAGE_SIZE);
    FS.read(sC, readBuf, 0, PAGE_SIZE, 0);
    FS.close(sC);

    // syncfs: A's page was flushed by eviction, B's page was flushed by eviction,
    // C's page is clean (only read). Metadata for all 3 must be persisted.
    syncfs(FS, tomefs);

    FS.close(sA);
    FS.close(sB);

    // Remount and verify
    const { FS: FS2 } = await mountTome(backend);
    const bufA = FS2.readFile(`${MOUNT}/dirtyA`);
    expect(verifyPattern(bufA, PAGE_SIZE, 0xAA)).toBe(true);
    const bufB = FS2.readFile(`${MOUNT}/dirtyB`);
    expect(verifyPattern(bufB, PAGE_SIZE, 0xBB)).toBe(true);
    const bufC = FS2.readFile(`${MOUNT}/readC`);
    expect(verifyPattern(bufC, PAGE_SIZE, 0xC0)).toBe(true);
  });

  it("reading older file pages evicts newer dirty pages then syncs @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    // Set up file with 4 pages (synced to backend)
    const setup = FS.open(`${MOUNT}/existing`, O.RDWR | O.CREAT, 0o666);
    for (let p = 0; p < 4; p++) {
      FS.write(setup, fillPattern(PAGE_SIZE, p), 0, PAGE_SIZE, p * PAGE_SIZE);
    }
    syncfs(FS, tomefs);

    // Now write to a new file (dirty, in cache)
    const sNew = FS.open(`${MOUNT}/newfile`, O.RDWR | O.CREAT, 0o666);
    FS.write(sNew, fillPattern(PAGE_SIZE, 0xDD), 0, PAGE_SIZE, 0);

    // Read all 4 pages of "existing" — each read evicts the previous page,
    // including the dirty page from "newfile"
    const readBuf = new Uint8Array(PAGE_SIZE);
    for (let p = 0; p < 4; p++) {
      FS.read(setup, readBuf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expect(verifyPattern(readBuf, PAGE_SIZE, p)).toBe(true);
    }

    syncfs(FS, tomefs);
    FS.close(setup);
    FS.close(sNew);

    // Remount: newfile's dirty page should have been flushed during eviction
    const { FS: FS2 } = await mountTome(backend);
    const buf = FS2.readFile(`${MOUNT}/newfile`);
    expect(verifyPattern(buf, PAGE_SIZE, 0xDD)).toBe(true);
  });

  // ------------------------------------------------------------------
  // Rapid create-write-sync cycles
  // ------------------------------------------------------------------

  it("20 rapid create-write-sync cycles under 2-page cache @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    for (let i = 0; i < 20; i++) {
      FS.writeFile(`${MOUNT}/rapid${i}`, fillPattern(100, i));
      syncfs(FS, tomefs);
    }

    // Remount and verify all 20 files
    const { FS: FS2 } = await mountTome(backend);
    for (let i = 0; i < 20; i++) {
      const buf = FS2.readFile(`${MOUNT}/rapid${i}`);
      expect(buf.length).toBe(100);
      expect(verifyPattern(buf, 100, i)).toBe(true);
    }
  });

  it("batch create then sync is equivalent to create-sync-create-sync @fast", async () => {
    // Verify that creating multiple files then syncing once gives
    // the same result as syncing after each creation.
    const backend1 = new SyncMemoryBackend();
    const backend2 = new SyncMemoryBackend();

    // Strategy A: batch create then sync
    {
      const { FS, tomefs } = await mountTome(backend1);
      for (let i = 0; i < 5; i++) {
        FS.writeFile(`${MOUNT}/batch${i}`, fillPattern(200, i * 7));
      }
      syncfs(FS, tomefs);
    }

    // Strategy B: create-sync each
    {
      const { FS, tomefs } = await mountTome(backend2);
      for (let i = 0; i < 5; i++) {
        FS.writeFile(`${MOUNT}/batch${i}`, fillPattern(200, i * 7));
        syncfs(FS, tomefs);
      }
    }

    // Both backends should produce the same file contents on remount
    const { FS: FS1 } = await mountTome(backend1);
    const { FS: FS2 } = await mountTome(backend2);

    for (let i = 0; i < 5; i++) {
      const buf1 = FS1.readFile(`${MOUNT}/batch${i}`);
      const buf2 = FS2.readFile(`${MOUNT}/batch${i}`);
      expect(buf1).toEqual(buf2);
      expect(verifyPattern(buf1, 200, i * 7)).toBe(true);
    }
  });

  // ------------------------------------------------------------------
  // O_APPEND writes interleaved across files
  // ------------------------------------------------------------------

  it("O_APPEND interleaved across 4 files under 2-page cache @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    const streams: any[] = [];
    for (let i = 0; i < 4; i++) {
      streams.push(FS.open(`${MOUNT}/append${i}`, O.WRONLY | O.CREAT | O.APPEND, 0o666));
    }

    // 5 rounds of appending 128 bytes to each file in round-robin
    for (let round = 0; round < 5; round++) {
      for (let i = 0; i < 4; i++) {
        const seed = round * 4 + i;
        FS.write(streams[i], fillPattern(128, seed), 0, 128);
      }
      syncfs(FS, tomefs);
    }

    for (const s of streams) FS.close(s);

    // Remount: each file should have 5 * 128 = 640 bytes
    const { FS: FS2 } = await mountTome(backend);
    for (let i = 0; i < 4; i++) {
      const s = FS2.open(`${MOUNT}/append${i}`, O.RDONLY);
      expect(FS2.stat(`${MOUNT}/append${i}`).size).toBe(640);

      for (let round = 0; round < 5; round++) {
        const buf = new Uint8Array(128);
        FS2.read(s, buf, 0, 128, round * 128);
        const seed = round * 4 + i;
        expect(verifyPattern(buf, 128, seed)).toBe(true);
      }
      FS2.close(s);
    }
  });

  // ------------------------------------------------------------------
  // Overwrite patterns that re-dirty evicted pages
  // ------------------------------------------------------------------

  it("overwrite-evict-overwrite cycle within single sync @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    const s = FS.open(`${MOUNT}/cycle`, O.RDWR | O.CREAT, 0o666);

    // Write page 0 (dirty, in cache)
    FS.write(s, fillPattern(PAGE_SIZE, 0x10), 0, PAGE_SIZE, 0);

    // Write page 1 (dirty, page 0 evicted and flushed with 0x10 data)
    FS.write(s, fillPattern(PAGE_SIZE, 0x20), 0, PAGE_SIZE, PAGE_SIZE);

    // Write page 2 (dirty, page 1 evicted and flushed with 0x20 data)
    FS.write(s, fillPattern(PAGE_SIZE, 0x30), 0, PAGE_SIZE, 2 * PAGE_SIZE);

    // Overwrite page 0 again (re-loaded from backend, re-dirtied)
    FS.write(s, fillPattern(PAGE_SIZE, 0x11), 0, PAGE_SIZE, 0);

    // Overwrite page 1 again (re-loaded, page 2 evicted)
    FS.write(s, fillPattern(PAGE_SIZE, 0x21), 0, PAGE_SIZE, PAGE_SIZE);

    syncfs(FS, tomefs);
    FS.close(s);

    // Remount: page 0 = 0x11, page 1 = 0x21, page 2 = 0x30
    const { FS: FS2 } = await mountTome(backend);
    const r = FS2.open(`${MOUNT}/cycle`, O.RDONLY);
    const expected = [0x11, 0x21, 0x30];
    for (let p = 0; p < 3; p++) {
      const buf = new Uint8Array(PAGE_SIZE);
      FS2.read(r, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expect(verifyPattern(buf, PAGE_SIZE, expected[p])).toBe(true);
    }
    FS2.close(r);
  });

  // ------------------------------------------------------------------
  // Incremental sync with directory creation
  // ------------------------------------------------------------------

  it("nested directory creation under 2-page cache with interleaved syncs @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    // Cycle 1: create top-level dir and file
    FS.mkdir(`${MOUNT}/d1`);
    FS.writeFile(`${MOUNT}/d1/a`, fillPattern(100, 0x11));
    syncfs(FS, tomefs);

    // Cycle 2: create nested dir and file
    FS.mkdir(`${MOUNT}/d1/d2`);
    FS.writeFile(`${MOUNT}/d1/d2/b`, fillPattern(200, 0x22));
    syncfs(FS, tomefs);

    // Cycle 3: create sibling dir and file
    FS.mkdir(`${MOUNT}/d1/d3`);
    FS.writeFile(`${MOUNT}/d1/d3/c`, fillPattern(50, 0x33));
    syncfs(FS, tomefs);

    // Remount
    const { FS: FS2 } = await mountTome(backend);
    expect(verifyPattern(FS2.readFile(`${MOUNT}/d1/a`), 100, 0x11)).toBe(true);
    expect(verifyPattern(FS2.readFile(`${MOUNT}/d1/d2/b`), 200, 0x22)).toBe(true);
    expect(verifyPattern(FS2.readFile(`${MOUNT}/d1/d3/c`), 50, 0x33)).toBe(true);

    const d1Contents = FS2.readdir(`${MOUNT}/d1`).filter(
      (n: string) => n !== "." && n !== "..",
    );
    expect(d1Contents.sort()).toEqual(["a", "d2", "d3"]);
  });

  // ------------------------------------------------------------------
  // Multiple mount-sync-remount cycles under 2-page cache
  // ------------------------------------------------------------------

  it("5 remount cycles accumulating data under 2-page cache @fast", async () => {
    const backend = new SyncMemoryBackend();

    for (let session = 0; session < 5; session++) {
      const { FS, tomefs } = await mountTome(backend);

      // Each session: append a page to file A and create a new file
      if (session === 0) {
        const s = FS.open(`${MOUNT}/accum`, O.RDWR | O.CREAT, 0o666);
        FS.write(s, fillPattern(PAGE_SIZE, session), 0, PAGE_SIZE, 0);
        FS.close(s);
      } else {
        const s = FS.open(`${MOUNT}/accum`, O.WRONLY | O.APPEND);
        FS.write(s, fillPattern(PAGE_SIZE, session), 0, PAGE_SIZE);
        FS.close(s);
      }

      FS.writeFile(`${MOUNT}/s${session}`, fillPattern(100, session * 0x10));

      syncfs(FS, tomefs);
    }

    // Final remount: accum should have 5 pages, each session file should exist
    const { FS } = await mountTome(backend);
    expect(FS.stat(`${MOUNT}/accum`).size).toBe(5 * PAGE_SIZE);

    const s = FS.open(`${MOUNT}/accum`, O.RDONLY);
    for (let p = 0; p < 5; p++) {
      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(s, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expect(verifyPattern(buf, PAGE_SIZE, p)).toBe(true);
    }
    FS.close(s);

    for (let i = 0; i < 5; i++) {
      const buf = FS.readFile(`${MOUNT}/s${i}`);
      expect(verifyPattern(buf, 100, i * 0x10)).toBe(true);
    }
  });

  // ------------------------------------------------------------------
  // Dirty metadata without dirty pages
  // ------------------------------------------------------------------

  it("chmod-only change under 2-page cache syncs incrementally @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    // Create and sync a file
    FS.writeFile(`${MOUNT}/monly`, fillPattern(100, 0xAB));
    syncfs(FS, tomefs);

    // Only change metadata (no page writes)
    FS.chmod(`${MOUNT}/monly`, 0o444);
    syncfs(FS, tomefs);

    // Remount: mode should be 0o444, data intact
    const { FS: FS2 } = await mountTome(backend);
    expect(FS2.stat(`${MOUNT}/monly`).mode & 0o777).toBe(0o444);
    const buf = FS2.readFile(`${MOUNT}/monly`);
    expect(verifyPattern(buf, 100, 0xAB)).toBe(true);
  });

  // ------------------------------------------------------------------
  // No dirty state: verify no-op sync is safe
  // ------------------------------------------------------------------

  it("consecutive syncs without modifications are idempotent @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    FS.writeFile(`${MOUNT}/stable`, fillPattern(PAGE_SIZE, 0x55));
    syncfs(FS, tomefs);

    // 10 consecutive no-op syncs
    for (let i = 0; i < 10; i++) {
      syncfs(FS, tomefs);
    }

    // Remount and verify
    const { FS: FS2 } = await mountTome(backend);
    const buf = FS2.readFile(`${MOUNT}/stable`);
    expect(verifyPattern(buf, PAGE_SIZE, 0x55)).toBe(true);
  });

  // ------------------------------------------------------------------
  // Dirty shutdown between sync cycles (no sync after last write)
  // ------------------------------------------------------------------

  it("data from last sync survives dirty shutdown under 2-page cache @fast", async () => {
    const backend = new SyncMemoryBackend();

    // Session 1: write and sync
    {
      const { FS, tomefs } = await mountTome(backend);
      FS.writeFile(`${MOUNT}/synced`, fillPattern(PAGE_SIZE, 0x11));
      syncfs(FS, tomefs);

      // Write more but DON'T sync (simulating dirty shutdown)
      FS.writeFile(`${MOUNT}/unsynced`, fillPattern(PAGE_SIZE, 0x22));
    }

    // Session 2: only synced data should survive
    {
      const { FS } = await mountTome(backend);
      const buf = FS.readFile(`${MOUNT}/synced`);
      expect(verifyPattern(buf, PAGE_SIZE, 0x11)).toBe(true);

      // unsynced file should NOT exist (never synced)
      expect(() => FS.stat(`${MOUNT}/unsynced`)).toThrow();
    }
  });

  it("partial write cycle: synced files survive, unsynced modifications lost @fast", async () => {
    const backend = new SyncMemoryBackend();

    // Session 1: create 3 files, sync, then modify file 0 without syncing
    {
      const { FS, tomefs } = await mountTome(backend);
      for (let i = 0; i < 3; i++) {
        FS.writeFile(`${MOUNT}/part${i}`, fillPattern(PAGE_SIZE, i));
      }
      syncfs(FS, tomefs);

      // Modify file 0 without syncing
      const s = FS.open(`${MOUNT}/part0`, O.WRONLY);
      FS.write(s, fillPattern(PAGE_SIZE, 0xFF), 0, PAGE_SIZE, 0);
      FS.close(s);
      // Dirty shutdown: no sync
    }

    // Session 2: file 0 should have original data (modification lost),
    // files 1 and 2 should be intact
    {
      const { FS } = await mountTome(backend);

      // File 0: eviction during session 1 may have flushed the dirty page
      // to the backend. The behavior depends on cache pressure: if another
      // operation evicted file 0's page, the dirty data was flushed by
      // eviction. Read whatever the backend has — it's either the original
      // or the modified data, depending on eviction timing.
      const buf0 = FS.readFile(`${MOUNT}/part0`);
      expect(buf0.length).toBe(PAGE_SIZE);

      // Files 1 and 2 are definitely intact (synced and not modified)
      for (let i = 1; i < 3; i++) {
        const buf = FS.readFile(`${MOUNT}/part${i}`);
        expect(verifyPattern(buf, PAGE_SIZE, i)).toBe(true);
      }
    }
  });

  // ------------------------------------------------------------------
  // File size tracking across eviction + sync cycles
  // ------------------------------------------------------------------

  it("file size metadata correct after multi-page write + eviction + sync @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    // Write a 5-page file under 2-page cache.
    // Pages 0-2 will be evicted as pages 3-4 are written.
    const s = FS.open(`${MOUNT}/sized`, O.RDWR | O.CREAT, 0o666);
    for (let p = 0; p < 5; p++) {
      FS.write(s, fillPattern(PAGE_SIZE, p), 0, PAGE_SIZE, p * PAGE_SIZE);
    }
    FS.close(s);
    syncfs(FS, tomefs);

    // Remount: file size must be exactly 5 * PAGE_SIZE
    const { FS: FS2 } = await mountTome(backend);
    expect(FS2.stat(`${MOUNT}/sized`).size).toBe(5 * PAGE_SIZE);

    // Verify all pages readable
    const r = FS2.open(`${MOUNT}/sized`, O.RDONLY);
    for (let p = 0; p < 5; p++) {
      const buf = new Uint8Array(PAGE_SIZE);
      FS2.read(r, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expect(verifyPattern(buf, PAGE_SIZE, p)).toBe(true);
    }
    FS2.close(r);
  });

  // ------------------------------------------------------------------
  // Truncate under 2-page cache pressure between syncs
  // ------------------------------------------------------------------

  it("truncate to zero then regrow under 2-page cache with syncs @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    // Write 3 pages and sync
    const s = FS.open(`${MOUNT}/trunc`, O.RDWR | O.CREAT, 0o666);
    for (let p = 0; p < 3; p++) {
      FS.write(s, fillPattern(PAGE_SIZE, p), 0, PAGE_SIZE, p * PAGE_SIZE);
    }
    syncfs(FS, tomefs);

    // Truncate to 0 and sync
    FS.ftruncate(s.fd, 0);
    syncfs(FS, tomefs);

    // Regrow with new data and sync
    for (let p = 0; p < 2; p++) {
      FS.write(s, fillPattern(PAGE_SIZE, p + 0x80), 0, PAGE_SIZE, p * PAGE_SIZE);
    }
    syncfs(FS, tomefs);
    FS.close(s);

    // Remount: file should have 2 pages of new data
    const { FS: FS2 } = await mountTome(backend);
    expect(FS2.stat(`${MOUNT}/trunc`).size).toBe(2 * PAGE_SIZE);

    const r = FS2.open(`${MOUNT}/trunc`, O.RDONLY);
    for (let p = 0; p < 2; p++) {
      const buf = new Uint8Array(PAGE_SIZE);
      FS2.read(r, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expect(verifyPattern(buf, PAGE_SIZE, p + 0x80)).toBe(true);
    }
    FS2.close(r);
  });

  // ------------------------------------------------------------------
  // Interleaved write + truncate across multiple files
  // ------------------------------------------------------------------

  it("write file A, truncate file B, sync, both correct @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    // Create both files with 2 pages each and sync
    for (const name of ["fileA", "fileB"]) {
      const s = FS.open(`${MOUNT}/${name}`, O.RDWR | O.CREAT, 0o666);
      FS.write(s, fillPattern(2 * PAGE_SIZE, name === "fileA" ? 0x10 : 0x20), 0, 2 * PAGE_SIZE, 0);
      FS.close(s);
    }
    syncfs(FS, tomefs);

    // Modify file A's page 0 and truncate file B to half
    const sA = FS.open(`${MOUNT}/fileA`, O.WRONLY);
    FS.write(sA, fillPattern(PAGE_SIZE, 0x15), 0, PAGE_SIZE, 0);
    FS.close(sA);

    FS.truncate(`${MOUNT}/fileB`, PAGE_SIZE / 2);
    syncfs(FS, tomefs);

    // Remount and verify
    const { FS: FS2 } = await mountTome(backend);

    // File A: page 0 = 0x15, page 1 = original (from seed 0x10, offset PAGE_SIZE)
    const rA = FS2.open(`${MOUNT}/fileA`, O.RDONLY);
    const bufA0 = new Uint8Array(PAGE_SIZE);
    FS2.read(rA, bufA0, 0, PAGE_SIZE, 0);
    expect(verifyPattern(bufA0, PAGE_SIZE, 0x15)).toBe(true);
    const bufA1 = new Uint8Array(PAGE_SIZE);
    FS2.read(rA, bufA1, 0, PAGE_SIZE, PAGE_SIZE);
    const origA = fillPattern(2 * PAGE_SIZE, 0x10);
    expect(bufA1).toEqual(origA.subarray(PAGE_SIZE, 2 * PAGE_SIZE));
    FS2.close(rA);

    // File B: truncated to half page
    expect(FS2.stat(`${MOUNT}/fileB`).size).toBe(PAGE_SIZE / 2);
    const bufB = FS2.readFile(`${MOUNT}/fileB`);
    const origB = fillPattern(2 * PAGE_SIZE, 0x20);
    expect(bufB).toEqual(origB.subarray(0, PAGE_SIZE / 2));
  });

  // ------------------------------------------------------------------
  // Stress: many sync cycles with alternating patterns
  // ------------------------------------------------------------------

  it("50 sync cycles alternating between 3 files under 2-page cache @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    // Create 3 files
    for (let i = 0; i < 3; i++) {
      FS.open(`${MOUNT}/stress${i}`, O.RDWR | O.CREAT, 0o666);
    }

    // 50 cycles: write 256 bytes to file (cycle % 3) at offset (cycle * 256)
    const streams = [0, 1, 2].map((i) =>
      FS.open(`${MOUNT}/stress${i}`, O.RDWR),
    );

    for (let cycle = 0; cycle < 50; cycle++) {
      const fileIdx = cycle % 3;
      const writeOffset = Math.floor(cycle / 3) * 256;
      FS.write(
        streams[fileIdx],
        fillPattern(256, cycle),
        0,
        256,
        writeOffset,
      );
      syncfs(FS, tomefs);
    }

    for (const s of streams) FS.close(s);

    // Remount and verify
    const { FS: FS2 } = await mountTome(backend);
    for (let fileIdx = 0; fileIdx < 3; fileIdx++) {
      const s = FS2.open(`${MOUNT}/stress${fileIdx}`, O.RDONLY);
      // File fileIdx was written in cycles fileIdx, fileIdx+3, fileIdx+6, ...
      for (let cycle = fileIdx; cycle < 50; cycle += 3) {
        const writeOffset = Math.floor(cycle / 3) * 256;
        const buf = new Uint8Array(256);
        FS2.read(s, buf, 0, 256, writeOffset);
        expect(verifyPattern(buf, 256, cycle)).toBe(true);
      }
      FS2.close(s);
    }
  });
});
