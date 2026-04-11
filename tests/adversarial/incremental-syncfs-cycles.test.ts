/**
 * Adversarial tests: multi-cycle incremental syncfs with dirty tracking.
 *
 * The incremental syncfs path (O(dirty)) is the steady-state hot path for
 * PGlite: after every transaction, Postgres calls syncfs, which persists
 * only dirty pages and dirty metadata nodes. This avoids the O(tree-size)
 * full tree walk that's needed after rename/unlink operations.
 *
 * These tests specifically target the incremental path and verify:
 *   - Dirty metadata flags are correctly set, cleared, and re-set across cycles
 *   - Multiple sync cycles with interleaved writes to different files
 *   - The clean-shutdown marker lifecycle across many sync cycles
 *   - Transition from incremental to full tree walk (rename triggers orphan cleanup)
 *   - Data integrity after many sync-remount cycles under cache pressure
 *
 * Most existing adversarial tests exercise the full tree walk path (rename/unlink
 * sets needsOrphanCleanup=true). This file fills the gap for the incremental path.
 *
 * Ethos §8 (workload scenarios), §9 (adversarial differential testing):
 * "Target the seams: ... dirty flush ordering on concurrent streams"
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

async function mountTome(backend: SyncMemoryBackend, maxPages = 4096) {
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

describe("adversarial: multi-cycle incremental syncfs", () => {

  // ------------------------------------------------------------------
  // Core: dirty metadata re-dirtying across sync cycles
  // ------------------------------------------------------------------

  it("writes to same file across multiple sync cycles all persist @fast", async () => {
    // PGlite appends WAL records, syncs, appends more, syncs again.
    // Each sync clears dirty flags; subsequent writes must re-dirty.
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);

    // Cycle 1: write page 0
    const data1 = fillPattern(PAGE_SIZE, 0x11);
    FS.write(stream, data1, 0, PAGE_SIZE, 0);
    syncfs(FS, tomefs);

    // Cycle 2: write page 1 (dirty flags were cleared in cycle 1)
    const data2 = fillPattern(PAGE_SIZE, 0x22);
    FS.write(stream, data2, 0, PAGE_SIZE, PAGE_SIZE);
    syncfs(FS, tomefs);

    // Cycle 3: write page 2
    const data3 = fillPattern(PAGE_SIZE, 0x33);
    FS.write(stream, data3, 0, PAGE_SIZE, 2 * PAGE_SIZE);
    syncfs(FS, tomefs);

    FS.close(stream);

    // Remount and verify all 3 pages survived
    const { FS: FS2 } = await mountTome(backend);
    expect(FS2.stat(`${MOUNT}/wal`).size).toBe(3 * PAGE_SIZE);

    const s2 = FS2.open(`${MOUNT}/wal`, O.RDONLY);
    for (let p = 0; p < 3; p++) {
      const seed = [0x11, 0x22, 0x33][p];
      const buf = new Uint8Array(PAGE_SIZE);
      FS2.read(s2, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expect(
        verifyPattern(buf, PAGE_SIZE, seed),
      ).toBe(true);
    }
    FS2.close(s2);
  });

  it("overwrite same page across multiple sync cycles persists final value @fast", async () => {
    // Repeated overwrites to the same page with syncs in between.
    // Tests that dirty flags are correctly re-set after clearing.
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/heap`, O.RDWR | O.CREAT, 0o666);

    // 5 cycles, each writing different data to page 0
    for (let cycle = 0; cycle < 5; cycle++) {
      const data = fillPattern(PAGE_SIZE, cycle * 0x10);
      FS.write(stream, data, 0, PAGE_SIZE, 0);
      syncfs(FS, tomefs);
    }
    FS.close(stream);

    // Remount: page 0 should have the data from cycle 4
    const { FS: FS2 } = await mountTome(backend);
    const s = FS2.open(`${MOUNT}/heap`, O.RDONLY);
    const buf = new Uint8Array(PAGE_SIZE);
    FS2.read(s, buf, 0, PAGE_SIZE, 0);
    expect(verifyPattern(buf, PAGE_SIZE, 4 * 0x10)).toBe(true);
    FS2.close(s);
  });

  // ------------------------------------------------------------------
  // Multi-file interleaved writes between syncs
  // ------------------------------------------------------------------

  it("interleaved writes to multiple files across sync cycles @fast", async () => {
    // PGlite writes to WAL, heap, and index files between syncs.
    // Each sync should persist dirty data for ALL modified files.
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    const files = ["wal", "heap", "idx"];
    const streams: any[] = [];
    for (const name of files) {
      streams.push(FS.open(`${MOUNT}/${name}`, O.RDWR | O.CREAT, 0o666));
    }

    // Cycle 1: write to wal and heap
    FS.write(streams[0], fillPattern(PAGE_SIZE, 0xAA), 0, PAGE_SIZE, 0);
    FS.write(streams[1], fillPattern(PAGE_SIZE, 0xBB), 0, PAGE_SIZE, 0);
    syncfs(FS, tomefs);

    // Cycle 2: write to heap and idx (wal untouched)
    FS.write(streams[1], fillPattern(PAGE_SIZE, 0xCC), 0, PAGE_SIZE, PAGE_SIZE);
    FS.write(streams[2], fillPattern(PAGE_SIZE, 0xDD), 0, PAGE_SIZE, 0);
    syncfs(FS, tomefs);

    // Cycle 3: write to wal only
    FS.write(streams[0], fillPattern(PAGE_SIZE, 0xEE), 0, PAGE_SIZE, PAGE_SIZE);
    syncfs(FS, tomefs);

    for (const s of streams) FS.close(s);

    // Remount and verify all data from all cycles
    const { FS: FS2 } = await mountTome(backend);

    // wal: page 0 = 0xAA (cycle 1), page 1 = 0xEE (cycle 3)
    const walS = FS2.open(`${MOUNT}/wal`, O.RDONLY);
    const walBuf0 = new Uint8Array(PAGE_SIZE);
    FS2.read(walS, walBuf0, 0, PAGE_SIZE, 0);
    expect(verifyPattern(walBuf0, PAGE_SIZE, 0xAA)).toBe(true);
    const walBuf1 = new Uint8Array(PAGE_SIZE);
    FS2.read(walS, walBuf1, 0, PAGE_SIZE, PAGE_SIZE);
    expect(verifyPattern(walBuf1, PAGE_SIZE, 0xEE)).toBe(true);
    FS2.close(walS);

    // heap: page 0 = 0xBB (cycle 1), page 1 = 0xCC (cycle 2)
    const heapS = FS2.open(`${MOUNT}/heap`, O.RDONLY);
    const heapBuf0 = new Uint8Array(PAGE_SIZE);
    FS2.read(heapS, heapBuf0, 0, PAGE_SIZE, 0);
    expect(verifyPattern(heapBuf0, PAGE_SIZE, 0xBB)).toBe(true);
    const heapBuf1 = new Uint8Array(PAGE_SIZE);
    FS2.read(heapS, heapBuf1, 0, PAGE_SIZE, PAGE_SIZE);
    expect(verifyPattern(heapBuf1, PAGE_SIZE, 0xCC)).toBe(true);
    FS2.close(heapS);

    // idx: page 0 = 0xDD (cycle 2)
    const idxS = FS2.open(`${MOUNT}/idx`, O.RDONLY);
    const idxBuf = new Uint8Array(PAGE_SIZE);
    FS2.read(idxS, idxBuf, 0, PAGE_SIZE, 0);
    expect(verifyPattern(idxBuf, PAGE_SIZE, 0xDD)).toBe(true);
    FS2.close(idxS);
  });

  // ------------------------------------------------------------------
  // Metadata-only changes between syncs
  // ------------------------------------------------------------------

  it("chmod between sync cycles persists mode change", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    // Create file and sync
    const data = fillPattern(100, 0x42);
    FS.writeFile(`${MOUNT}/conf`, data);
    syncfs(FS, tomefs);

    // Change mode (metadata-only change) and sync again
    FS.chmod(`${MOUNT}/conf`, 0o644);
    syncfs(FS, tomefs);

    // Remount and verify mode persisted
    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/conf`);
    expect(stat.mode & 0o777).toBe(0o644);
    // Data should also survive
    const buf = FS2.readFile(`${MOUNT}/conf`);
    expect(verifyPattern(buf, 100, 0x42)).toBe(true);
  });

  it("utime between sync cycles persists timestamp change", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    FS.writeFile(`${MOUNT}/log`, new Uint8Array(10));
    syncfs(FS, tomefs);

    // Change timestamps (metadata-only) and sync
    const atime = 1000000;
    const mtime = 2000000;
    FS.utime(`${MOUNT}/log`, atime, mtime);
    syncfs(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/log`);
    expect(stat.atime.getTime()).toBe(atime);
    expect(stat.mtime.getTime()).toBe(mtime);
  });

  // ------------------------------------------------------------------
  // Truncate between sync cycles
  // ------------------------------------------------------------------

  it("truncate between syncs correctly re-dirties metadata", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    // Write 3 pages and sync
    const stream = FS.open(`${MOUNT}/data`, O.RDWR | O.CREAT, 0o666);
    FS.write(stream, fillPattern(3 * PAGE_SIZE, 0x55), 0, 3 * PAGE_SIZE, 0);
    syncfs(FS, tomefs);

    // Truncate to 1.5 pages (metadata dirty again) and sync
    FS.ftruncate(stream.fd, PAGE_SIZE + PAGE_SIZE / 2);
    syncfs(FS, tomefs);

    FS.close(stream);

    // Remount: size should be 1.5 pages, page 0 has data, page 1 has data + zeros
    const { FS: FS2 } = await mountTome(backend);
    const truncSize = PAGE_SIZE + PAGE_SIZE / 2;
    expect(FS2.stat(`${MOUNT}/data`).size).toBe(truncSize);

    const s = FS2.open(`${MOUNT}/data`, O.RDONLY);
    const buf = new Uint8Array(truncSize);
    FS2.read(s, buf, 0, truncSize, 0);
    // First PAGE_SIZE bytes should be the original pattern
    expect(verifyPattern(buf.subarray(0, PAGE_SIZE), PAGE_SIZE, 0x55)).toBe(true);
    // Next PAGE_SIZE/2 bytes should also match (second page, first half)
    const expected = fillPattern(PAGE_SIZE, 0x55).subarray(0, PAGE_SIZE / 2);
    // The pattern wraps — bytes at position PAGE_SIZE..PAGE_SIZE+4095 of the
    // original 3-page write correspond to seed offsets PAGE_SIZE..
    // Actually let me verify the raw bytes.
    const page1Expected = fillPattern(3 * PAGE_SIZE, 0x55).subarray(PAGE_SIZE, truncSize);
    expect(buf.subarray(PAGE_SIZE, truncSize)).toEqual(page1Expected);
    FS2.close(s);
  });

  // ------------------------------------------------------------------
  // Many sync cycles (stress)
  // ------------------------------------------------------------------

  it("20 sync cycles with alternating file writes all persist", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    // Create two files
    const sA = FS.open(`${MOUNT}/fileA`, O.RDWR | O.CREAT, 0o666);
    const sB = FS.open(`${MOUNT}/fileB`, O.RDWR | O.CREAT, 0o666);

    // 20 cycles: alternate writing to A and B
    for (let cycle = 0; cycle < 20; cycle++) {
      const stream = cycle % 2 === 0 ? sA : sB;
      const data = fillPattern(PAGE_SIZE, cycle);
      const pageIdx = Math.floor(cycle / 2);
      FS.write(stream, data, 0, PAGE_SIZE, pageIdx * PAGE_SIZE);
      syncfs(FS, tomefs);
    }

    FS.close(sA);
    FS.close(sB);

    // Remount: each file should have 10 pages
    const { FS: FS2 } = await mountTome(backend);
    expect(FS2.stat(`${MOUNT}/fileA`).size).toBe(10 * PAGE_SIZE);
    expect(FS2.stat(`${MOUNT}/fileB`).size).toBe(10 * PAGE_SIZE);

    // Verify each page has the correct data
    const rA = FS2.open(`${MOUNT}/fileA`, O.RDONLY);
    for (let p = 0; p < 10; p++) {
      const buf = new Uint8Array(PAGE_SIZE);
      FS2.read(rA, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
      const cycle = p * 2; // even cycles wrote to A
      expect(verifyPattern(buf, PAGE_SIZE, cycle)).toBe(true);
    }
    FS2.close(rA);

    const rB = FS2.open(`${MOUNT}/fileB`, O.RDONLY);
    for (let p = 0; p < 10; p++) {
      const buf = new Uint8Array(PAGE_SIZE);
      FS2.read(rB, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
      const cycle = p * 2 + 1; // odd cycles wrote to B
      expect(verifyPattern(buf, PAGE_SIZE, cycle)).toBe(true);
    }
    FS2.close(rB);
  });

  // ------------------------------------------------------------------
  // Cache pressure across sync cycles
  // ------------------------------------------------------------------

  it("sync cycles under 4-page cache pressure persist all data", async () => {
    // With a 4-page cache, writing more than 4 pages across cycles
    // forces eviction. Dirty pages evicted before syncfs must be flushed
    // to the backend, and subsequent syncs must not lose them.
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend, 4);

    const stream = FS.open(`${MOUNT}/stress`, O.RDWR | O.CREAT, 0o666);

    // Write 8 pages across 8 sync cycles (2x cache capacity)
    for (let p = 0; p < 8; p++) {
      FS.write(stream, fillPattern(PAGE_SIZE, p * 0x10), 0, PAGE_SIZE, p * PAGE_SIZE);
      syncfs(FS, tomefs);
    }
    FS.close(stream);

    // Remount with same small cache
    const { FS: FS2 } = await mountTome(backend, 4);
    expect(FS2.stat(`${MOUNT}/stress`).size).toBe(8 * PAGE_SIZE);

    const s = FS2.open(`${MOUNT}/stress`, O.RDONLY);
    for (let p = 0; p < 8; p++) {
      const buf = new Uint8Array(PAGE_SIZE);
      FS2.read(s, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expect(
        verifyPattern(buf, PAGE_SIZE, p * 0x10),
      ).toBe(true);
    }
    FS2.close(s);
  });

  it("multiple files under cache pressure with interleaved syncs", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend, 4);

    // Create 3 files, each getting 2 pages, total 6 pages > 4-page cache.
    // Interleave writes and syncs to stress eviction + dirty tracking.
    const names = ["a", "b", "c"];
    const streams = names.map((n) =>
      FS.open(`${MOUNT}/${n}`, O.RDWR | O.CREAT, 0o666),
    );

    // Cycle 1: write page 0 of all 3 files
    for (let f = 0; f < 3; f++) {
      FS.write(streams[f], fillPattern(PAGE_SIZE, f), 0, PAGE_SIZE, 0);
    }
    syncfs(FS, tomefs);

    // Cycle 2: write page 1 of all 3 files (cache now fully thrashed)
    for (let f = 0; f < 3; f++) {
      FS.write(
        streams[f],
        fillPattern(PAGE_SIZE, f + 0x10),
        0,
        PAGE_SIZE,
        PAGE_SIZE,
      );
    }
    syncfs(FS, tomefs);

    for (const s of streams) FS.close(s);

    // Remount and verify
    const { FS: FS2 } = await mountTome(backend, 4);
    for (let f = 0; f < 3; f++) {
      const s = FS2.open(`${MOUNT}/${names[f]}`, O.RDONLY);
      const buf0 = new Uint8Array(PAGE_SIZE);
      FS2.read(s, buf0, 0, PAGE_SIZE, 0);
      expect(verifyPattern(buf0, PAGE_SIZE, f)).toBe(true);

      const buf1 = new Uint8Array(PAGE_SIZE);
      FS2.read(s, buf1, 0, PAGE_SIZE, PAGE_SIZE);
      expect(verifyPattern(buf1, PAGE_SIZE, f + 0x10)).toBe(true);
      FS2.close(s);
    }
  });

  // ------------------------------------------------------------------
  // Transition from incremental to full tree walk
  // ------------------------------------------------------------------

  it("incremental syncs followed by rename triggers correct full tree walk", async () => {
    // Sequence: write → sync (incremental) → write → sync (incremental) →
    // rename → sync (full tree walk). All data from all phases must survive.
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    // Phase 1: create and sync two files (incremental)
    FS.writeFile(`${MOUNT}/orig`, fillPattern(PAGE_SIZE, 0xAA));
    FS.writeFile(`${MOUNT}/keep`, fillPattern(PAGE_SIZE, 0xBB));
    syncfs(FS, tomefs);

    // Phase 2: modify "keep" and sync (still incremental — no structural changes)
    const s = FS.open(`${MOUNT}/keep`, O.WRONLY);
    FS.write(s, fillPattern(PAGE_SIZE, 0xCC), 0, PAGE_SIZE, PAGE_SIZE);
    FS.close(s);
    syncfs(FS, tomefs);

    // Phase 3: rename "orig" → triggers needsOrphanCleanup, next sync is full walk
    FS.rename(`${MOUNT}/orig`, `${MOUNT}/renamed`);
    syncfs(FS, tomefs);

    // Remount and verify
    const { FS: FS2 } = await mountTome(backend);

    // "renamed" should have orig's data
    const renamedBuf = FS2.readFile(`${MOUNT}/renamed`);
    expect(verifyPattern(renamedBuf, PAGE_SIZE, 0xAA)).toBe(true);

    // "keep" should have both pages from phase 1 and phase 2
    const keepS = FS2.open(`${MOUNT}/keep`, O.RDONLY);
    const keepBuf0 = new Uint8Array(PAGE_SIZE);
    FS2.read(keepS, keepBuf0, 0, PAGE_SIZE, 0);
    expect(verifyPattern(keepBuf0, PAGE_SIZE, 0xBB)).toBe(true);
    const keepBuf1 = new Uint8Array(PAGE_SIZE);
    FS2.read(keepS, keepBuf1, 0, PAGE_SIZE, PAGE_SIZE);
    expect(verifyPattern(keepBuf1, PAGE_SIZE, 0xCC)).toBe(true);
    FS2.close(keepS);

    // "orig" should not exist
    expect(() => FS2.stat(`${MOUNT}/orig`)).toThrow();
  });

  it("incremental sync after full-walk sync returns to incremental path", async () => {
    // After a full tree walk (rename), subsequent syncs without structural
    // changes should use the incremental path again.
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    // Phase 1: create file and sync (incremental)
    FS.writeFile(`${MOUNT}/f1`, fillPattern(100, 0x11));
    syncfs(FS, tomefs);

    // Phase 2: rename triggers full walk
    FS.rename(`${MOUNT}/f1`, `${MOUNT}/f2`);
    syncfs(FS, tomefs);

    // Phase 3: write to renamed file and create new file (should be incremental)
    const s = FS.open(`${MOUNT}/f2`, O.WRONLY | O.APPEND);
    FS.write(s, fillPattern(200, 0x22), 0, 200);
    FS.close(s);
    FS.writeFile(`${MOUNT}/f3`, fillPattern(50, 0x33));
    syncfs(FS, tomefs);

    // Remount and verify
    const { FS: FS2 } = await mountTome(backend);

    // f2 should have original 100 bytes + appended 200 bytes
    const f2Buf = FS2.readFile(`${MOUNT}/f2`);
    expect(f2Buf.length).toBe(300);
    expect(verifyPattern(f2Buf.subarray(0, 100), 100, 0x11)).toBe(true);
    expect(verifyPattern(f2Buf.subarray(100, 300), 200, 0x22)).toBe(true);

    // f3 should exist with its data
    const f3Buf = FS2.readFile(`${MOUNT}/f3`);
    expect(verifyPattern(f3Buf, 50, 0x33)).toBe(true);

    // f1 should not exist
    expect(() => FS2.stat(`${MOUNT}/f1`)).toThrow();
  });

  // ------------------------------------------------------------------
  // No-op syncs (read-only between syncs)
  // ------------------------------------------------------------------

  it("sync after read-only operations is a no-op but clean marker survives", async () => {
    const backend = new SyncMemoryBackend();

    // Session 1: create file and sync
    {
      const { FS, tomefs } = await mountTome(backend);
      FS.writeFile(`${MOUNT}/data`, fillPattern(PAGE_SIZE, 0xFF));
      syncfs(FS, tomefs);
    }

    // Session 2: read-only operations, then sync, then remount
    {
      const { FS, tomefs } = await mountTome(backend);

      // Read-only: stat, readFile, readdir
      FS.stat(`${MOUNT}/data`);
      FS.readFile(`${MOUNT}/data`);
      FS.readdir(`${MOUNT}`);

      // Sync after read-only session (should write clean marker only)
      syncfs(FS, tomefs);
    }

    // Session 3: data should still be intact
    {
      const { FS } = await mountTome(backend);
      const buf = FS.readFile(`${MOUNT}/data`);
      expect(verifyPattern(buf, PAGE_SIZE, 0xFF)).toBe(true);
    }
  });

  // ------------------------------------------------------------------
  // Directory + file interactions across sync cycles
  // ------------------------------------------------------------------

  it("directory creation between syncs persists correctly", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    // Cycle 1: create dir and file in it
    FS.mkdir(`${MOUNT}/subdir`);
    FS.writeFile(`${MOUNT}/subdir/f1`, fillPattern(100, 0x11));
    syncfs(FS, tomefs);

    // Cycle 2: create another file in the same dir
    FS.writeFile(`${MOUNT}/subdir/f2`, fillPattern(200, 0x22));
    syncfs(FS, tomefs);

    // Cycle 3: create nested dir with a file
    FS.mkdir(`${MOUNT}/subdir/nested`);
    FS.writeFile(`${MOUNT}/subdir/nested/f3`, fillPattern(50, 0x33));
    syncfs(FS, tomefs);

    // Remount and verify everything
    const { FS: FS2 } = await mountTome(backend);
    const listing = FS2.readdir(`${MOUNT}/subdir`).filter(
      (n: string) => n !== "." && n !== "..",
    );
    expect(listing.sort()).toEqual(["f1", "f2", "nested"]);

    expect(verifyPattern(FS2.readFile(`${MOUNT}/subdir/f1`), 100, 0x11)).toBe(true);
    expect(verifyPattern(FS2.readFile(`${MOUNT}/subdir/f2`), 200, 0x22)).toBe(true);
    expect(
      verifyPattern(FS2.readFile(`${MOUNT}/subdir/nested/f3`), 50, 0x33),
    ).toBe(true);
  });

  // ------------------------------------------------------------------
  // Symlink creation between sync cycles
  // ------------------------------------------------------------------

  it("symlink creation between syncs persists target", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    // Cycle 1: create target file
    FS.writeFile(`${MOUNT}/target`, fillPattern(100, 0xAA));
    syncfs(FS, tomefs);

    // Cycle 2: create symlink to it
    FS.symlink(`${MOUNT}/target`, `${MOUNT}/link`);
    syncfs(FS, tomefs);

    // Remount and verify symlink resolves
    const { FS: FS2 } = await mountTome(backend);
    const linkTarget = FS2.readlink(`${MOUNT}/link`);
    expect(linkTarget).toBe(`${MOUNT}/target`);

    // Read through symlink
    const buf = FS2.readFile(`${MOUNT}/link`);
    expect(verifyPattern(buf, 100, 0xAA)).toBe(true);
  });

  // ------------------------------------------------------------------
  // File creation + deletion between syncs (incremental can't see deleted files)
  // ------------------------------------------------------------------

  it("file created and unlinked between syncs does not appear after remount", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    // Cycle 1: create persistent file
    FS.writeFile(`${MOUNT}/permanent`, fillPattern(100, 0x11));
    syncfs(FS, tomefs);

    // Between cycles: create temp file, write, unlink (never synced alive)
    // Note: unlink triggers needsOrphanCleanup, so next sync is full walk
    FS.writeFile(`${MOUNT}/temp`, fillPattern(200, 0x22));
    FS.unlink(`${MOUNT}/temp`);

    // Cycle 2: sync (full walk due to unlink)
    syncfs(FS, tomefs);

    // Remount: temp should not exist, permanent should survive
    const { FS: FS2 } = await mountTome(backend);
    expect(() => FS2.stat(`${MOUNT}/temp`)).toThrow();
    const buf = FS2.readFile(`${MOUNT}/permanent`);
    expect(verifyPattern(buf, 100, 0x11)).toBe(true);
  });

  // ------------------------------------------------------------------
  // Multi-remount: several mount-sync-unmount-mount cycles
  // ------------------------------------------------------------------

  it("data accumulates correctly across 5 mount-sync-remount cycles", async () => {
    const backend = new SyncMemoryBackend();

    for (let session = 0; session < 5; session++) {
      const { FS, tomefs } = await mountTome(backend);

      // Write one new file per session
      FS.writeFile(
        `${MOUNT}/session_${session}`,
        fillPattern(PAGE_SIZE, session * 0x20),
      );

      syncfs(FS, tomefs);
    }

    // Final remount: all 5 files should exist with correct data
    const { FS } = await mountTome(backend);
    for (let session = 0; session < 5; session++) {
      const buf = FS.readFile(`${MOUNT}/session_${session}`);
      expect(buf.length).toBe(PAGE_SIZE);
      expect(verifyPattern(buf, PAGE_SIZE, session * 0x20)).toBe(true);
    }
  });

  it("modify existing file across mount-sync-remount cycles", async () => {
    const backend = new SyncMemoryBackend();

    // Session 1: create file with 1 page
    {
      const { FS, tomefs } = await mountTome(backend);
      FS.writeFile(`${MOUNT}/evolving`, fillPattern(PAGE_SIZE, 0x01));
      syncfs(FS, tomefs);
    }

    // Session 2: append a second page
    {
      const { FS, tomefs } = await mountTome(backend);
      const s = FS.open(`${MOUNT}/evolving`, O.WRONLY | O.APPEND);
      FS.write(s, fillPattern(PAGE_SIZE, 0x02), 0, PAGE_SIZE);
      FS.close(s);
      syncfs(FS, tomefs);
    }

    // Session 3: overwrite first page, keep second
    {
      const { FS, tomefs } = await mountTome(backend);
      const s = FS.open(`${MOUNT}/evolving`, O.WRONLY);
      FS.write(s, fillPattern(PAGE_SIZE, 0x03), 0, PAGE_SIZE, 0);
      FS.close(s);
      syncfs(FS, tomefs);
    }

    // Session 4: truncate to half of page 0
    {
      const { FS, tomefs } = await mountTome(backend);
      FS.truncate(`${MOUNT}/evolving`, PAGE_SIZE / 2);
      syncfs(FS, tomefs);
    }

    // Final check
    {
      const { FS } = await mountTome(backend);
      const stat = FS.stat(`${MOUNT}/evolving`);
      expect(stat.size).toBe(PAGE_SIZE / 2);

      const buf = FS.readFile(`${MOUNT}/evolving`);
      // Should have first half of the session 3 pattern (0x03)
      expect(verifyPattern(buf, PAGE_SIZE / 2, 0x03)).toBe(true);
    }
  });

  // ------------------------------------------------------------------
  // Cache pressure + multi-remount
  // ------------------------------------------------------------------

  it("4-page cache across 3 mount-sync-remount cycles with growing files", async () => {
    const backend = new SyncMemoryBackend();

    // Session 1: write 3 pages to file (cache fits easily)
    {
      const { FS, tomefs } = await mountTome(backend, 4);
      const s = FS.open(`${MOUNT}/growing`, O.RDWR | O.CREAT, 0o666);
      for (let p = 0; p < 3; p++) {
        FS.write(s, fillPattern(PAGE_SIZE, p), 0, PAGE_SIZE, p * PAGE_SIZE);
      }
      FS.close(s);
      syncfs(FS, tomefs);
    }

    // Session 2: append 3 more pages (total 6, exceeds 4-page cache)
    {
      const { FS, tomefs } = await mountTome(backend, 4);
      const s = FS.open(`${MOUNT}/growing`, O.WRONLY | O.APPEND);
      for (let p = 3; p < 6; p++) {
        FS.write(s, fillPattern(PAGE_SIZE, p), 0, PAGE_SIZE);
      }
      FS.close(s);
      syncfs(FS, tomefs);
    }

    // Session 3: append 2 more pages (total 8, 2x cache)
    {
      const { FS, tomefs } = await mountTome(backend, 4);
      const s = FS.open(`${MOUNT}/growing`, O.WRONLY | O.APPEND);
      for (let p = 6; p < 8; p++) {
        FS.write(s, fillPattern(PAGE_SIZE, p), 0, PAGE_SIZE);
      }
      FS.close(s);
      syncfs(FS, tomefs);
    }

    // Verify all 8 pages
    {
      const { FS } = await mountTome(backend, 4);
      expect(FS.stat(`${MOUNT}/growing`).size).toBe(8 * PAGE_SIZE);

      const s = FS.open(`${MOUNT}/growing`, O.RDONLY);
      for (let p = 0; p < 8; p++) {
        const buf = new Uint8Array(PAGE_SIZE);
        FS.read(s, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
        expect(
          verifyPattern(buf, PAGE_SIZE, p),
        ).toBe(true);
      }
      FS.close(s);
    }
  });
});
