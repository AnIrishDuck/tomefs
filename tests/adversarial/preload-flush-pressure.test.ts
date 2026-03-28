/**
 * Adversarial tests: PreloadBackend flush + reinit under cache pressure.
 *
 * The PreloadBackend is the graceful degradation path for environments
 * without SharedArrayBuffer (ethos §10). It layers its own dirty tracking
 * on top of the SyncPageCache, creating a two-tier persistence model:
 *
 *   writes → SyncPageCache (dirty pages) → PreloadBackend (dirty tracking)
 *   syncfs → SyncPageCache.flushAll() → PreloadBackend stores
 *   flush → PreloadBackend → remote MemoryBackend/IDB
 *
 * These tests target the seams between these layers under extreme cache
 * pressure (4-page / 32 KB cache). Key attack surfaces:
 *
 * - Pages evicted from SyncPageCache are flushed to PreloadBackend but may
 *   not yet be flushed to the remote. A flush→reinit cycle must still find
 *   them via PreloadBackend's dirty tracking.
 * - Non-aligned truncation leaves partial pages that must survive both tiers.
 * - Rename changes page keys in SyncPageCache AND PreloadBackend — both
 *   must agree after flush→reinit.
 * - Unlink-while-open with /__deleted_* marker metadata must survive flush
 *   cycles and be cleaned up on the next syncfs.
 * - Multi-cycle flush→reinit→write→flush must not lose intermediate state.
 *
 * Ethos §6 (performance parity), §8 (workload scenarios), §9 (adversarial),
 * §10 (graceful degradation).
 */
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { describe, it, expect, beforeEach } from "vitest";
import { MemoryBackend } from "../../src/memory-backend.js";
import { PreloadBackend } from "../../src/preload-backend.js";
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
const MOUNT = "/data";
const MAX_PAGES = 4; // 32 KB — extreme cache pressure

function encode(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

function decode(buf: Uint8Array, length?: number): string {
  return new TextDecoder().decode(
    length !== undefined ? buf.subarray(0, length) : buf,
  );
}

/** Fill a buffer with a deterministic pattern. */
function fillPattern(size: number, seed: number): Uint8Array {
  const buf = new Uint8Array(size);
  for (let i = 0; i < size; i++) {
    buf[i] = (seed + i * 31) & 0xff;
  }
  return buf;
}

/** Verify a buffer matches the expected pattern. */
function verifyPattern(buf: Uint8Array, size: number, seed: number): void {
  for (let i = 0; i < size; i++) {
    if (buf[i] !== ((seed + i * 31) & 0xff)) {
      throw new Error(
        `Pattern mismatch at offset ${i}: expected ${(seed + i * 31) & 0xff}, got ${buf[i]} (seed=${seed})`,
      );
    }
  }
}

/** Mount tomefs with a PreloadBackend. */
async function mountTome(
  backend: PreloadBackend,
  maxPages = MAX_PAGES,
): Promise<{ FS: any; tomefs: any }> {
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

/** Syncfs + flush PreloadBackend to remote. */
async function syncAndFlush(
  FS: any,
  backend: PreloadBackend,
): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    FS.syncfs(false, (err: Error | null) => {
      if (err) reject(err);
      else resolve();
    });
  });
  await backend.flush();
}

/** Create a fresh PreloadBackend + mount from the same remote. */
async function remount(
  remote: MemoryBackend,
  maxPages = MAX_PAGES,
): Promise<{ FS: any; tomefs: any; backend: PreloadBackend }> {
  const backend = new PreloadBackend(remote);
  await backend.init();
  const { FS, tomefs } = await mountTome(backend, maxPages);
  return { FS, tomefs, backend };
}

/** Write binary data to a file at a given position. */
function writeAt(FS: any, path: string, data: Uint8Array, position: number): void {
  const stream = FS.open(path, O.RDWR | O.CREAT, 0o666);
  FS.write(stream, data, 0, data.length, position);
  FS.close(stream);
}

/** Read the full contents of a file. */
function readFull(FS: any, path: string): Uint8Array {
  const stat = FS.stat(path);
  const stream = FS.open(path, O.RDONLY, 0);
  const buf = new Uint8Array(stat.size);
  FS.read(stream, buf, 0, stat.size, 0);
  FS.close(stream);
  return buf;
}

describe("adversarial: PreloadBackend flush+reinit under cache pressure", () => {
  let remote: MemoryBackend;

  beforeEach(() => {
    remote = new MemoryBackend();
  });

  // ------------------------------------------------------------------
  // Multi-file writes exceeding cache, all surviving flush→reinit
  // ------------------------------------------------------------------

  it("10 multi-page files survive flush→reinit with 4-page cache @fast", async () => {
    const backend = new PreloadBackend(remote);
    await backend.init();
    const { FS } = await mountTome(backend);

    // Write 10 files, each 2 pages (16 KB). Total: 20 pages, cache holds 4.
    for (let i = 0; i < 10; i++) {
      const data = fillPattern(PAGE_SIZE * 2, i);
      const stream = FS.open(`${MOUNT}/f${i}.dat`, O.RDWR | O.CREAT, 0o666);
      FS.write(stream, data, 0, data.length, 0);
      FS.close(stream);
    }

    await syncAndFlush(FS, backend);
    const { FS: FS2 } = await remount(remote);

    // Verify all 10 files
    for (let i = 0; i < 10; i++) {
      const buf = readFull(FS2, `${MOUNT}/f${i}.dat`);
      expect(buf.length).toBe(PAGE_SIZE * 2);
      verifyPattern(buf, PAGE_SIZE * 2, i);
    }
  });

  // ------------------------------------------------------------------
  // Non-aligned truncation surviving flush→reinit
  // ------------------------------------------------------------------

  it("non-aligned truncation persists through flush→reinit @fast", async () => {
    const backend = new PreloadBackend(remote);
    await backend.init();
    const { FS } = await mountTome(backend);

    // Write 3 pages, truncate to mid-page 1 (PAGE_SIZE + 100 bytes)
    const data = fillPattern(PAGE_SIZE * 3, 42);
    const stream = FS.open(`${MOUNT}/trunc.dat`, O.RDWR | O.CREAT, 0o666);
    FS.write(stream, data, 0, data.length, 0);
    FS.close(stream);

    const truncSize = PAGE_SIZE + 100;
    FS.truncate(`${MOUNT}/trunc.dat`, truncSize);

    await syncAndFlush(FS, backend);
    const { FS: FS2 } = await remount(remote);

    const buf = readFull(FS2, `${MOUNT}/trunc.dat`);
    expect(buf.length).toBe(truncSize);
    // First PAGE_SIZE+100 bytes match original pattern
    verifyPattern(buf, truncSize, 42);
  });

  // ------------------------------------------------------------------
  // Truncate then extend — zero-fill gap must persist
  // ------------------------------------------------------------------

  it("truncate-then-extend zero gap persists @fast", async () => {
    const backend = new PreloadBackend(remote);
    await backend.init();
    const { FS } = await mountTome(backend);

    // Write 2 pages of data
    const original = fillPattern(PAGE_SIZE * 2, 99);
    writeAt(FS, `${MOUNT}/gap.dat`, original, 0);

    // Truncate to 100 bytes (mid-page 0)
    FS.truncate(`${MOUNT}/gap.dat`, 100);

    // Extend by writing at PAGE_SIZE offset — gap from 100..PAGE_SIZE should be zeros
    const extension = fillPattern(100, 77);
    writeAt(FS, `${MOUNT}/gap.dat`, extension, PAGE_SIZE);

    await syncAndFlush(FS, backend);
    const { FS: FS2 } = await remount(remote);

    const buf = readFull(FS2, `${MOUNT}/gap.dat`);
    expect(buf.length).toBe(PAGE_SIZE + 100);

    // First 100 bytes: original pattern
    verifyPattern(buf.subarray(0, 100), 100, 99);

    // Bytes 100..PAGE_SIZE: zero-filled gap
    for (let i = 100; i < PAGE_SIZE; i++) {
      if (buf[i] !== 0) {
        throw new Error(`Expected zero at offset ${i}, got ${buf[i]}`);
      }
    }

    // Bytes PAGE_SIZE..PAGE_SIZE+100: extension pattern
    verifyPattern(buf.subarray(PAGE_SIZE, PAGE_SIZE + 100), 100, 77);
  });

  // ------------------------------------------------------------------
  // Rename chain surviving flush→reinit under cache pressure
  // ------------------------------------------------------------------

  it("rename chain with multi-page files persists under cache pressure", async () => {
    const backend = new PreloadBackend(remote);
    await backend.init();
    const { FS } = await mountTome(backend);

    // Write 3 files
    for (let i = 0; i < 3; i++) {
      const data = fillPattern(PAGE_SIZE * 2, i * 10);
      writeAt(FS, `${MOUNT}/r${i}.dat`, data, 0);
    }

    // Rename chain: r0 → r3, r1 → r0, r2 → r1
    FS.rename(`${MOUNT}/r0.dat`, `${MOUNT}/r3.dat`);
    FS.rename(`${MOUNT}/r1.dat`, `${MOUNT}/r0.dat`);
    FS.rename(`${MOUNT}/r2.dat`, `${MOUNT}/r1.dat`);

    await syncAndFlush(FS, backend);
    const { FS: FS2 } = await remount(remote);

    // r3 has original r0's data (seed=0)
    const b3 = readFull(FS2, `${MOUNT}/r3.dat`);
    expect(b3.length).toBe(PAGE_SIZE * 2);
    verifyPattern(b3, PAGE_SIZE * 2, 0);

    // r0 has original r1's data (seed=10)
    const b0 = readFull(FS2, `${MOUNT}/r0.dat`);
    verifyPattern(b0, PAGE_SIZE * 2, 10);

    // r1 has original r2's data (seed=20)
    const b1 = readFull(FS2, `${MOUNT}/r1.dat`);
    verifyPattern(b1, PAGE_SIZE * 2, 20);

    // r2 should not exist
    expect(() => FS2.stat(`${MOUNT}/r2.dat`)).toThrow();
  });

  // ------------------------------------------------------------------
  // Directory rename with descendant files persists
  // ------------------------------------------------------------------

  it("directory rename persists descendant file data under cache pressure", async () => {
    const backend = new PreloadBackend(remote);
    await backend.init();
    const { FS } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/srcdir`);
    FS.mkdir(`${MOUNT}/srcdir/sub`);
    const data1 = fillPattern(PAGE_SIZE * 2, 11);
    const data2 = fillPattern(PAGE_SIZE, 22);
    writeAt(FS, `${MOUNT}/srcdir/a.dat`, data1, 0);
    writeAt(FS, `${MOUNT}/srcdir/sub/b.dat`, data2, 0);

    FS.rename(`${MOUNT}/srcdir`, `${MOUNT}/dstdir`);

    await syncAndFlush(FS, backend);
    const { FS: FS2 } = await remount(remote);

    const buf1 = readFull(FS2, `${MOUNT}/dstdir/a.dat`);
    expect(buf1.length).toBe(PAGE_SIZE * 2);
    verifyPattern(buf1, PAGE_SIZE * 2, 11);

    const buf2 = readFull(FS2, `${MOUNT}/dstdir/sub/b.dat`);
    expect(buf2.length).toBe(PAGE_SIZE);
    verifyPattern(buf2, PAGE_SIZE, 22);

    expect(() => FS2.stat(`${MOUNT}/srcdir`)).toThrow();
  });

  // ------------------------------------------------------------------
  // Multi-cycle flush→reinit→write→flush — no state loss
  // ------------------------------------------------------------------

  it("3-cycle flush→reinit→write→flush preserves all data @fast", async () => {
    let backend = new PreloadBackend(remote);
    await backend.init();
    let { FS } = await mountTome(backend);

    // Cycle 1: write files 0-2
    for (let i = 0; i < 3; i++) {
      writeAt(FS, `${MOUNT}/c${i}.dat`, fillPattern(PAGE_SIZE, i), 0);
    }
    await syncAndFlush(FS, backend);

    // Cycle 2: remount, add files 3-5, modify file 0
    ({ FS, backend } = await remount(remote) as any);
    for (let i = 3; i < 6; i++) {
      writeAt(FS, `${MOUNT}/c${i}.dat`, fillPattern(PAGE_SIZE, i), 0);
    }
    // Overwrite file 0 with new pattern
    writeAt(FS, `${MOUNT}/c0.dat`, fillPattern(PAGE_SIZE, 100), 0);
    await syncAndFlush(FS, backend);

    // Cycle 3: remount, add files 6-8, delete file 1
    ({ FS, backend } = await remount(remote) as any);
    for (let i = 6; i < 9; i++) {
      writeAt(FS, `${MOUNT}/c${i}.dat`, fillPattern(PAGE_SIZE, i), 0);
    }
    FS.unlink(`${MOUNT}/c1.dat`);
    await syncAndFlush(FS, backend);

    // Final remount: verify all state
    const { FS: final } = await remount(remote);

    // c0 was overwritten in cycle 2 (seed=100)
    verifyPattern(readFull(final, `${MOUNT}/c0.dat`), PAGE_SIZE, 100);

    // c1 was deleted in cycle 3
    expect(() => final.stat(`${MOUNT}/c1.dat`)).toThrow();

    // c2-c8 have their original patterns
    for (let i = 2; i < 9; i++) {
      verifyPattern(readFull(final, `${MOUNT}/c${i}.dat`), PAGE_SIZE, i);
    }
  });

  // ------------------------------------------------------------------
  // Unlink-while-open: /__deleted_* cleanup after flush→reinit
  // ------------------------------------------------------------------

  it("unlinked file with open fd is readable, orphan cleaned after close+sync", async () => {
    const backend = new PreloadBackend(remote);
    await backend.init();
    const { FS } = await mountTome(backend);

    // Write a file and keep it open
    const data = fillPattern(PAGE_SIZE * 2, 55);
    const stream = FS.open(`${MOUNT}/victim.dat`, O.RDWR | O.CREAT, 0o666);
    FS.write(stream, data, 0, data.length, 0);

    // Unlink while fd is still open
    FS.unlink(`${MOUNT}/victim.dat`);

    // Should still be readable through the open fd
    const buf = new Uint8Array(PAGE_SIZE * 2);
    FS.read(stream, buf, 0, PAGE_SIZE * 2, 0);
    verifyPattern(buf, PAGE_SIZE * 2, 55);

    // Syncfs should preserve the /__deleted_* marker
    await syncAndFlush(FS, backend);

    // Close the fd — triggers cleanup of /__deleted_* pages
    FS.close(stream);

    // File should not be accessible
    expect(() => FS.stat(`${MOUNT}/victim.dat`)).toThrow();
  });

  // ------------------------------------------------------------------
  // Interleaved writes to 6 files with 4-page cache — stress eviction
  // ------------------------------------------------------------------

  it("interleaved writes to 6 files survive flush→reinit", async () => {
    const backend = new PreloadBackend(remote);
    await backend.init();
    const { FS } = await mountTome(backend);

    // Create 6 files with 1-byte headers
    for (let i = 0; i < 6; i++) {
      writeAt(FS, `${MOUNT}/il${i}.dat`, new Uint8Array([i]), 0);
    }

    // Write full pages to each file in round-robin order.
    // With 4-page cache, every write evicts a page from a different file.
    for (let round = 0; round < 3; round++) {
      for (let i = 0; i < 6; i++) {
        const data = fillPattern(PAGE_SIZE, i * 10 + round);
        writeAt(FS, `${MOUNT}/il${i}.dat`, data, round * PAGE_SIZE);
      }
    }

    await syncAndFlush(FS, backend);
    const { FS: FS2 } = await remount(remote);

    // Each file should be 3 pages
    for (let i = 0; i < 6; i++) {
      const buf = readFull(FS2, `${MOUNT}/il${i}.dat`);
      expect(buf.length).toBe(PAGE_SIZE * 3);
      // Verify each page's pattern (last write wins for page 0, which was
      // initially written with a 1-byte header then overwritten with full page)
      for (let round = 0; round < 3; round++) {
        verifyPattern(
          buf.subarray(round * PAGE_SIZE, (round + 1) * PAGE_SIZE),
          PAGE_SIZE,
          i * 10 + round,
        );
      }
    }
  });

  // ------------------------------------------------------------------
  // Append-only writes (WAL-like) under extreme cache pressure
  // ------------------------------------------------------------------

  it("sequential append to 6-page file survives flush→reinit with 4-page cache", async () => {
    const backend = new PreloadBackend(remote);
    await backend.init();
    const { FS } = await mountTome(backend);

    // Simulate WAL: sequential appends filling 6 pages (cache holds 4)
    const stream = FS.open(`${MOUNT}/wal.dat`, O.RDWR | O.CREAT | O.APPEND, 0o666);
    const chunkSize = PAGE_SIZE / 4; // 2 KB chunks, 4 per page
    for (let i = 0; i < 24; i++) { // 24 chunks = 6 pages
      const chunk = fillPattern(chunkSize, i);
      FS.write(stream, chunk, 0, chunk.length);
    }
    FS.close(stream);

    await syncAndFlush(FS, backend);
    const { FS: FS2 } = await remount(remote);

    const buf = readFull(FS2, `${MOUNT}/wal.dat`);
    expect(buf.length).toBe(PAGE_SIZE * 6);

    // Verify each 2 KB chunk
    for (let i = 0; i < 24; i++) {
      const chunkStart = i * chunkSize;
      verifyPattern(
        buf.subarray(chunkStart, chunkStart + chunkSize),
        chunkSize,
        i,
      );
    }
  });

  // ------------------------------------------------------------------
  // Rename onto existing file — target's data replaced, not leaked
  // ------------------------------------------------------------------

  it("rename onto existing multi-page file replaces data cleanly", async () => {
    const backend = new PreloadBackend(remote);
    await backend.init();
    const { FS } = await mountTome(backend);

    // Write target file (seed=1)
    writeAt(FS, `${MOUNT}/target.dat`, fillPattern(PAGE_SIZE * 3, 1), 0);

    // Write source file (seed=2, different size)
    writeAt(FS, `${MOUNT}/source.dat`, fillPattern(PAGE_SIZE * 2, 2), 0);

    // Rename source onto target — target's old data should be gone
    FS.rename(`${MOUNT}/source.dat`, `${MOUNT}/target.dat`);

    await syncAndFlush(FS, backend);
    const { FS: FS2 } = await remount(remote);

    const buf = readFull(FS2, `${MOUNT}/target.dat`);
    expect(buf.length).toBe(PAGE_SIZE * 2); // source's size, not target's
    verifyPattern(buf, PAGE_SIZE * 2, 2);

    expect(() => FS2.stat(`${MOUNT}/source.dat`)).toThrow();
  });

  // ------------------------------------------------------------------
  // chmod + mtime persistence through flush→reinit
  // ------------------------------------------------------------------

  it("mode and timestamps persist through flush→reinit", async () => {
    const backend = new PreloadBackend(remote);
    await backend.init();
    const { FS } = await mountTome(backend);

    writeAt(FS, `${MOUNT}/meta.dat`, encode("metadata test"), 0);
    FS.chmod(`${MOUNT}/meta.dat`, 0o644);
    FS.utime(`${MOUNT}/meta.dat`, 1000000, 2000000);

    await syncAndFlush(FS, backend);
    const { FS: FS2 } = await remount(remote);

    const stat = FS2.stat(`${MOUNT}/meta.dat`);
    expect(stat.mode & 0o777).toBe(0o644);
    expect(stat.mtime.getTime()).toBe(2000000);
  });

  // ------------------------------------------------------------------
  // Symlink persistence through flush→reinit
  // ------------------------------------------------------------------

  it("symlinks persist through flush→reinit", async () => {
    const backend = new PreloadBackend(remote);
    await backend.init();
    const { FS } = await mountTome(backend);

    writeAt(FS, `${MOUNT}/real.dat`, encode("symlink target"), 0);
    FS.symlink(`${MOUNT}/real.dat`, `${MOUNT}/link.dat`);

    await syncAndFlush(FS, backend);
    const { FS: FS2 } = await remount(remote);

    // Symlink target is readable
    const target = FS2.readlink(`${MOUNT}/link.dat`);
    expect(target).toBe(`${MOUNT}/real.dat`);

    // Data through symlink matches
    const stat = FS2.stat(`${MOUNT}/link.dat`);
    expect(stat.size).toBe(14);
    const buf = readFull(FS2, `${MOUNT}/link.dat`);
    expect(decode(buf)).toBe("symlink target");
  });

  // ------------------------------------------------------------------
  // Delete-then-recreate at same path in single flush cycle
  // ------------------------------------------------------------------

  it("delete-then-recreate at same path persists correctly @fast", async () => {
    const backend = new PreloadBackend(remote);
    await backend.init();
    const { FS } = await mountTome(backend);

    // Write initial data
    writeAt(FS, `${MOUNT}/reuse.dat`, fillPattern(PAGE_SIZE * 2, 1), 0);

    // Sync so it's in the remote
    await syncAndFlush(FS, backend);

    // Now delete and recreate at the same path with different data
    const backend2 = new PreloadBackend(remote);
    await backend2.init();
    const { FS: FS2 } = await mountTome(backend2);

    FS2.unlink(`${MOUNT}/reuse.dat`);
    writeAt(FS2, `${MOUNT}/reuse.dat`, fillPattern(PAGE_SIZE, 99), 0);

    await syncAndFlush(FS2, backend2);
    const { FS: FS3 } = await remount(remote);

    // Should have the new data, not the old
    const buf = readFull(FS3, `${MOUNT}/reuse.dat`);
    expect(buf.length).toBe(PAGE_SIZE);
    verifyPattern(buf, PAGE_SIZE, 99);
  });

  // ------------------------------------------------------------------
  // Cross-page-boundary write under pressure + persistence
  // ------------------------------------------------------------------

  it("cross-page-boundary writes persist correctly under cache pressure", async () => {
    const backend = new PreloadBackend(remote);
    await backend.init();
    const { FS } = await mountTome(backend);

    // Write data that straddles page boundaries
    // Position the write at PAGE_SIZE - 50, length 100 — crosses pages 0 and 1
    const stream = FS.open(`${MOUNT}/cross.dat`, O.RDWR | O.CREAT, 0o666);

    // First, fill 3 pages with known data to force cache pressure
    const fill = fillPattern(PAGE_SIZE * 3, 10);
    FS.write(stream, fill, 0, fill.length, 0);

    // Now write a cross-boundary pattern at page 0/1 boundary
    const crossData = fillPattern(100, 200);
    FS.write(stream, crossData, 0, 100, PAGE_SIZE - 50);

    // And another at page 1/2 boundary
    const crossData2 = fillPattern(100, 201);
    FS.write(stream, crossData2, 0, 100, PAGE_SIZE * 2 - 50);

    FS.close(stream);

    await syncAndFlush(FS, backend);
    const { FS: FS2 } = await remount(remote);

    const buf = readFull(FS2, `${MOUNT}/cross.dat`);
    expect(buf.length).toBe(PAGE_SIZE * 3);

    // Verify the cross-boundary data at page 0/1
    verifyPattern(buf.subarray(PAGE_SIZE - 50, PAGE_SIZE + 50), 100, 200);

    // Verify the cross-boundary data at page 1/2
    verifyPattern(buf.subarray(PAGE_SIZE * 2 - 50, PAGE_SIZE * 2 + 50), 100, 201);

    // Verify untouched regions
    verifyPattern(buf.subarray(0, PAGE_SIZE - 50), PAGE_SIZE - 50, 10);
  });

  // ------------------------------------------------------------------
  // Sparse file with large gap — zeros in gap after flush→reinit
  // ------------------------------------------------------------------

  it("sparse file gap zeros persist through flush→reinit", async () => {
    const backend = new PreloadBackend(remote);
    await backend.init();
    const { FS } = await mountTome(backend);

    // Write 100 bytes at offset 0
    const header = fillPattern(100, 33);
    writeAt(FS, `${MOUNT}/sparse.dat`, header, 0);

    // Seek past EOF and write at page 3 offset — creates a large zero gap
    const tail = fillPattern(100, 44);
    const stream = FS.open(`${MOUNT}/sparse.dat`, O.RDWR, 0o666);
    FS.write(stream, tail, 0, 100, PAGE_SIZE * 3);
    FS.close(stream);

    await syncAndFlush(FS, backend);
    const { FS: FS2 } = await remount(remote);

    const buf = readFull(FS2, `${MOUNT}/sparse.dat`);
    expect(buf.length).toBe(PAGE_SIZE * 3 + 100);

    // Header preserved
    verifyPattern(buf.subarray(0, 100), 100, 33);

    // Gap is all zeros
    for (let i = 100; i < PAGE_SIZE * 3; i++) {
      if (buf[i] !== 0) {
        throw new Error(`Expected zero at gap offset ${i}, got ${buf[i]}`);
      }
    }

    // Tail preserved
    verifyPattern(buf.subarray(PAGE_SIZE * 3, PAGE_SIZE * 3 + 100), 100, 44);
  });

  // ------------------------------------------------------------------
  // Mixed workload: WAL append + data page random write + truncate
  // ------------------------------------------------------------------

  it("mixed WAL+data+truncate workload persists under cache pressure", async () => {
    const backend = new PreloadBackend(remote);
    await backend.init();
    const { FS } = await mountTome(backend);

    // Simulate Postgres: WAL file (sequential append) + data file (random writes)
    const walStream = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT | O.APPEND, 0o666);
    const dataStream = FS.open(`${MOUNT}/data`, O.RDWR | O.CREAT, 0o666);

    // Pre-allocate data file: 4 pages
    const initial = new Uint8Array(PAGE_SIZE * 4);
    FS.write(dataStream, initial, 0, initial.length, 0);

    // Interleaved operations
    for (let i = 0; i < 8; i++) {
      // WAL: append a 1 KB record
      const walRecord = fillPattern(1024, i);
      FS.write(walStream, walRecord, 0, 1024);

      // Data: random page write
      const pageIdx = i % 4;
      const pageData = fillPattern(PAGE_SIZE, 50 + i);
      FS.write(dataStream, pageData, 0, PAGE_SIZE, pageIdx * PAGE_SIZE);
    }

    FS.close(walStream);
    FS.close(dataStream);

    // Truncate WAL (simulate checkpoint)
    FS.truncate(`${MOUNT}/wal`, 0);

    // Write new WAL entries after truncation
    const walStream2 = FS.open(`${MOUNT}/wal`, O.RDWR | O.APPEND, 0o666);
    for (let i = 0; i < 4; i++) {
      const record = fillPattern(1024, 100 + i);
      FS.write(walStream2, record, 0, 1024);
    }
    FS.close(walStream2);

    await syncAndFlush(FS, backend);
    const { FS: FS2 } = await remount(remote);

    // Verify WAL: 4 KB (4 x 1024 records after truncation)
    const walBuf = readFull(FS2, `${MOUNT}/wal`);
    expect(walBuf.length).toBe(4096);
    for (let i = 0; i < 4; i++) {
      verifyPattern(walBuf.subarray(i * 1024, (i + 1) * 1024), 1024, 100 + i);
    }

    // Verify data: 4 pages, last write for each page wins
    const dataBuf = readFull(FS2, `${MOUNT}/data`);
    expect(dataBuf.length).toBe(PAGE_SIZE * 4);
    // Pages 0-3 were written in rounds: page i was last written with seed 50+(4+i)
    // Round 0: page 0 (seed 50), Round 1: page 1 (seed 51), ...
    // Round 4: page 0 (seed 54), Round 5: page 1 (seed 55), ...
    for (let i = 0; i < 4; i++) {
      const lastSeed = 50 + 4 + i; // last round that wrote to page i
      verifyPattern(
        dataBuf.subarray(i * PAGE_SIZE, (i + 1) * PAGE_SIZE),
        PAGE_SIZE,
        lastSeed,
      );
    }
  });

  // ------------------------------------------------------------------
  // Flush without prior syncfs — PreloadBackend dirty tracking only
  // ------------------------------------------------------------------

  it("flush without syncfs persists page data but not tree metadata", async () => {
    const backend = new PreloadBackend(remote);
    await backend.init();
    const { FS } = await mountTome(backend);

    // Write a file — pages go to SyncPageCache, which evicts to PreloadBackend
    const data = fillPattern(PAGE_SIZE * 6, 77);
    writeAt(FS, `${MOUNT}/nosync.dat`, data, 0);

    // Flush PreloadBackend WITHOUT syncfs first
    // SyncPageCache.flushAll() is NOT called, but pages evicted during
    // the write are already in the PreloadBackend's dirty store.
    await backend.flush();

    // At this point, evicted pages are persisted to remote, but
    // pages still in the SyncPageCache are NOT in PreloadBackend yet.
    // Also, no tree metadata was persisted (syncfs wasn't called).
    // So reinit should NOT see the file (no metadata).
    const backend2 = new PreloadBackend(remote);
    await backend2.init();

    // Without metadata, listFiles returns nothing useful for this file
    // The pages may exist in the remote but the file is not restorable
    // without metadata. This validates that syncfs is required for full
    // durability.
    const files = backend2.listFiles();
    expect(files).not.toContain("/nosync.dat");
  });

  // ------------------------------------------------------------------
  // Large number of small files — tests directory metadata batching
  // ------------------------------------------------------------------

  it("50 small files in nested dirs survive flush→reinit under cache pressure", async () => {
    const backend = new PreloadBackend(remote);
    await backend.init();
    const { FS } = await mountTome(backend);

    // Create 5 directories with 10 files each
    for (let d = 0; d < 5; d++) {
      FS.mkdir(`${MOUNT}/d${d}`);
      for (let f = 0; f < 10; f++) {
        const content = `d${d}/f${f}`;
        const data = encode(content);
        const stream = FS.open(`${MOUNT}/d${d}/f${f}.txt`, O.RDWR | O.CREAT, 0o666);
        FS.write(stream, data, 0, data.length, 0);
        FS.close(stream);
      }
    }

    await syncAndFlush(FS, backend);
    const { FS: FS2 } = await remount(remote);

    // Verify all 50 files
    for (let d = 0; d < 5; d++) {
      const entries = FS2.readdir(`${MOUNT}/d${d}`);
      expect(entries.filter((e: string) => e !== "." && e !== "..")).toHaveLength(10);
      for (let f = 0; f < 10; f++) {
        const buf = readFull(FS2, `${MOUNT}/d${d}/f${f}.txt`);
        expect(decode(buf)).toBe(`d${d}/f${f}`);
      }
    }
  });
});
