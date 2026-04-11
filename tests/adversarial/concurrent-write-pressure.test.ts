/**
 * Adversarial tests: Concurrent multi-fd writes under cache pressure with
 * persistence verification.
 *
 * These tests target the intersection of three dimensions no single existing
 * test covers:
 *   1. Multiple open file descriptors writing to the same file
 *   2. Writes that straddle page boundaries (PAGE_SIZE = 8192)
 *   3. Extreme cache pressure (4-page LRU) forcing eviction mid-operation
 *
 * After each scenario, data is persisted via syncfs, the filesystem is
 * unmounted and remounted from the same backend, and all bytes are verified.
 *
 * Real-world motivation: Postgres backends can write to the same relation
 * file at different offsets. Under tomefs, each write may evict dirty pages
 * belonging to a different fd's recent write — the page cache must not lose
 * or corrupt data across this eviction/reload cycle.
 *
 * Ethos §6 (performance parity), §9 (adversarial differential testing).
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

const MOUNT = "/tome";
const MAX_PAGES = 4; // 32 KB cache — extreme pressure

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

/** Verify a buffer matches the expected pattern. */
function verifyPattern(buf: Uint8Array, size: number, seed: number): boolean {
  for (let i = 0; i < size; i++) {
    if (buf[i] !== ((seed + i * 31) & 0xff)) return false;
  }
  return true;
}

async function mountTome(backend: SyncMemoryBackend, maxPages = MAX_PAGES) {
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

describe("adversarial: concurrent multi-fd writes under cache pressure with persistence", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  // ------------------------------------------------------------------
  // Two fds writing overlapping page-boundary regions
  // ------------------------------------------------------------------

  it("overlapping page-boundary writes from two fds persist correctly @fast", async () => {
    // fd-A writes bytes 8188..8195 (straddles page 0/1 boundary)
    // fd-B writes bytes 8192..8199 (overwrites first 4 bytes of page 1)
    // Last-write-wins: page 0 tail from A, page 1 head from B
    const { FS, tomefs } = await mountTome(backend);

    const fdA = FS.open(`${MOUNT}/overlap`, O.RDWR | O.CREAT, 0o666);
    const fdB = FS.open(`${MOUNT}/overlap`, O.RDWR);

    // fd-A: write 0xAA pattern at offset 8188, length 8
    const dataA = new Uint8Array(8);
    dataA.fill(0xaa);
    FS.write(fdA, dataA, 0, 8, PAGE_SIZE - 4);

    // fd-B: write 0xBB pattern at offset 8192, length 8
    const dataB = new Uint8Array(8);
    dataB.fill(0xbb);
    FS.write(fdB, dataB, 0, 8, PAGE_SIZE);

    FS.close(fdA);
    FS.close(fdB);

    syncAndUnmount(FS, tomefs);

    // Remount and verify byte-level correctness
    const { FS: FS2 } = await mountTome(backend);
    const buf = new Uint8Array(PAGE_SIZE + 8);
    const r = FS2.open(`${MOUNT}/overlap`, O.RDONLY);
    FS2.read(r, buf, 0, buf.length, 0);
    FS2.close(r);

    // Bytes 0..8187: zeros (never written)
    for (let i = 0; i < PAGE_SIZE - 4; i++) {
      expect(buf[i]).toBe(0);
    }
    // Bytes 8188..8191: 0xAA from fd-A (page 0 tail)
    for (let i = PAGE_SIZE - 4; i < PAGE_SIZE; i++) {
      expect(buf[i]).toBe(0xaa);
    }
    // Bytes 8192..8199: 0xBB from fd-B (overwrote fd-A's page 1 portion)
    for (let i = PAGE_SIZE; i < PAGE_SIZE + 8; i++) {
      expect(buf[i]).toBe(0xbb);
    }
  });

  // ------------------------------------------------------------------
  // Alternating writes from two fds, each straddling page boundaries
  // ------------------------------------------------------------------

  it("alternating cross-page writes from two fds survive eviction + persist", async () => {
    // Two fds alternate writing 256-byte chunks that straddle page boundaries.
    // With a 4-page cache and writes spanning 3+ pages, pages are constantly
    // evicted and reloaded. Each chunk has a unique fill byte for verification.
    const { FS, tomefs } = await mountTome(backend);

    const fdA = FS.open(`${MOUNT}/alternate`, O.RDWR | O.CREAT, 0o666);
    const fdB = FS.open(`${MOUNT}/alternate`, O.RDWR);

    const chunkSize = 256;
    const rounds = 16;

    // Pre-compute expected file contents
    const fileSize = PAGE_SIZE * 3; // write within first 3 pages
    const expected = new Uint8Array(fileSize);

    for (let r = 0; r < rounds; r++) {
      // Alternate between fdA and fdB
      const fd = r % 2 === 0 ? fdA : fdB;
      const fillByte = (r * 17 + 3) & 0xff;
      const chunk = new Uint8Array(chunkSize);
      chunk.fill(fillByte);

      // Write at an offset that straddles a page boundary
      // Offsets cycle through boundary-straddling positions
      const offset = PAGE_SIZE - chunkSize / 2 + (r * 100) % (PAGE_SIZE * 2);
      FS.write(fd, chunk, 0, chunkSize, offset);

      // Track expected state (last-write-wins)
      for (let i = 0; i < chunkSize; i++) {
        const pos = offset + i;
        if (pos < fileSize) {
          expected[pos] = fillByte;
        }
      }
    }

    FS.close(fdA);
    FS.close(fdB);

    syncAndUnmount(FS, tomefs);

    // Remount and verify
    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/alternate`);
    const buf = new Uint8Array(stat.size);
    const r2 = FS2.open(`${MOUNT}/alternate`, O.RDONLY);
    FS2.read(r2, buf, 0, buf.length, 0);
    FS2.close(r2);

    // Verify every byte within the expected region
    const verifyLen = Math.min(buf.length, fileSize);
    for (let i = 0; i < verifyLen; i++) {
      expect(buf[i]).toBe(expected[i]);
    }
  });

  // ------------------------------------------------------------------
  // Multiple fds writing to distinct page-aligned regions of same file
  // ------------------------------------------------------------------

  it("three fds writing distinct regions of same file under pressure persist", async () => {
    // fd-A writes pages 0-1, fd-B writes pages 2-3, fd-C writes pages 4-5.
    // With a 4-page cache, writing 6 pages forces eviction. Each fd's writes
    // must not corrupt the others.
    const { FS, tomefs } = await mountTome(backend);

    const fdA = FS.open(`${MOUNT}/regions`, O.RDWR | O.CREAT, 0o666);
    const fdB = FS.open(`${MOUNT}/regions`, O.RDWR);
    const fdC = FS.open(`${MOUNT}/regions`, O.RDWR);

    const dataA = fillPattern(PAGE_SIZE * 2, 10);
    const dataB = fillPattern(PAGE_SIZE * 2, 20);
    const dataC = fillPattern(PAGE_SIZE * 2, 30);

    FS.write(fdA, dataA, 0, dataA.length, 0);
    FS.write(fdB, dataB, 0, dataB.length, PAGE_SIZE * 2);
    FS.write(fdC, dataC, 0, dataC.length, PAGE_SIZE * 4);

    FS.close(fdA);
    FS.close(fdB);
    FS.close(fdC);

    syncAndUnmount(FS, tomefs);

    // Remount and verify each region
    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/regions`);
    expect(stat.size).toBe(PAGE_SIZE * 6);

    const buf = new Uint8Array(PAGE_SIZE * 6);
    const rd = FS2.open(`${MOUNT}/regions`, O.RDONLY);
    FS2.read(rd, buf, 0, buf.length, 0);
    FS2.close(rd);

    expect(verifyPattern(buf.subarray(0, PAGE_SIZE * 2), PAGE_SIZE * 2, 10)).toBe(true);
    expect(verifyPattern(buf.subarray(PAGE_SIZE * 2, PAGE_SIZE * 4), PAGE_SIZE * 2, 20)).toBe(true);
    expect(verifyPattern(buf.subarray(PAGE_SIZE * 4, PAGE_SIZE * 6), PAGE_SIZE * 2, 30)).toBe(true);
  });

  // ------------------------------------------------------------------
  // Read through one fd while another writes under cache pressure
  // ------------------------------------------------------------------

  it("reader fd sees writer fd data through eviction cycle", async () => {
    // Writer fills 8 pages (64 KB) sequentially. After each page-write,
    // reader verifies the just-written page. With a 4-page cache, earlier
    // pages are evicted by the time the reader checks them — they must be
    // correctly reloaded from the backend (where they landed via eviction flush).
    const { FS, tomefs } = await mountTome(backend);

    const writer = FS.open(`${MOUNT}/readwrite`, O.RDWR | O.CREAT, 0o666);
    const reader = FS.open(`${MOUNT}/readwrite`, O.RDONLY);

    const pageCount = 8;
    for (let p = 0; p < pageCount; p++) {
      // Write one page
      const data = new Uint8Array(PAGE_SIZE);
      data.fill(p + 1);
      FS.write(writer, data, 0, PAGE_SIZE, p * PAGE_SIZE);

      // Read back through reader fd — may trigger cache reload
      const buf = new Uint8Array(PAGE_SIZE);
      const n = FS.read(reader, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expect(n).toBe(PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) {
        expect(buf[i]).toBe(p + 1);
      }
    }

    // Now go back and re-read page 0 (long evicted)
    const firstPage = new Uint8Array(PAGE_SIZE);
    const n = FS.read(reader, firstPage, 0, PAGE_SIZE, 0);
    expect(n).toBe(PAGE_SIZE);
    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(firstPage[i]).toBe(1);
    }

    FS.close(writer);
    FS.close(reader);

    // Persist and verify on remount
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const full = new Uint8Array(pageCount * PAGE_SIZE);
    const rd = FS2.open(`${MOUNT}/readwrite`, O.RDONLY);
    FS2.read(rd, full, 0, full.length, 0);
    FS2.close(rd);

    for (let p = 0; p < pageCount; p++) {
      for (let i = 0; i < PAGE_SIZE; i++) {
        expect(full[p * PAGE_SIZE + i]).toBe(p + 1);
      }
    }
  });

  // ------------------------------------------------------------------
  // Two fds doing interleaved page-boundary writes to different files
  // competing for the same 4-page cache
  // ------------------------------------------------------------------

  it("two files competing for cache slots with cross-page writes persist", async () => {
    // fd-A writes to /fileA, fd-B writes to /fileB. Both files grow to 3 pages.
    // Interleaved writes force pages from one file to evict the other's.
    const { FS, tomefs } = await mountTome(backend);

    const fdA = FS.open(`${MOUNT}/fileA`, O.RDWR | O.CREAT, 0o666);
    const fdB = FS.open(`${MOUNT}/fileB`, O.RDWR | O.CREAT, 0o666);

    // Interleave: write 1 page to A, 1 page to B, repeat
    for (let p = 0; p < 3; p++) {
      const chunkA = new Uint8Array(PAGE_SIZE);
      chunkA.fill(0x10 + p);
      FS.write(fdA, chunkA, 0, PAGE_SIZE, p * PAGE_SIZE);

      const chunkB = new Uint8Array(PAGE_SIZE);
      chunkB.fill(0x80 + p);
      FS.write(fdB, chunkB, 0, PAGE_SIZE, p * PAGE_SIZE);
    }

    // Now write a cross-boundary region to each file
    const crossA = new Uint8Array(64);
    crossA.fill(0xff);
    FS.write(fdA, crossA, 0, 64, PAGE_SIZE - 32); // straddles page 0/1

    const crossB = new Uint8Array(64);
    crossB.fill(0xee);
    FS.write(fdB, crossB, 0, 64, PAGE_SIZE * 2 - 32); // straddles page 1/2

    FS.close(fdA);
    FS.close(fdB);

    syncAndUnmount(FS, tomefs);

    // Remount and verify both files
    const { FS: FS2 } = await mountTome(backend);

    // Verify fileA
    const bufA = new Uint8Array(PAGE_SIZE * 3);
    const rA = FS2.open(`${MOUNT}/fileA`, O.RDONLY);
    FS2.read(rA, bufA, 0, bufA.length, 0);
    FS2.close(rA);

    // Page 0: 0x10 except last 32 bytes which are 0xFF
    for (let i = 0; i < PAGE_SIZE - 32; i++) {
      expect(bufA[i]).toBe(0x10);
    }
    for (let i = PAGE_SIZE - 32; i < PAGE_SIZE; i++) {
      expect(bufA[i]).toBe(0xff);
    }
    // Page 1: first 32 bytes 0xFF, rest 0x11
    for (let i = PAGE_SIZE; i < PAGE_SIZE + 32; i++) {
      expect(bufA[i]).toBe(0xff);
    }
    for (let i = PAGE_SIZE + 32; i < PAGE_SIZE * 2; i++) {
      expect(bufA[i]).toBe(0x11);
    }
    // Page 2: all 0x12
    for (let i = PAGE_SIZE * 2; i < PAGE_SIZE * 3; i++) {
      expect(bufA[i]).toBe(0x12);
    }

    // Verify fileB
    const bufB = new Uint8Array(PAGE_SIZE * 3);
    const rB = FS2.open(`${MOUNT}/fileB`, O.RDONLY);
    FS2.read(rB, bufB, 0, bufB.length, 0);
    FS2.close(rB);

    // Page 0: all 0x80
    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(bufB[i]).toBe(0x80);
    }
    // Page 1: all 0x81 except last 32 bytes which are 0xEE
    for (let i = PAGE_SIZE; i < PAGE_SIZE * 2 - 32; i++) {
      expect(bufB[i]).toBe(0x81);
    }
    for (let i = PAGE_SIZE * 2 - 32; i < PAGE_SIZE * 2; i++) {
      expect(bufB[i]).toBe(0xee);
    }
    // Page 2: first 32 bytes 0xEE, rest 0x82
    for (let i = PAGE_SIZE * 2; i < PAGE_SIZE * 2 + 32; i++) {
      expect(bufB[i]).toBe(0xee);
    }
    for (let i = PAGE_SIZE * 2 + 32; i < PAGE_SIZE * 3; i++) {
      expect(bufB[i]).toBe(0x82);
    }
  });

  // ------------------------------------------------------------------
  // Append from multiple fds with syncfs between rounds
  // ------------------------------------------------------------------

  it("multi-fd append with syncfs between rounds preserves all data", async () => {
    // Two fds append to the same file. After each round of appends,
    // syncfs is called (without unmount). This exercises the dirty-flush
    // path with open file descriptors under cache pressure.
    const { FS, tomefs } = await mountTome(backend);

    const fdA = FS.open(`${MOUNT}/applog`, O.RDWR | O.CREAT, 0o666);
    const fdB = FS.open(`${MOUNT}/applog`, O.RDWR);

    let expectedSize = 0;
    const allWrites: Array<{ offset: number; data: Uint8Array }> = [];

    for (let round = 0; round < 5; round++) {
      // fd-A appends a chunk at current end
      const chunkA = fillPattern(PAGE_SIZE / 2, round * 2);
      FS.write(fdA, chunkA, 0, chunkA.length, expectedSize);
      allWrites.push({ offset: expectedSize, data: chunkA });
      expectedSize += chunkA.length;

      // fd-B appends a chunk at new end
      const chunkB = fillPattern(PAGE_SIZE / 2, round * 2 + 1);
      FS.write(fdB, chunkB, 0, chunkB.length, expectedSize);
      allWrites.push({ offset: expectedSize, data: chunkB });
      expectedSize += chunkB.length;

      // Sync with both fds still open
      syncfs(FS, tomefs);
    }

    FS.close(fdA);
    FS.close(fdB);
    FS.unmount(MOUNT);

    // Remount and verify all appended data survived
    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/applog`);
    expect(stat.size).toBe(expectedSize);

    const buf = new Uint8Array(expectedSize);
    const rd = FS2.open(`${MOUNT}/applog`, O.RDONLY);
    FS2.read(rd, buf, 0, buf.length, 0);
    FS2.close(rd);

    // Verify each write landed at the correct offset
    for (const w of allWrites) {
      const region = buf.subarray(w.offset, w.offset + w.data.length);
      for (let i = 0; i < w.data.length; i++) {
        expect(region[i]).toBe(w.data[i]);
      }
    }
  });

  // ------------------------------------------------------------------
  // Write + truncate from different fds under pressure
  // ------------------------------------------------------------------

  it("fd-A writes while fd-B truncates under cache pressure + persist", async () => {
    // fd-A fills 4 pages. fd-B truncates to 1 page. fd-A then writes
    // page 1 again. This exercises the interaction between truncation
    // invalidating cached pages and subsequent writes to the same region.
    const { FS, tomefs } = await mountTome(backend);

    const fdA = FS.open(`${MOUNT}/trunc`, O.RDWR | O.CREAT, 0o666);
    const fdB = FS.open(`${MOUNT}/trunc`, O.RDWR);

    // fd-A: fill 4 pages with distinct patterns
    for (let p = 0; p < 4; p++) {
      const data = new Uint8Array(PAGE_SIZE);
      data.fill(0x10 + p);
      FS.write(fdA, data, 0, PAGE_SIZE, p * PAGE_SIZE);
    }

    // fd-B: truncate to 1 page
    FS.ftruncate(fdB.fd, PAGE_SIZE);

    // fd-A: write new data at page 1 (extending file back to 2 pages)
    const newPage1 = new Uint8Array(PAGE_SIZE);
    newPage1.fill(0xdd);
    FS.write(fdA, newPage1, 0, PAGE_SIZE, PAGE_SIZE);

    FS.close(fdA);
    FS.close(fdB);

    syncAndUnmount(FS, tomefs);

    // Remount and verify
    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/trunc`);
    expect(stat.size).toBe(PAGE_SIZE * 2);

    const buf = new Uint8Array(PAGE_SIZE * 2);
    const rd = FS2.open(`${MOUNT}/trunc`, O.RDONLY);
    FS2.read(rd, buf, 0, buf.length, 0);
    FS2.close(rd);

    // Page 0: original data (0x10) — survived truncation
    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(buf[i]).toBe(0x10);
    }
    // Page 1: new data (0xDD) — written after truncation
    for (let i = PAGE_SIZE; i < PAGE_SIZE * 2; i++) {
      expect(buf[i]).toBe(0xdd);
    }
  });

  // ------------------------------------------------------------------
  // Rapid open/write/close cycle from multiple "connections" under pressure
  // ------------------------------------------------------------------

  it("rapid open-write-close cycle simulating connection pool under pressure", async () => {
    // Simulates multiple short-lived connections (like a connection pool)
    // each opening, writing a small amount, and closing. Under cache pressure,
    // the page cache must correctly persist all dirty pages during syncfs.
    const { FS, tomefs } = await mountTome(backend);

    const connectionCount = 20;
    const writeSize = 512;

    for (let c = 0; c < connectionCount; c++) {
      const fd = FS.open(`${MOUNT}/pool`, O.RDWR | O.CREAT, 0o666);
      const data = new Uint8Array(writeSize);
      data.fill((c + 1) & 0xff);
      // Each connection writes to its own region
      FS.write(fd, data, 0, writeSize, c * writeSize);
      FS.close(fd);
    }

    syncAndUnmount(FS, tomefs);

    // Remount and verify each connection's write survived
    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/pool`);
    expect(stat.size).toBe(connectionCount * writeSize);

    const buf = new Uint8Array(stat.size);
    const rd = FS2.open(`${MOUNT}/pool`, O.RDONLY);
    FS2.read(rd, buf, 0, buf.length, 0);
    FS2.close(rd);

    for (let c = 0; c < connectionCount; c++) {
      const expected = (c + 1) & 0xff;
      for (let i = 0; i < writeSize; i++) {
        expect(buf[c * writeSize + i]).toBe(expected);
      }
    }
  });

  // ------------------------------------------------------------------
  // Scattered positional writes from multiple fds across sparse regions
  // ------------------------------------------------------------------

  it("scattered positional writes from two fds create sparse file that persists", async () => {
    // fd-A and fd-B write to non-contiguous regions, creating a sparse file.
    // Under cache pressure, the zero-filled gap pages may never enter the
    // cache at all. On remount, the gaps must still read as zeros.
    const { FS, tomefs } = await mountTome(backend);

    const fdA = FS.open(`${MOUNT}/sparse`, O.RDWR | O.CREAT, 0o666);
    const fdB = FS.open(`${MOUNT}/sparse`, O.RDWR);

    // fd-A: write at page 0
    const a0 = new Uint8Array(100);
    a0.fill(0xaa);
    FS.write(fdA, a0, 0, 100, 0);

    // fd-B: write at page 5 (creating a 4-page gap)
    const b5 = new Uint8Array(100);
    b5.fill(0xbb);
    FS.write(fdB, b5, 0, 100, PAGE_SIZE * 5);

    // fd-A: write at page 10 (creating another gap)
    const a10 = new Uint8Array(100);
    a10.fill(0xcc);
    FS.write(fdA, a10, 0, 100, PAGE_SIZE * 10);

    FS.close(fdA);
    FS.close(fdB);

    syncAndUnmount(FS, tomefs);

    // Remount and verify data + gaps
    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/sparse`);
    expect(stat.size).toBe(PAGE_SIZE * 10 + 100);

    // Verify page 0 region
    const buf0 = new Uint8Array(PAGE_SIZE);
    const rd = FS2.open(`${MOUNT}/sparse`, O.RDONLY);
    FS2.read(rd, buf0, 0, PAGE_SIZE, 0);
    for (let i = 0; i < 100; i++) expect(buf0[i]).toBe(0xaa);
    for (let i = 100; i < PAGE_SIZE; i++) expect(buf0[i]).toBe(0);

    // Verify gap (page 2 — definitely in the gap)
    const gapBuf = new Uint8Array(PAGE_SIZE);
    FS2.read(rd, gapBuf, 0, PAGE_SIZE, PAGE_SIZE * 2);
    for (let i = 0; i < PAGE_SIZE; i++) expect(gapBuf[i]).toBe(0);

    // Verify page 5 region
    const buf5 = new Uint8Array(PAGE_SIZE);
    FS2.read(rd, buf5, 0, PAGE_SIZE, PAGE_SIZE * 5);
    for (let i = 0; i < 100; i++) expect(buf5[i]).toBe(0xbb);
    for (let i = 100; i < PAGE_SIZE; i++) expect(buf5[i]).toBe(0);

    // Verify page 10 region
    const buf10 = new Uint8Array(100);
    FS2.read(rd, buf10, 0, 100, PAGE_SIZE * 10);
    for (let i = 0; i < 100; i++) expect(buf10[i]).toBe(0xcc);

    FS2.close(rd);
  });
});
