/**
 * Adversarial tests: Non-page-aligned truncation under cache pressure
 * with persistence round-trips.
 *
 * Targets the zeroTailAfterTruncate code path in SyncPageCache where
 * the last surviving page has been evicted from cache and must be
 * loaded from the backend, zeroed, and written back. Combined with
 * syncfs persistence to verify the zeroed tail survives remount.
 *
 * Existing test coverage:
 * - truncate-dirty.test.ts: non-aligned truncation but no cache pressure
 * - write-truncate-pressure.test.ts: cache pressure but page-aligned only
 * - Neither tests persistence (syncfs → remount) after non-aligned truncation
 *
 * This file fills the gap: non-aligned truncation + eviction + persistence.
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

function encode(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

function decode(buf: Uint8Array, length?: number): string {
  return new TextDecoder().decode(
    length !== undefined ? buf.subarray(0, length) : buf,
  );
}

/**
 * Mount tomefs with a specific cache size. Small caches force eviction
 * so the zeroTailAfterTruncate backend path gets exercised.
 */
async function mountTome(backend: SyncMemoryBackend, maxPages = 4) {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  const Module = await createModule();
  const FS = Module.FS;
  const tomefs = createTomeFS(FS, { backend, maxPages });
  FS.mkdir("/tome");
  FS.mount(tomefs, {}, "/tome");
  return { FS, tomefs, Module };
}

function syncfs(FS: any, tomefs: any) {
  tomefs.syncfs(FS.lookupPath("/tome").node.mount, false, (err: any) => {
    if (err) throw err;
  });
}

function syncAndUnmount(FS: any, tomefs: any) {
  syncfs(FS, tomefs);
  FS.unmount("/tome");
}

const MOUNT = "/tome";

describe("adversarial: non-aligned truncation under cache pressure + persistence", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  // ------------------------------------------------------------------
  // Core: non-aligned truncation with evicted last page
  // ------------------------------------------------------------------

  it("truncate to mid-page when last page is evicted zeros the tail @fast", async () => {
    // With maxPages=4, writing 8 pages guarantees pages 0-3 are evicted.
    // Truncating to 2.5 pages means page 2 (the last surviving page) was
    // evicted, exercising the backend read→zero→write path.
    const { FS } = await mountTome(backend, 4);

    const stream = FS.open(`${MOUNT}/file`, O.RDWR | O.CREAT, 0o666);

    // Fill 8 pages with 0xFF (exceeds 4-page cache → pages 0-3 evicted)
    const data = new Uint8Array(PAGE_SIZE * 8);
    data.fill(0xff);
    FS.write(stream, data, 0, data.length, 0);

    // Truncate to 2.5 pages — page 2 is the last surviving page
    const truncSize = PAGE_SIZE * 2 + PAGE_SIZE / 2;
    FS.ftruncate(stream.fd, truncSize);
    expect(FS.stat(`${MOUNT}/file`).size).toBe(truncSize);

    // Extend back to 3 pages by writing a single byte at the end of page 2.
    // The region between PAGE_SIZE*2 + PAGE_SIZE/2 and PAGE_SIZE*3-1 must
    // be zeros (from the tail zeroing), not 0xFF (stale data).
    const marker = new Uint8Array([0x42]);
    FS.write(stream, marker, 0, 1, PAGE_SIZE * 3 - 1);

    // Read the previously-truncated tail region
    const tailStart = PAGE_SIZE * 2 + PAGE_SIZE / 2;
    const tailLen = PAGE_SIZE / 2 - 1; // up to the marker
    const tailBuf = new Uint8Array(tailLen);
    const n = FS.read(stream, tailBuf, 0, tailLen, tailStart);
    expect(n).toBe(tailLen);
    for (let i = 0; i < tailLen; i++) {
      if (tailBuf[i] !== 0) {
        throw new Error(
          `Byte ${tailStart + i}: expected 0x00 after truncate, got 0x${tailBuf[i].toString(16)} (stale data leak from evicted page)`,
        );
      }
    }

    // Preserved prefix (first 2.5 pages) should still be 0xFF
    const prefixBuf = new Uint8Array(truncSize);
    FS.read(stream, prefixBuf, 0, truncSize, 0);
    for (let i = 0; i < truncSize; i++) {
      if (prefixBuf[i] !== 0xff) {
        throw new Error(
          `Prefix byte ${i}: expected 0xFF, got 0x${prefixBuf[i].toString(16)}`,
        );
      }
    }

    FS.close(stream);
  });

  // ------------------------------------------------------------------
  // Non-aligned truncation + persistence round-trip
  // ------------------------------------------------------------------

  it("non-aligned truncation survives syncfs → remount @fast", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    const stream = FS.open(`${MOUNT}/persist`, O.RDWR | O.CREAT, 0o666);

    // Write 6 pages with position-dependent data
    for (let p = 0; p < 6; p++) {
      const page = new Uint8Array(PAGE_SIZE);
      page.fill((p * 37 + 11) & 0xff);
      FS.write(stream, page, 0, PAGE_SIZE, p * PAGE_SIZE);
    }

    // Truncate to 1.75 pages (non-aligned, page 1 tail gets zeroed)
    const truncSize = PAGE_SIZE + (PAGE_SIZE * 3) / 4;
    FS.ftruncate(stream.fd, truncSize);
    FS.close(stream);

    // Persist and remount
    syncAndUnmount(FS, tomefs);
    const { FS: FS2 } = await mountTome(backend, 4);

    // Verify restored size
    const stat = FS2.stat(`${MOUNT}/persist`);
    expect(stat.size).toBe(truncSize);

    // Verify page 0 data intact
    const reader = FS2.open(`${MOUNT}/persist`, O.RDONLY);
    const page0 = new Uint8Array(PAGE_SIZE);
    FS2.read(reader, page0, 0, PAGE_SIZE, 0);
    const expected0 = (0 * 37 + 11) & 0xff;
    for (let i = 0; i < PAGE_SIZE; i++) {
      if (page0[i] !== expected0) {
        throw new Error(
          `Page 0 byte ${i}: expected 0x${expected0.toString(16)}, got 0x${page0[i].toString(16)}`,
        );
      }
    }

    // Verify page 1 prefix intact (first 3/4 of page)
    const prefixLen = (PAGE_SIZE * 3) / 4;
    const page1prefix = new Uint8Array(prefixLen);
    FS2.read(reader, page1prefix, 0, prefixLen, PAGE_SIZE);
    const expected1 = (1 * 37 + 11) & 0xff;
    for (let i = 0; i < prefixLen; i++) {
      if (page1prefix[i] !== expected1) {
        throw new Error(
          `Page 1 byte ${i}: expected 0x${expected1.toString(16)}, got 0x${page1prefix[i].toString(16)}`,
        );
      }
    }

    // Reading beyond the truncated size should return 0 bytes
    const beyondBuf = new Uint8Array(PAGE_SIZE);
    const n = FS2.read(reader, beyondBuf, 0, PAGE_SIZE, truncSize);
    expect(n).toBe(0);

    FS2.close(reader);
  });

  it("non-aligned truncation then extend: zeroed tail persists across remount", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    const stream = FS.open(`${MOUNT}/extend`, O.RDWR | O.CREAT, 0o666);

    // Write 8 pages of 0xAA (all evicted from 4-page cache)
    const data = new Uint8Array(PAGE_SIZE * 8);
    data.fill(0xaa);
    FS.write(stream, data, 0, data.length, 0);

    // Truncate to 3 pages + 100 bytes
    const truncSize = PAGE_SIZE * 3 + 100;
    FS.ftruncate(stream.fd, truncSize);

    // Extend by writing at page 4 (creates a zero gap in page 3's tail)
    const extendData = new Uint8Array(PAGE_SIZE);
    extendData.fill(0xbb);
    FS.write(stream, extendData, 0, PAGE_SIZE, PAGE_SIZE * 4);
    FS.close(stream);

    // Persist and remount
    syncAndUnmount(FS, tomefs);
    const { FS: FS2 } = await mountTome(backend, 4);

    const reader = FS2.open(`${MOUNT}/extend`, O.RDONLY);

    // Page 3: first 100 bytes should be 0xAA, rest should be 0x00
    const page3 = new Uint8Array(PAGE_SIZE);
    FS2.read(reader, page3, 0, PAGE_SIZE, PAGE_SIZE * 3);
    for (let i = 0; i < 100; i++) {
      if (page3[i] !== 0xaa) {
        throw new Error(
          `Page 3 byte ${i}: expected 0xAA (preserved), got 0x${page3[i].toString(16)}`,
        );
      }
    }
    for (let i = 100; i < PAGE_SIZE; i++) {
      if (page3[i] !== 0) {
        throw new Error(
          `Page 3 byte ${i}: expected 0x00 (zeroed tail), got 0x${page3[i].toString(16)}`,
        );
      }
    }

    // Page 4: should be 0xBB (the extend write)
    const page4 = new Uint8Array(PAGE_SIZE);
    FS2.read(reader, page4, 0, PAGE_SIZE, PAGE_SIZE * 4);
    for (let i = 0; i < PAGE_SIZE; i++) {
      if (page4[i] !== 0xbb) {
        throw new Error(
          `Page 4 byte ${i}: expected 0xBB, got 0x${page4[i].toString(16)}`,
        );
      }
    }

    FS2.close(reader);
  });

  // ------------------------------------------------------------------
  // Multiple non-aligned truncation cycles with persistence
  // ------------------------------------------------------------------

  it("repeated non-aligned truncate+extend+persist cycles", async () => {
    // Each cycle: write data, truncate non-aligned, extend, persist, remount.
    // Verify no stale data from any previous cycle leaks through.
    for (let cycle = 0; cycle < 4; cycle++) {
      const { FS, tomefs } = await mountTome(backend, 4);

      const fill = ((cycle * 41 + 7) & 0xff) || 1; // non-zero fill
      const stream = FS.open(`${MOUNT}/cycles`, O.RDWR | O.CREAT, 0o666);

      // Write 5 pages with cycle-specific data
      const writeData = new Uint8Array(PAGE_SIZE * 5);
      writeData.fill(fill);
      FS.write(stream, writeData, 0, writeData.length, 0);

      // Truncate to non-aligned: 1 page + (cycle+1)*100 bytes
      const tailBytes = (cycle + 1) * 100;
      const truncSize = PAGE_SIZE + tailBytes;
      FS.ftruncate(stream.fd, truncSize);

      // Extend by writing at page 2
      const extendBuf = new Uint8Array(PAGE_SIZE);
      extendBuf.fill((fill + 0x11) & 0xff);
      FS.write(stream, extendBuf, 0, PAGE_SIZE, PAGE_SIZE * 2);
      FS.close(stream);

      // Persist and remount
      syncAndUnmount(FS, tomefs);
      const { FS: FS2, tomefs: t2 } = await mountTome(backend, 4);

      const reader = FS2.open(`${MOUNT}/cycles`, O.RDONLY);

      // Page 0: full page of cycle fill
      const p0 = new Uint8Array(PAGE_SIZE);
      FS2.read(reader, p0, 0, PAGE_SIZE, 0);
      for (let i = 0; i < PAGE_SIZE; i++) {
        if (p0[i] !== fill) {
          throw new Error(
            `Cycle ${cycle} page 0 byte ${i}: expected 0x${fill.toString(16)}, got 0x${p0[i].toString(16)}`,
          );
        }
      }

      // Page 1: first tailBytes are cycle fill, rest is zero gap
      const p1 = new Uint8Array(PAGE_SIZE);
      FS2.read(reader, p1, 0, PAGE_SIZE, PAGE_SIZE);
      for (let i = 0; i < tailBytes; i++) {
        if (p1[i] !== fill) {
          throw new Error(
            `Cycle ${cycle} page 1 byte ${i}: expected 0x${fill.toString(16)} (preserved), got 0x${p1[i].toString(16)}`,
          );
        }
      }
      for (let i = tailBytes; i < PAGE_SIZE; i++) {
        if (p1[i] !== 0) {
          throw new Error(
            `Cycle ${cycle} page 1 byte ${i}: expected 0x00 (zeroed), got 0x${p1[i].toString(16)}`,
          );
        }
      }

      // Page 2: extend data
      const p2 = new Uint8Array(PAGE_SIZE);
      FS2.read(reader, p2, 0, PAGE_SIZE, PAGE_SIZE * 2);
      const extFill = (fill + 0x11) & 0xff;
      for (let i = 0; i < PAGE_SIZE; i++) {
        if (p2[i] !== extFill) {
          throw new Error(
            `Cycle ${cycle} page 2 byte ${i}: expected 0x${extFill.toString(16)}, got 0x${p2[i].toString(16)}`,
          );
        }
      }

      FS2.close(reader);

      // Clean up for next cycle: truncate to zero
      const cleanup = FS2.open(`${MOUNT}/cycles`, O.RDWR | O.TRUNC);
      FS2.close(cleanup);
      syncAndUnmount(FS2, t2);
    }
  });

  // ------------------------------------------------------------------
  // Truncate to 1 byte: extreme non-alignment
  // ------------------------------------------------------------------

  it("truncate to 1 byte under pressure preserves and zeros correctly", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    const stream = FS.open(`${MOUNT}/onebyte`, O.RDWR | O.CREAT, 0o666);

    // Write 6 pages of 0xDD
    const data = new Uint8Array(PAGE_SIZE * 6);
    data.fill(0xdd);
    FS.write(stream, data, 0, data.length, 0);

    // Truncate to 1 byte
    FS.ftruncate(stream.fd, 1);
    expect(FS.stat(`${MOUNT}/onebyte`).size).toBe(1);

    // The surviving byte should be 0xDD
    const oneBuf = new Uint8Array(1);
    FS.read(stream, oneBuf, 0, 1, 0);
    expect(oneBuf[0]).toBe(0xdd);

    // Extend to full page — bytes 1..PAGE_SIZE-1 must be zero
    const extBuf = new Uint8Array([0xee]);
    FS.write(stream, extBuf, 0, 1, PAGE_SIZE - 1);

    const fullPage = new Uint8Array(PAGE_SIZE);
    FS.read(stream, fullPage, 0, PAGE_SIZE, 0);
    expect(fullPage[0]).toBe(0xdd);
    for (let i = 1; i < PAGE_SIZE - 1; i++) {
      if (fullPage[i] !== 0) {
        throw new Error(
          `Byte ${i}: expected 0x00 after truncate-to-1+extend, got 0x${fullPage[i].toString(16)}`,
        );
      }
    }
    expect(fullPage[PAGE_SIZE - 1]).toBe(0xee);

    FS.close(stream);

    // Persist and verify
    syncAndUnmount(FS, tomefs);
    const { FS: FS2 } = await mountTome(backend, 4);

    const reader = FS2.open(`${MOUNT}/onebyte`, O.RDONLY);
    const restored = new Uint8Array(PAGE_SIZE);
    FS2.read(reader, restored, 0, PAGE_SIZE, 0);
    expect(restored[0]).toBe(0xdd);
    for (let i = 1; i < PAGE_SIZE - 1; i++) {
      if (restored[i] !== 0) {
        throw new Error(
          `Restored byte ${i}: expected 0x00, got 0x${restored[i].toString(16)}`,
        );
      }
    }
    expect(restored[PAGE_SIZE - 1]).toBe(0xee);
    FS2.close(reader);
  });

  // ------------------------------------------------------------------
  // Truncate mid-page on two competing files under shared cache
  // ------------------------------------------------------------------

  it("non-aligned truncation on two files sharing a 4-page cache", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    // Create two files, each 4 pages — total 8 pages in a 4-page cache
    const fileA = FS.open(`${MOUNT}/a`, O.RDWR | O.CREAT, 0o666);
    const fileB = FS.open(`${MOUNT}/b`, O.RDWR | O.CREAT, 0o666);

    const dataA = new Uint8Array(PAGE_SIZE * 4);
    dataA.fill(0xaa);
    FS.write(fileA, dataA, 0, dataA.length, 0);

    const dataB = new Uint8Array(PAGE_SIZE * 4);
    dataB.fill(0xbb);
    FS.write(fileB, dataB, 0, dataB.length, 0);

    // Truncate both to non-aligned sizes
    FS.ftruncate(fileA.fd, PAGE_SIZE + 200);
    FS.ftruncate(fileB.fd, PAGE_SIZE * 2 + 500);

    FS.close(fileA);
    FS.close(fileB);

    // Persist and remount
    syncAndUnmount(FS, tomefs);
    const { FS: FS2 } = await mountTome(backend, 4);

    // Verify file A: PAGE_SIZE + 200 bytes
    const statA = FS2.stat(`${MOUNT}/a`);
    expect(statA.size).toBe(PAGE_SIZE + 200);
    const readerA = FS2.open(`${MOUNT}/a`, O.RDONLY);
    const bufA = new Uint8Array(PAGE_SIZE + 200);
    FS2.read(readerA, bufA, 0, bufA.length, 0);
    for (let i = 0; i < PAGE_SIZE; i++) {
      if (bufA[i] !== 0xaa) {
        throw new Error(`File A page 0 byte ${i}: expected 0xAA, got 0x${bufA[i].toString(16)}`);
      }
    }
    for (let i = PAGE_SIZE; i < PAGE_SIZE + 200; i++) {
      if (bufA[i] !== 0xaa) {
        throw new Error(`File A page 1 byte ${i}: expected 0xAA, got 0x${bufA[i].toString(16)}`);
      }
    }
    FS2.close(readerA);

    // Verify file B: PAGE_SIZE * 2 + 500 bytes
    const statB = FS2.stat(`${MOUNT}/b`);
    expect(statB.size).toBe(PAGE_SIZE * 2 + 500);
    const readerB = FS2.open(`${MOUNT}/b`, O.RDONLY);
    const bufB = new Uint8Array(PAGE_SIZE * 2 + 500);
    FS2.read(readerB, bufB, 0, bufB.length, 0);
    for (let i = 0; i < PAGE_SIZE * 2 + 500; i++) {
      if (bufB[i] !== 0xbb) {
        throw new Error(`File B byte ${i}: expected 0xBB, got 0x${bufB[i].toString(16)}`);
      }
    }
    FS2.close(readerB);
  });

  // ------------------------------------------------------------------
  // Postgres-realistic: WAL truncate + checkpoint under pressure
  // ------------------------------------------------------------------

  it("WAL-style: non-aligned truncation + checkpoint + append + persist", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    // Simulate WAL: append records, then truncate to recycle, then append more
    const wal = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);

    // Phase 1: Write 6 pages of WAL records (exceeds 4-page cache)
    for (let i = 0; i < 6; i++) {
      const record = new Uint8Array(PAGE_SIZE);
      record.fill((i + 1) & 0xff);
      FS.write(wal, record, 0, PAGE_SIZE, i * PAGE_SIZE);
    }

    // Phase 2: Checkpoint — truncate WAL to mid-page (simulates partial recycle)
    // Keep first 2 pages + 1000 bytes (committed records)
    const checkpointSize = PAGE_SIZE * 2 + 1000;
    FS.ftruncate(wal.fd, checkpointSize);

    // Phase 3: syncfs (checkpoint persists)
    syncfs(FS, tomefs);

    // Phase 4: Append new WAL records after checkpoint
    const newRecord = new Uint8Array(PAGE_SIZE);
    newRecord.fill(0xf0);
    FS.write(wal, newRecord, 0, PAGE_SIZE, checkpointSize);

    FS.close(wal);

    // Final persist and remount
    syncAndUnmount(FS, tomefs);
    const { FS: FS2 } = await mountTome(backend, 4);

    const reader = FS2.open(`${MOUNT}/wal`, O.RDONLY);
    const totalSize = checkpointSize + PAGE_SIZE;
    expect(FS2.stat(`${MOUNT}/wal`).size).toBe(totalSize);

    // Page 0-1: original data
    for (let p = 0; p < 2; p++) {
      const buf = new Uint8Array(PAGE_SIZE);
      FS2.read(reader, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) {
        if (buf[i] !== ((p + 1) & 0xff)) {
          throw new Error(
            `Page ${p} byte ${i}: expected 0x${((p + 1) & 0xff).toString(16)}, got 0x${buf[i].toString(16)}`,
          );
        }
      }
    }

    // Page 2: first 1000 bytes preserved, rest is zero (from truncation)
    // followed by gap to next write
    const page2 = new Uint8Array(PAGE_SIZE);
    FS2.read(reader, page2, 0, PAGE_SIZE, PAGE_SIZE * 2);
    for (let i = 0; i < 1000; i++) {
      if (page2[i] !== 3) { // page 2 was filled with 3
        throw new Error(
          `Page 2 byte ${i}: expected 0x03 (preserved), got 0x${page2[i].toString(16)}`,
        );
      }
    }
    // Bytes 1000..PAGE_SIZE-1: depends on whether new write filled them
    // The new write started at checkpointSize = PAGE_SIZE*2 + 1000, so
    // bytes 1000..PAGE_SIZE-1 on page 2 are part of the new write
    for (let i = 1000; i < PAGE_SIZE; i++) {
      if (page2[i] !== 0xf0) {
        throw new Error(
          `Page 2 byte ${i}: expected 0xF0 (new write), got 0x${page2[i].toString(16)}`,
        );
      }
    }

    // Remaining bytes of the new record on page 3
    const remaining = checkpointSize + PAGE_SIZE - PAGE_SIZE * 3;
    if (remaining > 0) {
      const page3 = new Uint8Array(remaining);
      FS2.read(reader, page3, 0, remaining, PAGE_SIZE * 3);
      for (let i = 0; i < remaining; i++) {
        if (page3[i] !== 0xf0) {
          throw new Error(
            `Page 3 byte ${i}: expected 0xF0, got 0x${page3[i].toString(16)}`,
          );
        }
      }
    }

    FS2.close(reader);
  });
});
