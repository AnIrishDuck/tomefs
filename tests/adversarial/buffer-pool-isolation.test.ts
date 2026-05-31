/**
 * Adversarial tests for page cache buffer pool data isolation.
 *
 * The SyncPageCache maintains a buffer pool (max 64 buffers) to reduce
 * allocation pressure. When a page is evicted, its buffer is returned to
 * the pool. When a new page is loaded and the backend returns null (page
 * doesn't exist), a buffer is acquired from the pool and must be zeroed.
 *
 * These tests verify that stale data from evicted pages NEVER leaks into
 * new pages via the buffer pool. This is a security-relevant property:
 * without the fill(0) in acquireBuffer(), one file's data could be visible
 * when reading unwritten regions of a different file.
 *
 * Target: SyncPageCache.acquireBuffer() zero-fill guarantee.
 *
 * Ethos §9: "Write tests designed to break tomefs specifically — things
 * that pass against MEMFS but expose real bugs in the page cache layer.
 * Target the seams."
 */

import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { describe, it, expect } from "vitest";
import { createTomeFS } from "../../src/tomefs.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
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

function syncfs(FS: any, tomefs: any): void {
  tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
    if (err) throw err;
  });
}

describe("adversarial: buffer pool data isolation", () => {
  it("evicted page data does not leak into new file via buffer pool @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend, 4);

    // Fill file A with recognizable data (all 0xFF)
    const poison = new Uint8Array(PAGE_SIZE);
    poison.fill(0xFF);
    const sA = FS.open(`${MOUNT}/fileA`, O.RDWR | O.CREAT, 0o666);
    for (let p = 0; p < 4; p++) {
      FS.write(sA, poison, 0, PAGE_SIZE, p * PAGE_SIZE);
    }
    FS.close(sA);
    syncfs(FS, tomefs);

    // Fill file B to force eviction of A's pages (cache is 4 pages)
    const fillerB = new Uint8Array(PAGE_SIZE);
    fillerB.fill(0xAA);
    const sB = FS.open(`${MOUNT}/fileB`, O.RDWR | O.CREAT, 0o666);
    for (let p = 0; p < 4; p++) {
      FS.write(sB, fillerB, 0, PAGE_SIZE, p * PAGE_SIZE);
    }
    FS.close(sB);

    // A's pages are now evicted and their buffers are in the pool.
    // Create file C and extend it WITHOUT writing data — this triggers
    // acquireBuffer() from the pool to create zero-filled pages.
    const sC = FS.open(`${MOUNT}/fileC`, O.RDWR | O.CREAT, 0o666);
    // Extend file via allocate (or truncate to larger size)
    FS.ftruncate(sC.fd, PAGE_SIZE * 2);

    // Read file C — all bytes must be zero, NOT 0xFF from file A's evicted pages
    const readBuf = new Uint8Array(PAGE_SIZE * 2);
    FS.read(sC, readBuf, 0, PAGE_SIZE * 2, 0);
    FS.close(sC);

    for (let i = 0; i < readBuf.length; i++) {
      if (readBuf[i] !== 0) {
        throw new Error(
          `Data leak at byte ${i}: expected 0x00, got 0x${readBuf[i].toString(16).padStart(2, "0")} ` +
          `(likely stale data from evicted page buffer pool)`,
        );
      }
    }
  });

  it("buffer reuse after eviction cycle does not corrupt new page writes @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend, 2);

    // Write two pages to file A with distinct patterns
    const sA = FS.open(`${MOUNT}/a`, O.RDWR | O.CREAT, 0o666);
    const patA0 = new Uint8Array(PAGE_SIZE);
    patA0.fill(0xDE);
    const patA1 = new Uint8Array(PAGE_SIZE);
    patA1.fill(0xAD);
    FS.write(sA, patA0, 0, PAGE_SIZE, 0);
    FS.write(sA, patA1, 0, PAGE_SIZE, PAGE_SIZE);
    FS.close(sA);
    syncfs(FS, tomefs);

    // Write two pages to file B — evicts A's pages to pool
    const sB = FS.open(`${MOUNT}/b`, O.RDWR | O.CREAT, 0o666);
    const patB0 = new Uint8Array(PAGE_SIZE);
    patB0.fill(0x11);
    const patB1 = new Uint8Array(PAGE_SIZE);
    patB1.fill(0x22);
    FS.write(sB, patB0, 0, PAGE_SIZE, 0);
    FS.write(sB, patB1, 0, PAGE_SIZE, PAGE_SIZE);
    FS.close(sB);

    // Now read file A again — pages must be loaded fresh from backend,
    // not corrupted by B's writes into recycled buffers
    const rA = FS.open(`${MOUNT}/a`, O.RDONLY);
    const readBuf0 = new Uint8Array(PAGE_SIZE);
    FS.read(rA, readBuf0, 0, PAGE_SIZE, 0);
    expect(readBuf0).toEqual(patA0);

    const readBuf1 = new Uint8Array(PAGE_SIZE);
    FS.read(rA, readBuf1, 0, PAGE_SIZE, PAGE_SIZE);
    expect(readBuf1).toEqual(patA1);
    FS.close(rA);
  });

  it("getPageNoRead returns zeroed buffer from pool after eviction @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend, 2);

    // Fill the cache with poison data
    const poison = new Uint8Array(PAGE_SIZE);
    poison.fill(0xBE);
    const sA = FS.open(`${MOUNT}/poison`, O.RDWR | O.CREAT, 0o666);
    FS.write(sA, poison, 0, PAGE_SIZE, 0);
    FS.write(sA, poison, 0, PAGE_SIZE, PAGE_SIZE);
    FS.close(sA);
    syncfs(FS, tomefs);

    // Evict poison pages by reading/writing a different file
    const evict = new Uint8Array(PAGE_SIZE);
    evict.fill(0x01);
    const sE = FS.open(`${MOUNT}/evict`, O.RDWR | O.CREAT, 0o666);
    FS.write(sE, evict, 0, PAGE_SIZE, 0);
    FS.write(sE, evict, 0, PAGE_SIZE, PAGE_SIZE);
    FS.close(sE);

    // Create a new file and write a PARTIAL page (sub-page write to new page
    // triggers getPageNoRead for the page, then writes only part of it).
    // The unwritten portion must be zero, not 0xBE from the pool buffer.
    const sNew = FS.open(`${MOUNT}/newfile`, O.RDWR | O.CREAT, 0o666);
    const smallWrite = new Uint8Array(100);
    smallWrite.fill(0x42);
    FS.write(sNew, smallWrite, 0, 100, 0);

    // Read the full page — bytes 0-99 should be 0x42, bytes 100+ should be 0x00
    const fullPage = new Uint8Array(PAGE_SIZE);
    FS.read(sNew, fullPage, 0, PAGE_SIZE, 0);
    FS.close(sNew);

    // Verify written portion
    for (let i = 0; i < 100; i++) {
      expect(fullPage[i]).toBe(0x42);
    }
    // Verify unwritten portion is zero (not stale pool data)
    for (let i = 100; i < PAGE_SIZE; i++) {
      if (fullPage[i] !== 0) {
        throw new Error(
          `Buffer pool leak at byte ${i}: expected 0x00, got 0x${fullPage[i].toString(16).padStart(2, "0")}`,
        );
      }
    }
  });

  it("multiple eviction-reuse cycles maintain isolation @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend, 2);

    // Run 10 cycles of: write distinctive data → evict → verify new file is clean
    for (let cycle = 0; cycle < 10; cycle++) {
      const marker = (cycle * 0x17 + 0x30) & 0xFF;
      const path = `${MOUNT}/cycle_${cycle}`;

      // Write poison data
      const poison = new Uint8Array(PAGE_SIZE);
      poison.fill(marker);
      const sW = FS.open(path, O.RDWR | O.CREAT, 0o666);
      FS.write(sW, poison, 0, PAGE_SIZE, 0);
      FS.write(sW, poison, 0, PAGE_SIZE, PAGE_SIZE);
      FS.close(sW);
      syncfs(FS, tomefs);

      // Evict by writing another file
      const evictBuf = new Uint8Array(PAGE_SIZE);
      evictBuf.fill(0x00);
      const sE = FS.open(`${MOUNT}/evict_${cycle}`, O.RDWR | O.CREAT, 0o666);
      FS.write(sE, evictBuf, 0, PAGE_SIZE, 0);
      FS.write(sE, evictBuf, 0, PAGE_SIZE, PAGE_SIZE);
      FS.close(sE);

      // Create a new file via truncate (extend) — must be zeroed
      const newPath = `${MOUNT}/new_${cycle}`;
      const sN = FS.open(newPath, O.RDWR | O.CREAT, 0o666);
      FS.ftruncate(sN.fd, PAGE_SIZE);
      const readBuf = new Uint8Array(PAGE_SIZE);
      FS.read(sN, readBuf, 0, PAGE_SIZE, 0);
      FS.close(sN);

      for (let i = 0; i < PAGE_SIZE; i++) {
        if (readBuf[i] !== 0) {
          throw new Error(
            `Cycle ${cycle}: buffer pool leak at byte ${i}: ` +
            `expected 0x00, got 0x${readBuf[i].toString(16).padStart(2, "0")} ` +
            `(poison marker was 0x${marker.toString(16).padStart(2, "0")})`,
          );
        }
      }
    }
  });

  it("sparse file extension via write beyond EOF uses clean buffers @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend, 2);

    // Fill cache with poison
    const poison = new Uint8Array(PAGE_SIZE);
    poison.fill(0xCC);
    const sP = FS.open(`${MOUNT}/poison`, O.RDWR | O.CREAT, 0o666);
    FS.write(sP, poison, 0, PAGE_SIZE, 0);
    FS.write(sP, poison, 0, PAGE_SIZE, PAGE_SIZE);
    FS.close(sP);
    syncfs(FS, tomefs);

    // Evict poison
    const sE = FS.open(`${MOUNT}/evict`, O.RDWR | O.CREAT, 0o666);
    const zero = new Uint8Array(PAGE_SIZE);
    FS.write(sE, zero, 0, PAGE_SIZE, 0);
    FS.write(sE, zero, 0, PAGE_SIZE, PAGE_SIZE);
    FS.close(sE);

    // Write to page 2 of a new file (skipping pages 0-1 — sparse extension).
    // Pages 0-1 are never explicitly written but must read as zeros.
    const sNew = FS.open(`${MOUNT}/sparse`, O.RDWR | O.CREAT, 0o666);
    const data = new Uint8Array(100);
    data.fill(0x99);
    FS.write(sNew, data, 0, 100, 2 * PAGE_SIZE);

    // Read pages 0-1 (the sparse hole) — must be all zeros
    const hole = new Uint8Array(2 * PAGE_SIZE);
    FS.read(sNew, hole, 0, 2 * PAGE_SIZE, 0);
    FS.close(sNew);

    for (let i = 0; i < hole.length; i++) {
      if (hole[i] !== 0) {
        throw new Error(
          `Sparse hole leak at byte ${i}: expected 0x00, got 0x${hole[i].toString(16).padStart(2, "0")}`,
        );
      }
    }
  });

  it("buffer pool isolation survives persistence round-trip @fast", async () => {
    const backend = new SyncMemoryBackend();

    // Session 1: fill backend with poison data, sync
    {
      const { FS, tomefs } = await mountTome(backend, 4);
      const poison = new Uint8Array(PAGE_SIZE);
      poison.fill(0xEE);
      const s = FS.open(`${MOUNT}/persistent`, O.RDWR | O.CREAT, 0o666);
      for (let p = 0; p < 4; p++) {
        FS.write(s, poison, 0, PAGE_SIZE, p * PAGE_SIZE);
      }
      FS.close(s);
      syncfs(FS, tomefs);
    }

    // Session 2: remount, evict persistent file's pages, create new file
    {
      const { FS } = await mountTome(backend, 4);

      // Read persistent file to load its pages into cache
      const sP = FS.open(`${MOUNT}/persistent`, O.RDONLY);
      const dummy = new Uint8Array(PAGE_SIZE * 4);
      FS.read(sP, dummy, 0, PAGE_SIZE * 4, 0);
      FS.close(sP);

      // Evict by filling cache with a new file
      const evictBuf = new Uint8Array(PAGE_SIZE);
      evictBuf.fill(0x01);
      const sE = FS.open(`${MOUNT}/evictor`, O.RDWR | O.CREAT, 0o666);
      for (let p = 0; p < 4; p++) {
        FS.write(sE, evictBuf, 0, PAGE_SIZE, p * PAGE_SIZE);
      }
      FS.close(sE);

      // Extend a new file — pool buffers from persistent file must be zeroed
      const sNew = FS.open(`${MOUNT}/clean`, O.RDWR | O.CREAT, 0o666);
      FS.ftruncate(sNew.fd, PAGE_SIZE * 2);
      const readBuf = new Uint8Array(PAGE_SIZE * 2);
      FS.read(sNew, readBuf, 0, PAGE_SIZE * 2, 0);
      FS.close(sNew);

      for (let i = 0; i < readBuf.length; i++) {
        if (readBuf[i] !== 0) {
          throw new Error(
            `Post-remount buffer pool leak at byte ${i}: ` +
            `expected 0x00, got 0x${readBuf[i].toString(16).padStart(2, "0")}`,
          );
        }
      }
    }
  });
});
