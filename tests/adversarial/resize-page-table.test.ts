/**
 * Adversarial tests: per-node _pages table survival across resize operations.
 *
 * The per-node page table (node._pages) provides O(1) page lookups by
 * caching CachedPage references directly on the node, bypassing the
 * page cache's Map lookup + key construction. This optimization is
 * critical for Postgres I/O patterns (page-aligned reads/writes to the
 * same file in tight loops).
 *
 * resizeFileStorage must invalidate _pages on truncation (pages are
 * deleted) but should preserve it on grow (existing pages are untouched).
 * These tests verify that _pages references remain valid after file
 * extension, including under cache pressure where eviction could create
 * stale references that must be lazily detected via the evicted flag.
 *
 * Ethos 6 (performance parity), 9 (adversarial -- target the seams).
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

function fillPage(seed: number): Uint8Array {
  const buf = new Uint8Array(PAGE_SIZE);
  for (let i = 0; i < PAGE_SIZE; i++) {
    buf[i] = (seed + i * 31) & 0xff;
  }
  return buf;
}

function verifyPage(buf: Uint8Array, seed: number): boolean {
  for (let i = 0; i < PAGE_SIZE; i++) {
    if (buf[i] !== ((seed + i * 31) & 0xff)) return false;
  }
  return true;
}

async function mountTome(backend: SyncMemoryBackend, maxPages?: number) {
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

describe("adversarial: _pages table survival across resize", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("write + truncate-extend + re-read preserves data (large cache) @fast", async () => {
    // Write data, extend via truncate, verify original data survives.
    // Large cache: no eviction, _pages references remain valid throughout.
    const { FS } = await mountTome(backend, 4096);
    const path = `${MOUNT}/extend`;

    const fd = FS.open(path, O.RDWR | O.CREAT, 0o666);
    const data = fillPage(42);
    FS.write(fd, data, 0, PAGE_SIZE); // page 0

    // Extend file by 3 pages (truncate to larger size)
    FS.ftruncate(fd.fd, PAGE_SIZE * 4);
    expect(FS.fstat(fd.fd).size).toBe(PAGE_SIZE * 4);

    // Re-read page 0 — should still have the written data
    const buf = new Uint8Array(PAGE_SIZE);
    FS.read(fd, buf, 0, PAGE_SIZE, 0);
    expect(verifyPage(buf, 42)).toBe(true);

    // Extended pages should be zeros
    const zeroBuf = new Uint8Array(PAGE_SIZE);
    FS.read(fd, zeroBuf, 0, PAGE_SIZE, PAGE_SIZE);
    expect(zeroBuf.every((b) => b === 0)).toBe(true);

    FS.close(fd);
  });

  it("write multiple pages + extend + re-read all (large cache)", async () => {
    // Write 3 pages, extend to 6, verify all 3 original pages survive.
    const { FS } = await mountTome(backend, 4096);
    const path = `${MOUNT}/multi`;

    const fd = FS.open(path, O.RDWR | O.CREAT, 0o666);
    for (let i = 0; i < 3; i++) {
      FS.write(fd, fillPage(i * 10), 0, PAGE_SIZE, i * PAGE_SIZE);
    }

    // Extend to 6 pages
    FS.ftruncate(fd.fd, PAGE_SIZE * 6);

    // Re-read all original pages
    const buf = new Uint8Array(PAGE_SIZE);
    for (let i = 0; i < 3; i++) {
      FS.read(fd, buf, 0, PAGE_SIZE, i * PAGE_SIZE);
      expect(verifyPage(buf, i * 10)).toBe(true);
    }

    FS.close(fd);
  });

  it("write + allocate + write more + read all (large cache) @fast", async () => {
    // Simulates Postgres WAL: write records, allocate more space, write more.
    const { FS } = await mountTome(backend, 4096);
    const path = `${MOUNT}/wal`;

    const fd = FS.open(path, O.RDWR | O.CREAT, 0o666);
    // Write first 2 pages
    FS.write(fd, fillPage(1), 0, PAGE_SIZE, 0);
    FS.write(fd, fillPage(2), 0, PAGE_SIZE, PAGE_SIZE);

    // Allocate space for 4 more pages
    fd.stream_ops.allocate(fd, 0, PAGE_SIZE * 6);
    expect(FS.fstat(fd.fd).size).toBe(PAGE_SIZE * 6);

    // Write to pages 2 and 3 (newly allocated region)
    FS.write(fd, fillPage(3), 0, PAGE_SIZE, PAGE_SIZE * 2);
    FS.write(fd, fillPage(4), 0, PAGE_SIZE, PAGE_SIZE * 3);

    // Verify all 4 written pages
    const buf = new Uint8Array(PAGE_SIZE);
    for (let i = 0; i < 4; i++) {
      FS.read(fd, buf, 0, PAGE_SIZE, i * PAGE_SIZE);
      expect(verifyPage(buf, i + 1)).toBe(true);
    }

    FS.close(fd);
  });

  it("write + extend + write + extend cycle (4-page cache) @fast", async () => {
    // Tests _pages under cache pressure: extend fills cache with the
    // sentinel page, potentially evicting existing page references.
    // The evicted flag in _pages must be detected and refreshed.
    const { FS } = await mountTome(backend, 4);
    const path = `${MOUNT}/cycle`;

    const fd = FS.open(path, O.RDWR | O.CREAT, 0o666);

    // Write page 0
    FS.write(fd, fillPage(10), 0, PAGE_SIZE, 0);

    // Extend to 4 pages (fills 4-page cache: pages 0 + sentinel page 3)
    FS.ftruncate(fd.fd, PAGE_SIZE * 4);

    // Write page 1 (may evict page 0 due to cache pressure)
    FS.write(fd, fillPage(20), 0, PAGE_SIZE, PAGE_SIZE);

    // Extend to 8 pages (sentinel page 7 enters cache, more eviction)
    FS.ftruncate(fd.fd, PAGE_SIZE * 8);

    // Write page 4 (newly allocated region)
    FS.write(fd, fillPage(30), 0, PAGE_SIZE, PAGE_SIZE * 4);

    // Verify all written pages — some may require backend round-trips
    // due to eviction, but _pages stale detection should handle this.
    const buf = new Uint8Array(PAGE_SIZE);
    FS.read(fd, buf, 0, PAGE_SIZE, 0);
    expect(verifyPage(buf, 10)).toBe(true);

    FS.read(fd, buf, 0, PAGE_SIZE, PAGE_SIZE);
    expect(verifyPage(buf, 20)).toBe(true);

    FS.read(fd, buf, 0, PAGE_SIZE, PAGE_SIZE * 4);
    expect(verifyPage(buf, 30)).toBe(true);

    FS.close(fd);
  });

  it("repeated allocate on same fd preserves early-page data (4-page cache)", async () => {
    // Simulates Postgres extending a relation file multiple times during
    // bulk INSERT. Each allocate grows the file, and subsequent writes
    // fill in the allocated space. Under a 4-page cache, earlier pages
    // are frequently evicted.
    const { FS } = await mountTome(backend, 4);
    const path = `${MOUNT}/bulkext`;

    const fd = FS.open(path, O.RDWR | O.CREAT, 0o666);
    const writtenPages: number[] = [];

    for (let round = 0; round < 5; round++) {
      const basePageIdx = round * 3;
      // Allocate 3 more pages
      fd.stream_ops.allocate(fd, 0, (basePageIdx + 3) * PAGE_SIZE);
      // Write to the first page of this round's allocation
      FS.write(
        fd,
        fillPage(basePageIdx),
        0,
        PAGE_SIZE,
        basePageIdx * PAGE_SIZE,
      );
      writtenPages.push(basePageIdx);
    }

    // Verify all written pages (most will require backend reads due to
    // 4-page cache with 15 total pages)
    const buf = new Uint8Array(PAGE_SIZE);
    for (const pageIdx of writtenPages) {
      FS.read(fd, buf, 0, PAGE_SIZE, pageIdx * PAGE_SIZE);
      expect(verifyPage(buf, pageIdx)).toBe(true);
    }

    FS.close(fd);
  });

  it("allocate + syncfs + write + read round-trip (4-page cache)", async () => {
    // Verify that allocate'd files persist correctly and _pages works
    // after syncfs flushes dirty state.
    const { FS, tomefs } = await mountTome(backend, 4);
    const path = `${MOUNT}/persist`;

    const fd = FS.open(path, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, fillPage(55), 0, PAGE_SIZE, 0);

    // Allocate to 6 pages
    fd.stream_ops.allocate(fd, 0, PAGE_SIZE * 6);

    // Syncfs to persist
    tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
      if (err) throw err;
    });

    // Write to an allocated page (page 3) AFTER sync
    FS.write(fd, fillPage(66), 0, PAGE_SIZE, PAGE_SIZE * 3);

    // Read back both pages — page 0 should survive cache pressure + sync
    const buf = new Uint8Array(PAGE_SIZE);
    FS.read(fd, buf, 0, PAGE_SIZE, 0);
    expect(verifyPage(buf, 55)).toBe(true);

    FS.read(fd, buf, 0, PAGE_SIZE, PAGE_SIZE * 3);
    expect(verifyPage(buf, 66)).toBe(true);

    FS.close(fd);
  });

  it("interleaved allocate on two files (4-page cache)", async () => {
    // Two files being extended alternately — cache pressure forces cross-file
    // eviction, testing that _pages stale detection works for both files.
    const { FS } = await mountTome(backend, 4);
    const pathA = `${MOUNT}/fileA`;
    const pathB = `${MOUNT}/fileB`;

    const fdA = FS.open(pathA, O.RDWR | O.CREAT, 0o666);
    const fdB = FS.open(pathB, O.RDWR | O.CREAT, 0o666);

    // Write page 0 of each file
    FS.write(fdA, fillPage(100), 0, PAGE_SIZE, 0);
    FS.write(fdB, fillPage(200), 0, PAGE_SIZE, 0);

    // Allocate 3 pages on each file
    fdA.stream_ops.allocate(fdA, 0, PAGE_SIZE * 3);
    fdB.stream_ops.allocate(fdB, 0, PAGE_SIZE * 3);

    // Write page 1 of each
    FS.write(fdA, fillPage(101), 0, PAGE_SIZE, PAGE_SIZE);
    FS.write(fdB, fillPage(201), 0, PAGE_SIZE, PAGE_SIZE);

    // Extend again
    fdA.stream_ops.allocate(fdA, 0, PAGE_SIZE * 5);
    fdB.stream_ops.allocate(fdB, 0, PAGE_SIZE * 5);

    // Write to page 3 of each
    FS.write(fdA, fillPage(103), 0, PAGE_SIZE, PAGE_SIZE * 3);
    FS.write(fdB, fillPage(203), 0, PAGE_SIZE, PAGE_SIZE * 3);

    // Verify all written pages
    const buf = new Uint8Array(PAGE_SIZE);
    for (const [fd, seeds] of [
      [fdA, [100, 101, 103]] as const,
      [fdB, [200, 201, 203]] as const,
    ]) {
      for (let i = 0; i < seeds.length; i++) {
        const pos = [0, PAGE_SIZE, PAGE_SIZE * 3][i];
        FS.read(fd, buf, 0, PAGE_SIZE, pos);
        expect(verifyPage(buf, seeds[i])).toBe(true);
      }
    }

    FS.close(fdA);
    FS.close(fdB);
  });

  it("truncate-down then extend preserves truncation (clears _pages)", async () => {
    // Shrink MUST clear _pages: pages beyond the new size are invalidated.
    // Then extend should NOT corrupt the surviving data.
    const { FS } = await mountTome(backend, 4096);
    const path = `${MOUNT}/trunc-extend`;

    const fd = FS.open(path, O.RDWR | O.CREAT, 0o666);
    // Write 4 pages
    for (let i = 0; i < 4; i++) {
      FS.write(fd, fillPage(i), 0, PAGE_SIZE, i * PAGE_SIZE);
    }

    // Truncate to 2 pages — must clear _pages
    FS.ftruncate(fd.fd, PAGE_SIZE * 2);
    expect(FS.fstat(fd.fd).size).toBe(PAGE_SIZE * 2);

    // Extend back to 4 pages
    FS.ftruncate(fd.fd, PAGE_SIZE * 4);
    expect(FS.fstat(fd.fd).size).toBe(PAGE_SIZE * 4);

    // Pages 0-1 should have original data, pages 2-3 should be zeros
    const buf = new Uint8Array(PAGE_SIZE);
    FS.read(fd, buf, 0, PAGE_SIZE, 0);
    expect(verifyPage(buf, 0)).toBe(true);

    FS.read(fd, buf, 0, PAGE_SIZE, PAGE_SIZE);
    expect(verifyPage(buf, 1)).toBe(true);

    FS.read(fd, buf, 0, PAGE_SIZE, PAGE_SIZE * 2);
    expect(buf.every((b) => b === 0)).toBe(true);

    FS.read(fd, buf, 0, PAGE_SIZE, PAGE_SIZE * 3);
    expect(buf.every((b) => b === 0)).toBe(true);

    FS.close(fd);
  });

  it("sub-page writes survive allocate without corruption (4-page cache)", async () => {
    // Small writes (like WAL records) that don't fill a page.
    // After allocate, the partial page must retain its data.
    const { FS } = await mountTome(backend, 4);
    const path = `${MOUNT}/subpage`;

    const fd = FS.open(path, O.RDWR | O.CREAT, 0o666);
    const marker = new TextEncoder().encode("WAL-RECORD-42");
    FS.write(fd, marker, 0, marker.length, 0);

    // Allocate many more pages (causes cache pressure)
    fd.stream_ops.allocate(fd, 0, PAGE_SIZE * 10);
    expect(FS.fstat(fd.fd).size).toBe(PAGE_SIZE * 10);

    // Write to a distant page to force eviction
    FS.write(fd, fillPage(77), 0, PAGE_SIZE, PAGE_SIZE * 9);

    // Original sub-page data should survive eviction + re-read
    const readBuf = new Uint8Array(marker.length);
    FS.read(fd, readBuf, 0, marker.length, 0);
    expect(new TextDecoder().decode(readBuf)).toBe("WAL-RECORD-42");

    FS.close(fd);
  });
});
