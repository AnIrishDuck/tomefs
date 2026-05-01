/**
 * Adversarial tests: multi-page per-node _pages table fast path.
 *
 * The per-node page table was extended to multi-page reads and writes
 * (cross-page boundary I/O). When all pages in a range are cached and
 * not evicted, data is copied directly via the node._pages references
 * without any page cache Map lookups, key construction, or LRU
 * reordering.
 *
 * These tests target the seams between the multi-page fast path and:
 * - Eviction (partial eviction within a page range)
 * - Rename (page table cleared, cold path must re-populate)
 * - Truncation (page table reset, stale references must not be used)
 * - Interleaved multi-file access under cache pressure
 * - LRU ordering (fast path bypasses LRU reordering)
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

function fillBuf(size: number, seed: number): Uint8Array {
  const buf = new Uint8Array(size);
  for (let i = 0; i < size; i++) {
    buf[i] = (seed + i * 37) & 0xff;
  }
  return buf;
}

function verifyBuf(buf: Uint8Array, size: number, seed: number): boolean {
  for (let i = 0; i < size; i++) {
    if (buf[i] !== ((seed + i * 37) & 0xff)) return false;
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

describe("adversarial: multi-page _pages table fast path", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("cross-page write then read uses fast path on second access @fast", async () => {
    const { FS } = await mountTome(backend, 64);
    const path = `${MOUNT}/cross`;
    const fd = FS.open(path, O.RDWR | O.CREAT, 0o666);

    // Write spanning pages 0-1 (cross-page boundary)
    const writeSize = PAGE_SIZE + 100;
    const data = fillBuf(writeSize, 1);
    FS.write(fd, data, 0, writeSize, PAGE_SIZE - 50);

    // First read: cold path (populates page table)
    const buf1 = new Uint8Array(writeSize);
    FS.read(fd, buf1, 0, writeSize, PAGE_SIZE - 50);
    expect(verifyBuf(buf1, writeSize, 1)).toBe(true);

    // Second read: warm path (should use page table fast path)
    const buf2 = new Uint8Array(writeSize);
    FS.read(fd, buf2, 0, writeSize, PAGE_SIZE - 50);
    expect(verifyBuf(buf2, writeSize, 1)).toBe(true);

    FS.close(fd);
  });

  it("cross-page write overwrite uses fast path on second write @fast", async () => {
    const { FS } = await mountTome(backend, 64);
    const path = `${MOUNT}/overwrite`;
    const fd = FS.open(path, O.RDWR | O.CREAT, 0o666);

    const writeSize = PAGE_SIZE + 200;
    // First write: cold path (populates page table)
    FS.write(fd, fillBuf(writeSize, 10), 0, writeSize, PAGE_SIZE - 100);

    // Second write at same position: should use page table fast path
    FS.write(fd, fillBuf(writeSize, 20), 0, writeSize, PAGE_SIZE - 100);

    // Verify the second write took effect
    const buf = new Uint8Array(writeSize);
    FS.read(fd, buf, 0, writeSize, PAGE_SIZE - 100);
    expect(verifyBuf(buf, writeSize, 20)).toBe(true);

    FS.close(fd);
  });

  it("partial eviction within cross-page range forces cold path @fast", async () => {
    // 4-page cache: write across pages 0-1, then fill cache to evict page 0.
    // Cross-page read should detect the eviction and fall to cold path.
    const { FS } = await mountTome(backend, 4);
    const path = `${MOUNT}/partial`;
    const otherPath = `${MOUNT}/other`;
    const fd = FS.open(path, O.RDWR | O.CREAT, 0o666);

    // Write spanning pages 0-1
    const writeSize = PAGE_SIZE + 500;
    FS.write(fd, fillBuf(writeSize, 42), 0, writeSize, PAGE_SIZE - 250);

    // Read to populate page table
    const readBuf = new Uint8Array(writeSize);
    FS.read(fd, readBuf, 0, writeSize, PAGE_SIZE - 250);
    expect(verifyBuf(readBuf, writeSize, 42)).toBe(true);

    // Fill cache with another file's pages to evict our pages
    const fd2 = FS.open(otherPath, O.RDWR | O.CREAT, 0o666);
    for (let i = 0; i < 4; i++) {
      FS.write(fd2, fillBuf(PAGE_SIZE, 100 + i), 0, PAGE_SIZE, i * PAGE_SIZE);
    }
    FS.close(fd2);

    // Cross-page read again — page table has stale evicted references.
    // Must detect eviction and fall to cold path.
    const buf2 = new Uint8Array(writeSize);
    FS.read(fd, buf2, 0, writeSize, PAGE_SIZE - 250);
    expect(verifyBuf(buf2, writeSize, 42)).toBe(true);

    FS.close(fd);
  });

  it("rename clears page table, multi-page read re-populates @fast", async () => {
    const { FS } = await mountTome(backend, 64);
    const path1 = `${MOUNT}/before`;
    const path2 = `${MOUNT}/after`;
    const fd = FS.open(path1, O.RDWR | O.CREAT, 0o666);

    // Write spanning pages 0-2 (3 pages)
    const writeSize = PAGE_SIZE * 2 + 100;
    FS.write(fd, fillBuf(writeSize, 77), 0, writeSize, 0);

    // Read to populate page table
    const buf = new Uint8Array(writeSize);
    FS.read(fd, buf, 0, writeSize, 0);
    expect(verifyBuf(buf, writeSize, 77)).toBe(true);

    FS.close(fd);

    // Rename clears _pages on the node
    FS.rename(path1, path2);

    // Re-open and multi-page read — cold path must work with new storage path
    const fd2 = FS.open(path2, O.RDONLY, 0o666);
    const buf2 = new Uint8Array(writeSize);
    FS.read(fd2, buf2, 0, writeSize, 0);
    expect(verifyBuf(buf2, writeSize, 77)).toBe(true);

    // Third read should use re-populated page table (warm path)
    const buf3 = new Uint8Array(writeSize);
    FS.read(fd2, buf3, 0, writeSize, 0);
    expect(verifyBuf(buf3, writeSize, 77)).toBe(true);

    FS.close(fd2);
  });

  it("truncate + extend resets page table for multi-page reads @fast", async () => {
    const { FS } = await mountTome(backend, 64);
    const path = `${MOUNT}/trunc`;
    const fd = FS.open(path, O.RDWR | O.CREAT, 0o666);

    // Write 3 pages worth of data
    const writeSize = PAGE_SIZE * 3;
    FS.write(fd, fillBuf(writeSize, 33), 0, writeSize, 0);

    // Read to populate page table for pages 0-2
    const buf = new Uint8Array(writeSize);
    FS.read(fd, buf, 0, writeSize, 0);
    expect(verifyBuf(buf, writeSize, 33)).toBe(true);

    // Truncate to 1 page — pages 1-2 are invalidated, _pages is cleared
    FS.ftruncate(fd.fd, PAGE_SIZE);

    // Extend back to 3 pages
    FS.ftruncate(fd.fd, PAGE_SIZE * 3);

    // Multi-page read: page 0 has original data, pages 1-2 are zeros
    const buf2 = new Uint8Array(PAGE_SIZE);
    FS.read(fd, buf2, 0, PAGE_SIZE, 0);
    // Page 0 should still have data (survived truncation)
    expect(verifyBuf(buf2, PAGE_SIZE, 33)).toBe(true);

    // Cross-page read spanning pages 1-2 should be all zeros
    const zeroBuf = new Uint8Array(PAGE_SIZE * 2);
    FS.read(fd, zeroBuf, 0, PAGE_SIZE * 2, PAGE_SIZE);
    expect(zeroBuf.every((b) => b === 0)).toBe(true);

    FS.close(fd);
  });

  it("interleaved multi-file cross-page writes under cache pressure", async () => {
    // Two files with cross-page writes, 4-page cache. Each write spans 2
    // pages at non-overlapping positions, so both files' page tables
    // compete for 4 cache slots.
    const { FS } = await mountTome(backend, 4);
    const pathA = `${MOUNT}/fileA`;
    const pathB = `${MOUNT}/fileB`;
    const fdA = FS.open(pathA, O.RDWR | O.CREAT, 0o666);
    const fdB = FS.open(pathB, O.RDWR | O.CREAT, 0o666);

    const crossSize = PAGE_SIZE + 500;

    // Alternate cross-page writes between two files.
    // Space writes 2 pages apart so they don't overlap.
    for (let round = 0; round < 4; round++) {
      const pos = round * PAGE_SIZE * 2;
      FS.write(fdA, fillBuf(crossSize, round * 10), 0, crossSize, pos);
      FS.write(fdB, fillBuf(crossSize, round * 10 + 1), 0, crossSize, pos);
    }

    // Verify data in both files (earlier pages may have been evicted)
    const buf = new Uint8Array(crossSize);
    for (let round = 0; round < 4; round++) {
      const pos = round * PAGE_SIZE * 2;
      FS.read(fdA, buf, 0, crossSize, pos);
      expect(verifyBuf(buf, crossSize, round * 10)).toBe(true);

      FS.read(fdB, buf, 0, crossSize, pos);
      expect(verifyBuf(buf, crossSize, round * 10 + 1)).toBe(true);
    }

    FS.close(fdA);
    FS.close(fdB);
  });

  it("sequential scan exceeding cache rotates page table entries", async () => {
    // Large sequential read through a file bigger than cache.
    // Each cross-page read evicts earlier pages. Page table must
    // handle continuous eviction-and-repopulation.
    const { FS } = await mountTome(backend, 4);
    const path = `${MOUNT}/scan`;
    const fd = FS.open(path, O.RDWR | O.CREAT, 0o666);

    const totalPages = 12;
    // Write all pages with distinct data
    for (let i = 0; i < totalPages; i++) {
      FS.write(fd, fillBuf(PAGE_SIZE, i), 0, PAGE_SIZE, i * PAGE_SIZE);
    }

    // Sequential cross-page reads spanning 2 pages each
    const crossSize = PAGE_SIZE + 100;
    const buf = new Uint8Array(crossSize);
    for (let i = 0; i < totalPages - 1; i++) {
      FS.read(fd, buf, 0, crossSize, i * PAGE_SIZE);
      // Verify the first PAGE_SIZE bytes match page i's data
      expect(verifyBuf(buf, PAGE_SIZE, i)).toBe(true);
    }

    // Reverse scan — re-reads all evicted pages
    for (let i = totalPages - 2; i >= 0; i--) {
      FS.read(fd, buf, 0, crossSize, i * PAGE_SIZE);
      expect(verifyBuf(buf, PAGE_SIZE, i)).toBe(true);
    }

    FS.close(fd);
  });

  it("cross-page write + syncfs + re-read verifies persistence @fast", async () => {
    const { FS, tomefs } = await mountTome(backend, 64);
    const path = `${MOUNT}/persist`;
    const fd = FS.open(path, O.RDWR | O.CREAT, 0o666);

    // Cross-page write spanning pages 1-2
    const writeSize = PAGE_SIZE + 300;
    FS.write(fd, fillBuf(writeSize, 88), 0, writeSize, PAGE_SIZE - 150);

    // Sync to backend
    tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
      if (err) throw err;
    });

    // Read back — should use page table fast path
    const buf = new Uint8Array(writeSize);
    FS.read(fd, buf, 0, writeSize, PAGE_SIZE - 150);
    expect(verifyBuf(buf, writeSize, 88)).toBe(true);

    FS.close(fd);
  });

  it("cross-page write at file end extends correctly @fast", async () => {
    const { FS } = await mountTome(backend, 64);
    const path = `${MOUNT}/extend`;
    const fd = FS.open(path, O.RDWR | O.CREAT, 0o666);

    // Write first page
    FS.write(fd, fillBuf(PAGE_SIZE, 1), 0, PAGE_SIZE, 0);

    // Cross-page write that extends the file into page 1
    const extendSize = 500;
    FS.write(
      fd,
      fillBuf(extendSize, 2),
      0,
      extendSize,
      PAGE_SIZE - 200,
    );

    // Verify: first PAGE_SIZE - 200 bytes are from seed 1,
    // then 500 bytes from seed 2
    const fullBuf = new Uint8Array(PAGE_SIZE + 300);
    FS.read(fd, fullBuf, 0, PAGE_SIZE + 300, 0);

    // Check the original data before the cross-page write
    const prefix = fullBuf.subarray(0, PAGE_SIZE - 200);
    expect(verifyBuf(prefix, PAGE_SIZE - 200, 1)).toBe(true);

    // Check the cross-page written data
    const crossData = fullBuf.subarray(PAGE_SIZE - 200, PAGE_SIZE - 200 + extendSize);
    expect(verifyBuf(crossData, extendSize, 2)).toBe(true);

    FS.close(fd);
  });

  it("rename-over-target with multi-page data preserves source @fast", async () => {
    const { FS } = await mountTome(backend, 64);
    const src = `${MOUNT}/src`;
    const dst = `${MOUNT}/dst`;

    // Create source with cross-page data
    const fd1 = FS.open(src, O.RDWR | O.CREAT, 0o666);
    const writeSize = PAGE_SIZE * 2 + 500;
    FS.write(fd1, fillBuf(writeSize, 55), 0, writeSize, 0);
    FS.close(fd1);

    // Create destination with different data
    const fd2 = FS.open(dst, O.RDWR | O.CREAT, 0o666);
    FS.write(fd2, fillBuf(PAGE_SIZE, 99), 0, PAGE_SIZE, 0);
    FS.close(fd2);

    // Rename source over destination
    FS.rename(src, dst);

    // Read destination — should have source's multi-page data
    const fd3 = FS.open(dst, O.RDONLY, 0o666);
    const buf = new Uint8Array(writeSize);
    FS.read(fd3, buf, 0, writeSize, 0);
    expect(verifyBuf(buf, writeSize, 55)).toBe(true);

    // Second read should use page table fast path
    const buf2 = new Uint8Array(writeSize);
    FS.read(fd3, buf2, 0, writeSize, 0);
    expect(verifyBuf(buf2, writeSize, 55)).toBe(true);

    FS.close(fd3);
  });

  it("alternating single-page and multi-page reads share page table", async () => {
    // Verify that single-page reads populate _pages entries that the
    // multi-page fast path can reuse, and vice versa.
    const { FS } = await mountTome(backend, 64);
    const path = `${MOUNT}/mixed`;
    const fd = FS.open(path, O.RDWR | O.CREAT, 0o666);

    // Write 4 pages
    for (let i = 0; i < 4; i++) {
      FS.write(fd, fillBuf(PAGE_SIZE, i * 10), 0, PAGE_SIZE, i * PAGE_SIZE);
    }

    // Single-page reads to populate pages 0, 1, 2, 3 in page table
    const pageBuf = new Uint8Array(PAGE_SIZE);
    for (let i = 0; i < 4; i++) {
      FS.read(fd, pageBuf, 0, PAGE_SIZE, i * PAGE_SIZE);
      expect(verifyBuf(pageBuf, PAGE_SIZE, i * 10)).toBe(true);
    }

    // Multi-page read spanning pages 1-2 — should use fast path since
    // both pages are already in the page table from single-page reads
    const crossBuf = new Uint8Array(PAGE_SIZE * 2);
    FS.read(fd, crossBuf, 0, PAGE_SIZE * 2, PAGE_SIZE);
    expect(verifyBuf(crossBuf, PAGE_SIZE, 10)).toBe(true);
    const secondPage = crossBuf.subarray(PAGE_SIZE);
    expect(verifyBuf(secondPage, PAGE_SIZE, 20)).toBe(true);

    // Multi-page read spanning all 4 pages
    const fullBuf = new Uint8Array(PAGE_SIZE * 4);
    FS.read(fd, fullBuf, 0, PAGE_SIZE * 4, 0);
    for (let i = 0; i < 4; i++) {
      const slice = fullBuf.subarray(i * PAGE_SIZE, (i + 1) * PAGE_SIZE);
      expect(verifyBuf(slice, PAGE_SIZE, i * 10)).toBe(true);
    }

    FS.close(fd);
  });

  it("multi-page write after eviction re-populates page table (4-page cache)", async () => {
    const { FS } = await mountTome(backend, 4);
    const path = `${MOUNT}/repop`;
    const otherPath = `${MOUNT}/other`;
    const fd = FS.open(path, O.RDWR | O.CREAT, 0o666);

    // Cross-page write to populate pages 0-1 in page table
    const crossSize = PAGE_SIZE + 200;
    FS.write(fd, fillBuf(crossSize, 11), 0, crossSize, 0);

    // Evict by writing to another file
    const fd2 = FS.open(otherPath, O.RDWR | O.CREAT, 0o666);
    for (let i = 0; i < 4; i++) {
      FS.write(fd2, fillBuf(PAGE_SIZE, 50 + i), 0, PAGE_SIZE, i * PAGE_SIZE);
    }

    // Cross-page overwrite at same position — cold path, re-populates table
    FS.write(fd, fillBuf(crossSize, 22), 0, crossSize, 0);

    // Verify with a cross-page read
    const buf = new Uint8Array(crossSize);
    FS.read(fd, buf, 0, crossSize, 0);
    expect(verifyBuf(buf, crossSize, 22)).toBe(true);

    // Second read should use re-populated page table
    FS.read(fd, buf, 0, crossSize, 0);
    expect(verifyBuf(buf, crossSize, 22)).toBe(true);

    FS.close(fd);
    FS.close(fd2);
  });

  it("three-page cross-boundary read under extreme cache pressure (cache=3)", async () => {
    // Read spanning 3 pages with only 3 cache slots — exactly fills cache.
    // Any other access forces eviction of pages from this read.
    const { FS } = await mountTome(backend, 3);
    const path = `${MOUNT}/tight`;
    const fd = FS.open(path, O.RDWR | O.CREAT, 0o666);

    // Write 3 pages
    for (let i = 0; i < 3; i++) {
      FS.write(fd, fillBuf(PAGE_SIZE, i * 5), 0, PAGE_SIZE, i * PAGE_SIZE);
    }

    // Read spanning all 3 pages (cross-boundary)
    const fullSize = PAGE_SIZE * 3;
    const buf = new Uint8Array(fullSize);
    FS.read(fd, buf, 0, fullSize, 0);
    for (let i = 0; i < 3; i++) {
      const slice = buf.subarray(i * PAGE_SIZE, (i + 1) * PAGE_SIZE);
      expect(verifyBuf(slice, PAGE_SIZE, i * 5)).toBe(true);
    }

    // Write a 4th page — must evict one of the 3 read pages
    FS.write(fd, fillBuf(PAGE_SIZE, 99), 0, PAGE_SIZE, PAGE_SIZE * 3);

    // Re-read spanning pages 0-2 — at least one was evicted
    const buf2 = new Uint8Array(fullSize);
    FS.read(fd, buf2, 0, fullSize, 0);
    for (let i = 0; i < 3; i++) {
      const slice = buf2.subarray(i * PAGE_SIZE, (i + 1) * PAGE_SIZE);
      expect(verifyBuf(slice, PAGE_SIZE, i * 5)).toBe(true);
    }

    FS.close(fd);
  });

  it("cross-page write + truncate-to-boundary + read is consistent", async () => {
    // Write crosses pages 0-1 boundary, then truncate to exactly PAGE_SIZE.
    // The truncation cuts the write in half. Read should return only the
    // first-page portion.
    const { FS } = await mountTome(backend, 64);
    const path = `${MOUNT}/cutwrite`;
    const fd = FS.open(path, O.RDWR | O.CREAT, 0o666);

    const crossSize = PAGE_SIZE + 1000;
    FS.write(fd, fillBuf(crossSize, 7), 0, crossSize, PAGE_SIZE - 500);

    // Truncate to exactly PAGE_SIZE — removes the page-1 portion
    FS.ftruncate(fd.fd, PAGE_SIZE);
    expect(FS.fstat(fd.fd).size).toBe(PAGE_SIZE);

    // Read the last 500 bytes of page 0 — should have data from the write
    const buf = new Uint8Array(500);
    FS.read(fd, buf, 0, 500, PAGE_SIZE - 500);
    expect(verifyBuf(buf, 500, 7)).toBe(true);

    // Read from position PAGE_SIZE should return 0 bytes (EOF)
    const emptyBuf = new Uint8Array(100);
    const bytesRead = FS.read(fd, emptyBuf, 0, 100, PAGE_SIZE);
    expect(bytesRead).toBe(0);

    FS.close(fd);
  });
});
