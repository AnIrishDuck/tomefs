/**
 * Adversarial differential tests: Concurrent FDs under cache eviction pressure.
 *
 * Targets the intersection of concurrent file descriptor semantics and cache
 * eviction — specifically the per-node page table's stale reference detection
 * when multiple FDs to the same file are active while pages are being evicted
 * by other files' operations.
 *
 * These scenarios model real Postgres patterns: multiple backends sharing
 * heap/index files while background writers, checkpointers, and sequential
 * scans compete for cache slots.
 *
 * Ethos §9: "concurrent streams competing for cache slots"
 */
import {
  createFS,
  O,
  SEEK_SET,
  type FSHarness,
} from "../harness/emscripten-fs.js";

const PAGE_SIZE = 8192;

async function createSmallCacheFS(): Promise<FSHarness> {
  const origMaxPages = process.env.TOMEFS_MAX_PAGES;
  process.env.TOMEFS_MAX_PAGES = "4";
  try {
    return await createFS();
  } finally {
    if (origMaxPages !== undefined) {
      process.env.TOMEFS_MAX_PAGES = origMaxPages;
    } else {
      delete process.env.TOMEFS_MAX_PAGES;
    }
  }
}

describe("adversarial: concurrent FDs under cache eviction", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createSmallCacheFS();
  });

  // ------------------------------------------------------------------
  // FD1 populates page table, eviction clears cache, FD2 writes,
  // FD1 reads — must see FD2's write despite stale page table entry
  // ------------------------------------------------------------------

  it("stale page table entry detected after cross-file eviction @fast", () => {
    const { FS } = h;

    // Create target file with 2 pages
    const setup = FS.open("/target", O.RDWR | O.CREAT, 0o666);
    const page0 = new Uint8Array(PAGE_SIZE);
    page0.fill(0x11);
    FS.write(setup, page0, 0, PAGE_SIZE, 0);
    const page1 = new Uint8Array(PAGE_SIZE);
    page1.fill(0x22);
    FS.write(setup, page1, 0, PAGE_SIZE, PAGE_SIZE);
    FS.close(setup);

    // FD1 reads page 0 — populates per-node page table
    const fd1 = FS.open("/target", O.RDONLY);
    const buf1 = new Uint8Array(PAGE_SIZE);
    FS.read(fd1, buf1, 0, PAGE_SIZE, 0);
    expect(buf1[0]).toBe(0x11);

    // Thrash cache via a different file to evict target's pages
    const thrash = FS.open("/thrash", O.RDWR | O.CREAT, 0o666);
    for (let p = 0; p < 8; p++) {
      const fill = new Uint8Array(PAGE_SIZE);
      fill.fill(p + 0x80);
      FS.write(thrash, fill, 0, PAGE_SIZE, p * PAGE_SIZE);
    }
    FS.close(thrash);

    // FD2 writes new data to page 0 of target
    const fd2 = FS.open("/target", O.RDWR);
    const newData = new Uint8Array(PAGE_SIZE);
    newData.fill(0xFF);
    FS.write(fd2, newData, 0, PAGE_SIZE, 0);

    // FD1 reads page 0 again — must see FD2's write, not stale data
    const buf2 = new Uint8Array(PAGE_SIZE);
    FS.read(fd1, buf2, 0, PAGE_SIZE, 0);
    expect(buf2[0]).toBe(0xFF);
    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(buf2[i]).toBe(0xFF);
    }

    FS.close(fd1);
    FS.close(fd2);
  });

  // ------------------------------------------------------------------
  // Multiple FDs alternating reads/writes on different pages of the
  // same file while cache can't hold all pages simultaneously
  // ------------------------------------------------------------------

  it("alternating reads/writes across FDs with full cache rotation", () => {
    const { FS } = h;
    const numPages = 12; // 3x cache size

    // Create file with unique data per page
    const init = FS.open("/shared", O.RDWR | O.CREAT, 0o666);
    for (let p = 0; p < numPages; p++) {
      const data = new Uint8Array(PAGE_SIZE);
      data.fill(p & 0xFF);
      FS.write(init, data, 0, PAGE_SIZE, p * PAGE_SIZE);
    }
    FS.close(init);

    // Open two FDs to the same file
    const reader = FS.open("/shared", O.RDONLY);
    const writer = FS.open("/shared", O.RDWR);

    // Interleave: writer modifies page N, reader reads page N-1
    // This forces cache eviction between operations
    for (let p = 1; p < numPages; p++) {
      // Writer modifies page p with new pattern
      const writeData = new Uint8Array(PAGE_SIZE);
      writeData.fill((p + 0x80) & 0xFF);
      FS.write(writer, writeData, 0, PAGE_SIZE, p * PAGE_SIZE);

      // Reader reads page p-1 — previously read or unmodified
      const readBuf = new Uint8Array(PAGE_SIZE);
      FS.read(reader, readBuf, 0, PAGE_SIZE, (p - 1) * PAGE_SIZE);

      const expected = p === 1 ? 0 : ((p - 1) + 0x80) & 0xFF;
      for (let i = 0; i < PAGE_SIZE; i++) {
        if (readBuf[i] !== expected) {
          throw new Error(
            `Page ${p - 1} byte ${i}: expected 0x${expected.toString(16)}, got 0x${readBuf[i].toString(16)}`,
          );
        }
      }
    }

    FS.close(reader);
    FS.close(writer);
  });

  // ------------------------------------------------------------------
  // Two FDs to different files competing for the same cache slots,
  // with concurrent positional I/O
  // ------------------------------------------------------------------

  it("cross-file FD competition with positional I/O @fast", () => {
    const { FS } = h;
    const pagesPerFile = 8; // Each file 2x cache size

    // Create two files with distinct patterns
    for (const [name, base] of [["/alpha", 0x10], ["/beta", 0xA0]] as const) {
      const s = FS.open(name, O.RDWR | O.CREAT, 0o666);
      for (let p = 0; p < pagesPerFile; p++) {
        const data = new Uint8Array(PAGE_SIZE);
        data.fill((base + p) & 0xFF);
        FS.write(s, data, 0, PAGE_SIZE, p * PAGE_SIZE);
      }
      FS.close(s);
    }

    // Open both files and interleave positional reads
    const fdA = FS.open("/alpha", O.RDONLY);
    const fdB = FS.open("/beta", O.RDONLY);
    const buf = new Uint8Array(PAGE_SIZE);

    // Round-robin reads that exceed cache capacity
    for (let round = 0; round < 3; round++) {
      for (let p = 0; p < pagesPerFile; p++) {
        // Read from alpha
        FS.read(fdA, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
        const expectedA = (0x10 + p) & 0xFF;
        expect(buf[0]).toBe(expectedA);
        expect(buf[PAGE_SIZE - 1]).toBe(expectedA);

        // Read from beta (evicts alpha's page)
        FS.read(fdB, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
        const expectedB = (0xA0 + p) & 0xFF;
        expect(buf[0]).toBe(expectedB);
        expect(buf[PAGE_SIZE - 1]).toBe(expectedB);
      }
    }

    FS.close(fdA);
    FS.close(fdB);
  });

  // ------------------------------------------------------------------
  // One FD writes sequentially while another does random reads,
  // cache too small for all pages (Postgres bgwriter + scan conflict)
  // ------------------------------------------------------------------

  it("sequential writer vs random reader under eviction pressure", () => {
    const { FS } = h;
    const numPages = 16;

    // Create file with initial data
    const init = FS.open("/bgwriter", O.RDWR | O.CREAT, 0o666);
    for (let p = 0; p < numPages; p++) {
      const data = new Uint8Array(PAGE_SIZE);
      data.fill(p & 0xFF);
      FS.write(init, data, 0, PAGE_SIZE, p * PAGE_SIZE);
    }
    FS.close(init);

    const seqWriter = FS.open("/bgwriter", O.RDWR);
    const randReader = FS.open("/bgwriter", O.RDONLY);

    // Pseudorandom page access order for reader
    const readOrder = [7, 2, 15, 0, 11, 4, 13, 8, 1, 14, 6, 3, 9, 12, 5, 10];
    const modified = new Set<number>();

    // Writer overwrites pages sequentially; reader reads randomly
    for (let i = 0; i < numPages; i++) {
      // Writer modifies page i
      const writeData = new Uint8Array(PAGE_SIZE);
      writeData.fill((i + 0x40) & 0xFF);
      FS.write(seqWriter, writeData, 0, PAGE_SIZE, i * PAGE_SIZE);
      modified.add(i);

      // Reader reads a random page
      const readPage = readOrder[i];
      const readBuf = new Uint8Array(PAGE_SIZE);
      FS.read(randReader, readBuf, 0, PAGE_SIZE, readPage * PAGE_SIZE);

      const expected = modified.has(readPage)
        ? (readPage + 0x40) & 0xFF
        : readPage & 0xFF;
      for (let j = 0; j < PAGE_SIZE; j++) {
        if (readBuf[j] !== expected) {
          throw new Error(
            `After writing page ${i}, reading page ${readPage} byte ${j}: ` +
            `expected 0x${expected.toString(16)}, got 0x${readBuf[j].toString(16)}`,
          );
        }
      }
    }

    FS.close(seqWriter);
    FS.close(randReader);
  });

  // ------------------------------------------------------------------
  // Read through FD1, evict via other file, write + flush via FD2,
  // re-read through FD1 — verifies dirty eviction + re-fetch path
  // ------------------------------------------------------------------

  it("dirty eviction between concurrent FD accesses", () => {
    const { FS } = h;

    // Create target with 2 pages
    const init = FS.open("/dirty", O.RDWR | O.CREAT, 0o666);
    const initial = new Uint8Array(PAGE_SIZE * 2);
    initial.fill(0xAA);
    FS.write(init, initial, 0, initial.length, 0);
    FS.close(init);

    const fd1 = FS.open("/dirty", O.RDONLY);
    const fd2 = FS.open("/dirty", O.RDWR);

    // FD1 reads page 0 (caches it)
    const buf = new Uint8Array(PAGE_SIZE);
    FS.read(fd1, buf, 0, PAGE_SIZE, 0);
    expect(buf[0]).toBe(0xAA);

    // FD2 writes to page 0 (marks dirty)
    const newData = new Uint8Array(PAGE_SIZE);
    newData.fill(0xBB);
    FS.write(fd2, newData, 0, PAGE_SIZE, 0);

    // Thrash cache to force dirty eviction of page 0
    const thrash = FS.open("/thrash2", O.RDWR | O.CREAT, 0o666);
    for (let p = 0; p < 8; p++) {
      const fill = new Uint8Array(PAGE_SIZE);
      fill.fill(p);
      FS.write(thrash, fill, 0, PAGE_SIZE, p * PAGE_SIZE);
    }
    FS.close(thrash);

    // FD1 reads page 0 again — must get 0xBB (written by FD2, flushed
    // during dirty eviction, then re-fetched from backend)
    const buf2 = new Uint8Array(PAGE_SIZE);
    FS.read(fd1, buf2, 0, PAGE_SIZE, 0);
    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(buf2[i]).toBe(0xBB);
    }

    FS.close(fd1);
    FS.close(fd2);
  });

  // ------------------------------------------------------------------
  // Three FDs (reader, writer, scanner) on the same file with extreme
  // cache pressure — models Postgres with backend, bgwriter, and
  // sequential scan all active simultaneously
  // ------------------------------------------------------------------

  it("three-way FD contention under extreme cache pressure", () => {
    const { FS } = h;
    const numPages = 8;

    // Create file
    const init = FS.open("/three_way", O.RDWR | O.CREAT, 0o666);
    for (let p = 0; p < numPages; p++) {
      const data = new Uint8Array(PAGE_SIZE);
      data.fill(p & 0xFF);
      FS.write(init, data, 0, PAGE_SIZE, p * PAGE_SIZE);
    }
    FS.close(init);

    const scanner = FS.open("/three_way", O.RDONLY);
    const writer = FS.open("/three_way", O.RDWR);
    const reader = FS.open("/three_way", O.RDONLY);

    // Scanner reads pages forward, writer modifies pages, reader
    // verifies random pages — all competing for 4 cache slots
    const buf = new Uint8Array(PAGE_SIZE);

    for (let p = 0; p < numPages; p++) {
      // Scanner reads page p sequentially
      FS.read(scanner, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expect(buf[0]).toBe(p & 0xFF);

      // Writer modifies page p
      const writeData = new Uint8Array(PAGE_SIZE);
      writeData.fill((p + 0x50) & 0xFF);
      FS.write(writer, writeData, 0, PAGE_SIZE, p * PAGE_SIZE);

      // Reader verifies the write (reads same page through different FD)
      FS.read(reader, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expect(buf[0]).toBe((p + 0x50) & 0xFF);
    }

    // Final verification: scan all pages to ensure everything is consistent
    for (let p = 0; p < numPages; p++) {
      FS.read(scanner, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
      const expected = (p + 0x50) & 0xFF;
      for (let i = 0; i < PAGE_SIZE; i++) {
        if (buf[i] !== expected) {
          throw new Error(
            `Final scan page ${p} byte ${i}: expected 0x${expected.toString(16)}, got 0x${buf[i].toString(16)}`,
          );
        }
      }
    }

    FS.close(scanner);
    FS.close(writer);
    FS.close(reader);
  });

  // ------------------------------------------------------------------
  // FD1 extends file via append, FD2 reads earlier pages — eviction
  // during extension must not corrupt already-written pages
  // ------------------------------------------------------------------

  it("append extension through FD1 while FD2 reads under eviction", () => {
    const { FS } = h;

    // Start with 2 pages
    const init = FS.open("/extend", O.RDWR | O.CREAT, 0o666);
    const initData = new Uint8Array(PAGE_SIZE * 2);
    initData.subarray(0, PAGE_SIZE).fill(0x11);
    initData.subarray(PAGE_SIZE).fill(0x22);
    FS.write(init, initData, 0, initData.length, 0);
    FS.close(init);

    const appender = FS.open("/extend", O.RDWR);
    const reader = FS.open("/extend", O.RDONLY);

    // Append 8 more pages (extend file beyond cache capacity)
    for (let p = 2; p < 10; p++) {
      const data = new Uint8Array(PAGE_SIZE);
      data.fill((p + 0x30) & 0xFF);
      FS.write(appender, data, 0, PAGE_SIZE, p * PAGE_SIZE);

      // After each append, reader reads an earlier page to verify
      // it wasn't corrupted by the extension + eviction
      const readPage = p % 2 === 0 ? 0 : 1;
      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(reader, buf, 0, PAGE_SIZE, readPage * PAGE_SIZE);
      const expected = readPage === 0 ? 0x11 : 0x22;
      for (let i = 0; i < PAGE_SIZE; i++) {
        if (buf[i] !== expected) {
          throw new Error(
            `After appending page ${p}, reading page ${readPage} byte ${i}: ` +
            `expected 0x${expected.toString(16)}, got 0x${buf[i].toString(16)}`,
          );
        }
      }
    }

    // Verify all appended pages are readable
    for (let p = 2; p < 10; p++) {
      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(reader, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
      const expected = (p + 0x30) & 0xFF;
      for (let i = 0; i < PAGE_SIZE; i++) {
        if (buf[i] !== expected) {
          throw new Error(
            `Appended page ${p} byte ${i}: expected 0x${expected.toString(16)}, got 0x${buf[i].toString(16)}`,
          );
        }
      }
    }

    FS.close(appender);
    FS.close(reader);
  });

  // ------------------------------------------------------------------
  // Cross-page boundary reads through one FD while another FD writes
  // to the same page boundary region — eviction between operations
  // ------------------------------------------------------------------

  it("cross-page boundary read/write with concurrent FDs under eviction", () => {
    const { FS } = h;

    // Create 8-page file
    const init = FS.open("/boundary", O.RDWR | O.CREAT, 0o666);
    const full = new Uint8Array(PAGE_SIZE * 8);
    full.fill(0xCC);
    FS.write(init, full, 0, full.length, 0);
    FS.close(init);

    const fd1 = FS.open("/boundary", O.RDWR);
    const fd2 = FS.open("/boundary", O.RDONLY);

    // Write data straddling page boundaries through FD1
    for (let boundary = 1; boundary < 8; boundary++) {
      const pos = boundary * PAGE_SIZE - 64;
      const data = new Uint8Array(128);
      data.fill(boundary & 0xFF);
      FS.write(fd1, data, 0, 128, pos);
    }

    // Read through FD2 and verify each boundary region
    for (let boundary = 1; boundary < 8; boundary++) {
      const pos = boundary * PAGE_SIZE - 64;
      const buf = new Uint8Array(128);
      FS.read(fd2, buf, 0, 128, pos);
      for (let i = 0; i < 128; i++) {
        if (buf[i] !== (boundary & 0xFF)) {
          throw new Error(
            `Boundary ${boundary} byte ${i}: expected 0x${(boundary & 0xFF).toString(16)}, got 0x${buf[i].toString(16)}`,
          );
        }
      }
    }

    FS.close(fd1);
    FS.close(fd2);
  });

  // ------------------------------------------------------------------
  // Truncate through one FD while another has pages cached — cached
  // pages beyond new size must not be served after truncation
  // ------------------------------------------------------------------

  it("truncate through FD1 invalidates FD2's cached pages beyond new size", () => {
    const { FS } = h;

    // Create 8-page file
    const init = FS.open("/trunc_shared", O.RDWR | O.CREAT, 0o666);
    for (let p = 0; p < 8; p++) {
      const data = new Uint8Array(PAGE_SIZE);
      data.fill((p + 1) & 0xFF);
      FS.write(init, data, 0, PAGE_SIZE, p * PAGE_SIZE);
    }
    FS.close(init);

    // FD2 reads pages 4-7 to populate cache/page table
    const fd2 = FS.open("/trunc_shared", O.RDONLY);
    for (let p = 4; p < 8; p++) {
      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(fd2, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expect(buf[0]).toBe((p + 1) & 0xFF);
    }

    // FD1 truncates to 3 pages — pages 3-7 should be invalidated
    const fd1 = FS.open("/trunc_shared", O.RDWR);
    FS.ftruncate(fd1.fd, PAGE_SIZE * 3);

    // FD2: size should reflect truncation
    expect(FS.fstat(fd2.fd).size).toBe(PAGE_SIZE * 3);

    // FD2: reads beyond new size should return 0 bytes
    const buf = new Uint8Array(PAGE_SIZE);
    const n = FS.read(fd2, buf, 0, PAGE_SIZE, PAGE_SIZE * 4);
    expect(n).toBe(0);

    // FD2: pages within new size should still be correct
    FS.read(fd2, buf, 0, PAGE_SIZE, 0);
    expect(buf[0]).toBe(0x01);
    FS.read(fd2, buf, 0, PAGE_SIZE, PAGE_SIZE * 2);
    expect(buf[0]).toBe(0x03);

    FS.close(fd1);
    FS.close(fd2);
  });
});
