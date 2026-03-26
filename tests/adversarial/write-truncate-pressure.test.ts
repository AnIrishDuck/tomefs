/**
 * Adversarial differential tests: Write + truncate interactions under cache pressure.
 *
 * These tests target the seams between write, truncate, append, and rename
 * operations when the page cache is under extreme eviction pressure. They
 * mirror real database access patterns:
 *
 * - WAL appends while data pages are being written (O_APPEND + random write)
 * - VACUUM truncation while queries read from the same file
 * - Relation file rename during active reads (Postgres temp table patterns)
 * - Multi-page writes that force eviction of the file being written to
 *
 * All tests pass against MEMFS (no cache) and expose bugs in the page cache's
 * eviction/dirty-flush/re-fetch paths under concurrent-like access patterns.
 */
import {
  createFS,
  encode,
  decode,
  O,
  SEEK_SET,
  SEEK_CUR,
  SEEK_END,
  type FSHarness,
} from "../harness/emscripten-fs.js";

const PAGE_SIZE = 8192;

/**
 * Create a FS harness with a tiny cache to force eviction.
 */
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

describe("adversarial: write + truncate under cache pressure", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createSmallCacheFS();
  });

  // ------------------------------------------------------------------
  // O_APPEND under cache pressure: append forces eviction of own pages
  // ------------------------------------------------------------------

  it("O_APPEND writes survive eviction of earlier pages @fast", () => {
    const { FS } = h;
    const stream = FS.open("/wal", O.RDWR | O.CREAT | O.APPEND, 0o666);

    // Append enough data to exceed the 4-page cache multiple times.
    // Each append should flush/evict earlier pages correctly.
    const records: Uint8Array[] = [];
    for (let i = 0; i < 20; i++) {
      const record = new Uint8Array(PAGE_SIZE / 2); // half-page records
      record.fill(i & 0xff);
      records.push(record);
      FS.write(stream, record, 0, record.length);
    }

    // Verify total size = 20 * (PAGE_SIZE / 2) = 10 pages
    const expectedSize = 20 * (PAGE_SIZE / 2);
    const stat = FS.stat("/wal");
    expect(stat.size).toBe(expectedSize);

    // Read back each record and verify
    const reader = FS.open("/wal", O.RDONLY);
    for (let i = 0; i < 20; i++) {
      const buf = new Uint8Array(PAGE_SIZE / 2);
      const n = FS.read(reader, buf, 0, buf.length, i * (PAGE_SIZE / 2));
      expect(n).toBe(buf.length);
      for (let j = 0; j < buf.length; j++) {
        if (buf[j] !== (i & 0xff)) {
          throw new Error(
            `Record ${i} byte ${j}: expected ${i & 0xff}, got ${buf[j]}`,
          );
        }
      }
    }
    FS.close(reader);
    FS.close(stream);
  });

  it("O_APPEND interleaved with positional reads on same file", () => {
    const { FS } = h;
    const writer = FS.open("/append-read", O.RDWR | O.CREAT | O.APPEND, 0o666);
    const reader = FS.open("/append-read", O.RDONLY);

    // Interleave: append a record, then read back a previous record.
    // This forces the cache to juggle between the append frontier and
    // random reads of already-evicted pages.
    for (let i = 0; i < 16; i++) {
      // Append
      const record = new Uint8Array(PAGE_SIZE);
      record.fill((i * 7 + 3) & 0xff);
      FS.write(writer, record, 0, record.length);

      // Read back a random earlier record (if any exist)
      if (i > 0) {
        const readIdx = (i * 13) % i; // deterministic "random" earlier record
        const buf = new Uint8Array(PAGE_SIZE);
        const n = FS.read(reader, buf, 0, PAGE_SIZE, readIdx * PAGE_SIZE);
        expect(n).toBe(PAGE_SIZE);
        const expected = (readIdx * 7 + 3) & 0xff;
        for (let j = 0; j < PAGE_SIZE; j++) {
          if (buf[j] !== expected) {
            throw new Error(
              `After append ${i}, read of record ${readIdx} byte ${j}: ` +
              `expected ${expected}, got ${buf[j]}`,
            );
          }
        }
      }
    }

    FS.close(writer);
    FS.close(reader);
  });

  // ------------------------------------------------------------------
  // Truncate while another fd reads: simulates VACUUM during queries
  // ------------------------------------------------------------------

  it("truncate file while reader has it open at beyond-EOF position", () => {
    const { FS } = h;

    // Create large file
    const stream = FS.open("/vacuumed", O.RDWR | O.CREAT, 0o666);
    const data = new Uint8Array(PAGE_SIZE * 8);
    for (let i = 0; i < data.length; i++) data[i] = (i * 3) & 0xff;
    FS.write(stream, data, 0, data.length, 0);

    // Open reader positioned at page 6
    const reader = FS.open("/vacuumed", O.RDONLY);
    const buf = new Uint8Array(PAGE_SIZE);
    FS.read(reader, buf, 0, PAGE_SIZE, 6 * PAGE_SIZE);

    // Truncate to 2 pages via the writer
    FS.ftruncate(stream.fd, PAGE_SIZE * 2);

    // Reader should get 0 bytes when reading beyond new EOF
    const buf2 = new Uint8Array(PAGE_SIZE);
    const n = FS.read(reader, buf2, 0, PAGE_SIZE, 6 * PAGE_SIZE);
    expect(n).toBe(0);

    // Reader can still read data within the new size
    const buf3 = new Uint8Array(PAGE_SIZE);
    const n2 = FS.read(reader, buf3, 0, PAGE_SIZE, 0);
    expect(n2).toBe(PAGE_SIZE);
    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(buf3[i]).toBe((i * 3) & 0xff);
    }

    FS.close(reader);
    FS.close(stream);
  });

  it("truncate to zero then regrow: old data must not reappear", () => {
    const { FS } = h;
    const stream = FS.open("/regrow", O.RDWR | O.CREAT, 0o666);

    // Write distinctive data across 6 pages (exceeds 4-page cache)
    const original = new Uint8Array(PAGE_SIZE * 6);
    original.fill(0xde);
    FS.write(stream, original, 0, original.length, 0);

    // Truncate to zero
    FS.ftruncate(stream.fd, 0);
    expect(FS.stat("/regrow").size).toBe(0);

    // Regrow by writing new data at the same positions
    const fresh = new Uint8Array(PAGE_SIZE * 6);
    fresh.fill(0x00); // zeros — must not see 0xDE
    FS.write(stream, fresh, 0, fresh.length, 0);

    // Verify no old data leaks through
    for (let p = 0; p < 6; p++) {
      const buf = new Uint8Array(PAGE_SIZE);
      const n = FS.read(stream, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expect(n).toBe(PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) {
        if (buf[i] !== 0) {
          throw new Error(
            `Page ${p} byte ${i}: expected 0x00 after truncate+regrow, got 0x${buf[i].toString(16)}`,
          );
        }
      }
    }

    FS.close(stream);
  });

  it("repeated truncate-extend cycles preserve correct data", () => {
    const { FS } = h;
    const stream = FS.open("/cycles", O.RDWR | O.CREAT, 0o666);

    // Cycle: write N pages, truncate to 1, extend by writing new data
    // Each cycle's data pattern must not bleed into the next.
    for (let cycle = 0; cycle < 5; cycle++) {
      const fill = (cycle * 37 + 11) & 0xff;

      // Write 8 pages (2x cache)
      const data = new Uint8Array(PAGE_SIZE * 8);
      data.fill(fill);
      FS.write(stream, data, 0, data.length, 0);

      // Verify
      for (let p = 0; p < 8; p++) {
        const buf = new Uint8Array(PAGE_SIZE);
        FS.read(stream, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
        for (let i = 0; i < PAGE_SIZE; i++) {
          if (buf[i] !== fill) {
            throw new Error(
              `Cycle ${cycle} page ${p} byte ${i}: expected 0x${fill.toString(16)}, got 0x${buf[i].toString(16)}`,
            );
          }
        }
      }

      // Truncate to 1 page
      FS.ftruncate(stream.fd, PAGE_SIZE);
    }

    FS.close(stream);
  });

  // ------------------------------------------------------------------
  // Write spanning page boundaries during eviction
  // ------------------------------------------------------------------

  it("cross-page write that evicts own earlier pages", () => {
    const { FS } = h;
    const stream = FS.open("/boundary", O.RDWR | O.CREAT, 0o666);

    // Fill the file with 8 pages of known data
    for (let p = 0; p < 8; p++) {
      const data = new Uint8Array(PAGE_SIZE);
      data.fill(p & 0xff);
      FS.write(stream, data, 0, PAGE_SIZE, p * PAGE_SIZE);
    }

    // Now write a large buffer that spans pages 2-6 (5 pages).
    // With a 4-page cache, this must evict and re-fetch mid-write.
    const crossWrite = new Uint8Array(PAGE_SIZE * 5);
    crossWrite.fill(0xcc);
    FS.write(stream, crossWrite, 0, crossWrite.length, 2 * PAGE_SIZE);

    // Verify: pages 0-1 unchanged, pages 2-6 = 0xCC, page 7 unchanged
    for (let p = 0; p < 8; p++) {
      const buf = new Uint8Array(PAGE_SIZE);
      const n = FS.read(stream, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expect(n).toBe(PAGE_SIZE);

      let expected: number;
      if (p < 2) {
        expected = p & 0xff;
      } else if (p <= 6) {
        expected = 0xcc;
      } else {
        expected = p & 0xff;
      }

      for (let i = 0; i < PAGE_SIZE; i++) {
        if (buf[i] !== expected) {
          throw new Error(
            `Page ${p} byte ${i}: expected 0x${expected.toString(16)}, got 0x${buf[i].toString(16)}`,
          );
        }
      }
    }

    FS.close(stream);
  });

  it("unaligned cross-page write straddles page boundary correctly", () => {
    const { FS } = h;
    const stream = FS.open("/straddle", O.RDWR | O.CREAT, 0o666);

    // Write 6 pages of 0xAA
    const fill = new Uint8Array(PAGE_SIZE * 6);
    fill.fill(0xaa);
    FS.write(stream, fill, 0, fill.length, 0);

    // Write 100 bytes of 0xBB straddling the boundary between pages 2 and 3
    const offset = PAGE_SIZE * 3 - 50; // 50 bytes before page 3 boundary
    const straddle = new Uint8Array(100);
    straddle.fill(0xbb);
    FS.write(stream, straddle, 0, 100, offset);

    // Read the affected region and verify byte-by-byte
    const buf = new Uint8Array(200);
    FS.read(stream, buf, 0, 200, offset - 50);
    for (let i = 0; i < 200; i++) {
      const absPos = offset - 50 + i;
      const expected = absPos >= offset && absPos < offset + 100 ? 0xbb : 0xaa;
      if (buf[i] !== expected) {
        throw new Error(
          `Byte at file offset ${absPos}: expected 0x${expected.toString(16)}, got 0x${buf[i].toString(16)}`,
        );
      }
    }

    FS.close(stream);
  });

  // ------------------------------------------------------------------
  // Rename while reading: reader fd survives rename
  // ------------------------------------------------------------------

  it("rename file while reader has open fd: reads still work", () => {
    const { FS } = h;

    // Create multi-page file
    const w = FS.open("/before", O.RDWR | O.CREAT, 0o666);
    const data = new Uint8Array(PAGE_SIZE * 6);
    for (let i = 0; i < data.length; i++) data[i] = (i * 11 + 5) & 0xff;
    FS.write(w, data, 0, data.length, 0);
    FS.close(w);

    // Open reader
    const reader = FS.open("/before", O.RDONLY);

    // Read page 0 to confirm it works
    const buf0 = new Uint8Array(PAGE_SIZE);
    FS.read(reader, buf0, 0, PAGE_SIZE, 0);
    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(buf0[i]).toBe((i * 11 + 5) & 0xff);
    }

    // Rename the file
    FS.rename("/before", "/after");

    // Reader fd should still work — reads from all pages including evicted ones
    for (let p = 0; p < 6; p++) {
      const buf = new Uint8Array(PAGE_SIZE);
      const n = FS.read(reader, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expect(n).toBe(PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) {
        const expected = ((p * PAGE_SIZE + i) * 11 + 5) & 0xff;
        if (buf[i] !== expected) {
          throw new Error(
            `After rename, page ${p} byte ${i}: expected ${expected}, got ${buf[i]}`,
          );
        }
      }
    }

    FS.close(reader);
  });

  it("rename then write via old fd: data lands under new path", () => {
    const { FS } = h;

    const stream = FS.open("/orig", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("before-rename"), 0, 13, 0);

    FS.rename("/orig", "/moved");

    // Write more data through the open fd
    FS.write(stream, encode("after-rename!"), 0, 13, 13);
    FS.close(stream);

    // Read through the new path
    const reader = FS.open("/moved", O.RDONLY);
    const buf = new Uint8Array(26);
    const n = FS.read(reader, buf, 0, 26, 0);
    expect(n).toBe(26);
    expect(decode(buf, 26)).toBe("before-renameafter-rename!");
    FS.close(reader);
  });

  // ------------------------------------------------------------------
  // Mixed workload: simultaneous append + random write + truncate
  // ------------------------------------------------------------------

  it("append + random write + truncate cycle on same file", () => {
    const { FS } = h;

    // Simulates Postgres: WAL append + heap page write + VACUUM truncate
    const stream = FS.open("/mixed", O.RDWR | O.CREAT, 0o666);

    // Phase 1: build up a file with sequential writes (like initial load)
    for (let p = 0; p < 12; p++) {
      const data = new Uint8Array(PAGE_SIZE);
      data.fill(p & 0xff);
      FS.write(stream, data, 0, PAGE_SIZE, p * PAGE_SIZE);
    }

    // Phase 2: random overwrite of page 3 (like a heap page update)
    const update = new Uint8Array(PAGE_SIZE);
    update.fill(0xfe);
    FS.write(stream, update, 0, PAGE_SIZE, 3 * PAGE_SIZE);

    // Phase 3: truncate the tail (like VACUUM freeing empty pages at end)
    FS.ftruncate(stream.fd, 8 * PAGE_SIZE);

    // Phase 4: append new data (like WAL growth after VACUUM)
    const appender = FS.open("/mixed", O.WRONLY | O.APPEND);
    const walRecord = new Uint8Array(PAGE_SIZE);
    walRecord.fill(0xab);
    FS.write(appender, walRecord, 0, PAGE_SIZE);
    FS.close(appender);

    // Verify final state: 9 pages total
    expect(FS.stat("/mixed").size).toBe(9 * PAGE_SIZE);

    // Pages 0-2: original data
    for (let p = 0; p < 3; p++) {
      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(stream, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) {
        expect(buf[i]).toBe(p & 0xff);
      }
    }

    // Page 3: updated data
    const buf3 = new Uint8Array(PAGE_SIZE);
    FS.read(stream, buf3, 0, PAGE_SIZE, 3 * PAGE_SIZE);
    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(buf3[i]).toBe(0xfe);
    }

    // Pages 4-7: original data
    for (let p = 4; p < 8; p++) {
      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(stream, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) {
        expect(buf[i]).toBe(p & 0xff);
      }
    }

    // Page 8: appended WAL record
    const buf8 = new Uint8Array(PAGE_SIZE);
    FS.read(stream, buf8, 0, PAGE_SIZE, 8 * PAGE_SIZE);
    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(buf8[i]).toBe(0xab);
    }

    FS.close(stream);
  });

  // ------------------------------------------------------------------
  // Extend via write beyond EOF with gap pages: gap must be zeros
  // ------------------------------------------------------------------

  it("write beyond EOF creates zero-filled gap pages under pressure", () => {
    const { FS } = h;
    const stream = FS.open("/gap", O.RDWR | O.CREAT, 0o666);

    // Write page 0
    const page0 = new Uint8Array(PAGE_SIZE);
    page0.fill(0x11);
    FS.write(stream, page0, 0, PAGE_SIZE, 0);

    // Write page 7 (skipping pages 1-6: gap)
    const page7 = new Uint8Array(PAGE_SIZE);
    page7.fill(0x77);
    FS.write(stream, page7, 0, PAGE_SIZE, 7 * PAGE_SIZE);

    expect(FS.stat("/gap").size).toBe(8 * PAGE_SIZE);

    // Gap pages (1-6) must read as zeros
    for (let p = 1; p <= 6; p++) {
      const buf = new Uint8Array(PAGE_SIZE);
      const n = FS.read(stream, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expect(n).toBe(PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) {
        if (buf[i] !== 0) {
          throw new Error(
            `Gap page ${p} byte ${i}: expected 0x00, got 0x${buf[i].toString(16)}`,
          );
        }
      }
    }

    // Page 0 and 7 retain their data
    const buf0 = new Uint8Array(PAGE_SIZE);
    FS.read(stream, buf0, 0, PAGE_SIZE, 0);
    for (let i = 0; i < PAGE_SIZE; i++) expect(buf0[i]).toBe(0x11);

    const buf7 = new Uint8Array(PAGE_SIZE);
    FS.read(stream, buf7, 0, PAGE_SIZE, 7 * PAGE_SIZE);
    for (let i = 0; i < PAGE_SIZE; i++) expect(buf7[i]).toBe(0x77);

    FS.close(stream);
  });

  // ------------------------------------------------------------------
  // Two files trading cache slots: dirty eviction ordering
  // ------------------------------------------------------------------

  it("alternating writes to two files under 4-page cache", () => {
    const { FS } = h;
    const a = FS.open("/fileA", O.RDWR | O.CREAT, 0o666);
    const b = FS.open("/fileB", O.RDWR | O.CREAT, 0o666);

    // Write interleaved: A page 0, B page 0, A page 1, B page 1, ...
    // With a 4-page cache, this maximizes eviction churn.
    for (let p = 0; p < 8; p++) {
      const dataA = new Uint8Array(PAGE_SIZE);
      dataA.fill((0xa0 + p) & 0xff);
      FS.write(a, dataA, 0, PAGE_SIZE, p * PAGE_SIZE);

      const dataB = new Uint8Array(PAGE_SIZE);
      dataB.fill((0xb0 + p) & 0xff);
      FS.write(b, dataB, 0, PAGE_SIZE, p * PAGE_SIZE);
    }

    // Verify both files
    for (let p = 0; p < 8; p++) {
      const bufA = new Uint8Array(PAGE_SIZE);
      FS.read(a, bufA, 0, PAGE_SIZE, p * PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) {
        if (bufA[i] !== ((0xa0 + p) & 0xff)) {
          throw new Error(
            `fileA page ${p} byte ${i}: expected 0x${((0xa0 + p) & 0xff).toString(16)}, got 0x${bufA[i].toString(16)}`,
          );
        }
      }

      const bufB = new Uint8Array(PAGE_SIZE);
      FS.read(b, bufB, 0, PAGE_SIZE, p * PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) {
        if (bufB[i] !== ((0xb0 + p) & 0xff)) {
          throw new Error(
            `fileB page ${p} byte ${i}: expected 0x${((0xb0 + p) & 0xff).toString(16)}, got 0x${bufB[i].toString(16)}`,
          );
        }
      }
    }

    FS.close(a);
    FS.close(b);
  });

  // ------------------------------------------------------------------
  // ftruncate then immediate append via SEEK_END
  // ------------------------------------------------------------------

  it("ftruncate then seek to end and write: no stale data leaks", () => {
    const { FS } = h;
    const stream = FS.open("/truncseek", O.RDWR | O.CREAT, 0o666);

    // Fill 6 pages with 0xFF
    const fill = new Uint8Array(PAGE_SIZE * 6);
    fill.fill(0xff);
    FS.write(stream, fill, 0, fill.length, 0);

    // Truncate to 2 pages
    FS.ftruncate(stream.fd, PAGE_SIZE * 2);

    // Seek to end and write a new page
    FS.llseek(stream, 0, SEEK_END);
    const newData = new Uint8Array(PAGE_SIZE);
    newData.fill(0x33);
    FS.write(stream, newData, 0, PAGE_SIZE);

    // File should be 3 pages
    expect(FS.stat("/truncseek").size).toBe(PAGE_SIZE * 3);

    // Page 2 (just written) should be 0x33, not 0xFF from before truncate
    const buf = new Uint8Array(PAGE_SIZE);
    FS.read(stream, buf, 0, PAGE_SIZE, 2 * PAGE_SIZE);
    for (let i = 0; i < PAGE_SIZE; i++) {
      if (buf[i] !== 0x33) {
        throw new Error(
          `Page 2 byte ${i}: expected 0x33, got 0x${buf[i].toString(16)} (stale data leak)`,
        );
      }
    }

    FS.close(stream);
  });
});
