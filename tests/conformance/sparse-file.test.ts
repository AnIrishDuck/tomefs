/**
 * Conformance tests for sparse file semantics.
 *
 * Sparse files are created when writes skip over regions of a file (via
 * lseek past end + write, pwrite past end, ftruncate to extend, or
 * allocate/fallocate). The skipped regions should read as zeros.
 *
 * These tests exercise multi-page sparse patterns that are critical for
 * Postgres: fallocate pre-extends WAL segments and relation files, creating
 * files where metadata.size implies pages that were never written. The page
 * cache must serve zero-filled pages for gaps, and restoreTree must correctly
 * reconstruct sparse file sizes after syncfs → remount.
 *
 * Unlike tests/conformance/seek.test.ts (which tests small, single-page
 * sparse gaps) and tests/adversarial/allocate-persistence.test.ts (which
 * tests allocate-specific persistence), these tests verify POSIX sparse
 * file semantics at page granularity — the exact boundary where tomefs's
 * page-level storage diverges from MEMFS's contiguous array.
 */
import {
  createFS,
  encode,
  decode,
  O,
  SEEK_SET,
  SEEK_END,
  type FSHarness,
} from "../harness/emscripten-fs.js";

/** Page size matching tomefs internals and Postgres. */
const PAGE_SIZE = 8192;

/** Create a deterministic data pattern for verification. */
function pattern(size: number, seed: number): Uint8Array {
  const buf = new Uint8Array(size);
  for (let i = 0; i < size; i++) {
    buf[i] = ((seed + i * 31) & 0xff) || 1; // avoid zero to distinguish from gaps
  }
  return buf;
}

/** Verify that a region of a buffer is all zeros. */
function expectZeros(buf: Uint8Array, from: number, to: number, label?: string): void {
  for (let i = from; i < to; i++) {
    if (buf[i] !== 0) {
      throw new Error(
        `Expected zero at byte ${i}${label ? ` (${label})` : ""}, got ${buf[i]}`,
      );
    }
  }
}

/** Verify a pattern in a buffer region. */
function expectPattern(
  buf: Uint8Array,
  offset: number,
  expected: Uint8Array,
): void {
  for (let i = 0; i < expected.length; i++) {
    if (buf[offset + i] !== expected[i]) {
      throw new Error(
        `Pattern mismatch at byte ${offset + i}: expected ${expected[i]}, got ${buf[offset + i]}`,
      );
    }
  }
}

describe("sparse file (multi-page gaps)", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  // -------------------------------------------------------------------
  // Basic multi-page sparse creation
  // -------------------------------------------------------------------

  it("lseek past end by multiple pages creates zero-filled gap @fast", () => {
    const { FS } = h;
    const data = pattern(100, 42);

    const stream = FS.open("/sparse_seek", O.RDWR | O.CREAT, 0o666);
    // Write 100 bytes at offset 0 (within page 0)
    FS.write(stream, data, 0, data.length);
    // Seek to page 3 (24576 bytes from start) — skipping pages 1 and 2
    FS.llseek(stream, 3 * PAGE_SIZE, SEEK_SET);
    // Write another 100 bytes
    FS.write(stream, data, 0, data.length);
    FS.close(stream);

    // Verify file size
    const stat = FS.stat("/sparse_seek");
    expect(stat.size).toBe(3 * PAGE_SIZE + 100);

    // Read entire file and verify
    const stream2 = FS.open("/sparse_seek", O.RDONLY);
    const buf = new Uint8Array(stat.size);
    const n = FS.read(stream2, buf, 0, buf.length);
    expect(n).toBe(stat.size);

    // First 100 bytes: pattern data
    expectPattern(buf, 0, data);
    // Bytes 100 through 3*PAGE_SIZE-1: zeros (the gap)
    expectZeros(buf, 100, 3 * PAGE_SIZE, "gap between page 0 and page 3");
    // Last 100 bytes: pattern data
    expectPattern(buf, 3 * PAGE_SIZE, data);

    FS.close(stream2);
  });

  it("pwrite past end creates sparse gap spanning pages @fast", () => {
    const { FS } = h;
    const data = pattern(200, 77);

    const stream = FS.open("/sparse_pwrite", O.RDWR | O.CREAT, 0o666);
    // Write 200 bytes at start
    FS.write(stream, data, 0, data.length);
    // pwrite at offset 5*PAGE_SIZE (skipping pages 1-4)
    FS.write(stream, data, 0, data.length, 5 * PAGE_SIZE);
    FS.close(stream);

    const stat = FS.stat("/sparse_pwrite");
    expect(stat.size).toBe(5 * PAGE_SIZE + 200);

    // Read and verify gap
    const stream2 = FS.open("/sparse_pwrite", O.RDONLY);
    const buf = new Uint8Array(stat.size);
    FS.read(stream2, buf, 0, buf.length);

    expectPattern(buf, 0, data);
    expectZeros(buf, 200, 5 * PAGE_SIZE, "gap pages 1-4");
    expectPattern(buf, 5 * PAGE_SIZE, data);

    FS.close(stream2);
  });

  it("ftruncate extend creates multi-page sparse region @fast", () => {
    const { FS } = h;
    const data = pattern(PAGE_SIZE, 11);

    const stream = FS.open("/sparse_ftrunc", O.RDWR | O.CREAT, 0o666);
    // Write one full page
    FS.write(stream, data, 0, data.length);
    // Extend to 8 pages via ftruncate (pages 1-7 are sparse)
    FS.ftruncate(stream.fd, 8 * PAGE_SIZE);
    FS.close(stream);

    const stat = FS.stat("/sparse_ftrunc");
    expect(stat.size).toBe(8 * PAGE_SIZE);

    // Read the gap region (pages 1-7) — should be all zeros
    const stream2 = FS.open("/sparse_ftrunc", O.RDONLY);
    FS.llseek(stream2, PAGE_SIZE, SEEK_SET);
    const gap = new Uint8Array(7 * PAGE_SIZE);
    const n = FS.read(stream2, gap, 0, gap.length);
    expect(n).toBe(7 * PAGE_SIZE);
    expectZeros(gap, 0, gap.length, "ftruncate extended region");

    // Verify first page is intact
    FS.llseek(stream2, 0, SEEK_SET);
    const page0 = new Uint8Array(PAGE_SIZE);
    FS.read(stream2, page0, 0, PAGE_SIZE);
    expectPattern(page0, 0, data);

    FS.close(stream2);
  });

  // -------------------------------------------------------------------
  // Page-aligned sparse patterns (write page 0, skip, write page N)
  // -------------------------------------------------------------------

  it("write page 0 and page 4, pages 1-3 are zero @fast", () => {
    const { FS } = h;
    const page0Data = pattern(PAGE_SIZE, 1);
    const page4Data = pattern(PAGE_SIZE, 2);

    const stream = FS.open("/sparse_pages", O.RDWR | O.CREAT, 0o666);
    // Write full page 0
    FS.write(stream, page0Data, 0, PAGE_SIZE);
    // Seek to page 4 and write
    FS.llseek(stream, 4 * PAGE_SIZE, SEEK_SET);
    FS.write(stream, page4Data, 0, PAGE_SIZE);
    FS.close(stream);

    expect(FS.stat("/sparse_pages").size).toBe(5 * PAGE_SIZE);

    // Read pages 1, 2, 3 individually — all should be zeros
    const stream2 = FS.open("/sparse_pages", O.RDONLY);
    const pageBuf = new Uint8Array(PAGE_SIZE);

    for (let p = 1; p <= 3; p++) {
      FS.llseek(stream2, p * PAGE_SIZE, SEEK_SET);
      const n = FS.read(stream2, pageBuf, 0, PAGE_SIZE);
      expect(n).toBe(PAGE_SIZE);
      expectZeros(pageBuf, 0, PAGE_SIZE, `sparse page ${p}`);
    }

    // Verify page 0 and page 4 data
    FS.llseek(stream2, 0, SEEK_SET);
    FS.read(stream2, pageBuf, 0, PAGE_SIZE);
    expectPattern(pageBuf, 0, page0Data);

    FS.llseek(stream2, 4 * PAGE_SIZE, SEEK_SET);
    FS.read(stream2, pageBuf, 0, PAGE_SIZE);
    expectPattern(pageBuf, 0, page4Data);

    FS.close(stream2);
  });

  it("multiple non-contiguous page writes create correct sparse layout", () => {
    const { FS } = h;
    // Write pages 0, 3, 7 — leaving gaps at 1-2, 4-6
    const pages = [0, 3, 7];
    const data: Uint8Array[] = pages.map((_, i) => pattern(PAGE_SIZE, 10 + i));

    const stream = FS.open("/sparse_multi", O.RDWR | O.CREAT, 0o666);
    for (let i = 0; i < pages.length; i++) {
      FS.llseek(stream, pages[i] * PAGE_SIZE, SEEK_SET);
      FS.write(stream, data[i], 0, PAGE_SIZE);
    }
    FS.close(stream);

    expect(FS.stat("/sparse_multi").size).toBe(8 * PAGE_SIZE);

    // Verify all pages
    const stream2 = FS.open("/sparse_multi", O.RDONLY);
    const pageBuf = new Uint8Array(PAGE_SIZE);
    const writtenPages = new Set(pages);

    for (let p = 0; p < 8; p++) {
      FS.llseek(stream2, p * PAGE_SIZE, SEEK_SET);
      FS.read(stream2, pageBuf, 0, PAGE_SIZE);

      if (writtenPages.has(p)) {
        const idx = pages.indexOf(p);
        expectPattern(pageBuf, 0, data[idx]);
      } else {
        expectZeros(pageBuf, 0, PAGE_SIZE, `gap page ${p}`);
      }
    }
    FS.close(stream2);
  });

  // -------------------------------------------------------------------
  // Cross-page boundary reads through sparse regions
  // -------------------------------------------------------------------

  it("read spanning written page → sparse page returns correct data", () => {
    const { FS } = h;
    const data = pattern(PAGE_SIZE, 55);

    const stream = FS.open("/sparse_cross", O.RDWR | O.CREAT, 0o666);
    // Write page 0 only, but make file 2 pages long
    FS.write(stream, data, 0, PAGE_SIZE);
    FS.ftruncate(stream.fd, 2 * PAGE_SIZE);
    FS.close(stream);

    // Read 256 bytes spanning the page 0 → page 1 boundary
    const stream2 = FS.open("/sparse_cross", O.RDONLY);
    FS.llseek(stream2, PAGE_SIZE - 128, SEEK_SET);
    const buf = new Uint8Array(256);
    const n = FS.read(stream2, buf, 0, 256);
    expect(n).toBe(256);

    // First 128 bytes: tail of page 0 (pattern data)
    const expectedTail = data.subarray(PAGE_SIZE - 128, PAGE_SIZE);
    expectPattern(buf, 0, expectedTail);
    // Last 128 bytes: head of page 1 (zeros — sparse)
    expectZeros(buf, 128, 256, "cross-boundary into sparse page");

    FS.close(stream2);
  });

  it("read spanning sparse page → written page returns correct data", () => {
    const { FS } = h;
    const page1Data = pattern(PAGE_SIZE, 66);

    const stream = FS.open("/sparse_cross2", O.RDWR | O.CREAT, 0o666);
    // Create 2-page file: page 0 is sparse, page 1 has data
    FS.ftruncate(stream.fd, 2 * PAGE_SIZE);
    FS.llseek(stream, PAGE_SIZE, SEEK_SET);
    FS.write(stream, page1Data, 0, PAGE_SIZE);
    FS.close(stream);

    // Read 256 bytes spanning page 0 → page 1 boundary
    const stream2 = FS.open("/sparse_cross2", O.RDONLY);
    FS.llseek(stream2, PAGE_SIZE - 128, SEEK_SET);
    const buf = new Uint8Array(256);
    const n = FS.read(stream2, buf, 0, 256);
    expect(n).toBe(256);

    // First 128 bytes: tail of page 0 (zeros — sparse)
    expectZeros(buf, 0, 128, "tail of sparse page 0");
    // Last 128 bytes: head of page 1 (pattern data)
    expectPattern(buf, 128, page1Data.subarray(0, 128));

    FS.close(stream2);
  });

  // -------------------------------------------------------------------
  // Overwrite sparse regions
  // -------------------------------------------------------------------

  it("writing into a sparse gap fills it correctly @fast", () => {
    const { FS } = h;
    const page0 = pattern(PAGE_SIZE, 1);
    const page4 = pattern(PAGE_SIZE, 2);
    const fill = pattern(PAGE_SIZE, 99);

    const stream = FS.open("/sparse_fill", O.RDWR | O.CREAT, 0o666);
    // Create sparse file: page 0 and page 4 written
    FS.write(stream, page0, 0, PAGE_SIZE);
    FS.llseek(stream, 4 * PAGE_SIZE, SEEK_SET);
    FS.write(stream, page4, 0, PAGE_SIZE);

    // Now fill page 2 (in the gap)
    FS.llseek(stream, 2 * PAGE_SIZE, SEEK_SET);
    FS.write(stream, fill, 0, PAGE_SIZE);
    FS.close(stream);

    // Verify: page 1 and 3 still zero, page 2 has fill data
    const stream2 = FS.open("/sparse_fill", O.RDONLY);
    const buf = new Uint8Array(PAGE_SIZE);

    // Page 1: still sparse (zero)
    FS.llseek(stream2, PAGE_SIZE, SEEK_SET);
    FS.read(stream2, buf, 0, PAGE_SIZE);
    expectZeros(buf, 0, PAGE_SIZE, "page 1 still sparse");

    // Page 2: filled
    FS.llseek(stream2, 2 * PAGE_SIZE, SEEK_SET);
    FS.read(stream2, buf, 0, PAGE_SIZE);
    expectPattern(buf, 0, fill);

    // Page 3: still sparse (zero)
    FS.llseek(stream2, 3 * PAGE_SIZE, SEEK_SET);
    FS.read(stream2, buf, 0, PAGE_SIZE);
    expectZeros(buf, 0, PAGE_SIZE, "page 3 still sparse");

    FS.close(stream2);
  });

  // -------------------------------------------------------------------
  // Truncate interactions with sparse files
  // -------------------------------------------------------------------

  it("truncate shrink then extend preserves sparse zeros", () => {
    const { FS } = h;
    const data = pattern(100, 33);

    const stream = FS.open("/sparse_trunc_ext", O.RDWR | O.CREAT, 0o666);
    // Write at start and at page 2
    FS.write(stream, data, 0, data.length);
    FS.llseek(stream, 2 * PAGE_SIZE, SEEK_SET);
    FS.write(stream, data, 0, data.length);
    FS.close(stream);

    // Truncate to 1 page (discard page 2 data)
    FS.truncate("/sparse_trunc_ext", PAGE_SIZE);
    expect(FS.stat("/sparse_trunc_ext").size).toBe(PAGE_SIZE);

    // Extend back to 3 pages
    FS.truncate("/sparse_trunc_ext", 3 * PAGE_SIZE);
    expect(FS.stat("/sparse_trunc_ext").size).toBe(3 * PAGE_SIZE);

    // Pages 1 and 2 should be zeros (page 2 data was truncated away)
    const stream2 = FS.open("/sparse_trunc_ext", O.RDONLY);
    FS.llseek(stream2, PAGE_SIZE, SEEK_SET);
    const gap = new Uint8Array(2 * PAGE_SIZE);
    const n = FS.read(stream2, gap, 0, gap.length);
    expect(n).toBe(2 * PAGE_SIZE);
    expectZeros(gap, 0, gap.length, "truncated-then-extended region");

    // Page 0 data intact
    FS.llseek(stream2, 0, SEEK_SET);
    const page0 = new Uint8Array(100);
    FS.read(stream2, page0, 0, 100);
    expectPattern(page0, 0, data);

    FS.close(stream2);
  });

  it("truncate to mid-page zeroes tail, extending preserves zeros @fast", () => {
    const { FS } = h;
    const data = pattern(PAGE_SIZE, 44);

    // Write a full page
    const stream = FS.open("/sparse_midtrunc", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, data, 0, PAGE_SIZE);
    FS.close(stream);

    // Truncate to half a page
    FS.truncate("/sparse_midtrunc", PAGE_SIZE / 2);
    expect(FS.stat("/sparse_midtrunc").size).toBe(PAGE_SIZE / 2);

    // Extend to 2 pages
    FS.truncate("/sparse_midtrunc", 2 * PAGE_SIZE);
    expect(FS.stat("/sparse_midtrunc").size).toBe(2 * PAGE_SIZE);

    // Read entire file
    const stream2 = FS.open("/sparse_midtrunc", O.RDONLY);
    const buf = new Uint8Array(2 * PAGE_SIZE);
    FS.read(stream2, buf, 0, buf.length);

    // First half page: original data
    expectPattern(buf, 0, data.subarray(0, PAGE_SIZE / 2));
    // Second half of page 0 through end: zeros
    expectZeros(buf, PAGE_SIZE / 2, 2 * PAGE_SIZE, "truncated-then-extended tail");

    FS.close(stream2);
  });

  // -------------------------------------------------------------------
  // Large sparse files
  // -------------------------------------------------------------------

  it("sparse file with 10-page gap reads correctly", () => {
    const { FS } = h;
    const header = pattern(256, 1);
    const trailer = pattern(256, 2);

    const stream = FS.open("/sparse_large", O.RDWR | O.CREAT, 0o666);
    // Write header at start
    FS.write(stream, header, 0, header.length);
    // Write trailer at page 10 (80KB offset) — 9-page gap
    FS.llseek(stream, 10 * PAGE_SIZE, SEEK_SET);
    FS.write(stream, trailer, 0, trailer.length);
    FS.close(stream);

    const size = FS.stat("/sparse_large").size;
    expect(size).toBe(10 * PAGE_SIZE + 256);

    // Spot-check middle of gap (page 5)
    const stream2 = FS.open("/sparse_large", O.RDONLY);
    FS.llseek(stream2, 5 * PAGE_SIZE, SEEK_SET);
    const mid = new Uint8Array(PAGE_SIZE);
    const n = FS.read(stream2, mid, 0, PAGE_SIZE);
    expect(n).toBe(PAGE_SIZE);
    expectZeros(mid, 0, PAGE_SIZE, "middle of 10-page gap");

    // Verify trailer
    FS.llseek(stream2, 10 * PAGE_SIZE, SEEK_SET);
    const tail = new Uint8Array(256);
    FS.read(stream2, tail, 0, 256);
    expectPattern(tail, 0, trailer);

    FS.close(stream2);
  });

  // -------------------------------------------------------------------
  // Sub-page sparse patterns at page boundaries
  // -------------------------------------------------------------------

  it("write at end of page 0 and start of page 2 leaves page 1 zero", () => {
    const { FS } = h;
    const tail = pattern(64, 10);
    const head = pattern(64, 20);

    const stream = FS.open("/sparse_subpage", O.RDWR | O.CREAT, 0o666);
    // Write 64 bytes at end of page 0
    FS.llseek(stream, PAGE_SIZE - 64, SEEK_SET);
    FS.write(stream, tail, 0, 64);
    // Write 64 bytes at start of page 2
    FS.llseek(stream, 2 * PAGE_SIZE, SEEK_SET);
    FS.write(stream, head, 0, 64);
    FS.close(stream);

    expect(FS.stat("/sparse_subpage").size).toBe(2 * PAGE_SIZE + 64);

    // Entire page 1 should be zeros
    const stream2 = FS.open("/sparse_subpage", O.RDONLY);
    FS.llseek(stream2, PAGE_SIZE, SEEK_SET);
    const page1 = new Uint8Array(PAGE_SIZE);
    const n = FS.read(stream2, page1, 0, PAGE_SIZE);
    expect(n).toBe(PAGE_SIZE);
    expectZeros(page1, 0, PAGE_SIZE, "page 1 between sub-page writes");

    // Also verify bytes 0 through PAGE_SIZE-65 are zeros (start of page 0)
    FS.llseek(stream2, 0, SEEK_SET);
    const page0Start = new Uint8Array(PAGE_SIZE - 64);
    FS.read(stream2, page0Start, 0, page0Start.length);
    expectZeros(page0Start, 0, page0Start.length, "start of page 0 before write");

    FS.close(stream2);
  });

  // -------------------------------------------------------------------
  // Stat correctness for sparse files
  // -------------------------------------------------------------------

  it("stat reports correct size for sparse files after various operations @fast", () => {
    const { FS } = h;
    const data = pattern(100, 1);

    const stream = FS.open("/sparse_stat", O.RDWR | O.CREAT, 0o666);

    // Empty file
    expect(FS.fstat(stream.fd).size).toBe(0);

    // Write 100 bytes
    FS.write(stream, data, 0, 100);
    expect(FS.fstat(stream.fd).size).toBe(100);

    // Seek to page 5 and write — creates 4-page gap
    FS.llseek(stream, 5 * PAGE_SIZE, SEEK_SET);
    FS.write(stream, data, 0, 100);
    expect(FS.fstat(stream.fd).size).toBe(5 * PAGE_SIZE + 100);

    // ftruncate to shrink back to 2 pages
    FS.ftruncate(stream.fd, 2 * PAGE_SIZE);
    expect(FS.fstat(stream.fd).size).toBe(2 * PAGE_SIZE);

    // ftruncate to extend to 10 pages
    FS.ftruncate(stream.fd, 10 * PAGE_SIZE);
    expect(FS.fstat(stream.fd).size).toBe(10 * PAGE_SIZE);

    FS.close(stream);
    // stat by path
    expect(FS.stat("/sparse_stat").size).toBe(10 * PAGE_SIZE);
  });

  // -------------------------------------------------------------------
  // Sparse file rename preserves sparsity
  // -------------------------------------------------------------------

  it("rename preserves sparse file content", () => {
    const { FS } = h;
    const data = pattern(PAGE_SIZE, 88);

    const stream = FS.open("/sparse_rename_src", O.RDWR | O.CREAT, 0o666);
    // Page 0 written, pages 1-2 sparse, page 3 written
    FS.write(stream, data, 0, PAGE_SIZE);
    FS.llseek(stream, 3 * PAGE_SIZE, SEEK_SET);
    FS.write(stream, data, 0, PAGE_SIZE);
    FS.close(stream);

    FS.rename("/sparse_rename_src", "/sparse_rename_dst");

    const stat = FS.stat("/sparse_rename_dst");
    expect(stat.size).toBe(4 * PAGE_SIZE);

    // Verify gaps are still zero
    const stream2 = FS.open("/sparse_rename_dst", O.RDONLY);
    const page1 = new Uint8Array(PAGE_SIZE);
    FS.llseek(stream2, PAGE_SIZE, SEEK_SET);
    FS.read(stream2, page1, 0, PAGE_SIZE);
    expectZeros(page1, 0, PAGE_SIZE, "page 1 after rename");

    const page2 = new Uint8Array(PAGE_SIZE);
    FS.llseek(stream2, 2 * PAGE_SIZE, SEEK_SET);
    FS.read(stream2, page2, 0, PAGE_SIZE);
    expectZeros(page2, 0, PAGE_SIZE, "page 2 after rename");

    // Written pages still correct
    FS.llseek(stream2, 0, SEEK_SET);
    const p0 = new Uint8Array(PAGE_SIZE);
    FS.read(stream2, p0, 0, PAGE_SIZE);
    expectPattern(p0, 0, data);

    FS.llseek(stream2, 3 * PAGE_SIZE, SEEK_SET);
    const p3 = new Uint8Array(PAGE_SIZE);
    FS.read(stream2, p3, 0, PAGE_SIZE);
    expectPattern(p3, 0, data);

    FS.close(stream2);
  });

  // -------------------------------------------------------------------
  // Concurrent access to sparse files
  // -------------------------------------------------------------------

  it("two fds on same sparse file see consistent zeros in gaps", () => {
    const { FS } = h;
    const data = pattern(PAGE_SIZE, 77);

    // Create sparse file
    const w = FS.open("/sparse_twofd", O.RDWR | O.CREAT, 0o666);
    FS.write(w, data, 0, PAGE_SIZE);
    FS.ftruncate(w.fd, 4 * PAGE_SIZE);
    FS.close(w);

    // Open two readers
    const r1 = FS.open("/sparse_twofd", O.RDONLY);
    const r2 = FS.open("/sparse_twofd", O.RDONLY);

    const buf1 = new Uint8Array(PAGE_SIZE);
    const buf2 = new Uint8Array(PAGE_SIZE);

    // Both should see page 2 as zeros
    FS.llseek(r1, 2 * PAGE_SIZE, SEEK_SET);
    FS.read(r1, buf1, 0, PAGE_SIZE);
    FS.llseek(r2, 2 * PAGE_SIZE, SEEK_SET);
    FS.read(r2, buf2, 0, PAGE_SIZE);

    expectZeros(buf1, 0, PAGE_SIZE, "fd1 sparse page");
    expectZeros(buf2, 0, PAGE_SIZE, "fd2 sparse page");

    FS.close(r1);
    FS.close(r2);
  });
});
