/**
 * Conformance tests for positioned I/O (pread/pwrite semantics).
 *
 * Postgres uses positioned I/O extensively: the buffer manager reads and
 * writes individual 8 KB pages at specific file offsets without seeking.
 * The WAL writer appends records at known positions. The checkpointer
 * writes dirty pages back to their exact file offsets.
 *
 * In Emscripten's FS API, positioned I/O is done via the optional `position`
 * parameter to FS.read() and FS.write():
 *   FS.read(stream, buf, 0, length, position)   // pread
 *   FS.write(stream, buf, 0, length, position)   // pwrite
 *
 * These tests verify that:
 *   - Positioned reads return correct data without moving stream position
 *   - Positioned writes store data at the correct offset without moving stream position
 *   - Reads and writes at page boundaries work correctly (tomefs page seam)
 *   - Multiple positioned operations on the same file are consistent
 *   - Two file descriptors on the same file see each other's positioned writes
 *   - Positioned I/O interacts correctly with sequential I/O
 *
 * Runs against both MEMFS (reference) and tomefs (via TOMEFS_BACKEND=tomefs).
 */
import { createFS, O, SEEK_SET, SEEK_CUR, type FSHarness } from "../harness/emscripten-fs.js";

const PAGE_SIZE = 8192;

function pattern(size: number, seed: number): Uint8Array {
  const buf = new Uint8Array(size);
  for (let i = 0; i < size; i++) {
    buf[i] = ((seed + i * 31) & 0xff) || 1;
  }
  return buf;
}

describe("positioned I/O (pread/pwrite)", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  // -------------------------------------------------------------------
  // Basic positioned read
  // -------------------------------------------------------------------

  it("positioned read returns data at specified offset @fast", () => {
    const { FS } = h;
    const data = pattern(PAGE_SIZE * 3, 42);
    const stream = FS.open("/pread_basic", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, data, 0, data.length);

    // Read page 1 (middle) using positioned read
    const buf = new Uint8Array(PAGE_SIZE);
    const n = FS.read(stream, buf, 0, PAGE_SIZE, PAGE_SIZE);
    expect(n).toBe(PAGE_SIZE);
    expect(buf).toEqual(data.subarray(PAGE_SIZE, 2 * PAGE_SIZE));

    FS.close(stream);
  });

  it("positioned read does not move stream position @fast", () => {
    const { FS } = h;
    const stream = FS.open("/pread_pos", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, pattern(PAGE_SIZE * 2, 10), 0, PAGE_SIZE * 2);

    // Set position to 100
    FS.llseek(stream, 100, SEEK_SET);

    // Positioned read at offset PAGE_SIZE
    const buf = new Uint8Array(256);
    FS.read(stream, buf, 0, 256, PAGE_SIZE);

    // Stream position should still be 100
    const pos = FS.llseek(stream, 0, SEEK_CUR);
    expect(pos).toBe(100);

    FS.close(stream);
  });

  it("positioned read at offset 0 returns file start @fast", () => {
    const { FS } = h;
    const data = pattern(500, 77);
    const stream = FS.open("/pread_zero", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, data, 0, data.length);

    // Move position away from 0
    FS.llseek(stream, 250, SEEK_SET);

    // Positioned read at 0
    const buf = new Uint8Array(100);
    FS.read(stream, buf, 0, 100, 0);
    expect(buf).toEqual(data.subarray(0, 100));

    FS.close(stream);
  });

  it("positioned read past end of file returns 0 bytes @fast", () => {
    const { FS } = h;
    const stream = FS.open("/pread_eof", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, pattern(100, 1), 0, 100);

    const buf = new Uint8Array(50);
    buf.fill(0xff);
    const n = FS.read(stream, buf, 0, 50, 200);
    expect(n).toBe(0);

    FS.close(stream);
  });

  it("positioned read at file size boundary returns 0 bytes", () => {
    const { FS } = h;
    const stream = FS.open("/pread_exact_eof", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, pattern(PAGE_SIZE, 1), 0, PAGE_SIZE);

    const buf = new Uint8Array(10);
    const n = FS.read(stream, buf, 0, 10, PAGE_SIZE);
    expect(n).toBe(0);

    FS.close(stream);
  });

  it("positioned read that extends past EOF is truncated", () => {
    const { FS } = h;
    const data = pattern(100, 33);
    const stream = FS.open("/pread_trunc", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, data, 0, data.length);

    // Read 200 bytes starting at offset 50 — only 50 bytes available
    const buf = new Uint8Array(200);
    const n = FS.read(stream, buf, 0, 200, 50);
    expect(n).toBe(50);
    expect(buf.subarray(0, 50)).toEqual(data.subarray(50, 100));

    FS.close(stream);
  });

  // -------------------------------------------------------------------
  // Basic positioned write
  // -------------------------------------------------------------------

  it("positioned write stores data at specified offset @fast", () => {
    const { FS } = h;
    const stream = FS.open("/pwrite_basic", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, new Uint8Array(PAGE_SIZE * 2), 0, PAGE_SIZE * 2);

    // Write pattern at page 1
    const data = pattern(PAGE_SIZE, 55);
    FS.write(stream, data, 0, PAGE_SIZE, PAGE_SIZE);

    // Read back at same position
    const buf = new Uint8Array(PAGE_SIZE);
    FS.read(stream, buf, 0, PAGE_SIZE, PAGE_SIZE);
    expect(buf).toEqual(data);

    // Page 0 should still be zeros
    const page0 = new Uint8Array(PAGE_SIZE);
    FS.read(stream, page0, 0, PAGE_SIZE, 0);
    for (let i = 0; i < PAGE_SIZE; i++) expect(page0[i]).toBe(0);

    FS.close(stream);
  });

  it("positioned write does not move stream position @fast", () => {
    const { FS } = h;
    const stream = FS.open("/pwrite_pos", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, new Uint8Array(PAGE_SIZE), 0, PAGE_SIZE);

    // Set position to 200
    FS.llseek(stream, 200, SEEK_SET);

    // Positioned write at offset 5000
    FS.write(stream, pattern(100, 9), 0, 100, 5000);

    // Stream position should still be 200
    const pos = FS.llseek(stream, 0, SEEK_CUR);
    expect(pos).toBe(200);

    FS.close(stream);
  });

  it("positioned write beyond file end extends the file @fast", () => {
    const { FS } = h;
    const stream = FS.open("/pwrite_extend", O.RDWR | O.CREAT, 0o666);

    // File starts empty. Positioned write at offset 1000
    const data = pattern(100, 22);
    FS.write(stream, data, 0, data.length, 1000);

    expect(FS.fstat(stream.fd).size).toBe(1100);

    // Bytes 0-999 should be zeros (gap)
    const gap = new Uint8Array(1000);
    FS.read(stream, gap, 0, 1000, 0);
    for (let i = 0; i < 1000; i++) expect(gap[i]).toBe(0);

    // Bytes 1000-1099 should be the written data
    const buf = new Uint8Array(100);
    FS.read(stream, buf, 0, 100, 1000);
    expect(buf).toEqual(data);

    FS.close(stream);
  });

  // -------------------------------------------------------------------
  // Page boundary operations (tomefs page cache seam)
  // -------------------------------------------------------------------

  it("positioned read spanning page boundary returns correct data @fast", () => {
    const { FS } = h;
    const fullData = pattern(PAGE_SIZE * 2, 44);
    const stream = FS.open("/pread_boundary", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, fullData, 0, fullData.length);

    // Read 256 bytes straddling the page boundary
    const buf = new Uint8Array(256);
    const n = FS.read(stream, buf, 0, 256, PAGE_SIZE - 128);
    expect(n).toBe(256);
    expect(buf).toEqual(fullData.subarray(PAGE_SIZE - 128, PAGE_SIZE + 128));

    FS.close(stream);
  });

  it("positioned write spanning page boundary stores correctly", () => {
    const { FS } = h;
    const stream = FS.open("/pwrite_boundary", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, new Uint8Array(PAGE_SIZE * 2), 0, PAGE_SIZE * 2);

    // Write 512 bytes centered on the page boundary
    const data = pattern(512, 66);
    FS.write(stream, data, 0, 512, PAGE_SIZE - 256);

    // Read back
    const buf = new Uint8Array(512);
    FS.read(stream, buf, 0, 512, PAGE_SIZE - 256);
    expect(buf).toEqual(data);

    FS.close(stream);
  });

  it("positioned write spanning 3 pages @fast", () => {
    const { FS } = h;
    const stream = FS.open("/pwrite_3page", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, new Uint8Array(PAGE_SIZE * 4), 0, PAGE_SIZE * 4);

    // Write data that spans pages 0, 1, and 2
    const size = PAGE_SIZE * 2 + 100;
    const data = pattern(size, 88);
    FS.write(stream, data, 0, size, PAGE_SIZE - 50);

    // Read back
    const buf = new Uint8Array(size);
    FS.read(stream, buf, 0, size, PAGE_SIZE - 50);
    expect(buf).toEqual(data);

    FS.close(stream);
  });

  // -------------------------------------------------------------------
  // Interleaved positioned and sequential I/O
  // -------------------------------------------------------------------

  it("sequential read after positioned write sees written data", () => {
    const { FS } = h;
    const stream = FS.open("/seq_after_pwrite", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, new Uint8Array(PAGE_SIZE), 0, PAGE_SIZE);

    // Positioned write at offset 100
    const data = pattern(50, 11);
    FS.write(stream, data, 0, 50, 100);

    // Sequential read from offset 100
    FS.llseek(stream, 100, SEEK_SET);
    const buf = new Uint8Array(50);
    FS.read(stream, buf, 0, 50);
    expect(buf).toEqual(data);

    FS.close(stream);
  });

  it("positioned read after sequential write sees written data", () => {
    const { FS } = h;
    const stream = FS.open("/pread_after_seq", O.RDWR | O.CREAT, 0o666);

    // Sequential write starting at 0
    const data = pattern(PAGE_SIZE, 22);
    FS.write(stream, data, 0, PAGE_SIZE);

    // Positioned read at offset 500
    const buf = new Uint8Array(100);
    FS.read(stream, buf, 0, 100, 500);
    expect(buf).toEqual(data.subarray(500, 600));

    FS.close(stream);
  });

  it("alternating positioned and sequential writes produce correct file", () => {
    const { FS } = h;
    const stream = FS.open("/alt_writes", O.RDWR | O.CREAT, 0o666);

    // Sequential write: 100 bytes at position 0 (stream pos → 100)
    const d1 = pattern(100, 1);
    FS.write(stream, d1, 0, 100);

    // Positioned write: 100 bytes at position 500
    const d2 = pattern(100, 2);
    FS.write(stream, d2, 0, 100, 500);

    // Sequential write: 50 bytes at position 100 (continues from first write)
    const d3 = pattern(50, 3);
    FS.write(stream, d3, 0, 50);

    // Verify file
    expect(FS.fstat(stream.fd).size).toBe(600);

    const buf = new Uint8Array(600);
    FS.read(stream, buf, 0, 600, 0);

    // 0-99: d1
    expect(buf.subarray(0, 100)).toEqual(d1);
    // 100-149: d3
    expect(buf.subarray(100, 150)).toEqual(d3);
    // 150-499: zeros
    for (let i = 150; i < 500; i++) expect(buf[i]).toBe(0);
    // 500-599: d2
    expect(buf.subarray(500, 600)).toEqual(d2);

    FS.close(stream);
  });

  // -------------------------------------------------------------------
  // Multi-fd positioned I/O (Postgres pattern: WAL writer + checkpointer)
  // -------------------------------------------------------------------

  it("positioned write on fd1 is visible to positioned read on fd2 @fast", () => {
    const { FS } = h;
    const stream1 = FS.open("/multi_fd", O.RDWR | O.CREAT, 0o666);
    FS.write(stream1, new Uint8Array(PAGE_SIZE * 2), 0, PAGE_SIZE * 2);

    const stream2 = FS.open("/multi_fd", O.RDONLY);

    // Write via fd1 at page 1
    const data = pattern(PAGE_SIZE, 77);
    FS.write(stream1, data, 0, PAGE_SIZE, PAGE_SIZE);

    // Read via fd2 at same position
    const buf = new Uint8Array(PAGE_SIZE);
    const n = FS.read(stream2, buf, 0, PAGE_SIZE, PAGE_SIZE);
    expect(n).toBe(PAGE_SIZE);
    expect(buf).toEqual(data);

    FS.close(stream1);
    FS.close(stream2);
  });

  it("two fds writing to different pages of same file @fast", () => {
    const { FS } = h;
    const stream1 = FS.open("/two_writers", O.RDWR | O.CREAT, 0o666);
    FS.write(stream1, new Uint8Array(PAGE_SIZE * 4), 0, PAGE_SIZE * 4);
    const stream2 = FS.open("/two_writers", O.RDWR);

    // fd1 writes to pages 0 and 2
    const d0 = pattern(PAGE_SIZE, 10);
    const d2 = pattern(PAGE_SIZE, 20);
    FS.write(stream1, d0, 0, PAGE_SIZE, 0);
    FS.write(stream1, d2, 0, PAGE_SIZE, 2 * PAGE_SIZE);

    // fd2 writes to pages 1 and 3
    const d1 = pattern(PAGE_SIZE, 30);
    const d3 = pattern(PAGE_SIZE, 40);
    FS.write(stream2, d1, 0, PAGE_SIZE, PAGE_SIZE);
    FS.write(stream2, d3, 0, PAGE_SIZE, 3 * PAGE_SIZE);

    // Read entire file via a third fd and verify all pages
    const stream3 = FS.open("/two_writers", O.RDONLY);
    const expected = [d0, d1, d2, d3];
    for (let p = 0; p < 4; p++) {
      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(stream3, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expect(buf).toEqual(expected[p]);
    }

    FS.close(stream1);
    FS.close(stream2);
    FS.close(stream3);
  });

  it("fd positions are independent for same file", () => {
    const { FS } = h;
    const data = pattern(PAGE_SIZE, 55);
    const s1 = FS.open("/indep_pos", O.RDWR | O.CREAT, 0o666);
    FS.write(s1, data, 0, PAGE_SIZE);

    const s2 = FS.open("/indep_pos", O.RDONLY);

    // Seek fd1 to 500
    FS.llseek(s1, 500, SEEK_SET);
    // fd2 position should still be 0
    const pos2 = FS.llseek(s2, 0, SEEK_CUR);
    expect(pos2).toBe(0);

    // Sequential read on fd2 starts at 0
    const buf = new Uint8Array(100);
    FS.read(s2, buf, 0, 100);
    expect(buf).toEqual(data.subarray(0, 100));

    FS.close(s1);
    FS.close(s2);
  });

  // -------------------------------------------------------------------
  // Random access patterns (database buffer manager)
  // -------------------------------------------------------------------

  it("random page-aligned reads return correct data @fast", () => {
    const { FS } = h;
    const numPages = 8;
    const stream = FS.open("/random_read", O.RDWR | O.CREAT, 0o666);

    // Write 8 pages with distinct patterns
    for (let p = 0; p < numPages; p++) {
      FS.write(stream, pattern(PAGE_SIZE, p * 10), 0, PAGE_SIZE, p * PAGE_SIZE);
    }

    // Read pages in random order
    const readOrder = [5, 2, 7, 0, 3, 6, 1, 4];
    for (const p of readOrder) {
      const buf = new Uint8Array(PAGE_SIZE);
      const n = FS.read(stream, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expect(n).toBe(PAGE_SIZE);
      expect(buf).toEqual(pattern(PAGE_SIZE, p * 10));
    }

    FS.close(stream);
  });

  it("random page-aligned writes then sequential read @fast", () => {
    const { FS } = h;
    const numPages = 8;
    const stream = FS.open("/random_write", O.RDWR | O.CREAT, 0o666);

    // Pre-allocate file
    FS.write(stream, new Uint8Array(numPages * PAGE_SIZE), 0, numPages * PAGE_SIZE);

    // Write pages in random order
    const writeOrder = [4, 1, 6, 3, 0, 7, 2, 5];
    for (const p of writeOrder) {
      FS.write(stream, pattern(PAGE_SIZE, p * 10), 0, PAGE_SIZE, p * PAGE_SIZE);
    }

    // Sequential read of entire file should see all pages correctly
    FS.llseek(stream, 0, SEEK_SET);
    for (let p = 0; p < numPages; p++) {
      const buf = new Uint8Array(PAGE_SIZE);
      const n = FS.read(stream, buf, 0, PAGE_SIZE);
      expect(n).toBe(PAGE_SIZE);
      expect(buf).toEqual(pattern(PAGE_SIZE, p * 10));
    }

    FS.close(stream);
  });

  it("overwrite specific pages with positioned writes", () => {
    const { FS } = h;
    const stream = FS.open("/overwrite_pages", O.RDWR | O.CREAT, 0o666);

    // Write 4 pages with seed 1
    for (let p = 0; p < 4; p++) {
      FS.write(stream, pattern(PAGE_SIZE, 1), 0, PAGE_SIZE, p * PAGE_SIZE);
    }

    // Overwrite pages 1 and 3 with different seed
    FS.write(stream, pattern(PAGE_SIZE, 2), 0, PAGE_SIZE, 1 * PAGE_SIZE);
    FS.write(stream, pattern(PAGE_SIZE, 2), 0, PAGE_SIZE, 3 * PAGE_SIZE);

    // Verify: pages 0,2 have seed 1; pages 1,3 have seed 2
    for (let p = 0; p < 4; p++) {
      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(stream, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
      const expectedSeed = (p === 1 || p === 3) ? 2 : 1;
      expect(buf).toEqual(pattern(PAGE_SIZE, expectedSeed));
    }

    FS.close(stream);
  });

  // -------------------------------------------------------------------
  // Buffer offset parameter
  // -------------------------------------------------------------------

  it("positioned read with non-zero buffer offset stores at correct position @fast", () => {
    const { FS } = h;
    const data = pattern(500, 12);
    const stream = FS.open("/pread_bufoff", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, data, 0, data.length);

    // Read 100 bytes from file position 200, storing at buffer offset 50
    const buf = new Uint8Array(200);
    buf.fill(0xff);
    const n = FS.read(stream, buf, 50, 100, 200);
    expect(n).toBe(100);

    // Bytes 0-49: still 0xff
    for (let i = 0; i < 50; i++) expect(buf[i]).toBe(0xff);
    // Bytes 50-149: file data from offset 200
    expect(buf.subarray(50, 150)).toEqual(data.subarray(200, 300));
    // Bytes 150-199: still 0xff
    for (let i = 150; i < 200; i++) expect(buf[i]).toBe(0xff);

    FS.close(stream);
  });

  it("positioned write with non-zero buffer offset reads from correct position @fast", () => {
    const { FS } = h;
    const srcBuf = new Uint8Array(300);
    srcBuf.fill(0xaa, 0, 100);
    srcBuf.fill(0xbb, 100, 200);
    srcBuf.fill(0xcc, 200, 300);

    const stream = FS.open("/pwrite_bufoff", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, new Uint8Array(500), 0, 500);

    // Write 100 bytes from srcBuf offset 100 to file position 300
    FS.write(stream, srcBuf, 100, 100, 300);

    // Read back from file position 300
    const readBuf = new Uint8Array(100);
    FS.read(stream, readBuf, 0, 100, 300);

    // Should be 0xbb (from srcBuf[100..199])
    for (let i = 0; i < 100; i++) expect(readBuf[i]).toBe(0xbb);

    FS.close(stream);
  });

  // -------------------------------------------------------------------
  // Edge cases
  // -------------------------------------------------------------------

  it("positioned read of 0 bytes returns 0", () => {
    const { FS } = h;
    const stream = FS.open("/pread_zero", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, pattern(100, 1), 0, 100);

    const buf = new Uint8Array(10);
    const n = FS.read(stream, buf, 0, 0, 50);
    expect(n).toBe(0);

    FS.close(stream);
  });

  it("positioned write of 0 bytes is a no-op", () => {
    const { FS } = h;
    const stream = FS.open("/pwrite_zero", O.RDWR | O.CREAT, 0o666);

    FS.write(stream, new Uint8Array(0), 0, 0, 100);
    expect(FS.fstat(stream.fd).size).toBe(0);

    FS.close(stream);
  });

  it("positioned read on empty file returns 0 @fast", () => {
    const { FS } = h;
    const stream = FS.open("/pread_empty", O.RDWR | O.CREAT, 0o666);

    const buf = new Uint8Array(100);
    const n = FS.read(stream, buf, 0, 100, 0);
    expect(n).toBe(0);

    FS.close(stream);
  });
});
