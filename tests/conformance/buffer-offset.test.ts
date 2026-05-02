/**
 * Conformance tests for non-zero buffer offsets in read/write.
 *
 * FS.read(stream, buffer, offset, length, position) and
 * FS.write(stream, buffer, offset, length, position) both accept a
 * buffer offset parameter that specifies where in the buffer to
 * start reading from or writing to. All other conformance tests use
 * offset=0. These tests verify correct behavior with non-zero offsets,
 * including page-boundary crossings.
 *
 * Postgres uses non-zero buffer offsets for scattered I/O patterns:
 * reading page headers separately from page bodies, writing WAL
 * records from mid-buffer positions, and buffer pool management.
 */
import {
  createFS,
  O,
  SEEK_SET,
  type FSHarness,
} from "../harness/emscripten-fs.js";

const PAGE_SIZE = 8192;

describe("buffer offset: read and write (ethos §2)", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  // -------------------------------------------------------------------
  // Write with non-zero buffer offset
  // -------------------------------------------------------------------

  it("write with non-zero buffer offset writes correct slice @fast", () => {
    const { FS } = h;
    const stream = FS.open("/buf-write", O.RDWR | O.CREAT, 0o666);

    // Buffer: [garbage, garbage, 0x41, 0x42, 0x43, garbage]
    const buf = new Uint8Array([0xff, 0xff, 0x41, 0x42, 0x43, 0xff]);
    // Write 3 bytes starting from buf[2]
    FS.write(stream, buf, 2, 3);

    // Read back — should be "ABC"
    const readBuf = new Uint8Array(3);
    FS.read(stream, readBuf, 0, 3, 0);
    expect(readBuf[0]).toBe(0x41);
    expect(readBuf[1]).toBe(0x42);
    expect(readBuf[2]).toBe(0x43);

    FS.close(stream);
  });

  it("write with offset at end of buffer writes last bytes @fast", () => {
    const { FS } = h;
    const stream = FS.open("/buf-write-end", O.RDWR | O.CREAT, 0o666);

    const buf = new Uint8Array([0x00, 0x00, 0x00, 0xDE, 0xAD]);
    // Write last 2 bytes
    FS.write(stream, buf, 3, 2);

    const readBuf = new Uint8Array(2);
    FS.read(stream, readBuf, 0, 2, 0);
    expect(readBuf[0]).toBe(0xDE);
    expect(readBuf[1]).toBe(0xAD);

    FS.close(stream);
  });

  it("write with large buffer offset and small length", () => {
    const { FS } = h;
    const stream = FS.open("/buf-write-large-off", O.RDWR | O.CREAT, 0o666);

    const buf = new Uint8Array(1024);
    buf[500] = 0xCA;
    buf[501] = 0xFE;
    // Write 2 bytes starting from buf[500]
    FS.write(stream, buf, 500, 2);

    const readBuf = new Uint8Array(2);
    FS.read(stream, readBuf, 0, 2, 0);
    expect(readBuf[0]).toBe(0xCA);
    expect(readBuf[1]).toBe(0xFE);

    FS.close(stream);
  });

  // -------------------------------------------------------------------
  // Read with non-zero buffer offset
  // -------------------------------------------------------------------

  it("read with non-zero buffer offset fills correct slice @fast", () => {
    const { FS } = h;
    const stream = FS.open("/buf-read", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, new Uint8Array([0x41, 0x42, 0x43]), 0, 3);

    // Read into middle of buffer
    const readBuf = new Uint8Array(10).fill(0xff);
    FS.read(stream, readBuf, 4, 3, 0);

    // Bytes before offset should be untouched
    expect(readBuf[0]).toBe(0xff);
    expect(readBuf[1]).toBe(0xff);
    expect(readBuf[2]).toBe(0xff);
    expect(readBuf[3]).toBe(0xff);
    // Read data at offset 4
    expect(readBuf[4]).toBe(0x41);
    expect(readBuf[5]).toBe(0x42);
    expect(readBuf[6]).toBe(0x43);
    // Bytes after read should be untouched
    expect(readBuf[7]).toBe(0xff);
    expect(readBuf[8]).toBe(0xff);
    expect(readBuf[9]).toBe(0xff);

    FS.close(stream);
  });

  it("read with offset fills only requested region", () => {
    const { FS } = h;
    const stream = FS.open("/buf-read-region", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]), 0, 8);

    // Read 3 bytes from file position 2 into buffer at offset 5
    const readBuf = new Uint8Array(12).fill(0);
    FS.read(stream, readBuf, 5, 3, 2);

    // buf[0..4] untouched
    for (let i = 0; i < 5; i++) expect(readBuf[i]).toBe(0);
    // buf[5..7] has file data from position 2
    expect(readBuf[5]).toBe(3);
    expect(readBuf[6]).toBe(4);
    expect(readBuf[7]).toBe(5);
    // buf[8..11] untouched
    for (let i = 8; i < 12; i++) expect(readBuf[i]).toBe(0);

    FS.close(stream);
  });

  // -------------------------------------------------------------------
  // Combined write+read with non-zero offsets
  // -------------------------------------------------------------------

  it("write from mid-buffer then read into mid-buffer @fast", () => {
    const { FS } = h;
    const stream = FS.open("/buf-roundtrip", O.RDWR | O.CREAT, 0o666);

    // Source buffer with data at offset 10
    const src = new Uint8Array(20);
    src[10] = 0xBE;
    src[11] = 0xEF;
    src[12] = 0xCA;
    src[13] = 0xFE;
    FS.write(stream, src, 10, 4);

    // Read into dest buffer at offset 6
    const dest = new Uint8Array(16).fill(0xAA);
    FS.read(stream, dest, 6, 4, 0);

    expect(dest[5]).toBe(0xAA);
    expect(dest[6]).toBe(0xBE);
    expect(dest[7]).toBe(0xEF);
    expect(dest[8]).toBe(0xCA);
    expect(dest[9]).toBe(0xFE);
    expect(dest[10]).toBe(0xAA);

    FS.close(stream);
  });

  // -------------------------------------------------------------------
  // Page-boundary interactions with buffer offset
  // -------------------------------------------------------------------

  it("write with offset crossing page boundary", () => {
    const { FS } = h;
    const stream = FS.open("/buf-page-cross", O.RDWR | O.CREAT, 0o666);

    // Create a buffer with data at a non-zero offset
    const dataLen = 256;
    const bufOffset = 64;
    const buf = new Uint8Array(bufOffset + dataLen);
    for (let i = 0; i < dataLen; i++) {
      buf[bufOffset + i] = i & 0xff;
    }

    // Write at file position that crosses a page boundary
    const filePos = PAGE_SIZE - 128;
    FS.write(stream, buf, bufOffset, dataLen, filePos);

    // Read back and verify
    const readBuf = new Uint8Array(dataLen);
    FS.read(stream, readBuf, 0, dataLen, filePos);
    for (let i = 0; i < dataLen; i++) {
      expect(readBuf[i]).toBe(i & 0xff);
    }

    FS.close(stream);
  });

  it("read with offset from data spanning page boundary", () => {
    const { FS } = h;
    const stream = FS.open("/buf-page-read", O.RDWR | O.CREAT, 0o666);

    // Write data spanning a page boundary
    const filePos = PAGE_SIZE - 4;
    const data = new Uint8Array([0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88]);
    FS.write(stream, data, 0, 8, filePos);

    // Read into buffer at non-zero offset
    const readBuf = new Uint8Array(20).fill(0);
    FS.read(stream, readBuf, 8, 8, filePos);

    for (let i = 0; i < 8; i++) expect(readBuf[i]).toBe(0);
    expect(readBuf[8]).toBe(0x11);
    expect(readBuf[9]).toBe(0x22);
    expect(readBuf[10]).toBe(0x33);
    expect(readBuf[11]).toBe(0x44);
    expect(readBuf[12]).toBe(0x55);
    expect(readBuf[13]).toBe(0x66);
    expect(readBuf[14]).toBe(0x77);
    expect(readBuf[15]).toBe(0x88);
    for (let i = 16; i < 20; i++) expect(readBuf[i]).toBe(0);

    FS.close(stream);
  });

  it("write with offset spanning multiple pages @fast", () => {
    const { FS } = h;
    const stream = FS.open("/buf-multi-page", O.RDWR | O.CREAT, 0o666);

    // Large write from mid-buffer spanning 3+ pages
    const dataLen = PAGE_SIZE * 2 + 500;
    const bufOffset = 100;
    const buf = new Uint8Array(bufOffset + dataLen);
    for (let i = 0; i < dataLen; i++) {
      buf[bufOffset + i] = (i * 7 + 13) & 0xff;
    }
    FS.write(stream, buf, bufOffset, dataLen, 0);

    // Read back at different buffer offset and verify
    const readOffset = 50;
    const readBuf = new Uint8Array(readOffset + dataLen).fill(0xFE);
    FS.read(stream, readBuf, readOffset, dataLen, 0);

    // Prefix untouched
    for (let i = 0; i < readOffset; i++) {
      expect(readBuf[i]).toBe(0xFE);
    }
    // Data matches
    for (let i = 0; i < dataLen; i++) {
      expect(readBuf[readOffset + i]).toBe((i * 7 + 13) & 0xff);
    }

    FS.close(stream);
  });

  // -------------------------------------------------------------------
  // Positioned I/O with buffer offsets
  // -------------------------------------------------------------------

  it("positioned read with non-zero buffer offset preserves position @fast", () => {
    const { FS } = h;
    const stream = FS.open("/buf-pos-read", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, new Uint8Array([10, 20, 30, 40, 50, 60, 70, 80]), 0, 8);

    // Seek to position 2
    FS.llseek(stream, 2, SEEK_SET);

    // Positioned read from position 5 into buffer at offset 3
    // (should NOT change stream position)
    const buf = new Uint8Array(10).fill(0);
    FS.read(stream, buf, 3, 3, 5);

    expect(buf[3]).toBe(60);
    expect(buf[4]).toBe(70);
    expect(buf[5]).toBe(80);

    FS.close(stream);
  });

  it("positioned write with non-zero buffer offset", () => {
    const { FS } = h;
    const stream = FS.open("/buf-pos-write", O.RDWR | O.CREAT, 0o666);

    // Write initial data
    FS.write(stream, new Uint8Array(16).fill(0xAA), 0, 16);

    // Positioned write from buf[4..7] to file position 8
    const writeBuf = new Uint8Array([0, 0, 0, 0, 0xDE, 0xAD, 0xBE, 0xEF]);
    FS.write(stream, writeBuf, 4, 4, 8);

    // Verify: bytes 0-7 untouched, bytes 8-11 overwritten, bytes 12-15 untouched
    const readBuf = new Uint8Array(16);
    FS.read(stream, readBuf, 0, 16, 0);

    for (let i = 0; i < 8; i++) expect(readBuf[i]).toBe(0xAA);
    expect(readBuf[8]).toBe(0xDE);
    expect(readBuf[9]).toBe(0xAD);
    expect(readBuf[10]).toBe(0xBE);
    expect(readBuf[11]).toBe(0xEF);
    for (let i = 12; i < 16; i++) expect(readBuf[i]).toBe(0xAA);

    FS.close(stream);
  });

  // -------------------------------------------------------------------
  // Edge cases
  // -------------------------------------------------------------------

  it("read with offset returns correct count when file is shorter than request", () => {
    const { FS } = h;
    const stream = FS.open("/buf-short-read", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, new Uint8Array([1, 2, 3]), 0, 3);

    // Request 10 bytes into buffer at offset 5 — only 3 available
    const buf = new Uint8Array(20).fill(0xCC);
    const n = FS.read(stream, buf, 5, 10, 0);

    expect(n).toBe(3);
    // buf[0..4] untouched
    for (let i = 0; i < 5; i++) expect(buf[i]).toBe(0xCC);
    // buf[5..7] has file data
    expect(buf[5]).toBe(1);
    expect(buf[6]).toBe(2);
    expect(buf[7]).toBe(3);
    // buf[8..19] untouched
    for (let i = 8; i < 20; i++) expect(buf[i]).toBe(0xCC);

    FS.close(stream);
  });

  it("write with offset 0 and length 0 is a no-op @fast", () => {
    const { FS } = h;
    const stream = FS.open("/buf-zero-len", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, new Uint8Array([1, 2, 3]), 0, 3);

    // Zero-length write with non-zero offset should not affect file
    FS.write(stream, new Uint8Array(10), 5, 0);

    expect(FS.fstat(stream.fd).size).toBe(3);
    FS.close(stream);
  });

  it("sequential writes with different buffer offsets build correct file", () => {
    const { FS } = h;
    const stream = FS.open("/buf-seq-writes", O.RDWR | O.CREAT, 0o666);

    // Write "Hello" from different buffer positions
    const buf1 = new Uint8Array([0, 0, 72, 101]); // "He" at offset 2
    FS.write(stream, buf1, 2, 2);

    const buf2 = new Uint8Array([0, 0, 0, 108, 108, 111]); // "llo" at offset 3
    FS.write(stream, buf2, 3, 3);

    // Read back
    const readBuf = new Uint8Array(5);
    FS.read(stream, readBuf, 0, 5, 0);
    const result = String.fromCharCode(...readBuf);
    expect(result).toBe("Hello");

    FS.close(stream);
  });
});
