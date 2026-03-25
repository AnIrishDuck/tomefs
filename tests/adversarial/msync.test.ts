/**
 * Adversarial differential tests: msync correctness.
 *
 * Emscripten's FS.msync(stream, buffer, offset, length, mmapFlags) writes
 * `buffer[0..length]` to file position `offset`. This matches MEMFS's
 * implementation: `write(stream, buffer, 0, length, offset)`.
 *
 * These tests exercise msync directly via stream_ops to verify that the
 * buffer offset and file position are handled correctly — a common source
 * of parameter-swap bugs in custom FS implementations.
 */
import {
  createFS,
  O,
  SEEK_SET,
  type FSHarness,
} from "../harness/emscripten-fs.js";

/** Page size used by tomefs — tests target this boundary. */
const PAGE_SIZE = 8192;

describe("adversarial: msync correctness", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  it("msync writes buffer contents to the correct file position @fast", () => {
    const { FS } = h;

    // Create a file with known content: 256 bytes of 0xAA
    const initial = new Uint8Array(256).fill(0xaa);
    const stream = FS.open("/msync-pos", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, initial, 0, 256);

    // Simulate what munmap does: create a buffer representing the mmap'd
    // region and call msync to write it back at the original file offset.
    const mmapBuf = new Uint8Array([0xde, 0xad, 0xbe, 0xef]);
    const fileOffset = 100;

    // FS.msync(stream, buffer, offset=filePosition, length, flags)
    // This should write mmapBuf[0..4] to file position 100
    stream.stream_ops.msync(stream, mmapBuf, fileOffset, 4, 0);

    // Read back the whole file and verify
    const readBuf = new Uint8Array(256);
    FS.read(stream, readBuf, 0, 256, 0);

    // Bytes before offset 100 should be untouched
    for (let i = 0; i < 100; i++) {
      expect(readBuf[i]).toBe(0xaa);
    }

    // Bytes at offset 100-103 should be the msync'd data
    expect(readBuf[100]).toBe(0xde);
    expect(readBuf[101]).toBe(0xad);
    expect(readBuf[102]).toBe(0xbe);
    expect(readBuf[103]).toBe(0xef);

    // Bytes after offset 103 should be untouched
    for (let i = 104; i < 256; i++) {
      expect(readBuf[i]).toBe(0xaa);
    }

    FS.close(stream);
  });

  it("msync at page boundary writes to correct position", () => {
    const { FS } = h;

    // Create a file spanning two pages
    const size = PAGE_SIZE + 1024;
    const initial = new Uint8Array(size).fill(0x55);
    const stream = FS.open("/msync-boundary", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, initial, 0, size);

    // msync a small buffer at exactly the page boundary
    const mmapBuf = new Uint8Array([0x01, 0x02, 0x03, 0x04]);
    const fileOffset = PAGE_SIZE - 2; // straddles page boundary
    stream.stream_ops.msync(stream, mmapBuf, fileOffset, 4, 0);

    // Read back and verify the 4 bytes at the page boundary
    const readBuf = new Uint8Array(4);
    FS.read(stream, readBuf, 0, 4, PAGE_SIZE - 2);
    expect(readBuf[0]).toBe(0x01);
    expect(readBuf[1]).toBe(0x02);
    expect(readBuf[2]).toBe(0x03);
    expect(readBuf[3]).toBe(0x04);

    // Verify surrounding bytes are untouched
    const before = new Uint8Array(1);
    FS.read(stream, before, 0, 1, PAGE_SIZE - 3);
    expect(before[0]).toBe(0x55);

    const after = new Uint8Array(1);
    FS.read(stream, after, 0, 1, PAGE_SIZE + 2);
    expect(after[0]).toBe(0x55);

    FS.close(stream);
  });

  it("msync at non-zero file offset does not corrupt position 0", () => {
    const { FS } = h;

    // Write a sentinel at position 0, then fill the rest
    const stream = FS.open("/msync-no-clobber", O.RDWR | O.CREAT, 0o666);
    const sentinel = new Uint8Array([0xca, 0xfe, 0xba, 0xbe]);
    FS.write(stream, sentinel, 0, 4, 0);

    const padding = new Uint8Array(252).fill(0x00);
    FS.write(stream, padding, 0, 252, 4);

    // msync at offset 200 — must NOT overwrite position 0
    const mmapBuf = new Uint8Array([0x11, 0x22, 0x33, 0x44]);
    stream.stream_ops.msync(stream, mmapBuf, 200, 4, 0);

    // Verify sentinel at position 0 is intact
    const check = new Uint8Array(4);
    FS.read(stream, check, 0, 4, 0);
    expect(check[0]).toBe(0xca);
    expect(check[1]).toBe(0xfe);
    expect(check[2]).toBe(0xba);
    expect(check[3]).toBe(0xbe);

    // Verify msync'd data at position 200
    const check2 = new Uint8Array(4);
    FS.read(stream, check2, 0, 4, 200);
    expect(check2[0]).toBe(0x11);
    expect(check2[1]).toBe(0x22);
    expect(check2[2]).toBe(0x33);
    expect(check2[3]).toBe(0x44);

    FS.close(stream);
  });
});
