/**
 * Conformance tests ported from: emscripten/test/fs/test_writev.c
 *
 * Tests: pwritev-style scattered I/O — writing multiple buffers in
 *        a single logical operation. Since Emscripten's JS FS API doesn't
 *        expose writev directly, we simulate scattered I/O by writing
 *        multiple chunks sequentially and verifying the result matches
 *        what a single contiguous write would produce.
 *
 * Also tests: write at specific positions (pwrite), verifying that
 *        scattered positional writes produce correct file content.
 */
import {
  createFS,
  encode,
  decode,
  O,
  SEEK_SET,
  type FSHarness,
} from "../harness/emscripten-fs.js";

describe("writev (test_writev.c)", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  it("scattered sequential writes produce contiguous data @fast", () => {
    const { FS } = h;
    const stream = FS.open("/writev", O.RDWR | O.CREAT, 0o777);

    // Simulate writev with 3 iovecs
    const iov0 = encode("Hello");
    const iov1 = encode(", ");
    const iov2 = encode("World!");

    FS.write(stream, iov0, 0, iov0.length);
    FS.write(stream, iov1, 0, iov1.length);
    FS.write(stream, iov2, 0, iov2.length);

    // Read back
    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(100);
    const n = FS.read(stream, buf, 0, buf.length);
    expect(n).toBe(13);
    expect(decode(buf, n)).toBe("Hello, World!");

    FS.close(stream);
  });

  it("positional writes to non-contiguous offsets (pwritev pattern) @fast", () => {
    const { FS } = h;
    const stream = FS.open("/pwritev", O.RDWR | O.CREAT, 0o777);

    // Write chunks at specific positions (simulating pwritev with iovecs
    // at different file offsets)
    const chunk0 = encode("AAAA"); // 4 bytes at offset 0
    const chunk1 = encode("BBBB"); // 4 bytes at offset 8
    const chunk2 = encode("CCCC"); // 4 bytes at offset 4

    FS.write(stream, chunk0, 0, 4, 0);
    FS.write(stream, chunk1, 0, 4, 8);
    FS.write(stream, chunk2, 0, 4, 4);

    // Read back — should be "AAAACCCCBBBB"
    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(20);
    const n = FS.read(stream, buf, 0, 20);
    expect(n).toBe(12);
    expect(decode(buf, n)).toBe("AAAACCCCBBBB");

    FS.close(stream);
  });

  it("scattered writes with empty iovec components", () => {
    const { FS } = h;
    const stream = FS.open("/writev_empty", O.RDWR | O.CREAT, 0o777);

    // Simulate writev with some zero-length iovecs interspersed
    FS.write(stream, encode("A"), 0, 1);
    FS.write(stream, new Uint8Array(0), 0, 0); // empty
    FS.write(stream, encode("B"), 0, 1);
    FS.write(stream, new Uint8Array(0), 0, 0); // empty
    FS.write(stream, encode("C"), 0, 1);

    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(10);
    const n = FS.read(stream, buf, 0, 10);
    expect(n).toBe(3);
    expect(decode(buf, n)).toBe("ABC");

    FS.close(stream);
  });

  it("scattered writes with binary data (non-UTF8 bytes)", () => {
    const { FS } = h;
    const stream = FS.open("/writev_bin", O.RDWR | O.CREAT, 0o777);

    const iov0 = new Uint8Array([0x00, 0x01, 0x02, 0x03]);
    const iov1 = new Uint8Array([0xff, 0xfe, 0xfd]);
    const iov2 = new Uint8Array([0x80, 0x81]);

    FS.write(stream, iov0, 0, iov0.length);
    FS.write(stream, iov1, 0, iov1.length);
    FS.write(stream, iov2, 0, iov2.length);

    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(20);
    const n = FS.read(stream, buf, 0, 20);
    expect(n).toBe(9);

    expect(Array.from(buf.subarray(0, 9))).toEqual([
      0x00, 0x01, 0x02, 0x03, 0xff, 0xfe, 0xfd, 0x80, 0x81,
    ]);

    FS.close(stream);
  });

  it("large scattered write produces correct total size", () => {
    const { FS } = h;
    const stream = FS.open("/writev_large", O.RDWR | O.CREAT, 0o777);

    // Write 100 chunks of 100 bytes each
    const chunk = new Uint8Array(100);
    for (let i = 0; i < 100; i++) {
      chunk.fill(i);
      FS.write(stream, chunk, 0, chunk.length);
    }

    const stat = FS.fstat(stream.fd);
    expect(stat.size).toBe(10000);

    // Spot-check: read the 50th chunk (offset 5000-5099)
    const buf = new Uint8Array(100);
    const n = FS.read(stream, buf, 0, 100, 5000);
    expect(n).toBe(100);
    expect(buf[0]).toBe(50);
    expect(buf[99]).toBe(50);

    FS.close(stream);
  });
});
