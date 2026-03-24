/**
 * Conformance tests ported from: emscripten/test/wasmfs/wasmfs_create.c
 *
 * Tests: open, write, read, close, O_CREAT, O_EXCL, O_DIRECTORY,
 *        zero-length r/w, large writes, file mode bits.
 */
import {
  createFS,
  encode,
  decode,
  expectErrno,
  O,
  SEEK_SET,
  S_IFMT,
  S_IFREG,
  S_IRWXUGO,
  type FSHarness,
} from "../harness/emscripten-fs.js";

describe("create (wasmfs_create.c)", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  it("creates a new file and verifies mode bits @fast", () => {
    const { FS } = h;
    const stream = FS.open("/test", O.RDWR | O.CREAT, 0o777);

    const stat = FS.stat("/test");
    // File type should be regular file
    expect(stat.mode & S_IFMT).toBe(S_IFREG);
    // Permission bits should be 0o777
    expect(stat.mode).toBe(S_IRWXUGO | S_IFREG);

    FS.close(stream);
  });

  it("writes to a file and reads it back via a second fd @fast", () => {
    const { FS } = h;
    const stream = FS.open("/test", O.RDWR | O.CREAT, 0o777);

    const msg = encode("Test\n");
    FS.write(stream, msg, 0, msg.length);

    // Open another fd to the same file and read
    const stream2 = FS.open("/test", O.RDWR);
    const buf = new Uint8Array(100);
    const n = FS.read(stream2, buf, 0, buf.length);
    expect(decode(buf, n)).toBe("Test\n");

    FS.close(stream);
    FS.close(stream2);
  });

  it("O_EXCL | O_CREAT fails with EEXIST on existing file", () => {
    const { FS, E } = h;
    FS.open("/test", O.RDWR | O.CREAT, 0o777);

    expectErrno(
      () => FS.open("/test", O.RDWR | O.CREAT | O.EXCL, S_IRWXUGO),
      E.EEXIST,
    );
  });

  it("O_DIRECTORY on a regular file fails with ENOTDIR", () => {
    const { FS, E } = h;
    // Create a regular file first
    const s = FS.open("/test", O.RDWR | O.CREAT, 0o777);
    FS.close(s);

    expectErrno(() => FS.open("/test", O.RDWR | O.DIRECTORY), E.ENOTDIR);
  });

  it("O_DIRECTORY on a directory succeeds", () => {
    const { FS } = h;
    // /dev exists by default in Emscripten
    const stream = FS.open("/dev", O.RDONLY | O.DIRECTORY);
    expect(stream.fd).toBeGreaterThanOrEqual(0);
    FS.close(stream);
  });

  it("zero-length read and write succeed", () => {
    const { FS } = h;
    const stream = FS.open("/newFile", O.RDWR | O.CREAT, S_IRWXUGO);

    const buf = new Uint8Array(100);
    const nRead = FS.read(stream, buf, 0, 0);
    expect(nRead).toBe(0);

    const msg = encode("hello");
    const nWritten = FS.write(stream, msg, 0, 0);
    expect(nWritten).toBe(0);

    FS.close(stream);
  });

  it("multiple writes accumulate and can be read back", () => {
    const { FS } = h;
    const msg = encode("Test\n");

    const stream = FS.open("/testFile", O.RDWR | O.CREAT, S_IRWXUGO);

    // Write msg with length extending beyond actual data (write pads with
    // whatever is in the buffer beyond msg). The C test writes strlen(msg)+20
    // bytes — we replicate by writing a larger buffer.
    const bigBuf = new Uint8Array(msg.length + 20);
    bigBuf.set(msg);
    FS.write(stream, bigBuf, 0, bigBuf.length);

    // Second write: just the message
    FS.write(stream, msg, 0, msg.length);

    // Third write: msg + 30 extra bytes
    const bigBuf2 = new Uint8Array(msg.length + 30);
    bigBuf2.set(msg);
    FS.write(stream, bigBuf2, 0, bigBuf2.length);

    // Open a new fd and read the entire file
    const stream2 = FS.open("/testFile", O.RDWR);
    const readBuf = new Uint8Array(100);
    const n = FS.read(stream2, readBuf, 0, readBuf.length);

    // Total bytes written: (5+20) + 5 + (5+30) = 65
    expect(n).toBe(65);

    // First 5 bytes should be "Test\n"
    expect(decode(readBuf, 5)).toBe("Test\n");

    FS.close(stream);
    FS.close(stream2);
  });
});
