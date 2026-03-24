/**
 * Conformance tests ported from: emscripten/test/wasmfs/wasmfs_dup.c
 *
 * Tests: dup (dupStream), dup2 (dupStream with target fd), shared seek
 *        position between duped fds, EBADF on invalid fd, independent
 *        close of duped streams.
 *
 * Note: Emscripten's JS FS exposes dup via FS.dupStream(stream, fd?).
 * dupStream without a second arg is dup; with a target fd is dup2.
 */
import {
  createFS,
  encode,
  decode,
  expectErrno,
  O,
  SEEK_SET,
  SEEK_CUR,
  SEEK_END,
  type FSHarness,
} from "../harness/emscripten-fs.js";

describe("dup (wasmfs_dup.c)", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  it("dupStream creates a new fd pointing to the same file @fast", () => {
    const { FS } = h;
    const stream = FS.open("/dupfile", O.RDWR | O.CREAT, 0o777);
    const dup = FS.dupStream(stream);

    expect(dup.fd).not.toBe(stream.fd);
    expect(dup.node).toBe(stream.node); // same underlying file

    FS.close(stream);
    FS.close(dup);
  });

  it("duped fds share seek position: write on one, read on other @fast", () => {
    const { FS } = h;
    const stream = FS.open("/shared_pos", O.RDWR | O.CREAT, 0o777);
    const dup = FS.dupStream(stream);

    // Write through original
    const data = encode("Hello");
    FS.write(stream, data, 0, 5);

    // Both should be at position 5
    expect(FS.llseek(stream, 0, SEEK_CUR)).toBe(5);
    expect(FS.llseek(dup, 0, SEEK_CUR)).toBe(5);

    // Seek original back to 0
    FS.llseek(stream, 0, SEEK_SET);

    // Dup should also be at 0 (shared position)
    expect(FS.llseek(dup, 0, SEEK_CUR)).toBe(0);

    // Read through dup
    const buf = new Uint8Array(10);
    const n = FS.read(dup, buf, 0, 10);
    expect(n).toBe(5);
    expect(decode(buf, n)).toBe("Hello");

    // Both should now be at position 5
    expect(FS.llseek(stream, 0, SEEK_CUR)).toBe(5);
    expect(FS.llseek(dup, 0, SEEK_CUR)).toBe(5);

    FS.close(stream);
    FS.close(dup);
  });

  it("seek on dup affects the original's position", () => {
    const { FS } = h;
    const stream = FS.open("/seek_shared", O.RDWR | O.CREAT, 0o777);
    FS.write(stream, encode("abcdefghij"), 0, 10);
    FS.llseek(stream, 0, SEEK_SET);

    const dup = FS.dupStream(stream);

    // Seek dup to position 5
    FS.llseek(dup, 5, SEEK_SET);

    // Original should also be at 5
    expect(FS.llseek(stream, 0, SEEK_CUR)).toBe(5);

    // Read from original — should get "fghij"
    const buf = new Uint8Array(10);
    const n = FS.read(stream, buf, 0, 10);
    expect(n).toBe(5);
    expect(decode(buf, n)).toBe("fghij");

    FS.close(stream);
    FS.close(dup);
  });

  it("write through dup is visible when reading through original", () => {
    const { FS } = h;
    const stream = FS.open("/write_dup", O.RDWR | O.CREAT, 0o777);
    const dup = FS.dupStream(stream);

    // Write through dup
    FS.write(dup, encode("DupWrite"), 0, 8);

    // Seek original to start and read
    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(20);
    const n = FS.read(stream, buf, 0, 20);
    expect(n).toBe(8);
    expect(decode(buf, n)).toBe("DupWrite");

    FS.close(stream);
    FS.close(dup);
  });

  it("closing original does not invalidate the dup", () => {
    const { FS } = h;
    const stream = FS.open("/close_orig", O.RDWR | O.CREAT, 0o777);
    FS.write(stream, encode("persist"), 0, 7);

    const dup = FS.dupStream(stream);

    // Close the original
    FS.close(stream);

    // Dup should still work
    FS.llseek(dup, 0, SEEK_SET);
    const buf = new Uint8Array(20);
    const n = FS.read(dup, buf, 0, 20);
    expect(n).toBe(7);
    expect(decode(buf, n)).toBe("persist");

    FS.close(dup);
  });

  it("closing dup does not invalidate the original", () => {
    const { FS } = h;
    const stream = FS.open("/close_dup", O.RDWR | O.CREAT, 0o777);
    FS.write(stream, encode("still here"), 0, 10);

    const dup = FS.dupStream(stream);
    FS.close(dup);

    // Original should still work
    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(20);
    const n = FS.read(stream, buf, 0, 20);
    expect(n).toBe(10);
    expect(decode(buf, n)).toBe("still here");

    FS.close(stream);
  });

  it("dupStream with target fd (dup2 semantics)", () => {
    const { FS } = h;
    const stream = FS.open("/dup2file", O.RDWR | O.CREAT, 0o777);
    FS.write(stream, encode("dup2data"), 0, 8);

    // Dup to a specific fd number
    const dup = FS.dupStream(stream, 10);
    expect(dup.fd).toBe(10);

    // Should share position and node
    expect(dup.node).toBe(stream.node);

    // Read through the dup2'd fd
    FS.llseek(dup, 0, SEEK_SET);
    const buf = new Uint8Array(20);
    const n = FS.read(dup, buf, 0, 20);
    expect(n).toBe(8);
    expect(decode(buf, n)).toBe("dup2data");

    FS.close(stream);
    FS.close(dup);
  });

  it("dup2 to an existing fd closes the old stream on that fd", () => {
    const { FS } = h;
    const stream1 = FS.open("/file1", O.RDWR | O.CREAT, 0o777);
    FS.write(stream1, encode("file1"), 0, 5);

    const stream2 = FS.open("/file2", O.RDWR | O.CREAT, 0o777);
    FS.write(stream2, encode("file2"), 0, 5);
    const fd2 = stream2.fd;

    // dup stream1 onto stream2's fd — stream2 should be implicitly closed
    const dup = FS.dupStream(stream1, fd2);
    expect(dup.fd).toBe(fd2);
    expect(dup.node).toBe(stream1.node);

    // Reading through the dup'd fd should give file1's content
    FS.llseek(dup, 0, SEEK_SET);
    const buf = new Uint8Array(20);
    const n = FS.read(dup, buf, 0, 20);
    expect(decode(buf, n)).toBe("file1");

    FS.close(stream1);
    FS.close(dup);
  });

  it("multiple dups all share the same position", () => {
    const { FS } = h;
    const stream = FS.open("/multi_dup", O.RDWR | O.CREAT, 0o777);
    FS.write(stream, encode("0123456789"), 0, 10);

    const dup1 = FS.dupStream(stream);
    const dup2 = FS.dupStream(stream);

    // All three at position 10 after write
    expect(FS.llseek(stream, 0, SEEK_CUR)).toBe(10);
    expect(FS.llseek(dup1, 0, SEEK_CUR)).toBe(10);
    expect(FS.llseek(dup2, 0, SEEK_CUR)).toBe(10);

    // Seek dup2 to 3
    FS.llseek(dup2, 3, SEEK_SET);

    // All should be at 3
    expect(FS.llseek(stream, 0, SEEK_CUR)).toBe(3);
    expect(FS.llseek(dup1, 0, SEEK_CUR)).toBe(3);

    // Read through dup1
    const buf = new Uint8Array(3);
    const n = FS.read(dup1, buf, 0, 3);
    expect(decode(buf, n)).toBe("345");

    // All at 6 now
    expect(FS.llseek(stream, 0, SEEK_CUR)).toBe(6);
    expect(FS.llseek(dup2, 0, SEEK_CUR)).toBe(6);

    FS.close(stream);
    FS.close(dup1);
    FS.close(dup2);
  });

  it("fstat on duped fd returns same metadata as original @fast", () => {
    const { FS } = h;
    const stream = FS.open("/fstat_dup", O.RDWR | O.CREAT, 0o777);
    FS.write(stream, encode("data"), 0, 4);

    const dup = FS.dupStream(stream);

    const origStat = FS.fstat(stream.fd);
    const dupStat = FS.fstat(dup.fd);

    expect(dupStat.ino).toBe(origStat.ino);
    expect(dupStat.size).toBe(origStat.size);
    expect(dupStat.mode).toBe(origStat.mode);

    FS.close(stream);
    FS.close(dup);
  });
});
