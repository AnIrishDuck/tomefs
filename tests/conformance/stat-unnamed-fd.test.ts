/**
 * Conformance tests ported from: emscripten/test/fs/test_stat_unnamed_file_descriptor.c
 *
 * Tests: fstat, fchmod, ftruncate, read/write on a file descriptor after
 *        the file has been unlinked. POSIX requires that an open fd remains
 *        valid until closed, even if the directory entry is removed.
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
  type FSHarness,
} from "../harness/emscripten-fs.js";

describe("stat unnamed file descriptor (test_stat_unnamed_file_descriptor.c)", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  it("fstat works on fd after file is unlinked @fast", () => {
    const { FS } = h;
    const stream = FS.open("/willunlink", O.RDWR | O.CREAT, 0o777);
    FS.write(stream, encode("hello"), 0, 5);

    FS.unlink("/willunlink");

    const stat = FS.fstat(stream.fd);
    expect(stat.size).toBe(5);
    expect(stat.mode & S_IFMT).toBe(S_IFREG);

    FS.close(stream);
  });

  it("fchmod works on fd after file is unlinked", () => {
    const { FS } = h;
    const stream = FS.open("/fchmod_unlinked", O.RDWR | O.CREAT, 0o777);

    FS.unlink("/fchmod_unlinked");

    FS.fchmod(stream.fd, 0o644);
    const stat = FS.fstat(stream.fd);
    expect(stat.mode & 0o777).toBe(0o644);

    FS.close(stream);
  });

  it("ftruncate works on fd after file is unlinked @fast", () => {
    const { FS } = h;
    const stream = FS.open("/ftrunc_unlinked", O.RDWR | O.CREAT, 0o777);
    FS.write(stream, encode("abcdefghij"), 0, 10);

    FS.unlink("/ftrunc_unlinked");

    FS.ftruncate(stream.fd, 5);
    const stat = FS.fstat(stream.fd);
    expect(stat.size).toBe(5);

    FS.close(stream);
  });

  it("read works on fd after file is unlinked", () => {
    const { FS } = h;
    const stream = FS.open("/read_unlinked", O.RDWR | O.CREAT, 0o777);
    FS.write(stream, encode("readable"), 0, 8);

    FS.unlink("/read_unlinked");

    // Seek back and read
    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(20);
    const n = FS.read(stream, buf, 0, 20);
    expect(n).toBe(8);
    expect(decode(buf, n)).toBe("readable");

    FS.close(stream);
  });

  it("write works on fd after file is unlinked", () => {
    const { FS } = h;
    const stream = FS.open("/write_unlinked", O.RDWR | O.CREAT, 0o777);

    FS.unlink("/write_unlinked");

    // Write to the unlinked fd
    FS.write(stream, encode("post-unlink"), 0, 11);

    // Seek back and verify
    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(20);
    const n = FS.read(stream, buf, 0, 20);
    expect(n).toBe(11);
    expect(decode(buf, n)).toBe("post-unlink");

    FS.close(stream);
  });

  it("stat by path fails after unlink (ENOENT) while fd still works", () => {
    const { FS, E } = h;
    const stream = FS.open("/gone", O.RDWR | O.CREAT, 0o777);
    FS.write(stream, encode("data"), 0, 4);

    FS.unlink("/gone");

    // Path-based stat fails
    expectErrno(() => FS.stat("/gone"), E.ENOENT);

    // fd-based fstat still works
    const stat = FS.fstat(stream.fd);
    expect(stat.size).toBe(4);

    FS.close(stream);
  });

  it("multiple fds to same file: unlink + close one, other still works", () => {
    const { FS } = h;
    const stream1 = FS.open("/multi_fd", O.RDWR | O.CREAT, 0o777);
    FS.write(stream1, encode("shared"), 0, 6);

    const stream2 = FS.open("/multi_fd", O.RDONLY);

    FS.unlink("/multi_fd");
    FS.close(stream1);

    // stream2 should still be readable
    const buf = new Uint8Array(20);
    const n = FS.read(stream2, buf, 0, 20);
    expect(n).toBe(6);
    expect(decode(buf, n)).toBe("shared");

    FS.close(stream2);
  });
});
