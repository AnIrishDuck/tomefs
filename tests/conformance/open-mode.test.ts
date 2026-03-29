/**
 * Conformance tests for open mode flag enforcement.
 *
 * Tests: O_RDONLY prevents writes, O_WRONLY prevents reads,
 *        O_RDWR allows both, ftruncate on read-only fd fails,
 *        mode enforcement survives dup.
 *
 * Source: POSIX open(2) specification. "The file status flags and file access
 * modes of the open file description shall be set according to the value of
 * oflag." The access mode (O_RDONLY, O_WRONLY, O_RDWR) constrains which
 * operations are permitted on the returned file descriptor.
 *
 * These are critical for database safety: Postgres opens WAL files with
 * specific modes and relies on the OS to prevent accidental reads/writes
 * through the wrong fd.
 */
import {
  createFS,
  encode,
  decode,
  expectErrno,
  O,
  SEEK_SET,
  type FSHarness,
} from "../harness/emscripten-fs.js";

describe("open mode enforcement", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  // -------------------------------------------------------------------
  // O_RDONLY: read-only mode
  // -------------------------------------------------------------------

  it("O_RDONLY allows reads @fast", () => {
    const { FS } = h;
    // Create file with data first
    const ws = FS.open("/rdonly_read", O.RDWR | O.CREAT, 0o666);
    FS.write(ws, encode("hello"), 0, 5);
    FS.close(ws);

    // Open read-only and verify read works
    const rs = FS.open("/rdonly_read", O.RDONLY);
    const buf = new Uint8Array(10);
    const n = FS.read(rs, buf, 0, 10);
    expect(n).toBe(5);
    expect(decode(buf, n)).toBe("hello");
    FS.close(rs);
  });

  it("O_RDONLY prevents writes (EBADF) @fast", () => {
    const { FS, E } = h;
    const ws = FS.open("/rdonly_write", O.RDWR | O.CREAT, 0o666);
    FS.write(ws, encode("data"), 0, 4);
    FS.close(ws);

    const rs = FS.open("/rdonly_write", O.RDONLY);
    expectErrno(() => FS.write(rs, encode("x"), 0, 1), E.EBADF);
    FS.close(rs);
  });

  it("O_RDONLY prevents ftruncate (EINVAL)", () => {
    const { FS, E } = h;
    const ws = FS.open("/rdonly_trunc", O.RDWR | O.CREAT, 0o666);
    FS.write(ws, encode("data"), 0, 4);
    FS.close(ws);

    const rs = FS.open("/rdonly_trunc", O.RDONLY);
    expectErrno(() => FS.ftruncate(rs.fd, 2), E.EINVAL);
    FS.close(rs);
  });

  it("O_RDONLY llseek still works", () => {
    const { FS } = h;
    const ws = FS.open("/rdonly_seek", O.RDWR | O.CREAT, 0o666);
    FS.write(ws, encode("abcdef"), 0, 6);
    FS.close(ws);

    const rs = FS.open("/rdonly_seek", O.RDONLY);
    FS.llseek(rs, 3, SEEK_SET);
    const buf = new Uint8Array(10);
    const n = FS.read(rs, buf, 0, 10);
    expect(n).toBe(3);
    expect(decode(buf, n)).toBe("def");
    FS.close(rs);
  });

  // -------------------------------------------------------------------
  // O_WRONLY: write-only mode
  // -------------------------------------------------------------------

  it("O_WRONLY allows writes @fast", () => {
    const { FS } = h;
    const ws = FS.open("/wronly_write", O.WRONLY | O.CREAT, 0o666);
    const n = FS.write(ws, encode("hello"), 0, 5);
    expect(n).toBe(5);
    FS.close(ws);

    // Verify via read-write fd
    const rs = FS.open("/wronly_write", O.RDONLY);
    const buf = new Uint8Array(10);
    const nr = FS.read(rs, buf, 0, 10);
    expect(decode(buf, nr)).toBe("hello");
    FS.close(rs);
  });

  it("O_WRONLY prevents reads (EBADF) @fast", () => {
    const { FS, E } = h;
    const ws = FS.open("/wronly_read", O.WRONLY | O.CREAT, 0o666);
    FS.write(ws, encode("data"), 0, 4);

    const buf = new Uint8Array(10);
    expectErrno(() => FS.read(ws, buf, 0, 10), E.EBADF);
    FS.close(ws);
  });

  it("O_WRONLY llseek still works", () => {
    const { FS } = h;
    const ws = FS.open("/wronly_seek", O.WRONLY | O.CREAT, 0o666);
    FS.write(ws, encode("aaa"), 0, 3);
    FS.llseek(ws, 0, SEEK_SET);
    FS.write(ws, encode("bbb"), 0, 3);
    FS.close(ws);

    // Verify overwrite via read fd
    const rs = FS.open("/wronly_seek", O.RDONLY);
    const buf = new Uint8Array(10);
    const n = FS.read(rs, buf, 0, 10);
    expect(decode(buf, n)).toBe("bbb");
    FS.close(rs);
  });

  it("O_WRONLY allows ftruncate", () => {
    const { FS } = h;
    const ws = FS.open("/wronly_trunc", O.WRONLY | O.CREAT, 0o666);
    FS.write(ws, encode("abcdef"), 0, 6);
    FS.ftruncate(ws.fd, 3);
    FS.close(ws);

    const rs = FS.open("/wronly_trunc", O.RDONLY);
    const buf = new Uint8Array(10);
    const n = FS.read(rs, buf, 0, 10);
    expect(n).toBe(3);
    expect(decode(buf, n)).toBe("abc");
    FS.close(rs);
  });

  // -------------------------------------------------------------------
  // O_RDWR: read-write mode
  // -------------------------------------------------------------------

  it("O_RDWR allows both reads and writes @fast", () => {
    const { FS } = h;
    const stream = FS.open("/rdwr", O.RDWR | O.CREAT, 0o666);

    // Write
    FS.write(stream, encode("test"), 0, 4);

    // Read back
    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(10);
    const n = FS.read(stream, buf, 0, 10);
    expect(n).toBe(4);
    expect(decode(buf, n)).toBe("test");

    FS.close(stream);
  });

  it("O_RDWR allows ftruncate", () => {
    const { FS } = h;
    const stream = FS.open("/rdwr_trunc", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("abcdef"), 0, 6);
    FS.ftruncate(stream.fd, 2);

    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(10);
    const n = FS.read(stream, buf, 0, 10);
    expect(n).toBe(2);
    expect(decode(buf, n)).toBe("ab");

    FS.close(stream);
  });

  // -------------------------------------------------------------------
  // Mode enforcement through dup
  // -------------------------------------------------------------------

  it("dup preserves O_RDONLY mode enforcement", () => {
    const { FS, E } = h;
    const ws = FS.open("/dup_rdonly", O.RDWR | O.CREAT, 0o666);
    FS.write(ws, encode("data"), 0, 4);
    FS.close(ws);

    const rs = FS.open("/dup_rdonly", O.RDONLY);
    const dup = FS.dupStream(rs);

    // Read through dup works
    const buf = new Uint8Array(10);
    const n = FS.read(dup, buf, 0, 10);
    expect(n).toBe(4);

    // Write through dup fails
    FS.llseek(dup, 0, SEEK_SET);
    expectErrno(() => FS.write(dup, encode("x"), 0, 1), E.EBADF);

    FS.close(rs);
    FS.close(dup);
  });

  it("dup preserves O_WRONLY mode enforcement", () => {
    const { FS, E } = h;
    const ws = FS.open("/dup_wronly", O.WRONLY | O.CREAT, 0o666);
    const dup = FS.dupStream(ws);

    // Write through dup works
    FS.write(dup, encode("ok"), 0, 2);

    // Read through dup fails
    FS.llseek(dup, 0, SEEK_SET);
    const buf = new Uint8Array(10);
    expectErrno(() => FS.read(dup, buf, 0, 10), E.EBADF);

    FS.close(ws);
    FS.close(dup);
  });

  // -------------------------------------------------------------------
  // Mode enforcement with O_APPEND
  // -------------------------------------------------------------------

  it("O_WRONLY | O_APPEND allows writes but not reads", () => {
    const { FS, E } = h;
    const ws = FS.open("/append_wronly", O.RDWR | O.CREAT, 0o666);
    FS.write(ws, encode("initial"), 0, 7);
    FS.close(ws);

    const as = FS.open("/append_wronly", O.WRONLY | O.APPEND);
    // Append write works
    FS.write(as, encode("!"), 0, 1);

    // Read fails
    FS.llseek(as, 0, SEEK_SET);
    const buf = new Uint8Array(20);
    expectErrno(() => FS.read(as, buf, 0, 20), E.EBADF);
    FS.close(as);

    // Verify content via read fd
    const rs = FS.open("/append_wronly", O.RDONLY);
    const rbuf = new Uint8Array(20);
    const n = FS.read(rs, rbuf, 0, 20);
    expect(decode(rbuf, n)).toBe("initial!");
    FS.close(rs);
  });

  it("O_RDONLY | O_APPEND allows reads (append flag is for writes)", () => {
    const { FS } = h;
    const ws = FS.open("/append_rdonly", O.RDWR | O.CREAT, 0o666);
    FS.write(ws, encode("data"), 0, 4);
    FS.close(ws);

    // O_RDONLY with O_APPEND — POSIX says O_APPEND is ignored for reads
    const rs = FS.open("/append_rdonly", O.RDONLY | O.APPEND);
    const buf = new Uint8Array(10);
    FS.llseek(rs, 0, SEEK_SET);
    const n = FS.read(rs, buf, 0, 10);
    expect(n).toBe(4);
    expect(decode(buf, n)).toBe("data");
    FS.close(rs);
  });

  // -------------------------------------------------------------------
  // Multiple fds with different modes on same file
  // -------------------------------------------------------------------

  it("different fds on same file have independent mode enforcement @fast", () => {
    const { FS, E } = h;
    const stream = FS.open("/multi_mode", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("hello"), 0, 5);
    FS.close(stream);

    const rdonly = FS.open("/multi_mode", O.RDONLY);
    const wronly = FS.open("/multi_mode", O.WRONLY);
    const rdwr = FS.open("/multi_mode", O.RDWR);

    // rdonly: can read, can't write
    const buf = new Uint8Array(10);
    expect(FS.read(rdonly, buf, 0, 10)).toBe(5);
    FS.llseek(rdonly, 0, SEEK_SET);
    expectErrno(() => FS.write(rdonly, encode("x"), 0, 1), E.EBADF);

    // wronly: can write, can't read
    FS.write(wronly, encode("w"), 0, 1);
    FS.llseek(wronly, 0, SEEK_SET);
    expectErrno(() => FS.read(wronly, buf, 0, 10), E.EBADF);

    // rdwr: can do both
    FS.llseek(rdwr, 0, SEEK_SET);
    expect(FS.read(rdwr, buf, 0, 10)).toBe(5);
    FS.llseek(rdwr, 0, SEEK_SET);
    FS.write(rdwr, encode("r"), 0, 1);

    FS.close(rdonly);
    FS.close(wronly);
    FS.close(rdwr);
  });

  // -------------------------------------------------------------------
  // Write through wronly is visible through rdonly
  // -------------------------------------------------------------------

  it("write through O_WRONLY fd is visible through O_RDONLY fd", () => {
    const { FS } = h;
    const stream = FS.open("/cross_mode", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("AAAA"), 0, 4);
    FS.close(stream);

    const rdonly = FS.open("/cross_mode", O.RDONLY);
    const wronly = FS.open("/cross_mode", O.WRONLY);

    // Overwrite first byte
    FS.write(wronly, encode("B"), 0, 1);

    // Read should see the overwrite
    const buf = new Uint8Array(10);
    const n = FS.read(rdonly, buf, 0, 10);
    expect(n).toBe(4);
    expect(decode(buf, n)).toBe("BAAA");

    FS.close(rdonly);
    FS.close(wronly);
  });
});
