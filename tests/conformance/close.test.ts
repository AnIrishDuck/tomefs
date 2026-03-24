/**
 * Conformance tests ported from: emscripten/test/unistd/close.c
 *
 * Tests: close, double-close (EBADF), operations on closed fd (EBADF),
 *        close with pending data.
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

describe("close (close.c)", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  it("close returns normally on a valid fd @fast", () => {
    const { FS } = h;
    const stream = FS.open("/closeme", O.RDWR | O.CREAT, 0o777);
    FS.write(stream, encode("data"), 0, 4);

    // close should not throw
    FS.close(stream);
  });

  it("double close throws EBADF @fast", () => {
    const { FS, E } = h;
    const stream = FS.open("/dblclose", O.RDWR | O.CREAT, 0o777);
    FS.close(stream);

    expectErrno(() => FS.close(stream), E.EBADF);
  });

  it("read on closed fd throws EBADF", () => {
    const { FS, E } = h;
    const stream = FS.open("/readclosed", O.RDWR | O.CREAT, 0o777);
    FS.write(stream, encode("data"), 0, 4);
    FS.close(stream);

    const buf = new Uint8Array(10);
    expectErrno(() => FS.read(stream, buf, 0, 10), E.EBADF);
  });

  it("write on closed fd throws EBADF", () => {
    const { FS, E } = h;
    const stream = FS.open("/writeclosed", O.RDWR | O.CREAT, 0o777);
    FS.close(stream);

    expectErrno(() => FS.write(stream, encode("x"), 0, 1), E.EBADF);
  });

  it("llseek on closed fd throws EBADF", () => {
    const { FS, E } = h;
    const stream = FS.open("/seekclosed", O.RDWR | O.CREAT, 0o777);
    FS.close(stream);

    expectErrno(() => FS.llseek(stream, 0, SEEK_SET), E.EBADF);
  });

  it("data written before close is persisted @fast", () => {
    const { FS } = h;
    const stream = FS.open("/persist", O.RDWR | O.CREAT, 0o777);
    FS.write(stream, encode("saved"), 0, 5);
    FS.close(stream);

    // Re-open and verify data
    const stream2 = FS.open("/persist", O.RDONLY);
    const buf = new Uint8Array(10);
    const n = FS.read(stream2, buf, 0, 10);
    expect(n).toBe(5);
    expect(decode(buf, n)).toBe("saved");
    FS.close(stream2);
  });

  it("fd number is reusable after close", () => {
    const { FS } = h;
    const stream1 = FS.open("/reuse1", O.RDWR | O.CREAT, 0o777);
    const fd1 = stream1.fd;
    FS.close(stream1);

    // The next open may reuse the same fd number
    const stream2 = FS.open("/reuse2", O.RDWR | O.CREAT, 0o777);
    // We don't mandate fd reuse, but ensure the new fd is valid
    expect(stream2.fd).toBeGreaterThanOrEqual(0);

    const buf = new Uint8Array(10);
    FS.write(stream2, encode("new"), 0, 3);
    FS.llseek(stream2, 0, SEEK_SET);
    const n = FS.read(stream2, buf, 0, 10);
    expect(n).toBe(3);
    expect(decode(buf, n)).toBe("new");

    FS.close(stream2);
  });
});
