/**
 * Conformance tests ported from: emscripten/test/unistd/truncate.c
 *
 * Tests: ftruncate grow/shrink, truncate by path, readonly fd,
 *        O_TRUNC on open, negative size, zero-length truncate,
 *        truncate extending with zero fill.
 */
import {
  createFS,
  encode,
  decode,
  expectErrno,
  O,
  SEEK_SET,
  SEEK_END,
  type FSHarness,
} from "../harness/emscripten-fs.js";

describe("truncate (truncate.c)", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  it("ftruncate shrinks a file @fast", () => {
    const { FS } = h;
    const stream = FS.open("/shrink", O.RDWR | O.CREAT, 0o777);
    FS.write(stream, encode("0123456789"), 0, 10);

    FS.ftruncate(stream.fd, 5);

    const stat = FS.fstat(stream.fd);
    expect(stat.size).toBe(5);

    // Read back — only first 5 bytes
    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(20);
    const n = FS.read(stream, buf, 0, 20);
    expect(n).toBe(5);
    expect(decode(buf, n)).toBe("01234");

    FS.close(stream);
  });

  it("ftruncate grows a file with zero-fill @fast", () => {
    const { FS } = h;
    const stream = FS.open("/grow", O.RDWR | O.CREAT, 0o777);
    FS.write(stream, encode("abc"), 0, 3);

    FS.ftruncate(stream.fd, 10);

    const stat = FS.fstat(stream.fd);
    expect(stat.size).toBe(10);

    // Read back — first 3 bytes "abc", next 7 should be zero
    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(20);
    const n = FS.read(stream, buf, 0, 20);
    expect(n).toBe(10);
    expect(decode(buf, 3)).toBe("abc");
    for (let i = 3; i < 10; i++) {
      expect(buf[i]).toBe(0);
    }

    FS.close(stream);
  });

  it("truncate by path shrinks a file", () => {
    const { FS } = h;
    FS.writeFile("/bypath", "Hello World!");

    FS.truncate("/bypath", 5);

    const stat = FS.stat("/bypath");
    expect(stat.size).toBe(5);

    const content = FS.readFile("/bypath", { encoding: "utf8" }) as string;
    expect(content).toBe("Hello");
  });

  it("truncate by path grows a file with zero-fill", () => {
    const { FS } = h;
    FS.writeFile("/growpath", "Hi");

    FS.truncate("/growpath", 8);

    const stat = FS.stat("/growpath");
    expect(stat.size).toBe(8);

    const data = FS.readFile("/growpath") as Uint8Array;
    expect(data[0]).toBe(72); // 'H'
    expect(data[1]).toBe(105); // 'i'
    for (let i = 2; i < 8; i++) {
      expect(data[i]).toBe(0);
    }
  });

  it("ftruncate to zero empties the file @fast", () => {
    const { FS } = h;
    const stream = FS.open("/empty", O.RDWR | O.CREAT, 0o777);
    FS.write(stream, encode("some data here"), 0, 14);

    FS.ftruncate(stream.fd, 0);

    const stat = FS.fstat(stream.fd);
    expect(stat.size).toBe(0);

    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(20);
    const n = FS.read(stream, buf, 0, 20);
    expect(n).toBe(0);

    FS.close(stream);
  });

  it("ftruncate on read-only fd throws EINVAL", () => {
    const { FS, E } = h;
    FS.writeFile("/readonly_trunc", "test data");
    const stream = FS.open("/readonly_trunc", O.RDONLY);

    expectErrno(() => FS.ftruncate(stream.fd, 3), E.EINVAL);

    FS.close(stream);
  });

  it("truncate on non-existent path throws ENOENT", () => {
    const { FS, E } = h;
    expectErrno(() => FS.truncate("/no/such/file", 0), E.ENOENT);
  });

  it("O_TRUNC truncates file on open @fast", () => {
    const { FS } = h;
    FS.writeFile("/trunc_open", "existing content");

    const stream = FS.open("/trunc_open", O.RDWR | O.TRUNC);

    const stat = FS.fstat(stream.fd);
    expect(stat.size).toBe(0);

    FS.close(stream);
  });

  it("O_TRUNC on a new file with O_CREAT creates an empty file", () => {
    const { FS } = h;
    const stream = FS.open(
      "/trunc_new",
      O.RDWR | O.CREAT | O.TRUNC,
      0o777,
    );

    const stat = FS.fstat(stream.fd);
    expect(stat.size).toBe(0);

    FS.close(stream);
  });

  it("ftruncate preserves data before the truncation point", () => {
    const { FS } = h;
    const stream = FS.open("/preserve", O.RDWR | O.CREAT, 0o777);
    FS.write(stream, encode("ABCDEFGHIJ"), 0, 10);

    FS.ftruncate(stream.fd, 7);

    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(20);
    const n = FS.read(stream, buf, 0, 20);
    expect(n).toBe(7);
    expect(decode(buf, n)).toBe("ABCDEFG");

    FS.close(stream);
  });

  it("ftruncate then write extends from truncation point", () => {
    const { FS } = h;
    const stream = FS.open("/trunc_write", O.RDWR | O.CREAT, 0o777);
    FS.write(stream, encode("0123456789"), 0, 10);

    // Truncate to 5, then seek to end and write more
    FS.ftruncate(stream.fd, 5);
    FS.llseek(stream, 0, SEEK_END);
    FS.write(stream, encode("XYZ"), 0, 3);

    // File should be "01234XYZ" (8 bytes)
    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(20);
    const n = FS.read(stream, buf, 0, 20);
    expect(n).toBe(8);
    expect(decode(buf, n)).toBe("01234XYZ");

    FS.close(stream);
  });

  it("repeated truncate grow/shrink cycles work correctly", () => {
    const { FS } = h;
    const stream = FS.open("/cycle", O.RDWR | O.CREAT, 0o777);
    FS.write(stream, encode("ABCDE"), 0, 5);

    // Grow to 10
    FS.ftruncate(stream.fd, 10);
    expect(FS.fstat(stream.fd).size).toBe(10);

    // Shrink to 2
    FS.ftruncate(stream.fd, 2);
    expect(FS.fstat(stream.fd).size).toBe(2);

    // Grow to 6
    FS.ftruncate(stream.fd, 6);
    expect(FS.fstat(stream.fd).size).toBe(6);

    // Read — "AB" + 4 zero bytes
    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(10);
    const n = FS.read(stream, buf, 0, 10);
    expect(n).toBe(6);
    expect(decode(buf, 2)).toBe("AB");
    for (let i = 2; i < 6; i++) {
      expect(buf[i]).toBe(0);
    }

    FS.close(stream);
  });

  it("ftruncate with negative size throws EINVAL", () => {
    const { FS, E } = h;
    const stream = FS.open("/neg_trunc", O.RDWR | O.CREAT, 0o777);
    FS.write(stream, encode("data"), 0, 4);

    expectErrno(() => FS.ftruncate(stream.fd, -1), E.EINVAL);

    // File should be unchanged
    expect(FS.fstat(stream.fd).size).toBe(4);

    FS.close(stream);
  });
});
