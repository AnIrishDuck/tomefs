/**
 * Conformance tests ported from: emscripten/test/test_files.c
 *
 * Tests: fopen/fclose/fread/fwrite semantics via the FS API,
 *        write+read round-trips, seeking, O_TRUNC, /dev/null,
 *        tmpfile/mkstemp equivalents.
 *
 * Note: The C test uses stdio (fopen/fwrite/etc). We translate to Emscripten's
 * FS.open/FS.write/FS.read which are the underlying operations. Some stdio-
 * specific tests (fscanf, fgets from stdin) are not applicable.
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
  S_IRUSR,
  type FSHarness,
} from "../harness/emscripten-fs.js";

describe("files (test_files.c)", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  describe("writing and reading back", () => {
    it("writes binary data and reads it back @fast", () => {
      const { FS } = h;
      const data = new Uint8Array([10, 30, 20, 11, 88]);

      const outStream = FS.open("/go.out", O.WRONLY | O.CREAT, 0o666);
      const nw = FS.write(outStream, data, 0, 5);
      expect(nw).toBe(5);
      FS.close(outStream);

      const inStream = FS.open("/go.out", O.RDONLY);
      const readBuf = new Uint8Array(10);
      const nr = FS.read(inStream, readBuf, 0, 10);
      expect(nr).toBe(5);
      expect(readBuf[0]).toBe(10);
      expect(readBuf[1]).toBe(30);
      expect(readBuf[2]).toBe(20);
      expect(readBuf[3]).toBe(11);
      expect(readBuf[4]).toBe(88);
      FS.close(inStream);
    });
  });

  describe("seeking", () => {
    it("seeks with SEEK_SET, SEEK_CUR, SEEK_END", () => {
      const { FS } = h;

      // Create a file with known content
      const content = encode("some data.");
      const stream = FS.open("/test.file", O.RDWR | O.CREAT, 0o666);
      FS.write(stream, content, 0, content.length);
      FS.close(stream);

      const other = FS.open("/test.file", O.RDONLY);
      const buf = new Uint8Array(1000);

      // Read first 9 bytes
      let n = FS.read(other, buf, 0, 9);
      expect(decode(buf, n)).toBe("some data");

      // SEEK_SET to 2, read 5
      FS.llseek(other, 2, SEEK_SET);
      n = FS.read(other, buf, 0, 5);
      expect(decode(buf, n)).toBe("me da");

      // SEEK_CUR -1, read 3
      FS.llseek(other, -1, SEEK_CUR);
      n = FS.read(other, buf, 0, 3);
      expect(decode(buf, n)).toBe("ata");

      // SEEK_END -2, read 2
      FS.llseek(other, -2, SEEK_END);
      n = FS.read(other, buf, 0, 2);
      expect(decode(buf, n)).toBe("a.");

      FS.close(other);
    });
  });

  describe("/dev/null", () => {
    it("can open and write to /dev/null", () => {
      const { FS } = h;
      // /dev/null should be writable
      const stream = FS.open("/dev/null", O.WRONLY);
      expect(stream.fd).toBeGreaterThanOrEqual(0);

      const data = new Uint8Array([10, 30, 20, 11, 88]);
      const nw = FS.write(stream, data, 0, 5);
      expect(nw).toBe(5);

      FS.close(stream);
    });
  });

  describe("O_TRUNC", () => {
    it("O_TRUNC truncates file on open @fast", () => {
      const { FS } = h;

      // Create file with some content (use 0o666 so we can reopen for write)
      const stream1 = FS.open("/test.out", O.WRONLY | O.CREAT, 0o666);
      FS.write(stream1, encode("blablabla\n"), 0, 10);
      FS.close(stream1);

      // Verify file has content
      let stat = FS.stat("/test.out");
      expect(stat.size).toBe(10);

      // Open with O_TRUNC
      const stream2 = FS.open(
        "/test.out",
        O.WRONLY | O.CREAT | O.TRUNC,
        0o666,
      );

      // File should be empty now
      stat = FS.stat("/test.out");
      expect(stat.size).toBe(0);

      // Write new content
      const nw = FS.write(stream2, encode("blablabla\n"), 0, 10);
      expect(nw).toBe(10);
      FS.close(stream2);

      // Verify final size
      stat = FS.stat("/test.out");
      expect(stat.size).toBe(10);
    });
  });

  describe("writeFile / readFile round-trip", () => {
    it("writeFile then readFile returns same content", () => {
      const { FS } = h;

      const data = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]);
      FS.writeFile("/roundtrip", data);

      const result = FS.readFile("/roundtrip") as Uint8Array;
      expect(result.length).toBe(8);
      for (let i = 0; i < 8; i++) {
        expect(result[i]).toBe(i + 1);
      }
    });

    it("writeFile with string data, readFile with encoding", () => {
      const { FS } = h;

      FS.writeFile("/textfile", "hello world");
      const result = FS.readFile("/textfile", {
        encoding: "utf8",
      }) as string;
      expect(result).toBe("hello world");
    });
  });

  describe("file size tracking", () => {
    it("file size updates after writes @fast", () => {
      const { FS } = h;
      const stream = FS.open("/sizefile", O.RDWR | O.CREAT, 0o666);

      // Empty file
      expect(FS.stat("/sizefile").size).toBe(0);

      // Write 5 bytes
      FS.write(stream, encode("hello"), 0, 5);
      expect(FS.stat("/sizefile").size).toBe(5);

      // Write 5 more bytes
      FS.write(stream, encode("world"), 0, 5);
      expect(FS.stat("/sizefile").size).toBe(10);

      FS.close(stream);
    });
  });

  describe("multiple opens of same file", () => {
    it("two fds can read the same file independently", () => {
      const { FS } = h;

      // Create file
      const ws = FS.open("/shared", O.WRONLY | O.CREAT, 0o666);
      FS.write(ws, encode("abcdefghij"), 0, 10);
      FS.close(ws);

      // Open two readers
      const r1 = FS.open("/shared", O.RDONLY);
      const r2 = FS.open("/shared", O.RDONLY);

      // Read 3 bytes from r1
      const buf1 = new Uint8Array(3);
      FS.read(r1, buf1, 0, 3);
      expect(decode(buf1)).toBe("abc");

      // Read 5 bytes from r2 (starts at 0 independently)
      const buf2 = new Uint8Array(5);
      FS.read(r2, buf2, 0, 5);
      expect(decode(buf2)).toBe("abcde");

      // r1's position should still be at 3
      const buf3 = new Uint8Array(3);
      FS.read(r1, buf3, 0, 3);
      expect(decode(buf3)).toBe("def");

      FS.close(r1);
      FS.close(r2);
    });
  });
});
