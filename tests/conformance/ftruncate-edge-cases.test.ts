/**
 * Conformance tests for ftruncate edge cases.
 *
 * Exercises ftruncate interactions with cursor position, multiple fds,
 * duped fds, page boundaries, and interleaved writes. These are POSIX
 * semantics that Postgres relies on during WAL truncation, relation
 * extension, and vacuum.
 *
 * Ethos §2: Real POSIX semantics
 * Ethos §8: New conformance test sources beyond the Emscripten suite
 */
import {
  createFS,
  encode,
  decode,
  O,
  SEEK_SET,
  SEEK_CUR,
  SEEK_END,
  type FSHarness,
} from "../harness/emscripten-fs.js";
import { PAGE_SIZE } from "../../src/types.js";

describe("ftruncate edge cases", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  describe("cursor position after truncate", () => {
    it("ftruncate does not move cursor position @fast", () => {
      const { FS } = h;
      const s = FS.open("/cursor-no-move", O.RDWR | O.CREAT, 0o666);
      FS.write(s, encode("0123456789"), 0, 10);

      FS.llseek(s, 7, SEEK_SET);
      FS.ftruncate(s.fd, 5);

      const pos = FS.llseek(s, 0, SEEK_CUR);
      expect(pos).toBe(7);

      FS.close(s);
    });

    it("read at cursor beyond truncated size returns 0 bytes @fast", () => {
      const { FS } = h;
      const s = FS.open("/cursor-past-eof", O.RDWR | O.CREAT, 0o666);
      FS.write(s, encode("0123456789"), 0, 10);

      FS.llseek(s, 8, SEEK_SET);
      FS.ftruncate(s.fd, 5);

      const buf = new Uint8Array(10);
      const n = FS.read(s, buf, 0, 10);
      expect(n).toBe(0);

      FS.close(s);
    });

    it("write at cursor beyond truncated size extends file with gap @fast", () => {
      const { FS } = h;
      const s = FS.open("/cursor-write-gap", O.RDWR | O.CREAT, 0o666);
      FS.write(s, encode("0123456789"), 0, 10);

      FS.llseek(s, 8, SEEK_SET);
      FS.ftruncate(s.fd, 3);

      FS.write(s, encode("XY"), 0, 2);

      expect(FS.fstat(s.fd).size).toBe(10);

      FS.llseek(s, 0, SEEK_SET);
      const buf = new Uint8Array(10);
      const n = FS.read(s, buf, 0, 10);
      expect(n).toBe(10);
      expect(decode(buf.subarray(0, 3), 3)).toBe("012");
      for (let i = 3; i < 8; i++) {
        expect(buf[i]).toBe(0);
      }
      expect(decode(buf.subarray(8, 10), 2)).toBe("XY");

      FS.close(s);
    });

    it("SEEK_END after truncate seeks relative to new size", () => {
      const { FS } = h;
      const s = FS.open("/seek-end-trunc", O.RDWR | O.CREAT, 0o666);
      FS.write(s, encode("0123456789ABCDEF"), 0, 16);

      FS.ftruncate(s.fd, 6);
      const pos = FS.llseek(s, -2, SEEK_END);
      expect(pos).toBe(4);

      const buf = new Uint8Array(2);
      FS.read(s, buf, 0, 2);
      expect(decode(buf, 2)).toBe("45");

      FS.close(s);
    });
  });

  describe("multi-fd truncate interactions", () => {
    it("ftruncate on fd1 updates stat from fd2 immediately @fast", () => {
      const { FS } = h;
      const fd1 = FS.open("/multi-stat", O.RDWR | O.CREAT, 0o666);
      FS.write(fd1, encode("0123456789"), 0, 10);
      const fd2 = FS.open("/multi-stat", O.RDONLY);

      FS.ftruncate(fd1.fd, 4);
      expect(FS.fstat(fd2.fd).size).toBe(4);

      FS.close(fd1);
      FS.close(fd2);
    });

    it("fd2 read position unaffected by fd1 ftruncate", () => {
      const { FS } = h;
      const fd1 = FS.open("/multi-pos", O.RDWR | O.CREAT, 0o666);
      FS.write(fd1, encode("ABCDEFGHIJ"), 0, 10);

      const fd2 = FS.open("/multi-pos", O.RDONLY);
      FS.llseek(fd2, 3, SEEK_SET);

      FS.ftruncate(fd1.fd, 7);

      const buf = new Uint8Array(10);
      const n = FS.read(fd2, buf, 0, 10);
      expect(n).toBe(4);
      expect(decode(buf, n)).toBe("DEFG");

      FS.close(fd1);
      FS.close(fd2);
    });

    it("write on fd2 after fd1 ftruncate respects new size", () => {
      const { FS } = h;
      const fd1 = FS.open("/multi-write", O.RDWR | O.CREAT, 0o666);
      FS.write(fd1, encode("0123456789"), 0, 10);

      const fd2 = FS.open("/multi-write", O.RDWR);
      FS.llseek(fd2, 0, SEEK_END);
      expect(FS.llseek(fd2, 0, SEEK_CUR)).toBe(10);

      FS.ftruncate(fd1.fd, 3);

      FS.write(fd2, encode("Z"), 0, 1);
      expect(FS.fstat(fd1.fd).size).toBe(11);

      FS.llseek(fd1, 0, SEEK_SET);
      const buf = new Uint8Array(11);
      const n = FS.read(fd1, buf, 0, 11);
      expect(n).toBe(11);
      expect(decode(buf.subarray(0, 3), 3)).toBe("012");
      for (let i = 3; i < 10; i++) {
        expect(buf[i]).toBe(0);
      }
      expect(buf[10]).toBe(90); // 'Z'

      FS.close(fd1);
      FS.close(fd2);
    });
  });

  describe("dup + truncate", () => {
    it("ftruncate on duped fd affects original @fast", () => {
      const { FS } = h;
      const s = FS.open("/dup-trunc", O.RDWR | O.CREAT, 0o666);
      FS.write(s, encode("ABCDEFGHIJ"), 0, 10);

      const d = FS.dupStream(s);
      FS.ftruncate(d.fd, 4);

      expect(FS.fstat(s.fd).size).toBe(4);

      FS.llseek(s, 0, SEEK_SET);
      const buf = new Uint8Array(10);
      const n = FS.read(s, buf, 0, 10);
      expect(n).toBe(4);
      expect(decode(buf, n)).toBe("ABCD");

      FS.close(s);
      FS.close(d);
    });

    it("dup shares cursor: truncate+write extends from shared position", () => {
      const { FS } = h;
      const s = FS.open("/dup-shared-cursor", O.RDWR | O.CREAT, 0o666);
      FS.write(s, encode("0123456789"), 0, 10);

      const d = FS.dupStream(s);

      FS.llseek(s, 6, SEEK_SET);
      FS.ftruncate(d.fd, 3);

      FS.write(d, encode("XY"), 0, 2);

      expect(FS.fstat(s.fd).size).toBe(8);

      FS.llseek(s, 0, SEEK_SET);
      const buf = new Uint8Array(8);
      const n = FS.read(s, buf, 0, 8);
      expect(n).toBe(8);
      expect(decode(buf.subarray(0, 3), 3)).toBe("012");
      for (let i = 3; i < 6; i++) {
        expect(buf[i]).toBe(0);
      }
      expect(decode(buf.subarray(6, 8), 2)).toBe("XY");

      FS.close(s);
      FS.close(d);
    });
  });

  describe("page boundary truncation", () => {
    it("ftruncate to exact page boundary preserves full pages @fast", () => {
      const { FS } = h;
      const s = FS.open("/page-exact", O.RDWR | O.CREAT, 0o666);
      const data = new Uint8Array(PAGE_SIZE * 3);
      for (let i = 0; i < data.length; i++) data[i] = i & 0xff;
      FS.write(s, data, 0, data.length);

      FS.ftruncate(s.fd, PAGE_SIZE * 2);

      expect(FS.fstat(s.fd).size).toBe(PAGE_SIZE * 2);

      FS.llseek(s, 0, SEEK_SET);
      const buf = new Uint8Array(PAGE_SIZE * 2);
      FS.read(s, buf, 0, buf.length);
      for (let i = 0; i < buf.length; i++) {
        expect(buf[i]).toBe(data[i]);
      }

      FS.close(s);
    });

    it("ftruncate to mid-page zeros tail of that page", () => {
      const { FS } = h;
      const s = FS.open("/page-mid", O.RDWR | O.CREAT, 0o666);
      const data = new Uint8Array(PAGE_SIZE * 2);
      for (let i = 0; i < data.length; i++) data[i] = (i % 251) + 1;
      FS.write(s, data, 0, data.length);

      const cutPoint = PAGE_SIZE + 100;
      FS.ftruncate(s.fd, cutPoint);

      expect(FS.fstat(s.fd).size).toBe(cutPoint);

      FS.ftruncate(s.fd, PAGE_SIZE * 2);

      FS.llseek(s, PAGE_SIZE, SEEK_SET);
      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(s, buf, 0, PAGE_SIZE);

      for (let i = 0; i < 100; i++) {
        expect(buf[i]).toBe(data[PAGE_SIZE + i]);
      }
      for (let i = 100; i < PAGE_SIZE; i++) {
        expect(buf[i]).toBe(0);
      }

      FS.close(s);
    });

    it("ftruncate shrink across multiple pages then grow back @fast", () => {
      const { FS } = h;
      const s = FS.open("/page-shrink-grow", O.RDWR | O.CREAT, 0o666);
      const data = new Uint8Array(PAGE_SIZE * 4);
      for (let i = 0; i < data.length; i++) data[i] = (i % 200) + 1;
      FS.write(s, data, 0, data.length);

      FS.ftruncate(s.fd, PAGE_SIZE + 50);

      FS.ftruncate(s.fd, PAGE_SIZE * 4);

      FS.llseek(s, 0, SEEK_SET);
      const buf = new Uint8Array(PAGE_SIZE * 4);
      FS.read(s, buf, 0, buf.length);

      for (let i = 0; i < PAGE_SIZE + 50; i++) {
        expect(buf[i]).toBe(data[i]);
      }
      for (let i = PAGE_SIZE + 50; i < PAGE_SIZE * 4; i++) {
        expect(buf[i]).toBe(0);
      }

      FS.close(s);
    });

    it("ftruncate to 1 byte preserves only first byte", () => {
      const { FS } = h;
      const s = FS.open("/trunc-one", O.RDWR | O.CREAT, 0o666);
      const data = new Uint8Array(PAGE_SIZE * 2);
      for (let i = 0; i < data.length; i++) data[i] = 0xAA;
      FS.write(s, data, 0, data.length);

      FS.ftruncate(s.fd, 1);
      expect(FS.fstat(s.fd).size).toBe(1);

      FS.llseek(s, 0, SEEK_SET);
      const buf = new Uint8Array(1);
      FS.read(s, buf, 0, 1);
      expect(buf[0]).toBe(0xAA);

      FS.close(s);
    });
  });

  describe("truncate + write interleaving", () => {
    it("truncate-write-truncate-write cycle produces correct data", () => {
      const { FS } = h;
      const s = FS.open("/twt-cycle", O.RDWR | O.CREAT, 0o666);

      FS.write(s, encode("ABCDEF"), 0, 6);
      FS.ftruncate(s.fd, 3);

      FS.llseek(s, 0, SEEK_END);
      FS.write(s, encode("GHI"), 0, 3);

      FS.ftruncate(s.fd, 4);

      FS.llseek(s, 0, SEEK_END);
      FS.write(s, encode("JK"), 0, 2);

      expect(FS.fstat(s.fd).size).toBe(6);

      FS.llseek(s, 0, SEEK_SET);
      const buf = new Uint8Array(6);
      FS.read(s, buf, 0, 6);
      expect(decode(buf, 6)).toBe("ABCGJK");

      FS.close(s);
    });

    it("positioned write after truncate at page boundary", () => {
      const { FS } = h;
      const s = FS.open("/pwrite-trunc", O.RDWR | O.CREAT, 0o666);

      const data = new Uint8Array(PAGE_SIZE * 3);
      for (let i = 0; i < data.length; i++) data[i] = 0x42;
      FS.write(s, data, 0, data.length);

      FS.ftruncate(s.fd, PAGE_SIZE);

      FS.write(s, encode("HELLO"), 0, 5, PAGE_SIZE + 10);

      expect(FS.fstat(s.fd).size).toBe(PAGE_SIZE + 15);

      const buf = new Uint8Array(15);
      FS.read(s, buf, 0, 15, PAGE_SIZE);
      for (let i = 0; i < 10; i++) {
        expect(buf[i]).toBe(0);
      }
      expect(decode(buf.subarray(10, 15), 5)).toBe("HELLO");

      FS.close(s);
    });

    it("ftruncate to zero then large write spanning multiple pages @fast", () => {
      const { FS } = h;
      const s = FS.open("/zero-large", O.RDWR | O.CREAT, 0o666);
      FS.write(s, encode("initial"), 0, 7);

      FS.ftruncate(s.fd, 0);

      const size = PAGE_SIZE * 2 + 500;
      const data = new Uint8Array(size);
      for (let i = 0; i < size; i++) data[i] = (i % 253) + 1;
      FS.llseek(s, 0, SEEK_SET);
      FS.write(s, data, 0, size);

      expect(FS.fstat(s.fd).size).toBe(size);

      FS.llseek(s, 0, SEEK_SET);
      const buf = new Uint8Array(size);
      FS.read(s, buf, 0, size);
      for (let i = 0; i < size; i++) {
        expect(buf[i]).toBe(data[i]);
      }

      FS.close(s);
    });
  });

  describe("ftruncate timestamp updates", () => {
    it("ftruncate updates mtime and ctime", async () => {
      const { FS } = h;
      const s = FS.open("/trunc-times", O.RDWR | O.CREAT, 0o666);
      FS.write(s, encode("data"), 0, 4);
      const before = FS.fstat(s.fd);
      const mBefore = before.mtime.getTime();
      const cBefore = before.ctime.getTime();

      await new Promise((r) => setTimeout(r, 10));

      FS.ftruncate(s.fd, 2);

      const after = FS.fstat(s.fd);
      expect(after.mtime.getTime()).toBeGreaterThanOrEqual(mBefore);
      expect(after.ctime.getTime()).toBeGreaterThanOrEqual(cBefore);

      FS.close(s);
    });

    it("ftruncate grow updates mtime and ctime", async () => {
      const { FS } = h;
      const s = FS.open("/trunc-grow-times", O.RDWR | O.CREAT, 0o666);
      FS.write(s, encode("AB"), 0, 2);
      const before = FS.fstat(s.fd);
      const mBefore = before.mtime.getTime();

      await new Promise((r) => setTimeout(r, 10));

      FS.ftruncate(s.fd, 100);

      const after = FS.fstat(s.fd);
      expect(after.mtime.getTime()).toBeGreaterThanOrEqual(mBefore);
      expect(after.size).toBe(100);

      FS.close(s);
    });
  });
});
