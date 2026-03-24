/**
 * Conformance tests ported from: emscripten/test/wasmfs/wasmfs_seek.c
 *
 * Tests: read, write, lseek (SEEK_SET/CUR/END), pread, pwrite,
 *        seek past end, negative seek, write-after-seek gaps.
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

describe("seek (wasmfs_seek.c)", () => {
  let h: FSHarness;
  let stream: ReturnType<FSHarness["FS"]["open"]>;

  beforeEach(async () => {
    h = await createFS();
    const { FS } = h;
    // Create file with "1234567890"
    stream = FS.open("/file", O.RDWR | O.CREAT);
    const msg = encode("1234567890");
    FS.write(stream, msg, 0, msg.length);
    FS.llseek(stream, 0, SEEK_SET);
  });

  it("reads entire file after write @fast", () => {
    const { FS } = h;
    const buf = new Uint8Array(256);
    const n = FS.read(stream, buf, 0, buf.length);
    expect(n).toBe(10);
    expect(decode(buf, n)).toBe("1234567890");
  });

  it("pread past end of file returns 0 bytes", () => {
    const { FS } = h;
    const buf = new Uint8Array(256);
    const n = FS.read(stream, buf, 0, buf.length, 999999999);
    expect(n).toBe(0);
  });

  it("SEEK_SET to offset 3, then partial read", () => {
    const { FS } = h;
    const pos = FS.llseek(stream, 3, SEEK_SET);
    expect(pos).toBe(3);

    const buf = new Uint8Array(3);
    const n = FS.read(stream, buf, 0, 3);
    expect(n).toBe(3);
    expect(decode(buf, n)).toBe("456");
  });

  it("SEEK_END with negative offset", () => {
    const { FS } = h;
    const pos = FS.llseek(stream, -2, SEEK_END);
    expect(pos).toBe(8);

    const buf = new Uint8Array(3);
    const n = FS.read(stream, buf, 0, 3);
    expect(n).toBe(2); // only 2 bytes left
    expect(decode(buf, n)).toBe("90");
  });

  it("negative SEEK_CUR that would go before start fails", () => {
    const { FS } = h;
    // Read to end first
    const buf = new Uint8Array(256);
    FS.read(stream, buf, 0, buf.length);
    const posBefore = FS.llseek(stream, 0, SEEK_CUR);
    expect(posBefore).toBe(10);

    // Try to seek -15 from current (10) = -5, which is invalid
    expect(() => FS.llseek(stream, -15, SEEK_CUR)).toThrow();

    // Position should be unchanged after failed seek
    const posAfter = FS.llseek(stream, 0, SEEK_CUR);
    expect(posAfter).toBe(10);
  });

  it("write to start of file (overwrite)", () => {
    const { FS } = h;
    FS.llseek(stream, 0, SEEK_SET);
    const data = encode("wri");
    FS.write(stream, data, 0, 3);

    // Read entire file
    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(256);
    const n = FS.read(stream, buf, 0, buf.length);
    expect(decode(buf, n)).toBe("wri4567890");
  });

  it("write to end of file (append)", () => {
    const { FS } = h;
    const pos = FS.llseek(stream, 0, SEEK_END);
    expect(pos).toBe(10);

    const data = encode("wri");
    FS.write(stream, data, 0, 3);

    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(256);
    const n = FS.read(stream, buf, 0, buf.length);
    expect(decode(buf, n)).toBe("1234567890wri");
  });

  it("write after gap (seek past end) fills with zeros", () => {
    const { FS } = h;
    // Seek 10 bytes past end
    FS.llseek(stream, 10, SEEK_END);
    // position is now at 20

    const data = encode("writeme\0"); // 8 bytes including null
    FS.write(stream, data, 0, data.length);

    // File should be: "1234567890" + 10 zero bytes + "writeme\0"
    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(256);
    const n = FS.read(stream, buf, 0, buf.length);
    expect(n).toBe(28); // 10 + 10 + 8

    // Original data intact
    expect(decode(buf, 10)).toBe("1234567890");

    // Gap should be zeros
    for (let i = 10; i < 20; i++) {
      expect(buf[i]).toBe(0);
    }

    // Written data
    expect(decode(buf.subarray(20), 7)).toBe("writeme");
  });

  it("pwrite to middle of file does not change stream position", () => {
    const { FS } = h;
    // First extend file: seek past end and write
    FS.llseek(stream, 10, SEEK_END);
    const data = encode("writeme\0");
    FS.write(stream, data, 0, data.length);
    // Position is now 28

    const posBefore = FS.llseek(stream, 0, SEEK_CUR);

    // pwrite "ite" at position 17
    const pdata = encode("ite");
    FS.write(stream, pdata, 0, 3, 17);

    // Position should be unchanged
    const posAfter = FS.llseek(stream, 0, SEEK_CUR);
    expect(posAfter).toBe(posBefore);
  });

  it("pwrite past end of file extends the file", () => {
    const { FS } = h;
    const data = encode("write");
    FS.write(stream, data, 0, 5, 32);

    // File size should now be 37
    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(256);
    const n = FS.read(stream, buf, 0, buf.length);
    expect(n).toBe(37);

    // Bytes 10-31 should be zero (gap)
    for (let i = 10; i < 32; i++) {
      expect(buf[i]).toBe(0);
    }

    // Bytes 32-36 should be "write"
    expect(decode(buf.subarray(32), 5)).toBe("write");
  });

  it("combined read/write sequence produces correct final content", () => {
    const { FS } = h;

    // Overwrite start: "wri" at 0
    FS.llseek(stream, 0, SEEK_SET);
    FS.write(stream, encode("wri"), 0, 3);

    // Append at end
    FS.llseek(stream, 0, SEEK_END);
    FS.write(stream, encode("wri"), 0, 3);

    // Write past end with 10 byte gap
    FS.llseek(stream, 10, SEEK_END);
    FS.write(stream, encode("writeme\0"), 0, 8);

    // pwrite "ite" at position 17
    FS.write(stream, encode("ite"), 0, 3, 17);

    // pwrite "write" at position 32
    FS.write(stream, encode("write"), 0, 5, 32);

    // Read everything
    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(256);
    const n = FS.read(stream, buf, 0, buf.length);
    expect(n).toBe(37);

    // Build expected content
    // Start: "wri4567890" (overwrite first 3 bytes)
    // Append "wri" → "wri4567890wri"
    // Gap of 10 zeros + "writeme\0" → total 31 bytes appended
    // pwrite "ite" at 17 → overwrites bytes 17-19
    // pwrite "write" at 32 → overwrites/extends bytes 32-36

    // Verify first 3 bytes
    expect(decode(buf, 3)).toBe("wri");
    // Bytes 3-9 unchanged
    expect(decode(buf.subarray(3), 7)).toBe("4567890");
  });
});
