/**
 * Conformance tests for dup + ftruncate interactions.
 *
 * POSIX requires that dup'd file descriptors share the same underlying
 * file description (open file table entry). This means ftruncate through
 * any fd immediately affects all dup'd fds — file size changes are
 * visible through fstat on any fd, and read/write positions beyond the
 * new EOF see zero-filled gaps (extend) or truncated data (shrink).
 *
 * These interactions matter for Postgres: it uses dup'd fds for WAL
 * writes, relation extension, and vacuum. A ftruncate through one fd
 * that isn't visible through a dup'd fd would corrupt the database.
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
  type FSHarness,
} from "../harness/emscripten-fs.js";
import { PAGE_SIZE } from "../../src/types.js";

describe("dup + ftruncate interactions", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  it("ftruncate to zero through dup'd fd empties file @fast", () => {
    const { FS } = h;
    const stream = FS.open("/dft_zero", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("Hello, World!"), 0, 13);
    const dup = FS.dupStream(stream);

    FS.ftruncate(dup.fd, 0);

    expect(FS.fstat(stream.fd).size).toBe(0);
    expect(FS.fstat(dup.fd).size).toBe(0);

    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(16);
    const n = FS.read(stream, buf, 0, buf.length);
    expect(n).toBe(0);

    FS.close(dup);
    FS.close(stream);
  });

  it("ftruncate-as-extend through dup'd fd grows file with zeros @fast", () => {
    const { FS } = h;
    const stream = FS.open("/dft_extend", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("ABC"), 0, 3);
    const dup = FS.dupStream(stream);

    FS.ftruncate(dup.fd, 10);

    expect(FS.fstat(stream.fd).size).toBe(10);
    expect(FS.fstat(dup.fd).size).toBe(10);

    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(10);
    const n = FS.read(stream, buf, 0, 10);
    expect(n).toBe(10);
    expect(decode(buf.subarray(0, 3))).toBe("ABC");
    for (let i = 3; i < 10; i++) {
      expect(buf[i]).toBe(0);
    }

    FS.close(dup);
    FS.close(stream);
  });

  it("alternating shrink/grow through different dup'd fds @fast", () => {
    const { FS } = h;
    const stream = FS.open("/dft_cycle", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("0123456789"), 0, 10);
    const dup = FS.dupStream(stream);

    // Shrink via dup
    FS.ftruncate(dup.fd, 5);
    expect(FS.fstat(stream.fd).size).toBe(5);

    // Grow via original
    FS.ftruncate(stream.fd, 8);
    expect(FS.fstat(dup.fd).size).toBe(8);

    // Shrink via dup again
    FS.ftruncate(dup.fd, 2);
    expect(FS.fstat(stream.fd).size).toBe(2);

    // Read back through original — should see "01"
    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(10);
    const n = FS.read(stream, buf, 0, 10);
    expect(n).toBe(2);
    expect(decode(buf, n)).toBe("01");

    FS.close(dup);
    FS.close(stream);
  });

  it("ftruncate through dup'd fd after original is closed @fast", () => {
    const { FS } = h;
    const stream = FS.open("/dft_close_orig", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("ABCDEFGHIJ"), 0, 10);
    const dup = FS.dupStream(stream);

    FS.close(stream);

    // Truncate through surviving dup
    FS.ftruncate(dup.fd, 4);
    expect(FS.fstat(dup.fd).size).toBe(4);

    // Read back through dup
    FS.llseek(dup, 0, SEEK_SET);
    const buf = new Uint8Array(10);
    const n = FS.read(dup, buf, 0, 10);
    expect(n).toBe(4);
    expect(decode(buf, n)).toBe("ABCD");

    FS.close(dup);
  });

  it("ftruncate through dup'd fd at exact page boundary @fast", () => {
    const { FS } = h;
    const stream = FS.open("/dft_page", O.RDWR | O.CREAT, 0o666);

    // Write 2 full pages of data
    const data = new Uint8Array(PAGE_SIZE * 2);
    for (let i = 0; i < data.length; i++) data[i] = (i & 0xff) || 1;
    FS.write(stream, data, 0, data.length);

    const dup = FS.dupStream(stream);

    // Truncate to exactly 1 page via dup
    FS.ftruncate(dup.fd, PAGE_SIZE);
    expect(FS.fstat(stream.fd).size).toBe(PAGE_SIZE);

    // Read first page through original — should be intact
    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(PAGE_SIZE);
    const n = FS.read(stream, buf, 0, PAGE_SIZE);
    expect(n).toBe(PAGE_SIZE);
    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(buf[i]).toBe(data[i]);
    }

    // Attempt to read second page — should return 0 bytes
    const buf2 = new Uint8Array(PAGE_SIZE);
    const n2 = FS.read(stream, buf2, 0, PAGE_SIZE);
    expect(n2).toBe(0);

    FS.close(dup);
    FS.close(stream);
  });

  it("extend through dup'd fd past page boundary then read via original @fast", () => {
    const { FS } = h;
    const stream = FS.open("/dft_extend_page", O.RDWR | O.CREAT, 0o666);
    const initial = new Uint8Array(100);
    for (let i = 0; i < 100; i++) initial[i] = 0x42;
    FS.write(stream, initial, 0, 100);

    const dup = FS.dupStream(stream);

    // Extend past page boundary through dup
    FS.ftruncate(dup.fd, PAGE_SIZE + 512);
    expect(FS.fstat(stream.fd).size).toBe(PAGE_SIZE + 512);

    // Read through original — first 100 bytes should be 0x42, rest zeros
    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(PAGE_SIZE + 512);
    const n = FS.read(stream, buf, 0, buf.length);
    expect(n).toBe(PAGE_SIZE + 512);
    for (let i = 0; i < 100; i++) {
      expect(buf[i]).toBe(0x42);
    }
    for (let i = 100; i < PAGE_SIZE + 512; i++) {
      expect(buf[i]).toBe(0);
    }

    FS.close(dup);
    FS.close(stream);
  });

  it("interleaved write + ftruncate through dup'd fds @fast", () => {
    const { FS } = h;
    const stream = FS.open("/dft_interleave", O.RDWR | O.CREAT, 0o666);
    const dup = FS.dupStream(stream);

    // Write through original
    FS.write(stream, encode("ABCDEFGHIJ"), 0, 10);

    // Truncate through dup to 5
    FS.ftruncate(dup.fd, 5);

    // Write through dup at current position (shared, still at 10)
    // Writing at position 10 when file is 5 creates a gap
    FS.write(dup, encode("XY"), 0, 2);
    expect(FS.fstat(stream.fd).size).toBe(12);

    // Read full file through original
    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(12);
    const n = FS.read(stream, buf, 0, 12);
    expect(n).toBe(12);
    // First 5 bytes: "ABCDE" (survived truncation)
    expect(decode(buf.subarray(0, 5))).toBe("ABCDE");
    // Bytes 5-9: zero gap (created by write at position 10 after truncate to 5)
    for (let i = 5; i < 10; i++) {
      expect(buf[i]).toBe(0);
    }
    // Bytes 10-11: "XY"
    expect(decode(buf.subarray(10, 12))).toBe("XY");

    FS.close(dup);
    FS.close(stream);
  });

  it("ftruncate through dup after write does not affect cursor @fast", () => {
    const { FS } = h;
    const stream = FS.open("/dft_cursor", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("0123456789"), 0, 10);
    const dup = FS.dupStream(stream);

    // Seek original to position 4
    FS.llseek(stream, 4, SEEK_SET);

    // Truncate to 3 through dup — cursor (at 4) is now beyond EOF
    FS.ftruncate(dup.fd, 3);

    // Read from original at position 4 — should return 0 bytes (past EOF)
    const buf = new Uint8Array(10);
    const n = FS.read(stream, buf, 0, 10);
    expect(n).toBe(0);

    // Write from original at position 4 — extends file with zero gap at byte 3
    FS.write(stream, encode("AB"), 0, 2);
    expect(FS.fstat(dup.fd).size).toBe(6);

    // Read full file from dup
    FS.llseek(dup, 0, SEEK_SET);
    const full = new Uint8Array(6);
    const n2 = FS.read(dup, full, 0, 6);
    expect(n2).toBe(6);
    expect(decode(full.subarray(0, 3))).toBe("012");
    expect(full[3]).toBe(0);
    expect(decode(full.subarray(4, 6))).toBe("AB");

    FS.close(dup);
    FS.close(stream);
  });

  it("ftruncate through dup on unlinked file @fast", () => {
    const { FS } = h;
    const stream = FS.open("/dft_unlink", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("Hello World"), 0, 11);
    const dup = FS.dupStream(stream);

    // Unlink the file — fds remain valid
    FS.unlink("/dft_unlink");

    // Truncate through dup
    FS.ftruncate(dup.fd, 5);
    expect(FS.fstat(stream.fd).size).toBe(5);

    // Read back through original
    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(11);
    const n = FS.read(stream, buf, 0, 11);
    expect(n).toBe(5);
    expect(decode(buf, n)).toBe("Hello");

    FS.close(dup);
    FS.close(stream);
  });

  it("ftruncate multi-page shrink through dup preserves first page @fast", () => {
    const { FS } = h;
    const stream = FS.open("/dft_multi", O.RDWR | O.CREAT, 0o666);

    // Write data spanning 3 pages
    const data = new Uint8Array(PAGE_SIZE * 3);
    for (let i = 0; i < data.length; i++) data[i] = ((i % 253) + 1) & 0xff;
    FS.write(stream, data, 0, data.length);

    const dup = FS.dupStream(stream);

    // Truncate to middle of first page through dup
    const truncSize = PAGE_SIZE / 2;
    FS.ftruncate(dup.fd, truncSize);
    expect(FS.fstat(stream.fd).size).toBe(truncSize);

    // Verify first half of first page is intact
    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(truncSize);
    const n = FS.read(stream, buf, 0, truncSize);
    expect(n).toBe(truncSize);
    for (let i = 0; i < truncSize; i++) {
      expect(buf[i]).toBe(data[i]);
    }

    FS.close(dup);
    FS.close(stream);
  });

  it("extend via dup then shrink via original then read via dup @fast", () => {
    const { FS } = h;
    const stream = FS.open("/dft_ext_shrink", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("DATA"), 0, 4);
    const dup = FS.dupStream(stream);

    // Extend to 20 bytes through dup
    FS.ftruncate(dup.fd, 20);
    expect(FS.fstat(stream.fd).size).toBe(20);

    // Shrink to 6 bytes through original
    FS.ftruncate(stream.fd, 6);
    expect(FS.fstat(dup.fd).size).toBe(6);

    // Read through dup
    FS.llseek(dup, 0, SEEK_SET);
    const buf = new Uint8Array(10);
    const n = FS.read(dup, buf, 0, 10);
    expect(n).toBe(6);
    expect(decode(buf.subarray(0, 4))).toBe("DATA");
    expect(buf[4]).toBe(0);
    expect(buf[5]).toBe(0);

    FS.close(dup);
    FS.close(stream);
  });

  it("ftruncate through third dup affects all fds @fast", () => {
    const { FS } = h;
    const fd1 = FS.open("/dft_three", O.RDWR | O.CREAT, 0o666);
    FS.write(fd1, encode("ABCDEFGHIJ"), 0, 10);
    const fd2 = FS.dupStream(fd1);
    const fd3 = FS.dupStream(fd2);

    // Truncate through third dup
    FS.ftruncate(fd3.fd, 3);
    expect(FS.fstat(fd1.fd).size).toBe(3);
    expect(FS.fstat(fd2.fd).size).toBe(3);
    expect(FS.fstat(fd3.fd).size).toBe(3);

    // Read through first fd
    FS.llseek(fd1, 0, SEEK_SET);
    const buf = new Uint8Array(10);
    const n = FS.read(fd1, buf, 0, 10);
    expect(n).toBe(3);
    expect(decode(buf, n)).toBe("ABC");

    FS.close(fd3);
    FS.close(fd2);
    FS.close(fd1);
  });
});
