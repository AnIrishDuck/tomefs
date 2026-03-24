/**
 * Adversarial differential tests: Page boundary operations.
 *
 * These tests are designed to break tomefs specifically — they pass against
 * MEMFS but target seams in the page cache layer. Every test here should
 * work identically on MEMFS and tomefs; any divergence is a bug.
 *
 * Focus: reads and writes that span page boundaries (PAGE_SIZE = 8192).
 */
import {
  createFS,
  encode,
  decode,
  O,
  SEEK_SET,
  type FSHarness,
} from "../harness/emscripten-fs.js";

/** Page size used by tomefs — tests target this boundary. */
const PAGE_SIZE = 8192;

describe("adversarial: page boundary operations", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  // ------------------------------------------------------------------
  // Single-byte straddle: write across exact page boundary
  // ------------------------------------------------------------------

  it("write straddling a page boundary preserves all bytes @fast", () => {
    const { FS } = h;
    const stream = FS.open("/boundary", O.RDWR | O.CREAT, 0o666);

    // Write a pattern that straddles byte 8191–8192 (page 0/1 boundary)
    const data = new Uint8Array(4);
    data[0] = 0xde;
    data[1] = 0xad;
    data[2] = 0xbe;
    data[3] = 0xef;
    FS.write(stream, data, 0, 4, PAGE_SIZE - 2); // pos 8190..8193

    // Read back and verify
    const buf = new Uint8Array(4);
    const n = FS.read(stream, buf, 0, 4, PAGE_SIZE - 2);
    expect(n).toBe(4);
    expect(buf[0]).toBe(0xde);
    expect(buf[1]).toBe(0xad);
    expect(buf[2]).toBe(0xbe);
    expect(buf[3]).toBe(0xef);

    FS.close(stream);
  });

  it("read straddling page boundary after seek returns correct data", () => {
    const { FS } = h;
    const stream = FS.open("/boundary2", O.RDWR | O.CREAT, 0o666);

    // Fill two pages with distinct patterns
    const page0 = new Uint8Array(PAGE_SIZE);
    const page1 = new Uint8Array(PAGE_SIZE);
    page0.fill(0xaa);
    page1.fill(0xbb);
    FS.write(stream, page0, 0, PAGE_SIZE, 0);
    FS.write(stream, page1, 0, PAGE_SIZE, PAGE_SIZE);

    // Read 8 bytes straddling the boundary
    const buf = new Uint8Array(8);
    const n = FS.read(stream, buf, 0, 8, PAGE_SIZE - 4);
    expect(n).toBe(8);
    // First 4 bytes from page 0
    expect(buf[0]).toBe(0xaa);
    expect(buf[1]).toBe(0xaa);
    expect(buf[2]).toBe(0xaa);
    expect(buf[3]).toBe(0xaa);
    // Last 4 bytes from page 1
    expect(buf[4]).toBe(0xbb);
    expect(buf[5]).toBe(0xbb);
    expect(buf[6]).toBe(0xbb);
    expect(buf[7]).toBe(0xbb);

    FS.close(stream);
  });

  // ------------------------------------------------------------------
  // Multi-page spanning write
  // ------------------------------------------------------------------

  it("single write spanning 3 pages preserves all data", () => {
    const { FS } = h;
    const stream = FS.open("/span3", O.RDWR | O.CREAT, 0o666);

    // Write starting at offset 4096 (mid-page-0), spanning into page 2
    const size = PAGE_SIZE * 2;
    const data = new Uint8Array(size);
    for (let i = 0; i < size; i++) {
      data[i] = i & 0xff;
    }
    const startPos = PAGE_SIZE / 2; // 4096
    FS.write(stream, data, 0, size, startPos);

    // Read back
    const buf = new Uint8Array(size);
    const n = FS.read(stream, buf, 0, size, startPos);
    expect(n).toBe(size);
    for (let i = 0; i < size; i++) {
      expect(buf[i]).toBe(data[i]);
    }

    FS.close(stream);
  });

  // ------------------------------------------------------------------
  // Write at exact page boundary offset
  // ------------------------------------------------------------------

  it("write starting at exact page boundary", () => {
    const { FS } = h;
    const stream = FS.open("/exact", O.RDWR | O.CREAT, 0o666);

    // Write page 0
    const page0 = new Uint8Array(PAGE_SIZE);
    page0.fill(0x11);
    FS.write(stream, page0, 0, PAGE_SIZE, 0);

    // Write starting exactly at page 1
    const data = new Uint8Array(100);
    data.fill(0x22);
    FS.write(stream, data, 0, 100, PAGE_SIZE);

    // Verify page 0 untouched
    const buf0 = new Uint8Array(PAGE_SIZE);
    FS.read(stream, buf0, 0, PAGE_SIZE, 0);
    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(buf0[i]).toBe(0x11);
    }

    // Verify page 1 data
    const buf1 = new Uint8Array(100);
    FS.read(stream, buf1, 0, 100, PAGE_SIZE);
    for (let i = 0; i < 100; i++) {
      expect(buf1[i]).toBe(0x22);
    }

    FS.close(stream);
  });

  // ------------------------------------------------------------------
  // Partial page write then full page read
  // ------------------------------------------------------------------

  it("partial write into page leaves zero-fill in unwritten region", () => {
    const { FS } = h;
    const stream = FS.open("/partial", O.RDWR | O.CREAT, 0o666);

    // Write 100 bytes at offset 50 within page 0
    const data = new Uint8Array(100);
    data.fill(0xff);
    FS.write(stream, data, 0, 100, 50);

    // File size should be 150
    const stat = FS.stat("/partial");
    expect(stat.size).toBe(150);

    // Read from 0: first 50 bytes should be zero
    const buf = new Uint8Array(150);
    FS.llseek(stream, 0, SEEK_SET);
    const n = FS.read(stream, buf, 0, 150);
    expect(n).toBe(150);
    for (let i = 0; i < 50; i++) {
      expect(buf[i]).toBe(0);
    }
    for (let i = 50; i < 150; i++) {
      expect(buf[i]).toBe(0xff);
    }

    FS.close(stream);
  });

  // ------------------------------------------------------------------
  // Write gap creates zero-filled hole
  // ------------------------------------------------------------------

  it("write beyond EOF creates zero-filled gap", () => {
    const { FS } = h;
    const stream = FS.open("/gap", O.RDWR | O.CREAT, 0o666);

    // Write 10 bytes at offset 0
    const first = encode("0123456789");
    FS.write(stream, first, 0, 10, 0);

    // Write 10 bytes at offset PAGE_SIZE + 100 (creating a gap spanning page boundary)
    const second = encode("abcdefghij");
    FS.write(stream, second, 0, 10, PAGE_SIZE + 100);

    // Read the gap region — should be zeros
    const gapBuf = new Uint8Array(PAGE_SIZE + 100 - 10);
    const n = FS.read(stream, gapBuf, 0, gapBuf.length, 10);
    expect(n).toBe(gapBuf.length);
    for (let i = 0; i < gapBuf.length; i++) {
      expect(gapBuf[i]).toBe(0);
    }

    // Read back the second write
    const readBack = new Uint8Array(10);
    FS.read(stream, readBack, 0, 10, PAGE_SIZE + 100);
    expect(decode(readBack, 10)).toBe("abcdefghij");

    FS.close(stream);
  });

  // ------------------------------------------------------------------
  // Byte-level verification across many page boundaries
  // ------------------------------------------------------------------

  it("sequential write/read across 8 pages with unique byte pattern", () => {
    const { FS } = h;
    const stream = FS.open("/multipage", O.RDWR | O.CREAT, 0o666);

    const totalSize = PAGE_SIZE * 8;
    const data = new Uint8Array(totalSize);
    // Each byte is unique based on position
    for (let i = 0; i < totalSize; i++) {
      data[i] = (i * 7 + 13) & 0xff;
    }
    FS.write(stream, data, 0, totalSize, 0);

    // Read back in chunks that DON'T align to page boundaries
    const chunkSize = PAGE_SIZE - 1; // deliberately misaligned
    let pos = 0;
    while (pos < totalSize) {
      const toRead = Math.min(chunkSize, totalSize - pos);
      const buf = new Uint8Array(toRead);
      const n = FS.read(stream, buf, 0, toRead, pos);
      expect(n).toBe(toRead);
      for (let i = 0; i < toRead; i++) {
        expect(buf[i]).toBe(data[pos + i]);
      }
      pos += toRead;
    }

    FS.close(stream);
  });

  // ------------------------------------------------------------------
  // Overwrite spanning page boundary preserves adjacent data
  // ------------------------------------------------------------------

  it("overwrite at page boundary preserves adjacent bytes", () => {
    const { FS } = h;
    const stream = FS.open("/overwrite", O.RDWR | O.CREAT, 0o666);

    // Fill 2 pages with 0xCC
    const fill = new Uint8Array(PAGE_SIZE * 2);
    fill.fill(0xcc);
    FS.write(stream, fill, 0, fill.length, 0);

    // Overwrite 2 bytes at the page boundary
    const patch = new Uint8Array([0x01, 0x02]);
    FS.write(stream, patch, 0, 2, PAGE_SIZE - 1);

    // Read full file and verify
    const buf = new Uint8Array(PAGE_SIZE * 2);
    FS.read(stream, buf, 0, buf.length, 0);

    // Before the patch: all 0xCC
    for (let i = 0; i < PAGE_SIZE - 1; i++) {
      expect(buf[i]).toBe(0xcc);
    }
    // The patch
    expect(buf[PAGE_SIZE - 1]).toBe(0x01);
    expect(buf[PAGE_SIZE]).toBe(0x02);
    // After the patch: all 0xCC
    for (let i = PAGE_SIZE + 1; i < PAGE_SIZE * 2; i++) {
      expect(buf[i]).toBe(0xcc);
    }

    FS.close(stream);
  });
});
