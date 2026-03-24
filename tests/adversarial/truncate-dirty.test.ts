/**
 * Adversarial differential tests: Truncate with dirty pages.
 *
 * These tests exercise the interaction between truncate/extend and the
 * dirty page tracking in the page cache. The critical invariant: after
 * truncation, stale dirty pages beyond the new size must never be
 * flushed or served from cache.
 */
import {
  createFS,
  encode,
  decode,
  O,
  SEEK_SET,
  type FSHarness,
} from "../harness/emscripten-fs.js";

const PAGE_SIZE = 8192;

describe("adversarial: truncate with dirty pages", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  // ------------------------------------------------------------------
  // Truncate discards data beyond new size
  // ------------------------------------------------------------------

  it("truncate discards pages beyond new size @fast", () => {
    const { FS } = h;
    const stream = FS.open("/trunc1", O.RDWR | O.CREAT, 0o666);

    // Write 4 pages
    const data = new Uint8Array(PAGE_SIZE * 4);
    data.fill(0xff);
    FS.write(stream, data, 0, data.length, 0);

    // Truncate to 1 page
    FS.ftruncate(stream.fd, PAGE_SIZE);

    // File size should be 1 page
    const stat = FS.stat("/trunc1");
    expect(stat.size).toBe(PAGE_SIZE);

    // Read at the old page 2 position — should fail (beyond EOF)
    const buf = new Uint8Array(PAGE_SIZE);
    const n = FS.read(stream, buf, 0, PAGE_SIZE, PAGE_SIZE * 2);
    expect(n).toBe(0);

    FS.close(stream);
  });

  // ------------------------------------------------------------------
  // Truncate then extend: gap must be zero-filled
  // ------------------------------------------------------------------

  it("truncate then extend fills gap with zeros", () => {
    const { FS } = h;
    const stream = FS.open("/trunc-extend", O.RDWR | O.CREAT, 0o666);

    // Write 2 pages of 0xFF
    const data = new Uint8Array(PAGE_SIZE * 2);
    data.fill(0xff);
    FS.write(stream, data, 0, data.length, 0);

    // Truncate to half a page
    FS.ftruncate(stream.fd, PAGE_SIZE / 2);

    // Extend by writing at page 2
    const marker = encode("MARKER");
    FS.write(stream, marker, 0, marker.length, PAGE_SIZE * 2);

    // The gap (PAGE_SIZE/2 to PAGE_SIZE*2) should be zeros
    const gapSize = PAGE_SIZE * 2 - PAGE_SIZE / 2;
    const gapBuf = new Uint8Array(gapSize);
    const n = FS.read(stream, gapBuf, 0, gapSize, PAGE_SIZE / 2);
    expect(n).toBe(gapSize);
    for (let i = 0; i < gapSize; i++) {
      expect(gapBuf[i]).toBe(0);
    }

    // Marker should be readable
    const readMarker = new Uint8Array(marker.length);
    FS.read(stream, readMarker, 0, marker.length, PAGE_SIZE * 2);
    expect(decode(readMarker, marker.length)).toBe("MARKER");

    // Original data before truncation point should be preserved
    const preserved = new Uint8Array(PAGE_SIZE / 2);
    FS.read(stream, preserved, 0, PAGE_SIZE / 2, 0);
    for (let i = 0; i < PAGE_SIZE / 2; i++) {
      expect(preserved[i]).toBe(0xff);
    }

    FS.close(stream);
  });

  // ------------------------------------------------------------------
  // Truncate to zero then rebuild
  // ------------------------------------------------------------------

  it("truncate to zero then rebuild file from scratch", () => {
    const { FS } = h;
    const stream = FS.open("/rebuild", O.RDWR | O.CREAT, 0o666);

    // Write 8 pages of data
    const original = new Uint8Array(PAGE_SIZE * 8);
    for (let i = 0; i < original.length; i++) {
      original[i] = 0xaa;
    }
    FS.write(stream, original, 0, original.length, 0);

    // Truncate to zero
    FS.ftruncate(stream.fd, 0);
    expect(FS.stat("/rebuild").size).toBe(0);

    // Write new data — must not see any 0xAA ghosts
    const replacement = new Uint8Array(PAGE_SIZE * 2);
    replacement.fill(0x55);
    FS.write(stream, replacement, 0, replacement.length, 0);

    const buf = new Uint8Array(PAGE_SIZE * 2);
    FS.read(stream, buf, 0, buf.length, 0);
    for (let i = 0; i < buf.length; i++) {
      expect(buf[i]).toBe(0x55);
    }

    FS.close(stream);
  });

  // ------------------------------------------------------------------
  // Repeated truncate/grow cycles
  // ------------------------------------------------------------------

  it("repeated truncate/grow cycles maintain data integrity", () => {
    const { FS } = h;
    const stream = FS.open("/cycles", O.RDWR | O.CREAT, 0o666);

    for (let cycle = 0; cycle < 10; cycle++) {
      const fillByte = cycle & 0xff;
      const size = PAGE_SIZE * (2 + (cycle % 3));

      // Truncate to zero
      FS.ftruncate(stream.fd, 0);

      // Write new data
      const data = new Uint8Array(size);
      data.fill(fillByte);
      FS.write(stream, data, 0, size, 0);

      // Verify
      expect(FS.stat("/cycles").size).toBe(size);
      const buf = new Uint8Array(size);
      FS.read(stream, buf, 0, size, 0);
      for (let i = 0; i < size; i++) {
        if (buf[i] !== fillByte) {
          throw new Error(
            `Cycle ${cycle} byte ${i}: expected ${fillByte}, got ${buf[i]}`,
          );
        }
      }
    }

    FS.close(stream);
  });

  // ------------------------------------------------------------------
  // Truncate mid-page: tail of last page must be zeroed
  // ------------------------------------------------------------------

  it("truncate mid-page zeros the tail of the surviving page", () => {
    const { FS } = h;
    const stream = FS.open("/midpage", O.RDWR | O.CREAT, 0o666);

    // Write a full page of 0xFF
    const data = new Uint8Array(PAGE_SIZE);
    data.fill(0xff);
    FS.write(stream, data, 0, PAGE_SIZE, 0);

    // Truncate to 100 bytes
    FS.ftruncate(stream.fd, 100);

    // Extend back to full page by writing at end
    const tail = new Uint8Array(1);
    tail[0] = 0x42;
    FS.write(stream, tail, 0, 1, PAGE_SIZE - 1);

    // Bytes 0-99: should still be 0xFF
    const head = new Uint8Array(100);
    FS.read(stream, head, 0, 100, 0);
    for (let i = 0; i < 100; i++) {
      expect(head[i]).toBe(0xff);
    }

    // Bytes 100 to PAGE_SIZE-2: should be zero (truncated region)
    const mid = new Uint8Array(PAGE_SIZE - 2 - 100);
    FS.read(stream, mid, 0, mid.length, 100);
    for (let i = 0; i < mid.length; i++) {
      if (mid[i] !== 0) {
        throw new Error(
          `Byte ${100 + i} should be 0 after truncate, got ${mid[i]}`,
        );
      }
    }

    // Last byte: our marker
    const lastBuf = new Uint8Array(1);
    FS.read(stream, lastBuf, 0, 1, PAGE_SIZE - 1);
    expect(lastBuf[0]).toBe(0x42);

    FS.close(stream);
  });

  // ------------------------------------------------------------------
  // Truncate while file has data across many pages
  // ------------------------------------------------------------------

  it("truncate large file to mid-page boundary preserves prefix", () => {
    const { FS } = h;
    const stream = FS.open("/largetrunc", O.RDWR | O.CREAT, 0o666);

    // Write 32 pages with position-dependent data
    const totalSize = PAGE_SIZE * 32;
    const data = new Uint8Array(totalSize);
    for (let i = 0; i < totalSize; i++) {
      data[i] = (i * 13 + 5) & 0xff;
    }
    FS.write(stream, data, 0, totalSize, 0);

    // Truncate to 2.5 pages
    const truncSize = PAGE_SIZE * 2 + PAGE_SIZE / 2;
    FS.ftruncate(stream.fd, truncSize);

    expect(FS.stat("/largetrunc").size).toBe(truncSize);

    // Verify preserved data
    const buf = new Uint8Array(truncSize);
    const n = FS.read(stream, buf, 0, truncSize, 0);
    expect(n).toBe(truncSize);
    for (let i = 0; i < truncSize; i++) {
      const expected = (i * 13 + 5) & 0xff;
      if (buf[i] !== expected) {
        throw new Error(
          `Byte ${i}: expected ${expected}, got ${buf[i]}`,
        );
      }
    }

    FS.close(stream);
  });
});
