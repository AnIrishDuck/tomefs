/**
 * Adversarial tests: Truncation + eviction + read interaction.
 *
 * These tests target the seam between truncation (which zeros page tails and
 * invalidates pages), cache eviction (which flushes dirty pages to the
 * backend), and subsequent reads (which may reload from the backend).
 *
 * The key invariants:
 * - After mid-page truncation, cached page tails must read as zeros
 * - After eviction and re-load, page data must match what was written
 * - Cross-page reads that span a cached + evicted boundary must stitch
 *   correctly after reloading the evicted page from the backend
 *
 * Ethos §9: "Target the seams: reads that span page boundaries, writes
 * during eviction, metadata updates after flush"
 */
import {
  createFS,
  O,
  SEEK_SET,
  type FSHarness,
} from "../harness/emscripten-fs.js";

const PAGE_SIZE = 8192;

/** Create a FS harness with a tiny 4-page cache. */
async function createSmallCacheFS(): Promise<FSHarness> {
  const origMaxPages = process.env.TOMEFS_MAX_PAGES;
  process.env.TOMEFS_MAX_PAGES = "4";
  try {
    return await createFS();
  } finally {
    if (origMaxPages !== undefined) {
      process.env.TOMEFS_MAX_PAGES = origMaxPages;
    } else {
      delete process.env.TOMEFS_MAX_PAGES;
    }
  }
}

describe("adversarial: truncation + eviction + read interaction", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createSmallCacheFS();
  });

  // ------------------------------------------------------------------
  // 1. Truncate mid-page then read the zeroed tail
  // ------------------------------------------------------------------

  it("truncate mid-page: read clamped to new file size", () => {
    const { FS } = h;
    const stream = FS.open("/trunc_read", O.RDWR | O.CREAT, 0o666);

    // Write a full page of 0xFF
    const fullPage = new Uint8Array(PAGE_SIZE);
    fullPage.fill(0xff);
    FS.write(stream, fullPage, 0, PAGE_SIZE, 0);

    // Truncate to 100 bytes (mid-page)
    FS.ftruncate(stream.fd, 100);

    // Read from position 0, length PAGE_SIZE — should return only 100 bytes
    const readBuf = new Uint8Array(PAGE_SIZE);
    const n = FS.read(stream, readBuf, 0, PAGE_SIZE, 0);
    expect(n).toBe(100);
    // First 100 bytes should be 0xFF
    for (let i = 0; i < 100; i++) {
      expect(readBuf[i]).toBe(0xff);
    }
    // Bytes beyond the read count should be untouched (zero from allocation)
    for (let i = 100; i < PAGE_SIZE; i++) {
      expect(readBuf[i]).toBe(0);
    }

    FS.close(stream);
  });

  it("truncate mid-page then extend: gap reads as zeros", () => {
    const { FS } = h;
    const stream = FS.open("/trunc_extend", O.RDWR | O.CREAT, 0o666);

    // Write a full page of 0xAA
    const fullPage = new Uint8Array(PAGE_SIZE);
    fullPage.fill(0xaa);
    FS.write(stream, fullPage, 0, PAGE_SIZE, 0);

    // Truncate to 100 bytes
    FS.ftruncate(stream.fd, 100);

    // Extend by writing at position 200
    const marker = new Uint8Array([0xBB, 0xCC, 0xDD]);
    FS.write(stream, marker, 0, 3, 200);

    // Read the gap region (bytes 100-199): should be zeros
    const gapBuf = new Uint8Array(100);
    const gapN = FS.read(stream, gapBuf, 0, 100, 100);
    expect(gapN).toBe(100);
    for (let i = 0; i < 100; i++) {
      expect(gapBuf[i]).toBe(0);
    }

    // Read the marker at position 200
    const markerBuf = new Uint8Array(3);
    const markerN = FS.read(stream, markerBuf, 0, 3, 200);
    expect(markerN).toBe(3);
    expect(markerBuf[0]).toBe(0xbb);
    expect(markerBuf[1]).toBe(0xcc);
    expect(markerBuf[2]).toBe(0xdd);

    FS.close(stream);
  });

  it("truncate mid-page with page still cached: tail reads as zeros after extend @fast", () => {
    const { FS } = h;
    const stream = FS.open("/trunc_cached", O.RDWR | O.CREAT, 0o666);

    // Write exactly 1 page of 0xFF — page 0 is now cached and dirty
    const fullPage = new Uint8Array(PAGE_SIZE);
    fullPage.fill(0xff);
    FS.write(stream, fullPage, 0, PAGE_SIZE, 0);

    // Truncate to 4000 bytes (mid-page). zeroTailAfterTruncate should
    // zero bytes 4000..8191 in the cached page.
    FS.ftruncate(stream.fd, 4000);

    // Extend the file by writing at byte 6000 (still within page 0)
    const marker = new Uint8Array([0x42]);
    FS.write(stream, marker, 0, 1, 6000);

    // Read byte 4000..5999 (the gap between truncation point and new write)
    // This region was zeroed by truncation and not written to — must be 0
    const gapBuf = new Uint8Array(2000);
    const n = FS.read(stream, gapBuf, 0, 2000, 4000);
    expect(n).toBe(2000);
    for (let i = 0; i < 2000; i++) {
      expect(gapBuf[i]).toBe(0);
    }

    // Read byte 6000 — should be our marker
    const markerBuf = new Uint8Array(1);
    FS.read(stream, markerBuf, 0, 1, 6000);
    expect(markerBuf[0]).toBe(0x42);

    FS.close(stream);
  });

  // ------------------------------------------------------------------
  // 2. Cross-page read after partial eviction
  // ------------------------------------------------------------------

  it("cross-page read after one page evicted: data stitched correctly @fast", () => {
    const { FS } = h;
    // With 4-page cache, writing to 5+ different files will evict earlier pages.

    // Write 2 pages to /target with distinct patterns
    const stream = FS.open("/target", O.RDWR | O.CREAT, 0o666);
    const page0 = new Uint8Array(PAGE_SIZE);
    const page1 = new Uint8Array(PAGE_SIZE);
    for (let i = 0; i < PAGE_SIZE; i++) {
      page0[i] = (i * 7 + 3) & 0xff;
      page1[i] = (i * 13 + 11) & 0xff;
    }
    FS.write(stream, page0, 0, PAGE_SIZE, 0);
    FS.write(stream, page1, 0, PAGE_SIZE, PAGE_SIZE);

    // Evict /target's pages by loading 5 other files (fills 4-page cache)
    for (let f = 0; f < 5; f++) {
      const s = FS.open(`/evict_${f}`, O.RDWR | O.CREAT, 0o666);
      const data = new Uint8Array(PAGE_SIZE);
      data.fill(f + 1);
      FS.write(s, data, 0, PAGE_SIZE, 0);
      FS.close(s);
    }

    // Now read across the page 0→1 boundary in /target.
    // Page 0 must be reloaded from the backend, page 1 may or may not
    // be cached. The read must stitch data from both pages correctly.
    const crossBuf = new Uint8Array(512);
    const readStart = PAGE_SIZE - 256; // last 256 bytes of page 0 + first 256 bytes of page 1
    const n = FS.read(stream, crossBuf, 0, 512, readStart);
    expect(n).toBe(512);

    // Verify first 256 bytes come from page 0
    for (let i = 0; i < 256; i++) {
      const pos = readStart + i;
      const expected = (pos * 7 + 3) & 0xff;
      expect(crossBuf[i]).toBe(expected);
    }
    // Verify last 256 bytes come from page 1
    for (let i = 256; i < 512; i++) {
      const pos = (readStart + i) - PAGE_SIZE; // position within page 1
      const expected = (pos * 13 + 11) & 0xff;
      expect(crossBuf[i]).toBe(expected);
    }

    FS.close(stream);
  });

  it("sequential read of entire file after full cache flush via eviction", () => {
    const { FS } = h;

    // Write 8 pages to a file (double the 4-page cache)
    const stream = FS.open("/bigfile", O.RDWR | O.CREAT, 0o666);
    for (let p = 0; p < 8; p++) {
      const data = new Uint8Array(PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) {
        data[i] = ((p * 37 + i * 7) & 0xff);
      }
      FS.write(stream, data, 0, PAGE_SIZE, p * PAGE_SIZE);
    }

    // Read all 8 pages sequentially — each read must reload from backend
    // since the 4-page cache can't hold all of them
    for (let p = 0; p < 8; p++) {
      const readBuf = new Uint8Array(PAGE_SIZE);
      const n = FS.read(stream, readBuf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expect(n).toBe(PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) {
        const expected = ((p * 37 + i * 7) & 0xff);
        expect(readBuf[i]).toBe(expected);
      }
    }

    FS.close(stream);
  });

  // ------------------------------------------------------------------
  // 3. Truncation under eviction pressure
  // ------------------------------------------------------------------

  it("truncate then read under eviction pressure: no stale data", () => {
    const { FS } = h;

    // Fill the cache: write 4 pages (fills 4-page cache)
    const stream = FS.open("/trunc_pressure", O.RDWR | O.CREAT, 0o666);
    for (let p = 0; p < 4; p++) {
      const data = new Uint8Array(PAGE_SIZE);
      data.fill(0xff);
      FS.write(stream, data, 0, PAGE_SIZE, p * PAGE_SIZE);
    }

    // Truncate to mid-page-1 (PAGE_SIZE + 500 bytes)
    const truncSize = PAGE_SIZE + 500;
    FS.ftruncate(stream.fd, truncSize);

    // Load other files to evict /trunc_pressure's pages
    for (let f = 0; f < 4; f++) {
      const s = FS.open(`/filler_${f}`, O.RDWR | O.CREAT, 0o666);
      const data = new Uint8Array(PAGE_SIZE);
      data.fill(f + 0x10);
      FS.write(s, data, 0, PAGE_SIZE, 0);
      FS.close(s);
    }

    // Re-read the truncated file — pages reload from backend
    const readBuf = new Uint8Array(truncSize);
    const n = FS.read(stream, readBuf, 0, truncSize, 0);
    expect(n).toBe(truncSize);

    // Page 0 should be 0xFF (fully written before truncation)
    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(readBuf[i]).toBe(0xff);
    }
    // Page 1 bytes 0..499 should be 0xFF (within truncated extent)
    for (let i = PAGE_SIZE; i < PAGE_SIZE + 500; i++) {
      expect(readBuf[i]).toBe(0xff);
    }

    // Extend the file and verify the old tail region is zeros
    const extendMarker = new Uint8Array([0x42]);
    FS.write(stream, extendMarker, 0, 1, PAGE_SIZE + 1000);

    const tailBuf = new Uint8Array(500);
    const tailN = FS.read(stream, tailBuf, 0, 500, PAGE_SIZE + 500);
    expect(tailN).toBe(500);
    for (let i = 0; i < 500; i++) {
      if (i === 500) {
        // byte at PAGE_SIZE + 1000 is our marker
        expect(tailBuf[i]).toBe(0x42);
      } else {
        expect(tailBuf[i]).toBe(0);
      }
    }

    FS.close(stream);
  });

  // ------------------------------------------------------------------
  // 4. Repeated truncate/extend cycles with eviction
  // ------------------------------------------------------------------

  it("repeated truncate-extend cycles with eviction between each", () => {
    const { FS } = h;
    const stream = FS.open("/cycle", O.RDWR | O.CREAT, 0o666);

    for (let cycle = 0; cycle < 5; cycle++) {
      // Write 2 pages
      const data = new Uint8Array(PAGE_SIZE * 2);
      data.fill(cycle + 1);
      FS.write(stream, data, 0, data.length, 0);

      // Truncate to half a page
      FS.ftruncate(stream.fd, PAGE_SIZE / 2);

      // Evict by writing to other files
      for (let f = 0; f < 4; f++) {
        const s = FS.open(`/cycle_filler_${f}`, O.RDWR | O.CREAT, 0o666);
        const filler = new Uint8Array(PAGE_SIZE);
        filler.fill(0);
        FS.write(s, filler, 0, PAGE_SIZE, 0);
        FS.close(s);
      }

      // Read back — should see only the first half-page
      const readBuf = new Uint8Array(PAGE_SIZE);
      const n = FS.read(stream, readBuf, 0, PAGE_SIZE, 0);
      expect(n).toBe(PAGE_SIZE / 2);
      for (let i = 0; i < PAGE_SIZE / 2; i++) {
        expect(readBuf[i]).toBe(cycle + 1);
      }
    }

    FS.close(stream);
  });

  // ------------------------------------------------------------------
  // 5. Interleaved multi-file writes + truncation + cross-page reads
  // ------------------------------------------------------------------

  it("interleaved file ops: write A, write B, truncate A, read A cross-page", () => {
    const { FS } = h;
    const streamA = FS.open("/interA", O.RDWR | O.CREAT, 0o666);
    const streamB = FS.open("/interB", O.RDWR | O.CREAT, 0o666);

    // Write 3 pages to A with byte pattern
    for (let p = 0; p < 3; p++) {
      const data = new Uint8Array(PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) {
        data[i] = ((p + 1) * 10 + i) & 0xff;
      }
      FS.write(streamA, data, 0, PAGE_SIZE, p * PAGE_SIZE);
    }

    // Write 2 pages to B (may evict A's pages from 4-page cache)
    for (let p = 0; p < 2; p++) {
      const data = new Uint8Array(PAGE_SIZE);
      data.fill(0xbb + p);
      FS.write(streamB, data, 0, PAGE_SIZE, p * PAGE_SIZE);
    }

    // Truncate A to 1.5 pages
    const truncSize = PAGE_SIZE + PAGE_SIZE / 2;
    FS.ftruncate(streamA.fd, truncSize);

    // Read A across page boundary (last 128 bytes of page 0 + first 128 of page 1)
    const crossBuf = new Uint8Array(256);
    const readStart = PAGE_SIZE - 128;
    const n = FS.read(streamA, crossBuf, 0, 256, readStart);
    expect(n).toBe(256);

    // Verify page 0 data (bytes PAGE_SIZE-128..PAGE_SIZE-1)
    for (let i = 0; i < 128; i++) {
      const pos = readStart + i;
      const expected = ((1 * 10 + pos) & 0xff);
      expect(crossBuf[i]).toBe(expected);
    }
    // Verify page 1 data (bytes 0..127 — written as part of 3-page write)
    for (let i = 128; i < 256; i++) {
      const pos = (readStart + i) - PAGE_SIZE;
      const expected = ((2 * 10 + pos) & 0xff);
      expect(crossBuf[i]).toBe(expected);
    }

    FS.close(streamA);
    FS.close(streamB);
  });
});
