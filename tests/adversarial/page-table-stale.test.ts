/**
 * Adversarial tests: Per-node page table stale reference detection.
 *
 * tomefs maintains a per-node sparse array of CachedPage references
 * (`node._pages`) for O(1) page lookup on single-page reads and writes.
 * When a page is evicted from the LRU cache, its `evicted` flag is set
 * to true. The page table must detect stale references and re-fetch
 * from the cache (which may reload from the backend).
 *
 * These tests target the interaction between the per-node page table and:
 * - Cache eviction under pressure
 * - File rename (which resets `node._pages`)
 * - Truncation (which resets `node._pages`)
 * - Multiple open fds sharing the same node
 * - Interleaved reads/writes across files that compete for cache slots
 *
 * Ethos §9: "Write tests designed to break tomefs specifically — things
 * that pass against MEMFS but expose real bugs in the page cache layer.
 * Target the seams: reads that span page boundaries, writes during
 * eviction, metadata updates after flush."
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

/**
 * Create a FS harness with a tiny cache (4 pages = 32 KB).
 * This forces eviction on nearly every operation, stressing the
 * per-node page table's stale detection logic.
 */
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

/** Write a full page of fill byte at the given page index. */
function writePage(FS: any, stream: any, pageIndex: number, fill: number): void {
  const data = new Uint8Array(PAGE_SIZE);
  data.fill(fill);
  FS.write(stream, data, 0, PAGE_SIZE, pageIndex * PAGE_SIZE);
}

/** Read a full page at the given page index and return the fill byte. */
function readPageFill(FS: any, stream: any, pageIndex: number): number {
  const buf = new Uint8Array(PAGE_SIZE);
  FS.read(stream, buf, 0, PAGE_SIZE, pageIndex * PAGE_SIZE);
  return buf[0];
}

describe("adversarial: per-node page table stale detection", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createSmallCacheFS();
  });

  // ------------------------------------------------------------------
  // Basic stale detection: evict a page, then re-read through page table
  // ------------------------------------------------------------------

  it("read returns correct data after page table entry is evicted @fast", () => {
    const { FS } = h;
    const s = FS.open("/file", O.RDWR | O.CREAT, 0o666);

    // Write page 0 with fill byte 0xAA
    writePage(FS, s, 0, 0xAA);

    // Read page 0 — populates the per-node page table entry
    expect(readPageFill(FS, s, 0)).toBe(0xAA);

    // Write 4 more pages to different files to evict page 0 from the cache
    for (let i = 0; i < 4; i++) {
      const f = FS.open(`/evict${i}`, O.RDWR | O.CREAT, 0o666);
      writePage(FS, f, 0, i);
      FS.close(f);
    }

    // Read page 0 again — the page table entry should detect eviction
    // and re-fetch from the backend
    expect(readPageFill(FS, s, 0)).toBe(0xAA);
    FS.close(s);
  });

  // ------------------------------------------------------------------
  // Write through stale page table entry
  // ------------------------------------------------------------------

  it("write through stale page table entry persists correctly", () => {
    const { FS } = h;
    const s = FS.open("/file", O.RDWR | O.CREAT, 0o666);

    // Write page 0 to populate the page table
    writePage(FS, s, 0, 0xBB);
    expect(readPageFill(FS, s, 0)).toBe(0xBB);

    // Evict page 0 by filling the cache with other files
    for (let i = 0; i < 5; i++) {
      const f = FS.open(`/evict${i}`, O.RDWR | O.CREAT, 0o666);
      writePage(FS, f, 0, i);
      FS.close(f);
    }

    // Write new data through the now-stale page table entry
    writePage(FS, s, 0, 0xCC);

    // Evict again to force the write to backend
    for (let i = 0; i < 5; i++) {
      const f = FS.open(`/evict_b${i}`, O.RDWR | O.CREAT, 0o666);
      writePage(FS, f, 0, i + 10);
      FS.close(f);
    }

    // Read back — should see the updated data
    expect(readPageFill(FS, s, 0)).toBe(0xCC);
    FS.close(s);
  });

  // ------------------------------------------------------------------
  // Multiple pages with interleaved eviction
  // ------------------------------------------------------------------

  it("alternating reads across files with tiny cache return correct data", () => {
    const { FS } = h;

    // Create 8 files, each with a unique page 0
    const streams: any[] = [];
    for (let i = 0; i < 8; i++) {
      const s = FS.open(`/f${i}`, O.RDWR | O.CREAT, 0o666);
      writePage(FS, s, 0, 0x10 + i);
      streams.push(s);
    }

    // Read each file's page 0 in round-robin — each read evicts another
    // file's page, testing page table stale detection on every access
    for (let round = 0; round < 3; round++) {
      for (let i = 0; i < 8; i++) {
        expect(readPageFill(FS, streams[i], 0)).toBe(0x10 + i);
      }
    }

    for (const s of streams) FS.close(s);
  });

  // ------------------------------------------------------------------
  // Page table coherence across dup'd file descriptors
  // ------------------------------------------------------------------

  it("write through dup'd fd is visible through original fd @fast", () => {
    const { FS } = h;
    const s1 = FS.open("/shared", O.RDWR | O.CREAT, 0o666);
    writePage(FS, s1, 0, 0x11);

    // Dup the fd — both point to the same node and share node._pages
    const s2 = FS.dupStream(s1);

    // Read through s1 to populate page table
    expect(readPageFill(FS, s1, 0)).toBe(0x11);

    // Write through s2 (same node) — modifies the shared page table entry
    writePage(FS, s2, 0, 0x22);

    // Read through s1 — should see the s2 write immediately (same page object)
    expect(readPageFill(FS, s1, 0)).toBe(0x22);

    FS.close(s2);
    FS.close(s1);
  });

  it("dup'd fd sees correct data after eviction of shared page table entry", () => {
    const { FS } = h;
    const s1 = FS.open("/shared", O.RDWR | O.CREAT, 0o666);
    writePage(FS, s1, 0, 0x33);

    const s2 = FS.dupStream(s1);

    // Read through both to populate page table
    expect(readPageFill(FS, s1, 0)).toBe(0x33);
    expect(readPageFill(FS, s2, 0)).toBe(0x33);

    // Evict the shared page
    for (let i = 0; i < 5; i++) {
      const f = FS.open(`/evict${i}`, O.RDWR | O.CREAT, 0o666);
      writePage(FS, f, 0, i);
      FS.close(f);
    }

    // Both fds should detect the stale entry and re-fetch
    expect(readPageFill(FS, s1, 0)).toBe(0x33);
    expect(readPageFill(FS, s2, 0)).toBe(0x33);

    FS.close(s2);
    FS.close(s1);
  });

  // ------------------------------------------------------------------
  // Page table reset on truncate
  // ------------------------------------------------------------------

  it("truncate invalidates page table, subsequent read sees zeros @fast", () => {
    const { FS } = h;
    const s = FS.open("/trunc", O.RDWR | O.CREAT, 0o666);

    // Write 3 pages
    writePage(FS, s, 0, 0xAA);
    writePage(FS, s, 1, 0xBB);
    writePage(FS, s, 2, 0xCC);

    // Read page 2 to populate page table
    expect(readPageFill(FS, s, 2)).toBe(0xCC);

    // Truncate to 1 page — should invalidate page table entries for pages 1,2
    FS.ftruncate(s.fd ?? s, PAGE_SIZE);

    // Extend back to 3 pages by writing at page 2
    writePage(FS, s, 2, 0xDD);

    // Page 1 was truncated and not re-written — should be zeros
    const buf = new Uint8Array(PAGE_SIZE);
    FS.read(s, buf, 0, PAGE_SIZE, PAGE_SIZE);
    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(buf[i]).toBe(0);
    }

    // Page 2 should have the new data
    expect(readPageFill(FS, s, 2)).toBe(0xDD);

    // Page 0 should be untouched
    expect(readPageFill(FS, s, 0)).toBe(0xAA);

    FS.close(s);
  });

  it("truncate to zero then regrow — page table entries fully stale", () => {
    const { FS } = h;
    const s = FS.open("/trunc0", O.RDWR | O.CREAT, 0o666);

    // Write and read to populate page table
    writePage(FS, s, 0, 0xEE);
    expect(readPageFill(FS, s, 0)).toBe(0xEE);

    // Truncate to 0
    FS.ftruncate(s.fd ?? s, 0);

    // Regrow with different data
    writePage(FS, s, 0, 0xFF);
    expect(readPageFill(FS, s, 0)).toBe(0xFF);

    FS.close(s);
  });

  // ------------------------------------------------------------------
  // Page table after rename
  // ------------------------------------------------------------------

  it("page table is reset after rename — reads use new storage path", () => {
    const { FS } = h;
    const s = FS.open("/before", O.RDWR | O.CREAT, 0o666);

    // Write and read to populate page table
    writePage(FS, s, 0, 0x55);
    expect(readPageFill(FS, s, 0)).toBe(0x55);

    // Rename the file while fd is open
    FS.rename("/before", "/after");

    // Read through the same fd — page table was reset by rename,
    // so this re-fetches through the cache with the new storage path
    expect(readPageFill(FS, s, 0)).toBe(0x55);

    // Write through the fd after rename
    writePage(FS, s, 0, 0x66);
    expect(readPageFill(FS, s, 0)).toBe(0x66);

    // Verify data is at the new path
    FS.close(s);
    const s2 = FS.open("/after", O.RDONLY);
    expect(readPageFill(FS, s2, 0)).toBe(0x66);
    FS.close(s2);
  });

  // ------------------------------------------------------------------
  // Rapid eviction cycle: write-evict-read on same page repeatedly
  // ------------------------------------------------------------------

  it("repeated write-evict-read cycle on same page preserves data", () => {
    const { FS } = h;
    const s = FS.open("/cycle", O.RDWR | O.CREAT, 0o666);

    for (let round = 0; round < 10; round++) {
      const fill = (round * 37 + 13) & 0xFF;

      // Write to page 0
      writePage(FS, s, 0, fill);

      // Evict by writing to other files
      for (let i = 0; i < 4; i++) {
        const f = FS.open(`/evict_cycle_${i}`, O.RDWR | O.CREAT, 0o666);
        writePage(FS, f, 0, round + i);
        FS.close(f);
      }

      // Read back — stale page table entry should be detected
      expect(readPageFill(FS, s, 0)).toBe(fill);
    }

    FS.close(s);
  });

  // ------------------------------------------------------------------
  // Sub-page writes through stale page table entries
  // ------------------------------------------------------------------

  it("small write to stale page table entry preserves rest of page", () => {
    const { FS } = h;
    const s = FS.open("/subpage", O.RDWR | O.CREAT, 0o666);

    // Fill page 0 with 0xAA
    writePage(FS, s, 0, 0xAA);

    // Read to populate page table
    expect(readPageFill(FS, s, 0)).toBe(0xAA);

    // Evict the page
    for (let i = 0; i < 5; i++) {
      const f = FS.open(`/evict${i}`, O.RDWR | O.CREAT, 0o666);
      writePage(FS, f, 0, i);
      FS.close(f);
    }

    // Write only 4 bytes at offset 100 within page 0
    // The page table entry is stale, so this must re-fetch the page
    // from the backend before applying the partial write
    const patch = new Uint8Array([0x01, 0x02, 0x03, 0x04]);
    FS.write(s, patch, 0, 4, 100);

    // Evict again to force round-trip through backend
    for (let i = 0; i < 5; i++) {
      const f = FS.open(`/evict_b${i}`, O.RDWR | O.CREAT, 0o666);
      writePage(FS, f, 0, i + 20);
      FS.close(f);
    }

    // Read back the full page — bytes 100-103 should be patched,
    // everything else should be 0xAA
    const buf = new Uint8Array(PAGE_SIZE);
    FS.read(s, buf, 0, PAGE_SIZE, 0);

    for (let i = 0; i < PAGE_SIZE; i++) {
      if (i >= 100 && i < 104) {
        expect(buf[i]).toBe(i - 100 + 1);
      } else {
        expect(buf[i]).toBe(0xAA);
      }
    }

    FS.close(s);
  });

  // ------------------------------------------------------------------
  // Multiple pages per file with selective eviction
  // ------------------------------------------------------------------

  it("multi-page file with selective page eviction", () => {
    const { FS } = h;
    const s = FS.open("/multi", O.RDWR | O.CREAT, 0o666);

    // Write 6 pages with distinct fills
    for (let p = 0; p < 6; p++) {
      writePage(FS, s, p, 0x10 * (p + 1));
    }

    // Read pages 0 and 1 to populate page table
    expect(readPageFill(FS, s, 0)).toBe(0x10);
    expect(readPageFill(FS, s, 1)).toBe(0x20);

    // Read pages 2-5 to evict pages 0 and 1 from cache (cache=4)
    expect(readPageFill(FS, s, 2)).toBe(0x30);
    expect(readPageFill(FS, s, 3)).toBe(0x40);
    expect(readPageFill(FS, s, 4)).toBe(0x50);
    expect(readPageFill(FS, s, 5)).toBe(0x60);

    // Now read pages 0 and 1 again — their page table entries point to
    // evicted CachedPage objects. Stale detection must re-fetch them.
    expect(readPageFill(FS, s, 0)).toBe(0x10);
    expect(readPageFill(FS, s, 1)).toBe(0x20);

    FS.close(s);
  });

  // ------------------------------------------------------------------
  // Concurrent access: two files competing for cache, both using page tables
  // ------------------------------------------------------------------

  it("two files competing for cache slots both get correct data @fast", () => {
    const { FS } = h;

    const s1 = FS.open("/file1", O.RDWR | O.CREAT, 0o666);
    const s2 = FS.open("/file2", O.RDWR | O.CREAT, 0o666);

    // Write 3 pages to each file
    for (let p = 0; p < 3; p++) {
      writePage(FS, s1, p, 0xA0 + p);
      writePage(FS, s2, p, 0xB0 + p);
    }

    // Interleaved reads — with a 4-page cache, each file's reads evict
    // the other file's pages, testing stale detection on every access
    for (let round = 0; round < 5; round++) {
      for (let p = 0; p < 3; p++) {
        expect(readPageFill(FS, s1, p)).toBe(0xA0 + p);
        expect(readPageFill(FS, s2, p)).toBe(0xB0 + p);
      }
    }

    FS.close(s1);
    FS.close(s2);
  });

  // ------------------------------------------------------------------
  // Page table coherence after close+reopen cycle (simulates persist)
  // ------------------------------------------------------------------

  it("close and reopen preserves data written through page table", () => {
    const { FS } = h;
    const s = FS.open("/persist", O.RDWR | O.CREAT, 0o666);

    // Write and read to populate page table
    writePage(FS, s, 0, 0x77);
    expect(readPageFill(FS, s, 0)).toBe(0x77);

    // Close flushes dirty pages to the backend
    FS.close(s);

    // Reopen — page table is fresh, data must come from cache/backend
    const s2 = FS.open("/persist", O.RDWR);
    expect(readPageFill(FS, s2, 0)).toBe(0x77);

    // Write new data and verify the page table entry is coherent
    writePage(FS, s2, 0, 0x88);
    expect(readPageFill(FS, s2, 0)).toBe(0x88);

    FS.close(s2);
  });

  // ------------------------------------------------------------------
  // Page table with allocate (fallocate) extending file
  // ------------------------------------------------------------------

  it("allocate extends file without corrupting existing page table entries", () => {
    const { FS } = h;
    const s = FS.open("/alloc", O.RDWR | O.CREAT, 0o666);

    // Write page 0
    writePage(FS, s, 0, 0xDD);
    expect(readPageFill(FS, s, 0)).toBe(0xDD);

    // Allocate extends the file to 5 pages — page table for page 0
    // should remain valid since allocate resets node._pages
    if (s.stream_ops?.allocate) {
      s.stream_ops.allocate(s, 0, PAGE_SIZE * 5);
    } else {
      // MEMFS fallback: manually extend
      const stat = FS.fstat(s.fd ?? s);
      if (stat.size < PAGE_SIZE * 5) {
        FS.ftruncate(s.fd ?? s, PAGE_SIZE * 5);
      }
    }

    // Verify page 0 still has correct data (page table was reset,
    // but data should be re-fetched from cache or backend)
    expect(readPageFill(FS, s, 0)).toBe(0xDD);

    // Verify extended pages read as zeros
    const buf = new Uint8Array(PAGE_SIZE);
    FS.read(s, buf, 0, PAGE_SIZE, PAGE_SIZE * 4);
    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(buf[i]).toBe(0);
    }

    FS.close(s);
  });

  // ------------------------------------------------------------------
  // Stress: write-read cycle across many pages with tiny cache
  // ------------------------------------------------------------------

  it("sequential write then random-order read of 20 pages with 4-page cache", () => {
    const { FS } = h;
    const s = FS.open("/stress", O.RDWR | O.CREAT, 0o666);
    const numPages = 20;

    // Sequential write — each page gets a unique fill
    for (let p = 0; p < numPages; p++) {
      writePage(FS, s, p, (p * 13 + 7) & 0xFF);
    }

    // Read in a scrambled order to maximize cache thrashing
    // and page table stale detection
    const readOrder = Array.from({ length: numPages }, (_, i) => i);
    // Deterministic shuffle: reverse pairs
    for (let i = 0; i < readOrder.length - 1; i += 2) {
      const tmp = readOrder[i];
      readOrder[i] = readOrder[i + 1];
      readOrder[i + 1] = tmp;
    }

    for (const p of readOrder) {
      const expected = (p * 13 + 7) & 0xFF;
      expect(readPageFill(FS, s, p)).toBe(expected);
    }

    // Second pass in reverse order
    for (let p = numPages - 1; p >= 0; p--) {
      const expected = (p * 13 + 7) & 0xFF;
      expect(readPageFill(FS, s, p)).toBe(expected);
    }

    FS.close(s);
  });

  // ------------------------------------------------------------------
  // Write to page N, evict, write to page N via different offset
  // ------------------------------------------------------------------

  it("partial writes to same page across eviction boundaries merge correctly", () => {
    const { FS } = h;
    const s = FS.open("/merge", O.RDWR | O.CREAT, 0o666);

    // Write first half of page 0
    const firstHalf = new Uint8Array(PAGE_SIZE / 2);
    firstHalf.fill(0x11);
    FS.write(s, firstHalf, 0, firstHalf.length, 0);

    // Evict page 0
    for (let i = 0; i < 5; i++) {
      const f = FS.open(`/evict${i}`, O.RDWR | O.CREAT, 0o666);
      writePage(FS, f, 0, i);
      FS.close(f);
    }

    // Write second half of page 0 — must re-fetch page from backend
    // to preserve first half, then overlay second half
    const secondHalf = new Uint8Array(PAGE_SIZE / 2);
    secondHalf.fill(0x22);
    FS.write(s, secondHalf, 0, secondHalf.length, PAGE_SIZE / 2);

    // Evict again
    for (let i = 0; i < 5; i++) {
      const f = FS.open(`/evict_b${i}`, O.RDWR | O.CREAT, 0o666);
      writePage(FS, f, 0, i + 20);
      FS.close(f);
    }

    // Read full page — first half should be 0x11, second half 0x22
    const buf = new Uint8Array(PAGE_SIZE);
    FS.read(s, buf, 0, PAGE_SIZE, 0);

    for (let i = 0; i < PAGE_SIZE / 2; i++) {
      expect(buf[i]).toBe(0x11);
    }
    for (let i = PAGE_SIZE / 2; i < PAGE_SIZE; i++) {
      expect(buf[i]).toBe(0x22);
    }

    FS.close(s);
  });
});
