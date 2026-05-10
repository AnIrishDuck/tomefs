/**
 * Adversarial tests for stale per-node page table entries after eviction.
 *
 * tomefs maintains a per-node page table (node._pages[]) for O(1) page
 * access, bypassing Map lookups in the page cache. When a page is evicted,
 * the per-node table entry becomes stale (CachedPage.evicted = true).
 *
 * The single-page fast path clears stale entries and re-populates them.
 * But the multi-page cold path's population loop originally checked
 * `if (!node._pages[p])` — which doesn't catch stale entries (a stale
 * CachedPage object is truthy). This caused repeated cold-path fallthrough
 * on every subsequent multi-page read/write for those pages.
 *
 * These tests verify that stale entries are properly refreshed, ensuring
 * the multi-page warm path is used after the first cold-path reload.
 */

import { describe, it, expect } from "vitest";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { createTomeFS } from "../../src/tomefs.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { PAGE_SIZE } from "../../src/types.js";
import type { SyncPageCache } from "../../src/sync-page-cache.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

async function createHarness(maxPages: number) {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  const Module = await createModule();
  const rawFS = Module.FS;

  const backend = new SyncMemoryBackend();
  const tomefs = createTomeFS(rawFS, { backend, maxPages });

  rawFS.mkdir("/tome");
  rawFS.mount(tomefs, {}, "/tome");

  const pageCache = tomefs.pageCache as SyncPageCache;

  return { FS: rawFS, pageCache };
}

describe("adversarial: stale per-node page table after eviction", () => {
  it("multi-page read refreshes stale page table entries @fast", async () => {
    const { FS, pageCache } = await createHarness(4);

    // Write 2 pages to file A (16 KB total)
    const stream = FS.open("/tome/fileA", 2 | 64, 0o666); // O_RDWR | O_CREAT
    const page0Data = new Uint8Array(PAGE_SIZE);
    const page1Data = new Uint8Array(PAGE_SIZE);
    page0Data.fill(0xaa);
    page1Data.fill(0xbb);
    FS.write(stream, page0Data, 0, PAGE_SIZE, 0);
    FS.write(stream, page1Data, 0, PAGE_SIZE, PAGE_SIZE);

    // Read each page individually (single-page path) to populate
    // the per-node page table with live CachedPage references.
    const buf = new Uint8Array(PAGE_SIZE);
    FS.read(stream, buf, 0, PAGE_SIZE, 0);
    expect(buf[0]).toBe(0xaa);
    FS.read(stream, buf, 0, PAGE_SIZE, PAGE_SIZE);
    expect(buf[0]).toBe(0xbb);

    // Fill cache with other files to evict file A's pages.
    // With maxPages=4, writing 4 new pages pushes out file A's 2 pages.
    for (let i = 0; i < 4; i++) {
      const s = FS.open(`/tome/filler${i}`, 2 | 64, 0o666);
      const d = new Uint8Array(PAGE_SIZE);
      d.fill(i + 1);
      FS.write(s, d, 0, PAGE_SIZE, 0);
      FS.close(s);
    }

    // At this point, file A's per-node table entries are stale
    // (pointing to evicted CachedPage objects).

    // Multi-page read spanning pages 0 and 1 — this hits the cold path
    // because the warm path detects stale entries. The cold path reloads
    // from backend and should refresh the per-node table entries.
    const crossBuf = new Uint8Array(200);
    const crossPos = PAGE_SIZE - 100; // spans page boundary
    FS.read(stream, crossBuf, 0, 200, crossPos);

    // Verify data correctness
    for (let i = 0; i < 100; i++) {
      expect(crossBuf[i]).toBe(0xaa);
    }
    for (let i = 100; i < 200; i++) {
      expect(crossBuf[i]).toBe(0xbb);
    }

    // Now read the same range again. With the fix, the per-node table
    // was refreshed during the first multi-page read, so this should
    // hit the warm path (zero cache operations).
    pageCache.resetStats();

    FS.read(stream, crossBuf, 0, 200, crossPos);

    // Verify data still correct
    for (let i = 0; i < 100; i++) {
      expect(crossBuf[i]).toBe(0xaa);
    }
    for (let i = 100; i < 200; i++) {
      expect(crossBuf[i]).toBe(0xbb);
    }

    // With the fix: warm path was used, no cache operations.
    // Without the fix: cold path was used, generating cache hits.
    const stats = pageCache.getStats();
    expect(stats.hits).toBe(0);
    expect(stats.misses).toBe(0);

    FS.close(stream);
  });

  it("multi-page write refreshes stale page table entries @fast", async () => {
    const { FS, pageCache } = await createHarness(4);

    // Write 2 pages to file A
    const stream = FS.open("/tome/fileA", 2 | 64, 0o666);
    const initData = new Uint8Array(PAGE_SIZE * 2);
    initData.fill(0x11);
    FS.write(stream, initData, 0, PAGE_SIZE * 2, 0);

    // Single-page reads to populate per-node table
    const buf = new Uint8Array(PAGE_SIZE);
    FS.read(stream, buf, 0, PAGE_SIZE, 0);
    FS.read(stream, buf, 0, PAGE_SIZE, PAGE_SIZE);

    // Evict file A's pages
    for (let i = 0; i < 4; i++) {
      const s = FS.open(`/tome/filler${i}`, 2 | 64, 0o666);
      const d = new Uint8Array(PAGE_SIZE);
      d.fill(i + 1);
      FS.write(s, d, 0, PAGE_SIZE, 0);
      FS.close(s);
    }

    // Multi-page write spanning page boundary — cold path reloads and
    // should refresh stale per-node table entries.
    const writeData = new Uint8Array(200);
    writeData.fill(0xcc);
    FS.write(stream, writeData, 0, 200, PAGE_SIZE - 100);

    // Evict again to force stale entries if not refreshed
    for (let i = 0; i < 4; i++) {
      const s = FS.open(`/tome/filler2_${i}`, 2 | 64, 0o666);
      const d = new Uint8Array(PAGE_SIZE);
      d.fill(i + 10);
      FS.write(s, d, 0, PAGE_SIZE, 0);
      FS.close(s);
    }

    // Second multi-page write at same position — should refresh entries
    // on the cold path, then the third write should hit warm path.
    const writeData2 = new Uint8Array(200);
    writeData2.fill(0xdd);
    FS.write(stream, writeData2, 0, 200, PAGE_SIZE - 100);

    // Reset stats and do a third write — should use warm path
    pageCache.resetStats();

    const writeData3 = new Uint8Array(200);
    writeData3.fill(0xee);
    FS.write(stream, writeData3, 0, 200, PAGE_SIZE - 100);

    const stats = pageCache.getStats();
    expect(stats.hits).toBe(0);
    expect(stats.misses).toBe(0);

    // Verify final data
    const readBuf = new Uint8Array(200);
    FS.read(stream, readBuf, 0, 200, PAGE_SIZE - 100);
    for (let i = 0; i < 200; i++) {
      expect(readBuf[i]).toBe(0xee);
    }

    FS.close(stream);
  });

  it("single-page read after eviction still works (baseline) @fast", async () => {
    const { FS, pageCache } = await createHarness(4);

    // Write 1 page
    const stream = FS.open("/tome/fileA", 2 | 64, 0o666);
    const data = new Uint8Array(PAGE_SIZE);
    data.fill(0xff);
    FS.write(stream, data, 0, PAGE_SIZE, 0);

    // Single-page read to populate per-node table
    const buf = new Uint8Array(PAGE_SIZE);
    FS.read(stream, buf, 0, PAGE_SIZE, 0);
    expect(buf[0]).toBe(0xff);

    // Evict
    for (let i = 0; i < 4; i++) {
      const s = FS.open(`/tome/filler${i}`, 2 | 64, 0o666);
      const d = new Uint8Array(PAGE_SIZE);
      FS.write(s, d, 0, PAGE_SIZE, 0);
      FS.close(s);
    }

    // Single-page read — the fast path clears stale entries correctly
    FS.read(stream, buf, 0, PAGE_SIZE, 0);
    expect(buf[0]).toBe(0xff);

    // Second single-page read — should use per-node table (warm)
    pageCache.resetStats();
    FS.read(stream, buf, 0, PAGE_SIZE, 0);
    expect(buf[0]).toBe(0xff);

    const stats = pageCache.getStats();
    expect(stats.hits).toBe(0);
    expect(stats.misses).toBe(0);

    FS.close(stream);
  });

  it("interleaved single and multi-page reads after eviction @fast", async () => {
    const { FS, pageCache } = await createHarness(4);

    // Write 3 pages
    const stream = FS.open("/tome/fileA", 2 | 64, 0o666);
    for (let p = 0; p < 3; p++) {
      const d = new Uint8Array(PAGE_SIZE);
      d.fill(p + 0x10);
      FS.write(stream, d, 0, PAGE_SIZE, p * PAGE_SIZE);
    }

    // Read pages individually to populate per-node table
    const buf = new Uint8Array(PAGE_SIZE);
    for (let p = 0; p < 3; p++) {
      FS.read(stream, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
    }

    // Evict all pages
    for (let i = 0; i < 4; i++) {
      const s = FS.open(`/tome/filler${i}`, 2 | 64, 0o666);
      const d = new Uint8Array(PAGE_SIZE);
      FS.write(s, d, 0, PAGE_SIZE, 0);
      FS.close(s);
    }

    // Single-page read of page 0 — clears stale entry via fast path
    FS.read(stream, buf, 0, 100, 0);
    expect(buf[0]).toBe(0x10);

    // Multi-page read spanning pages 1 and 2 — cold path should
    // refresh stale entries for these pages
    const crossBuf = new Uint8Array(200);
    FS.read(stream, crossBuf, 0, 200, 2 * PAGE_SIZE - 100);
    for (let i = 0; i < 100; i++) {
      expect(crossBuf[i]).toBe(0x11);
    }
    for (let i = 100; i < 200; i++) {
      expect(crossBuf[i]).toBe(0x12);
    }

    // Now all entries should be fresh. Verify warm path for all pages.
    pageCache.resetStats();

    // Single-page read of page 0 — should be warm
    FS.read(stream, buf, 0, 100, 0);
    expect(buf[0]).toBe(0x10);

    // Multi-page read of pages 1-2 — should be warm
    FS.read(stream, crossBuf, 0, 200, 2 * PAGE_SIZE - 100);
    for (let i = 0; i < 100; i++) {
      expect(crossBuf[i]).toBe(0x11);
    }

    const stats = pageCache.getStats();
    expect(stats.hits).toBe(0);
    expect(stats.misses).toBe(0);

    FS.close(stream);
  });

  it("repeated eviction cycles don't accumulate stale entries @fast", async () => {
    const { FS, pageCache } = await createHarness(4);

    const stream = FS.open("/tome/fileA", 2 | 64, 0o666);
    const data = new Uint8Array(PAGE_SIZE * 2);
    data.subarray(0, PAGE_SIZE).fill(0x41);
    data.subarray(PAGE_SIZE).fill(0x42);
    FS.write(stream, data, 0, PAGE_SIZE * 2, 0);

    // Run 5 eviction+reload cycles via multi-page reads
    for (let cycle = 0; cycle < 5; cycle++) {
      // Evict file A's pages
      for (let i = 0; i < 4; i++) {
        const s = FS.open(`/tome/cycle${cycle}_${i}`, 2 | 64, 0o666);
        const d = new Uint8Array(PAGE_SIZE);
        d.fill(cycle * 10 + i);
        FS.write(s, d, 0, PAGE_SIZE, 0);
        FS.close(s);
      }

      // Multi-page read to reload and refresh entries
      const crossBuf = new Uint8Array(200);
      FS.read(stream, crossBuf, 0, 200, PAGE_SIZE - 100);
      for (let i = 0; i < 100; i++) {
        expect(crossBuf[i]).toBe(0x41);
      }
      for (let i = 100; i < 200; i++) {
        expect(crossBuf[i]).toBe(0x42);
      }
    }

    // After 5 cycles, per-node table should be clean.
    // Verify warm path works.
    pageCache.resetStats();

    const finalBuf = new Uint8Array(200);
    FS.read(stream, finalBuf, 0, 200, PAGE_SIZE - 100);
    for (let i = 0; i < 100; i++) {
      expect(finalBuf[i]).toBe(0x41);
    }
    for (let i = 100; i < 200; i++) {
      expect(finalBuf[i]).toBe(0x42);
    }

    const stats = pageCache.getStats();
    expect(stats.hits).toBe(0);
    expect(stats.misses).toBe(0);

    FS.close(stream);
  });
});
