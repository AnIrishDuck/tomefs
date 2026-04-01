/**
 * Adversarial tests: multi-page read/write self-eviction.
 *
 * When a multi-page read or write has some pages cached and some missing,
 * batchEvict must not evict the already-cached pages of the same file.
 * Without protection, the LRU end may contain our own pages (if they were
 * accessed early and pushed down by other files' accesses), causing them
 * to be evicted and then immediately reloaded — wasting backend round-trips.
 *
 * Ethos §6: "Any measurable regression when the working set fits in memory
 * is a bug."
 * Ethos §9: "Target the seams: reads that span page boundaries, writes
 * during eviction."
 */

import { describe, it, expect, beforeEach } from "vitest";
import { SyncPageCache } from "../../src/sync-page-cache.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { PAGE_SIZE } from "../../src/types.js";

describe("multi-page self-eviction protection", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("multi-page read does not evict already-cached pages of the same file", () => {
    // Cache holds 6 pages. File A has 4 pages, file B has 2.
    // Access order: A:0, A:1, B:0, B:1 — so A:0 and A:1 are at the LRU end.
    // Then read A pages 0-3 (2 cached, 2 missing).
    // Without protection, batchEvict(2) would evict A:0 and A:1.
    const cache = new SyncPageCache(backend, 6);

    // Write 4 pages to file A in the backend
    const pageA = new Uint8Array(PAGE_SIZE);
    for (let i = 0; i < 4; i++) {
      pageA.fill(0x10 + i);
      backend.writePage("/fileA", i, pageA);
    }

    // Cache pages A:0 and A:1 (these go to LRU end = oldest)
    cache.getPage("/fileA", 0);
    cache.getPage("/fileA", 1);

    // Cache pages from file B, pushing A:0 and A:1 toward the LRU end
    const pageB = new Uint8Array(PAGE_SIZE);
    pageB.fill(0xBB);
    cache.write("/fileB", pageB, 0, PAGE_SIZE, 0, 0);
    cache.write("/fileB", pageB, 0, PAGE_SIZE, PAGE_SIZE, PAGE_SIZE);

    // Now cache has: [A:0, A:1, B:0, B:1] (4 of 6 slots used)
    // A:0 is the oldest (LRU end).
    expect(cache.size).toBe(4);

    // Fill the remaining 2 slots with file C
    const pageC = new Uint8Array(PAGE_SIZE);
    pageC.fill(0xCC);
    cache.write("/fileC", pageC, 0, PAGE_SIZE, 0, 0);
    cache.write("/fileC", pageC, 0, PAGE_SIZE, PAGE_SIZE, PAGE_SIZE);

    // Cache is now full: [A:0, A:1, B:0, B:1, C:0, C:1]
    expect(cache.size).toBe(6);
    cache.resetStats();

    // Multi-page read of A pages 0-3. Pages 0,1 are cached; 2,3 are missing.
    // batchEvict(2) is needed — without protection it would evict A:0, A:1.
    const buf = new Uint8Array(4 * PAGE_SIZE);
    const bytesRead = cache.read("/fileA", buf, 0, 4 * PAGE_SIZE, 0, 4 * PAGE_SIZE);
    expect(bytesRead).toBe(4 * PAGE_SIZE);

    const stats = cache.getStats();
    // With protection: evicts B:0 and B:1 (not A:0 and A:1).
    // A:0 and A:1 are cache hits, A:2 and A:3 are batch-loaded misses.
    // Without protection: A:0 and A:1 would be evicted, then reloaded
    // as misses in the read loop — causing 2 extra backend reads and
    // 2 extra evictions (of B:0 and B:1 or C:0 and C:1).
    expect(stats.evictions).toBe(2); // exactly 2 evictions needed
    // No extra backend reads — A:0 and A:1 stayed in cache
    expect(stats.misses).toBe(2); // only A:2 and A:3

    // Verify data correctness
    for (let i = 0; i < 4; i++) {
      const slice = buf.slice(i * PAGE_SIZE, (i + 1) * PAGE_SIZE);
      const expected = new Uint8Array(PAGE_SIZE);
      expected.fill(0x10 + i);
      expect(slice).toEqual(expected);
    }
  });

  it("multi-page write does not evict already-cached pages of the same file", () => {
    // Same setup but with writes. File A has 2 existing pages cached,
    // and we write across 4 pages (2 existing + 2 new).
    const cache = new SyncPageCache(backend, 6);

    // Write 2 pages to file A in backend
    const pageA = new Uint8Array(PAGE_SIZE);
    pageA.fill(0xAA);
    backend.writePage("/fileA", 0, pageA);
    backend.writePage("/fileA", 1, pageA);

    // Cache A:0 and A:1
    cache.getPage("/fileA", 0);
    cache.getPage("/fileA", 1);

    // Fill cache with other files
    const filler = new Uint8Array(PAGE_SIZE);
    filler.fill(0xFF);
    cache.write("/filler1", filler, 0, PAGE_SIZE, 0, 0);
    cache.write("/filler2", filler, 0, PAGE_SIZE, 0, 0);
    cache.write("/filler3", filler, 0, PAGE_SIZE, 0, 0);
    cache.write("/filler4", filler, 0, PAGE_SIZE, 0, 0);

    // Cache is full: [A:0, A:1, f1:0, f2:0, f3:0, f4:0]
    expect(cache.size).toBe(6);
    cache.resetStats();

    // Multi-page write across A pages 0-3 (0,1 cached; 2,3 new)
    const writeData = new Uint8Array(4 * PAGE_SIZE);
    for (let i = 0; i < 4; i++) {
      writeData.fill(0x20 + i, i * PAGE_SIZE, (i + 1) * PAGE_SIZE);
    }
    const result = cache.write(
      "/fileA", writeData, 0, 4 * PAGE_SIZE, 0, 2 * PAGE_SIZE,
    );
    expect(result.bytesWritten).toBe(4 * PAGE_SIZE);
    expect(result.newFileSize).toBe(4 * PAGE_SIZE);

    const stats = cache.getStats();
    // With protection: filler pages evicted (not A:0 and A:1)
    // Misses should only be for A:2 and A:3 (new pages, no backend read)
    expect(stats.evictions).toBeLessThanOrEqual(4); // at most 4 filler pages evicted

    // Read back and verify all data is correct
    cache.resetStats();
    const readBuf = new Uint8Array(4 * PAGE_SIZE);
    cache.read("/fileA", readBuf, 0, 4 * PAGE_SIZE, 0, 4 * PAGE_SIZE);
    // All 4 pages should be cache hits (no eviction of our own pages)
    expect(cache.getStats().misses).toBe(0);

    for (let i = 0; i < 4; i++) {
      const slice = readBuf.slice(i * PAGE_SIZE, (i + 1) * PAGE_SIZE);
      const expected = new Uint8Array(PAGE_SIZE);
      expected.fill(0x20 + i);
      expect(slice).toEqual(expected);
    }
  });

  it("dirty cached pages survive self-eviction protection", () => {
    // Verify that dirty pages at the LRU end are protected and
    // their dirty data is not lost.
    const cache = new SyncPageCache(backend, 4);

    // Write dirty data to A:0
    const dirtyData = new Uint8Array(PAGE_SIZE);
    dirtyData.fill(0xDD);
    cache.write("/fileA", dirtyData, 0, PAGE_SIZE, 0, 0);
    expect(cache.isDirty("/fileA", 0)).toBe(true);

    // Fill remaining cache with other files
    const filler = new Uint8Array(PAGE_SIZE);
    filler.fill(0xFF);
    cache.write("/filler1", filler, 0, PAGE_SIZE, 0, 0);
    cache.write("/filler2", filler, 0, PAGE_SIZE, 0, 0);
    cache.write("/filler3", filler, 0, PAGE_SIZE, 0, 0);

    // Cache full: [A:0(dirty), f1:0, f2:0, f3:0]
    expect(cache.size).toBe(4);

    // Write backend pages for A:1 and A:2 so multi-page read has misses
    backend.writePage("/fileA", 1, new Uint8Array(PAGE_SIZE));
    backend.writePage("/fileA", 2, new Uint8Array(PAGE_SIZE));

    // Multi-page read of A:0-2. A:0 is cached+dirty; A:1, A:2 are missing.
    const buf = new Uint8Array(3 * PAGE_SIZE);
    cache.read("/fileA", buf, 0, 3 * PAGE_SIZE, 0, 3 * PAGE_SIZE);

    // A:0's dirty data must be preserved in the read result
    const page0 = buf.slice(0, PAGE_SIZE);
    expect(page0).toEqual(dirtyData);

    // A:0 should still be dirty in cache (not evicted and reloaded clean)
    expect(cache.isDirty("/fileA", 0)).toBe(true);
  });
});
