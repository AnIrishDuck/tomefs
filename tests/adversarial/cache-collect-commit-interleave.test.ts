/**
 * Adversarial tests for SyncPageCache and PageCache collect/commit interleaving.
 *
 * The two-phase dirty commit pattern (collectDirtyPages → commitDirtyPages)
 * has important constraints:
 *
 * - Pages at DIFFERENT keys dirtied between collect and commit are preserved.
 * - The SAME page re-dirtied between collect and commit has its dirty flag
 *   cleared unconditionally (no generation counter). This is safe because
 *   the sync FS path prevents interleaving, but these tests document the
 *   behavior explicitly.
 *
 * This also tests that markPageDirty() correctly ensures pages exist in
 * cache AND are marked dirty — the contract needed by allocate().
 *
 * Ethos §9: "Write tests designed to break tomefs specifically"
 */

import { describe, it, expect } from "vitest";
import { SyncPageCache } from "../../src/sync-page-cache.js";
import { PageCache } from "../../src/page-cache.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { MemoryBackend } from "../../src/memory-backend.js";
import { PAGE_SIZE } from "../../src/types.js";

function filledBuf(v: number, size = PAGE_SIZE): Uint8Array {
  const buf = new Uint8Array(size);
  buf.fill(v);
  return buf;
}

describe("SyncPageCache: collect/commit interleaving", () => {
  it("same page re-dirtied between collect and commit loses dirty flag @fast", () => {
    const backend = new SyncMemoryBackend();
    const cache = new SyncPageCache(backend, 16);

    cache.write("/f", filledBuf(0xaa), 0, PAGE_SIZE, 0, 0);
    expect(cache.dirtyCount).toBe(1);

    const collected = cache.collectDirtyPages();
    expect(collected.length).toBe(1);
    expect(cache.dirtyCount).toBe(1);

    // Re-dirty the SAME page — the dirty flag is already true,
    // so the cache can't distinguish old vs new dirty.
    cache.write("/f", filledBuf(0xbb), 0, PAGE_SIZE, 0, PAGE_SIZE);

    backend.writePages(collected);
    cache.commitDirtyPages(collected);

    // The dirty flag is cleared even though the page was re-written.
    // The cache data has 0xbb but the backend has 0xaa (from collected).
    // This is safe in practice because the sync FS path prevents
    // interleaving between collect and commit.
    expect(cache.dirtyCount).toBe(0);

    // Verify the cache data is the latest write (0xbb)
    const readBuf = new Uint8Array(PAGE_SIZE);
    cache.read("/f", readBuf, 0, PAGE_SIZE, 0, PAGE_SIZE);
    expect(readBuf).toEqual(filledBuf(0xbb));
  });

  it("writes to different page between collect and commit are preserved @fast", () => {
    const backend = new SyncMemoryBackend();
    const cache = new SyncPageCache(backend, 16);

    cache.write("/f", filledBuf(0xaa), 0, PAGE_SIZE, 0, 0);
    const collected = cache.collectDirtyPages();
    expect(collected.length).toBe(1);

    // Write to a different page of the same file
    cache.write("/f", filledBuf(0xcc), 0, PAGE_SIZE, PAGE_SIZE, PAGE_SIZE);

    backend.writePages(collected);
    cache.commitDirtyPages(collected);

    // Page 0 was committed, page 1 was dirtied after collect
    expect(cache.isDirty("/f", 0)).toBe(false);
    expect(cache.isDirty("/f", 1)).toBe(true);
    expect(cache.dirtyCount).toBe(1);
  });

  it("writes to different file between collect and commit are preserved @fast", () => {
    const backend = new SyncMemoryBackend();
    const cache = new SyncPageCache(backend, 16);

    cache.write("/a", filledBuf(0xaa), 0, PAGE_SIZE, 0, 0);
    const collected = cache.collectDirtyPages();

    // Write to a completely different file
    cache.write("/b", filledBuf(0xbb), 0, PAGE_SIZE, 0, 0);

    backend.writePages(collected);
    cache.commitDirtyPages(collected);

    expect(cache.isDirty("/a", 0)).toBe(false);
    expect(cache.isDirty("/b", 0)).toBe(true);
    expect(cache.dirtyCount).toBe(1);
  });

  it("newly dirtied pages not in collected set are preserved", () => {
    const backend = new SyncMemoryBackend();
    const cache = new SyncPageCache(backend, 16);

    cache.write("/f", filledBuf(0xaa), 0, PAGE_SIZE, 0, 0);
    cache.write("/f", filledBuf(0xbb), 0, PAGE_SIZE, PAGE_SIZE, PAGE_SIZE);
    expect(cache.dirtyCount).toBe(2);

    const collected = cache.collectDirtyPages();
    expect(collected.length).toBe(2);

    // Add a new page (page 2) not in the collected set
    cache.write("/f", filledBuf(0x33), 0, PAGE_SIZE, 2 * PAGE_SIZE, 2 * PAGE_SIZE);

    backend.writePages(collected);
    cache.commitDirtyPages(collected);

    // Pages 0 and 1 were committed; page 2 was NOT in collected, stays dirty
    expect(cache.isDirty("/f", 0)).toBe(false);
    expect(cache.isDirty("/f", 1)).toBe(false);
    expect(cache.isDirty("/f", 2)).toBe(true);
    expect(cache.dirtyCount).toBe(1);
  });

  it("collect returns empty after flushAll", () => {
    const backend = new SyncMemoryBackend();
    const cache = new SyncPageCache(backend, 16);

    cache.write("/f", filledBuf(0xaa), 0, PAGE_SIZE, 0, 0);
    cache.flushAll();

    const collected = cache.collectDirtyPages();
    expect(collected.length).toBe(0);
    expect(cache.dirtyCount).toBe(0);
  });

  it("commitDirtyPages is idempotent for already-clean pages", () => {
    const backend = new SyncMemoryBackend();
    const cache = new SyncPageCache(backend, 16);

    cache.write("/f", filledBuf(0xaa), 0, PAGE_SIZE, 0, 0);
    const collected = cache.collectDirtyPages();
    backend.writePages(collected);
    cache.commitDirtyPages(collected);
    expect(cache.dirtyCount).toBe(0);

    cache.commitDirtyPages(collected);
    expect(cache.dirtyCount).toBe(0);
  });

  it("commitDirtyPages on evicted pages is a no-op", () => {
    const backend = new SyncMemoryBackend();
    const cache = new SyncPageCache(backend, 2);

    cache.write("/f", filledBuf(0xaa), 0, PAGE_SIZE, 0, 0);
    const collected = cache.collectDirtyPages();

    // Force eviction by filling cache
    cache.write("/g", filledBuf(0x11), 0, PAGE_SIZE, 0, 0);
    cache.write("/g", filledBuf(0x22), 0, PAGE_SIZE, PAGE_SIZE, PAGE_SIZE);

    // /f page 0 was evicted (and flushed during eviction)
    expect(cache.has("/f", 0)).toBe(false);

    // Commit should be a no-op for evicted pages
    cache.commitDirtyPages(collected);
    expect(cache.dirtyCount).toBe(2);
  });
});

describe("SyncPageCache: markPageDirty contract", () => {
  it("creates page in cache if not present @fast", () => {
    const backend = new SyncMemoryBackend();
    const cache = new SyncPageCache(backend, 16);

    expect(cache.has("/f", 3)).toBe(false);
    cache.markPageDirty("/f", 3);
    expect(cache.has("/f", 3)).toBe(true);
    expect(cache.isDirty("/f", 3)).toBe(true);
  });

  it("marks existing clean page as dirty", () => {
    const backend = new SyncMemoryBackend();
    const cache = new SyncPageCache(backend, 16);

    cache.write("/f", filledBuf(0xaa), 0, PAGE_SIZE, 0, 0);
    cache.flushFile("/f");
    expect(cache.isDirty("/f", 0)).toBe(false);
    expect(cache.has("/f", 0)).toBe(true);

    cache.markPageDirty("/f", 0);
    expect(cache.isDirty("/f", 0)).toBe(true);
  });

  it("is idempotent on already-dirty page", () => {
    const backend = new SyncMemoryBackend();
    const cache = new SyncPageCache(backend, 16);

    cache.write("/f", filledBuf(0xaa), 0, PAGE_SIZE, 0, 0);
    expect(cache.dirtyCount).toBe(1);

    cache.markPageDirty("/f", 0);
    expect(cache.dirtyCount).toBe(1);
  });

  it("marked page survives eviction and reload @fast", () => {
    const backend = new SyncMemoryBackend();
    const cache = new SyncPageCache(backend, 4);

    cache.markPageDirty("/f", 0);
    expect(cache.isDirty("/f", 0)).toBe(true);

    // Fill cache to force eviction of /f page 0
    for (let i = 0; i < 5; i++) {
      cache.write("/other", filledBuf(i), 0, PAGE_SIZE, i * PAGE_SIZE, i * PAGE_SIZE);
    }

    // The dirty page was flushed to backend during eviction
    const backendPage = backend.readPage("/f", 0);
    expect(backendPage).not.toBeNull();
    expect(backendPage!).toEqual(new Uint8Array(PAGE_SIZE));
  });

  it("marked page data is flushed by flushAll @fast", () => {
    const backend = new SyncMemoryBackend();
    const cache = new SyncPageCache(backend, 16);

    cache.markPageDirty("/f", 2);
    cache.flushAll();

    const page = backend.readPage("/f", 2);
    expect(page).not.toBeNull();
    expect(page!).toEqual(new Uint8Array(PAGE_SIZE));
    expect(cache.dirtyCount).toBe(0);
  });

  it("marked page is included in collectDirtyPages @fast", () => {
    const backend = new SyncMemoryBackend();
    const cache = new SyncPageCache(backend, 16);

    cache.markPageDirty("/f", 0);
    const collected = cache.collectDirtyPages();
    expect(collected.length).toBe(1);
    expect(collected[0].path).toBe("/f");
    expect(collected[0].pageIndex).toBe(0);
  });
});

describe("PageCache (async): collect/commit interleaving", () => {
  it("same page re-dirtied between collect and commit loses dirty flag @fast", async () => {
    const backend = new MemoryBackend();
    const cache = new PageCache(backend, 16);

    await cache.write("/f", filledBuf(0xaa), 0, PAGE_SIZE, 0, 0);
    expect(cache.dirtyCount).toBe(1);

    const collected = cache.collectDirtyPages();
    expect(collected.length).toBe(1);

    await cache.write("/f", filledBuf(0xbb), 0, PAGE_SIZE, 0, PAGE_SIZE);

    await backend.writePages(collected);
    cache.commitDirtyPages(collected);

    // Same limitation as SyncPageCache — dirty flag cleared
    expect(cache.dirtyCount).toBe(0);

    const readBuf = new Uint8Array(PAGE_SIZE);
    await cache.read("/f", readBuf, 0, PAGE_SIZE, 0, PAGE_SIZE);
    expect(readBuf).toEqual(filledBuf(0xbb));
  });

  it("writes to different file between collect and commit are preserved @fast", async () => {
    const backend = new MemoryBackend();
    const cache = new PageCache(backend, 16);

    await cache.write("/a", filledBuf(0xaa), 0, PAGE_SIZE, 0, 0);
    const collected = cache.collectDirtyPages();

    await cache.write("/b", filledBuf(0xbb), 0, PAGE_SIZE, 0, 0);

    await backend.writePages(collected);
    cache.commitDirtyPages(collected);

    expect(cache.isDirty("/a", 0)).toBe(false);
    expect(cache.isDirty("/b", 0)).toBe(true);
    expect(cache.dirtyCount).toBe(1);
  });

  it("newly dirtied pages not in collected set are preserved @fast", async () => {
    const backend = new MemoryBackend();
    const cache = new PageCache(backend, 16);

    await cache.write("/f", filledBuf(0xaa), 0, PAGE_SIZE, 0, 0);
    const collected = cache.collectDirtyPages();

    await cache.write("/f", filledBuf(0xbb), 0, PAGE_SIZE, PAGE_SIZE, PAGE_SIZE);

    await backend.writePages(collected);
    cache.commitDirtyPages(collected);

    expect(cache.isDirty("/f", 0)).toBe(false);
    expect(cache.isDirty("/f", 1)).toBe(true);
    expect(cache.dirtyCount).toBe(1);
  });
});
