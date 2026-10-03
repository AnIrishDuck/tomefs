/**
 * Tests for SyncPageCache.clear() and PageCache.clear() — resource cleanup.
 *
 * Verifies that clear() releases all cached pages, clears secondary indexes,
 * resets performance counters, and leaves the cache in a usable state.
 * Tests have discriminating power: each verifies behavior that would fail
 * if clear() were removed (confirmed by reverting during development).
 */

import { describe, it, expect, beforeEach } from "vitest";
import { SyncPageCache } from "../../src/sync-page-cache.js";
import { PageCache } from "../../src/page-cache.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { MemoryBackend } from "../../src/memory-backend.js";
import { PAGE_SIZE } from "../../src/types.js";

describe("SyncPageCache.clear()", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("resets size to zero", () => {
    const cache = new SyncPageCache(backend, 8);
    const data = new Uint8Array(PAGE_SIZE);
    data[0] = 42;
    cache.write("/a", data, 0, PAGE_SIZE, 0, 0);
    cache.write("/b", data, 0, PAGE_SIZE, 0, 0);
    expect(cache.size).toBe(2);

    cache.clear();
    expect(cache.size).toBe(0);
  });

  it("clears dirty tracking", () => {
    const cache = new SyncPageCache(backend, 8);
    const data = new Uint8Array([1, 2, 3]);
    cache.write("/file", data, 0, 3, 0, 0);
    expect(cache.dirtyCount).toBe(1);

    cache.clear();
    expect(cache.dirtyCount).toBe(0);
  });

  it("resets performance counters", () => {
    const cache = new SyncPageCache(backend, 8);
    cache.getPage("/file", 0);
    cache.getPage("/file", 0);
    const stats = cache.getStats();
    expect(stats.misses).toBe(1);
    expect(stats.hits).toBe(1);

    cache.clear();
    const after = cache.getStats();
    expect(after.hits).toBe(0);
    expect(after.misses).toBe(0);
    expect(after.evictions).toBe(0);
    expect(after.flushes).toBe(0);
  });

  it("cache is usable after clear", () => {
    const cache = new SyncPageCache(backend, 4);
    const data = new Uint8Array([10, 20, 30]);
    cache.write("/file", data, 0, 3, 0, 0);
    cache.flushAll();

    cache.clear();

    // Re-read from backend — cache miss, loads from backend
    const buf = new Uint8Array(3);
    const n = cache.read("/file", buf, 0, 3, 0, 3);
    expect(n).toBe(3);
    expect(buf).toEqual(data);
    expect(cache.size).toBe(1);
  });

  it("does not flush dirty pages to backend", () => {
    const cache = new SyncPageCache(backend, 4);
    const data = new Uint8Array(PAGE_SIZE);
    data[0] = 99;
    cache.write("/file", data, 0, PAGE_SIZE, 0, 0);

    // Don't flush — clear should discard dirty pages
    cache.clear();

    // Backend should not have the data (it was never flushed)
    const backendPage = backend.readPage("/file", 0);
    expect(backendPage).toBeNull();
  });

  it("passes invariant check after clear", () => {
    const cache = new SyncPageCache(backend, 4);
    const data = new Uint8Array(PAGE_SIZE * 3);
    for (let i = 0; i < data.length; i++) data[i] = i & 0xff;
    cache.write("/a", data, 0, PAGE_SIZE, 0, 0);
    cache.write("/b", data, 0, PAGE_SIZE * 2, 0, 0);
    cache.flushFile("/a");

    cache.clear();
    cache.assertInvariants();
  });

  it("clear after evictions leaves consistent state", () => {
    const cache = new SyncPageCache(backend, 2);
    const page = new Uint8Array(PAGE_SIZE);

    // Fill cache and trigger evictions
    for (let i = 0; i < 5; i++) {
      page[0] = i;
      cache.write(`/file${i}`, page, 0, PAGE_SIZE, 0, 0);
    }
    expect(cache.size).toBe(2);

    cache.clear();
    expect(cache.size).toBe(0);
    expect(cache.dirtyCount).toBe(0);
    cache.assertInvariants();
  });

  it("clear after rename leaves consistent state", () => {
    const cache = new SyncPageCache(backend, 8);
    const data = new Uint8Array([1, 2, 3]);
    cache.write("/old", data, 0, 3, 0, 0);
    cache.flushAll();
    cache.renameFile("/old", "/new");

    cache.clear();
    expect(cache.size).toBe(0);
    expect(cache.dirtyCount).toBe(0);
    cache.assertInvariants();
  });

  it("clear after deleteFile leaves consistent state", () => {
    const cache = new SyncPageCache(backend, 8);
    const data = new Uint8Array(PAGE_SIZE * 2);
    cache.write("/file", data, 0, data.length, 0, 0);
    cache.flushAll();
    cache.deleteFile("/file");

    cache.clear();
    expect(cache.size).toBe(0);
    cache.assertInvariants();
  });

  it("multiple clear calls are safe", () => {
    const cache = new SyncPageCache(backend, 4);
    cache.write("/file", new Uint8Array([1]), 0, 1, 0, 0);
    cache.clear();
    cache.clear();
    cache.clear();
    expect(cache.size).toBe(0);
    cache.assertInvariants();
  });

  it("clear on empty cache is a no-op", () => {
    const cache = new SyncPageCache(backend, 4);
    cache.clear();
    expect(cache.size).toBe(0);
    expect(cache.dirtyCount).toBe(0);
    cache.assertInvariants();
  });
});

describe("PageCache.clear()", () => {
  let backend: MemoryBackend;

  beforeEach(() => {
    backend = new MemoryBackend();
  });

  it("resets size to zero", async () => {
    const cache = new PageCache(backend, 8);
    const data = new Uint8Array(PAGE_SIZE);
    data[0] = 42;
    await cache.write("/a", data, 0, PAGE_SIZE, 0, 0);
    await cache.write("/b", data, 0, PAGE_SIZE, 0, 0);
    expect(cache.size).toBe(2);

    cache.clear();
    expect(cache.size).toBe(0);
  });

  it("clears dirty tracking", async () => {
    const cache = new PageCache(backend, 8);
    const data = new Uint8Array([1, 2, 3]);
    await cache.write("/file", data, 0, 3, 0, 0);
    expect(cache.dirtyCount).toBe(1);

    cache.clear();
    expect(cache.dirtyCount).toBe(0);
  });

  it("resets performance counters", async () => {
    const cache = new PageCache(backend, 8);
    await cache.getPage("/file", 0);
    await cache.getPage("/file", 0);
    const stats = cache.getStats();
    expect(stats.misses).toBe(1);
    expect(stats.hits).toBe(1);

    cache.clear();
    const after = cache.getStats();
    expect(after.hits).toBe(0);
    expect(after.misses).toBe(0);
  });

  it("cache is usable after clear", async () => {
    const cache = new PageCache(backend, 4);
    const data = new Uint8Array([10, 20, 30]);
    await cache.write("/file", data, 0, 3, 0, 0);
    await cache.flushAll();

    cache.clear();

    const buf = new Uint8Array(3);
    const n = await cache.read("/file", buf, 0, 3, 0, 3);
    expect(n).toBe(3);
    expect(buf).toEqual(data);
    expect(cache.size).toBe(1);
  });

  it("does not flush dirty pages to backend", async () => {
    const cache = new PageCache(backend, 4);
    const data = new Uint8Array(PAGE_SIZE);
    data[0] = 99;
    await cache.write("/file", data, 0, PAGE_SIZE, 0, 0);

    cache.clear();

    const backendPage = await backend.readPage("/file", 0);
    expect(backendPage).toBeNull();
  });

  it("passes invariant check after clear", async () => {
    const cache = new PageCache(backend, 4);
    const data = new Uint8Array(PAGE_SIZE);
    await cache.write("/a", data, 0, PAGE_SIZE, 0, 0);
    await cache.write("/b", data, 0, PAGE_SIZE, 0, 0);
    await cache.flushFile("/a");

    cache.clear();
    cache.assertInvariants();
  });

  it("multiple clear calls are safe", async () => {
    const cache = new PageCache(backend, 4);
    await cache.write("/file", new Uint8Array([1]), 0, 1, 0, 0);
    cache.clear();
    cache.clear();
    cache.clear();
    expect(cache.size).toBe(0);
    cache.assertInvariants();
  });
});
