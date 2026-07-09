/**
 * Unit tests for PageCache.assertInvariants() validation.
 *
 * Mirrors sync-page-cache-invariants.test.ts for the async PageCache.
 * The fuzz tests rely on assertInvariants() to detect corruption in the
 * five concurrent data structures the page cache manages (cache, mruPage,
 * filePages, dirtyKeys, dirtyFileKeys). If the invariant checker has a
 * bug — e.g., misses a certain type of violation — fuzz tests could pass
 * even when the cache is internally corrupted.
 *
 * These tests intentionally inject each type of invariant violation and
 * verify that assertInvariants() detects it with a descriptive error
 * message. Each test starts with a valid cache state, corrupts one
 * specific aspect, and checks detection.
 *
 * Invariants validated:
 *   1. Cache size within bounds (≤ maxPages)
 *   2. No evicted pages in the cache
 *   3. Every dirty key in dirtyKeys has dirty=true in cache
 *   4. Every page with dirty=true is tracked in dirtyKeys
 *   5. filePages covers exactly the keys in cache (bidirectional)
 *   6. dirtyFileKeys is consistent with dirtyKeys
 *   7. mruPage (if set) is in the cache
 *   8. Page key/path/pageIndex consistency
 */

import { describe, it, expect, beforeEach } from "vitest";
import { PageCache } from "../../src/page-cache.js";
import { MemoryBackend } from "../../src/memory-backend.js";
import { PAGE_SIZE, pageKeyStr } from "../../src/types.js";

function filledPage(value: number): Uint8Array {
  const buf = new Uint8Array(PAGE_SIZE);
  buf.fill(value);
  return buf;
}

describe("PageCache.assertInvariants() violation detection @fast", () => {
  let backend: MemoryBackend;
  let cache: PageCache;

  beforeEach(() => {
    backend = new MemoryBackend();
    cache = new PageCache(backend, 8);
  });

  function internals(): any {
    return cache as any;
  }

  async function setupValidState() {
    await cache.write("/fileA", filledPage(0xaa), 0, PAGE_SIZE, 0, 0);
    await cache.write("/fileA", filledPage(0xbb), 0, PAGE_SIZE, PAGE_SIZE, PAGE_SIZE);
    await cache.write("/fileB", filledPage(0xcc), 0, PAGE_SIZE, 0, 0);
    await cache.flushFile("/fileB");
  }

  it("valid state passes without error", async () => {
    await setupValidState();
    expect(() => cache.assertInvariants()).not.toThrow();
  });

  // ---------------------------------------------------------------
  // Invariant 1: Cache size within bounds
  // ---------------------------------------------------------------

  it("detects cache size exceeding maxPages", async () => {
    const tinyCache = new PageCache(backend, 2) as any;
    await tinyCache.write("/f", filledPage(1), 0, PAGE_SIZE, 0, 0);
    const key2 = pageKeyStr("/f", 1);
    const key3 = pageKeyStr("/f", 2);
    const page2 = { key: key2, path: "/f", pageIndex: 1, data: new Uint8Array(PAGE_SIZE), dirty: false, evicted: false };
    const page3 = { key: key3, path: "/f", pageIndex: 2, data: new Uint8Array(PAGE_SIZE), dirty: false, evicted: false };
    tinyCache.cache.set(key2, page2);
    tinyCache.cache.set(key3, page3);
    const fileKeys = tinyCache.filePages.get("/f");
    fileKeys.add(key2);
    fileKeys.add(key3);

    expect(() => (tinyCache as PageCache).assertInvariants()).toThrow(
      /cache size .* exceeds maxPages/,
    );
  });

  // ---------------------------------------------------------------
  // Invariant 2: No evicted pages in the cache
  // ---------------------------------------------------------------

  it("detects evicted page still in cache", async () => {
    await setupValidState();
    const c = internals();
    const firstEntry = c.cache.values().next().value;
    firstEntry.evicted = true;

    expect(() => cache.assertInvariants()).toThrow(
      /evicted page .* still in cache/,
    );
  });

  // ---------------------------------------------------------------
  // Invariant 3: Every dirty key exists in cache with dirty=true
  // ---------------------------------------------------------------

  it("detects dirtyKeys entry not in cache", async () => {
    await setupValidState();
    const c = internals();
    const phantomKey = pageKeyStr("/phantom", 99);
    c.dirtyKeys.add(phantomKey);

    expect(() => cache.assertInvariants()).toThrow(
      /dirtyKeys contains .* not in cache/,
    );
  });

  it("detects dirtyKeys entry with dirty=false in cache", async () => {
    await setupValidState();
    const c = internals();
    for (const key of c.dirtyKeys) {
      const page = c.cache.get(key);
      if (page) {
        page.dirty = false;
        break;
      }
    }

    expect(() => cache.assertInvariants()).toThrow(
      /dirtyKeys contains .* but page\.dirty is false/,
    );
  });

  // ---------------------------------------------------------------
  // Invariant 4: Every page with dirty=true is in dirtyKeys
  // ---------------------------------------------------------------

  it("detects dirty page not tracked in dirtyKeys", async () => {
    await setupValidState();
    const c = internals();
    for (const [, page] of c.cache) {
      if (!page.dirty) {
        page.dirty = true;
        break;
      }
    }

    expect(() => cache.assertInvariants()).toThrow(
      /page .* is dirty but not in dirtyKeys/,
    );
  });

  // ---------------------------------------------------------------
  // Invariant 5: filePages covers exactly the keys in cache
  // ---------------------------------------------------------------

  it("detects filePages entry not in cache", async () => {
    await setupValidState();
    const c = internals();
    const fileKeys = c.filePages.get("/fileA");
    const phantomKey = pageKeyStr("/fileA", 99);
    fileKeys.add(phantomKey);

    expect(() => cache.assertInvariants()).toThrow(
      /filePages\[.*\] contains .* not in cache/,
    );
  });

  it("detects filePages key under wrong path", async () => {
    await setupValidState();
    const c = internals();
    const fileAKeys = c.filePages.get("/fileA");
    const fileBKeys = c.filePages.get("/fileB");
    const stolenKey = fileAKeys.values().next().value;
    fileAKeys.delete(stolenKey);
    fileBKeys.add(stolenKey);

    expect(() => cache.assertInvariants()).toThrow(
      /filePages\[.*\] contains .* but page\.path is/,
    );
  });

  it("detects cache key not tracked in any filePages", async () => {
    await setupValidState();
    const c = internals();
    const fileAKeys = c.filePages.get("/fileA");
    const removedKey = fileAKeys.values().next().value;
    fileAKeys.delete(removedKey);

    expect(() => cache.assertInvariants()).toThrow(
      /cache key .* not tracked in filePages/,
    );
  });

  it("detects same key appearing under multiple paths in filePages", async () => {
    await setupValidState();
    const c = internals();
    const fileAKeys = c.filePages.get("/fileA");
    const fileBKeys = c.filePages.get("/fileB");
    const sharedKey = fileAKeys.values().next().value;
    fileBKeys.add(sharedKey);

    expect(() => cache.assertInvariants()).toThrow(
      /filePages key .* appears under multiple paths/,
    );
  });

  // ---------------------------------------------------------------
  // Invariant 6: dirtyFileKeys consistent with dirtyKeys
  // ---------------------------------------------------------------

  it("detects empty dirtyFileKeys set (should be deleted)", async () => {
    await setupValidState();
    const c = internals();
    c.dirtyFileKeys.set("/empty", new Set<string>());

    expect(() => cache.assertInvariants()).toThrow(
      /dirtyFileKeys\[.*\] is empty \(should be deleted\)/,
    );
  });

  it("detects dirtyFileKeys entry not in dirtyKeys", async () => {
    await setupValidState();
    const c = internals();
    const phantomKey = pageKeyStr("/fileA", 99);
    let dfk = c.dirtyFileKeys.get("/fileA");
    if (!dfk) {
      dfk = new Set<string>();
      c.dirtyFileKeys.set("/fileA", dfk);
    }
    dfk.add(phantomKey);

    expect(() => cache.assertInvariants()).toThrow(
      /dirtyFileKeys\[.*\] contains .* not in dirtyKeys/,
    );
  });

  it("detects dirtyFileKeys entry with wrong path in cache", async () => {
    await setupValidState();
    const c = internals();
    let fileADirtyKey: string | null = null;
    for (const key of c.dirtyKeys) {
      const page = c.cache.get(key);
      if (page && page.path === "/fileA") {
        fileADirtyKey = key;
        break;
      }
    }
    if (fileADirtyKey) {
      let dfk = c.dirtyFileKeys.get("/fileB");
      if (!dfk) {
        dfk = new Set<string>();
        c.dirtyFileKeys.set("/fileB", dfk);
      }
      dfk.add(fileADirtyKey);
    }

    expect(() => cache.assertInvariants()).toThrow(
      /dirtyFileKeys\[.*\] contains .* but page\.path is/,
    );
  });

  it("detects dirtyFileKeys entry not in filePages for same path", async () => {
    await setupValidState();
    const c = internals();
    await cache.write("/fileC", filledPage(0xdd), 0, PAGE_SIZE, 0, 0);
    expect(() => cache.assertInvariants()).not.toThrow();
    const key = pageKeyStr("/fileC", 0);
    const fileCKeys = c.filePages.get("/fileC");
    fileCKeys.delete(key);

    expect(() => cache.assertInvariants()).toThrow();
  });

  it("detects dirtyKeys entry not tracked in dirtyFileKeys", async () => {
    await setupValidState();
    const c = internals();
    for (const key of c.dirtyKeys) {
      const page = c.cache.get(key);
      if (page) {
        const dfk = c.dirtyFileKeys.get(page.path);
        if (dfk) {
          dfk.delete(key);
          if (dfk.size === 0) c.dirtyFileKeys.delete(page.path);
          break;
        }
      }
    }

    expect(() => cache.assertInvariants()).toThrow(
      /dirtyKeys contains .* not tracked in dirtyFileKeys/,
    );
  });

  // ---------------------------------------------------------------
  // Invariant 7: mruPage is in the cache
  // ---------------------------------------------------------------

  it("detects mruPage not in cache", async () => {
    await setupValidState();
    const c = internals();
    c.mruPage = {
      key: pageKeyStr("/ghost", 0),
      path: "/ghost",
      pageIndex: 0,
      data: new Uint8Array(PAGE_SIZE),
      dirty: false,
      evicted: false,
    };

    expect(() => cache.assertInvariants()).toThrow(
      /mruPage .* not in cache/,
    );
  });

  it("mruPage=null passes validation", async () => {
    await setupValidState();
    const c = internals();
    c.mruPage = null;
    expect(() => cache.assertInvariants()).not.toThrow();
  });

  // ---------------------------------------------------------------
  // Invariant 8: Page key/path/pageIndex consistency
  // ---------------------------------------------------------------

  it("detects page.key mismatch with cache map key", async () => {
    await setupValidState();
    const c = internals();
    const [, page] = c.cache.entries().next().value;
    page.key = "wrong-key";

    expect(() => cache.assertInvariants()).toThrow(
      /cache key .* but page\.key is/,
    );
  });

  it("detects key not matching pageKeyStr(path, pageIndex)", async () => {
    await setupValidState();
    const c = internals();
    const [, page] = c.cache.entries().next().value;
    page.pageIndex = 999;

    expect(() => cache.assertInvariants()).toThrow(
      /page key .* doesn't match pageKeyStr/,
    );
  });

  // ---------------------------------------------------------------
  // Compound violations: multiple invariants broken simultaneously
  // ---------------------------------------------------------------

  it("detects first violation in a multiply-corrupted cache", async () => {
    await setupValidState();
    const c = internals();
    const firstEntry = c.cache.values().next().value;
    firstEntry.evicted = true;
    c.dirtyKeys.add("phantom");

    expect(() => cache.assertInvariants()).toThrow();
  });

  // ---------------------------------------------------------------
  // Edge case: empty cache passes all invariants
  // ---------------------------------------------------------------

  it("empty cache passes all invariants", () => {
    expect(() => cache.assertInvariants()).not.toThrow();
  });

  // ---------------------------------------------------------------
  // Edge case: full cache at exact capacity passes
  // ---------------------------------------------------------------

  it("cache at exactly maxPages passes", async () => {
    for (let i = 0; i < 8; i++) {
      await cache.write("/full", filledPage(i), 0, PAGE_SIZE, i * PAGE_SIZE, i * PAGE_SIZE);
    }
    expect(cache.size).toBe(8);
    expect(() => cache.assertInvariants()).not.toThrow();
  });

  // ---------------------------------------------------------------
  // Invariant checks survive legitimate operations
  // ---------------------------------------------------------------

  it("passes after flush + read cycle", async () => {
    await setupValidState();
    await cache.flushAll();
    const buf = new Uint8Array(PAGE_SIZE);
    await cache.read("/fileA", buf, 0, PAGE_SIZE, 0, PAGE_SIZE * 2);
    expect(() => cache.assertInvariants()).not.toThrow();
  });

  it("passes after eviction fills cache", async () => {
    for (let i = 0; i < 12; i++) {
      await cache.write(`/evict${i}`, filledPage(i), 0, PAGE_SIZE, 0, 0);
    }
    expect(cache.size).toBe(8);
    expect(() => cache.assertInvariants()).not.toThrow();
  });

  it("passes after deleteFile", async () => {
    await setupValidState();
    await cache.deleteFile("/fileA");
    expect(() => cache.assertInvariants()).not.toThrow();
  });

  it("passes after renameFile", async () => {
    await setupValidState();
    await cache.flushFile("/fileA");
    await cache.renameFile("/fileA", "/fileC");
    expect(() => cache.assertInvariants()).not.toThrow();
  });

  it("passes after invalidatePagesFrom", async () => {
    await setupValidState();
    cache.invalidatePagesFrom("/fileA", 1);
    expect(() => cache.assertInvariants()).not.toThrow();
  });
});
