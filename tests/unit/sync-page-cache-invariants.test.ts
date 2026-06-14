/**
 * Unit tests for SyncPageCache.assertInvariants() validation.
 *
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
import { SyncPageCache } from "../../src/sync-page-cache.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { PAGE_SIZE, pageKeyStr } from "../../src/types.js";

function filledPage(value: number): Uint8Array {
  const buf = new Uint8Array(PAGE_SIZE);
  buf.fill(value);
  return buf;
}

describe("SyncPageCache.assertInvariants() violation detection @fast", () => {
  let backend: SyncMemoryBackend;
  let cache: SyncPageCache;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
    cache = new SyncPageCache(backend, 8);
  });

  function internals(): any {
    return cache as any;
  }

  function setupValidState() {
    // Write 3 pages across 2 files to create a rich internal state
    cache.write("/fileA", filledPage(0xaa), 0, PAGE_SIZE, 0, 0);
    cache.write("/fileA", filledPage(0xbb), 0, PAGE_SIZE, PAGE_SIZE, PAGE_SIZE);
    cache.write("/fileB", filledPage(0xcc), 0, PAGE_SIZE, 0, 0);
    // Flush one file to have a mix of dirty/clean pages
    cache.flushFile("/fileB");
  }

  it("valid state passes without error", () => {
    setupValidState();
    expect(() => cache.assertInvariants()).not.toThrow();
  });

  // ---------------------------------------------------------------
  // Invariant 1: Cache size within bounds
  // ---------------------------------------------------------------

  it("detects cache size exceeding maxPages", () => {
    setupValidState();
    // Manually insert an extra page beyond maxPages by reducing the limit
    // We can't easily exceed the real limit, so create a cache with maxPages=2
    // and put 3 pages in it
    const tinyCache = new SyncPageCache(backend, 2) as any;
    // Write one page normally
    tinyCache.write("/f", filledPage(1), 0, PAGE_SIZE, 0, 0);
    // Force-insert extra pages bypassing capacity check
    const key2 = pageKeyStr("/f", 1);
    const key3 = pageKeyStr("/f", 2);
    const page2 = { key: key2, path: "/f", pageIndex: 1, data: new Uint8Array(PAGE_SIZE), dirty: false, evicted: false };
    const page3 = { key: key3, path: "/f", pageIndex: 2, data: new Uint8Array(PAGE_SIZE), dirty: false, evicted: false };
    tinyCache.cache.set(key2, page2);
    tinyCache.cache.set(key3, page3);
    const fileKeys = tinyCache.filePages.get("/f");
    fileKeys.add(key2);
    fileKeys.add(key3);

    expect(() => (tinyCache as SyncPageCache).assertInvariants()).toThrow(
      /cache size .* exceeds maxPages/,
    );
  });

  // ---------------------------------------------------------------
  // Invariant 2: No evicted pages in the cache
  // ---------------------------------------------------------------

  it("detects evicted page still in cache", () => {
    setupValidState();
    const c = internals();
    // Mark a page as evicted without removing from cache
    const firstEntry = c.cache.values().next().value;
    firstEntry.evicted = true;

    expect(() => cache.assertInvariants()).toThrow(
      /evicted page .* still in cache/,
    );
  });

  // ---------------------------------------------------------------
  // Invariant 3: Every dirty key exists in cache with dirty=true
  // ---------------------------------------------------------------

  it("detects dirtyKeys entry not in cache", () => {
    setupValidState();
    const c = internals();
    // Add a phantom key to dirtyKeys
    const phantomKey = pageKeyStr("/phantom", 99);
    c.dirtyKeys.add(phantomKey);

    expect(() => cache.assertInvariants()).toThrow(
      /dirtyKeys contains .* not in cache/,
    );
  });

  it("detects dirtyKeys entry with dirty=false in cache", () => {
    setupValidState();
    const c = internals();
    // Find a dirty page and clear its flag without removing from dirtyKeys
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

  it("detects dirty page not tracked in dirtyKeys", () => {
    setupValidState();
    const c = internals();
    // Find a clean page and mark it dirty without adding to dirtyKeys
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

  it("detects filePages entry not in cache", () => {
    setupValidState();
    const c = internals();
    // Add a phantom key to filePages for an existing file
    const fileKeys = c.filePages.get("/fileA");
    const phantomKey = pageKeyStr("/fileA", 99);
    fileKeys.add(phantomKey);

    expect(() => cache.assertInvariants()).toThrow(
      /filePages\[.*\] contains .* not in cache/,
    );
  });

  it("detects filePages key under wrong path", () => {
    setupValidState();
    const c = internals();
    // Move a key from fileA's set to fileB's set (cross-contamination)
    const fileAKeys = c.filePages.get("/fileA");
    const fileBKeys = c.filePages.get("/fileB");
    // Get a key that belongs to /fileA
    const stolenKey = fileAKeys.values().next().value;
    fileAKeys.delete(stolenKey);
    fileBKeys.add(stolenKey);

    expect(() => cache.assertInvariants()).toThrow(
      /filePages\[.*\] contains .* but page\.path is/,
    );
  });

  it("detects cache key not tracked in any filePages", () => {
    setupValidState();
    const c = internals();
    // Remove a key from filePages without removing from cache
    const fileAKeys = c.filePages.get("/fileA");
    const removedKey = fileAKeys.values().next().value;
    fileAKeys.delete(removedKey);

    expect(() => cache.assertInvariants()).toThrow(
      /cache key .* not tracked in filePages/,
    );
  });

  it("detects same key appearing under multiple paths in filePages", () => {
    setupValidState();
    const c = internals();
    // Add the same key to both file's sets
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

  it("detects empty dirtyFileKeys set (should be deleted)", () => {
    setupValidState();
    const c = internals();
    // Add an empty set for a path
    c.dirtyFileKeys.set("/empty", new Set<string>());

    expect(() => cache.assertInvariants()).toThrow(
      /dirtyFileKeys\[.*\] is empty \(should be deleted\)/,
    );
  });

  it("detects dirtyFileKeys entry not in dirtyKeys", () => {
    setupValidState();
    const c = internals();
    // Add a key to dirtyFileKeys that isn't in dirtyKeys
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

  it("detects dirtyFileKeys entry with wrong path in cache", () => {
    setupValidState();
    const c = internals();
    // Get a dirty key from /fileA and add it to /fileB's dirtyFileKeys
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

  it("detects dirtyFileKeys entry not in filePages for same path", () => {
    setupValidState();
    const c = internals();
    // Create a scenario where a key is in dirtyKeys and dirtyFileKeys
    // but not in filePages for that path
    // First, write a page to create a valid dirty state
    cache.write("/fileC", filledPage(0xdd), 0, PAGE_SIZE, 0, 0);
    // Verify valid
    expect(() => cache.assertInvariants()).not.toThrow();
    // Now remove from filePages but keep in dirtyFileKeys and dirtyKeys
    const key = pageKeyStr("/fileC", 0);
    const fileCKeys = c.filePages.get("/fileC");
    fileCKeys.delete(key);

    // This should trigger either "cache key not tracked in filePages"
    // or the dirtyFileKeys check depending on iteration order
    expect(() => cache.assertInvariants()).toThrow();
  });

  it("detects dirtyKeys entry not tracked in dirtyFileKeys", () => {
    setupValidState();
    const c = internals();
    // Find a dirty key and remove it from dirtyFileKeys without
    // removing from dirtyKeys
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

  it("detects mruPage not in cache", () => {
    setupValidState();
    const c = internals();
    // Set mruPage to a fake page that's not in the cache
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

  it("mruPage=null passes validation", () => {
    setupValidState();
    const c = internals();
    c.mruPage = null;
    expect(() => cache.assertInvariants()).not.toThrow();
  });

  // ---------------------------------------------------------------
  // Invariant 8: Page key/path/pageIndex consistency
  // ---------------------------------------------------------------

  it("detects page.key mismatch with cache map key", () => {
    setupValidState();
    const c = internals();
    // Change a page's key field to not match the map key
    const [, page] = c.cache.entries().next().value;
    page.key = "wrong-key";

    expect(() => cache.assertInvariants()).toThrow(
      /cache key .* but page\.key is/,
    );
  });

  it("detects key not matching pageKeyStr(path, pageIndex)", () => {
    setupValidState();
    const c = internals();
    // Change a page's pageIndex so the derived key doesn't match
    const [, page] = c.cache.entries().next().value;
    page.pageIndex = 999;

    expect(() => cache.assertInvariants()).toThrow(
      /page key .* doesn't match pageKeyStr/,
    );
  });

  // ---------------------------------------------------------------
  // Compound violations: multiple invariants broken simultaneously
  // ---------------------------------------------------------------

  it("detects first violation in a multiply-corrupted cache", () => {
    setupValidState();
    const c = internals();
    // Corrupt both evicted flag and dirtyKeys — first detected wins
    const firstEntry = c.cache.values().next().value;
    firstEntry.evicted = true;
    c.dirtyKeys.add("phantom");

    // Should throw (either invariant 2 or 3, depending on iteration order)
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

  it("cache at exactly maxPages passes", () => {
    // maxPages = 8, write exactly 8 pages
    for (let i = 0; i < 8; i++) {
      cache.write("/full", filledPage(i), 0, PAGE_SIZE, i * PAGE_SIZE, i * PAGE_SIZE);
    }
    expect(cache.size).toBe(8);
    expect(() => cache.assertInvariants()).not.toThrow();
  });

  // ---------------------------------------------------------------
  // Invariant checks survive legitimate operations
  // ---------------------------------------------------------------

  it("passes after flush + read cycle", () => {
    setupValidState();
    cache.flushAll();
    // Read triggers cache hit
    const buf = new Uint8Array(PAGE_SIZE);
    cache.read("/fileA", buf, 0, PAGE_SIZE, 0, PAGE_SIZE * 2);
    expect(() => cache.assertInvariants()).not.toThrow();
  });

  it("passes after eviction fills cache", () => {
    // Write enough pages to force eviction (maxPages = 8)
    for (let i = 0; i < 12; i++) {
      cache.write(`/evict${i}`, filledPage(i), 0, PAGE_SIZE, 0, 0);
    }
    expect(cache.size).toBe(8);
    expect(() => cache.assertInvariants()).not.toThrow();
  });

  it("passes after deleteFile", () => {
    setupValidState();
    cache.deleteFile("/fileA");
    expect(() => cache.assertInvariants()).not.toThrow();
  });

  it("passes after renameFile", () => {
    setupValidState();
    cache.flushFile("/fileA");
    cache.renameFile("/fileA", "/fileC");
    expect(() => cache.assertInvariants()).not.toThrow();
  });

  it("passes after invalidatePagesFrom", () => {
    setupValidState();
    cache.invalidatePagesFrom("/fileA", 1);
    expect(() => cache.assertInvariants()).not.toThrow();
  });
});
