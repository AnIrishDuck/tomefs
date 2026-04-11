/**
 * Differential fuzz tests for SyncPageCache in isolation.
 *
 * Generates random sequences of page cache operations and executes them
 * against both the real SyncPageCache (with a SyncMemoryBackend) and a
 * simple Map-based reference model. After each operation, compares
 * observable state (read data, dirty counts, file contents after flush)
 * to verify the cache behaves identically to the reference.
 *
 * This catches bugs in the five concurrent data structures the cache
 * manages (cache Map, mruPage, filePages index, dirtyKeys set,
 * dirtyFileKeys map) that higher-level fuzz tests through the full
 * tomefs layer might miss — the tomefs layer adds its own logic for
 * metadata, node management, and persistence that can mask or compensate
 * for cache-level inconsistencies.
 *
 * Uses a seeded PRNG for reproducibility — failing seeds can be replayed.
 * Runs at multiple cache pressure levels to maximize eviction coverage.
 *
 * Ethos §9: "Write tests designed to break tomefs specifically —
 * target the seams: reads that span page boundaries, writes during
 * eviction, metadata updates after flush"
 */

import { describe, it, expect } from "vitest";
import { SyncPageCache } from "../../src/sync-page-cache.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { PAGE_SIZE, pageKeyStr } from "../../src/types.js";

// ---------------------------------------------------------------
// Seeded PRNG (xorshift128+) — same as differential.test.ts
// ---------------------------------------------------------------

class Rng {
  private s0: number;
  private s1: number;

  constructor(seed: number) {
    this.s0 = this.splitmix32(seed);
    this.s1 = this.splitmix32(this.s0);
    if (this.s0 === 0 && this.s1 === 0) this.s1 = 1;
  }

  private splitmix32(x: number): number {
    x = (x + 0x9e3779b9) | 0;
    x = Math.imul(x ^ (x >>> 16), 0x85ebca6b);
    x = Math.imul(x ^ (x >>> 13), 0xc2b2ae35);
    return (x ^ (x >>> 16)) >>> 0;
  }

  next(): number {
    let s0 = this.s0;
    let s1 = this.s1;
    const result = (s0 + s1) >>> 0;
    s1 ^= s0;
    this.s0 = ((s0 << 26) | (s0 >>> 6)) ^ s1 ^ (s1 << 9);
    this.s1 = (s1 << 13) | (s1 >>> 19);
    return result;
  }

  int(max: number): number {
    return this.next() % max;
  }

  pick<T>(arr: T[]): T {
    return arr[this.int(arr.length)];
  }

  /** Returns a buffer filled with a deterministic pattern.
   *  Uses a single RNG call for the seed value to avoid O(n) PRNG calls. */
  bytes(length: number): Uint8Array {
    const buf = new Uint8Array(length);
    const seed = this.next();
    for (let i = 0; i < length; i++) {
      buf[i] = ((seed + i) * 31 + 17) & 0xff;
    }
    return buf;
  }
}

// ---------------------------------------------------------------
// Reference model: simple Map-based page store
// ---------------------------------------------------------------

/**
 * Tracks expected file sizes and page data for comparison with SyncPageCache.
 * All operations are trivial Map manipulations — no LRU, no eviction, no
 * batching — so correctness is self-evident.
 */
class ReferenceModel {
  /** Maps "path\0pageIndex" → page data (copy). */
  private pages = new Map<string, Uint8Array>();
  /** Maps path → file size (logical). */
  fileSizes = new Map<string, number>();

  private key(path: string, pageIndex: number): string {
    return pageKeyStr(path, pageIndex);
  }

  write(
    path: string,
    buffer: Uint8Array,
    offset: number,
    length: number,
    position: number,
  ): void {
    const currentSize = this.fileSizes.get(path) ?? 0;
    let written = 0;
    let pos = position;

    while (written < length) {
      const pi = Math.floor(pos / PAGE_SIZE);
      const po = pos - pi * PAGE_SIZE;
      const bytesInPage = Math.min(PAGE_SIZE - po, length - written);

      const key = this.key(path, pi);
      let page = this.pages.get(key);
      if (!page) {
        page = new Uint8Array(PAGE_SIZE);
        this.pages.set(key, page);
      }
      page.set(
        buffer.subarray(offset + written, offset + written + bytesInPage),
        po,
      );

      written += bytesInPage;
      pos += bytesInPage;
    }

    this.fileSizes.set(path, Math.max(currentSize, position + length));
  }

  read(
    path: string,
    buffer: Uint8Array,
    offset: number,
    length: number,
    position: number,
  ): number {
    const size = this.fileSizes.get(path) ?? 0;
    const available = Math.max(0, size - position);
    const toRead = Math.min(length, available);
    if (toRead === 0) return 0;

    let bytesRead = 0;
    let pos = position;

    while (bytesRead < toRead) {
      const pi = Math.floor(pos / PAGE_SIZE);
      const po = pos - pi * PAGE_SIZE;
      const bytesInPage = Math.min(PAGE_SIZE - po, toRead - bytesRead);

      const key = this.key(path, pi);
      const page = this.pages.get(key);
      if (page) {
        buffer.set(
          page.subarray(po, po + bytesInPage),
          offset + bytesRead,
        );
      } else {
        // No page → zeros (already zero in fresh Uint8Array, but be explicit)
        buffer.fill(0, offset + bytesRead, offset + bytesRead + bytesInPage);
      }

      bytesRead += bytesInPage;
      pos += bytesInPage;
    }

    return bytesRead;
  }

  deleteFile(path: string): void {
    const size = this.fileSizes.get(path) ?? 0;
    const pageCount = size > 0 ? Math.ceil(size / PAGE_SIZE) : 0;
    for (let i = 0; i < pageCount; i++) {
      this.pages.delete(this.key(path, i));
    }
    // Also delete any pages beyond the tracked size (from prior renames etc.)
    for (const key of this.pages.keys()) {
      if (key.startsWith(path + "\0")) {
        this.pages.delete(key);
      }
    }
    this.fileSizes.delete(path);
  }

  renameFile(oldPath: string, newPath: string): void {
    if (oldPath === newPath) return;
    // Delete destination first
    this.deleteFile(newPath);
    // Move pages
    const toAdd: Array<[string, Uint8Array]> = [];
    for (const [key, data] of this.pages) {
      if (key.startsWith(oldPath + "\0")) {
        const suffix = key.substring(oldPath.length);
        toAdd.push([newPath + suffix, data]);
        this.pages.delete(key);
      }
    }
    for (const [key, data] of toAdd) {
      this.pages.set(key, data);
    }
    // Move size
    const size = this.fileSizes.get(oldPath) ?? 0;
    this.fileSizes.delete(oldPath);
    this.fileSizes.set(newPath, size);
  }

  truncate(path: string, newSize: number): void {
    const oldSize = this.fileSizes.get(path) ?? 0;
    if (newSize < oldSize) {
      // Shrink: zero tail of last surviving page, delete pages beyond
      const neededPages = newSize > 0 ? Math.ceil(newSize / PAGE_SIZE) : 0;
      const tailOffset = newSize % PAGE_SIZE;
      if (tailOffset > 0 && neededPages > 0) {
        const lastKey = this.key(path, neededPages - 1);
        const page = this.pages.get(lastKey);
        if (page) {
          page.fill(0, tailOffset);
        }
      }
      // Delete pages beyond the new size
      const maxOldPages = oldSize > 0 ? Math.ceil(oldSize / PAGE_SIZE) : 0;
      for (let i = neededPages; i < maxOldPages; i++) {
        this.pages.delete(this.key(path, i));
      }
    }
    // For grow: new pages are implicitly zero (read returns zeros)
    this.fileSizes.set(path, newSize);
  }

  /** Read a page directly for verification. */
  getPage(path: string, pageIndex: number): Uint8Array {
    return this.pages.get(this.key(path, pageIndex)) ?? new Uint8Array(PAGE_SIZE);
  }
}

// ---------------------------------------------------------------
// Operation types and weights
// ---------------------------------------------------------------

type OpType =
  | "write"
  | "read"
  | "writeFull"
  | "writeMulti"
  | "readMulti"
  | "flushFile"
  | "flushAll"
  | "deleteFile"
  | "renameFile"
  | "truncate"
  | "collectCommit"
  | "evictFile"
  | "getPage"
  | "markPageDirty";

const OP_WEIGHTS: Array<[OpType, number]> = [
  ["write", 20],
  ["read", 15],
  ["writeFull", 8],
  ["writeMulti", 8],
  ["readMulti", 8],
  ["flushFile", 5],
  ["flushAll", 3],
  ["deleteFile", 3],
  ["renameFile", 3],
  ["truncate", 4],
  ["collectCommit", 4],
  ["evictFile", 2],
  ["getPage", 6],
  ["markPageDirty", 3],
];

function pickOp(rng: Rng): OpType {
  const total = OP_WEIGHTS.reduce((s, [, w]) => s + w, 0);
  let r = rng.int(total);
  for (const [op, weight] of OP_WEIGHTS) {
    r -= weight;
    if (r < 0) return op;
  }
  return "write";
}

// ---------------------------------------------------------------
// Fuzz runner
// ---------------------------------------------------------------

const FILE_POOL = ["/a", "/b", "/c", "/d", "/e"];

// Reusable read buffers to reduce allocation pressure
const READ_BUF_1 = new Uint8Array(PAGE_SIZE * 3);
const READ_BUF_2 = new Uint8Array(PAGE_SIZE * 3);

function runFuzzSession(
  seed: number,
  maxPages: number,
  opCount: number,
): void {
  const rng = new Rng(seed);
  const backend = new SyncMemoryBackend();
  const cache = new SyncPageCache(backend, maxPages);
  const model = new ReferenceModel();

  // Track which files have been created (have data)
  const activeFiles = new Set<string>();

  for (let step = 0; step < opCount; step++) {
    const op = pickOp(rng);

    try {
      switch (op) {
        case "write": {
          // Sub-page write at random position within a file
          const path = rng.pick(FILE_POOL);
          const fileSize = model.fileSizes.get(path) ?? 0;
          // Cap max position to 4 pages to keep files small
          const maxPos = Math.min(Math.max(fileSize, PAGE_SIZE * 2), PAGE_SIZE * 4);
          const position = rng.int(maxPos);
          // Cap write size to quarter page to keep operations fast
          const length = rng.int(PAGE_SIZE / 4) + 1;
          const data = rng.bytes(length);

          cache.write(path, data, 0, length, position, fileSize);
          model.write(path, data, 0, length, position);
          activeFiles.add(path);
          break;
        }

        case "writeFull": {
          // Full-page-aligned write (exercises skip-backend-read optimization)
          const path = rng.pick(FILE_POOL);
          const fileSize = model.fileSizes.get(path) ?? 0;
          const pageIndex = rng.int(4);
          const position = pageIndex * PAGE_SIZE;
          const data = rng.bytes(PAGE_SIZE);

          cache.write(path, data, 0, PAGE_SIZE, position, fileSize);
          model.write(path, data, 0, PAGE_SIZE, position);
          activeFiles.add(path);
          break;
        }

        case "writeMulti": {
          // Multi-page write spanning 2-3 pages
          const path = rng.pick(FILE_POOL);
          const fileSize = model.fileSizes.get(path) ?? 0;
          const startPage = rng.int(3);
          const pageOffset = rng.int(PAGE_SIZE);
          const position = startPage * PAGE_SIZE + pageOffset;
          // Write just enough to cross a page boundary (PAGE_SIZE + 1 to 2*PAGE_SIZE)
          const crossLen = PAGE_SIZE - pageOffset + 1 + rng.int(PAGE_SIZE);
          const length = Math.min(crossLen, PAGE_SIZE * 2);
          const data = rng.bytes(length);

          cache.write(path, data, 0, length, position, fileSize);
          model.write(path, data, 0, length, position);
          activeFiles.add(path);
          break;
        }

        case "read": {
          // Read and compare data
          if (activeFiles.size === 0) break;
          const path = rng.pick([...activeFiles]);
          const fileSize = model.fileSizes.get(path) ?? 0;
          if (fileSize === 0) break;
          const position = rng.int(fileSize);
          const maxLen = Math.min(fileSize - position, PAGE_SIZE);
          if (maxLen === 0) break;
          const length = rng.int(maxLen) + 1;

          READ_BUF_1.fill(0, 0, length);
          READ_BUF_2.fill(0, 0, length);
          const cacheRead = cache.read(path, READ_BUF_1, 0, length, position, fileSize);
          const modelRead = model.read(path, READ_BUF_2, 0, length, position);

          expect(cacheRead).toBe(modelRead);
          expect(READ_BUF_1.subarray(0, length)).toEqual(READ_BUF_2.subarray(0, length));
          break;
        }

        case "readMulti": {
          // Multi-page read spanning 2+ pages
          if (activeFiles.size === 0) break;
          const path = rng.pick([...activeFiles]);
          const fileSize = model.fileSizes.get(path) ?? 0;
          if (fileSize < PAGE_SIZE + 1) break;
          const startPage = rng.int(Math.ceil(fileSize / PAGE_SIZE));
          const position = startPage * PAGE_SIZE;
          const maxLen = Math.min(fileSize - position, PAGE_SIZE * 2);
          if (maxLen <= PAGE_SIZE) break;
          const length = PAGE_SIZE + rng.int(maxLen - PAGE_SIZE) + 1;

          READ_BUF_1.fill(0, 0, length);
          READ_BUF_2.fill(0, 0, length);
          const cacheRead = cache.read(path, READ_BUF_1, 0, length, position, fileSize);
          const modelRead = model.read(path, READ_BUF_2, 0, length, position);

          expect(cacheRead).toBe(modelRead);
          expect(READ_BUF_1.subarray(0, length)).toEqual(READ_BUF_2.subarray(0, length));
          break;
        }

        case "getPage": {
          // Direct page access and comparison
          if (activeFiles.size === 0) break;
          const path = rng.pick([...activeFiles]);
          const fileSize = model.fileSizes.get(path) ?? 0;
          if (fileSize === 0) break;
          const pageCount = Math.ceil(fileSize / PAGE_SIZE);
          const pageIndex = rng.int(pageCount);

          const cachedPage = cache.getPage(path, pageIndex);
          const expected = model.getPage(path, pageIndex);
          expect(cachedPage.data).toEqual(expected);
          break;
        }

        case "flushFile": {
          if (activeFiles.size === 0) break;
          const path = rng.pick([...activeFiles]);
          cache.flushFile(path);
          break;
        }

        case "flushAll": {
          cache.flushAll();
          break;
        }

        case "collectCommit": {
          // Two-phase dirty commit: collect → commit
          const dirty = cache.collectDirtyPages();
          // Dirty count should remain positive until commit
          if (dirty.length > 0) {
            expect(cache.dirtyCount).toBeGreaterThanOrEqual(dirty.length);
          }
          // Write to backend (simulates what tomefs syncfs does)
          backend.writePages(dirty);
          cache.commitDirtyPages(dirty);
          expect(cache.dirtyCount).toBe(0);
          break;
        }

        case "deleteFile": {
          if (activeFiles.size === 0) break;
          const path = rng.pick([...activeFiles]);
          cache.deleteFile(path);
          model.deleteFile(path);
          activeFiles.delete(path);
          break;
        }

        case "renameFile": {
          if (activeFiles.size === 0) break;
          const oldPath = rng.pick([...activeFiles]);
          const newPath = rng.pick(FILE_POOL);
          if (oldPath === newPath) break;

          cache.renameFile(oldPath, newPath);
          model.renameFile(oldPath, newPath);
          activeFiles.delete(oldPath);
          activeFiles.add(newPath);
          break;
        }

        case "truncate": {
          if (activeFiles.size === 0) break;
          const path = rng.pick([...activeFiles]);
          const fileSize = model.fileSizes.get(path) ?? 0;
          // Mix of shrink and grow (cap at 4 pages to keep files small)
          const newSize = rng.int(2) === 0
            ? rng.int(Math.max(fileSize, 1)) // shrink
            : Math.min(fileSize + rng.int(PAGE_SIZE), PAGE_SIZE * 4); // grow
          if (newSize === fileSize) break;

          if (newSize < fileSize) {
            // Shrink: zero tail, invalidate pages, delete from backend
            cache.zeroTailAfterTruncate(path, newSize);
            const neededPages = newSize > 0 ? Math.ceil(newSize / PAGE_SIZE) : 0;
            cache.invalidatePagesFrom(path, neededPages);
            backend.deletePagesFrom(path, neededPages);
          } else {
            // Grow: mark sentinel page dirty
            const lastPageIdx = Math.ceil(newSize / PAGE_SIZE) - 1;
            const firstNewPage = fileSize > 0 ? Math.ceil(fileSize / PAGE_SIZE) : 0;
            if (lastPageIdx >= firstNewPage) {
              cache.markPageDirty(path, lastPageIdx);
            }
          }
          model.truncate(path, newSize);
          break;
        }

        case "evictFile": {
          if (activeFiles.size === 0) break;
          const path = rng.pick([...activeFiles]);
          cache.evictFile(path);
          // Data should still be readable (reloaded from backend on next access)
          const fileSize = model.fileSizes.get(path) ?? 0;
          if (fileSize > 0) {
            const readLen = Math.min(fileSize, PAGE_SIZE);
            const cacheBuf = new Uint8Array(readLen);
            const modelBuf = new Uint8Array(readLen);
            cache.read(path, cacheBuf, 0, readLen, 0, fileSize);
            model.read(path, modelBuf, 0, readLen, 0);
            expect(cacheBuf).toEqual(modelBuf);
          }
          break;
        }

        case "markPageDirty": {
          const path = rng.pick(FILE_POOL);
          const pageIndex = rng.int(4);
          cache.markPageDirty(path, pageIndex);
          // markPageDirty on the cache doesn't change data — it just ensures
          // the page exists and is dirty. The model doesn't need updating.
          // But we need to track it if it creates a new file.
          if (!model.fileSizes.has(path)) {
            // The page was created but with zero data. The model should know
            // about this file's pages for verification.
            model.fileSizes.set(path, 0);
          }
          activeFiles.add(path);
          break;
        }
      }
    } catch (e) {
      throw new Error(
        `Seed ${seed}, step ${step}, op ${op} failed: ${(e as Error).message}`,
      );
    }
  }

  // Final verification: flush everything and compare all files
  cache.flushAll();
  for (const path of activeFiles) {
    verifyBackendFile(backend, model, path);
  }
}

/**
 * Verify that all pages of a file in the backend match the reference model.
 */
function verifyBackendFile(
  backend: SyncMemoryBackend,
  model: ReferenceModel,
  path: string,
): void {
  const fileSize = model.fileSizes.get(path) ?? 0;
  const pageCount = fileSize > 0 ? Math.ceil(fileSize / PAGE_SIZE) : 0;

  for (let i = 0; i < pageCount; i++) {
    const backendPage = backend.readPage(path, i);
    const modelPage = model.getPage(path, i);
    if (backendPage) {
      expect(backendPage).toEqual(modelPage);
    } else {
      // No backend page means all zeros
      expect(modelPage).toEqual(new Uint8Array(PAGE_SIZE));
    }
  }
}

// ---------------------------------------------------------------
// Test cases
// ---------------------------------------------------------------

describe("SyncPageCache differential fuzz", () => {
  // Large cache (no eviction pressure) — exercises basic correctness
  describe("large cache (64 pages)", () => {
    const MAX_PAGES = 64;
    const OPS = 200;

    for (let seed = 1; seed <= 5; seed++) {
      it(`seed ${seed} (${OPS} ops, ${MAX_PAGES}-page cache) @fast`, () => {
        runFuzzSession(seed, MAX_PAGES, OPS);
      });
    }
  });

  // Medium cache (moderate eviction) — exercises LRU + dirty flush on eviction
  describe("medium cache (8 pages)", () => {
    const MAX_PAGES = 8;
    const OPS = 200;

    for (let seed = 100; seed <= 110; seed++) {
      const tag = seed <= 102 ? " @fast" : "";
      it(`seed ${seed} (${OPS} ops, ${MAX_PAGES}-page cache)${tag}`, () => {
        runFuzzSession(seed, MAX_PAGES, OPS);
      });
    }
  });

  // Tiny cache (extreme eviction pressure) — exercises batch eviction, dirty tracking
  describe("tiny cache (3 pages)", () => {
    const MAX_PAGES = 3;
    const OPS = 150;

    for (let seed = 200; seed <= 208; seed++) {
      it(`seed ${seed} (${OPS} ops, ${MAX_PAGES}-page cache)`, () => {
        runFuzzSession(seed, MAX_PAGES, OPS);
      });
    }
  });

  // Long-running sessions with moderate pressure — exercises index consistency
  describe("extended sessions (16-page cache, 500 ops)", () => {
    const MAX_PAGES = 16;
    const OPS = 500;

    for (let seed = 300; seed <= 304; seed++) {
      it(`seed ${seed} (${OPS} ops, ${MAX_PAGES}-page cache)`, () => {
        runFuzzSession(seed, MAX_PAGES, OPS);
      });
    }
  });

  // Minimal cache (2 pages) — nearly every access evicts
  describe("minimal cache (2 pages)", () => {
    const MAX_PAGES = 2;
    const OPS = 100;

    for (let seed = 400; seed <= 404; seed++) {
      it(`seed ${seed} (${OPS} ops, ${MAX_PAGES}-page cache)`, () => {
        runFuzzSession(seed, MAX_PAGES, OPS);
      });
    }
  });
});
