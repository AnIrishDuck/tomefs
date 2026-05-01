/**
 * Differential fuzz tests for the async PageCache in isolation.
 *
 * Mirrors tests/fuzz/sync-page-cache.test.ts but exercises the async
 * PageCache (which wraps an async StorageBackend). The async variant
 * has `await` points in getPage, ensureCapacity, evictOne, and
 * batchEvict that introduce different control flow compared to the
 * synchronous SyncPageCache. These tests verify that the five
 * concurrent data structures (cache Map, mruPage, filePages index,
 * dirtyKeys set, dirtyFileKeys map) remain consistent across await
 * boundaries.
 *
 * Generates random sequences of page cache operations and executes
 * them against both the real PageCache (with a MemoryBackend) and a
 * simple Map-based reference model. After each operation, compares
 * observable state (read data, dirty counts, file contents after flush)
 * to verify the cache behaves identically to the reference.
 *
 * Uses a seeded PRNG for reproducibility — failing seeds can be replayed.
 * Runs at multiple cache pressure levels to maximize eviction coverage.
 *
 * Ethos §9: "Write tests designed to break tomefs specifically —
 * target the seams: reads that span page boundaries, writes during
 * eviction, metadata updates after flush"
 */

import { describe, it, expect } from "vitest";
import { PageCache } from "../../src/page-cache.js";
import { SyncPageCache } from "../../src/sync-page-cache.js";
import { MemoryBackend } from "../../src/memory-backend.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { PAGE_SIZE, pageKeyStr } from "../../src/types.js";

// ---------------------------------------------------------------
// Seeded PRNG (xorshift128+) — same as other fuzz tests
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

class ReferenceModel {
  private pages = new Map<string, Uint8Array>();
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
    for (const key of this.pages.keys()) {
      if (key.startsWith(path + "\0")) {
        this.pages.delete(key);
      }
    }
    this.fileSizes.delete(path);
  }

  renameFile(oldPath: string, newPath: string): void {
    if (oldPath === newPath) return;
    this.deleteFile(newPath);
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
    const size = this.fileSizes.get(oldPath) ?? 0;
    this.fileSizes.delete(oldPath);
    this.fileSizes.set(newPath, size);
  }

  truncate(path: string, newSize: number): void {
    const oldSize = this.fileSizes.get(path) ?? 0;
    if (newSize < oldSize) {
      const neededPages = newSize > 0 ? Math.ceil(newSize / PAGE_SIZE) : 0;
      const tailOffset = newSize % PAGE_SIZE;
      if (tailOffset > 0 && neededPages > 0) {
        const lastKey = this.key(path, neededPages - 1);
        const page = this.pages.get(lastKey);
        if (page) {
          page.fill(0, tailOffset);
        }
      }
      const maxOldPages = oldSize > 0 ? Math.ceil(oldSize / PAGE_SIZE) : 0;
      for (let i = neededPages; i < maxOldPages; i++) {
        this.pages.delete(this.key(path, i));
      }
    }
    this.fileSizes.set(path, newSize);
  }

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

const READ_BUF_1 = new Uint8Array(PAGE_SIZE * 3);
const READ_BUF_2 = new Uint8Array(PAGE_SIZE * 3);

async function runFuzzSession(
  seed: number,
  maxPages: number,
  opCount: number,
): Promise<void> {
  const rng = new Rng(seed);
  const backend = new MemoryBackend();
  const cache = new PageCache(backend, maxPages);
  const model = new ReferenceModel();

  const activeFiles = new Set<string>();

  for (let step = 0; step < opCount; step++) {
    const op = pickOp(rng);

    try {
      switch (op) {
        case "write": {
          const path = rng.pick(FILE_POOL);
          const fileSize = model.fileSizes.get(path) ?? 0;
          const maxPos = Math.min(Math.max(fileSize, PAGE_SIZE * 2), PAGE_SIZE * 4);
          const position = rng.int(maxPos);
          const length = rng.int(PAGE_SIZE / 4) + 1;
          const data = rng.bytes(length);

          await cache.write(path, data, 0, length, position, fileSize);
          model.write(path, data, 0, length, position);
          activeFiles.add(path);
          break;
        }

        case "writeFull": {
          const path = rng.pick(FILE_POOL);
          const fileSize = model.fileSizes.get(path) ?? 0;
          const pageIndex = rng.int(4);
          const position = pageIndex * PAGE_SIZE;
          const data = rng.bytes(PAGE_SIZE);

          await cache.write(path, data, 0, PAGE_SIZE, position, fileSize);
          model.write(path, data, 0, PAGE_SIZE, position);
          activeFiles.add(path);
          break;
        }

        case "writeMulti": {
          const path = rng.pick(FILE_POOL);
          const fileSize = model.fileSizes.get(path) ?? 0;
          const startPage = rng.int(3);
          const pageOffset = rng.int(PAGE_SIZE);
          const position = startPage * PAGE_SIZE + pageOffset;
          const crossLen = PAGE_SIZE - pageOffset + 1 + rng.int(PAGE_SIZE);
          const length = Math.min(crossLen, PAGE_SIZE * 2);
          const data = rng.bytes(length);

          await cache.write(path, data, 0, length, position, fileSize);
          model.write(path, data, 0, length, position);
          activeFiles.add(path);
          break;
        }

        case "read": {
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
          const cacheRead = await cache.read(path, READ_BUF_1, 0, length, position, fileSize);
          const modelRead = model.read(path, READ_BUF_2, 0, length, position);

          expect(cacheRead).toBe(modelRead);
          expect(READ_BUF_1.subarray(0, length)).toEqual(READ_BUF_2.subarray(0, length));
          break;
        }

        case "readMulti": {
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
          const cacheRead = await cache.read(path, READ_BUF_1, 0, length, position, fileSize);
          const modelRead = model.read(path, READ_BUF_2, 0, length, position);

          expect(cacheRead).toBe(modelRead);
          expect(READ_BUF_1.subarray(0, length)).toEqual(READ_BUF_2.subarray(0, length));
          break;
        }

        case "getPage": {
          if (activeFiles.size === 0) break;
          const path = rng.pick([...activeFiles]);
          const fileSize = model.fileSizes.get(path) ?? 0;
          if (fileSize === 0) break;
          const pageCount = Math.ceil(fileSize / PAGE_SIZE);
          const pageIndex = rng.int(pageCount);

          const cachedPage = await cache.getPage(path, pageIndex);
          const expected = model.getPage(path, pageIndex);
          expect(cachedPage.data).toEqual(expected);
          break;
        }

        case "flushFile": {
          if (activeFiles.size === 0) break;
          const path = rng.pick([...activeFiles]);
          await cache.flushFile(path);
          break;
        }

        case "flushAll": {
          await cache.flushAll();
          break;
        }

        case "collectCommit": {
          const dirty = cache.collectDirtyPages();
          if (dirty.length > 0) {
            expect(cache.dirtyCount).toBeGreaterThanOrEqual(dirty.length);
          }
          await backend.writePages(dirty);
          cache.commitDirtyPages(dirty);
          expect(cache.dirtyCount).toBe(0);
          break;
        }

        case "deleteFile": {
          if (activeFiles.size === 0) break;
          const path = rng.pick([...activeFiles]);
          await cache.deleteFile(path);
          model.deleteFile(path);
          activeFiles.delete(path);
          break;
        }

        case "renameFile": {
          if (activeFiles.size === 0) break;
          const oldPath = rng.pick([...activeFiles]);
          const newPath = rng.pick(FILE_POOL);
          if (oldPath === newPath) break;

          await cache.renameFile(oldPath, newPath);
          model.renameFile(oldPath, newPath);
          activeFiles.delete(oldPath);
          activeFiles.add(newPath);
          break;
        }

        case "truncate": {
          if (activeFiles.size === 0) break;
          const path = rng.pick([...activeFiles]);
          const fileSize = model.fileSizes.get(path) ?? 0;
          const newSize = rng.int(2) === 0
            ? rng.int(Math.max(fileSize, 1))
            : Math.min(fileSize + rng.int(PAGE_SIZE), PAGE_SIZE * 4);
          if (newSize === fileSize) break;

          if (newSize < fileSize) {
            await cache.zeroTailAfterTruncate(path, newSize);
            const neededPages = newSize > 0 ? Math.ceil(newSize / PAGE_SIZE) : 0;
            cache.invalidatePagesFrom(path, neededPages);
            await backend.deletePagesFrom(path, neededPages);
          } else {
            const lastPageIdx = Math.ceil(newSize / PAGE_SIZE) - 1;
            const firstNewPage = fileSize > 0 ? Math.ceil(fileSize / PAGE_SIZE) : 0;
            if (lastPageIdx >= firstNewPage) {
              await cache.markPageDirty(path, lastPageIdx);
            }
          }
          model.truncate(path, newSize);
          break;
        }

        case "evictFile": {
          if (activeFiles.size === 0) break;
          const path = rng.pick([...activeFiles]);
          await cache.evictFile(path);
          const fileSize = model.fileSizes.get(path) ?? 0;
          if (fileSize > 0) {
            const readLen = Math.min(fileSize, PAGE_SIZE);
            const cacheBuf = new Uint8Array(readLen);
            const modelBuf = new Uint8Array(readLen);
            await cache.read(path, cacheBuf, 0, readLen, 0, fileSize);
            model.read(path, modelBuf, 0, readLen, 0);
            expect(cacheBuf).toEqual(modelBuf);
          }
          break;
        }

        case "markPageDirty": {
          const path = rng.pick(FILE_POOL);
          const pageIndex = rng.int(4);
          await cache.markPageDirty(path, pageIndex);
          if (!model.fileSizes.has(path)) {
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

  await cache.flushAll();
  for (const path of activeFiles) {
    await verifyBackendFile(backend, model, path);
  }
}

async function verifyBackendFile(
  backend: MemoryBackend,
  model: ReferenceModel,
  path: string,
): Promise<void> {
  const fileSize = model.fileSizes.get(path) ?? 0;
  const pageCount = fileSize > 0 ? Math.ceil(fileSize / PAGE_SIZE) : 0;

  for (let i = 0; i < pageCount; i++) {
    const backendPage = await backend.readPage(path, i);
    const modelPage = model.getPage(path, i);
    if (backendPage) {
      expect(backendPage).toEqual(modelPage);
    } else {
      expect(modelPage).toEqual(new Uint8Array(PAGE_SIZE));
    }
  }
}

// ---------------------------------------------------------------
// Test cases
// ---------------------------------------------------------------

describe("PageCache (async) differential fuzz", () => {
  describe("large cache (64 pages)", () => {
    const MAX_PAGES = 64;
    const OPS = 200;

    for (let seed = 1; seed <= 5; seed++) {
      it(`seed ${seed} (${OPS} ops, ${MAX_PAGES}-page cache) @fast`, async () => {
        await runFuzzSession(seed, MAX_PAGES, OPS);
      });
    }
  });

  describe("medium cache (8 pages)", () => {
    const MAX_PAGES = 8;
    const OPS = 200;

    for (let seed = 100; seed <= 110; seed++) {
      const tag = seed <= 102 ? " @fast" : "";
      it(`seed ${seed} (${OPS} ops, ${MAX_PAGES}-page cache)${tag}`, async () => {
        await runFuzzSession(seed, MAX_PAGES, OPS);
      });
    }
  });

  describe("tiny cache (3 pages)", () => {
    const MAX_PAGES = 3;
    const OPS = 150;

    for (let seed = 200; seed <= 208; seed++) {
      it(`seed ${seed} (${OPS} ops, ${MAX_PAGES}-page cache)`, async () => {
        await runFuzzSession(seed, MAX_PAGES, OPS);
      });
    }
  });

  describe("extended sessions (16-page cache, 500 ops)", () => {
    const MAX_PAGES = 16;
    const OPS = 500;

    for (let seed = 300; seed <= 304; seed++) {
      it(`seed ${seed} (${OPS} ops, ${MAX_PAGES}-page cache)`, async () => {
        await runFuzzSession(seed, MAX_PAGES, OPS);
      });
    }
  });

  describe("minimal cache (2 pages)", () => {
    const MAX_PAGES = 2;
    const OPS = 100;

    for (let seed = 400; seed <= 404; seed++) {
      it(`seed ${seed} (${OPS} ops, ${MAX_PAGES}-page cache)`, async () => {
        await runFuzzSession(seed, MAX_PAGES, OPS);
      });
    }
  });
});

// ---------------------------------------------------------------
// Parity tests: async PageCache vs sync SyncPageCache
//
// Run the same PRNG-generated operations through both implementations
// and verify identical backend state. Catches any behavioral drift
// between the async and sync variants.
// ---------------------------------------------------------------

type ParityOpType =
  | "write"
  | "writeFull"
  | "writeMulti"
  | "flushFile"
  | "flushAll"
  | "deleteFile"
  | "renameFile"
  | "truncate"
  | "evictFile"
  | "markPageDirty";

const PARITY_OP_WEIGHTS: Array<[ParityOpType, number]> = [
  ["write", 25],
  ["writeFull", 10],
  ["writeMulti", 10],
  ["flushFile", 5],
  ["flushAll", 3],
  ["deleteFile", 4],
  ["renameFile", 4],
  ["truncate", 5],
  ["evictFile", 2],
  ["markPageDirty", 3],
];

function pickParityOp(rng: Rng): ParityOpType {
  const total = PARITY_OP_WEIGHTS.reduce((s, [, w]) => s + w, 0);
  let r = rng.int(total);
  for (const [op, weight] of PARITY_OP_WEIGHTS) {
    r -= weight;
    if (r < 0) return op;
  }
  return "write";
}

async function runParitySession(
  seed: number,
  maxPages: number,
  opCount: number,
): Promise<void> {
  const asyncBackend = new MemoryBackend();
  const syncBackend = new SyncMemoryBackend();
  const asyncCache = new PageCache(asyncBackend, maxPages);
  const syncCache = new SyncPageCache(syncBackend, maxPages);

  const fileSizes = new Map<string, number>();
  const activeFiles = new Set<string>();

  // Two independent RNGs with the same seed — one for each cache.
  // Both caches see the exact same sequence of operations.
  const rng = new Rng(seed);

  for (let step = 0; step < opCount; step++) {
    const op = pickParityOp(rng);

    try {
      switch (op) {
        case "write": {
          const path = rng.pick(FILE_POOL);
          const fileSize = fileSizes.get(path) ?? 0;
          const maxPos = Math.min(Math.max(fileSize, PAGE_SIZE * 2), PAGE_SIZE * 4);
          const position = rng.int(maxPos);
          const length = rng.int(PAGE_SIZE / 4) + 1;
          const data = rng.bytes(length);

          const asyncResult = await asyncCache.write(path, data, 0, length, position, fileSize);
          const syncResult = syncCache.write(path, data, 0, length, position, fileSize);
          expect(asyncResult).toEqual(syncResult);
          fileSizes.set(path, asyncResult.newFileSize);
          activeFiles.add(path);
          break;
        }

        case "writeFull": {
          const path = rng.pick(FILE_POOL);
          const fileSize = fileSizes.get(path) ?? 0;
          const pageIndex = rng.int(4);
          const position = pageIndex * PAGE_SIZE;
          const data = rng.bytes(PAGE_SIZE);

          const asyncResult = await asyncCache.write(path, data, 0, PAGE_SIZE, position, fileSize);
          const syncResult = syncCache.write(path, data, 0, PAGE_SIZE, position, fileSize);
          expect(asyncResult).toEqual(syncResult);
          fileSizes.set(path, asyncResult.newFileSize);
          activeFiles.add(path);
          break;
        }

        case "writeMulti": {
          const path = rng.pick(FILE_POOL);
          const fileSize = fileSizes.get(path) ?? 0;
          const startPage = rng.int(3);
          const pageOffset = rng.int(PAGE_SIZE);
          const position = startPage * PAGE_SIZE + pageOffset;
          const crossLen = PAGE_SIZE - pageOffset + 1 + rng.int(PAGE_SIZE);
          const length = Math.min(crossLen, PAGE_SIZE * 2);
          const data = rng.bytes(length);

          const asyncResult = await asyncCache.write(path, data, 0, length, position, fileSize);
          const syncResult = syncCache.write(path, data, 0, length, position, fileSize);
          expect(asyncResult).toEqual(syncResult);
          fileSizes.set(path, asyncResult.newFileSize);
          activeFiles.add(path);
          break;
        }

        case "flushFile": {
          if (activeFiles.size === 0) break;
          const path = rng.pick([...activeFiles]);
          await asyncCache.flushFile(path);
          syncCache.flushFile(path);
          break;
        }

        case "flushAll": {
          await asyncCache.flushAll();
          syncCache.flushAll();
          break;
        }

        case "deleteFile": {
          if (activeFiles.size === 0) break;
          const path = rng.pick([...activeFiles]);
          await asyncCache.deleteFile(path);
          syncCache.deleteFile(path);
          fileSizes.delete(path);
          activeFiles.delete(path);
          break;
        }

        case "renameFile": {
          if (activeFiles.size === 0) break;
          const oldPath = rng.pick([...activeFiles]);
          const newPath = rng.pick(FILE_POOL);
          if (oldPath === newPath) break;

          await asyncCache.renameFile(oldPath, newPath);
          syncCache.renameFile(oldPath, newPath);
          const size = fileSizes.get(oldPath) ?? 0;
          fileSizes.delete(oldPath);
          fileSizes.set(newPath, size);
          activeFiles.delete(oldPath);
          activeFiles.add(newPath);
          break;
        }

        case "truncate": {
          if (activeFiles.size === 0) break;
          const path = rng.pick([...activeFiles]);
          const fileSize = fileSizes.get(path) ?? 0;
          const newSize = rng.int(2) === 0
            ? rng.int(Math.max(fileSize, 1))
            : Math.min(fileSize + rng.int(PAGE_SIZE), PAGE_SIZE * 4);
          if (newSize === fileSize) break;

          if (newSize < fileSize) {
            await asyncCache.zeroTailAfterTruncate(path, newSize);
            syncCache.zeroTailAfterTruncate(path, newSize);
            const neededPages = newSize > 0 ? Math.ceil(newSize / PAGE_SIZE) : 0;
            asyncCache.invalidatePagesFrom(path, neededPages);
            syncCache.invalidatePagesFrom(path, neededPages);
            await asyncBackend.deletePagesFrom(path, neededPages);
            syncBackend.deletePagesFrom(path, neededPages);
          } else {
            const lastPageIdx = Math.ceil(newSize / PAGE_SIZE) - 1;
            const firstNewPage = fileSize > 0 ? Math.ceil(fileSize / PAGE_SIZE) : 0;
            if (lastPageIdx >= firstNewPage) {
              await asyncCache.markPageDirty(path, lastPageIdx);
              syncCache.markPageDirty(path, lastPageIdx);
            }
          }
          fileSizes.set(path, newSize);
          break;
        }

        case "evictFile": {
          if (activeFiles.size === 0) break;
          const path = rng.pick([...activeFiles]);
          await asyncCache.evictFile(path);
          syncCache.evictFile(path);
          break;
        }

        case "markPageDirty": {
          const path = rng.pick(FILE_POOL);
          const pageIndex = rng.int(4);
          await asyncCache.markPageDirty(path, pageIndex);
          syncCache.markPageDirty(path, pageIndex);
          if (!fileSizes.has(path)) {
            fileSizes.set(path, 0);
          }
          activeFiles.add(path);
          break;
        }
      }
    } catch (e) {
      throw new Error(
        `Parity seed ${seed}, step ${step}, op ${op} failed: ${(e as Error).message}`,
      );
    }
  }

  // Flush both caches and compare backend state
  await asyncCache.flushAll();
  syncCache.flushAll();

  for (const path of activeFiles) {
    const fileSize = fileSizes.get(path) ?? 0;
    const pageCount = fileSize > 0 ? Math.ceil(fileSize / PAGE_SIZE) : 0;

    for (let i = 0; i < pageCount; i++) {
      const asyncPage = await asyncBackend.readPage(path, i);
      const syncPage = syncBackend.readPage(path, i);

      if (asyncPage === null && syncPage === null) continue;
      expect(asyncPage).not.toBeNull();
      expect(syncPage).not.toBeNull();
      expect(
        asyncPage!,
        `Backend parity mismatch at ${path}:${i} (seed ${seed})`,
      ).toEqual(syncPage!);
    }
  }
}

describe("PageCache/SyncPageCache parity fuzz", () => {
  describe("large cache (64 pages)", () => {
    for (let seed = 1; seed <= 3; seed++) {
      it(`seed ${seed} (200 ops, 64-page cache) @fast`, async () => {
        await runParitySession(seed, 64, 200);
      });
    }
  });

  describe("medium cache (8 pages)", () => {
    for (let seed = 100; seed <= 105; seed++) {
      const tag = seed <= 101 ? " @fast" : "";
      it(`seed ${seed} (200 ops, 8-page cache)${tag}`, async () => {
        await runParitySession(seed, 8, 200);
      });
    }
  });

  describe("tiny cache (3 pages)", () => {
    for (let seed = 200; seed <= 205; seed++) {
      it(`seed ${seed} (150 ops, 3-page cache)`, async () => {
        await runParitySession(seed, 3, 150);
      });
    }
  });

  describe("extended (16 pages, 500 ops)", () => {
    for (let seed = 300; seed <= 302; seed++) {
      it(`seed ${seed} (500 ops, 16-page cache)`, async () => {
        await runParitySession(seed, 16, 500);
      });
    }
  });
});
