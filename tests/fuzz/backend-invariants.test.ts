/**
 * Differential fuzz tests for backend secondary index invariants.
 *
 * SyncMemoryBackend and PreloadBackend maintain 5+ concurrent data
 * structures (pages Map, meta Map, filePageKeys index, filePageIndices
 * index, fileMaxIdx cache; plus dirty tracking in PreloadBackend). These
 * tests generate random sequences of backend operations and call
 * assertInvariants() after each operation to catch index corruption.
 *
 * Additionally verifies observable behavior against a naive reference
 * model (simple Map without secondary indexes) to ensure the optimized
 * backends produce identical results.
 *
 * Uses a seeded PRNG for reproducibility — failing seeds can be replayed.
 *
 * Ethos §9: "Write tests designed to break tomefs specifically —
 * target the seams"
 */

import { describe, it, expect } from "vitest";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { PreloadBackend } from "../../src/preload-backend.js";
import { MemoryBackend } from "../../src/memory-backend.js";
import { PAGE_SIZE, pageKeyStr } from "../../src/types.js";
import type { FileMeta } from "../../src/types.js";

// ---------------------------------------------------------------
// Seeded PRNG (xorshift128+)
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

  int(min: number, max: number): number {
    return min + (this.next() % (max - min + 1));
  }

  pick<T>(arr: T[]): T {
    return arr[this.next() % arr.length];
  }

  bytes(len: number): Uint8Array {
    const buf = new Uint8Array(len);
    for (let i = 0; i < len; i++) {
      buf[i] = this.next() & 0xff;
    }
    return buf;
  }
}

// ---------------------------------------------------------------
// Naive reference model (no secondary indexes)
// ---------------------------------------------------------------

class ReferenceBackend {
  pages = new Map<string, Uint8Array>();
  meta = new Map<string, FileMeta>();

  writePage(path: string, pageIndex: number, data: Uint8Array): void {
    this.pages.set(pageKeyStr(path, pageIndex), new Uint8Array(data));
  }

  writePages(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
  ): void {
    for (const { path, pageIndex, data } of pages) {
      this.writePage(path, pageIndex, data);
    }
  }

  readPage(path: string, pageIndex: number): Uint8Array | null {
    const data = this.pages.get(pageKeyStr(path, pageIndex));
    return data ? new Uint8Array(data) : null;
  }

  deleteFile(path: string): void {
    const prefix = path + "\0";
    for (const key of this.pages.keys()) {
      if (key.startsWith(prefix)) {
        this.pages.delete(key);
      }
    }
  }

  deletePagesFrom(path: string, fromPageIndex: number): void {
    const prefix = path + "\0";
    for (const [key] of this.pages) {
      if (key.startsWith(prefix)) {
        const idx = parseInt(key.substring(prefix.length), 10);
        if (idx >= fromPageIndex) {
          this.pages.delete(key);
        }
      }
    }
  }

  renameFile(oldPath: string, newPath: string): void {
    if (oldPath === newPath) return;
    this.deleteFile(newPath);
    const oldPrefix = oldPath + "\0";
    const toAdd: Array<[string, Uint8Array]> = [];
    for (const [key, data] of this.pages) {
      if (key.startsWith(oldPrefix)) {
        const idx = parseInt(key.substring(oldPrefix.length), 10);
        toAdd.push([pageKeyStr(newPath, idx), data]);
        this.pages.delete(key);
      }
    }
    for (const [key, data] of toAdd) {
      this.pages.set(key, data);
    }
  }

  countPages(path: string): number {
    const prefix = path + "\0";
    let count = 0;
    for (const key of this.pages.keys()) {
      if (key.startsWith(prefix)) count++;
    }
    return count;
  }

  maxPageIndex(path: string): number {
    const prefix = path + "\0";
    let max = -1;
    for (const key of this.pages.keys()) {
      if (key.startsWith(prefix)) {
        const idx = parseInt(key.substring(prefix.length), 10);
        if (idx > max) max = idx;
      }
    }
    return max;
  }

  writeMeta(path: string, meta: FileMeta): void {
    this.meta.set(path, { ...meta });
  }

  writeMetas(entries: Array<{ path: string; meta: FileMeta }>): void {
    for (const { path, meta } of entries) {
      this.writeMeta(path, meta);
    }
  }

  readMeta(path: string): FileMeta | null {
    return this.meta.get(path) ?? null;
  }

  deleteMeta(path: string): void {
    this.meta.delete(path);
  }

  deleteMetas(paths: string[]): void {
    for (const path of paths) {
      this.deleteMeta(path);
    }
  }

  deleteFiles(paths: string[]): void {
    for (const path of paths) {
      this.deleteFile(path);
    }
  }

  cleanupOrphanedPages(): number {
    const metaPaths = new Set(this.meta.keys());
    const pagePaths = new Set<string>();
    for (const key of this.pages.keys()) {
      const nullIdx = key.indexOf("\0");
      pagePaths.add(key.substring(0, nullIdx));
    }
    let removed = 0;
    for (const path of pagePaths) {
      if (!metaPaths.has(path)) {
        this.deleteFile(path);
        removed++;
      }
    }
    return removed;
  }

  listFiles(): string[] {
    return [...this.meta.keys()];
  }
}

// ---------------------------------------------------------------
// Operation types
// ---------------------------------------------------------------

type Op =
  | { type: "writePage"; path: string; pageIndex: number; data: Uint8Array }
  | { type: "writePages"; pages: Array<{ path: string; pageIndex: number; data: Uint8Array }> }
  | { type: "deleteFile"; path: string }
  | { type: "deleteFiles"; paths: string[] }
  | { type: "deletePagesFrom"; path: string; fromPageIndex: number }
  | { type: "renameFile"; oldPath: string; newPath: string }
  | { type: "writeMeta"; path: string; meta: FileMeta }
  | { type: "writeMetas"; metas: Array<{ path: string; meta: FileMeta }> }
  | { type: "deleteMeta"; path: string }
  | { type: "deleteMetas"; paths: string[] }
  | { type: "syncAll"; pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>; metas: Array<{ path: string; meta: FileMeta }> }
  | { type: "deleteAll"; paths: string[] }
  | { type: "cleanupOrphanedPages" };

const FILE_PATHS = ["/a", "/b", "/c", "/d", "/e"];

function generateOp(rng: Rng): Op {
  const opType = rng.int(0, 12);
  switch (opType) {
    case 0: {
      return {
        type: "writePage",
        path: rng.pick(FILE_PATHS),
        pageIndex: rng.int(0, 7),
        data: rng.bytes(PAGE_SIZE),
      };
    }
    case 1: {
      const count = rng.int(1, 4);
      const pages: Array<{ path: string; pageIndex: number; data: Uint8Array }> = [];
      for (let i = 0; i < count; i++) {
        pages.push({
          path: rng.pick(FILE_PATHS),
          pageIndex: rng.int(0, 7),
          data: rng.bytes(PAGE_SIZE),
        });
      }
      return { type: "writePages", pages };
    }
    case 2: {
      return { type: "deleteFile", path: rng.pick(FILE_PATHS) };
    }
    case 3: {
      return {
        type: "deletePagesFrom",
        path: rng.pick(FILE_PATHS),
        fromPageIndex: rng.int(0, 5),
      };
    }
    case 4: {
      return {
        type: "renameFile",
        oldPath: rng.pick(FILE_PATHS),
        newPath: rng.pick(FILE_PATHS),
      };
    }
    case 5: {
      return {
        type: "writeMeta",
        path: rng.pick(FILE_PATHS),
        meta: { size: rng.int(0, PAGE_SIZE * 8), mode: 0o100644, ctime: Date.now(), mtime: Date.now() },
      };
    }
    case 6: {
      return { type: "deleteMeta", path: rng.pick(FILE_PATHS) };
    }
    case 7: {
      const count = rng.int(1, 3);
      const pages: Array<{ path: string; pageIndex: number; data: Uint8Array }> = [];
      const metas: Array<{ path: string; meta: FileMeta }> = [];
      for (let i = 0; i < count; i++) {
        pages.push({
          path: rng.pick(FILE_PATHS),
          pageIndex: rng.int(0, 7),
          data: rng.bytes(PAGE_SIZE),
        });
      }
      for (let i = 0; i < rng.int(0, 2); i++) {
        metas.push({
          path: rng.pick(FILE_PATHS),
          meta: { size: rng.int(0, PAGE_SIZE * 8), mode: 0o100644, ctime: Date.now(), mtime: Date.now() },
        });
      }
      return { type: "syncAll", pages, metas };
    }
    case 8: {
      const count = rng.int(1, 3);
      const paths = new Set<string>();
      for (let i = 0; i < count; i++) {
        paths.add(rng.pick(FILE_PATHS));
      }
      return { type: "deleteAll", paths: [...paths] };
    }
    case 9: {
      const count = rng.int(1, 3);
      const paths = new Set<string>();
      for (let i = 0; i < count; i++) {
        paths.add(rng.pick(FILE_PATHS));
      }
      return { type: "deleteFiles", paths: [...paths] };
    }
    case 10: {
      const count = rng.int(1, 3);
      const metas: Array<{ path: string; meta: FileMeta }> = [];
      for (let i = 0; i < count; i++) {
        metas.push({
          path: rng.pick(FILE_PATHS),
          meta: { size: rng.int(0, PAGE_SIZE * 8), mode: 0o100644, ctime: Date.now(), mtime: Date.now() },
        });
      }
      return { type: "writeMetas", metas };
    }
    case 11: {
      const count = rng.int(1, 3);
      const paths = new Set<string>();
      for (let i = 0; i < count; i++) {
        paths.add(rng.pick(FILE_PATHS));
      }
      return { type: "deleteMetas", paths: [...paths] };
    }
    case 12: {
      return { type: "cleanupOrphanedPages" };
    }
    default:
      throw new Error(`unreachable: opType=${opType}`);
  }
}

// ---------------------------------------------------------------
// Apply operation to both SyncMemoryBackend and reference
// ---------------------------------------------------------------

function applyOp(backend: SyncMemoryBackend, ref: ReferenceBackend, op: Op): void {
  switch (op.type) {
    case "writePage":
      backend.writePage(op.path, op.pageIndex, op.data);
      ref.writePage(op.path, op.pageIndex, op.data);
      break;
    case "writePages":
      backend.writePages(op.pages);
      ref.writePages(op.pages);
      break;
    case "deleteFile":
      backend.deleteFile(op.path);
      ref.deleteFile(op.path);
      break;
    case "deleteFiles":
      backend.deleteFiles(op.paths);
      ref.deleteFiles(op.paths);
      break;
    case "deletePagesFrom":
      backend.deletePagesFrom(op.path, op.fromPageIndex);
      ref.deletePagesFrom(op.path, op.fromPageIndex);
      break;
    case "renameFile":
      backend.renameFile(op.oldPath, op.newPath);
      ref.renameFile(op.oldPath, op.newPath);
      break;
    case "writeMeta":
      backend.writeMeta(op.path, op.meta);
      ref.writeMeta(op.path, op.meta);
      break;
    case "writeMetas":
      backend.writeMetas(op.metas);
      ref.writeMetas(op.metas);
      break;
    case "deleteMeta":
      backend.deleteMeta(op.path);
      ref.deleteMeta(op.path);
      break;
    case "deleteMetas":
      backend.deleteMetas(op.paths);
      ref.deleteMetas(op.paths);
      break;
    case "syncAll":
      backend.syncAll(op.pages, op.metas);
      ref.writePages(op.pages);
      for (const { path, meta } of op.metas) {
        ref.writeMeta(path, meta);
      }
      break;
    case "deleteAll":
      backend.deleteAll(op.paths);
      for (const path of op.paths) {
        ref.deleteFile(path);
        ref.deleteMeta(path);
      }
      break;
    case "cleanupOrphanedPages": {
      const backendRemoved = backend.cleanupOrphanedPages();
      const refRemoved = ref.cleanupOrphanedPages();
      expect(backendRemoved, "cleanupOrphanedPages count mismatch").toBe(refRemoved);
      break;
    }
  }
}

function verifyAgainstReference(
  backend: SyncMemoryBackend,
  ref: ReferenceBackend,
  context: string,
): void {
  for (const path of FILE_PATHS) {
    const bCount = backend.countPages(path);
    const rCount = ref.countPages(path);
    expect(bCount, `${context}: countPages(${path})`).toBe(rCount);

    const bMax = backend.maxPageIndex(path);
    const rMax = ref.maxPageIndex(path);
    expect(bMax, `${context}: maxPageIndex(${path})`).toBe(rMax);

    for (let i = 0; i <= Math.max(bMax, rMax, 7); i++) {
      const bPage = backend.readPage(path, i);
      const rPage = ref.readPage(path, i);
      if (rPage === null) {
        expect(bPage, `${context}: readPage(${path}, ${i})`).toBeNull();
      } else {
        expect(bPage, `${context}: readPage(${path}, ${i}) missing`).not.toBeNull();
        expect(
          Buffer.from(bPage!).equals(Buffer.from(rPage)),
          `${context}: readPage(${path}, ${i}) data mismatch`,
        ).toBe(true);
      }
    }

    const bMeta = backend.readMeta(path);
    const rMeta = ref.readMeta(path);
    if (rMeta === null) {
      expect(bMeta, `${context}: readMeta(${path})`).toBeNull();
    } else {
      expect(bMeta, `${context}: readMeta(${path}) missing`).not.toBeNull();
      expect(bMeta, `${context}: readMeta(${path}) data`).toEqual(rMeta);
    }
  }

  const bFiles = backend.listFiles().sort();
  const rFiles = ref.listFiles().sort();
  expect(bFiles, `${context}: listFiles`).toEqual(rFiles);

  const bCounts = backend.countPagesBatch(FILE_PATHS);
  const rCounts = FILE_PATHS.map((p) => ref.countPages(p));
  expect(bCounts, `${context}: countPagesBatch`).toEqual(rCounts);

  const bMaxBatch = backend.maxPageIndexBatch(FILE_PATHS);
  const rMaxBatch = FILE_PATHS.map((p) => ref.maxPageIndex(p));
  expect(bMaxBatch, `${context}: maxPageIndexBatch`).toEqual(rMaxBatch);

  const bMetas = backend.readMetas(FILE_PATHS);
  const rMetas = FILE_PATHS.map((p) => ref.readMeta(p));
  expect(bMetas, `${context}: readMetas`).toEqual(rMetas);
}

// ---------------------------------------------------------------
// SyncMemoryBackend fuzz
// ---------------------------------------------------------------

function runSyncMemoryFuzz(seed: number, numOps: number): void {
  const rng = new Rng(seed);
  const backend = new SyncMemoryBackend();
  const ref = new ReferenceBackend();

  for (let i = 0; i < numOps; i++) {
    const op = generateOp(rng);
    applyOp(backend, ref, op);
    backend.assertInvariants();
  }

  verifyAgainstReference(backend, ref, `seed ${seed} final`);
}

describe("fuzz: SyncMemoryBackend index invariants", () => {
  describe("100-op sequences", () => {
    for (let seed = 5001; seed <= 5020; seed++) {
      it(`seed ${seed} @fast`, () => {
        runSyncMemoryFuzz(seed, 100);
      });
    }
  });

  describe("500-op sequences (heavy)", () => {
    for (let seed = 6001; seed <= 6010; seed++) {
      it(`seed ${seed}`, () => {
        runSyncMemoryFuzz(seed, 500);
      });
    }
  });
});

// ---------------------------------------------------------------
// PreloadBackend fuzz
// ---------------------------------------------------------------

function applyOpToPreload(
  backend: PreloadBackend,
  ref: ReferenceBackend,
  op: Op,
): void {
  switch (op.type) {
    case "writePage":
      backend.writePage(op.path, op.pageIndex, op.data);
      ref.writePage(op.path, op.pageIndex, op.data);
      break;
    case "writePages":
      backend.writePages(op.pages);
      ref.writePages(op.pages);
      break;
    case "deleteFile":
      backend.deleteFile(op.path);
      ref.deleteFile(op.path);
      break;
    case "deleteFiles":
      backend.deleteFiles(op.paths);
      ref.deleteFiles(op.paths);
      break;
    case "deletePagesFrom":
      backend.deletePagesFrom(op.path, op.fromPageIndex);
      ref.deletePagesFrom(op.path, op.fromPageIndex);
      break;
    case "renameFile":
      backend.renameFile(op.oldPath, op.newPath);
      ref.renameFile(op.oldPath, op.newPath);
      break;
    case "writeMeta":
      backend.writeMeta(op.path, op.meta);
      ref.writeMeta(op.path, op.meta);
      break;
    case "writeMetas":
      backend.writeMetas(op.metas);
      ref.writeMetas(op.metas);
      break;
    case "deleteMeta":
      backend.deleteMeta(op.path);
      ref.deleteMeta(op.path);
      break;
    case "deleteMetas":
      backend.deleteMetas(op.paths);
      ref.deleteMetas(op.paths);
      break;
    case "syncAll":
      backend.syncAll(op.pages, op.metas);
      ref.writePages(op.pages);
      for (const { path, meta } of op.metas) {
        ref.writeMeta(path, meta);
      }
      break;
    case "deleteAll":
      backend.deleteAll(op.paths);
      for (const path of op.paths) {
        ref.deleteFile(path);
        ref.deleteMeta(path);
      }
      break;
    case "cleanupOrphanedPages": {
      const backendRemoved = backend.cleanupOrphanedPages();
      const refRemoved = ref.cleanupOrphanedPages();
      expect(backendRemoved, "cleanupOrphanedPages count mismatch").toBe(refRemoved);
      break;
    }
  }
}

function verifyPreloadAgainstReference(
  backend: PreloadBackend,
  ref: ReferenceBackend,
  context: string,
): void {
  for (const path of FILE_PATHS) {
    const bCount = backend.countPages(path);
    const rCount = ref.countPages(path);
    expect(bCount, `${context}: countPages(${path})`).toBe(rCount);

    const bMax = backend.maxPageIndex(path);
    const rMax = ref.maxPageIndex(path);
    expect(bMax, `${context}: maxPageIndex(${path})`).toBe(rMax);

    for (let i = 0; i <= Math.max(bMax, rMax, 7); i++) {
      const bPage = backend.readPage(path, i);
      const rPage = ref.readPage(path, i);
      if (rPage === null) {
        expect(bPage, `${context}: readPage(${path}, ${i})`).toBeNull();
      } else {
        expect(bPage, `${context}: readPage(${path}, ${i}) missing`).not.toBeNull();
        expect(
          Buffer.from(bPage!).equals(Buffer.from(rPage)),
          `${context}: readPage(${path}, ${i}) data mismatch`,
        ).toBe(true);
      }
    }

    const bMeta = backend.readMeta(path);
    const rMeta = ref.readMeta(path);
    if (rMeta === null) {
      expect(bMeta, `${context}: readMeta(${path})`).toBeNull();
    } else {
      expect(bMeta, `${context}: readMeta(${path}) missing`).not.toBeNull();
      expect(bMeta, `${context}: readMeta(${path}) data`).toEqual(rMeta);
    }
  }

  const bFiles = backend.listFiles().sort();
  const rFiles = ref.listFiles().sort();
  expect(bFiles, `${context}: listFiles`).toEqual(rFiles);

  const bCounts = backend.countPagesBatch(FILE_PATHS);
  const rCounts = FILE_PATHS.map((p) => ref.countPages(p));
  expect(bCounts, `${context}: countPagesBatch`).toEqual(rCounts);

  const bMaxBatch = backend.maxPageIndexBatch(FILE_PATHS);
  const rMaxBatch = FILE_PATHS.map((p) => ref.maxPageIndex(p));
  expect(bMaxBatch, `${context}: maxPageIndexBatch`).toEqual(rMaxBatch);

  const bMetas = backend.readMetas(FILE_PATHS);
  const rMetas = FILE_PATHS.map((p) => ref.readMeta(p));
  expect(bMetas, `${context}: readMetas`).toEqual(rMetas);
}

async function runPreloadFuzz(seed: number, numOps: number): Promise<void> {
  const rng = new Rng(seed);
  const remote = new MemoryBackend();
  const backend = new PreloadBackend(remote);
  await backend.init();
  const ref = new ReferenceBackend();

  for (let i = 0; i < numOps; i++) {
    const op = generateOp(rng);
    applyOpToPreload(backend, ref, op);
    backend.assertInvariants();
  }

  verifyPreloadAgainstReference(backend, ref, `seed ${seed} final`);
}

describe("fuzz: PreloadBackend index invariants", () => {
  describe("100-op sequences", () => {
    for (let seed = 7001; seed <= 7020; seed++) {
      it(`seed ${seed} @fast`, async () => {
        await runPreloadFuzz(seed, 100);
      });
    }
  });

  describe("500-op sequences (heavy)", () => {
    for (let seed = 8001; seed <= 8010; seed++) {
      it(`seed ${seed}`, async () => {
        await runPreloadFuzz(seed, 500);
      });
    }
  });
});

// ---------------------------------------------------------------
// PreloadBackend flush + invariant verification
// ---------------------------------------------------------------

async function runPreloadFlushFuzz(seed: number, numOps: number): Promise<void> {
  const rng = new Rng(seed);
  const remote = new MemoryBackend();
  const backend = new PreloadBackend(remote);
  await backend.init();
  const ref = new ReferenceBackend();

  for (let i = 0; i < numOps; i++) {
    const op = generateOp(rng);
    applyOpToPreload(backend, ref, op);
    backend.assertInvariants();

    // Periodically flush and re-verify invariants
    if (i > 0 && i % 15 === 0) {
      await backend.flush();
      backend.assertInvariants();
    }
  }

  // Final flush
  await backend.flush();
  backend.assertInvariants();
  verifyPreloadAgainstReference(backend, ref, `seed ${seed} post-flush`);

  // Verify round-trip: re-init from remote and check invariants
  const backend2 = new PreloadBackend(remote);
  await backend2.init();
  backend2.assertInvariants();
}

describe("fuzz: PreloadBackend flush + invariants", () => {
  describe("100-op sequences with periodic flush", () => {
    for (let seed = 9001; seed <= 9010; seed++) {
      it(`seed ${seed} @fast`, async () => {
        await runPreloadFlushFuzz(seed, 100);
      });
    }
  });

  describe("300-op sequences with periodic flush (heavy)", () => {
    for (let seed = 9501; seed <= 9505; seed++) {
      it(`seed ${seed}`, async () => {
        await runPreloadFlushFuzz(seed, 300);
      });
    }
  });
});

// ---------------------------------------------------------------
// Targeted regression tests: specific operation patterns that
// stress secondary index maintenance
// ---------------------------------------------------------------

describe("targeted: index-stressing operation patterns", () => {
  it("write-then-delete-then-rewrite same path @fast", () => {
    const backend = new SyncMemoryBackend();
    const data = new Uint8Array(PAGE_SIZE).fill(0xab);

    backend.writePage("/x", 0, data);
    backend.writePage("/x", 3, data);
    backend.assertInvariants();
    expect(backend.maxPageIndex("/x")).toBe(3);
    expect(backend.countPages("/x")).toBe(2);

    backend.deleteFile("/x");
    backend.assertInvariants();
    expect(backend.maxPageIndex("/x")).toBe(-1);
    expect(backend.countPages("/x")).toBe(0);

    backend.writePage("/x", 1, data);
    backend.assertInvariants();
    expect(backend.maxPageIndex("/x")).toBe(1);
    expect(backend.countPages("/x")).toBe(1);
  });

  it("rename to self is a no-op @fast", () => {
    const backend = new SyncMemoryBackend();
    const data = new Uint8Array(PAGE_SIZE).fill(0xcd);

    backend.writePage("/a", 0, data);
    backend.writePage("/a", 5, data);
    backend.assertInvariants();

    backend.renameFile("/a", "/a");
    backend.assertInvariants();
    expect(backend.countPages("/a")).toBe(2);
    expect(backend.maxPageIndex("/a")).toBe(5);
  });

  it("rename overwrites destination pages @fast", () => {
    const backend = new SyncMemoryBackend();
    const data1 = new Uint8Array(PAGE_SIZE).fill(0x11);
    const data2 = new Uint8Array(PAGE_SIZE).fill(0x22);

    backend.writePage("/src", 0, data1);
    backend.writePage("/dst", 0, data2);
    backend.writePage("/dst", 1, data2);
    backend.writePage("/dst", 2, data2);
    backend.assertInvariants();

    backend.renameFile("/src", "/dst");
    backend.assertInvariants();
    expect(backend.countPages("/src")).toBe(0);
    expect(backend.countPages("/dst")).toBe(1);
    expect(backend.maxPageIndex("/dst")).toBe(0);
    const page = backend.readPage("/dst", 0)!;
    expect(page[0]).toBe(0x11);
  });

  it("deletePagesFrom updates maxPageIndex @fast", () => {
    const backend = new SyncMemoryBackend();
    const data = new Uint8Array(PAGE_SIZE).fill(0xee);

    for (let i = 0; i < 5; i++) {
      backend.writePage("/f", i, data);
    }
    backend.assertInvariants();
    expect(backend.maxPageIndex("/f")).toBe(4);

    backend.deletePagesFrom("/f", 3);
    backend.assertInvariants();
    expect(backend.maxPageIndex("/f")).toBe(2);
    expect(backend.countPages("/f")).toBe(3);

    backend.deletePagesFrom("/f", 0);
    backend.assertInvariants();
    expect(backend.maxPageIndex("/f")).toBe(-1);
    expect(backend.countPages("/f")).toBe(0);
  });

  it("writePages batch with interleaved paths @fast", () => {
    const backend = new SyncMemoryBackend();
    const data = new Uint8Array(PAGE_SIZE).fill(0xff);

    // Interleaved paths stress the batch optimization in writePages
    // which groups by file path
    backend.writePages([
      { path: "/a", pageIndex: 0, data },
      { path: "/b", pageIndex: 1, data },
      { path: "/a", pageIndex: 2, data },
      { path: "/c", pageIndex: 0, data },
      { path: "/b", pageIndex: 0, data },
      { path: "/a", pageIndex: 1, data },
    ]);
    backend.assertInvariants();

    expect(backend.countPages("/a")).toBe(3);
    expect(backend.countPages("/b")).toBe(2);
    expect(backend.countPages("/c")).toBe(1);
    expect(backend.maxPageIndex("/a")).toBe(2);
    expect(backend.maxPageIndex("/b")).toBe(1);
    expect(backend.maxPageIndex("/c")).toBe(0);
  });

  it("syncAll combines pages and metas correctly @fast", () => {
    const backend = new SyncMemoryBackend();
    const data = new Uint8Array(PAGE_SIZE).fill(0xaa);

    backend.syncAll(
      [
        { path: "/x", pageIndex: 0, data },
        { path: "/y", pageIndex: 2, data },
      ],
      [
        { path: "/x", meta: { size: PAGE_SIZE, mode: 0o100644, ctime: 1, mtime: 1 } },
        { path: "/z", meta: { size: 0, mode: 0o100644, ctime: 1, mtime: 1 } },
      ],
    );
    backend.assertInvariants();
    expect(backend.countPages("/x")).toBe(1);
    expect(backend.countPages("/y")).toBe(1);
    expect(backend.readMeta("/z")).not.toBeNull();
  });

  it("deleteAll cleans up both pages and meta @fast", () => {
    const backend = new SyncMemoryBackend();
    const data = new Uint8Array(PAGE_SIZE).fill(0xbb);

    backend.writePage("/a", 0, data);
    backend.writePage("/a", 1, data);
    backend.writeMeta("/a", { size: PAGE_SIZE * 2, mode: 0o100644, ctime: 1, mtime: 1 });
    backend.writePage("/b", 0, data);
    backend.writeMeta("/b", { size: PAGE_SIZE, mode: 0o100644, ctime: 1, mtime: 1 });
    backend.assertInvariants();

    backend.deleteAll(["/a"]);
    backend.assertInvariants();
    expect(backend.countPages("/a")).toBe(0);
    expect(backend.readMeta("/a")).toBeNull();
    expect(backend.countPages("/b")).toBe(1);
    expect(backend.readMeta("/b")).not.toBeNull();
  });

  it("PreloadBackend rename + flush preserves invariants @fast", async () => {
    const remote = new MemoryBackend();
    const backend = new PreloadBackend(remote);
    await backend.init();

    const data = new Uint8Array(PAGE_SIZE).fill(0xdd);
    backend.writePage("/src", 0, data);
    backend.writePage("/src", 1, data);
    backend.writeMeta("/src", { size: PAGE_SIZE * 2, mode: 0o100644, ctime: 1, mtime: 1 });
    backend.assertInvariants();

    backend.renameFile("/src", "/dst");
    backend.assertInvariants();

    await backend.flush();
    backend.assertInvariants();

    expect(backend.countPages("/src")).toBe(0);
    expect(backend.countPages("/dst")).toBe(2);
  });

  it("PreloadBackend delete + recreate at same path @fast", async () => {
    const remote = new MemoryBackend();
    const backend = new PreloadBackend(remote);
    await backend.init();

    const data1 = new Uint8Array(PAGE_SIZE).fill(0x11);
    const data2 = new Uint8Array(PAGE_SIZE).fill(0x22);

    backend.writePage("/f", 0, data1);
    backend.writeMeta("/f", { size: PAGE_SIZE, mode: 0o100644, ctime: 1, mtime: 1 });
    await backend.flush();
    backend.assertInvariants();

    backend.deleteFile("/f");
    backend.deleteMeta("/f");
    backend.assertInvariants();

    backend.writePage("/f", 0, data2);
    backend.writeMeta("/f", { size: PAGE_SIZE, mode: 0o100644, ctime: 2, mtime: 2 });
    backend.assertInvariants();

    await backend.flush();
    backend.assertInvariants();

    const page = backend.readPage("/f", 0)!;
    expect(page[0]).toBe(0x22);
  });

  it("PreloadBackend truncation + extend @fast", async () => {
    const remote = new MemoryBackend();
    const backend = new PreloadBackend(remote);
    await backend.init();

    const data = new Uint8Array(PAGE_SIZE).fill(0xcc);
    for (let i = 0; i < 5; i++) {
      backend.writePage("/f", i, data);
    }
    backend.assertInvariants();

    backend.deletePagesFrom("/f", 2);
    backend.assertInvariants();
    expect(backend.maxPageIndex("/f")).toBe(1);

    backend.writePage("/f", 4, data);
    backend.assertInvariants();
    expect(backend.maxPageIndex("/f")).toBe(4);
    expect(backend.countPages("/f")).toBe(3);

    await backend.flush();
    backend.assertInvariants();
  });

  it("deleteFiles batch cleans up indexes for all paths @fast", () => {
    const backend = new SyncMemoryBackend();
    const data = new Uint8Array(PAGE_SIZE).fill(0xaa);

    backend.writePage("/a", 0, data);
    backend.writePage("/a", 1, data);
    backend.writePage("/b", 0, data);
    backend.writePage("/c", 0, data);
    backend.writePage("/c", 3, data);
    backend.assertInvariants();

    backend.deleteFiles(["/a", "/c"]);
    backend.assertInvariants();
    expect(backend.countPages("/a")).toBe(0);
    expect(backend.maxPageIndex("/a")).toBe(-1);
    expect(backend.countPages("/b")).toBe(1);
    expect(backend.countPages("/c")).toBe(0);
    expect(backend.maxPageIndex("/c")).toBe(-1);
  });

  it("writeMetas batch stores all entries @fast", () => {
    const backend = new SyncMemoryBackend();
    const m1 = { size: 100, mode: 0o100644, ctime: 1, mtime: 1 };
    const m2 = { size: 200, mode: 0o100755, ctime: 2, mtime: 2 };

    backend.writeMetas([
      { path: "/a", meta: m1 },
      { path: "/b", meta: m2 },
    ]);
    backend.assertInvariants();
    expect(backend.readMeta("/a")).toEqual(m1);
    expect(backend.readMeta("/b")).toEqual(m2);
    expect(backend.listFiles().sort()).toEqual(["/a", "/b"]);
  });

  it("deleteMetas batch removes all entries @fast", () => {
    const backend = new SyncMemoryBackend();
    const meta = { size: 100, mode: 0o100644, ctime: 1, mtime: 1 };

    backend.writeMeta("/a", meta);
    backend.writeMeta("/b", meta);
    backend.writeMeta("/c", meta);
    backend.assertInvariants();

    backend.deleteMetas(["/a", "/c"]);
    backend.assertInvariants();
    expect(backend.readMeta("/a")).toBeNull();
    expect(backend.readMeta("/b")).toEqual(meta);
    expect(backend.readMeta("/c")).toBeNull();
    expect(backend.listFiles()).toEqual(["/b"]);
  });

  it("cleanupOrphanedPages removes pages without metadata @fast", () => {
    const backend = new SyncMemoryBackend();
    const data = new Uint8Array(PAGE_SIZE).fill(0xdd);
    const meta = { size: PAGE_SIZE, mode: 0o100644, ctime: 1, mtime: 1 };

    backend.writePage("/orphan1", 0, data);
    backend.writePage("/orphan1", 1, data);
    backend.writePage("/orphan2", 0, data);
    backend.writePage("/valid", 0, data);
    backend.writeMeta("/valid", meta);
    backend.assertInvariants();

    const removed = backend.cleanupOrphanedPages();
    backend.assertInvariants();
    expect(removed).toBe(2);
    expect(backend.readPage("/orphan1", 0)).toBeNull();
    expect(backend.readPage("/orphan2", 0)).toBeNull();
    expect(backend.readPage("/valid", 0)).toEqual(data);
    expect(backend.countPages("/valid")).toBe(1);
  });

  it("cleanupOrphanedPages returns 0 when no orphans @fast", () => {
    const backend = new SyncMemoryBackend();
    const data = new Uint8Array(PAGE_SIZE).fill(0xee);
    const meta = { size: PAGE_SIZE, mode: 0o100644, ctime: 1, mtime: 1 };

    backend.writePage("/f", 0, data);
    backend.writeMeta("/f", meta);
    backend.assertInvariants();

    expect(backend.cleanupOrphanedPages()).toBe(0);
    backend.assertInvariants();
  });

  it("PreloadBackend deleteFiles + writeMetas batch @fast", async () => {
    const remote = new MemoryBackend();
    const backend = new PreloadBackend(remote);
    await backend.init();

    const data = new Uint8Array(PAGE_SIZE).fill(0x11);
    backend.writePage("/a", 0, data);
    backend.writePage("/b", 0, data);
    backend.writePage("/c", 0, data);
    backend.assertInvariants();

    backend.deleteFiles(["/a", "/b"]);
    backend.assertInvariants();
    expect(backend.countPages("/a")).toBe(0);
    expect(backend.countPages("/b")).toBe(0);
    expect(backend.countPages("/c")).toBe(1);

    const meta = { size: PAGE_SIZE, mode: 0o100644, ctime: 1, mtime: 1 };
    backend.writeMetas([
      { path: "/c", meta },
      { path: "/d", meta: { ...meta, size: 0 } },
    ]);
    backend.assertInvariants();
    expect(backend.readMeta("/c")).toEqual(meta);
    expect(backend.readMeta("/d")).toEqual({ ...meta, size: 0 });

    await backend.flush();
    backend.assertInvariants();
  });

  it("PreloadBackend cleanupOrphanedPages + flush @fast", async () => {
    const remote = new MemoryBackend();
    const backend = new PreloadBackend(remote);
    await backend.init();

    const data = new Uint8Array(PAGE_SIZE).fill(0x22);
    const meta = { size: PAGE_SIZE, mode: 0o100644, ctime: 1, mtime: 1 };

    backend.writePage("/orphan", 0, data);
    backend.writePage("/valid", 0, data);
    backend.writeMeta("/valid", meta);
    backend.assertInvariants();

    const removed = backend.cleanupOrphanedPages();
    backend.assertInvariants();
    expect(removed).toBe(1);
    expect(backend.countPages("/orphan")).toBe(0);
    expect(backend.countPages("/valid")).toBe(1);

    await backend.flush();
    backend.assertInvariants();
  });

  it("deleteMetas + writeMetas interleave on same paths @fast", () => {
    const backend = new SyncMemoryBackend();
    const m1 = { size: 100, mode: 0o100644, ctime: 1, mtime: 1 };
    const m2 = { size: 200, mode: 0o100755, ctime: 2, mtime: 2 };

    backend.writeMetas([{ path: "/a", meta: m1 }, { path: "/b", meta: m1 }]);
    backend.assertInvariants();

    backend.deleteMetas(["/a"]);
    backend.assertInvariants();

    backend.writeMetas([{ path: "/a", meta: m2 }]);
    backend.assertInvariants();
    expect(backend.readMeta("/a")).toEqual(m2);
    expect(backend.readMeta("/b")).toEqual(m1);
  });
});
