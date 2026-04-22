/**
 * Differential fuzz test: IdbBackend vs MemoryBackend.
 *
 * Generates random sequences of StorageBackend operations using a seeded
 * PRNG, executes them against both IdbBackend (via fake-indexeddb) and
 * MemoryBackend, and verifies identical observable state after each operation.
 *
 * The backend-contract tests (tests/unit/backend-contract.test.ts) verify
 * individual operations in isolation. This test targets *interaction* bugs:
 * sequences of operations that individually work but produce divergent state
 * when combined (e.g., rename + deletePagesFrom + writePage at the same path,
 * or syncAll interleaved with individual writes).
 *
 * IdbBackend is the production backend — it uses compound keys [path, pageIndex],
 * IDB key ranges for deletions, and cursor iteration for renames. These
 * implementation details create surface area for subtle behavioral differences
 * vs the simpler Map-based MemoryBackend.
 *
 * Ethos §9: "Write tests designed to break tomefs specifically"
 * Ethos §5: "we never use mocks — we use fakes"
 */

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import "fake-indexeddb/auto";
import { IdbBackend } from "../../src/idb-backend.js";
import { MemoryBackend } from "../../src/memory-backend.js";
import { PAGE_SIZE } from "../../src/types.js";
import type { StorageBackend } from "../../src/storage-backend.js";
import type { FileMeta } from "../../src/types.js";

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
    for (let i = 0; i < length; i++) {
      buf[i] = this.next() & 0xff;
    }
    return buf;
  }
}

// ---------------------------------------------------------------
// Operation types
// ---------------------------------------------------------------

type Op =
  | { type: "writePage"; path: string; pageIndex: number; data: Uint8Array }
  | { type: "writePages"; pages: Array<{ path: string; pageIndex: number; data: Uint8Array }> }
  | { type: "deletePagesFrom"; path: string; fromPageIndex: number }
  | { type: "deleteFile"; path: string }
  | { type: "deleteFiles"; paths: string[] }
  | { type: "renameFile"; oldPath: string; newPath: string }
  | { type: "writeMeta"; path: string; meta: FileMeta }
  | { type: "writeMetas"; entries: Array<{ path: string; meta: FileMeta }> }
  | { type: "deleteMeta"; path: string }
  | { type: "deleteMetas"; paths: string[] }
  | { type: "syncAll"; pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>; metas: Array<{ path: string; meta: FileMeta }> }
  | { type: "deleteAll"; paths: string[] };

const FILE_PATHS = ["/a", "/b", "/c", "/d", "/sub/x", "/sub/y"];
const MAX_PAGE_INDEX = 5;

function randomMeta(rng: Rng): FileMeta {
  return {
    size: rng.int(PAGE_SIZE * 4),
    mode: 0o100644,
    ctime: 1000 + rng.int(10000),
    mtime: 2000 + rng.int(10000),
    atime: 3000 + rng.int(10000),
  };
}

// ---------------------------------------------------------------
// Operation generator
// ---------------------------------------------------------------

function generateOps(rng: Rng, count: number): Op[] {
  const ops: Op[] = [];

  // Track which paths have pages/meta so we can generate meaningful operations
  const knownPaths = new Set<string>();

  for (let i = 0; i < count; i++) {
    const weights: [string, number][] = [
      ["writePage", 20],
      ["writePages", 8],
      ["deletePagesFrom", knownPaths.size > 0 ? 6 : 0],
      ["deleteFile", knownPaths.size > 0 ? 5 : 0],
      ["deleteFiles", knownPaths.size > 1 ? 3 : 0],
      ["renameFile", knownPaths.size > 0 ? 8 : 0],
      ["writeMeta", 15],
      ["writeMetas", 6],
      ["deleteMeta", knownPaths.size > 0 ? 5 : 0],
      ["deleteMetas", knownPaths.size > 1 ? 3 : 0],
      ["syncAll", knownPaths.size > 0 ? 6 : 2],
      ["deleteAll", knownPaths.size > 0 ? 4 : 0],
    ];

    const totalWeight = weights.reduce((s, [, w]) => s + w, 0);
    let r = rng.int(totalWeight);
    let opType = weights[0][0];
    for (const [t, w] of weights) {
      r -= w;
      if (r < 0) {
        opType = t;
        break;
      }
    }

    switch (opType) {
      case "writePage": {
        const path = rng.pick(FILE_PATHS);
        const pageIndex = rng.int(MAX_PAGE_INDEX);
        const data = rng.bytes(PAGE_SIZE);
        knownPaths.add(path);
        ops.push({ type: "writePage", path, pageIndex, data });
        break;
      }

      case "writePages": {
        const pageCount = 1 + rng.int(4);
        const pages: Array<{ path: string; pageIndex: number; data: Uint8Array }> = [];
        for (let p = 0; p < pageCount; p++) {
          const path = rng.pick(FILE_PATHS);
          knownPaths.add(path);
          pages.push({ path, pageIndex: rng.int(MAX_PAGE_INDEX), data: rng.bytes(PAGE_SIZE) });
        }
        ops.push({ type: "writePages", pages });
        break;
      }

      case "deletePagesFrom": {
        const path = rng.pick([...knownPaths]);
        const fromPageIndex = rng.int(MAX_PAGE_INDEX + 1);
        ops.push({ type: "deletePagesFrom", path, fromPageIndex });
        break;
      }

      case "deleteFile": {
        const path = rng.pick([...knownPaths]);
        ops.push({ type: "deleteFile", path });
        break;
      }

      case "deleteFiles": {
        const paths = [...knownPaths].slice(0, 1 + rng.int(Math.min(3, knownPaths.size)));
        ops.push({ type: "deleteFiles", paths });
        break;
      }

      case "renameFile": {
        const oldPath = rng.pick([...knownPaths]);
        const newPath = rng.pick(FILE_PATHS);
        knownPaths.add(newPath);
        ops.push({ type: "renameFile", oldPath, newPath });
        break;
      }

      case "writeMeta": {
        const path = rng.pick(FILE_PATHS);
        knownPaths.add(path);
        ops.push({ type: "writeMeta", path, meta: randomMeta(rng) });
        break;
      }

      case "writeMetas": {
        const entryCount = 1 + rng.int(4);
        const entries: Array<{ path: string; meta: FileMeta }> = [];
        for (let e = 0; e < entryCount; e++) {
          const path = rng.pick(FILE_PATHS);
          knownPaths.add(path);
          entries.push({ path, meta: randomMeta(rng) });
        }
        ops.push({ type: "writeMetas", entries });
        break;
      }

      case "deleteMeta": {
        const path = rng.pick([...knownPaths]);
        ops.push({ type: "deleteMeta", path });
        break;
      }

      case "deleteMetas": {
        const paths = [...knownPaths].slice(0, 1 + rng.int(Math.min(3, knownPaths.size)));
        ops.push({ type: "deleteMetas", paths });
        break;
      }

      case "syncAll": {
        const pageCount = rng.int(4);
        const metaCount = rng.int(4);
        const pages: Array<{ path: string; pageIndex: number; data: Uint8Array }> = [];
        for (let p = 0; p < pageCount; p++) {
          const path = rng.pick(FILE_PATHS);
          knownPaths.add(path);
          pages.push({ path, pageIndex: rng.int(MAX_PAGE_INDEX), data: rng.bytes(PAGE_SIZE) });
        }
        const metas: Array<{ path: string; meta: FileMeta }> = [];
        for (let m = 0; m < metaCount; m++) {
          const path = rng.pick(FILE_PATHS);
          knownPaths.add(path);
          metas.push({ path, meta: randomMeta(rng) });
        }
        ops.push({ type: "syncAll", pages, metas });
        break;
      }

      case "deleteAll": {
        const paths = [...knownPaths].slice(0, 1 + rng.int(Math.min(4, knownPaths.size)));
        ops.push({ type: "deleteAll", paths });
        break;
      }
    }
  }

  return ops;
}

// ---------------------------------------------------------------
// Operation executor
// ---------------------------------------------------------------

async function executeOp(backend: StorageBackend, op: Op): Promise<void> {
  switch (op.type) {
    case "writePage":
      await backend.writePage(op.path, op.pageIndex, op.data);
      break;
    case "writePages":
      await backend.writePages(op.pages);
      break;
    case "deletePagesFrom":
      await backend.deletePagesFrom(op.path, op.fromPageIndex);
      break;
    case "deleteFile":
      await backend.deleteFile(op.path);
      break;
    case "deleteFiles":
      await backend.deleteFiles(op.paths);
      break;
    case "renameFile":
      await backend.renameFile(op.oldPath, op.newPath);
      break;
    case "writeMeta":
      await backend.writeMeta(op.path, op.meta);
      break;
    case "writeMetas":
      await backend.writeMetas(op.entries);
      break;
    case "deleteMeta":
      await backend.deleteMeta(op.path);
      break;
    case "deleteMetas":
      await backend.deleteMetas(op.paths);
      break;
    case "syncAll":
      await backend.syncAll(op.pages, op.metas);
      break;
    case "deleteAll":
      await backend.deleteAll(op.paths);
      break;
  }
}

// ---------------------------------------------------------------
// State comparison
// ---------------------------------------------------------------

async function compareState(
  idb: StorageBackend,
  mem: StorageBackend,
  context: string,
): Promise<void> {
  // Compare listFiles
  const idbFiles = (await idb.listFiles()).sort();
  const memFiles = (await mem.listFiles()).sort();
  expect(idbFiles, `${context}: listFiles`).toEqual(memFiles);

  // Compare metadata for all known files
  const allPaths = [...new Set([...idbFiles, ...memFiles])];
  for (const path of allPaths) {
    const idbMeta = await idb.readMeta(path);
    const memMeta = await mem.readMeta(path);
    expect(idbMeta, `${context}: readMeta(${path})`).toEqual(memMeta);
  }

  // Compare page data for all known files
  for (const path of allPaths) {
    const idbMax = await idb.maxPageIndex(path);
    const memMax = await mem.maxPageIndex(path);
    expect(idbMax, `${context}: maxPageIndex(${path})`).toBe(memMax);

    const idbCount = await idb.countPages(path);
    const memCount = await mem.countPages(path);
    expect(idbCount, `${context}: countPages(${path})`).toBe(memCount);

    // Read all pages up to max index
    const maxIdx = Math.max(idbMax, memMax);
    if (maxIdx >= 0) {
      const indices = Array.from({ length: maxIdx + 1 }, (_, i) => i);
      const idbPages = await idb.readPages(path, indices);
      const memPages = await mem.readPages(path, indices);
      for (let i = 0; i <= maxIdx; i++) {
        if (memPages[i] === null) {
          expect(idbPages[i], `${context}: readPage(${path}, ${i}) should be null`).toBeNull();
        } else {
          expect(
            idbPages[i],
            `${context}: readPage(${path}, ${i}) should not be null`,
          ).not.toBeNull();
          expect(
            idbPages[i],
            `${context}: readPage(${path}, ${i}) data mismatch`,
          ).toEqual(memPages[i]);
        }
      }
    }
  }

  // Compare batch operations with all paths
  if (allPaths.length > 0) {
    const idbCounts = await idb.countPagesBatch(allPaths);
    const memCounts = await mem.countPagesBatch(allPaths);
    expect(idbCounts, `${context}: countPagesBatch`).toEqual(memCounts);

    const idbMaxIndices = await idb.maxPageIndexBatch(allPaths);
    const memMaxIndices = await mem.maxPageIndexBatch(allPaths);
    expect(idbMaxIndices, `${context}: maxPageIndexBatch`).toEqual(memMaxIndices);

    const idbMetas = await idb.readMetas(allPaths);
    const memMetas = await mem.readMetas(allPaths);
    expect(idbMetas, `${context}: readMetas`).toEqual(memMetas);
  }
}

// ---------------------------------------------------------------
// Format operation for error reporting
// ---------------------------------------------------------------

function formatOp(op: Op, index: number): string {
  switch (op.type) {
    case "writePage":
      return `[${index}] writePage(${op.path}, ${op.pageIndex})`;
    case "writePages":
      return `[${index}] writePages(${op.pages.map((p) => `${p.path}:${p.pageIndex}`).join(", ")})`;
    case "deletePagesFrom":
      return `[${index}] deletePagesFrom(${op.path}, ${op.fromPageIndex})`;
    case "deleteFile":
      return `[${index}] deleteFile(${op.path})`;
    case "deleteFiles":
      return `[${index}] deleteFiles(${op.paths.join(", ")})`;
    case "renameFile":
      return `[${index}] renameFile(${op.oldPath} -> ${op.newPath})`;
    case "writeMeta":
      return `[${index}] writeMeta(${op.path})`;
    case "writeMetas":
      return `[${index}] writeMetas(${op.entries.map((e) => e.path).join(", ")})`;
    case "deleteMeta":
      return `[${index}] deleteMeta(${op.path})`;
    case "deleteMetas":
      return `[${index}] deleteMetas(${op.paths.join(", ")})`;
    case "syncAll":
      return `[${index}] syncAll(${op.pages.length} pages, ${op.metas.length} metas)`;
    case "deleteAll":
      return `[${index}] deleteAll(${op.paths.join(", ")})`;
  }
}

// ---------------------------------------------------------------
// Test runner
// ---------------------------------------------------------------

async function runFuzzSeed(
  seed: number,
  opCount: number,
  verifyEvery: number,
): Promise<void> {
  const rng = new Rng(seed);
  const ops = generateOps(rng, opCount);

  let dbCounter = 0;
  const idb = new IdbBackend({ dbName: `fuzz_idb_${seed}_${dbCounter++}` });
  const mem = new MemoryBackend();

  try {
    for (let i = 0; i < ops.length; i++) {
      const op = ops[i];
      const context = `seed=${seed} after ${formatOp(op, i)}`;

      await executeOp(idb, op);
      await executeOp(mem, op);

      // Verify state periodically (not every op, for performance)
      if ((i + 1) % verifyEvery === 0 || i === ops.length - 1) {
        await compareState(idb, mem, context);
      }
    }
  } finally {
    idb.close();
    await idb.destroy();
  }
}

// ---------------------------------------------------------------
// Test suites
// ---------------------------------------------------------------

describe("IDB backend differential fuzz", () => {
  // Short sequences — verify after every operation
  describe("short sequences (verify every op) @fast", () => {
    for (const seed of [1, 2, 3, 42, 100]) {
      it(`seed ${seed}: 20 ops`, async () => {
        await runFuzzSeed(seed, 20, 1);
      });
    }
  });

  // Medium sequences — verify every 5 ops
  describe("medium sequences (50 ops)", () => {
    for (const seed of [200, 201, 202, 303, 404, 505, 606, 777, 888, 999]) {
      it(`seed ${seed}`, async () => {
        await runFuzzSeed(seed, 50, 5);
      });
    }
  });

  // Longer sequences — verify every 10 ops
  describe("long sequences (150 ops)", () => {
    for (const seed of [1000, 1001, 1002, 2000, 3000]) {
      it(`seed ${seed}`, async () => {
        await runFuzzSeed(seed, 150, 10);
      });
    }
  });

  // Rename-heavy: higher chance of rename operations exercising IDB cursor logic
  describe("rename-heavy sequences", () => {
    for (const seed of [5000, 5001, 5002, 5003, 5004]) {
      it(`seed ${seed}: 80 ops`, async () => {
        await runRenameHeavySeed(seed, 80);
      });
    }
  });

  // syncAll-heavy: exercises atomic multi-store transactions
  describe("syncAll-heavy sequences", () => {
    for (const seed of [6000, 6001, 6002, 6003, 6004]) {
      it(`seed ${seed}: 60 ops`, async () => {
        await runSyncAllHeavySeed(seed, 60);
      });
    }
  });
});

// ---------------------------------------------------------------
// Variant generators for specific operation mixes
// ---------------------------------------------------------------

async function runRenameHeavySeed(seed: number, opCount: number): Promise<void> {
  const rng = new Rng(seed);
  const ops: Op[] = [];
  const knownPaths = new Set<string>();

  for (let i = 0; i < opCount; i++) {
    const weights: [string, number][] = [
      ["writePage", 10],
      ["writeMeta", 8],
      ["renameFile", knownPaths.size > 0 ? 20 : 0],
      ["deleteFile", knownPaths.size > 0 ? 5 : 0],
      ["deletePagesFrom", knownPaths.size > 0 ? 5 : 0],
      ["deleteMeta", knownPaths.size > 0 ? 3 : 0],
    ];

    const totalWeight = weights.reduce((s, [, w]) => s + w, 0);
    let r = rng.int(totalWeight);
    let opType = weights[0][0];
    for (const [t, w] of weights) {
      r -= w;
      if (r < 0) {
        opType = t;
        break;
      }
    }

    switch (opType) {
      case "writePage": {
        const path = rng.pick(FILE_PATHS);
        knownPaths.add(path);
        ops.push({
          type: "writePage",
          path,
          pageIndex: rng.int(MAX_PAGE_INDEX),
          data: rng.bytes(PAGE_SIZE),
        });
        break;
      }
      case "writeMeta": {
        const path = rng.pick(FILE_PATHS);
        knownPaths.add(path);
        ops.push({ type: "writeMeta", path, meta: randomMeta(rng) });
        break;
      }
      case "renameFile": {
        const oldPath = rng.pick([...knownPaths]);
        const newPath = rng.pick(FILE_PATHS);
        knownPaths.add(newPath);
        ops.push({ type: "renameFile", oldPath, newPath });
        break;
      }
      case "deleteFile": {
        const path = rng.pick([...knownPaths]);
        ops.push({ type: "deleteFile", path });
        break;
      }
      case "deletePagesFrom": {
        const path = rng.pick([...knownPaths]);
        ops.push({ type: "deletePagesFrom", path, fromPageIndex: rng.int(MAX_PAGE_INDEX + 1) });
        break;
      }
      case "deleteMeta": {
        const path = rng.pick([...knownPaths]);
        ops.push({ type: "deleteMeta", path });
        break;
      }
    }
  }

  let dbCounter = 0;
  const idb = new IdbBackend({ dbName: `fuzz_rename_${seed}_${dbCounter++}` });
  const mem = new MemoryBackend();

  try {
    for (let i = 0; i < ops.length; i++) {
      await executeOp(idb, ops[i]);
      await executeOp(mem, ops[i]);

      if ((i + 1) % 5 === 0 || i === ops.length - 1) {
        await compareState(idb, mem, `rename-heavy seed=${seed} after op ${i}`);
      }
    }
  } finally {
    idb.close();
    await idb.destroy();
  }
}

async function runSyncAllHeavySeed(seed: number, opCount: number): Promise<void> {
  const rng = new Rng(seed);
  const ops: Op[] = [];
  const knownPaths = new Set<string>();

  for (let i = 0; i < opCount; i++) {
    const weights: [string, number][] = [
      ["writePage", 8],
      ["writeMeta", 6],
      ["syncAll", 20],
      ["deleteFile", knownPaths.size > 0 ? 4 : 0],
      ["renameFile", knownPaths.size > 0 ? 5 : 0],
      ["deleteMeta", knownPaths.size > 0 ? 3 : 0],
      ["deletePagesFrom", knownPaths.size > 0 ? 3 : 0],
    ];

    const totalWeight = weights.reduce((s, [, w]) => s + w, 0);
    let r = rng.int(totalWeight);
    let opType = weights[0][0];
    for (const [t, w] of weights) {
      r -= w;
      if (r < 0) {
        opType = t;
        break;
      }
    }

    switch (opType) {
      case "writePage": {
        const path = rng.pick(FILE_PATHS);
        knownPaths.add(path);
        ops.push({
          type: "writePage",
          path,
          pageIndex: rng.int(MAX_PAGE_INDEX),
          data: rng.bytes(PAGE_SIZE),
        });
        break;
      }
      case "writeMeta": {
        const path = rng.pick(FILE_PATHS);
        knownPaths.add(path);
        ops.push({ type: "writeMeta", path, meta: randomMeta(rng) });
        break;
      }
      case "syncAll": {
        const pageCount = rng.int(5);
        const metaCount = rng.int(5);
        const pages: Array<{ path: string; pageIndex: number; data: Uint8Array }> = [];
        for (let p = 0; p < pageCount; p++) {
          const path = rng.pick(FILE_PATHS);
          knownPaths.add(path);
          pages.push({ path, pageIndex: rng.int(MAX_PAGE_INDEX), data: rng.bytes(PAGE_SIZE) });
        }
        const metas: Array<{ path: string; meta: FileMeta }> = [];
        for (let m = 0; m < metaCount; m++) {
          const path = rng.pick(FILE_PATHS);
          knownPaths.add(path);
          metas.push({ path, meta: randomMeta(rng) });
        }
        ops.push({ type: "syncAll", pages, metas });
        break;
      }
      case "deleteFile": {
        const path = rng.pick([...knownPaths]);
        ops.push({ type: "deleteFile", path });
        break;
      }
      case "renameFile": {
        const oldPath = rng.pick([...knownPaths]);
        const newPath = rng.pick(FILE_PATHS);
        knownPaths.add(newPath);
        ops.push({ type: "renameFile", oldPath, newPath });
        break;
      }
      case "deleteMeta": {
        const path = rng.pick([...knownPaths]);
        ops.push({ type: "deleteMeta", path });
        break;
      }
      case "deletePagesFrom": {
        const path = rng.pick([...knownPaths]);
        ops.push({ type: "deletePagesFrom", path, fromPageIndex: rng.int(MAX_PAGE_INDEX + 1) });
        break;
      }
    }
  }

  let dbCounter = 0;
  const idb = new IdbBackend({ dbName: `fuzz_syncall_${seed}_${dbCounter++}` });
  const mem = new MemoryBackend();

  try {
    for (let i = 0; i < ops.length; i++) {
      await executeOp(idb, ops[i]);
      await executeOp(mem, ops[i]);

      if ((i + 1) % 5 === 0 || i === ops.length - 1) {
        await compareState(idb, mem, `syncall-heavy seed=${seed} after op ${i}`);
      }
    }
  } finally {
    idb.close();
    await idb.destroy();
  }
}
