/**
 * Differential fuzz tests for PreloadBackend flush roundtrip.
 *
 * Generates random sequences of SyncStorageBackend operations against a
 * PreloadBackend wrapping a MemoryBackend. After each batch of operations,
 * calls flush() and verifies that the underlying MemoryBackend contains
 * exactly the expected state by creating a fresh PreloadBackend, init()'ing
 * it from the same MemoryBackend, and comparing all observable state.
 *
 * This tests two critical invariants:
 *   1. flush() correctly propagates ALL local changes to the remote backend
 *      (dirty pages, metadata, deletions, truncations, renames)
 *   2. init() correctly reloads ALL state from the remote backend
 *
 * The flush() method in PreloadBackend is the most complex code path in the
 * graceful degradation scenario — 5 phases with early/late partitioning for
 * crash safety. This fuzz test exercises the full matrix of interactions
 * between page writes, metadata writes, file deletions, truncations, and
 * renames that flush must handle correctly.
 *
 * Uses a seeded PRNG for reproducibility — failing seeds can be replayed.
 *
 * Ethos §9: "Write tests designed to break tomefs specifically"
 * Ethos §10: "Graceful degradation without SharedArrayBuffer"
 */

import { describe, it, expect } from "vitest";
import { MemoryBackend } from "../../src/memory-backend.js";
import { PreloadBackend } from "../../src/preload-backend.js";
import { PAGE_SIZE, pageKeyStr } from "../../src/types.js";
import type { FileMeta } from "../../src/types.js";

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
// Reference model: tracks expected state for verification
// ---------------------------------------------------------------

/**
 * Tracks expected page data and metadata for comparison with PreloadBackend.
 * All operations are trivial Map manipulations — no dirty tracking, no
 * partitioning — so correctness is self-evident.
 *
 * Uses collect-then-delete pattern for all mutations to avoid any
 * Map iteration-while-deleting edge cases.
 */
class ReferenceModel {
  pages = new Map<string, Uint8Array>();
  meta = new Map<string, FileMeta>();

  writePage(path: string, pageIndex: number, data: Uint8Array): void {
    this.pages.set(pageKeyStr(path, pageIndex), new Uint8Array(data));
  }

  readPage(path: string, pageIndex: number): Uint8Array | null {
    const data = this.pages.get(pageKeyStr(path, pageIndex));
    return data ? new Uint8Array(data) : null;
  }

  deleteFile(path: string): void {
    const prefix = path + "\0";
    const toDelete: string[] = [];
    for (const key of this.pages.keys()) {
      if (key.startsWith(prefix)) {
        toDelete.push(key);
      }
    }
    for (const key of toDelete) {
      this.pages.delete(key);
    }
  }

  deletePagesFrom(path: string, fromPageIndex: number): void {
    const prefix = path + "\0";
    const toDelete: string[] = [];
    for (const key of this.pages.keys()) {
      if (key.startsWith(prefix)) {
        const idx = parseInt(key.substring(key.indexOf("\0") + 1), 10);
        if (idx >= fromPageIndex) {
          toDelete.push(key);
        }
      }
    }
    for (const key of toDelete) {
      this.pages.delete(key);
    }
  }

  renameFile(oldPath: string, newPath: string): void {
    if (oldPath === newPath) return;
    // Delete destination first
    this.deleteFile(newPath);
    // Collect source pages, then delete, then add at new path
    const prefix = oldPath + "\0";
    const toMove: Array<[string, Uint8Array]> = [];
    const toDelete: string[] = [];
    for (const [key, data] of this.pages) {
      if (key.startsWith(prefix)) {
        const suffix = key.substring(oldPath.length);
        toMove.push([newPath + suffix, new Uint8Array(data)]);
        toDelete.push(key);
      }
    }
    for (const key of toDelete) {
      this.pages.delete(key);
    }
    for (const [key, data] of toMove) {
      this.pages.set(key, data);
    }
  }

  writeMeta(path: string, meta: FileMeta): void {
    this.meta.set(path, { ...meta });
  }

  deleteMeta(path: string): void {
    this.meta.delete(path);
  }

  deleteAll(paths: string[]): void {
    for (const path of paths) {
      this.deleteFile(path);
      this.deleteMeta(path);
    }
  }

  listFiles(): string[] {
    return [...this.meta.keys()].sort();
  }

  /** Get all page indices for a file, sorted. */
  pageIndicesForFile(path: string): number[] {
    const prefix = path + "\0";
    const indices: number[] = [];
    for (const key of this.pages.keys()) {
      if (key.startsWith(prefix)) {
        indices.push(parseInt(key.substring(key.indexOf("\0") + 1), 10));
      }
    }
    return indices.sort((a, b) => a - b);
  }
}

// ---------------------------------------------------------------
// Operation types and weights
// ---------------------------------------------------------------

type OpType =
  | "writePage"
  | "writePages"
  | "deleteFile"
  | "renameFile"
  | "deletePagesFrom"
  | "writeMeta"
  | "writeMetas"
  | "deleteMeta"
  | "deleteMetas"
  | "syncAll"
  | "deleteAll"
  | "readVerify";

const OP_WEIGHTS: Array<[OpType, number]> = [
  ["writePage", 20],
  ["writePages", 10],
  ["deleteFile", 5],
  ["renameFile", 5],
  ["deletePagesFrom", 5],
  ["writeMeta", 15],
  ["writeMetas", 5],
  ["deleteMeta", 5],
  ["deleteMetas", 3],
  ["syncAll", 5],
  ["deleteAll", 5],
  ["readVerify", 10],
];

function buildOpTable(weights: Array<[OpType, number]>): OpType[] {
  const table: OpType[] = [];
  for (const [op, weight] of weights) {
    for (let i = 0; i < weight; i++) {
      table.push(op);
    }
  }
  return table;
}

const OP_TABLE = buildOpTable(OP_WEIGHTS);

// File paths used in fuzzing — small pool to maximize interactions
const FILE_POOL = ["/a", "/b", "/c", "/d", "/e"];

/** Generate a random FileMeta. */
function randomMeta(rng: Rng): FileMeta {
  return {
    size: rng.int(PAGE_SIZE * 4),
    mode: 0o100644,
    ctime: 1000000 + rng.int(100000),
    mtime: 1000000 + rng.int(100000),
    atime: 1000000 + rng.int(100000),
  };
}

// ---------------------------------------------------------------
// Shared operation executor
// ---------------------------------------------------------------

function executeOp(
  op: OpType,
  rng: Rng,
  preload: PreloadBackend,
  model: ReferenceModel,
  activeFiles: Set<string>,
  context: string,
): void {
  switch (op) {
    case "writePage": {
      const path = rng.pick(FILE_POOL);
      const pageIndex = rng.int(6);
      const data = rng.bytes(PAGE_SIZE);
      preload.writePage(path, pageIndex, data);
      model.writePage(path, pageIndex, data);
      activeFiles.add(path);
      break;
    }

    case "writePages": {
      const path = rng.pick(FILE_POOL);
      const count = rng.int(3) + 1;
      const pages: Array<{
        path: string;
        pageIndex: number;
        data: Uint8Array;
      }> = [];
      for (let i = 0; i < count; i++) {
        const pageIndex = rng.int(6);
        const data = rng.bytes(PAGE_SIZE);
        pages.push({ path, pageIndex, data });
        model.writePage(path, pageIndex, data);
      }
      preload.writePages(pages);
      activeFiles.add(path);
      break;
    }

    case "deleteFile": {
      if (activeFiles.size === 0) break;
      const path = rng.pick([...activeFiles]);
      preload.deleteFile(path);
      model.deleteFile(path);
      break;
    }

    case "renameFile": {
      if (activeFiles.size === 0) break;
      const oldPath = rng.pick([...activeFiles]);
      const newPath = rng.pick(FILE_POOL);
      if (oldPath === newPath) break;
      preload.renameFile(oldPath, newPath);
      model.renameFile(oldPath, newPath);
      activeFiles.add(newPath);
      break;
    }

    case "deletePagesFrom": {
      if (activeFiles.size === 0) break;
      const path = rng.pick([...activeFiles]);
      const fromIndex = rng.int(5);
      preload.deletePagesFrom(path, fromIndex);
      model.deletePagesFrom(path, fromIndex);
      break;
    }

    case "writeMeta": {
      const path = rng.pick(FILE_POOL);
      const meta = randomMeta(rng);
      preload.writeMeta(path, meta);
      model.writeMeta(path, meta);
      activeFiles.add(path);
      break;
    }

    case "writeMetas": {
      const count = rng.int(3) + 1;
      const entries: Array<{ path: string; meta: FileMeta }> = [];
      for (let i = 0; i < count; i++) {
        const path = rng.pick(FILE_POOL);
        const meta = randomMeta(rng);
        entries.push({ path, meta });
        model.writeMeta(path, meta);
        activeFiles.add(path);
      }
      preload.writeMetas(entries);
      break;
    }

    case "deleteMeta": {
      if (activeFiles.size === 0) break;
      const path = rng.pick([...activeFiles]);
      preload.deleteMeta(path);
      model.deleteMeta(path);
      break;
    }

    case "deleteMetas": {
      if (activeFiles.size === 0) break;
      const count = Math.min(rng.int(3) + 1, activeFiles.size);
      const paths = [...activeFiles].slice(0, count);
      preload.deleteMetas(paths);
      for (const p of paths) {
        model.deleteMeta(p);
      }
      break;
    }

    case "syncAll": {
      const path = rng.pick(FILE_POOL);
      const pageIndex = rng.int(6);
      const data = rng.bytes(PAGE_SIZE);
      const meta = randomMeta(rng);
      preload.syncAll(
        [{ path, pageIndex, data }],
        [{ path, meta }],
      );
      model.writePage(path, pageIndex, data);
      model.writeMeta(path, meta);
      activeFiles.add(path);
      break;
    }

    case "deleteAll": {
      if (activeFiles.size === 0) break;
      const count = Math.min(rng.int(3) + 1, activeFiles.size);
      const paths = [...activeFiles].slice(0, count);
      preload.deleteAll(paths);
      model.deleteAll(paths);
      break;
    }

    case "readVerify": {
      // Mid-cycle read verification: PreloadBackend's in-memory state
      // should match the reference model exactly.
      if (activeFiles.size === 0) break;
      const path = rng.pick([...activeFiles]);
      const pageIndex = rng.int(6);
      const actual = preload.readPage(path, pageIndex);
      const expected = model.readPage(path, pageIndex);
      if (expected === null) {
        expect(
          actual,
          `${context}, readVerify page ${path}:${pageIndex} — expected null`,
        ).toBeNull();
      } else {
        expect(actual).not.toBeNull();
        expect(
          actual!,
          `${context}, readVerify page ${path}:${pageIndex} — data mismatch`,
        ).toEqual(expected);
      }

      const actualMeta = preload.readMeta(path);
      const expectedMeta = model.meta.get(path) ?? null;
      if (expectedMeta === null) {
        expect(
          actualMeta,
          `${context}, readVerify meta ${path} — expected null`,
        ).toBeNull();
      } else {
        expect(
          actualMeta,
          `${context}, readVerify meta ${path} — data mismatch`,
        ).toEqual(expectedMeta);
      }
      break;
    }
  }
}

// ---------------------------------------------------------------
// Core fuzz session
// ---------------------------------------------------------------

async function runFuzzSession(
  seed: number,
  opsPerCycle: number,
  flushCycles: number,
  opTable: OpType[] = OP_TABLE,
): Promise<void> {
  const rng = new Rng(seed);
  const remote = new MemoryBackend();
  const preload = new PreloadBackend(remote);
  await preload.init();

  const model = new ReferenceModel();
  const activeFiles = new Set<string>();

  for (let cycle = 0; cycle < flushCycles; cycle++) {
    for (let step = 0; step < opsPerCycle; step++) {
      const op = rng.pick(opTable);
      const context = `seed ${seed}, cycle ${cycle}, step ${step}, op ${op}`;
      try {
        executeOp(op, rng, preload, model, activeFiles, context);
      } catch (e) {
        throw new Error(`${context}: ${(e as Error).message}`);
      }
    }

    // Flush all dirty state to the remote backend
    await preload.flush();

    // Verify: create a fresh PreloadBackend from the same remote and compare
    await verifyRoundtrip(remote, model, seed, cycle);
  }
}

/**
 * Create a fresh PreloadBackend from the remote, init it, and verify
 * all observable state matches the reference model.
 *
 * Only verifies pages at paths that have metadata in the model, since
 * PreloadBackend.init() discovers files via listFiles() (which returns
 * paths with metadata). Pages at paths without metadata are invisible
 * after a roundtrip — this is by design, not a bug.
 */
async function verifyRoundtrip(
  remote: MemoryBackend,
  model: ReferenceModel,
  seed: number,
  cycle: number,
): Promise<void> {
  const fresh = new PreloadBackend(remote);
  await fresh.init();

  const context = `seed ${seed}, after cycle ${cycle}`;

  // 1. Verify file list matches
  const actualFiles = fresh.listFiles().sort();
  const expectedFiles = model.listFiles();
  expect(actualFiles, `listFiles mismatch (${context})`).toEqual(expectedFiles);

  // 2. Verify all metadata matches
  for (const path of expectedFiles) {
    const actualMeta = fresh.readMeta(path);
    const expectedMeta = model.meta.get(path)!;
    expect(actualMeta, `metadata mismatch for ${path} (${context})`).toEqual(
      expectedMeta,
    );
  }

  // 3. Verify pages at paths with metadata
  for (const path of expectedFiles) {
    const expectedIndices = model.pageIndicesForFile(path);

    for (const idx of expectedIndices) {
      const actual = fresh.readPage(path, idx);
      const expected = model.readPage(path, idx)!;
      expect(actual).not.toBeNull();
      expect(
        actual!,
        `page data mismatch at ${path}:${idx} (${context})`,
      ).toEqual(expected);
    }

    // Check for orphan pages in the remote beyond what the model expects
    const maxIdx = fresh.maxPageIndex(path);
    for (let i = 0; i <= maxIdx; i++) {
      const actual = fresh.readPage(path, i);
      const expected = model.readPage(path, i);
      if (expected === null && actual !== null) {
        // Check if the actual page is all zeros — remote may have sparse pages
        // from allocate/truncate. If it's non-zero, it's a real orphan.
        const isZero = actual.every((b) => b === 0);
        if (!isZero) {
          throw new Error(
            `Orphan page at ${path}:${i} (${context}): remote has non-zero data but model doesn't`,
          );
        }
      }
    }
  }
}

// ---------------------------------------------------------------
// Test cases
// ---------------------------------------------------------------

describe("PreloadBackend flush roundtrip fuzz", () => {
  // Basic: few ops per cycle, many cycles — exercises repeated flush
  describe("frequent flush (10 ops × 10 cycles)", () => {
    const OPS = 10;
    const CYCLES = 10;

    for (let seed = 1; seed <= 3; seed++) {
      it(`seed ${seed} (${OPS} ops × ${CYCLES} cycles) @fast`, async () => {
        await runFuzzSession(seed, OPS, CYCLES);
      });
    }
  });

  // Medium: moderate ops with multiple flush cycles
  describe("medium batches (50 ops × 5 cycles)", () => {
    const OPS = 50;
    const CYCLES = 5;

    for (let seed = 100; seed <= 104; seed++) {
      const tag = seed <= 102 ? " @fast" : "";
      it(`seed ${seed} (${OPS} ops × ${CYCLES} cycles)${tag}`, async () => {
        await runFuzzSession(seed, OPS, CYCLES);
      });
    }
  });

  // Heavy: many ops between flushes — maximizes dirty state complexity
  describe("large batches (200 ops × 3 cycles)", () => {
    const OPS = 200;
    const CYCLES = 3;

    for (let seed = 200; seed <= 202; seed++) {
      it(`seed ${seed} (${OPS} ops × ${CYCLES} cycles)`, async () => {
        await runFuzzSession(seed, OPS, CYCLES);
      });
    }
  });

  // Extended: long sessions with steady flush cadence
  describe("extended sessions (100 ops × 8 cycles)", () => {
    const OPS = 100;
    const CYCLES = 8;

    for (let seed = 300; seed <= 301; seed++) {
      it(`seed ${seed} (${OPS} ops × ${CYCLES} cycles)`, async () => {
        await runFuzzSession(seed, OPS, CYCLES);
      });
    }
  });

  // Delete-heavy: biased toward deletions and renames to stress flush partitioning
  describe("delete-heavy (80 ops × 5 cycles)", () => {
    const opTable = buildOpTable([
      ["writePage", 10],
      ["writePages", 5],
      ["deleteFile", 15],
      ["renameFile", 15],
      ["deletePagesFrom", 10],
      ["writeMeta", 10],
      ["writeMetas", 3],
      ["deleteMeta", 10],
      ["deleteMetas", 5],
      ["syncAll", 5],
      ["deleteAll", 8],
      ["readVerify", 5],
    ]);

    for (let seed = 400; seed <= 402; seed++) {
      it(`seed ${seed} (delete-heavy)`, async () => {
        await runFuzzSession(seed, 80, 5, opTable);
      });
    }
  });

  // Rename-overwrite: specifically targets the delete-then-recreate flush path
  describe("rename-overwrite stress (60 ops × 6 cycles)", () => {
    const opTable = buildOpTable([
      ["writePage", 15],
      ["writePages", 5],
      ["deleteFile", 5],
      ["renameFile", 25],
      ["deletePagesFrom", 5],
      ["writeMeta", 15],
      ["writeMetas", 3],
      ["deleteMeta", 3],
      ["deleteMetas", 2],
      ["syncAll", 5],
      ["deleteAll", 5],
      ["readVerify", 10],
    ]);

    for (let seed = 500; seed <= 502; seed++) {
      it(`seed ${seed} (rename-overwrite stress)`, async () => {
        await runFuzzSession(seed, 60, 6, opTable);
      });
    }
  });

  // Truncation stress: biased toward deletePagesFrom + writes
  describe("truncation stress (80 ops × 5 cycles)", () => {
    const opTable = buildOpTable([
      ["writePage", 20],
      ["writePages", 10],
      ["deleteFile", 3],
      ["renameFile", 3],
      ["deletePagesFrom", 25],
      ["writeMeta", 10],
      ["writeMetas", 3],
      ["deleteMeta", 2],
      ["deleteMetas", 1],
      ["syncAll", 5],
      ["deleteAll", 3],
      ["readVerify", 10],
    ]);

    for (let seed = 600; seed <= 602; seed++) {
      it(`seed ${seed} (truncation stress)`, async () => {
        await runFuzzSession(seed, 80, 5, opTable);
      });
    }
  });

  // deleteAll stress: biased toward deleteAll + writes to exercise the
  // combined page+metadata deletion path and its interaction with flush
  // partitioning. deleteAll adds paths to BOTH deletedFiles and deletedMeta,
  // so subsequent writes at the same path must be deferred to the late batch.
  describe("deleteAll stress (80 ops × 5 cycles)", () => {
    const opTable = buildOpTable([
      ["writePage", 15],
      ["writePages", 5],
      ["deleteFile", 3],
      ["renameFile", 5],
      ["deletePagesFrom", 3],
      ["writeMeta", 15],
      ["writeMetas", 3],
      ["deleteMeta", 3],
      ["deleteMetas", 2],
      ["syncAll", 5],
      ["deleteAll", 20],
      ["readVerify", 10],
    ]);

    for (let seed = 700; seed <= 704; seed++) {
      it(`seed ${seed} (deleteAll stress)`, async () => {
        await runFuzzSession(seed, 80, 5, opTable);
      });
    }
  });
});
