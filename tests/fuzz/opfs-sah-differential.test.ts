/**
 * Differential fuzz tests for OpfsSahBackend.
 *
 * Compares random operation sequences against MemoryBackend (the simplest
 * correct reference) to verify behavioral equivalence. While the contract
 * tests verify individual operations in isolation, this test targets
 * *interaction* bugs: sequences that individually work but produce divergent
 * state when combined.
 *
 * OpfsSahBackend-specific implementation details that create surface area:
 * - Single OPFS file per virtual file with pages at fixed offsets
 * - Sparse writes create zero-filled gaps (gap pages are not null)
 * - LRU handle cache with eviction (handles can be closed/reopened)
 * - renameFile: read-entire-file + write-new + delete-old
 * - deletePagesFrom: truncate() on sync access handle
 * - Hex path encoding/decoding for OPFS filenames
 *
 * Verification accounts for dense storage: gap pages may be zero-filled
 * buffers in the SAH backend where MemoryBackend returns null.
 *
 * Uses a seeded PRNG for reproducibility — failing seeds can be replayed.
 *
 * Ethos §9: "Write tests designed to break tomefs specifically"
 */

import { describe, it, expect } from "vitest";
import { OpfsSahBackend } from "../../src/opfs-sah-backend.js";
import { MemoryBackend } from "../../src/memory-backend.js";
import { PAGE_SIZE } from "../../src/types.js";
import type { StorageBackend } from "../../src/storage-backend.js";
import type { FileMeta } from "../../src/types.js";
import { createFakeOpfsRoot } from "../harness/fake-opfs.js";

// ---------------------------------------------------------------
// Seeded PRNG (xorshift128+) for reproducible random sequences
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

// ---------------------------------------------------------------
// Operation generation
// ---------------------------------------------------------------

const FILE_PATHS = ["/a", "/b", "/c", "/d", "/e", "/f", "/g", "/h"];
const MAX_PAGE_INDEX = 15;

function randomMeta(rng: Rng): FileMeta {
  return {
    size: rng.int(100000),
    mode: 0o100644,
    ctime: 1000 + rng.int(10000),
    mtime: 2000 + rng.int(10000),
  };
}

function randomPageData(rng: Rng): Uint8Array {
  return rng.bytes(PAGE_SIZE);
}

interface WeightedOp {
  weight: number;
  generate: (rng: Rng) => Op;
}

const OP_TABLE: WeightedOp[] = [
  {
    weight: 20,
    generate: (rng) => ({
      type: "writePage",
      path: rng.pick(FILE_PATHS),
      pageIndex: rng.int(MAX_PAGE_INDEX),
      data: randomPageData(rng),
    }),
  },
  {
    weight: 8,
    generate: (rng) => {
      const count = 1 + rng.int(4);
      const pages: Array<{ path: string; pageIndex: number; data: Uint8Array }> = [];
      for (let i = 0; i < count; i++) {
        pages.push({
          path: rng.pick(FILE_PATHS),
          pageIndex: rng.int(MAX_PAGE_INDEX),
          data: randomPageData(rng),
        });
      }
      return { type: "writePages", pages };
    },
  },
  {
    weight: 10,
    generate: (rng) => ({
      type: "deletePagesFrom",
      path: rng.pick(FILE_PATHS),
      fromPageIndex: rng.int(MAX_PAGE_INDEX + 2),
    }),
  },
  {
    weight: 8,
    generate: (rng) => ({
      type: "deleteFile",
      path: rng.pick(FILE_PATHS),
    }),
  },
  {
    weight: 4,
    generate: (rng) => {
      const count = 1 + rng.int(3);
      const paths = new Set<string>();
      for (let i = 0; i < count; i++) {
        paths.add(rng.pick(FILE_PATHS));
      }
      return { type: "deleteFiles", paths: [...paths] };
    },
  },
  {
    weight: 12,
    generate: (rng) => ({
      type: "renameFile",
      oldPath: rng.pick(FILE_PATHS),
      newPath: rng.pick(FILE_PATHS),
    }),
  },
  {
    weight: 10,
    generate: (rng) => ({
      type: "writeMeta",
      path: rng.pick(FILE_PATHS),
      meta: randomMeta(rng),
    }),
  },
  {
    weight: 5,
    generate: (rng) => {
      const count = 1 + rng.int(3);
      const entries: Array<{ path: string; meta: FileMeta }> = [];
      for (let i = 0; i < count; i++) {
        entries.push({
          path: rng.pick(FILE_PATHS),
          meta: randomMeta(rng),
        });
      }
      return { type: "writeMetas", entries };
    },
  },
  {
    weight: 6,
    generate: (rng) => ({
      type: "deleteMeta",
      path: rng.pick(FILE_PATHS),
    }),
  },
  {
    weight: 4,
    generate: (rng) => {
      const count = 1 + rng.int(3);
      const paths = new Set<string>();
      for (let i = 0; i < count; i++) {
        paths.add(rng.pick(FILE_PATHS));
      }
      return { type: "deleteMetas", paths: [...paths] };
    },
  },
  {
    weight: 8,
    generate: (rng) => {
      const pageCount = rng.int(4);
      const metaCount = rng.int(3);
      const pages: Array<{ path: string; pageIndex: number; data: Uint8Array }> = [];
      for (let i = 0; i < pageCount; i++) {
        pages.push({
          path: rng.pick(FILE_PATHS),
          pageIndex: rng.int(MAX_PAGE_INDEX),
          data: randomPageData(rng),
        });
      }
      const metas: Array<{ path: string; meta: FileMeta }> = [];
      for (let i = 0; i < metaCount; i++) {
        metas.push({
          path: rng.pick(FILE_PATHS),
          meta: randomMeta(rng),
        });
      }
      return { type: "syncAll", pages, metas };
    },
  },
  {
    weight: 5,
    generate: (rng) => {
      const count = 1 + rng.int(3);
      const paths = new Set<string>();
      for (let i = 0; i < count; i++) {
        paths.add(rng.pick(FILE_PATHS));
      }
      return { type: "deleteAll", paths: [...paths] };
    },
  },
];

const TOTAL_WEIGHT = OP_TABLE.reduce((sum, op) => sum + op.weight, 0);

function generateOp(rng: Rng): Op {
  let roll = rng.int(TOTAL_WEIGHT);
  for (const entry of OP_TABLE) {
    roll -= entry.weight;
    if (roll < 0) return entry.generate(rng);
  }
  return OP_TABLE[OP_TABLE.length - 1].generate(rng);
}

// ---------------------------------------------------------------
// Operation execution
// ---------------------------------------------------------------

async function executeOp(backend: StorageBackend, op: Op): Promise<void> {
  switch (op.type) {
    case "writePage":
      await backend.writePage(op.path, op.pageIndex, new Uint8Array(op.data));
      break;
    case "writePages":
      await backend.writePages(
        op.pages.map((p) => ({ ...p, data: new Uint8Array(p.data) })),
      );
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
      await backend.writeMeta(op.path, { ...op.meta });
      break;
    case "writeMetas":
      await backend.writeMetas(
        op.entries.map((e) => ({ path: e.path, meta: { ...e.meta } })),
      );
      break;
    case "deleteMeta":
      await backend.deleteMeta(op.path);
      break;
    case "deleteMetas":
      await backend.deleteMetas(op.paths);
      break;
    case "syncAll":
      await backend.syncAll(
        op.pages.map((p) => ({ ...p, data: new Uint8Array(p.data) })),
        op.metas.map((e) => ({ path: e.path, meta: { ...e.meta } })),
      );
      break;
    case "deleteAll":
      await backend.deleteAll(op.paths);
      break;
  }
}

// ---------------------------------------------------------------
// State verification (dense-storage aware)
// ---------------------------------------------------------------

const ZERO_PAGE = new Uint8Array(PAGE_SIZE);

/**
 * Verify that sah and mem backends are in equivalent state.
 *
 * Accounts for the OpfsSahBackend's dense storage model:
 * - Gap pages return zero-filled buffers instead of null
 * - maxPageIndex may be higher than MemoryBackend's after truncation
 *   of sparse files (dense storage retains all pages up to the truncation
 *   point, even if MemoryBackend had no explicit pages in that range)
 * - countPages diverges for sparse files (dense counts all slots)
 *
 * The invariant we verify: every page that MemoryBackend reports as
 * written must exist with identical content in SAH, and any "extra"
 * pages in SAH (due to dense storage) must be zero-filled.
 */
async function verifyEquivalence(
  sah: StorageBackend,
  mem: StorageBackend,
  label: string,
): Promise<void> {
  const sahFiles = (await sah.listFiles()).sort();
  const memFiles = (await mem.listFiles()).sort();
  expect(sahFiles, `${label}: listFiles`).toEqual(memFiles);

  const allPaths = new Set([...sahFiles, ...FILE_PATHS]);
  for (const path of allPaths) {
    const sahMeta = await sah.readMeta(path);
    const memMeta = await mem.readMeta(path);
    expect(sahMeta, `${label}: readMeta(${path})`).toEqual(memMeta);
  }

  const allPathsArr = [...allPaths];
  const sahMetas = await sah.readMetas(allPathsArr);
  const memMetas = await mem.readMetas(allPathsArr);
  expect(sahMetas, `${label}: readMetas`).toEqual(memMetas);

  const sahMaxIdx = await sah.maxPageIndexBatch(allPathsArr);
  const memMaxIdx = await mem.maxPageIndexBatch(allPathsArr);

  for (let i = 0; i < allPathsArr.length; i++) {
    const path = allPathsArr[i];
    const memMax = memMaxIdx[i];
    const sahMax = sahMaxIdx[i];

    // SAH's maxPageIndex must be >= mem's (dense storage may have extra
    // zero-filled pages, but must never lose pages that mem reports)
    expect(
      sahMax >= memMax,
      `${label}: maxPageIndex(${path}) — SAH ${sahMax} < mem ${memMax}`,
    ).toBe(true);

    const effectiveMax = Math.max(memMax, sahMax);
    if (effectiveMax < 0) continue;

    const indices = Array.from({ length: effectiveMax + 1 }, (_, j) => j);
    const sahPages = await sah.readPages(path, indices);
    const memPages = await mem.readPages(path, indices);
    for (let j = 0; j <= effectiveMax; j++) {
      const sahPage = sahPages[j];
      const memPage = memPages[j];
      if (memPage === null) {
        if (sahPage !== null) {
          expect(
            buffersEqual(sahPage, ZERO_PAGE),
            `${label}: readPage(${path}, ${j}) — expected null or zero page`,
          ).toBe(true);
        }
      } else {
        expect(sahPage, `${label}: readPage(${path}, ${j}) not null`).not.toBeNull();
        if (!buffersEqual(sahPage!, memPage)) {
          expect.fail(
            `${label}: readPage(${path}, ${j}) content mismatch — ` +
              `first diff at byte ${findFirstDiff(sahPage!, memPage)}`,
          );
        }
      }
    }

    // Verify no data exists beyond SAH's own maxPageIndex
    if (sahMax >= 0) {
      const beyondPage = await sah.readPage(path, sahMax + 1);
      expect(
        beyondPage,
        `${label}: readPage(${path}, ${sahMax + 1}) should be null beyond max`,
      ).toBeNull();
    }
  }
}

function buffersEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

function findFirstDiff(a: Uint8Array, b: Uint8Array): number {
  const len = Math.min(a.length, b.length);
  for (let i = 0; i < len; i++) {
    if (a[i] !== b[i]) return i;
  }
  return len;
}

// ---------------------------------------------------------------
// Operation formatter (for debugging failing seeds)
// ---------------------------------------------------------------

function formatOp(op: Op): string {
  switch (op.type) {
    case "writePage":
      return `writePage(${op.path}, ${op.pageIndex})`;
    case "writePages":
      return `writePages([${op.pages.map((p) => `${p.path}:${p.pageIndex}`).join(", ")}])`;
    case "deletePagesFrom":
      return `deletePagesFrom(${op.path}, ${op.fromPageIndex})`;
    case "deleteFile":
      return `deleteFile(${op.path})`;
    case "deleteFiles":
      return `deleteFiles([${op.paths.join(", ")}])`;
    case "renameFile":
      return `renameFile(${op.oldPath}, ${op.newPath})`;
    case "writeMeta":
      return `writeMeta(${op.path})`;
    case "writeMetas":
      return `writeMetas([${op.entries.map((e) => e.path).join(", ")}])`;
    case "deleteMeta":
      return `deleteMeta(${op.path})`;
    case "deleteMetas":
      return `deleteMetas([${op.paths.join(", ")}])`;
    case "syncAll":
      return `syncAll(pages=[${op.pages.map((p) => `${p.path}:${p.pageIndex}`).join(", ")}], metas=[${op.metas.map((m) => m.path).join(", ")}])`;
    case "deleteAll":
      return `deleteAll([${op.paths.join(", ")}])`;
  }
}

// ---------------------------------------------------------------
// Core fuzz runner
// ---------------------------------------------------------------

async function runFuzz(
  seed: number,
  opsCount: number,
  verifyEvery: number,
): Promise<void> {
  const rng = new Rng(seed);
  const root = createFakeOpfsRoot();
  const sah: StorageBackend = new OpfsSahBackend({ root: root as any });
  const mem: StorageBackend = new MemoryBackend();

  const ops: Op[] = [];
  for (let i = 0; i < opsCount; i++) {
    ops.push(generateOp(rng));
  }

  for (let i = 0; i < ops.length; i++) {
    const op = ops[i];
    try {
      await executeOp(sah, op);
      await executeOp(mem, op);
    } catch (err) {
      throw new Error(
        `seed=${seed} op=${i}/${ops.length} ${formatOp(op)} threw: ${err instanceof Error ? err.message : String(err)}`,
      );
    }

    if ((i + 1) % verifyEvery === 0 || i === ops.length - 1) {
      await verifyEquivalence(
        sah,
        mem,
        `seed=${seed} after op ${i + 1}/${ops.length} (${formatOp(op)})`,
      );
    }
  }

  await (sah as OpfsSahBackend).destroy();
}

// ---------------------------------------------------------------
// Test suites
// ---------------------------------------------------------------

describe("OpfsSahBackend differential fuzz", () => {
  describe("short sequences (verify every op)", () => {
    const seeds = [1, 2, 3, 4, 5];
    for (const seed of seeds) {
      const tag = seed <= 2 ? " @fast" : "";
      it(`seed ${seed}, 20 ops${tag}`, async () => {
        await runFuzz(seed, 20, 1);
      }, 10_000);
    }
  });

  describe("medium sequences", () => {
    const seeds = [100, 101, 102, 103, 104, 105, 106, 107, 108, 109];
    for (const seed of seeds) {
      const tag = seed <= 101 ? " @fast" : "";
      it(`seed ${seed}, 50 ops${tag}`, async () => {
        await runFuzz(seed, 50, 5);
      }, 15_000);
    }
  });

  describe("long sequences", () => {
    const seeds = [200, 201, 202, 203, 204];
    for (const seed of seeds) {
      it(`seed ${seed}, 150 ops`, async () => {
        await runFuzz(seed, 150, 10);
      }, 30_000);
    }
  });

  // Rename-heavy: the SAH backend's rename reads the entire source file
  // into memory and writes it to a new file. With handle cache eviction,
  // this can interact with stale handle state.
  describe("rename-heavy sequences", () => {
    const RENAME_HEAVY_OPS: WeightedOp[] = [
      {
        weight: 10,
        generate: (rng) => ({
          type: "writePage",
          path: rng.pick(FILE_PATHS),
          pageIndex: rng.int(MAX_PAGE_INDEX),
          data: randomPageData(rng),
        }),
      },
      {
        weight: 30,
        generate: (rng) => ({
          type: "renameFile",
          oldPath: rng.pick(FILE_PATHS),
          newPath: rng.pick(FILE_PATHS),
        }),
      },
      {
        weight: 8,
        generate: (rng) => ({
          type: "writeMeta",
          path: rng.pick(FILE_PATHS),
          meta: randomMeta(rng),
        }),
      },
      {
        weight: 5,
        generate: (rng) => ({
          type: "deleteFile",
          path: rng.pick(FILE_PATHS),
        }),
      },
      {
        weight: 5,
        generate: (rng) => ({
          type: "deletePagesFrom",
          path: rng.pick(FILE_PATHS),
          fromPageIndex: rng.int(MAX_PAGE_INDEX + 2),
        }),
      },
    ];

    const RENAME_TOTAL = RENAME_HEAVY_OPS.reduce((s, o) => s + o.weight, 0);

    function generateRenameOp(rng: Rng): Op {
      let roll = rng.int(RENAME_TOTAL);
      for (const entry of RENAME_HEAVY_OPS) {
        roll -= entry.weight;
        if (roll < 0) return entry.generate(rng);
      }
      return RENAME_HEAVY_OPS[0].generate(rng);
    }

    async function runRenameFuzz(seed: number): Promise<void> {
      const rng = new Rng(seed);
      const root = createFakeOpfsRoot();
      const sah: StorageBackend = new OpfsSahBackend({ root: root as any });
      const mem: StorageBackend = new MemoryBackend();

      const ops: Op[] = [];
      for (let i = 0; i < 80; i++) {
        ops.push(generateRenameOp(rng));
      }

      for (let i = 0; i < ops.length; i++) {
        await executeOp(sah, ops[i]);
        await executeOp(mem, ops[i]);

        if ((i + 1) % 5 === 0 || i === ops.length - 1) {
          await verifyEquivalence(
            sah,
            mem,
            `rename-heavy seed=${seed} after op ${i + 1}`,
          );
        }
      }

      await (sah as OpfsSahBackend).destroy();
    }

    const seeds = [300, 301, 302, 303, 304];
    for (const seed of seeds) {
      const tag = seed === 300 ? " @fast" : "";
      it(`seed ${seed}, 80 ops${tag}`, async () => {
        await runRenameFuzz(seed);
      }, 20_000);
    }
  });

  // Handle-cache pressure: use a small maxOpenHandles to force frequent
  // handle eviction and reopening. This targets bugs where stale handles
  // survive eviction or where handle close/reopen loses unflushed data.
  describe("handle-cache pressure sequences", () => {
    async function runHandlePressureFuzz(seed: number): Promise<void> {
      const rng = new Rng(seed);
      const root = createFakeOpfsRoot();
      const sah: StorageBackend = new OpfsSahBackend({
        root: root as any,
        maxOpenHandles: 3,
      });
      const mem: StorageBackend = new MemoryBackend();

      const ops: Op[] = [];
      for (let i = 0; i < 60; i++) {
        ops.push(generateOp(rng));
      }

      for (let i = 0; i < ops.length; i++) {
        await executeOp(sah, ops[i]);
        await executeOp(mem, ops[i]);

        if ((i + 1) % 5 === 0 || i === ops.length - 1) {
          await verifyEquivalence(
            sah,
            mem,
            `handle-pressure seed=${seed} after op ${i + 1}`,
          );
        }
      }

      await (sah as OpfsSahBackend).destroy();
    }

    const seeds = [600, 601, 602, 603, 604];
    for (const seed of seeds) {
      const tag = seed === 600 ? " @fast" : "";
      it(`seed ${seed}, 60 ops (maxOpenHandles=3)${tag}`, async () => {
        await runHandlePressureFuzz(seed);
      }, 20_000);
    }
  });

  // cleanupOrphanedPages verification
  describe("cleanupOrphanedPages verification", () => {
    it("removes page files with no metadata after partial deleteAll @fast", async () => {
      const root = createFakeOpfsRoot();
      const sah = new OpfsSahBackend({ root: root as any });

      for (const path of ["/x", "/y", "/z"]) {
        await sah.writePage(path, 0, randomPageData(new Rng(42)));
        await sah.writeMeta(path, { size: PAGE_SIZE, mode: 0o100644, ctime: 1000, mtime: 2000 });
      }

      await sah.deleteMeta("/x");
      await sah.deleteMeta("/z");

      const orphans = await sah.cleanupOrphanedPages();
      expect(orphans).toBe(2);

      const files = await sah.listFiles();
      expect(files).toEqual(["/y"]);
      expect(await sah.readPage("/y", 0)).not.toBeNull();
      expect(await sah.readPage("/x", 0)).toBeNull();
      expect(await sah.readPage("/z", 0)).toBeNull();

      await sah.destroy();
    });

    it("no-ops when all pages have metadata", async () => {
      const root = createFakeOpfsRoot();
      const sah = new OpfsSahBackend({ root: root as any });

      for (const path of ["/a", "/b"]) {
        await sah.writePage(path, 0, randomPageData(new Rng(99)));
        await sah.writeMeta(path, { size: PAGE_SIZE, mode: 0o100644, ctime: 1000, mtime: 2000 });
      }

      expect(await sah.cleanupOrphanedPages()).toBe(0);
      expect((await sah.listFiles()).sort()).toEqual(["/a", "/b"]);

      await sah.destroy();
    });

    it("idempotent — second cleanup returns 0", async () => {
      const root = createFakeOpfsRoot();
      const sah = new OpfsSahBackend({ root: root as any });

      await sah.writePage("/stale", 0, randomPageData(new Rng(1)));

      expect(await sah.cleanupOrphanedPages()).toBe(1);
      expect(await sah.cleanupOrphanedPages()).toBe(0);

      await sah.destroy();
    });

    it("handles cleanup after fuzz sequence with deleteAll", async () => {
      const rng = new Rng(777);
      const root = createFakeOpfsRoot();
      const sah = new OpfsSahBackend({ root: root as any });

      for (let i = 0; i < 20; i++) {
        const path = rng.pick(FILE_PATHS);
        await sah.writePage(path, rng.int(MAX_PAGE_INDEX), randomPageData(rng));
        await sah.writeMeta(path, randomMeta(rng));
      }

      await sah.deleteAll(["/a", "/c", "/e"]);

      expect(await sah.cleanupOrphanedPages()).toBe(0);

      await sah.writePage("/orphan1", 0, randomPageData(rng));
      await sah.writePage("/orphan2", 0, randomPageData(rng));

      expect(await sah.cleanupOrphanedPages()).toBe(2);
      expect(await sah.readPage("/orphan1", 0)).toBeNull();
      expect(await sah.readPage("/orphan2", 0)).toBeNull();

      await sah.destroy();
    });
  });
});
