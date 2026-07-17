/**
 * Differential fuzz tests for OpfsBackend.
 *
 * Compares random operation sequences against MemoryBackend (the simplest
 * correct reference) to verify behavioral equivalence. While the contract
 * tests verify individual operations in isolation, this test targets
 * *interaction* bugs: sequences that individually work but produce divergent
 * state when combined.
 *
 * OPFS-specific implementation details that create surface area for bugs:
 * - Hex path encoding/decoding (encodePath/decodePath)
 * - Per-file directory structures (one OPFS dir per virtual file)
 * - renameFile: copy-all-pages + verify-count + delete-old
 * - deletePagesFrom: async iteration over directory entries
 * - syncAll: sequential writePages + writeMetas (no atomicity)
 *
 * Uses a seeded PRNG for reproducibility — failing seeds can be replayed.
 *
 * Ethos §9: "Write tests designed to break tomefs specifically"
 */

import { describe, it, expect } from "vitest";
import { OpfsBackend } from "../../src/opfs-backend.js";
import { MemoryBackend } from "../../src/memory-backend.js";
import { PAGE_SIZE } from "../../src/types.js";
import type { StorageBackend } from "../../src/storage-backend.js";
import type { FileMeta } from "../../src/types.js";
import { createFakeOpfsRoot } from "../harness/fake-opfs.js";
import { Rng } from "../harness/rng.js";

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

/** Pool of virtual file paths used by the fuzz generator. */
const FILE_PATHS = ["/a", "/b", "/c", "/d", "/e", "/f", "/g", "/h"];

/** Maximum page index used in generated operations. */
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

/** Weighted operation selection. */
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
      fromPageIndex: rng.int(MAX_PAGE_INDEX + 2), // +2 so we sometimes truncate beyond all pages
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
// State verification
// ---------------------------------------------------------------

async function verifyEquivalence(
  opfs: StorageBackend,
  mem: StorageBackend,
  label: string,
): Promise<void> {
  // 1. listFiles should return the same set
  const opfsFiles = (await opfs.listFiles()).sort();
  const memFiles = (await mem.listFiles()).sort();
  expect(opfsFiles, `${label}: listFiles`).toEqual(memFiles);

  // 2. For every known path, compare metadata
  const allPaths = new Set([...opfsFiles, ...FILE_PATHS]);
  for (const path of allPaths) {
    const opfsMeta = await opfs.readMeta(path);
    const memMeta = await mem.readMeta(path);
    expect(opfsMeta, `${label}: readMeta(${path})`).toEqual(memMeta);
  }

  // 3. Batch metadata reads
  const allPathsArr = [...allPaths];
  const opfsMetas = await opfs.readMetas(allPathsArr);
  const memMetas = await mem.readMetas(allPathsArr);
  expect(opfsMetas, `${label}: readMetas`).toEqual(memMetas);

  // 4. For every known path, compare page counts and max page index
  const opfsCounts = await opfs.countPagesBatch(allPathsArr);
  const memCounts = await mem.countPagesBatch(allPathsArr);
  expect(opfsCounts, `${label}: countPagesBatch`).toEqual(memCounts);

  const opfsMaxIdx = await opfs.maxPageIndexBatch(allPathsArr);
  const memMaxIdx = await mem.maxPageIndexBatch(allPathsArr);
  expect(opfsMaxIdx, `${label}: maxPageIndexBatch`).toEqual(memMaxIdx);

  // 5. For every known path, read all pages up to maxPageIndex and compare
  for (let i = 0; i < allPathsArr.length; i++) {
    const path = allPathsArr[i];
    const maxIdx = memMaxIdx[i];
    if (maxIdx < 0) continue;

    const indices = Array.from({ length: maxIdx + 1 }, (_, j) => j);
    const opfsPages = await opfs.readPages(path, indices);
    const memPages = await mem.readPages(path, indices);
    for (let j = 0; j <= maxIdx; j++) {
      const opfsPage = opfsPages[j];
      const memPage = memPages[j];
      if (memPage === null) {
        expect(opfsPage, `${label}: readPage(${path}, ${j})`).toBeNull();
      } else {
        expect(opfsPage, `${label}: readPage(${path}, ${j}) not null`).not.toBeNull();
        expect(
          opfsPage!.length,
          `${label}: readPage(${path}, ${j}) length`,
        ).toBe(memPage.length);
        // Compare content byte-by-byte only on first mismatch to avoid huge diffs
        if (!buffersEqual(opfsPage!, memPage)) {
          expect.fail(
            `${label}: readPage(${path}, ${j}) content mismatch — ` +
              `first diff at byte ${findFirstDiff(opfsPage!, memPage)}`,
          );
        }
      }
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
  return len; // length difference
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
  const opfs: StorageBackend = new OpfsBackend({ root: root as any });
  const mem: StorageBackend = new MemoryBackend();

  const ops: Op[] = [];
  for (let i = 0; i < opsCount; i++) {
    ops.push(generateOp(rng));
  }

  for (let i = 0; i < ops.length; i++) {
    const op = ops[i];
    try {
      await executeOp(opfs, op);
      await executeOp(mem, op);
    } catch (err) {
      throw new Error(
        `seed=${seed} op=${i}/${ops.length} ${formatOp(op)} threw: ${err instanceof Error ? err.message : String(err)}`,
      );
    }

    if ((i + 1) % verifyEvery === 0 || i === ops.length - 1) {
      await verifyEquivalence(
        opfs,
        mem,
        `seed=${seed} after op ${i + 1}/${ops.length} (${formatOp(op)})`,
      );
    }
  }

  // Final cleanup
  await (opfs as OpfsBackend).destroy();
}

// ---------------------------------------------------------------
// Test suites
// ---------------------------------------------------------------

describe("OpfsBackend differential fuzz", () => {
  // Short sequences — full state check after every operation
  describe("short sequences (verify every op)", () => {
    const seeds = [1, 2, 3, 4, 5];
    for (const seed of seeds) {
      const tag = seed <= 2 ? " @fast" : "";
      it(`seed ${seed}, 20 ops${tag}`, async () => {
        await runFuzz(seed, 20, 1);
      }, 10_000);
    }
  });

  // Medium sequences — checkpoint every 5 ops
  describe("medium sequences", () => {
    const seeds = [100, 101, 102, 103, 104, 105, 106, 107, 108, 109];
    for (const seed of seeds) {
      const tag = seed <= 101 ? " @fast" : "";
      it(`seed ${seed}, 50 ops${tag}`, async () => {
        await runFuzz(seed, 50, 5);
      }, 15_000);
    }
  });

  // Long sequences — checkpoint every 10 ops
  describe("long sequences", () => {
    const seeds = [200, 201, 202, 203, 204];
    for (const seed of seeds) {
      it(`seed ${seed}, 150 ops`, async () => {
        await runFuzz(seed, 150, 10);
      }, 30_000);
    }
  });

  // Rename-heavy — stress the copy+verify+delete rename path
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
      const opfs: StorageBackend = new OpfsBackend({ root: root as any });
      const mem: StorageBackend = new MemoryBackend();

      const ops: Op[] = [];
      for (let i = 0; i < 80; i++) {
        ops.push(generateRenameOp(rng));
      }

      for (let i = 0; i < ops.length; i++) {
        await executeOp(opfs, ops[i]);
        await executeOp(mem, ops[i]);

        if ((i + 1) % 5 === 0 || i === ops.length - 1) {
          await verifyEquivalence(
            opfs,
            mem,
            `rename-heavy seed=${seed} after op ${i + 1}`,
          );
        }
      }

      await (opfs as OpfsBackend).destroy();
    }

    const seeds = [300, 301, 302, 303, 304];
    for (const seed of seeds) {
      const tag = seed === 300 ? " @fast" : "";
      it(`seed ${seed}, 80 ops${tag}`, async () => {
        await runRenameFuzz(seed);
      }, 20_000);
    }
  });

  // syncAll-heavy — stress the non-atomic multi-store write path
  describe("syncAll-heavy sequences", () => {
    const SYNCALL_HEAVY_OPS: WeightedOp[] = [
      {
        weight: 8,
        generate: (rng) => ({
          type: "writePage",
          path: rng.pick(FILE_PATHS),
          pageIndex: rng.int(MAX_PAGE_INDEX),
          data: randomPageData(rng),
        }),
      },
      {
        weight: 25,
        generate: (rng) => {
          const pageCount = 1 + rng.int(5);
          const metaCount = 1 + rng.int(3);
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
        weight: 6,
        generate: (rng) => ({
          type: "renameFile",
          oldPath: rng.pick(FILE_PATHS),
          newPath: rng.pick(FILE_PATHS),
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
        weight: 4,
        generate: (rng) => ({
          type: "deleteMeta",
          path: rng.pick(FILE_PATHS),
        }),
      },
    ];

    const SYNCALL_TOTAL = SYNCALL_HEAVY_OPS.reduce((s, o) => s + o.weight, 0);

    function generateSyncAllOp(rng: Rng): Op {
      let roll = rng.int(SYNCALL_TOTAL);
      for (const entry of SYNCALL_HEAVY_OPS) {
        roll -= entry.weight;
        if (roll < 0) return entry.generate(rng);
      }
      return SYNCALL_HEAVY_OPS[0].generate(rng);
    }

    async function runSyncAllFuzz(seed: number): Promise<void> {
      const rng = new Rng(seed);
      const root = createFakeOpfsRoot();
      const opfs: StorageBackend = new OpfsBackend({ root: root as any });
      const mem: StorageBackend = new MemoryBackend();

      const ops: Op[] = [];
      for (let i = 0; i < 60; i++) {
        ops.push(generateSyncAllOp(rng));
      }

      for (let i = 0; i < ops.length; i++) {
        await executeOp(opfs, ops[i]);
        await executeOp(mem, ops[i]);

        if ((i + 1) % 5 === 0 || i === ops.length - 1) {
          await verifyEquivalence(
            opfs,
            mem,
            `syncAll-heavy seed=${seed} after op ${i + 1}`,
          );
        }
      }

      await (opfs as OpfsBackend).destroy();
    }

    const seeds = [400, 401, 402, 403, 404];
    for (const seed of seeds) {
      it(`seed ${seed}, 60 ops`, async () => {
        await runSyncAllFuzz(seed);
      }, 20_000);
    }
  });

  // deleteAll-heavy — stress the combined delete-pages + delete-metadata path.
  // deleteAll is used by tomefs syncfs for orphan cleanup; OPFS implements it
  // as sequential deleteFiles + deleteMetas (no atomicity). This exercises
  // interactions between deleteAll and concurrent writes/renames that could
  // leave inconsistent state.
  describe("deleteAll-heavy sequences", () => {
    async function runDeleteAllFuzz(seed: number): Promise<void> {
      const rng = new Rng(seed);
      const root = createFakeOpfsRoot();
      const opfs = new OpfsBackend({ root: root as any });
      const mem: StorageBackend = new MemoryBackend();

      const ops: Op[] = [];
      for (let i = 0; i < 60; i++) {
        const roll = rng.int(100);
        let op: Op;
        if (roll < 20) {
          op = {
            type: "writePage",
            path: rng.pick(FILE_PATHS),
            pageIndex: rng.int(MAX_PAGE_INDEX),
            data: randomPageData(rng),
          };
        } else if (roll < 30) {
          const count = 1 + rng.int(4);
          const pages: Array<{ path: string; pageIndex: number; data: Uint8Array }> = [];
          for (let j = 0; j < count; j++) {
            pages.push({
              path: rng.pick(FILE_PATHS),
              pageIndex: rng.int(MAX_PAGE_INDEX),
              data: randomPageData(rng),
            });
          }
          op = { type: "writePages", pages };
        } else if (roll < 45) {
          op = {
            type: "writeMeta",
            path: rng.pick(FILE_PATHS),
            meta: randomMeta(rng),
          };
        } else if (roll < 55) {
          const pageCount = 1 + rng.int(3);
          const metaCount = 1 + rng.int(3);
          const pages: Array<{ path: string; pageIndex: number; data: Uint8Array }> = [];
          for (let j = 0; j < pageCount; j++) {
            pages.push({
              path: rng.pick(FILE_PATHS),
              pageIndex: rng.int(MAX_PAGE_INDEX),
              data: randomPageData(rng),
            });
          }
          const metas: Array<{ path: string; meta: FileMeta }> = [];
          for (let j = 0; j < metaCount; j++) {
            metas.push({
              path: rng.pick(FILE_PATHS),
              meta: randomMeta(rng),
            });
          }
          op = { type: "syncAll", pages, metas };
        } else if (roll < 80) {
          const count = 1 + rng.int(3);
          const paths = new Set<string>();
          for (let j = 0; j < count; j++) {
            paths.add(rng.pick(FILE_PATHS));
          }
          op = { type: "deleteAll", paths: [...paths] };
        } else if (roll < 90) {
          op = {
            type: "renameFile",
            oldPath: rng.pick(FILE_PATHS),
            newPath: rng.pick(FILE_PATHS),
          };
        } else {
          op = { type: "deleteFile", path: rng.pick(FILE_PATHS) };
        }
        ops.push(op);
      }

      for (let i = 0; i < ops.length; i++) {
        await executeOp(opfs, ops[i]);
        await executeOp(mem, ops[i]);

        if ((i + 1) % 5 === 0 || i === ops.length - 1) {
          await verifyEquivalence(
            opfs,
            mem,
            `deleteAll-heavy seed=${seed} after op ${i + 1}`,
          );
        }
      }

      await opfs.destroy();
    }

    const seeds = [500, 501, 502, 503, 504];
    for (const seed of seeds) {
      it(`seed ${seed}, 60 ops`, async () => {
        await runDeleteAllFuzz(seed);
      }, 20_000);
    }
  });

  // cleanupOrphanedPages — verify that orphaned page directories (pages
  // without corresponding metadata) are correctly identified and removed.
  // This exercises the OPFS-specific recovery mechanism used during mount
  // when a crash left pages behind without metadata.
  describe("cleanupOrphanedPages verification", () => {
    it("removes page dirs with no metadata after deleteAll partial simulation @fast", async () => {
      const root = createFakeOpfsRoot();
      const opfs = new OpfsBackend({ root: root as any });

      // Write pages and metadata for several files
      for (const path of ["/x", "/y", "/z"]) {
        await opfs.writePage(path, 0, randomPageData(new Rng(42)));
        await opfs.writeMeta(path, { size: PAGE_SIZE, mode: 0o100644, ctime: 1000, mtime: 2000 });
      }

      // Manually delete metadata only (simulating crash mid-deleteAll)
      await opfs.deleteMeta("/x");
      await opfs.deleteMeta("/z");

      // Pages for /x and /z are now orphaned — no metadata
      const orphans = await opfs.cleanupOrphanedPages();
      expect(orphans).toBe(2);

      // /y should still be intact
      const files = await opfs.listFiles();
      expect(files).toEqual(["/y"]);
      const page = await opfs.readPage("/y", 0);
      expect(page).not.toBeNull();

      // Orphaned pages should be gone
      expect(await opfs.readPage("/x", 0)).toBeNull();
      expect(await opfs.readPage("/z", 0)).toBeNull();

      await opfs.destroy();
    });

    it("no-ops when all pages have metadata", async () => {
      const root = createFakeOpfsRoot();
      const opfs = new OpfsBackend({ root: root as any });

      for (const path of ["/a", "/b"]) {
        await opfs.writePage(path, 0, randomPageData(new Rng(99)));
        await opfs.writeMeta(path, { size: PAGE_SIZE, mode: 0o100644, ctime: 1000, mtime: 2000 });
      }

      const orphans = await opfs.cleanupOrphanedPages();
      expect(orphans).toBe(0);

      // All data intact
      expect((await opfs.listFiles()).sort()).toEqual(["/a", "/b"]);

      await opfs.destroy();
    });

    it("handles cleanup after fuzz sequence with deleteAll", async () => {
      const rng = new Rng(777);
      const root = createFakeOpfsRoot();
      const opfs = new OpfsBackend({ root: root as any });

      // Build up state
      for (let i = 0; i < 20; i++) {
        const path = rng.pick(FILE_PATHS);
        await opfs.writePage(path, rng.int(MAX_PAGE_INDEX), randomPageData(rng));
        await opfs.writeMeta(path, randomMeta(rng));
      }

      // deleteAll some paths
      await opfs.deleteAll(["/a", "/c", "/e"]);

      // After deleteAll, no orphans should exist
      const orphans = await opfs.cleanupOrphanedPages();
      expect(orphans).toBe(0);

      // Now simulate partial failure: write pages without metadata
      await opfs.writePage("/orphan1", 0, randomPageData(rng));
      await opfs.writePage("/orphan2", 0, randomPageData(rng));

      const orphans2 = await opfs.cleanupOrphanedPages();
      expect(orphans2).toBe(2);

      // Verify orphans are gone and legitimate files intact
      expect(await opfs.readPage("/orphan1", 0)).toBeNull();
      expect(await opfs.readPage("/orphan2", 0)).toBeNull();

      await opfs.destroy();
    });

    it("idempotent — second cleanup returns 0", async () => {
      const root = createFakeOpfsRoot();
      const opfs = new OpfsBackend({ root: root as any });

      await opfs.writePage("/stale", 0, randomPageData(new Rng(1)));
      // No metadata → orphan

      expect(await opfs.cleanupOrphanedPages()).toBe(1);
      expect(await opfs.cleanupOrphanedPages()).toBe(0);

      await opfs.destroy();
    });
  });
});
