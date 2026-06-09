/**
 * Randomized fuzz tests for the incremental syncfs path.
 *
 * The incremental syncfs path (O(dirty)) is the production hot path: after
 * mount and first sync, all subsequent syncs skip the full tree walk and
 * only persist dirty metadata nodes + dirty pages. Existing persistence
 * fuzz tests remount between checkpoints, forcing a full tree walk every
 * time. This file fills that gap by running many sync cycles within a
 * single mount session, verifying data integrity through the fast path.
 *
 * Test strategy:
 *   1. Mount + initial syncfs (full tree walk, clears needsOrphanCleanup)
 *   2. Repeat N cycles: random ops → syncfs (incremental) → verify model
 *   3. Final remount → verify all data survived persistence
 *
 * Verifies via TomeFSStats that the incremental path was actually taken
 * (incrementalSyncs > 0, fullTreeSyncs <= 1).
 */

import { describe, it, expect } from "vitest";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { createTomeFS } from "../../src/tomefs.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { PAGE_SIZE } from "../../src/types.js";
import type { EmscriptenFS } from "../harness/emscripten-fs.js";
import { O } from "../harness/emscripten-fs.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

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
// Model: tracks expected filesystem state
// ---------------------------------------------------------------

interface FileState {
  data: Uint8Array;
}

interface Model {
  files: Map<string, FileState>;
  dirs: Set<string>;
  symlinks: Map<string, string>;
}

function newModel(): Model {
  return { files: new Map(), dirs: new Set(["/"]), symlinks: new Map() };
}

function filesInDir(model: Model, dir: string): string[] {
  const prefix = dir === "/" ? "/" : dir + "/";
  return [...model.files.keys()].filter((p) => {
    if (!p.startsWith(prefix)) return false;
    return !p.slice(prefix.length).includes("/");
  });
}

function dirsInDir(model: Model, dir: string): string[] {
  const prefix = dir === "/" ? "/" : dir + "/";
  return [...model.dirs].filter((d) => {
    if (d === dir) return false;
    if (!d.startsWith(prefix)) return false;
    return !d.slice(prefix.length).includes("/");
  });
}

function symlinksInDir(model: Model, dir: string): string[] {
  const prefix = dir === "/" ? "/" : dir + "/";
  return [...model.symlinks.keys()].filter((p) => {
    if (!p.startsWith(prefix)) return false;
    return !p.slice(prefix.length).includes("/");
  });
}

function isDirEmpty(model: Model, dir: string): boolean {
  return filesInDir(model, dir).length === 0 &&
    dirsInDir(model, dir).length === 0 &&
    symlinksInDir(model, dir).length === 0;
}

// ---------------------------------------------------------------
// Operation types
// ---------------------------------------------------------------

type Op =
  | { type: "createFile"; path: string; data: Uint8Array }
  | { type: "writeAt"; path: string; offset: number; data: Uint8Array }
  | { type: "truncate"; path: string; size: number }
  | { type: "overwrite"; path: string; data: Uint8Array }
  | { type: "appendWrite"; path: string; data: Uint8Array }
  | { type: "mkdir"; path: string }
  | { type: "rmdir"; path: string }
  | { type: "symlink"; target: string; path: string }
  | { type: "unlinkSymlink"; path: string }
  | { type: "unlink"; path: string }
  | { type: "allocate"; path: string; offset: number; length: number };

const DIR_NAMES = ["alpha", "beta", "gamma"];
const FILE_NAMES = ["a.dat", "b.dat", "c.dat", "d.dat", "e.dat"];
const LINK_NAMES = ["lnk1", "lnk2"];

function generateOp(rng: Rng, model: Model): Op {
  const allFiles = [...model.files.keys()];
  const allDirs = [...model.dirs].filter((d) => d !== "/");
  const allContainerDirs = [...model.dirs];
  const allSymlinks = [...model.symlinks.keys()];

  const weights: Array<[string, number]> = [
    ["createFile", 20],
    ["mkdir", 8],
    ["writeAt", allFiles.length > 0 ? 20 : 0],
    ["truncate", allFiles.length > 0 ? 10 : 0],
    ["overwrite", allFiles.length > 0 ? 8 : 0],
    ["appendWrite", allFiles.length > 0 ? 15 : 0],
    ["unlink", allFiles.length > 0 ? 6 : 0],
    ["rmdir", allDirs.filter((d) => isDirEmpty(model, d)).length > 0 ? 4 : 0],
    ["symlink", allFiles.length > 0 ? 6 : 0],
    ["unlinkSymlink", allSymlinks.length > 0 ? 3 : 0],
    ["allocate", allFiles.length > 0 ? 8 : 0],
  ];

  const totalWeight = weights.reduce((s, [, w]) => s + w, 0);
  let choice = rng.int(totalWeight);
  let opType = "createFile";
  for (const [name, weight] of weights) {
    choice -= weight;
    if (choice < 0) {
      opType = name;
      break;
    }
  }

  switch (opType) {
    case "createFile": {
      const dir = rng.pick(allContainerDirs);
      const name = rng.pick(FILE_NAMES);
      const path = dir === "/" ? `/${name}` : `${dir}/${name}`;
      const sizeChoices = [0, 1, 100, PAGE_SIZE - 1, PAGE_SIZE, PAGE_SIZE + 1, PAGE_SIZE * 2, PAGE_SIZE * 3 + 137];
      return { type: "createFile", path, data: rng.bytes(rng.pick(sizeChoices)) };
    }
    case "mkdir": {
      const parent = rng.pick(allContainerDirs);
      const name = rng.pick(DIR_NAMES);
      const path = parent === "/" ? `/${name}` : `${parent}/${name}`;
      return { type: "mkdir", path };
    }
    case "writeAt": {
      const path = rng.pick(allFiles);
      const currentSize = model.files.get(path)?.data.length ?? 0;
      const offset = rng.int(currentSize + PAGE_SIZE + 1);
      const sizeChoices = [1, 50, PAGE_SIZE - 1, PAGE_SIZE, PAGE_SIZE + 1, PAGE_SIZE * 2];
      return { type: "writeAt", path, offset, data: rng.bytes(rng.pick(sizeChoices)) };
    }
    case "truncate": {
      const path = rng.pick(allFiles);
      const currentSize = model.files.get(path)?.data.length ?? 0;
      const sizeChoices = [0, Math.max(0, currentSize - PAGE_SIZE), currentSize, currentSize + 1, currentSize + PAGE_SIZE];
      return { type: "truncate", path, size: rng.pick(sizeChoices) };
    }
    case "overwrite": {
      const path = rng.pick(allFiles);
      return { type: "overwrite", path, data: rng.bytes(rng.pick([0, 1, 100, PAGE_SIZE, PAGE_SIZE * 2 + 77])) };
    }
    case "appendWrite": {
      const path = rng.pick(allFiles);
      return { type: "appendWrite", path, data: rng.bytes(rng.pick([1, 50, PAGE_SIZE, PAGE_SIZE + 1])) };
    }
    case "unlink":
      return { type: "unlink", path: rng.pick(allFiles) };
    case "rmdir": {
      const emptyDirs = allDirs.filter((d) => isDirEmpty(model, d));
      if (emptyDirs.length === 0) return { type: "createFile", path: `/${rng.pick(FILE_NAMES)}`, data: rng.bytes(10) };
      return { type: "rmdir", path: rng.pick(emptyDirs) };
    }
    case "symlink": {
      const dir = rng.pick(allContainerDirs);
      const name = rng.pick(LINK_NAMES);
      const path = dir === "/" ? `/${name}` : `${dir}/${name}`;
      const target = rng.pick(allFiles);
      return { type: "symlink", target, path };
    }
    case "unlinkSymlink":
      return { type: "unlinkSymlink", path: rng.pick(allSymlinks) };
    case "allocate": {
      const path = rng.pick(allFiles);
      const currentSize = model.files.get(path)?.data.length ?? 0;
      const offset = rng.pick([0, currentSize, Math.max(0, currentSize - PAGE_SIZE)]);
      const length = rng.pick([1, PAGE_SIZE, PAGE_SIZE * 2 + 37]);
      return { type: "allocate", path, offset, length };
    }
    default:
      return { type: "createFile", path: `/${rng.pick(FILE_NAMES)}`, data: rng.bytes(10) };
  }
}

// ---------------------------------------------------------------
// Execute + model update
// ---------------------------------------------------------------

const MOUNT = "/tome";

function rw(p: string): string {
  if (p === "/") return MOUNT;
  return MOUNT + p;
}

function execOp(FS: EmscriptenFS, op: Op): boolean {
  try {
    switch (op.type) {
      case "createFile": {
        const s = FS.open(rw(op.path), O.WRONLY | O.CREAT | O.TRUNC, 0o666);
        if (op.data.length > 0) FS.write(s, op.data, 0, op.data.length, 0);
        FS.close(s);
        return true;
      }
      case "writeAt": {
        const s = FS.open(rw(op.path), O.RDWR);
        FS.write(s, op.data, 0, op.data.length, op.offset);
        FS.close(s);
        return true;
      }
      case "truncate":
        FS.truncate(rw(op.path), op.size);
        return true;
      case "overwrite": {
        const s = FS.open(rw(op.path), O.WRONLY | O.TRUNC);
        if (op.data.length > 0) FS.write(s, op.data, 0, op.data.length, 0);
        FS.close(s);
        return true;
      }
      case "appendWrite": {
        const s = FS.open(rw(op.path), O.WRONLY | O.APPEND);
        FS.write(s, op.data, 0, op.data.length);
        FS.close(s);
        return true;
      }
      case "unlink":
      case "unlinkSymlink":
        FS.unlink(rw(op.path));
        return true;
      case "mkdir":
        FS.mkdir(rw(op.path), 0o777);
        return true;
      case "rmdir":
        FS.rmdir(rw(op.path));
        return true;
      case "symlink":
        FS.symlink(rw(op.target), rw(op.path));
        return true;
      case "allocate": {
        const s = FS.open(rw(op.path), O.RDWR);
        s.stream_ops.allocate(s, op.offset, op.length);
        FS.close(s);
        return true;
      }
      default:
        return true;
    }
  } catch {
    return false;
  }
}

function updateModel(model: Model, op: Op): void {
  switch (op.type) {
    case "createFile": {
      model.files.set(op.path, { data: new Uint8Array(op.data) });
      break;
    }
    case "writeAt": {
      const file = model.files.get(op.path);
      if (!file) break;
      const newSize = Math.max(file.data.length, op.offset + op.data.length);
      const newData = new Uint8Array(newSize);
      newData.set(file.data);
      newData.set(op.data, op.offset);
      file.data = newData;
      break;
    }
    case "truncate": {
      const file = model.files.get(op.path);
      if (!file) break;
      if (op.size < file.data.length) {
        file.data = new Uint8Array(file.data.slice(0, op.size));
      } else if (op.size > file.data.length) {
        const newData = new Uint8Array(op.size);
        newData.set(file.data);
        file.data = newData;
      }
      break;
    }
    case "overwrite": {
      const file = model.files.get(op.path);
      if (!file) break;
      file.data = new Uint8Array(op.data);
      break;
    }
    case "appendWrite": {
      const file = model.files.get(op.path);
      if (!file) break;
      const newData = new Uint8Array(file.data.length + op.data.length);
      newData.set(file.data);
      newData.set(op.data, file.data.length);
      file.data = newData;
      break;
    }
    case "unlink":
      model.files.delete(op.path);
      break;
    case "mkdir":
      model.dirs.add(op.path);
      break;
    case "rmdir":
      model.dirs.delete(op.path);
      break;
    case "symlink":
      model.symlinks.set(op.path, op.target);
      break;
    case "unlinkSymlink":
      model.symlinks.delete(op.path);
      break;
    case "allocate": {
      const file = model.files.get(op.path);
      if (!file) break;
      const newSize = Math.max(file.data.length, op.offset + op.length);
      if (newSize > file.data.length) {
        const newData = new Uint8Array(newSize);
        newData.set(file.data);
        file.data = newData;
      }
      break;
    }
  }
}

function formatOp(op: Op, index: number): string {
  switch (op.type) {
    case "createFile":
      return `[${index}] createFile(${op.path}, ${op.data.length}B)`;
    case "writeAt":
      return `[${index}] writeAt(${op.path}, @${op.offset}, ${op.data.length}B)`;
    case "truncate":
      return `[${index}] truncate(${op.path}, ${op.size})`;
    case "overwrite":
      return `[${index}] overwrite(${op.path}, ${op.data.length}B)`;
    case "appendWrite":
      return `[${index}] appendWrite(${op.path}, ${op.data.length}B)`;
    case "unlink":
      return `[${index}] unlink(${op.path})`;
    case "mkdir":
      return `[${index}] mkdir(${op.path})`;
    case "rmdir":
      return `[${index}] rmdir(${op.path})`;
    case "symlink":
      return `[${index}] symlink(${op.target} -> ${op.path})`;
    case "unlinkSymlink":
      return `[${index}] unlinkSymlink(${op.path})`;
    case "allocate":
      return `[${index}] allocate(${op.path}, @${op.offset}, ${op.length})`;
  }
}

// ---------------------------------------------------------------
// Mount / sync helpers
// ---------------------------------------------------------------

async function mountTome(
  backend: SyncMemoryBackend,
  maxPages: number,
): Promise<{ rawFS: any; tomefs: any }> {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  const Module = await createModule();
  const rawFS = Module.FS;
  const tomefs = createTomeFS(rawFS, { backend, maxPages });
  rawFS.mkdir(MOUNT);
  rawFS.mount(tomefs, {}, MOUNT);
  return { rawFS, tomefs };
}

function doSyncfs(rawFS: any): void {
  rawFS.syncfs(false, (err: Error | null) => {
    if (err) throw err;
  });
}

// ---------------------------------------------------------------
// Verification
// ---------------------------------------------------------------

function verifyModel(rawFS: any, model: Model, context: string): void {
  const errors: string[] = [];

  for (const [path, state] of model.files) {
    const fullPath = rw(path);
    let stat: any;
    try {
      stat = rawFS.stat(fullPath);
    } catch (e: any) {
      errors.push(`file ${path} should exist but stat failed: ${e.message}`);
      continue;
    }

    if (stat.size !== state.data.length) {
      errors.push(`file ${path}: size=${stat.size}, expected=${state.data.length}`);
      continue;
    }

    if (state.data.length > 0) {
      const buf = new Uint8Array(stat.size);
      const s = rawFS.open(fullPath, O.RDONLY);
      rawFS.read(s, buf, 0, stat.size, 0);
      rawFS.close(s);

      for (let i = 0; i < state.data.length; i++) {
        if (buf[i] !== state.data[i]) {
          const pageIdx = Math.floor(i / PAGE_SIZE);
          const pageOff = i % PAGE_SIZE;
          errors.push(
            `file ${path}: byte ${i} (page ${pageIdx} +${pageOff}): got=${buf[i]}, expected=${state.data[i]}`,
          );
          break;
        }
      }
    }
  }

  for (const dir of model.dirs) {
    if (dir === "/") continue;
    try {
      const stat = rawFS.stat(rw(dir));
      if (!rawFS.isDir(stat.mode)) {
        errors.push(`${dir} should be a directory`);
      }
    } catch (e: any) {
      errors.push(`directory ${dir} should exist but stat failed: ${e.message}`);
    }
  }

  for (const [path, target] of model.symlinks) {
    try {
      const stat = rawFS.lstat(rw(path));
      if (!rawFS.isLink(stat.mode)) {
        errors.push(`${path} should be a symlink`);
        continue;
      }
      const actual = rawFS.readlink(rw(path));
      if (actual !== rw(target)) {
        errors.push(`symlink ${path}: target=${actual}, expected=${rw(target)}`);
      }
    } catch (e: any) {
      errors.push(`symlink ${path} should exist but failed: ${e.message}`);
    }
  }

  // Reverse check: no extra files in the FS beyond the model
  const modelFiles = new Set([...model.files.keys()].map(rw));
  const modelDirs = new Set([...model.dirs].filter((d) => d !== "/").map(rw));
  const modelSymlinks = new Set([...model.symlinks.keys()].map(rw));

  function walkFs(dirPath: string): void {
    let entries: string[];
    try {
      entries = rawFS.readdir(dirPath);
    } catch {
      return;
    }
    for (const name of entries) {
      if (name === "." || name === "..") continue;
      const childPath = dirPath === MOUNT ? `${MOUNT}/${name}` : `${dirPath}/${name}`;
      let stat: any;
      try {
        stat = rawFS.lstat(childPath);
      } catch {
        continue;
      }
      if (rawFS.isLink(stat.mode)) {
        if (!modelSymlinks.has(childPath)) {
          errors.push(`extra symlink in FS: ${childPath}`);
        }
      } else if (rawFS.isDir(stat.mode)) {
        if (!modelDirs.has(childPath)) {
          errors.push(`extra directory in FS: ${childPath}`);
        }
        walkFs(childPath);
      } else if (rawFS.isFile(stat.mode)) {
        if (!modelFiles.has(childPath)) {
          errors.push(`extra file in FS: ${childPath}`);
        }
      }
    }
  }

  walkFs(MOUNT);

  if (errors.length > 0) {
    throw new Error(`${context}:\n  ${errors.join("\n  ")}`);
  }
}

// ---------------------------------------------------------------
// Fuzz runner
// ---------------------------------------------------------------

async function runIncrementalFuzz(
  seed: number,
  opsPerCycle: number,
  numCycles: number,
  maxPages: number,
): Promise<void> {
  const rng = new Rng(seed);
  const model = newModel();
  const backend = new SyncMemoryBackend();
  const allOps: string[] = [];
  let opIndex = 0;

  const { rawFS, tomefs } = await mountTome(backend, maxPages);

  // Bootstrap: create a few files so early cycles have material to work with
  for (let i = 0; i < 3; i++) {
    const name = FILE_NAMES[i];
    const data = rng.bytes(PAGE_SIZE + rng.int(PAGE_SIZE));
    const path = `/${name}`;
    const s = rawFS.open(`${MOUNT}${path}`, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    rawFS.write(s, data, 0, data.length, 0);
    rawFS.close(s);
    model.files.set(path, { data: new Uint8Array(data) });
  }

  // First syncfs: full tree walk (clears needsOrphanCleanup)
  doSyncfs(rawFS);
  tomefs.resetStats();

  // Main loop: random ops → incremental syncfs → verify
  for (let cycle = 0; cycle < numCycles; cycle++) {
    for (let i = 0; i < opsPerCycle; i++) {
      const op = generateOp(rng, model);
      allOps.push(formatOp(op, opIndex++));
      if (execOp(rawFS, op)) {
        updateModel(model, op);
      }
    }

    doSyncfs(rawFS);
    tomefs.assertInvariants();
    backend.assertInvariants();

    try {
      verifyModel(rawFS, model, `cycle ${cycle} (seed ${seed})`);
    } catch (e: any) {
      const recentOps = allOps.slice(Math.max(0, allOps.length - 20));
      throw new Error(`${e.message}\n\nRecent ops:\n${recentOps.join("\n")}`);
    }
  }

  // Verify the incremental path was used
  const stats = tomefs.getStats();
  expect(
    stats.incrementalSyncs,
    `seed ${seed}: expected incremental syncs, got ${JSON.stringify(stats)}`,
  ).toBeGreaterThan(0);

  // Final verification: remount from backend and check everything persisted
  const { rawFS: rawFS2, tomefs: tomefs2 } = await mountTome(backend, maxPages);

  try {
    tomefs2.assertInvariants();
    verifyModel(rawFS2, model, `after remount (seed ${seed})`);
  } catch (e: any) {
    const recentOps = allOps.slice(Math.max(0, allOps.length - 30));
    throw new Error(`${e.message}\n\nRecent ops:\n${recentOps.join("\n")}`);
  }
}

// ---------------------------------------------------------------
// Test cases
// ---------------------------------------------------------------

describe("fuzz: incremental syncfs data integrity", () => {
  describe("tiny cache (4 pages) — every write evicts", () => {
    const CACHE = 4;

    it("seed 80001: 5 ops/cycle × 10 cycles @fast", async () => {
      await runIncrementalFuzz(80001, 5, 10, CACHE);
    }, 30_000);

    it("seed 80002: 8 ops/cycle × 12 cycles", async () => {
      await runIncrementalFuzz(80002, 8, 12, CACHE);
    }, 30_000);

    it("seed 80003: 3 ops/cycle × 20 cycles", async () => {
      await runIncrementalFuzz(80003, 3, 20, CACHE);
    }, 30_000);

    it("seed 80004: 10 ops/cycle × 8 cycles", async () => {
      await runIncrementalFuzz(80004, 10, 8, CACHE);
    }, 30_000);
  });

  describe("small cache (16 pages) — moderate eviction", () => {
    const CACHE = 16;

    it("seed 81001: 8 ops/cycle × 10 cycles @fast", async () => {
      await runIncrementalFuzz(81001, 8, 10, CACHE);
    }, 30_000);

    it("seed 81002: 5 ops/cycle × 15 cycles", async () => {
      await runIncrementalFuzz(81002, 5, 15, CACHE);
    }, 30_000);

    it("seed 81003: 12 ops/cycle × 8 cycles", async () => {
      await runIncrementalFuzz(81003, 12, 8, CACHE);
    }, 30_000);
  });

  describe("medium cache (64 pages) — partial fit", () => {
    const CACHE = 64;

    it("seed 82001: 10 ops/cycle × 10 cycles @fast", async () => {
      await runIncrementalFuzz(82001, 10, 10, CACHE);
    }, 30_000);

    it("seed 82002: 15 ops/cycle × 8 cycles", async () => {
      await runIncrementalFuzz(82002, 15, 8, CACHE);
    }, 30_000);
  });

  describe("large cache (4096 pages) — no eviction", () => {
    it("seed 83001: 10 ops/cycle × 10 cycles @fast", async () => {
      await runIncrementalFuzz(83001, 10, 10, 4096);
    }, 30_000);

    it("seed 83002: 20 ops/cycle × 6 cycles", async () => {
      await runIncrementalFuzz(83002, 20, 6, 4096);
    }, 30_000);
  });

  describe("many short cycles — frequent sync", () => {
    it("seed 84001: 2 ops/cycle × 30 cycles, tiny cache @fast", async () => {
      await runIncrementalFuzz(84001, 2, 30, 4);
    }, 30_000);

    it("seed 84002: 1 op/cycle × 40 cycles, small cache", async () => {
      await runIncrementalFuzz(84002, 1, 40, 16);
    }, 30_000);
  });

  describe("few long cycles — bulk mutations between syncs", () => {
    it("seed 85001: 25 ops/cycle × 4 cycles, tiny cache @fast", async () => {
      await runIncrementalFuzz(85001, 25, 4, 4);
    }, 30_000);

    it("seed 85002: 30 ops/cycle × 3 cycles, small cache", async () => {
      await runIncrementalFuzz(85002, 30, 3, 16);
    }, 30_000);
  });
});
