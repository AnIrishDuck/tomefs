/**
 * Continuous invariant checking fuzz tests for tomefs.
 *
 * The differential fuzz test checks assertInvariants() every 10 operations.
 * This test checks invariants after EVERY operation, catching transient
 * violations that self-heal before periodic checks catch them.
 *
 * Runs at extreme cache pressure (2 pages) to maximize eviction-related
 * invariant stress: every write evicts, every read after a write to a
 * different page evicts, and rename/truncate must handle evicted pages.
 *
 * Operations are weighted toward mutation-heavy patterns that modify
 * internal tracking state (dirtyMetaNodes, allFileNodes, filePages index):
 *   - rename (clears dirty flags, updates storagePaths)
 *   - unlink (removes from tracking sets)
 *   - truncate (invalidates pages, modifies dirty state)
 *   - write (marks dirty, triggers eviction)
 *   - syncfs (clears dirty state, transitions mode)
 *
 * Ethos §9: "Write tests designed to break tomefs specifically — target
 * the seams: metadata updates after flush, dirty flush ordering"
 */

import { describe, it } from "vitest";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { createTomeFS } from "../../src/tomefs.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { PAGE_SIZE } from "../../src/types.js";
import type { EmscriptenFS } from "../harness/emscripten-fs.js";
import { O } from "../harness/emscripten-fs.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

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

  int(max: number): number { return this.next() % max; }
  pick<T>(arr: T[]): T { return arr[this.int(arr.length)]; }
  bytes(length: number): Uint8Array {
    const buf = new Uint8Array(length);
    for (let i = 0; i < length; i++) buf[i] = this.next() & 0xff;
    return buf;
  }
}

// ---------------------------------------------------------------
// Model: minimal state tracking for valid operation generation
// ---------------------------------------------------------------

interface Model {
  files: Map<string, number>; // path → size
  dirs: Set<string>;
  symlinks: Map<string, string>;
  openFds: Map<number, { path: string; orphaned: boolean }>;
  nextFdId: number;
}

function newModel(): Model {
  return {
    files: new Map(),
    dirs: new Set(["/"]),
    symlinks: new Map(),
    openFds: new Map(),
    nextFdId: 0,
  };
}

function filesInDir(model: Model, dir: string): string[] {
  const prefix = dir === "/" ? "/" : dir + "/";
  return [...model.files.keys()].filter(
    (p) => p.startsWith(prefix) && !p.slice(prefix.length).includes("/"),
  );
}

function dirsInDir(model: Model, dir: string): string[] {
  const prefix = dir === "/" ? "/" : dir + "/";
  return [...model.dirs].filter(
    (d) => d !== dir && d.startsWith(prefix) && !d.slice(prefix.length).includes("/"),
  );
}

function symlinksInDir(model: Model, dir: string): string[] {
  const prefix = dir === "/" ? "/" : dir + "/";
  return [...model.symlinks.keys()].filter(
    (p) => p.startsWith(prefix) && !p.slice(prefix.length).includes("/"),
  );
}

function isDirEmpty(model: Model, dir: string): boolean {
  return filesInDir(model, dir).length === 0 &&
    dirsInDir(model, dir).length === 0 &&
    symlinksInDir(model, dir).length === 0;
}

// ---------------------------------------------------------------
// Operation types (mutation-heavy subset)
// ---------------------------------------------------------------

type Op =
  | { type: "createFile"; path: string; data: Uint8Array }
  | { type: "writeAt"; path: string; offset: number; data: Uint8Array }
  | { type: "truncate"; path: string; size: number }
  | { type: "renameFile"; oldPath: string; newPath: string }
  | { type: "unlink"; path: string }
  | { type: "mkdir"; path: string }
  | { type: "rmdir"; path: string }
  | { type: "renameDir"; oldPath: string; newPath: string }
  | { type: "symlink"; target: string; path: string }
  | { type: "unlinkSymlink"; path: string }
  | { type: "openFd"; path: string; fdId: number }
  | { type: "writeFd"; fdId: number; data: Uint8Array }
  | { type: "closeFd"; fdId: number }
  | { type: "allocate"; path: string; offset: number; length: number }
  | { type: "syncfs" };

const DIR_NAMES = ["aa", "bb", "cc"];
const FILE_NAMES = ["x.dat", "y.dat", "z.dat", "w.dat"];

function generateOp(rng: Rng, model: Model): Op {
  const allFiles = [...model.files.keys()];
  const allDirs = [...model.dirs].filter((d) => d !== "/");
  const allContainerDirs = [...model.dirs];
  const allSymlinks = [...model.symlinks.keys()];
  const activeFdIds = [...model.openFds.keys()].filter(
    (id) => !model.openFds.get(id)!.orphaned,
  );

  const weights: Array<[string, number]> = [
    ["createFile", 18],
    ["mkdir", 6],
    ["writeAt", allFiles.length > 0 ? 20 : 0],
    ["truncate", allFiles.length > 0 ? 12 : 0],
    ["renameFile", allFiles.length > 0 ? 14 : 0],
    ["unlink", allFiles.length > 0 ? 10 : 0],
    ["rmdir", allDirs.filter((d) => isDirEmpty(model, d)).length > 0 ? 4 : 0],
    ["renameDir", allDirs.length > 0 ? 10 : 0],
    ["symlink", allFiles.length > 0 ? 6 : 0],
    ["unlinkSymlink", allSymlinks.length > 0 ? 4 : 0],
    ["openFd", allFiles.length > 0 && model.openFds.size < 3 ? 8 : 0],
    ["writeFd", activeFdIds.length > 0 ? 12 : 0],
    ["closeFd", model.openFds.size > 0 ? 6 : 0],
    ["allocate", allFiles.length > 0 ? 8 : 0],
    ["syncfs", 6],
  ];

  const totalWeight = weights.reduce((s, [, w]) => s + w, 0);
  let choice = rng.int(totalWeight);
  let opType = "createFile";
  for (const [name, weight] of weights) {
    choice -= weight;
    if (choice < 0) { opType = name; break; }
  }

  switch (opType) {
    case "createFile": {
      const dir = rng.pick(allContainerDirs);
      const name = rng.pick(FILE_NAMES);
      const path = dir === "/" ? `/${name}` : `${dir}/${name}`;
      const sizes = [0, 10, PAGE_SIZE - 1, PAGE_SIZE, PAGE_SIZE + 1, PAGE_SIZE * 2 + 33];
      return { type: "createFile", path, data: rng.bytes(rng.pick(sizes)) };
    }
    case "mkdir": {
      const parent = rng.pick(allContainerDirs);
      const name = rng.pick(DIR_NAMES);
      const path = parent === "/" ? `/${name}` : `${parent}/${name}`;
      return { type: "mkdir", path };
    }
    case "writeAt": {
      const path = rng.pick(allFiles);
      const currentSize = model.files.get(path) ?? 0;
      const offset = rng.int(currentSize + PAGE_SIZE + 1);
      const sizes = [1, 50, PAGE_SIZE, PAGE_SIZE + 1];
      return { type: "writeAt", path, offset, data: rng.bytes(rng.pick(sizes)) };
    }
    case "truncate": {
      const path = rng.pick(allFiles);
      const currentSize = model.files.get(path) ?? 0;
      const choices = [0, Math.max(0, currentSize - PAGE_SIZE), currentSize, currentSize + PAGE_SIZE];
      return { type: "truncate", path, size: rng.pick(choices) };
    }
    case "renameFile": {
      const oldPath = rng.pick(allFiles);
      const dir = rng.pick(allContainerDirs);
      const name = rng.pick(FILE_NAMES);
      const newPath = dir === "/" ? `/${name}` : `${dir}/${name}`;
      return { type: "renameFile", oldPath, newPath };
    }
    case "unlink":
      return { type: "unlink", path: rng.pick(allFiles) };
    case "rmdir": {
      const emptyDirs = allDirs.filter((d) => isDirEmpty(model, d));
      if (emptyDirs.length === 0) return { type: "syncfs" };
      return { type: "rmdir", path: rng.pick(emptyDirs) };
    }
    case "renameDir": {
      const oldPath = rng.pick(allDirs);
      const parent = rng.pick(allContainerDirs);
      const name = rng.pick(DIR_NAMES);
      const newPath = parent === "/" ? `/${name}` : `${parent}/${name}`;
      if (newPath.startsWith(oldPath + "/") || newPath === oldPath) {
        return { type: "syncfs" };
      }
      return { type: "renameDir", oldPath, newPath };
    }
    case "symlink": {
      const dir = rng.pick(allContainerDirs);
      const target = rng.pick(allFiles);
      const name = `lnk${rng.int(3)}`;
      const path = dir === "/" ? `/${name}` : `${dir}/${name}`;
      return { type: "symlink", target, path };
    }
    case "unlinkSymlink":
      return { type: "unlinkSymlink", path: rng.pick(allSymlinks) };
    case "openFd": {
      const path = rng.pick(allFiles);
      const fdId = model.nextFdId;
      return { type: "openFd", path, fdId };
    }
    case "writeFd": {
      const fdId = rng.pick(activeFdIds);
      const sizes = [1, 50, PAGE_SIZE, PAGE_SIZE + 1];
      return { type: "writeFd", fdId, data: rng.bytes(rng.pick(sizes)) };
    }
    case "closeFd": {
      const fdId = rng.pick([...model.openFds.keys()]);
      return { type: "closeFd", fdId };
    }
    case "allocate": {
      const path = rng.pick(allFiles);
      const currentSize = model.files.get(path) ?? 0;
      const offsets = [0, currentSize, currentSize + PAGE_SIZE];
      const lengths = [1, PAGE_SIZE, PAGE_SIZE * 2];
      return { type: "allocate", path, offset: rng.pick(offsets), length: rng.pick(lengths) };
    }
    default:
      return { type: "syncfs" };
  }
}

// ---------------------------------------------------------------
// Apply operation to model (track expected state)
// ---------------------------------------------------------------

function applyToModel(op: Op, model: Model): void {
  switch (op.type) {
    case "createFile":
      model.files.set(op.path, op.data.length);
      break;
    case "writeAt": {
      const cur = model.files.get(op.path) ?? 0;
      model.files.set(op.path, Math.max(cur, op.offset + op.data.length));
      break;
    }
    case "truncate":
      model.files.set(op.path, op.size);
      break;
    case "renameFile": {
      const size = model.files.get(op.oldPath);
      if (size !== undefined) {
        model.files.delete(op.oldPath);
        model.files.set(op.newPath, size);
        for (const fd of model.openFds.values()) {
          if (fd.path === op.oldPath) fd.path = op.newPath;
        }
      }
      break;
    }
    case "unlink": {
      model.files.delete(op.path);
      for (const fd of model.openFds.values()) {
        if (fd.path === op.path) fd.orphaned = true;
      }
      break;
    }
    case "mkdir":
      model.dirs.add(op.path);
      break;
    case "rmdir":
      model.dirs.delete(op.path);
      break;
    case "renameDir": {
      const prefix = op.oldPath + "/";
      const toMove: string[] = [];
      for (const p of model.files.keys()) {
        if (p.startsWith(prefix)) toMove.push(p);
      }
      for (const p of toMove) {
        const size = model.files.get(p)!;
        model.files.delete(p);
        model.files.set(op.newPath + p.substring(op.oldPath.length), size);
      }
      const dirsToMove = [...model.dirs].filter(
        (d) => d === op.oldPath || d.startsWith(prefix),
      );
      for (const d of dirsToMove) {
        model.dirs.delete(d);
        model.dirs.add(op.newPath + d.substring(op.oldPath.length));
      }
      const linksToMove = [...model.symlinks.keys()].filter(
        (p) => p.startsWith(prefix),
      );
      for (const p of linksToMove) {
        const target = model.symlinks.get(p)!;
        model.symlinks.delete(p);
        model.symlinks.set(op.newPath + p.substring(op.oldPath.length), target);
      }
      for (const fd of model.openFds.values()) {
        if (fd.path.startsWith(prefix) || fd.path === op.oldPath) {
          fd.path = op.newPath + fd.path.substring(op.oldPath.length);
        }
      }
      break;
    }
    case "symlink":
      model.symlinks.set(op.path, op.target);
      break;
    case "unlinkSymlink":
      model.symlinks.delete(op.path);
      break;
    case "openFd":
      model.openFds.set(op.fdId, { path: op.path, orphaned: false });
      model.nextFdId++;
      break;
    case "writeFd": {
      const fd = model.openFds.get(op.fdId);
      if (fd && !fd.orphaned) {
        const cur = model.files.get(fd.path) ?? 0;
        model.files.set(fd.path, Math.max(cur, op.data.length));
      }
      break;
    }
    case "closeFd":
      model.openFds.delete(op.fdId);
      break;
    case "allocate": {
      const cur = model.files.get(op.path) ?? 0;
      model.files.set(op.path, Math.max(cur, op.offset + op.length));
      break;
    }
    case "syncfs":
      break;
  }
}

// ---------------------------------------------------------------
// Execute operation on tomefs
// ---------------------------------------------------------------

function execOp(
  FS: EmscriptenFS,
  op: Op,
  syncfsFn: () => void,
  fdStreams: Map<number, any>,
): boolean {
  try {
    switch (op.type) {
      case "createFile": {
        const s = FS.open(op.path, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
        if (op.data.length > 0) FS.write(s, op.data, 0, op.data.length, 0);
        FS.close(s);
        return true;
      }
      case "writeAt": {
        const s = FS.open(op.path, O.RDWR);
        FS.write(s, op.data, 0, op.data.length, op.offset);
        FS.close(s);
        return true;
      }
      case "truncate":
        FS.truncate(op.path, op.size);
        return true;
      case "renameFile":
      case "renameDir":
        FS.rename(op.oldPath, op.newPath);
        return true;
      case "unlink":
      case "unlinkSymlink":
        FS.unlink(op.path);
        return true;
      case "mkdir":
        FS.mkdir(op.path, 0o777);
        return true;
      case "rmdir":
        FS.rmdir(op.path);
        return true;
      case "symlink":
        FS.symlink(op.target, op.path);
        return true;
      case "openFd": {
        const stream = FS.open(op.path, O.RDWR);
        fdStreams.set(op.fdId, stream);
        return true;
      }
      case "writeFd": {
        const stream = fdStreams.get(op.fdId);
        if (!stream) return false;
        FS.write(stream, op.data, 0, op.data.length);
        return true;
      }
      case "closeFd": {
        const stream = fdStreams.get(op.fdId);
        if (!stream) return false;
        FS.close(stream);
        fdStreams.delete(op.fdId);
        return true;
      }
      case "allocate": {
        const s = FS.open(op.path, O.RDWR);
        (FS as any).allocate(s, op.offset, op.length);
        FS.close(s);
        return true;
      }
      case "syncfs":
        syncfsFn();
        return true;
    }
  } catch (_e) {
    return false;
  }
}

// ---------------------------------------------------------------
// Main fuzz driver
// ---------------------------------------------------------------

const MOUNT = "/tome";

async function runContinuousInvariantCheck(
  seed: number,
  numOps: number,
  maxPages: number,
): Promise<void> {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  const Module = await createModule();
  const FS = Module.FS as EmscriptenFS;

  const backend = new SyncMemoryBackend();
  const tomefs = createTomeFS(FS, { backend, maxPages });
  FS.mkdir(MOUNT);
  FS.mount(tomefs, {}, MOUNT);

  const rng = new Rng(seed);
  const model = newModel();
  const fdStreams = new Map<number, any>();

  const syncfsFn = () => {
    tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
      if (err) throw err;
    });
  };

  // Initial syncfs to transition from full-tree-walk to incremental
  syncfsFn();
  tomefs.assertInvariants();
  backend.assertInvariants();

  for (let i = 0; i < numOps; i++) {
    const op = generateOp(rng, model);

    // Prefix paths with mount point for FS operations
    const fsOp = prefixOp(op);
    const succeeded = execOp(FS, fsOp, syncfsFn, fdStreams);

    if (succeeded) {
      applyToModel(op, model);
    }

    // The core assertion: invariants must hold after EVERY operation
    try {
      tomefs.assertInvariants();
    } catch (e) {
      throw new Error(
        `Invariant violation after op ${i} (seed=${seed}): ${op.type}\n` +
        `Op details: ${JSON.stringify(op, (_, v) => v instanceof Uint8Array ? `Uint8Array(${v.length})` : v)}\n` +
        `Error: ${(e as Error).message}`,
      );
    }

    try {
      backend.assertInvariants();
    } catch (e) {
      throw new Error(
        `Backend invariant violation after op ${i} (seed=${seed}): ${op.type}\n` +
        `Op details: ${JSON.stringify(op, (_, v) => v instanceof Uint8Array ? `Uint8Array(${v.length})` : v)}\n` +
        `Error: ${(e as Error).message}`,
      );
    }
  }

  // Final: close any remaining fds
  for (const stream of fdStreams.values()) {
    try { FS.close(stream); } catch (_e) { /* already closed */ }
  }

  // Verify syncfs + invariants one last time
  syncfsFn();
  tomefs.assertInvariants();
  backend.assertInvariants();
}

function prefixOp(op: Op): Op {
  switch (op.type) {
    case "createFile":
      return { ...op, path: MOUNT + op.path };
    case "writeAt":
      return { ...op, path: MOUNT + op.path };
    case "truncate":
      return { ...op, path: MOUNT + op.path };
    case "renameFile":
      return { ...op, oldPath: MOUNT + op.oldPath, newPath: MOUNT + op.newPath };
    case "unlink":
      return { ...op, path: MOUNT + op.path };
    case "mkdir":
      return { ...op, path: MOUNT + op.path };
    case "rmdir":
      return { ...op, path: MOUNT + op.path };
    case "renameDir":
      return { ...op, oldPath: MOUNT + op.oldPath, newPath: MOUNT + op.newPath };
    case "symlink":
      return { ...op, target: op.target, path: MOUNT + op.path };
    case "unlinkSymlink":
      return { ...op, path: MOUNT + op.path };
    case "openFd":
      return { ...op, path: MOUNT + op.path };
    case "allocate":
      return { ...op, path: MOUNT + op.path };
    case "writeFd":
    case "closeFd":
    case "syncfs":
      return op;
  }
}

// ---------------------------------------------------------------
// Test cases
// ---------------------------------------------------------------

describe("fuzz: continuous invariant checking (2-page cache)", () => {
  const CACHE = 2;
  const OPS = 60;

  it("seed 10001 @fast", async () => {
    await runContinuousInvariantCheck(10001, OPS, CACHE);
  }, 30_000);

  it("seed 20002", async () => {
    await runContinuousInvariantCheck(20002, OPS, CACHE);
  }, 30_000);

  it("seed 30003", async () => {
    await runContinuousInvariantCheck(30003, OPS, CACHE);
  }, 30_000);

  it("seed 40004", async () => {
    await runContinuousInvariantCheck(40004, OPS, CACHE);
  }, 30_000);

  it("seed 50005", async () => {
    await runContinuousInvariantCheck(50005, OPS, CACHE);
  }, 30_000);

  it("seed 60006", async () => {
    await runContinuousInvariantCheck(60006, OPS, CACHE);
  }, 30_000);

  it("seed 70007", async () => {
    await runContinuousInvariantCheck(70007, OPS, CACHE);
  }, 30_000);

  it("seed 80008", async () => {
    await runContinuousInvariantCheck(80008, OPS, CACHE);
  }, 30_000);
});

describe("fuzz: continuous invariant checking (4-page cache)", () => {
  const CACHE = 4;
  const OPS = 80;

  it("seed 11111 @fast", async () => {
    await runContinuousInvariantCheck(11111, OPS, CACHE);
  }, 30_000);

  it("seed 22222", async () => {
    await runContinuousInvariantCheck(22222, OPS, CACHE);
  }, 30_000);

  it("seed 33333", async () => {
    await runContinuousInvariantCheck(33333, OPS, CACHE);
  }, 30_000);

  it("seed 44444", async () => {
    await runContinuousInvariantCheck(44444, OPS, CACHE);
  }, 30_000);

  it("seed 55555", async () => {
    await runContinuousInvariantCheck(55555, OPS, CACHE);
  }, 30_000);
});

describe("fuzz: continuous invariant checking (extended sequences)", () => {
  it("150 ops, 2-page cache, seed 99999", async () => {
    await runContinuousInvariantCheck(99999, 150, 2);
  }, 60_000);

  it("200 ops, 3-page cache, seed 12345", async () => {
    await runContinuousInvariantCheck(12345, 200, 3);
  }, 60_000);

  it("100 ops, 1-page cache, seed 77777", async () => {
    await runContinuousInvariantCheck(77777, 100, 1);
  }, 60_000);
});
