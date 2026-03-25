/**
 * Randomized persistence fuzz tests for tomefs.
 *
 * Exercises syncfs → remount cycles within random operation sequences.
 * After a syncfs+remount, all file data and directory structure must
 * survive exactly. This targets the class of bugs found in PRs #31
 * (dir rename persistence) and #32 (syncfs crash safety).
 *
 * Each test generates a random sequence of file operations interleaved
 * with "checkpoint" operations that:
 *   1. syncfs() to persist all state
 *   2. Remount tomefs from the same backend
 *   3. Verify all file contents match expected state
 */

import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { createTomeFS } from "../../src/tomefs.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { PAGE_SIZE } from "../../src/types.js";
import type { EmscriptenFS, EmscriptenStream } from "../harness/emscripten-fs.js";
import { O } from "../harness/emscripten-fs.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

// ---------------------------------------------------------------
// Seeded PRNG (same as differential.test.ts)
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
// Expected state model
// ---------------------------------------------------------------

interface FileState {
  /** Full file content (ground truth). */
  content: Uint8Array;
}

interface Model {
  files: Map<string, FileState>;
  dirs: Set<string>;
}

function newModel(): Model {
  return { files: new Map(), dirs: new Set(["/"]) };
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
    if (d === dir || !d.startsWith(prefix)) return false;
    return !d.slice(prefix.length).includes("/");
  });
}

function isDirEmpty(model: Model, dir: string): boolean {
  return filesInDir(model, dir).length === 0 && dirsInDir(model, dir).length === 0;
}

// ---------------------------------------------------------------
// Operations
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
  | { type: "checkpoint" };

const DIR_NAMES = ["aa", "bb", "cc"];
const FILE_NAMES = ["x.dat", "y.dat", "z.dat", "w.dat"];

function generateOp(rng: Rng, model: Model): Op {
  const allFiles = [...model.files.keys()];
  const allDirs = [...model.dirs].filter((d) => d !== "/");
  const allContainerDirs = [...model.dirs];

  const weights: Array<[string, number]> = [
    ["createFile", 20],
    ["mkdir", 8],
    ["writeAt", allFiles.length > 0 ? 15 : 0],
    ["truncate", allFiles.length > 0 ? 8 : 0],
    ["renameFile", allFiles.length > 0 ? 10 : 0],
    ["unlink", allFiles.length > 0 ? 6 : 0],
    ["rmdir", allDirs.filter((d) => isDirEmpty(model, d)).length > 0 ? 4 : 0],
    ["renameDir", allDirs.length > 0 ? 6 : 0],
    ["checkpoint", 8], // ~8% chance of checkpoint
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
      const sizeChoices = [0, 10, PAGE_SIZE - 1, PAGE_SIZE, PAGE_SIZE + 1, PAGE_SIZE * 2 + 33];
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
      const currentSize = model.files.get(path)!.content.length;
      const maxOffset = currentSize + PAGE_SIZE;
      const offset = rng.int(maxOffset + 1);
      const size = rng.pick([1, 50, PAGE_SIZE, PAGE_SIZE + 1]);
      return { type: "writeAt", path, offset, data: rng.bytes(size) };
    }

    case "truncate": {
      const path = rng.pick(allFiles);
      const currentSize = model.files.get(path)!.content.length;
      const sizeChoices = [0, Math.max(0, currentSize - PAGE_SIZE), currentSize, currentSize + PAGE_SIZE];
      return { type: "truncate", path, size: rng.pick(sizeChoices) };
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
      if (emptyDirs.length === 0) return { type: "checkpoint" };
      return { type: "rmdir", path: rng.pick(emptyDirs) };
    }

    case "renameDir": {
      const oldPath = rng.pick(allDirs);
      const parent = rng.pick(allContainerDirs);
      const name = rng.pick(DIR_NAMES);
      const newPath = parent === "/" ? `/${name}` : `${parent}/${name}`;
      if (newPath.startsWith(oldPath + "/") || newPath === oldPath) {
        return { type: "checkpoint" };
      }
      return { type: "renameDir", oldPath, newPath };
    }

    case "checkpoint":
    default:
      return { type: "checkpoint" };
  }
}

function formatOp(op: Op, index: number): string {
  switch (op.type) {
    case "createFile": return `[${index}] create(${op.path}, ${op.data.length}B)`;
    case "writeAt": return `[${index}] write(${op.path}, @${op.offset}, ${op.data.length}B)`;
    case "truncate": return `[${index}] truncate(${op.path}, ${op.size})`;
    case "renameFile": return `[${index}] rename(${op.oldPath} -> ${op.newPath})`;
    case "renameDir": return `[${index}] renameDir(${op.oldPath} -> ${op.newPath})`;
    case "unlink": return `[${index}] unlink(${op.path})`;
    case "mkdir": return `[${index}] mkdir(${op.path})`;
    case "rmdir": return `[${index}] rmdir(${op.path})`;
    case "checkpoint": return `[${index}] CHECKPOINT`;
  }
}

// ---------------------------------------------------------------
// Model update
// ---------------------------------------------------------------

function applyToModel(model: Model, op: Op, success: boolean): void {
  if (!success) return;

  switch (op.type) {
    case "createFile": {
      model.files.set(op.path, { content: new Uint8Array(op.data) });
      break;
    }
    case "writeAt": {
      const state = model.files.get(op.path)!;
      const newSize = Math.max(state.content.length, op.offset + op.data.length);
      const newContent = new Uint8Array(newSize);
      newContent.set(state.content); // copy existing (zero-extends if growing)
      newContent.set(op.data, op.offset);
      state.content = newContent;
      break;
    }
    case "truncate": {
      const state = model.files.get(op.path)!;
      if (op.size < state.content.length) {
        state.content = state.content.slice(0, op.size);
      } else if (op.size > state.content.length) {
        const newContent = new Uint8Array(op.size);
        newContent.set(state.content);
        state.content = newContent;
      }
      break;
    }
    case "renameFile": {
      const state = model.files.get(op.oldPath)!;
      model.files.delete(op.oldPath);
      model.files.set(op.newPath, state);
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
    case "renameDir": {
      const oldPrefix = op.oldPath + "/";
      for (const [path, state] of [...model.files]) {
        if (path.startsWith(oldPrefix)) {
          model.files.delete(path);
          model.files.set(op.newPath + path.slice(op.oldPath.length), state);
        }
      }
      for (const dir of [...model.dirs]) {
        if (dir === op.oldPath || dir.startsWith(oldPrefix)) {
          model.dirs.delete(dir);
          model.dirs.add(op.newPath + dir.slice(op.oldPath.length));
        }
      }
      if (!model.dirs.has(op.newPath)) model.dirs.add(op.newPath);
      break;
    }
  }
}

// ---------------------------------------------------------------
// Persistence harness: tomefs with remountable backend
// ---------------------------------------------------------------

const TOME_MOUNT = "/tome";

interface PersistenceHarness {
  FS: EmscriptenFS;
  rawFS: any;
  backend: SyncMemoryBackend;
  tomefs: any;
  maxPages: number;
}

async function createPersistenceHarness(maxPages: number): Promise<PersistenceHarness> {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  const Module = await createModule();
  const rawFS = Module.FS;
  const backend = new SyncMemoryBackend();
  const tomefs = createTomeFS(rawFS, { backend, maxPages });
  rawFS.mkdir(TOME_MOUNT);
  rawFS.mount(tomefs, {}, TOME_MOUNT);

  const FS = createRewriter(rawFS);
  return { FS, rawFS, backend, tomefs, maxPages };
}

/** Remount: unmount tomefs, create new Emscripten module, mount from same backend. */
async function remount(h: PersistenceHarness): Promise<PersistenceHarness> {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  const Module = await createModule();
  const rawFS = Module.FS;
  const tomefs = createTomeFS(rawFS, { backend: h.backend, maxPages: h.maxPages });
  rawFS.mkdir(TOME_MOUNT);
  rawFS.mount(tomefs, {}, TOME_MOUNT);

  const FS = createRewriter(rawFS);
  return { FS, rawFS, backend: h.backend, tomefs, maxPages: h.maxPages };
}

function createRewriter(realFS: any): EmscriptenFS {
  function rw(p: string): string {
    if (!p.startsWith("/") || p.startsWith("/dev") || p.startsWith("/proc") || p.startsWith("/tmp")) return p;
    if (p.startsWith(TOME_MOUNT + "/") || p === TOME_MOUNT) return p;
    if (p === "/") return TOME_MOUNT;
    return TOME_MOUNT + p;
  }
  return {
    open(path, flags, mode?) { return realFS.open(rw(path), flags, mode); },
    close(stream) { return realFS.close(stream); },
    read(stream, buffer, offset, length, position?) { return realFS.read(stream, buffer, offset, length, position); },
    write(stream, buffer, offset, length, position?) { return realFS.write(stream, buffer, offset, length, position); },
    llseek(stream, offset, whence) { return realFS.llseek(stream, offset, whence); },
    stat(path) { return realFS.stat(rw(path)); },
    fstat(fd) { return realFS.fstat(fd); },
    lstat(path) { return realFS.lstat(rw(path)); },
    chmod(path, mode) { return realFS.chmod(rw(path), mode); },
    fchmod(fd, mode) { return realFS.fchmod(fd, mode); },
    utime(path, atime, mtime) { return realFS.utime(rw(path), atime, mtime); },
    truncate(path, len) { return realFS.truncate(rw(path), len); },
    ftruncate(fd, len) { return realFS.ftruncate(fd, len); },
    mkdir(path, mode?) { return realFS.mkdir(rw(path), mode); },
    rmdir(path) { return realFS.rmdir(rw(path)); },
    readdir(path) { return realFS.readdir(rw(path)); },
    unlink(path) { return realFS.unlink(rw(path)); },
    rename(oldPath, newPath) { return realFS.rename(rw(oldPath), rw(newPath)); },
    symlink(target, linkpath) { return realFS.symlink(target.startsWith("/") ? rw(target) : target, rw(linkpath)); },
    readlink(path) { return realFS.readlink(rw(path)); },
    writeFile(path, data, opts?) { return realFS.writeFile(rw(path), data, opts); },
    readFile(path, opts?) { return realFS.readFile(rw(path), opts); },
    isFile(mode) { return realFS.isFile(mode); },
    isDir(mode) { return realFS.isDir(mode); },
    isLink(mode) { return realFS.isLink(mode); },
    getStream(fd) { return realFS.getStream(fd); },
    dupStream(stream, fd?) { return realFS.dupStream(stream, fd); },
    mknod(path, mode, dev) { return realFS.mknod(rw(path), mode, dev); },
    cwd() { return realFS.cwd(); },
    chdir(path) { return realFS.chdir(rw(path)); },
    ErrnoError: realFS.ErrnoError,
  } as EmscriptenFS;
}

// ---------------------------------------------------------------
// Execute op on FS
// ---------------------------------------------------------------

function execOp(FS: EmscriptenFS, op: Op): boolean {
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
        FS.unlink(op.path);
        return true;
      case "mkdir":
        FS.mkdir(op.path, 0o777);
        return true;
      case "rmdir":
        FS.rmdir(op.path);
        return true;
      default:
        return true;
    }
  } catch {
    return false;
  }
}

// ---------------------------------------------------------------
// Verify all model files against FS
// ---------------------------------------------------------------

function verifyModel(FS: EmscriptenFS, model: Model, context: string): void {
  for (const [path, state] of model.files) {
    let stat: any;
    try {
      stat = FS.stat(path);
    } catch (e: any) {
      throw new Error(`${context}: file ${path} should exist but stat failed: ${e.message}`);
    }

    expect(stat.size, `${context}: size mismatch for ${path}`).toBe(state.content.length);

    if (state.content.length > 0) {
      const buf = new Uint8Array(stat.size);
      const s = FS.open(path, O.RDONLY);
      FS.read(s, buf, 0, stat.size, 0);
      FS.close(s);

      for (let i = 0; i < state.content.length; i++) {
        if (buf[i] !== state.content[i]) {
          const pageIdx = Math.floor(i / PAGE_SIZE);
          const pageOff = i % PAGE_SIZE;
          throw new Error(
            `${context}: content mismatch for ${path} at byte ${i} ` +
            `(page ${pageIdx}, offset ${pageOff}): ` +
            `expected=${state.content[i]}, got=${buf[i]}`,
          );
        }
      }
    }
  }

  // Verify directories exist
  for (const dir of model.dirs) {
    if (dir === "/") continue;
    try {
      const stat = FS.stat(dir);
      expect(FS.isDir(stat.mode), `${context}: ${dir} should be a directory`).toBe(true);
    } catch (e: any) {
      throw new Error(`${context}: directory ${dir} should exist but stat failed: ${e.message}`);
    }
  }
}

// ---------------------------------------------------------------
// Fuzz runner with persistence checkpoints
// ---------------------------------------------------------------

async function runPersistenceFuzz(
  seed: number,
  numOps: number,
  maxPages: number,
): Promise<void> {
  const rng = new Rng(seed);
  const model = newModel();
  let harness = await createPersistenceHarness(maxPages);

  const ops: Op[] = [];
  for (let i = 0; i < numOps; i++) {
    ops.push(generateOp(rng, model));
  }

  let checkpoints = 0;

  for (let i = 0; i < ops.length; i++) {
    const op = ops[i];

    if (op.type === "checkpoint") {
      // Persist and remount
      harness.rawFS.syncfs(false, (err: Error | null) => {
        if (err) throw err;
      });
      harness = await remount(harness);
      checkpoints++;

      // Verify all files survived the remount
      verifyModel(harness.FS, model, `checkpoint ${checkpoints} (after op ${i})`);
      continue;
    }

    const success = execOp(harness.FS, op);
    applyToModel(model, op, success);
  }

  // Final checkpoint: persist + remount + verify
  harness.rawFS.syncfs(false, (err: Error | null) => {
    if (err) throw err;
  });
  harness = await remount(harness);
  verifyModel(harness.FS, model, "final checkpoint");
}

// ---------------------------------------------------------------
// Test cases
// ---------------------------------------------------------------

describe("fuzz: persistence through syncfs + remount cycles", () => {
  describe("tiny cache (4 pages) — heavy eviction + persistence", () => {
    it("seed 10001 @fast", async () => {
      await runPersistenceFuzz(10001, 60, 4);
    }, 30_000);

    it("seed 20002", async () => {
      await runPersistenceFuzz(20002, 60, 4);
    }, 30_000);

    it("seed 30003", async () => {
      await runPersistenceFuzz(30003, 60, 4);
    }, 30_000);

    it("seed 40004", async () => {
      await runPersistenceFuzz(40004, 80, 4);
    }, 30_000);
  });

  describe("small cache (16 pages) — moderate eviction + persistence", () => {
    it("seed 50005 @fast", async () => {
      await runPersistenceFuzz(50005, 80, 16);
    }, 30_000);

    it("seed 60006", async () => {
      await runPersistenceFuzz(60006, 80, 16);
    }, 30_000);
  });

  describe("large cache (4096 pages) — baseline persistence", () => {
    it("seed 70007", async () => {
      await runPersistenceFuzz(70007, 80, 4096);
    }, 30_000);
  });

  describe("extended sequences — more checkpoints, more chances to fail", () => {
    it("150 ops, tiny cache, seed 99", async () => {
      await runPersistenceFuzz(99, 150, 4);
    }, 60_000);

    it("150 ops, small cache, seed 256", async () => {
      await runPersistenceFuzz(256, 150, 16);
    }, 60_000);
  });
});
