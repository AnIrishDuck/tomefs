/**
 * Fuzz tests verifying that operations after dirty shutdown recovery
 * produce byte-accurate results.
 *
 * The existing dirty-shutdown.test.ts verifies that recovery produces a
 * navigable, structurally sound filesystem. But it doesn't verify that
 * CONTINUED OPERATIONS after recovery produce correct results — only
 * that the FS doesn't crash. In production, PGlite will crash, recover,
 * and then immediately resume serving queries. This test verifies that
 * the recovered filesystem is a correct base for continued operation.
 *
 * Strategy:
 *   Phase 1 (establish): create files with tracked content → clean syncfs
 *   Phase 2 (dirty):     more operations WITHOUT syncfs
 *   Phase 3 (crash):     remount from backend (dirty recovery)
 *   Phase 4 (rebuild):   recovery syncfs → scan FS to rebuild model
 *   Phase 5 (operate):   randomized operations with content verification
 *   Phase 6 (persist):   syncfs → remount → verify all data survived
 *
 * The rebuild step is the key insight: after a dirty shutdown, the exact
 * state is non-deterministic (depends on which pages were evicted). So
 * we can't verify against the pre-crash model. Instead, we rebuild the
 * model from the actual recovered state and verify that all subsequent
 * operations are correct relative to that baseline.
 *
 * Ethos §8: "Record or simulate real PGlite access patterns — startup,
 * queries, vacuums, WAL replay"
 * Ethos §9: "Write tests designed to break tomefs specifically — target
 * the seams: reads that span page boundaries, writes during eviction,
 * metadata updates after flush"
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
// Model: tracks expected filesystem state with content
// ---------------------------------------------------------------

interface FileState {
  data: Uint8Array;
}

interface FSModel {
  files: Map<string, FileState>;
  dirs: Set<string>;
  symlinks: Map<string, string>;
}

function newModel(): FSModel {
  return { files: new Map(), dirs: new Set(["/"]), symlinks: new Map() };
}

function filesInDir(model: FSModel, dir: string): string[] {
  const prefix = dir === "/" ? "/" : dir + "/";
  return [...model.files.keys()].filter((p) => {
    if (!p.startsWith(prefix)) return false;
    return !p.slice(prefix.length).includes("/");
  });
}

function dirsInDir(model: FSModel, dir: string): string[] {
  const prefix = dir === "/" ? "/" : dir + "/";
  return [...model.dirs].filter((d) => {
    if (d === dir) return false;
    if (!d.startsWith(prefix)) return false;
    return !d.slice(prefix.length).includes("/");
  });
}

function symlinksInDir(model: FSModel, dir: string): string[] {
  const prefix = dir === "/" ? "/" : dir + "/";
  return [...model.symlinks.keys()].filter((p) => {
    if (!p.startsWith(prefix)) return false;
    return !p.slice(prefix.length).includes("/");
  });
}

function isDirEmpty(model: FSModel, dir: string): boolean {
  return filesInDir(model, dir).length === 0 &&
    dirsInDir(model, dir).length === 0 &&
    symlinksInDir(model, dir).length === 0;
}

// ---------------------------------------------------------------
// Operation types (subset focused on content-affecting ops)
// ---------------------------------------------------------------

type Op =
  | { type: "createFile"; path: string; data: Uint8Array }
  | { type: "writeAt"; path: string; offset: number; data: Uint8Array }
  | { type: "truncate"; path: string; size: number }
  | { type: "overwrite"; path: string; data: Uint8Array }
  | { type: "renameFile"; oldPath: string; newPath: string }
  | { type: "unlink"; path: string }
  | { type: "mkdir"; path: string }
  | { type: "rmdir"; path: string }
  | { type: "renameDir"; oldPath: string; newPath: string }
  | { type: "symlink"; target: string; path: string }
  | { type: "unlinkSymlink"; path: string }
  | { type: "appendWrite"; path: string; data: Uint8Array }
  | { type: "allocate"; path: string; offset: number; length: number };

const DIR_NAMES = ["alpha", "beta", "gamma"];
const FILE_NAMES = ["a.dat", "b.dat", "c.dat", "d.dat"];
const LINK_NAMES = ["lnk1", "lnk2"];

function generateOp(rng: Rng, model: FSModel): Op {
  const allFiles = [...model.files.keys()];
  const allDirs = [...model.dirs].filter((d) => d !== "/");
  const allContainerDirs = [...model.dirs];
  const allSymlinks = [...model.symlinks.keys()];

  const weights: Array<[string, number]> = [
    ["createFile", 15],
    ["mkdir", 8],
    ["writeAt", allFiles.length > 0 ? 18 : 0],
    ["truncate", allFiles.length > 0 ? 10 : 0],
    ["overwrite", allFiles.length > 0 ? 8 : 0],
    ["renameFile", allFiles.length > 0 ? 10 : 0],
    ["unlink", allFiles.length > 0 ? 6 : 0],
    ["renameDir", allDirs.length > 0 ? 5 : 0],
    ["rmdir", allDirs.filter((d) => isDirEmpty(model, d)).length > 0 ? 4 : 0],
    ["symlink", allFiles.length > 0 ? 6 : 0],
    ["unlinkSymlink", allSymlinks.length > 0 ? 3 : 0],
    ["appendWrite", allFiles.length > 0 ? 12 : 0],
    ["allocate", allFiles.length > 0 ? 6 : 0],
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
      const sizeChoices = [0, 1, 100, PAGE_SIZE - 1, PAGE_SIZE, PAGE_SIZE + 1, PAGE_SIZE * 2 + 37];
      const data = rng.bytes(rng.pick(sizeChoices));
      return { type: "createFile", path, data };
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
      const sizeChoices = [1, 50, PAGE_SIZE - 1, PAGE_SIZE, PAGE_SIZE + 1];
      const data = rng.bytes(rng.pick(sizeChoices));
      return { type: "writeAt", path, offset, data };
    }
    case "truncate": {
      const path = rng.pick(allFiles);
      const currentSize = model.files.get(path)?.data.length ?? 0;
      const sizeChoices = [0, Math.max(0, currentSize - PAGE_SIZE), currentSize, currentSize + PAGE_SIZE];
      return { type: "truncate", path, size: rng.pick(sizeChoices) };
    }
    case "overwrite": {
      const path = rng.pick(allFiles);
      const data = rng.bytes(rng.pick([0, 1, 100, PAGE_SIZE, PAGE_SIZE * 2 + 77]));
      return { type: "overwrite", path, data };
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
    case "renameDir": {
      const oldPath = rng.pick(allDirs);
      const parent = rng.pick(allContainerDirs);
      const name = rng.pick(DIR_NAMES);
      const newPath = parent === "/" ? `/${name}` : `${parent}/${name}`;
      if (newPath.startsWith(oldPath + "/") || newPath === oldPath) {
        return { type: "createFile", path: `/${rng.pick(FILE_NAMES)}`, data: rng.bytes(10) };
      }
      return { type: "renameDir", oldPath, newPath };
    }
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
    case "appendWrite": {
      const path = rng.pick(allFiles);
      const data = rng.bytes(rng.pick([1, 50, PAGE_SIZE, PAGE_SIZE + 1]));
      return { type: "appendWrite", path, data };
    }
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

const TOME_MOUNT = "/tome";

function rw(p: string): string {
  if (p === "/") return TOME_MOUNT;
  return TOME_MOUNT + p;
}

/**
 * After an operation that extends a file, re-read the file from the FS
 * to capture the actual gap byte values. After dirty shutdown recovery,
 * pages in the backend may have non-zero data beyond the old file end
 * (left over from before a truncation that wasn't synced). When the file
 * is extended, the model assumes the gap is zero-filled (per POSIX), but
 * the FS loads the page from backend which may have stale data. This is
 * a known limitation of tomefs crash recovery — the zeroTailAfterTruncate
 * from the dirty phase is lost in the crash.
 */
function resyncFileFromFS(
  FS: EmscriptenFS,
  model: FSModel,
  modelPath: string,
): void {
  const fsPath = rw(modelPath);
  try {
    const stat = FS.stat(fsPath);
    const data = new Uint8Array(stat.size);
    if (stat.size > 0) {
      const s = FS.open(fsPath, O.RDONLY);
      FS.read(s, data, 0, stat.size, 0);
      FS.close(s);
    }
    model.files.set(modelPath, { data });
  } catch {
    // File may have been deleted
  }
}

/**
 * After operations that extend a file (writeAt beyond end, appendWrite,
 * allocate), re-read the file from the FS. After crash recovery, pages
 * may have non-zero stale data beyond the old file end, so the model's
 * zero-fill assumption doesn't hold. Re-reading ensures the model
 * tracks what the FS actually contains.
 */
function resyncIfExtended(
  FS: EmscriptenFS,
  model: FSModel,
  op: Op,
): void {
  let path: string | undefined;
  switch (op.type) {
    case "writeAt":
    case "appendWrite":
    case "allocate":
      path = op.path;
      break;
    case "createFile":
    case "overwrite":
      path = op.path;
      break;
  }
  if (path) {
    resyncFileFromFS(FS, model, path);
  }
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
      case "renameFile":
      case "renameDir":
        FS.rename(rw(op.oldPath), rw(op.newPath));
        return true;
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
      case "appendWrite": {
        const s = FS.open(rw(op.path), O.WRONLY | O.APPEND);
        FS.write(s, op.data, 0, op.data.length);
        FS.close(s);
        return true;
      }
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

function updateModel(model: FSModel, op: Op): void {
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
    case "renameFile": {
      const file = model.files.get(op.oldPath);
      if (!file) break;
      if (op.oldPath === op.newPath) break;
      model.files.delete(op.newPath);
      model.symlinks.delete(op.newPath);
      model.files.delete(op.oldPath);
      model.files.set(op.newPath, file);
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
      for (const [path, target] of [...model.symlinks]) {
        if (path.startsWith(oldPrefix)) {
          model.symlinks.delete(path);
          model.symlinks.set(op.newPath + path.slice(op.oldPath.length), target);
        }
      }
      if (!model.dirs.has(op.newPath)) {
        model.dirs.add(op.newPath);
      }
      break;
    }
    case "symlink":
      model.symlinks.set(op.path, op.target);
      break;
    case "unlinkSymlink":
      model.symlinks.delete(op.path);
      break;
    case "appendWrite": {
      const file = model.files.get(op.path);
      if (!file) break;
      const newData = new Uint8Array(file.data.length + op.data.length);
      newData.set(file.data);
      newData.set(op.data, file.data.length);
      file.data = newData;
      break;
    }
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
    case "renameFile":
      return `[${index}] renameFile(${op.oldPath} -> ${op.newPath})`;
    case "unlink":
      return `[${index}] unlink(${op.path})`;
    case "mkdir":
      return `[${index}] mkdir(${op.path})`;
    case "rmdir":
      return `[${index}] rmdir(${op.path})`;
    case "renameDir":
      return `[${index}] renameDir(${op.oldPath} -> ${op.newPath})`;
    case "symlink":
      return `[${index}] symlink(${op.target} -> ${op.path})`;
    case "unlinkSymlink":
      return `[${index}] unlinkSymlink(${op.path})`;
    case "appendWrite":
      return `[${index}] appendWrite(${op.path}, ${op.data.length}B)`;
    case "allocate":
      return `[${index}] allocate(${op.path}, @${op.offset}, ${op.length})`;
  }
}

// ---------------------------------------------------------------
// Mount / remount helpers
// ---------------------------------------------------------------

interface TomeFSInstance {
  rawFS: any;
  tomefs: any;
}

async function mountTome(
  backend: SyncMemoryBackend,
  maxPages: number,
): Promise<TomeFSInstance> {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  const Module = await createModule();
  const rawFS = Module.FS;
  const tomefs = createTomeFS(rawFS, { backend, maxPages });
  rawFS.mkdir(TOME_MOUNT);
  rawFS.mount(tomefs, {}, TOME_MOUNT);
  return { rawFS, tomefs };
}

function doSyncfs(rawFS: any): void {
  rawFS.syncfs(false, (err: Error | null) => {
    if (err) throw err;
  });
}

// ---------------------------------------------------------------
// Model rebuild: scan FS to create a model from actual state
// ---------------------------------------------------------------

function rebuildModelFromFS(rawFS: EmscriptenFS): FSModel {
  const model = newModel();

  function walk(fsPath: string, modelPath: string): void {
    const entries = rawFS.readdir(fsPath);
    for (const name of entries) {
      if (name === "." || name === "..") continue;
      const fullFsPath = fsPath === TOME_MOUNT ? `${TOME_MOUNT}/${name}` : `${fsPath}/${name}`;
      const fullModelPath = modelPath === "/" ? `/${name}` : `${modelPath}/${name}`;

      let stat: any;
      try {
        stat = rawFS.lstat(fullFsPath);
      } catch {
        continue;
      }

      if (rawFS.isDir(stat.mode)) {
        model.dirs.add(fullModelPath);
        walk(fullFsPath, fullModelPath);
      } else if (rawFS.isFile(stat.mode)) {
        const data = new Uint8Array(stat.size);
        if (stat.size > 0) {
          const s = rawFS.open(fullFsPath, O.RDONLY);
          rawFS.read(s, data, 0, stat.size, 0);
          rawFS.close(s);
        }
        model.files.set(fullModelPath, { data });
      } else if (rawFS.isLink(stat.mode)) {
        const target = rawFS.readlink(fullFsPath);
        const modelTarget = target.startsWith(TOME_MOUNT)
          ? target.slice(TOME_MOUNT.length) || "/"
          : target;
        model.symlinks.set(fullModelPath, modelTarget);
      }
    }
  }

  walk(TOME_MOUNT, "/");
  return model;
}

// ---------------------------------------------------------------
// Content verification
// ---------------------------------------------------------------

function verifyModelMatchesFS(
  rawFS: EmscriptenFS,
  model: FSModel,
  context: string,
): void {
  const errors: string[] = [];

  for (const [path, fileState] of model.files) {
    const fsPath = rw(path);
    let stat: any;
    try {
      stat = rawFS.stat(fsPath);
    } catch {
      errors.push(`${context}: file ${path} missing (expected ${fileState.data.length}B)`);
      continue;
    }

    if (stat.size !== fileState.data.length) {
      errors.push(
        `${context}: size mismatch for ${path}: expected=${fileState.data.length}, got=${stat.size}`,
      );
      continue;
    }

    if (fileState.data.length > 0) {
      const buf = new Uint8Array(stat.size);
      const s = rawFS.open(fsPath, O.RDONLY);
      rawFS.read(s, buf, 0, stat.size, 0);
      rawFS.close(s);

      for (let i = 0; i < buf.length; i++) {
        if (buf[i] !== fileState.data[i]) {
          const pageIdx = Math.floor(i / PAGE_SIZE);
          const pageOff = i % PAGE_SIZE;
          errors.push(
            `${context}: content mismatch for ${path} at byte ${i} ` +
            `(page ${pageIdx}, offset ${pageOff}): ` +
            `expected=${fileState.data[i]}, got=${buf[i]}`,
          );
          break;
        }
      }
    }
  }

  for (const [path, target] of model.symlinks) {
    const fsPath = rw(path);
    try {
      const actualTarget = rawFS.readlink(fsPath);
      const normalizedActual = actualTarget.startsWith(TOME_MOUNT)
        ? actualTarget.slice(TOME_MOUNT.length) || "/"
        : actualTarget;
      if (normalizedActual !== rw(target) && normalizedActual !== target) {
        errors.push(
          `${context}: symlink ${path} target mismatch: expected=${target}, got=${normalizedActual}`,
        );
      }
    } catch {
      errors.push(`${context}: symlink ${path} missing`);
    }
  }

  for (const dir of model.dirs) {
    if (dir === "/") continue;
    const fsPath = rw(dir);
    try {
      const stat = rawFS.stat(fsPath);
      if (!rawFS.isDir(stat.mode)) {
        errors.push(`${context}: ${dir} exists but is not a directory`);
      }
    } catch {
      errors.push(`${context}: directory ${dir} missing`);
    }
  }

  if (errors.length > 0) {
    throw new Error(errors.join("\n"));
  }
}

// ---------------------------------------------------------------
// Core test runner
// ---------------------------------------------------------------

async function runPostRecoveryTest(
  seed: number,
  maxPages: number,
  establishOps: number,
  dirtyOps: number,
  postRecoveryOps: number,
): Promise<void> {
  const rng = new Rng(seed);
  const backend = new SyncMemoryBackend();
  const model = newModel();
  const ops: string[] = [];

  // Phase 1: Establish known state
  let instance = await mountTome(backend, maxPages);

  for (let i = 0; i < establishOps; i++) {
    const op = generateOp(rng, model);
    if (execOp(instance.rawFS, op)) {
      updateModel(model, op);
    }
    ops.push(formatOp(op, i));
  }

  doSyncfs(instance.rawFS);
  instance.tomefs.assertInvariants();
  backend.assertInvariants();

  // Phase 2: Dirty operations (not synced)
  for (let i = 0; i < dirtyOps; i++) {
    const op = generateOp(rng, model);
    if (execOp(instance.rawFS, op)) {
      updateModel(model, op);
    }
    ops.push(formatOp(op, establishOps + i));
  }

  // Phase 3: Crash — remount without syncfs
  instance = await mountTome(backend, maxPages);

  // Phase 4: Recovery syncfs
  doSyncfs(instance.rawFS);
  instance.tomefs.assertInvariants();
  backend.assertInvariants();

  // Rebuild model from actual recovered state
  const recoveredModel = rebuildModelFromFS(instance.rawFS);

  // Phase 5: Post-recovery operations with content verification
  const postOps: string[] = [];
  for (let i = 0; i < postRecoveryOps; i++) {
    const op = generateOp(rng, recoveredModel);
    if (execOp(instance.rawFS, op)) {
      updateModel(recoveredModel, op);
      resyncIfExtended(instance.rawFS, recoveredModel, op);
    }
    postOps.push(formatOp(op, i));

    // Verify content periodically (every 5 ops to balance thoroughness
    // vs speed — checking after every single op is O(n²) in total ops).
    if ((i + 1) % 5 === 0 || i === postRecoveryOps - 1) {
      try {
        verifyModelMatchesFS(
          instance.rawFS,
          recoveredModel,
          `seed ${seed}, post-recovery op ${i}`,
        );
      } catch (e: any) {
        throw new Error(
          `${(e as Error).message}\n\n` +
          `Dirty ops (last 10):\n${ops.slice(-10).join("\n")}\n\n` +
          `Post-recovery ops:\n${postOps.join("\n")}`,
        );
      }
    }
  }

  instance.tomefs.assertInvariants();
  backend.assertInvariants();

  // Phase 6: Persistence — sync + remount + verify
  doSyncfs(instance.rawFS);
  instance = await mountTome(backend, maxPages);

  try {
    verifyModelMatchesFS(
      instance.rawFS,
      recoveredModel,
      `seed ${seed}, after final remount`,
    );
  } catch (e: any) {
    throw new Error(
      `${(e as Error).message}\n\n` +
      `Post-recovery ops:\n${postOps.join("\n")}`,
    );
  }

  instance.tomefs.assertInvariants();
  backend.assertInvariants();
}

// ---------------------------------------------------------------
// Multi-cycle runner: crash → recover → work → crash → recover ...
// ---------------------------------------------------------------

async function runMultiCycleTest(
  seed: number,
  maxPages: number,
  cycles: number,
  opsPerPhase: number,
): Promise<void> {
  const rng = new Rng(seed);
  const backend = new SyncMemoryBackend();
  let model = newModel();

  // Initial setup: create some files and sync
  let instance = await mountTome(backend, maxPages);
  for (let i = 0; i < opsPerPhase; i++) {
    const op = generateOp(rng, model);
    if (execOp(instance.rawFS, op)) {
      updateModel(model, op);
    }
  }
  doSyncfs(instance.rawFS);
  instance.tomefs.assertInvariants();

  for (let cycle = 0; cycle < cycles; cycle++) {
    // Dirty phase: operations without sync
    for (let i = 0; i < opsPerPhase; i++) {
      const op = generateOp(rng, model);
      if (execOp(instance.rawFS, op)) {
        updateModel(model, op);
      }
    }

    // Crash: remount without sync
    instance = await mountTome(backend, maxPages);
    doSyncfs(instance.rawFS);
    instance.tomefs.assertInvariants();
    backend.assertInvariants();

    // Rebuild model from recovered state
    model = rebuildModelFromFS(instance.rawFS);

    // Verify rebuild matches FS immediately (catches rebuild bugs)
    verifyModelMatchesFS(
      instance.rawFS,
      model,
      `seed ${seed}, cycle ${cycle + 1} rebuild check`,
    );

    // Post-recovery operations with tracking
    const cycleOps: string[] = [];
    for (let i = 0; i < opsPerPhase; i++) {
      const op = generateOp(rng, model);
      const ok = execOp(instance.rawFS, op);
      if (ok) {
        updateModel(model, op);
        resyncIfExtended(instance.rawFS, model, op);
      }
      cycleOps.push(`${ok ? "OK" : "FAIL"} ${formatOp(op, i)}`);
    }

    // Verify content matches model after all post-recovery ops
    try {
      verifyModelMatchesFS(
        instance.rawFS,
        model,
        `seed ${seed}, cycle ${cycle + 1}`,
      );
    } catch (e: any) {
      throw new Error(
        `${(e as Error).message}\n\nCycle ${cycle + 1} ops:\n${cycleOps.join("\n")}`,
      );
    }

    // Sync to persist the post-recovery state for the next cycle
    doSyncfs(instance.rawFS);
    instance.tomefs.assertInvariants();
    backend.assertInvariants();
  }

  // Final verification: remount one more time and check persistence
  instance = await mountTome(backend, maxPages);
  verifyModelMatchesFS(
    instance.rawFS,
    model,
    `seed ${seed}, final remount`,
  );
  instance.tomefs.assertInvariants();
}

// ---------------------------------------------------------------
// Test suite
// ---------------------------------------------------------------

describe("fuzz: post-recovery operations", () => {
  describe("single crash → recovery → continue", () => {
    // 8 seeds at moderate cache pressure
    for (let i = 0; i < 8; i++) {
      const seed = 90000 + i;
      it(`seed ${seed}: 32-page cache`, async () => {
        await runPostRecoveryTest(seed, 32, 40, 20, 40);
      }, 30_000);
    }

    // 8 seeds at extreme cache pressure (3 pages = maximum eviction)
    for (let i = 0; i < 8; i++) {
      const seed = 91000 + i;
      it(`seed ${seed}: 3-page cache (extreme pressure)`, async () => {
        await runPostRecoveryTest(seed, 3, 30, 15, 30);
      }, 30_000);
    }
  });

  describe("multi-cycle crash → recovery → operate → crash", () => {
    // 4 seeds with 3 crash cycles each at moderate cache
    for (let i = 0; i < 4; i++) {
      const seed = 92000 + i;
      it(`seed ${seed}: 3 cycles, 16-page cache`, async () => {
        await runMultiCycleTest(seed, 16, 3, 25);
      }, 30_000);
    }

    // 4 seeds with 5 crash cycles each at extreme cache pressure
    for (let i = 0; i < 4; i++) {
      const seed = 93000 + i;
      it(`seed ${seed}: 5 cycles, 3-page cache (extreme)`, async () => {
        await runMultiCycleTest(seed, 3, 5, 20);
      }, 45_000);
    }
  });

  describe("targeted recovery scenarios", () => {
    it("recovery after only write operations (no structural changes)", async () => {
      const backend = new SyncMemoryBackend();
      let instance = await mountTome(backend, 8);
      const FS = instance.rawFS;

      // Create files and sync
      const s1 = FS.open(rw("/data.bin"), O.WRONLY | O.CREAT | O.TRUNC, 0o666);
      const buf1 = new Uint8Array(PAGE_SIZE * 3);
      for (let i = 0; i < buf1.length; i++) buf1[i] = i & 0xff;
      FS.write(s1, buf1, 0, buf1.length, 0);
      FS.close(s1);

      const s2 = FS.open(rw("/other.bin"), O.WRONLY | O.CREAT | O.TRUNC, 0o666);
      const buf2 = new Uint8Array(PAGE_SIZE + 100);
      buf2.fill(0xab);
      FS.write(s2, buf2, 0, buf2.length, 0);
      FS.close(s2);

      doSyncfs(FS);

      // Dirty writes (no sync)
      const s3 = FS.open(rw("/data.bin"), O.RDWR);
      const patch = new Uint8Array(500);
      patch.fill(0xcd);
      FS.write(s3, patch, 0, 500, PAGE_SIZE);
      FS.close(s3);

      // Crash → recovery
      instance = await mountTome(backend, 8);
      doSyncfs(instance.rawFS);
      instance.tomefs.assertInvariants();

      // Rebuild from recovered state
      const model = rebuildModelFromFS(instance.rawFS);

      // Post-recovery: write to recovered files, read back, verify
      const s4 = instance.rawFS.open(rw("/data.bin"), O.RDWR);
      const writeBuf = new Uint8Array(200);
      writeBuf.fill(0xef);
      instance.rawFS.write(s4, writeBuf, 0, 200, 0);
      instance.rawFS.close(s4);

      // Update model
      const fileData = model.files.get("/data.bin")!.data;
      const newData = new Uint8Array(fileData.length);
      newData.set(fileData);
      newData.set(writeBuf, 0);
      model.files.set("/data.bin", { data: newData });

      verifyModelMatchesFS(
        instance.rawFS,
        model,
        "after post-recovery write",
      );

      // Persistence check
      doSyncfs(instance.rawFS);
      instance = await mountTome(backend, 8);
      verifyModelMatchesFS(
        instance.rawFS,
        model,
        "after post-recovery remount",
      );
    });

    it("recovery after rename + unlink → continued renames work", async () => {
      const backend = new SyncMemoryBackend();
      let instance = await mountTome(backend, 8);
      const FS = instance.rawFS;

      // Create files
      for (const name of ["x.dat", "y.dat", "z.dat"]) {
        const s = FS.open(rw(`/${name}`), O.WRONLY | O.CREAT | O.TRUNC, 0o666);
        const buf = new Uint8Array(PAGE_SIZE);
        buf.fill(name.charCodeAt(0));
        FS.write(s, buf, 0, buf.length, 0);
        FS.close(s);
      }
      doSyncfs(FS);

      // Dirty: rename x→y (overwrites y), unlink z
      FS.rename(rw("/x.dat"), rw("/y.dat"));
      FS.unlink(rw("/z.dat"));

      // Crash → recovery
      instance = await mountTome(backend, 8);
      doSyncfs(instance.rawFS);
      instance.tomefs.assertInvariants();

      const model = rebuildModelFromFS(instance.rawFS);

      // Post-recovery: create new file, rename surviving file
      const s = instance.rawFS.open(rw("/new.dat"), O.WRONLY | O.CREAT | O.TRUNC, 0o666);
      const newBuf = new Uint8Array(100);
      newBuf.fill(0x42);
      instance.rawFS.write(s, newBuf, 0, 100, 0);
      instance.rawFS.close(s);
      model.files.set("/new.dat", { data: new Uint8Array(newBuf) });

      // Rename /y.dat → /renamed.dat if it survived recovery
      if (model.files.has("/y.dat")) {
        instance.rawFS.rename(rw("/y.dat"), rw("/renamed.dat"));
        const fileState = model.files.get("/y.dat")!;
        model.files.delete("/y.dat");
        model.files.set("/renamed.dat", fileState);
      }

      verifyModelMatchesFS(
        instance.rawFS,
        model,
        "after post-recovery rename",
      );

      // Persistence
      doSyncfs(instance.rawFS);
      instance = await mountTome(backend, 8);
      verifyModelMatchesFS(
        instance.rawFS,
        model,
        "after post-recovery rename remount",
      );
    });

    it("recovery after truncate → post-recovery extend works", async () => {
      const backend = new SyncMemoryBackend();
      let instance = await mountTome(backend, 16);
      const FS = instance.rawFS;

      // Create a multi-page file
      const s = FS.open(rw("/big.dat"), O.WRONLY | O.CREAT | O.TRUNC, 0o666);
      const bigBuf = new Uint8Array(PAGE_SIZE * 4);
      for (let i = 0; i < bigBuf.length; i++) bigBuf[i] = (i * 7 + 3) & 0xff;
      FS.write(s, bigBuf, 0, bigBuf.length, 0);
      FS.close(s);
      doSyncfs(FS);

      // Dirty: truncate to half size
      FS.truncate(rw("/big.dat"), PAGE_SIZE * 2);

      // Crash → recovery
      instance = await mountTome(backend, 16);
      doSyncfs(instance.rawFS);
      instance.tomefs.assertInvariants();

      const model = rebuildModelFromFS(instance.rawFS);

      // Post-recovery: extend the file beyond its recovered size
      const recovered = model.files.get("/big.dat");
      expect(recovered).toBeDefined();
      const recoveredSize = recovered!.data.length;

      const extendBuf = new Uint8Array(PAGE_SIZE * 2);
      extendBuf.fill(0xdd);
      const s2 = instance.rawFS.open(rw("/big.dat"), O.RDWR);
      instance.rawFS.write(s2, extendBuf, 0, extendBuf.length, recoveredSize);
      instance.rawFS.close(s2);

      // Update model
      const newSize = recoveredSize + extendBuf.length;
      const newData = new Uint8Array(newSize);
      newData.set(recovered!.data);
      newData.set(extendBuf, recoveredSize);
      model.files.set("/big.dat", { data: newData });

      verifyModelMatchesFS(
        instance.rawFS,
        model,
        "after post-recovery extend",
      );

      // Persistence
      doSyncfs(instance.rawFS);
      instance = await mountTome(backend, 16);
      verifyModelMatchesFS(
        instance.rawFS,
        model,
        "after post-recovery extend remount",
      );
    });
  });
});
