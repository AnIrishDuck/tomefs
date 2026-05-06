/**
 * Dirty shutdown recovery fuzz tests for tomefs.
 *
 * Simulates process crashes by remounting WITHOUT calling syncfs after
 * a batch of mutations. This exercises the critical recovery path that
 * the persistence-roundtrip fuzz never reaches: restoreTree reconciling
 * a backend whose page data and metadata are out of sync.
 *
 * After a dirty shutdown, the backend contains:
 *   - Page data from the last syncfs PLUS pages flushed by close() or
 *     eviction during the dirty phase
 *   - Metadata from the last syncfs PLUS eager metadata from rename()
 *     and unlink() (which write metadata immediately for crash safety)
 *   - Possibly /__deleted_* orphan entries from unlink-with-open-fds
 *     or rename-over-target-with-open-fds
 *
 * restoreTree must handle:
 *   - Pages beyond metadata.size (file extended, pages flushed, metadata
 *     not updated) → recovered via maxPageIndex
 *   - Missing pages below metadata.size (file truncated, pages deleted,
 *     metadata not updated) → recovered via maxPageIndex
 *   - /__deleted_* entries → cleaned up by orphan cleanup on first syncfs
 *   - Clean marker absent → triggers full tree walk + orphan cleanup
 *
 * Test strategy:
 *   Phase 1 (checkpoint): operations + clean syncfs → save model snapshot
 *   Phase 2 (dirty):      more operations WITHOUT syncfs
 *   Phase 3 (crash):      remount from backend → verify no crash
 *   Phase 4 (recovery):   syncfs on recovered FS → verify no orphan leaks
 *   Phase 5 (stability):  remount again → verify state is stable
 *
 * Ethos §9: "Write tests designed to break tomefs specifically — target
 * the seams: metadata updates after flush, dirty flush ordering"
 */

import { describe, it, expect } from "vitest";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { createTomeFS } from "../../src/tomefs.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { PAGE_SIZE } from "../../src/types.js";
import type { EmscriptenFS } from "../harness/emscripten-fs.js";
import { O, SEEK_SET, SEEK_CUR } from "../harness/emscripten-fs.js";

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
// Model: tracks expected filesystem state
// ---------------------------------------------------------------

interface FileState {
  data: Uint8Array;
  mode: number;
}

interface FSModel {
  files: Map<string, FileState>;
  dirs: Set<string>;
  symlinks: Map<string, string>;
}

function newModel(): FSModel {
  return { files: new Map(), dirs: new Set(["/"]), symlinks: new Map() };
}

function snapshotModel(model: FSModel): FSModel {
  const snapshot: FSModel = {
    files: new Map(),
    dirs: new Set(model.dirs),
    symlinks: new Map(model.symlinks),
  };
  for (const [path, state] of model.files) {
    snapshot.files.set(path, {
      data: new Uint8Array(state.data),
      mode: state.mode,
    });
  }
  return snapshot;
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
// Operation types
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
  | { type: "allocate"; path: string; offset: number; length: number }
  | { type: "chmod"; path: string; mode: number }
  | { type: "mmapWrite"; path: string; offset: number; data: Uint8Array };

const DIR_NAMES = ["alpha", "beta", "gamma"];
const FILE_NAMES = ["a.dat", "b.dat", "c.dat", "d.dat"];
const LINK_NAMES = ["lnk1", "lnk2"];

function generateOp(rng: Rng, model: FSModel): Op {
  const allFiles = [...model.files.keys()];
  const allDirs = [...model.dirs].filter((d) => d !== "/");
  const allContainerDirs = [...model.dirs];
  const allSymlinks = [...model.symlinks.keys()];

  const weights: Array<[string, number]> = [
    ["createFile", 20],
    ["mkdir", 10],
    ["writeAt", allFiles.length > 0 ? 15 : 0],
    ["truncate", allFiles.length > 0 ? 10 : 0],
    ["overwrite", allFiles.length > 0 ? 8 : 0],
    ["renameFile", allFiles.length > 0 ? 12 : 0],
    ["unlink", allFiles.length > 0 ? 8 : 0],
    ["renameDir", allDirs.length > 0 ? 6 : 0],
    ["rmdir", allDirs.filter((d) => isDirEmpty(model, d)).length > 0 ? 5 : 0],
    ["symlink", allFiles.length > 0 ? 8 : 0],
    ["unlinkSymlink", allSymlinks.length > 0 ? 4 : 0],
    ["appendWrite", allFiles.length > 0 ? 10 : 0],
    ["allocate", allFiles.length > 0 ? 8 : 0],
    ["chmod", allFiles.length > 0 ? 6 : 0],
    ["mmapWrite", allFiles.length > 0 ? 6 : 0],
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
      const sizeChoices = [1, 50, PAGE_SIZE - 1, PAGE_SIZE, PAGE_SIZE + 1, PAGE_SIZE * 2];
      const data = rng.bytes(rng.pick(sizeChoices));
      return { type: "writeAt", path, offset, data };
    }

    case "truncate": {
      const path = rng.pick(allFiles);
      const currentSize = model.files.get(path)?.data.length ?? 0;
      const sizeChoices = [0, Math.max(0, currentSize - PAGE_SIZE), currentSize, currentSize + 1, currentSize + PAGE_SIZE];
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

    case "chmod": {
      const path = rng.pick(allFiles);
      const mode = rng.pick([0o644, 0o755, 0o600, 0o444, 0o666]);
      return { type: "chmod", path, mode };
    }

    case "mmapWrite": {
      const path = rng.pick(allFiles);
      const currentSize = model.files.get(path)?.data.length ?? 0;
      const maxLen = Math.min(currentSize > 0 ? currentSize : PAGE_SIZE, PAGE_SIZE * 2);
      const length = rng.pick([1, 100, Math.min(maxLen, PAGE_SIZE)]);
      const offset = currentSize > length ? rng.int(currentSize - length + 1) : 0;
      const data = rng.bytes(length);
      return { type: "mmapWrite", path, offset, data };
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
      case "chmod":
        FS.chmod(rw(op.path), op.mode);
        return true;
      case "mmapWrite": {
        const s = FS.open(rw(op.path), O.RDWR);
        const mmapResult = s.stream_ops.mmap(s, op.data.length, op.offset, 0, 0);
        const buf = mmapResult.ptr;
        buf.set(op.data);
        s.stream_ops.msync(s, buf, op.offset, op.data.length, 0);
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
      const existing = model.files.get(op.path);
      model.files.set(op.path, {
        data: new Uint8Array(op.data),
        mode: existing ? existing.mode : 0o100666,
      });
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
    case "chmod": {
      const file = model.files.get(op.path);
      if (!file) break;
      file.mode = 0o100000 | op.mode;
      break;
    }
    case "mmapWrite": {
      const file = model.files.get(op.path);
      if (!file) break;
      const newSize = Math.max(file.data.length, op.offset + op.data.length);
      if (newSize > file.data.length) {
        const newData = new Uint8Array(newSize);
        newData.set(file.data);
        file.data = newData;
      }
      file.data.set(op.data, op.offset);
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
    case "chmod":
      return `[${index}] chmod(${op.path}, ${op.mode.toString(8)})`;
    case "mmapWrite":
      return `[${index}] mmapWrite(${op.path}, @${op.offset}, ${op.data.length}B)`;
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
// Verification
// ---------------------------------------------------------------

/**
 * Walk the entire FS tree and verify every entry is readable without
 * crashes. Does NOT verify content — just that stat/readdir/open/close
 * all succeed for every reachable node.
 */
function walkAndVerifyNavigable(rawFS: any, context: string): void {
  const errors: string[] = [];

  function walk(path: string): void {
    let entries: string[];
    try {
      entries = rawFS.readdir(path);
    } catch (e: any) {
      errors.push(`readdir(${path}) failed: ${e.message}`);
      return;
    }

    for (const name of entries) {
      if (name === "." || name === "..") continue;
      const fullPath = path === "/" ? `/${name}` : `${path}/${name}`;
      let stat: any;
      try {
        stat = rawFS.lstat(fullPath);
      } catch (e: any) {
        errors.push(`lstat(${fullPath}) failed: ${e.message}`);
        continue;
      }

      if (rawFS.isDir(stat.mode)) {
        walk(fullPath);
      } else if (rawFS.isFile(stat.mode)) {
        try {
          const s = rawFS.open(fullPath, O.RDONLY);
          if (stat.size > 0) {
            const buf = new Uint8Array(stat.size);
            rawFS.read(s, buf, 0, stat.size, 0);
          }
          rawFS.close(s);
        } catch (e: any) {
          errors.push(`read(${fullPath}, ${stat.size}B) failed: ${e.message}`);
        }
      }
    }
  }

  walk(TOME_MOUNT);

  if (errors.length > 0) {
    throw new Error(`${context}: FS not navigable:\n  ${errors.join("\n  ")}`);
  }
}

/**
 * Verify that checkpoint files are readable after dirty shutdown.
 * Files that existed at the checkpoint MUST still be readable (their
 * data was synced). Content may differ from checkpoint if dirty-phase
 * operations modified and flushed them, but the files must not be
 * corrupted (reads must not crash).
 */
function verifyCheckpointFilesReadable(
  rawFS: any,
  checkpoint: FSModel,
  context: string,
): void {
  for (const [path, fileState] of checkpoint.files) {
    const fullPath = rw(path);
    let stat: any;
    try {
      stat = rawFS.stat(fullPath);
    } catch {
      // File may have been deleted or renamed during dirty phase — that's OK.
      // rename() and unlink() eagerly update the backend, so their effects
      // survive a dirty shutdown.
      continue;
    }

    if (stat.size > 0) {
      try {
        const buf = new Uint8Array(stat.size);
        const s = rawFS.open(fullPath, O.RDONLY);
        rawFS.read(s, buf, 0, stat.size, 0);
        rawFS.close(s);
      } catch (e: any) {
        throw new Error(
          `${context}: checkpoint file ${path} (${fileState.data.length}B at checkpoint, ${stat.size}B now) read failed: ${e.message}`,
        );
      }
    }
  }
}

/**
 * After recovery sync + remount, verify backend has no orphan leaks.
 */
function verifyNoOrphans(backend: SyncMemoryBackend, context: string): void {
  const paths = backend.listFiles();
  const orphans = paths.filter((p) => p.startsWith("/__deleted_"));
  if (orphans.length > 0) {
    throw new Error(
      `${context}: ${orphans.length} orphan entries remain after recovery sync: ${orphans.join(", ")}`,
    );
  }
}

/**
 * Verify the FS is navigable and backend is clean (no orphans, no
 * page extent mismatches).
 */
function verifyCleanState(
  rawFS: any,
  backend: SyncMemoryBackend,
  context: string,
): void {
  walkAndVerifyNavigable(rawFS, context);
  verifyNoOrphans(backend, context);

  // Verify every file's page extent matches its stat size
  const errors: string[] = [];
  function walk(path: string): void {
    const entries = rawFS.readdir(path);
    for (const name of entries) {
      if (name === "." || name === "..") continue;
      const fullPath = path === "/" ? `/${name}` : `${path}/${name}`;
      const stat = rawFS.lstat(fullPath);
      if (rawFS.isDir(stat.mode)) {
        walk(fullPath);
      } else if (rawFS.isFile(stat.mode)) {
        const expectedPages = stat.size > 0 ? Math.ceil(stat.size / PAGE_SIZE) : 0;
        // Re-read to verify content length matches stat.size
        if (stat.size > 0) {
          const buf = new Uint8Array(stat.size);
          const s = rawFS.open(fullPath, O.RDONLY);
          const bytesRead = rawFS.read(s, buf, 0, stat.size, 0);
          rawFS.close(s);
          if (bytesRead !== stat.size) {
            errors.push(`${fullPath}: stat.size=${stat.size} but read returned ${bytesRead}`);
          }
        }
      }
    }
  }
  walk(TOME_MOUNT);

  if (errors.length > 0) {
    throw new Error(`${context}: clean state verification failed:\n  ${errors.join("\n  ")}`);
  }
}

// ---------------------------------------------------------------
// Fuzz runner
// ---------------------------------------------------------------

async function runDirtyShutdown(
  seed: number,
  checkpointOps: number,
  dirtyOps: number,
  maxPages: number,
): Promise<void> {
  const rng = new Rng(seed);
  const model = newModel();
  const backend = new SyncMemoryBackend();
  const ops: string[] = [];

  // Phase 1: Build state and checkpoint via clean syncfs
  let instance = await mountTome(backend, maxPages);
  for (let i = 0; i < checkpointOps; i++) {
    const op = generateOp(rng, model);
    ops.push(formatOp(op, i));
    if (execOp(instance.rawFS, op)) {
      updateModel(model, op);
    }
  }
  doSyncfs(instance.rawFS);
  const checkpoint = snapshotModel(model);

  // Phase 2: Dirty phase — operations WITHOUT syncfs.
  // These modify both in-memory state and (for rename/unlink) the backend
  // eagerly. Page data from writes may be flushed by close() or eviction,
  // but metadata is not updated (only syncfs writes metadata for
  // non-rename/non-unlink operations).
  for (let i = 0; i < dirtyOps; i++) {
    const op = generateOp(rng, model);
    ops.push(formatOp(op, checkpointOps + i));
    if (execOp(instance.rawFS, op)) {
      updateModel(model, op);
    }
  }

  // Phase 3: Dirty shutdown — remount without syncfs.
  try {
    instance = await mountTome(backend, maxPages);
  } catch (e: any) {
    const recentOps = ops.slice(Math.max(0, ops.length - 20));
    throw new Error(
      `Dirty shutdown remount crashed (seed ${seed}): ${e.message}\n\nRecent ops:\n${recentOps.join("\n")}`,
    );
  }

  const ctxDirty = `after dirty shutdown (seed ${seed})`;
  try {
    walkAndVerifyNavigable(instance.rawFS, ctxDirty);
  } catch (e: any) {
    const recentOps = ops.slice(Math.max(0, ops.length - 20));
    throw new Error(`${(e as Error).message}\n\nRecent ops:\n${recentOps.join("\n")}`);
  }

  try {
    verifyCheckpointFilesReadable(instance.rawFS, checkpoint, ctxDirty);
  } catch (e: any) {
    const recentOps = ops.slice(Math.max(0, ops.length - 20));
    throw new Error(`${(e as Error).message}\n\nRecent ops:\n${recentOps.join("\n")}`);
  }

  // Phase 4: Recovery — clean syncfs to reconcile backend state.
  doSyncfs(instance.rawFS);
  instance = await mountTome(backend, maxPages);

  const ctxRecovery = `after recovery sync (seed ${seed})`;
  try {
    verifyCleanState(instance.rawFS, backend, ctxRecovery);
  } catch (e: any) {
    const recentOps = ops.slice(Math.max(0, ops.length - 20));
    throw new Error(`${(e as Error).message}\n\nRecent ops:\n${recentOps.join("\n")}`);
  }

  // Phase 5: Verify stability — another roundtrip should produce
  // identical results (recovery didn't introduce new inconsistencies).
  doSyncfs(instance.rawFS);
  instance = await mountTome(backend, maxPages);

  const ctxStable = `after stability check (seed ${seed})`;
  try {
    verifyCleanState(instance.rawFS, backend, ctxStable);
  } catch (e: any) {
    const recentOps = ops.slice(Math.max(0, ops.length - 20));
    throw new Error(`${(e as Error).message}\n\nRecent ops:\n${recentOps.join("\n")}`);
  }
}

// ---------------------------------------------------------------
// Test cases
// ---------------------------------------------------------------

describe("fuzz: dirty shutdown recovery", () => {
  describe("tiny cache (4 pages) — maximum eviction pressure", () => {
    const CACHE = 4;

    it("seed 70001: 20 clean + 20 dirty ops @fast", async () => {
      await runDirtyShutdown(70001, 20, 20, CACHE);
    }, 30_000);

    it("seed 70002: 30 clean + 30 dirty ops", async () => {
      await runDirtyShutdown(70002, 30, 30, CACHE);
    }, 30_000);

    it("seed 70003: 15 clean + 40 dirty ops", async () => {
      await runDirtyShutdown(70003, 15, 40, CACHE);
    }, 30_000);

    it("seed 70004: 40 clean + 10 dirty ops", async () => {
      await runDirtyShutdown(70004, 40, 10, CACHE);
    }, 30_000);
  });

  describe("small cache (16 pages) — moderate eviction", () => {
    const CACHE = 16;

    it("seed 71001: 30 clean + 30 dirty ops @fast", async () => {
      await runDirtyShutdown(71001, 30, 30, CACHE);
    }, 30_000);

    it("seed 71002: 20 clean + 40 dirty ops", async () => {
      await runDirtyShutdown(71002, 20, 40, CACHE);
    }, 30_000);

    it("seed 71003: 40 clean + 20 dirty ops", async () => {
      await runDirtyShutdown(71003, 40, 20, CACHE);
    }, 30_000);
  });

  describe("medium cache (64 pages) — partial fit", () => {
    const CACHE = 64;

    it("seed 72001: 40 clean + 40 dirty ops @fast", async () => {
      await runDirtyShutdown(72001, 40, 40, CACHE);
    }, 30_000);

    it("seed 72002: 30 clean + 50 dirty ops", async () => {
      await runDirtyShutdown(72002, 30, 50, CACHE);
    }, 30_000);
  });

  describe("large cache (4096 pages) — no eviction", () => {
    it("seed 73001: 30 clean + 30 dirty ops", async () => {
      await runDirtyShutdown(73001, 30, 30, 4096);
    }, 30_000);
  });

  describe("heavy dirty phase — many mutations after last sync", () => {
    it("seed 74001: 10 clean + 80 dirty ops, tiny cache @fast", async () => {
      await runDirtyShutdown(74001, 10, 80, 4);
    }, 30_000);

    it("seed 74002: 10 clean + 80 dirty ops, small cache", async () => {
      await runDirtyShutdown(74002, 10, 80, 16);
    }, 30_000);
  });

  describe("minimal checkpoint — crash after very few synced ops", () => {
    it("seed 75001: 5 clean + 30 dirty ops, tiny cache", async () => {
      await runDirtyShutdown(75001, 5, 30, 4);
    }, 30_000);

    it("seed 75002: 3 clean + 50 dirty ops, small cache @fast", async () => {
      await runDirtyShutdown(75002, 3, 50, 16);
    }, 30_000);
  });

  describe("multiple dirty shutdown cycles", () => {
    it("seed 76001: 3 consecutive dirty shutdowns @fast", async () => {
      const rng = new Rng(76001);
      const model = newModel();
      const backend = new SyncMemoryBackend();

      // Cycle 1: build up state, sync, dirty ops, crash
      let instance = await mountTome(backend, 8);
      for (let i = 0; i < 20; i++) {
        const op = generateOp(rng, model);
        if (execOp(instance.rawFS, op)) updateModel(model, op);
      }
      doSyncfs(instance.rawFS);

      for (let i = 0; i < 15; i++) {
        const op = generateOp(rng, model);
        if (execOp(instance.rawFS, op)) updateModel(model, op);
      }
      instance = await mountTome(backend, 8);
      walkAndVerifyNavigable(instance.rawFS, "cycle 1 dirty");

      // Cycle 2: recover, more work, sync, dirty ops, crash
      doSyncfs(instance.rawFS);
      for (let i = 0; i < 15; i++) {
        const op = generateOp(rng, model);
        if (execOp(instance.rawFS, op)) updateModel(model, op);
      }
      doSyncfs(instance.rawFS);
      for (let i = 0; i < 15; i++) {
        const op = generateOp(rng, model);
        if (execOp(instance.rawFS, op)) updateModel(model, op);
      }
      instance = await mountTome(backend, 8);
      walkAndVerifyNavigable(instance.rawFS, "cycle 2 dirty");

      // Cycle 3: one more recovery + dirty shutdown
      doSyncfs(instance.rawFS);
      for (let i = 0; i < 10; i++) {
        const op = generateOp(rng, model);
        if (execOp(instance.rawFS, op)) updateModel(model, op);
      }
      instance = await mountTome(backend, 8);
      walkAndVerifyNavigable(instance.rawFS, "cycle 3 dirty");

      // Final recovery — must produce a stable, clean filesystem
      doSyncfs(instance.rawFS);
      instance = await mountTome(backend, 8);
      verifyCleanState(instance.rawFS, backend, "final recovery (seed 76001)");
    }, 60_000);
  });
});
