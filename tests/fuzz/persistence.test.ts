/**
 * Randomized persistence fuzz tests for tomefs.
 *
 * Exercises syncfs → remount cycles within random operation sequences.
 * After a syncfs+remount, all file data, directory structure, and
 * symlinks must survive exactly. This targets the class of bugs found
 * in PRs #31 (dir rename persistence) and #32 (syncfs crash safety).
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
import { O, SEEK_SET, SEEK_CUR, SEEK_END } from "../harness/emscripten-fs.js";

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
  /** Permission bits (e.g. 0o644). Excludes type bits (S_IFREG). */
  mode: number;
  /** Access time in ms (set by utime). null = not explicitly set. */
  atime: number | null;
  /** Modification time in ms (set by utime). null = not explicitly set. */
  mtime: number | null;
}

interface OpenFdState {
  /** Unique slot id for this fd. */
  id: number;
  /** Current file path (updated on rename). */
  path: string;
  /** Current write position in the file. */
  position: number;
  /** True if the file was unlinked while fd was open. */
  orphaned: boolean;
}

interface Model {
  files: Map<string, FileState>;
  dirs: Set<string>;
  /** Directory permission bits: path → mode (excludes type bits). */
  dirModes: Map<string, number>;
  /** Symlinks: path → target string. */
  symlinks: Map<string, string>;
  /** Open file descriptors: slot id → fd state. */
  openFds: Map<number, OpenFdState>;
  /** Counter for unique fd slot ids. */
  nextFdId: number;
}

/** Default file mode after creation (Emscripten default umask = 0). */
const DEFAULT_FILE_MODE = 0o666;
/** Default directory mode (Emscripten passes 0o777 as-is). */
const DEFAULT_DIR_MODE = 0o777;

function newModel(): Model {
  return { files: new Map(), dirs: new Set(["/"]), dirModes: new Map(), symlinks: new Map(), openFds: new Map(), nextFdId: 0 };
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
  | { type: "symlink"; target: string; path: string }
  | { type: "unlinkSymlink"; path: string }
  | { type: "renameSymlink"; oldPath: string; newPath: string }
  | { type: "chmodFile"; path: string; mode: number }
  | { type: "chmodDir"; path: string; mode: number }
  | { type: "openFd"; path: string; fdId: number }
  | { type: "writeFd"; fdId: number; data: Uint8Array }
  | { type: "closeFd"; fdId: number }
  | { type: "dupFd"; srcFdId: number; newFdId: number }
  | { type: "seekFd"; fdId: number; offset: number; whence: number }
  | { type: "ftruncateFd"; fdId: number; size: number }
  | { type: "appendWrite"; path: string; data: Uint8Array }
  | { type: "allocate"; path: string; offset: number; length: number }
  | { type: "utime"; path: string; atime: number; mtime: number }
  | { type: "mmapWriteAt"; path: string; position: number; data: Uint8Array }
  | { type: "checkpoint" };

const DIR_NAMES = ["aa", "bb", "cc"];
const FILE_NAMES = ["x.dat", "y.dat", "z.dat", "w.dat"];
const LINK_NAMES = ["lnk1", "lnk2", "lnk3"];

function generateOp(rng: Rng, model: Model): Op {
  const allFiles = [...model.files.keys()];
  const allDirs = [...model.dirs].filter((d) => d !== "/");
  const allContainerDirs = [...model.dirs];
  const allSymlinks = [...model.symlinks.keys()];
  // Files without an open fd (safe to open a new fd on)
  const unopenedFiles = allFiles.filter(
    (p) => ![...model.openFds.values()].some((fd) => fd.path === p && !fd.orphaned),
  );
  const openFdIds = [...model.openFds.keys()];
  // Non-orphaned fds (for write/ftruncate — orphaned fds can't be safely written through
  // because the model tracks content by path, and orphaned paths are gone from model.files)
  const activeFdIds = openFdIds.filter((id) => !model.openFds.get(id)!.orphaned);

  const weights: Array<[string, number]> = [
    ["createFile", 20],
    ["mkdir", 8],
    ["writeAt", allFiles.length > 0 ? 15 : 0],
    ["truncate", allFiles.length > 0 ? 8 : 0],
    ["renameFile", allFiles.length > 0 ? 10 : 0],
    ["unlink", allFiles.length > 0 ? 6 : 0],
    ["rmdir", allDirs.filter((d) => isDirEmpty(model, d)).length > 0 ? 4 : 0],
    ["renameDir", allDirs.length > 0 ? 6 : 0],
    ["symlink", allFiles.length > 0 ? 8 : 0],
    ["unlinkSymlink", allSymlinks.length > 0 ? 4 : 0],
    ["renameSymlink", allSymlinks.length > 0 ? 6 : 0],
    ["chmodFile", allFiles.length > 0 ? 6 : 0],
    ["chmodDir", allDirs.length > 0 ? 4 : 0],
    ["openFd", unopenedFiles.length > 0 && model.openFds.size < 4 ? 10 : 0],
    ["writeFd", activeFdIds.length > 0 ? 12 : 0],
    ["closeFd", openFdIds.length > 0 ? 6 : 0],
    ["dupFd", openFdIds.length > 0 && model.openFds.size < 8 ? 6 : 0],
    ["seekFd", activeFdIds.length > 0 ? 6 : 0],
    ["ftruncateFd", activeFdIds.length > 0 ? 5 : 0],
    ["appendWrite", allFiles.length > 0 ? 8 : 0],
    ["allocate", allFiles.length > 0 ? 8 : 0],
    ["utime", allFiles.length > 0 ? 6 : 0],
    ["mmapWriteAt", allFiles.length > 0 ? 6 : 0],
    ["checkpoint", 8],
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

    case "symlink": {
      const dir = rng.pick(allContainerDirs);
      const name = rng.pick(LINK_NAMES);
      const path = dir === "/" ? `/${name}` : `${dir}/${name}`;
      // Point at a random existing file (use its full path as target)
      const target = rng.pick(allFiles);
      return { type: "symlink", target, path };
    }

    case "unlinkSymlink":
      return { type: "unlinkSymlink", path: rng.pick(allSymlinks) };

    case "renameSymlink": {
      const oldPath = rng.pick(allSymlinks);
      const dir = rng.pick(allContainerDirs);
      const name = rng.pick(LINK_NAMES);
      const newPath = dir === "/" ? `/${name}` : `${dir}/${name}`;
      return { type: "renameSymlink", oldPath, newPath };
    }

    case "chmodFile": {
      const path = rng.pick(allFiles);
      const modeChoices = [0o444, 0o644, 0o666, 0o755, 0o700, 0o600, 0o400];
      return { type: "chmodFile", path, mode: rng.pick(modeChoices) };
    }

    case "chmodDir": {
      const path = rng.pick(allDirs);
      const modeChoices = [0o555, 0o755, 0o777, 0o700, 0o750];
      return { type: "chmodDir", path, mode: rng.pick(modeChoices) };
    }

    case "openFd": {
      const path = rng.pick(unopenedFiles);
      const fdId = model.nextFdId;
      return { type: "openFd", path, fdId };
    }

    case "writeFd": {
      const fdId = rng.pick(activeFdIds);
      const sizeChoices = [1, 50, PAGE_SIZE, PAGE_SIZE + 1];
      return { type: "writeFd", fdId, data: rng.bytes(rng.pick(sizeChoices)) };
    }

    case "closeFd":
      return { type: "closeFd", fdId: rng.pick(openFdIds) };

    case "dupFd": {
      const srcFdId = rng.pick(openFdIds);
      const newFdId = model.nextFdId;
      return { type: "dupFd", srcFdId, newFdId };
    }

    case "seekFd": {
      const fdId = rng.pick(activeFdIds);
      const fd = model.openFds.get(fdId)!;
      const fileState = model.files.get(fd.path);
      const fileSize = fileState ? fileState.content.length : 0;
      // Choose a whence and offset that produce a valid position
      const whence = rng.pick([SEEK_SET, SEEK_CUR, SEEK_END]);
      let offset: number;
      if (whence === SEEK_SET) {
        offset = rng.int(Math.max(1, fileSize + PAGE_SIZE));
      } else if (whence === SEEK_CUR) {
        // Seek relative to current position — keep result non-negative
        const maxForward = fileSize + PAGE_SIZE - fd.position;
        offset = rng.int(Math.max(1, fd.position + maxForward + 1)) - fd.position;
      } else {
        // SEEK_END: offset relative to end of file
        offset = -rng.int(Math.max(1, fileSize + 1));
      }
      return { type: "seekFd", fdId, offset, whence };
    }

    case "ftruncateFd": {
      const fdId = rng.pick(activeFdIds);
      const fd = model.openFds.get(fdId)!;
      const fileState = model.files.get(fd.path);
      const currentSize = fileState ? fileState.content.length : 0;
      const sizeChoices = [0, Math.max(0, currentSize - PAGE_SIZE), currentSize, currentSize + PAGE_SIZE];
      return { type: "ftruncateFd", fdId, size: rng.pick(sizeChoices) };
    }

    case "appendWrite": {
      const path = rng.pick(allFiles);
      const sizeChoices = [1, 50, PAGE_SIZE, PAGE_SIZE + 1];
      return { type: "appendWrite", path, data: rng.bytes(rng.pick(sizeChoices)) };
    }

    case "allocate": {
      const path = rng.pick(allFiles);
      const currentSize = model.files.get(path)!.content.length;
      // Allocate beyond current size (may be page-aligned or not)
      const sizeChoices = [
        currentSize + PAGE_SIZE,
        currentSize + 3 * PAGE_SIZE,
        currentSize + PAGE_SIZE + 500,
        Math.max(PAGE_SIZE, currentSize),
      ];
      const totalSize = rng.pick(sizeChoices);
      return { type: "allocate", path, offset: 0, length: totalSize };
    }

    case "utime": {
      const path = rng.pick(allFiles);
      // Use fixed timestamps for deterministic verification
      const baseTime = 1000000000000; // ~2001
      const atime = baseTime + rng.int(1000000000);
      const mtime = baseTime + rng.int(1000000000);
      return { type: "utime", path, atime, mtime };
    }

    case "mmapWriteAt": {
      const path = rng.pick(allFiles);
      const state = model.files.get(path)!;
      const fileSize = state.content.length;
      if (fileSize === 0) return { type: "checkpoint" };
      // Write within existing file bounds via mmap+msync
      const maxLen = Math.min(fileSize, PAGE_SIZE * 2);
      const length = Math.max(1, rng.int(maxLen + 1));
      const position = rng.int(Math.max(1, fileSize - length + 1));
      const data = rng.bytes(length);
      return { type: "mmapWriteAt", path, position, data };
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
    case "symlink": return `[${index}] symlink(${op.target} -> ${op.path})`;
    case "unlinkSymlink": return `[${index}] unlinkSymlink(${op.path})`;
    case "renameSymlink": return `[${index}] renameSymlink(${op.oldPath} -> ${op.newPath})`;
    case "chmodFile": return `[${index}] chmod(${op.path}, 0o${op.mode.toString(8)})`;
    case "chmodDir": return `[${index}] chmod(${op.path}, 0o${op.mode.toString(8)})`;
    case "openFd": return `[${index}] openFd(${op.path}, fd#${op.fdId})`;
    case "writeFd": return `[${index}] writeFd(fd#${op.fdId}, ${op.data.length}B)`;
    case "closeFd": return `[${index}] closeFd(fd#${op.fdId})`;
    case "dupFd": return `[${index}] dupFd(fd#${op.srcFdId} -> fd#${op.newFdId})`;
    case "seekFd": return `[${index}] seekFd(fd#${op.fdId}, ${op.offset}, whence=${op.whence})`;
    case "ftruncateFd": return `[${index}] ftruncateFd(fd#${op.fdId}, ${op.size})`;
    case "appendWrite": return `[${index}] appendWrite(${op.path}, ${op.data.length}B)`;
    case "allocate": return `[${index}] allocate(${op.path}, @${op.offset}, ${op.length})`;
    case "utime": return `[${index}] utime(${op.path}, atime=${op.atime}, mtime=${op.mtime})`;
    case "mmapWriteAt": return `[${index}] mmapWriteAt(${op.path}, @${op.position}, ${op.data.length}B)`;

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
      model.files.set(op.path, { content: new Uint8Array(op.data), mode: DEFAULT_FILE_MODE, atime: null, mtime: null });
      break;
    }
    case "writeAt": {
      const state = model.files.get(op.path)!;
      const newSize = Math.max(state.content.length, op.offset + op.data.length);
      const newContent = new Uint8Array(newSize);
      newContent.set(state.content); // copy existing (zero-extends if growing)
      newContent.set(op.data, op.offset);
      state.content = newContent;
      // Write updates mtime, invalidating any explicit utime value
      state.mtime = null;
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
      state.mtime = null;
      break;
    }
    case "renameFile": {
      const state = model.files.get(op.oldPath)!;
      model.files.delete(op.oldPath);
      // If destination had an open fd, mark it orphaned (overwritten file)
      for (const fd of model.openFds.values()) {
        if (fd.path === op.newPath && !fd.orphaned) fd.orphaned = true;
      }
      model.files.set(op.newPath, state);
      // Update any open fds that referenced the old path
      for (const fd of model.openFds.values()) {
        if (fd.path === op.oldPath) fd.path = op.newPath;
      }
      break;
    }
    case "unlink": {
      // Mark any open fds on this file as orphaned
      for (const fd of model.openFds.values()) {
        if (fd.path === op.path && !fd.orphaned) fd.orphaned = true;
      }
      model.files.delete(op.path);
      break;
    }
    case "mkdir":
      model.dirs.add(op.path);
      model.dirModes.set(op.path, DEFAULT_DIR_MODE);
      break;
    case "rmdir":
      model.dirs.delete(op.path);
      model.dirModes.delete(op.path);
      break;
    case "renameDir": {
      const oldPrefix = op.oldPath + "/";
      for (const [path, state] of [...model.files]) {
        if (path.startsWith(oldPrefix)) {
          const newPath = op.newPath + path.slice(op.oldPath.length);
          model.files.delete(path);
          model.files.set(newPath, state);
          // Update open fds that referenced files under the old directory
          for (const fd of model.openFds.values()) {
            if (fd.path === path) fd.path = newPath;
          }
        }
      }
      for (const dir of [...model.dirs]) {
        if (dir === op.oldPath || dir.startsWith(oldPrefix)) {
          model.dirs.delete(dir);
          const newDir = op.newPath + dir.slice(op.oldPath.length);
          model.dirs.add(newDir);
          // Move directory mode
          const dirMode = model.dirModes.get(dir);
          if (dirMode !== undefined) {
            model.dirModes.delete(dir);
            model.dirModes.set(newDir, dirMode);
          }
        }
      }
      for (const [path, target] of [...model.symlinks]) {
        if (path.startsWith(oldPrefix)) {
          model.symlinks.delete(path);
          model.symlinks.set(op.newPath + path.slice(op.oldPath.length), target);
        }
      }
      if (!model.dirs.has(op.newPath)) model.dirs.add(op.newPath);
      break;
    }
    case "symlink":
      model.symlinks.set(op.path, op.target);
      break;
    case "unlinkSymlink":
      model.symlinks.delete(op.path);
      break;
    case "renameSymlink": {
      const target = model.symlinks.get(op.oldPath)!;
      model.symlinks.delete(op.oldPath);
      // Overwrite destination symlink if it exists
      model.symlinks.set(op.newPath, target);
      break;
    }
    case "chmodFile": {
      const state = model.files.get(op.path);
      if (state) state.mode = op.mode;
      break;
    }
    case "chmodDir":
      model.dirModes.set(op.path, op.mode);
      break;
    case "openFd": {
      const fileState = model.files.get(op.path);
      model.openFds.set(op.fdId, {
        id: op.fdId,
        path: op.path,
        position: fileState ? fileState.content.length : 0,
        orphaned: false,
      });
      model.nextFdId = op.fdId + 1;
      break;
    }
    case "writeFd": {
      const fd = model.openFds.get(op.fdId)!;
      const fileState = model.files.get(fd.path)!;
      const newSize = Math.max(fileState.content.length, fd.position + op.data.length);
      const newContent = new Uint8Array(newSize);
      newContent.set(fileState.content);
      newContent.set(op.data, fd.position);
      fileState.content = newContent;
      fd.position += op.data.length;
      fileState.mtime = null;
      break;
    }
    case "closeFd":
      model.openFds.delete(op.fdId);
      break;
    case "dupFd": {
      const srcFd = model.openFds.get(op.srcFdId)!;
      model.openFds.set(op.newFdId, {
        id: op.newFdId,
        path: srcFd.path,
        position: srcFd.position,
        orphaned: srcFd.orphaned,
      });
      model.nextFdId = op.newFdId + 1;
      break;
    }
    case "seekFd": {
      const fd = model.openFds.get(op.fdId)!;
      const fileState = model.files.get(fd.path);
      const fileSize = fileState ? fileState.content.length : 0;
      let newPos: number;
      if (op.whence === SEEK_SET) {
        newPos = op.offset;
      } else if (op.whence === SEEK_CUR) {
        newPos = fd.position + op.offset;
      } else {
        newPos = fileSize + op.offset;
      }
      if (newPos >= 0) fd.position = newPos;
      break;
    }
    case "appendWrite": {
      const state = model.files.get(op.path)!;
      const newContent = new Uint8Array(state.content.length + op.data.length);
      newContent.set(state.content);
      newContent.set(op.data, state.content.length);
      state.content = newContent;
      state.mtime = null;
      break;
    }
    case "ftruncateFd": {
      const fd = model.openFds.get(op.fdId)!;
      const fileState = model.files.get(fd.path)!;
      if (op.size < fileState.content.length) {
        fileState.content = fileState.content.slice(0, op.size);
      } else if (op.size > fileState.content.length) {
        const newContent = new Uint8Array(op.size);
        newContent.set(fileState.content);
        fileState.content = newContent;
      }
      fileState.mtime = null;
      break;
    }
    case "allocate": {
      const state = model.files.get(op.path)!;
      const newSize = Math.max(state.content.length, op.offset + op.length);
      if (newSize > state.content.length) {
        const newContent = new Uint8Array(newSize);
        newContent.set(state.content);
        state.content = newContent;
      }
      break;
    }
    case "utime": {
      const state = model.files.get(op.path);
      if (state) {
        state.atime = op.atime;
        state.mtime = op.mtime;
      }
      break;
    }
    case "mmapWriteAt": {
      const state = model.files.get(op.path)!;
      state.content.set(op.data, op.position);
      // msync modifies file content but doesn't change size
      // mtime is updated by the write (clear explicit utime tracking)
      state.mtime = null;
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
  /** Maps model fd slot id → live Emscripten stream object. */
  liveStreams: Map<number, EmscriptenStream>;
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
  return { FS, rawFS, backend, tomefs, maxPages, liveStreams: new Map() };
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
  return { FS, rawFS, backend: h.backend, tomefs, maxPages: h.maxPages, liveStreams: new Map() };
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

function execOp(harness: PersistenceHarness, op: Op): boolean {
  const FS = harness.FS;
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
      case "symlink":
        FS.symlink(op.target, op.path);
        return true;
      case "renameSymlink":
        FS.rename(op.oldPath, op.newPath);
        return true;
      case "unlinkSymlink":
      case "unlink":
        FS.unlink(op.path);
        return true;
      case "mkdir":
        FS.mkdir(op.path, 0o777);
        return true;
      case "rmdir":
        FS.rmdir(op.path);
        return true;
      case "chmodFile":
      case "chmodDir":
        FS.chmod(op.path, op.mode);
        return true;
      case "openFd": {
        // Open with RDWR | APPEND so writes go to end (matching model behavior)
        const s = FS.open(op.path, O.RDWR);
        // Seek to end to match model's initial position = file size
        FS.llseek(s, 0, 2 /* SEEK_END */);
        harness.liveStreams.set(op.fdId, s);
        return true;
      }
      case "writeFd": {
        const s = harness.liveStreams.get(op.fdId)!;
        FS.write(s, op.data, 0, op.data.length);
        return true;
      }
      case "closeFd": {
        const s = harness.liveStreams.get(op.fdId)!;
        FS.close(s);
        harness.liveStreams.delete(op.fdId);
        return true;
      }
      case "dupFd": {
        const srcStream = harness.liveStreams.get(op.srcFdId)!;
        const dupStream = FS.dupStream(srcStream);
        harness.liveStreams.set(op.newFdId, dupStream);
        return true;
      }
      case "seekFd": {
        const s = harness.liveStreams.get(op.fdId)!;
        FS.llseek(s, op.offset, op.whence);
        return true;
      }
      case "appendWrite": {
        const s = FS.open(op.path, O.WRONLY | O.APPEND);
        FS.write(s, op.data, 0, op.data.length);
        FS.close(s);
        return true;
      }
      case "ftruncateFd": {
        const s = harness.liveStreams.get(op.fdId)!;
        FS.ftruncate(s.fd, op.size);
        return true;
      }
      case "allocate": {
        const s = FS.open(op.path, O.RDWR);
        s.stream_ops.allocate(s, op.offset, op.length);
        FS.close(s);
        return true;
      }
      case "utime":
        FS.utime(op.path, op.atime, op.mtime);
        return true;
      case "mmapWriteAt": {
        const s = FS.open(op.path, O.RDWR);
        // mmap the region, write data, msync back
        const mmapResult = s.stream_ops.mmap(s, op.data.length, op.position, 0, 0);
        const buf = mmapResult.ptr instanceof Uint8Array ? mmapResult.ptr : new Uint8Array(mmapResult.ptr);
        buf.set(op.data);
        s.stream_ops.msync(s, buf, op.position, op.data.length, 0);
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

// ---------------------------------------------------------------
// Verify all model files against FS
// ---------------------------------------------------------------

/** Extract permission bits (lower 12 bits, excluding type). */
const PERM_MASK = 0o7777;

function verifyModel(FS: EmscriptenFS, model: Model, context: string): void {
  for (const [path, state] of model.files) {
    let stat: any;
    try {
      stat = FS.stat(path);
    } catch (e: any) {
      throw new Error(`${context}: file ${path} should exist but stat failed: ${e.message}`);
    }

    expect(stat.size, `${context}: size mismatch for ${path}`).toBe(state.content.length);

    // Verify permission mode survived persistence
    const actualPerm = stat.mode & PERM_MASK;
    expect(
      actualPerm,
      `${context}: mode mismatch for ${path}: expected 0o${state.mode.toString(8)}, got 0o${actualPerm.toString(8)}`,
    ).toBe(state.mode);

    // Verify timestamps survived persistence (only when explicitly set via utime)
    if (state.atime !== null) {
      const actualAtime = new Date(stat.atime).getTime();
      expect(
        actualAtime,
        `${context}: atime mismatch for ${path}: expected ${state.atime}, got ${actualAtime}`,
      ).toBe(state.atime);
    }
    if (state.mtime !== null) {
      const actualMtime = new Date(stat.mtime).getTime();
      expect(
        actualMtime,
        `${context}: mtime mismatch for ${path}: expected ${state.mtime}, got ${actualMtime}`,
      ).toBe(state.mtime);
    }

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

  // Verify directories exist and modes match
  for (const dir of model.dirs) {
    if (dir === "/") continue;
    try {
      const stat = FS.stat(dir);
      expect(FS.isDir(stat.mode), `${context}: ${dir} should be a directory`).toBe(true);
      const expectedMode = model.dirModes.get(dir);
      if (expectedMode !== undefined) {
        const actualPerm = stat.mode & PERM_MASK;
        expect(
          actualPerm,
          `${context}: dir mode mismatch for ${dir}: expected 0o${expectedMode.toString(8)}, got 0o${actualPerm.toString(8)}`,
        ).toBe(expectedMode);
      }
    } catch (e: any) {
      throw new Error(`${context}: directory ${dir} should exist but stat failed: ${e.message}`);
    }
  }

  // Verify symlinks survive persistence
  for (const [path, target] of model.symlinks) {
    let linkTarget: string;
    try {
      const stat = FS.lstat(path);
      expect(FS.isLink(stat.mode), `${context}: ${path} should be a symlink`).toBe(true);
      linkTarget = FS.readlink(path);
    } catch (e: any) {
      throw new Error(`${context}: symlink ${path} should exist but failed: ${e.message}`);
    }
    expect(linkTarget, `${context}: symlink ${path} target mismatch`).toBe(target);
  }
}

// ---------------------------------------------------------------
// Fuzz runner with persistence checkpoints
// ---------------------------------------------------------------

/** Close all open fds on a harness and clear them from the model. */
function closeAllFds(harness: PersistenceHarness, model: Model): void {
  for (const [fdId, stream] of harness.liveStreams) {
    try { harness.FS.close(stream); } catch { /* already closed or invalid */ }
  }
  harness.liveStreams.clear();
  model.openFds.clear();
}

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
      // Persist with fds still open (like Postgres during fsync/checkpoint)
      harness.rawFS.syncfs(false, (err: Error | null) => {
        if (err) throw err;
      });
      // Close all fds before remount — fds don't survive module teardown
      closeAllFds(harness, model);
      harness = await remount(harness);
      checkpoints++;

      // Verify all files survived the remount
      verifyModel(harness.FS, model, `checkpoint ${checkpoints} (after op ${i})`);
      continue;
    }

    const success = execOp(harness, op);
    applyToModel(model, op, success);
  }

  // Close open fds, persist, remount, verify
  closeAllFds(harness, model);
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

  describe("fd operations across syncfs + remount (ethos §9)", () => {
    it("seed 60001 — tiny cache, fds open during syncfs @fast", async () => {
      await runPersistenceFuzz(60001, 80, 4);
    }, 30_000);

    it("seed 60002 — tiny cache, fd writes interleaved with renames", async () => {
      await runPersistenceFuzz(60002, 80, 4);
    }, 30_000);

    it("seed 60003 — small cache, fds + unlink + persist", async () => {
      await runPersistenceFuzz(60003, 80, 16);
    }, 30_000);

    it("seed 60004 — large cache, fds + truncate + persist", async () => {
      await runPersistenceFuzz(60004, 100, 4096);
    }, 30_000);

    it("150 ops, tiny cache, seed 60099 — extended fd+persist sequence", async () => {
      await runPersistenceFuzz(60099, 150, 4);
    }, 60_000);

    it("150 ops, small cache, seed 60100 — extended fd+persist sequence", async () => {
      await runPersistenceFuzz(60100, 150, 16);
    }, 60_000);
  });

  describe("dupFd + seekFd + appendWrite across remount cycles", () => {
    it("seed 70001 — tiny cache, dup fds open during syncfs @fast", async () => {
      await runPersistenceFuzz(70001, 80, 4);
    }, 30_000);

    it("seed 70002 — tiny cache, seek + write interleaved with renames", async () => {
      await runPersistenceFuzz(70002, 80, 4);
    }, 30_000);

    it("seed 70003 — small cache, dup + unlink + persist", async () => {
      await runPersistenceFuzz(70003, 80, 16);
    }, 30_000);

    it("seed 70004 — large cache, append + seek + persist", async () => {
      await runPersistenceFuzz(70004, 100, 4096);
    }, 30_000);

    it("150 ops, tiny cache, seed 70099 — extended dup+seek+append sequence", async () => {
      await runPersistenceFuzz(70099, 150, 4);
    }, 60_000);

    it("150 ops, small cache, seed 70100 — extended dup+seek+append sequence", async () => {
      await runPersistenceFuzz(70100, 150, 16);
    }, 60_000);
  });

  describe("chmod + mode persistence through remount cycles", () => {
    it("seed 11111 — tiny cache, mode changes between checkpoints @fast", async () => {
      await runPersistenceFuzz(11111, 80, 4);
    }, 30_000);

    it("seed 22222 — small cache, chmod + rename + persist", async () => {
      await runPersistenceFuzz(22222, 80, 16);
    }, 30_000);

    it("seed 33333 — dir chmod + renameDir + persist", async () => {
      await runPersistenceFuzz(33333, 80, 16);
    }, 30_000);

    it("seed 44444 — large cache, chmod interleaved with writes", async () => {
      await runPersistenceFuzz(44444, 100, 4096);
    }, 30_000);

    it("150 ops, tiny cache, seed 55555 — extended chmod sequence", async () => {
      await runPersistenceFuzz(55555, 150, 4);
    }, 60_000);
  });

  describe("utime + timestamp persistence through remount cycles", () => {
    it("seed 80001 — tiny cache, utime between checkpoints @fast", async () => {
      await runPersistenceFuzz(80001, 80, 4);
    }, 30_000);

    it("seed 80002 — small cache, utime + rename + persist", async () => {
      await runPersistenceFuzz(80002, 80, 16);
    }, 30_000);

    it("seed 80003 — tiny cache, utime + write invalidation + persist", async () => {
      await runPersistenceFuzz(80003, 80, 4);
    }, 30_000);

    it("seed 80004 — large cache, utime interleaved with truncate + allocate", async () => {
      await runPersistenceFuzz(80004, 100, 4096);
    }, 30_000);

    it("150 ops, tiny cache, seed 80099 — extended utime sequence", async () => {
      await runPersistenceFuzz(80099, 150, 4);
    }, 60_000);
  });

  describe("mmapWrite (msync) persistence through remount cycles", () => {
    it("seed 90001 — tiny cache, msync writes between checkpoints @fast", async () => {
      await runPersistenceFuzz(90001, 80, 4);
    }, 30_000);

    it("seed 90002 — small cache, msync + rename + persist", async () => {
      await runPersistenceFuzz(90002, 80, 16);
    }, 30_000);

    it("seed 90003 — tiny cache, msync interleaved with fd writes", async () => {
      await runPersistenceFuzz(90003, 80, 4);
    }, 30_000);

    it("seed 90004 — large cache, msync + truncate + allocate + persist", async () => {
      await runPersistenceFuzz(90004, 100, 4096);
    }, 30_000);

    it("150 ops, tiny cache, seed 90099 — extended msync+utime sequence", async () => {
      await runPersistenceFuzz(90099, 150, 4);
    }, 60_000);
  });
});
