/**
 * Persistence roundtrip fuzz tests for tomefs.
 *
 * Generates random sequences of filesystem operations against tomefs, then
 * at periodic intervals performs a full persistence roundtrip: syncfs →
 * create new Emscripten module → mount fresh tomefs with the same backend →
 * verify all file contents, metadata, and directory structure survived.
 *
 * This targets the seam between the page cache and the persist/restore path
 * (restoreTree). The existing differential fuzz test (differential.test.ts)
 * compares tomefs vs MEMFS during a single session but never remounts. This
 * test catches bugs where data is lost or corrupted across syncfs + remount
 * cycles: unflushed pages, incorrect metadata persistence, restoreTree size
 * miscalculation, lost symlink targets, directory structure corruption, and
 * timestamp rounding errors.
 *
 * Uses a seeded PRNG for reproducibility — failing seeds can be replayed.
 * Runs at multiple cache pressure levels to maximize eviction coverage.
 *
 * Ethos §8: "Workload scenarios verify that tomefs works end-to-end"
 * Ethos §9: "Write tests designed to break tomefs specifically — target the seams"
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
    for (let i = 0; i < length; i++) {
      buf[i] = this.next() & 0xff;
    }
    return buf;
  }
}

// ---------------------------------------------------------------
// Model: tracks expected filesystem state for verification
// ---------------------------------------------------------------

interface FileState {
  /** Expected file contents (the source of truth after each operation). */
  data: Uint8Array;
  /** File mode. */
  mode: number;
  /** Access time in ms (set by utime). null = not explicitly set. */
  atime: number | null;
  /** Modification time in ms (set by utime). null = not explicitly set. */
  mtime: number | null;
}

interface DirState {
  mode: number;
}

interface SymlinkState {
  target: string;
}

interface OpenFdState {
  /** Model path of the open file. */
  path: string;
  /** Current stream position. */
  position: number;
}

interface FSModel {
  files: Map<string, FileState>;
  dirs: Map<string, DirState>;
  symlinks: Map<string, SymlinkState>;
  /** Open file descriptors: fdId → state. */
  openFds: Map<number, OpenFdState>;
  /** Counter for unique fd slot ids. */
  nextFdId: number;
}

function newModel(): FSModel {
  return {
    files: new Map(),
    dirs: new Map([["/" , { mode: 0o40777 }]]),
    symlinks: new Map(),
    openFds: new Map(),
    nextFdId: 0,
  };
}

// ---------------------------------------------------------------
// Operation types (subset focused on data-mutating operations)
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
  | { type: "symlink"; target: string; path: string }
  | { type: "unlinkSymlink"; path: string }
  | { type: "renameSymlink"; oldPath: string; newPath: string }
  | { type: "renameDir"; oldPath: string; newPath: string }
  | { type: "appendWrite"; path: string; data: Uint8Array }
  | { type: "chmod"; path: string; mode: number }
  | { type: "openFd"; path: string; fdId: number }
  | { type: "writeFd"; fdId: number; data: Uint8Array }
  | { type: "seekFd"; fdId: number; offset: number; whence: number }
  | { type: "closeFd"; fdId: number }
  | { type: "allocate"; path: string; offset: number; length: number }
  | { type: "utime"; path: string; atime: number; mtime: number };

const DIR_NAMES = ["alpha", "beta", "gamma"];
const FILE_NAMES = ["a.dat", "b.dat", "c.dat", "d.dat"];
const LINK_NAMES = ["lnk1", "lnk2"];

function filesInDir(model: FSModel, dir: string): string[] {
  const prefix = dir === "/" ? "/" : dir + "/";
  return [...model.files.keys()].filter((p) => {
    if (!p.startsWith(prefix)) return false;
    return !p.slice(prefix.length).includes("/");
  });
}

function dirsInDir(model: FSModel, dir: string): string[] {
  const prefix = dir === "/" ? "/" : dir + "/";
  return [...model.dirs.keys()].filter((d) => {
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
// Operation generator
// ---------------------------------------------------------------

function generateOp(rng: Rng, model: FSModel): Op {
  const allFiles = [...model.files.keys()];
  const allDirs = [...model.dirs.keys()].filter((d) => d !== "/");
  const allContainerDirs = [...model.dirs.keys()];
  const allSymlinks = [...model.symlinks.keys()];
  // Files without an open fd (safe to open a new fd on)
  const unopenedFiles = allFiles.filter(
    (p) => ![...model.openFds.values()].some((fd) => fd.path === p),
  );
  const openFdIds = [...model.openFds.keys()];

  const weights: Array<[string, number]> = [
    ["createFile", 25],
    ["mkdir", 12],
    ["writeAt", allFiles.length > 0 ? 20 : 0],
    ["truncate", allFiles.length > 0 ? 10 : 0],
    ["overwrite", allFiles.length > 0 ? 10 : 0],
    ["renameFile", allFiles.length > 0 ? 10 : 0],
    ["unlink", allFiles.length > 0 ? 8 : 0],
    ["appendWrite", allFiles.length > 0 ? 10 : 0],
    ["chmod", allFiles.length > 0 ? 5 : 0],
    ["symlink", allFiles.length > 0 ? 8 : 0],
    ["unlinkSymlink", allSymlinks.length > 0 ? 4 : 0],
    ["renameSymlink", allSymlinks.length > 0 ? 5 : 0],
    ["rmdir", allDirs.filter((d) => isDirEmpty(model, d)).length > 0 ? 5 : 0],
    ["renameDir", allDirs.length > 0 ? 5 : 0],
    ["openFd", unopenedFiles.length > 0 && model.openFds.size < 4 ? 10 : 0],
    ["writeFd", openFdIds.length > 0 ? 12 : 0],
    ["seekFd", openFdIds.length > 0 ? 6 : 0],
    ["closeFd", openFdIds.length > 0 ? 6 : 0],
    ["allocate", allFiles.length > 0 ? 8 : 0],
    ["utime", allFiles.length > 0 ? 6 : 0],
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
      const currentSize = model.files.get(path)!.data.length;
      const maxOffset = currentSize + PAGE_SIZE;
      const offset = rng.int(maxOffset + 1);
      const sizeChoices = [1, 50, PAGE_SIZE - 1, PAGE_SIZE, PAGE_SIZE + 1, PAGE_SIZE * 2];
      const data = rng.bytes(rng.pick(sizeChoices));
      return { type: "writeAt", path, offset, data };
    }

    case "truncate": {
      const path = rng.pick(allFiles);
      const currentSize = model.files.get(path)!.data.length;
      const sizeChoices = [
        0,
        Math.max(0, currentSize - PAGE_SIZE),
        Math.max(0, currentSize - 1),
        currentSize,
        currentSize + 1,
        currentSize + PAGE_SIZE,
      ];
      return { type: "truncate", path, size: rng.pick(sizeChoices) };
    }

    case "overwrite": {
      const path = rng.pick(allFiles);
      const sizeChoices = [0, 1, 100, PAGE_SIZE, PAGE_SIZE * 2 + 77];
      const data = rng.bytes(rng.pick(sizeChoices));
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

    case "appendWrite": {
      const path = rng.pick(allFiles);
      const sizeChoices = [1, 50, PAGE_SIZE - 1, PAGE_SIZE, PAGE_SIZE + 1];
      const data = rng.bytes(rng.pick(sizeChoices));
      return { type: "appendWrite", path, data };
    }

    case "chmod": {
      const path = rng.pick(allFiles);
      const modeChoices = [0o444, 0o555, 0o644, 0o666, 0o755, 0o777];
      return { type: "chmod", path, mode: rng.pick(modeChoices) };
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

    case "renameSymlink": {
      const oldPath = rng.pick(allSymlinks);
      const dir = rng.pick(allContainerDirs);
      const name = rng.pick(LINK_NAMES);
      const newPath = dir === "/" ? `/${name}` : `${dir}/${name}`;
      return { type: "renameSymlink", oldPath, newPath };
    }

    case "rmdir": {
      const emptyDirs = allDirs.filter((d) => isDirEmpty(model, d));
      if (emptyDirs.length === 0) return { type: "createFile", path: `/${rng.pick(FILE_NAMES)}`, data: rng.bytes(10) };
      return { type: "rmdir", path: rng.pick(emptyDirs) };
    }

    case "renameDir": {
      const oldPath = rng.pick(allDirs);
      const parent = rng.pick(allContainerDirs);
      const name = rng.pick(DIR_NAMES);
      const newPath = parent === "/" ? `/${name}` : `${parent}/${name}`;
      if (newPath.startsWith(oldPath + "/") || newPath === oldPath) {
        // Avoid renaming into self; generate a safe fallback
        return { type: "createFile", path: `/${rng.pick(FILE_NAMES)}`, data: rng.bytes(10) };
      }
      return { type: "renameDir", oldPath, newPath };
    }

    case "openFd": {
      const path = rng.pick(unopenedFiles);
      const fdId = model.nextFdId;
      return { type: "openFd", path, fdId };
    }

    case "writeFd": {
      const fdId = rng.pick(openFdIds);
      const sizeChoices = [1, 50, PAGE_SIZE - 1, PAGE_SIZE, PAGE_SIZE + 1];
      const data = rng.bytes(rng.pick(sizeChoices));
      return { type: "writeFd", fdId, data };
    }

    case "seekFd": {
      const fdId = rng.pick(openFdIds);
      const fd = model.openFds.get(fdId)!;
      const file = model.files.get(fd.path);
      const fileSize = file ? file.data.length : 0;
      const whenceChoices = [SEEK_SET, SEEK_SET, SEEK_CUR]; // bias toward SEEK_SET for simplicity
      const whence = rng.pick(whenceChoices);
      let offset: number;
      if (whence === SEEK_SET) {
        offset = rng.int(fileSize + PAGE_SIZE + 1);
      } else {
        // SEEK_CUR: relative to current position, keep non-negative result
        const maxForward = fileSize + PAGE_SIZE - fd.position;
        offset = rng.int(Math.max(1, maxForward + 1));
      }
      return { type: "seekFd", fdId, offset, whence };
    }

    case "closeFd":
      return { type: "closeFd", fdId: rng.pick(openFdIds) };

    case "allocate": {
      const path = rng.pick(allFiles);
      const file = model.files.get(path)!;
      const currentSize = file.data.length;
      // Extend by various amounts, sometimes beyond current size
      const totalSizeChoices = [
        currentSize + 1,
        currentSize + 100,
        currentSize + PAGE_SIZE,
        currentSize + PAGE_SIZE * 2 + 37,
        // Sometimes allocate within existing range (no-op for size)
        Math.max(1, Math.floor(currentSize / 2)),
      ];
      const totalSize = rng.pick(totalSizeChoices);
      return { type: "allocate", path, offset: 0, length: totalSize };
    }

    case "utime": {
      const path = rng.pick(allFiles);
      // Generate timestamps in milliseconds (Emscripten FS.utime takes ms)
      // Use whole-second values to avoid rounding issues in persistence
      const baseTime = 1700000000000; // ~Nov 2023 in ms
      const atime = baseTime + rng.int(10000000) * 1000;
      const mtime = baseTime + rng.int(10000000) * 1000;
      return { type: "utime", path, atime, mtime };
    }

    default:
      return { type: "createFile", path: `/${rng.pick(FILE_NAMES)}`, data: rng.bytes(10) };
  }
}

// ---------------------------------------------------------------
// Execute an operation and update the model
// ---------------------------------------------------------------

const TOME_MOUNT = "/tome";

function rw(p: string): string {
  if (!p.startsWith("/") || p.startsWith("/dev") || p.startsWith("/proc") || p.startsWith("/tmp")) return p;
  if (p.startsWith(TOME_MOUNT + "/") || p === TOME_MOUNT) return p;
  if (p === "/") return TOME_MOUNT;
  return TOME_MOUNT + p;
}

/** Maps fdId → Emscripten stream for open FDs during a fuzz run. */
type StreamMap = Map<number, any>;

function execOp(FS: EmscriptenFS, op: Op, streams: StreamMap): boolean {
  try {
    switch (op.type) {
      case "createFile": {
        const s = FS.open(rw(op.path), O.WRONLY | O.CREAT | O.TRUNC, 0o666);
        if (op.data.length > 0) {
          FS.write(s, op.data, 0, op.data.length, 0);
        }
        FS.close(s);
        return true;
      }

      case "writeAt": {
        const s = FS.open(rw(op.path), O.RDWR);
        FS.write(s, op.data, 0, op.data.length, op.offset);
        FS.close(s);
        return true;
      }

      case "truncate": {
        FS.truncate(rw(op.path), op.size);
        return true;
      }

      case "overwrite": {
        const s = FS.open(rw(op.path), O.WRONLY | O.TRUNC);
        if (op.data.length > 0) {
          FS.write(s, op.data, 0, op.data.length, 0);
        }
        FS.close(s);
        return true;
      }

      case "renameFile":
      case "renameDir":
      case "renameSymlink": {
        FS.rename(rw(op.oldPath), rw(op.newPath));
        return true;
      }

      case "unlink":
      case "unlinkSymlink": {
        FS.unlink(rw(op.path));
        return true;
      }

      case "mkdir": {
        FS.mkdir(rw(op.path), 0o777);
        return true;
      }

      case "rmdir": {
        FS.rmdir(rw(op.path));
        return true;
      }

      case "symlink": {
        FS.symlink(rw(op.target), rw(op.path));
        return true;
      }

      case "appendWrite": {
        const s = FS.open(rw(op.path), O.WRONLY | O.APPEND);
        FS.write(s, op.data, 0, op.data.length);
        FS.close(s);
        return true;
      }

      case "chmod": {
        FS.chmod(rw(op.path), op.mode);
        return true;
      }

      case "openFd": {
        const s = FS.open(rw(op.path), O.RDWR);
        streams.set(op.fdId, s);
        return true;
      }

      case "writeFd": {
        const s = streams.get(op.fdId);
        if (!s) return false;
        FS.write(s, op.data, 0, op.data.length);
        return true;
      }

      case "seekFd": {
        const s = streams.get(op.fdId);
        if (!s) return false;
        FS.llseek(s, op.offset, op.whence);
        return true;
      }

      case "closeFd": {
        const s = streams.get(op.fdId);
        if (!s) return false;
        FS.close(s);
        streams.delete(op.fdId);
        return true;
      }

      case "allocate": {
        const s = FS.open(rw(op.path), O.RDWR);
        s.stream_ops.allocate(s, op.offset, op.length);
        FS.close(s);
        return true;
      }

      case "utime": {
        FS.utime(rw(op.path), op.atime, op.mtime);
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
      // O_CREAT | O_TRUNC on an existing file preserves its mode
      // but O_TRUNC modifies the file, invalidating utime-set timestamps
      const mode = existing ? existing.mode : 0o100666;
      model.files.set(op.path, { data: new Uint8Array(op.data), mode, atime: null, mtime: null });
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
      // Write updates mtime, invalidating explicit utime
      file.mtime = null;
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
      // Truncate updates mtime
      file.mtime = null;
      break;
    }

    case "overwrite": {
      const file = model.files.get(op.path);
      if (!file) break;
      file.data = new Uint8Array(op.data);
      // O_TRUNC + write updates mtime
      file.mtime = null;
      break;
    }

    case "renameFile": {
      const file = model.files.get(op.oldPath);
      if (!file) break;
      // If destination has an existing file, FDs pointing at it become orphaned
      // (their inode is being replaced, but the FD still references the old inode)
      if (model.files.has(op.newPath)) {
        for (const [fdId, fd] of [...model.openFds]) {
          if (fd.path === op.newPath) model.openFds.delete(fdId);
        }
      }
      model.files.delete(op.newPath);
      // If destination is a symlink, it's replaced
      model.symlinks.delete(op.newPath);
      model.files.delete(op.oldPath);
      model.files.set(op.newPath, file);
      // Update any open FDs pointing at the source path to follow the rename
      for (const fd of model.openFds.values()) {
        if (fd.path === op.oldPath) fd.path = op.newPath;
      }
      break;
    }

    case "unlink":
      model.files.delete(op.path);
      // Remove any open FDs pointing at the unlinked path (data is orphaned)
      for (const [fdId, fd] of [...model.openFds]) {
        if (fd.path === op.path) model.openFds.delete(fdId);
      }
      break;

    case "mkdir":
      model.dirs.set(op.path, { mode: 0o40777 });
      break;

    case "rmdir":
      model.dirs.delete(op.path);
      break;

    case "symlink":
      // If destination already exists as a symlink, it would error; but
      // we let the FS error and skip the model update via the success check
      model.symlinks.set(op.path, { target: op.target });
      break;

    case "unlinkSymlink":
      model.symlinks.delete(op.path);
      break;

    case "renameSymlink": {
      const symlink = model.symlinks.get(op.oldPath);
      if (!symlink) break;
      // Destination could be an existing file, symlink, or empty dir
      model.files.delete(op.newPath);
      model.symlinks.delete(op.newPath);
      model.symlinks.delete(op.oldPath);
      model.symlinks.set(op.newPath, symlink);
      break;
    }

    case "renameDir": {
      const oldPrefix = op.oldPath + "/";
      // Move files under old dir
      for (const [path, state] of [...model.files]) {
        if (path.startsWith(oldPrefix)) {
          const newFilePath = op.newPath + path.slice(op.oldPath.length);
          model.files.delete(path);
          model.files.set(newFilePath, state);
        }
      }
      // Move subdirs
      for (const [dir, state] of [...model.dirs]) {
        if (dir === op.oldPath || dir.startsWith(oldPrefix)) {
          model.dirs.delete(dir);
          model.dirs.set(op.newPath + dir.slice(op.oldPath.length), state);
        }
      }
      // Move symlinks
      for (const [path, state] of [...model.symlinks]) {
        if (path.startsWith(oldPrefix)) {
          model.symlinks.delete(path);
          model.symlinks.set(op.newPath + path.slice(op.oldPath.length), state);
        }
      }
      if (!model.dirs.has(op.newPath)) {
        model.dirs.set(op.newPath, { mode: 0o40777 });
      }
      // Update any open FDs pointing at paths under the old dir
      for (const fd of model.openFds.values()) {
        if (fd.path.startsWith(oldPrefix)) {
          fd.path = op.newPath + fd.path.slice(op.oldPath.length);
        }
      }
      break;
    }

    case "appendWrite": {
      const file = model.files.get(op.path);
      if (!file) break;
      const newData = new Uint8Array(file.data.length + op.data.length);
      newData.set(file.data);
      newData.set(op.data, file.data.length);
      file.data = newData;
      // Append updates mtime
      file.mtime = null;
      break;
    }

    case "chmod": {
      const file = model.files.get(op.path);
      if (!file) break;
      // Emscripten preserves the file type bits; chmod only changes permission bits
      file.mode = (file.mode & 0o170000) | (op.mode & 0o7777);
      break;
    }

    case "openFd": {
      model.openFds.set(op.fdId, { path: op.path, position: 0 });
      model.nextFdId = op.fdId + 1;
      break;
    }

    case "writeFd": {
      const fd = model.openFds.get(op.fdId);
      if (!fd) break;
      const file = model.files.get(fd.path);
      if (!file) break;
      const pos = fd.position;
      const newSize = Math.max(file.data.length, pos + op.data.length);
      const newData = new Uint8Array(newSize);
      newData.set(file.data);
      newData.set(op.data, pos);
      file.data = newData;
      fd.position = pos + op.data.length;
      // Write via fd updates mtime
      file.mtime = null;
      break;
    }

    case "seekFd": {
      const fd = model.openFds.get(op.fdId);
      if (!fd) break;
      const file = model.files.get(fd.path);
      if (!file) break;
      if (op.whence === SEEK_SET) {
        fd.position = op.offset;
      } else if (op.whence === SEEK_CUR) {
        fd.position = fd.position + op.offset;
      }
      break;
    }

    case "closeFd": {
      model.openFds.delete(op.fdId);
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
        // Allocate that extends the file updates mtime
        file.mtime = null;
      }
      break;
    }

    case "utime": {
      const file = model.files.get(op.path);
      if (!file) break;
      file.atime = op.atime;
      file.mtime = op.mtime;
      break;
    }
  }
}

function formatOp(op: Op, index: number): string {
  switch (op.type) {
    case "createFile":
      return `[${index}] createFile(${op.path}, ${op.data.length}B)`;
    case "writeAt":
      return `[${index}] writeAt(${op.path}, offset=${op.offset}, ${op.data.length}B)`;
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
    case "symlink":
      return `[${index}] symlink(${op.target} -> ${op.path})`;
    case "unlinkSymlink":
      return `[${index}] unlinkSymlink(${op.path})`;
    case "renameSymlink":
      return `[${index}] renameSymlink(${op.oldPath} -> ${op.newPath})`;
    case "renameDir":
      return `[${index}] renameDir(${op.oldPath} -> ${op.newPath})`;
    case "appendWrite":
      return `[${index}] appendWrite(${op.path}, ${op.data.length}B)`;
    case "chmod":
      return `[${index}] chmod(${op.path}, 0o${op.mode.toString(8)})`;
    case "openFd":
      return `[${index}] openFd(${op.path}, fdId=${op.fdId})`;
    case "writeFd":
      return `[${index}] writeFd(fdId=${op.fdId}, ${op.data.length}B)`;
    case "seekFd":
      return `[${index}] seekFd(fdId=${op.fdId}, offset=${op.offset}, whence=${op.whence})`;
    case "closeFd":
      return `[${index}] closeFd(fdId=${op.fdId})`;
    case "allocate":
      return `[${index}] allocate(${op.path}, @${op.offset}, ${op.length})`;
    case "utime":
      return `[${index}] utime(${op.path}, atime=${op.atime}, mtime=${op.mtime})`;
  }
}

// ---------------------------------------------------------------
// Mount / remount helpers
// ---------------------------------------------------------------

interface TomeFSInstance {
  rawFS: any;
  tomefs: any;
}

async function createTomeFSInstance(
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

function syncfs(rawFS: any): void {
  rawFS.syncfs(false, (err: Error | null) => {
    if (err) throw err;
  });
}

// ---------------------------------------------------------------
// Verification: compare FS state against model after remount
// ---------------------------------------------------------------

function verifyAfterRemount(
  rawFS: any,
  model: FSModel,
  context: string,
): void {
  // Verify all files exist with correct contents
  for (const [path, fileState] of model.files) {
    const fsPath = rw(path);
    let stat: any;
    try {
      stat = rawFS.stat(fsPath);
    } catch (e: any) {
      throw new Error(
        `${context}: file ${path} missing after remount (expected ${fileState.data.length}B)`,
      );
    }

    expect(stat.size, `${context}: size mismatch for ${path}`).toBe(fileState.data.length);

    // Verify contents
    if (fileState.data.length > 0) {
      const buf = new Uint8Array(stat.size);
      const s = rawFS.open(fsPath, O.RDONLY);
      rawFS.read(s, buf, 0, stat.size, 0);
      rawFS.close(s);

      for (let i = 0; i < fileState.data.length; i++) {
        if (buf[i] !== fileState.data[i]) {
          const pageIdx = Math.floor(i / PAGE_SIZE);
          const pageOff = i % PAGE_SIZE;
          throw new Error(
            `${context}: content mismatch for ${path} at byte ${i} ` +
            `(page ${pageIdx}, offset ${pageOff}): ` +
            `expected=${fileState.data[i]}, got=${buf[i]}`,
          );
        }
      }
    }

    // Verify mode (permission bits)
    const expectedPerms = fileState.mode & 0o7777;
    const actualPerms = stat.mode & 0o7777;
    expect(actualPerms, `${context}: mode mismatch for ${path}`).toBe(expectedPerms);

    // Verify timestamps if explicitly set via utime
    // Compare at second granularity (model stores ms, stat returns Date)
    if (fileState.mtime !== null) {
      const expectedSec = Math.floor(fileState.mtime / 1000);
      const actualSec = Math.floor(stat.mtime.getTime() / 1000);
      expect(
        actualSec,
        `${context}: mtime mismatch for ${path} (expected=${expectedSec}s, got=${actualSec}s)`,
      ).toBe(expectedSec);
    }
    if (fileState.atime !== null) {
      const expectedSec = Math.floor(fileState.atime / 1000);
      const actualSec = Math.floor(stat.atime.getTime() / 1000);
      expect(
        actualSec,
        `${context}: atime mismatch for ${path} (expected=${expectedSec}s, got=${actualSec}s)`,
      ).toBe(expectedSec);
    }
  }

  // Verify all directories exist
  for (const [path] of model.dirs) {
    if (path === "/") continue;
    const fsPath = rw(path);
    try {
      const stat = rawFS.stat(fsPath);
      expect(rawFS.isDir(stat.mode), `${context}: ${path} should be a directory`).toBe(true);
    } catch (e: any) {
      throw new Error(`${context}: directory ${path} missing after remount`);
    }
  }

  // Verify all symlinks exist with correct targets
  for (const [path, linkState] of model.symlinks) {
    const fsPath = rw(path);
    try {
      const target = rawFS.readlink(fsPath);
      // Strip mount prefix from target
      let expected = rw(linkState.target);
      expect(target, `${context}: symlink target mismatch for ${path}`).toBe(expected);
    } catch (e: any) {
      throw new Error(`${context}: symlink ${path} missing after remount`);
    }
  }

  // Verify directory listings match (no extra files/dirs from stale metadata)
  for (const [dir] of model.dirs) {
    if (dir === "/") continue;
    const fsPath = rw(dir);
    const entries = new Set(rawFS.readdir(fsPath) as string[]);
    entries.delete(".");
    entries.delete("..");

    const expectedFiles = filesInDir(model, dir).map((p) => {
      const lastSlash = p.lastIndexOf("/");
      return p.substring(lastSlash + 1);
    });
    const expectedDirs = dirsInDir(model, dir).map((d) => {
      const lastSlash = d.lastIndexOf("/");
      return d.substring(lastSlash + 1);
    });
    const expectedLinks = symlinksInDir(model, dir).map((p) => {
      const lastSlash = p.lastIndexOf("/");
      return p.substring(lastSlash + 1);
    });
    const expectedEntries = new Set([...expectedFiles, ...expectedDirs, ...expectedLinks]);

    // Check for missing entries
    for (const name of expectedEntries) {
      expect(entries.has(name), `${context}: ${dir}/${name} missing from readdir`).toBe(true);
    }

    // Check for extra entries (stale metadata)
    for (const name of entries) {
      expect(
        expectedEntries.has(name),
        `${context}: unexpected entry ${dir}/${name} in readdir (stale metadata?)`,
      ).toBe(true);
    }
  }
}

// ---------------------------------------------------------------
// Fuzz test runner with persistence roundtrips
// ---------------------------------------------------------------

/**
 * Close all open FDs in both the real FS and the model.
 * Must be called before syncfs+remount since FDs don't survive remount.
 */
function closeAllFds(
  FS: EmscriptenFS,
  model: FSModel,
  streams: StreamMap,
): void {
  for (const [fdId, stream] of streams) {
    try {
      FS.close(stream);
    } catch {
      // Ignore errors from already-closed or invalid streams
    }
    model.openFds.delete(fdId);
  }
  streams.clear();
}

async function runPersistenceRoundtrip(
  seed: number,
  numOps: number,
  maxPages: number,
  remountInterval: number,
): Promise<void> {
  const rng = new Rng(seed);
  const model = newModel();
  const backend = new SyncMemoryBackend();

  let instance = await createTomeFSInstance(backend, maxPages);
  let streams: StreamMap = new Map();
  const ops: string[] = [];

  for (let i = 0; i < numOps; i++) {
    const op = generateOp(rng, model);
    const desc = formatOp(op, i);
    ops.push(desc);

    const success = execOp(instance.rawFS, op, streams);
    if (success) {
      updateModel(model, op);
    }

    // Periodically perform a persistence roundtrip
    if ((i + 1) % remountInterval === 0 && model.files.size > 0) {
      // Close all open FDs before syncfs (FDs don't survive remount)
      closeAllFds(instance.rawFS, model, streams);

      // Persist current state
      syncfs(instance.rawFS);

      // Create a fresh Emscripten module and mount tomefs with the same backend.
      // This exercises the full restoreTree path: reading metadata from the
      // backend, creating nodes, verifying page counts, and computing file sizes.
      instance = await createTomeFSInstance(backend, maxPages);
      streams = new Map();

      // Verify all data survived the roundtrip
      const context = `remount after op ${i} (seed ${seed})`;
      try {
        verifyAfterRemount(instance.rawFS, model, context);
      } catch (e) {
        // Include recent ops for debugging
        const recentOps = ops.slice(Math.max(0, ops.length - 20));
        throw new Error(
          `${(e as Error).message}\n\nRecent ops:\n${recentOps.join("\n")}`,
        );
      }
    }
  }

  // Close any remaining FDs before final roundtrip
  closeAllFds(instance.rawFS, model, streams);

  // Final roundtrip: persist and verify one last time
  syncfs(instance.rawFS);
  instance = await createTomeFSInstance(backend, maxPages);
  const context = `final remount (seed ${seed})`;
  try {
    verifyAfterRemount(instance.rawFS, model, context);
  } catch (e) {
    const recentOps = ops.slice(Math.max(0, ops.length - 20));
    throw new Error(
      `${(e as Error).message}\n\nRecent ops:\n${recentOps.join("\n")}`,
    );
  }
}

// ---------------------------------------------------------------
// Test cases
// ---------------------------------------------------------------

describe("fuzz: persistence roundtrip testing", () => {
  describe("tiny cache (4 pages) — maximum eviction pressure", () => {
    const CACHE = 4;
    const OPS = 60;
    const INTERVAL = 15;

    it("seed 10001 @fast", async () => {
      await runPersistenceRoundtrip(10001, OPS, CACHE, INTERVAL);
    }, 30_000);

    it("seed 10002", async () => {
      await runPersistenceRoundtrip(10002, OPS, CACHE, INTERVAL);
    }, 30_000);

    it("seed 10003", async () => {
      await runPersistenceRoundtrip(10003, OPS, CACHE, INTERVAL);
    }, 30_000);

    it("seed 10004", async () => {
      await runPersistenceRoundtrip(10004, OPS, CACHE, INTERVAL);
    }, 30_000);
  });

  describe("small cache (16 pages) — moderate eviction", () => {
    const CACHE = 16;
    const OPS = 80;
    const INTERVAL = 20;

    it("seed 20001 @fast", async () => {
      await runPersistenceRoundtrip(20001, OPS, CACHE, INTERVAL);
    }, 30_000);

    it("seed 20002", async () => {
      await runPersistenceRoundtrip(20002, OPS, CACHE, INTERVAL);
    }, 30_000);

    it("seed 20003", async () => {
      await runPersistenceRoundtrip(20003, OPS, CACHE, INTERVAL);
    }, 30_000);
  });

  describe("medium cache (64 pages) — partial eviction", () => {
    const CACHE = 64;
    const OPS = 100;
    const INTERVAL = 25;

    it("seed 30001 @fast", async () => {
      await runPersistenceRoundtrip(30001, OPS, CACHE, INTERVAL);
    }, 30_000);

    it("seed 30002", async () => {
      await runPersistenceRoundtrip(30002, OPS, CACHE, INTERVAL);
    }, 30_000);
  });

  describe("large cache (4096 pages) — no eviction baseline", () => {
    const CACHE = 4096;
    const OPS = 80;
    const INTERVAL = 20;

    it("seed 40001", async () => {
      await runPersistenceRoundtrip(40001, OPS, CACHE, INTERVAL);
    }, 30_000);
  });

  describe("frequent remounts — stress persist/restore path", () => {
    // Remount every 5 ops to maximize coverage of the persist/restore cycle
    it("seed 50001, tiny cache, remount every 5 ops @fast", async () => {
      await runPersistenceRoundtrip(50001, 50, 4, 5);
    }, 30_000);

    it("seed 50002, small cache, remount every 5 ops", async () => {
      await runPersistenceRoundtrip(50002, 60, 16, 5);
    }, 30_000);

    it("seed 50003, tiny cache, remount every 3 ops", async () => {
      await runPersistenceRoundtrip(50003, 45, 4, 3);
    }, 30_000);
  });

  describe("extended sequences — long operation chains with remounts", () => {
    it("200 ops, tiny cache, seed 60001", async () => {
      await runPersistenceRoundtrip(60001, 200, 4, 20);
    }, 60_000);

    it("200 ops, small cache, seed 60002", async () => {
      await runPersistenceRoundtrip(60002, 200, 16, 25);
    }, 60_000);
  });

  describe("FD operations — write via open descriptors across remounts", () => {
    it("seed 70001, tiny cache, FD writes + seek + remount @fast", async () => {
      await runPersistenceRoundtrip(70001, 80, 4, 15);
    }, 30_000);

    it("seed 70002, small cache, interleaved FD and path writes", async () => {
      await runPersistenceRoundtrip(70002, 100, 16, 20);
    }, 30_000);

    it("seed 70003, tiny cache, frequent remount with FDs", async () => {
      await runPersistenceRoundtrip(70003, 60, 4, 5);
    }, 30_000);

    it("seed 70004, medium cache, long FD sequence", async () => {
      await runPersistenceRoundtrip(70004, 150, 64, 25);
    }, 60_000);
  });

  describe("allocate + utime — metadata persistence across remounts", () => {
    it("seed 80001, tiny cache, allocate + utime + remount @fast", async () => {
      await runPersistenceRoundtrip(80001, 80, 4, 15);
    }, 30_000);

    it("seed 80002, small cache, allocate extends + truncate shrinks", async () => {
      await runPersistenceRoundtrip(80002, 100, 16, 20);
    }, 30_000);

    it("seed 80003, tiny cache, frequent remount stress", async () => {
      await runPersistenceRoundtrip(80003, 60, 4, 3);
    }, 30_000);

    it("seed 80004, large cache, utime + allocate baseline", async () => {
      await runPersistenceRoundtrip(80004, 80, 4096, 20);
    }, 30_000);
  });
});
