/**
 * Randomized differential fuzz tests for tomefs.
 *
 * Generates random sequences of filesystem operations and executes them
 * against both MEMFS (reference) and tomefs. After each operation, compares
 * observable state (file contents, sizes, directory listings, errors) to
 * verify tomefs behaves identically to MEMFS.
 *
 * Uses a seeded PRNG for reproducibility — failing seeds can be replayed.
 * Runs at multiple cache pressure levels to maximize eviction coverage.
 *
 * Ethos §8: "look for new sources of conformance tests beyond the Emscripten
 * suite — filesystem fuzzers, database-specific FS stress tests"
 * Ethos §9: "Write tests designed to break tomefs specifically"
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
// Seeded PRNG (xorshift128+) for reproducible random sequences
// ---------------------------------------------------------------

class Rng {
  private s0: number;
  private s1: number;

  constructor(seed: number) {
    // Initialize state from seed using splitmix32
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

  /** Returns a random integer in [0, 2^32). */
  next(): number {
    let s0 = this.s0;
    let s1 = this.s1;
    const result = (s0 + s1) >>> 0;
    s1 ^= s0;
    this.s0 = ((s0 << 26) | (s0 >>> 6)) ^ s1 ^ (s1 << 9);
    this.s1 = (s1 << 13) | (s1 >>> 19);
    return result;
  }

  /** Returns a random integer in [0, max). */
  int(max: number): number {
    return this.next() % max;
  }

  /** Pick a random element from an array. */
  pick<T>(arr: T[]): T {
    return arr[this.int(arr.length)];
  }

  /** Returns random bytes of the given length. */
  bytes(length: number): Uint8Array {
    const buf = new Uint8Array(length);
    for (let i = 0; i < length; i++) {
      buf[i] = this.next() & 0xff;
    }
    return buf;
  }
}

// ---------------------------------------------------------------
// Filesystem state model (tracks what exists for valid op generation)
// ---------------------------------------------------------------

interface OpenFd {
  id: number;       // unique id for the fd slot
  path: string;     // path at time of open (may have been renamed since)
  currentPath: string; // tracks the file's current path after renames
}

interface FSModel {
  files: Map<string, number>; // path -> size
  dirs: Set<string>;          // directory paths
  symlinks: Map<string, string>; // link path -> target
  openFds: Map<number, OpenFd>; // fd slot id -> open fd info
  nextFdId: number;
}

function newModel(): FSModel {
  return { files: new Map(), dirs: new Set(["/"]), symlinks: new Map(), openFds: new Map(), nextFdId: 0 };
}

/** List files in a specific directory. */
function filesInDir(model: FSModel, dir: string): string[] {
  const prefix = dir === "/" ? "/" : dir + "/";
  return [...model.files.keys()].filter((p) => {
    if (!p.startsWith(prefix)) return false;
    const rest = p.slice(prefix.length);
    return !rest.includes("/"); // direct children only
  });
}

/** List subdirectories of a directory. */
function dirsInDir(model: FSModel, dir: string): string[] {
  const prefix = dir === "/" ? "/" : dir + "/";
  return [...model.dirs].filter((d) => {
    if (d === dir) return false;
    if (!d.startsWith(prefix)) return false;
    const rest = d.slice(prefix.length);
    return !rest.includes("/");
  });
}

/** List symlinks in a specific directory. */
function symlinksInDir(model: FSModel, dir: string): string[] {
  const prefix = dir === "/" ? "/" : dir + "/";
  return [...model.symlinks.keys()].filter((p) => {
    if (!p.startsWith(prefix)) return false;
    const rest = p.slice(prefix.length);
    return !rest.includes("/");
  });
}

/** Check if a directory is empty (no files, subdirs, or symlinks). */
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
  | { type: "readFile"; path: string }
  | { type: "truncate"; path: string; size: number }
  | { type: "renameFile"; oldPath: string; newPath: string }
  | { type: "unlink"; path: string }
  | { type: "mkdir"; path: string }
  | { type: "rmdir"; path: string }
  | { type: "renameDir"; oldPath: string; newPath: string }
  | { type: "overwrite"; path: string; data: Uint8Array }
  | { type: "chmod"; path: string; mode: number }
  | { type: "utime"; path: string; atime: number; mtime: number }
  | { type: "symlink"; target: string; path: string }
  | { type: "readlink"; path: string }
  | { type: "unlinkSymlink"; path: string }
  | { type: "readThroughSymlink"; path: string; realPath: string }
  | { type: "openFd"; path: string; fdId: number }
  | { type: "readFd"; fdId: number }
  | { type: "writeFd"; fdId: number; data: Uint8Array; offset: number }
  | { type: "closeFd"; fdId: number }
  | { type: "dupFd"; srcFdId: number; newFdId: number }
  | { type: "seekFd"; fdId: number; offset: number; whence: number }
  | { type: "appendWrite"; path: string; data: Uint8Array }
  | { type: "ftruncateFd"; fdId: number; size: number }
  | { type: "syncfs" };

const DIR_NAMES = ["alpha", "beta", "gamma", "delta"];
const FILE_NAMES = ["a.dat", "b.dat", "c.dat", "d.dat", "e.dat", "f.dat"];
const LINK_NAMES = ["lnk1", "lnk2", "lnk3"];

function generateOp(rng: Rng, model: FSModel): Op {
  const allFiles = [...model.files.keys()];
  const allDirs = [...model.dirs].filter((d) => d !== "/");
  const allContainerDirs = [...model.dirs];
  const allSymlinks = [...model.symlinks.keys()];
  // Symlinks that point to files that still exist (for reading through)
  const validSymlinks = allSymlinks.filter((p) => {
    const target = model.symlinks.get(p)!;
    return model.files.has(target);
  });

  // Weight operation types based on state
  const weights: Array<[string, number]> = [
    ["createFile", 20],
    ["mkdir", 10],
    ["writeAt", allFiles.length > 0 ? 15 : 0],
    ["readFile", allFiles.length > 0 ? 10 : 0],
    ["truncate", allFiles.length > 0 ? 10 : 0],
    ["renameFile", allFiles.length > 0 ? 10 : 0],
    ["unlink", allFiles.length > 0 ? 8 : 0],
    ["overwrite", allFiles.length > 0 ? 8 : 0],
    ["chmod", allFiles.length > 0 ? 6 : 0],
    ["utime", allFiles.length > 0 ? 6 : 0],
    ["rmdir", allDirs.filter((d) => isDirEmpty(model, d)).length > 0 ? 5 : 0],
    ["renameDir", allDirs.length > 0 ? 5 : 0],
    ["symlink", allFiles.length > 0 ? 8 : 0],
    ["readlink", allSymlinks.length > 0 ? 4 : 0],
    ["unlinkSymlink", allSymlinks.length > 0 ? 4 : 0],
    ["readThroughSymlink", validSymlinks.length > 0 ? 6 : 0],
    ["openFd", allFiles.length > 0 && model.openFds.size < 4 ? 8 : 0],
    ["readFd", model.openFds.size > 0 ? 8 : 0],
    ["writeFd", model.openFds.size > 0 ? 8 : 0],
    ["closeFd", model.openFds.size > 0 ? 6 : 0],
    ["dupFd", model.openFds.size > 0 && model.openFds.size < 8 ? 6 : 0],
    ["seekFd", model.openFds.size > 0 ? 6 : 0],
    ["appendWrite", allFiles.length > 0 ? 8 : 0],
    ["ftruncateFd", model.openFds.size > 0 ? 5 : 0],
    ["syncfs", 3],
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
      // Vary sizes around page boundaries
      const sizeChoices = [0, 1, 100, PAGE_SIZE - 1, PAGE_SIZE, PAGE_SIZE + 1, PAGE_SIZE * 2, PAGE_SIZE * 3 + 137];
      const size = rng.pick(sizeChoices);
      const data = rng.bytes(size);
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
      const currentSize = model.files.get(path) ?? 0;
      // Write at various offsets including beyond current size (extending)
      const maxOffset = currentSize + PAGE_SIZE;
      const offset = rng.int(maxOffset + 1);
      const sizeChoices = [1, 50, PAGE_SIZE - 1, PAGE_SIZE, PAGE_SIZE + 1, PAGE_SIZE * 2];
      const size = rng.pick(sizeChoices);
      const data = rng.bytes(size);
      return { type: "writeAt", path, offset, data };
    }

    case "readFile": {
      return { type: "readFile", path: rng.pick(allFiles) };
    }

    case "truncate": {
      const path = rng.pick(allFiles);
      const currentSize = model.files.get(path) ?? 0;
      // Truncate to various sizes: 0, smaller, same, larger
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

    case "renameFile": {
      const oldPath = rng.pick(allFiles);
      const dir = rng.pick(allContainerDirs);
      const name = rng.pick(FILE_NAMES);
      const newPath = dir === "/" ? `/${name}` : `${dir}/${name}`;
      return { type: "renameFile", oldPath, newPath };
    }

    case "unlink": {
      return { type: "unlink", path: rng.pick(allFiles) };
    }

    case "overwrite": {
      const path = rng.pick(allFiles);
      const sizeChoices = [0, 1, 100, PAGE_SIZE, PAGE_SIZE * 2 + 77];
      const size = rng.pick(sizeChoices);
      const data = rng.bytes(size);
      return { type: "overwrite", path, data };
    }

    case "chmod": {
      const path = rng.pick(allFiles);
      const modeChoices = [0o444, 0o555, 0o644, 0o666, 0o755, 0o777];
      return { type: "chmod", path, mode: rng.pick(modeChoices) };
    }

    case "utime": {
      const path = rng.pick(allFiles);
      // Use fixed timestamps for deterministic comparison
      const baseTime = 1000000000000; // ~2001
      const atime = baseTime + rng.int(1000000000);
      const mtime = baseTime + rng.int(1000000000);
      return { type: "utime", path, atime, mtime };
    }

    case "rmdir": {
      const emptyDirs = allDirs.filter((d) => isDirEmpty(model, d));
      if (emptyDirs.length === 0) return { type: "readFile", path: rng.pick(allFiles) };
      return { type: "rmdir", path: rng.pick(emptyDirs) };
    }

    case "renameDir": {
      const oldPath = rng.pick(allDirs);
      const parent = rng.pick(allContainerDirs);
      const name = rng.pick(DIR_NAMES);
      const newPath = parent === "/" ? `/${name}` : `${parent}/${name}`;
      // Don't rename into self
      if (newPath.startsWith(oldPath + "/") || newPath === oldPath) {
        return { type: "syncfs" };
      }
      return { type: "renameDir", oldPath, newPath };
    }

    case "symlink": {
      const dir = rng.pick(allContainerDirs);
      const name = rng.pick(LINK_NAMES);
      const path = dir === "/" ? `/${name}` : `${dir}/${name}`;
      // Point at a random existing file
      const target = rng.pick(allFiles);
      return { type: "symlink", target, path };
    }

    case "readlink":
      return { type: "readlink", path: rng.pick(allSymlinks) };

    case "unlinkSymlink":
      return { type: "unlinkSymlink", path: rng.pick(allSymlinks) };

    case "readThroughSymlink": {
      const linkPath = rng.pick(validSymlinks);
      const realPath = model.symlinks.get(linkPath)!;
      return { type: "readThroughSymlink", path: linkPath, realPath };
    }

    case "openFd": {
      // Open a random existing file and keep the fd
      const path = rng.pick(allFiles);
      const fdId = model.nextFdId;
      return { type: "openFd", path, fdId };
    }

    case "readFd": {
      const fds = [...model.openFds.keys()];
      return { type: "readFd", fdId: rng.pick(fds) };
    }

    case "writeFd": {
      const fds = [...model.openFds.keys()];
      const fdId = rng.pick(fds);
      const sizeChoices = [1, 50, PAGE_SIZE - 1, PAGE_SIZE, PAGE_SIZE + 1];
      const size = rng.pick(sizeChoices);
      const data = rng.bytes(size);
      // Write at beginning (to test data overwrite via fd)
      const fdInfo = model.openFds.get(fdId)!;
      const currentSize = model.files.get(fdInfo.currentPath) ?? 0;
      const maxOffset = currentSize + PAGE_SIZE;
      const offset = rng.int(maxOffset + 1);
      return { type: "writeFd", fdId, data, offset };
    }

    case "closeFd": {
      const fds = [...model.openFds.keys()];
      return { type: "closeFd", fdId: rng.pick(fds) };
    }

    case "dupFd": {
      const fds = [...model.openFds.keys()];
      const srcFdId = rng.pick(fds);
      const newFdId = model.nextFdId;
      return { type: "dupFd", srcFdId, newFdId };
    }

    case "seekFd": {
      const fds = [...model.openFds.keys()];
      const fdId = rng.pick(fds);
      const whenceChoices = [SEEK_SET, SEEK_CUR, SEEK_END];
      const whence = rng.pick(whenceChoices);
      const fdInfo = model.openFds.get(fdId)!;
      const currentSize = model.files.get(fdInfo.currentPath) ?? 0;
      // Generate an offset that makes sense for the whence mode
      let offset: number;
      if (whence === SEEK_SET) {
        offset = rng.int(currentSize + PAGE_SIZE + 1);
      } else if (whence === SEEK_CUR) {
        // SEEK_CUR: offset relative to current position (may be negative)
        offset = rng.int(PAGE_SIZE * 2 + 1) - PAGE_SIZE;
      } else {
        // SEEK_END: offset relative to end (typically 0 or negative)
        offset = rng.int(currentSize + 1) * -1;
      }
      return { type: "seekFd", fdId, offset, whence };
    }

    case "appendWrite": {
      const path = rng.pick(allFiles);
      const sizeChoices = [1, 50, PAGE_SIZE - 1, PAGE_SIZE, PAGE_SIZE + 1];
      const size = rng.pick(sizeChoices);
      const data = rng.bytes(size);
      return { type: "appendWrite", path, data };
    }

    case "ftruncateFd": {
      const fds = [...model.openFds.keys()];
      const fdId = rng.pick(fds);
      const fdInfo = model.openFds.get(fdId)!;
      const currentSize = model.files.get(fdInfo.currentPath) ?? 0;
      const sizeChoices = [
        0,
        Math.max(0, currentSize - PAGE_SIZE),
        Math.max(0, currentSize - 1),
        currentSize,
        currentSize + 1,
        currentSize + PAGE_SIZE,
      ];
      return { type: "ftruncateFd", fdId, size: rng.pick(sizeChoices) };
    }

    case "syncfs":
    default:
      return { type: "syncfs" };
  }
}

// ---------------------------------------------------------------
// Execute an operation on an FS, catching errors
// ---------------------------------------------------------------

interface OpResult {
  error: string | null;
  data?: Uint8Array;
  size?: number;
}

/** Tracks actual FS stream objects for open fds on a specific FS instance. */
type FdStreamMap = Map<number, any>; // fdId -> FS stream object

function execOp(FS: EmscriptenFS, op: Op, syncfsFn?: () => void, fdStreams?: FdStreamMap): OpResult {
  try {
    switch (op.type) {
      case "createFile": {
        const s = FS.open(op.path, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
        if (op.data.length > 0) {
          FS.write(s, op.data, 0, op.data.length, 0);
        }
        FS.close(s);
        return { error: null, size: op.data.length };
      }

      case "writeAt": {
        const s = FS.open(op.path, O.RDWR);
        FS.write(s, op.data, 0, op.data.length, op.offset);
        const stat = FS.stat(op.path);
        FS.close(s);
        return { error: null, size: stat.size };
      }

      case "readFile": {
        const stat = FS.stat(op.path);
        const buf = new Uint8Array(stat.size);
        if (stat.size > 0) {
          const s = FS.open(op.path, O.RDONLY);
          FS.read(s, buf, 0, stat.size, 0);
          FS.close(s);
        }
        return { error: null, data: buf, size: stat.size };
      }

      case "truncate": {
        FS.truncate(op.path, op.size);
        return { error: null, size: op.size };
      }

      case "renameFile":
      case "renameDir": {
        FS.rename(op.oldPath, op.newPath);
        return { error: null };
      }

      case "unlink": {
        FS.unlink(op.path);
        return { error: null };
      }

      case "overwrite": {
        const s = FS.open(op.path, O.WRONLY | O.TRUNC);
        if (op.data.length > 0) {
          FS.write(s, op.data, 0, op.data.length, 0);
        }
        FS.close(s);
        return { error: null, size: op.data.length };
      }

      case "chmod": {
        FS.chmod(op.path, op.mode);
        return { error: null };
      }

      case "utime": {
        FS.utime(op.path, op.atime, op.mtime);
        return { error: null };
      }

      case "mkdir": {
        FS.mkdir(op.path, 0o777);
        return { error: null };
      }

      case "rmdir": {
        FS.rmdir(op.path);
        return { error: null };
      }

      case "symlink": {
        FS.symlink(op.target, op.path);
        return { error: null };
      }

      case "readlink": {
        const target = FS.readlink(op.path);
        return { error: null, data: new TextEncoder().encode(target) };
      }

      case "unlinkSymlink": {
        FS.unlink(op.path);
        return { error: null };
      }

      case "readThroughSymlink": {
        // Read file contents through the symlink path
        const stat = FS.stat(op.path);
        const buf = new Uint8Array(stat.size);
        if (stat.size > 0) {
          const s = FS.open(op.path, O.RDONLY);
          FS.read(s, buf, 0, stat.size, 0);
          FS.close(s);
        }
        return { error: null, data: buf, size: stat.size };
      }

      case "openFd": {
        const stream = FS.open(op.path, O.RDWR);
        if (fdStreams) fdStreams.set(op.fdId, stream);
        return { error: null };
      }

      case "readFd": {
        const stream = fdStreams?.get(op.fdId);
        if (!stream) return { error: "no-fd" };
        // Read entire file from position 0 using fstat for size
        const fstatResult = FS.fstat(stream.fd ?? stream);
        const buf = new Uint8Array(fstatResult.size);
        if (fstatResult.size > 0) {
          FS.read(stream, buf, 0, fstatResult.size, 0);
        }
        return { error: null, data: buf, size: fstatResult.size };
      }

      case "writeFd": {
        const stream = fdStreams?.get(op.fdId);
        if (!stream) return { error: "no-fd" };
        FS.write(stream, op.data, 0, op.data.length, op.offset);
        const fstatResult = FS.fstat(stream.fd ?? stream);
        return { error: null, size: fstatResult.size };
      }

      case "closeFd": {
        const stream = fdStreams?.get(op.fdId);
        if (!stream) return { error: "no-fd" };
        FS.close(stream);
        fdStreams?.delete(op.fdId);
        return { error: null };
      }

      case "dupFd": {
        const srcStream = fdStreams?.get(op.srcFdId);
        if (!srcStream) return { error: "no-fd" };
        const dupStream = FS.dupStream(srcStream);
        if (fdStreams) fdStreams.set(op.newFdId, dupStream);
        return { error: null };
      }

      case "seekFd": {
        const stream = fdStreams?.get(op.fdId);
        if (!stream) return { error: "no-fd" };
        const pos = FS.llseek(stream, op.offset, op.whence);
        return { error: null, size: pos };
      }

      case "appendWrite": {
        const s = FS.open(op.path, O.WRONLY | O.APPEND);
        FS.write(s, op.data, 0, op.data.length);
        const stat = FS.stat(op.path);
        FS.close(s);
        return { error: null, size: stat.size };
      }

      case "ftruncateFd": {
        const stream = fdStreams?.get(op.fdId);
        if (!stream) return { error: "no-fd" };
        FS.ftruncate(stream.fd ?? stream, op.size);
        const fstatResult = FS.fstat(stream.fd ?? stream);
        return { error: null, size: fstatResult.size };
      }

      case "syncfs": {
        if (syncfsFn) syncfsFn();
        return { error: null };
      }

      default:
        return { error: null };
    }
  } catch (e: unknown) {
    if (e instanceof Error && "errno" in e) {
      return { error: `errno:${(e as any).errno}` };
    }
    return { error: (e as Error).message };
  }
}

/** Update the model after a successful operation. */
function updateModel(model: FSModel, op: Op, result: OpResult): void {
  if (result.error) return;

  switch (op.type) {
    case "createFile":
      model.files.set(op.path, op.data.length);
      break;
    case "writeAt":
      model.files.set(op.path, Math.max(model.files.get(op.path) ?? 0, op.offset + op.data.length));
      break;
    case "truncate":
      model.files.set(op.path, op.size);
      break;
    case "overwrite":
      model.files.set(op.path, op.data.length);
      break;
    case "unlink":
      model.files.delete(op.path);
      break;
    case "mkdir":
      model.dirs.add(op.path);
      break;
    case "rmdir":
      model.dirs.delete(op.path);
      break;
    case "renameFile": {
      const size = model.files.get(op.oldPath) ?? 0;
      model.files.delete(op.oldPath);
      // If target was a file, it's replaced
      model.files.set(op.newPath, size);
      // Update open fds that track the renamed file
      for (const fd of model.openFds.values()) {
        if (fd.currentPath === op.oldPath) {
          fd.currentPath = op.newPath;
        }
      }
      break;
    }
    case "renameDir": {
      const oldPrefix = op.oldPath + "/";
      // Move all files under old dir
      for (const [path, size] of [...model.files]) {
        if (path.startsWith(oldPrefix)) {
          model.files.delete(path);
          model.files.set(op.newPath + path.slice(op.oldPath.length), size);
        }
      }
      // Move all subdirs under old dir
      for (const dir of [...model.dirs]) {
        if (dir === op.oldPath || dir.startsWith(oldPrefix)) {
          model.dirs.delete(dir);
          model.dirs.add(op.newPath + dir.slice(op.oldPath.length));
        }
      }
      // Move all symlinks under old dir
      for (const [path, target] of [...model.symlinks]) {
        if (path.startsWith(oldPrefix)) {
          model.symlinks.delete(path);
          model.symlinks.set(op.newPath + path.slice(op.oldPath.length), target);
        }
      }
      // Update open fds that track files under the renamed dir
      for (const fd of model.openFds.values()) {
        if (fd.currentPath.startsWith(oldPrefix)) {
          fd.currentPath = op.newPath + fd.currentPath.slice(op.oldPath.length);
        }
      }
      // If target was an existing empty dir, it's replaced
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
    case "openFd":
      model.openFds.set(op.fdId, { id: op.fdId, path: op.path, currentPath: op.path });
      model.nextFdId++;
      break;
    case "writeFd": {
      const fdInfo = model.openFds.get(op.fdId);
      if (fdInfo) {
        const currentSize = model.files.get(fdInfo.currentPath) ?? 0;
        model.files.set(fdInfo.currentPath, Math.max(currentSize, op.offset + op.data.length));
      }
      break;
    }
    case "closeFd":
      model.openFds.delete(op.fdId);
      break;
    case "dupFd": {
      const srcFd = model.openFds.get(op.srcFdId);
      if (srcFd) {
        model.openFds.set(op.newFdId, { id: op.newFdId, path: srcFd.path, currentPath: srcFd.currentPath });
        model.nextFdId++;
      }
      break;
    }
    case "appendWrite": {
      const currentSize = model.files.get(op.path) ?? 0;
      model.files.set(op.path, currentSize + op.data.length);
      break;
    }
    case "ftruncateFd": {
      const fdInfo = model.openFds.get(op.fdId);
      if (fdInfo) {
        model.files.set(fdInfo.currentPath, op.size);
      }
      break;
    }
    // readFd, readlink, readThroughSymlink, seekFd don't modify file state
  }
}

/** Format an operation for error reporting. */
function formatOp(op: Op, index: number): string {
  switch (op.type) {
    case "createFile":
      return `[${index}] createFile(${op.path}, ${op.data.length}B)`;
    case "writeAt":
      return `[${index}] writeAt(${op.path}, offset=${op.offset}, ${op.data.length}B)`;
    case "readFile":
      return `[${index}] readFile(${op.path})`;
    case "truncate":
      return `[${index}] truncate(${op.path}, ${op.size})`;
    case "renameFile":
      return `[${index}] renameFile(${op.oldPath} -> ${op.newPath})`;
    case "renameDir":
      return `[${index}] renameDir(${op.oldPath} -> ${op.newPath})`;
    case "unlink":
      return `[${index}] unlink(${op.path})`;
    case "overwrite":
      return `[${index}] overwrite(${op.path}, ${op.data.length}B)`;
    case "chmod":
      return `[${index}] chmod(${op.path}, 0o${op.mode.toString(8)})`;
    case "utime":
      return `[${index}] utime(${op.path}, atime=${op.atime}, mtime=${op.mtime})`;
    case "mkdir":
      return `[${index}] mkdir(${op.path})`;
    case "rmdir":
      return `[${index}] rmdir(${op.path})`;
    case "symlink":
      return `[${index}] symlink(${op.target} -> ${op.path})`;
    case "readlink":
      return `[${index}] readlink(${op.path})`;
    case "unlinkSymlink":
      return `[${index}] unlinkSymlink(${op.path})`;
    case "readThroughSymlink":
      return `[${index}] readThroughSymlink(${op.path} -> ${op.realPath})`;
    case "openFd":
      return `[${index}] openFd(${op.path}, fdId=${op.fdId})`;
    case "readFd":
      return `[${index}] readFd(fdId=${op.fdId})`;
    case "writeFd":
      return `[${index}] writeFd(fdId=${op.fdId}, offset=${op.offset}, ${op.data.length}B)`;
    case "closeFd":
      return `[${index}] closeFd(fdId=${op.fdId})`;
    case "dupFd":
      return `[${index}] dupFd(src=${op.srcFdId}, new=${op.newFdId})`;
    case "seekFd": {
      const whenceName = op.whence === SEEK_SET ? "SET" : op.whence === SEEK_CUR ? "CUR" : "END";
      return `[${index}] seekFd(fdId=${op.fdId}, offset=${op.offset}, SEEK_${whenceName})`;
    }
    case "appendWrite":
      return `[${index}] appendWrite(${op.path}, ${op.data.length}B)`;
    case "ftruncateFd":
      return `[${index}] ftruncateFd(fdId=${op.fdId}, size=${op.size})`;
    case "syncfs":
      return `[${index}] syncfs()`;
  }
}

// ---------------------------------------------------------------
// Harness: create paired MEMFS + tomefs instances
// ---------------------------------------------------------------

const TOME_MOUNT = "/tome";

interface DualFS {
  memFS: EmscriptenFS;
  tomeFS: EmscriptenFS;
  syncTomeFS: () => void;
}

async function createDualFS(maxPages: number): Promise<DualFS> {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );

  // MEMFS instance
  const memModule = await createModule();
  const memFS = memModule.FS as EmscriptenFS;

  // tomefs instance
  const tomeModule = await createModule();
  const rawTomeFS = tomeModule.FS;
  const backend = new SyncMemoryBackend();
  const tomefs = createTomeFS(rawTomeFS, { backend, maxPages });
  rawTomeFS.mkdir(TOME_MOUNT);
  rawTomeFS.mount(tomefs, {}, TOME_MOUNT);

  // Path-rewriting wrapper for tomefs
  const tomeFS = createTomePathRewriter(rawTomeFS);

  const syncTomeFS = () => {
    rawTomeFS.syncfs(false, (err: Error | null) => {
      if (err) throw err;
    });
  };

  return { memFS, tomeFS, syncTomeFS };
}

function createTomePathRewriter(realFS: any): EmscriptenFS {
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
    readlink(path) {
      const target = realFS.readlink(rw(path));
      // Strip mount prefix from returned target so it matches MEMFS paths
      if (target.startsWith(TOME_MOUNT + "/")) return target.slice(TOME_MOUNT.length);
      if (target === TOME_MOUNT) return "/";
      return target;
    },
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
// Compare file contents between two FSes
// ---------------------------------------------------------------

function compareFileContents(
  memFS: EmscriptenFS,
  tomeFS: EmscriptenFS,
  path: string,
  context: string,
): void {
  let memStat: any, tomeStat: any;
  let memErr: string | null = null, tomeErr: string | null = null;

  try { memStat = memFS.stat(path); } catch (e: any) { memErr = `errno:${e.errno}`; }
  try { tomeStat = tomeFS.stat(path); } catch (e: any) { tomeErr = `errno:${e.errno}`; }

  if (memErr || tomeErr) {
    expect(tomeErr, `${context}: stat error mismatch for ${path}`).toBe(memErr);
    return;
  }

  expect(tomeStat.size, `${context}: size mismatch for ${path}`).toBe(memStat.size);
  expect(tomeStat.mode, `${context}: mode mismatch for ${path}`).toBe(memStat.mode);

  if (memStat.size > 0) {
    const memBuf = new Uint8Array(memStat.size);
    const tomeBuf = new Uint8Array(tomeStat.size);

    const ms = memFS.open(path, O.RDONLY);
    memFS.read(ms, memBuf, 0, memStat.size, 0);
    memFS.close(ms);

    const ts = tomeFS.open(path, O.RDONLY);
    tomeFS.read(ts, tomeBuf, 0, tomeStat.size, 0);
    tomeFS.close(ts);

    // Compare byte-by-byte for clear error messages
    for (let i = 0; i < memStat.size; i++) {
      if (memBuf[i] !== tomeBuf[i]) {
        const pageIdx = Math.floor(i / PAGE_SIZE);
        const pageOff = i % PAGE_SIZE;
        throw new Error(
          `${context}: content mismatch for ${path} at byte ${i} ` +
          `(page ${pageIdx}, offset ${pageOff}): ` +
          `MEMFS=${memBuf[i]}, tomefs=${tomeBuf[i]}`,
        );
      }
    }
  }
}

/** Compare symlink targets between two FSes. */
function compareSymlinks(
  memFS: EmscriptenFS,
  tomeFS: EmscriptenFS,
  model: FSModel,
  context: string,
): void {
  for (const [path, _target] of model.symlinks) {
    let memErr: string | null = null, tomeErr: string | null = null;
    let memTarget: string | undefined, tomeTarget: string | undefined;

    try { memTarget = memFS.readlink(path); } catch (e: any) { memErr = `errno:${e.errno}`; }
    try { tomeTarget = tomeFS.readlink(path); } catch (e: any) { tomeErr = `errno:${e.errno}`; }

    if (memErr || tomeErr) {
      expect(tomeErr, `${context}: readlink error mismatch for ${path}`).toBe(memErr);
      continue;
    }

    expect(tomeTarget, `${context}: symlink target mismatch for ${path}`).toBe(memTarget);

    // Also verify lstat reports a symlink
    const memLstat = memFS.lstat(path);
    const tomeLstat = tomeFS.lstat(path);
    expect(tomeFS.isLink(tomeLstat.mode), `${context}: ${path} should be a symlink in tomefs`).toBe(
      memFS.isLink(memLstat.mode),
    );
  }
}

/** Compare all files and symlinks tracked by the model between two FSes. */
function compareAllFiles(
  memFS: EmscriptenFS,
  tomeFS: EmscriptenFS,
  model: FSModel,
  context: string,
): void {
  for (const path of model.files.keys()) {
    compareFileContents(memFS, tomeFS, path, context);
  }
  compareSymlinks(memFS, tomeFS, model, context);
}

// ---------------------------------------------------------------
// The actual fuzz test runner
// ---------------------------------------------------------------

async function runFuzzSequence(
  seed: number,
  numOps: number,
  maxPages: number,
): Promise<void> {
  const rng = new Rng(seed);
  const model = newModel();
  const { memFS, tomeFS, syncTomeFS } = await createDualFS(maxPages);

  // Track actual stream objects for open fds per FS
  const memFdStreams: FdStreamMap = new Map();
  const tomeFdStreams: FdStreamMap = new Map();

  // Generate ops lazily so fd-related state (nextFdId, openFds) is current.
  // Previous approach of pre-generating all ops doesn't work with stateful
  // fd tracking since fdIds depend on execution order.
  for (let i = 0; i < numOps; i++) {
    const op = generateOp(rng, model);
    const desc = formatOp(op, i);

    const memResult = execOp(memFS, op, undefined, memFdStreams);
    const tomeResult = execOp(tomeFS, op, syncTomeFS, tomeFdStreams);

    // Error behavior must match
    expect(tomeResult.error, `${desc}: error mismatch`).toBe(memResult.error);

    // Update model on success
    updateModel(model, op, memResult);

    // For utime operations, verify timestamps were set identically
    if (op.type === "utime" && !memResult.error) {
      const memStat = memFS.stat(op.path);
      const tomeStat = tomeFS.stat(op.path);
      expect(
        new Date(tomeStat.atime).getTime(),
        `${desc}: atime mismatch after utime`,
      ).toBe(new Date(memStat.atime).getTime());
      expect(
        new Date(tomeStat.mtime).getTime(),
        `${desc}: mtime mismatch after utime`,
      ).toBe(new Date(memStat.mtime).getTime());
    }

    // For seek operations, compare returned positions
    if (op.type === "seekFd" && !memResult.error) {
      expect(tomeResult.size, `${desc}: seek position mismatch`).toBe(memResult.size);
    }

    // For ftruncate and appendWrite, compare resulting file sizes
    if ((op.type === "ftruncateFd" || op.type === "appendWrite") && !memResult.error) {
      expect(tomeResult.size, `${desc}: size mismatch`).toBe(memResult.size);
    }

    // For chmod operations, verify mode was set identically
    if (op.type === "chmod" && !memResult.error) {
      const memStat = memFS.stat(op.path);
      const tomeStat = tomeFS.stat(op.path);
      expect(tomeStat.mode, `${desc}: mode mismatch after chmod`).toBe(memStat.mode);
    }

    // For read operations, compare returned data
    if ((op.type === "readFile" || op.type === "readThroughSymlink" || op.type === "readlink" || op.type === "readFd") && !memResult.error && memResult.data && tomeResult.data) {
      expect(tomeResult.data.length, `${desc}: read length mismatch`).toBe(memResult.data.length);
      for (let j = 0; j < memResult.data.length; j++) {
        if (memResult.data[j] !== tomeResult.data[j]) {
          const pageIdx = Math.floor(j / PAGE_SIZE);
          const pageOff = j % PAGE_SIZE;
          throw new Error(
            `${desc}: read data mismatch at byte ${j} ` +
            `(page ${pageIdx}, offset ${pageOff}): ` +
            `MEMFS=${memResult.data[j]}, tomefs=${tomeResult.data[j]}`,
          );
        }
      }
    }

    // Periodically do a full comparison (every 10 ops)
    if (i > 0 && i % 10 === 0) {
      compareAllFiles(memFS, tomeFS, model, `after op ${i}`);
    }
  }

  // Final full comparison
  compareAllFiles(memFS, tomeFS, model, "final");
}

// ---------------------------------------------------------------
// Test cases
// ---------------------------------------------------------------

describe("fuzz: randomized differential testing", () => {
  // Use fixed seeds for reproducibility. If a test fails, the seed
  // in the test name tells you exactly how to reproduce it.

  describe("tiny cache (4 pages = 32 KB) — maximum eviction pressure", () => {
    const CACHE = 4;
    const OPS = 80;

    it("seed 1001 @fast", async () => {
      await runFuzzSequence(1001, OPS, CACHE);
    }, 30_000);

    it("seed 2002", async () => {
      await runFuzzSequence(2002, OPS, CACHE);
    }, 30_000);

    it("seed 3003", async () => {
      await runFuzzSequence(3003, OPS, CACHE);
    }, 30_000);

    it("seed 4004", async () => {
      await runFuzzSequence(4004, OPS, CACHE);
    }, 30_000);

    it("seed 5005", async () => {
      await runFuzzSequence(5005, OPS, CACHE);
    }, 30_000);
  });

  describe("small cache (16 pages = 128 KB) — moderate eviction", () => {
    const CACHE = 16;
    const OPS = 100;

    it("seed 6006 @fast", async () => {
      await runFuzzSequence(6006, OPS, CACHE);
    }, 30_000);

    it("seed 7007", async () => {
      await runFuzzSequence(7007, OPS, CACHE);
    }, 30_000);

    it("seed 8008", async () => {
      await runFuzzSequence(8008, OPS, CACHE);
    }, 30_000);
  });

  describe("medium cache (64 pages = 512 KB) — partial fit", () => {
    const CACHE = 64;
    const OPS = 120;

    it("seed 9009 @fast", async () => {
      await runFuzzSequence(9009, OPS, CACHE);
    }, 30_000);

    it("seed 1010", async () => {
      await runFuzzSequence(1010, OPS, CACHE);
    }, 30_000);
  });

  describe("large cache (4096 pages = 32 MB) — no eviction baseline", () => {
    const CACHE = 4096;
    const OPS = 100;

    it("seed 1111", async () => {
      await runFuzzSequence(1111, OPS, CACHE);
    }, 30_000);
  });

  describe("extended sequences — stress longer operation chains", () => {
    it("200 ops under tiny cache, seed 42", async () => {
      await runFuzzSequence(42, 200, 4);
    }, 60_000);

    it("200 ops under small cache, seed 137", async () => {
      await runFuzzSequence(137, 200, 16);
    }, 60_000);
  });

  describe("page-boundary focused", () => {
    // These seeds were chosen to exercise page-boundary operations
    // heavily by running with specific PRNG states that generate
    // more writeAt operations near page boundaries.

    it("seed 31337 tiny cache", async () => {
      await runFuzzSequence(31337, 100, 4);
    }, 30_000);

    it("seed 65536 tiny cache", async () => {
      await runFuzzSequence(65536, 100, 4);
    }, 30_000);

    it("seed 8192 tiny cache", async () => {
      await runFuzzSequence(8192, 100, 4);
    }, 30_000);
  });

  describe("fd-heavy operations (dup, seek, append, ftruncate)", () => {
    // Seeds chosen to generate sequences that frequently exercise the new
    // fd-centric operations: dupFd, seekFd, appendWrite, ftruncateFd.
    // These target tomefs stream_ops code paths (dup openCount tracking,
    // llseek with SEEK_CUR/SEEK_END, O_APPEND writes, ftruncate via fd).

    it("seed 77777 tiny cache @fast", async () => {
      await runFuzzSequence(77777, 120, 4);
    }, 30_000);

    it("seed 88888 tiny cache", async () => {
      await runFuzzSequence(88888, 120, 4);
    }, 30_000);

    it("seed 99999 small cache", async () => {
      await runFuzzSequence(99999, 150, 16);
    }, 30_000);

    it("seed 12321 tiny cache", async () => {
      await runFuzzSequence(12321, 120, 4);
    }, 30_000);
  });
});
