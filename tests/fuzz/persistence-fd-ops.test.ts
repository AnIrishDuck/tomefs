/**
 * Persistence roundtrip fuzz tests targeting file descriptor operations.
 *
 * Complements persistence-roundtrip.test.ts (file-level operations) by
 * focusing on fd-based operations: open, dup, seek, positional write,
 * ftruncate, allocate, and close — combined with syncfs → remount
 * verification.
 *
 * These operations are critical for database workloads:
 * - Postgres writes WAL records through dup'd file descriptors
 * - Heap page updates use positional seeks + writes
 * - WAL recycling uses ftruncate
 * - Pre-allocation uses allocate/fallocate
 *
 * The existing persistence-roundtrip.test.ts uses file-level APIs (writeFile,
 * truncate, rename) but never exercises the fd → page cache → persistence
 * path that databases actually use. This test catches bugs where:
 * - Writes through dup'd fds aren't persisted (dirty tracking per-node)
 * - Positional seeks + writes lose data across page boundaries on remount
 * - ftruncate through a dup'd fd doesn't update metadata correctly
 * - allocate extends a file but the extension is lost on remount
 * - Close ordering (original fd vs dup'd fd) affects persistence
 *
 * Uses a seeded PRNG for reproducibility — failing seeds can be replayed.
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
import type { EmscriptenFS, EmscriptenStream } from "../harness/emscripten-fs.js";
import { O, SEEK_SET, SEEK_CUR, SEEK_END } from "../harness/emscripten-fs.js";

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
// Model: tracks expected filesystem state
// ---------------------------------------------------------------

interface FileState {
  /** Expected file contents (source of truth). */
  data: Uint8Array;
  /** File mode. */
  mode: number;
}

/** Open fd in the model. */
interface FdState {
  /** Unique id for this fd slot. */
  id: number;
  /** Path of the file this fd refers to. */
  path: string;
  /** Current seek position. */
  position: number;
  /** Whether this fd was opened with O_APPEND. */
  append: boolean;
}

interface FSModel {
  files: Map<string, FileState>;
  dirs: Set<string>;
  fds: Map<number, FdState>;
  nextFdId: number;
}

function newModel(): FSModel {
  return {
    files: new Map(),
    dirs: new Set(["/"]),
    fds: new Map(),
    nextFdId: 0,
  };
}

// ---------------------------------------------------------------
// Operation types — focused on fd-based operations
// ---------------------------------------------------------------

type Op =
  | { type: "createFile"; path: string; data: Uint8Array }
  | { type: "openFd"; path: string; fdId: number; flags: number }
  | { type: "writeFd"; fdId: number; data: Uint8Array }
  | { type: "readFd"; fdId: number; length: number }
  | { type: "seekFd"; fdId: number; offset: number; whence: number }
  | { type: "dupFd"; srcFdId: number; newFdId: number }
  | { type: "closeFd"; fdId: number }
  | { type: "ftruncateFd"; fdId: number; size: number }
  | { type: "allocateFd"; fdId: number; offset: number; length: number }
  | { type: "positionalWrite"; fdId: number; position: number; data: Uint8Array }
  | { type: "positionalRead"; fdId: number; position: number; length: number }
  | { type: "mkdir"; path: string }
  | { type: "unlink"; path: string };

const DIR_NAMES = ["alpha", "beta"];
const FILE_NAMES = ["wal.dat", "heap.dat", "idx.dat", "tmp.dat", "cat.dat"];
const TOME_MOUNT = "/tome";

function rw(p: string): string {
  return TOME_MOUNT + p;
}

// ---------------------------------------------------------------
// Operation generator
// ---------------------------------------------------------------

function generateOp(rng: Rng, model: FSModel): Op {
  const allFiles = [...model.files.keys()];
  const allContainerDirs = [...model.dirs];
  const openFdIds = [...model.fds.keys()];
  // Fds pointing to files that still exist
  const validFdIds = openFdIds.filter((id) => {
    const fd = model.fds.get(id)!;
    return model.files.has(fd.path);
  });
  // Files without open fds (safe to unlink without complicating the model)
  const unlinkableFiles = allFiles.filter((path) => {
    for (const fd of model.fds.values()) {
      if (fd.path === path) return false;
    }
    return true;
  });

  const weights: Array<[string, number]> = [
    ["createFile", 15],
    ["mkdir", 5],
    ["openFd", allFiles.length > 0 && model.fds.size < 6 ? 15 : 0],
    ["writeFd", validFdIds.length > 0 ? 20 : 0],
    ["readFd", validFdIds.length > 0 ? 8 : 0],
    ["seekFd", validFdIds.length > 0 ? 12 : 0],
    ["dupFd", validFdIds.length > 0 && model.fds.size < 10 ? 10 : 0],
    ["closeFd", openFdIds.length > 0 ? 8 : 0],
    ["ftruncateFd", validFdIds.length > 0 ? 8 : 0],
    ["allocateFd", validFdIds.length > 0 ? 8 : 0],
    ["positionalWrite", validFdIds.length > 0 ? 15 : 0],
    ["positionalRead", validFdIds.length > 0 ? 6 : 0],
    ["unlink", unlinkableFiles.length > 0 ? 5 : 0],
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

    case "openFd": {
      const path = rng.pick(allFiles);
      const flagChoices = [
        O.RDONLY,
        O.WRONLY,
        O.RDWR,
        O.WRONLY | O.APPEND,
        O.RDWR | O.APPEND,
      ];
      return {
        type: "openFd",
        path,
        fdId: model.nextFdId,
        flags: rng.pick(flagChoices),
      };
    }

    case "writeFd": {
      const fdId = rng.pick(validFdIds);
      const sizeChoices = [1, 50, 128, PAGE_SIZE - 1, PAGE_SIZE, PAGE_SIZE + 1, PAGE_SIZE * 2];
      const data = rng.bytes(rng.pick(sizeChoices));
      return { type: "writeFd", fdId, data };
    }

    case "readFd": {
      const fdId = rng.pick(validFdIds);
      const lengthChoices = [1, 100, PAGE_SIZE, PAGE_SIZE * 2];
      return { type: "readFd", fdId, length: rng.pick(lengthChoices) };
    }

    case "seekFd": {
      const fdId = rng.pick(validFdIds);
      const fd = model.fds.get(fdId)!;
      const fileSize = model.files.get(fd.path)?.data.length ?? 0;
      const whenceChoices = [SEEK_SET, SEEK_CUR, SEEK_END];
      const whence = rng.pick(whenceChoices);
      let offset: number;
      switch (whence) {
        case SEEK_SET:
          // Seek to various positions including beyond EOF
          offset = rng.int(Math.max(1, fileSize + PAGE_SIZE));
          break;
        case SEEK_CUR: {
          // Relative offset — can be forward or backward
          const range = Math.max(1, fileSize + PAGE_SIZE);
          offset = rng.int(range * 2) - range;
          // Ensure we don't seek before position 0
          if (fd.position + offset < 0) offset = -fd.position;
          break;
        }
        case SEEK_END:
          // From end — usually negative or small positive
          offset = rng.int(Math.max(1, fileSize + PAGE_SIZE)) - fileSize;
          if (fileSize + offset < 0) offset = -fileSize;
          break;
        default:
          offset = 0;
      }
      return { type: "seekFd", fdId, offset, whence };
    }

    case "dupFd": {
      const srcFdId = rng.pick(validFdIds);
      return { type: "dupFd", srcFdId, newFdId: model.nextFdId };
    }

    case "closeFd": {
      return { type: "closeFd", fdId: rng.pick(openFdIds) };
    }

    case "ftruncateFd": {
      const fdId = rng.pick(validFdIds);
      const fd = model.fds.get(fdId)!;
      const fileSize = model.files.get(fd.path)?.data.length ?? 0;
      const sizeChoices = [
        0,
        Math.max(0, fileSize - PAGE_SIZE),
        Math.max(0, fileSize - 1),
        fileSize,
        fileSize + 1,
        fileSize + PAGE_SIZE,
      ];
      return { type: "ftruncateFd", fdId, size: rng.pick(sizeChoices) };
    }

    case "allocateFd": {
      const fdId = rng.pick(validFdIds);
      const fd = model.fds.get(fdId)!;
      const fileSize = model.files.get(fd.path)?.data.length ?? 0;
      // Allocate at various offsets with various lengths
      const offset = rng.int(Math.max(1, fileSize + PAGE_SIZE));
      const lengthChoices = [1, PAGE_SIZE, PAGE_SIZE * 2, PAGE_SIZE * 3];
      const length = rng.pick(lengthChoices);
      return { type: "allocateFd", fdId, offset, length };
    }

    case "positionalWrite": {
      const fdId = rng.pick(validFdIds);
      const fd = model.fds.get(fdId)!;
      const fileSize = model.files.get(fd.path)?.data.length ?? 0;
      // Write at various positions including beyond EOF
      const position = rng.int(Math.max(1, fileSize + PAGE_SIZE));
      const sizeChoices = [1, 50, 128, PAGE_SIZE - 1, PAGE_SIZE, PAGE_SIZE + 1];
      const data = rng.bytes(rng.pick(sizeChoices));
      return { type: "positionalWrite", fdId, position, data };
    }

    case "positionalRead": {
      const fdId = rng.pick(validFdIds);
      const fd = model.fds.get(fdId)!;
      const fileSize = model.files.get(fd.path)?.data.length ?? 0;
      const position = rng.int(Math.max(1, fileSize + 1));
      const lengthChoices = [1, 100, PAGE_SIZE, PAGE_SIZE * 2];
      return { type: "positionalRead", fdId, position, length: rng.pick(lengthChoices) };
    }

    case "unlink":
      return { type: "unlink", path: rng.pick(unlinkableFiles) };

    default:
      return { type: "createFile", path: `/${rng.pick(FILE_NAMES)}`, data: rng.bytes(10) };
  }
}

// ---------------------------------------------------------------
// Model update: maintain expected state after each operation
// ---------------------------------------------------------------

function updateModel(model: FSModel, op: Op): void {
  switch (op.type) {
    case "createFile": {
      model.files.set(op.path, { data: new Uint8Array(op.data), mode: 0o100666 });
      break;
    }

    case "mkdir":
      model.dirs.add(op.path);
      break;

    case "openFd": {
      const fd: FdState = {
        id: op.fdId,
        path: op.path,
        position: 0,
        append: (op.flags & O.APPEND) !== 0,
      };
      model.fds.set(op.fdId, fd);
      model.nextFdId++;
      break;
    }

    case "writeFd": {
      const fd = model.fds.get(op.fdId)!;
      const file = model.files.get(fd.path)!;
      const writePos = fd.append ? file.data.length : fd.position;
      const newEnd = writePos + op.data.length;
      if (newEnd > file.data.length) {
        const newData = new Uint8Array(newEnd);
        newData.set(file.data);
        file.data = newData;
      }
      file.data.set(op.data, writePos);
      fd.position = newEnd;
      break;
    }

    case "readFd": {
      const fd = model.fds.get(op.fdId)!;
      const file = model.files.get(fd.path)!;
      const available = Math.max(0, file.data.length - fd.position);
      const toRead = Math.min(op.length, available);
      fd.position += toRead;
      break;
    }

    case "seekFd": {
      const fd = model.fds.get(op.fdId)!;
      const fileSize = model.files.get(fd.path)?.data.length ?? 0;
      switch (op.whence) {
        case SEEK_SET:
          fd.position = op.offset;
          break;
        case SEEK_CUR:
          fd.position += op.offset;
          break;
        case SEEK_END:
          fd.position = fileSize + op.offset;
          break;
      }
      if (fd.position < 0) fd.position = 0;
      break;
    }

    case "dupFd": {
      const srcFd = model.fds.get(op.srcFdId)!;
      model.fds.set(op.newFdId, {
        id: op.newFdId,
        path: srcFd.path,
        position: srcFd.position,
        append: srcFd.append,
      });
      model.nextFdId++;
      break;
    }

    case "closeFd":
      model.fds.delete(op.fdId);
      break;

    case "ftruncateFd": {
      const fd = model.fds.get(op.fdId)!;
      const file = model.files.get(fd.path)!;
      if (op.size < file.data.length) {
        file.data = file.data.slice(0, op.size);
      } else if (op.size > file.data.length) {
        const newData = new Uint8Array(op.size);
        newData.set(file.data);
        file.data = newData;
      }
      break;
    }

    case "allocateFd": {
      const fd = model.fds.get(op.fdId)!;
      const file = model.files.get(fd.path)!;
      const newEnd = op.offset + op.length;
      if (newEnd > file.data.length) {
        const newData = new Uint8Array(newEnd);
        newData.set(file.data);
        file.data = newData;
      }
      break;
    }

    case "positionalWrite": {
      const fd = model.fds.get(op.fdId)!;
      const file = model.files.get(fd.path)!;
      const newEnd = op.position + op.data.length;
      if (newEnd > file.data.length) {
        const newData = new Uint8Array(newEnd);
        newData.set(file.data);
        file.data = newData;
      }
      file.data.set(op.data, op.position);
      // Positional write does NOT update the fd's position in Emscripten
      break;
    }

    case "positionalRead":
      // Positional read does NOT update the fd's position
      break;

    case "unlink":
      model.files.delete(op.path);
      break;
  }
}

// ---------------------------------------------------------------
// Execute operation against the real filesystem
// ---------------------------------------------------------------

/** Map from model fd id → real Emscripten stream. */
type StreamMap = Map<number, EmscriptenStream>;

function execOp(rawFS: any, op: Op, streams: StreamMap): boolean {
  try {
    switch (op.type) {
      case "createFile": {
        const s = rawFS.open(rw(op.path), O.WRONLY | O.CREAT | O.TRUNC, 0o666);
        if (op.data.length > 0) {
          rawFS.write(s, op.data, 0, op.data.length);
        }
        rawFS.close(s);
        return true;
      }

      case "mkdir":
        rawFS.mkdir(rw(op.path));
        return true;

      case "openFd": {
        const s = rawFS.open(rw(op.path), op.flags, 0o666);
        streams.set(op.fdId, s);
        return true;
      }

      case "writeFd": {
        const s = streams.get(op.fdId)!;
        rawFS.write(s, op.data, 0, op.data.length);
        return true;
      }

      case "readFd": {
        const s = streams.get(op.fdId)!;
        const buf = new Uint8Array(op.length);
        rawFS.read(s, buf, 0, op.length);
        return true;
      }

      case "seekFd": {
        const s = streams.get(op.fdId)!;
        rawFS.llseek(s, op.offset, op.whence);
        return true;
      }

      case "dupFd": {
        const srcStream = streams.get(op.srcFdId)!;
        const newStream = rawFS.dupStream(srcStream);
        streams.set(op.newFdId, newStream);
        return true;
      }

      case "closeFd": {
        const s = streams.get(op.fdId);
        if (s) {
          rawFS.close(s);
          streams.delete(op.fdId);
        }
        return true;
      }

      case "ftruncateFd": {
        const s = streams.get(op.fdId)!;
        rawFS.ftruncate(s.fd, op.size);
        return true;
      }

      case "allocateFd": {
        const s = streams.get(op.fdId)!;
        // Emscripten allocate is available through stream_ops
        if (s.stream_ops && s.stream_ops.allocate) {
          s.stream_ops.allocate(s, op.offset, op.length);
        }
        return true;
      }

      case "positionalWrite": {
        const s = streams.get(op.fdId)!;
        rawFS.write(s, op.data, 0, op.data.length, op.position);
        return true;
      }

      case "positionalRead": {
        const s = streams.get(op.fdId)!;
        const buf = new Uint8Array(op.length);
        rawFS.read(s, buf, 0, op.length, op.position);
        return true;
      }

      case "unlink":
        rawFS.unlink(rw(op.path));
        return true;
    }
  } catch (_e) {
    return false;
  }
}

// ---------------------------------------------------------------
// Operation formatting for debugging
// ---------------------------------------------------------------

function formatOp(op: Op, index: number): string {
  switch (op.type) {
    case "createFile":
      return `[${index}] createFile(${op.path}, ${op.data.length}B)`;
    case "openFd":
      return `[${index}] openFd(${op.path}, flags=0x${op.flags.toString(16)}, fd=${op.fdId})`;
    case "writeFd":
      return `[${index}] writeFd(fd=${op.fdId}, ${op.data.length}B)`;
    case "readFd":
      return `[${index}] readFd(fd=${op.fdId}, ${op.length}B)`;
    case "seekFd":
      return `[${index}] seekFd(fd=${op.fdId}, offset=${op.offset}, whence=${op.whence})`;
    case "dupFd":
      return `[${index}] dupFd(src=${op.srcFdId}, new=${op.newFdId})`;
    case "closeFd":
      return `[${index}] closeFd(fd=${op.fdId})`;
    case "ftruncateFd":
      return `[${index}] ftruncateFd(fd=${op.fdId}, size=${op.size})`;
    case "allocateFd":
      return `[${index}] allocateFd(fd=${op.fdId}, offset=${op.offset}, len=${op.length})`;
    case "positionalWrite":
      return `[${index}] positionalWrite(fd=${op.fdId}, pos=${op.position}, ${op.data.length}B)`;
    case "positionalRead":
      return `[${index}] positionalRead(fd=${op.fdId}, pos=${op.position}, ${op.length}B)`;
    case "mkdir":
      return `[${index}] mkdir(${op.path})`;
    case "unlink":
      return `[${index}] unlink(${op.path})`;
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
  }

  // Verify no extra files exist (stale metadata from previous operations)
  for (const dir of model.dirs) {
    const fsPath = rw(dir);
    try {
      const entries = new Set(rawFS.readdir(fsPath) as string[]);
      entries.delete(".");
      entries.delete("..");

      // Build expected entries for this directory
      const prefix = dir === "/" ? "/" : dir + "/";
      const expectedNames = new Set<string>();
      for (const filePath of model.files.keys()) {
        if (filePath.startsWith(prefix) && !filePath.slice(prefix.length).includes("/")) {
          expectedNames.add(filePath.slice(prefix.length));
        }
      }
      for (const d of model.dirs) {
        if (d !== dir && d.startsWith(prefix) && !d.slice(prefix.length).includes("/")) {
          expectedNames.add(d.slice(prefix.length));
        }
      }

      for (const name of entries) {
        expect(
          expectedNames.has(name),
          `${context}: unexpected entry ${dir}/${name} in readdir (stale metadata?)`,
        ).toBe(true);
      }
      for (const name of expectedNames) {
        expect(entries.has(name), `${context}: ${dir}/${name} missing from readdir`).toBe(true);
      }
    } catch (e: any) {
      if (dir !== "/") {
        throw new Error(`${context}: directory ${dir} missing after remount`);
      }
    }
  }
}

// ---------------------------------------------------------------
// Fuzz test runner with persistence roundtrips
// ---------------------------------------------------------------

async function runFdPersistenceFuzz(
  seed: number,
  numOps: number,
  maxPages: number,
  remountInterval: number,
): Promise<void> {
  const rng = new Rng(seed);
  const model = newModel();
  const backend = new SyncMemoryBackend();
  let streams: StreamMap = new Map();

  let instance = await createTomeFSInstance(backend, maxPages);
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
      // Close all open fds before remount (WASM module is destroyed)
      for (const [fdId, stream] of streams) {
        try {
          instance.rawFS.close(stream);
        } catch (_e) {
          // Ignore close errors on already-closed/invalid streams
        }
      }
      streams.clear();
      // Also clear fds from the model
      model.fds.clear();

      // Persist and remount
      syncfs(instance.rawFS);
      instance = await createTomeFSInstance(backend, maxPages);

      const context = `remount after op ${i} (seed ${seed})`;
      try {
        verifyAfterRemount(instance.rawFS, model, context);
      } catch (e) {
        const recentOps = ops.slice(Math.max(0, ops.length - 20));
        throw new Error(
          `${(e as Error).message}\n\nRecent ops:\n${recentOps.join("\n")}`,
        );
      }
    }
  }

  // Close remaining fds
  for (const [fdId, stream] of streams) {
    try {
      instance.rawFS.close(stream);
    } catch (_e) { /* ignore */ }
  }
  streams.clear();
  model.fds.clear();

  // Final roundtrip
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

describe("fuzz: fd-ops persistence roundtrip", () => {
  describe("tiny cache (4 pages) — maximum eviction pressure", () => {
    const CACHE = 4;
    const OPS = 80;
    const INTERVAL = 15;

    it("seed 70001 @fast", async () => {
      await runFdPersistenceFuzz(70001, OPS, CACHE, INTERVAL);
    }, 30_000);

    it("seed 70002", async () => {
      await runFdPersistenceFuzz(70002, OPS, CACHE, INTERVAL);
    }, 30_000);

    it("seed 70003", async () => {
      await runFdPersistenceFuzz(70003, OPS, CACHE, INTERVAL);
    }, 30_000);

    it("seed 70005", async () => {
      await runFdPersistenceFuzz(70005, OPS, CACHE, INTERVAL);
    }, 30_000);
  });

  describe("small cache (16 pages) — moderate eviction", () => {
    const CACHE = 16;
    const OPS = 80;
    const INTERVAL = 15;

    it("seed 71006 @fast", async () => {
      await runFdPersistenceFuzz(71006, OPS, CACHE, INTERVAL);
    }, 30_000);

    it("seed 71002", async () => {
      await runFdPersistenceFuzz(71002, OPS, CACHE, INTERVAL);
    }, 30_000);

    it("seed 71005", async () => {
      await runFdPersistenceFuzz(71005, OPS, CACHE, INTERVAL);
    }, 30_000);
  });

  describe("medium cache (64 pages) — partial eviction", () => {
    const CACHE = 64;
    const OPS = 80;
    const INTERVAL = 15;

    it("seed 72001 @fast", async () => {
      await runFdPersistenceFuzz(72001, OPS, CACHE, INTERVAL);
    }, 30_000);

    it("seed 72002", async () => {
      await runFdPersistenceFuzz(72002, OPS, CACHE, INTERVAL);
    }, 30_000);
  });

  describe("large cache (4096 pages) — no eviction baseline", () => {
    const CACHE = 4096;
    const OPS = 80;
    const INTERVAL = 20;

    it("seed 73001", async () => {
      await runFdPersistenceFuzz(73001, OPS, CACHE, INTERVAL);
    }, 30_000);
  });

  describe("frequent remounts — stress persist/restore with open fds", () => {
    it("seed 74001, tiny cache, remount every 5 ops @fast", async () => {
      await runFdPersistenceFuzz(74001, 60, 4, 5);
    }, 30_000);

    it("seed 74002, small cache, remount every 5 ops", async () => {
      await runFdPersistenceFuzz(74002, 60, 16, 5);
    }, 30_000);

    it("seed 74003, tiny cache, remount every 3 ops", async () => {
      await runFdPersistenceFuzz(74003, 45, 4, 3);
    }, 30_000);
  });

  describe("extended sequences — long fd operation chains with remounts", () => {
    it("200 ops, tiny cache, seed 75010", async () => {
      await runFdPersistenceFuzz(75010, 200, 4, 20);
    }, 60_000);

    it("200 ops, small cache, seed 75002", async () => {
      await runFdPersistenceFuzz(75002, 200, 16, 20);
    }, 60_000);
  });
});
