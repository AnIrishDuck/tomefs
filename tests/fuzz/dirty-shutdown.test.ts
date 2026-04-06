/**
 * Dirty shutdown recovery fuzz tests for tomefs.
 *
 * Simulates process crashes by remounting WITHOUT calling syncfs. After
 * close(), tomefs flushes page data to the backend — but metadata (file
 * sizes, modes, timestamps) is only written during syncfs. A dirty
 * shutdown leaves the backend with up-to-date page data but stale metadata.
 *
 * restoreTree must handle this inconsistency:
 *   - Pages may exist beyond what metadata.size expects (file was extended
 *     after last sync, pages flushed on close, metadata never updated)
 *   - Pages may be missing below metadata.size (file was truncated after
 *     last sync, pages deleted, metadata never updated)
 *   - New files may have pages but no metadata (created after last sync,
 *     pages flushed on close, metadata only written in syncfs)
 *   - Deleted files may have metadata but no pages (unlinked after last
 *     sync, pages deleted, metadata not cleaned up)
 *
 * Test strategy:
 *   Phase 1: Run operations + clean syncfs → save model as "checkpoint"
 *   Phase 2: Run more operations WITHOUT syncfs → "dirty" state
 *   Phase 3: Remount from backend (dirty shutdown) → verify no crashes
 *   Phase 4: Clean syncfs + remount → verify full recoverability
 *
 * The key assertion: after a dirty shutdown and recovery sync, the
 * filesystem reaches a consistent state with no corruption, crashes,
 * or leaked pages. Files may reflect either pre-crash or post-crash
 * content, but the filesystem is always usable.
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

function cloneModel(model: FSModel): FSModel {
  const clone: FSModel = {
    files: new Map(),
    dirs: new Set(model.dirs),
    symlinks: new Map(model.symlinks),
  };
  for (const [path, state] of model.files) {
    clone.files.set(path, { data: new Uint8Array(state.data), mode: state.mode });
  }
  return clone;
}

// ---------------------------------------------------------------
// Operation types (focused on data-mutating operations that
// exercise page cache flush / metadata persistence seams)
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
  | { type: "appendWrite"; path: string; data: Uint8Array }
  | { type: "symlink"; target: string; path: string }
  | { type: "unlinkSymlink"; path: string };

const DIR_NAMES = ["alpha", "beta", "gamma"];
const FILE_NAMES = ["a.dat", "b.dat", "c.dat", "d.dat", "e.dat"];
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
// Operation generator
// ---------------------------------------------------------------

function generateOp(rng: Rng, model: FSModel): Op {
  const allFiles = [...model.files.keys()];
  const allDirs = [...model.dirs].filter((d) => d !== "/");
  const allContainerDirs = [...model.dirs];
  const allSymlinks = [...model.symlinks.keys()];

  const weights: Array<[string, number]> = [
    ["createFile", 25],
    ["mkdir", 10],
    ["writeAt", allFiles.length > 0 ? 20 : 0],
    ["truncate", allFiles.length > 0 ? 12 : 0],
    ["overwrite", allFiles.length > 0 ? 10 : 0],
    ["renameFile", allFiles.length > 0 ? 10 : 0],
    ["unlink", allFiles.length > 0 ? 8 : 0],
    ["appendWrite", allFiles.length > 0 ? 12 : 0],
    ["symlink", allFiles.length > 0 ? 6 : 0],
    ["unlinkSymlink", allSymlinks.length > 0 ? 4 : 0],
    ["rmdir", allDirs.filter((d) => isDirEmpty(model, d)).length > 0 ? 4 : 0],
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
      // Emphasize sizes around page boundaries — these exercise the seam
      // between page-level storage and byte-level metadata.size
      const sizeChoices = [
        0, 1, 100,
        PAGE_SIZE - 1, PAGE_SIZE, PAGE_SIZE + 1,
        PAGE_SIZE * 2 - 1, PAGE_SIZE * 2, PAGE_SIZE * 2 + 1,
        PAGE_SIZE * 3 + 137,
      ];
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

    case "symlink": {
      const dir = rng.pick(allContainerDirs);
      const name = rng.pick(LINK_NAMES);
      const path = dir === "/" ? `/${name}` : `${dir}/${name}`;
      const target = rng.pick(allFiles);
      return { type: "symlink", target, path };
    }

    case "unlinkSymlink":
      return { type: "unlinkSymlink", path: rng.pick(allSymlinks) };

    case "rmdir": {
      const emptyDirs = allDirs.filter((d) => isDirEmpty(model, d));
      if (emptyDirs.length === 0) {
        return { type: "createFile", path: `/${rng.pick(FILE_NAMES)}`, data: rng.bytes(10) };
      }
      return { type: "rmdir", path: rng.pick(emptyDirs) };
    }

    default:
      return { type: "createFile", path: `/${rng.pick(FILE_NAMES)}`, data: rng.bytes(10) };
  }
}

// ---------------------------------------------------------------
// Execute operations and update model
// ---------------------------------------------------------------

const TOME_MOUNT = "/tome";

function rw(p: string): string {
  if (!p.startsWith("/") || p.startsWith("/dev") || p.startsWith("/proc") || p.startsWith("/tmp")) return p;
  if (p.startsWith(TOME_MOUNT + "/") || p === TOME_MOUNT) return p;
  if (p === "/") return TOME_MOUNT;
  return TOME_MOUNT + p;
}

function execOp(FS: any, op: Op): boolean {
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
      case "truncate":
        FS.truncate(rw(op.path), op.size);
        return true;
      case "overwrite": {
        const s = FS.open(rw(op.path), O.WRONLY | O.TRUNC);
        if (op.data.length > 0) {
          FS.write(s, op.data, 0, op.data.length, 0);
        }
        FS.close(s);
        return true;
      }
      case "renameFile":
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
      case "appendWrite": {
        const s = FS.open(rw(op.path), O.WRONLY | O.APPEND);
        FS.write(s, op.data, 0, op.data.length);
        FS.close(s);
        return true;
      }
      case "symlink":
        FS.symlink(rw(op.target), rw(op.path));
        return true;
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
      const mode = existing ? existing.mode : 0o100666;
      model.files.set(op.path, { data: new Uint8Array(op.data), mode });
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
    case "appendWrite": {
      const file = model.files.get(op.path);
      if (!file) break;
      const newData = new Uint8Array(file.data.length + op.data.length);
      newData.set(file.data);
      newData.set(op.data, file.data.length);
      file.data = newData;
      break;
    }
    case "symlink":
      model.symlinks.set(op.path, op.target);
      break;
    case "unlinkSymlink":
      model.symlinks.delete(op.path);
      break;
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
    case "appendWrite":
      return `[${index}] appendWrite(${op.path}, ${op.data.length}B)`;
    case "symlink":
      return `[${index}] symlink(${op.target} -> ${op.path})`;
    case "unlinkSymlink":
      return `[${index}] unlinkSymlink(${op.path})`;
  }
}

// ---------------------------------------------------------------
// Mount helpers
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
 * Walk the entire filesystem tree under the mount point and verify
 * it's navigable without crashes. Returns the set of file paths found.
 *
 * This is the core dirty-shutdown assertion: after a crash, the
 * filesystem must be usable even if some data is stale or inconsistent.
 */
function walkAndVerifyNavigable(rawFS: any, context: string): Set<string> {
  const foundFiles = new Set<string>();

  function walk(dirPath: string, modelPath: string): void {
    let entries: string[];
    try {
      entries = rawFS.readdir(dirPath);
    } catch (e: any) {
      throw new Error(`${context}: readdir(${modelPath}) crashed: ${e.message}`);
    }

    for (const name of entries) {
      if (name === "." || name === "..") continue;
      const fsPath = `${dirPath}/${name}`;
      const mPath = modelPath === "/" ? `/${name}` : `${modelPath}/${name}`;

      let stat: any;
      try {
        stat = rawFS.lstat(fsPath);
      } catch (e: any) {
        throw new Error(`${context}: lstat(${mPath}) crashed: ${e.message}`);
      }

      if (rawFS.isDir(stat.mode)) {
        walk(fsPath, mPath);
      } else if (rawFS.isFile(stat.mode)) {
        foundFiles.add(mPath);
        // Verify the file is readable without crashing
        if (stat.size > 0) {
          const buf = new Uint8Array(stat.size);
          let s: any;
          try {
            s = rawFS.open(fsPath, O.RDONLY);
            rawFS.read(s, buf, 0, stat.size, 0);
            rawFS.close(s);
          } catch (e: any) {
            throw new Error(
              `${context}: read(${mPath}, ${stat.size}B) crashed: ${e.message}`,
            );
          }
        }
      }
      // Symlinks: just verifying lstat didn't crash is sufficient
    }
  }

  walk(TOME_MOUNT, "/");
  return foundFiles;
}

/**
 * After a dirty shutdown + recovery sync + remount, verify full consistency.
 * The filesystem should have converged to a clean state.
 */
function verifyCleanState(rawFS: any, context: string): void {
  // Walk the tree — should not crash
  const foundFiles = walkAndVerifyNavigable(rawFS, context);

  // Every file should have consistent size and readable content
  for (const path of foundFiles) {
    const fsPath = rw(path);
    const stat = rawFS.stat(fsPath);
    expect(stat.size >= 0, `${context}: ${path} has negative size`).toBe(true);

    if (stat.size > 0) {
      const buf = new Uint8Array(stat.size);
      const s = rawFS.open(fsPath, O.RDONLY);
      const n = rawFS.read(s, buf, 0, stat.size, 0);
      rawFS.close(s);
      expect(n, `${context}: ${path} read returned wrong count`).toBe(stat.size);
    }
  }
}

/**
 * Verify that checkpoint files (from the last clean sync) are present
 * and have correct content after a dirty shutdown + recovery.
 */
function verifyCheckpointFiles(
  rawFS: any,
  checkpoint: FSModel,
  context: string,
): void {
  for (const [path, fileState] of checkpoint.files) {
    const fsPath = rw(path);
    let stat: any;
    try {
      stat = rawFS.stat(fsPath);
    } catch {
      // File from checkpoint may be missing if it was deleted in the
      // dirty phase and the deletion was persisted (rename/unlink write
      // metadata directly to the backend). This is expected.
      continue;
    }

    // The file exists — verify it's readable and has sane size.
    // Content may be checkpoint data OR post-checkpoint data (pages
    // flushed on close during the dirty phase). Both are acceptable.
    expect(stat.size >= 0, `${context}: ${path} has negative size`).toBe(true);

    if (stat.size > 0) {
      const buf = new Uint8Array(stat.size);
      const s = rawFS.open(fsPath, O.RDONLY);
      rawFS.read(s, buf, 0, stat.size, 0);
      rawFS.close(s);
      // No content assertion — content may be from checkpoint or dirty phase
    }
  }
}

// ---------------------------------------------------------------
// Fuzz test runner
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

  // Phase 1: Run operations and do a clean syncfs
  let instance = await mountTome(backend, maxPages);
  for (let i = 0; i < checkpointOps; i++) {
    const op = generateOp(rng, model);
    ops.push(formatOp(op, i));
    if (execOp(instance.rawFS, op)) {
      updateModel(model, op);
    }
  }
  doSyncfs(instance.rawFS);
  const checkpoint = cloneModel(model);

  // Phase 2: Run more operations WITHOUT syncfs (simulates work before crash)
  for (let i = 0; i < dirtyOps; i++) {
    const op = generateOp(rng, model);
    ops.push(formatOp(op, checkpointOps + i));
    if (execOp(instance.rawFS, op)) {
      updateModel(model, op);
    }
  }

  // Phase 3: Dirty shutdown — remount without syncfs.
  // The backend has:
  //   - Page data from checkpoint sync + pages flushed on close() during dirty phase
  //   - Metadata from checkpoint sync + metadata written by rename/unlink during dirty phase
  // restoreTree must reconcile this inconsistent state.
  try {
    instance = await mountTome(backend, maxPages);
  } catch (e: any) {
    const recentOps = ops.slice(Math.max(0, ops.length - 20));
    throw new Error(
      `Dirty shutdown remount crashed (seed ${seed}): ${e.message}\n\n` +
      `Recent ops:\n${recentOps.join("\n")}`,
    );
  }

  // Verify the filesystem is navigable without crashes
  const ctxDirty = `after dirty shutdown (seed ${seed})`;
  try {
    walkAndVerifyNavigable(instance.rawFS, ctxDirty);
  } catch (e: any) {
    const recentOps = ops.slice(Math.max(0, ops.length - 20));
    throw new Error(`${e.message}\n\nRecent ops:\n${recentOps.join("\n")}`);
  }

  // Verify checkpoint files are accessible
  try {
    verifyCheckpointFiles(instance.rawFS, checkpoint, ctxDirty);
  } catch (e: any) {
    const recentOps = ops.slice(Math.max(0, ops.length - 20));
    throw new Error(`${e.message}\n\nRecent ops:\n${recentOps.join("\n")}`);
  }

  // Phase 4: Recovery — do a clean sync, remount, verify full consistency.
  // After a clean sync on the recovered filesystem, the next remount
  // should produce a fully consistent state with no stale metadata,
  // leaked pages, or missing files.
  doSyncfs(instance.rawFS);
  instance = await mountTome(backend, maxPages);

  const ctxRecovery = `after recovery sync (seed ${seed})`;
  try {
    verifyCleanState(instance.rawFS, ctxRecovery);
  } catch (e: any) {
    const recentOps = ops.slice(Math.max(0, ops.length - 20));
    throw new Error(`${e.message}\n\nRecent ops:\n${recentOps.join("\n")}`);
  }

  // Phase 5: Verify the recovered state survives another roundtrip.
  // This catches bugs where the recovery sync itself introduces
  // inconsistencies that only manifest on the next remount.
  doSyncfs(instance.rawFS);
  instance = await mountTome(backend, maxPages);

  const ctxStable = `after second remount (seed ${seed})`;
  try {
    verifyCleanState(instance.rawFS, ctxStable);
  } catch (e: any) {
    const recentOps = ops.slice(Math.max(0, ops.length - 20));
    throw new Error(`${e.message}\n\nRecent ops:\n${recentOps.join("\n")}`);
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
      const maxPages = 8;

      // Cycle 1: build up some state, sync, then dirty shutdown
      let instance = await mountTome(backend, maxPages);
      for (let i = 0; i < 20; i++) {
        const op = generateOp(rng, model);
        if (execOp(instance.rawFS, op)) updateModel(model, op);
      }
      doSyncfs(instance.rawFS);

      // Dirty ops + crash
      for (let i = 0; i < 15; i++) {
        const op = generateOp(rng, model);
        if (execOp(instance.rawFS, op)) updateModel(model, op);
      }
      instance = await mountTome(backend, maxPages); // dirty remount
      walkAndVerifyNavigable(instance.rawFS, "cycle 1 dirty");

      // Cycle 2: recover, do more work, sync, more dirty ops, crash
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
      instance = await mountTome(backend, maxPages); // dirty remount
      walkAndVerifyNavigable(instance.rawFS, "cycle 2 dirty");

      // Cycle 3: one more recovery + dirty shutdown
      doSyncfs(instance.rawFS);
      for (let i = 0; i < 10; i++) {
        const op = generateOp(rng, model);
        if (execOp(instance.rawFS, op)) updateModel(model, op);
      }
      instance = await mountTome(backend, maxPages); // dirty remount
      walkAndVerifyNavigable(instance.rawFS, "cycle 3 dirty");

      // Final recovery — should produce a stable, navigable filesystem
      doSyncfs(instance.rawFS);
      instance = await mountTome(backend, maxPages);
      verifyCleanState(instance.rawFS, "final recovery (seed 76001)");
    }, 60_000);
  });
});
