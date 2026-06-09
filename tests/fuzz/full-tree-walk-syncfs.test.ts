/**
 * Fuzz tests that specifically exercise the full-tree-walk syncfs path.
 *
 * The full-tree-walk path is triggered when `needsOrphanCleanup` is true —
 * which happens on mount when the clean-shutdown marker is absent. The
 * marker is absent when the previous session crashed between a rename/unlink
 * (which invalidates the marker) and the next syncfs (which re-writes it).
 *
 * Existing fuzz tests (persistence, dirty-shutdown) exercise this path only
 * by chance — their random operations MAY include renames/unlinks in the
 * dirty phase, but it's not guaranteed. This file ensures the path is
 * exercised reliably by:
 *
 *   1. Injecting rename/unlink operations that invalidate the clean marker
 *   2. Verifying via TomeFSStats that fullTreeSyncs > 0
 *   3. Verifying orphan cleanup produces a clean backend
 *   4. Testing the full-tree → incremental transition within a session
 *   5. Verifying data integrity on clean sync cycles after recovery
 *
 * After a dirty shutdown, the filesystem state is unpredictable — some
 * operations are eagerly persisted (rename, unlink) while others are
 * deferred to syncfs (write, truncate, mkdir). So we verify structural
 * integrity and recovery behavior, NOT exact data matching. We rebuild
 * the ground-truth model from the actual recovered state, then verify
 * that subsequent clean operations produce correct results.
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
import type { TomeFSStats } from "../../src/types.js";

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
// Filesystem state model
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
// Operations — weighted heavily toward rename/unlink to ensure
// the clean-shutdown marker gets invalidated
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
  | { type: "overwrite"; path: string; data: Uint8Array };

const DIR_NAMES = ["alpha", "beta", "gamma", "delta"];
const FILE_NAMES = ["a.dat", "b.dat", "c.dat", "d.dat", "e.dat"];
const LINK_NAMES = ["lnk1", "lnk2"];

function generateOp(rng: Rng, model: FSModel): Op {
  const allFiles = [...model.files.keys()];
  const allDirs = [...model.dirs].filter((d) => d !== "/");
  const allContainerDirs = [...model.dirs];
  const allSymlinks = [...model.symlinks.keys()];

  const weights: Array<[string, number]> = [
    ["createFile", 15],
    ["mkdir", 8],
    ["writeAt", allFiles.length > 0 ? 10 : 0],
    ["truncate", allFiles.length > 0 ? 6 : 0],
    ["overwrite", allFiles.length > 0 ? 6 : 0],
    ["renameFile", allFiles.length > 0 ? 20 : 0],
    ["unlink", allFiles.length > 0 ? 15 : 0],
    ["renameDir", allDirs.length > 0 ? 12 : 0],
    ["rmdir", allDirs.filter((d) => isDirEmpty(model, d)).length > 0 ? 4 : 0],
    ["symlink", allFiles.length > 0 ? 6 : 0],
    ["unlinkSymlink", allSymlinks.length > 0 ? 6 : 0],
    ["renameSymlink", allSymlinks.length > 0 ? 8 : 0],
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
      const sizeChoices = [0, 1, 100, PAGE_SIZE - 1, PAGE_SIZE, PAGE_SIZE + 1, PAGE_SIZE * 2];
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
    case "renameSymlink": {
      const oldPath = rng.pick(allSymlinks);
      const dir = rng.pick(allContainerDirs);
      const name = rng.pick(LINK_NAMES);
      const newPath = dir === "/" ? `/${name}` : `${dir}/${name}`;
      return { type: "renameSymlink", oldPath, newPath };
    }
    default:
      return { type: "createFile", path: `/${rng.pick(FILE_NAMES)}`, data: rng.bytes(10) };
  }
}

// ---------------------------------------------------------------
// Execute + model update
// ---------------------------------------------------------------

const O = {
  RDONLY: 0,
  WRONLY: 1,
  RDWR: 2,
  CREAT: 64,
  TRUNC: 512,
} as const;

const TOME_MOUNT = "/tome";

function rw(p: string): string {
  if (p === "/") return TOME_MOUNT;
  return TOME_MOUNT + p;
}

function execOp(FS: any, op: Op): boolean {
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
      case "renameSymlink":
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
      default:
        return true;
    }
  } catch {
    return false;
  }
}

function updateModel(model: FSModel, op: Op): void {
  switch (op.type) {
    case "createFile":
      model.files.set(op.path, { data: new Uint8Array(op.data) });
      break;
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
      const target = model.symlinks.get(op.oldPath);
      if (!target) break;
      if (op.oldPath === op.newPath) break;
      model.symlinks.delete(op.oldPath);
      model.files.delete(op.newPath);
      model.symlinks.delete(op.newPath);
      model.symlinks.set(op.newPath, target);
      break;
    }
  }
}

// ---------------------------------------------------------------
// Mount helpers
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
// Verification helpers
// ---------------------------------------------------------------

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
        if (stat.size > 0) {
          try {
            const buf = new Uint8Array(stat.size);
            const s = rawFS.open(fullPath, O.RDONLY);
            rawFS.read(s, buf, 0, stat.size, 0);
            rawFS.close(s);
          } catch (e: any) {
            errors.push(`read(${fullPath}, ${stat.size}B) failed: ${e.message}`);
          }
        }
      } else if (rawFS.isLink(stat.mode)) {
        try {
          rawFS.readlink(fullPath);
        } catch (e: any) {
          errors.push(`readlink(${fullPath}) failed: ${e.message}`);
        }
      }
    }
  }

  walk(TOME_MOUNT);

  if (errors.length > 0) {
    throw new Error(`${context}: FS not navigable:\n  ${errors.join("\n  ")}`);
  }
}

function verifyNoOrphans(backend: SyncMemoryBackend, context: string): void {
  const paths = backend.listFiles();
  const orphans = paths.filter((p) => p.startsWith("/__deleted_"));
  if (orphans.length > 0) {
    throw new Error(
      `${context}: ${orphans.length} orphan entries remain: ${orphans.join(", ")}`,
    );
  }
}

function buildModelFromFS(rawFS: any): FSModel {
  const model = newModel();

  function walk(path: string, modelPath: string): void {
    const entries = rawFS.readdir(path);
    for (const name of entries) {
      if (name === "." || name === "..") continue;
      const fullPath = `${path}/${name}`;
      const mPath = modelPath === "/" ? `/${name}` : `${modelPath}/${name}`;
      const stat = rawFS.lstat(fullPath);

      if (rawFS.isDir(stat.mode)) {
        model.dirs.add(mPath);
        walk(fullPath, mPath);
      } else if (rawFS.isFile(stat.mode)) {
        const data = new Uint8Array(stat.size);
        if (stat.size > 0) {
          const s = rawFS.open(fullPath, O.RDONLY);
          rawFS.read(s, data, 0, stat.size, 0);
          rawFS.close(s);
        }
        model.files.set(mPath, { data });
      } else if (rawFS.isLink(stat.mode)) {
        const target = rawFS.readlink(fullPath);
        const strippedTarget = target.startsWith(TOME_MOUNT)
          ? target.slice(TOME_MOUNT.length) || "/"
          : target;
        model.symlinks.set(mPath, strippedTarget);
      }
    }
  }

  walk(TOME_MOUNT, "/");
  return model;
}

function verifyDataIntegrity(
  rawFS: any,
  model: FSModel,
  context: string,
): void {
  const errors: string[] = [];

  for (const [path, expected] of model.files) {
    const fullPath = rw(path);
    try {
      const stat = rawFS.stat(fullPath);
      if (stat.size !== expected.data.length) {
        errors.push(`${path}: size mismatch: expected ${expected.data.length}, got ${stat.size}`);
        continue;
      }
      if (stat.size > 0) {
        const buf = new Uint8Array(stat.size);
        const s = rawFS.open(fullPath, O.RDONLY);
        rawFS.read(s, buf, 0, stat.size, 0);
        rawFS.close(s);
        for (let i = 0; i < buf.length; i++) {
          if (buf[i] !== expected.data[i]) {
            errors.push(`${path}: data mismatch at byte ${i}`);
            break;
          }
        }
      }
    } catch (e: any) {
      errors.push(`${path}: ${e.message}`);
    }
  }

  for (const dir of model.dirs) {
    if (dir === "/") continue;
    try {
      const stat = rawFS.stat(rw(dir));
      if (!rawFS.isDir(stat.mode)) {
        errors.push(`${dir}: expected directory`);
      }
    } catch (e: any) {
      errors.push(`dir ${dir}: ${e.message}`);
    }
  }

  if (errors.length > 0) {
    throw new Error(
      `${context}: data integrity failures (${errors.length}):\n  - ${errors.join("\n  - ")}`,
    );
  }
}

// ---------------------------------------------------------------
// Test runner
// ---------------------------------------------------------------

async function runFullTreeWalkTest(config: {
  seed: number;
  setupOps: number;
  dirtyOps: number;
  maxPages: number;
  syncCycles: number;
}): Promise<void> {
  const { seed, setupOps, dirtyOps, maxPages, syncCycles } = config;
  const rng = new Rng(seed);
  const model = newModel();
  const backend = new SyncMemoryBackend();

  // Phase 1: Build initial state with a clean sync.
  let instance = await mountTome(backend, maxPages);
  for (let i = 0; i < setupOps; i++) {
    const op = generateOp(rng, model);
    if (execOp(instance.rawFS, op)) updateModel(model, op);
  }
  doSyncfs(instance.rawFS);
  instance.tomefs.assertInvariants();
  backend.assertInvariants();

  let stats: TomeFSStats = instance.tomefs.getStats();
  expect(stats.fullTreeSyncs).toBeGreaterThan(0);

  // Verify data integrity after initial sync via remount.
  instance = await mountTome(backend, maxPages);
  verifyDataIntegrity(instance.rawFS, model, `seed ${seed}: initial`);

  // Phase 2: Multiple dirty-shutdown + recovery cycles.
  for (let cycle = 0; cycle < syncCycles; cycle++) {
    const ctx = `seed ${seed}, cycle ${cycle}`;

    // Do rename/unlink-heavy ops WITHOUT updating model (state after
    // crash is unpredictable — some ops are eagerly persisted, others not).
    const dirtyModel = newModel();
    dirtyModel.files = new Map(model.files);
    dirtyModel.dirs = new Set(model.dirs);
    dirtyModel.symlinks = new Map(model.symlinks);

    let hadMarkerInvalidation = false;
    for (let i = 0; i < dirtyOps; i++) {
      const op = generateOp(rng, dirtyModel);
      if (execOp(instance.rawFS, op)) {
        updateModel(dirtyModel, op);
        if (op.type === "renameFile" || op.type === "unlink" ||
            op.type === "renameDir" || op.type === "renameSymlink" ||
            op.type === "unlinkSymlink") {
          hadMarkerInvalidation = true;
        }
      }
    }

    // Force marker invalidation if random ops didn't produce one.
    if (!hadMarkerInvalidation) {
      if (dirtyModel.files.size > 0) {
        const path = [...dirtyModel.files.keys()][0];
        try {
          instance.rawFS.unlink(rw(path));
          hadMarkerInvalidation = true;
        } catch { /* ignore */ }
      }
      if (!hadMarkerInvalidation) {
        const tempPath = `/__force_${cycle}`;
        try {
          const s = instance.rawFS.open(rw(tempPath), O.WRONLY | O.CREAT | O.TRUNC, 0o666);
          instance.rawFS.write(s, rng.bytes(10), 0, 10, 0);
          instance.rawFS.close(s);
          instance.rawFS.unlink(rw(tempPath));
        } catch { /* ignore */ }
      }
    }

    // Dirty shutdown: remount WITHOUT syncfs.
    instance = await mountTome(backend, maxPages);

    instance.tomefs.assertInvariants();
    backend.assertInvariants();
    walkAndVerifyNavigable(instance.rawFS, `${ctx}: post-crash`);

    // Recovery syncfs: should take the full-tree-walk path.
    instance.tomefs.resetStats();
    doSyncfs(instance.rawFS);
    stats = instance.tomefs.getStats();

    expect(
      stats.fullTreeSyncs,
      `${ctx}: expected full tree walk on recovery sync`,
    ).toBeGreaterThan(0);

    instance.tomefs.assertInvariants();
    backend.assertInvariants();
    verifyNoOrphans(backend, `${ctx}: post-recovery`);

    // Verify full-tree → incremental transition: second syncfs should
    // use the incremental path (needsOrphanCleanup cleared).
    instance.tomefs.resetStats();
    doSyncfs(instance.rawFS);
    stats = instance.tomefs.getStats();
    expect(
      stats.incrementalSyncs + stats.noopSyncs,
      `${ctx}: expected incremental/noop on second sync`,
    ).toBeGreaterThan(0);
    expect(
      stats.fullTreeSyncs,
      `${ctx}: unexpected full tree walk on second sync`,
    ).toBe(0);

    // Rebuild the ground-truth model from actual recovered state.
    const recoveredModel = buildModelFromFS(instance.rawFS);

    // Do clean operations, update model, sync, and verify via remount.
    for (let i = 0; i < setupOps; i++) {
      const op = generateOp(rng, recoveredModel);
      if (execOp(instance.rawFS, op)) updateModel(recoveredModel, op);
    }
    doSyncfs(instance.rawFS);
    instance.tomefs.assertInvariants();
    backend.assertInvariants();

    instance = await mountTome(backend, maxPages);
    verifyDataIntegrity(instance.rawFS, recoveredModel, `${ctx}: clean verify`);

    // Carry the verified model forward for the next cycle.
    model.files = recoveredModel.files;
    model.dirs = recoveredModel.dirs;
    model.symlinks = recoveredModel.symlinks;
  }

  // Final stability: clean sync → remount → verify.
  doSyncfs(instance.rawFS);
  instance = await mountTome(backend, maxPages);
  instance.tomefs.assertInvariants();
  backend.assertInvariants();
  verifyDataIntegrity(instance.rawFS, model, `seed ${seed}: final`);
  verifyNoOrphans(backend, `seed ${seed}: final`);
}

// ---------------------------------------------------------------
// Test cases
// ---------------------------------------------------------------

describe("fuzz: full-tree-walk syncfs path", () => {
  describe("tiny cache (4 pages) — every write evicts", () => {
    it("seed 80001: 15 setup + 20 dirty × 3 cycles @fast", async () => {
      await runFullTreeWalkTest({
        seed: 80001, setupOps: 15, dirtyOps: 20, maxPages: 4, syncCycles: 3,
      });
    }, 60_000);

    it("seed 80002: 10 setup + 30 dirty × 4 cycles", async () => {
      await runFullTreeWalkTest({
        seed: 80002, setupOps: 10, dirtyOps: 30, maxPages: 4, syncCycles: 4,
      });
    }, 60_000);

    it("seed 80003: 20 setup + 15 dirty × 5 cycles", async () => {
      await runFullTreeWalkTest({
        seed: 80003, setupOps: 20, dirtyOps: 15, maxPages: 4, syncCycles: 5,
      });
    }, 60_000);
  });

  describe("small cache (16 pages) — moderate eviction", () => {
    it("seed 81001: 20 setup + 25 dirty × 3 cycles @fast", async () => {
      await runFullTreeWalkTest({
        seed: 81001, setupOps: 20, dirtyOps: 25, maxPages: 16, syncCycles: 3,
      });
    }, 60_000);

    it("seed 81002: 15 setup + 35 dirty × 4 cycles", async () => {
      await runFullTreeWalkTest({
        seed: 81002, setupOps: 15, dirtyOps: 35, maxPages: 16, syncCycles: 4,
      });
    }, 60_000);

    it("seed 81003: 25 setup + 20 dirty × 3 cycles", async () => {
      await runFullTreeWalkTest({
        seed: 81003, setupOps: 25, dirtyOps: 20, maxPages: 16, syncCycles: 3,
      });
    }, 60_000);
  });

  describe("medium cache (64 pages) — partial working set", () => {
    it("seed 82001: 25 setup + 30 dirty × 3 cycles @fast", async () => {
      await runFullTreeWalkTest({
        seed: 82001, setupOps: 25, dirtyOps: 30, maxPages: 64, syncCycles: 3,
      });
    }, 60_000);

    it("seed 82002: 20 setup + 40 dirty × 4 cycles", async () => {
      await runFullTreeWalkTest({
        seed: 82002, setupOps: 20, dirtyOps: 40, maxPages: 64, syncCycles: 4,
      });
    }, 60_000);
  });

  describe("large cache (4096 pages) — no eviction", () => {
    it("seed 83001: 30 setup + 30 dirty × 3 cycles @fast", async () => {
      await runFullTreeWalkTest({
        seed: 83001, setupOps: 30, dirtyOps: 30, maxPages: 4096, syncCycles: 3,
      });
    }, 60_000);

    it("seed 83002: 20 setup + 50 dirty × 4 cycles", async () => {
      await runFullTreeWalkTest({
        seed: 83002, setupOps: 20, dirtyOps: 50, maxPages: 4096, syncCycles: 4,
      });
    }, 60_000);
  });

  describe("many short cycles — frequent crash recovery", () => {
    it("seed 84001: 8 setup + 8 dirty × 8 cycles, tiny cache @fast", async () => {
      await runFullTreeWalkTest({
        seed: 84001, setupOps: 8, dirtyOps: 8, maxPages: 4, syncCycles: 8,
      });
    }, 120_000);

    it("seed 84002: 5 setup + 10 dirty × 10 cycles, small cache", async () => {
      await runFullTreeWalkTest({
        seed: 84002, setupOps: 5, dirtyOps: 10, maxPages: 16, syncCycles: 10,
      });
    }, 120_000);
  });

  describe("heavy dirty phase — many mutations before crash", () => {
    it("seed 85001: 10 setup + 60 dirty × 2 cycles, tiny cache", async () => {
      await runFullTreeWalkTest({
        seed: 85001, setupOps: 10, dirtyOps: 60, maxPages: 4, syncCycles: 2,
      });
    }, 60_000);

    it("seed 85002: 10 setup + 60 dirty × 2 cycles, small cache @fast", async () => {
      await runFullTreeWalkTest({
        seed: 85002, setupOps: 10, dirtyOps: 60, maxPages: 16, syncCycles: 2,
      });
    }, 60_000);
  });
});
