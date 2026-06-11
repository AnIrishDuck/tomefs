/**
 * Fuzz tests for syncfs retry after transient backend failures.
 *
 * Generates random filesystem operations, then periodically injects syncAll
 * failures. Verifies two properties:
 *
 * 1. **Retry idempotency**: after a transient failure, calling syncfs again
 *    succeeds and all data is durable (verified by remount).
 *
 * 2. **Mutation accumulation**: mutations made between a failed syncfs and
 *    its retry are included in the retry — nothing is lost.
 *
 * Uses a fault-injecting backend fake (not a mock) that wraps
 * SyncMemoryBackend and fails syncAll with configurable probability.
 *
 * Runs at multiple cache pressure levels to exercise eviction during the
 * retry window: when syncAll fails, dirty flags are preserved, so eviction
 * during the failure→retry gap must flush via writePage (not syncAll).
 *
 * Ethos §9: adversarial differential testing targeting syncfs seams.
 */

import { describe, it, expect } from "vitest";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { createTomeFS } from "../../src/tomefs.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { PAGE_SIZE } from "../../src/types.js";
import type { SyncStorageBackend } from "../../src/sync-storage-backend.js";
import type { FileMeta } from "../../src/types.js";

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
    for (let i = 0; i < length; i++) buf[i] = this.next() & 0xff;
    return buf;
  }

  bool(probability: number): boolean {
    return (this.next() / 0x100000000) < probability;
  }
}

// ---------------------------------------------------------------
// Fault-injecting backend fake
// ---------------------------------------------------------------

class TransientFailBackend implements SyncStorageBackend {
  readonly inner = new SyncMemoryBackend();
  failNextSyncAll = false;
  totalFailures = 0;

  readPage(path: string, pageIndex: number): Uint8Array | null {
    return this.inner.readPage(path, pageIndex);
  }
  readPages(path: string, pageIndices: number[]): Array<Uint8Array | null> {
    return this.inner.readPages(path, pageIndices);
  }
  writePage(path: string, pageIndex: number, data: Uint8Array): void {
    this.inner.writePage(path, pageIndex, data);
  }
  writePages(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
  ): void {
    this.inner.writePages(pages);
  }
  deleteFile(path: string): void {
    this.inner.deleteFile(path);
  }
  deleteFiles(paths: string[]): void {
    this.inner.deleteFiles(paths);
  }
  deletePagesFrom(path: string, fromPageIndex: number): void {
    this.inner.deletePagesFrom(path, fromPageIndex);
  }
  renameFile(oldPath: string, newPath: string): void {
    this.inner.renameFile(oldPath, newPath);
  }
  countPages(path: string): number {
    return this.inner.countPages(path);
  }
  countPagesBatch(paths: string[]): number[] {
    return this.inner.countPagesBatch(paths);
  }
  maxPageIndex(path: string): number {
    return this.inner.maxPageIndex(path);
  }
  maxPageIndexBatch(paths: string[]): number[] {
    return this.inner.maxPageIndexBatch(paths);
  }
  readMeta(path: string): FileMeta | null {
    return this.inner.readMeta(path);
  }
  readMetas(paths: string[]): Array<FileMeta | null> {
    return this.inner.readMetas(paths);
  }
  writeMeta(path: string, meta: FileMeta): void {
    this.inner.writeMeta(path, meta);
  }
  writeMetas(entries: Array<{ path: string; meta: FileMeta }>): void {
    this.inner.writeMetas(entries);
  }
  deleteMeta(path: string): void {
    this.inner.deleteMeta(path);
  }
  deleteMetas(paths: string[]): void {
    this.inner.deleteMetas(paths);
  }
  listFiles(): string[] {
    return this.inner.listFiles();
  }
  syncAll(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
    metas: Array<{ path: string; meta: FileMeta }>,
  ): void {
    if (this.failNextSyncAll) {
      this.failNextSyncAll = false;
      this.totalFailures++;
      throw new Error("transient syncAll failure (injected)");
    }
    this.inner.syncAll(pages, metas);
  }
  deleteAll(paths: string[]): void {
    this.inner.deleteAll(paths);
  }
  cleanupOrphanedPages(): number {
    return this.inner.cleanupOrphanedPages();
  }
  assertInvariants(): void {
    this.inner.assertInvariants();
  }
}

// ---------------------------------------------------------------
// Model: tracks expected file contents for verification
// ---------------------------------------------------------------

interface FileModel {
  contents: Uint8Array;
}

interface Model {
  files: Map<string, FileModel>;
  dirs: Set<string>;
}

function newModel(): Model {
  return { files: new Map(), dirs: new Set(["/"]) };
}

function writeModelFile(
  model: Model,
  path: string,
  data: Uint8Array,
  offset: number,
): void {
  let file = model.files.get(path);
  if (!file) {
    file = { contents: new Uint8Array(0) };
    model.files.set(path, file);
  }
  const newSize = Math.max(file.contents.length, offset + data.length);
  if (newSize > file.contents.length) {
    const expanded = new Uint8Array(newSize);
    expanded.set(file.contents);
    file.contents = expanded;
  }
  file.contents.set(data, offset);
}

function truncateModelFile(model: Model, path: string, size: number): void {
  const file = model.files.get(path);
  if (!file) return;
  if (size < file.contents.length) {
    file.contents = file.contents.slice(0, size);
  } else if (size > file.contents.length) {
    const expanded = new Uint8Array(size);
    expanded.set(file.contents);
    file.contents = expanded;
  }
}

// ---------------------------------------------------------------
// Operation types
// ---------------------------------------------------------------

const O = {
  RDONLY: 0,
  WRONLY: 1,
  RDWR: 2,
  CREAT: 64,
  TRUNC: 512,
} as const;

const MOUNT = "/tome";
const FILE_NAMES = ["a.dat", "b.dat", "c.dat", "d.dat", "e.dat"];
const DIR_NAMES = ["d1", "d2"];

type Op =
  | { type: "createFile"; path: string; data: Uint8Array }
  | { type: "writeAt"; path: string; offset: number; data: Uint8Array }
  | { type: "truncate"; path: string; size: number }
  | { type: "renameFile"; oldPath: string; newPath: string }
  | { type: "unlink"; path: string }
  | { type: "mkdir"; path: string };

function generateOp(rng: Rng, model: Model): Op {
  const allFiles = [...model.files.keys()];
  const allDirs = [...model.dirs];

  const weights: Array<[string, number]> = [
    ["createFile", 20],
    ["writeAt", allFiles.length > 0 ? 25 : 0],
    ["truncate", allFiles.length > 0 ? 10 : 0],
    ["renameFile", allFiles.length > 0 ? 12 : 0],
    ["unlink", allFiles.length > 1 ? 8 : 0],
    ["mkdir", 5],
  ];

  const total = weights.reduce((s, [, w]) => s + w, 0);
  let choice = rng.int(total);
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
      const dir = rng.pick(allDirs);
      const name = rng.pick(FILE_NAMES);
      const path = dir === "/" ? `/${name}` : `${dir}/${name}`;
      const sizes = [0, 10, PAGE_SIZE - 1, PAGE_SIZE, PAGE_SIZE + 1, PAGE_SIZE * 2 + 33];
      return { type: "createFile", path, data: rng.bytes(rng.pick(sizes)) };
    }
    case "writeAt": {
      const path = rng.pick(allFiles);
      const cur = model.files.get(path)!.contents.length;
      const offset = rng.int(Math.max(1, cur + PAGE_SIZE));
      const sizes = [1, 50, PAGE_SIZE, PAGE_SIZE + 1];
      return { type: "writeAt", path, offset, data: rng.bytes(rng.pick(sizes)) };
    }
    case "truncate": {
      const path = rng.pick(allFiles);
      const cur = model.files.get(path)!.contents.length;
      const choices = [0, Math.max(0, cur - PAGE_SIZE), cur, cur + PAGE_SIZE];
      return { type: "truncate", path, size: rng.pick(choices) };
    }
    case "renameFile": {
      const oldPath = rng.pick(allFiles);
      const dir = rng.pick(allDirs);
      const name = rng.pick(FILE_NAMES);
      const newPath = dir === "/" ? `/${name}` : `${dir}/${name}`;
      return { type: "renameFile", oldPath, newPath };
    }
    case "unlink":
      return { type: "unlink", path: rng.pick(allFiles) };
    case "mkdir": {
      const parent = rng.pick(allDirs);
      const name = rng.pick(DIR_NAMES);
      const path = parent === "/" ? `/${name}` : `${parent}/${name}`;
      return { type: "mkdir", path };
    }
    default:
      return { type: "createFile", path: "/fallback.dat", data: rng.bytes(10) };
  }
}

function applyToModel(op: Op, model: Model): void {
  switch (op.type) {
    case "createFile": {
      const file = { contents: new Uint8Array(op.data) };
      model.files.set(op.path, file);
      break;
    }
    case "writeAt":
      writeModelFile(model, op.path, op.data, op.offset);
      break;
    case "truncate":
      truncateModelFile(model, op.path, op.size);
      break;
    case "renameFile": {
      const file = model.files.get(op.oldPath);
      if (file) {
        model.files.delete(op.oldPath);
        model.files.set(op.newPath, file);
      }
      break;
    }
    case "unlink":
      model.files.delete(op.path);
      break;
    case "mkdir":
      model.dirs.add(op.path);
      break;
  }
}

function execOp(FS: any, op: Op): boolean {
  try {
    switch (op.type) {
      case "createFile": {
        const s = FS.open(
          `${MOUNT}${op.path}`,
          O.WRONLY | O.CREAT | O.TRUNC,
          0o666,
        );
        if (op.data.length > 0) FS.write(s, op.data, 0, op.data.length, 0);
        FS.close(s);
        return true;
      }
      case "writeAt": {
        const s = FS.open(`${MOUNT}${op.path}`, O.RDWR);
        FS.write(s, op.data, 0, op.data.length, op.offset);
        FS.close(s);
        return true;
      }
      case "truncate":
        FS.truncate(`${MOUNT}${op.path}`, op.size);
        return true;
      case "renameFile":
        FS.rename(`${MOUNT}${op.oldPath}`, `${MOUNT}${op.newPath}`);
        return true;
      case "unlink":
        FS.unlink(`${MOUNT}${op.path}`);
        return true;
      case "mkdir":
        FS.mkdir(`${MOUNT}${op.path}`, 0o777);
        return true;
    }
  } catch {
    return false;
  }
}

// ---------------------------------------------------------------
// Verification: remount and compare file contents to model
// ---------------------------------------------------------------

async function verifyViaRemount(
  backend: TransientFailBackend,
  model: Model,
  seed: number,
  phase: string,
): Promise<void> {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  const Module = await createModule();
  const FS2 = Module.FS as any;
  const tomefs2 = createTomeFS(FS2, { backend, maxPages: 256 });
  FS2.mkdir(MOUNT);
  FS2.mount(tomefs2, {}, MOUNT);

  for (const [path, file] of model.files) {
    const fullPath = `${MOUNT}${path}`;
    let stat;
    try {
      stat = FS2.stat(fullPath);
    } catch {
      throw new Error(
        `[seed=${seed}, ${phase}] File ${path} missing after remount`,
      );
    }
    const expectedSize = file.contents.length;
    if (stat.size !== expectedSize) {
      throw new Error(
        `[seed=${seed}, ${phase}] File ${path}: size ${stat.size} != expected ${expectedSize}`,
      );
    }
    if (expectedSize > 0) {
      const buf = new Uint8Array(expectedSize);
      const fd = FS2.open(fullPath, O.RDONLY);
      FS2.read(fd, buf, 0, expectedSize, 0);
      FS2.close(fd);
      for (let i = 0; i < expectedSize; i++) {
        if (buf[i] !== file.contents[i]) {
          throw new Error(
            `[seed=${seed}, ${phase}] File ${path}: byte ${i} is ${buf[i]}, expected ${file.contents[i]}`,
          );
        }
      }
    }
  }

  FS2.unmount(MOUNT);
}

// ---------------------------------------------------------------
// Main fuzz driver
// ---------------------------------------------------------------

async function runSyncfsRetryFuzz(
  seed: number,
  numOps: number,
  maxPages: number,
  failureProbability: number,
): Promise<number> {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  const Module = await createModule();
  const FS = Module.FS as any;

  const backend = new TransientFailBackend();
  const tomefs = createTomeFS(FS, { backend, maxPages });
  FS.mkdir(MOUNT);
  FS.mount(tomefs, {}, MOUNT);

  const rng = new Rng(seed);
  const model = newModel();

  const doSyncfs = (expectFail: boolean): boolean => {
    let err: Error | null = null;
    tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (e: Error | null) => {
      err = e;
    });
    if (err && !expectFail) {
      throw new Error(`Unexpected syncfs failure: ${(err as Error).message}`);
    }
    return err === null;
  };

  // Initial sync to establish baseline
  doSyncfs(false);

  let failureInjections = 0;

  for (let i = 0; i < numOps; i++) {
    const op = generateOp(rng, model);
    const succeeded = execOp(FS, op);
    if (succeeded) {
      applyToModel(op, model);
    }

    // Periodically attempt syncfs with possible failure
    if ((i + 1) % 8 === 0) {
      if (rng.bool(failureProbability)) {
        // Inject failure
        backend.failNextSyncAll = true;
        failureInjections++;
        const syncOk = doSyncfs(true);
        expect(syncOk).toBe(false);

        // Optionally do more mutations between failure and retry
        if (rng.bool(0.5)) {
          const extraOps = rng.int(4) + 1;
          for (let j = 0; j < extraOps; j++) {
            const extraOp = generateOp(rng, model);
            if (execOp(FS, extraOp)) {
              applyToModel(extraOp, model);
            }
          }
        }

        // Retry must succeed
        doSyncfs(false);

        // Invariants must hold after retry
        tomefs.assertInvariants();
        backend.assertInvariants();
      } else {
        // Normal syncfs (no failure)
        doSyncfs(false);
      }
    }
  }

  // Close any remaining state and do final syncfs
  doSyncfs(false);
  tomefs.assertInvariants();
  backend.assertInvariants();

  // Verify all file contents via remount
  await verifyViaRemount(backend, model, seed, "final");

  return failureInjections;
}

// ---------------------------------------------------------------
// Test cases
// ---------------------------------------------------------------

describe("syncfs retry after transient failures", () => {
  const SEEDS = [1001, 2002, 3003, 4004, 5005, 6006, 7007, 8008];

  describe("moderate cache (32 pages)", () => {
    it.each(SEEDS.map((s) => [s]))("seed %i: retry preserves all data", async (seed) => {
      const injections = await runSyncfsRetryFuzz(seed, 120, 32, 0.4);
      expect(injections).toBeGreaterThan(0);
    });
  });

  describe("extreme cache pressure (3 pages)", () => {
    it.each(SEEDS.map((s) => [s]))("seed %i: retry under eviction pressure", async (seed) => {
      const injections = await runSyncfsRetryFuzz(seed, 80, 3, 0.5);
      expect(injections).toBeGreaterThan(0);
    });
  });

  describe("high failure rate (80%)", () => {
    it.each(SEEDS.slice(0, 4).map((s) => [s]))("seed %i: repeated failures then success", async (seed) => {
      const injections = await runSyncfsRetryFuzz(seed, 60, 16, 0.8);
      expect(injections).toBeGreaterThan(0);
    });
  });

  it("mutations between failure and retry are preserved @fast", async () => {
    const { default: createModule } = await import(
      join(__dirname, "../harness/emscripten_fs.mjs")
    );
    const Module = await createModule();
    const FS = Module.FS as any;

    const backend = new TransientFailBackend();
    const tomefs = createTomeFS(FS, { backend, maxPages: 16 });
    FS.mkdir(MOUNT);
    FS.mount(tomefs, {}, MOUNT);

    const syncfs = (expectFail: boolean) => {
      let err: Error | null = null;
      tomefs.syncfs(
        FS.lookupPath(MOUNT).node.mount,
        false,
        (e: Error | null) => { err = e; },
      );
      if (err && !expectFail) throw err;
      return err === null;
    };

    // Create initial file and sync
    const data1 = new Uint8Array([1, 2, 3, 4, 5]);
    const fd1 = FS.open(`${MOUNT}/file1`, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    FS.write(fd1, data1, 0, data1.length, 0);
    FS.close(fd1);
    syncfs(false);

    // Write update, then fail syncfs
    const data2 = new Uint8Array([10, 20, 30, 40, 50]);
    const fd2 = FS.open(`${MOUNT}/file1`, O.WRONLY, 0o666);
    FS.write(fd2, data2, 0, data2.length, 0);
    FS.close(fd2);

    backend.failNextSyncAll = true;
    expect(syncfs(true)).toBe(false);

    // Mutation BETWEEN failure and retry: create a new file
    const data3 = new Uint8Array([100, 200]);
    const fd3 = FS.open(`${MOUNT}/file2`, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    FS.write(fd3, data3, 0, data3.length, 0);
    FS.close(fd3);

    // Retry must include both the original update AND the new file
    syncfs(false);

    // Verify via remount
    const Module2 = await (await import(join(__dirname, "../harness/emscripten_fs.mjs"))).default();
    const FS2 = Module2.FS as any;
    const tomefs2 = createTomeFS(FS2, { backend, maxPages: 64 });
    FS2.mkdir(MOUNT);
    FS2.mount(tomefs2, {}, MOUNT);

    // file1 should have the updated content
    const buf1 = new Uint8Array(5);
    const rfd1 = FS2.open(`${MOUNT}/file1`, O.RDONLY);
    FS2.read(rfd1, buf1, 0, 5, 0);
    FS2.close(rfd1);
    expect(Array.from(buf1)).toEqual([10, 20, 30, 40, 50]);

    // file2 should exist with correct content
    const buf2 = new Uint8Array(2);
    const rfd2 = FS2.open(`${MOUNT}/file2`, O.RDONLY);
    FS2.read(rfd2, buf2, 0, 2, 0);
    FS2.close(rfd2);
    expect(Array.from(buf2)).toEqual([100, 200]);
  });

  it("failure during full-tree-walk path retries correctly @fast", async () => {
    const { default: createModule } = await import(
      join(__dirname, "../harness/emscripten_fs.mjs")
    );
    const Module = await createModule();
    const FS = Module.FS as any;

    const backend = new TransientFailBackend();
    const tomefs = createTomeFS(FS, { backend, maxPages: 16 });
    FS.mkdir(MOUNT);
    FS.mount(tomefs, {}, MOUNT);

    const syncfs = (expectFail: boolean) => {
      let err: Error | null = null;
      tomefs.syncfs(
        FS.lookupPath(MOUNT).node.mount,
        false,
        (e: Error | null) => { err = e; },
      );
      if (err && !expectFail) throw err;
      return err === null;
    };

    // Create two files and sync
    const rng = new Rng(42);
    const fileData = rng.bytes(PAGE_SIZE * 2 + 100);
    const fd = FS.open(`${MOUNT}/keep`, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    FS.write(fd, fileData, 0, fileData.length, 0);
    FS.close(fd);

    const dummy = rng.bytes(50);
    const dfd = FS.open(`${MOUNT}/remove`, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    FS.write(dfd, dummy, 0, dummy.length, 0);
    FS.close(dfd);
    syncfs(false);

    // Update keep, unlink remove → triggers full-tree-walk (orphan cleanup)
    const update = rng.bytes(PAGE_SIZE);
    const fd2 = FS.open(`${MOUNT}/keep`, O.WRONLY, 0o666);
    FS.write(fd2, update, 0, update.length, 0);
    FS.close(fd2);
    FS.unlink(`${MOUNT}/remove`);

    // Fail syncfs on the full-tree-walk path
    backend.failNextSyncAll = true;
    expect(syncfs(true)).toBe(false);

    // Retry
    syncfs(false);
    tomefs.assertInvariants();

    // Verify via remount
    const Module2 = await (await import(join(__dirname, "../harness/emscripten_fs.mjs"))).default();
    const FS2 = Module2.FS as any;
    const tomefs2 = createTomeFS(FS2, { backend, maxPages: 64 });
    FS2.mkdir(MOUNT);
    FS2.mount(tomefs2, {}, MOUNT);

    // keep should have the updated content (first PAGE_SIZE bytes replaced)
    const expected = new Uint8Array(fileData.length);
    expected.set(fileData);
    expected.set(update, 0);

    const buf = new Uint8Array(expected.length);
    const rfd = FS2.open(`${MOUNT}/keep`, O.RDONLY);
    FS2.read(rfd, buf, 0, buf.length, 0);
    FS2.close(rfd);
    for (let i = 0; i < expected.length; i++) {
      expect(buf[i]).toBe(expected[i]);
    }

    // remove should not exist
    expect(() => FS2.stat(`${MOUNT}/remove`)).toThrow();
  });

  it("eviction during failure window flushes dirty pages correctly @fast", async () => {
    const { default: createModule } = await import(
      join(__dirname, "../harness/emscripten_fs.mjs")
    );
    const Module = await createModule();
    const FS = Module.FS as any;

    const backend = new TransientFailBackend();
    const tomefs = createTomeFS(FS, { backend, maxPages: 3 });
    FS.mkdir(MOUNT);
    FS.mount(tomefs, {}, MOUNT);

    const syncfs = (expectFail: boolean) => {
      let err: Error | null = null;
      tomefs.syncfs(
        FS.lookupPath(MOUNT).node.mount,
        false,
        (e: Error | null) => { err = e; },
      );
      if (err && !expectFail) throw err;
      return err === null;
    };

    // Fill cache (3 pages) with file A
    const dataA = new Uint8Array(PAGE_SIZE * 3);
    for (let i = 0; i < dataA.length; i++) dataA[i] = (i * 7 + 3) & 0xff;
    const fdA = FS.open(`${MOUNT}/fileA`, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    FS.write(fdA, dataA, 0, dataA.length, 0);
    FS.close(fdA);

    // Fail syncfs — dirty pages for A are NOT committed
    backend.failNextSyncAll = true;
    expect(syncfs(true)).toBe(false);

    // Write to file B — this EVICTS dirty pages from A.
    // Since dirty flags are preserved, eviction flushes A's pages via writePage.
    const dataB = new Uint8Array(PAGE_SIZE * 3);
    for (let i = 0; i < dataB.length; i++) dataB[i] = (i * 11 + 5) & 0xff;
    const fdB = FS.open(`${MOUNT}/fileB`, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    FS.write(fdB, dataB, 0, dataB.length, 0);
    FS.close(fdB);

    // Retry syncfs — should succeed, both A and B persisted
    syncfs(false);
    tomefs.assertInvariants();
    backend.assertInvariants();

    // Verify via remount
    const Module2 = await (await import(join(__dirname, "../harness/emscripten_fs.mjs"))).default();
    const FS2 = Module2.FS as any;
    const tomefs2 = createTomeFS(FS2, { backend, maxPages: 64 });
    FS2.mkdir(MOUNT);
    FS2.mount(tomefs2, {}, MOUNT);

    const bufA = new Uint8Array(PAGE_SIZE * 3);
    const rfdA = FS2.open(`${MOUNT}/fileA`, O.RDONLY);
    FS2.read(rfdA, bufA, 0, bufA.length, 0);
    FS2.close(rfdA);
    for (let i = 0; i < dataA.length; i++) {
      expect(bufA[i]).toBe(dataA[i]);
    }

    const bufB = new Uint8Array(PAGE_SIZE * 3);
    const rfdB = FS2.open(`${MOUNT}/fileB`, O.RDONLY);
    FS2.read(rfdB, bufB, 0, bufB.length, 0);
    FS2.close(rfdB);
    for (let i = 0; i < dataB.length; i++) {
      expect(bufB[i]).toBe(dataB[i]);
    }
  });
});
