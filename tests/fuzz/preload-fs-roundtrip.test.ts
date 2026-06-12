/**
 * Fuzz tests for PreloadBackend through the filesystem API.
 *
 * Existing PreloadBackend fuzz tests (preload-flush-roundtrip.test.ts,
 * backend-invariants.test.ts) exercise the backend API directly. This
 * test exercises the full deployment stack for environments without
 * SharedArrayBuffer:
 *
 *   FS API (open/read/write/close) → tomefs → SyncPageCache
 *     → PreloadBackend → MemoryBackend → flush → remount → verify
 *
 * After batches of randomized FS operations, the test:
 *   1. Calls syncfs to push dirty pages/metadata to PreloadBackend
 *   2. Calls flush() to propagate to the remote MemoryBackend
 *   3. Creates a fresh PreloadBackend from the same remote, init()s it
 *   4. Mounts on a new Emscripten module and verifies file contents match
 *
 * This catches bugs at the seams between layers — dirty tracking,
 * page cache eviction under flush, metadata persistence ordering,
 * and PreloadBackend's deferred-write model.
 *
 * Ethos §8 (workload scenarios), §9 (adversarial — target the seams)
 */

import { describe, it, expect } from "vitest";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { createTomeFS } from "../../src/tomefs.js";
import { PreloadBackend } from "../../src/preload-backend.js";
import { MemoryBackend } from "../../src/memory-backend.js";
import { PAGE_SIZE } from "../../src/types.js";
import type { EmscriptenFS } from "../harness/emscripten-fs.js";
import { O } from "../harness/emscripten-fs.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

// ---------------------------------------------------------------
// Seeded PRNG (xorshift128+)
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
// Model: track expected file contents for verification
// ---------------------------------------------------------------

interface FileState {
  data: Uint8Array;
}

class Model {
  files = new Map<string, FileState>();

  createFile(path: string, data: Uint8Array): void {
    this.files.set(path, { data: new Uint8Array(data) });
  }

  writeAt(path: string, offset: number, data: Uint8Array): void {
    const file = this.files.get(path);
    if (!file) return;
    const newSize = Math.max(file.data.length, offset + data.length);
    if (newSize > file.data.length) {
      const expanded = new Uint8Array(newSize);
      expanded.set(file.data);
      file.data = expanded;
    }
    file.data.set(data, offset);
  }

  truncate(path: string, size: number): void {
    const file = this.files.get(path);
    if (!file) return;
    if (size < file.data.length) {
      file.data = new Uint8Array(file.data.subarray(0, size));
    } else if (size > file.data.length) {
      const expanded = new Uint8Array(size);
      expanded.set(file.data);
      file.data = expanded;
    }
  }

  rename(oldPath: string, newPath: string): void {
    const file = this.files.get(oldPath);
    if (!file) return;
    this.files.delete(oldPath);
    this.files.set(newPath, file);
  }

  unlink(path: string): void {
    this.files.delete(path);
  }
}

// ---------------------------------------------------------------
// Operations
// ---------------------------------------------------------------

type Op =
  | { type: "createFile"; path: string; data: Uint8Array }
  | { type: "writeAt"; path: string; offset: number; data: Uint8Array }
  | { type: "truncate"; path: string; size: number }
  | { type: "rename"; oldPath: string; newPath: string }
  | { type: "unlink"; path: string };

const FILE_NAMES = ["a.dat", "b.dat", "c.dat", "d.dat", "e.dat"];
const MOUNT = "/tome";

function generateOp(rng: Rng, model: Model): Op {
  const allFiles = [...model.files.keys()];

  const weights: Array<[string, number]> = [
    ["createFile", 20],
    ["writeAt", allFiles.length > 0 ? 25 : 0],
    ["truncate", allFiles.length > 0 ? 15 : 0],
    ["rename", allFiles.length > 0 ? 15 : 0],
    ["unlink", allFiles.length > 1 ? 10 : 0],
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
      const name = rng.pick(FILE_NAMES);
      const sizes = [0, 10, PAGE_SIZE - 1, PAGE_SIZE, PAGE_SIZE + 1, PAGE_SIZE * 2 + 33];
      return { type: "createFile", path: `/${name}`, data: rng.bytes(rng.pick(sizes)) };
    }
    case "writeAt": {
      const path = rng.pick(allFiles);
      const fileSize = model.files.get(path)!.data.length;
      const offset = rng.int(fileSize + PAGE_SIZE + 1);
      const sizes = [1, 50, PAGE_SIZE, PAGE_SIZE + 1];
      return { type: "writeAt", path, offset, data: rng.bytes(rng.pick(sizes)) };
    }
    case "truncate": {
      const path = rng.pick(allFiles);
      const fileSize = model.files.get(path)!.data.length;
      const choices = [0, Math.max(0, fileSize - PAGE_SIZE), fileSize, fileSize + PAGE_SIZE];
      return { type: "truncate", path, size: rng.pick(choices) };
    }
    case "rename": {
      const oldPath = rng.pick(allFiles);
      const newName = rng.pick(FILE_NAMES);
      return { type: "rename", oldPath, newPath: `/${newName}` };
    }
    case "unlink":
      return { type: "unlink", path: rng.pick(allFiles) };
    default:
      return { type: "createFile", path: `/${rng.pick(FILE_NAMES)}`, data: rng.bytes(10) };
  }
}

// ---------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------

async function createModule() {
  const { default: create } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  return await create();
}

function mountPreload(
  FS: EmscriptenFS,
  preload: PreloadBackend,
  maxPages: number,
) {
  const tomefs = createTomeFS(FS as any, { backend: preload, maxPages });
  (FS as any).mkdir(MOUNT);
  (FS as any).mount(tomefs, {}, MOUNT);
  return tomefs;
}

function syncfs(FS: any, tomefs: any): void {
  tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
    if (err) throw err;
  });
}

function execOp(FS: any, op: Op): boolean {
  try {
    switch (op.type) {
      case "createFile": {
        const s = FS.open(MOUNT + op.path, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
        if (op.data.length > 0) FS.write(s, op.data, 0, op.data.length, 0);
        FS.close(s);
        return true;
      }
      case "writeAt": {
        const s = FS.open(MOUNT + op.path, O.RDWR);
        FS.write(s, op.data, 0, op.data.length, op.offset);
        FS.close(s);
        return true;
      }
      case "truncate":
        FS.truncate(MOUNT + op.path, op.size);
        return true;
      case "rename":
        FS.rename(MOUNT + op.oldPath, MOUNT + op.newPath);
        return true;
      case "unlink":
        FS.unlink(MOUNT + op.path);
        return true;
    }
  } catch {
    return false;
  }
}

function verifyContents(
  FS: any,
  model: Model,
  context: string,
): void {
  for (const [path, file] of model.files) {
    const fullPath = MOUNT + path;
    let stat;
    try {
      stat = FS.stat(fullPath);
    } catch {
      throw new Error(`${context}: file ${path} missing after remount`);
    }
    expect(stat.size, `${context}: ${path} size`).toBe(file.data.length);

    if (file.data.length > 0) {
      const buf = new Uint8Array(file.data.length);
      const fd = FS.open(fullPath, O.RDONLY);
      const n = FS.read(fd, buf, 0, file.data.length, 0);
      FS.close(fd);
      expect(n, `${context}: ${path} bytes read`).toBe(file.data.length);
      for (let i = 0; i < file.data.length; i++) {
        if (buf[i] !== file.data[i]) {
          throw new Error(
            `${context}: ${path} data mismatch at byte ${i}: ` +
            `expected ${file.data[i]}, got ${buf[i]}`,
          );
        }
      }
    }
  }

  const listing = FS.readdir(MOUNT).filter(
    (f: string) => f !== "." && f !== "..",
  );
  const expectedFiles = new Set([...model.files.keys()].map((p) => p.slice(1)));
  for (const name of listing) {
    if (!expectedFiles.has(name)) {
      throw new Error(`${context}: unexpected file ${name} after remount`);
    }
  }
}

// ---------------------------------------------------------------
// Main fuzz driver
// ---------------------------------------------------------------

async function runPreloadFsRoundtrip(
  seed: number,
  numOps: number,
  maxPages: number,
  checkpointInterval: number,
): Promise<void> {
  const remote = new MemoryBackend();
  const model = new Model();
  const rng = new Rng(seed);

  let preload = new PreloadBackend(remote);
  await preload.init();
  let Module = await createModule();
  let FS = Module.FS;
  let tomefs = mountPreload(FS, preload, maxPages);

  syncfs(FS, tomefs);

  for (let i = 0; i < numOps; i++) {
    const op = generateOp(rng, model);
    const succeeded = execOp(FS, op);
    if (succeeded) {
      switch (op.type) {
        case "createFile": model.createFile(op.path, op.data); break;
        case "writeAt": model.writeAt(op.path, op.offset, op.data); break;
        case "truncate": model.truncate(op.path, op.size); break;
        case "rename": model.rename(op.oldPath, op.newPath); break;
        case "unlink": model.unlink(op.path); break;
      }
    }

    preload.assertInvariants();

    if ((i + 1) % checkpointInterval === 0) {
      syncfs(FS, tomefs);
      await preload.flush();
      preload.assertInvariants();

      const freshPreload = new PreloadBackend(remote);
      await freshPreload.init();
      freshPreload.assertInvariants();

      const FreshModule = await createModule();
      const freshFS = FreshModule.FS;
      mountPreload(freshFS, freshPreload, maxPages);

      verifyContents(
        freshFS,
        model,
        `seed ${seed}, checkpoint after op ${i}`,
      );
    }
  }

  syncfs(FS, tomefs);
  await preload.flush();
  preload.assertInvariants();

  const finalPreload = new PreloadBackend(remote);
  await finalPreload.init();
  finalPreload.assertInvariants();

  const FinalModule = await createModule();
  const finalFS = FinalModule.FS;
  mountPreload(finalFS, finalPreload, maxPages);

  verifyContents(finalFS, model, `seed ${seed}, final`);
}

// ---------------------------------------------------------------
// Test cases
// ---------------------------------------------------------------

describe("fuzz: PreloadBackend full-stack FS roundtrip (2-page cache)", () => {
  const CACHE = 2;
  const OPS = 40;
  const CHECKPOINT = 10;

  it("seed 90001 @fast", async () => {
    await runPreloadFsRoundtrip(90001, OPS, CACHE, CHECKPOINT);
  }, 60_000);

  it("seed 90002 @fast", async () => {
    await runPreloadFsRoundtrip(90002, OPS, CACHE, CHECKPOINT);
  }, 60_000);

  it("seed 90003", async () => {
    await runPreloadFsRoundtrip(90003, OPS, CACHE, CHECKPOINT);
  }, 60_000);

  it("seed 90004", async () => {
    await runPreloadFsRoundtrip(90004, OPS, CACHE, CHECKPOINT);
  }, 60_000);

  it("seed 90005", async () => {
    await runPreloadFsRoundtrip(90005, OPS, CACHE, CHECKPOINT);
  }, 60_000);
});

describe("fuzz: PreloadBackend full-stack FS roundtrip (4-page cache)", () => {
  const CACHE = 4;
  const OPS = 60;
  const CHECKPOINT = 15;

  it("seed 91001 @fast", async () => {
    await runPreloadFsRoundtrip(91001, OPS, CACHE, CHECKPOINT);
  }, 60_000);

  it("seed 91002", async () => {
    await runPreloadFsRoundtrip(91002, OPS, CACHE, CHECKPOINT);
  }, 60_000);

  it("seed 91003", async () => {
    await runPreloadFsRoundtrip(91003, OPS, CACHE, CHECKPOINT);
  }, 60_000);

  it("seed 91004", async () => {
    await runPreloadFsRoundtrip(91004, OPS, CACHE, CHECKPOINT);
  }, 60_000);
});

describe("fuzz: PreloadBackend full-stack FS roundtrip (extended)", () => {
  it("100 ops, 3-page cache, seed 92001", async () => {
    await runPreloadFsRoundtrip(92001, 100, 3, 20);
  }, 120_000);

  it("80 ops, 1-page cache, seed 92002", async () => {
    await runPreloadFsRoundtrip(92002, 80, 1, 10);
  }, 120_000);
});
