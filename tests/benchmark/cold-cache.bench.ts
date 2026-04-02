/**
 * Performance benchmarks: cold-cache scenarios.
 *
 * These benchmarks measure the cost of cache misses — the most important
 * performance characteristic of a bounded cache system. When the working set
 * exceeds the cache, every access may require a backend read and an eviction.
 *
 * Complements throughput.bench.ts (warm-cache) by measuring:
 * - Sequential scan of a file larger than the cache
 * - Random reads with a working set that doesn't fit in cache
 * - Write-heavy workload under cache pressure (dirty eviction cost)
 * - Flush cost with many dirty pages
 *
 * Run: npx vitest bench tests/benchmark/cold-cache.bench.ts
 */

import { bench, describe } from "vitest";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { createTomeFS } from "../../src/tomefs.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { PAGE_SIZE } from "../../src/types.js";
import type { EmscriptenFS } from "../harness/emscripten-fs.js";
import { O, SEEK_SET } from "../harness/emscripten-fs.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

const TOME_MOUNT = "/tome";

function rewritePath(p: string): string {
  if (!p.startsWith("/")) return p;
  if (p.startsWith(TOME_MOUNT + "/") || p === TOME_MOUNT) return p;
  if (p.startsWith("/dev") || p.startsWith("/proc") || p.startsWith("/tmp"))
    return p;
  if (p === "/") return TOME_MOUNT;
  return TOME_MOUNT + p;
}

interface BenchHarness {
  FS: EmscriptenFS;
  label: string;
}

async function createTomeFSHarness(maxPages: number): Promise<BenchHarness> {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  const Module = await createModule();
  const rawFS = Module.FS;

  const backend = new SyncMemoryBackend();
  const tomefs = createTomeFS(rawFS, { backend, maxPages });

  rawFS.mkdir(TOME_MOUNT);
  rawFS.mount(tomefs, {}, TOME_MOUNT);

  const methodCache = new Map<string, Function>();
  const pathMethods = new Set([
    "open", "stat", "lstat", "truncate", "mkdir", "rmdir",
    "readdir", "unlink", "writeFile", "readFile", "mknod",
    "chmod", "utime",
  ]);

  const wrappedFS = new Proxy(rawFS, {
    get(target: any, prop: string) {
      const cached = methodCache.get(prop);
      if (cached) return cached;
      const val = target[prop];
      if (typeof val !== "function") return val;
      let wrapped: Function;
      if (pathMethods.has(prop)) {
        wrapped = (path: string, ...args: any[]) =>
          val.call(target, rewritePath(path), ...args);
      } else if (prop === "rename") {
        wrapped = (oldPath: string, newPath: string) =>
          val.call(target, rewritePath(oldPath), rewritePath(newPath));
      } else {
        wrapped = val.bind(target);
      }
      methodCache.set(prop, wrapped);
      return wrapped;
    },
  }) as unknown as EmscriptenFS;

  return { FS: wrappedFS, label: `tomefs-${maxPages}` };
}

// Reusable data buffers
const PAGE_DATA = new Uint8Array(PAGE_SIZE);
for (let i = 0; i < PAGE_SIZE; i++) PAGE_DATA[i] = (i * 31 + 17) & 0xff;
const READ_BUF = new Uint8Array(PAGE_SIZE);

// ---------------------------------------------------------------------------
// Cold Sequential Scan: file is 4x larger than the cache
// ---------------------------------------------------------------------------

const COLD_FILE_PAGES = 256; // 2 MB file
const COLD_CACHE_PAGES = 64; // 512 KB cache — file is 4x larger

describe("Cold Sequential Scan (256-page file, 64-page cache)", async () => {
  const large = await createTomeFSHarness(4096); // warm: everything fits
  const cold = await createTomeFSHarness(COLD_CACHE_PAGES); // cold: 4x eviction pressure

  // Pre-populate files
  for (const h of [large, cold]) {
    const s = h.FS.open("/cold_seq", O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    for (let i = 0; i < COLD_FILE_PAGES; i++) {
      h.FS.write(s, PAGE_DATA, 0, PAGE_SIZE);
    }
    h.FS.close(s);
  }

  function seqScan(h: BenchHarness) {
    const stream = h.FS.open("/cold_seq", O.RDONLY);
    for (let i = 0; i < COLD_FILE_PAGES; i++) {
      h.FS.read(stream, READ_BUF, 0, PAGE_SIZE);
    }
    h.FS.close(stream);
  }

  bench("warm cache (4096 pages)", () => seqScan(large));
  bench("cold cache (64 pages)", () => seqScan(cold));
});

// ---------------------------------------------------------------------------
// Cold Random Reads: random access to a file much larger than the cache
// ---------------------------------------------------------------------------

const RAND_FILE_PAGES = 512; // 4 MB file
const RAND_CACHE_PAGES = 32; // 256 KB cache — 16x smaller
const RAND_READS = 200;
// Worst-case: maximize cache misses by spreading reads uniformly
const COLD_RAND_INDICES = Array.from({ length: RAND_READS }, (_, i) =>
  (i * 251 + 13) % RAND_FILE_PAGES,
);

describe("Cold Random Read (512-page file, 32-page cache, 200 reads)", async () => {
  const warm = await createTomeFSHarness(4096);
  const cold = await createTomeFSHarness(RAND_CACHE_PAGES);

  for (const h of [warm, cold]) {
    const s = h.FS.open("/cold_rand", O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    for (let i = 0; i < RAND_FILE_PAGES; i++) {
      h.FS.write(s, PAGE_DATA, 0, PAGE_SIZE);
    }
    h.FS.close(s);
  }

  function randRead(h: BenchHarness) {
    const stream = h.FS.open("/cold_rand", O.RDONLY);
    for (const idx of COLD_RAND_INDICES) {
      h.FS.llseek(stream, idx * PAGE_SIZE, SEEK_SET);
      h.FS.read(stream, READ_BUF, 0, PAGE_SIZE);
    }
    h.FS.close(stream);
  }

  bench("warm cache (4096 pages)", () => randRead(warm));
  bench("cold cache (32 pages)", () => randRead(cold));
});

// ---------------------------------------------------------------------------
// Dirty Eviction: writes under cache pressure (evicted pages must be flushed)
// ---------------------------------------------------------------------------

const DIRTY_FILE_PAGES = 128;
const DIRTY_CACHE_PAGES = 16; // 8x pressure

describe("Dirty Eviction (128-page write, 16-page cache)", async () => {
  const warm = await createTomeFSHarness(4096);
  const cold = await createTomeFSHarness(DIRTY_CACHE_PAGES);

  function dirtyWrite(h: BenchHarness) {
    const stream = h.FS.open("/dirty_evict", O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    for (let i = 0; i < DIRTY_FILE_PAGES; i++) {
      h.FS.write(stream, PAGE_DATA, 0, PAGE_SIZE);
    }
    h.FS.close(stream);
    h.FS.unlink("/dirty_evict");
  }

  bench("warm cache (4096 pages)", () => dirtyWrite(warm));
  bench("cold cache (16 pages, dirty eviction)", () => dirtyWrite(cold));
});

// ---------------------------------------------------------------------------
// Batch Flush: syncfs cost with many dirty pages across multiple files
// ---------------------------------------------------------------------------

const FLUSH_FILES = 20;
const FLUSH_PAGES_PER_FILE = 8;

describe(`Batch Flush (${FLUSH_FILES} files × ${FLUSH_PAGES_PER_FILE} dirty pages)`, async () => {
  const large = await createTomeFSHarness(4096);
  const small = await createTomeFSHarness(64);

  function dirtyAndFlush(h: BenchHarness) {
    // Create dirty pages across many files
    for (let f = 0; f < FLUSH_FILES; f++) {
      const s = h.FS.open(
        `/flush_${f}`,
        O.WRONLY | O.CREAT | O.TRUNC,
        0o666,
      );
      for (let p = 0; p < FLUSH_PAGES_PER_FILE; p++) {
        h.FS.write(s, PAGE_DATA, 0, PAGE_SIZE);
      }
      h.FS.close(s);
    }

    // Sync forces a flush of all dirty pages
    // Use callback-style since syncfs is async in the Emscripten API
    h.FS.syncfs(false, () => {});

    // Clean up for next iteration
    for (let f = 0; f < FLUSH_FILES; f++) {
      h.FS.unlink(`/flush_${f}`);
    }
  }

  bench("large cache (4096 pages)", () => dirtyAndFlush(large));
  bench("small cache (64 pages, eviction during write)", () =>
    dirtyAndFlush(small),
  );
});

// ---------------------------------------------------------------------------
// Multi-File Working Set Rotation: simulate DB with many relations
// ---------------------------------------------------------------------------

const ROTATION_FILES = 16;
const ROTATION_PAGES_PER_FILE = 16; // total 256 pages across all files
const ROTATION_CACHE = 32; // only 12.5% fits
const ROTATION_READS = 100;

describe("Multi-File Working Set Rotation (16 files, 32-page cache)", async () => {
  const warm = await createTomeFSHarness(4096);
  const cold = await createTomeFSHarness(ROTATION_CACHE);

  for (const h of [warm, cold]) {
    for (let f = 0; f < ROTATION_FILES; f++) {
      const s = h.FS.open(
        `/rot_${f}`,
        O.WRONLY | O.CREAT | O.TRUNC,
        0o666,
      );
      for (let p = 0; p < ROTATION_PAGES_PER_FILE; p++) {
        h.FS.write(s, PAGE_DATA, 0, PAGE_SIZE);
      }
      h.FS.close(s);
    }
  }

  function rotate(h: BenchHarness) {
    for (let r = 0; r < ROTATION_READS; r++) {
      // Access a different file each round, cycling through all of them
      const f = r % ROTATION_FILES;
      const p = (r * 7) % ROTATION_PAGES_PER_FILE;
      const stream = h.FS.open(`/rot_${f}`, O.RDONLY);
      h.FS.llseek(stream, p * PAGE_SIZE, SEEK_SET);
      h.FS.read(stream, READ_BUF, 0, PAGE_SIZE);
      h.FS.close(stream);
    }
  }

  bench("warm cache (4096 pages)", () => rotate(warm));
  bench("cold cache (32 pages)", () => rotate(cold));
});
