/**
 * Performance benchmarks: tomefs vs MEMFS.
 *
 * Ethos §6 requires performance parity with IDBFS when the working set fits
 * in the cache. These benchmarks measure the overhead of the page cache layer
 * by comparing tomefs (SyncMemoryBackend) against raw MEMFS for core I/O
 * operations.
 *
 * Run: npx vitest bench
 */

import { bench, describe, beforeEach } from "vitest";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { createTomeFS } from "../../src/tomefs.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { PAGE_SIZE } from "../../src/types.js";
import type { EmscriptenFS } from "../harness/emscripten-fs.js";
import { O, SEEK_SET } from "../harness/emscripten-fs.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

// ---------------------------------------------------------------------------
// Harness setup
// ---------------------------------------------------------------------------

const TOME_MOUNT = "/tome";

function isSystemPath(p: string): boolean {
  return p.startsWith("/dev") || p.startsWith("/proc") || p.startsWith("/tmp");
}

function rewritePath(p: string): string {
  if (!p.startsWith("/")) return p;
  if (p.startsWith(TOME_MOUNT + "/") || p === TOME_MOUNT) return p;
  if (isSystemPath(p)) return p;
  if (p === "/") return TOME_MOUNT;
  return TOME_MOUNT + p;
}

interface BenchHarness {
  FS: EmscriptenFS;
  label: string;
}

async function createMemFSHarness(): Promise<BenchHarness> {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  const Module = await createModule();
  return { FS: Module.FS as EmscriptenFS, label: "memfs" };
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

  // Wrap FS with path rewriting so benchmarks use clean paths.
  // Uses a plain object with pre-bound methods instead of a Proxy to avoid
  // unfairly penalizing tomefs benchmarks — Proxy get traps prevent V8's
  // inline cache optimization, adding ~200-500ns overhead per method access
  // that doesn't exist in the MEMFS baseline.
  const wrappedFS = Object.create(rawFS);
  const pathMethods = [
    "open", "stat", "lstat", "truncate", "mkdir", "rmdir",
    "readdir", "unlink", "writeFile", "readFile", "mknod",
    "chmod", "utime",
  ];
  for (const method of pathMethods) {
    const fn = rawFS[method];
    if (typeof fn === "function") {
      wrappedFS[method] = (path: string, ...args: any[]) =>
        fn.call(rawFS, rewritePath(path), ...args);
    }
  }
  wrappedFS.rename = (oldPath: string, newPath: string) =>
    rawFS.rename.call(rawFS, rewritePath(oldPath), rewritePath(newPath));
  // Bind non-path methods directly (read, write, close, llseek)
  for (const method of ["read", "write", "close", "llseek"] as const) {
    const fn = rawFS[method];
    if (typeof fn === "function") {
      wrappedFS[method] = fn.bind(rawFS);
    }
  }

  return { FS: wrappedFS as unknown as EmscriptenFS, label: `tomefs-${maxPages}` };
}

// Reusable data buffers
const PAGE_DATA = new Uint8Array(PAGE_SIZE);
for (let i = 0; i < PAGE_SIZE; i++) PAGE_DATA[i] = (i * 31 + 17) & 0xff;

const READ_BUF = new Uint8Array(PAGE_SIZE);

// ---------------------------------------------------------------------------
// Sequential Write: write N pages to a single file
// ---------------------------------------------------------------------------

const SEQ_PAGES = 64; // 512 KB

describe("Sequential Write (64 pages = 512 KB)", async () => {
  const memfs = await createMemFSHarness();
  const tome4096 = await createTomeFSHarness(4096); // large cache, no eviction
  const tome64 = await createTomeFSHarness(64); // cache fits exactly

  function seqWrite(h: BenchHarness) {
    const stream = h.FS.open("/bench_seqw", O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    for (let i = 0; i < SEQ_PAGES; i++) {
      h.FS.write(stream, PAGE_DATA, 0, PAGE_SIZE);
    }
    h.FS.close(stream);
    h.FS.unlink("/bench_seqw");
  }

  bench("MEMFS", () => seqWrite(memfs));
  bench("tomefs (4096 pages)", () => seqWrite(tome4096));
  bench("tomefs (64 pages)", () => seqWrite(tome64));
});

// ---------------------------------------------------------------------------
// Sequential Read: read N pages from a single file
// ---------------------------------------------------------------------------

describe("Sequential Read (64 pages = 512 KB)", async () => {
  const memfs = await createMemFSHarness();
  const tome4096 = await createTomeFSHarness(4096);
  const tome64 = await createTomeFSHarness(64);

  // Pre-populate files
  for (const h of [memfs, tome4096, tome64]) {
    const s = h.FS.open("/bench_seqr", O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    for (let i = 0; i < SEQ_PAGES; i++) {
      h.FS.write(s, PAGE_DATA, 0, PAGE_SIZE);
    }
    h.FS.close(s);
  }

  function seqRead(h: BenchHarness) {
    const stream = h.FS.open("/bench_seqr", O.RDONLY);
    for (let i = 0; i < SEQ_PAGES; i++) {
      h.FS.read(stream, READ_BUF, 0, PAGE_SIZE);
    }
    h.FS.close(stream);
  }

  bench("MEMFS", () => seqRead(memfs));
  bench("tomefs (4096 pages)", () => seqRead(tome4096));
  bench("tomefs (64 pages)", () => seqRead(tome64));
});

// ---------------------------------------------------------------------------
// Random Read: seek to random pages and read
// ---------------------------------------------------------------------------

const RANDOM_FILE_PAGES = 128; // 1 MB file
const RANDOM_READS = 100;
// Pre-compute deterministic "random" page indices
const RANDOM_INDICES = Array.from({ length: RANDOM_READS }, (_, i) =>
  (i * 41 + 7) % RANDOM_FILE_PAGES,
);

describe("Random Read (100 reads from 128-page file)", async () => {
  const memfs = await createMemFSHarness();
  const tome4096 = await createTomeFSHarness(4096);
  const tome128 = await createTomeFSHarness(128); // exact fit
  const tome16 = await createTomeFSHarness(16); // heavy eviction

  // Pre-populate
  for (const h of [memfs, tome4096, tome128, tome16]) {
    const s = h.FS.open("/bench_rand", O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    for (let i = 0; i < RANDOM_FILE_PAGES; i++) {
      h.FS.write(s, PAGE_DATA, 0, PAGE_SIZE);
    }
    h.FS.close(s);
  }

  function randRead(h: BenchHarness) {
    const stream = h.FS.open("/bench_rand", O.RDONLY);
    for (const idx of RANDOM_INDICES) {
      h.FS.llseek(stream, idx * PAGE_SIZE, SEEK_SET);
      h.FS.read(stream, READ_BUF, 0, PAGE_SIZE);
    }
    h.FS.close(stream);
  }

  bench("MEMFS", () => randRead(memfs));
  bench("tomefs (4096 pages)", () => randRead(tome4096));
  bench("tomefs (128 pages)", () => randRead(tome128));
  bench("tomefs (16 pages, eviction)", () => randRead(tome16));
});

// ---------------------------------------------------------------------------
// Small Writes: sub-page writes (simulating WAL records)
// ---------------------------------------------------------------------------

const SMALL_WRITE_COUNT = 500;
const SMALL_WRITE_SIZE = 128; // bytes
const SMALL_DATA = new Uint8Array(SMALL_WRITE_SIZE);
for (let i = 0; i < SMALL_WRITE_SIZE; i++) SMALL_DATA[i] = (i * 13 + 5) & 0xff;

describe("Small Writes (500 x 128B appends)", async () => {
  const memfs = await createMemFSHarness();
  const tome4096 = await createTomeFSHarness(4096);

  function smallWrites(h: BenchHarness) {
    const stream = h.FS.open("/bench_small", O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    for (let i = 0; i < SMALL_WRITE_COUNT; i++) {
      h.FS.write(stream, SMALL_DATA, 0, SMALL_WRITE_SIZE);
    }
    h.FS.close(stream);
    h.FS.unlink("/bench_small");
  }

  bench("MEMFS", () => smallWrites(memfs));
  bench("tomefs (4096 pages)", () => smallWrites(tome4096));
});

// ---------------------------------------------------------------------------
// File Create/Delete Churn
// ---------------------------------------------------------------------------

const CHURN_COUNT = 100;
const CHURN_DATA = new Uint8Array(PAGE_SIZE);

describe("File Create/Delete Churn (100 files)", async () => {
  const memfs = await createMemFSHarness();
  const tome4096 = await createTomeFSHarness(4096);

  function churn(h: BenchHarness) {
    for (let i = 0; i < CHURN_COUNT; i++) {
      const path = `/bench_churn_${i}`;
      const s = h.FS.open(path, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
      h.FS.write(s, CHURN_DATA, 0, PAGE_SIZE);
      h.FS.close(s);
      h.FS.unlink(path);
    }
  }

  bench("MEMFS", () => churn(memfs));
  bench("tomefs (4096 pages)", () => churn(tome4096));
});

// ---------------------------------------------------------------------------
// Cross-Page Boundary Writes
// ---------------------------------------------------------------------------

const CROSS_PAGE_WRITES = 200;
// Write data that straddles a page boundary
const CROSS_SIZE = 256;
const CROSS_DATA = new Uint8Array(CROSS_SIZE);
for (let i = 0; i < CROSS_SIZE; i++) CROSS_DATA[i] = (i * 7 + 3) & 0xff;

describe("Cross-Page Boundary Writes (200 writes)", async () => {
  const memfs = await createMemFSHarness();
  const tome4096 = await createTomeFSHarness(4096);

  function crossPageWrites(h: BenchHarness) {
    const stream = h.FS.open("/bench_cross", O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    for (let i = 0; i < CROSS_PAGE_WRITES; i++) {
      // Position at PAGE_SIZE - 128, so each write straddles the boundary
      const pos = i * PAGE_SIZE + (PAGE_SIZE - 128);
      h.FS.llseek(stream, pos, SEEK_SET);
      h.FS.write(stream, CROSS_DATA, 0, CROSS_SIZE);
    }
    h.FS.close(stream);
    h.FS.unlink("/bench_cross");
  }

  bench("MEMFS", () => crossPageWrites(memfs));
  bench("tomefs (4096 pages)", () => crossPageWrites(tome4096));
});

// ---------------------------------------------------------------------------
// Mixed Read/Write on Multiple Files
// ---------------------------------------------------------------------------

const MIX_FILES = 8;
const MIX_PAGES_PER_FILE = 8;
const MIX_ROUNDS = 50;

describe("Mixed Multi-File Read/Write (8 files, 50 rounds)", async () => {
  const memfs = await createMemFSHarness();
  const tome4096 = await createTomeFSHarness(4096);
  const tome16 = await createTomeFSHarness(16); // heavy eviction

  // Pre-populate
  for (const h of [memfs, tome4096, tome16]) {
    for (let f = 0; f < MIX_FILES; f++) {
      const s = h.FS.open(`/bench_mix_${f}`, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
      for (let p = 0; p < MIX_PAGES_PER_FILE; p++) {
        h.FS.write(s, PAGE_DATA, 0, PAGE_SIZE);
      }
      h.FS.close(s);
    }
  }

  function mixedOps(h: BenchHarness) {
    for (let r = 0; r < MIX_ROUNDS; r++) {
      const fRead = r % MIX_FILES;
      const fWrite = (r * 3 + 1) % MIX_FILES;
      const pageIdx = (r * 7) % MIX_PAGES_PER_FILE;

      // Read a page from one file
      const rs = h.FS.open(`/bench_mix_${fRead}`, O.RDONLY);
      h.FS.llseek(rs, pageIdx * PAGE_SIZE, SEEK_SET);
      h.FS.read(rs, READ_BUF, 0, PAGE_SIZE);
      h.FS.close(rs);

      // Append to another file
      const ws = h.FS.open(`/bench_mix_${fWrite}`, O.WRONLY | O.APPEND);
      h.FS.write(ws, SMALL_DATA, 0, SMALL_WRITE_SIZE);
      h.FS.close(ws);
    }
  }

  bench("MEMFS", () => mixedOps(memfs));
  bench("tomefs (4096 pages)", () => mixedOps(tome4096));
  bench("tomefs (16 pages, eviction)", () => mixedOps(tome16));
});
