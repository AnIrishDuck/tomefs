/**
 * Cache eviction & mmap/msync performance benchmarks.
 *
 * Complements throughput.bench.ts (which has a few eviction data points in
 * scattered benchmarks) by systematically measuring:
 *
 * 1. Eviction overhead at different cache pressure levels
 * 2. Dirty vs clean eviction penalty (dirty must flush before evict)
 * 3. mmap+msync write throughput (Postgres's primary buffer write path)
 * 4. Working set rotation (sequential scan that exceeds cache)
 * 5. Multi-file cache competition (concurrent table/index access)
 *
 * Ethos §6 requires performance parity with IDBFS when the working set fits
 * in cache. These benchmarks measure what happens at and beyond that boundary.
 *
 * Run: npx vitest bench tests/benchmark/eviction.bench.ts
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
  rawFS: EmscriptenFS;
  label: string;
}

async function createMemFSHarness(): Promise<BenchHarness> {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  const Module = await createModule();
  const fs = Module.FS as EmscriptenFS;
  return { FS: fs, rawFS: fs, label: "memfs" };
}

async function createTomeFSHarness(maxPages: number): Promise<BenchHarness> {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  const Module = await createModule();
  const rawFS = Module.FS as EmscriptenFS;

  const backend = new SyncMemoryBackend();
  const tomefs = createTomeFS(rawFS, { backend, maxPages });

  rawFS.mkdir(TOME_MOUNT);
  rawFS.mount(tomefs, {}, TOME_MOUNT);

  const methodCache = new Map<string, Function>();
  const pathMethods = new Set([
    "open", "stat", "lstat", "truncate", "mkdir", "rmdir",
    "readdir", "unlink", "writeFile", "readFile", "mknod",
    "chmod", "utime", "syncfs",
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

  return { FS: wrappedFS, rawFS, label: `tomefs-${maxPages}` };
}

// Reusable data buffers
const PAGE_DATA = new Uint8Array(PAGE_SIZE);
for (let i = 0; i < PAGE_SIZE; i++) PAGE_DATA[i] = (i * 31 + 17) & 0xff;

const READ_BUF = new Uint8Array(PAGE_SIZE);

// ---------------------------------------------------------------------------
// 1. Clean Eviction: read-only workload that exceeds cache
// ---------------------------------------------------------------------------

const EVICT_FILE_PAGES = 64; // 512 KB file

describe("Clean Eviction: sequential read of 64-page file", async () => {
  const memfs = await createMemFSHarness();
  const tomeNoEvict = await createTomeFSHarness(128);  // no eviction
  const tome64 = await createTomeFSHarness(64);         // exact fit
  const tome32 = await createTomeFSHarness(32);         // 2x oversubscribed
  const tome8 = await createTomeFSHarness(8);           // 8x oversubscribed

  // Pre-populate files
  for (const h of [memfs, tomeNoEvict, tome64, tome32, tome8]) {
    const s = h.FS.open("/evict_clean", O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    for (let i = 0; i < EVICT_FILE_PAGES; i++) {
      h.FS.write(s, PAGE_DATA, 0, PAGE_SIZE);
    }
    h.FS.close(s);
  }

  function seqRead(h: BenchHarness) {
    const stream = h.FS.open("/evict_clean", O.RDONLY);
    for (let i = 0; i < EVICT_FILE_PAGES; i++) {
      h.FS.read(stream, READ_BUF, 0, PAGE_SIZE);
    }
    h.FS.close(stream);
  }

  bench("MEMFS (baseline)", () => seqRead(memfs));
  bench("tomefs (128 pages, no eviction)", () => seqRead(tomeNoEvict));
  bench("tomefs (64 pages, exact fit)", () => seqRead(tome64));
  bench("tomefs (32 pages, 2x pressure)", () => seqRead(tome32));
  bench("tomefs (8 pages, 8x pressure)", () => seqRead(tome8));
});

// ---------------------------------------------------------------------------
// 2. Dirty Eviction: write-heavy workload that forces dirty page flush
// ---------------------------------------------------------------------------

describe("Dirty Eviction: sequential write of 64-page file", async () => {
  const memfs = await createMemFSHarness();
  const tomeNoEvict = await createTomeFSHarness(128);
  const tome64 = await createTomeFSHarness(64);
  const tome32 = await createTomeFSHarness(32);
  const tome8 = await createTomeFSHarness(8);

  function seqWrite(h: BenchHarness) {
    const stream = h.FS.open("/evict_dirty", O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    for (let i = 0; i < EVICT_FILE_PAGES; i++) {
      h.FS.write(stream, PAGE_DATA, 0, PAGE_SIZE);
    }
    h.FS.close(stream);
    h.FS.unlink("/evict_dirty");
  }

  bench("MEMFS (baseline)", () => seqWrite(memfs));
  bench("tomefs (128 pages, no eviction)", () => seqWrite(tomeNoEvict));
  bench("tomefs (64 pages, exact fit)", () => seqWrite(tome64));
  bench("tomefs (32 pages, 2x pressure)", () => seqWrite(tome32));
  bench("tomefs (8 pages, 8x pressure)", () => seqWrite(tome8));
});

// ---------------------------------------------------------------------------
// 3. mmap+msync Write Throughput vs regular write()
//
// Postgres uses mmap to read pages into a buffer, modify them, then msync
// to write back. This measures the overhead of that path vs direct write().
// ---------------------------------------------------------------------------

const MMAP_PAGES = 32;

// Note: MEMFS mmap is not benchmarked because the Emscripten harness does
// not export emscripten_builtin_memalign. tomefs implements its own mmap/msync
// at the page cache level, which is the path Postgres actually uses.

describe("mmap+msync vs write(): 32 single-page writes", async () => {
  const memfs = await createMemFSHarness();
  const tome4096 = await createTomeFSHarness(4096);
  const tome32 = await createTomeFSHarness(32);

  // Pre-populate files for all harnesses
  for (const h of [memfs, tome4096, tome32]) {
    const s = h.FS.open("/mmap_bench", O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    for (let i = 0; i < MMAP_PAGES; i++) {
      h.FS.write(s, PAGE_DATA, 0, PAGE_SIZE);
    }
    h.FS.close(s);
  }

  // Regular write() path: seek + write each page
  function writePages(h: BenchHarness) {
    const stream = h.FS.open("/mmap_bench", O.WRONLY);
    for (let i = 0; i < MMAP_PAGES; i++) {
      h.FS.llseek(stream, i * PAGE_SIZE, SEEK_SET);
      h.FS.write(stream, PAGE_DATA, 0, PAGE_SIZE);
    }
    h.FS.close(stream);
  }

  // mmap+msync path: mmap page, modify buffer, msync back
  // Only available through tomefs (not MEMFS in this harness)
  function mmapMsyncPages(h: BenchHarness) {
    const stream = h.FS.open("/mmap_bench", O.RDWR);
    for (let i = 0; i < MMAP_PAGES; i++) {
      const result = (stream as any).stream_ops.mmap(
        stream, PAGE_SIZE, i * PAGE_SIZE, 3, 1,
      );
      result.ptr.set(PAGE_DATA);
      (stream as any).stream_ops.msync(
        stream, result.ptr, i * PAGE_SIZE, PAGE_SIZE, 0,
      );
    }
    h.FS.close(stream);
  }

  bench("MEMFS write()", () => writePages(memfs));
  bench("tomefs (4096) write()", () => writePages(tome4096));
  bench("tomefs (32) write()", () => writePages(tome32));
  bench("tomefs (4096) mmap+msync", () => mmapMsyncPages(tome4096));
  bench("tomefs (32) mmap+msync", () => mmapMsyncPages(tome32));
});

// ---------------------------------------------------------------------------
// 4. mmap+msync Multi-Page (WAL-style bulk write)
//
// Simulates Postgres WAL writes: mmap a multi-page region, write a batch
// of records, then msync the whole region at once.
// ---------------------------------------------------------------------------

const WAL_PAGES = 16;
const WAL_ROUNDS = 10;

describe("mmap+msync WAL-style bulk write (16 pages x 10 rounds)", async () => {
  const tome4096 = await createTomeFSHarness(4096);
  const tome16 = await createTomeFSHarness(16); // exact cache fit

  // Pre-populate WAL file
  for (const h of [tome4096, tome16]) {
    const s = h.FS.open("/wal_bench", O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    for (let i = 0; i < WAL_PAGES; i++) {
      h.FS.write(s, PAGE_DATA, 0, PAGE_SIZE);
    }
    h.FS.close(s);
  }

  function walBulkWrite(h: BenchHarness) {
    const stream = h.FS.open("/wal_bench", O.RDWR);
    for (let r = 0; r < WAL_ROUNDS; r++) {
      const regionSize = WAL_PAGES * PAGE_SIZE;
      const result = (stream as any).stream_ops.mmap(
        stream, regionSize, 0, 3, 1,
      );
      // Simulate writing WAL records across the region
      for (let i = 0; i < regionSize; i += 256) {
        result.ptr[i] = (r + i) & 0xff;
      }
      (stream as any).stream_ops.msync(
        stream, result.ptr, 0, regionSize, 0,
      );
    }
    h.FS.close(stream);
  }

  bench("tomefs (4096 pages)", () => walBulkWrite(tome4096));
  bench("tomefs (16 pages, exact fit)", () => walBulkWrite(tome16));
});

// ---------------------------------------------------------------------------
// 5. Working Set Rotation: sequential scan over data larger than cache
//
// Simulates a full table scan where the relation doesn't fit in cache.
// Each pass reads the entire file, forcing continuous eviction.
// ---------------------------------------------------------------------------

const ROTATION_FILE_PAGES = 128; // 1 MB file
const ROTATION_PASSES = 3;

describe("Working Set Rotation: 3 full passes over 128-page file", async () => {
  const memfs = await createMemFSHarness();
  const tome4096 = await createTomeFSHarness(4096); // no eviction
  const tome64 = await createTomeFSHarness(64);     // 2x rotation
  const tome16 = await createTomeFSHarness(16);     // 8x rotation

  // Pre-populate
  for (const h of [memfs, tome4096, tome64, tome16]) {
    const s = h.FS.open("/rotation", O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    for (let i = 0; i < ROTATION_FILE_PAGES; i++) {
      h.FS.write(s, PAGE_DATA, 0, PAGE_SIZE);
    }
    h.FS.close(s);
  }

  function fullScanPasses(h: BenchHarness) {
    for (let pass = 0; pass < ROTATION_PASSES; pass++) {
      const stream = h.FS.open("/rotation", O.RDONLY);
      for (let i = 0; i < ROTATION_FILE_PAGES; i++) {
        h.FS.read(stream, READ_BUF, 0, PAGE_SIZE);
      }
      h.FS.close(stream);
    }
  }

  bench("MEMFS (baseline)", () => fullScanPasses(memfs));
  bench("tomefs (4096 pages, no eviction)", () => fullScanPasses(tome4096));
  bench("tomefs (64 pages, 2x rotation)", () => fullScanPasses(tome64));
  bench("tomefs (16 pages, 8x rotation)", () => fullScanPasses(tome16));
});

// ---------------------------------------------------------------------------
// 6. Multi-File Cache Competition
//
// Simulates PGlite accessing multiple relations simultaneously: a table,
// an index, and the WAL. Each gets random reads/writes, competing for
// cache slots.
// ---------------------------------------------------------------------------

const COMPETITION_PAGES_PER_FILE = 16;
const COMPETITION_FILES = 4;
const COMPETITION_ROUNDS = 100;
// Pre-computed deterministic access pattern
const COMP_FILE_INDICES = Array.from({ length: COMPETITION_ROUNDS }, (_, i) =>
  (i * 3 + 7) % COMPETITION_FILES,
);
const COMP_PAGE_INDICES = Array.from({ length: COMPETITION_ROUNDS }, (_, i) =>
  (i * 41 + 13) % COMPETITION_PAGES_PER_FILE,
);

describe("Multi-File Competition: 4 files, 100 random accesses", async () => {
  const memfs = await createMemFSHarness();
  const tome4096 = await createTomeFSHarness(4096); // all 64 pages fit
  const tome32 = await createTomeFSHarness(32);      // half fit
  const tome8 = await createTomeFSHarness(8);         // heavy thrashing

  // Pre-populate files
  for (const h of [memfs, tome4096, tome32, tome8]) {
    for (let f = 0; f < COMPETITION_FILES; f++) {
      const s = h.FS.open(`/comp_${f}`, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
      for (let p = 0; p < COMPETITION_PAGES_PER_FILE; p++) {
        h.FS.write(s, PAGE_DATA, 0, PAGE_SIZE);
      }
      h.FS.close(s);
    }
  }

  function competingAccess(h: BenchHarness) {
    for (let r = 0; r < COMPETITION_ROUNDS; r++) {
      const f = COMP_FILE_INDICES[r];
      const p = COMP_PAGE_INDICES[r];
      const isWrite = r % 3 === 0; // 1/3 writes, 2/3 reads

      const flags = isWrite ? O.RDWR : O.RDONLY;
      const stream = h.FS.open(`/comp_${f}`, flags);
      h.FS.llseek(stream, p * PAGE_SIZE, SEEK_SET);

      if (isWrite) {
        h.FS.write(stream, PAGE_DATA, 0, PAGE_SIZE);
      } else {
        h.FS.read(stream, READ_BUF, 0, PAGE_SIZE);
      }
      h.FS.close(stream);
    }
  }

  bench("MEMFS (baseline)", () => competingAccess(memfs));
  bench("tomefs (4096 pages, no eviction)", () => competingAccess(tome4096));
  bench("tomefs (32 pages, 50% fit)", () => competingAccess(tome32));
  bench("tomefs (8 pages, heavy thrashing)", () => competingAccess(tome8));
});

// ---------------------------------------------------------------------------
// 7. Dirty Flush Cost: measure syncfs cost scaling with dirty page count
// ---------------------------------------------------------------------------

const FLUSH_FILE_PAGES = 32;

describe("Syncfs Flush Cost: scaling with dirty page count", async () => {
  const tome4096 = await createTomeFSHarness(4096);

  // Pre-populate file
  {
    const s = tome4096.FS.open("/flush_scale", O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    for (let i = 0; i < FLUSH_FILE_PAGES; i++) {
      tome4096.FS.write(s, PAGE_DATA, 0, PAGE_SIZE);
    }
    tome4096.FS.close(s);
    // Initial sync to start clean
    tome4096.rawFS.syncfs(false, (err: Error | null) => { if (err) throw err; });
  }

  // Dirty 1 page, then flush
  bench("1 dirty page", () => {
    const s = tome4096.FS.open("/flush_scale", O.WRONLY);
    tome4096.FS.write(s, PAGE_DATA, 0, PAGE_SIZE);
    tome4096.FS.close(s);
    tome4096.rawFS.syncfs(false, (err: Error | null) => { if (err) throw err; });
  });

  // Dirty 8 pages, then flush
  bench("8 dirty pages", () => {
    const s = tome4096.FS.open("/flush_scale", O.WRONLY);
    for (let i = 0; i < 8; i++) {
      tome4096.FS.llseek(s, i * PAGE_SIZE, SEEK_SET);
      tome4096.FS.write(s, PAGE_DATA, 0, PAGE_SIZE);
    }
    tome4096.FS.close(s);
    tome4096.rawFS.syncfs(false, (err: Error | null) => { if (err) throw err; });
  });

  // Dirty all 32 pages, then flush
  bench("32 dirty pages", () => {
    const s = tome4096.FS.open("/flush_scale", O.WRONLY);
    for (let i = 0; i < FLUSH_FILE_PAGES; i++) {
      tome4096.FS.llseek(s, i * PAGE_SIZE, SEEK_SET);
      tome4096.FS.write(s, PAGE_DATA, 0, PAGE_SIZE);
    }
    tome4096.FS.close(s);
    tome4096.rawFS.syncfs(false, (err: Error | null) => { if (err) throw err; });
  });
});
