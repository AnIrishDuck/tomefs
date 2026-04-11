/**
 * Metadata & syncfs performance benchmarks: tomefs vs MEMFS.
 *
 * Complements throughput.bench.ts (raw I/O) and pglite.bench.ts (SQL) by
 * measuring metadata-intensive operations and syncfs flush overhead.
 * These are the paths optimized by batch SAB bridge operations (readMetas,
 * writeMetas, deleteFiles, maxPageIndexBatch) and syncfs orphan-skip logic.
 *
 * Run: npx vitest bench tests/benchmark/metadata.bench.ts
 */

import { bench, describe } from "vitest";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { createTomeFS } from "../../src/tomefs.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { PAGE_SIZE } from "../../src/types.js";
import type { EmscriptenFS } from "../harness/emscripten-fs.js";
import { O } from "../harness/emscripten-fs.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

// ---------------------------------------------------------------------------
// Harness setup (shared with throughput.bench.ts)
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

// Reusable data buffer
const PAGE_DATA = new Uint8Array(PAGE_SIZE);
for (let i = 0; i < PAGE_SIZE; i++) PAGE_DATA[i] = (i * 31 + 17) & 0xff;

// ---------------------------------------------------------------------------
// Stat: repeated stat calls on existing files
// ---------------------------------------------------------------------------

const STAT_FILES = 20;
const STAT_CALLS = 200;

describe("Stat Calls (200 stats across 20 files)", async () => {
  const memfs = await createMemFSHarness();
  const tome4096 = await createTomeFSHarness(4096);

  // Pre-populate files
  for (const h of [memfs, tome4096]) {
    h.FS.mkdir("/bench_stat_dir");
    for (let i = 0; i < STAT_FILES; i++) {
      const s = h.FS.open(`/bench_stat_dir/f${i}`, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
      h.FS.write(s, PAGE_DATA, 0, PAGE_SIZE);
      h.FS.close(s);
    }
  }

  function statCalls(h: BenchHarness) {
    for (let i = 0; i < STAT_CALLS; i++) {
      h.FS.stat(`/bench_stat_dir/f${i % STAT_FILES}`);
    }
  }

  bench("MEMFS", () => statCalls(memfs));
  bench("tomefs (4096 pages)", () => statCalls(tome4096));
});

// ---------------------------------------------------------------------------
// Readdir: list directories of varying sizes
// ---------------------------------------------------------------------------

const READDIR_ENTRIES = 50;
const READDIR_CALLS = 50;

describe("Readdir (50 calls on 50-entry directory)", async () => {
  const memfs = await createMemFSHarness();
  const tome4096 = await createTomeFSHarness(4096);

  for (const h of [memfs, tome4096]) {
    h.FS.mkdir("/bench_readdir");
    for (let i = 0; i < READDIR_ENTRIES; i++) {
      const s = h.FS.open(`/bench_readdir/file_${i}`, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
      h.FS.write(s, PAGE_DATA, 0, 64);
      h.FS.close(s);
    }
  }

  function readdirCalls(h: BenchHarness) {
    for (let i = 0; i < READDIR_CALLS; i++) {
      h.FS.readdir("/bench_readdir");
    }
  }

  bench("MEMFS", () => readdirCalls(memfs));
  bench("tomefs (4096 pages)", () => readdirCalls(tome4096));
});

// ---------------------------------------------------------------------------
// Rename: rename files within the same directory
// ---------------------------------------------------------------------------

const RENAME_COUNT = 50;

describe("Rename (50 file renames, same directory)", async () => {
  const memfs = await createMemFSHarness();
  const tome4096 = await createTomeFSHarness(4096);

  for (const h of [memfs, tome4096]) {
    h.FS.mkdir("/bench_rename");
    for (let i = 0; i < RENAME_COUNT; i++) {
      const s = h.FS.open(`/bench_rename/f${i}`, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
      h.FS.write(s, PAGE_DATA, 0, PAGE_SIZE);
      h.FS.close(s);
    }
  }

  function renameCalls(h: BenchHarness) {
    // Rename f0→g0, f1→g1, ...
    for (let i = 0; i < RENAME_COUNT; i++) {
      h.FS.rename(`/bench_rename/f${i}`, `/bench_rename/g${i}`);
    }
    // Rename back: g0→f0, g1→f1, ...
    for (let i = 0; i < RENAME_COUNT; i++) {
      h.FS.rename(`/bench_rename/g${i}`, `/bench_rename/f${i}`);
    }
  }

  bench("MEMFS", () => renameCalls(memfs));
  bench("tomefs (4096 pages)", () => renameCalls(tome4096));
});

// ---------------------------------------------------------------------------
// Truncate: truncate files to various sizes
// ---------------------------------------------------------------------------

const TRUNC_FILES = 20;
const TRUNC_ROUNDS = 10;
const TRUNC_PAGES = 8; // 64 KB files

describe("Truncate (10 rounds of truncate/extend on 20 files)", async () => {
  const memfs = await createMemFSHarness();
  const tome4096 = await createTomeFSHarness(4096);
  const tome32 = await createTomeFSHarness(32); // cache pressure

  for (const h of [memfs, tome4096, tome32]) {
    h.FS.mkdir("/bench_trunc");
    for (let i = 0; i < TRUNC_FILES; i++) {
      const s = h.FS.open(`/bench_trunc/f${i}`, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
      for (let p = 0; p < TRUNC_PAGES; p++) {
        h.FS.write(s, PAGE_DATA, 0, PAGE_SIZE);
      }
      h.FS.close(s);
    }
  }

  function truncateCalls(h: BenchHarness) {
    for (let r = 0; r < TRUNC_ROUNDS; r++) {
      for (let i = 0; i < TRUNC_FILES; i++) {
        // Truncate to half size
        h.FS.truncate(`/bench_trunc/f${i}`, TRUNC_PAGES * PAGE_SIZE / 2);
      }
      for (let i = 0; i < TRUNC_FILES; i++) {
        // Extend back to full size (sparse)
        h.FS.truncate(`/bench_trunc/f${i}`, TRUNC_PAGES * PAGE_SIZE);
      }
    }
  }

  bench("MEMFS", () => truncateCalls(memfs));
  bench("tomefs (4096 pages)", () => truncateCalls(tome4096));
  bench("tomefs (32 pages, eviction)", () => truncateCalls(tome32));
});

// ---------------------------------------------------------------------------
// Chmod: repeated mode changes
// ---------------------------------------------------------------------------

const CHMOD_FILES = 20;
const CHMOD_ROUNDS = 100;

describe("Chmod (100 rounds on 20 files)", async () => {
  const memfs = await createMemFSHarness();
  const tome4096 = await createTomeFSHarness(4096);

  for (const h of [memfs, tome4096]) {
    h.FS.mkdir("/bench_chmod");
    for (let i = 0; i < CHMOD_FILES; i++) {
      const s = h.FS.open(`/bench_chmod/f${i}`, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
      h.FS.write(s, PAGE_DATA, 0, 64);
      h.FS.close(s);
    }
  }

  function chmodCalls(h: BenchHarness) {
    for (let r = 0; r < CHMOD_ROUNDS; r++) {
      const mode = r % 2 === 0 ? 0o644 : 0o755;
      for (let i = 0; i < CHMOD_FILES; i++) {
        h.FS.chmod(`/bench_chmod/f${i}`, mode);
      }
    }
  }

  bench("MEMFS", () => chmodCalls(memfs));
  bench("tomefs (4096 pages)", () => chmodCalls(tome4096));
});

// ---------------------------------------------------------------------------
// Mkdir/Rmdir Churn: create and remove directories
// ---------------------------------------------------------------------------

const DIR_CHURN_COUNT = 100;

describe("Mkdir/Rmdir Churn (100 directories)", async () => {
  const memfs = await createMemFSHarness();
  const tome4096 = await createTomeFSHarness(4096);

  for (const h of [memfs, tome4096]) {
    h.FS.mkdir("/bench_dirchurn");
  }

  function dirChurn(h: BenchHarness) {
    for (let i = 0; i < DIR_CHURN_COUNT; i++) {
      h.FS.mkdir(`/bench_dirchurn/d${i}`);
    }
    for (let i = 0; i < DIR_CHURN_COUNT; i++) {
      h.FS.rmdir(`/bench_dirchurn/d${i}`);
    }
  }

  bench("MEMFS", () => dirChurn(memfs));
  bench("tomefs (4096 pages)", () => dirChurn(tome4096));
});

// ---------------------------------------------------------------------------
// Directory Rename: rename directory trees (triggers descendant metadata ops)
// ---------------------------------------------------------------------------

const DIR_RENAME_FILES_PER_DIR = 10;
const DIR_RENAME_ROUNDS = 20;

describe("Directory Rename (20 renames of 10-file directories)", async () => {
  const memfs = await createMemFSHarness();
  const tome4096 = await createTomeFSHarness(4096);

  for (const h of [memfs, tome4096]) {
    h.FS.mkdir("/bench_dirren");
    h.FS.mkdir("/bench_dirren/src");
    for (let i = 0; i < DIR_RENAME_FILES_PER_DIR; i++) {
      const s = h.FS.open(`/bench_dirren/src/f${i}`, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
      h.FS.write(s, PAGE_DATA, 0, PAGE_SIZE);
      h.FS.close(s);
    }
  }

  function dirRename(h: BenchHarness) {
    for (let r = 0; r < DIR_RENAME_ROUNDS; r++) {
      if (r % 2 === 0) {
        h.FS.rename("/bench_dirren/src", "/bench_dirren/dst");
      } else {
        h.FS.rename("/bench_dirren/dst", "/bench_dirren/src");
      }
    }
  }

  bench("MEMFS", () => dirRename(memfs));
  bench("tomefs (4096 pages)", () => dirRename(tome4096));
});

// ---------------------------------------------------------------------------
// Syncfs Flush: measure cost of persisting dirty pages + metadata
// ---------------------------------------------------------------------------

const SYNCFS_FILES = 20;
const SYNCFS_PAGES_PER_FILE = 4;

describe("Syncfs Flush (20 files x 4 pages, all dirty)", async () => {
  const tome4096 = await createTomeFSHarness(4096);
  const tome32 = await createTomeFSHarness(32); // cache pressure

  // Pre-populate for both harnesses
  for (const h of [tome4096, tome32]) {
    h.FS.mkdir("/bench_syncfs");
    for (let i = 0; i < SYNCFS_FILES; i++) {
      const s = h.FS.open(`/bench_syncfs/f${i}`, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
      for (let p = 0; p < SYNCFS_PAGES_PER_FILE; p++) {
        h.FS.write(s, PAGE_DATA, 0, PAGE_SIZE);
      }
      h.FS.close(s);
    }
  }

  function syncfsFlush(h: BenchHarness) {
    // Dirty all files by writing 1 byte to each
    const oneByte = new Uint8Array([0x42]);
    for (let i = 0; i < SYNCFS_FILES; i++) {
      const s = h.FS.open(`/bench_syncfs/f${i}`, O.WRONLY, 0o666);
      h.FS.write(s, oneByte, 0, 1);
      h.FS.close(s);
    }
    // Flush
    h.rawFS.syncfs(false, (err: Error | null) => {
      if (err) throw err;
    });
  }

  bench("tomefs (4096 pages)", () => syncfsFlush(tome4096));
  bench("tomefs (32 pages, eviction)", () => syncfsFlush(tome32));
});

// ---------------------------------------------------------------------------
// Syncfs No-Op: measure cost when nothing is dirty (orphan skip path)
// ---------------------------------------------------------------------------

describe("Syncfs No-Op (no dirty pages, no tree mutations)", async () => {
  const tome4096 = await createTomeFSHarness(4096);

  // Create files and do initial sync
  tome4096.FS.mkdir("/bench_syncnoop");
  for (let i = 0; i < SYNCFS_FILES; i++) {
    const s = tome4096.FS.open(`/bench_syncnoop/f${i}`, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    for (let p = 0; p < SYNCFS_PAGES_PER_FILE; p++) {
      tome4096.FS.write(s, PAGE_DATA, 0, PAGE_SIZE);
    }
    tome4096.FS.close(s);
  }
  // Initial sync to persist everything
  tome4096.rawFS.syncfs(false, (err: Error | null) => {
    if (err) throw err;
  });

  bench("tomefs (4096 pages)", () => {
    tome4096.rawFS.syncfs(false, (err: Error | null) => {
      if (err) throw err;
    });
  });
});

// ---------------------------------------------------------------------------
// Mixed Metadata Workload: simulates PGlite startup pattern
// (stat, readdir, open/close, truncate, rename)
// ---------------------------------------------------------------------------

const MIX_META_FILES = 15;
const MIX_META_ROUNDS = 10;

describe("Mixed Metadata Workload (PGlite-like startup pattern)", async () => {
  const memfs = await createMemFSHarness();
  const tome4096 = await createTomeFSHarness(4096);

  for (const h of [memfs, tome4096]) {
    h.FS.mkdir("/bench_mixmeta");
    h.FS.mkdir("/bench_mixmeta/base");
    h.FS.mkdir("/bench_mixmeta/pg_wal");
    for (let i = 0; i < MIX_META_FILES; i++) {
      const dir = i < 10 ? "/bench_mixmeta/base" : "/bench_mixmeta/pg_wal";
      const s = h.FS.open(`${dir}/f${i}`, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
      h.FS.write(s, PAGE_DATA, 0, PAGE_SIZE * 2); // 2 pages each
      h.FS.close(s);
    }
  }

  function mixedMeta(h: BenchHarness) {
    for (let r = 0; r < MIX_META_ROUNDS; r++) {
      // readdir (catalog scan)
      h.FS.readdir("/bench_mixmeta/base");
      h.FS.readdir("/bench_mixmeta/pg_wal");

      // stat each file (metadata lookup)
      for (let i = 0; i < MIX_META_FILES; i++) {
        const dir = i < 10 ? "/bench_mixmeta/base" : "/bench_mixmeta/pg_wal";
        h.FS.stat(`${dir}/f${i}`);
      }

      // open+close cycle (simulating Postgres probing files)
      for (let i = 0; i < 5; i++) {
        const s = h.FS.open(`/bench_mixmeta/base/f${i}`, O.RDONLY);
        h.FS.close(s);
      }

      // truncate one WAL file
      h.FS.truncate(`/bench_mixmeta/pg_wal/f${10 + (r % 5)}`, PAGE_SIZE);

      // chmod (simulating permission adjustments)
      h.FS.chmod(`/bench_mixmeta/base/f${r % 10}`, 0o600);
    }
  }

  bench("MEMFS", () => mixedMeta(memfs));
  bench("tomefs (4096 pages)", () => mixedMeta(tome4096));
});

// ---------------------------------------------------------------------------
// RestoreTree: measure mount time (remount from backend) with varying file counts
// ---------------------------------------------------------------------------

describe("RestoreTree Mount (100 files, 3-level dirs)", async () => {
  async function benchRemount(fileCount: number, dirDepth: number) {
    const { default: createModule } = await import(
      join(__dirname, "../harness/emscripten_fs.mjs")
    );

    // Phase 1: populate a backend with files via a temporary mount
    const setupBackend = new SyncMemoryBackend();
    const setupModule = await createModule();
    const setupFS = setupModule.FS as EmscriptenFS;
    const setupTomefs = createTomeFS(setupFS, { backend: setupBackend, maxPages: 4096 });
    setupFS.mkdir(TOME_MOUNT);
    setupFS.mount(setupTomefs, {}, TOME_MOUNT);

    // Create nested directory structure with files
    const dirs: string[] = ["/d"];
    for (let d = 1; d < dirDepth; d++) {
      dirs.push(dirs[d - 1] + `/sub${d}`);
    }
    for (const dir of dirs) {
      setupFS.mkdir(TOME_MOUNT + dir);
    }
    for (let i = 0; i < fileCount; i++) {
      const dir = dirs[i % dirs.length];
      const fd = setupFS.open(
        `${TOME_MOUNT}${dir}/f${i}`,
        O.WRONLY | O.CREAT | O.TRUNC,
        0o666,
      );
      setupFS.write(fd, PAGE_DATA, 0, Math.min(PAGE_SIZE, 512));
      setupFS.close(fd);
    }
    // Sync to persist everything to backend
    setupFS.syncfs(false, (err: Error | null) => { if (err) throw err; });

    // Phase 2: benchmark remount — each iteration creates a fresh Emscripten
    // module and mounts tomefs against the pre-populated backend
    return async () => {
      const m = await createModule();
      const fs = m.FS as EmscriptenFS;
      const tome = createTomeFS(fs, { backend: setupBackend, maxPages: 4096 });
      fs.mkdir(TOME_MOUNT);
      fs.mount(tome, {}, TOME_MOUNT);
      // Verify restore worked by stat-ing a file
      fs.stat(`${TOME_MOUNT}/d/f0`);
    };
  }

  const remount100 = await benchRemount(100, 3);
  bench("100 files, 3-level dirs", remount100);
});

describe("RestoreTree Mount (500 files, 5-level dirs)", async () => {
  async function benchRemount(fileCount: number, dirDepth: number) {
    const { default: createModule } = await import(
      join(__dirname, "../harness/emscripten_fs.mjs")
    );

    const setupBackend = new SyncMemoryBackend();
    const setupModule = await createModule();
    const setupFS = setupModule.FS as EmscriptenFS;
    const setupTomefs = createTomeFS(setupFS, { backend: setupBackend, maxPages: 4096 });
    setupFS.mkdir(TOME_MOUNT);
    setupFS.mount(setupTomefs, {}, TOME_MOUNT);

    const dirs: string[] = ["/d"];
    for (let d = 1; d < dirDepth; d++) {
      dirs.push(dirs[d - 1] + `/sub${d}`);
    }
    for (const dir of dirs) {
      setupFS.mkdir(TOME_MOUNT + dir);
    }
    for (let i = 0; i < fileCount; i++) {
      const dir = dirs[i % dirs.length];
      const fd = setupFS.open(
        `${TOME_MOUNT}${dir}/f${i}`,
        O.WRONLY | O.CREAT | O.TRUNC,
        0o666,
      );
      setupFS.write(fd, PAGE_DATA, 0, Math.min(PAGE_SIZE, 512));
      setupFS.close(fd);
    }
    setupFS.syncfs(false, (err: Error | null) => { if (err) throw err; });

    return async () => {
      const m = await createModule();
      const fs = m.FS as EmscriptenFS;
      const tome = createTomeFS(fs, { backend: setupBackend, maxPages: 4096 });
      fs.mkdir(TOME_MOUNT);
      fs.mount(tome, {}, TOME_MOUNT);
      fs.stat(`${TOME_MOUNT}/d/f0`);
    };
  }

  const remount500 = await benchRemount(500, 5);
  bench("500 files, 5-level dirs", remount500);
});
