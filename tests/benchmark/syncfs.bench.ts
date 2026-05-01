/**
 * syncfs performance benchmarks.
 *
 * syncfs is called after every PGlite query via syncToFs(). It's the single
 * most frequently called operation in production. Three code paths exist:
 *
 * 1. Fast path: nothing dirty — O(1) check + return
 * 2. Incremental path: flush dirty pages + persist dirty metadata — O(dirty)
 * 3. Full tree walk: rebuild currentPaths + orphan cleanup — O(tree)
 *
 * These benchmarks measure the cost of each path at realistic tree sizes
 * to validate ethos §6 (performance parity) and quantify the incremental
 * sync optimization that avoids full tree walks on every call.
 *
 * Run: npx vitest bench tests/benchmark/syncfs.bench.ts
 */

import { bench, describe } from "vitest";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { createTomeFS } from "../../src/tomefs.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { PAGE_SIZE } from "../../src/types.js";
import { O } from "../harness/emscripten-fs.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

// ---------------------------------------------------------------------------
// Harness setup
// ---------------------------------------------------------------------------

const TOME_MOUNT = "/tome";

interface SyncBenchHarness {
  rawFS: any;
  tomefs: any;
  backend: SyncMemoryBackend;
}

async function createHarness(
  maxPages: number,
): Promise<SyncBenchHarness> {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  const Module = await createModule();
  const rawFS = Module.FS;

  const backend = new SyncMemoryBackend();
  const tomefs = createTomeFS(rawFS, { backend, maxPages });

  rawFS.mkdir(TOME_MOUNT);
  rawFS.mount(tomefs, {}, TOME_MOUNT);

  return { rawFS, tomefs, backend };
}

function doSyncfs(rawFS: any): void {
  rawFS.syncfs(false, (err: Error | null) => {
    if (err) throw err;
  });
}

/** Create a directory tree with N files, each containing 1 page of data. */
function populateFiles(rawFS: any, count: number): void {
  const data = new Uint8Array(PAGE_SIZE);
  for (let i = 0; i < PAGE_SIZE; i++) data[i] = (i * 31 + 17) & 0xff;

  // Create files spread across subdirectories (10 per dir) to simulate
  // a realistic Postgres directory structure (base/oid/relfilenode).
  for (let i = 0; i < count; i++) {
    const dir = `${TOME_MOUNT}/dir${Math.floor(i / 10)}`;
    try {
      rawFS.mkdir(dir);
    } catch (_e) {
      // already exists
    }
    const path = `${dir}/file${i}`;
    const stream = rawFS.open(path, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    rawFS.write(stream, data, 0, PAGE_SIZE);
    rawFS.close(stream);
  }
}

/** Write 1 page to N randomly selected existing files to dirty them. */
function dirtyNFiles(rawFS: any, totalFiles: number, dirtyCount: number): void {
  const data = new Uint8Array(PAGE_SIZE).fill(0xab);
  const step = Math.max(1, Math.floor(totalFiles / dirtyCount));
  for (let d = 0; d < dirtyCount; d++) {
    const i = (d * step) % totalFiles;
    const dir = `${TOME_MOUNT}/dir${Math.floor(i / 10)}`;
    const path = `${dir}/file${i}`;
    const stream = rawFS.open(path, O.WRONLY, 0o666);
    rawFS.write(stream, data, 0, PAGE_SIZE);
    rawFS.close(stream);
  }
}

/** Touch metadata (chmod) on N files to dirty metadata without dirtying pages. */
function dirtyMetadataNFiles(
  rawFS: any,
  totalFiles: number,
  dirtyCount: number,
): void {
  const step = Math.max(1, Math.floor(totalFiles / dirtyCount));
  for (let d = 0; d < dirtyCount; d++) {
    const i = (d * step) % totalFiles;
    const dir = `${TOME_MOUNT}/dir${Math.floor(i / 10)}`;
    const path = `${dir}/file${i}`;
    rawFS.chmod(path, 0o644);
  }
}

// ---------------------------------------------------------------------------
// 1. Fast path: nothing dirty
//    The common case after read-only PGlite queries. Should be near-zero cost
//    regardless of tree size.
// ---------------------------------------------------------------------------

describe("syncfs fast path (nothing dirty)", async () => {
  const h10 = await createHarness(4096);
  populateFiles(h10.rawFS, 10);
  doSyncfs(h10.rawFS);

  const h100 = await createHarness(4096);
  populateFiles(h100.rawFS, 100);
  doSyncfs(h100.rawFS);

  const h500 = await createHarness(4096);
  populateFiles(h500.rawFS, 500);
  doSyncfs(h500.rawFS);

  bench("10 files", () => doSyncfs(h10.rawFS));
  bench("100 files", () => doSyncfs(h100.rawFS));
  bench("500 files", () => doSyncfs(h500.rawFS));
});

// ---------------------------------------------------------------------------
// 2. Incremental path: dirty pages + metadata
//    The common case after write PGlite queries. Cost should scale with dirty
//    count, not tree size.
// ---------------------------------------------------------------------------

describe("syncfs incremental (pages + metadata dirty)", async () => {
  // 100-file tree, vary dirty count
  const h1 = await createHarness(4096);
  populateFiles(h1.rawFS, 100);
  doSyncfs(h1.rawFS);

  const h5 = await createHarness(4096);
  populateFiles(h5.rawFS, 100);
  doSyncfs(h5.rawFS);

  const h20 = await createHarness(4096);
  populateFiles(h20.rawFS, 100);
  doSyncfs(h20.rawFS);

  const h50 = await createHarness(4096);
  populateFiles(h50.rawFS, 100);
  doSyncfs(h50.rawFS);

  bench("100 files, 1 dirty", () => {
    dirtyNFiles(h1.rawFS, 100, 1);
    doSyncfs(h1.rawFS);
  });

  bench("100 files, 5 dirty", () => {
    dirtyNFiles(h5.rawFS, 100, 5);
    doSyncfs(h5.rawFS);
  });

  bench("100 files, 20 dirty", () => {
    dirtyNFiles(h20.rawFS, 100, 20);
    doSyncfs(h20.rawFS);
  });

  bench("100 files, 50 dirty", () => {
    dirtyNFiles(h50.rawFS, 100, 50);
    doSyncfs(h50.rawFS);
  });
});

// ---------------------------------------------------------------------------
// 3. Incremental path: metadata-only dirty
//    chmod/utime operations dirty metadata but not pages. Measures the metadata
//    collection and batch write overhead without page flush cost.
// ---------------------------------------------------------------------------

describe("syncfs incremental (metadata-only dirty)", async () => {
  const h1 = await createHarness(4096);
  populateFiles(h1.rawFS, 100);
  doSyncfs(h1.rawFS);

  const h10 = await createHarness(4096);
  populateFiles(h10.rawFS, 100);
  doSyncfs(h10.rawFS);

  const h50 = await createHarness(4096);
  populateFiles(h50.rawFS, 100);
  doSyncfs(h50.rawFS);

  bench("100 files, 1 dirty metadata", () => {
    dirtyMetadataNFiles(h1.rawFS, 100, 1);
    doSyncfs(h1.rawFS);
  });

  bench("100 files, 10 dirty metadata", () => {
    dirtyMetadataNFiles(h10.rawFS, 100, 10);
    doSyncfs(h10.rawFS);
  });

  bench("100 files, 50 dirty metadata", () => {
    dirtyMetadataNFiles(h50.rawFS, 100, 50);
    doSyncfs(h50.rawFS);
  });
});

// ---------------------------------------------------------------------------
// 4. Incremental scaling: same dirty count, different tree sizes
//    Validates that incremental path cost is O(dirty), not O(tree).
// ---------------------------------------------------------------------------

describe("syncfs incremental scaling (5 dirty, varying tree size)", async () => {
  const h10 = await createHarness(4096);
  populateFiles(h10.rawFS, 10);
  doSyncfs(h10.rawFS);

  const h100 = await createHarness(4096);
  populateFiles(h100.rawFS, 100);
  doSyncfs(h100.rawFS);

  const h500 = await createHarness(4096);
  populateFiles(h500.rawFS, 500);
  doSyncfs(h500.rawFS);

  bench("10 files, 5 dirty", () => {
    dirtyNFiles(h10.rawFS, 10, 5);
    doSyncfs(h10.rawFS);
  });

  bench("100 files, 5 dirty", () => {
    dirtyNFiles(h100.rawFS, 100, 5);
    doSyncfs(h100.rawFS);
  });

  bench("500 files, 5 dirty", () => {
    dirtyNFiles(h500.rawFS, 500, 5);
    doSyncfs(h500.rawFS);
  });
});

// ---------------------------------------------------------------------------
// 5. Mount + restore: tree reconstruction from backend
//    Measures the startup cost for PGlite — mounting tomefs with an existing
//    backend triggers restoreTree(), which reads all metadata and rebuilds
//    the directory tree. This is O(files) and includes batch readMetas +
//    maxPageIndexBatch calls. Each iteration creates a fresh tomefs instance,
//    mounts it (triggering restore), then unmounts.
// ---------------------------------------------------------------------------

describe("mount + restore from backend", async () => {
  async function prepareBackend(fileCount: number): Promise<{
    backend: SyncMemoryBackend;
    rawFS: any;
  }> {
    const h = await createHarness(4096);
    populateFiles(h.rawFS, fileCount);
    doSyncfs(h.rawFS);
    h.rawFS.unmount(TOME_MOUNT);
    return { backend: h.backend, rawFS: h.rawFS };
  }

  const b10 = await prepareBackend(10);
  const b100 = await prepareBackend(100);
  const b500 = await prepareBackend(500);

  bench("10 files", () => {
    const tomefs = createTomeFS(b10.rawFS, {
      backend: b10.backend,
      maxPages: 4096,
    });
    b10.rawFS.mount(tomefs, {}, TOME_MOUNT);
    b10.rawFS.unmount(TOME_MOUNT);
  });

  bench("100 files", () => {
    const tomefs = createTomeFS(b100.rawFS, {
      backend: b100.backend,
      maxPages: 4096,
    });
    b100.rawFS.mount(tomefs, {}, TOME_MOUNT);
    b100.rawFS.unmount(TOME_MOUNT);
  });

  bench("500 files", () => {
    const tomefs = createTomeFS(b500.rawFS, {
      backend: b500.backend,
      maxPages: 4096,
    });
    b500.rawFS.mount(tomefs, {}, TOME_MOUNT);
    b500.rawFS.unmount(TOME_MOUNT);
  });
});

// ---------------------------------------------------------------------------
// 6. Steady-state PGlite pattern: write 2 files (WAL + heap), then sync
//    Simulates the most common PGlite workload: an INSERT/UPDATE query that
//    modifies the WAL file and one heap file, followed by syncToFs().
// ---------------------------------------------------------------------------

describe("syncfs steady-state (2 dirty = WAL + heap pattern)", async () => {
  const h50 = await createHarness(4096);
  populateFiles(h50.rawFS, 50);
  doSyncfs(h50.rawFS);

  const h200 = await createHarness(4096);
  populateFiles(h200.rawFS, 200);
  doSyncfs(h200.rawFS);

  const h500 = await createHarness(4096);
  populateFiles(h500.rawFS, 500);
  doSyncfs(h500.rawFS);

  bench("50 files, 2 dirty", () => {
    dirtyNFiles(h50.rawFS, 50, 2);
    doSyncfs(h50.rawFS);
  });

  bench("200 files, 2 dirty", () => {
    dirtyNFiles(h200.rawFS, 200, 2);
    doSyncfs(h200.rawFS);
  });

  bench("500 files, 2 dirty", () => {
    dirtyNFiles(h500.rawFS, 500, 2);
    doSyncfs(h500.rawFS);
  });
});
