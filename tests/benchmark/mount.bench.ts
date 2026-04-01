/**
 * Mount / restoreTree performance benchmarks.
 *
 * Measures the cost of mounting a tomefs filesystem from a pre-populated
 * backend — the most user-visible startup cost. This exercises restoreTree
 * (metadata batch reads, countPages batch, directory creation) and validates
 * the optimizations in batch SAB operations and path lookup maps.
 *
 * Ethos §6: startup should not regress as file counts grow.
 *
 * Run: npx vitest bench tests/benchmark/mount.bench.ts
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

const TOME_MOUNT = "/tome";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function loadEmscriptenModule(): Promise<EmscriptenFS> {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  const Module = await createModule();
  return Module.FS as EmscriptenFS;
}

/**
 * Pre-populate a SyncMemoryBackend with a Postgres-like directory structure.
 *
 * Creates:
 *   /base/         — fileCount data files (1-4 pages each)
 *   /pg_wal/       — 3 WAL segment files (8 pages each)
 *   /global/       — 5 shared catalog files (1 page each)
 *   /pg_tblspc/    — empty directory
 */
function populateBackend(
  backend: SyncMemoryBackend,
  fileCount: number,
): void {
  const now = Date.now();
  const PAGE_DATA = new Uint8Array(PAGE_SIZE);
  for (let i = 0; i < PAGE_SIZE; i++) PAGE_DATA[i] = (i * 31 + 17) & 0xff;

  // Directory metadata
  const dirMode = 0o40755;
  const dirs = ["/base", "/pg_wal", "/global", "/pg_tblspc"];
  for (const dir of dirs) {
    backend.writeMeta(dir, { size: 0, mode: dirMode, ctime: now, mtime: now });
  }

  // Data files in /base — varying sizes to simulate real tables/indexes
  const fileMode = 0o100644;
  for (let i = 0; i < fileCount; i++) {
    const path = `/base/${i}`;
    const pages = (i % 4) + 1; // 1-4 pages per file
    const size = pages * PAGE_SIZE;
    backend.writeMeta(path, { size, mode: fileMode, ctime: now, mtime: now });
    for (let p = 0; p < pages; p++) {
      backend.writePage(path, p, PAGE_DATA);
    }
  }

  // WAL segments — larger files
  for (let i = 0; i < 3; i++) {
    const path = `/pg_wal/00000001000000000000000${i}`;
    const pages = 8;
    const size = pages * PAGE_SIZE;
    backend.writeMeta(path, { size, mode: fileMode, ctime: now, mtime: now });
    for (let p = 0; p < pages; p++) {
      backend.writePage(path, p, PAGE_DATA);
    }
  }

  // Global catalog files
  for (let i = 0; i < 5; i++) {
    const path = `/global/${1200 + i}`;
    backend.writeMeta(path, {
      size: PAGE_SIZE,
      mode: fileMode,
      ctime: now,
      mtime: now,
    });
    backend.writePage(path, 0, PAGE_DATA);
  }
}

/**
 * Mount tomefs on a fresh Emscripten FS instance with a pre-populated backend.
 * Returns the wrapped FS for verification.
 */
function mountTome(
  rawFS: EmscriptenFS,
  backend: SyncMemoryBackend,
  maxPages: number,
): void {
  const tomefs = createTomeFS(rawFS, { backend, maxPages });
  rawFS.mkdir(TOME_MOUNT);
  rawFS.mount(tomefs, {}, TOME_MOUNT);
}

// ---------------------------------------------------------------------------
// Mount with varying file counts
// ---------------------------------------------------------------------------

describe("Mount: 20 files (small database)", async () => {
  const FILE_COUNT = 20;

  bench("restoreTree", async () => {
    const rawFS = await loadEmscriptenModule();
    const backend = new SyncMemoryBackend();
    populateBackend(backend, FILE_COUNT);
    mountTome(rawFS, backend, 4096);
  });
});

describe("Mount: 100 files (medium database)", async () => {
  const FILE_COUNT = 100;

  bench("restoreTree", async () => {
    const rawFS = await loadEmscriptenModule();
    const backend = new SyncMemoryBackend();
    populateBackend(backend, FILE_COUNT);
    mountTome(rawFS, backend, 4096);
  });
});

describe("Mount: 500 files (large database)", async () => {
  const FILE_COUNT = 500;

  bench("restoreTree", async () => {
    const rawFS = await loadEmscriptenModule();
    const backend = new SyncMemoryBackend();
    populateBackend(backend, FILE_COUNT);
    mountTome(rawFS, backend, 4096);
  });
});

describe("Mount: 1000 files (very large database)", async () => {
  const FILE_COUNT = 1000;

  bench("restoreTree", async () => {
    const rawFS = await loadEmscriptenModule();
    const backend = new SyncMemoryBackend();
    populateBackend(backend, FILE_COUNT);
    mountTome(rawFS, backend, 4096);
  });
});

// ---------------------------------------------------------------------------
// Mount + first read (cold start latency)
// ---------------------------------------------------------------------------

describe("Mount + first read: 100 files", async () => {
  const FILE_COUNT = 100;
  const READ_BUF = new Uint8Array(PAGE_SIZE);

  bench("mount then read 10 files", async () => {
    const rawFS = await loadEmscriptenModule();
    const backend = new SyncMemoryBackend();
    populateBackend(backend, FILE_COUNT);
    mountTome(rawFS, backend, 4096);

    // Read first page of 10 files — simulates Postgres probing catalogs at startup
    for (let i = 0; i < 10; i++) {
      const stream = rawFS.open(`${TOME_MOUNT}/base/${i}`, O.RDONLY);
      rawFS.read(stream, READ_BUF, 0, PAGE_SIZE);
      rawFS.close(stream);
    }
  });
});

// ---------------------------------------------------------------------------
// Syncfs round-trip: persist then remount
// ---------------------------------------------------------------------------

describe("Syncfs + remount: 100 files", async () => {
  const FILE_COUNT = 100;

  bench("persist then restore", async () => {
    // Phase 1: create files, write data, syncfs
    const rawFS1 = await loadEmscriptenModule();
    const backend = new SyncMemoryBackend();
    const tomefs1 = createTomeFS(rawFS1, { backend, maxPages: 4096 });
    rawFS1.mkdir(TOME_MOUNT);
    rawFS1.mount(tomefs1, {}, TOME_MOUNT);

    const PAGE_DATA = new Uint8Array(PAGE_SIZE);
    for (let i = 0; i < PAGE_SIZE; i++) PAGE_DATA[i] = (i * 31 + 17) & 0xff;

    // Create directory structure
    rawFS1.mkdir(`${TOME_MOUNT}/base`);
    rawFS1.mkdir(`${TOME_MOUNT}/pg_wal`);

    // Create files
    for (let i = 0; i < FILE_COUNT; i++) {
      const s = rawFS1.open(
        `${TOME_MOUNT}/base/${i}`,
        O.WRONLY | O.CREAT | O.TRUNC,
        0o666,
      );
      rawFS1.write(s, PAGE_DATA, 0, PAGE_SIZE);
      rawFS1.close(s);
    }

    // Persist
    rawFS1.syncfs(false, (err: Error | null) => {
      if (err) throw err;
    });

    // Phase 2: remount from the same backend (simulates restart)
    const rawFS2 = await loadEmscriptenModule();
    mountTome(rawFS2, backend, 4096);
  });
});

// ---------------------------------------------------------------------------
// Mount with deep directory nesting
// ---------------------------------------------------------------------------

describe("Mount: 100 files across 20 nested directories", async () => {
  bench("restoreTree with nested dirs", async () => {
    const rawFS = await loadEmscriptenModule();
    const backend = new SyncMemoryBackend();
    const now = Date.now();
    const dirMode = 0o40755;
    const fileMode = 0o100644;
    const PAGE_DATA = new Uint8Array(PAGE_SIZE);

    // Create 20 nested directories: /d0, /d0/d1, /d0/d1/d2, ...
    // then /e0, /e0/e1, etc. to avoid overly deep single chain
    const dirs: string[] = [];
    for (let g = 0; g < 4; g++) {
      let path = "";
      for (let d = 0; d < 5; d++) {
        path += `/g${g}_d${d}`;
        dirs.push(path);
        backend.writeMeta(path, {
          size: 0,
          mode: dirMode,
          ctime: now,
          mtime: now,
        });
      }
    }

    // Distribute 100 files across leaf directories
    for (let i = 0; i < 100; i++) {
      const dir = dirs[dirs.length - 1 - (i % 4) * 5]; // pick leaf dirs
      const path = `${dir}/file_${i}`;
      backend.writeMeta(path, {
        size: PAGE_SIZE,
        mode: fileMode,
        ctime: now,
        mtime: now,
      });
      backend.writePage(path, 0, PAGE_DATA);
    }

    mountTome(rawFS, backend, 4096);
  });
});
