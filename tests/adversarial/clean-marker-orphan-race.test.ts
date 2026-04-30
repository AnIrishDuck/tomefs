/**
 * Adversarial test for clean-shutdown marker timing relative to orphan cleanup.
 *
 * The full-tree-walk syncfs path must write the clean-shutdown marker AFTER
 * orphan cleanup (deleteAll) succeeds — not before. If the marker were
 * written atomically with data (before deleteAll) and the process crashed
 * between syncAll and deleteAll, the marker would persist alongside
 * /__deleted_* orphans. Although restoreTree's dual check currently catches
 * this (marker + /__deleted_* → force cleanup), writing the marker after
 * cleanup is correct by construction and doesn't rely on restoreTree
 * having exactly the right check for every orphan type.
 *
 * This test verifies:
 * 1. A crash during orphan cleanup does NOT leave a clean marker
 * 2. Retry after failed cleanup succeeds
 * 3. Remount after failed cleanup forces orphan cleanup (no stale marker)
 * 4. Normal cleanup flow writes the marker only after success
 *
 * Ethos §9: "Write tests designed to break tomefs specifically — target the
 * seams: metadata updates after flush, dirty flush ordering"
 */

import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { describe, it, expect } from "vitest";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { createTomeFS } from "../../src/tomefs.js";
import { PAGE_SIZE } from "../../src/types.js";
import type { SyncStorageBackend } from "../../src/sync-storage-backend.js";
import type { FileMeta } from "../../src/types.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

const O = {
  RDONLY: 0,
  WRONLY: 1,
  RDWR: 2,
  CREAT: 64,
  TRUNC: 512,
} as const;

const MOUNT = "/tome";
const CLEAN_MARKER = "/__tomefs_clean";

function encode(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

function decode(buf: Uint8Array, length?: number): string {
  return new TextDecoder().decode(
    length !== undefined ? buf.subarray(0, length) : buf,
  );
}

/**
 * SyncStorageBackend that can selectively fail deleteAll while allowing
 * all other operations to succeed. Used to simulate a crash during
 * orphan cleanup.
 */
class DeleteAllFailBackend implements SyncStorageBackend {
  readonly inner = new SyncMemoryBackend();
  deleteAllShouldFail = false;
  deleteAllCallCount = 0;

  readPage(p: string, i: number) { return this.inner.readPage(p, i); }
  readPages(p: string, is: number[]) { return this.inner.readPages(p, is); }
  readMeta(p: string) { return this.inner.readMeta(p); }
  readMetas(ps: string[]) { return this.inner.readMetas(ps); }
  countPages(p: string) { return this.inner.countPages(p); }
  countPagesBatch(ps: string[]) { return this.inner.countPagesBatch(ps); }
  maxPageIndex(p: string) { return this.inner.maxPageIndex(p); }
  maxPageIndexBatch(ps: string[]) { return this.inner.maxPageIndexBatch(ps); }
  listFiles() { return this.inner.listFiles(); }

  writePage(p: string, i: number, d: Uint8Array) { this.inner.writePage(p, i, d); }
  writePages(pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>) {
    this.inner.writePages(pages);
  }
  writeMeta(p: string, m: FileMeta) { this.inner.writeMeta(p, m); }
  writeMetas(entries: Array<{ path: string; meta: FileMeta }>) {
    this.inner.writeMetas(entries);
  }
  deleteFile(p: string) { this.inner.deleteFile(p); }
  deleteFiles(ps: string[]) { this.inner.deleteFiles(ps); }
  deletePagesFrom(p: string, i: number) { this.inner.deletePagesFrom(p, i); }
  deleteMeta(p: string) { this.inner.deleteMeta(p); }
  deleteMetas(ps: string[]) { this.inner.deleteMetas(ps); }
  renameFile(o: string, n: string) { this.inner.renameFile(o, n); }
  syncAll(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
    metas: Array<{ path: string; meta: FileMeta }>,
  ) {
    this.inner.syncAll(pages, metas);
  }
  deleteAll(paths: string[]) {
    this.deleteAllCallCount++;
    if (this.deleteAllShouldFail) {
      throw new Error("simulated crash during orphan cleanup");
    }
    this.inner.deleteAll(paths);
  }
}

async function mountTome(backend: SyncStorageBackend, maxPages?: number) {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  const Module = await createModule();
  const FS = Module.FS;
  const tomefs = createTomeFS(FS, { backend, maxPages });
  FS.mkdir(MOUNT);
  FS.mount(tomefs, {}, MOUNT);
  return { FS, tomefs, Module };
}

function syncfs(FS: any, tomefs: any): Error | null {
  let error: Error | null = null;
  tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
    error = err;
  });
  return error;
}

/**
 * Seed the backend with state that produces /__deleted_* orphans:
 * a real file /file with data, plus /__deleted_N entries that simulate
 * a prior session's crash between unlink (which created the marker) and
 * the next syncfs (which would have cleaned it up).
 */
function seedWithOrphans(backend: DeleteAllFailBackend) {
  const data = new Uint8Array(PAGE_SIZE);
  data.fill(0xAA);
  backend.inner.writePage("/file", 0, data);
  backend.inner.writeMeta("/file", {
    size: PAGE_SIZE, mode: 0o100644, ctime: 1000, mtime: 1000,
  });
  // Orphan: /__deleted_* marker from a previous session's unlink-with-open-fds
  // that crashed before syncfs cleanup ran.
  backend.inner.writePage("/__deleted_0", 0, new Uint8Array(PAGE_SIZE));
  backend.inner.writeMeta("/__deleted_0", {
    size: PAGE_SIZE, mode: 0o100644, ctime: 500, mtime: 500,
  });
}

describe("clean marker written after orphan cleanup", () => {
  it("crash during deleteAll does not leave a clean marker @fast", async () => {
    const backend = new DeleteAllFailBackend();
    seedWithOrphans(backend);

    const { FS, tomefs } = await mountTome(backend);
    backend.deleteAllShouldFail = true;
    const err = syncfs(FS, tomefs);

    expect(err).not.toBeNull();
    expect(err!.message).toContain("simulated crash");

    // The clean marker must NOT be in the backend (marker written after cleanup)
    expect(backend.inner.readMeta(CLEAN_MARKER)).toBeNull();

    // The orphan still exists (deleteAll failed)
    expect(backend.inner.listFiles()).toContain("/__deleted_0");

    // Real file data is persisted (syncAll succeeded before deleteAll)
    expect(backend.inner.readMeta("/file")).not.toBeNull();
  });

  it("retry after deleteAll failure succeeds and writes marker @fast", async () => {
    const backend = new DeleteAllFailBackend();
    seedWithOrphans(backend);

    const { FS, tomefs } = await mountTome(backend);

    // First attempt: crash during cleanup
    backend.deleteAllShouldFail = true;
    const err1 = syncfs(FS, tomefs);
    expect(err1).not.toBeNull();
    expect(backend.inner.readMeta(CLEAN_MARKER)).toBeNull();

    // Retry: cleanup succeeds
    backend.deleteAllShouldFail = false;
    const err2 = syncfs(FS, tomefs);
    expect(err2).toBeNull();

    // Clean marker written after successful cleanup
    expect(backend.inner.readMeta(CLEAN_MARKER)).not.toBeNull();

    // Orphan removed
    expect(backend.inner.listFiles()).not.toContain("/__deleted_0");

    // Real file survived
    expect(backend.inner.listFiles()).toContain("/file");
  });

  it("remount after failed orphan cleanup forces cleanup @fast", async () => {
    const backend = new DeleteAllFailBackend();
    seedWithOrphans(backend);

    // Session B: mount, syncfs crashes during deleteAll
    {
      const { FS, tomefs } = await mountTome(backend);
      backend.deleteAllShouldFail = true;
      const err = syncfs(FS, tomefs);
      expect(err).not.toBeNull();
      FS.unmount(MOUNT);
    }

    // No clean marker after crash
    expect(backend.inner.readMeta(CLEAN_MARKER)).toBeNull();

    // Session C: remount — no marker → needsOrphanCleanup=true → full walk
    backend.deleteAllShouldFail = false;
    backend.deleteAllCallCount = 0;
    {
      const { FS, tomefs } = await mountTome(backend);
      const err = syncfs(FS, tomefs);
      expect(err).toBeNull();

      // deleteAll was called to clean up the orphan
      expect(backend.deleteAllCallCount).toBeGreaterThan(0);

      // Orphan gone
      expect(backend.inner.listFiles()).not.toContain("/__deleted_0");

      // Clean marker present
      expect(backend.inner.readMeta(CLEAN_MARKER)).not.toBeNull();

      // File data intact through the whole sequence
      const readBuf = new Uint8Array(PAGE_SIZE);
      const fd = FS.open(`${MOUNT}/file`, O.RDONLY);
      FS.read(fd, readBuf, 0, PAGE_SIZE, 0);
      FS.close(fd);
      expect(readBuf[0]).toBe(0xAA);
      expect(readBuf[PAGE_SIZE - 1]).toBe(0xAA);
    }
  });

  it("successful cleanup writes marker and clears orphans in one pass @fast", async () => {
    const backend = new DeleteAllFailBackend();
    seedWithOrphans(backend);

    const { FS, tomefs } = await mountTome(backend);

    // First syncfs: should do full tree walk + orphan cleanup
    const err = syncfs(FS, tomefs);
    expect(err).toBeNull();

    // deleteAll was called exactly once (for the orphan batch)
    expect(backend.deleteAllCallCount).toBe(1);

    // Orphan removed
    expect(backend.inner.listFiles()).not.toContain("/__deleted_0");

    // Clean marker written
    expect(backend.inner.readMeta(CLEAN_MARKER)).not.toBeNull();

    // Subsequent syncfs takes incremental path (no deleteAll)
    backend.deleteAllCallCount = 0;
    const fd = FS.open(`${MOUNT}/file`, O.WRONLY);
    const writeBuf = new Uint8Array(10);
    writeBuf.fill(0xEE);
    FS.write(fd, writeBuf, 0, 10, 0);
    FS.close(fd);

    const err2 = syncfs(FS, tomefs);
    expect(err2).toBeNull();
    expect(backend.deleteAllCallCount).toBe(0);
  });

  it("no orphans: marker still written after empty check @fast", async () => {
    const backend = new DeleteAllFailBackend();

    // No orphans — just a clean file
    const data = new Uint8Array(PAGE_SIZE);
    data.fill(0xFF);
    backend.inner.writePage("/clean", 0, data);
    backend.inner.writeMeta("/clean", {
      size: PAGE_SIZE, mode: 0o100644, ctime: 1000, mtime: 1000,
    });

    const { FS, tomefs } = await mountTome(backend);
    const err = syncfs(FS, tomefs);
    expect(err).toBeNull();

    // No deleteAll needed
    expect(backend.deleteAllCallCount).toBe(0);

    // Marker still written (backend verified clean)
    expect(backend.inner.readMeta(CLEAN_MARKER)).not.toBeNull();
  });

  it("multiple orphans cleaned up atomically before marker @fast", async () => {
    const backend = new DeleteAllFailBackend();

    // Real file
    const data = new Uint8Array(PAGE_SIZE);
    data.fill(0xBB);
    backend.inner.writePage("/real", 0, data);
    backend.inner.writeMeta("/real", {
      size: PAGE_SIZE, mode: 0o100644, ctime: 1000, mtime: 1000,
    });

    // Multiple orphans from different prior operations
    for (let i = 0; i < 5; i++) {
      backend.inner.writePage(`/__deleted_${i}`, 0, new Uint8Array(PAGE_SIZE));
      backend.inner.writeMeta(`/__deleted_${i}`, {
        size: PAGE_SIZE, mode: 0o100644, ctime: 100 * i, mtime: 100 * i,
      });
    }

    const { FS, tomefs } = await mountTome(backend);
    const err = syncfs(FS, tomefs);
    expect(err).toBeNull();

    // Single deleteAll call for all orphans
    expect(backend.deleteAllCallCount).toBe(1);

    // All orphans removed
    const files = backend.inner.listFiles();
    for (let i = 0; i < 5; i++) {
      expect(files).not.toContain(`/__deleted_${i}`);
    }

    // Real file and marker present
    expect(files).toContain("/real");
    expect(backend.inner.readMeta(CLEAN_MARKER)).not.toBeNull();
  });
});
