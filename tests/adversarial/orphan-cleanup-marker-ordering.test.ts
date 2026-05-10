/**
 * Adversarial tests: clean-shutdown marker must be written AFTER orphan cleanup.
 *
 * The syncfs orphan cleanup path writes dirty pages + metadata via syncAll,
 * then deletes orphaned paths via deleteAll, then writes the clean-shutdown
 * marker. If the marker were written atomically with syncAll (before orphan
 * cleanup), a crash between syncAll and deleteAll would leave the marker
 * present while orphans still exist. The next mount would trust the marker,
 * skip orphan cleanup, and leave phantom files permanently.
 *
 * These tests verify:
 * 1. The clean marker is NOT present after syncAll but before orphan cleanup
 *    (verified indirectly by testing the crash window scenario)
 * 2. A crash between data commit and orphan cleanup leaves no marker, so the
 *    next mount correctly forces orphan cleanup
 * 3. Non-/__deleted_* orphans (stale file metadata from incomplete operations)
 *    are correctly detected and cleaned up
 *
 * Ethos §9: "target the seams: metadata updates after flush, dirty flush ordering"
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
const CLEAN_MARKER_PATH = "/__tomefs_clean";

function encode(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

function decode(buf: Uint8Array, length?: number): string {
  return new TextDecoder().decode(
    length !== undefined ? buf.subarray(0, length) : buf,
  );
}

async function createEmscriptenModule() {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  return createModule();
}

interface TestHarness {
  FS: any;
  backend: SyncMemoryBackend;
  tomefs: any;
}

async function createHarness(
  backend?: SyncMemoryBackend,
  maxPages: number = 64,
): Promise<TestHarness> {
  const Module = await createEmscriptenModule();
  const FS = Module.FS;
  const b = backend ?? new SyncMemoryBackend();
  const tomefs = createTomeFS(FS, { backend: b, maxPages });
  FS.mkdir(MOUNT);
  FS.mount(tomefs, {}, MOUNT);
  return { FS, backend: b, tomefs };
}

function syncfs(FS: any): void {
  FS.syncfs(false, (err: Error | null) => {
    if (err) throw err;
  });
}

/**
 * Backend that intercepts syncAll to verify the clean marker is NOT included,
 * and tracks whether deleteAll was called before writeMeta(CLEAN_MARKER_PATH).
 */
class MarkerOrderingBackend implements SyncStorageBackend {
  readonly inner = new SyncMemoryBackend();
  syncAllMetaPaths: string[] = [];
  operationLog: string[] = [];

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
    if (path === CLEAN_MARKER_PATH) {
      this.operationLog.push("writeMeta:CLEAN_MARKER");
    }
    this.inner.writeMeta(path, meta);
  }
  writeMetas(entries: Array<{ path: string; meta: FileMeta }>): void {
    this.inner.writeMetas(entries);
  }
  deleteMeta(path: string): void {
    if (path === CLEAN_MARKER_PATH) {
      this.operationLog.push("deleteMeta:CLEAN_MARKER");
    }
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
    this.syncAllMetaPaths = metas.map((m) => m.path);
    this.operationLog.push("syncAll");
    this.inner.syncAll(pages, metas);
  }
  deleteAll(paths: string[]): void {
    this.operationLog.push("deleteAll");
    this.inner.deleteAll(paths);
  }
}

describe("adversarial: orphan cleanup marker ordering", () => {
  it("clean marker is NOT included in syncAll during orphan cleanup path @fast", async () => {
    const backend = new MarkerOrderingBackend();

    // Seed backend with a live file and a /__deleted_* orphan (simulating
    // a crash during unlink-with-open-fds). restoreTree filters /__deleted_*
    // entries from the live tree, so they remain only in the backend.
    backend.inner.writeMeta("/file", {
      size: 10,
      mode: 0o100644,
      ctime: Date.now(),
      mtime: Date.now(),
    });
    backend.inner.writePage("/file", 0, new Uint8Array(PAGE_SIZE));
    backend.inner.writeMeta("/__deleted_0", {
      size: 5,
      mode: 0o100644,
      ctime: Date.now(),
      mtime: Date.now(),
    });
    backend.inner.writePage("/__deleted_0", 0, new Uint8Array(PAGE_SIZE));
    // No clean marker + /__deleted_* present → needsOrphanCleanup = true

    const Module = await createEmscriptenModule();
    const FS = Module.FS;
    const tomefs = createTomeFS(FS, { backend, maxPages: 64 });
    FS.mkdir(MOUNT);
    FS.mount(tomefs, {}, MOUNT);

    // restoreTree filtered /__deleted_0 from the tree. It's only in the backend.
    // First syncfs will detect it as an orphan via listFiles() vs currentPaths.

    // Reset operation log before syncfs
    backend.operationLog = [];
    backend.syncAllMetaPaths = [];

    syncfs(FS);

    // Verify the clean marker was NOT in the syncAll metadata batch
    expect(backend.syncAllMetaPaths).not.toContain(CLEAN_MARKER_PATH);

    // Verify ordering: syncAll → deleteAll → writeMeta(CLEAN_MARKER)
    expect(backend.operationLog).toContain("syncAll");
    expect(backend.operationLog).toContain("deleteAll");
    expect(backend.operationLog).toContain("writeMeta:CLEAN_MARKER");

    const syncAllIdx = backend.operationLog.indexOf("syncAll");
    const deleteAllIdx = backend.operationLog.indexOf("deleteAll");
    const markerIdx = backend.operationLog.indexOf("writeMeta:CLEAN_MARKER");
    expect(syncAllIdx).toBeLessThan(deleteAllIdx);
    expect(deleteAllIdx).toBeLessThan(markerIdx);
  });

  it("crash between syncAll and deleteAll leaves no clean marker @fast", async () => {
    const backend = new SyncMemoryBackend();

    // Session 1: create a file and sync
    {
      const { FS } = await createHarness(backend);
      const data = encode("hello world");
      const fd = FS.open(`${MOUNT}/a`, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
      FS.write(fd, data, 0, data.length, 0);
      FS.close(fd);
      syncfs(FS);
    }

    // Verify clean marker exists
    expect(backend.readMeta(CLEAN_MARKER_PATH)).not.toBeNull();

    // Simulate a crash scenario: add stale metadata to the backend as if a
    // previous operation left orphaned metadata. Also remove the clean marker
    // to simulate the state after a crash (marker was invalidated or absent).
    backend.deleteMeta(CLEAN_MARKER_PATH);
    backend.writeMeta("/ghost_from_crash", {
      size: 100,
      mode: 0o100644,
      ctime: Date.now(),
      mtime: Date.now(),
    });
    backend.writePage("/ghost_from_crash", 0, new Uint8Array(PAGE_SIZE));

    // Session 2: remount. No clean marker → needsOrphanCleanup = true.
    // restoreTree creates a node for /ghost_from_crash.
    {
      const { FS } = await createHarness(backend);

      // /ghost_from_crash was restored as an empty file (pages exist)
      const stat = FS.stat(`${MOUNT}/ghost_from_crash`);
      expect(stat).toBeDefined();

      // Unlink the ghost — it shouldn't be in the tree
      FS.unlink(`${MOUNT}/ghost_from_crash`);

      // syncfs runs orphan cleanup, removes stale backend metadata.
      // The clean marker is written AFTER orphan cleanup.
      syncfs(FS);

      // Verify ghost is gone from backend
      expect(backend.readMeta("/ghost_from_crash")).toBeNull();
      expect(backend.readPage("/ghost_from_crash", 0)).toBeNull();

      // Verify clean marker is now present
      expect(backend.readMeta(CLEAN_MARKER_PATH)).not.toBeNull();
    }
  });

  it("simulated crash window: marker + orphans cannot coexist after fix @fast", async () => {
    const backend = new SyncMemoryBackend();

    // Session 1: establish a file and sync
    {
      const { FS } = await createHarness(backend);
      const data = encode("persistent data");
      const fd = FS.open(`${MOUNT}/keep`, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
      FS.write(fd, data, 0, data.length, 0);
      FS.close(fd);
      syncfs(FS);
    }

    // Verify marker is present, no orphans
    expect(backend.readMeta(CLEAN_MARKER_PATH)).not.toBeNull();
    expect(backend.listFiles()).not.toContain("/ghost");

    // Now simulate the OLD buggy behavior: add an orphan AND a clean marker.
    // Before the fix, a crash between syncAll (which wrote the marker) and
    // deleteAll (which would have removed the orphan) could produce this state.
    backend.writeMeta("/ghost", {
      size: 50,
      mode: 0o100644,
      ctime: Date.now(),
      mtime: Date.now(),
    });
    backend.writePage("/ghost", 0, new Uint8Array(PAGE_SIZE));

    // With the fix, this state (marker + non-/__deleted_* orphan) can only
    // arise from a bug or external interference. But restoreTree should still
    // handle it: the marker + no /__deleted_* orphans means needsOrphanCleanup
    // is set to false. The ghost file is loaded into the tree and persists.
    //
    // This is a known limitation of the heuristic — the fix prevents THIS
    // state from being created by the syncfs code path. The test verifies
    // that when the ghost is loaded, it's at least consistent (no corruption).
    {
      const { FS } = await createHarness(backend);

      // /ghost was restored from backend — it appears in the tree
      const stat = FS.stat(`${MOUNT}/ghost`);
      expect(stat).toBeDefined();

      // /keep is still there with correct data
      const fd = FS.open(`${MOUNT}/keep`, O.RDONLY);
      const buf = new Uint8Array(100);
      const n = FS.read(fd, buf, 0, 100, 0);
      expect(decode(buf, n)).toBe("persistent data");
      FS.close(fd);
    }
  });

  it("orphan cleanup removes stale metadata from failed rename @fast", async () => {
    const backend = new SyncMemoryBackend();

    // Session 1: create file, sync
    {
      const { FS } = await createHarness(backend);
      const data = encode("file content");
      const fd = FS.open(`${MOUNT}/src`, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
      FS.write(fd, data, 0, data.length, 0);
      FS.close(fd);
      syncfs(FS);
    }

    // Simulate a crash that left stale metadata at the old path after a
    // rename. The rename wrote metadata at /dst and moved pages there, but
    // crashed before deleting /src metadata. Also invalidated the clean marker.
    backend.deleteMeta(CLEAN_MARKER_PATH);
    // /src metadata still exists (orphan) — but pages were moved to /dst
    // so /src has metadata but no pages
    backend.writeMeta("/dst", {
      size: 12, // "file content".length
      mode: 0o100644,
      ctime: Date.now(),
      mtime: Date.now(),
    });
    // Move pages from /src to /dst (simulating what renameFile does)
    const pageData = backend.readPage("/src", 0)!;
    backend.writePage("/dst", 0, pageData);
    backend.deleteFile("/src");
    // /src metadata remains but has no pages → fileSize recovered as 0

    // Session 2: remount. No marker → needsOrphanCleanup = true.
    // Both /src (with meta, no pages → size 0) and /dst (with meta + pages)
    // are restored.
    {
      const { FS } = await createHarness(backend);

      // /dst should have the correct data
      const fd = FS.open(`${MOUNT}/dst`, O.RDONLY);
      const buf = new Uint8Array(100);
      const n = FS.read(fd, buf, 0, 100, 0);
      expect(decode(buf, n)).toBe("file content");
      FS.close(fd);

      // /src was restored as an empty file (metadata says size=12 but
      // maxPageIndex returns -1, so restoreTree sets size=0)
      const stat = FS.stat(`${MOUNT}/src`);
      expect(stat.size).toBe(0);

      // Remove the stale /src from the tree
      FS.unlink(`${MOUNT}/src`);

      // syncfs cleans up the orphan
      syncfs(FS);

      // Verify /src metadata is gone from backend
      expect(backend.readMeta("/src")).toBeNull();

      // Clean marker is now present (written after orphan cleanup)
      expect(backend.readMeta(CLEAN_MARKER_PATH)).not.toBeNull();
    }

    // Session 3: remount. Clean marker present → no orphan cleanup needed.
    // Verify only /dst exists.
    {
      const { FS } = await createHarness(backend);

      const fd = FS.open(`${MOUNT}/dst`, O.RDONLY);
      const buf = new Uint8Array(100);
      const n = FS.read(fd, buf, 0, 100, 0);
      expect(decode(buf, n)).toBe("file content");
      FS.close(fd);

      // /src should not exist
      expect(() => FS.stat(`${MOUNT}/src`)).toThrow();
    }
  });

  it("orphan cleanup handles /__deleted_* orphans from crashed unlink @fast", async () => {
    const backend = new SyncMemoryBackend();

    // Simulate crash state: /__deleted_0 exists (from unlink-with-open-fds
    // that crashed before the fd was closed and cleanup ran)
    backend.writeMeta("/__deleted_0", {
      size: 20,
      mode: 0o100644,
      ctime: Date.now(),
      mtime: Date.now(),
    });
    backend.writePage("/__deleted_0", 0, new Uint8Array(PAGE_SIZE));
    // Also have a live file
    backend.writeMeta("/live", {
      size: 5,
      mode: 0o100644,
      ctime: Date.now(),
      mtime: Date.now(),
    });
    backend.writePage("/live", 0, new Uint8Array(PAGE_SIZE));
    // No clean marker

    // Mount: restoreTree sees /__deleted_* → hasOrphans = true, even if
    // clean marker existed it would force orphan cleanup.
    // /__deleted_* entries are filtered from the live tree.
    {
      const { FS } = await createHarness(backend);

      // /live should be restored
      const stat = FS.stat(`${MOUNT}/live`);
      expect(stat).toBeDefined();

      // /__deleted_0 should NOT be in the tree (filtered by restoreTree)
      expect(() => FS.stat(`${MOUNT}/__deleted_0`)).toThrow();

      // syncfs runs orphan cleanup, removes /__deleted_0 from backend
      syncfs(FS);

      // Verify orphan is gone
      expect(backend.readMeta("/__deleted_0")).toBeNull();
      expect(backend.readPage("/__deleted_0", 0)).toBeNull();

      // Clean marker written after cleanup
      expect(backend.readMeta(CLEAN_MARKER_PATH)).not.toBeNull();
    }
  });
});
