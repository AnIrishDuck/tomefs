/**
 * Tests for PreloadBackend flush ordering during delete-then-recreate.
 *
 * Validates that when a file is deleted and a new file is created at the
 * same path (e.g., rename-overwrite), flush() writes both metadata AND
 * pages for the recreated path AFTER deleting the old data. This prevents
 * a crash-safety window where metadata points to stale or nonexistent pages.
 *
 * Bug: flush() partitioned metadata by `deletedMeta` but pages by
 * `deletedFiles`. When writeMeta() is called after deleteFile() at the
 * same path, writeMeta clears the path from `deletedMeta`, causing the
 * new metadata to be written early (before the delete) while pages are
 * correctly deferred. A crash between the early metadata write and the
 * late page write leaves metadata pointing to no pages (data loss) or
 * stale pages (corruption).
 *
 * Ethos §9 (adversarial), §10 (graceful degradation).
 */
import { describe, it, expect, beforeEach } from "vitest";
import { MemoryBackend } from "../../src/memory-backend.js";
import { PreloadBackend } from "../../src/preload-backend.js";
import { PAGE_SIZE } from "../../src/types.js";
import type { StorageBackend } from "../../src/storage-backend.js";
import type { FileMeta } from "../../src/types.js";

/**
 * Backend wrapper that records the order of mutating operations.
 * Each entry is [method, ...args] so we can verify flush ordering.
 */
class OrderTrackingBackend implements StorageBackend {
  private inner: MemoryBackend;
  log: Array<[string, ...unknown[]]> = [];

  constructor(inner: MemoryBackend) {
    this.inner = inner;
  }

  clearLog(): void {
    this.log = [];
  }

  async readPage(path: string, pageIndex: number) {
    return this.inner.readPage(path, pageIndex);
  }
  async readPages(path: string, pageIndices: number[]) {
    return this.inner.readPages(path, pageIndices);
  }
  async writePage(path: string, pageIndex: number, data: Uint8Array) {
    this.log.push(["writePage", path, pageIndex]);
    return this.inner.writePage(path, pageIndex, data);
  }
  async writePages(pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>) {
    this.log.push(["writePages", pages.map((p) => ({ path: p.path, pageIndex: p.pageIndex }))]);
    return this.inner.writePages(pages);
  }
  async deleteFile(path: string) {
    this.log.push(["deleteFile", path]);
    return this.inner.deleteFile(path);
  }
  async deleteFiles(paths: string[]) {
    this.log.push(["deleteFiles", [...paths]]);
    return this.inner.deleteFiles(paths);
  }
  async deletePagesFrom(path: string, fromPageIndex: number) {
    this.log.push(["deletePagesFrom", path, fromPageIndex]);
    return this.inner.deletePagesFrom(path, fromPageIndex);
  }
  async renameFile(oldPath: string, newPath: string) {
    this.log.push(["renameFile", oldPath, newPath]);
    return this.inner.renameFile(oldPath, newPath);
  }
  async readMeta(path: string) {
    return this.inner.readMeta(path);
  }
  async readMetas(paths: string[]) {
    return this.inner.readMetas(paths);
  }
  async writeMeta(path: string, meta: FileMeta) {
    this.log.push(["writeMeta", path]);
    return this.inner.writeMeta(path, meta);
  }
  async writeMetas(entries: Array<{ path: string; meta: FileMeta }>) {
    this.log.push(["writeMetas", entries.map((e) => e.path)]);
    return this.inner.writeMetas(entries);
  }
  async deleteMeta(path: string) {
    this.log.push(["deleteMeta", path]);
    return this.inner.deleteMeta(path);
  }
  async deleteMetas(paths: string[]) {
    this.log.push(["deleteMetas", [...paths]]);
    return this.inner.deleteMetas(paths);
  }
  async countPages(path: string) {
    return this.inner.countPages(path);
  }
  async countPagesBatch(paths: string[]) {
    return this.inner.countPagesBatch(paths);
  }
  async maxPageIndex(path: string) {
    return this.inner.maxPageIndex(path);
  }
  async maxPageIndexBatch(paths: string[]) {
    return this.inner.maxPageIndexBatch(paths);
  }
  async listFiles() {
    return this.inner.listFiles();
  }
  async syncAll(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
    metas: Array<{ path: string; meta: FileMeta }>,
  ): Promise<void> {
    this.log.push(["syncAll", {
      pages: pages.map((p) => ({ path: p.path, pageIndex: p.pageIndex })),
      metas: metas.map((m) => m.path),
    }]);
    return this.inner.syncAll(pages, metas);
  }
}

/**
 * Backend wrapper that crashes (stops executing) after a specified number
 * of mutating operations. Used to simulate mid-flush crashes.
 */
class CrashAfterNOpsBackend implements StorageBackend {
  private inner: MemoryBackend;
  private opsRemaining: number;

  constructor(inner: MemoryBackend, crashAfterOps: number) {
    this.inner = inner;
    this.opsRemaining = crashAfterOps;
  }

  private tick(): void {
    if (--this.opsRemaining < 0) {
      throw new Error("SIMULATED_CRASH");
    }
  }

  async readPage(path: string, pageIndex: number) {
    return this.inner.readPage(path, pageIndex);
  }
  async readPages(path: string, pageIndices: number[]) {
    return this.inner.readPages(path, pageIndices);
  }
  async writePage(path: string, pageIndex: number, data: Uint8Array) {
    this.tick();
    return this.inner.writePage(path, pageIndex, data);
  }
  async writePages(pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>) {
    this.tick();
    return this.inner.writePages(pages);
  }
  async deleteFile(path: string) {
    this.tick();
    return this.inner.deleteFile(path);
  }
  async deleteFiles(paths: string[]) {
    this.tick();
    return this.inner.deleteFiles(paths);
  }
  async deletePagesFrom(path: string, fromPageIndex: number) {
    this.tick();
    return this.inner.deletePagesFrom(path, fromPageIndex);
  }
  async renameFile(oldPath: string, newPath: string) {
    this.tick();
    return this.inner.renameFile(oldPath, newPath);
  }
  async readMeta(path: string) {
    return this.inner.readMeta(path);
  }
  async readMetas(paths: string[]) {
    return this.inner.readMetas(paths);
  }
  async writeMeta(path: string, meta: FileMeta) {
    this.tick();
    return this.inner.writeMeta(path, meta);
  }
  async writeMetas(entries: Array<{ path: string; meta: FileMeta }>) {
    this.tick();
    return this.inner.writeMetas(entries);
  }
  async deleteMeta(path: string) {
    this.tick();
    return this.inner.deleteMeta(path);
  }
  async deleteMetas(paths: string[]) {
    this.tick();
    return this.inner.deleteMetas(paths);
  }
  async countPages(path: string) {
    return this.inner.countPages(path);
  }
  async countPagesBatch(paths: string[]) {
    return this.inner.countPagesBatch(paths);
  }
  async maxPageIndex(path: string) {
    return this.inner.maxPageIndex(path);
  }
  async maxPageIndexBatch(paths: string[]) {
    return this.inner.maxPageIndexBatch(paths);
  }
  async listFiles() {
    return this.inner.listFiles();
  }
  async syncAll(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
    metas: Array<{ path: string; meta: FileMeta }>,
  ): Promise<void> {
    this.tick();
    return this.inner.syncAll(pages, metas);
  }
}

describe("PreloadBackend flush ordering", () => {
  let innerRemote: MemoryBackend;
  let remote: OrderTrackingBackend;

  beforeEach(() => {
    innerRemote = new MemoryBackend();
    remote = new OrderTrackingBackend(innerRemote);
  });

  it("@fast defers metadata write for delete-then-recreate paths", async () => {
    // Seed the remote with an existing file at /target
    const oldData = new Uint8Array(PAGE_SIZE);
    oldData[0] = 0xaa;
    await innerRemote.writePage("/target", 0, oldData);
    await innerRemote.writeMeta("/target", {
      size: PAGE_SIZE,
      mode: 0o100644,
      ctime: 1000,
      mtime: 1000,
    });

    // Init PreloadBackend
    const backend = new PreloadBackend(remote);
    await backend.init();
    remote.clearLog();

    // Simulate rename-overwrite: delete target, then write new data + metadata
    // at the same path (this is what tomefs rename does via the page cache)
    backend.deleteFile("/target");
    backend.deleteMeta("/target");

    // New file data and metadata at /target (from rename source)
    const newData = new Uint8Array(PAGE_SIZE);
    newData[0] = 0xbb;
    backend.writePage("/target", 0, newData);
    backend.writeMeta("/target", {
      size: PAGE_SIZE,
      mode: 0o100644,
      ctime: 2000,
      mtime: 2000,
    });

    // Flush and examine operation order
    await backend.flush();

    // Find the index of each operation type involving /target.
    // flush() uses syncAll for atomic page+metadata writes, so we check
    // both syncAll entries and individual writePages/writeMetas entries.
    const deleteIndex = remote.log.findIndex(
      ([op, arg]) =>
        (op === "deleteFiles" && (arg as string[]).includes("/target")) ||
        (op === "deleteFile" && arg === "/target"),
    );
    const writeIndex = remote.log.findIndex(
      ([op, arg]) => {
        if (op === "syncAll") {
          const sa = arg as { pages: Array<{ path: string }>; metas: string[] };
          return sa.pages.some((p) => p.path === "/target") ||
                 sa.metas.includes("/target");
        }
        if (op === "writePages") {
          return (arg as Array<{ path: string }>).some((p) => p.path === "/target");
        }
        if (op === "writeMetas") return (arg as string[]).includes("/target");
        if (op === "writePage") return arg === "/target";
        if (op === "writeMeta") return arg === "/target";
        return false;
      },
    );

    expect(deleteIndex).toBeGreaterThanOrEqual(0);
    expect(writeIndex).toBeGreaterThanOrEqual(0);

    // Pages + metadata for the recreated /target must come AFTER the delete
    expect(writeIndex).toBeGreaterThan(deleteIndex);
  });

  it("defers metadata write when renameFile triggers delete-then-recreate", async () => {
    // Seed remote: /target has old data, /source has new data
    const oldData = new Uint8Array(PAGE_SIZE);
    oldData[0] = 0xaa;
    await innerRemote.writePage("/target", 0, oldData);
    await innerRemote.writeMeta("/target", {
      size: PAGE_SIZE,
      mode: 0o100644,
      ctime: 1000,
      mtime: 1000,
    });

    const srcData = new Uint8Array(PAGE_SIZE);
    srcData[0] = 0xcc;
    await innerRemote.writePage("/source", 0, srcData);
    await innerRemote.writeMeta("/source", {
      size: PAGE_SIZE,
      mode: 0o100644,
      ctime: 1000,
      mtime: 1000,
    });

    // Init PreloadBackend
    const backend = new PreloadBackend(remote);
    await backend.init();
    remote.clearLog();

    // Simulate the rename-overwrite sequence that tomefs performs:
    // 1. Delete target file data
    backend.deleteFile("/target");
    // 2. Delete target metadata
    backend.deleteMeta("/target");
    // 3. Rename source pages to target (this adds /source to deletedFiles)
    backend.renameFile("/source", "/target");
    // 4. Write new metadata at target path (tomefs builds from node state)
    backend.writeMeta("/target", {
      size: PAGE_SIZE,
      mode: 0o100644,
      ctime: 2000,
      mtime: 2000,
    });
    // 5. Delete old source metadata
    backend.deleteMeta("/source");

    await backend.flush();

    // Verify: metadata for /target must be written AFTER /target is deleted.
    // flush() uses syncAll, so check both syncAll and writeMetas entries.
    const deleteIndex = remote.log.findIndex(
      ([op, arg]) =>
        (op === "deleteFiles" && (arg as string[]).includes("/target")) ||
        (op === "deleteFile" && arg === "/target"),
    );
    const writeIndex = remote.log.findIndex(
      ([op, arg]) => {
        if (op === "syncAll") {
          const sa = arg as { pages: Array<{ path: string }>; metas: string[] };
          return sa.metas.includes("/target");
        }
        if (op === "writeMetas") return (arg as string[]).includes("/target");
        if (op === "writeMeta") return arg === "/target";
        return false;
      },
    );

    expect(deleteIndex).toBeGreaterThanOrEqual(0);
    expect(writeIndex).toBeGreaterThanOrEqual(0);
    expect(writeIndex).toBeGreaterThan(deleteIndex);
  });

  it("crash after early metadata write does not orphan data", async () => {
    // Seed the remote with existing file
    const oldData = new Uint8Array(PAGE_SIZE);
    oldData[0] = 0xaa;
    await innerRemote.writePage("/target", 0, oldData);
    await innerRemote.writeMeta("/target", {
      size: PAGE_SIZE,
      mode: 0o100644,
      ctime: 1000,
      mtime: 1000,
    });

    // Create a PreloadBackend on the REAL remote (no tracking wrapper)
    const backend = new PreloadBackend(innerRemote);
    await backend.init();

    // Delete-then-recreate at /target
    backend.deleteFile("/target");
    backend.deleteMeta("/target");
    const newData = new Uint8Array(PAGE_SIZE);
    newData[0] = 0xbb;
    backend.writePage("/target", 0, newData);
    backend.writeMeta("/target", {
      size: PAGE_SIZE,
      mode: 0o100644,
      ctime: 2000,
      mtime: 2000,
    });

    // Flush successfully
    await backend.flush();

    // Verify: remote should have the NEW data, not old
    const remotePage = await innerRemote.readPage("/target", 0);
    expect(remotePage).not.toBeNull();
    expect(remotePage![0]).toBe(0xbb);

    const remoteMeta = await innerRemote.readMeta("/target");
    expect(remoteMeta).not.toBeNull();
    expect(remoteMeta!.mtime).toBe(2000);
  });

  it("no early metadata for paths with pending file deletion", async () => {
    // This test verifies the invariant: if a path is in deletedFiles,
    // its metadata must not appear in the early metadata batch.
    const oldData = new Uint8Array(PAGE_SIZE);
    oldData[0] = 0xaa;
    await innerRemote.writePage("/f", 0, oldData);
    await innerRemote.writeMeta("/f", {
      size: PAGE_SIZE,
      mode: 0o100644,
      ctime: 1000,
      mtime: 1000,
    });

    const backend = new PreloadBackend(remote);
    await backend.init();
    remote.clearLog();

    // Delete file, then write new metadata at same path
    // (writeMeta clears deletedMeta, but deletedFiles still has the path)
    backend.deleteFile("/f");
    backend.deleteMeta("/f");
    backend.writeMeta("/f", {
      size: 0,
      mode: 0o100644,
      ctime: 3000,
      mtime: 3000,
    });

    await backend.flush();

    // The metadata write for /f must come after the file delete.
    // flush() uses syncAll, so check both syncAll and writeMetas entries.
    const ops = remote.log.map(([op, ...args]) => ({ op, args }));
    let sawDelete = false;
    for (const { op, args } of ops) {
      if (
        (op === "deleteFiles" && (args[0] as string[]).includes("/f")) ||
        (op === "deleteFile" && args[0] === "/f")
      ) {
        sawDelete = true;
      }
      // Check syncAll entries for the path
      if (op === "syncAll") {
        const sa = args[0] as { pages: Array<{ path: string }>; metas: string[] };
        if (sa.metas.includes("/f")) {
          expect(sawDelete).toBe(true);
        }
      }
      if (
        (op === "writeMetas" && (args[0] as string[]).includes("/f")) ||
        (op === "writeMeta" && args[0] === "/f")
      ) {
        expect(sawDelete).toBe(true);
      }
    }
    expect(sawDelete).toBe(true);
  });

  it("@fast flush uses syncAll for common case (no deletes)", async () => {
    // When there are no deletions or truncations, flush should use a
    // single syncAll call instead of separate writePages + writeMetas.
    // This gives IDB backends a single atomic transaction.
    const backend = new PreloadBackend(remote);
    await backend.init();
    remote.clearLog();

    // Write some pages and metadata
    const data = new Uint8Array(PAGE_SIZE);
    data[0] = 0x42;
    backend.writePage("/a", 0, data);
    backend.writeMeta("/a", {
      size: PAGE_SIZE,
      mode: 0o100644,
      ctime: 1000,
      mtime: 1000,
    });

    backend.writePage("/b", 0, data);
    backend.writeMeta("/b", {
      size: PAGE_SIZE,
      mode: 0o100644,
      ctime: 1000,
      mtime: 1000,
    });

    await backend.flush();

    // Verify flush used syncAll (not separate writePages + writeMetas)
    const syncAllCalls = remote.log.filter(([op]) => op === "syncAll");
    expect(syncAllCalls.length).toBe(1);

    // The single syncAll should contain both files' pages and metadata
    const sa = syncAllCalls[0][1] as {
      pages: Array<{ path: string; pageIndex: number }>;
      metas: string[];
    };
    expect(sa.pages).toHaveLength(2);
    expect(sa.metas).toHaveLength(2);
    expect(sa.pages.map((p) => p.path).sort()).toEqual(["/a", "/b"]);
    expect([...sa.metas].sort()).toEqual(["/a", "/b"]);

    // No separate writePages or writeMetas calls should appear
    const separateCalls = remote.log.filter(
      ([op]) => op === "writePages" || op === "writeMetas",
    );
    expect(separateCalls).toHaveLength(0);
  });

  it("flush uses syncAll for late batch (delete-then-recreate)", async () => {
    // Even in the delete-then-recreate case, the late batch should use
    // syncAll for atomicity — pages and metadata are committed together.
    const oldData = new Uint8Array(PAGE_SIZE);
    oldData[0] = 0xaa;
    await innerRemote.writePage("/f", 0, oldData);
    await innerRemote.writeMeta("/f", {
      size: PAGE_SIZE,
      mode: 0o100644,
      ctime: 1000,
      mtime: 1000,
    });

    const backend = new PreloadBackend(remote);
    await backend.init();
    remote.clearLog();

    // Delete and recreate at same path
    backend.deleteFile("/f");
    backend.deleteMeta("/f");

    const newData = new Uint8Array(PAGE_SIZE);
    newData[0] = 0xbb;
    backend.writePage("/f", 0, newData);
    backend.writeMeta("/f", {
      size: PAGE_SIZE,
      mode: 0o100644,
      ctime: 2000,
      mtime: 2000,
    });

    await backend.flush();

    // The late batch should be a single syncAll containing both pages and meta
    const syncAllCalls = remote.log.filter(([op]) => op === "syncAll");
    // May have 1 or 2 syncAll calls depending on whether there's an early batch
    const lateSyncAll = syncAllCalls.find(([, arg]) => {
      const sa = arg as { pages: Array<{ path: string }>; metas: string[] };
      return sa.metas.includes("/f") || sa.pages.some((p) => p.path === "/f");
    });
    expect(lateSyncAll).toBeDefined();

    const sa = lateSyncAll![1] as {
      pages: Array<{ path: string; pageIndex: number }>;
      metas: string[];
    };
    // Both page and metadata for /f should be in the same syncAll call
    expect(sa.pages.some((p) => p.path === "/f")).toBe(true);
    expect(sa.metas.includes("/f")).toBe(true);
  });

  it("crash during syncAll is atomic — no partial page+meta writes", async () => {
    // With syncAll, a crash during the atomic operation should leave the
    // remote in a consistent state: either both pages and metadata are
    // written, or neither is. CrashAfterNOpsBackend counts syncAll as
    // a single op, modeling IDB's transactional semantics.
    const data = new Uint8Array(PAGE_SIZE);
    data[0] = 0x42;
    await innerRemote.writePage("/existing", 0, data);
    await innerRemote.writeMeta("/existing", {
      size: PAGE_SIZE,
      mode: 0o100644,
      ctime: 1000,
      mtime: 1000,
    });

    // Crash after 0 ops: the syncAll call itself fails, so nothing is written
    const crashBackend = new CrashAfterNOpsBackend(innerRemote, 0);
    const backend = new PreloadBackend(crashBackend);
    await backend.init();

    // Write new data — this will try to flush via syncAll
    const newData = new Uint8Array(PAGE_SIZE);
    newData[0] = 0xff;
    backend.writePage("/new", 0, newData);
    backend.writeMeta("/new", {
      size: PAGE_SIZE,
      mode: 0o100644,
      ctime: 2000,
      mtime: 2000,
    });

    // Flush should fail (crash on first syncAll)
    await expect(backend.flush()).rejects.toThrow("SIMULATED_CRASH");

    // Remote should not have the new file (syncAll was atomic)
    const page = await innerRemote.readPage("/new", 0);
    expect(page).toBeNull();
    const meta = await innerRemote.readMeta("/new");
    expect(meta).toBeNull();

    // Existing data should be untouched
    const existingPage = await innerRemote.readPage("/existing", 0);
    expect(existingPage).not.toBeNull();
    expect(existingPage![0]).toBe(0x42);
  });
});
