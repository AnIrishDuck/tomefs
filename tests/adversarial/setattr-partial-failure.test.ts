/**
 * Adversarial tests for setattr behavior when resizeFileStorage fails.
 *
 * Verifies that attribute mutations (mode, timestamps) applied before a
 * resize failure are still marked dirty and persisted on the next syncfs.
 *
 * Without the fix, setattr applies mode/timestamp changes in-memory,
 * then calls resizeFileStorage which can throw (backend I/O error,
 * page cache eviction failure, etc.). If markMetaDirty is placed after
 * resizeFileStorage, the throw bypasses it — the node has new in-memory
 * mode/timestamps but is never marked dirty. On remount, the file
 * reverts to its pre-setattr metadata.
 *
 * Ethos §9: "Write tests designed to break tomefs specifically —
 * target the seams: metadata updates after flush"
 */

import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { describe, it, expect } from "vitest";
import { createTomeFS } from "../../src/tomefs.js";
import { PAGE_SIZE } from "../../src/types.js";
import type { SyncStorageBackend } from "../../src/sync-storage-backend.js";
import type { FileMeta } from "../../src/types.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

const O = {
  RDONLY: 0,
  WRONLY: 1,
  RDWR: 2,
  CREAT: 64,
  TRUNC: 512,
} as const;

const MOUNT = "/tome";

/**
 * Backend that wraps SyncMemoryBackend and can inject failures in
 * deletePagesFrom (called during truncation in resizeFileStorage).
 */
class FailOnTruncateBackend implements SyncStorageBackend {
  readonly inner = new SyncMemoryBackend();
  deletePagesFromFails = false;

  readPage(path: string, pageIndex: number) { return this.inner.readPage(path, pageIndex); }
  readPages(path: string, pageIndices: number[]) { return this.inner.readPages(path, pageIndices); }
  readPageBatch(entries: Array<{ path: string; pageIndex: number }>) { return this.inner.readPageBatch(entries); }
  writePage(path: string, pageIndex: number, data: Uint8Array) { this.inner.writePage(path, pageIndex, data); }
  writePages(pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>) { this.inner.writePages(pages); }
  deleteFile(path: string) { this.inner.deleteFile(path); }
  deleteFiles(paths: string[]) { this.inner.deleteFiles(paths); }
  deletePagesFrom(path: string, fromPageIndex: number) {
    if (this.deletePagesFromFails) {
      throw new Error("injected deletePagesFrom failure");
    }
    this.inner.deletePagesFrom(path, fromPageIndex);
  }
  renameFile(oldPath: string, newPath: string) { this.inner.renameFile(oldPath, newPath); }
  countPages(path: string) { return this.inner.countPages(path); }
  countPagesBatch(paths: string[]) { return this.inner.countPagesBatch(paths); }
  maxPageIndex(path: string) { return this.inner.maxPageIndex(path); }
  maxPageIndexBatch(paths: string[]) { return this.inner.maxPageIndexBatch(paths); }
  readMeta(path: string) { return this.inner.readMeta(path); }
  readMetas(paths: string[]) { return this.inner.readMetas(paths); }
  writeMeta(path: string, meta: FileMeta) { this.inner.writeMeta(path, meta); }
  writeMetas(entries: Array<{ path: string; meta: FileMeta }>) { this.inner.writeMetas(entries); }
  deleteMeta(path: string) { this.inner.deleteMeta(path); }
  deleteMetas(paths: string[]) { this.inner.deleteMetas(paths); }
  listFiles() { return this.inner.listFiles(); }
  syncAll(pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>, metas: Array<{ path: string; meta: FileMeta }>) {
    this.inner.syncAll(pages, metas);
  }
  deleteAll(paths: string[]) { this.inner.deleteAll(paths); }
  cleanupOrphanedPages() { return this.inner.cleanupOrphanedPages(); }
}

async function createHarness(backend: FailOnTruncateBackend, maxPages: number) {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  const Module = await createModule();
  const FS = Module.FS as any;

  const tomefs = createTomeFS(FS, { backend, maxPages });
  FS.mkdir(MOUNT);
  FS.mount(tomefs, {}, MOUNT);

  return { FS, tomefs };
}

function syncfs(FS: any): void {
  let syncErr: Error | null = null;
  FS.syncfs(false, (err: Error | null) => { syncErr = err; });
  if (syncErr) throw syncErr;
}

describe("setattr partial failure: mode persists despite resize failure", () => {
  it("marks node dirty when setattr combines mode+size and resize throws @fast", async () => {
    const backend = new FailOnTruncateBackend();
    const { FS } = await createHarness(backend, 64);

    // Create a multi-page file with mode 0o755
    const path = `${MOUNT}/testfile`;
    const fd = FS.open(path, O.CREAT | O.WRONLY, 0o755);
    const data = new Uint8Array(PAGE_SIZE * 3);
    data.fill(0x42);
    FS.write(fd, data, 0, data.length, 0);
    FS.close(fd);
    syncfs(FS);

    // Verify initial mode in backend
    const initialMeta = backend.inner.readMeta("/testfile");
    expect(initialMeta!.mode & 0o777).toBe(0o755);

    // Call setattr directly with BOTH mode change AND size change.
    // This exercises the code path where mode is applied in-memory
    // but markMetaDirty is only reached if resizeFileStorage doesn't throw.
    const node = FS.lookupPath(path).node;
    backend.deletePagesFromFails = true;
    expect(() => {
      node.node_ops.setattr(node, { mode: (node.mode & ~0o777) | 0o644, size: PAGE_SIZE });
    }).toThrow("injected deletePagesFrom failure");

    // In-memory mode should be updated despite the throw
    const stat = FS.stat(path);
    expect(stat.mode & 0o777).toBe(0o644);

    // Sync with failure disabled — the mode change must be persisted
    backend.deletePagesFromFails = false;
    syncfs(FS);

    const meta = backend.inner.readMeta("/testfile");
    expect(meta).not.toBeNull();
    expect(meta!.mode & 0o777).toBe(0o644);
  });

  it("mode change survives remount after combined setattr failure @fast", async () => {
    const backend = new FailOnTruncateBackend();
    const { FS: FS1 } = await createHarness(backend, 64);

    // Create a file with mode 0o777
    const path = `${MOUNT}/remountfile`;
    const fd = FS1.open(path, O.CREAT | O.WRONLY, 0o777);
    const data = new Uint8Array(PAGE_SIZE * 3);
    data.fill(0xAA);
    FS1.write(fd, data, 0, data.length, 0);
    FS1.close(fd);
    syncfs(FS1);

    // Combined mode+size setattr with resize failure
    const node = FS1.lookupPath(path).node;
    backend.deletePagesFromFails = true;
    try {
      node.node_ops.setattr(node, { mode: (node.mode & ~0o777) | 0o444, size: PAGE_SIZE });
    } catch {
      // Expected
    }

    // Sync and remount
    backend.deletePagesFromFails = false;
    syncfs(FS1);

    const { FS: FS2 } = await createHarness(backend, 64);
    const stat = FS2.stat(`${MOUNT}/remountfile`);
    expect(stat.mode & 0o777).toBe(0o444);
  });

  it("timestamp changes persist despite resize failure @fast", async () => {
    const backend = new FailOnTruncateBackend();
    const { FS } = await createHarness(backend, 64);

    // Create a file
    const path = `${MOUNT}/tsfile`;
    const fd = FS.open(path, O.CREAT | O.WRONLY);
    const data = new Uint8Array(PAGE_SIZE * 2);
    FS.write(fd, data, 0, data.length, 0);
    FS.close(fd);
    syncfs(FS);

    const initialMeta = backend.inner.readMeta("/tsfile");
    const newMtime = initialMeta!.mtime + 5000;

    // Combined timestamp+size setattr with resize failure
    const node = FS.lookupPath(path).node;
    backend.deletePagesFromFails = true;
    try {
      node.node_ops.setattr(node, { mtime: newMtime, size: PAGE_SIZE });
    } catch {
      // Expected
    }

    // Sync with failure disabled
    backend.deletePagesFromFails = false;
    syncfs(FS);

    const meta = backend.inner.readMeta("/tsfile");
    expect(meta!.mtime).toBe(newMtime);
  });

  it("truncate-only setattr still marks node dirty on failure @fast", async () => {
    const backend = new FailOnTruncateBackend();
    const { FS } = await createHarness(backend, 64);

    // Create a file, sync, then modify it without syncing
    const path = `${MOUNT}/dirtycheck`;
    const fd = FS.open(path, O.CREAT | O.WRONLY);
    const data = new Uint8Array(PAGE_SIZE * 3);
    data.fill(0x55);
    FS.write(fd, data, 0, data.length, 0);
    FS.close(fd);
    syncfs(FS);

    // Change mode (marks dirty), then sync (clears dirty)
    FS.chmod(path, 0o600);
    syncfs(FS);

    // Now the node is clean. Attempt a truncate that fails.
    // Without the fix, the node stays clean after the failed truncate,
    // meaning any metadata state won't be re-persisted on next syncfs.
    const dirtyCountBefore = (FS.lookupPath(MOUNT).node.mount.type as any).getStats().dirtyMetaCount;

    backend.deletePagesFromFails = true;
    try {
      FS.truncate(path, PAGE_SIZE);
    } catch {
      // Expected
    }
    backend.deletePagesFromFails = false;

    const dirtyCountAfter = (FS.lookupPath(MOUNT).node.mount.type as any).getStats().dirtyMetaCount;
    expect(dirtyCountAfter).toBeGreaterThan(dirtyCountBefore);
  });
});
