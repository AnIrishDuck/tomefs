/**
 * Adversarial tests for syncfs orphan cleanup failure and retry.
 *
 * The orphan cleanup path runs during syncfs when needsOrphanCleanup is true
 * (first syncfs after mount with no clean marker, or when /__deleted_* entries
 * exist). It calls backend.listFiles() to find stale paths not in the live
 * tree, then backend.deleteAll() to remove them.
 *
 * If deleteAll throws (IDB transaction abort, quota exceeded), dirty data
 * is already committed by syncAll (which runs before orphan cleanup), but
 * needsOrphanCleanup stays true — forcing a full tree walk on the next
 * syncfs to retry cleanup.
 *
 * Orphans arise from two sources:
 * 1. /__deleted_* entries: pages from unlinked files with open fds that
 *    survived a crash (restoreTree filters these from the live tree)
 * 2. Stale entries: files deleted from the live tree after restoreTree
 *    but before orphan cleanup runs in syncfs
 *
 * Ethos §9: "target the seams... metadata updates after flush"
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
const CLEAN_MARKER_PATH = "/__tomefs_clean";

function encode(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

function decode(buf: Uint8Array, length: number): string {
  return new TextDecoder().decode(buf.subarray(0, length));
}

/**
 * Backend fake that can inject failures in deleteAll (orphan cleanup)
 * and listFiles (orphan detection), while letting syncAll succeed.
 */
class OrphanCleanupFailBackend implements SyncStorageBackend {
  readonly inner = new SyncMemoryBackend();
  deleteAllFails = false;
  deleteAllFailCount = 0;
  listFilesFails = false;
  listFilesFailCount = 0;
  private listFilesCallCount = 0;
  listFilesFailAfterN = -1;

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
    this.inner.writeMeta(path, meta);
  }
  writeMetas(entries: Array<{ path: string; meta: FileMeta }>): void {
    this.inner.writeMetas(entries);
  }
  deleteMeta(path: string): void {
    this.inner.deleteMeta(path);
  }
  deleteMetas(paths: string[]): void {
    this.inner.deleteMetas(paths);
  }
  listFiles(): string[] {
    this.listFilesCallCount++;
    if (this.listFilesFails) {
      this.listFilesFailCount++;
      throw new Error("injected listFiles failure");
    }
    if (
      this.listFilesFailAfterN >= 0 &&
      this.listFilesCallCount > this.listFilesFailAfterN
    ) {
      this.listFilesFailCount++;
      throw new Error("injected listFiles failure (after N)");
    }
    return this.inner.listFiles();
  }
  syncAll(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
    metas: Array<{ path: string; meta: FileMeta }>,
  ): void {
    this.inner.syncAll(pages, metas);
  }
  deleteAll(paths: string[]): void {
    if (this.deleteAllFails) {
      this.deleteAllFailCount++;
      throw new Error("injected deleteAll failure");
    }
    this.inner.deleteAll(paths);
  }
}

async function freshMount(
  backend: OrphanCleanupFailBackend,
  maxPages: number,
) {
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

function syncfs(FS: any): Error | null {
  let err: Error | null = null;
  FS.syncfs(false, (e: Error | null) => {
    err = e;
  });
  return err;
}

/**
 * Inject a /__deleted_* orphan entry into the backend.
 * These are the real-world orphan type: pages from unlinked files with
 * open fds that survived a crash. restoreTree filters them from the live
 * tree but they remain in the backend until orphan cleanup runs.
 *
 * The presence of /__deleted_* entries forces needsOrphanCleanup = true
 * even when a clean marker is present.
 */
function injectDeletedOrphan(
  backend: OrphanCleanupFailBackend,
  id: number,
  dataSize: number = PAGE_SIZE,
) {
  const path = `/__deleted_${id}`;
  const pageCount = Math.ceil(dataSize / PAGE_SIZE) || 1;
  backend.inner.writeMeta(path, {
    size: dataSize,
    mode: 0o100666,
    ctime: Date.now(),
    mtime: Date.now(),
    atime: Date.now(),
  });
  for (let i = 0; i < pageCount; i++) {
    const page = new Uint8Array(PAGE_SIZE);
    page.fill((id * 17 + i * 3) & 0xff);
    backend.inner.writePage(path, i, page);
  }
}

describe("syncfs orphan cleanup failure and retry", () => {
  it("deleteAll failure preserves committed data and retries cleanup @fast", async () => {
    const backend = new OrphanCleanupFailBackend();
    const { FS } = await freshMount(backend, 64);

    // Create a file and sync
    const fd = FS.open(`${MOUNT}/keep`, O.WRONLY | O.CREAT, 0o666);
    FS.write(fd, encode("keep-data"), 0, 9);
    FS.close(fd);
    expect(syncfs(FS)).toBeNull();

    // Inject /__deleted_* orphan — forces needsOrphanCleanup = true on remount
    injectDeletedOrphan(backend, 1);

    // Remount
    const { FS: FS2 } = await freshMount(backend, 64);

    // First syncfs: deleteAll fails during orphan cleanup
    backend.deleteAllFails = true;
    const err = syncfs(FS2);
    expect(err).not.toBeNull();
    expect(err!.message).toContain("injected deleteAll failure");

    // The "keep" file's data was committed by syncAll before deleteAll.
    const buf = new Uint8Array(64);
    const rfd = FS2.open(`${MOUNT}/keep`, O.RDONLY);
    FS2.read(rfd, buf, 0, 64);
    FS2.close(rfd);
    expect(decode(buf, 9)).toBe("keep-data");
  });

  it("retry syncfs after deleteAll failure cleans up orphans @fast", async () => {
    const backend = new OrphanCleanupFailBackend();
    const { FS } = await freshMount(backend, 64);

    const fd = FS.open(`${MOUNT}/live`, O.WRONLY | O.CREAT, 0o666);
    FS.write(fd, encode("live"), 0, 4);
    FS.close(fd);
    expect(syncfs(FS)).toBeNull();

    // Inject orphan
    injectDeletedOrphan(backend, 42);

    // Verify orphan exists
    expect(backend.inner.readMeta("/__deleted_42")).not.toBeNull();

    // Remount
    const { FS: FS2 } = await freshMount(backend, 64);

    // First syncfs: deleteAll fails
    backend.deleteAllFails = true;
    expect(syncfs(FS2)).not.toBeNull();
    expect(backend.deleteAllFailCount).toBe(1);

    // Orphan still in backend
    expect(backend.inner.readMeta("/__deleted_42")).not.toBeNull();

    // Retry: deleteAll succeeds
    backend.deleteAllFails = false;
    expect(syncfs(FS2)).toBeNull();

    // Orphan cleaned up
    expect(backend.inner.readMeta("/__deleted_42")).toBeNull();
    expect(backend.inner.countPages("/__deleted_42")).toBe(0);
  });

  it("multiple deleteAll failures before success still cleans up", async () => {
    const backend = new OrphanCleanupFailBackend();
    const { FS } = await freshMount(backend, 64);

    const fd = FS.open(`${MOUNT}/file`, O.WRONLY | O.CREAT, 0o666);
    FS.write(fd, encode("data"), 0, 4);
    FS.close(fd);
    expect(syncfs(FS)).toBeNull();

    injectDeletedOrphan(backend, 99);
    const { FS: FS2 } = await freshMount(backend, 64);

    // Fail three times
    backend.deleteAllFails = true;
    for (let i = 0; i < 3; i++) {
      expect(syncfs(FS2)).not.toBeNull();
    }
    expect(backend.deleteAllFailCount).toBe(3);

    // Succeed on fourth
    backend.deleteAllFails = false;
    expect(syncfs(FS2)).toBeNull();

    expect(backend.inner.readMeta("/__deleted_99")).toBeNull();
    expect(backend.inner.countPages("/__deleted_99")).toBe(0);
  });

  it("new writes between failed and retried syncfs are preserved @fast", async () => {
    const backend = new OrphanCleanupFailBackend();
    const { FS } = await freshMount(backend, 64);

    const fd = FS.open(`${MOUNT}/a`, O.WRONLY | O.CREAT, 0o666);
    FS.write(fd, encode("aaa"), 0, 3);
    FS.close(fd);
    expect(syncfs(FS)).toBeNull();

    injectDeletedOrphan(backend, 1);
    const { FS: FS2 } = await freshMount(backend, 64);

    // First syncfs fails at deleteAll
    backend.deleteAllFails = true;
    expect(syncfs(FS2)).not.toBeNull();

    // Write more data between failed and retry
    const fd2 = FS2.open(`${MOUNT}/a`, O.WRONLY, 0o666);
    FS2.write(fd2, encode("AAA"), 0, 3);
    FS2.close(fd2);

    const fd3 = FS2.open(`${MOUNT}/b`, O.WRONLY | O.CREAT, 0o666);
    FS2.write(fd3, encode("bbb"), 0, 3);
    FS2.close(fd3);

    // Retry succeeds — commits new writes AND cleans up orphans
    backend.deleteAllFails = false;
    expect(syncfs(FS2)).toBeNull();

    // Verify by remounting
    const { FS: FS3 } = await freshMount(backend, 64);
    const buf = new Uint8Array(64);

    const ra = FS3.open(`${MOUNT}/a`, O.RDONLY);
    FS3.read(ra, buf, 0, 64);
    FS3.close(ra);
    expect(decode(buf, 3)).toBe("AAA");

    const rb = FS3.open(`${MOUNT}/b`, O.RDONLY);
    FS3.read(rb, buf, 0, 64);
    FS3.close(rb);
    expect(decode(buf, 3)).toBe("bbb");

    expect(backend.inner.readMeta("/__deleted_1")).toBeNull();
  });

  it("listFiles failure during orphan detection preserves data @fast", async () => {
    const backend = new OrphanCleanupFailBackend();
    const { FS } = await freshMount(backend, 64);

    const fd = FS.open(`${MOUNT}/safe`, O.WRONLY | O.CREAT, 0o666);
    FS.write(fd, encode("safe-data"), 0, 9);
    FS.close(fd);
    expect(syncfs(FS)).toBeNull();

    // Inject orphan to force orphan cleanup path on remount
    injectDeletedOrphan(backend, 1);
    const { FS: FS2 } = await freshMount(backend, 64);

    // Update safe file
    const fd2 = FS2.open(`${MOUNT}/safe`, O.WRONLY, 0o666);
    FS2.write(fd2, encode("updated!"), 0, 8);
    FS2.close(fd2);

    // listFiles fails during orphan detection. restoreTree calls listFiles
    // once at mount time; the syncfs orphan path calls it a second time.
    // Fail only the syncfs call.
    backend.listFilesFailAfterN = 1;
    const err = syncfs(FS2);
    expect(err).not.toBeNull();
    expect(err!.message).toContain("injected listFiles failure");

    // syncAll committed the updated data before listFiles was called.
    backend.listFilesFailAfterN = -1;
    const { FS: FS3 } = await freshMount(backend, 64);
    const buf = new Uint8Array(64);
    const rfd = FS3.open(`${MOUNT}/safe`, O.RDONLY);
    FS3.read(rfd, buf, 0, 64);
    FS3.close(rfd);
    expect(decode(buf, 8)).toBe("updated!");
  });

  it("deleteAll failure under cache pressure preserves evicted data @fast", async () => {
    const backend = new OrphanCleanupFailBackend();
    const { FS } = await freshMount(backend, 4); // tiny cache

    // Create file with 4 pages (fills entire cache), sync
    const data = new Uint8Array(PAGE_SIZE * 4);
    for (let i = 0; i < data.length; i++) data[i] = (i * 7 + 3) & 0xff;
    const fd = FS.open(`${MOUNT}/big`, O.WRONLY | O.CREAT, 0o666);
    FS.write(fd, data, 0, data.length);
    FS.close(fd);
    expect(syncfs(FS)).toBeNull();

    injectDeletedOrphan(backend, 1);
    const { FS: FS2 } = await freshMount(backend, 4);

    // Write to big file — causes eviction under 4-page cache
    const update = new Uint8Array(PAGE_SIZE);
    update.fill(0xee);
    const fd2 = FS2.open(`${MOUNT}/big`, O.WRONLY, 0o666);
    FS2.write(fd2, update, 0, PAGE_SIZE, 0);
    FS2.close(fd2);

    // syncfs fails at deleteAll
    backend.deleteAllFails = true;
    expect(syncfs(FS2)).not.toBeNull();

    // Retry succeeds
    backend.deleteAllFails = false;
    expect(syncfs(FS2)).toBeNull();

    // Verify big file page 0 has the update
    const { FS: FS3 } = await freshMount(backend, 64);
    const buf = new Uint8Array(PAGE_SIZE);
    const rfd = FS3.open(`${MOUNT}/big`, O.RDONLY);
    FS3.read(rfd, buf, 0, PAGE_SIZE);
    FS3.close(rfd);
    for (let i = 0; i < PAGE_SIZE; i++) {
      if (buf[i] !== 0xee) {
        throw new Error(`Byte ${i}: expected 0xee, got 0x${buf[i].toString(16)}`);
      }
    }

    // Orphan cleaned up
    expect(backend.inner.readMeta("/__deleted_1")).toBeNull();
  });

  it("multiple orphans cleaned up after single retry @fast", async () => {
    const backend = new OrphanCleanupFailBackend();
    const { FS } = await freshMount(backend, 64);

    const fd = FS.open(`${MOUNT}/live`, O.WRONLY | O.CREAT, 0o666);
    FS.write(fd, encode("live"), 0, 4);
    FS.close(fd);
    expect(syncfs(FS)).toBeNull();

    // Inject multiple orphans
    injectDeletedOrphan(backend, 10, PAGE_SIZE);
    injectDeletedOrphan(backend, 20, PAGE_SIZE * 2);
    injectDeletedOrphan(backend, 30, PAGE_SIZE * 3);

    const { FS: FS2 } = await freshMount(backend, 64);

    // First syncfs: deleteAll fails
    backend.deleteAllFails = true;
    expect(syncfs(FS2)).not.toBeNull();

    // All orphans still present
    expect(backend.inner.readMeta("/__deleted_10")).not.toBeNull();
    expect(backend.inner.readMeta("/__deleted_20")).not.toBeNull();
    expect(backend.inner.readMeta("/__deleted_30")).not.toBeNull();

    // Retry succeeds
    backend.deleteAllFails = false;
    expect(syncfs(FS2)).toBeNull();

    // All orphans cleaned up
    expect(backend.inner.readMeta("/__deleted_10")).toBeNull();
    expect(backend.inner.readMeta("/__deleted_20")).toBeNull();
    expect(backend.inner.readMeta("/__deleted_30")).toBeNull();
    expect(backend.inner.countPages("/__deleted_10")).toBe(0);
    expect(backend.inner.countPages("/__deleted_20")).toBe(0);
    expect(backend.inner.countPages("/__deleted_30")).toBe(0);

    // Live file intact
    const buf = new Uint8Array(64);
    const rfd = FS2.open(`${MOUNT}/live`, O.RDONLY);
    FS2.read(rfd, buf, 0, 64);
    FS2.close(rfd);
    expect(decode(buf, 4)).toBe("live");
  });

  it("file unlink after mount creates orphan cleaned up on retry", async () => {
    const backend = new OrphanCleanupFailBackend();
    const { FS } = await freshMount(backend, 64);

    // Create two files, sync
    const fd1 = FS.open(`${MOUNT}/keep`, O.WRONLY | O.CREAT, 0o666);
    FS.write(fd1, encode("keep"), 0, 4);
    FS.close(fd1);
    const fd2 = FS.open(`${MOUNT}/remove`, O.WRONLY | O.CREAT, 0o666);
    FS.write(fd2, encode("remove"), 0, 6);
    FS.close(fd2);
    expect(syncfs(FS)).toBeNull();

    // Inject /__deleted_* to force needsOrphanCleanup = true on remount
    injectDeletedOrphan(backend, 1);

    const { FS: FS2 } = await freshMount(backend, 64);

    // Delete "remove" from the live tree — creates stale backend entry
    // (unlink deletes pages+meta immediately, but the orphan cleanup
    // path still runs because of the injected /__deleted_* entry)
    FS2.unlink(`${MOUNT}/remove`);

    // First syncfs: deleteAll fails
    backend.deleteAllFails = true;
    expect(syncfs(FS2)).not.toBeNull();

    // Retry succeeds
    backend.deleteAllFails = false;
    expect(syncfs(FS2)).toBeNull();

    // Verify state
    const { FS: FS3 } = await freshMount(backend, 64);
    const buf = new Uint8Array(64);
    const rfd = FS3.open(`${MOUNT}/keep`, O.RDONLY);
    FS3.read(rfd, buf, 0, 64);
    FS3.close(rfd);
    expect(decode(buf, 4)).toBe("keep");
    expect(() => FS3.stat(`${MOUNT}/remove`)).toThrow();
  });

  it("chmod + deleteAll failure preserves mode on retry @fast", async () => {
    const backend = new OrphanCleanupFailBackend();
    const { FS } = await freshMount(backend, 64);

    const fd = FS.open(`${MOUNT}/meta`, O.WRONLY | O.CREAT, 0o666);
    FS.write(fd, encode("data"), 0, 4);
    FS.close(fd);
    expect(syncfs(FS)).toBeNull();

    injectDeletedOrphan(backend, 1);
    const { FS: FS2 } = await freshMount(backend, 64);

    // Modify metadata
    FS2.chmod(`${MOUNT}/meta`, 0o600);

    // syncfs fails
    backend.deleteAllFails = true;
    expect(syncfs(FS2)).not.toBeNull();

    // Retry
    backend.deleteAllFails = false;
    expect(syncfs(FS2)).toBeNull();

    // Verify by remounting
    const { FS: FS3 } = await freshMount(backend, 64);
    const stat = FS3.stat(`${MOUNT}/meta`);
    expect(stat.mode & 0o777).toBe(0o600);
  });
});
