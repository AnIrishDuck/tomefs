/**
 * Adversarial tests for syncfs behavior when the backend write fails.
 *
 * Verifies that dirty page flags are preserved when backend.syncAll() throws,
 * so the next syncfs retries the flush instead of silently losing data.
 *
 * This targets a specific data loss scenario:
 *   1. syncfs collects dirty pages and calls backend.syncAll()
 *   2. syncAll throws (IDB quota exceeded, network error, etc.)
 *   3. If dirty flags were cleared before syncAll, the pages are now
 *      "clean" in the cache despite never being written to the backend
 *   4. Under cache pressure, these pages are evicted without flushing
 *   5. Data is silently lost
 *
 * The fix: two-phase commit — collectDirtyPages() preserves dirty flags,
 * commitDirtyPages() clears them only after confirmed backend write.
 *
 * Ethos §9: "Write tests designed to break tomefs specifically — target
 * the seams: metadata updates after flush, dirty flush ordering"
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

function encode(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

function decode(buf: Uint8Array, length?: number): string {
  return new TextDecoder().decode(
    length !== undefined ? buf.subarray(0, length) : buf,
  );
}

/**
 * SyncStorageBackend fake that wraps SyncMemoryBackend and can inject
 * failures in syncAll (the critical path for syncfs data persistence).
 */
class FailOnSyncBackend implements SyncStorageBackend {
  readonly inner = new SyncMemoryBackend();
  syncAllFails = false;
  syncAllFailCount = 0;

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
    return this.inner.listFiles();
  }
  syncAll(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
    metas: Array<{ path: string; meta: FileMeta }>,
  ): void {
    if (this.syncAllFails) {
      this.syncAllFailCount++;
      throw new Error("injected syncAll failure");
    }
    this.inner.syncAll(pages, metas);
  }
}

async function createHarness(backend: FailOnSyncBackend, maxPages: number) {
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

describe("syncfs write failure recovery", () => {
  it("preserves dirty page flags when syncAll fails (incremental path)", async () => {
    const backend = new FailOnSyncBackend();
    const { FS } = await createHarness(backend, 64);

    // Write data
    const data = encode("important data that must survive");
    const fd = FS.open(`${MOUNT}/file`, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    FS.write(fd, data, 0, data.length);
    FS.close(fd);

    // First syncfs succeeds — establishes baseline
    let syncErr: Error | null = null;
    FS.syncfs(false, (err: Error | null) => {
      syncErr = err;
    });
    expect(syncErr).toBeNull();

    // Write more data
    const data2 = encode("second write");
    const fd2 = FS.open(`${MOUNT}/file`, O.WRONLY, 0o666);
    FS.write(fd2, data2, 0, data2.length);
    FS.close(fd2);

    // syncfs fails — backend rejects the write
    backend.syncAllFails = true;
    syncErr = null;
    FS.syncfs(false, (err: Error | null) => {
      syncErr = err;
    });
    expect(syncErr).not.toBeNull();
    expect(syncErr!.message).toContain("injected syncAll failure");

    // Retry syncfs — should re-flush the dirty pages
    backend.syncAllFails = false;
    syncErr = null;
    FS.syncfs(false, (err: Error | null) => {
      syncErr = err;
    });
    expect(syncErr).toBeNull();

    // Verify: data survived by remounting from the backend
    const { default: createModule2 } = await import(
      join(__dirname, "../harness/emscripten_fs.mjs")
    );
    const Module2 = await createModule2();
    const FS2 = Module2.FS as any;
    const tomefs2 = createTomeFS(FS2, { backend, maxPages: 64 });
    FS2.mkdir(MOUNT);
    FS2.mount(tomefs2, {}, MOUNT);

    const readBuf = new Uint8Array(256);
    const rfd = FS2.open(`${MOUNT}/file`, O.RDONLY);
    const bytesRead = FS2.read(rfd, readBuf, 0, readBuf.length);
    FS2.close(rfd);
    expect(decode(readBuf, data2.length)).toBe("second write");
  });

  it("preserves dirty page flags when syncAll fails (full tree walk path)", async () => {
    const backend = new FailOnSyncBackend();
    const { FS } = await createHarness(backend, 64);

    // Write data
    const data = encode("tree walk data");
    const fd = FS.open(`${MOUNT}/treetest`, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    FS.write(fd, data, 0, data.length);
    FS.close(fd);

    // First sync
    FS.syncfs(false, (err: Error | null) => {
      if (err) throw err;
    });

    // Write more data then unlink a different file to force full tree walk
    // (unlink sets needsOrphanCleanup = true)
    const dummy = encode("dummy");
    const dfd = FS.open(
      `${MOUNT}/dummy`,
      O.WRONLY | O.CREAT | O.TRUNC,
      0o666,
    );
    FS.write(dfd, dummy, 0, dummy.length);
    FS.close(dfd);
    FS.syncfs(false, (err: Error | null) => {
      if (err) throw err;
    });

    // Now write to treetest and unlink dummy to trigger orphan cleanup path
    const data2 = encode("updated tree walk data");
    const fd2 = FS.open(`${MOUNT}/treetest`, O.WRONLY, 0o666);
    FS.write(fd2, data2, 0, data2.length);
    FS.close(fd2);
    FS.unlink(`${MOUNT}/dummy`);

    // syncfs fails on full tree walk path
    backend.syncAllFails = true;
    let syncErr: Error | null = null;
    FS.syncfs(false, (err: Error | null) => {
      syncErr = err;
    });
    expect(syncErr).not.toBeNull();

    // Retry succeeds
    backend.syncAllFails = false;
    syncErr = null;
    FS.syncfs(false, (err: Error | null) => {
      syncErr = err;
    });
    expect(syncErr).toBeNull();

    // Verify data by remounting
    const { default: createModule2 } = await import(
      join(__dirname, "../harness/emscripten_fs.mjs")
    );
    const Module2 = await createModule2();
    const FS2 = Module2.FS as any;
    const tomefs2 = createTomeFS(FS2, { backend, maxPages: 64 });
    FS2.mkdir(MOUNT);
    FS2.mount(tomefs2, {}, MOUNT);

    const readBuf = new Uint8Array(256);
    const rfd = FS2.open(`${MOUNT}/treetest`, O.RDONLY);
    const bytesRead = FS2.read(rfd, readBuf, 0, readBuf.length);
    FS2.close(rfd);
    expect(decode(readBuf, data2.length)).toBe("updated tree walk data");
  });

  it("dirty pages survive cache eviction after failed syncfs + retry", async () => {
    // This is the critical scenario: if dirty flags were cleared on failure,
    // eviction would silently discard the data.
    const backend = new FailOnSyncBackend();
    const { FS, tomefs } = await createHarness(backend, 4); // tiny cache

    // Fill cache with dirty pages from one file (4 pages = 32 KB)
    const fileData = new Uint8Array(PAGE_SIZE * 4);
    for (let i = 0; i < fileData.length; i++) {
      fileData[i] = (i * 31 + 17) & 0xff;
    }
    const fd = FS.open(
      `${MOUNT}/bigfile`,
      O.WRONLY | O.CREAT | O.TRUNC,
      0o666,
    );
    FS.write(fd, fileData, 0, fileData.length);
    FS.close(fd);

    // syncfs fails
    backend.syncAllFails = true;
    let syncErr: Error | null = null;
    FS.syncfs(false, (err: Error | null) => {
      syncErr = err;
    });
    expect(syncErr).not.toBeNull();

    // Write to a DIFFERENT file, causing cache pressure that evicts
    // pages from bigfile. If dirty flags were cleared, these evictions
    // would not flush the data to the backend -> data loss.
    const otherData = new Uint8Array(PAGE_SIZE * 4);
    otherData.fill(0xaa);
    backend.syncAllFails = false; // let eviction flushes succeed
    const fd2 = FS.open(
      `${MOUNT}/other`,
      O.WRONLY | O.CREAT | O.TRUNC,
      0o666,
    );
    FS.write(fd2, otherData, 0, otherData.length);
    FS.close(fd2);

    // Now sync everything
    syncErr = null;
    FS.syncfs(false, (err: Error | null) => {
      syncErr = err;
    });
    expect(syncErr).toBeNull();

    // Verify bigfile data by remounting
    const { default: createModule2 } = await import(
      join(__dirname, "../harness/emscripten_fs.mjs")
    );
    const Module2 = await createModule2();
    const FS2 = Module2.FS as any;
    const tomefs2 = createTomeFS(FS2, { backend, maxPages: 64 });
    FS2.mkdir(MOUNT);
    FS2.mount(tomefs2, {}, MOUNT);

    const readBuf = new Uint8Array(PAGE_SIZE * 4);
    const rfd = FS2.open(`${MOUNT}/bigfile`, O.RDONLY);
    const bytesRead = FS2.read(rfd, readBuf, 0, readBuf.length);
    FS2.close(rfd);
    expect(bytesRead).toBe(PAGE_SIZE * 4);
    // Verify every byte matches the original data
    for (let i = 0; i < fileData.length; i++) {
      if (readBuf[i] !== fileData[i]) {
        throw new Error(
          `Data mismatch at byte ${i}: expected ${fileData[i]}, got ${readBuf[i]}`,
        );
      }
    }
  });

  it("metadata dirty flags preserved on syncfs failure", async () => {
    const backend = new FailOnSyncBackend();
    const { FS } = await createHarness(backend, 64);

    // Create file and initial sync
    const data = encode("hello");
    const fd = FS.open(`${MOUNT}/meta`, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    FS.write(fd, data, 0, data.length);
    FS.close(fd);
    FS.syncfs(false, (err: Error | null) => {
      if (err) throw err;
    });

    // Modify metadata (chmod)
    FS.chmod(`${MOUNT}/meta`, 0o600);

    // syncfs fails
    backend.syncAllFails = true;
    let syncErr: Error | null = null;
    FS.syncfs(false, (err: Error | null) => {
      syncErr = err;
    });
    expect(syncErr).not.toBeNull();

    // Retry
    backend.syncAllFails = false;
    syncErr = null;
    FS.syncfs(false, (err: Error | null) => {
      syncErr = err;
    });
    expect(syncErr).toBeNull();

    // Verify metadata by remounting
    const { default: createModule2 } = await import(
      join(__dirname, "../harness/emscripten_fs.mjs")
    );
    const Module2 = await createModule2();
    const FS2 = Module2.FS as any;
    const tomefs2 = createTomeFS(FS2, { backend, maxPages: 64 });
    FS2.mkdir(MOUNT);
    FS2.mount(tomefs2, {}, MOUNT);

    const stat = FS2.stat(`${MOUNT}/meta`);
    expect(stat.mode & 0o777).toBe(0o600);
  });
});
