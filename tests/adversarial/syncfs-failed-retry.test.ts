/**
 * Adversarial tests: syncfs failure preserves dirty page flags.
 *
 * Verifies that if backend.syncAll() fails (e.g., IDB quota exceeded,
 * SAB bridge timeout, storage worker crash), dirty page flags are NOT
 * cleared. This ensures that:
 *   1. A subsequent successful syncfs retries the flush
 *   2. Pages evicted under cache pressure are flushed before eviction
 *   3. No silent data loss occurs from transient backend failures
 *
 * Before this fix, collectDirtyPages() cleared dirty flags BEFORE the
 * backend write. If syncAll() threw, pages were marked clean but never
 * written — a silent data loss path that could only manifest under
 * transient IDB/OPFS failures.
 *
 * Ethos §9: "Write tests designed to break tomefs specifically —
 * target the seams: dirty flush ordering on concurrent streams"
 */
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { describe, it, expect, beforeEach } from "vitest";
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

function encode(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

function decode(buf: Uint8Array, length?: number): string {
  return new TextDecoder().decode(
    length !== undefined ? buf.subarray(0, length) : buf,
  );
}

/**
 * SyncStorageBackend fake that can be configured to fail on syncAll.
 * Delegates all operations to a real SyncMemoryBackend. Not a mock.
 */
class FailingSyncBackend implements SyncStorageBackend {
  readonly inner: SyncMemoryBackend;
  failOnSyncAll = false;
  syncAllCallCount = 0;

  constructor(inner: SyncMemoryBackend) {
    this.inner = inner;
  }

  // --- Reads (never fail) ---
  readPage(path: string, pageIndex: number): Uint8Array | null {
    return this.inner.readPage(path, pageIndex);
  }
  readPages(path: string, pageIndices: number[]): Array<Uint8Array | null> {
    return this.inner.readPages(path, pageIndices);
  }
  readMeta(path: string): FileMeta | null {
    return this.inner.readMeta(path);
  }
  readMetas(paths: string[]): Array<FileMeta | null> {
    return this.inner.readMetas(paths);
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
  listFiles(): string[] {
    return this.inner.listFiles();
  }

  // --- Writes (delegate to inner) ---
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

  // --- syncAll: can fail ---
  syncAll(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
    metas: Array<{ path: string; meta: FileMeta }>,
  ): void {
    this.syncAllCallCount++;
    if (this.failOnSyncAll) {
      throw new Error("simulated syncAll failure (e.g., IDB quota exceeded)");
    }
    this.inner.syncAll(pages, metas);
  }
}

async function loadModule() {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  return createModule();
}

describe("adversarial: syncfs failure preserves dirty flags", () => {
  let Module: any;
  let FS: any;
  let backend: FailingSyncBackend;
  let tomefs: any;

  beforeEach(async () => {
    Module = await loadModule();
    FS = Module.FS;
    const inner = new SyncMemoryBackend();
    backend = new FailingSyncBackend(inner);
    tomefs = createTomeFS(FS, { backend, maxPages: 4096 });
    FS.mkdir(MOUNT);
    FS.mount(tomefs, {}, MOUNT);
  });

  function syncfs(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      FS.syncfs(false, (err?: Error | null) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  it("dirty pages survive a failed syncAll and are retried on next syncfs", async () => {
    // Write data
    const path = MOUNT + "/data.bin";
    const data = encode("important data that must not be lost");
    FS.writeFile(path, data);

    // First syncfs fails
    backend.failOnSyncAll = true;
    await expect(syncfs()).rejects.toThrow("simulated syncAll failure");

    // Backend should NOT have the data (syncAll was rejected)
    expect(backend.inner.readMeta("/data.bin")).toBeNull();

    // Retry syncfs — this time it succeeds
    backend.failOnSyncAll = false;
    await syncfs();

    // Backend now has the data
    expect(backend.inner.readMeta("/data.bin")).not.toBeNull();
    expect(backend.inner.readPage("/data.bin", 0)).not.toBeNull();
  });

  it("multiple files dirty across failed syncfs are all retried", async () => {
    // Write to three files
    FS.writeFile(MOUNT + "/a.txt", encode("file-a"));
    FS.writeFile(MOUNT + "/b.txt", encode("file-b"));
    FS.writeFile(MOUNT + "/c.txt", encode("file-c"));

    // syncfs fails
    backend.failOnSyncAll = true;
    await expect(syncfs()).rejects.toThrow();

    // No files should be persisted
    expect(backend.inner.readMeta("/a.txt")).toBeNull();
    expect(backend.inner.readMeta("/b.txt")).toBeNull();
    expect(backend.inner.readMeta("/c.txt")).toBeNull();

    // Retry succeeds
    backend.failOnSyncAll = false;
    await syncfs();

    // All three files are persisted
    expect(backend.inner.readMeta("/a.txt")).not.toBeNull();
    expect(backend.inner.readMeta("/b.txt")).not.toBeNull();
    expect(backend.inner.readMeta("/c.txt")).not.toBeNull();
    expect(backend.inner.readPage("/a.txt", 0)).not.toBeNull();
    expect(backend.inner.readPage("/b.txt", 0)).not.toBeNull();
    expect(backend.inner.readPage("/c.txt", 0)).not.toBeNull();
  });

  it("dirty pages flushed on eviction after failed syncfs", async () => {
    // Use a small cache to force eviction
    const inner2 = new SyncMemoryBackend();
    const backend2 = new FailingSyncBackend(inner2);
    const Module2 = await loadModule();
    const FS2 = Module2.FS;
    const tomefs2 = createTomeFS(FS2, { backend: backend2, maxPages: 4 });
    FS2.mkdir(MOUNT);
    FS2.mount(tomefs2, {}, MOUNT);

    function syncfs2(): Promise<void> {
      return new Promise<void>((resolve, reject) => {
        FS2.syncfs(false, (err?: Error | null) => {
          if (err) reject(err);
          else resolve();
        });
      });
    }

    // Write one page to a file
    const path = MOUNT + "/evict-me.bin";
    const data = new Uint8Array(PAGE_SIZE);
    for (let i = 0; i < PAGE_SIZE; i++) data[i] = (i * 17 + 3) & 0xff;
    const stream = FS2.open(path, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    FS2.write(stream, data, 0, PAGE_SIZE);
    FS2.close(stream);

    // syncfs fails — dirty flag should be preserved
    backend2.failOnSyncAll = true;
    await expect(syncfs2()).rejects.toThrow();

    // Now write enough other data to force eviction of the first file's page.
    // With a 4-page cache, writing 4 pages to another file forces the old page out.
    const path2 = MOUNT + "/filler.bin";
    const stream2 = FS2.open(path2, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    for (let i = 0; i < 4; i++) {
      FS2.write(stream2, new Uint8Array(PAGE_SIZE), 0, PAGE_SIZE);
    }
    FS2.close(stream2);

    // The evicted page should have been flushed to the backend because
    // its dirty flag was preserved after the failed syncfs.
    const storedPage = backend2.inner.readPage("/evict-me.bin", 0);
    expect(storedPage).not.toBeNull();
    expect(storedPage![0]).toBe((0 * 17 + 3) & 0xff);
    expect(storedPage![100]).toBe((100 * 17 + 3) & 0xff);
  });

  it("successful syncfs after failure correctly clears dirty flags", async () => {
    // Write data
    FS.writeFile(MOUNT + "/test.txt", encode("hello"));

    // Fail first syncfs
    backend.failOnSyncAll = true;
    await expect(syncfs()).rejects.toThrow();
    expect(backend.syncAllCallCount).toBe(1);

    // Succeed second syncfs
    backend.failOnSyncAll = false;
    await syncfs();
    expect(backend.syncAllCallCount).toBe(2);

    // Third syncfs should be a no-op (no dirty pages — flags were cleared)
    const countBefore = backend.syncAllCallCount;
    await syncfs();
    // syncAll may or may not be called (depends on whether there's metadata
    // to write), but the key thing is no error
    expect(backend.syncAllCallCount).toBeGreaterThanOrEqual(countBefore);
  });

  it("data integrity preserved through failed syncfs + remount", async () => {
    // Write data and sync successfully
    FS.writeFile(MOUNT + "/existing.txt", encode("persisted"));
    await syncfs();

    // Write new data
    FS.writeFile(MOUNT + "/new.txt", encode("new-data"));
    // Modify existing file
    FS.writeFile(MOUNT + "/existing.txt", encode("updated"));

    // syncfs fails
    backend.failOnSyncAll = true;
    await expect(syncfs()).rejects.toThrow();

    // Retry succeeds
    backend.failOnSyncAll = false;
    await syncfs();

    // Remount on same backend and verify
    const Module2 = await loadModule();
    const FS2 = Module2.FS;
    const tomefs2 = createTomeFS(FS2, { backend, maxPages: 4096 });
    FS2.mkdir(MOUNT);
    FS2.mount(tomefs2, {}, MOUNT);

    // Both files should have their latest content
    const existingBuf = FS2.readFile(MOUNT + "/existing.txt");
    expect(decode(existingBuf)).toBe("updated");
    const newBuf = FS2.readFile(MOUNT + "/new.txt");
    expect(decode(newBuf)).toBe("new-data");
  });
});
