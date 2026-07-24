/**
 * PreloadBackend — graceful degradation for environments without SharedArrayBuffer.
 *
 * When COOP/COEP headers aren't available, the SAB+Atomics sync bridge can't
 * be used. PreloadBackend provides an alternative: it wraps any async
 * StorageBackend, preloads all data into memory at init time, and exposes
 * the SyncStorageBackend interface for use with SyncPageCache and tomefs.
 *
 * Dirty tracking allows flushing modified data back to the underlying async
 * backend via the async flush() method (called explicitly or via FS.syncfs).
 *
 * Trade-offs vs SAB bridge:
 *   - Slower startup (must load all pages into memory)
 *   - Higher memory usage (all accessed data stays in memory, though the
 *     SyncPageCache still bounds the working set)
 *   - Writes are only durable after explicit flush()
 *   + Works without COOP/COEP headers
 *   + No Web Worker required for storage
 *   + Simpler deployment
 *
 * Usage:
 *   const idb = new IdbBackend({ dbName: 'mydb' });
 *   const backend = new PreloadBackend(idb);
 *   await backend.init();
 *   const tomefs = createTomeFS(Module.FS, { backend });
 *   // ... use tomefs ...
 *   await backend.flush(); // persist dirty data
 */

import type { StorageBackend } from "./storage-backend.js";
import type { SyncStorageBackend } from "./sync-storage-backend.js";
import type { FileMeta } from "./types.js";
import { pageKeyStr } from "./types.js";
import { InMemoryPageStore } from "./in-memory-page-store.js";

export class PreloadBackend implements SyncStorageBackend {
  private readonly remote: StorageBackend;
  private readonly store = new InMemoryPageStore();

  /** Pages that have been written locally but not yet flushed. */
  private dirtyPages = new Set<string>();
  /** Metadata entries that have been written locally but not yet flushed. */
  private dirtyMeta = new Set<string>();
  /** Files whose pages have been deleted locally (need backend deleteFile). */
  private deletedFiles = new Set<string>();
  /** Files with pages truncated from a given index (need backend deletePagesFrom). */
  private truncations = new Map<string, number>();
  /** Metadata entries deleted locally but not yet flushed. */
  private deletedMeta = new Set<string>();

  private initialized = false;
  private initPromise: Promise<void> | null = null;

  constructor(remote: StorageBackend) {
    this.remote = remote;
  }

  /**
   * Load all metadata and pages from the remote backend into memory.
   * Must be called (and awaited) before using any sync methods.
   *
   * Idempotent: concurrent or repeated calls return the same promise.
   */
  async init(): Promise<void> {
    if (this.initialized) return;
    if (this.initPromise) return this.initPromise;

    this.initPromise = this.doInit().catch((err) => {
      // Clear the cached promise so a subsequent init() call can retry
      // instead of returning the same rejected promise forever.
      this.initPromise = null;
      throw err;
    });
    return this.initPromise;
  }

  private async doInit(): Promise<void> {
    this.store.clear();
    this.dirtyPages.clear();
    this.dirtyMeta.clear();
    this.deletedFiles.clear();
    this.truncations.clear();
    this.deletedMeta.clear();

    const files = await this.remote.listFiles();

    const [allMeta, allMaxIdx] = await Promise.all([
      this.remote.readMetas(files),
      this.remote.maxPageIndexBatch(files),
    ]);

    for (let i = 0; i < files.length; i++) {
      if (allMeta[i]) {
        this.store.meta.set(files[i], allMeta[i]!);
      }
    }

    await Promise.all(
      files.map((path, i) => this.loadFilePages(path, allMaxIdx[i])),
    );

    this.initialized = true;
  }

  private async loadFilePages(
    path: string,
    maxPageIdx: number,
  ): Promise<void> {
    if (maxPageIdx < 0) return;

    const totalPages = maxPageIdx + 1;
    const indices = Array.from({ length: totalPages }, (_, i) => i);
    const pages = await this.remote.readPages(path, indices);
    for (let i = 0; i < pages.length; i++) {
      if (pages[i]) {
        const key = pageKeyStr(path, i);
        this.store.pages.set(key, new Uint8Array(pages[i]!));
        this.store.trackPage(path, key, i);
      }
    }
  }

  private assertInitialized(): void {
    if (!this.initialized) {
      throw new Error("PreloadBackend.init() must be called before use");
    }
  }

  // --- SyncStorageBackend implementation ---

  readPage(path: string, pageIndex: number): Uint8Array | null {
    this.assertInitialized();
    return this.store.readPage(path, pageIndex);
  }

  readPages(path: string, pageIndices: number[]): Array<Uint8Array | null> {
    this.assertInitialized();
    return this.store.readPages(path, pageIndices);
  }

  readPageBatch(
    entries: Array<{ path: string; pageIndex: number }>,
  ): Array<Uint8Array | null> {
    this.assertInitialized();
    return this.store.readPageBatch(entries);
  }

  writePage(path: string, pageIndex: number, data: Uint8Array): void {
    this.assertInitialized();
    this.store.writePage(path, pageIndex, data);
    this.dirtyPages.add(pageKeyStr(path, pageIndex));
  }

  writePages(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
  ): void {
    this.assertInitialized();
    this.store.writePages(pages);
    for (const { path, pageIndex } of pages) {
      this.dirtyPages.add(pageKeyStr(path, pageIndex));
    }
  }

  deleteFile(path: string): void {
    this.assertInitialized();
    const keys = this.store.filePageKeys.get(path);
    if (keys) {
      for (const key of keys) {
        this.dirtyPages.delete(key);
      }
    }
    this.store.deleteFile(path);
    this.deletedFiles.add(path);
    this.truncations.delete(path);
  }

  countPages(path: string): number {
    this.assertInitialized();
    return this.store.countPages(path);
  }

  countPagesBatch(paths: string[]): number[] {
    this.assertInitialized();
    return this.store.countPagesBatch(paths);
  }

  maxPageIndex(path: string): number {
    this.assertInitialized();
    return this.store.maxPageIndex(path);
  }

  maxPageIndexBatch(paths: string[]): number[] {
    this.assertInitialized();
    return this.store.maxPageIndexBatch(paths);
  }

  deleteFiles(paths: string[]): void {
    this.assertInitialized();
    for (const path of paths) {
      this.deleteFile(path);
    }
  }

  renameFile(oldPath: string, newPath: string): void {
    this.assertInitialized();
    if (oldPath === newPath) return;

    // Clean destination dirty tracking before the store removes dest pages
    const destKeys = this.store.filePageKeys.get(newPath);
    if (destKeys) {
      for (const key of destKeys) {
        this.dirtyPages.delete(key);
      }
      this.deletedFiles.add(newPath);
      this.truncations.delete(newPath);
    }

    // Clean source dirty tracking before the store moves pages
    const oldIndices = this.store.filePageIndices.get(oldPath);
    if (oldIndices) {
      for (const [, key] of oldIndices) {
        this.dirtyPages.delete(key);
      }
    }

    this.store.renameFile(oldPath, newPath);

    // Mark all new-path keys as dirty
    const newIndices = this.store.filePageIndices.get(newPath);
    if (newIndices) {
      for (const [, key] of newIndices) {
        this.dirtyPages.add(key);
      }
    }

    this.deletedFiles.add(oldPath);
    this.truncations.delete(oldPath);
  }

  deletePagesFrom(path: string, fromPageIndex: number): void {
    this.assertInitialized();
    // Clean dirty tracking for pages being deleted
    const indices = this.store.filePageIndices.get(path);
    if (indices) {
      for (const [idx, key] of indices) {
        if (idx >= fromPageIndex) {
          this.dirtyPages.delete(key);
        }
      }
    }
    this.store.deletePagesFrom(path, fromPageIndex);
    const existing = this.truncations.get(path);
    if (existing === undefined || fromPageIndex < existing) {
      this.truncations.set(path, fromPageIndex);
    }
  }

  readMeta(path: string): FileMeta | null {
    this.assertInitialized();
    return this.store.readMeta(path);
  }

  readMetas(paths: string[]): Array<FileMeta | null> {
    this.assertInitialized();
    return this.store.readMetas(paths);
  }

  writeMeta(path: string, meta: FileMeta): void {
    this.assertInitialized();
    this.store.writeMeta(path, meta);
    this.dirtyMeta.add(path);
    this.deletedMeta.delete(path);
  }

  writeMetas(entries: Array<{ path: string; meta: FileMeta }>): void {
    this.assertInitialized();
    this.store.writeMetas(entries);
    for (const { path } of entries) {
      this.dirtyMeta.add(path);
      this.deletedMeta.delete(path);
    }
  }

  deleteMeta(path: string): void {
    this.assertInitialized();
    this.store.deleteMeta(path);
    this.dirtyMeta.delete(path);
    this.deletedMeta.add(path);
  }

  deleteMetas(paths: string[]): void {
    this.assertInitialized();
    this.store.deleteMetas(paths);
    for (const path of paths) {
      this.dirtyMeta.delete(path);
      this.deletedMeta.add(path);
    }
  }

  listFiles(): string[] {
    this.assertInitialized();
    return this.store.listFiles();
  }

  syncAll(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
    metas: Array<{ path: string; meta: FileMeta }>,
  ): void {
    this.writePages(pages);
    this.writeMetas(metas);
  }

  deleteAll(paths: string[]): void {
    this.deleteMetas(paths);
    this.deleteFiles(paths);
  }

  cleanupOrphanedPages(): number {
    const pagePaths = [...this.store.filePageKeys.keys()];
    let removed = 0;
    for (const path of pagePaths) {
      if (!this.store.meta.has(path)) {
        this.deleteFile(path);
        removed++;
      }
    }
    return removed;
  }

  // --- Flush: persist dirty state back to the async backend ---

  /** Number of dirty pages pending flush. */
  get dirtyPageCount(): number {
    return this.dirtyPages.size;
  }

  /** Number of dirty metadata entries pending flush. */
  get dirtyMetaCount(): number {
    return this.dirtyMeta.size;
  }

  /** Whether there are any pending changes that need flushing. */
  get isDirty(): boolean {
    return (
      this.dirtyPages.size > 0 ||
      this.dirtyMeta.size > 0 ||
      this.deletedFiles.size > 0 ||
      this.truncations.size > 0 ||
      this.deletedMeta.size > 0
    );
  }

  /**
   * Flush all dirty state to the remote backend.
   *
   * Ordering is designed for crash safety: new data is written before
   * old data is deleted. This way, a crash mid-flush leaves duplicate
   * data (cleaned up on next syncfs) rather than losing data.
   *
   * For delete-then-recreate at the same path, we must delete first
   * to avoid the subsequent deleteFile removing newly written pages.
   * These are handled in a separate pass after deletions.
   *
   * Pages and metadata within each phase are written atomically via
   * syncAll(). For IDB backends, this means a single multi-store
   * transaction — the same atomicity guarantee as the SAB bridge path.
   * This eliminates the crash window between separate writePages and
   * writeMetas calls that could leave pages without metadata (or vice
   * versa).
   *
   * Safe to call multiple times — no-op if nothing is dirty.
   */
  async flush(): Promise<void> {
    this.assertInitialized();

    if (
      this.deletedFiles.size === 0 &&
      this.deletedMeta.size === 0 &&
      this.truncations.size === 0
    ) {
      if (this.dirtyPages.size === 0 && this.dirtyMeta.size === 0) return;

      const flushedPageKeys = this.dirtyPages;
      const flushedMetaPaths = this.dirtyMeta;
      this.dirtyPages = new Set();
      this.dirtyMeta = new Set();

      const pageBatch: Array<{
        path: string;
        pageIndex: number;
        data: Uint8Array;
      }> = [];
      for (const key of flushedPageKeys) {
        const data = this.store.pages.get(key);
        if (data) {
          const nullIdx = key.indexOf("\0");
          const path = key.substring(0, nullIdx);
          const pageIndex = parseInt(key.substring(nullIdx + 1), 10);
          pageBatch.push({ path, pageIndex, data });
        }
      }

      const metaBatch: Array<{ path: string; meta: FileMeta }> = [];
      for (const path of flushedMetaPaths) {
        const m = this.store.meta.get(path);
        if (m) metaBatch.push({ path, meta: m });
      }

      if (pageBatch.length > 0 || metaBatch.length > 0) {
        try {
          await this.remote.syncAll(pageBatch, metaBatch);
        } catch (e) {
          for (const key of flushedPageKeys) this.dirtyPages.add(key);
          for (const path of flushedMetaPaths) this.dirtyMeta.add(path);
          throw e;
        }
      }
      return;
    }

    const flushedPageKeys = this.dirtyPages;
    const flushedMetaPaths = this.dirtyMeta;
    this.dirtyPages = new Set();
    this.dirtyMeta = new Set();
    const flushedTruncations = new Map(this.truncations);
    const flushedDeletedFiles = new Set(this.deletedFiles);
    const flushedDeletedMeta = new Set(this.deletedMeta);

    const earlyBatch: Array<{
      path: string;
      pageIndex: number;
      data: Uint8Array;
    }> = [];
    const lateBatch: Array<{
      path: string;
      pageIndex: number;
      data: Uint8Array;
    }> = [];
    for (const key of flushedPageKeys) {
      const data = this.store.pages.get(key);
      if (data) {
        const nullIdx = key.indexOf("\0");
        const path = key.substring(0, nullIdx);
        const pageIndex = parseInt(key.substring(nullIdx + 1), 10);
        const entry = { path, pageIndex, data };
        if (flushedDeletedFiles.has(path)) {
          lateBatch.push(entry);
        } else {
          earlyBatch.push(entry);
        }
      }
    }

    const earlyMeta: Array<{ path: string; meta: FileMeta }> = [];
    const lateMeta: Array<{ path: string; meta: FileMeta }> = [];
    for (const path of flushedMetaPaths) {
      const m = this.store.meta.get(path);
      if (m) {
        if (flushedDeletedMeta.has(path) || flushedDeletedFiles.has(path)) {
          lateMeta.push({ path, meta: m });
        } else {
          earlyMeta.push({ path, meta: m });
        }
      }
    }

    try {
      if (flushedTruncations.size > 0) {
        await Promise.all(
          [...flushedTruncations].map(([path, fromIndex]) =>
            this.remote.deletePagesFrom(path, fromIndex),
          ),
        );
      }
      for (const [path, fromIndex] of flushedTruncations) {
        if (this.truncations.get(path) === fromIndex) {
          this.truncations.delete(path);
        }
      }

      if (earlyBatch.length > 0 || earlyMeta.length > 0) {
        await this.remote.syncAll(earlyBatch, earlyMeta);
      }

      if (flushedDeletedFiles.size > 0) {
        await this.remote.deleteFiles([...flushedDeletedFiles]);
      }
      for (const path of flushedDeletedFiles) {
        this.deletedFiles.delete(path);
      }

      if (flushedDeletedMeta.size > 0) {
        await this.remote.deleteMetas([...flushedDeletedMeta]);
      }
      for (const path of flushedDeletedMeta) {
        this.deletedMeta.delete(path);
      }

      if (lateBatch.length > 0 || lateMeta.length > 0) {
        await this.remote.syncAll(lateBatch, lateMeta);
      }
    } catch (e) {
      for (const key of flushedPageKeys) this.dirtyPages.add(key);
      for (const path of flushedMetaPaths) this.dirtyMeta.add(path);
      throw e;
    }
  }

  assertInvariants(): void {
    const errors = this.store.assertInvariants();

    for (const key of this.dirtyPages) {
      if (!this.store.pages.has(key)) {
        errors.push(`dirtyPages contains ${key} not in pages`);
      }
    }

    for (const path of this.dirtyMeta) {
      if (!this.store.meta.has(path)) {
        errors.push(`dirtyMeta contains ${path} not in meta`);
      }
    }

    for (const path of this.deletedFiles) {
      if (this.store.filePageKeys.has(path)) {
        const keys = this.store.filePageKeys.get(path)!;
        for (const key of keys) {
          if (!this.dirtyPages.has(key)) {
            errors.push(
              `deletedFiles contains ${path} which has non-dirty page ${key}`,
            );
          }
        }
      }
    }

    for (const path of this.deletedMeta) {
      if (this.store.meta.has(path)) {
        errors.push(`deletedMeta contains ${path} which still exists in meta`);
      }
    }

    for (const [path, fromIndex] of this.truncations) {
      const indices = this.store.filePageIndices.get(path);
      if (indices) {
        for (const [idx, key] of indices) {
          if (idx >= fromIndex && !this.dirtyPages.has(key)) {
            errors.push(
              `truncations[${path}] = ${fromIndex} but non-dirty page ${idx} still exists`,
            );
          }
        }
      }
    }

    if (errors.length > 0) {
      throw new Error(
        `PreloadBackend invariant violations (${errors.length}):\n  - ${errors.join("\n  - ")}`,
      );
    }
  }
}
