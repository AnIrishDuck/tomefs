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

export class PreloadBackend implements SyncStorageBackend {
  private readonly remote: StorageBackend;
  private pages = new Map<string, Uint8Array>();
  private meta = new Map<string, FileMeta>();

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

  constructor(remote: StorageBackend) {
    this.remote = remote;
  }

  /**
   * Load all metadata and pages from the remote backend into memory.
   * Must be called (and awaited) before using any sync methods.
   */
  async init(): Promise<void> {
    const files = await this.remote.listFiles();

    // Load all metadata
    for (const path of files) {
      const m = await this.remote.readMeta(path);
      if (m) {
        this.meta.set(path, m);
      }
    }

    // Load all pages for every file
    for (const path of files) {
      const m = this.meta.get(path);
      if (!m || m.size === 0) continue;

      const pageCount = Math.ceil(m.size / 8192); // PAGE_SIZE
      for (let i = 0; i < pageCount; i++) {
        const data = await this.remote.readPage(path, i);
        if (data) {
          this.pages.set(pageKeyStr(path, i), new Uint8Array(data));
        }
      }
    }

    this.initialized = true;
  }

  private assertInitialized(): void {
    if (!this.initialized) {
      throw new Error("PreloadBackend.init() must be called before use");
    }
  }

  // --- SyncStorageBackend implementation ---

  readPage(path: string, pageIndex: number): Uint8Array | null {
    this.assertInitialized();
    const data = this.pages.get(pageKeyStr(path, pageIndex));
    return data ? new Uint8Array(data) : null;
  }

  writePage(path: string, pageIndex: number, data: Uint8Array): void {
    this.assertInitialized();
    const key = pageKeyStr(path, pageIndex);
    this.pages.set(key, new Uint8Array(data));
    this.dirtyPages.add(key);
  }

  writePages(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
  ): void {
    this.assertInitialized();
    for (const { path, pageIndex, data } of pages) {
      this.writePage(path, pageIndex, data);
    }
  }

  deleteFile(path: string): void {
    this.assertInitialized();
    const prefix = `${path}\0`;
    for (const key of this.pages.keys()) {
      if (key.startsWith(prefix)) {
        this.pages.delete(key);
        this.dirtyPages.delete(key);
      }
    }
    this.deletedFiles.add(path);
    // Clear any pending truncation for this file
    this.truncations.delete(path);
  }

  deletePagesFrom(path: string, fromPageIndex: number): void {
    this.assertInitialized();
    const prefix = `${path}\0`;
    for (const key of this.pages.keys()) {
      if (key.startsWith(prefix)) {
        const idx = parseInt(key.slice(prefix.length), 10);
        if (idx >= fromPageIndex) {
          this.pages.delete(key);
          this.dirtyPages.delete(key);
        }
      }
    }
    // Track the lowest truncation point
    const existing = this.truncations.get(path);
    if (existing === undefined || fromPageIndex < existing) {
      this.truncations.set(path, fromPageIndex);
    }
  }

  readMeta(path: string): FileMeta | null {
    this.assertInitialized();
    const m = this.meta.get(path);
    return m ? { ...m } : null;
  }

  writeMeta(path: string, meta: FileMeta): void {
    this.assertInitialized();
    this.meta.set(path, { ...meta });
    this.dirtyMeta.add(path);
    this.deletedMeta.delete(path);
  }

  deleteMeta(path: string): void {
    this.assertInitialized();
    this.meta.delete(path);
    this.dirtyMeta.delete(path);
    this.deletedMeta.add(path);
  }

  listFiles(): string[] {
    this.assertInitialized();
    return [...this.meta.keys()];
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
   * This writes dirty pages and metadata, applies file deletions and
   * truncations, then clears the dirty tracking state. Safe to call
   * multiple times — no-op if nothing is dirty.
   */
  async flush(): Promise<void> {
    this.assertInitialized();

    // 1. Delete files from remote
    for (const path of this.deletedFiles) {
      await this.remote.deleteFile(path);
    }
    this.deletedFiles.clear();

    // 2. Apply truncations
    for (const [path, fromIndex] of this.truncations) {
      await this.remote.deletePagesFrom(path, fromIndex);
    }
    this.truncations.clear();

    // 3. Delete metadata
    for (const path of this.deletedMeta) {
      await this.remote.deleteMeta(path);
    }
    this.deletedMeta.clear();

    // 4. Write dirty pages in batches
    if (this.dirtyPages.size > 0) {
      const batch: Array<{ path: string; pageIndex: number; data: Uint8Array }> = [];
      for (const key of this.dirtyPages) {
        const data = this.pages.get(key);
        if (data) {
          const nullIdx = key.indexOf("\0");
          const path = key.substring(0, nullIdx);
          const pageIndex = parseInt(key.substring(nullIdx + 1), 10);
          batch.push({ path, pageIndex, data });
        }
      }
      if (batch.length > 0) {
        await this.remote.writePages(batch);
      }
      this.dirtyPages.clear();
    }

    // 5. Write dirty metadata
    for (const path of this.dirtyMeta) {
      const m = this.meta.get(path);
      if (m) {
        await this.remote.writeMeta(path, m);
      }
    }
    this.dirtyMeta.clear();
  }
}
