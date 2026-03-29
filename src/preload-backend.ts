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
import { PAGE_SIZE, MAX_PROBE_PAGE, pageKeyStr } from "./types.js";

export class PreloadBackend implements SyncStorageBackend {
  private readonly remote: StorageBackend;
  private pages = new Map<string, Uint8Array>();
  private meta = new Map<string, FileMeta>();

  /** Secondary index: file path → set of page keys belonging to that file.
   *  Avoids O(total-pages) full-map scans in deleteFile, renameFile, deletePagesFrom. */
  private filePageKeys = new Map<string, Set<string>>();

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
    const files = await this.remote.listFiles();

    // Batch-read all metadata in a single call to reduce round-trips
    const allMeta = await this.remote.readMetas(files);
    for (let i = 0; i < files.length; i++) {
      if (allMeta[i]) {
        this.meta.set(files[i], allMeta[i]!);
      }
    }

    // Load all pages for every file in batches
    for (const path of files) {
      const m = this.meta.get(path);
      if (!m) continue;

      const pageCount = m.size > 0 ? Math.ceil(m.size / PAGE_SIZE) : 0;

      // Load pages accounted for by metadata
      if (pageCount > 0) {
        const indices = Array.from({ length: pageCount }, (_, i) => i);
        const pages = await this.remote.readPages(path, indices);
        for (let i = 0; i < pages.length; i++) {
          if (pages[i]) {
            const key = pageKeyStr(path, i);
            this.pages.set(key, new Uint8Array(pages[i]!));
            this.trackPage(path, key);
          }
        }
      }

      // Probe for pages beyond meta.size that may exist from a prior crash
      // (pages written through the page cache but metadata not yet synced).
      // Uses exponential probe + binary search (O(log n) reads) to find the
      // true extent, then loads any discovered extra pages.
      const nextPage = await this.remote.readPage(path, pageCount);
      if (nextPage) {
        const probeKey = pageKeyStr(path, pageCount);
        this.pages.set(probeKey, new Uint8Array(nextPage));
        this.trackPage(path, probeKey);

        // Exponential probe to find upper bound
        let lo = pageCount;
        let hi = pageCount + 1;
        while (await this.remote.readPage(path, hi)) {
          lo = hi;
          hi = Math.min(hi * 2, MAX_PROBE_PAGE);
          if (hi === lo) break; // hit cap — lo is the last known page
        }

        // Binary search between lo (exists) and hi (missing)
        while (hi - lo > 1) {
          const mid = (lo + hi) >>> 1;
          if (await this.remote.readPage(path, mid)) {
            lo = mid;
          } else {
            hi = mid;
          }
        }

        // Load all extra pages in a batch (pageCount+1 through lo inclusive)
        const extraStart = pageCount + 1;
        if (lo >= extraStart) {
          const extraIndices = Array.from(
            { length: lo - extraStart + 1 },
            (_, i) => extraStart + i,
          );
          const extraPages = await this.remote.readPages(path, extraIndices);
          for (let i = 0; i < extraPages.length; i++) {
            if (extraPages[i]) {
              const extraKey = pageKeyStr(path, extraIndices[i]);
              this.pages.set(extraKey, new Uint8Array(extraPages[i]!));
              this.trackPage(path, extraKey);
            }
          }
        }
      }
    }

    this.initialized = true;
  }

  /** Add a page key to the filePageKeys index. */
  private trackPage(path: string, key: string): void {
    let keys = this.filePageKeys.get(path);
    if (!keys) {
      keys = new Set();
      this.filePageKeys.set(path, keys);
    }
    keys.add(key);
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

  readPages(path: string, pageIndices: number[]): Array<Uint8Array | null> {
    this.assertInitialized();
    return pageIndices.map((i) => {
      const data = this.pages.get(pageKeyStr(path, i));
      return data ? new Uint8Array(data) : null;
    });
  }

  writePage(path: string, pageIndex: number, data: Uint8Array): void {
    this.assertInitialized();
    const key = pageKeyStr(path, pageIndex);
    this.pages.set(key, new Uint8Array(data));
    this.trackPage(path, key);
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
    const keys = this.filePageKeys.get(path);
    if (keys) {
      for (const key of keys) {
        this.pages.delete(key);
        this.dirtyPages.delete(key);
      }
      this.filePageKeys.delete(path);
    }
    this.deletedFiles.add(path);
    // Clear any pending truncation for this file
    this.truncations.delete(path);
  }

  countPages(path: string): number {
    this.assertInitialized();
    const keys = this.filePageKeys.get(path);
    return keys ? keys.size : 0;
  }

  maxPageIndex(path: string): number {
    this.assertInitialized();
    const keys = this.filePageKeys.get(path);
    if (!keys || keys.size === 0) return -1;
    let max = -1;
    for (const key of keys) {
      const nullIdx = key.indexOf("\0");
      const idx = parseInt(key.substring(nullIdx + 1), 10);
      if (idx > max) max = idx;
    }
    return max;
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
    const oldKeys = this.filePageKeys.get(oldPath);
    if (!oldKeys) {
      // No pages to move — still track the deletion for flush
      this.deletedFiles.add(oldPath);
      this.truncations.delete(oldPath);
      return;
    }

    // Clear any pre-existing destination pages to prevent orphans when the
    // destination has more pages than the source (matches IDB/OPFS behavior).
    const destKeys = this.filePageKeys.get(newPath);
    if (destKeys) {
      for (const key of destKeys) {
        this.pages.delete(key);
        this.dirtyPages.delete(key);
      }
      this.filePageKeys.delete(newPath);
      this.deletedFiles.add(newPath);
      this.truncations.delete(newPath);
    }

    const oldPrefix = `${oldPath}\0`;
    const toAdd: Array<[string, Uint8Array]> = [];
    for (const key of oldKeys) {
      const data = this.pages.get(key)!;
      const pageIndex = key.slice(oldPrefix.length);
      const newKey = `${newPath}\0${pageIndex}`;
      toAdd.push([newKey, data]);
      // Transfer dirty tracking to new key
      if (this.dirtyPages.has(key)) {
        this.dirtyPages.delete(key);
      }
      this.pages.delete(key);
    }
    this.filePageKeys.delete(oldPath);

    for (const [key, data] of toAdd) {
      this.pages.set(key, data);
      this.trackPage(newPath, key);
      this.dirtyPages.add(key);
    }
    // Track as: delete old file + dirty-write all new pages
    this.deletedFiles.add(oldPath);
    this.truncations.delete(oldPath);
  }

  deletePagesFrom(path: string, fromPageIndex: number): void {
    this.assertInitialized();
    const keys = this.filePageKeys.get(path);
    if (keys) {
      const prefix = `${path}\0`;
      for (const key of keys) {
        const idx = parseInt(key.slice(prefix.length), 10);
        if (idx >= fromPageIndex) {
          this.pages.delete(key);
          this.dirtyPages.delete(key);
          keys.delete(key);
        }
      }
      if (keys.size === 0) this.filePageKeys.delete(path);
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

  readMetas(paths: string[]): Array<FileMeta | null> {
    this.assertInitialized();
    return paths.map((path) => {
      const m = this.meta.get(path);
      return m ? { ...m } : null;
    });
  }

  writeMeta(path: string, meta: FileMeta): void {
    this.assertInitialized();
    this.meta.set(path, { ...meta });
    this.dirtyMeta.add(path);
    this.deletedMeta.delete(path);
  }

  writeMetas(entries: Array<{ path: string; meta: FileMeta }>): void {
    this.assertInitialized();
    for (const { path, meta } of entries) {
      this.meta.set(path, { ...meta });
      this.dirtyMeta.add(path);
      this.deletedMeta.delete(path);
    }
  }

  deleteMeta(path: string): void {
    this.assertInitialized();
    this.meta.delete(path);
    this.dirtyMeta.delete(path);
    this.deletedMeta.add(path);
  }

  deleteMetas(paths: string[]): void {
    this.assertInitialized();
    for (const path of paths) {
      this.meta.delete(path);
      this.dirtyMeta.delete(path);
      this.deletedMeta.add(path);
    }
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
   * Ordering is designed for crash safety: new data is written before
   * old data is deleted. This way, a crash mid-flush leaves duplicate
   * data (cleaned up on next syncfs) rather than losing data.
   *
   * For delete-then-recreate at the same path, we must delete first
   * to avoid the subsequent deleteFile removing newly written pages.
   * These are handled in a separate pass after deletions.
   *
   * Operations within each step are batched to minimize round-trips
   * to the remote backend (important when the remote is IDB behind
   * a SAB bridge, where each call is a cross-worker round-trip).
   *
   * Safe to call multiple times — no-op if nothing is dirty.
   */
  async flush(): Promise<void> {
    this.assertInitialized();

    // Partition dirty pages: pages at paths pending deletion must be
    // written AFTER the delete (to handle delete-then-recreate at the
    // same path). All other dirty pages are written first for crash safety.
    // Snapshot the dirty keys we intend to flush. We must NOT clear
    // dirtyPages/dirtyMeta until all operations succeed — otherwise a
    // mid-flush failure loses dirty tracking and the data is never retried.
    const flushedPageKeys = new Set(this.dirtyPages);
    const flushedMetaPaths = new Set(this.dirtyMeta);

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
      const data = this.pages.get(key);
      if (data) {
        const nullIdx = key.indexOf("\0");
        const path = key.substring(0, nullIdx);
        const pageIndex = parseInt(key.substring(nullIdx + 1), 10);
        const entry = { path, pageIndex, data };
        if (this.deletedFiles.has(path)) {
          lateBatch.push(entry);
        } else {
          earlyBatch.push(entry);
        }
      }
    }

    // Partition dirty metadata the same way as pages.
    const earlyMeta: Array<{ path: string; meta: FileMeta }> = [];
    const lateMeta: Array<{ path: string; meta: FileMeta }> = [];
    for (const path of flushedMetaPaths) {
      const m = this.meta.get(path);
      if (m) {
        if (this.deletedMeta.has(path)) {
          lateMeta.push({ path, meta: m });
        } else {
          earlyMeta.push({ path, meta: m });
        }
      }
    }

    // 1. Apply truncations FIRST — before page writes.
    // Truncations delete stale tail pages from the remote. If a file was
    // truncated and then extended (e.g., truncate to 100 bytes then write
    // at offset 8192), the new page at the truncation point is dirty and
    // will be written in step 3. Without this ordering, page writes would
    // go to the remote first, then the truncation would delete them.
    if (this.truncations.size > 0) {
      await Promise.all(
        [...this.truncations].map(([path, fromIndex]) =>
          this.remote.deletePagesFrom(path, fromIndex),
        ),
      );
    }
    this.truncations.clear();

    // 2. Write new pages at non-deleted paths (after truncation cleanup)
    if (earlyBatch.length > 0) {
      await this.remote.writePages(earlyBatch);
    }

    // 3. Batch-write dirty metadata for non-deleted paths
    if (earlyMeta.length > 0) {
      await this.remote.writeMetas(earlyMeta);
    }

    // 4. Batch-delete files from remote (single call instead of O(n))
    if (this.deletedFiles.size > 0) {
      await this.remote.deleteFiles([...this.deletedFiles]);
    }
    this.deletedFiles.clear();

    // 5. Batch-delete metadata
    if (this.deletedMeta.size > 0) {
      await this.remote.deleteMetas([...this.deletedMeta]);
    }
    this.deletedMeta.clear();

    // 6. Write pages for delete-then-recreate paths
    if (lateBatch.length > 0) {
      await this.remote.writePages(lateBatch);
    }

    // 7. Batch-write metadata for delete-then-recreate paths
    if (lateMeta.length > 0) {
      await this.remote.writeMetas(lateMeta);
    }

    // 8. All operations succeeded — clear dirty tracking for flushed entries.
    // We clear individual entries (not the whole set) so that writes that
    // occurred during flush are preserved for the next flush cycle.
    for (const key of flushedPageKeys) {
      this.dirtyPages.delete(key);
    }
    for (const path of flushedMetaPaths) {
      this.dirtyMeta.delete(path);
    }
  }
}
