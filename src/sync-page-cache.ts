import type { SyncStorageBackend } from "./sync-storage-backend.js";
import { PAGE_SIZE, DEFAULT_MAX_PAGES, pageKeyStr } from "./types.js";
import type { CachedPage } from "./types.js";

/**
 * Synchronous LRU page cache with dirty tracking.
 *
 * Same algorithm as PageCache but fully synchronous — required because
 * Emscripten FS operations (read, write, llseek, etc.) are synchronous
 * C-style callbacks that cannot await promises.
 *
 * Pages are loaded on demand from a SyncStorageBackend and cached in memory.
 * When the cache is full, the least-recently-used page is evicted. If the
 * evicted page is dirty, it is flushed to the backend before eviction.
 *
 * Maintains secondary indexes for efficient file-scoped operations:
 * - filePages: maps file path → set of cache keys for that file
 * - dirtyKeys: set of all dirty cache keys
 *
 * This avoids O(cache-size) scans in flushFile, evictFile, deleteFile,
 * invalidatePagesFrom, flushAll, and dirtyCount — all of which are called
 * frequently during PGlite workloads (e.g., close → flushFile on every fd).
 */
export class SyncPageCache {
  private readonly maxPages: number;
  private readonly backend: SyncStorageBackend;

  /** Cache entries keyed by pageKeyStr. Insertion order = LRU order (oldest first). */
  private cache = new Map<string, CachedPage>();

  /** Key of the most-recently-used page (last entry in Map). Skip LRU touch if hit. */
  private mruKey: string | null = null;

  /** Secondary index: file path → set of cache keys belonging to that file. */
  private filePages = new Map<string, Set<string>>();

  /** Secondary index: set of cache keys for dirty pages. */
  private dirtyKeys = new Set<string>();

  constructor(
    backend: SyncStorageBackend,
    maxPages: number = DEFAULT_MAX_PAGES,
  ) {
    if (maxPages < 1) {
      throw new Error("maxPages must be at least 1");
    }
    this.backend = backend;
    this.maxPages = maxPages;
  }

  /** Number of pages currently in the cache. */
  get size(): number {
    return this.cache.size;
  }

  /** Maximum number of pages the cache can hold. */
  get capacity(): number {
    return this.maxPages;
  }

  /**
   * Get a page from cache or backend.
   *
   * If the page is not in cache, it is loaded from the backend (or created
   * as a zero-filled page if it doesn't exist in the backend either).
   * The page is marked as most-recently-used.
   */
  getPage(path: string, pageIndex: number): CachedPage {
    const key = pageKeyStr(path, pageIndex);
    // Fast path: if this is already the MRU page, skip Map reordering
    if (key === this.mruKey) {
      return this.cache.get(key)!;
    }

    const existing = this.cache.get(key);
    if (existing) {
      // Move to end (most recently used)
      this.cache.delete(key);
      this.cache.set(key, existing);
      this.mruKey = key;
      return existing;
    }

    // Cache miss — load from backend
    const data = this.backend.readPage(path, pageIndex);
    const page: CachedPage = {
      path,
      pageIndex,
      data: data ? new Uint8Array(data) : new Uint8Array(PAGE_SIZE),
      dirty: false,
    };

    this.ensureCapacity();
    this.cache.set(key, page);
    this.mruKey = key;
    this.trackPage(path, key);
    return page;
  }

  /**
   * Read bytes from a file at a given position.
   *
   * Handles reads that span multiple pages. Returns the number of bytes
   * actually read (may be less than length if position + length > fileSize).
   *
   * When a read spans multiple pages, cache misses are batched into a single
   * backend.readPages() call to reduce SAB bridge round-trips.
   */
  read(
    path: string,
    buffer: Uint8Array,
    offset: number,
    length: number,
    position: number,
    fileSize: number,
  ): number {
    const available = Math.max(0, fileSize - position);
    const toRead = Math.min(length, available);
    if (toRead === 0) return 0;

    // Determine which pages this read spans
    const firstPage = Math.floor(position / PAGE_SIZE);
    const lastPage = Math.floor((position + toRead - 1) / PAGE_SIZE);

    // Find cache misses — only batch when there are multiple misses
    if (lastPage > firstPage) {
      const missingIndices: number[] = [];
      for (let p = firstPage; p <= lastPage; p++) {
        if (!this.cache.has(pageKeyStr(path, p))) {
          missingIndices.push(p);
        }
      }

      if (missingIndices.length > 1) {
        const results = this.backend.readPages(path, missingIndices);
        for (let i = 0; i < missingIndices.length; i++) {
          const pageIndex = missingIndices[i];
          const key = pageKeyStr(path, pageIndex);
          if (this.cache.has(key)) continue; // may appear via eviction cascade
          const page: CachedPage = {
            path,
            pageIndex,
            data: results[i]
              ? new Uint8Array(results[i]!)
              : new Uint8Array(PAGE_SIZE),
            dirty: false,
          };
          this.ensureCapacity();
          this.cache.set(key, page);
          this.mruKey = key;
          this.trackPage(path, key);
        }
      }
    }

    // Read from cache (all multi-miss pages are pre-loaded; single misses use getPage)
    let bytesRead = 0;
    let pos = position;

    while (bytesRead < toRead) {
      const pageIndex = Math.floor(pos / PAGE_SIZE);
      const pageOffset = pos % PAGE_SIZE;
      const bytesInPage = Math.min(
        PAGE_SIZE - pageOffset,
        toRead - bytesRead,
      );

      const page = this.getPage(path, pageIndex);
      buffer.set(
        page.data.subarray(pageOffset, pageOffset + bytesInPage),
        offset + bytesRead,
      );

      bytesRead += bytesInPage;
      pos += bytesInPage;
    }

    return bytesRead;
  }

  /**
   * Write bytes to a file at a given position.
   *
   * Handles writes that span multiple pages. Pages are loaded (or created)
   * as needed and marked dirty. Returns the number of bytes written and the
   * new file size.
   */
  write(
    path: string,
    buffer: Uint8Array,
    offset: number,
    length: number,
    position: number,
    currentFileSize: number,
  ): { bytesWritten: number; newFileSize: number } {
    if (length === 0)
      return { bytesWritten: 0, newFileSize: currentFileSize };

    let bytesWritten = 0;
    let pos = position;

    while (bytesWritten < length) {
      const pageIndex = Math.floor(pos / PAGE_SIZE);
      const pageOffset = pos % PAGE_SIZE;
      const bytesInPage = Math.min(
        PAGE_SIZE - pageOffset,
        length - bytesWritten,
      );

      const page = this.getPage(path, pageIndex);
      page.data.set(
        buffer.subarray(
          offset + bytesWritten,
          offset + bytesWritten + bytesInPage,
        ),
        pageOffset,
      );
      if (!page.dirty) {
        page.dirty = true;
        this.dirtyKeys.add(pageKeyStr(path, pageIndex));
      }

      bytesWritten += bytesInPage;
      pos += bytesInPage;
    }

    const newFileSize = Math.max(currentFileSize, position + length);
    return { bytesWritten, newFileSize };
  }

  /**
   * Flush all dirty pages for a specific file to the backend.
   * O(pages-for-file) via filePages index instead of O(cache-size).
   */
  flushFile(path: string): number {
    const keys = this.filePages.get(path);
    if (!keys || keys.size === 0) return 0;

    const dirtyPages: Array<{
      path: string;
      pageIndex: number;
      data: Uint8Array;
    }> = [];

    for (const key of keys) {
      if (this.dirtyKeys.has(key)) {
        const page = this.cache.get(key)!;
        dirtyPages.push({
          path: page.path,
          pageIndex: page.pageIndex,
          data: page.data,
        });
      }
    }

    if (dirtyPages.length > 0) {
      this.backend.writePages(dirtyPages);
      for (const key of keys) {
        if (this.dirtyKeys.has(key)) {
          this.cache.get(key)!.dirty = false;
          this.dirtyKeys.delete(key);
        }
      }
    }

    return dirtyPages.length;
  }

  /**
   * Flush all dirty pages across all files to the backend.
   * O(dirty-count) via dirtyKeys index instead of O(cache-size).
   */
  flushAll(): number {
    if (this.dirtyKeys.size === 0) return 0;

    const dirtyPages: Array<{
      path: string;
      pageIndex: number;
      data: Uint8Array;
    }> = [];

    for (const key of this.dirtyKeys) {
      const page = this.cache.get(key)!;
      dirtyPages.push({
        path: page.path,
        pageIndex: page.pageIndex,
        data: page.data,
      });
    }

    this.backend.writePages(dirtyPages);
    for (const key of this.dirtyKeys) {
      this.cache.get(key)!.dirty = false;
    }
    this.dirtyKeys.clear();

    return dirtyPages.length;
  }

  /**
   * Evict all cached pages for a file. Dirty pages are flushed first.
   * O(pages-for-file) via filePages index.
   */
  evictFile(path: string): void {
    this.flushFile(path);
    const keys = this.filePages.get(path);
    if (!keys) return;
    for (const key of keys) {
      this.cache.delete(key);
      if (key === this.mruKey) this.mruKey = null;
    }
    this.filePages.delete(path);
  }

  /**
   * Invalidate cached pages for a file beyond a given page index.
   * Used after truncation to discard stale pages. Does NOT flush — caller
   * should handle backend deletion separately.
   * O(pages-for-file) via filePages index.
   */
  invalidatePagesFrom(path: string, fromPageIndex: number): void {
    const keys = this.filePages.get(path);
    if (!keys) return;
    for (const key of keys) {
      const page = this.cache.get(key)!;
      if (page.pageIndex >= fromPageIndex) {
        this.cache.delete(key);
        if (key === this.mruKey) this.mruKey = null;
        this.dirtyKeys.delete(key);
        keys.delete(key);
      }
    }
    if (keys.size === 0) {
      this.filePages.delete(path);
    }
  }

  /**
   * Zero-fill the tail of the last page after truncation to a smaller size.
   * If the page is cached, modifies it in place. Does not load from backend.
   */
  zeroTailAfterTruncate(path: string, newSize: number): void {
    const lastPageIndex = Math.floor(newSize / PAGE_SIZE);
    const tailOffset = newSize % PAGE_SIZE;
    if (tailOffset === 0) return;

    const key = pageKeyStr(path, lastPageIndex);
    const page = this.cache.get(key);
    if (page) {
      page.data.fill(0, tailOffset);
      if (!page.dirty) {
        page.dirty = true;
        this.dirtyKeys.add(key);
      }
    }
  }

  /**
   * Delete all pages for a file from both cache and backend.
   * O(pages-for-file) via filePages index.
   */
  deleteFile(path: string): void {
    // Remove from cache (no flush — file is being deleted)
    const keys = this.filePages.get(path);
    if (keys) {
      for (const key of keys) {
        this.cache.delete(key);
        if (key === this.mruKey) this.mruKey = null;
        this.dirtyKeys.delete(key);
      }
      this.filePages.delete(path);
    }
    this.backend.deleteFile(path);
  }

  /**
   * Rename a file's pages: flush, move in backend, update cache keys.
   */
  renameFile(oldPath: string, newPath: string): void {
    // Flush dirty pages for old path so backend has the latest data
    this.flushFile(oldPath);

    // Collect all cached pages for old path via filePages index
    const oldKeys = this.filePages.get(oldPath);
    const toMove: CachedPage[] = [];
    if (oldKeys) {
      for (const key of oldKeys) {
        toMove.push(this.cache.get(key)!);
        this.cache.delete(key);
        if (key === this.mruKey) this.mruKey = null;
        this.dirtyKeys.delete(key);
      }
      this.filePages.delete(oldPath);
    }

    // Build the complete set of pages to write under the new path.
    // Read from backend first, then overlay any cached pages (which may
    // be newer than what was just flushed if flush was a no-op).
    const newPages: Array<{
      path: string;
      pageIndex: number;
      data: Uint8Array;
    }> = [];

    for (let i = 0; ; i++) {
      const data = this.backend.readPage(oldPath, i);
      if (!data) break;
      newPages.push({ path: newPath, pageIndex: i, data });
    }

    for (const page of toMove) {
      const existing = newPages.findIndex(
        (p) => p.pageIndex === page.pageIndex,
      );
      if (existing >= 0) {
        newPages[existing] = {
          path: newPath,
          pageIndex: page.pageIndex,
          data: page.data,
        };
      } else {
        newPages.push({
          path: newPath,
          pageIndex: page.pageIndex,
          data: page.data,
        });
      }
    }

    // Write new path BEFORE deleting old path. If write fails, old data
    // is still intact in the backend — no data loss on partial failure.
    if (newPages.length > 0) {
      this.backend.writePages(newPages);
    }
    this.backend.deleteFile(oldPath);

    // Re-insert cached pages under new path
    for (const page of toMove) {
      page.path = newPath;
      const newKey = pageKeyStr(newPath, page.pageIndex);
      this.ensureCapacity();
      this.cache.set(newKey, page);
      this.trackPage(newPath, newKey);
    }
  }

  /** Check if a specific page is in the cache. */
  has(path: string, pageIndex: number): boolean {
    return this.cache.has(pageKeyStr(path, pageIndex));
  }

  /** Check if a specific page is dirty. Returns false if not cached. */
  isDirty(path: string, pageIndex: number): boolean {
    return this.dirtyKeys.has(pageKeyStr(path, pageIndex));
  }

  /** Count dirty pages in cache. O(1) via dirtyKeys index. */
  get dirtyCount(): number {
    return this.dirtyKeys.size;
  }

  /**
   * Add a cache key to the filePages index for the given path.
   */
  private trackPage(path: string, key: string): void {
    let keys = this.filePages.get(path);
    if (!keys) {
      keys = new Set();
      this.filePages.set(path, keys);
    }
    keys.add(key);
  }

  /**
   * Ensure there is room for at least one more page.
   * Evicts the least-recently-used page if at capacity.
   */
  private ensureCapacity(): void {
    while (this.cache.size >= this.maxPages) {
      const firstKey = this.cache.keys().next().value!;
      const victim = this.cache.get(firstKey)!;

      if (victim.dirty) {
        // Flush before eviction — if write fails, keep the page in cache
        // to avoid silent data loss.
        this.backend.writePage(victim.path, victim.pageIndex, victim.data);
        victim.dirty = false;
        this.dirtyKeys.delete(firstKey);
      }

      this.cache.delete(firstKey);
      if (firstKey === this.mruKey) this.mruKey = null;

      // Remove from filePages index
      const keys = this.filePages.get(victim.path);
      if (keys) {
        keys.delete(firstKey);
        if (keys.size === 0) {
          this.filePages.delete(victim.path);
        }
      }
    }
  }
}
