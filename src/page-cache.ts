import type { StorageBackend } from "./storage-backend.js";
import { PAGE_SIZE, DEFAULT_MAX_PAGES, pageKeyStr } from "./types.js";
import type { CachedPage, CacheStats } from "./types.js";

/**
 * LRU page cache with dirty tracking.
 *
 * Pages are loaded on demand from a StorageBackend and cached in memory.
 * When the cache is full, the least-recently-used page is evicted. If the
 * evicted page is dirty, it is flushed to the backend before eviction.
 *
 * Maintains secondary indexes for efficient file-scoped operations:
 * - filePages: maps file path → set of cache keys for that file
 * - dirtyKeys: set of all dirty cache keys
 *
 * This avoids O(cache-size) scans in flushFile, evictFile, deleteFile,
 * invalidatePagesFrom, flushAll, and dirtyCount.
 */
export class PageCache {
  private readonly maxPages: number;
  private readonly backend: StorageBackend;

  /** Cache entries keyed by pageKeyStr. Insertion order = LRU order (oldest first). */
  private cache = new Map<string, CachedPage>();

  /** Key of the most-recently-used page (last entry in Map). Skip LRU touch if hit. */
  private mruKey: string | null = null;

  /** Secondary index: file path → set of cache keys belonging to that file. */
  private filePages = new Map<string, Set<string>>();

  /** Secondary index: set of cache keys for dirty pages. */
  private dirtyKeys = new Set<string>();

  /** Performance counters. */
  private _hits = 0;
  private _misses = 0;
  private _evictions = 0;
  private _flushes = 0;

  constructor(backend: StorageBackend, maxPages: number = DEFAULT_MAX_PAGES) {
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
   *
   * Returns a reference to the cached page. Callers must not hold references
   * across await points, as the page may be evicted.
   */
  async getPage(path: string, pageIndex: number): Promise<CachedPage> {
    const key = pageKeyStr(path, pageIndex);
    // Fast path: if this is already the MRU page, skip Map reordering
    if (key === this.mruKey) {
      this._hits++;
      return this.cache.get(key)!;
    }

    const existing = this.cache.get(key);
    if (existing) {
      this._hits++;
      // Move to end (most recently used)
      this.cache.delete(key);
      this.cache.set(key, existing);
      this.mruKey = key;
      return existing;
    }

    this._misses++;
    // Cache miss — load from backend
    const data = await this.backend.readPage(path, pageIndex);
    const page: CachedPage = {
      path,
      pageIndex,
      data: data ? new Uint8Array(data) : new Uint8Array(PAGE_SIZE),
      dirty: false,
    };

    await this.ensureCapacity();
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
   * backend.readPages() call to reduce round-trips.
   */
  async read(
    path: string,
    buffer: Uint8Array,
    offset: number,
    length: number,
    position: number,
    fileSize: number,
  ): Promise<number> {
    // Clamp to file size
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
        // Batch-evict space before pre-loading
        await this.batchEvict(missingIndices.length);

        this._misses += missingIndices.length;
        const results = await this.backend.readPages(path, missingIndices);
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
          await this.ensureCapacity();
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
      const bytesInPage = Math.min(PAGE_SIZE - pageOffset, toRead - bytesRead);

      const page = await this.getPage(path, pageIndex);
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
   *
   * When a write spans multiple pages, cache misses are pre-computed and
   * eviction + loading are batched to reduce round-trips.
   */
  async write(
    path: string,
    buffer: Uint8Array,
    offset: number,
    length: number,
    position: number,
    currentFileSize: number,
  ): Promise<{ bytesWritten: number; newFileSize: number }> {
    if (length === 0) return { bytesWritten: 0, newFileSize: currentFileSize };

    // Determine which pages this write spans
    const firstPage = Math.floor(position / PAGE_SIZE);
    const lastPage = Math.floor((position + length - 1) / PAGE_SIZE);

    // For multi-page writes, batch eviction and pre-loading of cache misses
    if (lastPage > firstPage) {
      const missingIndices: number[] = [];
      for (let p = firstPage; p <= lastPage; p++) {
        if (!this.cache.has(pageKeyStr(path, p))) {
          missingIndices.push(p);
        }
      }

      if (missingIndices.length > 0) {
        // Batch-evict space for all missing pages at once
        await this.batchEvict(missingIndices.length);

        // Batch-load all missing pages from backend
        if (missingIndices.length > 1) {
          this._misses += missingIndices.length;
          const results = await this.backend.readPages(path, missingIndices);
          for (let i = 0; i < missingIndices.length; i++) {
            const pageIndex = missingIndices[i];
            const key = pageKeyStr(path, pageIndex);
            if (this.cache.has(key)) continue;
            const page: CachedPage = {
              path,
              pageIndex,
              data: results[i]
                ? new Uint8Array(results[i]!)
                : new Uint8Array(PAGE_SIZE),
              dirty: false,
            };
            await this.ensureCapacity();
            this.cache.set(key, page);
            this.mruKey = key;
            this.trackPage(path, key);
          }
        }
        // Single misses are handled efficiently by getPage below
      }
    }

    // Write data into pages (all multi-miss pages are pre-loaded)
    let bytesWritten = 0;
    let pos = position;

    while (bytesWritten < length) {
      const pageIndex = Math.floor(pos / PAGE_SIZE);
      const pageOffset = pos % PAGE_SIZE;
      const bytesInPage = Math.min(
        PAGE_SIZE - pageOffset,
        length - bytesWritten,
      );

      const page = await this.getPage(path, pageIndex);
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
  async flushFile(path: string): Promise<number> {
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
      await this.backend.writePages(dirtyPages);
      this._flushes += dirtyPages.length;
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
  async flushAll(): Promise<number> {
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

    await this.backend.writePages(dirtyPages);
    this._flushes += dirtyPages.length;
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
  async evictFile(path: string): Promise<void> {
    await this.flushFile(path);
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
   * If the page is cached, modifies it in place. If the page is only in
   * the backend (evicted from cache), loads it, zeros the tail, and writes
   * it back. This prevents stale data from being served if the file is
   * later extended without writing to the truncated region.
   */
  async zeroTailAfterTruncate(path: string, newSize: number): Promise<void> {
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
    } else {
      // Page is not cached — check if it exists in the backend
      const data = await this.backend.readPage(path, lastPageIndex);
      if (data) {
        const updated = new Uint8Array(data);
        updated.fill(0, tailOffset);
        await this.backend.writePage(path, lastPageIndex, updated);
      }
    }
  }

  /**
   * Delete all pages for a file from both cache and backend.
   * O(pages-for-file) via filePages index.
   */
  async deleteFile(path: string): Promise<void> {
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
    await this.backend.deleteFile(path);
  }

  /**
   * Rename a file's pages: flush, move in backend, update cache keys.
   *
   * Flushes dirty pages first so the backend has the latest data,
   * then delegates to the backend's renameFile for an atomic re-key.
   */
  async renameFile(oldPath: string, newPath: string): Promise<void> {
    // Flush dirty pages so backend has the latest data before re-keying
    await this.flushFile(oldPath);

    // Re-key all pages in the backend
    await this.backend.renameFile(oldPath, newPath);

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
    for (const page of toMove) {
      page.path = newPath;
      const newKey = pageKeyStr(newPath, page.pageIndex);
      await this.ensureCapacity();
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

  /** Snapshot of performance counters (hits, misses, evictions, flushes). */
  getStats(): CacheStats {
    return {
      hits: this._hits,
      misses: this._misses,
      evictions: this._evictions,
      flushes: this._flushes,
    };
  }

  /** Reset all performance counters to zero. */
  resetStats(): void {
    this._hits = 0;
    this._misses = 0;
    this._evictions = 0;
    this._flushes = 0;
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
  private async ensureCapacity(): Promise<void> {
    while (this.cache.size >= this.maxPages) {
      await this.evictOne();
    }
  }

  /**
   * Evict the single least-recently-used page.
   * If the page is dirty, it is flushed individually to the backend.
   */
  private async evictOne(): Promise<void> {
    const firstKey = this.cache.keys().next().value!;
    const victim = this.cache.get(firstKey)!;

    if (victim.dirty) {
      await this.backend.writePage(
        victim.path,
        victim.pageIndex,
        victim.data,
      );
      victim.dirty = false;
      this.dirtyKeys.delete(firstKey);
      this._flushes++;
    }

    this._evictions++;
    this.cache.delete(firstKey);
    if (firstKey === this.mruKey) this.mruKey = null;

    const fileKeys = this.filePages.get(victim.path);
    if (fileKeys) {
      fileKeys.delete(firstKey);
      if (fileKeys.size === 0) this.filePages.delete(victim.path);
    }
  }

  /**
   * Batch-evict pages to make room for `count` new entries.
   *
   * Collects all dirty victims and flushes them in a single writePages
   * call, reducing round-trips from O(dirty-evictions) to O(1).
   * Falls back to single-page eviction when only one page needs flushing.
   */
  private async batchEvict(count: number): Promise<void> {
    const needed = Math.min(
      this.cache.size,
      this.cache.size + count - this.maxPages,
    );
    if (needed <= 0) return;

    // For a single eviction, use the simple path (no array allocation)
    if (needed === 1) {
      await this.evictOne();
      return;
    }

    // Collect victims from the LRU end (front of the Map)
    const victimKeys: string[] = [];
    const victims: CachedPage[] = [];
    const iter = this.cache.keys();
    for (let i = 0; i < needed; i++) {
      const key = iter.next().value!;
      victimKeys.push(key);
      victims.push(this.cache.get(key)!);
    }

    // Batch-flush all dirty victims in one backend call
    const dirtyPages: Array<{
      path: string;
      pageIndex: number;
      data: Uint8Array;
    }> = [];
    for (const v of victims) {
      if (v.dirty) {
        dirtyPages.push({ path: v.path, pageIndex: v.pageIndex, data: v.data });
      }
    }
    if (dirtyPages.length === 1) {
      const p = dirtyPages[0];
      await this.backend.writePage(p.path, p.pageIndex, p.data);
    } else if (dirtyPages.length > 1) {
      await this.backend.writePages(dirtyPages);
    }
    this._flushes += dirtyPages.length;

    // Remove victims from cache and indexes
    this._evictions += victimKeys.length;
    for (let i = 0; i < victimKeys.length; i++) {
      const key = victimKeys[i];
      const victim = victims[i];

      this.cache.delete(key);
      if (key === this.mruKey) this.mruKey = null;

      if (victim.dirty) {
        victim.dirty = false;
        this.dirtyKeys.delete(key);
      }

      const fileKeys = this.filePages.get(victim.path);
      if (fileKeys) {
        fileKeys.delete(key);
        if (fileKeys.size === 0) this.filePages.delete(victim.path);
      }
    }
  }
}
