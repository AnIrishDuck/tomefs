import type { SyncStorageBackend } from "./sync-storage-backend.js";
import { PAGE_SIZE, DEFAULT_MAX_PAGES, pageKeyStr } from "./types.js";
import type { CachedPage, CacheStats } from "./types.js";

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
 * - dirtyFileKeys: maps file path → set of dirty cache keys for that file
 *
 * This avoids O(cache-size) scans in flushFile, evictFile, deleteFile,
 * invalidatePagesFrom, flushAll, and dirtyCount — all of which are called
 * frequently during PGlite workloads (e.g., close → flushFile on every fd).
 *
 * The dirtyFileKeys index makes flushFile O(dirty-for-file) instead of
 * O(cached-for-file), eliminating wasteful scans on close() when a file
 * has many cached pages but none are dirty (common for read-heavy workloads).
 */
export class SyncPageCache {
  private readonly maxPages: number;
  private readonly backend: SyncStorageBackend;

  /** Cache entries keyed by pageKeyStr. Insertion order = LRU order (oldest first). */
  private cache = new Map<string, CachedPage>();

  /** Most-recently-used page reference. Avoids key construction + Map lookup on hot path. */
  private mruPage: CachedPage | null = null;

  /** Secondary index: file path → set of cache keys belonging to that file. */
  private filePages = new Map<string, Set<string>>();

  /** Secondary index: set of cache keys for dirty pages. */
  private dirtyKeys = new Set<string>();

  /** Secondary index: file path → set of dirty cache keys for that file.
   *  Enables O(dirty-for-file) flushFile instead of O(cached-for-file). */
  private dirtyFileKeys = new Map<string, Set<string>>();

  /** Reusable buffer pool to reduce Uint8Array allocation pressure.
   *  Buffers are returned here on eviction/deletion and reused on cache miss. */
  private bufferPool: Uint8Array[] = [];

  /** Performance counters. */
  private _hits = 0;
  private _misses = 0;
  private _evictions = 0;
  private _flushes = 0;

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
    return this.getPageInternal(path, pageIndex, true);
  }

  /**
   * Get a page from cache, or create a zero-filled page on cache miss
   * without reading from the backend.
   *
   * Used for pages beyond the current file extent during writes — we know
   * these pages don't exist in the backend, so the readPage call would be
   * a wasted round-trip (a synchronous SAB bridge call returning null).
   */
  getPageNoRead(path: string, pageIndex: number): CachedPage {
    return this.getPageInternal(path, pageIndex, false);
  }

  /**
   * Get or create a page. When readBackend is false, skips the backend read
   * and creates a zero-filled page on cache miss. Used for pages beyond the
   * current file extent during writes — we know these pages don't exist in
   * the backend, so the read would be a wasted round-trip.
   */
  private getPageInternal(
    path: string,
    pageIndex: number,
    readBackend: boolean,
  ): CachedPage {
    // Fast path: exact same page as last access — no key construction or Map ops
    const mru = this.mruPage;
    if (mru !== null && mru.path === path && mru.pageIndex === pageIndex) {
      this._hits++;
      return mru;
    }

    const key = pageKeyStr(path, pageIndex);
    const existing = this.cache.get(key);
    if (existing) {
      this._hits++;
      // Reorder for LRU on every cache hit. Pages accessed below capacity
      // must still be reordered — otherwise they retain their insertion-time
      // position, and when the cache later fills, they're evicted first
      // despite being recently accessed. The MRU fast path above already
      // skips this for repeated accesses to the exact same page.
      this.cache.delete(key);
      this.cache.set(key, existing);
      this.mruPage = existing;
      return existing;
    }

    this._misses++;
    // Cache miss — load from backend (or create zero-filled if beyond file extent).
    // Backend.readPage returns a defensive copy, so we use it directly
    // (no second copy needed). For new/missing pages, acquire a zeroed buffer.
    const data = readBackend
      ? this.backend.readPage(path, pageIndex)
      : null;
    const page: CachedPage = {
      key,
      path,
      pageIndex,
      data: data ?? this.acquireBuffer(),
      dirty: false,
      evicted: false,
    };

    this.ensureCapacity();
    this.cache.set(key, page);
    this.mruPage = page;
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

    const firstPage = Math.floor(position / PAGE_SIZE);
    const pageOffset = position - firstPage * PAGE_SIZE;

    // Fast path: entire read fits within a single page (common case for
    // page-aligned Postgres I/O). Skips multi-page setup, loop, and
    // per-iteration index computation.
    if (pageOffset + toRead <= PAGE_SIZE) {
      const page = this.getPage(path, firstPage);
      buffer.set(
        page.data.subarray(pageOffset, pageOffset + toRead),
        offset,
      );
      return toRead;
    }

    // Multi-page path
    const lastPage = Math.floor((position + toRead - 1) / PAGE_SIZE);

    // Find cache misses — only batch when there are multiple misses
    const missingIndices: number[] = [];
    for (let p = firstPage; p <= lastPage; p++) {
      if (!this.cache.has(pageKeyStr(path, p))) {
        missingIndices.push(p);
      }
    }

    if (missingIndices.length > 1) {
      // Batch-evict space before pre-loading to reduce bridge round-trips
      this.batchEvict(missingIndices.length);

      this._misses += missingIndices.length;
      const results = this.backend.readPages(path, missingIndices);
      for (let i = 0; i < missingIndices.length; i++) {
        const pi = missingIndices[i];
        const pkey = pageKeyStr(path, pi);
        if (this.cache.has(pkey)) continue; // may appear via eviction cascade
        const page: CachedPage = {
          key: pkey,
          path,
          pageIndex: pi,
          data: results[i] ?? this.acquireBuffer(),
          dirty: false,
          evicted: false,
        };
        this.ensureCapacity();
        this.cache.set(pkey, page);
        this.mruPage = page;
        this.trackPage(path, pkey);
      }
    }

    // Read from cache (all multi-miss pages are pre-loaded; single misses use getPage)
    let bytesRead = 0;
    let pos = position;

    while (bytesRead < toRead) {
      const pi = Math.floor(pos / PAGE_SIZE);
      const po = pos - pi * PAGE_SIZE;
      const bytesInPage = Math.min(PAGE_SIZE - po, toRead - bytesRead);

      const page = this.getPage(path, pi);
      buffer.set(
        page.data.subarray(po, po + bytesInPage),
        offset + bytesRead,
      );

      bytesRead += bytesInPage;
      pos += bytesInPage;
    }

    // Compensate for false hits: batch-loaded pages were counted as misses
    // above, but getPage() also counted them as hits when it found them
    // in cache. Subtract the false hits to keep stats accurate.
    if (missingIndices.length > 1) {
      this._hits -= missingIndices.length;
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
   * eviction + loading are batched to reduce SAB bridge round-trips.
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

    const firstPage = Math.floor(position / PAGE_SIZE);
    const pageOffset = position - firstPage * PAGE_SIZE;

    // Pages at or beyond this index don't exist in the backend, so we can
    // skip the readPage call and create zero-filled pages directly. This
    // avoids wasted SAB round-trips when extending files (the common case
    // for sequential Postgres writes).
    const firstNewPage =
      currentFileSize > 0 ? Math.ceil(currentFileSize / PAGE_SIZE) : 0;

    // Fast path: entire write fits within a single page
    if (pageOffset + length <= PAGE_SIZE) {
      // Skip backend read when the entire page will be overwritten —
      // every byte is about to be replaced, so reading old data is wasted.
      const needsRead =
        firstPage < firstNewPage &&
        !(pageOffset === 0 && length >= PAGE_SIZE);
      const page = this.getPageInternal(path, firstPage, needsRead);
      page.data.set(
        buffer.subarray(offset, offset + length),
        pageOffset,
      );
      if (!page.dirty) {
        page.dirty = true;
        this.trackDirty(path, page.key);
      }
      const newFileSize = Math.max(currentFileSize, position + length);
      return { bytesWritten: length, newFileSize };
    }

    // Multi-page path
    const lastPage = Math.floor((position + length - 1) / PAGE_SIZE);

    // Separate cache misses into pages that need backend reads vs pages
    // that can skip them (beyond file extent, or fully overwritten).
    const existingMissing: number[] = [];
    let totalMissing = 0;
    const writeEnd = position + length;
    for (let p = firstPage; p <= lastPage; p++) {
      if (!this.cache.has(pageKeyStr(path, p))) {
        totalMissing++;
        if (p < firstNewPage) {
          // Skip preloading pages that will be completely overwritten —
          // every byte will be replaced, so the backend read is wasted.
          const fullyOverwritten =
            position <= p * PAGE_SIZE &&
            writeEnd >= (p + 1) * PAGE_SIZE;
          if (!fullyOverwritten) {
            existingMissing.push(p);
          }
        }
      }
    }

    if (totalMissing > 0) {
      // Batch-evict space for all missing pages at once
      this.batchEvict(totalMissing);

      // Batch-load only pages that exist in the backend
      if (existingMissing.length > 1) {
        this._misses += existingMissing.length;
        const results = this.backend.readPages(path, existingMissing);
        for (let i = 0; i < existingMissing.length; i++) {
          const pi = existingMissing[i];
          const pkey = pageKeyStr(path, pi);
          if (this.cache.has(pkey)) continue;
          const page: CachedPage = {
            key: pkey,
            path,
            pageIndex: pi,
            data: results[i] ?? this.acquireBuffer(),
            dirty: false,
            evicted: false,
          };
          this.ensureCapacity();
          this.cache.set(pkey, page);
          this.mruPage = page;
          this.trackPage(path, pkey);
        }
      }
      // Single existing misses and all new pages are handled by
      // getPageInternal in the write loop below.
    }

    // Write data into pages (pre-loaded existing pages are cache hits;
    // new pages beyond file extent skip the backend read)
    let bytesWritten = 0;
    let pos = position;

    while (bytesWritten < length) {
      const pi = Math.floor(pos / PAGE_SIZE);
      const po = pos - pi * PAGE_SIZE;
      const bytesInPage = Math.min(PAGE_SIZE - po, length - bytesWritten);

      // Skip backend read for fully-overwritten pages (po === 0 means
      // write starts at page boundary; bytesInPage === PAGE_SIZE means
      // the entire page is covered).
      const needsRead =
        pi < firstNewPage && !(po === 0 && bytesInPage === PAGE_SIZE);
      const page = this.getPageInternal(path, pi, needsRead);
      page.data.set(
        buffer.subarray(
          offset + bytesWritten,
          offset + bytesWritten + bytesInPage,
        ),
        po,
      );
      if (!page.dirty) {
        page.dirty = true;
        this.trackDirty(path, page.key);
      }

      bytesWritten += bytesInPage;
      pos += bytesInPage;
    }

    // Compensate for false hits: batch-loaded pages were counted as misses
    // above, but getPageInternal() also counted them as hits when it found
    // them in cache. Subtract the false hits to keep stats accurate.
    if (existingMissing.length > 1) {
      this._hits -= existingMissing.length;
    }

    const newFileSize = Math.max(currentFileSize, position + length);
    return { bytesWritten, newFileSize };
  }

  /**
   * Flush all dirty pages for a specific file to the backend.
   * O(dirty-for-file) via dirtyFileKeys index — no scan of clean pages.
   */
  flushFile(path: string): number {
    const keys = this.dirtyFileKeys.get(path);
    if (!keys || keys.size === 0) return 0;

    const dirtyPages: Array<{
      path: string;
      pageIndex: number;
      data: Uint8Array;
    }> = [];

    for (const key of keys) {
      const page = this.cache.get(key)!;
      dirtyPages.push({
        path: page.path,
        pageIndex: page.pageIndex,
        data: page.data,
      });
    }

    this.backend.writePages(dirtyPages);
    this._flushes += dirtyPages.length;
    for (const key of keys) {
      this.cache.get(key)!.dirty = false;
      this.dirtyKeys.delete(key);
    }
    this.dirtyFileKeys.delete(path);

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
    this._flushes += dirtyPages.length;
    for (const key of this.dirtyKeys) {
      this.cache.get(key)!.dirty = false;
    }
    this.dirtyKeys.clear();
    this.dirtyFileKeys.clear();

    return dirtyPages.length;
  }

  /**
   * Collect all dirty pages without clearing their dirty flags.
   *
   * Used by tomefs syncfs to combine dirty pages with metadata into a
   * single backend.syncAll() call, reducing SAB round-trips from 2→1
   * and enabling atomic IDB commits (pages + metadata in one transaction).
   *
   * Call commitDirtyPages() after the backend write succeeds to clear
   * dirty flags. This two-phase approach preserves dirty state if the
   * write fails (e.g., IDB quota exceeded, crash during syncAll), so
   * the next sync retries the flush instead of silently losing data.
   */
  collectDirtyPages(): Array<{
    path: string;
    pageIndex: number;
    data: Uint8Array;
  }> {
    if (this.dirtyKeys.size === 0) return [];

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

    return dirtyPages;
  }

  /**
   * Clear dirty flags for pages previously returned by collectDirtyPages.
   *
   * Only clears pages that are still in the cache and still dirty.
   * Pages at DIFFERENT keys dirtied between collect and commit are
   * preserved. However, if the SAME page (same path + pageIndex) is
   * re-dirtied between collect and commit, the dirty flag is cleared
   * unconditionally — there is no generation counter to distinguish
   * "dirty from before collect" vs "newly dirty." This is safe because
   * the collect→syncAll→commit sequence in tomefs syncfs is fully
   * synchronous (SAB bridge), so no writes can interleave.
   */
  commitDirtyPages(
    pages: Array<{ path: string; pageIndex: number }>,
  ): void {
    let committed = 0;
    for (const { path, pageIndex } of pages) {
      const key = pageKeyStr(path, pageIndex);
      const page = this.cache.get(key);
      if (page && page.dirty) {
        page.dirty = false;
        this.dirtyKeys.delete(key);
        const fileSet = this.dirtyFileKeys.get(path);
        if (fileSet) {
          fileSet.delete(key);
          if (fileSet.size === 0) this.dirtyFileKeys.delete(path);
        }
        committed++;
      }
    }
    this._flushes += committed;
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
      const page = this.cache.get(key)!;
      page.evicted = true;
      this.cache.delete(key);
      this.releaseBuffer(page.data);
      if (page === this.mruPage) this.mruPage = null;
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
    const fileDirty = this.dirtyFileKeys.get(path);
    for (const key of keys) {
      const page = this.cache.get(key)!;
      if (page.pageIndex >= fromPageIndex) {
        page.evicted = true;
        this.cache.delete(key);
        this.releaseBuffer(page.data);
        if (page === this.mruPage) this.mruPage = null;
        this.dirtyKeys.delete(key);
        if (fileDirty) fileDirty.delete(key);
        keys.delete(key);
      }
    }
    if (keys.size === 0) {
      this.filePages.delete(path);
    }
    if (fileDirty && fileDirty.size === 0) {
      this.dirtyFileKeys.delete(path);
    }
  }

  /**
   * Zero-fill the tail of the last page after truncation to a smaller size.
   * If the page is cached, modifies it in place. If the page is only in
   * the backend (evicted from cache), loads it, zeros the tail, and writes
   * it back. This prevents stale data from being served if the file is
   * later extended without writing to the truncated region.
   */
  zeroTailAfterTruncate(path: string, newSize: number): void {
    const lastPageIndex = Math.floor(newSize / PAGE_SIZE);
    const tailOffset = newSize % PAGE_SIZE;
    if (tailOffset === 0) return;

    // Load the page through the cache (from backend if not cached).
    // This avoids bypassing the cache with direct backend read+write,
    // which would cause an unnecessary write round-trip and a guaranteed
    // cache miss on the next access to this page.
    const page = this.getPageInternal(path, lastPageIndex, true);
    page.data.fill(0, tailOffset);
    if (!page.dirty) {
      page.dirty = true;
      this.trackDirty(path, page.key);
    }
  }

  /**
   * Remove all pages for a file from the cache without touching the backend.
   * Callers that need atomic backend cleanup (e.g. deleteAll) use this to
   * clear the cache separately from the backend operation.
   * O(pages-for-file) via filePages index.
   */
  discardFile(path: string): void {
    const keys = this.filePages.get(path);
    if (keys) {
      for (const key of keys) {
        const page = this.cache.get(key)!;
        page.evicted = true;
        this.cache.delete(key);
        this.releaseBuffer(page.data);
        if (page === this.mruPage) this.mruPage = null;
        this.dirtyKeys.delete(key);
      }
      this.filePages.delete(path);
    }
    this.dirtyFileKeys.delete(path);
  }

  /**
   * Delete all pages for a file from both cache and backend.
   * O(pages-for-file) via filePages index.
   */
  deleteFile(path: string): void {
    this.discardFile(path);
    this.backend.deleteFile(path);
  }

  /**
   * Rename a file's pages: flush, move in backend, update cache keys.
   *
   * Delegates to the backend's renameFile for an atomic re-key of all pages.
   * Through the SAB bridge, this is a single round-trip instead of O(pages)
   * individual readPage calls.
   */
  renameFile(oldPath: string, newPath: string): void {
    if (oldPath === newPath) return;
    // Flush dirty pages so backend has the latest data before re-keying
    this.flushFile(oldPath);

    // Evict any cached pages for the destination path. The backend's
    // renameFile clears destination pages before copying, so cached
    // destination pages would be stale after the backend call. Evict
    // (not flush) because they're about to be overwritten in the backend.
    const destKeys = this.filePages.get(newPath);
    if (destKeys) {
      for (const key of destKeys) {
        const page = this.cache.get(key)!;
        page.evicted = true;
        this.cache.delete(key);
        this.releaseBuffer(page.data);
        if (page === this.mruPage) this.mruPage = null;
        this.dirtyKeys.delete(key);
      }
      this.filePages.delete(newPath);
    }
    this.dirtyFileKeys.delete(newPath);

    // Re-key all pages in the backend atomically
    this.backend.renameFile(oldPath, newPath);

    // Collect all cached pages for old path via filePages index.
    // Track which moved pages are dirty so we can rebuild dirtyFileKeys.
    const oldKeys = this.filePages.get(oldPath);
    const toMove: CachedPage[] = [];
    if (oldKeys) {
      for (const key of oldKeys) {
        const page = this.cache.get(key)!;
        toMove.push(page);
        this.cache.delete(key);
        if (page === this.mruPage) this.mruPage = null;
        this.dirtyKeys.delete(key);
      }
      this.filePages.delete(oldPath);
    }
    this.dirtyFileKeys.delete(oldPath);
    for (const page of toMove) {
      page.path = newPath;
      const newKey = pageKeyStr(newPath, page.pageIndex);
      page.key = newKey;
      this.ensureCapacity();
      this.cache.set(newKey, page);
      this.trackPage(newPath, newKey);
      if (page.dirty) {
        this.trackDirty(newPath, newKey);
      }
    }
  }

  /**
   * Ensure a page exists in the cache and is marked dirty.
   *
   * Used by resizeFileStorage when growing a file via allocate(). The
   * extended region is zero-filled but pages aren't materialized — they're
   * created on demand. Without marking the last page dirty, flushAll()
   * won't write it to the backend, and restoreTree will conclude the
   * metadata is stale (pages missing) and shrink the file on remount.
   */
  markPageDirty(path: string, pageIndex: number): void {
    const page = this.getPage(path, pageIndex);
    if (!page.dirty) {
      page.dirty = true;
      this.trackDirty(path, page.key);
    }
  }

  /**
   * Ensure a page exists in the cache and is marked dirty, without reading
   * from the backend on cache miss.
   *
   * Used by resizeFileStorage when growing a file via allocate(). The
   * sentinel page is always beyond the current file extent, so it cannot
   * exist in the backend — the readPage call would be a wasted SAB bridge
   * round-trip returning null. This skips that read and creates a zero-
   * filled page directly.
   *
   * IMPORTANT: Only safe for pages known to not exist in the backend.
   * For pages that may have existing data, use markPageDirty() instead.
   */
  markPageDirtyNoRead(path: string, pageIndex: number): void {
    const page = this.getPageNoRead(path, pageIndex);
    if (!page.dirty) {
      page.dirty = true;
      this.trackDirty(path, page.key);
    }
  }

  /**
   * Register a page as dirty by its cache key and file path (no page lookup
   * or key construction).
   *
   * Used by tomefs's per-node page table optimization: when the caller already
   * holds a valid CachedPage reference and has set page.dirty = true, this
   * adds the key to the dirty indexes without the overhead of getPage()
   * or pageKeyStr().
   */
  addDirtyKey(key: string, path: string): void {
    this.trackDirty(path, key);
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
   * Add a cache key to the dirtyFileKeys index for the given path.
   */
  private trackDirty(path: string, key: string): void {
    this.dirtyKeys.add(key);
    let keys = this.dirtyFileKeys.get(path);
    if (!keys) {
      keys = new Set();
      this.dirtyFileKeys.set(path, keys);
    }
    keys.add(key);
  }

  /**
   * Remove a cache key from the dirtyFileKeys index.
   */
  private untrackDirty(path: string, key: string): void {
    this.dirtyKeys.delete(key);
    const keys = this.dirtyFileKeys.get(path);
    if (keys) {
      keys.delete(key);
      if (keys.size === 0) this.dirtyFileKeys.delete(path);
    }
  }

  /**
   * Ensure there is room for at least one more page.
   * Evicts the least-recently-used page if at capacity.
   */
  private ensureCapacity(): void {
    while (this.cache.size >= this.maxPages) {
      this.evictOne();
    }
  }

  /**
   * Evict the single least-recently-used page.
   * If the page is dirty, it is flushed individually to the backend.
   */
  private evictOne(): void {
    const firstKey = this.cache.keys().next().value!;
    const victim = this.cache.get(firstKey)!;

    if (victim.dirty) {
      this.backend.writePage(victim.path, victim.pageIndex, victim.data);
      victim.dirty = false;
      this.untrackDirty(victim.path, firstKey);
      this._flushes++;
    }

    victim.evicted = true;
    this._evictions++;
    this.cache.delete(firstKey);
    this.releaseBuffer(victim.data);
    if (victim === this.mruPage) this.mruPage = null;

    const keys = this.filePages.get(victim.path);
    if (keys) {
      keys.delete(firstKey);
      if (keys.size === 0) {
        this.filePages.delete(victim.path);
      }
    }
  }

  /**
   * Batch-evict pages to make room for `count` new entries.
   *
   * Collects all dirty victims and flushes them in a single writePages
   * call, reducing SAB bridge round-trips from O(dirty-evictions) to O(1).
   * Falls back to single-page eviction when only one page needs flushing.
   */
  private batchEvict(count: number): void {
    const needed = Math.min(
      this.cache.size,
      this.cache.size + count - this.maxPages,
    );
    if (needed <= 0) return;

    // For a single eviction, use the simple path (no array allocation)
    if (needed === 1) {
      this.evictOne();
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
      this.backend.writePage(p.path, p.pageIndex, p.data);
    } else if (dirtyPages.length > 1) {
      this.backend.writePages(dirtyPages);
    }
    this._flushes += dirtyPages.length;

    // Remove victims from cache and indexes
    this._evictions += victimKeys.length;
    for (let i = 0; i < victimKeys.length; i++) {
      const key = victimKeys[i];
      const victim = victims[i];

      victim.evicted = true;
      this.cache.delete(key);
      this.releaseBuffer(victim.data);
      if (victim === this.mruPage) this.mruPage = null;

      if (victim.dirty) {
        victim.dirty = false;
        this.untrackDirty(victim.path, key);
      }

      const fileKeys = this.filePages.get(victim.path);
      if (fileKeys) {
        fileKeys.delete(key);
        if (fileKeys.size === 0) this.filePages.delete(victim.path);
      }
    }
  }

  /** Acquire a zeroed PAGE_SIZE buffer from the pool, or allocate a new one. */
  private acquireBuffer(): Uint8Array {
    const buf = this.bufferPool.pop();
    if (buf) {
      buf.fill(0);
      return buf;
    }
    return new Uint8Array(PAGE_SIZE);
  }

  /** Return a buffer to the pool for reuse (max 64 buffers pooled). */
  private releaseBuffer(buf: Uint8Array): void {
    if (this.bufferPool.length < 64) {
      this.bufferPool.push(buf);
    }
  }

  /**
   * Validate internal index consistency. Throws if any invariant is violated.
   *
   * Checks that the five concurrent data structures (cache, mruPage,
   * filePages, dirtyKeys, dirtyFileKeys) are mutually consistent. Intended
   * for use in fuzz tests to catch index corruption that doesn't immediately
   * manifest as incorrect data but degrades performance or leaks memory.
   */
  assertInvariants(): void {
    // 1. Cache size within bounds
    if (this.cache.size > this.maxPages) {
      throw new Error(
        `cache size ${this.cache.size} exceeds maxPages ${this.maxPages}`,
      );
    }

    // 2. No evicted pages in the cache
    for (const [key, page] of this.cache) {
      if (page.evicted) {
        throw new Error(`evicted page ${key} still in cache`);
      }
    }

    // 3. Every dirty key exists in cache with dirty=true
    for (const key of this.dirtyKeys) {
      const page = this.cache.get(key);
      if (!page) {
        throw new Error(`dirtyKeys contains ${key} not in cache`);
      }
      if (!page.dirty) {
        throw new Error(`dirtyKeys contains ${key} but page.dirty is false`);
      }
    }

    // 4. Every page with dirty=true is in dirtyKeys
    for (const [key, page] of this.cache) {
      if (page.dirty && !this.dirtyKeys.has(key)) {
        throw new Error(`page ${key} is dirty but not in dirtyKeys`);
      }
    }

    // 5. filePages covers exactly the keys in cache
    const filePagesUnion = new Set<string>();
    for (const [path, keys] of this.filePages) {
      for (const key of keys) {
        if (filePagesUnion.has(key)) {
          throw new Error(`filePages key ${key} appears under multiple paths`);
        }
        filePagesUnion.add(key);
        const page = this.cache.get(key);
        if (!page) {
          throw new Error(
            `filePages[${path}] contains ${key} not in cache`,
          );
        }
        if (page.path !== path) {
          throw new Error(
            `filePages[${path}] contains ${key} but page.path is ${page.path}`,
          );
        }
      }
    }
    for (const key of this.cache.keys()) {
      if (!filePagesUnion.has(key)) {
        throw new Error(`cache key ${key} not tracked in filePages`);
      }
    }

    // 6. dirtyFileKeys is consistent with dirtyKeys
    const dirtyFileKeysUnion = new Set<string>();
    for (const [path, keys] of this.dirtyFileKeys) {
      if (keys.size === 0) {
        throw new Error(
          `dirtyFileKeys[${path}] is empty (should be deleted)`,
        );
      }
      for (const key of keys) {
        dirtyFileKeysUnion.add(key);
        if (!this.dirtyKeys.has(key)) {
          throw new Error(
            `dirtyFileKeys[${path}] contains ${key} not in dirtyKeys`,
          );
        }
        const page = this.cache.get(key);
        if (page && page.path !== path) {
          throw new Error(
            `dirtyFileKeys[${path}] contains ${key} but page.path is ${page.path}`,
          );
        }
      }
      const fileKeys = this.filePages.get(path);
      if (fileKeys) {
        for (const key of keys) {
          if (!fileKeys.has(key)) {
            throw new Error(
              `dirtyFileKeys[${path}] contains ${key} not in filePages[${path}]`,
            );
          }
        }
      }
    }
    for (const key of this.dirtyKeys) {
      if (!dirtyFileKeysUnion.has(key)) {
        throw new Error(
          `dirtyKeys contains ${key} not tracked in dirtyFileKeys`,
        );
      }
    }

    // 7. mruPage (if set) is in the cache
    if (this.mruPage !== null) {
      if (!this.cache.has(this.mruPage.key)) {
        throw new Error(
          `mruPage ${this.mruPage.key} not in cache`,
        );
      }
    }

    // 8. Page key/path/pageIndex consistency
    for (const [key, page] of this.cache) {
      if (page.key !== key) {
        throw new Error(
          `cache key ${key} but page.key is ${page.key}`,
        );
      }
      const expectedKey = pageKeyStr(page.path, page.pageIndex);
      if (key !== expectedKey) {
        throw new Error(
          `page key ${key} doesn't match pageKeyStr(${page.path}, ${page.pageIndex}) = ${expectedKey}`,
        );
      }
    }
  }
}
