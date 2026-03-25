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
 */
export class SyncPageCache {
  private readonly maxPages: number;
  private readonly backend: SyncStorageBackend;

  /** Cache entries keyed by pageKeyStr. Insertion order = LRU order (oldest first). */
  private cache = new Map<string, CachedPage>();

  /** Key of the most-recently-used page (last entry in Map). Skip LRU touch if hit. */
  private mruKey: string | null = null;

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
    return page;
  }

  /**
   * Read bytes from a file at a given position.
   *
   * Handles reads that span multiple pages. Returns the number of bytes
   * actually read (may be less than length if position + length > fileSize).
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
      page.dirty = true;

      bytesWritten += bytesInPage;
      pos += bytesInPage;
    }

    const newFileSize = Math.max(currentFileSize, position + length);
    return { bytesWritten, newFileSize };
  }

  /**
   * Flush all dirty pages for a specific file to the backend.
   */
  flushFile(path: string): number {
    const dirtyPages: Array<{
      path: string;
      pageIndex: number;
      data: Uint8Array;
    }> = [];

    for (const page of this.cache.values()) {
      if (page.path === path && page.dirty) {
        dirtyPages.push({
          path: page.path,
          pageIndex: page.pageIndex,
          data: page.data,
        });
      }
    }

    if (dirtyPages.length > 0) {
      this.backend.writePages(dirtyPages);
      for (const page of this.cache.values()) {
        if (page.path === path && page.dirty) {
          page.dirty = false;
        }
      }
    }

    return dirtyPages.length;
  }

  /**
   * Flush all dirty pages across all files to the backend.
   */
  flushAll(): number {
    const dirtyPages: Array<{
      path: string;
      pageIndex: number;
      data: Uint8Array;
    }> = [];

    for (const page of this.cache.values()) {
      if (page.dirty) {
        dirtyPages.push({
          path: page.path,
          pageIndex: page.pageIndex,
          data: page.data,
        });
      }
    }

    if (dirtyPages.length > 0) {
      this.backend.writePages(dirtyPages);
      for (const page of this.cache.values()) {
        if (page.dirty) {
          page.dirty = false;
        }
      }
    }

    return dirtyPages.length;
  }

  /**
   * Evict all cached pages for a file. Dirty pages are flushed first.
   */
  evictFile(path: string): void {
    this.flushFile(path);
    const prefix = `${path}\0`;
    for (const key of this.cache.keys()) {
      if (key.startsWith(prefix)) {
        this.cache.delete(key);
        if (key === this.mruKey) this.mruKey = null;
      }
    }
  }

  /**
   * Invalidate cached pages for a file beyond a given page index.
   * Used after truncation to discard stale pages. Does NOT flush — caller
   * should handle backend deletion separately.
   */
  invalidatePagesFrom(path: string, fromPageIndex: number): void {
    const prefix = `${path}\0`;
    for (const [key, page] of this.cache.entries()) {
      if (key.startsWith(prefix) && page.pageIndex >= fromPageIndex) {
        this.cache.delete(key);
        if (key === this.mruKey) this.mruKey = null;
      }
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
      page.dirty = true;
    }
  }

  /**
   * Delete all pages for a file from both cache and backend.
   */
  deleteFile(path: string): void {
    // Remove from cache (no flush — file is being deleted)
    const prefix = `${path}\0`;
    for (const key of this.cache.keys()) {
      if (key.startsWith(prefix)) {
        this.cache.delete(key);
        if (key === this.mruKey) this.mruKey = null;
      }
    }
    this.backend.deleteFile(path);
  }

  /**
   * Rename a file's pages: flush, move in backend, update cache keys.
   */
  renameFile(oldPath: string, newPath: string): void {
    // Flush dirty pages for old path so backend has the latest data
    this.flushFile(oldPath);

    // Collect all cached pages for old path
    const oldPrefix = `${oldPath}\0`;
    const toMove: CachedPage[] = [];
    for (const [key, page] of this.cache.entries()) {
      if (key.startsWith(oldPrefix)) {
        toMove.push(page);
        this.cache.delete(key);
        if (key === this.mruKey) this.mruKey = null;
      }
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
    }
  }

  /** Check if a specific page is in the cache. */
  has(path: string, pageIndex: number): boolean {
    return this.cache.has(pageKeyStr(path, pageIndex));
  }

  /** Check if a specific page is dirty. Returns false if not cached. */
  isDirty(path: string, pageIndex: number): boolean {
    const page = this.cache.get(pageKeyStr(path, pageIndex));
    return page ? page.dirty : false;
  }

  /** Count dirty pages in cache. */
  get dirtyCount(): number {
    let count = 0;
    for (const page of this.cache.values()) {
      if (page.dirty) count++;
    }
    return count;
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
      }

      this.cache.delete(firstKey);
      if (firstKey === this.mruKey) this.mruKey = null;
    }
  }
}
