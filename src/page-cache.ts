import type { StorageBackend } from "./storage-backend.js";
import { PAGE_SIZE, DEFAULT_MAX_PAGES, pageKeyStr } from "./types.js";
import type { CachedPage } from "./types.js";

/**
 * LRU page cache with dirty tracking.
 *
 * Pages are loaded on demand from a StorageBackend and cached in memory.
 * When the cache is full, the least-recently-used page is evicted. If the
 * evicted page is dirty, it is flushed to the backend before eviction.
 *
 * The cache is keyed by (path, pageIndex) and stores PAGE_SIZE-byte buffers.
 */
export class PageCache {
  private readonly maxPages: number;
  private readonly backend: StorageBackend;

  /** Cache entries keyed by pageKeyStr. Insertion order = LRU order (oldest first). */
  private cache = new Map<string, CachedPage>();

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
    const existing = this.cache.get(key);
    if (existing) {
      // Move to end (most recently used)
      this.cache.delete(key);
      this.cache.set(key, existing);
      return existing;
    }

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
    return page;
  }

  /**
   * Read bytes from a file at a given position.
   *
   * Handles reads that span multiple pages. Returns the number of bytes
   * actually read (may be less than length if position + length > fileSize).
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
  async flushFile(path: string): Promise<number> {
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
      await this.backend.writePages(dirtyPages);
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
  async flushAll(): Promise<number> {
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
      await this.backend.writePages(dirtyPages);
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
  async evictFile(path: string): Promise<void> {
    await this.flushFile(path);
    const prefix = `${path}\0`;
    for (const key of this.cache.keys()) {
      if (key.startsWith(prefix)) {
        this.cache.delete(key);
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
  private async ensureCapacity(): Promise<void> {
    while (this.cache.size >= this.maxPages) {
      // Map iteration order is insertion order — first entry is LRU
      const firstKey = this.cache.keys().next().value!;
      const victim = this.cache.get(firstKey)!;

      if (victim.dirty) {
        await this.backend.writePage(
          victim.path,
          victim.pageIndex,
          victim.data,
        );
      }

      this.cache.delete(firstKey);
    }
  }
}
