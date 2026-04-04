/** Page size in bytes — matches Postgres's internal page size. */
export const PAGE_SIZE = 8192;

/** Default maximum number of pages in the LRU cache (4096 * 8KB = 32MB). */
export const DEFAULT_MAX_PAGES = 4096;

/** A single cached page. */
export interface CachedPage {
  /** Cache key for this page (avoids redundant pageKeyStr on hot paths). */
  key: string;
  /** File path this page belongs to. */
  path: string;
  /** Zero-based page index within the file. */
  pageIndex: number;
  /** The page data (always PAGE_SIZE bytes; trailing bytes zero-filled). */
  data: Uint8Array;
  /** Whether this page has been modified since last flush. */
  dirty: boolean;
  /**
   * Set to true when this page is evicted from the cache.
   *
   * Enables external code (e.g., tomefs per-node page table) to hold
   * references to CachedPage objects and detect when the reference is
   * stale. After eviction, the page data reflects the last flushed state
   * but may not match the current cache contents if the same page was
   * later re-loaded and modified.
   */
  evicted: boolean;
}

/** File metadata stored alongside page data. */
export interface FileMeta {
  size: number;
  mode: number;
  ctime: number;
  mtime: number;
  /** Access time (optional, defaults to mtime). */
  atime?: number;
  /** Symlink target path (only for symlinks). */
  link?: string;
}

/** Compound key for page storage: [path, pageIndex]. */
export type PageKey = [string, number];

/** Serialize a page key to a string for use in Maps. */
export function pageKeyStr(path: string, pageIndex: number): string {
  return `${path}\0${pageIndex}`;
}

/** Snapshot of page cache performance counters. */
export interface CacheStats {
  /** Total cache hits (page found in cache). */
  hits: number;
  /** Total cache misses (page loaded from backend). */
  misses: number;
  /** Total pages evicted from the cache. */
  evictions: number;
  /** Total dirty pages flushed to the backend. */
  flushes: number;
}
