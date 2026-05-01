import type { FileMeta } from "./types.js";

/**
 * Interface for pluggable storage backends.
 *
 * Backends handle persistent storage of file pages and metadata.
 * All methods are async to support IndexedDB and OPFS backends.
 * For testing, MemoryBackend provides a synchronous-under-the-hood implementation.
 */
export interface StorageBackend {
  /** Read a single page. Returns null if the page doesn't exist. */
  readPage(path: string, pageIndex: number): Promise<Uint8Array | null>;

  /** Read multiple pages in a single batch. Returns an array parallel to pageIndices. */
  readPages(
    path: string,
    pageIndices: number[],
  ): Promise<Array<Uint8Array | null>>;

  /** Write a single page. */
  writePage(path: string, pageIndex: number, data: Uint8Array): Promise<void>;

  /** Write multiple pages in a single batch. */
  writePages(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
  ): Promise<void>;

  /** Delete all pages for a file. */
  deleteFile(path: string): Promise<void>;

  /** Delete all pages for multiple files in a single batch. */
  deleteFiles(paths: string[]): Promise<void>;

  /** Delete pages beyond a given index (for truncation). */
  deletePagesFrom(path: string, fromPageIndex: number): Promise<void>;

  /** Rename all pages from one path to another. */
  renameFile(oldPath: string, newPath: string): Promise<void>;

  /** Count the number of pages stored for a file. Returns 0 if no pages exist. */
  countPages(path: string): Promise<number>;

  /** Count pages for multiple files in a single batch. Returns an array parallel to paths. */
  countPagesBatch(paths: string[]): Promise<number[]>;

  /** Return the highest page index stored for a file, or -1 if no pages exist. */
  maxPageIndex(path: string): Promise<number>;

  /** Return the highest page index for multiple files in a single batch. Returns an array parallel to paths. */
  maxPageIndexBatch(paths: string[]): Promise<number[]>;

  /** Read file metadata. Returns null if the file doesn't exist. */
  readMeta(path: string): Promise<FileMeta | null>;

  /** Write file metadata. */
  writeMeta(path: string, meta: FileMeta): Promise<void>;

  /** Write multiple metadata entries in a single batch. */
  writeMetas(entries: Array<{ path: string; meta: FileMeta }>): Promise<void>;

  /** Delete file metadata. */
  deleteMeta(path: string): Promise<void>;

  /** Read multiple metadata entries in a single batch. Returns an array parallel to paths. */
  readMetas(paths: string[]): Promise<Array<FileMeta | null>>;

  /** Delete multiple metadata entries in a single batch. */
  deleteMetas(paths: string[]): Promise<void>;

  /** List all file paths that have metadata stored. */
  listFiles(): Promise<string[]>;

  /**
   * Atomically write dirty pages and metadata in a single operation.
   *
   * For IDB backends, this uses a single multi-store transaction spanning
   * both the pages and metadata stores, ensuring page data and metadata
   * are committed together. This eliminates the crash window between
   * separate writePages + writeMetas calls during syncfs, and halves
   * SAB bridge round-trips (2→1).
   */
  syncAll(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
    metas: Array<{ path: string; meta: FileMeta }>,
  ): Promise<void>;

  /**
   * Atomically delete all pages and metadata for the given paths.
   *
   * Combines deleteFiles + deleteMetas into a single operation. For IDB,
   * this executes both in one multi-store transaction — a crash can never
   * leave pages deleted but metadata intact (or vice versa).
   */
  deleteAll(paths: string[]): Promise<void>;
}
