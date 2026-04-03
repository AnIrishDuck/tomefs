import type { FileMeta } from "./types.js";

/**
 * Synchronous storage backend interface.
 *
 * Emscripten FS operations are synchronous (C-style), so tomefs needs
 * synchronous page access. This interface is the synchronous counterpart
 * to StorageBackend.
 *
 * - MemoryBackend implements this natively (all ops are Map lookups).
 * - IDB appears synchronous via the SAB+Atomics bridge (SabClient).
 */
export interface SyncStorageBackend {
  /** Read a single page. Returns null if the page doesn't exist. */
  readPage(path: string, pageIndex: number): Uint8Array | null;

  /** Read multiple pages in a single batch. Returns an array parallel to pageIndices. */
  readPages(
    path: string,
    pageIndices: number[],
  ): Array<Uint8Array | null>;

  /** Write a single page. */
  writePage(path: string, pageIndex: number, data: Uint8Array): void;

  /** Write multiple pages in a single batch. */
  writePages(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
  ): void;

  /** Delete all pages for a file. */
  deleteFile(path: string): void;

  /** Delete all pages for multiple files in a single batch. */
  deleteFiles(paths: string[]): void;

  /** Delete pages beyond a given index (for truncation). */
  deletePagesFrom(path: string, fromPageIndex: number): void;

  /** Read file metadata. Returns null if not found. */
  readMeta(path: string): FileMeta | null;

  /** Write file metadata. */
  writeMeta(path: string, meta: FileMeta): void;

  /** Write multiple metadata entries in a single batch. */
  writeMetas(entries: Array<{ path: string; meta: FileMeta }>): void;

  /** Delete file metadata. */
  deleteMeta(path: string): void;

  /** Read multiple metadata entries in a single batch. Returns an array parallel to paths. */
  readMetas(paths: string[]): Array<FileMeta | null>;

  /** Delete multiple metadata entries in a single batch. */
  deleteMetas(paths: string[]): void;

  /** Rename all pages from one path to another. */
  renameFile(oldPath: string, newPath: string): void;

  /** Count the number of pages stored for a file. Returns 0 if no pages exist. */
  countPages(path: string): number;

  /** Count pages for multiple files in a single batch. Returns an array parallel to paths. */
  countPagesBatch(paths: string[]): number[];

  /** Return the highest page index stored for a file, or -1 if no pages exist. */
  maxPageIndex(path: string): number;

  /** Return the highest page index for multiple files in a single batch. Returns an array parallel to paths. */
  maxPageIndexBatch(paths: string[]): number[];

  /** List all paths that have metadata stored. */
  listFiles(): string[];
}
