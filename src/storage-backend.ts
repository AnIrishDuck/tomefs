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

  /** Delete pages beyond a given index (for truncation). */
  deletePagesFrom(path: string, fromPageIndex: number): Promise<void>;

  /** Read file metadata. Returns null if the file doesn't exist. */
  readMeta(path: string): Promise<FileMeta | null>;

  /** Write file metadata. */
  writeMeta(path: string, meta: FileMeta): Promise<void>;

  /** Delete file metadata. */
  deleteMeta(path: string): Promise<void>;

  /** List all file paths that have metadata stored. */
  listFiles(): Promise<string[]>;
}
