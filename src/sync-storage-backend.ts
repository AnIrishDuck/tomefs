import type { FileMeta } from "./types.js";

/**
 * Synchronous storage backend interface.
 *
 * Emscripten FS operations are synchronous (C-style), so tomefs needs
 * synchronous page access. This interface is the synchronous counterpart
 * to StorageBackend.
 *
 * - MemoryBackend implements this natively (all ops are Map lookups).
 * - IDB will appear synchronous via the SAB+Atomics bridge (future).
 */
export interface SyncStorageBackend {
  /** Read a single page. Returns null if the page doesn't exist. */
  readPage(path: string, pageIndex: number): Uint8Array | null;

  /** Write a single page. */
  writePage(path: string, pageIndex: number, data: Uint8Array): void;

  /** Write multiple pages in a single batch. */
  writePages(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
  ): void;

  /** Delete all pages for a file. */
  deleteFile(path: string): void;

  /** Delete pages beyond a given index (for truncation). */
  deletePagesFrom(path: string, fromPageIndex: number): void;

  /** Read file metadata. Returns null if not found. */
  readMeta(path: string): FileMeta | null;

  /** Write file metadata. */
  writeMeta(path: string, meta: FileMeta): void;

  /** Delete file metadata. */
  deleteMeta(path: string): void;

  /** List all paths that have metadata stored. */
  listFiles(): string[];
}
