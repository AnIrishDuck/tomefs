import type { StorageBackend } from "./storage-backend.js";
import type { FileMeta } from "./types.js";

/**
 * FileSystemDirectoryHandle with async iterable methods.
 * TypeScript's DOM lib omits keys()/values()/entries() even though
 * they are part of the spec.
 */
interface IterableDirectoryHandle extends FileSystemDirectoryHandle {
  keys(): AsyncIterableIterator<string>;
}

/** Return true if the error is a DOMException with name "NotFoundError". */
function isNotFoundError(err: unknown): boolean {
  return err instanceof DOMException && err.name === "NotFoundError";
}

/** Subdirectory names within the OPFS root. */
const PAGES_DIR = "pages";
const META_DIR = "meta";

/** Precomputed hex lookup table — avoids per-byte toString(16) + padStart. */
const HEX_TABLE: string[] = new Array(256);
for (let i = 0; i < 256; i++) {
  HEX_TABLE[i] = i.toString(16).padStart(2, "0");
}

/** Encode a virtual file path as a hex string safe for use as an OPFS name. */
function encodePath(path: string): string {
  const bytes = new TextEncoder().encode(path);
  const parts = new Array<string>(bytes.length);
  for (let i = 0; i < bytes.length; i++) {
    parts[i] = HEX_TABLE[bytes[i]];
  }
  return parts.join("");
}

/** Decode a hex-encoded OPFS name back to the original virtual file path. */
function decodePath(hex: string): string {
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return new TextDecoder().decode(bytes);
}

/** Options for creating an OpfsBackend. */
export interface OpfsBackendOptions {
  /**
   * Root OPFS directory handle. If not provided, uses
   * `navigator.storage.getDirectory()` on first access.
   */
  root?: FileSystemDirectoryHandle;
}

/**
 * Origin Private File System (OPFS) storage backend.
 *
 * Stores file pages and metadata in two subdirectories under the root:
 * - `pages/<hex-path>/<pageIndex>` — one OPFS file per page
 * - `meta/<hex-path>` — JSON metadata per virtual file
 *
 * Each virtual file gets its own OPFS directory under `pages/`, making
 * `deleteFile` a single recursive removal.
 */
export class OpfsBackend implements StorageBackend {
  private root: FileSystemDirectoryHandle | null;
  private pagesDir: FileSystemDirectoryHandle | null = null;
  private metaDir: FileSystemDirectoryHandle | null = null;
  private initPromise: Promise<void> | null = null;

  constructor(options?: OpfsBackendOptions) {
    this.root = options?.root ?? null;
  }

  /** Lazily initialize the root and subdirectories. */
  private async init(): Promise<void> {
    if (this.pagesDir && this.metaDir) return;
    if (this.initPromise) return this.initPromise;

    this.initPromise = (async () => {
      if (!this.root) {
        this.root = await navigator.storage.getDirectory();
      }
      this.pagesDir = await this.root.getDirectoryHandle(PAGES_DIR, {
        create: true,
      });
      this.metaDir = await this.root.getDirectoryHandle(META_DIR, {
        create: true,
      });
    })().catch((err) => {
      // Clear the cached promise so a subsequent init() call can retry
      // instead of returning the same rejected promise forever.
      this.initPromise = null;
      throw err;
    });

    return this.initPromise;
  }

  /**
   * Get or create the per-file page directory.
   * Returns null if create is false and the directory doesn't exist.
   */
  private async getFileDir(
    path: string,
    create: boolean,
  ): Promise<FileSystemDirectoryHandle | null> {
    await this.init();
    const encoded = encodePath(path);
    try {
      return await this.pagesDir!.getDirectoryHandle(encoded, { create });
    } catch (err) {
      if (isNotFoundError(err)) return null;
      throw err;
    }
  }

  async readPage(
    path: string,
    pageIndex: number,
  ): Promise<Uint8Array | null> {
    const fileDir = await this.getFileDir(path, false);
    if (!fileDir) return null;

    let handle: FileSystemFileHandle;
    try {
      handle = await fileDir.getFileHandle(String(pageIndex));
    } catch (err) {
      if (isNotFoundError(err)) return null;
      throw err;
    }

    const file = await handle.getFile();
    const buffer = await file.arrayBuffer();
    return new Uint8Array(buffer);
  }

  async readPages(
    path: string,
    pageIndices: number[],
  ): Promise<Array<Uint8Array | null>> {
    const fileDir = await this.getFileDir(path, false);
    if (!fileDir) return pageIndices.map(() => null);

    return Promise.all(
      pageIndices.map(async (pageIndex) => {
        let handle: FileSystemFileHandle;
        try {
          handle = await fileDir.getFileHandle(String(pageIndex));
        } catch (err) {
          if (isNotFoundError(err)) return null;
          throw err;
        }
        const file = await handle.getFile();
        const buffer = await file.arrayBuffer();
        return new Uint8Array(buffer);
      }),
    );
  }

  async writePage(
    path: string,
    pageIndex: number,
    data: Uint8Array,
  ): Promise<void> {
    const fileDir = await this.getFileDir(path, true);
    const handle = await fileDir!.getFileHandle(String(pageIndex), {
      create: true,
    });
    const writable = await handle.createWritable();
    try {
      await writable.write(new Uint8Array(data));
    } finally {
      await writable.close();
    }
  }

  async writePages(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
  ): Promise<void> {
    if (pages.length === 0) return;
    if (pages.length === 1) {
      await this.writePage(pages[0].path, pages[0].pageIndex, pages[0].data);
      return;
    }

    // Group pages by file path so each unique path gets a single
    // getFileDir call instead of one per page. During syncfs, many
    // dirty pages often belong to the same file (e.g., WAL, heap),
    // so this reduces redundant OPFS directory handle lookups.
    const byPath = new Map<
      string,
      Array<{ pageIndex: number; data: Uint8Array }>
    >();
    for (const { path, pageIndex, data } of pages) {
      let group = byPath.get(path);
      if (!group) {
        group = [];
        byPath.set(path, group);
      }
      group.push({ pageIndex, data });
    }

    await Promise.all(
      [...byPath.entries()].map(async ([path, group]) => {
        const fileDir = await this.getFileDir(path, true);
        await Promise.all(
          group.map(async ({ pageIndex, data }) => {
            const handle = await fileDir!.getFileHandle(String(pageIndex), {
              create: true,
            });
            const writable = await handle.createWritable();
            try {
              await writable.write(new Uint8Array(data));
            } finally {
              await writable.close();
            }
          }),
        );
      }),
    );
  }

  async deleteFile(path: string): Promise<void> {
    await this.init();
    const encoded = encodePath(path);
    try {
      await this.pagesDir!.removeEntry(encoded, { recursive: true });
    } catch (err) {
      if (!isNotFoundError(err)) throw err;
    }
  }

  async countPages(path: string): Promise<number> {
    await this.init();
    const encoded = encodePath(path);
    try {
      const fileDir = (await this.pagesDir!.getDirectoryHandle(
        encoded,
      )) as IterableDirectoryHandle;
      let count = 0;
      for await (const _ of fileDir.keys()) {
        count++;
      }
      return count;
    } catch (err) {
      if (isNotFoundError(err)) return 0;
      throw err;
    }
  }

  async countPagesBatch(paths: string[]): Promise<number[]> {
    if (paths.length === 0) return [];
    await this.init();
    return Promise.all(paths.map((path) => this.countPages(path)));
  }

  async maxPageIndex(path: string): Promise<number> {
    await this.init();
    const encoded = encodePath(path);
    try {
      const fileDir = (await this.pagesDir!.getDirectoryHandle(
        encoded,
      )) as IterableDirectoryHandle;
      let max = -1;
      for await (const name of fileDir.keys()) {
        const idx = parseInt(name, 10);
        if (idx > max) max = idx;
      }
      return max;
    } catch (err) {
      if (isNotFoundError(err)) return -1;
      throw err;
    }
  }

  async maxPageIndexBatch(paths: string[]): Promise<number[]> {
    if (paths.length === 0) return [];
    await this.init();
    return Promise.all(paths.map((path) => this.maxPageIndex(path)));
  }

  async deleteFiles(paths: string[]): Promise<void> {
    if (paths.length === 0) return;
    if (paths.length === 1) {
      await this.deleteFile(paths[0]);
      return;
    }
    await Promise.all(paths.map((path) => this.deleteFile(path)));
  }

  async renameFile(oldPath: string, newPath: string): Promise<void> {
    if (oldPath === newPath) return;
    await this.init();
    const oldEncoded = encodePath(oldPath);
    const newEncoded = encodePath(newPath);

    // Remove destination if it already exists (overwrite semantics).
    try {
      await this.pagesDir!.removeEntry(newEncoded, { recursive: true });
    } catch (err) {
      if (!isNotFoundError(err)) throw err;
    }

    // Move page files from old directory to new directory.
    let oldDir: FileSystemDirectoryHandle;
    try {
      oldDir = await this.pagesDir!.getDirectoryHandle(oldEncoded);
    } catch (err) {
      if (isNotFoundError(err)) return;
      throw err;
    }

    const newDir = await this.pagesDir!.getDirectoryHandle(newEncoded, {
      create: true,
    });

    // Collect page names, then copy all in parallel.
    const names: string[] = [];
    for await (const name of (oldDir as IterableDirectoryHandle).keys()) {
      names.push(name);
    }

    try {
      await Promise.all(
        names.map(async (name) => {
          const srcHandle = await oldDir.getFileHandle(name);
          const file = await srcHandle.getFile();
          const data = await file.arrayBuffer();

          const dstHandle = await newDir.getFileHandle(name, { create: true });
          const writable = await dstHandle.createWritable();
          try {
            await writable.write(new Uint8Array(data));
          } finally {
            await writable.close();
          }
        }),
      );
    } catch (err) {
      // Copy failed — clean up the partial new directory so we don't
      // leave orphaned pages. The old directory is still intact.
      try {
        await this.pagesDir!.removeEntry(newEncoded, { recursive: true });
      } catch (cleanupErr) {
        if (!isNotFoundError(cleanupErr)) {
          // Surface both the original error and the cleanup failure
          throw new Error(
            `OPFS renameFile: copy failed (${err instanceof Error ? err.message : String(err)}) ` +
              `and cleanup also failed (${cleanupErr instanceof Error ? cleanupErr.message : String(cleanupErr)})`,
          );
        }
      }
      throw err;
    }

    // Verify all pages were copied before deleting the source.
    // Without this check, a partial copy failure would cause data loss
    // when the old directory is removed.
    const copiedNames: string[] = [];
    for await (const name of (newDir as IterableDirectoryHandle).keys()) {
      copiedNames.push(name);
    }
    if (copiedNames.length < names.length) {
      // Verification failed — clean up the partial new directory.
      try {
        await this.pagesDir!.removeEntry(newEncoded, { recursive: true });
      } catch (cleanupErr) {
        if (!isNotFoundError(cleanupErr)) {
          throw new Error(
            `OPFS renameFile: copy verification failed (expected ${names.length} pages but found ${copiedNames.length}) ` +
              `and cleanup also failed (${cleanupErr instanceof Error ? cleanupErr.message : String(cleanupErr)})`,
          );
        }
      }
      throw new Error(
        `OPFS renameFile: copy verification failed — expected ${names.length} pages but found ${copiedNames.length}`,
      );
    }

    // Remove old pages directory.
    await this.pagesDir!.removeEntry(oldEncoded, { recursive: true });
  }

  async deletePagesFrom(
    path: string,
    fromPageIndex: number,
  ): Promise<void> {
    const fileDir = await this.getFileDir(path, false);
    if (!fileDir) return;

    const toRemove: string[] = [];
    for await (const name of (fileDir as IterableDirectoryHandle).keys()) {
      const idx = parseInt(name, 10);
      if (idx >= fromPageIndex) {
        toRemove.push(name);
      }
    }

    const results = await Promise.allSettled(
      toRemove.map((name) => fileDir.removeEntry(name)),
    );
    const failures = results.filter(
      (r): r is PromiseRejectedResult => r.status === "rejected",
    );
    if (failures.length > 0) {
      throw new Error(
        `OPFS deletePagesFrom: ${failures.length}/${toRemove.length} page removals failed: ${failures[0].reason}`,
      );
    }
  }

  async readMeta(path: string): Promise<FileMeta | null> {
    await this.init();
    const encoded = encodePath(path);

    let handle: FileSystemFileHandle;
    try {
      handle = await this.metaDir!.getFileHandle(encoded);
    } catch (err) {
      if (isNotFoundError(err)) return null;
      throw err;
    }

    const file = await handle.getFile();
    const text = await file.text();
    try {
      return JSON.parse(text) as FileMeta;
    } catch {
      // Corrupted metadata (e.g., partial write on tab close).
      // Treat as missing rather than crashing the filesystem.
      return null;
    }
  }

  async readMetas(paths: string[]): Promise<Array<FileMeta | null>> {
    if (paths.length === 0) return [];
    if (paths.length === 1) {
      return [await this.readMeta(paths[0])];
    }
    return Promise.all(paths.map((path) => this.readMeta(path)));
  }

  async writeMeta(path: string, meta: FileMeta): Promise<void> {
    await this.init();
    const encoded = encodePath(path);
    const handle = await this.metaDir!.getFileHandle(encoded, {
      create: true,
    });
    const writable = await handle.createWritable();
    try {
      await writable.write(JSON.stringify(meta));
    } finally {
      await writable.close();
    }
  }

  async writeMetas(
    entries: Array<{ path: string; meta: FileMeta }>,
  ): Promise<void> {
    if (entries.length === 0) return;
    if (entries.length === 1) {
      await this.writeMeta(entries[0].path, entries[0].meta);
      return;
    }
    await Promise.all(
      entries.map(({ path, meta }) => this.writeMeta(path, meta)),
    );
  }

  async deleteMeta(path: string): Promise<void> {
    await this.init();
    const encoded = encodePath(path);
    try {
      await this.metaDir!.removeEntry(encoded);
    } catch (err) {
      if (!isNotFoundError(err)) throw err;
    }
  }

  async deleteMetas(paths: string[]): Promise<void> {
    if (paths.length === 0) return;
    if (paths.length === 1) {
      await this.deleteMeta(paths[0]);
      return;
    }
    await Promise.all(paths.map((path) => this.deleteMeta(path)));
  }

  async listFiles(): Promise<string[]> {
    await this.init();
    const paths: string[] = [];
    for await (const name of (this.metaDir as IterableDirectoryHandle).keys()) {
      paths.push(decodePath(name));
    }
    return paths;
  }

  async syncAll(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
    metas: Array<{ path: string; meta: FileMeta }>,
  ): Promise<void> {
    // OPFS has no multi-operation transactions, so execute sequentially.
    // Write metadata BEFORE pages so that a crash mid-sync leaves metadata
    // pointing at stale/missing pages (recoverable via maxPageIndex on
    // remount) rather than orphaned page directories with no metadata
    // (permanently leaked storage, invisible to listFiles/orphan cleanup).
    // IDB doesn't have this problem — its syncAll uses a single atomic
    // multi-store transaction.
    await this.writeMetas(metas);
    await this.writePages(pages);
  }

  /**
   * Remove page directories that have no corresponding metadata entry.
   *
   * OPFS syncAll is non-atomic: if the process crashes after writing pages
   * but before writing metadata (or before this fix, which reversed the
   * ordering), page directories can be left behind with no way to discover
   * them — listFiles() only iterates metadata entries.
   *
   * This method cross-references the pages/ and meta/ directories and
   * removes any pages/<hex-path> directory that has no matching
   * meta/<hex-path> entry. Safe to call at any time; no-op if there are
   * no orphans.
   *
   * Returns the number of orphaned page directories removed.
   */
  async cleanupOrphanedPages(): Promise<number> {
    await this.init();

    // Collect all hex-encoded names that have metadata entries.
    const metaNames = new Set<string>();
    for await (const name of (this.metaDir as IterableDirectoryHandle).keys()) {
      metaNames.add(name);
    }

    // Find page directories with no corresponding metadata.
    const orphans: string[] = [];
    for await (const name of (this.pagesDir as IterableDirectoryHandle).keys()) {
      if (!metaNames.has(name)) {
        orphans.push(name);
      }
    }

    // Remove orphaned page directories.
    for (const name of orphans) {
      try {
        await this.pagesDir!.removeEntry(name, { recursive: true });
      } catch (err) {
        if (!isNotFoundError(err)) throw err;
      }
    }

    return orphans.length;
  }

  async deleteAll(paths: string[]): Promise<void> {
    if (paths.length === 0) return;
    // Delete metadata first so files become invisible to listFiles().
    // If a crash occurs between the two operations, orphaned page
    // directories (pages without metadata) are cleaned up by
    // cleanupOrphanedPages(). The reverse order would leave stale
    // metadata pointing at deleted pages — invisible to orphan cleanup.
    await this.deleteMetas(paths);
    await this.deleteFiles(paths);
  }

  /**
   * Remove all data and metadata. The backend should not be used after this.
   */
  async destroy(): Promise<void> {
    await this.init();
    try {
      await this.root!.removeEntry(PAGES_DIR, { recursive: true });
    } catch (err) {
      if (!isNotFoundError(err)) throw err;
    }
    try {
      await this.root!.removeEntry(META_DIR, { recursive: true });
    } catch (err) {
      if (!isNotFoundError(err)) throw err;
    }
    this.pagesDir = null;
    this.metaDir = null;
    this.initPromise = null;
  }
}
