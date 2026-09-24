import {
  type IterableDirectoryHandle,
  isNotFoundError,
  PAGES_DIR,
  META_DIR,
  encodePath,
} from "./opfs-utils.js";
import { OpfsBackendBase } from "./opfs-backend-base.js";

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
export class OpfsBackend extends OpfsBackendBase {
  constructor(options?: OpfsBackendOptions) {
    super(options?.root);
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

  async readPageBatch(
    entries: Array<{ path: string; pageIndex: number }>,
  ): Promise<Array<Uint8Array | null>> {
    if (entries.length === 0) return [];
    return Promise.all(
      entries.map(({ path, pageIndex }) => this.readPage(path, pageIndex)),
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

  async cleanupOrphanedPages(): Promise<number> {
    await this.init();

    const metaNames = new Set<string>();
    for await (const name of (this.metaDir as IterableDirectoryHandle).keys()) {
      metaNames.add(name);
    }

    const orphans: string[] = [];
    for await (const name of (this.pagesDir as IterableDirectoryHandle).keys()) {
      if (!metaNames.has(name)) {
        orphans.push(name);
      }
    }

    for (const name of orphans) {
      try {
        await this.pagesDir!.removeEntry(name, { recursive: true });
      } catch (err) {
        if (!isNotFoundError(err)) throw err;
      }
    }

    return orphans.length;
  }

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
