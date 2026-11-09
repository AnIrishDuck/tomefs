import type { StorageBackend } from "./storage-backend.js";
import type { FileMeta } from "./types.js";
import {
  type IterableDirectoryHandle,
  isNotFoundError,
  PAGES_DIR,
  META_DIR,
  encodePath,
  decodePath,
} from "./opfs-utils.js";

/**
 * Shared base for OPFS storage backends.
 *
 * Both OpfsBackend (directory-per-file) and OpfsSahBackend
 * (SyncAccessHandle) use identical metadata operations and
 * directory layout. This base class provides them once.
 */
export abstract class OpfsBackendBase implements StorageBackend {
  protected root: FileSystemDirectoryHandle | null;
  protected pagesDir: FileSystemDirectoryHandle | null = null;
  protected metaDir: FileSystemDirectoryHandle | null = null;
  protected initPromise: Promise<void> | null = null;

  constructor(root?: FileSystemDirectoryHandle) {
    this.root = root ?? null;
  }

  protected async init(): Promise<void> {
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
      this.initPromise = null;
      throw err;
    });

    return this.initPromise;
  }

  abstract readPage(
    path: string,
    pageIndex: number,
  ): Promise<Uint8Array | null>;

  abstract readPages(
    path: string,
    pageIndices: number[],
  ): Promise<Array<Uint8Array | null>>;

  abstract readPageBatch(
    entries: Array<{ path: string; pageIndex: number }>,
  ): Promise<Array<Uint8Array | null>>;

  abstract writePage(
    path: string,
    pageIndex: number,
    data: Uint8Array,
  ): Promise<void>;

  abstract writePages(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
  ): Promise<void>;

  abstract deleteFile(path: string): Promise<void>;

  abstract deleteFiles(paths: string[]): Promise<void>;

  abstract deletePagesFrom(
    path: string,
    fromPageIndex: number,
  ): Promise<void>;

  abstract renameFile(oldPath: string, newPath: string): Promise<void>;

  abstract countPages(path: string): Promise<number>;

  abstract countPagesBatch(paths: string[]): Promise<number[]>;

  abstract maxPageIndex(path: string): Promise<number>;

  abstract maxPageIndexBatch(paths: string[]): Promise<number[]>;

  abstract cleanupOrphanedPages(): Promise<number>;

  abstract destroy(): Promise<void>;

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
    if (paths.length === 1) return [await this.readMeta(paths[0])];
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
    // Write pages BEFORE metadata so that a crash mid-sync leaves orphaned
    // pages (cleaned up by cleanupOrphanedPages on next mount) rather than
    // metadata pointing at stale page content. The metadata batch includes
    // the clean-shutdown marker — writing it before pages would tell the
    // next mount the backend is consistent when page data is stale, causing
    // silent data corruption. Pages-first ensures the marker is absent
    // after a mid-sync crash, forcing a full recovery pass.
    await this.writePages(pages);
    await this.writeMetas(metas);
  }

  async deleteAll(paths: string[]): Promise<void> {
    if (paths.length === 0) return;
    // Delete metadata BEFORE pages. OPFS has no multi-operation transactions,
    // so a crash between the two phases is possible. Metadata-first ensures a
    // crash leaves orphaned pages (cleaned up by cleanupOrphanedPages) rather
    // than ghost metadata entries (files visible in listFiles with no data).
    await this.deleteMetas(paths);
    await this.deleteFiles(paths);
  }
}
