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

/** Encode a virtual file path as a hex string safe for use as an OPFS name. */
function encodePath(path: string): string {
  const bytes = new TextEncoder().encode(path);
  let hex = "";
  for (const b of bytes) {
    hex += b.toString(16).padStart(2, "0");
  }
  return hex;
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
    })();

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
    await Promise.all(
      pages.map(({ path, pageIndex, data }) =>
        this.writePage(path, pageIndex, data),
      ),
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

  async renameFile(oldPath: string, newPath: string): Promise<void> {
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

    for (const name of toRemove) {
      await fileDir.removeEntry(name);
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
    return JSON.parse(text) as FileMeta;
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

  async deleteMeta(path: string): Promise<void> {
    await this.init();
    const encoded = encodePath(path);
    try {
      await this.metaDir!.removeEntry(encoded);
    } catch (err) {
      if (!isNotFoundError(err)) throw err;
    }
  }

  async listFiles(): Promise<string[]> {
    await this.init();
    const paths: string[] = [];
    for await (const name of (this.metaDir as IterableDirectoryHandle).keys()) {
      paths.push(decodePath(name));
    }
    return paths;
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
