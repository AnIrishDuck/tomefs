import type { SyncStorageBackend } from "./sync-storage-backend.js";
import type { FileMeta } from "./types.js";
import { pageKeyStr } from "./types.js";

/**
 * Synchronous in-memory storage backend.
 *
 * Same behavior as MemoryBackend but with a synchronous interface,
 * suitable for use with SyncPageCache inside Emscripten FS operations.
 */
export class SyncMemoryBackend implements SyncStorageBackend {
  private pages = new Map<string, Uint8Array>();
  private meta = new Map<string, FileMeta>();

  /** Secondary index: file path → set of page keys belonging to that file.
   *  Avoids O(total-pages) full-map scans in deleteFile, renameFile, deletePagesFrom. */
  private filePageKeys = new Map<string, Set<string>>();

  /** Add a page key to the filePageKeys index. */
  private trackPage(path: string, key: string): void {
    let keys = this.filePageKeys.get(path);
    if (!keys) {
      keys = new Set();
      this.filePageKeys.set(path, keys);
    }
    keys.add(key);
  }

  readPage(path: string, pageIndex: number): Uint8Array | null {
    const key = pageKeyStr(path, pageIndex);
    const data = this.pages.get(key);
    return data ? new Uint8Array(data) : null;
  }

  readPages(path: string, pageIndices: number[]): Array<Uint8Array | null> {
    return pageIndices.map((i) => this.readPage(path, i));
  }

  writePage(path: string, pageIndex: number, data: Uint8Array): void {
    const key = pageKeyStr(path, pageIndex);
    this.pages.set(key, new Uint8Array(data));
    this.trackPage(path, key);
  }

  writePages(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
  ): void {
    for (const { path, pageIndex, data } of pages) {
      this.writePage(path, pageIndex, data);
    }
  }

  deleteFile(path: string): void {
    const keys = this.filePageKeys.get(path);
    if (keys) {
      for (const key of keys) {
        this.pages.delete(key);
      }
      this.filePageKeys.delete(path);
    }
  }

  deleteFiles(paths: string[]): void {
    for (const path of paths) {
      this.deleteFile(path);
    }
  }

  deletePagesFrom(path: string, fromPageIndex: number): void {
    const keys = this.filePageKeys.get(path);
    if (!keys) return;
    for (const key of keys) {
      const page = this.pages.get(key);
      if (page) {
        const nullIdx = key.indexOf("\0");
        const idx = parseInt(key.substring(nullIdx + 1), 10);
        if (idx >= fromPageIndex) {
          this.pages.delete(key);
          keys.delete(key);
        }
      }
    }
    if (keys.size === 0) this.filePageKeys.delete(path);
  }

  countPages(path: string): number {
    return this.filePageKeys.get(path)?.size ?? 0;
  }

  maxPageIndex(path: string): number {
    const keys = this.filePageKeys.get(path);
    if (!keys || keys.size === 0) return -1;
    let max = -1;
    for (const key of keys) {
      const nullIdx = key.indexOf("\0");
      const idx = parseInt(key.substring(nullIdx + 1), 10);
      if (idx > max) max = idx;
    }
    return max;
  }

  renameFile(oldPath: string, newPath: string): void {
    if (oldPath === newPath) return;
    // Clear destination pages first to avoid orphans when source has fewer
    // pages than destination (same contract as IDB and OPFS backends).
    this.deleteFile(newPath);

    const oldKeys = this.filePageKeys.get(oldPath);
    if (!oldKeys) return;
    const oldPrefix = `${oldPath}\0`;
    const toAdd: Array<[string, Uint8Array]> = [];
    for (const key of oldKeys) {
      const data = this.pages.get(key)!;
      const pageIndex = key.slice(oldPrefix.length);
      toAdd.push([`${newPath}\0${pageIndex}`, data]);
      this.pages.delete(key);
    }
    this.filePageKeys.delete(oldPath);
    for (const [key, data] of toAdd) {
      this.pages.set(key, data);
      this.trackPage(newPath, key);
    }
  }

  readMeta(path: string): FileMeta | null {
    const m = this.meta.get(path);
    return m ? { ...m } : null;
  }

  readMetas(paths: string[]): Array<FileMeta | null> {
    return paths.map((path) => this.readMeta(path));
  }

  writeMeta(path: string, meta: FileMeta): void {
    this.meta.set(path, { ...meta });
  }

  writeMetas(entries: Array<{ path: string; meta: FileMeta }>): void {
    for (const { path, meta } of entries) {
      this.meta.set(path, { ...meta });
    }
  }

  deleteMeta(path: string): void {
    this.meta.delete(path);
  }

  deleteMetas(paths: string[]): void {
    for (const path of paths) {
      this.meta.delete(path);
    }
  }

  listFiles(): string[] {
    return [...this.meta.keys()];
  }
}
