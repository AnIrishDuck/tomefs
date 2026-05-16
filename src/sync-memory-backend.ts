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

  /** Secondary index: file path → Map<pageIndex, key>.
   *  Avoids string parsing (indexOf + parseInt) in maxPageIndex and deletePagesFrom. */
  private filePageIndices = new Map<string, Map<number, string>>();

  /** Cached max page index per file for O(1) maxPageIndex lookups.
   *  Updated on write/delete; -1 means no pages exist. */
  private fileMaxIdx = new Map<string, number>();

  /** Add a page key to the secondary indexes. */
  private trackPage(path: string, key: string, pageIndex: number): void {
    let keys = this.filePageKeys.get(path);
    if (!keys) {
      keys = new Set();
      this.filePageKeys.set(path, keys);
    }
    keys.add(key);

    let indices = this.filePageIndices.get(path);
    if (!indices) {
      indices = new Map();
      this.filePageIndices.set(path, indices);
    }
    indices.set(pageIndex, key);

    const cur = this.fileMaxIdx.get(path) ?? -1;
    if (pageIndex > cur) {
      this.fileMaxIdx.set(path, pageIndex);
    }
  }

  readPage(path: string, pageIndex: number): Uint8Array | null {
    const key = pageKeyStr(path, pageIndex);
    const data = this.pages.get(key);
    return data ? new Uint8Array(data) : null;
  }

  readPages(path: string, pageIndices: number[]): Array<Uint8Array | null> {
    if (pageIndices.length === 0) return [];
    const prefix = path + "\0";
    return pageIndices.map((i) => {
      const data = this.pages.get(prefix + i);
      return data ? new Uint8Array(data) : null;
    });
  }

  writePage(path: string, pageIndex: number, data: Uint8Array): void {
    const key = pageKeyStr(path, pageIndex);
    this.pages.set(key, new Uint8Array(data));
    this.trackPage(path, key, pageIndex);
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
    this.filePageIndices.delete(path);
    this.fileMaxIdx.delete(path);
  }

  deleteFiles(paths: string[]): void {
    for (const path of paths) {
      this.deleteFile(path);
    }
  }

  deletePagesFrom(path: string, fromPageIndex: number): void {
    const indices = this.filePageIndices.get(path);
    if (!indices) return;
    const keys = this.filePageKeys.get(path);
    for (const [idx, key] of indices) {
      if (idx >= fromPageIndex) {
        this.pages.delete(key);
        keys?.delete(key);
        indices.delete(idx);
      }
    }
    if (indices.size === 0) {
      this.filePageIndices.delete(path);
      this.filePageKeys.delete(path);
      this.fileMaxIdx.delete(path);
    } else {
      const prevMax = this.fileMaxIdx.get(path) ?? -1;
      if (prevMax >= fromPageIndex) {
        let newMax = -1;
        for (const idx of indices.keys()) {
          if (idx > newMax) newMax = idx;
        }
        this.fileMaxIdx.set(path, newMax);
      }
    }
  }

  countPages(path: string): number {
    return this.filePageKeys.get(path)?.size ?? 0;
  }

  countPagesBatch(paths: string[]): number[] {
    return paths.map((path) => this.filePageKeys.get(path)?.size ?? 0);
  }

  maxPageIndex(path: string): number {
    return this.fileMaxIdx.get(path) ?? -1;
  }

  maxPageIndexBatch(paths: string[]): number[] {
    return paths.map((path) => this.maxPageIndex(path));
  }

  renameFile(oldPath: string, newPath: string): void {
    if (oldPath === newPath) return;
    // Clear destination pages first to avoid orphans when source has fewer
    // pages than destination (same contract as IDB and OPFS backends).
    this.deleteFile(newPath);

    const oldIndices = this.filePageIndices.get(oldPath);
    if (!oldIndices) return;
    const oldMax = this.fileMaxIdx.get(oldPath) ?? -1;
    const toAdd: Array<[number, string, Uint8Array]> = [];
    for (const [pageIndex, key] of oldIndices) {
      const data = this.pages.get(key)!;
      const newKey = pageKeyStr(newPath, pageIndex);
      toAdd.push([pageIndex, newKey, data]);
      this.pages.delete(key);
    }
    this.filePageKeys.delete(oldPath);
    this.filePageIndices.delete(oldPath);
    this.fileMaxIdx.delete(oldPath);
    for (const [pageIndex, key, data] of toAdd) {
      this.pages.set(key, data);
      this.trackPage(newPath, key, pageIndex);
    }
    if (oldMax >= 0) {
      this.fileMaxIdx.set(newPath, oldMax);
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

  syncAll(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
    metas: Array<{ path: string; meta: FileMeta }>,
  ): void {
    this.writePages(pages);
    this.writeMetas(metas);
  }

  deleteAll(paths: string[]): void {
    this.deleteFiles(paths);
    this.deleteMetas(paths);
  }
}
