import type { StorageBackend } from "./storage-backend.js";
import type { FileMeta } from "./types.js";
import { pageKeyStr } from "./types.js";

/**
 * In-memory storage backend for testing.
 *
 * Stores pages and metadata in Maps. No persistence — data is lost when
 * the instance is garbage collected. This is a fake (not a mock) per
 * project conventions.
 */
export class MemoryBackend implements StorageBackend {
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

  async readPage(path: string, pageIndex: number): Promise<Uint8Array | null> {
    const key = pageKeyStr(path, pageIndex);
    const data = this.pages.get(key);
    return data ? new Uint8Array(data) : null;
  }

  async readPages(
    path: string,
    pageIndices: number[],
  ): Promise<Array<Uint8Array | null>> {
    return pageIndices.map((i) => {
      const key = pageKeyStr(path, i);
      const data = this.pages.get(key);
      return data ? new Uint8Array(data) : null;
    });
  }

  async writePage(
    path: string,
    pageIndex: number,
    data: Uint8Array,
  ): Promise<void> {
    const key = pageKeyStr(path, pageIndex);
    this.pages.set(key, new Uint8Array(data));
    this.trackPage(path, key);
  }

  async writePages(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
  ): Promise<void> {
    for (const { path, pageIndex, data } of pages) {
      await this.writePage(path, pageIndex, data);
    }
  }

  async deleteFile(path: string): Promise<void> {
    const keys = this.filePageKeys.get(path);
    if (keys) {
      for (const key of keys) {
        this.pages.delete(key);
      }
      this.filePageKeys.delete(path);
    }
  }

  async deleteFiles(paths: string[]): Promise<void> {
    for (const path of paths) {
      await this.deleteFile(path);
    }
  }

  async deletePagesFrom(path: string, fromPageIndex: number): Promise<void> {
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

  async countPages(path: string): Promise<number> {
    return this.filePageKeys.get(path)?.size ?? 0;
  }

  async maxPageIndex(path: string): Promise<number> {
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

  async renameFile(oldPath: string, newPath: string): Promise<void> {
    // Clear destination pages first to avoid orphans when source has fewer
    // pages than destination (same contract as IDB and OPFS backends).
    await this.deleteFile(newPath);

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

  async readMeta(path: string): Promise<FileMeta | null> {
    const meta = this.meta.get(path);
    return meta ? { ...meta } : null;
  }

  async readMetas(paths: string[]): Promise<Array<FileMeta | null>> {
    return paths.map((path) => {
      const meta = this.meta.get(path);
      return meta ? { ...meta } : null;
    });
  }

  async writeMeta(path: string, meta: FileMeta): Promise<void> {
    this.meta.set(path, { ...meta });
  }

  async writeMetas(
    entries: Array<{ path: string; meta: FileMeta }>,
  ): Promise<void> {
    for (const { path, meta } of entries) {
      this.meta.set(path, { ...meta });
    }
  }

  async deleteMeta(path: string): Promise<void> {
    this.meta.delete(path);
  }

  async deleteMetas(paths: string[]): Promise<void> {
    for (const path of paths) {
      this.meta.delete(path);
    }
  }

  async listFiles(): Promise<string[]> {
    return Array.from(this.meta.keys());
  }
}
