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
  }

  async writePages(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
  ): Promise<void> {
    for (const { path, pageIndex, data } of pages) {
      await this.writePage(path, pageIndex, data);
    }
  }

  async deleteFile(path: string): Promise<void> {
    const prefix = `${path}\0`;
    for (const key of this.pages.keys()) {
      if (key.startsWith(prefix)) {
        this.pages.delete(key);
      }
    }
  }

  async deletePagesFrom(path: string, fromPageIndex: number): Promise<void> {
    const prefix = `${path}\0`;
    for (const key of this.pages.keys()) {
      if (key.startsWith(prefix)) {
        const idx = parseInt(key.slice(prefix.length), 10);
        if (idx >= fromPageIndex) {
          this.pages.delete(key);
        }
      }
    }
  }

  async countPages(path: string): Promise<number> {
    const prefix = `${path}\0`;
    let count = 0;
    for (const key of this.pages.keys()) {
      if (key.startsWith(prefix)) {
        count++;
      }
    }
    return count;
  }

  async renameFile(oldPath: string, newPath: string): Promise<void> {
    const prefix = `${oldPath}\0`;
    const toAdd: Array<[string, Uint8Array]> = [];
    for (const [key, data] of this.pages.entries()) {
      if (key.startsWith(prefix)) {
        const pageIndex = key.slice(prefix.length);
        toAdd.push([`${newPath}\0${pageIndex}`, data]);
        this.pages.delete(key);
      }
    }
    for (const [key, data] of toAdd) {
      this.pages.set(key, data);
    }
  }

  async readMeta(path: string): Promise<FileMeta | null> {
    const meta = this.meta.get(path);
    return meta ? { ...meta } : null;
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
