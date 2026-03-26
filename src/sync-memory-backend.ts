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
  }

  writePages(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
  ): void {
    for (const { path, pageIndex, data } of pages) {
      this.writePage(path, pageIndex, data);
    }
  }

  deleteFile(path: string): void {
    const prefix = `${path}\0`;
    for (const key of this.pages.keys()) {
      if (key.startsWith(prefix)) {
        this.pages.delete(key);
      }
    }
  }

  deletePagesFrom(path: string, fromPageIndex: number): void {
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

  renameFile(oldPath: string, newPath: string): void {
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

  readMeta(path: string): FileMeta | null {
    return this.meta.get(path) ?? null;
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
