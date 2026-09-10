import { PAGE_SIZE } from "./types.js";
import {
  type IterableDirectoryHandle,
  isNotFoundError,
  PAGES_DIR,
  META_DIR,
  encodePath,
} from "./opfs-utils.js";
import { OpfsBackendBase } from "./opfs-backend-base.js";

// FileSystemSyncAccessHandle is declared in src/opfs-augments.d.ts
// (the Web API lives in lib.webworker.d.ts but this project only
// includes lib.dom.d.ts).
type SyncAccessHandle = FileSystemSyncAccessHandle;

export interface OpfsSahBackendOptions {
  root?: FileSystemDirectoryHandle;
  maxOpenHandles?: number;
}

/**
 * OPFS backend using FileSystemSyncAccessHandle for fast I/O.
 *
 * Stores each virtual file as a single OPFS file containing all pages
 * at fixed offsets (page N starts at N * PAGE_SIZE). This eliminates
 * per-page directory lookups and createWritable/close overhead.
 *
 * Sync access handles provide synchronous read/write/truncate once
 * opened, dramatically reducing per-operation latency compared to the
 * standard OPFS backend's createWritable + write + close cycle.
 *
 * Metadata is stored as separate JSON files (same as OpfsBackend).
 */
export class OpfsSahBackend extends OpfsBackendBase {
  private readonly maxOpenHandles: number;

  private handleCache = new Map<string, SyncAccessHandle>();
  private handleLru: string[] = [];

  constructor(options?: OpfsSahBackendOptions) {
    super(options?.root);
    this.maxOpenHandles = options?.maxOpenHandles ?? 64;
  }

  private async getHandle(
    path: string,
    create: boolean,
  ): Promise<SyncAccessHandle | null> {
    const encoded = encodePath(path);

    const cached = this.handleCache.get(encoded);
    if (cached) {
      this.touchLru(encoded);
      return cached;
    }

    await this.init();

    let fileHandle: FileSystemFileHandle;
    try {
      fileHandle = await this.pagesDir!.getFileHandle(encoded, { create });
    } catch (err) {
      if (isNotFoundError(err)) return null;
      throw err;
    }

    if (this.handleCache.size >= this.maxOpenHandles) {
      this.evictHandle();
    }

    const sah = await fileHandle.createSyncAccessHandle();
    this.handleCache.set(encoded, sah);
    this.handleLru.push(encoded);
    return sah;
  }

  private touchLru(encoded: string): void {
    const idx = this.handleLru.indexOf(encoded);
    if (idx >= 0 && idx !== this.handleLru.length - 1) {
      this.handleLru.splice(idx, 1);
      this.handleLru.push(encoded);
    }
  }

  private evictHandle(): void {
    if (this.handleLru.length === 0) return;
    const victim = this.handleLru.shift()!;
    const handle = this.handleCache.get(victim);
    if (handle) {
      handle.close();
      this.handleCache.delete(victim);
    }
  }

  private closeHandle(encoded: string): void {
    const handle = this.handleCache.get(encoded);
    if (handle) {
      handle.close();
      this.handleCache.delete(encoded);
      const idx = this.handleLru.indexOf(encoded);
      if (idx >= 0) this.handleLru.splice(idx, 1);
    }
  }

  async readPage(
    path: string,
    pageIndex: number,
  ): Promise<Uint8Array | null> {
    const handle = await this.getHandle(path, false);
    if (!handle) return null;

    const offset = pageIndex * PAGE_SIZE;
    const fileSize = handle.getSize();
    if (offset >= fileSize) return null;

    const buffer = new Uint8Array(PAGE_SIZE);
    handle.read(buffer, { at: offset });
    return buffer;
  }

  async readPages(
    path: string,
    pageIndices: number[],
  ): Promise<Array<Uint8Array | null>> {
    if (pageIndices.length === 0) return [];

    const handle = await this.getHandle(path, false);
    if (!handle) return pageIndices.map(() => null);

    const fileSize = handle.getSize();
    return pageIndices.map((pageIndex) => {
      const offset = pageIndex * PAGE_SIZE;
      if (offset >= fileSize) return null;
      const buffer = new Uint8Array(PAGE_SIZE);
      handle.read(buffer, { at: offset });
      return buffer;
    });
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
    const handle = await this.getHandle(path, true);
    const offset = pageIndex * PAGE_SIZE;
    handle!.write(new Uint8Array(data), { at: offset });
    handle!.flush();
  }

  async writePages(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
  ): Promise<void> {
    if (pages.length === 0) return;
    if (pages.length === 1) {
      await this.writePage(pages[0].path, pages[0].pageIndex, pages[0].data);
      return;
    }

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

    for (const [path, group] of byPath) {
      const handle = await this.getHandle(path, true);
      for (const { pageIndex, data } of group) {
        handle!.write(new Uint8Array(data), { at: pageIndex * PAGE_SIZE });
      }
      handle!.flush();
    }
  }

  async deleteFile(path: string): Promise<void> {
    const encoded = encodePath(path);
    this.closeHandle(encoded);
    await this.init();
    try {
      await this.pagesDir!.removeEntry(encoded);
    } catch (err) {
      if (!isNotFoundError(err)) throw err;
    }
  }

  async deleteFiles(paths: string[]): Promise<void> {
    if (paths.length === 0) return;
    if (paths.length === 1) {
      await this.deleteFile(paths[0]);
      return;
    }
    for (const path of paths) {
      await this.deleteFile(path);
    }
  }

  async deletePagesFrom(
    path: string,
    fromPageIndex: number,
  ): Promise<void> {
    const handle = await this.getHandle(path, false);
    if (!handle) return;

    const newSize = fromPageIndex * PAGE_SIZE;
    const currentSize = handle.getSize();
    if (newSize < currentSize) {
      handle.truncate(newSize);
      handle.flush();
    }
  }

  async renameFile(oldPath: string, newPath: string): Promise<void> {
    if (oldPath === newPath) return;

    const oldEncoded = encodePath(oldPath);
    const newEncoded = encodePath(newPath);

    // Close handles for both paths
    this.closeHandle(oldEncoded);
    this.closeHandle(newEncoded);

    await this.init();

    // Remove destination if it exists
    try {
      await this.pagesDir!.removeEntry(newEncoded);
    } catch (err) {
      if (!isNotFoundError(err)) throw err;
    }

    // Read all data from old file
    let oldHandle: FileSystemFileHandle;
    try {
      oldHandle = await this.pagesDir!.getFileHandle(oldEncoded);
    } catch (err) {
      if (isNotFoundError(err)) return;
      throw err;
    }

    const oldSah = await oldHandle.createSyncAccessHandle();
    const size = oldSah.getSize();
    let data: Uint8Array | null = null;
    if (size > 0) {
      data = new Uint8Array(size);
      oldSah.read(data, { at: 0 });
    }
    oldSah.close();

    // Write to new file — wrap in try/catch so we can clean up the
    // partial new file on failure. The old file is still intact at this
    // point, so cleanup just removes the new file.
    if (data && data.byteLength > 0) {
      const newFileHandle = await this.pagesDir!.getFileHandle(newEncoded, {
        create: true,
      });
      const newSah = await newFileHandle.createSyncAccessHandle();
      try {
        newSah.write(data, { at: 0 });
        newSah.flush();
      } catch (err) {
        newSah.close();
        try {
          await this.pagesDir!.removeEntry(newEncoded);
        } catch (cleanupErr) {
          if (!isNotFoundError(cleanupErr)) {
            throw new Error(
              `OpfsSahBackend renameFile: write failed (${err instanceof Error ? err.message : String(err)}) ` +
                `and cleanup also failed (${cleanupErr instanceof Error ? cleanupErr.message : String(cleanupErr)})`,
            );
          }
        }
        throw err;
      }
      newSah.close();
    } else {
      await this.pagesDir!.getFileHandle(newEncoded, { create: true });
    }

    // Remove old file only after successful write.
    await this.pagesDir!.removeEntry(oldEncoded);
  }

  async countPages(path: string): Promise<number> {
    const handle = await this.getHandle(path, false);
    if (!handle) return 0;
    const size = handle.getSize();
    return Math.ceil(size / PAGE_SIZE);
  }

  async countPagesBatch(paths: string[]): Promise<number[]> {
    if (paths.length === 0) return [];
    const results: number[] = [];
    for (const path of paths) {
      results.push(await this.countPages(path));
    }
    return results;
  }

  async maxPageIndex(path: string): Promise<number> {
    const handle = await this.getHandle(path, false);
    if (!handle) return -1;
    const size = handle.getSize();
    if (size === 0) return -1;
    return Math.ceil(size / PAGE_SIZE) - 1;
  }

  async maxPageIndexBatch(paths: string[]): Promise<number[]> {
    if (paths.length === 0) return [];
    const results: number[] = [];
    for (const path of paths) {
      results.push(await this.maxPageIndex(path));
    }
    return results;
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
      this.closeHandle(name);
      try {
        await this.pagesDir!.removeEntry(name);
      } catch (err) {
        if (!isNotFoundError(err)) throw err;
      }
    }

    return orphans.length;
  }

  async destroy(): Promise<void> {
    for (const [, handle] of this.handleCache) {
      handle.close();
    }
    this.handleCache.clear();
    this.handleLru = [];

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
