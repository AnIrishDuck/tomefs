import type { StorageBackend } from "./storage-backend.js";
import type { FileMeta } from "./types.js";
import { SyncMemoryBackend } from "./sync-memory-backend.js";

/**
 * In-memory storage backend for testing (async interface).
 *
 * Thin async wrapper around SyncMemoryBackend — all operations delegate
 * to the synchronous implementation. This avoids duplicating the page
 * storage, secondary indexes, and rename logic across two files.
 *
 * No persistence — data is lost when the instance is garbage collected.
 * This is a fake (not a mock) per project conventions.
 */
export class MemoryBackend implements StorageBackend {
  private readonly inner = new SyncMemoryBackend();

  async readPage(path: string, pageIndex: number): Promise<Uint8Array | null> {
    return this.inner.readPage(path, pageIndex);
  }

  async readPages(
    path: string,
    pageIndices: number[],
  ): Promise<Array<Uint8Array | null>> {
    return this.inner.readPages(path, pageIndices);
  }

  async writePage(
    path: string,
    pageIndex: number,
    data: Uint8Array,
  ): Promise<void> {
    this.inner.writePage(path, pageIndex, data);
  }

  async writePages(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
  ): Promise<void> {
    this.inner.writePages(pages);
  }

  async deleteFile(path: string): Promise<void> {
    this.inner.deleteFile(path);
  }

  async deleteFiles(paths: string[]): Promise<void> {
    this.inner.deleteFiles(paths);
  }

  async deletePagesFrom(path: string, fromPageIndex: number): Promise<void> {
    this.inner.deletePagesFrom(path, fromPageIndex);
  }

  async countPages(path: string): Promise<number> {
    return this.inner.countPages(path);
  }

  async countPagesBatch(paths: string[]): Promise<number[]> {
    return this.inner.countPagesBatch(paths);
  }

  async maxPageIndex(path: string): Promise<number> {
    return this.inner.maxPageIndex(path);
  }

  async maxPageIndexBatch(paths: string[]): Promise<number[]> {
    return this.inner.maxPageIndexBatch(paths);
  }

  async renameFile(oldPath: string, newPath: string): Promise<void> {
    this.inner.renameFile(oldPath, newPath);
  }

  async readMeta(path: string): Promise<FileMeta | null> {
    return this.inner.readMeta(path);
  }

  async readMetas(paths: string[]): Promise<Array<FileMeta | null>> {
    return this.inner.readMetas(paths);
  }

  async writeMeta(path: string, meta: FileMeta): Promise<void> {
    this.inner.writeMeta(path, meta);
  }

  async writeMetas(
    entries: Array<{ path: string; meta: FileMeta }>,
  ): Promise<void> {
    this.inner.writeMetas(entries);
  }

  async deleteMeta(path: string): Promise<void> {
    this.inner.deleteMeta(path);
  }

  async deleteMetas(paths: string[]): Promise<void> {
    this.inner.deleteMetas(paths);
  }

  async listFiles(): Promise<string[]> {
    return this.inner.listFiles();
  }

  async syncAll(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
    metas: Array<{ path: string; meta: FileMeta }>,
  ): Promise<void> {
    this.inner.syncAll(pages, metas);
  }
}
