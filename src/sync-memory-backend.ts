import type { SyncStorageBackend } from "./sync-storage-backend.js";
import type { FileMeta } from "./types.js";
import { InMemoryPageStore } from "./in-memory-page-store.js";

/**
 * Synchronous in-memory storage backend.
 *
 * Same behavior as MemoryBackend but with a synchronous interface,
 * suitable for use with SyncPageCache inside Emscripten FS operations.
 */
export class SyncMemoryBackend implements SyncStorageBackend {
  private readonly store = new InMemoryPageStore();

  readPage(path: string, pageIndex: number): Uint8Array | null {
    return this.store.readPage(path, pageIndex);
  }

  readPages(path: string, pageIndices: number[]): Array<Uint8Array | null> {
    return this.store.readPages(path, pageIndices);
  }

  readPageBatch(
    entries: Array<{ path: string; pageIndex: number }>,
  ): Array<Uint8Array | null> {
    return this.store.readPageBatch(entries);
  }

  writePage(path: string, pageIndex: number, data: Uint8Array): void {
    this.store.writePage(path, pageIndex, data);
  }

  writePages(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
  ): void {
    this.store.writePages(pages);
  }

  deleteFile(path: string): void {
    this.store.deleteFile(path);
  }

  deleteFiles(paths: string[]): void {
    this.store.deleteFiles(paths);
  }

  deletePagesFrom(path: string, fromPageIndex: number): void {
    this.store.deletePagesFrom(path, fromPageIndex);
  }

  countPages(path: string): number {
    return this.store.countPages(path);
  }

  countPagesBatch(paths: string[]): number[] {
    return this.store.countPagesBatch(paths);
  }

  maxPageIndex(path: string): number {
    return this.store.maxPageIndex(path);
  }

  maxPageIndexBatch(paths: string[]): number[] {
    return this.store.maxPageIndexBatch(paths);
  }

  renameFile(oldPath: string, newPath: string): void {
    this.store.renameFile(oldPath, newPath);
  }

  readMeta(path: string): FileMeta | null {
    return this.store.readMeta(path);
  }

  readMetas(paths: string[]): Array<FileMeta | null> {
    return this.store.readMetas(paths);
  }

  writeMeta(path: string, meta: FileMeta): void {
    this.store.writeMeta(path, meta);
  }

  writeMetas(entries: Array<{ path: string; meta: FileMeta }>): void {
    this.store.writeMetas(entries);
  }

  deleteMeta(path: string): void {
    this.store.deleteMeta(path);
  }

  deleteMetas(paths: string[]): void {
    this.store.deleteMetas(paths);
  }

  listFiles(): string[] {
    return this.store.listFiles();
  }

  syncAll(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
    metas: Array<{ path: string; meta: FileMeta }>,
  ): void {
    this.writePages(pages);
    this.writeMetas(metas);
  }

  deleteAll(paths: string[]): void {
    this.deleteMetas(paths);
    this.deleteFiles(paths);
  }

  cleanupOrphanedPages(): number {
    return this.store.cleanupOrphanedPages();
  }

  assertInvariants(): void {
    const errors = this.store.assertInvariants();
    if (errors.length > 0) {
      throw new Error(
        `SyncMemoryBackend invariant violations (${errors.length}):\n  - ${errors.join("\n  - ")}`,
      );
    }
  }
}
