import type { FileMeta } from "./types.js";
import { pageKeyStr } from "./types.js";

/**
 * In-memory page and metadata store with secondary indices.
 *
 * Manages page data, file metadata, and three secondary indices for
 * efficient lookups. Used as the internal storage engine by both
 * SyncMemoryBackend and PreloadBackend to avoid duplicating the ~250
 * lines of index-management, batch-optimized writes, and invariant
 * checking that are identical between the two.
 */
export class InMemoryPageStore {
  readonly pages = new Map<string, Uint8Array>();
  readonly meta = new Map<string, FileMeta>();

  /** file path → set of page keys belonging to that file. */
  readonly filePageKeys = new Map<string, Set<string>>();

  /** file path → Map<pageIndex, key>. */
  readonly filePageIndices = new Map<string, Map<number, string>>();

  /** Cached max page index per file (-1 means no pages). */
  readonly fileMaxIdx = new Map<string, number>();

  clear(): void {
    this.pages.clear();
    this.meta.clear();
    this.filePageKeys.clear();
    this.filePageIndices.clear();
    this.fileMaxIdx.clear();
  }

  trackPage(path: string, key: string, pageIndex: number): void {
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
    const data = this.pages.get(pageKeyStr(path, pageIndex));
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

  readPageBatch(
    entries: Array<{ path: string; pageIndex: number }>,
  ): Array<Uint8Array | null> {
    if (entries.length === 0) return [];
    return entries.map(({ path, pageIndex }) => this.readPage(path, pageIndex));
  }

  writePage(path: string, pageIndex: number, data: Uint8Array): void {
    const key = pageKeyStr(path, pageIndex);
    this.pages.set(key, new Uint8Array(data));
    this.trackPage(path, key, pageIndex);
  }

  writePages(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
  ): void {
    if (pages.length === 0) return;
    if (pages.length === 1) {
      this.writePage(pages[0].path, pages[0].pageIndex, pages[0].data);
      return;
    }

    let prevPath = "";
    let keys: Set<string> | undefined;
    let indices: Map<number, string> | undefined;
    let maxIdx = -1;

    for (const { path, pageIndex, data } of pages) {
      const key = pageKeyStr(path, pageIndex);
      this.pages.set(key, new Uint8Array(data));

      if (path !== prevPath) {
        if (prevPath !== "" && maxIdx >= 0) {
          const curMax = this.fileMaxIdx.get(prevPath) ?? -1;
          if (maxIdx > curMax) this.fileMaxIdx.set(prevPath, maxIdx);
        }

        prevPath = path;
        keys = this.filePageKeys.get(path);
        if (!keys) {
          keys = new Set();
          this.filePageKeys.set(path, keys);
        }
        indices = this.filePageIndices.get(path);
        if (!indices) {
          indices = new Map();
          this.filePageIndices.set(path, indices);
        }
        maxIdx = this.fileMaxIdx.get(path) ?? -1;
      }

      keys!.add(key);
      indices!.set(pageIndex, key);
      if (pageIndex > maxIdx) maxIdx = pageIndex;
    }

    if (prevPath !== "" && maxIdx >= 0) {
      const curMax = this.fileMaxIdx.get(prevPath) ?? -1;
      if (maxIdx > curMax) this.fileMaxIdx.set(prevPath, maxIdx);
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

  cleanupOrphanedPages(): number {
    const pagePaths = [...this.filePageKeys.keys()];
    let removed = 0;
    for (const path of pagePaths) {
      if (!this.meta.has(path)) {
        this.deleteFile(path);
        removed++;
      }
    }
    return removed;
  }

  /**
   * Check data structure invariants. Returns an array of error messages
   * (empty if all invariants hold). Callers append backend-specific
   * checks and decide how to surface errors.
   */
  assertInvariants(): string[] {
    const errors: string[] = [];

    const allTrackedKeys = new Set<string>();
    for (const [path, keys] of this.filePageKeys) {
      if (keys.size === 0) {
        errors.push(`filePageKeys[${path}] is empty (should be deleted)`);
      }
      for (const key of keys) {
        if (allTrackedKeys.has(key)) {
          errors.push(`filePageKeys: key ${key} appears under multiple paths`);
        }
        allTrackedKeys.add(key);
        if (!this.pages.has(key)) {
          errors.push(`filePageKeys[${path}] contains ${key} not in pages`);
        }
        const nullIdx = key.indexOf("\0");
        const keyPath = key.substring(0, nullIdx);
        if (keyPath !== path) {
          errors.push(
            `filePageKeys[${path}] contains key with path=${keyPath}`,
          );
        }
      }
    }
    for (const key of this.pages.keys()) {
      if (!allTrackedKeys.has(key)) {
        errors.push(`pages contains ${key} not tracked in filePageKeys`);
      }
    }

    for (const [path, indices] of this.filePageIndices) {
      if (indices.size === 0) {
        errors.push(`filePageIndices[${path}] is empty (should be deleted)`);
      }
      const keys = this.filePageKeys.get(path);
      if (!keys) {
        errors.push(
          `filePageIndices has path ${path} not in filePageKeys`,
        );
        continue;
      }
      if (indices.size !== keys.size) {
        errors.push(
          `filePageIndices[${path}] size ${indices.size} !== filePageKeys[${path}] size ${keys.size}`,
        );
      }
      for (const [pageIndex, key] of indices) {
        if (!keys.has(key)) {
          errors.push(
            `filePageIndices[${path}][${pageIndex}] = ${key} not in filePageKeys[${path}]`,
          );
        }
        const expected = pageKeyStr(path, pageIndex);
        if (key !== expected) {
          errors.push(
            `filePageIndices[${path}][${pageIndex}] = ${key} but expected ${expected}`,
          );
        }
      }
    }
    for (const path of this.filePageKeys.keys()) {
      if (!this.filePageIndices.has(path)) {
        errors.push(
          `filePageKeys has path ${path} not in filePageIndices`,
        );
      }
    }

    for (const [path, cachedMax] of this.fileMaxIdx) {
      const indices = this.filePageIndices.get(path);
      if (!indices || indices.size === 0) {
        errors.push(
          `fileMaxIdx[${path}] = ${cachedMax} but no pages exist`,
        );
        continue;
      }
      let actualMax = -1;
      for (const idx of indices.keys()) {
        if (idx > actualMax) actualMax = idx;
      }
      if (cachedMax !== actualMax) {
        errors.push(
          `fileMaxIdx[${path}] = ${cachedMax} but actual max is ${actualMax}`,
        );
      }
    }
    for (const path of this.filePageIndices.keys()) {
      if (!this.fileMaxIdx.has(path)) {
        errors.push(
          `filePageIndices has path ${path} not in fileMaxIdx`,
        );
      }
    }

    return errors;
  }
}
