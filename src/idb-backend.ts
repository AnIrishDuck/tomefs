import type { StorageBackend } from "./storage-backend.js";
import type { FileMeta } from "./types.js";

/** Object store names used by IdbBackend. */
const PAGES_STORE = "pages";
const META_STORE = "file_meta";

/** Default database name. */
const DEFAULT_DB_NAME = "tomefs";

/** Default database version. */
const DEFAULT_DB_VERSION = 1;

/** Options for creating an IdbBackend. */
export interface IdbBackendOptions {
  /** Database name. Default: "tomefs". */
  dbName?: string;
  /** Database version. Default: 1. */
  dbVersion?: number;
  /** Pre-opened IDBDatabase instance (overrides dbName/dbVersion). */
  db?: IDBDatabase;
}

/**
 * IndexedDB storage backend.
 *
 * Stores file pages and metadata in two object stores:
 * - `pages`: keyed by compound key [path, pageIndex] → Uint8Array (8 KB)
 * - `file_meta`: keyed by path → FileMeta
 *
 * Compound keys enable efficient range queries (all pages for a file).
 * Batch writes use a single IDB transaction for atomicity.
 */
export class IdbBackend implements StorageBackend {
  private db: IDBDatabase | null;
  private readonly dbName: string;
  private readonly dbVersion: number;
  private initPromise: Promise<IDBDatabase> | null = null;

  constructor(options?: IdbBackendOptions) {
    this.db = options?.db ?? null;
    this.dbName = options?.dbName ?? DEFAULT_DB_NAME;
    this.dbVersion = options?.dbVersion ?? DEFAULT_DB_VERSION;
  }

  /**
   * Open (or create) the IndexedDB database.
   * Idempotent — returns the same db if already open.
   */
  private async getDb(): Promise<IDBDatabase> {
    if (this.db) return this.db;
    if (this.initPromise) return this.initPromise;

    this.initPromise = new Promise<IDBDatabase>((resolve, reject) => {
      const request = indexedDB.open(this.dbName, this.dbVersion);

      request.onupgradeneeded = () => {
        const db = request.result;
        if (!db.objectStoreNames.contains(PAGES_STORE)) {
          db.createObjectStore(PAGES_STORE);
        }
        if (!db.objectStoreNames.contains(META_STORE)) {
          db.createObjectStore(META_STORE);
        }
      };

      request.onsuccess = () => {
        this.db = request.result;
        resolve(this.db);
      };

      request.onerror = () => {
        this.initPromise = null;
        reject(request.error);
      };
    });

    return this.initPromise;
  }

  /** Build the compound key for a page: [path, pageIndex]. */
  private pageKey(path: string, pageIndex: number): [string, number] {
    return [path, pageIndex];
  }

  async readPage(
    path: string,
    pageIndex: number,
  ): Promise<Uint8Array | null> {
    const db = await this.getDb();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(PAGES_STORE, "readonly");
      const store = tx.objectStore(PAGES_STORE);
      const request = store.get(this.pageKey(path, pageIndex));

      request.onsuccess = () => {
        const result = request.result;
        if (result == null) {
          resolve(null);
        } else {
          resolve(new Uint8Array(result));
        }
      };

      request.onerror = () => reject(request.error);
    });
  }

  async readPages(
    path: string,
    pageIndices: number[],
  ): Promise<Array<Uint8Array | null>> {
    if (pageIndices.length === 0) return [];
    const db = await this.getDb();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(PAGES_STORE, "readonly");
      const store = tx.objectStore(PAGES_STORE);
      const results: Array<Uint8Array | null> = new Array(pageIndices.length);
      let completed = 0;

      for (let i = 0; i < pageIndices.length; i++) {
        const request = store.get(this.pageKey(path, pageIndices[i]));
        request.onsuccess = () => {
          results[i] =
            request.result == null ? null : new Uint8Array(request.result);
          completed++;
          if (completed === pageIndices.length) resolve(results);
        };
        request.onerror = () => reject(request.error);
      }

      tx.onerror = () => reject(tx.error);
    });
  }

  async writePage(
    path: string,
    pageIndex: number,
    data: Uint8Array,
  ): Promise<void> {
    const db = await this.getDb();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(PAGES_STORE, "readwrite");
      const store = tx.objectStore(PAGES_STORE);
      store.put(new Uint8Array(data), this.pageKey(path, pageIndex));

      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  async writePages(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
  ): Promise<void> {
    if (pages.length === 0) return;

    const db = await this.getDb();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(PAGES_STORE, "readwrite");
      const store = tx.objectStore(PAGES_STORE);

      for (const { path, pageIndex, data } of pages) {
        store.put(new Uint8Array(data), this.pageKey(path, pageIndex));
      }

      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  async deleteFile(path: string): Promise<void> {
    const db = await this.getDb();

    // Use IDB key range to target only this file's pages.
    // Compound keys are [path, pageIndex] where pageIndex is a number.
    // In IDB key ordering, strings sort after numbers, so [path, ""]
    // sorts after [path, <any number>], bounding just this file's pages.
    return new Promise((resolve, reject) => {
      const tx = db.transaction(PAGES_STORE, "readwrite");
      const store = tx.objectStore(PAGES_STORE);
      const range = IDBKeyRange.bound([path, 0], [path, ""], false, true);
      store.delete(range);

      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  async deleteFiles(paths: string[]): Promise<void> {
    if (paths.length === 0) return;
    if (paths.length === 1) {
      await this.deleteFile(paths[0]);
      return;
    }

    const db = await this.getDb();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(PAGES_STORE, "readwrite");
      const store = tx.objectStore(PAGES_STORE);

      for (const path of paths) {
        const range = IDBKeyRange.bound([path, 0], [path, ""], false, true);
        store.delete(range);
      }

      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  async deletePagesFrom(
    path: string,
    fromPageIndex: number,
  ): Promise<void> {
    const db = await this.getDb();

    // Use IDB key range to target pages at and beyond fromPageIndex.
    // Lower bound [path, fromPageIndex] captures the first page to delete;
    // upper bound [path, ""] captures all higher-numbered pages for this path.
    return new Promise((resolve, reject) => {
      const tx = db.transaction(PAGES_STORE, "readwrite");
      const store = tx.objectStore(PAGES_STORE);
      const range = IDBKeyRange.bound(
        [path, fromPageIndex],
        [path, ""],
        false,
        true,
      );
      store.delete(range);

      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  async countPages(path: string): Promise<number> {
    const db = await this.getDb();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(PAGES_STORE, "readonly");
      const store = tx.objectStore(PAGES_STORE);
      const range = IDBKeyRange.bound([path, 0], [path, ""], false, true);
      const request = store.count(range);

      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });
  }

  async countPagesBatch(paths: string[]): Promise<number[]> {
    if (paths.length === 0) return [];
    const db = await this.getDb();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(PAGES_STORE, "readonly");
      const store = tx.objectStore(PAGES_STORE);
      const results = new Array<number>(paths.length);
      let completed = 0;

      for (let i = 0; i < paths.length; i++) {
        const range = IDBKeyRange.bound([paths[i], 0], [paths[i], ""], false, true);
        const request = store.count(range);
        request.onsuccess = () => {
          results[i] = request.result;
          completed++;
          if (completed === paths.length) resolve(results);
        };
        request.onerror = () => reject(request.error);
      }

      tx.onerror = () => reject(tx.error);
    });
  }

  async maxPageIndex(path: string): Promise<number> {
    const db = await this.getDb();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(PAGES_STORE, "readonly");
      const store = tx.objectStore(PAGES_STORE);
      // Open a reverse cursor over this file's key range to find the highest index.
      const range = IDBKeyRange.bound([path, 0], [path, ""], false, true);
      const request = store.openCursor(range, "prev");

      request.onsuccess = () => {
        const cursor = request.result;
        if (!cursor) {
          resolve(-1);
        } else {
          const [, pageIndex] = cursor.key as [string, number];
          resolve(pageIndex);
        }
      };

      request.onerror = () => reject(request.error);
    });
  }

  async maxPageIndexBatch(paths: string[]): Promise<number[]> {
    if (paths.length === 0) return [];
    const db = await this.getDb();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(PAGES_STORE, "readonly");
      const store = tx.objectStore(PAGES_STORE);
      const results = new Array<number>(paths.length).fill(-1);
      let completed = 0;

      for (let i = 0; i < paths.length; i++) {
        const range = IDBKeyRange.bound([paths[i], 0], [paths[i], ""], false, true);
        const request = store.openCursor(range, "prev");
        request.onsuccess = () => {
          const cursor = request.result;
          if (cursor) {
            const [, pageIndex] = cursor.key as [string, number];
            results[i] = pageIndex;
          }
          completed++;
          if (completed === paths.length) resolve(results);
        };
        request.onerror = () => reject(request.error);
      }

      tx.onerror = () => reject(tx.error);
    });
  }

  async renameFile(oldPath: string, newPath: string): Promise<void> {
    if (oldPath === newPath) return;
    const db = await this.getDb();

    // Single transaction: delete destination pages, then cursor-copy old → new.
    // Clearing the destination first prevents orphan pages when the destination
    // has more pages than the source (mirrors OpfsBackend behavior).
    return new Promise((resolve, reject) => {
      const tx = db.transaction(PAGES_STORE, "readwrite");
      const store = tx.objectStore(PAGES_STORE);

      // Delete any pre-existing pages at the destination path.
      const destRange = IDBKeyRange.bound(
        [newPath, 0],
        [newPath, ""],
        false,
        true,
      );
      store.delete(destRange);

      // Cursor over old pages: copy to new key, delete old.
      const range = IDBKeyRange.bound([oldPath, 0], [oldPath, ""], false, true);
      const request = store.openCursor(range);

      request.onsuccess = () => {
        const cursor = request.result;
        if (!cursor) return; // iteration complete, tx will auto-commit
        const [, pageIndex] = cursor.key as [string, number];
        store.put(cursor.value, this.pageKey(newPath, pageIndex));
        cursor.delete();
        cursor.continue();
      };

      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  async readMeta(path: string): Promise<FileMeta | null> {
    const db = await this.getDb();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(META_STORE, "readonly");
      const store = tx.objectStore(META_STORE);
      const request = store.get(path);

      request.onsuccess = () => {
        resolve(request.result ?? null);
      };

      request.onerror = () => reject(request.error);
    });
  }

  async readMetas(paths: string[]): Promise<Array<FileMeta | null>> {
    if (paths.length === 0) return [];
    const db = await this.getDb();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(META_STORE, "readonly");
      const store = tx.objectStore(META_STORE);
      const results: Array<FileMeta | null> = new Array(paths.length);
      let completed = 0;

      for (let i = 0; i < paths.length; i++) {
        const request = store.get(paths[i]);
        request.onsuccess = () => {
          results[i] = request.result ?? null;
          completed++;
          if (completed === paths.length) resolve(results);
        };
        request.onerror = () => reject(request.error);
      }

      tx.onerror = () => reject(tx.error);
    });
  }

  async writeMeta(path: string, meta: FileMeta): Promise<void> {
    const db = await this.getDb();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(META_STORE, "readwrite");
      const store = tx.objectStore(META_STORE);
      store.put({ ...meta }, path);

      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  async writeMetas(
    entries: Array<{ path: string; meta: FileMeta }>,
  ): Promise<void> {
    if (entries.length === 0) return;

    const db = await this.getDb();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(META_STORE, "readwrite");
      const store = tx.objectStore(META_STORE);

      for (const { path, meta } of entries) {
        store.put({ ...meta }, path);
      }

      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  async deleteMeta(path: string): Promise<void> {
    const db = await this.getDb();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(META_STORE, "readwrite");
      const store = tx.objectStore(META_STORE);
      store.delete(path);

      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  async deleteMetas(paths: string[]): Promise<void> {
    if (paths.length === 0) return;

    const db = await this.getDb();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(META_STORE, "readwrite");
      const store = tx.objectStore(META_STORE);

      for (const path of paths) {
        store.delete(path);
      }

      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  async listFiles(): Promise<string[]> {
    const db = await this.getDb();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(META_STORE, "readonly");
      const store = tx.objectStore(META_STORE);
      const request = store.getAllKeys();

      request.onsuccess = () => {
        resolve(request.result as string[]);
      };

      request.onerror = () => reject(request.error);
    });
  }

  async syncAll(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
    metas: Array<{ path: string; meta: FileMeta }>,
  ): Promise<void> {
    if (pages.length === 0 && metas.length === 0) return;

    const db = await this.getDb();
    return new Promise((resolve, reject) => {
      // Single transaction spanning both stores — pages and metadata
      // are committed atomically. A crash can never leave pages updated
      // without their corresponding metadata (or vice versa).
      const tx = db.transaction([PAGES_STORE, META_STORE], "readwrite");
      const pageStore = tx.objectStore(PAGES_STORE);
      const metaStore = tx.objectStore(META_STORE);

      for (const { path, pageIndex, data } of pages) {
        pageStore.put(new Uint8Array(data), this.pageKey(path, pageIndex));
      }

      for (const { path, meta } of metas) {
        metaStore.put({ ...meta }, path);
      }

      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  async deleteAll(paths: string[]): Promise<void> {
    if (paths.length === 0) return;

    const db = await this.getDb();
    return new Promise((resolve, reject) => {
      const tx = db.transaction([PAGES_STORE, META_STORE], "readwrite");
      const pageStore = tx.objectStore(PAGES_STORE);
      const metaStore = tx.objectStore(META_STORE);

      for (const path of paths) {
        const range = IDBKeyRange.bound([path, 0], [path, ""], false, true);
        pageStore.delete(range);
        metaStore.delete(path);
      }

      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  /** Close the database connection. */
  close(): void {
    if (this.db) {
      this.db.close();
      this.db = null;
      this.initPromise = null;
    }
  }

  /**
   * Delete the entire database. Useful for cleanup in tests.
   * The backend must not be used after calling this.
   */
  async destroy(): Promise<void> {
    this.close();
    return new Promise((resolve, reject) => {
      const request = indexedDB.deleteDatabase(this.dbName);
      request.onsuccess = () => resolve();
      request.onerror = () => reject(request.error);
    });
  }
}
