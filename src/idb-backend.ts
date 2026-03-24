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

    // Delete all pages for this file using a cursor over all keys
    return new Promise((resolve, reject) => {
      const tx = db.transaction(PAGES_STORE, "readwrite");
      const store = tx.objectStore(PAGES_STORE);
      const request = store.openCursor();

      request.onsuccess = () => {
        const cursor = request.result;
        if (!cursor) return; // Done — tx.oncomplete will fire

        const key = cursor.key as [string, number];
        if (Array.isArray(key) && key[0] === path) {
          cursor.delete();
        }
        cursor.continue();
      };

      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  async deletePagesFrom(
    path: string,
    fromPageIndex: number,
  ): Promise<void> {
    const db = await this.getDb();

    return new Promise((resolve, reject) => {
      const tx = db.transaction(PAGES_STORE, "readwrite");
      const store = tx.objectStore(PAGES_STORE);
      const request = store.openCursor();

      request.onsuccess = () => {
        const cursor = request.result;
        if (!cursor) return;

        const key = cursor.key as [string, number];
        if (Array.isArray(key) && key[0] === path && key[1] >= fromPageIndex) {
          cursor.delete();
        }
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
