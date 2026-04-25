/**
 * Package export validation tests.
 *
 * Verify that all three package entry points (`.`, `./worker`, `./pglite`)
 * export the expected symbols with the correct types. Catches regressions
 * where exports are accidentally removed, renamed, or mis-typed.
 *
 * These tests import from the source (not dist/) so they run without a
 * build step, but they validate the same export surface that consumers see.
 */
import { describe, it, expect } from "vitest";

import {
  PageCache,
  MemoryBackend,
  IdbBackend,
  OpfsBackend,
  SyncPageCache,
  SyncMemoryBackend,
  createTomeFS,
  PreloadBackend,
  SabClient,
  SabWorker,
  PAGE_SIZE,
  DEFAULT_MAX_PAGES,
  pageKeyStr,
} from "../../src/index.js";

import type {
  StorageBackend,
  SyncStorageBackend,
  TomeFSOptions,
  CachedPage,
  CacheStats,
  FileMeta,
  PageKey,
  IdbBackendOptions,
  OpfsBackendOptions,
  SabClientOptions,
} from "../../src/index.js";

import {
  SabWorker as WorkerSabWorker,
  IdbBackend as WorkerIdbBackend,
  OpfsBackend as WorkerOpfsBackend,
  PAGE_SIZE as WorkerPageSize,
} from "../../src/worker.js";

import type {
  StorageBackend as WorkerStorageBackend,
  IdbBackendOptions as WorkerIdbBackendOptions,
  OpfsBackendOptions as WorkerOpfsBackendOptions,
  FileMeta as WorkerFileMeta,
} from "../../src/worker.js";

import { createTomeFSPGlite } from "../../src/pglite.js";
import type { TomeFSPGliteOptions } from "../../src/pglite.js";

describe("package exports: main entry point", () => {
  it("exports all core classes @fast", () => {
    expect(PageCache).toBeDefined();
    expect(SyncPageCache).toBeDefined();
    expect(MemoryBackend).toBeDefined();
    expect(SyncMemoryBackend).toBeDefined();
    expect(IdbBackend).toBeDefined();
    expect(OpfsBackend).toBeDefined();
    expect(PreloadBackend).toBeDefined();
    expect(SabClient).toBeDefined();
    expect(SabWorker).toBeDefined();
  });

  it("exports createTomeFS factory function @fast", () => {
    expect(typeof createTomeFS).toBe("function");
  });

  it("exports PAGE_SIZE as 8192 @fast", () => {
    expect(PAGE_SIZE).toBe(8192);
  });

  it("exports DEFAULT_MAX_PAGES as a positive integer @fast", () => {
    expect(typeof DEFAULT_MAX_PAGES).toBe("number");
    expect(DEFAULT_MAX_PAGES).toBeGreaterThan(0);
    expect(Number.isInteger(DEFAULT_MAX_PAGES)).toBe(true);
  });

  it("exports pageKeyStr utility @fast", () => {
    expect(typeof pageKeyStr).toBe("function");
    const key = pageKeyStr("/file", 0);
    expect(typeof key).toBe("string");
    expect(key).toContain("/file");
  });

  it("SyncMemoryBackend implements SyncStorageBackend @fast", () => {
    const backend = new SyncMemoryBackend();
    expect(typeof backend.readPage).toBe("function");
    expect(typeof backend.writePage).toBe("function");
    expect(typeof backend.readPages).toBe("function");
    expect(typeof backend.writePages).toBe("function");
    expect(typeof backend.deleteFile).toBe("function");
    expect(typeof backend.deleteFiles).toBe("function");
    expect(typeof backend.deleteAll).toBe("function");
    expect(typeof backend.deletePagesFrom).toBe("function");
    expect(typeof backend.renameFile).toBe("function");
    expect(typeof backend.readMeta).toBe("function");
    expect(typeof backend.writeMeta).toBe("function");
    expect(typeof backend.readMetas).toBe("function");
    expect(typeof backend.writeMetas).toBe("function");
    expect(typeof backend.deleteMeta).toBe("function");
    expect(typeof backend.deleteMetas).toBe("function");
    expect(typeof backend.countPages).toBe("function");
    expect(typeof backend.countPagesBatch).toBe("function");
    expect(typeof backend.maxPageIndex).toBe("function");
    expect(typeof backend.maxPageIndexBatch).toBe("function");
    expect(typeof backend.listFiles).toBe("function");
    expect(typeof backend.syncAll).toBe("function");
  });

  it("MemoryBackend implements StorageBackend @fast", () => {
    const backend = new MemoryBackend();
    expect(typeof backend.readPage).toBe("function");
    expect(typeof backend.writePage).toBe("function");
    expect(typeof backend.readPages).toBe("function");
    expect(typeof backend.writePages).toBe("function");
    expect(typeof backend.deleteFile).toBe("function");
    expect(typeof backend.deleteFiles).toBe("function");
    expect(typeof backend.deleteAll).toBe("function");
    expect(typeof backend.deletePagesFrom).toBe("function");
    expect(typeof backend.renameFile).toBe("function");
    expect(typeof backend.readMeta).toBe("function");
    expect(typeof backend.writeMeta).toBe("function");
    expect(typeof backend.readMetas).toBe("function");
    expect(typeof backend.writeMetas).toBe("function");
    expect(typeof backend.deleteMeta).toBe("function");
    expect(typeof backend.deleteMetas).toBe("function");
    expect(typeof backend.countPages).toBe("function");
    expect(typeof backend.countPagesBatch).toBe("function");
    expect(typeof backend.maxPageIndex).toBe("function");
    expect(typeof backend.maxPageIndexBatch).toBe("function");
    expect(typeof backend.listFiles).toBe("function");
    expect(typeof backend.syncAll).toBe("function");
  });

  it("SyncPageCache constructs with SyncMemoryBackend @fast", () => {
    const backend = new SyncMemoryBackend();
    const cache = new SyncPageCache(backend, 16);
    expect(cache).toBeDefined();
    const stats = cache.getStats();
    expect(stats.hits).toBe(0);
    expect(stats.misses).toBe(0);
    expect(stats.evictions).toBe(0);
  });

  it("PageCache constructs with MemoryBackend @fast", () => {
    const backend = new MemoryBackend();
    const cache = new PageCache(backend, 16);
    expect(cache).toBeDefined();
    const stats = cache.getStats();
    expect(stats.hits).toBe(0);
  });
});

describe("package exports: worker entry point", () => {
  it("exports SabWorker @fast", () => {
    expect(WorkerSabWorker).toBeDefined();
    expect(WorkerSabWorker).toBe(SabWorker);
  });

  it("exports IdbBackend @fast", () => {
    expect(WorkerIdbBackend).toBeDefined();
    expect(WorkerIdbBackend).toBe(IdbBackend);
  });

  it("exports OpfsBackend @fast", () => {
    expect(WorkerOpfsBackend).toBeDefined();
    expect(WorkerOpfsBackend).toBe(OpfsBackend);
  });

  it("exports PAGE_SIZE matching main @fast", () => {
    expect(WorkerPageSize).toBe(PAGE_SIZE);
    expect(WorkerPageSize).toBe(8192);
  });
});

describe("package exports: pglite entry point", () => {
  it("exports createTomeFSPGlite factory @fast", () => {
    expect(typeof createTomeFSPGlite).toBe("function");
  });
});
