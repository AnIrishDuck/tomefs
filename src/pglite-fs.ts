/**
 * PGlite filesystem adapter for tomefs.
 *
 * Extends PGlite's MemoryFS to mount tomefs as an Emscripten FS inside
 * PGlite's WASM module. This lets PGlite store its Postgres data files
 * on tomefs's bounded, page-cached filesystem instead of the default
 * in-memory MEMFS or IDBFS.
 *
 * Usage:
 *   import { PGlite } from '@electric-sql/pglite';
 *   import { TomeFSPGlite } from 'tomefs/pglite';
 *
 *   const fs = new TomeFSPGlite({ maxPages: 4096 });
 *   const pg = new PGlite({ fs });
 */

import { createTomeFS } from "./tomefs.js";
import { SyncMemoryBackend } from "./sync-memory-backend.js";
import type { SyncStorageBackend } from "./sync-storage-backend.js";

/** PGlite's data directory inside the WASM filesystem. */
const PGLITE_DATA = "/pglite/data";

/** Options for creating a TomeFSPGlite adapter. */
export interface TomeFSPGliteOptions {
  /** Storage backend. Defaults to SyncMemoryBackend (in-memory). */
  backend?: SyncStorageBackend;
  /** Maximum pages in the LRU cache. Default: 4096 (32 MB). */
  maxPages?: number;
  /**
   * PGlite's MemoryFS class. Must be provided since PGlite is a peer
   * dependency and we avoid importing it directly.
   *
   * Usage: `import { MemoryFS } from '@electric-sql/pglite'`
   */
  MemoryFS: new () => any;
}

/**
 * Create a TomeFSPGlite adapter instance.
 *
 * Returns a PGlite Filesystem that extends MemoryFS with a preRun hook
 * to mount tomefs at /pglite/data. Inherits dumpTar and other methods
 * from MemoryFS.
 *
 * Usage:
 *   import { PGlite, MemoryFS } from '@electric-sql/pglite';
 *   import { createTomeFSPGlite } from 'tomefs/pglite';
 *
 *   const fs = createTomeFSPGlite({ MemoryFS, maxPages: 4096 });
 *   const pg = new PGlite({ fs });
 */
export function createTomeFSPGlite(options: TomeFSPGliteOptions): any {
  const backend = options.backend ?? new SyncMemoryBackend();
  const maxPages = options.maxPages ?? 4096;
  const { MemoryFS } = options;

  // Extend MemoryFS to inherit dumpTar, closeFs, etc.
  const adapter = new MemoryFS();
  let moduleFS: any = null;
  let tomefs: any = null;

  // Store original init
  const originalInit = adapter.init.bind(adapter);

  // Override init to add tomefs mount
  adapter.init = async (pg: any, emscriptenOptions: any) => {
    // Call parent init to store pg reference (needed for dumpTar)
    const result = await originalInit(pg, emscriptenOptions);

    // Add preRun hook that mounts tomefs. The hook is idempotent —
    // Emscripten may run preRun hooks multiple times during module
    // initialization (e.g., when PGlite restarts the WASM module).
    // Only the first invocation creates and mounts tomefs.
    return {
      emscriptenOpts: {
        ...result.emscriptenOpts,
        preRun: [
          ...(result.emscriptenOpts.preRun || []),
          (mod: any) => {
            if (tomefs) return; // Already mounted
            moduleFS = mod.FS;
            tomefs = createTomeFS(mod.FS, { backend, maxPages });
            mod.FS.mkdir(PGLITE_DATA);
            mod.FS.mount(tomefs, {}, PGLITE_DATA);
          },
        ],
      },
    };
  };

  // Sync tomefs dirty state to the backend, then flush to persistent storage
  // if the backend supports it. PreloadBackend has a two-phase persistence
  // model: syncfs writes to its in-memory store, then flush() propagates to
  // the underlying async backend (IDB/OPFS). Without this auto-flush, data
  // written through PreloadBackend would never reach persistent storage.
  // SyncMemoryBackend and SabClient don't have flush(), so the check is a
  // no-op for those backends.
  const syncAndFlush = async () => {
    await new Promise<void>((resolve, reject) => {
      moduleFS.syncfs(false, (err: Error | null) => {
        if (err) reject(err);
        else resolve();
      });
    });
    if (typeof (backend as any).flush === "function") {
      await (backend as any).flush();
    }
  };

  // Override syncToFs to flush tomefs + propagate to persistent storage
  adapter.syncToFs = async (_relaxedDurability?: boolean) => {
    if (!moduleFS) return;
    await syncAndFlush();
  };

  // Override closeFs to persist all state before shutdown.
  // During pg.close(), Postgres modifies files (WAL, temp cleanup), so we
  // must re-persist both pages AND metadata — not just flush pages.
  const originalCloseFs = adapter.closeFs.bind(adapter);
  adapter.closeFs = async () => {
    let syncError: Error | null = null;
    if (moduleFS) {
      try {
        await syncAndFlush();
      } catch (e) {
        syncError = e as Error;
      }
    }
    await originalCloseFs();
    if (syncError) throw syncError;
  };

  // Expose internals for testing
  Object.defineProperty(adapter, "storageBackend", {
    get: () => backend,
  });
  Object.defineProperty(adapter, "tomefsInstance", {
    get: () => tomefs,
  });
  Object.defineProperty(adapter, "pageCache", {
    get: () => tomefs?.pageCache,
  });

  return adapter;
}
