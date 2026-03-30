/**
 * PGlite + tomefs test harness.
 *
 * Provides helpers to create PGlite instances backed by tomefs at
 * various cache pressure levels. Each harness is independent — tests
 * don't share state.
 */

import { createTomeFSPGlite } from "../../src/pglite-fs.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import type { SyncStorageBackend } from "../../src/sync-storage-backend.js";

/** Cache size configurations matching workload test conventions. */
export const CACHE_CONFIGS = {
  tiny: 4,       // 32 KB — maximum eviction pressure
  small: 16,     // 128 KB — moderate eviction
  medium: 64,    // 512 KB — working set partially fits
  large: 4096,   // 32 MB — working set fits, baseline
} as const;

export type CacheSize = keyof typeof CACHE_CONFIGS;

export interface PGliteHarness {
  /** The PGlite instance. */
  pg: any;
  /** The tomefs PGlite adapter (for cache/backend inspection). */
  adapter: any;
  /** The storage backend (for persistence testing). */
  backend: SyncStorageBackend;
  /** Flush tomefs data to the backend (calls syncToFs). */
  syncToFs(): Promise<void>;
  /** Clean up resources. */
  destroy(): Promise<void>;
  /**
   * Simulate a dirty shutdown (e.g., browser tab crash).
   * Abandons the PGlite instance WITHOUT calling syncfs or close.
   * The backend retains whatever pages were evicted during operations
   * plus metadata from the last explicit syncToFs call. On remount,
   * Postgres must recover via WAL replay.
   */
  dirtyDestroy(): void;
}

export interface PGliteHarnessOptions {
  /** Cache size preset or explicit page count. */
  cacheSize: CacheSize | number;
  /** Existing backend to reuse (for persistence round-trip tests). */
  backend?: SyncStorageBackend;
}

/**
 * Create a PGlite instance backed by tomefs with a specific cache size.
 *
 * PGlite is imported dynamically to keep it as a devDependency.
 * Pass an existing backend to test persistence round-trips (remount).
 */
export async function createPGliteHarness(
  cacheSizeOrOptions: CacheSize | number | PGliteHarnessOptions,
): Promise<PGliteHarness> {
  const options: PGliteHarnessOptions =
    typeof cacheSizeOrOptions === "object" && "cacheSize" in cacheSizeOrOptions
      ? cacheSizeOrOptions
      : { cacheSize: cacheSizeOrOptions };

  const maxPages =
    typeof options.cacheSize === "number"
      ? options.cacheSize
      : CACHE_CONFIGS[options.cacheSize];

  const backend = options.backend ?? new SyncMemoryBackend();
  const { PGlite, MemoryFS } = await import("@electric-sql/pglite");
  const adapter = createTomeFSPGlite({ MemoryFS, backend, maxPages });

  const pg = new PGlite({ fs: adapter });
  await pg.waitReady;

  return {
    pg,
    adapter,
    backend,
    async syncToFs() {
      await adapter.syncToFs();
    },
    async destroy() {
      try {
        await pg.close();
      } catch (_e) {
        // Ignore "PGlite is closed" errors from double-close
      }
    },
    dirtyDestroy() {
      // Intentionally do NOT call pg.close() or syncToFs().
      // The backend retains partially-flushed state from cache evictions.
    },
  };
}
