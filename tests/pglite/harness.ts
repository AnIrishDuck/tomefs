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
  /** Clean up resources. */
  destroy(): Promise<void>;
}

/**
 * Create a PGlite instance backed by tomefs with a specific cache size.
 *
 * PGlite is imported dynamically to keep it as a devDependency.
 */
export async function createPGliteHarness(
  cacheSize: CacheSize | number,
): Promise<PGliteHarness> {
  const maxPages =
    typeof cacheSize === "number" ? cacheSize : CACHE_CONFIGS[cacheSize];

  const backend = new SyncMemoryBackend();
  const { PGlite, MemoryFS } = await import("@electric-sql/pglite");
  const adapter = createTomeFSPGlite({ MemoryFS, backend, maxPages });

  const pg = new PGlite({ fs: adapter });
  await pg.waitReady;

  return {
    pg,
    adapter,
    backend,
    async destroy() {
      await pg.close();
    },
  };
}
