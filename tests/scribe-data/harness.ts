/**
 * Scribe-data integration test harness.
 *
 * Creates PGlite instances backed by tomefs with a FakeServer for sync
 * simulation. Each harness is isolated — no shared state between tests.
 *
 * Follows the same cache pressure conventions as tests/pglite/harness.ts
 * and tests/workload/harness.ts.
 */

import { createTomeFSPGlite } from "../../src/pglite-fs.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import type { SyncStorageBackend } from "../../src/sync-storage-backend.js";
import { FakeServer, type SyncStatus, type Blob } from "./fake-tributary.js";

/** Cache size configurations matching existing test conventions. */
export const CACHE_CONFIGS = {
  tiny: 4,       // 32 KB — maximum eviction pressure
  small: 16,     // 128 KB — moderate eviction
  medium: 64,    // 512 KB — working set partially fits
  large: 4096,   // 32 MB — working set fits, baseline
} as const;

export type CacheSize = keyof typeof CACHE_CONFIGS;

/**
 * A test stream backed by PGlite-on-tomefs.
 *
 * Streams model tributary's TributaryStream concept: each stream has its
 * own schema (namespace) and sync state. Writers produce blobs (SQL) on
 * the fake server; readers sync by fetching blobs in pages and replaying
 * SQL against PGlite.
 */
export interface TestStream {
  /** Stream identifier (also the PG schema name). */
  name: string;
  /** The stream key on the fake server. */
  streamKey: string;
  /** Current sync position (last replayed sequence number). */
  syncIndex: number;
  /** Execute SQL against this stream's schema. */
  exec(sql: string, params?: any[]): Promise<void>;
  /** Query SQL against this stream's schema. */
  query<T = any>(sql: string, params?: any[]): Promise<{ rows: T[] }>;
}

export interface ScribeTestHarness {
  /** PGlite instance backed by tomefs. */
  pg: any;
  /** The tomefs PGlite adapter (for cache/backend inspection). */
  adapter: any;
  /** The storage backend. */
  backend: SyncStorageBackend;
  /** Fake server for blob storage. */
  server: FakeServer;

  /**
   * Create a writer stream. Sets up a PG schema with scribe-like tables
   * and returns a stream that can write SQL blobs to the fake server.
   */
  createWriterStream(name: string): Promise<TestStream>;

  /**
   * Create a reader stream on the same PGlite instance. The reader syncs
   * from the fake server by fetching blobs in pages and replaying SQL.
   */
  createReaderStream(name: string): Promise<TestStream>;

  /**
   * Sync a reader stream by fetching one page of blobs from the server
   * and replaying them as SQL. Returns sync status.
   */
  syncStream(stream: TestStream, max: number): Promise<SyncStatus>;

  /**
   * Sync a stream fully (all pages) with a given page size.
   * Returns the total number of iterations.
   */
  syncStreamFully(stream: TestStream, max: number): Promise<number>;

  /** Clean up resources. */
  destroy(): Promise<void>;
}

/**
 * SQL schema for a scribe-like stream. Each stream gets its own PG schema
 * to simulate tributary's multi-library isolation.
 */
function schemaSQL(schemaName: string): string {
  return `
    CREATE SCHEMA IF NOT EXISTS "${schemaName}";

    CREATE TABLE IF NOT EXISTS "${schemaName}".block (
      id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      content TEXT NOT NULL DEFAULT '',
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS "${schemaName}".block_version (
      id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      block_id TEXT NOT NULL REFERENCES "${schemaName}".block(id),
      content TEXT NOT NULL DEFAULT '',
      version_number INTEGER NOT NULL DEFAULT 1,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS "${schemaName}".collection (
      id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      name TEXT NOT NULL,
      parent_id TEXT REFERENCES "${schemaName}".collection(id),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS "${schemaName}".block_collection (
      block_id TEXT NOT NULL REFERENCES "${schemaName}".block(id),
      collection_id TEXT NOT NULL REFERENCES "${schemaName}".collection(id),
      PRIMARY KEY (block_id, collection_id)
    );

    CREATE TABLE IF NOT EXISTS "${schemaName}".block_search_index (
      block_id TEXT PRIMARY KEY REFERENCES "${schemaName}".block(id),
      search_vector TSVECTOR,
      indexed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_block_search_${schemaName}
      ON "${schemaName}".block_search_index USING GIN (search_vector);

    CREATE TABLE IF NOT EXISTS "${schemaName}".sync_state (
      stream_key TEXT PRIMARY KEY,
      last_sequence INTEGER NOT NULL DEFAULT 0,
      last_hash TEXT NOT NULL DEFAULT 'genesis',
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `;
}

/**
 * Create a scribe-data test harness.
 */
export async function createHarness(
  cacheSize: CacheSize | number,
): Promise<ScribeTestHarness> {
  const maxPages =
    typeof cacheSize === "number" ? cacheSize : CACHE_CONFIGS[cacheSize];

  const backend = new SyncMemoryBackend();
  const server = new FakeServer();

  const { PGlite, MemoryFS } = await import("@electric-sql/pglite");
  const adapter = createTomeFSPGlite({ MemoryFS, backend, maxPages });
  const pg = new PGlite({ fs: adapter });
  await pg.waitReady;

  /** Track which schemas have been initialized. */
  const initializedSchemas = new Set<string>();

  async function ensureSchema(name: string): Promise<void> {
    if (initializedSchemas.has(name)) return;
    await pg.exec(schemaSQL(name));
    initializedSchemas.add(name);
  }

  function makeStream(name: string, streamKey: string): TestStream {
    return {
      name,
      streamKey,
      syncIndex: 0,
      async exec(sql: string, params?: any[]): Promise<void> {
        if (params && params.length > 0) {
          await pg.query(sql, params);
        } else {
          await pg.exec(sql);
        }
      },
      async query<T = any>(sql: string, params?: any[]): Promise<{ rows: T[] }> {
        return pg.query(sql, params);
      },
    };
  }

  return {
    pg,
    adapter,
    backend,
    server,

    async createWriterStream(name: string): Promise<TestStream> {
      const streamKey = `writer-${name}`;
      await ensureSchema(name);
      return makeStream(name, streamKey);
    },

    async createReaderStream(name: string): Promise<TestStream> {
      const streamKey = `writer-${name}`;
      await ensureSchema(name);

      // Load persisted sync state if any
      const state = await pg.query(
        `SELECT last_sequence FROM "${name}".sync_state WHERE stream_key = $1`,
        [streamKey],
      );
      const stream = makeStream(name, streamKey);
      if (state.rows.length > 0) {
        stream.syncIndex = state.rows[0].last_sequence;
      }
      return stream;
    },

    async syncStream(stream: TestStream, max: number): Promise<SyncStatus> {
      const page = server.getBlobsPage(stream.streamKey, stream.syncIndex, max);

      // Replay each blob's SQL within a transaction
      if (page.blobs.length > 0) {
        await pg.exec("BEGIN");
        try {
          for (const blob of page.blobs) {
            await pg.exec(blob.data);
          }

          // Update sync state
          const lastBlob = page.blobs[page.blobs.length - 1];
          await pg.query(
            `INSERT INTO "${stream.name}".sync_state (stream_key, last_sequence, last_hash, updated_at)
             VALUES ($1, $2, $3, NOW())
             ON CONFLICT (stream_key) DO UPDATE
             SET last_sequence = $2, last_hash = $3, updated_at = NOW()`,
            [stream.streamKey, lastBlob.sequence, lastBlob.hash],
          );

          await pg.exec("COMMIT");
          stream.syncIndex = lastBlob.sequence;
        } catch (e) {
          await pg.exec("ROLLBACK");
          throw e;
        }
      }

      return {
        fetched: page.blobs.length,
        total: page.totalCount,
        complete: !page.hasMore,
      };
    },

    async syncStreamFully(stream: TestStream, max: number): Promise<number> {
      let iterations = 0;
      let complete = false;
      while (!complete) {
        const status = await this.syncStream(stream, max);
        complete = status.complete;
        iterations++;
        if (iterations > 200) {
          throw new Error(`syncStreamFully: exceeded 200 iterations`);
        }
      }
      return iterations;
    },

    async destroy(): Promise<void> {
      await pg.close();
    },
  };
}
