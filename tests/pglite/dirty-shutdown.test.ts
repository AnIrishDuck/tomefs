/**
 * PGlite + tomefs dirty shutdown tests (WAL replay on remount).
 *
 * All existing persistence tests perform clean shutdowns: syncToFs() before
 * destroy(). In production, browser tabs crash mid-operation. Under cache
 * pressure, dirty pages are evicted to the backend during normal operations
 * (before any explicit sync). A "dirty shutdown" leaves the backend in an
 * inconsistent state: pages from recent writes exist but metadata reflects
 * the last syncToFs checkpoint.
 *
 * On remount, restoreTree detects the page/metadata mismatch and adjusts
 * the file size. Postgres then performs WAL replay to recover to a
 * consistent state.
 *
 * These tests verify the full crash recovery path:
 *   1. Checkpoint (syncToFs) → establishes a known-good baseline
 *   2. Additional operations → pages evicted to backend under pressure
 *   3. Dirty shutdown (dirtyDestroy) → abandon without sync
 *   4. Remount on same backend → restoreTree + WAL replay
 *   5. Verify data integrity
 *
 * Ethos §8: "simulate real PGlite access patterns — startup, queries,
 * vacuums, WAL replay"
 * Ethos §9: "target the seams"
 */

import { describe, it, expect, afterEach } from "vitest";
import {
  createPGliteHarness,
  CACHE_CONFIGS,
  type CacheSize,
  type PGliteHarness,
} from "./harness.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";

let harnesses: PGliteHarness[] = [];

afterEach(async () => {
  for (const h of harnesses) {
    try {
      await h.destroy();
    } catch (_e) {
      // May already be dirty-destroyed
    }
  }
  harnesses = [];
});

async function create(
  cacheSize: CacheSize | number,
  backend?: SyncMemoryBackend,
): Promise<PGliteHarness> {
  const h = await createPGliteHarness(
    backend ? { cacheSize, backend } : { cacheSize },
  );
  harnesses.push(h);
  return h;
}

const PRESSURE_CONFIGS: CacheSize[] = ["tiny", "small", "medium", "large"];

// ---------------------------------------------------------------------------
// Scenario 1: Insert after checkpoint, dirty shutdown, verify checkpoint data
// ---------------------------------------------------------------------------

describe("Dirty shutdown: checkpoint data survives", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const backend = new SyncMemoryBackend();

      // Phase 1: insert and checkpoint
      const h1 = await create(size, backend);
      await h1.pg.query(
        `CREATE TABLE items (id SERIAL PRIMARY KEY, value TEXT)`,
      );
      for (let i = 0; i < 20; i++) {
        await h1.pg.query(`INSERT INTO items (value) VALUES ($1)`, [
          `checkpoint-${i}-${"x".repeat(100)}`,
        ]);
      }
      await h1.syncToFs();

      // Phase 2: more inserts WITHOUT syncing, then dirty shutdown
      for (let i = 0; i < 10; i++) {
        await h1.pg.query(`INSERT INTO items (value) VALUES ($1)`, [
          `post-checkpoint-${i}-${"y".repeat(100)}`,
        ]);
      }
      h1.dirtyDestroy();

      // Phase 3: remount — Postgres must recover via WAL replay
      const h2 = await create(size, backend);

      // At minimum, the 20 checkpointed rows must survive.
      // The 10 post-checkpoint rows MAY survive (if WAL replay recovers
      // them) or may not (if WAL was incomplete). Either is correct —
      // what matters is that the database is consistent and queryable.
      const result = await h2.pg.query(
        `SELECT COUNT(*)::int as count FROM items`,
      );
      expect(result.rows[0].count).toBeGreaterThanOrEqual(20);

      // Verify checkpointed data integrity
      const checkpoint = await h2.pg.query(
        `SELECT id, value FROM items WHERE value LIKE 'checkpoint-%' ORDER BY id`,
      );
      expect(checkpoint.rows.length).toBe(20);
      for (let i = 0; i < 20; i++) {
        expect(checkpoint.rows[i].value).toBe(
          `checkpoint-${i}-${"x".repeat(100)}`,
        );
      }
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 2: Update storm after checkpoint, dirty shutdown
// ---------------------------------------------------------------------------

describe("Dirty shutdown: updates after checkpoint", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages)`, async () => {
      const backend = new SyncMemoryBackend();

      // Phase 1: seed data and checkpoint
      const h1 = await create(size, backend);
      await h1.pg.query(
        `CREATE TABLE counters (id SERIAL PRIMARY KEY, value INTEGER DEFAULT 0)`,
      );
      for (let i = 0; i < 10; i++) {
        await h1.pg.query(`INSERT INTO counters (value) VALUES (0)`);
      }

      // Committed update: increment all rows
      await h1.pg.query(`UPDATE counters SET value = value + 100`);
      await h1.syncToFs();

      // Phase 2: heavy updates without syncing
      for (let round = 0; round < 10; round++) {
        await h1.pg.query(`UPDATE counters SET value = value + 1`);
      }
      h1.dirtyDestroy();

      // Phase 3: remount — verify at least checkpoint state
      const h2 = await create(size, backend);
      const result = await h2.pg.query(
        `SELECT id, value FROM counters ORDER BY id`,
      );
      expect(result.rows.length).toBe(10);
      for (const row of result.rows) {
        // All rows must have at least the checkpoint value (100).
        // WAL replay may recover some or all of the +10 updates.
        expect(row.value).toBeGreaterThanOrEqual(100);
        // But never more than 110 (checkpoint 100 + 10 post-checkpoint rounds)
        expect(row.value).toBeLessThanOrEqual(110);
        // All rows should have the same value (updates were uniform)
        expect(row.value).toBe(result.rows[0].value);
      }
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 3: Transaction rollback before dirty shutdown
// ---------------------------------------------------------------------------

describe("Dirty shutdown: rolled-back transaction does not persist", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const backend = new SyncMemoryBackend();

      // Phase 1: seed + checkpoint
      const h1 = await create(size, backend);
      await h1.pg.query(
        `CREATE TABLE ledger (id SERIAL PRIMARY KEY, amount INTEGER)`,
      );
      for (let i = 0; i < 15; i++) {
        await h1.pg.query(`INSERT INTO ledger (amount) VALUES (100)`);
      }
      await h1.syncToFs();

      // Phase 2: committed update, then rolled-back update, then crash
      await h1.pg.query("BEGIN");
      await h1.pg.query(`UPDATE ledger SET amount = amount + 50`);
      await h1.pg.query("COMMIT");

      // Rolled-back transaction — under cache pressure, dirty pages
      // from this may be evicted to the backend before rollback completes
      await h1.pg.query("BEGIN");
      await h1.pg.query(`UPDATE ledger SET amount = amount + 9999`);
      await h1.pg.query("ROLLBACK");

      h1.dirtyDestroy();

      // Phase 3: remount — rolled-back data must not appear
      const h2 = await create(size, backend);
      const result = await h2.pg.query(
        `SELECT DISTINCT amount FROM ledger`,
      );

      // Every row should be either 100 (checkpoint) or 150 (+50 committed).
      // WAL replay may or may not recover the +50 commit, but the
      // rolled-back +9999 must NEVER appear.
      for (const row of result.rows) {
        expect(row.amount).toBeLessThanOrEqual(150);
        expect(row.amount).toBeGreaterThanOrEqual(100);
      }

      // All rows should be consistent (same value)
      const values = result.rows.map((r: any) => r.amount);
      expect(new Set(values).size).toBe(1);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 4: Multiple checkpoint-crash cycles
// ---------------------------------------------------------------------------

describe("Dirty shutdown: repeated checkpoint-crash cycles", () => {
  for (const size of ["tiny", "small"] as CacheSize[]) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages)`, async () => {
      const backend = new SyncMemoryBackend();

      // Cycle 1: create + insert + checkpoint
      let h = await create(size, backend);
      await h.pg.query(
        `CREATE TABLE log (id SERIAL PRIMARY KEY, cycle INTEGER, msg TEXT)`,
      );
      for (let i = 0; i < 10; i++) {
        await h.pg.query(`INSERT INTO log (cycle, msg) VALUES (1, $1)`, [
          `c1-${i}`,
        ]);
      }
      await h.syncToFs();
      // More inserts, then dirty crash
      for (let i = 0; i < 5; i++) {
        await h.pg.query(`INSERT INTO log (cycle, msg) VALUES (1, $1)`, [
          `c1-unsaved-${i}`,
        ]);
      }
      h.dirtyDestroy();

      // Cycle 2: remount, insert more, checkpoint, crash
      h = await create(size, backend);
      harnesses.push(h);
      const c1Count = await h.pg.query(
        `SELECT COUNT(*)::int as count FROM log WHERE cycle = 1`,
      );
      // At least the 10 checkpointed rows from cycle 1
      expect(c1Count.rows[0].count).toBeGreaterThanOrEqual(10);

      for (let i = 0; i < 10; i++) {
        await h.pg.query(`INSERT INTO log (cycle, msg) VALUES (2, $1)`, [
          `c2-${i}`,
        ]);
      }
      await h.syncToFs();
      // Unsaved work
      await h.pg.query(`INSERT INTO log (cycle, msg) VALUES (2, 'unsaved')`);
      h.dirtyDestroy();

      // Cycle 3: final remount and verify
      h = await create(size, backend);
      harnesses.push(h);
      const total = await h.pg.query(
        `SELECT COUNT(*)::int as count FROM log`,
      );
      // Must have at least 10 (cycle 1 checkpoint) + 10 (cycle 2 checkpoint)
      expect(total.rows[0].count).toBeGreaterThanOrEqual(20);

      // Cycle 2 checkpointed rows must all be present
      const c2 = await h.pg.query(
        `SELECT msg FROM log WHERE cycle = 2 AND msg LIKE 'c2-%' ORDER BY msg`,
      );
      expect(c2.rows.length).toBeGreaterThanOrEqual(10);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 5: DDL after checkpoint, dirty shutdown
// ---------------------------------------------------------------------------

describe("Dirty shutdown: DDL after checkpoint", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const backend = new SyncMemoryBackend();

      // Phase 1: create base table + checkpoint
      const h1 = await create(size, backend);
      await h1.pg.query(
        `CREATE TABLE config (key TEXT PRIMARY KEY, val TEXT)`,
      );
      await h1.pg.query(
        `INSERT INTO config (key, val) VALUES ('version', '1.0')`,
      );
      await h1.syncToFs();

      // Phase 2: DDL changes without syncing
      await h1.pg.query(
        `CREATE TABLE events (id SERIAL PRIMARY KEY, name TEXT)`,
      );
      await h1.pg.query(`INSERT INTO events (name) VALUES ('boot')`);
      await h1.pg.query(`ALTER TABLE config ADD COLUMN updated_at TEXT`);
      await h1.pg.query(
        `UPDATE config SET updated_at = 'now' WHERE key = 'version'`,
      );
      h1.dirtyDestroy();

      // Phase 3: remount — at minimum, checkpointed state must survive.
      // DDL changes may or may not survive depending on WAL replay.
      const h2 = await create(size, backend);

      // config table and its data must exist (checkpointed)
      const config = await h2.pg.query(
        `SELECT val FROM config WHERE key = 'version'`,
      );
      expect(config.rows[0].val).toBe("1.0");

      // Verify database is consistent and queryable regardless of which
      // DDL changes survived
      const tables = await h2.pg.query(`
        SELECT tablename FROM pg_tables
        WHERE schemaname = 'public'
        ORDER BY tablename
      `);
      expect(tables.rows.map((r: any) => r.tablename)).toContain("config");

      // Must be able to insert new data
      await h2.pg.query(
        `INSERT INTO config (key, val) VALUES ('test', 'ok')`,
      );
      const check = await h2.pg.query(
        `SELECT val FROM config WHERE key = 'test'`,
      );
      expect(check.rows[0].val).toBe("ok");
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 6: Large dataset dirty shutdown under maximum pressure
// ---------------------------------------------------------------------------

describe("Dirty shutdown: large dataset under tiny cache", () => {
  it("cache=tiny (4 pages) — heavy eviction during operations", async () => {
    const backend = new SyncMemoryBackend();

    // Phase 1: large dataset that far exceeds cache
    const h1 = await create("tiny", backend);
    await h1.pg.query(
      `CREATE TABLE big (id SERIAL PRIMARY KEY, data TEXT, hash TEXT)`,
    );
    await h1.pg.query(`CREATE INDEX idx_big_hash ON big (hash)`);

    for (let i = 0; i < 50; i++) {
      const data = `row-${i}-${"z".repeat(200)}`;
      await h1.pg.query(
        `INSERT INTO big (data, hash) VALUES ($1, $2)`,
        [data, `hash-${i}`],
      );
    }
    await h1.syncToFs();

    // Phase 2: mixed operations without syncing
    // DELETE + INSERT causes page reuse — dangerous under cache pressure
    await h1.pg.query(`DELETE FROM big WHERE id <= 10`);
    for (let i = 50; i < 60; i++) {
      await h1.pg.query(
        `INSERT INTO big (data, hash) VALUES ($1, $2)`,
        [`new-${i}-${"w".repeat(200)}`, `hash-${i}`],
      );
    }
    // Update to cause more dirty pages
    await h1.pg.query(
      `UPDATE big SET data = data || '-modified' WHERE id > 40 AND id <= 50`,
    );
    h1.dirtyDestroy();

    // Phase 3: remount — verify recovery
    const h2 = await create("tiny", backend);

    // At minimum: the 50 checkpointed rows should be accessible,
    // minus however many the WAL replay does or doesn't recover
    const total = await h2.pg.query(
      `SELECT COUNT(*)::int as count FROM big`,
    );
    // At least the 40 rows that weren't deleted at checkpoint time
    // (checkpoint had 50 rows, post-checkpoint deleted 10 and added 10)
    expect(total.rows[0].count).toBeGreaterThanOrEqual(40);

    // Index must still work
    const indexed = await h2.pg.query(
      `SELECT id FROM big WHERE hash = 'hash-25'`,
    );
    expect(indexed.rows.length).toBe(1);

    // Can still write new data
    await h2.pg.query(
      `INSERT INTO big (data, hash) VALUES ('recovery-test', 'recovery')`,
    );
    const check = await h2.pg.query(
      `SELECT hash FROM big WHERE hash = 'recovery'`,
    );
    expect(check.rows[0].hash).toBe("recovery");
  });
});

// ---------------------------------------------------------------------------
// Scenario 7: Dirty shutdown immediately after checkpoint (no post-checkpoint ops)
// ---------------------------------------------------------------------------

describe("Dirty shutdown: crash immediately after checkpoint", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const backend = new SyncMemoryBackend();

      const h1 = await create(size, backend);
      await h1.pg.query(
        `CREATE TABLE clean (id SERIAL PRIMARY KEY, value INTEGER)`,
      );
      for (let i = 0; i < 25; i++) {
        await h1.pg.query(`INSERT INTO clean (value) VALUES ($1)`, [i * 10]);
      }
      await h1.syncToFs();
      // Dirty shutdown with no additional operations — this tests that
      // checkpoint state is fully self-consistent for recovery
      h1.dirtyDestroy();

      const h2 = await create(size, backend);
      const result = await h2.pg.query(
        `SELECT id, value FROM clean ORDER BY id`,
      );
      expect(result.rows.length).toBe(25);
      for (let i = 0; i < 25; i++) {
        expect(result.rows[i].value).toBe(i * 10);
      }
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 8: VACUUM after checkpoint, then dirty shutdown
// ---------------------------------------------------------------------------

describe("Dirty shutdown: VACUUM after checkpoint", () => {
  for (const size of ["tiny", "small", "medium"] as CacheSize[]) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages)`, async () => {
      const backend = new SyncMemoryBackend();

      // Phase 1: create data, delete half, checkpoint
      const h1 = await create(size, backend);
      await h1.pg.query(
        `CREATE TABLE vactest (id SERIAL PRIMARY KEY, keep BOOLEAN, data TEXT)`,
      );
      for (let i = 0; i < 40; i++) {
        await h1.pg.query(
          `INSERT INTO vactest (keep, data) VALUES ($1, $2)`,
          [i % 2 === 0, `data-${i}-${"v".repeat(100)}`],
        );
      }
      await h1.pg.query(`DELETE FROM vactest WHERE keep = false`);
      await h1.syncToFs();

      // Phase 2: VACUUM (heavy I/O) without syncing, then crash
      await h1.pg.query(`VACUUM vactest`);
      // Insert after VACUUM to generate more WAL
      for (let i = 0; i < 5; i++) {
        await h1.pg.query(
          `INSERT INTO vactest (keep, data) VALUES (true, $1)`,
          [`post-vacuum-${i}`],
        );
      }
      h1.dirtyDestroy();

      // Phase 3: remount — recovery after mid-VACUUM crash
      const h2 = await create(size, backend);

      // At minimum: the 20 surviving rows from checkpoint
      const count = await h2.pg.query(
        `SELECT COUNT(*)::int as count FROM vactest`,
      );
      expect(count.rows[0].count).toBeGreaterThanOrEqual(20);

      // All surviving rows from checkpoint should have keep=true
      const keepCheck = await h2.pg.query(
        `SELECT COUNT(*)::int as count FROM vactest WHERE keep = false`,
      );
      expect(keepCheck.rows[0].count).toBe(0);

      // Database must be functional
      await h2.pg.query(
        `INSERT INTO vactest (keep, data) VALUES (true, 'after-recovery')`,
      );
      const verify = await h2.pg.query(
        `SELECT data FROM vactest WHERE data = 'after-recovery'`,
      );
      expect(verify.rows.length).toBe(1);
    });
  }
});
