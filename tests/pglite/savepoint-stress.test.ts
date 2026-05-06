/**
 * PGlite + tomefs savepoint and nested transaction stress tests.
 *
 * Savepoints are a common real-world PGlite pattern: application code wraps
 * operations in savepoints for graceful error handling within transactions.
 * Under cache pressure, savepoints exercise a fundamentally different
 * Postgres code path than simple transactions:
 *
 *   - SAVEPOINT creates a sub-transaction with its own WAL entries
 *   - ROLLBACK TO SAVEPOINT reverts heap/index changes from the sub-xact,
 *     but the outer transaction's changes (and pages) must survive
 *   - RELEASE SAVEPOINT commits sub-transaction state into the parent
 *   - Nested savepoints create a stack of sub-transaction states
 *
 * Under tiny cache (4 pages), dirty pages from a sub-transaction may be
 * evicted and flushed to the backend BEFORE the ROLLBACK TO SAVEPOINT.
 * Postgres must then undo those changes via WAL replay on the next read.
 * This is the most dangerous scenario for page cache correctness: the
 * backend has "committed" pages that Postgres considers rolled back.
 *
 * Ethos §8: "simulate real PGlite access patterns"
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
    await h.destroy();
  }
  harnesses = [];
});

const PRESSURE_CONFIGS: CacheSize[] = ["tiny", "small", "medium", "large"];

// ---------------------------------------------------------------------------
// Savepoint + release (happy path)
// ---------------------------------------------------------------------------

describe("Savepoint release preserves data", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];
    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await createPGliteHarness(size);
      harnesses.push(h);
      const { pg } = h;

      await pg.query(`CREATE TABLE sp_release (id INT, val TEXT)`);
      await pg.query(`BEGIN`);
      await pg.query(`INSERT INTO sp_release VALUES (1, 'before')`);
      await pg.query(`SAVEPOINT sp1`);
      await pg.query(`INSERT INTO sp_release VALUES (2, 'in-savepoint')`);
      await pg.query(`RELEASE SAVEPOINT sp1`);
      await pg.query(`COMMIT`);

      const res = await pg.query(
        `SELECT id, val FROM sp_release ORDER BY id`,
      );
      expect(res.rows).toEqual([
        { id: 1, val: "before" },
        { id: 2, val: "in-savepoint" },
      ]);
    });
  }
});

// ---------------------------------------------------------------------------
// Savepoint + rollback to savepoint
// ---------------------------------------------------------------------------

describe("Savepoint rollback discards sub-transaction data", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];
    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await createPGliteHarness(size);
      harnesses.push(h);
      const { pg } = h;

      await pg.query(`CREATE TABLE sp_rollback (id INT, val TEXT)`);
      await pg.query(`BEGIN`);
      await pg.query(`INSERT INTO sp_rollback VALUES (1, 'kept')`);
      await pg.query(`SAVEPOINT sp1`);
      await pg.query(`INSERT INTO sp_rollback VALUES (2, 'discarded')`);
      await pg.query(`UPDATE sp_rollback SET val = 'modified' WHERE id = 1`);
      await pg.query(`ROLLBACK TO SAVEPOINT sp1`);
      await pg.query(`INSERT INTO sp_rollback VALUES (3, 'after-rollback')`);
      await pg.query(`COMMIT`);

      const res = await pg.query(
        `SELECT id, val FROM sp_rollback ORDER BY id`,
      );
      expect(res.rows).toEqual([
        { id: 1, val: "kept" },
        { id: 3, val: "after-rollback" },
      ]);
    });
  }
});

// ---------------------------------------------------------------------------
// Nested savepoints (3 levels)
// ---------------------------------------------------------------------------

describe("Nested savepoints with mixed release/rollback", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];
    it(`cache=${size} (${pages} pages)`, async () => {
      const h = await createPGliteHarness(size);
      harnesses.push(h);
      const { pg } = h;

      await pg.query(`CREATE TABLE sp_nested (id INT, level TEXT)`);
      await pg.query(`BEGIN`);

      // Level 0: outer transaction
      await pg.query(`INSERT INTO sp_nested VALUES (1, 'outer')`);

      // Level 1: savepoint — will be released
      await pg.query(`SAVEPOINT sp_level1`);
      await pg.query(`INSERT INTO sp_nested VALUES (2, 'level1')`);

      // Level 2: nested savepoint — will be rolled back
      await pg.query(`SAVEPOINT sp_level2`);
      await pg.query(`INSERT INTO sp_nested VALUES (3, 'level2-discarded')`);
      await pg.query(`ROLLBACK TO SAVEPOINT sp_level2`);

      // Back at level 1: insert after nested rollback
      await pg.query(`INSERT INTO sp_nested VALUES (4, 'level1-after')`);

      // Level 2 again: this time release
      await pg.query(`SAVEPOINT sp_level2b`);
      await pg.query(`INSERT INTO sp_nested VALUES (5, 'level2b-kept')`);
      await pg.query(`RELEASE SAVEPOINT sp_level2b`);

      // Release level 1
      await pg.query(`RELEASE SAVEPOINT sp_level1`);

      await pg.query(`COMMIT`);

      const res = await pg.query(
        `SELECT id, level FROM sp_nested ORDER BY id`,
      );
      expect(res.rows).toEqual([
        { id: 1, level: "outer" },
        { id: 2, level: "level1" },
        { id: 4, level: "level1-after" },
        { id: 5, level: "level2b-kept" },
      ]);
    });
  }
});

// ---------------------------------------------------------------------------
// Savepoint rollback with bulk writes (cache rotation)
// ---------------------------------------------------------------------------

describe("Savepoint rollback after bulk writes forces page reversion", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];
    it(`cache=${size} (${pages} pages)`, async () => {
      const h = await createPGliteHarness(size);
      harnesses.push(h);
      const { pg } = h;

      await pg.query(`CREATE TABLE sp_bulk (id INT, payload TEXT)`);

      // Insert baseline data that must survive
      await pg.query(`BEGIN`);
      for (let i = 0; i < 20; i++) {
        await pg.query(`INSERT INTO sp_bulk VALUES ($1, $2)`, [
          i,
          `baseline-${i}`,
        ]);
      }

      // Savepoint: bulk-write enough data to rotate the cache
      await pg.query(`SAVEPOINT sp_bulk_write`);
      for (let i = 0; i < 30; i++) {
        await pg.query(`INSERT INTO sp_bulk VALUES ($1, $2)`, [
          100 + i,
          `x`.repeat(200),
        ]);
      }

      // Also update all baseline rows to dirty those pages
      await pg.query(
        `UPDATE sp_bulk SET payload = 'overwritten' WHERE id < 20`,
      );

      // Rollback the entire sub-transaction
      await pg.query(`ROLLBACK TO SAVEPOINT sp_bulk_write`);
      await pg.query(`COMMIT`);

      // Baseline data must be intact with original values
      const res = await pg.query(
        `SELECT id, payload FROM sp_bulk ORDER BY id`,
      );
      expect(res.rows.length).toBe(20);
      for (let i = 0; i < 20; i++) {
        expect(res.rows[i].id).toBe(i);
        expect(res.rows[i].payload).toBe(`baseline-${i}`);
      }
    });
  }
});

// ---------------------------------------------------------------------------
// Savepoint with index modifications
// ---------------------------------------------------------------------------

describe("Savepoint rollback reverts index entries", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];
    it(`cache=${size} (${pages} pages)`, async () => {
      const h = await createPGliteHarness(size);
      harnesses.push(h);
      const { pg } = h;

      await pg.query(`CREATE TABLE sp_idx (id INT PRIMARY KEY, val INT)`);
      await pg.query(`CREATE INDEX sp_idx_val ON sp_idx (val)`);

      await pg.query(`BEGIN`);
      for (let i = 0; i < 10; i++) {
        await pg.query(`INSERT INTO sp_idx VALUES ($1, $2)`, [i, i * 10]);
      }

      await pg.query(`SAVEPOINT sp_index`);

      // Insert rows that create new index entries
      for (let i = 10; i < 25; i++) {
        await pg.query(`INSERT INTO sp_idx VALUES ($1, $2)`, [i, i * 10]);
      }

      // Rollback: index entries for rows 10-24 must be reverted
      await pg.query(`ROLLBACK TO SAVEPOINT sp_index`);
      await pg.query(`COMMIT`);

      // Index scan should find exactly the original 10 rows
      const res = await pg.query(
        `SELECT id FROM sp_idx WHERE val >= 0 ORDER BY val`,
      );
      expect(res.rows.length).toBe(10);
      for (let i = 0; i < 10; i++) {
        expect(res.rows[i].id).toBe(i);
      }
    });
  }
});

// ---------------------------------------------------------------------------
// Savepoint + persistence round-trip
// ---------------------------------------------------------------------------

describe("Savepoint operations persist through syncToFs + remount", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];
    it(`cache=${size} (${pages} pages)`, async () => {
      const backend = new SyncMemoryBackend();

      // Phase 1: perform savepoint operations
      const h1 = await createPGliteHarness({ cacheSize: size, backend });
      harnesses.push(h1);

      await h1.pg.query(`CREATE TABLE sp_persist (id INT, phase TEXT)`);
      await h1.pg.query(`BEGIN`);
      await h1.pg.query(`INSERT INTO sp_persist VALUES (1, 'committed')`);
      await h1.pg.query(`SAVEPOINT sp1`);
      await h1.pg.query(`INSERT INTO sp_persist VALUES (2, 'rolled-back')`);
      await h1.pg.query(`ROLLBACK TO SAVEPOINT sp1`);
      await h1.pg.query(`INSERT INTO sp_persist VALUES (3, 'after-rollback')`);
      await h1.pg.query(`COMMIT`);

      await h1.syncToFs();
      await h1.destroy();

      // Phase 2: remount and verify
      const h2 = await createPGliteHarness({ cacheSize: size, backend });
      harnesses.push(h2);

      const res = await h2.pg.query(
        `SELECT id, phase FROM sp_persist ORDER BY id`,
      );
      expect(res.rows).toEqual([
        { id: 1, phase: "committed" },
        { id: 3, phase: "after-rollback" },
      ]);
    });
  }
});

// ---------------------------------------------------------------------------
// Multiple savepoint rollback cycles in one transaction
// ---------------------------------------------------------------------------

describe("Repeated savepoint rollback cycles", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];
    it(`cache=${size} (${pages} pages)`, async () => {
      const h = await createPGliteHarness(size);
      harnesses.push(h);
      const { pg } = h;

      await pg.query(`CREATE TABLE sp_cycles (id INT, attempt INT, val TEXT)`);
      await pg.query(`BEGIN`);

      let nextId = 1;

      // Simulate 5 retry cycles: attempt, fail, rollback, retry
      for (let attempt = 0; attempt < 5; attempt++) {
        await pg.query(`SAVEPOINT retry`);
        // Insert some rows in this attempt
        for (let j = 0; j < 3; j++) {
          await pg.query(`INSERT INTO sp_cycles VALUES ($1, $2, $3)`, [
            nextId,
            attempt,
            `attempt-${attempt}-row-${j}`,
          ]);
          nextId++;
        }
        if (attempt < 4) {
          // Rollback this attempt (simulate error)
          await pg.query(`ROLLBACK TO SAVEPOINT retry`);
          nextId -= 3; // IDs rolled back
        } else {
          // Last attempt succeeds
          await pg.query(`RELEASE SAVEPOINT retry`);
        }
      }

      await pg.query(`COMMIT`);

      const res = await pg.query(`SELECT * FROM sp_cycles ORDER BY id`);
      // Only the 5th attempt's 3 rows should exist
      expect(res.rows.length).toBe(3);
      for (let j = 0; j < 3; j++) {
        expect(res.rows[j].attempt).toBe(4);
        expect(res.rows[j].val).toBe(`attempt-4-row-${j}`);
      }
    });
  }
});

// ---------------------------------------------------------------------------
// Savepoint rollback under dirty-shutdown + WAL recovery
// ---------------------------------------------------------------------------

describe("Savepoint state survives dirty shutdown", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];
    it(`cache=${size} (${pages} pages)`, async () => {
      const backend = new SyncMemoryBackend();

      // Phase 1: committed savepoint data, then checkpoint
      const h1 = await createPGliteHarness({ cacheSize: size, backend });
      harnesses.push(h1);

      await h1.pg.query(`CREATE TABLE sp_crash (id INT, val TEXT)`);
      await h1.pg.query(`BEGIN`);
      await h1.pg.query(`INSERT INTO sp_crash VALUES (1, 'pre-savepoint')`);
      await h1.pg.query(`SAVEPOINT sp1`);
      await h1.pg.query(`INSERT INTO sp_crash VALUES (2, 'in-savepoint')`);
      await h1.pg.query(`RELEASE SAVEPOINT sp1`);
      await h1.pg.query(`COMMIT`);

      // Checkpoint to establish baseline
      await h1.syncToFs();

      // More work after checkpoint
      await h1.pg.query(`INSERT INTO sp_crash VALUES (3, 'post-checkpoint')`);

      // Dirty shutdown — no syncfs
      h1.dirtyDestroy();

      // Phase 2: remount on same backend, WAL replay recovers
      const h2 = await createPGliteHarness({ cacheSize: size, backend });
      harnesses.push(h2);

      const res = await h2.pg.query(
        `SELECT id, val FROM sp_crash ORDER BY id`,
      );
      // Rows 1 and 2 were committed + synced — must survive.
      // Row 3 was committed but not synced — may or may not survive
      // depending on cache pressure (dirty page eviction). We verify
      // at minimum the checkpointed data is intact.
      expect(res.rows.length).toBeGreaterThanOrEqual(2);
      expect(res.rows[0]).toEqual({ id: 1, val: "pre-savepoint" });
      expect(res.rows[1]).toEqual({ id: 2, val: "in-savepoint" });
    });
  }
});

// ---------------------------------------------------------------------------
// Savepoint with UPDATE rollback preserves original values
// ---------------------------------------------------------------------------

describe("Savepoint UPDATE rollback preserves original row data", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];
    it(`cache=${size} (${pages} pages)`, async () => {
      const h = await createPGliteHarness(size);
      harnesses.push(h);
      const { pg } = h;

      await pg.query(`CREATE TABLE sp_update (id INT PRIMARY KEY, counter INT, label TEXT)`);

      // Insert initial rows
      for (let i = 0; i < 15; i++) {
        await pg.query(`INSERT INTO sp_update VALUES ($1, $2, $3)`, [
          i,
          0,
          `original-${i}`,
        ]);
      }

      await pg.query(`BEGIN`);

      // Update some rows in outer transaction
      await pg.query(`UPDATE sp_update SET counter = 1 WHERE id < 5`);

      await pg.query(`SAVEPOINT sp_update1`);

      // Update ALL rows in savepoint (causes many dirty pages)
      await pg.query(
        `UPDATE sp_update SET counter = 99, label = 'overwritten'`,
      );

      // Rollback the savepoint
      await pg.query(`ROLLBACK TO SAVEPOINT sp_update1`);

      await pg.query(`COMMIT`);

      // Verify: rows 0-4 have counter=1 from outer tx, rest have counter=0
      const res = await pg.query(
        `SELECT id, counter, label FROM sp_update ORDER BY id`,
      );
      expect(res.rows.length).toBe(15);
      for (let i = 0; i < 15; i++) {
        expect(res.rows[i].id).toBe(i);
        expect(res.rows[i].counter).toBe(i < 5 ? 1 : 0);
        expect(res.rows[i].label).toBe(`original-${i}`);
      }
    });
  }
});

// ---------------------------------------------------------------------------
// Exception-driven savepoint pattern (try/catch)
// ---------------------------------------------------------------------------

describe("Exception-driven savepoint pattern", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];
    it(`cache=${size} (${pages} pages)`, async () => {
      const h = await createPGliteHarness(size);
      harnesses.push(h);
      const { pg } = h;

      await pg.query(
        `CREATE TABLE sp_exception (id INT PRIMARY KEY, val TEXT)`,
      );

      await pg.query(`BEGIN`);

      // Successful insert
      await pg.query(`INSERT INTO sp_exception VALUES (1, 'ok')`);

      // Simulate application-level try/catch with savepoint
      await pg.query(`SAVEPOINT try_block`);
      try {
        await pg.query(`INSERT INTO sp_exception VALUES (2, 'will-conflict')`);
        // Deliberately cause a unique violation
        await pg.query(`INSERT INTO sp_exception VALUES (2, 'duplicate')`);
        // Should not reach here
        expect.unreachable("should have thrown on duplicate key");
      } catch (_e) {
        await pg.query(`ROLLBACK TO SAVEPOINT try_block`);
      }

      // Continue after the caught error
      await pg.query(`INSERT INTO sp_exception VALUES (3, 'after-catch')`);
      await pg.query(`COMMIT`);

      const res = await pg.query(
        `SELECT id, val FROM sp_exception ORDER BY id`,
      );
      expect(res.rows).toEqual([
        { id: 1, val: "ok" },
        { id: 3, val: "after-catch" },
      ]);
    });
  }
});

// ---------------------------------------------------------------------------
// Savepoint + DELETE rollback
// ---------------------------------------------------------------------------

describe("Savepoint DELETE rollback preserves rows", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];
    it(`cache=${size} (${pages} pages)`, async () => {
      const h = await createPGliteHarness(size);
      harnesses.push(h);
      const { pg } = h;

      await pg.query(`CREATE TABLE sp_delete (id INT, val TEXT)`);

      // Insert 20 rows
      for (let i = 0; i < 20; i++) {
        await pg.query(`INSERT INTO sp_delete VALUES ($1, $2)`, [
          i,
          `row-${i}`,
        ]);
      }

      await pg.query(`BEGIN`);
      await pg.query(`SAVEPOINT sp_del`);

      // Delete all rows
      await pg.query(`DELETE FROM sp_delete`);

      // Verify they're gone within the savepoint
      const mid = await pg.query(`SELECT COUNT(*)::int as c FROM sp_delete`);
      expect(mid.rows[0].c).toBe(0);

      // Rollback the delete
      await pg.query(`ROLLBACK TO SAVEPOINT sp_del`);
      await pg.query(`COMMIT`);

      // All 20 rows must be back
      const res = await pg.query(
        `SELECT id, val FROM sp_delete ORDER BY id`,
      );
      expect(res.rows.length).toBe(20);
      for (let i = 0; i < 20; i++) {
        expect(res.rows[i]).toEqual({ id: i, val: `row-${i}` });
      }
    });
  }
});
