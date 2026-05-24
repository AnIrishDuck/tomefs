/**
 * PGlite hash index stress tests under cache pressure.
 *
 * Hash indexes have a fundamentally different on-disk structure from B-tree:
 *   - Metapage (page 0): global state, bucket count, high mask / low mask
 *   - Bucket pages: fixed-size hash buckets, each holding tuples hashing
 *     to the same bucket number
 *   - Overflow pages: chained from buckets when they fill up
 *   - Bitmap pages: track allocation of overflow pages
 *
 * This creates unique page access patterns under cache pressure:
 *   - Every hash lookup reads the metapage + one bucket page (minimum 2
 *     page accesses), plus overflow pages if the bucket has spilled
 *   - The metapage is touched on EVERY operation — under a 4-page cache,
 *     it competes with bucket pages and gets evicted between queries
 *   - Bucket splits (when load factor exceeds threshold) read the old
 *     bucket, write the old + new bucket, update the metapage, and may
 *     allocate overflow/bitmap pages — a burst of 4-6 page accesses
 *   - Unlike B-tree (root→internal→leaf traversal), hash probes jump
 *     directly to a random bucket page — no locality between queries
 *
 * These patterns are pathological for a small LRU cache: the metapage
 * is evicted between every query, and bucket pages are essentially
 * random-access with no temporal locality.
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

let harness: PGliteHarness | null = null;

afterEach(async () => {
  if (harness) {
    await harness.destroy();
    harness = null;
  }
});

const PRESSURE_CONFIGS: CacheSize[] = ["tiny", "small", "medium", "large"];

function describeScenario(
  name: string,
  scenarioFn: (h: PGliteHarness, cacheSize: CacheSize) => void | Promise<void>,
  fast = true,
) {
  describe(name, () => {
    for (const size of PRESSURE_CONFIGS) {
      const pages = CACHE_CONFIGS[size];
      const tag = fast && size !== "large" ? " @fast" : "";
      it(`cache=${size} (${pages} pages)${tag}`, async () => {
        harness = await createPGliteHarness(size);
        await scenarioFn(harness, size);
      });
    }
  });
}

// ---------------------------------------------------------------------------
// Scenario 1: CREATE INDEX USING hash on a populated table
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 1: hash index creation on populated table",
  async (h) => {
    const { pg } = h;

    await pg.query(
      `CREATE TABLE hash_pop (id SERIAL PRIMARY KEY, val INT, label TEXT)`,
    );

    for (let i = 0; i < 100; i++) {
      await pg.query(`INSERT INTO hash_pop (val, label) VALUES ($1, $2)`, [
        i * 7 % 200,
        `label-${i}-${"x".repeat(40)}`,
      ]);
    }

    await pg.query(`CREATE INDEX hash_pop_val ON hash_pop USING hash (val)`);

    // Hash index supports equality lookups (not range scans)
    const result = await pg.query(
      `SELECT label FROM hash_pop WHERE val = 49`,
    );
    expect(result.rows.length).toBeGreaterThan(0);
    for (const row of result.rows) {
      expect(row.label).toMatch(/^label-/);
    }

    const count = await pg.query(`SELECT COUNT(*)::int as c FROM hash_pop`);
    expect(count.rows[0].c).toBe(100);
  },
);

// ---------------------------------------------------------------------------
// Scenario 2: Hash index equality lookups with many distinct values
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 2: hash index equality lookups stress",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE hash_eq (id SERIAL, key INT)`);
    await pg.query(`CREATE INDEX hash_eq_key ON hash_eq USING hash (key)`);

    // Insert rows with many distinct hash keys to spread across buckets
    for (let i = 0; i < 80; i++) {
      await pg.query(`INSERT INTO hash_eq (key) VALUES ($1)`, [i * 13]);
    }

    // Probe each value — under tiny cache, every probe evicts the metapage
    for (let i = 0; i < 80; i++) {
      const result = await pg.query(
        `SELECT COUNT(*)::int as c FROM hash_eq WHERE key = $1`,
        [i * 13],
      );
      expect(result.rows[0].c).toBe(1);
    }

    // Non-existent values should return 0
    const miss = await pg.query(
      `SELECT COUNT(*)::int as c FROM hash_eq WHERE key = 999999`,
    );
    expect(miss.rows[0].c).toBe(0);
  },
);

// ---------------------------------------------------------------------------
// Scenario 3: Hash index on text column (variable-length keys)
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 3: hash index on text column",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE hash_text (id SERIAL, name TEXT)`);
    await pg.query(`CREATE INDEX hash_text_name ON hash_text USING hash (name)`);

    // Variable-length text keys hash differently and may cause uneven
    // bucket distribution
    const names: string[] = [];
    for (let i = 0; i < 60; i++) {
      const name = `user-${"a".repeat(i % 30)}-${i}`;
      names.push(name);
      await pg.query(`INSERT INTO hash_text (name) VALUES ($1)`, [name]);
    }

    // Lookup each name via hash index
    for (const name of names) {
      const result = await pg.query(
        `SELECT id FROM hash_text WHERE name = $1`,
        [name],
      );
      expect(result.rows.length).toBe(1);
    }
  },
);

// ---------------------------------------------------------------------------
// Scenario 4: Hash index with duplicate keys (overflow pages)
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 4: hash index with many duplicates (overflow stress)",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE hash_dup (id SERIAL, category INT)`);
    await pg.query(
      `CREATE INDEX hash_dup_cat ON hash_dup USING hash (category)`,
    );

    // Only 5 distinct categories but 100 rows — each bucket accumulates
    // ~20 entries, likely causing overflow pages in at least some buckets
    for (let i = 0; i < 100; i++) {
      await pg.query(`INSERT INTO hash_dup (category) VALUES ($1)`, [i % 5]);
    }

    for (let cat = 0; cat < 5; cat++) {
      const result = await pg.query(
        `SELECT COUNT(*)::int as c FROM hash_dup WHERE category = $1`,
        [cat],
      );
      expect(result.rows[0].c).toBe(20);
    }
  },
);

// ---------------------------------------------------------------------------
// Scenario 5: Hash index maintenance under UPDATE
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 5: hash index maintenance under UPDATE",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE hash_upd (id SERIAL PRIMARY KEY, val INT)`);
    await pg.query(`CREATE INDEX hash_upd_val ON hash_upd USING hash (val)`);

    for (let i = 0; i < 60; i++) {
      await pg.query(`INSERT INTO hash_upd (val) VALUES ($1)`, [i]);
    }

    // Update half the values — each UPDATE removes old hash entry and
    // inserts new one, potentially in a different bucket
    for (let i = 0; i < 30; i++) {
      await pg.query(`UPDATE hash_upd SET val = val + 1000 WHERE id = $1`, [
        i + 1,
      ]);
    }

    // Verify lookups for updated values
    for (let i = 0; i < 30; i++) {
      const result = await pg.query(
        `SELECT id FROM hash_upd WHERE val = $1`,
        [i + 1000],
      );
      expect(result.rows.length).toBe(1);
      expect(result.rows[0].id).toBe(i + 1);
    }

    // Verify lookups for unchanged values
    for (let i = 30; i < 60; i++) {
      const result = await pg.query(
        `SELECT id FROM hash_upd WHERE val = $1`,
        [i],
      );
      expect(result.rows.length).toBe(1);
    }
  },
);

// ---------------------------------------------------------------------------
// Scenario 6: Hash index + DELETE + VACUUM
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 6: hash index DELETE + VACUUM consistency",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE hash_del (id SERIAL PRIMARY KEY, val INT)`);
    await pg.query(`CREATE INDEX hash_del_val ON hash_del USING hash (val)`);

    for (let i = 0; i < 80; i++) {
      await pg.query(`INSERT INTO hash_del (val) VALUES ($1)`, [i]);
    }

    // Delete every other row
    for (let i = 0; i < 80; i += 2) {
      await pg.query(`DELETE FROM hash_del WHERE val = $1`, [i]);
    }

    // VACUUM cleans up dead tuples and hash index entries
    await pg.query(`VACUUM hash_del`);

    // Deleted values should not be found
    for (let i = 0; i < 80; i += 2) {
      const result = await pg.query(
        `SELECT COUNT(*)::int as c FROM hash_del WHERE val = $1`,
        [i],
      );
      expect(result.rows[0].c).toBe(0);
    }

    // Surviving values should still be found
    for (let i = 1; i < 80; i += 2) {
      const result = await pg.query(
        `SELECT COUNT(*)::int as c FROM hash_del WHERE val = $1`,
        [i],
      );
      expect(result.rows[0].c).toBe(1);
    }

    const total = await pg.query(
      `SELECT COUNT(*)::int as c FROM hash_del`,
    );
    expect(total.rows[0].c).toBe(40);
  },
);

// ---------------------------------------------------------------------------
// Scenario 7: Hash + B-tree indexes on same table
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 7: hash + B-tree indexes on same table",
  async (h) => {
    const { pg } = h;

    await pg.query(
      `CREATE TABLE hash_mixed (id SERIAL PRIMARY KEY, eq_col INT, range_col INT, label TEXT)`,
    );

    // Hash index for equality lookups
    await pg.query(
      `CREATE INDEX hash_mixed_eq ON hash_mixed USING hash (eq_col)`,
    );
    // B-tree index for range lookups
    await pg.query(
      `CREATE INDEX hash_mixed_range ON hash_mixed (range_col)`,
    );

    for (let i = 0; i < 80; i++) {
      await pg.query(
        `INSERT INTO hash_mixed (eq_col, range_col, label) VALUES ($1, $2, $3)`,
        [i % 20, i, `item-${i}`],
      );
    }

    // Hash index equality lookup
    const eqResult = await pg.query(
      `SELECT COUNT(*)::int as c FROM hash_mixed WHERE eq_col = 5`,
    );
    expect(eqResult.rows[0].c).toBe(4);

    // B-tree range lookup
    const rangeResult = await pg.query(
      `SELECT COUNT(*)::int as c FROM hash_mixed WHERE range_col BETWEEN 20 AND 40`,
    );
    expect(rangeResult.rows[0].c).toBe(21);

    // Combined: both indexes could be used (planner decides)
    const combined = await pg.query(
      `SELECT label FROM hash_mixed WHERE eq_col = 10 AND range_col < 50 ORDER BY range_col`,
    );
    expect(combined.rows.length).toBeGreaterThan(0);
    for (const row of combined.rows) {
      expect(row.label).toMatch(/^item-/);
    }
  },
);

// ---------------------------------------------------------------------------
// Scenario 8: Hash index persistence round-trip
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 8: hash index persistence round-trip",
  async (h) => {
    const { pg } = h;
    const backend = h.backend as SyncMemoryBackend;

    await pg.query(`CREATE TABLE hash_persist (id SERIAL PRIMARY KEY, val INT)`);
    await pg.query(
      `CREATE INDEX hash_persist_val ON hash_persist USING hash (val)`,
    );

    for (let i = 0; i < 50; i++) {
      await pg.query(`INSERT INTO hash_persist (val) VALUES ($1)`, [i * 7]);
    }

    // Sync and remount
    await h.syncToFs();
    await h.destroy();

    const maxPages = h.adapter.tomefsInstance?.pageCache?.capacity ?? 4096;
    harness = await createPGliteHarness({ cacheSize: maxPages, backend });

    // Verify hash index works after remount
    const result = await harness.pg.query(
      `SELECT id FROM hash_persist WHERE val = 49`,
    );
    expect(result.rows.length).toBe(1);

    // Check all values survive
    for (let i = 0; i < 50; i++) {
      const r = await harness.pg.query(
        `SELECT COUNT(*)::int as c FROM hash_persist WHERE val = $1`,
        [i * 7],
      );
      expect(r.rows[0].c).toBe(1);
    }

    const total = await harness.pg.query(
      `SELECT COUNT(*)::int as c FROM hash_persist`,
    );
    expect(total.rows[0].c).toBe(50);
  },
);

// ---------------------------------------------------------------------------
// Scenario 9: REINDEX on hash index
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 9: REINDEX rebuilds hash index",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE hash_reidx (id SERIAL PRIMARY KEY, val INT)`);
    await pg.query(
      `CREATE INDEX hash_reidx_val ON hash_reidx USING hash (val)`,
    );

    for (let i = 0; i < 80; i++) {
      await pg.query(`INSERT INTO hash_reidx (val) VALUES ($1)`, [i]);
    }

    // Churn to create dead tuples and stale hash entries
    for (let i = 0; i < 40; i++) {
      await pg.query(`UPDATE hash_reidx SET val = val + 500 WHERE id = $1`, [
        i + 1,
      ]);
    }

    // REINDEX rebuilds the hash index from scratch — scans the heap and
    // recomputes all hash bucket assignments
    await pg.query(`REINDEX INDEX hash_reidx_val`);

    const high = await pg.query(
      `SELECT COUNT(*)::int as c FROM hash_reidx WHERE val >= 500`,
    );
    expect(high.rows[0].c).toBe(40);

    const low = await pg.query(
      `SELECT COUNT(*)::int as c FROM hash_reidx WHERE val < 500`,
    );
    expect(low.rows[0].c).toBe(40);

    const total = await pg.query(
      `SELECT COUNT(*)::int as c FROM hash_reidx`,
    );
    expect(total.rows[0].c).toBe(80);
  },
);

// ---------------------------------------------------------------------------
// Scenario 10: Rapid insert-delete cycles forcing bucket splits + cleanup
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 10: rapid insert-delete cycles with hash index",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE hash_churn (id SERIAL PRIMARY KEY, val INT)`);
    await pg.query(
      `CREATE INDEX hash_churn_val ON hash_churn USING hash (val)`,
    );

    // Multiple rounds of insert + delete to stress bucket allocation
    // and deallocation under cache pressure
    for (let round = 0; round < 3; round++) {
      const base = round * 100;

      // Insert batch
      for (let i = 0; i < 40; i++) {
        await pg.query(`INSERT INTO hash_churn (val) VALUES ($1)`, [base + i]);
      }

      // Delete half
      for (let i = 0; i < 20; i++) {
        await pg.query(`DELETE FROM hash_churn WHERE val = $1`, [base + i]);
      }

      await pg.query(`VACUUM hash_churn`);
    }

    // Verify only surviving values exist
    const total = await pg.query(
      `SELECT COUNT(*)::int as c FROM hash_churn`,
    );
    expect(total.rows[0].c).toBe(60); // 3 rounds * 20 surviving

    // Spot-check a few values from each round
    for (let round = 0; round < 3; round++) {
      const base = round * 100;
      // Deleted values
      const deleted = await pg.query(
        `SELECT COUNT(*)::int as c FROM hash_churn WHERE val = $1`,
        [base + 5],
      );
      expect(deleted.rows[0].c).toBe(0);

      // Surviving values
      const alive = await pg.query(
        `SELECT COUNT(*)::int as c FROM hash_churn WHERE val = $1`,
        [base + 25],
      );
      expect(alive.rows[0].c).toBe(1);
    }
  },
);
