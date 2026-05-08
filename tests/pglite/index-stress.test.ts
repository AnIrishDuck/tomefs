/**
 * PGlite index operation stress tests under cache pressure.
 *
 * Index operations have uniquely demanding page access patterns for a bounded
 * cache. CREATE INDEX performs a full sequential scan of the heap table while
 * simultaneously building a btree with random leaf page insertions. Under tiny
 * cache (4 pages = 32KB), heap pages and btree pages compete for the same
 * cache slots — every heap page read during the scan can evict a btree page
 * that's needed for the next insertion, and vice versa.
 *
 * Index scans (btree traversal) have a very different access pattern from
 * sequential scans: they descend the btree from root to leaf (random pages),
 * then follow the leaf chain (sequential), then fetch heap pages for matching
 * rows (random again). Under cache pressure, the root/internal pages get
 * evicted between scans, forcing repeated re-reads.
 *
 * These tests exercise:
 *   - CREATE INDEX on a populated table (heap scan + btree build)
 *   - UNIQUE INDEX with constraint enforcement
 *   - Multi-column composite indexes
 *   - Multiple independent indexes on the same table
 *   - Partial indexes (CREATE INDEX ... WHERE)
 *   - Expression indexes (CREATE INDEX ON ... (lower(col)))
 *   - Index-guided ORDER BY (avoid explicit sort)
 *   - Index maintenance under UPDATE (insert new + delete old entry)
 *   - DROP INDEX + recreate (cleanup + rebuild)
 *   - Index + persistence round-trip (sync → remount → verify)
 *   - REINDEX (drop + rebuild in place)
 *   - Range scan using index (btree traversal + heap fetch)
 *
 * Ethos §8: "Workload scenarios verify that tomefs works end-to-end under
 * realistic use"
 * Ethos §9: "Target the seams: large sequential scans that rotate the entire
 * cache"
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
// Scenario 1: CREATE INDEX on a populated table
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 1: CREATE INDEX on populated table",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE idx_pop (id SERIAL PRIMARY KEY, val INT, label TEXT)`);

    for (let i = 0; i < 100; i++) {
      await pg.query(`INSERT INTO idx_pop (val, label) VALUES ($1, $2)`, [
        i * 7 % 200,
        `label-${i}-${"x".repeat(50)}`,
      ]);
    }

    await pg.query(`CREATE INDEX idx_pop_val ON idx_pop (val)`);

    const result = await pg.query(
      `SELECT val FROM idx_pop WHERE val BETWEEN 50 AND 100 ORDER BY val`,
    );
    expect(result.rows.length).toBeGreaterThan(0);
    for (let i = 1; i < result.rows.length; i++) {
      expect(result.rows[i].val).toBeGreaterThanOrEqual(result.rows[i - 1].val);
    }

    const count = await pg.query(`SELECT COUNT(*)::int as c FROM idx_pop`);
    expect(count.rows[0].c).toBe(100);
  },
);

// ---------------------------------------------------------------------------
// Scenario 2: CREATE UNIQUE INDEX with constraint enforcement
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 2: UNIQUE INDEX with constraint enforcement",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE idx_uniq (id SERIAL, email TEXT)`);

    for (let i = 0; i < 50; i++) {
      await pg.query(`INSERT INTO idx_uniq (email) VALUES ($1)`, [
        `user${i}@example.com`,
      ]);
    }

    await pg.query(`CREATE UNIQUE INDEX idx_uniq_email ON idx_uniq (email)`);

    // Verify constraint enforcement
    let caught = false;
    try {
      await pg.query(`INSERT INTO idx_uniq (email) VALUES ('user0@example.com')`);
    } catch (e: any) {
      caught = true;
      expect(e.message).toContain("unique");
    }
    expect(caught).toBe(true);

    // Verify lookups still work
    const result = await pg.query(
      `SELECT email FROM idx_uniq WHERE email = 'user25@example.com'`,
    );
    expect(result.rows.length).toBe(1);
    expect(result.rows[0].email).toBe("user25@example.com");
  },
);

// ---------------------------------------------------------------------------
// Scenario 3: Multi-column composite index
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 3: Multi-column composite index",
  async (h) => {
    const { pg } = h;

    await pg.query(
      `CREATE TABLE idx_comp (id SERIAL, category INT, priority INT, name TEXT)`,
    );

    for (let i = 0; i < 80; i++) {
      await pg.query(
        `INSERT INTO idx_comp (category, priority, name) VALUES ($1, $2, $3)`,
        [i % 5, i % 10, `item-${i}`],
      );
    }

    await pg.query(
      `CREATE INDEX idx_comp_cat_pri ON idx_comp (category, priority)`,
    );

    // Query using both columns of the composite index
    const result = await pg.query(
      `SELECT name FROM idx_comp WHERE category = 2 AND priority >= 5 ORDER BY priority`,
    );
    expect(result.rows.length).toBeGreaterThan(0);
    for (const row of result.rows) {
      expect(row.name).toMatch(/^item-/);
    }

    // Query using only the leading column
    const leading = await pg.query(
      `SELECT COUNT(*)::int as c FROM idx_comp WHERE category = 3`,
    );
    expect(leading.rows[0].c).toBe(16);
  },
);

// ---------------------------------------------------------------------------
// Scenario 4: Multiple independent indexes on same table
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 4: Multiple indexes on same table",
  async (h) => {
    const { pg } = h;

    await pg.query(
      `CREATE TABLE idx_multi (id SERIAL, a INT, b TEXT, c TIMESTAMP)`,
    );

    for (let i = 0; i < 60; i++) {
      await pg.query(
        `INSERT INTO idx_multi (a, b, c) VALUES ($1, $2, NOW() + ($3 || ' hours')::interval)`,
        [i * 3, `val-${i % 20}`, i],
      );
    }

    // Create three independent indexes — each btree competes for cache slots
    await pg.query(`CREATE INDEX idx_multi_a ON idx_multi (a)`);
    await pg.query(`CREATE INDEX idx_multi_b ON idx_multi (b)`);
    await pg.query(`CREATE INDEX idx_multi_c ON idx_multi (c)`);

    // Use each index
    const ra = await pg.query(`SELECT COUNT(*)::int as c FROM idx_multi WHERE a < 50`);
    expect(ra.rows[0].c).toBeGreaterThan(0);

    const rb = await pg.query(`SELECT COUNT(*)::int as c FROM idx_multi WHERE b = 'val-5'`);
    expect(rb.rows[0].c).toBe(3);

    const rc = await pg.query(
      `SELECT COUNT(*)::int as c FROM idx_multi WHERE c > NOW() + '10 hours'::interval`,
    );
    expect(rc.rows[0].c).toBeGreaterThan(0);
  },
);

// ---------------------------------------------------------------------------
// Scenario 5: Partial index (CREATE INDEX ... WHERE)
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 5: Partial index with WHERE clause",
  async (h) => {
    const { pg } = h;

    await pg.query(
      `CREATE TABLE idx_partial (id SERIAL, status TEXT, amount INT)`,
    );

    for (let i = 0; i < 100; i++) {
      const status = i % 3 === 0 ? "active" : i % 3 === 1 ? "pending" : "closed";
      await pg.query(
        `INSERT INTO idx_partial (status, amount) VALUES ($1, $2)`,
        [status, i * 10],
      );
    }

    // Partial index — only indexes active rows
    await pg.query(
      `CREATE INDEX idx_partial_active ON idx_partial (amount) WHERE status = 'active'`,
    );

    // Query that can use the partial index
    const active = await pg.query(
      `SELECT COUNT(*)::int as c FROM idx_partial WHERE status = 'active' AND amount > 500`,
    );
    expect(active.rows[0].c).toBeGreaterThan(0);

    // Full count for verification
    const total = await pg.query(
      `SELECT COUNT(*)::int as c FROM idx_partial WHERE status = 'active'`,
    );
    expect(total.rows[0].c).toBe(34);
  },
);

// ---------------------------------------------------------------------------
// Scenario 6: Expression index
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 6: Expression index (lower(col))",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE idx_expr (id SERIAL, name TEXT, city TEXT)`);

    const cities = ["New York", "Los Angeles", "CHICAGO", "San Francisco", "SEATTLE"];
    for (let i = 0; i < 60; i++) {
      await pg.query(
        `INSERT INTO idx_expr (name, city) VALUES ($1, $2)`,
        [`person-${i}`, cities[i % cities.length]],
      );
    }

    await pg.query(`CREATE INDEX idx_expr_city ON idx_expr (lower(city))`);

    // Case-insensitive lookup using the expression index
    const result = await pg.query(
      `SELECT COUNT(*)::int as c FROM idx_expr WHERE lower(city) = 'chicago'`,
    );
    expect(result.rows[0].c).toBe(12);

    const seattle = await pg.query(
      `SELECT name FROM idx_expr WHERE lower(city) = 'seattle' ORDER BY name LIMIT 3`,
    );
    expect(seattle.rows.length).toBe(3);
  },
);

// ---------------------------------------------------------------------------
// Scenario 7: Index-guided ORDER BY (avoids sort node)
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 7: Index-guided ORDER BY",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE idx_order (id SERIAL, score INT, tag TEXT)`);

    for (let i = 0; i < 80; i++) {
      await pg.query(
        `INSERT INTO idx_order (score, tag) VALUES ($1, $2)`,
        [(i * 13 + 7) % 500, `tag-${i % 10}`],
      );
    }

    await pg.query(`CREATE INDEX idx_order_score ON idx_order (score)`);

    // ORDER BY on the indexed column — Postgres can follow the btree in order
    const result = await pg.query(
      `SELECT score FROM idx_order ORDER BY score LIMIT 20`,
    );
    expect(result.rows.length).toBe(20);
    for (let i = 1; i < result.rows.length; i++) {
      expect(result.rows[i].score).toBeGreaterThanOrEqual(result.rows[i - 1].score);
    }

    // DESC order — follow btree backwards
    const desc = await pg.query(
      `SELECT score FROM idx_order ORDER BY score DESC LIMIT 10`,
    );
    expect(desc.rows.length).toBe(10);
    for (let i = 1; i < desc.rows.length; i++) {
      expect(desc.rows[i].score).toBeLessThanOrEqual(desc.rows[i - 1].score);
    }
  },
);

// ---------------------------------------------------------------------------
// Scenario 8: Index maintenance under UPDATE
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 8: Index maintenance under UPDATE",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE idx_upd (id SERIAL PRIMARY KEY, val INT)`);
    await pg.query(`CREATE INDEX idx_upd_val ON idx_upd (val)`);

    for (let i = 0; i < 60; i++) {
      await pg.query(`INSERT INTO idx_upd (val) VALUES ($1)`, [i]);
    }

    // Update indexed column — causes btree insert (new) + delete (old)
    for (let i = 0; i < 30; i++) {
      await pg.query(`UPDATE idx_upd SET val = val + 1000 WHERE id = $1`, [i + 1]);
    }

    // Verify updated values are findable via index
    const high = await pg.query(
      `SELECT COUNT(*)::int as c FROM idx_upd WHERE val >= 1000`,
    );
    expect(high.rows[0].c).toBe(30);

    // Verify non-updated values are still correct
    const low = await pg.query(
      `SELECT COUNT(*)::int as c FROM idx_upd WHERE val < 1000`,
    );
    expect(low.rows[0].c).toBe(30);

    // Total must be unchanged
    const total = await pg.query(`SELECT COUNT(*)::int as c FROM idx_upd`);
    expect(total.rows[0].c).toBe(60);
  },
);

// ---------------------------------------------------------------------------
// Scenario 9: DROP INDEX + recreate
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 9: DROP INDEX and recreate",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE idx_drop (id SERIAL, val INT, data TEXT)`);

    for (let i = 0; i < 50; i++) {
      await pg.query(
        `INSERT INTO idx_drop (val, data) VALUES ($1, $2)`,
        [i, `data-${i}-${"y".repeat(40)}`],
      );
    }

    // Create, use, drop, recreate
    await pg.query(`CREATE INDEX idx_drop_val ON idx_drop (val)`);

    const before = await pg.query(
      `SELECT COUNT(*)::int as c FROM idx_drop WHERE val BETWEEN 10 AND 30`,
    );
    expect(before.rows[0].c).toBe(21);

    await pg.query(`DROP INDEX idx_drop_val`);

    // Insert more data while index doesn't exist
    for (let i = 50; i < 80; i++) {
      await pg.query(
        `INSERT INTO idx_drop (val, data) VALUES ($1, $2)`,
        [i, `data-${i}-${"z".repeat(40)}`],
      );
    }

    // Recreate the index — must scan the full heap including new rows
    await pg.query(`CREATE INDEX idx_drop_val ON idx_drop (val)`);

    const after = await pg.query(
      `SELECT COUNT(*)::int as c FROM idx_drop WHERE val BETWEEN 10 AND 60`,
    );
    expect(after.rows[0].c).toBe(51);

    const total = await pg.query(`SELECT COUNT(*)::int as c FROM idx_drop`);
    expect(total.rows[0].c).toBe(80);
  },
);

// ---------------------------------------------------------------------------
// Scenario 10: Index + persistence round-trip
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 10: Index + persistence round-trip",
  async (h) => {
    const { pg } = h;
    const backend = h.backend as SyncMemoryBackend;

    await pg.query(`CREATE TABLE idx_persist (id SERIAL PRIMARY KEY, val INT, name TEXT)`);
    await pg.query(`CREATE INDEX idx_persist_val ON idx_persist (val)`);

    for (let i = 0; i < 50; i++) {
      await pg.query(
        `INSERT INTO idx_persist (val, name) VALUES ($1, $2)`,
        [i * 3, `name-${i}`],
      );
    }

    // Sync and remount
    await h.syncToFs();
    await h.destroy();

    const maxPages = (h.adapter.tomefsInstance?.pageCache?.capacity) ?? 4096;
    harness = await createPGliteHarness({ cacheSize: maxPages, backend });

    // Verify index works after remount
    const result = await harness.pg.query(
      `SELECT name FROM idx_persist WHERE val = 30`,
    );
    expect(result.rows.length).toBe(1);
    expect(result.rows[0].name).toBe("name-10");

    // Range query via index
    const range = await harness.pg.query(
      `SELECT COUNT(*)::int as c FROM idx_persist WHERE val BETWEEN 50 AND 100`,
    );
    expect(range.rows[0].c).toBeGreaterThan(0);

    const total = await harness.pg.query(
      `SELECT COUNT(*)::int as c FROM idx_persist`,
    );
    expect(total.rows[0].c).toBe(50);
  },
);

// ---------------------------------------------------------------------------
// Scenario 11: REINDEX
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 11: REINDEX rebuilds index from scratch",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE idx_reindex (id SERIAL PRIMARY KEY, val INT)`);
    await pg.query(`CREATE INDEX idx_reindex_val ON idx_reindex (val)`);

    for (let i = 0; i < 80; i++) {
      await pg.query(`INSERT INTO idx_reindex (val) VALUES ($1)`, [i]);
    }

    // Update many values to create dead index entries
    for (let i = 0; i < 40; i++) {
      await pg.query(`UPDATE idx_reindex SET val = val + 500 WHERE id = $1`, [i + 1]);
    }

    // REINDEX rebuilds the index from scratch — full heap scan + btree build
    await pg.query(`REINDEX INDEX idx_reindex_val`);

    // Verify correctness after reindex
    const high = await pg.query(
      `SELECT COUNT(*)::int as c FROM idx_reindex WHERE val >= 500`,
    );
    expect(high.rows[0].c).toBe(40);

    const low = await pg.query(
      `SELECT COUNT(*)::int as c FROM idx_reindex WHERE val < 500`,
    );
    expect(low.rows[0].c).toBe(40);

    // Verify ordered scan
    const ordered = await pg.query(
      `SELECT val FROM idx_reindex ORDER BY val`,
    );
    expect(ordered.rows.length).toBe(80);
    for (let i = 1; i < ordered.rows.length; i++) {
      expect(ordered.rows[i].val).toBeGreaterThanOrEqual(ordered.rows[i - 1].val);
    }
  },
);

// ---------------------------------------------------------------------------
// Scenario 12: Range scan with large result set
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 12: Range scan via index with large result",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE idx_range (id SERIAL, score INT, payload TEXT)`);

    for (let i = 0; i < 100; i++) {
      await pg.query(
        `INSERT INTO idx_range (score, payload) VALUES ($1, $2)`,
        [i, `payload-${i}-${"p".repeat(30)}`],
      );
    }

    await pg.query(`CREATE INDEX idx_range_score ON idx_range (score)`);

    // Large range scan — reads many btree leaf pages + heap pages
    const result = await pg.query(
      `SELECT score, payload FROM idx_range WHERE score BETWEEN 20 AND 80 ORDER BY score`,
    );
    expect(result.rows.length).toBe(61);
    for (let i = 0; i < result.rows.length; i++) {
      expect(result.rows[i].score).toBe(20 + i);
      expect(result.rows[i].payload).toContain(`payload-${20 + i}-`);
    }

    // Aggregate via index
    const agg = await pg.query(
      `SELECT MIN(score) as mn, MAX(score) as mx, AVG(score)::int as av
       FROM idx_range WHERE score BETWEEN 20 AND 80`,
    );
    expect(agg.rows[0].mn).toBe(20);
    expect(agg.rows[0].mx).toBe(80);
    expect(agg.rows[0].av).toBe(50);
  },
);
