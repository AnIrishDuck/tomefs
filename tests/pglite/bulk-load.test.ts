/**
 * PGlite + tomefs bulk data loading tests (ethos §8 — workload scenarios).
 *
 * Exercises Postgres's bulk insert code paths (heap_multi_insert via
 * generate_series, INSERT ... SELECT, and large row payloads) under
 * cache pressure. These patterns create sequential write storms that
 * rotate the entire page cache — fundamentally different from the
 * scattered point writes in other PGlite tests.
 *
 * Key scenarios:
 * - Bulk INSERT via generate_series (single-statement mass write)
 * - Wide rows spanning multiple Postgres pages
 * - Index creation after bulk load (btree build scan)
 * - Sequential scan after bulk load (full cache rotation)
 * - Bulk DELETE + VACUUM (page reclamation under cache pressure)
 * - INSERT ... SELECT cross-table bulk copy
 * - Persistence round-trip after bulk load
 * - Dirty shutdown during bulk load + WAL recovery
 */

import { describe, it, expect, afterEach } from "vitest";
import {
  createPGliteHarness,
  CACHE_CONFIGS,
  type CacheSize,
  type PGliteHarness,
} from "./harness.js";

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
) {
  describe(name, () => {
    for (const size of PRESSURE_CONFIGS) {
      const pages = CACHE_CONFIGS[size];
      it(`cache=${size} (${pages} pages)`, async () => {
        harness = await createPGliteHarness(size);
        await scenarioFn(harness, size);
      });
    }
  });
}

// ---------------------------------------------------------------------------
// Scenario 1: Bulk INSERT via generate_series
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 1: bulk INSERT via generate_series",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE bulk_gen (
      id INTEGER PRIMARY KEY,
      val TEXT NOT NULL
    )`);

    await pg.query(`
      INSERT INTO bulk_gen (id, val)
      SELECT i, 'row-' || i || '-' || repeat('x', 80)
      FROM generate_series(1, 500) AS s(i)
    `);

    const count = await pg.query(
      `SELECT COUNT(*)::int AS n FROM bulk_gen`,
    );
    expect(count.rows[0].n).toBe(500);

    const spot = await pg.query(
      `SELECT val FROM bulk_gen WHERE id = 250`,
    );
    expect(spot.rows[0].val).toMatch(/^row-250-x+$/);

    const minMax = await pg.query(
      `SELECT MIN(id)::int AS lo, MAX(id)::int AS hi FROM bulk_gen`,
    );
    expect(minMax.rows[0].lo).toBe(1);
    expect(minMax.rows[0].hi).toBe(500);
  },
);

// ---------------------------------------------------------------------------
// Scenario 2: Wide rows spanning multiple pages
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 2: wide rows with large payloads @fast",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE wide_rows (
      id SERIAL PRIMARY KEY,
      payload TEXT NOT NULL
    )`);

    for (let i = 0; i < 20; i++) {
      const size = 2000 + i * 500;
      await pg.query(
        `INSERT INTO wide_rows (payload) VALUES ($1)`,
        [String.fromCharCode(65 + (i % 26)).repeat(size)],
      );
    }

    const count = await pg.query(
      `SELECT COUNT(*)::int AS n FROM wide_rows`,
    );
    expect(count.rows[0].n).toBe(20);

    const lengths = await pg.query(`
      SELECT id, LENGTH(payload)::int AS len FROM wide_rows ORDER BY id
    `);
    for (let i = 0; i < 20; i++) {
      expect(lengths.rows[i].len).toBe(2000 + i * 500);
    }
  },
);

// ---------------------------------------------------------------------------
// Scenario 3: Index creation after bulk load
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 3: CREATE INDEX after bulk load",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE idx_bulk (
      id INTEGER,
      category INTEGER,
      payload TEXT
    )`);

    await pg.query(`
      INSERT INTO idx_bulk (id, category, payload)
      SELECT i, i % 10, 'data-' || i || '-' || repeat('y', 60)
      FROM generate_series(1, 300) AS s(i)
    `);

    await pg.query(`CREATE INDEX idx_bulk_cat ON idx_bulk (category)`);
    await pg.query(`CREATE INDEX idx_bulk_id ON idx_bulk (id)`);

    const catCount = await pg.query(`
      SELECT COUNT(*)::int AS n FROM idx_bulk WHERE category = 5
    `);
    expect(catCount.rows[0].n).toBe(30);

    const rangeCount = await pg.query(`
      SELECT COUNT(*)::int AS n FROM idx_bulk WHERE id BETWEEN 100 AND 200
    `);
    expect(rangeCount.rows[0].n).toBe(101);
  },
);

// ---------------------------------------------------------------------------
// Scenario 4: Sequential scan after bulk load rotates cache
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 4: full sequential scan after bulk load",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE seq_scan (
      id INTEGER PRIMARY KEY,
      checksum INTEGER NOT NULL
    )`);

    await pg.query(`
      INSERT INTO seq_scan (id, checksum)
      SELECT i, (i * 31 + 17) % 997
      FROM generate_series(1, 400) AS s(i)
    `);

    const agg = await pg.query(`
      SELECT SUM(checksum)::int AS total FROM seq_scan
    `);

    let expected = 0;
    for (let i = 1; i <= 400; i++) {
      expected += (i * 31 + 17) % 997;
    }
    expect(agg.rows[0].total).toBe(expected);

    const ordered = await pg.query(`
      SELECT id, checksum FROM seq_scan ORDER BY id
    `);
    expect(ordered.rows.length).toBe(400);
    for (let i = 0; i < 400; i++) {
      const row = ordered.rows[i];
      expect(row.id).toBe(i + 1);
      expect(row.checksum).toBe(((i + 1) * 31 + 17) % 997);
    }
  },
);

// ---------------------------------------------------------------------------
// Scenario 5: Bulk DELETE + VACUUM
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 5: bulk DELETE then VACUUM reclaims pages",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE del_bulk (
      id INTEGER PRIMARY KEY,
      filler TEXT
    )`);

    await pg.query(`
      INSERT INTO del_bulk (id, filler)
      SELECT i, repeat('z', 100)
      FROM generate_series(1, 300) AS s(i)
    `);

    const before = await pg.query(
      `SELECT COUNT(*)::int AS n FROM del_bulk`,
    );
    expect(before.rows[0].n).toBe(300);

    await pg.query(`DELETE FROM del_bulk WHERE id <= 200`);

    const after = await pg.query(
      `SELECT COUNT(*)::int AS n FROM del_bulk`,
    );
    expect(after.rows[0].n).toBe(100);

    await pg.query(`VACUUM del_bulk`);

    const postVac = await pg.query(
      `SELECT COUNT(*)::int AS n FROM del_bulk`,
    );
    expect(postVac.rows[0].n).toBe(100);

    const remaining = await pg.query(`
      SELECT MIN(id)::int AS lo, MAX(id)::int AS hi FROM del_bulk
    `);
    expect(remaining.rows[0].lo).toBe(201);
    expect(remaining.rows[0].hi).toBe(300);
  },
);

// ---------------------------------------------------------------------------
// Scenario 6: INSERT ... SELECT cross-table bulk copy
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 6: INSERT ... SELECT cross-table copy @fast",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE src_table (
      id INTEGER PRIMARY KEY,
      data TEXT
    )`);
    await pg.query(`CREATE TABLE dst_table (
      id INTEGER PRIMARY KEY,
      data TEXT,
      copied_at TIMESTAMPTZ DEFAULT NOW()
    )`);

    await pg.query(`
      INSERT INTO src_table (id, data)
      SELECT i, 'source-' || i || '-' || repeat('s', 50)
      FROM generate_series(1, 200) AS s(i)
    `);

    await pg.query(`
      INSERT INTO dst_table (id, data)
      SELECT id, data FROM src_table
    `);

    const srcCount = await pg.query(
      `SELECT COUNT(*)::int AS n FROM src_table`,
    );
    const dstCount = await pg.query(
      `SELECT COUNT(*)::int AS n FROM dst_table`,
    );
    expect(srcCount.rows[0].n).toBe(200);
    expect(dstCount.rows[0].n).toBe(200);

    const diff = await pg.query(`
      SELECT COUNT(*)::int AS n FROM src_table s
      FULL OUTER JOIN dst_table d ON s.id = d.id AND s.data = d.data
      WHERE s.id IS NULL OR d.id IS NULL
    `);
    expect(diff.rows[0].n).toBe(0);
  },
);

// ---------------------------------------------------------------------------
// Scenario 7: Persistence round-trip after bulk load
// ---------------------------------------------------------------------------

describe("Scenario 7: bulk load persists across remount", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];
    it(`cache=${size} (${pages} pages) @fast`, async () => {
      harness = await createPGliteHarness(size);
      const { pg, backend } = harness;

      await pg.query(`CREATE TABLE persist_bulk (
        id INTEGER PRIMARY KEY,
        val TEXT NOT NULL
      )`);

      await pg.query(`
        INSERT INTO persist_bulk (id, val)
        SELECT i, 'persisted-' || i || '-' || repeat('p', 40)
        FROM generate_series(1, 200) AS s(i)
      `);

      await harness.syncToFs();
      await harness.destroy();
      harness = null;

      harness = await createPGliteHarness({ cacheSize: size, backend });
      const pg2 = harness.pg;

      const count = await pg2.query(
        `SELECT COUNT(*)::int AS n FROM persist_bulk`,
      );
      expect(count.rows[0].n).toBe(200);

      const spot = await pg2.query(
        `SELECT val FROM persist_bulk WHERE id = 100`,
      );
      expect(spot.rows[0].val).toMatch(/^persisted-100-p+$/);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 8: Dirty shutdown during bulk load + WAL recovery
// ---------------------------------------------------------------------------

describe("Scenario 8: dirty shutdown after bulk load + WAL recovery", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];
    it(`cache=${size} (${pages} pages)`, async () => {
      harness = await createPGliteHarness(size);
      const { pg, backend } = harness;

      await pg.query(`CREATE TABLE crash_bulk (
        id INTEGER PRIMARY KEY,
        val TEXT NOT NULL
      )`);

      await pg.query(`
        INSERT INTO crash_bulk (id, val)
        SELECT i, 'before-crash-' || i
        FROM generate_series(1, 100) AS s(i)
      `);

      await harness.syncToFs();

      await pg.query(`
        INSERT INTO crash_bulk (id, val)
        SELECT i, 'after-sync-' || i
        FROM generate_series(101, 200) AS s(i)
      `);

      harness.dirtyDestroy();
      harness = null;

      harness = await createPGliteHarness({ cacheSize: size, backend });
      const pg2 = harness.pg;

      const synced = await pg2.query(
        `SELECT COUNT(*)::int AS n FROM crash_bulk WHERE id <= 100`,
      );
      expect(synced.rows[0].n).toBe(100);

      const total = await pg2.query(
        `SELECT COUNT(*)::int AS n FROM crash_bulk`,
      );
      expect(total.rows[0].n).toBeGreaterThanOrEqual(100);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 9: Repeated bulk load cycles (append-heavy ETL pattern)
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 9: repeated bulk load cycles",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE etl_target (
      batch INTEGER NOT NULL,
      id INTEGER NOT NULL,
      data TEXT,
      PRIMARY KEY (batch, id)
    )`);

    for (let batch = 1; batch <= 5; batch++) {
      await pg.query(`
        INSERT INTO etl_target (batch, id, data)
        SELECT ${batch}, i, 'batch-${batch}-row-' || i || '-' || repeat('e', 30)
        FROM generate_series(1, 100) AS s(i)
      `);
    }

    const total = await pg.query(
      `SELECT COUNT(*)::int AS n FROM etl_target`,
    );
    expect(total.rows[0].n).toBe(500);

    for (let batch = 1; batch <= 5; batch++) {
      const batchCount = await pg.query(
        `SELECT COUNT(*)::int AS n FROM etl_target WHERE batch = $1`,
        [batch],
      );
      expect(batchCount.rows[0].n).toBe(100);
    }
  },
);

// ---------------------------------------------------------------------------
// Scenario 10: Bulk UPDATE rewrites heap pages
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 10: bulk UPDATE rewrites heap pages",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE upd_bulk (
      id INTEGER PRIMARY KEY,
      version INTEGER NOT NULL DEFAULT 1,
      payload TEXT NOT NULL
    )`);

    await pg.query(`
      INSERT INTO upd_bulk (id, payload)
      SELECT i, 'original-' || i || '-' || repeat('o', 40)
      FROM generate_series(1, 200) AS s(i)
    `);

    await pg.query(`
      UPDATE upd_bulk
      SET version = 2, payload = 'updated-' || id || '-' || repeat('u', 60)
      WHERE id % 2 = 0
    `);

    const v1 = await pg.query(`
      SELECT COUNT(*)::int AS n FROM upd_bulk WHERE version = 1
    `);
    expect(v1.rows[0].n).toBe(100);

    const v2 = await pg.query(`
      SELECT COUNT(*)::int AS n FROM upd_bulk WHERE version = 2
    `);
    expect(v2.rows[0].n).toBe(100);

    const spot = await pg.query(
      `SELECT payload FROM upd_bulk WHERE id = 100`,
    );
    expect(spot.rows[0].payload).toMatch(/^updated-100-u+$/);
  },
);

// ---------------------------------------------------------------------------
// Scenario 11: Bulk load with UNIQUE constraint violation recovery
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 11: bulk load handles constraint violations @fast",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE uniq_bulk (
      id INTEGER PRIMARY KEY,
      email TEXT UNIQUE NOT NULL
    )`);

    await pg.query(`
      INSERT INTO uniq_bulk (id, email)
      SELECT i, 'user-' || i || '@example.com'
      FROM generate_series(1, 100) AS s(i)
    `);

    let caught = false;
    try {
      await pg.query(`
        INSERT INTO uniq_bulk (id, email)
        SELECT i + 100, 'user-' || i || '@example.com'
        FROM generate_series(1, 50) AS s(i)
      `);
    } catch (e: any) {
      caught = true;
      expect(e.message).toContain("unique");
    }
    expect(caught).toBe(true);

    const count = await pg.query(
      `SELECT COUNT(*)::int AS n FROM uniq_bulk`,
    );
    expect(count.rows[0].n).toBe(100);
  },
);

// ---------------------------------------------------------------------------
// Scenario 12: Bulk load + aggregation pipeline
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 12: aggregation over bulk-loaded data",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE agg_bulk (
      id INTEGER PRIMARY KEY,
      category INTEGER NOT NULL,
      amount NUMERIC(10,2) NOT NULL
    )`);

    await pg.query(`
      INSERT INTO agg_bulk (id, category, amount)
      SELECT i, (i % 5) + 1, (i * 7.3 + 0.5)::numeric(10,2)
      FROM generate_series(1, 500) AS s(i)
    `);

    const grouped = await pg.query(`
      SELECT category, COUNT(*)::int AS cnt, SUM(amount)::float AS total
      FROM agg_bulk
      GROUP BY category
      ORDER BY category
    `);
    expect(grouped.rows.length).toBe(5);
    for (const row of grouped.rows) {
      expect(row.cnt).toBe(100);
      expect(row.total).toBeGreaterThan(0);
    }

    const windowResult = await pg.query(`
      SELECT COUNT(DISTINCT rn)::int AS n FROM (
        SELECT ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) AS rn
        FROM agg_bulk
      ) sub WHERE rn <= 10
    `);
    expect(windowResult.rows[0].n).toBe(10);
  },
);
