/**
 * PGlite + tomefs schema migration tests.
 *
 * Schema migrations (ALTER TABLE, CREATE TABLE AS SELECT, CLUSTER)
 * exercise filesystem patterns distinct from normal DML:
 *   - ALTER TABLE ALTER TYPE rewrites every heap page in the table
 *   - CREATE TABLE AS SELECT does a bulk sequential read + sequential write
 *   - CLUSTER rewrites the table ordered by an index
 *   - Multiple ALTER TABLEs in a transaction touch catalog pages repeatedly
 *
 * Under a small page cache, these operations force massive eviction of
 * both catalog and heap pages mid-rewrite. This is exactly the scenario
 * where cache coherence bugs surface — dirty catalog pages evicted while
 * a DDL operation is in flight, or heap pages from the old table file
 * mixed with pages from the new rewritten file.
 *
 * Ethos 8: "simulate real PGlite access patterns"
 * Ethos 9: "target the seams"
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
// Scenario 1: ALTER TABLE ADD COLUMN with DEFAULT
// ---------------------------------------------------------------------------

describe("ALTER TABLE ADD COLUMN with DEFAULT", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(
        `CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT)`,
      );
      for (let i = 0; i < 50; i++) {
        await pg.query(`INSERT INTO users (name) VALUES ($1)`, [
          `user-${i}-${"x".repeat(100)}`,
        ]);
      }

      // ADD COLUMN with DEFAULT — PG stores the default in the catalog
      // but existing rows get the value lazily on read. This exercises
      // catalog page writes under cache pressure.
      await pg.query(
        `ALTER TABLE users ADD COLUMN active BOOLEAN DEFAULT true`,
      );
      await pg.query(
        `ALTER TABLE users ADD COLUMN score INTEGER DEFAULT 0`,
      );
      await pg.query(
        `ALTER TABLE users ADD COLUMN bio TEXT DEFAULT 'no bio'`,
      );

      // Verify existing rows see the defaults
      const result = await pg.query(
        `SELECT id, name, active, score, bio FROM users WHERE id = 1`,
      );
      expect(result.rows[0].active).toBe(true);
      expect(result.rows[0].score).toBe(0);
      expect(result.rows[0].bio).toBe("no bio");

      // Verify all rows
      const allActive = await pg.query(
        `SELECT COUNT(*)::int as count FROM users WHERE active = true`,
      );
      expect(allActive.rows[0].count).toBe(50);

      // Insert a new row with non-default values
      await pg.query(
        `INSERT INTO users (name, active, score, bio) VALUES ('new', false, 42, 'hello')`,
      );
      const newRow = await pg.query(
        `SELECT active, score, bio FROM users WHERE name = 'new'`,
      );
      expect(newRow.rows[0]).toEqual({
        active: false,
        score: 42,
        bio: "hello",
      });
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 2: ALTER TABLE ALTER COLUMN TYPE (full table rewrite)
// ---------------------------------------------------------------------------

describe("ALTER TABLE ALTER COLUMN TYPE (table rewrite)", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages)`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(
        `CREATE TABLE measurements (id SERIAL PRIMARY KEY, value INTEGER, label TEXT)`,
      );
      for (let i = 0; i < 60; i++) {
        await pg.query(
          `INSERT INTO measurements (value, label) VALUES ($1, $2)`,
          [i * 100, `m-${i}-${"d".repeat(80)}`],
        );
      }

      // ALTER COLUMN TYPE forces a full table rewrite — every heap page
      // is read, transformed, and written to a new file. Under a tiny
      // cache this forces the entire old and new files through eviction.
      await pg.query(
        `ALTER TABLE measurements ALTER COLUMN value TYPE BIGINT`,
      );

      // Verify data integrity after rewrite
      const count = await pg.query(
        `SELECT COUNT(*)::int as count FROM measurements`,
      );
      expect(count.rows[0].count).toBe(60);

      // Verify values survived the type change
      const sample = await pg.query(
        `SELECT value, label FROM measurements WHERE id = 30`,
      );
      expect(Number(sample.rows[0].value)).toBe(2900);
      expect(sample.rows[0].label).toMatch(/^m-29-/);

      // Verify we can still INSERT with the new type
      await pg.query(
        `INSERT INTO measurements (value, label) VALUES ($1, 'big')`,
        [9999999],
      );
      const big = await pg.query(
        `SELECT value FROM measurements WHERE label = 'big'`,
      );
      expect(Number(big.rows[0].value)).toBe(9999999);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 3: CREATE TABLE AS SELECT (bulk copy)
// ---------------------------------------------------------------------------

describe("CREATE TABLE AS SELECT (bulk copy)", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages)`, async () => {
      const h = await create(size);
      const { pg } = h;

      // Create and populate source table
      await pg.query(
        `CREATE TABLE source (id SERIAL PRIMARY KEY, category INTEGER, payload TEXT)`,
      );
      for (let i = 0; i < 80; i++) {
        await pg.query(
          `INSERT INTO source (category, payload) VALUES ($1, $2)`,
          [i % 5, `payload-${i}-${"s".repeat(120)}`],
        );
      }

      // CTAS: sequential scan of source + sequential write of new table.
      // Under cache pressure, source pages are evicted to make room for
      // destination pages, then re-read if the scan isn't complete.
      await pg.query(
        `CREATE TABLE filtered AS SELECT * FROM source WHERE category IN (0, 2, 4)`,
      );

      // Verify the new table has correct data
      const count = await pg.query(
        `SELECT COUNT(*)::int as count FROM filtered`,
      );
      // categories 0, 2, 4: 16 rows each = 48
      expect(count.rows[0].count).toBe(48);

      // Verify data integrity
      const categories = await pg.query(
        `SELECT DISTINCT category FROM filtered ORDER BY category`,
      );
      expect(categories.rows.map((r: any) => r.category)).toEqual([0, 2, 4]);

      // Source table should be unchanged
      const srcCount = await pg.query(
        `SELECT COUNT(*)::int as count FROM source`,
      );
      expect(srcCount.rows[0].count).toBe(80);

      // CTAS with aggregation — exercises hash/sort + write pattern
      await pg.query(`
        CREATE TABLE summary AS
        SELECT category, COUNT(*)::int as cnt, MIN(id) as first_id
        FROM source GROUP BY category
      `);
      const summary = await pg.query(
        `SELECT category, cnt FROM summary ORDER BY category`,
      );
      expect(summary.rows.length).toBe(5);
      for (const row of summary.rows) {
        expect(row.cnt).toBe(16);
      }
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 4: CLUSTER (table reorder on index)
// ---------------------------------------------------------------------------

describe("CLUSTER (table reorder by index)", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages)`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(
        `CREATE TABLE events (id SERIAL PRIMARY KEY, ts INTEGER, data TEXT)`,
      );
      await pg.query(`CREATE INDEX idx_events_ts ON events (ts)`);

      // Insert rows with non-sequential timestamps so CLUSTER actually
      // reorders them. This makes the rewrite touch every page.
      for (let i = 0; i < 60; i++) {
        const ts = (i * 37) % 60; // Scrambled order
        await pg.query(
          `INSERT INTO events (ts, data) VALUES ($1, $2)`,
          [ts, `event-${i}-${"e".repeat(100)}`],
        );
      }

      // CLUSTER rewrites the entire table ordered by the index.
      // Like VACUUM FULL, this creates a new heap file, copies all
      // rows in index order, then swaps files.
      await pg.query(`CLUSTER events USING idx_events_ts`);

      // Verify all data survived
      const count = await pg.query(
        `SELECT COUNT(*)::int as count FROM events`,
      );
      expect(count.rows[0].count).toBe(60);

      // Verify ordering — a sequential scan should now return rows
      // roughly in ts order (CLUSTER doesn't guarantee perfect order
      // for future inserts, but the existing rows should be sorted)
      const rows = await pg.query(
        `SELECT ts FROM events ORDER BY ctid LIMIT 10`,
      );
      for (let i = 1; i < rows.rows.length; i++) {
        expect(rows.rows[i].ts).toBeGreaterThanOrEqual(rows.rows[i - 1].ts);
      }

      // Verify we can still insert and query after CLUSTER
      await pg.query(
        `INSERT INTO events (ts, data) VALUES (999, 'post-cluster')`,
      );
      const post = await pg.query(
        `SELECT data FROM events WHERE ts = 999`,
      );
      expect(post.rows[0].data).toBe("post-cluster");
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 5: Multiple ALTER TABLEs in a transaction
// ---------------------------------------------------------------------------

describe("Multiple ALTER TABLEs in a transaction", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages)`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`
        CREATE TABLE products (
          id SERIAL PRIMARY KEY,
          name TEXT NOT NULL,
          price INTEGER NOT NULL,
          stock INTEGER DEFAULT 0
        )
      `);
      for (let i = 0; i < 40; i++) {
        await pg.query(
          `INSERT INTO products (name, price, stock) VALUES ($1, $2, $3)`,
          [`product-${i}`, (i + 1) * 100, i * 10],
        );
      }

      // Multiple schema changes in a single transaction — exercises
      // repeated catalog page writes under cache pressure.
      await pg.query("BEGIN");
      await pg.query(
        `ALTER TABLE products ADD COLUMN category TEXT DEFAULT 'general'`,
      );
      await pg.query(
        `ALTER TABLE products ADD COLUMN weight REAL DEFAULT 0.0`,
      );
      await pg.query(
        `ALTER TABLE products ADD COLUMN active BOOLEAN DEFAULT true`,
      );
      await pg.query(
        `ALTER TABLE products DROP COLUMN stock`,
      );
      await pg.query(
        `ALTER TABLE products RENAME COLUMN price TO cost`,
      );
      await pg.query("COMMIT");

      // Verify schema changes applied correctly
      const result = await pg.query(
        `SELECT id, name, cost, category, weight, active FROM products WHERE id = 1`,
      );
      expect(result.rows[0]).toEqual({
        id: 1,
        name: "product-0",
        cost: 100,
        category: "general",
        weight: 0,
        active: true,
      });

      // Verify dropped column is gone
      await expect(
        pg.query(`SELECT stock FROM products LIMIT 1`),
      ).rejects.toThrow();

      // Verify all rows accessible
      const count = await pg.query(
        `SELECT COUNT(*)::int as count FROM products`,
      );
      expect(count.rows[0].count).toBe(40);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 6: ALTER TABLE rollback
// ---------------------------------------------------------------------------

describe("ALTER TABLE rollback under cache pressure", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages)`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(
        `CREATE TABLE config (id SERIAL PRIMARY KEY, key TEXT UNIQUE, val TEXT)`,
      );
      for (let i = 0; i < 30; i++) {
        await pg.query(
          `INSERT INTO config (key, val) VALUES ($1, $2)`,
          [`key-${i}`, `val-${i}-${"c".repeat(80)}`],
        );
      }

      // Rolled-back ALTER TABLE — catalog modifications must be undone.
      // Under cache pressure, catalog dirty pages may be evicted before
      // the rollback completes.
      await pg.query("BEGIN");
      await pg.query(`ALTER TABLE config ADD COLUMN extra TEXT DEFAULT 'gone'`);
      await pg.query(`INSERT INTO config (key, val, extra) VALUES ('new', 'v', 'e')`);
      await pg.query("ROLLBACK");

      // Verify schema is unchanged — column should not exist
      await expect(
        pg.query(`SELECT extra FROM config LIMIT 1`),
      ).rejects.toThrow();

      // Verify data is unchanged
      const count = await pg.query(
        `SELECT COUNT(*)::int as count FROM config`,
      );
      expect(count.rows[0].count).toBe(30);

      // Verify we can still use the table normally
      await pg.query(
        `INSERT INTO config (key, val) VALUES ('after-rollback', 'ok')`,
      );
      const check = await pg.query(
        `SELECT val FROM config WHERE key = 'after-rollback'`,
      );
      expect(check.rows[0].val).toBe("ok");
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 7: Schema migration + persistence round-trip
// ---------------------------------------------------------------------------

describe("Schema migration + persistence round-trip", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages)`, async () => {
      const backend = new SyncMemoryBackend();

      // Phase 1: create table, populate, alter, sync
      const h1 = await create(size, backend);
      await h1.pg.query(
        `CREATE TABLE app_data (id SERIAL PRIMARY KEY, name TEXT, value INTEGER)`,
      );
      for (let i = 0; i < 40; i++) {
        await h1.pg.query(
          `INSERT INTO app_data (name, value) VALUES ($1, $2)`,
          [`item-${i}`, i * 10],
        );
      }

      // Schema migration: add columns, create index
      await h1.pg.query(
        `ALTER TABLE app_data ADD COLUMN tags TEXT[] DEFAULT '{}'`,
      );
      await h1.pg.query(
        `ALTER TABLE app_data ADD COLUMN updated_at TIMESTAMPTZ DEFAULT NOW()`,
      );
      await h1.pg.query(
        `CREATE INDEX idx_app_data_value ON app_data (value)`,
      );

      // Update some rows to use new columns
      await h1.pg.query(
        `UPDATE app_data SET tags = ARRAY['important'] WHERE value >= 300`,
      );

      await h1.syncToFs();
      await h1.destroy();

      // Phase 2: remount and verify schema + data survived
      const h2 = await create(size, backend);

      // Verify row count
      const count = await h2.pg.query(
        `SELECT COUNT(*)::int as count FROM app_data`,
      );
      expect(count.rows[0].count).toBe(40);

      // Verify new columns work
      const tagged = await h2.pg.query(
        `SELECT COUNT(*)::int as count FROM app_data WHERE 'important' = ANY(tags)`,
      );
      expect(tagged.rows[0].count).toBe(10); // values 300-390

      // Verify index survives remount
      const indexed = await h2.pg.query(
        `SELECT name FROM app_data WHERE value = 200`,
      );
      expect(indexed.rows[0].name).toBe("item-20");

      // Apply another migration on the remounted instance
      await h2.pg.query(
        `ALTER TABLE app_data ADD COLUMN version INTEGER DEFAULT 1`,
      );
      await h2.pg.query(
        `UPDATE app_data SET version = 2 WHERE value >= 300`,
      );

      await h2.syncToFs();
      await h2.destroy();

      // Phase 3: verify second migration persisted
      const h3 = await create(size, backend);
      const v2Count = await h3.pg.query(
        `SELECT COUNT(*)::int as count FROM app_data WHERE version = 2`,
      );
      expect(v2Count.rows[0].count).toBe(10);

      const v1Count = await h3.pg.query(
        `SELECT COUNT(*)::int as count FROM app_data WHERE version = 1`,
      );
      expect(v1Count.rows[0].count).toBe(30);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 8: CREATE TABLE AS SELECT + persistence
// ---------------------------------------------------------------------------

describe("CREATE TABLE AS SELECT + persistence", () => {
  for (const size of ["tiny", "small", "large"] as CacheSize[]) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const backend = new SyncMemoryBackend();

      // Phase 1: create source, CTAS, sync
      const h1 = await create(size, backend);
      await h1.pg.query(
        `CREATE TABLE orders (id SERIAL PRIMARY KEY, customer_id INTEGER, total INTEGER, status TEXT)`,
      );
      for (let i = 0; i < 50; i++) {
        await h1.pg.query(
          `INSERT INTO orders (customer_id, total, status) VALUES ($1, $2, $3)`,
          [i % 10, (i + 1) * 50, i % 3 === 0 ? "completed" : "pending"],
        );
      }

      // CTAS to create a summary table
      await h1.pg.query(`
        CREATE TABLE customer_totals AS
        SELECT customer_id, SUM(total)::int as total_spent, COUNT(*)::int as order_count
        FROM orders
        GROUP BY customer_id
      `);

      await h1.syncToFs();
      await h1.destroy();

      // Phase 2: verify both tables survived
      const h2 = await create(size, backend);

      const orderCount = await h2.pg.query(
        `SELECT COUNT(*)::int as count FROM orders`,
      );
      expect(orderCount.rows[0].count).toBe(50);

      const summaryCount = await h2.pg.query(
        `SELECT COUNT(*)::int as count FROM customer_totals`,
      );
      expect(summaryCount.rows[0].count).toBe(10);

      // Verify summary data is correct
      const topCustomer = await h2.pg.query(
        `SELECT customer_id, order_count FROM customer_totals ORDER BY total_spent DESC LIMIT 1`,
      );
      expect(topCustomer.rows[0].order_count).toBe(5);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 9: DROP TABLE + recreate under pressure
// ---------------------------------------------------------------------------

describe("DROP TABLE + recreate under cache pressure", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages)`, async () => {
      const h = await create(size);
      const { pg } = h;

      // Create and populate multiple tables
      for (let t = 0; t < 5; t++) {
        await pg.query(
          `CREATE TABLE batch_${t} (id SERIAL PRIMARY KEY, data TEXT)`,
        );
        for (let i = 0; i < 20; i++) {
          await pg.query(
            `INSERT INTO batch_${t} (data) VALUES ($1)`,
            [`t${t}-row${i}-${"r".repeat(80)}`],
          );
        }
      }

      // Drop and recreate tables — this exercises file deletion + creation
      // under cache pressure. Pages from deleted files may still be in the
      // cache when the new table's pages arrive.
      for (let t = 0; t < 3; t++) {
        await pg.query(`DROP TABLE batch_${t}`);
        await pg.query(
          `CREATE TABLE batch_${t} (id SERIAL PRIMARY KEY, data TEXT, version INTEGER)`,
        );
        for (let i = 0; i < 10; i++) {
          await pg.query(
            `INSERT INTO batch_${t} (data, version) VALUES ($1, 2)`,
            [`v2-t${t}-row${i}`],
          );
        }
      }

      // Verify recreated tables have new schema and data
      for (let t = 0; t < 3; t++) {
        const count = await pg.query(
          `SELECT COUNT(*)::int as count FROM batch_${t}`,
        );
        expect(count.rows[0].count).toBe(10);

        const version = await pg.query(
          `SELECT DISTINCT version FROM batch_${t}`,
        );
        expect(version.rows[0].version).toBe(2);
      }

      // Verify untouched tables are still intact
      for (let t = 3; t < 5; t++) {
        const count = await pg.query(
          `SELECT COUNT(*)::int as count FROM batch_${t}`,
        );
        expect(count.rows[0].count).toBe(20);
      }
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 10: CLUSTER + persistence round-trip
// ---------------------------------------------------------------------------

describe("CLUSTER + persistence round-trip", () => {
  for (const size of ["tiny", "small", "large"] as CacheSize[]) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages)`, async () => {
      const backend = new SyncMemoryBackend();

      const h1 = await create(size, backend);
      await h1.pg.query(
        `CREATE TABLE timeseries (id SERIAL PRIMARY KEY, ts INTEGER, sensor TEXT, reading REAL)`,
      );
      await h1.pg.query(
        `CREATE INDEX idx_ts_ts ON timeseries (ts)`,
      );

      // Insert scrambled timestamps
      for (let i = 0; i < 50; i++) {
        const ts = (i * 31) % 50;
        await h1.pg.query(
          `INSERT INTO timeseries (ts, sensor, reading) VALUES ($1, $2, $3)`,
          [ts, `sensor-${i % 5}`, Math.round(i * 1.5 * 100) / 100],
        );
      }

      // CLUSTER to reorder on disk, then persist
      await h1.pg.query(`CLUSTER timeseries USING idx_ts_ts`);
      await h1.syncToFs();
      await h1.destroy();

      // Verify data + ordering survived remount
      const h2 = await create(size, backend);

      const count = await h2.pg.query(
        `SELECT COUNT(*)::int as count FROM timeseries`,
      );
      expect(count.rows[0].count).toBe(50);

      // Verify physical ordering survived (ctid-order should match ts-order)
      const rows = await h2.pg.query(
        `SELECT ts FROM timeseries ORDER BY ctid LIMIT 10`,
      );
      for (let i = 1; i < rows.rows.length; i++) {
        expect(rows.rows[i].ts).toBeGreaterThanOrEqual(rows.rows[i - 1].ts);
      }

      // Index still works after remount
      const range = await h2.pg.query(
        `SELECT COUNT(*)::int as count FROM timeseries WHERE ts BETWEEN 10 AND 20`,
      );
      expect(range.rows[0].count).toBeGreaterThan(0);
    });
  }
});
