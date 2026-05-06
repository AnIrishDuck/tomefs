/**
 * PGlite + tomefs schema evolution tests.
 *
 * Tests ALTER TABLE operations under cache pressure with persistence
 * round-trips. Schema evolution is a common real-world PGlite pattern:
 * application upgrades add columns, change types, add/drop indexes, and
 * add constraints — all of which modify pg_attribute, pg_type, and heap
 * files while the database is in active use.
 *
 * These operations exercise:
 *   - Page rewrites when adding columns with defaults (Postgres 11+ fast default)
 *   - Catalog page churn (pg_attribute, pg_class, pg_index updates)
 *   - File creation/deletion for indexes
 *   - Full table rewrites for ALTER COLUMN TYPE
 *   - WAL traffic from DDL operations
 *
 * Under small cache, every DDL operation causes page eviction from both
 * the catalog and the heap, exercising dirty page flush ordering between
 * system catalog pages and user data pages.
 *
 * Ethos §8: "record or simulate real PGlite access patterns"
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
// Scenario 1: ALTER TABLE ADD COLUMN with default values
// ---------------------------------------------------------------------------

describe("Schema evolution: ADD COLUMN with default", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(
        `CREATE TABLE evolve_add (id SERIAL PRIMARY KEY, name TEXT)`,
      );

      for (let i = 0; i < 40; i++) {
        await pg.query(`INSERT INTO evolve_add (name) VALUES ($1)`, [
          `row-${i}`,
        ]);
      }

      // ADD COLUMN with default — Postgres 11+ uses fast default (no rewrite)
      await pg.query(
        `ALTER TABLE evolve_add ADD COLUMN status TEXT DEFAULT 'active'`,
      );
      await pg.query(
        `ALTER TABLE evolve_add ADD COLUMN priority INTEGER DEFAULT 0`,
      );

      // Verify existing rows have defaults
      const result = await pg.query(
        `SELECT id, name, status, priority FROM evolve_add ORDER BY id LIMIT 5`,
      );
      for (const row of result.rows) {
        expect(row.status).toBe("active");
        expect(row.priority).toBe(0);
      }

      // Insert new rows with new columns
      await pg.query(
        `INSERT INTO evolve_add (name, status, priority) VALUES ('new-row', 'pending', 5)`,
      );

      // Update some existing rows' new columns
      await pg.query(
        `UPDATE evolve_add SET status = 'archived', priority = 10 WHERE id <= 10`,
      );

      // Verify mixed state
      const archived = await pg.query(
        `SELECT COUNT(*)::int as count FROM evolve_add WHERE status = 'archived'`,
      );
      expect(archived.rows[0].count).toBe(10);

      const active = await pg.query(
        `SELECT COUNT(*)::int as count FROM evolve_add WHERE status = 'active'`,
      );
      expect(active.rows[0].count).toBe(30);

      const pending = await pg.query(
        `SELECT COUNT(*)::int as count FROM evolve_add WHERE status = 'pending'`,
      );
      expect(pending.rows[0].count).toBe(1);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 2: ALTER TABLE DROP COLUMN
// ---------------------------------------------------------------------------

describe("Schema evolution: DROP COLUMN", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(
        `CREATE TABLE evolve_drop (
          id SERIAL PRIMARY KEY,
          name TEXT,
          obsolete_field TEXT,
          value INTEGER
        )`,
      );

      for (let i = 0; i < 30; i++) {
        await pg.query(
          `INSERT INTO evolve_drop (name, obsolete_field, value) VALUES ($1, $2, $3)`,
          [`row-${i}`, `old-data-${i}`, i * 10],
        );
      }

      // DROP COLUMN — marks column as dropped in pg_attribute
      await pg.query(`ALTER TABLE evolve_drop DROP COLUMN obsolete_field`);

      // Verify remaining columns are intact
      const result = await pg.query(
        `SELECT id, name, value FROM evolve_drop ORDER BY id`,
      );
      expect(result.rows.length).toBe(30);
      for (let i = 0; i < 30; i++) {
        expect(result.rows[i].name).toBe(`row-${i}`);
        expect(result.rows[i].value).toBe(i * 10);
      }

      // Verify dropped column is inaccessible
      await expect(
        pg.query(`SELECT obsolete_field FROM evolve_drop LIMIT 1`),
      ).rejects.toThrow();

      // Insert new rows (should work without the dropped column)
      await pg.query(
        `INSERT INTO evolve_drop (name, value) VALUES ('after-drop', 999)`,
      );
      const check = await pg.query(
        `SELECT value FROM evolve_drop WHERE name = 'after-drop'`,
      );
      expect(check.rows[0].value).toBe(999);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 3: CREATE INDEX + DROP INDEX cycle
// ---------------------------------------------------------------------------

describe("Schema evolution: index create/drop cycle", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages)`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(
        `CREATE TABLE evolve_idx (
          id SERIAL PRIMARY KEY,
          category INTEGER,
          tag TEXT,
          value REAL
        )`,
      );

      for (let i = 0; i < 60; i++) {
        await pg.query(
          `INSERT INTO evolve_idx (category, tag, value) VALUES ($1, $2, $3)`,
          [i % 8, `tag-${i % 15}`, Math.random() * 100],
        );
      }

      // Create indexes
      await pg.query(`CREATE INDEX idx_evolve_cat ON evolve_idx (category)`);
      await pg.query(`CREATE INDEX idx_evolve_tag ON evolve_idx (tag)`);

      // Query using indexes
      const catResult = await pg.query(
        `SELECT COUNT(*)::int as count FROM evolve_idx WHERE category = 3`,
      );
      expect(catResult.rows[0].count).toBeGreaterThan(0);

      // Drop one index, create a different one
      await pg.query(`DROP INDEX idx_evolve_tag`);
      await pg.query(
        `CREATE INDEX idx_evolve_composite ON evolve_idx (category, value)`,
      );

      // Verify composite index works
      const compositeResult = await pg.query(
        `SELECT COUNT(*)::int as count FROM evolve_idx WHERE category = 2 AND value > 50`,
      );
      expect(compositeResult.rows[0].count).toBeGreaterThanOrEqual(0);

      // Drop and recreate the same index name
      await pg.query(`DROP INDEX idx_evolve_cat`);
      await pg.query(`CREATE INDEX idx_evolve_cat ON evolve_idx (category DESC)`);

      // Verify data integrity after all index churn
      const total = await pg.query(
        `SELECT COUNT(*)::int as count FROM evolve_idx`,
      );
      expect(total.rows[0].count).toBe(60);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 4: ALTER COLUMN TYPE (full table rewrite)
// ---------------------------------------------------------------------------

describe("Schema evolution: ALTER COLUMN TYPE", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages)`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(
        `CREATE TABLE evolve_type (
          id SERIAL PRIMARY KEY,
          code TEXT,
          amount INTEGER
        )`,
      );

      for (let i = 0; i < 40; i++) {
        await pg.query(
          `INSERT INTO evolve_type (code, amount) VALUES ($1, $2)`,
          [`CODE-${i}`, i * 100],
        );
      }

      // ALTER COLUMN TYPE — triggers full table rewrite
      await pg.query(
        `ALTER TABLE evolve_type ALTER COLUMN amount TYPE BIGINT`,
      );

      // Verify data survived the rewrite
      const result = await pg.query(
        `SELECT id, code, amount FROM evolve_type ORDER BY id`,
      );
      expect(result.rows.length).toBe(40);
      for (let i = 0; i < 40; i++) {
        expect(result.rows[i].code).toBe(`CODE-${i}`);
        expect(Number(result.rows[i].amount)).toBe(i * 100);
      }

      // Insert values that need BIGINT range
      await pg.query(
        `INSERT INTO evolve_type (code, amount) VALUES ('BIG', 9999999999)`,
      );
      const big = await pg.query(
        `SELECT amount FROM evolve_type WHERE code = 'BIG'`,
      );
      expect(Number(big.rows[0].amount)).toBe(9999999999);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 5: ADD CONSTRAINT (NOT NULL, CHECK)
// ---------------------------------------------------------------------------

describe("Schema evolution: ADD CONSTRAINT", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(
        `CREATE TABLE evolve_constraint (
          id SERIAL PRIMARY KEY,
          name TEXT,
          score INTEGER
        )`,
      );

      for (let i = 0; i < 30; i++) {
        await pg.query(
          `INSERT INTO evolve_constraint (name, score) VALUES ($1, $2)`,
          [`item-${i}`, 50 + i],
        );
      }

      // Add CHECK constraint
      await pg.query(
        `ALTER TABLE evolve_constraint ADD CONSTRAINT score_positive CHECK (score >= 0)`,
      );

      // Verify constraint works
      await expect(
        pg.query(
          `INSERT INTO evolve_constraint (name, score) VALUES ('bad', -1)`,
        ),
      ).rejects.toThrow();

      // Valid insert still works
      await pg.query(
        `INSERT INTO evolve_constraint (name, score) VALUES ('good', 100)`,
      );

      // Add NOT NULL constraint (requires backfill first)
      await pg.query(
        `ALTER TABLE evolve_constraint ADD COLUMN status TEXT DEFAULT 'active' NOT NULL`,
      );

      const result = await pg.query(
        `SELECT COUNT(*)::int as count FROM evolve_constraint WHERE status = 'active'`,
      );
      expect(result.rows[0].count).toBe(31);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 6: Multi-step schema evolution with persistence
// ---------------------------------------------------------------------------

describe("Schema evolution: multi-step evolution persists across remount", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages)`, async () => {
      const backend = new SyncMemoryBackend();

      // Phase 1: create table, populate, evolve schema
      const h1 = await create(size, backend);
      await h1.pg.query(
        `CREATE TABLE evolving (
          id SERIAL PRIMARY KEY,
          name TEXT,
          value INTEGER
        )`,
      );

      for (let i = 0; i < 30; i++) {
        await h1.pg.query(
          `INSERT INTO evolving (name, value) VALUES ($1, $2)`,
          [`item-${i}`, i * 10],
        );
      }

      // First evolution: add columns
      await h1.pg.query(
        `ALTER TABLE evolving ADD COLUMN created_at TIMESTAMP DEFAULT NOW()`,
      );
      await h1.pg.query(
        `ALTER TABLE evolving ADD COLUMN tags TEXT[] DEFAULT '{}'`,
      );

      // Insert with new columns
      await h1.pg.query(
        `INSERT INTO evolving (name, value, tags) VALUES ('evolved-1', 999, ARRAY['tag1', 'tag2'])`,
      );

      // Create an index on the new column
      await h1.pg.query(`CREATE INDEX idx_evolving_value ON evolving (value)`);

      await h1.syncToFs();
      await h1.destroy();

      // Phase 2: remount, verify, evolve further
      const h2 = await create(size, backend);

      // Verify phase 1 data
      const count1 = await h2.pg.query(
        `SELECT COUNT(*)::int as count FROM evolving`,
      );
      expect(count1.rows[0].count).toBe(31);

      const evolved = await h2.pg.query(
        `SELECT tags FROM evolving WHERE name = 'evolved-1'`,
      );
      expect(evolved.rows[0].tags).toEqual(["tag1", "tag2"]);

      // Verify index survived
      const indexed = await h2.pg.query(
        `SELECT name FROM evolving WHERE value = 150`,
      );
      expect(indexed.rows[0].name).toBe("item-15");

      // Phase 2 evolution: drop column, add new one
      await h2.pg.query(`ALTER TABLE evolving DROP COLUMN tags`);
      await h2.pg.query(
        `ALTER TABLE evolving ADD COLUMN version INTEGER DEFAULT 1`,
      );

      await h2.pg.query(
        `UPDATE evolving SET version = 2 WHERE id <= 10`,
      );

      await h2.syncToFs();
      await h2.destroy();

      // Phase 3: final remount, verify full evolution history
      const h3 = await create(size, backend);

      const total = await h3.pg.query(
        `SELECT COUNT(*)::int as count FROM evolving`,
      );
      expect(total.rows[0].count).toBe(31);

      // Verify version column from phase 2
      const v2 = await h3.pg.query(
        `SELECT COUNT(*)::int as count FROM evolving WHERE version = 2`,
      );
      expect(v2.rows[0].count).toBe(10);

      const v1 = await h3.pg.query(
        `SELECT COUNT(*)::int as count FROM evolving WHERE version = 1`,
      );
      expect(v1.rows[0].count).toBe(21);

      // Verify tags column is gone
      await expect(
        h3.pg.query(`SELECT tags FROM evolving LIMIT 1`),
      ).rejects.toThrow();

      // Verify original data is intact
      const original = await h3.pg.query(
        `SELECT name, value FROM evolving WHERE id = 1`,
      );
      expect(original.rows[0].name).toBe("item-0");
      expect(original.rows[0].value).toBe(0);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 7: DROP TABLE + CREATE TABLE at same name
// ---------------------------------------------------------------------------

describe("Schema evolution: drop and recreate table at same name", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages)`, async () => {
      const h = await create(size);
      const { pg } = h;

      // Create v1
      await pg.query(
        `CREATE TABLE versioned (id SERIAL PRIMARY KEY, data TEXT)`,
      );
      for (let i = 0; i < 20; i++) {
        await pg.query(`INSERT INTO versioned (data) VALUES ($1)`, [
          `v1-${i}`,
        ]);
      }

      // Drop and recreate with different schema
      await pg.query(`DROP TABLE versioned`);
      await pg.query(
        `CREATE TABLE versioned (
          id SERIAL PRIMARY KEY,
          data TEXT,
          category INTEGER,
          active BOOLEAN DEFAULT true
        )`,
      );

      // Verify clean slate
      const empty = await pg.query(
        `SELECT COUNT(*)::int as count FROM versioned`,
      );
      expect(empty.rows[0].count).toBe(0);

      // Insert with new schema
      for (let i = 0; i < 15; i++) {
        await pg.query(
          `INSERT INTO versioned (data, category, active) VALUES ($1, $2, $3)`,
          [`v2-${i}`, i % 3, i % 4 !== 0],
        );
      }

      const result = await pg.query(
        `SELECT COUNT(*)::int as count FROM versioned WHERE active = true`,
      );
      expect(result.rows[0].count).toBe(11);

      // Verify no ghost data from v1
      const v1check = await pg.query(
        `SELECT COUNT(*)::int as count FROM versioned WHERE data LIKE 'v1-%'`,
      );
      expect(v1check.rows[0].count).toBe(0);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 8: DROP TABLE + recreate with persistence round-trip
// ---------------------------------------------------------------------------

describe("Schema evolution: drop/recreate persists correctly", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages)`, async () => {
      const backend = new SyncMemoryBackend();

      // Phase 1: create, populate, drop, recreate
      const h1 = await create(size, backend);
      await h1.pg.query(
        `CREATE TABLE recycled (id SERIAL PRIMARY KEY, val TEXT)`,
      );
      for (let i = 0; i < 20; i++) {
        await h1.pg.query(`INSERT INTO recycled (val) VALUES ($1)`, [
          `old-${i}`,
        ]);
      }

      await h1.pg.query(`DROP TABLE recycled`);
      await h1.pg.query(
        `CREATE TABLE recycled (id SERIAL PRIMARY KEY, val TEXT, gen INTEGER DEFAULT 2)`,
      );
      for (let i = 0; i < 10; i++) {
        await h1.pg.query(
          `INSERT INTO recycled (val) VALUES ($1)`,
          [`new-${i}`],
        );
      }

      await h1.syncToFs();
      await h1.destroy();

      // Phase 2: remount, verify only new data exists
      const h2 = await create(size, backend);

      const total = await h2.pg.query(
        `SELECT COUNT(*)::int as count FROM recycled`,
      );
      expect(total.rows[0].count).toBe(10);

      const gen = await h2.pg.query(
        `SELECT DISTINCT gen FROM recycled`,
      );
      expect(gen.rows[0].gen).toBe(2);

      const noOld = await h2.pg.query(
        `SELECT COUNT(*)::int as count FROM recycled WHERE val LIKE 'old-%'`,
      );
      expect(noOld.rows[0].count).toBe(0);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 9: Rapid schema churn (multiple ALTER TABLE in sequence)
// ---------------------------------------------------------------------------

describe("Schema evolution: rapid schema churn", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages)`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(
        `CREATE TABLE churn (id SERIAL PRIMARY KEY, base_col TEXT)`,
      );

      for (let i = 0; i < 20; i++) {
        await pg.query(`INSERT INTO churn (base_col) VALUES ($1)`, [
          `base-${i}`,
        ]);
      }

      // Rapid schema changes
      for (let round = 0; round < 5; round++) {
        const colName = `col_${round}`;
        await pg.query(
          `ALTER TABLE churn ADD COLUMN ${colName} INTEGER DEFAULT ${round}`,
        );
        await pg.query(
          `UPDATE churn SET ${colName} = ${round * 10} WHERE id <= 5`,
        );
      }

      // Verify all columns exist with correct values
      const result = await pg.query(
        `SELECT * FROM churn WHERE id = 1`,
      );
      expect(result.rows[0].base_col).toBe("base-0");
      for (let round = 0; round < 5; round++) {
        expect(result.rows[0][`col_${round}`]).toBe(round * 10);
      }

      // Verify non-updated rows have defaults
      const defaults = await pg.query(
        `SELECT * FROM churn WHERE id = 10`,
      );
      for (let round = 0; round < 5; round++) {
        expect(defaults.rows[0][`col_${round}`]).toBe(round);
      }

      // Drop half the added columns
      await pg.query(`ALTER TABLE churn DROP COLUMN col_1`);
      await pg.query(`ALTER TABLE churn DROP COLUMN col_3`);

      // Verify remaining columns
      const afterDrop = await pg.query(
        `SELECT id, base_col, col_0, col_2, col_4 FROM churn WHERE id = 1`,
      );
      expect(afterDrop.rows[0].col_0).toBe(0);
      expect(afterDrop.rows[0].col_2).toBe(20);
      expect(afterDrop.rows[0].col_4).toBe(40);

      // Total row count unchanged
      const total = await pg.query(
        `SELECT COUNT(*)::int as count FROM churn`,
      );
      expect(total.rows[0].count).toBe(20);
    });
  }
});
