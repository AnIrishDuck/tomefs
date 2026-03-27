/**
 * PGlite + tomefs VACUUM and table-rewrite stress tests.
 *
 * VACUUM is Postgres's heaviest filesystem operation: it sequentially
 * scans the entire table, rewrites live tuples in place, and truncates
 * trailing empty pages. Under a small page cache, this forces massive
 * eviction of dirty pages mid-operation — exactly the scenario most
 * likely to expose cache coherence bugs.
 *
 * These tests target the seams between:
 *   - Sequential scan (loading every page) + LRU eviction
 *   - In-place page rewrites (dirty pages being evicted mid-VACUUM)
 *   - File truncation after VACUUM (shrinking the heap file)
 *   - WAL writes concurrent with heap rewrites
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
// Scenario 1: DELETE + VACUUM + verify data integrity
// ---------------------------------------------------------------------------

describe("VACUUM after bulk delete", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages)`, async () => {
      const h = await create(size);
      const { pg } = h;

      // Create table and insert rows
      await pg.query(
        `CREATE TABLE vacuum_test (id SERIAL PRIMARY KEY, data TEXT, keep BOOLEAN)`,
      );

      // Insert 100 rows, half marked for deletion
      for (let i = 0; i < 100; i++) {
        await pg.query(
          `INSERT INTO vacuum_test (data, keep) VALUES ($1, $2)`,
          [`row-${i}-${"x".repeat(200)}`, i % 2 === 0],
        );
      }

      // Delete half the rows
      await pg.query(`DELETE FROM vacuum_test WHERE keep = false`);

      // VACUUM to reclaim space — this is the critical operation
      await pg.query(`VACUUM vacuum_test`);

      // Verify surviving rows are intact
      const result = await pg.query(
        `SELECT COUNT(*)::int as count FROM vacuum_test`,
      );
      expect(result.rows[0].count).toBe(50);

      // Verify data integrity of surviving rows
      const rows = await pg.query(
        `SELECT id, data FROM vacuum_test ORDER BY id LIMIT 5`,
      );
      for (const row of rows.rows) {
        expect(row.data).toMatch(/^row-\d+-x+$/);
        // Only even IDs should survive (keep = true for i % 2 === 0)
        expect(row.id % 2).toBe(1); // SERIAL starts at 1, so id=1 is i=0 (even)
      }
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 2: VACUUM FULL (table rewrite) under cache pressure
// ---------------------------------------------------------------------------

describe("VACUUM FULL table rewrite", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages)`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(
        `CREATE TABLE rewrite_test (id SERIAL PRIMARY KEY, value INTEGER, payload TEXT)`,
      );

      // Insert rows with large payloads to create multi-page table
      for (let i = 0; i < 60; i++) {
        await pg.query(
          `INSERT INTO rewrite_test (value, payload) VALUES ($1, $2)`,
          [i * 10, `payload-${i}-${"z".repeat(150)}`],
        );
      }

      // Delete 2/3 of rows to create fragmentation
      await pg.query(`DELETE FROM rewrite_test WHERE id % 3 != 0`);

      // VACUUM FULL rewrites the entire table file — creates new file,
      // copies live rows, swaps files, deletes old file
      await pg.query(`VACUUM FULL rewrite_test`);

      // Verify surviving rows
      const result = await pg.query(
        `SELECT COUNT(*)::int as count FROM rewrite_test`,
      );
      expect(result.rows[0].count).toBe(20);

      // Verify values are correct (only rows with id % 3 === 0 survived)
      const rows = await pg.query(
        `SELECT id, value FROM rewrite_test ORDER BY id`,
      );
      for (const row of rows.rows) {
        // id % 3 === 0 means original loop index was id-1, value = (id-1)*10
        expect(row.id % 3).toBe(0);
        expect(row.value).toBe((row.id - 1) * 10);
      }

      // Verify we can still insert after VACUUM FULL
      await pg.query(
        `INSERT INTO rewrite_test (value, payload) VALUES (999, 'after-vacuum')`,
      );
      const check = await pg.query(
        `SELECT value FROM rewrite_test WHERE value = 999`,
      );
      expect(check.rows.length).toBe(1);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 3: Repeated DELETE + VACUUM cycles (exercises truncation repeatedly)
// ---------------------------------------------------------------------------

describe("Repeated delete-vacuum cycles", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages)`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(
        `CREATE TABLE cycle_test (id SERIAL PRIMARY KEY, batch INTEGER, data TEXT)`,
      );

      for (let cycle = 0; cycle < 5; cycle++) {
        // Insert a batch of rows
        for (let i = 0; i < 30; i++) {
          await pg.query(
            `INSERT INTO cycle_test (batch, data) VALUES ($1, $2)`,
            [cycle, `cycle${cycle}-row${i}-${"d".repeat(100)}`],
          );
        }

        // Delete previous batch (keep only current)
        if (cycle > 0) {
          await pg.query(
            `DELETE FROM cycle_test WHERE batch < $1`,
            [cycle],
          );
          await pg.query(`VACUUM cycle_test`);
        }
      }

      // Only the last batch (cycle=4) should survive
      const result = await pg.query(
        `SELECT COUNT(*)::int as count FROM cycle_test WHERE batch = 4`,
      );
      expect(result.rows[0].count).toBe(30);

      const total = await pg.query(
        `SELECT COUNT(*)::int as count FROM cycle_test`,
      );
      expect(total.rows[0].count).toBe(30);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 4: CREATE INDEX on existing data under pressure
// (reads all table pages sequentially, writes index pages)
// ---------------------------------------------------------------------------

describe("CREATE INDEX on existing data", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(
        `CREATE TABLE index_test (id SERIAL PRIMARY KEY, category INTEGER, label TEXT)`,
      );

      // Insert enough data that index creation causes significant I/O
      for (let i = 0; i < 80; i++) {
        await pg.query(
          `INSERT INTO index_test (category, label) VALUES ($1, $2)`,
          [i % 10, `label-${i}-${"y".repeat(100)}`],
        );
      }

      // Create indexes after data exists — forces full table scan + index build
      await pg.query(`CREATE INDEX idx_cat ON index_test (category)`);
      await pg.query(`CREATE INDEX idx_label ON index_test (label)`);

      // Verify indexes work correctly
      const catResult = await pg.query(
        `SELECT COUNT(*)::int as count FROM index_test WHERE category = 5`,
      );
      expect(catResult.rows[0].count).toBe(8);

      const labelResult = await pg.query(
        `SELECT id FROM index_test WHERE label LIKE 'label-42-%'`,
      );
      expect(labelResult.rows.length).toBe(1);

      // REINDEX to exercise index rebuild
      await pg.query(`REINDEX TABLE index_test`);

      // Verify still correct after reindex
      const afterReindex = await pg.query(
        `SELECT COUNT(*)::int as count FROM index_test WHERE category = 5`,
      );
      expect(afterReindex.rows[0].count).toBe(8);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 5: VACUUM + persistence round-trip
// (the critical real-world scenario: VACUUM, sync, remount)
// ---------------------------------------------------------------------------

describe("VACUUM + persistence round-trip", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages)`, async () => {
      const backend = new SyncMemoryBackend();

      // Phase 1: create, delete, vacuum
      const h1 = await create(size, backend);
      await h1.pg.query(
        `CREATE TABLE persist_vacuum (id SERIAL PRIMARY KEY, value INTEGER, data TEXT)`,
      );
      for (let i = 0; i < 50; i++) {
        await h1.pg.query(
          `INSERT INTO persist_vacuum (value, data) VALUES ($1, $2)`,
          [i, `data-${i}-${"p".repeat(100)}`],
        );
      }
      await h1.pg.query(`DELETE FROM persist_vacuum WHERE value % 2 = 0`);
      await h1.pg.query(`VACUUM persist_vacuum`);

      // Insert more data after vacuum
      await h1.pg.query(
        `INSERT INTO persist_vacuum (value, data) VALUES (1000, 'post-vacuum')`,
      );

      await h1.syncToFs();
      await h1.destroy();

      // Phase 2: remount and verify
      const h2 = await create(size, backend);

      const count = await h2.pg.query(
        `SELECT COUNT(*)::int as count FROM persist_vacuum`,
      );
      // 25 odd rows survived delete + 1 post-vacuum insert = 26
      expect(count.rows[0].count).toBe(26);

      // Verify post-vacuum insert survived
      const post = await h2.pg.query(
        `SELECT value FROM persist_vacuum WHERE value = 1000`,
      );
      expect(post.rows.length).toBe(1);

      // Verify sequence continues correctly
      await h2.pg.query(
        `INSERT INTO persist_vacuum (value, data) VALUES (2000, 'after-remount')`,
      );
      const latest = await h2.pg.query(
        `SELECT id FROM persist_vacuum ORDER BY id DESC LIMIT 1`,
      );
      // id should be > 50 (all original IDs were 1-50)
      expect(latest.rows[0].id).toBeGreaterThan(50);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 6: ANALYZE under pressure (reads sample of pages)
// ---------------------------------------------------------------------------

describe("ANALYZE under cache pressure", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(
        `CREATE TABLE analyze_test (id SERIAL PRIMARY KEY, category INTEGER, data TEXT)`,
      );
      await pg.query(`CREATE INDEX idx_analyze_cat ON analyze_test (category)`);

      for (let i = 0; i < 100; i++) {
        await pg.query(
          `INSERT INTO analyze_test (category, data) VALUES ($1, $2)`,
          [i % 5, `data-${i}`],
        );
      }

      // ANALYZE updates planner statistics by sampling pages
      await pg.query(`ANALYZE analyze_test`);

      // Verify the table is still fully functional
      const result = await pg.query(
        `SELECT category, COUNT(*)::int as count FROM analyze_test GROUP BY category ORDER BY category`,
      );
      expect(result.rows.length).toBe(5);
      for (const row of result.rows) {
        expect(row.count).toBe(20);
      }
    });
  }
});
