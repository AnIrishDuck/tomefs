/**
 * PGlite + tomefs cache pressure tests.
 *
 * Runs SQL workloads at various cache sizes to verify that tomefs's
 * page cache eviction doesn't corrupt Postgres data. These tests
 * exercise the core value proposition: bounded memory with correct
 * behavior under eviction pressure.
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

/** Cache sizes to test. Tiny/small force eviction; large is baseline. */
const PRESSURE_CONFIGS: CacheSize[] = ["tiny", "small", "medium", "large"];

/**
 * Run a scenario against PGlite+tomefs at every cache pressure level.
 */
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
// Scenario 1: Bulk Insert + Full Scan
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 1: Bulk insert then full table scan",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE scan_test (id SERIAL PRIMARY KEY, payload TEXT)`);

    // Insert 50 rows with varying payload sizes
    for (let i = 0; i < 50; i++) {
      const payload = `row-${i}-${"x".repeat(100 + (i % 10) * 50)}`;
      await pg.query(`INSERT INTO scan_test (payload) VALUES ($1)`, [payload]);
    }

    // Full scan
    const result = await pg.query(
      `SELECT COUNT(*)::int as count FROM scan_test`,
    );
    expect(result.rows[0].count).toBe(50);

    // Verify ordering is intact
    const ordered = await pg.query(
      `SELECT id FROM scan_test ORDER BY id`,
    );
    for (let i = 0; i < 50; i++) {
      expect(ordered.rows[i].id).toBe(i + 1);
    }
  },
);

// ---------------------------------------------------------------------------
// Scenario 2: Write-Heavy Update Storm
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 2: Write-heavy update storm",
  async (h) => {
    const { pg } = h;

    await pg.query(
      `CREATE TABLE counters (id SERIAL PRIMARY KEY, value INTEGER DEFAULT 0)`,
    );

    // Create 10 counters
    for (let i = 0; i < 10; i++) {
      await pg.query(`INSERT INTO counters (value) VALUES (0)`);
    }

    // Update each counter 20 times (200 total UPDATEs)
    for (let round = 0; round < 20; round++) {
      for (let id = 1; id <= 10; id++) {
        await pg.query(
          `UPDATE counters SET value = value + 1 WHERE id = $1`,
          [id],
        );
      }
    }

    // Verify all counters have correct value
    const result = await pg.query(
      `SELECT id, value FROM counters ORDER BY id`,
    );
    for (let i = 0; i < 10; i++) {
      expect(result.rows[i].value).toBe(20);
    }
  },
);

// ---------------------------------------------------------------------------
// Scenario 3: Multi-Table Joins Under Pressure
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 3: Multi-table joins under pressure",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE authors (id SERIAL PRIMARY KEY, name TEXT)`);
    await pg.query(`CREATE TABLE books (id SERIAL PRIMARY KEY, author_id INTEGER REFERENCES authors(id), title TEXT)`);
    await pg.query(`CREATE TABLE reviews (id SERIAL PRIMARY KEY, book_id INTEGER REFERENCES books(id), rating INTEGER)`);

    // Insert 10 authors
    for (let i = 0; i < 10; i++) {
      await pg.query(`INSERT INTO authors (name) VALUES ($1)`, [`Author ${i}`]);
    }

    // Each author writes 3 books
    for (let a = 1; a <= 10; a++) {
      for (let b = 0; b < 3; b++) {
        await pg.query(
          `INSERT INTO books (author_id, title) VALUES ($1, $2)`,
          [a, `Book ${a}-${b}`],
        );
      }
    }

    // Each book gets 2 reviews
    for (let b = 1; b <= 30; b++) {
      for (let r = 0; r < 2; r++) {
        await pg.query(
          `INSERT INTO reviews (book_id, rating) VALUES ($1, $2)`,
          [b, 3 + (r % 3)],
        );
      }
    }

    // 3-table join: average rating per author
    const result = await pg.query(`
      SELECT a.name, AVG(r.rating)::float as avg_rating
      FROM authors a
      JOIN books b ON b.author_id = a.id
      JOIN reviews r ON r.book_id = b.id
      GROUP BY a.id, a.name
      ORDER BY a.id
    `);

    expect(result.rows.length).toBe(10);
    // Each author has 3 books * 2 reviews = 6 reviews with ratings 3,4,3,4,3,4
    for (const row of result.rows) {
      expect(row.avg_rating).toBeCloseTo(3.5, 1);
    }
  },
);

// ---------------------------------------------------------------------------
// Scenario 4: Index-Heavy Workload
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 4: Index-heavy workload @fast",
  async (h) => {
    const { pg } = h;

    await pg.query(`
      CREATE TABLE indexed_data (
        id SERIAL PRIMARY KEY,
        category INTEGER,
        label TEXT,
        value REAL
      )
    `);
    await pg.query(`CREATE INDEX idx_category ON indexed_data (category)`);
    await pg.query(`CREATE INDEX idx_label ON indexed_data (label)`);

    // Insert 100 rows across 5 categories
    for (let i = 0; i < 100; i++) {
      await pg.query(
        `INSERT INTO indexed_data (category, label, value) VALUES ($1, $2, $3)`,
        [i % 5, `label-${i}`, Math.random() * 100],
      );
    }

    // Query using index
    const catResult = await pg.query(
      `SELECT COUNT(*)::int as count FROM indexed_data WHERE category = 2`,
    );
    expect(catResult.rows[0].count).toBe(20);

    // Query using other index
    const labelResult = await pg.query(
      `SELECT id FROM indexed_data WHERE label = 'label-42'`,
    );
    expect(labelResult.rows.length).toBe(1);

    // Delete and verify index consistency
    await pg.query(`DELETE FROM indexed_data WHERE category = 0`);
    const afterDelete = await pg.query(
      `SELECT COUNT(*)::int as count FROM indexed_data`,
    );
    expect(afterDelete.rows[0].count).toBe(80);
  },
);

// ---------------------------------------------------------------------------
// Scenario 5: Transaction Isolation Under Pressure
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 5: Transaction isolation under pressure",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE ledger (id SERIAL PRIMARY KEY, amount INTEGER)`);

    // Seed with data
    for (let i = 0; i < 20; i++) {
      await pg.query(`INSERT INTO ledger (amount) VALUES ($1)`, [100]);
    }

    // Run multiple transactions, some committed, some rolled back
    for (let round = 0; round < 5; round++) {
      // Committed transaction: increment all
      await pg.query("BEGIN");
      await pg.query(`UPDATE ledger SET amount = amount + 10`);
      await pg.query("COMMIT");

      // Rolled back transaction: should have no effect
      await pg.query("BEGIN");
      await pg.query(`UPDATE ledger SET amount = amount + 1000`);
      await pg.query("ROLLBACK");
    }

    // Each row should be 100 + (5 * 10) = 150
    const result = await pg.query(`SELECT DISTINCT amount FROM ledger`);
    expect(result.rows).toEqual([{ amount: 150 }]);
  },
);

// ---------------------------------------------------------------------------
// Scenario 6: Large Text / TOAST-like Data
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 6: Large text data (TOAST)",
  async (h) => {
    const { pg } = h;

    await pg.query(
      `CREATE TABLE documents (id SERIAL PRIMARY KEY, content TEXT)`,
    );

    // Insert documents with varying sizes (some large enough to trigger TOAST)
    const sizes = [100, 500, 2000, 8000, 16000];
    for (let i = 0; i < sizes.length; i++) {
      const content = `doc-${i}:${"a".repeat(sizes[i])}`;
      await pg.query(`INSERT INTO documents (content) VALUES ($1)`, [content]);
    }

    // Verify all documents are readable and correct size
    const result = await pg.query(
      `SELECT id, LENGTH(content) as len FROM documents ORDER BY id`,
    );
    for (let i = 0; i < sizes.length; i++) {
      // content = "doc-N:" + "a"*size
      const expectedLen = `doc-${i}:`.length + sizes[i];
      expect(result.rows[i].len).toBe(expectedLen);
    }

    // Verify content integrity for the largest document
    const largest = await pg.query(
      `SELECT content FROM documents WHERE id = $1`,
      [sizes.length],
    );
    expect(largest.rows[0].content.length).toBe(
      `doc-${sizes.length - 1}:`.length + sizes[sizes.length - 1],
    );
    expect(largest.rows[0].content.startsWith(`doc-${sizes.length - 1}:`)).toBe(
      true,
    );
  },
);

// ---------------------------------------------------------------------------
// Scenario 7: Concurrent-Style Operations (Sequential Interleaving)
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 7: Interleaved reads and writes",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE stream_a (id SERIAL PRIMARY KEY, data TEXT)`);
    await pg.query(`CREATE TABLE stream_b (id SERIAL PRIMARY KEY, data TEXT)`);

    // Interleave writes to two tables
    for (let i = 0; i < 30; i++) {
      await pg.query(`INSERT INTO stream_a (data) VALUES ($1)`, [
        `a-${i}-${"x".repeat(200)}`,
      ]);
      await pg.query(`INSERT INTO stream_b (data) VALUES ($1)`, [
        `b-${i}-${"y".repeat(200)}`,
      ]);

      // Interleave reads
      if (i % 5 === 0) {
        const a = await pg.query(
          `SELECT COUNT(*)::int as count FROM stream_a`,
        );
        expect(a.rows[0].count).toBe(i + 1);
      }
    }

    // Final verification
    const countA = await pg.query(
      `SELECT COUNT(*)::int as count FROM stream_a`,
    );
    const countB = await pg.query(
      `SELECT COUNT(*)::int as count FROM stream_b`,
    );
    expect(countA.rows[0].count).toBe(30);
    expect(countB.rows[0].count).toBe(30);

    // Verify no cross-contamination
    const aData = await pg.query(
      `SELECT data FROM stream_a ORDER BY id LIMIT 1`,
    );
    expect(aData.rows[0].data.startsWith("a-0-")).toBe(true);

    const bData = await pg.query(
      `SELECT data FROM stream_b ORDER BY id LIMIT 1`,
    );
    expect(bData.rows[0].data.startsWith("b-0-")).toBe(true);
  },
);
