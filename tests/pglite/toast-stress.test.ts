/**
 * PGlite TOAST (The Oversized-Attribute Storage Technique) stress tests.
 *
 * TOAST is Postgres's mechanism for storing large values (>2KB) out-of-line.
 * Each large value is compressed and split into ~2000-byte chunks stored in
 * a separate TOAST table with its own heap and BTREE index. Reading a single
 * large value requires:
 *   1. Main heap page access (fetch the row with the TOAST pointer)
 *   2. TOAST index scan (find chunk OIDs for this value)
 *   3. Multiple TOAST heap page reads (one per chunk)
 *
 * Under tiny cache (4 pages = 32KB), a single 100KB value requires ~50 TOAST
 * chunks spread across ~6 heap pages, plus index pages — rotating the entire
 * cache multiple times within a single SELECT. This is the worst-case access
 * pattern for a page cache: high page fan-out from a single SQL statement.
 *
 * These tests exercise:
 *   - TOAST storage + retrieval at all cache pressure levels
 *   - Persistence round-trips (syncfs → remount → verify large values survive)
 *   - Dirty shutdown + WAL recovery with TOAST data
 *   - UPDATE of TOAST'd values (new chunks + old chunk reclamation)
 *   - VACUUM of TOAST tables under cache pressure
 *   - BYTEA exact byte-level integrity verification
 *   - Mixed TOAST + non-TOAST columns in the same table
 *   - TOAST with indexes (index scan → heap → TOAST decompression)
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

function generateLargeText(size: number, seed: number): string {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  const parts: string[] = [];
  let s = seed;
  for (let i = 0; i < size; i++) {
    s = (s * 1103515245 + 12345) & 0x7fffffff;
    parts.push(chars[s % chars.length]);
  }
  return parts.join("");
}

function generateLargeBytes(size: number, seed: number): Buffer {
  const buf = Buffer.alloc(size);
  let s = seed;
  for (let i = 0; i < size; i++) {
    s = (s * 1103515245 + 12345) & 0x7fffffff;
    buf[i] = s & 0xff;
  }
  return buf;
}

// ---------------------------------------------------------------------------
// Scenario 1: TOAST storage + retrieval with varying sizes
// ---------------------------------------------------------------------------

describeScenario(
  "TOAST: large text insert and retrieval @fast",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE toast_text (id SERIAL PRIMARY KEY, content TEXT)`);

    const sizes = [3000, 8000, 20000, 50000, 100000];
    const values: string[] = [];

    for (let i = 0; i < sizes.length; i++) {
      const content = generateLargeText(sizes[i], i * 7 + 42);
      values.push(content);
      await pg.query(`INSERT INTO toast_text (content) VALUES ($1)`, [content]);
    }

    for (let i = 0; i < sizes.length; i++) {
      const result = await pg.query(
        `SELECT content FROM toast_text WHERE id = $1`,
        [i + 1],
      );
      expect(result.rows[0].content).toBe(values[i]);
    }

    const lengths = await pg.query(
      `SELECT id, LENGTH(content) as len FROM toast_text ORDER BY id`,
    );
    for (let i = 0; i < sizes.length; i++) {
      expect(lengths.rows[i].len).toBe(sizes[i]);
    }
  },
);

// ---------------------------------------------------------------------------
// Scenario 2: TOAST with BYTEA — exact byte-level integrity
// ---------------------------------------------------------------------------

describeScenario(
  "TOAST: BYTEA byte-level integrity",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE toast_bytea (id SERIAL PRIMARY KEY, data BYTEA)`);

    const sizes = [4000, 16000, 64000];
    const buffers: Buffer[] = [];

    for (let i = 0; i < sizes.length; i++) {
      const data = generateLargeBytes(sizes[i], i * 13 + 99);
      buffers.push(data);
      await pg.query(`INSERT INTO toast_bytea (data) VALUES ($1)`, [data]);
    }

    for (let i = 0; i < sizes.length; i++) {
      const result = await pg.query(
        `SELECT data FROM toast_bytea WHERE id = $1`,
        [i + 1],
      );
      const retrieved = Buffer.from(result.rows[0].data);
      expect(retrieved.length).toBe(sizes[i]);
      expect(retrieved.equals(buffers[i])).toBe(true);
    }
  },
);

// ---------------------------------------------------------------------------
// Scenario 3: TOAST persistence round-trip
// ---------------------------------------------------------------------------

describe("TOAST: persistence round-trip", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];
    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const backend = new SyncMemoryBackend();

      // Phase 1: Create and populate
      harness = await createPGliteHarness({ cacheSize: size, backend });
      const { pg } = harness;

      await pg.query(`CREATE TABLE toast_persist (id SERIAL PRIMARY KEY, content TEXT, small_col INTEGER)`);

      const largeValue = generateLargeText(50000, 314);
      await pg.query(`INSERT INTO toast_persist (content, small_col) VALUES ($1, 42)`, [largeValue]);
      await pg.query(`INSERT INTO toast_persist (content, small_col) VALUES ($1, 99)`, [
        generateLargeText(30000, 271),
      ]);

      await harness.syncToFs();
      await harness.destroy();
      harness = null;

      // Phase 2: Remount and verify
      harness = await createPGliteHarness({ cacheSize: size, backend });
      const pg2 = harness.pg;

      const result = await pg2.query(
        `SELECT id, LENGTH(content) as len, small_col FROM toast_persist ORDER BY id`,
      );
      expect(result.rows.length).toBe(2);
      expect(result.rows[0].len).toBe(50000);
      expect(result.rows[0].small_col).toBe(42);
      expect(result.rows[1].len).toBe(30000);
      expect(result.rows[1].small_col).toBe(99);

      // Verify exact content
      const content = await pg2.query(
        `SELECT content FROM toast_persist WHERE id = 1`,
      );
      expect(content.rows[0].content).toBe(largeValue);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 4: TOAST dirty shutdown + WAL recovery
// ---------------------------------------------------------------------------

describe("TOAST: dirty shutdown + WAL recovery", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];
    it(`cache=${size} (${pages} pages)`, async () => {
      const backend = new SyncMemoryBackend();

      // Phase 1: Establish baseline with syncfs
      harness = await createPGliteHarness({ cacheSize: size, backend });
      const { pg } = harness;

      await pg.query(`CREATE TABLE toast_crash (id SERIAL PRIMARY KEY, content TEXT)`);
      const baseValue = generateLargeText(40000, 555);
      await pg.query(`INSERT INTO toast_crash (content) VALUES ($1)`, [baseValue]);
      await harness.syncToFs();

      // Phase 2: Write more data WITHOUT syncfs (dirty phase)
      const dirtyValue = generateLargeText(60000, 777);
      await pg.query(`INSERT INTO toast_crash (content) VALUES ($1)`, [dirtyValue]);

      // Dirty shutdown — don't call syncToFs or close
      harness.dirtyDestroy();
      harness = null;

      // Phase 3: Remount — Postgres WAL replay recovers
      harness = await createPGliteHarness({ cacheSize: size, backend });
      const pg2 = harness.pg;

      // Baseline row must survive
      const result = await pg2.query(
        `SELECT id, LENGTH(content) as len FROM toast_crash ORDER BY id`,
      );
      expect(result.rows.length).toBeGreaterThanOrEqual(1);
      expect(result.rows[0].len).toBe(40000);

      const content = await pg2.query(
        `SELECT content FROM toast_crash WHERE id = 1`,
      );
      expect(content.rows[0].content).toBe(baseValue);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 5: TOAST UPDATE replaces large values
// ---------------------------------------------------------------------------

describeScenario(
  "TOAST: UPDATE replaces large values",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE toast_update (id SERIAL PRIMARY KEY, content TEXT)`);

    // Insert initial large value
    const v1 = generateLargeText(40000, 111);
    await pg.query(`INSERT INTO toast_update (content) VALUES ($1)`, [v1]);

    // Update with different large value (creates new TOAST chunks)
    const v2 = generateLargeText(60000, 222);
    await pg.query(`UPDATE toast_update SET content = $1 WHERE id = 1`, [v2]);

    // Verify update took effect
    const result = await pg.query(`SELECT content FROM toast_update WHERE id = 1`);
    expect(result.rows[0].content).toBe(v2);

    // Update again with smaller value
    const v3 = generateLargeText(10000, 333);
    await pg.query(`UPDATE toast_update SET content = $1 WHERE id = 1`, [v3]);

    const result2 = await pg.query(`SELECT content FROM toast_update WHERE id = 1`);
    expect(result2.rows[0].content).toBe(v3);
  },
);

// ---------------------------------------------------------------------------
// Scenario 6: VACUUM reclaims TOAST chunks
// ---------------------------------------------------------------------------

describeScenario(
  "TOAST: VACUUM reclaims dead chunks @fast",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE toast_vacuum (id SERIAL PRIMARY KEY, content TEXT)`);

    // Insert several large values
    for (let i = 0; i < 5; i++) {
      await pg.query(`INSERT INTO toast_vacuum (content) VALUES ($1)`, [
        generateLargeText(30000, i * 37),
      ]);
    }

    // Delete most rows — their TOAST chunks become dead
    await pg.query(`DELETE FROM toast_vacuum WHERE id <= 4`);

    // VACUUM should reclaim the dead TOAST chunks
    await pg.query(`VACUUM toast_vacuum`);

    // Surviving row still intact
    const result = await pg.query(`SELECT content FROM toast_vacuum WHERE id = 5`);
    const expected = generateLargeText(30000, 4 * 37);
    expect(result.rows[0].content).toBe(expected);

    // Insert more data after vacuum (reuses space)
    const newValue = generateLargeText(25000, 999);
    await pg.query(`INSERT INTO toast_vacuum (content) VALUES ($1)`, [newValue]);
    const result2 = await pg.query(`SELECT content FROM toast_vacuum WHERE id = 6`);
    expect(result2.rows[0].content).toBe(newValue);
  },
);

// ---------------------------------------------------------------------------
// Scenario 7: Mixed TOAST + non-TOAST columns
// ---------------------------------------------------------------------------

describeScenario(
  "TOAST: mixed large and small columns",
  async (h) => {
    const { pg } = h;

    await pg.query(`
      CREATE TABLE toast_mixed (
        id SERIAL PRIMARY KEY,
        title VARCHAR(100),
        body TEXT,
        score INTEGER,
        metadata TEXT
      )
    `);

    const rows: Array<{ title: string; body: string; score: number; metadata: string }> = [];
    for (let i = 0; i < 10; i++) {
      const row = {
        title: `Title ${i}`,
        body: generateLargeText(i < 5 ? 5000 : 50000, i * 11),
        score: i * 10,
        metadata: i % 2 === 0 ? generateLargeText(8000, i * 23) : `small-${i}`,
      };
      rows.push(row);
      await pg.query(
        `INSERT INTO toast_mixed (title, body, score, metadata) VALUES ($1, $2, $3, $4)`,
        [row.title, row.body, row.score, row.metadata],
      );
    }

    // Verify all rows — accesses both TOAST'd and inline columns
    for (let i = 0; i < 10; i++) {
      const result = await pg.query(
        `SELECT title, body, score, metadata FROM toast_mixed WHERE id = $1`,
        [i + 1],
      );
      expect(result.rows[0].title).toBe(rows[i].title);
      expect(result.rows[0].body).toBe(rows[i].body);
      expect(result.rows[0].score).toBe(rows[i].score);
      expect(result.rows[0].metadata).toBe(rows[i].metadata);
    }

    // Aggregate over non-TOAST column (should not require TOAST access)
    const sum = await pg.query(`SELECT SUM(score)::int as total FROM toast_mixed`);
    expect(sum.rows[0].total).toBe(450);
  },
);

// ---------------------------------------------------------------------------
// Scenario 8: TOAST with index scan
// ---------------------------------------------------------------------------

describeScenario(
  "TOAST: index scan triggers TOAST decompression",
  async (h) => {
    const { pg } = h;

    await pg.query(`
      CREATE TABLE toast_indexed (
        id SERIAL PRIMARY KEY,
        category INTEGER,
        payload TEXT
      )
    `);
    await pg.query(`CREATE INDEX idx_toast_cat ON toast_indexed (category)`);

    // Insert rows with large payloads across 3 categories
    for (let i = 0; i < 15; i++) {
      await pg.query(
        `INSERT INTO toast_indexed (category, payload) VALUES ($1, $2)`,
        [i % 3, generateLargeText(20000, i * 53)],
      );
    }

    // Index scan on category → heap fetch → TOAST decompression
    const result = await pg.query(
      `SELECT id, payload FROM toast_indexed WHERE category = 1 ORDER BY id`,
    );
    expect(result.rows.length).toBe(5);
    for (let i = 0; i < 5; i++) {
      const expectedId = 2 + i * 3; // ids: 2, 5, 8, 11, 14
      expect(result.rows[i].id).toBe(expectedId);
      expect(result.rows[i].payload).toBe(
        generateLargeText(20000, (expectedId - 1) * 53),
      );
    }
  },
);

// ---------------------------------------------------------------------------
// Scenario 9: Multiple large value UPDATEs + persistence
// ---------------------------------------------------------------------------

describe("TOAST: update cycle + persistence round-trip", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];
    it(`cache=${size} (${pages} pages)`, async () => {
      const backend = new SyncMemoryBackend();

      harness = await createPGliteHarness({ cacheSize: size, backend });
      const { pg } = harness;

      await pg.query(`CREATE TABLE toast_evolve (id SERIAL PRIMARY KEY, content TEXT)`);
      await pg.query(`INSERT INTO toast_evolve (content) VALUES ($1)`, [
        generateLargeText(30000, 100),
      ]);

      // Perform 3 update cycles with sync between each
      for (let cycle = 0; cycle < 3; cycle++) {
        const newContent = generateLargeText(25000 + cycle * 10000, 200 + cycle);
        await pg.query(`UPDATE toast_evolve SET content = $1 WHERE id = 1`, [newContent]);
        await harness.syncToFs();
      }

      // Final value
      const finalContent = generateLargeText(25000 + 2 * 10000, 202);
      await harness.destroy();
      harness = null;

      // Remount and verify
      harness = await createPGliteHarness({ cacheSize: size, backend });
      const pg2 = harness.pg;

      const result = await pg2.query(`SELECT content FROM toast_evolve WHERE id = 1`);
      expect(result.rows[0].content).toBe(finalContent);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 10: TOAST + FULL VACUUM (reclaims space to OS)
// ---------------------------------------------------------------------------

describeScenario(
  "TOAST: VACUUM FULL rewrite under cache pressure",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE toast_full_vac (id SERIAL PRIMARY KEY, data TEXT)`);

    // Fill with large values
    for (let i = 0; i < 8; i++) {
      await pg.query(`INSERT INTO toast_full_vac (data) VALUES ($1)`, [
        generateLargeText(15000, i * 71),
      ]);
    }

    // Delete half
    await pg.query(`DELETE FROM toast_full_vac WHERE id <= 4`);

    // VACUUM FULL rewrites both main table and TOAST table
    await pg.query(`VACUUM FULL toast_full_vac`);

    // Verify survivors intact
    for (let i = 4; i < 8; i++) {
      const result = await pg.query(
        `SELECT data FROM toast_full_vac WHERE id = $1`,
        [i + 1],
      );
      expect(result.rows[0].data).toBe(generateLargeText(15000, i * 71));
    }

    // Table still functional after VACUUM FULL
    await pg.query(`INSERT INTO toast_full_vac (data) VALUES ($1)`, [
      generateLargeText(20000, 888),
    ]);
    const latest = await pg.query(
      `SELECT data FROM toast_full_vac ORDER BY id DESC LIMIT 1`,
    );
    expect(latest.rows[0].data).toBe(generateLargeText(20000, 888));
  },
);

// ---------------------------------------------------------------------------
// Scenario 11: TOAST with string aggregation
// ---------------------------------------------------------------------------

describeScenario(
  "TOAST: string_agg produces large output",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE toast_agg (id SERIAL PRIMARY KEY, chunk TEXT)`);

    // Insert many medium chunks that aggregate into a large result
    const chunks: string[] = [];
    for (let i = 0; i < 20; i++) {
      const chunk = generateLargeText(2000, i * 41);
      chunks.push(chunk);
      await pg.query(`INSERT INTO toast_agg (chunk) VALUES ($1)`, [chunk]);
    }

    // Aggregate all chunks — result is ~40KB TOAST'd in the query result
    const result = await pg.query(
      `SELECT string_agg(chunk, '|' ORDER BY id) as combined FROM toast_agg`,
    );
    const expected = chunks.join("|");
    expect(result.rows[0].combined).toBe(expected);
  },
);

// ---------------------------------------------------------------------------
// Scenario 12: TOAST column added via ALTER TABLE
// ---------------------------------------------------------------------------

describeScenario(
  "TOAST: ALTER TABLE ADD COLUMN with large default",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE toast_alter (id SERIAL PRIMARY KEY, name TEXT)`);
    for (let i = 0; i < 10; i++) {
      await pg.query(`INSERT INTO toast_alter (name) VALUES ($1)`, [`row-${i}`]);
    }

    // Add a column with a large default — 4KB triggers TOAST but fits
    // within Postgres's 8160-byte max tuple size (name + description + overhead)
    const largeDefault = generateLargeText(4000, 654);
    // DDL doesn't support parameterized queries — use dollar-quoted literal
    await pg.query(
      `ALTER TABLE toast_alter ADD COLUMN description TEXT DEFAULT $tag$${largeDefault}$tag$`,
    );

    // Verify all existing rows got the default
    const result = await pg.query(
      `SELECT id, description FROM toast_alter ORDER BY id`,
    );
    expect(result.rows.length).toBe(10);
    for (const row of result.rows) {
      expect(row.description).toBe(largeDefault);
    }

    // New inserts also get the default
    await pg.query(`INSERT INTO toast_alter (name) VALUES ('new-row')`);
    const newRow = await pg.query(
      `SELECT description FROM toast_alter WHERE name = 'new-row'`,
    );
    expect(newRow.rows[0].description).toBe(largeDefault);
  },
);
