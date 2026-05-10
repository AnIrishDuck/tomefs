/**
 * PGlite + tomefs table-rewrite DDL stress tests (ethos §8, §9).
 *
 * Table-rewrite operations create the most demanding I/O pattern for a
 * bounded page cache: they read ALL pages of one or more source relations
 * while simultaneously writing ALL pages of a new destination relation.
 * Under tiny cache (4 pages), source and destination pages compete for
 * the same cache slots, forcing eviction cascades on every page.
 *
 * Key operations tested:
 *   - CREATE TABLE AS SELECT (CTAS) — reads source heap, writes new heap
 *   - TRUNCATE — drops old heap, creates empty replacement
 *   - CLUSTER — rewrites table ordered by an index
 *   - REFRESH MATERIALIZED VIEW — rewrites materialized view from query
 *   - DO $$ procedural loops — tight read/write cycles in PL/pgSQL
 *   - Large transaction rollback — allocate many dirty pages then discard
 *
 * These differ from INSERT-level tests (cache-pressure.test.ts) because
 * the FS must handle file creation + deletion of entire heap files as
 * atomic DDL operations, not individual tuple inserts.
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
// Scenario 1: CREATE TABLE AS SELECT from populated table
// ---------------------------------------------------------------------------

describe("CTAS from populated table", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(
        `CREATE TABLE ctas_source (id SERIAL PRIMARY KEY, val INT, payload TEXT)`,
      );
      for (let i = 0; i < 80; i++) {
        await pg.query(
          `INSERT INTO ctas_source (val, payload) VALUES ($1, $2)`,
          [i * 3, `src-${i}-${"a".repeat(100)}`],
        );
      }

      // CTAS reads all source pages and writes all destination pages
      await pg.query(
        `CREATE TABLE ctas_dest AS SELECT id, val, payload FROM ctas_source WHERE val % 2 = 0`,
      );

      const srcCount = await pg.query(
        `SELECT COUNT(*)::int AS c FROM ctas_source`,
      );
      expect(srcCount.rows[0].c).toBe(80);

      const destCount = await pg.query(
        `SELECT COUNT(*)::int AS c FROM ctas_dest`,
      );
      // val = i*3, so val%2=0 when i is even → 40 rows
      expect(destCount.rows[0].c).toBe(40);

      // Verify data integrity in destination
      const sample = await pg.query(
        `SELECT val FROM ctas_dest ORDER BY val LIMIT 3`,
      );
      expect(sample.rows[0].val).toBe(0);
      expect(sample.rows[1].val).toBe(6);
      expect(sample.rows[2].val).toBe(12);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 2: TRUNCATE vs DELETE behavior under cache pressure
// ---------------------------------------------------------------------------

describe("TRUNCATE clears table and allows re-population", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(
        `CREATE TABLE trunc_test (id SERIAL PRIMARY KEY, data TEXT)`,
      );
      for (let i = 0; i < 60; i++) {
        await pg.query(
          `INSERT INTO trunc_test (data) VALUES ($1)`,
          [`row-${i}-${"t".repeat(150)}`],
        );
      }

      // TRUNCATE drops the old heap file and creates an empty one —
      // fundamentally different from DELETE which marks tuples dead
      await pg.query(`TRUNCATE trunc_test`);

      const afterTrunc = await pg.query(
        `SELECT COUNT(*)::int AS c FROM trunc_test`,
      );
      expect(afterTrunc.rows[0].c).toBe(0);

      // Re-populate after TRUNCATE — new heap file gets new pages
      for (let i = 0; i < 40; i++) {
        await pg.query(
          `INSERT INTO trunc_test (data) VALUES ($1)`,
          [`new-${i}-${"n".repeat(100)}`],
        );
      }

      const afterRepop = await pg.query(
        `SELECT COUNT(*)::int AS c FROM trunc_test`,
      );
      expect(afterRepop.rows[0].c).toBe(40);

      // Sequence should continue (TRUNCATE doesn't reset SERIAL by default)
      const maxId = await pg.query(
        `SELECT MAX(id)::int AS m FROM trunc_test`,
      );
      expect(maxId.rows[0].m).toBeGreaterThan(60);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 3: CLUSTER rewrites table ordered by index
// ---------------------------------------------------------------------------

describe("CLUSTER reorders table by index", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(
        `CREATE TABLE cluster_test (id SERIAL PRIMARY KEY, sort_key INT, data TEXT)`,
      );
      // Insert in reverse order of sort_key
      for (let i = 49; i >= 0; i--) {
        await pg.query(
          `INSERT INTO cluster_test (sort_key, data) VALUES ($1, $2)`,
          [i, `item-${i}-${"c".repeat(120)}`],
        );
      }

      await pg.query(`CREATE INDEX idx_cluster_sort ON cluster_test (sort_key)`);

      // CLUSTER rewrites the entire table ordered by idx_cluster_sort —
      // reads all pages via index scan, writes new heap in sorted order
      await pg.query(`CLUSTER cluster_test USING idx_cluster_sort`);

      const result = await pg.query(
        `SELECT sort_key FROM cluster_test ORDER BY ctid LIMIT 5`,
      );
      // After CLUSTER, physical order should match sort_key order
      for (let i = 0; i < 5; i++) {
        expect(result.rows[i].sort_key).toBe(i);
      }

      // Verify all data survived
      const count = await pg.query(
        `SELECT COUNT(*)::int AS c FROM cluster_test`,
      );
      expect(count.rows[0].c).toBe(50);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 4: REFRESH MATERIALIZED VIEW (table rewrite from query)
// ---------------------------------------------------------------------------

describe("Materialized view REFRESH under cache pressure", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(
        `CREATE TABLE matview_source (id SERIAL PRIMARY KEY, category INT, amount INT)`,
      );
      for (let i = 0; i < 80; i++) {
        await pg.query(
          `INSERT INTO matview_source (category, amount) VALUES ($1, $2)`,
          [i % 5, (i + 1) * 10],
        );
      }

      await pg.query(`
        CREATE MATERIALIZED VIEW matview_summary AS
        SELECT category, COUNT(*)::int AS cnt, SUM(amount)::int AS total
        FROM matview_source GROUP BY category
      `);

      const initial = await pg.query(
        `SELECT category, cnt FROM matview_summary ORDER BY category`,
      );
      expect(initial.rows.length).toBe(5);
      for (const row of initial.rows) {
        expect(row.cnt).toBe(16);
      }

      // Add more data to source
      for (let i = 0; i < 20; i++) {
        await pg.query(
          `INSERT INTO matview_source (category, amount) VALUES ($1, $2)`,
          [i % 5, 1000],
        );
      }

      // REFRESH rewrites the materialized view — reads source, writes new view
      await pg.query(`REFRESH MATERIALIZED VIEW matview_summary`);

      const refreshed = await pg.query(
        `SELECT category, cnt FROM matview_summary ORDER BY category`,
      );
      expect(refreshed.rows.length).toBe(5);
      for (const row of refreshed.rows) {
        expect(row.cnt).toBe(20); // 16 original + 4 new per category
      }
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 5: DO $$ procedural loop with tight read/write cycles
// ---------------------------------------------------------------------------

describe("DO block procedural loop under cache pressure", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(
        `CREATE TABLE do_loop (id INT PRIMARY KEY, counter INT, data TEXT)`,
      );
      // Pre-populate
      for (let i = 0; i < 30; i++) {
        await pg.query(
          `INSERT INTO do_loop VALUES ($1, 0, $2)`,
          [i, `initial-${i}`],
        );
      }

      // DO block performs 10 update cycles over all rows — sustained
      // read/write pressure from PL/pgSQL, not client round-trips
      await pg.query(`
        DO $$
        BEGIN
          FOR cycle IN 1..10 LOOP
            UPDATE do_loop SET counter = counter + 1, data = 'cycle-' || cycle;
          END LOOP;
        END;
        $$
      `);

      const result = await pg.query(
        `SELECT id, counter, data FROM do_loop ORDER BY id LIMIT 5`,
      );
      for (const row of result.rows) {
        expect(row.counter).toBe(10);
        expect(row.data).toBe("cycle-10");
      }

      const total = await pg.query(
        `SELECT COUNT(*)::int AS c FROM do_loop`,
      );
      expect(total.rows[0].c).toBe(30);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 6: Large transaction rollback (allocate many pages, discard all)
// ---------------------------------------------------------------------------

describe("Large transaction rollback discards all dirty pages", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(
        `CREATE TABLE rollback_test (id INT, data TEXT)`,
      );
      // Insert baseline that must survive
      for (let i = 0; i < 10; i++) {
        await pg.query(
          `INSERT INTO rollback_test VALUES ($1, $2)`,
          [i, `keep-${i}`],
        );
      }

      // Large transaction that writes many pages then rolls back
      await pg.query(`BEGIN`);
      for (let i = 0; i < 50; i++) {
        await pg.query(
          `INSERT INTO rollback_test VALUES ($1, $2)`,
          [100 + i, `discard-${i}-${"x".repeat(200)}`],
        );
      }
      await pg.query(`ROLLBACK`);

      // Only baseline rows should exist
      const result = await pg.query(
        `SELECT COUNT(*)::int AS c FROM rollback_test`,
      );
      expect(result.rows[0].c).toBe(10);

      const rows = await pg.query(
        `SELECT id, data FROM rollback_test ORDER BY id`,
      );
      for (let i = 0; i < 10; i++) {
        expect(rows.rows[i].id).toBe(i);
        expect(rows.rows[i].data).toBe(`keep-${i}`);
      }
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 7: TRUNCATE RESTART IDENTITY resets sequence
// ---------------------------------------------------------------------------

describe("TRUNCATE RESTART IDENTITY resets sequence and allows clean re-insert", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(
        `CREATE TABLE trunc_restart (id SERIAL PRIMARY KEY, val TEXT)`,
      );
      for (let i = 0; i < 40; i++) {
        await pg.query(
          `INSERT INTO trunc_restart (val) VALUES ($1)`,
          [`old-${i}`],
        );
      }

      await pg.query(`TRUNCATE trunc_restart RESTART IDENTITY`);

      // Re-insert — IDs should start from 1 again
      for (let i = 0; i < 20; i++) {
        await pg.query(
          `INSERT INTO trunc_restart (val) VALUES ($1)`,
          [`new-${i}`],
        );
      }

      const result = await pg.query(
        `SELECT id, val FROM trunc_restart ORDER BY id`,
      );
      expect(result.rows.length).toBe(20);
      expect(result.rows[0].id).toBe(1);
      expect(result.rows[0].val).toBe("new-0");
      expect(result.rows[19].id).toBe(20);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 8: CTAS + persistence round-trip
// ---------------------------------------------------------------------------

describe("CTAS survives persistence round-trip", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages)`, async () => {
      const backend = new SyncMemoryBackend();

      // Phase 1: create source, CTAS, verify
      const h1 = await create(size, backend);
      await h1.pg.query(
        `CREATE TABLE ctas_persist_src (id SERIAL, category INT, data TEXT)`,
      );
      for (let i = 0; i < 60; i++) {
        await h1.pg.query(
          `INSERT INTO ctas_persist_src (category, data) VALUES ($1, $2)`,
          [i % 3, `data-${i}-${"p".repeat(80)}`],
        );
      }

      await h1.pg.query(
        `CREATE TABLE ctas_persist_dest AS SELECT * FROM ctas_persist_src WHERE category = 1`,
      );

      await h1.syncToFs();
      await h1.destroy();

      // Phase 2: remount, verify both tables
      const h2 = await create(size, backend);

      const srcCount = await h2.pg.query(
        `SELECT COUNT(*)::int AS c FROM ctas_persist_src`,
      );
      expect(srcCount.rows[0].c).toBe(60);

      const destCount = await h2.pg.query(
        `SELECT COUNT(*)::int AS c FROM ctas_persist_dest`,
      );
      expect(destCount.rows[0].c).toBe(20);

      // Verify we can still query the CTAS destination
      const sample = await h2.pg.query(
        `SELECT category FROM ctas_persist_dest LIMIT 1`,
      );
      expect(sample.rows[0].category).toBe(1);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 9: TRUNCATE + persistence round-trip
// ---------------------------------------------------------------------------

describe("TRUNCATE persists through syncfs + remount", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages)`, async () => {
      const backend = new SyncMemoryBackend();

      // Phase 1: populate, truncate, re-populate
      const h1 = await create(size, backend);
      await h1.pg.query(`CREATE TABLE trunc_persist (id SERIAL, val TEXT)`);
      for (let i = 0; i < 40; i++) {
        await h1.pg.query(
          `INSERT INTO trunc_persist (val) VALUES ($1)`,
          [`old-${i}`],
        );
      }
      await h1.pg.query(`TRUNCATE trunc_persist`);
      for (let i = 0; i < 15; i++) {
        await h1.pg.query(
          `INSERT INTO trunc_persist (val) VALUES ($1)`,
          [`new-${i}`],
        );
      }

      await h1.syncToFs();
      await h1.destroy();

      // Phase 2: remount
      const h2 = await create(size, backend);

      const count = await h2.pg.query(
        `SELECT COUNT(*)::int AS c FROM trunc_persist`,
      );
      expect(count.rows[0].c).toBe(15);

      const first = await h2.pg.query(
        `SELECT val FROM trunc_persist ORDER BY id LIMIT 1`,
      );
      expect(first.rows[0].val).toBe("new-0");
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 10: Multiple CTAS from same source (competing rewrites)
// ---------------------------------------------------------------------------

describe("Multiple CTAS from same source table", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(
        `CREATE TABLE multi_ctas_src (id SERIAL, grp INT, val INT)`,
      );
      for (let i = 0; i < 60; i++) {
        await pg.query(
          `INSERT INTO multi_ctas_src (grp, val) VALUES ($1, $2)`,
          [i % 4, i * 7],
        );
      }

      // Create 4 destination tables from the same source — each CTAS
      // reads the full source heap, creating repeated cache pressure
      await pg.query(
        `CREATE TABLE ctas_grp0 AS SELECT * FROM multi_ctas_src WHERE grp = 0`,
      );
      await pg.query(
        `CREATE TABLE ctas_grp1 AS SELECT * FROM multi_ctas_src WHERE grp = 1`,
      );
      await pg.query(
        `CREATE TABLE ctas_grp2 AS SELECT * FROM multi_ctas_src WHERE grp = 2`,
      );
      await pg.query(
        `CREATE TABLE ctas_grp3 AS SELECT * FROM multi_ctas_src WHERE grp = 3`,
      );

      // Verify each has the correct count
      for (let g = 0; g < 4; g++) {
        const count = await pg.query(
          `SELECT COUNT(*)::int AS c FROM ctas_grp${g}`,
        );
        expect(count.rows[0].c).toBe(15);
      }

      // Verify source is untouched
      const srcCount = await pg.query(
        `SELECT COUNT(*)::int AS c FROM multi_ctas_src`,
      );
      expect(srcCount.rows[0].c).toBe(60);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 11: DO block + VACUUM combined workload
// ---------------------------------------------------------------------------

describe("DO block bulk insert + VACUUM cycle", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`CREATE TABLE do_vacuum (id INT, batch INT, data TEXT)`);

      // DO block inserts 3 batches, each followed by deleting the previous
      await pg.query(`
        DO $$
        DECLARE
          b INT;
        BEGIN
          FOR b IN 1..3 LOOP
            FOR i IN 1..25 LOOP
              INSERT INTO do_vacuum VALUES (i + (b - 1) * 25, b, 'batch-' || b || '-row-' || i);
            END LOOP;
            IF b > 1 THEN
              DELETE FROM do_vacuum WHERE batch < b;
            END IF;
          END LOOP;
        END;
        $$
      `);

      await pg.query(`VACUUM do_vacuum`);

      // After all batches, we should have rows from batch 2 and 3
      // (batch 1 deleted when batch=2, batch 2 deleted when batch=3...
      // actually let me re-read the logic)
      // The DELETE in each batch deletes rows where batch < current_batch
      // So after batch=2: rows from batch 1 are deleted
      // After batch=3: rows from batch 1 and 2 are deleted
      // Final state: only batch=3 rows remain
      const result = await pg.query(
        `SELECT COUNT(*)::int AS c FROM do_vacuum WHERE batch = 3`,
      );
      expect(result.rows[0].c).toBe(25);

      const total = await pg.query(
        `SELECT COUNT(*)::int AS c FROM do_vacuum`,
      );
      expect(total.rows[0].c).toBe(25);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 12: Large rollback + persistence (verify rollback doesn't persist)
// ---------------------------------------------------------------------------

describe("Large rollback data does not persist through syncfs + remount", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages)`, async () => {
      const backend = new SyncMemoryBackend();

      // Phase 1: committed data + rolled-back data + syncfs
      const h1 = await create(size, backend);
      await h1.pg.query(`CREATE TABLE rollback_persist (id INT, status TEXT)`);
      for (let i = 0; i < 10; i++) {
        await h1.pg.query(
          `INSERT INTO rollback_persist VALUES ($1, 'committed')`,
          [i],
        );
      }

      // Large rolled-back transaction
      await h1.pg.query(`BEGIN`);
      for (let i = 0; i < 60; i++) {
        await h1.pg.query(
          `INSERT INTO rollback_persist VALUES ($1, $2)`,
          [100 + i, `rolled-back-${"r".repeat(150)}`],
        );
      }
      await h1.pg.query(`ROLLBACK`);

      // Add more committed data after rollback
      await h1.pg.query(
        `INSERT INTO rollback_persist VALUES (99, 'after-rollback')`,
      );

      await h1.syncToFs();
      await h1.destroy();

      // Phase 2: remount and verify
      const h2 = await create(size, backend);

      const count = await h2.pg.query(
        `SELECT COUNT(*)::int AS c FROM rollback_persist`,
      );
      expect(count.rows[0].c).toBe(11); // 10 + 1 after-rollback

      // Verify no rolled-back data persisted
      const rolledBack = await h2.pg.query(
        `SELECT COUNT(*)::int AS c FROM rollback_persist WHERE id >= 100`,
      );
      expect(rolledBack.rows[0].c).toBe(0);

      const afterRow = await h2.pg.query(
        `SELECT status FROM rollback_persist WHERE id = 99`,
      );
      expect(afterRow.rows[0].status).toBe("after-rollback");
    });
  }
});
