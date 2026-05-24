/**
 * PGlite + tomefs cursor-based iteration stress tests (ethos §8, §9).
 *
 * Cursors create uniquely challenging page access patterns for a bounded
 * page cache:
 *
 * - Each FETCH reads a batch of rows from the heap, touching pages that
 *   may have been evicted since the last FETCH. Under tiny cache, every
 *   FETCH batch is a guaranteed cache miss.
 * - SCROLL cursors allow backward iteration, creating bidirectional
 *   access that is adversarial to LRU — forward scans evict early pages,
 *   then backward FETCHes need them again.
 * - Multiple open cursors on different tables interleave page access,
 *   so each cursor's FETCH evicts pages needed by other cursors.
 * - WITH HOLD cursors survive transaction boundaries, testing the
 *   interaction between cursor state and commit/sync behavior.
 *
 * These patterns differ fundamentally from INSERT/SELECT stress tests
 * because the cursor holds a stable position in the result set across
 * multiple round-trips, forcing the page cache to re-materialize
 * previously-evicted pages at unpredictable intervals.
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
// Scenario 1: Forward cursor iteration through large result set
// ---------------------------------------------------------------------------

describe("Forward cursor iteration through large result set", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`CREATE TABLE cursor_data (id INT PRIMARY KEY, val TEXT)`);
      await pg.query(
        `INSERT INTO cursor_data SELECT g, 'row-' || g || '-${"x".repeat(80)}'
         FROM generate_series(1, 100) g`,
      );

      await pg.query("BEGIN");
      await pg.query(
        "DECLARE fwd_cursor CURSOR FOR SELECT id, val FROM cursor_data ORDER BY id",
      );

      const allIds: number[] = [];
      for (let batch = 0; batch < 10; batch++) {
        const result = await pg.query("FETCH 10 FROM fwd_cursor");
        expect(result.rows.length).toBe(10);
        for (const row of result.rows) {
          allIds.push(Number(row.id));
        }
      }

      const empty = await pg.query("FETCH 10 FROM fwd_cursor");
      expect(empty.rows.length).toBe(0);

      await pg.query("CLOSE fwd_cursor");
      await pg.query("COMMIT");

      for (let i = 0; i < 100; i++) {
        expect(allIds[i]).toBe(i + 1);
      }
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 2: Multiple concurrent cursors on different tables
// ---------------------------------------------------------------------------

describe("Multiple concurrent cursors on different tables", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`CREATE TABLE tbl_a (id INT PRIMARY KEY, data TEXT)`);
      await pg.query(`CREATE TABLE tbl_b (id INT PRIMARY KEY, data TEXT)`);
      await pg.query(
        `INSERT INTO tbl_a SELECT g, 'a-' || g FROM generate_series(1, 50) g`,
      );
      await pg.query(
        `INSERT INTO tbl_b SELECT g, 'b-' || g FROM generate_series(1, 50) g`,
      );

      await pg.query("BEGIN");
      await pg.query(
        "DECLARE cur_a CURSOR FOR SELECT id FROM tbl_a ORDER BY id",
      );
      await pg.query(
        "DECLARE cur_b CURSOR FOR SELECT id FROM tbl_b ORDER BY id",
      );

      const idsA: number[] = [];
      const idsB: number[] = [];

      for (let i = 0; i < 10; i++) {
        const ra = await pg.query("FETCH 5 FROM cur_a");
        const rb = await pg.query("FETCH 5 FROM cur_b");
        for (const row of ra.rows) idsA.push(Number(row.id));
        for (const row of rb.rows) idsB.push(Number(row.id));
      }

      await pg.query("CLOSE cur_a");
      await pg.query("CLOSE cur_b");
      await pg.query("COMMIT");

      expect(idsA.length).toBe(50);
      expect(idsB.length).toBe(50);
      for (let i = 0; i < 50; i++) {
        expect(idsA[i]).toBe(i + 1);
        expect(idsB[i]).toBe(i + 1);
      }
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 3: Scrollable cursor with backward iteration
// ---------------------------------------------------------------------------

describe("Scrollable cursor with backward iteration", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`CREATE TABLE scroll_data (id INT PRIMARY KEY, val INT)`);
      await pg.query(
        `INSERT INTO scroll_data SELECT g, g * 10
         FROM generate_series(1, 60) g`,
      );

      await pg.query("BEGIN");
      await pg.query(
        "DECLARE scroll_cur SCROLL CURSOR FOR SELECT id, val FROM scroll_data ORDER BY id",
      );

      const fwd = await pg.query("FETCH 30 FROM scroll_cur");
      expect(fwd.rows.length).toBe(30);
      expect(Number(fwd.rows[29].id)).toBe(30);

      const bwd = await pg.query("FETCH BACKWARD 15 FROM scroll_cur");
      expect(bwd.rows.length).toBe(15);
      expect(Number(bwd.rows[0].id)).toBe(29);
      expect(Number(bwd.rows[14].id)).toBe(15);

      const fwd2 = await pg.query("FETCH 20 FROM scroll_cur");
      expect(fwd2.rows.length).toBe(20);
      expect(Number(fwd2.rows[0].id)).toBe(16);

      await pg.query("CLOSE scroll_cur");
      await pg.query("COMMIT");
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 4: WITH HOLD cursor across transaction boundaries
// ---------------------------------------------------------------------------

describe("WITH HOLD cursor across transaction boundaries", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`CREATE TABLE hold_data (id INT PRIMARY KEY)`);
      await pg.query(
        `INSERT INTO hold_data SELECT g FROM generate_series(1, 40) g`,
      );

      await pg.query("BEGIN");
      await pg.query(
        "DECLARE hold_cur CURSOR WITH HOLD FOR SELECT id FROM hold_data ORDER BY id",
      );

      const batch1 = await pg.query("FETCH 20 FROM hold_cur");
      expect(batch1.rows.length).toBe(20);

      await pg.query("COMMIT");

      const batch2 = await pg.query("FETCH 20 FROM hold_cur");
      expect(batch2.rows.length).toBe(20);
      expect(Number(batch2.rows[0].id)).toBe(21);
      expect(Number(batch2.rows[19].id)).toBe(40);

      await pg.query("CLOSE hold_cur");
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 5: Cursor + concurrent DML (MVCC snapshot isolation)
// ---------------------------------------------------------------------------

describe("Cursor sees stable snapshot despite concurrent DML", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`CREATE TABLE snap_data (id INT PRIMARY KEY, val TEXT)`);
      await pg.query(
        `INSERT INTO snap_data SELECT g, 'original-' || g
         FROM generate_series(1, 40) g`,
      );

      await pg.query("BEGIN ISOLATION LEVEL REPEATABLE READ");
      await pg.query(
        "DECLARE snap_cur CURSOR FOR SELECT id, val FROM snap_data ORDER BY id",
      );

      const batch1 = await pg.query("FETCH 20 FROM snap_cur");
      expect(batch1.rows.length).toBe(20);
      for (const row of batch1.rows) {
        expect(row.val).toMatch(/^original-/);
      }

      await pg.query("SAVEPOINT sp1");
      await pg.query(
        `UPDATE snap_data SET val = 'modified-' || id WHERE id > 20`,
      );
      await pg.query(
        `INSERT INTO snap_data SELECT g, 'new-' || g
         FROM generate_series(41, 50) g`,
      );

      const batch2 = await pg.query("FETCH 20 FROM snap_cur");
      expect(batch2.rows.length).toBe(20);
      for (const row of batch2.rows) {
        expect(row.val).toMatch(/^original-/);
      }

      await pg.query("ROLLBACK TO sp1");
      await pg.query("CLOSE snap_cur");
      await pg.query("COMMIT");

      const check = await pg.query(
        `SELECT count(*) AS c FROM snap_data WHERE val LIKE 'original-%'`,
      );
      expect(Number(check.rows[0].c)).toBe(40);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 6: MOVE to skip rows then FETCH
// ---------------------------------------------------------------------------

describe("MOVE to skip rows then FETCH", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`CREATE TABLE move_data (id INT PRIMARY KEY, val INT)`);
      await pg.query(
        `INSERT INTO move_data SELECT g, g * 7
         FROM generate_series(1, 80) g`,
      );

      await pg.query("BEGIN");
      await pg.query(
        "DECLARE move_cur CURSOR FOR SELECT id, val FROM move_data ORDER BY id",
      );

      await pg.query("MOVE 25 FROM move_cur");

      const batch = await pg.query("FETCH 10 FROM move_cur");
      expect(batch.rows.length).toBe(10);
      expect(Number(batch.rows[0].id)).toBe(26);
      expect(Number(batch.rows[0].val)).toBe(26 * 7);
      expect(Number(batch.rows[9].id)).toBe(35);

      await pg.query("MOVE 30 FROM move_cur");

      const batch2 = await pg.query("FETCH 10 FROM move_cur");
      expect(batch2.rows.length).toBe(10);
      expect(Number(batch2.rows[0].id)).toBe(66);

      await pg.query("CLOSE move_cur");
      await pg.query("COMMIT");
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 7: Cursor with ORDER BY on unindexed column (sort temp files)
// ---------------------------------------------------------------------------

describe("Cursor with ORDER BY on unindexed column", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(
        `CREATE TABLE unsorted (id INT PRIMARY KEY, sort_key INT, payload TEXT)`,
      );
      await pg.query(
        `INSERT INTO unsorted
         SELECT g, (g * 7919) % 200, repeat('z', 50)
         FROM generate_series(1, 200) g`,
      );

      await pg.query("BEGIN");
      await pg.query(
        "DECLARE sort_cur CURSOR FOR SELECT id, sort_key FROM unsorted ORDER BY sort_key, id",
      );

      let prevSortKey = -1;
      let totalRows = 0;
      for (let batch = 0; batch < 20; batch++) {
        const result = await pg.query("FETCH 10 FROM sort_cur");
        if (result.rows.length === 0) break;
        totalRows += result.rows.length;
        for (const row of result.rows) {
          const sk = Number(row.sort_key);
          expect(sk).toBeGreaterThanOrEqual(prevSortKey);
          prevSortKey = sk;
        }
      }

      expect(totalRows).toBe(200);

      await pg.query("CLOSE sort_cur");
      await pg.query("COMMIT");
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 8: Cursor + interleaved cache-evicting queries
// ---------------------------------------------------------------------------

describe("Cursor survives interleaved cache-evicting queries", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`CREATE TABLE evict_main (id INT PRIMARY KEY, val TEXT)`);
      await pg.query(`CREATE TABLE evict_other (id INT PRIMARY KEY, data TEXT)`);
      await pg.query(
        `INSERT INTO evict_main SELECT g, 'main-' || g
         FROM generate_series(1, 60) g`,
      );
      await pg.query(
        `INSERT INTO evict_other SELECT g, repeat('d', 200)
         FROM generate_series(1, 100) g`,
      );

      await pg.query("BEGIN");
      await pg.query(
        "DECLARE evict_cur CURSOR FOR SELECT id, val FROM evict_main ORDER BY id",
      );

      const allIds: number[] = [];

      for (let round = 0; round < 6; round++) {
        const rows = await pg.query("FETCH 10 FROM evict_cur");
        expect(rows.rows.length).toBe(10);
        for (const row of rows.rows) allIds.push(Number(row.id));

        await pg.query(
          `SELECT count(*) FROM evict_other WHERE data LIKE '%d%'`,
        );
        await pg.query(
          `UPDATE evict_other SET data = repeat('e', 200)
           WHERE id BETWEEN ${round * 10 + 1} AND ${round * 10 + 10}`,
        );
      }

      await pg.query("CLOSE evict_cur");
      await pg.query("COMMIT");

      expect(allIds.length).toBe(60);
      for (let i = 0; i < 60; i++) {
        expect(allIds[i]).toBe(i + 1);
      }
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 9: Cursor persistence across syncfs
// ---------------------------------------------------------------------------

describe("Cursor iteration works across syncfs calls", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`CREATE TABLE sync_data (id INT PRIMARY KEY, val INT)`);
      await pg.query(
        `INSERT INTO sync_data SELECT g, g * 3 FROM generate_series(1, 50) g`,
      );

      await pg.query("BEGIN");
      await pg.query(
        "DECLARE sync_cur CURSOR FOR SELECT id, val FROM sync_data ORDER BY id",
      );

      const batch1 = await pg.query("FETCH 25 FROM sync_cur");
      expect(batch1.rows.length).toBe(25);
      expect(Number(batch1.rows[24].id)).toBe(25);

      await h.syncToFs();

      const batch2 = await pg.query("FETCH 25 FROM sync_cur");
      expect(batch2.rows.length).toBe(25);
      expect(Number(batch2.rows[0].id)).toBe(26);
      expect(Number(batch2.rows[24].id)).toBe(50);

      await pg.query("CLOSE sync_cur");
      await pg.query("COMMIT");
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 10: Rapid open/close cursor cycles
// ---------------------------------------------------------------------------

describe("Rapid open/close cursor cycles", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`CREATE TABLE cycle_data (id INT PRIMARY KEY, val TEXT)`);
      await pg.query(
        `INSERT INTO cycle_data SELECT g, 'v' || g FROM generate_series(1, 30) g`,
      );

      for (let cycle = 0; cycle < 10; cycle++) {
        await pg.query("BEGIN");
        await pg.query(
          `DECLARE cycle_cur CURSOR FOR
           SELECT id, val FROM cycle_data WHERE id > ${cycle * 3} ORDER BY id`,
        );

        const rows = await pg.query("FETCH 5 FROM cycle_cur");
        expect(rows.rows.length).toBeGreaterThan(0);
        const firstId = Number(rows.rows[0].id);
        expect(firstId).toBe(cycle * 3 + 1);

        await pg.query("CLOSE cycle_cur");
        await pg.query("COMMIT");
      }
    });
  }
});
