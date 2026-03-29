/**
 * PGlite + tomefs persistence under cache pressure.
 *
 * Validates that PGlite data survives syncToFs + remount cycles at every
 * cache pressure level. This fills the gap between:
 *   - persistence.test.ts (remount, but only "large" cache)
 *   - cache-pressure.test.ts (all cache sizes, but no remount)
 *
 * Under tiny cache (4 pages = 32 KB), Postgres page writes cause constant
 * eviction of dirty pages mid-operation. Those evicted pages are flushed
 * to the backend, then on remount they must be reloaded correctly. This
 * is the most dangerous scenario for data corruption: dirty pages from
 * a rolled-back transaction can be flushed to the backend, and Postgres
 * must recover via WAL replay on remount.
 *
 * Ethos §8: "record or simulate real PGlite access patterns"
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

/** Create a harness and track it for cleanup. */
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

/** Cache sizes to test. Tiny/small force eviction; large is baseline. */
const PRESSURE_CONFIGS: CacheSize[] = ["tiny", "small", "medium", "large"];

// ---------------------------------------------------------------------------
// Scenario 1: Bulk insert + persist + remount
// ---------------------------------------------------------------------------

describe("Persistence under cache pressure: bulk insert + remount", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const backend = new SyncMemoryBackend();

      // Phase 1: populate
      const h1 = await create(size, backend);
      await h1.pg.query(
        `CREATE TABLE items (id SERIAL PRIMARY KEY, payload TEXT)`,
      );
      for (let i = 0; i < 30; i++) {
        const payload = `item-${i}-${"x".repeat(100 + (i % 5) * 80)}`;
        await h1.pg.query(`INSERT INTO items (payload) VALUES ($1)`, [payload]);
      }

      // Checkpoint: syncfs + destroy
      await h1.syncToFs();
      await h1.destroy();

      // Phase 2: remount on same backend, verify all data
      const h2 = await create(size, backend);
      const result = await h2.pg.query(
        `SELECT id, payload FROM items ORDER BY id`,
      );
      expect(result.rows.length).toBe(30);
      for (let i = 0; i < 30; i++) {
        expect(result.rows[i].id).toBe(i + 1);
        const expectedPayload = `item-${i}-${"x".repeat(100 + (i % 5) * 80)}`;
        expect(result.rows[i].payload).toBe(expectedPayload);
      }
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 2: Transaction rollback + persist + remount
// ---------------------------------------------------------------------------

describe("Persistence under cache pressure: rollback does not persist", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages)`, async () => {
      const backend = new SyncMemoryBackend();

      // Phase 1: seed data, then rollback a change
      const h1 = await create(size, backend);
      await h1.pg.query(
        `CREATE TABLE ledger (id SERIAL PRIMARY KEY, amount INTEGER)`,
      );
      for (let i = 0; i < 20; i++) {
        await h1.pg.query(`INSERT INTO ledger (amount) VALUES (100)`);
      }

      // Committed update
      await h1.pg.query("BEGIN");
      await h1.pg.query(`UPDATE ledger SET amount = amount + 50`);
      await h1.pg.query("COMMIT");

      // Rolled-back update — under tiny cache, dirty pages from this
      // transaction may be evicted and flushed to the backend. Postgres
      // must recover correct state via WAL replay on remount.
      await h1.pg.query("BEGIN");
      await h1.pg.query(`UPDATE ledger SET amount = amount + 9999`);
      await h1.pg.query("ROLLBACK");

      await h1.syncToFs();
      await h1.destroy();

      // Phase 2: remount and verify rollback was not persisted
      const h2 = await create(size, backend);
      const result = await h2.pg.query(
        `SELECT DISTINCT amount FROM ledger`,
      );
      expect(result.rows.length).toBe(1);
      expect(result.rows[0].amount).toBe(150); // 100 + 50, not + 9999
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 3: Multi-table schema + persist + remount under pressure
// ---------------------------------------------------------------------------

describe("Persistence under cache pressure: multi-table schema survives remount", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages)`, async () => {
      const backend = new SyncMemoryBackend();

      // Phase 1: create schema with foreign keys + indexes
      const h1 = await create(size, backend);
      await h1.pg.query(
        `CREATE TABLE departments (id SERIAL PRIMARY KEY, name TEXT NOT NULL)`,
      );
      await h1.pg.query(
        `CREATE TABLE employees (
          id SERIAL PRIMARY KEY,
          dept_id INTEGER REFERENCES departments(id),
          name TEXT NOT NULL,
          salary INTEGER
        )`,
      );
      await h1.pg.query(
        `CREATE INDEX idx_emp_dept ON employees (dept_id)`,
      );
      await h1.pg.query(
        `CREATE INDEX idx_emp_salary ON employees (salary)`,
      );

      // Populate
      for (let d = 0; d < 5; d++) {
        await h1.pg.query(
          `INSERT INTO departments (name) VALUES ($1)`,
          [`dept-${d}`],
        );
      }
      for (let e = 0; e < 40; e++) {
        await h1.pg.query(
          `INSERT INTO employees (dept_id, name, salary) VALUES ($1, $2, $3)`,
          [(e % 5) + 1, `emp-${e}`, 50000 + e * 1000],
        );
      }

      await h1.syncToFs();
      await h1.destroy();

      // Phase 2: remount, verify schema + data + indexes
      const h2 = await create(size, backend);

      // Verify departments
      const depts = await h2.pg.query(
        `SELECT COUNT(*)::int as count FROM departments`,
      );
      expect(depts.rows[0].count).toBe(5);

      // Verify employees via join (exercises index + FK)
      const byDept = await h2.pg.query(`
        SELECT d.name, COUNT(e.id)::int as emp_count
        FROM departments d
        JOIN employees e ON e.dept_id = d.id
        GROUP BY d.id, d.name
        ORDER BY d.id
      `);
      expect(byDept.rows.length).toBe(5);
      for (const row of byDept.rows) {
        expect(row.emp_count).toBe(8);
      }

      // Verify index-based queries work after remount
      const highSalary = await h2.pg.query(
        `SELECT COUNT(*)::int as count FROM employees WHERE salary >= 80000`,
      );
      expect(highSalary.rows[0].count).toBe(10); // emp-30..emp-39
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 4: Update storm + persist + remount
// ---------------------------------------------------------------------------

describe("Persistence under cache pressure: update storm survives remount @fast", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages)`, async () => {
      const backend = new SyncMemoryBackend();

      // Phase 1: heavy in-place updates that thrash the page cache
      const h1 = await create(size, backend);
      await h1.pg.query(
        `CREATE TABLE counters (id SERIAL PRIMARY KEY, value INTEGER DEFAULT 0)`,
      );
      for (let i = 0; i < 10; i++) {
        await h1.pg.query(`INSERT INTO counters (value) VALUES (0)`);
      }

      // 15 rounds of updates across all 10 rows
      for (let round = 0; round < 15; round++) {
        for (let id = 1; id <= 10; id++) {
          await h1.pg.query(
            `UPDATE counters SET value = value + 1 WHERE id = $1`,
            [id],
          );
        }
      }

      await h1.syncToFs();
      await h1.destroy();

      // Phase 2: remount and verify
      const h2 = await create(size, backend);
      const result = await h2.pg.query(
        `SELECT id, value FROM counters ORDER BY id`,
      );
      expect(result.rows.length).toBe(10);
      for (const row of result.rows) {
        expect(row.value).toBe(15);
      }
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 5: Multiple persist+remount cycles under pressure
// ---------------------------------------------------------------------------

describe("Persistence under cache pressure: multiple sync+remount cycles", () => {
  for (const size of ["tiny", "small"] as CacheSize[]) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages)`, async () => {
      const backend = new SyncMemoryBackend();

      // Cycle 1: create + insert
      let h = await create(size, backend);
      await h.pg.query(
        `CREATE TABLE log (id SERIAL PRIMARY KEY, msg TEXT)`,
      );
      for (let i = 0; i < 10; i++) {
        await h.pg.query(`INSERT INTO log (msg) VALUES ($1)`, [`cycle1-${i}`]);
      }
      await h.syncToFs();
      await h.destroy();

      // Cycle 2: more inserts on remounted instance
      h = await create(size, backend);
      harnesses.push(h);
      for (let i = 0; i < 10; i++) {
        await h.pg.query(`INSERT INTO log (msg) VALUES ($1)`, [`cycle2-${i}`]);
      }
      await h.syncToFs();
      await h.destroy();

      // Cycle 3: updates on remounted instance
      h = await create(size, backend);
      harnesses.push(h);
      await h.pg.query(`UPDATE log SET msg = msg || '-updated' WHERE id <= 5`);
      await h.syncToFs();
      await h.destroy();

      // Final verification
      h = await create(size, backend);
      harnesses.push(h);
      const total = await h.pg.query(
        `SELECT COUNT(*)::int as count FROM log`,
      );
      expect(total.rows[0].count).toBe(20);

      // Verify cycle 1 rows (first 5 updated, rest original)
      const updated = await h.pg.query(
        `SELECT msg FROM log WHERE id <= 5 ORDER BY id`,
      );
      for (const row of updated.rows) {
        expect(row.msg).toMatch(/^cycle1-\d+-updated$/);
      }

      const untouched = await h.pg.query(
        `SELECT msg FROM log WHERE id > 5 AND id <= 10 ORDER BY id`,
      );
      for (const row of untouched.rows) {
        expect(row.msg).toMatch(/^cycle1-\d+$/);
      }

      // Verify cycle 2 rows are intact
      const cycle2 = await h.pg.query(
        `SELECT msg FROM log WHERE id > 10 ORDER BY id`,
      );
      expect(cycle2.rows.length).toBe(10);
      for (const row of cycle2.rows) {
        expect(row.msg).toMatch(/^cycle2-\d+$/);
      }
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 6: DDL rollback + persist + remount
// ---------------------------------------------------------------------------

describe("Persistence under cache pressure: DDL rollback does not persist", () => {
  it("cache=tiny (4 pages)", async () => {
    const backend = new SyncMemoryBackend();

    // Phase 1: create base table, then rollback a DDL change
    const h1 = await create("tiny", backend);
    await h1.pg.query(
      `CREATE TABLE config (key TEXT PRIMARY KEY, val TEXT)`,
    );
    await h1.pg.query(
      `INSERT INTO config (key, val) VALUES ('version', '1.0')`,
    );

    // Committed DDL
    await h1.pg.query(
      `CREATE TABLE audit (id SERIAL PRIMARY KEY, action TEXT)`,
    );
    await h1.pg.query(
      `INSERT INTO audit (action) VALUES ('init')`,
    );

    // Rolled-back DDL — table should not exist after remount
    await h1.pg.query("BEGIN");
    await h1.pg.query(
      `CREATE TABLE temp_data (id SERIAL PRIMARY KEY, data TEXT)`,
    );
    await h1.pg.query(
      `INSERT INTO temp_data (data) VALUES ('should not exist')`,
    );
    await h1.pg.query("ROLLBACK");

    await h1.syncToFs();
    await h1.destroy();

    // Phase 2: remount and verify
    const h2 = await create("tiny", backend);

    // Committed data survives
    const config = await h2.pg.query(`SELECT val FROM config WHERE key = 'version'`);
    expect(config.rows[0].val).toBe("1.0");

    const audit = await h2.pg.query(`SELECT action FROM audit`);
    expect(audit.rows[0].action).toBe("init");

    // Rolled-back table does not exist
    const tables = await h2.pg.query(`
      SELECT tablename FROM pg_tables
      WHERE schemaname = 'public'
      ORDER BY tablename
    `);
    const tableNames = tables.rows.map((r: any) => r.tablename);
    expect(tableNames).toContain("config");
    expect(tableNames).toContain("audit");
    expect(tableNames).not.toContain("temp_data");
  });
});
