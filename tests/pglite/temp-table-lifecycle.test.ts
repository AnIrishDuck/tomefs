/**
 * PGlite + tomefs temporary table lifecycle tests.
 *
 * Validates that temp table operations (create, use, drop) don't interfere
 * with regular table persistence, even under cache pressure. Temp tables
 * are session-scoped — they create relation files that are unlinked on
 * session close. This exercises the interaction between:
 *   - File creation (regular + temp relation files)
 *   - Concurrent page cache usage across many files
 *   - Unlink/cleanup of temp files during closeFs
 *   - Persistence correctness of regular tables after temp table churn
 *
 * Postgres stores temp table data in the same pg_tblspc/pgdata directory
 * structure. Under cache pressure, temp table pages compete with regular
 * table pages for cache slots — eviction of regular table pages during
 * heavy temp table usage must not cause data loss.
 *
 * Ethos §8: "simulate real PGlite access patterns"
 * Ethos §6: "performance parity" — temp table ops shouldn't degrade persistence
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
    try {
      await h.destroy();
    } catch (_e) {
      // May already be destroyed
    }
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

describe("PGlite + tomefs temp table lifecycle", () => {
  it("regular table persists after temp table create and drop @fast", async () => {
    const backend = new SyncMemoryBackend();

    const h1 = await create("large", backend);
    await h1.pg.query(
      `CREATE TABLE persistent (id SERIAL PRIMARY KEY, value TEXT)`,
    );
    await h1.pg.query(
      `INSERT INTO persistent (value) VALUES ('survives'), ('session')`,
    );

    await h1.pg.query(
      `CREATE TEMP TABLE scratch (id SERIAL PRIMARY KEY, data TEXT)`,
    );
    await h1.pg.query(
      `INSERT INTO scratch (data) VALUES ('temporary'), ('gone')`,
    );

    const tempResult = await h1.pg.query(`SELECT data FROM scratch ORDER BY id`);
    expect(tempResult.rows).toHaveLength(2);

    await h1.pg.query(`DROP TABLE scratch`);
    await h1.syncToFs();
    await h1.destroy();

    const h2 = await create("large", backend);
    const result = await h2.pg.query(
      `SELECT value FROM persistent ORDER BY id`,
    );
    expect(result.rows).toEqual([
      { value: "survives" },
      { value: "session" },
    ]);

    const tables = await h2.pg.query(
      `SELECT tablename FROM pg_tables WHERE schemaname = 'pg_temp_1'`,
    );
    expect(tables.rows).toHaveLength(0);
  });

  it("temp table implicit drop on session close preserves regular data @fast", async () => {
    const backend = new SyncMemoryBackend();

    const h1 = await create("large", backend);
    await h1.pg.query(
      `CREATE TABLE durable (id INT PRIMARY KEY, msg TEXT)`,
    );
    await h1.pg.query(
      `INSERT INTO durable VALUES (1, 'hello'), (2, 'world')`,
    );

    await h1.pg.query(
      `CREATE TEMP TABLE ephemeral (x INT)`,
    );
    await h1.pg.query(`INSERT INTO ephemeral VALUES (99), (100)`);

    await h1.syncToFs();
    await h1.destroy();

    const h2 = await create("large", backend);
    const result = await h2.pg.query(`SELECT msg FROM durable ORDER BY id`);
    expect(result.rows).toEqual([{ msg: "hello" }, { msg: "world" }]);
  });

  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`temp table churn under cache pressure (${size}=${pages} pages)`, async () => {
      const backend = new SyncMemoryBackend();

      const h1 = await create(size, backend);
      await h1.pg.query(
        `CREATE TABLE anchor (id SERIAL PRIMARY KEY, payload TEXT)`,
      );

      const rows = 20;
      const values = Array.from(
        { length: rows },
        (_, i) => `('row_${i}_${"x".repeat(50)}')`,
      ).join(",");
      await h1.pg.query(`INSERT INTO anchor (payload) VALUES ${values}`);

      for (let cycle = 0; cycle < 3; cycle++) {
        await h1.pg.query(
          `CREATE TEMP TABLE IF NOT EXISTS tmp_${cycle} (id SERIAL, data TEXT)`,
        );
        const tmpValues = Array.from(
          { length: 10 },
          (_, i) => `('tmp_${cycle}_${i}_${"y".repeat(30)}')`,
        ).join(",");
        await h1.pg.query(
          `INSERT INTO tmp_${cycle} (data) VALUES ${tmpValues}`,
        );

        const tmpCount = await h1.pg.query(
          `SELECT COUNT(*) as cnt FROM tmp_${cycle}`,
        );
        expect(Number(tmpCount.rows[0].cnt)).toBe(10);

        await h1.pg.query(`DROP TABLE tmp_${cycle}`);
      }

      await h1.syncToFs();
      await h1.destroy();

      const h2 = await create(size, backend);
      const result = await h2.pg.query(
        `SELECT COUNT(*) as cnt FROM anchor`,
      );
      expect(Number(result.rows[0].cnt)).toBe(rows);

      const sample = await h2.pg.query(
        `SELECT payload FROM anchor WHERE id = 1`,
      );
      expect(sample.rows[0].payload).toBe(`row_0_${"x".repeat(50)}`);
    });
  }

  it("transaction spanning regular and temp tables persists correctly @fast", async () => {
    const backend = new SyncMemoryBackend();

    const h1 = await create("medium", backend);
    await h1.pg.query(`CREATE TABLE ledger (id SERIAL PRIMARY KEY, amount INT)`);
    await h1.pg.query(`CREATE TEMP TABLE staging (amount INT)`);

    await h1.pg.query(`BEGIN`);
    await h1.pg.query(`INSERT INTO staging VALUES (100), (200), (300)`);
    await h1.pg.query(
      `INSERT INTO ledger (amount) SELECT amount FROM staging`,
    );
    await h1.pg.query(`COMMIT`);

    await h1.pg.query(`DROP TABLE staging`);
    await h1.syncToFs();
    await h1.destroy();

    const h2 = await create("medium", backend);
    const result = await h2.pg.query(
      `SELECT amount FROM ledger ORDER BY id`,
    );
    expect(result.rows).toEqual([
      { amount: 100 },
      { amount: 200 },
      { amount: 300 },
    ]);
  });

  it("temp table with indexes under cache pressure persists regular data", async () => {
    const backend = new SyncMemoryBackend();

    const h1 = await create("small", backend);
    await h1.pg.query(`CREATE TABLE products (id SERIAL PRIMARY KEY, name TEXT, price INT)`);
    await h1.pg.query(
      `INSERT INTO products (name, price) VALUES ('widget', 10), ('gadget', 25), ('gizmo', 15)`,
    );

    await h1.pg.query(`CREATE TEMP TABLE temp_prices (name TEXT, new_price INT)`);
    await h1.pg.query(`CREATE INDEX ON temp_prices (name)`);
    await h1.pg.query(
      `INSERT INTO temp_prices VALUES ('widget', 12), ('gadget', 30)`,
    );

    await h1.pg.query(`
      UPDATE products SET price = tp.new_price
      FROM temp_prices tp WHERE products.name = tp.name
    `);

    await h1.pg.query(`DROP TABLE temp_prices`);
    await h1.syncToFs();
    await h1.destroy();

    const h2 = await create("small", backend);
    const result = await h2.pg.query(
      `SELECT name, price FROM products ORDER BY name`,
    );
    expect(result.rows).toEqual([
      { name: "gadget", price: 30 },
      { name: "gizmo", price: 15 },
      { name: "widget", price: 12 },
    ]);
  });

  it("multiple temp table create/drop cycles within single session @fast", async () => {
    const backend = new SyncMemoryBackend();

    const h1 = await create("medium", backend);
    await h1.pg.query(`CREATE TABLE accumulator (total INT DEFAULT 0)`);
    await h1.pg.query(`INSERT INTO accumulator VALUES (0)`);

    for (let i = 1; i <= 5; i++) {
      await h1.pg.query(`CREATE TEMP TABLE batch (val INT)`);
      const batchValues = Array.from({ length: i * 2 }, (_, j) => `(${j + 1})`).join(",");
      await h1.pg.query(`INSERT INTO batch VALUES ${batchValues}`);
      await h1.pg.query(
        `UPDATE accumulator SET total = total + (SELECT SUM(val) FROM batch)`,
      );
      await h1.pg.query(`DROP TABLE batch`);
    }

    await h1.syncToFs();
    await h1.destroy();

    const h2 = await create("medium", backend);
    const result = await h2.pg.query(`SELECT total FROM accumulator`);
    // Sum of 1..2 + 1..4 + 1..6 + 1..8 + 1..10 = 3 + 10 + 21 + 36 + 55 = 125
    expect(result.rows[0].total).toBe(125);
  });
});
