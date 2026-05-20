/**
 * PGlite + tomefs partitioned table and materialized view stress tests.
 *
 * Partitioned tables create multiple physical heap files for a single
 * logical table — each partition has its own set of Postgres data files
 * (heap, TOAST, indexes). This stresses tomefs's file management and
 * page cache in ways that single-table tests don't:
 *
 *  - Many files compete for cache slots simultaneously
 *  - Partition routing writes to different files per INSERT
 *  - Cross-partition queries scan multiple files sequentially
 *  - Dropping partitions deletes files while others remain active
 *  - Attaching/detaching partitions restructures the file tree
 *
 * Materialized views (CREATE MATERIALIZED VIEW, REFRESH) perform
 * atomic bulk rewrites — the old data is dropped and replaced in a
 * single transaction. This exercises truncate+bulk-write patterns
 * under cache pressure.
 *
 * Both features are heavily used in real Postgres applications.
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
// Scenario 1: Range-partitioned table with inserts routed to partitions
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 1: range-partitioned table with routed inserts",
  async (h) => {
    const { pg } = h;

    await pg.query(`
      CREATE TABLE events (
        id SERIAL,
        event_date DATE NOT NULL,
        payload TEXT NOT NULL,
        PRIMARY KEY (id, event_date)
      ) PARTITION BY RANGE (event_date)
    `);

    await pg.query(`CREATE TABLE events_q1 PARTITION OF events
      FOR VALUES FROM ('2025-01-01') TO ('2025-04-01')`);
    await pg.query(`CREATE TABLE events_q2 PARTITION OF events
      FOR VALUES FROM ('2025-04-01') TO ('2025-07-01')`);
    await pg.query(`CREATE TABLE events_q3 PARTITION OF events
      FOR VALUES FROM ('2025-07-01') TO ('2025-10-01')`);
    await pg.query(`CREATE TABLE events_q4 PARTITION OF events
      FOR VALUES FROM ('2025-10-01') TO ('2026-01-01')`);

    for (let month = 1; month <= 12; month++) {
      const date = `2025-${String(month).padStart(2, "0")}-15`;
      await pg.query(`
        INSERT INTO events (event_date, payload)
        SELECT $1::date, 'event-' || i || '-' || repeat('x', 50)
        FROM generate_series(1, 25) AS s(i)
      `, [date]);
    }

    const total = await pg.query(`SELECT COUNT(*)::int AS n FROM events`);
    expect(total.rows[0].n).toBe(300);

    const q1 = await pg.query(`SELECT COUNT(*)::int AS n FROM events_q1`);
    expect(q1.rows[0].n).toBe(75);

    const q2 = await pg.query(`SELECT COUNT(*)::int AS n FROM events_q2`);
    expect(q2.rows[0].n).toBe(75);

    const q3 = await pg.query(`SELECT COUNT(*)::int AS n FROM events_q3`);
    expect(q3.rows[0].n).toBe(75);

    const q4 = await pg.query(`SELECT COUNT(*)::int AS n FROM events_q4`);
    expect(q4.rows[0].n).toBe(75);
  },
);

// ---------------------------------------------------------------------------
// Scenario 2: Cross-partition queries with partition pruning
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 2: cross-partition query scans multiple files @fast",
  async (h) => {
    const { pg } = h;

    await pg.query(`
      CREATE TABLE metrics (
        ts TIMESTAMPTZ NOT NULL,
        sensor_id INTEGER NOT NULL,
        value REAL NOT NULL
      ) PARTITION BY RANGE (ts)
    `);

    await pg.query(`CREATE TABLE metrics_jan PARTITION OF metrics
      FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')`);
    await pg.query(`CREATE TABLE metrics_feb PARTITION OF metrics
      FOR VALUES FROM ('2025-02-01') TO ('2025-03-01')`);
    await pg.query(`CREATE TABLE metrics_mar PARTITION OF metrics
      FOR VALUES FROM ('2025-03-01') TO ('2025-04-01')`);

    for (const [month, table] of [["01", "jan"], ["02", "feb"], ["03", "mar"]] as const) {
      await pg.query(`
        INSERT INTO metrics (ts, sensor_id, value)
        SELECT
          '2025-${month}-01'::timestamptz + (i || ' hours')::interval,
          (i % 10) + 1,
          (i * 3.7 + 0.1)::real
        FROM generate_series(1, 100) AS s(i)
      `);
    }

    const total = await pg.query(`SELECT COUNT(*)::int AS n FROM metrics`);
    expect(total.rows[0].n).toBe(300);

    const pruned = await pg.query(`
      SELECT COUNT(*)::int AS n FROM metrics
      WHERE ts >= '2025-02-01' AND ts < '2025-03-01'
    `);
    expect(pruned.rows[0].n).toBe(100);

    const crossPart = await pg.query(`
      SELECT sensor_id, AVG(value)::real AS avg_val
      FROM metrics
      GROUP BY sensor_id
      ORDER BY sensor_id
    `);
    expect(crossPart.rows.length).toBe(10);
    for (const row of crossPart.rows) {
      expect(row.avg_val).toBeGreaterThan(0);
    }
  },
);

// ---------------------------------------------------------------------------
// Scenario 3: List-partitioned table
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 3: list-partitioned table with category routing",
  async (h) => {
    const { pg } = h;

    await pg.query(`
      CREATE TABLE orders (
        id SERIAL,
        region TEXT NOT NULL,
        amount NUMERIC(10,2) NOT NULL,
        PRIMARY KEY (id, region)
      ) PARTITION BY LIST (region)
    `);

    await pg.query(`CREATE TABLE orders_us PARTITION OF orders FOR VALUES IN ('US')`);
    await pg.query(`CREATE TABLE orders_eu PARTITION OF orders FOR VALUES IN ('EU')`);
    await pg.query(`CREATE TABLE orders_ap PARTITION OF orders FOR VALUES IN ('APAC')`);

    const regions = ["US", "EU", "APAC"];
    for (const region of regions) {
      await pg.query(`
        INSERT INTO orders (region, amount)
        SELECT $1, (i * 9.99 + 0.01)::numeric(10,2)
        FROM generate_series(1, 80) AS s(i)
      `, [region]);
    }

    const total = await pg.query(`SELECT COUNT(*)::int AS n FROM orders`);
    expect(total.rows[0].n).toBe(240);

    for (const region of regions) {
      const count = await pg.query(
        `SELECT COUNT(*)::int AS n FROM orders WHERE region = $1`,
        [region],
      );
      expect(count.rows[0].n).toBe(80);
    }

    const sums = await pg.query(`
      SELECT region, SUM(amount)::float AS total
      FROM orders GROUP BY region ORDER BY region
    `);
    expect(sums.rows.length).toBe(3);
    for (const row of sums.rows) {
      expect(row.total).toBeGreaterThan(0);
    }
  },
);

// ---------------------------------------------------------------------------
// Scenario 4: Drop and recreate partitions
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 4: drop partition deletes files, data survives in others",
  async (h) => {
    const { pg } = h;

    await pg.query(`
      CREATE TABLE logs (
        id SERIAL,
        log_month INTEGER NOT NULL,
        message TEXT NOT NULL,
        PRIMARY KEY (id, log_month)
      ) PARTITION BY LIST (log_month)
    `);

    for (let m = 1; m <= 4; m++) {
      await pg.query(`CREATE TABLE logs_m${m} PARTITION OF logs FOR VALUES IN (${m})`);
      await pg.query(`
        INSERT INTO logs (log_month, message)
        SELECT ${m}, 'log-' || ${m} || '-' || i || '-' || repeat('l', 30)
        FROM generate_series(1, 50) AS s(i)
      `);
    }

    const before = await pg.query(`SELECT COUNT(*)::int AS n FROM logs`);
    expect(before.rows[0].n).toBe(200);

    await pg.query(`DROP TABLE logs_m2`);

    const after = await pg.query(`SELECT COUNT(*)::int AS n FROM logs`);
    expect(after.rows[0].n).toBe(150);

    const m1 = await pg.query(`SELECT COUNT(*)::int AS n FROM logs WHERE log_month = 1`);
    expect(m1.rows[0].n).toBe(50);

    const m3 = await pg.query(`SELECT COUNT(*)::int AS n FROM logs WHERE log_month = 3`);
    expect(m3.rows[0].n).toBe(50);

    await pg.query(`CREATE TABLE logs_m5 PARTITION OF logs FOR VALUES IN (5)`);
    await pg.query(`
      INSERT INTO logs (log_month, message)
      SELECT 5, 'new-log-' || i FROM generate_series(1, 30) AS s(i)
    `);

    const final = await pg.query(`SELECT COUNT(*)::int AS n FROM logs`);
    expect(final.rows[0].n).toBe(180);
  },
);

// ---------------------------------------------------------------------------
// Scenario 5: Materialized view creation and refresh
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 5: materialized view create and refresh @fast",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE sales (
      id SERIAL PRIMARY KEY,
      product TEXT NOT NULL,
      quantity INTEGER NOT NULL,
      price NUMERIC(10,2) NOT NULL
    )`);

    await pg.query(`
      INSERT INTO sales (product, quantity, price)
      SELECT
        'product-' || ((i % 20) + 1),
        (i % 50) + 1,
        ((i * 7 + 3) % 1000)::numeric(10,2) / 10
      FROM generate_series(1, 200) AS s(i)
    `);

    await pg.query(`
      CREATE MATERIALIZED VIEW sales_summary AS
      SELECT
        product,
        COUNT(*)::int AS num_sales,
        SUM(quantity)::int AS total_qty,
        SUM(quantity * price)::float AS revenue
      FROM sales
      GROUP BY product
    `);

    const mv = await pg.query(`
      SELECT COUNT(*)::int AS n FROM sales_summary
    `);
    expect(mv.rows[0].n).toBe(20);

    const topProduct = await pg.query(`
      SELECT product, revenue FROM sales_summary ORDER BY revenue DESC LIMIT 1
    `);
    expect(topProduct.rows[0].revenue).toBeGreaterThan(0);

    await pg.query(`
      INSERT INTO sales (product, quantity, price)
      SELECT 'product-1', 100, 99.99
      FROM generate_series(1, 10) AS s(i)
    `);

    await pg.query(`REFRESH MATERIALIZED VIEW sales_summary`);

    const refreshed = await pg.query(`
      SELECT total_qty::int AS qty FROM sales_summary WHERE product = 'product-1'
    `);
    expect(refreshed.rows[0].qty).toBeGreaterThan(100);
  },
);

// ---------------------------------------------------------------------------
// Scenario 6: Materialized view with index
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 6: indexed materialized view with concurrent refresh",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE inventory (
      id SERIAL PRIMARY KEY,
      warehouse TEXT NOT NULL,
      sku TEXT NOT NULL,
      qty INTEGER NOT NULL
    )`);

    await pg.query(`
      INSERT INTO inventory (warehouse, sku, qty)
      SELECT
        'wh-' || ((i % 5) + 1),
        'SKU-' || lpad((i % 100 + 1)::text, 5, '0'),
        (i * 3 + 7) % 200
      FROM generate_series(1, 300) AS s(i)
    `);

    await pg.query(`
      CREATE MATERIALIZED VIEW inventory_levels AS
      SELECT
        warehouse,
        sku,
        SUM(qty)::int AS total_qty
      FROM inventory
      GROUP BY warehouse, sku
    `);

    await pg.query(`CREATE UNIQUE INDEX idx_inv_levels ON inventory_levels (warehouse, sku)`);

    const count = await pg.query(`SELECT COUNT(*)::int AS n FROM inventory_levels`);
    expect(count.rows[0].n).toBeGreaterThan(0);

    const lookup = await pg.query(`
      SELECT total_qty FROM inventory_levels WHERE warehouse = 'wh-1' AND sku = 'SKU-00001'
    `);
    expect(lookup.rows.length).toBe(1);
    expect(lookup.rows[0].total_qty).toBeGreaterThan(0);

    await pg.query(`
      INSERT INTO inventory (warehouse, sku, qty) VALUES ('wh-1', 'SKU-00001', 500)
    `);

    await pg.query(`REFRESH MATERIALIZED VIEW CONCURRENTLY inventory_levels`);

    const after = await pg.query(`
      SELECT total_qty FROM inventory_levels WHERE warehouse = 'wh-1' AND sku = 'SKU-00001'
    `);
    expect(after.rows[0].total_qty).toBeGreaterThanOrEqual(500);
  },
);

// ---------------------------------------------------------------------------
// Scenario 7: Partitioned table with indexes per partition
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 7: partition-local indexes stress file management",
  async (h) => {
    const { pg } = h;

    await pg.query(`
      CREATE TABLE transactions (
        id SERIAL,
        account_id INTEGER NOT NULL,
        tx_type TEXT NOT NULL,
        amount NUMERIC(12,2) NOT NULL,
        PRIMARY KEY (id, tx_type)
      ) PARTITION BY LIST (tx_type)
    `);

    await pg.query(`CREATE TABLE tx_credit PARTITION OF transactions FOR VALUES IN ('credit')`);
    await pg.query(`CREATE TABLE tx_debit PARTITION OF transactions FOR VALUES IN ('debit')`);
    await pg.query(`CREATE TABLE tx_transfer PARTITION OF transactions FOR VALUES IN ('transfer')`);

    await pg.query(`CREATE INDEX idx_tx_credit_acct ON tx_credit (account_id)`);
    await pg.query(`CREATE INDEX idx_tx_debit_acct ON tx_debit (account_id)`);
    await pg.query(`CREATE INDEX idx_tx_transfer_acct ON tx_transfer (account_id)`);

    const types = ["credit", "debit", "transfer"];
    for (const txType of types) {
      await pg.query(`
        INSERT INTO transactions (account_id, tx_type, amount)
        SELECT
          (i % 20) + 1,
          $1,
          ((i * 13 + 7) % 10000)::numeric(12,2) / 100
        FROM generate_series(1, 100) AS s(i)
      `, [txType]);
    }

    const total = await pg.query(`SELECT COUNT(*)::int AS n FROM transactions`);
    expect(total.rows[0].n).toBe(300);

    const acctBalance = await pg.query(`
      SELECT account_id,
        SUM(CASE WHEN tx_type = 'credit' THEN amount ELSE 0 END)::float AS credits,
        SUM(CASE WHEN tx_type = 'debit' THEN amount ELSE 0 END)::float AS debits
      FROM transactions
      WHERE account_id = 1
      GROUP BY account_id
    `);
    expect(acctBalance.rows.length).toBe(1);
    expect(acctBalance.rows[0].credits).toBeGreaterThan(0);
    expect(acctBalance.rows[0].debits).toBeGreaterThan(0);

    const indexScan = await pg.query(`
      SELECT COUNT(*)::int AS n FROM tx_credit WHERE account_id = 5
    `);
    expect(indexScan.rows[0].n).toBe(5);
  },
);

// ---------------------------------------------------------------------------
// Scenario 8: Partition persistence across remount
// ---------------------------------------------------------------------------

describe("Scenario 8: partitioned table persists across remount", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];
    it(`cache=${size} (${pages} pages) @fast`, async () => {
      harness = await createPGliteHarness(size);
      const { pg, backend } = harness;

      await pg.query(`
        CREATE TABLE persist_part (
          id SERIAL,
          bucket INTEGER NOT NULL,
          data TEXT NOT NULL,
          PRIMARY KEY (id, bucket)
        ) PARTITION BY LIST (bucket)
      `);

      for (let b = 1; b <= 3; b++) {
        await pg.query(`CREATE TABLE persist_part_${b} PARTITION OF persist_part FOR VALUES IN (${b})`);
        await pg.query(`
          INSERT INTO persist_part (bucket, data)
          SELECT ${b}, 'bucket-${b}-row-' || i || '-' || repeat('p', 30)
          FROM generate_series(1, 40) AS s(i)
        `);
      }

      await harness.syncToFs();
      await harness.destroy();
      harness = null;

      harness = await createPGliteHarness({ cacheSize: size, backend });
      const pg2 = harness.pg;

      const total = await pg2.query(`SELECT COUNT(*)::int AS n FROM persist_part`);
      expect(total.rows[0].n).toBe(120);

      for (let b = 1; b <= 3; b++) {
        const count = await pg2.query(
          `SELECT COUNT(*)::int AS n FROM persist_part WHERE bucket = $1`,
          [b],
        );
        expect(count.rows[0].n).toBe(40);
      }
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 9: Materialized view persistence across remount
// ---------------------------------------------------------------------------

describe("Scenario 9: materialized view persists across remount", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];
    it(`cache=${size} (${pages} pages)`, async () => {
      harness = await createPGliteHarness(size);
      const { pg, backend } = harness;

      await pg.query(`CREATE TABLE mv_source (
        id SERIAL PRIMARY KEY,
        category INTEGER NOT NULL,
        value REAL NOT NULL
      )`);

      await pg.query(`
        INSERT INTO mv_source (category, value)
        SELECT (i % 5) + 1, (i * 2.5 + 0.1)::real
        FROM generate_series(1, 150) AS s(i)
      `);

      await pg.query(`
        CREATE MATERIALIZED VIEW mv_agg AS
        SELECT category, COUNT(*)::int AS cnt, AVG(value)::real AS avg_val
        FROM mv_source GROUP BY category
      `);

      await harness.syncToFs();
      await harness.destroy();
      harness = null;

      harness = await createPGliteHarness({ cacheSize: size, backend });
      const pg2 = harness.pg;

      const mv = await pg2.query(`SELECT COUNT(*)::int AS n FROM mv_agg`);
      expect(mv.rows[0].n).toBe(5);

      const check = await pg2.query(`
        SELECT category, cnt, avg_val FROM mv_agg ORDER BY category
      `);
      for (const row of check.rows) {
        expect(row.cnt).toBe(30);
        expect(row.avg_val).toBeGreaterThan(0);
      }
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 10: Heavy partition churn (create, populate, drop, repeat)
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 10: partition churn stresses file create/delete",
  async (h) => {
    const { pg } = h;

    await pg.query(`
      CREATE TABLE churn (
        id SERIAL,
        batch INTEGER NOT NULL,
        data TEXT NOT NULL,
        PRIMARY KEY (id, batch)
      ) PARTITION BY LIST (batch)
    `);

    for (let cycle = 1; cycle <= 5; cycle++) {
      await pg.query(
        `CREATE TABLE churn_b${cycle} PARTITION OF churn FOR VALUES IN (${cycle})`,
      );
      await pg.query(`
        INSERT INTO churn (batch, data)
        SELECT ${cycle}, 'cycle-${cycle}-' || i || '-' || repeat('c', 20)
        FROM generate_series(1, 30) AS s(i)
      `);

      if (cycle >= 3) {
        const dropBatch = cycle - 2;
        await pg.query(`DROP TABLE churn_b${dropBatch}`);
      }
    }

    const remaining = await pg.query(`SELECT COUNT(*)::int AS n FROM churn`);
    expect(remaining.rows[0].n).toBe(60);

    const batches = await pg.query(`
      SELECT DISTINCT batch FROM churn ORDER BY batch
    `);
    expect(batches.rows.map((r: any) => r.batch)).toEqual([4, 5]);
  },
);
