/**
 * PGlite CTE, window function, and prepared statement stress tests.
 *
 * CTEs materialize intermediate results in temp storage, window functions
 * perform sequential scans with lookups to earlier rows, and recursive CTEs
 * create unbounded temp table growth. Under small page caches, these operations
 * generate heavy eviction pressure from the temp materialization pages competing
 * with heap/index pages for cache slots.
 *
 * Ethos §8: workload scenarios verify end-to-end behavior under realistic use.
 */

import { describe, it, expect, afterEach } from "vitest";
import { createPGliteHarness, type PGliteHarness } from "./harness.js";
import { CACHE_CONFIGS, type CacheSize } from "./harness.js";

let harness: PGliteHarness | null = null;

afterEach(async () => {
  if (harness) {
    await harness.destroy();
    harness = null;
  }
});

const PRESSURE_SIZES: CacheSize[] = ["tiny", "small", "medium", "large"];

function describeWithPressure(
  name: string,
  fn: (pg: any) => Promise<void>,
  sizes: CacheSize[] = PRESSURE_SIZES,
) {
  for (const size of sizes) {
    const pages = CACHE_CONFIGS[size];
    describe(`${name} @fast`, () => {
      it(`cache=${size} (${pages} pages)`, async () => {
        harness = await createPGliteHarness(size);
        await fn(harness.pg);
      });
    });
  }
}

// ---------------------------------------------------------------------------
// CTE scenarios
// ---------------------------------------------------------------------------

describeWithPressure(
  "CTE: simple materialized CTE with aggregation",
  async (pg) => {
    await pg.query(`
      CREATE TABLE orders (
        id SERIAL PRIMARY KEY,
        customer_id INT,
        amount NUMERIC(10,2)
      )
    `);

    for (let i = 0; i < 50; i++) {
      await pg.query(
        `INSERT INTO orders (customer_id, amount) VALUES ($1, $2)`,
        [i % 10, (i * 7.5 + 1.25)],
      );
    }

    const result = await pg.query(`
      WITH customer_totals AS MATERIALIZED (
        SELECT customer_id, SUM(amount) as total, COUNT(*)::int as cnt
        FROM orders
        GROUP BY customer_id
      )
      SELECT customer_id, total, cnt
      FROM customer_totals
      WHERE cnt >= 5
      ORDER BY total DESC
    `);

    expect(result.rows.length).toBe(10);
    for (const row of result.rows) {
      expect(row.cnt).toBe(5);
    }
  },
);

describeWithPressure(
  "CTE: chained CTEs with joins",
  async (pg) => {
    await pg.query(`CREATE TABLE products (id SERIAL PRIMARY KEY, name TEXT, price NUMERIC(10,2))`);
    await pg.query(`CREATE TABLE sales (id SERIAL PRIMARY KEY, product_id INT, qty INT, sold_at DATE)`);

    for (let i = 0; i < 20; i++) {
      await pg.query(
        `INSERT INTO products (name, price) VALUES ($1, $2)`,
        [`product_${i}`, 10 + i * 2.5],
      );
    }
    for (let i = 0; i < 100; i++) {
      await pg.query(
        `INSERT INTO sales (product_id, qty, sold_at) VALUES ($1, $2, $3)`,
        [(i % 20) + 1, 1 + (i % 5), `2024-01-${(i % 28) + 1}`],
      );
    }

    const result = await pg.query(`
      WITH sale_totals AS (
        SELECT product_id, SUM(qty)::int as total_qty
        FROM sales
        GROUP BY product_id
      ),
      ranked AS (
        SELECT p.name, p.price, st.total_qty,
               p.price * st.total_qty as revenue
        FROM products p
        JOIN sale_totals st ON st.product_id = p.id
      )
      SELECT name, total_qty, revenue
      FROM ranked
      ORDER BY revenue DESC
      LIMIT 5
    `);

    expect(result.rows.length).toBe(5);
    for (const row of result.rows) {
      expect(Number(row.total_qty)).toBeGreaterThan(0);
      expect(Number(row.revenue)).toBeGreaterThan(0);
    }
  },
);

describeWithPressure(
  "CTE: recursive CTE generates series",
  async (pg) => {
    const result = await pg.query(`
      WITH RECURSIVE counter(n) AS (
        SELECT 1
        UNION ALL
        SELECT n + 1 FROM counter WHERE n < 100
      )
      SELECT COUNT(*)::int as cnt, SUM(n)::int as total FROM counter
    `);

    expect(result.rows[0].cnt).toBe(100);
    expect(result.rows[0].total).toBe(5050);
  },
);

describeWithPressure(
  "CTE: recursive CTE tree traversal under cache pressure",
  async (pg) => {
    await pg.query(`
      CREATE TABLE categories (
        id SERIAL PRIMARY KEY,
        parent_id INT REFERENCES categories(id),
        name TEXT
      )
    `);

    await pg.query(`INSERT INTO categories (name) VALUES ('root')`);
    let id = 2;
    for (let depth = 0; depth < 4; depth++) {
      const parentStart = depth === 0 ? 1 : id - (3 ** depth);
      for (let j = 0; j < 3 ** (depth + 1); j++) {
        const parentId = parentStart + Math.floor(j / 3);
        await pg.query(
          `INSERT INTO categories (id, parent_id, name) VALUES ($1, $2, $3)`,
          [id, parentId, `cat_${id}`],
        );
        id++;
      }
    }

    const result = await pg.query(`
      WITH RECURSIVE tree AS (
        SELECT id, name, 0 as depth
        FROM categories WHERE parent_id IS NULL
        UNION ALL
        SELECT c.id, c.name, t.depth + 1
        FROM categories c
        JOIN tree t ON c.parent_id = t.id
      )
      SELECT depth, COUNT(*)::int as cnt FROM tree GROUP BY depth ORDER BY depth
    `);

    expect(result.rows.length).toBe(5);
    expect(result.rows[0].cnt).toBe(1);
    expect(result.rows[1].cnt).toBe(3);
    expect(result.rows[2].cnt).toBe(9);
    expect(result.rows[3].cnt).toBe(27);
    expect(result.rows[4].cnt).toBe(81);
  },
);

describeWithPressure(
  "CTE: recursive CTE with accumulation and filtering",
  async (pg) => {
    await pg.query(`
      CREATE TABLE employees (
        id SERIAL PRIMARY KEY,
        manager_id INT,
        name TEXT,
        salary NUMERIC(10,2)
      )
    `);

    await pg.query(`INSERT INTO employees (name, salary) VALUES ('CEO', 200000)`);
    for (let i = 0; i < 30; i++) {
      const managerId = Math.floor(i / 3) + 1;
      await pg.query(
        `INSERT INTO employees (manager_id, name, salary) VALUES ($1, $2, $3)`,
        [managerId, `emp_${i}`, 50000 + i * 1000],
      );
    }

    const result = await pg.query(`
      WITH RECURSIVE org AS (
        SELECT id, name, salary, 0 as depth
        FROM employees WHERE manager_id IS NULL
        UNION ALL
        SELECT e.id, e.name, e.salary, o.depth + 1
        FROM employees e
        JOIN org o ON e.manager_id = o.id
      )
      SELECT depth, COUNT(*)::int as cnt,
             SUM(salary)::numeric as level_salary
      FROM org
      GROUP BY depth
      ORDER BY depth
    `);

    expect(result.rows.length).toBeGreaterThanOrEqual(2);
    expect(result.rows[0].cnt).toBe(1);
  },
);

// ---------------------------------------------------------------------------
// Window function scenarios
// ---------------------------------------------------------------------------

describeWithPressure(
  "Window: ROW_NUMBER and RANK over sorted data",
  async (pg) => {
    await pg.query(`
      CREATE TABLE scores (
        id SERIAL PRIMARY KEY,
        player TEXT,
        game TEXT,
        score INT
      )
    `);

    const games = ["chess", "go", "checkers"];
    for (let i = 0; i < 60; i++) {
      await pg.query(
        `INSERT INTO scores (player, game, score) VALUES ($1, $2, $3)`,
        [`player_${i % 10}`, games[i % 3], (i * 17 + 31) % 100],
      );
    }

    const result = await pg.query(`
      SELECT player, game, score,
             ROW_NUMBER() OVER (PARTITION BY game ORDER BY score DESC) as rn,
             RANK() OVER (PARTITION BY game ORDER BY score DESC) as rnk
      FROM scores
      ORDER BY game, rn
      LIMIT 15
    `);

    expect(result.rows.length).toBe(15);

    for (const game of games) {
      const gameRows = result.rows.filter((r: any) => r.game === game);
      for (let i = 1; i < gameRows.length; i++) {
        expect(gameRows[i].score).toBeLessThanOrEqual(gameRows[i - 1].score);
      }
    }
  },
);

describeWithPressure(
  "Window: running aggregates (SUM, AVG) over partitions",
  async (pg) => {
    await pg.query(`
      CREATE TABLE daily_sales (
        id SERIAL PRIMARY KEY,
        store TEXT,
        sale_date DATE,
        revenue NUMERIC(10,2)
      )
    `);

    const stores = ["north", "south", "east"];
    for (let i = 0; i < 90; i++) {
      await pg.query(
        `INSERT INTO daily_sales (store, sale_date, revenue) VALUES ($1, $2, $3)`,
        [stores[i % 3], `2024-01-${(i % 30) + 1}`, 100 + (i * 13) % 500],
      );
    }

    const result = await pg.query(`
      SELECT store, sale_date, revenue,
             SUM(revenue) OVER (
               PARTITION BY store ORDER BY sale_date
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
             ) as cumulative_revenue,
             AVG(revenue) OVER (
               PARTITION BY store ORDER BY sale_date
               ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
             ) as moving_avg_3
      FROM daily_sales
      ORDER BY store, sale_date
    `);

    expect(result.rows.length).toBe(90);
    const northRows = result.rows.filter((r: any) => r.store === "north");
    let cumulative = 0;
    for (const row of northRows) {
      cumulative += Number(row.revenue);
      expect(Number(row.cumulative_revenue)).toBeCloseTo(cumulative, 1);
    }
  },
);

describeWithPressure(
  "Window: LAG/LEAD for period-over-period comparison",
  async (pg) => {
    await pg.query(`
      CREATE TABLE monthly_metrics (
        id SERIAL PRIMARY KEY,
        metric TEXT,
        month INT,
        value NUMERIC(10,2)
      )
    `);

    for (let m = 1; m <= 12; m++) {
      await pg.query(
        `INSERT INTO monthly_metrics (metric, month, value) VALUES ('revenue', $1, $2)`,
        [m, 1000 + m * 100 + (m % 3) * 50],
      );
      await pg.query(
        `INSERT INTO monthly_metrics (metric, month, value) VALUES ('users', $1, $2)`,
        [m, 500 + m * 30],
      );
    }

    const result = await pg.query(`
      SELECT metric, month, value,
             LAG(value, 1) OVER (PARTITION BY metric ORDER BY month) as prev_value,
             LEAD(value, 1) OVER (PARTITION BY metric ORDER BY month) as next_value,
             value - LAG(value, 1) OVER (PARTITION BY metric ORDER BY month) as delta
      FROM monthly_metrics
      ORDER BY metric, month
    `);

    expect(result.rows.length).toBe(24);
    const revenueRows = result.rows.filter((r: any) => r.metric === "revenue");
    expect(revenueRows[0].prev_value).toBeNull();
    expect(revenueRows[0].delta).toBeNull();
    for (let i = 1; i < revenueRows.length; i++) {
      expect(Number(revenueRows[i].prev_value)).toBe(
        Number(revenueRows[i - 1].value),
      );
    }
  },
);

describeWithPressure(
  "Window: NTILE distribution and percentile calculation",
  async (pg) => {
    await pg.query(`
      CREATE TABLE measurements (
        id SERIAL PRIMARY KEY,
        sensor TEXT,
        reading NUMERIC(10,4)
      )
    `);

    for (let i = 0; i < 100; i++) {
      await pg.query(
        `INSERT INTO measurements (sensor, reading) VALUES ($1, $2)`,
        [`sensor_${i % 5}`, Math.sin(i * 0.1) * 50 + 50],
      );
    }

    const result = await pg.query(`
      SELECT sensor, reading,
             NTILE(4) OVER (PARTITION BY sensor ORDER BY reading) as quartile,
             PERCENT_RANK() OVER (PARTITION BY sensor ORDER BY reading) as pct_rank
      FROM measurements
      ORDER BY sensor, reading
    `);

    expect(result.rows.length).toBe(100);
    for (const row of result.rows) {
      expect(Number(row.quartile)).toBeGreaterThanOrEqual(1);
      expect(Number(row.quartile)).toBeLessThanOrEqual(4);
      expect(Number(row.pct_rank)).toBeGreaterThanOrEqual(0);
      expect(Number(row.pct_rank)).toBeLessThanOrEqual(1);
    }
  },
);

// ---------------------------------------------------------------------------
// Combined CTE + window function scenarios
// ---------------------------------------------------------------------------

describeWithPressure(
  "CTE + Window: materialized CTE feeds window function",
  async (pg) => {
    await pg.query(`
      CREATE TABLE transactions (
        id SERIAL PRIMARY KEY,
        account_id INT,
        amount NUMERIC(10,2),
        txn_date DATE
      )
    `);

    for (let i = 0; i < 80; i++) {
      await pg.query(
        `INSERT INTO transactions (account_id, amount, txn_date) VALUES ($1, $2, $3)`,
        [
          i % 8,
          ((i * 31 + 7) % 200) - 50,
          `2024-${String(Math.floor(i / 10) + 1).padStart(2, "0")}-${String((i % 28) + 1).padStart(2, "0")}`,
        ],
      );
    }

    const result = await pg.query(`
      WITH monthly AS MATERIALIZED (
        SELECT account_id,
               DATE_TRUNC('month', txn_date) as month,
               SUM(amount) as monthly_total
        FROM transactions
        GROUP BY account_id, DATE_TRUNC('month', txn_date)
      )
      SELECT account_id, month, monthly_total,
             SUM(monthly_total) OVER (
               PARTITION BY account_id ORDER BY month
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
             ) as running_balance,
             RANK() OVER (
               PARTITION BY account_id ORDER BY monthly_total DESC
             ) as best_month_rank
      FROM monthly
      ORDER BY account_id, month
    `);

    expect(result.rows.length).toBeGreaterThan(0);
    const acct0 = result.rows.filter((r: any) => r.account_id === 0);
    if (acct0.length > 1) {
      let running = 0;
      for (const row of acct0) {
        running += Number(row.monthly_total);
        expect(Number(row.running_balance)).toBeCloseTo(running, 1);
      }
    }
  },
);

describeWithPressure(
  "CTE + Window: recursive hierarchy with window ranking",
  async (pg) => {
    await pg.query(`
      CREATE TABLE departments (
        id SERIAL PRIMARY KEY,
        parent_id INT,
        name TEXT,
        budget NUMERIC(12,2)
      )
    `);

    await pg.query(`INSERT INTO departments (name, budget) VALUES ('Company', 1000000)`);
    const depts = ["Engineering", "Sales", "Support", "Marketing"];
    for (let i = 0; i < depts.length; i++) {
      await pg.query(
        `INSERT INTO departments (parent_id, name, budget) VALUES (1, $1, $2)`,
        [depts[i], 200000 + i * 50000],
      );
    }
    for (let i = 0; i < 12; i++) {
      await pg.query(
        `INSERT INTO departments (parent_id, name, budget) VALUES ($1, $2, $3)`,
        [(i % 4) + 2, `team_${i}`, 30000 + i * 5000],
      );
    }

    const result = await pg.query(`
      WITH RECURSIVE dept_tree AS (
        SELECT id, parent_id, name, budget, 0 as depth
        FROM departments WHERE parent_id IS NULL
        UNION ALL
        SELECT d.id, d.parent_id, d.name, d.budget, dt.depth + 1
        FROM departments d
        JOIN dept_tree dt ON d.parent_id = dt.id
      )
      SELECT name, depth, budget,
             RANK() OVER (PARTITION BY depth ORDER BY budget DESC) as budget_rank,
             SUM(budget) OVER (PARTITION BY depth) as level_total_budget
      FROM dept_tree
      ORDER BY depth, budget DESC
    `);

    expect(result.rows.length).toBe(17);
    expect(result.rows[0].depth).toBe(0);
    expect(Number(result.rows[0].budget_rank)).toBe(1);
  },
);

// ---------------------------------------------------------------------------
// Prepared statement scenarios
// ---------------------------------------------------------------------------

describeWithPressure(
  "Prepared: repeated parameterized queries stress page cache",
  async (pg) => {
    await pg.query(`
      CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT)
    `);
    await pg.query(`CREATE INDEX idx_kv_value ON kv (value)`);

    for (let i = 0; i < 50; i++) {
      await pg.query(`INSERT INTO kv (key, value) VALUES ($1, $2)`, [
        `key_${i}`,
        `value_${i % 10}`,
      ]);
    }

    for (let round = 0; round < 3; round++) {
      for (let i = 0; i < 50; i++) {
        const k = (i * 7 + round * 13) % 50;
        const result = await pg.query(
          `SELECT value FROM kv WHERE key = $1`,
          [`key_${k}`],
        );
        expect(result.rows[0].value).toBe(`value_${k % 10}`);
      }
    }
  },
);

describeWithPressure(
  "Prepared: parameterized INSERT + SELECT interleave",
  async (pg) => {
    await pg.query(`
      CREATE TABLE events (
        id SERIAL PRIMARY KEY,
        type TEXT,
        payload JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);
    await pg.query(`CREATE INDEX idx_events_type ON events (type)`);

    const types = ["click", "view", "purchase", "signup", "logout"];
    for (let i = 0; i < 100; i++) {
      const t = types[i % types.length];
      await pg.query(
        `INSERT INTO events (type, payload) VALUES ($1, $2)`,
        [t, JSON.stringify({ seq: i, data: `item_${i}` })],
      );

      if (i > 0 && i % 20 === 0) {
        const counts = await pg.query(`
          SELECT type, COUNT(*)::int as cnt
          FROM events
          GROUP BY type
          ORDER BY cnt DESC
        `);
        expect(counts.rows.length).toBeGreaterThan(0);
        const total = counts.rows.reduce(
          (s: number, r: any) => s + r.cnt,
          0,
        );
        expect(total).toBe(i + 1);
      }
    }

    const final = await pg.query(
      `SELECT COUNT(*)::int as cnt FROM events`,
    );
    expect(final.rows[0].cnt).toBe(100);
  },
);

// ---------------------------------------------------------------------------
// Persistence scenarios
// ---------------------------------------------------------------------------

describe("CTE + Window: persistence round-trip", () => {
  it("complex query results survive syncfs + remount @fast", async () => {
    harness = await createPGliteHarness({
      cacheSize: "small",
    });
    const { pg, backend } = harness;

    await pg.query(`
      CREATE TABLE readings (
        id SERIAL PRIMARY KEY,
        sensor_id INT,
        value NUMERIC(10,2),
        ts TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    for (let i = 0; i < 60; i++) {
      await pg.query(
        `INSERT INTO readings (sensor_id, value) VALUES ($1, $2)`,
        [i % 6, 20 + (i * 3.7) % 40],
      );
    }

    const beforeResult = await pg.query(`
      WITH sensor_stats AS (
        SELECT sensor_id, AVG(value) as avg_val, COUNT(*)::int as cnt
        FROM readings GROUP BY sensor_id
      )
      SELECT sensor_id, avg_val, cnt,
             RANK() OVER (ORDER BY avg_val DESC) as rank
      FROM sensor_stats
      ORDER BY sensor_id
    `);

    await harness.syncToFs();
    await harness.destroy();
    harness = null;

    harness = await createPGliteHarness({
      cacheSize: "small",
      backend,
    });

    const afterResult = await harness.pg.query(`
      WITH sensor_stats AS (
        SELECT sensor_id, AVG(value) as avg_val, COUNT(*)::int as cnt
        FROM readings GROUP BY sensor_id
      )
      SELECT sensor_id, avg_val, cnt,
             RANK() OVER (ORDER BY avg_val DESC) as rank
      FROM sensor_stats
      ORDER BY sensor_id
    `);

    expect(afterResult.rows.length).toBe(beforeResult.rows.length);
    for (let i = 0; i < beforeResult.rows.length; i++) {
      expect(afterResult.rows[i].sensor_id).toBe(beforeResult.rows[i].sensor_id);
      expect(afterResult.rows[i].cnt).toBe(beforeResult.rows[i].cnt);
      expect(Number(afterResult.rows[i].avg_val)).toBeCloseTo(
        Number(beforeResult.rows[i].avg_val),
        1,
      );
    }
  });
});

describe("Recursive CTE: dirty shutdown + WAL recovery", () => {
  it("tree data survives crash recovery @fast", async () => {
    harness = await createPGliteHarness({
      cacheSize: "small",
    });
    const { pg, backend } = harness;

    await pg.query(`
      CREATE TABLE tree_nodes (
        id SERIAL PRIMARY KEY,
        parent_id INT REFERENCES tree_nodes(id),
        label TEXT
      )
    `);

    await pg.query(`INSERT INTO tree_nodes (label) VALUES ('root')`);
    for (let i = 0; i < 20; i++) {
      await pg.query(
        `INSERT INTO tree_nodes (parent_id, label) VALUES ($1, $2)`,
        [(i % 5) + 1, `node_${i}`],
      );
    }

    await harness.syncToFs();

    for (let i = 0; i < 10; i++) {
      await pg.query(
        `INSERT INTO tree_nodes (parent_id, label) VALUES ($1, $2)`,
        [(i % 10) + 1, `post_sync_${i}`],
      );
    }

    harness.dirtyDestroy();
    harness = null;

    harness = await createPGliteHarness({
      cacheSize: "small",
      backend,
    });

    const result = await harness.pg.query(`
      WITH RECURSIVE tree AS (
        SELECT id, label, 0 as depth
        FROM tree_nodes WHERE parent_id IS NULL
        UNION ALL
        SELECT tn.id, tn.label, t.depth + 1
        FROM tree_nodes tn
        JOIN tree t ON tn.parent_id = t.id
      )
      SELECT COUNT(*)::int as cnt FROM tree
    `);

    expect(result.rows[0].cnt).toBeGreaterThanOrEqual(21);
  });
});
