/**
 * PGlite + tomefs multi-table JOIN stress tests (ethos §8 — workload scenarios).
 *
 * Exercises Postgres JOIN execution under cache pressure. JOINs are
 * uniquely demanding for a page cache because they access pages from
 * multiple tables simultaneously: the planner may choose nested-loop,
 * hash, or merge joins, each with different cache access patterns.
 *
 * Under tiny cache (4 pages), a hash join must build the hash table
 * from one relation (reading its heap pages) then probe with the other
 * (reading different heap pages), evicting the first relation's pages.
 * If the hash table spills, Postgres creates temporary batch files,
 * adding file creation/deletion pressure on top of the page cache.
 *
 * Key scenarios:
 * - Inner JOIN between two tables (cross-table page access)
 * - LEFT JOIN with NULLable matches (hash table build + probe)
 * - Multi-table JOIN chain (3 tables, cascading eviction)
 * - JOIN with aggregate (GROUP BY forces sort/hash after join)
 * - Self-join (same table pages accessed via two scan nodes)
 * - Subquery in WHERE (nested scan under cache pressure)
 * - CTE + JOIN (materialized intermediate result + join)
 * - JOIN with ORDER BY (sort node after join, temp file I/O)
 * - JOIN + UPDATE (read two tables, write one)
 * - Persistence round-trip after join-heavy workload
 * - JOIN with index scan (btree traversal interleaved with heap access)
 * - Cross-join with LIMIT (nested loop with early termination)
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

async function seedTwoTables(pg: any, ordersCount: number, productsCount: number) {
  await pg.query(`
    CREATE TABLE products (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      price NUMERIC(10,2) NOT NULL,
      category TEXT NOT NULL
    )
  `);
  await pg.query(`
    CREATE TABLE orders (
      id INTEGER PRIMARY KEY,
      product_id INTEGER NOT NULL REFERENCES products(id),
      quantity INTEGER NOT NULL,
      customer TEXT NOT NULL
    )
  `);

  await pg.query(`
    INSERT INTO products (id, name, price, category)
    SELECT i,
           'product-' || i || '-' || repeat('x', 40),
           (i % 100) + 0.99,
           CASE i % 4
             WHEN 0 THEN 'electronics'
             WHEN 1 THEN 'clothing'
             WHEN 2 THEN 'food'
             ELSE 'other'
           END
    FROM generate_series(1, ${productsCount}) AS s(i)
  `);

  await pg.query(`
    INSERT INTO orders (id, product_id, quantity, customer)
    SELECT i,
           (i % ${productsCount}) + 1,
           (i % 10) + 1,
           'customer-' || (i % 20) || '-' || repeat('y', 30)
    FROM generate_series(1, ${ordersCount}) AS s(i)
  `);
}

// ---------------------------------------------------------------------------
// Scenario 1: Inner JOIN between two tables
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 1 @fast: inner JOIN between two tables",
  async (h) => {
    const { pg } = h;
    await seedTwoTables(pg, 200, 50);

    const result = await pg.query(`
      SELECT o.id AS order_id, p.name, o.quantity, p.price
      FROM orders o
      JOIN products p ON o.product_id = p.id
      ORDER BY o.id
      LIMIT 10
    `);

    expect(result.rows.length).toBe(10);
    expect(result.rows[0].order_id).toBe(1);
    expect(result.rows[0].name).toContain("product-");

    const total = await pg.query(`
      SELECT COUNT(*)::int AS n FROM orders o JOIN products p ON o.product_id = p.id
    `);
    expect(total.rows[0].n).toBe(200);
  },
);

// ---------------------------------------------------------------------------
// Scenario 2: LEFT JOIN with NULLable matches
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 2 @fast: LEFT JOIN with unmatched rows",
  async (h) => {
    const { pg } = h;
    await seedTwoTables(pg, 100, 50);

    await pg.query(`
      INSERT INTO products (id, name, price, category)
      VALUES (999, 'orphan-product', 0.01, 'other')
    `);

    const result = await pg.query(`
      SELECT p.id, p.name, COUNT(o.id)::int AS order_count
      FROM products p
      LEFT JOIN orders o ON o.product_id = p.id
      GROUP BY p.id, p.name
      ORDER BY order_count ASC, p.id ASC
      LIMIT 5
    `);

    expect(result.rows.length).toBe(5);
    expect(result.rows[0].order_count).toBe(0);
    expect(result.rows[0].id).toBe(999);
  },
);

// ---------------------------------------------------------------------------
// Scenario 3: Multi-table JOIN chain (3 tables)
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 3 @fast: three-table JOIN chain",
  async (h) => {
    const { pg } = h;
    await seedTwoTables(pg, 150, 40);

    await pg.query(`
      CREATE TABLE reviews (
        id INTEGER PRIMARY KEY,
        order_id INTEGER NOT NULL REFERENCES orders(id),
        rating INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 5),
        comment TEXT
      )
    `);

    await pg.query(`
      INSERT INTO reviews (id, order_id, rating, comment)
      SELECT i,
             (i % 150) + 1,
             (i % 5) + 1,
             'review-' || i || '-' || repeat('z', 50)
      FROM generate_series(1, 300) AS s(i)
    `);

    const result = await pg.query(`
      SELECT p.category,
             AVG(r.rating)::numeric(3,1) AS avg_rating,
             COUNT(DISTINCT o.id)::int AS order_count
      FROM products p
      JOIN orders o ON o.product_id = p.id
      JOIN reviews r ON r.order_id = o.id
      GROUP BY p.category
      ORDER BY avg_rating DESC
    `);

    expect(result.rows.length).toBe(4);
    for (const row of result.rows) {
      expect(Number(row.avg_rating)).toBeGreaterThanOrEqual(1);
      expect(Number(row.avg_rating)).toBeLessThanOrEqual(5);
      expect(row.order_count).toBeGreaterThan(0);
    }
  },
);

// ---------------------------------------------------------------------------
// Scenario 4: JOIN with aggregate (GROUP BY + HAVING)
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 4: JOIN with GROUP BY and HAVING",
  async (h) => {
    const { pg } = h;
    await seedTwoTables(pg, 300, 50);

    const result = await pg.query(`
      SELECT p.category,
             SUM(o.quantity * p.price)::numeric(12,2) AS revenue,
             COUNT(*)::int AS order_count
      FROM orders o
      JOIN products p ON o.product_id = p.id
      GROUP BY p.category
      HAVING COUNT(*) > 10
      ORDER BY revenue DESC
    `);

    expect(result.rows.length).toBeGreaterThan(0);
    for (const row of result.rows) {
      expect(row.order_count).toBeGreaterThan(10);
      expect(Number(row.revenue)).toBeGreaterThan(0);
    }

    const verifyTotal = await pg.query(`
      SELECT SUM(sub.revenue)::numeric(12,2) AS total
      FROM (
        SELECT SUM(o.quantity * p.price) AS revenue
        FROM orders o JOIN products p ON o.product_id = p.id
        GROUP BY p.category
        HAVING COUNT(*) > 10
      ) sub
    `);
    expect(Number(verifyTotal.rows[0].total)).toBeGreaterThan(0);
  },
);

// ---------------------------------------------------------------------------
// Scenario 5: Self-join (same table accessed via two scan nodes)
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 5 @fast: self-join on orders",
  async (h) => {
    const { pg } = h;
    await seedTwoTables(pg, 100, 30);

    const result = await pg.query(`
      SELECT a.customer, COUNT(DISTINCT b.product_id)::int AS also_bought
      FROM orders a
      JOIN orders b ON a.customer = b.customer AND a.product_id <> b.product_id
      GROUP BY a.customer
      ORDER BY also_bought DESC
      LIMIT 5
    `);

    expect(result.rows.length).toBeGreaterThan(0);
    for (const row of result.rows) {
      expect(row.also_bought).toBeGreaterThan(0);
    }
  },
);

// ---------------------------------------------------------------------------
// Scenario 6: Subquery in WHERE clause
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 6: correlated subquery under cache pressure",
  async (h) => {
    const { pg } = h;
    await seedTwoTables(pg, 200, 50);

    const result = await pg.query(`
      SELECT p.id, p.name, p.category, p.price
      FROM products p
      WHERE p.price > (
        SELECT AVG(p2.price) FROM products p2 WHERE p2.category = p.category
      )
      ORDER BY p.id
    `);

    expect(result.rows.length).toBeGreaterThan(0);

    for (const row of result.rows) {
      const avgResult = await pg.query(
        `SELECT AVG(price)::numeric(10,2) AS avg FROM products WHERE category = $1`,
        [row.category],
      );
      expect(Number(row.price)).toBeGreaterThan(Number(avgResult.rows[0].avg));
    }
  },
);

// ---------------------------------------------------------------------------
// Scenario 7: CTE materialization + JOIN
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 7 @fast: CTE materialized then joined",
  async (h) => {
    const { pg } = h;
    await seedTwoTables(pg, 200, 50);

    const result = await pg.query(`
      WITH category_stats AS MATERIALIZED (
        SELECT p.category,
               COUNT(*)::int AS product_count,
               AVG(p.price)::numeric(10,2) AS avg_price
        FROM products p
        GROUP BY p.category
      ),
      order_stats AS MATERIALIZED (
        SELECT p.category,
               SUM(o.quantity)::int AS total_qty
        FROM orders o
        JOIN products p ON o.product_id = p.id
        GROUP BY p.category
      )
      SELECT cs.category, cs.product_count, cs.avg_price, os.total_qty
      FROM category_stats cs
      JOIN order_stats os ON cs.category = os.category
      ORDER BY os.total_qty DESC
    `);

    expect(result.rows.length).toBe(4);
    for (const row of result.rows) {
      expect(row.product_count).toBeGreaterThan(0);
      expect(row.total_qty).toBeGreaterThan(0);
      expect(Number(row.avg_price)).toBeGreaterThan(0);
    }
  },
);

// ---------------------------------------------------------------------------
// Scenario 8: JOIN with ORDER BY (sort after join)
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 8: JOIN with multi-column ORDER BY",
  async (h) => {
    const { pg } = h;
    await seedTwoTables(pg, 300, 50);

    const result = await pg.query(`
      SELECT p.category, o.customer, SUM(o.quantity)::int AS total_qty
      FROM orders o
      JOIN products p ON o.product_id = p.id
      GROUP BY p.category, o.customer
      ORDER BY p.category ASC, total_qty DESC
    `);

    expect(result.rows.length).toBeGreaterThan(0);

    let prevCategory = "";
    let prevQty = Infinity;
    for (const row of result.rows) {
      if (row.category !== prevCategory) {
        prevCategory = row.category;
        prevQty = Infinity;
      }
      expect(row.total_qty).toBeLessThanOrEqual(prevQty);
      prevQty = row.total_qty;
    }
  },
);

// ---------------------------------------------------------------------------
// Scenario 9: JOIN + UPDATE (read two tables, write one)
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 9 @fast: JOIN-based UPDATE modifying rows",
  async (h) => {
    const { pg } = h;
    await seedTwoTables(pg, 200, 50);

    await pg.query(`ALTER TABLE orders ADD COLUMN total_price NUMERIC(12,2)`);

    await pg.query(`
      UPDATE orders o
      SET total_price = o.quantity * p.price
      FROM products p
      WHERE o.product_id = p.id
    `);

    const result = await pg.query(`
      SELECT COUNT(*)::int AS n FROM orders WHERE total_price IS NOT NULL
    `);
    expect(result.rows[0].n).toBe(200);

    const verify = await pg.query(`
      SELECT o.total_price, (o.quantity * p.price)::numeric(12,2) AS expected
      FROM orders o
      JOIN products p ON o.product_id = p.id
      ORDER BY o.id
      LIMIT 5
    `);
    for (const row of verify.rows) {
      expect(Number(row.total_price)).toBe(Number(row.expected));
    }
  },
);

// ---------------------------------------------------------------------------
// Scenario 10: Persistence round-trip after join-heavy workload
// ---------------------------------------------------------------------------

describe("Scenario 10 @fast: persistence round-trip after joins", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];
    it(`cache=${size} (${pages} pages)`, async () => {
      harness = await createPGliteHarness(size);
      const { pg } = harness;

      await seedTwoTables(pg, 100, 30);

      const joinResult = await pg.query(`
        SELECT p.category, SUM(o.quantity)::int AS total
        FROM orders o JOIN products p ON o.product_id = p.id
        GROUP BY p.category ORDER BY p.category
      `);
      const expectedRows = joinResult.rows;

      await harness.syncToFs();
      const savedBackend = harness.backend;
      await harness.destroy();

      harness = await createPGliteHarness({
        cacheSize: size,
        backend: savedBackend,
      });

      const afterResult = await harness.pg.query(`
        SELECT p.category, SUM(o.quantity)::int AS total
        FROM orders o JOIN products p ON o.product_id = p.id
        GROUP BY p.category ORDER BY p.category
      `);

      expect(afterResult.rows).toEqual(expectedRows);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 11: JOIN with index scan (btree + heap interleave)
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 11 @fast: indexed JOIN forces btree + heap interleave",
  async (h) => {
    const { pg } = h;
    await seedTwoTables(pg, 200, 50);

    await pg.query(`CREATE INDEX idx_orders_product ON orders (product_id)`);
    await pg.query(`CREATE INDEX idx_products_category ON products (category)`);

    const result = await pg.query(`
      SELECT p.name, SUM(o.quantity)::int AS total_qty
      FROM products p
      JOIN orders o ON o.product_id = p.id
      WHERE p.category = 'electronics'
      GROUP BY p.name
      ORDER BY total_qty DESC
      LIMIT 5
    `);

    expect(result.rows.length).toBeGreaterThan(0);
    for (const row of result.rows) {
      expect(row.total_qty).toBeGreaterThan(0);
    }

    const directCount = await pg.query(`
      SELECT COUNT(*)::int AS n
      FROM orders o
      JOIN products p ON o.product_id = p.id
      WHERE p.category = 'electronics'
    `);
    expect(directCount.rows[0].n).toBeGreaterThan(0);
  },
);

// ---------------------------------------------------------------------------
// Scenario 12: Cross-join with LIMIT (nested loop early termination)
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 12 @fast: cross-join with LIMIT forces early nested-loop exit",
  async (h) => {
    const { pg } = h;
    await seedTwoTables(pg, 100, 30);

    const result = await pg.query(`
      SELECT a.name AS product_a, b.name AS product_b
      FROM products a
      CROSS JOIN products b
      WHERE a.category <> b.category AND a.id < b.id
      ORDER BY a.id, b.id
      LIMIT 20
    `);

    expect(result.rows.length).toBe(20);
    for (const row of result.rows) {
      expect(row.product_a).not.toBe(row.product_b);
    }
  },
);
