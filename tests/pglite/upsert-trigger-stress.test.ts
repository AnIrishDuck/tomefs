/**
 * PGlite + tomefs UPSERT and trigger cascade stress tests (ethos §8).
 *
 * Exercises Postgres workload patterns that create unique page access
 * interleaving not covered by pure INSERT/SELECT tests:
 *
 * 1. UPSERT (INSERT ... ON CONFLICT DO UPDATE):
 *    Each operation requires an index lookup (btree traversal) to detect
 *    conflicts, followed by either an INSERT (new heap tuple + index entry)
 *    or UPDATE (HOT update if possible, otherwise new tuple + index update).
 *    Under cache pressure, index pages and heap pages compete for cache
 *    slots — the index lookup evicts heap pages needed for the subsequent
 *    write, and vice versa.
 *
 * 2. Trigger cascades:
 *    INSERT/UPDATE/DELETE triggers that write to audit/log tables create
 *    cross-table write storms within a single statement. The triggering
 *    table's heap + index pages and the audit table's heap pages all
 *    compete for the same cache slots, creating an interleaved write
 *    pattern that sequential bulk tests don't exercise.
 *
 * 3. UPSERT + trigger combination:
 *    The most complex pattern — each upsert triggers an index lookup,
 *    potential heap update, PLUS trigger-initiated writes to other tables.
 *    Under tiny cache, every operation evicts pages needed by the next.
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
      it(`cache=${size} (${pages} pages) @fast`, async () => {
        harness = await createPGliteHarness(size);
        await scenarioFn(harness, size);
      });
    }
  });
}

// ---------------------------------------------------------------------------
// Scenario 1: Basic UPSERT — insert then update same rows
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 1: basic UPSERT insert-then-update cycle",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE kv (
      key INTEGER PRIMARY KEY,
      value TEXT NOT NULL,
      version INTEGER NOT NULL DEFAULT 1
    )`);

    const N = 100;

    // Phase 1: Insert N rows via UPSERT (all inserts, no conflicts)
    for (let i = 0; i < N; i++) {
      await pg.query(
        `INSERT INTO kv (key, value) VALUES ($1, $2)
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, version = kv.version + 1`,
        [i, `initial-${i}`],
      );
    }

    const insertCount = await pg.query(`SELECT COUNT(*)::int AS n FROM kv`);
    expect(insertCount.rows[0].n).toBe(N);

    // Phase 2: Update all rows via UPSERT (all conflicts)
    for (let i = 0; i < N; i++) {
      await pg.query(
        `INSERT INTO kv (key, value) VALUES ($1, $2)
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, version = kv.version + 1`,
        [i, `updated-${i}`],
      );
    }

    // Verify all rows updated
    const updated = await pg.query(
      `SELECT COUNT(*)::int AS n FROM kv WHERE version = 2`,
    );
    expect(updated.rows[0].n).toBe(N);

    const spot = await pg.query(`SELECT value FROM kv WHERE key = 50`);
    expect(spot.rows[0].value).toBe("updated-50");
  },
);

// ---------------------------------------------------------------------------
// Scenario 2: UPSERT with secondary index
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 2: UPSERT with secondary index pressure",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE products (
      sku TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      price NUMERIC NOT NULL,
      updated_at TIMESTAMP DEFAULT now()
    )`);
    await pg.query(`CREATE INDEX idx_products_price ON products (price)`);

    const N = 80;

    // Insert products
    for (let i = 0; i < N; i++) {
      await pg.query(
        `INSERT INTO products (sku, name, price) VALUES ($1, $2, $3)
         ON CONFLICT (sku) DO UPDATE SET
           name = EXCLUDED.name,
           price = EXCLUDED.price,
           updated_at = now()`,
        [`SKU-${i}`, `Product ${i}`, (i * 9.99).toFixed(2)],
      );
    }

    // Update prices (triggers index maintenance on the price index)
    for (let i = 0; i < N; i++) {
      await pg.query(
        `INSERT INTO products (sku, name, price) VALUES ($1, $2, $3)
         ON CONFLICT (sku) DO UPDATE SET
           price = EXCLUDED.price,
           updated_at = now()`,
        [`SKU-${i}`, `Product ${i}`, ((N - i) * 7.77).toFixed(2)],
      );
    }

    const count = await pg.query(`SELECT COUNT(*)::int AS n FROM products`);
    expect(count.rows[0].n).toBe(N);

    // Verify index is consistent via index scan
    const ordered = await pg.query(
      `SELECT sku FROM products ORDER BY price LIMIT 5`,
    );
    expect(ordered.rows.length).toBe(5);
  },
);

// ---------------------------------------------------------------------------
// Scenario 3: INSERT trigger writes to audit table
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 3: INSERT trigger with audit table",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE orders (
      id SERIAL PRIMARY KEY,
      customer TEXT NOT NULL,
      amount NUMERIC NOT NULL
    )`);

    await pg.query(`CREATE TABLE audit_log (
      id SERIAL PRIMARY KEY,
      table_name TEXT NOT NULL,
      operation TEXT NOT NULL,
      row_id INTEGER,
      logged_at TIMESTAMP DEFAULT now()
    )`);

    await pg.query(`
      CREATE OR REPLACE FUNCTION log_order_insert()
      RETURNS TRIGGER AS $$
      BEGIN
        INSERT INTO audit_log (table_name, operation, row_id)
        VALUES ('orders', 'INSERT', NEW.id);
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql
    `);

    await pg.query(`
      CREATE TRIGGER orders_after_insert
      AFTER INSERT ON orders
      FOR EACH ROW EXECUTE FUNCTION log_order_insert()
    `);

    const N = 120;
    for (let i = 0; i < N; i++) {
      await pg.query(
        `INSERT INTO orders (customer, amount) VALUES ($1, $2)`,
        [`customer-${i}`, (i * 42.5).toFixed(2)],
      );
    }

    // Verify both tables have correct row counts
    const orderCount = await pg.query(
      `SELECT COUNT(*)::int AS n FROM orders`,
    );
    expect(orderCount.rows[0].n).toBe(N);

    const auditCount = await pg.query(
      `SELECT COUNT(*)::int AS n FROM audit_log`,
    );
    expect(auditCount.rows[0].n).toBe(N);

    // Verify audit log references match
    const mismatch = await pg.query(`
      SELECT COUNT(*)::int AS n FROM audit_log a
      WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.id = a.row_id)
    `);
    expect(mismatch.rows[0].n).toBe(0);
  },
);

// ---------------------------------------------------------------------------
// Scenario 4: UPDATE trigger cascade across tables
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 4: UPDATE trigger cascade",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE accounts (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      balance NUMERIC NOT NULL DEFAULT 0
    )`);

    await pg.query(`CREATE TABLE balance_history (
      id SERIAL PRIMARY KEY,
      account_id INTEGER NOT NULL,
      old_balance NUMERIC NOT NULL,
      new_balance NUMERIC NOT NULL,
      changed_at TIMESTAMP DEFAULT now()
    )`);

    await pg.query(`
      CREATE OR REPLACE FUNCTION log_balance_change()
      RETURNS TRIGGER AS $$
      BEGIN
        IF OLD.balance IS DISTINCT FROM NEW.balance THEN
          INSERT INTO balance_history (account_id, old_balance, new_balance)
          VALUES (NEW.id, OLD.balance, NEW.balance);
        END IF;
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql
    `);

    await pg.query(`
      CREATE TRIGGER accounts_balance_change
      AFTER UPDATE ON accounts
      FOR EACH ROW EXECUTE FUNCTION log_balance_change()
    `);

    // Create accounts
    const numAccounts = 30;
    for (let i = 0; i < numAccounts; i++) {
      await pg.query(
        `INSERT INTO accounts (name, balance) VALUES ($1, $2)`,
        [`account-${i}`, 1000],
      );
    }

    // Perform many balance updates (each triggers history write)
    const numUpdates = 100;
    for (let i = 0; i < numUpdates; i++) {
      const accountId = (i % numAccounts) + 1;
      const delta = (i % 2 === 0 ? 1 : -1) * ((i % 50) + 1);
      await pg.query(
        `UPDATE accounts SET balance = balance + $1 WHERE id = $2`,
        [delta, accountId],
      );
    }

    const historyCount = await pg.query(
      `SELECT COUNT(*)::int AS n FROM balance_history`,
    );
    expect(historyCount.rows[0].n).toBe(numUpdates);

    // Verify balance consistency: sum of deltas in history should match
    // current balance minus initial balance for each account
    const consistency = await pg.query(`
      SELECT a.id, a.balance,
             1000 + COALESCE(SUM(bh.new_balance - bh.old_balance), 0) AS expected
      FROM accounts a
      LEFT JOIN balance_history bh ON bh.account_id = a.id
      GROUP BY a.id, a.balance
      HAVING a.balance != 1000 + COALESCE(SUM(bh.new_balance - bh.old_balance), 0)
    `);
    expect(consistency.rows.length).toBe(0);
  },
);

// ---------------------------------------------------------------------------
// Scenario 5: UPSERT with trigger (combined pressure)
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 5: UPSERT with trigger — combined pressure",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE config (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL,
      updated_count INTEGER NOT NULL DEFAULT 0
    )`);

    await pg.query(`CREATE TABLE config_changes (
      id SERIAL PRIMARY KEY,
      config_key TEXT NOT NULL,
      old_value TEXT,
      new_value TEXT NOT NULL,
      changed_at TIMESTAMP DEFAULT now()
    )`);

    await pg.query(`
      CREATE OR REPLACE FUNCTION log_config_change()
      RETURNS TRIGGER AS $$
      BEGIN
        IF TG_OP = 'INSERT' THEN
          INSERT INTO config_changes (config_key, old_value, new_value)
          VALUES (NEW.key, NULL, NEW.value);
        ELSIF OLD.value IS DISTINCT FROM NEW.value THEN
          INSERT INTO config_changes (config_key, old_value, new_value)
          VALUES (NEW.key, OLD.value, NEW.value);
        END IF;
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql
    `);

    await pg.query(`
      CREATE TRIGGER config_after_upsert
      AFTER INSERT OR UPDATE ON config
      FOR EACH ROW EXECUTE FUNCTION log_config_change()
    `);

    // 3 rounds of upserts: first round inserts, subsequent rounds update
    const keys = 40;
    const rounds = 3;

    for (let round = 0; round < rounds; round++) {
      for (let k = 0; k < keys; k++) {
        await pg.query(
          `INSERT INTO config (key, value) VALUES ($1, $2)
           ON CONFLICT (key) DO UPDATE SET
             value = EXCLUDED.value,
             updated_count = config.updated_count + 1`,
          [`key-${k}`, `value-round${round}-${k}`],
        );
      }
    }

    const configCount = await pg.query(
      `SELECT COUNT(*)::int AS n FROM config`,
    );
    expect(configCount.rows[0].n).toBe(keys);

    // Each key: 1 insert + (rounds-1) updates = rounds changes total
    // But updates where value changed trigger logs
    const changeCount = await pg.query(
      `SELECT COUNT(*)::int AS n FROM config_changes`,
    );
    expect(changeCount.rows[0].n).toBe(keys * rounds);

    // Verify update counts
    const allUpdated = await pg.query(
      `SELECT COUNT(*)::int AS n FROM config WHERE updated_count = $1`,
      [rounds - 1],
    );
    expect(allUpdated.rows[0].n).toBe(keys);
  },
);

// ---------------------------------------------------------------------------
// Scenario 6: DELETE trigger with cascading cleanup
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 6: DELETE trigger with cascading cleanup",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE users (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL
    )`);

    await pg.query(`CREATE TABLE user_sessions (
      id SERIAL PRIMARY KEY,
      user_id INTEGER NOT NULL,
      token TEXT NOT NULL
    )`);

    await pg.query(`CREATE TABLE deletion_log (
      id SERIAL PRIMARY KEY,
      user_id INTEGER NOT NULL,
      sessions_removed INTEGER NOT NULL,
      deleted_at TIMESTAMP DEFAULT now()
    )`);

    await pg.query(`
      CREATE OR REPLACE FUNCTION cleanup_user_sessions()
      RETURNS TRIGGER AS $$
      DECLARE
        removed INTEGER;
      BEGIN
        DELETE FROM user_sessions WHERE user_id = OLD.id;
        GET DIAGNOSTICS removed = ROW_COUNT;
        INSERT INTO deletion_log (user_id, sessions_removed)
        VALUES (OLD.id, removed);
        RETURN OLD;
      END;
      $$ LANGUAGE plpgsql
    `);

    await pg.query(`
      CREATE TRIGGER users_before_delete
      BEFORE DELETE ON users
      FOR EACH ROW EXECUTE FUNCTION cleanup_user_sessions()
    `);

    // Create users with multiple sessions each
    const numUsers = 30;
    const sessionsPerUser = 3;
    for (let u = 0; u < numUsers; u++) {
      await pg.query(
        `INSERT INTO users (name) VALUES ($1)`,
        [`user-${u}`],
      );
      for (let s = 0; s < sessionsPerUser; s++) {
        await pg.query(
          `INSERT INTO user_sessions (user_id, token) VALUES ($1, $2)`,
          [u + 1, `token-${u}-${s}`],
        );
      }
    }

    // Delete half the users (triggers session cleanup + logging)
    const toDelete = numUsers / 2;
    for (let u = 0; u < toDelete; u++) {
      await pg.query(`DELETE FROM users WHERE id = $1`, [u + 1]);
    }

    const remainingUsers = await pg.query(
      `SELECT COUNT(*)::int AS n FROM users`,
    );
    expect(remainingUsers.rows[0].n).toBe(numUsers - toDelete);

    // Sessions for deleted users should be gone
    const orphanSessions = await pg.query(`
      SELECT COUNT(*)::int AS n FROM user_sessions us
      WHERE NOT EXISTS (SELECT 1 FROM users u WHERE u.id = us.user_id)
    `);
    expect(orphanSessions.rows[0].n).toBe(0);

    // Deletion log should have entries for each deleted user
    const logCount = await pg.query(
      `SELECT COUNT(*)::int AS n FROM deletion_log`,
    );
    expect(logCount.rows[0].n).toBe(toDelete);

    // Each deletion should record the correct session count
    const wrongCount = await pg.query(`
      SELECT COUNT(*)::int AS n FROM deletion_log
      WHERE sessions_removed != $1
    `, [sessionsPerUser]);
    expect(wrongCount.rows[0].n).toBe(0);
  },
);

// ---------------------------------------------------------------------------
// Scenario 7: Multi-table UPSERT batch (interleaved cross-table writes)
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 7: multi-table UPSERT batch",
  async (h) => {
    const { pg } = h;

    // Three tables receiving interleaved upserts — cache slots are
    // contested by heap + index pages from all three tables simultaneously
    await pg.query(`CREATE TABLE inventory_a (
      item_id INTEGER PRIMARY KEY,
      quantity INTEGER NOT NULL DEFAULT 0
    )`);
    await pg.query(`CREATE TABLE inventory_b (
      item_id INTEGER PRIMARY KEY,
      quantity INTEGER NOT NULL DEFAULT 0
    )`);
    await pg.query(`CREATE TABLE inventory_c (
      item_id INTEGER PRIMARY KEY,
      quantity INTEGER NOT NULL DEFAULT 0
    )`);

    const items = 50;
    const rounds = 3;

    for (let round = 0; round < rounds; round++) {
      for (let i = 0; i < items; i++) {
        // Interleave writes across tables in round-robin
        await pg.query(
          `INSERT INTO inventory_a (item_id, quantity) VALUES ($1, $2)
           ON CONFLICT (item_id) DO UPDATE SET quantity = inventory_a.quantity + EXCLUDED.quantity`,
          [i, round + 1],
        );
        await pg.query(
          `INSERT INTO inventory_b (item_id, quantity) VALUES ($1, $2)
           ON CONFLICT (item_id) DO UPDATE SET quantity = inventory_b.quantity + EXCLUDED.quantity`,
          [i, (round + 1) * 10],
        );
        await pg.query(
          `INSERT INTO inventory_c (item_id, quantity) VALUES ($1, $2)
           ON CONFLICT (item_id) DO UPDATE SET quantity = inventory_c.quantity + EXCLUDED.quantity`,
          [i, (round + 1) * 100],
        );
      }
    }

    // Verify totals: each item was upserted `rounds` times
    // Sum for table a: 1+2+3 = 6, table b: 10+20+30 = 60, table c: 100+200+300 = 600
    const expectedA = (rounds * (rounds + 1)) / 2;
    const expectedB = expectedA * 10;
    const expectedC = expectedA * 100;

    const sumA = await pg.query(
      `SELECT SUM(quantity)::int AS total FROM inventory_a`,
    );
    expect(sumA.rows[0].total).toBe(expectedA * items);

    const sumB = await pg.query(
      `SELECT SUM(quantity)::int AS total FROM inventory_b`,
    );
    expect(sumB.rows[0].total).toBe(expectedB * items);

    const sumC = await pg.query(
      `SELECT SUM(quantity)::int AS total FROM inventory_c`,
    );
    expect(sumC.rows[0].total).toBe(expectedC * items);
  },
);

// ---------------------------------------------------------------------------
// Scenario 8: UPSERT storm then persistence round-trip
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 8: UPSERT persistence round-trip",
  async (h, cacheSize) => {
    const { pg, backend, syncToFs } = h;

    await pg.query(`CREATE TABLE counters (
      name TEXT PRIMARY KEY,
      count INTEGER NOT NULL DEFAULT 0
    )`);

    const numCounters = 20;
    const increments = 50;

    // Rapid-fire upserts
    for (let i = 0; i < increments; i++) {
      const counterName = `counter-${i % numCounters}`;
      await pg.query(
        `INSERT INTO counters (name, count) VALUES ($1, 1)
         ON CONFLICT (name) DO UPDATE SET count = counters.count + 1`,
        [counterName],
      );
    }

    // Sync and remount
    await syncToFs();
    await h.destroy();

    const maxPages = typeof cacheSize === "number"
      ? cacheSize
      : CACHE_CONFIGS[cacheSize];
    const { createPGliteHarness: create } = await import("./harness.js");
    const h2 = await create({ cacheSize: maxPages, backend });
    harness = h2;

    // Verify data survived remount
    const total = await h2.pg.query(
      `SELECT SUM(count)::int AS total FROM counters`,
    );
    expect(total.rows[0].total).toBe(increments);

    const numRows = await h2.pg.query(
      `SELECT COUNT(*)::int AS n FROM counters`,
    );
    expect(numRows.rows[0].n).toBe(numCounters);
  },
);

// ---------------------------------------------------------------------------
// Scenario 9: trigger + UPSERT dirty shutdown and WAL recovery
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 9: trigger + UPSERT dirty shutdown recovery",
  async (h, cacheSize) => {
    const { pg, backend, syncToFs } = h;

    await pg.query(`CREATE TABLE events (
      id TEXT PRIMARY KEY,
      data TEXT NOT NULL
    )`);

    await pg.query(`CREATE TABLE event_counts (
      id INTEGER PRIMARY KEY DEFAULT 1,
      total INTEGER NOT NULL DEFAULT 0
    )`);
    await pg.query(`INSERT INTO event_counts (total) VALUES (0)`);

    await pg.query(`
      CREATE OR REPLACE FUNCTION increment_event_count()
      RETURNS TRIGGER AS $$
      BEGIN
        UPDATE event_counts SET total = total + 1 WHERE id = 1;
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql
    `);

    await pg.query(`
      CREATE TRIGGER events_after_upsert
      AFTER INSERT OR UPDATE ON events
      FOR EACH ROW EXECUTE FUNCTION increment_event_count()
    `);

    // Phase 1: write some events and sync (checkpoint)
    const checkpoint = 30;
    for (let i = 0; i < checkpoint; i++) {
      await pg.query(
        `INSERT INTO events (id, data) VALUES ($1, $2)
         ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data`,
        [`evt-${i}`, `data-${i}`],
      );
    }
    await syncToFs();

    // Phase 2: write more events WITHOUT syncing (dirty data)
    const postCheckpoint = 20;
    for (let i = checkpoint; i < checkpoint + postCheckpoint; i++) {
      await pg.query(
        `INSERT INTO events (id, data) VALUES ($1, $2)
         ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data`,
        [`evt-${i}`, `data-${i}`],
      );
    }

    // Dirty shutdown — no sync, no close
    h.dirtyDestroy();

    // Remount — Postgres should recover via WAL replay
    const maxPages = typeof cacheSize === "number"
      ? cacheSize
      : CACHE_CONFIGS[cacheSize];
    const { createPGliteHarness: create } = await import("./harness.js");
    const h2 = await create({ cacheSize: maxPages, backend });
    harness = h2;

    // At minimum, the checkpointed data must survive
    const eventCount = await h2.pg.query(
      `SELECT COUNT(*)::int AS n FROM events`,
    );
    expect(eventCount.rows[0].n).toBeGreaterThanOrEqual(checkpoint);

    // The event_counts trigger total must be consistent with events
    const total = await h2.pg.query(
      `SELECT total FROM event_counts WHERE id = 1`,
    );
    const actualEvents = eventCount.rows[0].n;
    expect(total.rows[0].total).toBe(actualEvents);
  },
);

// ---------------------------------------------------------------------------
// Scenario 10: Statement-level trigger with aggregate computation
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 10: statement-level trigger with summary update",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE sales (
      id SERIAL PRIMARY KEY,
      product TEXT NOT NULL,
      amount NUMERIC NOT NULL
    )`);

    await pg.query(`CREATE TABLE sales_summary (
      product TEXT PRIMARY KEY,
      total_amount NUMERIC NOT NULL DEFAULT 0,
      sale_count INTEGER NOT NULL DEFAULT 0
    )`);

    await pg.query(`
      CREATE OR REPLACE FUNCTION refresh_sales_summary()
      RETURNS TRIGGER AS $$
      BEGIN
        INSERT INTO sales_summary (product, total_amount, sale_count)
        SELECT product, SUM(amount), COUNT(*)
        FROM sales
        GROUP BY product
        ON CONFLICT (product) DO UPDATE SET
          total_amount = EXCLUDED.total_amount,
          sale_count = EXCLUDED.sale_count;
        RETURN NULL;
      END;
      $$ LANGUAGE plpgsql
    `);

    await pg.query(`
      CREATE TRIGGER sales_after_statement
      AFTER INSERT ON sales
      FOR EACH STATEMENT EXECUTE FUNCTION refresh_sales_summary()
    `);

    // Batch inserts — each INSERT statement triggers a full re-aggregation
    const products = ["widget", "gadget", "doohickey", "thingamajig"];
    const batches = 5;
    const perBatch = 20;

    for (let b = 0; b < batches; b++) {
      const values: string[] = [];
      const params: any[] = [];
      for (let i = 0; i < perBatch; i++) {
        const product = products[(b * perBatch + i) % products.length];
        const amount = ((b * perBatch + i) * 12.5 + 10).toFixed(2);
        const idx = i * 2;
        values.push(`($${idx + 1}, $${idx + 2})`);
        params.push(product, amount);
      }
      await pg.query(
        `INSERT INTO sales (product, amount) VALUES ${values.join(", ")}`,
        params,
      );
    }

    const totalSales = await pg.query(
      `SELECT COUNT(*)::int AS n FROM sales`,
    );
    expect(totalSales.rows[0].n).toBe(batches * perBatch);

    // Verify summary matches actual sales data
    const summaryCheck = await pg.query(`
      SELECT s.product,
             s.total_amount,
             s.sale_count,
             (SELECT SUM(amount) FROM sales WHERE product = s.product) AS expected_total,
             (SELECT COUNT(*) FROM sales WHERE product = s.product)::int AS expected_count
      FROM sales_summary s
    `);
    for (const row of summaryCheck.rows) {
      expect(Number(row.total_amount)).toBeCloseTo(Number(row.expected_total), 2);
      expect(row.sale_count).toBe(row.expected_count);
    }
  },
);

// ---------------------------------------------------------------------------
// Scenario 11: UPSERT with RETURNING clause
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 11: UPSERT with RETURNING tracks insert vs update",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE cache_entries (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL,
      hits INTEGER NOT NULL DEFAULT 0
    )`);

    const N = 60;
    let insertCount = 0;
    let updateCount = 0;

    // Two passes: first inserts everything, second updates everything
    for (let pass = 0; pass < 2; pass++) {
      for (let i = 0; i < N; i++) {
        const result = await pg.query(
          `INSERT INTO cache_entries (key, value) VALUES ($1, $2)
           ON CONFLICT (key) DO UPDATE SET
             value = EXCLUDED.value,
             hits = cache_entries.hits + 1
           RETURNING hits`,
          [`key-${i}`, `value-pass${pass}-${i}`],
        );
        if (result.rows[0].hits === 0) {
          insertCount++;
        } else {
          updateCount++;
        }
      }
    }

    expect(insertCount).toBe(N);
    expect(updateCount).toBe(N);

    // All entries should have hits = 1 (updated once)
    const allHits = await pg.query(
      `SELECT COUNT(*)::int AS n FROM cache_entries WHERE hits = 1`,
    );
    expect(allHits.rows[0].n).toBe(N);
  },
);

// ---------------------------------------------------------------------------
// Scenario 12: Multi-column UNIQUE constraint UPSERT
// ---------------------------------------------------------------------------

describeScenario(
  "Scenario 12: multi-column UNIQUE constraint UPSERT",
  async (h) => {
    const { pg } = h;

    await pg.query(`CREATE TABLE metrics (
      source TEXT NOT NULL,
      metric_name TEXT NOT NULL,
      value NUMERIC NOT NULL,
      sample_count INTEGER NOT NULL DEFAULT 1,
      UNIQUE (source, metric_name)
    )`);

    const sources = ["web", "api", "mobile"];
    const metrics = ["latency", "errors", "requests", "throughput"];
    const rounds = 10;

    for (let r = 0; r < rounds; r++) {
      for (const src of sources) {
        for (const m of metrics) {
          await pg.query(
            `INSERT INTO metrics (source, metric_name, value)
             VALUES ($1, $2, $3)
             ON CONFLICT (source, metric_name) DO UPDATE SET
               value = (metrics.value * metrics.sample_count + EXCLUDED.value) / (metrics.sample_count + 1),
               sample_count = metrics.sample_count + 1`,
            [src, m, Math.random() * 100],
          );
        }
      }
    }

    const count = await pg.query(`SELECT COUNT(*)::int AS n FROM metrics`);
    expect(count.rows[0].n).toBe(sources.length * metrics.length);

    // All entries should have sample_count = rounds
    const wrongSamples = await pg.query(
      `SELECT COUNT(*)::int AS n FROM metrics WHERE sample_count != $1`,
      [rounds],
    );
    expect(wrongSamples.rows[0].n).toBe(0);
  },
);
