/**
 * PGlite + tomefs sequence and SERIAL column stress tests.
 *
 * Sequences have uniquely demanding page access patterns for a bounded
 * cache: each nextval() call reads AND writes the sequence relation's
 * single-tuple page. Under tiny cache (4 pages), multiple sequences
 * compete for cache slots — every nextval() on sequence B can evict
 * sequence A's page, forcing a backend read on the next nextval(A).
 *
 * SERIAL columns implicitly call nextval() on every INSERT, so bulk
 * inserts with multiple SERIAL tables create rapid interleaved
 * sequence page access combined with heap and index page writes.
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
// Scenario 1: Rapid nextval() on multiple sequences
// ---------------------------------------------------------------------------

describe("Rapid nextval on multiple sequences", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`CREATE SEQUENCE seq_a`);
      await pg.query(`CREATE SEQUENCE seq_b`);
      await pg.query(`CREATE SEQUENCE seq_c`);

      const results: Record<string, number[]> = { a: [], b: [], c: [] };

      for (let i = 0; i < 20; i++) {
        const ra = await pg.query(`SELECT nextval('seq_a') AS v`);
        const rb = await pg.query(`SELECT nextval('seq_b') AS v`);
        const rc = await pg.query(`SELECT nextval('seq_c') AS v`);
        results.a.push(Number(ra.rows[0].v));
        results.b.push(Number(rb.rows[0].v));
        results.c.push(Number(rc.rows[0].v));
      }

      for (const [_name, vals] of Object.entries(results)) {
        for (let i = 0; i < vals.length; i++) {
          expect(vals[i]).toBe(i + 1);
        }
      }
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 2: SERIAL columns with bulk inserts
// ---------------------------------------------------------------------------

describe("SERIAL columns with bulk inserts", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`CREATE TABLE items (
        id SERIAL PRIMARY KEY,
        data TEXT NOT NULL
      )`);

      for (let i = 0; i < 50; i++) {
        await pg.query(`INSERT INTO items (data) VALUES ($1)`, [
          `item-${i}-${"x".repeat(100)}`,
        ]);
      }

      const count = await pg.query(`SELECT count(*) AS c FROM items`);
      expect(Number(count.rows[0].c)).toBe(50);

      const maxId = await pg.query(`SELECT max(id) AS m FROM items`);
      expect(Number(maxId.rows[0].m)).toBe(50);

      const ordered = await pg.query(`SELECT id FROM items ORDER BY id`);
      for (let i = 0; i < 50; i++) {
        expect(Number(ordered.rows[i].id)).toBe(i + 1);
      }
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 3: Multiple tables with SERIAL — interleaved inserts
// ---------------------------------------------------------------------------

describe("Interleaved inserts into multiple SERIAL tables", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`CREATE TABLE orders (id SERIAL PRIMARY KEY, amount INT)`);
      await pg.query(`CREATE TABLE products (id SERIAL PRIMARY KEY, name TEXT)`);
      await pg.query(`CREATE TABLE customers (id SERIAL PRIMARY KEY, email TEXT)`);

      for (let i = 0; i < 30; i++) {
        await pg.query(`INSERT INTO orders (amount) VALUES ($1)`, [i * 10]);
        await pg.query(`INSERT INTO products (name) VALUES ($1)`, [`prod-${i}`]);
        await pg.query(`INSERT INTO customers (email) VALUES ($1)`, [
          `user${i}@test.com`,
        ]);
      }

      const oc = await pg.query(`SELECT count(*) AS c FROM orders`);
      const pc = await pg.query(`SELECT count(*) AS c FROM products`);
      const cc = await pg.query(`SELECT count(*) AS c FROM customers`);
      expect(Number(oc.rows[0].c)).toBe(30);
      expect(Number(pc.rows[0].c)).toBe(30);
      expect(Number(cc.rows[0].c)).toBe(30);

      const om = await pg.query(`SELECT max(id) AS m FROM orders`);
      const pm = await pg.query(`SELECT max(id) AS m FROM products`);
      const cm = await pg.query(`SELECT max(id) AS m FROM customers`);
      expect(Number(om.rows[0].m)).toBe(30);
      expect(Number(pm.rows[0].m)).toBe(30);
      expect(Number(cm.rows[0].m)).toBe(30);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 4: Sequence + persistence roundtrip
// ---------------------------------------------------------------------------

describe("Sequence values survive persistence roundtrip", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const backend = new SyncMemoryBackend();

      const h1 = await create(size, backend);
      await h1.pg.query(`CREATE TABLE persisted (id SERIAL PRIMARY KEY, val TEXT)`);
      for (let i = 0; i < 20; i++) {
        await h1.pg.query(`INSERT INTO persisted (val) VALUES ($1)`, [`v${i}`]);
      }
      await h1.syncToFs();
      await h1.destroy();
      harnesses = [];

      const h2 = await create(size, backend);
      const count = await h2.pg.query(`SELECT count(*) AS c FROM persisted`);
      expect(Number(count.rows[0].c)).toBe(20);

      await h2.pg.query(`INSERT INTO persisted (val) VALUES ('after-remount')`);
      const newRow = await h2.pg.query(
        `SELECT id FROM persisted WHERE val = 'after-remount'`,
      );
      expect(Number(newRow.rows[0].id)).toBeGreaterThan(20);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 5: ALTER SEQUENCE RESTART under cache pressure
// ---------------------------------------------------------------------------

describe("ALTER SEQUENCE RESTART under cache pressure", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`CREATE SEQUENCE resettable START 100`);

      for (let i = 0; i < 10; i++) {
        await pg.query(`SELECT nextval('resettable')`);
      }
      const before = await pg.query(`SELECT nextval('resettable') AS v`);
      expect(Number(before.rows[0].v)).toBe(110);

      await pg.query(`ALTER SEQUENCE resettable RESTART WITH 1`);
      const after = await pg.query(`SELECT nextval('resettable') AS v`);
      expect(Number(after.rows[0].v)).toBe(1);

      for (let i = 0; i < 5; i++) {
        const r = await pg.query(`SELECT nextval('resettable') AS v`);
        expect(Number(r.rows[0].v)).toBe(i + 2);
      }
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 6: setval + nextval interaction
// ---------------------------------------------------------------------------

describe("setval + nextval interaction under cache pressure", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`CREATE SEQUENCE sv_seq`);

      for (let i = 0; i < 5; i++) {
        await pg.query(`SELECT nextval('sv_seq')`);
      }

      await pg.query(`SELECT setval('sv_seq', 1000)`);
      const r1 = await pg.query(`SELECT nextval('sv_seq') AS v`);
      expect(Number(r1.rows[0].v)).toBe(1001);

      await pg.query(`SELECT setval('sv_seq', 500, false)`);
      const r2 = await pg.query(`SELECT nextval('sv_seq') AS v`);
      expect(Number(r2.rows[0].v)).toBe(500);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 7: SERIAL with ON CONFLICT upsert
// ---------------------------------------------------------------------------

describe("SERIAL with ON CONFLICT upsert", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`CREATE TABLE upserts (
        key TEXT PRIMARY KEY,
        id SERIAL,
        counter INT DEFAULT 1
      )`);

      for (let i = 0; i < 30; i++) {
        const key = `k-${i % 10}`;
        await pg.query(
          `INSERT INTO upserts (key) VALUES ($1)
           ON CONFLICT (key) DO UPDATE SET counter = upserts.counter + 1`,
          [key],
        );
      }

      const result = await pg.query(
        `SELECT key, counter FROM upserts ORDER BY key`,
      );
      expect(result.rows.length).toBe(10);
      for (const row of result.rows) {
        expect(Number(row.counter)).toBe(3);
      }
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 8: BIGSERIAL with large values
// ---------------------------------------------------------------------------

describe("BIGSERIAL with large ID values", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`CREATE SEQUENCE big_seq AS BIGINT START WITH 9000000000`);
      await pg.query(`CREATE TABLE big_items (
        id BIGINT DEFAULT nextval('big_seq') PRIMARY KEY,
        data TEXT
      )`);

      for (let i = 0; i < 20; i++) {
        await pg.query(`INSERT INTO big_items (data) VALUES ($1)`, [`d${i}`]);
      }

      const result = await pg.query(
        `SELECT min(id) AS lo, max(id) AS hi, count(*) AS c FROM big_items`,
      );
      expect(Number(result.rows[0].c)).toBe(20);
      expect(Number(result.rows[0].lo)).toBe(9000000000);
      expect(Number(result.rows[0].hi)).toBe(9000000019);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 9: Sequence gaps from failed inserts
// ---------------------------------------------------------------------------

describe("Sequence gaps from constraint violations", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`CREATE TABLE strict (
        id SERIAL PRIMARY KEY,
        val INT UNIQUE NOT NULL
      )`);

      let successes = 0;
      for (let i = 0; i < 30; i++) {
        try {
          await pg.query(`INSERT INTO strict (val) VALUES ($1)`, [i % 15]);
          successes++;
        } catch {
          // duplicate val — expected, sequence still advances
        }
      }

      expect(successes).toBe(15);
      const maxId = await pg.query(`SELECT max(id) AS m FROM strict`);
      expect(Number(maxId.rows[0].m)).toBeGreaterThanOrEqual(15);

      const count = await pg.query(`SELECT count(*) AS c FROM strict`);
      expect(Number(count.rows[0].c)).toBe(15);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 10: Sequence + DROP + recreate cycle
// ---------------------------------------------------------------------------

describe("DROP and recreate sequence cycle", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      for (let cycle = 0; cycle < 5; cycle++) {
        await pg.query(`CREATE SEQUENCE cycle_seq START WITH ${cycle * 100 + 1}`);

        const vals: number[] = [];
        for (let i = 0; i < 10; i++) {
          const r = await pg.query(`SELECT nextval('cycle_seq') AS v`);
          vals.push(Number(r.rows[0].v));
        }

        for (let i = 0; i < 10; i++) {
          expect(vals[i]).toBe(cycle * 100 + 1 + i);
        }

        await pg.query(`DROP SEQUENCE cycle_seq`);
      }
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 11: Multiple SERIAL tables + generate_series bulk load
// ---------------------------------------------------------------------------

describe("SERIAL tables with generate_series bulk load", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`CREATE TABLE gs_a (id SERIAL PRIMARY KEY, n INT)`);
      await pg.query(`CREATE TABLE gs_b (id SERIAL PRIMARY KEY, n INT)`);

      await pg.query(
        `INSERT INTO gs_a (n) SELECT g FROM generate_series(1, 50) g`,
      );
      await pg.query(
        `INSERT INTO gs_b (n) SELECT g FROM generate_series(1, 50) g`,
      );

      const ac = await pg.query(`SELECT count(*) AS c, max(id) AS m FROM gs_a`);
      const bc = await pg.query(`SELECT count(*) AS c, max(id) AS m FROM gs_b`);
      expect(Number(ac.rows[0].c)).toBe(50);
      expect(Number(ac.rows[0].m)).toBe(50);
      expect(Number(bc.rows[0].c)).toBe(50);
      expect(Number(bc.rows[0].m)).toBe(50);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 12: Sequence persistence across dirty shutdown + WAL replay
// ---------------------------------------------------------------------------

describe("Sequence survives dirty shutdown + WAL replay", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages)`, async () => {
      const backend = new SyncMemoryBackend();

      const h1 = await create(size, backend);
      await h1.pg.query(`CREATE TABLE wal_seq (id SERIAL PRIMARY KEY, data TEXT)`);
      for (let i = 0; i < 15; i++) {
        await h1.pg.query(`INSERT INTO wal_seq (data) VALUES ($1)`, [`r${i}`]);
      }
      await h1.syncToFs();

      for (let i = 15; i < 25; i++) {
        await h1.pg.query(`INSERT INTO wal_seq (data) VALUES ($1)`, [`r${i}`]);
      }
      h1.dirtyDestroy();
      harnesses = [];

      const h2 = await create(size, backend);
      const synced = await h2.pg.query(`SELECT count(*) AS c FROM wal_seq`);
      const syncedCount = Number(synced.rows[0].c);
      expect(syncedCount).toBeGreaterThanOrEqual(15);

      await h2.pg.query(`INSERT INTO wal_seq (data) VALUES ('post-recovery')`);
      const post = await h2.pg.query(
        `SELECT id FROM wal_seq WHERE data = 'post-recovery'`,
      );
      expect(Number(post.rows[0].id)).toBeGreaterThan(syncedCount);
    });
  }
});
