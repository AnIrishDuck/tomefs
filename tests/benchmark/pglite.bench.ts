/**
 * PGlite-level performance benchmarks: tomefs vs MemoryFS.
 *
 * Ethos §6 requires performance parity when the working set fits in cache.
 * tests/benchmark/throughput.bench.ts measures raw FS overhead; this file
 * measures the end-to-end impact on actual SQL workloads through PGlite.
 *
 * This is what users care about: does switching from MemoryFS to tomefs
 * make their queries slower?
 *
 * Run: npx vitest bench tests/benchmark/pglite.bench.ts
 */

import { bench, describe, beforeAll, afterAll } from "vitest";
import { createTomeFSPGlite } from "../../src/pglite-fs.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";

// ---------------------------------------------------------------------------
// Harness: PGlite on MemoryFS (baseline) vs tomefs
// ---------------------------------------------------------------------------

interface PGBenchHarness {
  pg: any;
  label: string;
  destroy(): Promise<void>;
}

async function createMemFSPGlite(): Promise<PGBenchHarness> {
  const { PGlite } = await import("@electric-sql/pglite");
  const pg = new PGlite();
  await pg.waitReady;
  return {
    pg,
    label: "memfs",
    async destroy() {
      await pg.close();
    },
  };
}

async function createTomeFSPGliteHarness(
  maxPages: number,
): Promise<PGBenchHarness> {
  const { PGlite, MemoryFS } = await import("@electric-sql/pglite");
  const backend = new SyncMemoryBackend();
  const adapter = createTomeFSPGlite({ MemoryFS, backend, maxPages });
  const pg = new PGlite({ fs: adapter });
  await pg.waitReady;
  return {
    pg,
    label: `tomefs-${maxPages}`,
    async destroy() {
      await pg.close();
    },
  };
}

// ---------------------------------------------------------------------------
// Benchmark 1: Bulk INSERT throughput
// ---------------------------------------------------------------------------

describe("Bulk INSERT (100 rows)", async () => {
  const memfs = await createMemFSPGlite();
  const tome = await createTomeFSPGliteHarness(4096);

  // Setup tables once
  for (const h of [memfs, tome]) {
    await h.pg.query(
      `CREATE TABLE IF NOT EXISTS bench_insert (id SERIAL PRIMARY KEY, payload TEXT)`,
    );
  }

  let memCounter = 0;
  let tomeCounter = 0;

  bench("MemoryFS", async () => {
    const batch = ++memCounter;
    for (let i = 0; i < 100; i++) {
      await memfs.pg.query(`INSERT INTO bench_insert (payload) VALUES ($1)`, [
        `row-${batch}-${i}-${"x".repeat(200)}`,
      ]);
    }
  });

  bench("tomefs (4096 pages)", async () => {
    const batch = ++tomeCounter;
    for (let i = 0; i < 100; i++) {
      await tome.pg.query(`INSERT INTO bench_insert (payload) VALUES ($1)`, [
        `row-${batch}-${i}-${"x".repeat(200)}`,
      ]);
    }
  });

  afterAll(async () => {
    await memfs.destroy();
    await tome.destroy();
  });
});

// ---------------------------------------------------------------------------
// Benchmark 2: Point SELECT by primary key
// ---------------------------------------------------------------------------

describe("Point SELECT by PK (100 lookups)", async () => {
  const memfs = await createMemFSPGlite();
  const tome = await createTomeFSPGliteHarness(4096);

  // Seed data
  for (const h of [memfs, tome]) {
    await h.pg.query(
      `CREATE TABLE bench_select (id SERIAL PRIMARY KEY, value TEXT)`,
    );
    for (let i = 0; i < 200; i++) {
      await h.pg.query(`INSERT INTO bench_select (value) VALUES ($1)`, [
        `value-${i}-${"d".repeat(100)}`,
      ]);
    }
  }

  // Deterministic lookup pattern
  const ids = Array.from({ length: 100 }, (_, i) => ((i * 37 + 13) % 200) + 1);

  bench("MemoryFS", async () => {
    for (const id of ids) {
      await memfs.pg.query(`SELECT value FROM bench_select WHERE id = $1`, [
        id,
      ]);
    }
  });

  bench("tomefs (4096 pages)", async () => {
    for (const id of ids) {
      await tome.pg.query(`SELECT value FROM bench_select WHERE id = $1`, [
        id,
      ]);
    }
  });

  afterAll(async () => {
    await memfs.destroy();
    await tome.destroy();
  });
});

// ---------------------------------------------------------------------------
// Benchmark 3: Full table scan with aggregation
// ---------------------------------------------------------------------------

describe("Full Scan + Aggregation (1000 rows)", async () => {
  const memfs = await createMemFSPGlite();
  const tome = await createTomeFSPGliteHarness(4096);

  // Seed data
  for (const h of [memfs, tome]) {
    await h.pg.query(
      `CREATE TABLE bench_scan (id SERIAL PRIMARY KEY, category INT, amount REAL)`,
    );
    for (let i = 0; i < 1000; i++) {
      await h.pg.query(
        `INSERT INTO bench_scan (category, amount) VALUES ($1, $2)`,
        [i % 10, Math.random() * 1000],
      );
    }
    await h.pg.query(`ANALYZE bench_scan`);
  }

  bench("MemoryFS", async () => {
    await memfs.pg.query(`
      SELECT category, COUNT(*)::int as cnt, AVG(amount)::float as avg_amt, SUM(amount)::float as total
      FROM bench_scan GROUP BY category ORDER BY category
    `);
  });

  bench("tomefs (4096 pages)", async () => {
    await tome.pg.query(`
      SELECT category, COUNT(*)::int as cnt, AVG(amount)::float as avg_amt, SUM(amount)::float as total
      FROM bench_scan GROUP BY category ORDER BY category
    `);
  });

  afterAll(async () => {
    await memfs.destroy();
    await tome.destroy();
  });
});

// ---------------------------------------------------------------------------
// Benchmark 4: UPDATE storm (simulating counter increments)
// ---------------------------------------------------------------------------

describe("UPDATE Storm (200 updates on 10 rows)", async () => {
  const memfs = await createMemFSPGlite();
  const tome = await createTomeFSPGliteHarness(4096);

  for (const h of [memfs, tome]) {
    await h.pg.query(
      `CREATE TABLE bench_update (id SERIAL PRIMARY KEY, counter INT DEFAULT 0)`,
    );
    for (let i = 0; i < 10; i++) {
      await h.pg.query(`INSERT INTO bench_update (counter) VALUES (0)`);
    }
  }

  bench("MemoryFS", async () => {
    for (let i = 0; i < 200; i++) {
      const id = (i % 10) + 1;
      await memfs.pg.query(
        `UPDATE bench_update SET counter = counter + 1 WHERE id = $1`,
        [id],
      );
    }
  });

  bench("tomefs (4096 pages)", async () => {
    for (let i = 0; i < 200; i++) {
      const id = (i % 10) + 1;
      await tome.pg.query(
        `UPDATE bench_update SET counter = counter + 1 WHERE id = $1`,
        [id],
      );
    }
  });

  afterAll(async () => {
    await memfs.destroy();
    await tome.destroy();
  });
});

// ---------------------------------------------------------------------------
// Benchmark 5: Multi-table JOIN
// ---------------------------------------------------------------------------

describe("3-Table JOIN (authors/books/reviews)", async () => {
  const memfs = await createMemFSPGlite();
  const tome = await createTomeFSPGliteHarness(4096);

  for (const h of [memfs, tome]) {
    await h.pg.query(`CREATE TABLE bench_authors (id SERIAL PRIMARY KEY, name TEXT)`);
    await h.pg.query(`CREATE TABLE bench_books (id SERIAL PRIMARY KEY, author_id INT REFERENCES bench_authors(id), title TEXT)`);
    await h.pg.query(`CREATE TABLE bench_reviews (id SERIAL PRIMARY KEY, book_id INT REFERENCES bench_books(id), rating INT)`);

    for (let a = 0; a < 20; a++) {
      await h.pg.query(`INSERT INTO bench_authors (name) VALUES ($1)`, [
        `Author ${a}`,
      ]);
    }
    for (let a = 1; a <= 20; a++) {
      for (let b = 0; b < 5; b++) {
        await h.pg.query(
          `INSERT INTO bench_books (author_id, title) VALUES ($1, $2)`,
          [a, `Book ${a}-${b}`],
        );
      }
    }
    for (let b = 1; b <= 100; b++) {
      for (let r = 0; r < 3; r++) {
        await h.pg.query(
          `INSERT INTO bench_reviews (book_id, rating) VALUES ($1, $2)`,
          [b, 1 + (r % 5)],
        );
      }
    }
    await h.pg.query(`ANALYZE bench_authors`);
    await h.pg.query(`ANALYZE bench_books`);
    await h.pg.query(`ANALYZE bench_reviews`);
  }

  bench("MemoryFS", async () => {
    await memfs.pg.query(`
      SELECT a.name, COUNT(r.id)::int as review_count, AVG(r.rating)::float as avg_rating
      FROM bench_authors a
      JOIN bench_books b ON b.author_id = a.id
      JOIN bench_reviews r ON r.book_id = b.id
      GROUP BY a.id, a.name
      ORDER BY avg_rating DESC
    `);
  });

  bench("tomefs (4096 pages)", async () => {
    await tome.pg.query(`
      SELECT a.name, COUNT(r.id)::int as review_count, AVG(r.rating)::float as avg_rating
      FROM bench_authors a
      JOIN bench_books b ON b.author_id = a.id
      JOIN bench_reviews r ON r.book_id = b.id
      GROUP BY a.id, a.name
      ORDER BY avg_rating DESC
    `);
  });

  afterAll(async () => {
    await memfs.destroy();
    await tome.destroy();
  });
});

// ---------------------------------------------------------------------------
// Benchmark 6: Transaction throughput (BEGIN/INSERT/COMMIT cycles)
// ---------------------------------------------------------------------------

describe("Transaction Throughput (50 BEGIN/INSERT/COMMIT cycles)", async () => {
  const memfs = await createMemFSPGlite();
  const tome = await createTomeFSPGliteHarness(4096);

  for (const h of [memfs, tome]) {
    await h.pg.query(
      `CREATE TABLE bench_txn (id SERIAL PRIMARY KEY, data TEXT)`,
    );
  }

  let memTxnCounter = 0;
  let tomeTxnCounter = 0;

  bench("MemoryFS", async () => {
    const batch = ++memTxnCounter;
    for (let i = 0; i < 50; i++) {
      await memfs.pg.query("BEGIN");
      await memfs.pg.query(`INSERT INTO bench_txn (data) VALUES ($1)`, [
        `txn-${batch}-${i}`,
      ]);
      await memfs.pg.query("COMMIT");
    }
  });

  bench("tomefs (4096 pages)", async () => {
    const batch = ++tomeTxnCounter;
    for (let i = 0; i < 50; i++) {
      await tome.pg.query("BEGIN");
      await tome.pg.query(`INSERT INTO bench_txn (data) VALUES ($1)`, [
        `txn-${batch}-${i}`,
      ]);
      await tome.pg.query("COMMIT");
    }
  });

  afterAll(async () => {
    await memfs.destroy();
    await tome.destroy();
  });
});

// ---------------------------------------------------------------------------
// Benchmark 7: JSONB operations (insert + query)
// ---------------------------------------------------------------------------

describe("JSONB Insert + Query (50 docs, 10 queries)", async () => {
  const memfs = await createMemFSPGlite();
  const tome = await createTomeFSPGliteHarness(4096);

  for (const h of [memfs, tome]) {
    await h.pg.query(
      `CREATE TABLE bench_json (id SERIAL PRIMARY KEY, data JSONB)`,
    );
    await h.pg.query(`CREATE INDEX idx_bench_json ON bench_json USING GIN (data)`);
  }

  let memJsonCounter = 0;
  let tomeJsonCounter = 0;

  bench("MemoryFS", async () => {
    const batch = ++memJsonCounter;
    // Insert 50 JSONB docs
    for (let i = 0; i < 50; i++) {
      await memfs.pg.query(`INSERT INTO bench_json (data) VALUES ($1)`, [
        JSON.stringify({
          batch,
          index: i,
          tags: [`tag-${i % 5}`, `tag-${(i + 1) % 5}`],
          nested: { value: i * 10, label: `item-${i}` },
        }),
      ]);
    }
    // Query 10 times
    for (let q = 0; q < 10; q++) {
      await memfs.pg.query(
        `SELECT COUNT(*)::int FROM bench_json WHERE data @> $1`,
        [JSON.stringify({ tags: [`tag-${q % 5}`] })],
      );
    }
  });

  bench("tomefs (4096 pages)", async () => {
    const batch = ++tomeJsonCounter;
    for (let i = 0; i < 50; i++) {
      await tome.pg.query(`INSERT INTO bench_json (data) VALUES ($1)`, [
        JSON.stringify({
          batch,
          index: i,
          tags: [`tag-${i % 5}`, `tag-${(i + 1) % 5}`],
          nested: { value: i * 10, label: `item-${i}` },
        }),
      ]);
    }
    for (let q = 0; q < 10; q++) {
      await tome.pg.query(
        `SELECT COUNT(*)::int FROM bench_json WHERE data @> $1`,
        [JSON.stringify({ tags: [`tag-${q % 5}`] })],
      );
    }
  });

  afterAll(async () => {
    await memfs.destroy();
    await tome.destroy();
  });
});

// ---------------------------------------------------------------------------
// Benchmark 8: Index-heavy workload (CREATE INDEX + indexed queries)
// ---------------------------------------------------------------------------

describe("Indexed Queries (500 rows, 3 indexes, 50 queries)", async () => {
  const memfs = await createMemFSPGlite();
  const tome = await createTomeFSPGliteHarness(4096);

  for (const h of [memfs, tome]) {
    await h.pg.query(`
      CREATE TABLE bench_indexed (
        id SERIAL PRIMARY KEY,
        category INT,
        status TEXT,
        amount REAL,
        created_at TIMESTAMP DEFAULT NOW()
      )
    `);
    for (let i = 0; i < 500; i++) {
      await h.pg.query(
        `INSERT INTO bench_indexed (category, status, amount) VALUES ($1, $2, $3)`,
        [
          i % 10,
          ["active", "inactive", "pending"][i % 3],
          Math.random() * 10000,
        ],
      );
    }
    await h.pg.query(
      `CREATE INDEX idx_bench_cat ON bench_indexed (category)`,
    );
    await h.pg.query(
      `CREATE INDEX idx_bench_status ON bench_indexed (status)`,
    );
    await h.pg.query(
      `CREATE INDEX idx_bench_amount ON bench_indexed (amount)`,
    );
    await h.pg.query(`ANALYZE bench_indexed`);
  }

  bench("MemoryFS", async () => {
    for (let q = 0; q < 50; q++) {
      const cat = q % 10;
      const status = ["active", "inactive", "pending"][q % 3];
      await memfs.pg.query(
        `SELECT id, amount FROM bench_indexed WHERE category = $1 AND status = $2 ORDER BY amount DESC LIMIT 5`,
        [cat, status],
      );
    }
  });

  bench("tomefs (4096 pages)", async () => {
    for (let q = 0; q < 50; q++) {
      const cat = q % 10;
      const status = ["active", "inactive", "pending"][q % 3];
      await tome.pg.query(
        `SELECT id, amount FROM bench_indexed WHERE category = $1 AND status = $2 ORDER BY amount DESC LIMIT 5`,
        [cat, status],
      );
    }
  });

  afterAll(async () => {
    await memfs.destroy();
    await tome.destroy();
  });
});
