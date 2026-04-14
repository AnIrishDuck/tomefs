/**
 * PGlite-level performance benchmarks (part 1): tomefs vs MemoryFS.
 *
 * Ethos §6 requires performance parity when the working set fits in cache.
 * tests/benchmark/throughput.bench.ts measures raw FS overhead; this file
 * measures the end-to-end impact on actual SQL workloads through PGlite.
 *
 * Split across two files (pglite.bench.ts + pglite-query.bench.ts) to avoid
 * V8 WASM code space exhaustion — each PGlite instance compiles a Postgres
 * WASM module, and having too many compiled modules in a single process
 * exceeds the code space limit.
 *
 * Run: npx vitest bench tests/benchmark/pglite.bench.ts
 */

import { bench, describe, afterAll } from "vitest";
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
