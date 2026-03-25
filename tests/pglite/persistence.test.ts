/**
 * PGlite + tomefs persistence round-trip tests.
 *
 * Validates that PGlite data survives syncToFs + remount cycles.
 * This is the core value proposition of tomefs over MEMFS: data persists
 * across page cache eviction and process restarts.
 *
 * Each test follows the pattern:
 *   1. Create PGlite on tomefs with a SyncMemoryBackend
 *   2. Run SQL operations (create tables, insert data)
 *   3. Call syncToFs (flushes page cache + persists metadata)
 *   4. Destroy PGlite
 *   5. Create a NEW PGlite on the SAME backend
 *   6. Verify all data survived the round-trip
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

describe("PGlite + tomefs persistence round-trip @fast", () => {
  it("persists a table with data across remount", async () => {
    const backend = new SyncMemoryBackend();

    // Phase 1: create and populate
    const h1 = await create("large", backend);
    await h1.pg.query(
      `CREATE TABLE notes (id SERIAL PRIMARY KEY, title TEXT NOT NULL)`,
    );
    await h1.pg.query(
      `INSERT INTO notes (title) VALUES ('alpha'), ('beta'), ('gamma')`,
    );
    await h1.syncToFs();
    await h1.destroy();

    // Phase 2: remount and verify
    const h2 = await create("large", backend);
    const result = await h2.pg.query(
      `SELECT title FROM notes ORDER BY id`,
    );
    expect(result.rows).toEqual([
      { title: "alpha" },
      { title: "beta" },
      { title: "gamma" },
    ]);
  });

  it("persists schema with indexes and constraints", async () => {
    const backend = new SyncMemoryBackend();

    const h1 = await create("large", backend);
    await h1.pg.query(`
      CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        email TEXT UNIQUE NOT NULL,
        name TEXT NOT NULL,
        active BOOLEAN DEFAULT true
      )
    `);
    await h1.pg.query(`CREATE INDEX idx_users_name ON users (name)`);
    await h1.pg.query(`
      INSERT INTO users (email, name) VALUES
        ('a@test.com', 'Alice'),
        ('b@test.com', 'Bob')
    `);
    await h1.syncToFs();
    await h1.destroy();

    const h2 = await create("large", backend);

    // Verify data
    const rows = await h2.pg.query(`SELECT email, name FROM users ORDER BY id`);
    expect(rows.rows).toEqual([
      { email: "a@test.com", name: "Alice" },
      { email: "b@test.com", name: "Bob" },
    ]);

    // Verify unique constraint survived
    await expect(
      h2.pg.query(`INSERT INTO users (email, name) VALUES ('a@test.com', 'Dup')`),
    ).rejects.toThrow();

    // Verify index is usable (query planner can use it)
    const indexed = await h2.pg.query(
      `SELECT email FROM users WHERE name = 'Alice'`,
    );
    expect(indexed.rows).toEqual([{ email: "a@test.com" }]);
  });

  it("persists multiple tables with foreign keys", async () => {
    const backend = new SyncMemoryBackend();

    const h1 = await create("large", backend);
    await h1.pg.query(`CREATE TABLE authors (id SERIAL PRIMARY KEY, name TEXT)`);
    await h1.pg.query(`
      CREATE TABLE books (
        id SERIAL PRIMARY KEY,
        author_id INTEGER REFERENCES authors(id),
        title TEXT
      )
    `);
    await h1.pg.query(`INSERT INTO authors (name) VALUES ('Tolkien'), ('Asimov')`);
    await h1.pg.query(`
      INSERT INTO books (author_id, title) VALUES
        (1, 'The Hobbit'), (1, 'The Silmarillion'),
        (2, 'Foundation'), (2, 'I, Robot')
    `);
    await h1.syncToFs();
    await h1.destroy();

    const h2 = await create("large", backend);

    // Verify join works
    const result = await h2.pg.query(`
      SELECT a.name, COUNT(b.id)::int as book_count
      FROM authors a JOIN books b ON b.author_id = a.id
      GROUP BY a.id, a.name ORDER BY a.name
    `);
    expect(result.rows).toEqual([
      { name: "Asimov", book_count: 2 },
      { name: "Tolkien", book_count: 2 },
    ]);

    // Verify FK constraint survived
    await expect(
      h2.pg.query(`INSERT INTO books (author_id, title) VALUES (999, 'Ghost')`),
    ).rejects.toThrow();
  });

  it("persists data across multiple sync cycles", async () => {
    const backend = new SyncMemoryBackend();

    // Cycle 1: create table and initial data
    const h1 = await create("large", backend);
    await h1.pg.query(`CREATE TABLE log (id SERIAL PRIMARY KEY, msg TEXT)`);
    await h1.pg.query(`INSERT INTO log (msg) VALUES ('first')`);
    await h1.syncToFs();
    await h1.destroy();

    // Cycle 2: add more data
    const h2 = await create("large", backend);
    const check1 = await h2.pg.query(`SELECT msg FROM log ORDER BY id`);
    expect(check1.rows).toEqual([{ msg: "first" }]);
    await h2.pg.query(`INSERT INTO log (msg) VALUES ('second')`);
    await h2.syncToFs();
    await h2.destroy();

    // Cycle 3: add more and verify all
    const h3 = await create("large", backend);
    await h3.pg.query(`INSERT INTO log (msg) VALUES ('third')`);
    await h3.syncToFs();
    await h3.destroy();

    // Cycle 4: final verification
    const h4 = await create("large", backend);
    const result = await h4.pg.query(`SELECT msg FROM log ORDER BY id`);
    expect(result.rows).toEqual([
      { msg: "first" },
      { msg: "second" },
      { msg: "third" },
    ]);
  });

  it("persists sequences correctly across remount", async () => {
    const backend = new SyncMemoryBackend();

    const h1 = await create("large", backend);
    await h1.pg.query(`CREATE TABLE items (id SERIAL PRIMARY KEY, name TEXT)`);
    await h1.pg.query(`INSERT INTO items (name) VALUES ('a'), ('b'), ('c')`);
    await h1.syncToFs();
    await h1.destroy();

    // After remount, SERIAL should continue from where it left off
    const h2 = await create("large", backend);
    await h2.pg.query(`INSERT INTO items (name) VALUES ('d')`);
    const result = await h2.pg.query(`SELECT id, name FROM items ORDER BY id`);
    expect(result.rows).toEqual([
      { id: 1, name: "a" },
      { id: 2, name: "b" },
      { id: 3, name: "c" },
      { id: 4, name: "d" },
    ]);
  });

  it("persists JSONB data", async () => {
    const backend = new SyncMemoryBackend();

    const h1 = await create("large", backend);
    await h1.pg.query(`CREATE TABLE docs (id SERIAL PRIMARY KEY, data JSONB)`);
    const doc = { title: "test", tags: ["a", "b"], nested: { x: 42 } };
    await h1.pg.query(`INSERT INTO docs (data) VALUES ($1)`, [
      JSON.stringify(doc),
    ]);
    await h1.syncToFs();
    await h1.destroy();

    const h2 = await create("large", backend);
    const result = await h2.pg.query(`SELECT data FROM docs`);
    expect(result.rows[0].data).toEqual(doc);
  });

  it("persists large text data (TOAST)", async () => {
    const backend = new SyncMemoryBackend();

    const h1 = await create("large", backend);
    await h1.pg.query(`CREATE TABLE blobs (id SERIAL PRIMARY KEY, content TEXT)`);
    // 16KB string — large enough to trigger TOAST storage
    const largeContent = "x".repeat(16000);
    await h1.pg.query(`INSERT INTO blobs (content) VALUES ($1)`, [largeContent]);
    await h1.syncToFs();
    await h1.destroy();

    const h2 = await create("large", backend);
    const result = await h2.pg.query(`SELECT LENGTH(content)::int as len FROM blobs`);
    expect(result.rows[0].len).toBe(16000);
  });
});

// ---------------------------------------------------------------------------
// Persistence under cache pressure
// ---------------------------------------------------------------------------

const PRESSURE_CONFIGS: CacheSize[] = ["tiny", "small", "medium", "large"];

describe("PGlite persistence under cache pressure", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`bulk insert + remount at cache=${size} (${pages} pages)`, async () => {
      const backend = new SyncMemoryBackend();

      const h1 = await create(size, backend);
      await h1.pg.query(`CREATE TABLE bulk (id SERIAL PRIMARY KEY, value TEXT)`);
      for (let i = 0; i < 50; i++) {
        await h1.pg.query(`INSERT INTO bulk (value) VALUES ($1)`, [
          `row-${i}-${"x".repeat(100)}`,
        ]);
      }
      await h1.syncToFs();
      await h1.destroy();

      const h2 = await create(size, backend);
      const count = await h2.pg.query(
        `SELECT COUNT(*)::int as count FROM bulk`,
      );
      expect(count.rows[0].count).toBe(50);

      // Verify ordering intact
      const first = await h2.pg.query(
        `SELECT value FROM bulk ORDER BY id LIMIT 1`,
      );
      expect(first.rows[0].value).toMatch(/^row-0-/);

      const last = await h2.pg.query(
        `SELECT value FROM bulk ORDER BY id DESC LIMIT 1`,
      );
      expect(last.rows[0].value).toMatch(/^row-49-/);
    });

    it(`update storm + remount at cache=${size} (${pages} pages)`, async () => {
      const backend = new SyncMemoryBackend();

      const h1 = await create(size, backend);
      await h1.pg.query(
        `CREATE TABLE counters (id SERIAL PRIMARY KEY, value INTEGER DEFAULT 0)`,
      );
      for (let i = 0; i < 10; i++) {
        await h1.pg.query(`INSERT INTO counters (value) VALUES (0)`);
      }
      // 100 updates total
      for (let round = 0; round < 10; round++) {
        for (let id = 1; id <= 10; id++) {
          await h1.pg.query(
            `UPDATE counters SET value = value + 1 WHERE id = $1`,
            [id],
          );
        }
      }
      await h1.syncToFs();
      await h1.destroy();

      const h2 = await create(size, backend);
      const result = await h2.pg.query(
        `SELECT id, value FROM counters ORDER BY id`,
      );
      for (const row of result.rows) {
        expect(row.value).toBe(10);
      }
    });
  }
});
