/**
 * Basic PGlite + tomefs integration tests.
 *
 * Validates that PGlite can run SQL operations through tomefs's
 * page-cached filesystem. This is the Phase 4 smoke test — proving
 * that Postgres works correctly on tomefs.
 */

import { describe, it, expect, afterEach } from "vitest";
import { createPGliteHarness, type PGliteHarness } from "./harness.js";

let harness: PGliteHarness | null = null;

afterEach(async () => {
  if (harness) {
    await harness.destroy();
    harness = null;
  }
});

describe("PGlite + tomefs basic integration @fast", () => {
  it("creates a table, inserts, and selects", async () => {
    harness = await createPGliteHarness("large");
    const { pg } = harness;

    await pg.query(`CREATE TABLE notes (id SERIAL PRIMARY KEY, title TEXT NOT NULL)`);
    await pg.query(`INSERT INTO notes (title) VALUES ('hello'), ('world')`);

    const result = await pg.query(`SELECT title FROM notes ORDER BY id`);
    expect(result.rows).toEqual([
      { title: "hello" },
      { title: "world" },
    ]);
  });

  it("handles multiple data types", async () => {
    harness = await createPGliteHarness("large");
    const { pg } = harness;

    await pg.query(`
      CREATE TABLE mixed (
        id SERIAL PRIMARY KEY,
        name TEXT,
        count INTEGER,
        score REAL,
        active BOOLEAN,
        data BYTEA,
        created_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    await pg.query(`
      INSERT INTO mixed (name, count, score, active, data)
      VALUES ('test', 42, 3.14, true, '\\x deadbeef')
    `);

    const result = await pg.query(`SELECT name, count, score, active FROM mixed`);
    expect(result.rows[0].name).toBe("test");
    expect(result.rows[0].count).toBe(42);
    expect(result.rows[0].active).toBe(true);
  });

  it("supports transactions", async () => {
    harness = await createPGliteHarness("large");
    const { pg } = harness;

    await pg.query(`CREATE TABLE accounts (id SERIAL PRIMARY KEY, balance INTEGER)`);
    await pg.query(`INSERT INTO accounts (balance) VALUES (100), (200)`);

    // Transaction that transfers between accounts
    await pg.query("BEGIN");
    await pg.query(`UPDATE accounts SET balance = balance - 50 WHERE id = 1`);
    await pg.query(`UPDATE accounts SET balance = balance + 50 WHERE id = 2`);
    await pg.query("COMMIT");

    const result = await pg.query(`SELECT balance FROM accounts ORDER BY id`);
    expect(result.rows).toEqual([{ balance: 50 }, { balance: 250 }]);
  });

  it("handles rollback correctly", async () => {
    harness = await createPGliteHarness("large");
    const { pg } = harness;

    await pg.query(`CREATE TABLE items (id SERIAL PRIMARY KEY, name TEXT)`);
    await pg.query(`INSERT INTO items (name) VALUES ('original')`);

    await pg.query("BEGIN");
    await pg.query(`INSERT INTO items (name) VALUES ('should_vanish')`);
    await pg.query("ROLLBACK");

    const result = await pg.query(`SELECT name FROM items`);
    expect(result.rows).toEqual([{ name: "original" }]);
  });

  it("supports indexes and queries using them", async () => {
    harness = await createPGliteHarness("large");
    const { pg } = harness;

    await pg.query(`CREATE TABLE users (id SERIAL PRIMARY KEY, email TEXT UNIQUE, name TEXT)`);
    await pg.query(`CREATE INDEX idx_users_name ON users (name)`);

    for (let i = 0; i < 20; i++) {
      await pg.query(`INSERT INTO users (email, name) VALUES ($1, $2)`, [
        `user${i}@test.com`,
        `User ${i}`,
      ]);
    }

    const result = await pg.query(`SELECT email FROM users WHERE name = 'User 10'`);
    expect(result.rows).toEqual([{ email: "user10@test.com" }]);
  });

  it("handles bulk inserts", async () => {
    harness = await createPGliteHarness("large");
    const { pg } = harness;

    await pg.query(`CREATE TABLE bulk (id SERIAL PRIMARY KEY, value TEXT)`);

    // Insert 100 rows
    for (let i = 0; i < 100; i++) {
      await pg.query(`INSERT INTO bulk (value) VALUES ($1)`, [`row-${i}`]);
    }

    const result = await pg.query(`SELECT COUNT(*)::int as count FROM bulk`);
    expect(result.rows[0].count).toBe(100);

    // Verify ordering
    const first = await pg.query(`SELECT value FROM bulk ORDER BY id LIMIT 1`);
    expect(first.rows[0].value).toBe("row-0");

    const last = await pg.query(`SELECT value FROM bulk ORDER BY id DESC LIMIT 1`);
    expect(last.rows[0].value).toBe("row-99");
  });

  it("supports JSON operations", async () => {
    harness = await createPGliteHarness("large");
    const { pg } = harness;

    await pg.query(`CREATE TABLE docs (id SERIAL PRIMARY KEY, data JSONB)`);
    await pg.query(`INSERT INTO docs (data) VALUES ($1)`, [
      JSON.stringify({ title: "hello", tags: ["a", "b"], nested: { x: 1 } }),
    ]);

    const result = await pg.query(`SELECT data->>'title' as title, data->'nested'->>'x' as x FROM docs`);
    expect(result.rows[0].title).toBe("hello");
    expect(result.rows[0].x).toBe("1");
  });

  it("supports full-text search", async () => {
    harness = await createPGliteHarness("large");
    const { pg } = harness;

    await pg.query(`
      CREATE TABLE articles (
        id SERIAL PRIMARY KEY,
        title TEXT,
        body TEXT,
        tsv TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', title || ' ' || body)) STORED
      )
    `);
    await pg.query(`CREATE INDEX idx_articles_tsv ON articles USING GIN (tsv)`);

    await pg.query(`INSERT INTO articles (title, body) VALUES ('Postgres on WASM', 'Running a database in the browser is now possible')`);
    await pg.query(`INSERT INTO articles (title, body) VALUES ('Recipe for Cake', 'Mix flour and sugar together')`);

    const result = await pg.query(`SELECT title FROM articles WHERE tsv @@ to_tsquery('english', 'browser')`);
    expect(result.rows).toEqual([{ title: "Postgres on WASM" }]);
  });
});
