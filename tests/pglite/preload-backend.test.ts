/**
 * PGlite + tomefs + PreloadBackend — graceful degradation path.
 *
 * Validates that PGlite SQL operations work correctly through the
 * PreloadBackend (no SharedArrayBuffer required). This is the production
 * deployment path for environments without COOP/COEP headers (ethos §10).
 *
 * Unlike the SyncMemoryBackend tests, PreloadBackend has two-phase
 * persistence: syncToFs flushes dirty pages to PreloadBackend's in-memory
 * store, then flush() persists to the async remote (MemoryBackend/IDB).
 * These tests verify the complete round-trip at the SQL level.
 */

import { describe, it, expect, afterEach } from "vitest";
import { createTomeFSPGlite } from "../../src/pglite-fs.js";
import { MemoryBackend } from "../../src/memory-backend.js";
import { IdbBackend } from "../../src/idb-backend.js";
import { PreloadBackend } from "../../src/preload-backend.js";
import "fake-indexeddb/auto";

interface PreloadHarness {
  pg: any;
  adapter: any;
  backend: PreloadBackend;
  syncToFs(): Promise<void>;
  /** Flush PreloadBackend's dirty data to the async remote. */
  flush(): Promise<void>;
  destroy(): Promise<void>;
}

let harnesses: PreloadHarness[] = [];

afterEach(async () => {
  for (const h of harnesses) {
    await h.destroy();
  }
  harnesses = [];
});

async function createPreloadHarness(
  remote: MemoryBackend | IdbBackend,
  maxPages = 4096,
): Promise<PreloadHarness> {
  const backend = new PreloadBackend(remote);
  await backend.init();

  const { PGlite, MemoryFS } = await import("@electric-sql/pglite");
  const adapter = createTomeFSPGlite({ MemoryFS, backend, maxPages });
  const pg = new PGlite({ fs: adapter });
  await pg.waitReady;

  const h: PreloadHarness = {
    pg,
    adapter,
    backend,
    async syncToFs() {
      await adapter.syncToFs();
    },
    async flush() {
      await backend.flush();
    },
    async destroy() {
      try {
        await pg.close();
      } catch (_e) {
        // Ignore "PGlite is closed" errors
      }
    },
  };
  harnesses.push(h);
  return h;
}

// ---------------------------------------------------------------------------
// Basic SQL through PreloadBackend
// ---------------------------------------------------------------------------

describe("PGlite + PreloadBackend (no SAB) @fast", () => {
  it("basic SQL operations through PreloadBackend", async () => {
    const remote = new MemoryBackend();
    const h = await createPreloadHarness(remote);

    await h.pg.query(
      `CREATE TABLE items (id SERIAL PRIMARY KEY, name TEXT NOT NULL)`,
    );
    await h.pg.query(
      `INSERT INTO items (name) VALUES ('alpha'), ('beta'), ('gamma')`,
    );

    const result = await h.pg.query(`SELECT name FROM items ORDER BY id`);
    expect(result.rows).toEqual([
      { name: "alpha" },
      { name: "beta" },
      { name: "gamma" },
    ]);
  });

  it("transactions work through PreloadBackend", async () => {
    const remote = new MemoryBackend();
    const h = await createPreloadHarness(remote);

    await h.pg.query(`CREATE TABLE ledger (id SERIAL PRIMARY KEY, amount INT)`);
    await h.pg.query(`INSERT INTO ledger (amount) VALUES (100)`);

    // Committed transaction
    await h.pg.query("BEGIN");
    await h.pg.query(`UPDATE ledger SET amount = 200 WHERE id = 1`);
    await h.pg.query("COMMIT");

    // Rolled back transaction
    await h.pg.query("BEGIN");
    await h.pg.query(`UPDATE ledger SET amount = 999 WHERE id = 1`);
    await h.pg.query("ROLLBACK");

    const result = await h.pg.query(`SELECT amount FROM ledger WHERE id = 1`);
    expect(result.rows[0].amount).toBe(200);
  });
});

// ---------------------------------------------------------------------------
// Persistence round-trip through PreloadBackend + MemoryBackend
// ---------------------------------------------------------------------------

describe("PGlite + PreloadBackend persistence (MemoryBackend remote)", () => {
  it("data survives syncToFs + flush + remount", async () => {
    const remote = new MemoryBackend();

    // Session 1: create data
    const h1 = await createPreloadHarness(remote);
    await h1.pg.query(
      `CREATE TABLE notes (id SERIAL PRIMARY KEY, title TEXT)`,
    );
    await h1.pg.query(
      `INSERT INTO notes (title) VALUES ('first'), ('second'), ('third')`,
    );
    await h1.syncToFs();
    await h1.flush();
    await h1.destroy();

    // Session 2: verify data survived
    const h2 = await createPreloadHarness(remote);
    const result = await h2.pg.query(`SELECT title FROM notes ORDER BY id`);
    expect(result.rows).toEqual([
      { title: "first" },
      { title: "second" },
      { title: "third" },
    ]);
  });

  it("schema with indexes persists through PreloadBackend", async () => {
    const remote = new MemoryBackend();

    const h1 = await createPreloadHarness(remote);
    await h1.pg.query(`
      CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        email TEXT UNIQUE NOT NULL,
        name TEXT NOT NULL
      )
    `);
    await h1.pg.query(`CREATE INDEX idx_users_name ON users (name)`);
    await h1.pg.query(`
      INSERT INTO users (email, name) VALUES
        ('a@test.com', 'Alice'),
        ('b@test.com', 'Bob')
    `);
    await h1.syncToFs();
    await h1.flush();
    await h1.destroy();

    const h2 = await createPreloadHarness(remote);
    const rows = await h2.pg.query(
      `SELECT email, name FROM users ORDER BY id`,
    );
    expect(rows.rows).toEqual([
      { email: "a@test.com", name: "Alice" },
      { email: "b@test.com", name: "Bob" },
    ]);

    // Unique constraint survived
    await expect(
      h2.pg.query(
        `INSERT INTO users (email, name) VALUES ('a@test.com', 'Dup')`,
      ),
    ).rejects.toThrow();
  });

  it("multiple sync cycles through PreloadBackend", async () => {
    const remote = new MemoryBackend();

    // Cycle 1
    const h1 = await createPreloadHarness(remote);
    await h1.pg.query(`CREATE TABLE log (id SERIAL PRIMARY KEY, msg TEXT)`);
    await h1.pg.query(`INSERT INTO log (msg) VALUES ('one')`);
    await h1.syncToFs();
    await h1.flush();
    await h1.destroy();

    // Cycle 2
    const h2 = await createPreloadHarness(remote);
    await h2.pg.query(`INSERT INTO log (msg) VALUES ('two')`);
    await h2.syncToFs();
    await h2.flush();
    await h2.destroy();

    // Cycle 3: verify all
    const h3 = await createPreloadHarness(remote);
    const result = await h3.pg.query(`SELECT msg FROM log ORDER BY id`);
    expect(result.rows).toEqual([{ msg: "one" }, { msg: "two" }]);

    // Sequence continues (IDs increase but may not be consecutive across remounts)
    await h3.pg.query(`INSERT INTO log (msg) VALUES ('three')`);
    const all = await h3.pg.query(`SELECT msg FROM log ORDER BY id`);
    expect(all.rows.map((r: any) => r.msg)).toEqual(["one", "two", "three"]);
  });

  it("JSONB and TOAST data persists through PreloadBackend", async () => {
    const remote = new MemoryBackend();

    const h1 = await createPreloadHarness(remote);
    await h1.pg.query(`CREATE TABLE docs (id SERIAL PRIMARY KEY, data JSONB)`);
    const doc = { tags: ["a", "b"], nested: { x: 42 } };
    await h1.pg.query(`INSERT INTO docs (data) VALUES ($1)`, [
      JSON.stringify(doc),
    ]);
    // Large text to trigger TOAST
    await h1.pg.query(
      `CREATE TABLE blobs (id SERIAL PRIMARY KEY, content TEXT)`,
    );
    await h1.pg.query(`INSERT INTO blobs (content) VALUES ($1)`, [
      "x".repeat(16000),
    ]);
    await h1.syncToFs();
    await h1.flush();
    await h1.destroy();

    const h2 = await createPreloadHarness(remote);
    const jsonResult = await h2.pg.query(`SELECT data FROM docs`);
    expect(jsonResult.rows[0].data).toEqual(doc);

    const blobResult = await h2.pg.query(
      `SELECT LENGTH(content)::int as len FROM blobs`,
    );
    expect(blobResult.rows[0].len).toBe(16000);
  });
});

// ---------------------------------------------------------------------------
// Cache pressure with PreloadBackend
// ---------------------------------------------------------------------------

describe("PGlite + PreloadBackend under cache pressure", () => {
  it("bulk insert + read under tiny cache (4 pages)", async () => {
    const remote = new MemoryBackend();
    const h = await createPreloadHarness(remote, 4);

    await h.pg.query(
      `CREATE TABLE bulk (id SERIAL PRIMARY KEY, value TEXT)`,
    );
    for (let i = 0; i < 30; i++) {
      await h.pg.query(`INSERT INTO bulk (value) VALUES ($1)`, [
        `row-${i}-${"x".repeat(100)}`,
      ]);
    }

    const count = await h.pg.query(
      `SELECT COUNT(*)::int as count FROM bulk`,
    );
    expect(count.rows[0].count).toBe(30);
  });

  it("persistence round-trip under cache pressure", async () => {
    const remote = new MemoryBackend();

    // Write under pressure
    const h1 = await createPreloadHarness(remote, 8);
    await h1.pg.query(
      `CREATE TABLE pressure (id SERIAL PRIMARY KEY, data TEXT)`,
    );
    for (let i = 0; i < 20; i++) {
      await h1.pg.query(`INSERT INTO pressure (data) VALUES ($1)`, [
        `item-${i}-${"y".repeat(200)}`,
      ]);
    }
    await h1.syncToFs();
    await h1.flush();
    await h1.destroy();

    // Re-read under pressure
    const h2 = await createPreloadHarness(remote, 8);
    const count = await h2.pg.query(
      `SELECT COUNT(*)::int as count FROM pressure`,
    );
    expect(count.rows[0].count).toBe(20);

    const first = await h2.pg.query(
      `SELECT data FROM pressure ORDER BY id LIMIT 1`,
    );
    expect(first.rows[0].data).toMatch(/^item-0-/);
  });
});

// ---------------------------------------------------------------------------
// IdbBackend remote (full persistence stack)
// ---------------------------------------------------------------------------

describe("PGlite + PreloadBackend + IdbBackend", () => {
  it("@fast SQL data persists through IDB round-trip", async () => {
    const dbName = `pglite-preload-idb-${Date.now()}`;

    // Session 1: write through IDB-backed PreloadBackend
    const idb1 = new IdbBackend({ dbName });
    const h1 = await createPreloadHarness(idb1);
    await h1.pg.query(
      `CREATE TABLE products (id SERIAL PRIMARY KEY, name TEXT, price REAL)`,
    );
    await h1.pg.query(`
      INSERT INTO products (name, price) VALUES
        ('Widget', 9.99),
        ('Gadget', 24.50),
        ('Doohickey', 4.25)
    `);
    await h1.syncToFs();
    await h1.flush();
    await h1.destroy();

    // Session 2: fresh PGlite on same IDB
    const idb2 = new IdbBackend({ dbName });
    const h2 = await createPreloadHarness(idb2);
    const result = await h2.pg.query(
      `SELECT name, price FROM products ORDER BY id`,
    );
    expect(result.rows).toEqual([
      { name: "Widget", price: 9.99 },
      { name: "Gadget", price: 24.5 },
      { name: "Doohickey", price: 4.25 },
    ]);

    // Can continue writing
    await h2.pg.query(
      `INSERT INTO products (name, price) VALUES ('Thingamajig', 15.00)`,
    );
    const count = await h2.pg.query(
      `SELECT COUNT(*)::int as count FROM products`,
    );
    expect(count.rows[0].count).toBe(4);
  });
});

// ---------------------------------------------------------------------------
// Auto-flush: pglite-fs adapter automatically flushes PreloadBackend
// ---------------------------------------------------------------------------

describe("PGlite + PreloadBackend auto-flush (ethos §10)", () => {
  it("syncToFs() auto-flushes PreloadBackend to remote", async () => {
    const remote = new MemoryBackend();

    // Session 1: create data, syncToFs WITHOUT explicit flush()
    const h1 = await createPreloadHarness(remote);
    await h1.pg.query(`CREATE TABLE auto_flush (id SERIAL PRIMARY KEY, val TEXT)`);
    await h1.pg.query(`INSERT INTO auto_flush (val) VALUES ('persisted')`);
    await h1.syncToFs();
    // No explicit h1.flush() — the adapter should have auto-flushed

    // Verify data reached the remote backend
    const remoteFiles = await remote.listFiles();
    expect(remoteFiles.length).toBeGreaterThan(0);

    await h1.destroy();

    // Session 2: verify data survived remount
    const h2 = await createPreloadHarness(remote);
    const result = await h2.pg.query(`SELECT val FROM auto_flush`);
    expect(result.rows).toEqual([{ val: "persisted" }]);
  });

  it("syncToFs(true) with relaxed durability skips flush call", async () => {
    const remote = new MemoryBackend();

    const h = await createPreloadHarness(remote);
    await h.pg.query(`CREATE TABLE relaxed (id SERIAL PRIMARY KEY, val TEXT)`);

    // Track flush calls by wrapping the method
    let flushCallCount = 0;
    const originalFlush = h.backend.flush.bind(h.backend);
    h.backend.flush = async () => {
      flushCallCount++;
      return originalFlush();
    };

    // Relaxed durability: should NOT call flush on the backend
    flushCallCount = 0;
    await h.adapter.syncToFs(true);
    expect(flushCallCount).toBe(0);

    // Non-relaxed: should call flush
    flushCallCount = 0;
    await h.adapter.syncToFs(false);
    expect(flushCallCount).toBe(1);

    // Default (undefined): should call flush
    flushCallCount = 0;
    await h.adapter.syncToFs();
    expect(flushCallCount).toBe(1);
  });

  it("pg.close() auto-flushes PreloadBackend to remote", async () => {
    const remote = new MemoryBackend();

    // Session 1: create data, close WITHOUT explicit syncToFs or flush
    const h1 = await createPreloadHarness(remote);
    await h1.pg.query(`CREATE TABLE close_flush (id SERIAL PRIMARY KEY, val TEXT)`);
    await h1.pg.query(`INSERT INTO close_flush (val) VALUES ('durable')`);
    // pg.close() triggers closeFs which should syncfs + flush
    await h1.destroy();

    // Verify data reached the remote backend
    const remoteFiles = await remote.listFiles();
    expect(remoteFiles.length).toBeGreaterThan(0);

    // Session 2: verify data survived
    const h2 = await createPreloadHarness(remote);
    const result = await h2.pg.query(`SELECT val FROM close_flush`);
    expect(result.rows).toEqual([{ val: "durable" }]);
  });

  it("auto-flush persists multiple sync cycles without explicit flush", async () => {
    const remote = new MemoryBackend();

    // Cycle 1
    const h1 = await createPreloadHarness(remote);
    await h1.pg.query(`CREATE TABLE cycles (id SERIAL PRIMARY KEY, msg TEXT)`);
    await h1.pg.query(`INSERT INTO cycles (msg) VALUES ('one')`);
    await h1.syncToFs(); // auto-flushes
    await h1.destroy();

    // Cycle 2
    const h2 = await createPreloadHarness(remote);
    await h2.pg.query(`INSERT INTO cycles (msg) VALUES ('two')`);
    await h2.syncToFs(); // auto-flushes
    await h2.destroy();

    // Cycle 3: verify all data
    const h3 = await createPreloadHarness(remote);
    const result = await h3.pg.query(`SELECT msg FROM cycles ORDER BY id`);
    expect(result.rows).toEqual([{ msg: "one" }, { msg: "two" }]);
  });
});
