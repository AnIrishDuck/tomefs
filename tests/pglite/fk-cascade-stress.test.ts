/**
 * PGlite + tomefs foreign key CASCADE and referential integrity stress tests.
 *
 * CASCADE operations trigger multi-table modifications from a single
 * statement: a DELETE on a parent table must scan child indexes to find
 * matching rows, then delete/update them. Under cache pressure, this
 * creates cross-file eviction cascades — the parent table's heap pages
 * compete with child index pages and child heap pages for cache slots.
 *
 * Key I/O patterns tested:
 *   - Cross-table read+write from a single DELETE/UPDATE statement
 *   - Index scans interleaved with heap modifications on different tables
 *   - Transaction atomicity across multi-table CASCADE modifications
 *   - Deep cascade chains (grandparent → parent → child)
 *   - Self-referential FK (tree structures)
 *   - ON CONFLICT (UPSERT) with FK constraints
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
// Scenario 1: CASCADE DELETE across parent-child tables
// ---------------------------------------------------------------------------

describe("CASCADE DELETE parent-child", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`CREATE TABLE parents (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
      )`);
      await pg.query(`CREATE TABLE children (
        id SERIAL PRIMARY KEY,
        parent_id INTEGER NOT NULL REFERENCES parents(id) ON DELETE CASCADE,
        value TEXT NOT NULL
      )`);

      for (let i = 0; i < 20; i++) {
        await pg.query(
          `INSERT INTO parents (name) VALUES ($1)`,
          [`parent-${i}-${"p".repeat(50)}`],
        );
      }

      for (let parentId = 1; parentId <= 20; parentId++) {
        for (let j = 0; j < 5; j++) {
          await pg.query(
            `INSERT INTO children (parent_id, value) VALUES ($1, $2)`,
            [parentId, `child-${parentId}-${j}-${"c".repeat(40)}`],
          );
        }
      }

      const beforeParents = await pg.query(
        `SELECT COUNT(*)::int AS n FROM parents`,
      );
      expect(beforeParents.rows[0].n).toBe(20);
      const beforeChildren = await pg.query(
        `SELECT COUNT(*)::int AS n FROM children`,
      );
      expect(beforeChildren.rows[0].n).toBe(100);

      // Delete half the parents — should cascade to their children
      await pg.query(`DELETE FROM parents WHERE id <= 10`);

      const afterParents = await pg.query(
        `SELECT COUNT(*)::int AS n FROM parents`,
      );
      expect(afterParents.rows[0].n).toBe(10);

      const afterChildren = await pg.query(
        `SELECT COUNT(*)::int AS n FROM children`,
      );
      expect(afterChildren.rows[0].n).toBe(50);

      // Verify only children of surviving parents remain
      const orphans = await pg.query(
        `SELECT COUNT(*)::int AS n FROM children WHERE parent_id <= 10`,
      );
      expect(orphans.rows[0].n).toBe(0);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 2: CASCADE UPDATE across parent-child tables
// ---------------------------------------------------------------------------

describe("CASCADE UPDATE parent-child", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`CREATE TABLE upd_parents (
        id INTEGER PRIMARY KEY,
        code TEXT NOT NULL UNIQUE
      )`);
      await pg.query(`CREATE TABLE upd_children (
        id SERIAL PRIMARY KEY,
        parent_code TEXT NOT NULL REFERENCES upd_parents(code) ON UPDATE CASCADE,
        data TEXT
      )`);

      for (let i = 1; i <= 15; i++) {
        await pg.query(
          `INSERT INTO upd_parents (id, code) VALUES ($1, $2)`,
          [i, `CODE-${i}`],
        );
      }

      for (let i = 1; i <= 15; i++) {
        for (let j = 0; j < 4; j++) {
          await pg.query(
            `INSERT INTO upd_children (parent_code, data) VALUES ($1, $2)`,
            [`CODE-${i}`, `data-${i}-${j}-${"d".repeat(30)}`],
          );
        }
      }

      // Update parent codes — children should follow via CASCADE
      await pg.query(
        `UPDATE upd_parents SET code = 'NEW-' || id WHERE id <= 5`,
      );

      // Verify children have updated references
      const updated = await pg.query(
        `SELECT COUNT(*)::int AS n FROM upd_children WHERE parent_code LIKE 'NEW-%'`,
      );
      expect(updated.rows[0].n).toBe(20);

      const unchanged = await pg.query(
        `SELECT COUNT(*)::int AS n FROM upd_children WHERE parent_code LIKE 'CODE-%'`,
      );
      expect(unchanged.rows[0].n).toBe(40);

      // Verify referential integrity
      const broken = await pg.query(`
        SELECT COUNT(*)::int AS n FROM upd_children c
        WHERE NOT EXISTS (SELECT 1 FROM upd_parents p WHERE p.code = c.parent_code)
      `);
      expect(broken.rows[0].n).toBe(0);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 3: Deep cascade chain (grandparent → parent → child)
// ---------------------------------------------------------------------------

describe("Deep 3-level cascade chain", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`CREATE TABLE level1 (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
      )`);
      await pg.query(`CREATE TABLE level2 (
        id SERIAL PRIMARY KEY,
        l1_id INTEGER NOT NULL REFERENCES level1(id) ON DELETE CASCADE,
        label TEXT NOT NULL
      )`);
      await pg.query(`CREATE TABLE level3 (
        id SERIAL PRIMARY KEY,
        l2_id INTEGER NOT NULL REFERENCES level2(id) ON DELETE CASCADE,
        payload TEXT NOT NULL
      )`);

      // 10 grandparents × 3 parents each × 4 children each = 120 leaf rows
      for (let g = 1; g <= 10; g++) {
        await pg.query(
          `INSERT INTO level1 (name) VALUES ($1)`,
          [`grand-${g}`],
        );
      }

      for (let g = 1; g <= 10; g++) {
        for (let p = 0; p < 3; p++) {
          await pg.query(
            `INSERT INTO level2 (l1_id, label) VALUES ($1, $2)`,
            [g, `parent-${g}-${p}-${"m".repeat(30)}`],
          );
        }
      }

      const l2Rows = await pg.query(`SELECT id, l1_id FROM level2 ORDER BY id`);
      for (const row of l2Rows.rows) {
        for (let c = 0; c < 4; c++) {
          await pg.query(
            `INSERT INTO level3 (l2_id, payload) VALUES ($1, $2)`,
            [row.id, `child-${row.id}-${c}-${"x".repeat(20)}`],
          );
        }
      }

      // Delete 5 grandparents — should cascade through parents to children
      await pg.query(`DELETE FROM level1 WHERE id <= 5`);

      const l1Count = await pg.query(
        `SELECT COUNT(*)::int AS n FROM level1`,
      );
      expect(l1Count.rows[0].n).toBe(5);

      const l2Count = await pg.query(
        `SELECT COUNT(*)::int AS n FROM level2`,
      );
      expect(l2Count.rows[0].n).toBe(15); // 5 grandparents × 3

      const l3Count = await pg.query(
        `SELECT COUNT(*)::int AS n FROM level3`,
      );
      expect(l3Count.rows[0].n).toBe(60); // 15 parents × 4
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 4: CASCADE DELETE with many matching child rows
// ---------------------------------------------------------------------------

describe("CASCADE DELETE with large child set", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages)`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`CREATE TABLE few_parents (
        id SERIAL PRIMARY KEY,
        category TEXT NOT NULL
      )`);
      await pg.query(`CREATE TABLE many_children (
        id SERIAL PRIMARY KEY,
        parent_id INTEGER NOT NULL REFERENCES few_parents(id) ON DELETE CASCADE,
        data TEXT NOT NULL
      )`);
      await pg.query(
        `CREATE INDEX idx_many_children_parent ON many_children(parent_id)`,
      );

      // 3 parents with 50 children each
      for (let p = 1; p <= 3; p++) {
        await pg.query(
          `INSERT INTO few_parents (category) VALUES ($1)`,
          [`cat-${p}`],
        );
      }

      for (let p = 1; p <= 3; p++) {
        await pg.query(`
          INSERT INTO many_children (parent_id, data)
          SELECT ${p}, 'row-' || i || '-${"r".repeat(60)}'
          FROM generate_series(1, 50) AS s(i)
        `);
      }

      const beforeTotal = await pg.query(
        `SELECT COUNT(*)::int AS n FROM many_children`,
      );
      expect(beforeTotal.rows[0].n).toBe(150);

      // Delete 1 parent — cascades to 50 children
      await pg.query(`DELETE FROM few_parents WHERE id = 1`);

      const afterTotal = await pg.query(
        `SELECT COUNT(*)::int AS n FROM many_children`,
      );
      expect(afterTotal.rows[0].n).toBe(100);

      // Verify integrity
      const orphans = await pg.query(
        `SELECT COUNT(*)::int AS n FROM many_children WHERE parent_id = 1`,
      );
      expect(orphans.rows[0].n).toBe(0);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 5: SET NULL on delete
// ---------------------------------------------------------------------------

describe("SET NULL on delete", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`CREATE TABLE sn_parents (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
      )`);
      await pg.query(`CREATE TABLE sn_children (
        id SERIAL PRIMARY KEY,
        parent_id INTEGER REFERENCES sn_parents(id) ON DELETE SET NULL,
        data TEXT NOT NULL
      )`);

      for (let i = 1; i <= 10; i++) {
        await pg.query(
          `INSERT INTO sn_parents (name) VALUES ($1)`,
          [`parent-${i}`],
        );
      }

      for (let p = 1; p <= 10; p++) {
        for (let c = 0; c < 3; c++) {
          await pg.query(
            `INSERT INTO sn_children (parent_id, data) VALUES ($1, $2)`,
            [p, `child-${p}-${c}-${"s".repeat(30)}`],
          );
        }
      }

      // Delete parents — children should have parent_id set to NULL
      await pg.query(`DELETE FROM sn_parents WHERE id <= 5`);

      const nulled = await pg.query(
        `SELECT COUNT(*)::int AS n FROM sn_children WHERE parent_id IS NULL`,
      );
      expect(nulled.rows[0].n).toBe(15);

      const intact = await pg.query(
        `SELECT COUNT(*)::int AS n FROM sn_children WHERE parent_id IS NOT NULL`,
      );
      expect(intact.rows[0].n).toBe(15);

      // All children still exist (not deleted)
      const total = await pg.query(
        `SELECT COUNT(*)::int AS n FROM sn_children`,
      );
      expect(total.rows[0].n).toBe(30);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 6: Multiple foreign keys on same table
// ---------------------------------------------------------------------------

describe("Multiple FKs on same child table", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`CREATE TABLE authors (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
      )`);
      await pg.query(`CREATE TABLE categories (
        id SERIAL PRIMARY KEY,
        label TEXT NOT NULL
      )`);
      await pg.query(`CREATE TABLE articles (
        id SERIAL PRIMARY KEY,
        author_id INTEGER NOT NULL REFERENCES authors(id) ON DELETE CASCADE,
        category_id INTEGER NOT NULL REFERENCES categories(id) ON DELETE CASCADE,
        title TEXT NOT NULL,
        body TEXT
      )`);

      for (let a = 1; a <= 8; a++) {
        await pg.query(
          `INSERT INTO authors (name) VALUES ($1)`,
          [`author-${a}`],
        );
      }
      for (let c = 1; c <= 4; c++) {
        await pg.query(
          `INSERT INTO categories (label) VALUES ($1)`,
          [`category-${c}`],
        );
      }

      // 8 authors × 4 categories = 32 articles
      for (let a = 1; a <= 8; a++) {
        for (let c = 1; c <= 4; c++) {
          await pg.query(
            `INSERT INTO articles (author_id, category_id, title, body) VALUES ($1, $2, $3, $4)`,
            [a, c, `title-${a}-${c}`, `body-${"b".repeat(40)}`],
          );
        }
      }

      const before = await pg.query(
        `SELECT COUNT(*)::int AS n FROM articles`,
      );
      expect(before.rows[0].n).toBe(32);

      // Delete an author — cascades to their 4 articles
      await pg.query(`DELETE FROM authors WHERE id = 1`);
      const afterAuthor = await pg.query(
        `SELECT COUNT(*)::int AS n FROM articles`,
      );
      expect(afterAuthor.rows[0].n).toBe(28);

      // Delete a category — cascades to articles in that category
      // (remaining: 7 authors × 1 category = 7 articles removed)
      await pg.query(`DELETE FROM categories WHERE id = 1`);
      const afterCat = await pg.query(
        `SELECT COUNT(*)::int AS n FROM articles`,
      );
      expect(afterCat.rows[0].n).toBe(21);

      // Verify referential integrity
      const broken = await pg.query(`
        SELECT COUNT(*)::int AS n FROM articles a
        WHERE NOT EXISTS (SELECT 1 FROM authors au WHERE au.id = a.author_id)
           OR NOT EXISTS (SELECT 1 FROM categories c WHERE c.id = a.category_id)
      `);
      expect(broken.rows[0].n).toBe(0);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 7: CASCADE DELETE + persistence round-trip
// ---------------------------------------------------------------------------

describe("CASCADE DELETE + persistence round-trip", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const backend = new SyncMemoryBackend();

      // Phase 1: create schema, populate, cascade delete, sync
      const h1 = await create(size, backend);
      await h1.pg.query(`CREATE TABLE p_parents (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
      )`);
      await h1.pg.query(`CREATE TABLE p_children (
        id SERIAL PRIMARY KEY,
        parent_id INTEGER NOT NULL REFERENCES p_parents(id) ON DELETE CASCADE,
        data TEXT NOT NULL
      )`);

      for (let i = 1; i <= 10; i++) {
        await h1.pg.query(
          `INSERT INTO p_parents (name) VALUES ($1)`,
          [`parent-${i}-${"p".repeat(30)}`],
        );
      }
      for (let p = 1; p <= 10; p++) {
        for (let c = 0; c < 5; c++) {
          await h1.pg.query(
            `INSERT INTO p_children (parent_id, data) VALUES ($1, $2)`,
            [p, `child-${p}-${c}-${"d".repeat(40)}`],
          );
        }
      }

      await h1.pg.query(`DELETE FROM p_parents WHERE id <= 4`);

      // Insert more data after cascade
      await h1.pg.query(
        `INSERT INTO p_parents (name) VALUES ('post-cascade')`,
      );

      await h1.syncToFs();
      await h1.destroy();
      harnesses = [];

      // Phase 2: remount and verify
      const h2 = await create(size, backend);

      const parents = await h2.pg.query(
        `SELECT COUNT(*)::int AS n FROM p_parents`,
      );
      expect(parents.rows[0].n).toBe(7); // 10 - 4 + 1

      const children = await h2.pg.query(
        `SELECT COUNT(*)::int AS n FROM p_children`,
      );
      expect(children.rows[0].n).toBe(30); // 6 × 5

      const postCascade = await h2.pg.query(
        `SELECT name FROM p_parents WHERE name = 'post-cascade'`,
      );
      expect(postCascade.rows.length).toBe(1);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 8: RESTRICT prevents deletion (rollback under pressure)
// ---------------------------------------------------------------------------

describe("RESTRICT prevents deletion with rollback", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`CREATE TABLE r_parents (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
      )`);
      await pg.query(`CREATE TABLE r_children (
        id SERIAL PRIMARY KEY,
        parent_id INTEGER NOT NULL REFERENCES r_parents(id) ON DELETE RESTRICT,
        data TEXT NOT NULL
      )`);

      for (let i = 1; i <= 5; i++) {
        await pg.query(
          `INSERT INTO r_parents (name) VALUES ($1)`,
          [`parent-${i}`],
        );
      }
      for (let p = 1; p <= 5; p++) {
        await pg.query(
          `INSERT INTO r_children (parent_id, data) VALUES ($1, $2)`,
          [p, `child-of-${p}`],
        );
      }

      // Attempt to delete a parent with children — should fail
      let caught = false;
      try {
        await pg.query(`DELETE FROM r_parents WHERE id = 1`);
      } catch (e: any) {
        caught = true;
        expect(e.message).toMatch(/violates foreign key/i);
      }
      expect(caught).toBe(true);

      // Verify nothing was deleted
      const parents = await pg.query(
        `SELECT COUNT(*)::int AS n FROM r_parents`,
      );
      expect(parents.rows[0].n).toBe(5);

      // Delete child first, then parent should succeed
      await pg.query(`DELETE FROM r_children WHERE parent_id = 1`);
      await pg.query(`DELETE FROM r_parents WHERE id = 1`);

      const afterParents = await pg.query(
        `SELECT COUNT(*)::int AS n FROM r_parents`,
      );
      expect(afterParents.rows[0].n).toBe(4);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 9: Self-referential FK (tree structure)
// ---------------------------------------------------------------------------

describe("Self-referential FK tree structure", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`CREATE TABLE tree_nodes (
        id SERIAL PRIMARY KEY,
        parent_id INTEGER REFERENCES tree_nodes(id) ON DELETE CASCADE,
        name TEXT NOT NULL,
        depth INTEGER NOT NULL DEFAULT 0
      )`);
      await pg.query(
        `CREATE INDEX idx_tree_parent ON tree_nodes(parent_id)`,
      );

      // Build a tree: 3 roots, each with 3 children, each with 3 grandchildren
      for (let r = 0; r < 3; r++) {
        const rootRes = await pg.query(
          `INSERT INTO tree_nodes (parent_id, name, depth) VALUES (NULL, $1, 0) RETURNING id`,
          [`root-${r}`],
        );
        const rootId = rootRes.rows[0].id;

        for (let c = 0; c < 3; c++) {
          const childRes = await pg.query(
            `INSERT INTO tree_nodes (parent_id, name, depth) VALUES ($1, $2, 1) RETURNING id`,
            [rootId, `child-${r}-${c}`],
          );
          const childId = childRes.rows[0].id;

          for (let g = 0; g < 3; g++) {
            await pg.query(
              `INSERT INTO tree_nodes (parent_id, name, depth) VALUES ($1, $2, 2)`,
              [childId, `grandchild-${r}-${c}-${g}`],
            );
          }
        }
      }

      const total = await pg.query(
        `SELECT COUNT(*)::int AS n FROM tree_nodes`,
      );
      expect(total.rows[0].n).toBe(39); // 3 + 9 + 27

      // Delete one root — should cascade through all descendants
      const firstRoot = await pg.query(
        `SELECT id FROM tree_nodes WHERE depth = 0 ORDER BY id LIMIT 1`,
      );
      await pg.query(`DELETE FROM tree_nodes WHERE id = $1`, [
        firstRoot.rows[0].id,
      ]);

      const afterDelete = await pg.query(
        `SELECT COUNT(*)::int AS n FROM tree_nodes`,
      );
      expect(afterDelete.rows[0].n).toBe(26); // 39 - 13 (1 root + 3 children + 9 grandchildren)

      // Verify no orphans
      const orphans = await pg.query(`
        SELECT COUNT(*)::int AS n FROM tree_nodes t
        WHERE t.parent_id IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM tree_nodes p WHERE p.id = t.parent_id)
      `);
      expect(orphans.rows[0].n).toBe(0);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 10: ON CONFLICT (UPSERT) with FK constraints
// ---------------------------------------------------------------------------

describe("UPSERT with FK constraints", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`CREATE TABLE u_parents (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
      )`);
      await pg.query(`CREATE TABLE u_entries (
        id SERIAL PRIMARY KEY,
        parent_id INTEGER NOT NULL REFERENCES u_parents(id),
        key TEXT NOT NULL,
        value TEXT NOT NULL,
        version INTEGER NOT NULL DEFAULT 1,
        UNIQUE(parent_id, key)
      )`);

      for (let p = 1; p <= 5; p++) {
        await pg.query(
          `INSERT INTO u_parents (name) VALUES ($1)`,
          [`parent-${p}`],
        );
      }

      // Initial inserts
      for (let p = 1; p <= 5; p++) {
        for (let k = 0; k < 6; k++) {
          await pg.query(`
            INSERT INTO u_entries (parent_id, key, value)
            VALUES ($1, $2, $3)
            ON CONFLICT (parent_id, key) DO UPDATE
              SET value = EXCLUDED.value, version = u_entries.version + 1
          `, [p, `key-${k}`, `original-${p}-${k}-${"o".repeat(30)}`]);
        }
      }

      const count1 = await pg.query(
        `SELECT COUNT(*)::int AS n FROM u_entries`,
      );
      expect(count1.rows[0].n).toBe(30);

      // Upsert — should update existing rows
      for (let p = 1; p <= 5; p++) {
        for (let k = 0; k < 6; k++) {
          await pg.query(`
            INSERT INTO u_entries (parent_id, key, value)
            VALUES ($1, $2, $3)
            ON CONFLICT (parent_id, key) DO UPDATE
              SET value = EXCLUDED.value, version = u_entries.version + 1
          `, [p, `key-${k}`, `updated-${p}-${k}-${"u".repeat(30)}`]);
        }
      }

      // Row count should not change
      const count2 = await pg.query(
        `SELECT COUNT(*)::int AS n FROM u_entries`,
      );
      expect(count2.rows[0].n).toBe(30);

      // All should be at version 2
      const versions = await pg.query(
        `SELECT DISTINCT version FROM u_entries`,
      );
      expect(versions.rows.length).toBe(1);
      expect(versions.rows[0].version).toBe(2);

      // Values should be updated
      const spot = await pg.query(
        `SELECT value FROM u_entries WHERE parent_id = 3 AND key = 'key-2'`,
      );
      expect(spot.rows[0].value).toMatch(/^updated-3-2-u+$/);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 11: CASCADE operations + VACUUM
// ---------------------------------------------------------------------------

describe("CASCADE DELETE then VACUUM reclaims pages", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages)`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`CREATE TABLE v_parents (
        id SERIAL PRIMARY KEY,
        data TEXT NOT NULL
      )`);
      await pg.query(`CREATE TABLE v_children (
        id SERIAL PRIMARY KEY,
        parent_id INTEGER NOT NULL REFERENCES v_parents(id) ON DELETE CASCADE,
        payload TEXT NOT NULL
      )`);

      for (let p = 1; p <= 10; p++) {
        await pg.query(
          `INSERT INTO v_parents (data) VALUES ($1)`,
          [`parent-${p}-${"p".repeat(40)}`],
        );
      }

      for (let p = 1; p <= 10; p++) {
        await pg.query(`
          INSERT INTO v_children (parent_id, payload)
          SELECT ${p}, 'child-' || i || '-${"c".repeat(50)}'
          FROM generate_series(1, 10) AS s(i)
        `);
      }

      // Cascade delete then vacuum both tables
      await pg.query(`DELETE FROM v_parents WHERE id <= 5`);

      await pg.query(`VACUUM v_parents`);
      await pg.query(`VACUUM v_children`);

      const parents = await pg.query(
        `SELECT COUNT(*)::int AS n FROM v_parents`,
      );
      expect(parents.rows[0].n).toBe(5);

      const children = await pg.query(
        `SELECT COUNT(*)::int AS n FROM v_children`,
      );
      expect(children.rows[0].n).toBe(50);

      // Verify we can still insert after VACUUM
      await pg.query(
        `INSERT INTO v_parents (data) VALUES ('post-vacuum')`,
      );
      const newParent = await pg.query(
        `SELECT id FROM v_parents WHERE data = 'post-vacuum'`,
      );
      await pg.query(
        `INSERT INTO v_children (parent_id, payload) VALUES ($1, 'new-child')`,
        [newParent.rows[0].id],
      );

      const finalChildren = await pg.query(
        `SELECT COUNT(*)::int AS n FROM v_children`,
      );
      expect(finalChildren.rows[0].n).toBe(51);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 12: Cascading multi-table transaction with SAVEPOINT
// ---------------------------------------------------------------------------

describe("CASCADE within savepoint rollback", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`CREATE TABLE sp_parents (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
      )`);
      await pg.query(`CREATE TABLE sp_children (
        id SERIAL PRIMARY KEY,
        parent_id INTEGER NOT NULL REFERENCES sp_parents(id) ON DELETE CASCADE,
        value TEXT NOT NULL
      )`);

      for (let i = 1; i <= 6; i++) {
        await pg.query(
          `INSERT INTO sp_parents (name) VALUES ($1)`,
          [`parent-${i}`],
        );
      }
      for (let p = 1; p <= 6; p++) {
        for (let c = 0; c < 3; c++) {
          await pg.query(
            `INSERT INTO sp_children (parent_id, value) VALUES ($1, $2)`,
            [p, `child-${p}-${c}`],
          );
        }
      }

      // Begin transaction, delete with cascade, rollback
      await pg.query(`BEGIN`);
      await pg.query(`SAVEPOINT sp1`);
      await pg.query(`DELETE FROM sp_parents WHERE id <= 3`);

      // Verify cascade happened within transaction
      const midParents = await pg.query(
        `SELECT COUNT(*)::int AS n FROM sp_parents`,
      );
      expect(midParents.rows[0].n).toBe(3);

      // Rollback — should undo the cascade
      await pg.query(`ROLLBACK TO SAVEPOINT sp1`);

      // Delete different parents and commit
      await pg.query(`DELETE FROM sp_parents WHERE id = 6`);
      await pg.query(`COMMIT`);

      const finalParents = await pg.query(
        `SELECT COUNT(*)::int AS n FROM sp_parents`,
      );
      expect(finalParents.rows[0].n).toBe(5);

      const finalChildren = await pg.query(
        `SELECT COUNT(*)::int AS n FROM sp_children`,
      );
      expect(finalChildren.rows[0].n).toBe(15); // 5 parents × 3
    });
  }
});
