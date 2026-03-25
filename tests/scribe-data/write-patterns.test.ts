/**
 * Write pattern tests for scribe-data workloads.
 *
 * Tests the filesystem under scribe-data's actual SQL patterns: note
 * creation, versioning, collection hierarchy, and moves. Each scenario
 * runs at multiple cache pressure levels.
 */

import { describe, it, expect, afterEach } from "vitest";
import {
  createHarness,
  CACHE_CONFIGS,
  type CacheSize,
  type ScribeTestHarness,
} from "./harness.js";

let harness: ScribeTestHarness | null = null;

afterEach(async () => {
  if (harness) {
    await harness.destroy();
    harness = null;
  }
});

const PRESSURE_CONFIGS: CacheSize[] = ["tiny", "small", "medium", "large"];

/**
 * Run a scenario against PGlite+tomefs at every cache pressure level.
 */
function describeScenario(
  name: string,
  scenarioFn: (h: ScribeTestHarness, size: CacheSize) => Promise<void>,
) {
  describe(name, () => {
    for (const size of PRESSURE_CONFIGS) {
      const pages = CACHE_CONFIGS[size];
      it(`cache=${size} (${pages} pages)`, async () => {
        harness = await createHarness(size);
        await scenarioFn(harness, size);
      });
    }
  });
}

// ---------------------------------------------------------------------------
// Scenario 1: Burst Note Creation
// ---------------------------------------------------------------------------

describeScenario(
  "Burst note creation — 100 notes in rapid succession",
  async (h) => {
    const stream = await h.createWriterStream("burst");

    for (let i = 0; i < 100; i++) {
      const id = `note-${i.toString().padStart(4, "0")}`;
      const content = `Burst note ${i}: ${"content-".repeat(20)}`;
      await stream.exec(
        `INSERT INTO "burst".block (id, content) VALUES ($1, $2)`,
        [id, content],
      );
    }

    const result = await stream.query(
      `SELECT COUNT(*)::int as count FROM "burst".block`,
    );
    expect(result.rows[0].count).toBe(100);

    // Verify first and last notes
    const first = await stream.query(
      `SELECT content FROM "burst".block WHERE id = 'note-0000'`,
    );
    expect(first.rows[0].content).toMatch(/^Burst note 0:/);

    const last = await stream.query(
      `SELECT content FROM "burst".block WHERE id = 'note-0099'`,
    );
    expect(last.rows[0].content).toMatch(/^Burst note 99:/);
  },
);

// ---------------------------------------------------------------------------
// Scenario 2: Version Chain
// ---------------------------------------------------------------------------

describeScenario(
  "Version chain — 20 versions of a single note",
  async (h) => {
    const stream = await h.createWriterStream("vchain");

    // Create the note
    await stream.exec(
      `INSERT INTO "vchain".block (id, content) VALUES ('note-1', 'Version 0')`,
    );

    // Create 20 versions
    for (let v = 1; v <= 20; v++) {
      const versionId = `ver-${v.toString().padStart(3, "0")}`;
      await stream.exec(
        `INSERT INTO "vchain".block_version (id, block_id, content, version_number)
         VALUES ($1, 'note-1', $2, $3)`,
        [versionId, `Version ${v} content: ${"edit-".repeat(v * 5)}`, v],
      );

      // Update the block's content to latest version
      await stream.exec(
        `UPDATE "vchain".block SET content = $1, updated_at = NOW() WHERE id = 'note-1'`,
        [`Version ${v} content: ${"edit-".repeat(v * 5)}`],
      );
    }

    // Verify version count
    const versions = await stream.query(
      `SELECT version_number FROM "vchain".block_version
       WHERE block_id = 'note-1' ORDER BY version_number`,
    );
    expect(versions.rows.length).toBe(20);
    for (let v = 0; v < 20; v++) {
      expect(versions.rows[v].version_number).toBe(v + 1);
    }

    // Verify current block content matches latest version
    const current = await stream.query(
      `SELECT content FROM "vchain".block WHERE id = 'note-1'`,
    );
    expect(current.rows[0].content).toMatch(/^Version 20 content:/);
  },
);

// ---------------------------------------------------------------------------
// Scenario 3: Collection Hierarchy
// ---------------------------------------------------------------------------

describeScenario(
  "Collection hierarchy — 5 levels deep with notes at each level",
  async (h) => {
    const stream = await h.createWriterStream("hier");

    // Create 5-level hierarchy
    let parentId: string | null = null;
    const collectionIds: string[] = [];

    for (let level = 0; level < 5; level++) {
      const colId = `col-level-${level}`;
      collectionIds.push(colId);

      if (parentId === null) {
        await stream.exec(
          `INSERT INTO "hier".collection (id, name) VALUES ($1, $2)`,
          [colId, `Level ${level}`],
        );
      } else {
        await stream.exec(
          `INSERT INTO "hier".collection (id, name, parent_id) VALUES ($1, $2, $3)`,
          [colId, `Level ${level}`, parentId],
        );
      }
      parentId = colId;

      // Create 3 notes at each level
      for (let n = 0; n < 3; n++) {
        const noteId = `note-L${level}-${n}`;
        await stream.exec(
          `INSERT INTO "hier".block (id, content) VALUES ($1, $2)`,
          [noteId, `Note at level ${level}, index ${n}`],
        );
        await stream.exec(
          `INSERT INTO "hier".block_collection (block_id, collection_id) VALUES ($1, $2)`,
          [noteId, colId],
        );
      }
    }

    // Verify total collections
    const cols = await stream.query(
      `SELECT COUNT(*)::int as count FROM "hier".collection`,
    );
    expect(cols.rows[0].count).toBe(5);

    // Verify total notes
    const notes = await stream.query(
      `SELECT COUNT(*)::int as count FROM "hier".block`,
    );
    expect(notes.rows[0].count).toBe(15);

    // Query breadcrumbs via recursive CTE
    const breadcrumbs = await stream.query(`
      WITH RECURSIVE ancestors AS (
        SELECT id, name, parent_id, 0 as depth
        FROM "hier".collection WHERE id = 'col-level-4'
        UNION ALL
        SELECT c.id, c.name, c.parent_id, a.depth + 1
        FROM "hier".collection c
        JOIN ancestors a ON c.id = a.parent_id
      )
      SELECT name FROM ancestors ORDER BY depth DESC
    `);

    expect(breadcrumbs.rows.map((r: any) => r.name)).toEqual([
      "Level 0",
      "Level 1",
      "Level 2",
      "Level 3",
      "Level 4",
    ]);

    // Verify notes at deepest level
    const deepNotes = await stream.query(
      `SELECT b.id FROM "hier".block b
       JOIN "hier".block_collection bc ON b.id = bc.block_id
       WHERE bc.collection_id = 'col-level-4'
       ORDER BY b.id`,
    );
    expect(deepNotes.rows.length).toBe(3);
  },
);

// ---------------------------------------------------------------------------
// Scenario 4: Move Operations (re-assign collection)
// ---------------------------------------------------------------------------

describeScenario(
  "Move operations — move notes between collections",
  async (h) => {
    const stream = await h.createWriterStream("move");

    // Create two collections
    await stream.exec(
      `INSERT INTO "move".collection (id, name) VALUES ('inbox', 'Inbox')`,
    );
    await stream.exec(
      `INSERT INTO "move".collection (id, name) VALUES ('archive', 'Archive')`,
    );

    // Create 10 notes in inbox
    for (let i = 0; i < 10; i++) {
      const noteId = `note-${i}`;
      await stream.exec(
        `INSERT INTO "move".block (id, content) VALUES ($1, $2)`,
        [noteId, `Note ${i}`],
      );
      await stream.exec(
        `INSERT INTO "move".block_collection (block_id, collection_id) VALUES ($1, 'inbox')`,
        [noteId],
      );
    }

    // Move first 5 notes to archive
    for (let i = 0; i < 5; i++) {
      const noteId = `note-${i}`;
      await stream.exec(
        `DELETE FROM "move".block_collection WHERE block_id = $1 AND collection_id = 'inbox'`,
        [noteId],
      );
      await stream.exec(
        `INSERT INTO "move".block_collection (block_id, collection_id) VALUES ($1, 'archive')`,
        [noteId],
      );
    }

    // Verify inbox has 5 notes
    const inbox = await stream.query(
      `SELECT COUNT(*)::int as count FROM "move".block_collection WHERE collection_id = 'inbox'`,
    );
    expect(inbox.rows[0].count).toBe(5);

    // Verify archive has 5 notes
    const archive = await stream.query(
      `SELECT COUNT(*)::int as count FROM "move".block_collection WHERE collection_id = 'archive'`,
    );
    expect(archive.rows[0].count).toBe(5);

    // Verify total notes unchanged
    const total = await stream.query(
      `SELECT COUNT(*)::int as count FROM "move".block`,
    );
    expect(total.rows[0].count).toBe(10);
  },
);

// ---------------------------------------------------------------------------
// Scenario 5: Mixed Reads and Writes (simulating UI)
// ---------------------------------------------------------------------------

describeScenario(
  "Mixed reads and writes — interleaved like a live UI @fast",
  async (h) => {
    const stream = await h.createWriterStream("mixed");

    for (let i = 0; i < 30; i++) {
      // Write a note
      const noteId = `note-${i.toString().padStart(3, "0")}`;
      await stream.exec(
        `INSERT INTO "mixed".block (id, content) VALUES ($1, $2)`,
        [noteId, `Mixed note ${i}: ${"data-".repeat(30)}`],
      );

      // Interleave reads every 5 writes (simulating UI refreshes)
      if (i % 5 === 4) {
        const count = await stream.query(
          `SELECT COUNT(*)::int as count FROM "mixed".block`,
        );
        expect(count.rows[0].count).toBe(i + 1);

        // Also read the latest note to simulate rendering
        const latest = await stream.query(
          `SELECT id, content FROM "mixed".block ORDER BY id DESC LIMIT 1`,
        );
        expect(latest.rows[0].id).toBe(noteId);
      }
    }

    // Final verification
    const result = await stream.query(
      `SELECT COUNT(*)::int as count FROM "mixed".block`,
    );
    expect(result.rows[0].count).toBe(30);
  },
);
