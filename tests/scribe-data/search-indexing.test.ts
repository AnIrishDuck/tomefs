/**
 * Search indexing tests for scribe-data workloads.
 *
 * Tests the write-heavy burst pattern of search indexing: after sync,
 * scribe-data scans blocks, extracts text, and writes tsvector entries
 * to block_search_index. This is a scan+write pattern that stresses the
 * page cache differently than pure inserts.
 */

import { describe, it, expect, afterEach } from "vitest";
import {
  createHarness,
  CACHE_CONFIGS,
  type CacheSize,
  type ScribeTestHarness,
  type TestStream,
} from "./harness.js";

let harness: ScribeTestHarness | null = null;

afterEach(async () => {
  if (harness) {
    await harness.destroy();
    harness = null;
  }
});

const PRESSURE_CONFIGS: CacheSize[] = ["tiny", "small", "medium", "large"];

/** Insert N notes with searchable content. */
async function seedNotes(
  stream: TestStream,
  schema: string,
  count: number,
): Promise<void> {
  const topics = [
    "database performance optimization techniques",
    "browser rendering pipeline and compositing",
    "filesystem page cache design patterns",
    "distributed consensus algorithms and Raft",
    "TypeScript generics and type inference",
    "WebAssembly memory model and shared buffers",
    "indexeddb transaction lifecycle management",
    "LRU cache eviction strategies comparison",
    "postgres WAL write-ahead logging internals",
    "emscripten virtual filesystem architecture",
  ];

  for (let i = 0; i < count; i++) {
    const id = `note-${i.toString().padStart(4, "0")}`;
    const topic = topics[i % topics.length];
    const content = `${topic} — entry ${i}. ${"Detail paragraph about the topic. ".repeat(5)}`;
    await stream.exec(
      `INSERT INTO "${schema}".block (id, content) VALUES ($1, $2)`,
      [id, content],
    );
  }
}

/**
 * Index unindexed blocks in batches. Simulates scribe-data's
 * indexSearchVectors() pattern.
 */
async function indexBlocks(
  stream: TestStream,
  schema: string,
  batchSize: number,
): Promise<number> {
  let totalIndexed = 0;

  while (true) {
    // Find unindexed blocks
    const unindexed = await stream.query(
      `SELECT b.id, b.content FROM "${schema}".block b
       LEFT JOIN "${schema}".block_search_index si ON b.id = si.block_id
       WHERE si.block_id IS NULL
       LIMIT $1`,
      [batchSize],
    );

    if (unindexed.rows.length === 0) break;

    // Index each block
    for (const row of unindexed.rows) {
      await stream.exec(
        `INSERT INTO "${schema}".block_search_index (block_id, search_vector)
         VALUES ($1, to_tsvector('english', $2))`,
        [row.id, row.content],
      );
    }

    totalIndexed += unindexed.rows.length;
  }

  return totalIndexed;
}

// ---------------------------------------------------------------------------
// Scenario 1: Index After Sync
// ---------------------------------------------------------------------------

describe("Search indexing: index after sync", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`50 notes indexed in batches of 10 — cache=${size} (${pages} pages)`, async () => {
      harness = await createHarness(size);
      const stream = await harness.createWriterStream("idx1");

      await seedNotes(stream, "idx1", 50);

      const indexed = await indexBlocks(stream, "idx1", 10);
      expect(indexed).toBe(50);

      // Verify all notes are indexed
      const count = await stream.query(
        `SELECT COUNT(*)::int as count FROM "idx1".block_search_index`,
      );
      expect(count.rows[0].count).toBe(50);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 2: Incremental Indexing
// ---------------------------------------------------------------------------

describe("Search indexing: incremental indexing", () => {
  it("sync 20, index, sync 30 more, re-index — only new notes processed @fast", async () => {
    harness = await createHarness("medium");
    const stream = await harness.createWriterStream("idx2");

    // First batch: 20 notes
    await seedNotes(stream, "idx2", 20);
    const indexed1 = await indexBlocks(stream, "idx2", 10);
    expect(indexed1).toBe(20);

    // Second batch: 30 more notes
    for (let i = 20; i < 50; i++) {
      const id = `note-${i.toString().padStart(4, "0")}`;
      await stream.exec(
        `INSERT INTO "idx2".block (id, content) VALUES ($1, $2)`,
        [id, `Additional note ${i} about advanced topics`],
      );
    }

    // Re-index — should only pick up the 30 new notes
    const indexed2 = await indexBlocks(stream, "idx2", 10);
    expect(indexed2).toBe(30);

    // Total indexed
    const count = await stream.query(
      `SELECT COUNT(*)::int as count FROM "idx2".block_search_index`,
    );
    expect(count.rows[0].count).toBe(50);
  });
});

// ---------------------------------------------------------------------------
// Scenario 3: Search After Index
// ---------------------------------------------------------------------------

describe("Search indexing: search queries after indexing", () => {
  it("full-text search returns correct results @fast", async () => {
    harness = await createHarness("medium");
    const stream = await harness.createWriterStream("idx3");

    await seedNotes(stream, "idx3", 50);
    await indexBlocks(stream, "idx3", 10);

    // Search for "database" — should match notes with "database performance"
    const dbResults = await stream.query(
      `SELECT b.id FROM "idx3".block b
       JOIN "idx3".block_search_index si ON b.id = si.block_id
       WHERE si.search_vector @@ to_tsquery('english', 'database')
       ORDER BY b.id`,
    );
    // "database" appears in topic index 0, which hits notes 0, 10, 20, 30, 40
    expect(dbResults.rows.length).toBe(5);
    expect(dbResults.rows[0].id).toBe("note-0000");

    // Search for "typescript" — topic index 4
    const tsResults = await stream.query(
      `SELECT b.id FROM "idx3".block b
       JOIN "idx3".block_search_index si ON b.id = si.block_id
       WHERE si.search_vector @@ to_tsquery('english', 'typescript')
       ORDER BY b.id`,
    );
    expect(tsResults.rows.length).toBe(5);
    expect(tsResults.rows[0].id).toBe("note-0004");

    // Combined search: "database & optimization"
    const combined = await stream.query(
      `SELECT b.id FROM "idx3".block b
       JOIN "idx3".block_search_index si ON b.id = si.block_id
       WHERE si.search_vector @@ to_tsquery('english', 'database & optimization')
       ORDER BY b.id`,
    );
    expect(combined.rows.length).toBe(5);
  });

  it("search with no results returns empty", async () => {
    harness = await createHarness("medium");
    const stream = await harness.createWriterStream("idx4");

    await seedNotes(stream, "idx4", 10);
    await indexBlocks(stream, "idx4", 10);

    const results = await stream.query(
      `SELECT b.id FROM "idx4".block b
       JOIN "idx4".block_search_index si ON b.id = si.block_id
       WHERE si.search_vector @@ to_tsquery('english', 'xyznonexistent')`,
    );
    expect(results.rows.length).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// Scenario 4: Search Pagination
// ---------------------------------------------------------------------------

describe("Search indexing: search pagination", () => {
  it("paginated search — no duplicates or gaps", async () => {
    harness = await createHarness("medium");
    const stream = await harness.createWriterStream("idx5");

    // Create 50 notes that all match "paragraph" (from seedNotes content)
    await seedNotes(stream, "idx5", 50);
    await indexBlocks(stream, "idx5", 10);

    // Paginate through results
    const allIds: string[] = [];
    const pageSize = 10;
    let offset = 0;

    while (true) {
      const page = await stream.query(
        `SELECT b.id FROM "idx5".block b
         JOIN "idx5".block_search_index si ON b.id = si.block_id
         WHERE si.search_vector @@ to_tsquery('english', 'paragraph')
         ORDER BY b.id
         LIMIT $1 OFFSET $2`,
        [pageSize, offset],
      );

      if (page.rows.length === 0) break;
      allIds.push(...page.rows.map((r: any) => r.id));
      offset += pageSize;
    }

    // Should get all 50 notes (every note contains "paragraph")
    expect(allIds.length).toBe(50);

    // No duplicates
    const unique = new Set(allIds);
    expect(unique.size).toBe(50);

    // Verify ordering
    expect(allIds[0]).toBe("note-0000");
    expect(allIds[49]).toBe("note-0049");
  });
});

// ---------------------------------------------------------------------------
// Scenario 5: Index Under Tiny Cache
// ---------------------------------------------------------------------------

describe("Search indexing: heavy eviction during indexing", () => {
  it("index 50 notes under tiny cache — all survive eviction", async () => {
    harness = await createHarness("tiny");
    const stream = await harness.createWriterStream("idx6");

    await seedNotes(stream, "idx6", 50);

    // Index with small batches to maximize cache churn
    const indexed = await indexBlocks(stream, "idx6", 5);
    expect(indexed).toBe(50);

    // Verify all search entries exist
    const count = await stream.query(
      `SELECT COUNT(*)::int as count FROM "idx6".block_search_index`,
    );
    expect(count.rows[0].count).toBe(50);

    // Verify search still works after heavy eviction
    const results = await stream.query(
      `SELECT b.id FROM "idx6".block b
       JOIN "idx6".block_search_index si ON b.id = si.block_id
       WHERE si.search_vector @@ to_tsquery('english', 'filesystem')
       ORDER BY b.id`,
    );
    // "filesystem" appears in topic index 2 (every 10th note): 2,12,22,32,42
    // With 50 notes, that's indices 2,12,22,32,42 = 5 notes.
    // But "filesystem" also matches topic index 9 ("emscripten virtual filesystem").
    // So notes at indices 2,9,12,19,22,29,32,39,42,49 = 10 notes.
    expect(results.rows.length).toBe(10);
  });
});
