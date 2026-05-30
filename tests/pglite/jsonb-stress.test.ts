/**
 * PGlite JSONB stress tests under cache pressure.
 *
 * JSONB is Postgres's binary JSON type with a unique on-disk representation:
 * values are stored as a binary tree of JEntry headers + data, enabling
 * efficient containment checks and key lookups without full deserialization.
 * Large JSONB values (>2KB) are TOAST-compressed, combining the TOAST page
 * fan-out from toast-stress.test.ts with JSONB's internal binary traversal.
 *
 * GIN indexes on JSONB columns use jsonb_ops or jsonb_path_ops strategies,
 * which decompose the document into (key, value) pairs or hashed paths
 * respectively. Under cache pressure, a single containment query (@>) must:
 *   1. Read the GIN metapage (always needed)
 *   2. Traverse GIN entry tree pages to find the posting list
 *   3. Read posting list pages to get heap TIDs
 *   4. Read heap pages to fetch the actual rows
 *   5. Decompress TOAST chunks if the JSONB value is large
 *
 * With a 4-page cache, every step evicts pages needed by the next step,
 * creating a worst-case access pattern for LRU caches.
 *
 * Scenarios:
 *   1. JSONB insert + containment query (@>)
 *   2. GIN index on JSONB with containment queries
 *   3. Nested JSONB documents with deep key paths
 *   4. JSONB array operations (? and ?| operators)
 *   5. jsonb_set / jsonb_insert modifications
 *   6. JSONB aggregation (jsonb_agg, jsonb_object_agg)
 *   7. Large JSONB documents exceeding TOAST threshold
 *   8. JSONB with GIN index persistence round-trip
 *   9. DELETE + VACUUM on JSONB with GIN indexes
 *  10. Mixed JSONB sizes under concurrent-query cache churn
 */

import { describe, it, expect, afterEach } from "vitest";
import {
  createPGliteHarness,
  CACHE_CONFIGS,
  type CacheSize,
  type PGliteHarness,
} from "./harness.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";

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

function generateJsonDoc(id: number, depth: number = 1): object {
  const doc: Record<string, unknown> = {
    id,
    name: `item_${id}`,
    tags: [`tag_${id % 5}`, `tag_${id % 3}`, `category_${id % 7}`],
    metadata: {
      created: `2024-01-${(id % 28) + 1}`,
      priority: id % 5,
      active: id % 2 === 0,
    },
  };
  if (depth > 0) {
    doc.nested = generateJsonDoc(id + 1000, depth - 1);
  }
  return doc;
}

function generateLargeJsonDoc(id: number, sizeKb: number): object {
  const doc: Record<string, unknown> = {
    id,
    name: `large_item_${id}`,
    tags: Array.from({ length: 20 }, (_, i) => `tag_${id}_${i}`),
  };
  const padding: string[] = [];
  const targetChars = sizeKb * 1024 - JSON.stringify(doc).length - 20;
  const chunkSize = 100;
  for (let i = 0; i < Math.ceil(targetChars / chunkSize); i++) {
    const seed = id * 1000 + i;
    let s = seed;
    const chars: string[] = [];
    for (let j = 0; j < chunkSize; j++) {
      s = (s * 1103515245 + 12345) & 0x7fffffff;
      chars.push(String.fromCharCode(97 + (s % 26)));
    }
    padding.push(chars.join(""));
  }
  doc.payload = padding;
  return doc;
}

// 1. JSONB insert + containment query (@>)
describeScenario(
  "JSONB insert and containment query",
  async (h) => {
    const { pg } = h;

    await pg.exec(`
      CREATE TABLE docs (
        id SERIAL PRIMARY KEY,
        data JSONB NOT NULL
      )
    `);

    for (let i = 0; i < 30; i++) {
      const doc = generateJsonDoc(i, 2);
      await pg.query("INSERT INTO docs (data) VALUES ($1)", [JSON.stringify(doc)]);
    }

    const result = await pg.query(
      `SELECT id, data FROM docs WHERE data @> $1`,
      [JSON.stringify({ metadata: { priority: 2 } })],
    );
    expect(result.rows.length).toBeGreaterThan(0);
    for (const row of result.rows) {
      expect(row.data.metadata.priority).toBe(2);
    }

    const exactMatch = await pg.query(
      `SELECT id FROM docs WHERE data @> $1`,
      [JSON.stringify({ id: 5, name: "item_5" })],
    );
    expect(exactMatch.rows.length).toBe(1);
  },
);

// 2. GIN index on JSONB with containment queries
describeScenario(
  "GIN index on JSONB with containment queries",
  async (h) => {
    const { pg } = h;

    await pg.exec(`
      CREATE TABLE indexed_docs (
        id SERIAL PRIMARY KEY,
        data JSONB NOT NULL
      )
    `);
    await pg.exec(`CREATE INDEX idx_docs_gin ON indexed_docs USING GIN (data)`);

    for (let i = 0; i < 50; i++) {
      const doc = generateJsonDoc(i);
      await pg.query("INSERT INTO indexed_docs (data) VALUES ($1)", [
        JSON.stringify(doc),
      ]);
    }

    await pg.exec("ANALYZE indexed_docs");

    const tagResult = await pg.query(
      `SELECT id FROM indexed_docs WHERE data @> $1`,
      [JSON.stringify({ tags: ["tag_0"] })],
    );
    expect(tagResult.rows.length).toBeGreaterThan(0);

    const metaResult = await pg.query(
      `SELECT id FROM indexed_docs WHERE data @> $1`,
      [JSON.stringify({ metadata: { active: true } })],
    );
    expect(metaResult.rows.length).toBeGreaterThan(0);

    const multiResult = await pg.query(
      `SELECT id FROM indexed_docs WHERE data @> $1 AND data @> $2`,
      [
        JSON.stringify({ metadata: { priority: 1 } }),
        JSON.stringify({ metadata: { active: false } }),
      ],
    );
    for (const row of multiResult.rows) {
      const fullRow = await pg.query(
        "SELECT data FROM indexed_docs WHERE id = $1",
        [row.id],
      );
      expect(fullRow.rows[0].data.metadata.priority).toBe(1);
      expect(fullRow.rows[0].data.metadata.active).toBe(false);
    }
  },
);

// 3. Nested JSONB documents with deep key path access
describeScenario(
  "Nested JSONB with deep key path access",
  async (h) => {
    const { pg } = h;

    await pg.exec(`
      CREATE TABLE nested_docs (
        id SERIAL PRIMARY KEY,
        data JSONB NOT NULL
      )
    `);

    for (let i = 0; i < 20; i++) {
      const doc = generateJsonDoc(i, 4);
      await pg.query("INSERT INTO nested_docs (data) VALUES ($1)", [
        JSON.stringify(doc),
      ]);
    }

    const deepResult = await pg.query(
      `SELECT id, data->'nested'->'nested'->'name' AS deep_name FROM nested_docs WHERE id = 1`,
    );
    expect(deepResult.rows.length).toBe(1);
    expect(deepResult.rows[0].deep_name).not.toBeNull();

    const pathResult = await pg.query(
      `SELECT id, data #> '{nested,metadata,priority}' AS deep_priority
       FROM nested_docs
       WHERE data #> '{nested,metadata,priority}' IS NOT NULL
       LIMIT 5`,
    );
    expect(pathResult.rows.length).toBeGreaterThan(0);

    const existsResult = await pg.query(
      `SELECT count(*) as cnt FROM nested_docs WHERE data ? 'nested'`,
    );
    expect(Number(existsResult.rows[0].cnt)).toBe(20);
  },
);

// 4. JSONB array operations (? and ?| operators)
describeScenario(
  "JSONB array key existence operators",
  async (h) => {
    const { pg } = h;

    await pg.exec(`
      CREATE TABLE tagged_items (
        id SERIAL PRIMARY KEY,
        tags JSONB NOT NULL,
        props JSONB NOT NULL
      )
    `);
    await pg.exec(`CREATE INDEX idx_tags_gin ON tagged_items USING GIN (tags)`);
    await pg.exec(`CREATE INDEX idx_props_gin ON tagged_items USING GIN (props)`);

    for (let i = 0; i < 40; i++) {
      const tags = JSON.stringify([
        `color_${i % 4}`,
        `size_${i % 3}`,
        `type_${i % 5}`,
      ]);
      const props = JSON.stringify({
        [`prop_${i % 6}`]: true,
        [`attr_${i % 4}`]: i,
      });
      await pg.query(
        "INSERT INTO tagged_items (tags, props) VALUES ($1, $2)",
        [tags, props],
      );
    }

    await pg.exec("ANALYZE tagged_items");

    const singleKey = await pg.query(
      `SELECT count(*) as cnt FROM tagged_items WHERE tags ? $1`,
      ["color_0"],
    );
    expect(Number(singleKey.rows[0].cnt)).toBeGreaterThan(0);

    const anyKey = await pg.query(
      `SELECT count(*) as cnt FROM tagged_items WHERE tags ?| array[$1, $2]`,
      ["color_0", "color_1"],
    );
    expect(Number(anyKey.rows[0].cnt)).toBeGreaterThan(0);

    const propKey = await pg.query(
      `SELECT count(*) as cnt FROM tagged_items WHERE props ? $1`,
      ["prop_0"],
    );
    expect(Number(propKey.rows[0].cnt)).toBeGreaterThan(0);
  },
);

// 5. jsonb_set / jsonb_insert modifications
describeScenario(
  "JSONB modification operators (jsonb_set)",
  async (h) => {
    const { pg } = h;

    await pg.exec(`
      CREATE TABLE mutable_docs (
        id SERIAL PRIMARY KEY,
        data JSONB NOT NULL
      )
    `);

    for (let i = 0; i < 25; i++) {
      const doc = generateJsonDoc(i);
      await pg.query("INSERT INTO mutable_docs (data) VALUES ($1)", [
        JSON.stringify(doc),
      ]);
    }

    await pg.exec(`
      UPDATE mutable_docs
      SET data = jsonb_set(data, '{metadata,updated}', '"2024-06-15"')
      WHERE (data->'metadata'->>'priority')::int >= 3
    `);

    const updated = await pg.query(
      `SELECT id, data->'metadata'->>'updated' AS upd
       FROM mutable_docs
       WHERE data->'metadata' ? 'updated'`,
    );
    expect(updated.rows.length).toBeGreaterThan(0);
    for (const row of updated.rows) {
      expect(row.upd).toBe("2024-06-15");
    }

    await pg.exec(`
      UPDATE mutable_docs
      SET data = data || '{"extra_field": "added"}'::jsonb
    `);

    const merged = await pg.query(
      `SELECT count(*) as cnt FROM mutable_docs WHERE data ? 'extra_field'`,
    );
    expect(Number(merged.rows[0].cnt)).toBe(25);

    await pg.exec(`
      UPDATE mutable_docs
      SET data = data - 'extra_field'
      WHERE id <= 10
    `);

    const afterRemove = await pg.query(
      `SELECT count(*) as cnt FROM mutable_docs WHERE data ? 'extra_field'`,
    );
    expect(Number(afterRemove.rows[0].cnt)).toBe(15);
  },
);

// 6. JSONB aggregation (jsonb_agg, jsonb_object_agg)
describeScenario(
  "JSONB aggregation functions",
  async (h) => {
    const { pg } = h;

    await pg.exec(`
      CREATE TABLE events (
        id SERIAL PRIMARY KEY,
        category TEXT NOT NULL,
        payload JSONB NOT NULL
      )
    `);

    for (let i = 0; i < 40; i++) {
      const category = `cat_${i % 4}`;
      const payload = JSON.stringify({
        value: i * 10,
        label: `event_${i}`,
      });
      await pg.query(
        "INSERT INTO events (category, payload) VALUES ($1, $2)",
        [category, payload],
      );
    }

    const aggResult = await pg.query(`
      SELECT category, jsonb_agg(payload ORDER BY id) AS payloads
      FROM events
      GROUP BY category
      ORDER BY category
    `);
    expect(aggResult.rows.length).toBe(4);
    for (const row of aggResult.rows) {
      expect(Array.isArray(row.payloads)).toBe(true);
      expect(row.payloads.length).toBe(10);
    }

    const objAgg = await pg.query(`
      SELECT jsonb_object_agg(category, event_count) AS summary
      FROM (
        SELECT category, count(*)::text AS event_count
        FROM events
        GROUP BY category
      ) sub
    `);
    expect(objAgg.rows.length).toBe(1);
    const summary = objAgg.rows[0].summary;
    expect(Object.keys(summary).length).toBe(4);
  },
);

// 7. Large JSONB documents exceeding TOAST threshold
describeScenario(
  "Large JSONB documents with TOAST compression",
  async (h, cacheSize) => {
    const { pg } = h;

    await pg.exec(`
      CREATE TABLE large_docs (
        id SERIAL PRIMARY KEY,
        data JSONB NOT NULL
      )
    `);

    const docCount = cacheSize === "tiny" ? 3 : 5;
    const docSizeKb = cacheSize === "tiny" ? 4 : 8;

    for (let i = 0; i < docCount; i++) {
      const doc = generateLargeJsonDoc(i, docSizeKb);
      await pg.query("INSERT INTO large_docs (data) VALUES ($1)", [
        JSON.stringify(doc),
      ]);
    }

    for (let i = 0; i < docCount; i++) {
      const result = await pg.query(
        "SELECT data FROM large_docs WHERE id = $1",
        [i + 1],
      );
      expect(result.rows.length).toBe(1);
      const retrieved = result.rows[0].data;
      expect(retrieved.id).toBe(i);
      expect(retrieved.name).toBe(`large_item_${i}`);
      expect(retrieved.tags.length).toBe(20);
      expect(Array.isArray(retrieved.payload)).toBe(true);
    }

    const containment = await pg.query(
      `SELECT id FROM large_docs WHERE data @> $1`,
      [JSON.stringify({ name: "large_item_0" })],
    );
    expect(containment.rows.length).toBe(1);
  },
);

// 8. JSONB with GIN index persistence round-trip
describeScenario(
  "JSONB GIN index persistence round-trip",
  async (h) => {
    const { pg } = h;
    const backend = h.backend as SyncMemoryBackend;

    await pg.exec(`
      CREATE TABLE persistent_docs (
        id SERIAL PRIMARY KEY,
        data JSONB NOT NULL
      )
    `);
    await pg.exec(
      `CREATE INDEX idx_persistent_gin ON persistent_docs USING GIN (data jsonb_path_ops)`,
    );

    const docs: Record<string, unknown>[] = [];
    for (let i = 0; i < 20; i++) {
      const doc = generateJsonDoc(i, 2) as Record<string, unknown>;
      docs.push(doc);
      await pg.query("INSERT INTO persistent_docs (data) VALUES ($1)", [
        JSON.stringify(doc),
      ]);
    }

    await h.syncToFs();
    await h.destroy();
    harness = null;

    const h2 = await createPGliteHarness({ cacheSize: "large", backend });
    harness = h2;

    const count = await h2.pg.query("SELECT count(*) as cnt FROM persistent_docs");
    expect(Number(count.rows[0].cnt)).toBe(20);

    const containment = await h2.pg.query(
      `SELECT id FROM persistent_docs WHERE data @> $1`,
      [JSON.stringify({ metadata: { priority: 0 } })],
    );
    expect(containment.rows.length).toBeGreaterThan(0);

    for (let i = 0; i < 5; i++) {
      const result = await h2.pg.query(
        "SELECT data FROM persistent_docs WHERE id = $1",
        [i + 1],
      );
      expect(result.rows[0].data.id).toBe(docs[i].id);
      expect(result.rows[0].data.name).toBe((docs[i] as Record<string, unknown>).name);
    }
  },
);

// 9. DELETE + VACUUM on JSONB with GIN indexes
describeScenario(
  "DELETE and VACUUM with JSONB GIN indexes",
  async (h) => {
    const { pg } = h;

    await pg.exec(`
      CREATE TABLE vacuumable_docs (
        id SERIAL PRIMARY KEY,
        data JSONB NOT NULL
      )
    `);
    await pg.exec(
      `CREATE INDEX idx_vacuum_gin ON vacuumable_docs USING GIN (data)`,
    );

    for (let i = 0; i < 40; i++) {
      const doc = generateJsonDoc(i);
      await pg.query("INSERT INTO vacuumable_docs (data) VALUES ($1)", [
        JSON.stringify(doc),
      ]);
    }

    const beforeDelete = await pg.query(
      `SELECT count(*) as cnt FROM vacuumable_docs WHERE data @> $1`,
      [JSON.stringify({ metadata: { priority: 1 } })],
    );
    const expectedBefore = Number(beforeDelete.rows[0].cnt);
    expect(expectedBefore).toBeGreaterThan(0);

    await pg.exec(`
      DELETE FROM vacuumable_docs
      WHERE (data->'metadata'->>'priority')::int = 1
    `);

    const afterDelete = await pg.query(
      `SELECT count(*) as cnt FROM vacuumable_docs WHERE data @> $1`,
      [JSON.stringify({ metadata: { priority: 1 } })],
    );
    expect(Number(afterDelete.rows[0].cnt)).toBe(0);

    await pg.exec("VACUUM vacuumable_docs");

    const afterVacuum = await pg.query(
      `SELECT count(*) as cnt FROM vacuumable_docs`,
    );
    const remaining = 40 - expectedBefore;
    expect(Number(afterVacuum.rows[0].cnt)).toBe(remaining);

    const otherPriority = await pg.query(
      `SELECT count(*) as cnt FROM vacuumable_docs WHERE data @> $1`,
      [JSON.stringify({ metadata: { priority: 2 } })],
    );
    expect(Number(otherPriority.rows[0].cnt)).toBeGreaterThan(0);
  },
);

// 10. Mixed JSONB sizes under concurrent-query cache churn
describeScenario(
  "Mixed JSONB sizes with interleaved queries",
  async (h, cacheSize) => {
    const { pg } = h;

    await pg.exec(`
      CREATE TABLE mixed_docs (
        id SERIAL PRIMARY KEY,
        doc_type TEXT NOT NULL,
        data JSONB NOT NULL
      )
    `);
    await pg.exec(`CREATE INDEX idx_mixed_gin ON mixed_docs USING GIN (data)`);

    for (let i = 0; i < 15; i++) {
      const smallDoc = generateJsonDoc(i);
      await pg.query(
        "INSERT INTO mixed_docs (doc_type, data) VALUES ('small', $1)",
        [JSON.stringify(smallDoc)],
      );
    }

    const largeSizeKb = cacheSize === "tiny" ? 3 : 6;
    for (let i = 0; i < 5; i++) {
      const largeDoc = generateLargeJsonDoc(i + 100, largeSizeKb);
      await pg.query(
        "INSERT INTO mixed_docs (doc_type, data) VALUES ('large', $1)",
        [JSON.stringify(largeDoc)],
      );
    }

    for (let round = 0; round < 3; round++) {
      await pg.query(
        `SELECT id, data FROM mixed_docs
         WHERE doc_type = 'small' AND data @> $1
         ORDER BY id LIMIT 3`,
        [JSON.stringify({ metadata: { priority: round % 5 } })],
      );

      const largeResult = await pg.query(
        `SELECT id, data FROM mixed_docs
         WHERE doc_type = 'large'
         ORDER BY id LIMIT 2`,
      );
      for (const row of largeResult.rows) {
        expect(row.data.id).toBeGreaterThanOrEqual(100);
        expect(Array.isArray(row.data.payload)).toBe(true);
      }

      await pg.query(
        `UPDATE mixed_docs
         SET data = jsonb_set(data, '{metadata,round}', $1)
         WHERE doc_type = 'small' AND id = $2`,
        [JSON.stringify(round), (round % 15) + 1],
      );
    }

    const finalCount = await pg.query(
      `SELECT count(*) as cnt FROM mixed_docs`,
    );
    expect(Number(finalCount.rows[0].cnt)).toBe(20);

    const withRound = await pg.query(
      `SELECT count(*) as cnt FROM mixed_docs WHERE data->'metadata' ? 'round'`,
    );
    expect(Number(withRound.rows[0].cnt)).toBeGreaterThan(0);
  },
);
