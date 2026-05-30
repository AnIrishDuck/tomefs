/**
 * PGlite full-text search with GIN index stress tests under cache pressure.
 *
 * GIN (Generalized Inverted Index) indexes have fundamentally different
 * on-disk structure and access patterns from B-tree indexes:
 *
 *   - **Pending list**: GIN uses a fastupdate pending list for inserts.
 *     New entries go to the pending list first, then get batch-merged
 *     into the main entry tree during VACUUM or when the list grows too
 *     large. Under cache pressure, the pending list pages compete with
 *     entry tree pages.
 *
 *   - **Entry tree**: The main GIN structure is a B-tree of lexemes, where
 *     each leaf points to a posting list (or posting tree) of heap TIDs.
 *     Lookups traverse the entry tree then scan posting lists — a pattern
 *     that touches many small, scattered pages.
 *
 *   - **Posting trees**: For high-frequency terms, posting lists overflow
 *     into dedicated posting trees — nested B-trees within the GIN index.
 *     These create deep, narrow page access patterns unlike the wide,
 *     shallow patterns of heap scans.
 *
 * These patterns stress the page cache differently from B-tree indexes:
 *   - GIN pending list flushes create bursts of random writes
 *   - Full-text queries touch many index pages (one per matching lexeme)
 *   - ts_rank scoring requires heap fetches for each matching document
 *   - VACUUM on GIN indexes merges the pending list (read pending + write
 *     entry tree + update posting lists — three competing access streams)
 *
 * Ethos §8: "Workload scenarios verify that tomefs works end-to-end under
 * realistic use"
 * Ethos §9: "Target the seams"
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

const SAMPLE_DOCS = [
  "The quick brown fox jumps over the lazy dog",
  "PostgreSQL is a powerful open source relational database system",
  "Full text search allows natural language queries on document collections",
  "Inverted indexes map terms to the documents containing them",
  "GIN indexes support multiple keys per row unlike B-tree indexes",
  "The page cache evicts least recently used pages when full",
  "Bounded memory usage is critical for embedded database applications",
  "Write-ahead logging ensures durability after system crashes",
  "Index maintenance during updates requires both insert and delete operations",
  "Vacuum reclaims dead tuples and merges the GIN pending list",
  "Trigram indexes enable fuzzy string matching and similarity search",
  "Lexeme normalization reduces inflected words to their dictionary forms",
  "Phrase search requires positional information in the inverted index",
  "Ranking functions compute relevance scores for matched documents",
  "The pending list amortizes insert cost by batching index updates",
];

function docText(i: number): string {
  const base = SAMPLE_DOCS[i % SAMPLE_DOCS.length];
  return `${base} (document ${i})`;
}

// ---------------------------------------------------------------------------
// Scenario 1: Create GIN index on tsvector column, insert, and query
// ---------------------------------------------------------------------------

describe("Scenario 1: GIN index on tsvector column", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`
        CREATE TABLE docs (
          id SERIAL PRIMARY KEY,
          body TEXT NOT NULL,
          tsv TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', body)) STORED
        )
      `);

      await pg.query(`CREATE INDEX idx_docs_tsv ON docs USING gin (tsv)`);

      const rowCount = size === "tiny" ? 15 : 30;
      for (let i = 0; i < rowCount; i++) {
        await pg.query(`INSERT INTO docs (body) VALUES ($1)`, [docText(i)]);
      }

      const result = await pg.query(
        `SELECT id, body FROM docs WHERE tsv @@ to_tsquery('english', 'database')
         ORDER BY id`,
      );
      expect(result.rows.length).toBeGreaterThan(0);
      for (const row of result.rows) {
        expect(row.body.toLowerCase()).toContain("database");
      }

      const noMatch = await pg.query(
        `SELECT id FROM docs WHERE tsv @@ to_tsquery('english', 'xyznonexistent')`,
      );
      expect(noMatch.rows.length).toBe(0);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 2: GIN index with ts_rank scoring under cache pressure
// ---------------------------------------------------------------------------

describe("Scenario 2: ts_rank scoring with GIN index", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`
        CREATE TABLE articles (
          id SERIAL PRIMARY KEY,
          title TEXT NOT NULL,
          content TEXT NOT NULL,
          tsv TSVECTOR GENERATED ALWAYS AS (
            setweight(to_tsvector('english', title), 'A') ||
            setweight(to_tsvector('english', content), 'B')
          ) STORED
        )
      `);

      await pg.query(`CREATE INDEX idx_articles_tsv ON articles USING gin (tsv)`);

      const articles = [
        { title: "Index structures in databases", content: "B-tree and GIN indexes provide efficient lookups" },
        { title: "Page cache design patterns", content: "LRU eviction with dirty page tracking" },
        { title: "Database vacuum operations", content: "Reclaiming dead tuples and updating indexes" },
        { title: "Write-ahead log recovery", content: "WAL replay ensures database durability after crashes" },
        { title: "Full text search ranking", content: "The ts_rank function scores documents by relevance to a query" },
        { title: "Inverted index maintenance", content: "GIN pending list merges during vacuum operations" },
        { title: "Cache pressure testing", content: "Small cache sizes force eviction of index and heap pages" },
        { title: "Postgres internal page layout", content: "Each database page is 8KB containing tuple headers and data" },
      ];

      for (const a of articles) {
        await pg.query(
          `INSERT INTO articles (title, content) VALUES ($1, $2)`,
          [a.title, a.content],
        );
      }

      const ranked = await pg.query(
        `SELECT id, title, ts_rank(tsv, q) AS rank
         FROM articles, to_tsquery('english', 'index & database') AS q
         WHERE tsv @@ q
         ORDER BY rank DESC`,
      );
      expect(ranked.rows.length).toBeGreaterThan(0);
      for (let i = 1; i < ranked.rows.length; i++) {
        expect(ranked.rows[i - 1].rank).toBeGreaterThanOrEqual(ranked.rows[i].rank);
      }
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 3: High-volume insert then VACUUM (pending list merge)
// ---------------------------------------------------------------------------

describe("Scenario 3: bulk insert + VACUUM (GIN pending list merge)", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`
        CREATE TABLE logs (
          id SERIAL PRIMARY KEY,
          message TEXT NOT NULL,
          tsv TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', message)) STORED
        )
      `);

      await pg.query(`CREATE INDEX idx_logs_tsv ON logs USING gin (tsv)`);

      const rowCount = size === "tiny" ? 20 : 40;
      for (let i = 0; i < rowCount; i++) {
        await pg.query(`INSERT INTO logs (message) VALUES ($1)`, [docText(i)]);
      }

      const beforeVacuum = await pg.query(
        `SELECT count(*) AS count FROM logs WHERE tsv @@ to_tsquery('english', 'search')`,
      );

      await pg.query(`VACUUM logs`);

      const afterVacuum = await pg.query(
        `SELECT count(*) AS count FROM logs WHERE tsv @@ to_tsquery('english', 'search')`,
      );
      expect(afterVacuum.rows[0].count).toBe(beforeVacuum.rows[0].count);

      const allRows = await pg.query(
        `SELECT id, message FROM logs ORDER BY id`,
      );
      expect(allRows.rows.length).toBe(rowCount);
      for (let i = 0; i < rowCount; i++) {
        expect(allRows.rows[i].message).toBe(docText(i));
      }
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 4: DELETE + VACUUM + verify GIN index consistency
// ---------------------------------------------------------------------------

describe("Scenario 4: delete + VACUUM + GIN consistency check", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`
        CREATE TABLE notes (
          id SERIAL PRIMARY KEY,
          body TEXT NOT NULL,
          tsv TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', body)) STORED
        )
      `);

      await pg.query(`CREATE INDEX idx_notes_tsv ON notes USING gin (tsv)`);

      const rowCount = size === "tiny" ? 20 : 30;
      for (let i = 0; i < rowCount; i++) {
        await pg.query(`INSERT INTO notes (body) VALUES ($1)`, [docText(i)]);
      }

      await pg.query(`DELETE FROM notes WHERE id % 3 = 0`);
      await pg.query(`VACUUM notes`);

      const remaining = await pg.query(
        `SELECT id, body FROM notes ORDER BY id`,
      );
      for (const row of remaining.rows) {
        expect(row.id % 3).not.toBe(0);
      }

      const searchResult = await pg.query(
        `SELECT id FROM notes WHERE tsv @@ to_tsquery('english', 'database') ORDER BY id`,
      );
      const seqScan = await pg.query(
        `SELECT id FROM notes WHERE body ILIKE '%database%' ORDER BY id`,
      );
      expect(searchResult.rows.map((r: any) => r.id)).toEqual(
        seqScan.rows.map((r: any) => r.id),
      );
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 5: UPDATE documents and verify GIN index tracks changes
// ---------------------------------------------------------------------------

describe("Scenario 5: UPDATE documents with GIN re-indexing", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`
        CREATE TABLE wiki (
          id SERIAL PRIMARY KEY,
          body TEXT NOT NULL,
          tsv TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', body)) STORED
        )
      `);

      await pg.query(`CREATE INDEX idx_wiki_tsv ON wiki USING gin (tsv)`);

      for (let i = 0; i < 10; i++) {
        await pg.query(`INSERT INTO wiki (body) VALUES ($1)`, [docText(i)]);
      }

      const uniqueTerm = "xylophone_unique_marker";
      await pg.query(`UPDATE wiki SET body = $1 WHERE id = 1`, [
        `This document now contains ${uniqueTerm} for testing`,
      ]);

      const found = await pg.query(
        `SELECT id FROM wiki WHERE tsv @@ to_tsquery('english', $1)`,
        [uniqueTerm],
      );
      expect(found.rows.length).toBe(1);
      expect(found.rows[0].id).toBe(1);

      const oldTermGone = await pg.query(
        `SELECT id FROM wiki WHERE tsv @@ to_tsquery('english', 'fox') AND id = 1`,
      );
      expect(oldTermGone.rows.length).toBe(0);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 6: Multiple GIN indexes on same table (compound pressure)
// ---------------------------------------------------------------------------

describe("Scenario 6: multiple GIN indexes on same table", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`
        CREATE TABLE products (
          id SERIAL PRIMARY KEY,
          name TEXT NOT NULL,
          description TEXT NOT NULL,
          tags TEXT[] NOT NULL DEFAULT '{}',
          name_tsv TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', name)) STORED,
          desc_tsv TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', description)) STORED
        )
      `);

      await pg.query(`CREATE INDEX idx_prod_name ON products USING gin (name_tsv)`);
      await pg.query(`CREATE INDEX idx_prod_desc ON products USING gin (desc_tsv)`);
      await pg.query(`CREATE INDEX idx_prod_tags ON products USING gin (tags)`);

      const products = [
        { name: "Database Engine Pro", desc: "High performance relational database with full text search", tags: ["database", "search"] },
        { name: "Cache Manager", desc: "LRU page cache with dirty tracking and eviction", tags: ["cache", "memory"] },
        { name: "Index Builder", desc: "Automated GIN and B-tree index creation and maintenance", tags: ["database", "index"] },
        { name: "WAL Recovery Tool", desc: "Write-ahead log replay for crash recovery", tags: ["database", "recovery"] },
        { name: "Search Analyzer", desc: "Natural language processing for full text search queries", tags: ["search", "nlp"] },
      ];

      for (const p of products) {
        await pg.query(
          `INSERT INTO products (name, description, tags) VALUES ($1, $2, $3)`,
          [p.name, p.desc, p.tags],
        );
      }

      const nameSearch = await pg.query(
        `SELECT id FROM products WHERE name_tsv @@ to_tsquery('english', 'database')`,
      );
      expect(nameSearch.rows.length).toBe(1);

      const descSearch = await pg.query(
        `SELECT id FROM products WHERE desc_tsv @@ to_tsquery('english', 'database')`,
      );
      expect(descSearch.rows.length).toBeGreaterThanOrEqual(1);

      const tagSearch = await pg.query(
        `SELECT id FROM products WHERE tags @> ARRAY['database']`,
      );
      expect(tagSearch.rows.length).toBe(3);

      const combined = await pg.query(
        `SELECT id FROM products
         WHERE desc_tsv @@ to_tsquery('english', 'search')
         AND tags @> ARRAY['search']`,
      );
      expect(combined.rows.length).toBeGreaterThanOrEqual(1);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 7: REINDEX GIN under cache pressure
// ---------------------------------------------------------------------------

describe("Scenario 7: REINDEX GIN index", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`
        CREATE TABLE emails (
          id SERIAL PRIMARY KEY,
          subject TEXT NOT NULL,
          tsv TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', subject)) STORED
        )
      `);

      await pg.query(`CREATE INDEX idx_emails_tsv ON emails USING gin (tsv)`);

      for (let i = 0; i < 15; i++) {
        await pg.query(`INSERT INTO emails (subject) VALUES ($1)`, [docText(i)]);
      }

      const beforeReindex = await pg.query(
        `SELECT id FROM emails WHERE tsv @@ to_tsquery('english', 'cache') ORDER BY id`,
      );

      await pg.query(`REINDEX INDEX idx_emails_tsv`);

      const afterReindex = await pg.query(
        `SELECT id FROM emails WHERE tsv @@ to_tsquery('english', 'cache') ORDER BY id`,
      );
      expect(afterReindex.rows.map((r: any) => r.id)).toEqual(
        beforeReindex.rows.map((r: any) => r.id),
      );

      const allRows = await pg.query(
        `SELECT count(*) AS count FROM emails`,
      );
      expect(parseInt(allRows.rows[0].count)).toBe(15);
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 8: Persistence round-trip with GIN index
// ---------------------------------------------------------------------------

describe("Scenario 8: GIN index persistence round-trip", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages)`, async () => {
      const backend = new SyncMemoryBackend();

      const h1 = await create(size, backend);
      const { pg: pg1 } = h1;

      await pg1.query(`
        CREATE TABLE persisted_docs (
          id SERIAL PRIMARY KEY,
          body TEXT NOT NULL,
          tsv TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', body)) STORED
        )
      `);

      await pg1.query(
        `CREATE INDEX idx_persisted_tsv ON persisted_docs USING gin (tsv)`,
      );

      for (let i = 0; i < 10; i++) {
        await pg1.query(`INSERT INTO persisted_docs (body) VALUES ($1)`, [
          docText(i),
        ]);
      }

      await h1.syncToFs();
      await h1.destroy();
      harnesses = harnesses.filter((x) => x !== h1);

      const h2 = await create(size, backend);
      const { pg: pg2 } = h2;

      const rows = await pg2.query(
        `SELECT id, body FROM persisted_docs ORDER BY id`,
      );
      expect(rows.rows.length).toBe(10);

      const searchResult = await pg2.query(
        `SELECT id FROM persisted_docs
         WHERE tsv @@ to_tsquery('english', 'search')
         ORDER BY id`,
      );
      expect(searchResult.rows.length).toBeGreaterThan(0);

      const seqScan = await pg2.query(
        `SELECT id FROM persisted_docs
         WHERE body ILIKE '%search%'
         ORDER BY id`,
      );
      expect(searchResult.rows.map((r: any) => r.id)).toEqual(
        seqScan.rows.map((r: any) => r.id),
      );
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 9: Phrase search and complex tsquery under cache pressure
// ---------------------------------------------------------------------------

describe("Scenario 9: phrase search and complex tsquery", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`
        CREATE TABLE passages (
          id SERIAL PRIMARY KEY,
          text TEXT NOT NULL,
          tsv TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', text)) STORED
        )
      `);

      await pg.query(`CREATE INDEX idx_passages_tsv ON passages USING gin (tsv)`);

      const passages = [
        "The quick brown fox jumps over the lazy dog in the garden",
        "A lazy cat sleeps on the brown sofa near the garden fence",
        "Quick database queries require proper index maintenance",
        "The garden has brown and green plants growing quickly",
        "Fox hunting is banned in many countries around the world",
      ];

      for (const p of passages) {
        await pg.query(`INSERT INTO passages (text) VALUES ($1)`, [p]);
      }

      const phraseResult = await pg.query(
        `SELECT id FROM passages WHERE tsv @@ phraseto_tsquery('english', 'brown fox')`,
      );
      expect(phraseResult.rows.length).toBeGreaterThanOrEqual(1);

      const orQuery = await pg.query(
        `SELECT id FROM passages WHERE tsv @@ to_tsquery('english', 'fox | cat')`,
      );
      expect(orQuery.rows.length).toBeGreaterThanOrEqual(2);

      const andQuery = await pg.query(
        `SELECT id FROM passages WHERE tsv @@ to_tsquery('english', 'garden & brown')`,
      );
      expect(andQuery.rows.length).toBeGreaterThanOrEqual(1);

      const notQuery = await pg.query(
        `SELECT id FROM passages
         WHERE tsv @@ to_tsquery('english', 'garden & !fox')`,
      );
      for (const row of notQuery.rows) {
        const text = (
          await pg.query(
            `SELECT text FROM passages WHERE id = $1`,
            [row.id],
          )
        ).rows[0].text;
        expect(text.toLowerCase()).toContain("garden");
        expect(text.toLowerCase()).not.toMatch(/\bfox\b/);
      }
    });
  }
});

// ---------------------------------------------------------------------------
// Scenario 10: GIN array index with overlapping operations
// ---------------------------------------------------------------------------

describe("Scenario 10: GIN array index operations", () => {
  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`cache=${size} (${pages} pages) @fast`, async () => {
      const h = await create(size);
      const { pg } = h;

      await pg.query(`
        CREATE TABLE tagged_items (
          id SERIAL PRIMARY KEY,
          name TEXT NOT NULL,
          labels TEXT[] NOT NULL
        )
      `);

      await pg.query(
        `CREATE INDEX idx_labels ON tagged_items USING gin (labels)`,
      );

      const items = [
        { name: "item1", labels: ["red", "blue", "green"] },
        { name: "item2", labels: ["blue", "yellow"] },
        { name: "item3", labels: ["red", "yellow", "purple"] },
        { name: "item4", labels: ["green", "purple"] },
        { name: "item5", labels: ["red", "green", "blue", "yellow"] },
        { name: "item6", labels: ["purple"] },
        { name: "item7", labels: ["red"] },
        { name: "item8", labels: ["blue", "purple", "green"] },
      ];

      for (const item of items) {
        await pg.query(
          `INSERT INTO tagged_items (name, labels) VALUES ($1, $2)`,
          [item.name, item.labels],
        );
      }

      const containsRed = await pg.query(
        `SELECT name FROM tagged_items WHERE labels @> ARRAY['red'] ORDER BY name`,
      );
      const expectedRed = items
        .filter((i) => i.labels.includes("red"))
        .map((i) => i.name)
        .sort();
      expect(containsRed.rows.map((r: any) => r.name)).toEqual(expectedRed);

      const containsBoth = await pg.query(
        `SELECT name FROM tagged_items
         WHERE labels @> ARRAY['red', 'blue']
         ORDER BY name`,
      );
      const expectedBoth = items
        .filter((i) => i.labels.includes("red") && i.labels.includes("blue"))
        .map((i) => i.name)
        .sort();
      expect(containsBoth.rows.map((r: any) => r.name)).toEqual(expectedBoth);

      const overlap = await pg.query(
        `SELECT name FROM tagged_items
         WHERE labels && ARRAY['yellow', 'purple']
         ORDER BY name`,
      );
      const expectedOverlap = items
        .filter(
          (i) =>
            i.labels.includes("yellow") || i.labels.includes("purple"),
        )
        .map((i) => i.name)
        .sort();
      expect(overlap.rows.map((r: any) => r.name)).toEqual(expectedOverlap);

      await pg.query(
        `UPDATE tagged_items SET labels = ARRAY['orange'] WHERE name = 'item1'`,
      );

      const noLongerRed = await pg.query(
        `SELECT name FROM tagged_items WHERE labels @> ARRAY['red'] ORDER BY name`,
      );
      expect(noLongerRed.rows.map((r: any) => r.name)).not.toContain("item1");

      const nowOrange = await pg.query(
        `SELECT name FROM tagged_items WHERE labels @> ARRAY['orange']`,
      );
      expect(nowOrange.rows.length).toBe(1);
      expect(nowOrange.rows[0].name).toBe("item1");
    });
  }
});
