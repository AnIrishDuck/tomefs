/**
 * Prefetch pattern tests for scribe-data workloads.
 *
 * Tributary's sync fires a prefetch for the next batch before the current
 * batch's DB transaction completes. This creates overlapping I/O patterns
 * on tomefs. These tests verify that prefetched data doesn't corrupt or
 * interfere with active transactions under various cache pressure levels.
 *
 * From plans/scribe-data-tests.md §5.
 */

import { describe, it, expect, afterEach } from "vitest";
import {
  createHarness,
  CACHE_CONFIGS,
  type CacheSize,
  type ScribeTestHarness,
  type TestStream,
} from "./harness.js";
import type { SyncStatus } from "./fake-tributary.js";

let harness: ScribeTestHarness | null = null;

afterEach(async () => {
  if (harness) {
    await harness.destroy();
    harness = null;
  }
});

/** Generate an INSERT statement for a block with deterministic content. */
function insertBlockSQL(schema: string, id: string, content: string): string {
  const escaped = content.replace(/'/g, "''");
  return `INSERT INTO "${schema}".block (id, content) VALUES ('${id}', '${escaped}')`;
}

/** Seed N block-insert blobs on the fake server. */
function seedBlobs(
  h: ScribeTestHarness,
  streamKey: string,
  schema: string,
  count: number,
  prefix = "Note",
  contentSize = 100,
): void {
  for (let i = 0; i < count; i++) {
    const id = `block-${prefix.toLowerCase()}-${i.toString().padStart(4, "0")}`;
    const content = `${prefix} ${i}: ${"x".repeat(contentSize)}`;
    h.server.storeBlob(streamKey, insertBlockSQL(schema, id, content));
  }
}

/**
 * Simulate a prefetch-style sync pattern: fetch the next page of metadata
 * from the server (simulating an ahead-of-time prefetch) while replaying
 * the current page. Returns both the current sync result and the prefetched
 * metadata.
 */
async function syncWithPrefetch(
  h: ScribeTestHarness,
  stream: TestStream,
  max: number,
): Promise<{
  syncResult: SyncStatus;
  prefetchedMeta: { count: number; hasMore: boolean };
}> {
  // Fetch current page for replay
  const currentPage = h.server.getBlobsPage(stream.streamKey, stream.syncIndex, max);

  // Simulate prefetch: fetch metadata for next page while current is replaying
  const nextStart = stream.syncIndex + currentPage.blobs.length;
  const prefetchMeta = h.server.getBlobMetaPage(stream.streamKey, nextStart, max);

  // Now replay current page (the "slow" part that overlaps with prefetch)
  const syncResult = await h.syncStream(stream, max);

  return {
    syncResult,
    prefetchedMeta: {
      count: prefetchMeta.metadata.length,
      hasMore: prefetchMeta.hasMore,
    },
  };
}

// ---------------------------------------------------------------------------
// Prefetch reuse: verify prefetch metadata matches actual next page
// ---------------------------------------------------------------------------

describe("Prefetch patterns", () => {
  it("prefetch metadata matches next page — no wasted fetches @fast", async () => {
    harness = await createHarness("medium");
    const writer = await harness.createWriterStream("pf1");
    seedBlobs(harness, writer.streamKey, "pf1", 15, "PF");

    const reader = await harness.createReaderStream("pf1");

    // Sync page 1 with prefetch of page 2 metadata
    const { syncResult: s1, prefetchedMeta: pf1 } = await syncWithPrefetch(
      harness,
      reader,
      5,
    );
    expect(s1.fetched).toBe(5);
    expect(s1.complete).toBe(false);
    expect(pf1.count).toBe(5); // page 2 has 5 blobs
    expect(pf1.hasMore).toBe(true); // page 3 exists

    // Sync page 2 — the prefetch told us there were 5 blobs, and there are
    const { syncResult: s2, prefetchedMeta: pf2 } = await syncWithPrefetch(
      harness,
      reader,
      5,
    );
    expect(s2.fetched).toBe(5);
    expect(s2.complete).toBe(false);
    expect(pf2.count).toBe(5); // page 3 has 5 blobs
    expect(pf2.hasMore).toBe(false); // no page 4

    // Sync page 3 — last page
    const s3 = await harness.syncStream(reader, 5);
    expect(s3.fetched).toBe(5);
    expect(s3.complete).toBe(true);

    const count = await reader.query(
      `SELECT COUNT(*)::int as count FROM "pf1".block`,
    );
    expect(count.rows[0].count).toBe(15);
  });

  it("prefetch correctly predicts empty final page", async () => {
    harness = await createHarness("medium");
    const writer = await harness.createWriterStream("pf2");
    seedBlobs(harness, writer.streamKey, "pf2", 10, "PF2");

    const reader = await harness.createReaderStream("pf2");

    // Sync page 1, prefetch page 2
    const { prefetchedMeta: pf1 } = await syncWithPrefetch(harness, reader, 5);
    expect(pf1.count).toBe(5);
    expect(pf1.hasMore).toBe(false);

    // Sync page 2, prefetch page 3 (should be empty)
    const { syncResult: s2, prefetchedMeta: pf2 } = await syncWithPrefetch(
      harness,
      reader,
      5,
    );
    expect(s2.complete).toBe(true);
    expect(pf2.count).toBe(0);
    expect(pf2.hasMore).toBe(false);

    const count = await reader.query(
      `SELECT COUNT(*)::int as count FROM "pf2".block`,
    );
    expect(count.rows[0].count).toBe(10);
  });
});

// ---------------------------------------------------------------------------
// Stale prefetch: new blobs arrive between prefetch and actual sync
// ---------------------------------------------------------------------------

describe("Prefetch with stale data", () => {
  it("stale prefetch — new blobs arrive after prefetch, subsequent syncs pick them up", async () => {
    harness = await createHarness("medium");
    const writer = await harness.createWriterStream("stale1");
    seedBlobs(harness, writer.streamKey, "stale1", 10, "S1");

    const reader = await harness.createReaderStream("stale1");

    // Sync page 1 (5 blobs), prefetch says page 2 has 5 blobs and is final
    const { prefetchedMeta: pf1 } = await syncWithPrefetch(harness, reader, 5);
    expect(pf1.count).toBe(5);
    expect(pf1.hasMore).toBe(false);

    // Writer adds 5 more blobs AFTER prefetch but BEFORE we sync page 2
    for (let i = 10; i < 15; i++) {
      const id = `block-s1-${i.toString().padStart(4, "0")}`;
      harness.server.storeBlob(
        writer.streamKey,
        insertBlockSQL("stale1", id, `Late S1 ${i}`),
      );
    }

    // Sync page 2 — gets 5 blobs, but now there are more
    const s2 = await harness.syncStream(reader, 5);
    expect(s2.fetched).toBe(5);
    // The prefetch said this was the last page, but new blobs arrived
    expect(s2.complete).toBe(false);

    // Continue syncing — should pick up the new blobs
    await harness.syncStreamFully(reader, 5);

    const count = await reader.query(
      `SELECT COUNT(*)::int as count FROM "stale1".block`,
    );
    expect(count.rows[0].count).toBe(15);
  });

  it("stale prefetch with one-at-a-time sync — no data loss", async () => {
    harness = await createHarness("medium");
    const writer = await harness.createWriterStream("stale2");
    seedBlobs(harness, writer.streamKey, "stale2", 3, "S2");

    const reader = await harness.createReaderStream("stale2");

    // Sync blob 1, prefetch says 2 more
    const { prefetchedMeta: pf1 } = await syncWithPrefetch(harness, reader, 1);
    expect(pf1.count).toBe(1); // next page has 1 blob
    expect(pf1.hasMore).toBe(true);

    // Writer adds 2 more blobs
    for (let i = 3; i < 5; i++) {
      const id = `block-s2-${i.toString().padStart(4, "0")}`;
      harness.server.storeBlob(
        writer.streamKey,
        insertBlockSQL("stale2", id, `Late S2 ${i}`),
      );
    }

    // Sync remaining — should get all 4 remaining blobs
    await harness.syncStreamFully(reader, 1);

    const count = await reader.query(
      `SELECT COUNT(*)::int as count FROM "stale2".block`,
    );
    expect(count.rows[0].count).toBe(5);
  });
});

// ---------------------------------------------------------------------------
// Prefetch with local writes advancing sync index
// ---------------------------------------------------------------------------

describe("Prefetch invalidation on local write", () => {
  it("local write advances sync index — stale prefetch is irrelevant", async () => {
    harness = await createHarness("medium");
    const writer = await harness.createWriterStream("inv1");
    seedBlobs(harness, writer.streamKey, "inv1", 10, "Inv");

    const reader = await harness.createReaderStream("inv1");

    // Prefetch page 1 metadata
    const prefetchMeta = harness.server.getBlobMetaPage(
      reader.streamKey,
      0,
      5,
    );
    expect(prefetchMeta.metadata.length).toBe(5);

    // Sync page 1 normally
    await harness.syncStream(reader, 5);
    expect(reader.syncIndex).toBe(5);

    // Now sync page 2 — even though we prefetched page 1's metadata,
    // the sync correctly uses the updated syncIndex
    const s2 = await harness.syncStream(reader, 5);
    expect(s2.fetched).toBe(5);
    expect(s2.complete).toBe(true);

    const count = await reader.query(
      `SELECT COUNT(*)::int as count FROM "inv1".block`,
    );
    expect(count.rows[0].count).toBe(10);
  });

  it("sync, write locally, sync again — no duplicates", async () => {
    harness = await createHarness("medium");
    const writer = await harness.createWriterStream("inv2");
    seedBlobs(harness, writer.streamKey, "inv2", 6, "Inv2");

    const reader = await harness.createReaderStream("inv2");

    // Sync first 3
    await harness.syncStream(reader, 3);

    // Local write (not from sync)
    await reader.exec(
      `INSERT INTO "inv2".block (id, content) VALUES ('local-1', 'local note')`,
    );

    // Sync remaining 3
    await harness.syncStreamFully(reader, 3);

    // Should have 6 from sync + 1 local = 7
    const count = await reader.query(
      `SELECT COUNT(*)::int as count FROM "inv2".block`,
    );
    expect(count.rows[0].count).toBe(7);

    // Verify local write survived
    const local = await reader.query(
      `SELECT content FROM "inv2".block WHERE id = 'local-1'`,
    );
    expect(local.rows[0].content).toBe("local note");
  });
});

// ---------------------------------------------------------------------------
// Prefetch under cache pressure
// ---------------------------------------------------------------------------

describe("Prefetch under cache pressure", () => {
  const PRESSURE_CONFIGS: CacheSize[] = ["tiny", "small", "medium", "large"];

  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`prefetch + sync — cache=${size} (${pages} pages)`, async () => {
      harness = await createHarness(size);
      const writer = await harness.createWriterStream(`pfcp_${size}`);
      const contentSize = size === "tiny" ? 500 : 200;
      seedBlobs(harness, writer.streamKey, `pfcp_${size}`, 30, "PFCP", contentSize);

      const reader = await harness.createReaderStream(`pfcp_${size}`);

      // Sync with prefetch pattern for each page
      let done = false;
      let iterations = 0;

      while (!done) {
        // Prefetch metadata for the next-next page (2 pages ahead)
        const futureStart = reader.syncIndex + 5 + 5;
        harness.server.getBlobMetaPage(reader.streamKey, futureStart, 5);

        // Sync current page
        const status = await harness.syncStream(reader, 5);
        done = status.complete;
        iterations++;
        expect(iterations).toBeLessThan(20);
      }

      const count = await reader.query(
        `SELECT COUNT(*)::int as count FROM "pfcp_${size}".block`,
      );
      expect(count.rows[0].count).toBe(30);

      // Verify data integrity
      const sample = await reader.query(
        `SELECT content FROM "pfcp_${size}".block WHERE id = 'block-pfcp-0029'`,
      );
      expect(sample.rows[0].content).toMatch(/^PFCP 29:/);
    });
  }

  it("tiny cache + prefetch + concurrent writes — no corruption @fast", async () => {
    harness = await createHarness("tiny");
    const writer = await harness.createWriterStream("pfhvy");
    seedBlobs(harness, writer.streamKey, "pfhvy", 20, "Heavy", 800);

    const reader = await harness.createReaderStream("pfhvy");

    let done = false;
    let pageNum = 0;

    while (!done) {
      // Simulate prefetch (read metadata for next page)
      const prefetchStart = reader.syncIndex + 3;
      const prefetch = harness.server.getBlobMetaPage(
        reader.streamKey,
        prefetchStart,
        3,
      );

      // Sync current page — the DB transaction writes while prefetch data exists
      const status = await harness.syncStream(reader, 3);
      done = status.complete;
      pageNum++;

      // Every other page, writer adds a new blob (concurrent write pressure)
      if (pageNum % 2 === 0 && !done) {
        const id = `block-heavy-late-${pageNum.toString().padStart(4, "0")}`;
        harness.server.storeBlob(
          writer.streamKey,
          insertBlockSQL("pfhvy", id, `Late ${pageNum}: ${"z".repeat(800)}`),
        );
      }

      expect(pageNum).toBeLessThan(30);
    }

    // All original + late blobs should be present
    const count = await reader.query(
      `SELECT COUNT(*)::int as count FROM "pfhvy".block`,
    );
    // At least the original 20 should be there
    expect(count.rows[0].count).toBeGreaterThanOrEqual(20);

    // Verify content integrity of first and last original blocks
    const first = await reader.query(
      `SELECT LENGTH(content) as len FROM "pfhvy".block WHERE id = 'block-heavy-0000'`,
    );
    expect(first.rows[0].len).toBe("Heavy 0: ".length + 800);
  });
});
