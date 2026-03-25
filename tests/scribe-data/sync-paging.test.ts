/**
 * Sync paging tests for scribe-data workloads.
 *
 * These test that paged sync correctly replays SQL against PGlite-on-tomefs
 * without data loss or corruption. Highest priority in the scribe-data test
 * plan — they exercise the paging/sync patterns that stress the page cache.
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

/** Generate an INSERT statement for a block with deterministic content. */
function insertBlockSQL(schema: string, id: string, content: string): string {
  // Escape single quotes for SQL
  const escaped = content.replace(/'/g, "''");
  return `INSERT INTO "${schema}".block (id, content) VALUES ('${id}', '${escaped}')`;
}

/** Store N block-insert blobs on the fake server for a given stream. */
function seedBlobs(
  h: ScribeTestHarness,
  streamKey: string,
  schema: string,
  count: number,
  contentSize = 100,
): void {
  for (let i = 0; i < count; i++) {
    const id = `block-${i.toString().padStart(4, "0")}`;
    const content = `Note ${i}: ${"x".repeat(contentSize)}`;
    h.server.storeBlob(streamKey, insertBlockSQL(schema, id, content));
  }
}

// ---------------------------------------------------------------------------
// Scenario matrix: sync paging under various blob/page/cache combinations
// ---------------------------------------------------------------------------

describe("Sync paging scenarios", () => {
  // ----- Single page: all blobs in one fetch -----
  it("single page — all blobs fit in one fetch @fast", async () => {
    harness = await createHarness("medium");
    const writer = await harness.createWriterStream("sp1");
    seedBlobs(harness, writer.streamKey, "sp1", 5);

    const reader = await harness.createReaderStream("sp1");
    const status = await harness.syncStream(reader, 100);

    expect(status.complete).toBe(true);
    expect(status.fetched).toBe(5);

    const result = await reader.query(
      `SELECT COUNT(*)::int as count FROM "sp1".block`,
    );
    expect(result.rows[0].count).toBe(5);
  });

  // ----- Exact fit: blobs divide evenly into pages -----
  it("exact fit — two pages, no remainder", async () => {
    harness = await createHarness("medium");
    const writer = await harness.createWriterStream("sp2");
    seedBlobs(harness, writer.streamKey, "sp2", 6);

    const reader = await harness.createReaderStream("sp2");
    const iterations = await harness.syncStreamFully(reader, 3);

    expect(iterations).toBe(2);
    const result = await reader.query(
      `SELECT COUNT(*)::int as count FROM "sp2".block`,
    );
    expect(result.rows[0].count).toBe(6);
  });

  // ----- Remainder: pages don't divide evenly -----
  it("remainder — two full pages + partial third", async () => {
    harness = await createHarness("medium");
    const writer = await harness.createWriterStream("sp3");
    seedBlobs(harness, writer.streamKey, "sp3", 7);

    const reader = await harness.createReaderStream("sp3");
    const iterations = await harness.syncStreamFully(reader, 3);

    expect(iterations).toBe(3); // 3 + 3 + 1
    const result = await reader.query(
      `SELECT COUNT(*)::int as count FROM "sp3".block`,
    );
    expect(result.rows[0].count).toBe(7);
  });

  // ----- One at a time: max=1 edge case -----
  it("one-at-a-time — max=1 edge case", async () => {
    harness = await createHarness("medium");
    const writer = await harness.createWriterStream("sp4");
    seedBlobs(harness, writer.streamKey, "sp4", 5);

    const reader = await harness.createReaderStream("sp4");
    const iterations = await harness.syncStreamFully(reader, 1);

    expect(iterations).toBe(5);
    const result = await reader.query(
      `SELECT COUNT(*)::int as count FROM "sp4".block`,
    );
    expect(result.rows[0].count).toBe(5);
  });

  // ----- Empty server: no-op sync -----
  it("empty server — no-op sync", async () => {
    harness = await createHarness("medium");
    await harness.createWriterStream("sp5");
    // Don't seed any blobs

    const reader = await harness.createReaderStream("sp5");
    const status = await harness.syncStream(reader, 10);

    expect(status.complete).toBe(true);
    expect(status.fetched).toBe(0);

    const result = await reader.query(
      `SELECT COUNT(*)::int as count FROM "sp5".block`,
    );
    expect(result.rows[0].count).toBe(0);
  });

  // ----- Already synced: re-sync returns immediately -----
  it("already synced — re-sync is a no-op", async () => {
    harness = await createHarness("medium");
    const writer = await harness.createWriterStream("sp6");
    seedBlobs(harness, writer.streamKey, "sp6", 10);

    const reader = await harness.createReaderStream("sp6");
    await harness.syncStreamFully(reader, 10);

    // Sync again — should be a no-op
    const status = await harness.syncStream(reader, 10);
    expect(status.complete).toBe(true);
    expect(status.fetched).toBe(0);

    const result = await reader.query(
      `SELECT COUNT(*)::int as count FROM "sp6".block`,
    );
    expect(result.rows[0].count).toBe(10);
  });

  // ----- Large batch, small page size -----
  it("large batch with small page size — many iterations", async () => {
    harness = await createHarness("medium");
    const writer = await harness.createWriterStream("sp7");
    seedBlobs(harness, writer.streamKey, "sp7", 100);

    const reader = await harness.createReaderStream("sp7");
    const iterations = await harness.syncStreamFully(reader, 2);

    expect(iterations).toBe(50);
    const result = await reader.query(
      `SELECT COUNT(*)::int as count FROM "sp7".block`,
    );
    expect(result.rows[0].count).toBe(100);

    // Verify data ordering
    const ordered = await reader.query(
      `SELECT id FROM "sp7".block ORDER BY id`,
    );
    expect(ordered.rows[0].id).toBe("block-0000");
    expect(ordered.rows[99].id).toBe("block-0099");
  });
});

// ---------------------------------------------------------------------------
// Sync under cache pressure
// ---------------------------------------------------------------------------

describe("Sync paging under cache pressure", () => {
  const PRESSURE_CONFIGS: CacheSize[] = ["tiny", "small", "medium", "large"];

  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`50 blobs in pages of 5 — cache=${size} (${pages} pages)`, async () => {
      harness = await createHarness(size);
      const writer = await harness.createWriterStream("cp1");
      seedBlobs(harness, writer.streamKey, "cp1", 50);

      const reader = await harness.createReaderStream("cp1");
      const iterations = await harness.syncStreamFully(reader, 5);

      expect(iterations).toBe(10);

      const result = await reader.query(
        `SELECT COUNT(*)::int as count FROM "cp1".block`,
      );
      expect(result.rows[0].count).toBe(50);

      // Verify content integrity of first and last blocks
      const first = await reader.query(
        `SELECT content FROM "cp1".block WHERE id = 'block-0000'`,
      );
      expect(first.rows[0].content).toMatch(/^Note 0:/);

      const last = await reader.query(
        `SELECT content FROM "cp1".block WHERE id = 'block-0049'`,
      );
      expect(last.rows[0].content).toMatch(/^Note 49:/);
    });
  }

  it("tiny cache + large sync — heavy eviction during SQL replay @fast", async () => {
    harness = await createHarness("tiny");
    const writer = await harness.createWriterStream("cp2");
    // 50 blobs with larger content to increase page pressure
    seedBlobs(harness, writer.streamKey, "cp2", 50, 500);

    const reader = await harness.createReaderStream("cp2");
    await harness.syncStreamFully(reader, 5);

    const result = await reader.query(
      `SELECT COUNT(*)::int as count FROM "cp2".block`,
    );
    expect(result.rows[0].count).toBe(50);

    // Verify all content survived eviction
    const all = await reader.query(
      `SELECT id, LENGTH(content) as len FROM "cp2".block ORDER BY id`,
    );
    for (let i = 0; i < 50; i++) {
      const expected = `Note ${i}: `.length + 500;
      expect(all.rows[i].len).toBe(expected);
    }
  });
});

// ---------------------------------------------------------------------------
// Sync edge cases
// ---------------------------------------------------------------------------

describe("Sync paging edge cases", () => {
  // ----- Disconnect mid-sync -----
  it("disconnect mid-sync — error, reconnect, resume", async () => {
    harness = await createHarness("medium");
    const writer = await harness.createWriterStream("ec1");
    seedBlobs(harness, writer.streamKey, "ec1", 10);

    const reader = await harness.createReaderStream("ec1");

    // Sync first page
    const status1 = await harness.syncStream(reader, 3);
    expect(status1.fetched).toBe(3);
    expect(status1.complete).toBe(false);

    // Disconnect
    harness.server.disconnect();
    await expect(harness.syncStream(reader, 3)).rejects.toThrow("disconnected");

    // Data from first page should still be intact
    const partial = await reader.query(
      `SELECT COUNT(*)::int as count FROM "ec1".block`,
    );
    expect(partial.rows[0].count).toBe(3);

    // Reconnect and resume
    harness.server.reconnect();
    await harness.syncStreamFully(reader, 3);

    const final = await reader.query(
      `SELECT COUNT(*)::int as count FROM "ec1".block`,
    );
    expect(final.rows[0].count).toBe(10);
  });

  // ----- New blobs arrive during sync -----
  it("new blobs arrive during sync — reader eventually gets all data", async () => {
    harness = await createHarness("medium");
    const writer = await harness.createWriterStream("ec2");
    seedBlobs(harness, writer.streamKey, "ec2", 6);

    const reader = await harness.createReaderStream("ec2");

    // Sync first page of 3
    const status1 = await harness.syncStream(reader, 3);
    expect(status1.fetched).toBe(3);

    // Writer adds more blobs between sync pages
    for (let i = 6; i < 10; i++) {
      const id = `block-${i.toString().padStart(4, "0")}`;
      harness.server.storeBlob(
        writer.streamKey,
        insertBlockSQL("ec2", id, `Late note ${i}`),
      );
    }

    // Continue syncing — should pick up all remaining blobs
    await harness.syncStreamFully(reader, 3);

    const result = await reader.query(
      `SELECT COUNT(*)::int as count FROM "ec2".block`,
    );
    expect(result.rows[0].count).toBe(10);
  });

  // ----- Hash chain verification across page boundaries -----
  it("hash chain integrity across page boundaries", async () => {
    harness = await createHarness("medium");
    const writer = await harness.createWriterStream("ec3");
    seedBlobs(harness, writer.streamKey, "ec3", 9);

    // Verify that blobs fetched across pages maintain chain integrity
    const page1 = harness.server.getBlobsPage(writer.streamKey, 0, 3);
    const page2 = harness.server.getBlobsPage(writer.streamKey, 3, 3);
    const page3 = harness.server.getBlobsPage(writer.streamKey, 6, 3);

    // Last blob of page1's hash should be prevHash of first blob of page2
    expect(page2.blobs[0].prevHash).toBe(page1.blobs[2].hash);
    // Same for page2 -> page3
    expect(page3.blobs[0].prevHash).toBe(page2.blobs[2].hash);

    // Full sync should work
    const reader = await harness.createReaderStream("ec3");
    await harness.syncStreamFully(reader, 3);

    const result = await reader.query(
      `SELECT COUNT(*)::int as count FROM "ec3".block`,
    );
    expect(result.rows[0].count).toBe(9);
  });

  // ----- Sync index persistence after "restart" -----
  it("sync index persists — resume from last committed position", async () => {
    harness = await createHarness("medium");
    const writer = await harness.createWriterStream("ec4");
    seedBlobs(harness, writer.streamKey, "ec4", 10);

    // Sync first 5 blobs
    const reader1 = await harness.createReaderStream("ec4");
    await harness.syncStream(reader1, 5);
    expect(reader1.syncIndex).toBe(5);

    // "Restart" — create a new reader stream on the same PGlite.
    // It should pick up the persisted sync index.
    const reader2 = await harness.createReaderStream("ec4");
    expect(reader2.syncIndex).toBe(5);

    // Continue syncing from where we left off
    await harness.syncStreamFully(reader2, 5);

    const result = await reader2.query(
      `SELECT COUNT(*)::int as count FROM "ec4".block`,
    );
    expect(result.rows[0].count).toBe(10);
  });
});
