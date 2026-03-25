/**
 * Multi-stream tests for scribe-data workloads.
 *
 * Scribe supports multiple libraries, each backed by a separate tributary
 * stream with its own PG schema. These tests verify that concurrent sync
 * of multiple streams under cache pressure doesn't cause cross-contamination
 * or data corruption.
 *
 * From plans/scribe-data-tests.md §4.
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
  const escaped = content.replace(/'/g, "''");
  return `INSERT INTO "${schema}".block (id, content) VALUES ('${id}', '${escaped}')`;
}

/** Seed N block-insert blobs on the fake server for a given stream/schema. */
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

// ---------------------------------------------------------------------------
// Sequential sync of multiple streams
// ---------------------------------------------------------------------------

describe("Multi-stream: sequential sync", () => {
  it("two streams, sequential sync — both have correct data @fast", async () => {
    harness = await createHarness("medium");
    const writerA = await harness.createWriterStream("libA");
    const writerB = await harness.createWriterStream("libB");

    seedBlobs(harness, writerA.streamKey, "libA", 20, "LibA");
    seedBlobs(harness, writerB.streamKey, "libB", 20, "LibB");

    // Sync A fully, then B fully
    const readerA = await harness.createReaderStream("libA");
    await harness.syncStreamFully(readerA, 5);

    const readerB = await harness.createReaderStream("libB");
    await harness.syncStreamFully(readerB, 5);

    // Verify A
    const resultA = await readerA.query(
      `SELECT COUNT(*)::int as count FROM "libA".block`,
    );
    expect(resultA.rows[0].count).toBe(20);

    // Verify B
    const resultB = await readerB.query(
      `SELECT COUNT(*)::int as count FROM "libB".block`,
    );
    expect(resultB.rows[0].count).toBe(20);

    // Verify content is from correct stream
    const sampleA = await readerA.query(
      `SELECT content FROM "libA".block WHERE id = 'block-liba-0000'`,
    );
    expect(sampleA.rows[0].content).toMatch(/^LibA 0:/);

    const sampleB = await readerB.query(
      `SELECT content FROM "libB".block WHERE id = 'block-libb-0000'`,
    );
    expect(sampleB.rows[0].content).toMatch(/^LibB 0:/);
  });

  it("three streams, different sizes — all sync correctly", async () => {
    harness = await createHarness("medium");
    const writerA = await harness.createWriterStream("alpha");
    const writerB = await harness.createWriterStream("beta");
    const writerC = await harness.createWriterStream("gamma");

    seedBlobs(harness, writerA.streamKey, "alpha", 5, "Alpha");
    seedBlobs(harness, writerB.streamKey, "beta", 30, "Beta");
    seedBlobs(harness, writerC.streamKey, "gamma", 15, "Gamma");

    const readerA = await harness.createReaderStream("alpha");
    const readerB = await harness.createReaderStream("beta");
    const readerC = await harness.createReaderStream("gamma");

    await harness.syncStreamFully(readerA, 10);
    await harness.syncStreamFully(readerB, 10);
    await harness.syncStreamFully(readerC, 10);

    const countA = await readerA.query(
      `SELECT COUNT(*)::int as count FROM "alpha".block`,
    );
    const countB = await readerB.query(
      `SELECT COUNT(*)::int as count FROM "beta".block`,
    );
    const countC = await readerC.query(
      `SELECT COUNT(*)::int as count FROM "gamma".block`,
    );

    expect(countA.rows[0].count).toBe(5);
    expect(countB.rows[0].count).toBe(30);
    expect(countC.rows[0].count).toBe(15);
  });
});

// ---------------------------------------------------------------------------
// Interleaved sync of multiple streams
// ---------------------------------------------------------------------------

describe("Multi-stream: interleaved sync", () => {
  it("two streams, interleaved page-by-page — no cross-contamination @fast", async () => {
    harness = await createHarness("medium");
    const writerA = await harness.createWriterStream("intA");
    const writerB = await harness.createWriterStream("intB");

    seedBlobs(harness, writerA.streamKey, "intA", 12, "StreamA");
    seedBlobs(harness, writerB.streamKey, "intB", 12, "StreamB");

    const readerA = await harness.createReaderStream("intA");
    const readerB = await harness.createReaderStream("intB");

    // Interleave: sync A page, sync B page, repeat
    let doneA = false;
    let doneB = false;
    let iterations = 0;

    while (!doneA || !doneB) {
      if (!doneA) {
        const statusA = await harness.syncStream(readerA, 3);
        doneA = statusA.complete;
      }
      if (!doneB) {
        const statusB = await harness.syncStream(readerB, 3);
        doneB = statusB.complete;
      }
      iterations++;
      expect(iterations).toBeLessThan(20);
    }

    // Both should have exactly their own data
    const countA = await readerA.query(
      `SELECT COUNT(*)::int as count FROM "intA".block`,
    );
    const countB = await readerB.query(
      `SELECT COUNT(*)::int as count FROM "intB".block`,
    );
    expect(countA.rows[0].count).toBe(12);
    expect(countB.rows[0].count).toBe(12);

    // Verify no cross-contamination: A's blocks should only contain StreamA content
    const allA = await readerA.query(
      `SELECT content FROM "intA".block ORDER BY id`,
    );
    for (const row of allA.rows) {
      expect(row.content).toMatch(/^StreamA/);
    }

    const allB = await readerB.query(
      `SELECT content FROM "intB".block ORDER BY id`,
    );
    for (const row of allB.rows) {
      expect(row.content).toMatch(/^StreamB/);
    }
  });

  it("three streams, round-robin interleave — all data correct", async () => {
    harness = await createHarness("medium");
    const writers = await Promise.all([
      harness.createWriterStream("rr1"),
      harness.createWriterStream("rr2"),
      harness.createWriterStream("rr3"),
    ]);

    seedBlobs(harness, writers[0].streamKey, "rr1", 9, "R1");
    seedBlobs(harness, writers[1].streamKey, "rr2", 9, "R2");
    seedBlobs(harness, writers[2].streamKey, "rr3", 9, "R3");

    const readers = await Promise.all([
      harness.createReaderStream("rr1"),
      harness.createReaderStream("rr2"),
      harness.createReaderStream("rr3"),
    ]);

    const done = [false, false, false];
    let iterations = 0;

    while (done.some((d) => !d)) {
      for (let i = 0; i < 3; i++) {
        if (!done[i]) {
          const status = await harness.syncStream(readers[i], 3);
          done[i] = status.complete;
        }
      }
      iterations++;
      expect(iterations).toBeLessThan(20);
    }

    for (let i = 0; i < 3; i++) {
      const schema = `rr${i + 1}`;
      const count = await readers[i].query(
        `SELECT COUNT(*)::int as count FROM "${schema}".block`,
      );
      expect(count.rows[0].count).toBe(9);
    }
  });
});

// ---------------------------------------------------------------------------
// Schema isolation
// ---------------------------------------------------------------------------

describe("Multi-stream: schema isolation", () => {
  it("stream B cannot see stream A data", async () => {
    harness = await createHarness("medium");
    const writerA = await harness.createWriterStream("isoA");
    const writerB = await harness.createWriterStream("isoB");

    seedBlobs(harness, writerA.streamKey, "isoA", 10, "OnlyA");

    // Sync A
    const readerA = await harness.createReaderStream("isoA");
    await harness.syncStreamFully(readerA, 10);

    // B should have zero blocks
    const countB = await readerB_query();

    async function readerB_query() {
      const readerB = await harness!.createReaderStream("isoB");
      return readerB.query(
        `SELECT COUNT(*)::int as count FROM "isoB".block`,
      );
    }

    expect(countB.rows[0].count).toBe(0);

    // A should have its data
    const countA = await readerA.query(
      `SELECT COUNT(*)::int as count FROM "isoA".block`,
    );
    expect(countA.rows[0].count).toBe(10);
  });

  it("each stream has independent sync state", async () => {
    harness = await createHarness("medium");
    const writerA = await harness.createWriterStream("ssA");
    const writerB = await harness.createWriterStream("ssB");

    seedBlobs(harness, writerA.streamKey, "ssA", 10, "SA");
    seedBlobs(harness, writerB.streamKey, "ssB", 5, "SB");

    // Sync A partially (5 of 10)
    const readerA = await harness.createReaderStream("ssA");
    await harness.syncStream(readerA, 5);
    expect(readerA.syncIndex).toBe(5);

    // Sync B fully
    const readerB = await harness.createReaderStream("ssB");
    await harness.syncStreamFully(readerB, 10);
    expect(readerB.syncIndex).toBe(5);

    // Recreate readers to check persisted state
    const readerA2 = await harness.createReaderStream("ssA");
    const readerB2 = await harness.createReaderStream("ssB");

    // A should resume from 5, B from 5
    expect(readerA2.syncIndex).toBe(5);
    expect(readerB2.syncIndex).toBe(5);

    // Finish syncing A
    await harness.syncStreamFully(readerA2, 10);

    const countA = await readerA2.query(
      `SELECT COUNT(*)::int as count FROM "ssA".block`,
    );
    expect(countA.rows[0].count).toBe(10);
  });
});

// ---------------------------------------------------------------------------
// Multi-stream under cache pressure
// ---------------------------------------------------------------------------

describe("Multi-stream under cache pressure", () => {
  const PRESSURE_CONFIGS: CacheSize[] = ["tiny", "small", "medium", "large"];

  for (const size of PRESSURE_CONFIGS) {
    const pages = CACHE_CONFIGS[size];

    it(`two streams interleaved — cache=${size} (${pages} pages)`, async () => {
      harness = await createHarness(size);
      const writerA = await harness.createWriterStream(`cp_a_${size}`);
      const writerB = await harness.createWriterStream(`cp_b_${size}`);

      // Use larger content under pressure to force more eviction
      const contentSize = size === "tiny" ? 500 : 200;
      seedBlobs(harness, writerA.streamKey, `cp_a_${size}`, 20, "CpA", contentSize);
      seedBlobs(harness, writerB.streamKey, `cp_b_${size}`, 20, "CpB", contentSize);

      const readerA = await harness.createReaderStream(`cp_a_${size}`);
      const readerB = await harness.createReaderStream(`cp_b_${size}`);

      // Interleaved sync
      let doneA = false;
      let doneB = false;

      while (!doneA || !doneB) {
        if (!doneA) {
          const s = await harness.syncStream(readerA, 5);
          doneA = s.complete;
        }
        if (!doneB) {
          const s = await harness.syncStream(readerB, 5);
          doneB = s.complete;
        }
      }

      const countA = await readerA.query(
        `SELECT COUNT(*)::int as count FROM "cp_a_${size}".block`,
      );
      const countB = await readerB.query(
        `SELECT COUNT(*)::int as count FROM "cp_b_${size}".block`,
      );

      expect(countA.rows[0].count).toBe(20);
      expect(countB.rows[0].count).toBe(20);

      // Verify content survived eviction
      const sampleA = await readerA.query(
        `SELECT content FROM "cp_a_${size}".block WHERE id = 'block-cpa-0019'`,
      );
      expect(sampleA.rows[0].content).toMatch(/^CpA 19:/);

      const sampleB = await readerB.query(
        `SELECT content FROM "cp_b_${size}".block WHERE id = 'block-cpb-0019'`,
      );
      expect(sampleB.rows[0].content).toMatch(/^CpB 19:/);
    });
  }

  it("tiny cache + two streams + large content — heavy eviction @fast", async () => {
    harness = await createHarness("tiny");
    const writerA = await harness.createWriterStream("hvy_a");
    const writerB = await harness.createWriterStream("hvy_b");

    // Large content to maximize cache pressure
    seedBlobs(harness, writerA.streamKey, "hvy_a", 30, "HeavyA", 800);
    seedBlobs(harness, writerB.streamKey, "hvy_b", 30, "HeavyB", 800);

    const readerA = await harness.createReaderStream("hvy_a");
    const readerB = await harness.createReaderStream("hvy_b");

    // Interleave with small pages to maximize sync iterations + eviction
    let doneA = false;
    let doneB = false;

    while (!doneA || !doneB) {
      if (!doneA) {
        const s = await harness.syncStream(readerA, 3);
        doneA = s.complete;
      }
      if (!doneB) {
        const s = await harness.syncStream(readerB, 3);
        doneB = s.complete;
      }
    }

    // All data must survive heavy eviction
    const countA = await readerA.query(
      `SELECT COUNT(*)::int as count FROM "hvy_a".block`,
    );
    const countB = await readerB.query(
      `SELECT COUNT(*)::int as count FROM "hvy_b".block`,
    );

    expect(countA.rows[0].count).toBe(30);
    expect(countB.rows[0].count).toBe(30);

    // Verify content integrity at boundaries
    const firstA = await readerA.query(
      `SELECT LENGTH(content) as len FROM "hvy_a".block WHERE id = 'block-heavya-0000'`,
    );
    expect(firstA.rows[0].len).toBe("HeavyA 0: ".length + 800);

    const lastB = await readerB.query(
      `SELECT LENGTH(content) as len FROM "hvy_b".block WHERE id = 'block-heavyb-0029'`,
    );
    expect(lastB.rows[0].len).toBe("HeavyB 29: ".length + 800);
  });
});
