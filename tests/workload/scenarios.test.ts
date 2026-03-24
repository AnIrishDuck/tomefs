/**
 * Workload scenario tests for tomefs.
 *
 * These verify that tomefs works end-to-end under realistic usage patterns,
 * not just individual POSIX operations. Each scenario runs at multiple cache
 * sizes to force different eviction paths.
 *
 * See plans/workload-scenario-plan.md for the full design.
 */

import {
  createTomeFSHarness,
  createMemFSHarness,
  encode,
  decode,
  generatePageData,
  writeFileData,
  verifyFileData,
  O,
  SEEK_SET,
  SEEK_END,
  PAGE_SIZE,
  CACHE_CONFIGS,
  type CacheSize,
  type WorkloadHarness,
  type EmscriptenFS,
} from "./harness.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Fast byte-array comparison that avoids 8192 individual expect() calls per page.
 * On mismatch, reports the first differing byte for debugging.
 */
function expectBytesEqual(actual: Uint8Array, expected: Uint8Array, length?: number): void {
  const len = length ?? expected.length;
  for (let i = 0; i < len; i++) {
    if (actual[i] !== expected[i]) {
      throw new Error(
        `Byte mismatch at offset ${i}: expected ${expected[i]}, got ${actual[i]}`,
      );
    }
  }
}

/** Cache sizes to test. Tiny/small force eviction; large is the baseline. */
const PRESSURE_CONFIGS: CacheSize[] = ["tiny", "small", "medium", "large"];

/**
 * Run a scenario against tomefs at every cache pressure level.
 * Also runs against MEMFS once as a differential reference.
 */
function describeScenario(
  name: string,
  scenarioFn: (h: WorkloadHarness) => void | Promise<void>,
) {
  describe(name, () => {
    for (const size of PRESSURE_CONFIGS) {
      const pages = CACHE_CONFIGS[size];
      it(`tomefs cache=${size} (${pages} pages)`, async () => {
        const h = await createTomeFSHarness(size);
        await scenarioFn(h);
      });
    }

    it("memfs (reference)", async () => {
      const h = await createMemFSHarness();
      await scenarioFn(h);
    });
  });
}

// ---------------------------------------------------------------------------
// Scenario 1: Sequential Table Scan
// ---------------------------------------------------------------------------

describeScenario("Scenario 1: Sequential Table Scan @fast", (h) => {
  const { FS } = h;
  // 256 KB = 32 pages
  const totalBytes = PAGE_SIZE * 32;

  writeFileData(FS, "/table", totalBytes);
  verifyFileData(FS, "/table", totalBytes);
});

// ---------------------------------------------------------------------------
// Scenario 2: Write-Heavy Append (WAL Simulation)
// ---------------------------------------------------------------------------

describeScenario("Scenario 2: Write-Heavy Append (WAL)", (h) => {
  const { FS } = h;
  const stream = FS.open("/wal", O.WRONLY | O.CREAT | O.APPEND, 0o666);

  // Write 200 small records of varying sizes (64-512 bytes)
  const records: Uint8Array[] = [];
  for (let i = 0; i < 200; i++) {
    const size = 64 + (i * 7 % 449); // 64..512 deterministic
    const data = new Uint8Array(size);
    for (let j = 0; j < size; j++) {
      data[j] = ((i * 37 + j * 13) & 0xff);
    }
    records.push(data);
    FS.write(stream, data, 0, size);
  }
  FS.close(stream);

  // Read back and verify entire file
  const readStream = FS.open("/wal", O.RDONLY);
  for (const record of records) {
    const buf = new Uint8Array(record.length);
    const n = FS.read(readStream, buf, 0, record.length);
    expect(n).toBe(record.length);
    expect(Array.from(buf)).toEqual(Array.from(record));
  }
  FS.close(readStream);
});

// ---------------------------------------------------------------------------
// Scenario 3: Mixed Read/Write on Multiple Files
// ---------------------------------------------------------------------------

describeScenario("Scenario 3: Mixed Read/Write Multiple Files", (h) => {
  const { FS } = h;
  const fileCount = 8;
  const pagesPerFile = 8; // 64 KB each
  const totalPerFile = PAGE_SIZE * pagesPerFile;

  // Create all files with known data
  for (let f = 0; f < fileCount; f++) {
    writeFileData(FS, `/file${f}`, totalPerFile);
  }

  // Interleaved reads and writes across files
  for (let round = 0; round < 20; round++) {
    const fRead = round % fileCount;
    const fWrite = (round * 3 + 1) % fileCount;

    // Read a random-ish page from one file
    const readPageIdx = (round * 7) % pagesPerFile;
    const rStream = FS.open(`/file${fRead}`, O.RDONLY);
    FS.llseek(rStream, readPageIdx * PAGE_SIZE, SEEK_SET);
    const buf = new Uint8Array(PAGE_SIZE);
    const n = FS.read(rStream, buf, 0, PAGE_SIZE);
    expect(n).toBe(PAGE_SIZE);
    const expected = generatePageData(readPageIdx);
    expectBytesEqual(buf, expected);
    FS.close(rStream);

    // Append some data to another file
    const wStream = FS.open(`/file${fWrite}`, O.WRONLY | O.APPEND);
    const extra = encode(`round-${round}`);
    FS.write(wStream, extra, 0, extra.length);
    FS.close(wStream);
  }

  // Verify original data is still intact for all files
  for (let f = 0; f < fileCount; f++) {
    const stream = FS.open(`/file${f}`, O.RDONLY);
    const buf = new Uint8Array(PAGE_SIZE);
    for (let p = 0; p < pagesPerFile; p++) {
      const n = FS.read(stream, buf, 0, PAGE_SIZE);
      expect(n).toBe(PAGE_SIZE);
      const exp = generatePageData(p);
      expectBytesEqual(buf, exp);
    }
    FS.close(stream);
  }
});

// ---------------------------------------------------------------------------
// Scenario 4: Truncate Under Load
// ---------------------------------------------------------------------------

describeScenario("Scenario 4: Truncate Under Load @fast", (h) => {
  const { FS } = h;
  const totalPages = 16;
  const totalBytes = PAGE_SIZE * totalPages;

  // Write a large file
  writeFileData(FS, "/data", totalBytes);

  // Verify it's there
  verifyFileData(FS, "/data", totalBytes);

  // Truncate to half
  const halfBytes = PAGE_SIZE * 8;
  FS.truncate("/data", halfBytes);
  expect(FS.stat("/data").size).toBe(halfBytes);

  // Verify first half is still intact
  verifyFileData(FS, "/data", halfBytes);

  // Write new data at the end (append)
  const appendStream = FS.open("/data", O.WRONLY | O.APPEND);
  const newData = generatePageData(999, PAGE_SIZE * 4);
  FS.write(appendStream, newData, 0, newData.length);
  FS.close(appendStream);

  // Read back and verify: first half is original, latter is new data
  const readStream = FS.open("/data", O.RDONLY);
  const buf = new Uint8Array(PAGE_SIZE);
  for (let p = 0; p < 8; p++) {
    const n = FS.read(readStream, buf, 0, PAGE_SIZE);
    expect(n).toBe(PAGE_SIZE);
    const exp = generatePageData(p);
    expectBytesEqual(buf, exp);
  }
  // Read the appended data
  const appendBuf = new Uint8Array(PAGE_SIZE * 4);
  const n = FS.read(readStream, appendBuf, 0, PAGE_SIZE * 4);
  expect(n).toBe(PAGE_SIZE * 4);
  expectBytesEqual(appendBuf, newData, PAGE_SIZE * 4);
  FS.close(readStream);

  // Truncate to zero and rebuild
  FS.truncate("/data", 0);
  expect(FS.stat("/data").size).toBe(0);
  writeFileData(FS, "/data", PAGE_SIZE * 4);
  verifyFileData(FS, "/data", PAGE_SIZE * 4);
});

// ---------------------------------------------------------------------------
// Scenario 5: Create/Delete Churn (Temp Files)
// ---------------------------------------------------------------------------

describeScenario("Scenario 5: Create/Delete Churn @fast", (h) => {
  const { FS } = h;

  for (let i = 0; i < 50; i++) {
    const path = `/tmp_${i}`;
    const size = PAGE_SIZE * (1 + (i % 4)); // 1-4 pages

    // Create and write
    writeFileData(FS, path, size);

    // Read back and verify
    verifyFileData(FS, path, size);

    // Delete
    FS.unlink(path);

    // Verify it's gone
    let threw = false;
    try {
      FS.stat(path);
    } catch {
      threw = true;
    }
    expect(threw).toBe(true);
  }

  // Create a final file to ensure the FS is still healthy
  writeFileData(FS, "/survivor", PAGE_SIZE * 2);
  verifyFileData(FS, "/survivor", PAGE_SIZE * 2);
});

// ---------------------------------------------------------------------------
// Scenario 6: Directory Operations Under File Churn
// ---------------------------------------------------------------------------

describeScenario("Scenario 6: Directory Ops Under Churn", (h) => {
  const { FS } = h;

  // Create a directory tree mimicking Postgres data directory
  FS.mkdir("/base");
  FS.mkdir("/base/16384");
  FS.mkdir("/base/16385");

  // Create files inside directories
  for (let i = 0; i < 5; i++) {
    writeFileData(FS, `/base/16384/rel_${i}`, PAGE_SIZE * 2);
    writeFileData(FS, `/base/16385/rel_${i}`, PAGE_SIZE);
  }

  // Verify readdir
  const entries1 = FS.readdir("/base/16384").filter(
    (e: string) => e !== "." && e !== "..",
  );
  expect(entries1.sort()).toEqual(
    ["rel_0", "rel_1", "rel_2", "rel_3", "rel_4"],
  );

  // Rename files between directories
  FS.rename("/base/16384/rel_2", "/base/16385/moved_2");
  verifyFileData(FS, "/base/16385/moved_2", PAGE_SIZE * 2);

  // Delete some files
  FS.unlink("/base/16384/rel_0");
  FS.unlink("/base/16385/rel_0");

  // Verify readdir after mutations
  const entries2 = FS.readdir("/base/16384").filter(
    (e: string) => e !== "." && e !== "..",
  );
  expect(entries2.sort()).toEqual(["rel_1", "rel_3", "rel_4"]);

  const entries3 = FS.readdir("/base/16385").filter(
    (e: string) => e !== "." && e !== "..",
  );
  expect(entries3.sort()).toEqual(
    ["moved_2", "rel_1", "rel_2", "rel_3", "rel_4"],
  );

  // Verify remaining files are still intact
  verifyFileData(FS, "/base/16384/rel_1", PAGE_SIZE * 2);
  verifyFileData(FS, "/base/16385/moved_2", PAGE_SIZE * 2);
  verifyFileData(FS, "/base/16385/rel_1", PAGE_SIZE);

  // Delete all files and rmdir
  for (const e of FS.readdir("/base/16384").filter(
    (e: string) => e !== "." && e !== "..",
  )) {
    FS.unlink(`/base/16384/${e}`);
  }
  FS.rmdir("/base/16384");

  // Confirm directory is gone
  let threw = false;
  try {
    FS.stat("/base/16384");
  } catch {
    threw = true;
  }
  expect(threw).toBe(true);
});

// ---------------------------------------------------------------------------
// Scenario 7: Large Sequential Write Then Random Read
// ---------------------------------------------------------------------------

describeScenario("Scenario 7: Large Write Then Random Read", (h) => {
  const { FS } = h;
  const totalPages = 64; // 512 KB
  const totalBytes = PAGE_SIZE * totalPages;

  // Sequential write
  writeFileData(FS, "/big", totalBytes);

  // Random reads of individual pages
  const readOrder = [];
  for (let i = 0; i < 50; i++) {
    // Deterministic "random" page indices
    readOrder.push((i * 41 + 7) % totalPages);
  }

  const stream = FS.open("/big", O.RDONLY);
  const buf = new Uint8Array(PAGE_SIZE);

  for (const pageIdx of readOrder) {
    FS.llseek(stream, pageIdx * PAGE_SIZE, SEEK_SET);
    const n = FS.read(stream, buf, 0, PAGE_SIZE);
    expect(n).toBe(PAGE_SIZE);
    const expected = generatePageData(pageIdx);
    for (let i = 0; i < PAGE_SIZE; i++) {
      if (buf[i] !== expected[i]) {
        FS.close(stream);
        throw new Error(
          `Random read mismatch at page ${pageIdx} offset ${i}: ` +
          `expected ${expected[i]}, got ${buf[i]}`,
        );
      }
    }
  }
  FS.close(stream);

  // Re-read some pages to test cache hit path
  const stream2 = FS.open("/big", O.RDONLY);
  for (const pageIdx of readOrder.slice(0, 10)) {
    FS.llseek(stream2, pageIdx * PAGE_SIZE, SEEK_SET);
    const n = FS.read(stream2, buf, 0, PAGE_SIZE);
    expect(n).toBe(PAGE_SIZE);
    const expected = generatePageData(pageIdx);
    expectBytesEqual(buf, expected);
  }
  FS.close(stream2);
});

// ---------------------------------------------------------------------------
// Scenario 8: Rename Atomicity (Safe-Write Pattern)
// ---------------------------------------------------------------------------

describeScenario("Scenario 8: Rename Atomicity @fast", (h) => {
  const { FS } = h;

  // Create initial file
  writeFileData(FS, "/config", PAGE_SIZE * 2);
  verifyFileData(FS, "/config", PAGE_SIZE * 2);

  // Repeat the safe-write pattern multiple times
  for (let round = 0; round < 10; round++) {
    // Write new version to temp file
    const newSize = PAGE_SIZE * (2 + (round % 3)); // varying sizes
    const tmpStream = FS.open("/config.tmp", O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    for (let p = 0; p < Math.ceil(newSize / PAGE_SIZE); p++) {
      const chunkSize = Math.min(PAGE_SIZE, newSize - p * PAGE_SIZE);
      // Use round-specific data so each version is unique
      const data = new Uint8Array(chunkSize);
      for (let i = 0; i < chunkSize; i++) {
        data[i] = ((round * 53 + p * 251 + i * 31 + 17) & 0xff);
      }
      FS.write(tmpStream, data, 0, chunkSize);
    }
    FS.close(tmpStream);

    // Rename over original
    FS.rename("/config.tmp", "/config");

    // Verify the new content is served
    expect(FS.stat("/config").size).toBe(newSize);
    const readStream = FS.open("/config", O.RDONLY);
    const buf = new Uint8Array(PAGE_SIZE);
    for (let p = 0; p < Math.ceil(newSize / PAGE_SIZE); p++) {
      const chunkSize = Math.min(PAGE_SIZE, newSize - p * PAGE_SIZE);
      const n = FS.read(readStream, buf, 0, chunkSize);
      expect(n).toBe(chunkSize);
      const expectedPage = new Uint8Array(chunkSize);
      for (let i = 0; i < chunkSize; i++) {
        expectedPage[i] = ((round * 53 + p * 251 + i * 31 + 17) & 0xff);
      }
      expectBytesEqual(buf, expectedPage, chunkSize);
    }
    FS.close(readStream);

    // Temp file should no longer exist
    let threw = false;
    try {
      FS.stat("/config.tmp");
    } catch {
      threw = true;
    }
    expect(threw).toBe(true);
  }
});
