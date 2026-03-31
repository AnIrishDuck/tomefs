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

// ---------------------------------------------------------------------------
// Scenario 9: B-tree Index Traversal (Random Read-Modify-Write)
//
// Postgres B-tree index lookups follow a root→internal→leaf page path,
// reading 3-4 random pages per lookup, then potentially modifying a leaf
// page (INSERT/UPDATE). This creates scattered dirty pages across the
// cache — a fundamentally different access pattern than sequential scan
// or append. At small cache sizes, the random access pattern thrashes
// the LRU cache, forcing eviction of recently-dirtied pages.
// ---------------------------------------------------------------------------

describeScenario("Scenario 9: B-tree Index Traversal", (h) => {
  const { FS } = h;
  const indexPages = 32; // 256 KB index file
  const totalBytes = PAGE_SIZE * indexPages;

  // Create an index file with known data
  writeFileData(FS, "/idx", totalBytes);

  // Simulate 100 index lookups, each reading a "root→internal→leaf" path
  // and then modifying the leaf page (simulating an INSERT updating the index)
  const stream = FS.open("/idx", O.RDWR);
  const buf = new Uint8Array(PAGE_SIZE);

  // Track modifications: pageIndex → last modification seed
  const modifications = new Map<number, number>();

  for (let lookup = 0; lookup < 100; lookup++) {
    // Deterministic "random" page path: root(0) → internal → leaf
    const root = 0;
    const internal = 1 + ((lookup * 7 + 3) % 4); // pages 1-4 (internal nodes)
    const leaf = 5 + ((lookup * 13 + 11) % (indexPages - 5)); // pages 5-31

    // Read root page
    FS.llseek(stream, root * PAGE_SIZE, SEEK_SET);
    FS.read(stream, buf, 0, PAGE_SIZE);

    // Read internal page
    FS.llseek(stream, internal * PAGE_SIZE, SEEK_SET);
    FS.read(stream, buf, 0, PAGE_SIZE);

    // Read leaf page
    FS.llseek(stream, leaf * PAGE_SIZE, SEEK_SET);
    const n = FS.read(stream, buf, 0, PAGE_SIZE);
    expect(n).toBe(PAGE_SIZE);

    // Verify leaf page has expected content (original or last modification)
    const expectedSeed = modifications.get(leaf);
    if (expectedSeed !== undefined) {
      // Was modified — check modification pattern
      for (let i = 0; i < 64; i++) {
        if (buf[i] !== ((expectedSeed + i * 37) & 0xff)) {
          throw new Error(
            `Leaf page ${leaf} modified data mismatch at byte ${i} ` +
            `(lookup ${lookup}): expected ${(expectedSeed + i * 37) & 0xff}, got ${buf[i]}`,
          );
        }
      }
    } else {
      // Unmodified — check original pattern
      const expected = generatePageData(leaf);
      expectBytesEqual(buf, expected);
    }

    // Modify the leaf page header (first 64 bytes) — simulates index tuple insert
    const modSeed = lookup * 1000 + leaf;
    for (let i = 0; i < 64; i++) {
      buf[i] = (modSeed + i * 37) & 0xff;
    }
    FS.llseek(stream, leaf * PAGE_SIZE, SEEK_SET);
    FS.write(stream, buf, 0, PAGE_SIZE);
    modifications.set(leaf, modSeed);
  }
  FS.close(stream);

  // Verify all pages: unmodified pages should have original data,
  // modified pages should have the last modification applied
  const verifyStream = FS.open("/idx", O.RDONLY);
  for (let p = 0; p < indexPages; p++) {
    FS.llseek(verifyStream, p * PAGE_SIZE, SEEK_SET);
    const n = FS.read(verifyStream, buf, 0, PAGE_SIZE);
    expect(n).toBe(PAGE_SIZE);

    const modSeed = modifications.get(p);
    if (modSeed !== undefined) {
      // First 64 bytes modified, rest is original
      for (let i = 0; i < 64; i++) {
        if (buf[i] !== ((modSeed + i * 37) & 0xff)) {
          throw new Error(
            `Final verify page ${p} header mismatch at byte ${i}: ` +
            `expected ${(modSeed + i * 37) & 0xff}, got ${buf[i]}`,
          );
        }
      }
      const expected = generatePageData(p);
      expectBytesEqual(
        buf.subarray(64),
        expected.subarray(64),
        PAGE_SIZE - 64,
      );
    } else {
      const expected = generatePageData(p);
      expectBytesEqual(buf, expected);
    }
  }
  FS.close(verifyStream);
});

// ---------------------------------------------------------------------------
// Scenario 10: Concurrent Sequential Scan + Point Lookups
//
// Realistic OLTP+analytic mixed workload: one fd performs a full sequential
// scan (analytic query / VACUUM) while another fd does random point lookups
// (OLTP SELECTs by primary key). The sequential scan evicts pages that the
// point lookup needs, and vice versa. At small cache sizes this creates
// severe thrashing — the LRU cache bounces between scan and lookup pages.
// ---------------------------------------------------------------------------

describeScenario("Scenario 10: Sequential Scan + Point Lookups", (h) => {
  const { FS } = h;
  const heapPages = 48; // 384 KB heap
  const totalBytes = PAGE_SIZE * heapPages;

  // Create heap file
  writeFileData(FS, "/heap", totalBytes);

  // Open two fds: one for sequential scan, one for point lookups
  const scanFd = FS.open("/heap", O.RDONLY);
  const lookupFd = FS.open("/heap", O.RDONLY);

  const scanBuf = new Uint8Array(PAGE_SIZE);
  const lookupBuf = new Uint8Array(PAGE_SIZE);

  // Interleave: scan reads 2 pages, then a point lookup reads 1 random page
  let scanPos = 0;
  for (let round = 0; round < heapPages / 2; round++) {
    // Sequential scan: read 2 pages
    for (let i = 0; i < 2 && scanPos < heapPages; i++) {
      FS.llseek(scanFd, scanPos * PAGE_SIZE, SEEK_SET);
      const n = FS.read(scanFd, scanBuf, 0, PAGE_SIZE);
      expect(n).toBe(PAGE_SIZE);
      const expected = generatePageData(scanPos);
      expectBytesEqual(scanBuf, expected);
      scanPos++;
    }

    // Point lookup: read a random page (deterministic)
    const lookupPage = (round * 31 + 5) % heapPages;
    FS.llseek(lookupFd, lookupPage * PAGE_SIZE, SEEK_SET);
    const n = FS.read(lookupFd, lookupBuf, 0, PAGE_SIZE);
    expect(n).toBe(PAGE_SIZE);
    const expected = generatePageData(lookupPage);
    expectBytesEqual(lookupBuf, expected);
  }

  FS.close(scanFd);
  FS.close(lookupFd);
});

// ---------------------------------------------------------------------------
// Scenario 11: Multi-file Random Access (Heap + Index + TOAST)
//
// Postgres queries often access multiple files simultaneously: the heap
// for tuple data, one or more indexes for lookup, and TOAST tables for
// large values. Each file competes for the same cache slots. This tests
// interleaved random reads across 3 files with different sizes and access
// patterns, exercising inter-file eviction pressure.
// ---------------------------------------------------------------------------

describeScenario("Scenario 11: Multi-file Random Access", (h) => {
  const { FS } = h;
  const heapPages = 24;
  const idxPages = 16;
  const toastPages = 12;

  // Create three files with distinct data
  writeFileData(FS, "/heap", PAGE_SIZE * heapPages);
  writeFileData(FS, "/idx", PAGE_SIZE * idxPages);
  writeFileData(FS, "/toast", PAGE_SIZE * toastPages);

  const heapFd = FS.open("/heap", O.RDONLY);
  const idxFd = FS.open("/idx", O.RDONLY);
  const toastFd = FS.open("/toast", O.RDONLY);
  const buf = new Uint8Array(PAGE_SIZE);

  // Simulate 60 queries: each does index lookup → heap fetch → optional TOAST fetch
  for (let q = 0; q < 60; q++) {
    // Index lookup (random page in index)
    const idxPage = (q * 7 + 3) % idxPages;
    FS.llseek(idxFd, idxPage * PAGE_SIZE, SEEK_SET);
    let n = FS.read(idxFd, buf, 0, PAGE_SIZE);
    expect(n).toBe(PAGE_SIZE);
    expectBytesEqual(buf, generatePageData(idxPage));

    // Heap fetch (random page derived from "index lookup result")
    const heapPage = (idxPage * 3 + q * 11 + 1) % heapPages;
    FS.llseek(heapFd, heapPage * PAGE_SIZE, SEEK_SET);
    n = FS.read(heapFd, buf, 0, PAGE_SIZE);
    expect(n).toBe(PAGE_SIZE);
    expectBytesEqual(buf, generatePageData(heapPage));

    // TOAST fetch (every 3rd query, simulating large column access)
    if (q % 3 === 0) {
      const toastPage = (q * 5 + 2) % toastPages;
      FS.llseek(toastFd, toastPage * PAGE_SIZE, SEEK_SET);
      n = FS.read(toastFd, buf, 0, PAGE_SIZE);
      expect(n).toBe(PAGE_SIZE);
      expectBytesEqual(buf, generatePageData(toastPage));
    }
  }

  FS.close(heapFd);
  FS.close(idxFd);
  FS.close(toastFd);
});

// ---------------------------------------------------------------------------
// Scenario 12: VACUUM (Sequential Rewrite + Truncate)
//
// PostgreSQL's VACUUM reads every page of a table sequentially, rewrites
// pages in place to compact dead tuples, then truncates trailing empty
// pages. Under cache pressure this is demanding:
//   - The sequential scan rotates the entire cache
//   - In-place writes dirty pages that may be evicted before the scan
//     reaches the next page (dirty eviction under forward pressure)
//   - Truncation removes pages that may be cached, dirty, or already
//     evicted — all three states must be handled correctly
//   - After truncation, remaining data must be intact
// ---------------------------------------------------------------------------

describeScenario("Scenario 12: VACUUM (rewrite + truncate) @fast", (h) => {
  const { FS } = h;
  const totalPages = 32;
  const totalBytes = PAGE_SIZE * totalPages;

  // Phase 1: Populate the "table" with deterministic data
  writeFileData(FS, "/table", totalBytes);

  // Phase 2: VACUUM scan — read each page, "compact" it by rewriting
  // with modified data (simulating dead tuple removal / tuple compaction).
  // The rewritten pages get a different byte pattern so we can verify them.
  const vacuumFd = FS.open("/table", O.RDWR);
  const readBuf = new Uint8Array(PAGE_SIZE);

  // Track which pages become "empty" (all dead tuples) — these will be
  // truncated away. Last 25% of pages are "empty" after vacuum.
  const survivingPages = Math.floor(totalPages * 0.75); // 24 pages survive

  for (let p = 0; p < totalPages; p++) {
    // Read the page (sequential scan)
    FS.llseek(vacuumFd, p * PAGE_SIZE, SEEK_SET);
    const n = FS.read(vacuumFd, readBuf, 0, PAGE_SIZE);
    expect(n).toBe(PAGE_SIZE);

    // Verify we read the original data
    const expected = generatePageData(p);
    expectBytesEqual(readBuf, expected);

    if (p < survivingPages) {
      // "Compact" this page: rewrite with modified data.
      // XOR each byte with the page index to create a distinguishable pattern.
      const compacted = new Uint8Array(PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) {
        compacted[i] = readBuf[i] ^ (p & 0xff);
      }
      FS.llseek(vacuumFd, p * PAGE_SIZE, SEEK_SET);
      FS.write(vacuumFd, compacted, 0, PAGE_SIZE);
    }
    // Pages >= survivingPages are "empty" — will be truncated
  }
  FS.close(vacuumFd);

  // Phase 3: Truncate trailing empty pages (VACUUM's final step)
  FS.truncate("/table", survivingPages * PAGE_SIZE);

  // Phase 4: Verify the file is the correct size
  const stat = FS.stat("/table");
  expect(stat.size).toBe(survivingPages * PAGE_SIZE);

  // Phase 5: Verify all surviving pages have the compacted data
  const verifyFd = FS.open("/table", O.RDONLY);
  const verifyBuf = new Uint8Array(PAGE_SIZE);
  for (let p = 0; p < survivingPages; p++) {
    const n = FS.read(verifyFd, verifyBuf, 0, PAGE_SIZE);
    expect(n).toBe(PAGE_SIZE);
    const original = generatePageData(p);
    for (let i = 0; i < PAGE_SIZE; i++) {
      const expectedByte = original[i] ^ (p & 0xff);
      if (verifyBuf[i] !== expectedByte) {
        FS.close(verifyFd);
        throw new Error(
          `VACUUM compacted data mismatch at page ${p}, offset ${i}: ` +
          `expected ${expectedByte}, got ${verifyBuf[i]}`,
        );
      }
    }
  }
  FS.close(verifyFd);
});

// ---------------------------------------------------------------------------
// Scenario 13: VACUUM + Concurrent Reads
//
// While VACUUM rewrites and truncates, other queries continue reading the
// same table through a separate fd. This simulates the real PGlite pattern
// where SELECTs run concurrently with VACUUM. The reader fd holds pages
// in cache that VACUUM wants to rewrite, creating cross-fd cache contention.
// ---------------------------------------------------------------------------

describeScenario("Scenario 13: VACUUM + concurrent reads", (h) => {
  const { FS } = h;
  const totalPages = 24;
  const totalBytes = PAGE_SIZE * totalPages;
  const survivingPages = 18; // truncate last 6 pages

  // Populate
  writeFileData(FS, "/table", totalBytes);

  // Open two fds: VACUUM (read-write) and reader (read-only)
  const vacuumFd = FS.open("/table", O.RDWR);
  const readerFd = FS.open("/table", O.RDONLY);
  const buf = new Uint8Array(PAGE_SIZE);

  // Interleaved: VACUUM processes 2 pages, then reader reads 1 random page
  for (let step = 0; step < totalPages; step += 2) {
    // VACUUM: read + rewrite 2 pages
    for (let i = 0; i < 2 && step + i < totalPages; i++) {
      const p = step + i;
      FS.llseek(vacuumFd, p * PAGE_SIZE, SEEK_SET);
      const n = FS.read(vacuumFd, buf, 0, PAGE_SIZE);
      expect(n).toBe(PAGE_SIZE);

      if (p < survivingPages) {
        // Compact: invert all bytes
        const compacted = new Uint8Array(PAGE_SIZE);
        for (let j = 0; j < PAGE_SIZE; j++) {
          compacted[j] = buf[j] ^ 0xff;
        }
        FS.llseek(vacuumFd, p * PAGE_SIZE, SEEK_SET);
        FS.write(vacuumFd, compacted, 0, PAGE_SIZE);
      }
    }

    // Reader: read a "random" page from already-vacuumed region
    if (step > 0) {
      const readPage = (step * 7 + 3) % step; // page in already-processed region
      FS.llseek(readerFd, readPage * PAGE_SIZE, SEEK_SET);
      const n = FS.read(readerFd, buf, 0, PAGE_SIZE);
      expect(n).toBe(PAGE_SIZE);
      // Verify we see the compacted (post-VACUUM) data
      const original = generatePageData(readPage);
      for (let j = 0; j < PAGE_SIZE; j++) {
        const expectedByte = original[j] ^ 0xff;
        if (buf[j] !== expectedByte) {
          FS.close(vacuumFd);
          FS.close(readerFd);
          throw new Error(
            `Concurrent read mismatch at page ${readPage}, offset ${j}: ` +
            `expected ${expectedByte}, got ${buf[j]}`,
          );
        }
      }
    }
  }

  FS.close(vacuumFd);

  // Truncate trailing empty pages
  FS.truncate("/table", survivingPages * PAGE_SIZE);
  expect(FS.stat("/table").size).toBe(survivingPages * PAGE_SIZE);

  // Reader fd should still work for surviving pages
  FS.llseek(readerFd, 0, SEEK_SET);
  for (let p = 0; p < survivingPages; p++) {
    const n = FS.read(readerFd, buf, 0, PAGE_SIZE);
    expect(n).toBe(PAGE_SIZE);
    const original = generatePageData(p);
    for (let j = 0; j < PAGE_SIZE; j++) {
      const expectedByte = original[j] ^ 0xff;
      if (buf[j] !== expectedByte) {
        FS.close(readerFd);
        throw new Error(
          `Post-truncate read mismatch at page ${p}, offset ${j}: ` +
          `expected ${expectedByte}, got ${buf[j]}`,
        );
      }
    }
  }
  FS.close(readerFd);
});
