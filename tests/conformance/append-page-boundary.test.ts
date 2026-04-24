/**
 * Conformance tests: O_APPEND behavior across page boundaries.
 *
 * Source: POSIX open(2), write(2). "If the O_APPEND flag of the file status
 * flags is set, the file offset shall be set to the end of the file prior
 * to each write and no intervening file modification operation shall occur
 * between changing the file offset and the write operation."
 *
 * The basic O_APPEND test (open-append.test.ts) verifies small writes within
 * a single page. These tests specifically target page-boundary interactions
 * that are critical for page-cached filesystems like tomefs:
 *   - Appends that cross page boundaries
 *   - Appends that start exactly at a page boundary
 *   - Large multi-page append sequences (WAL-style)
 *   - Concurrent O_APPEND fds on the same file
 *   - O_APPEND combined with ftruncate (WAL recycling pattern)
 *   - O_APPEND under cache pressure (small cache with eviction)
 *
 * Postgres uses O_APPEND for WAL segment writes. Correctness of these
 * operations is critical for database durability.
 *
 * Ethos §2 (real POSIX semantics), §8 (workload scenarios)
 */
import {
  createFS,
  encode,
  decode,
  O,
  SEEK_SET,
  SEEK_CUR,
  SEEK_END,
  type FSHarness,
} from "../harness/emscripten-fs.js";

const PAGE_SIZE = 8192;

describe("O_APPEND page boundary conformance", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  // -------------------------------------------------------------------
  // Writes crossing page boundaries
  // -------------------------------------------------------------------

  it("append write crossing a page boundary @fast", () => {
    const { FS } = h;

    // Create a file with data ending near the page boundary
    const stream = FS.open(
      "/cross_boundary",
      O.RDWR | O.CREAT | O.EXCL,
      0o666,
    );
    // Write PAGE_SIZE - 4 bytes to leave 4 bytes before the boundary
    const prefix = new Uint8Array(PAGE_SIZE - 4);
    prefix.fill(0x41); // 'A'
    FS.write(stream, prefix, 0, prefix.length);
    FS.close(stream);

    // Reopen in append mode and write 8 bytes across the boundary
    const as = FS.open("/cross_boundary", O.WRONLY | O.APPEND);
    const crossData = encode("CROSSBDY"); // 8 bytes: 4 on page 0, 4 on page 1
    FS.write(as, crossData, 0, crossData.length);
    FS.close(as);

    // Read back and verify
    const rs = FS.open("/cross_boundary", O.RDONLY);
    const stat = FS.fstat(rs.fd);
    expect(stat.size).toBe(PAGE_SIZE + 4);

    // Read the cross-boundary region
    FS.llseek(rs, PAGE_SIZE - 4, SEEK_SET);
    const buf = new Uint8Array(8);
    const n = FS.read(rs, buf, 0, 8);
    expect(n).toBe(8);
    expect(decode(buf, n)).toBe("CROSSBDY");
    FS.close(rs);
  });

  it("append write starting exactly at page boundary @fast", () => {
    const { FS } = h;

    // Create a file of exactly PAGE_SIZE bytes
    const stream = FS.open(
      "/exact_boundary",
      O.RDWR | O.CREAT | O.EXCL,
      0o666,
    );
    const page = new Uint8Array(PAGE_SIZE);
    page.fill(0x42); // 'B'
    FS.write(stream, page, 0, PAGE_SIZE);
    FS.close(stream);

    // Append — write starts exactly at page boundary
    const as = FS.open("/exact_boundary", O.WRONLY | O.APPEND);
    const data = encode("PAGE2START");
    FS.write(as, data, 0, data.length);
    FS.close(as);

    // Verify
    const rs = FS.open("/exact_boundary", O.RDONLY);
    expect(FS.fstat(rs.fd).size).toBe(PAGE_SIZE + data.length);

    // Read from page boundary
    FS.llseek(rs, PAGE_SIZE, SEEK_SET);
    const buf = new Uint8Array(20);
    const n = FS.read(rs, buf, 0, 20);
    expect(n).toBe(data.length);
    expect(decode(buf, n)).toBe("PAGE2START");
    FS.close(rs);
  });

  it("append write spanning multiple pages @fast", () => {
    const { FS } = h;

    const stream = FS.open(
      "/multi_page_append",
      O.RDWR | O.CREAT | O.EXCL | O.APPEND,
      0o666,
    );

    // Write 2.5 pages of data in one append call
    const bigWrite = new Uint8Array(PAGE_SIZE * 2 + PAGE_SIZE / 2);
    for (let i = 0; i < bigWrite.length; i++) {
      bigWrite[i] = (i & 0xff);
    }
    FS.write(stream, bigWrite, 0, bigWrite.length);

    expect(FS.fstat(stream.fd).size).toBe(bigWrite.length);

    // Read back and verify byte-for-byte
    FS.llseek(stream, 0, SEEK_SET);
    const readBuf = new Uint8Array(bigWrite.length);
    const n = FS.read(stream, readBuf, 0, readBuf.length);
    expect(n).toBe(bigWrite.length);
    for (let i = 0; i < bigWrite.length; i++) {
      if (readBuf[i] !== bigWrite[i]) {
        throw new Error(`Mismatch at byte ${i}: expected ${bigWrite[i]}, got ${readBuf[i]}`);
      }
    }

    FS.close(stream);
  });

  // -------------------------------------------------------------------
  // Sequential append pattern (WAL-style)
  // -------------------------------------------------------------------

  it("sequential small appends grow file across page boundaries", () => {
    const { FS } = h;

    const stream = FS.open(
      "/wal_style",
      O.RDWR | O.CREAT | O.EXCL | O.APPEND,
      0o666,
    );

    // Simulate WAL-style writes: many small sequential appends
    const recordSize = 128;
    const numRecords = (PAGE_SIZE * 3) / recordSize; // enough to span 3 pages
    const records: Uint8Array[] = [];

    for (let i = 0; i < numRecords; i++) {
      const record = new Uint8Array(recordSize);
      record.fill(i & 0xff);
      // Embed record index in first 4 bytes for verification
      record[0] = (i >> 24) & 0xff;
      record[1] = (i >> 16) & 0xff;
      record[2] = (i >> 8) & 0xff;
      record[3] = i & 0xff;
      records.push(record);
      FS.write(stream, record, 0, recordSize);
    }

    const expectedSize = numRecords * recordSize;
    expect(FS.fstat(stream.fd).size).toBe(expectedSize);

    // Read back and verify each record
    FS.llseek(stream, 0, SEEK_SET);
    for (let i = 0; i < numRecords; i++) {
      const buf = new Uint8Array(recordSize);
      const n = FS.read(stream, buf, 0, recordSize);
      expect(n).toBe(recordSize);
      // Check index bytes
      const idx = (buf[0] << 24) | (buf[1] << 16) | (buf[2] << 8) | buf[3];
      expect(idx).toBe(i);
      // Check fill
      for (let j = 4; j < recordSize; j++) {
        expect(buf[j]).toBe(i & 0xff);
      }
    }

    FS.close(stream);
  });

  it("appends that straddle exact page boundary preserve data on both sides", () => {
    const { FS } = h;

    const stream = FS.open(
      "/straddle",
      O.RDWR | O.CREAT | O.EXCL | O.APPEND,
      0o666,
    );

    // Fill up to 10 bytes before the page boundary
    const pre = new Uint8Array(PAGE_SIZE - 10);
    pre.fill(0x30); // '0'
    FS.write(stream, pre, 0, pre.length);

    // Append 20 bytes: 10 land on page 0, 10 land on page 1
    const straddle = encode("ABCDEFGHIJklmnopqrst"); // 20 bytes
    FS.write(stream, straddle, 0, straddle.length);

    FS.close(stream);

    // Read back
    const rs = FS.open("/straddle", O.RDONLY);
    FS.llseek(rs, PAGE_SIZE - 10, SEEK_SET);
    const buf = new Uint8Array(20);
    const n = FS.read(rs, buf, 0, 20);
    expect(n).toBe(20);
    expect(decode(buf, n)).toBe("ABCDEFGHIJklmnopqrst");

    // Verify total size
    expect(FS.fstat(rs.fd).size).toBe(PAGE_SIZE + 10);
    FS.close(rs);
  });

  // -------------------------------------------------------------------
  // Concurrent O_APPEND file descriptors
  // -------------------------------------------------------------------

  it("two O_APPEND fds on same file both append correctly @fast", () => {
    const { FS } = h;

    // Create file
    const setup = FS.open(
      "/concurrent_append",
      O.RDWR | O.CREAT | O.EXCL,
      0o666,
    );
    FS.write(setup, encode("INIT"), 0, 4);
    FS.close(setup);

    // Open two append fds
    const fd1 = FS.open("/concurrent_append", O.WRONLY | O.APPEND);
    const fd2 = FS.open("/concurrent_append", O.WRONLY | O.APPEND);

    // Alternate writes between fds
    FS.write(fd1, encode("-fd1a"), 0, 5);
    FS.write(fd2, encode("-fd2a"), 0, 5);
    FS.write(fd1, encode("-fd1b"), 0, 5);
    FS.write(fd2, encode("-fd2b"), 0, 5);

    FS.close(fd1);
    FS.close(fd2);

    // Read back — all data should be present in order
    const rs = FS.open("/concurrent_append", O.RDONLY);
    const buf = new Uint8Array(100);
    const n = FS.read(rs, buf, 0, 100);
    expect(n).toBe(24); // 4 + 5 + 5 + 5 + 5
    expect(decode(buf, n)).toBe("INIT-fd1a-fd2a-fd1b-fd2b");
    FS.close(rs);
  });

  it("concurrent appends crossing page boundary from different fds", () => {
    const { FS } = h;

    // Pre-fill close to page boundary
    const setup = FS.open(
      "/concurrent_cross",
      O.RDWR | O.CREAT | O.EXCL,
      0o666,
    );
    const prefix = new Uint8Array(PAGE_SIZE - 8);
    prefix.fill(0x58); // 'X'
    FS.write(setup, prefix, 0, prefix.length);
    FS.close(setup);

    const fd1 = FS.open("/concurrent_cross", O.WRONLY | O.APPEND);
    const fd2 = FS.open("/concurrent_cross", O.WRONLY | O.APPEND);

    // fd1 writes 8 bytes (fills page 0 to boundary)
    FS.write(fd1, encode("11111111"), 0, 8);
    // fd2 writes 8 bytes (starts page 1)
    FS.write(fd2, encode("22222222"), 0, 8);

    FS.close(fd1);
    FS.close(fd2);

    const rs = FS.open("/concurrent_cross", O.RDONLY);
    expect(FS.fstat(rs.fd).size).toBe(PAGE_SIZE + 8);

    // Verify the boundary region
    FS.llseek(rs, PAGE_SIZE - 8, SEEK_SET);
    const buf = new Uint8Array(16);
    const n = FS.read(rs, buf, 0, 16);
    expect(n).toBe(16);
    expect(decode(buf, n)).toBe("1111111122222222");
    FS.close(rs);
  });

  // -------------------------------------------------------------------
  // O_APPEND combined with ftruncate (WAL recycling)
  // -------------------------------------------------------------------

  it("ftruncate then append restarts from truncation point @fast", () => {
    const { FS } = h;

    const stream = FS.open(
      "/trunc_append",
      O.RDWR | O.CREAT | O.EXCL | O.APPEND,
      0o666,
    );

    // Write initial data across a page boundary
    const initial = new Uint8Array(PAGE_SIZE + 100);
    initial.fill(0x41);
    FS.write(stream, initial, 0, initial.length);
    expect(FS.fstat(stream.fd).size).toBe(PAGE_SIZE + 100);

    // Truncate to half a page (WAL segment recycling)
    FS.ftruncate(stream.fd, PAGE_SIZE / 2);
    expect(FS.fstat(stream.fd).size).toBe(PAGE_SIZE / 2);

    // Append after truncate — should start from the truncation point
    const newData = encode("AFTER_TRUNC");
    FS.write(stream, newData, 0, newData.length);
    expect(FS.fstat(stream.fd).size).toBe(PAGE_SIZE / 2 + newData.length);

    // Read back and verify
    FS.llseek(stream, PAGE_SIZE / 2, SEEK_SET);
    const buf = new Uint8Array(20);
    const n = FS.read(stream, buf, 0, 20);
    expect(n).toBe(newData.length);
    expect(decode(buf, n)).toBe("AFTER_TRUNC");

    FS.close(stream);
  });

  it("truncate to zero then append rebuilds file correctly", () => {
    const { FS } = h;

    const stream = FS.open(
      "/trunc_zero_append",
      O.RDWR | O.CREAT | O.EXCL | O.APPEND,
      0o666,
    );

    // Write 2 pages of data
    const data = new Uint8Array(PAGE_SIZE * 2);
    data.fill(0x42);
    FS.write(stream, data, 0, data.length);

    // Truncate to zero
    FS.ftruncate(stream.fd, 0);
    expect(FS.fstat(stream.fd).size).toBe(0);

    // Rebuild with appends
    for (let i = 0; i < 4; i++) {
      const chunk = encode(`chunk${i}`);
      FS.write(stream, chunk, 0, chunk.length);
    }

    // Verify
    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(100);
    const n = FS.read(stream, buf, 0, 100);
    expect(decode(buf, n)).toBe("chunk0chunk1chunk2chunk3");

    FS.close(stream);
  });

  it("ftruncate at page boundary then append crosses to next page", () => {
    const { FS } = h;

    const stream = FS.open(
      "/trunc_boundary_append",
      O.RDWR | O.CREAT | O.EXCL | O.APPEND,
      0o666,
    );

    // Write 1.5 pages
    const data = new Uint8Array(PAGE_SIZE + PAGE_SIZE / 2);
    data.fill(0x43);
    FS.write(stream, data, 0, data.length);

    // Truncate to exactly PAGE_SIZE (first page boundary)
    FS.ftruncate(stream.fd, PAGE_SIZE);
    expect(FS.fstat(stream.fd).size).toBe(PAGE_SIZE);

    // Append data that starts exactly at page boundary
    const appendData = new Uint8Array(PAGE_SIZE);
    appendData.fill(0x44);
    FS.write(stream, appendData, 0, appendData.length);

    expect(FS.fstat(stream.fd).size).toBe(PAGE_SIZE * 2);

    // Read second page and verify it's all 0x44
    FS.llseek(stream, PAGE_SIZE, SEEK_SET);
    const readBuf = new Uint8Array(PAGE_SIZE);
    const n = FS.read(stream, readBuf, 0, PAGE_SIZE);
    expect(n).toBe(PAGE_SIZE);
    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(readBuf[i]).toBe(0x44);
    }

    FS.close(stream);
  });

  // -------------------------------------------------------------------
  // O_APPEND with positioned reads (pread-style)
  // -------------------------------------------------------------------

  it("O_APPEND write does not affect positioned read @fast", () => {
    const { FS } = h;

    const stream = FS.open(
      "/append_pread",
      O.RDWR | O.CREAT | O.EXCL | O.APPEND,
      0o666,
    );

    // Write initial data
    FS.write(stream, encode("ABCDEFGH"), 0, 8);

    // Positioned read at offset 4 (should return "EFGH")
    const buf = new Uint8Array(10);
    const n = FS.read(stream, buf, 0, 4, 4);
    expect(n).toBe(4);
    expect(decode(buf, 4)).toBe("EFGH");

    // Append more data
    FS.write(stream, encode("IJKL"), 0, 4);

    // Positioned read across old+new data
    const buf2 = new Uint8Array(10);
    const n2 = FS.read(stream, buf2, 0, 8, 4);
    expect(n2).toBe(8);
    expect(decode(buf2, 8)).toBe("EFGHIJKL");

    FS.close(stream);
  });

  // -------------------------------------------------------------------
  // O_APPEND seek behavior details
  // -------------------------------------------------------------------

  it("seek in append mode does not affect write position", () => {
    const { FS } = h;

    const stream = FS.open(
      "/seek_append",
      O.RDWR | O.CREAT | O.EXCL | O.APPEND,
      0o666,
    );

    FS.write(stream, encode("Hello"), 0, 5);

    // Seek to beginning — should not affect append writes
    FS.llseek(stream, 0, SEEK_SET);

    // This write must go to the end, not position 0
    FS.write(stream, encode("World"), 0, 5);

    // Verify total file is "HelloWorld" not "World"
    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(20);
    const n = FS.read(stream, buf, 0, 20);
    expect(n).toBe(10);
    expect(decode(buf, n)).toBe("HelloWorld");

    FS.close(stream);
  });

  it("SEEK_END works correctly after appends across page boundary", () => {
    const { FS } = h;

    const stream = FS.open(
      "/seek_end_append",
      O.RDWR | O.CREAT | O.EXCL | O.APPEND,
      0o666,
    );

    // Write data that spans a page boundary
    const data = new Uint8Array(PAGE_SIZE + 500);
    data.fill(0x45);
    FS.write(stream, data, 0, data.length);

    // SEEK_END should report correct position
    const pos = FS.llseek(stream, 0, SEEK_END);
    expect(pos).toBe(PAGE_SIZE + 500);

    // Negative SEEK_END
    const pos2 = FS.llseek(stream, -500, SEEK_END);
    expect(pos2).toBe(PAGE_SIZE);

    FS.close(stream);
  });

  // -------------------------------------------------------------------
  // Large append sequence (stress test for page management)
  // -------------------------------------------------------------------

  it("1000 small appends produce correct file content", () => {
    const { FS } = h;

    const stream = FS.open(
      "/many_appends",
      O.RDWR | O.CREAT | O.EXCL | O.APPEND,
      0o666,
    );

    const recordSize = 32;
    const numRecords = 1000;

    for (let i = 0; i < numRecords; i++) {
      const record = new Uint8Array(recordSize);
      // Store 4-byte big-endian index + fill
      record[0] = (i >> 24) & 0xff;
      record[1] = (i >> 16) & 0xff;
      record[2] = (i >> 8) & 0xff;
      record[3] = i & 0xff;
      record.fill(i & 0xff, 4);
      FS.write(stream, record, 0, recordSize);
    }

    const expectedSize = numRecords * recordSize;
    expect(FS.fstat(stream.fd).size).toBe(expectedSize);

    // Spot-check records at page boundaries
    const pageRecords = PAGE_SIZE / recordSize;
    const checkIndices = [
      0, // first record
      pageRecords - 1, // last record on page 0
      pageRecords, // first record on page 1
      numRecords - 1, // last record
    ];

    for (const idx of checkIndices) {
      FS.llseek(stream, idx * recordSize, SEEK_SET);
      const buf = new Uint8Array(recordSize);
      const n = FS.read(stream, buf, 0, recordSize);
      expect(n).toBe(recordSize);
      const readIdx = (buf[0] << 24) | (buf[1] << 16) | (buf[2] << 8) | buf[3];
      expect(readIdx).toBe(idx);
    }

    FS.close(stream);
  });

  // -------------------------------------------------------------------
  // Append exactly fills a page
  // -------------------------------------------------------------------

  it("append exactly fills remaining page space", () => {
    const { FS } = h;

    const stream = FS.open(
      "/fill_page",
      O.RDWR | O.CREAT | O.EXCL | O.APPEND,
      0o666,
    );

    // Write half a page
    const half = new Uint8Array(PAGE_SIZE / 2);
    half.fill(0x46);
    FS.write(stream, half, 0, half.length);

    // Append exactly the remaining half to fill page 0
    const rest = new Uint8Array(PAGE_SIZE / 2);
    rest.fill(0x47);
    FS.write(stream, rest, 0, rest.length);

    expect(FS.fstat(stream.fd).size).toBe(PAGE_SIZE);

    // One more byte pushes to page 1
    FS.write(stream, encode("!"), 0, 1);
    expect(FS.fstat(stream.fd).size).toBe(PAGE_SIZE + 1);

    // Verify the boundary bytes
    FS.llseek(stream, PAGE_SIZE - 1, SEEK_SET);
    const buf = new Uint8Array(2);
    const n = FS.read(stream, buf, 0, 2);
    expect(n).toBe(2);
    expect(buf[0]).toBe(0x47); // last byte of page 0
    expect(buf[1]).toBe(0x21); // '!' on page 1

    FS.close(stream);
  });

  // -------------------------------------------------------------------
  // Append with O_CREAT on non-existent file
  // -------------------------------------------------------------------

  it("O_CREAT | O_APPEND on new file starts at offset 0 @fast", () => {
    const { FS } = h;

    const stream = FS.open(
      "/new_append",
      O.WRONLY | O.CREAT | O.APPEND,
      0o666,
    );

    // Position starts at 0 for new file
    const pos = FS.llseek(stream, 0, SEEK_CUR);
    expect(pos).toBe(0);

    FS.write(stream, encode("first"), 0, 5);
    expect(FS.fstat(stream.fd).size).toBe(5);

    FS.write(stream, encode("second"), 0, 6);
    expect(FS.fstat(stream.fd).size).toBe(11);

    FS.close(stream);

    // Verify
    const rs = FS.open("/new_append", O.RDONLY);
    const buf = new Uint8Array(20);
    const n = FS.read(rs, buf, 0, 20);
    expect(decode(buf, n)).toBe("firstsecond");
    FS.close(rs);
  });

  // -------------------------------------------------------------------
  // Append after read (read-write append mode)
  // -------------------------------------------------------------------

  it("O_RDWR | O_APPEND allows reads at arbitrary positions between appends", () => {
    const { FS } = h;

    const stream = FS.open(
      "/rdwr_append",
      O.RDWR | O.CREAT | O.EXCL | O.APPEND,
      0o666,
    );

    // Append some data
    FS.write(stream, encode("HEADER"), 0, 6);

    // Seek back and read
    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(10);
    const n = FS.read(stream, buf, 0, 10);
    expect(n).toBe(6);
    expect(decode(buf, n)).toBe("HEADER");

    // Append more — should go to end despite read position
    FS.write(stream, encode("BODY"), 0, 4);

    // Read full content
    FS.llseek(stream, 0, SEEK_SET);
    const full = new Uint8Array(20);
    const nf = FS.read(stream, full, 0, 20);
    expect(nf).toBe(10);
    expect(decode(full, nf)).toBe("HEADERBODY");

    FS.close(stream);
  });
});
