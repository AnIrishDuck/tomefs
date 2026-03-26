/**
 * Adversarial differential tests: Cache eviction under pressure.
 *
 * These tests force the LRU cache to evict pages during active operations.
 * They pass against MEMFS (which has no cache) and expose bugs in the
 * page cache's eviction, dirty-flush, and re-fetch paths.
 *
 * To be meaningful against tomefs, these must run with a small cache.
 * The harness creates tomefs with maxPages=4 (32 KB cache) by default.
 */
import {
  createFS,
  encode,
  decode,
  O,
  SEEK_SET,
  type FSHarness,
} from "../harness/emscripten-fs.js";

const PAGE_SIZE = 8192;

/**
 * Create a FS harness. When running against tomefs, uses a tiny cache
 * to force eviction on every operation.
 */
async function createSmallCacheFS(): Promise<FSHarness> {
  // For tomefs, we want a tiny cache. The harness reads TOMEFS_BACKEND
  // and TOMEFS_MAX_PAGES from env.
  const origMaxPages = process.env.TOMEFS_MAX_PAGES;
  process.env.TOMEFS_MAX_PAGES = "4";
  try {
    return await createFS();
  } finally {
    if (origMaxPages !== undefined) {
      process.env.TOMEFS_MAX_PAGES = origMaxPages;
    } else {
      delete process.env.TOMEFS_MAX_PAGES;
    }
  }
}

describe("adversarial: cache eviction under pressure", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createSmallCacheFS();
  });

  // ------------------------------------------------------------------
  // Write many pages, then read them all back
  // ------------------------------------------------------------------

  it("write 16 pages then read all back correctly @fast", () => {
    const { FS } = h;
    const stream = FS.open("/evict1", O.RDWR | O.CREAT, 0o666);

    const numPages = 16;
    // Write each page with a unique fill byte
    for (let p = 0; p < numPages; p++) {
      const data = new Uint8Array(PAGE_SIZE);
      data.fill(p & 0xff);
      FS.write(stream, data, 0, PAGE_SIZE, p * PAGE_SIZE);
    }

    // Read each page back — forces re-fetch from backend after eviction
    for (let p = 0; p < numPages; p++) {
      const buf = new Uint8Array(PAGE_SIZE);
      const n = FS.read(stream, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expect(n).toBe(PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) {
        expect(buf[i]).toBe(p & 0xff);
      }
    }

    FS.close(stream);
  });

  // ------------------------------------------------------------------
  // Interleaved writes to multiple files competing for cache
  // ------------------------------------------------------------------

  it("interleaved writes to 5 files all preserve data", () => {
    const { FS } = h;
    const files = [];
    const numFiles = 5;
    const pagesPerFile = 4;

    // Open all files
    for (let f = 0; f < numFiles; f++) {
      files.push(FS.open(`/file${f}`, O.RDWR | O.CREAT, 0o666));
    }

    // Write to files in round-robin order — each write evicts another file's pages
    for (let p = 0; p < pagesPerFile; p++) {
      for (let f = 0; f < numFiles; f++) {
        const data = new Uint8Array(PAGE_SIZE);
        data.fill((f * 16 + p) & 0xff);
        FS.write(files[f], data, 0, PAGE_SIZE, p * PAGE_SIZE);
      }
    }

    // Read all files back in reverse order
    for (let f = numFiles - 1; f >= 0; f--) {
      for (let p = 0; p < pagesPerFile; p++) {
        const buf = new Uint8Array(PAGE_SIZE);
        const n = FS.read(files[f], buf, 0, PAGE_SIZE, p * PAGE_SIZE);
        expect(n).toBe(PAGE_SIZE);
        const expected = (f * 16 + p) & 0xff;
        for (let i = 0; i < PAGE_SIZE; i++) {
          if (buf[i] !== expected) {
            throw new Error(
              `file${f} page ${p} byte ${i}: expected ${expected}, got ${buf[i]}`,
            );
          }
        }
      }
    }

    for (const stream of files) {
      FS.close(stream);
    }
  });

  // ------------------------------------------------------------------
  // Overwrite previously evicted pages
  // ------------------------------------------------------------------

  it("overwrite evicted pages with new data", () => {
    const { FS } = h;
    const stream = FS.open("/rewrite", O.RDWR | O.CREAT, 0o666);
    const numPages = 8;

    // First pass: write all pages with pattern A
    for (let p = 0; p < numPages; p++) {
      const data = new Uint8Array(PAGE_SIZE);
      data.fill(0xaa);
      FS.write(stream, data, 0, PAGE_SIZE, p * PAGE_SIZE);
    }

    // Second pass: overwrite all pages with pattern B
    for (let p = 0; p < numPages; p++) {
      const data = new Uint8Array(PAGE_SIZE);
      data.fill(0xbb);
      FS.write(stream, data, 0, PAGE_SIZE, p * PAGE_SIZE);
    }

    // Read back — must see pattern B, not A
    for (let p = 0; p < numPages; p++) {
      const buf = new Uint8Array(PAGE_SIZE);
      const n = FS.read(stream, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expect(n).toBe(PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) {
        expect(buf[i]).toBe(0xbb);
      }
    }

    FS.close(stream);
  });

  // ------------------------------------------------------------------
  // Read-after-write with cache rotation between them
  // ------------------------------------------------------------------

  it("read-your-writes survives cache rotation", () => {
    const { FS } = h;
    const target = FS.open("/target", O.RDWR | O.CREAT, 0o666);

    // Write to target file
    const data = encode("important data that must survive eviction");
    FS.write(target, data, 0, data.length, 0);

    // Now thrash the cache by writing to a different file
    const thrash = FS.open("/thrash", O.RDWR | O.CREAT, 0o666);
    for (let p = 0; p < 32; p++) {
      const fill = new Uint8Array(PAGE_SIZE);
      fill.fill(p);
      FS.write(thrash, fill, 0, PAGE_SIZE, p * PAGE_SIZE);
    }
    FS.close(thrash);

    // Read target — data must still be correct after cache was rotated
    const buf = new Uint8Array(data.length);
    const n = FS.read(target, buf, 0, data.length, 0);
    expect(n).toBe(data.length);
    expect(decode(buf, n)).toBe("important data that must survive eviction");

    FS.close(target);
  });

  // ------------------------------------------------------------------
  // Large sequential scan rotating entire cache multiple times
  // ------------------------------------------------------------------

  it("sequential scan of file larger than cache", () => {
    const { FS } = h;
    const stream = FS.open("/bigscan", O.RDWR | O.CREAT, 0o666);

    // Write 64 pages (512 KB) — much larger than typical 4-page test cache
    const numPages = 64;
    const totalSize = numPages * PAGE_SIZE;
    const data = new Uint8Array(totalSize);
    for (let i = 0; i < totalSize; i++) {
      data[i] = (i * 31 + 17) & 0xff;
    }
    FS.write(stream, data, 0, totalSize, 0);

    // Sequential forward scan
    let pos = 0;
    const readSize = 1000; // deliberately not page-aligned
    while (pos < totalSize) {
      const toRead = Math.min(readSize, totalSize - pos);
      const buf = new Uint8Array(toRead);
      const n = FS.read(stream, buf, 0, toRead, pos);
      expect(n).toBe(toRead);
      for (let i = 0; i < toRead; i++) {
        if (buf[i] !== data[pos + i]) {
          throw new Error(
            `Mismatch at position ${pos + i}: expected ${data[pos + i]}, got ${buf[i]}`,
          );
        }
      }
      pos += toRead;
    }

    FS.close(stream);
  });

  // ------------------------------------------------------------------
  // Reverse scan (worst case for forward-biased prefetching)
  // ------------------------------------------------------------------

  it("reverse scan reads correct data", () => {
    const { FS } = h;
    const stream = FS.open("/reverse", O.RDWR | O.CREAT, 0o666);

    const numPages = 16;
    const totalSize = numPages * PAGE_SIZE;
    const data = new Uint8Array(totalSize);
    for (let i = 0; i < totalSize; i++) {
      data[i] = (i ^ 0x5a) & 0xff;
    }
    FS.write(stream, data, 0, totalSize, 0);

    // Read pages in reverse order
    for (let p = numPages - 1; p >= 0; p--) {
      const buf = new Uint8Array(PAGE_SIZE);
      const n = FS.read(stream, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expect(n).toBe(PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) {
        const expected = ((p * PAGE_SIZE + i) ^ 0x5a) & 0xff;
        expect(buf[i]).toBe(expected);
      }
    }

    FS.close(stream);
  });

  // ------------------------------------------------------------------
  // Random access pattern
  // ------------------------------------------------------------------

  it("random access pattern reads correct data after writes", () => {
    const { FS } = h;
    const stream = FS.open("/random", O.RDWR | O.CREAT, 0o666);

    const numPages = 20;
    const totalSize = numPages * PAGE_SIZE;
    const data = new Uint8Array(totalSize);
    for (let i = 0; i < totalSize; i++) {
      data[i] = (i * 3 + 7) & 0xff;
    }
    FS.write(stream, data, 0, totalSize, 0);

    // Pseudorandom page access order (deterministic)
    const order = [7, 2, 15, 0, 19, 4, 11, 8, 13, 1, 17, 6, 3, 14, 9, 18, 5, 12, 10, 16];
    for (const p of order) {
      const buf = new Uint8Array(PAGE_SIZE);
      const n = FS.read(stream, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expect(n).toBe(PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) {
        const expected = ((p * PAGE_SIZE + i) * 3 + 7) & 0xff;
        if (buf[i] !== expected) {
          throw new Error(
            `Page ${p} byte ${i}: expected ${expected}, got ${buf[i]}`,
          );
        }
      }
    }

    FS.close(stream);
  });
});
