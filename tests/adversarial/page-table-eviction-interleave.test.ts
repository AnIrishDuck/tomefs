/**
 * Adversarial tests: per-node page table (_pages) under extreme cache pressure.
 *
 * tomefs maintains a per-node sparse array (_pages) of CachedPage references
 * for O(1) page access. When a page is evicted from the LRU cache, its
 * `evicted` flag is set to true. The _pages fast path checks this flag and
 * falls through to the page cache on eviction.
 *
 * These tests stress the eviction detection with a 2-page cache (the minimum
 * viable cache size), forcing every operation to potentially evict the
 * previous operation's page. This exercises:
 *
 * 1. Stale _pages reference detection (evicted flag check)
 * 2. Multi-file writes competing for cache slots
 * 3. Read-after-write consistency when the written page was evicted
 * 4. The multi-page warm path fallback to the page cache cold path
 * 5. Page data integrity through evict → backend → reload cycles
 *
 * Ethos §9: adversarial differential testing targeting page cache seams.
 */
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { describe, it, expect } from "vitest";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { createTomeFS } from "../../src/tomefs.js";
import { PAGE_SIZE } from "../../src/types.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

const O = {
  RDONLY: 0,
  WRONLY: 1,
  RDWR: 2,
  CREAT: 64,
  TRUNC: 512,
  APPEND: 1024,
} as const;

const MOUNT = "/tome";

async function createHarness(maxPages = 2) {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  const Module = await createModule();
  const FS = Module.FS;
  const backend = new SyncMemoryBackend();
  const tomefs = createTomeFS(FS, { backend, maxPages });
  FS.mkdir(MOUNT);
  FS.mount(tomefs, {}, MOUNT);
  return { FS, backend, tomefs };
}

function syncfs(FS: any, tomefs: any) {
  tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
    if (err) throw err;
  });
}

function filledBuffer(size: number, value: number): Uint8Array {
  const buf = new Uint8Array(size);
  buf.fill(value);
  return buf;
}

function patternBuffer(size: number, seed: number): Uint8Array {
  const buf = new Uint8Array(size);
  for (let i = 0; i < size; i++) {
    buf[i] = (seed + i * 7 + (i >> 8) * 13) & 0xff;
  }
  return buf;
}

describe("adversarial: per-node page table under 2-page cache", () => {
  describe("single-file eviction cycling", () => {
    it("write page 0, write page 1, read page 0 — evicted page reloads correctly @fast", async () => {
      const { FS } = await createHarness(2);
      const fd = FS.open(`${MOUNT}/f`, O.RDWR | O.CREAT, 0o666);

      const p0 = filledBuffer(PAGE_SIZE, 0xaa);
      const p1 = filledBuffer(PAGE_SIZE, 0xbb);

      FS.write(fd, p0, 0, PAGE_SIZE, 0);
      FS.write(fd, p1, 0, PAGE_SIZE, PAGE_SIZE);

      // Page 0 was evicted when page 1 was loaded (only 2 slots).
      // Read page 0 — must reload from backend via evicted flag detection.
      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(fd, buf, 0, PAGE_SIZE, 0);
      expect(buf[0]).toBe(0xaa);
      expect(buf[PAGE_SIZE - 1]).toBe(0xaa);

      FS.close(fd);
    });

    it("sequential writes to 4 pages then random reads — all data intact @fast", async () => {
      const { FS } = await createHarness(2);
      const fd = FS.open(`${MOUNT}/seq`, O.RDWR | O.CREAT, 0o666);

      for (let i = 0; i < 4; i++) {
        FS.write(fd, filledBuffer(PAGE_SIZE, i + 1), 0, PAGE_SIZE, i * PAGE_SIZE);
      }

      // Read in reverse order — every read evicts the previous read's page
      for (let i = 3; i >= 0; i--) {
        const buf = new Uint8Array(PAGE_SIZE);
        FS.read(fd, buf, 0, PAGE_SIZE, i * PAGE_SIZE);
        expect(buf[0]).toBe(i + 1);
        expect(buf[PAGE_SIZE - 1]).toBe(i + 1);
      }

      FS.close(fd);
    });

    it("interleaved read-write on alternating pages @fast", async () => {
      const { FS } = await createHarness(2);
      const fd = FS.open(`${MOUNT}/rw`, O.RDWR | O.CREAT, 0o666);

      // Write 3 pages
      for (let i = 0; i < 3; i++) {
        FS.write(fd, filledBuffer(PAGE_SIZE, 0x10 + i), 0, PAGE_SIZE, i * PAGE_SIZE);
      }

      // Alternating: read page 0, write page 1, read page 2, write page 0
      const r0 = new Uint8Array(PAGE_SIZE);
      FS.read(fd, r0, 0, PAGE_SIZE, 0);
      expect(r0[0]).toBe(0x10);

      FS.write(fd, filledBuffer(PAGE_SIZE, 0xff), 0, PAGE_SIZE, PAGE_SIZE);

      const r2 = new Uint8Array(PAGE_SIZE);
      FS.read(fd, r2, 0, PAGE_SIZE, 2 * PAGE_SIZE);
      expect(r2[0]).toBe(0x12);

      FS.write(fd, filledBuffer(PAGE_SIZE, 0xee), 0, PAGE_SIZE, 0);

      // Verify final state
      const check0 = new Uint8Array(PAGE_SIZE);
      FS.read(fd, check0, 0, PAGE_SIZE, 0);
      expect(check0[0]).toBe(0xee);

      const check1 = new Uint8Array(PAGE_SIZE);
      FS.read(fd, check1, 0, PAGE_SIZE, PAGE_SIZE);
      expect(check1[0]).toBe(0xff);

      const check2 = new Uint8Array(PAGE_SIZE);
      FS.read(fd, check2, 0, PAGE_SIZE, 2 * PAGE_SIZE);
      expect(check2[0]).toBe(0x12);

      FS.close(fd);
    });
  });

  describe("multi-file competition", () => {
    it("writes to two files evict each other's pages @fast", async () => {
      const { FS } = await createHarness(2);
      const fd1 = FS.open(`${MOUNT}/a`, O.RDWR | O.CREAT, 0o666);
      const fd2 = FS.open(`${MOUNT}/b`, O.RDWR | O.CREAT, 0o666);

      // Write to file A page 0
      FS.write(fd1, filledBuffer(PAGE_SIZE, 0xaa), 0, PAGE_SIZE, 0);
      // Write to file B page 0 — evicts file A's page
      FS.write(fd2, filledBuffer(PAGE_SIZE, 0xbb), 0, PAGE_SIZE, 0);

      // Read file A — must reload evicted page
      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(fd1, buf, 0, PAGE_SIZE, 0);
      expect(buf[0]).toBe(0xaa);
      expect(buf[PAGE_SIZE - 1]).toBe(0xaa);

      FS.close(fd1);
      FS.close(fd2);
    });

    it("three files round-robin writes preserve all data @fast", async () => {
      const { FS } = await createHarness(2);
      const fds = [];
      for (let i = 0; i < 3; i++) {
        fds.push(FS.open(`${MOUNT}/file${i}`, O.RDWR | O.CREAT, 0o666));
      }

      // Round-robin writes: each write evicts the oldest page
      for (let round = 0; round < 3; round++) {
        for (let f = 0; f < 3; f++) {
          const value = round * 3 + f + 1;
          FS.write(fds[f], filledBuffer(PAGE_SIZE, value), 0, PAGE_SIZE, round * PAGE_SIZE);
        }
      }

      // Verify all data
      for (let round = 0; round < 3; round++) {
        for (let f = 0; f < 3; f++) {
          const expected = round * 3 + f + 1;
          const buf = new Uint8Array(PAGE_SIZE);
          FS.read(fds[f], buf, 0, PAGE_SIZE, round * PAGE_SIZE);
          expect(buf[0]).toBe(expected);
        }
      }

      for (const fd of fds) FS.close(fd);
    });

    it("write file A, write file B, overwrite file A — no stale data leak @fast", async () => {
      const { FS } = await createHarness(2);
      const a = FS.open(`${MOUNT}/a`, O.RDWR | O.CREAT, 0o666);
      const b = FS.open(`${MOUNT}/b`, O.RDWR | O.CREAT, 0o666);

      // Original write to A
      FS.write(a, filledBuffer(PAGE_SIZE, 0x11), 0, PAGE_SIZE, 0);
      // Write to B (evicts A's page from cache but it's dirty → flushed to backend)
      FS.write(b, filledBuffer(PAGE_SIZE, 0x22), 0, PAGE_SIZE, 0);
      // Overwrite A (reloads page from backend, then overwrites)
      FS.write(a, filledBuffer(PAGE_SIZE, 0x33), 0, PAGE_SIZE, 0);

      // Read A — should see the overwrite, not the original
      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(a, buf, 0, PAGE_SIZE, 0);
      expect(buf[0]).toBe(0x33);
      expect(buf[PAGE_SIZE - 1]).toBe(0x33);

      FS.close(a);
      FS.close(b);
    });
  });

  describe("cross-page-boundary operations", () => {
    it("write spanning two pages with 2-page cache — both halves correct @fast", async () => {
      const { FS } = await createHarness(2);
      const fd = FS.open(`${MOUNT}/span`, O.RDWR | O.CREAT, 0o666);

      // Write exactly at the page boundary (straddles page 0 and page 1)
      const data = patternBuffer(100, 42);
      const boundary = PAGE_SIZE - 50; // 50 bytes on page 0, 50 on page 1
      FS.write(fd, data, 0, 100, boundary);

      // Read back — with 2-page cache this is fine (both pages fit)
      const buf = new Uint8Array(100);
      FS.read(fd, buf, 0, 100, boundary);
      expect(buf).toEqual(data);

      FS.close(fd);
    });

    it("write spanning 3 pages with 2-page cache — first page evicted during write @fast", async () => {
      const { FS } = await createHarness(2);
      const fd = FS.open(`${MOUNT}/span3`, O.RDWR | O.CREAT, 0o666);

      // Write spanning pages 0, 1, and 2
      const size = PAGE_SIZE * 2 + 100;
      const data = patternBuffer(size, 99);
      FS.write(fd, data, 0, size, PAGE_SIZE - 50); // starts 50 bytes before page 1

      // Read back in chunks that force eviction
      const buf = new Uint8Array(size);
      FS.read(fd, buf, 0, size, PAGE_SIZE - 50);
      expect(buf).toEqual(data);

      FS.close(fd);
    });

    it("write page 0, write across pages 1-2, read page 0 — evicted page intact", async () => {
      const { FS } = await createHarness(2);
      const fd = FS.open(`${MOUNT}/evict_mid`, O.RDWR | O.CREAT, 0o666);

      // Fill page 0
      FS.write(fd, filledBuffer(PAGE_SIZE, 0xcc), 0, PAGE_SIZE, 0);

      // Write across pages 1-2 (forces page 0 out of cache)
      const crossData = filledBuffer(PAGE_SIZE, 0xdd);
      FS.write(fd, crossData, 0, PAGE_SIZE, PAGE_SIZE + 100);

      // Read page 0 back
      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(fd, buf, 0, PAGE_SIZE, 0);
      expect(buf[0]).toBe(0xcc);
      expect(buf[PAGE_SIZE - 1]).toBe(0xcc);

      FS.close(fd);
    });
  });

  describe("per-node page table warm path fallback", () => {
    it("multi-page read falls back to cold path when any page is evicted @fast", async () => {
      const { FS } = await createHarness(2);
      const fd = FS.open(`${MOUNT}/warm`, O.RDWR | O.CREAT, 0o666);

      // Write 3 pages
      for (let i = 0; i < 3; i++) {
        FS.write(fd, filledBuffer(PAGE_SIZE, 0x40 + i), 0, PAGE_SIZE, i * PAGE_SIZE);
      }

      // Read pages 0-1 (multi-page read, 2 pages fit in cache)
      const buf2 = new Uint8Array(PAGE_SIZE * 2);
      FS.read(fd, buf2, 0, PAGE_SIZE * 2, 0);
      expect(buf2[0]).toBe(0x40);
      expect(buf2[PAGE_SIZE]).toBe(0x41);

      // Now read page 2 (evicts one of 0-1)
      const tmp = new Uint8Array(PAGE_SIZE);
      FS.read(fd, tmp, 0, PAGE_SIZE, 2 * PAGE_SIZE);
      expect(tmp[0]).toBe(0x42);

      // Multi-page read of pages 0-1 again — one is evicted,
      // warm path detects eviction and falls back to cold path
      const buf2again = new Uint8Array(PAGE_SIZE * 2);
      FS.read(fd, buf2again, 0, PAGE_SIZE * 2, 0);
      expect(buf2again[0]).toBe(0x40);
      expect(buf2again[PAGE_SIZE]).toBe(0x41);

      FS.close(fd);
    });

    it("multi-page write falls back to cold path when page table has evicted entry", async () => {
      const { FS } = await createHarness(2);
      const fd = FS.open(`${MOUNT}/warm_w`, O.RDWR | O.CREAT, 0o666);

      // Prime pages 0-1 in the page table
      FS.write(fd, filledBuffer(PAGE_SIZE, 0x01), 0, PAGE_SIZE, 0);
      FS.write(fd, filledBuffer(PAGE_SIZE, 0x02), 0, PAGE_SIZE, PAGE_SIZE);

      // Evict page 0 by writing page 2
      FS.write(fd, filledBuffer(PAGE_SIZE, 0x03), 0, PAGE_SIZE, 2 * PAGE_SIZE);

      // Multi-page write across pages 0-1 (page 0 is evicted in _pages)
      const overwrite = filledBuffer(PAGE_SIZE * 2, 0xff);
      FS.write(fd, overwrite, 0, PAGE_SIZE * 2, 0);

      // Verify the overwrite took effect
      const r0 = new Uint8Array(PAGE_SIZE);
      FS.read(fd, r0, 0, PAGE_SIZE, 0);
      expect(r0[0]).toBe(0xff);

      const r1 = new Uint8Array(PAGE_SIZE);
      FS.read(fd, r1, 0, PAGE_SIZE, PAGE_SIZE);
      expect(r1[0]).toBe(0xff);

      FS.close(fd);
    });
  });

  describe("persistence through eviction cycles", () => {
    it("data survives syncfs → remount after extreme eviction @fast", async () => {
      const { FS, backend, tomefs } = await createHarness(2);
      const fd = FS.open(`${MOUNT}/persist`, O.RDWR | O.CREAT, 0o666);

      // Write 4 pages (twice cache capacity)
      for (let i = 0; i < 4; i++) {
        FS.write(fd, filledBuffer(PAGE_SIZE, 0xa0 + i), 0, PAGE_SIZE, i * PAGE_SIZE);
      }
      FS.close(fd);

      syncfs(FS, tomefs);

      // Remount with fresh tomefs instance on same backend
      FS.unmount(MOUNT);
      const tomefs2 = createTomeFS(FS, { backend, maxPages: 2 });
      FS.mount(tomefs2, {}, MOUNT);

      const fd2 = FS.open(`${MOUNT}/persist`, O.RDONLY);
      for (let i = 0; i < 4; i++) {
        const buf = new Uint8Array(PAGE_SIZE);
        FS.read(fd2, buf, 0, PAGE_SIZE, i * PAGE_SIZE);
        expect(buf[0]).toBe(0xa0 + i);
        expect(buf[PAGE_SIZE - 1]).toBe(0xa0 + i);
      }
      FS.close(fd2);
    });

    it("multi-file data survives syncfs with interleaved eviction", async () => {
      const { FS, backend, tomefs } = await createHarness(2);

      // Create 3 files, each with 2 pages, interleaving writes
      const files = ["alpha", "beta", "gamma"];
      const fds = files.map(f =>
        FS.open(`${MOUNT}/${f}`, O.RDWR | O.CREAT, 0o666)
      );

      for (let page = 0; page < 2; page++) {
        for (let f = 0; f < 3; f++) {
          const val = (f + 1) * 0x10 + page;
          FS.write(fds[f], filledBuffer(PAGE_SIZE, val), 0, PAGE_SIZE, page * PAGE_SIZE);
        }
      }
      for (const fd of fds) FS.close(fd);

      syncfs(FS, tomefs);

      // Remount
      FS.unmount(MOUNT);
      const tomefs2 = createTomeFS(FS, { backend, maxPages: 2 });
      FS.mount(tomefs2, {}, MOUNT);

      for (let f = 0; f < 3; f++) {
        const fd = FS.open(`${MOUNT}/${files[f]}`, O.RDONLY);
        for (let page = 0; page < 2; page++) {
          const expected = (f + 1) * 0x10 + page;
          const buf = new Uint8Array(PAGE_SIZE);
          FS.read(fd, buf, 0, PAGE_SIZE, page * PAGE_SIZE);
          expect(buf[0]).toBe(expected);
        }
        FS.close(fd);
      }
    });
  });

  describe("sub-page writes under eviction pressure", () => {
    it("small writes at different offsets within evicted pages @fast", async () => {
      const { FS } = await createHarness(2);
      const fd = FS.open(`${MOUNT}/small`, O.RDWR | O.CREAT, 0o666);

      // Write a marker at offset 100 on page 0
      FS.write(fd, new Uint8Array([0xde, 0xad]), 0, 2, 100);
      // Write a marker at offset 200 on page 1 (may evict page 0)
      FS.write(fd, new Uint8Array([0xbe, 0xef]), 0, 2, PAGE_SIZE + 200);
      // Write a marker at offset 300 on page 2 (evicts another page)
      FS.write(fd, new Uint8Array([0xca, 0xfe]), 0, 2, 2 * PAGE_SIZE + 300);

      // Read back all markers — pages must be reloaded correctly
      const b0 = new Uint8Array(2);
      FS.read(fd, b0, 0, 2, 100);
      expect(b0[0]).toBe(0xde);
      expect(b0[1]).toBe(0xad);

      const b1 = new Uint8Array(2);
      FS.read(fd, b1, 0, 2, PAGE_SIZE + 200);
      expect(b1[0]).toBe(0xbe);
      expect(b1[1]).toBe(0xef);

      const b2 = new Uint8Array(2);
      FS.read(fd, b2, 0, 2, 2 * PAGE_SIZE + 300);
      expect(b2[0]).toBe(0xca);
      expect(b2[1]).toBe(0xfe);

      FS.close(fd);
    });

    it("overwrite within evicted page preserves surrounding data @fast", async () => {
      const { FS } = await createHarness(2);
      const fd = FS.open(`${MOUNT}/partial`, O.RDWR | O.CREAT, 0o666);

      // Fill page 0 with a pattern
      FS.write(fd, patternBuffer(PAGE_SIZE, 77), 0, PAGE_SIZE, 0);
      // Write page 1 to evict page 0
      FS.write(fd, filledBuffer(PAGE_SIZE, 0xff), 0, PAGE_SIZE, PAGE_SIZE);

      // Overwrite 10 bytes in the middle of page 0 (page 0 reloaded from backend)
      const patch = new Uint8Array(10);
      patch.fill(0x00);
      FS.write(fd, patch, 0, 10, 1000);

      // Verify: bytes 0-999 have original pattern, 1000-1009 are zeros, 1010+ original
      const full = new Uint8Array(PAGE_SIZE);
      FS.read(fd, full, 0, PAGE_SIZE, 0);

      const expected = patternBuffer(PAGE_SIZE, 77);
      expected.set(patch, 1000);
      expect(full).toEqual(expected);

      FS.close(fd);
    });
  });

  describe("truncation under eviction pressure", () => {
    it("truncate then extend with 2-page cache — extended region reads as zeros @fast", async () => {
      const { FS } = await createHarness(2);
      const fd = FS.open(`${MOUNT}/trunc`, O.RDWR | O.CREAT, 0o666);

      // Write 3 pages
      for (let i = 0; i < 3; i++) {
        FS.write(fd, filledBuffer(PAGE_SIZE, 0x50 + i), 0, PAGE_SIZE, i * PAGE_SIZE);
      }

      // Truncate to 1 page
      FS.ftruncate(fd.fd, PAGE_SIZE);

      // Extend back to 3 pages by writing at page 2
      FS.write(fd, filledBuffer(PAGE_SIZE, 0x99), 0, PAGE_SIZE, 2 * PAGE_SIZE);

      // Page 0 should have original data
      const b0 = new Uint8Array(PAGE_SIZE);
      FS.read(fd, b0, 0, PAGE_SIZE, 0);
      expect(b0[0]).toBe(0x50);

      // Page 1 should be zeros (was truncated away, then gap from extension)
      const b1 = new Uint8Array(PAGE_SIZE);
      FS.read(fd, b1, 0, PAGE_SIZE, PAGE_SIZE);
      expect(b1[0]).toBe(0);
      expect(b1[PAGE_SIZE - 1]).toBe(0);

      // Page 2 should have new data
      const b2 = new Uint8Array(PAGE_SIZE);
      FS.read(fd, b2, 0, PAGE_SIZE, 2 * PAGE_SIZE);
      expect(b2[0]).toBe(0x99);

      FS.close(fd);
    });

    it("truncate to mid-page with evicted page preserves partial data @fast", async () => {
      const { FS } = await createHarness(2);
      const fd = FS.open(`${MOUNT}/midtrunc`, O.RDWR | O.CREAT, 0o666);

      // Write 2 full pages
      FS.write(fd, filledBuffer(PAGE_SIZE, 0xaa), 0, PAGE_SIZE, 0);
      FS.write(fd, filledBuffer(PAGE_SIZE, 0xbb), 0, PAGE_SIZE, PAGE_SIZE);

      // Write page 2 to evict page 0
      FS.write(fd, filledBuffer(PAGE_SIZE, 0xcc), 0, PAGE_SIZE, 2 * PAGE_SIZE);

      // Truncate to middle of page 0
      const truncSize = PAGE_SIZE / 2;
      FS.ftruncate(fd.fd, truncSize);

      // Read page 0 — first half should have original data
      const stat = FS.fstat(fd.fd);
      expect(stat.size).toBe(truncSize);

      const buf = new Uint8Array(truncSize);
      FS.read(fd, buf, 0, truncSize, 0);
      expect(buf[0]).toBe(0xaa);
      expect(buf[truncSize - 1]).toBe(0xaa);

      FS.close(fd);
    });
  });

  describe("dup'd file descriptors under eviction pressure", () => {
    it("write through dup fd, evict, read through original — data consistent @fast", async () => {
      const { FS } = await createHarness(2);
      const orig = FS.open(`${MOUNT}/dupfile`, O.RDWR | O.CREAT, 0o666);
      const dup = FS.dupStream(orig);

      // Write through dup
      FS.write(dup, filledBuffer(PAGE_SIZE, 0x77), 0, PAGE_SIZE, 0);

      // Write another file to evict the page
      const other = FS.open(`${MOUNT}/other`, O.RDWR | O.CREAT, 0o666);
      FS.write(other, filledBuffer(PAGE_SIZE, 0x88), 0, PAGE_SIZE, 0);
      FS.write(other, filledBuffer(PAGE_SIZE, 0x99), 0, PAGE_SIZE, PAGE_SIZE);

      // Read through original fd — evicted page must reload
      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(orig, buf, 0, PAGE_SIZE, 0);
      expect(buf[0]).toBe(0x77);
      expect(buf[PAGE_SIZE - 1]).toBe(0x77);

      FS.close(orig);
      FS.close(dup);
      FS.close(other);
    });
  });

  describe("sequential scan and point lookup interleave", () => {
    it("Postgres-like: sequential scan of file A while doing point lookups on file B @fast", async () => {
      const { FS } = await createHarness(2);

      // File A: 4-page heap table
      const heap = FS.open(`${MOUNT}/heap`, O.RDWR | O.CREAT, 0o666);
      for (let i = 0; i < 4; i++) {
        FS.write(heap, filledBuffer(PAGE_SIZE, 0x60 + i), 0, PAGE_SIZE, i * PAGE_SIZE);
      }

      // File B: 2-page index
      const idx = FS.open(`${MOUNT}/idx`, O.RDWR | O.CREAT, 0o666);
      FS.write(idx, filledBuffer(PAGE_SIZE, 0xf0), 0, PAGE_SIZE, 0);
      FS.write(idx, filledBuffer(PAGE_SIZE, 0xf1), 0, PAGE_SIZE, PAGE_SIZE);

      // Sequential scan of heap interleaved with index lookups
      for (let page = 0; page < 4; page++) {
        // Read heap page
        const hbuf = new Uint8Array(PAGE_SIZE);
        FS.read(heap, hbuf, 0, PAGE_SIZE, page * PAGE_SIZE);
        expect(hbuf[0]).toBe(0x60 + page);

        // Point lookup in index (evicts the heap page we just read)
        const ibuf = new Uint8Array(PAGE_SIZE);
        const idxPage = page % 2;
        FS.read(idx, ibuf, 0, PAGE_SIZE, idxPage * PAGE_SIZE);
        expect(ibuf[0]).toBe(0xf0 + idxPage);
      }

      FS.close(heap);
      FS.close(idx);
    });
  });

  describe("O_APPEND writes under eviction pressure", () => {
    it("multiple appends with eviction between each — all data present @fast", async () => {
      const { FS } = await createHarness(2);
      const fd = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT | O.APPEND, 0o666);

      // Simulate WAL: append 4 page-sized records
      for (let i = 0; i < 4; i++) {
        FS.write(fd, filledBuffer(PAGE_SIZE, 0xd0 + i), 0, PAGE_SIZE);
      }

      // Read back all 4 pages
      for (let i = 0; i < 4; i++) {
        const buf = new Uint8Array(PAGE_SIZE);
        FS.read(fd, buf, 0, PAGE_SIZE, i * PAGE_SIZE);
        expect(buf[0]).toBe(0xd0 + i);
        expect(buf[PAGE_SIZE - 1]).toBe(0xd0 + i);
      }

      FS.close(fd);
    });

    it("interleaved appends to two files — no data mixing @fast", async () => {
      const { FS } = await createHarness(2);
      const wal1 = FS.open(`${MOUNT}/wal1`, O.RDWR | O.CREAT | O.APPEND, 0o666);
      const wal2 = FS.open(`${MOUNT}/wal2`, O.RDWR | O.CREAT | O.APPEND, 0o666);

      for (let i = 0; i < 3; i++) {
        FS.write(wal1, filledBuffer(PAGE_SIZE, 0xa0 + i), 0, PAGE_SIZE);
        FS.write(wal2, filledBuffer(PAGE_SIZE, 0xb0 + i), 0, PAGE_SIZE);
      }

      for (let i = 0; i < 3; i++) {
        const b1 = new Uint8Array(PAGE_SIZE);
        FS.read(wal1, b1, 0, PAGE_SIZE, i * PAGE_SIZE);
        expect(b1[0]).toBe(0xa0 + i);

        const b2 = new Uint8Array(PAGE_SIZE);
        FS.read(wal2, b2, 0, PAGE_SIZE, i * PAGE_SIZE);
        expect(b2[0]).toBe(0xb0 + i);
      }

      FS.close(wal1);
      FS.close(wal2);
    });
  });

  describe("rename under eviction pressure", () => {
    it("rename file with evicted dirty pages — data survives at new path @fast", async () => {
      const { FS, backend, tomefs } = await createHarness(2);

      const fd = FS.open(`${MOUNT}/src`, O.RDWR | O.CREAT, 0o666);
      FS.write(fd, filledBuffer(PAGE_SIZE, 0x11), 0, PAGE_SIZE, 0);
      FS.write(fd, filledBuffer(PAGE_SIZE, 0x22), 0, PAGE_SIZE, PAGE_SIZE);
      FS.write(fd, filledBuffer(PAGE_SIZE, 0x33), 0, PAGE_SIZE, 2 * PAGE_SIZE);
      FS.close(fd);

      FS.rename(`${MOUNT}/src`, `${MOUNT}/dst`);
      syncfs(FS, tomefs);

      // Remount and verify
      FS.unmount(MOUNT);
      const tomefs2 = createTomeFS(FS, { backend, maxPages: 2 });
      FS.mount(tomefs2, {}, MOUNT);

      const fd2 = FS.open(`${MOUNT}/dst`, O.RDONLY);
      for (let i = 0; i < 3; i++) {
        const buf = new Uint8Array(PAGE_SIZE);
        FS.read(fd2, buf, 0, PAGE_SIZE, i * PAGE_SIZE);
        expect(buf[0]).toBe(0x11 * (i + 1));
      }
      FS.close(fd2);

      // Source should not exist
      expect(() => FS.stat(`${MOUNT}/src`)).toThrow();
    });
  });
});
