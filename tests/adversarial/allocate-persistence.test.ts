/**
 * Adversarial tests: allocate() + persistence (syncfs → remount).
 *
 * posix_fallocate() is used by PostgreSQL to pre-allocate WAL segments and
 * relation files. allocate() extends the file's usedBytes without writing
 * actual page data — the extended region is zero-filled on demand. This
 * creates "sparse" files where metadata claims a size larger than the number
 * of materialized pages in the backend.
 *
 * The critical seam: restoreTree verifies metadata against actual backend
 * pages. If the last expected page doesn't exist, it assumes stale metadata
 * (crash during truncation) and shrinks the file. This is WRONG for
 * allocate'd files where the metadata is correct but pages are intentionally
 * sparse.
 *
 * These tests verify that allocate'd file sizes survive syncfs → remount
 * cycles, including under cache pressure.
 *
 * Ethos §6 (performance parity), §9 (adversarial), §8 (workload scenarios).
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
const MAX_PAGES = 4; // 32 KB cache -- extreme pressure

function fillPattern(size: number, seed: number): Uint8Array {
  const buf = new Uint8Array(size);
  for (let i = 0; i < size; i++) {
    buf[i] = (seed + i * 31) & 0xff;
  }
  return buf;
}

function verifyPattern(buf: Uint8Array, size: number, seed: number): boolean {
  for (let i = 0; i < size; i++) {
    if (buf[i] !== ((seed + i * 31) & 0xff)) return false;
  }
  return true;
}

async function mountTome(backend: SyncMemoryBackend, maxPages = MAX_PAGES) {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  const Module = await createModule();
  const FS = Module.FS;
  const tomefs = createTomeFS(FS, { backend, maxPages });
  FS.mkdir(MOUNT);
  FS.mount(tomefs, {}, MOUNT);
  return { FS, tomefs };
}

function syncfs(FS: any, tomefs: any) {
  tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
    if (err) throw err;
  });
}

describe("adversarial: allocate + persistence", () => {
  it("allocate-only file preserves size across syncfs + remount @fast", async () => {
    const backend = new SyncMemoryBackend();

    // Mount, allocate an empty file to 3 pages (24 KB), syncfs
    {
      const { FS, tomefs } = await mountTome(backend);
      const stream = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);
      stream.stream_ops.allocate(stream, 0, 3 * PAGE_SIZE);
      const stat = FS.fstat(stream.fd);
      expect(stat.size).toBe(3 * PAGE_SIZE);
      FS.close(stream);
      syncfs(FS, tomefs);
    }

    // Remount and verify size survived
    {
      const { FS } = await mountTome(backend);
      const stat = FS.stat(`${MOUNT}/wal`);
      expect(stat.size).toBe(3 * PAGE_SIZE);

      // Unwritten region must read as zeros
      const stream = FS.open(`${MOUNT}/wal`, O.RDONLY);
      const buf = new Uint8Array(3 * PAGE_SIZE);
      FS.read(stream, buf, 0, 3 * PAGE_SIZE, 0);
      for (let i = 0; i < buf.length; i++) {
        expect(buf[i]).toBe(0);
      }
      FS.close(stream);
    }
  });

  it("allocate after partial write preserves both data and extended size", async () => {
    const backend = new SyncMemoryBackend();
    const data = fillPattern(PAGE_SIZE, 0x42);

    // Write 1 page, then allocate to 5 pages
    {
      const { FS, tomefs } = await mountTome(backend);
      const stream = FS.open(`${MOUNT}/rel`, O.RDWR | O.CREAT, 0o666);
      FS.write(stream, data, 0, PAGE_SIZE, 0);
      stream.stream_ops.allocate(stream, 0, 5 * PAGE_SIZE);
      expect(FS.fstat(stream.fd).size).toBe(5 * PAGE_SIZE);
      FS.close(stream);
      syncfs(FS, tomefs);
    }

    // Remount: size should be 5 pages, page 0 has data, pages 1-4 are zeros
    {
      const { FS } = await mountTome(backend);
      const stat = FS.stat(`${MOUNT}/rel`);
      expect(stat.size).toBe(5 * PAGE_SIZE);

      const stream = FS.open(`${MOUNT}/rel`, O.RDONLY);
      // Verify page 0 data
      const buf0 = new Uint8Array(PAGE_SIZE);
      FS.read(stream, buf0, 0, PAGE_SIZE, 0);
      expect(verifyPattern(buf0, PAGE_SIZE, 0x42)).toBe(true);

      // Verify pages 1-4 are zeros
      for (let p = 1; p < 5; p++) {
        const buf = new Uint8Array(PAGE_SIZE);
        FS.read(stream, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
        for (let i = 0; i < PAGE_SIZE; i++) {
          if (buf[i] !== 0) {
            expect.fail(`page ${p} offset ${i}: expected 0, got ${buf[i]}`);
          }
        }
      }
      FS.close(stream);
    }
  });

  it("allocate with non-page-aligned size survives remount", async () => {
    const backend = new SyncMemoryBackend();
    const allocSize = 2 * PAGE_SIZE + 1000; // mid-page allocation

    {
      const { FS, tomefs } = await mountTome(backend);
      const stream = FS.open(`${MOUNT}/wal2`, O.RDWR | O.CREAT, 0o666);
      stream.stream_ops.allocate(stream, 0, allocSize);
      expect(FS.fstat(stream.fd).size).toBe(allocSize);
      FS.close(stream);
      syncfs(FS, tomefs);
    }

    {
      const { FS } = await mountTome(backend);
      expect(FS.stat(`${MOUNT}/wal2`).size).toBe(allocSize);
    }
  });

  it("allocate under extreme cache pressure (allocation > cache) preserves size", async () => {
    const backend = new SyncMemoryBackend();
    // MAX_PAGES = 4, allocate 8 pages — more than the cache can hold
    const allocSize = 8 * PAGE_SIZE;

    {
      const { FS, tomefs } = await mountTome(backend, MAX_PAGES);
      const stream = FS.open(`${MOUNT}/big`, O.RDWR | O.CREAT, 0o666);
      // Write first page so the file has some content
      const data = fillPattern(PAGE_SIZE, 0xAA);
      FS.write(stream, data, 0, PAGE_SIZE, 0);
      // Allocate well beyond cache capacity
      stream.stream_ops.allocate(stream, 0, allocSize);
      expect(FS.fstat(stream.fd).size).toBe(allocSize);
      FS.close(stream);
      syncfs(FS, tomefs);
    }

    {
      const { FS } = await mountTome(backend, MAX_PAGES);
      expect(FS.stat(`${MOUNT}/big`).size).toBe(allocSize);

      // Verify first page data survived
      const stream = FS.open(`${MOUNT}/big`, O.RDONLY);
      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(stream, buf, 0, PAGE_SIZE, 0);
      expect(verifyPattern(buf, PAGE_SIZE, 0xAA)).toBe(true);
      FS.close(stream);
    }
  });

  it("allocate + write at end + syncfs + remount round-trip", async () => {
    const backend = new SyncMemoryBackend();

    {
      const { FS, tomefs } = await mountTome(backend);
      const stream = FS.open(`${MOUNT}/wal3`, O.RDWR | O.CREAT, 0o666);
      // Allocate 4 pages
      stream.stream_ops.allocate(stream, 0, 4 * PAGE_SIZE);
      // Write data at the last page
      const data = fillPattern(100, 0xBB);
      FS.write(stream, data, 0, 100, 3 * PAGE_SIZE);
      expect(FS.fstat(stream.fd).size).toBe(4 * PAGE_SIZE);
      FS.close(stream);
      syncfs(FS, tomefs);
    }

    {
      const { FS } = await mountTome(backend);
      expect(FS.stat(`${MOUNT}/wal3`).size).toBe(4 * PAGE_SIZE);

      // Verify data at last page
      const stream = FS.open(`${MOUNT}/wal3`, O.RDONLY);
      const buf = new Uint8Array(100);
      FS.read(stream, buf, 0, 100, 3 * PAGE_SIZE);
      expect(verifyPattern(buf, 100, 0xBB)).toBe(true);

      // Verify middle pages are zeros
      const zbuf = new Uint8Array(PAGE_SIZE);
      FS.read(stream, zbuf, 0, PAGE_SIZE, PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) {
        expect(zbuf[i]).toBe(0);
      }
      FS.close(stream);
    }
  });

  it("allocate + truncate + allocate cycle persists correctly", async () => {
    const backend = new SyncMemoryBackend();

    {
      const { FS, tomefs } = await mountTome(backend);
      const stream = FS.open(`${MOUNT}/cycle`, O.RDWR | O.CREAT, 0o666);

      // Allocate to 5 pages
      stream.stream_ops.allocate(stream, 0, 5 * PAGE_SIZE);
      expect(FS.fstat(stream.fd).size).toBe(5 * PAGE_SIZE);

      // Truncate to 2 pages
      FS.ftruncate(stream.fd, 2 * PAGE_SIZE);
      expect(FS.fstat(stream.fd).size).toBe(2 * PAGE_SIZE);

      // Re-allocate to 4 pages
      stream.stream_ops.allocate(stream, 0, 4 * PAGE_SIZE);
      expect(FS.fstat(stream.fd).size).toBe(4 * PAGE_SIZE);

      FS.close(stream);
      syncfs(FS, tomefs);
    }

    {
      const { FS } = await mountTome(backend);
      expect(FS.stat(`${MOUNT}/cycle`).size).toBe(4 * PAGE_SIZE);
    }
  });

  it("multiple allocate'd files coexist across persistence cycles @fast", async () => {
    const backend = new SyncMemoryBackend();
    const files = [
      { name: "wal1", size: 2 * PAGE_SIZE },
      { name: "wal2", size: 5 * PAGE_SIZE },
      { name: "wal3", size: PAGE_SIZE + 500 },
    ];

    {
      const { FS, tomefs } = await mountTome(backend);
      for (const f of files) {
        const stream = FS.open(
          `${MOUNT}/${f.name}`,
          O.RDWR | O.CREAT,
          0o666,
        );
        stream.stream_ops.allocate(stream, 0, f.size);
        expect(FS.fstat(stream.fd).size).toBe(f.size);
        FS.close(stream);
      }
      syncfs(FS, tomefs);
    }

    {
      const { FS } = await mountTome(backend);
      for (const f of files) {
        expect(FS.stat(`${MOUNT}/${f.name}`).size).toBe(f.size);
      }
    }
  });

  it("large allocate far exceeding cache does not thrash (O(1) materialization) @fast", async () => {
    const backend = new SyncMemoryBackend();
    // Allocate 32 pages with a 4-page cache. Before optimization, this
    // materialized all 32 pages — causing 28 eviction flushes of zero-filled
    // pages. After optimization, only the last page is materialized (O(1)).
    const ALLOC_PAGES = 32;
    const allocSize = ALLOC_PAGES * PAGE_SIZE;

    {
      const { FS, tomefs } = await mountTome(backend, MAX_PAGES);
      const stream = FS.open(`${MOUNT}/big_wal`, O.RDWR | O.CREAT, 0o666);

      // Write first page so the file has some real data
      const data = fillPattern(PAGE_SIZE, 0xDD);
      FS.write(stream, data, 0, PAGE_SIZE, 0);

      // Pre-allocate far beyond cache capacity
      stream.stream_ops.allocate(stream, 0, allocSize);
      expect(FS.fstat(stream.fd).size).toBe(allocSize);

      // Check cache stats: only 2 pages should have been loaded/created
      // (page 0 from the write, page 31 from allocate's sentinel).
      // Without the optimization, all 32 pages would have been touched.
      const stats = tomefs.pageCache.getStats();
      expect(stats.evictions).toBeLessThan(ALLOC_PAGES);

      FS.close(stream);
      syncfs(FS, tomefs);
    }

    // Verify persistence: size, data, and zeros all survive remount
    {
      const { FS } = await mountTome(backend, MAX_PAGES);
      expect(FS.stat(`${MOUNT}/big_wal`).size).toBe(allocSize);

      const stream = FS.open(`${MOUNT}/big_wal`, O.RDONLY);

      // Page 0 has written data
      const buf0 = new Uint8Array(PAGE_SIZE);
      FS.read(stream, buf0, 0, PAGE_SIZE, 0);
      expect(verifyPattern(buf0, PAGE_SIZE, 0xDD)).toBe(true);

      // Intermediate sparse pages are zeros
      const midBuf = new Uint8Array(PAGE_SIZE);
      FS.read(stream, midBuf, 0, PAGE_SIZE, 15 * PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) {
        expect(midBuf[i]).toBe(0);
      }

      // Last page is zeros (sentinel)
      const lastBuf = new Uint8Array(PAGE_SIZE);
      FS.read(stream, lastBuf, 0, PAGE_SIZE, (ALLOC_PAGES - 1) * PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) {
        expect(lastBuf[i]).toBe(0);
      }

      FS.close(stream);
    }
  });

  it("allocate + write at sparse middle + syncfs preserves all data @fast", async () => {
    const backend = new SyncMemoryBackend();
    // Allocate large, write at a sparse middle page, verify persistence
    const ALLOC_PAGES = 16;

    {
      const { FS, tomefs } = await mountTome(backend, MAX_PAGES);
      const stream = FS.open(`${MOUNT}/sparse`, O.RDWR | O.CREAT, 0o666);

      stream.stream_ops.allocate(stream, 0, ALLOC_PAGES * PAGE_SIZE);

      // Write data at page 7 (middle of sparse region)
      const data = fillPattern(PAGE_SIZE, 0xEE);
      FS.write(stream, data, 0, PAGE_SIZE, 7 * PAGE_SIZE);

      expect(FS.fstat(stream.fd).size).toBe(ALLOC_PAGES * PAGE_SIZE);
      FS.close(stream);
      syncfs(FS, tomefs);
    }

    {
      const { FS } = await mountTome(backend, MAX_PAGES);
      expect(FS.stat(`${MOUNT}/sparse`).size).toBe(ALLOC_PAGES * PAGE_SIZE);

      const stream = FS.open(`${MOUNT}/sparse`, O.RDONLY);

      // Page 7 has written data
      const buf7 = new Uint8Array(PAGE_SIZE);
      FS.read(stream, buf7, 0, PAGE_SIZE, 7 * PAGE_SIZE);
      expect(verifyPattern(buf7, PAGE_SIZE, 0xEE)).toBe(true);

      // Pages before and after the write are zeros
      for (const p of [0, 3, 10, ALLOC_PAGES - 2]) {
        const buf = new Uint8Array(PAGE_SIZE);
        FS.read(stream, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
        for (let i = 0; i < PAGE_SIZE; i++) {
          if (buf[i] !== 0) {
            expect.fail(`page ${p} offset ${i}: expected 0, got ${buf[i]}`);
          }
        }
      }

      FS.close(stream);
    }
  });

  it("WAL preallocation pattern: allocate → sequential write → syncfs", async () => {
    const backend = new SyncMemoryBackend();
    const WAL_SIZE = 6 * PAGE_SIZE; // pre-allocate 6 pages
    const WRITE_SIZE = 2 * PAGE_SIZE; // write 2 pages of WAL records

    {
      const { FS, tomefs } = await mountTome(backend);
      const stream = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);

      // Pre-allocate (Postgres does this for new WAL segments)
      stream.stream_ops.allocate(stream, 0, WAL_SIZE);

      // Sequential WAL writes (Postgres appends records)
      const data = fillPattern(WRITE_SIZE, 0xCC);
      FS.write(stream, data, 0, WRITE_SIZE, 0);

      expect(FS.fstat(stream.fd).size).toBe(WAL_SIZE);
      FS.close(stream);
      syncfs(FS, tomefs);
    }

    {
      const { FS } = await mountTome(backend);
      expect(FS.stat(`${MOUNT}/wal`).size).toBe(WAL_SIZE);

      // Verify written data
      const stream = FS.open(`${MOUNT}/wal`, O.RDONLY);
      const buf = new Uint8Array(WRITE_SIZE);
      FS.read(stream, buf, 0, WRITE_SIZE, 0);
      expect(verifyPattern(buf, WRITE_SIZE, 0xCC)).toBe(true);

      // Verify unwritten region is zeros
      const zbuf = new Uint8Array(PAGE_SIZE);
      FS.read(stream, zbuf, 0, PAGE_SIZE, 5 * PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) {
        expect(zbuf[i]).toBe(0);
      }
      FS.close(stream);
    }
  });
});
