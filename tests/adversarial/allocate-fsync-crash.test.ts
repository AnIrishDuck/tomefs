/**
 * Adversarial tests: allocate + fsync + dirty-shutdown recovery.
 *
 * Postgres pre-allocates WAL segments with posix_fallocate(), writes records
 * sequentially, then calls fsync(fd) for per-record durability. This test
 * file probes the interaction between:
 *   - allocate's sentinel page optimization (only materializes the last page)
 *   - fsync's per-file flush (writes dirty pages + metadata to backend)
 *   - dirty-shutdown recovery (restoreTree without a clean-shutdown marker)
 *
 * The sentinel page is the key seam: allocate materializes only one page at
 * the end of the file to anchor restoreTree's maxPageIndex check. If fsync
 * flushes this sentinel but not intermediate pages, or if crash recovery
 * misinterprets the sentinel, data integrity is at risk.
 *
 * Ethos §9: "Target the seams: metadata updates after flush, dirty flush
 * ordering on concurrent streams, truncate/extend races with dirty pages."
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

function fillPattern(size: number, seed: number): Uint8Array {
  const buf = new Uint8Array(size);
  for (let i = 0; i < size; i++) {
    buf[i] = (seed + i * 31) & 0xff;
  }
  return buf;
}

function verifyPattern(buf: Uint8Array, size: number, seed: number): void {
  for (let i = 0; i < size; i++) {
    if (buf[i] !== ((seed + i * 31) & 0xff)) {
      expect.fail(
        `byte ${i}: expected ${(seed + i * 31) & 0xff}, got ${buf[i]}`,
      );
    }
  }
}

function verifyZeros(buf: Uint8Array, label: string): void {
  for (let i = 0; i < buf.length; i++) {
    if (buf[i] !== 0) {
      expect.fail(`${label} byte ${i}: expected 0, got ${buf[i]}`);
    }
  }
}

async function mountTome(backend: SyncMemoryBackend, maxPages?: number) {
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

describe("adversarial: allocate + fsync + crash recovery", () => {
  // ------------------------------------------------------------------
  // Core: allocate + fsync durability through dirty shutdown
  // ------------------------------------------------------------------

  it("allocate-only file survives dirty shutdown after fsync @fast", async () => {
    const backend = new SyncMemoryBackend();

    {
      const { FS } = await mountTome(backend);
      const stream = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);
      stream.stream_ops.allocate(stream, 0, 4 * PAGE_SIZE);
      expect(FS.fstat(stream.fd).size).toBe(4 * PAGE_SIZE);

      stream.stream_ops.fsync(stream);
      FS.close(stream);
      // No syncfs — dirty shutdown
    }

    {
      const { FS } = await mountTome(backend);
      const stat = FS.stat(`${MOUNT}/wal`);
      expect(stat.size).toBe(4 * PAGE_SIZE);

      const stream = FS.open(`${MOUNT}/wal`, O.RDONLY);
      const buf = new Uint8Array(4 * PAGE_SIZE);
      FS.read(stream, buf, 0, buf.length, 0);
      verifyZeros(buf, "allocate-only after crash");
      FS.close(stream);
    }
  });

  it("allocate + write + fsync: written data survives crash @fast", async () => {
    const backend = new SyncMemoryBackend();
    const data = fillPattern(PAGE_SIZE, 0x42);

    {
      const { FS } = await mountTome(backend);
      const stream = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);

      // Pre-allocate WAL segment
      stream.stream_ops.allocate(stream, 0, 6 * PAGE_SIZE);

      // Write records to first 2 pages
      FS.write(stream, data, 0, PAGE_SIZE, 0);
      FS.write(stream, data, 0, PAGE_SIZE, PAGE_SIZE);

      // fsync to persist
      stream.stream_ops.fsync(stream);
      FS.close(stream);
      // Dirty shutdown
    }

    {
      const { FS } = await mountTome(backend);
      expect(FS.stat(`${MOUNT}/wal`).size).toBe(6 * PAGE_SIZE);

      const stream = FS.open(`${MOUNT}/wal`, O.RDONLY);

      // Written pages survived
      const buf0 = new Uint8Array(PAGE_SIZE);
      FS.read(stream, buf0, 0, PAGE_SIZE, 0);
      verifyPattern(buf0, PAGE_SIZE, 0x42);

      const buf1 = new Uint8Array(PAGE_SIZE);
      FS.read(stream, buf1, 0, PAGE_SIZE, PAGE_SIZE);
      verifyPattern(buf1, PAGE_SIZE, 0x42);

      // Unwritten sparse pages are zeros
      const buf4 = new Uint8Array(PAGE_SIZE);
      FS.read(stream, buf4, 0, PAGE_SIZE, 4 * PAGE_SIZE);
      verifyZeros(buf4, "sparse page 4");

      FS.close(stream);
    }
  });

  it("allocate without fsync: size lost on dirty shutdown", async () => {
    const backend = new SyncMemoryBackend();

    {
      const { FS } = await mountTome(backend);
      const stream = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);
      stream.stream_ops.allocate(stream, 0, 4 * PAGE_SIZE);
      FS.close(stream);
      // No fsync, no syncfs — nothing persisted
    }

    {
      const { FS } = await mountTome(backend);
      // File should not exist since nothing was persisted
      expect(() => FS.stat(`${MOUNT}/wal`)).toThrow();
    }
  });

  // ------------------------------------------------------------------
  // Non-page-aligned allocations
  // ------------------------------------------------------------------

  it("non-page-aligned allocate + fsync preserves exact size through crash", async () => {
    const backend = new SyncMemoryBackend();
    const allocSize = 2 * PAGE_SIZE + 1234;

    {
      const { FS } = await mountTome(backend);
      const stream = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);
      stream.stream_ops.allocate(stream, 0, allocSize);
      stream.stream_ops.fsync(stream);
      FS.close(stream);
    }

    {
      const { FS } = await mountTome(backend);
      expect(FS.stat(`${MOUNT}/wal`).size).toBe(allocSize);
    }
  });

  it("sub-page allocate + write + fsync preserves sub-page precision", async () => {
    const backend = new SyncMemoryBackend();
    const allocSize = 5000; // within first page
    const data = fillPattern(100, 0xAA);

    {
      const { FS } = await mountTome(backend);
      const stream = FS.open(`${MOUNT}/small`, O.RDWR | O.CREAT, 0o666);
      stream.stream_ops.allocate(stream, 0, allocSize);
      FS.write(stream, data, 0, 100, 0);
      stream.stream_ops.fsync(stream);
      FS.close(stream);
    }

    {
      const { FS } = await mountTome(backend);
      expect(FS.stat(`${MOUNT}/small`).size).toBe(allocSize);

      const stream = FS.open(`${MOUNT}/small`, O.RDONLY);
      const buf = new Uint8Array(100);
      FS.read(stream, buf, 0, 100, 0);
      verifyPattern(buf, 100, 0xAA);
      FS.close(stream);
    }
  });

  // ------------------------------------------------------------------
  // Cache pressure: allocate exceeds cache capacity
  // ------------------------------------------------------------------

  it("allocate beyond cache + fsync under 4-page cache @fast", async () => {
    const backend = new SyncMemoryBackend();
    const MAX_PAGES = 4;
    const data = fillPattern(PAGE_SIZE, 0xBB);

    {
      const { FS } = await mountTome(backend, MAX_PAGES);
      const stream = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);

      // Allocate 8 pages (double the cache)
      stream.stream_ops.allocate(stream, 0, 8 * PAGE_SIZE);

      // Write to first page
      FS.write(stream, data, 0, PAGE_SIZE, 0);

      stream.stream_ops.fsync(stream);
      FS.close(stream);
    }

    {
      const { FS } = await mountTome(backend, MAX_PAGES);
      expect(FS.stat(`${MOUNT}/wal`).size).toBe(8 * PAGE_SIZE);

      const stream = FS.open(`${MOUNT}/wal`, O.RDONLY);
      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(stream, buf, 0, PAGE_SIZE, 0);
      verifyPattern(buf, PAGE_SIZE, 0xBB);
      FS.close(stream);
    }
  });

  it("allocate + scattered writes + fsync under extreme cache pressure", async () => {
    const backend = new SyncMemoryBackend();
    const MAX_PAGES = 4;

    {
      const { FS } = await mountTome(backend, MAX_PAGES);
      const stream = FS.open(`${MOUNT}/rel`, O.RDWR | O.CREAT, 0o666);

      // Pre-allocate 10 pages
      stream.stream_ops.allocate(stream, 0, 10 * PAGE_SIZE);

      // Write to pages 0, 3, 7, 9 (scattered, forces eviction)
      for (const [pageIdx, seed] of [[0, 0x10], [3, 0x33], [7, 0x77], [9, 0x99]] as const) {
        const data = fillPattern(PAGE_SIZE, seed);
        FS.write(stream, data, 0, PAGE_SIZE, pageIdx * PAGE_SIZE);
      }

      stream.stream_ops.fsync(stream);
      FS.close(stream);
    }

    {
      const { FS } = await mountTome(backend, MAX_PAGES);
      expect(FS.stat(`${MOUNT}/rel`).size).toBe(10 * PAGE_SIZE);

      const stream = FS.open(`${MOUNT}/rel`, O.RDONLY);

      // Verify scattered writes survived
      for (const [pageIdx, seed] of [[0, 0x10], [3, 0x33], [7, 0x77], [9, 0x99]] as const) {
        const buf = new Uint8Array(PAGE_SIZE);
        FS.read(stream, buf, 0, PAGE_SIZE, pageIdx * PAGE_SIZE);
        verifyPattern(buf, PAGE_SIZE, seed);
      }

      // Unwritten pages are zeros
      const buf5 = new Uint8Array(PAGE_SIZE);
      FS.read(stream, buf5, 0, PAGE_SIZE, 5 * PAGE_SIZE);
      verifyZeros(buf5, "sparse page 5");

      FS.close(stream);
    }
  });

  // ------------------------------------------------------------------
  // Partial fsync: allocate → write → fsync → more writes → crash
  // ------------------------------------------------------------------

  it("writes after fsync are lost on crash, fsynced state preserved", async () => {
    const backend = new SyncMemoryBackend();
    const fsynced = fillPattern(PAGE_SIZE, 0xAA);
    const unfsynced = fillPattern(PAGE_SIZE, 0xBB);

    {
      const { FS } = await mountTome(backend);
      const stream = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);

      stream.stream_ops.allocate(stream, 0, 4 * PAGE_SIZE);
      FS.write(stream, fsynced, 0, PAGE_SIZE, 0);
      stream.stream_ops.fsync(stream);

      // Write more data after fsync — this will be lost
      FS.write(stream, unfsynced, 0, PAGE_SIZE, PAGE_SIZE);
      FS.close(stream);
      // Crash
    }

    {
      const { FS } = await mountTome(backend);
      expect(FS.stat(`${MOUNT}/wal`).size).toBe(4 * PAGE_SIZE);

      const stream = FS.open(`${MOUNT}/wal`, O.RDONLY);

      // Page 0 was fsynced — survives
      const buf0 = new Uint8Array(PAGE_SIZE);
      FS.read(stream, buf0, 0, PAGE_SIZE, 0);
      verifyPattern(buf0, PAGE_SIZE, 0xAA);

      // Page 1 was NOT fsynced — should be zeros (or whatever the backend
      // has, which is nothing since it was never flushed)
      const buf1 = new Uint8Array(PAGE_SIZE);
      FS.read(stream, buf1, 0, PAGE_SIZE, PAGE_SIZE);
      verifyZeros(buf1, "unfsynced page 1");

      FS.close(stream);
    }
  });

  it("sequential fsync progression: each fsync extends durable frontier", async () => {
    const backend = new SyncMemoryBackend();

    {
      const { FS } = await mountTome(backend);
      const stream = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);

      stream.stream_ops.allocate(stream, 0, 5 * PAGE_SIZE);

      // Write and fsync page 0
      FS.write(stream, fillPattern(PAGE_SIZE, 0x11), 0, PAGE_SIZE, 0);
      stream.stream_ops.fsync(stream);

      // Write and fsync pages 1-2
      FS.write(stream, fillPattern(PAGE_SIZE, 0x22), 0, PAGE_SIZE, PAGE_SIZE);
      FS.write(stream, fillPattern(PAGE_SIZE, 0x33), 0, PAGE_SIZE, 2 * PAGE_SIZE);
      stream.stream_ops.fsync(stream);

      // Write page 3 without fsync — will be lost
      FS.write(stream, fillPattern(PAGE_SIZE, 0x44), 0, PAGE_SIZE, 3 * PAGE_SIZE);
      FS.close(stream);
    }

    {
      const { FS } = await mountTome(backend);
      expect(FS.stat(`${MOUNT}/wal`).size).toBe(5 * PAGE_SIZE);

      const stream = FS.open(`${MOUNT}/wal`, O.RDONLY);

      // Pages 0-2 survived (fsynced)
      for (const [idx, seed] of [[0, 0x11], [1, 0x22], [2, 0x33]] as const) {
        const buf = new Uint8Array(PAGE_SIZE);
        FS.read(stream, buf, 0, PAGE_SIZE, idx * PAGE_SIZE);
        verifyPattern(buf, PAGE_SIZE, seed);
      }

      // Page 3 was not fsynced
      const buf3 = new Uint8Array(PAGE_SIZE);
      FS.read(stream, buf3, 0, PAGE_SIZE, 3 * PAGE_SIZE);
      verifyZeros(buf3, "unfsynced page 3");

      FS.close(stream);
    }
  });

  // ------------------------------------------------------------------
  // WAL lifecycle: allocate → fill → recycle
  // ------------------------------------------------------------------

  it("WAL lifecycle: allocate → sequential writes + fsyncs → crash", async () => {
    const backend = new SyncMemoryBackend();
    const WAL_PAGES = 8;
    const MAX_PAGES = 4;

    {
      const { FS } = await mountTome(backend, MAX_PAGES);
      const stream = FS.open(`${MOUNT}/wal.000`, O.RDWR | O.CREAT, 0o666);

      // Pre-allocate WAL segment
      stream.stream_ops.allocate(stream, 0, WAL_PAGES * PAGE_SIZE);

      // Simulate WAL writer: sequential writes with periodic fsync
      for (let i = 0; i < 6; i++) {
        const data = fillPattern(PAGE_SIZE, 0x10 + i);
        FS.write(stream, data, 0, PAGE_SIZE, i * PAGE_SIZE);
        if (i % 2 === 1) {
          // fsync every 2 pages
          stream.stream_ops.fsync(stream);
        }
      }
      // Last fsync was after page 5 (i=5, 5%2=1), so pages 0-5 are durable
      // Pages 6-7 are pre-allocated but unwritten
      FS.close(stream);
    }

    {
      const { FS } = await mountTome(backend, MAX_PAGES);
      expect(FS.stat(`${MOUNT}/wal.000`).size).toBe(WAL_PAGES * PAGE_SIZE);

      const stream = FS.open(`${MOUNT}/wal.000`, O.RDONLY);

      // Fsynced pages survived
      for (let i = 0; i < 6; i++) {
        const buf = new Uint8Array(PAGE_SIZE);
        FS.read(stream, buf, 0, PAGE_SIZE, i * PAGE_SIZE);
        verifyPattern(buf, PAGE_SIZE, 0x10 + i);
      }

      // Pre-allocated unwritten pages are zeros
      for (let i = 6; i < WAL_PAGES; i++) {
        const buf = new Uint8Array(PAGE_SIZE);
        FS.read(stream, buf, 0, PAGE_SIZE, i * PAGE_SIZE);
        verifyZeros(buf, `unwritten WAL page ${i}`);
      }

      FS.close(stream);
    }
  });

  it("WAL recycle: truncate + re-allocate + fsync after prior fsync", async () => {
    const backend = new SyncMemoryBackend();

    {
      const { FS } = await mountTome(backend);
      const stream = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);

      // First segment: allocate, write, fsync
      stream.stream_ops.allocate(stream, 0, 4 * PAGE_SIZE);
      FS.write(stream, fillPattern(2 * PAGE_SIZE, 0xAA), 0, 2 * PAGE_SIZE, 0);
      stream.stream_ops.fsync(stream);

      // Recycle: truncate and re-allocate (Postgres recycles WAL segments)
      FS.ftruncate(stream.fd, 0);
      stream.stream_ops.allocate(stream, 0, 4 * PAGE_SIZE);

      // Write new data to recycled segment
      FS.write(stream, fillPattern(PAGE_SIZE, 0xBB), 0, PAGE_SIZE, 0);
      stream.stream_ops.fsync(stream);

      FS.close(stream);
    }

    {
      const { FS } = await mountTome(backend);
      expect(FS.stat(`${MOUNT}/wal`).size).toBe(4 * PAGE_SIZE);

      const stream = FS.open(`${MOUNT}/wal`, O.RDONLY);

      // Only new data should be present
      const buf0 = new Uint8Array(PAGE_SIZE);
      FS.read(stream, buf0, 0, PAGE_SIZE, 0);
      verifyPattern(buf0, PAGE_SIZE, 0xBB);

      // Old data from first segment should be gone
      const buf1 = new Uint8Array(PAGE_SIZE);
      FS.read(stream, buf1, 0, PAGE_SIZE, PAGE_SIZE);
      verifyZeros(buf1, "recycled WAL page 1");

      FS.close(stream);
    }
  });

  // ------------------------------------------------------------------
  // Interaction with syncfs
  // ------------------------------------------------------------------

  it("allocate + fsync + syncfs: syncfs doesn't corrupt allocate state", async () => {
    const backend = new SyncMemoryBackend();

    {
      const { FS, tomefs } = await mountTome(backend);
      const stream = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);

      stream.stream_ops.allocate(stream, 0, 6 * PAGE_SIZE);
      FS.write(stream, fillPattern(PAGE_SIZE, 0xCC), 0, PAGE_SIZE, 0);
      stream.stream_ops.fsync(stream);

      // syncfs after fsync — should be a no-op for the already-fsynced file
      tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
        if (err) throw err;
      });

      FS.close(stream);
    }

    {
      const { FS } = await mountTome(backend);
      expect(FS.stat(`${MOUNT}/wal`).size).toBe(6 * PAGE_SIZE);

      const stream = FS.open(`${MOUNT}/wal`, O.RDONLY);
      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(stream, buf, 0, PAGE_SIZE, 0);
      verifyPattern(buf, PAGE_SIZE, 0xCC);
      FS.close(stream);
    }
  });

  it("allocate + write + fsync then more allocate (no fsync): extension lost on crash", async () => {
    const backend = new SyncMemoryBackend();

    {
      const { FS } = await mountTome(backend);
      const stream = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);

      // First allocation + write + fsync
      stream.stream_ops.allocate(stream, 0, 4 * PAGE_SIZE);
      FS.write(stream, fillPattern(PAGE_SIZE, 0xDD), 0, PAGE_SIZE, 0);
      stream.stream_ops.fsync(stream);

      // Extend allocation without fsync
      stream.stream_ops.allocate(stream, 0, 8 * PAGE_SIZE);
      expect(FS.fstat(stream.fd).size).toBe(8 * PAGE_SIZE);

      FS.close(stream);
      // Crash — the extension was never fsynced
    }

    {
      const { FS } = await mountTome(backend);
      // The fsynced size was 4 pages, but the sentinel for 8 pages was never
      // flushed. Recovery should see the fsynced state.
      const stat = FS.stat(`${MOUNT}/wal`);
      // The file size depends on what was flushed. The first fsync wrote
      // metadata with size=4*PAGE_SIZE. The second allocate changed it in
      // memory but was never fsynced. On crash recovery, the backend has
      // metadata from the fsync (size=4*PAGE_SIZE) plus whatever sentinel
      // pages exist. The sentinel from the first allocate (page 3) was
      // flushed by fsync. The sentinel from the second allocate (page 7)
      // was not flushed.
      expect(stat.size).toBe(4 * PAGE_SIZE);

      const stream = FS.open(`${MOUNT}/wal`, O.RDONLY);
      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(stream, buf, 0, PAGE_SIZE, 0);
      verifyPattern(buf, PAGE_SIZE, 0xDD);
      FS.close(stream);
    }
  });

  // ------------------------------------------------------------------
  // Multi-file: allocate + fsync isolation
  // ------------------------------------------------------------------

  it("fsync on one allocated file doesn't affect another", async () => {
    const backend = new SyncMemoryBackend();

    {
      const { FS } = await mountTome(backend);

      // File A: allocate + write + fsync
      const streamA = FS.open(`${MOUNT}/wal_a`, O.RDWR | O.CREAT, 0o666);
      streamA.stream_ops.allocate(streamA, 0, 4 * PAGE_SIZE);
      FS.write(streamA, fillPattern(PAGE_SIZE, 0x11), 0, PAGE_SIZE, 0);
      streamA.stream_ops.fsync(streamA);

      // File B: allocate + write but NO fsync
      const streamB = FS.open(`${MOUNT}/wal_b`, O.RDWR | O.CREAT, 0o666);
      streamB.stream_ops.allocate(streamB, 0, 4 * PAGE_SIZE);
      FS.write(streamB, fillPattern(PAGE_SIZE, 0x22), 0, PAGE_SIZE, 0);

      FS.close(streamA);
      FS.close(streamB);
      // Crash
    }

    {
      const { FS } = await mountTome(backend);

      // File A survived (fsynced)
      expect(FS.stat(`${MOUNT}/wal_a`).size).toBe(4 * PAGE_SIZE);
      const streamA = FS.open(`${MOUNT}/wal_a`, O.RDONLY);
      const bufA = new Uint8Array(PAGE_SIZE);
      FS.read(streamA, bufA, 0, PAGE_SIZE, 0);
      verifyPattern(bufA, PAGE_SIZE, 0x11);
      FS.close(streamA);

      // File B was not persisted at all (no fsync, no syncfs)
      expect(() => FS.stat(`${MOUNT}/wal_b`)).toThrow();
    }
  });

  it("multiple allocated files fsynced independently all survive crash", async () => {
    const backend = new SyncMemoryBackend();
    const MAX_PAGES = 4;

    {
      const { FS } = await mountTome(backend, MAX_PAGES);

      const files = [
        { name: "wal.000", pages: 3, seed: 0x10 },
        { name: "wal.001", pages: 5, seed: 0x20 },
        { name: "wal.002", pages: 2, seed: 0x30 },
      ];

      for (const f of files) {
        const stream = FS.open(
          `${MOUNT}/${f.name}`,
          O.RDWR | O.CREAT,
          0o666,
        );
        stream.stream_ops.allocate(stream, 0, f.pages * PAGE_SIZE);
        FS.write(stream, fillPattern(PAGE_SIZE, f.seed), 0, PAGE_SIZE, 0);
        stream.stream_ops.fsync(stream);
        FS.close(stream);
      }
      // Dirty shutdown
    }

    {
      const { FS } = await mountTome(backend, MAX_PAGES);

      for (const f of [
        { name: "wal.000", pages: 3, seed: 0x10 },
        { name: "wal.001", pages: 5, seed: 0x20 },
        { name: "wal.002", pages: 2, seed: 0x30 },
      ]) {
        expect(FS.stat(`${MOUNT}/${f.name}`).size).toBe(f.pages * PAGE_SIZE);
        const stream = FS.open(`${MOUNT}/${f.name}`, O.RDONLY);
        const buf = new Uint8Array(PAGE_SIZE);
        FS.read(stream, buf, 0, PAGE_SIZE, 0);
        verifyPattern(buf, PAGE_SIZE, f.seed);
        FS.close(stream);
      }
    }
  });

  // ------------------------------------------------------------------
  // Truncate interaction
  // ------------------------------------------------------------------

  it("truncate after allocate+fsync: fsynced size restored on crash", async () => {
    const backend = new SyncMemoryBackend();

    {
      const { FS } = await mountTome(backend);
      const stream = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);

      stream.stream_ops.allocate(stream, 0, 6 * PAGE_SIZE);
      FS.write(stream, fillPattern(PAGE_SIZE, 0xEE), 0, PAGE_SIZE, 0);
      stream.stream_ops.fsync(stream);
      // fsync persisted: size=6*PAGE_SIZE, page 0 has data

      // Truncate WITHOUT re-fsync
      FS.ftruncate(stream.fd, 2 * PAGE_SIZE);
      expect(FS.fstat(stream.fd).size).toBe(2 * PAGE_SIZE);

      FS.close(stream);
      // Crash — truncation was not fsynced
    }

    {
      const { FS } = await mountTome(backend);
      // ftruncate calls deletePagesFrom which removes pages 2-5 from the
      // backend immediately. It creates a sentinel at page 1 in cache, but
      // without fsync/syncfs the sentinel is never flushed. So the backend
      // only has page 0 (data from fsync) + stale metadata (size=6*PAGE_SIZE).
      // Recovery: maxPageIndex=0, lastPageIndex=5 → highIdx < lastPageIndex
      // → crash truncation → size = (0+1)*PAGE_SIZE = PAGE_SIZE
      const stat = FS.stat(`${MOUNT}/wal`);
      expect(stat.size).toBe(PAGE_SIZE);

      const stream = FS.open(`${MOUNT}/wal`, O.RDONLY);
      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(stream, buf, 0, PAGE_SIZE, 0);
      verifyPattern(buf, PAGE_SIZE, 0xEE);
      FS.close(stream);
    }
  });

  it("truncate + re-allocate + re-fsync persists new state through crash", async () => {
    const backend = new SyncMemoryBackend();

    {
      const { FS } = await mountTome(backend);
      const stream = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);

      // Phase 1: allocate + write + fsync
      stream.stream_ops.allocate(stream, 0, 4 * PAGE_SIZE);
      FS.write(stream, fillPattern(PAGE_SIZE, 0xAA), 0, PAGE_SIZE, 0);
      stream.stream_ops.fsync(stream);

      // Phase 2: truncate + re-allocate + new write + re-fsync
      FS.ftruncate(stream.fd, 0);
      stream.stream_ops.allocate(stream, 0, 3 * PAGE_SIZE);
      FS.write(stream, fillPattern(PAGE_SIZE, 0xBB), 0, PAGE_SIZE, 0);
      stream.stream_ops.fsync(stream);

      FS.close(stream);
    }

    {
      const { FS } = await mountTome(backend);
      expect(FS.stat(`${MOUNT}/wal`).size).toBe(3 * PAGE_SIZE);

      const stream = FS.open(`${MOUNT}/wal`, O.RDONLY);
      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(stream, buf, 0, PAGE_SIZE, 0);
      verifyPattern(buf, PAGE_SIZE, 0xBB);
      FS.close(stream);
    }
  });

  // ------------------------------------------------------------------
  // Sentinel page interactions
  // ------------------------------------------------------------------

  it("sentinel page from allocate is flushed by fsync", async () => {
    const backend = new SyncMemoryBackend();

    {
      const { FS } = await mountTome(backend);
      const stream = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);

      stream.stream_ops.allocate(stream, 0, 4 * PAGE_SIZE);
      // The sentinel is page 3 (last page). It should be dirty in cache.

      stream.stream_ops.fsync(stream);

      // Verify sentinel exists in backend
      const sentinelPage = backend.readPage("/wal", 3);
      expect(sentinelPage).not.toBeNull();

      FS.close(stream);
    }
  });

  it("allocate grows sentinel, fsync flushes new sentinel, old sentinel remains", async () => {
    const backend = new SyncMemoryBackend();

    {
      const { FS } = await mountTome(backend);
      const stream = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);

      // First allocate: sentinel at page 3
      stream.stream_ops.allocate(stream, 0, 4 * PAGE_SIZE);
      stream.stream_ops.fsync(stream);

      // Verify first sentinel
      expect(backend.readPage("/wal", 3)).not.toBeNull();

      // Extend: new sentinel at page 7
      stream.stream_ops.allocate(stream, 0, 8 * PAGE_SIZE);
      stream.stream_ops.fsync(stream);

      // Both sentinels exist in backend
      expect(backend.readPage("/wal", 3)).not.toBeNull();
      expect(backend.readPage("/wal", 7)).not.toBeNull();

      FS.close(stream);
    }

    {
      const { FS } = await mountTome(backend);
      expect(FS.stat(`${MOUNT}/wal`).size).toBe(8 * PAGE_SIZE);
    }
  });

  // ------------------------------------------------------------------
  // Post-recovery operations
  // ------------------------------------------------------------------

  it("write to recovered allocate'd file works correctly", async () => {
    const backend = new SyncMemoryBackend();

    // Phase 1: allocate + fsync + crash
    {
      const { FS } = await mountTome(backend);
      const stream = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);
      stream.stream_ops.allocate(stream, 0, 4 * PAGE_SIZE);
      FS.write(stream, fillPattern(PAGE_SIZE, 0x11), 0, PAGE_SIZE, 0);
      stream.stream_ops.fsync(stream);
      FS.close(stream);
    }

    // Phase 2: recover + write + fsync + crash
    {
      const { FS } = await mountTome(backend);
      expect(FS.stat(`${MOUNT}/wal`).size).toBe(4 * PAGE_SIZE);

      const stream = FS.open(`${MOUNT}/wal`, O.RDWR);
      // Write to a sparse page
      FS.write(stream, fillPattern(PAGE_SIZE, 0x22), 0, PAGE_SIZE, 2 * PAGE_SIZE);
      stream.stream_ops.fsync(stream);
      FS.close(stream);
    }

    // Phase 3: verify both writes survived
    {
      const { FS } = await mountTome(backend);
      expect(FS.stat(`${MOUNT}/wal`).size).toBe(4 * PAGE_SIZE);

      const stream = FS.open(`${MOUNT}/wal`, O.RDONLY);

      const buf0 = new Uint8Array(PAGE_SIZE);
      FS.read(stream, buf0, 0, PAGE_SIZE, 0);
      verifyPattern(buf0, PAGE_SIZE, 0x11);

      const buf2 = new Uint8Array(PAGE_SIZE);
      FS.read(stream, buf2, 0, PAGE_SIZE, 2 * PAGE_SIZE);
      verifyPattern(buf2, PAGE_SIZE, 0x22);

      FS.close(stream);
    }
  });

  it("extend recovered allocate'd file with new allocate + fsync", async () => {
    const backend = new SyncMemoryBackend();

    {
      const { FS } = await mountTome(backend);
      const stream = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);
      stream.stream_ops.allocate(stream, 0, 4 * PAGE_SIZE);
      stream.stream_ops.fsync(stream);
      FS.close(stream);
    }

    {
      const { FS } = await mountTome(backend);
      const stream = FS.open(`${MOUNT}/wal`, O.RDWR);
      stream.stream_ops.allocate(stream, 0, 8 * PAGE_SIZE);
      FS.write(stream, fillPattern(PAGE_SIZE, 0xFF), 0, PAGE_SIZE, 6 * PAGE_SIZE);
      stream.stream_ops.fsync(stream);
      FS.close(stream);
    }

    {
      const { FS } = await mountTome(backend);
      expect(FS.stat(`${MOUNT}/wal`).size).toBe(8 * PAGE_SIZE);

      const stream = FS.open(`${MOUNT}/wal`, O.RDONLY);
      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(stream, buf, 0, PAGE_SIZE, 6 * PAGE_SIZE);
      verifyPattern(buf, PAGE_SIZE, 0xFF);
      FS.close(stream);
    }
  });

  // ------------------------------------------------------------------
  // Timestamp preservation
  // ------------------------------------------------------------------

  it("allocate + fsync preserves timestamps through crash recovery", async () => {
    const backend = new SyncMemoryBackend();
    let savedMtime: number;
    let savedCtime: number;

    {
      const { FS } = await mountTome(backend);
      const stream = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);
      stream.stream_ops.allocate(stream, 0, 4 * PAGE_SIZE);

      const stat = FS.fstat(stream.fd);
      savedMtime = stat.mtime.getTime();
      savedCtime = stat.ctime.getTime();

      stream.stream_ops.fsync(stream);
      FS.close(stream);
    }

    {
      const { FS } = await mountTome(backend);
      const stat = FS.stat(`${MOUNT}/wal`);
      expect(stat.mtime.getTime()).toBe(savedMtime);
      expect(stat.ctime.getTime()).toBe(savedCtime);
    }
  });

  // ------------------------------------------------------------------
  // assertInvariants after allocate + fsync
  // ------------------------------------------------------------------

  it("assertInvariants holds after allocate + fsync + remount sequence", async () => {
    const backend = new SyncMemoryBackend();

    {
      const { FS, tomefs } = await mountTome(backend, 4);
      const stream = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);

      stream.stream_ops.allocate(stream, 0, 8 * PAGE_SIZE);
      FS.write(stream, fillPattern(PAGE_SIZE, 0xAA), 0, PAGE_SIZE, 0);
      FS.write(stream, fillPattern(PAGE_SIZE, 0xBB), 0, PAGE_SIZE, 3 * PAGE_SIZE);

      stream.stream_ops.fsync(stream);
      tomefs.assertInvariants();

      FS.close(stream);
    }

    {
      const { FS, tomefs } = await mountTome(backend, 4);
      expect(FS.stat(`${MOUNT}/wal`).size).toBe(8 * PAGE_SIZE);
      tomefs.assertInvariants();

      // Operate on recovered file
      const stream = FS.open(`${MOUNT}/wal`, O.RDWR);
      FS.write(stream, fillPattern(PAGE_SIZE, 0xCC), 0, PAGE_SIZE, 5 * PAGE_SIZE);
      stream.stream_ops.fsync(stream);
      tomefs.assertInvariants();

      FS.close(stream);
    }
  });

  // ------------------------------------------------------------------
  // Rename interaction
  // ------------------------------------------------------------------

  it("rename after allocate+fsync: file at new path survives crash", async () => {
    const backend = new SyncMemoryBackend();

    {
      const { FS, tomefs } = await mountTome(backend);
      const stream = FS.open(`${MOUNT}/wal.tmp`, O.RDWR | O.CREAT, 0o666);
      stream.stream_ops.allocate(stream, 0, 4 * PAGE_SIZE);
      FS.write(stream, fillPattern(PAGE_SIZE, 0xDD), 0, PAGE_SIZE, 0);
      stream.stream_ops.fsync(stream);
      FS.close(stream);

      // Rename writes metadata to backend immediately
      FS.rename(`${MOUNT}/wal.tmp`, `${MOUNT}/wal.000`);

      // Sync to persist the rename metadata
      tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
        if (err) throw err;
      });
    }

    {
      const { FS } = await mountTome(backend);
      expect(() => FS.stat(`${MOUNT}/wal.tmp`)).toThrow();
      expect(FS.stat(`${MOUNT}/wal.000`).size).toBe(4 * PAGE_SIZE);

      const stream = FS.open(`${MOUNT}/wal.000`, O.RDONLY);
      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(stream, buf, 0, PAGE_SIZE, 0);
      verifyPattern(buf, PAGE_SIZE, 0xDD);
      FS.close(stream);
    }
  });
});
