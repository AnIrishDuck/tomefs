/**
 * Adversarial tests: syncfs persistence under cache pressure.
 *
 * Target the seam between LRU eviction and persist/restore: when the page
 * cache is tiny (4 pages = 32 KB), dirty pages are evicted to the backend
 * during writes. syncfs must still correctly flush remaining dirty pages,
 * persist accurate metadata, and clean up orphans. On remount, restoreTree
 * must reconstruct the full tree from backend state.
 *
 * These tests combine multi-file workloads, multi-cycle syncs, and
 * file deletion/recreation patterns under extreme cache pressure --
 * exactly the scenario that breaks if flushAll/persistTree/restoreTree
 * have subtle ordering or state bugs.
 *
 * Ethos §6 (performance parity), §8 (workload scenarios), §9 (adversarial).
 */
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { describe, it, expect, beforeEach } from "vitest";
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

function encode(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

function decode(buf: Uint8Array, length?: number): string {
  return new TextDecoder().decode(
    length !== undefined ? buf.subarray(0, length) : buf,
  );
}

/** Fill a buffer with a deterministic pattern based on a seed byte. */
function fillPattern(size: number, seed: number): Uint8Array {
  const buf = new Uint8Array(size);
  for (let i = 0; i < size; i++) {
    buf[i] = (seed + i * 31) & 0xff;
  }
  return buf;
}

/** Verify a buffer matches the expected pattern. */
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

function syncAndUnmount(FS: any, tomefs: any) {
  syncfs(FS, tomefs);
  FS.unmount(MOUNT);
}

describe("adversarial: syncfs persistence under cache pressure", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("10 multi-page files survive sync+remount with 4-page cache @fast", async () => {
    // Write 10 files, each 2 pages (16 KB). Total: 20 pages.
    // Cache holds 4, so 16 pages are evicted during writes.
    const { FS, tomefs } = await mountTome(backend);

    const fileCount = 10;
    for (let i = 0; i < fileCount; i++) {
      const data = fillPattern(PAGE_SIZE * 2, i);
      const s = FS.open(`${MOUNT}/file${i}`, O.RDWR | O.CREAT, 0o666);
      FS.write(s, data, 0, data.length);
      FS.close(s);
    }

    syncAndUnmount(FS, tomefs);

    // Remount and verify every file
    const { FS: FS2 } = await mountTome(backend);
    for (let i = 0; i < fileCount; i++) {
      const stat = FS2.stat(`${MOUNT}/file${i}`);
      expect(stat.size).toBe(PAGE_SIZE * 2);

      const buf = new Uint8Array(PAGE_SIZE * 2);
      const s = FS2.open(`${MOUNT}/file${i}`, O.RDONLY);
      FS2.read(s, buf, 0, buf.length);
      FS2.close(s);
      expect(verifyPattern(buf, buf.length, i)).toBe(true);
    }
  });

  it("modify files between sync cycles under pressure", async () => {
    // Cycle 1: create 5 files
    const { FS: FS1, tomefs: t1 } = await mountTome(backend);

    for (let i = 0; i < 5; i++) {
      const data = fillPattern(PAGE_SIZE, i);
      const s = FS1.open(`${MOUNT}/f${i}`, O.RDWR | O.CREAT, 0o666);
      FS1.write(s, data, 0, data.length);
      FS1.close(s);
    }
    syncAndUnmount(FS1, t1);

    // Cycle 2: modify files 0-2, leave 3-4 unchanged
    const { FS: FS2, tomefs: t2 } = await mountTome(backend);
    for (let i = 0; i < 3; i++) {
      // Overwrite with different seed
      const data = fillPattern(PAGE_SIZE + 100, i + 100);
      const s = FS2.open(`${MOUNT}/f${i}`, O.RDWR | O.TRUNC);
      FS2.write(s, data, 0, data.length);
      FS2.close(s);
    }
    syncAndUnmount(FS2, t2);

    // Cycle 3: verify
    const { FS: FS3 } = await mountTome(backend);

    // Modified files should have new content and size
    for (let i = 0; i < 3; i++) {
      const stat = FS3.stat(`${MOUNT}/f${i}`);
      expect(stat.size).toBe(PAGE_SIZE + 100);

      const buf = new Uint8Array(PAGE_SIZE + 100);
      const s = FS3.open(`${MOUNT}/f${i}`, O.RDONLY);
      FS3.read(s, buf, 0, buf.length);
      FS3.close(s);
      expect(verifyPattern(buf, buf.length, i + 100)).toBe(true);
    }

    // Unmodified files should retain original content
    for (let i = 3; i < 5; i++) {
      const stat = FS3.stat(`${MOUNT}/f${i}`);
      expect(stat.size).toBe(PAGE_SIZE);

      const buf = new Uint8Array(PAGE_SIZE);
      const s = FS3.open(`${MOUNT}/f${i}`, O.RDONLY);
      FS3.read(s, buf, 0, buf.length);
      FS3.close(s);
      expect(verifyPattern(buf, buf.length, i)).toBe(true);
    }
  });

  it("delete and recreate files at same path under pressure", async () => {
    // Cycle 1: create files
    const { FS: FS1, tomefs: t1 } = await mountTome(backend);
    for (let i = 0; i < 4; i++) {
      const data = fillPattern(PAGE_SIZE, i);
      const s = FS1.open(`${MOUNT}/target${i}`, O.RDWR | O.CREAT, 0o666);
      FS1.write(s, data, 0, data.length);
      FS1.close(s);
    }
    syncAndUnmount(FS1, t1);

    // Cycle 2: delete all, recreate with different content
    const { FS: FS2, tomefs: t2 } = await mountTome(backend);
    for (let i = 0; i < 4; i++) {
      FS2.unlink(`${MOUNT}/target${i}`);
    }
    for (let i = 0; i < 4; i++) {
      const data = fillPattern(PAGE_SIZE * 2, i + 50); // different size + seed
      const s = FS2.open(`${MOUNT}/target${i}`, O.RDWR | O.CREAT, 0o666);
      FS2.write(s, data, 0, data.length);
      FS2.close(s);
    }
    syncAndUnmount(FS2, t2);

    // Cycle 3: verify new content
    const { FS: FS3 } = await mountTome(backend);
    for (let i = 0; i < 4; i++) {
      const stat = FS3.stat(`${MOUNT}/target${i}`);
      expect(stat.size).toBe(PAGE_SIZE * 2);

      const buf = new Uint8Array(PAGE_SIZE * 2);
      const s = FS3.open(`${MOUNT}/target${i}`, O.RDONLY);
      FS3.read(s, buf, 0, buf.length);
      FS3.close(s);
      expect(verifyPattern(buf, buf.length, i + 50)).toBe(true);
    }
  });

  it("open files during syncfs still persist under pressure", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Write to file but don't close it
    const data = fillPattern(PAGE_SIZE * 3, 42);
    const s = FS.open(`${MOUNT}/open_file`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);

    // Also create a closed file for comparison
    const data2 = fillPattern(PAGE_SIZE, 99);
    const s2 = FS.open(`${MOUNT}/closed_file`, O.RDWR | O.CREAT, 0o666);
    FS.write(s2, data2, 0, data2.length);
    FS.close(s2);

    // syncfs with open fd -- should flush dirty pages
    syncfs(FS, tomefs);
    FS.close(s);
    FS.unmount(MOUNT);

    // Remount and verify both files
    const { FS: FS2 } = await mountTome(backend);

    const stat1 = FS2.stat(`${MOUNT}/open_file`);
    expect(stat1.size).toBe(PAGE_SIZE * 3);
    const buf1 = new Uint8Array(PAGE_SIZE * 3);
    const r1 = FS2.open(`${MOUNT}/open_file`, O.RDONLY);
    FS2.read(r1, buf1, 0, buf1.length);
    FS2.close(r1);
    expect(verifyPattern(buf1, buf1.length, 42)).toBe(true);

    const stat2 = FS2.stat(`${MOUNT}/closed_file`);
    expect(stat2.size).toBe(PAGE_SIZE);
    const buf2 = new Uint8Array(PAGE_SIZE);
    const r2 = FS2.open(`${MOUNT}/closed_file`, O.RDONLY);
    FS2.read(r2, buf2, 0, buf2.length);
    FS2.close(r2);
    expect(verifyPattern(buf2, buf2.length, 99)).toBe(true);
  });

  it("large sequential file (20 pages) rotates entire cache and persists", async () => {
    // 20 pages = 160 KB. Cache is 4 pages = 32 KB.
    // Writing this file rotates the cache 5x. Every page is evicted at least once.
    const { FS, tomefs } = await mountTome(backend);

    const totalPages = 20;
    const totalSize = totalPages * PAGE_SIZE;
    const data = fillPattern(totalSize, 7);

    const s = FS.open(`${MOUNT}/big`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    FS.close(s);

    syncAndUnmount(FS, tomefs);

    // Remount and verify -- sequential read also rotates the cache
    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/big`);
    expect(stat.size).toBe(totalSize);

    const buf = new Uint8Array(totalSize);
    const r = FS2.open(`${MOUNT}/big`, O.RDONLY);
    FS2.read(r, buf, 0, buf.length);
    FS2.close(r);
    expect(verifyPattern(buf, buf.length, 7)).toBe(true);
  });

  it("mixed dirs, files, and symlinks survive multi-cycle under pressure", async () => {
    // Cycle 1: create complex tree
    const { FS: FS1, tomefs: t1 } = await mountTome(backend);

    FS1.mkdir(`${MOUNT}/src`);
    FS1.mkdir(`${MOUNT}/src/sub`);

    const d1 = fillPattern(PAGE_SIZE + 500, 10);
    let s = FS1.open(`${MOUNT}/src/a.txt`, O.RDWR | O.CREAT, 0o666);
    FS1.write(s, d1, 0, d1.length);
    FS1.close(s);

    const d2 = fillPattern(PAGE_SIZE, 20);
    s = FS1.open(`${MOUNT}/src/sub/b.txt`, O.RDWR | O.CREAT, 0o666);
    FS1.write(s, d2, 0, d2.length);
    FS1.close(s);

    FS1.symlink(`${MOUNT}/src/a.txt`, `${MOUNT}/link_a`);
    FS1.mkdir(`${MOUNT}/empty`);

    syncAndUnmount(FS1, t1);

    // Cycle 2: modify sub/b.txt, delete src/a.txt, create new file
    const { FS: FS2, tomefs: t2 } = await mountTome(backend);

    FS2.unlink(`${MOUNT}/src/a.txt`);

    const d3 = fillPattern(PAGE_SIZE * 2, 30);
    s = FS2.open(`${MOUNT}/src/sub/b.txt`, O.RDWR | O.TRUNC);
    FS2.write(s, d3, 0, d3.length);
    FS2.close(s);

    const d4 = fillPattern(500, 40);
    s = FS2.open(`${MOUNT}/src/c.txt`, O.RDWR | O.CREAT, 0o666);
    FS2.write(s, d4, 0, d4.length);
    FS2.close(s);

    syncAndUnmount(FS2, t2);

    // Cycle 3: verify final state
    const { FS: FS3 } = await mountTome(backend);

    // a.txt should be gone
    expect(() => FS3.stat(`${MOUNT}/src/a.txt`)).toThrow();

    // b.txt should have new content
    const statB = FS3.stat(`${MOUNT}/src/sub/b.txt`);
    expect(statB.size).toBe(PAGE_SIZE * 2);
    const bufB = new Uint8Array(PAGE_SIZE * 2);
    s = FS3.open(`${MOUNT}/src/sub/b.txt`, O.RDONLY);
    FS3.read(s, bufB, 0, bufB.length);
    FS3.close(s);
    expect(verifyPattern(bufB, bufB.length, 30)).toBe(true);

    // c.txt should exist
    const statC = FS3.stat(`${MOUNT}/src/c.txt`);
    expect(statC.size).toBe(500);
    const bufC = new Uint8Array(500);
    s = FS3.open(`${MOUNT}/src/c.txt`, O.RDONLY);
    FS3.read(s, bufC, 0, bufC.length);
    FS3.close(s);
    expect(verifyPattern(bufC, bufC.length, 40)).toBe(true);

    // Symlink should still point to the (now-deleted) target
    const target = FS3.readlink(`${MOUNT}/link_a`);
    expect(target).toBe(`${MOUNT}/src/a.txt`);

    // Empty dir should survive
    const entries = FS3.readdir(`${MOUNT}/empty`);
    expect(entries.sort()).toEqual([".", ".."]);

    // Directories should survive
    expect(FS3.readdir(`${MOUNT}/src`)).toContain("sub");
    expect(FS3.readdir(`${MOUNT}/src`)).toContain("c.txt");
  });

  it("interleaved writes to multiple files force cross-file eviction", async () => {
    // Write small chunks to 8 files in round-robin. Each write touches a
    // different file, maximizing cache thrashing as pages from different
    // files compete for the 4 cache slots.
    const { FS, tomefs } = await mountTome(backend);

    const fileCount = 8;
    const rounds = 10;
    const chunkSize = 200;

    // Open all files
    const streams: any[] = [];
    for (let i = 0; i < fileCount; i++) {
      streams.push(FS.open(`${MOUNT}/rr${i}`, O.RDWR | O.CREAT, 0o666));
    }

    // Round-robin writes
    for (let r = 0; r < rounds; r++) {
      for (let i = 0; i < fileCount; i++) {
        const chunk = new Uint8Array(chunkSize);
        chunk.fill((i * 10 + r) & 0xff);
        FS.write(streams[i], chunk, 0, chunkSize);
      }
    }

    // Close all
    for (const s of streams) FS.close(s);

    syncAndUnmount(FS, tomefs);

    // Remount and verify sizes + content
    const { FS: FS2 } = await mountTome(backend);
    const expectedSize = rounds * chunkSize;

    for (let i = 0; i < fileCount; i++) {
      const stat = FS2.stat(`${MOUNT}/rr${i}`);
      expect(stat.size).toBe(expectedSize);

      const buf = new Uint8Array(expectedSize);
      const s = FS2.open(`${MOUNT}/rr${i}`, O.RDONLY);
      FS2.read(s, buf, 0, buf.length);
      FS2.close(s);

      // Verify each chunk
      for (let r = 0; r < rounds; r++) {
        const offset = r * chunkSize;
        const expected = (i * 10 + r) & 0xff;
        for (let b = 0; b < chunkSize; b++) {
          expect(buf[offset + b]).toBe(expected);
        }
      }
    }
  });

  it("truncate between syncs under pressure preserves correct size", async () => {
    // Cycle 1: create a 3-page file
    const { FS: FS1, tomefs: t1 } = await mountTome(backend);
    const data = fillPattern(PAGE_SIZE * 3, 77);
    let s = FS1.open(`${MOUNT}/trunc`, O.RDWR | O.CREAT, 0o666);
    FS1.write(s, data, 0, data.length);
    FS1.close(s);
    syncAndUnmount(FS1, t1);

    // Cycle 2: truncate to sub-page size, then extend with new data
    const { FS: FS2, tomefs: t2 } = await mountTome(backend);
    FS2.truncate(`${MOUNT}/trunc`, 100);

    const ext = fillPattern(500, 88);
    s = FS2.open(`${MOUNT}/trunc`, O.RDWR | O.APPEND);
    FS2.write(s, ext, 0, ext.length);
    FS2.close(s);
    syncAndUnmount(FS2, t2);

    // Cycle 3: verify size = 100 + 500 = 600
    const { FS: FS3 } = await mountTome(backend);
    const stat = FS3.stat(`${MOUNT}/trunc`);
    expect(stat.size).toBe(600);

    const buf = new Uint8Array(600);
    s = FS3.open(`${MOUNT}/trunc`, O.RDONLY);
    FS3.read(s, buf, 0, 600);
    FS3.close(s);

    // First 100 bytes should be from original pattern (seed 77)
    for (let i = 0; i < 100; i++) {
      expect(buf[i]).toBe((77 + i * 31) & 0xff);
    }
    // Next 500 bytes should be from extension pattern (seed 88)
    for (let i = 0; i < 500; i++) {
      expect(buf[100 + i]).toBe((88 + i * 31) & 0xff);
    }
  });

  it("double syncfs is idempotent under pressure @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const data = fillPattern(PAGE_SIZE * 2, 55);
    const s = FS.open(`${MOUNT}/idem`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    FS.close(s);

    // Sync twice without modifications
    syncfs(FS, tomefs);
    syncfs(FS, tomefs);

    FS.unmount(MOUNT);

    // Verify data intact
    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/idem`);
    expect(stat.size).toBe(PAGE_SIZE * 2);

    const buf = new Uint8Array(PAGE_SIZE * 2);
    const r = FS2.open(`${MOUNT}/idem`, O.RDONLY);
    FS2.read(r, buf, 0, buf.length);
    FS2.close(r);
    expect(verifyPattern(buf, buf.length, 55)).toBe(true);
  });

  it("rename during multi-sync cycle under pressure preserves data", async () => {
    // Cycle 1: create file
    const { FS: FS1, tomefs: t1 } = await mountTome(backend);
    const data = fillPattern(PAGE_SIZE * 2, 33);
    let s = FS1.open(`${MOUNT}/orig`, O.RDWR | O.CREAT, 0o666);
    FS1.write(s, data, 0, data.length);
    FS1.close(s);
    syncAndUnmount(FS1, t1);

    // Cycle 2: rename + modify
    const { FS: FS2, tomefs: t2 } = await mountTome(backend);
    FS2.rename(`${MOUNT}/orig`, `${MOUNT}/renamed`);

    // Append to renamed file
    const extra = fillPattern(500, 44);
    s = FS2.open(`${MOUNT}/renamed`, O.RDWR | O.APPEND);
    FS2.write(s, extra, 0, extra.length);
    FS2.close(s);
    syncAndUnmount(FS2, t2);

    // Cycle 3: verify
    const { FS: FS3 } = await mountTome(backend);

    // Original path should be gone
    expect(() => FS3.stat(`${MOUNT}/orig`)).toThrow();

    // Renamed file should have original data + extension
    const stat = FS3.stat(`${MOUNT}/renamed`);
    expect(stat.size).toBe(PAGE_SIZE * 2 + 500);

    const buf = new Uint8Array(PAGE_SIZE * 2 + 500);
    s = FS3.open(`${MOUNT}/renamed`, O.RDONLY);
    FS3.read(s, buf, 0, buf.length);
    FS3.close(s);

    // Verify original data
    expect(verifyPattern(buf.subarray(0, PAGE_SIZE * 2), PAGE_SIZE * 2, 33)).toBe(true);
    // Verify appended data
    expect(verifyPattern(buf.subarray(PAGE_SIZE * 2), 500, 44)).toBe(true);
  });

  it("concurrent file creation and deletion across syncs under pressure", async () => {
    // Cycle 1: create files 0-9
    const { FS: FS1, tomefs: t1 } = await mountTome(backend);
    for (let i = 0; i < 10; i++) {
      const d = fillPattern(PAGE_SIZE, i);
      const s = FS1.open(`${MOUNT}/slot${i}`, O.RDWR | O.CREAT, 0o666);
      FS1.write(s, d, 0, d.length);
      FS1.close(s);
    }
    syncAndUnmount(FS1, t1);

    // Cycle 2: delete even files, replace odd files with bigger content
    const { FS: FS2, tomefs: t2 } = await mountTome(backend);
    for (let i = 0; i < 10; i++) {
      if (i % 2 === 0) {
        FS2.unlink(`${MOUNT}/slot${i}`);
      } else {
        const d = fillPattern(PAGE_SIZE * 2, i + 200);
        const s = FS2.open(`${MOUNT}/slot${i}`, O.RDWR | O.TRUNC);
        FS2.write(s, d, 0, d.length);
        FS2.close(s);
      }
    }
    syncAndUnmount(FS2, t2);

    // Cycle 3: verify
    const { FS: FS3 } = await mountTome(backend);
    for (let i = 0; i < 10; i++) {
      if (i % 2 === 0) {
        expect(() => FS3.stat(`${MOUNT}/slot${i}`)).toThrow();
      } else {
        const stat = FS3.stat(`${MOUNT}/slot${i}`);
        expect(stat.size).toBe(PAGE_SIZE * 2);

        const buf = new Uint8Array(PAGE_SIZE * 2);
        const s = FS3.open(`${MOUNT}/slot${i}`, O.RDONLY);
        FS3.read(s, buf, 0, buf.length);
        FS3.close(s);
        expect(verifyPattern(buf, buf.length, i + 200)).toBe(true);
      }
    }
  });

  it("WAL-style append + checkpoint pattern under pressure", async () => {
    // Simulates a common Postgres pattern:
    // 1. Append small records to a WAL file
    // 2. Checkpoint: copy data to a heap file
    // 3. Truncate WAL
    // 4. Repeat across sync cycles
    const { FS: FS1, tomefs: t1 } = await mountTome(backend);

    // Phase 1: append WAL records
    let wal = FS1.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);
    for (let i = 0; i < 20; i++) {
      const record = encode(`record-${i}\n`);
      FS1.write(wal, record, 0, record.length);
    }
    FS1.close(wal);

    // Checkpoint: copy WAL content to heap
    const walBuf = new Uint8Array(PAGE_SIZE * 2);
    wal = FS1.open(`${MOUNT}/wal`, O.RDONLY);
    const walSize = FS1.read(wal, walBuf, 0, walBuf.length);
    FS1.close(wal);

    const heap = FS1.open(`${MOUNT}/heap`, O.RDWR | O.CREAT, 0o666);
    FS1.write(heap, walBuf, 0, walSize);
    FS1.close(heap);

    // Truncate WAL
    FS1.truncate(`${MOUNT}/wal`, 0);

    syncAndUnmount(FS1, t1);

    // Phase 2: new WAL records
    const { FS: FS2, tomefs: t2 } = await mountTome(backend);

    // WAL should be empty
    const walStat = FS2.stat(`${MOUNT}/wal`);
    expect(walStat.size).toBe(0);

    // Append more records
    wal = FS2.open(`${MOUNT}/wal`, O.RDWR | O.APPEND);
    for (let i = 20; i < 30; i++) {
      const record = encode(`record-${i}\n`);
      FS2.write(wal, record, 0, record.length);
    }
    FS2.close(wal);

    syncAndUnmount(FS2, t2);

    // Phase 3: verify both heap and WAL
    const { FS: FS3 } = await mountTome(backend);

    // Read heap and verify it has records 0-19
    const heapBuf = new Uint8Array(PAGE_SIZE * 2);
    const h = FS3.open(`${MOUNT}/heap`, O.RDONLY);
    const heapSize = FS3.read(h, heapBuf, 0, heapBuf.length);
    FS3.close(h);
    const heapContent = decode(heapBuf, heapSize);
    for (let i = 0; i < 20; i++) {
      expect(heapContent).toContain(`record-${i}\n`);
    }

    // Read WAL and verify it has records 20-29
    const walBuf2 = new Uint8Array(PAGE_SIZE);
    const w = FS3.open(`${MOUNT}/wal`, O.RDONLY);
    const walSize2 = FS3.read(w, walBuf2, 0, walBuf2.length);
    FS3.close(w);
    const walContent = decode(walBuf2, walSize2);
    for (let i = 20; i < 30; i++) {
      expect(walContent).toContain(`record-${i}\n`);
    }
    // WAL should NOT have old records
    expect(walContent).not.toContain("record-0\n");
  });
});
