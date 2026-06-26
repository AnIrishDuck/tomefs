/**
 * Adversarial tests: fsync + dirty-shutdown recovery interactions.
 *
 * Verifies that fsync provides per-file durability guarantees that survive
 * dirty shutdowns (crash without syncfs). This is the mechanism Postgres
 * uses for WAL durability — after writing WAL records, it calls fsync(fd)
 * to ensure data is on stable storage before acknowledging transactions.
 *
 * Key invariants:
 *   - Fsynced data survives a dirty shutdown (no syncfs before crash)
 *   - Non-fsynced writes are lost on dirty shutdown
 *   - restoreTree correctly recovers files from fsynced backend state
 *   - fsync interacts correctly with truncation, rename, unlink
 *   - Cache-pressure-evicted pages interact with fsync durability
 *   - Multiple fsyncs track cumulative state accurately
 *
 * Ethos §9: "Target the seams: metadata updates after flush, dirty flush
 * ordering on concurrent streams, truncate/extend races with dirty pages."
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

function encode(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

function decode(buf: Uint8Array, length?: number): string {
  return new TextDecoder().decode(
    length !== undefined ? buf.subarray(0, length) : buf,
  );
}

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
      throw new Error(
        `Pattern mismatch at offset ${i}: expected ${(seed + i * 31) & 0xff}, got ${buf[i]}`,
      );
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

function syncfs(FS: any, tomefs: any) {
  tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
    if (err) throw err;
  });
}

describe("adversarial: fsync + dirty-shutdown recovery", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  // ------------------------------------------------------------------
  // Core: fsync durability through dirty shutdown
  // ------------------------------------------------------------------

  it("fsynced data survives dirty shutdown @fast", async () => {
    const { FS } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);
    const data = encode("committed transaction");
    FS.write(stream, data, 0, data.length, 0);
    stream.stream_ops.fsync(stream);
    FS.close(stream);

    // Crash: no syncfs, abandon mount, remount with same backend
    const { FS: FS2 } = await mountTome(backend);

    const stat = FS2.stat(`${MOUNT}/wal`);
    expect(stat.size).toBe(data.length);

    const s2 = FS2.open(`${MOUNT}/wal`, O.RDONLY);
    const buf = new Uint8Array(data.length);
    const n = FS2.read(s2, buf, 0, data.length, 0);
    expect(n).toBe(data.length);
    expect(decode(buf)).toBe("committed transaction");
    FS2.close(s2);
  });

  it("unfsynced writes lost on dirty shutdown", async () => {
    const { FS } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/volatile`, O.RDWR | O.CREAT, 0o666);
    const data = encode("will be lost");
    FS.write(stream, data, 0, data.length, 0);
    // No fsync, no syncfs
    FS.close(stream);

    // Crash + remount
    const { FS: FS2 } = await mountTome(backend);

    // File should not exist — nothing was persisted
    expect(() => FS2.stat(`${MOUNT}/volatile`)).toThrow();
  });

  it("fsync partial durability: fsynced data survives, post-fsync writes lost", async () => {
    const { FS } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/partial`, O.RDWR | O.CREAT, 0o666);

    // Write and fsync first batch
    const committed = encode("COMMITTED");
    FS.write(stream, committed, 0, committed.length, 0);
    stream.stream_ops.fsync(stream);

    // Write more without fsync
    const uncommitted = encode("-UNCOMMITTED");
    FS.write(stream, uncommitted, 0, uncommitted.length, committed.length);
    // No fsync or syncfs for the second write

    FS.close(stream);

    // Crash + remount
    const { FS: FS2 } = await mountTome(backend);

    // File exists with fsynced metadata (size = committed.length)
    const stat = FS2.stat(`${MOUNT}/partial`);
    expect(stat.size).toBe(committed.length);

    const s2 = FS2.open(`${MOUNT}/partial`, O.RDONLY);
    const buf = new Uint8Array(committed.length);
    const n = FS2.read(s2, buf, 0, committed.length, 0);
    expect(n).toBe(committed.length);
    expect(decode(buf)).toBe("COMMITTED");
    FS2.close(s2);
  });

  // ------------------------------------------------------------------
  // Isolation: fsync one file, not another
  // ------------------------------------------------------------------

  it("fsync isolation: only fsynced file survives crash", async () => {
    const { FS } = await mountTome(backend);

    // File A: written + fsynced
    const sA = FS.open(`${MOUNT}/durable`, O.RDWR | O.CREAT, 0o666);
    FS.write(sA, encode("durable"), 0, 7, 0);
    sA.stream_ops.fsync(sA);
    FS.close(sA);

    // File B: written, NOT fsynced
    const sB = FS.open(`${MOUNT}/volatile`, O.RDWR | O.CREAT, 0o666);
    FS.write(sB, encode("volatile"), 0, 8, 0);
    FS.close(sB);

    // Crash + remount
    const { FS: FS2 } = await mountTome(backend);

    // File A survives
    const stat = FS2.stat(`${MOUNT}/durable`);
    expect(stat.size).toBe(7);
    const s2 = FS2.open(`${MOUNT}/durable`, O.RDONLY);
    const buf = new Uint8Array(7);
    FS2.read(s2, buf, 0, 7, 0);
    expect(decode(buf)).toBe("durable");
    FS2.close(s2);

    // File B does not exist
    expect(() => FS2.stat(`${MOUNT}/volatile`)).toThrow();
  });

  // ------------------------------------------------------------------
  // Multiple fsyncs: cumulative state tracking
  // ------------------------------------------------------------------

  it("multiple fsyncs track latest state through crash", async () => {
    const { FS } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/multi`, O.RDWR | O.CREAT, 0o666);

    // First write + fsync
    FS.write(stream, encode("AAA"), 0, 3, 0);
    stream.stream_ops.fsync(stream);

    // Second write + fsync (appends)
    FS.write(stream, encode("BBB"), 0, 3, 3);
    stream.stream_ops.fsync(stream);

    // Third write + fsync (more appending)
    FS.write(stream, encode("CCC"), 0, 3, 6);
    stream.stream_ops.fsync(stream);

    FS.close(stream);

    // Crash + remount
    const { FS: FS2 } = await mountTome(backend);

    const stat = FS2.stat(`${MOUNT}/multi`);
    expect(stat.size).toBe(9);

    const s2 = FS2.open(`${MOUNT}/multi`, O.RDONLY);
    const buf = new Uint8Array(9);
    FS2.read(s2, buf, 0, 9, 0);
    expect(decode(buf)).toBe("AAABBBCCC");
    FS2.close(s2);
  });

  // ------------------------------------------------------------------
  // Overwrite after fsync: old fsynced data restored on crash
  // ------------------------------------------------------------------

  it("overwrite after fsync reverts to fsynced state on crash", async () => {
    const { FS } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/overwrite`, O.RDWR | O.CREAT, 0o666);

    // Write original data and fsync
    FS.write(stream, encode("ORIGINAL"), 0, 8, 0);
    stream.stream_ops.fsync(stream);

    // Overwrite first 4 bytes WITHOUT re-fsyncing
    FS.write(stream, encode("XXXX"), 0, 4, 0);

    FS.close(stream);

    // Crash + remount — the overwrite was only in cache
    const { FS: FS2 } = await mountTome(backend);

    const s2 = FS2.open(`${MOUNT}/overwrite`, O.RDONLY);
    const buf = new Uint8Array(8);
    FS2.read(s2, buf, 0, 8, 0);
    // The fsynced version had "ORIGINAL" in the backend page.
    // The overwrite dirtied page 0 in cache, but wasn't fsynced.
    // On crash, page 0 from backend has "ORIGINAL".
    expect(decode(buf)).toBe("ORIGINAL");
    FS2.close(s2);
  });

  // ------------------------------------------------------------------
  // fsync + truncate: truncation not persisted without fsync/syncfs
  // ------------------------------------------------------------------

  it("truncate after fsync: fsynced size restored on crash", async () => {
    const { FS } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/trunc`, O.RDWR | O.CREAT, 0o666);
    const data = new Uint8Array(1000);
    data.fill(0xab);
    FS.write(stream, data, 0, 1000, 0);
    stream.stream_ops.fsync(stream);

    // Truncate to 100 without re-fsyncing
    FS.ftruncate(stream.fd, 100);

    FS.close(stream);

    // Crash + remount — truncation wasn't persisted
    const { FS: FS2 } = await mountTome(backend);

    // Fsynced metadata says size=1000
    const stat = FS2.stat(`${MOUNT}/trunc`);
    expect(stat.size).toBe(1000);

    // Data from the fsynced page should be intact
    const s2 = FS2.open(`${MOUNT}/trunc`, O.RDONLY);
    const buf = new Uint8Array(1000);
    FS2.read(s2, buf, 0, 1000, 0);
    for (let i = 0; i < 1000; i++) {
      expect(buf[i]).toBe(0xab);
    }
    FS2.close(s2);
  });

  it("truncate + re-fsync persists new size through crash", async () => {
    const { FS } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/retreunc`, O.RDWR | O.CREAT, 0o666);
    const data = new Uint8Array(2000);
    data.fill(0xcd);
    FS.write(stream, data, 0, 2000, 0);
    stream.stream_ops.fsync(stream);

    // Truncate and re-fsync
    FS.ftruncate(stream.fd, 500);
    stream.stream_ops.fsync(stream);

    FS.close(stream);

    // Crash + remount — second fsync persisted the truncated state
    const { FS: FS2 } = await mountTome(backend);

    const stat = FS2.stat(`${MOUNT}/retreunc`);
    expect(stat.size).toBe(500);

    const s2 = FS2.open(`${MOUNT}/retreunc`, O.RDONLY);
    const buf = new Uint8Array(500);
    FS2.read(s2, buf, 0, 500, 0);
    for (let i = 0; i < 500; i++) {
      expect(buf[i]).toBe(0xcd);
    }
    FS2.close(s2);
  });

  // ------------------------------------------------------------------
  // fsync + extend past page boundary: unfsynced extension lost
  // ------------------------------------------------------------------

  it("extend past page boundary after fsync: extension lost on crash", async () => {
    const { FS } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/extend`, O.RDWR | O.CREAT, 0o666);

    // Write 100 bytes on page 0, fsync
    const first = encode("a]".repeat(50));
    FS.write(stream, first, 0, first.length, 0);
    stream.stream_ops.fsync(stream);

    // Extend to page 1 without re-fsyncing
    const ext = new Uint8Array(PAGE_SIZE);
    ext.fill(0xee);
    FS.write(stream, ext, 0, ext.length, PAGE_SIZE);

    FS.close(stream);

    // Crash + remount
    const { FS: FS2 } = await mountTome(backend);

    // Only page 0 was fsynced; page 1 was only in cache
    // Metadata from fsync says size=100
    const stat = FS2.stat(`${MOUNT}/extend`);
    expect(stat.size).toBe(first.length);

    FS2.close(FS2.open(`${MOUNT}/extend`, O.RDONLY));
  });

  // ------------------------------------------------------------------
  // Cache pressure: eviction writes pages to backend even without fsync
  // ------------------------------------------------------------------

  it("cache eviction writes unfsynced pages: recoverable on crash", async () => {
    // 4-page cache: writing 6 pages forces eviction of pages 0-1
    const { FS } = await mountTome(backend, 4);

    const stream = FS.open(`${MOUNT}/evict`, O.RDWR | O.CREAT, 0o666);

    // Write 6 pages worth of data with a unique pattern
    for (let i = 0; i < 6; i++) {
      const page = fillPattern(PAGE_SIZE, i + 1);
      FS.write(stream, page, 0, PAGE_SIZE, i * PAGE_SIZE);
    }

    // fsync: flushes remaining dirty pages + writes metadata
    stream.stream_ops.fsync(stream);
    FS.close(stream);

    // Crash + remount
    const { FS: FS2 } = await mountTome(backend);

    const stat = FS2.stat(`${MOUNT}/evict`);
    expect(stat.size).toBe(6 * PAGE_SIZE);

    const s2 = FS2.open(`${MOUNT}/evict`, O.RDONLY);
    for (let i = 0; i < 6; i++) {
      const buf = new Uint8Array(PAGE_SIZE);
      FS2.read(s2, buf, 0, PAGE_SIZE, i * PAGE_SIZE);
      verifyPattern(buf, PAGE_SIZE, i + 1);
    }
    FS2.close(s2);
  });

  it("cache eviction without fsync: evicted pages survive, cached pages lost", async () => {
    // 4-page cache. Write 8 pages → pages 0-3 evicted, pages 4-7 in cache.
    // No fsync or syncfs. On crash, only evicted pages survive.
    const { FS } = await mountTome(backend, 4);

    const stream = FS.open(`${MOUNT}/partial_evict`, O.RDWR | O.CREAT, 0o666);

    for (let i = 0; i < 8; i++) {
      const page = fillPattern(PAGE_SIZE, i + 10);
      FS.write(stream, page, 0, PAGE_SIZE, i * PAGE_SIZE);
    }

    // No fsync, no syncfs — crash
    FS.close(stream);

    // Check backend state: some pages were evicted dirty
    // The exact number depends on eviction order, but at least some pages
    // should be in the backend from dirty eviction.
    let pagesInBackend = 0;
    for (let i = 0; i < 8; i++) {
      if (backend.readPage("/partial_evict", i) !== null) {
        pagesInBackend++;
      }
    }

    // With a 4-page cache writing 8 pages, at least 4 pages must have
    // been evicted to the backend
    expect(pagesInBackend).toBeGreaterThanOrEqual(4);

    // No metadata was written (no fsync or syncfs), so restoreTree
    // won't find this file
    expect(backend.readMeta("/partial_evict")).toBeNull();
  });

  it("fsync after cache pressure: all pages durable", async () => {
    // 4-page cache, 8 pages → eviction during writes, then fsync
    const { FS } = await mountTome(backend, 4);

    const stream = FS.open(`${MOUNT}/pressure`, O.RDWR | O.CREAT, 0o666);

    for (let i = 0; i < 8; i++) {
      const page = fillPattern(PAGE_SIZE, i + 20);
      FS.write(stream, page, 0, PAGE_SIZE, i * PAGE_SIZE);
    }

    // fsync: flush remaining dirty + metadata
    stream.stream_ops.fsync(stream);
    FS.close(stream);

    // Crash + remount
    const { FS: FS2 } = await mountTome(backend);

    const stat = FS2.stat(`${MOUNT}/pressure`);
    expect(stat.size).toBe(8 * PAGE_SIZE);

    const s2 = FS2.open(`${MOUNT}/pressure`, O.RDONLY);
    for (let i = 0; i < 8; i++) {
      const buf = new Uint8Array(PAGE_SIZE);
      FS2.read(s2, buf, 0, PAGE_SIZE, i * PAGE_SIZE);
      verifyPattern(buf, PAGE_SIZE, i + 20);
    }
    FS2.close(s2);
  });

  // ------------------------------------------------------------------
  // fsync + rename: rename metadata not persisted without syncfs
  // ------------------------------------------------------------------

  it("rename after fsync: file at renamed path on crash (rename writes to backend)", async () => {
    const { FS } = await mountTome(backend);

    // Create and fsync at original path
    const stream = FS.open(`${MOUNT}/original`, O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("before rename"), 0, 13, 0);
    stream.stream_ops.fsync(stream);
    FS.close(stream);

    // Rename without syncfs — rename writes to backend immediately
    // (metadata at new path + page rename + delete old metadata)
    FS.rename(`${MOUNT}/original`, `${MOUNT}/renamed`);

    // Crash + remount — rename was already persisted to backend
    const { FS: FS2 } = await mountTome(backend);

    // File appears at the renamed path
    const stat = FS2.stat(`${MOUNT}/renamed`);
    expect(stat.size).toBe(13);

    const s2 = FS2.open(`${MOUNT}/renamed`, O.RDONLY);
    const buf = new Uint8Array(13);
    FS2.read(s2, buf, 0, 13, 0);
    expect(decode(buf)).toBe("before rename");
    FS2.close(s2);

    // Original path should not exist
    expect(() => FS2.stat(`${MOUNT}/original`)).toThrow();
  });

  it("rename + re-fsync at new path persists through crash", async () => {
    const { FS } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/a`, O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("movable"), 0, 7, 0);
    stream.stream_ops.fsync(stream);
    FS.close(stream);

    // Rename + syncfs (syncfs persists the rename)
    FS.rename(`${MOUNT}/a`, `${MOUNT}/b`);
    syncfs(FS);

    // Crash + remount
    const { FS: FS2 } = await mountTome(backend);

    // File at new path
    const stat = FS2.stat(`${MOUNT}/b`);
    expect(stat.size).toBe(7);

    // Old path gone
    expect(() => FS2.stat(`${MOUNT}/a`)).toThrow();

    function syncfs(fs: any) {
      const tomefs = fs.lookupPath(MOUNT).node.mount.type;
      tomefs.syncfs(fs.lookupPath(MOUNT).node.mount, false, (err: any) => {
        if (err) throw err;
      });
    }
  });

  // ------------------------------------------------------------------
  // fsync + multi-page non-aligned: sub-page precision through crash
  // ------------------------------------------------------------------

  it("fsync non-page-aligned multi-page file: size precision through crash", async () => {
    const { FS } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/nonaligned`, O.RDWR | O.CREAT, 0o666);

    // Write 1.5 pages (PAGE_SIZE + PAGE_SIZE/2)
    const size = PAGE_SIZE + PAGE_SIZE / 2;
    const data = fillPattern(size, 42);
    FS.write(stream, data, 0, size, 0);

    stream.stream_ops.fsync(stream);
    FS.close(stream);

    // Crash + remount
    const { FS: FS2 } = await mountTome(backend);

    const stat = FS2.stat(`${MOUNT}/nonaligned`);
    expect(stat.size).toBe(size);

    const s2 = FS2.open(`${MOUNT}/nonaligned`, O.RDONLY);
    const buf = new Uint8Array(size);
    FS2.read(s2, buf, 0, size, 0);
    verifyPattern(buf, size, 42);
    FS2.close(s2);
  });

  // ------------------------------------------------------------------
  // fsync + timestamps: mtime/ctime preserved through crash
  // ------------------------------------------------------------------

  it("fsync preserves timestamps through dirty shutdown", async () => {
    const { FS } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/ts`, O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("timestamp"), 0, 9, 0);

    const statBefore = FS.stat(`${MOUNT}/ts`);
    stream.stream_ops.fsync(stream);
    FS.close(stream);

    // Crash + remount
    const { FS: FS2 } = await mountTome(backend);

    const statAfter = FS2.stat(`${MOUNT}/ts`);
    expect(statAfter.mtime.getTime()).toBe(statBefore.mtime.getTime());
    expect(statAfter.ctime.getTime()).toBe(statBefore.ctime.getTime());
    expect(statAfter.size).toBe(9);
  });

  // ------------------------------------------------------------------
  // fsync then syncfs: no double-write, clean marker written
  // ------------------------------------------------------------------

  it("fsync then syncfs: clean shutdown + fsync data both survive remount", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // fsync file A
    const sA = FS.open(`${MOUNT}/a`, O.RDWR | O.CREAT, 0o666);
    FS.write(sA, encode("fsynced"), 0, 7, 0);
    sA.stream_ops.fsync(sA);
    FS.close(sA);

    // Write file B (no fsync)
    const sB = FS.open(`${MOUNT}/b`, O.RDWR | O.CREAT, 0o666);
    FS.write(sB, encode("synced"), 0, 6, 0);
    FS.close(sB);

    // syncfs: persists everything + writes clean marker
    syncfs(FS, tomefs);

    // Clean remount (not a crash)
    const { FS: FS2 } = await mountTome(backend);

    // Both files survive a clean remount
    expect(FS2.stat(`${MOUNT}/a`).size).toBe(7);
    expect(FS2.stat(`${MOUNT}/b`).size).toBe(6);
  });

  // ------------------------------------------------------------------
  // Multiple files fsynced: all survive crash
  // ------------------------------------------------------------------

  it("multiple files fsynced independently all survive crash", async () => {
    const { FS } = await mountTome(backend);

    const files = ["alpha", "beta", "gamma", "delta"];
    for (let i = 0; i < files.length; i++) {
      const s = FS.open(`${MOUNT}/${files[i]}`, O.RDWR | O.CREAT, 0o666);
      const data = fillPattern(100 + i * 50, i + 1);
      FS.write(s, data, 0, data.length, 0);
      s.stream_ops.fsync(s);
      FS.close(s);
    }

    // Crash + remount
    const { FS: FS2 } = await mountTome(backend);

    for (let i = 0; i < files.length; i++) {
      const size = 100 + i * 50;
      const stat = FS2.stat(`${MOUNT}/${files[i]}`);
      expect(stat.size).toBe(size);

      const s = FS2.open(`${MOUNT}/${files[i]}`, O.RDONLY);
      const buf = new Uint8Array(size);
      FS2.read(s, buf, 0, size, 0);
      verifyPattern(buf, size, i + 1);
      FS2.close(s);
    }
  });

  // ------------------------------------------------------------------
  // fsync in subdirectory: directory not persisted without syncfs
  // ------------------------------------------------------------------

  it("fsync file in subdirectory: dir metadata must be synced for recovery", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create subdirectory + file
    FS.mkdir(`${MOUNT}/subdir`);
    const stream = FS.open(
      `${MOUNT}/subdir/file`,
      O.RDWR | O.CREAT,
      0o666,
    );
    FS.write(stream, encode("in subdir"), 0, 9, 0);
    stream.stream_ops.fsync(stream);
    FS.close(stream);

    // fsync only persists the FILE's pages + metadata.
    // The DIRECTORY metadata must be synced for restoreTree to recreate
    // the path. Without syncfs, the directory may or may not be in the
    // backend depending on whether createNode wrote it.

    // Do a full syncfs to persist directory structure too
    syncfs(FS, tomefs);

    // Crash + remount
    const { FS: FS2 } = await mountTome(backend);

    const stat = FS2.stat(`${MOUNT}/subdir/file`);
    expect(stat.size).toBe(9);
  });

  // ------------------------------------------------------------------
  // fsync + dirty shutdown recovery flag: no clean marker
  // ------------------------------------------------------------------

  it("fsync without syncfs triggers dirty recovery path on remount", async () => {
    const { FS } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/dirtyrecov`, O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("test"), 0, 4, 0);
    stream.stream_ops.fsync(stream);
    FS.close(stream);

    // No syncfs → no clean marker → dirty recovery path on remount
    // Verify the clean marker is NOT in the backend
    expect(backend.readMeta("/__tomefs_clean")).toBeNull();

    // Remount — should still recover correctly via dirty path
    const { FS: FS2 } = await mountTome(backend);

    const stat = FS2.stat(`${MOUNT}/dirtyrecov`);
    expect(stat.size).toBe(4);
  });

  // ------------------------------------------------------------------
  // fsync + cache pressure + multi-file: interleaved eviction
  // ------------------------------------------------------------------

  it("fsync with interleaved multi-file writes under cache pressure", async () => {
    // 4-page cache, 3 files each with 3 pages → heavy eviction
    const { FS } = await mountTome(backend, 4);

    const streams: any[] = [];
    const sizes: number[] = [];

    // Interleave writes across 3 files
    for (let f = 0; f < 3; f++) {
      streams.push(
        FS.open(`${MOUNT}/f${f}`, O.RDWR | O.CREAT, 0o666),
      );
      sizes.push(0);
    }

    // Write one page to each file in round-robin, 3 rounds
    for (let round = 0; round < 3; round++) {
      for (let f = 0; f < 3; f++) {
        const data = fillPattern(PAGE_SIZE, f * 10 + round);
        FS.write(streams[f], data, 0, PAGE_SIZE, round * PAGE_SIZE);
        sizes[f] = (round + 1) * PAGE_SIZE;
      }
    }

    // fsync all files
    for (const s of streams) {
      s.stream_ops.fsync(s);
    }
    for (const s of streams) {
      FS.close(s);
    }

    // Crash + remount
    const { FS: FS2 } = await mountTome(backend);

    for (let f = 0; f < 3; f++) {
      const stat = FS2.stat(`${MOUNT}/f${f}`);
      expect(stat.size).toBe(3 * PAGE_SIZE);

      const s2 = FS2.open(`${MOUNT}/f${f}`, O.RDONLY);
      for (let round = 0; round < 3; round++) {
        const buf = new Uint8Array(PAGE_SIZE);
        FS2.read(s2, buf, 0, PAGE_SIZE, round * PAGE_SIZE);
        verifyPattern(buf, PAGE_SIZE, f * 10 + round);
      }
      FS2.close(s2);
    }
  });

  // ------------------------------------------------------------------
  // fsync + second crash: data survives double dirty shutdown
  // ------------------------------------------------------------------

  it("fsynced data survives two consecutive dirty shutdowns", async () => {
    const { FS } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/survive2`, O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("double crash"), 0, 12, 0);
    stream.stream_ops.fsync(stream);
    FS.close(stream);

    // First crash + remount
    const { FS: FS2 } = await mountTome(backend);
    expect(FS2.stat(`${MOUNT}/survive2`).size).toBe(12);

    // Write more on remount but don't fsync or syncfs
    const s2 = FS2.open(`${MOUNT}/survive2`, O.RDWR);
    FS2.write(s2, encode("X"), 0, 1, 12);
    FS2.close(s2);

    // Second crash + remount — original fsynced data should still be there,
    // but the new write (from the first recovery session) is lost because
    // neither fsync nor syncfs was called
    const { FS: FS3 } = await mountTome(backend);

    const stat = FS3.stat(`${MOUNT}/survive2`);
    expect(stat.size).toBe(12);

    const s3 = FS3.open(`${MOUNT}/survive2`, O.RDONLY);
    const buf = new Uint8Array(12);
    FS3.read(s3, buf, 0, 12, 0);
    expect(decode(buf)).toBe("double crash");
    FS3.close(s3);
  });

  // ------------------------------------------------------------------
  // fsync with empty file: metadata-only persistence
  // ------------------------------------------------------------------

  it("fsync empty file: metadata persists through crash", async () => {
    const { FS } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/empty`, O.RDWR | O.CREAT, 0o666);
    // No writes — just fsync the empty file
    stream.stream_ops.fsync(stream);
    FS.close(stream);

    // Crash + remount
    const { FS: FS2 } = await mountTome(backend);

    const stat = FS2.stat(`${MOUNT}/empty`);
    expect(stat.size).toBe(0);
  });

  // ------------------------------------------------------------------
  // WAL-like pattern: sequential fsyncs simulate WAL append
  // ------------------------------------------------------------------

  it("WAL append pattern: sequential writes + fsyncs simulate WAL durability", async () => {
    const { FS } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/wal_seg`, O.RDWR | O.CREAT, 0o666);
    let offset = 0;

    // Simulate 10 WAL record appends with fsync after each
    const records: string[] = [];
    for (let i = 0; i < 10; i++) {
      const record = `txn-${i.toString().padStart(3, "0")}|`;
      records.push(record);
      const data = encode(record);
      FS.write(stream, data, 0, data.length, offset);
      offset += data.length;
      stream.stream_ops.fsync(stream);
    }

    FS.close(stream);

    // Crash + remount
    const { FS: FS2 } = await mountTome(backend);

    const stat = FS2.stat(`${MOUNT}/wal_seg`);
    expect(stat.size).toBe(offset);

    const s2 = FS2.open(`${MOUNT}/wal_seg`, O.RDONLY);
    const buf = new Uint8Array(offset);
    FS2.read(s2, buf, 0, offset, 0);
    expect(decode(buf)).toBe(records.join(""));
    FS2.close(s2);
  });

  // ------------------------------------------------------------------
  // fsync + ftruncate extend: gap bytes should be zero
  // ------------------------------------------------------------------

  it("fsync + ftruncate extend: gap zeros preserved through crash", async () => {
    const { FS } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/gap`, O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("head"), 0, 4, 0);

    // Extend via ftruncate — creates a gap of zeros
    FS.ftruncate(stream.fd, 1000);

    stream.stream_ops.fsync(stream);
    FS.close(stream);

    // Crash + remount
    const { FS: FS2 } = await mountTome(backend);

    const stat = FS2.stat(`${MOUNT}/gap`);
    expect(stat.size).toBe(1000);

    const s2 = FS2.open(`${MOUNT}/gap`, O.RDONLY);
    const buf = new Uint8Array(1000);
    FS2.read(s2, buf, 0, 1000, 0);

    // First 4 bytes: "head"
    expect(decode(buf, 4)).toBe("head");

    // Bytes 4-999: POSIX-required zeros
    for (let i = 4; i < 1000; i++) {
      expect(buf[i]).toBe(0);
    }
    FS2.close(s2);
  });
});
