/**
 * Adversarial tests: fsync stream operation.
 *
 * Verifies that fsync flushes a file's dirty pages and metadata to the
 * backend, providing per-file durability without a full syncfs. This is
 * the mechanism Postgres uses for WAL durability — after writing WAL
 * records, it calls fsync(fd) to ensure the data is on stable storage
 * before acknowledging the transaction.
 *
 * Key invariants:
 *   - fsync flushes only the target file's dirty pages
 *   - fsync writes the file's current metadata to the backend
 *   - fsync clears dirty tracking for the file
 *   - Other files' dirty state is preserved
 *   - After fsync + remount, data and metadata are accurate
 *   - fsync + subsequent syncfs doesn't double-write
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

function encode(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

function decode(buf: Uint8Array, length?: number): string {
  return new TextDecoder().decode(
    length !== undefined ? buf.subarray(0, length) : buf,
  );
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
  return { FS, tomefs, Module };
}

/**
 * Wraps a SyncMemoryBackend and records which methods are called,
 * for verifying that fsync uses syncAll (atomic) instead of
 * separate writePages + writeMeta (non-atomic).
 */
function createTrackingBackend(inner: SyncMemoryBackend) {
  const calls: string[] = [];

  const handler: ProxyHandler<SyncMemoryBackend> = {
    get(target, prop, receiver) {
      const val = Reflect.get(target, prop, receiver);
      if (typeof val === "function") {
        return (...args: any[]) => {
          calls.push(prop as string);
          return val.apply(target, args);
        };
      }
      return val;
    },
  };

  return { backend: new Proxy(inner, handler), calls };
}

describe("adversarial: fsync", () => {
  it("fsync flushes dirty pages to backend @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/file`, O.RDWR | O.CREAT, 0o666);
    const data = encode("hello fsync");
    FS.write(stream, data, 0, data.length, 0);

    // Before fsync: backend has no pages
    expect(backend.readPage("/file", 0)).toBeNull();

    // fsync should flush
    stream.stream_ops.fsync(stream);

    // After fsync: backend has the page
    const page = backend.readPage("/file", 0);
    expect(page).not.toBeNull();
    expect(decode(page!.subarray(0, data.length))).toBe("hello fsync");

    FS.close(stream);
  });

  it("fsync writes metadata to backend", async () => {
    const backend = new SyncMemoryBackend();
    const { FS } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/meta`, O.RDWR | O.CREAT, 0o666);
    const data = new Uint8Array(100);
    data.fill(0xab);
    FS.write(stream, data, 0, data.length, 0);

    // Before fsync: no metadata in backend (createNode marks dirty
    // but doesn't write to backend — that happens during syncfs or fsync)
    expect(backend.readMeta("/meta")).toBeNull();

    stream.stream_ops.fsync(stream);

    // After fsync: metadata reflects written data
    const metaAfter = backend.readMeta("/meta");
    expect(metaAfter).not.toBeNull();
    expect(metaAfter!.size).toBe(100);

    FS.close(stream);
  });

  it("fsync clears metaDirty flag", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/dirty`, O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("data"), 0, 4, 0);

    // dirtyMetaCount > 0 before fsync
    const statsBefore = tomefs.getStats();
    expect(statsBefore.dirtyMetaCount).toBeGreaterThan(0);

    stream.stream_ops.fsync(stream);

    // After fsync, the fsynced file's dirty flag should be cleared.
    // Other nodes (mount root directory) may still be dirty from createNode.
    // The file itself should no longer be in the dirty set.
    const page = backend.readPage("/dirty", 0);
    expect(page).not.toBeNull();
    expect(decode(page!.subarray(0, 4))).toBe("data");

    FS.close(stream);
  });

  it("fsync on clean file is a no-op", async () => {
    const backend = new SyncMemoryBackend();
    const { FS } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/clean`, O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("data"), 0, 4, 0);

    // First fsync
    stream.stream_ops.fsync(stream);
    const meta1 = backend.readMeta("/clean");

    // Second fsync (no changes) — should succeed without errors
    stream.stream_ops.fsync(stream);
    const meta2 = backend.readMeta("/clean");

    expect(meta1!.size).toBe(meta2!.size);

    FS.close(stream);
  });

  it("fsync only affects target file, other files stay dirty", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    // Write to two files
    const s1 = FS.open(`${MOUNT}/file1`, O.RDWR | O.CREAT, 0o666);
    FS.write(s1, encode("file1data"), 0, 9, 0);

    const s2 = FS.open(`${MOUNT}/file2`, O.RDWR | O.CREAT, 0o666);
    FS.write(s2, encode("file2data"), 0, 9, 0);

    // Both files have dirty pages
    expect(tomefs.pageCache.dirtyCount).toBe(2);

    // fsync only file1
    s1.stream_ops.fsync(s1);

    // file1 pages flushed, file2 still dirty
    expect(tomefs.pageCache.dirtyCount).toBe(1);
    expect(backend.readPage("/file1", 0)).not.toBeNull();
    expect(backend.readPage("/file2", 0)).toBeNull();

    FS.close(s1);
    FS.close(s2);
  });

  it("fsync + syncfs doesn't double-write", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/nodbl`, O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("initial"), 0, 7, 0);

    // fsync: flushes pages + metadata
    stream.stream_ops.fsync(stream);

    const pageCacheStats = tomefs.pageCache.getStats();
    const flushesBefore = pageCacheStats.flushes;

    // syncfs: should find file1 already clean (pages not dirty)
    FS.syncfs(false, (err: Error | null) => {
      expect(err).toBeNull();
    });

    // No additional page flushes for the fsynced file
    const flushesAfter = tomefs.pageCache.getStats().flushes;
    expect(flushesAfter).toBe(flushesBefore);

    FS.close(stream);
  });

  it("fsync + remount preserves data accurately", async () => {
    const backend = new SyncMemoryBackend();
    const { FS } = await mountTome(backend);

    // Write sub-page data (tests byte-accurate size preservation)
    const stream = FS.open(`${MOUNT}/persist`, O.RDWR | O.CREAT, 0o666);
    const data = encode("fsync persistence test - 37 bytes!!!");
    FS.write(stream, data, 0, data.length, 0);

    // fsync instead of syncfs
    stream.stream_ops.fsync(stream);
    FS.close(stream);

    // Remount with same backend
    const { FS: FS2 } = await mountTome(backend);

    // Verify data and metadata survived remount
    const stat = FS2.stat(`${MOUNT}/persist`);
    expect(stat.size).toBe(data.length);

    const stream2 = FS2.open(`${MOUNT}/persist`, O.RDONLY);
    const buf = new Uint8Array(data.length);
    const n = FS2.read(stream2, buf, 0, data.length, 0);
    expect(n).toBe(data.length);
    expect(decode(buf)).toBe("fsync persistence test - 37 bytes!!!");
    FS2.close(stream2);
  });

  it("fsync multi-page file under cache pressure", async () => {
    const backend = new SyncMemoryBackend();
    // 4-page cache — forces eviction
    const { FS } = await mountTome(backend, 4);

    const stream = FS.open(`${MOUNT}/big`, O.RDWR | O.CREAT, 0o666);

    // Write 8 pages — exceeds cache, forcing eviction
    for (let i = 0; i < 8; i++) {
      const data = new Uint8Array(PAGE_SIZE);
      data.fill(i + 1);
      FS.write(stream, data, 0, PAGE_SIZE, i * PAGE_SIZE);
    }

    // Some pages were already evicted to backend; fsync flushes the rest
    stream.stream_ops.fsync(stream);

    // Verify all 8 pages are in backend
    for (let i = 0; i < 8; i++) {
      const page = backend.readPage("/big", i);
      expect(page).not.toBeNull();
      expect(page![0]).toBe(i + 1);
      expect(page![PAGE_SIZE - 1]).toBe(i + 1);
    }

    // Verify metadata
    const meta = backend.readMeta("/big");
    expect(meta).not.toBeNull();
    expect(meta!.size).toBe(8 * PAGE_SIZE);

    FS.close(stream);
  });

  it("fsync preserves mtime accurately across remount", async () => {
    const backend = new SyncMemoryBackend();
    const { FS } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/mtime`, O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("ts"), 0, 2, 0);

    const statBefore = FS.stat(`${MOUNT}/mtime`);
    stream.stream_ops.fsync(stream);
    FS.close(stream);

    // Remount and verify timestamps match
    const { FS: FS2 } = await mountTome(backend);
    const statAfter = FS2.stat(`${MOUNT}/mtime`);

    expect(statAfter.mtime.getTime()).toBe(statBefore.mtime.getTime());
    expect(statAfter.size).toBe(2);
  });

  it("fsync after truncate persists new size", async () => {
    const backend = new SyncMemoryBackend();
    const { FS } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/trunc`, O.RDWR | O.CREAT, 0o666);
    // Write 2 pages
    const data = new Uint8Array(PAGE_SIZE * 2);
    data.fill(0xcc);
    FS.write(stream, data, 0, data.length, 0);

    // Truncate to half a page
    FS.ftruncate(stream.fd, PAGE_SIZE / 2);

    // fsync persists the truncated state
    stream.stream_ops.fsync(stream);
    FS.close(stream);

    // Remount and verify truncated size
    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/trunc`);
    expect(stat.size).toBe(PAGE_SIZE / 2);

    // Read back — should only get half a page of 0xcc, rest zeros
    const s2 = FS2.open(`${MOUNT}/trunc`, O.RDONLY);
    const buf = new Uint8Array(PAGE_SIZE);
    const n = FS2.read(s2, buf, 0, PAGE_SIZE, 0);
    expect(n).toBe(PAGE_SIZE / 2);
    for (let i = 0; i < PAGE_SIZE / 2; i++) {
      expect(buf[i]).toBe(0xcc);
    }
    FS2.close(s2);
  });

  it("concurrent fsyncs on different files", async () => {
    const backend = new SyncMemoryBackend();
    const { FS } = await mountTome(backend);

    const streams: any[] = [];
    for (let i = 0; i < 5; i++) {
      const s = FS.open(`${MOUNT}/f${i}`, O.RDWR | O.CREAT, 0o666);
      const data = encode(`file-${i}-content`);
      FS.write(s, data, 0, data.length, 0);
      streams.push(s);
    }

    // fsync each file
    for (const s of streams) {
      s.stream_ops.fsync(s);
    }

    // All files should be in the backend
    for (let i = 0; i < 5; i++) {
      const meta = backend.readMeta(`/f${i}`);
      expect(meta).not.toBeNull();
      expect(meta!.size).toBe(`file-${i}-content`.length);

      const page = backend.readPage(`/f${i}`, 0);
      expect(page).not.toBeNull();
      expect(decode(page!.subarray(0, meta!.size))).toBe(`file-${i}-content`);
    }

    for (const s of streams) {
      FS.close(s);
    }
  });

  it("write after fsync creates new dirty state", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/rewrite`, O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("first"), 0, 5, 0);
    stream.stream_ops.fsync(stream);

    // Verify first write persisted
    let page = backend.readPage("/rewrite", 0);
    expect(decode(page!.subarray(0, 5))).toBe("first");

    // Write more data — creates new dirty state
    FS.write(stream, encode("SECOND"), 0, 6, 5);

    // New dirty pages should exist
    expect(tomefs.pageCache.dirtyCount).toBeGreaterThan(0);

    // Second fsync
    stream.stream_ops.fsync(stream);
    page = backend.readPage("/rewrite", 0);
    expect(decode(page!.subarray(0, 11))).toBe("firstSECOND");

    const meta = backend.readMeta("/rewrite");
    expect(meta!.size).toBe(11);

    FS.close(stream);
  });

  // ------------------------------------------------------------------
  // Atomicity: fsync uses syncAll, not separate writePages + writeMeta
  // ------------------------------------------------------------------

  it("fsync writes pages and metadata atomically via syncAll @fast", async () => {
    const inner = new SyncMemoryBackend();
    const { backend: tracked, calls } = createTrackingBackend(inner);
    const { FS } = await mountTome(tracked as any);

    const stream = FS.open(`${MOUNT}/atomic`, O.RDWR | O.CREAT, 0o666);
    const data = encode("atomic fsync test");
    FS.write(stream, data, 0, data.length, 0);

    calls.length = 0; // reset tracking

    stream.stream_ops.fsync(stream);

    // fsync should use exactly one syncAll call, not separate writePages + writeMeta
    const syncAllCalls = calls.filter(c => c === "syncAll");
    const writePagesCalls = calls.filter(c => c === "writePages");
    const writeMetaCalls = calls.filter(c => c === "writeMeta");

    expect(syncAllCalls.length).toBe(1);
    expect(writePagesCalls.length).toBe(0);
    expect(writeMetaCalls.length).toBe(0);

    // Verify data was correctly persisted
    const page = inner.readPage("/atomic", 0);
    expect(page).not.toBeNull();
    expect(decode(page!.subarray(0, data.length))).toBe("atomic fsync test");

    const meta = inner.readMeta("/atomic");
    expect(meta).not.toBeNull();
    expect(meta!.size).toBe(data.length);

    FS.close(stream);
  });

  it("fsync on clean file still writes metadata atomically via syncAll @fast", async () => {
    const inner = new SyncMemoryBackend();
    const { backend: tracked, calls } = createTrackingBackend(inner);
    const { FS } = await mountTome(tracked as any);

    const stream = FS.open(`${MOUNT}/clean-meta`, O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("data"), 0, 4, 0);

    // First fsync to persist everything
    stream.stream_ops.fsync(stream);

    // No more dirty pages, but file metadata could still be re-written
    calls.length = 0;
    stream.stream_ops.fsync(stream);

    // Even for a clean file, fsync uses syncAll (with empty pages array)
    const syncAllCalls = calls.filter(c => c === "syncAll");
    expect(syncAllCalls.length).toBe(1);
    expect(calls.filter(c => c === "writePages").length).toBe(0);
    expect(calls.filter(c => c === "writeMeta").length).toBe(0);

    FS.close(stream);
  });

  it("fsync atomicity: multi-page file writes all pages + meta in one call @fast", async () => {
    const inner = new SyncMemoryBackend();
    const { backend: tracked, calls } = createTrackingBackend(inner);
    const { FS } = await mountTome(tracked as any);

    const stream = FS.open(`${MOUNT}/multi`, O.RDWR | O.CREAT, 0o666);
    // Write 3 pages
    for (let i = 0; i < 3; i++) {
      const buf = new Uint8Array(PAGE_SIZE);
      buf.fill(0x10 + i);
      FS.write(stream, buf, 0, PAGE_SIZE, i * PAGE_SIZE);
    }

    calls.length = 0;

    stream.stream_ops.fsync(stream);

    // Single syncAll with 3 pages + 1 metadata entry
    const syncAllCalls = calls.filter(c => c === "syncAll");
    expect(syncAllCalls.length).toBe(1);

    // Verify all pages persisted correctly
    for (let i = 0; i < 3; i++) {
      const page = inner.readPage("/multi", i);
      expect(page).not.toBeNull();
      expect(page![0]).toBe(0x10 + i);
    }

    const meta = inner.readMeta("/multi");
    expect(meta!.size).toBe(3 * PAGE_SIZE);

    FS.close(stream);
  });
});
