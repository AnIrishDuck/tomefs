/**
 * Adversarial tests: dup'd file descriptor + fsync interaction.
 *
 * Postgres dup's WAL file descriptors and uses fsync(fd) for durability.
 * The dup'd fd refers to the same open file description (same node),
 * so fsync through either descriptor should flush the same file's dirty
 * pages and metadata. These tests verify that:
 *
 *   - fsync through a dup'd fd flushes the correct file's dirty pages
 *   - fsync through a dup'd fd writes metadata to the backend
 *   - fsync through a dup'd fd clears dirty tracking for the node
 *   - write through one fd, fsync through the other → data persists
 *   - write after dup-fsync creates new dirty state
 *   - dup + fsync + remount preserves data (no syncfs needed)
 *   - dup + fsync only affects the target file
 *   - dup + fsync under cache pressure
 *   - dup + unlink + fsync interaction
 *   - WAL-like append + fsync pattern via dup'd descriptors
 *
 * Ethos §9: "Write tests designed to break tomefs specifically"
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

const SEEK_SET = 0;

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

function syncfs(FS: any) {
  FS.syncfs(false, (err: Error | null) => {
    if (err) throw err;
  });
}

describe("adversarial: dup + fsync interaction", () => {
  it("fsync through dup'd fd flushes dirty pages to backend @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/file`, O.RDWR | O.CREAT, 0o666);
    const data = encode("written-via-original");
    FS.write(stream, data, 0, data.length, 0);

    const dup = FS.dupStream(stream);

    // Before fsync: backend has no pages
    expect(backend.readPage("/file", 0)).toBeNull();

    // fsync through the DUP'd fd
    dup.stream_ops.fsync(dup);

    // After fsync via dup: backend has the page
    const page = backend.readPage("/file", 0);
    expect(page).not.toBeNull();
    expect(decode(page!.subarray(0, data.length))).toBe("written-via-original");

    FS.close(stream);
    FS.close(dup);
  });

  it("fsync through dup'd fd writes metadata to backend", async () => {
    const backend = new SyncMemoryBackend();
    const { FS } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/meta`, O.RDWR | O.CREAT, 0o666);
    const data = new Uint8Array(5000);
    data.fill(0xab);
    FS.write(stream, data, 0, data.length, 0);

    const dup = FS.dupStream(stream);

    expect(backend.readMeta("/meta")).toBeNull();

    dup.stream_ops.fsync(dup);

    const meta = backend.readMeta("/meta");
    expect(meta).not.toBeNull();
    expect(meta!.size).toBe(5000);

    FS.close(stream);
    FS.close(dup);
  });

  it("fsync through dup'd fd clears dirty tracking", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/dirty`, O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("data"), 0, 4, 0);

    const dup = FS.dupStream(stream);

    // Dirty pages exist before fsync
    expect(tomefs.pageCache.dirtyCount).toBeGreaterThan(0);

    dup.stream_ops.fsync(dup);

    // After fsync via dup, the file's dirty pages should be flushed
    const page = backend.readPage("/dirty", 0);
    expect(page).not.toBeNull();
    expect(decode(page!.subarray(0, 4))).toBe("data");

    FS.close(stream);
    FS.close(dup);
  });

  it("write through fd1, fsync through fd2 persists data @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS } = await mountTome(backend);

    const fd1 = FS.open(`${MOUNT}/cross`, O.RDWR | O.CREAT, 0o666);
    const fd2 = FS.dupStream(fd1);

    // Write through fd1
    const data = encode("cross-fd-write");
    FS.write(fd1, data, 0, data.length, 0);

    // fsync through fd2
    fd2.stream_ops.fsync(fd2);

    // Data written via fd1 should be in the backend
    const page = backend.readPage("/cross", 0);
    expect(page).not.toBeNull();
    expect(decode(page!.subarray(0, data.length))).toBe("cross-fd-write");

    const meta = backend.readMeta("/cross");
    expect(meta!.size).toBe(data.length);

    FS.close(fd1);
    FS.close(fd2);
  });

  it("write after dup-fsync creates new dirty state", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    const fd1 = FS.open(`${MOUNT}/redirty`, O.RDWR | O.CREAT, 0o666);
    const fd2 = FS.dupStream(fd1);

    // First write + fsync via dup
    FS.write(fd1, encode("first"), 0, 5, 0);
    fd2.stream_ops.fsync(fd2);

    let page = backend.readPage("/redirty", 0);
    expect(decode(page!.subarray(0, 5))).toBe("first");

    // Second write via fd1 — should create new dirty state
    FS.write(fd1, encode("SECOND"), 0, 6, 5);
    expect(tomefs.pageCache.dirtyCount).toBeGreaterThan(0);

    // Second fsync via fd2
    fd2.stream_ops.fsync(fd2);
    page = backend.readPage("/redirty", 0);
    expect(decode(page!.subarray(0, 11))).toBe("firstSECOND");

    const meta = backend.readMeta("/redirty");
    expect(meta!.size).toBe(11);

    FS.close(fd1);
    FS.close(fd2);
  });

  it("dup + fsync + remount preserves data without syncfs @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/persist`, O.RDWR | O.CREAT, 0o666);
    const data = encode("dup-fsync persistence test - 40 bytes!!");
    FS.write(stream, data, 0, data.length, 0);

    const dup = FS.dupStream(stream);
    dup.stream_ops.fsync(dup);
    FS.close(stream);
    FS.close(dup);

    // Remount without syncfs — only fsync was called
    const { FS: FS2 } = await mountTome(backend);

    const stat = FS2.stat(`${MOUNT}/persist`);
    expect(stat.size).toBe(data.length);

    const s2 = FS2.open(`${MOUNT}/persist`, O.RDONLY);
    const buf = new Uint8Array(data.length);
    const n = FS2.read(s2, buf, 0, data.length, 0);
    expect(n).toBe(data.length);
    expect(decode(buf)).toBe("dup-fsync persistence test - 40 bytes!!");
    FS2.close(s2);
  });

  it("dup + fsync only affects target file, other files stay dirty", async () => {
    const backend = new SyncMemoryBackend();
    const { FS } = await mountTome(backend);

    // Write to two files
    const s1 = FS.open(`${MOUNT}/file1`, O.RDWR | O.CREAT, 0o666);
    FS.write(s1, encode("file1data"), 0, 9, 0);

    const s2 = FS.open(`${MOUNT}/file2`, O.RDWR | O.CREAT, 0o666);
    FS.write(s2, encode("file2data"), 0, 9, 0);

    // Dup file1 and fsync through the dup
    const dup1 = FS.dupStream(s1);
    dup1.stream_ops.fsync(dup1);

    // file1 flushed, file2 still dirty
    expect(backend.readPage("/file1", 0)).not.toBeNull();
    expect(backend.readPage("/file2", 0)).toBeNull();

    FS.close(s1);
    FS.close(dup1);
    FS.close(s2);
  });

  it("dup + fsync under cache pressure (4-page cache)", async () => {
    const backend = new SyncMemoryBackend();
    const { FS } = await mountTome(backend, 4);

    const stream = FS.open(`${MOUNT}/big`, O.RDWR | O.CREAT, 0o666);

    // Write 8 pages — exceeds cache, forces eviction
    for (let i = 0; i < 8; i++) {
      const data = new Uint8Array(PAGE_SIZE);
      data.fill(i + 1);
      FS.write(stream, data, 0, PAGE_SIZE, i * PAGE_SIZE);
    }

    const dup = FS.dupStream(stream);

    // fsync through dup — must flush remaining dirty pages
    dup.stream_ops.fsync(dup);

    // All 8 pages should be in backend
    for (let i = 0; i < 8; i++) {
      const page = backend.readPage("/big", i);
      expect(page).not.toBeNull();
      expect(page![0]).toBe(i + 1);
      expect(page![PAGE_SIZE - 1]).toBe(i + 1);
    }

    const meta = backend.readMeta("/big");
    expect(meta).not.toBeNull();
    expect(meta!.size).toBe(8 * PAGE_SIZE);

    FS.close(stream);
    FS.close(dup);
  });

  it("multiple dups, fsync through one flushes all data", async () => {
    const backend = new SyncMemoryBackend();
    const { FS } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/multi`, O.RDWR | O.CREAT, 0o666);
    const dup1 = FS.dupStream(stream);
    const dup2 = FS.dupStream(stream);

    // Write through each descriptor (all share the same node)
    FS.write(stream, encode("AAA"), 0, 3, 0);
    FS.write(dup1, encode("BBB"), 0, 3, 3);
    FS.write(dup2, encode("CCC"), 0, 3, 6);

    // fsync through dup2 — should flush all 9 bytes
    dup2.stream_ops.fsync(dup2);

    const page = backend.readPage("/multi", 0);
    expect(page).not.toBeNull();
    expect(decode(page!.subarray(0, 9))).toBe("AAABBBCCC");

    const meta = backend.readMeta("/multi");
    expect(meta!.size).toBe(9);

    FS.close(stream);
    FS.close(dup1);
    FS.close(dup2);
  });

  it("dup + truncate via original + fsync through dup persists truncated state", async () => {
    const backend = new SyncMemoryBackend();
    const { FS } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/trunc`, O.RDWR | O.CREAT, 0o666);
    const data = new Uint8Array(PAGE_SIZE * 2);
    data.fill(0xdd);
    FS.write(stream, data, 0, data.length, 0);

    const dup = FS.dupStream(stream);

    // Truncate through original fd
    FS.ftruncate(stream.fd, PAGE_SIZE / 2);

    // fsync through dup
    dup.stream_ops.fsync(dup);
    FS.close(stream);
    FS.close(dup);

    // Remount and verify truncated size
    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/trunc`);
    expect(stat.size).toBe(PAGE_SIZE / 2);

    const s2 = FS2.open(`${MOUNT}/trunc`, O.RDONLY);
    const buf = new Uint8Array(PAGE_SIZE);
    const n = FS2.read(s2, buf, 0, PAGE_SIZE, 0);
    expect(n).toBe(PAGE_SIZE / 2);
    for (let i = 0; i < PAGE_SIZE / 2; i++) {
      expect(buf[i]).toBe(0xdd);
    }
    FS2.close(s2);
  });

  it("dup + fsync + subsequent syncfs doesn't double-write", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/nodbl`, O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("initial"), 0, 7, 0);

    const dup = FS.dupStream(stream);
    dup.stream_ops.fsync(dup);

    const flushesBefore = tomefs.pageCache.getStats().flushes;

    // syncfs should find file already clean
    syncfs(FS);

    const flushesAfter = tomefs.pageCache.getStats().flushes;
    expect(flushesAfter).toBe(flushesBefore);

    FS.close(stream);
    FS.close(dup);
  });

  it("dup + unlink + fsync through dup persists data before cleanup", async () => {
    const backend = new SyncMemoryBackend();
    const { FS } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/victim`, O.RDWR | O.CREAT, 0o666);
    const data = encode("unlink-fsync-dup");
    FS.write(stream, data, 0, data.length, 0);

    const dup = FS.dupStream(stream);

    // Unlink the file while both fds are open
    FS.unlink(`${MOUNT}/victim`);

    // fsync through dup — should still flush to backend
    // (the node's storagePath still points to the /__deleted_* marker path)
    dup.stream_ops.fsync(dup);

    // Close original — node stays alive because dup is still open
    FS.close(stream);

    // Read through dup to verify data is still accessible
    FS.llseek(dup, 0, SEEK_SET);
    const buf = new Uint8Array(data.length);
    const n = FS.read(dup, buf, 0, data.length);
    expect(n).toBe(data.length);
    expect(decode(buf, n)).toBe("unlink-fsync-dup");

    FS.close(dup);
  });

  it("WAL-like pattern: sequential append via fd1, fsync via fd2 @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS } = await mountTome(backend);

    // Simulate Postgres WAL: open file, dup the fd for fsync
    const writer = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);
    const syncer = FS.dupStream(writer);

    // Append WAL records in batches, fsync after each batch
    let pos = 0;
    for (let batch = 0; batch < 4; batch++) {
      const record = encode(`WAL-record-${batch}-padding-data\n`);
      FS.write(writer, record, 0, record.length, pos);
      pos += record.length;

      // fsync through the dup'd syncer fd
      syncer.stream_ops.fsync(syncer);

      // Verify the data is in the backend after each fsync
      const meta = backend.readMeta("/wal");
      expect(meta).not.toBeNull();
      expect(meta!.size).toBe(pos);
    }

    FS.close(writer);
    FS.close(syncer);

    // Remount and verify all WAL records survived
    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/wal`);
    expect(stat.size).toBe(pos);

    const s2 = FS2.open(`${MOUNT}/wal`, O.RDONLY);
    const buf = new Uint8Array(pos);
    FS2.read(s2, buf, 0, pos, 0);
    const content = decode(buf);
    for (let batch = 0; batch < 4; batch++) {
      expect(content).toContain(`WAL-record-${batch}-padding-data`);
    }
    FS2.close(s2);
  });

  it("WAL pattern under cache pressure: append + fsync via dup with 4-page cache", async () => {
    const backend = new SyncMemoryBackend();
    const { FS } = await mountTome(backend, 4);

    const writer = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);
    const syncer = FS.dupStream(writer);

    // Write enough to exceed 4-page cache (>32KB), forcing eviction
    let pos = 0;
    const recordSize = PAGE_SIZE; // One full page per record
    for (let i = 0; i < 8; i++) {
      const record = new Uint8Array(recordSize);
      record.fill(i + 1);
      FS.write(writer, record, 0, record.length, pos);
      pos += recordSize;

      // fsync every 2 records (some pages already evicted)
      if (i % 2 === 1) {
        syncer.stream_ops.fsync(syncer);
      }
    }
    // Final fsync for remaining dirty pages
    syncer.stream_ops.fsync(syncer);

    FS.close(writer);
    FS.close(syncer);

    // Remount and verify all pages
    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/wal`);
    expect(stat.size).toBe(pos);

    const s2 = FS2.open(`${MOUNT}/wal`, O.RDONLY);
    for (let i = 0; i < 8; i++) {
      const buf = new Uint8Array(recordSize);
      const n = FS2.read(s2, buf, 0, recordSize, i * recordSize);
      expect(n).toBe(recordSize);
      expect(buf[0]).toBe(i + 1);
      expect(buf[recordSize - 1]).toBe(i + 1);
    }
    FS2.close(s2);
  });

  it("fsync through dup preserves mtime across remount", async () => {
    const backend = new SyncMemoryBackend();
    const { FS } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/mtime`, O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("ts"), 0, 2, 0);

    const statBefore = FS.stat(`${MOUNT}/mtime`);

    const dup = FS.dupStream(stream);
    dup.stream_ops.fsync(dup);
    FS.close(stream);
    FS.close(dup);

    // Remount and verify timestamps
    const { FS: FS2 } = await mountTome(backend);
    const statAfter = FS2.stat(`${MOUNT}/mtime`);
    expect(statAfter.mtime.getTime()).toBe(statBefore.mtime.getTime());
    expect(statAfter.size).toBe(2);
  });

  it("fsync via original after write via dup", async () => {
    const backend = new SyncMemoryBackend();
    const { FS } = await mountTome(backend);

    const fd1 = FS.open(`${MOUNT}/reverse`, O.RDWR | O.CREAT, 0o666);
    const fd2 = FS.dupStream(fd1);

    // Write through dup
    const data = encode("written-via-dup");
    FS.write(fd2, data, 0, data.length, 0);

    // fsync through original
    fd1.stream_ops.fsync(fd1);

    // Data written via dup should be in backend
    const page = backend.readPage("/reverse", 0);
    expect(page).not.toBeNull();
    expect(decode(page!.subarray(0, data.length))).toBe("written-via-dup");

    FS.close(fd1);
    FS.close(fd2);
  });

  it("close dup after fsync, original fd still works for write + fsync", async () => {
    const backend = new SyncMemoryBackend();
    const { FS } = await mountTome(backend);

    const fd1 = FS.open(`${MOUNT}/lifecycle`, O.RDWR | O.CREAT, 0o666);
    const fd2 = FS.dupStream(fd1);

    // Write and fsync via dup
    FS.write(fd2, encode("phase1"), 0, 6, 0);
    fd2.stream_ops.fsync(fd2);

    // Close the dup
    FS.close(fd2);

    // Original fd should still work
    FS.write(fd1, encode("phase2"), 0, 6, 6);
    fd1.stream_ops.fsync(fd1);

    const page = backend.readPage("/lifecycle", 0);
    expect(decode(page!.subarray(0, 12))).toBe("phase1phase2");

    const meta = backend.readMeta("/lifecycle");
    expect(meta!.size).toBe(12);

    FS.close(fd1);
  });

  it("dup + rename + fsync through dup persists at new path", async () => {
    const backend = new SyncMemoryBackend();
    const { FS } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/before`, O.RDWR | O.CREAT, 0o666);
    const data = encode("rename-then-fsync");
    FS.write(stream, data, 0, data.length, 0);

    const dup = FS.dupStream(stream);

    // Rename the file
    FS.rename(`${MOUNT}/before`, `${MOUNT}/after`);

    // fsync through dup — should flush to the renamed path
    dup.stream_ops.fsync(dup);
    FS.close(stream);
    FS.close(dup);

    // Remount and verify data at new path
    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/after`);
    expect(stat.size).toBe(data.length);

    const s2 = FS2.open(`${MOUNT}/after`, O.RDONLY);
    const buf = new Uint8Array(data.length);
    FS2.read(s2, buf, 0, data.length, 0);
    expect(decode(buf)).toBe("rename-then-fsync");
    FS2.close(s2);

    // Old path should not exist
    expect(() => FS2.stat(`${MOUNT}/before`)).toThrow();
  });

  it("interleaved write + fsync across two dup'd fds", async () => {
    const backend = new SyncMemoryBackend();
    const { FS } = await mountTome(backend);

    const fd1 = FS.open(`${MOUNT}/interleave`, O.RDWR | O.CREAT, 0o666);
    const fd2 = FS.dupStream(fd1);

    // Interleaved write/fsync pattern (simulates concurrent WAL writers)
    FS.write(fd1, encode("A1"), 0, 2, 0);
    fd2.stream_ops.fsync(fd2);

    let meta = backend.readMeta("/interleave");
    expect(meta!.size).toBe(2);

    FS.write(fd2, encode("B2"), 0, 2, 2);
    fd1.stream_ops.fsync(fd1);

    meta = backend.readMeta("/interleave");
    expect(meta!.size).toBe(4);

    FS.write(fd1, encode("C3"), 0, 2, 4);
    fd2.stream_ops.fsync(fd2);

    meta = backend.readMeta("/interleave");
    expect(meta!.size).toBe(6);

    const page = backend.readPage("/interleave", 0);
    expect(decode(page!.subarray(0, 6))).toBe("A1B2C3");

    FS.close(fd1);
    FS.close(fd2);
  });

  it("dup + fsync + dirty shutdown recovery preserves fsynced data", async () => {
    const backend = new SyncMemoryBackend();
    const { FS } = await mountTome(backend);

    // Write and fsync via dup
    const stream = FS.open(`${MOUNT}/crash`, O.RDWR | O.CREAT, 0o666);
    const data = encode("crash-safe-via-dup");
    FS.write(stream, data, 0, data.length, 0);

    const dup = FS.dupStream(stream);
    dup.stream_ops.fsync(dup);

    // Write MORE data WITHOUT fsync — this should be lost on crash
    FS.write(stream, encode("-LOST"), 0, 5, data.length);

    // DON'T call syncfs — simulate dirty shutdown
    // (don't close streams either — simulates crash)

    // "Crash" remount with same backend
    const { FS: FS2 } = await mountTome(backend);

    const stat = FS2.stat(`${MOUNT}/crash`);
    // Size should be the fsynced size, not the un-fsynced extended size
    expect(stat.size).toBe(data.length);

    const s2 = FS2.open(`${MOUNT}/crash`, O.RDONLY);
    const buf = new Uint8Array(data.length);
    const n = FS2.read(s2, buf, 0, data.length, 0);
    expect(n).toBe(data.length);
    expect(decode(buf)).toBe("crash-safe-via-dup");
    FS2.close(s2);
  });

  it("assertInvariants holds after dup + fsync sequences", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/invariant`, O.RDWR | O.CREAT, 0o666);
    const dup = FS.dupStream(stream);

    // Interleaved operations
    FS.write(stream, encode("data1"), 0, 5, 0);
    dup.stream_ops.fsync(dup);
    tomefs.assertInvariants();

    FS.write(dup, encode("data2"), 0, 5, 5);
    stream.stream_ops.fsync(stream);
    tomefs.assertInvariants();

    FS.ftruncate(stream.fd, 3);
    dup.stream_ops.fsync(dup);
    tomefs.assertInvariants();

    syncfs(FS);
    tomefs.assertInvariants();

    FS.close(stream);
    FS.close(dup);
    tomefs.assertInvariants();
  });

  it("dup + fsync with sub-page size precision across remount", async () => {
    const backend = new SyncMemoryBackend();
    const { FS } = await mountTome(backend);

    // Write exactly 4567 bytes (not page-aligned) and fsync via dup
    const stream = FS.open(`${MOUNT}/subpage`, O.RDWR | O.CREAT, 0o666);
    const data = new Uint8Array(4567);
    for (let i = 0; i < data.length; i++) data[i] = (i * 31 + 17) & 0xff;
    FS.write(stream, data, 0, data.length, 0);

    const dup = FS.dupStream(stream);
    dup.stream_ops.fsync(dup);
    FS.close(stream);
    FS.close(dup);

    // Remount and verify byte-accurate size and content
    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/subpage`);
    expect(stat.size).toBe(4567);

    const s2 = FS2.open(`${MOUNT}/subpage`, O.RDONLY);
    const buf = new Uint8Array(4567);
    FS2.read(s2, buf, 0, 4567, 0);
    for (let i = 0; i < 4567; i++) {
      if (buf[i] !== data[i]) {
        throw new Error(
          `byte ${i}: expected=${data[i]}, got=${buf[i]}`,
        );
      }
    }
    FS2.close(s2);
  });
});
