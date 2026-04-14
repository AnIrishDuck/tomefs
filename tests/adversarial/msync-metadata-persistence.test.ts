/**
 * Adversarial tests: msync metadata persistence.
 *
 * msync must update mtime/ctime and mark metadata dirty, matching MEMFS
 * behavior (where msync delegates to stream_ops.write which updates
 * timestamps). Without this, incremental syncfs won't persist metadata
 * for files modified only via msync — causing stale mtime and potentially
 * stale file sizes after crash + remount.
 *
 * These tests target the seam between msync and the incremental syncfs
 * path (ethos §9). The bug: msync called writePages directly, bypassing
 * the timestamp update and markMetaDirty call that stream_ops.write does.
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
} as const;

const MOUNT = "/tome";

async function mountTome(backend: SyncMemoryBackend, maxPages = 4096) {
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

function syncfs(FS: any, tomefs: any) {
  tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
    if (err) throw err;
  });
}

function syncAndUnmount(FS: any, tomefs: any) {
  syncfs(FS, tomefs);
  FS.unmount(MOUNT);
}

describe("adversarial: msync metadata and timestamp persistence", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("msync updates mtime @fast", async () => {
    const { FS } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/mtime`, O.RDWR | O.CREAT, 0o666);
    const data = new Uint8Array(256).fill(0xaa);
    FS.write(stream, data, 0, 256);

    // Record mtime after write
    const stat1 = FS.stat(`${MOUNT}/mtime`);
    const mtimeAfterWrite = new Date(stat1.mtime).getTime();

    // Wait a tick so Date.now() advances
    const start = Date.now();
    while (Date.now() === start) { /* spin */ }

    // msync should update mtime
    const buf = new Uint8Array([0xbb, 0xcc, 0xdd, 0xee]);
    stream.stream_ops.msync(stream, buf, 100, 4, 0);

    const stat2 = FS.stat(`${MOUNT}/mtime`);
    const mtimeAfterMsync = new Date(stat2.mtime).getTime();

    expect(mtimeAfterMsync).toBeGreaterThan(mtimeAfterWrite);

    FS.close(stream);
  });

  it("msync updates ctime @fast", async () => {
    const { FS } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/ctime`, O.RDWR | O.CREAT, 0o666);
    FS.write(stream, new Uint8Array(256).fill(0x11), 0, 256);

    const stat1 = FS.stat(`${MOUNT}/ctime`);
    const ctimeAfterWrite = new Date(stat1.ctime).getTime();

    const start = Date.now();
    while (Date.now() === start) { /* spin */ }

    stream.stream_ops.msync(stream, new Uint8Array([0x22]), 0, 1, 0);

    const stat2 = FS.stat(`${MOUNT}/ctime`);
    const ctimeAfterMsync = new Date(stat2.ctime).getTime();

    expect(ctimeAfterMsync).toBeGreaterThan(ctimeAfterWrite);

    FS.close(stream);
  });

  it("msync-only writes persist metadata after incremental syncfs + remount @fast", async () => {
    // Phase 1: create file, initial syncfs to clear dirty flags
    const { FS, tomefs } = await mountTome(backend);
    const stream = FS.open(`${MOUNT}/persist`, O.RDWR | O.CREAT, 0o666);
    const original = new Uint8Array(PAGE_SIZE).fill(0xaa);
    FS.write(stream, original, 0, PAGE_SIZE);
    FS.close(stream);

    // syncfs to persist and clear all dirty flags
    syncfs(FS, tomefs);

    // Phase 2: modify ONLY via msync — no stream_ops.write
    const s2 = FS.open(`${MOUNT}/persist`, O.RDWR);
    const result = s2.stream_ops.mmap(s2, PAGE_SIZE, 0, 3, 1);
    result.ptr.fill(0xbb);
    s2.stream_ops.msync(s2, result.ptr, 0, PAGE_SIZE, 0);
    FS.close(s2);

    // Incremental syncfs — must persist the msync'd data and metadata
    syncAndUnmount(FS, tomefs);

    // Phase 3: remount and verify data survived
    const { FS: FS2 } = await mountTome(backend);
    const readBuf = new Uint8Array(PAGE_SIZE);
    const s3 = FS2.open(`${MOUNT}/persist`, O.RDONLY);
    FS2.read(s3, readBuf, 0, PAGE_SIZE);
    FS2.close(s3);

    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(readBuf[i]).toBe(0xbb);
    }
  });

  it("msync-only writes persist file size after incremental syncfs + remount", async () => {
    // Create a small file, initial syncfs
    const { FS, tomefs } = await mountTome(backend);
    const stream = FS.open(`${MOUNT}/size`, O.RDWR | O.CREAT, 0o666);
    const original = new Uint8Array(100).fill(0x11);
    FS.write(stream, original, 0, 100);
    FS.close(stream);

    syncfs(FS, tomefs);

    // Verify initial size is persisted
    const stat1 = FS.stat(`${MOUNT}/size`);
    expect(stat1.size).toBe(100);

    // msync a region that doesn't extend the file
    const s2 = FS.open(`${MOUNT}/size`, O.RDWR);
    const buf = new Uint8Array([0x22, 0x33, 0x44, 0x55]);
    s2.stream_ops.msync(s2, buf, 50, 4, 0);
    FS.close(s2);

    // Incremental syncfs
    syncAndUnmount(FS, tomefs);

    // Remount: size must still be 100 (metadata was re-persisted)
    const { FS: FS2 } = await mountTome(backend);
    const stat2 = FS2.stat(`${MOUNT}/size`);
    expect(stat2.size).toBe(100);

    // Verify the msync'd bytes survived
    const readBuf = new Uint8Array(100);
    const s3 = FS2.open(`${MOUNT}/size`, O.RDONLY);
    FS2.read(s3, readBuf, 0, 100);
    FS2.close(s3);

    expect(readBuf[50]).toBe(0x22);
    expect(readBuf[51]).toBe(0x33);
    expect(readBuf[52]).toBe(0x44);
    expect(readBuf[53]).toBe(0x55);
    // Surrounding bytes unchanged
    expect(readBuf[49]).toBe(0x11);
    expect(readBuf[54]).toBe(0x11);
  });

  it("msync metadata persists with tiny cache under eviction pressure", async () => {
    // Use a tiny 4-page cache to force eviction
    const { FS, tomefs } = await mountTome(backend, 4);

    // Create a 3-page file, sync, clear dirty flags
    const stream = FS.open(`${MOUNT}/evict`, O.RDWR | O.CREAT, 0o666);
    const data = new Uint8Array(PAGE_SIZE * 3).fill(0x11);
    FS.write(stream, data, 0, data.length);
    FS.close(stream);
    syncfs(FS, tomefs);

    // msync page 1 — this alone should mark metadata dirty
    const s2 = FS.open(`${MOUNT}/evict`, O.RDWR);
    const result = s2.stream_ops.mmap(s2, PAGE_SIZE, PAGE_SIZE, 3, 1);
    result.ptr.fill(0x22);
    s2.stream_ops.msync(s2, result.ptr, PAGE_SIZE, PAGE_SIZE, 0);

    // Fill cache with other files to evict the msync'd page
    for (let f = 0; f < 4; f++) {
      const s = FS.open(`${MOUNT}/filler${f}`, O.RDWR | O.CREAT, 0o666);
      FS.write(s, new Uint8Array(PAGE_SIZE).fill(0xff), 0, PAGE_SIZE);
      FS.close(s);
    }

    FS.close(s2);
    syncAndUnmount(FS, tomefs);

    // Remount: msync'd data must survive eviction + syncfs
    const { FS: FS2 } = await mountTome(backend, 4);
    const readBuf = new Uint8Array(PAGE_SIZE * 3);
    const s3 = FS2.open(`${MOUNT}/evict`, O.RDONLY);
    FS2.read(s3, readBuf, 0, PAGE_SIZE * 3);
    FS2.close(s3);

    // Page 0: original 0x11
    expect(readBuf[0]).toBe(0x11);
    expect(readBuf[PAGE_SIZE - 1]).toBe(0x11);
    // Page 1: msync'd 0x22
    expect(readBuf[PAGE_SIZE]).toBe(0x22);
    expect(readBuf[PAGE_SIZE * 2 - 1]).toBe(0x22);
    // Page 2: original 0x11
    expect(readBuf[PAGE_SIZE * 2]).toBe(0x11);
    expect(readBuf[PAGE_SIZE * 3 - 1]).toBe(0x11);
  });

  it("msync marks metadata dirty for incremental syncfs path", async () => {
    // This test specifically targets the incremental syncfs path:
    // after an initial syncfs clears dirty flags, a subsequent msync
    // must re-mark metadata dirty so the next incremental syncfs
    // includes the metadata in its batch.
    const { FS, tomefs } = await mountTome(backend);

    // Create file and initial syncfs
    const stream = FS.open(`${MOUNT}/incr`, O.RDWR | O.CREAT, 0o666);
    FS.write(stream, new Uint8Array(PAGE_SIZE).fill(0xaa), 0, PAGE_SIZE);
    FS.close(stream);
    syncfs(FS, tomefs);

    // msync only — no write()
    const s2 = FS.open(`${MOUNT}/incr`, O.RDWR);
    const mmapResult = s2.stream_ops.mmap(s2, 100, 0, 3, 1);
    mmapResult.ptr.fill(0xbb);
    s2.stream_ops.msync(s2, mmapResult.ptr, 0, 100, 0);
    FS.close(s2);

    // Record mtime before syncfs
    const statBefore = FS.stat(`${MOUNT}/incr`);
    const mtimeBefore = new Date(statBefore.mtime).getTime();

    // Incremental syncfs
    syncAndUnmount(FS, tomefs);

    // Remount: verify mtime was persisted (not the old pre-msync value)
    const { FS: FS2 } = await mountTome(backend);
    const statAfter = FS2.stat(`${MOUNT}/incr`);
    const mtimeAfter = new Date(statAfter.mtime).getTime();

    expect(mtimeAfter).toBe(mtimeBefore);

    // Also verify the data
    const readBuf = new Uint8Array(PAGE_SIZE);
    const s3 = FS2.open(`${MOUNT}/incr`, O.RDONLY);
    FS2.read(s3, readBuf, 0, PAGE_SIZE);
    FS2.close(s3);

    // First 100 bytes: msync'd to 0xbb
    for (let i = 0; i < 100; i++) {
      expect(readBuf[i]).toBe(0xbb);
    }
    // Rest: original 0xaa
    for (let i = 100; i < PAGE_SIZE; i++) {
      expect(readBuf[i]).toBe(0xaa);
    }
  });
});
