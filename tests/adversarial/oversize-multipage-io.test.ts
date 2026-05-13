/**
 * Adversarial tests: multi-page I/O that exceeds cache capacity.
 *
 * When a single read or write spans more pages than the cache can hold,
 * the page cache's batch eviction and preloading paths hit edge cases:
 *
 * - batchEvict(count) where count > maxPages: must evict ALL cached pages
 * - Batch preloading of pages that immediately get evicted by later preloads
 *   within the same batch (because the cache overflows mid-preload)
 * - The write loop falls back to getPageInternal for pages that were preloaded
 *   then evicted, triggering individual cache misses
 * - Dirty pages from the write may be evicted before syncfs, requiring
 *   eviction-triggered flushes to preserve data
 *
 * These tests run under a 2-page cache (the minimum where batchEvict has
 * the multi-page code path) and verify data integrity + persistence.
 *
 * Ethos §9: adversarial differential testing — target the seams.
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

function fillBuf(size: number, seed: number): Uint8Array {
  const buf = new Uint8Array(size);
  for (let i = 0; i < size; i++) {
    buf[i] = (seed + i * 37) & 0xff;
  }
  return buf;
}

function verifyBuf(
  actual: Uint8Array,
  length: number,
  seed: number,
): boolean {
  for (let i = 0; i < length; i++) {
    if (actual[i] !== ((seed + i * 37) & 0xff)) return false;
  }
  return true;
}

async function createTestFS(
  maxPages: number,
  existingBackend?: SyncMemoryBackend,
) {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  const Module = await createModule();
  const rawFS = Module.FS;
  const backend = existingBackend ?? new SyncMemoryBackend();
  const tomefs = createTomeFS(rawFS, { backend, maxPages });
  rawFS.mkdir(MOUNT);
  rawFS.mount(tomefs, {}, MOUNT);
  return { FS: rawFS, backend, tomefs };
}

function syncfs(FS: any): void {
  let err: Error | null = null;
  FS.syncfs(false, (e: Error | null) => {
    err = e;
  });
  if (err) throw err;
}

describe("adversarial: oversize multi-page I/O (2-page cache)", () => {
  let FS: any;
  let backend: SyncMemoryBackend;
  let tomefs: any;

  beforeEach(async () => {
    ({ FS, backend, tomefs } = await createTestFS(2));
  });

  it("single write spanning 4 pages into 2-page cache @fast", () => {
    const path = MOUNT + "/oversize_write";
    const stream = FS.open(path, O.RDWR | O.CREAT, 0o666);

    const data = fillBuf(PAGE_SIZE * 4, 42);
    FS.write(stream, data, 0, data.length);

    FS.llseek(stream, 0, 0);
    const readBuf = new Uint8Array(data.length);
    const n = FS.read(stream, readBuf, 0, data.length);
    expect(n).toBe(data.length);
    expect(readBuf).toEqual(data);

    FS.close(stream);
  });

  it("single read spanning 4 pages from 2-page cache @fast", () => {
    const path = MOUNT + "/oversize_read";
    const stream = FS.open(path, O.RDWR | O.CREAT, 0o666);

    for (let p = 0; p < 4; p++) {
      const page = fillBuf(PAGE_SIZE, p + 1);
      FS.write(stream, page, 0, PAGE_SIZE, p * PAGE_SIZE);
    }

    const readBuf = new Uint8Array(PAGE_SIZE * 4);
    const n = FS.read(stream, readBuf, 0, PAGE_SIZE * 4, 0);
    expect(n).toBe(PAGE_SIZE * 4);

    for (let p = 0; p < 4; p++) {
      const expected = fillBuf(PAGE_SIZE, p + 1);
      expect(readBuf.subarray(p * PAGE_SIZE, (p + 1) * PAGE_SIZE)).toEqual(
        expected,
      );
    }

    FS.close(stream);
  });

  it("oversize write persists through syncfs + remount @fast", async () => {
    const path = MOUNT + "/oversize_persist";
    const stream = FS.open(path, O.RDWR | O.CREAT, 0o666);

    const data = fillBuf(PAGE_SIZE * 6, 77);
    FS.write(stream, data, 0, data.length);
    FS.close(stream);

    syncfs(FS);

    const { FS: FS2 } = await createTestFS(2, backend);
    const stream2 = FS2.open(MOUNT + "/oversize_persist", O.RDONLY);
    const readBuf = new Uint8Array(data.length);
    const n = FS2.read(stream2, readBuf, 0, data.length);
    expect(n).toBe(data.length);
    expect(readBuf).toEqual(data);
    FS2.close(stream2);
  });

  it("oversize write then oversize read on same fd @fast", () => {
    const path = MOUNT + "/oversize_rw";
    const stream = FS.open(path, O.RDWR | O.CREAT, 0o666);

    const size = PAGE_SIZE * 5;
    const data = fillBuf(size, 33);
    FS.write(stream, data, 0, size);

    FS.llseek(stream, 0, 0);
    const readBuf = new Uint8Array(size);
    const n = FS.read(stream, readBuf, 0, size);
    expect(n).toBe(size);
    expect(readBuf).toEqual(data);

    FS.close(stream);
  });

  it("oversize write at non-page-aligned offset @fast", () => {
    const path = MOUNT + "/oversize_unaligned";
    const stream = FS.open(path, O.RDWR | O.CREAT, 0o666);

    FS.write(stream, new Uint8Array(PAGE_SIZE * 6), 0, PAGE_SIZE * 6);

    const offset = PAGE_SIZE - 100;
    const size = PAGE_SIZE * 3 + 200;
    const data = fillBuf(size, 55);
    FS.write(stream, data, 0, size, offset);

    const readBuf = new Uint8Array(size);
    const n = FS.read(stream, readBuf, 0, size, offset);
    expect(n).toBe(size);
    expect(readBuf).toEqual(data);

    FS.close(stream);
  });

  it("interleaved oversize writes to two files @fast", () => {
    const s1 = FS.open(MOUNT + "/oversize_a", O.RDWR | O.CREAT, 0o666);
    const s2 = FS.open(MOUNT + "/oversize_b", O.RDWR | O.CREAT, 0o666);

    const size = PAGE_SIZE * 3;
    const d1 = fillBuf(size, 10);
    const d2 = fillBuf(size, 20);

    FS.write(s1, d1, 0, size);
    FS.write(s2, d2, 0, size);

    FS.llseek(s1, 0, 0);
    FS.llseek(s2, 0, 0);

    const r1 = new Uint8Array(size);
    const r2 = new Uint8Array(size);
    expect(FS.read(s1, r1, 0, size)).toBe(size);
    expect(FS.read(s2, r2, 0, size)).toBe(size);
    expect(r1).toEqual(d1);
    expect(r2).toEqual(d2);

    FS.close(s1);
    FS.close(s2);
  });

  it("oversize write extending file beyond current size @fast", () => {
    const path = MOUNT + "/oversize_extend";
    const stream = FS.open(path, O.RDWR | O.CREAT, 0o666);

    FS.write(stream, fillBuf(100, 1), 0, 100);

    const size = PAGE_SIZE * 4;
    const data = fillBuf(size, 88);
    FS.write(stream, data, 0, size, PAGE_SIZE);

    expect(FS.fstat(stream.fd).size).toBe(PAGE_SIZE + size);

    const header = new Uint8Array(100);
    FS.read(stream, header, 0, 100, 0);
    expect(verifyBuf(header, 100, 1)).toBe(true);

    const gap = new Uint8Array(PAGE_SIZE - 100);
    FS.read(stream, gap, 0, PAGE_SIZE - 100, 100);
    for (let i = 0; i < gap.length; i++) expect(gap[i]).toBe(0);

    const body = new Uint8Array(size);
    FS.read(stream, body, 0, size, PAGE_SIZE);
    expect(body).toEqual(data);

    FS.close(stream);
  });

  it("oversize write overwriting fully-dirty cache @fast", () => {
    const path = MOUNT + "/oversize_dirty_evict";
    const stream = FS.open(path, O.RDWR | O.CREAT, 0o666);

    FS.write(stream, fillBuf(PAGE_SIZE, 1), 0, PAGE_SIZE, 0);
    FS.write(stream, fillBuf(PAGE_SIZE, 2), 0, PAGE_SIZE, PAGE_SIZE);

    const size = PAGE_SIZE * 4;
    const data = fillBuf(size, 99);
    FS.write(stream, data, 0, size, 2 * PAGE_SIZE);

    expect(FS.fstat(stream.fd).size).toBe(6 * PAGE_SIZE);

    const p0 = new Uint8Array(PAGE_SIZE);
    FS.read(stream, p0, 0, PAGE_SIZE, 0);
    expect(verifyBuf(p0, PAGE_SIZE, 1)).toBe(true);

    const p1 = new Uint8Array(PAGE_SIZE);
    FS.read(stream, p1, 0, PAGE_SIZE, PAGE_SIZE);
    expect(verifyBuf(p1, PAGE_SIZE, 2)).toBe(true);

    const rest = new Uint8Array(size);
    FS.read(stream, rest, 0, size, 2 * PAGE_SIZE);
    expect(rest).toEqual(data);

    FS.close(stream);
  });

  it("oversize read after page-by-page writes @fast", () => {
    const path = MOUNT + "/oversize_read_after_paged";
    const stream = FS.open(path, O.RDWR | O.CREAT, 0o666);

    const numPages = 8;
    for (let p = 0; p < numPages; p++) {
      FS.write(stream, fillBuf(PAGE_SIZE, p * 10), 0, PAGE_SIZE, p * PAGE_SIZE);
    }

    const totalSize = numPages * PAGE_SIZE;
    const readBuf = new Uint8Array(totalSize);
    const n = FS.read(stream, readBuf, 0, totalSize, 0);
    expect(n).toBe(totalSize);

    for (let p = 0; p < numPages; p++) {
      const slice = readBuf.subarray(p * PAGE_SIZE, (p + 1) * PAGE_SIZE);
      expect(verifyBuf(slice, PAGE_SIZE, p * 10)).toBe(true);
    }

    FS.close(stream);
  });

  it("oversize write + syncfs + oversize read round-trip @fast", async () => {
    const path = MOUNT + "/oversize_roundtrip";
    const stream = FS.open(path, O.RDWR | O.CREAT, 0o666);

    const size = PAGE_SIZE * 8;
    const data = fillBuf(size, 44);
    FS.write(stream, data, 0, size);
    FS.close(stream);

    syncfs(FS);

    const { FS: FS2 } = await createTestFS(2, backend);
    const s2 = FS2.open(MOUNT + "/oversize_roundtrip", O.RDONLY);
    const readBuf = new Uint8Array(size);
    const n = FS2.read(s2, readBuf, 0, size);
    expect(n).toBe(size);
    expect(readBuf).toEqual(data);
    FS2.close(s2);
  });

  it("multiple oversize writes to same file accumulate correctly @fast", () => {
    const path = MOUNT + "/oversize_accum";
    const stream = FS.open(path, O.RDWR | O.CREAT, 0o666);

    const chunk = PAGE_SIZE * 3;
    const d1 = fillBuf(chunk, 10);
    const d2 = fillBuf(chunk, 20);
    const d3 = fillBuf(chunk, 30);

    FS.write(stream, d1, 0, chunk, 0);
    FS.write(stream, d2, 0, chunk, chunk);
    FS.write(stream, d3, 0, chunk, chunk * 2);

    expect(FS.fstat(stream.fd).size).toBe(chunk * 3);

    for (let i = 0; i < 3; i++) {
      const expected = fillBuf(chunk, (i + 1) * 10);
      const readBuf = new Uint8Array(chunk);
      FS.read(stream, readBuf, 0, chunk, i * chunk);
      expect(readBuf).toEqual(expected);
    }

    FS.close(stream);
  });

  it("oversize write then truncate then oversize write @fast", () => {
    const path = MOUNT + "/oversize_trunc_rewrite";
    const stream = FS.open(path, O.RDWR | O.CREAT, 0o666);

    const size = PAGE_SIZE * 4;
    FS.write(stream, fillBuf(size, 11), 0, size);

    FS.ftruncate(stream.fd, PAGE_SIZE);
    expect(FS.fstat(stream.fd).size).toBe(PAGE_SIZE);

    const newData = fillBuf(size, 22);
    FS.write(stream, newData, 0, size, 0);
    expect(FS.fstat(stream.fd).size).toBe(size);

    FS.llseek(stream, 0, 0);
    const readBuf = new Uint8Array(size);
    FS.read(stream, readBuf, 0, size);
    expect(readBuf).toEqual(newData);

    FS.close(stream);
  });

  it("oversize cross-page boundary write preserving adjacent data @fast", () => {
    const path = MOUNT + "/oversize_cross_preserve";
    const stream = FS.open(path, O.RDWR | O.CREAT, 0o666);

    const totalPages = 6;
    for (let p = 0; p < totalPages; p++) {
      FS.write(stream, fillBuf(PAGE_SIZE, p + 1), 0, PAGE_SIZE, p * PAGE_SIZE);
    }

    const writeOffset = PAGE_SIZE + 100;
    const writeSize = PAGE_SIZE * 3;
    const writeData = fillBuf(writeSize, 99);
    FS.write(stream, writeData, 0, writeSize, writeOffset);

    const p0 = new Uint8Array(PAGE_SIZE);
    FS.read(stream, p0, 0, PAGE_SIZE, 0);
    expect(verifyBuf(p0, PAGE_SIZE, 1)).toBe(true);

    const head = new Uint8Array(100);
    FS.read(stream, head, 0, 100, PAGE_SIZE);
    expect(verifyBuf(head, 100, 2)).toBe(true);

    const written = new Uint8Array(writeSize);
    FS.read(stream, written, 0, writeSize, writeOffset);
    expect(written).toEqual(writeData);

    const tailStart = writeOffset + writeSize;
    const tailSize = totalPages * PAGE_SIZE - tailStart;
    if (tailSize > 0) {
      const tail = new Uint8Array(tailSize);
      FS.read(stream, tail, 0, tailSize, tailStart);
      const p4Start = 4 * PAGE_SIZE;
      if (tailStart < p4Start) {
        const beforeP4 = p4Start - tailStart;
        const expectedP3 = fillBuf(PAGE_SIZE, 4);
        const p3Offset = tailStart - 3 * PAGE_SIZE;
        expect(tail.subarray(0, beforeP4)).toEqual(
          expectedP3.subarray(p3Offset, p3Offset + beforeP4),
        );
      }
    }

    FS.close(stream);
  });

  it("oversize persistence with interleaved files @fast", async () => {
    const s1 = FS.open(MOUNT + "/ov_persist_a", O.RDWR | O.CREAT, 0o666);
    const s2 = FS.open(MOUNT + "/ov_persist_b", O.RDWR | O.CREAT, 0o666);

    const size = PAGE_SIZE * 4;
    const d1 = fillBuf(size, 10);
    const d2 = fillBuf(size, 20);

    FS.write(s1, d1, 0, size);
    FS.write(s2, d2, 0, size);
    FS.close(s1);
    FS.close(s2);

    syncfs(FS);

    const { FS: FS2 } = await createTestFS(2, backend);
    for (const [name, expected] of [
      ["ov_persist_a", d1],
      ["ov_persist_b", d2],
    ] as const) {
      const s = FS2.open(MOUNT + "/" + name, O.RDONLY);
      const buf = new Uint8Array(size);
      expect(FS2.read(s, buf, 0, size)).toBe(size);
      expect(buf).toEqual(expected);
      FS2.close(s);
    }
  });

  it("cache stats reflect oversize eviction behavior", () => {
    const path = MOUNT + "/oversize_stats";
    const stream = FS.open(path, O.RDWR | O.CREAT, 0o666);

    tomefs.pageCache.resetStats();

    const size = PAGE_SIZE * 4;
    FS.write(stream, fillBuf(size, 1), 0, size);

    const stats = tomefs.pageCache.getStats();
    expect(stats.evictions).toBeGreaterThan(0);
    expect(stats.misses).toBeGreaterThan(0);

    FS.close(stream);
  });
});

describe("adversarial: oversize multi-page I/O (1-page cache)", () => {
  let FS: any;
  let backend: SyncMemoryBackend;

  beforeEach(async () => {
    ({ FS, backend } = await createTestFS(1));
  });

  it("write and read 4 pages with 1-page cache @fast", () => {
    const path = MOUNT + "/min_cache_rw";
    const stream = FS.open(path, O.RDWR | O.CREAT, 0o666);

    for (let p = 0; p < 4; p++) {
      FS.write(stream, fillBuf(PAGE_SIZE, p + 1), 0, PAGE_SIZE, p * PAGE_SIZE);
    }

    for (let p = 0; p < 4; p++) {
      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(stream, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expect(verifyBuf(buf, PAGE_SIZE, p + 1)).toBe(true);
    }

    FS.close(stream);
  });

  it("oversize multi-page read with 1-page cache @fast", () => {
    const path = MOUNT + "/min_cache_multiread";
    const stream = FS.open(path, O.RDWR | O.CREAT, 0o666);

    const numPages = 6;
    for (let p = 0; p < numPages; p++) {
      FS.write(stream, fillBuf(PAGE_SIZE, p), 0, PAGE_SIZE, p * PAGE_SIZE);
    }

    const totalSize = numPages * PAGE_SIZE;
    const readBuf = new Uint8Array(totalSize);
    const n = FS.read(stream, readBuf, 0, totalSize, 0);
    expect(n).toBe(totalSize);

    for (let p = 0; p < numPages; p++) {
      const slice = readBuf.subarray(p * PAGE_SIZE, (p + 1) * PAGE_SIZE);
      expect(verifyBuf(slice, PAGE_SIZE, p)).toBe(true);
    }

    FS.close(stream);
  });

  it("oversize multi-page write with 1-page cache @fast", () => {
    const path = MOUNT + "/min_cache_multiwrite";
    const stream = FS.open(path, O.RDWR | O.CREAT, 0o666);

    const size = PAGE_SIZE * 5;
    const data = fillBuf(size, 66);
    FS.write(stream, data, 0, size);

    FS.llseek(stream, 0, 0);
    const readBuf = new Uint8Array(size);
    FS.read(stream, readBuf, 0, size);
    expect(readBuf).toEqual(data);

    FS.close(stream);
  });

  it("1-page cache persistence round-trip @fast", async () => {
    const path = MOUNT + "/min_cache_persist";
    const stream = FS.open(path, O.RDWR | O.CREAT, 0o666);

    const size = PAGE_SIZE * 4;
    const data = fillBuf(size, 55);
    FS.write(stream, data, 0, size);
    FS.close(stream);

    syncfs(FS);

    const { FS: FS2 } = await createTestFS(1, backend);
    const s2 = FS2.open(MOUNT + "/min_cache_persist", O.RDONLY);
    const readBuf = new Uint8Array(size);
    expect(FS2.read(s2, readBuf, 0, size)).toBe(size);
    expect(readBuf).toEqual(data);
    FS2.close(s2);
  });

  it("round-robin writes across 4 files then read all back @fast", () => {
    const streams: any[] = [];
    for (let f = 0; f < 4; f++) {
      streams.push(FS.open(MOUNT + `/min_rr_${f}`, O.RDWR | O.CREAT, 0o666));
    }

    for (let round = 0; round < 3; round++) {
      for (let f = 0; f < 4; f++) {
        const data = fillBuf(PAGE_SIZE, f * 10 + round);
        FS.write(streams[f], data, 0, PAGE_SIZE);
      }
    }

    for (let f = 0; f < 4; f++) {
      FS.llseek(streams[f], 0, 0);
      for (let round = 0; round < 3; round++) {
        const buf = new Uint8Array(PAGE_SIZE);
        const n = FS.read(streams[f], buf, 0, PAGE_SIZE);
        expect(n).toBe(PAGE_SIZE);
        expect(verifyBuf(buf, PAGE_SIZE, f * 10 + round)).toBe(true);
      }
      FS.close(streams[f]);
    }
  });

  it("oversize write partially overwriting existing data @fast", () => {
    const path = MOUNT + "/min_partial_overwrite";
    const stream = FS.open(path, O.RDWR | O.CREAT, 0o666);

    for (let p = 0; p < 6; p++) {
      FS.write(stream, fillBuf(PAGE_SIZE, p + 1), 0, PAGE_SIZE, p * PAGE_SIZE);
    }

    const overwriteSize = PAGE_SIZE * 3;
    const overwriteData = fillBuf(overwriteSize, 99);
    FS.write(stream, overwriteData, 0, overwriteSize, PAGE_SIZE);

    const p0 = new Uint8Array(PAGE_SIZE);
    FS.read(stream, p0, 0, PAGE_SIZE, 0);
    expect(verifyBuf(p0, PAGE_SIZE, 1)).toBe(true);

    const mid = new Uint8Array(overwriteSize);
    FS.read(stream, mid, 0, overwriteSize, PAGE_SIZE);
    expect(mid).toEqual(overwriteData);

    const p4 = new Uint8Array(PAGE_SIZE);
    FS.read(stream, p4, 0, PAGE_SIZE, 4 * PAGE_SIZE);
    expect(verifyBuf(p4, PAGE_SIZE, 5)).toBe(true);

    const p5 = new Uint8Array(PAGE_SIZE);
    FS.read(stream, p5, 0, PAGE_SIZE, 5 * PAGE_SIZE);
    expect(verifyBuf(p5, PAGE_SIZE, 6)).toBe(true);

    FS.close(stream);
  });
});
