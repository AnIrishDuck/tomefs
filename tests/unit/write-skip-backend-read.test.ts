/**
 * Tests that the tomefs single-page write fast path skips backend reads
 * for pages beyond the current file extent.
 *
 * When appending or extending a file with page-aligned writes (the common
 * Postgres pattern), the page doesn't exist in the backend. Previously,
 * the single-page fast path in tomefs.ts always called getPage() which
 * reads from the backend — a wasted SAB bridge round-trip returning null.
 *
 * Now it uses getPageNoRead() for pages beyond the file extent.
 */

import { describe, it, expect } from "vitest";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { createTomeFS } from "../../src/tomefs.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { PAGE_SIZE } from "../../src/types.js";
import { pageKeyStr } from "../../src/types.js";
import type { EmscriptenFS } from "../harness/emscripten-fs.js";
import { O, SEEK_SET } from "../harness/emscripten-fs.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const MOUNT = "/tome";

/**
 * Counting wrapper around SyncMemoryBackend that tracks readPage calls.
 * This is a fake (not a mock) per project conventions — it wraps a real
 * SyncMemoryBackend and delegates all operations to it.
 */
function createCountingBackend() {
  const inner = new SyncMemoryBackend();
  let readPageCalls = 0;
  let readPagesCalls = 0;

  const counting: SyncMemoryBackend & {
    readPageCalls: number;
    readPagesCalls: number;
    resetCounts(): void;
  } = Object.create(inner);

  Object.defineProperty(counting, "readPageCalls", {
    get: () => readPageCalls,
  });
  Object.defineProperty(counting, "readPagesCalls", {
    get: () => readPagesCalls,
  });
  counting.resetCounts = () => {
    readPageCalls = 0;
    readPagesCalls = 0;
  };
  counting.readPage = (path: string, pageIndex: number) => {
    readPageCalls++;
    return inner.readPage(path, pageIndex);
  };
  counting.readPages = (path: string, pageIndices: number[]) => {
    readPagesCalls++;
    return inner.readPages(path, pageIndices);
  };
  return counting;
}

async function mountTome(backend: SyncMemoryBackend, maxPages?: number) {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  const Module = await createModule();
  const FS = Module.FS as EmscriptenFS;
  const tomefs = createTomeFS(FS, { backend, maxPages });
  FS.mkdir(MOUNT);
  FS.mount(tomefs, {}, MOUNT);
  return { FS, tomefs };
}

describe("tomefs single-page write: skip backend reads for new pages", () => {
  it("@fast sequential page-aligned writes skip backend reads", async () => {
    const cb = createCountingBackend();
    const { FS } = await mountTome(cb, 64);

    const stream = FS.open(`${MOUNT}/seq`, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    const pageData = new Uint8Array(PAGE_SIZE);

    // Write 10 pages sequentially (each is a single-page write on the fast path)
    cb.resetCounts();
    for (let i = 0; i < 10; i++) {
      pageData.fill(i + 1);
      FS.write(stream, pageData, 0, PAGE_SIZE);
    }

    // Every page is a new page beyond the file extent.
    // No backend reads should occur.
    expect(cb.readPageCalls).toBe(0);
    expect(cb.readPagesCalls).toBe(0);

    FS.close(stream);

    // Verify data integrity: read each page back
    const readStream = FS.open(`${MOUNT}/seq`, O.RDONLY);
    const readBuf = new Uint8Array(PAGE_SIZE);
    for (let i = 0; i < 10; i++) {
      FS.read(readStream, readBuf, 0, PAGE_SIZE);
      expect(readBuf[0]).toBe(i + 1);
      expect(readBuf[PAGE_SIZE - 1]).toBe(i + 1);
    }
    FS.close(readStream);
  });

  it("@fast overwrite of existing page still reads from backend", async () => {
    const cb = createCountingBackend();
    const { FS, tomefs } = await mountTome(cb, 64);

    // Write a page and flush it to backend
    const stream = FS.open(`${MOUNT}/existing`, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    const pageData = new Uint8Array(PAGE_SIZE);
    pageData.fill(0xaa);
    FS.write(stream, pageData, 0, PAGE_SIZE);
    FS.close(stream);

    // Flush and evict to force data out of cache
    tomefs.pageCache.flushAll();
    tomefs.pageCache.evictFile("/existing");

    // Now overwrite page 0 (which exists in backend)
    cb.resetCounts();
    const stream2 = FS.open(`${MOUNT}/existing`, O.WRONLY, 0o666);
    pageData.fill(0xbb);
    FS.write(stream2, pageData, 0, PAGE_SIZE);
    FS.close(stream2);

    // Page 0 is within existing extent → backend read is required
    expect(cb.readPageCalls).toBe(1);
  });

  it("@fast sub-page append within last page skips backend read", async () => {
    const cb = createCountingBackend();
    const { FS } = await mountTome(cb, 64);

    // Write initial sub-page data (stays within page 0)
    const stream = FS.open(`${MOUNT}/subpage`, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    const data = new Uint8Array(100);
    data.fill(0xcc);
    FS.write(stream, data, 0, 100);

    // Now append more data within page 0 (position 100, within same page)
    // MRU should still be page 0, so no cache or backend access needed
    cb.resetCounts();
    const more = new Uint8Array(50);
    more.fill(0xdd);
    FS.write(stream, more, 0, 50);

    expect(cb.readPageCalls).toBe(0);
    expect(cb.readPagesCalls).toBe(0);

    // Append into page 1 (new page, beyond extent)
    const page1Data = new Uint8Array(PAGE_SIZE);
    page1Data.fill(0xee);
    FS.llseek(stream, PAGE_SIZE, SEEK_SET);
    cb.resetCounts();
    FS.write(stream, page1Data, 0, PAGE_SIZE);

    // Page 1 is beyond file extent → no backend read
    expect(cb.readPageCalls).toBe(0);
    expect(cb.readPagesCalls).toBe(0);

    FS.close(stream);
  });

  it("simulated Postgres WAL append pattern: zero backend reads @fast", async () => {
    const cb = createCountingBackend();
    const { FS } = await mountTome(cb, 256);

    // Simulate Postgres WAL writing: many small appends that eventually
    // cross page boundaries. Each individual write is sub-page (128 bytes),
    // matching WAL record sizes.
    const stream = FS.open(`${MOUNT}/wal`, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    const record = new Uint8Array(128);

    cb.resetCounts();
    // Write enough records to span ~8 pages
    const numRecords = (PAGE_SIZE * 8) / 128;
    for (let i = 0; i < numRecords; i++) {
      record.fill(i & 0xff);
      FS.write(stream, record, 0, 128);
    }

    // All writes are extending the file — no backend reads needed.
    // Cross-page boundary writes might trigger reads for the page being
    // extended into if it doesn't exist yet, but getPageNoRead prevents this.
    expect(cb.readPageCalls).toBe(0);
    expect(cb.readPagesCalls).toBe(0);

    FS.close(stream);

    // Verify total size
    const stat = FS.stat(`${MOUNT}/wal`);
    expect(stat.size).toBe(PAGE_SIZE * 8);
  });
});
