/**
 * Adversarial tests: restoreTree with non-contiguous pages in backend.
 *
 * When a file is grown via allocate() or seek-past-end write, only the
 * sentinel page (last page) and any written pages exist in the backend.
 * Intermediate pages are sparse (zero-filled on demand, never persisted).
 *
 * If a crash occurs between allocate/write and syncfs, the backend has:
 * - Metadata from the PREVIOUS sync (old, smaller size)
 * - Non-contiguous pages: original pages + evicted sentinel/written pages
 *
 * restoreTree must detect the true file extent from the highest page
 * index, NOT from countPages (which just counts the number of stored
 * pages, not their maximum index).
 *
 * Example: file has pages at indices [0, 7]. countPages = 2.
 * Using countPages * PAGE_SIZE = 2 * PAGE_SIZE = 16 KB.
 * But the actual data extends to page 7 (offset 64 KB).
 * Page 7 becomes inaccessible — data loss.
 *
 * Ethos §9: "Write tests designed to break tomefs specifically"
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

function syncAndUnmount(FS: any, tomefs: any) {
  tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
    if (err) throw err;
  });
  FS.unmount(MOUNT);
}

describe("restoreTree: non-contiguous pages (allocate + crash)", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("recovers file extent from highest page index, not page count @fast", async () => {
    // Phase 1: create a 1-page file and sync normally
    const { FS, tomefs } = await mountTome(backend);
    const page0 = new Uint8Array(PAGE_SIZE);
    for (let i = 0; i < PAGE_SIZE; i++) page0[i] = (i * 7) & 0xff;
    const s = FS.open(`${MOUNT}/file`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, page0, 0, PAGE_SIZE);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // Verify baseline: meta.size = PAGE_SIZE, 1 page in backend
    expect(backend.readMeta("/file")!.size).toBe(PAGE_SIZE);
    expect(backend.countPages("/file")).toBe(1);

    // Simulate crash after allocate + eviction:
    // allocate(6 * PAGE_SIZE) materialized sentinel at page 5,
    // sentinel was evicted to backend, but syncfs never ran.
    // Backend now has pages [0, 5] — non-contiguous.
    const sentinel = new Uint8Array(PAGE_SIZE); // zeros (sentinel)
    backend.writePage("/file", 5, sentinel);

    // Backend state: pages at [0, 5], count = 2, meta.size = PAGE_SIZE
    expect(backend.countPages("/file")).toBe(2);

    // Phase 2: remount — restoreTree must detect page 5
    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/file`);

    // File size must cover page 5 (at minimum 6 * PAGE_SIZE)
    // Using countPages * PAGE_SIZE = 2 * PAGE_SIZE would lose page 5
    expect(stat.size).toBeGreaterThanOrEqual(6 * PAGE_SIZE);

    // Page 0 data must be intact
    const buf0 = new Uint8Array(PAGE_SIZE);
    const s2 = FS2.open(`${MOUNT}/file`, O.RDONLY);
    FS2.read(s2, buf0, 0, PAGE_SIZE, 0);
    expect(buf0).toEqual(page0);

    // Page 5 must be readable (zeros)
    const buf5 = new Uint8Array(PAGE_SIZE);
    FS2.read(s2, buf5, 0, PAGE_SIZE, 5 * PAGE_SIZE);
    expect(buf5).toEqual(sentinel);

    FS2.close(s2);
  });

  it("recovers when extra pages are far from metadata extent", async () => {
    // Sync a 2-page file
    const { FS, tomefs } = await mountTome(backend);
    const data = new Uint8Array(PAGE_SIZE * 2);
    for (let i = 0; i < data.length; i++) data[i] = (i * 3) & 0xff;
    const s = FS.open(`${MOUNT}/sparse`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // Simulate: allocate to 20 pages + crash. Sentinel at page 19.
    // Only page 19 was evicted; pages 2-18 were never materialized.
    backend.writePage("/sparse", 19, new Uint8Array(PAGE_SIZE));

    // Backend: pages [0, 1, 19], count = 3, meta.size = 2 * PAGE_SIZE
    expect(backend.countPages("/sparse")).toBe(3);

    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/sparse`);

    // Must cover page 19
    expect(stat.size).toBeGreaterThanOrEqual(20 * PAGE_SIZE);

    // Original data intact
    const buf = new Uint8Array(PAGE_SIZE * 2);
    const s2 = FS2.open(`${MOUNT}/sparse`, O.RDONLY);
    FS2.read(s2, buf, 0, PAGE_SIZE * 2, 0);
    expect(buf).toEqual(data);

    // Gap pages (e.g., page 10) read as zeros
    const gap = new Uint8Array(PAGE_SIZE);
    FS2.read(s2, gap, 0, PAGE_SIZE, 10 * PAGE_SIZE);
    expect(gap).toEqual(new Uint8Array(PAGE_SIZE));

    FS2.close(s2);
  });

  it("handles crash with non-contiguous pages below metadata extent", async () => {
    // File synced with 5 pages. Then truncation deleted pages 1-4,
    // but a concurrent allocate re-created a sentinel at page 7.
    // Crash leaves: pages [0, 7], meta.size = 5 * PAGE_SIZE.
    const { FS, tomefs } = await mountTome(backend);
    const data = new Uint8Array(PAGE_SIZE * 5);
    for (let i = 0; i < data.length; i++) data[i] = (i * 11) & 0xff;
    const s = FS.open(`${MOUNT}/mixed`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // Simulate: truncate to 1 page + allocate to 8 pages + crash
    backend.deletePagesFrom("/mixed", 1);
    backend.writePage("/mixed", 7, new Uint8Array(PAGE_SIZE));

    // Backend: pages [0, 7], count = 2, meta.size = 5 * PAGE_SIZE
    // pagesFromMeta = 5, actualPageCount = 2 < 5
    // Probe last page (index 4): missing
    // Old logic: fileSize = countPages * PAGE_SIZE = 2 * PAGE_SIZE (WRONG)
    // Correct: must detect page 7 and set size >= 8 * PAGE_SIZE

    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/mixed`);

    // File must cover page 7
    expect(stat.size).toBeGreaterThanOrEqual(8 * PAGE_SIZE);

    // Page 0 data intact
    const buf0 = new Uint8Array(PAGE_SIZE);
    const s2 = FS2.open(`${MOUNT}/mixed`, O.RDONLY);
    FS2.read(s2, buf0, 0, PAGE_SIZE, 0);
    expect(buf0).toEqual(data.subarray(0, PAGE_SIZE));

    // Page 7 readable
    const buf7 = new Uint8Array(PAGE_SIZE);
    FS2.read(s2, buf7, 0, PAGE_SIZE, 7 * PAGE_SIZE);
    expect(buf7).toEqual(new Uint8Array(PAGE_SIZE));

    FS2.close(s2);
  });

  it("does not affect normal recovery (contiguous pages beyond metadata)", async () => {
    // Standard crash recovery: pages 0-3 exist, meta says 2 pages.
    // countPages = 4, which equals max index + 1. No bug here.
    const { FS, tomefs } = await mountTome(backend);
    const data = new Uint8Array(PAGE_SIZE * 2);
    data.fill(0xaa);
    const s = FS.open(`${MOUNT}/contig`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // Simulate crash: 2 more contiguous pages were written
    backend.writePage("/contig", 2, new Uint8Array(PAGE_SIZE).fill(0xbb));
    backend.writePage("/contig", 3, new Uint8Array(PAGE_SIZE).fill(0xcc));

    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/contig`);
    expect(stat.size).toBe(4 * PAGE_SIZE);

    // Verify all 4 pages
    const buf = new Uint8Array(PAGE_SIZE);
    const s2 = FS2.open(`${MOUNT}/contig`, O.RDONLY);
    FS2.read(s2, buf, 0, PAGE_SIZE, 2 * PAGE_SIZE);
    expect(buf[0]).toBe(0xbb);
    FS2.read(s2, buf, 0, PAGE_SIZE, 3 * PAGE_SIZE);
    expect(buf[0]).toBe(0xcc);
    FS2.close(s2);
  });

  it("recovers with tiny cache forcing eviction during restore @fast", async () => {
    // Same non-contiguous scenario but with 4-page cache
    const { FS, tomefs } = await mountTome(backend, 4);
    const data = new Uint8Array(PAGE_SIZE);
    data.fill(0xdd);
    const s = FS.open(`${MOUNT}/tiny`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, PAGE_SIZE);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // Non-contiguous: page 0 + sentinel at page 9
    backend.writePage("/tiny", 9, new Uint8Array(PAGE_SIZE).fill(0xee));

    const { FS: FS2 } = await mountTome(backend, 4);
    const stat = FS2.stat(`${MOUNT}/tiny`);
    expect(stat.size).toBeGreaterThanOrEqual(10 * PAGE_SIZE);

    // Page 9 is readable
    const buf = new Uint8Array(PAGE_SIZE);
    const s2 = FS2.open(`${MOUNT}/tiny`, O.RDONLY);
    FS2.read(s2, buf, 0, PAGE_SIZE, 9 * PAGE_SIZE);
    expect(buf[0]).toBe(0xee);
    FS2.close(s2);
  });
});
