/**
 * Adversarial tests for restoreTree crash recovery.
 *
 * When Postgres writes files during shutdown, the page cache may flush
 * pages to the backend (via eviction or explicit flush) before syncfs
 * updates metadata. If the process crashes between page flush and
 * metadata sync, the backend has pages that extend beyond what metadata
 * reports. restoreTree must detect these extra pages and adjust file
 * sizes accordingly.
 *
 * This tests the recovery logic in tomefs.ts restoreTree that scans
 * for pages beyond metadata — a critical code path with no prior test
 * coverage.
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

function encode(s: string): Uint8Array {
  return new TextEncoder().encode(s);
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

function syncAndUnmount(FS: any, tomefs: any) {
  tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
    if (err) throw err;
  });
  FS.unmount(MOUNT);
}

describe("restoreTree crash recovery: pages beyond metadata", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("recovers file size when one extra page exists beyond metadata @fast", async () => {
    // Phase 1: create file with 1 page, sync normally
    const { FS, tomefs } = await mountTome(backend);
    const data = new Uint8Array(PAGE_SIZE);
    for (let i = 0; i < PAGE_SIZE; i++) data[i] = (i * 7) & 0xff;
    const s = FS.open(`${MOUNT}/file`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, PAGE_SIZE);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // Simulate crash: manually add an extra page to the backend
    // as if Postgres wrote more data and the page cache evicted it,
    // but metadata wasn't updated before crash
    const extraPage = new Uint8Array(PAGE_SIZE);
    for (let i = 0; i < PAGE_SIZE; i++) extraPage[i] = (i * 13) & 0xff;
    backend.writePage("/file", 1, extraPage);
    // Don't update metadata — this simulates the crash

    // Phase 2: remount — restoreTree should detect the extra page
    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/file`);
    expect(stat.size).toBe(PAGE_SIZE * 2);

    // Verify both pages are readable with correct data
    const buf = new Uint8Array(PAGE_SIZE * 2);
    const s2 = FS2.open(`${MOUNT}/file`, O.RDONLY);
    const n = FS2.read(s2, buf, 0, PAGE_SIZE * 2);
    FS2.close(s2);
    expect(n).toBe(PAGE_SIZE * 2);
    expect(buf.subarray(0, PAGE_SIZE)).toEqual(data);
    expect(buf.subarray(PAGE_SIZE, PAGE_SIZE * 2)).toEqual(extraPage);
  });

  it("recovers file size when multiple extra pages exist", async () => {
    // Phase 1: create file with 2 pages, sync normally
    const { FS, tomefs } = await mountTome(backend);
    const data = new Uint8Array(PAGE_SIZE * 2);
    for (let i = 0; i < data.length; i++) data[i] = (i * 3) & 0xff;
    const s = FS.open(`${MOUNT}/multi`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // Simulate crash: add 3 extra pages (pages 2, 3, 4)
    for (let i = 2; i <= 4; i++) {
      const page = new Uint8Array(PAGE_SIZE);
      for (let j = 0; j < PAGE_SIZE; j++) page[j] = ((i * 100 + j) * 17) & 0xff;
      backend.writePage("/multi", i, page);
    }

    // Phase 2: remount — should detect all extra pages
    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/multi`);
    expect(stat.size).toBe(PAGE_SIZE * 5);

    // Verify extra pages are readable
    const buf = new Uint8Array(PAGE_SIZE);
    const s2 = FS2.open(`${MOUNT}/multi`, O.RDONLY);
    for (let i = 2; i <= 4; i++) {
      FS2.read(s2, buf, 0, PAGE_SIZE, i * PAGE_SIZE);
      const expected = new Uint8Array(PAGE_SIZE);
      for (let j = 0; j < PAGE_SIZE; j++) expected[j] = ((i * 100 + j) * 17) & 0xff;
      expect(buf).toEqual(expected);
    }
    FS2.close(s2);
  });

  it("recovers file with pages when metadata says size=0", async () => {
    // Phase 1: create an empty file and sync
    const { FS, tomefs } = await mountTome(backend);
    const s = FS.open(`${MOUNT}/zero`, O.RDWR | O.CREAT, 0o666);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // Simulate crash: add pages to a file whose metadata says size=0
    const pageData = new Uint8Array(PAGE_SIZE);
    for (let i = 0; i < PAGE_SIZE; i++) pageData[i] = 0xab;
    backend.writePage("/zero", 0, pageData);

    // Phase 2: remount — should detect the page and adjust size
    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/zero`);
    expect(stat.size).toBe(PAGE_SIZE);

    // Verify data is readable
    const buf = new Uint8Array(PAGE_SIZE);
    const s2 = FS2.open(`${MOUNT}/zero`, O.RDONLY);
    FS2.read(s2, buf, 0, PAGE_SIZE);
    FS2.close(s2);
    expect(buf).toEqual(pageData);
  });

  it("does not change size when no extra pages exist (clean sync)", async () => {
    // Normal case: sync was clean, metadata matches pages
    const { FS, tomefs } = await mountTome(backend);
    const data = new Uint8Array(1234);
    for (let i = 0; i < data.length; i++) data[i] = i & 0xff;
    const s = FS.open(`${MOUNT}/clean`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, 1234);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // No tampering — just remount
    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/clean`);
    expect(stat.size).toBe(1234);
  });

  it("recovers correctly with tiny cache forcing eviction during restore", async () => {
    // Use a tiny cache (4 pages) and create a file that gets extra pages
    // This tests that page-loading during recovery works with eviction
    const { FS, tomefs } = await mountTome(backend, 4);
    const data = new Uint8Array(PAGE_SIZE * 3);
    for (let i = 0; i < data.length; i++) data[i] = (i * 5) & 0xff;
    const s = FS.open(`${MOUNT}/big`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // Add 3 more extra pages (pages 3, 4, 5) — total 6 pages with 4-page cache
    for (let i = 3; i <= 5; i++) {
      const page = new Uint8Array(PAGE_SIZE);
      page.fill(i & 0xff);
      backend.writePage("/big", i, page);
    }

    // Remount with tiny cache
    const { FS: FS2 } = await mountTome(backend, 4);
    const stat = FS2.stat(`${MOUNT}/big`);
    expect(stat.size).toBe(PAGE_SIZE * 6);

    // Verify last extra page is readable
    const buf = new Uint8Array(PAGE_SIZE);
    const s2 = FS2.open(`${MOUNT}/big`, O.RDONLY);
    FS2.read(s2, buf, 0, PAGE_SIZE, 5 * PAGE_SIZE);
    FS2.close(s2);
    const expected = new Uint8Array(PAGE_SIZE);
    expected.fill(5);
    expect(buf).toEqual(expected);
  });

  it("recovery applies per-file: only affected file gets size correction", async () => {
    // Create two files, sync normally
    const { FS, tomefs } = await mountTome(backend);
    for (const name of ["normal", "extra"]) {
      const s = FS.open(`${MOUNT}/${name}`, O.RDWR | O.CREAT, 0o666);
      FS.write(s, encode(`data-${name}`), 0, `data-${name}`.length);
      FS.close(s);
    }
    syncAndUnmount(FS, tomefs);

    // Add extra page only to "extra" file
    backend.writePage("/extra", 1, new Uint8Array(PAGE_SIZE));

    const { FS: FS2 } = await mountTome(backend);

    // "normal" file should retain its original size
    const normalStat = FS2.stat(`${MOUNT}/normal`);
    expect(normalStat.size).toBe("data-normal".length);

    // "extra" file should have expanded size
    const extraStat = FS2.stat(`${MOUNT}/extra`);
    expect(extraStat.size).toBe(PAGE_SIZE * 2);
  });

  it("recovered file can be written to and re-synced correctly", async () => {
    // Create file, sync, add extra page (crash scenario)
    const { FS, tomefs } = await mountTome(backend);
    const s = FS.open(`${MOUNT}/rw`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("original"), 0, 8);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    backend.writePage("/rw", 1, new Uint8Array(PAGE_SIZE));

    // Remount — file size recovered to 2 pages
    const { FS: FS2, tomefs: t2 } = await mountTome(backend);
    expect(FS2.stat(`${MOUNT}/rw`).size).toBe(PAGE_SIZE * 2);

    // Write more data and sync cleanly
    const s2 = FS2.open(`${MOUNT}/rw`, O.RDWR);
    FS2.write(s2, encode("updated!"), 0, 8);
    FS2.close(s2);
    syncAndUnmount(FS2, t2);

    // Third mount: verify clean sync after recovery
    const { FS: FS3 } = await mountTome(backend);
    const stat = FS3.stat(`${MOUNT}/rw`);
    expect(stat.size).toBe(PAGE_SIZE * 2);

    // Original write at position 0 should be the updated content
    const buf = new Uint8Array(8);
    const s3 = FS3.open(`${MOUNT}/rw`, O.RDONLY);
    FS3.read(s3, buf, 0, 8);
    FS3.close(s3);
    expect(new TextDecoder().decode(buf)).toBe("updated!");
  });

  it("recovery handles file in nested directory with extra pages", async () => {
    const { FS, tomefs } = await mountTome(backend);
    FS.mkdir(`${MOUNT}/pg_data`);
    FS.mkdir(`${MOUNT}/pg_data/base`);
    const s = FS.open(`${MOUNT}/pg_data/base/16384`, O.RDWR | O.CREAT, 0o666);
    const data = new Uint8Array(PAGE_SIZE);
    data.fill(0x42);
    FS.write(s, data, 0, PAGE_SIZE);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // Simulate Postgres extending a relation file during shutdown
    const walPage = new Uint8Array(PAGE_SIZE);
    walPage.fill(0xff);
    backend.writePage("/pg_data/base/16384", 1, walPage);

    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/pg_data/base/16384`);
    expect(stat.size).toBe(PAGE_SIZE * 2);

    // Verify both pages
    const buf = new Uint8Array(PAGE_SIZE * 2);
    const s2 = FS2.open(`${MOUNT}/pg_data/base/16384`, O.RDONLY);
    FS2.read(s2, buf, 0, PAGE_SIZE * 2);
    FS2.close(s2);
    expect(buf.subarray(0, PAGE_SIZE)).toEqual(data);
    expect(buf.subarray(PAGE_SIZE)).toEqual(walPage);
  });
});
