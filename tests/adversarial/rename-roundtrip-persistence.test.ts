/**
 * Adversarial tests: rename round-trip cycles (A→B→A) with persistence.
 *
 * Postgres recycles WAL segments by renaming old segments to new names.
 * In some configurations, files are renamed away (A→B) and then renamed
 * back (B→A) within the same session. This creates a round-trip that
 * exercises the page cache's secondary index maintenance (filePages,
 * dirtyKeys, dirtyFileKeys) through a complete cycle: the cache keys
 * are re-keyed from path A to path B, then back from path B to path A.
 *
 * The critical seam: after the A→B→A round-trip, page cache keys are
 * back at the original key strings — but the LRU order, dirty tracking,
 * and backend state have all been modified by the intermediate rename.
 * A bug in index maintenance could:
 * - Leave stale entries in filePages under the intermediate path
 * - Double-count dirty pages that were flushed during the first rename
 * - Corrupt the dirtyFileKeys mapping if the path→key linkage is wrong
 *
 * These tests verify data integrity, cache invariant consistency, and
 * persistence correctness through rename round-trips under cache pressure.
 *
 * Ethos §9: "Target the seams: ... metadata updates after flush"
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

function fillPattern(size: number, seed: number): Uint8Array {
  const buf = new Uint8Array(size);
  for (let i = 0; i < size; i++) {
    buf[i] = (seed + i * 31) & 0xff;
  }
  return buf;
}

function verifyPattern(buf: Uint8Array, size: number, seed: number): boolean {
  for (let i = 0; i < size; i++) {
    if (buf[i] !== ((seed + i * 31) & 0xff)) return false;
  }
  return true;
}

async function mountTome(backend: SyncMemoryBackend, maxPages = 4) {
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

describe("adversarial: rename round-trip (A→B→A) + persistence @fast", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("A→B→A round-trip preserves data and persists correctly", async () => {
    const { FS, tomefs } = await mountTome(backend);
    const data = fillPattern(PAGE_SIZE, 0xAA);

    const fd = FS.open(`${MOUNT}/fileA`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, data, 0, PAGE_SIZE, 0);
    FS.close(fd);

    FS.rename(`${MOUNT}/fileA`, `${MOUNT}/fileB`);
    FS.rename(`${MOUNT}/fileB`, `${MOUNT}/fileA`);

    const buf = new Uint8Array(PAGE_SIZE);
    const fd2 = FS.open(`${MOUNT}/fileA`, O.RDONLY);
    FS.read(fd2, buf, 0, PAGE_SIZE, 0);
    FS.close(fd2);
    expect(verifyPattern(buf, PAGE_SIZE, 0xAA)).toBe(true);

    syncfs(FS, tomefs);

    const m2 = await mountTome(backend);
    const stat = m2.FS.stat(`${MOUNT}/fileA`);
    expect(stat.size).toBe(PAGE_SIZE);

    const buf2 = new Uint8Array(PAGE_SIZE);
    const fd3 = m2.FS.open(`${MOUNT}/fileA`, O.RDONLY);
    m2.FS.read(fd3, buf2, 0, PAGE_SIZE, 0);
    m2.FS.close(fd3);
    expect(verifyPattern(buf2, PAGE_SIZE, 0xAA)).toBe(true);

    // fileB must not exist after remount
    expect(() => m2.FS.stat(`${MOUNT}/fileB`)).toThrow();
  });

  it("A→B→A with dirty writes at each step persists final state", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Write page 0 as file A
    const fd = FS.open(`${MOUNT}/a`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, fillPattern(PAGE_SIZE, 0x11), 0, PAGE_SIZE, 0);
    FS.close(fd);

    FS.rename(`${MOUNT}/a`, `${MOUNT}/b`);

    // Write page 1 while named B
    const fd2 = FS.open(`${MOUNT}/b`, O.WRONLY);
    FS.write(fd2, fillPattern(PAGE_SIZE, 0x22), 0, PAGE_SIZE, PAGE_SIZE);
    FS.close(fd2);

    FS.rename(`${MOUNT}/b`, `${MOUNT}/a`);

    // Write page 2 after round-trip
    const fd3 = FS.open(`${MOUNT}/a`, O.WRONLY);
    FS.write(fd3, fillPattern(PAGE_SIZE, 0x33), 0, PAGE_SIZE, PAGE_SIZE * 2);
    FS.close(fd3);

    syncfs(FS, tomefs);

    const m2 = await mountTome(backend);
    expect(m2.FS.stat(`${MOUNT}/a`).size).toBe(PAGE_SIZE * 3);
    expect(() => m2.FS.stat(`${MOUNT}/b`)).toThrow();

    const fd4 = m2.FS.open(`${MOUNT}/a`, O.RDONLY);
    const buf = new Uint8Array(PAGE_SIZE);

    m2.FS.read(fd4, buf, 0, PAGE_SIZE, 0);
    expect(verifyPattern(buf, PAGE_SIZE, 0x11)).toBe(true);

    m2.FS.read(fd4, buf, 0, PAGE_SIZE, PAGE_SIZE);
    expect(verifyPattern(buf, PAGE_SIZE, 0x22)).toBe(true);

    m2.FS.read(fd4, buf, 0, PAGE_SIZE, PAGE_SIZE * 2);
    expect(verifyPattern(buf, PAGE_SIZE, 0x33)).toBe(true);

    m2.FS.close(fd4);
  });

  it("multiple round-trips under 4-page cache pressure", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    // Write 2 pages
    const fd = FS.open(`${MOUNT}/f`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, fillPattern(PAGE_SIZE * 2, 0x55), 0, PAGE_SIZE * 2, 0);
    FS.close(fd);

    // 5 round-trip cycles
    for (let i = 0; i < 5; i++) {
      FS.rename(`${MOUNT}/f`, `${MOUNT}/tmp`);
      FS.rename(`${MOUNT}/tmp`, `${MOUNT}/f`);
    }

    // Data must survive all round-trips
    const buf = new Uint8Array(PAGE_SIZE * 2);
    const fd2 = FS.open(`${MOUNT}/f`, O.RDONLY);
    FS.read(fd2, buf, 0, PAGE_SIZE * 2, 0);
    FS.close(fd2);
    expect(verifyPattern(buf, PAGE_SIZE * 2, 0x55)).toBe(true);

    syncfs(FS, tomefs);

    const m2 = await mountTome(backend, 4);
    expect(m2.FS.stat(`${MOUNT}/f`).size).toBe(PAGE_SIZE * 2);

    const buf2 = new Uint8Array(PAGE_SIZE * 2);
    const fd3 = m2.FS.open(`${MOUNT}/f`, O.RDONLY);
    m2.FS.read(fd3, buf2, 0, PAGE_SIZE * 2, 0);
    m2.FS.close(fd3);
    expect(verifyPattern(buf2, PAGE_SIZE * 2, 0x55)).toBe(true);
  });

  it("three files competing during rename round-trips", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    // Create 3 files with different data
    for (const [name, seed] of [["x", 0x11], ["y", 0x22], ["z", 0x33]] as const) {
      const fd = FS.open(`${MOUNT}/${name}`, O.RDWR | O.CREAT, 0o666);
      FS.write(fd, fillPattern(PAGE_SIZE, seed), 0, PAGE_SIZE, 0);
      FS.close(fd);
    }

    // Rotate: x→tmp, y→x, z→y, tmp→z
    FS.rename(`${MOUNT}/x`, `${MOUNT}/tmp`);
    FS.rename(`${MOUNT}/y`, `${MOUNT}/x`);
    FS.rename(`${MOUNT}/z`, `${MOUNT}/y`);
    FS.rename(`${MOUNT}/tmp`, `${MOUNT}/z`);

    // Rotate back: z→tmp, y→z, x→y, tmp→x
    FS.rename(`${MOUNT}/z`, `${MOUNT}/tmp`);
    FS.rename(`${MOUNT}/y`, `${MOUNT}/z`);
    FS.rename(`${MOUNT}/x`, `${MOUNT}/y`);
    FS.rename(`${MOUNT}/tmp`, `${MOUNT}/x`);

    // After full round-trip, original data should be at original names
    syncfs(FS, tomefs);

    const m2 = await mountTome(backend, 4);
    const buf = new Uint8Array(PAGE_SIZE);

    for (const [name, seed] of [["x", 0x11], ["y", 0x22], ["z", 0x33]] as const) {
      const fd = m2.FS.open(`${MOUNT}/${name}`, O.RDONLY);
      m2.FS.read(fd, buf, 0, PAGE_SIZE, 0);
      m2.FS.close(fd);
      expect(verifyPattern(buf, PAGE_SIZE, seed)).toBe(true);
    }
  });

  it("A→B→A with truncation between renames", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    // Write 3 pages
    const fd = FS.open(`${MOUNT}/f`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, fillPattern(PAGE_SIZE * 3, 0xCC), 0, PAGE_SIZE * 3, 0);
    FS.close(fd);

    FS.rename(`${MOUNT}/f`, `${MOUNT}/g`);

    // Truncate while named g
    FS.truncate(`${MOUNT}/g`, PAGE_SIZE + 100);

    FS.rename(`${MOUNT}/g`, `${MOUNT}/f`);

    expect(FS.stat(`${MOUNT}/f`).size).toBe(PAGE_SIZE + 100);

    syncfs(FS, tomefs);

    const m2 = await mountTome(backend, 4);
    expect(m2.FS.stat(`${MOUNT}/f`).size).toBe(PAGE_SIZE + 100);

    const buf = new Uint8Array(PAGE_SIZE + 100);
    const fd2 = m2.FS.open(`${MOUNT}/f`, O.RDONLY);
    m2.FS.read(fd2, buf, 0, PAGE_SIZE + 100, 0);
    m2.FS.close(fd2);

    // Page 0 fully intact
    expect(verifyPattern(buf.subarray(0, PAGE_SIZE), PAGE_SIZE, 0xCC)).toBe(true);
    // Page 1 first 100 bytes intact
    for (let i = 0; i < 100; i++) {
      expect(buf[PAGE_SIZE + i]).toBe(((0xCC + (PAGE_SIZE + i) * 31) & 0xff));
    }
  });

  it("rename A→B, create new A, rename B→A overwrites new A", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    // Create original A
    const fd = FS.open(`${MOUNT}/a`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, fillPattern(PAGE_SIZE, 0x11), 0, PAGE_SIZE, 0);
    FS.close(fd);

    // Move A to B
    FS.rename(`${MOUNT}/a`, `${MOUNT}/b`);

    // Create a new A with different data
    const fd2 = FS.open(`${MOUNT}/a`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd2, fillPattern(PAGE_SIZE, 0x99), 0, PAGE_SIZE, 0);
    FS.close(fd2);

    // Move B back to A (overwrites the new A)
    FS.rename(`${MOUNT}/b`, `${MOUNT}/a`);

    const buf = new Uint8Array(PAGE_SIZE);
    const fd3 = FS.open(`${MOUNT}/a`, O.RDONLY);
    FS.read(fd3, buf, 0, PAGE_SIZE, 0);
    FS.close(fd3);
    // Should have original data (0x11), not new data (0x99)
    expect(verifyPattern(buf, PAGE_SIZE, 0x11)).toBe(true);

    syncfs(FS, tomefs);

    const m2 = await mountTome(backend, 4);
    const buf2 = new Uint8Array(PAGE_SIZE);
    const fd4 = m2.FS.open(`${MOUNT}/a`, O.RDONLY);
    m2.FS.read(fd4, buf2, 0, PAGE_SIZE, 0);
    m2.FS.close(fd4);
    expect(verifyPattern(buf2, PAGE_SIZE, 0x11)).toBe(true);

    expect(() => m2.FS.stat(`${MOUNT}/b`)).toThrow();
  });

  it("cache assertInvariants holds through rename round-trip cycle", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    const fd = FS.open(`${MOUNT}/f`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, fillPattern(PAGE_SIZE * 2, 0x77), 0, PAGE_SIZE * 2, 0);
    FS.close(fd);
    tomefs.pageCache.assertInvariants();

    FS.rename(`${MOUNT}/f`, `${MOUNT}/g`);
    tomefs.pageCache.assertInvariants();

    // Write while named g
    const fd2 = FS.open(`${MOUNT}/g`, O.WRONLY);
    FS.write(fd2, fillPattern(PAGE_SIZE, 0x88), 0, PAGE_SIZE, PAGE_SIZE * 2);
    FS.close(fd2);
    tomefs.pageCache.assertInvariants();

    FS.rename(`${MOUNT}/g`, `${MOUNT}/f`);
    tomefs.pageCache.assertInvariants();

    syncfs(FS, tomefs);
    tomefs.pageCache.assertInvariants();

    // Second round-trip
    FS.rename(`${MOUNT}/f`, `${MOUNT}/h`);
    tomefs.pageCache.assertInvariants();

    FS.rename(`${MOUNT}/h`, `${MOUNT}/f`);
    tomefs.pageCache.assertInvariants();

    syncfs(FS, tomefs);
    tomefs.pageCache.assertInvariants();
  });

  it("A→B→A with syncfs between each rename persists correctly", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    const fd = FS.open(`${MOUNT}/f`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, fillPattern(PAGE_SIZE, 0xDD), 0, PAGE_SIZE, 0);
    FS.close(fd);
    syncfs(FS, tomefs);

    FS.rename(`${MOUNT}/f`, `${MOUNT}/g`);
    syncfs(FS, tomefs);

    FS.rename(`${MOUNT}/g`, `${MOUNT}/f`);
    syncfs(FS, tomefs);

    // Remount and verify
    const m2 = await mountTome(backend, 4);
    expect(m2.FS.stat(`${MOUNT}/f`).size).toBe(PAGE_SIZE);
    expect(() => m2.FS.stat(`${MOUNT}/g`)).toThrow();

    const buf = new Uint8Array(PAGE_SIZE);
    const fd2 = m2.FS.open(`${MOUNT}/f`, O.RDONLY);
    m2.FS.read(fd2, buf, 0, PAGE_SIZE, 0);
    m2.FS.close(fd2);
    expect(verifyPattern(buf, PAGE_SIZE, 0xDD)).toBe(true);
  });

  it("directory rename round-trip preserves all descendant data", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    FS.mkdir(`${MOUNT}/dir`);
    // Create two files in the directory
    for (const [name, seed] of [["a.dat", 0x11], ["b.dat", 0x22]] as const) {
      const fd = FS.open(`${MOUNT}/dir/${name}`, O.RDWR | O.CREAT, 0o666);
      FS.write(fd, fillPattern(PAGE_SIZE, seed), 0, PAGE_SIZE, 0);
      FS.close(fd);
    }

    // Directory round-trip
    FS.rename(`${MOUNT}/dir`, `${MOUNT}/tmp_dir`);
    FS.rename(`${MOUNT}/tmp_dir`, `${MOUNT}/dir`);

    syncfs(FS, tomefs);

    const m2 = await mountTome(backend, 4);
    const buf = new Uint8Array(PAGE_SIZE);

    for (const [name, seed] of [["a.dat", 0x11], ["b.dat", 0x22]] as const) {
      const fd = m2.FS.open(`${MOUNT}/dir/${name}`, O.RDONLY);
      m2.FS.read(fd, buf, 0, PAGE_SIZE, 0);
      m2.FS.close(fd);
      expect(verifyPattern(buf, PAGE_SIZE, seed)).toBe(true);
    }

    expect(() => m2.FS.stat(`${MOUNT}/tmp_dir`)).toThrow();
  });

  it("WAL recycling pattern: rename away + overwrite content + rename back", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    // Create WAL segment with original content
    const fd = FS.open(`${MOUNT}/wal.001`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, fillPattern(PAGE_SIZE * 2, 0xAA), 0, PAGE_SIZE * 2, 0);
    FS.close(fd);
    syncfs(FS, tomefs);

    // Recycle: rename to temp, truncate, write new content, rename back
    FS.rename(`${MOUNT}/wal.001`, `${MOUNT}/wal.recycle`);

    // Truncate and write new content
    FS.truncate(`${MOUNT}/wal.recycle`, 0);
    const fd2 = FS.open(`${MOUNT}/wal.recycle`, O.WRONLY);
    FS.write(fd2, fillPattern(PAGE_SIZE * 2, 0xBB), 0, PAGE_SIZE * 2, 0);
    FS.close(fd2);

    // Rename back as a "new" WAL segment
    FS.rename(`${MOUNT}/wal.recycle`, `${MOUNT}/wal.001`);

    syncfs(FS, tomefs);

    const m2 = await mountTome(backend, 4);
    expect(m2.FS.stat(`${MOUNT}/wal.001`).size).toBe(PAGE_SIZE * 2);

    const buf = new Uint8Array(PAGE_SIZE * 2);
    const fd3 = m2.FS.open(`${MOUNT}/wal.001`, O.RDONLY);
    m2.FS.read(fd3, buf, 0, PAGE_SIZE * 2, 0);
    m2.FS.close(fd3);
    // Must have NEW content (0xBB), not original (0xAA)
    expect(verifyPattern(buf, PAGE_SIZE * 2, 0xBB)).toBe(true);

    expect(() => m2.FS.stat(`${MOUNT}/wal.recycle`)).toThrow();
  });

  it("safe-write via rename round-trip: tmp→target with pre-existing target", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    // Create target file with original content
    const fd = FS.open(`${MOUNT}/config`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, fillPattern(PAGE_SIZE, 0x10), 0, PAGE_SIZE, 0);
    FS.close(fd);
    syncfs(FS, tomefs);

    // Safe-write pattern: write new content to tmp, rename old→backup,
    // rename tmp→target, delete backup
    const fd2 = FS.open(`${MOUNT}/config.tmp`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd2, fillPattern(PAGE_SIZE, 0x20), 0, PAGE_SIZE, 0);
    FS.close(fd2);

    FS.rename(`${MOUNT}/config`, `${MOUNT}/config.bak`);
    FS.rename(`${MOUNT}/config.tmp`, `${MOUNT}/config`);
    FS.unlink(`${MOUNT}/config.bak`);

    syncfs(FS, tomefs);

    const m2 = await mountTome(backend, 4);
    const buf = new Uint8Array(PAGE_SIZE);
    const fd3 = m2.FS.open(`${MOUNT}/config`, O.RDONLY);
    m2.FS.read(fd3, buf, 0, PAGE_SIZE, 0);
    m2.FS.close(fd3);
    expect(verifyPattern(buf, PAGE_SIZE, 0x20)).toBe(true);

    expect(() => m2.FS.stat(`${MOUNT}/config.tmp`)).toThrow();
    expect(() => m2.FS.stat(`${MOUNT}/config.bak`)).toThrow();
  });
});
