/**
 * Adversarial tests: Rename and unlink with persistence round-trips.
 *
 * Existing rename adversarial tests (rename-cache, rename-open-fd) verify
 * in-memory correctness only. This file tests the full pipeline: rename →
 * syncfs → unmount → remount → verify data. This catches bugs in how
 * rename updates backend storage keys, metadata paths, and page cache
 * entries across persistence boundaries.
 *
 * Real-world motivation: Postgres renames WAL segments during rotation,
 * promotes temp files by renaming them into the data directory, and uses
 * the safe-write pattern (write tmp, rename over original). All of these
 * must survive a syncfs + process restart without data loss.
 *
 * Ethos §9: "Write tests designed to break tomefs specifically — things
 * that pass against MEMFS but expose real bugs in the page cache layer."
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
const MAX_PAGES = 4; // 32 KB cache — extreme eviction pressure

/** Fill a buffer with a deterministic pattern based on a seed byte. */
function fillPattern(size: number, seed: number): Uint8Array {
  const buf = new Uint8Array(size);
  for (let i = 0; i < size; i++) {
    buf[i] = (seed + i * 31) & 0xff;
  }
  return buf;
}

/** Verify a buffer matches the expected pattern. */
function verifyPattern(buf: Uint8Array, size: number, seed: number): void {
  for (let i = 0; i < size; i++) {
    if (buf[i] !== ((seed + i * 31) & 0xff)) {
      throw new Error(
        `Byte ${i}: expected ${(seed + i * 31) & 0xff}, got ${buf[i]}`,
      );
    }
  }
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

describe("adversarial: rename with persistence round-trips", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  // ------------------------------------------------------------------
  // Basic rename persists data under new name
  // ------------------------------------------------------------------

  it("renamed file data survives syncfs + remount @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const data = fillPattern(PAGE_SIZE * 2, 42);
    const fd = FS.open(`${MOUNT}/original`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, data, 0, data.length, 0);
    FS.close(fd);

    FS.rename(`${MOUNT}/original`, `${MOUNT}/renamed`);
    syncAndUnmount(FS, tomefs);

    // Remount and verify
    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/renamed`);
    expect(stat.size).toBe(PAGE_SIZE * 2);

    const buf = new Uint8Array(PAGE_SIZE * 2);
    const rd = FS2.open(`${MOUNT}/renamed`, O.RDONLY);
    FS2.read(rd, buf, 0, buf.length, 0);
    FS2.close(rd);
    verifyPattern(buf, PAGE_SIZE * 2, 42);

    // Old path must not exist
    expect(() => FS2.stat(`${MOUNT}/original`)).toThrow();
  });

  // ------------------------------------------------------------------
  // Rename over existing file: target replaced, survives remount
  // ------------------------------------------------------------------

  it("rename over existing file replaces data and persists", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create target with 2 pages of 0xAA
    const targetData = new Uint8Array(PAGE_SIZE * 2);
    targetData.fill(0xaa);
    const t = FS.open(`${MOUNT}/target`, O.RDWR | O.CREAT, 0o666);
    FS.write(t, targetData, 0, targetData.length, 0);
    FS.close(t);

    // Create source with 1 page of pattern
    const sourceData = fillPattern(PAGE_SIZE, 77);
    const s = FS.open(`${MOUNT}/source`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, sourceData, 0, sourceData.length, 0);
    FS.close(s);

    // Rename source over target
    FS.rename(`${MOUNT}/source`, `${MOUNT}/target`);
    syncAndUnmount(FS, tomefs);

    // Remount: /target should have source data, not target data
    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/target`);
    expect(stat.size).toBe(PAGE_SIZE); // source was 1 page, not 2

    const buf = new Uint8Array(PAGE_SIZE);
    const rd = FS2.open(`${MOUNT}/target`, O.RDONLY);
    FS2.read(rd, buf, 0, PAGE_SIZE, 0);
    FS2.close(rd);
    verifyPattern(buf, PAGE_SIZE, 77);

    // Source path must not exist
    expect(() => FS2.stat(`${MOUNT}/source`)).toThrow();
  });

  // ------------------------------------------------------------------
  // Rename chain A → B → C persists correctly
  // ------------------------------------------------------------------

  it("chained renames persist final location correctly", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const data = fillPattern(PAGE_SIZE + 100, 13);
    const fd = FS.open(`${MOUNT}/hop0`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, data, 0, data.length, 0);
    FS.close(fd);

    FS.rename(`${MOUNT}/hop0`, `${MOUNT}/hop1`);
    FS.rename(`${MOUNT}/hop1`, `${MOUNT}/hop2`);
    FS.rename(`${MOUNT}/hop2`, `${MOUNT}/final`);

    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/final`);
    expect(stat.size).toBe(PAGE_SIZE + 100);

    const buf = new Uint8Array(PAGE_SIZE + 100);
    const rd = FS2.open(`${MOUNT}/final`, O.RDONLY);
    FS2.read(rd, buf, 0, buf.length, 0);
    FS2.close(rd);
    verifyPattern(buf, PAGE_SIZE + 100, 13);

    // Intermediate paths must not exist
    expect(() => FS2.stat(`${MOUNT}/hop0`)).toThrow();
    expect(() => FS2.stat(`${MOUNT}/hop1`)).toThrow();
    expect(() => FS2.stat(`${MOUNT}/hop2`)).toThrow();
  });

  // ------------------------------------------------------------------
  // Rename then create new file at old path — both persist independently
  // ------------------------------------------------------------------

  it("new file at old path after rename: both files persist", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create /slot with pattern seed=10
    const data1 = fillPattern(PAGE_SIZE, 10);
    const fd1 = FS.open(`${MOUNT}/slot`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd1, data1, 0, data1.length, 0);
    FS.close(fd1);

    // Rename /slot → /archive
    FS.rename(`${MOUNT}/slot`, `${MOUNT}/archive`);

    // Create NEW /slot with pattern seed=20
    const data2 = fillPattern(PAGE_SIZE, 20);
    const fd2 = FS.open(`${MOUNT}/slot`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd2, data2, 0, data2.length, 0);
    FS.close(fd2);

    syncAndUnmount(FS, tomefs);

    // Remount and verify both files are independent
    const { FS: FS2 } = await mountTome(backend);

    const bufArchive = new Uint8Array(PAGE_SIZE);
    const rA = FS2.open(`${MOUNT}/archive`, O.RDONLY);
    FS2.read(rA, bufArchive, 0, PAGE_SIZE, 0);
    FS2.close(rA);
    verifyPattern(bufArchive, PAGE_SIZE, 10);

    const bufSlot = new Uint8Array(PAGE_SIZE);
    const rS = FS2.open(`${MOUNT}/slot`, O.RDONLY);
    FS2.read(rS, bufSlot, 0, PAGE_SIZE, 0);
    FS2.close(rS);
    verifyPattern(bufSlot, PAGE_SIZE, 20);
  });

  // ------------------------------------------------------------------
  // Safe-write pattern with persistence: write tmp, rename over original
  // ------------------------------------------------------------------

  it("safe-write pattern (write tmp + rename over) persists through multiple cycles", async () => {
    // This mirrors Postgres's strategy for updating config files and
    // pg_control: write new data to a .tmp, fsync, rename over original.
    const { FS, tomefs } = await mountTome(backend);

    // Create initial file
    const v0 = fillPattern(PAGE_SIZE, 0);
    const fd = FS.open(`${MOUNT}/data`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, v0, 0, v0.length, 0);
    FS.close(fd);

    // 5 rounds of safe-write + sync
    for (let version = 1; version <= 5; version++) {
      const tmp = FS.open(
        `${MOUNT}/data.tmp`,
        O.RDWR | O.CREAT | O.TRUNC,
        0o666,
      );
      const content = fillPattern(PAGE_SIZE, version);
      FS.write(tmp, content, 0, content.length, 0);
      FS.close(tmp);

      FS.rename(`${MOUNT}/data.tmp`, `${MOUNT}/data`);
      syncfs(FS, tomefs);
    }

    FS.unmount(MOUNT);

    // Remount and verify final version
    const { FS: FS2 } = await mountTome(backend);
    const buf = new Uint8Array(PAGE_SIZE);
    const rd = FS2.open(`${MOUNT}/data`, O.RDONLY);
    FS2.read(rd, buf, 0, PAGE_SIZE, 0);
    FS2.close(rd);
    verifyPattern(buf, PAGE_SIZE, 5);

    // .tmp should not exist
    expect(() => FS2.stat(`${MOUNT}/data.tmp`)).toThrow();
  });

  // ------------------------------------------------------------------
  // Rename across directories with persistence
  // ------------------------------------------------------------------

  it("rename across directories persists under new parent", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/src`);
    FS.mkdir(`${MOUNT}/dst`);

    const data = fillPattern(PAGE_SIZE * 3, 99);
    const fd = FS.open(`${MOUNT}/src/bigfile`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, data, 0, data.length, 0);
    FS.close(fd);

    FS.rename(`${MOUNT}/src/bigfile`, `${MOUNT}/dst/bigfile`);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/dst/bigfile`);
    expect(stat.size).toBe(PAGE_SIZE * 3);

    const buf = new Uint8Array(PAGE_SIZE * 3);
    const rd = FS2.open(`${MOUNT}/dst/bigfile`, O.RDONLY);
    FS2.read(rd, buf, 0, buf.length, 0);
    FS2.close(rd);
    verifyPattern(buf, PAGE_SIZE * 3, 99);

    expect(() => FS2.stat(`${MOUNT}/src/bigfile`)).toThrow();
  });

  // ------------------------------------------------------------------
  // Directory rename with files — all descendants persist
  // ------------------------------------------------------------------

  it("directory rename persists all descendant files", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/olddir`);

    // Create 3 files with distinct patterns
    for (let i = 0; i < 3; i++) {
      const data = fillPattern(PAGE_SIZE, 50 + i);
      const fd = FS.open(
        `${MOUNT}/olddir/file${i}`,
        O.RDWR | O.CREAT,
        0o666,
      );
      FS.write(fd, data, 0, data.length, 0);
      FS.close(fd);
    }

    FS.rename(`${MOUNT}/olddir`, `${MOUNT}/newdir`);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    expect(() => FS2.stat(`${MOUNT}/olddir`)).toThrow();

    for (let i = 0; i < 3; i++) {
      const stat = FS2.stat(`${MOUNT}/newdir/file${i}`);
      expect(stat.size).toBe(PAGE_SIZE);

      const buf = new Uint8Array(PAGE_SIZE);
      const rd = FS2.open(`${MOUNT}/newdir/file${i}`, O.RDONLY);
      FS2.read(rd, buf, 0, PAGE_SIZE, 0);
      FS2.close(rd);
      verifyPattern(buf, PAGE_SIZE, 50 + i);
    }
  });

  // ------------------------------------------------------------------
  // Rename with dirty pages under cache pressure + persistence
  // ------------------------------------------------------------------

  it("rename with dirty pages under cache pressure persists all data", async () => {
    // Write 6 pages (exceeding 4-page cache), rename, sync, remount.
    // Pages evicted before rename must end up under the new path.
    const { FS, tomefs } = await mountTome(backend);

    const data = fillPattern(PAGE_SIZE * 6, 33);
    const fd = FS.open(`${MOUNT}/pressure`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, data, 0, data.length, 0);
    FS.close(fd);

    FS.rename(`${MOUNT}/pressure`, `${MOUNT}/moved`);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/moved`);
    expect(stat.size).toBe(PAGE_SIZE * 6);

    const buf = new Uint8Array(PAGE_SIZE * 6);
    const rd = FS2.open(`${MOUNT}/moved`, O.RDONLY);
    FS2.read(rd, buf, 0, buf.length, 0);
    FS2.close(rd);
    verifyPattern(buf, PAGE_SIZE * 6, 33);
  });

  // ------------------------------------------------------------------
  // WAL rotation: append → rename → new file → repeat → persist
  // ------------------------------------------------------------------

  it("WAL rotation pattern persists all segments", async () => {
    // Simulates Postgres WAL segment rotation:
    // 1. Write WAL records to /wal
    // 2. Rename /wal to /wal.N (archive segment)
    // 3. Create new /wal
    // 4. Repeat
    // After syncing and remounting, all segments must survive.
    const { FS, tomefs } = await mountTome(backend);

    const segments = 4;
    for (let seg = 0; seg < segments; seg++) {
      const walFd = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);
      const data = fillPattern(PAGE_SIZE, 100 + seg);
      FS.write(walFd, data, 0, data.length, 0);
      FS.close(walFd);

      FS.rename(`${MOUNT}/wal`, `${MOUNT}/wal.${seg}`);
    }

    syncAndUnmount(FS, tomefs);

    // Remount and verify all archived segments
    const { FS: FS2 } = await mountTome(backend);
    for (let seg = 0; seg < segments; seg++) {
      const stat = FS2.stat(`${MOUNT}/wal.${seg}`);
      expect(stat.size).toBe(PAGE_SIZE);

      const buf = new Uint8Array(PAGE_SIZE);
      const rd = FS2.open(`${MOUNT}/wal.${seg}`, O.RDONLY);
      FS2.read(rd, buf, 0, PAGE_SIZE, 0);
      FS2.close(rd);
      verifyPattern(buf, PAGE_SIZE, 100 + seg);
    }

    // Current /wal should not exist (last one was renamed)
    expect(() => FS2.stat(`${MOUNT}/wal`)).toThrow();
  });

  // ------------------------------------------------------------------
  // Unlink + recreate at same path: no data leakage across remount
  // ------------------------------------------------------------------

  it("unlink + recreate at same path: no data leakage after remount", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create file with old data
    const oldData = new Uint8Array(PAGE_SIZE * 2);
    oldData.fill(0xaa);
    const fd1 = FS.open(`${MOUNT}/reused`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd1, oldData, 0, oldData.length, 0);
    FS.close(fd1);

    // Unlink
    FS.unlink(`${MOUNT}/reused`);

    // Recreate with smaller, different data
    const newData = fillPattern(100, 55);
    const fd2 = FS.open(`${MOUNT}/reused`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd2, newData, 0, newData.length, 0);
    FS.close(fd2);

    syncAndUnmount(FS, tomefs);

    // Remount: must see new data only, no leaked pages from old file
    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/reused`);
    expect(stat.size).toBe(100);

    const buf = new Uint8Array(100);
    const rd = FS2.open(`${MOUNT}/reused`, O.RDONLY);
    FS2.read(rd, buf, 0, 100, 0);
    FS2.close(rd);
    verifyPattern(buf, 100, 55);
  });

  // ------------------------------------------------------------------
  // Rename + write through old fd + persist
  // ------------------------------------------------------------------

  it("write through fd after rename persists under new name", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const fd = FS.open(`${MOUNT}/wrfd`, O.RDWR | O.CREAT, 0o666);
    const part1 = fillPattern(PAGE_SIZE, 11);
    FS.write(fd, part1, 0, part1.length, 0);

    // Rename while fd is open
    FS.rename(`${MOUNT}/wrfd`, `${MOUNT}/wrfd_moved`);

    // Write more through the old fd
    const part2 = fillPattern(PAGE_SIZE, 22);
    FS.write(fd, part2, 0, part2.length, PAGE_SIZE);
    FS.close(fd);

    syncAndUnmount(FS, tomefs);

    // Remount and verify combined data under new name
    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/wrfd_moved`);
    expect(stat.size).toBe(PAGE_SIZE * 2);

    const buf = new Uint8Array(PAGE_SIZE * 2);
    const rd = FS2.open(`${MOUNT}/wrfd_moved`, O.RDONLY);
    FS2.read(rd, buf, 0, buf.length, 0);
    FS2.close(rd);
    verifyPattern(buf.subarray(0, PAGE_SIZE), PAGE_SIZE, 11);
    verifyPattern(buf.subarray(PAGE_SIZE), PAGE_SIZE, 22);

    expect(() => FS2.stat(`${MOUNT}/wrfd`)).toThrow();
  });

  // ------------------------------------------------------------------
  // Rename with syncfs between operations (incremental persistence)
  // ------------------------------------------------------------------

  it("rename with syncfs between each step persists incrementally", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Step 1: create file
    const data = fillPattern(PAGE_SIZE, 60);
    const fd = FS.open(`${MOUNT}/step`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, data, 0, data.length, 0);
    FS.close(fd);
    syncfs(FS, tomefs);

    // Step 2: rename
    FS.rename(`${MOUNT}/step`, `${MOUNT}/step2`);
    syncfs(FS, tomefs);

    // Step 3: rename again
    FS.rename(`${MOUNT}/step2`, `${MOUNT}/step3`);
    syncfs(FS, tomefs);

    FS.unmount(MOUNT);

    // Remount — only step3 should exist
    const { FS: FS2 } = await mountTome(backend);
    const buf = new Uint8Array(PAGE_SIZE);
    const rd = FS2.open(`${MOUNT}/step3`, O.RDONLY);
    FS2.read(rd, buf, 0, PAGE_SIZE, 0);
    FS2.close(rd);
    verifyPattern(buf, PAGE_SIZE, 60);

    expect(() => FS2.stat(`${MOUNT}/step`)).toThrow();
    expect(() => FS2.stat(`${MOUNT}/step2`)).toThrow();
  });

  // ------------------------------------------------------------------
  // Multiple files renamed in same syncfs batch
  // ------------------------------------------------------------------

  it("multiple file renames in same syncfs batch all persist", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create 4 files
    for (let i = 0; i < 4; i++) {
      const data = fillPattern(PAGE_SIZE, i);
      const fd = FS.open(`${MOUNT}/f${i}`, O.RDWR | O.CREAT, 0o666);
      FS.write(fd, data, 0, data.length, 0);
      FS.close(fd);
    }

    // Rename all 4 files before syncing
    for (let i = 0; i < 4; i++) {
      FS.rename(`${MOUNT}/f${i}`, `${MOUNT}/moved_f${i}`);
    }

    syncAndUnmount(FS, tomefs);

    // Remount and verify all 4
    const { FS: FS2 } = await mountTome(backend);
    for (let i = 0; i < 4; i++) {
      const buf = new Uint8Array(PAGE_SIZE);
      const rd = FS2.open(`${MOUNT}/moved_f${i}`, O.RDONLY);
      FS2.read(rd, buf, 0, PAGE_SIZE, 0);
      FS2.close(rd);
      verifyPattern(buf, PAGE_SIZE, i);

      expect(() => FS2.stat(`${MOUNT}/f${i}`)).toThrow();
    }
  });

  // ------------------------------------------------------------------
  // Rename swap pattern: A → tmp, B → A, tmp → B
  // ------------------------------------------------------------------

  it("rename swap (A↔B via tmp) persists correctly", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const dataA = fillPattern(PAGE_SIZE, 1);
    const dataB = fillPattern(PAGE_SIZE, 2);

    const fA = FS.open(`${MOUNT}/fileA`, O.RDWR | O.CREAT, 0o666);
    FS.write(fA, dataA, 0, dataA.length, 0);
    FS.close(fA);

    const fB = FS.open(`${MOUNT}/fileB`, O.RDWR | O.CREAT, 0o666);
    FS.write(fB, dataB, 0, dataB.length, 0);
    FS.close(fB);

    // Swap via temp
    FS.rename(`${MOUNT}/fileA`, `${MOUNT}/tmp`);
    FS.rename(`${MOUNT}/fileB`, `${MOUNT}/fileA`);
    FS.rename(`${MOUNT}/tmp`, `${MOUNT}/fileB`);

    syncAndUnmount(FS, tomefs);

    // After remount: fileA has B's data, fileB has A's data
    const { FS: FS2 } = await mountTome(backend);

    const bufA = new Uint8Array(PAGE_SIZE);
    const rA = FS2.open(`${MOUNT}/fileA`, O.RDONLY);
    FS2.read(rA, bufA, 0, PAGE_SIZE, 0);
    FS2.close(rA);
    verifyPattern(bufA, PAGE_SIZE, 2); // was B's data

    const bufB = new Uint8Array(PAGE_SIZE);
    const rB = FS2.open(`${MOUNT}/fileB`, O.RDONLY);
    FS2.read(rB, bufB, 0, PAGE_SIZE, 0);
    FS2.close(rB);
    verifyPattern(bufB, PAGE_SIZE, 1); // was A's data

    expect(() => FS2.stat(`${MOUNT}/tmp`)).toThrow();
  });

  // ------------------------------------------------------------------
  // Nested directory rename with deep files persists
  // ------------------------------------------------------------------

  it("nested directory rename persists deep descendant files", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/a`);
    FS.mkdir(`${MOUNT}/a/b`);
    FS.mkdir(`${MOUNT}/a/b/c`);

    // File at each level
    const seeds = [70, 71, 72];
    const paths = [
      `${MOUNT}/a/f1`,
      `${MOUNT}/a/b/f2`,
      `${MOUNT}/a/b/c/f3`,
    ];
    for (let i = 0; i < 3; i++) {
      const data = fillPattern(PAGE_SIZE, seeds[i]);
      const fd = FS.open(paths[i], O.RDWR | O.CREAT, 0o666);
      FS.write(fd, data, 0, data.length, 0);
      FS.close(fd);
    }

    // Rename top-level directory
    FS.rename(`${MOUNT}/a`, `${MOUNT}/x`);
    syncAndUnmount(FS, tomefs);

    // Remount and verify all descendants moved
    const { FS: FS2 } = await mountTome(backend);
    const newPaths = [
      `${MOUNT}/x/f1`,
      `${MOUNT}/x/b/f2`,
      `${MOUNT}/x/b/c/f3`,
    ];
    for (let i = 0; i < 3; i++) {
      const buf = new Uint8Array(PAGE_SIZE);
      const rd = FS2.open(newPaths[i], O.RDONLY);
      FS2.read(rd, buf, 0, PAGE_SIZE, 0);
      FS2.close(rd);
      verifyPattern(buf, PAGE_SIZE, seeds[i]);
    }

    expect(() => FS2.stat(`${MOUNT}/a`)).toThrow();
  });

  // ------------------------------------------------------------------
  // Metadata (mode, mtime) preserved through rename + persist
  // ------------------------------------------------------------------

  it("file metadata preserved through rename + persist", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const fd = FS.open(`${MOUNT}/metafile`, O.RDWR | O.CREAT, 0o644);
    const data = fillPattern(100, 88);
    FS.write(fd, data, 0, data.length, 0);
    FS.close(fd);

    // Set specific mode
    FS.chmod(`${MOUNT}/metafile`, 0o755);
    const beforeStat = FS.stat(`${MOUNT}/metafile`);
    const beforeMode = beforeStat.mode;
    const beforeMtime = beforeStat.mtime;

    FS.rename(`${MOUNT}/metafile`, `${MOUNT}/metafile_moved`);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const afterStat = FS2.stat(`${MOUNT}/metafile_moved`);

    // Mode bits (lower 12 bits) must match
    expect(afterStat.mode & 0o7777).toBe(beforeMode & 0o7777);
    // mtime should be preserved (rename updates parent dir mtime, not file mtime)
    expect(afterStat.mtime.getTime()).toBe(beforeMtime.getTime());
  });

  // ------------------------------------------------------------------
  // Rename + truncate + persist: size correct after remount
  // ------------------------------------------------------------------

  it("rename then truncate: correct size after remount", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const data = fillPattern(PAGE_SIZE * 4, 44);
    const fd = FS.open(`${MOUNT}/bigfile`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, data, 0, data.length, 0);
    FS.close(fd);

    FS.rename(`${MOUNT}/bigfile`, `${MOUNT}/truncated`);
    FS.truncate(`${MOUNT}/truncated`, PAGE_SIZE);

    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/truncated`);
    expect(stat.size).toBe(PAGE_SIZE);

    const buf = new Uint8Array(PAGE_SIZE);
    const rd = FS2.open(`${MOUNT}/truncated`, O.RDONLY);
    FS2.read(rd, buf, 0, PAGE_SIZE, 0);
    FS2.close(rd);
    // First page should still have original pattern
    verifyPattern(buf, PAGE_SIZE, 44);
  });

  // ------------------------------------------------------------------
  // Unlink with open fd, then syncfs: /__deleted_* pages cleaned up
  // ------------------------------------------------------------------

  it("unlinked file with open fd: pages cleaned up after fd close + remount", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const data = fillPattern(PAGE_SIZE * 2, 66);
    const fd = FS.open(`${MOUNT}/ephemeral`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, data, 0, data.length, 0);

    // Unlink while fd is open — pages move to /__deleted_*
    FS.unlink(`${MOUNT}/ephemeral`);

    // Data still readable through fd
    const check = new Uint8Array(PAGE_SIZE * 2);
    FS.read(fd, check, 0, check.length, 0);
    verifyPattern(check, PAGE_SIZE * 2, 66);

    // Close fd — should release /__deleted_* pages
    FS.close(fd);

    syncAndUnmount(FS, tomefs);

    // Remount: file should not exist, no leaked /__deleted_* entries
    const { FS: FS2 } = await mountTome(backend);
    expect(() => FS2.stat(`${MOUNT}/ephemeral`)).toThrow();

    // Verify no /__deleted_ files are visible in the root listing
    const listing = FS2.readdir(`${MOUNT}`);
    for (const entry of listing) {
      expect(entry).not.toMatch(/^__deleted_/);
    }
  });

  // ------------------------------------------------------------------
  // Rename over file with open fd + persist
  // ------------------------------------------------------------------

  it("rename over file with open fd: old data accessible, new data persists", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create target with open fd
    const targetFd = FS.open(`${MOUNT}/target`, O.RDWR | O.CREAT, 0o666);
    const targetData = fillPattern(PAGE_SIZE, 30);
    FS.write(targetFd, targetData, 0, targetData.length, 0);

    // Create source
    const sourceData = fillPattern(PAGE_SIZE, 40);
    const sFd = FS.open(`${MOUNT}/source`, O.RDWR | O.CREAT, 0o666);
    FS.write(sFd, sourceData, 0, sourceData.length, 0);
    FS.close(sFd);

    // Rename source over target — target fd still holds old data
    FS.rename(`${MOUNT}/source`, `${MOUNT}/target`);

    // Old fd should still read target's original data
    const oldBuf = new Uint8Array(PAGE_SIZE);
    FS.read(targetFd, oldBuf, 0, PAGE_SIZE, 0);
    verifyPattern(oldBuf, PAGE_SIZE, 30);
    FS.close(targetFd);

    syncAndUnmount(FS, tomefs);

    // Remount: /target should have source's data
    const { FS: FS2 } = await mountTome(backend);
    const buf = new Uint8Array(PAGE_SIZE);
    const rd = FS2.open(`${MOUNT}/target`, O.RDONLY);
    FS2.read(rd, buf, 0, PAGE_SIZE, 0);
    FS2.close(rd);
    verifyPattern(buf, PAGE_SIZE, 40);

    expect(() => FS2.stat(`${MOUNT}/source`)).toThrow();
  });

  // ------------------------------------------------------------------
  // Rename + multiple syncfs cycles without unmount
  // ------------------------------------------------------------------

  it("rename survives multiple syncfs cycles without unmount", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const data = fillPattern(PAGE_SIZE, 80);
    const fd = FS.open(`${MOUNT}/cycling`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, data, 0, data.length, 0);
    FS.close(fd);
    syncfs(FS, tomefs);

    FS.rename(`${MOUNT}/cycling`, `${MOUNT}/cycled`);
    syncfs(FS, tomefs);

    // Modify after rename
    const fd2 = FS.open(`${MOUNT}/cycled`, O.RDWR);
    const patch = new Uint8Array(100);
    patch.fill(0xff);
    FS.write(fd2, patch, 0, 100, 0);
    FS.close(fd2);
    syncfs(FS, tomefs);

    FS.unmount(MOUNT);

    // Remount — should see modified data under /cycled
    const { FS: FS2 } = await mountTome(backend);
    const buf = new Uint8Array(PAGE_SIZE);
    const rd = FS2.open(`${MOUNT}/cycled`, O.RDONLY);
    FS2.read(rd, buf, 0, PAGE_SIZE, 0);
    FS2.close(rd);

    // First 100 bytes: 0xFF (patched)
    for (let i = 0; i < 100; i++) {
      expect(buf[i]).toBe(0xff);
    }
    // Rest: original pattern
    for (let i = 100; i < PAGE_SIZE; i++) {
      expect(buf[i]).toBe((80 + i * 31) & 0xff);
    }

    expect(() => FS2.stat(`${MOUNT}/cycling`)).toThrow();
  });
});
