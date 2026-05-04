/**
 * Adversarial tests: symlink target persistence through dirty shutdown.
 *
 * The dirty-shutdown fuzz test creates and manipulates symlinks but its
 * recovery verification only calls lstat() — never readlink(). This means
 * a symlink whose metadata survives with a corrupted or empty link target
 * would pass all existing verification checks.
 *
 * These tests explicitly verify that readlink() returns the correct target
 * after various crash and recovery scenarios:
 *   - Symlink survives clean syncfs → remount (baseline)
 *   - Synced symlink survives dirty shutdown with intervening file writes
 *   - Directory rename preserves symlink targets (eager metadata write)
 *   - Rename-over-existing-symlink preserves new target
 *   - Symlink created during dirty phase is correctly lost (not corrupted)
 *   - Many symlinks under cache pressure all preserve targets
 *   - Symlink with long target path preserves exact bytes
 *   - Symlink and its target file both survive rename + dirty shutdown
 *
 * Ethos §9: "Write tests designed to break tomefs specifically — target
 * the seams: metadata updates after flush"
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

async function mountTome(backend: SyncMemoryBackend, maxPages = 4096) {
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

function doSyncfs(rawFS: any): void {
  rawFS.syncfs(false, (err: Error | null) => {
    if (err) throw err;
  });
}

function writeFile(FS: any, path: string, data: Uint8Array): void {
  const fd = FS.open(path, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
  if (data.length > 0) FS.write(fd, data, 0, data.length, 0);
  FS.close(fd);
}

function readFile(FS: any, path: string): Uint8Array {
  const stat = FS.stat(path);
  const buf = new Uint8Array(stat.size);
  if (stat.size > 0) {
    const fd = FS.open(path, O.RDONLY);
    FS.read(fd, buf, 0, stat.size, 0);
    FS.close(fd);
  }
  return buf;
}

describe("adversarial: symlink target persistence through dirty shutdown", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("symlink target survives clean syncfs → remount @fast", async () => {
    const { FS } = await mountTome(backend);
    writeFile(FS, `${MOUNT}/target.dat`, fillPattern(100, 1));
    FS.symlink(`${MOUNT}/target.dat`, `${MOUNT}/link`);
    doSyncfs(FS);

    const { FS: FS2 } = await mountTome(backend);
    expect(FS2.readlink(`${MOUNT}/link`)).toBe(`${MOUNT}/target.dat`);
    const data = readFile(FS2, `${MOUNT}/link`);
    expect(verifyPattern(data, 100, 1)).toBe(true);
  });

  it("synced symlink survives dirty shutdown with intervening writes @fast", async () => {
    const { FS } = await mountTome(backend, 4);

    writeFile(FS, `${MOUNT}/real.dat`, fillPattern(PAGE_SIZE, 10));
    FS.symlink(`${MOUNT}/real.dat`, `${MOUNT}/lnk`);
    doSyncfs(FS);

    // Dirty phase: write enough to cause eviction but don't syncfs
    writeFile(FS, `${MOUNT}/big.dat`, fillPattern(PAGE_SIZE * 3, 20));
    writeFile(FS, `${MOUNT}/other.dat`, fillPattern(PAGE_SIZE * 2, 30));

    // Dirty shutdown — remount without syncfs
    const { FS: FS2 } = await mountTome(backend, 4);
    expect(FS2.readlink(`${MOUNT}/lnk`)).toBe(`${MOUNT}/real.dat`);
    const data = readFile(FS2, `${MOUNT}/lnk`);
    expect(verifyPattern(data, PAGE_SIZE, 10)).toBe(true);
  });

  it("directory rename preserves symlink targets via eager metadata write @fast", async () => {
    const { FS } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/dir`);
    FS.mkdir(`${MOUNT}/newdir`);
    writeFile(FS, `${MOUNT}/dir/file.dat`, fillPattern(200, 5));
    FS.symlink(`${MOUNT}/dir/file.dat`, `${MOUNT}/dir/link`);
    doSyncfs(FS);

    // Rename the directory — this eagerly writes metadata at new paths
    FS.rename(`${MOUNT}/dir`, `${MOUNT}/newdir/sub`);

    // Dirty shutdown — no syncfs after rename
    const { FS: FS2 } = await mountTome(backend);
    const target = FS2.readlink(`${MOUNT}/newdir/sub/link`);
    // Symlink target string is preserved exactly — it still points to the
    // OLD path. POSIX symlinks store the literal string, not a resolved reference.
    expect(target).toBe(`${MOUNT}/dir/file.dat`);

    // The file data was moved to the new location
    const data = readFile(FS2, `${MOUNT}/newdir/sub/file.dat`);
    expect(verifyPattern(data, 200, 5)).toBe(true);
  });

  it("symlink created during dirty phase is lost on crash (not corrupted)", async () => {
    const { FS } = await mountTome(backend);
    writeFile(FS, `${MOUNT}/base.dat`, fillPattern(50, 1));
    doSyncfs(FS);

    // Create symlink after last syncfs
    FS.symlink(`${MOUNT}/base.dat`, `${MOUNT}/dirty_link`);

    // Dirty shutdown
    const { FS: FS2 } = await mountTome(backend);

    // Symlink should not exist (never synced)
    expect(() => FS2.lstat(`${MOUNT}/dirty_link`)).toThrow();

    // Base file should still be readable
    const data = readFile(FS2, `${MOUNT}/base.dat`);
    expect(verifyPattern(data, 50, 1)).toBe(true);
  });

  it("rename-over-existing-symlink preserves file data after crash @fast", async () => {
    const { FS } = await mountTome(backend);

    writeFile(FS, `${MOUNT}/a.dat`, fillPattern(300, 1));
    writeFile(FS, `${MOUNT}/b.dat`, fillPattern(400, 2));
    FS.symlink(`${MOUNT}/a.dat`, `${MOUNT}/link_to_a`);
    doSyncfs(FS);

    // Rename file over the symlink — the symlink is replaced by the file
    FS.rename(`${MOUNT}/b.dat`, `${MOUNT}/link_to_a`);

    // Dirty shutdown
    const { FS: FS2 } = await mountTome(backend);

    // link_to_a should now be a regular file (rename overwrote the symlink)
    const stat = FS2.lstat(`${MOUNT}/link_to_a`);
    expect(FS2.isFile(stat.mode)).toBe(true);
    const data = readFile(FS2, `${MOUNT}/link_to_a`);
    expect(verifyPattern(data, 400, 2)).toBe(true);

    // a.dat should still exist
    const aData = readFile(FS2, `${MOUNT}/a.dat`);
    expect(verifyPattern(aData, 300, 1)).toBe(true);
  });

  it("many symlinks under 4-page cache pressure all preserve targets", async () => {
    const { FS } = await mountTome(backend, 4);
    const targets: string[] = [];

    // Create files that exceed cache capacity
    for (let i = 0; i < 6; i++) {
      const path = `${MOUNT}/file_${i}.dat`;
      writeFile(FS, path, fillPattern(PAGE_SIZE, i * 10));
      targets.push(path);
    }

    // Create symlinks to each file
    for (let i = 0; i < 6; i++) {
      FS.symlink(targets[i], `${MOUNT}/sym_${i}`);
    }

    doSyncfs(FS);

    // Dirty phase: write more data to thrash the cache
    writeFile(FS, `${MOUNT}/thrash.dat`, fillPattern(PAGE_SIZE * 3, 99));

    // Dirty shutdown
    const { FS: FS2 } = await mountTome(backend, 4);

    // All synced symlinks should have correct targets
    for (let i = 0; i < 6; i++) {
      expect(FS2.readlink(`${MOUNT}/sym_${i}`)).toBe(targets[i]);
    }

    // All target files should have correct data
    for (let i = 0; i < 6; i++) {
      const data = readFile(FS2, targets[i]);
      expect(verifyPattern(data, PAGE_SIZE, i * 10)).toBe(true);
    }
  });

  it("symlink with long path target preserves exact bytes", async () => {
    const { FS } = await mountTome(backend);

    // Create a deeply nested target path
    const dirs = ["a", "bb", "ccc", "dddd", "eeeee"];
    let targetDir = MOUNT;
    for (const d of dirs) {
      targetDir += `/${d}`;
      FS.mkdir(targetDir);
    }
    const targetPath = `${targetDir}/deeply_nested_file.dat`;
    writeFile(FS, targetPath, fillPattern(10, 42));

    FS.symlink(targetPath, `${MOUNT}/shortcut`);
    doSyncfs(FS);

    const { FS: FS2 } = await mountTome(backend);
    expect(FS2.readlink(`${MOUNT}/shortcut`)).toBe(targetPath);
    const data = readFile(FS2, `${MOUNT}/shortcut`);
    expect(verifyPattern(data, 10, 42)).toBe(true);
  });

  it("symlink renamed between directories preserves target after dirty shutdown", async () => {
    const { FS } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/src`);
    FS.mkdir(`${MOUNT}/dst`);
    writeFile(FS, `${MOUNT}/target.dat`, fillPattern(150, 7));
    FS.symlink(`${MOUNT}/target.dat`, `${MOUNT}/src/mylink`);
    doSyncfs(FS);

    // Rename symlink to different directory
    FS.rename(`${MOUNT}/src/mylink`, `${MOUNT}/dst/moved_link`);

    // Dirty shutdown
    const { FS: FS2 } = await mountTome(backend);

    // Old location should not exist
    expect(() => FS2.lstat(`${MOUNT}/src/mylink`)).toThrow();

    // New location should have correct target
    expect(FS2.readlink(`${MOUNT}/dst/moved_link`)).toBe(`${MOUNT}/target.dat`);
    const data = readFile(FS2, `${MOUNT}/dst/moved_link`);
    expect(verifyPattern(data, 150, 7)).toBe(true);
  });

  it("symlink and target file both survive directory rename + dirty shutdown", async () => {
    const { FS } = await mountTome(backend, 4);

    FS.mkdir(`${MOUNT}/app`);
    writeFile(FS, `${MOUNT}/app/data.dat`, fillPattern(PAGE_SIZE + 500, 3));
    FS.symlink(`${MOUNT}/app/data.dat`, `${MOUNT}/app/data_link`);
    doSyncfs(FS);

    // Rename the entire directory
    FS.rename(`${MOUNT}/app`, `${MOUNT}/moved_app`);

    // Write more data to force eviction (4-page cache)
    writeFile(FS, `${MOUNT}/filler.dat`, fillPattern(PAGE_SIZE * 3, 88));

    // Dirty shutdown
    const { FS: FS2 } = await mountTome(backend, 4);

    // Symlink target is the literal string — still points to old path
    expect(FS2.readlink(`${MOUNT}/moved_app/data_link`)).toBe(
      `${MOUNT}/app/data.dat`,
    );

    // The file itself was moved — reading the old target through the
    // symlink will fail, but reading directly at the new path works
    const data = readFile(FS2, `${MOUNT}/moved_app/data.dat`);
    expect(verifyPattern(data, PAGE_SIZE + 500, 3)).toBe(true);
  });

  it("orphan cleanup after dirty shutdown does not destroy synced symlinks @fast", async () => {
    const { FS } = await mountTome(backend);

    writeFile(FS, `${MOUNT}/target.dat`, fillPattern(100, 1));
    FS.symlink(`${MOUNT}/target.dat`, `${MOUNT}/link1`);
    FS.symlink(`${MOUNT}/target.dat`, `${MOUNT}/link2`);
    doSyncfs(FS);

    // Force needsOrphanCleanup by creating an unlinked-with-open-fd
    // scenario. rename() eagerly writes metadata then triggers orphan
    // cleanup on the next syncfs.
    writeFile(FS, `${MOUNT}/temp.dat`, fillPattern(50, 9));
    const fd = FS.open(`${MOUNT}/temp.dat`, O.RDONLY);
    FS.unlink(`${MOUNT}/temp.dat`);

    // Dirty shutdown with an open fd on an unlinked file
    // This leaves a /__deleted_* entry in the backend
    const { FS: FS2 } = await mountTome(backend);

    // Verify symlinks survived (orphan cleanup should NOT touch them)
    expect(FS2.readlink(`${MOUNT}/link1`)).toBe(`${MOUNT}/target.dat`);
    expect(FS2.readlink(`${MOUNT}/link2`)).toBe(`${MOUNT}/target.dat`);

    // Do a clean syncfs to trigger orphan cleanup
    doSyncfs(FS2);

    // Verify symlinks still intact after orphan cleanup
    const { FS: FS3 } = await mountTome(backend);
    expect(FS3.readlink(`${MOUNT}/link1`)).toBe(`${MOUNT}/target.dat`);
    expect(FS3.readlink(`${MOUNT}/link2`)).toBe(`${MOUNT}/target.dat`);

    // Verify no orphans remain
    const paths = backend.listFiles();
    const orphans = paths.filter((p: string) => p.startsWith("/__deleted_"));
    expect(orphans).toHaveLength(0);
  });

  it("symlink created between two syncfs cycles persists through dirty shutdown @fast", async () => {
    const { FS } = await mountTome(backend);

    writeFile(FS, `${MOUNT}/a.dat`, fillPattern(100, 1));
    doSyncfs(FS);

    // Second cycle: create symlink, then sync
    FS.symlink(`${MOUNT}/a.dat`, `${MOUNT}/delayed_link`);
    doSyncfs(FS);

    // Dirty phase: append to the target file (extends without truncating)
    const appendData = fillPattern(50, 99);
    const fd = FS.open(`${MOUNT}/a.dat`, O.WRONLY);
    FS.write(fd, appendData, 0, appendData.length, 100);
    FS.close(fd);

    // Dirty shutdown
    const { FS: FS2 } = await mountTome(backend);

    // Symlink was synced in the second cycle — should survive
    expect(FS2.readlink(`${MOUNT}/delayed_link`)).toBe(`${MOUNT}/a.dat`);

    // Target file should be at least its last-synced size (100 bytes).
    // The dirty append may or may not have been flushed by eviction.
    const data = readFile(FS2, `${MOUNT}/a.dat`);
    expect(data.length).toBeGreaterThanOrEqual(100);
    expect(verifyPattern(data, 100, 1)).toBe(true);
  });

  it("readlink returns correct target after multiple rename cycles + recovery", async () => {
    const { FS } = await mountTome(backend);

    writeFile(FS, `${MOUNT}/orig.dat`, fillPattern(100, 1));
    FS.symlink(`${MOUNT}/orig.dat`, `${MOUNT}/link`);
    doSyncfs(FS);

    // Multiple rename cycles (each eagerly writes metadata)
    FS.rename(`${MOUNT}/link`, `${MOUNT}/link_v2`);
    FS.rename(`${MOUNT}/link_v2`, `${MOUNT}/link_v3`);
    FS.rename(`${MOUNT}/link_v3`, `${MOUNT}/final_link`);

    // Dirty shutdown
    const { FS: FS2 } = await mountTome(backend);

    // Old paths should not exist
    expect(() => FS2.lstat(`${MOUNT}/link`)).toThrow();
    expect(() => FS2.lstat(`${MOUNT}/link_v2`)).toThrow();
    expect(() => FS2.lstat(`${MOUNT}/link_v3`)).toThrow();

    // Final location should have correct target
    expect(FS2.readlink(`${MOUNT}/final_link`)).toBe(`${MOUNT}/orig.dat`);
  });

  it("symlink target with relative path components preserved exactly", async () => {
    const { FS } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/dir`);
    writeFile(FS, `${MOUNT}/dir/file.dat`, fillPattern(50, 1));

    // Create symlink with relative target (Emscripten stores literal string)
    FS.symlink("dir/file.dat", `${MOUNT}/rel_link`);
    doSyncfs(FS);

    const { FS: FS2 } = await mountTome(backend);
    expect(FS2.readlink(`${MOUNT}/rel_link`)).toBe("dir/file.dat");
  });

  it("symlink survives recovery + clean sync + another recovery cycle @fast", async () => {
    const { FS } = await mountTome(backend, 4);

    writeFile(FS, `${MOUNT}/stable.dat`, fillPattern(PAGE_SIZE, 42));
    FS.symlink(`${MOUNT}/stable.dat`, `${MOUNT}/stable_link`);
    doSyncfs(FS);

    // Dirty writes + shutdown
    writeFile(FS, `${MOUNT}/noise.dat`, fillPattern(PAGE_SIZE * 3, 77));
    const { FS: FS2 } = await mountTome(backend, 4);

    // Recovery sync
    doSyncfs(FS2);
    expect(FS2.readlink(`${MOUNT}/stable_link`)).toBe(`${MOUNT}/stable.dat`);

    // More dirty writes + second shutdown
    writeFile(FS2, `${MOUNT}/noise2.dat`, fillPattern(PAGE_SIZE * 2, 88));
    const { FS: FS3 } = await mountTome(backend, 4);

    // Verify symlink still intact after second recovery
    expect(FS3.readlink(`${MOUNT}/stable_link`)).toBe(`${MOUNT}/stable.dat`);

    // Recovery sync + final verification
    doSyncfs(FS3);
    const { FS: FS4 } = await mountTome(backend, 4);
    expect(FS4.readlink(`${MOUNT}/stable_link`)).toBe(`${MOUNT}/stable.dat`);

    const data = readFile(FS4, `${MOUNT}/stable.dat`);
    expect(verifyPattern(data, PAGE_SIZE, 42)).toBe(true);
  });
});
