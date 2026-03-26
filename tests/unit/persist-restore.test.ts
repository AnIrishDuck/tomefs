/**
 * Tests for the persist/restore cycle in tomefs (persistTree + restoreTree).
 *
 * These cover critical paths in the sync→unmount→remount lifecycle that lack
 * dedicated test coverage:
 *   - Symlink persistence and restoration (S_IFLNK branch)
 *   - Deeply nested directory hierarchies
 *   - Timestamp preservation across cycles
 *   - syncfs(populate=true) no-op behavior
 *   - Mixed node types (files, dirs, symlinks) in a single tree
 *   - Empty directories surviving persist/restore
 */
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { describe, it, expect, beforeEach } from "vitest";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { createTomeFS } from "../../src/tomefs.js";

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

function syncfs(FS: any, tomefs: any, populate = false) {
  tomefs.syncfs(
    FS.lookupPath(MOUNT).node.mount,
    populate,
    (err: any) => {
      if (err) throw err;
    },
  );
}

function syncAndUnmount(FS: any, tomefs: any) {
  syncfs(FS, tomefs);
  FS.unmount(MOUNT);
}

describe("persist/restore: symlinks", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("symlink survives sync→unmount→remount cycle @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create a file and a symlink pointing to it
    const s = FS.open(`${MOUNT}/target`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("linked content"), 0, 14);
    FS.close(s);
    FS.symlink(`${MOUNT}/target`, `${MOUNT}/link`);

    syncAndUnmount(FS, tomefs);

    // Remount and verify symlink
    const { FS: FS2 } = await mountTome(backend);
    const linkStat = FS2.lstat(`${MOUNT}/link`);
    expect((linkStat.mode & 0o170000) === 0o120000).toBe(true); // S_IFLNK

    const linkTarget = FS2.readlink(`${MOUNT}/link`);
    expect(linkTarget).toBe(`${MOUNT}/target`);

    // Read through the symlink
    const s2 = FS2.open(`${MOUNT}/link`, O.RDONLY);
    const buf = new Uint8Array(20);
    const n = FS2.read(s2, buf, 0, 20);
    FS2.close(s2);
    expect(decode(buf, n)).toBe("linked content");
  });

  it("relative symlink persists correctly", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/dir`);
    const s = FS.open(`${MOUNT}/dir/real`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("hello"), 0, 5);
    FS.close(s);
    FS.symlink("real", `${MOUNT}/dir/alias`);

    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const target = FS2.readlink(`${MOUNT}/dir/alias`);
    expect(target).toBe("real");
  });

  it("symlink to directory persists correctly", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/realdir`);
    const s = FS.open(`${MOUNT}/realdir/file`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("nested"), 0, 6);
    FS.close(s);
    FS.symlink(`${MOUNT}/realdir`, `${MOUNT}/dirlink`);

    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const target = FS2.readlink(`${MOUNT}/dirlink`);
    expect(target).toBe(`${MOUNT}/realdir`);

    // Access file through the directory symlink
    const s2 = FS2.open(`${MOUNT}/dirlink/file`, O.RDONLY);
    const buf = new Uint8Array(10);
    const n = FS2.read(s2, buf, 0, 10);
    FS2.close(s2);
    expect(decode(buf, n)).toBe("nested");
  });

  it("dangling symlink persists and restores (no target)", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.symlink(`${MOUNT}/nonexistent`, `${MOUNT}/dangling`);

    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const target = FS2.readlink(`${MOUNT}/dangling`);
    expect(target).toBe(`${MOUNT}/nonexistent`);

    // Opening the dangling symlink should fail
    expect(() => FS2.open(`${MOUNT}/dangling`, O.RDONLY)).toThrow();
  });

  it("multiple symlinks in same directory persist", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const s = FS.open(`${MOUNT}/original`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("data"), 0, 4);
    FS.close(s);
    FS.symlink(`${MOUNT}/original`, `${MOUNT}/link1`);
    FS.symlink(`${MOUNT}/original`, `${MOUNT}/link2`);
    FS.symlink(`${MOUNT}/link1`, `${MOUNT}/link3`); // chain

    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    expect(FS2.readlink(`${MOUNT}/link1`)).toBe(`${MOUNT}/original`);
    expect(FS2.readlink(`${MOUNT}/link2`)).toBe(`${MOUNT}/original`);
    expect(FS2.readlink(`${MOUNT}/link3`)).toBe(`${MOUNT}/link1`);
  });
});

describe("persist/restore: deeply nested directories", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("5-level deep directory tree persists and restores @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/a`);
    FS.mkdir(`${MOUNT}/a/b`);
    FS.mkdir(`${MOUNT}/a/b/c`);
    FS.mkdir(`${MOUNT}/a/b/c/d`);
    FS.mkdir(`${MOUNT}/a/b/c/d/e`);

    const s = FS.open(`${MOUNT}/a/b/c/d/e/leaf`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("deep"), 0, 4);
    FS.close(s);

    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/a/b/c/d/e/leaf`);
    expect(stat.size).toBe(4);

    const s2 = FS2.open(`${MOUNT}/a/b/c/d/e/leaf`, O.RDONLY);
    const buf = new Uint8Array(10);
    const n = FS2.read(s2, buf, 0, 10);
    FS2.close(s2);
    expect(decode(buf, n)).toBe("deep");
  });

  it("files at every level of nested dirs persist", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const dirs = ["", "/l1", "/l1/l2", "/l1/l2/l3"];
    for (const dir of dirs) {
      if (dir) FS.mkdir(`${MOUNT}${dir}`);
      const s = FS.open(`${MOUNT}${dir}/file`, O.RDWR | O.CREAT, 0o666);
      FS.write(s, encode(`at${dir || "/root"}`), 0, `at${dir || "/root"}`.length);
      FS.close(s);
    }

    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    for (const dir of dirs) {
      const s = FS2.open(`${MOUNT}${dir}/file`, O.RDONLY);
      const buf = new Uint8Array(30);
      const n = FS2.read(s, buf, 0, 30);
      FS2.close(s);
      expect(decode(buf, n)).toBe(`at${dir || "/root"}`);
    }
  });

  it("empty directories persist and restore", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/empty1`);
    FS.mkdir(`${MOUNT}/empty2`);
    FS.mkdir(`${MOUNT}/parent`);
    FS.mkdir(`${MOUNT}/parent/empty_child`);

    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);

    // All empty directories should exist
    const stat1 = FS2.stat(`${MOUNT}/empty1`);
    expect((stat1.mode & 0o170000) === 0o040000).toBe(true);

    const stat2 = FS2.stat(`${MOUNT}/empty2`);
    expect((stat2.mode & 0o170000) === 0o040000).toBe(true);

    const childStat = FS2.stat(`${MOUNT}/parent/empty_child`);
    expect((childStat.mode & 0o170000) === 0o040000).toBe(true);

    // Can create files in restored empty directories
    const s = FS2.open(`${MOUNT}/empty1/newfile`, O.RDWR | O.CREAT, 0o666);
    FS2.write(s, encode("new"), 0, 3);
    FS2.close(s);
  });
});

describe("persist/restore: timestamps", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("file timestamps survive persist/restore cycle @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const s = FS.open(`${MOUNT}/timed`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("data"), 0, 4);
    FS.close(s);

    const beforeStat = FS.stat(`${MOUNT}/timed`);

    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const afterStat = FS2.stat(`${MOUNT}/timed`);

    expect(afterStat.mtime.getTime()).toBe(beforeStat.mtime.getTime());
    expect(afterStat.ctime.getTime()).toBe(beforeStat.ctime.getTime());
  });

  it("directory timestamps survive persist/restore cycle", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/dir`);
    const beforeStat = FS.stat(`${MOUNT}/dir`);

    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const afterStat = FS2.stat(`${MOUNT}/dir`);

    expect(afterStat.mtime.getTime()).toBe(beforeStat.mtime.getTime());
    expect(afterStat.ctime.getTime()).toBe(beforeStat.ctime.getTime());
  });

  it("symlink timestamps survive persist/restore cycle", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.symlink("/target", `${MOUNT}/tslink`);
    const beforeStat = FS.lstat(`${MOUNT}/tslink`);

    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const afterStat = FS2.lstat(`${MOUNT}/tslink`);

    expect(afterStat.mtime.getTime()).toBe(beforeStat.mtime.getTime());
    expect(afterStat.ctime.getTime()).toBe(beforeStat.ctime.getTime());
  });
});

describe("persist/restore: syncfs populate=true", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("syncfs(populate=true) is a no-op that does not modify backend @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const s = FS.open(`${MOUNT}/file`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("before"), 0, 6);
    FS.close(s);

    // First sync to persist
    syncfs(FS, tomefs, false);

    // Modify the file in-memory
    const s2 = FS.open(`${MOUNT}/file`, O.RDWR | O.TRUNC);
    FS.write(s2, encode("after"), 0, 5);
    FS.close(s2);

    // syncfs with populate=true should NOT flush the change
    syncfs(FS, tomefs, true);

    // Backend should still have the old metadata size
    const meta = backend.readMeta("/file");
    expect(meta!.size).toBe(6); // "before", not "after"

    FS.unmount(MOUNT);
  });

  it("syncfs(populate=true) callback fires without error", async () => {
    const { FS, tomefs } = await mountTome(backend);

    let callbackFired = false;
    let callbackError: any = null;

    tomefs.syncfs(
      FS.lookupPath(MOUNT).node.mount,
      true,
      (err: any) => {
        callbackFired = true;
        callbackError = err;
      },
    );

    expect(callbackFired).toBe(true);
    expect(callbackError).toBeNull();

    FS.unmount(MOUNT);
  });
});

describe("persist/restore: mixed node types", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("tree with files, dirs, and symlinks round-trips correctly @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Build a mixed tree
    FS.mkdir(`${MOUNT}/src`);
    FS.mkdir(`${MOUNT}/src/lib`);

    const mainContent = "import { foo } from './lib/foo'";
    const s1 = FS.open(`${MOUNT}/src/main.ts`, O.RDWR | O.CREAT, 0o666);
    FS.write(s1, encode(mainContent), 0, mainContent.length);
    FS.close(s1);

    const libContent = "export const foo = 42";
    const s2 = FS.open(`${MOUNT}/src/lib/foo.ts`, O.RDWR | O.CREAT, 0o666);
    FS.write(s2, encode(libContent), 0, libContent.length);
    FS.close(s2);

    FS.symlink(`${MOUNT}/src/lib`, `${MOUNT}/lib`);
    FS.symlink(`${MOUNT}/src/main.ts`, `${MOUNT}/entry`);

    FS.mkdir(`${MOUNT}/empty_dir`);

    syncAndUnmount(FS, tomefs);

    // Remount and verify everything
    const { FS: FS2 } = await mountTome(backend);

    // Directories
    expect((FS2.stat(`${MOUNT}/src`).mode & 0o170000) === 0o040000).toBe(true);
    expect(
      (FS2.stat(`${MOUNT}/src/lib`).mode & 0o170000) === 0o040000,
    ).toBe(true);
    expect(
      (FS2.stat(`${MOUNT}/empty_dir`).mode & 0o170000) === 0o040000,
    ).toBe(true);

    // Files
    let s = FS2.open(`${MOUNT}/src/main.ts`, O.RDONLY);
    let buf = new Uint8Array(50);
    let n = FS2.read(s, buf, 0, 50);
    FS2.close(s);
    expect(decode(buf, n)).toBe(mainContent);

    s = FS2.open(`${MOUNT}/src/lib/foo.ts`, O.RDONLY);
    buf = new Uint8Array(50);
    n = FS2.read(s, buf, 0, 50);
    FS2.close(s);
    expect(decode(buf, n)).toBe(libContent);

    // Symlinks
    expect(FS2.readlink(`${MOUNT}/lib`)).toBe(`${MOUNT}/src/lib`);
    expect(FS2.readlink(`${MOUNT}/entry`)).toBe(`${MOUNT}/src/main.ts`);
  });

  it("multiple sync cycles with additions and deletions", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Cycle 1: create initial structure
    FS.mkdir(`${MOUNT}/data`);
    const s = FS.open(`${MOUNT}/data/v1`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("version1"), 0, 8);
    FS.close(s);
    FS.symlink(`${MOUNT}/data/v1`, `${MOUNT}/current`);

    syncAndUnmount(FS, tomefs);

    // Cycle 2: add v2, update symlink
    const { FS: FS2, tomefs: t2 } = await mountTome(backend);

    const s2 = FS2.open(`${MOUNT}/data/v2`, O.RDWR | O.CREAT, 0o666);
    FS2.write(s2, encode("version2"), 0, 8);
    FS2.close(s2);

    // Remove old symlink and create new one
    FS2.unlink(`${MOUNT}/current`);
    FS2.symlink(`${MOUNT}/data/v2`, `${MOUNT}/current`);

    syncAndUnmount(FS2, t2);

    // Cycle 3: verify
    const { FS: FS3 } = await mountTome(backend);

    expect(FS3.readlink(`${MOUNT}/current`)).toBe(`${MOUNT}/data/v2`);

    const s3 = FS3.open(`${MOUNT}/current`, O.RDONLY);
    const buf = new Uint8Array(20);
    const n = FS3.read(s3, buf, 0, 20);
    FS3.close(s3);
    expect(decode(buf, n)).toBe("version2");

    // v1 should still exist
    const sv1 = FS3.open(`${MOUNT}/data/v1`, O.RDONLY);
    const buf2 = new Uint8Array(20);
    const n2 = FS3.read(sv1, buf2, 0, 20);
    FS3.close(sv1);
    expect(decode(buf2, n2)).toBe("version1");
  });
});

describe("persist/restore: file modes", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("file permission modes survive persist/restore @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const s = FS.open(`${MOUNT}/readonly`, O.RDWR | O.CREAT, 0o444);
    FS.write(s, encode("ro"), 0, 2);
    FS.close(s);

    const s2 = FS.open(`${MOUNT}/exec`, O.RDWR | O.CREAT, 0o755);
    FS.write(s2, encode("x"), 0, 1);
    FS.close(s2);

    const beforeRo = FS.stat(`${MOUNT}/readonly`);
    const beforeExec = FS.stat(`${MOUNT}/exec`);

    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);

    const afterRo = FS2.stat(`${MOUNT}/readonly`);
    expect(afterRo.mode).toBe(beforeRo.mode);

    const afterExec = FS2.stat(`${MOUNT}/exec`);
    expect(afterExec.mode).toBe(beforeExec.mode);
  });

  it("directory mode persists correctly", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/restricted`, 0o700);
    const beforeStat = FS.stat(`${MOUNT}/restricted`);

    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const afterStat = FS2.stat(`${MOUNT}/restricted`);
    expect(afterStat.mode).toBe(beforeStat.mode);
  });
});

describe("persist/restore: edge cases", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("empty filesystem (no files) round-trips without error @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);

    // Should be mountable with empty backend
    const entries = FS2.readdir(`${MOUNT}`);
    expect(entries).toEqual([".", ".."]);
  });

  it("file with exactly 0 bytes persists correctly", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const s = FS.open(`${MOUNT}/empty`, O.RDWR | O.CREAT, 0o666);
    FS.close(s);

    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/empty`);
    expect(stat.size).toBe(0);
  });

  it("file renamed before sync persists under new name", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const s = FS.open(`${MOUNT}/old`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("renamed"), 0, 7);
    FS.close(s);

    FS.rename(`${MOUNT}/old`, `${MOUNT}/new`);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);

    // Old name should not exist
    expect(() => FS2.stat(`${MOUNT}/old`)).toThrow();

    // New name should have the data
    const s2 = FS2.open(`${MOUNT}/new`, O.RDONLY);
    const buf = new Uint8Array(10);
    const n = FS2.read(s2, buf, 0, 10);
    FS2.close(s2);
    expect(decode(buf, n)).toBe("renamed");
  });

  it("directory renamed with contents persists correctly", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/olddir`);
    const s = FS.open(`${MOUNT}/olddir/child`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("inside"), 0, 6);
    FS.close(s);

    FS.rename(`${MOUNT}/olddir`, `${MOUNT}/newdir`);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);

    expect(() => FS2.stat(`${MOUNT}/olddir`)).toThrow();

    const s2 = FS2.open(`${MOUNT}/newdir/child`, O.RDONLY);
    const buf = new Uint8Array(10);
    const n = FS2.read(s2, buf, 0, 10);
    FS2.close(s2);
    expect(decode(buf, n)).toBe("inside");
  });

  it("directory renamed with symlink child persists correctly", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/olddir`);
    // Create a file and a symlink inside the directory
    const s = FS.open(`${MOUNT}/olddir/file`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("data"), 0, 4);
    FS.close(s);
    FS.symlink("file", `${MOUNT}/olddir/link`);

    FS.rename(`${MOUNT}/olddir`, `${MOUNT}/newdir`);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);

    expect(() => FS2.stat(`${MOUNT}/olddir`)).toThrow();

    // The symlink should survive under the new path
    const linkStat = FS2.lstat(`${MOUNT}/newdir/link`);
    expect((linkStat.mode & 0o170000) === 0o120000).toBe(true);
    expect(FS2.readlink(`${MOUNT}/newdir/link`)).toBe("file");

    // Following the symlink should reach the file
    const s2 = FS2.open(`${MOUNT}/newdir/link`, O.RDONLY);
    const buf = new Uint8Array(10);
    const n = FS2.read(s2, buf, 0, 10);
    FS2.close(s2);
    expect(decode(buf, n)).toBe("data");
  });

  it("directory renamed with nested subdirectory persists correctly", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/top`);
    FS.mkdir(`${MOUNT}/top/sub`);
    const s = FS.open(`${MOUNT}/top/sub/deep`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("nested"), 0, 6);
    FS.close(s);
    FS.symlink("deep", `${MOUNT}/top/sub/link`);

    FS.rename(`${MOUNT}/top`, `${MOUNT}/moved`);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);

    expect(() => FS2.stat(`${MOUNT}/top`)).toThrow();

    // Nested file should be accessible
    const s2 = FS2.open(`${MOUNT}/moved/sub/deep`, O.RDONLY);
    const buf = new Uint8Array(10);
    const n = FS2.read(s2, buf, 0, 10);
    FS2.close(s2);
    expect(decode(buf, n)).toBe("nested");

    // Nested symlink should survive
    expect(FS2.readlink(`${MOUNT}/moved/sub/link`)).toBe("deep");
  });

  it("large number of files in a single directory", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const count = 50;
    for (let i = 0; i < count; i++) {
      const s = FS.open(
        `${MOUNT}/file_${String(i).padStart(3, "0")}`,
        O.RDWR | O.CREAT,
        0o666,
      );
      FS.write(s, encode(`data_${i}`), 0, `data_${i}`.length);
      FS.close(s);
    }

    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);

    const entries = FS2.readdir(`${MOUNT}`).filter(
      (e: string) => e !== "." && e !== "..",
    );
    expect(entries.length).toBe(count);

    // Spot-check a few files
    for (const i of [0, 25, 49]) {
      const s = FS2.open(
        `${MOUNT}/file_${String(i).padStart(3, "0")}`,
        O.RDONLY,
      );
      const buf = new Uint8Array(20);
      const n = FS2.read(s, buf, 0, 20);
      FS2.close(s);
      expect(decode(buf, n)).toBe(`data_${i}`);
    }
  });
});
