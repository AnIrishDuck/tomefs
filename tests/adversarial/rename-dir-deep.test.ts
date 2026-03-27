/**
 * Adversarial tests: Deep directory rename with persistence.
 *
 * renameDescendantPaths() recursively updates storagePaths for all file
 * descendants when a directory is renamed. Bugs here cause silent data loss:
 * pages remain keyed under old paths in the backend while metadata points to
 * new paths, so a persist → restore cycle loses file contents.
 *
 * These tests target:
 * - Deeply nested trees (5+ levels) with files at every level
 * - Mid-level directory renames (not just top-level)
 * - Mixed node types (files, symlinks, subdirs) under renamed dirs
 * - Sequential renames of multiple directories
 * - Rename + create at old path (path reuse)
 * - Cache pressure during directory rename with persistence
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

function syncAndUnmount(FS: any, tomefs: any) {
  tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
    if (err) throw err;
  });
  FS.unmount(MOUNT);
}

/** Write a file with deterministic content derived from its path. */
function writeFile(FS: any, path: string, content: string) {
  const data = encode(content);
  const s = FS.open(path, O.RDWR | O.CREAT, 0o666);
  FS.write(s, data, 0, data.length, 0);
  FS.close(s);
}

/** Write a multi-page file with deterministic byte pattern. */
function writeMultiPageFile(FS: any, path: string, pages: number, seed: number) {
  const size = PAGE_SIZE * pages;
  const data = new Uint8Array(size);
  for (let i = 0; i < size; i++) data[i] = (i * seed + seed) & 0xff;
  const s = FS.open(path, O.RDWR | O.CREAT, 0o666);
  FS.write(s, data, 0, size, 0);
  FS.close(s);
  return data;
}

/** Read a file and return its text content. */
function readFile(FS: any, path: string): string {
  const stat = FS.stat(path);
  const buf = new Uint8Array(stat.size);
  const s = FS.open(path, O.RDONLY);
  const n = FS.read(s, buf, 0, stat.size, 0);
  FS.close(s);
  return decode(buf, n);
}

/** Read a file and return raw bytes. */
function readBytes(FS: any, path: string): Uint8Array {
  const stat = FS.stat(path);
  const buf = new Uint8Array(stat.size);
  const s = FS.open(path, O.RDONLY);
  FS.read(s, buf, 0, stat.size, 0);
  FS.close(s);
  return buf;
}

describe("adversarial: deep directory rename with persistence", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  // ------------------------------------------------------------------
  // 5-level deep rename: files at every level survive persistence
  // ------------------------------------------------------------------

  it("rename 5-level deep tree preserves all descendant files across remount @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Build /tome/root/l1/l2/l3/l4 with a file at each level
    const levels = ["root", "l1", "l2", "l3", "l4"];
    let dir = MOUNT;
    for (const level of levels) {
      dir += `/${level}`;
      FS.mkdir(dir);
      writeFile(FS, `${dir}/file.txt`, `content-at-${level}`);
    }

    // Rename the top-level directory
    FS.rename(`${MOUNT}/root`, `${MOUNT}/moved`);
    syncAndUnmount(FS, tomefs);

    // Remount and verify every file at every level
    const { FS: FS2 } = await mountTome(backend);
    let dir2 = `${MOUNT}/moved`;
    for (let i = 1; i < levels.length; i++) {
      dir2 += `/${levels[i]}`;
    }
    // Check files from top to bottom
    expect(readFile(FS2, `${MOUNT}/moved/file.txt`)).toBe("content-at-root");
    expect(readFile(FS2, `${MOUNT}/moved/l1/file.txt`)).toBe("content-at-l1");
    expect(readFile(FS2, `${MOUNT}/moved/l1/l2/file.txt`)).toBe("content-at-l2");
    expect(readFile(FS2, `${MOUNT}/moved/l1/l2/l3/file.txt`)).toBe("content-at-l3");
    expect(readFile(FS2, `${MOUNT}/moved/l1/l2/l3/l4/file.txt`)).toBe("content-at-l4");

    // Old path must not exist
    expect(() => FS2.stat(`${MOUNT}/root`)).toThrow();
  });

  // ------------------------------------------------------------------
  // Mid-level rename: rename a directory in the middle of a deep tree
  // ------------------------------------------------------------------

  it("rename mid-level directory preserves subtree across remount", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Build /tome/a/b/c/d/e with files
    FS.mkdir(`${MOUNT}/a`);
    FS.mkdir(`${MOUNT}/a/b`);
    FS.mkdir(`${MOUNT}/a/b/c`);
    FS.mkdir(`${MOUNT}/a/b/c/d`);
    FS.mkdir(`${MOUNT}/a/b/c/d/e`);
    writeFile(FS, `${MOUNT}/a/above.txt`, "above-rename");
    writeFile(FS, `${MOUNT}/a/b/at-rename.txt`, "at-rename");
    writeFile(FS, `${MOUNT}/a/b/c/below.txt`, "below-rename");
    writeFile(FS, `${MOUNT}/a/b/c/d/deep.txt`, "deep-file");
    writeFile(FS, `${MOUNT}/a/b/c/d/e/deepest.txt`, "deepest-file");

    // Rename the mid-level directory b -> b_moved
    FS.rename(`${MOUNT}/a/b`, `${MOUNT}/a/b_moved`);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);

    // Above the rename — unchanged
    expect(readFile(FS2, `${MOUNT}/a/above.txt`)).toBe("above-rename");

    // Renamed subtree
    expect(readFile(FS2, `${MOUNT}/a/b_moved/at-rename.txt`)).toBe("at-rename");
    expect(readFile(FS2, `${MOUNT}/a/b_moved/c/below.txt`)).toBe("below-rename");
    expect(readFile(FS2, `${MOUNT}/a/b_moved/c/d/deep.txt`)).toBe("deep-file");
    expect(readFile(FS2, `${MOUNT}/a/b_moved/c/d/e/deepest.txt`)).toBe("deepest-file");

    // Old path must not exist
    expect(() => FS2.stat(`${MOUNT}/a/b`)).toThrow();
  });

  // ------------------------------------------------------------------
  // Mixed node types: files + symlinks + subdirs under renamed dir
  // ------------------------------------------------------------------

  it("rename directory with mixed node types preserves all across remount", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/mixed`);
    FS.mkdir(`${MOUNT}/mixed/subdir`);
    writeFile(FS, `${MOUNT}/mixed/file.txt`, "regular-file");
    writeFile(FS, `${MOUNT}/mixed/subdir/nested.txt`, "nested-file");
    FS.symlink("file.txt", `${MOUNT}/mixed/link`);
    FS.symlink("subdir/nested.txt", `${MOUNT}/mixed/deep-link`);

    FS.rename(`${MOUNT}/mixed`, `${MOUNT}/mixed_moved`);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    expect(readFile(FS2, `${MOUNT}/mixed_moved/file.txt`)).toBe("regular-file");
    expect(readFile(FS2, `${MOUNT}/mixed_moved/subdir/nested.txt`)).toBe("nested-file");
    expect(FS2.readlink(`${MOUNT}/mixed_moved/link`)).toBe("file.txt");
    expect(FS2.readlink(`${MOUNT}/mixed_moved/deep-link`)).toBe("subdir/nested.txt");
    // Verify symlinks resolve correctly
    expect(readFile(FS2, `${MOUNT}/mixed_moved/link`)).toBe("regular-file");
    expect(readFile(FS2, `${MOUNT}/mixed_moved/deep-link`)).toBe("nested-file");
  });

  // ------------------------------------------------------------------
  // Sequential renames of sibling directories
  // ------------------------------------------------------------------

  it("sequential renames of sibling directories preserve all data across remount", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create three sibling directories with files
    for (const name of ["alpha", "beta", "gamma"]) {
      FS.mkdir(`${MOUNT}/${name}`);
      FS.mkdir(`${MOUNT}/${name}/child`);
      writeFile(FS, `${MOUNT}/${name}/data.txt`, `${name}-data`);
      writeFile(FS, `${MOUNT}/${name}/child/nested.txt`, `${name}-nested`);
    }

    // Rename all three
    FS.rename(`${MOUNT}/alpha`, `${MOUNT}/a`);
    FS.rename(`${MOUNT}/beta`, `${MOUNT}/b`);
    FS.rename(`${MOUNT}/gamma`, `${MOUNT}/c`);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    for (const [old, nu] of [["alpha", "a"], ["beta", "b"], ["gamma", "c"]]) {
      expect(readFile(FS2, `${MOUNT}/${nu}/data.txt`)).toBe(`${old}-data`);
      expect(readFile(FS2, `${MOUNT}/${nu}/child/nested.txt`)).toBe(`${old}-nested`);
      expect(() => FS2.stat(`${MOUNT}/${old}`)).toThrow();
    }
  });

  // ------------------------------------------------------------------
  // Rename then create new directory at old path
  // ------------------------------------------------------------------

  it("rename dir then create at old path produces independent trees across remount", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/slot`);
    FS.mkdir(`${MOUNT}/slot/inner`);
    writeFile(FS, `${MOUNT}/slot/original.txt`, "original-data");
    writeFile(FS, `${MOUNT}/slot/inner/deep.txt`, "original-deep");

    // Move away
    FS.rename(`${MOUNT}/slot`, `${MOUNT}/archive`);

    // Recreate at old path with different content
    FS.mkdir(`${MOUNT}/slot`);
    FS.mkdir(`${MOUNT}/slot/inner`);
    writeFile(FS, `${MOUNT}/slot/replacement.txt`, "replacement-data");
    writeFile(FS, `${MOUNT}/slot/inner/new-deep.txt`, "replacement-deep");

    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    // Archived tree has original data
    expect(readFile(FS2, `${MOUNT}/archive/original.txt`)).toBe("original-data");
    expect(readFile(FS2, `${MOUNT}/archive/inner/deep.txt`)).toBe("original-deep");
    // New tree at old path has replacement data
    expect(readFile(FS2, `${MOUNT}/slot/replacement.txt`)).toBe("replacement-data");
    expect(readFile(FS2, `${MOUNT}/slot/inner/new-deep.txt`)).toBe("replacement-deep");
  });

  // ------------------------------------------------------------------
  // Rename child within already-renamed directory
  // ------------------------------------------------------------------

  it("rename child within renamed parent preserves data across remount", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/parent`);
    FS.mkdir(`${MOUNT}/parent/child`);
    writeFile(FS, `${MOUNT}/parent/child/data.txt`, "child-data");

    // First rename the parent
    FS.rename(`${MOUNT}/parent`, `${MOUNT}/parent_new`);
    // Then rename a child within the already-renamed parent
    FS.rename(`${MOUNT}/parent_new/child`, `${MOUNT}/parent_new/child_new`);

    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    expect(readFile(FS2, `${MOUNT}/parent_new/child_new/data.txt`)).toBe("child-data");
    expect(() => FS2.stat(`${MOUNT}/parent`)).toThrow();
    expect(() => FS2.stat(`${MOUNT}/parent_new/child`)).toThrow();
  });

  // ------------------------------------------------------------------
  // Deep rename under cache pressure (4-page cache)
  // ------------------------------------------------------------------

  it("deep directory rename under cache pressure preserves multi-page files @fast", async () => {
    const { FS, tomefs } = await mountTome(backend, 4); // 4-page cache

    FS.mkdir(`${MOUNT}/src`);
    FS.mkdir(`${MOUNT}/src/sub`);
    FS.mkdir(`${MOUNT}/src/sub/deep`);

    // Create multi-page files (exceed cache capacity) at different levels
    const data1 = writeMultiPageFile(FS, `${MOUNT}/src/big1.bin`, 3, 7);
    const data2 = writeMultiPageFile(FS, `${MOUNT}/src/sub/big2.bin`, 2, 13);
    const data3 = writeMultiPageFile(FS, `${MOUNT}/src/sub/deep/big3.bin`, 3, 19);

    FS.rename(`${MOUNT}/src`, `${MOUNT}/dst`);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend, 4);
    expect(readBytes(FS2, `${MOUNT}/dst/big1.bin`)).toEqual(data1);
    expect(readBytes(FS2, `${MOUNT}/dst/sub/big2.bin`)).toEqual(data2);
    expect(readBytes(FS2, `${MOUNT}/dst/sub/deep/big3.bin`)).toEqual(data3);
  });

  // ------------------------------------------------------------------
  // Chain rename: A -> B, then B/child -> B/child2
  // ------------------------------------------------------------------

  it("chain of renames with nested modifications across remount", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/dir_a`);
    FS.mkdir(`${MOUNT}/dir_a/sub`);
    writeFile(FS, `${MOUNT}/dir_a/f1.txt`, "f1-original");
    writeFile(FS, `${MOUNT}/dir_a/sub/f2.txt`, "f2-original");

    // Rename dir_a -> dir_b
    FS.rename(`${MOUNT}/dir_a`, `${MOUNT}/dir_b`);

    // Modify a file within the renamed dir
    writeFile(FS, `${MOUNT}/dir_b/f1.txt`, "f1-modified");

    // Add a new file in the renamed dir
    writeFile(FS, `${MOUNT}/dir_b/f3.txt`, "f3-new");

    // Rename the subdirectory within the renamed parent
    FS.rename(`${MOUNT}/dir_b/sub`, `${MOUNT}/dir_b/sub_renamed`);

    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    expect(readFile(FS2, `${MOUNT}/dir_b/f1.txt`)).toBe("f1-modified");
    expect(readFile(FS2, `${MOUNT}/dir_b/sub_renamed/f2.txt`)).toBe("f2-original");
    expect(readFile(FS2, `${MOUNT}/dir_b/f3.txt`)).toBe("f3-new");
    expect(() => FS2.stat(`${MOUNT}/dir_a`)).toThrow();
    expect(() => FS2.stat(`${MOUNT}/dir_b/sub`)).toThrow();
  });

  // ------------------------------------------------------------------
  // Swap rename: A -> tmp, B -> A, tmp -> B
  // ------------------------------------------------------------------

  it("directory swap via temp name preserves both trees across remount", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/x`);
    FS.mkdir(`${MOUNT}/x/xchild`);
    writeFile(FS, `${MOUNT}/x/xchild/xdata.txt`, "x-content");

    FS.mkdir(`${MOUNT}/y`);
    FS.mkdir(`${MOUNT}/y/ychild`);
    writeFile(FS, `${MOUNT}/y/ychild/ydata.txt`, "y-content");

    // Swap x and y via tmp
    FS.rename(`${MOUNT}/x`, `${MOUNT}/tmp`);
    FS.rename(`${MOUNT}/y`, `${MOUNT}/x`);
    FS.rename(`${MOUNT}/tmp`, `${MOUNT}/y`);

    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    // x now has y's old content
    expect(readFile(FS2, `${MOUNT}/x/ychild/ydata.txt`)).toBe("y-content");
    // y now has x's old content
    expect(readFile(FS2, `${MOUNT}/y/xchild/xdata.txt`)).toBe("x-content");
  });

  // ------------------------------------------------------------------
  // Rename directory into a sibling directory
  // ------------------------------------------------------------------

  it("move directory into sibling preserves deep tree across remount", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/container`);
    FS.mkdir(`${MOUNT}/src_dir`);
    FS.mkdir(`${MOUNT}/src_dir/a`);
    FS.mkdir(`${MOUNT}/src_dir/a/b`);
    writeFile(FS, `${MOUNT}/src_dir/f.txt`, "src-root");
    writeFile(FS, `${MOUNT}/src_dir/a/f.txt`, "src-a");
    writeFile(FS, `${MOUNT}/src_dir/a/b/f.txt`, "src-a-b");

    // Move src_dir into container
    FS.rename(`${MOUNT}/src_dir`, `${MOUNT}/container/nested`);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    expect(readFile(FS2, `${MOUNT}/container/nested/f.txt`)).toBe("src-root");
    expect(readFile(FS2, `${MOUNT}/container/nested/a/f.txt`)).toBe("src-a");
    expect(readFile(FS2, `${MOUNT}/container/nested/a/b/f.txt`)).toBe("src-a-b");
    expect(() => FS2.stat(`${MOUNT}/src_dir`)).toThrow();
  });

  // ------------------------------------------------------------------
  // Wide tree: directory with many files renamed under cache pressure
  // ------------------------------------------------------------------

  it("rename wide directory (many files) under cache pressure preserves all @fast", async () => {
    const { FS, tomefs } = await mountTome(backend, 4); // tiny cache

    FS.mkdir(`${MOUNT}/wide`);
    const fileCount = 10;
    const expected: Map<string, string> = new Map();

    for (let i = 0; i < fileCount; i++) {
      const content = `file-${i}-data-${i * 37}`;
      writeFile(FS, `${MOUNT}/wide/f${i}.txt`, content);
      expected.set(`f${i}.txt`, content);
    }

    FS.rename(`${MOUNT}/wide`, `${MOUNT}/wide_moved`);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend, 4);
    for (const [name, content] of expected) {
      expect(readFile(FS2, `${MOUNT}/wide_moved/${name}`)).toBe(content);
    }
  });

  // ------------------------------------------------------------------
  // Rename directory, sync, rename again, sync — two cycles
  // ------------------------------------------------------------------

  it("double rename across two sync cycles preserves data", async () => {
    const { FS: FS1, tomefs: t1 } = await mountTome(backend);
    FS1.mkdir(`${MOUNT}/step1`);
    FS1.mkdir(`${MOUNT}/step1/inner`);
    writeFile(FS1, `${MOUNT}/step1/inner/data.txt`, "persistent-data");
    FS1.rename(`${MOUNT}/step1`, `${MOUNT}/step2`);
    syncAndUnmount(FS1, t1);

    // Second cycle: rename again
    const { FS: FS2, tomefs: t2 } = await mountTome(backend);
    expect(readFile(FS2, `${MOUNT}/step2/inner/data.txt`)).toBe("persistent-data");
    FS2.rename(`${MOUNT}/step2`, `${MOUNT}/step3`);
    syncAndUnmount(FS2, t2);

    // Third mount: verify final location
    const { FS: FS3 } = await mountTome(backend);
    expect(readFile(FS3, `${MOUNT}/step3/inner/data.txt`)).toBe("persistent-data");
    expect(() => FS3.stat(`${MOUNT}/step1`)).toThrow();
    expect(() => FS3.stat(`${MOUNT}/step2`)).toThrow();
  });

  // ------------------------------------------------------------------
  // WAL-like pattern: rename segment directories during append
  // ------------------------------------------------------------------

  it("WAL-style directory rotation under cache pressure preserves segments", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    // Simulate WAL segment rotation:
    // active/ accumulates writes, then gets renamed to archive_N
    for (let seg = 0; seg < 3; seg++) {
      FS.mkdir(`${MOUNT}/active`);
      // Write segment data (2 pages each to exceed cache)
      writeMultiPageFile(FS, `${MOUNT}/active/wal.dat`, 2, seg + 1);
      // Rotate: rename active -> archive_N
      FS.rename(`${MOUNT}/active`, `${MOUNT}/archive_${seg}`);
    }

    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend, 4);
    for (let seg = 0; seg < 3; seg++) {
      const expected = new Uint8Array(PAGE_SIZE * 2);
      const seed = seg + 1;
      for (let i = 0; i < expected.length; i++) expected[i] = (i * seed + seed) & 0xff;
      expect(readBytes(FS2, `${MOUNT}/archive_${seg}/wal.dat`)).toEqual(expected);
    }
    expect(() => FS2.stat(`${MOUNT}/active`)).toThrow();
  });

  // ------------------------------------------------------------------
  // Rename directory over empty directory
  // ------------------------------------------------------------------

  it("rename directory over empty target directory preserves subtree across remount", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/source`);
    FS.mkdir(`${MOUNT}/source/child`);
    writeFile(FS, `${MOUNT}/source/child/data.txt`, "overwrite-data");
    FS.mkdir(`${MOUNT}/target`); // empty target

    FS.rename(`${MOUNT}/source`, `${MOUNT}/target`);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    expect(readFile(FS2, `${MOUNT}/target/child/data.txt`)).toBe("overwrite-data");
    expect(() => FS2.stat(`${MOUNT}/source`)).toThrow();
  });
});
