/**
 * Adversarial tests: partial failure rollback in renameDescendantPaths.
 *
 * When renaming a directory with multiple file children,
 * renameDescendantPaths moves pages for each child file sequentially.
 * If one of these page renames fails (e.g., backend error through SAB
 * bridge), children that were already moved must be rolled back so the
 * in-memory state stays consistent with the node tree.
 *
 * Without rollback, partially-renamed children have storagePaths pointing
 * to new locations while the node tree (and Emscripten's FS) still places
 * them at the old path. This causes:
 *   - Corrupt paths on retry (substring calculation uses stale base)
 *   - Data loss on syncfs (metadata written to wrong paths)
 *   - Silent read corruption (reads go to wrong backend keys)
 *
 * Ethos §9: "Write tests designed to break tomefs specifically — target
 * the seams: metadata updates after flush, dirty flush ordering"
 */
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { describe, it, expect, beforeEach } from "vitest";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { createTomeFS } from "../../src/tomefs.js";
import { PAGE_SIZE } from "../../src/types.js";
import type { SyncStorageBackend } from "../../src/sync-storage-backend.js";
import type { FileMeta } from "../../src/types.js";

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

/**
 * Backend that fails renameFile for a specific target path.
 * All other operations pass through to the inner SyncMemoryBackend.
 */
class FailOnRenameBackend implements SyncStorageBackend {
  readonly inner = new SyncMemoryBackend();
  failOnPath: string | null = null;
  renameCallCount = 0;

  readPage(path: string, pageIndex: number): Uint8Array | null {
    return this.inner.readPage(path, pageIndex);
  }
  readPages(path: string, pageIndices: number[]): Array<Uint8Array | null> {
    return this.inner.readPages(path, pageIndices);
  }
  writePage(path: string, pageIndex: number, data: Uint8Array): void {
    this.inner.writePage(path, pageIndex, data);
  }
  writePages(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
  ): void {
    this.inner.writePages(pages);
  }
  deleteFile(path: string): void {
    this.inner.deleteFile(path);
  }
  deleteFiles(paths: string[]): void {
    this.inner.deleteFiles(paths);
  }
  deletePagesFrom(path: string, fromPageIndex: number): void {
    this.inner.deletePagesFrom(path, fromPageIndex);
  }
  renameFile(oldPath: string, newPath: string): void {
    this.renameCallCount++;
    if (this.failOnPath && oldPath === this.failOnPath) {
      throw new Error(`injected renameFile failure for ${oldPath}`);
    }
    this.inner.renameFile(oldPath, newPath);
  }
  readMeta(path: string): FileMeta | null {
    return this.inner.readMeta(path);
  }
  readMetas(paths: string[]): Array<FileMeta | null> {
    return this.inner.readMetas(paths);
  }
  writeMeta(path: string, meta: FileMeta): void {
    this.inner.writeMeta(path, meta);
  }
  writeMetas(entries: Array<{ path: string; meta: FileMeta }>): void {
    this.inner.writeMetas(entries);
  }
  deleteMeta(path: string): void {
    this.inner.deleteMeta(path);
  }
  deleteMetas(paths: string[]): void {
    this.inner.deleteMetas(paths);
  }
  countPages(path: string): number {
    return this.inner.countPages(path);
  }
  countPagesBatch(paths: string[]): number[] {
    return this.inner.countPagesBatch(paths);
  }
  maxPageIndex(path: string): number {
    return this.inner.maxPageIndex(path);
  }
  maxPageIndexBatch(paths: string[]): number[] {
    return this.inner.maxPageIndexBatch(paths);
  }
  listFiles(): string[] {
    return this.inner.listFiles();
  }
  syncAll(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
    metas: Array<{ path: string; meta: FileMeta }>,
  ): void {
    this.inner.syncAll(pages, metas);
  }
  deleteAll(paths: string[]): void {
    this.inner.deleteAll(paths);
  }
  assertInvariants(): void {
    this.inner.assertInvariants();
  }
}

async function mountTome(backend: SyncStorageBackend, maxPages?: number) {
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

function syncfs(FS: any, tomefs: any): Error | null {
  let error: Error | null = null;
  tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
    error = err;
  });
  return error;
}

function syncAndUnmount(FS: any, tomefs: any) {
  const err = syncfs(FS, tomefs);
  if (err) throw err;
  FS.unmount(MOUNT);
}

describe("directory rename partial failure rollback", () => {
  let backend: FailOnRenameBackend;

  beforeEach(() => {
    backend = new FailOnRenameBackend();
  });

  it("rolled-back children are readable after partial rename failure @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/dir`);
    const files = ["a.txt", "b.txt", "c.txt"];
    const contents: Record<string, string> = {};
    for (const name of files) {
      const content = `content-of-${name}`;
      contents[name] = content;
      const data = encode(content);
      const fd = FS.open(`${MOUNT}/dir/${name}`, O.RDWR | O.CREAT, 0o666);
      FS.write(fd, data, 0, data.length);
      FS.close(fd);
    }

    // Sync to persist all data
    const err = syncfs(FS, tomefs);
    expect(err).toBeNull();

    // Fail on the second file's page rename (b.txt).
    // a.txt should be rolled back, c.txt should never have been touched.
    backend.failOnPath = "/dir/b.txt";

    expect(() => FS.rename(`${MOUNT}/dir`, `${MOUNT}/newdir`)).toThrow();

    // ALL children should still be readable at the OLD path
    for (const name of files) {
      const stat = FS.stat(`${MOUNT}/dir/${name}`);
      expect(stat.size).toBe(contents[name].length);
      const buf = new Uint8Array(stat.size);
      const fd = FS.open(`${MOUNT}/dir/${name}`, O.RDONLY);
      FS.read(fd, buf, 0, stat.size, 0);
      FS.close(fd);
      expect(decode(buf)).toBe(contents[name]);
    }
  });

  it("retry succeeds after partial rename failure @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/dir`);
    const files = ["a.txt", "b.txt", "c.txt"];
    const contents: Record<string, string> = {};
    for (const name of files) {
      const content = `retry-test-${name}-${"x".repeat(50)}`;
      contents[name] = content;
      const data = encode(content);
      const fd = FS.open(`${MOUNT}/dir/${name}`, O.RDWR | O.CREAT, 0o666);
      FS.write(fd, data, 0, data.length);
      FS.close(fd);
    }

    const err = syncfs(FS, tomefs);
    expect(err).toBeNull();

    // Fail on c.txt rename
    backend.failOnPath = "/dir/c.txt";
    expect(() => FS.rename(`${MOUNT}/dir`, `${MOUNT}/newdir`)).toThrow();

    // Clear the failure and retry
    backend.failOnPath = null;
    FS.rename(`${MOUNT}/dir`, `${MOUNT}/newdir`);

    // All files should be at the new path with correct contents
    for (const name of files) {
      const stat = FS.stat(`${MOUNT}/newdir/${name}`);
      const buf = new Uint8Array(stat.size);
      const fd = FS.open(`${MOUNT}/newdir/${name}`, O.RDONLY);
      FS.read(fd, buf, 0, stat.size, 0);
      FS.close(fd);
      expect(decode(buf)).toBe(contents[name]);
    }
  });

  it("retry to different destination succeeds after partial failure @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/src`);
    const content = "retry-different-dest";
    const data = encode(content);
    const fd = FS.open(`${MOUNT}/src/file.txt`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, data, 0, data.length);
    FS.close(fd);

    const fd2 = FS.open(`${MOUNT}/src/other.txt`, O.RDWR | O.CREAT, 0o666);
    const data2 = encode("other-content");
    FS.write(fd2, data2, 0, data2.length);
    FS.close(fd2);

    const err = syncfs(FS, tomefs);
    expect(err).toBeNull();

    // Fail on other.txt rename
    backend.failOnPath = "/src/other.txt";
    expect(() => FS.rename(`${MOUNT}/src`, `${MOUNT}/dst`)).toThrow();

    // Clear failure and rename to a DIFFERENT (longer) destination name
    backend.failOnPath = null;
    FS.rename(`${MOUNT}/src`, `${MOUNT}/destination_dir`);

    // Verify both files are readable at the new path
    const stat = FS.stat(`${MOUNT}/destination_dir/file.txt`);
    const buf = new Uint8Array(stat.size);
    const rfd = FS.open(`${MOUNT}/destination_dir/file.txt`, O.RDONLY);
    FS.read(rfd, buf, 0, stat.size, 0);
    FS.close(rfd);
    expect(decode(buf)).toBe(content);

    const stat2 = FS.stat(`${MOUNT}/destination_dir/other.txt`);
    const buf2 = new Uint8Array(stat2.size);
    const rfd2 = FS.open(`${MOUNT}/destination_dir/other.txt`, O.RDONLY);
    FS.read(rfd2, buf2, 0, stat2.size, 0);
    FS.close(rfd2);
    expect(decode(buf2)).toBe("other-content");
  });

  it("deep nested directory partial failure rolls back all levels @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create a 3-level deep structure:
    //   /dir/sub1/file1.txt
    //   /dir/sub2/file2.txt
    FS.mkdir(`${MOUNT}/dir`);
    FS.mkdir(`${MOUNT}/dir/sub1`);
    FS.mkdir(`${MOUNT}/dir/sub2`);

    const content1 = "deep-level-1-file";
    const data1 = encode(content1);
    const fd1 = FS.open(`${MOUNT}/dir/sub1/file1.txt`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd1, data1, 0, data1.length);
    FS.close(fd1);

    const content2 = "deep-level-2-file";
    const data2 = encode(content2);
    const fd2 = FS.open(`${MOUNT}/dir/sub2/file2.txt`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd2, data2, 0, data2.length);
    FS.close(fd2);

    const err = syncfs(FS, tomefs);
    expect(err).toBeNull();

    // Fail on the second subdirectory's file
    backend.failOnPath = "/dir/sub2/file2.txt";
    expect(() => FS.rename(`${MOUNT}/dir`, `${MOUNT}/newdir`)).toThrow();

    // Both files must be readable at the old paths
    const stat1 = FS.stat(`${MOUNT}/dir/sub1/file1.txt`);
    const buf1 = new Uint8Array(stat1.size);
    const rfd1 = FS.open(`${MOUNT}/dir/sub1/file1.txt`, O.RDONLY);
    FS.read(rfd1, buf1, 0, stat1.size, 0);
    FS.close(rfd1);
    expect(decode(buf1)).toBe(content1);

    const stat2 = FS.stat(`${MOUNT}/dir/sub2/file2.txt`);
    const buf2 = new Uint8Array(stat2.size);
    const rfd2 = FS.open(`${MOUNT}/dir/sub2/file2.txt`, O.RDONLY);
    FS.read(rfd2, buf2, 0, stat2.size, 0);
    FS.close(rfd2);
    expect(decode(buf2)).toBe(content2);

    // Retry should succeed
    backend.failOnPath = null;
    FS.rename(`${MOUNT}/dir`, `${MOUNT}/newdir`);

    const s1 = FS.stat(`${MOUNT}/newdir/sub1/file1.txt`);
    const b1 = new Uint8Array(s1.size);
    const f1 = FS.open(`${MOUNT}/newdir/sub1/file1.txt`, O.RDONLY);
    FS.read(f1, b1, 0, s1.size, 0);
    FS.close(f1);
    expect(decode(b1)).toBe(content1);

    const s2 = FS.stat(`${MOUNT}/newdir/sub2/file2.txt`);
    const b2 = new Uint8Array(s2.size);
    const f2 = FS.open(`${MOUNT}/newdir/sub2/file2.txt`, O.RDONLY);
    FS.read(f2, b2, 0, s2.size, 0);
    FS.close(f2);
    expect(decode(b2)).toBe(content2);
  });

  it("multi-page files survive partial rollback @fast", async () => {
    const { FS, tomefs } = await mountTome(backend, 32);

    FS.mkdir(`${MOUNT}/dir`);

    // Create a file larger than one page (PAGE_SIZE = 8192)
    const bigContent = "B".repeat(PAGE_SIZE + 100);
    const bigData = encode(bigContent);
    const fd1 = FS.open(`${MOUNT}/dir/big.dat`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd1, bigData, 0, bigData.length);
    FS.close(fd1);

    const smallContent = "small-file-data";
    const smallData = encode(smallContent);
    const fd2 = FS.open(`${MOUNT}/dir/small.txt`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd2, smallData, 0, smallData.length);
    FS.close(fd2);

    const err = syncfs(FS, tomefs);
    expect(err).toBeNull();

    // Fail on the small file rename, after the big file (multi-page) succeeds
    backend.failOnPath = "/dir/small.txt";
    expect(() => FS.rename(`${MOUNT}/dir`, `${MOUNT}/moved`)).toThrow();

    // Big file should be rolled back — all pages readable at old path
    const stat = FS.stat(`${MOUNT}/dir/big.dat`);
    expect(stat.size).toBe(bigData.length);
    const buf = new Uint8Array(stat.size);
    const rfd = FS.open(`${MOUNT}/dir/big.dat`, O.RDONLY);
    FS.read(rfd, buf, 0, stat.size, 0);
    FS.close(rfd);
    expect(decode(buf)).toBe(bigContent);
  });

  it("syncfs succeeds after partial rename failure and rollback @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/dir`);
    const files = ["x.txt", "y.txt"];
    for (const name of files) {
      const fd = FS.open(`${MOUNT}/dir/${name}`, O.RDWR | O.CREAT, 0o666);
      const data = encode(`data-for-${name}`);
      FS.write(fd, data, 0, data.length);
      FS.close(fd);
    }

    let err = syncfs(FS, tomefs);
    expect(err).toBeNull();

    // Trigger partial failure
    backend.failOnPath = "/dir/y.txt";
    expect(() => FS.rename(`${MOUNT}/dir`, `${MOUNT}/newdir`)).toThrow();
    backend.failOnPath = null;

    // Modify a file to create dirty state
    const fd = FS.open(`${MOUNT}/dir/x.txt`, O.RDWR | O.TRUNC);
    const newData = encode("modified-x");
    FS.write(fd, newData, 0, newData.length);
    FS.close(fd);

    // syncfs should succeed without errors
    err = syncfs(FS, tomefs);
    expect(err).toBeNull();

    // Data should be readable and correct
    const stat = FS.stat(`${MOUNT}/dir/x.txt`);
    const buf = new Uint8Array(stat.size);
    const rfd = FS.open(`${MOUNT}/dir/x.txt`, O.RDONLY);
    FS.read(rfd, buf, 0, stat.size, 0);
    FS.close(rfd);
    expect(decode(buf)).toBe("modified-x");
  });

  it("persist and restore after failed rename + successful retry @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/dir`);
    const content = "persistence-test-data";
    const data = encode(content);
    const fd = FS.open(`${MOUNT}/dir/file.txt`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, data, 0, data.length);
    FS.close(fd);

    let err = syncfs(FS, tomefs);
    expect(err).toBeNull();

    // Partial failure then retry
    backend.failOnPath = "/dir/file.txt";
    expect(() => FS.rename(`${MOUNT}/dir`, `${MOUNT}/newdir`)).toThrow();
    backend.failOnPath = null;
    FS.rename(`${MOUNT}/dir`, `${MOUNT}/newdir`);

    // Sync and unmount
    syncAndUnmount(FS, tomefs);

    // Remount from same backend and verify data
    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/newdir/file.txt`);
    expect(stat.size).toBe(data.length);
    const buf = new Uint8Array(stat.size);
    const rfd = FS2.open(`${MOUNT}/newdir/file.txt`, O.RDONLY);
    FS2.read(rfd, buf, 0, stat.size, 0);
    FS2.close(rfd);
    expect(decode(buf)).toBe(content);
  });

  it("backend invariants hold after partial failure and rollback @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/dir`);
    for (const name of ["f1.txt", "f2.txt", "f3.txt"]) {
      const fd = FS.open(`${MOUNT}/dir/${name}`, O.RDWR | O.CREAT, 0o666);
      const data = encode(`inv-test-${name}`);
      FS.write(fd, data, 0, data.length);
      FS.close(fd);
    }

    let err = syncfs(FS, tomefs);
    expect(err).toBeNull();

    // Fail on f2.txt — f1.txt will be rolled back, f3.txt untouched
    backend.failOnPath = "/dir/f2.txt";
    expect(() => FS.rename(`${MOUNT}/dir`, `${MOUNT}/moved`)).toThrow();
    backend.failOnPath = null;

    // Backend invariants should still hold
    backend.inner.assertInvariants();

    // tomefs invariants should hold too
    tomefs.assertInvariants();
  });
});
