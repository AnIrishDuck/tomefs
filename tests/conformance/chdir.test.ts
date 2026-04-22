/**
 * Conformance tests for chdir() and relative path resolution.
 *
 * Source: POSIX chdir(2), getcwd(3), and path resolution rules.
 * Postgres calls chdir() to the data directory on startup, then uses
 * relative paths for all subsequent file operations. These tests verify
 * that the filesystem correctly resolves relative paths against the
 * current working directory set by chdir().
 *
 * Tests: chdir, cwd, relative open/stat/mkdir/rename/unlink/symlink,
 *        ".." navigation, error cases (ENOENT, ENOTDIR).
 */
import {
  createFS,
  encode,
  decode,
  expectErrno,
  O,
  SEEK_SET,
  type FSHarness,
} from "../harness/emscripten-fs.js";

describe("chdir and relative paths", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  // -------------------------------------------------------------------
  // Basic chdir / cwd
  // -------------------------------------------------------------------

  it("cwd starts at root @fast", () => {
    const { FS } = h;
    expect(FS.cwd()).toBe("/");
  });

  it("chdir changes cwd @fast", () => {
    const { FS } = h;
    FS.mkdir("/workdir");
    FS.chdir("/workdir");
    expect(FS.cwd()).toBe("/workdir");
  });

  it("chdir to nested directory", () => {
    const { FS } = h;
    FS.mkdir("/a");
    FS.mkdir("/a/b");
    FS.mkdir("/a/b/c");
    FS.chdir("/a/b/c");
    expect(FS.cwd()).toBe("/a/b/c");
  });

  it("chdir back to root", () => {
    const { FS } = h;
    FS.mkdir("/workdir");
    FS.chdir("/workdir");
    expect(FS.cwd()).toBe("/workdir");
    FS.chdir("/");
    expect(FS.cwd()).toBe("/");
  });

  it("chdir to non-existent directory throws ENOENT @fast", () => {
    const { FS, E } = h;
    expectErrno(() => FS.chdir("/nonexistent"), E.ENOENT);
  });

  it("chdir to a file throws ENOTDIR", () => {
    const { FS, E } = h;
    const s = FS.open("/afile", O.WRONLY | O.CREAT, 0o666);
    FS.close(s);
    expectErrno(() => FS.chdir("/afile"), E.ENOTDIR);
  });

  // -------------------------------------------------------------------
  // Relative path file operations
  // -------------------------------------------------------------------

  it("open with relative path resolves against cwd @fast", () => {
    const { FS } = h;
    FS.mkdir("/data");
    FS.chdir("/data");

    const s = FS.open("hello.txt", O.WRONLY | O.CREAT, 0o666);
    FS.write(s, encode("hello"), 0, 5);
    FS.close(s);

    // Verify via absolute path
    const stat = FS.stat("/data/hello.txt");
    expect(stat.size).toBe(5);
  });

  it("stat with relative path resolves against cwd", () => {
    const { FS } = h;
    FS.mkdir("/data");
    // Create file via absolute path
    const s = FS.open("/data/info.txt", O.WRONLY | O.CREAT, 0o666);
    FS.write(s, encode("info"), 0, 4);
    FS.close(s);

    FS.chdir("/data");
    const stat = FS.stat("info.txt");
    expect(stat.size).toBe(4);
  });

  it("mkdir with relative path creates in cwd", () => {
    const { FS } = h;
    FS.mkdir("/base");
    FS.chdir("/base");
    FS.mkdir("subdir");

    // Verify via absolute path
    const stat = FS.stat("/base/subdir");
    expect(FS.isDir(stat.mode)).toBe(true);
  });

  it("unlink with relative path removes file in cwd", () => {
    const { FS, E } = h;
    FS.mkdir("/data");
    const s = FS.open("/data/temp.txt", O.WRONLY | O.CREAT, 0o666);
    FS.close(s);

    FS.chdir("/data");
    FS.unlink("temp.txt");

    expectErrno(() => FS.stat("/data/temp.txt"), E.ENOENT);
  });

  it("rename with relative paths resolves against cwd", () => {
    const { FS, E } = h;
    FS.mkdir("/data");
    const s = FS.open("/data/old.txt", O.WRONLY | O.CREAT, 0o666);
    FS.write(s, encode("content"), 0, 7);
    FS.close(s);

    FS.chdir("/data");
    FS.rename("old.txt", "new.txt");

    expectErrno(() => FS.stat("/data/old.txt"), E.ENOENT);
    const stat = FS.stat("/data/new.txt");
    expect(stat.size).toBe(7);
  });

  it("readdir with relative path lists cwd contents", () => {
    const { FS } = h;
    FS.mkdir("/data");
    FS.mkdir("/data/sub1");
    FS.mkdir("/data/sub2");
    const s = FS.open("/data/file1", O.WRONLY | O.CREAT, 0o666);
    FS.close(s);

    FS.chdir("/data");
    const entries = FS.readdir(".");
    expect(entries).toContain("sub1");
    expect(entries).toContain("sub2");
    expect(entries).toContain("file1");
    expect(entries).toContain(".");
    expect(entries).toContain("..");
  });

  it("rmdir with relative path removes directory in cwd", () => {
    const { FS, E } = h;
    FS.mkdir("/data");
    FS.mkdir("/data/empty");

    FS.chdir("/data");
    FS.rmdir("empty");

    expectErrno(() => FS.stat("/data/empty"), E.ENOENT);
  });

  it("symlink with relative paths resolves against cwd", () => {
    const { FS } = h;
    FS.mkdir("/data");
    const s = FS.open("/data/target.txt", O.WRONLY | O.CREAT, 0o666);
    FS.write(s, encode("target"), 0, 6);
    FS.close(s);

    FS.chdir("/data");
    FS.symlink("target.txt", "link.txt");

    const target = FS.readlink("/data/link.txt");
    expect(target).toBe("target.txt");

    // Reading through the symlink should work
    const rs = FS.open("/data/link.txt", O.RDONLY);
    const buf = new Uint8Array(10);
    const n = FS.read(rs, buf, 0, 10);
    expect(decode(buf, n)).toBe("target");
    FS.close(rs);
  });

  it("truncate with relative path resolves against cwd", () => {
    const { FS } = h;
    FS.mkdir("/data");
    const s = FS.open("/data/big.txt", O.WRONLY | O.CREAT, 0o666);
    FS.write(s, encode("hello world"), 0, 11);
    FS.close(s);

    FS.chdir("/data");
    FS.truncate("big.txt", 5);

    const stat = FS.stat("/data/big.txt");
    expect(stat.size).toBe(5);
  });

  it("chmod with relative path resolves against cwd", () => {
    const { FS } = h;
    FS.mkdir("/data");
    const s = FS.open("/data/perm.txt", O.WRONLY | O.CREAT, 0o666);
    FS.close(s);

    FS.chdir("/data");
    FS.chmod("perm.txt", 0o444);

    const stat = FS.stat("/data/perm.txt");
    expect(stat.mode & 0o777).toBe(0o444);
  });

  it("utime with relative path resolves against cwd", () => {
    const { FS } = h;
    FS.mkdir("/data");
    const s = FS.open("/data/ts.txt", O.WRONLY | O.CREAT, 0o666);
    FS.close(s);

    FS.chdir("/data");
    FS.utime("ts.txt", 1000, 2000);

    const stat = FS.stat("/data/ts.txt");
    expect(stat.atime.getTime()).toBe(1000);
    expect(stat.mtime.getTime()).toBe(2000);
  });

  // -------------------------------------------------------------------
  // ".." navigation
  // -------------------------------------------------------------------

  it("open via .. resolves to parent directory @fast", () => {
    const { FS } = h;
    FS.mkdir("/parent");
    FS.mkdir("/parent/child");

    // Create file in parent
    const ws = FS.open("/parent/sibling.txt", O.WRONLY | O.CREAT, 0o666);
    FS.write(ws, encode("sibling"), 0, 7);
    FS.close(ws);

    // chdir to child, access parent via ..
    FS.chdir("/parent/child");
    const rs = FS.open("../sibling.txt", O.RDONLY);
    const buf = new Uint8Array(20);
    const n = FS.read(rs, buf, 0, 20);
    expect(decode(buf, n)).toBe("sibling");
    FS.close(rs);
  });

  it("chdir with .. navigates to parent", () => {
    const { FS } = h;
    FS.mkdir("/a");
    FS.mkdir("/a/b");

    FS.chdir("/a/b");
    expect(FS.cwd()).toBe("/a/b");

    FS.chdir("..");
    expect(FS.cwd()).toBe("/a");

    FS.chdir("..");
    expect(FS.cwd()).toBe("/");
  });

  it(".. from root stays at root", () => {
    const { FS } = h;
    FS.chdir("/");
    FS.chdir("..");
    expect(FS.cwd()).toBe("/");
  });

  it("stat via ../.. resolves multi-level parent", () => {
    const { FS } = h;
    FS.mkdir("/x");
    FS.mkdir("/x/y");
    FS.mkdir("/x/y/z");
    const s = FS.open("/x/root.txt", O.WRONLY | O.CREAT, 0o666);
    FS.write(s, encode("root"), 0, 4);
    FS.close(s);

    FS.chdir("/x/y/z");
    const stat = FS.stat("../../root.txt");
    expect(stat.size).toBe(4);
  });

  it("mkdir via .. creates directory in parent", () => {
    const { FS } = h;
    FS.mkdir("/base");
    FS.mkdir("/base/child");

    FS.chdir("/base/child");
    FS.mkdir("../sibling");

    const stat = FS.stat("/base/sibling");
    expect(FS.isDir(stat.mode)).toBe(true);
  });

  // -------------------------------------------------------------------
  // "." (current directory) references
  // -------------------------------------------------------------------

  it("stat on '.' returns cwd info", () => {
    const { FS } = h;
    FS.mkdir("/mydir");
    FS.chdir("/mydir");
    const stat = FS.stat(".");
    expect(FS.isDir(stat.mode)).toBe(true);
  });

  it("readdir on '.' lists cwd", () => {
    const { FS } = h;
    FS.mkdir("/mydir");
    const s = FS.open("/mydir/f1", O.WRONLY | O.CREAT, 0o666);
    FS.close(s);

    FS.chdir("/mydir");
    const entries = FS.readdir(".");
    expect(entries).toContain("f1");
    expect(entries).toContain(".");
    expect(entries).toContain("..");
  });

  // -------------------------------------------------------------------
  // Mixed relative and absolute paths
  // -------------------------------------------------------------------

  it("absolute paths are unaffected by chdir @fast", () => {
    const { FS } = h;
    FS.mkdir("/abs");
    const s = FS.open("/abs/file.txt", O.WRONLY | O.CREAT, 0o666);
    FS.write(s, encode("absolute"), 0, 8);
    FS.close(s);

    FS.mkdir("/other");
    FS.chdir("/other");

    // Absolute path should still work regardless of cwd
    const stat = FS.stat("/abs/file.txt");
    expect(stat.size).toBe(8);
  });

  it("relative path with subdirectory resolves from cwd", () => {
    const { FS } = h;
    FS.mkdir("/data");
    FS.mkdir("/data/sub");
    const s = FS.open("/data/sub/deep.txt", O.WRONLY | O.CREAT, 0o666);
    FS.write(s, encode("deep"), 0, 4);
    FS.close(s);

    FS.chdir("/data");
    const stat = FS.stat("sub/deep.txt");
    expect(stat.size).toBe(4);
  });

  // -------------------------------------------------------------------
  // Write + read round-trip via relative paths
  // -------------------------------------------------------------------

  it("full write/read cycle via relative paths @fast", () => {
    const { FS } = h;
    FS.mkdir("/pgdata");
    FS.chdir("/pgdata");

    // Create a subdirectory structure like Postgres
    FS.mkdir("base");
    FS.mkdir("base/1");

    // Write a "table file"
    const ws = FS.open("base/1/16384", O.WRONLY | O.CREAT, 0o600);
    const data = new Uint8Array(8192);
    for (let i = 0; i < data.length; i++) data[i] = i & 0xff;
    FS.write(ws, data, 0, data.length);
    FS.close(ws);

    // Read it back via relative path
    const rs = FS.open("base/1/16384", O.RDONLY);
    const buf = new Uint8Array(8192);
    const n = FS.read(rs, buf, 0, buf.length);
    expect(n).toBe(8192);
    for (let i = 0; i < n; i++) {
      if (buf[i] !== (i & 0xff)) {
        throw new Error(`Byte mismatch at offset ${i}: expected ${i & 0xff}, got ${buf[i]}`);
      }
    }
    FS.close(rs);
  });

  // -------------------------------------------------------------------
  // chdir + rename interaction
  // -------------------------------------------------------------------

  it("rename with relative source and absolute target", () => {
    const { FS, E } = h;
    FS.mkdir("/src");
    FS.mkdir("/dst");
    const s = FS.open("/src/file.txt", O.WRONLY | O.CREAT, 0o666);
    FS.write(s, encode("move me"), 0, 7);
    FS.close(s);

    FS.chdir("/src");
    FS.rename("file.txt", "/dst/file.txt");

    expectErrno(() => FS.stat("/src/file.txt"), E.ENOENT);
    expect(FS.stat("/dst/file.txt").size).toBe(7);
  });

  it("rename with absolute source and relative target", () => {
    const { FS, E } = h;
    FS.mkdir("/src");
    FS.mkdir("/dst");
    const s = FS.open("/src/file.txt", O.WRONLY | O.CREAT, 0o666);
    FS.write(s, encode("move me"), 0, 7);
    FS.close(s);

    FS.chdir("/dst");
    FS.rename("/src/file.txt", "file.txt");

    expectErrno(() => FS.stat("/src/file.txt"), E.ENOENT);
    expect(FS.stat("/dst/file.txt").size).toBe(7);
  });

  // -------------------------------------------------------------------
  // chdir survives after directory content changes
  // -------------------------------------------------------------------

  it("cwd remains valid after creating files in it", () => {
    const { FS } = h;
    FS.mkdir("/work");
    FS.chdir("/work");

    for (let i = 0; i < 5; i++) {
      const s = FS.open(`file${i}`, O.WRONLY | O.CREAT, 0o666);
      FS.write(s, encode(`data${i}`), 0, 5);
      FS.close(s);
    }

    expect(FS.cwd()).toBe("/work");
    const entries = FS.readdir(".");
    for (let i = 0; i < 5; i++) {
      expect(entries).toContain(`file${i}`);
    }
  });

  it("cwd remains valid after removing files from it", () => {
    const { FS } = h;
    FS.mkdir("/work");
    for (let i = 0; i < 3; i++) {
      const s = FS.open(`/work/file${i}`, O.WRONLY | O.CREAT, 0o666);
      FS.close(s);
    }

    FS.chdir("/work");
    FS.unlink("file0");
    FS.unlink("file1");

    expect(FS.cwd()).toBe("/work");
    const entries = FS.readdir(".");
    expect(entries).not.toContain("file0");
    expect(entries).not.toContain("file1");
    expect(entries).toContain("file2");
  });

  // -------------------------------------------------------------------
  // Postgres-like chdir + relative path workload
  // -------------------------------------------------------------------

  it("Postgres-style: chdir to datadir, create WAL + base files", () => {
    const { FS } = h;
    FS.mkdir("/pgdata");
    FS.chdir("/pgdata");

    // Create Postgres-like directory structure
    FS.mkdir("base");
    FS.mkdir("base/1");
    FS.mkdir("pg_wal");
    FS.mkdir("global");

    // Write a WAL segment (relative path)
    const wal = FS.open("pg_wal/000000010000000000000001", O.WRONLY | O.CREAT, 0o600);
    const walData = new Uint8Array(1024);
    walData.fill(0xaa);
    FS.write(wal, walData, 0, walData.length);
    FS.close(wal);

    // Write a table file (relative path)
    const tbl = FS.open("base/1/16384", O.WRONLY | O.CREAT, 0o600);
    const tblData = new Uint8Array(8192);
    tblData.fill(0xbb);
    FS.write(tbl, tblData, 0, tblData.length);
    FS.close(tbl);

    // Write a global catalog file (relative path)
    const gbl = FS.open("global/pg_control", O.WRONLY | O.CREAT, 0o600);
    const gblData = new Uint8Array(512);
    gblData.fill(0xcc);
    FS.write(gbl, gblData, 0, gblData.length);
    FS.close(gbl);

    // Verify all files exist via absolute paths
    expect(FS.stat("/pgdata/pg_wal/000000010000000000000001").size).toBe(1024);
    expect(FS.stat("/pgdata/base/1/16384").size).toBe(8192);
    expect(FS.stat("/pgdata/global/pg_control").size).toBe(512);

    // Verify content via relative paths
    const rdWal = FS.open("pg_wal/000000010000000000000001", O.RDONLY);
    const rBuf = new Uint8Array(1024);
    FS.read(rdWal, rBuf, 0, 1024);
    expect(rBuf[0]).toBe(0xaa);
    expect(rBuf[1023]).toBe(0xaa);
    FS.close(rdWal);

    // WAL segment recycling: rename old segment to new name
    FS.rename(
      "pg_wal/000000010000000000000001",
      "pg_wal/000000010000000000000002",
    );
    const entries = FS.readdir("pg_wal");
    expect(entries).toContain("000000010000000000000002");
    expect(entries).not.toContain("000000010000000000000001");
  });
});
