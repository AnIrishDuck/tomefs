/**
 * Conformance tests ported from: emscripten/test/wasmfs/wasmfs_stat.c
 *
 * Tests: fstat on files/dirs/invalid fds, stat by path, lstat,
 *        inode persistence, mode bits, mtime updates after write, utime.
 *
 * Note: We test against the JS FS (MEMFS) which is the non-WASMFS path,
 * so we follow the #else branches in the C source where applicable.
 */
import {
  createFS,
  encode,
  expectErrno,
  O,
  S_IFMT,
  S_IFREG,
  S_IFDIR,
  S_IFLNK,
  S_IRWXUGO,
  type FSHarness,
} from "../harness/emscripten-fs.js";

describe("stat (wasmfs_stat.c)", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  it("fstat on invalid fd throws EBADF @fast", () => {
    const { FS, E } = h;
    expectErrno(() => FS.fstat(-1), E.EBADF);
  });

  it("fstat on a regular file returns correct metadata", () => {
    const { FS } = h;
    const stream = FS.open("/testfile", O.RDWR | O.CREAT, 0o777);
    const stat = FS.fstat(stream.fd);

    expect(stat.size).toBe(0);
    expect(stat.mode & S_IFMT).toBe(S_IFREG);
    expect(stat.nlink).toBeGreaterThanOrEqual(1);
    expect(stat.uid).toBe(0);
    expect(stat.gid).toBe(0);
    expect(stat.blksize).toBe(4096);

    FS.close(stream);
  });

  it("fstat on a directory returns correct metadata @fast", () => {
    const { FS } = h;
    const stream = FS.open("/dev", O.RDONLY | O.DIRECTORY);
    const stat = FS.fstat(stream.fd);

    expect(stat.mode & S_IFMT).toBe(S_IFDIR);
    expect(stat.dev).toBeTruthy();
    expect(stat.nlink).toBeGreaterThanOrEqual(1);
    expect(stat.uid).toBe(0);
    expect(stat.gid).toBe(0);
    expect(stat.rdev).toBe(0);
    expect(stat.blksize).toBe(4096);

    FS.close(stream);
  });

  it("inode is persistent across reopen of same file", () => {
    const { FS } = h;
    const stream1 = FS.open("/dev", O.RDONLY | O.DIRECTORY);
    const inode1 = FS.fstat(stream1.fd).ino;
    FS.close(stream1);

    const stream2 = FS.open("/dev", O.RDONLY | O.DIRECTORY);
    const inode2 = FS.fstat(stream2.fd).ino;
    FS.close(stream2);

    expect(inode1).toBe(inode2);
  });

  it("stat by path on a file works without opening", () => {
    const { FS } = h;
    // Create a file first
    const stream = FS.open("/statme", O.RDWR | O.CREAT, 0o777);
    FS.close(stream);

    const stat = FS.stat("/statme");
    expect(stat.size).toBe(0);
    expect(stat.mode & S_IFMT).toBe(S_IFREG);
    expect(stat.dev).toBeTruthy();
    expect(stat.nlink).toBeGreaterThanOrEqual(1);
    expect(stat.uid).toBe(0);
    expect(stat.gid).toBe(0);
    expect(stat.blksize).toBe(4096);
  });

  it("stat by path on a directory works without opening", () => {
    const { FS } = h;
    const stat = FS.stat("/dev");

    expect(stat.mode & S_IFMT).toBe(S_IFDIR);
    expect(stat.dev).toBeTruthy();
    expect(stat.nlink).toBeGreaterThanOrEqual(1);
    expect(stat.uid).toBe(0);
    expect(stat.gid).toBe(0);
    expect(stat.rdev).toBe(0);
    expect(stat.blksize).toBe(4096);
  });

  it("stat on empty path throws ENOENT", () => {
    const { FS, E } = h;
    expectErrno(() => FS.stat(""), E.ENOENT);
  });

  it("stat on non-existent path throws ENOENT", () => {
    const { FS, E } = h;
    expectErrno(() => FS.stat("/non-existent"), E.ENOENT);
  });

  it("lstat on a symlink returns the link itself (S_IFLNK)", () => {
    const { FS } = h;
    // /dev/stdout is a symlink to /dev/tty in Emscripten's MEMFS
    const lstat = FS.lstat("/dev/stdout");
    expect(lstat.mode & S_IFMT).toBe(S_IFLNK);
    expect(lstat.nlink).toBeGreaterThanOrEqual(1);
    expect(lstat.uid).toBe(0);
    expect(lstat.gid).toBe(0);
    expect(lstat.blksize).toBe(4096);
    expect(lstat.rdev).toBe(0);
    // The JS FS reports symlink size as the length of the target path
    // /dev/stdout -> /dev/tty (8 bytes)
    expect(lstat.size).toBe(8);
  });

  it("lstat on a directory returns S_IFDIR (not S_IFLNK)", () => {
    const { FS } = h;
    const lstat = FS.lstat("/dev");
    expect(lstat.mode & S_IFMT).toBe(S_IFDIR);
    expect(lstat.dev).toBeTruthy();
    expect(lstat.nlink).toBeGreaterThanOrEqual(1);
    expect(lstat.uid).toBe(0);
    expect(lstat.gid).toBe(0);
    expect(lstat.rdev).toBe(0);
    expect(lstat.blksize).toBe(4096);
  });

  it("mtime is unchanged after zero-length write", () => {
    const { FS } = h;
    // Create file and set a known mtime via utime
    // FS.utime takes milliseconds (JS convention), not seconds (POSIX)
    const stream = FS.open("/mtimefile", O.WRONLY | O.CREAT | O.EXCL, 0o777);
    const testTimeMs = 1000000000; // 1e9 ms
    FS.utime("/mtimefile", testTimeMs, testTimeMs);

    // Zero-length write should not update mtime
    FS.write(stream, new Uint8Array(0), 0, 0);
    const stat1 = FS.fstat(stream.fd);
    expect(stat1.mtime.getTime()).toBe(testTimeMs);

    FS.close(stream);
  });

  it("mtime is updated after a real write", () => {
    const { FS } = h;
    // Create file and set a known mtime
    const stream = FS.open("/mtimefile2", O.WRONLY | O.CREAT | O.EXCL, 0o777);
    const testTimeMs = 1000000000; // 1e9 ms
    FS.utime("/mtimefile2", testTimeMs, testTimeMs);

    // Verify mtime was set
    let stat = FS.fstat(stream.fd);
    expect(stat.mtime.getTime()).toBe(testTimeMs);

    // Actual write should update mtime
    FS.write(stream, encode("abcdef"), 0, 6);
    stat = FS.fstat(stream.fd);
    expect(stat.mtime.getTime()).not.toBe(testTimeMs);

    FS.close(stream);
  });

  it("utime sets atime and mtime", () => {
    const { FS } = h;
    const stream = FS.open("/utimefile", O.WRONLY | O.CREAT, 0o777);
    FS.close(stream);

    // FS.utime takes milliseconds
    const atimeMs = 500000000;
    const mtimeMs = 600000000;
    FS.utime("/utimefile", atimeMs, mtimeMs);

    const stat = FS.stat("/utimefile");
    expect(stat.atime.getTime()).toBe(atimeMs);
    expect(stat.mtime.getTime()).toBe(mtimeMs);
  });

  it("file size reflects written data", () => {
    const { FS } = h;
    const stream = FS.open("/sizefile", O.RDWR | O.CREAT, 0o777);

    expect(FS.fstat(stream.fd).size).toBe(0);

    FS.write(stream, encode("hello"), 0, 5);
    expect(FS.fstat(stream.fd).size).toBe(5);

    FS.write(stream, encode("world!"), 0, 6);
    expect(FS.fstat(stream.fd).size).toBe(11);

    FS.close(stream);
  });
});
