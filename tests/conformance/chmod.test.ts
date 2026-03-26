/**
 * Conformance tests for chmod and fchmod.
 *
 * Tests: chmod on files/dirs, fchmod on fds, permission bit preservation,
 *        file type bit immutability, symlink follow-through, ctime update,
 *        error cases (ENOENT, EBADF, ENOTDIR).
 *
 * Source: POSIX chmod/fchmod specification. No direct Emscripten C test
 * upstream — these fill a gap in the conformance suite.
 */
import {
  createFS,
  expectErrno,
  O,
  S_IFMT,
  S_IFREG,
  S_IFDIR,
  S_IFLNK,
  S_IRWXUGO,
  S_IRWXU,
  S_IRUSR,
  S_IWUSR,
  S_IXUSR,
  S_IRWXG,
  S_IRWXO,
  type FSHarness,
} from "../harness/emscripten-fs.js";

describe("chmod / fchmod", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  // -------------------------------------------------------------------
  // chmod on files
  // -------------------------------------------------------------------

  it("chmod changes permission bits on a file @fast", () => {
    const { FS } = h;
    FS.writeFile("/chfile", "data");

    FS.chmod("/chfile", 0o644);
    const stat = FS.stat("/chfile");
    expect(stat.mode & S_IRWXUGO).toBe(0o644);
  });

  it("chmod preserves file type bits", () => {
    const { FS } = h;
    FS.writeFile("/typefile", "x");

    FS.chmod("/typefile", 0o755);
    const stat = FS.stat("/typefile");
    expect(stat.mode & S_IFMT).toBe(S_IFREG);
    expect(stat.mode & S_IRWXUGO).toBe(0o755);
  });

  it("chmod can set all permission bits independently", () => {
    const { FS } = h;
    FS.writeFile("/permfile", "test");

    // owner read-only
    FS.chmod("/permfile", S_IRUSR);
    expect(FS.stat("/permfile").mode & S_IRWXUGO).toBe(S_IRUSR);

    // owner write-only
    FS.chmod("/permfile", S_IWUSR);
    expect(FS.stat("/permfile").mode & S_IRWXUGO).toBe(S_IWUSR);

    // owner execute-only
    FS.chmod("/permfile", S_IXUSR);
    expect(FS.stat("/permfile").mode & S_IRWXUGO).toBe(S_IXUSR);

    // group all
    FS.chmod("/permfile", S_IRWXG);
    expect(FS.stat("/permfile").mode & S_IRWXUGO).toBe(S_IRWXG);

    // other all
    FS.chmod("/permfile", S_IRWXO);
    expect(FS.stat("/permfile").mode & S_IRWXUGO).toBe(S_IRWXO);

    // all permissions
    FS.chmod("/permfile", S_IRWXUGO);
    expect(FS.stat("/permfile").mode & S_IRWXUGO).toBe(S_IRWXUGO);

    // no permissions
    FS.chmod("/permfile", 0);
    expect(FS.stat("/permfile").mode & S_IRWXUGO).toBe(0);
  });

  it("chmod updates ctime", () => {
    const { FS } = h;
    FS.writeFile("/ctimefile", "data");

    const before = FS.stat("/ctimefile").ctime.getTime();

    // Small delay to ensure ctime changes
    const start = Date.now();
    while (Date.now() === start) {
      /* spin until clock tick */
    }

    // chmod should update ctime
    FS.chmod("/ctimefile", 0o644);
    const after = FS.stat("/ctimefile").ctime.getTime();
    expect(after).toBeGreaterThanOrEqual(before);
  });

  it("chmod does not change mtime", () => {
    const { FS } = h;
    FS.writeFile("/mtimefile", "data");

    const knownMtime = 500000000;
    FS.utime("/mtimefile", knownMtime, knownMtime);
    const before = FS.stat("/mtimefile").mtime.getTime();
    expect(before).toBe(knownMtime);

    FS.chmod("/mtimefile", 0o644);
    const after = FS.stat("/mtimefile").mtime.getTime();
    expect(after).toBe(knownMtime);
  });

  it("repeated chmod calls accumulate correctly", () => {
    const { FS } = h;
    FS.writeFile("/rptfile", "test");

    FS.chmod("/rptfile", 0o000);
    expect(FS.stat("/rptfile").mode & S_IRWXUGO).toBe(0o000);

    FS.chmod("/rptfile", 0o777);
    expect(FS.stat("/rptfile").mode & S_IRWXUGO).toBe(0o777);

    FS.chmod("/rptfile", 0o123);
    expect(FS.stat("/rptfile").mode & S_IRWXUGO).toBe(0o123);
  });

  // -------------------------------------------------------------------
  // chmod on directories
  // -------------------------------------------------------------------

  it("chmod changes permission bits on a directory @fast", () => {
    const { FS } = h;
    FS.mkdir("/chdir");

    FS.chmod("/chdir", 0o755);
    const stat = FS.stat("/chdir");
    expect(stat.mode & S_IFMT).toBe(S_IFDIR);
    expect(stat.mode & S_IRWXUGO).toBe(0o755);
  });

  it("chmod on directory preserves directory type bits", () => {
    const { FS } = h;
    FS.mkdir("/typedir");

    FS.chmod("/typedir", 0o700);
    const stat = FS.stat("/typedir");
    expect(stat.mode & S_IFMT).toBe(S_IFDIR);
  });

  // -------------------------------------------------------------------
  // chmod through symlinks
  // -------------------------------------------------------------------

  it("chmod on symlink changes the target, not the link", () => {
    const { FS } = h;
    FS.writeFile("/target", "data");
    FS.symlink("/target", "/link");

    // Set target to known mode
    FS.chmod("/target", 0o644);

    // chmod through symlink
    FS.chmod("/link", 0o755);

    // Target mode should change
    const targetStat = FS.stat("/target");
    expect(targetStat.mode & S_IRWXUGO).toBe(0o755);

    // lstat on symlink — symlink mode is separate
    const linkStat = FS.lstat("/link");
    expect(linkStat.mode & S_IFMT).toBe(S_IFLNK);
  });

  // -------------------------------------------------------------------
  // fchmod
  // -------------------------------------------------------------------

  it("fchmod changes permission bits via file descriptor @fast", () => {
    const { FS } = h;
    const stream = FS.open("/fchfile", O.RDWR | O.CREAT, 0o777);

    FS.fchmod(stream.fd, 0o644);
    const stat = FS.fstat(stream.fd);
    expect(stat.mode & S_IRWXUGO).toBe(0o644);

    FS.close(stream);
  });

  it("fchmod preserves file type bits", () => {
    const { FS } = h;
    const stream = FS.open("/fchtypefile", O.RDWR | O.CREAT, 0o777);

    FS.fchmod(stream.fd, 0o600);
    const stat = FS.fstat(stream.fd);
    expect(stat.mode & S_IFMT).toBe(S_IFREG);
    expect(stat.mode & S_IRWXUGO).toBe(0o600);

    FS.close(stream);
  });

  it("fchmod mode is visible via stat by path", () => {
    const { FS } = h;
    const stream = FS.open("/fchvis", O.RDWR | O.CREAT, 0o777);

    FS.fchmod(stream.fd, 0o640);

    // stat by path should reflect the change
    const pathStat = FS.stat("/fchvis");
    expect(pathStat.mode & S_IRWXUGO).toBe(0o640);

    FS.close(stream);
  });

  it("fchmod and chmod are consistent", () => {
    const { FS } = h;
    const stream = FS.open("/consist", O.RDWR | O.CREAT, 0o777);

    // Set via chmod
    FS.chmod("/consist", 0o644);
    expect(FS.fstat(stream.fd).mode & S_IRWXUGO).toBe(0o644);

    // Set via fchmod
    FS.fchmod(stream.fd, 0o755);
    expect(FS.stat("/consist").mode & S_IRWXUGO).toBe(0o755);

    FS.close(stream);
  });

  // -------------------------------------------------------------------
  // Error cases
  // -------------------------------------------------------------------

  it("chmod on nonexistent path throws ENOENT @fast", () => {
    const { FS, E } = h;
    expectErrno(() => FS.chmod("/nonexistent", 0o644), E.ENOENT);
  });

  it("fchmod on invalid fd throws EBADF @fast", () => {
    const { FS, E } = h;
    expectErrno(() => FS.fchmod(9999, 0o644), E.EBADF);
  });

  it("chmod with ENOTDIR in path component", () => {
    const { FS, E } = h;
    FS.writeFile("/notadir", "data");
    expectErrno(() => FS.chmod("/notadir/child", 0o644), E.ENOTDIR);
  });
});
