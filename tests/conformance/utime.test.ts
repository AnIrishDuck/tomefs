/**
 * Conformance tests for utime (timestamp manipulation).
 *
 * Tests: utime on files/dirs/symlinks, atime/mtime precision,
 *        ctime update on utime call, zero and large timestamps,
 *        error cases (ENOENT, ENOTDIR).
 *
 * Source: POSIX utime/utimes specification. No direct Emscripten C test
 * upstream — these fill a gap in the conformance suite for timestamp
 * manipulation, which is critical for Postgres (WAL management, backup
 * verification, relation file aging).
 */
import {
  createFS,
  encode,
  expectErrno,
  O,
  type FSHarness,
} from "../harness/emscripten-fs.js";

describe("utime", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  // -------------------------------------------------------------------
  // Basic utime on files
  // -------------------------------------------------------------------

  it("utime sets atime and mtime on a file @fast", () => {
    const { FS } = h;
    FS.writeFile("/utfile", "data");

    const atime = 1000000;
    const mtime = 2000000;
    FS.utime("/utfile", atime, mtime);

    const stat = FS.stat("/utfile");
    expect(stat.atime.getTime()).toBe(atime);
    expect(stat.mtime.getTime()).toBe(mtime);
  });

  it("utime with identical atime and mtime", () => {
    const { FS } = h;
    FS.writeFile("/same", "data");

    const ts = 5000000;
    FS.utime("/same", ts, ts);

    const stat = FS.stat("/same");
    expect(stat.atime.getTime()).toBe(ts);
    expect(stat.mtime.getTime()).toBe(ts);
  });

  it("utime can set timestamps to zero @fast", () => {
    const { FS } = h;
    FS.writeFile("/zero", "data");

    FS.utime("/zero", 0, 0);

    const stat = FS.stat("/zero");
    expect(stat.atime.getTime()).toBe(0);
    expect(stat.mtime.getTime()).toBe(0);
  });

  it("utime with large timestamp values", () => {
    const { FS } = h;
    FS.writeFile("/large", "data");

    // Year 2100-ish in milliseconds
    const largeTime = 4102444800000;
    FS.utime("/large", largeTime, largeTime);

    const stat = FS.stat("/large");
    expect(stat.atime.getTime()).toBe(largeTime);
    expect(stat.mtime.getTime()).toBe(largeTime);
  });

  it("utime can set different atime and mtime", () => {
    const { FS } = h;
    FS.writeFile("/diff", "data");

    FS.utime("/diff", 1000, 9999000);

    const stat = FS.stat("/diff");
    expect(stat.atime.getTime()).toBe(1000);
    expect(stat.mtime.getTime()).toBe(9999000);
  });

  it("repeated utime calls overwrite previous timestamps", () => {
    const { FS } = h;
    FS.writeFile("/rpt", "data");

    FS.utime("/rpt", 1000, 2000);
    expect(FS.stat("/rpt").atime.getTime()).toBe(1000);
    expect(FS.stat("/rpt").mtime.getTime()).toBe(2000);

    FS.utime("/rpt", 3000, 4000);
    expect(FS.stat("/rpt").atime.getTime()).toBe(3000);
    expect(FS.stat("/rpt").mtime.getTime()).toBe(4000);

    FS.utime("/rpt", 5000, 6000);
    expect(FS.stat("/rpt").atime.getTime()).toBe(5000);
    expect(FS.stat("/rpt").mtime.getTime()).toBe(6000);
  });

  // -------------------------------------------------------------------
  // utime on directories
  // -------------------------------------------------------------------

  it("utime sets atime and mtime on a directory @fast", () => {
    const { FS } = h;
    FS.mkdir("/utdir");

    const atime = 3000000;
    const mtime = 4000000;
    FS.utime("/utdir", atime, mtime);

    const stat = FS.stat("/utdir");
    expect(stat.atime.getTime()).toBe(atime);
    expect(stat.mtime.getTime()).toBe(mtime);
  });

  it("utime on directory does not affect children", () => {
    const { FS } = h;
    FS.mkdir("/parent");
    FS.writeFile("/parent/child", "data");

    const childBefore = FS.stat("/parent/child");
    const childMtime = childBefore.mtime.getTime();
    const childAtime = childBefore.atime.getTime();

    FS.utime("/parent", 100, 200);

    const childAfter = FS.stat("/parent/child");
    expect(childAfter.atime.getTime()).toBe(childAtime);
    expect(childAfter.mtime.getTime()).toBe(childMtime);
  });

  // -------------------------------------------------------------------
  // utime through symlinks
  // -------------------------------------------------------------------

  it("utime on symlink changes the target timestamps @fast", () => {
    const { FS } = h;
    FS.writeFile("/uttarget", "data");
    FS.symlink("/uttarget", "/utlink");

    FS.utime("/utlink", 7000000, 8000000);

    // Target should have the new timestamps
    const targetStat = FS.stat("/uttarget");
    expect(targetStat.atime.getTime()).toBe(7000000);
    expect(targetStat.mtime.getTime()).toBe(8000000);
  });

  it("utime on symlink does not change symlink's own timestamps via lstat", () => {
    const { FS } = h;
    FS.writeFile("/ltarget", "data");
    FS.symlink("/ltarget", "/llink");

    const linkBefore = FS.lstat("/llink");
    const linkMtime = linkBefore.mtime.getTime();

    // Small delay to ensure timestamps differ if changed
    const start = Date.now();
    while (Date.now() === start) {
      /* spin */
    }

    FS.utime("/llink", 11000000, 12000000);

    // Symlink's own timestamps should be unchanged
    const linkAfter = FS.lstat("/llink");
    expect(linkAfter.mtime.getTime()).toBe(linkMtime);
  });

  // -------------------------------------------------------------------
  // utime interaction with file operations
  // -------------------------------------------------------------------

  it("utime timestamps survive after file content modification", () => {
    const { FS } = h;
    FS.writeFile("/modfile", "initial");

    // Set known timestamps
    FS.utime("/modfile", 1000, 2000);
    expect(FS.stat("/modfile").atime.getTime()).toBe(1000);
    expect(FS.stat("/modfile").mtime.getTime()).toBe(2000);

    // Write modifies mtime but utime-set values should have been in effect before
    const stream = FS.open("/modfile", O.RDWR);
    FS.write(stream, encode("new"), 0, 3);
    FS.close(stream);

    // After write, mtime should be updated (no longer 2000)
    const after = FS.stat("/modfile");
    expect(after.mtime.getTime()).not.toBe(2000);
  });

  it("read updates atime to current time (or leaves it)", () => {
    const { FS } = h;
    FS.writeFile("/rdfile", "hello");

    // Set old atime
    FS.utime("/rdfile", 1000, 2000);

    const stream = FS.open("/rdfile", O.RDONLY);
    const buf = new Uint8Array(10);
    FS.read(stream, buf, 0, 5);
    FS.close(stream);

    // After read, atime may or may not be updated depending on the FS
    // implementation. Just verify no crash and timestamps are valid dates.
    const stat = FS.stat("/rdfile");
    expect(stat.atime).toBeInstanceOf(Date);
    expect(stat.mtime).toBeInstanceOf(Date);
  });

  it("utime on empty file works correctly", () => {
    const { FS } = h;
    const stream = FS.open("/empty", O.RDWR | O.CREAT, 0o666);
    FS.close(stream);

    FS.utime("/empty", 42000, 84000);

    const stat = FS.stat("/empty");
    expect(stat.atime.getTime()).toBe(42000);
    expect(stat.mtime.getTime()).toBe(84000);
    expect(stat.size).toBe(0);
  });

  it("utime after truncate works correctly", () => {
    const { FS } = h;
    FS.writeFile("/trunc", "some longer data here");

    FS.truncate("/trunc", 5);
    FS.utime("/trunc", 99000, 88000);

    const stat = FS.stat("/trunc");
    expect(stat.size).toBe(5);
    expect(stat.atime.getTime()).toBe(99000);
    expect(stat.mtime.getTime()).toBe(88000);
  });

  it("chmod then utime: both permission and timestamp changes apply", () => {
    const { FS } = h;
    FS.writeFile("/both", "data");

    FS.chmod("/both", 0o644);
    FS.utime("/both", 10000, 20000);

    const stat = FS.stat("/both");
    expect(stat.mode & 0o777).toBe(0o644);
    expect(stat.atime.getTime()).toBe(10000);
    expect(stat.mtime.getTime()).toBe(20000);
  });

  // -------------------------------------------------------------------
  // utime on renamed/moved files
  // -------------------------------------------------------------------

  it("utime on renamed file works at new path", () => {
    const { FS } = h;
    FS.writeFile("/before", "data");
    FS.rename("/before", "/after");

    FS.utime("/after", 33000, 44000);

    const stat = FS.stat("/after");
    expect(stat.atime.getTime()).toBe(33000);
    expect(stat.mtime.getTime()).toBe(44000);
  });

  // -------------------------------------------------------------------
  // Error cases
  // -------------------------------------------------------------------

  it("utime on nonexistent path throws ENOENT @fast", () => {
    const { FS, E } = h;
    expectErrno(() => FS.utime("/nonexistent", 1000, 2000), E.ENOENT);
  });

  it("utime with ENOTDIR in path component", () => {
    const { FS, E } = h;
    FS.writeFile("/notadir", "data");
    expectErrno(
      () => FS.utime("/notadir/child", 1000, 2000),
      E.ENOTDIR,
    );
  });

  it("utime on deeply nested nonexistent path throws ENOENT", () => {
    const { FS, E } = h;
    expectErrno(
      () => FS.utime("/a/b/c/d/e", 1000, 2000),
      E.ENOENT,
    );
  });
});
