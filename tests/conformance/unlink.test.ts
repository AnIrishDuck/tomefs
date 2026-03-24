/**
 * Conformance tests ported from: emscripten/test/unistd/unlink.c
 *
 * Tests: unlink files (ENOENT, EISDIR, EACCES, read-only files),
 *        rmdir (ENOENT, ENOTDIR, EACCES, ENOTEMPTY, EBUSY),
 *        symlink unlink doesn't follow link.
 */
import {
  createFS,
  encode,
  expectErrno,
  O,
  type FSHarness,
} from "../harness/emscripten-fs.js";

describe("unlink (unlink.c)", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
    const { FS } = h;

    // Replicate the C test's setup() — all under /working
    FS.mkdir("/working", 0o777);

    // create_file("file", "test", 0777)
    const s1 = FS.open("/working/file", O.WRONLY | O.CREAT | O.EXCL, 0o777);
    FS.write(s1, encode("test"), 0, 4);
    FS.close(s1);

    // create_file("file1", "test", 0777)
    const s2 = FS.open("/working/file1", O.WRONLY | O.CREAT | O.EXCL, 0o777);
    FS.write(s2, encode("test"), 0, 4);
    FS.close(s2);

    // create_file("file-readonly", "test", 0777) then chmod to 0555
    const s3 = FS.open(
      "/working/file-readonly",
      O.WRONLY | O.CREAT | O.EXCL,
      0o777,
    );
    FS.write(s3, encode("test"), 0, 4);
    FS.close(s3);
    FS.chmod("/working/file-readonly", 0o555);

    // symlink("file1", "file1-link")
    FS.symlink("/working/file1", "/working/file1-link");

    // dir-empty
    FS.mkdir("/working/dir-empty", 0o777);

    // symlink("dir-empty", "dir-empty-link")
    FS.symlink("/working/dir-empty", "/working/dir-empty-link");

    // dir-readonly with children, then chmod 0555
    FS.mkdir("/working/dir-readonly", 0o777);
    const s4 = FS.open(
      "/working/dir-readonly/anotherfile",
      O.WRONLY | O.CREAT | O.EXCL,
      0o777,
    );
    FS.write(s4, encode("test"), 0, 4);
    FS.close(s4);
    FS.mkdir("/working/dir-readonly/anotherdir", 0o777);
    FS.chmod("/working/dir-readonly", 0o555);

    // dir-full with a file
    FS.mkdir("/working/dir-full", 0o777);
    const s5 = FS.open(
      "/working/dir-full/anotherfile",
      O.WRONLY | O.CREAT | O.EXCL,
      0o777,
    );
    FS.write(s5, encode("test"), 0, 4);
    FS.close(s5);
  });

  // --- unlink tests ---

  it("unlink non-existent file throws ENOENT @fast", () => {
    const { FS, E } = h;
    expectErrno(() => FS.unlink("/working/noexist"), E.ENOENT);
  });

  it("unlink with non-existent parent throws ENOENT", () => {
    const { FS, E } = h;
    expectErrno(() => FS.unlink("/working/noexist/foo"), E.ENOENT);
  });

  it("unlink empty path throws ENOENT", () => {
    const { FS, E } = h;
    expectErrno(() => FS.unlink(""), E.ENOENT);
  });

  it("unlink a directory throws EISDIR @fast", () => {
    const { FS, E } = h;
    // Emscripten JS FS returns EISDIR when unlinking a directory
    expectErrno(() => FS.unlink("/working/dir-readonly"), E.EISDIR);
  });

  it("unlink file in read-only directory throws EACCES", () => {
    const { FS, E } = h;
    expectErrno(
      () => FS.unlink("/working/dir-readonly/anotherfile"),
      E.EACCES,
    );
  });

  it("unlink a symlink does not remove the target @fast", () => {
    const { FS, E } = h;
    // Unlinking the symlink should remove the link, not the target
    FS.unlink("/working/file1-link");

    // Target file1 should still exist
    const stat = FS.stat("/working/file1");
    expect(stat.size).toBe(4);

    // The link itself should be gone
    expectErrno(() => FS.lstat("/working/file1-link"), E.ENOENT);
  });

  it("unlink a regular file succeeds", () => {
    const { FS, E } = h;
    FS.unlink("/working/file");
    expectErrno(() => FS.stat("/working/file"), E.ENOENT);
  });

  it("unlink a read-only file succeeds (permission is on parent dir)", () => {
    const { FS, E } = h;
    // A read-only file can be deleted if the parent directory is writable
    FS.unlink("/working/file-readonly");
    expectErrno(() => FS.stat("/working/file-readonly"), E.ENOENT);
  });

  // --- rmdir tests ---

  it("rmdir non-existent path throws ENOENT @fast", () => {
    const { FS, E } = h;
    expectErrno(() => FS.rmdir("/working/noexist"), E.ENOENT);
  });

  it("rmdir on a file throws ENOTDIR", () => {
    const { FS, E } = h;
    expectErrno(() => FS.rmdir("/working/file1"), E.ENOTDIR);
  });

  it("rmdir in read-only directory throws EACCES", () => {
    const { FS, E } = h;
    expectErrno(
      () => FS.rmdir("/working/dir-readonly/anotherdir"),
      E.EACCES,
    );
  });

  it("rmdir non-empty directory throws ENOTEMPTY @fast", () => {
    const { FS, E } = h;
    expectErrno(() => FS.rmdir("/working/dir-full"), E.ENOTEMPTY);
  });

  it("rmdir root throws EBUSY", () => {
    const { FS, E } = h;
    // Emscripten JS FS returns EBUSY for rmdir("/")
    // Note: also accepts EISDIR per platform differences
    try {
      FS.rmdir("/");
      throw new Error("Expected error but none thrown");
    } catch (e: unknown) {
      if (e instanceof Error && "errno" in e) {
        const err = e as Error & { errno: number };
        // Accept EBUSY or EISDIR (platform-dependent)
        expect([h.E.EBUSY, h.E.EISDIR]).toContain(err.errno);
      } else {
        throw e;
      }
    }
  });

  it("rmdir on a symlink to a directory throws ENOTDIR", () => {
    const { FS, E } = h;
    // rmdir should not follow the symlink — it sees a non-directory entry
    expectErrno(() => FS.rmdir("/working/dir-empty-link"), E.ENOTDIR);
  });

  it("rmdir empty directory succeeds @fast", () => {
    const { FS, E } = h;
    FS.rmdir("/working/dir-empty");
    expectErrno(() => FS.stat("/working/dir-empty"), E.ENOENT);
  });

  it("readdir reflects unlinked files and removed directories", () => {
    const { FS } = h;
    // Remove a file and a directory, verify readdir updates
    FS.unlink("/working/file");
    FS.rmdir("/working/dir-empty");

    const entries = FS.readdir("/working");
    expect(entries).not.toContain("file");
    expect(entries).not.toContain("dir-empty");
    // Other entries should still be present
    expect(entries).toContain("file1");
    expect(entries).toContain("dir-full");
  });
});
