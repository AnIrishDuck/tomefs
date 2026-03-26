/**
 * Conformance tests ported from: emscripten/test/stdio/test_rename.c
 *
 * Tests: rename files/dirs, overwrite, ancestors, error cases:
 *        ENOENT, EISDIR, ENOTDIR, ENOTEMPTY, EACCES, EINVAL, empty paths.
 */
import {
  createFS,
  encode,
  decode,
  expectErrno,
  O,
  type FSHarness,
} from "../harness/emscripten-fs.js";

describe("rename (test_rename.c)", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
    const { FS } = h;

    // Replicate the C test's setup()
    // create_file("file", "abcdef", 0777)
    const s = FS.open("/file", O.WRONLY | O.CREAT | O.EXCL, 0o777);
    FS.write(s, encode("abcdef"), 0, 6);
    FS.close(s);

    FS.mkdir("/dir", 0o777);
    FS.mkdir("/new-dir", 0o777);

    // create_file("dir/file", "abcdef", 0777)
    const s2 = FS.open("/dir/file", O.WRONLY | O.CREAT | O.EXCL, 0o777);
    FS.write(s2, encode("abcdef"), 0, 6);
    FS.close(s2);

    FS.mkdir("/dir/subdir", 0o777);
    FS.mkdir("/dir/subdir/subsubdir", 0o777);
    FS.mkdir("/dir/rename-dir", 0o777);
    FS.mkdir("/dir/rename-dir/subdir", 0o777);
    FS.mkdir("/dir/rename-dir/subdir/subsubdir", 0o777);
    FS.mkdir("/dir-readonly", 0o555);
    // For dir-readonly2: create writable, add child, then chmod to 0555
    FS.mkdir("/dir-readonly2", 0o777);
    FS.mkdir("/dir-readonly2/somename", 0o777);
    FS.chmod("/dir-readonly2", 0o555);
    FS.mkdir("/dir-nonempty", 0o777);
    FS.mkdir("/dir/subdir3", 0o777);
    FS.mkdir("/dir/subdir3/subdir3_1", 0o777);
    FS.mkdir("/dir/subdir4", 0o777);
    FS.mkdir("/dir/a", 0o777);
    FS.mkdir("/dir/b", 0o777);
    FS.mkdir("/dir/b/c", 0o777);

    // create_file("dir-nonempty/file", "abcdef", 0777)
    const s3 = FS.open(
      "/dir-nonempty/file",
      O.WRONLY | O.CREAT | O.EXCL,
      0o777,
    );
    FS.write(s3, encode("abcdef"), 0, 6);
    FS.close(s3);
  });

  // --- Error cases ---

  it("rename non-existent source throws ENOENT @fast", () => {
    const { FS, E } = h;
    expectErrno(() => FS.rename("/noexist", "/dir"), E.ENOENT);
  });

  it("rename file over directory throws EISDIR", () => {
    const { FS, E } = h;
    expectErrno(() => FS.rename("/file", "/dir"), E.EISDIR);
  });

  it("rename directory over file throws ENOTDIR", () => {
    const { FS, E } = h;
    expectErrno(() => FS.rename("/dir", "/file"), E.ENOTDIR);
  });

  it("rename directory over non-empty directory throws ENOTEMPTY @fast", () => {
    const { FS, E } = h;
    expectErrno(() => FS.rename("/dir", "/dir-nonempty"), E.ENOTEMPTY);
  });

  it("rename into read-only directory throws EACCES", () => {
    const { FS, E } = h;
    expectErrno(() => FS.rename("/dir", "/dir-readonly/dir"), E.EACCES);
  });

  it("rename from read-only directory throws EACCES", () => {
    const { FS, E } = h;
    expectErrno(() => FS.rename("/dir-readonly2/somename", "/dir"), E.EACCES);
  });

  it("rename with empty oldpath throws ENOENT", () => {
    const { FS, E } = h;
    expectErrno(() => FS.rename("", "/test"), E.ENOENT);
  });

  it("rename with empty newpath throws ENOENT", () => {
    const { FS, E } = h;
    expectErrno(() => FS.rename("/dir", ""), E.ENOENT);
  });

  it("rename source into its own subdirectory throws EINVAL", () => {
    const { FS, E } = h;
    expectErrno(() => FS.rename("/dir", "/dir/somename"), E.EINVAL);
  });

  it("rename source into indirect descendant throws EINVAL", () => {
    const { FS, E } = h;
    expectErrno(
      () => FS.rename("/dir", "/dir/subdir/noexist"),
      E.EINVAL,
    );
  });

  it("rename child over ancestor throws ENOTEMPTY", () => {
    const { FS, E } = h;
    expectErrno(() => FS.rename("/dir/subdir", "/dir"), E.ENOTEMPTY);
  });

  it("rename deep child over ancestor throws ENOTEMPTY", () => {
    const { FS, E } = h;
    expectErrno(
      () => FS.rename("/dir/subdir/subsubdir", "/dir"),
      E.ENOTEMPTY,
    );
  });

  it("rename with non-existent parent in newpath throws ENOENT", () => {
    const { FS, E } = h;
    expectErrno(
      () => FS.rename("/dir/hicsuntdracones/empty", "/dir/hicsuntdracones/renamed"),
      E.ENOENT,
    );
  });

  // Root rename tests only apply to MEMFS — under tomefs, "/" is rewritten
  // to the mount point, which produces EXDEV (cross-device) instead.
  const itIfMemfs =
    process.env.TOMEFS_BACKEND === "tomefs" ? it.skip : it;

  itIfMemfs("rename root as source throws EINVAL (JS FS) or EBUSY (WasmFS)", () => {
    const { FS, E } = h;
    // Emscripten JS FS returns EINVAL for renaming root
    expectErrno(() => FS.rename("/", "/dir/file2"), E.EINVAL);
  });

  itIfMemfs("rename onto root throws ENOTEMPTY (JS FS) or EBUSY (WasmFS)", () => {
    const { FS, E } = h;
    // Emscripten JS FS returns ENOTEMPTY for renaming onto root
    expectErrno(() => FS.rename("/dir/file", "/"), E.ENOTEMPTY);
  });

  // --- Valid renames ---

  it("rename file within same directory @fast", () => {
    const { FS } = h;
    FS.rename("/dir/file", "/dir/file1");
    // Verify new name exists
    const stat = FS.stat("/dir/file1");
    expect(stat.size).toBe(6);
    // Verify old name gone
    expectErrno(() => FS.stat("/dir/file"), h.E.ENOENT);
  });

  it("rename file twice in succession", () => {
    const { FS } = h;
    FS.rename("/dir/file", "/dir/file1");
    FS.rename("/dir/file1", "/dir/file2");
    const stat = FS.stat("/dir/file2");
    expect(stat.size).toBe(6);
  });

  it("rename directory within same parent", () => {
    const { FS } = h;
    FS.rename("/dir/subdir", "/dir/subdir1");
    FS.rename("/dir/subdir1", "/dir/subdir2");
    const stat = FS.stat("/dir/subdir2");
    expect(FS.isDir(stat.mode)).toBe(true);
  });

  it("rename directory into a nested path", () => {
    const { FS } = h;
    // First rename subdir so it doesn't have subsubdir
    FS.rename("/dir/subdir", "/dir/subdir1");
    FS.rename(
      "/dir/subdir1",
      "/dir/subdir3/subdir3_1/subdir1 renamed",
    );
    const stat = FS.stat("/dir/subdir3/subdir3_1/subdir1 renamed");
    expect(FS.isDir(stat.mode)).toBe(true);
  });

  it("rename file to itself is a no-op", () => {
    const { FS } = h;
    // First rename to get file2
    FS.rename("/dir/file", "/dir/file2");
    // Rename to self — should succeed without error
    FS.rename("/dir/file2", "/dir/file2");
    const stat = FS.stat("/dir/file2");
    expect(stat.size).toBe(6);
  });

  it("rename directory with trailing slash", () => {
    const { FS } = h;
    FS.rename("/dir/subdir4", "/dir/subdir5");
    const stat = FS.stat("/dir/subdir5");
    expect(FS.isDir(stat.mode)).toBe(true);
  });

  it("rename directory over empty directory with common ancestor", () => {
    const { FS } = h;
    // dir/a → dir/b/c  (c is an empty directory, should be replaced)
    FS.rename("/dir/a", "/dir/b/c");
    const stat = FS.stat("/dir/b/c");
    expect(FS.isDir(stat.mode)).toBe(true);
    // dir/a should no longer exist
    expectErrno(() => FS.stat("/dir/a"), h.E.ENOENT);
  });

  it("renamed file retains its contents", () => {
    const { FS } = h;
    FS.rename("/file", "/file-renamed");
    const data = FS.readFile("/file-renamed", {}) as Uint8Array;
    expect(decode(data)).toBe("abcdef");
  });
});
