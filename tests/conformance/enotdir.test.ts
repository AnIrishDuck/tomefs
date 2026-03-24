/**
 * Conformance tests ported from: emscripten/test/fs/test_fs_enotdir.c
 *
 * Tests: ENOTDIR when mkdir under a file, EISDIR for path ending in '/',
 *        ENOTDIR when a path component is a regular file.
 */
import {
  createFS,
  encode,
  expectErrno,
  O,
  type FSHarness,
} from "../harness/emscripten-fs.js";

describe("enotdir (test_fs_enotdir.c)", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
    const { FS } = h;
    // Create a regular file at /file
    FS.writeFile("/file", "content");
    // Create a directory at /dir
    FS.mkdir("/dir", 0o777);
    FS.writeFile("/dir/child", "data");
  });

  it("mkdir under a regular file throws ENOTDIR @fast", () => {
    const { FS, E } = h;
    // /file is a regular file, can't create child under it
    expectErrno(() => FS.mkdir("/file/subdir", 0o777), E.ENOTDIR);
  });

  it("open with path component as regular file throws ENOTDIR @fast", () => {
    const { FS, E } = h;
    // /file is a regular file, not a directory — can't traverse through it
    expectErrno(() => FS.open("/file/foo", O.RDONLY), E.ENOTDIR);
  });

  it("stat with path component as regular file throws ENOTDIR", () => {
    const { FS, E } = h;
    expectErrno(() => FS.stat("/file/bar"), E.ENOTDIR);
  });

  it("unlink with path component as regular file throws ENOTDIR", () => {
    const { FS, E } = h;
    expectErrno(() => FS.unlink("/file/baz"), E.ENOTDIR);
  });

  it("readdir on a regular file throws ENOTDIR", () => {
    const { FS, E } = h;
    expectErrno(() => FS.readdir("/file"), E.ENOTDIR);
  });

  it("creating a file with trailing slash fails with EISDIR @fast", () => {
    const { FS, E } = h;
    // A path ending in "/" implies a directory; creating a file there should fail
    expectErrno(
      () => FS.open("/newfile/", O.RDWR | O.CREAT, 0o777),
      E.EISDIR,
    );
  });

  it("open existing directory with O_RDONLY succeeds (no ENOTDIR)", () => {
    const { FS } = h;
    // Opening a directory for reading is valid
    const stream = FS.open("/dir", O.RDONLY | O.DIRECTORY);
    expect(stream.fd).toBeGreaterThanOrEqual(0);
    FS.close(stream);
  });

  it("symlink through a file path component throws ENOTDIR", () => {
    const { FS, E } = h;
    expectErrno(() => FS.symlink("target", "/file/link"), E.ENOTDIR);
  });

  it("rename with source path component as file throws ENOTDIR", () => {
    const { FS, E } = h;
    expectErrno(() => FS.rename("/file/a", "/newname"), E.ENOTDIR);
  });

  it("rename with dest path component as file throws ENOTDIR", () => {
    const { FS, E } = h;
    FS.writeFile("/src", "data");
    expectErrno(() => FS.rename("/src", "/file/dest"), E.ENOTDIR);
  });
});
