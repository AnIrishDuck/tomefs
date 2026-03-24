/**
 * Conformance tests ported from: emscripten/test/fs/test_fs_mkdir_dotdot.c
 *
 * Tests: mkdir("a/b/..") → EEXIST, mkdir("a/b/.") → EEXIST,
 *        and related dot/dotdot edge cases in path resolution.
 */
import {
  createFS,
  expectErrno,
  type FSHarness,
} from "../harness/emscripten-fs.js";

describe("mkdir dotdot (test_fs_mkdir_dotdot.c)", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  it("mkdir('a') succeeds @fast", () => {
    const { FS } = h;
    FS.mkdir("/a", 0o777);
    const stat = FS.stat("/a");
    expect(FS.isDir(stat.mode)).toBe(true);
  });

  it("mkdir('a/b') succeeds", () => {
    const { FS } = h;
    FS.mkdir("/a", 0o777);
    FS.mkdir("/a/b", 0o777);
    const stat = FS.stat("/a/b");
    expect(FS.isDir(stat.mode)).toBe(true);
  });

  it("mkdir('a/b/..') throws EEXIST because it resolves to 'a' @fast", () => {
    const { FS, E } = h;
    FS.mkdir("/a", 0o777);
    FS.mkdir("/a/b", 0o777);

    // "a/b/.." resolves to "a", which already exists
    expectErrno(() => FS.mkdir("/a/b/..", 0o777), E.EEXIST);
  });

  it("mkdir('a/b/.') throws EEXIST because it resolves to 'a/b' @fast", () => {
    const { FS, E } = h;
    FS.mkdir("/a2", 0o777);
    FS.mkdir("/a2/b", 0o777);

    // "a2/b/." resolves to "a2/b", which already exists
    expectErrno(() => FS.mkdir("/a2/b/.", 0o777), E.EEXIST);
  });

  it("mkdir('/') throws EEXIST (root always exists)", () => {
    const { FS, E } = h;
    expectErrno(() => FS.mkdir("/", 0o777), E.EEXIST);
  });

  it("mkdir('/.') throws EEXIST", () => {
    const { FS, E } = h;
    expectErrno(() => FS.mkdir("/.", 0o777), E.EEXIST);
  });

  it("mkdir('/..') throws EEXIST (root's parent is root)", () => {
    const { FS, E } = h;
    expectErrno(() => FS.mkdir("/..", 0o777), E.EEXIST);
  });

  it("mkdir with dotdot that traverses to non-existent parent throws ENOENT", () => {
    const { FS, E } = h;
    // /nosuch doesn't exist, so /nosuch/../foo can't resolve
    expectErrno(() => FS.mkdir("/nosuch/../foo", 0o777), E.ENOENT);
  });
});
