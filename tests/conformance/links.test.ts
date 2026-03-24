/**
 * Conformance tests ported from: emscripten/test/unistd/links.c
 *
 * Tests: symlink, readlink, ELOOP, relative/absolute symlink path resolution,
 *        lstat vs stat on symlinks, readlink error cases, nested symlinks,
 *        symlink to directory, open through symlink.
 */
import {
  createFS,
  encode,
  decode,
  expectErrno,
  O,
  S_IFMT,
  S_IFREG,
  S_IFDIR,
  S_IFLNK,
  type FSHarness,
} from "../harness/emscripten-fs.js";

describe("links (links.c)", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  it("symlink creates a link and readlink returns the target @fast", () => {
    const { FS } = h;
    FS.writeFile("/target", "hello");
    FS.symlink("/target", "/link");

    expect(FS.readlink("/link")).toBe("/target");
  });

  it("stat on symlink follows it to the target (S_IFREG)", () => {
    const { FS } = h;
    FS.writeFile("/target", "hello");
    FS.symlink("/target", "/link");

    const stat = FS.stat("/link");
    expect(stat.mode & S_IFMT).toBe(S_IFREG);
    expect(stat.size).toBe(5);
  });

  it("lstat on symlink returns the link itself (S_IFLNK) @fast", () => {
    const { FS } = h;
    FS.writeFile("/target", "hello");
    FS.symlink("/target", "/link");

    const lstat = FS.lstat("/link");
    expect(lstat.mode & S_IFMT).toBe(S_IFLNK);
    // symlink size is the length of the target path string
    expect(lstat.size).toBe("/target".length);
  });

  it("reading through a symlink returns the target file content", () => {
    const { FS } = h;
    FS.writeFile("/target", "symlink-data");
    FS.symlink("/target", "/link");

    const content = FS.readFile("/link", { encoding: "utf8" }) as string;
    expect(content).toBe("symlink-data");
  });

  it("writing through a symlink modifies the target file", () => {
    const { FS } = h;
    FS.writeFile("/target", "original");
    FS.symlink("/target", "/link");

    FS.writeFile("/link", "modified");

    const content = FS.readFile("/target", { encoding: "utf8" }) as string;
    expect(content).toBe("modified");
  });

  it("relative symlink resolves relative to the link's directory", () => {
    const { FS } = h;
    FS.mkdir("/testdir");
    FS.writeFile("/testdir/file", "relative-data");
    FS.symlink("file", "/testdir/rellink");

    const content = FS.readFile("/testdir/rellink", {
      encoding: "utf8",
    }) as string;
    expect(content).toBe("relative-data");
  });

  it("symlink to a directory allows traversal @fast", () => {
    const { FS } = h;
    FS.mkdir("/realdir");
    FS.writeFile("/realdir/child", "child-data");
    FS.symlink("/realdir", "/dirlink");

    // stat the symlink — should follow to directory
    const stat = FS.stat("/dirlink");
    expect(stat.mode & S_IFMT).toBe(S_IFDIR);

    // traverse through the symlink
    const content = FS.readFile("/dirlink/child", {
      encoding: "utf8",
    }) as string;
    expect(content).toBe("child-data");
  });

  it("readdir through a directory symlink lists entries", () => {
    const { FS } = h;
    FS.mkdir("/realdir2");
    FS.writeFile("/realdir2/a", "a");
    FS.writeFile("/realdir2/b", "b");
    FS.symlink("/realdir2", "/dirlink2");

    const entries = FS.readdir("/dirlink2");
    expect(entries).toContain("a");
    expect(entries).toContain("b");
    expect(entries).toContain(".");
    expect(entries).toContain("..");
  });

  it("chained symlinks resolve transitively", () => {
    const { FS } = h;
    FS.writeFile("/real", "chain-data");
    FS.symlink("/real", "/link1");
    FS.symlink("/link1", "/link2");
    FS.symlink("/link2", "/link3");

    const content = FS.readFile("/link3", { encoding: "utf8" }) as string;
    expect(content).toBe("chain-data");

    // lstat on intermediate links should show S_IFLNK
    expect(FS.lstat("/link2").mode & S_IFMT).toBe(S_IFLNK);
    expect(FS.lstat("/link3").mode & S_IFMT).toBe(S_IFLNK);
  });

  it("circular symlinks cause ELOOP", () => {
    const { FS, E } = h;
    FS.symlink("/loop2", "/loop1");
    FS.symlink("/loop1", "/loop2");

    expectErrno(() => FS.stat("/loop1"), E.ELOOP);
    expectErrno(() => FS.stat("/loop2"), E.ELOOP);
  });

  it("self-referencing symlink causes ELOOP", () => {
    const { FS, E } = h;
    FS.symlink("/self", "/self");

    expectErrno(() => FS.stat("/self"), E.ELOOP);
  });

  it("ELOOP on open through circular symlink", () => {
    const { FS, E } = h;
    FS.symlink("/loop_b", "/loop_a");
    FS.symlink("/loop_a", "/loop_b");

    expectErrno(() => FS.open("/loop_a", O.RDONLY), E.ELOOP);
  });

  it("readlink on a non-symlink throws EINVAL", () => {
    const { FS, E } = h;
    FS.writeFile("/regular", "data");

    expectErrno(() => FS.readlink("/regular"), E.EINVAL);
  });

  it("readlink on a directory throws EINVAL", () => {
    const { FS, E } = h;
    FS.mkdir("/somedir");

    expectErrno(() => FS.readlink("/somedir"), E.EINVAL);
  });

  it("readlink on non-existent path throws ENOENT", () => {
    const { FS, E } = h;

    expectErrno(() => FS.readlink("/nonexistent"), E.ENOENT);
  });

  it("symlink to non-existent target creates a dangling symlink", () => {
    const { FS, E } = h;
    FS.symlink("/no-such-file", "/dangling");

    // readlink works (the symlink itself exists)
    expect(FS.readlink("/dangling")).toBe("/no-such-file");

    // lstat works (the symlink node exists)
    const lstat = FS.lstat("/dangling");
    expect(lstat.mode & S_IFMT).toBe(S_IFLNK);

    // stat fails (target doesn't exist)
    expectErrno(() => FS.stat("/dangling"), E.ENOENT);
  });

  it("open with O_NOFOLLOW on a symlink throws ELOOP @fast", () => {
    const { FS, E } = h;
    FS.writeFile("/nf_target", "data");
    FS.symlink("/nf_target", "/nf_link");

    expectErrno(() => FS.open("/nf_link", O.RDONLY | O.NOFOLLOW), E.ELOOP);
  });

  it("open with O_NOFOLLOW on a regular file succeeds", () => {
    const { FS } = h;
    FS.writeFile("/nf_regular", "data");

    const stream = FS.open("/nf_regular", O.RDONLY | O.NOFOLLOW);
    expect(stream.fd).toBeGreaterThanOrEqual(0);
    FS.close(stream);
  });

  it("unlink removes the symlink, not the target", () => {
    const { FS, E } = h;
    FS.writeFile("/ul_target", "preserved");
    FS.symlink("/ul_target", "/ul_link");

    FS.unlink("/ul_link");

    // target still exists
    const content = FS.readFile("/ul_target", { encoding: "utf8" }) as string;
    expect(content).toBe("preserved");

    // link is gone
    expectErrno(() => FS.lstat("/ul_link"), E.ENOENT);
  });

  it("inode of symlink differs from inode of target", () => {
    const { FS } = h;
    FS.writeFile("/ino_target", "data");
    FS.symlink("/ino_target", "/ino_link");

    const targetIno = FS.stat("/ino_target").ino;
    const linkIno = FS.lstat("/ino_link").ino;

    expect(linkIno).not.toBe(targetIno);
  });
});
