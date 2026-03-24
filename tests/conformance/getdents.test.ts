/**
 * Conformance tests ported from: emscripten/test/wasmfs/wasmfs_getdents.c
 *
 * Tests: directory entry enumeration with type verification,
 *        entries after file/dir creation, nested directory listing.
 *
 * Note: The C test uses the getdents syscall and scandir directly.
 * The Emscripten JS FS exposes FS.readdir(path) for names and
 * FS.stat()/FS.isDir()/FS.isFile() for type checking. We verify
 * directory contents and entry types through these APIs.
 */
import {
  createFS,
  encode,
  expectErrno,
  O,
  S_IFMT,
  S_IFREG,
  S_IFDIR,
  type FSHarness,
} from "../harness/emscripten-fs.js";

describe("getdents (wasmfs_getdents.c)", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
    const { FS } = h;
    // Replicate the C test's directory setup
    FS.mkdir("/root", 0o777);
    FS.mkdir("/root/working", 0o777);
    FS.mkdir("/root/working/test", 0o777);
  });

  it("lists directory with subdirectory and dot entries @fast", () => {
    const { FS } = h;
    const entries = FS.readdir("/root/working");

    expect(entries).toContain(".");
    expect(entries).toContain("..");
    expect(entries).toContain("test");
    expect(entries.length).toBe(3);
  });

  it("dot entry is a directory", () => {
    const { FS } = h;
    const stat = FS.stat("/root/working/.");
    expect(FS.isDir(stat.mode)).toBe(true);
  });

  it("dotdot entry is a directory", () => {
    const { FS } = h;
    const stat = FS.stat("/root/working/..");
    expect(FS.isDir(stat.mode)).toBe(true);
  });

  it("subdirectory entry has correct type", () => {
    const { FS } = h;
    const stat = FS.stat("/root/working/test");
    expect(stat.mode & S_IFMT).toBe(S_IFDIR);
    expect(FS.isDir(stat.mode)).toBe(true);
    expect(FS.isFile(stat.mode)).toBe(false);
  });

  it("/dev entries include character devices", () => {
    const { FS } = h;
    const entries = FS.readdir("/dev");

    expect(entries).toContain(".");
    expect(entries).toContain("..");
    expect(entries).toContain("null");
    expect(entries).toContain("stdin");
    expect(entries).toContain("stdout");
    expect(entries).toContain("stderr");
  });

  it("entries update after adding a file @fast", () => {
    const { FS } = h;
    // Before: only "test" subdir
    let entries = FS.readdir("/root/working");
    expect(entries).not.toContain("foobar");

    // Create a file
    const stream = FS.open(
      "/root/working/foobar",
      O.CREAT,
      0o040, // S_IRGRP
    );
    FS.close(stream);

    // After: should include foobar
    entries = FS.readdir("/root/working");
    expect(entries).toContain("foobar");
    expect(entries).toContain("test");
    expect(entries).toContain(".");
    expect(entries).toContain("..");
    expect(entries.length).toBe(4);
  });

  it("new file entry has regular file type", () => {
    const { FS } = h;
    const stream = FS.open("/root/working/myfile", O.CREAT | O.RDWR, 0o666);
    FS.write(stream, encode("data"), 0, 4);
    FS.close(stream);

    const stat = FS.stat("/root/working/myfile");
    expect(stat.mode & S_IFMT).toBe(S_IFREG);
    expect(FS.isFile(stat.mode)).toBe(true);
    expect(FS.isDir(stat.mode)).toBe(false);
  });

  it("sorted directory listing matches scandir alphasort", () => {
    const { FS } = h;
    // Create several entries with known names
    const stream = FS.open("/root/working/aaa", O.CREAT, 0o666);
    FS.close(stream);
    FS.mkdir("/root/working/bbb", 0o777);
    const stream2 = FS.open("/root/working/zzz", O.CREAT, 0o666);
    FS.close(stream2);

    const entries = FS.readdir("/root/working");
    const sorted = [...entries].sort();

    // Verify alphabetical sort (mimics scandir + alphasort)
    expect(sorted).toEqual([".", "..", "aaa", "bbb", "test", "zzz"]);
  });

  it("deeply nested directory listing works", () => {
    const { FS } = h;
    // /root/working/test already exists
    FS.mkdir("/root/working/test/deep", 0o777);
    const stream = FS.open(
      "/root/working/test/deep/leaf.txt",
      O.CREAT | O.WRONLY,
      0o666,
    );
    FS.close(stream);

    const entries = FS.readdir("/root/working/test/deep");
    expect(entries).toContain(".");
    expect(entries).toContain("..");
    expect(entries).toContain("leaf.txt");
    expect(entries.length).toBe(3);
  });
});
