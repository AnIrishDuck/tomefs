/**
 * Conformance tests ported from: emscripten/test/dirent/test_readdir.c
 *
 * Tests: FS.readdir listing (. and .. entries, regular files),
 *        error cases (ENOENT, ENOTDIR), directory after file creation.
 *
 * Note: The C test uses opendir/readdir/rewinddir/seekdir/telldir/scandir
 * which are POSIX C APIs. The Emscripten JS FS exposes FS.readdir(path)
 * which returns a string array. We test the JS-level semantics.
 */
import {
  createFS,
  encode,
  expectErrno,
  O,
  type FSHarness,
} from "../harness/emscripten-fs.js";

describe("readdir (test_readdir.c)", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
    const { FS } = h;
    // Replicate the C test's setup()
    FS.mkdir("/testtmp", 0o777);
    FS.mkdir("/testtmp/nocanread", 0o111);
    FS.mkdir("/testtmp/foobar", 0o777);
    // Create a file inside foobar
    const stream = FS.open(
      "/testtmp/foobar/file.txt",
      O.WRONLY | O.CREAT | O.EXCL,
      0o666,
    );
    const data = encode("ride into the danger zone");
    FS.write(stream, data, 0, data.length);
    FS.close(stream);
  });

  it("readdir on non-existent path throws ENOENT @fast", () => {
    const { FS, E } = h;
    expectErrno(() => FS.readdir("/testtmp/noexist"), E.ENOENT);
  });

  it("readdir on a regular file throws ENOTDIR", () => {
    const { FS, E } = h;
    expectErrno(() => FS.readdir("/testtmp/foobar/file.txt"), E.ENOTDIR);
  });

  it("readdir includes dot, dotdot, and file entries @fast", () => {
    const { FS } = h;
    const entries = FS.readdir("/testtmp/foobar");

    expect(entries).toContain(".");
    expect(entries).toContain("..");
    expect(entries).toContain("file.txt");
    expect(entries.length).toBe(3);
  });

  it("readdir on root contains expected directories", () => {
    const { FS } = h;
    const entries = FS.readdir("/");

    expect(entries).toContain(".");
    expect(entries).toContain("..");
    expect(entries).toContain("dev");
    expect(entries).toContain("testtmp");
  });

  it("readdir on /dev lists device files", () => {
    const { FS } = h;
    const entries = FS.readdir("/dev");

    expect(entries).toContain(".");
    expect(entries).toContain("..");
    // Emscripten always creates these device entries
    expect(entries).toContain("null");
  });

  it("readdir reflects newly created files", () => {
    const { FS } = h;
    // Before: only file.txt
    let entries = FS.readdir("/testtmp/foobar");
    expect(entries).not.toContain("newfile.txt");

    // Create a new file
    const stream = FS.open(
      "/testtmp/foobar/newfile.txt",
      O.WRONLY | O.CREAT,
      0o666,
    );
    FS.close(stream);

    // After: should include newfile.txt
    entries = FS.readdir("/testtmp/foobar");
    expect(entries).toContain("file.txt");
    expect(entries).toContain("newfile.txt");
    expect(entries).toContain(".");
    expect(entries).toContain("..");
    expect(entries.length).toBe(4);
  });

  it("readdir reflects newly created subdirectories", () => {
    const { FS } = h;
    FS.mkdir("/testtmp/foobar/subdir", 0o777);

    const entries = FS.readdir("/testtmp/foobar");
    expect(entries).toContain("subdir");
    expect(entries).toContain("file.txt");
    expect(entries).toContain(".");
    expect(entries).toContain("..");
  });

  it("readdir after unlink no longer shows removed file", () => {
    const { FS } = h;
    // Verify file exists
    let entries = FS.readdir("/testtmp/foobar");
    expect(entries).toContain("file.txt");

    // Remove it
    FS.unlink("/testtmp/foobar/file.txt");

    // Should no longer appear
    entries = FS.readdir("/testtmp/foobar");
    expect(entries).not.toContain("file.txt");
    expect(entries.length).toBe(2); // just . and ..
  });

  it("readdir returns entries that can be sorted alphabetically", () => {
    const { FS } = h;
    // Create multiple files for sorting
    for (const name of ["charlie", "alpha", "bravo"]) {
      const stream = FS.open(
        `/testtmp/foobar/${name}`,
        O.WRONLY | O.CREAT,
        0o666,
      );
      FS.close(stream);
    }

    const entries = FS.readdir("/testtmp/foobar");
    const sorted = [...entries].sort();

    // Verify we can sort and get expected order
    // (mirrors scandir with alphasort from the C test)
    expect(sorted[0]).toBe(".");
    expect(sorted[1]).toBe("..");
    expect(sorted).toContain("alpha");
    expect(sorted).toContain("bravo");
    expect(sorted).toContain("charlie");
    expect(sorted).toContain("file.txt");
  });
});
