/**
 * Conformance tests ported from: emscripten/test/fs/test_fs_symlink_resolution.c
 *
 * Tests: creating files and directories through symlink/../path resolution.
 * Verifies that intermediate symlinks in paths are resolved correctly
 * when combined with ".." traversal.
 */
import {
  createFS,
  encode,
  decode,
  expectErrno,
  O,
  S_IFMT,
  S_IFDIR,
  S_IFREG,
  type FSHarness,
} from "../harness/emscripten-fs.js";

describe("symlink resolution (test_fs_symlink_resolution.c)", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
    const { FS } = h;
    // Set up: /a/b with a symlink /symlink -> /a/b
    FS.mkdir("/a");
    FS.mkdir("/a/b");
    FS.symlink("/a/b", "/symlink");
  });

  it("create file through symlink/../path @fast", () => {
    const { FS } = h;
    // /symlink points to /a/b, so /symlink/../newfile resolves to /a/newfile
    FS.writeFile("/symlink/../newfile", "created");

    const content = FS.readFile("/a/newfile", { encoding: "utf8" }) as string;
    expect(content).toBe("created");

    const stat = FS.stat("/a/newfile");
    expect(stat.mode & S_IFMT).toBe(S_IFREG);
  });

  it("create directory through symlink/../path", () => {
    const { FS } = h;
    // /symlink/../newdir should create /a/newdir
    FS.mkdir("/symlink/../newdir");

    const stat = FS.stat("/a/newdir");
    expect(stat.mode & S_IFMT).toBe(S_IFDIR);
  });

  it("open file through symlink/../path", () => {
    const { FS } = h;
    // Create /a/openme
    FS.writeFile("/a/openme", "open-through-symlink");

    // Open through /symlink/../openme
    const stream = FS.open("/symlink/../openme", O.RDONLY);
    const buf = new Uint8Array(100);
    const n = FS.read(stream, buf, 0, buf.length);
    expect(decode(buf, n)).toBe("open-through-symlink");
    FS.close(stream);
  });

  it("stat through symlink/../path resolves correctly", () => {
    const { FS } = h;
    FS.writeFile("/a/statme", "data");

    const stat = FS.stat("/symlink/../statme");
    expect(stat.mode & S_IFMT).toBe(S_IFREG);
    expect(stat.size).toBe(4);
  });

  it("nested symlink/../symlink/../path resolution", () => {
    const { FS } = h;
    // /a/b already exists, /symlink -> /a/b
    // Create /a/b/c
    FS.mkdir("/a/b/c");
    FS.symlink("/a/b/c", "/symlink2");

    // /symlink2/../../../toplevel should resolve to /toplevel
    // /symlink2 -> /a/b/c, so /symlink2/../../.. -> /a/b/c/../../.. -> /
    FS.writeFile("/symlink2/../../../toplevel", "top");

    const content = FS.readFile("/toplevel", { encoding: "utf8" }) as string;
    expect(content).toBe("top");
  });

  it("readdir through symlink/../ lists correct entries", () => {
    const { FS } = h;
    FS.writeFile("/a/entry1", "x");
    FS.writeFile("/a/entry2", "y");

    // /symlink -> /a/b, so /symlink/.. -> /a
    const entries = FS.readdir("/symlink/..");
    expect(entries).toContain("entry1");
    expect(entries).toContain("entry2");
    expect(entries).toContain("b");
  });
});
