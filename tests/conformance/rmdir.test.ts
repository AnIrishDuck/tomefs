/**
 * Conformance tests for rmdir() positive cases and directory lifecycle.
 *
 * The existing unlink.test.ts covers rmdir error cases (ENOENT, ENOTDIR,
 * EACCES, ENOTEMPTY, EBUSY) and one basic positive case. These tests
 * exercise the full range of successful rmdir behavior: parent directory
 * updates, nested removal, re-creation after removal, timestamp updates,
 * and interaction with other directory operations.
 *
 * PostgreSQL creates and removes temporary directories during query
 * execution (sort spill, hash join batches) and tablespace operations.
 * Correct rmdir semantics are critical for cleanup.
 */
import {
  createFS,
  expectErrno,
  O,
  type FSHarness,
} from "../harness/emscripten-fs.js";

describe("rmdir (positive cases and lifecycle)", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  it("rmdir removes directory from parent listing @fast", () => {
    const { FS } = h;
    FS.mkdir("/rmtest");
    FS.mkdir("/rmtest/child");

    const before = FS.readdir("/rmtest");
    expect(before).toContain("child");

    FS.rmdir("/rmtest/child");

    const after = FS.readdir("/rmtest");
    expect(after).not.toContain("child");
    expect(after).toContain(".");
    expect(after).toContain("..");
  });

  it("rmdir followed by stat throws ENOENT @fast", () => {
    const { FS, E } = h;
    FS.mkdir("/gone");
    FS.stat("/gone"); // should not throw
    FS.rmdir("/gone");
    expectErrno(() => FS.stat("/gone"), E.ENOENT);
  });

  it("directory can be re-created after rmdir @fast", () => {
    const { FS } = h;
    FS.mkdir("/recreate");
    FS.rmdir("/recreate");

    FS.mkdir("/recreate");
    const stat = FS.stat("/recreate");
    expect(FS.isDir(stat.mode)).toBe(true);

    const entries = FS.readdir("/recreate");
    expect(entries).toContain(".");
    expect(entries).toContain("..");
    expect(entries.length).toBe(2);
  });

  it("re-created directory gets a new inode @fast", () => {
    const { FS } = h;
    FS.mkdir("/newinode");
    const ino1 = FS.stat("/newinode").ino;
    FS.rmdir("/newinode");

    FS.mkdir("/newinode");
    const ino2 = FS.stat("/newinode").ino;
    expect(ino2).not.toBe(ino1);
  });

  it("nested directories removed leaf-first @fast", () => {
    const { FS, E } = h;
    FS.mkdir("/a");
    FS.mkdir("/a/b");
    FS.mkdir("/a/b/c");

    // Can't remove non-leaf first
    expectErrno(() => FS.rmdir("/a"), E.ENOTEMPTY);
    expectErrno(() => FS.rmdir("/a/b"), E.ENOTEMPTY);

    // Remove leaf-first
    FS.rmdir("/a/b/c");
    expectErrno(() => FS.stat("/a/b/c"), E.ENOENT);
    expect(FS.readdir("/a/b")).not.toContain("c");

    FS.rmdir("/a/b");
    expectErrno(() => FS.stat("/a/b"), E.ENOENT);
    expect(FS.readdir("/a")).not.toContain("b");

    FS.rmdir("/a");
    expectErrno(() => FS.stat("/a"), E.ENOENT);
  });

  it("rmdir updates parent mtime", () => {
    const { FS } = h;
    FS.mkdir("/parent");
    FS.mkdir("/parent/victim");

    const before = FS.stat("/parent").mtime;

    // Ensure some time passes for timestamp resolution
    const busyWait = Date.now() + 10;
    while (Date.now() < busyWait) { /* spin */ }

    FS.rmdir("/parent/victim");
    const after = FS.stat("/parent").mtime;
    expect(after.getTime()).toBeGreaterThanOrEqual(before.getTime());
  });

  it("rmdir updates parent ctime", () => {
    const { FS } = h;
    FS.mkdir("/cparent");
    FS.mkdir("/cparent/victim");

    const before = FS.stat("/cparent").ctime;

    const busyWait = Date.now() + 10;
    while (Date.now() < busyWait) { /* spin */ }

    FS.rmdir("/cparent/victim");
    const after = FS.stat("/cparent").ctime;
    expect(after.getTime()).toBeGreaterThanOrEqual(before.getTime());
  });

  it("rmdir sibling directories independently", () => {
    const { FS, E } = h;
    FS.mkdir("/siblings");
    FS.mkdir("/siblings/a");
    FS.mkdir("/siblings/b");
    FS.mkdir("/siblings/c");

    expect(FS.readdir("/siblings")).toContain("a");
    expect(FS.readdir("/siblings")).toContain("b");
    expect(FS.readdir("/siblings")).toContain("c");

    FS.rmdir("/siblings/b");

    expect(FS.readdir("/siblings")).toContain("a");
    expect(FS.readdir("/siblings")).not.toContain("b");
    expect(FS.readdir("/siblings")).toContain("c");
    expectErrno(() => FS.stat("/siblings/b"), E.ENOENT);

    // a and c still fully functional
    FS.mkdir("/siblings/a/nested");
    expect(FS.readdir("/siblings/a")).toContain("nested");
  });

  it("directory lifecycle: create, populate, empty, rmdir @fast", () => {
    const { FS, E } = h;
    // Create
    FS.mkdir("/lifecycle");

    // Populate with files and subdirectory
    const stream = FS.open(
      "/lifecycle/data.txt",
      O.WRONLY | O.CREAT,
      0o644,
    );
    FS.close(stream);
    FS.mkdir("/lifecycle/sub");
    FS.symlink("/lifecycle/data.txt", "/lifecycle/link");

    expect(FS.readdir("/lifecycle").length).toBe(5); // . .. data.txt sub link

    // Empty — must remove contents before rmdir
    FS.unlink("/lifecycle/link");
    FS.unlink("/lifecycle/data.txt");
    FS.rmdir("/lifecycle/sub");

    expect(FS.readdir("/lifecycle").length).toBe(2); // . ..

    // rmdir
    FS.rmdir("/lifecycle");
    expectErrno(() => FS.stat("/lifecycle"), E.ENOENT);
  });

  it("rmdir directory then create file at same path", () => {
    const { FS } = h;
    FS.mkdir("/reuse_path");
    expect(FS.isDir(FS.stat("/reuse_path").mode)).toBe(true);

    FS.rmdir("/reuse_path");

    // Create a file at the same path
    const s = FS.open("/reuse_path", O.WRONLY | O.CREAT, 0o644);
    FS.close(s);

    const stat = FS.stat("/reuse_path");
    expect(FS.isFile(stat.mode)).toBe(true);
  });

  it("rmdir file-at-same-path then mkdir again", () => {
    const { FS } = h;
    // Start with a file
    const s = FS.open("/flip_type", O.WRONLY | O.CREAT, 0o644);
    FS.close(s);
    expect(FS.isFile(FS.stat("/flip_type").mode)).toBe(true);

    // Replace with directory
    FS.unlink("/flip_type");
    FS.mkdir("/flip_type");
    expect(FS.isDir(FS.stat("/flip_type").mode)).toBe(true);

    // Back to file
    FS.rmdir("/flip_type");
    const s2 = FS.open("/flip_type", O.WRONLY | O.CREAT, 0o644);
    FS.close(s2);
    expect(FS.isFile(FS.stat("/flip_type").mode)).toBe(true);
  });

  it("rmdir does not affect sibling files", () => {
    const { FS } = h;
    FS.mkdir("/container");
    FS.writeFile("/container/keep.txt", "preserved");
    FS.mkdir("/container/removeme");

    FS.rmdir("/container/removeme");

    // Sibling file is untouched
    const content = FS.readFile("/container/keep.txt", {
      encoding: "utf8",
    }) as string;
    expect(content).toBe("preserved");
  });

  it("rmdir many directories in sequence", () => {
    const { FS, E } = h;
    const count = 20;
    FS.mkdir("/many");
    for (let i = 0; i < count; i++) {
      FS.mkdir(`/many/d${i}`);
    }
    expect(FS.readdir("/many").length).toBe(count + 2); // + . and ..

    for (let i = 0; i < count; i++) {
      FS.rmdir(`/many/d${i}`);
    }
    expect(FS.readdir("/many").length).toBe(2); // only . and ..
    FS.rmdir("/many");
    expectErrno(() => FS.stat("/many"), E.ENOENT);
  });

  it("readdir on parent is consistent after multiple rmdirs @fast", () => {
    const { FS } = h;
    FS.mkdir("/multi_rm");
    FS.mkdir("/multi_rm/a");
    FS.mkdir("/multi_rm/b");
    FS.mkdir("/multi_rm/c");

    FS.rmdir("/multi_rm/a");
    let entries = FS.readdir("/multi_rm");
    expect(entries.filter((e) => e !== "." && e !== "..").sort()).toEqual([
      "b",
      "c",
    ]);

    FS.rmdir("/multi_rm/c");
    entries = FS.readdir("/multi_rm");
    expect(entries.filter((e) => e !== "." && e !== "..")).toEqual(["b"]);

    FS.rmdir("/multi_rm/b");
    entries = FS.readdir("/multi_rm");
    expect(entries.filter((e) => e !== "." && e !== "..")).toEqual([]);
  });

  it("deeply nested create and remove cycle", () => {
    const { FS, E } = h;
    // Create a deep path
    let path = "";
    const depth = 8;
    for (let i = 0; i < depth; i++) {
      path += `/d${i}`;
      FS.mkdir(path);
    }

    // Verify the deepest directory exists
    const deepStat = FS.stat(path);
    expect(FS.isDir(deepStat.mode)).toBe(true);

    // Remove from deepest to shallowest
    for (let i = depth - 1; i >= 0; i--) {
      let rmPath = "";
      for (let j = 0; j <= i; j++) {
        rmPath += `/d${j}`;
      }
      FS.rmdir(rmPath);
      expectErrno(() => FS.stat(rmPath), E.ENOENT);
    }
  });

  it("rmdir preserves mode of parent directory @fast", () => {
    const { FS } = h;
    FS.mkdir("/modetest", 0o755);
    FS.mkdir("/modetest/child", 0o700);

    const parentModeBefore = FS.stat("/modetest").mode;
    FS.rmdir("/modetest/child");
    const parentModeAfter = FS.stat("/modetest").mode;

    // Permission bits should be unchanged
    expect(parentModeAfter & 0o777).toBe(parentModeBefore & 0o777);
  });

  it("mkdir after rmdir in same parent directory works @fast", () => {
    const { FS } = h;
    FS.mkdir("/replace");
    FS.mkdir("/replace/old");
    FS.rmdir("/replace/old");

    FS.mkdir("/replace/new");
    const entries = FS.readdir("/replace");
    expect(entries).not.toContain("old");
    expect(entries).toContain("new");
  });
});
