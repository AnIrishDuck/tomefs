/**
 * Conformance tests for rename interactions with symlinks.
 *
 * No upstream source — these are custom tests verifying correct behavior
 * when rename() involves symlinks as source or target. POSIX specifies
 * that rename does NOT follow symlinks: it operates on the link itself.
 *
 * Cross-type renames (file↔symlink) exercise metadata cleanup paths in
 * the filesystem that are distinct from same-type renames.
 */
import {
  createFS,
  encode,
  decode,
  expectErrno,
  O,
  S_IFMT,
  S_IFREG,
  S_IFLNK,
  type FSHarness,
} from "../harness/emscripten-fs.js";

describe("rename-symlink interactions", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  // --- Cross-type renames ---

  it("rename file over existing symlink replaces the symlink @fast", () => {
    const { FS, E } = h;

    FS.writeFile("/target", "symlink-target-data");
    FS.symlink("/target", "/link");
    FS.writeFile("/src", "file-data");

    FS.rename("/src", "/link");

    // /link should now be a regular file with src's data
    const stat = FS.stat("/link");
    expect(stat.mode & S_IFMT).toBe(S_IFREG);
    const content = FS.readFile("/link", { encoding: "utf8" }) as string;
    expect(content).toBe("file-data");

    // /src should no longer exist
    expectErrno(() => FS.stat("/src"), E.ENOENT);

    // /target should still exist (unaffected — the symlink was replaced, not the target)
    const targetContent = FS.readFile("/target", { encoding: "utf8" }) as string;
    expect(targetContent).toBe("symlink-target-data");
  });

  it("rename symlink over existing file replaces the file @fast", () => {
    const { FS, E } = h;

    FS.writeFile("/target", "link-destination");
    FS.symlink("/target", "/link");
    FS.writeFile("/existing", "will-be-replaced");

    FS.rename("/link", "/existing");

    // /existing should now be a symlink pointing to /target
    const lstat = FS.lstat("/existing");
    expect(lstat.mode & S_IFMT).toBe(S_IFLNK);
    expect(FS.readlink("/existing")).toBe("/target");

    // Reading through the symlink should reach /target
    const content = FS.readFile("/existing", { encoding: "utf8" }) as string;
    expect(content).toBe("link-destination");

    // /link should no longer exist
    expectErrno(() => FS.lstat("/link"), E.ENOENT);
  });

  it("rename symlink over existing symlink replaces the target symlink", () => {
    const { FS, E } = h;

    FS.writeFile("/a", "file-a");
    FS.writeFile("/b", "file-b");
    FS.symlink("/a", "/link-a");
    FS.symlink("/b", "/link-b");

    FS.rename("/link-a", "/link-b");

    // /link-b should now point to /a (from link-a)
    expect(FS.readlink("/link-b")).toBe("/a");
    const content = FS.readFile("/link-b", { encoding: "utf8" }) as string;
    expect(content).toBe("file-a");

    // /link-a should no longer exist
    expectErrno(() => FS.lstat("/link-a"), E.ENOENT);

    // Both target files should still exist
    expect((FS.readFile("/a", { encoding: "utf8" }) as string)).toBe("file-a");
    expect((FS.readFile("/b", { encoding: "utf8" }) as string)).toBe("file-b");
  });

  // --- Error cases ---

  it("rename directory over symlink throws ENOTDIR", () => {
    const { FS, E } = h;

    FS.writeFile("/target", "data");
    FS.symlink("/target", "/link");
    FS.mkdir("/mydir");

    expectErrno(() => FS.rename("/mydir", "/link"), E.ENOTDIR);
  });

  it("rename symlink over directory throws EISDIR", () => {
    const { FS, E } = h;

    FS.writeFile("/target", "data");
    FS.symlink("/target", "/link");
    FS.mkdir("/mydir");

    expectErrno(() => FS.rename("/link", "/mydir"), E.EISDIR);
  });

  // --- Symlink rename preserves target ---

  it("rename symlink to new name preserves link target @fast", () => {
    const { FS, E } = h;

    FS.writeFile("/target", "preserved");
    FS.symlink("/target", "/old-name");

    FS.rename("/old-name", "/new-name");

    expect(FS.readlink("/new-name")).toBe("/target");
    const content = FS.readFile("/new-name", { encoding: "utf8" }) as string;
    expect(content).toBe("preserved");
    expectErrno(() => FS.lstat("/old-name"), E.ENOENT);
  });

  it("rename symlink across directories preserves link target", () => {
    const { FS, E } = h;

    FS.mkdir("/src-dir");
    FS.mkdir("/dst-dir");
    FS.writeFile("/target", "cross-dir");
    FS.symlink("/target", "/src-dir/link");

    FS.rename("/src-dir/link", "/dst-dir/link");

    expect(FS.readlink("/dst-dir/link")).toBe("/target");
    const content = FS.readFile("/dst-dir/link", { encoding: "utf8" }) as string;
    expect(content).toBe("cross-dir");
    expectErrno(() => FS.lstat("/src-dir/link"), E.ENOENT);
  });

  // --- File content integrity after cross-type rename ---

  it("file data integrity after rename over symlink with large data", () => {
    const { FS } = h;

    // Create a file that spans multiple pages (> 8KB)
    const largeData = new Uint8Array(8192 * 3 + 137);
    for (let i = 0; i < largeData.length; i++) {
      largeData[i] = (i * 7 + 13) & 0xff;
    }

    FS.writeFile("/target", "symlink-dest");
    FS.symlink("/target", "/link");

    const s = FS.open("/bigfile", O.WRONLY | O.CREAT, 0o666);
    FS.write(s, largeData, 0, largeData.length);
    FS.close(s);

    FS.rename("/bigfile", "/link");

    // Verify full data integrity
    const readback = FS.readFile("/link") as Uint8Array;
    expect(readback.length).toBe(largeData.length);
    expect(readback).toEqual(largeData);
  });

  it("symlink target accessible after rename over file with open fd", () => {
    const { FS } = h;

    FS.writeFile("/existing", "old-file-data");
    FS.writeFile("/target", "link-destination");
    FS.symlink("/target", "/link");

    // Open the existing file before it gets replaced
    const fd = FS.open("/existing", O.RDONLY);

    FS.rename("/link", "/existing");

    // The open fd should still be readable (POSIX unlink semantics)
    const buf = new Uint8Array(32);
    const n = FS.read(fd, buf, 0, 32);
    FS.close(fd);
    expect(decode(buf, n)).toBe("old-file-data");

    // /existing should now be the symlink
    expect(FS.readlink("/existing")).toBe("/target");
  });

  // --- Rename symlink over file when file has open fd ---

  it("rename file over symlink when file has open fds at new path", () => {
    const { FS } = h;

    FS.writeFile("/target", "link-dest");
    FS.symlink("/target", "/slot");
    FS.writeFile("/src", "source-data");

    // Open /slot through the symlink — this opens /target (the file)
    // The symlink itself doesn't have an open fd count
    FS.rename("/src", "/slot");

    // /slot is now a regular file
    const stat = FS.stat("/slot");
    expect(stat.mode & S_IFMT).toBe(S_IFREG);
    const content = FS.readFile("/slot", { encoding: "utf8" }) as string;
    expect(content).toBe("source-data");
  });

  // --- Rename chains ---

  it("sequential renames: file → symlink → file path", () => {
    const { FS, E } = h;

    FS.writeFile("/a", "content-a");
    FS.writeFile("/b-target", "b-dest");
    FS.symlink("/b-target", "/b");

    // Rename file /a over symlink /b
    FS.rename("/a", "/b");

    // /b is now a regular file
    expect((FS.stat("/b").mode & S_IFMT)).toBe(S_IFREG);
    expect((FS.readFile("/b", { encoding: "utf8" }) as string)).toBe("content-a");

    // Create a new symlink at /a
    FS.symlink("/b-target", "/a");

    // Rename /a (symlink) over /b (file)
    FS.rename("/a", "/b");

    // /b is now a symlink pointing to /b-target
    expect((FS.lstat("/b").mode & S_IFMT)).toBe(S_IFLNK);
    expect(FS.readlink("/b")).toBe("/b-target");
    expect((FS.readFile("/b", { encoding: "utf8" }) as string)).toBe("b-dest");

    expectErrno(() => FS.lstat("/a"), E.ENOENT);
  });

  it("rename dangling symlink over file", () => {
    const { FS, E } = h;

    FS.symlink("/nonexistent", "/dangling");
    FS.writeFile("/existing", "file-data");

    FS.rename("/dangling", "/existing");

    // /existing is now a dangling symlink
    const lstat = FS.lstat("/existing");
    expect(lstat.mode & S_IFMT).toBe(S_IFLNK);
    expect(FS.readlink("/existing")).toBe("/nonexistent");

    // Following the symlink should fail
    expectErrno(() => FS.stat("/existing"), E.ENOENT);
    expectErrno(() => FS.lstat("/dangling"), E.ENOENT);
  });

  it("rename file over dangling symlink", () => {
    const { FS, E } = h;

    FS.symlink("/nonexistent", "/dangling");
    FS.writeFile("/src", "real-data");

    FS.rename("/src", "/dangling");

    // /dangling is now a regular file
    const stat = FS.stat("/dangling");
    expect(stat.mode & S_IFMT).toBe(S_IFREG);
    const content = FS.readFile("/dangling", { encoding: "utf8" }) as string;
    expect(content).toBe("real-data");

    expectErrno(() => FS.stat("/src"), E.ENOENT);
  });
});
