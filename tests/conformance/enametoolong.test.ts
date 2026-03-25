/**
 * ENAMETOOLONG conformance tests for tomefs.
 *
 * POSIX requires ENAMETOOLONG when a path component exceeds NAME_MAX (255).
 * Emscripten's legacy MEMFS does not enforce this; tomefs does.
 * These tests only run when TOMEFS_BACKEND=tomefs.
 */
import {
  createFS,
  expectErrno,
  O,
  type FSHarness,
} from "../harness/emscripten-fs.js";

const describeIfTomefs =
  process.env.TOMEFS_BACKEND === "tomefs" ? describe : describe.skip;

describeIfTomefs("ENAMETOOLONG enforcement (tomefs only)", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  const longName = "a".repeat(256);
  const maxName = "b".repeat(255);

  it("mkdir rejects name > 255 chars", () => {
    const { FS, E } = h;
    expectErrno(() => FS.mkdir("/" + longName, 0o777), E.ENAMETOOLONG);
  });

  it("mkdir accepts name of exactly 255 chars", () => {
    const { FS } = h;
    FS.mkdir("/" + maxName, 0o777);
    const stat = FS.stat("/" + maxName);
    expect(stat).toBeDefined();
  });

  it("file creation rejects name > 255 chars", () => {
    const { FS, E } = h;
    FS.mkdir("/d", 0o777);
    expectErrno(
      () => FS.open("/d/" + longName, O.RDWR | O.CREAT, 0o666),
      E.ENAMETOOLONG,
    );
  });

  it("file creation accepts name of exactly 255 chars", () => {
    const { FS } = h;
    FS.mkdir("/d", 0o777);
    const stream = FS.open("/d/" + maxName, O.RDWR | O.CREAT, 0o666);
    FS.close(stream);
    const stat = FS.stat("/d/" + maxName);
    expect(stat).toBeDefined();
  });

  it("symlink rejects target name > 255 chars", () => {
    const { FS, E } = h;
    FS.mkdir("/d", 0o777);
    expectErrno(
      () => FS.symlink("/d", "/d/" + longName),
      E.ENAMETOOLONG,
    );
  });

  it("symlink accepts link name of exactly 255 chars", () => {
    const { FS } = h;
    FS.mkdir("/d", 0o777);
    FS.mkdir("/d/target", 0o777);
    FS.symlink("/d/target", "/d/" + maxName);
    const link = FS.readlink("/d/" + maxName);
    expect(link).toBe("/d/target");
  });

  it("rename rejects new name > 255 chars", () => {
    const { FS, E } = h;
    FS.mkdir("/d", 0o777);
    const stream = FS.open("/d/src", O.RDWR | O.CREAT, 0o666);
    FS.close(stream);
    expectErrno(
      () => FS.rename("/d/src", "/d/" + longName),
      E.ENAMETOOLONG,
    );
  });

  it("rename accepts new name of exactly 255 chars", () => {
    const { FS } = h;
    FS.mkdir("/d", 0o777);
    const stream = FS.open("/d/src", O.RDWR | O.CREAT, 0o666);
    FS.close(stream);
    FS.rename("/d/src", "/d/" + maxName);
    const stat = FS.stat("/d/" + maxName);
    expect(stat).toBeDefined();
  });
});
