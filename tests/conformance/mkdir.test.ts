/**
 * Conformance tests ported from: emscripten/test/wasmfs/wasmfs_mkdir.c
 *
 * Tests: mkdir with mode bits, ENOENT/EEXIST/ENOTDIR/ENAMETOOLONG,
 *        creating files inside new directories, sticky bit.
 */
import {
  createFS,
  encode,
  decode,
  expectErrno,
  O,
  SEEK_SET,
  S_IFMT,
  S_IFDIR,
  S_IRWXUGO,
  type FSHarness,
} from "../harness/emscripten-fs.js";

describe("mkdir (wasmfs_mkdir.c)", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  it("creates a directory with correct mode bits @fast", () => {
    const { FS } = h;
    FS.mkdir("/working", 0o777);

    const stat = FS.stat("/working");
    expect(stat.mode & S_IFMT).toBe(S_IFDIR);
    expect(stat.mode).toBe(S_IRWXUGO | S_IFDIR);
  });

  it("creates a directory with sticky bit (01777)", () => {
    const { FS } = h;
    const S_ISVTX = 0o1000;
    FS.mkdir("/foobar", 0o1777);

    const stat = FS.stat("/foobar");
    expect(stat.mode).toBe(S_IRWXUGO | S_ISVTX | S_IFDIR);
  });

  it("can create and use a file inside a new directory @fast", () => {
    const { FS } = h;
    FS.mkdir("/working", 0o777);

    const stream = FS.open("/working/test", O.RDWR | O.CREAT, 0o777);
    const msg = encode("Test\n");
    FS.write(stream, msg, 0, msg.length);
    FS.llseek(stream, 0, SEEK_SET);

    const buf = new Uint8Array(100);
    const n = FS.read(stream, buf, 0, buf.length);
    expect(decode(buf, n)).toBe("Test\n");
    FS.close(stream);
  });

  it("empty pathname returns ENOENT", () => {
    const { FS, E } = h;
    expectErrno(() => FS.mkdir("", 0o777), E.ENOENT);
  });

  it("mkdir root returns EEXIST", () => {
    const { FS, E } = h;
    expectErrno(() => FS.mkdir("/", 0o777), E.EEXIST);
  });

  it("mkdir existing directory returns EEXIST", () => {
    const { FS, E } = h;
    expectErrno(() => FS.mkdir("/dev", 0o777), E.EEXIST);
  });

  it("mkdir under a non-directory returns ENOTDIR", () => {
    const { FS, E } = h;
    // /dev/stdout is a file (or symlink to one), not a directory
    expectErrno(
      () => FS.mkdir("/dev/stdout/fake-directory", 0o777),
      E.ENOTDIR,
    );
  });

  it("mkdir with non-existent parent returns ENOENT", () => {
    const { FS, E } = h;
    expectErrno(
      () => FS.mkdir("/dev/false-path/fake-directory", 0o777),
      E.ENOENT,
    );
  });

  it("can create nested directories", () => {
    const { FS } = h;
    FS.mkdir("/working", 0o777);
    FS.mkdir("/working/new-directory", 0o777);

    const stat = FS.stat("/working/new-directory");
    expect(stat.mode & S_IFMT).toBe(S_IFDIR);
  });

  // Note: ENAMETOOLONG is only enforced in WasmFS, not the legacy JS FS.
  // The upstream C test guards this with #ifdef WASMFS. We skip it for MEMFS
  // but include it so tomefs can enforce it when our FS implementation runs.
  it.skip("mkdir with name longer than 255 chars throws ENAMETOOLONG (WASMFS only)", () => {
    const { FS, E } = h;
    // Generate a name that's 256 characters long
    const longName = "a".repeat(256);
    expectErrno(() => FS.mkdir("/" + longName, 0o777), E.ENAMETOOLONG);
  });
});
