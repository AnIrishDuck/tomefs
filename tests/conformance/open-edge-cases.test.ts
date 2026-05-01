/**
 * Conformance tests for open() edge cases not covered by other test files.
 *
 * Source: POSIX open(2) specification and SUSv4 system interface definitions.
 * These cover error paths and boundary conditions that are critical for
 * database safety — Postgres relies on open() returning correct errors
 * to detect misconfiguration, symlink attacks, and path traversal issues.
 *
 * Ethos §2 (real POSIX semantics), §8 (additional conformance test sources)
 */
import {
  createFS,
  encode,
  decode,
  expectErrno,
  O,
  SEEK_SET,
  type FSHarness,
} from "../harness/emscripten-fs.js";

describe("open() edge cases", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  // -------------------------------------------------------------------
  // Empty path
  // -------------------------------------------------------------------

  it("open with empty path throws ENOENT @fast", () => {
    const { FS, E } = h;
    expectErrno(() => FS.open("", O.RDONLY), E.ENOENT);
  });

  it("open with empty path and O_CREAT throws ENOENT", () => {
    const { FS, E } = h;
    expectErrno(() => FS.open("", O.RDWR | O.CREAT, 0o666), E.ENOENT);
  });

  // -------------------------------------------------------------------
  // Dangling symlinks
  // -------------------------------------------------------------------

  it("open dangling symlink without O_CREAT throws ENOENT @fast", () => {
    const { FS, E } = h;
    FS.symlink("/nonexistent-target", "/dangling");
    expectErrno(() => FS.open("/dangling", O.RDONLY), E.ENOENT);
  });

  it("open dangling symlink with O_WRONLY throws ENOENT", () => {
    const { FS, E } = h;
    FS.symlink("/nonexistent-target", "/dangling_w");
    expectErrno(() => FS.open("/dangling_w", O.WRONLY), E.ENOENT);
  });

  it("open dangling symlink with O_CREAT creates file at target @fast", () => {
    const { FS } = h;
    FS.symlink("/created-via-symlink", "/creator-link");
    const stream = FS.open("/creator-link", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("through-link"), 0, 12);
    FS.close(stream);

    // The target file should now exist and be readable directly
    const rs = FS.open("/created-via-symlink", O.RDONLY);
    const buf = new Uint8Array(20);
    const n = FS.read(rs, buf, 0, 20);
    expect(decode(buf, n)).toBe("through-link");
    FS.close(rs);
  });

  // -------------------------------------------------------------------
  // O_EXCL edge cases
  // -------------------------------------------------------------------

  it("O_EXCL without O_CREAT is ignored (open existing file succeeds)", () => {
    const { FS } = h;
    FS.writeFile("/excl_no_creat", "data");
    // POSIX: O_EXCL without O_CREAT is undefined; Emscripten ignores it
    const stream = FS.open("/excl_no_creat", O.RDONLY | O.EXCL);
    expect(stream.fd).toBeGreaterThanOrEqual(0);
    FS.close(stream);
  });

  it("O_CREAT | O_EXCL on symlink to existing file throws EEXIST", () => {
    const { FS, E } = h;
    FS.writeFile("/excl_target", "data");
    FS.symlink("/excl_target", "/excl_link");
    // O_EXCL with O_CREAT should fail because the symlink resolves to an existing file
    expectErrno(
      () => FS.open("/excl_link", O.RDWR | O.CREAT | O.EXCL, 0o666),
      E.EEXIST,
    );
  });

  // -------------------------------------------------------------------
  // O_TRUNC
  // -------------------------------------------------------------------

  it("O_TRUNC on open truncates existing file to zero length @fast", () => {
    const { FS } = h;
    FS.writeFile("/trunc_me", "original content");

    const stream = FS.open("/trunc_me", O.RDWR | O.TRUNC);
    // File should be empty after O_TRUNC
    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(50);
    const n = FS.read(stream, buf, 0, 50);
    expect(n).toBe(0);
    FS.close(stream);

    // Verify via stat
    expect(FS.stat("/trunc_me").size).toBe(0);
  });

  it("O_TRUNC | O_WRONLY truncates and allows write", () => {
    const { FS } = h;
    FS.writeFile("/trunc_write", "old");

    const stream = FS.open("/trunc_write", O.WRONLY | O.TRUNC);
    FS.write(stream, encode("new content"), 0, 11);
    FS.close(stream);

    const rs = FS.open("/trunc_write", O.RDONLY);
    const buf = new Uint8Array(50);
    const n = FS.read(rs, buf, 0, 50);
    expect(decode(buf, n)).toBe("new content");
    FS.close(rs);
  });

  it("O_CREAT | O_TRUNC on nonexistent file creates empty file", () => {
    const { FS } = h;
    const stream = FS.open("/creat_trunc", O.RDWR | O.CREAT | O.TRUNC, 0o666);
    const buf = new Uint8Array(10);
    const n = FS.read(stream, buf, 0, 10);
    expect(n).toBe(0);
    FS.close(stream);
  });

  // -------------------------------------------------------------------
  // Directory open edge cases
  // -------------------------------------------------------------------

  it("open directory with O_WRONLY throws EISDIR @fast", () => {
    const { FS, E } = h;
    FS.mkdir("/dir_wronly");
    expectErrno(() => FS.open("/dir_wronly", O.WRONLY), E.EISDIR);
  });

  it("open directory with O_RDWR throws EISDIR", () => {
    const { FS, E } = h;
    FS.mkdir("/dir_rdwr");
    expectErrno(() => FS.open("/dir_rdwr", O.RDWR), E.EISDIR);
  });

  it("write to directory fd opened O_RDONLY throws", () => {
    const { FS, E } = h;
    FS.mkdir("/dir_wr");
    const stream = FS.open("/dir_wr", O.RDONLY | O.DIRECTORY);
    expectErrno(() => FS.write(stream, encode("x"), 0, 1), E.EBADF);
    FS.close(stream);
  });

  // -------------------------------------------------------------------
  // O_CREAT with nested nonexistent directories
  // -------------------------------------------------------------------

  it("O_CREAT does not create intermediate directories @fast", () => {
    const { FS, E } = h;
    expectErrno(
      () => FS.open("/no/such/dir/file", O.RDWR | O.CREAT, 0o666),
      E.ENOENT,
    );
  });

  // -------------------------------------------------------------------
  // Nonexistent paths
  // -------------------------------------------------------------------

  it("open nonexistent file without O_CREAT throws ENOENT", () => {
    const { FS, E } = h;
    expectErrno(() => FS.open("/no_such_file", O.RDONLY), E.ENOENT);
  });

  it("open nonexistent file with O_WRONLY without O_CREAT throws ENOENT", () => {
    const { FS, E } = h;
    expectErrno(() => FS.open("/no_such_file_w", O.WRONLY), E.ENOENT);
  });

  // -------------------------------------------------------------------
  // Symlink chain resolution
  // -------------------------------------------------------------------

  it("open through chain of symlinks resolves correctly @fast", () => {
    const { FS } = h;
    FS.writeFile("/chain_target", "found");
    FS.symlink("/chain_target", "/link1");
    FS.symlink("/link1", "/link2");
    FS.symlink("/link2", "/link3");

    const stream = FS.open("/link3", O.RDONLY);
    const buf = new Uint8Array(20);
    const n = FS.read(stream, buf, 0, 20);
    expect(decode(buf, n)).toBe("found");
    FS.close(stream);
  });

  it("O_NOFOLLOW on intermediate symlink in path succeeds", () => {
    const { FS } = h;
    // O_NOFOLLOW only applies to the final component, not intermediate ones
    FS.mkdir("/real_dir");
    FS.symlink("/real_dir", "/dir_link");
    FS.writeFile("/real_dir/file", "data");

    // Open /dir_link/file with O_NOFOLLOW — should succeed because
    // the final component "file" is not a symlink
    const stream = FS.open("/dir_link/file", O.RDONLY | O.NOFOLLOW);
    const buf = new Uint8Array(10);
    const n = FS.read(stream, buf, 0, 10);
    expect(decode(buf, n)).toBe("data");
    FS.close(stream);
  });

  // -------------------------------------------------------------------
  // O_CREAT mode bits
  // -------------------------------------------------------------------

  it("O_CREAT creates file with specified mode bits", () => {
    const { FS } = h;
    const stream = FS.open("/mode_test", O.RDWR | O.CREAT, 0o644);
    FS.close(stream);

    const stat = FS.stat("/mode_test");
    // Check the permission bits (lower 12 bits of mode)
    expect(stat.mode & 0o777).toBe(0o644);
  });

  // -------------------------------------------------------------------
  // Multiple opens
  // -------------------------------------------------------------------

  it("multiple opens of same file return distinct fds @fast", () => {
    const { FS } = h;
    FS.writeFile("/multi_open", "data");

    const s1 = FS.open("/multi_open", O.RDONLY);
    const s2 = FS.open("/multi_open", O.RDONLY);
    const s3 = FS.open("/multi_open", O.RDWR);

    expect(s1.fd).not.toBe(s2.fd);
    expect(s2.fd).not.toBe(s3.fd);

    FS.close(s1);
    FS.close(s2);
    FS.close(s3);
  });

  it("each fd has independent seek position", () => {
    const { FS } = h;
    FS.writeFile("/indep_pos", "abcdef");

    const s1 = FS.open("/indep_pos", O.RDONLY);
    const s2 = FS.open("/indep_pos", O.RDONLY);

    // Seek s1 to position 3
    FS.llseek(s1, 3, SEEK_SET);

    // s2 should still be at 0
    const buf1 = new Uint8Array(10);
    const n1 = FS.read(s2, buf1, 0, 10);
    expect(decode(buf1, n1)).toBe("abcdef");

    // s1 reads from position 3
    const buf2 = new Uint8Array(10);
    const n2 = FS.read(s1, buf2, 0, 10);
    expect(decode(buf2, n2)).toBe("def");

    FS.close(s1);
    FS.close(s2);
  });
});
