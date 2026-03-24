/**
 * Conformance tests ported from: emscripten/test/fs/test_fs_rename_on_existing.c
 *
 * Tests: rename a file over an existing file, then unlink and recreate.
 */
import {
  createFS,
  encode,
  decode,
  expectErrno,
  O,
  type FSHarness,
} from "../harness/emscripten-fs.js";

describe("rename on existing (test_fs_rename_on_existing.c)", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  it("rename overwrites existing file @fast", () => {
    const { FS } = h;

    // create_file("a", "abc")
    const s1 = FS.open("/a", O.WRONLY | O.CREAT | O.EXCL, 0o666);
    FS.write(s1, encode("abc"), 0, 3);
    FS.close(s1);

    // create_file("b", "xyz")
    const s2 = FS.open("/b", O.WRONLY | O.CREAT | O.EXCL, 0o666);
    FS.write(s2, encode("xyz"), 0, 3);
    FS.close(s2);

    // rename("a", "b") — should overwrite b with a's contents
    FS.rename("/a", "/b");

    // /a should no longer exist
    expectErrno(() => FS.stat("/a"), h.E.ENOENT);

    // /b should now have "abc" (a's old contents)
    const data = FS.readFile("/b", {}) as Uint8Array;
    expect(decode(data)).toBe("abc");
  });

  it("rename-overwrite then unlink then recreate @fast", () => {
    const { FS } = h;

    // create_file("a", "abc")
    const s1 = FS.open("/a", O.WRONLY | O.CREAT | O.EXCL, 0o666);
    FS.write(s1, encode("abc"), 0, 3);
    FS.close(s1);

    // create_file("b", "xyz")
    const s2 = FS.open("/b", O.WRONLY | O.CREAT | O.EXCL, 0o666);
    FS.write(s2, encode("xyz"), 0, 3);
    FS.close(s2);

    // rename a over b, then unlink b
    FS.rename("/a", "/b");
    FS.unlink("/b");

    // Recreate b — should succeed (O_EXCL ensures no leftover)
    const s3 = FS.open("/b", O.WRONLY | O.CREAT | O.EXCL, 0o666);
    FS.write(s3, encode("xyz"), 0, 3);
    FS.close(s3);

    const data = FS.readFile("/b", {}) as Uint8Array;
    expect(decode(data)).toBe("xyz");
  });
});
