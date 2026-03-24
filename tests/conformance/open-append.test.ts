/**
 * Conformance tests ported from: emscripten/test/wasmfs/wasmfs_open_append.c
 *
 * Tests: O_APPEND semantics — writes always go to end of file,
 *        even after seeking to a different position.
 */
import {
  createFS,
  encode,
  decode,
  O,
  SEEK_SET,
  SEEK_CUR,
  type FSHarness,
} from "../harness/emscripten-fs.js";

describe("open append (wasmfs_open_append.c)", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  it("O_APPEND writes always go to end of file @fast", () => {
    const { FS } = h;

    const stream = FS.open(
      "/foo.txt",
      O.RDWR | O.CREAT | O.EXCL | O.APPEND,
      0o777,
    );
    expect(stream.fd).toBeGreaterThan(0);

    // Initial position is 0
    let pos = FS.llseek(stream, 0, SEEK_CUR);
    expect(pos).toBe(0);

    // Write "Hello" — should go to position 0 (file is empty)
    let nwritten = FS.write(stream, encode("Hello"), 0, 5);
    expect(nwritten).toBe(5);

    pos = FS.llseek(stream, 0, SEEK_CUR);
    expect(pos).toBe(5);

    // Seek back to 0
    FS.llseek(stream, 0, SEEK_SET);
    pos = FS.llseek(stream, 0, SEEK_CUR);
    expect(pos).toBe(0);

    // Zero-length write — O_APPEND should still move position to end
    nwritten = FS.write(stream, encode(""), 0, 0);
    expect(nwritten).toBe(0);

    // Position should be at end (5) after zero-length append write
    pos = FS.llseek(stream, 0, SEEK_CUR);
    expect(pos).toBe(5);

    // Seek back to 0 again
    FS.llseek(stream, 0, SEEK_SET);
    pos = FS.llseek(stream, 0, SEEK_CUR);
    expect(pos).toBe(0);

    // Write ", world!" — should append at end despite seek to 0
    nwritten = FS.write(stream, encode(", world!"), 0, 8);
    expect(nwritten).toBe(8);

    pos = FS.llseek(stream, 0, SEEK_CUR);
    expect(pos).toBe(13);

    // Seek to arbitrary position 42
    FS.llseek(stream, 42, SEEK_SET);
    pos = FS.llseek(stream, 0, SEEK_CUR);
    expect(pos).toBe(42);

    // Write "!!" — should append at position 13 (end of data)
    nwritten = FS.write(stream, encode("!!"), 0, 2);
    expect(nwritten).toBe(2);

    pos = FS.llseek(stream, 0, SEEK_CUR);
    expect(pos).toBe(15);

    // Read back entire file content
    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(100);
    const nread = FS.read(stream, buf, 0, 100);
    expect(nread).toBe(15);
    expect(decode(buf, nread)).toBe("Hello, world!!!");

    FS.close(stream);
  });
});
