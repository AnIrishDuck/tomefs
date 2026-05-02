/**
 * Conformance tests ported from: emscripten/test/wasmfs/wasmfs_open_append.c
 *
 * Tests: O_APPEND semantics — writes always go to end of file,
 *        even after seeking to a different position.
 *
 * Extended with additional tests for multi-fd append, cross-page-boundary
 * appends, append-after-truncate, pwrite independence, and interleaved
 * append/non-append fd writes. These cover the I/O patterns used by
 * Postgres WAL writes and other append-heavy workloads.
 */
import {
  createFS,
  encode,
  decode,
  O,
  SEEK_SET,
  SEEK_CUR,
  SEEK_END,
  type FSHarness,
} from "../harness/emscripten-fs.js";

const PAGE_SIZE = 8192;

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

  // -------------------------------------------------------------------
  // Multi-fd append
  // -------------------------------------------------------------------

  it("two O_APPEND fds both append at the true end @fast", () => {
    const { FS } = h;

    FS.writeFile("/multi-append", "");
    const fd1 = FS.open("/multi-append", O.WRONLY | O.APPEND);
    const fd2 = FS.open("/multi-append", O.WRONLY | O.APPEND);

    FS.write(fd1, encode("AAA"), 0, 3);
    FS.write(fd2, encode("BBB"), 0, 3);
    FS.write(fd1, encode("CCC"), 0, 3);

    FS.close(fd1);
    FS.close(fd2);

    const content = FS.readFile("/multi-append", { encoding: "utf8" });
    expect(content).toBe("AAABBBCCC");
  });

  it("one O_APPEND fd and one regular fd interleave correctly @fast", () => {
    const { FS } = h;

    const data = encode("initial_data!");
    FS.writeFile("/interleave", data);

    const appendFd = FS.open("/interleave", O.WRONLY | O.APPEND);
    const normalFd = FS.open("/interleave", O.RDWR);

    // Append fd writes at end
    FS.write(appendFd, encode("_appended"), 0, 9);

    // Normal fd overwrites at position 0
    FS.write(normalFd, encode("OVERWRITE"), 0, 9);

    FS.close(appendFd);
    FS.close(normalFd);

    const content = FS.readFile("/interleave", { encoding: "utf8" });
    expect(content).toBe("OVERWRITEata!_appended");
  });

  // -------------------------------------------------------------------
  // Append after truncate
  // -------------------------------------------------------------------

  it("O_APPEND write after ftruncate goes to new end @fast", () => {
    const { FS } = h;

    const stream = FS.open("/append-trunc", O.RDWR | O.CREAT | O.APPEND, 0o666);

    // Write some data
    FS.write(stream, encode("ABCDEFGHIJ"), 0, 10);
    expect(FS.fstat(stream.fd).size).toBe(10);

    // Truncate to 5 bytes
    FS.ftruncate(stream.fd, 5);
    expect(FS.fstat(stream.fd).size).toBe(5);

    // Append write should go to position 5 (new end)
    FS.write(stream, encode("XYZ"), 0, 3);
    expect(FS.fstat(stream.fd).size).toBe(8);

    // Verify content
    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(8);
    const n = FS.read(stream, buf, 0, 8);
    expect(n).toBe(8);
    expect(decode(buf, n)).toBe("ABCDEXYZ");

    FS.close(stream);
  });

  it("O_APPEND write after truncate to zero", () => {
    const { FS } = h;

    const stream = FS.open("/append-trunc0", O.RDWR | O.CREAT | O.APPEND, 0o666);
    FS.write(stream, encode("data"), 0, 4);

    FS.ftruncate(stream.fd, 0);
    expect(FS.fstat(stream.fd).size).toBe(0);

    FS.write(stream, encode("new"), 0, 3);
    expect(FS.fstat(stream.fd).size).toBe(3);

    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(3);
    expect(FS.read(stream, buf, 0, 3)).toBe(3);
    expect(decode(buf, 3)).toBe("new");

    FS.close(stream);
  });

  // -------------------------------------------------------------------
  // Positioned write (pwrite) with O_APPEND
  // -------------------------------------------------------------------

  it("positioned write on O_APPEND fd writes at specified position @fast", () => {
    const { FS } = h;

    const stream = FS.open("/pwrite-append", O.RDWR | O.CREAT | O.APPEND, 0o666);
    FS.write(stream, encode("AAAAABBBBB"), 0, 10);

    // Positioned write at offset 3 — should write there, NOT at end
    FS.write(stream, encode("XX"), 0, 2, 3);

    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(10);
    const n = FS.read(stream, buf, 0, 10);
    expect(n).toBe(10);
    expect(decode(buf, n)).toBe("AAAXXBBBBB");

    FS.close(stream);
  });

  it("positioned write does not affect subsequent append writes", () => {
    const { FS } = h;

    const stream = FS.open("/pwrite-then-append", O.RDWR | O.CREAT | O.APPEND, 0o666);
    FS.write(stream, encode("12345"), 0, 5);

    // Positioned write in the middle
    FS.write(stream, encode("X"), 0, 1, 2);

    // Next non-positioned write should still append at end
    FS.write(stream, encode("END"), 0, 3);

    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(8);
    const n = FS.read(stream, buf, 0, 8);
    expect(n).toBe(8);
    expect(decode(buf, n)).toBe("12X45END");

    FS.close(stream);
  });

  // -------------------------------------------------------------------
  // Cross-page-boundary appends
  // -------------------------------------------------------------------

  it("O_APPEND write that crosses a page boundary @fast", () => {
    const { FS } = h;

    const stream = FS.open("/cross-page", O.RDWR | O.CREAT | O.APPEND, 0o666);

    // Fill to 100 bytes before page boundary
    const fill = new Uint8Array(PAGE_SIZE - 100);
    for (let i = 0; i < fill.length; i++) fill[i] = 0xaa;
    FS.write(stream, fill, 0, fill.length);

    // Append 200 bytes — crosses page boundary
    const cross = new Uint8Array(200);
    for (let i = 0; i < cross.length; i++) cross[i] = 0xbb;
    FS.write(stream, cross, 0, cross.length);

    expect(FS.fstat(stream.fd).size).toBe(PAGE_SIZE + 100);

    // Read back and verify
    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(PAGE_SIZE + 100);
    const n = FS.read(stream, buf, 0, buf.length);
    expect(n).toBe(PAGE_SIZE + 100);

    // First region: 0xAA
    for (let i = 0; i < PAGE_SIZE - 100; i++) {
      if (buf[i] !== 0xaa) {
        throw new Error(`Expected 0xAA at offset ${i}, got 0x${buf[i].toString(16)}`);
      }
    }
    // Second region: 0xBB
    for (let i = PAGE_SIZE - 100; i < PAGE_SIZE + 100; i++) {
      if (buf[i] !== 0xbb) {
        throw new Error(`Expected 0xBB at offset ${i}, got 0x${buf[i].toString(16)}`);
      }
    }

    FS.close(stream);
  });

  it("multiple small appends accumulating across page boundaries", () => {
    const { FS } = h;

    const stream = FS.open("/multi-cross", O.RDWR | O.CREAT | O.APPEND, 0o666);

    // Write 1000 small chunks of 100 bytes each = 100KB, crossing ~12 pages
    const chunk = new Uint8Array(100);
    for (let i = 0; i < 1000; i++) {
      chunk.fill(i & 0xff);
      FS.write(stream, chunk, 0, 100);
    }

    expect(FS.fstat(stream.fd).size).toBe(100000);

    // Verify by reading back and checking pattern
    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(100);
    for (let i = 0; i < 1000; i++) {
      const n = FS.read(stream, buf, 0, 100);
      expect(n).toBe(100);
      const expected = i & 0xff;
      for (let j = 0; j < 100; j++) {
        if (buf[j] !== expected) {
          throw new Error(
            `Chunk ${i} byte ${j}: expected ${expected}, got ${buf[j]}`,
          );
        }
      }
    }

    FS.close(stream);
  });

  // -------------------------------------------------------------------
  // O_APPEND + O_TRUNC
  // -------------------------------------------------------------------

  it("O_APPEND | O_TRUNC truncates then appends correctly", () => {
    const { FS } = h;

    FS.writeFile("/append-trunc-open", "old_content");

    const stream = FS.open(
      "/append-trunc-open",
      O.WRONLY | O.APPEND | O.TRUNC,
    );

    expect(FS.fstat(stream.fd).size).toBe(0);

    FS.write(stream, encode("fresh"), 0, 5);
    expect(FS.fstat(stream.fd).size).toBe(5);

    FS.close(stream);

    const content = FS.readFile("/append-trunc-open", { encoding: "utf8" });
    expect(content).toBe("fresh");
  });

  // -------------------------------------------------------------------
  // Append and read on same fd
  // -------------------------------------------------------------------

  it("read on O_APPEND|O_RDWR fd works from current position @fast", () => {
    const { FS } = h;

    const stream = FS.open("/append-read", O.RDWR | O.CREAT | O.APPEND, 0o666);
    FS.write(stream, encode("ABCDEF"), 0, 6);

    // Seek to position 2
    FS.llseek(stream, 2, SEEK_SET);
    const buf = new Uint8Array(3);
    const n = FS.read(stream, buf, 0, 3);
    expect(n).toBe(3);
    expect(decode(buf, n)).toBe("CDE");

    // Write should still append at end (position 6)
    FS.write(stream, encode("GH"), 0, 2);
    expect(FS.fstat(stream.fd).size).toBe(8);

    // Verify full content
    FS.llseek(stream, 0, SEEK_SET);
    const full = new Uint8Array(8);
    expect(FS.read(stream, full, 0, 8)).toBe(8);
    expect(decode(full, 8)).toBe("ABCDEFGH");

    FS.close(stream);
  });

  // -------------------------------------------------------------------
  // Append with SEEK_END
  // -------------------------------------------------------------------

  it("SEEK_END on O_APPEND fd reports correct end position", () => {
    const { FS } = h;

    const stream = FS.open("/append-seekend", O.RDWR | O.CREAT | O.APPEND, 0o666);
    FS.write(stream, encode("12345"), 0, 5);

    const pos = FS.llseek(stream, 0, SEEK_END);
    expect(pos).toBe(5);

    FS.write(stream, encode("67"), 0, 2);
    const pos2 = FS.llseek(stream, 0, SEEK_END);
    expect(pos2).toBe(7);

    FS.close(stream);
  });

  // -------------------------------------------------------------------
  // Append to file opened via O_CREAT (new file)
  // -------------------------------------------------------------------

  it("O_APPEND on a newly created file starts at offset 0", () => {
    const { FS } = h;

    const stream = FS.open("/new-append", O.WRONLY | O.CREAT | O.APPEND, 0o666);

    FS.write(stream, encode("first"), 0, 5);
    FS.write(stream, encode("second"), 0, 6);

    FS.close(stream);

    const content = FS.readFile("/new-append", { encoding: "utf8" });
    expect(content).toBe("firstsecond");
  });

  // -------------------------------------------------------------------
  // stat.size consistency during appends
  // -------------------------------------------------------------------

  it("stat reflects correct size after each append @fast", () => {
    const { FS } = h;

    const stream = FS.open("/stat-append", O.WRONLY | O.CREAT | O.APPEND, 0o666);

    expect(FS.fstat(stream.fd).size).toBe(0);

    FS.write(stream, encode("A"), 0, 1);
    expect(FS.fstat(stream.fd).size).toBe(1);

    FS.write(stream, encode("BB"), 0, 2);
    expect(FS.fstat(stream.fd).size).toBe(3);

    FS.write(stream, encode("CCC"), 0, 3);
    expect(FS.fstat(stream.fd).size).toBe(6);

    // stat by path should agree
    expect(FS.stat("/stat-append").size).toBe(6);

    FS.close(stream);
  });

  // -------------------------------------------------------------------
  // Append after dup
  // -------------------------------------------------------------------

  it("duped O_APPEND fd also appends @fast", () => {
    const { FS } = h;

    const stream = FS.open("/dup-append", O.RDWR | O.CREAT | O.APPEND, 0o666);
    FS.write(stream, encode("orig"), 0, 4);

    const dup = FS.dupStream(stream);
    FS.write(dup, encode("_dup"), 0, 4);

    FS.close(dup);
    FS.close(stream);

    const content = FS.readFile("/dup-append", { encoding: "utf8" });
    expect(content).toBe("orig_dup");
  });

  // -------------------------------------------------------------------
  // Large append write
  // -------------------------------------------------------------------

  it("single large append spanning multiple pages", () => {
    const { FS } = h;

    const stream = FS.open("/big-append", O.WRONLY | O.CREAT | O.APPEND, 0o666);

    // Write 3.5 pages in one call
    const size = PAGE_SIZE * 3 + PAGE_SIZE / 2;
    const data = new Uint8Array(size);
    for (let i = 0; i < size; i++) data[i] = (i * 37 + 13) & 0xff;
    FS.write(stream, data, 0, size);

    expect(FS.fstat(stream.fd).size).toBe(size);
    FS.close(stream);

    // Read back and verify
    const readStream = FS.open("/big-append", O.RDONLY);
    const buf = new Uint8Array(size);
    const n = FS.read(readStream, buf, 0, size);
    expect(n).toBe(size);
    for (let i = 0; i < size; i++) {
      if (buf[i] !== data[i]) {
        throw new Error(
          `Byte mismatch at offset ${i}: expected ${data[i]}, got ${buf[i]}`,
        );
      }
    }
    FS.close(readStream);
  });
});
