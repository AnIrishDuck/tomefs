/**
 * Adversarial differential tests: Unlink with open file descriptors.
 *
 * POSIX requires that unlinking a file while it has open fds keeps the
 * data accessible through those fds until the last fd is closed. This
 * is tricky for a page cache because the storage path is used as the
 * cache key — the cache must not delete pages while fds are still open.
 */
import {
  createFS,
  encode,
  decode,
  O,
  type FSHarness,
} from "../harness/emscripten-fs.js";

const PAGE_SIZE = 8192;

describe("adversarial: unlink with open file descriptors", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  // ------------------------------------------------------------------
  // Read from unlinked file
  // ------------------------------------------------------------------

  it("read from fd after unlink returns correct data @fast", () => {
    const { FS } = h;
    const s = FS.open("/ephemeral", O.RDWR | O.CREAT, 0o666);
    const data = encode("still readable after unlink");
    FS.write(s, data, 0, data.length, 0);

    // Unlink while fd is open
    FS.unlink("/ephemeral");

    // Read through the open fd — must still work
    const buf = new Uint8Array(data.length);
    const n = FS.read(s, buf, 0, data.length, 0);
    expect(n).toBe(data.length);
    expect(decode(buf, n)).toBe("still readable after unlink");

    FS.close(s);
  });

  // ------------------------------------------------------------------
  // Write to unlinked file
  // ------------------------------------------------------------------

  it("write to fd after unlink succeeds", () => {
    const { FS } = h;
    const s = FS.open("/writable-unlinked", O.RDWR | O.CREAT, 0o666);
    const initial = encode("hello");
    FS.write(s, initial, 0, initial.length, 0);

    FS.unlink("/writable-unlinked");

    // Write more data through the open fd
    const more = encode(" world");
    FS.write(s, more, 0, more.length, initial.length);

    // Read back
    const buf = new Uint8Array(11);
    const n = FS.read(s, buf, 0, 11, 0);
    expect(n).toBe(11);
    expect(decode(buf, n)).toBe("hello world");

    FS.close(s);
  });

  // ------------------------------------------------------------------
  // Multi-page file accessible after unlink
  // ------------------------------------------------------------------

  it("multi-page file fully readable after unlink", () => {
    const { FS } = h;
    const s = FS.open("/big-ephemeral", O.RDWR | O.CREAT, 0o666);

    const data = new Uint8Array(PAGE_SIZE * 4);
    for (let i = 0; i < data.length; i++) {
      data[i] = (i * 19 + 3) & 0xff;
    }
    FS.write(s, data, 0, data.length, 0);

    FS.unlink("/big-ephemeral");

    // Read all pages back
    for (let p = 0; p < 4; p++) {
      const buf = new Uint8Array(PAGE_SIZE);
      const n = FS.read(s, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expect(n).toBe(PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) {
        const expected = ((p * PAGE_SIZE + i) * 19 + 3) & 0xff;
        expect(buf[i]).toBe(expected);
      }
    }

    FS.close(s);
  });

  // ------------------------------------------------------------------
  // Unlink, then create new file at same path — no data leakage
  // ------------------------------------------------------------------

  it("new file at unlinked path has no data from old file", () => {
    const { FS } = h;

    // Create file with data
    const s1 = FS.open("/reuse", O.RDWR | O.CREAT, 0o666);
    const old = new Uint8Array(PAGE_SIZE);
    old.fill(0xaa);
    FS.write(s1, old, 0, PAGE_SIZE, 0);
    FS.close(s1);

    // Unlink
    FS.unlink("/reuse");

    // Create new file at same path
    const s2 = FS.open("/reuse", O.RDWR | O.CREAT, 0o666);
    const fresh = encode("fresh");
    FS.write(s2, fresh, 0, fresh.length, 0);

    // Verify new file has only our fresh data
    expect(FS.stat("/reuse").size).toBe(5);
    const buf = new Uint8Array(5);
    const n = FS.read(s2, buf, 0, 5, 0);
    expect(decode(buf, n)).toBe("fresh");

    FS.close(s2);
  });

  // ------------------------------------------------------------------
  // Multiple open fds, close one at a time
  // ------------------------------------------------------------------

  it("multiple fds: data survives until last fd closed", () => {
    const { FS } = h;
    const s1 = FS.open("/multi-fd", O.RDWR | O.CREAT, 0o666);
    const s2 = FS.open("/multi-fd", O.RDONLY);

    const data = encode("shared across fds");
    FS.write(s1, data, 0, data.length, 0);

    // Unlink
    FS.unlink("/multi-fd");

    // Close s1, s2 should still work
    FS.close(s1);

    const buf = new Uint8Array(data.length);
    const n = FS.read(s2, buf, 0, data.length, 0);
    expect(n).toBe(data.length);
    expect(decode(buf, n)).toBe("shared across fds");

    FS.close(s2);
  });

  // ------------------------------------------------------------------
  // Unlink during cache pressure
  // ------------------------------------------------------------------

  it("unlink multi-page file during cache pressure", () => {
    const { FS } = h;

    // Create a file we'll keep open and unlink
    const kept = FS.open("/kept", O.RDWR | O.CREAT, 0o666);
    const keptData = new Uint8Array(PAGE_SIZE * 2);
    keptData.fill(0xdd);
    FS.write(kept, keptData, 0, keptData.length, 0);

    // Unlink it
    FS.unlink("/kept");

    // Create cache pressure by writing many pages to another file
    const thrash = FS.open("/thrash", O.RDWR | O.CREAT, 0o666);
    for (let p = 0; p < 32; p++) {
      const fill = new Uint8Array(PAGE_SIZE);
      fill.fill(p);
      FS.write(thrash, fill, 0, PAGE_SIZE, p * PAGE_SIZE);
    }
    FS.close(thrash);

    // The unlinked file should still be fully readable
    const buf = new Uint8Array(PAGE_SIZE * 2);
    const n = FS.read(kept, buf, 0, buf.length, 0);
    expect(n).toBe(PAGE_SIZE * 2);
    for (let i = 0; i < buf.length; i++) {
      expect(buf[i]).toBe(0xdd);
    }

    FS.close(kept);
  });
});
