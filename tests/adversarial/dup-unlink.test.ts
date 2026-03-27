/**
 * Adversarial tests: dup + unlink interaction with openCount tracking.
 *
 * Emscripten's FS.dupStream() calls stream_ops.dup() (NOT stream_ops.open()).
 * If tomefs doesn't implement the dup stream_op, openCount won't be
 * incremented for dup'd fds. This causes premature page cleanup when
 * the original fd is closed on an unlinked file — the dup'd fd loses
 * its data.
 *
 * This is a real-world scenario: Postgres dup's WAL file descriptors
 * and may unlink/rename files while dup'd fds are still in use.
 *
 * Ethos §9: "Write tests designed to break tomefs specifically"
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

const PAGE_SIZE = 8192;

describe("adversarial: dup + unlink openCount tracking", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  // ------------------------------------------------------------------
  // Core bug: dup + unlink + close original → dup should still work
  // ------------------------------------------------------------------

  it("read through dup'd fd after unlink + close original @fast", () => {
    const { FS } = h;
    const data = encode("dup-unlink-data");

    const s = FS.open("/victim", O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length, 0);

    // Dup the fd
    const dup = FS.dupStream(s);

    // Unlink while both fds are open
    FS.unlink("/victim");

    // Close the ORIGINAL fd first
    FS.close(s);

    // The dup'd fd must still be readable
    FS.llseek(dup, 0, SEEK_SET);
    const buf = new Uint8Array(data.length);
    const n = FS.read(dup, buf, 0, data.length);
    expect(n).toBe(data.length);
    expect(decode(buf, n)).toBe("dup-unlink-data");

    FS.close(dup);
  });

  it("write through dup'd fd after unlink + close original", () => {
    const { FS } = h;
    const initial = encode("initial");

    const s = FS.open("/victim2", O.RDWR | O.CREAT, 0o666);
    FS.write(s, initial, 0, initial.length, 0);

    const dup = FS.dupStream(s);

    FS.unlink("/victim2");
    FS.close(s);

    // Write NEW data through the dup'd fd
    const newData = encode("updated-via-dup");
    FS.llseek(dup, 0, SEEK_SET);
    FS.write(dup, newData, 0, newData.length, 0);

    // Read it back
    FS.llseek(dup, 0, SEEK_SET);
    const buf = new Uint8Array(newData.length);
    const n = FS.read(dup, buf, 0, newData.length);
    expect(n).toBe(newData.length);
    expect(decode(buf, n)).toBe("updated-via-dup");

    FS.close(dup);
  });

  // ------------------------------------------------------------------
  // Multi-page: ensure pages survive across dup + unlink
  // ------------------------------------------------------------------

  it("dup'd fd reads multi-page file after unlink + close original @fast", () => {
    const { FS } = h;
    // Create a file spanning multiple pages
    const data = new Uint8Array(PAGE_SIZE * 2 + 500);
    for (let i = 0; i < data.length; i++) data[i] = i & 0xff;

    const s = FS.open("/bigfile", O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length, 0);

    const dup = FS.dupStream(s);

    FS.unlink("/bigfile");
    FS.close(s);

    // Read entire file through dup
    FS.llseek(dup, 0, SEEK_SET);
    const buf = new Uint8Array(data.length);
    const n = FS.read(dup, buf, 0, data.length);
    expect(n).toBe(data.length);
    for (let i = 0; i < data.length; i++) {
      if (buf[i] !== data[i]) {
        throw new Error(
          `content mismatch at byte ${i}: expected=${data[i]}, got=${buf[i]}`,
        );
      }
    }

    FS.close(dup);
  });

  // ------------------------------------------------------------------
  // Multiple dups: all must survive until the very last close
  // ------------------------------------------------------------------

  it("multiple dup'd fds survive unlink, pages cleaned up only after last close", () => {
    const { FS } = h;
    const data = encode("triple-dup");

    const s = FS.open("/multi", O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length, 0);

    const dup1 = FS.dupStream(s);
    const dup2 = FS.dupStream(s);

    FS.unlink("/multi");

    // Close original and first dup
    FS.close(s);
    FS.close(dup1);

    // dup2 must still be readable
    FS.llseek(dup2, 0, SEEK_SET);
    const buf = new Uint8Array(data.length);
    const n = FS.read(dup2, buf, 0, data.length);
    expect(n).toBe(data.length);
    expect(decode(buf, n)).toBe("triple-dup");

    FS.close(dup2);
  });

  // ------------------------------------------------------------------
  // Dup + rename + unlink: fd survives path changes
  // ------------------------------------------------------------------

  it("dup'd fd survives rename of the file", () => {
    const { FS } = h;
    const data = encode("rename-me");

    const s = FS.open("/before", O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length, 0);

    const dup = FS.dupStream(s);

    // Rename the file
    FS.rename("/before", "/after");

    // Both fds should still work
    FS.llseek(s, 0, SEEK_SET);
    const buf1 = new Uint8Array(data.length);
    expect(FS.read(s, buf1, 0, data.length)).toBe(data.length);
    expect(decode(buf1, data.length)).toBe("rename-me");

    FS.llseek(dup, 0, SEEK_SET);
    const buf2 = new Uint8Array(data.length);
    expect(FS.read(dup, buf2, 0, data.length)).toBe(data.length);
    expect(decode(buf2, data.length)).toBe("rename-me");

    FS.close(s);
    FS.close(dup);
  });

  // ------------------------------------------------------------------
  // Dup + unlink + create same path: no storagePath collision
  // ------------------------------------------------------------------

  it("new file at same path doesn't corrupt dup'd fd of unlinked file @fast", () => {
    const { FS } = h;
    const oldData = encode("old-data");
    const newData = encode("new-data-different");

    // Create original, dup it, unlink
    const s = FS.open("/reuse", O.RDWR | O.CREAT, 0o666);
    FS.write(s, oldData, 0, oldData.length, 0);
    const dup = FS.dupStream(s);
    FS.unlink("/reuse");

    // Create a new file at the same path
    const s2 = FS.open("/reuse", O.RDWR | O.CREAT, 0o666);
    FS.write(s2, newData, 0, newData.length, 0);

    // Old dup'd fd should still read old data
    FS.llseek(dup, 0, SEEK_SET);
    const oldBuf = new Uint8Array(oldData.length);
    const n1 = FS.read(dup, oldBuf, 0, oldData.length);
    expect(n1).toBe(oldData.length);
    expect(decode(oldBuf, n1)).toBe("old-data");

    // New fd should read new data
    FS.llseek(s2, 0, SEEK_SET);
    const newBuf = new Uint8Array(newData.length);
    const n2 = FS.read(s2, newBuf, 0, newData.length);
    expect(n2).toBe(newData.length);
    expect(decode(newBuf, n2)).toBe("new-data-different");

    FS.close(s);
    FS.close(dup);
    FS.close(s2);
  });

  // ------------------------------------------------------------------
  // Dup with cache pressure: eviction must not lose dup'd file's pages
  // ------------------------------------------------------------------

  it("dup'd unlinked fd survives cache eviction pressure", () => {
    const { FS } = h;
    // Write data spanning multiple pages
    const data = new Uint8Array(PAGE_SIZE * 3);
    for (let i = 0; i < data.length; i++) data[i] = (i * 7 + 13) & 0xff;

    const s = FS.open("/evict-me", O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length, 0);
    const dup = FS.dupStream(s);

    FS.unlink("/evict-me");
    FS.close(s);

    // Create many other files to cause cache eviction
    for (let i = 0; i < 10; i++) {
      const tmp = FS.open(`/pressure_${i}`, O.RDWR | O.CREAT, 0o666);
      const fill = new Uint8Array(PAGE_SIZE * 2);
      fill.fill(i & 0xff);
      FS.write(tmp, fill, 0, fill.length, 0);
      FS.close(tmp);
    }

    // Dup'd fd should still read correct data
    FS.llseek(dup, 0, SEEK_SET);
    const buf = new Uint8Array(data.length);
    const n = FS.read(dup, buf, 0, data.length);
    expect(n).toBe(data.length);
    for (let i = 0; i < data.length; i++) {
      if (buf[i] !== data[i]) {
        throw new Error(
          `byte ${i}: expected=${data[i]}, got=${buf[i]}`,
        );
      }
    }

    FS.close(dup);
  });
});
