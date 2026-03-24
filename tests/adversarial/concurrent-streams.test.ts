/**
 * Adversarial differential tests: Concurrent streams on the same file.
 *
 * Multiple open file descriptors sharing the same underlying node.
 * Tests that writes through one stream are visible through another,
 * and that independent seek positions don't corrupt each other.
 */
import {
  createFS,
  encode,
  decode,
  O,
  SEEK_SET,
  SEEK_END,
  type FSHarness,
} from "../harness/emscripten-fs.js";

const PAGE_SIZE = 8192;

describe("adversarial: concurrent streams", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  // ------------------------------------------------------------------
  // Two readers see the same data
  // ------------------------------------------------------------------

  it("two readers on same file see identical data @fast", () => {
    const { FS } = h;

    // Write a multi-page file
    const data = new Uint8Array(PAGE_SIZE * 3);
    for (let i = 0; i < data.length; i++) {
      data[i] = (i * 11) & 0xff;
    }
    const w = FS.open("/shared", O.RDWR | O.CREAT, 0o666);
    FS.write(w, data, 0, data.length, 0);
    FS.close(w);

    // Open two independent read streams
    const r1 = FS.open("/shared", O.RDONLY);
    const r2 = FS.open("/shared", O.RDONLY);

    // Read different regions through each stream
    const buf1 = new Uint8Array(PAGE_SIZE);
    const buf2 = new Uint8Array(PAGE_SIZE);

    FS.read(r1, buf1, 0, PAGE_SIZE, 0);
    FS.read(r2, buf2, 0, PAGE_SIZE, PAGE_SIZE);

    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(buf1[i]).toBe((i * 11) & 0xff);
      expect(buf2[i]).toBe(((PAGE_SIZE + i) * 11) & 0xff);
    }

    FS.close(r1);
    FS.close(r2);
  });

  // ------------------------------------------------------------------
  // Write through one stream, read through another
  // ------------------------------------------------------------------

  it("write through stream A visible in stream B immediately", () => {
    const { FS } = h;
    const a = FS.open("/wrab", O.RDWR | O.CREAT, 0o666);
    const b = FS.open("/wrab", O.RDONLY);

    // Write through A
    const msg = encode("hello from stream A");
    FS.write(a, msg, 0, msg.length, 0);

    // Read through B — should see the data immediately (shared node)
    const buf = new Uint8Array(msg.length);
    const n = FS.read(b, buf, 0, msg.length, 0);
    expect(n).toBe(msg.length);
    expect(decode(buf, n)).toBe("hello from stream A");

    FS.close(a);
    FS.close(b);
  });

  // ------------------------------------------------------------------
  // Concurrent writes to different regions of same file
  // ------------------------------------------------------------------

  it("concurrent writes to non-overlapping regions", () => {
    const { FS } = h;
    const a = FS.open("/regions", O.RDWR | O.CREAT, 0o666);
    const b = FS.open("/regions", O.RDWR);

    // Stream A writes to page 0
    const dataA = new Uint8Array(PAGE_SIZE);
    dataA.fill(0xaa);
    FS.write(a, dataA, 0, PAGE_SIZE, 0);

    // Stream B writes to page 2
    const dataB = new Uint8Array(PAGE_SIZE);
    dataB.fill(0xbb);
    FS.write(b, dataB, 0, PAGE_SIZE, PAGE_SIZE * 2);

    // Read full file through a third stream
    const c = FS.open("/regions", O.RDONLY);
    const full = new Uint8Array(PAGE_SIZE * 3);
    const n = FS.read(c, full, 0, full.length, 0);
    expect(n).toBe(PAGE_SIZE * 3);

    // Page 0: 0xAA
    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(full[i]).toBe(0xaa);
    }
    // Page 1: zeros (gap)
    for (let i = PAGE_SIZE; i < PAGE_SIZE * 2; i++) {
      expect(full[i]).toBe(0);
    }
    // Page 2: 0xBB
    for (let i = PAGE_SIZE * 2; i < PAGE_SIZE * 3; i++) {
      expect(full[i]).toBe(0xbb);
    }

    FS.close(a);
    FS.close(b);
    FS.close(c);
  });

  // ------------------------------------------------------------------
  // Concurrent writes to overlapping region — last write wins
  // ------------------------------------------------------------------

  it("overlapping writes: last write wins at byte level", () => {
    const { FS } = h;
    const a = FS.open("/overlap", O.RDWR | O.CREAT, 0o666);
    const b = FS.open("/overlap", O.RDWR);

    // Stream A writes 0xAA to bytes 0-99
    const dataA = new Uint8Array(100);
    dataA.fill(0xaa);
    FS.write(a, dataA, 0, 100, 0);

    // Stream B writes 0xBB to bytes 50-149 (overlapping 50-99)
    const dataB = new Uint8Array(100);
    dataB.fill(0xbb);
    FS.write(b, dataB, 0, 100, 50);

    // Read back
    const buf = new Uint8Array(150);
    const n = FS.read(a, buf, 0, 150, 0);
    expect(n).toBe(150);

    // Bytes 0-49: 0xAA (only A wrote here)
    for (let i = 0; i < 50; i++) {
      expect(buf[i]).toBe(0xaa);
    }
    // Bytes 50-99: 0xBB (B overwrote A)
    for (let i = 50; i < 100; i++) {
      expect(buf[i]).toBe(0xbb);
    }
    // Bytes 100-149: 0xBB (only B wrote here)
    for (let i = 100; i < 150; i++) {
      expect(buf[i]).toBe(0xbb);
    }

    FS.close(a);
    FS.close(b);
  });

  // ------------------------------------------------------------------
  // One stream appends while another reads sequentially
  // ------------------------------------------------------------------

  it("append while reading: reader sees growing file", () => {
    const { FS } = h;

    // Create file with initial data
    const w = FS.open("/growing", O.RDWR | O.CREAT, 0o666);
    const initial = encode("initial");
    FS.write(w, initial, 0, initial.length, 0);

    const r = FS.open("/growing", O.RDONLY);

    // Read initial data
    const buf1 = new Uint8Array(7);
    const n1 = FS.read(r, buf1, 0, 7, 0);
    expect(n1).toBe(7);
    expect(decode(buf1, 7)).toBe("initial");

    // Append through writer
    const appended = encode("_appended");
    FS.write(w, appended, 0, appended.length, 7);

    // Reader should see the new data
    const buf2 = new Uint8Array(9);
    const n2 = FS.read(r, buf2, 0, 9, 7);
    expect(n2).toBe(9);
    expect(decode(buf2, 9)).toBe("_appended");

    FS.close(w);
    FS.close(r);
  });

  // ------------------------------------------------------------------
  // Close one stream while another keeps reading
  // ------------------------------------------------------------------

  it("close one stream, other stream unaffected", () => {
    const { FS } = h;
    const data = encode("shared data across streams");
    const w = FS.open("/closeable", O.RDWR | O.CREAT, 0o666);
    FS.write(w, data, 0, data.length, 0);

    const r1 = FS.open("/closeable", O.RDONLY);
    const r2 = FS.open("/closeable", O.RDONLY);

    // Close r1
    FS.close(r1);

    // r2 should still work fine
    const buf = new Uint8Array(data.length);
    const n = FS.read(r2, buf, 0, data.length, 0);
    expect(n).toBe(data.length);
    expect(decode(buf, n)).toBe("shared data across streams");

    FS.close(r2);
    FS.close(w);
  });

  // ------------------------------------------------------------------
  // Multiple streams competing for cache with small working set
  // ------------------------------------------------------------------

  it("three streams on different files with overlapping page access", () => {
    const { FS } = h;

    // Create three multi-page files
    const files = ["/alpha", "/beta", "/gamma"];
    for (let f = 0; f < 3; f++) {
      const s = FS.open(files[f], O.RDWR | O.CREAT, 0o666);
      const data = new Uint8Array(PAGE_SIZE * 4);
      data.fill(0x10 * (f + 1));
      FS.write(s, data, 0, data.length, 0);
      FS.close(s);
    }

    // Open all three for reading and interleave reads
    const streams = files.map((f) => FS.open(f, O.RDONLY));

    for (let page = 0; page < 4; page++) {
      for (let f = 0; f < 3; f++) {
        const buf = new Uint8Array(PAGE_SIZE);
        const n = FS.read(streams[f], buf, 0, PAGE_SIZE, page * PAGE_SIZE);
        expect(n).toBe(PAGE_SIZE);
        const expected = 0x10 * (f + 1);
        for (let i = 0; i < PAGE_SIZE; i++) {
          if (buf[i] !== expected) {
            throw new Error(
              `File ${files[f]} page ${page} byte ${i}: expected ${expected}, got ${buf[i]}`,
            );
          }
        }
      }
    }

    streams.forEach((s) => FS.close(s));
  });
});
