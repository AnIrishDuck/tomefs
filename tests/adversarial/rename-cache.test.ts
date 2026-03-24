/**
 * Adversarial differential tests: Rename and cache key consistency.
 *
 * Rename is dangerous for a page cache because the cache keys include
 * the file path. After rename, stale cache entries under the old path
 * must never be served, and the new path must see the correct data.
 */
import {
  createFS,
  encode,
  decode,
  O,
  type FSHarness,
} from "../harness/emscripten-fs.js";

const PAGE_SIZE = 8192;

describe("adversarial: rename cache consistency", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  // ------------------------------------------------------------------
  // Basic rename: data accessible under new name
  // ------------------------------------------------------------------

  it("data accessible under new name after rename @fast", () => {
    const { FS } = h;

    // Write multi-page file
    const data = new Uint8Array(PAGE_SIZE * 3);
    for (let i = 0; i < data.length; i++) {
      data[i] = (i * 7) & 0xff;
    }
    const s = FS.open("/original", O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length, 0);
    FS.close(s);

    // Rename
    FS.rename("/original", "/renamed");

    // Read under new name
    const r = FS.open("/renamed", O.RDONLY);
    const buf = new Uint8Array(PAGE_SIZE * 3);
    const n = FS.read(r, buf, 0, buf.length, 0);
    expect(n).toBe(PAGE_SIZE * 3);
    for (let i = 0; i < data.length; i++) {
      expect(buf[i]).toBe(data[i]);
    }
    FS.close(r);
  });

  // ------------------------------------------------------------------
  // Rename then write: new data under new path
  // ------------------------------------------------------------------

  it("write after rename uses new path correctly", () => {
    const { FS } = h;

    const s = FS.open("/before", O.RDWR | O.CREAT, 0o666);
    const first = encode("before rename");
    FS.write(s, first, 0, first.length, 0);
    FS.close(s);

    FS.rename("/before", "/after");

    // Write more data under new name
    const s2 = FS.open("/after", O.RDWR);
    const more = encode(" - and after rename");
    FS.write(s2, more, 0, more.length, first.length);
    FS.close(s2);

    // Read back full content
    const r = FS.open("/after", O.RDONLY);
    const buf = new Uint8Array(100);
    const n = FS.read(r, buf, 0, 100, 0);
    expect(decode(buf, n)).toBe("before rename - and after rename");
    FS.close(r);
  });

  // ------------------------------------------------------------------
  // Rename over existing file: old target data gone
  // ------------------------------------------------------------------

  it("rename over existing file replaces data completely", () => {
    const { FS } = h;

    // Create target with distinct data
    const target = FS.open("/target", O.RDWR | O.CREAT, 0o666);
    const targetData = new Uint8Array(PAGE_SIZE * 2);
    targetData.fill(0xaa);
    FS.write(target, targetData, 0, targetData.length, 0);
    FS.close(target);

    // Create source with different data
    const source = FS.open("/source", O.RDWR | O.CREAT, 0o666);
    const sourceData = new Uint8Array(PAGE_SIZE);
    sourceData.fill(0xbb);
    FS.write(source, sourceData, 0, sourceData.length, 0);
    FS.close(source);

    // Rename source over target
    FS.rename("/source", "/target");

    // Read /target — should have source's data (1 page of 0xBB), not target's
    const r = FS.open("/target", O.RDONLY);
    const stat = FS.stat("/target");
    expect(stat.size).toBe(PAGE_SIZE); // source was 1 page, not 2

    const buf = new Uint8Array(PAGE_SIZE);
    const n = FS.read(r, buf, 0, PAGE_SIZE, 0);
    expect(n).toBe(PAGE_SIZE);
    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(buf[i]).toBe(0xbb);
    }
    FS.close(r);
  });

  // ------------------------------------------------------------------
  // Rename chain: A -> B -> C
  // ------------------------------------------------------------------

  it("chained renames preserve data through multiple hops", () => {
    const { FS } = h;

    const data = encode("traveling data");
    const s = FS.open("/hop0", O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length, 0);
    FS.close(s);

    FS.rename("/hop0", "/hop1");
    FS.rename("/hop1", "/hop2");
    FS.rename("/hop2", "/hop3");

    const r = FS.open("/hop3", O.RDONLY);
    const buf = new Uint8Array(data.length);
    const n = FS.read(r, buf, 0, data.length, 0);
    expect(decode(buf, n)).toBe("traveling data");
    FS.close(r);
  });

  // ------------------------------------------------------------------
  // Rename then create new file at old path
  // ------------------------------------------------------------------

  it("new file at old path after rename is independent", () => {
    const { FS } = h;

    // Create and write to /slot
    const s1 = FS.open("/slot", O.RDWR | O.CREAT, 0o666);
    const first = new Uint8Array(PAGE_SIZE);
    first.fill(0x11);
    FS.write(s1, first, 0, PAGE_SIZE, 0);
    FS.close(s1);

    // Rename /slot -> /archive
    FS.rename("/slot", "/archive");

    // Create a NEW /slot with different data
    const s2 = FS.open("/slot", O.RDWR | O.CREAT, 0o666);
    const second = new Uint8Array(PAGE_SIZE);
    second.fill(0x22);
    FS.write(s2, second, 0, PAGE_SIZE, 0);
    FS.close(s2);

    // /archive should have the old data
    const rArchive = FS.open("/archive", O.RDONLY);
    const bufA = new Uint8Array(PAGE_SIZE);
    FS.read(rArchive, bufA, 0, PAGE_SIZE, 0);
    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(bufA[i]).toBe(0x11);
    }
    FS.close(rArchive);

    // /slot should have the new data
    const rSlot = FS.open("/slot", O.RDONLY);
    const bufS = new Uint8Array(PAGE_SIZE);
    FS.read(rSlot, bufS, 0, PAGE_SIZE, 0);
    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(bufS[i]).toBe(0x22);
    }
    FS.close(rSlot);
  });

  // ------------------------------------------------------------------
  // Rename between directories
  // ------------------------------------------------------------------

  it("rename across directories preserves multi-page data", () => {
    const { FS } = h;
    FS.mkdir("/dir_a");
    FS.mkdir("/dir_b");

    // Write multi-page file in dir_a
    const s = FS.open("/dir_a/bigfile", O.RDWR | O.CREAT, 0o666);
    const data = new Uint8Array(PAGE_SIZE * 5);
    for (let i = 0; i < data.length; i++) {
      data[i] = (i * 23 + 1) & 0xff;
    }
    FS.write(s, data, 0, data.length, 0);
    FS.close(s);

    // Rename to dir_b
    FS.rename("/dir_a/bigfile", "/dir_b/bigfile");

    // Verify data integrity
    const r = FS.open("/dir_b/bigfile", O.RDONLY);
    const buf = new Uint8Array(data.length);
    const n = FS.read(r, buf, 0, buf.length, 0);
    expect(n).toBe(data.length);
    for (let i = 0; i < data.length; i++) {
      if (buf[i] !== data[i]) {
        throw new Error(
          `Byte ${i}: expected ${data[i]}, got ${buf[i]}`,
        );
      }
    }
    FS.close(r);
  });

  // ------------------------------------------------------------------
  // Safe-write pattern: write tmp, rename over original
  // ------------------------------------------------------------------

  it("safe-write pattern (write tmp + rename) multiple iterations", () => {
    const { FS } = h;

    // Create initial file
    const s = FS.open("/data", O.RDWR | O.CREAT, 0o666);
    const v0 = encode("version 0");
    FS.write(s, v0, 0, v0.length, 0);
    FS.close(s);

    for (let version = 1; version <= 5; version++) {
      // Write new version to tmp
      const tmp = FS.open("/data.tmp", O.RDWR | O.CREAT | O.TRUNC, 0o666);
      const content = encode(`version ${version}`);
      FS.write(tmp, content, 0, content.length, 0);
      FS.close(tmp);

      // Atomic replace
      FS.rename("/data.tmp", "/data");

      // Verify
      const r = FS.open("/data", O.RDONLY);
      const buf = new Uint8Array(50);
      const n = FS.read(r, buf, 0, 50, 0);
      expect(decode(buf, n)).toBe(`version ${version}`);
      FS.close(r);
    }
  });
});
