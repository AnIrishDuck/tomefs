/**
 * Adversarial tests: Rename with open file descriptors.
 *
 * Postgres renames files while they're open (WAL segment rotation, temp file
 * promotion). POSIX requires open fds to remain valid after rename — reads
 * and writes through the old fd must use the renamed file's data.
 *
 * These tests target the interaction between stream_ops (which reference
 * node.storagePath) and the rename path (which updates storagePath and
 * re-keys page cache entries). A bug here means data corruption or loss
 * in database workloads.
 *
 * Ethos §9: "Write tests designed to break tomefs specifically — things
 * that pass against MEMFS but expose real bugs in the page cache layer."
 */
import {
  createFS,
  encode,
  decode,
  O,
  type FSHarness,
} from "../harness/emscripten-fs.js";

const PAGE_SIZE = 8192;

describe("adversarial: rename with open file descriptors", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  // ------------------------------------------------------------------
  // Basic: read through fd after rename
  // ------------------------------------------------------------------

  it("read through open fd after rename returns correct data @fast", () => {
    const { FS } = h;

    const data = encode("hello from before rename");
    const fd = FS.open("/file", O.RDWR | O.CREAT, 0o666);
    FS.write(fd, data, 0, data.length, 0);

    // Rename while fd is still open
    FS.rename("/file", "/renamed");

    // Read through the original fd
    const buf = new Uint8Array(data.length);
    const n = FS.read(fd, buf, 0, data.length, 0);
    expect(decode(buf, n)).toBe("hello from before rename");
    FS.close(fd);
  });

  // ------------------------------------------------------------------
  // Write through fd after rename
  // ------------------------------------------------------------------

  it("write through open fd after rename persists under new name", () => {
    const { FS } = h;

    const fd = FS.open("/original", O.RDWR | O.CREAT, 0o666);
    const before = encode("before");
    FS.write(fd, before, 0, before.length, 0);

    FS.rename("/original", "/moved");

    // Write more data through the old fd
    const after = encode(" after");
    FS.write(fd, after, 0, after.length, before.length);
    FS.close(fd);

    // Verify the full content is under the new name
    const rd = FS.open("/moved", O.RDONLY);
    const buf = new Uint8Array(50);
    const n = FS.read(rd, buf, 0, 50, 0);
    expect(decode(buf, n)).toBe("before after");
    FS.close(rd);
  });

  // ------------------------------------------------------------------
  // Dirty pages in cache during rename
  // ------------------------------------------------------------------

  it("dirty pages written through fd before rename are preserved", () => {
    const { FS } = h;

    // Write multi-page data without closing (pages are dirty in cache)
    const fd = FS.open("/dirty", O.RDWR | O.CREAT, 0o666);
    const data = new Uint8Array(PAGE_SIZE * 3);
    for (let i = 0; i < data.length; i++) data[i] = (i * 13) & 0xff;
    FS.write(fd, data, 0, data.length, 0);

    // Rename with dirty pages still in cache
    FS.rename("/dirty", "/clean");

    // Read back through same fd — data must match
    const buf = new Uint8Array(data.length);
    const n = FS.read(fd, buf, 0, buf.length, 0);
    expect(n).toBe(data.length);
    for (let i = 0; i < data.length; i++) {
      if (buf[i] !== data[i]) {
        throw new Error(`Byte ${i}: expected ${data[i]}, got ${buf[i]}`);
      }
    }
    FS.close(fd);
  });

  // ------------------------------------------------------------------
  // Rename then create new file at old path, verify fd isolation
  // ------------------------------------------------------------------

  it("fd still references renamed file when new file created at old path", () => {
    const { FS } = h;

    const fd = FS.open("/slot", O.RDWR | O.CREAT, 0o666);
    const original = encode("original content");
    FS.write(fd, original, 0, original.length, 0);

    FS.rename("/slot", "/archive");

    // Create a completely different file at /slot
    const fd2 = FS.open("/slot", O.RDWR | O.CREAT, 0o666);
    const replacement = encode("replacement content");
    FS.write(fd2, replacement, 0, replacement.length, 0);
    FS.close(fd2);

    // Old fd should still read the original (now at /archive)
    const buf = new Uint8Array(50);
    const n = FS.read(fd, buf, 0, 50, 0);
    expect(decode(buf, n)).toBe("original content");
    FS.close(fd);
  });

  // ------------------------------------------------------------------
  // Multiple fds open to same file, then rename
  // ------------------------------------------------------------------

  it("multiple open fds all see data after rename", () => {
    const { FS } = h;

    const data = encode("shared data across fds");
    const writer = FS.open("/shared", O.RDWR | O.CREAT, 0o666);
    FS.write(writer, data, 0, data.length, 0);

    // Open a second reader fd before rename
    const reader = FS.open("/shared", O.RDONLY);

    FS.rename("/shared", "/moved");

    // Both fds should read the same data
    const buf1 = new Uint8Array(50);
    const n1 = FS.read(writer, buf1, 0, 50, 0);
    expect(decode(buf1, n1)).toBe("shared data across fds");

    const buf2 = new Uint8Array(50);
    const n2 = FS.read(reader, buf2, 0, 50, 0);
    expect(decode(buf2, n2)).toBe("shared data across fds");

    FS.close(writer);
    FS.close(reader);
  });

  // ------------------------------------------------------------------
  // Write through fd1, read through fd2 after rename
  // ------------------------------------------------------------------

  it("write through one fd visible through another fd after rename", () => {
    const { FS } = h;

    const fd1 = FS.open("/file", O.RDWR | O.CREAT, 0o666);
    const fd2 = FS.open("/file", O.RDWR);

    const initial = encode("initial");
    FS.write(fd1, initial, 0, initial.length, 0);

    FS.rename("/file", "/newname");

    // Write more through fd1
    const extra = encode("-extra");
    FS.write(fd1, extra, 0, extra.length, initial.length);

    // Read through fd2 — should see the combined data
    const buf = new Uint8Array(50);
    const n = FS.read(fd2, buf, 0, 50, 0);
    expect(decode(buf, n)).toBe("initial-extra");

    FS.close(fd1);
    FS.close(fd2);
  });

  // ------------------------------------------------------------------
  // Rename chain with open fd: A -> B -> C
  // ------------------------------------------------------------------

  it("fd remains valid through chain of renames", () => {
    const { FS } = h;

    const fd = FS.open("/hop0", O.RDWR | O.CREAT, 0o666);
    const data = encode("traveling with open fd");
    FS.write(fd, data, 0, data.length, 0);

    FS.rename("/hop0", "/hop1");
    FS.rename("/hop1", "/hop2");
    FS.rename("/hop2", "/hop3");

    // fd should still work
    const buf = new Uint8Array(50);
    const n = FS.read(fd, buf, 0, 50, 0);
    expect(decode(buf, n)).toBe("traveling with open fd");

    // Write more through the fd
    const more = encode("!!!");
    FS.write(fd, more, 0, more.length, data.length);
    FS.close(fd);

    // Verify final content under /hop3
    const rd = FS.open("/hop3", O.RDONLY);
    const buf2 = new Uint8Array(50);
    const n2 = FS.read(rd, buf2, 0, 50, 0);
    expect(decode(buf2, n2)).toBe("traveling with open fd!!!");
    FS.close(rd);
  });

  // ------------------------------------------------------------------
  // Rename across directories with open fd
  // ------------------------------------------------------------------

  it("fd valid after rename across directories", () => {
    const { FS } = h;
    FS.mkdir("/src");
    FS.mkdir("/dst");

    const fd = FS.open("/src/file", O.RDWR | O.CREAT, 0o666);
    const data = new Uint8Array(PAGE_SIZE * 2 + 100);
    for (let i = 0; i < data.length; i++) data[i] = (i * 41) & 0xff;
    FS.write(fd, data, 0, data.length, 0);

    FS.rename("/src/file", "/dst/file");

    // Read back through original fd
    const buf = new Uint8Array(data.length);
    const n = FS.read(fd, buf, 0, buf.length, 0);
    expect(n).toBe(data.length);
    for (let i = 0; i < data.length; i++) {
      if (buf[i] !== data[i]) {
        throw new Error(`Byte ${i}: expected ${data[i]}, got ${buf[i]}`);
      }
    }
    FS.close(fd);
  });

  // ------------------------------------------------------------------
  // Directory rename with open fd to child file
  // ------------------------------------------------------------------

  it("open fd to child file survives parent directory rename", () => {
    const { FS } = h;
    FS.mkdir("/parent");

    const fd = FS.open("/parent/child.dat", O.RDWR | O.CREAT, 0o666);
    const data = encode("child file data");
    FS.write(fd, data, 0, data.length, 0);

    // Rename the parent directory
    FS.rename("/parent", "/renamed_parent");

    // fd should still work
    const buf = new Uint8Array(50);
    const n = FS.read(fd, buf, 0, 50, 0);
    expect(decode(buf, n)).toBe("child file data");

    // Write more through the fd
    const more = encode(" plus more");
    FS.write(fd, more, 0, more.length, data.length);
    FS.close(fd);

    // Verify through the new path
    const rd = FS.open("/renamed_parent/child.dat", O.RDONLY);
    const buf2 = new Uint8Array(50);
    const n2 = FS.read(rd, buf2, 0, 50, 0);
    expect(decode(buf2, n2)).toBe("child file data plus more");
    FS.close(rd);
  });

  // ------------------------------------------------------------------
  // WAL rotation pattern: open, append, rename, open new, append
  // ------------------------------------------------------------------

  it("WAL rotation: append → rename → new file → append cycle", () => {
    const { FS } = h;

    // Simulate WAL segment rotation (Postgres pattern)
    for (let seg = 0; seg < 3; seg++) {
      const walPath = "/wal";
      const archivePath = `/wal.${seg}`;

      // Open WAL for appending
      const fd = FS.open(walPath, O.RDWR | O.CREAT, 0o666);
      const record = encode(`WAL record segment ${seg}`);
      FS.write(fd, record, 0, record.length, 0);

      // "Rotate": rename current WAL to archive
      FS.rename(walPath, archivePath);

      // fd still references the archived segment — verify
      const buf = new Uint8Array(50);
      const n = FS.read(fd, buf, 0, 50, 0);
      expect(decode(buf, n)).toBe(`WAL record segment ${seg}`);
      FS.close(fd);

      // Verify archived file via path
      const rd = FS.open(archivePath, O.RDONLY);
      const buf2 = new Uint8Array(50);
      const n2 = FS.read(rd, buf2, 0, 50, 0);
      expect(decode(buf2, n2)).toBe(`WAL record segment ${seg}`);
      FS.close(rd);
    }
  });

  // ------------------------------------------------------------------
  // Seek position preserved after rename
  // ------------------------------------------------------------------

  it("seek position maintained through rename", () => {
    const { FS } = h;

    const fd = FS.open("/seektest", O.RDWR | O.CREAT, 0o666);
    const data = encode("abcdefghijklmnop");
    FS.write(fd, data, 0, data.length, 0);

    // Seek to middle of file
    FS.llseek(fd, 8, 0); // SEEK_SET to offset 8

    FS.rename("/seektest", "/seektest_renamed");

    // Read from current position (should be at offset 8)
    const buf = new Uint8Array(8);
    const n = FS.read(fd, buf, 0, 8);
    expect(decode(buf, n)).toBe("ijklmnop");
    FS.close(fd);
  });

  // ------------------------------------------------------------------
  // Rename over open fd's file from another file
  // ------------------------------------------------------------------

  it("fd to target file becomes invalid after rename-over", () => {
    const { FS } = h;

    // Create target and source
    const target_fd = FS.open("/target", O.RDWR | O.CREAT, 0o666);
    const target_data = encode("target data");
    FS.write(target_fd, target_data, 0, target_data.length, 0);

    const src = FS.open("/source", O.RDWR | O.CREAT, 0o666);
    const src_data = encode("source data");
    FS.write(src, src_data, 0, src_data.length, 0);
    FS.close(src);

    // Rename source over target (target is unlinked)
    FS.rename("/source", "/target");

    // The old target_fd references an unlinked node.
    // POSIX says the fd remains valid for the old inode's data.
    // Read through the old target fd — should still see "target data"
    const buf = new Uint8Array(50);
    const n = FS.read(target_fd, buf, 0, 50, 0);
    expect(decode(buf, n)).toBe("target data");
    FS.close(target_fd);

    // Reading /target via path should give source data
    const rd = FS.open("/target", O.RDONLY);
    const buf2 = new Uint8Array(50);
    const n2 = FS.read(rd, buf2, 0, 50, 0);
    expect(decode(buf2, n2)).toBe("source data");
    FS.close(rd);
  });

  // ------------------------------------------------------------------
  // Multi-page write through fd after rename under cache pressure
  // ------------------------------------------------------------------

  it("multi-page write through fd after rename under cache pressure", () => {
    const { FS } = h;

    // Write initial data
    const fd = FS.open("/pressure", O.RDWR | O.CREAT, 0o666);
    const initial = new Uint8Array(PAGE_SIZE * 2);
    for (let i = 0; i < initial.length; i++) initial[i] = 0xaa;
    FS.write(fd, initial, 0, initial.length, 0);

    FS.rename("/pressure", "/pressured");

    // Write more pages through old fd (may trigger eviction in tomefs)
    const extra = new Uint8Array(PAGE_SIZE * 3);
    for (let i = 0; i < extra.length; i++) extra[i] = 0xbb;
    FS.write(fd, extra, 0, extra.length, initial.length);

    // Read back everything through the fd
    const total = initial.length + extra.length;
    const buf = new Uint8Array(total);
    const n = FS.read(fd, buf, 0, total, 0);
    expect(n).toBe(total);

    // Verify first 2 pages are 0xAA
    for (let i = 0; i < initial.length; i++) {
      if (buf[i] !== 0xaa) {
        throw new Error(`Byte ${i}: expected 0xAA, got 0x${buf[i].toString(16)}`);
      }
    }
    // Verify next 3 pages are 0xBB
    for (let i = 0; i < extra.length; i++) {
      if (buf[initial.length + i] !== 0xbb) {
        throw new Error(
          `Byte ${initial.length + i}: expected 0xBB, got 0x${buf[initial.length + i].toString(16)}`,
        );
      }
    }
    FS.close(fd);
  });

  // ------------------------------------------------------------------
  // Truncate through fd after rename
  // ------------------------------------------------------------------

  it("truncate through fd after rename works correctly", () => {
    const { FS } = h;

    const fd = FS.open("/trunc", O.RDWR | O.CREAT, 0o666);
    const data = new Uint8Array(PAGE_SIZE * 3);
    data.fill(0xff);
    FS.write(fd, data, 0, data.length, 0);

    FS.rename("/trunc", "/trunc_renamed");

    // Truncate through the new path (fd's node is the same)
    FS.truncate("/trunc_renamed", PAGE_SIZE);

    // Read through old fd — should only get 1 page
    const buf = new Uint8Array(PAGE_SIZE * 3);
    const n = FS.read(fd, buf, 0, buf.length, 0);
    expect(n).toBe(PAGE_SIZE);
    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(buf[i]).toBe(0xff);
    }
    FS.close(fd);
  });

  // ------------------------------------------------------------------
  // Close fd after rename flushes to correct path
  // ------------------------------------------------------------------

  it("close after rename flushes data to new path", () => {
    const { FS } = h;

    const fd = FS.open("/flush_me", O.RDWR | O.CREAT, 0o666);
    const data = encode("persist this through rename");
    FS.write(fd, data, 0, data.length, 0);

    FS.rename("/flush_me", "/flushed");

    // Write more through old fd
    const more = encode("!!!");
    FS.write(fd, more, 0, more.length, data.length);

    // Close triggers flush
    FS.close(fd);

    // Read through path — should have all data
    const rd = FS.open("/flushed", O.RDONLY);
    const buf = new Uint8Array(50);
    const n = FS.read(rd, buf, 0, 50, 0);
    expect(decode(buf, n)).toBe("persist this through rename!!!");
    FS.close(rd);
  });
});
