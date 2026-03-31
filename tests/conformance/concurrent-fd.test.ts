/**
 * Conformance tests for concurrent file descriptor semantics.
 *
 * Tests that independently opened file descriptors to the same file
 * see a coherent view of file data — writes through one fd are
 * immediately visible through reads on another fd. This is critical
 * for Postgres, where bgwriter, checkpointer, and backends all open
 * the same heap/index files through independent fds.
 *
 * Also tests positional I/O (pread/pwrite semantics) via the explicit
 * `position` parameter in FS.read/FS.write. Postgres uses pread/pwrite
 * extensively for random page access without maintaining stream position.
 *
 * Ethos §2: Real POSIX semantics
 * Ethos §8: New conformance test sources beyond the Emscripten suite
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
import { PAGE_SIZE } from "../../src/types.js";

describe("concurrent fd semantics", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  describe("cross-fd write visibility", () => {
    it("write on one fd is immediately visible via read on another fd @fast", () => {
      const { FS } = h;

      // Open two independent fds to the same file
      const writer = FS.open("/shared", O.RDWR | O.CREAT, 0o666);
      const reader = FS.open("/shared", O.RDONLY);

      // Write through writer
      FS.write(writer, encode("hello"), 0, 5);

      // Read through reader — should see the write immediately
      const buf = new Uint8Array(10);
      const n = FS.read(reader, buf, 0, 10);
      expect(n).toBe(5);
      expect(decode(buf, n)).toBe("hello");

      FS.close(writer);
      FS.close(reader);
    });

    it("overwrite on one fd is visible on another fd @fast", () => {
      const { FS } = h;

      // Create file with initial content
      const w = FS.open("/overwrite", O.RDWR | O.CREAT, 0o666);
      FS.write(w, encode("AAAAAAAAAA"), 0, 10);
      FS.close(w);

      // Open two independent fds
      const fd1 = FS.open("/overwrite", O.RDWR);
      const fd2 = FS.open("/overwrite", O.RDONLY);

      // Overwrite middle 4 bytes through fd1
      FS.write(fd1, encode("BBBB"), 0, 4, 3);

      // Read all through fd2 — should see the overwrite
      const buf = new Uint8Array(10);
      const n = FS.read(fd2, buf, 0, 10);
      expect(n).toBe(10);
      expect(decode(buf, n)).toBe("AAABBBBAAA");

      FS.close(fd1);
      FS.close(fd2);
    });

    it("independent fds have independent positions @fast", () => {
      const { FS } = h;

      // Create file with content
      FS.writeFile("/indep", "abcdefghij");

      const fd1 = FS.open("/indep", O.RDONLY);
      const fd2 = FS.open("/indep", O.RDONLY);

      // Advance fd1 to position 5
      FS.llseek(fd1, 5, SEEK_SET);

      // fd2 should still be at 0
      const buf = new Uint8Array(3);
      FS.read(fd2, buf, 0, 3);
      expect(decode(buf)).toBe("abc");

      // fd1 should read from position 5
      FS.read(fd1, buf, 0, 3);
      expect(decode(buf)).toBe("fgh");

      FS.close(fd1);
      FS.close(fd2);
    });

    it("alternating writes from two fds produce correct interleaved content", () => {
      const { FS } = h;

      const fd1 = FS.open("/interleave", O.RDWR | O.CREAT, 0o666);
      const fd2 = FS.open("/interleave", O.RDWR);

      // fd1 writes at position 0
      FS.write(fd1, encode("AA"), 0, 2, 0);
      // fd2 writes at position 2
      FS.write(fd2, encode("BB"), 0, 2, 2);
      // fd1 writes at position 4
      FS.write(fd1, encode("CC"), 0, 2, 4);
      // fd2 writes at position 6
      FS.write(fd2, encode("DD"), 0, 2, 6);

      // Read all 8 bytes through a fresh fd
      const reader = FS.open("/interleave", O.RDONLY);
      const buf = new Uint8Array(8);
      const n = FS.read(reader, buf, 0, 8);
      expect(n).toBe(8);
      expect(decode(buf, n)).toBe("AABBCCDD");

      FS.close(fd1);
      FS.close(fd2);
      FS.close(reader);
    });

    it("write extending file through one fd is visible as new size on another fd", () => {
      const { FS } = h;

      const fd1 = FS.open("/extend", O.RDWR | O.CREAT, 0o666);
      const fd2 = FS.open("/extend", O.RDONLY);

      // Write 5 bytes through fd1
      FS.write(fd1, encode("hello"), 0, 5);
      expect(FS.fstat(fd2.fd).size).toBe(5);

      // Extend to 10 bytes
      FS.write(fd1, encode("world"), 0, 5);
      expect(FS.fstat(fd2.fd).size).toBe(10);

      // Read all 10 bytes through fd2
      const buf = new Uint8Array(10);
      const n = FS.read(fd2, buf, 0, 10);
      expect(n).toBe(10);
      expect(decode(buf, n)).toBe("helloworld");

      FS.close(fd1);
      FS.close(fd2);
    });

    it("truncate through one fd is visible on another fd", () => {
      const { FS } = h;

      // Create 20-byte file
      const fd1 = FS.open("/trunc_shared", O.RDWR | O.CREAT, 0o666);
      FS.write(fd1, encode("12345678901234567890"), 0, 20);

      const fd2 = FS.open("/trunc_shared", O.RDONLY);
      expect(FS.fstat(fd2.fd).size).toBe(20);

      // Truncate through ftruncate on fd1
      FS.ftruncate(fd1.fd, 5);
      expect(FS.fstat(fd2.fd).size).toBe(5);

      // Read through fd2 — should only get 5 bytes
      const buf = new Uint8Array(20);
      const n = FS.read(fd2, buf, 0, 20);
      expect(n).toBe(5);
      expect(decode(buf, n)).toBe("12345");

      FS.close(fd1);
      FS.close(fd2);
    });
  });

  describe("cross-fd visibility across page boundaries", () => {
    it("write spanning page boundary is visible from another fd @fast", () => {
      const { FS } = h;

      const fd1 = FS.open("/page_span", O.RDWR | O.CREAT, 0o666);
      const fd2 = FS.open("/page_span", O.RDONLY);

      // Write data that spans the first page boundary (PAGE_SIZE = 8192)
      const offset = PAGE_SIZE - 4;
      const data = new Uint8Array(8);
      data.fill(0xAA, 0, 4); // last 4 bytes of page 0
      data.fill(0xBB, 4, 8); // first 4 bytes of page 1

      FS.write(fd1, data, 0, 8, offset);

      // Read through fd2 and verify both pages
      const buf = new Uint8Array(8);
      const n = FS.read(fd2, buf, 0, 8, offset);
      expect(n).toBe(8);
      expect(buf[0]).toBe(0xAA);
      expect(buf[3]).toBe(0xAA);
      expect(buf[4]).toBe(0xBB);
      expect(buf[7]).toBe(0xBB);

      FS.close(fd1);
      FS.close(fd2);
    });

    it("writes to different pages from different fds are both visible", () => {
      const { FS } = h;

      const fd1 = FS.open("/multi_page", O.RDWR | O.CREAT, 0o666);
      const fd2 = FS.open("/multi_page", O.RDWR);

      // fd1 writes page 0
      const page0 = new Uint8Array(PAGE_SIZE);
      page0.fill(0x11);
      FS.write(fd1, page0, 0, PAGE_SIZE, 0);

      // fd2 writes page 1
      const page1 = new Uint8Array(PAGE_SIZE);
      page1.fill(0x22);
      FS.write(fd2, page1, 0, PAGE_SIZE, PAGE_SIZE);

      // Read both pages through a third fd
      const reader = FS.open("/multi_page", O.RDONLY);
      const buf = new Uint8Array(PAGE_SIZE * 2);
      const n = FS.read(reader, buf, 0, PAGE_SIZE * 2);
      expect(n).toBe(PAGE_SIZE * 2);

      // Page 0 should be 0x11
      expect(buf[0]).toBe(0x11);
      expect(buf[PAGE_SIZE - 1]).toBe(0x11);
      // Page 1 should be 0x22
      expect(buf[PAGE_SIZE]).toBe(0x22);
      expect(buf[PAGE_SIZE * 2 - 1]).toBe(0x22);

      FS.close(fd1);
      FS.close(fd2);
      FS.close(reader);
    });

    it("overwrite within a single page from one fd, read from another", () => {
      const { FS } = h;

      // Fill file with 2 pages of known data
      const fd1 = FS.open("/page_overwrite", O.RDWR | O.CREAT, 0o666);
      const initial = new Uint8Array(PAGE_SIZE * 2);
      initial.fill(0xAA);
      FS.write(fd1, initial, 0, initial.length);

      const fd2 = FS.open("/page_overwrite", O.RDONLY);

      // Overwrite 100 bytes in the middle of page 1 through fd1
      const patch = new Uint8Array(100);
      patch.fill(0xFF);
      FS.write(fd1, patch, 0, 100, PAGE_SIZE + 500);

      // Read the patched region through fd2
      const buf = new Uint8Array(100);
      const n = FS.read(fd2, buf, 0, 100, PAGE_SIZE + 500);
      expect(n).toBe(100);
      for (let i = 0; i < 100; i++) {
        expect(buf[i]).toBe(0xFF);
      }

      // Surrounding bytes should still be 0xAA
      const before = new Uint8Array(1);
      FS.read(fd2, before, 0, 1, PAGE_SIZE + 499);
      expect(before[0]).toBe(0xAA);

      const after = new Uint8Array(1);
      FS.read(fd2, after, 0, 1, PAGE_SIZE + 600);
      expect(after[0]).toBe(0xAA);

      FS.close(fd1);
      FS.close(fd2);
    });
  });

  describe("positional I/O (pread/pwrite)", () => {
    it("positional write does not advance stream position @fast", () => {
      const { FS } = h;

      const stream = FS.open("/pwrite_pos", O.RDWR | O.CREAT, 0o666);

      // Sequential write advances position to 5
      FS.write(stream, encode("hello"), 0, 5);
      expect(FS.llseek(stream, 0, SEEK_CUR)).toBe(5);

      // Positional write at offset 10 — stream position should not change
      FS.write(stream, encode("world"), 0, 5, 10);
      expect(FS.llseek(stream, 0, SEEK_CUR)).toBe(5);

      FS.close(stream);
    });

    it("positional read does not advance stream position @fast", () => {
      const { FS } = h;

      FS.writeFile("/pread_pos", "abcdefghijklmnop");
      const stream = FS.open("/pread_pos", O.RDONLY);

      // Sequential read advances position to 5
      const buf1 = new Uint8Array(5);
      FS.read(stream, buf1, 0, 5);
      expect(decode(buf1)).toBe("abcde");
      expect(FS.llseek(stream, 0, SEEK_CUR)).toBe(5);

      // Positional read at offset 10 — stream position should not change
      const buf2 = new Uint8Array(5);
      FS.read(stream, buf2, 0, 5, 10);
      expect(decode(buf2)).toBe("klmno");
      expect(FS.llseek(stream, 0, SEEK_CUR)).toBe(5);

      // Next sequential read should continue from position 5
      const buf3 = new Uint8Array(3);
      FS.read(stream, buf3, 0, 3);
      expect(decode(buf3)).toBe("fgh");

      FS.close(stream);
    });

    it("positional read at EOF returns 0 bytes", () => {
      const { FS } = h;

      FS.writeFile("/pread_eof", "short");
      const stream = FS.open("/pread_eof", O.RDONLY);

      const buf = new Uint8Array(10);
      const n = FS.read(stream, buf, 0, 10, 100);
      expect(n).toBe(0);

      FS.close(stream);
    });

    it("positional read returns partial data at end of file", () => {
      const { FS } = h;

      FS.writeFile("/pread_partial", "abcde");
      const stream = FS.open("/pread_partial", O.RDONLY);

      // Read 10 bytes starting at offset 3 — only 2 bytes available
      const buf = new Uint8Array(10);
      const n = FS.read(stream, buf, 0, 10, 3);
      expect(n).toBe(2);
      expect(decode(buf, n)).toBe("de");

      FS.close(stream);
    });

    it("positional write extends file without gap corruption", () => {
      const { FS } = h;

      const stream = FS.open("/pwrite_extend", O.RDWR | O.CREAT, 0o666);

      // Write at offset 0
      FS.write(stream, encode("AAA"), 0, 3, 0);

      // Write at offset 10 — creates a gap filled with zeros
      FS.write(stream, encode("BBB"), 0, 3, 10);

      expect(FS.fstat(stream.fd).size).toBe(13);

      // Read the gap — should be zeros
      const buf = new Uint8Array(13);
      const n = FS.read(stream, buf, 0, 13, 0);
      expect(n).toBe(13);
      expect(decode(buf.subarray(0, 3))).toBe("AAA");
      for (let i = 3; i < 10; i++) {
        expect(buf[i]).toBe(0);
      }
      expect(decode(buf.subarray(10, 13))).toBe("BBB");

      FS.close(stream);
    });

    it("interleaved positional reads and sequential reads", () => {
      const { FS } = h;

      FS.writeFile("/interleave_rw", "0123456789abcdef");
      const stream = FS.open("/interleave_rw", O.RDONLY);
      const buf = new Uint8Array(4);

      // Sequential read: position 0 → 4
      FS.read(stream, buf, 0, 4);
      expect(decode(buf)).toBe("0123");

      // Positional read at 12 — position stays at 4
      FS.read(stream, buf, 0, 4, 12);
      expect(decode(buf)).toBe("cdef");
      expect(FS.llseek(stream, 0, SEEK_CUR)).toBe(4);

      // Sequential read resumes from 4
      FS.read(stream, buf, 0, 4);
      expect(decode(buf)).toBe("4567");

      // Positional read at 0 — position stays at 8
      FS.read(stream, buf, 0, 4, 0);
      expect(decode(buf)).toBe("0123");
      expect(FS.llseek(stream, 0, SEEK_CUR)).toBe(8);

      FS.close(stream);
    });

    it("positional write on one fd, positional read on another @fast", () => {
      const { FS } = h;

      const writer = FS.open("/pwrite_pread", O.RDWR | O.CREAT, 0o666);
      const reader = FS.open("/pwrite_pread", O.RDONLY);

      // Positional writes at various offsets
      FS.write(writer, encode("AAA"), 0, 3, 0);
      FS.write(writer, encode("BBB"), 0, 3, PAGE_SIZE);
      FS.write(writer, encode("CCC"), 0, 3, PAGE_SIZE * 2);

      // Positional reads through reader — verify each write
      const buf = new Uint8Array(3);

      FS.read(reader, buf, 0, 3, 0);
      expect(decode(buf)).toBe("AAA");

      FS.read(reader, buf, 0, 3, PAGE_SIZE);
      expect(decode(buf)).toBe("BBB");

      FS.read(reader, buf, 0, 3, PAGE_SIZE * 2);
      expect(decode(buf)).toBe("CCC");

      // Neither fd's position should have moved from 0
      expect(FS.llseek(writer, 0, SEEK_CUR)).toBe(0);
      expect(FS.llseek(reader, 0, SEEK_CUR)).toBe(0);

      FS.close(writer);
      FS.close(reader);
    });

    it("positional I/O across page boundaries", () => {
      const { FS } = h;

      const stream = FS.open("/pio_boundary", O.RDWR | O.CREAT, 0o666);

      // Write 16 bytes straddling the page boundary
      const offset = PAGE_SIZE - 8;
      const data = encode("0123456789ABCDEF");
      FS.write(stream, data, 0, 16, offset);

      // Positional read of the same region
      const buf = new Uint8Array(16);
      const n = FS.read(stream, buf, 0, 16, offset);
      expect(n).toBe(16);
      expect(decode(buf, n)).toBe("0123456789ABCDEF");

      // Stream position should still be 0
      expect(FS.llseek(stream, 0, SEEK_CUR)).toBe(0);

      FS.close(stream);
    });
  });

  describe("Postgres-realistic patterns", () => {
    it("simulated heap page read-modify-write from independent fds", () => {
      const { FS } = h;

      // Create a "heap file" with 4 pages
      const writer = FS.open("/heap", O.RDWR | O.CREAT, 0o666);
      for (let i = 0; i < 4; i++) {
        const page = new Uint8Array(PAGE_SIZE);
        page.fill(i + 1);
        FS.write(writer, page, 0, PAGE_SIZE, i * PAGE_SIZE);
      }
      FS.close(writer);

      // "Backend 1" reads page 2, modifies it, writes it back
      const be1 = FS.open("/heap", O.RDWR);
      const page2 = new Uint8Array(PAGE_SIZE);
      FS.read(be1, page2, 0, PAGE_SIZE, 2 * PAGE_SIZE);
      expect(page2[0]).toBe(3); // originally filled with 3
      page2[0] = 0xFF;
      page2[100] = 0xFE;
      FS.write(be1, page2, 0, PAGE_SIZE, 2 * PAGE_SIZE);

      // "Backend 2" reads page 2 — should see Backend 1's modification
      const be2 = FS.open("/heap", O.RDONLY);
      const verify = new Uint8Array(PAGE_SIZE);
      FS.read(be2, verify, 0, PAGE_SIZE, 2 * PAGE_SIZE);
      expect(verify[0]).toBe(0xFF);
      expect(verify[100]).toBe(0xFE);
      expect(verify[1]).toBe(3); // rest unchanged

      // Other pages should be unaffected
      const page0 = new Uint8Array(PAGE_SIZE);
      FS.read(be2, page0, 0, PAGE_SIZE, 0);
      expect(page0[0]).toBe(1);

      FS.close(be1);
      FS.close(be2);
    });

    it("sequential scan while another fd writes (bgwriter pattern) @fast", () => {
      const { FS } = h;

      // Create a 4-page file
      const init = FS.open("/scan_write", O.RDWR | O.CREAT, 0o666);
      const initData = new Uint8Array(PAGE_SIZE * 4);
      initData.fill(0x01);
      FS.write(init, initData, 0, initData.length);
      FS.close(init);

      // Scanner reads sequentially
      const scanner = FS.open("/scan_write", O.RDONLY);
      // Writer modifies pages
      const bgwriter = FS.open("/scan_write", O.RDWR);

      // Scanner reads page 0
      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(scanner, buf, 0, PAGE_SIZE);
      expect(buf[0]).toBe(0x01);

      // Bgwriter modifies page 2 (ahead of scanner)
      const patch = new Uint8Array(PAGE_SIZE);
      patch.fill(0xFF);
      FS.write(bgwriter, patch, 0, PAGE_SIZE, 2 * PAGE_SIZE);

      // Scanner continues — reads page 1 (unmodified)
      FS.read(scanner, buf, 0, PAGE_SIZE);
      expect(buf[0]).toBe(0x01);

      // Scanner reads page 2 — should see bgwriter's modification
      FS.read(scanner, buf, 0, PAGE_SIZE);
      expect(buf[0]).toBe(0xFF);

      // Scanner reads page 3 (unmodified)
      FS.read(scanner, buf, 0, PAGE_SIZE);
      expect(buf[0]).toBe(0x01);

      FS.close(scanner);
      FS.close(bgwriter);
    });

    it("WAL append while reader scans earlier pages", () => {
      const { FS } = h;

      // Initial WAL with 2 pages
      const writer = FS.open("/wal", O.RDWR | O.CREAT, 0o666);
      const page = new Uint8Array(PAGE_SIZE);
      page.fill(0x01);
      FS.write(writer, page, 0, PAGE_SIZE, 0);
      page.fill(0x02);
      FS.write(writer, page, 0, PAGE_SIZE, PAGE_SIZE);

      // Reader opens and starts reading
      const reader = FS.open("/wal", O.RDONLY);
      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(reader, buf, 0, PAGE_SIZE, 0);
      expect(buf[0]).toBe(0x01);

      // Writer appends page 2 and page 3
      page.fill(0x03);
      FS.write(writer, page, 0, PAGE_SIZE, PAGE_SIZE * 2);
      page.fill(0x04);
      FS.write(writer, page, 0, PAGE_SIZE, PAGE_SIZE * 3);

      // Reader should see the new file size
      expect(FS.fstat(reader.fd).size).toBe(PAGE_SIZE * 4);

      // Reader reads the newly appended pages
      FS.read(reader, buf, 0, PAGE_SIZE, PAGE_SIZE * 2);
      expect(buf[0]).toBe(0x03);
      FS.read(reader, buf, 0, PAGE_SIZE, PAGE_SIZE * 3);
      expect(buf[0]).toBe(0x04);

      FS.close(writer);
      FS.close(reader);
    });
  });
});
