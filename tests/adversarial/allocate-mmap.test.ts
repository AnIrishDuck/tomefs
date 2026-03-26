/**
 * Adversarial tests: allocate() and mmap() edge cases (tomefs only).
 *
 * allocate() (posix_fallocate) pre-allocates file space by growing the file
 * to max(usedBytes, offset+length). This is used by PostgreSQL's WAL and
 * relation extension code. MEMFS does not expose allocate as a stream_op.
 *
 * mmap() in tomefs returns a fresh Uint8Array copy (no mmapAlloc needed),
 * unlike MEMFS which requires the native emscripten_builtin_memalign symbol.
 * These tests exercise tomefs's mmap directly via stream_ops.
 *
 * Both test suites require TOMEFS_BACKEND=tomefs.
 */
import {
  createFS,
  encode,
  decode,
  O,
  type FSHarness,
} from "../harness/emscripten-fs.js";

const PAGE_SIZE = 8192;

const describeIfTomefs =
  process.env.TOMEFS_BACKEND === "tomefs" ? describe : describe.skip;

describeIfTomefs("adversarial: allocate() stream_ops", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  it("allocate extends an empty file @fast", () => {
    const { FS } = h;
    const stream = FS.open("/alloc-empty", O.RDWR | O.CREAT, 0o666);

    stream.stream_ops.allocate(stream, 0, 1024);

    const stat = FS.fstat(stream.fd);
    expect(stat.size).toBe(1024);

    // Extended region should be zero-filled
    const buf = new Uint8Array(1024);
    FS.read(stream, buf, 0, 1024, 0);
    for (let i = 0; i < 1024; i++) {
      expect(buf[i]).toBe(0);
    }

    FS.close(stream);
  });

  it("allocate extends a file beyond current size", () => {
    const { FS } = h;
    const stream = FS.open("/alloc-grow", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("hello"), 0, 5);

    // allocate(offset=0, length=100) → file grows to 100
    stream.stream_ops.allocate(stream, 0, 100);

    const stat = FS.fstat(stream.fd);
    expect(stat.size).toBe(100);

    // Original data preserved
    const buf = new Uint8Array(5);
    FS.read(stream, buf, 0, 5, 0);
    expect(decode(buf, 5)).toBe("hello");

    FS.close(stream);
  });

  it("allocate does not shrink a file", () => {
    const { FS } = h;
    const stream = FS.open("/alloc-noshrink", O.RDWR | O.CREAT, 0o666);
    const data = new Uint8Array(500).fill(0xab);
    FS.write(stream, data, 0, 500);

    // allocate with smaller range — file should stay at 500
    stream.stream_ops.allocate(stream, 0, 100);

    const stat = FS.fstat(stream.fd);
    expect(stat.size).toBe(500);

    // Data intact
    const buf = new Uint8Array(500);
    FS.read(stream, buf, 0, 500, 0);
    expect(buf[0]).toBe(0xab);
    expect(buf[499]).toBe(0xab);

    FS.close(stream);
  });

  it("allocate with non-zero offset extends correctly", () => {
    const { FS } = h;
    const stream = FS.open("/alloc-offset", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("abc"), 0, 3);

    // allocate(offset=1000, length=500) → file grows to 1500
    stream.stream_ops.allocate(stream, 1000, 500);

    const stat = FS.fstat(stream.fd);
    expect(stat.size).toBe(1500);

    // Original data preserved
    const buf = new Uint8Array(3);
    FS.read(stream, buf, 0, 3, 0);
    expect(decode(buf, 3)).toBe("abc");

    FS.close(stream);
  });

  it("allocate spanning page boundary", () => {
    const { FS } = h;
    const stream = FS.open("/alloc-page-span", O.RDWR | O.CREAT, 0o666);

    // Allocate across a page boundary
    stream.stream_ops.allocate(stream, PAGE_SIZE - 100, 200);

    const stat = FS.fstat(stream.fd);
    expect(stat.size).toBe(PAGE_SIZE + 100);

    // Verify zero-fill at the page boundary region
    const buf = new Uint8Array(200);
    FS.read(stream, buf, 0, 200, PAGE_SIZE - 100);
    for (let i = 0; i < 200; i++) {
      expect(buf[i]).toBe(0);
    }

    FS.close(stream);
  });

  it("allocate preserves existing data at page boundaries", () => {
    const { FS } = h;
    const stream = FS.open("/alloc-preserve", O.RDWR | O.CREAT, 0o666);

    // Write data straddling a page boundary
    const data = new Uint8Array(PAGE_SIZE + 100).fill(0xcc);
    FS.write(stream, data, 0, PAGE_SIZE + 100);

    // Allocate beyond current size
    stream.stream_ops.allocate(stream, 0, PAGE_SIZE * 3);

    const stat = FS.fstat(stream.fd);
    expect(stat.size).toBe(PAGE_SIZE * 3);

    // Original data preserved
    const readBuf = new Uint8Array(PAGE_SIZE + 100);
    FS.read(stream, readBuf, 0, PAGE_SIZE + 100, 0);
    for (let i = 0; i < PAGE_SIZE + 100; i++) {
      expect(readBuf[i]).toBe(0xcc);
    }

    FS.close(stream);
  });

  it("allocate then write fills allocated region", () => {
    const { FS } = h;
    const stream = FS.open("/alloc-then-write", O.RDWR | O.CREAT, 0o666);

    // Pre-allocate
    stream.stream_ops.allocate(stream, 0, 2048);
    expect(FS.fstat(stream.fd).size).toBe(2048);

    // Write into the allocated region
    const data = new Uint8Array(100).fill(0x42);
    FS.write(stream, data, 0, 100, 1000);

    // Read back: zeros before, data in middle, zeros after
    const full = new Uint8Array(2048);
    FS.read(stream, full, 0, 2048, 0);

    for (let i = 0; i < 1000; i++) expect(full[i]).toBe(0);
    for (let i = 1000; i < 1100; i++) expect(full[i]).toBe(0x42);
    for (let i = 1100; i < 2048; i++) expect(full[i]).toBe(0);

    FS.close(stream);
  });

  it("multiple allocate calls are idempotent for same size", () => {
    const { FS } = h;
    const stream = FS.open("/alloc-idem", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("test"), 0, 4);

    stream.stream_ops.allocate(stream, 0, 1000);
    stream.stream_ops.allocate(stream, 0, 1000);
    stream.stream_ops.allocate(stream, 0, 1000);

    expect(FS.fstat(stream.fd).size).toBe(1000);

    const buf = new Uint8Array(4);
    FS.read(stream, buf, 0, 4, 0);
    expect(decode(buf, 4)).toBe("test");

    FS.close(stream);
  });
});

describeIfTomefs("adversarial: mmap() edge cases", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  it("mmap reads correct data from page boundary region @fast", () => {
    const { FS } = h;
    const stream = FS.open("/mmap-boundary", O.RDWR | O.CREAT, 0o666);

    // Write data spanning two pages
    const size = PAGE_SIZE + 512;
    const data = new Uint8Array(size);
    for (let i = 0; i < size; i++) data[i] = i & 0xff;
    FS.write(stream, data, 0, size);

    // mmap a region straddling the page boundary
    const result = stream.stream_ops.mmap(
      stream,
      256, // length
      PAGE_SIZE - 128, // position: 128 bytes before boundary
      1, // PROT_READ
      1, // MAP_SHARED
    );

    expect(result.ptr).toBeInstanceOf(Uint8Array);
    expect(result.ptr.length).toBe(256);

    // Verify mmap'd data matches what was written
    for (let i = 0; i < 256; i++) {
      expect(result.ptr[i]).toBe((PAGE_SIZE - 128 + i) & 0xff);
    }

    FS.close(stream);
  });

  it("mmap returns allocated=true for tomefs-backed files", () => {
    const { FS } = h;
    const stream = FS.open("/mmap-alloc", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, new Uint8Array(100), 0, 100);

    const result = stream.stream_ops.mmap(stream, 100, 0, 1, 1);
    expect(result.allocated).toBe(true);

    FS.close(stream);
  });

  it("mmap of zero-length region returns empty buffer", () => {
    const { FS } = h;
    const stream = FS.open("/mmap-zero", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, new Uint8Array(100), 0, 100);

    const result = stream.stream_ops.mmap(stream, 0, 0, 1, 1);
    expect(result.ptr).toBeInstanceOf(Uint8Array);
    expect(result.ptr.length).toBe(0);

    FS.close(stream);
  });

  it("mmap beyond file end returns zeros for unwritten region", () => {
    const { FS } = h;
    const stream = FS.open("/mmap-beyond", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("short"), 0, 5);

    // mmap 100 bytes starting at position 0, but file is only 5 bytes
    const result = stream.stream_ops.mmap(stream, 100, 0, 1, 1);
    expect(result.ptr.length).toBe(100);

    // First 5 bytes match written data
    expect(decode(result.ptr.subarray(0, 5), 5)).toBe("short");

    // Remaining bytes should be zero (unwritten pages)
    for (let i = 5; i < 100; i++) {
      expect(result.ptr[i]).toBe(0);
    }

    FS.close(stream);
  });

  it("mmap + msync round-trip preserves data across page boundary", () => {
    const { FS } = h;
    const stream = FS.open("/mmap-roundtrip", O.RDWR | O.CREAT, 0o666);

    // Pre-fill file with known data
    const size = PAGE_SIZE * 2;
    const data = new Uint8Array(size).fill(0xaa);
    FS.write(stream, data, 0, size);

    // mmap region straddling page boundary
    const mmapLen = 256;
    const mmapPos = PAGE_SIZE - 128;
    const result = stream.stream_ops.mmap(stream, mmapLen, mmapPos, 3, 1);

    // Modify the mmap'd buffer
    for (let i = 0; i < mmapLen; i++) result.ptr[i] = 0xbb;

    // msync back to file
    stream.stream_ops.msync(stream, result.ptr, mmapPos, mmapLen, 0);

    // Read back and verify
    const readBuf = new Uint8Array(size);
    FS.read(stream, readBuf, 0, size, 0);

    // Before mmap region: 0xaa
    for (let i = 0; i < mmapPos; i++) {
      expect(readBuf[i]).toBe(0xaa);
    }
    // mmap region: 0xbb
    for (let i = mmapPos; i < mmapPos + mmapLen; i++) {
      expect(readBuf[i]).toBe(0xbb);
    }
    // After mmap region: 0xaa
    for (let i = mmapPos + mmapLen; i < size; i++) {
      expect(readBuf[i]).toBe(0xaa);
    }

    FS.close(stream);
  });
});
