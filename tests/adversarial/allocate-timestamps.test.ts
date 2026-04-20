/**
 * Adversarial tests: allocate() POSIX timestamp compliance.
 *
 * posix_fallocate(3p) requires mtime and ctime to be updated when the file
 * size changes. PostgreSQL uses posix_fallocate for WAL segment pre-allocation
 * and may check modification times for checkpoint/archival logic.
 *
 * These tests only run when TOMEFS_BACKEND=tomefs (MEMFS doesn't implement
 * allocate — it throws EOPNOTSUPP).
 */
import {
  createFS,
  encode,
  O,
  SEEK_SET,
  type FSHarness,
} from "../harness/emscripten-fs.js";

const describeIfTomefs =
  process.env.TOMEFS_BACKEND === "tomefs" ? describe : describe.skip;

describeIfTomefs("adversarial: allocate() timestamps (ethos §2)", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  it("allocate updates mtime when file grows @fast", async () => {
    const { FS } = h;
    const stream = FS.open("/alloc-ts", O.RDWR | O.CREAT, 0o666);

    const before = FS.fstat(stream.fd);
    const beforeMtime = before.mtime.getTime();

    await new Promise((r) => setTimeout(r, 20));

    stream.stream_ops.allocate(stream, 0, 8192);

    const after = FS.fstat(stream.fd);
    expect(after.size).toBe(8192);
    expect(after.mtime.getTime()).toBeGreaterThan(beforeMtime);

    FS.close(stream);
  });

  it("allocate updates ctime when file grows @fast", async () => {
    const { FS } = h;
    const stream = FS.open("/alloc-ctime", O.RDWR | O.CREAT, 0o666);

    const before = FS.fstat(stream.fd);
    const beforeCtime = before.ctime.getTime();

    await new Promise((r) => setTimeout(r, 20));

    stream.stream_ops.allocate(stream, 0, 4096);

    const after = FS.fstat(stream.fd);
    expect(after.ctime.getTime()).toBeGreaterThan(beforeCtime);

    FS.close(stream);
  });

  it("allocate does not update timestamps when size unchanged", () => {
    const { FS } = h;
    const stream = FS.open("/alloc-noop", O.RDWR | O.CREAT, 0o666);

    FS.write(stream, encode("x".repeat(100)), 0, 100);
    const before = FS.fstat(stream.fd);

    // Allocate within existing size — should be a no-op
    stream.stream_ops.allocate(stream, 0, 50);

    const after = FS.fstat(stream.fd);
    expect(after.size).toBe(100);
    expect(after.mtime.getTime()).toBe(before.mtime.getTime());

    FS.close(stream);
  });

  it("allocate extending beyond existing data updates timestamps", async () => {
    const { FS } = h;
    const stream = FS.open("/alloc-extend", O.RDWR | O.CREAT, 0o666);

    FS.write(stream, encode("data"), 0, 4);
    const stat1 = FS.fstat(stream.fd);
    expect(stat1.size).toBe(4);

    await new Promise((r) => setTimeout(r, 20));

    stream.stream_ops.allocate(stream, 0, 16384);
    const stat2 = FS.fstat(stream.fd);
    expect(stat2.size).toBe(16384);
    expect(stat2.mtime.getTime()).toBeGreaterThan(stat1.mtime.getTime());
    expect(stat2.ctime.getTime()).toBeGreaterThan(stat1.ctime.getTime());

    // Original data should be preserved
    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(4);
    FS.read(stream, buf, 0, 4);
    expect(new TextDecoder().decode(buf)).toBe("data");

    FS.close(stream);
  });

  it("allocate with offset updates timestamps correctly", async () => {
    const { FS } = h;
    const stream = FS.open("/alloc-offset", O.RDWR | O.CREAT, 0o666);

    FS.write(stream, encode("x".repeat(100)), 0, 100);
    const before = FS.fstat(stream.fd);

    await new Promise((r) => setTimeout(r, 20));

    // offset + length > current size, so file must grow
    stream.stream_ops.allocate(stream, 100, 200);
    const after = FS.fstat(stream.fd);
    expect(after.size).toBe(300);
    expect(after.mtime.getTime()).toBeGreaterThan(before.mtime.getTime());

    FS.close(stream);
  });
});
