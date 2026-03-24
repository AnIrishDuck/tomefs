/**
 * Batch 6 conformance tests: Persistence
 *
 * No upstream source — these are custom tests verifying that tomefs
 * correctly persists and restores file data, metadata, and directory
 * structure across unmount/remount cycles.
 *
 * Tests run against tomefs only (MEMFS has no persistence concept).
 * A shared SyncMemoryBackend instance survives across mount/unmount,
 * simulating the role IDB will play in production.
 */
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { describe, it, expect, beforeEach } from "vitest";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { createTomeFS } from "../../src/tomefs.js";
import { PAGE_SIZE } from "../../src/types.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

/** Open flag constants (Linux/WASM values). */
const O = {
  RDONLY: 0,
  WRONLY: 1,
  RDWR: 2,
  CREAT: 64,
  TRUNC: 512,
  APPEND: 1024,
} as const;

const SEEK_SET = 0;
const SEEK_END = 2;
const MOUNT = "/tome";

/** Encode a string to Uint8Array. */
function encode(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

/** Decode a Uint8Array to string. */
function decode(buf: Uint8Array, length?: number): string {
  return new TextDecoder().decode(
    length !== undefined ? buf.subarray(0, length) : buf,
  );
}

/**
 * Create a fresh Emscripten module, mount tomefs at MOUNT with the given
 * backend, and return FS + tomefs instance.
 */
async function mountTome(backend: SyncMemoryBackend, maxPages?: number) {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  const Module = await createModule();
  const FS = Module.FS;
  const tomefs = createTomeFS(FS, { backend, maxPages });
  FS.mkdir(MOUNT);
  FS.mount(tomefs, {}, MOUNT);
  return { FS, tomefs, Module };
}

/**
 * Sync (persist metadata) then unmount tomefs.
 */
function syncAndUnmount(FS: any, tomefs: any) {
  // Trigger syncfs to persist directory tree metadata
  tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
    if (err) throw err;
  });
  FS.unmount(MOUNT);
}

describe("persistence (Batch 6)", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  // ------------------------------------------------------------------
  // Round-trip tests
  // ------------------------------------------------------------------

  it("file data survives unmount/remount @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);
    const stream = FS.open(`${MOUNT}/hello`, O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("hello world"), 0, 11);
    FS.close(stream);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const stream2 = FS2.open(`${MOUNT}/hello`, O.RDONLY);
    const buf = new Uint8Array(32);
    const n = FS2.read(stream2, buf, 0, 32);
    FS2.close(stream2);

    expect(n).toBe(11);
    expect(decode(buf, n)).toBe("hello world");
  });

  it("file size is preserved across remount", async () => {
    const { FS, tomefs } = await mountTome(backend);
    const data = new Uint8Array(1234);
    for (let i = 0; i < data.length; i++) data[i] = i & 0xff;
    const stream = FS.open(`${MOUNT}/sized`, O.RDWR | O.CREAT, 0o666);
    FS.write(stream, data, 0, data.length);
    FS.close(stream);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/sized`);
    expect(stat.size).toBe(1234);
  });

  it("file mode bits are preserved across remount", async () => {
    const { FS, tomefs } = await mountTome(backend);
    const stream = FS.open(`${MOUNT}/modefile`, O.RDWR | O.CREAT, 0o644);
    FS.write(stream, encode("x"), 0, 1);
    FS.close(stream);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/modefile`);
    expect(stat.mode & 0o777).toBe(0o644);
  });

  it("file timestamps are preserved across remount", async () => {
    const { FS, tomefs } = await mountTome(backend);
    const stream = FS.open(`${MOUNT}/timed`, O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("ts"), 0, 2);
    FS.close(stream);
    const stat1 = FS.stat(`${MOUNT}/timed`);
    const mtime1 = stat1.mtime.getTime();
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const stat2 = FS2.stat(`${MOUNT}/timed`);
    expect(stat2.mtime.getTime()).toBe(mtime1);
  });

  it("multiple files persist independently @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);
    for (const name of ["a", "b", "c"]) {
      const s = FS.open(`${MOUNT}/${name}`, O.RDWR | O.CREAT, 0o666);
      FS.write(s, encode(`file-${name}`), 0, 6);
      FS.close(s);
    }
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    for (const name of ["a", "b", "c"]) {
      const s = FS2.open(`${MOUNT}/${name}`, O.RDONLY);
      const buf = new Uint8Array(10);
      const n = FS2.read(s, buf, 0, 10);
      FS2.close(s);
      expect(decode(buf, n)).toBe(`file-${name}`);
    }
  });

  it("overwritten file has new content after remount", async () => {
    const { FS, tomefs } = await mountTome(backend);
    let s = FS.open(`${MOUNT}/ow`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("original"), 0, 8);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2, tomefs: t2 } = await mountTome(backend);
    s = FS2.open(`${MOUNT}/ow`, O.RDWR | O.TRUNC);
    FS2.write(s, encode("replaced"), 0, 8);
    FS2.close(s);
    syncAndUnmount(FS2, t2);

    const { FS: FS3 } = await mountTome(backend);
    s = FS3.open(`${MOUNT}/ow`, O.RDONLY);
    const buf = new Uint8Array(20);
    const n = FS3.read(s, buf, 0, 20);
    FS3.close(s);
    expect(decode(buf, n)).toBe("replaced");
  });

  it("deleted file does not reappear after remount", async () => {
    const { FS, tomefs } = await mountTome(backend);
    const s = FS.open(`${MOUNT}/gone`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("bye"), 0, 3);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2, tomefs: t2 } = await mountTome(backend);
    FS2.unlink(`${MOUNT}/gone`);
    syncAndUnmount(FS2, t2);

    const { FS: FS3 } = await mountTome(backend);
    expect(() => FS3.stat(`${MOUNT}/gone`)).toThrow();
  });

  // ------------------------------------------------------------------
  // Multi-page tests
  // ------------------------------------------------------------------

  it("large file spanning multiple pages survives remount @fast", async () => {
    const size = PAGE_SIZE * 5 + 1234; // 5+ pages
    const data = new Uint8Array(size);
    for (let i = 0; i < size; i++) data[i] = (i * 7 + 13) & 0xff;

    const { FS, tomefs } = await mountTome(backend);
    const s = FS.open(`${MOUNT}/large`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, size);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const s2 = FS2.open(`${MOUNT}/large`, O.RDONLY);
    const buf = new Uint8Array(size);
    const n = FS2.read(s2, buf, 0, size);
    FS2.close(s2);
    expect(n).toBe(size);
    expect(buf).toEqual(data);
  });

  it("large file with tiny cache survives remount via eviction round-trip", async () => {
    const size = PAGE_SIZE * 8; // 8 pages through a 4-page cache
    const data = new Uint8Array(size);
    for (let i = 0; i < size; i++) data[i] = (i * 3 + 7) & 0xff;

    const { FS, tomefs } = await mountTome(backend, /* maxPages */ 4);
    const s = FS.open(`${MOUNT}/evicted`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, size);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend, 4);
    const s2 = FS2.open(`${MOUNT}/evicted`, O.RDONLY);
    const buf = new Uint8Array(size);
    const n = FS2.read(s2, buf, 0, size);
    FS2.close(s2);
    expect(n).toBe(size);
    expect(buf).toEqual(data);
  });

  it("partial last page is preserved correctly", async () => {
    const size = PAGE_SIZE + 1;
    const data = new Uint8Array(size);
    data[0] = 0xaa;
    data[PAGE_SIZE - 1] = 0xbb;
    data[PAGE_SIZE] = 0xcc;

    const { FS, tomefs } = await mountTome(backend);
    const s = FS.open(`${MOUNT}/partial`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, size);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const s2 = FS2.open(`${MOUNT}/partial`, O.RDONLY);
    const buf = new Uint8Array(size + 10);
    const n = FS2.read(s2, buf, 0, size + 10);
    FS2.close(s2);
    expect(n).toBe(size);
    expect(buf[0]).toBe(0xaa);
    expect(buf[PAGE_SIZE - 1]).toBe(0xbb);
    expect(buf[PAGE_SIZE]).toBe(0xcc);
  });

  // ------------------------------------------------------------------
  // Directory tree persistence
  // ------------------------------------------------------------------

  it("nested directory structure survives remount @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);
    FS.mkdir(`${MOUNT}/a`);
    FS.mkdir(`${MOUNT}/a/b`);
    FS.mkdir(`${MOUNT}/a/b/c`);
    const s = FS.open(`${MOUNT}/a/b/c/deep`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("deep file"), 0, 9);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const entries_a = FS2.readdir(`${MOUNT}/a`);
    expect(entries_a).toContain("b");
    const entries_b = FS2.readdir(`${MOUNT}/a/b`);
    expect(entries_b).toContain("c");
    const s2 = FS2.open(`${MOUNT}/a/b/c/deep`, O.RDONLY);
    const buf = new Uint8Array(20);
    const n = FS2.read(s2, buf, 0, 20);
    FS2.close(s2);
    expect(decode(buf, n)).toBe("deep file");
  });

  it("directory mode bits are preserved across remount", async () => {
    const { FS, tomefs } = await mountTome(backend);
    FS.mkdir(`${MOUNT}/perms`, 0o755);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/perms`);
    expect(stat.mode & 0o777).toBe(0o755);
  });

  it("removed directory does not reappear after remount", async () => {
    const { FS, tomefs } = await mountTome(backend);
    FS.mkdir(`${MOUNT}/tempdir`);
    const s = FS.open(`${MOUNT}/tempdir/f`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("x"), 0, 1);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2, tomefs: t2 } = await mountTome(backend);
    FS2.unlink(`${MOUNT}/tempdir/f`);
    FS2.rmdir(`${MOUNT}/tempdir`);
    syncAndUnmount(FS2, t2);

    const { FS: FS3 } = await mountTome(backend);
    expect(() => FS3.stat(`${MOUNT}/tempdir`)).toThrow();
  });

  it("files and directories interleaved at multiple levels", async () => {
    const { FS, tomefs } = await mountTome(backend);
    FS.mkdir(`${MOUNT}/d1`);
    FS.mkdir(`${MOUNT}/d1/d2`);
    for (const [dir, name, content] of [
      [`${MOUNT}`, "root.txt", "at root"],
      [`${MOUNT}/d1`, "mid.txt", "at d1"],
      [`${MOUNT}/d1/d2`, "leaf.txt", "at d2"],
    ] as const) {
      const s = FS.open(`${dir}/${name}`, O.RDWR | O.CREAT, 0o666);
      FS.write(s, encode(content), 0, content.length);
      FS.close(s);
    }
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    for (const [dir, name, content] of [
      [`${MOUNT}`, "root.txt", "at root"],
      [`${MOUNT}/d1`, "mid.txt", "at d1"],
      [`${MOUNT}/d1/d2`, "leaf.txt", "at d2"],
    ] as const) {
      const s = FS2.open(`${dir}/${name}`, O.RDONLY);
      const buf = new Uint8Array(20);
      const n = FS2.read(s, buf, 0, 20);
      FS2.close(s);
      expect(decode(buf, n)).toBe(content);
    }
  });

  it("symlink target is preserved across remount", async () => {
    const { FS, tomefs } = await mountTome(backend);
    const s = FS.open(`${MOUNT}/target`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("link-data"), 0, 9);
    FS.close(s);
    FS.symlink(`${MOUNT}/target`, `${MOUNT}/link`);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const linkTarget = FS2.readlink(`${MOUNT}/link`);
    expect(linkTarget).toBe(`${MOUNT}/target`);
    const s2 = FS2.open(`${MOUNT}/link`, O.RDONLY);
    const buf = new Uint8Array(20);
    const n = FS2.read(s2, buf, 0, 20);
    FS2.close(s2);
    expect(decode(buf, n)).toBe("link-data");
  });

  // ------------------------------------------------------------------
  // Edge cases
  // ------------------------------------------------------------------

  it("empty file persists correctly (zero bytes)", async () => {
    const { FS, tomefs } = await mountTome(backend);
    const s = FS.open(`${MOUNT}/empty`, O.RDWR | O.CREAT, 0o666);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/empty`);
    expect(stat.size).toBe(0);
  });

  it("truncated file has correct size after remount", async () => {
    const { FS, tomefs } = await mountTome(backend);
    const s = FS.open(`${MOUNT}/trunc`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("long content here"), 0, 17);
    FS.ftruncate(s.fd, 4);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/trunc`);
    expect(stat.size).toBe(4);
    const s2 = FS2.open(`${MOUNT}/trunc`, O.RDONLY);
    const buf = new Uint8Array(20);
    const n = FS2.read(s2, buf, 0, 20);
    FS2.close(s2);
    expect(n).toBe(4);
    expect(decode(buf, n)).toBe("long");
  });

  it("append-mode writes persist correctly", async () => {
    const { FS, tomefs } = await mountTome(backend);
    let s = FS.open(`${MOUNT}/app`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("first"), 0, 5);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2, tomefs: t2 } = await mountTome(backend);
    s = FS2.open(`${MOUNT}/app`, O.WRONLY | O.APPEND);
    FS2.write(s, encode("-second"), 0, 7);
    FS2.close(s);
    syncAndUnmount(FS2, t2);

    const { FS: FS3 } = await mountTome(backend);
    s = FS3.open(`${MOUNT}/app`, O.RDONLY);
    const buf = new Uint8Array(30);
    const n = FS3.read(s, buf, 0, 30);
    FS3.close(s);
    expect(decode(buf, n)).toBe("first-second");
  });

  it("renamed file persists under new name", async () => {
    const { FS, tomefs } = await mountTome(backend);
    const s = FS.open(`${MOUNT}/oldname`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("renamed"), 0, 7);
    FS.close(s);
    FS.rename(`${MOUNT}/oldname`, `${MOUNT}/newname`);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    expect(() => FS2.stat(`${MOUNT}/oldname`)).toThrow();
    const s2 = FS2.open(`${MOUNT}/newname`, O.RDONLY);
    const buf = new Uint8Array(20);
    const n = FS2.read(s2, buf, 0, 20);
    FS2.close(s2);
    expect(decode(buf, n)).toBe("renamed");
  });

  it("three mount cycles with modifications each time", async () => {
    // Cycle 1: create a, b
    const { FS: FS1, tomefs: t1 } = await mountTome(backend);
    for (const name of ["a", "b"]) {
      const s = FS1.open(`${MOUNT}/${name}`, O.RDWR | O.CREAT, 0o666);
      FS1.write(s, encode(`v1-${name}`), 0, 4);
      FS1.close(s);
    }
    syncAndUnmount(FS1, t1);

    // Cycle 2: modify a, delete b, create c
    const { FS: FS2, tomefs: t2 } = await mountTome(backend);
    let s = FS2.open(`${MOUNT}/a`, O.RDWR | O.TRUNC);
    FS2.write(s, encode("v2-a"), 0, 4);
    FS2.close(s);
    FS2.unlink(`${MOUNT}/b`);
    s = FS2.open(`${MOUNT}/c`, O.RDWR | O.CREAT, 0o666);
    FS2.write(s, encode("v2-c"), 0, 4);
    FS2.close(s);
    syncAndUnmount(FS2, t2);

    // Cycle 3: verify
    const { FS: FS3 } = await mountTome(backend);
    s = FS3.open(`${MOUNT}/a`, O.RDONLY);
    const buf = new Uint8Array(10);
    let n = FS3.read(s, buf, 0, 10);
    FS3.close(s);
    expect(decode(buf, n)).toBe("v2-a");
    expect(() => FS3.stat(`${MOUNT}/b`)).toThrow();
    s = FS3.open(`${MOUNT}/c`, O.RDONLY);
    n = FS3.read(s, buf, 0, 10);
    FS3.close(s);
    expect(decode(buf, n)).toBe("v2-c");
  });
});
