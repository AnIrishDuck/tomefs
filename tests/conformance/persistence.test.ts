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

  it("chmod-modified mode bits persist across remount", async () => {
    const { FS, tomefs } = await mountTome(backend);
    const stream = FS.open(`${MOUNT}/chmodfile`, O.RDWR | O.CREAT, 0o777);
    FS.write(stream, encode("data"), 0, 4);
    FS.close(stream);

    // Change mode after creation
    FS.chmod(`${MOUNT}/chmodfile`, 0o640);
    expect(FS.stat(`${MOUNT}/chmodfile`).mode & 0o777).toBe(0o640);

    syncAndUnmount(FS, tomefs);

    // Remount and verify the chmod'd mode persisted (not the creation mode)
    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/chmodfile`);
    expect(stat.mode & 0o777).toBe(0o640);
  });

  it("directory chmod persists across remount", async () => {
    const { FS, tomefs } = await mountTome(backend);
    FS.mkdir(`${MOUNT}/chmoddir`);
    FS.chmod(`${MOUNT}/chmoddir`, 0o750);
    expect(FS.stat(`${MOUNT}/chmoddir`).mode & 0o777).toBe(0o750);

    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/chmoddir`);
    expect(stat.mode & 0o777).toBe(0o750);
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

  it("all three timestamps (atime, mtime, ctime) persist across remount", async () => {
    const { FS, tomefs } = await mountTome(backend);
    const stream = FS.open(`${MOUNT}/allts`, O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("timestamps"), 0, 10);
    FS.close(stream);
    const stat1 = FS.stat(`${MOUNT}/allts`);
    const atime1 = stat1.atime.getTime();
    const mtime1 = stat1.mtime.getTime();
    const ctime1 = stat1.ctime.getTime();
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const stat2 = FS2.stat(`${MOUNT}/allts`);
    expect(stat2.atime.getTime()).toBe(atime1);
    expect(stat2.mtime.getTime()).toBe(mtime1);
    expect(stat2.ctime.getTime()).toBe(ctime1);
  });

  it("utime-modified timestamps persist across remount @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);
    const stream = FS.open(`${MOUNT}/utimed`, O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("data"), 0, 4);
    FS.close(stream);

    FS.utime(`${MOUNT}/utimed`, 1000000, 2000000);
    const stat1 = FS.stat(`${MOUNT}/utimed`);
    expect(stat1.atime.getTime()).toBe(1000000);
    expect(stat1.mtime.getTime()).toBe(2000000);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const stat2 = FS2.stat(`${MOUNT}/utimed`);
    expect(stat2.atime.getTime()).toBe(1000000);
    expect(stat2.mtime.getTime()).toBe(2000000);
  });

  it("directory timestamps persist across remount", async () => {
    const { FS, tomefs } = await mountTome(backend);
    FS.mkdir(`${MOUNT}/tsdir`, 0o755);
    FS.utime(`${MOUNT}/tsdir`, 3000000, 4000000);
    const stat1 = FS.stat(`${MOUNT}/tsdir`);
    expect(stat1.atime.getTime()).toBe(3000000);
    expect(stat1.mtime.getTime()).toBe(4000000);
    const ctime1 = stat1.ctime.getTime();
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const stat2 = FS2.stat(`${MOUNT}/tsdir`);
    expect(stat2.atime.getTime()).toBe(3000000);
    expect(stat2.mtime.getTime()).toBe(4000000);
    expect(stat2.ctime.getTime()).toBe(ctime1);
  });

  it("ctime updates from chmod persist across remount", async () => {
    const { FS, tomefs } = await mountTome(backend);
    const stream = FS.open(`${MOUNT}/chts`, O.RDWR | O.CREAT, 0o644);
    FS.write(stream, encode("x"), 0, 1);
    FS.close(stream);
    const before = FS.stat(`${MOUNT}/chts`);
    const ctimeBefore = before.ctime.getTime();

    // Small delay to ensure ctime changes
    const later = ctimeBefore + 100;
    FS.utime(`${MOUNT}/chts`, later, later);
    FS.chmod(`${MOUNT}/chts`, 0o600);
    const after = FS.stat(`${MOUNT}/chts`);
    const ctimeAfter = after.ctime.getTime();
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const stat2 = FS2.stat(`${MOUNT}/chts`);
    expect(stat2.ctime.getTime()).toBe(ctimeAfter);
    expect(stat2.mode & 0o777).toBe(0o600);
  });

  it("symlink timestamps persist across remount", async () => {
    const { FS, tomefs } = await mountTome(backend);
    FS.writeFile(`${MOUNT}/symtarget`, "target-data");
    FS.symlink(`${MOUNT}/symtarget`, `${MOUNT}/symts`);
    const lstat1 = FS.lstat(`${MOUNT}/symts`);
    const ctime1 = lstat1.ctime.getTime();
    const mtime1 = lstat1.mtime.getTime();
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const lstat2 = FS2.lstat(`${MOUNT}/symts`);
    expect(lstat2.ctime.getTime()).toBe(ctime1);
    expect(lstat2.mtime.getTime()).toBe(mtime1);
    const target = FS2.readlink(`${MOUNT}/symts`);
    expect(target).toBe(`${MOUNT}/symtarget`);
  });

  it("multiple files with distinct utime values persist independently", async () => {
    const { FS, tomefs } = await mountTome(backend);
    for (let i = 0; i < 5; i++) {
      const s = FS.open(`${MOUNT}/ts${i}`, O.RDWR | O.CREAT, 0o666);
      FS.write(s, encode(`f${i}`), 0, 2);
      FS.close(s);
      FS.utime(`${MOUNT}/ts${i}`, (i + 1) * 1000000, (i + 1) * 2000000);
    }
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    for (let i = 0; i < 5; i++) {
      const stat = FS2.stat(`${MOUNT}/ts${i}`);
      expect(stat.atime.getTime()).toBe((i + 1) * 1000000);
      expect(stat.mtime.getTime()).toBe((i + 1) * 2000000);
    }
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

  it("renamed directory preserves child file data across remount", async () => {
    const { FS, tomefs } = await mountTome(backend);
    FS.mkdir(`${MOUNT}/mydir`);
    const s = FS.open(`${MOUNT}/mydir/data.txt`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("child file data"), 0, 15);
    FS.close(s);
    FS.rename(`${MOUNT}/mydir`, `${MOUNT}/renamed`);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    expect(() => FS2.stat(`${MOUNT}/mydir`)).toThrow();
    const stat = FS2.stat(`${MOUNT}/renamed`);
    expect(FS2.isDir(stat.mode)).toBe(true);
    const s2 = FS2.open(`${MOUNT}/renamed/data.txt`, O.RDONLY);
    const buf = new Uint8Array(30);
    const n = FS2.read(s2, buf, 0, 30);
    FS2.close(s2);
    expect(decode(buf, n)).toBe("child file data");
  });

  it("renamed nested directory preserves deeply nested files across remount", async () => {
    const { FS, tomefs } = await mountTome(backend);
    FS.mkdir(`${MOUNT}/top`);
    FS.mkdir(`${MOUNT}/top/mid`);
    FS.mkdir(`${MOUNT}/top/mid/bottom`);
    // Create files at each level
    for (const [path, content] of [
      [`${MOUNT}/top/f1.txt`, "level-1"],
      [`${MOUNT}/top/mid/f2.txt`, "level-2"],
      [`${MOUNT}/top/mid/bottom/f3.txt`, "level-3"],
    ] as const) {
      const s = FS.open(path, O.RDWR | O.CREAT, 0o666);
      FS.write(s, encode(content), 0, content.length);
      FS.close(s);
    }

    // Rename the top-level directory
    FS.rename(`${MOUNT}/top`, `${MOUNT}/moved`);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    for (const [path, content] of [
      [`${MOUNT}/moved/f1.txt`, "level-1"],
      [`${MOUNT}/moved/mid/f2.txt`, "level-2"],
      [`${MOUNT}/moved/mid/bottom/f3.txt`, "level-3"],
    ] as const) {
      const s = FS2.open(path, O.RDONLY);
      const buf = new Uint8Array(20);
      const n = FS2.read(s, buf, 0, 20);
      FS2.close(s);
      expect(decode(buf, n)).toBe(content);
    }
  });

  it("renamed directory with multi-page files preserves data across remount", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);
    FS.mkdir(`${MOUNT}/bigdir`);
    // Create a file larger than the cache
    const size = PAGE_SIZE * 6;
    const data = new Uint8Array(size);
    for (let i = 0; i < size; i++) data[i] = (i * 11 + 3) & 0xff;
    const s = FS.open(`${MOUNT}/bigdir/large.bin`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, size);
    FS.close(s);

    FS.rename(`${MOUNT}/bigdir`, `${MOUNT}/newbigdir`);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend, 4);
    const s2 = FS2.open(`${MOUNT}/newbigdir/large.bin`, O.RDONLY);
    const buf = new Uint8Array(size);
    const n = FS2.read(s2, buf, 0, size);
    FS2.close(s2);
    expect(n).toBe(size);
    expect(buf).toEqual(data);
  });

  // ------------------------------------------------------------------
  // Cross-type rename persistence
  // ------------------------------------------------------------------

  it("rename file over symlink persists correctly", async () => {
    const { FS, tomefs } = await mountTome(backend);
    // Create a file and a symlink at different paths
    const s = FS.open(`${MOUNT}/target`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("symlink-dest"), 0, 12);
    FS.close(s);
    FS.symlink(`${MOUNT}/target`, `${MOUNT}/link`);
    const s2 = FS.open(`${MOUNT}/src`, O.RDWR | O.CREAT, 0o666);
    FS.write(s2, encode("file-data"), 0, 9);
    FS.close(s2);

    // Rename file over symlink
    FS.rename(`${MOUNT}/src`, `${MOUNT}/link`);
    syncAndUnmount(FS, tomefs);

    // Remount and verify
    const { FS: FS2 } = await mountTome(backend);
    // /link should be a regular file, not a symlink
    const stat = FS2.stat(`${MOUNT}/link`);
    expect(FS2.isFile(stat.mode)).toBe(true);
    expect(stat.size).toBe(9);
    const rd = FS2.open(`${MOUNT}/link`, O.RDONLY);
    const buf = new Uint8Array(20);
    const n = FS2.read(rd, buf, 0, 20);
    FS2.close(rd);
    expect(decode(buf, n)).toBe("file-data");
    // /src should not exist
    expect(() => FS2.stat(`${MOUNT}/src`)).toThrow();
    // /target should still exist (untouched)
    const rd2 = FS2.open(`${MOUNT}/target`, O.RDONLY);
    const buf2 = new Uint8Array(20);
    const n2 = FS2.read(rd2, buf2, 0, 20);
    FS2.close(rd2);
    expect(decode(buf2, n2)).toBe("symlink-dest");
  });

  it("rename symlink over file persists correctly", async () => {
    const { FS, tomefs } = await mountTome(backend);
    const s = FS.open(`${MOUNT}/existing`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("will-be-replaced"), 0, 16);
    FS.close(s);
    FS.symlink(`${MOUNT}/sym-target`, `${MOUNT}/link`);
    // Create the symlink target too
    const s2 = FS.open(`${MOUNT}/sym-target`, O.RDWR | O.CREAT, 0o666);
    FS.write(s2, encode("link-dest"), 0, 9);
    FS.close(s2);

    // Rename symlink over file
    FS.rename(`${MOUNT}/link`, `${MOUNT}/existing`);
    syncAndUnmount(FS, tomefs);

    // Remount and verify
    const { FS: FS2 } = await mountTome(backend);
    // /existing should be a symlink
    const lstat = FS2.lstat(`${MOUNT}/existing`);
    expect(FS2.isLink(lstat.mode)).toBe(true);
    expect(FS2.readlink(`${MOUNT}/existing`)).toBe(`${MOUNT}/sym-target`);
    // Following the symlink should reach the target
    const rd = FS2.open(`${MOUNT}/existing`, O.RDONLY);
    const buf = new Uint8Array(20);
    const n = FS2.read(rd, buf, 0, 20);
    FS2.close(rd);
    expect(decode(buf, n)).toBe("link-dest");
    // /link should not exist
    expect(() => FS2.lstat(`${MOUNT}/link`)).toThrow();
  });

  it("rename symlink over symlink persists correctly", async () => {
    const { FS, tomefs } = await mountTome(backend);
    FS.writeFile(`${MOUNT}/a`, "file-a");
    FS.writeFile(`${MOUNT}/b`, "file-b");
    FS.symlink(`${MOUNT}/a`, `${MOUNT}/link-a`);
    FS.symlink(`${MOUNT}/b`, `${MOUNT}/link-b`);

    // Rename link-a over link-b
    FS.rename(`${MOUNT}/link-a`, `${MOUNT}/link-b`);
    syncAndUnmount(FS, tomefs);

    // Remount and verify
    const { FS: FS2 } = await mountTome(backend);
    // /link-b should now point to /a
    expect(FS2.readlink(`${MOUNT}/link-b`)).toBe(`${MOUNT}/a`);
    const content = FS2.readFile(`${MOUNT}/link-b`, { encoding: "utf8" }) as string;
    expect(content).toBe("file-a");
    // /link-a should not exist
    expect(() => FS2.lstat(`${MOUNT}/link-a`)).toThrow();
    // Both target files should still exist
    expect((FS2.readFile(`${MOUNT}/a`, { encoding: "utf8" }) as string)).toBe("file-a");
    expect((FS2.readFile(`${MOUNT}/b`, { encoding: "utf8" }) as string)).toBe("file-b");
  });

  it("rename file over symlink with multi-page data persists correctly", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);
    // Create a large file that will cause eviction
    const size = PAGE_SIZE * 3 + 500;
    const data = new Uint8Array(size);
    for (let i = 0; i < size; i++) data[i] = (i * 13 + 7) & 0xff;

    FS.writeFile(`${MOUNT}/target`, "dest");
    FS.symlink(`${MOUNT}/target`, `${MOUNT}/link`);
    const s = FS.open(`${MOUNT}/bigfile`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, size);
    FS.close(s);

    FS.rename(`${MOUNT}/bigfile`, `${MOUNT}/link`);
    syncAndUnmount(FS, tomefs);

    // Remount with small cache and verify full data integrity
    const { FS: FS2 } = await mountTome(backend, 4);
    const stat = FS2.stat(`${MOUNT}/link`);
    expect(FS2.isFile(stat.mode)).toBe(true);
    expect(stat.size).toBe(size);
    const rd = FS2.open(`${MOUNT}/link`, O.RDONLY);
    const buf = new Uint8Array(size);
    const n = FS2.read(rd, buf, 0, size);
    FS2.close(rd);
    expect(n).toBe(size);
    expect(buf).toEqual(data);
  });

  it("rename dangling symlink over file persists correctly", async () => {
    const { FS, tomefs } = await mountTome(backend);
    FS.writeFile(`${MOUNT}/existing`, "file-data");
    FS.symlink(`${MOUNT}/nonexistent`, `${MOUNT}/dangling`);

    FS.rename(`${MOUNT}/dangling`, `${MOUNT}/existing`);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const lstat = FS2.lstat(`${MOUNT}/existing`);
    expect(FS2.isLink(lstat.mode)).toBe(true);
    expect(FS2.readlink(`${MOUNT}/existing`)).toBe(`${MOUNT}/nonexistent`);
    // Following the dangling symlink should fail
    expect(() => FS2.stat(`${MOUNT}/existing`)).toThrow();
  });

  it("cross-type rename survives two mount cycles", async () => {
    // Cycle 1: create file + symlink, rename file over symlink
    const { FS: FS1, tomefs: t1 } = await mountTome(backend);
    FS1.writeFile(`${MOUNT}/target`, "link-target");
    FS1.symlink(`${MOUNT}/target`, `${MOUNT}/slot`);
    FS1.writeFile(`${MOUNT}/file`, "file-content");
    FS1.rename(`${MOUNT}/file`, `${MOUNT}/slot`);
    syncAndUnmount(FS1, t1);

    // Cycle 2: verify, then rename symlink over the file
    const { FS: FS2, tomefs: t2 } = await mountTome(backend);
    expect(FS2.isFile(FS2.stat(`${MOUNT}/slot`).mode)).toBe(true);
    expect((FS2.readFile(`${MOUNT}/slot`, { encoding: "utf8" }) as string)).toBe("file-content");

    FS2.symlink(`${MOUNT}/target`, `${MOUNT}/newlink`);
    FS2.rename(`${MOUNT}/newlink`, `${MOUNT}/slot`);
    syncAndUnmount(FS2, t2);

    // Cycle 3: verify symlink persisted
    const { FS: FS3 } = await mountTome(backend);
    const lstat = FS3.lstat(`${MOUNT}/slot`);
    expect(FS3.isLink(lstat.mode)).toBe(true);
    expect(FS3.readlink(`${MOUNT}/slot`)).toBe(`${MOUNT}/target`);
    expect((FS3.readFile(`${MOUNT}/slot`, { encoding: "utf8" }) as string)).toBe("link-target");
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
