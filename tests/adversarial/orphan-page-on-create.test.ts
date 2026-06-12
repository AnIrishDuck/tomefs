/**
 * Adversarial tests: orphan page cleanup during file creation.
 *
 * When a new file is created at a storage path that already has orphan pages
 * in the backend (from a previous file that was deleted or crashed), createNode
 * must clean up those pages to prevent restoreTree from misinterpreting them
 * as the new file's data on remount.
 *
 * Without this cleanup, the orphan pages survive syncfs and cause restoreTree
 * to extend the file size on the next mount — silently corrupting the file
 * with stale data from the previous incarnation.
 */
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { describe, it, expect, beforeEach } from "vitest";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { createTomeFS } from "../../src/tomefs.js";
import { PAGE_SIZE } from "../../src/types.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

const O = {
  RDONLY: 0,
  WRONLY: 1,
  RDWR: 2,
  CREAT: 64,
  TRUNC: 512,
} as const;

const MOUNT = "/tome";

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

function syncAndUnmount(FS: any): void {
  let err: Error | null = null;
  FS.syncfs(false, (e: Error | null) => { err = e; });
  if (err) throw err;
}

describe("orphan page cleanup on file creation", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("single orphan page cleaned up when new file created at same path @fast", async () => {
    backend.writePage("/target", 0, new Uint8Array(PAGE_SIZE).fill(0xaa));

    const { FS } = await mountTome(backend);

    const fd = FS.open(`${MOUNT}/target`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, new Uint8Array([1, 2, 3]), 0, 3);
    FS.close(fd);

    // Orphan page should be gone from backend after file creation
    syncAndUnmount(FS);
    expect(backend.countPages("/target")).toBe(1);

    // Remount — file should have correct size, not extended by orphan
    const { FS: FS2 } = await mountTome(backend);
    expect(FS2.stat(`${MOUNT}/target`).size).toBe(3);
  });

  it("multiple orphan pages cleaned up @fast", async () => {
    backend.writePage("/multi", 0, new Uint8Array(PAGE_SIZE).fill(0x11));
    backend.writePage("/multi", 1, new Uint8Array(PAGE_SIZE).fill(0x22));
    backend.writePage("/multi", 2, new Uint8Array(PAGE_SIZE).fill(0x33));
    backend.writePage("/multi", 3, new Uint8Array(PAGE_SIZE).fill(0x44));

    const { FS } = await mountTome(backend);

    // Write a small file at the orphan path
    const data = new Uint8Array(100);
    for (let i = 0; i < 100; i++) data[i] = i;
    const fd = FS.open(`${MOUNT}/multi`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, data, 0, 100);
    FS.close(fd);

    syncAndUnmount(FS);
    const { FS: FS2 } = await mountTome(backend);

    // File has correct size — no extension from orphan pages
    expect(FS2.stat(`${MOUNT}/multi`).size).toBe(100);

    // Data is correct
    const buf = new Uint8Array(100);
    const fd2 = FS2.open(`${MOUNT}/multi`, O.RDONLY);
    FS2.read(fd2, buf, 0, 100);
    FS2.close(fd2);
    expect(buf).toEqual(data);
  });

  it("orphan cleanup does not affect restored files @fast", async () => {
    // Pre-existing file in backend with metadata
    const now = Date.now();
    backend.writeMeta("/existing", {
      size: PAGE_SIZE * 2,
      mode: 0o100644,
      ctime: now,
      mtime: now,
      atime: now,
    });
    backend.writePage("/existing", 0, new Uint8Array(PAGE_SIZE).fill(0xaa));
    backend.writePage("/existing", 1, new Uint8Array(PAGE_SIZE).fill(0xbb));

    const { FS } = await mountTome(backend);

    // Restored file should have all its pages intact
    const stat = FS.stat(`${MOUNT}/existing`);
    expect(stat.size).toBe(PAGE_SIZE * 2);

    const buf = new Uint8Array(PAGE_SIZE);
    const fd = FS.open(`${MOUNT}/existing`, O.RDONLY);
    FS.read(fd, buf, 0, PAGE_SIZE, 0);
    FS.close(fd);
    expect(buf[0]).toBe(0xaa);

    syncAndUnmount(FS);
    expect(backend.countPages("/existing")).toBe(2);
  });

  it("orphan cleanup on create followed by operations @fast", async () => {
    // Orphan pages at path
    backend.writePage("/ops", 0, new Uint8Array(PAGE_SIZE).fill(0xff));
    backend.writePage("/ops", 1, new Uint8Array(PAGE_SIZE).fill(0xfe));

    const { FS } = await mountTome(backend);

    // Create new file, write, truncate, extend
    const s = FS.open(`${MOUNT}/ops`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, new Uint8Array(PAGE_SIZE + 100).fill(0x42), 0, PAGE_SIZE + 100);
    FS.close(s);
    FS.truncate(`${MOUNT}/ops`, 50);
    const s2 = FS.open(`${MOUNT}/ops`, O.RDWR);
    FS.write(s2, new Uint8Array([1, 2, 3]), 0, 3, 50);
    FS.close(s2);

    syncAndUnmount(FS);
    const { FS: FS2 } = await mountTome(backend);

    expect(FS2.stat(`${MOUNT}/ops`).size).toBe(53);

    const buf = new Uint8Array(53);
    const fd2 = FS2.open(`${MOUNT}/ops`, O.RDONLY);
    FS2.read(fd2, buf, 0, 53);
    FS2.close(fd2);
    expect(buf[50]).toBe(1);
    expect(buf[51]).toBe(2);
    expect(buf[52]).toBe(3);
  });

  it("orphan pages at path used by new file in subdirectory @fast", async () => {
    // Orphan pages at /sub/file
    backend.writePage("/sub/file", 0, new Uint8Array(PAGE_SIZE).fill(0xcc));

    const { FS } = await mountTome(backend);

    // Create subdirectory and file at the orphan path
    FS.mkdir(`${MOUNT}/sub`);
    const fd = FS.open(`${MOUNT}/sub/file`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, new Uint8Array([10, 20, 30]), 0, 3);
    FS.close(fd);

    syncAndUnmount(FS);
    const { FS: FS2 } = await mountTome(backend);

    expect(FS2.stat(`${MOUNT}/sub/file`).size).toBe(3);
  });

  it("no orphan pages: create is not affected by cleanup call @fast", async () => {
    // No orphan pages — file creation should work normally
    const { FS } = await mountTome(backend);

    const fd = FS.open(`${MOUNT}/clean`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, new Uint8Array(500).fill(0x77), 0, 500);
    FS.close(fd);

    syncAndUnmount(FS);
    const { FS: FS2 } = await mountTome(backend);

    expect(FS2.stat(`${MOUNT}/clean`).size).toBe(500);
  });

  it("orphan cleanup under cache pressure @fast", async () => {
    // 3 orphan pages at path, 4-page cache
    backend.writePage("/pressure", 0, new Uint8Array(PAGE_SIZE).fill(0x11));
    backend.writePage("/pressure", 1, new Uint8Array(PAGE_SIZE).fill(0x22));
    backend.writePage("/pressure", 2, new Uint8Array(PAGE_SIZE).fill(0x33));

    const { FS } = await mountTome(backend, 4);

    // Create file and write enough to exercise cache eviction
    const fd = FS.open(`${MOUNT}/pressure`, O.RDWR | O.CREAT, 0o666);
    for (let i = 0; i < 6; i++) {
      const page = new Uint8Array(PAGE_SIZE);
      page[0] = i;
      FS.write(fd, page, 0, PAGE_SIZE, i * PAGE_SIZE);
    }
    FS.close(fd);

    syncAndUnmount(FS);
    const { FS: FS2 } = await mountTome(backend, 4);

    expect(FS2.stat(`${MOUNT}/pressure`).size).toBe(PAGE_SIZE * 6);

    // Verify data integrity
    const buf = new Uint8Array(1);
    const fd2 = FS2.open(`${MOUNT}/pressure`, O.RDONLY);
    for (let i = 0; i < 6; i++) {
      FS2.read(fd2, buf, 0, 1, i * PAGE_SIZE);
      expect(buf[0]).toBe(i);
    }
    FS2.close(fd2);
  });

  it("delete then recreate at same path cleans orphans @fast", async () => {
    const { FS } = await mountTome(backend);

    // Create a file, sync to persist
    const fd1 = FS.open(`${MOUNT}/recycle`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd1, new Uint8Array(PAGE_SIZE * 3).fill(0xab), 0, PAGE_SIZE * 3);
    FS.close(fd1);
    syncAndUnmount(FS);

    expect(backend.countPages("/recycle")).toBe(3);

    // Remount, delete, recreate smaller
    const { FS: FS2 } = await mountTome(backend);
    FS2.unlink(`${MOUNT}/recycle`);
    const fd2 = FS2.open(`${MOUNT}/recycle`, O.RDWR | O.CREAT, 0o666);
    FS2.write(fd2, new Uint8Array([42]), 0, 1);
    FS2.close(fd2);
    syncAndUnmount(FS2);

    // Only 1 page should remain
    expect(backend.countPages("/recycle")).toBe(1);

    const { FS: FS3 } = await mountTome(backend);
    expect(FS3.stat(`${MOUNT}/recycle`).size).toBe(1);
  });

  it("orphan cleanup preserves other files at different paths @fast", async () => {
    // Orphan pages at "/orphan"
    backend.writePage("/orphan", 0, new Uint8Array(PAGE_SIZE).fill(0xdd));

    // Legitimate file at "/keeper"
    const now = Date.now();
    backend.writeMeta("/keeper", {
      size: PAGE_SIZE,
      mode: 0o100644,
      ctime: now,
      mtime: now,
      atime: now,
    });
    backend.writePage("/keeper", 0, new Uint8Array(PAGE_SIZE).fill(0x99));

    const { FS } = await mountTome(backend);

    // Create new file at orphan path
    const fd = FS.open(`${MOUNT}/orphan`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, new Uint8Array([1]), 0, 1);
    FS.close(fd);

    // Keeper should be unaffected
    const buf = new Uint8Array(1);
    const kfd = FS.open(`${MOUNT}/keeper`, O.RDONLY);
    FS.read(kfd, buf, 0, 1, 0);
    FS.close(kfd);
    expect(buf[0]).toBe(0x99);

    syncAndUnmount(FS);

    // Verify keeper survives syncfs
    expect(backend.countPages("/keeper")).toBe(1);
    expect(backend.countPages("/orphan")).toBe(1);
  });

  it("orphan pages with metadata: create replaces both @fast", async () => {
    // Backend has stale metadata AND orphan pages (full ghost file)
    const now = Date.now();
    backend.writeMeta("/ghost", {
      size: PAGE_SIZE * 2,
      mode: 0o100644,
      ctime: now,
      mtime: now,
      atime: now,
    });
    backend.writePage("/ghost", 0, new Uint8Array(PAGE_SIZE).fill(0x11));
    backend.writePage("/ghost", 1, new Uint8Array(PAGE_SIZE).fill(0x22));

    const { FS } = await mountTome(backend);

    // The ghost file was restored by restoreTree. Delete it, then create new.
    FS.unlink(`${MOUNT}/ghost`);
    const fd = FS.open(`${MOUNT}/ghost`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, new Uint8Array([7, 8, 9]), 0, 3);
    FS.close(fd);

    syncAndUnmount(FS);
    const { FS: FS2 } = await mountTome(backend);

    expect(FS2.stat(`${MOUNT}/ghost`).size).toBe(3);
    const buf = new Uint8Array(3);
    const fd2 = FS2.open(`${MOUNT}/ghost`, O.RDONLY);
    FS2.read(fd2, buf, 0, 3);
    FS2.close(fd2);
    expect(buf).toEqual(new Uint8Array([7, 8, 9]));
  });
});
