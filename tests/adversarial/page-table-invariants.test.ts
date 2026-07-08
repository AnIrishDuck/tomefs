/**
 * Adversarial tests for per-node page table (_pages) invariant validation.
 *
 * tomefs.assertInvariants() validates that live (non-evicted) entries in the
 * per-node _pages array are consistent: correct path, correct pageIndex, within
 * file bounds, and present in the page cache. These tests exercise complex
 * lifecycle sequences that modify storagePath, usedBytes, and cache contents,
 * verifying that invariants hold throughout.
 *
 * Targets: _pages path/index consistency after rename, _pages bound consistency
 * after truncate/extend, _pages cache consistency after eviction+reload, and
 * cross-operation invariant preservation during multi-step PGlite-like workloads.
 *
 * Ethos §9: adversarial testing targeting page cache seams.
 */

import { describe, it, expect } from "vitest";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { createTomeFS } from "../../src/tomefs.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { PAGE_SIZE } from "../../src/types.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

const O = {
  RDONLY: 0,
  WRONLY: 1,
  RDWR: 2,
  CREAT: 64,
  TRUNC: 512,
  APPEND: 1024,
} as const;

const MOUNT = "/tome";

async function createHarness(maxPages = 8) {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  const Module = await createModule();
  const FS = Module.FS;
  const backend = new SyncMemoryBackend();
  const tomefs = createTomeFS(FS, { backend, maxPages });
  FS.mkdir(MOUNT);
  FS.mount(tomefs, {}, MOUNT);
  return { FS, backend, tomefs };
}

function syncfs(FS: any, tomefs: any) {
  tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
    if (err) throw err;
  });
}

function filledBuffer(size: number, value: number): Uint8Array {
  const buf = new Uint8Array(size);
  buf.fill(value);
  return buf;
}

describe("adversarial: _pages invariant validation through lifecycle", () => {
  describe("create → write → rename → verify invariants", () => {
    it("invariants hold after rename clears _pages @fast", async () => {
      const { FS, tomefs } = await createHarness();

      const fd = FS.open(`${MOUNT}/src`, O.RDWR | O.CREAT, 0o666);
      FS.write(fd, filledBuffer(PAGE_SIZE * 3, 0xaa), 0, PAGE_SIZE * 3, 0);
      FS.close(fd);
      tomefs.assertInvariants();

      FS.rename(`${MOUNT}/src`, `${MOUNT}/dst`);
      tomefs.assertInvariants();

      const fd2 = FS.open(`${MOUNT}/dst`, O.RDONLY);
      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(fd2, buf, 0, PAGE_SIZE, 0);
      expect(buf[0]).toBe(0xaa);
      FS.close(fd2);
      tomefs.assertInvariants();
    });

    it("invariants hold after rename-over-existing clears target _pages @fast", async () => {
      const { FS, tomefs } = await createHarness();

      const fd1 = FS.open(`${MOUNT}/a`, O.RDWR | O.CREAT, 0o666);
      FS.write(fd1, filledBuffer(PAGE_SIZE * 2, 0x11), 0, PAGE_SIZE * 2, 0);
      FS.close(fd1);

      const fd2 = FS.open(`${MOUNT}/b`, O.RDWR | O.CREAT, 0o666);
      FS.write(fd2, filledBuffer(PAGE_SIZE, 0x22), 0, PAGE_SIZE, 0);
      FS.close(fd2);
      tomefs.assertInvariants();

      FS.rename(`${MOUNT}/a`, `${MOUNT}/b`);
      tomefs.assertInvariants();

      const fd3 = FS.open(`${MOUNT}/b`, O.RDONLY);
      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(fd3, buf, 0, PAGE_SIZE, 0);
      expect(buf[0]).toBe(0x11);
      FS.close(fd3);
      tomefs.assertInvariants();
    });
  });

  describe("truncate → extend → verify bounds invariants", () => {
    it("invariants hold through truncate-to-zero then extend @fast", async () => {
      const { FS, tomefs } = await createHarness();

      const fd = FS.open(`${MOUNT}/f`, O.RDWR | O.CREAT, 0o666);
      FS.write(fd, filledBuffer(PAGE_SIZE * 4, 0xbb), 0, PAGE_SIZE * 4, 0);
      tomefs.assertInvariants();

      FS.ftruncate(fd.fd, 0);
      tomefs.assertInvariants();

      FS.write(fd, filledBuffer(PAGE_SIZE, 0xcc), 0, PAGE_SIZE, PAGE_SIZE * 2);
      tomefs.assertInvariants();

      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(fd, buf, 0, PAGE_SIZE, 0);
      expect(buf[0]).toBe(0);

      FS.read(fd, buf, 0, PAGE_SIZE, PAGE_SIZE * 2);
      expect(buf[0]).toBe(0xcc);

      FS.close(fd);
      tomefs.assertInvariants();
    });

    it("invariants hold through repeated truncate-extend oscillation @fast", async () => {
      const { FS, tomefs } = await createHarness();

      const fd = FS.open(`${MOUNT}/osc`, O.RDWR | O.CREAT, 0o666);

      for (let i = 0; i < 5; i++) {
        FS.write(fd, filledBuffer(PAGE_SIZE * 3, i + 1), 0, PAGE_SIZE * 3, 0);
        tomefs.assertInvariants();

        FS.ftruncate(fd.fd, PAGE_SIZE / 2);
        tomefs.assertInvariants();

        FS.write(fd, filledBuffer(PAGE_SIZE * 2, i + 0x10), 0, PAGE_SIZE * 2, 0);
        tomefs.assertInvariants();
      }

      FS.close(fd);
      tomefs.assertInvariants();
    });
  });

  describe("eviction pressure → reload → verify cache consistency", () => {
    it("invariants hold when pages are evicted and reloaded @fast", async () => {
      const { FS, tomefs } = await createHarness(3);

      const fd = FS.open(`${MOUNT}/target`, O.RDWR | O.CREAT, 0o666);
      FS.write(fd, filledBuffer(PAGE_SIZE * 2, 0xdd), 0, PAGE_SIZE * 2, 0);
      tomefs.assertInvariants();

      for (let i = 0; i < 5; i++) {
        const other = FS.open(`${MOUNT}/fill_${i}`, O.RDWR | O.CREAT, 0o666);
        FS.write(other, filledBuffer(PAGE_SIZE * 3, i + 1), 0, PAGE_SIZE * 3, 0);
        FS.close(other);
      }
      tomefs.assertInvariants();

      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(fd, buf, 0, PAGE_SIZE, 0);
      expect(buf[0]).toBe(0xdd);
      tomefs.assertInvariants();

      FS.read(fd, buf, 0, PAGE_SIZE, PAGE_SIZE);
      expect(buf[0]).toBe(0xdd);
      tomefs.assertInvariants();

      FS.close(fd);
      tomefs.assertInvariants();
    });

    it("invariants hold during read-write-evict-read cycling @fast", async () => {
      const { FS, tomefs } = await createHarness(2);

      const fd = FS.open(`${MOUNT}/cycle`, O.RDWR | O.CREAT, 0o666);

      for (let round = 0; round < 4; round++) {
        FS.write(fd, filledBuffer(PAGE_SIZE, 0x50 + round), 0, PAGE_SIZE, round * PAGE_SIZE);
        tomefs.assertInvariants();
      }

      for (let round = 3; round >= 0; round--) {
        const buf = new Uint8Array(PAGE_SIZE);
        FS.read(fd, buf, 0, PAGE_SIZE, round * PAGE_SIZE);
        expect(buf[0]).toBe(0x50 + round);
        tomefs.assertInvariants();
      }

      FS.close(fd);
      tomefs.assertInvariants();
    });
  });

  describe("multi-step PGlite-like lifecycle", () => {
    it("invariants hold through create → write → fsync → rename → truncate → syncfs → remount @fast", async () => {
      const { FS, backend, tomefs } = await createHarness(4);

      const fd = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);
      FS.write(fd, filledBuffer(PAGE_SIZE * 3, 0xee), 0, PAGE_SIZE * 3, 0);
      tomefs.assertInvariants();

      if (fd.stream_ops?.fsync) {
        fd.stream_ops.fsync(fd);
      }
      tomefs.assertInvariants();

      FS.close(fd);

      FS.rename(`${MOUNT}/wal`, `${MOUNT}/wal.archived`);
      tomefs.assertInvariants();

      const fd2 = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);
      FS.write(fd2, filledBuffer(PAGE_SIZE, 0xff), 0, PAGE_SIZE, 0);
      FS.close(fd2);
      tomefs.assertInvariants();

      const archived = FS.open(`${MOUNT}/wal.archived`, O.RDWR);
      FS.ftruncate(archived.fd, PAGE_SIZE);
      FS.close(archived);
      tomefs.assertInvariants();

      syncfs(FS, tomefs);
      tomefs.assertInvariants();

      FS.unmount(MOUNT);
      const tomefs2 = createTomeFS(FS, { backend, maxPages: 4 });
      FS.mount(tomefs2, {}, MOUNT);
      tomefs2.assertInvariants();

      const fdCheck = FS.open(`${MOUNT}/wal`, O.RDONLY);
      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(fdCheck, buf, 0, PAGE_SIZE, 0);
      expect(buf[0]).toBe(0xff);
      FS.close(fdCheck);

      const fdArch = FS.open(`${MOUNT}/wal.archived`, O.RDONLY);
      FS.read(fdArch, buf, 0, PAGE_SIZE, 0);
      expect(buf[0]).toBe(0xee);
      const stat = FS.fstat(fdArch.fd);
      expect(stat.size).toBe(PAGE_SIZE);
      FS.close(fdArch);
      tomefs2.assertInvariants();
    });

    it("invariants hold through unlink-while-open → close → syncfs lifecycle", async () => {
      const { FS, tomefs } = await createHarness(4);

      const fd = FS.open(`${MOUNT}/temp`, O.RDWR | O.CREAT, 0o666);
      FS.write(fd, filledBuffer(PAGE_SIZE * 2, 0x99), 0, PAGE_SIZE * 2, 0);
      tomefs.assertInvariants();

      FS.unlink(`${MOUNT}/temp`);
      tomefs.assertInvariants();

      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(fd, buf, 0, PAGE_SIZE, 0);
      expect(buf[0]).toBe(0x99);
      tomefs.assertInvariants();

      FS.close(fd);
      tomefs.assertInvariants();

      syncfs(FS, tomefs);
      tomefs.assertInvariants();
    });
  });

  describe("directory rename with file descendants", () => {
    it("invariants hold when directory rename updates descendant storagePaths @fast", async () => {
      const { FS, tomefs } = await createHarness();

      FS.mkdir(`${MOUNT}/dir`);
      FS.mkdir(`${MOUNT}/dir/sub`);

      const fd1 = FS.open(`${MOUNT}/dir/f1`, O.RDWR | O.CREAT, 0o666);
      FS.write(fd1, filledBuffer(PAGE_SIZE, 0x11), 0, PAGE_SIZE, 0);
      FS.close(fd1);

      const fd2 = FS.open(`${MOUNT}/dir/sub/f2`, O.RDWR | O.CREAT, 0o666);
      FS.write(fd2, filledBuffer(PAGE_SIZE * 2, 0x22), 0, PAGE_SIZE * 2, 0);
      FS.close(fd2);
      tomefs.assertInvariants();

      FS.rename(`${MOUNT}/dir`, `${MOUNT}/moved`);
      tomefs.assertInvariants();

      const buf = new Uint8Array(PAGE_SIZE);
      const fdCheck = FS.open(`${MOUNT}/moved/f1`, O.RDONLY);
      FS.read(fdCheck, buf, 0, PAGE_SIZE, 0);
      expect(buf[0]).toBe(0x11);
      FS.close(fdCheck);
      tomefs.assertInvariants();

      const fdCheck2 = FS.open(`${MOUNT}/moved/sub/f2`, O.RDONLY);
      FS.read(fdCheck2, buf, 0, PAGE_SIZE, 0);
      expect(buf[0]).toBe(0x22);
      FS.close(fdCheck2);
      tomefs.assertInvariants();
    });
  });

  describe("allocate → fsync → verify invariants", () => {
    it("invariants hold after allocate extends file without writing data @fast", async () => {
      const { FS, tomefs } = await createHarness();

      const fd = FS.open(`${MOUNT}/alloc`, O.RDWR | O.CREAT, 0o666);
      FS.write(fd, filledBuffer(PAGE_SIZE, 0x44), 0, PAGE_SIZE, 0);
      tomefs.assertInvariants();

      fd.stream_ops.allocate(fd, 0, PAGE_SIZE * 10);
      tomefs.assertInvariants();

      fd.stream_ops.fsync(fd);
      tomefs.assertInvariants();

      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(fd, buf, 0, PAGE_SIZE, 0);
      expect(buf[0]).toBe(0x44);

      FS.read(fd, buf, 0, PAGE_SIZE, PAGE_SIZE * 5);
      expect(buf[0]).toBe(0);

      FS.close(fd);
      tomefs.assertInvariants();
    });
  });
});
