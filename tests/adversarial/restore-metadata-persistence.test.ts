/**
 * Adversarial tests: restoreTree metadata persistence after size correction.
 *
 * When restoreTree adjusts file sizes due to page/metadata mismatch (crash
 * recovery), the corrected metadata must be persisted on the next syncfs.
 * Without this, every mount re-does the same correction — wasted work and
 * a window for inconsistency if the correction logic ever changes.
 *
 * These tests verify the full cycle:
 *   1. Backend has mismatched pages/metadata (simulated crash)
 *   2. Mount → restoreTree corrects file sizes
 *   3. syncfs → corrected metadata written to backend
 *   4. Remount → no further correction needed (stable)
 *
 * Ethos §9: "Every bug found here becomes a new regression test."
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

function syncAndUnmount(FS: any, tomefs: any) {
  tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
    if (err) throw err;
  });
  FS.unmount(MOUNT);
}

describe("restoreTree metadata persistence: crash extension (pages beyond metadata)", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("persists corrected size after extension recovery", async () => {
    // Phase 1: create 1-page file, sync
    const { FS, tomefs } = await mountTome(backend);
    const data = new Uint8Array(PAGE_SIZE);
    data.fill(0xaa);
    const s = FS.open(`${MOUNT}/file`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, PAGE_SIZE);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    expect(backend.readMeta("/file")!.size).toBe(PAGE_SIZE);

    // Simulate crash: extra page in backend, metadata not updated
    backend.writePage("/file", 1, new Uint8Array(PAGE_SIZE).fill(0xbb));

    // Phase 2: mount → recovery corrects size to 2 pages
    const { FS: FS2, tomefs: tomefs2 } = await mountTome(backend);
    expect(FS2.stat(`${MOUNT}/file`).size).toBe(PAGE_SIZE * 2);

    // Sync → corrected metadata should be written to backend
    syncAndUnmount(FS2, tomefs2);
    expect(backend.readMeta("/file")!.size).toBe(PAGE_SIZE * 2);

    // Phase 3: remount → no correction needed, metadata already correct
    const { FS: FS3 } = await mountTome(backend);
    expect(FS3.stat(`${MOUNT}/file`).size).toBe(PAGE_SIZE * 2);
  });

  it("persists corrected size for multi-page extension", async () => {
    // Create 2-page file, sync, then add pages 2-4
    const { FS, tomefs } = await mountTome(backend);
    const data = new Uint8Array(PAGE_SIZE * 2);
    data.fill(0x11);
    const s = FS.open(`${MOUNT}/big`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // Add 3 extra pages (simulating crash after write but before metadata sync)
    for (let p = 2; p <= 4; p++) {
      backend.writePage("/big", p, new Uint8Array(PAGE_SIZE).fill(p));
    }

    // Recovery → sync → verify backend metadata updated
    const { FS: FS2, tomefs: tomefs2 } = await mountTome(backend);
    expect(FS2.stat(`${MOUNT}/big`).size).toBe(PAGE_SIZE * 5);
    syncAndUnmount(FS2, tomefs2);
    expect(backend.readMeta("/big")!.size).toBe(PAGE_SIZE * 5);

    // Stable on remount
    const { FS: FS3 } = await mountTome(backend);
    expect(FS3.stat(`${MOUNT}/big`).size).toBe(PAGE_SIZE * 5);
  });
});

describe("restoreTree metadata persistence: crash truncation (pages below metadata)", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("persists corrected size after truncation recovery", async () => {
    // Create 3-page file, sync
    const { FS, tomefs } = await mountTome(backend);
    const data = new Uint8Array(PAGE_SIZE * 3);
    data.fill(0xcc);
    const s = FS.open(`${MOUNT}/file`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    expect(backend.readMeta("/file")!.size).toBe(PAGE_SIZE * 3);

    // Simulate crash: delete pages 1,2 (truncation not reflected in metadata)
    backend.deletePagesFrom("/file", 1);

    // Recovery → corrected to 1 page
    const { FS: FS2, tomefs: tomefs2 } = await mountTome(backend);
    expect(FS2.stat(`${MOUNT}/file`).size).toBe(PAGE_SIZE);

    // Sync → corrected metadata persisted
    syncAndUnmount(FS2, tomefs2);
    expect(backend.readMeta("/file")!.size).toBe(PAGE_SIZE);

    // Stable on remount
    const { FS: FS3 } = await mountTome(backend);
    expect(FS3.stat(`${MOUNT}/file`).size).toBe(PAGE_SIZE);
  });

  it("persists zero size when all pages lost", async () => {
    // Create 2-page file, sync, then delete all pages
    const { FS, tomefs } = await mountTome(backend);
    const data = new Uint8Array(PAGE_SIZE * 2);
    data.fill(0xdd);
    const s = FS.open(`${MOUNT}/gone`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // Delete all pages but keep metadata
    backend.deleteFile("/gone");
    backend.writeMeta("/gone", {
      size: PAGE_SIZE * 2,
      mode: 0o100666,
      ctime: Date.now(),
      mtime: Date.now(),
      atime: Date.now(),
    });

    // Recovery → size 0
    const { FS: FS2, tomefs: tomefs2 } = await mountTome(backend);
    expect(FS2.stat(`${MOUNT}/gone`).size).toBe(0);

    // Sync → metadata updated to size 0
    syncAndUnmount(FS2, tomefs2);
    expect(backend.readMeta("/gone")!.size).toBe(0);

    // Stable on remount
    const { FS: FS3 } = await mountTome(backend);
    expect(FS3.stat(`${MOUNT}/gone`).size).toBe(0);
  });

  it("persists sub-page correction when last page missing", async () => {
    // File with non-page-aligned size: PAGE_SIZE + 500
    const { FS, tomefs } = await mountTome(backend);
    const data = new Uint8Array(PAGE_SIZE + 500);
    data.fill(0xee);
    const s = FS.open(`${MOUNT}/sub`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    expect(backend.readMeta("/sub")!.size).toBe(PAGE_SIZE + 500);

    // Delete the partial last page
    backend.deletePagesFrom("/sub", 1);

    // Recovery → rounded down to 1 page
    const { FS: FS2, tomefs: tomefs2 } = await mountTome(backend);
    expect(FS2.stat(`${MOUNT}/sub`).size).toBe(PAGE_SIZE);

    // Sync → persisted
    syncAndUnmount(FS2, tomefs2);
    expect(backend.readMeta("/sub")!.size).toBe(PAGE_SIZE);

    // Stable on remount
    const { FS: FS3 } = await mountTome(backend);
    expect(FS3.stat(`${MOUNT}/sub`).size).toBe(PAGE_SIZE);
  });
});

describe("restoreTree metadata persistence: mixed and no-op cases", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("does not dirty files with correct metadata", async () => {
    // Create file, sync normally — metadata should match
    const { FS, tomefs } = await mountTome(backend);
    const data = new Uint8Array(PAGE_SIZE * 2);
    data.fill(0x42);
    const s = FS.open(`${MOUNT}/ok`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // Remount without any backend corruption
    const { FS: FS2, tomefs: tomefs2 } = await mountTome(backend);
    expect(FS2.stat(`${MOUNT}/ok`).size).toBe(PAGE_SIZE * 2);

    // Check stats: no dirty metadata means incremental sync path
    const stats = tomefs2.getStats();
    expect(stats.dirtyMetaCount).toBe(0);
  });

  it("handles mixed corrected and uncorrected files", async () => {
    // Create two files, sync
    const { FS, tomefs } = await mountTome(backend);
    for (const name of ["stable", "extended", "truncated"]) {
      const d = new Uint8Array(PAGE_SIZE * 2);
      d.fill(name.charCodeAt(0));
      const s = FS.open(`${MOUNT}/${name}`, O.RDWR | O.CREAT, 0o666);
      FS.write(s, d, 0, d.length);
      FS.close(s);
    }
    syncAndUnmount(FS, tomefs);

    // Corrupt only "extended" (add page) and "truncated" (remove page)
    backend.writePage("/extended", 2, new Uint8Array(PAGE_SIZE).fill(0xff));
    backend.deletePagesFrom("/truncated", 1);

    // Recovery
    const { FS: FS2, tomefs: tomefs2 } = await mountTome(backend);
    expect(FS2.stat(`${MOUNT}/stable`).size).toBe(PAGE_SIZE * 2);
    expect(FS2.stat(`${MOUNT}/extended`).size).toBe(PAGE_SIZE * 3);
    expect(FS2.stat(`${MOUNT}/truncated`).size).toBe(PAGE_SIZE);

    // Only corrected files should be dirty
    const stats = tomefs2.getStats();
    expect(stats.dirtyMetaCount).toBe(2);

    // Sync → all metadata correct in backend
    syncAndUnmount(FS2, tomefs2);
    expect(backend.readMeta("/stable")!.size).toBe(PAGE_SIZE * 2);
    expect(backend.readMeta("/extended")!.size).toBe(PAGE_SIZE * 3);
    expect(backend.readMeta("/truncated")!.size).toBe(PAGE_SIZE);

    // All stable on remount
    const { FS: FS3, tomefs: tomefs3 } = await mountTome(backend);
    expect(FS3.stat(`${MOUNT}/stable`).size).toBe(PAGE_SIZE * 2);
    expect(FS3.stat(`${MOUNT}/extended`).size).toBe(PAGE_SIZE * 3);
    expect(FS3.stat(`${MOUNT}/truncated`).size).toBe(PAGE_SIZE);
    expect(tomefs3.getStats().dirtyMetaCount).toBe(0);
  });

  it("correction persists even with cache pressure during syncfs", async () => {
    // Use a tiny 4-page cache with multiple corrected files
    const { FS, tomefs } = await mountTome(backend, 32);
    for (let i = 0; i < 5; i++) {
      const d = new Uint8Array(PAGE_SIZE * 3);
      d.fill(i);
      const s = FS.open(`${MOUNT}/f${i}`, O.RDWR | O.CREAT, 0o666);
      FS.write(s, d, 0, d.length);
      FS.close(s);
    }
    syncAndUnmount(FS, tomefs);

    // Extend all files by 1 page (simulated crash)
    for (let i = 0; i < 5; i++) {
      backend.writePage(`/f${i}`, 3, new Uint8Array(PAGE_SIZE).fill(0xf0 + i));
    }

    // Recovery with tiny cache (4 pages — forces eviction during sync)
    const { FS: FS2, tomefs: tomefs2 } = await mountTome(backend, 4);
    for (let i = 0; i < 5; i++) {
      expect(FS2.stat(`${MOUNT}/f${i}`).size).toBe(PAGE_SIZE * 4);
    }

    syncAndUnmount(FS2, tomefs2);

    // All corrected metadata persisted
    for (let i = 0; i < 5; i++) {
      expect(backend.readMeta(`/f${i}`)!.size).toBe(PAGE_SIZE * 4);
    }

    // Stable on remount
    const { FS: FS3, tomefs: tomefs3 } = await mountTome(backend, 4);
    for (let i = 0; i < 5; i++) {
      expect(FS3.stat(`${MOUNT}/f${i}`).size).toBe(PAGE_SIZE * 4);
    }
    expect(tomefs3.getStats().dirtyMetaCount).toBe(0);
  });

  it("operations on corrected files work correctly after recovery", async () => {
    // Create 1-page file, sync, then add extra page (crash simulation)
    const { FS, tomefs } = await mountTome(backend);
    const data = new Uint8Array(PAGE_SIZE);
    data.fill(0x55);
    const s = FS.open(`${MOUNT}/file`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, PAGE_SIZE);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    backend.writePage("/file", 1, new Uint8Array(PAGE_SIZE).fill(0x66));

    // Recovery
    const { FS: FS2, tomefs: tomefs2 } = await mountTome(backend);
    expect(FS2.stat(`${MOUNT}/file`).size).toBe(PAGE_SIZE * 2);

    // Write more data to the recovered file
    const extra = new Uint8Array(100);
    extra.fill(0x77);
    const s2 = FS2.open(`${MOUNT}/file`, O.WRONLY);
    FS2.llseek(s2, PAGE_SIZE * 2, 0);
    FS2.write(s2, extra, 0, 100);
    FS2.close(s2);

    expect(FS2.stat(`${MOUNT}/file`).size).toBe(PAGE_SIZE * 2 + 100);

    // Sync → both the recovery correction and the new write persisted
    syncAndUnmount(FS2, tomefs2);
    expect(backend.readMeta("/file")!.size).toBe(PAGE_SIZE * 2 + 100);

    // Stable on remount
    const { FS: FS3 } = await mountTome(backend);
    expect(FS3.stat(`${MOUNT}/file`).size).toBe(PAGE_SIZE * 2 + 100);
  });
});
