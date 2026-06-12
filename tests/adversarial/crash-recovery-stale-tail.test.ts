/**
 * Adversarial tests: Stale tail bytes after crash recovery.
 *
 * After a dirty shutdown, backend pages may retain non-zero data beyond
 * the file extent. This happens when:
 *   1. A file is written/extended (dirtying a page with data beyond the
 *      synced file size)
 *   2. The page is evicted from cache (writing it to the backend)
 *   3. The file is truncated back (zeroTailAfterTruncate zeros the tail
 *      in cache, marks dirty)
 *   4. Crash before the dirty page is flushed
 *
 * Result: backend page has stale data beyond the file's metadata size.
 * On recovery, extending the file would expose that stale data instead
 * of POSIX-required zeros.
 *
 * restoreTree now zeros stale tail bytes in the backend during dirty
 * recovery to prevent this. These tests verify that fix by directly
 * injecting stale data into the backend (simulating the eviction that
 * would occur naturally) and verifying it's cleaned up on mount.
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

async function mountTome(backend: SyncMemoryBackend, maxPages = 32) {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  const Module = await createModule();
  const FS = Module.FS;
  const tomefs = createTomeFS(FS, { backend, maxPages });
  FS.mkdir("/tome");
  FS.mount(tomefs, {}, "/tome");
  return { FS, tomefs, Module };
}

function syncfs(FS: any, tomefs: any) {
  tomefs.syncfs(FS.lookupPath("/tome").node.mount, false, (err: any) => {
    if (err) throw err;
  });
}

function syncAndUnmount(FS: any, tomefs: any) {
  syncfs(FS, tomefs);
  FS.unmount("/tome");
}

/**
 * Inject stale data into a backend page beyond a given offset.
 * Simulates what happens when a dirty page is evicted to the backend
 * and then the file is truncated (but the truncation's dirty page
 * isn't flushed before crash).
 */
function injectStaleTail(
  backend: SyncMemoryBackend,
  storagePath: string,
  pageIndex: number,
  validBytes: number,
  staleValue: number,
) {
  const page = backend.readPage(storagePath, pageIndex);
  if (!page) throw new Error(`No page at ${storagePath}:${pageIndex}`);
  page.fill(staleValue, validBytes);
  backend.writePage(storagePath, pageIndex, page);
}

/**
 * Remove the clean-shutdown marker from the backend, simulating a
 * dirty shutdown where syncfs never completed.
 */
function removeCleanMarker(backend: SyncMemoryBackend) {
  try {
    backend.deleteMeta("/__tomefs_clean");
  } catch {
    // Marker may not exist
  }
}

const MOUNT = "/tome";

describe("adversarial: crash recovery stale tail bytes", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("stale tail zeroed during dirty recovery + ftruncate extend @fast", async () => {
    // Establish: file at 5000 bytes, sync cleanly
    const m1 = await mountTome(backend);
    const s1 = m1.FS.open(`${MOUNT}/file`, O.RDWR | O.CREAT, 0o666);
    const data = new Uint8Array(5000);
    data.fill(0xAA);
    m1.FS.write(s1, data, 0, data.length, 0);
    m1.FS.close(s1);
    syncAndUnmount(m1.FS, m1.tomefs);

    // Inject stale data: simulate evicted dirty page with data beyond 5000
    const storagePath = backend.listFiles().find(p => !p.startsWith("/__"))!;
    injectStaleTail(backend, storagePath, 0, 5000, 0xFF);
    removeCleanMarker(backend);

    // Dirty recovery — restoreTree should zero the stale tail
    const m2 = await mountTome(backend);
    expect(m2.FS.stat(`${MOUNT}/file`).size).toBe(5000);

    // Extend via ftruncate
    const s2 = m2.FS.open(`${MOUNT}/file`, O.RDWR, 0o666);
    m2.FS.ftruncate(s2.fd, PAGE_SIZE);

    // Gap bytes should be zeros, not 0xFF
    const gapBuf = new Uint8Array(PAGE_SIZE - 5000);
    m2.FS.read(s2, gapBuf, 0, gapBuf.length, 5000);
    for (let i = 0; i < gapBuf.length; i++) {
      if (gapBuf[i] !== 0) {
        throw new Error(
          `Byte ${5000 + i}: expected 0x00, got 0x${gapBuf[i].toString(16)} — stale data leaked`,
        );
      }
    }

    // Preserved prefix should still have original data
    const prefBuf = new Uint8Array(5000);
    m2.FS.read(s2, prefBuf, 0, 5000, 0);
    for (let i = 0; i < 5000; i++) expect(prefBuf[i]).toBe(0xAA);

    m2.FS.close(s2);
    syncAndUnmount(m2.FS, m2.tomefs);
  });

  it("stale tail zeroed during dirty recovery + write extend", async () => {
    // Establish: file at 3000 bytes
    const m1 = await mountTome(backend);
    const s1 = m1.FS.open(`${MOUNT}/file`, O.RDWR | O.CREAT, 0o666);
    const data = new Uint8Array(3000);
    data.fill(0xBB);
    m1.FS.write(s1, data, 0, data.length, 0);
    m1.FS.close(s1);
    syncAndUnmount(m1.FS, m1.tomefs);

    // Inject stale data beyond 3000 and remove clean marker
    const storagePath = backend.listFiles().find(p => !p.startsWith("/__"))!;
    injectStaleTail(backend, storagePath, 0, 3000, 0xCC);
    removeCleanMarker(backend);

    // Dirty recovery + extend by writing at end of page
    const m2 = await mountTome(backend);
    const s2 = m2.FS.open(`${MOUNT}/file`, O.RDWR, 0o666);
    const marker = new Uint8Array([0x42]);
    m2.FS.write(s2, marker, 0, 1, PAGE_SIZE - 1);

    // Gap between 3000 and PAGE_SIZE-1 should be zeros
    const gapLen = PAGE_SIZE - 1 - 3000;
    const gapBuf = new Uint8Array(gapLen);
    m2.FS.read(s2, gapBuf, 0, gapLen, 3000);
    for (let i = 0; i < gapLen; i++) {
      if (gapBuf[i] !== 0) {
        throw new Error(
          `Byte ${3000 + i}: expected 0x00, got 0x${gapBuf[i].toString(16)} — stale data after write extend`,
        );
      }
    }
    m2.FS.close(s2);
    syncAndUnmount(m2.FS, m2.tomefs);
  });

  it("clean shutdown: no stale tail issue (zeroed during sync)", async () => {
    // File at 5000 bytes, sync cleanly → backend page tail is already zero
    const m1 = await mountTome(backend);
    const s1 = m1.FS.open(`${MOUNT}/file`, O.RDWR | O.CREAT, 0o666);
    const data = new Uint8Array(5000);
    data.fill(0xCC);
    m1.FS.write(s1, data, 0, data.length, 0);
    m1.FS.close(s1);
    syncAndUnmount(m1.FS, m1.tomefs);
    // Clean marker is present — no stale tail fix needed

    // Remount (clean recovery) and extend
    const m2 = await mountTome(backend);
    const s2 = m2.FS.open(`${MOUNT}/file`, O.RDWR, 0o666);
    m2.FS.ftruncate(s2.fd, PAGE_SIZE);

    const gapBuf = new Uint8Array(PAGE_SIZE - 5000);
    m2.FS.read(s2, gapBuf, 0, gapBuf.length, 5000);
    for (let i = 0; i < gapBuf.length; i++) {
      if (gapBuf[i] !== 0) {
        throw new Error(
          `Byte ${5000 + i}: expected 0x00 after clean recovery, got 0x${gapBuf[i].toString(16)}`,
        );
      }
    }
    m2.FS.close(s2);
    syncAndUnmount(m2.FS, m2.tomefs);
  });

  it("stale tail fix persists: second mount after fix is clean", async () => {
    // Establish + inject stale data + dirty recovery
    const m1 = await mountTome(backend);
    const s1 = m1.FS.open(`${MOUNT}/file`, O.RDWR | O.CREAT, 0o666);
    const data = new Uint8Array(6000);
    data.fill(0xDD);
    m1.FS.write(s1, data, 0, data.length, 0);
    m1.FS.close(s1);
    syncAndUnmount(m1.FS, m1.tomefs);

    const storagePath = backend.listFiles().find(p => !p.startsWith("/__"))!;
    injectStaleTail(backend, storagePath, 0, 6000, 0xEE);
    removeCleanMarker(backend);

    // First mount: dirty recovery zeros the tail + sync
    const m2 = await mountTome(backend);
    syncAndUnmount(m2.FS, m2.tomefs);

    // Second mount: clean recovery, no stale data
    const m3 = await mountTome(backend);
    const s3 = m3.FS.open(`${MOUNT}/file`, O.RDWR, 0o666);
    m3.FS.ftruncate(s3.fd, PAGE_SIZE);
    const gapBuf = new Uint8Array(PAGE_SIZE - 6000);
    m3.FS.read(s3, gapBuf, 0, gapBuf.length, 6000);
    for (let i = 0; i < gapBuf.length; i++) {
      if (gapBuf[i] !== 0) {
        throw new Error(
          `Byte ${6000 + i}: stale data survived across syncfs cycle`,
        );
      }
    }
    m3.FS.close(s3);
    syncAndUnmount(m3.FS, m3.tomefs);
  });

  it("multiple files with stale tails all zeroed @fast", async () => {
    // Create 3 files with different non-page-aligned sizes
    const m1 = await mountTome(backend);
    const sizes = [1000, 3000, 5000];
    for (let f = 0; f < 3; f++) {
      const s = m1.FS.open(`${MOUNT}/file${f}`, O.RDWR | O.CREAT, 0o666);
      const d = new Uint8Array(sizes[f]);
      d.fill(0xA0 + f);
      m1.FS.write(s, d, 0, d.length, 0);
      m1.FS.close(s);
    }
    syncAndUnmount(m1.FS, m1.tomefs);

    // Inject stale data into all files
    const paths = backend.listFiles().filter(p => !p.startsWith("/__"));
    paths.sort();
    for (let f = 0; f < 3; f++) {
      injectStaleTail(backend, paths[f], 0, sizes[f], 0xF0 + f);
    }
    removeCleanMarker(backend);

    // Dirty recovery + verify each file
    const m2 = await mountTome(backend);
    for (let f = 0; f < 3; f++) {
      const s = m2.FS.open(`${MOUNT}/file${f}`, O.RDWR, 0o666);
      expect(m2.FS.stat(`${MOUNT}/file${f}`).size).toBe(sizes[f]);
      m2.FS.ftruncate(s.fd, PAGE_SIZE);

      const gapBuf = new Uint8Array(PAGE_SIZE - sizes[f]);
      m2.FS.read(s, gapBuf, 0, gapBuf.length, sizes[f]);
      for (let i = 0; i < gapBuf.length; i++) {
        if (gapBuf[i] !== 0) {
          throw new Error(
            `file${f} byte ${sizes[f] + i}: expected 0x00, got 0x${gapBuf[i].toString(16)}`,
          );
        }
      }
      m2.FS.close(s);
    }
    syncAndUnmount(m2.FS, m2.tomefs);
  });

  it("page-aligned file not affected by stale tail fix @fast", async () => {
    // Page-aligned files have no tail — the fix should be a no-op
    const m1 = await mountTome(backend);
    const s1 = m1.FS.open(`${MOUNT}/file`, O.RDWR | O.CREAT, 0o666);
    const data = new Uint8Array(PAGE_SIZE);
    data.fill(0xFF);
    m1.FS.write(s1, data, 0, data.length, 0);
    m1.FS.close(s1);
    syncAndUnmount(m1.FS, m1.tomefs);

    removeCleanMarker(backend);

    // Dirty recovery — file is page-aligned, fix is a no-op
    const m2 = await mountTome(backend);
    expect(m2.FS.stat(`${MOUNT}/file`).size).toBe(PAGE_SIZE);
    const s2 = m2.FS.open(`${MOUNT}/file`, O.RDONLY, 0o666);
    const buf = new Uint8Array(PAGE_SIZE);
    m2.FS.read(s2, buf, 0, PAGE_SIZE, 0);
    for (let i = 0; i < PAGE_SIZE; i++) expect(buf[i]).toBe(0xFF);
    m2.FS.close(s2);
    syncAndUnmount(m2.FS, m2.tomefs);
  });

  it("stale tail under cache pressure (4-page cache) @fast", async () => {
    // With a tiny cache, restoreTree's backend reads don't pollute the cache
    const m1 = await mountTome(backend, 4);
    const sizes = [1000, 2000, 3000, 4000, 5000, 6000];
    for (let f = 0; f < 6; f++) {
      const s = m1.FS.open(`${MOUNT}/f${f}`, O.RDWR | O.CREAT, 0o666);
      const d = new Uint8Array(sizes[f]);
      d.fill(0xA0 + f);
      m1.FS.write(s, d, 0, d.length, 0);
      m1.FS.close(s);
    }
    syncAndUnmount(m1.FS, m1.tomefs);

    // Inject stale data into all
    const paths = backend.listFiles().filter(p => !p.startsWith("/__"));
    paths.sort();
    for (let f = 0; f < 6; f++) {
      injectStaleTail(backend, paths[f], 0, sizes[f], 0xB0 + f);
    }
    removeCleanMarker(backend);

    // Dirty recovery with 4-page cache
    const m2 = await mountTome(backend, 4);
    for (let f = 0; f < 6; f++) {
      const s = m2.FS.open(`${MOUNT}/f${f}`, O.RDWR, 0o666);
      m2.FS.ftruncate(s.fd, PAGE_SIZE);
      const gapBuf = new Uint8Array(PAGE_SIZE - sizes[f]);
      m2.FS.read(s, gapBuf, 0, gapBuf.length, sizes[f]);
      for (let i = 0; i < gapBuf.length; i++) {
        if (gapBuf[i] !== 0) {
          throw new Error(
            `f${f} byte ${sizes[f] + i}: expected 0x00, got 0x${gapBuf[i].toString(16)} — stale data under cache pressure`,
          );
        }
      }
      m2.FS.close(s);
    }
    syncAndUnmount(m2.FS, m2.tomefs);
  });

  it("multi-page file: stale tail on last page zeroed", async () => {
    // File spans 3 pages with non-aligned end
    const fileSize = PAGE_SIZE * 2 + 4000;
    const m1 = await mountTome(backend);
    const s1 = m1.FS.open(`${MOUNT}/file`, O.RDWR | O.CREAT, 0o666);
    const data = new Uint8Array(fileSize);
    data.fill(0x55);
    m1.FS.write(s1, data, 0, data.length, 0);
    m1.FS.close(s1);
    syncAndUnmount(m1.FS, m1.tomefs);

    // Inject stale data on the last page (page 2), beyond offset 4000
    const storagePath = backend.listFiles().find(p => !p.startsWith("/__"))!;
    injectStaleTail(backend, storagePath, 2, 4000, 0x77);
    removeCleanMarker(backend);

    // Dirty recovery
    const m2 = await mountTome(backend);
    expect(m2.FS.stat(`${MOUNT}/file`).size).toBe(fileSize);

    const s2 = m2.FS.open(`${MOUNT}/file`, O.RDWR, 0o666);
    m2.FS.ftruncate(s2.fd, PAGE_SIZE * 3);

    // First 2 pages should be fully 0x55
    const page0 = new Uint8Array(PAGE_SIZE);
    m2.FS.read(s2, page0, 0, PAGE_SIZE, 0);
    for (let i = 0; i < PAGE_SIZE; i++) expect(page0[i]).toBe(0x55);
    const page1 = new Uint8Array(PAGE_SIZE);
    m2.FS.read(s2, page1, 0, PAGE_SIZE, PAGE_SIZE);
    for (let i = 0; i < PAGE_SIZE; i++) expect(page1[i]).toBe(0x55);

    // First 4000 bytes of page 2: 0x55
    const validPart = new Uint8Array(4000);
    m2.FS.read(s2, validPart, 0, 4000, PAGE_SIZE * 2);
    for (let i = 0; i < 4000; i++) expect(validPart[i]).toBe(0x55);

    // Tail of page 2: zeros (not 0x77)
    const tailBuf = new Uint8Array(PAGE_SIZE - 4000);
    m2.FS.read(s2, tailBuf, 0, tailBuf.length, PAGE_SIZE * 2 + 4000);
    for (let i = 0; i < tailBuf.length; i++) {
      if (tailBuf[i] !== 0) {
        throw new Error(
          `Page 2 tail byte ${i}: expected 0x00, got 0x${tailBuf[i].toString(16)}`,
        );
      }
    }
    m2.FS.close(s2);
    syncAndUnmount(m2.FS, m2.tomefs);
  });

  it("no stale data when clean marker present", async () => {
    // Even with stale data in the backend, clean marker means the fix
    // should NOT run (the data was consistent at last sync)
    const m1 = await mountTome(backend);
    const s1 = m1.FS.open(`${MOUNT}/file`, O.RDWR | O.CREAT, 0o666);
    const data = new Uint8Array(5000);
    data.fill(0xDD);
    m1.FS.write(s1, data, 0, data.length, 0);
    m1.FS.close(s1);
    syncAndUnmount(m1.FS, m1.tomefs);

    // Inject stale data BUT keep the clean marker
    const storagePath = backend.listFiles().find(p => !p.startsWith("/__"))!;
    injectStaleTail(backend, storagePath, 0, 5000, 0xEE);
    // DON'T remove clean marker — it indicates a clean shutdown

    // Mount with clean marker: fix should not run, stale data visible
    // (This verifies the fix is conditional on dirty recovery)
    const m2 = await mountTome(backend);
    const s2 = m2.FS.open(`${MOUNT}/file`, O.RDWR, 0o666);
    m2.FS.ftruncate(s2.fd, PAGE_SIZE);
    const gapBuf = new Uint8Array(PAGE_SIZE - 5000);
    m2.FS.read(s2, gapBuf, 0, gapBuf.length, 5000);
    // With clean marker, the data should NOT be zeroed by the fix
    // (in real usage, a clean shutdown guarantees the tail is already zero)
    // Here we injected artificial stale data to verify the fix doesn't fire
    let hasNonZero = false;
    for (let i = 0; i < gapBuf.length; i++) {
      if (gapBuf[i] !== 0) { hasNonZero = true; break; }
    }
    expect(hasNonZero).toBe(true);
    m2.FS.close(s2);
    syncAndUnmount(m2.FS, m2.tomefs);
  });

  it("empty file: no crash", async () => {
    // Empty files (0 bytes) should not trigger the fix
    const m1 = await mountTome(backend);
    m1.FS.open(`${MOUNT}/empty`, O.WRONLY | O.CREAT, 0o666);
    syncAndUnmount(m1.FS, m1.tomefs);

    removeCleanMarker(backend);

    const m2 = await mountTome(backend);
    expect(m2.FS.stat(`${MOUNT}/empty`).size).toBe(0);
    syncAndUnmount(m2.FS, m2.tomefs);
  });
});
