/**
 * Adversarial tests for restoreTree sparse-file-extension crash recovery.
 *
 * When a sparse file (pages at non-contiguous indices) is extended past its
 * last-synced metadata size, and the extension page is flushed to the backend
 * via cache eviction before the process crashes, restoreTree must detect the
 * extra pages beyond the expected range.
 *
 * The bug: restoreTree's "fewer pages than expected" branch probes the last
 * expected page. If it exists (sparse file with gaps), the code trusts stale
 * metadata without checking for pages BEYOND the expected range. Pages written
 * by the extension are silently lost.
 *
 * This targets the seam between sparse file detection and crash recovery
 * in restoreTree (tomefs.ts).
 *
 * Ethos §2 (real POSIX semantics), §9 (adversarial — target the seams).
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

describe("restoreTree crash recovery: sparse file extended beyond metadata", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("detects extension page beyond sparse file's last expected page @fast", async () => {
    // Scenario: sparse file with metadata claiming 5 pages (indices 0-4),
    // only pages 0 and 4 stored (sparse: 1-3 missing), plus extension
    // page 5 flushed to backend before crash.
    //
    // countPages=3, pagesFromMeta=5 → "fewer pages" branch
    // Probe page 4 (last expected): exists → code trusts meta.size
    // BUG: page 5 (the extension) is lost.
    backend.writeMeta("/file", {
      size: PAGE_SIZE * 5,
      mode: 0o100666,
      ctime: Date.now(),
      mtime: Date.now(),
      atime: Date.now(),
    });
    backend.writePage("/file", 0, new Uint8Array(PAGE_SIZE).fill(0xaa));
    backend.writePage("/file", 4, new Uint8Array(PAGE_SIZE).fill(0xbb));

    const extPage = new Uint8Array(PAGE_SIZE);
    extPage.fill(0xcc);
    backend.writePage("/file", 5, extPage);

    const { FS } = await mountTome(backend);
    const stat = FS.stat(`${MOUNT}/file`);

    // After fix: detects page 5 via maxPageIndex and extends to 6 pages
    expect(stat.size).toBe(PAGE_SIZE * 6);

    // Verify extension page data is accessible
    const buf = new Uint8Array(PAGE_SIZE);
    const fd = FS.open(`${MOUNT}/file`, O.RDONLY);
    FS.read(fd, buf, 0, PAGE_SIZE, 5 * PAGE_SIZE);
    FS.close(fd);
    expect(buf).toEqual(extPage);
  });

  it("sparse file without extension still trusts metadata", async () => {
    // Sparse file: pages 0, 4 stored, meta says 5 pages. No extension.
    // Should trust meta.size for sub-page precision.
    backend.writeMeta("/sparse-ok", {
      size: PAGE_SIZE * 4 + 500,
      mode: 0o100666,
      ctime: Date.now(),
      mtime: Date.now(),
      atime: Date.now(),
    });
    // pagesFromMeta = ceil((PAGE_SIZE*4+500)/PAGE_SIZE) = 5
    // last expected page index = 4
    backend.writePage("/sparse-ok", 0, new Uint8Array(PAGE_SIZE).fill(0x11));
    backend.writePage("/sparse-ok", 4, new Uint8Array(PAGE_SIZE).fill(0x22));

    const { FS } = await mountTome(backend);
    const stat = FS.stat(`${MOUNT}/sparse-ok`);
    // No extension: maxPageIndex=4 == lastExpected=4 → trust meta.size
    expect(stat.size).toBe(PAGE_SIZE * 4 + 500);
  });

  it("sparse file with sub-page metadata and extension beyond range", async () => {
    // meta.size = PAGE_SIZE * 3 + 100 → pagesFromMeta = 4 (indices 0-3)
    // Stored: pages 0, 3, 5 → sparse, page 3 is last expected, page 5 is extension
    backend.writeMeta("/subpage", {
      size: PAGE_SIZE * 3 + 100,
      mode: 0o100666,
      ctime: Date.now(),
      mtime: Date.now(),
      atime: Date.now(),
    });
    backend.writePage("/subpage", 0, new Uint8Array(PAGE_SIZE).fill(0xaa));
    backend.writePage("/subpage", 3, new Uint8Array(PAGE_SIZE).fill(0xbb));
    backend.writePage("/subpage", 5, new Uint8Array(PAGE_SIZE).fill(0xdd));

    const { FS } = await mountTome(backend);
    const stat = FS.stat(`${MOUNT}/subpage`);
    // maxPageIndex=5 > lastExpected=3 → extension detected
    expect(stat.size).toBe(PAGE_SIZE * 6);
  });

  it("multiple sparse files: only those with extensions get size correction", async () => {
    // File A: sparse, no extension → trust meta
    backend.writeMeta("/a", {
      size: PAGE_SIZE * 4,
      mode: 0o100666,
      ctime: Date.now(),
      mtime: Date.now(),
      atime: Date.now(),
    });
    backend.writePage("/a", 0, new Uint8Array(PAGE_SIZE).fill(0x11));
    backend.writePage("/a", 3, new Uint8Array(PAGE_SIZE).fill(0x22));

    // File B: sparse WITH extension → recover
    backend.writeMeta("/b", {
      size: PAGE_SIZE * 4,
      mode: 0o100666,
      ctime: Date.now(),
      mtime: Date.now(),
      atime: Date.now(),
    });
    backend.writePage("/b", 0, new Uint8Array(PAGE_SIZE).fill(0x33));
    backend.writePage("/b", 3, new Uint8Array(PAGE_SIZE).fill(0x44));
    backend.writePage("/b", 5, new Uint8Array(PAGE_SIZE).fill(0x55));

    // File C: not sparse, clean → trust meta
    backend.writeMeta("/c", {
      size: PAGE_SIZE * 2,
      mode: 0o100666,
      ctime: Date.now(),
      mtime: Date.now(),
      atime: Date.now(),
    });
    backend.writePage("/c", 0, new Uint8Array(PAGE_SIZE).fill(0x66));
    backend.writePage("/c", 1, new Uint8Array(PAGE_SIZE).fill(0x77));

    const { FS } = await mountTome(backend);
    expect(FS.stat(`${MOUNT}/a`).size).toBe(PAGE_SIZE * 4);
    expect(FS.stat(`${MOUNT}/b`).size).toBe(PAGE_SIZE * 6);
    expect(FS.stat(`${MOUNT}/c`).size).toBe(PAGE_SIZE * 2);
  });

  it("extension with many pages beyond expected range", async () => {
    // Sparse file with 3 extension pages (simulating large write burst before crash)
    backend.writeMeta("/burst", {
      size: PAGE_SIZE * 3,
      mode: 0o100666,
      ctime: Date.now(),
      mtime: Date.now(),
      atime: Date.now(),
    });
    backend.writePage("/burst", 0, new Uint8Array(PAGE_SIZE).fill(0x01));
    backend.writePage("/burst", 2, new Uint8Array(PAGE_SIZE).fill(0x02));
    // Extension pages 3, 4, 5
    backend.writePage("/burst", 3, new Uint8Array(PAGE_SIZE).fill(0x03));
    backend.writePage("/burst", 4, new Uint8Array(PAGE_SIZE).fill(0x04));
    backend.writePage("/burst", 5, new Uint8Array(PAGE_SIZE).fill(0x05));

    const { FS } = await mountTome(backend);
    expect(FS.stat(`${MOUNT}/burst`).size).toBe(PAGE_SIZE * 6);
  });

  it("extension beyond missing last-expected page uses maxPageIndex", async () => {
    // Key case: highIdx > lastPageIndex AND the page at lastPageIndex
    // does NOT exist. The file extent is determined by maxPageIndex
    // regardless of whether the last expected page is present.
    //
    // meta.size = PAGE_SIZE * 4 → pagesFromMeta = 4, lastPageIndex = 3
    // Stored: pages 0, 5 only → countPages = 2 < 4
    // Page 3 (last expected) is MISSING, page 5 is extension
    // maxPageIndex = 5 > lastPageIndex = 3 → fileSize = 6 * PAGE_SIZE
    backend.writeMeta("/gap-ext", {
      size: PAGE_SIZE * 4,
      mode: 0o100666,
      ctime: Date.now(),
      mtime: Date.now(),
      atime: Date.now(),
    });
    backend.writePage("/gap-ext", 0, new Uint8Array(PAGE_SIZE).fill(0xaa));
    // Note: pages 1, 2, 3 are all missing — page 3 is the last expected
    backend.writePage("/gap-ext", 5, new Uint8Array(PAGE_SIZE).fill(0xee));

    const { FS } = await mountTome(backend);
    // maxPageIndex=5 > lastPageIndex=3 → extent is (5+1)*PAGE_SIZE
    expect(FS.stat(`${MOUNT}/gap-ext`).size).toBe(PAGE_SIZE * 6);

    // Verify extension page data is accessible
    const buf = new Uint8Array(PAGE_SIZE);
    const fd = FS.open(`${MOUNT}/gap-ext`, O.RDONLY);
    FS.read(fd, buf, 0, PAGE_SIZE, 5 * PAGE_SIZE);
    FS.close(fd);
    expect(buf).toEqual(new Uint8Array(PAGE_SIZE).fill(0xee));
  });

  it("recovered sparse+extension file can be written and re-synced", async () => {
    // Use pagesFromMeta > countPages to trigger the sparse branch.
    // meta.size = PAGE_SIZE * 5 → pagesFromMeta = 5
    // Stored: pages 0, 4 (sparse, count=2) + page 6 (extension, count=3)
    // 3 < 5 → sparse branch, probe page 4: exists, maxPageIndex=6 > 4
    backend.writeMeta("/rw", {
      size: PAGE_SIZE * 5,
      mode: 0o100666,
      ctime: Date.now(),
      mtime: Date.now(),
      atime: Date.now(),
    });
    backend.writePage("/rw", 0, new Uint8Array(PAGE_SIZE).fill(0xaa));
    backend.writePage("/rw", 4, new Uint8Array(PAGE_SIZE).fill(0xbb));
    backend.writePage("/rw", 6, new Uint8Array(PAGE_SIZE).fill(0xcc)); // extension

    // Mount — should recover to PAGE_SIZE * 7
    const { FS, tomefs } = await mountTome(backend);
    expect(FS.stat(`${MOUNT}/rw`).size).toBe(PAGE_SIZE * 7);

    // Write more data and sync cleanly
    const fd = FS.open(`${MOUNT}/rw`, O.RDWR);
    const marker = new TextEncoder().encode("recovered!");
    FS.write(fd, marker, 0, marker.length);
    FS.close(fd);

    // Sync and unmount
    tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
      if (err) throw err;
    });
    FS.unmount(MOUNT);

    // Remount and verify clean state
    const { FS: FS2 } = await mountTome(backend);
    expect(FS2.stat(`${MOUNT}/rw`).size).toBe(PAGE_SIZE * 7);

    const buf = new Uint8Array(marker.length);
    const fd2 = FS2.open(`${MOUNT}/rw`, O.RDONLY);
    FS2.read(fd2, buf, 0, marker.length);
    FS2.close(fd2);
    expect(new TextDecoder().decode(buf)).toBe("recovered!");
  });
});
