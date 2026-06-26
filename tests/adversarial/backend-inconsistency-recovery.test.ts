/**
 * Adversarial tests: restoreTree + syncfs recovery from inconsistent backend state.
 *
 * Production backends (IndexedDB, OPFS) can reach inconsistent states through
 * crashes, quota failures, or partial writes. These tests directly construct
 * inconsistent backend states and verify that the mount → operate → syncfs →
 * remount cycle produces a correct, usable filesystem.
 *
 * Unlike syncfs-partial-crash.test.ts (which simulates crashes during syncfs)
 * and restore-recovery.test.ts (which tests page count mismatches), these
 * tests target structural inconsistencies: missing parent directories, orphan
 * metadata without pages, stale deletion markers, and mixed-corruption states
 * that arise from real multi-step failure modes.
 *
 * Ethos §9: "Write tests designed to break tomefs specifically — target the
 * seams: metadata updates after flush, dirty flush ordering"
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

const S_IFDIR = 0o040000;
const S_IFREG = 0o100000;
const S_IFLNK = 0o120000;

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

function syncfs(FS: any, tomefs: any) {
  tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
    if (err) throw err;
  });
}

function syncAndUnmount(FS: any, tomefs: any) {
  syncfs(FS, tomefs);
  FS.unmount(MOUNT);
}

describe("restoreTree: metadata without pages (phantom files)", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("recovers file with metadata but no pages as empty @fast", async () => {
    // Simulate: metadata claims size=PAGE_SIZE but no pages exist.
    // This happens if syncfs writes metadata but crashes before writing
    // pages (metadata-first ordering, or pages in a separate transaction).
    const now = Date.now();
    backend.writeMeta("/phantom", {
      size: PAGE_SIZE,
      mode: S_IFREG | 0o666,
      ctime: now,
      mtime: now,
      atime: now,
    });

    const { FS } = await mountTome(backend);
    const stat = FS.stat(`${MOUNT}/phantom`);
    expect(stat.size).toBe(0);
  });

  it("phantom file is writable after recovery", async () => {
    const now = Date.now();
    backend.writeMeta("/phantom", {
      size: PAGE_SIZE * 3,
      mode: S_IFREG | 0o666,
      ctime: now,
      mtime: now,
      atime: now,
    });

    const { FS, tomefs } = await mountTome(backend);
    expect(FS.stat(`${MOUNT}/phantom`).size).toBe(0);

    // Write data to the recovered file
    const data = new Uint8Array(1024);
    for (let i = 0; i < data.length; i++) data[i] = (i * 7) & 0xff;
    const s = FS.open(`${MOUNT}/phantom`, O.RDWR, 0o666);
    FS.write(s, data, 0, data.length);
    FS.close(s);

    expect(FS.stat(`${MOUNT}/phantom`).size).toBe(1024);

    // Sync + remount: verify data persists correctly
    syncAndUnmount(FS, tomefs);
    const { FS: FS2 } = await mountTome(backend);
    expect(FS2.stat(`${MOUNT}/phantom`).size).toBe(1024);

    const buf = new Uint8Array(1024);
    const s2 = FS2.open(`${MOUNT}/phantom`, O.RDONLY);
    FS2.read(s2, buf, 0, 1024);
    FS2.close(s2);
    expect(buf).toEqual(data);
  });

  it("multi-page metadata with partial pages recovers from highest page", async () => {
    // Metadata says 5 pages, but only page 0 and page 2 exist.
    // restoreTree should recover size from maxPageIndex (2) → 3 pages.
    const now = Date.now();
    backend.writeMeta("/partial", {
      size: PAGE_SIZE * 5,
      mode: S_IFREG | 0o666,
      ctime: now,
      mtime: now,
      atime: now,
    });
    const p0 = new Uint8Array(PAGE_SIZE);
    p0.fill(0xaa);
    backend.writePage("/partial", 0, p0);
    const p2 = new Uint8Array(PAGE_SIZE);
    p2.fill(0xbb);
    backend.writePage("/partial", 2, p2);

    const { FS } = await mountTome(backend);
    const stat = FS.stat(`${MOUNT}/partial`);
    // maxPageIndex = 2, lastPageIndex from meta = 4.
    // highIdx(2) < lastPageIndex(4) → size = (2+1) * PAGE_SIZE
    expect(stat.size).toBe(PAGE_SIZE * 3);

    // Verify page data
    const buf = new Uint8Array(PAGE_SIZE);
    const s = FS.open(`${MOUNT}/partial`, O.RDONLY);
    FS.read(s, buf, 0, PAGE_SIZE, 0);
    expect(buf[0]).toBe(0xaa);
    FS.read(s, buf, 0, PAGE_SIZE, PAGE_SIZE * 2);
    expect(buf[0]).toBe(0xbb);
    // Gap page (page 1) reads as zeros
    FS.read(s, buf, 0, PAGE_SIZE, PAGE_SIZE);
    expect(buf).toEqual(new Uint8Array(PAGE_SIZE));
    FS.close(s);
  });
});

describe("restoreTree: missing parent directory metadata", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("drops file when parent directory metadata is missing @fast", async () => {
    // Simulate: file metadata at "/subdir/file" exists but "/subdir" metadata
    // is missing. This can happen if directory metadata write failed during
    // syncfs but child file metadata succeeded (non-atomic metadata writes).
    const now = Date.now();
    backend.writeMeta("/subdir/file", {
      size: 100,
      mode: S_IFREG | 0o666,
      ctime: now,
      mtime: now,
      atime: now,
    });
    backend.writePage("/subdir/file", 0, new Uint8Array(PAGE_SIZE).fill(0x42));

    const { FS } = await mountTome(backend);

    // File should not be accessible (parent doesn't exist)
    expect(() => FS.stat(`${MOUNT}/subdir/file`)).toThrow();
    expect(() => FS.stat(`${MOUNT}/subdir`)).toThrow();
  });

  it("orphaned child is cleaned up on syncfs with orphan cleanup", async () => {
    // Set up: orphaned child file (no parent dir)
    const now = Date.now();
    backend.writeMeta("/orphandir/child", {
      size: PAGE_SIZE,
      mode: S_IFREG | 0o666,
      ctime: now,
      mtime: now,
      atime: now,
    });
    backend.writePage("/orphandir/child", 0, new Uint8Array(PAGE_SIZE).fill(0xcc));

    const { FS, tomefs } = await mountTome(backend);

    // Create a real file to trigger a non-noop syncfs
    const s = FS.open(`${MOUNT}/real`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, new Uint8Array([1, 2, 3]), 0, 3);
    FS.close(s);

    // Force a rename to trigger needsOrphanCleanup (full tree walk)
    FS.rename(`${MOUNT}/real`, `${MOUNT}/real2`);
    syncAndUnmount(FS, tomefs);

    // Orphaned metadata and pages should be cleaned up
    expect(backend.readMeta("/orphandir/child")).toBeNull();
    expect(backend.countPages("/orphandir/child")).toBe(0);
  });

  it("deeply nested file without ancestor directories is dropped", async () => {
    const now = Date.now();
    // Only leaf file metadata, no parent/grandparent directory metadata
    backend.writeMeta("/a/b/c/deep", {
      size: 10,
      mode: S_IFREG | 0o666,
      ctime: now,
      mtime: now,
      atime: now,
    });

    const { FS } = await mountTome(backend);
    expect(() => FS.stat(`${MOUNT}/a`)).toThrow();
    expect(() => FS.stat(`${MOUNT}/a/b/c/deep`)).toThrow();
  });

  it("file with parent dir present but grandparent missing is dropped", async () => {
    const now = Date.now();
    // /a metadata is missing, /a/b exists, /a/b/file exists
    backend.writeMeta("/a/b", {
      size: 0,
      mode: S_IFDIR | 0o755,
      ctime: now,
      mtime: now,
      atime: now,
    });
    backend.writeMeta("/a/b/file", {
      size: 50,
      mode: S_IFREG | 0o666,
      ctime: now,
      mtime: now,
      atime: now,
    });

    const { FS } = await mountTome(backend);
    // /a doesn't exist, so /a/b can't be created, so /a/b/file can't be created
    expect(() => FS.stat(`${MOUNT}/a`)).toThrow();
  });
});

describe("restoreTree: stale /__deleted_* markers", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("stale deletion markers trigger orphan cleanup on first syncfs @fast", async () => {
    // Simulate: process crashed after unlink moved pages to /__deleted_0
    // but before syncfs could clean up the marker.
    const now = Date.now();
    backend.writeMeta("/__deleted_0", {
      size: PAGE_SIZE,
      mode: S_IFREG | 0o666,
      ctime: now,
      mtime: now,
      atime: now,
    });
    backend.writePage("/__deleted_0", 0, new Uint8Array(PAGE_SIZE).fill(0xdd));

    // Also add a valid file so the FS isn't empty
    backend.writeMeta("/valid", {
      size: 10,
      mode: S_IFREG | 0o666,
      ctime: now,
      mtime: now,
      atime: now,
    });
    backend.writePage("/valid", 0, new Uint8Array(PAGE_SIZE));

    const { FS, tomefs } = await mountTome(backend);

    // Valid file should be accessible
    expect(FS.stat(`${MOUNT}/valid`).size).toBe(10);

    // /__deleted_0 should not appear as a regular file
    expect(() => FS.stat(`${MOUNT}/__deleted_0`)).toThrow();

    // Trigger orphan cleanup via rename (sets needsOrphanCleanup)
    const s = FS.open(`${MOUNT}/trigger`, O.RDWR | O.CREAT, 0o666);
    FS.close(s);
    FS.rename(`${MOUNT}/trigger`, `${MOUNT}/trigger2`);
    syncfs(FS, tomefs);

    // Stale deletion marker should be cleaned up
    expect(backend.readMeta("/__deleted_0")).toBeNull();
    expect(backend.countPages("/__deleted_0")).toBe(0);
  });

  it("multiple stale deletion markers are all cleaned up", async () => {
    const now = Date.now();
    for (let i = 0; i < 5; i++) {
      backend.writeMeta(`/__deleted_${i}`, {
        size: PAGE_SIZE,
        mode: S_IFREG | 0o666,
        ctime: now,
        mtime: now,
        atime: now,
      });
      backend.writePage(`/__deleted_${i}`, 0, new Uint8Array(PAGE_SIZE).fill(i));
    }

    const { FS, tomefs } = await mountTome(backend);

    // Force orphan cleanup path
    const s = FS.open(`${MOUNT}/x`, O.RDWR | O.CREAT, 0o666);
    FS.close(s);
    FS.rename(`${MOUNT}/x`, `${MOUNT}/y`);
    syncfs(FS, tomefs);

    for (let i = 0; i < 5; i++) {
      expect(backend.readMeta(`/__deleted_${i}`)).toBeNull();
      expect(backend.countPages(`/__deleted_${i}`)).toBe(0);
    }
  });

  it("stale markers disable clean-marker optimization on mount", async () => {
    // If clean marker + orphan markers both exist, needsOrphanCleanup stays true
    const now = Date.now();
    backend.writeMeta("/__tomefs_clean", {
      size: 0,
      mode: 0,
      ctime: now,
      mtime: now,
    });
    backend.writeMeta("/__deleted_99", {
      size: PAGE_SIZE,
      mode: S_IFREG | 0o666,
      ctime: now,
      mtime: now,
      atime: now,
    });
    backend.writePage("/__deleted_99", 0, new Uint8Array(PAGE_SIZE));

    // Add a real file
    backend.writeMeta("/real", {
      size: 5,
      mode: S_IFREG | 0o666,
      ctime: now,
      mtime: now,
      atime: now,
    });
    backend.writePage("/real", 0, new Uint8Array(PAGE_SIZE));

    const { FS, tomefs } = await mountTome(backend);

    // Write to dirty something
    const s = FS.open(`${MOUNT}/real`, O.RDWR);
    FS.write(s, new Uint8Array([99]), 0, 1, 0);
    FS.close(s);

    // Syncfs should use full tree walk (not incremental) because
    // orphan markers were present at mount
    syncfs(FS, tomefs);

    // Stats should show a full tree sync, not incremental
    const stats = tomefs.getStats();
    expect(stats.fullTreeSyncs).toBeGreaterThanOrEqual(1);

    // Orphan should be cleaned up
    expect(backend.readMeta("/__deleted_99")).toBeNull();
  });
});

describe("restoreTree: orphan pages without metadata", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("orphan pages are cleaned up by cleanupOrphanedPages during syncfs @fast", async () => {
    // Pages exist at "/orphan" but no metadata entry.
    // This happens when page eviction writes pages to the backend but
    // the process crashes before syncfs writes the metadata.
    backend.writePage("/orphan", 0, new Uint8Array(PAGE_SIZE).fill(0xee));
    backend.writePage("/orphan", 1, new Uint8Array(PAGE_SIZE).fill(0xff));

    // Add a valid file with both metadata and pages
    const now = Date.now();
    backend.writeMeta("/valid", {
      size: PAGE_SIZE,
      mode: S_IFREG | 0o666,
      ctime: now,
      mtime: now,
      atime: now,
    });
    backend.writePage("/valid", 0, new Uint8Array(PAGE_SIZE).fill(0x11));

    const { FS, tomefs } = await mountTome(backend);

    // Valid file works
    expect(FS.stat(`${MOUNT}/valid`).size).toBe(PAGE_SIZE);

    // Orphan pages shouldn't be visible as a file
    expect(() => FS.stat(`${MOUNT}/orphan`)).toThrow();

    // Force full tree sync to trigger cleanupOrphanedPages
    const s = FS.open(`${MOUNT}/trigger`, O.RDWR | O.CREAT, 0o666);
    FS.close(s);
    FS.rename(`${MOUNT}/trigger`, `${MOUNT}/trigger2`);
    syncfs(FS, tomefs);

    // Orphan pages should be cleaned up
    expect(backend.countPages("/orphan")).toBe(0);
  });

  it("orphan pages don't interfere with new file at same path", async () => {
    // Orphan pages at "/reused" from prior crash
    backend.writePage("/reused", 0, new Uint8Array(PAGE_SIZE).fill(0xba));

    const { FS, tomefs } = await mountTome(backend);

    // Create a new file at the same path
    const s = FS.open(`${MOUNT}/reused`, O.RDWR | O.CREAT, 0o666);
    const fresh = new Uint8Array(100);
    fresh.fill(0x42);
    FS.write(s, fresh, 0, 100);
    FS.close(s);

    // Read back — should see fresh data, not orphan data
    const buf = new Uint8Array(100);
    const s2 = FS.open(`${MOUNT}/reused`, O.RDONLY);
    FS.read(s2, buf, 0, 100);
    FS.close(s2);
    expect(buf).toEqual(fresh);

    // Sync + remount
    syncAndUnmount(FS, tomefs);
    const { FS: FS2 } = await mountTome(backend);
    expect(FS2.stat(`${MOUNT}/reused`).size).toBe(100);
  });
});

describe("restoreTree: corrupt or unusual metadata values", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("unknown mode type is silently skipped @fast", async () => {
    // Metadata with a mode that isn't S_IFREG, S_IFDIR, or S_IFLNK
    const now = Date.now();
    backend.writeMeta("/unknown", {
      size: 100,
      mode: 0o060000 | 0o666, // block device mode
      ctime: now,
      mtime: now,
      atime: now,
    });

    // Add a valid file so the FS isn't empty
    backend.writeMeta("/normal", {
      size: 5,
      mode: S_IFREG | 0o666,
      ctime: now,
      mtime: now,
      atime: now,
    });
    backend.writePage("/normal", 0, new Uint8Array(PAGE_SIZE));

    const { FS } = await mountTome(backend);

    // Valid file should work
    expect(FS.stat(`${MOUNT}/normal`).size).toBe(5);

    // Unknown-mode entry should not exist
    expect(() => FS.stat(`${MOUNT}/unknown`)).toThrow();
  });

  it("file with zero-size metadata and no pages is empty", async () => {
    const now = Date.now();
    backend.writeMeta("/empty", {
      size: 0,
      mode: S_IFREG | 0o666,
      ctime: now,
      mtime: now,
      atime: now,
    });

    const { FS } = await mountTome(backend);
    expect(FS.stat(`${MOUNT}/empty`).size).toBe(0);
  });

  it("metadata without atime falls back to mtime", async () => {
    const mtime = 1700000000000;
    backend.writeMeta("/noatime", {
      size: 0,
      mode: S_IFREG | 0o666,
      ctime: mtime,
      mtime: mtime,
      // atime deliberately omitted
    });

    const { FS } = await mountTome(backend);
    const stat = FS.stat(`${MOUNT}/noatime`);
    // Emscripten returns Date objects, convert to ms
    const atime = stat.atime.getTime();
    expect(atime).toBe(mtime);
  });

  it("symlink metadata restores correctly", async () => {
    const now = Date.now();
    backend.writeMeta("/mylink", {
      size: 0,
      mode: S_IFLNK | 0o777,
      ctime: now,
      mtime: now,
      atime: now,
      link: "/some/target",
    });

    const { FS } = await mountTome(backend);
    const target = FS.readlink(`${MOUNT}/mylink`);
    expect(target).toBe("/some/target");
  });

  it("symlink without link field restores as empty string target", async () => {
    const now = Date.now();
    backend.writeMeta("/badlink", {
      size: 0,
      mode: S_IFLNK | 0o777,
      ctime: now,
      mtime: now,
      atime: now,
      // link deliberately omitted
    });

    const { FS } = await mountTome(backend);
    const target = FS.readlink(`${MOUNT}/badlink`);
    expect(target).toBe("");
  });
});

describe("recovery: mixed inconsistencies in a single backend", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("handles a mix of valid, phantom, and orphan entries @fast", async () => {
    const now = Date.now();

    // Valid directory
    backend.writeMeta("/dir", {
      size: 0,
      mode: S_IFDIR | 0o755,
      ctime: now,
      mtime: now,
      atime: now,
    });

    // Valid file with correct metadata + pages
    backend.writeMeta("/dir/good", {
      size: PAGE_SIZE,
      mode: S_IFREG | 0o666,
      ctime: now,
      mtime: now,
      atime: now,
    });
    const goodData = new Uint8Array(PAGE_SIZE);
    goodData.fill(0x11);
    backend.writePage("/dir/good", 0, goodData);

    // Phantom file: metadata but no pages
    backend.writeMeta("/dir/phantom", {
      size: PAGE_SIZE * 2,
      mode: S_IFREG | 0o666,
      ctime: now,
      mtime: now,
      atime: now,
    });

    // File with extra pages beyond metadata
    backend.writeMeta("/dir/extended", {
      size: PAGE_SIZE,
      mode: S_IFREG | 0o666,
      ctime: now,
      mtime: now,
      atime: now,
    });
    backend.writePage("/dir/extended", 0, new Uint8Array(PAGE_SIZE).fill(0x22));
    backend.writePage("/dir/extended", 1, new Uint8Array(PAGE_SIZE).fill(0x33));
    backend.writePage("/dir/extended", 2, new Uint8Array(PAGE_SIZE).fill(0x44));

    // Orphan pages (no metadata)
    backend.writePage("/dir/orphan", 0, new Uint8Array(PAGE_SIZE).fill(0xff));

    // Stale deletion marker
    backend.writeMeta("/__deleted_42", {
      size: PAGE_SIZE,
      mode: S_IFREG | 0o666,
      ctime: now,
      mtime: now,
      atime: now,
    });
    backend.writePage("/__deleted_42", 0, new Uint8Array(PAGE_SIZE));

    // Orphan child (no parent dir)
    backend.writeMeta("/missing_parent/child", {
      size: 50,
      mode: S_IFREG | 0o666,
      ctime: now,
      mtime: now,
      atime: now,
    });

    const { FS, tomefs } = await mountTome(backend);

    // Good file is intact
    expect(FS.stat(`${MOUNT}/dir/good`).size).toBe(PAGE_SIZE);
    const buf = new Uint8Array(PAGE_SIZE);
    const s = FS.open(`${MOUNT}/dir/good`, O.RDONLY);
    FS.read(s, buf, 0, PAGE_SIZE);
    FS.close(s);
    expect(buf).toEqual(goodData);

    // Phantom file recovered as empty
    expect(FS.stat(`${MOUNT}/dir/phantom`).size).toBe(0);

    // Extended file recovered with all pages
    expect(FS.stat(`${MOUNT}/dir/extended`).size).toBe(PAGE_SIZE * 3);

    // Orphan file not visible
    expect(() => FS.stat(`${MOUNT}/dir/orphan`)).toThrow();

    // Orphan child not visible
    expect(() => FS.stat(`${MOUNT}/missing_parent`)).toThrow();

    // Now sync + remount to verify cleanup
    // Force full tree walk via rename
    FS.rename(`${MOUNT}/dir/phantom`, `${MOUNT}/dir/phantom2`);
    syncAndUnmount(FS, tomefs);

    // Verify cleanup
    expect(backend.readMeta("/__deleted_42")).toBeNull();
    expect(backend.countPages("/__deleted_42")).toBe(0);
    expect(backend.readMeta("/missing_parent/child")).toBeNull();
    expect(backend.countPages("/dir/orphan")).toBe(0);

    // Remount — everything should be clean
    const { FS: FS2, tomefs: t2 } = await mountTome(backend);
    expect(FS2.stat(`${MOUNT}/dir/good`).size).toBe(PAGE_SIZE);
    expect(FS2.stat(`${MOUNT}/dir/phantom2`).size).toBe(0);
    expect(FS2.stat(`${MOUNT}/dir/extended`).size).toBe(PAGE_SIZE * 3);

    // Verify invariants
    t2.assertInvariants();
  });
});

describe("recovery: post-recovery operations produce correct state", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("write + truncate + rename on recovered files works correctly", async () => {
    const now = Date.now();

    // Set up inconsistent state: phantom file + extended file
    backend.writeMeta("/dir", {
      size: 0,
      mode: S_IFDIR | 0o755,
      ctime: now,
      mtime: now,
      atime: now,
    });
    backend.writeMeta("/dir/phantom", {
      size: PAGE_SIZE * 4,
      mode: S_IFREG | 0o666,
      ctime: now,
      mtime: now,
      atime: now,
    });
    // No pages for phantom — recovered as empty

    backend.writeMeta("/dir/extended", {
      size: PAGE_SIZE,
      mode: S_IFREG | 0o666,
      ctime: now,
      mtime: now,
      atime: now,
    });
    backend.writePage("/dir/extended", 0, new Uint8Array(PAGE_SIZE).fill(0xaa));
    backend.writePage("/dir/extended", 1, new Uint8Array(PAGE_SIZE).fill(0xbb));

    const { FS, tomefs } = await mountTome(backend);

    // Write to phantom file
    const data = new Uint8Array(500);
    for (let i = 0; i < 500; i++) data[i] = (i * 13) & 0xff;
    const s1 = FS.open(`${MOUNT}/dir/phantom`, O.RDWR, 0o666);
    FS.write(s1, data, 0, 500);
    FS.close(s1);
    expect(FS.stat(`${MOUNT}/dir/phantom`).size).toBe(500);

    // Truncate extended file
    FS.truncate(`${MOUNT}/dir/extended`, PAGE_SIZE);
    expect(FS.stat(`${MOUNT}/dir/extended`).size).toBe(PAGE_SIZE);

    // Rename phantom → moved
    FS.rename(`${MOUNT}/dir/phantom`, `${MOUNT}/dir/moved`);
    expect(FS.stat(`${MOUNT}/dir/moved`).size).toBe(500);
    expect(() => FS.stat(`${MOUNT}/dir/phantom`)).toThrow();

    // Sync + remount
    syncAndUnmount(FS, tomefs);
    const { FS: FS2, tomefs: t2 } = await mountTome(backend);

    // Verify everything persisted
    expect(FS2.stat(`${MOUNT}/dir/moved`).size).toBe(500);
    expect(FS2.stat(`${MOUNT}/dir/extended`).size).toBe(PAGE_SIZE);
    expect(() => FS2.stat(`${MOUNT}/dir/phantom`)).toThrow();

    // Verify data integrity
    const buf = new Uint8Array(500);
    const s2 = FS2.open(`${MOUNT}/dir/moved`, O.RDONLY);
    FS2.read(s2, buf, 0, 500);
    FS2.close(s2);
    expect(buf).toEqual(data);

    t2.assertInvariants();
  });

  it("unlink of recovered files cleans up correctly", async () => {
    const now = Date.now();
    backend.writeMeta("/todelete", {
      size: PAGE_SIZE,
      mode: S_IFREG | 0o666,
      ctime: now,
      mtime: now,
      atime: now,
    });
    // File has pages beyond metadata (recovery extends)
    backend.writePage("/todelete", 0, new Uint8Array(PAGE_SIZE).fill(0x11));
    backend.writePage("/todelete", 1, new Uint8Array(PAGE_SIZE).fill(0x22));

    const { FS, tomefs } = await mountTome(backend);
    expect(FS.stat(`${MOUNT}/todelete`).size).toBe(PAGE_SIZE * 2);

    // Unlink the recovered file
    FS.unlink(`${MOUNT}/todelete`);
    expect(() => FS.stat(`${MOUNT}/todelete`)).toThrow();

    // Sync + verify cleanup
    syncAndUnmount(FS, tomefs);
    expect(backend.readMeta("/todelete")).toBeNull();
    expect(backend.countPages("/todelete")).toBe(0);
  });

  it("new file at orphan path: orphan pages cleaned up on file creation", async () => {
    // Orphan pages at a path are cleaned up when a new file is created
    // at the same path. createNode calls backend.deleteFile() to remove
    // stale pages before the new file's data is written, preventing
    // restoreTree from misinterpreting orphan pages as the new file's data.

    // Orphan pages at "/reborn"
    backend.writePage("/reborn", 0, new Uint8Array(PAGE_SIZE).fill(0xdd));
    backend.writePage("/reborn", 1, new Uint8Array(PAGE_SIZE).fill(0xee));

    const { FS, tomefs } = await mountTome(backend);

    // Create a brand-new file at the orphan's path
    const fresh = new Uint8Array(200);
    for (let i = 0; i < 200; i++) fresh[i] = (i * 3) & 0xff;
    const s = FS.open(`${MOUNT}/reborn`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, fresh, 0, 200);
    FS.close(s);

    // Sync + remount
    syncAndUnmount(FS, tomefs);
    const { FS: FS2 } = await mountTome(backend);

    // Fresh data at offset 0 is correct
    const buf = new Uint8Array(200);
    const s2 = FS2.open(`${MOUNT}/reborn`, O.RDONLY);
    FS2.read(s2, buf, 0, 200);
    FS2.close(s2);
    expect(buf).toEqual(fresh);

    // File size matches the written data — orphan pages were cleaned up.
    expect(FS2.stat(`${MOUNT}/reborn`).size).toBe(200);
  });

  it("orphan pages at unused path are cleaned up by full tree sync", async () => {
    // Orphan pages at a path not used by any live file — these are properly
    // cleaned up by cleanupOrphanedPages during full tree walk syncfs.
    backend.writePage("/unused_orphan", 0, new Uint8Array(PAGE_SIZE).fill(0xaa));
    backend.writePage("/unused_orphan", 1, new Uint8Array(PAGE_SIZE).fill(0xbb));

    const { FS, tomefs } = await mountTome(backend);

    // Create a file at a different path and force full tree walk
    const fd = FS.open(`${MOUNT}/real`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, new Uint8Array([1, 2, 3]), 0, 3);
    FS.close(fd);
    FS.rename(`${MOUNT}/real`, `${MOUNT}/real2`);
    syncAndUnmount(FS, tomefs);

    // Orphan pages should be cleaned up
    expect(backend.countPages("/unused_orphan")).toBe(0);
  });
});

describe("recovery: clean marker + orphan interaction", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("clean marker without orphans enables incremental syncfs @fast", async () => {
    const now = Date.now();
    backend.writeMeta("/__tomefs_clean", {
      size: 0,
      mode: 0,
      ctime: now,
      mtime: now,
    });
    backend.writeMeta("/file", {
      size: 10,
      mode: S_IFREG | 0o666,
      ctime: now,
      mtime: now,
      atime: now,
    });
    backend.writePage("/file", 0, new Uint8Array(PAGE_SIZE));

    const { FS, tomefs } = await mountTome(backend);

    // Modify file
    const s = FS.open(`${MOUNT}/file`, O.RDWR);
    FS.write(s, new Uint8Array([99, 98, 97]), 0, 3, 0);
    FS.close(s);

    syncfs(FS, tomefs);

    // Should use incremental path (no full tree walk)
    const stats = tomefs.getStats();
    expect(stats.incrementalSyncs).toBe(1);
    expect(stats.fullTreeSyncs).toBe(0);
  });

  it("clean marker with /__deleted_ entries forces full tree walk", async () => {
    const now = Date.now();
    backend.writeMeta("/__tomefs_clean", {
      size: 0,
      mode: 0,
      ctime: now,
      mtime: now,
    });
    backend.writeMeta("/__deleted_0", {
      size: PAGE_SIZE,
      mode: S_IFREG | 0o666,
      ctime: now,
      mtime: now,
      atime: now,
    });
    backend.writeMeta("/file", {
      size: 10,
      mode: S_IFREG | 0o666,
      ctime: now,
      mtime: now,
      atime: now,
    });
    backend.writePage("/file", 0, new Uint8Array(PAGE_SIZE));

    const { FS, tomefs } = await mountTome(backend);

    // Modify file to trigger syncfs work
    const s = FS.open(`${MOUNT}/file`, O.RDWR);
    FS.write(s, new Uint8Array([1]), 0, 1, 0);
    FS.close(s);

    syncfs(FS, tomefs);

    // Should use full tree walk (orphans detected)
    const stats = tomefs.getStats();
    expect(stats.fullTreeSyncs).toBe(1);
    expect(stats.incrementalSyncs).toBe(0);
  });
});

describe("recovery: backend state is fully consistent after sync cycle", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("corrupted backend becomes clean after mount + syncfs + remount @fast", async () => {
    const now = Date.now();

    // Set up a messy backend with multiple inconsistencies
    // Valid directory and file
    backend.writeMeta("/data", {
      size: 0,
      mode: S_IFDIR | 0o755,
      ctime: now,
      mtime: now,
      atime: now,
    });
    backend.writeMeta("/data/file1", {
      size: PAGE_SIZE + 100,
      mode: S_IFREG | 0o666,
      ctime: now,
      mtime: now,
      atime: now,
    });
    backend.writePage("/data/file1", 0, new Uint8Array(PAGE_SIZE).fill(0x11));
    backend.writePage("/data/file1", 1, new Uint8Array(PAGE_SIZE).fill(0x22));

    // Orphan pages
    backend.writePage("/ghost", 0, new Uint8Array(PAGE_SIZE).fill(0xff));
    backend.writePage("/ghost", 1, new Uint8Array(PAGE_SIZE).fill(0xfe));

    // Stale deletion marker
    backend.writeMeta("/__deleted_5", {
      size: PAGE_SIZE,
      mode: S_IFREG | 0o666,
      ctime: now,
      mtime: now,
      atime: now,
    });
    backend.writePage("/__deleted_5", 0, new Uint8Array(PAGE_SIZE));

    const { FS, tomefs } = await mountTome(backend);

    // Force full tree walk
    const s = FS.open(`${MOUNT}/data/file1`, O.RDWR);
    FS.write(s, new Uint8Array([0x99]), 0, 1, 0);
    FS.close(s);

    // Trigger rename to force orphan cleanup
    const s2 = FS.open(`${MOUNT}/tmp`, O.RDWR | O.CREAT, 0o666);
    FS.close(s2);
    FS.rename(`${MOUNT}/tmp`, `${MOUNT}/tmp2`);
    FS.unlink(`${MOUNT}/tmp2`);

    syncAndUnmount(FS, tomefs);

    // Verify backend is clean
    expect(backend.readMeta("/__deleted_5")).toBeNull();
    expect(backend.countPages("/__deleted_5")).toBe(0);
    expect(backend.countPages("/ghost")).toBe(0);

    // Clean marker should be present
    expect(backend.readMeta("/__tomefs_clean")).not.toBeNull();

    // Remount from clean backend — should use incremental path
    const { FS: FS2, tomefs: t2 } = await mountTome(backend);
    expect(FS2.stat(`${MOUNT}/data/file1`).size).toBe(PAGE_SIZE + 100);

    // Modify and sync — should be incremental (no full tree walk needed)
    const s3 = FS2.open(`${MOUNT}/data/file1`, O.RDWR);
    FS2.write(s3, new Uint8Array([0x88]), 0, 1, 0);
    FS2.close(s3);

    syncfs(FS2, t2);
    const stats = t2.getStats();
    expect(stats.incrementalSyncs).toBe(1);
    expect(stats.fullTreeSyncs).toBe(0);

    t2.assertInvariants();
  });
});

describe("recovery: cache pressure during recovery operations", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("operations on recovered files work under extreme cache pressure @fast", async () => {
    const now = Date.now();

    // Set up multiple files with page mismatches
    backend.writeMeta("/dir", {
      size: 0,
      mode: S_IFDIR | 0o755,
      ctime: now,
      mtime: now,
      atime: now,
    });

    for (let i = 0; i < 5; i++) {
      // Each file has metadata claiming 1 page but 3 pages in backend
      backend.writeMeta(`/dir/f${i}`, {
        size: PAGE_SIZE,
        mode: S_IFREG | 0o666,
        ctime: now,
        mtime: now,
        atime: now,
      });
      for (let p = 0; p < 3; p++) {
        const data = new Uint8Array(PAGE_SIZE);
        data.fill((i * 16 + p) & 0xff);
        backend.writePage(`/dir/f${i}`, p, data);
      }
    }

    // Mount with only 4 pages in cache (extreme pressure for 5×3=15 pages)
    const { FS, tomefs } = await mountTome(backend, 4);

    // All files recovered to 3 pages
    for (let i = 0; i < 5; i++) {
      expect(FS.stat(`${MOUNT}/dir/f${i}`).size).toBe(PAGE_SIZE * 3);
    }

    // Read from each file under pressure — forces eviction
    for (let i = 0; i < 5; i++) {
      const buf = new Uint8Array(PAGE_SIZE);
      const s = FS.open(`${MOUNT}/dir/f${i}`, O.RDONLY);
      FS.read(s, buf, 0, PAGE_SIZE, PAGE_SIZE * 2); // read last page
      FS.close(s);
      expect(buf[0]).toBe((i * 16 + 2) & 0xff);
    }

    // Write to some files and verify
    for (let i = 0; i < 3; i++) {
      const s = FS.open(`${MOUNT}/dir/f${i}`, O.RDWR);
      const marker = new Uint8Array([0xde, 0xad]);
      FS.write(s, marker, 0, 2, 0);
      FS.close(s);
    }

    // Sync under pressure + remount
    syncAndUnmount(FS, tomefs);
    const { FS: FS2, tomefs: t2 } = await mountTome(backend, 4);

    // Verify writes persisted correctly
    for (let i = 0; i < 3; i++) {
      const buf = new Uint8Array(2);
      const s = FS2.open(`${MOUNT}/dir/f${i}`, O.RDONLY);
      FS2.read(s, buf, 0, 2, 0);
      FS2.close(s);
      expect(buf[0]).toBe(0xde);
      expect(buf[1]).toBe(0xad);
    }

    t2.assertInvariants();
  });
});
