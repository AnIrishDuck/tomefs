/**
 * Tests for orphaned page cleanup in tomefs.
 *
 * Orphaned pages accumulate when a crash occurs between dirty page eviction
 * (which writes pages to the backend via writePage/writePages) and the next
 * syncfs (which writes metadata via syncAll). The pages persist in the
 * backend with no metadata entry, invisible to listFiles() and unreachable
 * via restoreTree.
 *
 * This test suite verifies:
 * 1. SyncMemoryBackend.cleanupOrphanedPages() finds and removes page-only entries
 * 2. tomefs's full-tree-walk syncfs calls cleanupOrphanedPages on the backend
 * 3. The orphansDeleted counter in TomeFSStats reflects page orphans
 * 4. Page orphans from simulated crash scenarios are cleaned up on remount
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
  return { FS, tomefs };
}

function syncfs(FS: any, tomefs: any, populate = false) {
  tomefs.syncfs(
    FS.lookupPath(MOUNT).node.mount,
    populate,
    (err: any) => {
      if (err) throw err;
    },
  );
}

describe("SyncMemoryBackend.cleanupOrphanedPages", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("returns 0 when no pages or metadata exist @fast", () => {
    expect(backend.cleanupOrphanedPages()).toBe(0);
  });

  it("returns 0 when all pages have metadata @fast", () => {
    const data = new Uint8Array(PAGE_SIZE);
    backend.writePage("/file", 0, data);
    backend.writePage("/file", 1, data);
    backend.writeMeta("/file", { size: PAGE_SIZE * 2, mode: 0o100644, ctime: 0, mtime: 0 });
    expect(backend.cleanupOrphanedPages()).toBe(0);
  });

  it("removes pages without metadata @fast", () => {
    const data = new Uint8Array(PAGE_SIZE);
    backend.writePage("/orphan", 0, data);
    backend.writePage("/orphan", 1, data);
    // No writeMeta for /orphan

    backend.writePage("/good", 0, data);
    backend.writeMeta("/good", { size: PAGE_SIZE, mode: 0o100644, ctime: 0, mtime: 0 });

    expect(backend.cleanupOrphanedPages()).toBe(1);

    // Orphaned pages deleted
    expect(backend.readPage("/orphan", 0)).toBeNull();
    expect(backend.readPage("/orphan", 1)).toBeNull();
    // Good file survives
    expect(backend.readPage("/good", 0)).not.toBeNull();
    expect(backend.readMeta("/good")).not.toBeNull();
  });

  it("removes multiple orphan paths @fast", () => {
    const data = new Uint8Array(PAGE_SIZE);
    backend.writePage("/orphan1", 0, data);
    backend.writePage("/orphan2", 0, data);
    backend.writePage("/orphan2", 1, data);
    backend.writePage("/orphan3", 0, data);

    expect(backend.cleanupOrphanedPages()).toBe(3);
    expect(backend.readPage("/orphan1", 0)).toBeNull();
    expect(backend.readPage("/orphan2", 0)).toBeNull();
    expect(backend.readPage("/orphan3", 0)).toBeNull();
  });

  it("is idempotent @fast", () => {
    const data = new Uint8Array(PAGE_SIZE);
    backend.writePage("/orphan", 0, data);

    expect(backend.cleanupOrphanedPages()).toBe(1);
    expect(backend.cleanupOrphanedPages()).toBe(0);
  });

  it("preserves invariants after cleanup @fast", () => {
    const data = new Uint8Array(PAGE_SIZE);
    backend.writePage("/orphan", 0, data);
    backend.writePage("/orphan", 1, data);
    backend.writePage("/good", 0, data);
    backend.writeMeta("/good", { size: PAGE_SIZE, mode: 0o100644, ctime: 0, mtime: 0 });

    backend.cleanupOrphanedPages();
    backend.assertInvariants();
  });

  it("correctly identifies orphans among files with many pages @fast", () => {
    const data = new Uint8Array(PAGE_SIZE);
    for (let i = 0; i < 20; i++) {
      backend.writePage("/keep", i, data);
    }
    backend.writeMeta("/keep", { size: PAGE_SIZE * 20, mode: 0o100644, ctime: 0, mtime: 0 });

    for (let i = 0; i < 15; i++) {
      backend.writePage("/orphan", i, data);
    }

    for (let i = 0; i < 10; i++) {
      backend.writePage("/also-keep", i, data);
    }
    backend.writeMeta("/also-keep", { size: PAGE_SIZE * 10, mode: 0o100644, ctime: 0, mtime: 0 });

    expect(backend.cleanupOrphanedPages()).toBe(1);
    expect(backend.countPages("/keep")).toBe(20);
    expect(backend.countPages("/orphan")).toBe(0);
    expect(backend.countPages("/also-keep")).toBe(10);
    backend.assertInvariants();
  });
});

describe("tomefs orphan page cleanup via syncfs", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("full-tree syncfs cleans up page orphans @fast", async () => {
    // 1. Mount, create a file, sync to establish metadata
    const { FS: FS1, tomefs: tome1 } = await mountTome(backend);
    const data = new Uint8Array(PAGE_SIZE);
    data.fill(0xaa);
    const s = FS1.open(MOUNT + "/file.dat", O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    FS1.write(s, data, 0, PAGE_SIZE);
    FS1.close(s);
    syncfs(FS1, tome1);

    // 2. Simulate crash: delete clean marker and add orphaned pages
    backend.deleteMeta("/__tomefs_clean");
    const orphanData = new Uint8Array(PAGE_SIZE);
    orphanData.fill(0xbb);
    backend.writePage("/crash_orphan", 0, orphanData);
    backend.writePage("/crash_orphan", 1, orphanData);

    // Verify orphan pages exist
    expect(backend.readPage("/crash_orphan", 0)).not.toBeNull();

    // 3. Remount — needsOrphanCleanup=true (no clean marker)
    const { FS: FS2, tomefs: tome2 } = await mountTome(backend);

    // 4. First syncfs triggers full-tree walk with orphan cleanup
    syncfs(FS2, tome2);

    // 5. Orphan pages should be cleaned up
    expect(backend.readPage("/crash_orphan", 0)).toBeNull();
    expect(backend.readPage("/crash_orphan", 1)).toBeNull();

    // 6. Persisted file data survives
    const rs = FS2.open(MOUNT + "/file.dat", O.RDONLY);
    const readBuf = new Uint8Array(PAGE_SIZE);
    FS2.read(rs, readBuf, 0, PAGE_SIZE);
    FS2.close(rs);
    expect(readBuf[0]).toBe(0xaa);
  });

  it("orphansDeleted counter includes page orphans @fast", async () => {
    // 1. Mount, create file, sync
    const { FS: FS1, tomefs: tome1 } = await mountTome(backend);
    const data = new Uint8Array(PAGE_SIZE);
    const s = FS1.open(MOUNT + "/f.dat", O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    FS1.write(s, data, 0, PAGE_SIZE);
    FS1.close(s);
    syncfs(FS1, tome1);

    // 2. Simulate crash: delete clean marker, add page orphan
    backend.deleteMeta("/__tomefs_clean");
    backend.writePage("/page_orphan", 0, data);
    backend.writePage("/page_orphan", 1, data);

    // 3. Remount and sync — triggers full-tree walk
    const { FS: FS2, tomefs: tome2 } = await mountTome(backend);
    syncfs(FS2, tome2);

    const stats = tome2.getStats();
    // Page orphan (/page_orphan) cleaned up — 1 path with 2 pages
    expect(stats.orphansDeleted).toBeGreaterThanOrEqual(1);
    expect(stats.fullTreeSyncs).toBe(1);

    // Verify the orphan pages are gone
    expect(backend.readPage("/page_orphan", 0)).toBeNull();
    expect(backend.readPage("/page_orphan", 1)).toBeNull();
  });

  it("incremental syncfs does NOT clean page orphans @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create and sync a file
    const data = new Uint8Array(PAGE_SIZE);
    const s = FS.open(MOUNT + "/f.dat", O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    FS.write(s, data, 0, PAGE_SIZE);
    FS.close(s);
    syncfs(FS, tomefs);

    // Add page orphan
    backend.writePage("/sneaky_orphan", 0, data);

    // Incremental sync (no rename/unlink to force full tree walk)
    const s2 = FS.open(MOUNT + "/f.dat", O.WRONLY);
    FS.write(s2, data, 0, PAGE_SIZE);
    FS.close(s2);
    syncfs(FS, tomefs);

    // Orphan should still exist — incremental sync doesn't clean page orphans
    expect(backend.readPage("/sneaky_orphan", 0)).not.toBeNull();
  });

  it("page orphans cleaned on first syncfs after unclean shutdown @fast", async () => {
    // 1. Mount, create file, sync, then simulate unclean shutdown
    const { FS: FS1, tomefs: tome1 } = await mountTome(backend);
    const data = new Uint8Array(PAGE_SIZE);
    data.fill(0xcc);
    const s = FS1.open(MOUNT + "/persist.dat", O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    FS1.write(s, data, 0, PAGE_SIZE);
    FS1.close(s);
    syncfs(FS1, tome1);

    // 2. Simulate crash: delete clean-shutdown marker, add page orphans
    backend.deleteMeta("/__tomefs_clean");
    backend.writePage("/crash_eviction", 0, new Uint8Array(PAGE_SIZE));

    // 3. Remount — needsOrphanCleanup will be true (no clean marker)
    const { FS: FS2, tomefs: tome2 } = await mountTome(backend);

    // Verify the persisted file was restored
    const rs = FS2.open(MOUNT + "/persist.dat", O.RDONLY);
    const readBuf = new Uint8Array(PAGE_SIZE);
    FS2.read(rs, readBuf, 0, PAGE_SIZE);
    FS2.close(rs);
    expect(readBuf[0]).toBe(0xcc);

    // 4. First syncfs should force full tree walk and clean orphaned pages
    syncfs(FS2, tome2);

    expect(backend.readPage("/crash_eviction", 0)).toBeNull();
    const stats = tome2.getStats();
    expect(stats.fullTreeSyncs).toBe(1);
    expect(stats.orphansDeleted).toBeGreaterThanOrEqual(1);
  });
});
