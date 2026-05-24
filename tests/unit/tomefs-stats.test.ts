/**
 * Tests for tomefs filesystem-level stats (getStats / resetStats).
 *
 * Verifies that the performance counters accurately track:
 *   - syncfs call counts (total, incremental, full-tree, no-op)
 *   - Orphan cleanup operations
 *   - Live tracking set sizes (trackedFiles, dirtyMetaCount)
 */
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { describe, it, expect, beforeEach } from "vitest";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { createTomeFS } from "../../src/tomefs.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

const O = {
  RDONLY: 0,
  WRONLY: 1,
  RDWR: 2,
  CREAT: 64,
  TRUNC: 512,
} as const;

const MOUNT = "/tome";

function encode(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

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

describe("tomefs stats", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("initial stats are zero @fast", async () => {
    const { tomefs } = await mountTome(backend);
    const stats = tomefs.getStats();
    expect(stats.syncfsCount).toBe(0);
    expect(stats.incrementalSyncs).toBe(0);
    expect(stats.fullTreeSyncs).toBe(0);
    expect(stats.noopSyncs).toBe(0);
    expect(stats.orphansDeleted).toBe(0);
  });

  it("trackedFiles reflects file node count @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    expect(tomefs.getStats().trackedFiles).toBe(0);

    const s1 = FS.open(`${MOUNT}/a`, O.RDWR | O.CREAT, 0o666);
    FS.close(s1);
    expect(tomefs.getStats().trackedFiles).toBe(1);

    const s2 = FS.open(`${MOUNT}/b`, O.RDWR | O.CREAT, 0o666);
    FS.close(s2);
    expect(tomefs.getStats().trackedFiles).toBe(2);

    FS.unlink(`${MOUNT}/a`);
    expect(tomefs.getStats().trackedFiles).toBe(1);
  });

  it("dirtyMetaCount reflects unsaved metadata @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const s = FS.open(`${MOUNT}/file`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("data"), 0, 4);
    FS.close(s);

    // File and its parent dir have dirty metadata
    expect(tomefs.getStats().dirtyMetaCount).toBeGreaterThan(0);

    syncfs(FS, tomefs);

    // After sync, dirty count should be zero
    expect(tomefs.getStats().dirtyMetaCount).toBe(0);
  });

  it("noop syncfs is counted @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // First syncfs with no data — should be a no-op
    syncfs(FS, tomefs);
    // First syncfs is a full tree sync (needsOrphanCleanup=true on mount)
    const stats1 = tomefs.getStats();
    expect(stats1.syncfsCount).toBe(1);

    // Second syncfs with no changes — should be a no-op
    syncfs(FS, tomefs);
    const stats2 = tomefs.getStats();
    expect(stats2.syncfsCount).toBe(2);
    expect(stats2.noopSyncs).toBe(1);
  });

  it("first syncfs is full tree walk @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const s = FS.open(`${MOUNT}/file`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("hello"), 0, 5);
    FS.close(s);

    syncfs(FS, tomefs);
    const stats = tomefs.getStats();
    expect(stats.syncfsCount).toBe(1);
    expect(stats.fullTreeSyncs).toBe(1);
    expect(stats.incrementalSyncs).toBe(0);
  });

  it("subsequent syncfs uses incremental path @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const s = FS.open(`${MOUNT}/file`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("hello"), 0, 5);
    FS.close(s);

    // First sync: full tree walk
    syncfs(FS, tomefs);

    // Write more data
    const s2 = FS.open(`${MOUNT}/file`, O.RDWR, 0o666);
    FS.write(s2, encode("world"), 0, 5);
    FS.close(s2);

    // Second sync: incremental
    syncfs(FS, tomefs);
    const stats = tomefs.getStats();
    expect(stats.syncfsCount).toBe(2);
    expect(stats.fullTreeSyncs).toBe(1);
    expect(stats.incrementalSyncs).toBe(1);
  });

  it("rename uses incremental sync (marker invalidation is for crash recovery) @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const s = FS.open(`${MOUNT}/old`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("data"), 0, 4);
    FS.close(s);

    // First sync clears orphan cleanup flag
    syncfs(FS, tomefs);
    expect(tomefs.getStats().fullTreeSyncs).toBe(1);

    // Rename invalidates the clean-shutdown marker for crash safety,
    // but does NOT force full tree walk in the current session —
    // the incremental path is used.
    FS.rename(`${MOUNT}/old`, `${MOUNT}/new`);
    syncfs(FS, tomefs);

    const stats = tomefs.getStats();
    expect(stats.fullTreeSyncs).toBe(1);
    expect(stats.incrementalSyncs).toBe(1);
  });

  it("orphansDeleted tracks cleanup count @fast", async () => {
    // Inject orphan metadata with non-existent parent directories.
    // restoreTree skips entries whose parents don't exist in the map,
    // so these become genuine orphans detected during the first syncfs
    // full tree walk.
    backend.writeMeta("/missing-parent/orphan1", { size: 100, mode: 0o100644, ctime: 1, mtime: 1 });
    backend.writeMeta("/missing-parent/orphan2", { size: 200, mode: 0o100644, ctime: 1, mtime: 1 });

    const { FS, tomefs } = await mountTome(backend);
    expect(tomefs.getStats().orphansDeleted).toBe(0);

    // First syncfs does full tree walk and should detect + delete orphans
    syncfs(FS, tomefs);

    const stats = tomefs.getStats();
    expect(stats.orphansDeleted).toBe(2);
    expect(stats.fullTreeSyncs).toBe(1);
  });

  it("resetStats clears counters but not live tracking @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const s = FS.open(`${MOUNT}/file`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("data"), 0, 4);
    FS.close(s);
    syncfs(FS, tomefs);

    const before = tomefs.getStats();
    expect(before.syncfsCount).toBe(1);
    expect(before.trackedFiles).toBe(1);

    tomefs.resetStats();

    const after = tomefs.getStats();
    expect(after.syncfsCount).toBe(0);
    expect(after.incrementalSyncs).toBe(0);
    expect(after.fullTreeSyncs).toBe(0);
    expect(after.noopSyncs).toBe(0);
    expect(after.orphansDeleted).toBe(0);
    // Live tracking values are NOT reset
    expect(after.trackedFiles).toBe(1);
    expect(after.dirtyMetaCount).toBe(0);
  });

  it("stats are per-instance and reset on remount @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const s = FS.open(`${MOUNT}/file`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("data"), 0, 4);
    FS.close(s);
    syncfs(FS, tomefs);

    expect(tomefs.getStats().syncfsCount).toBe(1);

    // Unmount and remount with fresh tomefs instance
    FS.unmount(MOUNT);
    const tomefs2 = createTomeFS(FS, { backend });
    FS.mount(tomefs2, {}, MOUNT);

    // New instance starts with fresh counters
    expect(tomefs2.getStats().syncfsCount).toBe(0);
    // But has restored file nodes from backend
    expect(tomefs2.getStats().trackedFiles).toBe(1);
  });

  it("populate=true syncfs is not counted @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    syncfs(FS, tomefs, true);
    expect(tomefs.getStats().syncfsCount).toBe(0);
  });

  it("multiple syncs accumulate correctly @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // First sync (full tree)
    const s = FS.open(`${MOUNT}/f`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("1"), 0, 1);
    FS.close(s);
    syncfs(FS, tomefs);

    // Several incremental syncs
    for (let i = 0; i < 5; i++) {
      const si = FS.open(`${MOUNT}/f`, O.RDWR, 0o666);
      FS.write(si, encode(String(i)), 0, 1);
      FS.close(si);
      syncfs(FS, tomefs);
    }

    // Several no-op syncs
    for (let i = 0; i < 3; i++) {
      syncfs(FS, tomefs);
    }

    const stats = tomefs.getStats();
    expect(stats.syncfsCount).toBe(9); // 1 full + 5 incr + 3 noop
    expect(stats.fullTreeSyncs).toBe(1);
    expect(stats.incrementalSyncs).toBe(5);
    expect(stats.noopSyncs).toBe(3);
  });

  it("unlink uses incremental sync (marker invalidation is for crash recovery) @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const s = FS.open(`${MOUNT}/file`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("data"), 0, 4);
    FS.close(s);
    syncfs(FS, tomefs);

    // Open + unlink + close: close handler eagerly cleans up pages/meta.
    // invalidateCleanMarker() only affects crash recovery (next mount),
    // NOT the current session's sync path selection.
    const s2 = FS.open(`${MOUNT}/file`, O.RDONLY, 0o666);
    FS.unlink(`${MOUNT}/file`);
    FS.close(s2);

    syncfs(FS, tomefs);

    const stats = tomefs.getStats();
    expect(stats.fullTreeSyncs).toBe(1);
    // Second sync is incremental since orphan cleanup flag was cleared
    expect(stats.incrementalSyncs).toBe(1);
    expect(stats.trackedFiles).toBe(0);
  });
});
