/**
 * Adversarial tests: close() dirty tracking cleanup for unlinked files.
 *
 * When a file is unlinked while fds are still open, then written to via
 * those fds (a valid POSIX pattern for temporary files), markMetaDirty()
 * re-adds the node to dirtyMetaNodes. When the last fd closes, close()
 * deletes the pages and metadata from the backend and removes the node
 * from allFileNodes — but before the fix, it did NOT remove the node
 * from dirtyMetaNodes.
 *
 * The dead node lingered in dirtyMetaNodes indefinitely:
 * - Never persisted (incremental syncfs skips unlinked+closed nodes)
 * - But iterated on every syncfs cycle, wasting CPU
 * - And holding a reference to the node, preventing garbage collection
 * - In long-running PGlite sessions with WAL segment recycling (which
 *   uses unlink-while-open), this causes unbounded growth
 *
 * The key observable difference: with the fix, syncfs after close() takes
 * the O(1) fast path (dirtyMetaNodes is empty → no backend calls). Without
 * the fix, syncfs takes the O(dirty) incremental path (dead node in
 * dirtyMetaNodes → calls syncAll with the clean-shutdown marker).
 *
 * Tests isolate the leak by syncing after unlink (flushing the parent
 * directory's timestamp) before writing via fd, so the only dirty node
 * remaining is the unlinked file itself.
 *
 * Ethos §9 (adversarial — target the seams), §6 (performance parity).
 */
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { describe, it, expect } from "vitest";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { createTomeFS } from "../../src/tomefs.js";
import { PAGE_SIZE } from "../../src/types.js";
import type { FileMeta } from "../../src/types.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

const O = {
  RDONLY: 0,
  WRONLY: 1,
  RDWR: 2,
  CREAT: 64,
  TRUNC: 512,
} as const;

const MOUNT = "/tome";

/**
 * SyncMemoryBackend that counts syncAll calls.
 *
 * syncAll is only called from the incremental or full-tree-walk paths in
 * syncfs — never from the fast path. Tracking syncAll calls during a
 * syncfs where no real work is pending detects dirty tracking leaks:
 * the fast path makes zero backend calls, the incremental path calls
 * syncAll with at least the clean-shutdown marker.
 */
class TrackingBackend extends SyncMemoryBackend {
  syncAllCalls = 0;
  tracking = false;

  startTracking(): void {
    this.syncAllCalls = 0;
    this.tracking = true;
  }

  stopTracking(): void {
    this.tracking = false;
  }

  syncAll(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
    metas: Array<{ path: string; meta: FileMeta }>,
  ): void {
    if (this.tracking) {
      this.syncAllCalls++;
    }
    super.syncAll(pages, metas);
  }
}

async function mountTome(backend: SyncMemoryBackend, maxPages = 64) {
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

function syncfs(FS: any, tomefs: any) {
  tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
    if (err) throw err;
  });
}

describe("adversarial: close() dirty tracking cleanup for unlinked files", () => {
  it("write-after-unlink then close allows syncfs fast path @fast", async () => {
    const backend = new TrackingBackend();
    const { FS, tomefs } = await mountTome(backend);

    // Create file, sync to establish clean state
    const stream = FS.open(`${MOUNT}/tmp`, O.RDWR | O.CREAT, 0o666);
    FS.write(stream, new Uint8Array(100).fill(0xAB), 0, 100, 0);
    syncfs(FS, tomefs);

    // Unlink while fd is open, then sync to flush parent dir timestamp.
    // This isolates the test: after this sync, dirtyMetaNodes is empty.
    FS.unlink(`${MOUNT}/tmp`);
    syncfs(FS, tomefs);

    // Write through the still-open fd — re-adds unlinked node to dirtyMetaNodes
    FS.write(stream, new Uint8Array(50).fill(0xCD), 0, 50, 100);

    // Close the last fd — should clean up dirty tracking
    FS.close(stream);

    // Sync — with the fix, dirtyMetaNodes is empty → fast path (0 syncAll).
    // Without the fix, dead node in dirtyMetaNodes → incremental path (1 syncAll).
    backend.startTracking();
    syncfs(FS, tomefs);
    backend.stopTracking();

    expect(backend.syncAllCalls).toBe(0);
  });

  it("multiple unlink-write-close cycles: syncfs still takes fast path @fast", async () => {
    const backend = new TrackingBackend();
    const { FS, tomefs } = await mountTome(backend);

    // Simulate 10 temp-file cycles (mimics WAL segment recycling).
    // Sync after each unlink to flush parent dir timestamps, isolating
    // the dirty tracking from the file lifecycle.
    for (let i = 0; i < 10; i++) {
      const path = `${MOUNT}/tmp_${i}`;
      const stream = FS.open(path, O.RDWR | O.CREAT, 0o666);
      FS.write(stream, new Uint8Array(PAGE_SIZE).fill(i & 0xFF), 0, PAGE_SIZE, 0);
      syncfs(FS, tomefs);

      // Unlink while open, sync to flush parent dir
      FS.unlink(path);
      syncfs(FS, tomefs);

      // Write more data via fd, then close
      FS.write(stream, new Uint8Array(100).fill((i + 1) & 0xFF), 0, 100, PAGE_SIZE);
      FS.close(stream);
    }

    // All temp files closed. Without the fix, 10 dead nodes accumulate
    // in dirtyMetaNodes. With the fix, dirtyMetaNodes is empty.
    backend.startTracking();
    syncfs(FS, tomefs);
    backend.stopTracking();

    expect(backend.syncAllCalls).toBe(0);

    // No /__deleted_* entries in backend
    const files = backend.listFiles();
    const deletedEntries = files.filter((f: string) => f.startsWith("/__deleted_"));
    expect(deletedEntries).toHaveLength(0);
  });

  it("chmod-after-unlink then close allows syncfs fast path @fast", async () => {
    const backend = new TrackingBackend();
    const { FS, tomefs } = await mountTome(backend);

    // Create and sync
    const stream = FS.open(`${MOUNT}/f`, O.RDWR | O.CREAT, 0o666);
    FS.write(stream, new Uint8Array(10), 0, 10, 0);
    syncfs(FS, tomefs);

    // Unlink while open, sync to flush parent dir timestamp
    FS.unlink(`${MOUNT}/f`);
    syncfs(FS, tomefs);

    // chmod via fd — triggers markMetaDirty on the unlinked node
    FS.fchmod(stream.fd, 0o444);

    // Close — should clean dirty tracking
    FS.close(stream);

    // Sync — should take fast path
    backend.startTracking();
    syncfs(FS, tomefs);
    backend.stopTracking();

    expect(backend.syncAllCalls).toBe(0);
  });

  it("concurrent open fds: only last close cleans dirty tracking @fast", async () => {
    const backend = new TrackingBackend();
    const { FS, tomefs } = await mountTome(backend);

    // Create file with two fds, sync
    const stream1 = FS.open(`${MOUNT}/shared`, O.RDWR | O.CREAT, 0o666);
    const stream2 = FS.open(`${MOUNT}/shared`, O.RDWR, 0o666);
    FS.write(stream1, new Uint8Array(100).fill(0xAA), 0, 100, 0);
    syncfs(FS, tomefs);

    // Unlink, sync to flush parent dir
    FS.unlink(`${MOUNT}/shared`);
    syncfs(FS, tomefs);

    // Write via first fd, close it (not the last fd)
    FS.write(stream1, new Uint8Array(50).fill(0xBB), 0, 50, 100);
    FS.close(stream1);

    // File data should still be readable via stream2
    const buf = new Uint8Array(150);
    FS.read(stream2, buf, 0, 150, 0);
    expect(buf[0]).toBe(0xAA);
    expect(buf[100]).toBe(0xBB);

    // Write via second fd, close it (THIS is the last fd)
    FS.write(stream2, new Uint8Array(25).fill(0xCC), 0, 25, 150);
    FS.close(stream2);

    // Sync — should take fast path after last fd closes
    backend.startTracking();
    syncfs(FS, tomefs);
    backend.stopTracking();

    expect(backend.syncAllCalls).toBe(0);

    // No /__deleted_* entries remain
    const files = backend.listFiles();
    const deletedEntries = files.filter((f: string) => f.startsWith("/__deleted_"));
    expect(deletedEntries).toHaveLength(0);
  });

  it("unlink without write then close: fast path works @fast", async () => {
    const backend = new TrackingBackend();
    const { FS, tomefs } = await mountTome(backend);

    // Create, sync, unlink while open, sync to flush parent dir
    const stream = FS.open(`${MOUNT}/clean`, O.RDWR | O.CREAT, 0o666);
    FS.write(stream, new Uint8Array(50), 0, 50, 0);
    syncfs(FS, tomefs);

    FS.unlink(`${MOUNT}/clean`);
    syncfs(FS, tomefs);

    // Close (no write after unlink — node not re-dirtied)
    FS.close(stream);

    // Sync — should take fast path
    backend.startTracking();
    syncfs(FS, tomefs);
    backend.stopTracking();

    expect(backend.syncAllCalls).toBe(0);
  });
});
