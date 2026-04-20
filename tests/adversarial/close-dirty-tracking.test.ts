/**
 * Adversarial tests: close() dirty tracking cleanup for unlinked files.
 *
 * When a file is unlinked while fds are open (POSIX temp-file pattern),
 * writes through the surviving fds call markMetaDirty(). When the last fd
 * closes, close() must remove the dead node from dirtyMetaNodes — otherwise
 * the node leaks, preventing the O(1) syncfs fast path and causing unbounded
 * growth of the dirty set in long-running sessions.
 *
 * This pattern is common in PostgreSQL WAL segment recycling.
 *
 * These tests only run when TOMEFS_BACKEND=tomefs.
 */
import {
  createFS,
  encode,
  O,
  type FSHarness,
} from "../harness/emscripten-fs.js";

import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { createTomeFS } from "../../src/tomefs.js";

describe("adversarial: close() dirty tracking for unlinked files (ethos §6, §9)", () => {
  it("dead file node is not in syncAll batch after write+close @fast", async () => {
    const { default: createModule } = await import(
      "../harness/emscripten_fs.mjs"
    );
    const Module = await createModule();
    const rawFS = Module.FS;

    const backend = new SyncMemoryBackend();
    const tomefs = createTomeFS(rawFS, { backend, maxPages: 64 });

    rawFS.mkdir("/tome");
    rawFS.mount(tomefs, {}, "/tome");

    const FS = rawFS;

    // Create and write a file
    const stream = FS.open("/tome/tempfile", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("initial data"), 0, 12);

    // Sync to flush metadata
    await new Promise<void>((resolve, reject) => {
      FS.syncfs(false, (err: Error | null) => (err ? reject(err) : resolve()));
    });

    // Unlink while fd is still open (temp-file pattern)
    FS.unlink("/tome/tempfile");

    // Write through the surviving fd — this calls markMetaDirty on the node
    FS.write(stream, encode("more"), 0, 4);

    // Close the last fd — should clean up dirtyMetaNodes for dead node
    FS.close(stream);

    // Track what paths syncAll receives
    let syncAllMetaPaths: string[] = [];
    const origSyncAll = backend.syncAll.bind(backend);
    backend.syncAll = (pages: any[], metas: any[]) => {
      syncAllMetaPaths = metas.map((m: any) => m.path);
      return (origSyncAll as any)(pages, metas);
    };

    // syncfs will persist the parent dir (marked dirty by unlink) but
    // should NOT include the dead file's /__deleted_* path
    await new Promise<void>((resolve, reject) => {
      FS.syncfs(false, (err: Error | null) => (err ? reject(err) : resolve()));
    });

    // The dead node's metadata should not appear in the sync batch
    const deletedPaths = syncAllMetaPaths.filter((p) =>
      p.startsWith("/__deleted_"),
    );
    expect(deletedPaths).toEqual([]);
  });

  it("fast path is restored after unlink+write+close+sync cycle @fast", async () => {
    const { default: createModule } = await import(
      "../harness/emscripten_fs.mjs"
    );
    const Module = await createModule();
    const rawFS = Module.FS;

    const backend = new SyncMemoryBackend();
    const tomefs = createTomeFS(rawFS, { backend, maxPages: 64 });

    rawFS.mkdir("/tome");
    rawFS.mount(tomefs, {}, "/tome");

    const FS = rawFS;

    // Sync once to establish clean state
    await new Promise<void>((resolve, reject) => {
      FS.syncfs(false, (err: Error | null) => (err ? reject(err) : resolve()));
    });

    // Create, unlink, write, close
    const stream = FS.open("/tome/temp", O.RDWR | O.CREAT, 0o666);
    FS.write(stream, encode("data"), 0, 4);
    FS.unlink("/tome/temp");
    FS.write(stream, encode("more"), 0, 4);
    FS.close(stream);

    // One syncfs to flush the parent dir metadata (dirtied by unlink)
    await new Promise<void>((resolve, reject) => {
      FS.syncfs(false, (err: Error | null) => (err ? reject(err) : resolve()));
    });

    // NOW the fast path should work — no dirty nodes, no dirty pages
    let syncAllCalls = 0;
    const origSyncAll = backend.syncAll.bind(backend);
    backend.syncAll = (...args: any[]) => {
      syncAllCalls++;
      return (origSyncAll as any)(...args);
    };

    await new Promise<void>((resolve, reject) => {
      FS.syncfs(false, (err: Error | null) => (err ? reject(err) : resolve()));
    });

    expect(syncAllCalls).toBe(0);
  });

  it("multiple unlink+write+close cycles do not accumulate dead nodes @fast", async () => {
    const { default: createModule } = await import(
      "../harness/emscripten_fs.mjs"
    );
    const Module = await createModule();
    const rawFS = Module.FS;

    const backend = new SyncMemoryBackend();
    const tomefs = createTomeFS(rawFS, { backend, maxPages: 64 });

    rawFS.mkdir("/tome");
    rawFS.mount(tomefs, {}, "/tome");

    const FS = rawFS;

    // Initial sync
    await new Promise<void>((resolve, reject) => {
      FS.syncfs(false, (err: Error | null) => (err ? reject(err) : resolve()));
    });

    // 5 cycles of create → unlink → write → close (WAL recycling pattern)
    for (let i = 0; i < 5; i++) {
      const path = `/tome/wal_${i}`;
      const s = FS.open(path, O.RDWR | O.CREAT, 0o666);
      FS.write(s, encode(`data ${i}`), 0, 6 + String(i).length);
      FS.unlink(path);
      FS.write(s, encode("recycled"), 0, 8);
      FS.close(s);
    }

    // One syncfs to flush the parent dir metadata
    await new Promise<void>((resolve, reject) => {
      FS.syncfs(false, (err: Error | null) => (err ? reject(err) : resolve()));
    });

    // Verify fast path works — no dead nodes accumulated
    let syncAllCalls = 0;
    const origSyncAll = backend.syncAll.bind(backend);
    backend.syncAll = (...args: any[]) => {
      syncAllCalls++;
      return (origSyncAll as any)(...args);
    };

    await new Promise<void>((resolve, reject) => {
      FS.syncfs(false, (err: Error | null) => (err ? reject(err) : resolve()));
    });

    expect(syncAllCalls).toBe(0);
  });

  it("write-to-unlinked + close does not leak into subsequent syncs @fast", async () => {
    const { default: createModule } = await import(
      "../harness/emscripten_fs.mjs"
    );
    const Module = await createModule();
    const rawFS = Module.FS;

    const backend = new SyncMemoryBackend();
    const tomefs = createTomeFS(rawFS, { backend, maxPages: 64 });

    rawFS.mkdir("/tome");
    rawFS.mount(tomefs, {}, "/tome");

    const FS = rawFS;

    // Sync once to establish clean state + clean marker
    await new Promise<void>((resolve, reject) => {
      FS.syncfs(false, (err: Error | null) => (err ? reject(err) : resolve()));
    });

    // Create, unlink, write, close
    const s = FS.open("/tome/leak-test", O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("hello"), 0, 5);
    FS.unlink("/tome/leak-test");
    FS.write(s, encode("world"), 0, 5);
    FS.close(s);

    // Sync to flush the parent dir (dirty from unlink)
    await new Promise<void>((resolve, reject) => {
      FS.syncfs(false, (err: Error | null) => (err ? reject(err) : resolve()));
    });

    // Create a new file and sync
    const s2 = FS.open("/tome/real-file", O.RDWR | O.CREAT, 0o666);
    FS.write(s2, encode("real data"), 0, 9);
    FS.close(s2);

    let syncAllMetaPaths: string[] = [];
    const origSyncAll = backend.syncAll.bind(backend);
    backend.syncAll = (pages: any[], metas: any[]) => {
      syncAllMetaPaths = metas
        .filter((m: any) => m.path !== "/__tomefs_clean")
        .map((m: any) => m.path);
      return (origSyncAll as any)(pages, metas);
    };

    await new Promise<void>((resolve, reject) => {
      FS.syncfs(false, (err: Error | null) => (err ? reject(err) : resolve()));
    });

    // Should contain /real-file and / (parent dir updated by createNode)
    // but NOT any /__deleted_* path from the dead node
    const deletedPaths = syncAllMetaPaths.filter((p) =>
      p.startsWith("/__deleted_"),
    );
    expect(deletedPaths).toEqual([]);
    expect(syncAllMetaPaths).toContain("/real-file");
  });
});
