/**
 * Adversarial tests: renameDescendantPaths dirty tracking cleanup.
 *
 * When a directory is renamed, renameDescendantPaths writes metadata for all
 * descendants at their new paths. Without clearing _metaDirty flags, the next
 * incremental syncfs redundantly re-persists all descendants — O(descendants)
 * wasted backend writes per sync after a directory rename.
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

describe("adversarial: renameDescendantPaths dirty tracking (ethos §6)", () => {
  it("syncfs after dir rename does not redundantly persist descendants @fast", async () => {
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

    // Create directory with files
    FS.mkdir("/tome/src");
    const s1 = FS.open("/tome/src/a.txt", O.RDWR | O.CREAT, 0o666);
    FS.write(s1, encode("file a"), 0, 6);
    FS.close(s1);

    const s2 = FS.open("/tome/src/b.txt", O.RDWR | O.CREAT, 0o666);
    FS.write(s2, encode("file b"), 0, 6);
    FS.close(s2);

    const s3 = FS.open("/tome/src/c.txt", O.RDWR | O.CREAT, 0o666);
    FS.write(s3, encode("file c"), 0, 6);
    FS.close(s3);

    // Initial sync to persist everything
    await new Promise<void>((resolve, reject) => {
      FS.syncfs(false, (err: Error | null) => (err ? reject(err) : resolve()));
    });

    // Rename directory
    FS.rename("/tome/src", "/tome/dst");

    // First syncfs after rename — must persist the rename
    await new Promise<void>((resolve, reject) => {
      FS.syncfs(false, (err: Error | null) => (err ? reject(err) : resolve()));
    });

    // Second syncfs — descendants should NOT be re-persisted
    let syncAllMetaCount = 0;
    const origSyncAll = backend.syncAll.bind(backend);
    backend.syncAll = (pages: any[], metas: any[]) => {
      syncAllMetaCount = metas.filter(
        (m: any) => m.path !== "/__tomefs_clean",
      ).length;
      return (origSyncAll as any)(pages, metas);
    };

    await new Promise<void>((resolve, reject) => {
      FS.syncfs(false, (err: Error | null) => (err ? reject(err) : resolve()));
    });

    // No metadata should be dirty — all descendants were already persisted
    expect(syncAllMetaCount).toBe(0);
  });

  it("subsequent write after dir rename correctly re-dirties @fast", async () => {
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

    // Create and sync
    FS.mkdir("/tome/dir");
    const s = FS.open("/tome/dir/file.txt", O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("data"), 0, 4);
    FS.close(s);

    await new Promise<void>((resolve, reject) => {
      FS.syncfs(false, (err: Error | null) => (err ? reject(err) : resolve()));
    });

    // Rename
    FS.rename("/tome/dir", "/tome/moved");

    // Sync to persist the rename
    await new Promise<void>((resolve, reject) => {
      FS.syncfs(false, (err: Error | null) => (err ? reject(err) : resolve()));
    });

    // Write to the file at its new location — should re-dirty
    const s2 = FS.open("/tome/moved/file.txt", O.RDWR, 0o666);
    FS.write(s2, encode("new data"), 0, 8);
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

    // Only the modified file should be in the batch
    expect(syncAllMetaPaths).toEqual(["/moved/file.txt"]);
  });

  it("wide directory rename clears all descendant dirty flags @fast", async () => {
    const { default: createModule } = await import(
      "../harness/emscripten_fs.mjs"
    );
    const Module = await createModule();
    const rawFS = Module.FS;

    const backend = new SyncMemoryBackend();
    const tomefs = createTomeFS(rawFS, { backend, maxPages: 256 });

    rawFS.mkdir("/tome");
    rawFS.mount(tomefs, {}, "/tome");

    const FS = rawFS;

    // Create directory with 20 files
    FS.mkdir("/tome/wide");
    for (let i = 0; i < 20; i++) {
      const s = FS.open(`/tome/wide/f${i}.txt`, O.RDWR | O.CREAT, 0o666);
      FS.write(s, encode(`file ${i}`), 0, 6 + String(i).length);
      FS.close(s);
    }

    // Sync to persist
    await new Promise<void>((resolve, reject) => {
      FS.syncfs(false, (err: Error | null) => (err ? reject(err) : resolve()));
    });

    // Rename
    FS.rename("/tome/wide", "/tome/renamed");

    // Sync to persist the rename
    await new Promise<void>((resolve, reject) => {
      FS.syncfs(false, (err: Error | null) => (err ? reject(err) : resolve()));
    });

    // Next syncfs should take the fast path (zero metadata writes)
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

  it("data persists correctly after dir rename + syncfs @fast", async () => {
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

    // Create and write
    FS.mkdir("/tome/orig");
    const s = FS.open("/tome/orig/data.bin", O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("important data"), 0, 14);
    FS.close(s);

    await new Promise<void>((resolve, reject) => {
      FS.syncfs(false, (err: Error | null) => (err ? reject(err) : resolve()));
    });

    // Rename and sync
    FS.rename("/tome/orig", "/tome/moved");
    await new Promise<void>((resolve, reject) => {
      FS.syncfs(false, (err: Error | null) => (err ? reject(err) : resolve()));
    });

    // Verify data is still readable at the new path
    const s2 = FS.open("/tome/moved/data.bin", O.RDONLY, 0o666);
    const buf = new Uint8Array(20);
    const n = FS.read(s2, buf, 0, 20);
    expect(new TextDecoder().decode(buf.subarray(0, n))).toBe("important data");
    FS.close(s2);

    // Verify metadata was persisted at the new path
    const meta = backend.readMeta("/moved/data.bin");
    expect(meta).not.toBeNull();
    expect(meta!.size).toBe(14);
  });
});
