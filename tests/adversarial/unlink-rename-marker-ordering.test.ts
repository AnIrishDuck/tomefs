/**
 * Adversarial tests: crash safety of /__deleted_* marker metadata ordering.
 *
 * When a file with open fds is unlinked or overwritten by rename, its pages
 * are moved to a /__deleted_* temporary path. A marker metadata entry is
 * written so orphan cleanup can discover the pages after a crash.
 *
 * The critical ordering: marker metadata must be written BEFORE pages are
 * renamed. If the process crashes after page rename but before marker write,
 * the pages at /__deleted_* have no metadata and listFiles() won't return
 * them — they become permanently leaked.
 *
 * This test suite verifies:
 * 1. After crash at any point in the unlink/rename sequence, no pages are
 *    permanently leaked (all orphaned pages are discoverable and cleanable).
 * 2. The marker metadata is present before pages exist at the temp path.
 * 3. Orphan cleanup after remount correctly removes all /__deleted_* entries.
 *
 * Ethos §6 (correctness), §9 (adversarial).
 */
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { describe, it, expect } from "vitest";
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

function fillPattern(size: number, seed: number): Uint8Array {
  const buf = new Uint8Array(size);
  for (let i = 0; i < size; i++) {
    buf[i] = (seed + i * 31) & 0xff;
  }
  return buf;
}

function verifyPattern(buf: Uint8Array, size: number, seed: number): boolean {
  for (let i = 0; i < size; i++) {
    if (buf[i] !== ((seed + i * 31) & 0xff)) return false;
  }
  return true;
}

async function mountTome(backend: SyncStorageBackend, maxPages = 64) {
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

describe("adversarial: unlink/rename /__deleted_* marker ordering", () => {
  // ------------------------------------------------------------------
  // Verify marker metadata is written BEFORE pages are renamed
  // ------------------------------------------------------------------

  it("unlink with open fd: marker metadata exists before page rename @fast", async () => {
    // Use a tracking backend to observe operation ordering
    const inner = new SyncMemoryBackend();
    const ops: string[] = [];
    const tracking = createTrackingBackend(inner, ops);

    const { FS, tomefs } = await mountTome(tracking);

    // Create file and keep fd open
    const data = fillPattern(PAGE_SIZE, 0xaa);
    const fd = FS.open(MOUNT + "/victim", O.RDWR | O.CREAT, 0o666);
    FS.write(fd, data, 0, data.length, 0);

    // Sync to persist, then clear the log
    syncfs(FS, tomefs);
    ops.length = 0;

    // Unlink while fd is open — triggers /__deleted_* sequence
    FS.unlink(MOUNT + "/victim");

    // Find the writeMeta and renameFile calls for /__deleted_*
    const writeMarkerIdx = ops.findIndex(
      (op) => op.startsWith("writeMeta:/__deleted_"),
    );
    const renameIdx = ops.findIndex(
      (op) => op.startsWith("renameFile:") && op.includes("/__deleted_"),
    );

    // Marker write must come BEFORE page rename
    expect(writeMarkerIdx).toBeGreaterThanOrEqual(0);
    expect(renameIdx).toBeGreaterThanOrEqual(0);
    expect(writeMarkerIdx).toBeLessThan(renameIdx);

    FS.close(fd);
  });

  it("rename-overwrite with open fd: marker metadata exists before page rename @fast", async () => {
    const inner = new SyncMemoryBackend();
    const ops: string[] = [];
    const tracking = createTrackingBackend(inner, ops);

    const { FS, tomefs } = await mountTome(tracking);

    // Create target and keep fd open
    const targetData = fillPattern(PAGE_SIZE, 0xbb);
    const fd = FS.open(MOUNT + "/target", O.RDWR | O.CREAT, 0o666);
    FS.write(fd, targetData, 0, targetData.length, 0);

    // Create source
    FS.writeFile(MOUNT + "/source", fillPattern(PAGE_SIZE, 0xcc));

    // Sync, clear log
    syncfs(FS, tomefs);
    ops.length = 0;

    // Rename source over target — target has open fd
    FS.rename(MOUNT + "/source", MOUNT + "/target");

    // Find the operations
    const writeMarkerIdx = ops.findIndex(
      (op) => op.startsWith("writeMeta:/__deleted_"),
    );
    const renameIdx = ops.findIndex(
      (op) => op.startsWith("renameFile:") && op.includes("/__deleted_"),
    );

    // Marker write must come BEFORE page rename
    expect(writeMarkerIdx).toBeGreaterThanOrEqual(0);
    expect(renameIdx).toBeGreaterThanOrEqual(0);
    expect(writeMarkerIdx).toBeLessThan(renameIdx);

    FS.close(fd);
  });

  // ------------------------------------------------------------------
  // Simulate crash between marker write and page rename
  // ------------------------------------------------------------------

  it("unlink crash after marker write: orphan metadata cleaned up on remount @fast", async () => {
    const backend = new SyncMemoryBackend();

    // Session 1: create file, persist, then simulate crash during unlink
    {
      const { FS, tomefs } = await mountTome(backend);
      const data = fillPattern(PAGE_SIZE * 2, 0xdd);
      const fd = FS.open(MOUNT + "/crashfile", O.RDWR | O.CREAT, 0o666);
      FS.write(fd, data, 0, data.length, 0);
      syncfs(FS, tomefs);
      FS.close(fd);
    }

    // Simulate crash state: marker metadata written but pages NOT renamed.
    // This is the state if crash occurred between writeMeta and renameFile.
    backend.writeMeta("/__deleted_crash_0", {
      size: PAGE_SIZE * 2,
      mode: 0o100666,
      ctime: Date.now(),
      mtime: Date.now(),
      atime: Date.now(),
    });

    // Verify stale marker exists
    expect(backend.listFiles()).toContain("/__deleted_crash_0");

    // Session 2: remount + syncfs should clean up the stale marker
    {
      const { FS, tomefs } = await mountTome(backend);

      // /__deleted_* should not appear in directory listing
      const entries = FS.readdir(MOUNT);
      expect(entries.filter((e: string) => e.startsWith("__deleted_"))).toHaveLength(0);

      // Original file should be intact
      expect(entries).toContain("crashfile");
      const readBuf = new Uint8Array(PAGE_SIZE * 2);
      const readFd = FS.open(MOUNT + "/crashfile", O.RDONLY);
      FS.read(readFd, readBuf, 0, PAGE_SIZE * 2, 0);
      expect(verifyPattern(readBuf, PAGE_SIZE * 2, 0xdd)).toBe(true);
      FS.close(readFd);

      // syncfs orphan cleanup should remove the stale marker
      syncfs(FS, tomefs);

      const filesAfter = backend.listFiles();
      expect(filesAfter.filter((f) => f.startsWith("/__deleted_"))).toHaveLength(0);
    }
  });

  it("unlink crash after page rename: orphan pages+metadata cleaned on remount", async () => {
    const backend = new SyncMemoryBackend();

    // Session 1: create file, persist, unlink with open fd, sync, "crash"
    {
      const { FS, tomefs } = await mountTome(backend);
      const data = fillPattern(PAGE_SIZE * 3, 0xee);
      const fd = FS.open(MOUNT + "/bigfile", O.RDWR | O.CREAT, 0o666);
      FS.write(fd, data, 0, data.length, 0);
      syncfs(FS, tomefs);

      // Unlink while fd is open — creates /__deleted_* marker + renames pages
      FS.unlink(MOUNT + "/bigfile");

      // Sync to persist the /__deleted_* state
      syncfs(FS, tomefs);

      // "Crash" without closing fd
    }

    // After crash: /__deleted_* marker + pages should exist
    const orphansBefore = backend.listFiles().filter((f) =>
      f.startsWith("/__deleted_"),
    );
    expect(orphansBefore.length).toBeGreaterThan(0);
    // Pages should exist at the orphan path
    for (const orphanPath of orphansBefore) {
      expect(backend.countPages(orphanPath)).toBeGreaterThan(0);
    }

    // Session 2: remount + syncfs cleans up
    {
      const { FS, tomefs } = await mountTome(backend);

      // /__deleted_* not in directory
      const entries = FS.readdir(MOUNT);
      expect(entries.filter((e: string) => e.startsWith("__deleted_"))).toHaveLength(0);

      // Orphan cleanup
      syncfs(FS, tomefs);

      // Both metadata and pages should be gone
      const filesAfter = backend.listFiles();
      expect(filesAfter.filter((f) => f.startsWith("/__deleted_"))).toHaveLength(0);
      for (const orphanPath of orphansBefore) {
        expect(backend.countPages(orphanPath)).toBe(0);
      }
    }
  });

  it("rename-overwrite crash: open fd data readable, orphans cleaned after crash @fast", async () => {
    const backend = new SyncMemoryBackend();

    // Session 1: rename-overwrite with open fd, sync, crash
    {
      const { FS, tomefs } = await mountTome(backend);

      // Create target and source
      const targetData = fillPattern(PAGE_SIZE * 2, 0x11);
      const fd = FS.open(MOUNT + "/target", O.RDWR | O.CREAT, 0o666);
      FS.write(fd, targetData, 0, targetData.length, 0);
      FS.writeFile(MOUNT + "/source", fillPattern(PAGE_SIZE, 0x22));

      syncfs(FS, tomefs);

      // Rename source over target (target has open fd)
      FS.rename(MOUNT + "/source", MOUNT + "/target");

      // Open fd should still read the OLD target data
      const readBuf = new Uint8Array(PAGE_SIZE * 2);
      FS.read(fd, readBuf, 0, PAGE_SIZE * 2, 0);
      expect(verifyPattern(readBuf, PAGE_SIZE * 2, 0x11)).toBe(true);

      syncfs(FS, tomefs);
      // "Crash" — fd not closed
    }

    // Session 2: remount, verify target has new data, orphans cleaned
    {
      const { FS, tomefs } = await mountTome(backend);
      const entries = FS.readdir(MOUNT);
      expect(entries).toContain("target");
      expect(entries).not.toContain("source");

      // Target should have source's data (the rename succeeded)
      const readBuf = new Uint8Array(PAGE_SIZE);
      const fd = FS.open(MOUNT + "/target", O.RDONLY);
      FS.read(fd, readBuf, 0, PAGE_SIZE, 0);
      expect(verifyPattern(readBuf, PAGE_SIZE, 0x22)).toBe(true);
      FS.close(fd);

      // Clean up orphans
      syncfs(FS, tomefs);
      const filesAfter = backend.listFiles();
      expect(filesAfter.filter((f) => f.startsWith("/__deleted_"))).toHaveLength(0);
    }
  });

  it("multiple unlinks with open fds: all markers and pages cleaned up", async () => {
    const backend = new SyncMemoryBackend();

    {
      const { FS, tomefs } = await mountTome(backend);

      // Create multiple files and keep fds open
      const fds: any[] = [];
      for (let i = 0; i < 5; i++) {
        const data = fillPattern(PAGE_SIZE, 0x30 + i);
        const fd = FS.open(MOUNT + `/file${i}`, O.RDWR | O.CREAT, 0o666);
        FS.write(fd, data, 0, data.length, 0);
        fds.push(fd);
      }
      syncfs(FS, tomefs);

      // Unlink all while fds are open
      for (let i = 0; i < 5; i++) {
        FS.unlink(MOUNT + `/file${i}`);
      }
      syncfs(FS, tomefs);

      // All should have /__deleted_* entries
      const orphans = backend.listFiles().filter((f) =>
        f.startsWith("/__deleted_"),
      );
      expect(orphans).toHaveLength(5);

      // "Crash"
    }

    // Session 2: all orphans cleaned
    {
      const { FS, tomefs } = await mountTome(backend);
      syncfs(FS, tomefs);

      const filesAfter = backend.listFiles();
      expect(filesAfter.filter((f) => f.startsWith("/__deleted_"))).toHaveLength(0);
      // No directory entries for the deleted files
      const entries = FS.readdir(MOUNT);
      for (let i = 0; i < 5; i++) {
        expect(entries).not.toContain(`file${i}`);
      }
    }
  });
});

// ---------------------------------------------------------------
// Tracking backend wrapper for observing operation ordering
// ---------------------------------------------------------------

import type { SyncStorageBackend } from "../../src/sync-storage-backend.js";
import type { FileMeta } from "../../src/types.js";

/**
 * Wraps a SyncStorageBackend and logs operation names to an array.
 * Used to verify the ordering of writeMeta vs renameFile calls.
 */
function createTrackingBackend(
  inner: SyncStorageBackend,
  log: string[],
): SyncStorageBackend {
  return {
    readPage(path, pageIndex) {
      return inner.readPage(path, pageIndex);
    },
    readPages(path, pageIndices) {
      return inner.readPages(path, pageIndices);
    },
    writePage(path, pageIndex, data) {
      log.push(`writePage:${path}:${pageIndex}`);
      return inner.writePage(path, pageIndex, data);
    },
    writePages(pages) {
      for (const p of pages) {
        log.push(`writePage:${p.path}:${p.pageIndex}`);
      }
      return inner.writePages(pages);
    },
    deleteFile(path) {
      log.push(`deleteFile:${path}`);
      return inner.deleteFile(path);
    },
    deleteFiles(paths) {
      log.push(`deleteFiles:${paths.join(",")}`);
      return inner.deleteFiles(paths);
    },
    deletePagesFrom(path, fromPageIndex) {
      log.push(`deletePagesFrom:${path}:${fromPageIndex}`);
      return inner.deletePagesFrom(path, fromPageIndex);
    },
    renameFile(oldPath, newPath) {
      log.push(`renameFile:${oldPath}→${newPath}`);
      return inner.renameFile(oldPath, newPath);
    },
    readMeta(path) {
      return inner.readMeta(path);
    },
    readMetas(paths) {
      return inner.readMetas(paths);
    },
    writeMeta(path, meta) {
      log.push(`writeMeta:${path}`);
      return inner.writeMeta(path, meta);
    },
    writeMetas(entries) {
      for (const e of entries) {
        log.push(`writeMeta:${e.path}`);
      }
      return inner.writeMetas(entries);
    },
    deleteMeta(path) {
      log.push(`deleteMeta:${path}`);
      return inner.deleteMeta(path);
    },
    deleteMetas(paths) {
      for (const p of paths) {
        log.push(`deleteMeta:${p}`);
      }
      return inner.deleteMetas(paths);
    },
    countPages(path) {
      return inner.countPages(path);
    },
    countPagesBatch(paths) {
      return inner.countPagesBatch(paths);
    },
    maxPageIndex(path) {
      return inner.maxPageIndex(path);
    },
    maxPageIndexBatch(paths) {
      return inner.maxPageIndexBatch(paths);
    },
    listFiles() {
      return inner.listFiles();
    },
    syncAll(
      pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
      metas: Array<{ path: string; meta: FileMeta }>,
    ): void {
      inner.writePages(pages);
      inner.writeMetas(metas);
    },
    deleteAll(paths: string[]): void {
      log.push(`deleteAll:${paths.join(",")}`);
      inner.deleteAll(paths);
    },
  };
}
