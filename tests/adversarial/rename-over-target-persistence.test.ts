/**
 * Adversarial tests: rename-over-target with persistence round-trip.
 *
 * When Postgres rotates WAL segments or promotes temp files, it renames
 * a source file over an existing target. If the target has open file
 * descriptors, its pages move to a /__deleted_N temporary path so the
 * fds can still read their data (POSIX unlink semantics).
 *
 * This test exercises the full persistence lifecycle of rename-over:
 * 1. Rename source over target (target gets /__deleted_N path)
 * 2. Verify both renamed file and orphaned fd work correctly
 * 3. syncfs persists pages at new paths (including /__deleted_N marker)
 * 4. Close orphaned fd → triggers cleanup of /__deleted_N pages
 * 5. syncfs removes the /__deleted_N entries
 * 6. Remount from backend → only the renamed file exists
 *
 * This targets the interaction between:
 * - rename's /__deleted_N path creation
 * - syncfs's orphan cleanup (currentPaths vs backend paths)
 * - restoreTree's filtering of /__deleted_* entries
 * - Page cache's renameFile re-keying
 *
 * Ethos §9: "Write tests designed to break tomefs specifically — target
 * the seams: metadata updates after flush, dirty flush ordering"
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

function encode(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

function decode(buf: Uint8Array, length?: number): string {
  return new TextDecoder().decode(
    length !== undefined ? buf.subarray(0, length) : buf,
  );
}

async function createEmscriptenModule() {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  return createModule();
}

interface TestHarness {
  FS: any;
  backend: SyncMemoryBackend;
  tomefs: any;
}

async function createHarness(
  backend?: SyncMemoryBackend,
  maxPages: number = 64,
): Promise<TestHarness> {
  const Module = await createEmscriptenModule();
  const FS = Module.FS;
  const b = backend ?? new SyncMemoryBackend();
  const tomefs = createTomeFS(FS, { backend: b, maxPages });
  FS.mkdir(MOUNT);
  FS.mount(tomefs, {}, MOUNT);
  return { FS, backend: b, tomefs };
}

function syncfs(FS: any): void {
  FS.syncfs(false, (err: Error | null) => {
    if (err) throw err;
  });
}

describe("adversarial: rename-over-target with persistence round-trip", () => {
  // -----------------------------------------------------------------
  // Basic: rename over target with open fd, syncfs, remount
  // -----------------------------------------------------------------

  it("persists renamed file and cleans up orphaned target after fd close", async () => {
    const { FS, backend } = await createHarness();

    // Create target and source files
    const targetData = encode("target file data for WAL segment");
    const targetFd = FS.open(
      `${MOUNT}/target`, O.RDWR | O.CREAT | O.TRUNC, 0o666,
    );
    FS.write(targetFd, targetData, 0, targetData.length, 0);

    const sourceData = encode("source file data (promoted temp)");
    const srcFd = FS.open(
      `${MOUNT}/source`, O.WRONLY | O.CREAT | O.TRUNC, 0o666,
    );
    FS.write(srcFd, sourceData, 0, sourceData.length, 0);
    FS.close(srcFd);

    // Rename source over target — target's pages move to /__deleted_N
    FS.rename(`${MOUNT}/source`, `${MOUNT}/target`);

    // Old target fd should still read the original data (POSIX semantics)
    const readBuf = new Uint8Array(100);
    const n = FS.read(targetFd, readBuf, 0, 100, 0);
    expect(decode(readBuf, n)).toBe("target file data for WAL segment");

    // Sync while orphaned fd is still open — /__deleted_N should be preserved
    syncfs(FS);

    // Verify backend has both /target and /__deleted_N
    const files = backend.listFiles();
    expect(files).toContain("/target");
    const deletedPaths = files.filter((f: string) => f.startsWith("/__deleted_"));
    expect(deletedPaths.length).toBeGreaterThanOrEqual(1);

    // Close orphaned fd — triggers cleanup
    FS.close(targetFd);

    // Sync again — /__deleted_N should be removed
    syncfs(FS);

    // Verify backend no longer has /__deleted_N
    const filesAfterCleanup = backend.listFiles();
    const remainingDeleted = filesAfterCleanup.filter(
      (f: string) => f.startsWith("/__deleted_"),
    );
    expect(remainingDeleted).toEqual([]);

    // Remount from backend
    const h2 = await createHarness(backend);

    // Verify renamed file persisted correctly
    const fd2 = h2.FS.open(`${MOUNT}/target`, O.RDONLY);
    const buf2 = new Uint8Array(100);
    const n2 = h2.FS.read(fd2, buf2, 0, 100, 0);
    expect(decode(buf2, n2)).toBe("source file data (promoted temp)");
    h2.FS.close(fd2);

    // Source should not exist
    expect(() => h2.FS.stat(`${MOUNT}/source`)).toThrow();
  });

  // -----------------------------------------------------------------
  // Multi-page: rename-over with large files under cache pressure
  // -----------------------------------------------------------------

  it("preserves multi-page data through rename-over + persistence under cache pressure", async () => {
    // Small cache: 8 pages = 64KB (forces eviction during writes)
    const { FS, backend } = await createHarness(undefined, 8);

    // Create target with 3 pages of data
    const targetFd = FS.open(
      `${MOUNT}/target`, O.RDWR | O.CREAT | O.TRUNC, 0o666,
    );
    const targetPages = new Uint8Array(PAGE_SIZE * 3);
    for (let i = 0; i < targetPages.length; i++) targetPages[i] = 0xaa;
    FS.write(targetFd, targetPages, 0, targetPages.length, 0);

    // Create source with 4 pages of data
    const srcFd = FS.open(
      `${MOUNT}/source`, O.WRONLY | O.CREAT | O.TRUNC, 0o666,
    );
    const sourcePages = new Uint8Array(PAGE_SIZE * 4);
    for (let i = 0; i < sourcePages.length; i++) sourcePages[i] = 0xbb;
    FS.write(srcFd, sourcePages, 0, sourcePages.length, 0);
    FS.close(srcFd);

    // Rename source over target
    FS.rename(`${MOUNT}/source`, `${MOUNT}/target`);

    // Verify orphaned fd reads 3 pages of 0xAA
    const readBuf = new Uint8Array(PAGE_SIZE * 3);
    const n = FS.read(targetFd, readBuf, 0, readBuf.length, 0);
    expect(n).toBe(PAGE_SIZE * 3);
    for (let i = 0; i < n; i++) {
      if (readBuf[i] !== 0xaa) {
        throw new Error(`Orphaned fd byte ${i}: expected 0xAA, got 0x${readBuf[i].toString(16)}`);
      }
    }

    // Sync with orphaned fd open
    syncfs(FS);

    // Close and sync to clean up
    FS.close(targetFd);
    syncfs(FS);

    // Remount and verify
    const h2 = await createHarness(backend, 8);
    const fd2 = h2.FS.open(`${MOUNT}/target`, O.RDONLY);
    const buf2 = new Uint8Array(PAGE_SIZE * 4);
    const n2 = h2.FS.read(fd2, buf2, 0, buf2.length, 0);
    expect(n2).toBe(PAGE_SIZE * 4);
    for (let i = 0; i < n2; i++) {
      if (buf2[i] !== 0xbb) {
        throw new Error(`Remounted byte ${i}: expected 0xBB, got 0x${buf2[i].toString(16)}`);
      }
    }
    h2.FS.close(fd2);
  });

  // -----------------------------------------------------------------
  // Chain: multiple rename-overs in sequence before syncfs
  // -----------------------------------------------------------------

  it("handles chained rename-overs with persistence", async () => {
    const { FS, backend } = await createHarness();

    // Create three files: A, B, C
    const files = ["A", "B", "C"];
    const data: Record<string, string> = {
      A: "data-for-A-" + "x".repeat(100),
      B: "data-for-B-" + "y".repeat(100),
      C: "data-for-C-" + "z".repeat(100),
    };

    for (const name of files) {
      const d = encode(data[name]);
      const fd = FS.open(
        `${MOUNT}/${name}`, O.WRONLY | O.CREAT | O.TRUNC, 0o666,
      );
      FS.write(fd, d, 0, d.length, 0);
      FS.close(fd);
    }

    // Chain: rename B over A, then rename C over B (now at /A)
    // After: /A has C's data, B is gone, C is gone
    FS.rename(`${MOUNT}/B`, `${MOUNT}/A`); // A's data orphaned
    FS.rename(`${MOUNT}/C`, `${MOUNT}/A`); // B's data orphaned

    // Sync and remount
    syncfs(FS);
    const h2 = await createHarness(backend);

    // Only /A should exist with C's data
    const fd = h2.FS.open(`${MOUNT}/A`, O.RDONLY);
    const buf = new Uint8Array(200);
    const n = h2.FS.read(fd, buf, 0, 200, 0);
    expect(decode(buf, n)).toBe(data.C);
    h2.FS.close(fd);

    // B and C should not exist
    expect(() => h2.FS.stat(`${MOUNT}/B`)).toThrow();
    expect(() => h2.FS.stat(`${MOUNT}/C`)).toThrow();

    // No /__deleted_* leftovers
    const deleted = backend.listFiles().filter(
      (f: string) => f.startsWith("/__deleted_"),
    );
    expect(deleted).toEqual([]);
  });

  // -----------------------------------------------------------------
  // WAL rotation pattern: rename-over + write to new file at old path
  // -----------------------------------------------------------------

  it("handles WAL rotation: rename-over, then create new file at source path", async () => {
    const { FS, backend } = await createHarness();

    // Simulate WAL segment: current at /wal, promoted at /wal.done
    const walData = encode("WAL segment data: transaction log entries");
    const walFd = FS.open(
      `${MOUNT}/wal`, O.RDWR | O.CREAT | O.TRUNC, 0o666,
    );
    FS.write(walFd, walData, 0, walData.length, 0);
    FS.close(walFd);

    // Create the "done" file that will be overwritten
    const doneData = encode("old done segment");
    const doneFd = FS.open(
      `${MOUNT}/wal.done`, O.RDWR | O.CREAT | O.TRUNC, 0o666,
    );
    FS.write(doneFd, doneData, 0, doneData.length, 0);

    // Rotate: rename /wal → /wal.done (overwrites old done)
    FS.rename(`${MOUNT}/wal`, `${MOUNT}/wal.done`);

    // Create new WAL segment at the same path
    const newWalData = encode("new WAL segment: fresh transaction log");
    const newWalFd = FS.open(
      `${MOUNT}/wal`, O.RDWR | O.CREAT | O.TRUNC, 0o666,
    );
    FS.write(newWalFd, newWalData, 0, newWalData.length, 0);

    // Old done fd still reads original done data
    const oldBuf = new Uint8Array(100);
    const nOld = FS.read(doneFd, oldBuf, 0, 100, 0);
    expect(decode(oldBuf, nOld)).toBe("old done segment");
    FS.close(doneFd);
    FS.close(newWalFd);

    // Sync and remount
    syncfs(FS);
    const h2 = await createHarness(backend);

    // /wal should have new data
    const fd1 = h2.FS.open(`${MOUNT}/wal`, O.RDONLY);
    const buf1 = new Uint8Array(200);
    const n1 = h2.FS.read(fd1, buf1, 0, 200, 0);
    expect(decode(buf1, n1)).toBe("new WAL segment: fresh transaction log");
    h2.FS.close(fd1);

    // /wal.done should have the rotated WAL data
    const fd2 = h2.FS.open(`${MOUNT}/wal.done`, O.RDONLY);
    const buf2 = new Uint8Array(200);
    const n2 = h2.FS.read(fd2, buf2, 0, 200, 0);
    expect(decode(buf2, n2)).toBe("WAL segment data: transaction log entries");
    h2.FS.close(fd2);

    // No orphans
    const deleted = backend.listFiles().filter(
      (f: string) => f.startsWith("/__deleted_"),
    );
    expect(deleted).toEqual([]);
  });

  // -----------------------------------------------------------------
  // Dirty shutdown: rename-over with open fd, NO close before remount
  // -----------------------------------------------------------------

  it("survives dirty shutdown after rename-over with orphaned fd", async () => {
    const { FS, backend } = await createHarness();

    // Create target with known data
    const targetData = encode("target will be orphaned");
    const targetFd = FS.open(
      `${MOUNT}/target`, O.RDWR | O.CREAT | O.TRUNC, 0o666,
    );
    FS.write(targetFd, targetData, 0, targetData.length, 0);

    // Create source
    const sourceData = encode("source takes over");
    const srcFd = FS.open(
      `${MOUNT}/source`, O.WRONLY | O.CREAT | O.TRUNC, 0o666,
    );
    FS.write(srcFd, sourceData, 0, sourceData.length, 0);
    FS.close(srcFd);

    // Rename source over target
    FS.rename(`${MOUNT}/source`, `${MOUNT}/target`);

    // Sync while orphaned fd is open
    syncfs(FS);

    // Dirty shutdown: don't close targetFd, don't sync again.
    // The backend has /__deleted_N marker and pages for the orphaned target.

    // Remount — restoreTree filters /__deleted_* entries and orphan cleanup
    // removes them on first syncfs
    const h2 = await createHarness(backend);

    // /target should have source data
    const fd2 = h2.FS.open(`${MOUNT}/target`, O.RDONLY);
    const buf2 = new Uint8Array(100);
    const n2 = h2.FS.read(fd2, buf2, 0, 100, 0);
    expect(decode(buf2, n2)).toBe("source takes over");
    h2.FS.close(fd2);

    // /__deleted_* entries may still exist after mount (they're orphaned from
    // the crash). They should be cleaned up on first syncfs.
    syncfs(h2.FS);
    const filesAfterSync = backend.listFiles();
    const deleted = filesAfterSync.filter(
      (f: string) => f.startsWith("/__deleted_"),
    );
    expect(deleted).toEqual([]);
  });

  // -----------------------------------------------------------------
  // Rename-over into subdirectory with persistence
  // -----------------------------------------------------------------

  it("persists rename-over in subdirectory correctly", async () => {
    const { FS, backend } = await createHarness();

    FS.mkdir(`${MOUNT}/pg_wal`);

    // Create WAL segments in subdirectory
    for (let i = 0; i < 3; i++) {
      const d = encode(`segment-${i}-data-${"w".repeat(50)}`);
      const fd = FS.open(
        `${MOUNT}/pg_wal/seg${i}`, O.WRONLY | O.CREAT | O.TRUNC, 0o666,
      );
      FS.write(fd, d, 0, d.length, 0);
      FS.close(fd);
    }

    // Initial sync
    syncfs(FS);

    // Rotate: rename seg2 over seg0
    FS.rename(`${MOUNT}/pg_wal/seg2`, `${MOUNT}/pg_wal/seg0`);

    // Create new seg2
    const newSeg = encode("new-segment-2-data");
    const fd = FS.open(
      `${MOUNT}/pg_wal/seg2`, O.WRONLY | O.CREAT | O.TRUNC, 0o666,
    );
    FS.write(fd, newSeg, 0, newSeg.length, 0);
    FS.close(fd);

    // Sync and remount
    syncfs(FS);
    const h2 = await createHarness(backend);

    // seg0 should have seg2's original data
    const fd0 = h2.FS.open(`${MOUNT}/pg_wal/seg0`, O.RDONLY);
    const buf0 = new Uint8Array(200);
    const n0 = h2.FS.read(fd0, buf0, 0, 200, 0);
    expect(decode(buf0, n0)).toContain("segment-2-data");
    h2.FS.close(fd0);

    // seg1 should be unchanged
    const fd1 = h2.FS.open(`${MOUNT}/pg_wal/seg1`, O.RDONLY);
    const buf1 = new Uint8Array(200);
    const n1 = h2.FS.read(fd1, buf1, 0, 200, 0);
    expect(decode(buf1, n1)).toContain("segment-1-data");
    h2.FS.close(fd1);

    // seg2 should have new data
    const fd2 = h2.FS.open(`${MOUNT}/pg_wal/seg2`, O.RDONLY);
    const buf2 = new Uint8Array(200);
    const n2 = h2.FS.read(fd2, buf2, 0, 200, 0);
    expect(decode(buf2, n2)).toBe("new-segment-2-data");
    h2.FS.close(fd2);
  });

  // -----------------------------------------------------------------
  // Multiple simultaneous orphaned fds from rename-over
  // -----------------------------------------------------------------

  it("handles multiple simultaneous orphaned fds from rename-overs", async () => {
    const { FS, backend } = await createHarness();

    // Create 5 target files, each with an open fd
    const targetFds: number[] = [];
    const targetContents: string[] = [];

    for (let i = 0; i < 5; i++) {
      const content = `target-${i}-original-data-${"t".repeat(50 + i * 10)}`;
      targetContents.push(content);
      const d = encode(content);
      const fd = FS.open(
        `${MOUNT}/file${i}`, O.RDWR | O.CREAT | O.TRUNC, 0o666,
      );
      FS.write(fd, d, 0, d.length, 0);
      targetFds.push(fd);
    }

    // Create 5 source files
    const sourceContents: string[] = [];
    for (let i = 0; i < 5; i++) {
      const content = `source-${i}-replacement-${"s".repeat(50 + i * 10)}`;
      sourceContents.push(content);
      const d = encode(content);
      const fd = FS.open(
        `${MOUNT}/src${i}`, O.WRONLY | O.CREAT | O.TRUNC, 0o666,
      );
      FS.write(fd, d, 0, d.length, 0);
      FS.close(fd);
    }

    // Rename all sources over targets (5 simultaneous orphaned fds)
    for (let i = 0; i < 5; i++) {
      FS.rename(`${MOUNT}/src${i}`, `${MOUNT}/file${i}`);
    }

    // All 5 orphaned fds should still read their original data
    for (let i = 0; i < 5; i++) {
      const buf = new Uint8Array(200);
      const n = FS.read(targetFds[i], buf, 0, 200, 0);
      expect(decode(buf, n)).toBe(targetContents[i]);
    }

    // Sync with all orphans open
    syncfs(FS);

    // Verify backend has /__deleted_* entries for all 5
    const deletedPaths = backend.listFiles().filter(
      (f: string) => f.startsWith("/__deleted_"),
    );
    expect(deletedPaths.length).toBe(5);

    // Close all orphaned fds
    for (const fd of targetFds) {
      FS.close(fd);
    }

    // Sync to clean up
    syncfs(FS);

    // No /__deleted_* entries should remain
    const remaining = backend.listFiles().filter(
      (f: string) => f.startsWith("/__deleted_"),
    );
    expect(remaining).toEqual([]);

    // Remount and verify all files have source data
    const h2 = await createHarness(backend);
    for (let i = 0; i < 5; i++) {
      const fd = h2.FS.open(`${MOUNT}/file${i}`, O.RDONLY);
      const buf = new Uint8Array(200);
      const n = h2.FS.read(fd, buf, 0, 200, 0);
      expect(decode(buf, n)).toBe(sourceContents[i]);
      h2.FS.close(fd);
    }

    // Source paths should not exist
    for (let i = 0; i < 5; i++) {
      expect(() => h2.FS.stat(`${MOUNT}/src${i}`)).toThrow();
    }
  });
});
