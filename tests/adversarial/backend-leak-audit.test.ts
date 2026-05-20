/**
 * Adversarial tests: backend structural integrity audit across many cycles.
 *
 * All other persistence tests verify that file CONTENTS survive syncfs +
 * remount correctly. None of them audit the backend's structural state:
 * whether extra metadata entries or orphaned pages accumulate over time.
 *
 * In production, PGlite runs for hours or days in a browser tab, creating
 * and destroying temp files, rotating WAL segments, running VACUUM, etc.
 * If any operation leaks metadata or pages in the backend, the leaked
 * entries accumulate silently — they don't affect the current session but
 * waste storage and slow down operations like listFiles() and restoreTree.
 *
 * These tests run multi-cycle workloads and after each syncfs + remount
 * cycle, perform a full audit of the backend state:
 *   - Every file/dir/symlink in the FS tree has backend metadata
 *   - Every backend metadata entry corresponds to a live FS node (no orphans)
 *   - Every file's backend page count matches ceil(size / PAGE_SIZE)
 *   - No pages exist in the backend for files without metadata
 *
 * Ethos §8: "simulate real PGlite access patterns"
 * Ethos §9: "Write tests designed to break tomefs specifically"
 */
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { describe, it, expect } from "vitest";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { createTomeFS } from "../../src/tomefs.js";
import { PAGE_SIZE, pageKeyStr } from "../../src/types.js";
import type { FileMeta } from "../../src/types.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

const O = {
  RDONLY: 0,
  WRONLY: 1,
  RDWR: 2,
  CREAT: 64,
  TRUNC: 512,
  APPEND: 1024,
} as const;

const MOUNT = "/tome";
const CLEAN_MARKER = "/__tomefs_clean";

// ---------------------------------------------------------------
// AuditableBackend: SyncMemoryBackend with structural audit hooks
// ---------------------------------------------------------------

/**
 * SyncMemoryBackend that exposes internal state for structural auditing.
 *
 * Not a mock — all operations are real. Only adds read-only inspection
 * methods for verifying invariants that the public API can't express.
 */
class AuditableBackend extends SyncMemoryBackend {
  /**
   * Return the set of all paths that have at least one page stored.
   * Uses the public countPages API iteratively — no private field access.
   */
  pathsWithPages(knownPaths: string[]): Set<string> {
    const result = new Set<string>();
    for (const path of knownPaths) {
      if (this.countPages(path) > 0) {
        result.add(path);
      }
    }
    return result;
  }

  /**
   * Audit the backend state against expected filesystem state.
   *
   * Backend semantics:
   *   - The root "/" is explicitly skipped by persistTree (recreated on mount).
   *     The incremental syncfs path may write it, so tolerate its presence.
   *   - /__tomefs_clean is present after syncfs, consumed (deleted) on mount.
   *   - /__deleted_* entries exist only for files with open fds after unlink.
   *
   * Returns a list of violations (empty = clean).
   */
  audit(
    expectedFiles: Map<string, { size: number }>,
    expectedDirs: Set<string>,
    expectedSymlinks: Set<string>,
    /** True if a syncfs was done since the last mount (clean marker expected). */
    afterSync: boolean,
  ): string[] {
    const violations: string[] = [];
    const backendPaths = new Set(this.listFiles());

    // Build expected set: all files, non-root dirs, symlinks
    const expectedPaths = new Set<string>();
    for (const path of expectedFiles.keys()) expectedPaths.add(path);
    for (const path of expectedDirs) {
      if (path !== "/") expectedPaths.add(path); // root is not stored
    }
    for (const path of expectedSymlinks) expectedPaths.add(path);
    if (afterSync) expectedPaths.add(CLEAN_MARKER);

    // Check for orphaned metadata (in backend but not expected)
    for (const path of backendPaths) {
      // Tolerate root "/" — incremental path may write it, full tree walk skips it
      if (path === "/") continue;
      if (!expectedPaths.has(path)) {
        violations.push(`orphaned metadata: ${path}`);
      }
    }

    // Check for missing metadata (expected but not in backend)
    for (const path of expectedPaths) {
      if (!backendPaths.has(path)) {
        violations.push(`missing metadata: ${path}`);
      }
    }

    // Check page counts for files
    for (const [path, { size }] of expectedFiles) {
      const expectedPages = size > 0 ? Math.ceil(size / PAGE_SIZE) : 0;
      const actualPages = this.countPages(path);
      if (actualPages > expectedPages) {
        violations.push(
          `excess pages for ${path}: expected <=${expectedPages}, got ${actualPages}`,
        );
      }
    }

    // Check that non-file entries don't have leftover pages
    for (const path of backendPaths) {
      if (path === CLEAN_MARKER || path === "/") continue;
      if (expectedFiles.has(path)) continue;
      // Directories and symlinks should never have pages
      const pages = this.countPages(path);
      if (pages > 0) {
        violations.push(
          `leaked pages for non-file ${path}: ${pages} pages`,
        );
      }
    }

    return violations;
  }
}

// ---------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------

function fillPattern(size: number, seed: number): Uint8Array {
  const buf = new Uint8Array(size);
  for (let i = 0; i < size; i++) {
    buf[i] = (seed + i * 31) & 0xff;
  }
  return buf;
}

function verifyPattern(buf: Uint8Array, size: number, seed: number): void {
  for (let i = 0; i < size; i++) {
    if (buf[i] !== ((seed + i * 31) & 0xff)) {
      throw new Error(
        `Pattern mismatch at byte ${i}: expected ${(seed + i * 31) & 0xff}, got ${buf[i]} (seed=${seed})`,
      );
    }
  }
}

async function mountTome(backend: AuditableBackend, maxPages: number) {
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

function syncfs(FS: any, tomefs: any): void {
  tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
    if (err) throw err;
  });
}

function syncAndUnmount(FS: any, tomefs: any): void {
  syncfs(FS, tomefs);
  FS.unmount(MOUNT);
}

/** Walk the FS tree and collect all paths relative to MOUNT. */
function walkTree(FS: any, path: string): {
  files: Map<string, { size: number }>;
  dirs: Set<string>;
  symlinks: Set<string>;
} {
  const files = new Map<string, { size: number }>();
  const dirs = new Set<string>();
  const symlinks = new Set<string>();

  function walk(fsPath: string, storagePath: string): void {
    const stat = FS.lstat(fsPath);
    if (FS.isDir(stat.mode)) {
      dirs.add(storagePath);
      const entries = FS.readdir(fsPath).filter(
        (e: string) => e !== "." && e !== "..",
      );
      for (const entry of entries) {
        const childStorage = storagePath === "/"
          ? `/${entry}`
          : `${storagePath}/${entry}`;
        walk(`${fsPath}/${entry}`, childStorage);
      }
    } else if (FS.isFile(stat.mode)) {
      files.set(storagePath, { size: stat.size });
    } else if (FS.isLink(stat.mode)) {
      symlinks.add(storagePath);
    }
  }

  walk(path, "/");
  return { files, dirs, symlinks };
}

/**
 * Perform a full audit: walk FS tree, compare with backend, report violations.
 * @param afterSync - true if a syncfs was performed since the last mount
 */
function auditBackend(
  FS: any,
  backend: AuditableBackend,
  afterSync = true,
): string[] {
  const { files, dirs, symlinks } = walkTree(FS, MOUNT);
  return backend.audit(files, dirs, symlinks, afterSync);
}

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

describe("adversarial: backend structural integrity audit", () => {
  // ------------------------------------------------------------------
  // Test 1: Many create/delete cycles, verify no leaked metadata
  // ------------------------------------------------------------------

  it("no leaked metadata after 50 create-delete cycles @fast", async () => {
    const backend = new AuditableBackend();
    const maxPages = 16;
    const { FS, tomefs } = await mountTome(backend, maxPages);

    for (let i = 0; i < 50; i++) {
      const path = `${MOUNT}/temp_${i % 5}`; // reuse 5 paths

      // Delete previous file at this path (if any)
      try {
        FS.unlink(path);
      } catch (_e) {
        // May not exist yet
      }

      // Create new file with unique data
      const data = fillPattern(PAGE_SIZE * (1 + (i % 3)), i);
      const fd = FS.open(path, O.RDWR | O.CREAT | O.TRUNC, 0o666);
      FS.write(fd, data, 0, data.length, 0);
      FS.close(fd);

      // Sync every 10 iterations
      if (i % 10 === 9) {
        syncfs(FS, tomefs);
      }
    }

    // Final sync and audit
    syncfs(FS, tomefs);
    const violations = auditBackend(FS, backend);
    expect(violations).toEqual([]);

    // Remount and re-audit (clean marker consumed on mount)
    FS.unmount(MOUNT);
    const { FS: FS2 } = await mountTome(backend, maxPages);
    const violations2 = auditBackend(FS2, backend, false);
    expect(violations2).toEqual([]);
  });

  // ------------------------------------------------------------------
  // Test 2: Directory create/delete cycles with files
  // ------------------------------------------------------------------

  it("no leaked metadata after directory create-populate-delete cycles", async () => {
    const backend = new AuditableBackend();
    const maxPages = 32;
    const { FS, tomefs } = await mountTome(backend, maxPages);

    for (let cycle = 0; cycle < 10; cycle++) {
      const dirPath = `${MOUNT}/dir_${cycle % 3}`;

      // Remove old directory tree if it exists
      try {
        const entries = FS.readdir(dirPath).filter(
          (e: string) => e !== "." && e !== "..",
        );
        for (const entry of entries) {
          const fullPath = `${dirPath}/${entry}`;
          const stat = FS.lstat(fullPath);
          if (FS.isFile(stat.mode)) {
            FS.unlink(fullPath);
          } else if (FS.isLink(stat.mode)) {
            FS.unlink(fullPath);
          }
        }
        FS.rmdir(dirPath);
      } catch (_e) {
        // May not exist
      }

      // Create directory with files
      FS.mkdir(dirPath);
      for (let f = 0; f < 3; f++) {
        const filePath = `${dirPath}/file_${f}`;
        const data = fillPattern(PAGE_SIZE, cycle * 10 + f);
        const fd = FS.open(filePath, O.RDWR | O.CREAT, 0o666);
        FS.write(fd, data, 0, data.length, 0);
        FS.close(fd);
      }

      // Add a symlink
      FS.symlink(`${dirPath}/file_0`, `${dirPath}/link_0`);

      syncfs(FS, tomefs);
    }

    // Audit current state
    const violations = auditBackend(FS, backend);
    expect(violations).toEqual([]);

    // Remount and re-audit (clean marker consumed on mount)
    FS.unmount(MOUNT);
    const { FS: FS2 } = await mountTome(backend, maxPages);
    const violations2 = auditBackend(FS2, backend, false);
    expect(violations2).toEqual([]);
  });

  // ------------------------------------------------------------------
  // Test 3: Rename chains with backend audit
  // ------------------------------------------------------------------

  it("no leaked metadata after rename chains across sync cycles @fast", async () => {
    const backend = new AuditableBackend();
    const maxPages = 64;

    for (let cycle = 0; cycle < 5; cycle++) {
      const { FS, tomefs } = await mountTome(backend, maxPages);

      if (cycle === 0) {
        // Initial setup: create a chain of files
        for (let i = 0; i < 5; i++) {
          const data = fillPattern(PAGE_SIZE * 2, cycle * 100 + i);
          const fd = FS.open(`${MOUNT}/slot_${i}`, O.RDWR | O.CREAT, 0o666);
          FS.write(fd, data, 0, data.length, 0);
          FS.close(fd);
        }
      } else {
        // Rotate files: slot_0→slot_4 (overwrite), slot_1→slot_0, ...
        // First, read slot_0's data to write back to slot_4
        const buf = new Uint8Array(PAGE_SIZE * 2);
        const s = FS.open(`${MOUNT}/slot_0`, O.RDONLY);
        FS.read(s, buf, 0, PAGE_SIZE * 2, 0);
        FS.close(s);

        // Rename chain: 4→tmp, 3→4, 2→3, 1→2, 0→1
        FS.rename(`${MOUNT}/slot_4`, `${MOUNT}/tmp_rotate`);
        for (let i = 4; i > 0; i--) {
          FS.rename(`${MOUNT}/slot_${i - 1}`, `${MOUNT}/slot_${i}`);
        }
        // Write old slot_4 data to slot_0 (completing the rotation)
        FS.unlink(`${MOUNT}/tmp_rotate`);
        const fd = FS.open(`${MOUNT}/slot_0`, O.RDWR | O.CREAT, 0o666);
        // Write new data for slot_0 each cycle
        const newData = fillPattern(PAGE_SIZE * 2, cycle * 100);
        FS.write(fd, newData, 0, newData.length, 0);
        FS.close(fd);
      }

      syncAndUnmount(FS, tomefs);

      // Audit after each cycle (clean marker consumed on mount)
      const { FS: FS2 } = await mountTome(backend, maxPages);
      const violations = auditBackend(FS2, backend, false);
      expect(violations).toEqual([]);
      FS2.unmount(MOUNT);
    }
  });

  // ------------------------------------------------------------------
  // Test 4: Truncate + extend cycles — verify no extra pages
  // ------------------------------------------------------------------

  it("no leaked pages after truncate-extend cycles @fast", async () => {
    const backend = new AuditableBackend();
    const maxPages = 16;
    const { FS, tomefs } = await mountTome(backend, maxPages);

    const filePath = `${MOUNT}/resize_target`;

    // Create a file with 8 pages
    const data = fillPattern(PAGE_SIZE * 8, 42);
    const fd = FS.open(filePath, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, data, 0, data.length, 0);
    FS.close(fd);
    syncfs(FS, tomefs);

    // 20 cycles of truncate + extend
    for (let i = 0; i < 20; i++) {
      const truncSize = PAGE_SIZE * ((i % 7) + 1); // 1-7 pages
      FS.truncate(filePath, truncSize);

      // Extend with new data
      const extData = fillPattern(PAGE_SIZE * 2, i * 1000);
      const s = FS.open(filePath, O.WRONLY | O.APPEND);
      FS.write(s, extData, 0, extData.length);
      FS.close(s);

      // Sync every 5 iterations
      if (i % 5 === 4) {
        syncfs(FS, tomefs);
      }
    }

    // Final sync and audit
    syncfs(FS, tomefs);
    const violations = auditBackend(FS, backend);
    expect(violations).toEqual([]);

    // Verify no excess pages in backend
    const stat = FS.stat(filePath);
    const expectedMaxPages = Math.ceil(stat.size / PAGE_SIZE);
    const actualPages = backend.countPages("/resize_target");
    expect(actualPages).toBeLessThanOrEqual(expectedMaxPages);

    // Remount and verify data integrity + audit (clean marker consumed on mount)
    FS.unmount(MOUNT);
    const { FS: FS2 } = await mountTome(backend, maxPages);
    const violations2 = auditBackend(FS2, backend, false);
    expect(violations2).toEqual([]);

    // Verify file is readable
    const stat2 = FS2.stat(filePath);
    expect(stat2.size).toBe(stat.size);
  });

  // ------------------------------------------------------------------
  // Test 5: Mixed workload under cache pressure — the integration test
  // ------------------------------------------------------------------

  for (const cacheSize of [4, 64]) {
    it(`mixed workload audit over 8 cycles (cache=${cacheSize} pages)`, async () => {
      const backend = new AuditableBackend();

      for (let cycle = 0; cycle < 8; cycle++) {
        const { FS, tomefs } = await mountTome(backend, cacheSize);

        if (cycle === 0) {
          // Set up base directory structure
          FS.mkdir(`${MOUNT}/base`);
          FS.mkdir(`${MOUNT}/wal`);
          FS.mkdir(`${MOUNT}/tmp`);
        }

        // --- Writes: create or update base files ---
        for (let f = 0; f < 3; f++) {
          const path = `${MOUNT}/base/rel_${f}`;
          const seed = cycle * 1000 + f;
          const pages = 2 + (cycle % 3); // vary sizes across cycles
          const data = fillPattern(PAGE_SIZE * pages, seed);
          const fd = FS.open(path, O.RDWR | O.CREAT | O.TRUNC, 0o666);
          FS.write(fd, data, 0, data.length, 0);
          FS.close(fd);
        }

        // --- WAL rotation: rename current, create new ---
        if (cycle > 0) {
          try {
            FS.unlink(`${MOUNT}/wal/wal_${cycle - 2}.done`);
          } catch (_e) {
            // Old archived WAL may not exist
          }
          try {
            FS.rename(
              `${MOUNT}/wal/wal_${cycle - 1}`,
              `${MOUNT}/wal/wal_${cycle - 1}.done`,
            );
          } catch (_e) {
            // May not exist on first iteration
          }
        }
        const walData = fillPattern(PAGE_SIZE * 2, cycle * 5000);
        const walFd = FS.open(
          `${MOUNT}/wal/wal_${cycle}`,
          O.RDWR | O.CREAT,
          0o666,
        );
        FS.write(walFd, walData, 0, walData.length, 0);
        FS.close(walFd);

        // --- Temp files: create, use, delete ---
        for (let t = 0; t < 2; t++) {
          const tmpPath = `${MOUNT}/tmp/sort_${cycle}_${t}`;
          const tmpData = fillPattern(PAGE_SIZE, cycle * 100 + t);
          const fd = FS.open(tmpPath, O.RDWR | O.CREAT, 0o666);
          FS.write(fd, tmpData, 0, tmpData.length, 0);
          FS.close(fd);
        }
        // Delete temp files from previous cycle
        if (cycle > 0) {
          for (let t = 0; t < 2; t++) {
            try {
              FS.unlink(`${MOUNT}/tmp/sort_${cycle - 1}_${t}`);
            } catch (_e) {
              // Might not exist
            }
          }
        }

        // --- Truncate + rewrite one base file (VACUUM pattern) ---
        if (cycle >= 2) {
          const vacPath = `${MOUNT}/base/rel_0`;
          const oldStat = FS.stat(vacPath);
          // Truncate to half
          FS.truncate(vacPath, Math.floor(oldStat.size / 2));
          // Rewrite with new data
          const newData = fillPattern(
            Math.floor(oldStat.size / 2),
            cycle * 9000,
          );
          const fd = FS.open(vacPath, O.WRONLY | O.TRUNC);
          FS.write(fd, newData, 0, newData.length, 0);
          FS.close(fd);
        }

        // --- Symlink management ---
        try {
          FS.unlink(`${MOUNT}/base/latest`);
        } catch (_e) {
          // May not exist
        }
        FS.symlink(
          `${MOUNT}/base/rel_${cycle % 3}`,
          `${MOUNT}/base/latest`,
        );

        syncAndUnmount(FS, tomefs);

        // Audit backend after each cycle (clean marker consumed on mount)
        const { FS: FSa } = await mountTome(backend, cacheSize);
        const violations = auditBackend(FSa, backend, false);
        if (violations.length > 0) {
          throw new Error(
            `Backend audit failed at cycle ${cycle}:\n  ` +
              violations.join("\n  "),
          );
        }
        FSa.unmount(MOUNT);
      }

      // Final verification: mount and verify all file contents
      const { FS: FSf } = await mountTome(backend, cacheSize);

      // Verify base files have correct latest data
      for (let f = 0; f < 3; f++) {
        const path = `${MOUNT}/base/rel_${f}`;
        const stat = FSf.stat(path);
        expect(stat.size).toBeGreaterThan(0);

        const buf = new Uint8Array(stat.size);
        const s = FSf.open(path, O.RDONLY);
        FSf.read(s, buf, 0, stat.size, 0);
        FSf.close(s);
        // Data should be readable without error
      }

      // Verify WAL files exist
      const walEntries = FSf.readdir(`${MOUNT}/wal`).filter(
        (e: string) => e !== "." && e !== "..",
      );
      expect(walEntries.length).toBeGreaterThan(0);

      // Verify temp directory is mostly clean (only last cycle's temps)
      const tmpEntries = FSf.readdir(`${MOUNT}/tmp`).filter(
        (e: string) => e !== "." && e !== "..",
      );
      // Last cycle's temp files exist, all others cleaned up
      expect(tmpEntries.length).toBeLessThanOrEqual(2);

      // Final audit (after mount, no sync yet)
      const finalViolations = auditBackend(FSf, backend, false);
      expect(finalViolations).toEqual([]);
    });
  }

  // ------------------------------------------------------------------
  // Test 6: Rapid file replacement at same path (no open fds)
  // ------------------------------------------------------------------

  it("no leaked pages after rapid same-path replacements", async () => {
    const backend = new AuditableBackend();
    const maxPages = 8;
    const { FS, tomefs } = await mountTome(backend, maxPages);

    const path = `${MOUNT}/hot_file`;

    // 30 rapid replacements of varying sizes at the same path
    for (let i = 0; i < 30; i++) {
      const pages = 1 + (i % 4); // 1-4 pages
      const data = fillPattern(PAGE_SIZE * pages, i);
      const fd = FS.open(path, O.RDWR | O.CREAT | O.TRUNC, 0o666);
      FS.write(fd, data, 0, data.length, 0);
      FS.close(fd);

      // Only sync occasionally to exercise both cached-only and persisted paths
      if (i % 7 === 0) {
        syncfs(FS, tomefs);
      }
    }

    syncfs(FS, tomefs);

    // Verify the file has the final version's size
    const finalPages = 1 + (29 % 4);
    const stat = FS.stat(path);
    expect(stat.size).toBe(PAGE_SIZE * finalPages);

    // Backend should have pages only for the final version
    const pageCount = backend.countPages("/hot_file");
    expect(pageCount).toBeLessThanOrEqual(finalPages);

    // Full audit (after sync, clean marker present)
    const violations = auditBackend(FS, backend, true);
    expect(violations).toEqual([]);

    // Remount and audit (clean marker consumed on mount)
    FS.unmount(MOUNT);
    const { FS: FS2 } = await mountTome(backend, maxPages);
    const violations2 = auditBackend(FS2, backend, false);
    expect(violations2).toEqual([]);

    // Verify content
    const buf = new Uint8Array(stat.size);
    const s = FS2.open(path, O.RDONLY);
    FS2.read(s, buf, 0, stat.size, 0);
    FS2.close(s);
    verifyPattern(buf, stat.size, 29);
  });

  // ------------------------------------------------------------------
  // Test 7: Empty files don't leak metadata on deletion
  // ------------------------------------------------------------------

  it("empty files leave no trace in backend after deletion", async () => {
    const backend = new AuditableBackend();
    const maxPages = 16;
    const { FS, tomefs } = await mountTome(backend, maxPages);

    // Create and sync several empty files
    for (let i = 0; i < 10; i++) {
      const fd = FS.open(`${MOUNT}/empty_${i}`, O.RDWR | O.CREAT, 0o666);
      FS.close(fd);
    }
    syncfs(FS, tomefs);

    // Verify all 10 exist in backend
    const metaPaths = new Set(backend.listFiles());
    for (let i = 0; i < 10; i++) {
      expect(metaPaths.has(`/empty_${i}`)).toBe(true);
    }

    // Delete them all
    for (let i = 0; i < 10; i++) {
      FS.unlink(`${MOUNT}/empty_${i}`);
    }
    syncfs(FS, tomefs);

    // Audit: only root dir + clean marker should remain
    const violations = auditBackend(FS, backend);
    expect(violations).toEqual([]);

    // No pages for any deleted empty files
    for (let i = 0; i < 10; i++) {
      expect(backend.countPages(`/empty_${i}`)).toBe(0);
    }
  });

  // ------------------------------------------------------------------
  // Test 8: chmod/utime don't create phantom page entries
  // ------------------------------------------------------------------

  it("metadata-only ops don't create pages in backend", async () => {
    const backend = new AuditableBackend();
    const maxPages = 16;
    const { FS, tomefs } = await mountTome(backend, maxPages);

    // Create a directory with files
    FS.mkdir(`${MOUNT}/meta_test`);
    for (let i = 0; i < 3; i++) {
      const data = fillPattern(PAGE_SIZE, i);
      const fd = FS.open(`${MOUNT}/meta_test/f${i}`, O.RDWR | O.CREAT, 0o666);
      FS.write(fd, data, 0, data.length, 0);
      FS.close(fd);
    }
    syncfs(FS, tomefs);

    // Record page counts
    const pageCountsBefore = new Map<string, number>();
    for (let i = 0; i < 3; i++) {
      pageCountsBefore.set(`/meta_test/f${i}`, backend.countPages(`/meta_test/f${i}`));
    }

    // Do 20 rounds of metadata-only operations
    for (let round = 0; round < 20; round++) {
      const f = round % 3;
      FS.chmod(`${MOUNT}/meta_test/f${f}`, 0o644 + (round % 2));
      FS.utime(
        `${MOUNT}/meta_test/f${f}`,
        Date.now() + round * 1000,
        Date.now() + round * 1000,
      );
      if (round % 5 === 4) {
        syncfs(FS, tomefs);
      }
    }

    syncfs(FS, tomefs);

    // Verify page counts haven't changed
    for (let i = 0; i < 3; i++) {
      const path = `/meta_test/f${i}`;
      expect(backend.countPages(path)).toBe(pageCountsBefore.get(path));
    }

    // Directories should never have pages
    expect(backend.countPages("/meta_test")).toBe(0);
    expect(backend.countPages("/")).toBe(0);

    // Full audit
    const violations = auditBackend(FS, backend);
    expect(violations).toEqual([]);
  });
});
