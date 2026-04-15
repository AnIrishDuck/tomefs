/**
 * Adversarial tests: directory rename dirty tracking efficiency.
 *
 * When a directory is renamed, renameDescendantPaths() writes metadata for
 * all descendants to the backend at their new paths. After this, the
 * descendants' metadata is already persisted — no need to re-write on the
 * next syncfs. Without dirty flag cleanup, the incremental syncfs path
 * (O(dirty)) degrades to O(descendants) for every sync after a directory
 * rename, wasting backend writes in PGlite workloads with many files.
 *
 * These tests verify that:
 *   - Dirty flags are cleared on descendants after renameDescendantPaths
 *   - Incremental syncfs does NOT redundantly re-write descendant metadata
 *   - Subsequent writes to descendants correctly re-set dirty flags
 *   - Data integrity is preserved across rename + syncfs + remount
 *
 * Ethos §6: "When the working set fits in the cache, tomefs must be
 * performance-identical to IDBFS."
 * Ethos §9: "Target the seams: ... dirty flush ordering"
 */
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { describe, it, expect, beforeEach } from "vitest";
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
 * A SyncMemoryBackend that counts syncAll calls and records which metadata
 * paths are written. Not a mock — delegates to the real implementation and
 * records calls as a side effect (ethos §5).
 */
class CountingBackend extends SyncMemoryBackend {
  syncAllCalls = 0;
  lastSyncAllMetaPaths: string[] = [];

  syncAll(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
    metas: Array<{ path: string; meta: FileMeta }>,
  ): void {
    this.syncAllCalls++;
    this.lastSyncAllMetaPaths = metas.map((m) => m.path);
    super.syncAll(pages, metas);
  }

  resetCounters(): void {
    this.syncAllCalls = 0;
    this.lastSyncAllMetaPaths = [];
  }
}

function encode(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

function decode(buf: Uint8Array, length?: number): string {
  return new TextDecoder().decode(
    length !== undefined ? buf.subarray(0, length) : buf,
  );
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
  return { FS, tomefs, Module };
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

function writeFile(FS: any, path: string, content: string): void {
  const data = encode(content);
  const s = FS.open(path, O.RDWR | O.CREAT, 0o666);
  FS.write(s, data, 0, data.length, 0);
  FS.close(s);
}

function readFile(FS: any, path: string): string {
  const stat = FS.stat(path);
  const buf = new Uint8Array(stat.size);
  const s = FS.open(path, O.RDONLY);
  const n = FS.read(s, buf, 0, stat.size, 0);
  FS.close(s);
  return decode(buf, n);
}

describe("adversarial: directory rename dirty tracking efficiency", () => {
  let backend: CountingBackend;

  beforeEach(() => {
    backend = new CountingBackend();
  });

  // ------------------------------------------------------------------
  // Core test: rename clears dirty flags on descendants
  // ------------------------------------------------------------------

  it("syncfs after dir rename does not re-write descendant metadata @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create directory with several files
    FS.mkdir(`${MOUNT}/src`);
    FS.mkdir(`${MOUNT}/src/sub`);
    writeFile(FS, `${MOUNT}/src/a.txt`, "aaa");
    writeFile(FS, `${MOUNT}/src/b.txt`, "bbb");
    writeFile(FS, `${MOUNT}/src/sub/c.txt`, "ccc");
    FS.symlink("a.txt", `${MOUNT}/src/link`);

    // First syncfs — writes everything, clears all dirty flags
    syncfs(FS, tomefs);

    // Re-write files to make them dirty again. This is the key scenario:
    // files are dirty BEFORE the rename, so without dirty flag cleanup
    // in renameDescendantPaths, they'd be redundantly re-persisted.
    writeFile(FS, `${MOUNT}/src/a.txt`, "aaa");
    writeFile(FS, `${MOUNT}/src/b.txt`, "bbb");
    writeFile(FS, `${MOUNT}/src/sub/c.txt`, "ccc");

    backend.resetCounters();

    // Rename the directory — renameDescendantPaths writes metadata at new
    // paths and should clear dirty flags on all descendants.
    FS.rename(`${MOUNT}/src`, `${MOUNT}/dst`);

    // Second syncfs — incremental path. The only dirty metadata should be
    // the parent directories (timestamps updated by rename), NOT the
    // descendants whose metadata was already written during rename.
    syncfs(FS, tomefs);

    // Verify: the syncAll metadata batch should contain ONLY the clean
    // marker + parent dirs (mount root updated by rename), NOT the
    // descendant files/symlinks/subdirs.
    const descendantPaths = ["/dst/a.txt", "/dst/b.txt", "/dst/sub/c.txt",
      "/dst/sub", "/dst/link"];
    for (const path of descendantPaths) {
      expect(backend.lastSyncAllMetaPaths).not.toContain(path);
    }

    // Verify data integrity after the optimization
    expect(readFile(FS, `${MOUNT}/dst/a.txt`)).toBe("aaa");
    expect(readFile(FS, `${MOUNT}/dst/b.txt`)).toBe("bbb");
    expect(readFile(FS, `${MOUNT}/dst/sub/c.txt`)).toBe("ccc");
    expect(FS.readlink(`${MOUNT}/dst/link`)).toBe("a.txt");
  });

  // ------------------------------------------------------------------
  // Subsequent writes re-set dirty flags correctly
  // ------------------------------------------------------------------

  it("write after rename correctly re-sets dirty flag @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/dir`);
    writeFile(FS, `${MOUNT}/dir/file.txt`, "original");

    // Sync and clear counters
    syncfs(FS, tomefs);
    backend.resetCounters();

    // Rename the directory (clears dirty flags)
    FS.rename(`${MOUNT}/dir`, `${MOUNT}/moved`);

    // Modify the file AFTER rename — should re-set dirty flag
    writeFile(FS, `${MOUNT}/moved/file.txt`, "modified");

    // Sync — the modified file should be in the metadata batch
    syncfs(FS, tomefs);

    expect(backend.lastSyncAllMetaPaths).toContain("/moved/file.txt");
    expect(readFile(FS, `${MOUNT}/moved/file.txt`)).toBe("modified");
  });

  // ------------------------------------------------------------------
  // Multi-cycle: rename, sync, rename again, sync
  // ------------------------------------------------------------------

  it("double rename across sync cycles has minimal metadata writes @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/step1`);
    writeFile(FS, `${MOUNT}/step1/data.txt`, "persistent");

    // Initial sync
    syncfs(FS, tomefs);

    // Re-write to make dirty before first rename
    writeFile(FS, `${MOUNT}/step1/data.txt`, "persistent");

    // First rename + sync
    FS.rename(`${MOUNT}/step1`, `${MOUNT}/step2`);
    backend.resetCounters();
    syncfs(FS, tomefs);

    // Descendant should NOT be in the sync batch
    expect(backend.lastSyncAllMetaPaths).not.toContain("/step2/data.txt");

    // Re-write to make dirty before second rename
    writeFile(FS, `${MOUNT}/step2/data.txt`, "persistent");

    // Second rename + sync
    FS.rename(`${MOUNT}/step2`, `${MOUNT}/step3`);
    backend.resetCounters();
    syncfs(FS, tomefs);

    // Descendant should still NOT be redundantly written
    expect(backend.lastSyncAllMetaPaths).not.toContain("/step3/data.txt");

    // Data integrity preserved
    expect(readFile(FS, `${MOUNT}/step3/data.txt`)).toBe("persistent");
  });

  // ------------------------------------------------------------------
  // Large tree: efficiency matters most with many descendants
  // ------------------------------------------------------------------

  it("rename of wide directory avoids O(descendants) redundant writes", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/wide`);
    const fileCount = 20;
    for (let i = 0; i < fileCount; i++) {
      writeFile(FS, `${MOUNT}/wide/f${i}.txt`, `content-${i}`);
    }

    // Initial sync writes all metadata
    syncfs(FS, tomefs);

    // Re-write all files to make them dirty. Without dirty flag cleanup,
    // the next syncfs after rename would re-write all 20 files' metadata.
    for (let i = 0; i < fileCount; i++) {
      writeFile(FS, `${MOUNT}/wide/f${i}.txt`, `content-${i}`);
    }

    backend.resetCounters();

    // Rename the wide directory
    FS.rename(`${MOUNT}/wide`, `${MOUNT}/wide_moved`);

    // Sync — should NOT write metadata for all 20 files
    syncfs(FS, tomefs);

    // Count how many descendant file paths are in the sync batch
    const descendantCount = backend.lastSyncAllMetaPaths.filter(
      (p) => p.startsWith("/wide_moved/f"),
    ).length;
    expect(descendantCount).toBe(0);

    // Data integrity
    for (let i = 0; i < fileCount; i++) {
      expect(readFile(FS, `${MOUNT}/wide_moved/f${i}.txt`)).toBe(`content-${i}`);
    }
  });

  // ------------------------------------------------------------------
  // Persistence: verify data survives rename + syncfs + remount
  // ------------------------------------------------------------------

  it("data survives rename + dirty-optimized syncfs + remount @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/orig`);
    FS.mkdir(`${MOUNT}/orig/nested`);
    writeFile(FS, `${MOUNT}/orig/a.txt`, "alpha");
    writeFile(FS, `${MOUNT}/orig/nested/b.txt`, "beta");
    FS.symlink("a.txt", `${MOUNT}/orig/link`);

    // Initial sync + rename + optimized sync
    syncfs(FS, tomefs);
    FS.rename(`${MOUNT}/orig`, `${MOUNT}/final`);
    syncAndUnmount(FS, tomefs);

    // Remount and verify everything survived
    const { FS: FS2 } = await mountTome(backend);
    expect(readFile(FS2, `${MOUNT}/final/a.txt`)).toBe("alpha");
    expect(readFile(FS2, `${MOUNT}/final/nested/b.txt`)).toBe("beta");
    expect(FS2.readlink(`${MOUNT}/final/link`)).toBe("a.txt");

    // Old paths must not exist
    expect(() => FS2.stat(`${MOUNT}/orig`)).toThrow();
  });

  // ------------------------------------------------------------------
  // Mixed: some descendants dirty, some clean, then rename
  // ------------------------------------------------------------------

  it("rename with mixed dirty/clean descendants clears all correctly", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/mix`);
    writeFile(FS, `${MOUNT}/mix/clean.txt`, "will-not-change");
    writeFile(FS, `${MOUNT}/mix/dirty.txt`, "will-change");

    // Initial sync — both files are now clean
    syncfs(FS, tomefs);

    // Make only one file dirty (truncate first to avoid old data leaking)
    FS.truncate(`${MOUNT}/mix/dirty.txt`, 0);
    writeFile(FS, `${MOUNT}/mix/dirty.txt`, "changed");

    // Rename — renameDescendantPaths writes metadata for both and
    // should clear dirty flags on both
    FS.rename(`${MOUNT}/mix`, `${MOUNT}/mix_moved`);
    backend.resetCounters();

    // Sync — neither file should need metadata re-write
    syncfs(FS, tomefs);

    expect(backend.lastSyncAllMetaPaths).not.toContain("/mix_moved/clean.txt");
    expect(backend.lastSyncAllMetaPaths).not.toContain("/mix_moved/dirty.txt");

    // But data is correct
    expect(readFile(FS, `${MOUNT}/mix_moved/clean.txt`)).toBe("will-not-change");
    expect(readFile(FS, `${MOUNT}/mix_moved/dirty.txt`)).toBe("changed");
  });

  // ------------------------------------------------------------------
  // Cache pressure: rename under eviction doesn't lose dirty info
  // ------------------------------------------------------------------

  it("rename under cache pressure preserves data without redundant writes @fast", async () => {
    const { FS, tomefs } = await mountTome(backend, 4); // tiny cache

    FS.mkdir(`${MOUNT}/pressure`);
    // Write multi-page files to trigger eviction
    const bigData = new Uint8Array(PAGE_SIZE * 3);
    for (let i = 0; i < bigData.length; i++) bigData[i] = (i * 7 + 3) & 0xff;

    const s = FS.open(`${MOUNT}/pressure/big.bin`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, bigData, 0, bigData.length, 0);
    FS.close(s);

    writeFile(FS, `${MOUNT}/pressure/small.txt`, "small-data");

    syncfs(FS, tomefs);
    backend.resetCounters();

    // Rename under cache pressure
    FS.rename(`${MOUNT}/pressure`, `${MOUNT}/moved`);
    syncfs(FS, tomefs);

    // Descendants should not be redundantly written
    expect(backend.lastSyncAllMetaPaths).not.toContain("/moved/big.bin");
    expect(backend.lastSyncAllMetaPaths).not.toContain("/moved/small.txt");

    // Verify data integrity after eviction + rename
    const readBuf = new Uint8Array(PAGE_SIZE * 3);
    const rs = FS.open(`${MOUNT}/moved/big.bin`, O.RDONLY);
    FS.read(rs, readBuf, 0, readBuf.length, 0);
    FS.close(rs);
    expect(readBuf).toEqual(bigData);
    expect(readFile(FS, `${MOUNT}/moved/small.txt`)).toBe("small-data");
  });
});
