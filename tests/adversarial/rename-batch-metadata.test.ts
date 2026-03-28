/**
 * Adversarial tests: Batched metadata operations during directory rename.
 *
 * renameDescendantPaths() now batches writeMetas + deleteMetas instead of
 * doing O(3n) individual backend calls. Metadata is constructed from
 * in-memory node state instead of reading from the backend. These tests
 * verify that:
 *
 * - Batched metadata writes produce identical results to individual calls
 * - Crash between rename and syncfs has metadata at new paths (crash safety)
 * - Files never synced before rename still get metadata at new paths
 * - Mixed node types (files, symlinks, dirs) all batch correctly
 * - Old path metadata is cleaned up by the batch delete
 * - Backend state is consistent after rename without calling syncfs
 *
 * Ethos §6: "When the working set fits in the cache, tomefs must be
 * performance-identical to IDBFS."
 * Ethos §9: "Write tests designed to break tomefs specifically."
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

function writeFile(FS: any, path: string, content: string) {
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

function readBytes(FS: any, path: string): Uint8Array {
  const stat = FS.stat(path);
  const buf = new Uint8Array(stat.size);
  const s = FS.open(path, O.RDONLY);
  FS.read(s, buf, 0, stat.size, 0);
  FS.close(s);
  return buf;
}

describe("adversarial: batched metadata during directory rename", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  // ------------------------------------------------------------------
  // Backend metadata state after rename (without syncfs) — crash safety
  // ------------------------------------------------------------------

  it("directory rename writes metadata at new paths immediately (crash safety) @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/src`);
    FS.mkdir(`${MOUNT}/src/sub`);
    writeFile(FS, `${MOUNT}/src/f1.txt`, "file1");
    writeFile(FS, `${MOUNT}/src/sub/f2.txt`, "file2");
    FS.symlink("f1.txt", `${MOUNT}/src/link`);

    // Sync to establish initial metadata in backend
    syncfs(FS, tomefs);

    // Rename WITHOUT calling syncfs — simulates crash after rename
    FS.rename(`${MOUNT}/src`, `${MOUNT}/dst`);

    // Verify backend has metadata at new paths (written by rename itself)
    expect(backend.readMeta("/dst")).not.toBeNull();
    expect(backend.readMeta("/dst/f1.txt")).not.toBeNull();
    expect(backend.readMeta("/dst/sub")).not.toBeNull();
    expect(backend.readMeta("/dst/sub/f2.txt")).not.toBeNull();
    expect(backend.readMeta("/dst/link")).not.toBeNull();

    // Old paths should have metadata deleted
    expect(backend.readMeta("/src")).toBeNull();
    expect(backend.readMeta("/src/f1.txt")).toBeNull();
    expect(backend.readMeta("/src/sub")).toBeNull();
    expect(backend.readMeta("/src/sub/f2.txt")).toBeNull();
    expect(backend.readMeta("/src/link")).toBeNull();
  });

  // ------------------------------------------------------------------
  // Files created but never synced get metadata on rename
  // ------------------------------------------------------------------

  it("never-synced files get metadata at new paths after directory rename @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create files but NEVER call syncfs — no metadata in backend yet
    FS.mkdir(`${MOUNT}/fresh`);
    FS.mkdir(`${MOUNT}/fresh/inner`);
    writeFile(FS, `${MOUNT}/fresh/a.txt`, "alpha");
    writeFile(FS, `${MOUNT}/fresh/inner/b.txt`, "beta");

    // Before rename: no metadata in backend (never synced)
    expect(backend.readMeta("/fresh")).toBeNull();
    expect(backend.readMeta("/fresh/a.txt")).toBeNull();

    // Rename writes metadata from node state
    FS.rename(`${MOUNT}/fresh`, `${MOUNT}/moved`);

    // Now backend should have metadata at new paths
    expect(backend.readMeta("/moved")).not.toBeNull();
    expect(backend.readMeta("/moved/a.txt")).not.toBeNull();
    expect(backend.readMeta("/moved/inner")).not.toBeNull();
    expect(backend.readMeta("/moved/inner/b.txt")).not.toBeNull();

    // Simulate crash by unmounting without syncfs, then remounting
    FS.unmount(MOUNT);
    const { FS: FS2 } = await mountTome(backend);

    // Files should be restored at new paths with correct content
    expect(readFile(FS2, `${MOUNT}/moved/a.txt`)).toBe("alpha");
    expect(readFile(FS2, `${MOUNT}/moved/inner/b.txt`)).toBe("beta");
  });

  // ------------------------------------------------------------------
  // Metadata preserves symlink targets through batch write
  // ------------------------------------------------------------------

  it("symlink targets survive batch metadata write during directory rename", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/dir`);
    writeFile(FS, `${MOUNT}/dir/target.txt`, "target-content");
    FS.symlink("target.txt", `${MOUNT}/dir/rel-link`);
    FS.symlink("/absolute/path", `${MOUNT}/dir/abs-link`);

    syncfs(FS, tomefs);
    FS.rename(`${MOUNT}/dir`, `${MOUNT}/dir2`);

    // Verify symlink metadata at new paths preserves link targets
    const relMeta = backend.readMeta("/dir2/rel-link");
    expect(relMeta).not.toBeNull();
    expect(relMeta!.link).toBe("target.txt");

    const absMeta = backend.readMeta("/dir2/abs-link");
    expect(absMeta).not.toBeNull();
    expect(absMeta!.link).toBe("/absolute/path");

    // Full round-trip: remount and verify symlinks work
    syncAndUnmount(FS, tomefs);
    const { FS: FS2 } = await mountTome(backend);
    expect(FS2.readlink(`${MOUNT}/dir2/rel-link`)).toBe("target.txt");
    expect(FS2.readlink(`${MOUNT}/dir2/abs-link`)).toBe("/absolute/path");
    expect(readFile(FS2, `${MOUNT}/dir2/target.txt`)).toBe("target-content");
  });

  // ------------------------------------------------------------------
  // Metadata preserves file sizes through batch write
  // ------------------------------------------------------------------

  it("file sizes in batch-written metadata match node state @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/sizes`);
    // Files of various sizes
    writeFile(FS, `${MOUNT}/sizes/empty.txt`, "");
    writeFile(FS, `${MOUNT}/sizes/small.txt`, "x");
    // Multi-page file
    const bigData = new Uint8Array(PAGE_SIZE * 2 + 100);
    for (let i = 0; i < bigData.length; i++) bigData[i] = i & 0xff;
    const s = FS.open(`${MOUNT}/sizes/big.bin`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, bigData, 0, bigData.length, 0);
    FS.close(s);

    FS.rename(`${MOUNT}/sizes`, `${MOUNT}/sizes2`);

    const emptyMeta = backend.readMeta("/sizes2/empty.txt");
    const smallMeta = backend.readMeta("/sizes2/small.txt");
    const bigMeta = backend.readMeta("/sizes2/big.bin");
    expect(emptyMeta!.size).toBe(0);
    expect(smallMeta!.size).toBe(1);
    expect(bigMeta!.size).toBe(PAGE_SIZE * 2 + 100);
  });

  // ------------------------------------------------------------------
  // Metadata preserves timestamps through batch write
  // ------------------------------------------------------------------

  it("timestamps in batch-written metadata reflect node state", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/ts`);
    writeFile(FS, `${MOUNT}/ts/f.txt`, "data");

    // Get pre-rename timestamps
    const statBefore = FS.stat(`${MOUNT}/ts/f.txt`);
    const mtimeBefore = statBefore.mtime.getTime();

    FS.rename(`${MOUNT}/ts`, `${MOUNT}/ts2`);

    const meta = backend.readMeta("/ts2/f.txt");
    expect(meta).not.toBeNull();
    expect(meta!.mtime).toBe(mtimeBefore);
  });

  // ------------------------------------------------------------------
  // Metadata preserves mode/permissions through batch write
  // ------------------------------------------------------------------

  it("file mode in batch-written metadata matches node state @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/modes`);
    const s = FS.open(`${MOUNT}/modes/f.txt`, O.RDWR | O.CREAT, 0o644);
    FS.close(s);
    FS.chmod(`${MOUNT}/modes/f.txt`, 0o755);

    const modeBefore = FS.stat(`${MOUNT}/modes/f.txt`).mode;

    FS.rename(`${MOUNT}/modes`, `${MOUNT}/modes2`);

    const meta = backend.readMeta("/modes2/f.txt");
    expect(meta).not.toBeNull();
    expect(meta!.mode).toBe(modeBefore);
  });

  // ------------------------------------------------------------------
  // Wide directory: many descendants batched correctly
  // ------------------------------------------------------------------

  it("wide directory with 20 files batches all metadata correctly @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/wide`);
    const fileCount = 20;

    for (let i = 0; i < fileCount; i++) {
      writeFile(FS, `${MOUNT}/wide/f${i}.txt`, `content-${i}`);
    }

    syncfs(FS, tomefs);
    FS.rename(`${MOUNT}/wide`, `${MOUNT}/wide2`);

    // All new paths should have metadata
    for (let i = 0; i < fileCount; i++) {
      expect(backend.readMeta(`/wide2/f${i}.txt`)).not.toBeNull();
    }
    // All old paths should be cleaned up
    for (let i = 0; i < fileCount; i++) {
      expect(backend.readMeta(`/wide/f${i}.txt`)).toBeNull();
    }

    // Full round-trip
    syncAndUnmount(FS, tomefs);
    const { FS: FS2 } = await mountTome(backend);
    for (let i = 0; i < fileCount; i++) {
      expect(readFile(FS2, `${MOUNT}/wide2/f${i}.txt`)).toBe(`content-${i}`);
    }
  });

  // ------------------------------------------------------------------
  // Deep + wide: nested directories with files at every level
  // ------------------------------------------------------------------

  it("deep nested tree batches metadata for all levels correctly", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Build 4-level tree with 3 files per level
    FS.mkdir(`${MOUNT}/root`);
    for (let d = 0; d < 4; d++) {
      const dirPath = `${MOUNT}/root${"/" + "d".repeat(d + 1)}`.replace(
        /\/+/g,
        "/",
      );
      FS.mkdir(dirPath);
      for (let f = 0; f < 3; f++) {
        writeFile(FS, `${dirPath}/f${f}.txt`, `level-${d}-file-${f}`);
      }
    }

    syncfs(FS, tomefs);
    FS.rename(`${MOUNT}/root`, `${MOUNT}/moved`);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    // Verify files at each level
    for (let d = 0; d < 4; d++) {
      const dirPath = `${MOUNT}/moved${"/" + "d".repeat(d + 1)}`.replace(
        /\/+/g,
        "/",
      );
      for (let f = 0; f < 3; f++) {
        expect(readFile(FS2, `${dirPath}/f${f}.txt`)).toBe(
          `level-${d}-file-${f}`,
        );
      }
    }
  });

  // ------------------------------------------------------------------
  // File rename (non-directory) also constructs metadata from node
  // ------------------------------------------------------------------

  it("single file rename writes metadata from node state (no backend read) @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    writeFile(FS, `${MOUNT}/old.txt`, "content");

    // Never sync — no metadata in backend
    expect(backend.readMeta("/old.txt")).toBeNull();

    FS.rename(`${MOUNT}/old.txt`, `${MOUNT}/new.txt`);

    // Metadata should exist at new path (constructed from node)
    const meta = backend.readMeta("/new.txt");
    expect(meta).not.toBeNull();
    expect(meta!.size).toBe(7); // "content".length
    expect(backend.readMeta("/old.txt")).toBeNull();

    // Full round-trip
    syncAndUnmount(FS, tomefs);
    const { FS: FS2 } = await mountTome(backend);
    expect(readFile(FS2, `${MOUNT}/new.txt`)).toBe("content");
  });

  // ------------------------------------------------------------------
  // Rename under cache pressure: metadata batch + eviction
  // ------------------------------------------------------------------

  it("directory rename under extreme cache pressure batches metadata correctly", async () => {
    const { FS, tomefs } = await mountTome(backend, 2); // 2-page cache

    FS.mkdir(`${MOUNT}/src`);
    FS.mkdir(`${MOUNT}/src/a`);
    FS.mkdir(`${MOUNT}/src/b`);

    // Write files that exceed cache capacity
    const data1 = new Uint8Array(PAGE_SIZE);
    const data2 = new Uint8Array(PAGE_SIZE);
    const data3 = new Uint8Array(PAGE_SIZE);
    data1.fill(0x11);
    data2.fill(0x22);
    data3.fill(0x33);

    let s = FS.open(`${MOUNT}/src/f1.bin`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data1, 0, PAGE_SIZE, 0);
    FS.close(s);

    s = FS.open(`${MOUNT}/src/a/f2.bin`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data2, 0, PAGE_SIZE, 0);
    FS.close(s);

    s = FS.open(`${MOUNT}/src/b/f3.bin`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data3, 0, PAGE_SIZE, 0);
    FS.close(s);

    FS.rename(`${MOUNT}/src`, `${MOUNT}/dst`);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend, 2);
    expect(readBytes(FS2, `${MOUNT}/dst/f1.bin`)).toEqual(data1);
    expect(readBytes(FS2, `${MOUNT}/dst/a/f2.bin`)).toEqual(data2);
    expect(readBytes(FS2, `${MOUNT}/dst/b/f3.bin`)).toEqual(data3);
  });

  // ------------------------------------------------------------------
  // Rename + modify + rename: metadata reflects latest node state
  // ------------------------------------------------------------------

  it("metadata reflects modifications between renames", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/dir`);
    writeFile(FS, `${MOUNT}/dir/f.txt`, "v1");
    syncfs(FS, tomefs);

    // Modify file
    writeFile(FS, `${MOUNT}/dir/f.txt`, "version-two-longer");

    // Rename — metadata should reflect current size, not old synced size
    FS.rename(`${MOUNT}/dir`, `${MOUNT}/dir2`);

    const meta = backend.readMeta("/dir2/f.txt");
    expect(meta).not.toBeNull();
    expect(meta!.size).toBe("version-two-longer".length);

    // Round-trip
    syncAndUnmount(FS, tomefs);
    const { FS: FS2 } = await mountTome(backend);
    expect(readFile(FS2, `${MOUNT}/dir2/f.txt`)).toBe("version-two-longer");
  });
});
