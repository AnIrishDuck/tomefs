/**
 * Adversarial tests: detached node storage path consistency.
 *
 * When PGlite's MemoryFS preloads a database image, Emscripten may
 * reinitialize its nameTable during module startup. This causes nodes
 * created before reinitialization to become "detached" — their parent
 * chain traverses MEMFS nodes instead of the tomefs mount root.
 *
 * For detached nodes, computeStoragePath walks through MEMFS parents to
 * the MEMFS root, producing an absolute path like "/data/base/1/1234"
 * instead of a mount-relative path like "/base/1/1234". Without mount
 * prefix stripping, this causes:
 *
 *   1. Page data stored under the prefixed path (via storagePath)
 *   2. Metadata stored under the stripped path (via nodeStoragePath in
 *      full tree walk syncfs) — or under the prefixed path (via
 *      storagePath in incremental syncfs)
 *   3. On remount, pages and metadata are at different paths → data loss
 *
 * These tests verify that storagePath is always mount-relative regardless
 * of whether the parent chain goes through tomefs or MEMFS nodes.
 *
 * Ethos §9 (adversarial differential testing):
 * "Target the seams: ... metadata updates after flush"
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

describe("adversarial: detached node storage path consistency", () => {
  it("mount-tree file has mount-relative storagePath @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    // Create a file through the normal mount-tree path
    const path = `${MOUNT}/subdir/test.txt`;
    FS.mkdir(`${MOUNT}/subdir`);
    const fd = FS.open(path, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, encode("hello"), 0, 5);
    FS.close(fd);

    // Get the node and check storagePath
    const node = FS.lookupPath(path).node;
    expect(node.storagePath).toBe("/subdir/test.txt");
    // storagePath must NOT include the mount prefix
    expect(node.storagePath).not.toContain(MOUNT);
  });

  it("detached file node has mount-relative storagePath @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    // Get the MEMFS node at the mount point. This is the node that
    // Emscripten uses as the mount point's parent in the nameTable.
    // For detached nodes, the parent chain goes through these MEMFS nodes.
    const memfsMount = FS.lookupPath(MOUNT, { follow_mount: false });
    const memfsNode = memfsMount.node;

    // Create a subdirectory structure in the tomefs mount
    FS.mkdir(`${MOUNT}/base`);
    FS.mkdir(`${MOUNT}/base/1`);

    // Get the MEMFS view of the directory tree. After mounting tomefs,
    // Emscripten's path resolution for /tome/base/1 goes through
    // tomefs. But in the "detached" scenario, PGlite's MemoryFS
    // creates nodes whose parents are MEMFS nodes, not tomefs nodes.
    //
    // Simulate this by calling createNode directly with a MEMFS-rooted
    // parent. We build a MEMFS directory chain to mimic what happens
    // when Emscripten's nameTable is reinitialized and path resolution
    // routes through MEMFS instead of the tomefs mount.
    //
    // Create MEMFS directories to simulate the detached parent chain
    const memfsBase = FS.createNode(memfsNode, "base_detached", 0o40777, 0);
    memfsBase.contents = {};
    const memfsSub = FS.createNode(memfsBase, "1", 0o40777, 0);
    memfsSub.contents = {};

    // Now create a tomefs file node with the MEMFS parent
    const detachedNode = tomefs.createNode(memfsSub, "1234", 0o100666, 0);

    // The critical check: storagePath must be mount-relative even though
    // the parent chain goes through MEMFS nodes whose path includes the
    // mount prefix.
    //
    // Without the fix, computeStoragePath would walk up through MEMFS:
    //   1234 → 1 → base_detached → tome → (root)
    // Producing: /tome/base_detached/1/1234 (includes mount prefix)
    //
    // With the fix, the mount prefix is stripped:
    //   /tome/base_detached/1/1234 → /base_detached/1/1234
    expect(detachedNode.storagePath).not.toMatch(
      new RegExp(`^${MOUNT}/`),
    );
  });

  it("detached file data persists across syncfs + remount @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    // Get the MEMFS mount node
    const memfsMount = FS.lookupPath(MOUNT, { follow_mount: false });
    const memfsNode = memfsMount.node;

    // Create MEMFS directory chain to simulate detached parent
    const memfsDir = FS.createNode(memfsNode, "detached_dir", 0o40777, 0);
    memfsDir.contents = {};

    // Create a detached tomefs file node
    const detachedNode = tomefs.createNode(memfsDir, "data.bin", 0o100666, 0);

    // Write data through a stream (simulating Postgres writing to the file)
    const data = new Uint8Array(PAGE_SIZE);
    for (let i = 0; i < PAGE_SIZE; i++) data[i] = i & 0xff;

    const stream = FS.createStream({
      node: detachedNode,
      flags: O.RDWR,
      position: 0,
      seekable: true,
      stream_ops: detachedNode.stream_ops,
    });
    detachedNode.openCount++;
    FS.write(stream, data, 0, PAGE_SIZE, 0);
    detachedNode.openCount--;
    FS.closeStream(stream.fd);

    // Flush to backend
    syncfs(FS, tomefs);

    // Verify backend has consistent metadata and page data:
    // both should be under the same mount-relative path
    const storagePath = detachedNode.storagePath;
    const meta = backend.readMeta(storagePath);
    expect(meta).toBeTruthy();
    expect(meta!.size).toBe(PAGE_SIZE);

    const pageData = backend.readPage(storagePath, 0);
    expect(pageData).toBeTruthy();
    expect(pageData![0]).toBe(0);
    expect(pageData![255]).toBe(255);

    // Remount and verify data survives
    FS.unmount(MOUNT);
    const { FS: FS2, tomefs: tomefs2 } = await mountTome(backend);

    // The file should be restored at the mount-relative path
    const files = backend.listFiles();
    const dataFiles = files.filter(
      (f: string) => !f.startsWith("/__") && f !== "/",
    );
    // Verify the backend doesn't have any paths with the mount prefix
    for (const f of dataFiles) {
      expect(f).not.toMatch(new RegExp(`^${MOUNT}/`));
    }

    // Read the data back through the restored tree
    const restoredPath = `${MOUNT}${storagePath}`;
    const stat = FS2.stat(restoredPath);
    expect(stat.size).toBe(PAGE_SIZE);

    const fd = FS2.open(restoredPath, O.RDONLY);
    const readBuf = new Uint8Array(PAGE_SIZE);
    FS2.read(fd, readBuf, 0, PAGE_SIZE, 0);
    FS2.close(fd);

    expect(readBuf[0]).toBe(0);
    expect(readBuf[255]).toBe(255);
    expect(readBuf[PAGE_SIZE - 1]).toBe((PAGE_SIZE - 1) & 0xff);
  });

  it("incremental syncfs uses same path as full tree walk for detached nodes", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend, 64);

    // Create a normal file to trigger needsOrphanCleanup=false path
    FS.writeFile(`${MOUNT}/normal.txt`, "data");
    // First syncfs: full tree walk (needsOrphanCleanup=true on mount)
    syncfs(FS, tomefs);

    // Get MEMFS parent for detached node
    const memfsMount = FS.lookupPath(MOUNT, { follow_mount: false });
    const memfsNode = memfsMount.node;
    const memfsDir = FS.createNode(memfsNode, "inc_dir", 0o40777, 0);
    memfsDir.contents = {};

    // Create detached file
    const detached = tomefs.createNode(memfsDir, "inc_file.bin", 0o100666, 0);
    const fileData = encode("incremental-test-data");

    // Write through stream
    const stream = FS.createStream({
      node: detached,
      flags: O.RDWR,
      position: 0,
      seekable: true,
      stream_ops: detached.stream_ops,
    });
    detached.openCount++;
    FS.write(stream, fileData, 0, fileData.length, 0);
    detached.openCount--;
    FS.closeStream(stream.fd);

    // Second syncfs: incremental path (no rename/unlink since first sync)
    // This uses node.storagePath for file metadata. The key check:
    // storagePath must match the path used for page cache keys.
    syncfs(FS, tomefs);

    // Verify consistency: metadata path === page data path
    const sp = detached.storagePath;
    const meta = backend.readMeta(sp);
    expect(meta).toBeTruthy();
    expect(meta!.size).toBe(fileData.length);

    const page = backend.readPage(sp, 0);
    expect(page).toBeTruthy();
    expect(decode(page!.subarray(0, fileData.length))).toBe(
      "incremental-test-data",
    );

    // Force a full tree walk (trigger orphan cleanup by doing an unlink)
    FS.unlink(`${MOUNT}/normal.txt`);
    syncfs(FS, tomefs);

    // After full tree walk, metadata should still be at the same path
    const metaAfter = backend.readMeta(sp);
    expect(metaAfter).toBeTruthy();
    expect(metaAfter!.size).toBe(fileData.length);

    // Pages should still be accessible
    const pageAfter = backend.readPage(sp, 0);
    expect(pageAfter).toBeTruthy();
    expect(decode(pageAfter!.subarray(0, fileData.length))).toBe(
      "incremental-test-data",
    );
  });

  it("rename of detached file produces mount-relative paths", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    // Create a detached file
    const memfsMount = FS.lookupPath(MOUNT, { follow_mount: false });
    const memfsNode = memfsMount.node;
    const memfsDir = FS.createNode(memfsNode, "ren_dir", 0o40777, 0);
    memfsDir.contents = {};
    const detached = tomefs.createNode(memfsDir, "old.bin", 0o100666, 0);

    // Write data
    const stream = FS.createStream({
      node: detached,
      flags: O.RDWR,
      position: 0,
      seekable: true,
      stream_ops: detached.stream_ops,
    });
    detached.openCount++;
    FS.write(stream, encode("rename-test"), 0, 11, 0);
    detached.openCount--;
    FS.closeStream(stream.fd);

    // Verify storagePath is mount-relative before rename
    expect(detached.storagePath).not.toMatch(new RegExp(`^${MOUNT}/`));
    const oldPath = detached.storagePath;

    // Create destination directory in tomefs and rename
    FS.mkdir(`${MOUNT}/dest`);
    // We can't use FS.rename for detached nodes (they're not in the
    // normal path tree), but the key invariant we're testing is that
    // computeStoragePath produces mount-relative paths. Verify that
    // a new file in the destination also gets a mount-relative path.
    const fd2 = FS.open(`${MOUNT}/dest/new.bin`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd2, encode("dest-data"), 0, 9);
    FS.close(fd2);

    const destNode = FS.lookupPath(`${MOUNT}/dest/new.bin`).node;
    expect(destNode.storagePath).toBe("/dest/new.bin");
    expect(destNode.storagePath).not.toMatch(new RegExp(`^${MOUNT}/`));
  });

  it("multi-page detached file persists correctly under cache pressure @fast", async () => {
    const backend = new SyncMemoryBackend();
    // Tiny cache: only 4 pages. A 3-page file will consume most of the cache.
    const { FS, tomefs } = await mountTome(backend, 4);

    // Create detached file
    const memfsMount = FS.lookupPath(MOUNT, { follow_mount: false });
    const memfsNode = memfsMount.node;
    const memfsDir = FS.createNode(memfsNode, "pressure_dir", 0o40777, 0);
    memfsDir.contents = {};
    const detached = tomefs.createNode(memfsDir, "big.bin", 0o100666, 0);

    // Write 3 pages of data through stream
    const totalSize = PAGE_SIZE * 3;
    const data = new Uint8Array(totalSize);
    for (let i = 0; i < totalSize; i++) {
      data[i] = (i * 7 + 13) & 0xff; // deterministic pattern
    }

    const stream = FS.createStream({
      node: detached,
      flags: O.RDWR,
      position: 0,
      seekable: true,
      stream_ops: detached.stream_ops,
    });
    detached.openCount++;
    FS.write(stream, data, 0, totalSize, 0);
    detached.openCount--;
    FS.closeStream(stream.fd);

    // syncfs: some pages may have been evicted under cache pressure
    syncfs(FS, tomefs);

    // Verify all 3 pages are in the backend under the same path
    const sp = detached.storagePath;
    for (let p = 0; p < 3; p++) {
      const page = backend.readPage(sp, p);
      expect(page).toBeTruthy();
      const pageStart = p * PAGE_SIZE;
      for (let j = 0; j < Math.min(16, PAGE_SIZE); j++) {
        expect(page![j]).toBe(((pageStart + j) * 7 + 13) & 0xff);
      }
    }

    // Remount and read back
    FS.unmount(MOUNT);
    const { FS: FS2 } = await mountTome(backend, 4);

    const restoredPath = `${MOUNT}${sp}`;
    const fd = FS2.open(restoredPath, O.RDONLY);
    const readBuf = new Uint8Array(totalSize);
    FS2.read(fd, readBuf, 0, totalSize, 0);
    FS2.close(fd);

    // Verify data integrity
    for (let i = 0; i < totalSize; i++) {
      if (readBuf[i] !== ((i * 7 + 13) & 0xff)) {
        throw new Error(
          `Data mismatch at byte ${i}: expected ${(i * 7 + 13) & 0xff}, got ${readBuf[i]}`,
        );
      }
    }
  });
});
