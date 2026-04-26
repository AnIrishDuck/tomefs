/**
 * Tests for syncfs crash-safety ordering guarantees.
 *
 * Verifies that syncfs persists current metadata before deleting stale
 * entries. This ordering ensures that if interrupted mid-operation (e.g.,
 * tab close during IDB transactions), current data is never lost.
 */
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { describe, it, expect, beforeEach } from "vitest";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { createTomeFS } from "../../src/tomefs.js";
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

function encode(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

function decode(buf: Uint8Array, length?: number): string {
  return new TextDecoder().decode(
    length !== undefined ? buf.subarray(0, length) : buf,
  );
}

/**
 * A SyncMemoryBackend that records writeMeta/deleteMeta operations
 * for verifying ordering guarantees. Not a mock — it delegates to
 * the real implementation and records calls as a side effect.
 */
class RecordingBackend extends SyncMemoryBackend {
  operations: Array<{
    op: "writeMeta" | "deleteMeta" | "deleteFile" | "deleteAll" | "readMeta" | "listFiles";
    path: string;
  }> = [];
  recording = false;

  startRecording(): void {
    this.operations = [];
    this.recording = true;
  }

  stopRecording(): void {
    this.recording = false;
  }

  writeMeta(path: string, meta: FileMeta): void {
    if (this.recording) {
      this.operations.push({ op: "writeMeta", path });
    }
    super.writeMeta(path, meta);
  }

  writeMetas(entries: Array<{ path: string; meta: FileMeta }>): void {
    if (this.recording) {
      for (const { path } of entries) {
        this.operations.push({ op: "writeMeta", path });
      }
    }
    super.writeMetas(entries);
  }

  deleteMeta(path: string): void {
    if (this.recording) {
      this.operations.push({ op: "deleteMeta", path });
    }
    super.deleteMeta(path);
  }

  deleteMetas(paths: string[]): void {
    if (this.recording) {
      for (const path of paths) {
        this.operations.push({ op: "deleteMeta", path });
      }
    }
    super.deleteMetas(paths);
  }

  deleteFile(path: string): void {
    if (this.recording) {
      this.operations.push({ op: "deleteFile", path });
    }
    super.deleteFile(path);
  }

  readMeta(path: string): FileMeta | null {
    if (this.recording) {
      this.operations.push({ op: "readMeta", path });
    }
    return super.readMeta(path);
  }

  listFiles(): string[] {
    if (this.recording) {
      this.operations.push({ op: "listFiles", path: "*" });
    }
    return super.listFiles();
  }

  deleteAll(paths: string[]): void {
    if (this.recording) {
      for (const path of paths) {
        this.operations.push({ op: "deleteAll", path });
      }
    }
    super.deleteAll(paths);
  }
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

describe("syncfs safety", () => {
  let backend: RecordingBackend;

  beforeEach(() => {
    backend = new RecordingBackend();
  });

  it("writes current metadata before deleting stale metadata @fast", async () => {
    // Setup: create two files and sync to populate backend
    const { FS, tomefs } = await mountTome(backend);
    let s = FS.open(`${MOUNT}/keep`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("kept"), 0, 4);
    FS.close(s);
    s = FS.open(`${MOUNT}/stale`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("gone"), 0, 4);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // Simulate a crash: remove the clean-shutdown marker and leave stale
    // metadata in the backend. On remount, the missing marker triggers
    // needsOrphanCleanup = true and the first syncfs does a full tree walk.
    backend.deleteMeta("/__tomefs_clean");

    // Remount — restoreTree sees no clean marker, sets needsOrphanCleanup
    const { FS: FS2, tomefs: t2 } = await mountTome(backend);
    // Modify "keep" so its metadata is dirty and will be written during sync
    const s2 = FS2.open(`${MOUNT}/keep`, O.RDWR);
    FS2.write(s2, encode("kept"), 0, 4);
    FS2.close(s2);
    FS2.unlink(`${MOUNT}/stale`);

    // The unlink already called deleteMeta eagerly. Now we need a scenario
    // where syncfs itself must do the delete. Manually re-add stale metadata
    // to simulate leftover from a prior interrupted sync.
    backend.writeMeta("/stale", {
      size: 4,
      mode: 0o100666,
      ctime: Date.now(),
      mtime: Date.now(),
    });

    backend.startRecording();
    syncfs(FS2, t2);
    backend.stopRecording();

    // All writeMeta calls should come before any deleteMeta for stale paths
    const ops = backend.operations;
    const lastWriteIndex = ops.reduce(
      (max, op, i) => (op.op === "writeMeta" ? i : max),
      -1,
    );
    const firstStaleDeleteIndex = ops.findIndex(
      (op) => op.op === "deleteMeta" && op.path === "/stale",
    );

    expect(lastWriteIndex).toBeGreaterThanOrEqual(0);
    expect(firstStaleDeleteIndex).toBeGreaterThanOrEqual(0);
    expect(lastWriteIndex).toBeLessThan(firstStaleDeleteIndex);

    FS2.unmount(MOUNT);
  });

  it("syncfs after delete only removes deleted file metadata", async () => {
    // Create two files and sync
    const { FS, tomefs } = await mountTome(backend);
    let s = FS.open(`${MOUNT}/keep`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("kept"), 0, 4);
    FS.close(s);
    s = FS.open(`${MOUNT}/remove`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("gone"), 0, 4);
    FS.close(s);
    syncfs(FS, tomefs);

    expect(backend.listFiles()).toContain("/keep");
    expect(backend.listFiles()).toContain("/remove");

    // Delete one file and sync again
    FS.unlink(`${MOUNT}/remove`);

    backend.startRecording();
    syncfs(FS, tomefs);
    backend.stopRecording();

    // /keep should still exist, /remove should be gone
    const stored = backend.listFiles();
    expect(stored).toContain("/keep");
    expect(stored).not.toContain("/remove");

    // No deleteMeta should have been recorded for /keep
    const keepDeletes = backend.operations.filter(
      (o) => o.op === "deleteMeta" && o.path === "/keep",
    );
    expect(keepDeletes.length).toBe(0);

    // Verify data integrity
    s = FS.open(`${MOUNT}/keep`, O.RDONLY);
    const buf = new Uint8Array(20);
    const n = FS.read(s, buf, 0, 20);
    FS.close(s);
    expect(decode(buf, n)).toBe("kept");

    FS.unmount(MOUNT);
  });

  it("syncfs preserves directory tree with nested files", async () => {
    const { FS, tomefs } = await mountTome(backend);
    FS.mkdir(`${MOUNT}/dir`);
    const files = ["a", "b", "dir/c", "dir/d"];
    for (const name of files) {
      const s = FS.open(`${MOUNT}/${name}`, O.RDWR | O.CREAT, 0o666);
      FS.write(s, encode(`content-${name}`), 0, `content-${name}`.length);
      FS.close(s);
    }
    syncfs(FS, tomefs);

    // Delete one file, sync again
    FS.unlink(`${MOUNT}/b`);
    syncfs(FS, tomefs);

    // b is gone, everything else remains
    const stored = backend.listFiles();
    expect(stored).not.toContain("/b");
    expect(stored).toContain("/dir");
    expect(stored).toContain("/a");
    expect(stored).toContain("/dir/c");
    expect(stored).toContain("/dir/d");

    // Data integrity check
    for (const name of ["a", "dir/c", "dir/d"]) {
      const s = FS.open(`${MOUNT}/${name}`, O.RDONLY);
      const buf = new Uint8Array(50);
      const n = FS.read(s, buf, 0, 50);
      FS.close(s);
      expect(decode(buf, n)).toBe(`content-${name}`);
    }

    FS.unmount(MOUNT);
  });

  it("handles syncfs with no stale entries (common case)", async () => {
    const { FS, tomefs } = await mountTome(backend);
    const s = FS.open(`${MOUNT}/file`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("hello"), 0, 5);
    FS.close(s);

    backend.startRecording();
    syncfs(FS, tomefs);
    backend.stopRecording();

    // Should have writes but no deletes (no stale entries)
    const writes = backend.operations.filter((o) => o.op === "writeMeta");
    const deletes = backend.operations.filter((o) => o.op === "deleteMeta");
    expect(writes.length).toBeGreaterThan(0);
    expect(deletes.length).toBe(0);

    FS.unmount(MOUNT);
  });

  it("syncfs cleans up orphaned page data for stale entries", async () => {
    // Create a file and sync so metadata + pages are in the backend
    const { FS, tomefs } = await mountTome(backend);
    const s = FS.open(`${MOUNT}/orphan`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("data that will be orphaned"), 0, 26);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // Simulate crash: remove clean-shutdown marker. Orphan data for
    // /orphan stays in backend (as if the prior session crashed before
    // cleanup completed). Remove the file's metadata from the tree by
    // re-injecting it after unmount so it appears as an orphan.
    backend.deleteMeta("/__tomefs_clean");

    // Remount — restoreTree sees orphan data, missing clean marker
    const { FS: FS2, tomefs: t2 } = await mountTome(backend);
    FS2.unlink(`${MOUNT}/orphan`);

    // Re-inject stale metadata + pages as if crash left them behind
    backend.writeMeta("/orphan", {
      size: 26,
      mode: 0o100666,
      ctime: Date.now(),
      mtime: Date.now(),
    });
    backend.writePage("/orphan", 0, new Uint8Array(8192));

    // syncfs should clean up both metadata AND page data via full tree walk
    // (needsOrphanCleanup is true from mount with missing clean marker)
    backend.startRecording();
    syncfs(FS2, t2);
    backend.stopRecording();

    expect(backend.readMeta("/orphan")).toBeNull();
    expect(backend.readPage("/orphan", 0)).toBeNull();

    // Verify deleteFile was called for the stale path
    const fileDeletes = backend.operations.filter(
      (o) => o.op === "deleteFile" && o.path === "/orphan",
    );
    expect(fileDeletes.length).toBe(1);

    FS2.unmount(MOUNT);
  });

  it("syncfs orphan cleanup deletes multi-page stale data", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create a multi-page file
    const bigData = new Uint8Array(8192 * 3); // 3 pages
    for (let i = 0; i < bigData.length; i++) bigData[i] = (i * 17) & 0xff;
    const s = FS.open(`${MOUNT}/big`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, bigData, 0, bigData.length);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // Simulate crash: remove clean marker
    backend.deleteMeta("/__tomefs_clean");

    // Remount, delete the file, then re-inject stale data
    const { FS: FS2, tomefs: t2 } = await mountTome(backend);
    FS2.unlink(`${MOUNT}/big`);
    backend.writeMeta("/big", {
      size: bigData.length,
      mode: 0o100666,
      ctime: Date.now(),
      mtime: Date.now(),
    });
    for (let i = 0; i < 3; i++) {
      backend.writePage("/big", i, new Uint8Array(8192));
    }

    // Full tree walk triggered by missing clean marker on mount
    syncfs(FS2, t2);

    // All pages and metadata should be gone
    expect(backend.readMeta("/big")).toBeNull();
    for (let i = 0; i < 3; i++) {
      expect(backend.readPage("/big", i)).toBeNull();
    }

    FS2.unmount(MOUNT);
  });

  it("syncfs orphan cleanup does not affect current files", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create two files
    let s = FS.open(`${MOUNT}/current`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("live data"), 0, 9);
    FS.close(s);
    s = FS.open(`${MOUNT}/stale`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("dead data"), 0, 9);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // Simulate crash: remove clean marker
    backend.deleteMeta("/__tomefs_clean");

    // Remount, delete the stale file, re-inject stale data
    const { FS: FS2, tomefs: t2 } = await mountTome(backend);
    FS2.unlink(`${MOUNT}/stale`);
    backend.writeMeta("/stale", {
      size: 9,
      mode: 0o100666,
      ctime: Date.now(),
      mtime: Date.now(),
    });
    backend.writePage("/stale", 0, new Uint8Array(8192));

    backend.startRecording();
    syncfs(FS2, t2);
    backend.stopRecording();

    // Stale file cleaned up
    expect(backend.readMeta("/stale")).toBeNull();
    expect(backend.readPage("/stale", 0)).toBeNull();

    // Current file untouched
    expect(backend.readMeta("/current")).not.toBeNull();
    expect(backend.readPage("/current", 0)).not.toBeNull();

    // No deleteFile call for /current
    const currentDeletes = backend.operations.filter(
      (o) => o.op === "deleteFile" && o.path === "/current",
    );
    expect(currentDeletes.length).toBe(0);

    // Data integrity
    s = FS2.open(`${MOUNT}/current`, O.RDONLY);
    const buf = new Uint8Array(20);
    const n = FS2.read(s, buf, 0, 20);
    FS2.close(s);
    expect(decode(buf, n)).toBe("live data");

    FS2.unmount(MOUNT);
  });

  it("multiple sync cycles correctly track current paths", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Cycle 1: create a, b
    for (const name of ["a", "b"]) {
      const s = FS.open(`${MOUNT}/${name}`, O.RDWR | O.CREAT, 0o666);
      FS.write(s, encode(`v1-${name}`), 0, 4);
      FS.close(s);
    }
    syncfs(FS, tomefs);
    expect(backend.listFiles()).toContain("/a");
    expect(backend.listFiles()).toContain("/b");

    // Cycle 2: delete b, create c
    FS.unlink(`${MOUNT}/b`);
    const s = FS.open(`${MOUNT}/c`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("v1-c"), 0, 4);
    FS.close(s);
    syncfs(FS, tomefs);

    const stored = backend.listFiles();
    expect(stored).toContain("/a");
    expect(stored).not.toContain("/b");
    expect(stored).toContain("/c");

    FS.unmount(MOUNT);
  });

  it("syncfs does not call readMeta during persist phase @fast", async () => {
    // Verify that syncfs uses in-memory tracking (currentPaths set) instead
    // of backend.readMeta() calls. Each readMeta is a synchronous SAB bridge
    // round-trip in production, so eliminating them improves sync performance.
    const { FS, tomefs } = await mountTome(backend);

    // Create nested directory structure
    FS.mkdir(`${MOUNT}/dir`);
    FS.mkdir(`${MOUNT}/dir/sub`);
    let s = FS.open(`${MOUNT}/dir/sub/file`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("data"), 0, 4);
    FS.close(s);

    // First sync
    syncfs(FS, tomefs);

    // Second sync — should NOT call readMeta during persist
    backend.startRecording();
    syncfs(FS, tomefs);
    backend.stopRecording();

    const readMetaCalls = backend.operations.filter(
      (o) => o.op === "readMeta",
    );
    expect(readMetaCalls.length).toBe(0);

    FS.unmount(MOUNT);
  });

  it("syncfs updates metadata on every cycle (not just first sync)", async () => {
    // Regression test: metadata must be refreshed on each syncfs call, not
    // cached from the first sync. Previously, detached node metadata was only
    // written when backend.readMeta() returned null (first sync), so
    // subsequent syncs left stale metadata in the backend.
    const { FS, tomefs } = await mountTome(backend);

    // Create a file and sync
    let s = FS.open(`${MOUNT}/growing`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("small"), 0, 5);
    FS.close(s);
    syncfs(FS, tomefs);

    const meta1 = backend.readMeta("/growing");
    expect(meta1).not.toBeNull();
    expect(meta1!.size).toBe(5);

    // Modify the file (grow it) and sync again
    s = FS.open(`${MOUNT}/growing`, O.RDWR);
    FS.write(s, encode("much larger content here"), 0, 24);
    FS.close(s);
    syncfs(FS, tomefs);

    // Metadata should reflect the updated size
    const meta2 = backend.readMeta("/growing");
    expect(meta2).not.toBeNull();
    expect(meta2!.size).toBe(24);
    // mtime should have been updated
    expect(meta2!.mtime).toBeGreaterThanOrEqual(meta1!.mtime);

    FS.unmount(MOUNT);
  });

  it("syncfs updates directory metadata timestamps across cycles", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/dir`);
    syncfs(FS, tomefs);
    const meta1 = backend.readMeta("/dir");
    expect(meta1).not.toBeNull();

    // Adding a child to the directory updates its mtime
    const s = FS.open(`${MOUNT}/dir/child`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("x"), 0, 1);
    FS.close(s);
    syncfs(FS, tomefs);

    const meta2 = backend.readMeta("/dir");
    expect(meta2).not.toBeNull();
    expect(meta2!.mtime).toBeGreaterThanOrEqual(meta1!.mtime);

    FS.unmount(MOUNT);
  });
});

describe("syncfs detached node handling", () => {
  let backend: RecordingBackend;

  beforeEach(() => {
    backend = new RecordingBackend();
  });

  it("detached file node metadata is updated on every sync cycle", async () => {
    // Simulate a "detached" node: one that is tracked by allFileNodes but
    // not reachable from mount.root's subtree. This happens in PGlite when
    // Emscripten's path resolution routes through MEMFS parent nodes.
    const { FS, tomefs } = await mountTome(backend);
    const mountNode = FS.lookupPath(MOUNT).node;

    // Create a file node directly via tomefs.createNode — this adds it to
    // allFileNodes. Then detach it from the mount tree by removing it from
    // the parent's contents and re-parenting it under a MEMFS directory.
    const fileNode = tomefs.createNode(mountNode, "detached", 0o100666, 0);

    // Write initial data and open a fd BEFORE detaching — path resolution
    // won't work after detachment since the node isn't in Emscripten's
    // nameTable under the new parent. Keep the fd for later writes.
    const fd = FS.open(`${MOUNT}/detached`, O.RDWR);
    FS.write(fd, encode("initial"), 0, 7, 0);

    // Remove from mount tree so persistTree won't find it
    delete mountNode.contents["detached"];

    // Re-parent under a MEMFS node to simulate PGlite's preloading behavior.
    // The node is still in allFileNodes but not in mount.root's subtree.
    FS.mkdir("/memfs_parent");
    const memfsParent = FS.lookupPath("/memfs_parent").node;
    fileNode.parent = memfsParent;
    fileNode.name = "detached";
    memfsParent.contents["detached"] = fileNode;

    // First sync — should persist the detached node's metadata
    syncfs(FS, tomefs);
    // The detached node may or may not be found by nodeStoragePath depending
    // on mount prefix stripping. Check the backend for any metadata with size 7.
    const allFiles = backend.listFiles();
    const detachedMeta = allFiles
      .map((f) => ({ path: f, meta: backend.readMeta(f) }))
      .find((f) => f.meta && f.meta.size === 7);
    expect(detachedMeta).toBeDefined();

    // Write more data through the open fd — this goes through stream_ops.write
    // which marks metadata dirty via markMetaDirty(). Using the FS API
    // instead of direct pageCache access ensures dirty tracking works with
    // the incremental syncfs optimization (dirtyMetaNodes set).
    FS.write(fd, encode("much longer data"), 0, 16, 0);
    FS.close(fd);

    // Second sync — metadata MUST be updated (this was the bug)
    syncfs(FS, tomefs);

    const updatedFiles = backend.listFiles();
    const updatedMeta = updatedFiles
      .map((f) => ({ path: f, meta: backend.readMeta(f) }))
      .find((f) => f.meta && f.meta.size === 16);
    expect(updatedMeta).toBeDefined();

    FS.unmount(MOUNT);
  });

  it("detached node parent directories persisted without readMeta calls", async () => {
    const { FS, tomefs } = await mountTome(backend);
    const mountNode = FS.lookupPath(MOUNT).node;

    // Create a nested directory and file outside mount tree
    FS.mkdir("/ext_parent");
    FS.mkdir("/ext_parent/sub");
    const extSub = FS.lookupPath("/ext_parent/sub").node;

    const fileNode = tomefs.createNode(extSub, "file", 0o100666, 0);
    fileNode.usedBytes = 0;
    delete mountNode.contents["file"]; // remove if accidentally added

    // First sync
    syncfs(FS, tomefs);

    // Second sync — should NOT use readMeta for parent directory checks
    backend.startRecording();
    syncfs(FS, tomefs);
    backend.stopRecording();

    // readMeta should not be called during the persist phase
    const readMetaCalls = backend.operations.filter(
      (o) => o.op === "readMeta",
    );
    expect(readMetaCalls.length).toBe(0);

    FS.unmount(MOUNT);
  });

  // ------------------------------------------------------------------
  // Orphan cleanup skip optimization
  // ------------------------------------------------------------------

  it("skips backend.listFiles() on second syncfs when no tree mutations occurred @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);
    const s = FS.open(`${MOUNT}/data`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("hello"), 0, 5);
    FS.close(s);

    // First syncfs — must call listFiles for orphan cleanup
    backend.startRecording();
    syncfs(FS, tomefs);
    backend.stopRecording();
    expect(backend.operations.some((o) => o.op === "listFiles")).toBe(true);

    // Modify the file (but no unlink/rename/rmdir)
    const s2 = FS.open(`${MOUNT}/data`, O.RDWR, 0o666);
    FS.write(s2, encode("world"), 0, 5);
    FS.close(s2);

    // Second syncfs — should skip listFiles since no tree-mutating ops
    backend.startRecording();
    syncfs(FS, tomefs);
    backend.stopRecording();
    expect(backend.operations.some((o) => o.op === "listFiles")).toBe(false);

    FS.unmount(MOUNT);
  });

  it("uses incremental path after unlink (no open fds) @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create and sync
    const s = FS.open(`${MOUNT}/a`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("a"), 0, 1);
    FS.close(s);
    syncfs(FS, tomefs);

    // Unlink with no open fds cleans up backend directly — no orphan
    // risk, so incremental path is used (no listFiles call).
    FS.unlink(`${MOUNT}/a`);

    backend.startRecording();
    syncfs(FS, tomefs);
    backend.stopRecording();
    expect(backend.operations.some((o) => o.op === "listFiles")).toBe(false);

    // Subsequent sync without mutations also skips listFiles
    backend.startRecording();
    syncfs(FS, tomefs);
    backend.stopRecording();
    expect(backend.operations.some((o) => o.op === "listFiles")).toBe(false);

    FS.unmount(MOUNT);
  });

  it("uses incremental path after rename", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const s = FS.open(`${MOUNT}/old`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("data"), 0, 4);
    FS.close(s);
    syncfs(FS, tomefs);

    // Rename completes its own backend cleanup — no orphan risk,
    // so incremental path is used (no listFiles call).
    FS.rename(`${MOUNT}/old`, `${MOUNT}/new`);

    backend.startRecording();
    syncfs(FS, tomefs);
    backend.stopRecording();
    expect(backend.operations.some((o) => o.op === "listFiles")).toBe(false);

    FS.unmount(MOUNT);
  });

  it("uses incremental path after rmdir", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/dir`);
    syncfs(FS, tomefs);

    // rmdir deletes metadata directly — no orphan risk.
    FS.rmdir(`${MOUNT}/dir`);

    backend.startRecording();
    syncfs(FS, tomefs);
    backend.stopRecording();
    expect(backend.operations.some((o) => o.op === "listFiles")).toBe(false);

    FS.unmount(MOUNT);
  });

  it("skips metadata writes for unmodified files after first sync @fast", async () => {
    // Create several files and sync (first sync writes all metadata)
    const { FS, tomefs } = await mountTome(backend);
    for (let i = 0; i < 5; i++) {
      const s = FS.open(`${MOUNT}/f${i}`, O.RDWR | O.CREAT, 0o666);
      FS.write(s, encode(`data-${i}`), 0, 6);
      FS.close(s);
    }
    syncfs(FS, tomefs);

    // Second sync without modifications — should write zero metadata entries
    backend.startRecording();
    syncfs(FS, tomefs);
    backend.stopRecording();

    const writeOps = backend.operations.filter((o) => o.op === "writeMeta");
    expect(writeOps.length).toBe(0);

    // All files still readable
    for (let i = 0; i < 5; i++) {
      const s = FS.open(`${MOUNT}/f${i}`, O.RDONLY);
      const buf = new Uint8Array(20);
      const n = FS.read(s, buf, 0, 20);
      FS.close(s);
      expect(decode(buf, n)).toBe(`data-${i}`);
    }

    FS.unmount(MOUNT);
  });

  it("only writes metadata for modified files on subsequent sync @fast", async () => {
    // Create files and sync
    const { FS, tomefs } = await mountTome(backend);
    for (let i = 0; i < 5; i++) {
      const s = FS.open(`${MOUNT}/f${i}`, O.RDWR | O.CREAT, 0o666);
      FS.write(s, encode(`data-${i}`), 0, 6);
      FS.close(s);
    }
    syncfs(FS, tomefs);

    // Modify only one file
    const s = FS.open(`${MOUNT}/f2`, O.WRONLY);
    FS.write(s, encode("CHANGED"), 0, 7);
    FS.close(s);

    backend.startRecording();
    syncfs(FS, tomefs);
    backend.stopRecording();

    // Only the modified file and its parent dir should have metadata written
    const writeOps = backend.operations.filter((o) => o.op === "writeMeta");
    const writtenPaths = writeOps.map((o) => o.path);
    expect(writtenPaths).toContain("/f2");
    // Unmodified files should NOT be written
    expect(writtenPaths).not.toContain("/f0");
    expect(writtenPaths).not.toContain("/f1");
    expect(writtenPaths).not.toContain("/f3");
    expect(writtenPaths).not.toContain("/f4");

    // All files still readable with correct content
    const s2 = FS.open(`${MOUNT}/f2`, O.RDONLY);
    const buf = new Uint8Array(20);
    const n = FS.read(s2, buf, 0, 20);
    FS.close(s2);
    expect(decode(buf, n)).toBe("CHANGED");

    FS.unmount(MOUNT);
  });

  it("metadata persists correctly across remount after dirty-skip sync", async () => {
    // Create files and do initial sync
    const { FS, tomefs } = await mountTome(backend);
    for (let i = 0; i < 3; i++) {
      const s = FS.open(`${MOUNT}/f${i}`, O.RDWR | O.CREAT, 0o666);
      FS.write(s, encode(`v1-${i}`), 0, 4);
      FS.close(s);
    }
    syncAndUnmount(FS, tomefs);

    // Remount, modify one file, sync (dirty-skip should preserve others)
    const { FS: FS2, tomefs: t2 } = await mountTome(backend);
    const s = FS2.open(`${MOUNT}/f1`, O.WRONLY | O.TRUNC);
    FS2.write(s, encode("v2"), 0, 2);
    FS2.close(s);
    syncAndUnmount(FS2, t2);

    // Remount and verify all files
    const { FS: FS3 } = await mountTome(backend);
    for (let i = 0; i < 3; i++) {
      const s = FS3.open(`${MOUNT}/f${i}`, O.RDONLY);
      const buf = new Uint8Array(20);
      const n = FS3.read(s, buf, 0, 20);
      FS3.close(s);
      if (i === 1) {
        expect(decode(buf, n)).toBe("v2");
      } else {
        expect(decode(buf, n)).toBe(`v1-${i}`);
      }
    }
    FS3.unmount(MOUNT);
  });

  it("clean-shutdown marker skips first-syncfs tree walk @fast", async () => {
    // Phase 1: create files, sync (writes marker), unmount
    const { FS, tomefs } = await mountTome(backend);
    const s = FS.open(`${MOUNT}/data`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("hello"), 0, 5);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // Marker should be in the backend
    expect(backend.readMeta("/__tomefs_clean")).not.toBeNull();

    // Phase 2: remount cleanly — marker consumed
    const { FS: FS2, tomefs: t2 } = await mountTome(backend);

    // Marker was consumed (deleted from backend during mount)
    expect(backend.readMeta("/__tomefs_clean")).toBeNull();

    // First syncfs should be a no-op (no tree walk, no listFiles) because
    // the marker told us the backend is clean and no mutations occurred.
    backend.startRecording();
    syncfs(FS2, t2);
    backend.stopRecording();

    // Should NOT call listFiles (orphan cleanup) — that's the full tree walk
    const listOps = backend.operations.filter((o) => o.op === "listFiles");
    expect(listOps.length).toBe(0);

    // Marker re-written for next mount
    expect(backend.readMeta("/__tomefs_clean")).not.toBeNull();

    FS2.unmount(MOUNT);
  });

  it("clean-shutdown marker ignored when /__deleted_* orphans exist @fast", async () => {
    // Phase 1: create and sync
    const { FS, tomefs } = await mountTome(backend);
    const s = FS.open(`${MOUNT}/file`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("data"), 0, 4);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // Simulate crash: inject orphaned /__deleted_* entry
    backend.writeMeta("/__deleted_42", {
      size: 100,
      mode: 0o100666,
      ctime: Date.now(),
      mtime: Date.now(),
    });

    // Marker is still present from Phase 1
    expect(backend.readMeta("/__tomefs_clean")).not.toBeNull();

    // Phase 2: remount — marker should be ignored due to orphans
    const { FS: FS2, tomefs: t2 } = await mountTome(backend);

    // Trigger syncfs — should do full tree walk + orphan cleanup
    syncfs(FS2, t2);

    // Orphan should be cleaned up
    expect(backend.readMeta("/__deleted_42")).toBeNull();

    FS2.unmount(MOUNT);
  });

  it("orphan cleanup uses atomic deleteAll after data persist @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const s = FS.open(`${MOUNT}/live`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("keep"), 0, 4);
    FS.close(s);
    const s2 = FS.open(`${MOUNT}/orphan1`, O.RDWR | O.CREAT, 0o666);
    FS.write(s2, encode("gone"), 0, 4);
    FS.close(s2);
    const s3 = FS.open(`${MOUNT}/orphan2`, O.RDWR | O.CREAT, 0o666);
    FS.write(s3, encode("gone"), 0, 4);
    FS.close(s3);
    syncAndUnmount(FS, tomefs);

    // Simulate crash: remove clean marker
    backend.deleteMeta("/__tomefs_clean");

    // Remount — restoreTree restores all files. Then unlink orphans
    // from the tree so they exist in the backend but not in currentPaths.
    const { FS: FS2, tomefs: t2 } = await mountTome(backend);
    FS2.unlink(`${MOUNT}/orphan1`);
    FS2.unlink(`${MOUNT}/orphan2`);

    // Re-inject stale data as if crash left orphans behind
    const orphanMeta = { size: 4, mode: 0o100666, ctime: 0, mtime: 0 };
    backend.writeMeta("/orphan1", orphanMeta);
    backend.writePage("/orphan1", 0, new Uint8Array(8192));
    backend.writeMeta("/orphan2", orphanMeta);
    backend.writePage("/orphan2", 0, new Uint8Array(8192));

    backend.startRecording();
    syncfs(FS2, t2);
    backend.stopRecording();

    // deleteAll must be used (not individual deleteFile + deleteMeta)
    const deleteAllOps = backend.operations.filter((o) => o.op === "deleteAll");
    expect(deleteAllOps.length).toBeGreaterThanOrEqual(2);
    const deletedPaths = deleteAllOps.map((o) => o.path).sort();
    expect(deletedPaths).toContain("/orphan1");
    expect(deletedPaths).toContain("/orphan2");

    // deleteAll must come after writeMeta (data persisted first)
    const lastWriteIdx = backend.operations.reduce(
      (max, op, i) => (op.op === "writeMeta" ? i : max),
      -1,
    );
    const firstDeleteAllIdx = backend.operations.findIndex(
      (o) => o.op === "deleteAll",
    );
    if (lastWriteIdx >= 0 && firstDeleteAllIdx >= 0) {
      expect(lastWriteIdx).toBeLessThan(firstDeleteAllIdx);
    }

    // Orphans are fully cleaned
    expect(backend.readMeta("/orphan1")).toBeNull();
    expect(backend.readPage("/orphan1", 0)).toBeNull();
    expect(backend.readMeta("/orphan2")).toBeNull();
    expect(backend.readPage("/orphan2", 0)).toBeNull();

    // Live file survives
    const r = FS2.open(`${MOUNT}/live`, O.RDONLY);
    const buf = new Uint8Array(20);
    const n = FS2.read(r, buf, 0, 20);
    FS2.close(r);
    expect(decode(buf, n)).toBe("keep");

    FS2.unmount(MOUNT);
  });
});
