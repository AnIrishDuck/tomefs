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
    op: "writeMeta" | "deleteMeta" | "deleteFile";
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

  deleteMeta(path: string): void {
    if (this.recording) {
      this.operations.push({ op: "deleteMeta", path });
    }
    super.deleteMeta(path);
  }

  deleteFile(path: string): void {
    if (this.recording) {
      this.operations.push({ op: "deleteFile", path });
    }
    super.deleteFile(path);
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

    // Remount, delete one file, then record operations during syncfs
    const { FS: FS2, tomefs: t2 } = await mountTome(backend);
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
    syncfs(FS, tomefs);

    // Verify pages exist in backend
    expect(backend.readPage("/orphan", 0)).not.toBeNull();

    // Simulate a crash scenario: unlink the file (cleans pages from cache +
    // backend), then manually re-add stale metadata AND stale page data as
    // if the crash interrupted the cleanup.
    FS.unlink(`${MOUNT}/orphan`);
    backend.writeMeta("/orphan", {
      size: 26,
      mode: 0o100666,
      ctime: Date.now(),
      mtime: Date.now(),
    });
    backend.writePage("/orphan", 0, new Uint8Array(8192));

    // syncfs should clean up both metadata AND page data
    backend.startRecording();
    syncfs(FS, tomefs);
    backend.stopRecording();

    expect(backend.readMeta("/orphan")).toBeNull();
    expect(backend.readPage("/orphan", 0)).toBeNull();

    // Verify deleteFile was called for the stale path
    const fileDeletes = backend.operations.filter(
      (o) => o.op === "deleteFile" && o.path === "/orphan",
    );
    expect(fileDeletes.length).toBe(1);

    FS.unmount(MOUNT);
  });

  it("syncfs orphan cleanup deletes multi-page stale data", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create a multi-page file
    const bigData = new Uint8Array(8192 * 3); // 3 pages
    for (let i = 0; i < bigData.length; i++) bigData[i] = (i * 17) & 0xff;
    const s = FS.open(`${MOUNT}/big`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, bigData, 0, bigData.length);
    FS.close(s);
    syncfs(FS, tomefs);

    // Verify all 3 pages exist
    expect(backend.readPage("/big", 0)).not.toBeNull();
    expect(backend.readPage("/big", 1)).not.toBeNull();
    expect(backend.readPage("/big", 2)).not.toBeNull();

    // Delete the file, then simulate crash by re-adding stale data
    FS.unlink(`${MOUNT}/big`);
    backend.writeMeta("/big", {
      size: bigData.length,
      mode: 0o100666,
      ctime: Date.now(),
      mtime: Date.now(),
    });
    for (let i = 0; i < 3; i++) {
      backend.writePage("/big", i, new Uint8Array(8192));
    }

    syncfs(FS, tomefs);

    // All pages and metadata should be gone
    expect(backend.readMeta("/big")).toBeNull();
    for (let i = 0; i < 3; i++) {
      expect(backend.readPage("/big", i)).toBeNull();
    }

    FS.unmount(MOUNT);
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
    syncfs(FS, tomefs);

    // Delete the stale file, simulate crash with leftover data
    FS.unlink(`${MOUNT}/stale`);
    backend.writeMeta("/stale", {
      size: 9,
      mode: 0o100666,
      ctime: Date.now(),
      mtime: Date.now(),
    });
    backend.writePage("/stale", 0, new Uint8Array(8192));

    backend.startRecording();
    syncfs(FS, tomefs);
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
    s = FS.open(`${MOUNT}/current`, O.RDONLY);
    const buf = new Uint8Array(20);
    const n = FS.read(s, buf, 0, 20);
    FS.close(s);
    expect(decode(buf, n)).toBe("live data");

    FS.unmount(MOUNT);
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
});
