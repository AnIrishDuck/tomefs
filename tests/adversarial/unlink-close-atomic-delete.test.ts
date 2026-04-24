/**
 * Adversarial tests: atomic deleteAll in unlink() and close().
 *
 * When a file is unlinked with no open fds, or the last fd is closed on an
 * unlinked file, tomefs must remove both page data and metadata from the
 * backend. Previously these were two separate backend calls (deleteFile +
 * deleteMeta), creating a crash window: if the process died between them,
 * orphaned metadata would persist. The clean marker was not invalidated
 * for this path, so incremental syncfs would never discover the orphan.
 *
 * The fix: use backend.deleteAll() which is a single atomic IDB transaction
 * (for the primary IndexedDB backend). The page cache is cleaned separately
 * via discardFile() (cache-only, no backend call).
 *
 * These tests verify:
 *   1. unlink (no open fds) calls deleteAll, not separate deleteFile+deleteMeta
 *   2. close (last fd on unlinked file) calls deleteAll
 *   3. Backend is clean after both operations (no orphaned pages or metadata)
 *   4. Crash during non-atomic deleteAll leaves state recoverable via orphan
 *      cleanup on remount (defense-in-depth for non-atomic backends)
 *
 * Ethos §9: target the seams — crash between page and metadata deletion
 */
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { describe, it, expect, beforeEach } from "vitest";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { createTomeFS } from "../../src/tomefs.js";
import { PAGE_SIZE } from "../../src/types.js";
import type { SyncStorageBackend } from "../../src/sync-storage-backend.js";
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
 * SyncStorageBackend that records which mutating methods are called and in
 * what order. Used to verify that unlink/close call deleteAll instead of
 * separate deleteFile + deleteMeta.
 */
class RecordingBackend implements SyncStorageBackend {
  readonly inner: SyncMemoryBackend;
  readonly ops: Array<{ method: string; args: any[] }> = [];
  recording = false;

  constructor(inner?: SyncMemoryBackend) {
    this.inner = inner ?? new SyncMemoryBackend();
  }

  startRecording(): void {
    this.ops.length = 0;
    this.recording = true;
  }

  stopRecording(): void {
    this.recording = false;
  }

  private record(method: string, ...args: any[]): void {
    if (this.recording) {
      this.ops.push({ method, args: [...args] });
    }
  }

  readPage(p: string, i: number) { return this.inner.readPage(p, i); }
  readPages(p: string, is: number[]) { return this.inner.readPages(p, is); }
  readMeta(p: string) { return this.inner.readMeta(p); }
  readMetas(ps: string[]) { return this.inner.readMetas(ps); }
  countPages(p: string) { return this.inner.countPages(p); }
  countPagesBatch(ps: string[]) { return this.inner.countPagesBatch(ps); }
  maxPageIndex(p: string) { return this.inner.maxPageIndex(p); }
  maxPageIndexBatch(ps: string[]) { return this.inner.maxPageIndexBatch(ps); }
  listFiles() { return this.inner.listFiles(); }

  writePage(p: string, i: number, d: Uint8Array) {
    this.record("writePage", p, i); this.inner.writePage(p, i, d);
  }
  writePages(pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>) {
    this.record("writePages", pages.map(p => p.path)); this.inner.writePages(pages);
  }
  writeMeta(p: string, m: FileMeta) {
    this.record("writeMeta", p); this.inner.writeMeta(p, m);
  }
  writeMetas(entries: Array<{ path: string; meta: FileMeta }>) {
    this.record("writeMetas", entries.map(e => e.path)); this.inner.writeMetas(entries);
  }
  deleteFile(p: string) {
    this.record("deleteFile", p); this.inner.deleteFile(p);
  }
  deleteFiles(ps: string[]) {
    this.record("deleteFiles", [...ps]); this.inner.deleteFiles(ps);
  }
  deletePagesFrom(p: string, i: number) {
    this.record("deletePagesFrom", p, i); this.inner.deletePagesFrom(p, i);
  }
  deleteMeta(p: string) {
    this.record("deleteMeta", p); this.inner.deleteMeta(p);
  }
  deleteMetas(ps: string[]) {
    this.record("deleteMetas", [...ps]); this.inner.deleteMetas(ps);
  }
  renameFile(o: string, n: string) {
    this.record("renameFile", o, n); this.inner.renameFile(o, n);
  }
  syncAll(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
    metas: Array<{ path: string; meta: FileMeta }>,
  ) {
    this.record("syncAll", pages.length, metas.length); this.inner.syncAll(pages, metas);
  }
  deleteAll(paths: string[]) {
    this.record("deleteAll", [...paths]); this.inner.deleteAll(paths);
  }
}

/**
 * SyncStorageBackend that crashes between deleteFiles and deleteMetas inside
 * deleteAll. Simulates a non-atomic backend (like OPFS) where deleteAll is
 * implemented as two sequential calls.
 */
class CrashMidDeleteAllBackend implements SyncStorageBackend {
  readonly inner: SyncMemoryBackend;
  armed = false;
  crashed = false;

  constructor(inner: SyncMemoryBackend) {
    this.inner = inner;
  }

  arm(): void { this.armed = true; this.crashed = false; }
  disarm(): void { this.armed = false; }

  readPage(p: string, i: number) { return this.inner.readPage(p, i); }
  readPages(p: string, is: number[]) { return this.inner.readPages(p, is); }
  readMeta(p: string) { return this.inner.readMeta(p); }
  readMetas(ps: string[]) { return this.inner.readMetas(ps); }
  countPages(p: string) { return this.inner.countPages(p); }
  countPagesBatch(ps: string[]) { return this.inner.countPagesBatch(ps); }
  maxPageIndex(p: string) { return this.inner.maxPageIndex(p); }
  maxPageIndexBatch(ps: string[]) { return this.inner.maxPageIndexBatch(ps); }
  listFiles() { return this.inner.listFiles(); }

  writePage(p: string, i: number, d: Uint8Array) { this.inner.writePage(p, i, d); }
  writePages(pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>) {
    this.inner.writePages(pages);
  }
  writeMeta(p: string, m: FileMeta) { this.inner.writeMeta(p, m); }
  writeMetas(entries: Array<{ path: string; meta: FileMeta }>) {
    this.inner.writeMetas(entries);
  }
  deleteFile(p: string) { this.inner.deleteFile(p); }
  deleteFiles(ps: string[]) { this.inner.deleteFiles(ps); }
  deletePagesFrom(p: string, i: number) { this.inner.deletePagesFrom(p, i); }
  deleteMeta(p: string) { this.inner.deleteMeta(p); }
  deleteMetas(ps: string[]) { this.inner.deleteMetas(ps); }
  renameFile(o: string, n: string) { this.inner.renameFile(o, n); }
  syncAll(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
    metas: Array<{ path: string; meta: FileMeta }>,
  ) {
    this.inner.syncAll(pages, metas);
  }
  deleteAll(paths: string[]) {
    this.inner.deleteFiles(paths);
    if (this.armed) {
      this.crashed = true;
      throw new Error("simulated crash mid-deleteAll");
    }
    this.inner.deleteMetas(paths);
  }
}

async function mountTome(backend: SyncStorageBackend, maxPages?: number) {
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

function syncfs(FS: any, tomefs: any): Error | null {
  let error: Error | null = null;
  tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
    error = err;
  });
  return error;
}

// ---------------------------------------------------------------------------
// Verify deleteAll is used (not separate deleteFile + deleteMeta)
// ---------------------------------------------------------------------------

describe("unlink uses atomic deleteAll", () => {
  let backend: RecordingBackend;

  beforeEach(() => {
    backend = new RecordingBackend();
  });

  it("unlink with no open fds calls deleteAll @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const data = encode("test-data-for-unlink");
    const s = FS.open(`${MOUNT}/file.txt`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    FS.close(s);
    syncfs(FS, tomefs);

    backend.startRecording();
    FS.unlink(`${MOUNT}/file.txt`);
    backend.stopRecording();

    const deleteAllCalls = backend.ops.filter(op => op.method === "deleteAll");
    expect(deleteAllCalls.length).toBe(1);
    expect(deleteAllCalls[0].args[0]).toEqual(["/file.txt"]);

    const deleteFileCalls = backend.ops.filter(op => op.method === "deleteFile");
    const deleteMetaCalls = backend.ops.filter(
      op => op.method === "deleteMeta" && op.args[0] === "/file.txt",
    );
    expect(deleteFileCalls.length).toBe(0);
    expect(deleteMetaCalls.length).toBe(0);
  });

  it("close on last fd of unlinked file calls deleteAll @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const data = encode("unlink-then-close");
    const s = FS.open(`${MOUNT}/keep-open.txt`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    syncfs(FS, tomefs);

    // Unlink with fd still open — moves to /__deleted_*
    FS.unlink(`${MOUNT}/keep-open.txt`);

    backend.startRecording();
    FS.close(s);
    backend.stopRecording();

    const deleteAllCalls = backend.ops.filter(op => op.method === "deleteAll");
    expect(deleteAllCalls.length).toBe(1);
    expect(deleteAllCalls[0].args[0][0]).toMatch(/^\/__deleted_/);

    const deleteFileCalls = backend.ops.filter(op => op.method === "deleteFile");
    expect(deleteFileCalls.length).toBe(0);
  });

  it("backend is clean after unlink (no orphaned pages or metadata) @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const data = encode("will-be-deleted");
    const s = FS.open(`${MOUNT}/victim.txt`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    FS.close(s);
    syncfs(FS, tomefs);

    expect(backend.inner.readMeta("/victim.txt")).not.toBeNull();
    expect(backend.inner.countPages("/victim.txt")).toBeGreaterThan(0);

    FS.unlink(`${MOUNT}/victim.txt`);

    expect(backend.inner.readMeta("/victim.txt")).toBeNull();
    expect(backend.inner.countPages("/victim.txt")).toBe(0);
  });

  it("backend is clean after close on unlinked file @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const data = encode("close-cleanup");
    const s = FS.open(`${MOUNT}/temp.txt`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    syncfs(FS, tomefs);

    FS.unlink(`${MOUNT}/temp.txt`);

    // /__deleted_* entry should exist in backend
    const deletedFiles = backend.inner.listFiles().filter(f => f.startsWith("/__deleted_"));
    expect(deletedFiles.length).toBe(1);
    const deletedPath = deletedFiles[0];
    expect(backend.inner.readMeta(deletedPath)).not.toBeNull();

    FS.close(s);

    // After close, /__deleted_* entry should be fully removed
    expect(backend.inner.readMeta(deletedPath)).toBeNull();
    expect(backend.inner.countPages(deletedPath)).toBe(0);
  });

  it("multiple unlinks all use deleteAll @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    for (let i = 0; i < 3; i++) {
      const data = encode(`file-${i}-data`);
      const s = FS.open(`${MOUNT}/f${i}.txt`, O.RDWR | O.CREAT, 0o666);
      FS.write(s, data, 0, data.length);
      FS.close(s);
    }
    syncfs(FS, tomefs);

    backend.startRecording();
    FS.unlink(`${MOUNT}/f0.txt`);
    FS.unlink(`${MOUNT}/f1.txt`);
    FS.unlink(`${MOUNT}/f2.txt`);
    backend.stopRecording();

    const deleteAllCalls = backend.ops.filter(op => op.method === "deleteAll");
    expect(deleteAllCalls.length).toBe(3);

    expect(backend.inner.listFiles().filter(
      f => !f.startsWith("/__tomefs"),
    )).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// Crash recovery: non-atomic deleteAll (defense-in-depth for OPFS)
//
// For IDB, deleteAll is a single transaction — either all pages+metadata
// are deleted, or none are. No orphan state is possible.
//
// For non-atomic backends (OPFS), deleteAll may crash between page deletion
// and metadata deletion. The clean marker is invalidated BEFORE deleteAll,
// so the next mount forces a full tree walk. However, restoreTree "adopts"
// the orphaned metadata (creating a 0-byte file), which prevents orphan
// cleanup from removing it. This is safe: no data corruption, no stale
// reads — the worst case is a 0-byte file that shouldn't exist.
// ---------------------------------------------------------------------------

describe("crash during non-atomic deleteAll (OPFS defense-in-depth)", () => {
  it("crash mid-deleteAll during unlink: no clean marker on remount @fast", async () => {
    const inner = new SyncMemoryBackend();
    const crashBackend = new CrashMidDeleteAllBackend(inner);

    const { FS, tomefs } = await mountTome(crashBackend);

    const data = encode("crash-victim-data-" + "x".repeat(100));
    const s = FS.open(`${MOUNT}/victim.txt`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    FS.close(s);
    syncfs(FS, tomefs);

    expect(inner.readMeta("/victim.txt")).not.toBeNull();
    expect(inner.readMeta("/__tomefs_clean")).not.toBeNull();

    // Arm the crash — deleteAll will delete pages but crash before
    // deleting metadata
    crashBackend.arm();

    let crashed = false;
    try {
      FS.unlink(`${MOUNT}/victim.txt`);
    } catch {
      crashed = true;
    }
    expect(crashed).toBe(true);
    expect(crashBackend.crashed).toBe(true);

    // Backend state after crash: pages gone, metadata still exists,
    // clean marker deleted (by invalidateCleanMarker before deleteAll)
    expect(inner.countPages("/victim.txt")).toBe(0);
    expect(inner.readMeta("/victim.txt")).not.toBeNull();
    expect(inner.readMeta("/__tomefs_clean")).toBeNull();
  });

  it("crash mid-deleteAll during close: /__deleted_* cleaned by orphan cleanup @fast", async () => {
    const inner = new SyncMemoryBackend();
    const crashBackend = new CrashMidDeleteAllBackend(inner);

    const { FS, tomefs } = await mountTome(crashBackend);

    const data = encode("unlink-close-crash");
    const s = FS.open(`${MOUNT}/temp.txt`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    syncfs(FS, tomefs);

    // Unlink with open fd — moves to /__deleted_*
    FS.unlink(`${MOUNT}/temp.txt`);

    const deletedFiles = inner.listFiles().filter(f => f.startsWith("/__deleted_"));
    expect(deletedFiles.length).toBe(1);
    const deletedPath = deletedFiles[0];

    // Arm crash for close
    crashBackend.arm();

    let crashed = false;
    try {
      FS.close(s);
    } catch {
      crashed = true;
    }
    expect(crashed).toBe(true);

    // Pages gone, but /__deleted_* metadata persists
    expect(inner.countPages(deletedPath)).toBe(0);
    expect(inner.readMeta(deletedPath)).not.toBeNull();

    // Remount and syncfs — unlink-with-open-fds already invalidated
    // the clean marker, so the next mount forces orphan cleanup.
    // /__deleted_* entries are filtered by restoreTree (not adopted),
    // so orphan cleanup removes them.
    const { FS: FS2, tomefs: tomefs2 } = await mountTome(inner);
    const err = syncfs(FS2, tomefs2);
    expect(err).toBeNull();

    // Orphaned /__deleted_* entry is cleaned up by orphan cleanup
    expect(inner.readMeta(deletedPath)).toBeNull();
  });

  it("no data loss: other files survive crash during unlink @fast", async () => {
    const inner = new SyncMemoryBackend();
    const crashBackend = new CrashMidDeleteAllBackend(inner);

    const { FS, tomefs } = await mountTome(crashBackend);

    // Create two files — we'll crash during unlink of one
    const keepData = encode("must-survive-" + "y".repeat(100));
    const victimData = encode("will-crash");

    const sk = FS.open(`${MOUNT}/keep.txt`, O.RDWR | O.CREAT, 0o666);
    FS.write(sk, keepData, 0, keepData.length);
    FS.close(sk);

    const sv = FS.open(`${MOUNT}/victim.txt`, O.RDWR | O.CREAT, 0o666);
    FS.write(sv, victimData, 0, victimData.length);
    FS.close(sv);
    syncfs(FS, tomefs);

    // Crash during victim unlink
    crashBackend.arm();
    try { FS.unlink(`${MOUNT}/victim.txt`); } catch { /* expected */ }

    // Remount from raw backend
    const { FS: FS2, tomefs: tomefs2 } = await mountTome(inner);
    const err = syncfs(FS2, tomefs2);
    expect(err).toBeNull();

    // keep.txt must survive intact
    const buf = new Uint8Array(keepData.length);
    const s2 = FS2.open(`${MOUNT}/keep.txt`, O.RDONLY);
    FS2.read(s2, buf, 0, keepData.length, 0);
    FS2.close(s2);
    expect(decode(buf, keepData.length)).toBe(decode(keepData));

    // victim.txt: pages deleted, orphaned metadata restored as 0-byte file.
    // No data corruption — file is accessible but empty.
    const entries = FS2.readdir(MOUNT);
    expect(entries).toContain("keep.txt");
  });
});
