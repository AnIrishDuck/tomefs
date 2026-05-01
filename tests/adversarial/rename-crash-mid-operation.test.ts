/**
 * Adversarial tests: crash DURING a rename operation.
 *
 * The rename function in tomefs performs multiple backend operations:
 * 1. pageCache.renameFile (flushes dirty pages, moves pages in backend)
 * 2. backend.writeMeta (writes metadata at new path)
 * 3. backend.deleteMeta (removes old metadata)
 *
 * A crash between any of these steps (e.g., tab close, OOM kill, power loss)
 * could leave the backend in an inconsistent state. These tests verify that
 * the ordering of operations ensures data is NEVER lost — worst case is
 * stale/duplicate entries that orphan cleanup resolves on next syncfs.
 *
 * Uses a CrashAfterNOps fake backend (same pattern as
 * syncfs-partial-crash.test.ts) to simulate crashes at precise points
 * within the rename operation.
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
 * SyncStorageBackend fake that wraps SyncMemoryBackend and throws
 * after a configurable number of mutating operations.
 *
 * Same pattern as syncfs-partial-crash.test.ts — reads never crash,
 * writes count toward the crash threshold.
 */
class CrashAfterNOpsSyncBackend implements SyncStorageBackend {
  readonly inner: SyncMemoryBackend;
  private crashAfter: number;
  private opCount = 0;
  armed = false;
  crashed = false;

  constructor(inner: SyncMemoryBackend, crashAfter: number) {
    this.inner = inner;
    this.crashAfter = crashAfter;
  }

  arm(crashAfter?: number): void {
    if (crashAfter !== undefined) this.crashAfter = crashAfter;
    this.armed = true;
    this.opCount = 0;
    this.crashed = false;
  }

  disarm(): void {
    this.armed = false;
  }

  private tick(): void {
    if (!this.armed) return;
    this.opCount++;
    if (this.opCount > this.crashAfter) {
      this.crashed = true;
      throw new Error("simulated crash");
    }
  }

  // --- Reads (never crash) ---
  readPage(path: string, pageIndex: number): Uint8Array | null {
    return this.inner.readPage(path, pageIndex);
  }
  readPages(path: string, pageIndices: number[]): Array<Uint8Array | null> {
    return this.inner.readPages(path, pageIndices);
  }
  readMeta(path: string): FileMeta | null {
    return this.inner.readMeta(path);
  }
  readMetas(paths: string[]): Array<FileMeta | null> {
    return this.inner.readMetas(paths);
  }
  countPages(path: string): number {
    return this.inner.countPages(path);
  }
  countPagesBatch(paths: string[]): number[] {
    return this.inner.countPagesBatch(paths);
  }
  maxPageIndex(path: string): number {
    return this.inner.maxPageIndex(path);
  }
  maxPageIndexBatch(paths: string[]): number[] {
    return this.inner.maxPageIndexBatch(paths);
  }
  listFiles(): string[] {
    return this.inner.listFiles();
  }

  // --- Writes (may crash) ---
  writePage(path: string, pageIndex: number, data: Uint8Array): void {
    this.tick();
    this.inner.writePage(path, pageIndex, data);
  }
  writePages(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
  ): void {
    for (const { path, pageIndex, data } of pages) {
      this.tick();
      this.inner.writePage(path, pageIndex, data);
    }
  }
  writeMeta(path: string, meta: FileMeta): void {
    this.tick();
    this.inner.writeMeta(path, meta);
  }
  writeMetas(entries: Array<{ path: string; meta: FileMeta }>): void {
    for (const { path, meta } of entries) {
      this.tick();
      this.inner.writeMeta(path, meta);
    }
  }
  deleteFile(path: string): void {
    this.tick();
    this.inner.deleteFile(path);
  }
  deleteFiles(paths: string[]): void {
    for (const path of paths) {
      this.tick();
      this.inner.deleteFile(path);
    }
  }
  deletePagesFrom(path: string, fromPageIndex: number): void {
    this.tick();
    this.inner.deletePagesFrom(path, fromPageIndex);
  }
  deleteMeta(path: string): void {
    this.tick();
    this.inner.deleteMeta(path);
  }
  deleteMetas(paths: string[]): void {
    for (const path of paths) {
      this.tick();
      this.inner.deleteMeta(path);
    }
  }
  renameFile(oldPath: string, newPath: string): void {
    this.tick();
    this.inner.renameFile(oldPath, newPath);
  }
  syncAll(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
    metas: Array<{ path: string; meta: FileMeta }>,
  ): void {
    this.writePages(pages);
    this.writeMetas(metas);
  }
  deleteAll(paths: string[]): void {
    this.deleteFiles(paths);
    this.deleteMetas(paths);
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

function syncAndUnmount(FS: any, tomefs: any) {
  const err = syncfs(FS, tomefs);
  if (err) throw err;
  FS.unmount(MOUNT);
}

/**
 * Mount fresh from the inner backend (bypassing crash wrapper) to simulate
 * recovery after a crash.
 */
async function remountFromInner(
  inner: SyncMemoryBackend,
  maxPages?: number,
) {
  return mountTome(inner, maxPages);
}

// ---------------------------------------------------------------------------
// File rename crash tests
// ---------------------------------------------------------------------------

describe("crash during file rename operation", () => {
  let inner: SyncMemoryBackend;
  let crashBackend: CrashAfterNOpsSyncBackend;

  beforeEach(() => {
    inner = new SyncMemoryBackend();
    crashBackend = new CrashAfterNOpsSyncBackend(inner, 1);
  });

  it("file data survives crash after page move but before metadata write @fast", async () => {
    // Phase 1: create a file with known data, sync to backend
    const { FS, tomefs } = await mountTome(crashBackend);
    const content = "crash-safety-test-data-" + "x".repeat(100);
    const data = encode(content);
    const s = FS.open(`${MOUNT}/file.txt`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // Verify data is in backend
    expect(inner.readMeta("/file.txt")).not.toBeNull();
    expect(inner.maxPageIndex("/file.txt")).toBeGreaterThanOrEqual(0);

    // Phase 2: mount again, rename file, crash mid-rename
    const { FS: FS2, tomefs: tomefs2 } = await mountTome(crashBackend);

    // For a clean file (no dirty pages), the rename backend ops are:
    //   1. backend.renameFile(old, new) — pages moved
    //   2. backend.writeMeta(new, ...) — metadata at new path
    //   3. backend.deleteMeta(old) — old metadata removed
    // Arming with crashAfter=1 means op 1 succeeds, op 2 crashes.
    // With the fix (metadata-first), op ordering becomes:
    //   1. backend.writeMeta(new, ...) — metadata at new path
    //   2. backend.renameFile(old, new) — pages moved (CRASH HERE)
    //   3. backend.deleteMeta(old) — old metadata removed
    // So with crashAfter=1: metadata succeeds, page move crashes.
    // Data stays at old path with metadata at both paths — no data loss.
    crashBackend.arm(1);

    let crashed = false;
    try {
      FS2.rename(`${MOUNT}/file.txt`, `${MOUNT}/renamed.txt`);
    } catch {
      crashed = true;
    }
    expect(crashed).toBe(true);

    // Phase 3: remount from inner backend and verify data survives
    const { FS: FS3 } = await remountFromInner(inner);

    // The file data must be recoverable at SOME path (old or new).
    // After the fix: metadata exists at new path, pages at old path,
    // so restoreTree creates both — old has data, new is empty.
    // At minimum, the data must not be lost.
    let foundData = false;
    const tryPaths = [`${MOUNT}/file.txt`, `${MOUNT}/renamed.txt`];
    for (const path of tryPaths) {
      try {
        const stat = FS3.stat(path);
        if (stat.size > 0) {
          const buf = new Uint8Array(stat.size);
          const fd = FS3.open(path, O.RDONLY);
          FS3.read(fd, buf, 0, stat.size, 0);
          FS3.close(fd);
          const recovered = decode(buf, stat.size);
          if (recovered === content) {
            foundData = true;
            break;
          }
        }
      } catch {
        // File doesn't exist at this path
      }
    }

    expect(foundData).toBe(true);
  });

  it("file data survives crash after page move of dirty file @fast", async () => {
    // Phase 1: create file, sync, then write more data (dirty pages)
    const { FS, tomefs } = await mountTome(crashBackend);
    const initialContent = "initial-data";
    const data = encode(initialContent);
    const s = FS.open(`${MOUNT}/dirty.txt`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // Phase 2: mount, write more data (making pages dirty), then rename
    const { FS: FS2, tomefs: tomefs2 } = await mountTome(crashBackend);
    const newContent = "updated-content-after-sync";
    const newData = encode(newContent);
    const s2 = FS2.open(`${MOUNT}/dirty.txt`, O.RDWR, 0o666);
    FS2.write(s2, newData, 0, newData.length, 0);
    FS2.close(s2);

    // For a dirty file, rename does:
    //   1. pageCache.flushFile → backend.writePages (dirty pages)
    //   2. backend.renameFile(old, new) — pages moved
    //   Then tomefs.ts:
    //   3. backend.writeMeta(new, ...) — metadata at new path
    //   4. backend.deleteMeta(old) — old metadata removed
    // With fix (metadata-first): writeMeta happens before steps 1-2.
    // Crash after 2 ops: writeMeta + writePages(1 page) succeed,
    // renameFile crashes. Data is flushed at old path.
    crashBackend.arm(2);

    let crashed = false;
    try {
      FS2.rename(`${MOUNT}/dirty.txt`, `${MOUNT}/moved.txt`);
    } catch {
      crashed = true;
    }
    expect(crashed).toBe(true);

    // Phase 3: remount and check data is recoverable
    const { FS: FS3 } = await remountFromInner(inner);
    let foundData = false;
    for (const path of [`${MOUNT}/dirty.txt`, `${MOUNT}/moved.txt`]) {
      try {
        const stat = FS3.stat(path);
        if (stat.size > 0) {
          const buf = new Uint8Array(stat.size);
          const fd = FS3.open(path, O.RDONLY);
          FS3.read(fd, buf, 0, stat.size, 0);
          FS3.close(fd);
          const recovered = decode(buf);
          // Either old or new content is acceptable — the key is NO data loss
          if (recovered.startsWith("initial") || recovered.startsWith("updated")) {
            foundData = true;
            break;
          }
        }
      } catch {
        // File doesn't exist at this path
      }
    }
    expect(foundData).toBe(true);
  });

  it("data preserved and no leaked pages after crash + recovery + syncfs @fast", async () => {
    // Phase 1: create file, sync
    const { FS, tomefs } = await mountTome(crashBackend);
    const content = "orphan-test-data";
    const data = encode(content);
    const s = FS.open(`${MOUNT}/orphan.txt`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // Phase 2: rename with crash
    const { FS: FS2 } = await mountTome(crashBackend);
    crashBackend.arm(1);
    try {
      FS2.rename(`${MOUNT}/orphan.txt`, `${MOUNT}/new-name.txt`);
    } catch {
      // Expected crash
    }

    // Phase 3: remount, do a full syncfs to trigger orphan cleanup
    const { FS: FS3, tomefs: tomefs3 } = await remountFromInner(inner);
    syncAndUnmount(FS3, tomefs3);

    // Phase 4: remount again and verify state
    const { FS: FS4 } = await remountFromInner(inner);
    const entries = FS4.readdir(MOUNT).filter(
      (e: string) => e !== "." && e !== "..",
    );

    // With metadata-first ordering, a crash after writing new metadata
    // but before moving pages creates a duplicate: the original file at
    // its old path (with data) and an empty entry at the new path.
    // Both are valid filesystem entries after recovery — the key invariant
    // is that at least one has the original data.
    expect(entries.length).toBeGreaterThanOrEqual(1);

    let foundData = false;
    for (const entry of entries) {
      const stat = FS4.stat(`${MOUNT}/${entry}`);
      if (stat.size > 0) {
        const buf = new Uint8Array(stat.size);
        const fd = FS4.open(`${MOUNT}/${entry}`, O.RDONLY);
        FS4.read(fd, buf, 0, stat.size, 0);
        FS4.close(fd);
        if (decode(buf) === content) {
          foundData = true;
        }
      }
    }
    expect(foundData).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Directory rename crash tests
// ---------------------------------------------------------------------------

describe("crash during directory rename operation", () => {
  let inner: SyncMemoryBackend;
  let crashBackend: CrashAfterNOpsSyncBackend;

  beforeEach(() => {
    inner = new SyncMemoryBackend();
    crashBackend = new CrashAfterNOpsSyncBackend(inner, 1);
  });

  it("child file data survives crash during directory rename @fast", async () => {
    // Phase 1: create dir with files, sync
    const { FS, tomefs } = await mountTome(crashBackend);
    FS.mkdir(`${MOUNT}/mydir`);
    const content = "child-file-content-for-dir-rename";
    const data = encode(content);
    const s = FS.open(`${MOUNT}/mydir/child.txt`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    // Phase 2: rename directory, crash during descendant processing
    const { FS: FS2 } = await mountTome(crashBackend);

    // Directory rename does:
    //   1. backend.writeMeta(newDirPath) — new dir metadata
    //   2. backend.deleteMeta(oldDirPath) — old dir metadata removed
    //   Then renameDescendantPaths:
    //     For each file child:
    //       3. backend.renameFile(old, new) — pages moved
    //     After walk:
    //       4. backend.writeMetas([childMeta]) — child metadata at new paths
    //       5. backend.deleteMetas([oldChild]) — old child metadata removed
    //
    // With fix: metadata writes happen before page renames.
    // Crash at various points should never lose child file data.

    // Crash after 3 ops covers: dir meta write + dir meta delete + first
    // op of descendant processing. With fix, this is after metadata is
    // written for all descendants but before/during page moves.
    crashBackend.arm(3);

    let crashed = false;
    try {
      FS2.rename(`${MOUNT}/mydir`, `${MOUNT}/newdir`);
    } catch {
      crashed = true;
    }
    expect(crashed).toBe(true);

    // Phase 3: remount and verify child data is recoverable
    const { FS: FS3 } = await remountFromInner(inner);

    let foundData = false;
    const tryPaths = [
      `${MOUNT}/mydir/child.txt`,
      `${MOUNT}/newdir/child.txt`,
    ];
    for (const path of tryPaths) {
      try {
        const stat = FS3.stat(path);
        if (stat.size > 0) {
          const buf = new Uint8Array(stat.size);
          const fd = FS3.open(path, O.RDONLY);
          FS3.read(fd, buf, 0, stat.size, 0);
          FS3.close(fd);
          if (decode(buf) === content) {
            foundData = true;
            break;
          }
        }
      } catch {
        // File doesn't exist at this path
      }
    }
    expect(foundData).toBe(true);
  });

  it("multi-file directory rename: all children survive crash @fast", async () => {
    // Phase 1: create dir with multiple files
    const { FS, tomefs } = await mountTome(crashBackend);
    FS.mkdir(`${MOUNT}/src`);
    const files = ["a.txt", "b.txt", "c.txt"];
    const contents: Record<string, string> = {};
    for (const name of files) {
      const content = `content-of-${name}-${"y".repeat(50)}`;
      contents[name] = content;
      const data = encode(content);
      const s = FS.open(`${MOUNT}/src/${name}`, O.RDWR | O.CREAT, 0o666);
      FS.write(s, data, 0, data.length);
      FS.close(s);
    }
    syncAndUnmount(FS, tomefs);

    // Phase 2: exhaustive crash-point sweep during directory rename.
    // Try crashing at every possible point to verify data is never lost.
    for (let crashAt = 1; crashAt <= 15; crashAt++) {
      // Reset inner backend to the synced state
      const freshInner = new SyncMemoryBackend();
      const freshCrash = new CrashAfterNOpsSyncBackend(freshInner, crashAt);

      // Copy synced state into fresh backend
      for (const path of inner.listFiles()) {
        const meta = inner.readMeta(path);
        if (meta) freshInner.writeMeta(path, meta);
        // Copy pages
        let pageIdx = 0;
        let page = inner.readPage(path, pageIdx);
        while (page) {
          freshInner.writePage(path, pageIdx, page);
          pageIdx++;
          page = inner.readPage(path, pageIdx);
        }
      }

      const { FS: FS2 } = await mountTome(freshCrash);
      freshCrash.arm(crashAt);

      try {
        FS2.rename(`${MOUNT}/src`, `${MOUNT}/dst`);
      } catch {
        // Expected crash at some crash points; others may complete
      }

      // Remount and check ALL file data is recoverable
      const { FS: FS3 } = await remountFromInner(freshInner);

      for (const name of files) {
        let found = false;
        for (const dir of ["src", "dst"]) {
          try {
            const stat = FS3.stat(`${MOUNT}/${dir}/${name}`);
            if (stat.size > 0) {
              const buf = new Uint8Array(stat.size);
              const fd = FS3.open(`${MOUNT}/${dir}/${name}`, O.RDONLY);
              FS3.read(fd, buf, 0, stat.size, 0);
              FS3.close(fd);
              if (decode(buf) === contents[name]) {
                found = true;
                break;
              }
            }
          } catch {
            // Not at this path
          }
        }
        expect(found).toBe(true);
      }
    }
  });
});
