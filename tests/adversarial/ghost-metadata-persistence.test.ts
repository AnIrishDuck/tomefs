/**
 * Adversarial tests: ghost metadata re-persistence after node removal.
 *
 * When a directory, symlink, or file target is removed from the tree (via
 * rmdir, unlink, or rename overwrite), its node may still be in the
 * dirtyMetaNodes set if it was created or modified since the last syncfs.
 * Without proper cleanup, the incremental syncfs path would re-persist
 * this node's metadata — re-creating a backend entry that was just deleted,
 * producing a "ghost" entry that resurrects on remount.
 *
 * This is a real correctness bug: after rmdir + syncfs + remount, the
 * deleted directory reappears. The old workaround forced a full tree walk
 * (via needsOrphanCleanup = true) after every rmdir/unlink/rename, which
 * was correct but O(tree + backend) per syncfs — expensive for PGlite
 * workloads where syncToFs is called after every query.
 *
 * The fix: clean up dirtyMetaNodes when nodes are removed from the tree,
 * and stop setting needsOrphanCleanup for operations that complete their
 * own backend cleanup. This lets syncfs use the O(dirty) incremental path
 * instead of the O(tree + backend) full tree walk.
 *
 * Tests verify BOTH correctness (no ghosts) AND performance (incremental
 * path used) by counting backend.listFiles() calls during syncfs — the
 * full tree walk calls listFiles() for orphan cleanup, the incremental
 * path does not.
 *
 * Ethos §9: "Write tests designed to break tomefs specifically — target
 * the seams"
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
 * SyncMemoryBackend that counts listFiles() calls.
 *
 * The full tree walk path in syncfs calls listFiles() for orphan cleanup;
 * the incremental path does not. Counting calls lets tests verify which
 * path was taken — proving the performance benefit of the fix.
 *
 * Not a mock — delegates all operations to the real SyncMemoryBackend.
 */
class CountingBackend extends SyncMemoryBackend {
  listFilesCalls = 0;
  counting = false;

  startCounting(): void {
    this.listFilesCalls = 0;
    this.counting = true;
  }

  stopCounting(): void {
    this.counting = false;
  }

  listFiles(): string[] {
    if (this.counting) {
      this.listFilesCalls++;
    }
    return super.listFiles();
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

function syncfs(FS: any, tomefs: any, populate = false) {
  tomefs.syncfs(
    FS.lookupPath(MOUNT).node.mount,
    populate,
    (err: any) => {
      if (err) throw err;
    },
  );
}

describe("ghost metadata re-persistence", () => {
  let backend: CountingBackend;

  beforeEach(() => {
    backend = new CountingBackend();
  });

  // -------------------------------------------------------------------
  // mkdir + rmdir: incremental path, no ghosts
  // -------------------------------------------------------------------

  it("mkdir + rmdir before syncfs does not re-persist directory metadata @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Initial sync to establish clean state (uses full tree walk)
    syncfs(FS, tomefs);

    // Create and immediately remove a directory
    FS.mkdir(`${MOUNT}/ghost_dir`);
    FS.rmdir(`${MOUNT}/ghost_dir`);

    // Syncfs should use incremental path (no listFiles call)
    backend.startCounting();
    syncfs(FS, tomefs);
    backend.stopCounting();
    expect(backend.listFilesCalls).toBe(0);

    // Remount — ghost directory should not reappear
    const { FS: FS2 } = await mountTome(backend);
    const entries = FS2.readdir(`${MOUNT}`);
    expect(entries).toEqual([".", ".."]);
  });

  it("mkdir + sync + rmdir uses incremental path on second sync", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create directory and sync (persists metadata)
    FS.mkdir(`${MOUNT}/persisted_dir`);
    syncfs(FS, tomefs);
    expect(backend.readMeta("/persisted_dir")).not.toBeNull();

    // Remove and sync — should use incremental path
    FS.rmdir(`${MOUNT}/persisted_dir`);
    backend.startCounting();
    syncfs(FS, tomefs);
    backend.stopCounting();
    expect(backend.listFilesCalls).toBe(0);

    // Metadata should be gone from backend
    expect(backend.readMeta("/persisted_dir")).toBeNull();

    // Remount — directory should not reappear
    const { FS: FS2 } = await mountTome(backend);
    const entries = FS2.readdir(`${MOUNT}`);
    expect(entries).toEqual([".", ".."]);
  });

  // -------------------------------------------------------------------
  // symlink + unlink: incremental path, no ghosts
  // -------------------------------------------------------------------

  it("symlink + unlink before syncfs does not re-persist symlink metadata @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);
    syncfs(FS, tomefs);

    // Create and immediately remove a symlink
    FS.symlink("/target", `${MOUNT}/ghost_link`);
    FS.unlink(`${MOUNT}/ghost_link`);

    // Should use incremental path
    backend.startCounting();
    syncfs(FS, tomefs);
    backend.stopCounting();
    expect(backend.listFilesCalls).toBe(0);

    // Remount — ghost symlink should not reappear
    const { FS: FS2 } = await mountTome(backend);
    const entries = FS2.readdir(`${MOUNT}`);
    expect(entries).toEqual([".", ".."]);
  });

  it("symlink + sync + unlink uses incremental path on second sync", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.symlink("/target", `${MOUNT}/link`);
    syncfs(FS, tomefs);
    expect(backend.readMeta("/link")).not.toBeNull();

    FS.unlink(`${MOUNT}/link`);
    backend.startCounting();
    syncfs(FS, tomefs);
    backend.stopCounting();
    expect(backend.listFilesCalls).toBe(0);

    expect(backend.readMeta("/link")).toBeNull();
  });

  // -------------------------------------------------------------------
  // rename overwriting a file target: incremental path, no ghosts
  // -------------------------------------------------------------------

  it("rename overwriting a file uses incremental path @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.writeFile(`${MOUNT}/src`, "source data");
    FS.writeFile(`${MOUNT}/dst`, "destination data");
    syncfs(FS, tomefs);

    FS.rename(`${MOUNT}/src`, `${MOUNT}/dst`);
    backend.startCounting();
    syncfs(FS, tomefs);
    backend.stopCounting();
    expect(backend.listFilesCalls).toBe(0);

    // Remount and verify only dst exists with source data
    const { FS: FS2 } = await mountTome(backend);
    const entries = FS2.readdir(`${MOUNT}`).filter(
      (e: string) => e !== "." && e !== "..",
    );
    expect(entries).toEqual(["dst"]);
    const content = new TextDecoder().decode(FS2.readFile(`${MOUNT}/dst`));
    expect(content).toBe("source data");
  });

  it("create + rename overwrite before first syncfs uses incremental path", async () => {
    const { FS, tomefs } = await mountTome(backend);
    syncfs(FS, tomefs);

    // Create two files (both dirty) and rename to overwrite
    FS.writeFile(`${MOUNT}/a`, "alpha");
    FS.writeFile(`${MOUNT}/b`, "beta");
    FS.rename(`${MOUNT}/a`, `${MOUNT}/b`);

    backend.startCounting();
    syncfs(FS, tomefs);
    backend.stopCounting();
    expect(backend.listFilesCalls).toBe(0);

    // Remount and verify
    const { FS: FS2 } = await mountTome(backend);
    const entries = FS2.readdir(`${MOUNT}`).filter(
      (e: string) => e !== "." && e !== "..",
    );
    expect(entries).toEqual(["b"]);
    const content = new TextDecoder().decode(FS2.readFile(`${MOUNT}/b`));
    expect(content).toBe("alpha");
  });

  // -------------------------------------------------------------------
  // rename overwriting a directory target
  // -------------------------------------------------------------------

  it("rename directory over empty directory uses incremental path @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/dir_src`);
    FS.mkdir(`${MOUNT}/dir_dst`);
    syncfs(FS, tomefs);

    FS.rename(`${MOUNT}/dir_src`, `${MOUNT}/dir_dst`);
    backend.startCounting();
    syncfs(FS, tomefs);
    backend.stopCounting();
    expect(backend.listFilesCalls).toBe(0);

    // Remount — only dir_dst should exist
    const { FS: FS2 } = await mountTome(backend);
    const entries = FS2.readdir(`${MOUNT}`).filter(
      (e: string) => e !== "." && e !== "..",
    );
    expect(entries).toEqual(["dir_dst"]);
  });

  // -------------------------------------------------------------------
  // Multiple operations in sequence
  // -------------------------------------------------------------------

  it("rmdir + unlink symlink + rename in sequence all use incremental path", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/subdir`);
    FS.symlink("/target", `${MOUNT}/mylink`);
    FS.writeFile(`${MOUNT}/file1`, "one");
    FS.writeFile(`${MOUNT}/file2`, "two");
    syncfs(FS, tomefs);

    // Remove directory, symlink, and rename-overwrite file
    FS.rmdir(`${MOUNT}/subdir`);
    FS.unlink(`${MOUNT}/mylink`);
    FS.rename(`${MOUNT}/file1`, `${MOUNT}/file2`);

    backend.startCounting();
    syncfs(FS, tomefs);
    backend.stopCounting();
    expect(backend.listFilesCalls).toBe(0);

    // Backend should only have file2 metadata (plus root and clean marker)
    const files = backend.listFiles();
    const userFiles = files.filter(
      (f: string) => !f.startsWith("/__tomefs") && f !== "/",
    );
    expect(userFiles).toEqual(["/file2"]);

    // Remount and verify
    const { FS: FS2 } = await mountTome(backend);
    const entries = FS2.readdir(`${MOUNT}`).filter(
      (e: string) => e !== "." && e !== "..",
    );
    expect(entries).toEqual(["file2"]);
    const content = new TextDecoder().decode(FS2.readFile(`${MOUNT}/file2`));
    expect(content).toBe("one");
  });

  // -------------------------------------------------------------------
  // Repeated cycles
  // -------------------------------------------------------------------

  it("repeated mkdir + rmdir cycles don't accumulate ghost metadata", async () => {
    const { FS, tomefs } = await mountTome(backend);
    syncfs(FS, tomefs);

    for (let i = 0; i < 10; i++) {
      FS.mkdir(`${MOUNT}/cycle_dir`);
      FS.rmdir(`${MOUNT}/cycle_dir`);
      backend.startCounting();
      syncfs(FS, tomefs);
      backend.stopCounting();
      // Every cycle should use the incremental path
      expect(backend.listFilesCalls).toBe(0);
    }

    const files = backend.listFiles();
    const userFiles = files.filter(
      (f: string) => !f.startsWith("/__tomefs") && f !== "/",
    );
    expect(userFiles).toEqual([]);
  });

  it("repeated symlink + unlink cycles don't accumulate ghost metadata", async () => {
    const { FS, tomefs } = await mountTome(backend);
    syncfs(FS, tomefs);

    for (let i = 0; i < 10; i++) {
      FS.symlink("/target", `${MOUNT}/cycle_link`);
      FS.unlink(`${MOUNT}/cycle_link`);
      backend.startCounting();
      syncfs(FS, tomefs);
      backend.stopCounting();
      expect(backend.listFilesCalls).toBe(0);
    }

    const files = backend.listFiles();
    const userFiles = files.filter(
      (f: string) => !f.startsWith("/__tomefs") && f !== "/",
    );
    expect(userFiles).toEqual([]);
  });

  // -------------------------------------------------------------------
  // Deep directory trees
  // -------------------------------------------------------------------

  it("rmdir of nested directories uses incremental path", async () => {
    const { FS, tomefs } = await mountTome(backend);
    syncfs(FS, tomefs);

    FS.mkdir(`${MOUNT}/a`);
    FS.mkdir(`${MOUNT}/a/b`);
    FS.mkdir(`${MOUNT}/a/b/c`);

    FS.rmdir(`${MOUNT}/a/b/c`);
    FS.rmdir(`${MOUNT}/a/b`);
    FS.rmdir(`${MOUNT}/a`);

    backend.startCounting();
    syncfs(FS, tomefs);
    backend.stopCounting();
    expect(backend.listFilesCalls).toBe(0);

    const { FS: FS2 } = await mountTome(backend);
    const entries = FS2.readdir(`${MOUNT}`);
    expect(entries).toEqual([".", ".."]);
  });

  // -------------------------------------------------------------------
  // File data integrity
  // -------------------------------------------------------------------

  it("file data survives create + rename overwrite + sync + remount", async () => {
    const { FS, tomefs } = await mountTome(backend);
    syncfs(FS, tomefs);

    // Write multi-page file
    const data = new Uint8Array(PAGE_SIZE * 3);
    for (let i = 0; i < data.length; i++) data[i] = i & 0xff;

    const fd = FS.open(`${MOUNT}/big`, O.WRONLY | O.CREAT | O.TRUNC);
    FS.write(fd, data, 0, data.length, 0);
    FS.close(fd);

    FS.writeFile(`${MOUNT}/target`, "old");
    FS.rename(`${MOUNT}/big`, `${MOUNT}/target`);
    syncfs(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const readBuf = new Uint8Array(PAGE_SIZE * 3);
    const fd2 = FS2.open(`${MOUNT}/target`, O.RDONLY);
    FS2.read(fd2, readBuf, 0, readBuf.length, 0);
    FS2.close(fd2);
    expect(readBuf).toEqual(data);
  });

  // -------------------------------------------------------------------
  // Cache pressure
  // -------------------------------------------------------------------

  it("ghost cleanup works under cache pressure (maxPages=4) @fast", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);
    syncfs(FS, tomefs);

    for (let i = 0; i < 8; i++) {
      const data = new Uint8Array(PAGE_SIZE);
      data.fill(i);
      const fd = FS.open(
        `${MOUNT}/f${i}`,
        O.WRONLY | O.CREAT | O.TRUNC,
      );
      FS.write(fd, data, 0, data.length, 0);
      FS.close(fd);
    }
    syncfs(FS, tomefs);

    FS.mkdir(`${MOUNT}/dir_under_pressure`);
    FS.symlink("/target", `${MOUNT}/link_under_pressure`);
    FS.rmdir(`${MOUNT}/dir_under_pressure`);
    FS.unlink(`${MOUNT}/link_under_pressure`);
    FS.rename(`${MOUNT}/f0`, `${MOUNT}/f1`);

    backend.startCounting();
    syncfs(FS, tomefs);
    backend.stopCounting();
    expect(backend.listFilesCalls).toBe(0);

    const { FS: FS2 } = await mountTome(backend, 4);
    const entries = FS2.readdir(`${MOUNT}`).filter(
      (e: string) => e !== "." && e !== "..",
    );
    expect(entries.sort()).toEqual(
      ["f1", "f2", "f3", "f4", "f5", "f6", "f7"],
    );

    // Verify f1 has f0's data (all zeros)
    const buf = new Uint8Array(PAGE_SIZE);
    const fd = FS2.open(`${MOUNT}/f1`, O.RDONLY);
    FS2.read(fd, buf, 0, PAGE_SIZE, 0);
    FS2.close(fd);
    expect(buf[0]).toBe(0);
    expect(buf[PAGE_SIZE - 1]).toBe(0);
  });

  // -------------------------------------------------------------------
  // unlink file (no open fds) also uses incremental path
  // -------------------------------------------------------------------

  it("unlink file with no open fds uses incremental path", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.writeFile(`${MOUNT}/doomed`, "data");
    syncfs(FS, tomefs);

    FS.unlink(`${MOUNT}/doomed`);
    backend.startCounting();
    syncfs(FS, tomefs);
    backend.stopCounting();
    expect(backend.listFilesCalls).toBe(0);

    expect(backend.readMeta("/doomed")).toBeNull();
  });

  // -------------------------------------------------------------------
  // rename file (no target) uses incremental path
  // -------------------------------------------------------------------

  it("rename file to new name uses incremental path", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.writeFile(`${MOUNT}/old_name`, "data");
    syncfs(FS, tomefs);

    FS.rename(`${MOUNT}/old_name`, `${MOUNT}/new_name`);
    backend.startCounting();
    syncfs(FS, tomefs);
    backend.stopCounting();
    expect(backend.listFilesCalls).toBe(0);

    expect(backend.readMeta("/old_name")).toBeNull();
    expect(backend.readMeta("/new_name")).not.toBeNull();

    const { FS: FS2 } = await mountTome(backend);
    const content = new TextDecoder().decode(
      FS2.readFile(`${MOUNT}/new_name`),
    );
    expect(content).toBe("data");
  });
});
