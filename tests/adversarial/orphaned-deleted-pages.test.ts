/**
 * Adversarial tests: orphaned /__deleted_* page cleanup after crash.
 *
 * When a file with open fds is overwritten by rename or unlinked, its pages
 * are moved to a /__deleted_* temporary path. Normally these are cleaned up
 * when the last fd closes. But if the process crashes while fds are open,
 * the orphaned pages would persist in the backend forever.
 *
 * Fix: we write marker metadata for /__deleted_* paths, so syncfs orphan
 * cleanup can discover and remove them after a crash. restoreTree skips
 * these paths (they're not real files), and syncfs preserves them for
 * live unlinked nodes that still have open fds.
 *
 * Ethos §9 (adversarial), §6 (correctness).
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

function fillPattern(size: number, seed: number): Uint8Array {
  const buf = new Uint8Array(size);
  for (let i = 0; i < size; i++) {
    buf[i] = (seed + i * 31) & 0xff;
  }
  return buf;
}

async function mountTome(backend: SyncMemoryBackend, maxPages = 64) {
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

function syncfs(FS: any, tomefs: any) {
  tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
    if (err) throw err;
  });
}

describe("adversarial: orphaned /__deleted_* page cleanup", () => {
  // ------------------------------------------------------------------
  // Scenario: rename overwrites file with open fd, then crash
  // ------------------------------------------------------------------

  it("cleans up orphaned pages from rename-overwrite after simulated crash", async () => {
    const backend = new SyncMemoryBackend();

    // Session 1: create file, open it, overwrite via rename, then "crash"
    {
      const { FS, tomefs } = await mountTome(backend);

      // Create target file and keep fd open
      const data = fillPattern(PAGE_SIZE * 2, 0xaa);
      const fd = FS.open(MOUNT + "/target", O.RDWR | O.CREAT, 0o666);
      FS.write(fd, data, 0, data.length, 0);

      // Sync to persist
      syncfs(FS, tomefs);

      // Create replacement file
      const newData = fillPattern(PAGE_SIZE, 0xbb);
      FS.writeFile(MOUNT + "/replacement", newData);

      // Rename replacement over target — target's pages move to /__deleted_*
      // while fd is still open
      FS.rename(MOUNT + "/replacement", MOUNT + "/target");

      // Sync — /__deleted_* marker metadata should be preserved because fd is open
      syncfs(FS, tomefs);

      // Verify: backend should have /__deleted_* marker metadata
      const files = backend.listFiles();
      const deletedPaths = files.filter((f) => f.startsWith("/__deleted_"));
      expect(deletedPaths.length).toBeGreaterThan(0);

      // "Crash" — don't close the fd, don't clean up
      // The /__deleted_* pages and marker metadata survive in the backend
    }

    // Verify: orphaned /__deleted_* entries exist in backend after crash
    const filesBeforeRecover = backend.listFiles();
    const orphansBefore = filesBeforeRecover.filter((f) =>
      f.startsWith("/__deleted_"),
    );
    expect(orphansBefore.length).toBeGreaterThan(0);

    // Session 2: remount and syncfs — should clean up orphaned /__deleted_*
    {
      const { FS, tomefs } = await mountTome(backend);

      // restoreTree should NOT restore /__deleted_* entries as files
      const entries = FS.readdir(MOUNT);
      const deletedEntries = entries.filter((e: string) =>
        e.startsWith("__deleted_"),
      );
      expect(deletedEntries).toHaveLength(0);

      // The real file should be restored correctly
      expect(entries).toContain("target");

      // syncfs orphan cleanup should remove the /__deleted_* entries
      syncfs(FS, tomefs);

      const filesAfter = backend.listFiles();
      const orphansAfter = filesAfter.filter((f) =>
        f.startsWith("/__deleted_"),
      );
      expect(orphansAfter).toHaveLength(0);
    }
  });

  // ------------------------------------------------------------------
  // Scenario: unlink file with open fd, then crash
  // ------------------------------------------------------------------

  it("cleans up orphaned pages from unlink-with-open-fd after simulated crash", async () => {
    const backend = new SyncMemoryBackend();

    // Session 1: create file, open it, unlink it, then "crash"
    {
      const { FS, tomefs } = await mountTome(backend);

      // Create file with data spanning multiple pages
      const data = fillPattern(PAGE_SIZE * 3, 0xcc);
      const fd = FS.open(MOUNT + "/doomed", O.RDWR | O.CREAT, 0o666);
      FS.write(fd, data, 0, data.length, 0);

      // Sync to persist pages to backend
      syncfs(FS, tomefs);

      // Unlink while fd is open — pages move to /__deleted_*
      FS.unlink(MOUNT + "/doomed");

      // Verify fd still readable (POSIX unlink semantics)
      const buf = new Uint8Array(PAGE_SIZE * 3);
      FS.read(fd, buf, 0, PAGE_SIZE * 3, 0);
      expect(buf[0]).toBe(data[0]);

      // Sync — /__deleted_* preserved because fd is open
      syncfs(FS, tomefs);

      const files = backend.listFiles();
      const deletedPaths = files.filter((f) => f.startsWith("/__deleted_"));
      expect(deletedPaths.length).toBeGreaterThan(0);

      // "Crash" — fd never closed
    }

    // Session 2: remount, verify cleanup
    {
      const { FS, tomefs } = await mountTome(backend);

      // /doomed should not be restored (it was unlinked before sync)
      const entries = FS.readdir(MOUNT);
      expect(entries).not.toContain("doomed");

      // syncfs should clean up orphaned /__deleted_* entries
      syncfs(FS, tomefs);

      const files = backend.listFiles();
      const orphans = files.filter((f) => f.startsWith("/__deleted_"));
      expect(orphans).toHaveLength(0);
    }
  });

  // ------------------------------------------------------------------
  // Normal operation: /__deleted_* cleaned up on fd close (no crash)
  // ------------------------------------------------------------------

  it("cleans up /__deleted_* marker on normal fd close (no crash)", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    // Create target and open fd
    const data = encode("will be overwritten");
    const fd = FS.open(MOUNT + "/target", O.RDWR | O.CREAT, 0o666);
    FS.write(fd, data, 0, data.length, 0);
    syncfs(FS, tomefs);

    // Create replacement and rename over target
    FS.writeFile(MOUNT + "/replacement", encode("new content"));
    FS.rename(MOUNT + "/replacement", MOUNT + "/target");

    // /__deleted_* marker exists while fd is open
    let files = backend.listFiles();
    expect(files.some((f) => f.startsWith("/__deleted_"))).toBe(true);

    // Close the fd — should clean up /__deleted_* pages AND marker metadata
    FS.close(fd);

    files = backend.listFiles();
    expect(files.some((f) => f.startsWith("/__deleted_"))).toBe(false);
  });

  // ------------------------------------------------------------------
  // syncfs preserves /__deleted_* for live nodes with open fds
  // ------------------------------------------------------------------

  it("syncfs preserves /__deleted_* pages while fds are still open", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    // Create file and keep fd open
    const data = fillPattern(PAGE_SIZE, 0xdd);
    const fd = FS.open(MOUNT + "/keep", O.RDWR | O.CREAT, 0o666);
    FS.write(fd, data, 0, data.length, 0);
    syncfs(FS, tomefs);

    // Overwrite via rename — pages move to /__deleted_*
    FS.writeFile(MOUNT + "/new", encode("replacement"));
    FS.rename(MOUNT + "/new", MOUNT + "/keep");

    // syncfs should NOT clean up /__deleted_* — fd is still open
    syncfs(FS, tomefs);

    // Verify old data still readable through open fd
    const buf = new Uint8Array(PAGE_SIZE);
    FS.read(fd, buf, 0, PAGE_SIZE, 0);
    expect(buf[0]).toBe(data[0]);

    // Close fd — now cleanup happens
    FS.close(fd);

    const files = backend.listFiles();
    expect(files.some((f) => f.startsWith("/__deleted_"))).toBe(false);
  });

  // ------------------------------------------------------------------
  // Multiple orphaned paths cleaned up in one syncfs pass
  // ------------------------------------------------------------------

  it("cleans up multiple orphaned /__deleted_* paths after crash", async () => {
    const backend = new SyncMemoryBackend();

    // Session 1: create multiple files, open them, overwrite them all, crash
    {
      const { FS, tomefs } = await mountTome(backend);

      const fds: any[] = [];
      for (let i = 0; i < 5; i++) {
        const data = fillPattern(PAGE_SIZE, i);
        const fd = FS.open(MOUNT + `/file${i}`, O.RDWR | O.CREAT, 0o666);
        FS.write(fd, data, 0, data.length, 0);
        fds.push(fd);
      }
      syncfs(FS, tomefs);

      // Overwrite each via rename
      for (let i = 0; i < 5; i++) {
        FS.writeFile(MOUNT + `/new${i}`, encode(`replacement${i}`));
        FS.rename(MOUNT + `/new${i}`, MOUNT + `/file${i}`);
      }
      syncfs(FS, tomefs);

      // Verify multiple /__deleted_* markers exist
      const files = backend.listFiles();
      const deletedPaths = files.filter((f) => f.startsWith("/__deleted_"));
      expect(deletedPaths.length).toBe(5);

      // "Crash"
    }

    // Session 2: remount, syncfs cleans up all orphans
    {
      const { FS, tomefs } = await mountTome(backend);
      syncfs(FS, tomefs);

      const files = backend.listFiles();
      const orphans = files.filter((f) => f.startsWith("/__deleted_"));
      expect(orphans).toHaveLength(0);

      // Real files should still be there with replacement content
      for (let i = 0; i < 5; i++) {
        const content = FS.readFile(MOUNT + `/file${i}`, {
          encoding: "utf8",
        });
        expect(content).toBe(`replacement${i}`);
      }
    }
  });
});
