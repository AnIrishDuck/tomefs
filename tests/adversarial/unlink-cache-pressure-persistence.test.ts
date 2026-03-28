/**
 * Adversarial tests: unlinked files with open fds under extreme cache
 * pressure across persistence (syncfs → remount) cycles.
 *
 * Existing unlink-open-fd tests use the default 4096-page cache, so page
 * eviction never actually triggers. These tests use a 4-page (32 KB) cache
 * to force eviction of unlinked file pages, exercising the critical path:
 *
 *   1. File synced to backend → pages exist in backend at original path
 *   2. Unlink while fd open → pageCache.renameFile moves cache AND backend
 *      pages to /__deleted_* path
 *   3. Cache pressure evicts the unlinked file's pages
 *   4. Read through fd → cache miss → must reload from /__deleted_* in backend
 *
 * This targets the seam between unlink's backend.renameFile (inside
 * pageCache.renameFile) and subsequent cache-miss reloads from the renamed
 * backend path — a code path that no existing test exercises under real
 * eviction pressure.
 *
 * Ethos §9 (adversarial), §6 (correctness).
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
  APPEND: 1024,
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

async function mountTome(backend: SyncMemoryBackend, maxPages = 4) {
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

function syncAndUnmount(FS: any, tomefs: any) {
  syncfs(FS, tomefs);
  FS.unmount(MOUNT);
}

/** Write enough pages to a different file to evict all cached pages. */
function thrashCache(FS: any, pages: number) {
  const s = FS.open(`${MOUNT}/thrash`, O.RDWR | O.CREAT | O.TRUNC, 0o666);
  for (let p = 0; p < pages; p++) {
    const fill = new Uint8Array(PAGE_SIZE);
    fill.fill((p * 7) & 0xff);
    FS.write(s, fill, 0, PAGE_SIZE, p * PAGE_SIZE);
  }
  FS.close(s);
}

describe("adversarial: unlink + cache pressure + persistence (4-page cache)", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  // ------------------------------------------------------------------
  // Core scenario: synced file → unlink → evict → read from backend
  // ------------------------------------------------------------------

  it("synced unlinked file readable after cache eviction @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create 3-page file, sync to backend
    const data = fillPattern(PAGE_SIZE * 3, 0xaa);
    const fd = FS.open(`${MOUNT}/victim`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, data, 0, data.length, 0);
    syncfs(FS, tomefs);

    // Unlink while fd open — pages move to /__deleted_* in both cache and backend
    FS.unlink(`${MOUNT}/victim`);

    // Thrash cache to evict all pages (4-page cache, write 8 pages)
    thrashCache(FS, 8);

    // Read back through the unlinked fd — must reload from /__deleted_* in backend
    for (let p = 0; p < 3; p++) {
      const buf = new Uint8Array(PAGE_SIZE);
      const n = FS.read(fd, buf, 0, PAGE_SIZE, p * PAGE_SIZE);
      expect(n).toBe(PAGE_SIZE);
      expect(buf).toEqual(data.subarray(p * PAGE_SIZE, (p + 1) * PAGE_SIZE));
    }

    FS.close(fd);
  });

  // ------------------------------------------------------------------
  // Write to unlinked file → evict → read back
  // ------------------------------------------------------------------

  it("writes to unlinked file survive cache eviction", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create file, sync, open fd, unlink
    const fd = FS.open(`${MOUNT}/writable`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, fillPattern(PAGE_SIZE, 0x11), 0, PAGE_SIZE, 0);
    syncfs(FS, tomefs);
    FS.unlink(`${MOUNT}/writable`);

    // Write new data through the fd (at /__deleted_* path)
    const newData = fillPattern(PAGE_SIZE, 0x22);
    FS.write(fd, newData, 0, PAGE_SIZE, 0);

    // Also extend the file with a second page
    const page2 = fillPattern(PAGE_SIZE, 0x33);
    FS.write(fd, page2, 0, PAGE_SIZE, PAGE_SIZE);

    // Evict everything
    thrashCache(FS, 8);

    // Read back — page 0 should have new data (0x22 pattern), not old (0x11)
    const buf0 = new Uint8Array(PAGE_SIZE);
    FS.read(fd, buf0, 0, PAGE_SIZE, 0);
    expect(buf0).toEqual(newData);

    // Page 1 should have 0x33 pattern
    const buf1 = new Uint8Array(PAGE_SIZE);
    FS.read(fd, buf1, 0, PAGE_SIZE, PAGE_SIZE);
    expect(buf1).toEqual(page2);

    FS.close(fd);
  });

  // ------------------------------------------------------------------
  // Syncfs with unlinked fd under cache pressure
  // ------------------------------------------------------------------

  it("syncfs preserves unlinked file data under cache pressure @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create file, sync, unlink while fd open
    const data = fillPattern(PAGE_SIZE * 2, 0xbb);
    const fd = FS.open(`${MOUNT}/synced`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, data, 0, data.length, 0);
    syncfs(FS, tomefs);
    FS.unlink(`${MOUNT}/synced`);

    // Thrash cache
    thrashCache(FS, 8);

    // syncfs should preserve /__deleted_* marker and pages
    syncfs(FS, tomefs);

    // Thrash again after syncfs
    thrashCache(FS, 8);

    // Data must still be readable through the fd
    const buf = new Uint8Array(PAGE_SIZE * 2);
    FS.read(fd, buf, 0, buf.length, 0);
    expect(buf).toEqual(data);

    // Verify /__deleted_* marker exists in backend
    const files = backend.listFiles();
    const markers = files.filter((f) => f.startsWith("/__deleted_"));
    expect(markers.length).toBeGreaterThan(0);

    FS.close(fd);
  });

  // ------------------------------------------------------------------
  // Rename-overwrite with open fd under cache pressure
  // ------------------------------------------------------------------

  it("rename-overwrite target readable through fd after eviction", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create target file, open fd, sync
    const targetData = fillPattern(PAGE_SIZE * 2, 0xcc);
    const fd = FS.open(`${MOUNT}/target`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, targetData, 0, targetData.length, 0);
    syncfs(FS, tomefs);

    // Create replacement and rename onto target
    const replData = encode("replacement");
    FS.writeFile(`${MOUNT}/replacement`, replData);
    FS.rename(`${MOUNT}/replacement`, `${MOUNT}/target`);

    // Target's pages moved to /__deleted_* — thrash cache to evict them
    thrashCache(FS, 8);

    // Old data still readable through the pre-rename fd
    const buf = new Uint8Array(PAGE_SIZE * 2);
    const n = FS.read(fd, buf, 0, buf.length, 0);
    expect(n).toBe(PAGE_SIZE * 2);
    expect(buf).toEqual(targetData);

    FS.close(fd);
  });

  // ------------------------------------------------------------------
  // Multiple unlinked files competing for cache slots
  // ------------------------------------------------------------------

  it("multiple unlinked files survive interleaved reads under eviction", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create 3 files, each 2 pages, sync all
    const fds: any[] = [];
    const patterns: Uint8Array[] = [];
    for (let i = 0; i < 3; i++) {
      const data = fillPattern(PAGE_SIZE * 2, i * 50);
      const fd = FS.open(`${MOUNT}/file${i}`, O.RDWR | O.CREAT, 0o666);
      FS.write(fd, data, 0, data.length, 0);
      fds.push(fd);
      patterns.push(data);
    }
    syncfs(FS, tomefs);

    // Unlink all while fds open
    for (let i = 0; i < 3; i++) {
      FS.unlink(`${MOUNT}/file${i}`);
    }

    // Interleaved reads — with 4-page cache, each read evicts pages from others
    for (let round = 0; round < 3; round++) {
      for (let i = 0; i < 3; i++) {
        for (let p = 0; p < 2; p++) {
          const buf = new Uint8Array(PAGE_SIZE);
          const n = FS.read(fds[i], buf, 0, PAGE_SIZE, p * PAGE_SIZE);
          expect(n).toBe(PAGE_SIZE);
          expect(buf).toEqual(
            patterns[i].subarray(p * PAGE_SIZE, (p + 1) * PAGE_SIZE),
          );
        }
      }
    }

    for (const fd of fds) FS.close(fd);
  });

  // ------------------------------------------------------------------
  // Unlink → evict → syncfs → remount → verify cleanup
  // ------------------------------------------------------------------

  it("unlinked file pages cleaned up after close + syncfs + remount @fast", async () => {
    // Session 1: create, sync, unlink, close, syncfs
    {
      const { FS, tomefs } = await mountTome(backend);
      const data = fillPattern(PAGE_SIZE * 3, 0xdd);
      const fd = FS.open(`${MOUNT}/cleanup`, O.RDWR | O.CREAT, 0o666);
      FS.write(fd, data, 0, data.length, 0);
      syncfs(FS, tomefs);

      FS.unlink(`${MOUNT}/cleanup`);
      thrashCache(FS, 8);

      // Close fd — should delete /__deleted_* pages and metadata
      FS.close(fd);

      // Syncfs to finalize
      syncAndUnmount(FS, tomefs);
    }

    // Verify: no /__deleted_* entries in backend
    const files = backend.listFiles();
    const orphans = files.filter((f) => f.startsWith("/__deleted_"));
    expect(orphans).toHaveLength(0);

    // Session 2: remount — no ghost files
    {
      const { FS } = await mountTome(backend);
      const entries = FS.readdir(`${MOUNT}`);
      // Only . and .. and possibly thrash
      expect(entries).not.toContain("cleanup");
    }
  });

  // ------------------------------------------------------------------
  // Crash between unlink and close — orphan recovery under pressure
  // ------------------------------------------------------------------

  it("crash recovery: orphaned /__deleted_* cleaned up after remount + syncfs", async () => {
    // Session 1: create, sync, unlink with open fd, crash (no close)
    {
      const { FS, tomefs } = await mountTome(backend);
      const data = fillPattern(PAGE_SIZE * 2, 0xee);
      const fd = FS.open(`${MOUNT}/crashed`, O.RDWR | O.CREAT, 0o666);
      FS.write(fd, data, 0, data.length, 0);
      syncfs(FS, tomefs);

      FS.unlink(`${MOUNT}/crashed`);
      syncfs(FS, tomefs); // Persists /__deleted_* marker

      // "Crash" — fd never closed, no cleanup
    }

    // Verify /__deleted_* exists in backend
    const preFiles = backend.listFiles();
    expect(preFiles.some((f) => f.startsWith("/__deleted_"))).toBe(true);

    // Session 2: remount and syncfs — orphan cleanup
    {
      const { FS, tomefs } = await mountTome(backend);

      // /__deleted_* entries should NOT appear as files
      const entries = FS.readdir(`${MOUNT}`);
      expect(entries.filter((e: string) => e.startsWith("__deleted_"))).toHaveLength(0);

      // syncfs orphan cleanup removes the /__deleted_* entries
      syncfs(FS, tomefs);

      const postFiles = backend.listFiles();
      expect(postFiles.filter((f) => f.startsWith("/__deleted_"))).toHaveLength(0);
    }
  });

  // ------------------------------------------------------------------
  // New file at same path after unlink — no data leakage under pressure
  // ------------------------------------------------------------------

  it("new file at unlinked path has clean pages after cache eviction", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create file with known pattern, sync
    const oldData = fillPattern(PAGE_SIZE * 2, 0xff);
    const fd = FS.open(`${MOUNT}/reuse`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, oldData, 0, oldData.length, 0);
    syncfs(FS, tomefs);

    // Unlink while fd open — pages move to /__deleted_*
    FS.unlink(`${MOUNT}/reuse`);

    // Create new file at same path with different data
    const newData = fillPattern(PAGE_SIZE, 0x42);
    const fd2 = FS.open(`${MOUNT}/reuse`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd2, newData, 0, newData.length, 0);

    // Thrash to evict both files' pages
    thrashCache(FS, 8);

    // New file must have only new data
    const buf = new Uint8Array(PAGE_SIZE);
    FS.read(fd2, buf, 0, PAGE_SIZE, 0);
    expect(buf).toEqual(newData);
    expect(FS.fstat(fd2.fd).size).toBe(PAGE_SIZE);

    // Old file through original fd must still have old data
    const oldBuf = new Uint8Array(PAGE_SIZE * 2);
    FS.read(fd, oldBuf, 0, PAGE_SIZE * 2, 0);
    expect(oldBuf).toEqual(oldData);

    FS.close(fd);
    FS.close(fd2);
  });

  // ------------------------------------------------------------------
  // Dup'd fd on unlinked file under cache pressure
  // ------------------------------------------------------------------

  it("dup'd fd on unlinked file survives eviction", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create file, sync, dup the stream, unlink
    const data = fillPattern(PAGE_SIZE * 2, 0x77);
    const s = FS.open(`${MOUNT}/duped`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length, 0);
    syncfs(FS, tomefs);

    const dup = FS.dupStream(s);
    FS.unlink(`${MOUNT}/duped`);

    // Close original fd — dup still holds a reference
    FS.close(s);

    // Thrash to evict pages
    thrashCache(FS, 8);

    // Read through dup'd fd
    const buf = new Uint8Array(PAGE_SIZE * 2);
    const n = FS.read(dup, buf, 0, buf.length, 0);
    expect(n).toBe(PAGE_SIZE * 2);
    expect(buf).toEqual(data);

    FS.close(dup);
  });

  // ------------------------------------------------------------------
  // Write + truncate on unlinked file under cache pressure
  // ------------------------------------------------------------------

  it("truncate unlinked file under cache pressure preserves remaining pages", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create 3-page file, sync, unlink
    const data = fillPattern(PAGE_SIZE * 3, 0x55);
    const fd = FS.open(`${MOUNT}/trunc`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, data, 0, data.length, 0);
    syncfs(FS, tomefs);
    FS.unlink(`${MOUNT}/trunc`);

    // Truncate to 1 page via ftruncate
    FS.ftruncate(fd.fd, PAGE_SIZE);

    // Thrash to evict
    thrashCache(FS, 8);

    // Verify size and remaining data
    expect(FS.fstat(fd.fd).size).toBe(PAGE_SIZE);
    const buf = new Uint8Array(PAGE_SIZE);
    FS.read(fd, buf, 0, PAGE_SIZE, 0);
    expect(buf).toEqual(data.subarray(0, PAGE_SIZE));

    FS.close(fd);
  });

  // ------------------------------------------------------------------
  // Multi-cycle: unlink → syncfs → thrash → syncfs → thrash → read
  // ------------------------------------------------------------------

  it("unlinked file data survives multiple syncfs+eviction cycles @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const data = fillPattern(PAGE_SIZE * 2, 0x99);
    const fd = FS.open(`${MOUNT}/multicycle`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, data, 0, data.length, 0);
    syncfs(FS, tomefs);
    FS.unlink(`${MOUNT}/multicycle`);

    // 3 rounds of syncfs + full cache eviction
    for (let round = 0; round < 3; round++) {
      syncfs(FS, tomefs);
      thrashCache(FS, 8);
    }

    // Data still correct after all that pressure
    const buf = new Uint8Array(PAGE_SIZE * 2);
    FS.read(fd, buf, 0, buf.length, 0);
    expect(buf).toEqual(data);

    FS.close(fd);
  });
});
