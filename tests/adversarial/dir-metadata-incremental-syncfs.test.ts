/**
 * Adversarial tests: directory metadata persistence through incremental syncfs.
 *
 * The incremental syncfs path computes storage paths differently for files
 * and directories: files use node.storagePath (cached at creation), while
 * directories use nodeStoragePath() (walks the parent chain). This test
 * verifies that directory metadata (mode, timestamps) modified via chmod
 * and utime is correctly persisted through the incremental path and
 * survives remount.
 *
 * The existing incremental-syncfs-cycles tests exercise chmod/utime on
 * FILES, which go through node.storagePath. Directories go through a
 * different code path (nodeStoragePath) that could diverge if the mount
 * prefix stripping logic has a bug or if markMetaDirty is missed.
 *
 * Ethos §2 (real POSIX semantics), §9 (adversarial — target the seams).
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

function fillPattern(size: number, seed: number): Uint8Array {
  const buf = new Uint8Array(size);
  for (let i = 0; i < size; i++) {
    buf[i] = (seed + i * 31) & 0xff;
  }
  return buf;
}

async function mountTome(backend: SyncMemoryBackend, maxPages = 4096) {
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

function syncfs(FS: any, tomefs: any): void {
  tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
    if (err) throw err;
  });
}

describe("adversarial: directory metadata through incremental syncfs", () => {

  it("chmod on directory persists through incremental syncfs + remount @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/subdir`);
    syncfs(FS, tomefs);

    FS.chmod(`${MOUNT}/subdir`, 0o755);
    syncfs(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/subdir`);
    expect(stat.mode & 0o777).toBe(0o755);
  });

  it("utime on directory persists through incremental syncfs + remount @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/timestamps`);
    syncfs(FS, tomefs);

    const atime = 1500000;
    const mtime = 2500000;
    FS.utime(`${MOUNT}/timestamps`, atime, mtime);
    syncfs(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/timestamps`);
    expect(stat.atime.getTime()).toBe(atime);
    expect(stat.mtime.getTime()).toBe(mtime);
  });

  it("chmod on nested directory persists correctly @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/a`);
    FS.mkdir(`${MOUNT}/a/b`);
    FS.mkdir(`${MOUNT}/a/b/c`);
    syncfs(FS, tomefs);

    FS.chmod(`${MOUNT}/a/b/c`, 0o700);
    FS.chmod(`${MOUNT}/a`, 0o750);
    syncfs(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    expect(FS2.stat(`${MOUNT}/a`).mode & 0o777).toBe(0o750);
    expect(FS2.stat(`${MOUNT}/a/b/c`).mode & 0o777).toBe(0o700);
    // b was not chmod'd — should retain its original mode
    const bMode = FS2.stat(`${MOUNT}/a/b`).mode & 0o777;
    expect(bMode).toBe(0o777);
  });

  it("directory chmod + file write in same sync cycle both persist", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/mixed`);
    const data = fillPattern(100, 0xAB);
    FS.writeFile(`${MOUNT}/mixed/data.bin`, data);
    syncfs(FS, tomefs);

    // Both a directory metadata change and a file data change
    FS.chmod(`${MOUNT}/mixed`, 0o755);
    FS.writeFile(`${MOUNT}/mixed/data.bin`, fillPattern(200, 0xCD));
    syncfs(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    expect(FS2.stat(`${MOUNT}/mixed`).mode & 0o777).toBe(0o755);
    const restored = FS2.readFile(`${MOUNT}/mixed/data.bin`);
    expect(restored.length).toBe(200);
    for (let i = 0; i < 200; i++) {
      if (restored[i] !== ((0xCD + i * 31) & 0xff)) {
        throw new Error(`Byte mismatch at ${i}`);
      }
    }
  });

  it("repeated directory chmod across sync cycles persists last value", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/evolving`);
    syncfs(FS, tomefs);

    FS.chmod(`${MOUNT}/evolving`, 0o700);
    syncfs(FS, tomefs);

    FS.chmod(`${MOUNT}/evolving`, 0o755);
    syncfs(FS, tomefs);

    FS.chmod(`${MOUNT}/evolving`, 0o711);
    syncfs(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    expect(FS2.stat(`${MOUNT}/evolving`).mode & 0o777).toBe(0o711);
  });

  it("directory utime then file creation in subdir both persist", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/parent`);
    const atime = 3000000;
    const mtime = 4000000;
    FS.utime(`${MOUNT}/parent`, atime, mtime);
    syncfs(FS, tomefs);

    // Creating a file in the subdir updates parent mtime/ctime, but
    // the parent was already synced with explicit timestamps. The file
    // creation re-dirties the parent. After this second sync, the parent
    // has the creation-time timestamps (not the explicit utime ones).
    FS.writeFile(`${MOUNT}/parent/child.txt`, new Uint8Array([1, 2, 3]));
    syncfs(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    // Parent should exist with its current timestamps
    const stat = FS2.stat(`${MOUNT}/parent`);
    expect(stat).toBeDefined();
    // Child should exist with correct data
    const child = FS2.readFile(`${MOUNT}/parent/child.txt`);
    expect(child).toEqual(new Uint8Array([1, 2, 3]));
  });

  it("directory metadata survives mount-sync-remount cycle 3 times @fast", async () => {
    const backend = new SyncMemoryBackend();

    // Cycle 1: create directory structure
    let h = await mountTome(backend);
    h.FS.mkdir(`${MOUNT}/persistent`);
    h.FS.mkdir(`${MOUNT}/persistent/sub`);
    h.FS.chmod(`${MOUNT}/persistent`, 0o750);
    syncfs(h.FS, h.tomefs);

    // Cycle 2: remount, modify sub, sync
    h = await mountTome(backend);
    expect(h.FS.stat(`${MOUNT}/persistent`).mode & 0o777).toBe(0o750);
    h.FS.chmod(`${MOUNT}/persistent/sub`, 0o700);
    syncfs(h.FS, h.tomefs);

    // Cycle 3: remount, verify both
    h = await mountTome(backend);
    expect(h.FS.stat(`${MOUNT}/persistent`).mode & 0o777).toBe(0o750);
    expect(h.FS.stat(`${MOUNT}/persistent/sub`).mode & 0o777).toBe(0o700);
  });

  it("rmdir + recreate dir at same path gets new metadata", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/reuse`);
    FS.chmod(`${MOUNT}/reuse`, 0o700);
    syncfs(FS, tomefs);

    FS.rmdir(`${MOUNT}/reuse`);
    FS.mkdir(`${MOUNT}/reuse`);
    // New directory should have default mode, not 0o700
    syncfs(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/reuse`);
    expect(stat.mode & 0o777).toBe(0o777);
  });

  it("directory metadata under 4-page cache pressure", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend, 4);

    // Create directories and files that exceed cache capacity
    FS.mkdir(`${MOUNT}/d1`);
    FS.mkdir(`${MOUNT}/d2`);
    FS.writeFile(`${MOUNT}/d1/f1`, fillPattern(PAGE_SIZE * 3, 0x11));
    FS.writeFile(`${MOUNT}/d2/f2`, fillPattern(PAGE_SIZE * 3, 0x22));
    syncfs(FS, tomefs);

    // chmod both directories (metadata-only, no page cache involvement)
    FS.chmod(`${MOUNT}/d1`, 0o755);
    FS.chmod(`${MOUNT}/d2`, 0o700);
    syncfs(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend, 4);
    expect(FS2.stat(`${MOUNT}/d1`).mode & 0o777).toBe(0o755);
    expect(FS2.stat(`${MOUNT}/d2`).mode & 0o777).toBe(0o700);

    // Verify file data survived too (pages were evicted and re-loaded)
    const f1 = FS2.readFile(`${MOUNT}/d1/f1`);
    expect(f1.length).toBe(PAGE_SIZE * 3);
    for (let i = 0; i < f1.length; i++) {
      if (f1[i] !== ((0x11 + i * 31) & 0xff)) {
        throw new Error(`d1/f1 byte mismatch at ${i}`);
      }
    }
  });

  it("symlink in directory with chmod'd parent persists both @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/links`);
    FS.writeFile(`${MOUNT}/target`, new Uint8Array([42]));
    FS.symlink(`${MOUNT}/target`, `${MOUNT}/links/sym`);
    FS.chmod(`${MOUNT}/links`, 0o750);
    syncfs(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    expect(FS2.stat(`${MOUNT}/links`).mode & 0o777).toBe(0o750);
    expect(FS2.readlink(`${MOUNT}/links/sym`)).toBe(`${MOUNT}/target`);
    const data = FS2.readFile(`${MOUNT}/links/sym`);
    expect(data).toEqual(new Uint8Array([42]));
  });
});
