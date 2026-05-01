/**
 * Adversarial tests: allocate() timestamp updates (POSIX compliance).
 *
 * POSIX posix_fallocate(3p) specifies: "If the size of the file is changed
 * by posix_fallocate(), the last file modification and last file status
 * change timestamps of the file shall be updated accordingly."
 *
 * Before the fix, allocate() extended file size and marked metadata dirty
 * but never updated mtime/ctime. This caused stale timestamps on files
 * extended via fallocate — a real issue for PostgreSQL, which uses
 * posix_fallocate for WAL segment pre-allocation and checks modification
 * times for checkpoint/archival logic.
 *
 * Ethos §2 (real POSIX semantics), §9 (adversarial differential testing).
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

describe("adversarial: allocate timestamp updates", () => {
  it("allocate updates mtime and ctime when file size changes @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);
    const statBefore = FS.fstat(stream.fd);
    const mtimeBefore = statBefore.mtime.getTime();
    const ctimeBefore = statBefore.ctime.getTime();

    // Small delay to ensure timestamp difference is measurable
    await new Promise((r) => setTimeout(r, 10));

    // Extend file via allocate
    stream.stream_ops.allocate(stream, 0, 3 * PAGE_SIZE);

    const statAfter = FS.fstat(stream.fd);
    expect(statAfter.size).toBe(3 * PAGE_SIZE);
    expect(statAfter.mtime.getTime()).toBeGreaterThan(mtimeBefore);
    expect(statAfter.ctime.getTime()).toBeGreaterThan(ctimeBefore);

    FS.close(stream);
  });

  it("allocate does not update timestamps when size unchanged @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS } = await mountTome(backend);

    // Create file with some data
    const stream = FS.open(`${MOUNT}/file`, O.RDWR | O.CREAT, 0o666);
    const data = new Uint8Array(PAGE_SIZE * 2);
    FS.write(stream, data, 0, data.length, 0);

    await new Promise((r) => setTimeout(r, 10));

    const statBefore = FS.fstat(stream.fd);
    const mtimeBefore = statBefore.mtime.getTime();

    // Allocate within current size — no-op
    stream.stream_ops.allocate(stream, 0, PAGE_SIZE);

    const statAfter = FS.fstat(stream.fd);
    expect(statAfter.size).toBe(PAGE_SIZE * 2); // unchanged
    expect(statAfter.mtime.getTime()).toBe(mtimeBefore); // unchanged

    FS.close(stream);
  });

  it("allocate timestamps persist across syncfs + remount @fast", async () => {
    const backend = new SyncMemoryBackend();
    let allocateTime: number;

    // Mount, allocate, capture timestamp, syncfs
    {
      const { FS, tomefs } = await mountTome(backend);
      const stream = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);

      await new Promise((r) => setTimeout(r, 10));

      stream.stream_ops.allocate(stream, 0, 2 * PAGE_SIZE);
      allocateTime = FS.fstat(stream.fd).mtime.getTime();
      FS.close(stream);
      syncfs(FS, tomefs);
    }

    // Remount and verify timestamps survived
    {
      const { FS } = await mountTome(backend);
      const stat = FS.stat(`${MOUNT}/wal`);
      expect(stat.mtime.getTime()).toBe(allocateTime);
    }
  });

  it("allocate then write: write timestamp overwrites allocate timestamp @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);

    // Allocate to pre-extend
    stream.stream_ops.allocate(stream, 0, 3 * PAGE_SIZE);
    const allocateTime = FS.fstat(stream.fd).mtime.getTime();

    await new Promise((r) => setTimeout(r, 10));

    // Write some data — should update timestamp again
    const data = new Uint8Array(100);
    data.fill(0x42);
    FS.write(stream, data, 0, 100, 0);

    const statAfter = FS.fstat(stream.fd);
    expect(statAfter.mtime.getTime()).toBeGreaterThan(allocateTime);

    FS.close(stream);
  });

  it("multiple allocate calls update timestamps each time @fast", async () => {
    const backend = new SyncMemoryBackend();
    const { FS } = await mountTome(backend);

    const stream = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);

    stream.stream_ops.allocate(stream, 0, PAGE_SIZE);
    const time1 = FS.fstat(stream.fd).mtime.getTime();

    await new Promise((r) => setTimeout(r, 10));

    stream.stream_ops.allocate(stream, 0, 2 * PAGE_SIZE);
    const time2 = FS.fstat(stream.fd).mtime.getTime();
    expect(time2).toBeGreaterThan(time1);

    await new Promise((r) => setTimeout(r, 10));

    stream.stream_ops.allocate(stream, 0, 4 * PAGE_SIZE);
    const time3 = FS.fstat(stream.fd).mtime.getTime();
    expect(time3).toBeGreaterThan(time2);

    FS.close(stream);
  });
});
