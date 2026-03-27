/**
 * Adversarial tests: open file descriptors across syncfs persistence cycles.
 *
 * Postgres keeps files open during fsync/checkpoint — it calls syncfs while
 * WAL segments, relation files, and temp files are still held by open fds.
 * This exercises the interaction between:
 *   - Open fd dirty page tracking
 *   - syncfs flushAll() + persistTree() with active streams
 *   - LRU eviction of dirty pages during writes between syncfs cycles
 *   - remount + restoreTree after persisting with open fds
 *
 * The key invariant: syncfs must persist ALL dirty pages (including those
 * reachable only through open fds), and after remount the file contents
 * must match exactly what was written.
 *
 * Ethos §8 (workload scenarios), §9 (adversarial differential testing):
 * "Target the seams: ... dirty flush ordering on concurrent streams"
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

function readFile(FS: any, path: string, size: number): Uint8Array {
  const fd = FS.open(path, O.RDONLY);
  const buf = new Uint8Array(size);
  const n = FS.read(fd, buf, 0, size, 0);
  FS.close(fd);
  return buf.subarray(0, n);
}

describe("adversarial: open fds across syncfs persistence cycles", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  // ------------------------------------------------------------------
  // Basic: write through fd → syncfs → write more → syncfs → remount
  // ------------------------------------------------------------------

  it("multi-cycle writes through open fd survive remount @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const fd = FS.open(`${MOUNT}/wal`, O.RDWR | O.CREAT, 0o666);

    // Write first batch
    const batch1 = encode("batch-one|");
    FS.write(fd, batch1, 0, batch1.length, 0);

    // Sync with fd still open
    syncfs(FS, tomefs);

    // Write second batch (dirty pages after first sync)
    const batch2 = encode("batch-two|");
    FS.write(fd, batch2, 0, batch2.length, batch1.length);

    // Sync again and close
    FS.close(fd);
    syncAndUnmount(FS, tomefs);

    // Remount and verify all data survived
    const { FS: FS2 } = await mountTome(backend);
    const data = readFile(FS2, `${MOUNT}/wal`, 100);
    expect(decode(data)).toBe("batch-one|batch-two|");
  });

  // ------------------------------------------------------------------
  // Multiple open fds to different files during syncfs
  // ------------------------------------------------------------------

  it("multiple open fds to different files all persist through syncfs", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const fds: any[] = [];
    for (let i = 0; i < 5; i++) {
      const fd = FS.open(`${MOUNT}/file${i}`, O.RDWR | O.CREAT, 0o666);
      const data = encode(`content-${i}-initial`);
      FS.write(fd, data, 0, data.length, 0);
      fds.push(fd);
    }

    // Sync with all fds open
    syncfs(FS, tomefs);

    // Write more to each file
    for (let i = 0; i < 5; i++) {
      const more = encode(`+updated`);
      const pos = FS.stat(`${MOUNT}/file${i}`).size;
      FS.write(fds[i], more, 0, more.length, pos);
    }

    // Close all and sync
    for (const fd of fds) FS.close(fd);
    syncAndUnmount(FS, tomefs);

    // Verify after remount
    const { FS: FS2 } = await mountTome(backend);
    for (let i = 0; i < 5; i++) {
      const data = readFile(FS2, `${MOUNT}/file${i}`, 100);
      expect(decode(data)).toBe(`content-${i}-initial+updated`);
    }
  });

  // ------------------------------------------------------------------
  // Write beyond cache capacity with open fd → syncfs
  // ------------------------------------------------------------------

  it("writes exceeding cache capacity flush correctly through syncfs @fast", async () => {
    // 4-page cache = 32 KB. Write 64 KB through an open fd.
    const { FS, tomefs } = await mountTome(backend, 4);

    const fd = FS.open(`${MOUNT}/big`, O.RDWR | O.CREAT, 0o666);

    // Write 8 pages (64 KB) through 4-page cache — forces eviction
    const totalSize = PAGE_SIZE * 8;
    const data = new Uint8Array(totalSize);
    for (let i = 0; i < totalSize; i++) data[i] = (i * 7 + 13) & 0xff;
    FS.write(fd, data, 0, totalSize, 0);

    // Sync with fd still open — must flush all 8 pages
    syncfs(FS, tomefs);

    // Write 4 more pages (will evict the first 4)
    const extra = new Uint8Array(PAGE_SIZE * 4);
    extra.fill(0xab);
    FS.write(fd, extra, 0, extra.length, totalSize);

    FS.close(fd);
    syncAndUnmount(FS, tomefs);

    // Verify full content after remount
    const { FS: FS2 } = await mountTome(backend, 4);
    const fullSize = totalSize + extra.length;
    const result = readFile(FS2, `${MOUNT}/big`, fullSize);
    expect(result.length).toBe(fullSize);

    // First 8 pages: pattern data
    for (let i = 0; i < totalSize; i++) {
      if (result[i] !== ((i * 7 + 13) & 0xff)) {
        throw new Error(
          `Byte ${i}: expected ${(i * 7 + 13) & 0xff}, got ${result[i]}`,
        );
      }
    }
    // Last 4 pages: 0xAB
    for (let i = totalSize; i < fullSize; i++) {
      if (result[i] !== 0xab) {
        throw new Error(`Byte ${i}: expected 0xAB, got ${result[i]}`);
      }
    }
  });

  // ------------------------------------------------------------------
  // Open fd → truncate → syncfs → extend → syncfs → remount
  // ------------------------------------------------------------------

  it("truncate and extend through fd across syncfs cycles", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const fd = FS.open(`${MOUNT}/trunc`, O.RDWR | O.CREAT, 0o666);

    // Write 3 pages of data
    const data = new Uint8Array(PAGE_SIZE * 3);
    data.fill(0xcc);
    FS.write(fd, data, 0, data.length, 0);

    // Truncate to 1.5 pages
    FS.truncate(`${MOUNT}/trunc`, PAGE_SIZE + PAGE_SIZE / 2);

    // Sync with truncated size
    syncfs(FS, tomefs);

    // Extend by writing more at the end
    const more = encode("after-truncate");
    const pos = FS.stat(`${MOUNT}/trunc`).size;
    FS.write(fd, more, 0, more.length, pos);

    FS.close(fd);
    syncAndUnmount(FS, tomefs);

    // Verify after remount
    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/trunc`);
    expect(stat.size).toBe(PAGE_SIZE + PAGE_SIZE / 2 + more.length);

    const result = readFile(FS2, `${MOUNT}/trunc`, stat.size);

    // First PAGE_SIZE bytes: 0xCC (survived truncation)
    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(result[i]).toBe(0xcc);
    }
    // Next PAGE_SIZE/2 bytes: 0xCC (partial page survived)
    for (let i = PAGE_SIZE; i < PAGE_SIZE + PAGE_SIZE / 2; i++) {
      expect(result[i]).toBe(0xcc);
    }
    // Appended text
    const tail = decode(result.subarray(PAGE_SIZE + PAGE_SIZE / 2));
    expect(tail).toBe("after-truncate");
  });

  // ------------------------------------------------------------------
  // Dup fd → close original → syncfs → dup still valid → remount
  // ------------------------------------------------------------------

  it("dup'd fd keeps data alive through syncfs after original closed", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const original = FS.open(`${MOUNT}/dupme`, O.RDWR | O.CREAT, 0o666);
    const data = encode("dup-survives-sync");
    FS.write(original, data, 0, data.length, 0);

    const dup = FS.dupStream(original);

    // Close original, sync with only dup open
    FS.close(original);
    syncfs(FS, tomefs);

    // Write more through dup
    const more = encode("!!!");
    FS.write(dup, more, 0, more.length, data.length);

    FS.close(dup);
    syncAndUnmount(FS, tomefs);

    // Verify after remount
    const { FS: FS2 } = await mountTome(backend);
    const result = readFile(FS2, `${MOUNT}/dupme`, 100);
    expect(decode(result)).toBe("dup-survives-sync!!!");
  });

  // ------------------------------------------------------------------
  // Open fd → rename file → syncfs → remount → verify at new path
  // ------------------------------------------------------------------

  it("renamed file with open fd persists correctly through syncfs", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const fd = FS.open(`${MOUNT}/before`, O.RDWR | O.CREAT, 0o666);
    const data = encode("will-be-renamed");
    FS.write(fd, data, 0, data.length, 0);

    FS.rename(`${MOUNT}/before`, `${MOUNT}/after`);

    // Sync with fd still open (pointing to /after)
    syncfs(FS, tomefs);

    // Write more through the fd
    const more = encode("+post-sync");
    FS.write(fd, more, 0, more.length, data.length);

    FS.close(fd);
    syncAndUnmount(FS, tomefs);

    // Verify after remount
    const { FS: FS2 } = await mountTome(backend);
    const result = readFile(FS2, `${MOUNT}/after`, 100);
    expect(decode(result)).toBe("will-be-renamed+post-sync");

    // Old path should not exist
    expect(() => FS2.stat(`${MOUNT}/before`)).toThrow();
  });

  // ------------------------------------------------------------------
  // WAL rotation pattern across syncfs cycles
  // ------------------------------------------------------------------

  it("WAL rotation: open → append → syncfs → rename → new file → syncfs → remount", async () => {
    const { FS, tomefs } = await mountTome(backend);
    FS.mkdir(`${MOUNT}/pg_wal`);

    // Segment 0: write WAL records
    let walFd = FS.open(`${MOUNT}/pg_wal/current`, O.RDWR | O.CREAT, 0o666);
    const rec0 = encode("WAL-RECORD-SEG0");
    FS.write(walFd, rec0, 0, rec0.length, 0);

    // Checkpoint: syncfs with wal fd open
    syncfs(FS, tomefs);

    // Rotate: rename current → archive, create new current
    FS.close(walFd);
    FS.rename(`${MOUNT}/pg_wal/current`, `${MOUNT}/pg_wal/000000`);

    walFd = FS.open(`${MOUNT}/pg_wal/current`, O.RDWR | O.CREAT, 0o666);
    const rec1 = encode("WAL-RECORD-SEG1");
    FS.write(walFd, rec1, 0, rec1.length, 0);

    // Second checkpoint
    syncfs(FS, tomefs);
    FS.close(walFd);

    // Rotate again
    FS.rename(`${MOUNT}/pg_wal/current`, `${MOUNT}/pg_wal/000001`);
    syncAndUnmount(FS, tomefs);

    // Verify after remount
    const { FS: FS2 } = await mountTome(backend);
    const seg0 = readFile(FS2, `${MOUNT}/pg_wal/000000`, 100);
    expect(decode(seg0)).toBe("WAL-RECORD-SEG0");

    const seg1 = readFile(FS2, `${MOUNT}/pg_wal/000001`, 100);
    expect(decode(seg1)).toBe("WAL-RECORD-SEG1");

    expect(() => FS2.stat(`${MOUNT}/pg_wal/current`)).toThrow();
  });

  // ------------------------------------------------------------------
  // Interleaved writes to multiple files under extreme cache pressure
  // ------------------------------------------------------------------

  it("interleaved multi-file writes under 4-page cache pressure + syncfs", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    // Open 4 files, each will get 3 pages of data (12 pages through 4-page cache)
    const fds: any[] = [];
    const expected: Uint8Array[] = [];
    for (let i = 0; i < 4; i++) {
      fds.push(FS.open(`${MOUNT}/f${i}`, O.RDWR | O.CREAT, 0o666));
      expected.push(new Uint8Array(PAGE_SIZE * 3));
    }

    // Round-robin writes: write one page to each file in rotation
    for (let page = 0; page < 3; page++) {
      for (let i = 0; i < 4; i++) {
        const pageData = new Uint8Array(PAGE_SIZE);
        pageData.fill((i * 16 + page) & 0xff);
        FS.write(fds[i], pageData, 0, PAGE_SIZE, page * PAGE_SIZE);
        expected[i].set(pageData, page * PAGE_SIZE);
      }
    }

    // Sync with all fds still open
    syncfs(FS, tomefs);

    // Write one more page to each file (triggers more eviction)
    for (let i = 0; i < 4; i++) {
      const extra = new Uint8Array(PAGE_SIZE);
      extra.fill(0xfe - i);
      FS.write(fds[i], extra, 0, PAGE_SIZE, PAGE_SIZE * 3);
      // Extend expected
      const newExpected = new Uint8Array(PAGE_SIZE * 4);
      newExpected.set(expected[i]);
      newExpected.set(extra, PAGE_SIZE * 3);
      expected[i] = newExpected;
    }

    for (const fd of fds) FS.close(fd);
    syncAndUnmount(FS, tomefs);

    // Verify after remount under same cache pressure
    const { FS: FS2 } = await mountTome(backend, 4);
    for (let i = 0; i < 4; i++) {
      const result = readFile(FS2, `${MOUNT}/f${i}`, PAGE_SIZE * 4);
      expect(result.length).toBe(PAGE_SIZE * 4);
      for (let j = 0; j < result.length; j++) {
        if (result[j] !== expected[i][j]) {
          throw new Error(
            `File f${i} byte ${j}: expected ${expected[i][j]}, got ${result[j]}`,
          );
        }
      }
    }
  });

  // ------------------------------------------------------------------
  // syncfs idempotency: multiple syncs with open fds produce same result
  // ------------------------------------------------------------------

  it("repeated syncfs with open fd is idempotent", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const fd = FS.open(`${MOUNT}/idem`, O.RDWR | O.CREAT, 0o666);
    const data = encode("idempotent-check");
    FS.write(fd, data, 0, data.length, 0);

    // Sync three times without writing anything new
    syncfs(FS, tomefs);
    syncfs(FS, tomefs);
    syncfs(FS, tomefs);

    FS.close(fd);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const result = readFile(FS2, `${MOUNT}/idem`, 100);
    expect(decode(result)).toBe("idempotent-check");
  });

  // ------------------------------------------------------------------
  // Open fd to unlinked file does not appear after syncfs + remount
  // ------------------------------------------------------------------

  it("unlinked file with open fd does not persist through syncfs", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const fd = FS.open(`${MOUNT}/ghost`, O.RDWR | O.CREAT, 0o666);
    const data = encode("should-not-persist");
    FS.write(fd, data, 0, data.length, 0);

    // Unlink while fd is still open
    FS.unlink(`${MOUNT}/ghost`);

    // Sync — the unlinked file's pages should NOT be persisted
    syncfs(FS, tomefs);

    // Fd still works
    const buf = new Uint8Array(100);
    FS.llseek(fd, 0, 0);
    const n = FS.read(fd, buf, 0, 100);
    expect(decode(buf, n)).toBe("should-not-persist");

    FS.close(fd);
    syncAndUnmount(FS, tomefs);

    // After remount, file should not exist
    const { FS: FS2 } = await mountTome(backend);
    expect(() => FS2.stat(`${MOUNT}/ghost`)).toThrow();
  });

  // ------------------------------------------------------------------
  // Overwrite file while old version has open fd → syncfs
  // ------------------------------------------------------------------

  it("overwritten file: new content persists, old fd data does not", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Write original file
    const oldFd = FS.open(`${MOUNT}/overwrite`, O.RDWR | O.CREAT, 0o666);
    const oldData = encode("old-content");
    FS.write(oldFd, oldData, 0, oldData.length, 0);

    // syncfs to persist the first version
    syncfs(FS, tomefs);

    // Overwrite with new content (truncate + write)
    const newFd = FS.open(
      `${MOUNT}/overwrite`,
      O.RDWR | O.CREAT | O.TRUNC,
      0o666,
    );
    const newData = encode("new-content-longer-than-old");
    FS.write(newFd, newData, 0, newData.length, 0);
    FS.close(newFd);

    // Old fd still reads old data (Emscripten shares the node, so
    // it actually reads the new data — TRUNC modified the same node)
    // This is Emscripten's behavior, not POSIX. Just verify syncfs works.
    FS.close(oldFd);
    syncAndUnmount(FS, tomefs);

    // After remount, file should have the new content
    const { FS: FS2 } = await mountTome(backend);
    const result = readFile(FS2, `${MOUNT}/overwrite`, 100);
    expect(decode(result)).toBe("new-content-longer-than-old");
  });

  // ------------------------------------------------------------------
  // Symlink + open fd to target → syncfs → remount
  // ------------------------------------------------------------------

  it("symlink and target file both persist when target has open fd", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const fd = FS.open(`${MOUNT}/real`, O.RDWR | O.CREAT, 0o666);
    const data = encode("through-the-link");
    FS.write(fd, data, 0, data.length, 0);

    FS.symlink(`${MOUNT}/real`, `${MOUNT}/link`);

    // Sync with fd to target still open
    syncfs(FS, tomefs);

    // Write more through the fd
    const more = encode("+more");
    FS.write(fd, more, 0, more.length, data.length);

    FS.close(fd);
    syncAndUnmount(FS, tomefs);

    // Verify after remount
    const { FS: FS2 } = await mountTome(backend);
    // Read through symlink
    const linkTarget = FS2.readlink(`${MOUNT}/link`);
    expect(linkTarget).toBe(`${MOUNT}/real`);

    const result = readFile(FS2, `${MOUNT}/real`, 100);
    expect(decode(result)).toBe("through-the-link+more");
  });

  // ------------------------------------------------------------------
  // Rapid open→write→syncfs→close→reopen cycle under cache pressure
  // ------------------------------------------------------------------

  it("rapid fd lifecycle with syncfs between open and close under pressure", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    // 10 cycles: each cycle opens a file, writes, syncs, closes
    for (let cycle = 0; cycle < 10; cycle++) {
      const fd = FS.open(`${MOUNT}/cycle`, O.RDWR | O.CREAT | O.TRUNC, 0o666);
      const data = new Uint8Array(PAGE_SIZE * 2);
      data.fill(cycle & 0xff);
      FS.write(fd, data, 0, data.length, 0);

      // Sync while fd is open
      syncfs(FS, tomefs);
      FS.close(fd);
    }

    syncAndUnmount(FS, tomefs);

    // After remount, file should have the last cycle's data
    const { FS: FS2 } = await mountTome(backend, 4);
    const result = readFile(FS2, `${MOUNT}/cycle`, PAGE_SIZE * 2);
    expect(result.length).toBe(PAGE_SIZE * 2);
    for (let i = 0; i < result.length; i++) {
      expect(result[i]).toBe(9); // last cycle = 9
    }
  });

  // ------------------------------------------------------------------
  // Checkpoint pattern: open fd → write → syncfs → seek back → overwrite → syncfs
  // ------------------------------------------------------------------

  it("checkpoint-style overwrite: write → sync → seek → rewrite → sync", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    const fd = FS.open(`${MOUNT}/checkpoint`, O.RDWR | O.CREAT, 0o666);

    // Initial write: 2 pages of 0xAA
    const initial = new Uint8Array(PAGE_SIZE * 2);
    initial.fill(0xaa);
    FS.write(fd, initial, 0, initial.length, 0);

    // First checkpoint
    syncfs(FS, tomefs);

    // Seek back and overwrite first page with 0xBB
    const overwrite = new Uint8Array(PAGE_SIZE);
    overwrite.fill(0xbb);
    FS.write(fd, overwrite, 0, PAGE_SIZE, 0);

    // Second checkpoint
    syncfs(FS, tomefs);

    FS.close(fd);
    syncAndUnmount(FS, tomefs);

    // Verify: page 0 = 0xBB, page 1 = 0xAA
    const { FS: FS2 } = await mountTome(backend, 4);
    const result = readFile(FS2, `${MOUNT}/checkpoint`, PAGE_SIZE * 2);
    expect(result.length).toBe(PAGE_SIZE * 2);
    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(result[i]).toBe(0xbb);
    }
    for (let i = PAGE_SIZE; i < PAGE_SIZE * 2; i++) {
      expect(result[i]).toBe(0xaa);
    }
  });

  // ------------------------------------------------------------------
  // Mixed operations: create dir, write files, rename, truncate, syncfs
  // ------------------------------------------------------------------

  it("mixed dir/file/rename/truncate operations with open fds across syncfs", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    FS.mkdir(`${MOUNT}/db`);
    FS.mkdir(`${MOUNT}/db/base`);

    // Create relation file
    const relFd = FS.open(`${MOUNT}/db/base/16384`, O.RDWR | O.CREAT, 0o666);
    const relData = new Uint8Array(PAGE_SIZE * 3);
    for (let i = 0; i < relData.length; i++) relData[i] = (i * 37) & 0xff;
    FS.write(relFd, relData, 0, relData.length, 0);

    // Create temp file
    const tmpFd = FS.open(`${MOUNT}/db/base/t_16384`, O.RDWR | O.CREAT, 0o666);
    const tmpData = encode("temp-table-data");
    FS.write(tmpFd, tmpData, 0, tmpData.length, 0);

    // Sync with both fds open
    syncfs(FS, tomefs);

    // Truncate relation to 1 page (simulating VACUUM)
    FS.truncate(`${MOUNT}/db/base/16384`, PAGE_SIZE);

    // Promote temp: rename to permanent
    FS.close(tmpFd);
    FS.rename(`${MOUNT}/db/base/t_16384`, `${MOUNT}/db/base/16385`);

    // Write more to truncated relation
    const moreRel = new Uint8Array(PAGE_SIZE);
    moreRel.fill(0xee);
    FS.write(relFd, moreRel, 0, PAGE_SIZE, PAGE_SIZE);

    FS.close(relFd);
    syncAndUnmount(FS, tomefs);

    // Verify after remount
    const { FS: FS2 } = await mountTome(backend, 4);

    // Relation: page 0 = original pattern, page 1 = 0xEE
    const rel = readFile(FS2, `${MOUNT}/db/base/16384`, PAGE_SIZE * 2);
    expect(rel.length).toBe(PAGE_SIZE * 2);
    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(rel[i]).toBe((i * 37) & 0xff);
    }
    for (let i = PAGE_SIZE; i < PAGE_SIZE * 2; i++) {
      expect(rel[i]).toBe(0xee);
    }

    // Promoted temp file
    const promoted = readFile(FS2, `${MOUNT}/db/base/16385`, 100);
    expect(decode(promoted)).toBe("temp-table-data");

    // Old temp path should not exist
    expect(() => FS2.stat(`${MOUNT}/db/base/t_16384`)).toThrow();

    // Directories survived
    expect(FS2.stat(`${MOUNT}/db`)).toBeTruthy();
    expect(FS2.stat(`${MOUNT}/db/base`)).toBeTruthy();
  });
});
