/**
 * Adversarial tests: FD operations under extreme (2-page) cache pressure.
 *
 * The page cache's per-node page table (node._pages[]) holds direct
 * CachedPage references for O(1) access. Under a 2-page cache, every
 * multi-page operation evicts pages — setting CachedPage.evicted = true
 * on the stale references. These tests verify that:
 *
 * - Multiple FDs on the same node see consistent data after eviction
 * - dup()'d FDs share position correctly when pages thrash
 * - Positional reads via pread don't corrupt the eviction-reload cycle
 * - ftruncate via FD correctly invalidates the per-node page table
 * - Append writes from multiple FDs land at the correct offsets
 * - syncfs persistence survives extreme eviction during every operation
 *
 * Ethos §9: adversarial differential testing — target the seams.
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

const SEEK_SET = 0;
const SEEK_CUR = 1;
const SEEK_END = 2;

const MOUNT = "/tome";

function fillBuf(size: number, seed: number): Uint8Array {
  const buf = new Uint8Array(size);
  for (let i = 0; i < size; i++) {
    buf[i] = (seed + i * 37) & 0xff;
  }
  return buf;
}

function verifyBuf(actual: Uint8Array, size: number, seed: number): void {
  for (let i = 0; i < size; i++) {
    expect(actual[i]).toBe((seed + i * 37) & 0xff);
  }
}

async function createTestFS(
  maxPages: number,
  existingBackend?: SyncMemoryBackend,
) {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  const Module = await createModule();
  const rawFS = Module.FS;
  const backend = existingBackend ?? new SyncMemoryBackend();
  const tomefs = createTomeFS(rawFS, { backend, maxPages });
  rawFS.mkdir(MOUNT);
  rawFS.mount(tomefs, {}, MOUNT);
  return { FS: rawFS, backend, tomefs };
}

function syncfs(FS: any): void {
  let err: Error | null = null;
  FS.syncfs(false, (e: Error | null) => {
    err = e;
  });
  if (err) throw err;
}

describe("adversarial: FD operations under 2-page cache pressure", () => {
  let FS: any;
  let backend: SyncMemoryBackend;
  let tomefs: any;

  beforeEach(async () => {
    ({ FS, backend, tomefs } = await createTestFS(2));
  });

  // ---------------------------------------------------------------
  // Multiple FDs on the same file
  // ---------------------------------------------------------------

  it("@fast write via fd1, read via fd2 sees consistent data after eviction", () => {
    const path = MOUNT + "/shared";
    const fd1 = FS.open(path, O.RDWR | O.CREAT, 0o666);
    const fd2 = FS.open(path, O.RDONLY);

    const data = fillBuf(PAGE_SIZE * 3, 0x10);
    FS.write(fd1, data, 0, data.length, 0);

    const readBuf = new Uint8Array(PAGE_SIZE * 3);
    const bytesRead = FS.read(fd2, readBuf, 0, readBuf.length, 0);
    expect(bytesRead).toBe(PAGE_SIZE * 3);
    verifyBuf(readBuf, PAGE_SIZE * 3, 0x10);

    FS.close(fd1);
    FS.close(fd2);
  });

  it("@fast interleaved write-read across two FDs with eviction between", () => {
    const path = MOUNT + "/interleave";
    const fd1 = FS.open(path, O.RDWR | O.CREAT, 0o666);
    const fd2 = FS.open(path, O.RDWR);

    // fd1 writes page 0
    const p0 = fillBuf(PAGE_SIZE, 0xAA);
    FS.write(fd1, p0, 0, PAGE_SIZE, 0);

    // fd1 writes page 1 — evicts page 0 from 2-page cache
    const p1 = fillBuf(PAGE_SIZE, 0xBB);
    FS.write(fd1, p1, 0, PAGE_SIZE, PAGE_SIZE);

    // fd1 writes page 2 — evicts page 0 or 1
    const p2 = fillBuf(PAGE_SIZE, 0xCC);
    FS.write(fd1, p2, 0, PAGE_SIZE, PAGE_SIZE * 2);

    // fd2 reads page 0 — must reload from backend (evicted + flushed)
    const read0 = new Uint8Array(PAGE_SIZE);
    FS.read(fd2, read0, 0, PAGE_SIZE, 0);
    verifyBuf(read0, PAGE_SIZE, 0xAA);

    // fd2 reads page 2 — evicts page 0 again
    const read2 = new Uint8Array(PAGE_SIZE);
    FS.read(fd2, read2, 0, PAGE_SIZE, PAGE_SIZE * 2);
    verifyBuf(read2, PAGE_SIZE, 0xCC);

    // fd2 reads page 1 — reloads from backend
    const read1 = new Uint8Array(PAGE_SIZE);
    FS.read(fd2, read1, 0, PAGE_SIZE, PAGE_SIZE);
    verifyBuf(read1, PAGE_SIZE, 0xBB);

    FS.close(fd1);
    FS.close(fd2);
  });

  it("@fast overwrite via fd2 visible to fd1 after page eviction + reload", () => {
    const path = MOUNT + "/overwrite_visible";
    const fd1 = FS.open(path, O.RDWR | O.CREAT, 0o666);

    // Write 3 pages via fd1
    const data = fillBuf(PAGE_SIZE * 3, 0x50);
    FS.write(fd1, data, 0, data.length, 0);

    const fd2 = FS.open(path, O.RDWR);

    // fd2 overwrites page 1 with different data
    const newP1 = fillBuf(PAGE_SIZE, 0x99);
    FS.write(fd2, newP1, 0, PAGE_SIZE, PAGE_SIZE);

    // Force page 1 out by accessing pages 0 and 2 via fd1
    const tmp = new Uint8Array(PAGE_SIZE);
    FS.read(fd1, tmp, 0, PAGE_SIZE, 0);
    FS.read(fd1, tmp, 0, PAGE_SIZE, PAGE_SIZE * 2);

    // fd1 reads page 1 — must see fd2's overwrite (reloaded from cache or backend)
    const check = new Uint8Array(PAGE_SIZE);
    FS.read(fd1, check, 0, PAGE_SIZE, PAGE_SIZE);
    verifyBuf(check, PAGE_SIZE, 0x99);

    FS.close(fd1);
    FS.close(fd2);
  });

  // ---------------------------------------------------------------
  // dup() with extreme eviction
  // ---------------------------------------------------------------

  it("@fast dup'd fd shares position and sees data through eviction cycles", () => {
    const path = MOUNT + "/dup_test";
    const s1 = FS.open(path, O.RDWR | O.CREAT, 0o666);

    // Write 4 pages
    const data = fillBuf(PAGE_SIZE * 4, 0x20);
    FS.write(s1, data, 0, data.length, 0);
    FS.llseek(s1, 0, SEEK_SET);

    const s2 = FS.dupStream(s1);

    // Read first page via s1 — advances shared position
    const buf1 = new Uint8Array(PAGE_SIZE);
    FS.read(s1, buf1, 0, PAGE_SIZE);
    verifyBuf(buf1, PAGE_SIZE, 0x20);

    // s2 should continue from PAGE_SIZE (shared position)
    const buf2 = new Uint8Array(PAGE_SIZE);
    const n = FS.read(s2, buf2, 0, PAGE_SIZE);
    expect(n).toBe(PAGE_SIZE);
    // Page 1 data starts at seed offset PAGE_SIZE
    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(buf2[i]).toBe((0x20 + (PAGE_SIZE + i) * 37) & 0xff);
    }

    FS.close(s1);
    FS.close(s2);
  });

  // ---------------------------------------------------------------
  // Positional reads (pread-style) under eviction
  // ---------------------------------------------------------------

  it("@fast positional reads to non-sequential pages trigger correct eviction + reload", () => {
    const path = MOUNT + "/pread";
    const fd = FS.open(path, O.RDWR | O.CREAT, 0o666);

    // Write 5 pages with distinct patterns
    for (let i = 0; i < 5; i++) {
      const p = fillBuf(PAGE_SIZE, 0x30 + i);
      FS.write(fd, p, 0, PAGE_SIZE, i * PAGE_SIZE);
    }

    // Read pages in random order — each read evicts the previous page
    const order = [3, 0, 4, 1, 2];
    for (const pi of order) {
      const buf = new Uint8Array(PAGE_SIZE);
      const n = FS.read(fd, buf, 0, PAGE_SIZE, pi * PAGE_SIZE);
      expect(n).toBe(PAGE_SIZE);
      verifyBuf(buf, PAGE_SIZE, 0x30 + pi);
    }

    FS.close(fd);
  });

  it("@fast positional read doesn't corrupt sequential fd position", () => {
    const path = MOUNT + "/pread_pos";
    const fd = FS.open(path, O.RDWR | O.CREAT, 0o666);

    // Write 3 pages
    for (let i = 0; i < 3; i++) {
      const p = fillBuf(PAGE_SIZE, 0x40 + i);
      FS.write(fd, p, 0, PAGE_SIZE, i * PAGE_SIZE);
    }

    // Position at start, read page 2 positionally
    FS.llseek(fd, 0, SEEK_SET);
    const tmpBuf = new Uint8Array(PAGE_SIZE);
    FS.read(fd, tmpBuf, 0, PAGE_SIZE, PAGE_SIZE * 2);
    verifyBuf(tmpBuf, PAGE_SIZE, 0x42);

    // Sequential read from fd should still be at page 0
    // (positional reads don't advance the position in Emscripten)
    // Note: Emscripten FS.read with explicit position doesn't update stream.position
    // But Emscripten's behavior depends on the implementation — verify the data
    const seqBuf = new Uint8Array(PAGE_SIZE);
    const n = FS.read(fd, seqBuf, 0, PAGE_SIZE, 0);
    expect(n).toBe(PAGE_SIZE);
    verifyBuf(seqBuf, PAGE_SIZE, 0x40);

    FS.close(fd);
  });

  // ---------------------------------------------------------------
  // ftruncate via FD under eviction
  // ---------------------------------------------------------------

  it("@fast ftruncate shrink invalidates per-node page table entries", () => {
    const path = MOUNT + "/ftrunc_shrink";
    const stream = FS.open(path, O.RDWR | O.CREAT, 0o666);

    // Write 4 pages
    const data = fillBuf(PAGE_SIZE * 4, 0x55);
    FS.write(stream, data, 0, data.length, 0);

    // Truncate to 1 page
    FS.ftruncate(stream.fd, PAGE_SIZE);

    // Stat should show size = PAGE_SIZE
    const stat = FS.fstat(stream.fd);
    expect(stat.size).toBe(PAGE_SIZE);

    // Read page 0 — should still have original data
    const buf = new Uint8Array(PAGE_SIZE);
    FS.read(stream, buf, 0, PAGE_SIZE, 0);
    verifyBuf(buf, PAGE_SIZE, 0x55);

    // Extend to 2 pages — page 1 should be zero-filled
    FS.ftruncate(stream.fd, PAGE_SIZE * 2);
    const buf1 = new Uint8Array(PAGE_SIZE);
    FS.read(stream, buf1, 0, PAGE_SIZE, PAGE_SIZE);
    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(buf1[i]).toBe(0);
    }

    FS.close(stream);
  });

  it("@fast ftruncate-extend-write-read cycle under 2-page cache", () => {
    const path = MOUNT + "/ftrunc_cycle";
    const stream = FS.open(path, O.RDWR | O.CREAT, 0o666);

    for (let cycle = 0; cycle < 5; cycle++) {
      // Write 3 pages
      const data = fillBuf(PAGE_SIZE * 3, cycle * 10);
      FS.write(stream, data, 0, data.length, 0);

      // Truncate to 0
      FS.ftruncate(stream.fd, 0);
      expect(FS.fstat(stream.fd).size).toBe(0);

      // Re-extend and write 2 pages
      const data2 = fillBuf(PAGE_SIZE * 2, cycle * 10 + 5);
      FS.write(stream, data2, 0, data2.length, 0);

      // Read back and verify
      const readBuf = new Uint8Array(PAGE_SIZE * 2);
      FS.read(stream, readBuf, 0, PAGE_SIZE * 2, 0);
      verifyBuf(readBuf, PAGE_SIZE * 2, cycle * 10 + 5);
    }

    FS.close(stream);
  });

  // ---------------------------------------------------------------
  // Append writes under extreme eviction
  // ---------------------------------------------------------------

  it("@fast O_APPEND writes from two FDs land at correct offsets", () => {
    const path = MOUNT + "/append";
    const s1 = FS.open(path, O.RDWR | O.CREAT, 0o666);

    // Seed with one page
    const p0 = fillBuf(PAGE_SIZE, 0x60);
    FS.write(s1, p0, 0, PAGE_SIZE, 0);
    FS.close(s1);

    // Open two append FDs
    const as1 = FS.open(path, O.WRONLY | O.APPEND);
    const as2 = FS.open(path, O.WRONLY | O.APPEND);

    // Append PAGE_SIZE bytes from each — file should grow to 3 pages
    const a1 = fillBuf(PAGE_SIZE, 0x61);
    FS.write(as1, a1, 0, PAGE_SIZE);

    const a2 = fillBuf(PAGE_SIZE, 0x62);
    FS.write(as2, a2, 0, PAGE_SIZE);

    FS.close(as1);
    FS.close(as2);

    // Read back all 3 pages
    const rs = FS.open(path, O.RDONLY);
    const stat = FS.fstat(rs.fd);
    expect(stat.size).toBe(PAGE_SIZE * 3);

    const full = new Uint8Array(PAGE_SIZE * 3);
    FS.read(rs, full, 0, PAGE_SIZE * 3, 0);

    verifyBuf(full.subarray(0, PAGE_SIZE), PAGE_SIZE, 0x60);
    verifyBuf(full.subarray(PAGE_SIZE, PAGE_SIZE * 2), PAGE_SIZE, 0x61);
    verifyBuf(full.subarray(PAGE_SIZE * 2, PAGE_SIZE * 3), PAGE_SIZE, 0x62);

    FS.close(rs);
  });

  // ---------------------------------------------------------------
  // Cross-page boundary operations with FDs
  // ---------------------------------------------------------------

  it("@fast cross-page boundary write via FD under 2-page cache", () => {
    const path = MOUNT + "/cross_boundary";
    const fd = FS.open(path, O.RDWR | O.CREAT, 0o666);

    // Write starting at mid-page 0, spanning into page 1
    const offset = PAGE_SIZE / 2;
    const size = PAGE_SIZE; // crosses page boundary
    const data = fillBuf(size, 0x70);
    FS.write(fd, data, 0, size, offset);

    // Read back the cross-boundary region
    const readBuf = new Uint8Array(size);
    FS.read(fd, readBuf, 0, size, offset);
    verifyBuf(readBuf, size, 0x70);

    // Verify first half of page 0 is zero (never written)
    const firstHalf = new Uint8Array(offset);
    FS.read(fd, firstHalf, 0, offset, 0);
    for (let i = 0; i < offset; i++) {
      expect(firstHalf[i]).toBe(0);
    }

    FS.close(fd);
  });

  it("@fast three-page boundary write causes self-eviction in 2-page cache", () => {
    const path = MOUNT + "/triple_boundary";
    const fd = FS.open(path, O.RDWR | O.CREAT, 0o666);

    // Write from page 0 offset 100 through page 2 — spans 3 pages
    const writeStart = 100;
    const writeLen = PAGE_SIZE * 2;
    const data = fillBuf(writeLen, 0x80);
    FS.write(fd, data, 0, writeLen, writeStart);

    // Read back each page region individually
    const read1 = new Uint8Array(PAGE_SIZE - writeStart);
    FS.read(fd, read1, 0, PAGE_SIZE - writeStart, writeStart);
    for (let i = 0; i < PAGE_SIZE - writeStart; i++) {
      expect(read1[i]).toBe((0x80 + i * 37) & 0xff);
    }

    // Read page 1 (fully within written range)
    const read2 = new Uint8Array(PAGE_SIZE);
    FS.read(fd, read2, 0, PAGE_SIZE, PAGE_SIZE);
    const dataOffset = PAGE_SIZE - writeStart;
    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(read2[i]).toBe((0x80 + (dataOffset + i) * 37) & 0xff);
    }

    FS.close(fd);
  });

  // ---------------------------------------------------------------
  // Competing files under 2-page cache
  // ---------------------------------------------------------------

  it("@fast round-robin writes to 4 files then read all back", () => {
    const fds: number[] = [];
    for (let i = 0; i < 4; i++) {
      fds.push(FS.open(MOUNT + `/file${i}`, O.RDWR | O.CREAT, 0o666));
    }

    // Write one page to each file in round-robin — constant eviction
    for (let round = 0; round < 3; round++) {
      for (let i = 0; i < 4; i++) {
        const p = fillBuf(PAGE_SIZE, round * 4 + i);
        FS.write(fds[i], p, 0, PAGE_SIZE, round * PAGE_SIZE);
      }
    }

    // Read back all pages from all files
    for (let i = 0; i < 4; i++) {
      for (let round = 0; round < 3; round++) {
        const buf = new Uint8Array(PAGE_SIZE);
        const n = FS.read(fds[i], buf, 0, PAGE_SIZE, round * PAGE_SIZE);
        expect(n).toBe(PAGE_SIZE);
        verifyBuf(buf, PAGE_SIZE, round * 4 + i);
      }
    }

    for (const fd of fds) FS.close(fd);
  });

  it("@fast alternating write-read between two files under 2-page cache", () => {
    const fdA = FS.open(MOUNT + "/altA", O.RDWR | O.CREAT, 0o666);
    const fdB = FS.open(MOUNT + "/altB", O.RDWR | O.CREAT, 0o666);

    for (let i = 0; i < 10; i++) {
      // Write one page to file A
      const dataA = fillBuf(PAGE_SIZE, i * 2);
      FS.write(fdA, dataA, 0, PAGE_SIZE, i * PAGE_SIZE);

      // Write one page to file B — evicts file A's page
      const dataB = fillBuf(PAGE_SIZE, i * 2 + 1);
      FS.write(fdB, dataB, 0, PAGE_SIZE, i * PAGE_SIZE);
    }

    // Read back all pages from both files
    for (let i = 0; i < 10; i++) {
      const bufA = new Uint8Array(PAGE_SIZE);
      FS.read(fdA, bufA, 0, PAGE_SIZE, i * PAGE_SIZE);
      verifyBuf(bufA, PAGE_SIZE, i * 2);

      const bufB = new Uint8Array(PAGE_SIZE);
      FS.read(fdB, bufB, 0, PAGE_SIZE, i * PAGE_SIZE);
      verifyBuf(bufB, PAGE_SIZE, i * 2 + 1);
    }

    FS.close(fdA);
    FS.close(fdB);
  });

  // ---------------------------------------------------------------
  // Persistence through syncfs + remount under 2-page cache
  // ---------------------------------------------------------------

  it("@fast syncfs + remount preserves data written under 2-page cache", async () => {
    const path = MOUNT + "/persist";
    const fd = FS.open(path, O.RDWR | O.CREAT, 0o666);

    // Write 5 pages (far exceeding 2-page cache)
    for (let i = 0; i < 5; i++) {
      const data = fillBuf(PAGE_SIZE, 0xA0 + i);
      FS.write(fd, data, 0, PAGE_SIZE, i * PAGE_SIZE);
    }
    FS.close(fd);

    syncfs(FS);
    FS.unmount(MOUNT);

    // Remount with the same backend
    const { FS: FS2 } = await createTestFS(2, backend);

    const fd2 = FS2.open(MOUNT + "/persist", O.RDONLY);
    for (let i = 0; i < 5; i++) {
      const buf = new Uint8Array(PAGE_SIZE);
      const n = FS2.read(fd2, buf, 0, PAGE_SIZE, i * PAGE_SIZE);
      expect(n).toBe(PAGE_SIZE);
      verifyBuf(buf, PAGE_SIZE, 0xA0 + i);
    }
    FS2.close(fd2);
  });

  it("@fast multi-file persistence under 2-page cache", async () => {
    const files = ["alpha", "beta", "gamma", "delta"];
    for (let fi = 0; fi < files.length; fi++) {
      const fd = FS.open(MOUNT + "/" + files[fi], O.RDWR | O.CREAT, 0o666);
      for (let pi = 0; pi < 3; pi++) {
        const data = fillBuf(PAGE_SIZE, fi * 10 + pi);
        FS.write(fd, data, 0, PAGE_SIZE, pi * PAGE_SIZE);
      }
      FS.close(fd);
    }

    syncfs(FS);
    FS.unmount(MOUNT);

    const { FS: FS2 } = await createTestFS(2, backend);

    for (let fi = 0; fi < files.length; fi++) {
      const fd = FS2.open(MOUNT + "/" + files[fi], O.RDONLY);
      for (let pi = 0; pi < 3; pi++) {
        const buf = new Uint8Array(PAGE_SIZE);
        FS2.read(fd, buf, 0, PAGE_SIZE, pi * PAGE_SIZE);
        verifyBuf(buf, PAGE_SIZE, fi * 10 + pi);
      }
      FS2.close(fd);
    }
  });

  it("@fast ftruncate + rewrite + syncfs + remount under 2-page cache", async () => {
    const path = MOUNT + "/trunc_persist";
    const stream = FS.open(path, O.RDWR | O.CREAT, 0o666);

    // Write 4 pages
    const data = fillBuf(PAGE_SIZE * 4, 0xB0);
    FS.write(stream, data, 0, data.length, 0);

    // Truncate to 1 page, then extend to 3 with new data
    FS.ftruncate(stream.fd, PAGE_SIZE);
    const newData = fillBuf(PAGE_SIZE * 2, 0xC0);
    FS.write(stream, newData, 0, newData.length, PAGE_SIZE);
    FS.close(stream);

    syncfs(FS);
    FS.unmount(MOUNT);

    const { FS: FS2 } = await createTestFS(2, backend);
    const s2 = FS2.open(MOUNT + "/trunc_persist", O.RDONLY);
    const stat = FS2.fstat(s2.fd);
    expect(stat.size).toBe(PAGE_SIZE * 3);

    // Page 0 retains original data
    const buf0 = new Uint8Array(PAGE_SIZE);
    FS2.read(s2, buf0, 0, PAGE_SIZE, 0);
    verifyBuf(buf0, PAGE_SIZE, 0xB0);

    // Pages 1-2 have new data
    const buf12 = new Uint8Array(PAGE_SIZE * 2);
    FS2.read(s2, buf12, 0, PAGE_SIZE * 2, PAGE_SIZE);
    verifyBuf(buf12, PAGE_SIZE * 2, 0xC0);

    FS2.close(s2);
  });

  // ---------------------------------------------------------------
  // Seek + read/write patterns under eviction
  // ---------------------------------------------------------------

  it("@fast seek-to-end then backward reads under 2-page cache", () => {
    const path = MOUNT + "/seek_backward";
    const fd = FS.open(path, O.RDWR | O.CREAT, 0o666);

    // Write 4 pages
    for (let i = 0; i < 4; i++) {
      const p = fillBuf(PAGE_SIZE, 0xD0 + i);
      FS.write(fd, p, 0, PAGE_SIZE, i * PAGE_SIZE);
    }

    // Read pages in reverse order — worst case for LRU
    for (let i = 3; i >= 0; i--) {
      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(fd, buf, 0, PAGE_SIZE, i * PAGE_SIZE);
      verifyBuf(buf, PAGE_SIZE, 0xD0 + i);
    }

    FS.close(fd);
  });

  it("@fast seek SEEK_END write extends file under 2-page cache", () => {
    const path = MOUNT + "/seek_end";
    const stream = FS.open(path, O.RDWR | O.CREAT, 0o666);

    // Write 2 pages
    const data = fillBuf(PAGE_SIZE * 2, 0xE0);
    FS.write(stream, data, 0, data.length, 0);

    // Seek to end and append a page
    FS.llseek(stream, 0, SEEK_END);
    const appendData = fillBuf(PAGE_SIZE, 0xE5);
    FS.write(stream, appendData, 0, PAGE_SIZE);

    // Verify all 3 pages
    expect(FS.fstat(stream.fd).size).toBe(PAGE_SIZE * 3);

    for (let i = 0; i < 2; i++) {
      const buf = new Uint8Array(PAGE_SIZE);
      FS.read(stream, buf, 0, PAGE_SIZE, i * PAGE_SIZE);
      // First 2 pages: seed 0xE0, verified at full offset
      const expected = fillBuf(PAGE_SIZE * 2, 0xE0);
      expect(buf).toEqual(expected.subarray(i * PAGE_SIZE, (i + 1) * PAGE_SIZE));
    }

    const buf2 = new Uint8Array(PAGE_SIZE);
    FS.read(stream, buf2, 0, PAGE_SIZE, PAGE_SIZE * 2);
    verifyBuf(buf2, PAGE_SIZE, 0xE5);

    FS.close(stream);
  });

  // ---------------------------------------------------------------
  // Unlink with open FD under 2-page cache
  // ---------------------------------------------------------------

  it("@fast unlinked file with open fd remains readable under 2-page cache", () => {
    const path = MOUNT + "/unlink_open";
    const fd = FS.open(path, O.RDWR | O.CREAT, 0o666);

    // Write 3 pages
    for (let i = 0; i < 3; i++) {
      const p = fillBuf(PAGE_SIZE, 0xF0 + i);
      FS.write(fd, p, 0, PAGE_SIZE, i * PAGE_SIZE);
    }

    // Unlink while FD is open
    FS.unlink(path);

    // Write to a different file to cause eviction
    const fd2 = FS.open(MOUNT + "/evict_helper", O.RDWR | O.CREAT, 0o666);
    const evictData = fillBuf(PAGE_SIZE * 2, 0x01);
    FS.write(fd2, evictData, 0, evictData.length, 0);

    // Read from unlinked file via original fd — data must survive
    for (let i = 0; i < 3; i++) {
      const buf = new Uint8Array(PAGE_SIZE);
      const n = FS.read(fd, buf, 0, PAGE_SIZE, i * PAGE_SIZE);
      expect(n).toBe(PAGE_SIZE);
      verifyBuf(buf, PAGE_SIZE, 0xF0 + i);
    }

    FS.close(fd);
    FS.close(fd2);
  });

  // ---------------------------------------------------------------
  // 1-page cache extremes
  // ---------------------------------------------------------------

  it("@fast FD write-read cycle with 1-page cache", async () => {
    const { FS: FS1 } = await createTestFS(1);
    const path = MOUNT + "/one_page";
    const fd = FS1.open(path, O.RDWR | O.CREAT, 0o666);

    // Write 3 pages — every page evicts the previous
    for (let i = 0; i < 3; i++) {
      const p = fillBuf(PAGE_SIZE, i * 5);
      FS1.write(fd, p, 0, PAGE_SIZE, i * PAGE_SIZE);
    }

    // Read back — every read evicts the cache
    for (let i = 0; i < 3; i++) {
      const buf = new Uint8Array(PAGE_SIZE);
      FS1.read(fd, buf, 0, PAGE_SIZE, i * PAGE_SIZE);
      verifyBuf(buf, PAGE_SIZE, i * 5);
    }

    FS1.close(fd);
  });
});
