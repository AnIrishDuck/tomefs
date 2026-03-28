/**
 * Adversarial tests: lseek edge cases and page cache interactions.
 *
 * Targets the interaction between file position tracking (llseek) and
 * page cache state. Seeks don't touch the page cache directly, but
 * they determine WHICH pages are accessed on the next read/write.
 * Edge cases in position arithmetic can cause reads from wrong pages,
 * writes to wrong offsets, or stale data after cache eviction.
 *
 * These tests run with a tiny cache (4 pages = 32 KB) to force eviction
 * on every multi-page operation, exposing bugs that only manifest when
 * the page being seeked to has been evicted.
 *
 * Ethos §9 (adversarial differential testing).
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
const MAX_PAGES = 4; // 32 KB cache — extreme pressure

/** Fill a buffer with a deterministic pattern based on a seed byte. */
function fillPattern(size: number, seed: number): Uint8Array {
  const buf = new Uint8Array(size);
  for (let i = 0; i < size; i++) {
    buf[i] = (seed + i * 31) & 0xff;
  }
  return buf;
}

/** Verify a buffer matches the expected pattern. */
function verifyPattern(buf: Uint8Array, size: number, seed: number): boolean {
  for (let i = 0; i < size; i++) {
    if (buf[i] !== ((seed + i * 31) & 0xff)) return false;
  }
  return true;
}

async function mountTome(backend: SyncMemoryBackend, maxPages = MAX_PAGES) {
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

describe("adversarial: seek + page cache interactions", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("backward seek after eviction reloads correct page @fast", async () => {
    // Write a file spanning 6 pages (cache holds 4). After writing,
    // page 0 is evicted. Seeking back to page 0 and reading must
    // reload it from the backend with correct data.
    const { FS } = await mountTome(backend);

    const totalSize = PAGE_SIZE * 6;
    const data = fillPattern(totalSize, 11);
    const fd = FS.open(`${MOUNT}/big`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, data, 0, totalSize);

    // Seek back to start — page 0 was evicted during write
    FS.llseek(fd, 0, 0); // SEEK_SET

    const buf = new Uint8Array(PAGE_SIZE);
    const bytesRead = FS.read(fd, buf, 0, PAGE_SIZE);
    expect(bytesRead).toBe(PAGE_SIZE);
    expect(verifyPattern(buf, PAGE_SIZE, 11)).toBe(true);

    FS.close(fd);
  });

  it("SEEK_END tracks file size correctly after writes @fast", async () => {
    const { FS } = await mountTome(backend);

    const fd = FS.open(`${MOUNT}/growing`, O.RDWR | O.CREAT, 0o666);

    // Write 100 bytes
    const chunk1 = fillPattern(100, 1);
    FS.write(fd, chunk1, 0, 100);

    // SEEK_END should be at 100
    const pos1 = FS.llseek(fd, 0, 2); // SEEK_END + 0
    expect(pos1).toBe(100);

    // Write more — position advances from 100
    const chunk2 = fillPattern(200, 2);
    FS.write(fd, chunk2, 0, 200);

    // SEEK_END should now be at 300
    const pos2 = FS.llseek(fd, 0, 2);
    expect(pos2).toBe(300);

    FS.close(fd);
  });

  it("SEEK_END with negative offset reads from before EOF", async () => {
    const { FS } = await mountTome(backend);

    const data = fillPattern(PAGE_SIZE * 2, 22);
    const fd = FS.open(`${MOUNT}/file`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, data, 0, data.length);

    // Seek to 100 bytes before end
    const pos = FS.llseek(fd, -100, 2); // SEEK_END - 100
    expect(pos).toBe(PAGE_SIZE * 2 - 100);

    // Read those last 100 bytes
    const buf = new Uint8Array(100);
    const bytesRead = FS.read(fd, buf, 0, 100);
    expect(bytesRead).toBe(100);

    // Verify content matches the tail of the original pattern
    const offset = PAGE_SIZE * 2 - 100;
    for (let i = 0; i < 100; i++) {
      expect(buf[i]).toBe((22 + (offset + i) * 31) & 0xff);
    }

    FS.close(fd);
  });

  it("SEEK_CUR backward across page boundary under pressure", async () => {
    // Write 3 pages, read forward to page 2 (evicting page 0),
    // then seek backward across the page 1/0 boundary.
    const { FS } = await mountTome(backend);

    const data = fillPattern(PAGE_SIZE * 3, 33);
    const fd = FS.open(`${MOUNT}/cross`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, data, 0, data.length);

    // Seek to start and read into page 2 to populate cache
    FS.llseek(fd, PAGE_SIZE * 2, 0); // SEEK_SET to page 2
    const tmp = new Uint8Array(100);
    FS.read(fd, tmp, 0, 100);

    // Now seek backward across page boundary: from page 2 + 100 to page 0
    const seekBack = -(PAGE_SIZE * 2 + 100);
    const pos = FS.llseek(fd, seekBack, 1); // SEEK_CUR
    expect(pos).toBe(0);

    // Read page 0 — must be reloaded from backend
    const buf = new Uint8Array(PAGE_SIZE);
    FS.read(fd, buf, 0, PAGE_SIZE);
    expect(verifyPattern(buf, PAGE_SIZE, 33)).toBe(true);

    FS.close(fd);
  });

  it("seek past EOF then read returns 0 bytes without extending file", async () => {
    const { FS } = await mountTome(backend);

    const data = fillPattern(500, 44);
    const fd = FS.open(`${MOUNT}/small`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, data, 0, 500);

    // Seek well past end of file
    FS.llseek(fd, PAGE_SIZE * 10, 0); // SEEK_SET to 80 KB

    // Read should return 0 bytes — position is past EOF
    const buf = new Uint8Array(100);
    const bytesRead = FS.read(fd, buf, 0, 100);
    expect(bytesRead).toBe(0);

    // File size should NOT have changed
    const stat = FS.stat(`${MOUNT}/small`);
    expect(stat.size).toBe(500);

    FS.close(fd);
  });

  it("seek past EOF then write creates sparse gap that survives syncfs+remount @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Write initial data
    const initial = fillPattern(100, 55);
    const fd = FS.open(`${MOUNT}/sparse`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, initial, 0, 100);

    // Seek past end and write — creates sparse gap
    FS.llseek(fd, PAGE_SIZE + 500, 0); // SEEK_SET past page boundary
    const tail = fillPattern(200, 66);
    FS.write(fd, tail, 0, 200);
    FS.close(fd);

    // File should be 100 bytes gap zeros PAGE_SIZE+500-100=gap then 200 bytes
    const expectedSize = PAGE_SIZE + 500 + 200;
    expect(FS.stat(`${MOUNT}/sparse`).size).toBe(expectedSize);

    syncAndUnmount(FS, tomefs);

    // Remount and verify sparse gap is zero-filled
    const { FS: FS2 } = await mountTome(backend);
    const stat = FS2.stat(`${MOUNT}/sparse`);
    expect(stat.size).toBe(expectedSize);

    const buf = new Uint8Array(expectedSize);
    const r = FS2.open(`${MOUNT}/sparse`, O.RDONLY);
    FS2.read(r, buf, 0, expectedSize);
    FS2.close(r);

    // First 100 bytes: original pattern
    expect(verifyPattern(buf.subarray(0, 100), 100, 55)).toBe(true);

    // Gap (100 to PAGE_SIZE+500): must be all zeros
    for (let i = 100; i < PAGE_SIZE + 500; i++) {
      expect(buf[i]).toBe(0);
    }

    // Last 200 bytes: tail pattern
    const tailSlice = buf.subarray(PAGE_SIZE + 500);
    expect(verifyPattern(tailSlice, 200, 66)).toBe(true);
  });

  it("SEEK_END on empty file returns position 0", async () => {
    const { FS } = await mountTome(backend);

    const fd = FS.open(`${MOUNT}/empty`, O.RDWR | O.CREAT, 0o666);
    const pos = FS.llseek(fd, 0, 2); // SEEK_END
    expect(pos).toBe(0);

    // Write 1 byte, SEEK_END should now be 1
    const one = new Uint8Array([42]);
    FS.write(fd, one, 0, 1);
    const pos2 = FS.llseek(fd, 0, 2);
    expect(pos2).toBe(1);

    FS.close(fd);
  });

  it("negative seek position throws EINVAL", async () => {
    const { FS } = await mountTome(backend);

    const fd = FS.open(`${MOUNT}/neg`, O.RDWR | O.CREAT, 0o666);
    const data = fillPattern(100, 77);
    FS.write(fd, data, 0, 100);

    // SEEK_SET to negative position
    expect(() => FS.llseek(fd, -1, 0)).toThrow();

    // SEEK_CUR that would produce negative position
    FS.llseek(fd, 10, 0); // SEEK_SET to 10
    expect(() => FS.llseek(fd, -20, 1)).toThrow(); // SEEK_CUR -20 from 10

    // SEEK_END that would produce negative position
    expect(() => FS.llseek(fd, -200, 2)).toThrow(); // file is 100 bytes

    FS.close(fd);
  });

  it("seek to exact page boundary then write 1 byte touches next page only", async () => {
    const { FS } = await mountTome(backend);

    // Write 2 full pages
    const data = fillPattern(PAGE_SIZE * 2, 88);
    const fd = FS.open(`${MOUNT}/boundary`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, data, 0, data.length);

    // Seek to exact start of page 1
    FS.llseek(fd, PAGE_SIZE, 0);

    // Write 1 byte — should modify only byte 0 of page 1
    const marker = new Uint8Array([0xAB]);
    FS.write(fd, marker, 0, 1);

    // Read back page 0 — should be unmodified
    FS.llseek(fd, 0, 0);
    const page0 = new Uint8Array(PAGE_SIZE);
    FS.read(fd, page0, 0, PAGE_SIZE);
    expect(verifyPattern(page0, PAGE_SIZE, 88)).toBe(true);

    // Read back byte at page boundary — should be our marker
    FS.llseek(fd, PAGE_SIZE, 0);
    const check = new Uint8Array(1);
    FS.read(fd, check, 0, 1);
    expect(check[0]).toBe(0xAB);

    // Rest of page 1 (bytes 1 through PAGE_SIZE-1) should be original pattern
    FS.llseek(fd, PAGE_SIZE + 1, 0);
    const rest = new Uint8Array(PAGE_SIZE - 1);
    FS.read(fd, rest, 0, PAGE_SIZE - 1);
    for (let i = 0; i < PAGE_SIZE - 1; i++) {
      expect(rest[i]).toBe((88 + (PAGE_SIZE + 1 + i) * 31) & 0xff);
    }

    FS.close(fd);
  });

  it("independent fd positions on same file under cache pressure", async () => {
    // Two fds on the same file, seeking independently. Reads through
    // one fd must not affect the position of the other. Under cache
    // pressure, pages loaded by fd1 may evict pages needed by fd2.
    const { FS } = await mountTome(backend);

    const data = fillPattern(PAGE_SIZE * 6, 99);
    const fd = FS.open(`${MOUNT}/shared`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, data, 0, data.length);
    FS.close(fd);

    const fd1 = FS.open(`${MOUNT}/shared`, O.RDONLY);
    const fd2 = FS.open(`${MOUNT}/shared`, O.RDONLY);

    // fd1 at page 0, fd2 at page 4
    FS.llseek(fd1, 0, 0);
    FS.llseek(fd2, PAGE_SIZE * 4, 0);

    // Read from fd2 (page 4) — fills cache with pages 4+
    const buf2 = new Uint8Array(PAGE_SIZE);
    FS.read(fd2, buf2, 0, PAGE_SIZE);

    // Read from fd1 (page 0) — must reload page 0 from backend
    const buf1 = new Uint8Array(PAGE_SIZE);
    FS.read(fd1, buf1, 0, PAGE_SIZE);

    // Verify both reads got correct data
    expect(verifyPattern(buf1, PAGE_SIZE, 99)).toBe(true);
    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(buf2[i]).toBe((99 + (PAGE_SIZE * 4 + i) * 31) & 0xff);
    }

    // Verify positions advanced independently
    // fd1 should be at PAGE_SIZE, fd2 at PAGE_SIZE * 5
    const pos1 = FS.llseek(fd1, 0, 1); // SEEK_CUR + 0
    const pos2 = FS.llseek(fd2, 0, 1);
    expect(pos1).toBe(PAGE_SIZE);
    expect(pos2).toBe(PAGE_SIZE * 5);

    FS.close(fd1);
    FS.close(fd2);
  });

  it("rapid random seek+write pattern preserves all data after remount", async () => {
    // Write small amounts at random-ish page-crossing positions,
    // then syncfs+remount and verify every byte.
    const { FS, tomefs } = await mountTome(backend);

    // Pre-fill a 4-page file with zeros
    const size = PAGE_SIZE * 4;
    const fd = FS.open(`${MOUNT}/random`, O.RDWR | O.CREAT, 0o666);
    const zeros = new Uint8Array(size);
    FS.write(fd, zeros, 0, size);

    // Track what we write so we can verify later
    const expected = new Uint8Array(size);

    // Write small chunks at various positions
    const positions = [
      0,                        // start of page 0
      PAGE_SIZE - 2,            // end of page 0, crossing into page 1
      PAGE_SIZE,                // start of page 1
      PAGE_SIZE * 2 + 100,      // middle of page 2
      PAGE_SIZE * 3 + PAGE_SIZE - 1, // last byte of page 3
      PAGE_SIZE - 1,            // last byte of page 0
      PAGE_SIZE * 2,            // start of page 2
    ];

    for (let i = 0; i < positions.length; i++) {
      const pos = positions[i];
      const chunkSize = Math.min(50, size - pos);
      const chunk = new Uint8Array(chunkSize);
      chunk.fill((i + 1) * 17 & 0xff);

      FS.llseek(fd, pos, 0);
      FS.write(fd, chunk, 0, chunkSize);

      // Update expected buffer
      for (let j = 0; j < chunkSize; j++) {
        expected[pos + j] = chunk[j];
      }
    }
    FS.close(fd);

    syncAndUnmount(FS, tomefs);

    // Remount and verify
    const { FS: FS2 } = await mountTome(backend);
    const buf = new Uint8Array(size);
    const r = FS2.open(`${MOUNT}/random`, O.RDONLY);
    FS2.read(r, buf, 0, size);
    FS2.close(r);

    for (let i = 0; i < size; i++) {
      if (buf[i] !== expected[i]) {
        throw new Error(
          `Mismatch at byte ${i}: got ${buf[i]}, expected ${expected[i]}`,
        );
      }
    }
  });

  it("SEEK_END after another fd extends file sees new size", async () => {
    const { FS } = await mountTome(backend);

    const fd1 = FS.open(`${MOUNT}/extend`, O.RDWR | O.CREAT, 0o666);
    const fd2 = FS.open(`${MOUNT}/extend`, O.RDWR);

    // Write 100 bytes through fd1
    const data = fillPattern(100, 11);
    FS.write(fd1, data, 0, 100);

    // fd2 seeks to end — should see 100
    const end1 = FS.llseek(fd2, 0, 2);
    expect(end1).toBe(100);

    // Extend via fd1
    FS.llseek(fd1, 0, 2);
    const more = fillPattern(PAGE_SIZE, 22);
    FS.write(fd1, more, 0, PAGE_SIZE);

    // fd2 seeks to end again — should see 100 + PAGE_SIZE
    const end2 = FS.llseek(fd2, 0, 2);
    expect(end2).toBe(100 + PAGE_SIZE);

    FS.close(fd1);
    FS.close(fd2);
  });

  it("seek + truncate + seek + write: no stale data in gap", async () => {
    // Write a multi-page file, truncate it down, then seek past
    // the new end and write. The gap between new size and write
    // position must be zeros, not stale data from pre-truncation.
    const { FS, tomefs } = await mountTome(backend);

    const data = fillPattern(PAGE_SIZE * 3, 0xAA);
    const fd = FS.open(`${MOUNT}/trunc_seek`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, data, 0, data.length);
    FS.close(fd);

    // Truncate to 100 bytes
    FS.truncate(`${MOUNT}/trunc_seek`, 100);

    // Reopen, seek past end, write
    const fd2 = FS.open(`${MOUNT}/trunc_seek`, O.RDWR);
    FS.llseek(fd2, PAGE_SIZE + 500, 0);
    const tail = fillPattern(50, 0xBB);
    FS.write(fd2, tail, 0, 50);
    FS.close(fd2);

    syncAndUnmount(FS, tomefs);

    // Remount and verify the gap is zero
    const { FS: FS2 } = await mountTome(backend);
    const totalSize = PAGE_SIZE + 500 + 50;
    const buf = new Uint8Array(totalSize);
    const r = FS2.open(`${MOUNT}/trunc_seek`, O.RDONLY);
    FS2.read(r, buf, 0, totalSize);
    FS2.close(r);

    // First 100 bytes: original pattern (truncation preserved prefix)
    expect(verifyPattern(buf.subarray(0, 100), 100, 0xAA)).toBe(true);

    // Gap (100 to PAGE_SIZE+500): must be all zeros, NOT stale data
    for (let i = 100; i < PAGE_SIZE + 500; i++) {
      if (buf[i] !== 0) {
        throw new Error(
          `Stale data at byte ${i}: got ${buf[i]}, expected 0`,
        );
      }
    }

    // Tail: our written pattern
    expect(
      verifyPattern(buf.subarray(PAGE_SIZE + 500), 50, 0xBB),
    ).toBe(true);
  });

  it("alternating seek+read on two files thrashes cache correctly", async () => {
    // Two files, each 3 pages. Cache holds 4 pages total.
    // Alternating page reads between the files forces constant
    // eviction. Every read must return correct data.
    const { FS } = await mountTome(backend);

    const sizePerFile = PAGE_SIZE * 3;
    const dataA = fillPattern(sizePerFile, 0x10);
    const dataB = fillPattern(sizePerFile, 0x20);

    let fd = FS.open(`${MOUNT}/a`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, dataA, 0, sizePerFile);
    FS.close(fd);

    fd = FS.open(`${MOUNT}/b`, O.RDWR | O.CREAT, 0o666);
    FS.write(fd, dataB, 0, sizePerFile);
    FS.close(fd);

    const fdA = FS.open(`${MOUNT}/a`, O.RDONLY);
    const fdB = FS.open(`${MOUNT}/b`, O.RDONLY);

    // Read page-by-page, alternating between files
    for (let page = 0; page < 3; page++) {
      const bufA = new Uint8Array(PAGE_SIZE);
      FS.llseek(fdA, page * PAGE_SIZE, 0);
      FS.read(fdA, bufA, 0, PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) {
        expect(bufA[i]).toBe((0x10 + (page * PAGE_SIZE + i) * 31) & 0xff);
      }

      const bufB = new Uint8Array(PAGE_SIZE);
      FS.llseek(fdB, page * PAGE_SIZE, 0);
      FS.read(fdB, bufB, 0, PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) {
        expect(bufB[i]).toBe((0x20 + (page * PAGE_SIZE + i) * 31) & 0xff);
      }
    }

    FS.close(fdA);
    FS.close(fdB);
  });
});
