/**
 * Adversarial tests: mmap and msync under cache pressure with persistence.
 *
 * The existing allocate-mmap.test.ts and msync.test.ts exercise correctness
 * with the default 4096-page cache where no eviction occurs. These tests
 * use a 4-page (32 KB) cache to force page eviction during mmap reads and
 * msync writes, targeting the seam between mmap/msync and the page cache
 * eviction logic.
 *
 * PostgreSQL uses mmap for some I/O paths (relation files, WAL). When the
 * working set exceeds the page cache, mmap reads must reload evicted pages
 * from the backend, and msync writes must correctly handle pages that were
 * evicted between mmap and msync.
 *
 * Ethos §9: "Write tests designed to break tomefs specifically — things
 * that pass against MEMFS but expose real bugs in the page cache layer.
 * Target the seams."
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
} as const;

const MOUNT = "/tome";

/** Small cache: 4 pages = 32 KB. Forces eviction on multi-page operations. */
const TINY_CACHE = 4;

async function mountTome(backend: SyncMemoryBackend, maxPages = TINY_CACHE) {
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

function syncAndUnmount(FS: any, tomefs: any) {
  tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
    if (err) throw err;
  });
  FS.unmount(MOUNT);
}

describe("adversarial: mmap under cache pressure (4-page cache)", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("mmap reads correct data from a 6-page file with 4-page cache @fast", async () => {
    const { FS } = await mountTome(backend);

    // Write 6 pages of distinguishable data
    const stream = FS.open(`${MOUNT}/big`, O.RDWR | O.CREAT, 0o666);
    for (let p = 0; p < 6; p++) {
      const page = new Uint8Array(PAGE_SIZE);
      page.fill(p + 1); // page 0 = 0x01, page 1 = 0x02, ...
      FS.write(stream, page, 0, PAGE_SIZE, p * PAGE_SIZE);
    }

    // mmap the entire file — reads 6 pages through a 4-page cache,
    // forcing eviction of the first pages loaded during the read
    const result = stream.stream_ops.mmap(
      stream,
      PAGE_SIZE * 6,
      0,
      1, // PROT_READ
      1, // MAP_SHARED
    );

    expect(result.ptr.length).toBe(PAGE_SIZE * 6);

    // Verify each page has the correct fill value
    for (let p = 0; p < 6; p++) {
      const pageData = result.ptr.subarray(p * PAGE_SIZE, (p + 1) * PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) {
        expect(pageData[i]).toBe(p + 1);
      }
    }

    FS.close(stream);
  });

  it("mmap reads page-boundary-crossing region after eviction", async () => {
    const { FS } = await mountTome(backend);

    // Create file with 6 pages of byte-level unique data
    const stream = FS.open(`${MOUNT}/boundary`, O.RDWR | O.CREAT, 0o666);
    const fullData = new Uint8Array(PAGE_SIZE * 6);
    for (let i = 0; i < fullData.length; i++) fullData[i] = (i * 7) & 0xff;
    FS.write(stream, fullData, 0, fullData.length);

    // mmap a 2-page region crossing the page 2/3 boundary
    // With a 4-page cache, the earlier pages may be evicted
    const mmapPos = PAGE_SIZE * 2 + PAGE_SIZE / 2; // middle of page 2
    const mmapLen = PAGE_SIZE; // extends into page 3
    const result = stream.stream_ops.mmap(stream, mmapLen, mmapPos, 1, 1);

    expect(result.ptr.length).toBe(mmapLen);
    for (let i = 0; i < mmapLen; i++) {
      expect(result.ptr[i]).toBe(((mmapPos + i) * 7) & 0xff);
    }

    FS.close(stream);
  });

  it("msync writes correctly after source pages were evicted", async () => {
    const { FS } = await mountTome(backend);

    // Create file with 2 pages
    const stream = FS.open(`${MOUNT}/msync-evict`, O.RDWR | O.CREAT, 0o666);
    const data = new Uint8Array(PAGE_SIZE * 2).fill(0xaa);
    FS.write(stream, data, 0, data.length);

    // mmap page 0 region
    const result = stream.stream_ops.mmap(stream, PAGE_SIZE, 0, 3, 1);

    // Fill cache with other files to evict the mmap'd pages
    for (let f = 0; f < 4; f++) {
      const s = FS.open(`${MOUNT}/filler${f}`, O.RDWR | O.CREAT, 0o666);
      const filler = new Uint8Array(PAGE_SIZE).fill(0xff);
      FS.write(s, filler, 0, PAGE_SIZE);
      FS.close(s);
    }

    // Modify the mmap'd buffer
    result.ptr.fill(0xbb);

    // msync back — must reload page 0 from backend (evicted), write, mark dirty
    stream.stream_ops.msync(stream, result.ptr, 0, PAGE_SIZE, 0);

    // Read back and verify the msync'd data
    const readBuf = new Uint8Array(PAGE_SIZE * 2);
    FS.read(stream, readBuf, 0, PAGE_SIZE * 2, 0);

    // Page 0: msync'd to 0xbb
    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(readBuf[i]).toBe(0xbb);
    }
    // Page 1: untouched, still 0xaa
    for (let i = PAGE_SIZE; i < PAGE_SIZE * 2; i++) {
      expect(readBuf[i]).toBe(0xaa);
    }

    FS.close(stream);
  });

  it("msync spanning 3 pages under cache pressure writes all pages @fast", async () => {
    const { FS } = await mountTome(backend);

    // Create 5-page file
    const stream = FS.open(`${MOUNT}/msync-span`, O.RDWR | O.CREAT, 0o666);
    const data = new Uint8Array(PAGE_SIZE * 5);
    for (let i = 0; i < data.length; i++) data[i] = (i * 3) & 0xff;
    FS.write(stream, data, 0, data.length);

    // mmap 3 pages starting mid-page-1 (spans pages 1, 2, 3)
    const mmapPos = PAGE_SIZE + 100;
    const mmapLen = PAGE_SIZE * 3 - 200;
    const result = stream.stream_ops.mmap(stream, mmapLen, mmapPos, 3, 1);

    // Overwrite the mmap'd buffer with a pattern
    for (let i = 0; i < mmapLen; i++) result.ptr[i] = 0xdd;

    // msync under cache pressure
    stream.stream_ops.msync(stream, result.ptr, mmapPos, mmapLen, 0);

    // Verify by reading back the full file
    const readBuf = new Uint8Array(PAGE_SIZE * 5);
    FS.read(stream, readBuf, 0, PAGE_SIZE * 5, 0);

    // Before msync region: original data
    for (let i = 0; i < mmapPos; i++) {
      expect(readBuf[i]).toBe((i * 3) & 0xff);
    }
    // msync region: 0xdd
    for (let i = mmapPos; i < mmapPos + mmapLen; i++) {
      expect(readBuf[i]).toBe(0xdd);
    }
    // After msync region: original data
    for (let i = mmapPos + mmapLen; i < PAGE_SIZE * 5; i++) {
      expect(readBuf[i]).toBe((i * 3) & 0xff);
    }

    FS.close(stream);
  });
});

describe("adversarial: mmap + msync + persistence (syncfs + remount)", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("msync data survives syncfs + remount with tiny cache @fast", async () => {
    // Phase 1: write file, mmap, modify, msync, sync
    const { FS, tomefs } = await mountTome(backend);
    const stream = FS.open(`${MOUNT}/persist`, O.RDWR | O.CREAT, 0o666);
    const original = new Uint8Array(PAGE_SIZE * 3).fill(0x11);
    FS.write(stream, original, 0, original.length);

    // mmap page 1
    const result = stream.stream_ops.mmap(stream, PAGE_SIZE, PAGE_SIZE, 3, 1);
    result.ptr.fill(0x22);
    stream.stream_ops.msync(stream, result.ptr, PAGE_SIZE, PAGE_SIZE, 0);

    FS.close(stream);
    syncAndUnmount(FS, tomefs);

    // Phase 2: remount and verify
    const { FS: FS2 } = await mountTome(backend);
    const readBuf = new Uint8Array(PAGE_SIZE * 3);
    const s2 = FS2.open(`${MOUNT}/persist`, O.RDONLY);
    FS2.read(s2, readBuf, 0, PAGE_SIZE * 3);
    FS2.close(s2);

    // Page 0: original 0x11
    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(readBuf[i]).toBe(0x11);
    }
    // Page 1: msync'd 0x22
    for (let i = PAGE_SIZE; i < PAGE_SIZE * 2; i++) {
      expect(readBuf[i]).toBe(0x22);
    }
    // Page 2: original 0x11
    for (let i = PAGE_SIZE * 2; i < PAGE_SIZE * 3; i++) {
      expect(readBuf[i]).toBe(0x11);
    }
  });

  it("multiple mmap+msync cycles across remounts @fast", async () => {
    // Cycle 1: create file, mmap first half, modify, msync, sync
    const { FS: FS1, tomefs: t1 } = await mountTome(backend);
    const s1 = FS1.open(`${MOUNT}/multi`, O.RDWR | O.CREAT, 0o666);
    FS1.write(s1, new Uint8Array(PAGE_SIZE * 4).fill(0x00), 0, PAGE_SIZE * 4);

    const m1 = s1.stream_ops.mmap(s1, PAGE_SIZE * 2, 0, 3, 1);
    m1.ptr.fill(0xaa);
    s1.stream_ops.msync(s1, m1.ptr, 0, PAGE_SIZE * 2, 0);
    FS1.close(s1);
    syncAndUnmount(FS1, t1);

    // Cycle 2: remount, mmap second half, modify, msync, sync
    const { FS: FS2, tomefs: t2 } = await mountTome(backend);
    const s2 = FS2.open(`${MOUNT}/multi`, O.RDWR);

    const m2 = s2.stream_ops.mmap(s2, PAGE_SIZE * 2, PAGE_SIZE * 2, 3, 1);
    m2.ptr.fill(0xbb);
    s2.stream_ops.msync(s2, m2.ptr, PAGE_SIZE * 2, PAGE_SIZE * 2, 0);
    FS2.close(s2);
    syncAndUnmount(FS2, t2);

    // Cycle 3: verify both halves survived
    const { FS: FS3 } = await mountTome(backend);
    const buf = new Uint8Array(PAGE_SIZE * 4);
    const s3 = FS3.open(`${MOUNT}/multi`, O.RDONLY);
    FS3.read(s3, buf, 0, PAGE_SIZE * 4);
    FS3.close(s3);

    for (let i = 0; i < PAGE_SIZE * 2; i++) {
      expect(buf[i]).toBe(0xaa);
    }
    for (let i = PAGE_SIZE * 2; i < PAGE_SIZE * 4; i++) {
      expect(buf[i]).toBe(0xbb);
    }
  });

  it("allocate + mmap + msync PostgreSQL WAL pattern with persistence", async () => {
    // Simulates PostgreSQL's WAL pre-allocation pattern:
    // 1. allocate() to extend the WAL segment
    // 2. mmap the allocated region
    // 3. Write WAL records into the mmap'd buffer
    // 4. msync to flush
    // 5. syncfs + remount to verify durability
    const { FS, tomefs } = await mountTome(backend);
    const stream = FS.open(`${MOUNT}/wal-segment`, O.RDWR | O.CREAT, 0o666);

    // Pre-allocate 3 pages (WAL segment)
    stream.stream_ops.allocate(stream, 0, PAGE_SIZE * 3);
    expect(FS.fstat(stream.fd).size).toBe(PAGE_SIZE * 3);

    // mmap the first page, write a "WAL record"
    const m1 = stream.stream_ops.mmap(stream, PAGE_SIZE, 0, 3, 1);
    const walRecord = new Uint8Array(PAGE_SIZE);
    for (let i = 0; i < 256; i++) walRecord[i] = (i * 11) & 0xff;
    m1.ptr.set(walRecord);
    stream.stream_ops.msync(stream, m1.ptr, 0, PAGE_SIZE, 0);

    // Write another WAL record to page 1
    const m2 = stream.stream_ops.mmap(stream, PAGE_SIZE, PAGE_SIZE, 3, 1);
    for (let i = 0; i < 128; i++) m2.ptr[i] = (i * 13) & 0xff;
    stream.stream_ops.msync(stream, m2.ptr, PAGE_SIZE, PAGE_SIZE, 0);

    FS.close(stream);
    syncAndUnmount(FS, tomefs);

    // Remount and verify WAL records survived
    const { FS: FS2 } = await mountTome(backend);
    const buf = new Uint8Array(PAGE_SIZE * 3);
    const s2 = FS2.open(`${MOUNT}/wal-segment`, O.RDONLY);
    FS2.read(s2, buf, 0, PAGE_SIZE * 3);
    FS2.close(s2);

    // Page 0: WAL record 1
    for (let i = 0; i < 256; i++) {
      expect(buf[i]).toBe((i * 11) & 0xff);
    }
    // Page 0 remainder: zeros (allocated but unwritten)
    for (let i = 256; i < PAGE_SIZE; i++) {
      expect(buf[i]).toBe(0);
    }
    // Page 1: WAL record 2
    for (let i = 0; i < 128; i++) {
      expect(buf[PAGE_SIZE + i]).toBe((i * 13) & 0xff);
    }
    // Page 2: entirely zeros (allocated, never written)
    for (let i = PAGE_SIZE * 2; i < PAGE_SIZE * 3; i++) {
      expect(buf[i]).toBe(0);
    }
  });

  it("msync after intermediate write overwrites correctly", async () => {
    // Tests the interaction where:
    // 1. mmap reads data
    // 2. Regular write() modifies the same region
    // 3. msync writes the (stale) mmap'd data back, overwriting the write
    const { FS } = await mountTome(backend);
    const stream = FS.open(`${MOUNT}/overwrite`, O.RDWR | O.CREAT, 0o666);
    const data = new Uint8Array(PAGE_SIZE).fill(0x11);
    FS.write(stream, data, 0, PAGE_SIZE);

    // mmap page 0 — copies data into buffer (0x11)
    const result = stream.stream_ops.mmap(stream, PAGE_SIZE, 0, 3, 1);
    expect(result.ptr[0]).toBe(0x11);

    // Regular write overwrites the same page with 0x22
    const overwrite = new Uint8Array(PAGE_SIZE).fill(0x22);
    FS.write(stream, overwrite, 0, PAGE_SIZE, 0);

    // Verify write took effect
    const check = new Uint8Array(PAGE_SIZE);
    FS.read(stream, check, 0, PAGE_SIZE, 0);
    expect(check[0]).toBe(0x22);

    // msync the original mmap'd buffer (0x11) back — should overwrite
    stream.stream_ops.msync(stream, result.ptr, 0, PAGE_SIZE, 0);

    // Verify msync overwrote the regular write
    const final = new Uint8Array(PAGE_SIZE);
    FS.read(stream, final, 0, PAGE_SIZE, 0);
    expect(final[0]).toBe(0x11);

    FS.close(stream);
  });

  it("concurrent mmap from two files under cache pressure", async () => {
    const { FS } = await mountTome(backend);

    // Create two 3-page files (total 6 pages, but cache is 4)
    for (const name of ["alpha", "beta"]) {
      const s = FS.open(`${MOUNT}/${name}`, O.RDWR | O.CREAT, 0o666);
      const d = new Uint8Array(PAGE_SIZE * 3).fill(name === "alpha" ? 0xaa : 0xbb);
      FS.write(s, d, 0, d.length);
      FS.close(s);
    }

    // mmap both files (reads 6 pages total through 4-page cache)
    const sa = FS.open(`${MOUNT}/alpha`, O.RDWR);
    const sb = FS.open(`${MOUNT}/beta`, O.RDWR);

    const mmapA = sa.stream_ops.mmap(sa, PAGE_SIZE * 3, 0, 3, 1);
    const mmapB = sb.stream_ops.mmap(sb, PAGE_SIZE * 3, 0, 3, 1);

    // Verify both mmap'd buffers have correct data
    for (let i = 0; i < PAGE_SIZE * 3; i++) {
      expect(mmapA.ptr[i]).toBe(0xaa);
      expect(mmapB.ptr[i]).toBe(0xbb);
    }

    // Modify both buffers
    mmapA.ptr.fill(0xcc);
    mmapB.ptr.fill(0xdd);

    // msync both back (interleaved to maximize eviction)
    sa.stream_ops.msync(sa, mmapA.ptr, 0, PAGE_SIZE * 3, 0);
    sb.stream_ops.msync(sb, mmapB.ptr, 0, PAGE_SIZE * 3, 0);

    // Verify both files have the msync'd data
    const bufA = new Uint8Array(PAGE_SIZE * 3);
    const bufB = new Uint8Array(PAGE_SIZE * 3);
    FS.read(sa, bufA, 0, PAGE_SIZE * 3, 0);
    FS.read(sb, bufB, 0, PAGE_SIZE * 3, 0);

    for (let i = 0; i < PAGE_SIZE * 3; i++) {
      expect(bufA[i]).toBe(0xcc);
      expect(bufB[i]).toBe(0xdd);
    }

    FS.close(sa);
    FS.close(sb);
  });

  it("mmap after truncate + extend reads zeros in the gap", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create 3-page file
    const stream = FS.open(`${MOUNT}/trunc-ext`, O.RDWR | O.CREAT, 0o666);
    const data = new Uint8Array(PAGE_SIZE * 3).fill(0xee);
    FS.write(stream, data, 0, data.length);

    // Truncate to 1 page
    FS.ftruncate(stream.fd, PAGE_SIZE);

    // Extend back to 3 pages by writing at the end
    FS.write(stream, new Uint8Array([0xff]), 0, 1, PAGE_SIZE * 3 - 1);

    // mmap the gap region (page 1 should be zero-filled)
    const result = stream.stream_ops.mmap(stream, PAGE_SIZE, PAGE_SIZE, 1, 1);

    // The gap page should be zeros — not stale 0xee data
    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(result.ptr[i]).toBe(0);
    }

    // Verify persistence: write through msync, sync, remount
    result.ptr.fill(0x77);
    stream.stream_ops.msync(stream, result.ptr, PAGE_SIZE, PAGE_SIZE, 0);
    FS.close(stream);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    const buf = new Uint8Array(PAGE_SIZE);
    const s2 = FS2.open(`${MOUNT}/trunc-ext`, O.RDONLY);
    FS2.read(s2, buf, 0, PAGE_SIZE, PAGE_SIZE);
    FS2.close(s2);

    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(buf[i]).toBe(0x77);
    }
  });

  it("large mmap exceeding entire cache survives persistence cycle", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Create 8-page file (2x the 4-page cache)
    const stream = FS.open(`${MOUNT}/huge`, O.RDWR | O.CREAT, 0o666);
    const data = new Uint8Array(PAGE_SIZE * 8);
    for (let i = 0; i < data.length; i++) data[i] = (i * 5) & 0xff;
    FS.write(stream, data, 0, data.length);

    // mmap entire file — forces complete cache rotation
    const result = stream.stream_ops.mmap(stream, PAGE_SIZE * 8, 0, 3, 1);

    // Modify every page with a unique marker
    for (let p = 0; p < 8; p++) {
      for (let i = 0; i < PAGE_SIZE; i++) {
        result.ptr[p * PAGE_SIZE + i] = ((p + 1) * 0x10 + (i & 0x0f)) & 0xff;
      }
    }

    // msync entire file back — forces complete cache rotation again
    stream.stream_ops.msync(stream, result.ptr, 0, PAGE_SIZE * 8, 0);

    FS.close(stream);
    syncAndUnmount(FS, tomefs);

    // Remount with tiny cache and verify all pages
    const { FS: FS2 } = await mountTome(backend);
    const buf = new Uint8Array(PAGE_SIZE * 8);
    const s2 = FS2.open(`${MOUNT}/huge`, O.RDONLY);
    FS2.read(s2, buf, 0, PAGE_SIZE * 8);
    FS2.close(s2);

    for (let p = 0; p < 8; p++) {
      for (let i = 0; i < PAGE_SIZE; i++) {
        const expected = ((p + 1) * 0x10 + (i & 0x0f)) & 0xff;
        expect(buf[p * PAGE_SIZE + i]).toBe(expected);
      }
    }
  });
});
