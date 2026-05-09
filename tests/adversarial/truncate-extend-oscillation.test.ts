/**
 * Adversarial tests: Truncate-extend oscillation under cache pressure
 * with persistence round-trips.
 *
 * Targets the interaction between resizeFileStorage's two code paths:
 * - Shrink: zeroTailAfterTruncate + invalidatePagesFrom + deletePagesFrom
 * - Grow: markPageDirtyNoRead (sentinel page for restoreTree)
 *
 * Repeated truncate→extend cycles under tiny cache exercise:
 * - Tail page loaded from backend by zeroTailAfterTruncate (cache miss
 *   after eviction by competing file's I/O)
 * - Sentinel page created by markPageDirtyNoRead competing with the
 *   freshly-loaded tail page for cache slots
 * - syncfs persisting mid-cycle metadata + dirty pages atomically
 * - restoreTree recovering the file size from maxPageIndex when the
 *   sentinel page's existence (or absence) determines the recovered size
 *
 * Realistic motivation: Postgres VACUUM truncates heap files, then INSERTs
 * extend them again. This cycle repeats continuously in production.
 *
 * Ethos §9: "Think like an attacker trying to corrupt a database through
 * its filesystem."
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

async function mountTome(backend: SyncMemoryBackend, maxPages = 4) {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  const Module = await createModule();
  const FS = Module.FS;
  const tomefs = createTomeFS(FS, { backend, maxPages });
  FS.mkdir("/tome");
  FS.mount(tomefs, {}, "/tome");
  return { FS, tomefs, Module };
}

function syncfs(FS: any, tomefs: any) {
  tomefs.syncfs(FS.lookupPath("/tome").node.mount, false, (err: any) => {
    if (err) throw err;
  });
}

function syncAndUnmount(FS: any, tomefs: any) {
  syncfs(FS, tomefs);
  FS.unmount("/tome");
}

function writeFile(FS: any, path: string, data: Uint8Array) {
  const fd = FS.open(path, O.WRONLY | O.CREAT | O.TRUNC);
  FS.write(fd, data, 0, data.length, 0);
  FS.close(fd);
}

function readFile(FS: any, path: string, size: number): Uint8Array {
  const buf = new Uint8Array(size);
  const fd = FS.open(path, O.RDONLY);
  FS.read(fd, buf, 0, size, 0);
  FS.close(fd);
  return buf;
}

function filledData(pages: number, value: number): Uint8Array {
  const data = new Uint8Array(pages * PAGE_SIZE);
  data.fill(value);
  return data;
}

describe("adversarial: truncate-extend oscillation + persistence @fast", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("truncate mid-page then extend back: tail zeroing preserved after persistence", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    // Write 3 pages of 0xAA
    writeFile(FS, "/tome/f", filledData(3, 0xaa));

    // Truncate to mid-page (page 1, offset 100)
    const truncSize = PAGE_SIZE + 100;
    FS.truncate("/tome/f", truncSize);

    // Extend back to 3 pages
    FS.truncate("/tome/f", PAGE_SIZE * 3);

    // Persist and remount
    syncAndUnmount(FS, tomefs);
    const m2 = await mountTome(backend, 4);

    // Verify size
    const stat = m2.FS.stat("/tome/f");
    expect(stat.size).toBe(PAGE_SIZE * 3);

    // Read and verify: page 0 = 0xAA, page 1 = 0xAA for first 100 bytes
    // then zeros, pages 2 = all zeros (extended region)
    const data = readFile(m2.FS, "/tome/f", PAGE_SIZE * 3);
    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(data[i]).toBe(0xaa);
    }
    for (let i = PAGE_SIZE; i < PAGE_SIZE + 100; i++) {
      expect(data[i]).toBe(0xaa);
    }
    for (let i = PAGE_SIZE + 100; i < PAGE_SIZE * 2; i++) {
      expect(data[i]).toBe(0);
    }
    for (let i = PAGE_SIZE * 2; i < PAGE_SIZE * 3; i++) {
      expect(data[i]).toBe(0);
    }

    m2.FS.unmount("/tome");
  });

  it("repeated truncate-extend cycles with syncfs between each pair @fast", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    writeFile(FS, "/tome/f", filledData(4, 0xbb));

    for (let cycle = 0; cycle < 5; cycle++) {
      // Truncate to 1.5 pages
      FS.truncate("/tome/f", PAGE_SIZE + PAGE_SIZE / 2);

      // Extend back to 4 pages
      FS.truncate("/tome/f", PAGE_SIZE * 4);

      // Persist
      syncfs(FS, tomefs);
    }

    // Remount and verify
    syncAndUnmount(FS, tomefs);
    const m2 = await mountTome(backend, 4);

    const stat = m2.FS.stat("/tome/f");
    expect(stat.size).toBe(PAGE_SIZE * 4);

    const data = readFile(m2.FS, "/tome/f", PAGE_SIZE * 4);
    // Page 0 retains original data through all cycles
    expect(data[0]).toBe(0xbb);
    expect(data[PAGE_SIZE - 1]).toBe(0xbb);
    // Page 1 first half retains data (truncation kept it)
    expect(data[PAGE_SIZE]).toBe(0xbb);
    expect(data[PAGE_SIZE + PAGE_SIZE / 2 - 1]).toBe(0xbb);
    // Page 1 second half was zeroed by first truncation
    expect(data[PAGE_SIZE + PAGE_SIZE / 2]).toBe(0);
    // Pages 2-3 were zeroed by extension
    expect(data[PAGE_SIZE * 2]).toBe(0);
    expect(data[PAGE_SIZE * 3]).toBe(0);

    m2.FS.unmount("/tome");
  });

  it("truncate-extend without intermediate syncfs @fast", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    writeFile(FS, "/tome/f", filledData(3, 0xcc));
    syncfs(FS, tomefs);

    // Multiple truncate-extend cycles WITHOUT syncing
    for (let i = 0; i < 3; i++) {
      FS.truncate("/tome/f", PAGE_SIZE / 2);
      FS.truncate("/tome/f", PAGE_SIZE * 3);
    }

    // Single sync + remount
    syncAndUnmount(FS, tomefs);
    const m2 = await mountTome(backend, 4);

    const stat = m2.FS.stat("/tome/f");
    expect(stat.size).toBe(PAGE_SIZE * 3);

    const data = readFile(m2.FS, "/tome/f", PAGE_SIZE * 3);
    // First half-page retains original data
    for (let i = 0; i < PAGE_SIZE / 2; i++) {
      expect(data[i]).toBe(0xcc);
    }
    // Rest is zeroed
    for (let i = PAGE_SIZE / 2; i < PAGE_SIZE * 3; i++) {
      expect(data[i]).toBe(0);
    }

    m2.FS.unmount("/tome");
  });

  it("truncate file A while file B has dirty pages competing for cache @fast", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    // Write 2 pages each to A and B (fills 4-page cache)
    writeFile(FS, "/tome/a", filledData(2, 0xaa));
    writeFile(FS, "/tome/b", filledData(2, 0xbb));

    // Truncate A to mid-page — this reads A's tail page back from backend
    // (it may have been evicted by B's writes), zeros the tail, evicts
    // pages beyond the truncation point
    FS.truncate("/tome/a", PAGE_SIZE + 50);

    // Write more to B — this competes with A's pages for cache
    const fd = FS.open("/tome/b", O.WRONLY);
    const extra = new Uint8Array(PAGE_SIZE);
    extra.fill(0xdd);
    FS.write(fd, extra, 0, PAGE_SIZE, 0);
    FS.close(fd);

    // Persist both
    syncAndUnmount(FS, tomefs);
    const m2 = await mountTome(backend, 4);

    // Verify A: truncated correctly
    const statA = m2.FS.stat("/tome/a");
    expect(statA.size).toBe(PAGE_SIZE + 50);
    const dataA = readFile(m2.FS, "/tome/a", PAGE_SIZE + 50);
    expect(dataA[0]).toBe(0xaa);
    expect(dataA[PAGE_SIZE]).toBe(0xaa);
    expect(dataA[PAGE_SIZE + 49]).toBe(0xaa);

    // Verify B: overwrote page 0, page 1 untouched
    const statB = m2.FS.stat("/tome/b");
    expect(statB.size).toBe(PAGE_SIZE * 2);
    const dataB = readFile(m2.FS, "/tome/b", PAGE_SIZE * 2);
    expect(dataB[0]).toBe(0xdd);
    expect(dataB[PAGE_SIZE - 1]).toBe(0xdd);
    expect(dataB[PAGE_SIZE]).toBe(0xbb);
    expect(dataB[PAGE_SIZE * 2 - 1]).toBe(0xbb);

    m2.FS.unmount("/tome");
  });

  it("truncate to zero then extend with write: no stale data leaks @fast", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    // Write 2 pages of 0xFF
    writeFile(FS, "/tome/f", filledData(2, 0xff));
    syncfs(FS, tomefs);

    // Truncate to zero
    FS.truncate("/tome/f", 0);

    // Extend by allocating 2 pages (without writing data)
    FS.truncate("/tome/f", PAGE_SIZE * 2);

    // Persist and remount
    syncAndUnmount(FS, tomefs);
    const m2 = await mountTome(backend, 4);

    const stat = m2.FS.stat("/tome/f");
    expect(stat.size).toBe(PAGE_SIZE * 2);

    // All data must be zero — no stale 0xFF from before truncation
    const data = readFile(m2.FS, "/tome/f", PAGE_SIZE * 2);
    for (let i = 0; i < PAGE_SIZE * 2; i++) {
      expect(data[i]).toBe(0);
    }

    m2.FS.unmount("/tome");
  });

  it("alternating truncate and write-extend on two competing files @fast", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    // Both files start with 2 pages
    writeFile(FS, "/tome/x", filledData(2, 0x11));
    writeFile(FS, "/tome/y", filledData(2, 0x22));

    // Interleave: truncate X, extend Y, truncate Y, extend X
    FS.truncate("/tome/x", PAGE_SIZE / 4);  // X = quarter page
    // Extend Y by writing a 3rd page
    const fd1 = FS.open("/tome/y", O.WRONLY);
    const pg = new Uint8Array(PAGE_SIZE);
    pg.fill(0x33);
    FS.write(fd1, pg, 0, PAGE_SIZE, PAGE_SIZE * 2);
    FS.close(fd1);

    FS.truncate("/tome/y", PAGE_SIZE);  // Y = 1 page
    // Extend X by writing pages 1-2
    const fd2 = FS.open("/tome/x", O.WRONLY);
    const data = new Uint8Array(PAGE_SIZE * 2);
    data.fill(0x44);
    FS.write(fd2, data, 0, PAGE_SIZE * 2, PAGE_SIZE);
    FS.close(fd2);

    // Persist and remount
    syncAndUnmount(FS, tomefs);
    const m2 = await mountTome(backend, 4);

    // X: first quarter = 0x11, rest of page 0 = 0, pages 1-2 = 0x44
    const statX = m2.FS.stat("/tome/x");
    expect(statX.size).toBe(PAGE_SIZE * 3);
    const dataX = readFile(m2.FS, "/tome/x", PAGE_SIZE * 3);
    expect(dataX[0]).toBe(0x11);
    expect(dataX[PAGE_SIZE / 4 - 1]).toBe(0x11);
    expect(dataX[PAGE_SIZE / 4]).toBe(0);
    expect(dataX[PAGE_SIZE - 1]).toBe(0);
    expect(dataX[PAGE_SIZE]).toBe(0x44);
    expect(dataX[PAGE_SIZE * 3 - 1]).toBe(0x44);

    // Y: first page = 0x22
    const statY = m2.FS.stat("/tome/y");
    expect(statY.size).toBe(PAGE_SIZE);
    const dataY = readFile(m2.FS, "/tome/y", PAGE_SIZE);
    expect(dataY[0]).toBe(0x22);
    expect(dataY[PAGE_SIZE - 1]).toBe(0x22);

    m2.FS.unmount("/tome");
  });

  it("truncate shrinks below sentinel page: restoreTree must not use stale maxPageIndex @fast", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    // Write 1 page, then extend to 4 pages (creates sentinel at page 3)
    writeFile(FS, "/tome/f", filledData(1, 0xee));
    FS.truncate("/tome/f", PAGE_SIZE * 4);
    syncfs(FS, tomefs);

    // Now truncate back to 1 page — must invalidate sentinel
    FS.truncate("/tome/f", PAGE_SIZE);

    syncAndUnmount(FS, tomefs);
    const m2 = await mountTome(backend, 4);

    const stat = m2.FS.stat("/tome/f");
    expect(stat.size).toBe(PAGE_SIZE);

    const data = readFile(m2.FS, "/tome/f", PAGE_SIZE);
    expect(data[0]).toBe(0xee);
    expect(data[PAGE_SIZE - 1]).toBe(0xee);

    m2.FS.unmount("/tome");
  });

  it("extend then truncate to mid-new-page: sentinel and tail interact @fast", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    writeFile(FS, "/tome/f", filledData(1, 0x77));

    // Extend to 4 pages (creates sentinel at page 3)
    FS.truncate("/tome/f", PAGE_SIZE * 4);

    // Immediately truncate to mid-page 2 — this must:
    // 1. Zero tail of page 2 from offset onward
    // 2. Invalidate page 3 (the sentinel)
    // 3. Delete pages 3+ from backend
    const truncSize = PAGE_SIZE * 2 + 500;
    FS.truncate("/tome/f", truncSize);

    syncAndUnmount(FS, tomefs);
    const m2 = await mountTome(backend, 4);

    const stat = m2.FS.stat("/tome/f");
    expect(stat.size).toBe(truncSize);

    const data = readFile(m2.FS, "/tome/f", truncSize);
    // Page 0: original data
    expect(data[0]).toBe(0x77);
    // Pages 1-2: zero (extended region, never written)
    expect(data[PAGE_SIZE]).toBe(0);
    expect(data[PAGE_SIZE * 2]).toBe(0);
    expect(data[PAGE_SIZE * 2 + 499]).toBe(0);

    m2.FS.unmount("/tome");
  });

  it("3 files oscillating: maximum cache thrash with persistence @fast", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    // 3 files, each 2 pages — 6 pages total, cache holds 4
    writeFile(FS, "/tome/a", filledData(2, 0x11));
    writeFile(FS, "/tome/b", filledData(2, 0x22));
    writeFile(FS, "/tome/c", filledData(2, 0x33));
    syncfs(FS, tomefs);

    // Oscillate: truncate each, then extend each
    for (let cycle = 0; cycle < 3; cycle++) {
      FS.truncate("/tome/a", PAGE_SIZE + 10);
      FS.truncate("/tome/b", PAGE_SIZE + 20);
      FS.truncate("/tome/c", PAGE_SIZE + 30);

      FS.truncate("/tome/a", PAGE_SIZE * 2);
      FS.truncate("/tome/b", PAGE_SIZE * 2);
      FS.truncate("/tome/c", PAGE_SIZE * 2);
    }

    syncAndUnmount(FS, tomefs);
    const m2 = await mountTome(backend, 4);

    // All files: page 0 = original, page 1 = original prefix + zeros
    for (const [name, val, keep] of [
      ["/tome/a", 0x11, 10],
      ["/tome/b", 0x22, 20],
      ["/tome/c", 0x33, 30],
    ] as const) {
      const stat = m2.FS.stat(name);
      expect(stat.size).toBe(PAGE_SIZE * 2);

      const data = readFile(m2.FS, name, PAGE_SIZE * 2);
      expect(data[0]).toBe(val);
      expect(data[PAGE_SIZE - 1]).toBe(val);
      expect(data[PAGE_SIZE]).toBe(val);
      expect(data[PAGE_SIZE + keep - 1]).toBe(val);
      expect(data[PAGE_SIZE + keep]).toBe(0);
      expect(data[PAGE_SIZE * 2 - 1]).toBe(0);
    }

    m2.FS.unmount("/tome");
  });

  it("ftruncate via fd during active I/O on competing file @fast", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    writeFile(FS, "/tome/victim", filledData(3, 0xaa));
    writeFile(FS, "/tome/active", filledData(1, 0xbb));

    // Open fd on active file and keep writing
    const activeStream = FS.open("/tome/active", O.RDWR);

    // Truncate victim via fd while active file occupies cache
    const victimStream = FS.open("/tome/victim", O.RDWR);
    FS.ftruncate(victimStream.fd, PAGE_SIZE + 200);

    // Write to active file — evicts victim's pages
    const bigWrite = new Uint8Array(PAGE_SIZE * 2);
    bigWrite.fill(0xcc);
    FS.write(activeStream, bigWrite, 0, PAGE_SIZE * 2, 0);

    // Extend victim back
    FS.ftruncate(victimStream.fd, PAGE_SIZE * 3);

    // Write to extended region of victim (page 2)
    const extData = new Uint8Array(PAGE_SIZE);
    extData.fill(0xdd);
    FS.write(victimStream, extData, 0, PAGE_SIZE, PAGE_SIZE * 2);

    FS.close(victimStream);
    FS.close(activeStream);

    syncAndUnmount(FS, tomefs);
    const m2 = await mountTome(backend, 4);

    // Victim: page 0 = 0xAA, page 1 = 0xAA(200 bytes) + zeros, page 2 = 0xDD
    const dv = readFile(m2.FS, "/tome/victim", PAGE_SIZE * 3);
    expect(dv[0]).toBe(0xaa);
    expect(dv[PAGE_SIZE + 199]).toBe(0xaa);
    expect(dv[PAGE_SIZE + 200]).toBe(0);
    expect(dv[PAGE_SIZE * 2]).toBe(0xdd);
    expect(dv[PAGE_SIZE * 3 - 1]).toBe(0xdd);

    // Active: 2 pages of 0xCC
    const da = readFile(m2.FS, "/tome/active", PAGE_SIZE * 2);
    expect(da[0]).toBe(0xcc);
    expect(da[PAGE_SIZE * 2 - 1]).toBe(0xcc);

    m2.FS.unmount("/tome");
  });

  it("extend beyond cache capacity then truncate back: no orphan pages @fast", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    writeFile(FS, "/tome/f", filledData(1, 0x55));

    // Extend to 8 pages (double cache capacity) — sentinel at page 7
    FS.truncate("/tome/f", PAGE_SIZE * 8);
    syncfs(FS, tomefs);

    // Truncate back to 2 pages
    FS.truncate("/tome/f", PAGE_SIZE * 2);

    syncAndUnmount(FS, tomefs);
    const m2 = await mountTome(backend, 4);

    const stat = m2.FS.stat("/tome/f");
    expect(stat.size).toBe(PAGE_SIZE * 2);

    // Verify page 0 retains data, page 1 is zero
    const data = readFile(m2.FS, "/tome/f", PAGE_SIZE * 2);
    expect(data[0]).toBe(0x55);
    expect(data[PAGE_SIZE - 1]).toBe(0x55);
    expect(data[PAGE_SIZE]).toBe(0);

    // Verify no orphan pages in backend (pages 2-7 must be deleted)
    expect(backend.maxPageIndex("/f")).toBe(1);

    m2.FS.unmount("/tome");
  });

  it("page cache assertInvariants holds through truncate-extend cycle @fast", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    writeFile(FS, "/tome/f", filledData(3, 0x99));
    tomefs.pageCache.assertInvariants();

    FS.truncate("/tome/f", PAGE_SIZE + 100);
    tomefs.pageCache.assertInvariants();

    FS.truncate("/tome/f", PAGE_SIZE * 3);
    tomefs.pageCache.assertInvariants();

    syncfs(FS, tomefs);
    tomefs.pageCache.assertInvariants();

    // Second cycle with competing file
    writeFile(FS, "/tome/g", filledData(2, 0x88));
    tomefs.pageCache.assertInvariants();

    FS.truncate("/tome/f", PAGE_SIZE / 2);
    tomefs.pageCache.assertInvariants();

    FS.truncate("/tome/f", PAGE_SIZE * 4);
    tomefs.pageCache.assertInvariants();

    syncfs(FS, tomefs);
    tomefs.pageCache.assertInvariants();

    FS.unmount("/tome");
  });

  it("truncate to 1 byte: extreme non-alignment @fast", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    writeFile(FS, "/tome/f", filledData(3, 0xfe));

    // Truncate to just 1 byte
    FS.truncate("/tome/f", 1);
    syncfs(FS, tomefs);

    // Extend to 2 pages
    FS.truncate("/tome/f", PAGE_SIZE * 2);

    syncAndUnmount(FS, tomefs);
    const m2 = await mountTome(backend, 4);

    const stat = m2.FS.stat("/tome/f");
    expect(stat.size).toBe(PAGE_SIZE * 2);

    const data = readFile(m2.FS, "/tome/f", PAGE_SIZE * 2);
    expect(data[0]).toBe(0xfe);  // 1 byte survives
    expect(data[1]).toBe(0);      // rest zeroed
    expect(data[PAGE_SIZE]).toBe(0);

    m2.FS.unmount("/tome");
  });

  it("write after extend fills correct pages through eviction @fast", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    writeFile(FS, "/tome/f", filledData(1, 0xab));

    // Extend to 6 pages (well beyond cache)
    FS.truncate("/tome/f", PAGE_SIZE * 6);

    // Write specific data to page 5 (must not interfere with other pages)
    const fd = FS.open("/tome/f", O.WRONLY);
    const marker = new Uint8Array(PAGE_SIZE);
    marker.fill(0xcd);
    FS.write(fd, marker, 0, PAGE_SIZE, PAGE_SIZE * 5);
    FS.close(fd);

    syncAndUnmount(FS, tomefs);
    const m2 = await mountTome(backend, 4);

    const stat = m2.FS.stat("/tome/f");
    expect(stat.size).toBe(PAGE_SIZE * 6);

    // Page 0 = 0xAB, pages 1-4 = 0x00, page 5 = 0xCD
    const d0 = readFile(m2.FS, "/tome/f", PAGE_SIZE);
    expect(d0[0]).toBe(0xab);

    const d4 = new Uint8Array(PAGE_SIZE);
    const fd2 = m2.FS.open("/tome/f", O.RDONLY);
    m2.FS.read(fd2, d4, 0, PAGE_SIZE, PAGE_SIZE * 4);
    expect(d4[0]).toBe(0);

    const d5 = new Uint8Array(PAGE_SIZE);
    m2.FS.read(fd2, d5, 0, PAGE_SIZE, PAGE_SIZE * 5);
    expect(d5[0]).toBe(0xcd);
    expect(d5[PAGE_SIZE - 1]).toBe(0xcd);
    m2.FS.close(fd2);

    m2.FS.unmount("/tome");
  });
});
