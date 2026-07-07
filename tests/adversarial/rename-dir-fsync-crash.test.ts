/**
 * Adversarial tests: directory rename + selective fsync + dirty-shutdown recovery.
 *
 * Combines three dimensions not tested together elsewhere:
 * 1. Directory rename with open descendant FDs (renameDescendantPaths)
 * 2. Selective per-file fsync (some files fsynced, others not)
 * 3. Dirty-shutdown recovery (remount without syncfs)
 *
 * After a directory rename, renameDescendantPaths flushes dirty pages,
 * writes metadata at new paths, and updates each node's storagePath.
 * Subsequent writes through still-open FDs use the updated storagePath.
 * fsync on those FDs must correctly persist pages + metadata at the new
 * path. On dirty shutdown, fsynced files should have all data while
 * non-fsynced files should retain only the state persisted during rename.
 *
 * This is a realistic Postgres pattern: open FDs survive directory renames
 * during tablespace operations, and WAL fsync guarantees must hold.
 *
 * Ethos §9: "Target the seams: metadata updates after flush, dirty flush
 * ordering on concurrent streams, truncate/extend races with dirty pages."
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

function verifyPattern(buf: Uint8Array, size: number, seed: number): void {
  for (let i = 0; i < size; i++) {
    if (buf[i] !== ((seed + i * 31) & 0xff)) {
      throw new Error(
        `Pattern mismatch at offset ${i}: expected ${(seed + i * 31) & 0xff}, got ${buf[i]}`,
      );
    }
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
  return { FS, tomefs };
}

function syncfs(FS: any, tomefs: any) {
  tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
    if (err) throw err;
  });
}

describe("adversarial: dir rename + selective fsync + dirty shutdown", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("fsynced file in renamed dir survives dirty shutdown, non-fsynced does not", async () => {
    const { FS } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/dir`);
    const walFd = FS.open(`${MOUNT}/dir/wal`, O.RDWR | O.CREAT, 0o666);
    const heapFd = FS.open(`${MOUNT}/dir/heap`, O.RDWR | O.CREAT, 0o666);

    const walData1 = encode("wal-before-rename");
    const heapData1 = encode("heap-before-rename");
    FS.write(walFd, walData1, 0, walData1.length, 0);
    FS.write(heapFd, heapData1, 0, heapData1.length, 0);

    FS.rename(`${MOUNT}/dir`, `${MOUNT}/newdir`);

    const walData2 = encode("wal-after-rename!!");
    const heapData2 = encode("heap-after-rename!!");
    FS.write(walFd, walData2, 0, walData2.length, walData1.length);
    FS.write(heapFd, heapData2, 0, heapData2.length, heapData1.length);

    walFd.stream_ops.fsync(walFd);

    FS.close(walFd);
    FS.close(heapFd);

    // Dirty shutdown: no syncfs
    const { FS: FS2 } = await mountTome(backend);

    // WAL was fsynced: all data survives
    const walStat = FS2.stat(`${MOUNT}/newdir/wal`);
    expect(walStat.size).toBe(walData1.length + walData2.length);
    const walBuf = new Uint8Array(walStat.size);
    const walS = FS2.open(`${MOUNT}/newdir/wal`, O.RDONLY);
    FS2.read(walS, walBuf, 0, walBuf.length, 0);
    FS2.close(walS);
    expect(decode(walBuf)).toBe("wal-before-renamewal-after-rename!!");

    // Heap was NOT fsynced: only pre-rename data survives (persisted by renameDescendantPaths)
    const heapStat = FS2.stat(`${MOUNT}/newdir/heap`);
    expect(heapStat.size).toBe(heapData1.length);
    const heapBuf = new Uint8Array(heapStat.size);
    const heapS = FS2.open(`${MOUNT}/newdir/heap`, O.RDONLY);
    FS2.read(heapS, heapBuf, 0, heapBuf.length, 0);
    FS2.close(heapS);
    expect(decode(heapBuf)).toBe("heap-before-rename");
  });

  it("multiple fsyncs after dir rename accumulate correctly", async () => {
    const { FS } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/pg`);
    const fd = FS.open(`${MOUNT}/pg/wal`, O.RDWR | O.CREAT, 0o666);
    const d1 = encode("record-1|");
    FS.write(fd, d1, 0, d1.length, 0);

    FS.rename(`${MOUNT}/pg`, `${MOUNT}/pgdata`);

    const d2 = encode("record-2|");
    FS.write(fd, d2, 0, d2.length, d1.length);
    fd.stream_ops.fsync(fd);

    const d3 = encode("record-3|");
    FS.write(fd, d3, 0, d3.length, d1.length + d2.length);
    fd.stream_ops.fsync(fd);

    const d4 = encode("record-4|");
    FS.write(fd, d4, 0, d4.length, d1.length + d2.length + d3.length);
    // No fsync for record-4

    FS.close(fd);

    // Dirty shutdown
    const { FS: FS2 } = await mountTome(backend);

    const stat = FS2.stat(`${MOUNT}/pgdata/wal`);
    // Records 1-3 were fsynced, record 4 was not
    expect(stat.size).toBe(d1.length + d2.length + d3.length);
    const buf = new Uint8Array(stat.size);
    const s = FS2.open(`${MOUNT}/pgdata/wal`, O.RDONLY);
    FS2.read(s, buf, 0, buf.length, 0);
    FS2.close(s);
    expect(decode(buf)).toBe("record-1|record-2|record-3|");
  });

  it("fsync after dir rename with multi-page data under cache pressure", async () => {
    const { FS } = await mountTome(backend, 4);

    FS.mkdir(`${MOUNT}/tbs`);
    const fd = FS.open(`${MOUNT}/tbs/large`, O.RDWR | O.CREAT, 0o666);

    const preData = fillPattern(PAGE_SIZE * 2, 42);
    FS.write(fd, preData, 0, preData.length, 0);

    FS.rename(`${MOUNT}/tbs`, `${MOUNT}/newtbs`);

    const postData = fillPattern(PAGE_SIZE * 2, 99);
    FS.write(fd, postData, 0, postData.length, preData.length);
    fd.stream_ops.fsync(fd);

    FS.close(fd);

    // Dirty shutdown + remount
    const { FS: FS2 } = await mountTome(backend, 4);

    const stat = FS2.stat(`${MOUNT}/newtbs/large`);
    expect(stat.size).toBe(preData.length + postData.length);

    const buf = new Uint8Array(stat.size);
    const s = FS2.open(`${MOUNT}/newtbs/large`, O.RDONLY);
    FS2.read(s, buf, 0, buf.length, 0);
    FS2.close(s);

    verifyPattern(buf.subarray(0, preData.length), preData.length, 42);
    verifyPattern(
      buf.subarray(preData.length),
      postData.length,
      99,
    );
  });

  it("dir rename + fsync + second rename + fsync preserves data", async () => {
    const { FS } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/a`);
    const fd = FS.open(`${MOUNT}/a/file`, O.RDWR | O.CREAT, 0o666);
    const d1 = encode("phase-1");
    FS.write(fd, d1, 0, d1.length, 0);

    FS.rename(`${MOUNT}/a`, `${MOUNT}/b`);
    const d2 = encode("|phase-2");
    FS.write(fd, d2, 0, d2.length, d1.length);
    fd.stream_ops.fsync(fd);

    FS.rename(`${MOUNT}/b`, `${MOUNT}/c`);
    const d3 = encode("|phase-3");
    FS.write(fd, d3, 0, d3.length, d1.length + d2.length);
    fd.stream_ops.fsync(fd);

    FS.close(fd);

    // Dirty shutdown
    const { FS: FS2 } = await mountTome(backend);

    const stat = FS2.stat(`${MOUNT}/c/file`);
    expect(stat.size).toBe(d1.length + d2.length + d3.length);
    const buf = new Uint8Array(stat.size);
    const s = FS2.open(`${MOUNT}/c/file`, O.RDONLY);
    FS2.read(s, buf, 0, buf.length, 0);
    FS2.close(s);
    expect(decode(buf)).toBe("phase-1|phase-2|phase-3");
  });

  it("mixed fsynced and non-fsynced files across nested dir rename", async () => {
    const { FS } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/db`);
    FS.mkdir(`${MOUNT}/db/base`);

    const walFd = FS.open(`${MOUNT}/db/base/wal`, O.RDWR | O.CREAT, 0o666);
    const catFd = FS.open(`${MOUNT}/db/base/catalog`, O.RDWR | O.CREAT, 0o666);
    const tmpFd = FS.open(`${MOUNT}/db/base/temp`, O.RDWR | O.CREAT, 0o666);

    const walPre = encode("WAL-committed");
    const catPre = encode("catalog-data");
    const tmpPre = encode("temp-data");
    FS.write(walFd, walPre, 0, walPre.length, 0);
    FS.write(catFd, catPre, 0, catPre.length, 0);
    FS.write(tmpFd, tmpPre, 0, tmpPre.length, 0);

    FS.rename(`${MOUNT}/db`, `${MOUNT}/pgdata`);

    const walPost = encode("|WAL-new");
    const catPost = encode("|catalog-new");
    const tmpPost = encode("|temp-new");
    FS.write(walFd, walPost, 0, walPost.length, walPre.length);
    FS.write(catFd, catPost, 0, catPost.length, catPre.length);
    FS.write(tmpFd, tmpPost, 0, tmpPost.length, tmpPre.length);

    // Only fsync WAL — Postgres pattern: WAL is durable, heap/catalog are not
    walFd.stream_ops.fsync(walFd);

    FS.close(walFd);
    FS.close(catFd);
    FS.close(tmpFd);

    // Dirty shutdown
    const { FS: FS2 } = await mountTome(backend);

    // WAL: fsynced, all data survives
    const walBuf = new Uint8Array(walPre.length + walPost.length);
    const ws = FS2.open(`${MOUNT}/pgdata/base/wal`, O.RDONLY);
    FS2.read(ws, walBuf, 0, walBuf.length, 0);
    FS2.close(ws);
    expect(decode(walBuf)).toBe("WAL-committed|WAL-new");

    // Catalog: not fsynced, only pre-rename data
    const catStat = FS2.stat(`${MOUNT}/pgdata/base/catalog`);
    expect(catStat.size).toBe(catPre.length);
    const catBuf = new Uint8Array(catStat.size);
    const cs = FS2.open(`${MOUNT}/pgdata/base/catalog`, O.RDONLY);
    FS2.read(cs, catBuf, 0, catBuf.length, 0);
    FS2.close(cs);
    expect(decode(catBuf)).toBe("catalog-data");

    // Temp: not fsynced, only pre-rename data
    const tmpStat = FS2.stat(`${MOUNT}/pgdata/base/temp`);
    expect(tmpStat.size).toBe(tmpPre.length);
    const tmpBuf = new Uint8Array(tmpStat.size);
    const ts = FS2.open(`${MOUNT}/pgdata/base/temp`, O.RDONLY);
    FS2.read(ts, tmpBuf, 0, tmpBuf.length, 0);
    FS2.close(ts);
    expect(decode(tmpBuf)).toBe("temp-data");
  });

  it("fsync through dup'd fd after dir rename persists correctly", async () => {
    const { FS } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/src`);
    const fd = FS.open(`${MOUNT}/src/data`, O.RDWR | O.CREAT, 0o666);
    const d1 = encode("original");
    FS.write(fd, d1, 0, d1.length, 0);

    const dupFd = FS.dupStream(fd);

    FS.rename(`${MOUNT}/src`, `${MOUNT}/dst`);

    const d2 = encode("-via-dup");
    FS.write(dupFd, d2, 0, d2.length, d1.length);

    // fsync through the dup'd fd
    dupFd.stream_ops.fsync(dupFd);

    FS.close(fd);
    FS.close(dupFd);

    // Dirty shutdown
    const { FS: FS2 } = await mountTome(backend);

    const stat = FS2.stat(`${MOUNT}/dst/data`);
    expect(stat.size).toBe(d1.length + d2.length);
    const buf = new Uint8Array(stat.size);
    const s = FS2.open(`${MOUNT}/dst/data`, O.RDONLY);
    FS2.read(s, buf, 0, buf.length, 0);
    FS2.close(s);
    expect(decode(buf)).toBe("original-via-dup");
  });

  it("truncate after dir rename then fsync preserves truncated size", async () => {
    const { FS } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/d`);
    const fd = FS.open(`${MOUNT}/d/file`, O.RDWR | O.CREAT, 0o666);
    const data = encode("0123456789abcdef");
    FS.write(fd, data, 0, data.length, 0);

    FS.rename(`${MOUNT}/d`, `${MOUNT}/e`);

    FS.ftruncate(fd.fd, 8);
    fd.stream_ops.fsync(fd);

    FS.close(fd);

    // Dirty shutdown
    const { FS: FS2 } = await mountTome(backend);

    const stat = FS2.stat(`${MOUNT}/e/file`);
    expect(stat.size).toBe(8);
    const buf = new Uint8Array(8);
    const s = FS2.open(`${MOUNT}/e/file`, O.RDONLY);
    FS2.read(s, buf, 0, 8, 0);
    FS2.close(s);
    expect(decode(buf)).toBe("01234567");
  });

  it("clean syncfs after dir rename + fsync produces consistent state", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/pre`);
    const walFd = FS.open(`${MOUNT}/pre/wal`, O.RDWR | O.CREAT, 0o666);
    const heapFd = FS.open(`${MOUNT}/pre/heap`, O.RDWR | O.CREAT, 0o666);

    const w1 = encode("wal-init|");
    const h1 = encode("heap-init|");
    FS.write(walFd, w1, 0, w1.length, 0);
    FS.write(heapFd, h1, 0, h1.length, 0);

    FS.rename(`${MOUNT}/pre`, `${MOUNT}/post`);

    const w2 = encode("wal-post|");
    const h2 = encode("heap-post|");
    FS.write(walFd, w2, 0, w2.length, w1.length);
    FS.write(heapFd, h2, 0, h2.length, h1.length);

    walFd.stream_ops.fsync(walFd);

    FS.close(walFd);
    FS.close(heapFd);

    // Clean shutdown (with syncfs)
    syncfs(FS, tomefs);
    FS.unmount(MOUNT);

    // Remount
    const { FS: FS2 } = await mountTome(backend);

    // Both files should have ALL data after clean shutdown
    const walBuf = new Uint8Array(w1.length + w2.length);
    const ws = FS2.open(`${MOUNT}/post/wal`, O.RDONLY);
    FS2.read(ws, walBuf, 0, walBuf.length, 0);
    FS2.close(ws);
    expect(decode(walBuf)).toBe("wal-init|wal-post|");

    const heapBuf = new Uint8Array(h1.length + h2.length);
    const hs = FS2.open(`${MOUNT}/post/heap`, O.RDONLY);
    FS2.read(hs, heapBuf, 0, heapBuf.length, 0);
    FS2.close(hs);
    expect(decode(heapBuf)).toBe("heap-init|heap-post|");
  });

  it("page-boundary-spanning write after dir rename + fsync + crash", async () => {
    const { FS } = await mountTome(backend, 8);

    FS.mkdir(`${MOUNT}/x`);
    const fd = FS.open(`${MOUNT}/x/span`, O.RDWR | O.CREAT, 0o666);

    // Write at end of first page to set up boundary span
    const pos = PAGE_SIZE - 4;
    const preData = new Uint8Array(pos);
    preData.fill(0x41);
    FS.write(fd, preData, 0, preData.length, 0);

    FS.rename(`${MOUNT}/x`, `${MOUNT}/y`);

    // Write across page boundary through renamed path
    const crossData = encode("BOUNDARY");
    FS.write(fd, crossData, 0, crossData.length, pos);
    fd.stream_ops.fsync(fd);

    FS.close(fd);

    // Dirty shutdown
    const { FS: FS2 } = await mountTome(backend, 8);

    const stat = FS2.stat(`${MOUNT}/y/span`);
    expect(stat.size).toBe(pos + crossData.length);

    const buf = new Uint8Array(crossData.length);
    const s = FS2.open(`${MOUNT}/y/span`, O.RDONLY);
    FS2.read(s, buf, 0, crossData.length, pos);
    FS2.close(s);
    expect(decode(buf)).toBe("BOUNDARY");
  });
});
