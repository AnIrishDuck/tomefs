/**
 * Adversarial tests: directory rename with open descendant FDs + persistence.
 *
 * rename-open-fd.test.ts tests file-level renames with open FDs.
 * rename-dir-deep.test.ts tests deep directory renames with persistence.
 * This file targets the untested intersection: when a directory is renamed
 * while files inside it have open FDs, and the data must survive a
 * persistence roundtrip.
 *
 * renameDescendantPaths() rewrites storagePath and clears _pages for all
 * descendant files. If an FD is open to one of those files, subsequent
 * reads/writes go through stream.node, which now has the updated
 * storagePath. Bugs here would cause:
 * - Silent data loss (writes go to the wrong storage path)
 * - Cache incoherence (stale page table references after rename)
 * - Persistence failure (metadata at new path, pages at old path)
 *
 * Ethos §9: "Write tests designed to break tomefs specifically — things
 * that pass against MEMFS but expose real bugs in the page cache layer.
 * Target the seams: reads that span page boundaries, writes during eviction"
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

function syncAndUnmount(FS: any, tomefs: any) {
  tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
    if (err) throw err;
  });
  FS.unmount(MOUNT);
}

function writeFile(FS: any, path: string, content: string) {
  const data = encode(content);
  const s = FS.open(path, O.RDWR | O.CREAT, 0o666);
  FS.write(s, data, 0, data.length, 0);
  FS.close(s);
}

function readFile(FS: any, path: string): string {
  const stat = FS.stat(path);
  const buf = new Uint8Array(stat.size);
  const s = FS.open(path, O.RDONLY);
  const n = FS.read(s, buf, 0, stat.size, 0);
  FS.close(s);
  return decode(buf, n);
}

function writeMultiPageFile(
  FS: any,
  path: string,
  pages: number,
  seed: number,
): Uint8Array {
  const size = PAGE_SIZE * pages;
  const data = new Uint8Array(size);
  for (let i = 0; i < size; i++) data[i] = (i * seed + seed) & 0xff;
  const s = FS.open(path, O.RDWR | O.CREAT, 0o666);
  FS.write(s, data, 0, size, 0);
  FS.close(s);
  return data;
}

describe("adversarial: directory rename with open descendant FDs + persistence", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  // ------------------------------------------------------------------
  // Write through open FD after parent dir rename persists at new path
  // ------------------------------------------------------------------

  it("write through FD after parent dir rename persists correctly @fast", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/dir`);
    writeFile(FS, `${MOUNT}/dir/file.txt`, "original");

    // Open FD, then rename parent directory
    const fd = FS.open(`${MOUNT}/dir/file.txt`, O.RDWR, 0o666);
    FS.rename(`${MOUNT}/dir`, `${MOUNT}/moved`);

    // Write more data through the FD (storagePath was updated by rename)
    const extra = encode(" appended");
    FS.write(fd, extra, 0, extra.length, 8);
    FS.close(fd);

    // Persist and remount
    syncAndUnmount(FS, tomefs);
    const { FS: FS2 } = await mountTome(backend);

    expect(readFile(FS2, `${MOUNT}/moved/file.txt`)).toBe(
      "original appended",
    );
    expect(() => FS2.stat(`${MOUNT}/dir`)).toThrow();
  });

  // ------------------------------------------------------------------
  // Multiple open FDs at different nesting depths survive dir rename
  // ------------------------------------------------------------------

  it("FDs at multiple depth levels survive deep dir rename + persistence", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/root`);
    FS.mkdir(`${MOUNT}/root/sub`);
    FS.mkdir(`${MOUNT}/root/sub/deep`);
    writeFile(FS, `${MOUNT}/root/a.txt`, "level-0");
    writeFile(FS, `${MOUNT}/root/sub/b.txt`, "level-1");
    writeFile(FS, `${MOUNT}/root/sub/deep/c.txt`, "level-2");

    // Open FDs at all three levels
    const fd0 = FS.open(`${MOUNT}/root/a.txt`, O.RDWR, 0o666);
    const fd1 = FS.open(`${MOUNT}/root/sub/b.txt`, O.RDWR, 0o666);
    const fd2 = FS.open(`${MOUNT}/root/sub/deep/c.txt`, O.RDWR, 0o666);

    // Rename the top-level directory
    FS.rename(`${MOUNT}/root`, `${MOUNT}/renamed`);

    // Write through each FD
    const suffix0 = encode("-modified");
    FS.write(fd0, suffix0, 0, suffix0.length, 7);
    const suffix1 = encode("-modified");
    FS.write(fd1, suffix1, 0, suffix1.length, 7);
    const suffix2 = encode("-modified");
    FS.write(fd2, suffix2, 0, suffix2.length, 7);

    FS.close(fd0);
    FS.close(fd1);
    FS.close(fd2);

    syncAndUnmount(FS, tomefs);
    const { FS: FS2 } = await mountTome(backend);

    expect(readFile(FS2, `${MOUNT}/renamed/a.txt`)).toBe("level-0-modified");
    expect(readFile(FS2, `${MOUNT}/renamed/sub/b.txt`)).toBe(
      "level-1-modified",
    );
    expect(readFile(FS2, `${MOUNT}/renamed/sub/deep/c.txt`)).toBe(
      "level-2-modified",
    );
  });

  // ------------------------------------------------------------------
  // Multi-page writes through FD under cache pressure after dir rename
  // ------------------------------------------------------------------

  it("multi-page write through FD after dir rename under 4-page cache @fast", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    FS.mkdir(`${MOUNT}/dir`);
    // Create a 3-page file to fill most of the cache
    const original = writeMultiPageFile(
      FS,
      `${MOUNT}/dir/large.bin`,
      3,
      42,
    );

    const fd = FS.open(`${MOUNT}/dir/large.bin`, O.RDWR, 0o666);

    // Rename parent — this clears _pages and updates storagePath
    FS.rename(`${MOUNT}/dir`, `${MOUNT}/moved`);

    // Write a new page-spanning region through the FD.
    // With 4-page cache, this forces eviction of the original pages
    // and must correctly re-fetch from the new storage path.
    const writeData = new Uint8Array(PAGE_SIZE + 100);
    for (let i = 0; i < writeData.length; i++) writeData[i] = 0xaa;
    FS.write(fd, writeData, 0, writeData.length, PAGE_SIZE - 50);

    // Verify read through FD returns correct merged content
    const readBuf = new Uint8Array(3 * PAGE_SIZE);
    FS.read(fd, readBuf, 0, readBuf.length, 0);

    // First part: original data up to write position
    for (let i = 0; i < PAGE_SIZE - 50; i++) {
      expect(readBuf[i]).toBe(original[i]);
    }
    // Written region: all 0xAA
    for (let i = PAGE_SIZE - 50; i < PAGE_SIZE - 50 + writeData.length; i++) {
      expect(readBuf[i]).toBe(0xaa);
    }
    FS.close(fd);

    // Persist and verify
    syncAndUnmount(FS, tomefs);
    const { FS: FS2 } = await mountTome(backend, 4);

    const stat = FS2.stat(`${MOUNT}/moved/large.bin`);
    // Original file was 3 pages; write at PAGE_SIZE-50 with length PAGE_SIZE+100
    // ends at PAGE_SIZE-50 + PAGE_SIZE+100 = 2*PAGE_SIZE+50 < 3*PAGE_SIZE,
    // so file size stays at original 3 pages.
    expect(stat.size).toBe(3 * PAGE_SIZE);
  });

  // ------------------------------------------------------------------
  // Chain of directory renames (A→B, B→C) with open descendant FDs
  // ------------------------------------------------------------------

  it("FD survives chain of parent dir renames + persistence", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/alpha`);
    writeFile(FS, `${MOUNT}/alpha/data.txt`, "chain-test");

    const fd = FS.open(`${MOUNT}/alpha/data.txt`, O.RDWR, 0o666);

    // Rename A→B
    FS.rename(`${MOUNT}/alpha`, `${MOUNT}/beta`);
    // Rename B→C
    FS.rename(`${MOUNT}/beta`, `${MOUNT}/gamma`);

    // Write through FD — storagePath should reflect the final location
    const update = encode("-survived");
    FS.write(fd, update, 0, update.length, 10);

    // Read back through FD
    const buf = new Uint8Array(50);
    const n = FS.read(fd, buf, 0, 50, 0);
    expect(decode(buf, n)).toBe("chain-test-survived");
    FS.close(fd);

    // Persist and verify
    syncAndUnmount(FS, tomefs);
    const { FS: FS2 } = await mountTome(backend);

    expect(readFile(FS2, `${MOUNT}/gamma/data.txt`)).toBe(
      "chain-test-survived",
    );
    expect(() => FS2.stat(`${MOUNT}/alpha`)).toThrow();
    expect(() => FS2.stat(`${MOUNT}/beta`)).toThrow();
  });

  // ------------------------------------------------------------------
  // Truncate through FD after parent dir rename
  // ------------------------------------------------------------------

  it("ftruncate through FD after dir rename persists correct size", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/dir`);
    writeFile(FS, `${MOUNT}/dir/file.dat`, "abcdefghijklmnop");

    const fd = FS.open(`${MOUNT}/dir/file.dat`, O.RDWR, 0o666);
    FS.rename(`${MOUNT}/dir`, `${MOUNT}/moved`);

    // Truncate through the FD — must update storage at new path
    FS.ftruncate(fd.fd, 5);

    const buf = new Uint8Array(20);
    const n = FS.read(fd, buf, 0, 20, 0);
    expect(decode(buf, n)).toBe("abcde");
    FS.close(fd);

    syncAndUnmount(FS, tomefs);
    const { FS: FS2 } = await mountTome(backend);

    expect(readFile(FS2, `${MOUNT}/moved/file.dat`)).toBe("abcde");
    const stat = FS2.stat(`${MOUNT}/moved/file.dat`);
    expect(stat.size).toBe(5);
  });

  // ------------------------------------------------------------------
  // Multiple files with open FDs in renamed directory
  // ------------------------------------------------------------------

  it("multiple open FDs in renamed dir all persist independently", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/dir`);
    const fileCount = 5;
    const fds: any[] = [];

    for (let i = 0; i < fileCount; i++) {
      writeFile(FS, `${MOUNT}/dir/f${i}.txt`, `file-${i}-original`);
      fds.push(FS.open(`${MOUNT}/dir/f${i}.txt`, O.RDWR, 0o666));
    }

    FS.rename(`${MOUNT}/dir`, `${MOUNT}/moved`);

    // Write different data through each FD
    for (let i = 0; i < fileCount; i++) {
      const update = encode(`-updated-${i}`);
      const originalLen = `file-${i}-original`.length;
      FS.write(fds[i], update, 0, update.length, originalLen);
      FS.close(fds[i]);
    }

    syncAndUnmount(FS, tomefs);
    const { FS: FS2 } = await mountTome(backend);

    for (let i = 0; i < fileCount; i++) {
      expect(readFile(FS2, `${MOUNT}/moved/f${i}.txt`)).toBe(
        `file-${i}-original-updated-${i}`,
      );
    }
  });

  // ------------------------------------------------------------------
  // Dir rename where an unlinked-but-open descendant exists
  // ------------------------------------------------------------------

  it("dir rename with unlinked-but-open descendant preserves both paths", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/dir`);
    writeFile(FS, `${MOUNT}/dir/keep.txt`, "kept");
    writeFile(FS, `${MOUNT}/dir/temp.txt`, "temporary");

    // Open temp.txt, then unlink it (POSIX: fd stays valid)
    const tempFd = FS.open(`${MOUNT}/dir/temp.txt`, O.RDWR, 0o666);
    FS.unlink(`${MOUNT}/dir/temp.txt`);

    // Open keep.txt
    const keepFd = FS.open(`${MOUNT}/dir/keep.txt`, O.RDWR, 0o666);

    // Rename the directory — keep.txt's storagePath updates,
    // temp.txt is already at /__deleted_* and unaffected
    FS.rename(`${MOUNT}/dir`, `${MOUNT}/moved`);

    // Both FDs should still work
    const tempBuf = new Uint8Array(20);
    const tn = FS.read(tempFd, tempBuf, 0, 20, 0);
    expect(decode(tempBuf, tn)).toBe("temporary");

    const keepUpdate = encode("-still-here");
    FS.write(keepFd, keepUpdate, 0, keepUpdate.length, 4);

    FS.close(tempFd);
    FS.close(keepFd);

    syncAndUnmount(FS, tomefs);
    const { FS: FS2 } = await mountTome(backend);

    expect(readFile(FS2, `${MOUNT}/moved/keep.txt`)).toBe("kept-still-here");
    // temp.txt was unlinked — it should not exist
    expect(() => FS2.stat(`${MOUNT}/moved/temp.txt`)).toThrow();
  });

  // ------------------------------------------------------------------
  // Seek position preserved through parent dir rename
  // ------------------------------------------------------------------

  it("seek position in FD preserved through parent dir rename", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/dir`);
    writeFile(FS, `${MOUNT}/dir/seekfile.txt`, "0123456789abcdef");

    const fd = FS.open(`${MOUNT}/dir/seekfile.txt`, O.RDWR, 0o666);
    // Seek to offset 10
    FS.llseek(fd, 10, 0); // SEEK_SET

    FS.rename(`${MOUNT}/dir`, `${MOUNT}/moved`);

    // Write at current position (should be 10)
    const data = encode("XY");
    FS.write(fd, data, 0, 2);

    FS.close(fd);

    syncAndUnmount(FS, tomefs);
    const { FS: FS2 } = await mountTome(backend);

    expect(readFile(FS2, `${MOUNT}/moved/seekfile.txt`)).toBe(
      "0123456789XYcdef",
    );
  });

  // ------------------------------------------------------------------
  // O_APPEND FD through dir rename under cache pressure
  // ------------------------------------------------------------------

  it("append FD through dir rename under cache pressure + persistence", async () => {
    const { FS, tomefs } = await mountTome(backend, 4);

    FS.mkdir(`${MOUNT}/dir`);
    // Write just under 2 pages of initial data
    const initial = new Uint8Array(PAGE_SIZE * 2 - 100);
    for (let i = 0; i < initial.length; i++) initial[i] = 0x41;
    const s = FS.open(`${MOUNT}/dir/append.bin`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, initial, 0, initial.length, 0);
    FS.close(s);

    const fd = FS.open(`${MOUNT}/dir/append.bin`, O.RDWR | O.APPEND, 0o666);

    FS.rename(`${MOUNT}/dir`, `${MOUNT}/moved`);

    // Append data that crosses a page boundary
    const appendData = new Uint8Array(200);
    for (let i = 0; i < appendData.length; i++) appendData[i] = 0xbb;
    FS.write(fd, appendData, 0, appendData.length);

    FS.close(fd);

    syncAndUnmount(FS, tomefs);
    const { FS: FS2 } = await mountTome(backend, 4);

    const stat = FS2.stat(`${MOUNT}/moved/append.bin`);
    expect(stat.size).toBe(initial.length + appendData.length);

    // Verify last bytes are the appended data
    const rd = FS2.open(`${MOUNT}/moved/append.bin`, O.RDONLY);
    const tail = new Uint8Array(200);
    FS2.read(rd, tail, 0, 200, initial.length);
    FS2.close(rd);
    for (let i = 0; i < 200; i++) {
      expect(tail[i]).toBe(0xbb);
    }
  });

  // ------------------------------------------------------------------
  // Dir rename + write through FD + second syncfs cycle
  // ------------------------------------------------------------------

  it("FD write after dir rename survives two sync cycles", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/dir`);
    writeFile(FS, `${MOUNT}/dir/file.txt`, "initial");

    const fd = FS.open(`${MOUNT}/dir/file.txt`, O.RDWR, 0o666);
    FS.rename(`${MOUNT}/dir`, `${MOUNT}/moved`);

    // Write through FD
    const w1 = encode("-w1");
    FS.write(fd, w1, 0, w1.length, 7);

    // First syncfs (FD still open)
    tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
      if (err) throw err;
    });

    // Write more through the same FD
    const w2 = encode("-w2");
    FS.write(fd, w2, 0, w2.length, 10);
    FS.close(fd);

    // Second syncfs + unmount
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    expect(readFile(FS2, `${MOUNT}/moved/file.txt`)).toBe("initial-w1-w2");
  });

  // ------------------------------------------------------------------
  // Dir rename with symlink descendant + open FD on sibling file
  // ------------------------------------------------------------------

  it("dir rename with symlink sibling preserves both across persistence", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/dir`);
    writeFile(FS, `${MOUNT}/dir/real.txt`, "real-data");
    FS.symlink("real.txt", `${MOUNT}/dir/link.txt`);

    const fd = FS.open(`${MOUNT}/dir/real.txt`, O.RDWR, 0o666);
    FS.rename(`${MOUNT}/dir`, `${MOUNT}/moved`);

    const update = encode("-via-fd");
    FS.write(fd, update, 0, update.length, 9);
    FS.close(fd);

    syncAndUnmount(FS, tomefs);
    const { FS: FS2 } = await mountTome(backend);

    // File accessible through both direct path and symlink
    expect(readFile(FS2, `${MOUNT}/moved/real.txt`)).toBe("real-data-via-fd");
    expect(FS2.readlink(`${MOUNT}/moved/link.txt`)).toBe("real.txt");
    expect(readFile(FS2, `${MOUNT}/moved/link.txt`)).toBe("real-data-via-fd");
  });

  // ------------------------------------------------------------------
  // Open FD on grandchild file during nested dir rename
  // ------------------------------------------------------------------

  it("FD on grandchild survives mid-level dir rename + persistence", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/top`);
    FS.mkdir(`${MOUNT}/top/mid`);
    FS.mkdir(`${MOUNT}/top/mid/bottom`);
    writeFile(FS, `${MOUNT}/top/mid/bottom/leaf.txt`, "deep-data");

    const fd = FS.open(
      `${MOUNT}/top/mid/bottom/leaf.txt`,
      O.RDWR,
      0o666,
    );

    // Rename mid-level directory (not the root)
    FS.rename(`${MOUNT}/top/mid`, `${MOUNT}/top/renamed_mid`);

    const update = encode("-updated");
    FS.write(fd, update, 0, update.length, 9);

    // Read back through FD
    const buf = new Uint8Array(50);
    const n = FS.read(fd, buf, 0, 50, 0);
    expect(decode(buf, n)).toBe("deep-data-updated");
    FS.close(fd);

    syncAndUnmount(FS, tomefs);
    const { FS: FS2 } = await mountTome(backend);

    expect(
      readFile(FS2, `${MOUNT}/top/renamed_mid/bottom/leaf.txt`),
    ).toBe("deep-data-updated");
    expect(() => FS2.stat(`${MOUNT}/top/mid`)).toThrow();
  });

  // ------------------------------------------------------------------
  // assertInvariants after dir rename with open FDs under cache pressure
  // ------------------------------------------------------------------

  it("page cache invariants hold after dir rename with open FDs + writes @fast", async () => {
    const { FS, tomefs } = await mountTome(backend, 8);

    FS.mkdir(`${MOUNT}/dir`);
    FS.mkdir(`${MOUNT}/dir/sub`);

    // Create enough files to cause cache pressure with 8-page cache
    for (let i = 0; i < 4; i++) {
      writeMultiPageFile(FS, `${MOUNT}/dir/sub/f${i}.bin`, 2, i + 1);
    }

    // Open FDs on all files
    const fds: any[] = [];
    for (let i = 0; i < 4; i++) {
      fds.push(
        FS.open(`${MOUNT}/dir/sub/f${i}.bin`, O.RDWR, 0o666),
      );
    }

    FS.rename(`${MOUNT}/dir`, `${MOUNT}/moved`);

    // Write through all FDs (triggers cache misses + eviction at new paths)
    for (let i = 0; i < 4; i++) {
      const data = new Uint8Array(PAGE_SIZE);
      data.fill(0x10 + i);
      FS.write(fds[i], data, 0, data.length, 0);
    }

    // Verify page cache internal consistency
    tomefs.assertInvariants();

    for (const fd of fds) FS.close(fd);

    tomefs.assertInvariants();

    syncAndUnmount(FS, tomefs);
    const { FS: FS2, tomefs: tomefs2 } = await mountTome(backend, 8);

    // Verify data persisted correctly
    for (let i = 0; i < 4; i++) {
      const rd = FS2.open(`${MOUNT}/moved/sub/f${i}.bin`, O.RDONLY);
      const buf = new Uint8Array(PAGE_SIZE);
      FS2.read(rd, buf, 0, PAGE_SIZE, 0);
      FS2.close(rd);
      expect(buf[0]).toBe(0x10 + i);
      expect(buf[PAGE_SIZE - 1]).toBe(0x10 + i);
    }

    tomefs2.assertInvariants();
  });

  // ------------------------------------------------------------------
  // Dup'd FD survives dir rename (shared position)
  // ------------------------------------------------------------------

  it("dup'd FD survives parent dir rename with shared position", async () => {
    const { FS, tomefs } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/dir`);
    writeFile(FS, `${MOUNT}/dir/dupfile.txt`, "abcdefghij");

    const fd1 = FS.open(`${MOUNT}/dir/dupfile.txt`, O.RDWR, 0o666);
    const fd2 = FS.dupStream(fd1);

    FS.rename(`${MOUNT}/dir`, `${MOUNT}/moved`);

    // Write through original FD at position 0
    const w1 = encode("XY");
    FS.write(fd1, w1, 0, 2, 0);

    // Read through dup'd FD — should see the modified data
    const buf = new Uint8Array(10);
    const n = FS.read(fd2, buf, 0, 10, 0);
    expect(decode(buf, n)).toBe("XYcdefghij");

    FS.close(fd1);
    FS.close(fd2);

    syncAndUnmount(FS, tomefs);
    const { FS: FS2 } = await mountTome(backend);

    expect(readFile(FS2, `${MOUNT}/moved/dupfile.txt`)).toBe("XYcdefghij");
  });
});
