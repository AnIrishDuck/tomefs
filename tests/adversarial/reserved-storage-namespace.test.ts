/**
 * Adversarial: user paths that collide with tomefs's reserved backend
 * namespace.
 *
 * tomefs reserves backend keys whose first component starts with two
 * underscores — "/__tomefs_clean" for the clean-shutdown marker and
 * "/__deleted_<n>" for the tombstones minted by rename-over-target and
 * unlink-with-open-fds. Those are also legal POSIX filenames, so nothing
 * stops a caller from creating "<mount>/__tomefs_clean". Before storage
 * paths were escaped, such a file claimed the reserved key outright:
 * restoreTree classified it as an internal marker, deleted it, and left
 * its pages orphaned — total, silent data loss with no error raised at
 * any point in the write or sync path.
 *
 * These tests pin the escaping that keeps user paths out of that namespace.
 */
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { describe, it, expect, beforeEach } from "vitest";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { createTomeFS } from "../../src/tomefs.js";
import { PAGE_SIZE } from "../../src/types.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const MOUNT = "/tome";

const O = {
  RDONLY: 0,
  WRONLY: 1,
  RDWR: 2,
  CREAT: 64,
  TRUNC: 512,
} as const;

/** Backend keys tomefs reserves for its own bookkeeping. */
const RESERVED = /^\/__(tomefs_clean|deleted_\d+|root_\d+)$/;

function encode(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

function decode(b: Uint8Array, n?: number): string {
  return new TextDecoder().decode(n !== undefined ? b.subarray(0, n) : b);
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

function syncAndUnmount(FS: any, tomefs: any) {
  syncfs(FS, tomefs);
  FS.unmount(MOUNT);
}

function writeFile(FS: any, path: string, data: string) {
  const s = FS.open(path, O.RDWR | O.CREAT | O.TRUNC, 0o666);
  const bytes = encode(data);
  FS.write(s, bytes, 0, bytes.length);
  FS.close(s);
}

function readFile(FS: any, path: string): string {
  const size = FS.stat(path).size;
  const s = FS.open(path, O.RDONLY);
  const buf = new Uint8Array(size);
  const n = FS.read(s, buf, 0, size);
  FS.close(s);
  return decode(buf, n);
}

function entriesOf(FS: any, dir: string): string[] {
  return FS.readdir(dir)
    .filter((e: string) => e !== "." && e !== "..")
    .sort();
}

describe("reserved storage namespace", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  // ------------------------------------------------------------------
  // Round-trip: each reserved-looking name survives a remount
  // ------------------------------------------------------------------

  for (const name of [
    "__tomefs_clean",
    "__deleted_0",
    "__deleted_17",
    "__root_0",
  ]) {
    it(`file named ${name} survives remount with data intact`, async () => {
      const { FS, tomefs } = await mountTome(backend);
      writeFile(FS, `${MOUNT}/${name}`, `payload for ${name}`);
      syncAndUnmount(FS, tomefs);

      const { FS: FS2 } = await mountTome(backend);
      expect(entriesOf(FS2, MOUNT)).toContain(name);
      expect(readFile(FS2, `${MOUNT}/${name}`)).toBe(`payload for ${name}`);
    });
  }

  it("directory named __tomefs_clean survives remount with its children", async () => {
    const { FS, tomefs } = await mountTome(backend);
    FS.mkdir(`${MOUNT}/__tomefs_clean`);
    writeFile(FS, `${MOUNT}/__tomefs_clean/inner`, "nested data");
    FS.mkdir(`${MOUNT}/__tomefs_clean/sub`);
    writeFile(FS, `${MOUNT}/__tomefs_clean/sub/deep`, "deep data");
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    expect(FS2.isDir(FS2.stat(`${MOUNT}/__tomefs_clean`).mode)).toBe(true);
    expect(entriesOf(FS2, `${MOUNT}/__tomefs_clean`)).toEqual(["inner", "sub"]);
    expect(readFile(FS2, `${MOUNT}/__tomefs_clean/inner`)).toBe("nested data");
    expect(readFile(FS2, `${MOUNT}/__tomefs_clean/sub/deep`)).toBe("deep data");
  });

  it("symlink named __deleted_3 survives remount", async () => {
    const { FS, tomefs } = await mountTome(backend);
    writeFile(FS, `${MOUNT}/target`, "target data");
    FS.symlink("target", `${MOUNT}/__deleted_3`);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    expect(FS2.readlink(`${MOUNT}/__deleted_3`)).toBe("target");
    expect(readFile(FS2, `${MOUNT}/__deleted_3`)).toBe("target data");
  });

  // ------------------------------------------------------------------
  // The escape must be injective — escaped names must not collide with
  // each other or with names that already carry extra underscores.
  // ------------------------------------------------------------------

  it("names differing only in leading underscores stay distinct", async () => {
    const names = [
      "_tomefs_clean",
      "__tomefs_clean",
      "___tomefs_clean",
      "____tomefs_clean",
    ];
    const { FS, tomefs } = await mountTome(backend);
    for (const n of names) writeFile(FS, `${MOUNT}/${n}`, `data:${n}`);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    expect(entriesOf(FS2, MOUNT)).toEqual([...names].sort());
    for (const n of names) {
      expect(readFile(FS2, `${MOUNT}/${n}`)).toBe(`data:${n}`);
    }
  });

  it("reserved-looking names below the mount root are left alone", async () => {
    const { FS, tomefs } = await mountTome(backend);
    FS.mkdir(`${MOUNT}/dir`);
    writeFile(FS, `${MOUNT}/dir/__tomefs_clean`, "not at root");
    writeFile(FS, `${MOUNT}/dir/__deleted_0`, "also not at root");
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    expect(entriesOf(FS2, `${MOUNT}/dir`)).toEqual([
      "__deleted_0",
      "__tomefs_clean",
    ]);
    expect(readFile(FS2, `${MOUNT}/dir/__tomefs_clean`)).toBe("not at root");
    expect(readFile(FS2, `${MOUNT}/dir/__deleted_0`)).toBe("also not at root");
  });

  // ------------------------------------------------------------------
  // Structural: no user file may ever occupy a reserved backend key
  // ------------------------------------------------------------------

  it("no user file claims a reserved backend key", async () => {
    const { FS, tomefs } = await mountTome(backend);
    writeFile(FS, `${MOUNT}/__tomefs_clean`, "a");
    writeFile(FS, `${MOUNT}/__deleted_0`, "b");
    FS.mkdir(`${MOUNT}/__deleted_1`);
    writeFile(FS, `${MOUNT}/__deleted_1/c`, "c");
    syncfs(FS, tomefs);

    // The only reserved key present is the clean-shutdown marker tomefs
    // wrote itself; every user path escaped clear of the namespace.
    const reserved = backend.listFiles().filter((p) => RESERVED.test(p));
    expect(reserved).toEqual(["/__tomefs_clean"]);
    FS.unmount(MOUNT);
  });

  it("a user file survives a tombstone minted at its own name", async () => {
    const { FS, tomefs } = await mountTome(backend);

    // Mint one tombstone to observe where the (module-global) counter sits.
    writeFile(FS, `${MOUNT}/victim1`, "victim1");
    const fd1 = FS.open(`${MOUNT}/victim1`, O.RDONLY);
    FS.unlink(`${MOUNT}/victim1`);
    syncfs(FS, tomefs);
    const minted = backend
      .listFiles()
      .filter((p) => /^\/__deleted_\d+$/.test(p));
    expect(minted).toHaveLength(1);
    const next = Number(minted[0].slice("/__deleted_".length)) + 1;

    // Claim the name the *next* tombstone will use, then mint it.
    writeFile(FS, `${MOUNT}/__deleted_${next}`, "user payload");
    writeFile(FS, `${MOUNT}/victim2`, "victim2");
    const fd2 = FS.open(`${MOUNT}/victim2`, O.RDONLY);
    FS.unlink(`${MOUNT}/victim2`);

    // Both unlinked files are still readable through their open fds.
    expect(readFile(FS, `${MOUNT}/__deleted_${next}`)).toBe("user payload");
    FS.close(fd1);
    FS.close(fd2);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    expect(entriesOf(FS2, MOUNT)).toEqual([`__deleted_${next}`]);
    expect(readFile(FS2, `${MOUNT}/__deleted_${next}`)).toBe("user payload");
  });

  // ------------------------------------------------------------------
  // Renames across the namespace boundary
  // ------------------------------------------------------------------

  it("renaming a normal file to a reserved name survives remount", async () => {
    const { FS, tomefs } = await mountTome(backend);
    writeFile(FS, `${MOUNT}/plain`, "moved data");
    syncfs(FS, tomefs);
    FS.rename(`${MOUNT}/plain`, `${MOUNT}/__tomefs_clean`);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    expect(entriesOf(FS2, MOUNT)).toEqual(["__tomefs_clean"]);
    expect(readFile(FS2, `${MOUNT}/__tomefs_clean`)).toBe("moved data");
  });

  it("renaming a reserved-named file to a normal name survives remount", async () => {
    const { FS, tomefs } = await mountTome(backend);
    writeFile(FS, `${MOUNT}/__deleted_0`, "moved back");
    syncfs(FS, tomefs);
    FS.rename(`${MOUNT}/__deleted_0`, `${MOUNT}/plain`);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    expect(entriesOf(FS2, MOUNT)).toEqual(["plain"]);
    expect(readFile(FS2, `${MOUNT}/plain`)).toBe("moved back");
  });

  it("renaming a reserved-named directory moves its descendants", async () => {
    const { FS, tomefs } = await mountTome(backend);
    FS.mkdir(`${MOUNT}/__tomefs_clean`);
    writeFile(FS, `${MOUNT}/__tomefs_clean/a`, "aaa");
    writeFile(FS, `${MOUNT}/__tomefs_clean/b`, "bbb");
    syncfs(FS, tomefs);
    FS.rename(`${MOUNT}/__tomefs_clean`, `${MOUNT}/renamed`);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    expect(entriesOf(FS2, MOUNT)).toEqual(["renamed"]);
    expect(entriesOf(FS2, `${MOUNT}/renamed`)).toEqual(["a", "b"]);
    expect(readFile(FS2, `${MOUNT}/renamed/a`)).toBe("aaa");
    expect(readFile(FS2, `${MOUNT}/renamed/b`)).toBe("bbb");
  });

  it("renaming a normal directory to a reserved name moves its descendants", async () => {
    const { FS, tomefs } = await mountTome(backend);
    FS.mkdir(`${MOUNT}/plain`);
    writeFile(FS, `${MOUNT}/plain/a`, "aaa");
    FS.mkdir(`${MOUNT}/plain/sub`);
    writeFile(FS, `${MOUNT}/plain/sub/b`, "bbb");
    syncfs(FS, tomefs);
    FS.rename(`${MOUNT}/plain`, `${MOUNT}/__deleted_0`);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend);
    expect(entriesOf(FS2, MOUNT)).toEqual(["__deleted_0"]);
    expect(readFile(FS2, `${MOUNT}/__deleted_0/a`)).toBe("aaa");
    expect(readFile(FS2, `${MOUNT}/__deleted_0/sub/b`)).toBe("bbb");
  });

  // ------------------------------------------------------------------
  // Page-level integrity under cache pressure
  // ------------------------------------------------------------------

  it("multi-page reserved-named file round-trips under cache pressure", async () => {
    const pages = 6;
    const size = PAGE_SIZE * pages;
    const { FS, tomefs } = await mountTome(backend, 2);
    const s = FS.open(`${MOUNT}/__tomefs_clean`, O.RDWR | O.CREAT, 0o666);
    const buf = new Uint8Array(size);
    for (let i = 0; i < size; i++) buf[i] = (i * 31 + 7) & 0xff;
    FS.write(s, buf, 0, size);
    FS.close(s);
    syncAndUnmount(FS, tomefs);

    const { FS: FS2 } = await mountTome(backend, 2);
    expect(FS2.stat(`${MOUNT}/__tomefs_clean`).size).toBe(size);
    const s2 = FS2.open(`${MOUNT}/__tomefs_clean`, O.RDONLY);
    const out = new Uint8Array(size);
    expect(FS2.read(s2, out, 0, size)).toBe(size);
    FS2.close(s2);
    expect(out).toEqual(buf);
  });

  // ------------------------------------------------------------------
  // The reserved keys must still do their own job
  // ------------------------------------------------------------------

  it("clean-marker fast path still works alongside a reserved-named file", async () => {
    const { FS, tomefs } = await mountTome(backend);
    writeFile(FS, `${MOUNT}/__tomefs_clean`, "user data");
    writeAndSyncTwice(FS, tomefs);
    syncAndUnmount(FS, tomefs);

    // A clean marker was written, so the next mount trusts the backend
    // and takes the incremental path instead of a full-tree reconciliation.
    const { FS: FS2, tomefs: t2 } = await mountTome(backend);
    expect(readFile(FS2, `${MOUNT}/__tomefs_clean`)).toBe("user data");
    t2.resetStats();
    writeFile(FS2, `${MOUNT}/other`, "x");
    syncfs(FS2, t2);
    expect(t2.getStats().fullTreeSyncs).toBe(0);
    expect(t2.getStats().incrementalSyncs).toBe(1);
    FS2.unmount(MOUNT);
  });

  it("orphan cleanup does not delete a reserved-named file", async () => {
    const { FS, tomefs } = await mountTome(backend);
    writeFile(FS, `${MOUNT}/__tomefs_clean`, "keep me");
    writeFile(FS, `${MOUNT}/scratch`, "delete me");
    syncfs(FS, tomefs);

    // Unlinking forces the next syncfs onto the full-tree path, which
    // reconciles backend keys against the live tree and deletes orphans.
    FS.unlink(`${MOUNT}/scratch`);
    syncfs(FS, tomefs);
    expect(tomefs.getStats().fullTreeSyncs).toBeGreaterThan(0);
    FS.unmount(MOUNT);

    const { FS: FS2 } = await mountTome(backend);
    expect(entriesOf(FS2, MOUNT)).toEqual(["__tomefs_clean"]);
    expect(readFile(FS2, `${MOUNT}/__tomefs_clean`)).toBe("keep me");
  });
});

/** Two write+sync cycles, leaving the backend with a fresh clean marker. */
function writeAndSyncTwice(FS: any, tomefs: any) {
  syncfs(FS, tomefs);
  writeFile(FS, `${MOUNT}/__tomefs_clean`, "user data");
  syncfs(FS, tomefs);
}
