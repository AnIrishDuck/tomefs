/**
 * Adversarial tests: Same-node rename (rename to same path).
 *
 * POSIX specifies: "If the old argument and the new argument both refer to
 * links to the same existing file, rename() shall return successfully and
 * perform no other action."
 *
 * Emscripten's FS.rename already handles this (early return when
 * old_node === new_node at line 2580 of the runtime). tomefs adds a
 * defense-in-depth check in its own rename node_op: without it, the
 * target cleanup path would set old_node.unlinked=true (since
 * old_node === new_node), causing data loss when the last FD closes.
 *
 * These tests verify the POSIX-required behavior through the Emscripten
 * FS API. The tomefs-level guard is exercised indirectly and exists as
 * protection against future Emscripten changes.
 *
 * Ethos §2: "Real POSIX semantics, not toy coverage"
 * Ethos §9: "Write tests designed to break tomefs specifically"
 */
import {
  createFS,
  encode,
  decode,
  O,
  type FSHarness,
} from "../harness/emscripten-fs.js";
import { createTomeFS } from "../../src/tomefs.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { PAGE_SIZE } from "../../src/types.js";
import { dirname, join } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));

function syncfs(FS: any) {
  FS.syncfs(false, (err: Error | null) => {
    if (err) throw err;
  });
}

describe("adversarial: same-node rename", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  it("rename to same path is a no-op @fast", () => {
    const { FS } = h;
    const data = encode("original content");
    FS.writeFile("/file", data);
    FS.rename("/file", "/file");
    const result = FS.readFile("/file") as Uint8Array;
    expect(decode(result, result.length)).toBe("original content");
  });

  it("rename to same path with open fd preserves data @fast", () => {
    const { FS } = h;
    const data = encode("data with open fd");
    const fd = FS.open("/file", O.RDWR | O.CREAT, 0o666);
    FS.write(fd, data, 0, data.length, 0);

    FS.rename("/file", "/file");

    const buf = new Uint8Array(data.length);
    FS.read(fd, buf, 0, data.length, 0);
    expect(decode(buf, data.length)).toBe("data with open fd");
    FS.close(fd);
  });

  it("write through fd after same-path rename persists @fast", () => {
    const { FS } = h;
    const initial = encode("initial");
    const fd = FS.open("/file", O.RDWR | O.CREAT, 0o666);
    FS.write(fd, initial, 0, initial.length, 0);

    FS.rename("/file", "/file");

    const extra = encode(" plus more");
    FS.write(fd, extra, 0, extra.length, initial.length);
    FS.close(fd);

    const result = FS.readFile("/file") as Uint8Array;
    expect(decode(result, result.length)).toBe("initial plus more");
  });

  it("close after same-path rename does not delete file @fast", () => {
    const { FS } = h;
    const data = encode("must survive close");
    const fd = FS.open("/file", O.RDWR | O.CREAT, 0o666);
    FS.write(fd, data, 0, data.length, 0);

    FS.rename("/file", "/file");
    FS.close(fd);

    const result = FS.readFile("/file") as Uint8Array;
    expect(decode(result, result.length)).toBe("must survive close");
  });

  it("same-path rename preserves metadata @fast", () => {
    const { FS } = h;
    const data = encode("metadata test");
    FS.writeFile("/file", data);
    FS.chmod("/file", 0o644);
    const beforeStat = FS.stat("/file");

    FS.rename("/file", "/file");

    const afterStat = FS.stat("/file");
    expect(afterStat.mode & 0o7777).toBe(beforeStat.mode & 0o7777);
    expect(afterStat.size).toBe(beforeStat.size);
  });
});

describe("adversarial: same-node rename persistence (tomefs)", () => {
  it("data survives syncfs + remount after same-path rename @fast", async () => {
    const { default: createModule } = await import(
      join(__dirname, "../harness/emscripten_fs.mjs")
    );
    const Module = await createModule();
    const FS = Module.FS;
    const backend = new SyncMemoryBackend();
    const tomefs = createTomeFS(FS, { backend, maxPages: 64 });
    FS.mkdir("/tome");
    FS.mount(tomefs, {}, "/tome");

    const data = new Uint8Array(PAGE_SIZE * 2 + 100);
    for (let i = 0; i < data.length; i++) data[i] = (i * 37 + 13) & 0xff;

    FS.writeFile("/tome/file", data);
    FS.rename("/tome/file", "/tome/file");
    syncfs(FS);

    const Module2 = await createModule();
    const FS2 = Module2.FS;
    const tomefs2 = createTomeFS(FS2, { backend, maxPages: 64 });
    FS2.mkdir("/tome");
    FS2.mount(tomefs2, {}, "/tome");

    const stat = FS2.stat("/tome/file");
    expect(stat.size).toBe(data.length);

    const buf = new Uint8Array(stat.size);
    const s = FS2.open("/tome/file", O.RDONLY);
    FS2.read(s, buf, 0, stat.size, 0);
    FS2.close(s);

    for (let i = 0; i < data.length; i++) {
      expect(buf[i]).toBe(data[i]);
    }
  });

  it("no /__deleted_* entries after same-path rename + syncfs @fast", async () => {
    const { default: createModule } = await import(
      join(__dirname, "../harness/emscripten_fs.mjs")
    );
    const Module = await createModule();
    const FS = Module.FS;
    const backend = new SyncMemoryBackend();
    const tomefs = createTomeFS(FS, { backend, maxPages: 64 });
    FS.mkdir("/tome");
    FS.mount(tomefs, {}, "/tome");

    const data = new Uint8Array(PAGE_SIZE);
    data.fill(0x42);
    FS.writeFile("/tome/file", data);
    FS.rename("/tome/file", "/tome/file");
    syncfs(FS);

    const backendFiles = backend.listFiles();
    const deletedEntries = backendFiles.filter((p: string) =>
      p.startsWith("/__deleted_"),
    );
    expect(deletedEntries).toEqual([]);
  });
});
