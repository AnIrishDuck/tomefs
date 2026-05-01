/**
 * Adversarial tests: dup'd FD shared position behavior.
 *
 * POSIX dup(2) creates a new file descriptor that refers to the same open
 * file description. The file offset (position) is shared between the
 * original and dup'd descriptors — a write through one advances the
 * position seen by the other. Emscripten's FS.dupStream() preserves this
 * semantics.
 *
 * These tests verify that writes through dup'd FDs correctly share the
 * file position and that the resulting file contents survive syncfs +
 * remount under cache pressure. This catches model errors in fuzz tests
 * and implementation bugs where dup'd streams might have independent
 * positions.
 *
 * Ethos §2: "Real POSIX semantics, not toy coverage"
 * Ethos §9: "Write tests designed to break tomefs specifically"
 */
import {
  createFS,
  encode,
  decode,
  O,
  SEEK_SET,
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

describe("adversarial: dup'd FD shared position", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  it("write through fd1 advances fd2 position @fast", () => {
    const { FS } = h;
    FS.writeFile("/file", new Uint8Array(100));
    const fd1 = FS.open("/file", O.RDWR);
    const fd2 = FS.dupStream(fd1);

    const data = new Uint8Array([42, 43, 44]);
    FS.write(fd1, data, 0, 3);

    expect(fd1.position).toBe(3);
    expect(fd2.position).toBe(3);

    FS.close(fd2);
    FS.close(fd1);
  });

  it("interleaved writes through dup'd fds produce correct data @fast", () => {
    const { FS } = h;
    FS.writeFile("/file", new Uint8Array(100));
    const fd1 = FS.open("/file", O.RDWR);
    const fd2 = FS.dupStream(fd1);

    FS.write(fd1, new Uint8Array([10, 11, 12]), 0, 3);
    FS.write(fd2, new Uint8Array([20, 21, 22]), 0, 3);

    const result = FS.readFile("/file") as Uint8Array;
    expect(result[0]).toBe(10);
    expect(result[1]).toBe(11);
    expect(result[2]).toBe(12);
    expect(result[3]).toBe(20);
    expect(result[4]).toBe(21);
    expect(result[5]).toBe(22);

    FS.close(fd2);
    FS.close(fd1);
  });

  it("seek on fd1 affects fd2 position @fast", () => {
    const { FS } = h;
    FS.writeFile("/file", new Uint8Array(100));
    const fd1 = FS.open("/file", O.RDWR);
    const fd2 = FS.dupStream(fd1);

    FS.llseek(fd1, 50, SEEK_SET);
    expect(fd2.position).toBe(50);

    FS.write(fd2, new Uint8Array([99]), 0, 1);
    expect(fd1.position).toBe(51);

    const result = FS.readFile("/file") as Uint8Array;
    expect(result[50]).toBe(99);

    FS.close(fd2);
    FS.close(fd1);
  });

  it("triple dup chain shares position @fast", () => {
    const { FS } = h;
    FS.writeFile("/file", new Uint8Array(100));
    const fd1 = FS.open("/file", O.RDWR);
    const fd2 = FS.dupStream(fd1);
    const fd3 = FS.dupStream(fd2);

    FS.write(fd1, new Uint8Array([1]), 0, 1);
    expect(fd2.position).toBe(1);
    expect(fd3.position).toBe(1);

    FS.write(fd3, new Uint8Array([2]), 0, 1);
    expect(fd1.position).toBe(2);
    expect(fd2.position).toBe(2);

    FS.close(fd3);
    FS.close(fd2);
    FS.close(fd1);
  });
});

describe("adversarial: dup'd FD shared position persistence (tomefs)", () => {
  it("interleaved dup writes survive syncfs + remount @fast", async () => {
    const { default: createModule } = await import(
      join(__dirname, "../harness/emscripten_fs.mjs")
    );
    const backend = new SyncMemoryBackend();
    const Module1 = await createModule();
    const FS1 = Module1.FS;
    const tomefs1 = createTomeFS(FS1, { backend, maxPages: 4 });
    FS1.mkdir("/tome");
    FS1.mount(tomefs1, {}, "/tome");

    FS1.writeFile("/tome/file", new Uint8Array(PAGE_SIZE * 2));
    const fd1 = FS1.open("/tome/file", O.RDWR);
    const fd2 = FS1.dupStream(fd1);

    const data1 = new Uint8Array(PAGE_SIZE);
    data1.fill(0xAA);
    FS1.write(fd1, data1, 0, data1.length);

    const data2 = new Uint8Array(PAGE_SIZE);
    data2.fill(0xBB);
    FS1.write(fd2, data2, 0, data2.length);

    FS1.close(fd2);
    FS1.close(fd1);
    syncfs(FS1);

    const Module2 = await createModule();
    const FS2 = Module2.FS;
    const tomefs2 = createTomeFS(FS2, { backend, maxPages: 4 });
    FS2.mkdir("/tome");
    FS2.mount(tomefs2, {}, "/tome");

    const result = FS2.readFile("/tome/file") as Uint8Array;
    for (let i = 0; i < PAGE_SIZE; i++) {
      expect(result[i]).toBe(0xAA);
    }
    for (let i = PAGE_SIZE; i < PAGE_SIZE * 2; i++) {
      expect(result[i]).toBe(0xBB);
    }
  });

  it("dup write + ftruncate + write survives remount under cache pressure @fast", async () => {
    const { default: createModule } = await import(
      join(__dirname, "../harness/emscripten_fs.mjs")
    );
    const backend = new SyncMemoryBackend();
    const Module1 = await createModule();
    const FS1 = Module1.FS;
    const tomefs1 = createTomeFS(FS1, { backend, maxPages: 4 });
    FS1.mkdir("/tome");
    FS1.mount(tomefs1, {}, "/tome");

    const initial = new Uint8Array(PAGE_SIZE);
    initial.fill(51);
    FS1.writeFile("/tome/file", initial);

    const fd1 = FS1.open("/tome/file", O.RDWR);
    const fd2 = FS1.dupStream(fd1);

    const write1 = new Uint8Array(100);
    write1.fill(107);
    FS1.write(fd1, write1, 0, write1.length);

    FS1.ftruncate(fd1.fd, PAGE_SIZE * 5);

    const write2 = new Uint8Array(PAGE_SIZE);
    write2.fill(200);
    FS1.write(fd2, write2, 0, write2.length);

    FS1.close(fd2);
    FS1.close(fd1);
    syncfs(FS1);

    const Module2 = await createModule();
    const FS2 = Module2.FS;
    const tomefs2 = createTomeFS(FS2, { backend, maxPages: 4 });
    FS2.mkdir("/tome");
    FS2.mount(tomefs2, {}, "/tome");

    const result = FS2.readFile("/tome/file") as Uint8Array;
    for (let i = 0; i < 100; i++) {
      expect(result[i]).toBe(107);
    }
    for (let i = 100; i < 100 + PAGE_SIZE; i++) {
      expect(result[i]).toBe(200);
    }
  });
});
