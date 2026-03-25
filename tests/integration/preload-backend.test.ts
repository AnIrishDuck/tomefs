/**
 * Integration tests: tomefs + PreloadBackend — graceful degradation without SAB.
 *
 * Validates the complete no-SAB path where tomefs operates against a
 * PreloadBackend that wraps an async StorageBackend. Data is served
 * synchronously from a preloaded in-memory store, with async flush()
 * for persistence.
 *
 * Also tests the IDB roundtrip: write through tomefs → flush to IDB →
 * re-init from IDB → verify data intact.
 */
import { describe, it, expect, beforeEach } from "vitest";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { MemoryBackend } from "../../src/memory-backend.js";
import { IdbBackend } from "../../src/idb-backend.js";
import { PreloadBackend } from "../../src/preload-backend.js";
import { createTomeFS } from "../../src/tomefs.js";
import { PAGE_SIZE } from "../../src/types.js";
import "fake-indexeddb/auto";

const __dirname = dirname(fileURLToPath(import.meta.url));

/** Load the Emscripten module and mount tomefs with the given PreloadBackend. */
async function mountTomeFS(
  backend: PreloadBackend,
  maxPages = 64,
): Promise<{ FS: any; tomefs: any }> {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  const Module = await createModule();
  const FS = Module.FS;

  const tomefs = createTomeFS(FS, { backend, maxPages });
  FS.mkdir("/data");
  FS.mount(tomefs, {}, "/data");

  return { FS, tomefs };
}

/** Helper to write a text file through Emscripten FS. */
function writeFile(FS: any, path: string, content: string): void {
  const data = new TextEncoder().encode(content);
  const stream = FS.open(path, 64 | 1 | 512, 0o666); // O_CREAT|O_WRONLY|O_TRUNC
  FS.write(stream, data, 0, data.length, 0);
  FS.close(stream);
}

/** Helper to read a text file through Emscripten FS. */
function readFile(FS: any, path: string): string {
  const stat = FS.stat(path);
  const stream = FS.open(path, 0, 0);
  const buf = new Uint8Array(stat.size);
  FS.read(stream, buf, 0, stat.size, 0);
  FS.close(stream);
  return new TextDecoder().decode(buf);
}

describe("tomefs + PreloadBackend (no SAB)", () => {
  describe("with MemoryBackend remote", () => {
    let remote: MemoryBackend;

    beforeEach(() => {
      remote = new MemoryBackend();
    });

    it("@fast basic file I/O through PreloadBackend", async () => {
      const backend = new PreloadBackend(remote);
      await backend.init();
      const { FS } = await mountTomeFS(backend);

      writeFile(FS, "/data/hello.txt", "Hello, no SAB!");
      const content = readFile(FS, "/data/hello.txt");
      expect(content).toBe("Hello, no SAB!");
    });

    it("@fast multi-page file through PreloadBackend", async () => {
      const backend = new PreloadBackend(remote);
      await backend.init();
      const { FS } = await mountTomeFS(backend);

      // Write 4 pages of data
      const totalSize = PAGE_SIZE * 4;
      const data = new Uint8Array(totalSize);
      for (let i = 0; i < totalSize; i++) data[i] = i & 0xff;

      const stream = FS.open("/data/big.dat", 64 | 1 | 512, 0o666);
      FS.write(stream, data, 0, totalSize, 0);
      FS.close(stream);

      // Read back and verify
      const readStream = FS.open("/data/big.dat", 0, 0);
      const buf = new Uint8Array(totalSize);
      FS.read(readStream, buf, 0, totalSize, 0);
      FS.close(readStream);

      for (let i = 0; i < totalSize; i++) {
        expect(buf[i]).toBe(i & 0xff);
      }
    });

    it("@fast flush persists to remote, re-init restores", async () => {
      // Write through tomefs
      const backend1 = new PreloadBackend(remote);
      await backend1.init();
      const { FS: FS1, tomefs: tf1 } = await mountTomeFS(backend1);

      writeFile(FS1, "/data/persist.txt", "durable");
      FS1.mkdir("/data/subdir");
      writeFile(FS1, "/data/subdir/nested.txt", "nested data");

      // Trigger syncfs to persist metadata, then flush PreloadBackend
      await new Promise<void>((resolve, reject) => {
        FS1.syncfs(false, (err: Error | null) => {
          if (err) reject(err);
          else resolve();
        });
      });
      await backend1.flush();

      // Verify remote has the data
      const meta = await remote.readMeta("/persist.txt");
      expect(meta).not.toBeNull();
      expect(meta!.size).toBe(7);

      // Create fresh PreloadBackend + tomefs from same remote
      const backend2 = new PreloadBackend(remote);
      await backend2.init();
      const { FS: FS2 } = await mountTomeFS(backend2);

      // Verify data survives the roundtrip
      const content = readFile(FS2, "/data/persist.txt");
      expect(content).toBe("durable");

      const nested = readFile(FS2, "/data/subdir/nested.txt");
      expect(nested).toBe("nested data");
    });

    it("directory operations", async () => {
      const backend = new PreloadBackend(remote);
      await backend.init();
      const { FS } = await mountTomeFS(backend);

      FS.mkdir("/data/dir1");
      FS.mkdir("/data/dir1/sub");
      writeFile(FS, "/data/dir1/sub/file.txt", "deep");

      const entries = FS.readdir("/data/dir1/sub");
      expect(entries).toContain("file.txt");

      const content = readFile(FS, "/data/dir1/sub/file.txt");
      expect(content).toBe("deep");
    });

    it("unlink and rename", async () => {
      const backend = new PreloadBackend(remote);
      await backend.init();
      const { FS } = await mountTomeFS(backend);

      writeFile(FS, "/data/old.txt", "moving");
      FS.rename("/data/old.txt", "/data/new.txt");

      const content = readFile(FS, "/data/new.txt");
      expect(content).toBe("moving");

      expect(() => FS.stat("/data/old.txt")).toThrow();

      writeFile(FS, "/data/delete-me.txt", "gone");
      FS.unlink("/data/delete-me.txt");
      expect(() => FS.stat("/data/delete-me.txt")).toThrow();
    });

    it("truncate through PreloadBackend", async () => {
      const backend = new PreloadBackend(remote);
      await backend.init();
      const { FS } = await mountTomeFS(backend);

      writeFile(FS, "/data/trunc.txt", "hello world");
      FS.truncate("/data/trunc.txt", 5);

      const content = readFile(FS, "/data/trunc.txt");
      expect(content).toBe("hello");
    });

    it("cache pressure with small cache", async () => {
      const backend = new PreloadBackend(remote);
      await backend.init();
      const { FS } = await mountTomeFS(backend, 4); // tiny cache

      // Write 20 files, each 1 page — exceeds 4-page cache
      for (let i = 0; i < 20; i++) {
        writeFile(FS, `/data/f${i}.txt`, `content-${i}`);
      }

      // Read all back — forces eviction + re-read from PreloadBackend
      for (let i = 0; i < 20; i++) {
        const content = readFile(FS, `/data/f${i}.txt`);
        expect(content).toBe(`content-${i}`);
      }
    });
  });

  describe("with IdbBackend remote", () => {
    it("@fast full IDB roundtrip: write → flush → re-init → read", async () => {
      const dbName = `tomefs-preload-test-${Date.now()}`;
      const idb = new IdbBackend({ dbName });

      // Session 1: write data
      const backend1 = new PreloadBackend(idb);
      await backend1.init();
      const { FS: FS1 } = await mountTomeFS(backend1);

      writeFile(FS1, "/data/idb-test.txt", "persisted to IDB");
      FS1.mkdir("/data/idb-dir");
      writeFile(FS1, "/data/idb-dir/inner.txt", "nested IDB");

      // Persist via syncfs + flush
      await new Promise<void>((resolve, reject) => {
        FS1.syncfs(false, (err: Error | null) => {
          if (err) reject(err);
          else resolve();
        });
      });
      await backend1.flush();

      // Session 2: fresh PreloadBackend on same IDB
      const idb2 = new IdbBackend({ dbName });
      const backend2 = new PreloadBackend(idb2);
      await backend2.init();
      const { FS: FS2 } = await mountTomeFS(backend2);

      const content = readFile(FS2, "/data/idb-test.txt");
      expect(content).toBe("persisted to IDB");

      const nested = readFile(FS2, "/data/idb-dir/inner.txt");
      expect(nested).toBe("nested IDB");
    });

    it("IDB roundtrip with multi-page file", async () => {
      const dbName = `tomefs-preload-multi-${Date.now()}`;
      const idb = new IdbBackend({ dbName });

      const backend1 = new PreloadBackend(idb);
      await backend1.init();
      const { FS: FS1 } = await mountTomeFS(backend1);

      // Write a 3-page file
      const totalSize = PAGE_SIZE * 3;
      const data = new Uint8Array(totalSize);
      for (let i = 0; i < totalSize; i++) data[i] = (i * 7) & 0xff;

      const stream = FS1.open("/data/multi.dat", 64 | 1 | 512, 0o666);
      FS1.write(stream, data, 0, totalSize, 0);
      FS1.close(stream);

      await new Promise<void>((resolve, reject) => {
        FS1.syncfs(false, (err: Error | null) => {
          if (err) reject(err);
          else resolve();
        });
      });
      await backend1.flush();

      // Re-init from IDB
      const idb2 = new IdbBackend({ dbName });
      const backend2 = new PreloadBackend(idb2);
      await backend2.init();
      const { FS: FS2 } = await mountTomeFS(backend2);

      const readStream = FS2.open("/data/multi.dat", 0, 0);
      const buf = new Uint8Array(totalSize);
      FS2.read(readStream, buf, 0, totalSize, 0);
      FS2.close(readStream);

      for (let i = 0; i < totalSize; i++) {
        expect(buf[i]).toBe((i * 7) & 0xff);
      }
    });

    it("IDB roundtrip with cache pressure", async () => {
      const dbName = `tomefs-preload-pressure-${Date.now()}`;
      const idb = new IdbBackend({ dbName });

      const backend1 = new PreloadBackend(idb);
      await backend1.init();
      const { FS: FS1 } = await mountTomeFS(backend1, 4); // tiny cache

      // Write 10 files under tiny cache
      for (let i = 0; i < 10; i++) {
        writeFile(FS1, `/data/p${i}.txt`, `pressured-${i}`);
      }

      await new Promise<void>((resolve, reject) => {
        FS1.syncfs(false, (err: Error | null) => {
          if (err) reject(err);
          else resolve();
        });
      });
      await backend1.flush();

      // Re-init
      const idb2 = new IdbBackend({ dbName });
      const backend2 = new PreloadBackend(idb2);
      await backend2.init();
      const { FS: FS2 } = await mountTomeFS(backend2, 4);

      for (let i = 0; i < 10; i++) {
        const content = readFile(FS2, `/data/p${i}.txt`);
        expect(content).toBe(`pressured-${i}`);
      }
    });
  });
});
