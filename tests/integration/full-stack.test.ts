/**
 * Full-stack integration tests: tomefs → SabClient → SabWorker → MemoryBackend.
 *
 * Validates the complete production path where Emscripten FS operations go
 * through the SAB+Atomics bridge to reach the storage backend. This is the
 * exact path that PGlite will use in production (with IdbBackend instead of
 * MemoryBackend on the async side).
 *
 * Architecture:
 * - Main thread: SabWorker (async side) + MemoryBackend
 * - Worker thread: Emscripten FS → tomefs → SyncPageCache → SabClient (sync side)
 */
import { describe, it, expect, beforeEach, afterEach, beforeAll } from "vitest";
import { Worker } from "node:worker_threads";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { MemoryBackend } from "../../src/memory-backend.js";
import { SabWorker } from "../../src/sab-worker.js";
import { SabClient } from "../../src/sab-client.js";
import { PAGE_SIZE } from "../../src/types.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const WORKER_SRC = join(__dirname, "full-stack-worker.ts");
const WORKER_BUNDLE = join(__dirname, ".full-stack-worker.bundle.mjs");

/** Bundle the worker TS into a single JS file using esbuild. */
async function buildWorkerBundle(): Promise<void> {
  const { buildSync } = await import("esbuild");
  buildSync({
    entryPoints: [WORKER_SRC],
    bundle: true,
    format: "esm",
    platform: "node",
    outfile: WORKER_BUNDLE,
    external: ["node:worker_threads", "node:url", "node:path"],
    // The emscripten_fs.mjs is loaded dynamically, keep it external
    loader: { ".wasm": "file" },
  });
}

/** Send a command to the worker and wait for the result. */
function callWorker(worker: Worker, cmd: string, args: unknown[] = []): Promise<any> {
  return new Promise((resolve, reject) => {
    const id = Math.random();
    const handler = (msg: { id: number; result?: unknown; error?: string }) => {
      if (msg.id !== id) return;
      worker.off("message", handler);
      if (msg.error) reject(new Error(msg.error));
      else resolve(msg.result);
    };
    worker.on("message", handler);
    worker.postMessage({ cmd, args, id });
  });
}

/** Wait for the worker to signal it's ready. */
function waitReady(worker: Worker): Promise<void> {
  return new Promise((resolve, reject) => {
    const handler = (msg: { ready?: boolean; error?: string }) => {
      if (msg.ready) {
        worker.off("message", handler);
        resolve();
      } else if (msg.error) {
        worker.off("message", handler);
        reject(new Error(msg.error));
      }
    };
    worker.on("message", handler);
  });
}

describe("full-stack: tomefs → SAB bridge → MemoryBackend", () => {
  let backend: MemoryBackend;
  let sabWorker: SabWorker;
  let fsWorker: Worker;
  let sab: SharedArrayBuffer;

  beforeAll(async () => {
    await buildWorkerBundle();
  });

  beforeEach(async () => {
    backend = new MemoryBackend();
    sab = SabClient.createBuffer();
    sabWorker = new SabWorker(sab, backend);
    sabWorker.start();

    fsWorker = new Worker(WORKER_BUNDLE, {
      workerData: { sab, maxPages: 64 },
    });
    await waitReady(fsWorker);
  });

  afterEach(async () => {
    sabWorker.stop();
    await fsWorker.terminate();
  });

  describe("basic file I/O through SAB bridge", () => {
    it("@fast write and read a small file", async () => {
      await callWorker(fsWorker, "writeFile", ["/hello.txt", "Hello, tomefs!"]);
      const content = await callWorker(fsWorker, "readFile", ["/hello.txt"]);
      expect(content).toBe("Hello, tomefs!");
    });

    it("@fast overwrite a file and read new content", async () => {
      await callWorker(fsWorker, "writeFile", ["/data.txt", "version 1"]);
      await callWorker(fsWorker, "writeFile", ["/data.txt", "version 2"]);
      const content = await callWorker(fsWorker, "readFile", ["/data.txt"]);
      expect(content).toBe("version 2");
    });

    it("write and read binary data at specific position", async () => {
      await callWorker(fsWorker, "writeBytes", ["/bin", [1, 2, 3, 4, 5]]);
      const bytes = await callWorker(fsWorker, "readBytes", ["/bin", 0, 5]);
      expect(bytes).toEqual([1, 2, 3, 4, 5]);
    });

    it("partial read from middle of file", async () => {
      await callWorker(fsWorker, "writeBytes", ["/data", [10, 20, 30, 40, 50]]);
      const bytes = await callWorker(fsWorker, "readBytes", ["/data", 2, 2]);
      expect(bytes).toEqual([30, 40]);
    });
  });

  describe("multi-page files through SAB bridge", () => {
    it("@fast write and verify a multi-page file (4 pages = 32 KB)", async () => {
      await callWorker(fsWorker, "writeMultiPage", ["/large.dat", 4, 0]);
      const result = await callWorker(fsWorker, "verifyMultiPage", ["/large.dat", 4, 0]);
      expect(result).toEqual({ ok: true });
    });

    it("write a file larger than cache (128 pages, cache = 64)", async () => {
      await callWorker(fsWorker, "writeMultiPage", ["/huge.dat", 128, 42]);
      const result = await callWorker(fsWorker, "verifyMultiPage", ["/huge.dat", 128, 42]);
      expect(result).toEqual({ ok: true });
    });

    it("multiple large files competing for cache space", async () => {
      // 3 files × 32 pages = 96 pages, cache holds 64
      await callWorker(fsWorker, "writeMultiPage", ["/a.dat", 32, 1]);
      await callWorker(fsWorker, "writeMultiPage", ["/b.dat", 32, 2]);
      await callWorker(fsWorker, "writeMultiPage", ["/c.dat", 32, 3]);

      // Read all back — forces eviction and re-fetch through SAB bridge
      const ra = await callWorker(fsWorker, "verifyMultiPage", ["/a.dat", 32, 1]);
      const rb = await callWorker(fsWorker, "verifyMultiPage", ["/b.dat", 32, 2]);
      const rc = await callWorker(fsWorker, "verifyMultiPage", ["/c.dat", 32, 3]);

      expect(ra).toEqual({ ok: true });
      expect(rb).toEqual({ ok: true });
      expect(rc).toEqual({ ok: true });
    });
  });

  describe("directory operations through SAB bridge", () => {
    it("@fast create directories and files", async () => {
      await callWorker(fsWorker, "mkdir", ["/dir1"]);
      await callWorker(fsWorker, "mkdir", ["/dir1/sub"]);
      await callWorker(fsWorker, "writeFile", ["/dir1/sub/file.txt", "nested"]);

      const content = await callWorker(fsWorker, "readFile", ["/dir1/sub/file.txt"]);
      expect(content).toBe("nested");

      const entries = await callWorker(fsWorker, "readdir", ["/dir1/sub"]);
      expect(entries).toContain("file.txt");
      expect(entries).toContain(".");
      expect(entries).toContain("..");
    });

    it("unlink a file", async () => {
      await callWorker(fsWorker, "writeFile", ["/to-delete.txt", "bye"]);
      await callWorker(fsWorker, "unlink", ["/to-delete.txt"]);

      await expect(
        callWorker(fsWorker, "readFile", ["/to-delete.txt"]),
      ).rejects.toThrow();
    });

    it("rename a file preserves content", async () => {
      await callWorker(fsWorker, "writeFile", ["/old.txt", "moved data"]);
      await callWorker(fsWorker, "rename", ["/old.txt", "/new.txt"]);

      const content = await callWorker(fsWorker, "readFile", ["/new.txt"]);
      expect(content).toBe("moved data");

      await expect(
        callWorker(fsWorker, "readFile", ["/old.txt"]),
      ).rejects.toThrow();
    });
  });

  describe("truncate through SAB bridge", () => {
    it("truncate shrinks a file", async () => {
      await callWorker(fsWorker, "writeFile", ["/trunc.txt", "hello world"]);
      await callWorker(fsWorker, "truncate", ["/trunc.txt", 5]);

      const content = await callWorker(fsWorker, "readFile", ["/trunc.txt"]);
      expect(content).toBe("hello");
    });

    it("truncate a multi-page file", async () => {
      await callWorker(fsWorker, "writeMultiPage", ["/big.dat", 8, 0]);
      await callWorker(fsWorker, "truncate", ["/big.dat", PAGE_SIZE * 2]);

      const stat = await callWorker(fsWorker, "stat", ["/big.dat"]);
      expect(stat.size).toBe(PAGE_SIZE * 2);

      // First 2 pages should still be correct
      const result = await callWorker(fsWorker, "verifyMultiPage", ["/big.dat", 2, 0]);
      expect(result).toEqual({ ok: true });
    });
  });

  describe("syncfs persistence through SAB bridge", () => {
    it("@fast syncfs flushes dirty pages to backend", async () => {
      await callWorker(fsWorker, "writeFile", ["/persist.txt", "durable data"]);
      await callWorker(fsWorker, "syncfs", []);

      // Verify backend has the metadata
      const meta = await backend.readMeta("/persist.txt");
      expect(meta).not.toBeNull();
      expect(meta!.size).toBe(12); // "durable data".length

      // Verify backend has the page data
      const page = await backend.readPage("/persist.txt", 0);
      expect(page).not.toBeNull();
      const content = new TextDecoder().decode(page!.subarray(0, 12));
      expect(content).toBe("durable data");
    });

    it("syncfs persists directory tree", async () => {
      await callWorker(fsWorker, "mkdir", ["/mydir"]);
      await callWorker(fsWorker, "writeFile", ["/mydir/inner.txt", "inside"]);
      await callWorker(fsWorker, "syncfs", []);

      const dirMeta = await backend.readMeta("/mydir");
      expect(dirMeta).not.toBeNull();

      const fileMeta = await backend.readMeta("/mydir/inner.txt");
      expect(fileMeta).not.toBeNull();
      expect(fileMeta!.size).toBe(6);
    });
  });

  describe("cache pressure through SAB bridge", () => {
    it("many small files cycling through cache", async () => {
      // Create 100 files, each small (fits in 1 page) but total > cache
      for (let i = 0; i < 100; i++) {
        await callWorker(fsWorker, "writeFile", [`/f${i}.txt`, `content-${i}`]);
      }

      // Read all back
      for (let i = 0; i < 100; i++) {
        const content = await callWorker(fsWorker, "readFile", [`/f${i}.txt`]);
        expect(content).toBe(`content-${i}`);
      }
    });

    it("write-then-read with forced eviction between", async () => {
      // Write a file
      await callWorker(fsWorker, "writeFile", ["/target.txt", "preserved"]);

      // Flood the cache with other files to evict target.txt's pages
      for (let i = 0; i < 80; i++) {
        await callWorker(fsWorker, "writeFile", [`/flood${i}.txt`, `data-${i}`]);
      }

      // Read target back — must re-fetch from backend through SAB bridge
      const content = await callWorker(fsWorker, "readFile", ["/target.txt"]);
      expect(content).toBe("preserved");
    });
  });
});
