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

  // -------------------------------------------------------------------
  // Persistence across remount: destroy FS worker, start a new one,
  // verify data survives restoreTree through the full SAB bridge chain.
  //
  // This is the actual production restart path: PGlite tab closes →
  // Emscripten module destroyed → tab reopens → new Emscripten module
  // mounts tomefs → restoreTree reads from backend through SAB bridge.
  //
  // All other persistence tests (adversarial, workload, fuzz) use
  // SyncMemoryBackend directly, bypassing the SAB bridge. These tests
  // are the only ones exercising the full production persistence path.
  // -------------------------------------------------------------------

  /**
   * Terminate the current FS worker and start a fresh one with the same
   * SAB + backend. The new worker creates a new Emscripten module, mounts
   * tomefs, and restoreTree rebuilds the filesystem from backend metadata
   * through the SabClient → SabWorker → MemoryBackend chain.
   */
  async function remount(maxPages?: number): Promise<void> {
    await fsWorker.terminate();
    fsWorker = new Worker(WORKER_BUNDLE, {
      workerData: { sab, maxPages: maxPages ?? 64 },
    });
    await waitReady(fsWorker);
  }

  describe("persistence across remount (full SAB bridge roundtrip)", () => {
    it("@fast basic file persists across remount", async () => {
      await callWorker(fsWorker, "writeFile", ["/persist.txt", "survives restart"]);
      await callWorker(fsWorker, "syncfs", []);
      await remount();

      const content = await callWorker(fsWorker, "readFile", ["/persist.txt"]);
      expect(content).toBe("survives restart");
    });

    it("@fast multi-page file persists across remount", async () => {
      await callWorker(fsWorker, "writeMultiPage", ["/large.dat", 8, 42]);
      await callWorker(fsWorker, "syncfs", []);
      await remount();

      const result = await callWorker(fsWorker, "verifyMultiPage", ["/large.dat", 8, 42]);
      expect(result).toEqual({ ok: true });

      const stat = await callWorker(fsWorker, "stat", ["/large.dat"]);
      expect(stat.size).toBe(8 * PAGE_SIZE);
    });

    it("@fast directory tree persists across remount", async () => {
      await callWorker(fsWorker, "mkdir", ["/db"]);
      await callWorker(fsWorker, "mkdir", ["/db/base"]);
      await callWorker(fsWorker, "mkdir", ["/db/base/1"]);
      await callWorker(fsWorker, "writeFile", ["/db/base/1/pg_class", "catalog data"]);
      await callWorker(fsWorker, "writeFile", ["/db/base/1/pg_type", "type data"]);
      await callWorker(fsWorker, "writeFile", ["/db/wal.log", "wal content"]);
      await callWorker(fsWorker, "syncfs", []);
      await remount();

      // Verify directory structure
      const dbEntries = await callWorker(fsWorker, "readdir", ["/db"]);
      expect(dbEntries).toContain("base");
      expect(dbEntries).toContain("wal.log");

      const baseEntries = await callWorker(fsWorker, "readdir", ["/db/base/1"]);
      expect(baseEntries).toContain("pg_class");
      expect(baseEntries).toContain("pg_type");

      // Verify file contents
      expect(await callWorker(fsWorker, "readFile", ["/db/base/1/pg_class"])).toBe("catalog data");
      expect(await callWorker(fsWorker, "readFile", ["/db/base/1/pg_type"])).toBe("type data");
      expect(await callWorker(fsWorker, "readFile", ["/db/wal.log"])).toBe("wal content");
    });

    it("@fast many files under cache pressure persist across remount", async () => {
      // Write 100 files — total pages exceed the 64-page cache
      for (let i = 0; i < 100; i++) {
        await callWorker(fsWorker, "writeFile", [`/f${i}.txt`, `value-${i}`]);
      }
      await callWorker(fsWorker, "syncfs", []);
      await remount();

      // Every file must survive the full roundtrip
      for (let i = 0; i < 100; i++) {
        const content = await callWorker(fsWorker, "readFile", [`/f${i}.txt`]);
        expect(content).toBe(`value-${i}`);
      }
    });

    it("rename persists across remount", async () => {
      await callWorker(fsWorker, "writeFile", ["/original.txt", "moved data"]);
      await callWorker(fsWorker, "rename", ["/original.txt", "/renamed.txt"]);
      await callWorker(fsWorker, "syncfs", []);
      await remount();

      const content = await callWorker(fsWorker, "readFile", ["/renamed.txt"]);
      expect(content).toBe("moved data");

      // Original path must not exist
      await expect(
        callWorker(fsWorker, "readFile", ["/original.txt"]),
      ).rejects.toThrow();
    });

    it("unlink + create at same path persists across remount", async () => {
      await callWorker(fsWorker, "writeFile", ["/reuse.txt", "old data"]);
      await callWorker(fsWorker, "syncfs", []);

      await callWorker(fsWorker, "unlink", ["/reuse.txt"]);
      await callWorker(fsWorker, "writeFile", ["/reuse.txt", "new data"]);
      await callWorker(fsWorker, "syncfs", []);
      await remount();

      const content = await callWorker(fsWorker, "readFile", ["/reuse.txt"]);
      expect(content).toBe("new data");
    });

    it("truncate persists across remount", async () => {
      await callWorker(fsWorker, "writeMultiPage", ["/trunc.dat", 8, 0]);
      await callWorker(fsWorker, "truncate", ["/trunc.dat", PAGE_SIZE * 3]);
      await callWorker(fsWorker, "syncfs", []);
      await remount();

      const stat = await callWorker(fsWorker, "stat", ["/trunc.dat"]);
      expect(stat.size).toBe(PAGE_SIZE * 3);

      // First 3 pages should have original data
      const result = await callWorker(fsWorker, "verifyMultiPage", ["/trunc.dat", 3, 0]);
      expect(result).toEqual({ ok: true });
    });

    it("multiple syncfs cycles then remount preserves final state", async () => {
      // Cycle 1: create files
      await callWorker(fsWorker, "writeFile", ["/a.txt", "alpha"]);
      await callWorker(fsWorker, "writeFile", ["/b.txt", "beta"]);
      await callWorker(fsWorker, "syncfs", []);

      // Cycle 2: modify one, delete another, add new
      await callWorker(fsWorker, "writeFile", ["/a.txt", "alpha-v2"]);
      await callWorker(fsWorker, "unlink", ["/b.txt"]);
      await callWorker(fsWorker, "writeFile", ["/c.txt", "gamma"]);
      await callWorker(fsWorker, "syncfs", []);

      // Cycle 3: rename
      await callWorker(fsWorker, "rename", ["/c.txt", "/d.txt"]);
      await callWorker(fsWorker, "syncfs", []);

      await remount();

      expect(await callWorker(fsWorker, "readFile", ["/a.txt"])).toBe("alpha-v2");
      expect(await callWorker(fsWorker, "readFile", ["/d.txt"])).toBe("gamma");
      await expect(callWorker(fsWorker, "readFile", ["/b.txt"])).rejects.toThrow();
      await expect(callWorker(fsWorker, "readFile", ["/c.txt"])).rejects.toThrow();
    });

    it("multi-page files under cache pressure persist across remount", async () => {
      // 4 files × 32 pages = 128 pages, cache holds 64
      for (let i = 0; i < 4; i++) {
        await callWorker(fsWorker, "writeMultiPage", [`/big${i}.dat`, 32, i * 10]);
      }
      await callWorker(fsWorker, "syncfs", []);
      await remount();

      for (let i = 0; i < 4; i++) {
        const result = await callWorker(fsWorker, "verifyMultiPage", [`/big${i}.dat`, 32, i * 10]);
        expect(result).toEqual({ ok: true });
      }
    });

    it("incremental modifications between sync cycles persist", async () => {
      // Create 10 multi-page files
      for (let i = 0; i < 10; i++) {
        await callWorker(fsWorker, "writeMultiPage", [`/file${i}.dat`, 4, i]);
      }
      await callWorker(fsWorker, "syncfs", []);

      // Modify only files 3, 5, 7 with different patterns
      await callWorker(fsWorker, "writeMultiPage", ["/file3.dat", 4, 30]);
      await callWorker(fsWorker, "writeMultiPage", ["/file5.dat", 4, 50]);
      await callWorker(fsWorker, "writeMultiPage", ["/file7.dat", 4, 70]);
      await callWorker(fsWorker, "syncfs", []);
      await remount();

      // Unmodified files keep original patterns
      for (const i of [0, 1, 2, 4, 6, 8, 9]) {
        const result = await callWorker(fsWorker, "verifyMultiPage", [`/file${i}.dat`, 4, i]);
        expect(result).toEqual({ ok: true });
      }
      // Modified files have new patterns
      expect(await callWorker(fsWorker, "verifyMultiPage", ["/file3.dat", 4, 30])).toEqual({ ok: true });
      expect(await callWorker(fsWorker, "verifyMultiPage", ["/file5.dat", 4, 50])).toEqual({ ok: true });
      expect(await callWorker(fsWorker, "verifyMultiPage", ["/file7.dat", 4, 70])).toEqual({ ok: true });
    });

    it("remount with smaller cache still serves all data", async () => {
      // Write files with total pages >> small cache
      for (let i = 0; i < 5; i++) {
        await callWorker(fsWorker, "writeMultiPage", [`/data${i}.dat`, 8, i * 5]);
      }
      await callWorker(fsWorker, "syncfs", []);

      // Remount with 4-page cache — extreme eviction pressure
      await remount(4);

      for (let i = 0; i < 5; i++) {
        const result = await callWorker(fsWorker, "verifyMultiPage", [`/data${i}.dat`, 8, i * 5]);
        expect(result).toEqual({ ok: true });
      }
    });

    it("rename overwrite persists correctly across remount", async () => {
      await callWorker(fsWorker, "writeFile", ["/src.txt", "source content"]);
      await callWorker(fsWorker, "writeFile", ["/dst.txt", "destination content"]);
      await callWorker(fsWorker, "rename", ["/src.txt", "/dst.txt"]);
      await callWorker(fsWorker, "syncfs", []);
      await remount();

      // dst.txt should have source's content
      expect(await callWorker(fsWorker, "readFile", ["/dst.txt"])).toBe("source content");
      // src.txt should not exist
      await expect(callWorker(fsWorker, "readFile", ["/src.txt"])).rejects.toThrow();
    });

    it("directory rename persists across remount", async () => {
      await callWorker(fsWorker, "mkdir", ["/olddir"]);
      await callWorker(fsWorker, "writeFile", ["/olddir/child.txt", "child data"]);
      await callWorker(fsWorker, "rename", ["/olddir", "/newdir"]);
      await callWorker(fsWorker, "syncfs", []);
      await remount();

      const entries = await callWorker(fsWorker, "readdir", ["/newdir"]);
      expect(entries).toContain("child.txt");
      expect(await callWorker(fsWorker, "readFile", ["/newdir/child.txt"])).toBe("child data");
      await expect(callWorker(fsWorker, "readdir", ["/olddir"])).rejects.toThrow();
    });

    it("file size metadata survives remount", async () => {
      // Write a file with a non-page-aligned size
      const content = "x".repeat(PAGE_SIZE + 100);
      await callWorker(fsWorker, "writeFile", ["/sized.txt", content]);
      await callWorker(fsWorker, "syncfs", []);
      await remount();

      const stat = await callWorker(fsWorker, "stat", ["/sized.txt"]);
      expect(stat.size).toBe(PAGE_SIZE + 100);

      const readBack = await callWorker(fsWorker, "readFile", ["/sized.txt"]);
      expect(readBack).toBe(content);
    });

    it("multiple remount cycles preserve data integrity", async () => {
      // Build up data across 3 remount cycles
      await callWorker(fsWorker, "writeFile", ["/cycle1.txt", "first"]);
      await callWorker(fsWorker, "syncfs", []);
      await remount();

      // Verify cycle 1 data, add cycle 2 data
      expect(await callWorker(fsWorker, "readFile", ["/cycle1.txt"])).toBe("first");
      await callWorker(fsWorker, "writeFile", ["/cycle2.txt", "second"]);
      await callWorker(fsWorker, "syncfs", []);
      await remount();

      // Verify both, add cycle 3 data
      expect(await callWorker(fsWorker, "readFile", ["/cycle1.txt"])).toBe("first");
      expect(await callWorker(fsWorker, "readFile", ["/cycle2.txt"])).toBe("second");
      await callWorker(fsWorker, "writeFile", ["/cycle3.txt", "third"]);
      await callWorker(fsWorker, "syncfs", []);
      await remount();

      // All three must survive
      expect(await callWorker(fsWorker, "readFile", ["/cycle1.txt"])).toBe("first");
      expect(await callWorker(fsWorker, "readFile", ["/cycle2.txt"])).toBe("second");
      expect(await callWorker(fsWorker, "readFile", ["/cycle3.txt"])).toBe("third");
    });
  });

  // -------------------------------------------------------------------
  // Symlinks, permissions, and timestamps through the SAB bridge.
  //
  // These operations serialize metadata with non-numeric fields (symlink
  // targets) and optional fields (atime, mode) through JSON in the SAB
  // protocol. A bug in serialization could silently drop the `link` field
  // or corrupt mode bits — only manifest through the SAB bridge path.
  // -------------------------------------------------------------------

  describe("symlink operations through SAB bridge", () => {
    it("@fast create and resolve a symlink", async () => {
      await callWorker(fsWorker, "writeFile", ["/target.txt", "symlink target data"]);
      await callWorker(fsWorker, "symlink", ["/data/target.txt", "/link.txt"]);

      const resolved = await callWorker(fsWorker, "readlink", ["/link.txt"]);
      expect(resolved).toBe("/data/target.txt");

      // Reading through the symlink should return the target's content
      const content = await callWorker(fsWorker, "readFile", ["/link.txt"]);
      expect(content).toBe("symlink target data");
    });

    it("@fast symlink persists across remount", async () => {
      await callWorker(fsWorker, "writeFile", ["/real.txt", "persistent"]);
      await callWorker(fsWorker, "symlink", ["/data/real.txt", "/sym.txt"]);
      await callWorker(fsWorker, "syncfs", []);
      await remount();

      // Symlink target must survive JSON serialization through SAB bridge
      const target = await callWorker(fsWorker, "readlink", ["/sym.txt"]);
      expect(target).toBe("/data/real.txt");

      // Content accessible through the restored symlink
      const content = await callWorker(fsWorker, "readFile", ["/sym.txt"]);
      expect(content).toBe("persistent");
    });

    it("symlink in subdirectory persists across remount", async () => {
      await callWorker(fsWorker, "mkdir", ["/dir"]);
      await callWorker(fsWorker, "writeFile", ["/dir/file.txt", "nested target"]);
      await callWorker(fsWorker, "symlink", ["/data/dir/file.txt", "/dir/link.txt"]);
      await callWorker(fsWorker, "syncfs", []);
      await remount();

      const target = await callWorker(fsWorker, "readlink", ["/dir/link.txt"]);
      expect(target).toBe("/data/dir/file.txt");
      expect(await callWorker(fsWorker, "readFile", ["/dir/link.txt"])).toBe("nested target");
    });

    it("rename symlink preserves target across remount", async () => {
      await callWorker(fsWorker, "writeFile", ["/target.txt", "rename test"]);
      await callWorker(fsWorker, "symlink", ["/data/target.txt", "/old-link.txt"]);
      await callWorker(fsWorker, "rename", ["/old-link.txt", "/new-link.txt"]);
      await callWorker(fsWorker, "syncfs", []);
      await remount();

      const target = await callWorker(fsWorker, "readlink", ["/new-link.txt"]);
      expect(target).toBe("/data/target.txt");
      expect(await callWorker(fsWorker, "readFile", ["/new-link.txt"])).toBe("rename test");
      await expect(callWorker(fsWorker, "readlink", ["/old-link.txt"])).rejects.toThrow();
    });

    it("unlink symlink does not affect target across remount", async () => {
      await callWorker(fsWorker, "writeFile", ["/keep.txt", "untouched"]);
      await callWorker(fsWorker, "symlink", ["/data/keep.txt", "/doomed-link.txt"]);
      await callWorker(fsWorker, "syncfs", []);

      await callWorker(fsWorker, "unlink", ["/doomed-link.txt"]);
      await callWorker(fsWorker, "syncfs", []);
      await remount();

      // Target file must survive
      expect(await callWorker(fsWorker, "readFile", ["/keep.txt"])).toBe("untouched");
      // Symlink must be gone
      await expect(callWorker(fsWorker, "readlink", ["/doomed-link.txt"])).rejects.toThrow();
    });

    it("mixed tree with files, dirs, and symlinks persists", async () => {
      await callWorker(fsWorker, "mkdir", ["/app"]);
      await callWorker(fsWorker, "mkdir", ["/app/data"]);
      await callWorker(fsWorker, "writeFile", ["/app/data/config.json", '{"key":"value"}']);
      await callWorker(fsWorker, "writeMultiPage", ["/app/data/store.db", 4, 99]);
      await callWorker(fsWorker, "symlink", ["/data/app/data/config.json", "/app/config-link"]);
      await callWorker(fsWorker, "symlink", ["/data/app/data/store.db", "/app/db-link"]);
      await callWorker(fsWorker, "syncfs", []);
      await remount();

      // Directory structure
      const appEntries = await callWorker(fsWorker, "readdir", ["/app"]);
      expect(appEntries).toContain("data");
      expect(appEntries).toContain("config-link");
      expect(appEntries).toContain("db-link");

      // Regular file
      expect(await callWorker(fsWorker, "readFile", ["/app/data/config.json"])).toBe('{"key":"value"}');

      // Multi-page file
      const result = await callWorker(fsWorker, "verifyMultiPage", ["/app/data/store.db", 4, 99]);
      expect(result).toEqual({ ok: true });

      // Symlinks resolve correctly
      expect(await callWorker(fsWorker, "readlink", ["/app/config-link"])).toBe("/data/app/data/config.json");
      expect(await callWorker(fsWorker, "readFile", ["/app/config-link"])).toBe('{"key":"value"}');
    });
  });

  describe("file metadata persistence through SAB bridge", () => {
    it("@fast chmod persists across remount", async () => {
      await callWorker(fsWorker, "writeFile", ["/secret.txt", "classified"]);
      await callWorker(fsWorker, "chmod", ["/secret.txt", 0o100400]);
      await callWorker(fsWorker, "syncfs", []);
      await remount();

      const stat = await callWorker(fsWorker, "stat", ["/secret.txt"]);
      expect(stat.mode & 0o777).toBe(0o400);
    });

    it("utime persists across remount", async () => {
      await callWorker(fsWorker, "writeFile", ["/timed.txt", "timestamp test"]);
      // Emscripten's FS.utime takes milliseconds (matching Date.getTime())
      const atime = 1700000000000;
      const mtime = 1700000001000;
      await callWorker(fsWorker, "utime", ["/timed.txt", atime, mtime]);
      await callWorker(fsWorker, "syncfs", []);
      await remount();

      const stat = await callWorker(fsWorker, "lstat", ["/timed.txt"]);
      const restoredMtime = typeof stat.mtime === "object" ? stat.mtime.getTime() : stat.mtime;
      expect(restoredMtime).toBe(mtime);
    });

    it("directory permissions persist across remount", async () => {
      await callWorker(fsWorker, "mkdir", ["/restricted"]);
      await callWorker(fsWorker, "writeFile", ["/restricted/file.txt", "inside"]);
      await callWorker(fsWorker, "chmod", ["/restricted", 0o40755]);
      await callWorker(fsWorker, "syncfs", []);
      await remount();

      const stat = await callWorker(fsWorker, "stat", ["/restricted"]);
      expect(stat.mode & 0o777).toBe(0o755);
      expect(await callWorker(fsWorker, "readFile", ["/restricted/file.txt"])).toBe("inside");
    });
  });
});
