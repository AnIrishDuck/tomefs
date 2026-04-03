/**
 * Tests for the SAB+Atomics sync bridge.
 *
 * Architecture:
 * - Main thread: runs SabWorker (async side, non-blocking) with a MemoryBackend
 * - Worker thread: runs SabClient (sync side, uses Atomics.wait) via bundled worker
 *
 * The client worker is bundled with esbuild to resolve all TypeScript imports
 * into a single JS file that Node.js workers can execute directly.
 */
import { describe, it, expect, beforeEach, afterEach, beforeAll } from "vitest";
import { Worker } from "node:worker_threads";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { writeFileSync, mkdirSync, existsSync } from "node:fs";
import { MemoryBackend } from "../../src/memory-backend.js";
import { SabWorker } from "../../src/sab-worker.js";
import { SabClient } from "../../src/sab-client.js";
import { PAGE_SIZE } from "../../src/types.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const WORKER_SRC = join(__dirname, "sab-bridge-client-worker.ts");
const WORKER_BUNDLE = join(__dirname, ".sab-bridge-client-worker.bundle.mjs");

/** Bundle the client worker TS into a single JS file using esbuild. */
async function buildWorkerBundle(): Promise<void> {
  const { buildSync } = await import("esbuild");
  buildSync({
    entryPoints: [WORKER_SRC],
    bundle: true,
    format: "esm",
    platform: "node",
    outfile: WORKER_BUNDLE,
    external: ["node:worker_threads"],
  });
}

/** Helper to call a method on the SabClient running in the worker thread. */
function callClient(
  worker: Worker,
  cmd: string,
  args: unknown[],
): Promise<unknown> {
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
  return new Promise((resolve) => {
    const handler = (msg: { ready?: boolean }) => {
      if (msg.ready) {
        worker.off("message", handler);
        resolve();
      }
    };
    worker.on("message", handler);
  });
}

describe("SAB+Atomics Bridge", () => {
  let backend: MemoryBackend;
  let sabWorker: SabWorker;
  let clientWorker: Worker;
  let sab: SharedArrayBuffer;

  beforeAll(async () => {
    await buildWorkerBundle();
  });

  beforeEach(async () => {
    backend = new MemoryBackend();
    sab = SabClient.createBuffer();
    sabWorker = new SabWorker(sab, backend);

    // Start the async worker-side event loop (non-blocking via waitAsync)
    sabWorker.start();

    // Start the client in a real Worker thread (uses Atomics.wait, which blocks)
    clientWorker = new Worker(WORKER_BUNDLE, {
      workerData: { sab },
    });
    await waitReady(clientWorker);
  });

  afterEach(async () => {
    sabWorker.stop();
    await clientWorker.terminate();
  });

  describe("page operations", () => {
    it("@fast readPage returns null for non-existent page", async () => {
      const result = await callClient(clientWorker, "readPage", ["/test", 0]);
      expect(result).toBeNull();
    });

    it("@fast writePage + readPage round-trip", async () => {
      const data = new Uint8Array(PAGE_SIZE);
      data[0] = 0xde;
      data[1] = 0xad;
      data[PAGE_SIZE - 1] = 0xff;

      await callClient(clientWorker, "writePage", ["/test", 0, data]);
      const result = await callClient(clientWorker, "readPage", ["/test", 0]);

      // Structured clone may serialize Uint8Array differently
      const buf = toUint8Array(result);
      expect(buf[0]).toBe(0xde);
      expect(buf[1]).toBe(0xad);
      expect(buf[PAGE_SIZE - 1]).toBe(0xff);
    });

    it("writePage overwrites existing data", async () => {
      const data1 = new Uint8Array(PAGE_SIZE);
      data1[0] = 0x11;
      await callClient(clientWorker, "writePage", ["/test", 0, data1]);

      const data2 = new Uint8Array(PAGE_SIZE);
      data2[0] = 0x22;
      await callClient(clientWorker, "writePage", ["/test", 0, data2]);

      const result = await callClient(clientWorker, "readPage", ["/test", 0]);
      const buf = toUint8Array(result);
      expect(buf[0]).toBe(0x22);
    });

    it("writePages batch writes multiple pages", async () => {
      const page0 = new Uint8Array(PAGE_SIZE);
      page0[0] = 0xaa;
      const page1 = new Uint8Array(PAGE_SIZE);
      page1[0] = 0xbb;

      await callClient(clientWorker, "writePages", [
        [
          { path: "/test", pageIndex: 0, data: page0 },
          { path: "/test", pageIndex: 1, data: page1 },
        ],
      ]);

      const r0 = await callClient(clientWorker, "readPage", ["/test", 0]);
      const r1 = await callClient(clientWorker, "readPage", ["/test", 1]);

      expect(toUint8Array(r0)[0]).toBe(0xaa);
      expect(toUint8Array(r1)[0]).toBe(0xbb);
    });

    it("readPages batch reads multiple pages", async () => {
      const page0 = new Uint8Array(PAGE_SIZE);
      page0[0] = 0xaa;
      const page1 = new Uint8Array(PAGE_SIZE);
      page1[0] = 0xbb;

      await callClient(clientWorker, "writePage", ["/test", 0, page0]);
      await callClient(clientWorker, "writePage", ["/test", 1, page1]);

      const result = (await callClient(clientWorker, "readPages", [
        "/test",
        [0, 1, 2],
      ])) as Array<unknown>;

      expect(result).toHaveLength(3);
      expect(toUint8Array(result[0])[0]).toBe(0xaa);
      expect(toUint8Array(result[1])[0]).toBe(0xbb);
      expect(result[2]).toBeNull();
    });

    it("deleteFile removes all pages", async () => {
      const data = new Uint8Array(PAGE_SIZE);
      data[0] = 0x42;
      await callClient(clientWorker, "writePage", ["/test", 0, data]);
      await callClient(clientWorker, "writePage", ["/test", 1, data]);

      await callClient(clientWorker, "deleteFile", ["/test"]);

      expect(await callClient(clientWorker, "readPage", ["/test", 0])).toBeNull();
      expect(await callClient(clientWorker, "readPage", ["/test", 1])).toBeNull();
    });

    it("@fast deleteFiles removes pages for multiple files in one call", async () => {
      const data = new Uint8Array(PAGE_SIZE);
      data[0] = 0x42;
      await callClient(clientWorker, "writePage", ["/a", 0, data]);
      await callClient(clientWorker, "writePage", ["/a", 1, data]);
      await callClient(clientWorker, "writePage", ["/b", 0, data]);
      await callClient(clientWorker, "writePage", ["/c", 0, data]);

      await callClient(clientWorker, "deleteFiles", [["/a", "/b"]]);

      expect(await callClient(clientWorker, "readPage", ["/a", 0])).toBeNull();
      expect(await callClient(clientWorker, "readPage", ["/a", 1])).toBeNull();
      expect(await callClient(clientWorker, "readPage", ["/b", 0])).toBeNull();
      expect(await callClient(clientWorker, "readPage", ["/c", 0])).not.toBeNull();
    });

    it("deleteFiles with empty array is a no-op", async () => {
      const data = new Uint8Array(PAGE_SIZE);
      data[0] = 0x42;
      await callClient(clientWorker, "writePage", ["/test", 0, data]);

      await callClient(clientWorker, "deleteFiles", [[]]);

      expect(await callClient(clientWorker, "readPage", ["/test", 0])).not.toBeNull();
    });

    it("deletePagesFrom removes pages at and beyond index", async () => {
      const data = new Uint8Array(PAGE_SIZE);
      data[0] = 0x42;
      await callClient(clientWorker, "writePage", ["/test", 0, data]);
      await callClient(clientWorker, "writePage", ["/test", 1, data]);
      await callClient(clientWorker, "writePage", ["/test", 2, data]);

      await callClient(clientWorker, "deletePagesFrom", ["/test", 1]);

      expect(await callClient(clientWorker, "readPage", ["/test", 0])).not.toBeNull();
      expect(await callClient(clientWorker, "readPage", ["/test", 1])).toBeNull();
      expect(await callClient(clientWorker, "readPage", ["/test", 2])).toBeNull();
    });
  });

  describe("renameFile", () => {
    it("moves all pages from old path to new path", async () => {
      const d0 = new Uint8Array(PAGE_SIZE);
      d0[0] = 0xaa;
      const d1 = new Uint8Array(PAGE_SIZE);
      d1[0] = 0xbb;
      await callClient(clientWorker, "writePage", ["/old", 0, d0]);
      await callClient(clientWorker, "writePage", ["/old", 1, d1]);

      await callClient(clientWorker, "renameFile", ["/old", "/new"]);

      expect(await callClient(clientWorker, "readPage", ["/old", 0])).toBeNull();
      expect(await callClient(clientWorker, "readPage", ["/old", 1])).toBeNull();
      const p0 = await callClient(clientWorker, "readPage", ["/new", 0]) as Uint8Array;
      expect(p0[0]).toBe(0xaa);
      const p1 = await callClient(clientWorker, "readPage", ["/new", 1]) as Uint8Array;
      expect(p1[0]).toBe(0xbb);
    });
  });

  describe("countPages", () => {
    it("@fast returns 0 for non-existent file", async () => {
      expect(await callClient(clientWorker, "countPages", ["/test"])).toBe(0);
    });

    it("counts pages after writes", async () => {
      await callClient(clientWorker, "writePage", ["/test", 0, new Uint8Array(PAGE_SIZE)]);
      await callClient(clientWorker, "writePage", ["/test", 1, new Uint8Array(PAGE_SIZE)]);
      expect(await callClient(clientWorker, "countPages", ["/test"])).toBe(2);
    });
  });

  describe("countPagesBatch", () => {
    it("@fast returns empty array for empty input", async () => {
      const result = await callClient(clientWorker, "countPagesBatch", [[]]);
      expect(result).toEqual([]);
    });

    it("returns counts parallel to input paths", async () => {
      await callClient(clientWorker, "writePage", ["/a", 0, new Uint8Array(PAGE_SIZE)]);
      await callClient(clientWorker, "writePage", ["/b", 0, new Uint8Array(PAGE_SIZE)]);
      await callClient(clientWorker, "writePage", ["/b", 1, new Uint8Array(PAGE_SIZE)]);

      const result = await callClient(clientWorker, "countPagesBatch", [["/a", "/b", "/missing"]]);
      expect(result).toEqual([1, 2, 0]);
    });
  });

  describe("maxPageIndexBatch", () => {
    it("@fast returns empty array for empty input", async () => {
      const result = await callClient(clientWorker, "maxPageIndexBatch", [[]]);
      expect(result).toEqual([]);
    });

    it("returns max indices parallel to input paths", async () => {
      await callClient(clientWorker, "writePage", ["/a", 0, new Uint8Array(PAGE_SIZE)]);
      await callClient(clientWorker, "writePage", ["/b", 0, new Uint8Array(PAGE_SIZE)]);
      await callClient(clientWorker, "writePage", ["/b", 5, new Uint8Array(PAGE_SIZE)]);

      const result = await callClient(clientWorker, "maxPageIndexBatch", [["/a", "/b", "/missing"]]);
      expect(result).toEqual([0, 5, -1]);
    });
  });

  describe("metadata operations", () => {
    it("@fast readMeta returns null for non-existent file", async () => {
      const result = await callClient(clientWorker, "readMeta", ["/test"]);
      expect(result).toBeNull();
    });

    it("@fast writeMeta + readMeta round-trip", async () => {
      const meta = { size: 1024, mode: 0o644, ctime: 1000, mtime: 2000 };
      await callClient(clientWorker, "writeMeta", ["/test", meta]);

      const result = await callClient(clientWorker, "readMeta", ["/test"]);
      expect(result).toEqual(meta);
    });

    it("writeMeta overwrites existing metadata", async () => {
      const meta1 = { size: 100, mode: 0o644, ctime: 1000, mtime: 2000 };
      await callClient(clientWorker, "writeMeta", ["/test", meta1]);

      const meta2 = { size: 200, mode: 0o755, ctime: 1000, mtime: 3000 };
      await callClient(clientWorker, "writeMeta", ["/test", meta2]);

      expect(await callClient(clientWorker, "readMeta", ["/test"])).toEqual(meta2);
    });

    it("deleteMeta removes metadata", async () => {
      const meta = { size: 1024, mode: 0o644, ctime: 1000, mtime: 2000 };
      await callClient(clientWorker, "writeMeta", ["/test", meta]);
      await callClient(clientWorker, "deleteMeta", ["/test"]);

      expect(await callClient(clientWorker, "readMeta", ["/test"])).toBeNull();
    });

    it("listFiles returns all files with metadata", async () => {
      const meta = { size: 0, mode: 0o644, ctime: 0, mtime: 0 };
      await callClient(clientWorker, "writeMeta", ["/a", meta]);
      await callClient(clientWorker, "writeMeta", ["/b", meta]);
      await callClient(clientWorker, "writeMeta", ["/c", meta]);

      const files = (await callClient(clientWorker, "listFiles", [])) as string[];
      expect(files.sort()).toEqual(["/a", "/b", "/c"]);
    });

    it("@fast writeMetas batch writes multiple entries in one call", async () => {
      await callClient(clientWorker, "writeMetas", [
        [
          { path: "/x", meta: { size: 10, mode: 0o644, ctime: 1, mtime: 2 } },
          { path: "/y", meta: { size: 20, mode: 0o755, ctime: 3, mtime: 4 } },
          { path: "/z", meta: { size: 0, mode: 0o40755, ctime: 5, mtime: 6 } },
        ],
      ]);

      const x = await callClient(clientWorker, "readMeta", ["/x"]);
      const y = await callClient(clientWorker, "readMeta", ["/y"]);
      const z = await callClient(clientWorker, "readMeta", ["/z"]);
      expect(x).toEqual({ size: 10, mode: 0o644, ctime: 1, mtime: 2 });
      expect(y).toEqual({ size: 20, mode: 0o755, ctime: 3, mtime: 4 });
      expect(z).toEqual({ size: 0, mode: 0o40755, ctime: 5, mtime: 6 });
    });

    it("@fast readMetas batch reads multiple entries in one call", async () => {
      await callClient(clientWorker, "writeMetas", [
        [
          { path: "/p", meta: { size: 10, mode: 0o644, ctime: 1, mtime: 2 } },
          { path: "/q", meta: { size: 20, mode: 0o755, ctime: 3, mtime: 4 } },
        ],
      ]);

      const results = await callClient(clientWorker, "readMetas", [["/p", "/q", "/missing"]]) as unknown[];
      expect(results).toHaveLength(3);
      expect(results[0]).toEqual({ size: 10, mode: 0o644, ctime: 1, mtime: 2 });
      expect(results[1]).toEqual({ size: 20, mode: 0o755, ctime: 3, mtime: 4 });
      expect(results[2]).toBeNull();
    });

    it("@fast readMetas returns empty array for empty input", async () => {
      const results = await callClient(clientWorker, "readMetas", [[]]);
      expect(results).toEqual([]);
    });

    it("@fast deleteMetas batch deletes multiple entries in one call", async () => {
      const meta = { size: 0, mode: 0o644, ctime: 0, mtime: 0 };
      await callClient(clientWorker, "writeMeta", ["/a", meta]);
      await callClient(clientWorker, "writeMeta", ["/b", meta]);
      await callClient(clientWorker, "writeMeta", ["/c", meta]);

      await callClient(clientWorker, "deleteMetas", [["/a", "/c"]]);

      expect(await callClient(clientWorker, "readMeta", ["/a"])).toBeNull();
      expect(await callClient(clientWorker, "readMeta", ["/b"])).not.toBeNull();
      expect(await callClient(clientWorker, "readMeta", ["/c"])).toBeNull();
    });
  });

  describe("multi-file isolation", () => {
    it("pages from different files are independent", async () => {
      const data1 = new Uint8Array(PAGE_SIZE);
      data1[0] = 0x11;
      const data2 = new Uint8Array(PAGE_SIZE);
      data2[0] = 0x22;

      await callClient(clientWorker, "writePage", ["/file1", 0, data1]);
      await callClient(clientWorker, "writePage", ["/file2", 0, data2]);

      await callClient(clientWorker, "deleteFile", ["/file1"]);

      expect(await callClient(clientWorker, "readPage", ["/file1", 0])).toBeNull();
      const r = await callClient(clientWorker, "readPage", ["/file2", 0]);
      expect(toUint8Array(r)[0]).toBe(0x22);
    });
  });

  describe("sub-page data", () => {
    it("handles pages smaller than PAGE_SIZE", async () => {
      const data = new Uint8Array(100);
      for (let i = 0; i < 100; i++) data[i] = i;

      await callClient(clientWorker, "writePage", ["/small", 0, data]);

      const result = await callClient(clientWorker, "readPage", ["/small", 0]);
      const buf = toUint8Array(result);
      expect(buf[0]).toBe(0);
      expect(buf[50]).toBe(50);
      expect(buf[99]).toBe(99);
    });
  });

  describe("sequential operations", () => {
    it("handles many sequential calls without deadlock", async () => {
      const meta = { size: 0, mode: 0o644, ctime: 0, mtime: 0 };

      for (let i = 0; i < 50; i++) {
        await callClient(clientWorker, "writeMeta", [
          `/file${i}`,
          { ...meta, size: i },
        ]);
        const result = (await callClient(clientWorker, "readMeta", [
          `/file${i}`,
        ])) as typeof meta;
        expect(result.size).toBe(i);
      }

      const files = (await callClient(clientWorker, "listFiles", [])) as string[];
      expect(files.length).toBe(50);
    });
  });

  describe("page + metadata combined", () => {
    it("page data and metadata are independent", async () => {
      const data = new Uint8Array(PAGE_SIZE);
      data[0] = 0xff;
      const meta = { size: PAGE_SIZE, mode: 0o644, ctime: 100, mtime: 200 };

      await callClient(clientWorker, "writePage", ["/test", 0, data]);
      await callClient(clientWorker, "writeMeta", ["/test", meta]);

      // Delete metadata, pages should survive
      await callClient(clientWorker, "deleteMeta", ["/test"]);
      expect(await callClient(clientWorker, "readMeta", ["/test"])).toBeNull();
      const r = await callClient(clientWorker, "readPage", ["/test", 0]);
      expect(toUint8Array(r)[0]).toBe(0xff);

      // Delete pages, verify gone
      await callClient(clientWorker, "deleteFile", ["/test"]);
      expect(await callClient(clientWorker, "readPage", ["/test", 0])).toBeNull();
    });
  });
});

/** Convert a structured-clone result back to Uint8Array. */
function toUint8Array(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) return value;
  if (value && typeof value === "object" && "data" in value) {
    return new Uint8Array((value as { data: number[] }).data);
  }
  if (Array.isArray(value)) return new Uint8Array(value);
  throw new Error(`Cannot convert to Uint8Array: ${typeof value}`);
}
