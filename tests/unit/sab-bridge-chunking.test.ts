/**
 * SAB bridge batch-chunking tests.
 *
 * These tests use a small SharedArrayBuffer (12 KB data region) that forces
 * SabClient to chunk every batch operation at small sizes:
 *
 *   maxBatchPages = 1   (every 2+ page batch is chunked)
 *   maxBatchMetas = 16  (every 17+ meta batch is chunked)
 *   maxBatchFiles = 32  (every 33+ file listing is paginated)
 *
 * With the default 1 MB buffer, these limits are ~123 pages, ~2040 metas,
 * and ~4080 files — high enough that production workloads rarely trigger
 * chunking and the paths go untested.
 *
 * Ethos §9: "Write tests designed to break tomefs specifically — target the seams"
 */
import { describe, it, expect, beforeEach, afterEach, beforeAll } from "vitest";
import { Worker } from "node:worker_threads";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { MemoryBackend } from "../../src/memory-backend.js";
import { SabWorker } from "../../src/sab-worker.js";
import { PAGE_SIZE } from "../../src/types.js";
import type { FileMeta } from "../../src/types.js";
import { CONTROL_BYTES } from "../../src/sab-protocol.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const WORKER_SRC = join(__dirname, "sab-bridge-timeout-worker.ts");
const WORKER_BUNDLE = join(__dirname, ".sab-bridge-timeout-worker.bundle.mjs");

/** Build the client worker bundle (reuses the timeout worker script). */
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

/** Convert structured-clone result back to Uint8Array. */
function toUint8Array(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) return value;
  if (value && typeof value === "object" && "data" in value) {
    return new Uint8Array((value as { data: number[] }).data);
  }
  if (Array.isArray(value)) return new Uint8Array(value);
  throw new Error(`Cannot convert to Uint8Array: ${typeof value}`);
}

/** Convert structured-clone result back to array of (Uint8Array | null). */
function toPageArray(value: unknown): Array<Uint8Array | null> {
  if (!Array.isArray(value)) throw new Error("Expected array");
  return value.map((v) =>
    v === null || v === undefined ? null : toUint8Array(v),
  );
}

/** Create a page filled with a single byte value. */
function fillPage(value: number): Uint8Array {
  const buf = new Uint8Array(PAGE_SIZE);
  buf.fill(value);
  return buf;
}

/** Small buffer size: CONTROL_BYTES + 12 KB data region.
 *
 * This yields:
 *   maxBatchPages = max(1, floor((12288 - 4096) / (8192 + 256))) = max(1, 0) = 1
 *   maxBatchMetas = max(1, floor(8192 / 512)) = 16
 *   maxBatchFiles = max(1, floor(8192 / 256)) = 32
 */
const SMALL_BUFFER_SIZE = CONTROL_BYTES + 12288;

describe("SAB bridge: batch chunking with small buffer", () => {
  let backend: MemoryBackend;
  let sabWorker: SabWorker;
  let clientWorker: Worker;
  let sab: SharedArrayBuffer;

  beforeAll(async () => {
    await buildWorkerBundle();
  });

  beforeEach(async () => {
    backend = new MemoryBackend();
    sab = new SharedArrayBuffer(SMALL_BUFFER_SIZE);
    sabWorker = new SabWorker(sab, backend);
    sabWorker.start();

    clientWorker = new Worker(WORKER_BUNDLE, {
      workerData: { sab, timeout: 0 },
    });
    await waitReady(clientWorker);
  });

  afterEach(async () => {
    sabWorker.stop();
    await clientWorker.terminate();
  });

  // -----------------------------------------------------------------
  // Page chunking (maxBatchPages = 1)
  // -----------------------------------------------------------------

  describe("page chunking (maxBatchPages=1)", () => {
    it("@fast writePages + readPages with 1 page (fast path, no chunking)", async () => {
      const pages = [{ path: "/f", pageIndex: 0, data: fillPage(0xaa) }];
      await callClient(clientWorker, "writePages", [pages]);

      const result = toPageArray(
        await callClient(clientWorker, "readPages", ["/f", [0]]),
      );
      expect(result).toHaveLength(1);
      expect(result[0]![0]).toBe(0xaa);
    });

    it("@fast writePages + readPages with 5 pages (chunked into 5 calls)", async () => {
      const pages = Array.from({ length: 5 }, (_, i) => ({
        path: "/chunked",
        pageIndex: i,
        data: fillPage(i + 1),
      }));
      await callClient(clientWorker, "writePages", [pages]);

      // Read all 5 pages in a single chunked readPages call
      const indices = [0, 1, 2, 3, 4];
      const result = toPageArray(
        await callClient(clientWorker, "readPages", ["/chunked", indices]),
      );
      expect(result).toHaveLength(5);
      for (let i = 0; i < 5; i++) {
        expect(result[i]).not.toBeNull();
        expect(result[i]![0]).toBe(i + 1);
        expect(result[i]![PAGE_SIZE - 1]).toBe(i + 1);
      }
    });

    it("writePages across multiple files, then readPages each", async () => {
      const pages = [
        { path: "/a", pageIndex: 0, data: fillPage(0x10) },
        { path: "/a", pageIndex: 1, data: fillPage(0x11) },
        { path: "/b", pageIndex: 0, data: fillPage(0x20) },
        { path: "/b", pageIndex: 2, data: fillPage(0x22) },
      ];
      await callClient(clientWorker, "writePages", [pages]);

      // Read /a pages
      const aPages = toPageArray(
        await callClient(clientWorker, "readPages", ["/a", [0, 1]]),
      );
      expect(aPages[0]![0]).toBe(0x10);
      expect(aPages[1]![0]).toBe(0x11);

      // Read /b pages (sparse — page 1 doesn't exist)
      const bPages = toPageArray(
        await callClient(clientWorker, "readPages", ["/b", [0, 1, 2]]),
      );
      expect(bPages[0]![0]).toBe(0x20);
      expect(bPages[1]).toBeNull();
      expect(bPages[2]![0]).toBe(0x22);
    });

    it("writePages full page integrity across chunk boundary", async () => {
      // Write 3 pages with distinct data patterns
      const pages = Array.from({ length: 3 }, (_, i) => {
        const data = new Uint8Array(PAGE_SIZE);
        for (let j = 0; j < PAGE_SIZE; j++) {
          data[j] = (i * 37 + j) & 0xff;
        }
        return { path: "/integrity", pageIndex: i, data };
      });
      await callClient(clientWorker, "writePages", [pages]);

      // Read back and verify every byte
      const result = toPageArray(
        await callClient(clientWorker, "readPages", ["/integrity", [0, 1, 2]]),
      );
      for (let i = 0; i < 3; i++) {
        const expected = new Uint8Array(PAGE_SIZE);
        for (let j = 0; j < PAGE_SIZE; j++) {
          expected[j] = (i * 37 + j) & 0xff;
        }
        expect(result[i]).toEqual(expected);
      }
    });
  });

  // -----------------------------------------------------------------
  // Metadata chunking (maxBatchMetas = 16)
  // -----------------------------------------------------------------

  describe("metadata chunking (maxBatchMetas=16)", () => {
    it("writeMetas + readMetas with 16 entries (fast path)", async () => {
      const entries = Array.from({ length: 16 }, (_, i) => ({
        path: `/m${i}`,
        meta: { size: i * 100, mode: 0o100644, ctime: 1000 + i, mtime: 2000 + i } as FileMeta,
      }));
      await callClient(clientWorker, "writeMetas", [entries]);

      const paths = entries.map((e) => e.path);
      const result = (await callClient(clientWorker, "readMetas", [paths])) as Array<FileMeta | null>;
      expect(result).toHaveLength(16);
      for (let i = 0; i < 16; i++) {
        expect(result[i]).not.toBeNull();
        expect(result[i]!.size).toBe(i * 100);
        expect(result[i]!.mtime).toBe(2000 + i);
      }
    });

    it("@fast writeMetas + readMetas with 25 entries (chunked into [16, 9])", async () => {
      const entries = Array.from({ length: 25 }, (_, i) => ({
        path: `/meta_${i}`,
        meta: { size: i * 50, mode: 0o100755, ctime: 3000 + i, mtime: 4000 + i } as FileMeta,
      }));
      await callClient(clientWorker, "writeMetas", [entries]);

      const paths = entries.map((e) => e.path);
      const result = (await callClient(clientWorker, "readMetas", [paths])) as Array<FileMeta | null>;
      expect(result).toHaveLength(25);
      for (let i = 0; i < 25; i++) {
        expect(result[i]!.size).toBe(i * 50);
        expect(result[i]!.mode).toBe(0o100755);
        expect(result[i]!.ctime).toBe(3000 + i);
        expect(result[i]!.mtime).toBe(4000 + i);
      }
    });

    it("deleteMetas across chunk boundary", async () => {
      // Write 20 metas
      const entries = Array.from({ length: 20 }, (_, i) => ({
        path: `/del_${i}`,
        meta: { size: 0, mode: 0o100644, ctime: 0, mtime: 0 } as FileMeta,
      }));
      await callClient(clientWorker, "writeMetas", [entries]);

      // Delete all 20 (chunked into [16, 4])
      const paths = entries.map((e) => e.path);
      await callClient(clientWorker, "deleteMetas", [paths]);

      // Verify all deleted
      const result = (await callClient(clientWorker, "readMetas", [paths])) as Array<FileMeta | null>;
      for (let i = 0; i < 20; i++) {
        expect(result[i]).toBeNull();
      }
    });

    it("writeMetas preserves symlink metadata across chunks", async () => {
      const entries = Array.from({ length: 20 }, (_, i) => ({
        path: `/link_${i}`,
        meta: {
          size: 0,
          mode: 0o120777,
          ctime: 5000 + i,
          mtime: 6000 + i,
          link: `/target_${i}`,
        } as FileMeta,
      }));
      await callClient(clientWorker, "writeMetas", [entries]);

      const paths = entries.map((e) => e.path);
      const result = (await callClient(clientWorker, "readMetas", [paths])) as Array<FileMeta | null>;
      expect(result).toHaveLength(20);
      for (let i = 0; i < 20; i++) {
        expect(result[i]!.link).toBe(`/target_${i}`);
      }
    });
  });

  // -----------------------------------------------------------------
  // syncAll chunking
  // -----------------------------------------------------------------

  describe("syncAll chunking", () => {
    it("syncAll with 1 page + few metas (fast path — single SYNC_ALL call)", async () => {
      const pages = [{ path: "/sync1", pageIndex: 0, data: fillPage(0xbb) }];
      const metas = [
        { path: "/sync1", meta: { size: PAGE_SIZE, mode: 0o100644, ctime: 100, mtime: 200 } as FileMeta },
      ];
      await callClient(clientWorker, "syncAll", [pages, metas]);

      // Verify page
      const page = toUint8Array(
        await callClient(clientWorker, "readPage", ["/sync1", 0]),
      );
      expect(page[0]).toBe(0xbb);

      // Verify meta
      const meta = (await callClient(clientWorker, "readMeta", ["/sync1"])) as FileMeta;
      expect(meta.size).toBe(PAGE_SIZE);
      expect(meta.mtime).toBe(200);
    });

    it("@fast syncAll with 5 pages falls back to writePages + writeMetas", async () => {
      const pages = Array.from({ length: 5 }, (_, i) => ({
        path: "/syncmulti",
        pageIndex: i,
        data: fillPage(0xcc + i),
      }));
      const metas = [
        {
          path: "/syncmulti",
          meta: { size: 5 * PAGE_SIZE, mode: 0o100644, ctime: 300, mtime: 400 } as FileMeta,
        },
      ];
      await callClient(clientWorker, "syncAll", [pages, metas]);

      // Verify all pages survived
      const result = toPageArray(
        await callClient(clientWorker, "readPages", ["/syncmulti", [0, 1, 2, 3, 4]]),
      );
      for (let i = 0; i < 5; i++) {
        expect(result[i]![0]).toBe((0xcc + i) & 0xff);
      }

      // Verify meta survived
      const meta = (await callClient(clientWorker, "readMeta", ["/syncmulti"])) as FileMeta;
      expect(meta.size).toBe(5 * PAGE_SIZE);
    });

    it("syncAll with many metas falls back to chunked writeMetas", async () => {
      // 1 page + 20 metas. With the fix, metas.length (20) > maxBatchMetas (16)
      // triggers the fallback path which chunks metas into [16, 4].
      const pages = [{ path: "/file0", pageIndex: 0, data: fillPage(0xdd) }];
      const metas = Array.from({ length: 20 }, (_, i) => ({
        path: `/file${i}`,
        meta: { size: i === 0 ? PAGE_SIZE : 0, mode: 0o100644, ctime: 500 + i, mtime: 600 + i } as FileMeta,
      }));
      await callClient(clientWorker, "syncAll", [pages, metas]);

      // Verify page
      const page = toUint8Array(
        await callClient(clientWorker, "readPage", ["/file0", 0]),
      );
      expect(page[0]).toBe(0xdd);

      // Verify all 20 metas
      const paths = metas.map((m) => m.path);
      const readMetas = (await callClient(clientWorker, "readMetas", [paths])) as Array<FileMeta | null>;
      for (let i = 0; i < 20; i++) {
        expect(readMetas[i]).not.toBeNull();
        expect(readMetas[i]!.mtime).toBe(600 + i);
      }
    });

    it("syncAll with empty pages + many metas", async () => {
      const metas = Array.from({ length: 20 }, (_, i) => ({
        path: `/dironly_${i}`,
        meta: { size: 0, mode: 0o040755, ctime: 700, mtime: 800 } as FileMeta,
      }));
      await callClient(clientWorker, "syncAll", [[], metas]);

      const paths = metas.map((m) => m.path);
      const result = (await callClient(clientWorker, "readMetas", [paths])) as Array<FileMeta | null>;
      for (let i = 0; i < 20; i++) {
        expect(result[i]!.mode).toBe(0o040755);
      }
    });

    it("@fast syncAll with combined pages + metas overflowing buffer falls back gracefully", async () => {
      // With the small buffer (12 KB data region), maxBatchPages=1 and
      // maxBatchMetas=16. A single page passes the pages check (1 <= 1),
      // and 16 metas passes the metas check (16 <= 16). But each limit
      // assumes the FULL buffer for its payload type. With ~185-char paths,
      // the combined payload is ~12.5 KB: 8192 bytes of page binary data +
      // ~4.3 KB of JSON metadata, exceeding the 12 KB data region.
      //
      // Before the fix, this would throw "SAB buffer overflow" from
      // encodeMessage. After the fix, syncAll catches the overflow and
      // falls back to separate writePages + writeMetas calls.
      const longPath = (i: number) =>
        `/deeply/nested/directory/structure/that/inflates/json/size` +
        `_padding_to_make_this_much_longer_abcdefghijklmnopqrstuvwxyz_0123456789_abcdefghijklmnopqrstuvwxyz_extra_padding_here` +
        `/entry_${String(i).padStart(3, "0")}`;

      const pages = [{ path: longPath(0), pageIndex: 0, data: fillPage(0xee) }];
      const metas = Array.from({ length: 16 }, (_, i) => ({
        path: longPath(i),
        meta: {
          size: i === 0 ? PAGE_SIZE : 0,
          mode: 0o100644,
          ctime: 1100 + i,
          mtime: 1200 + i,
        } as FileMeta,
      }));

      // Should succeed via fallback — not throw
      await callClient(clientWorker, "syncAll", [pages, metas]);

      // Verify page data survived
      const page = toUint8Array(
        await callClient(clientWorker, "readPage", [longPath(0), 0]),
      );
      expect(page[0]).toBe(0xee);

      // Verify all 16 metas survived
      const paths = metas.map((m) => m.path);
      const readMetas = (await callClient(clientWorker, "readMetas", [paths])) as Array<FileMeta | null>;
      for (let i = 0; i < 16; i++) {
        expect(readMetas[i]).not.toBeNull();
        expect(readMetas[i]!.mtime).toBe(1200 + i);
      }
    });

    it("syncAll multi-file pages + metas all survive", async () => {
      const pages = [
        { path: "/x", pageIndex: 0, data: fillPage(0x10) },
        { path: "/x", pageIndex: 1, data: fillPage(0x11) },
        { path: "/y", pageIndex: 0, data: fillPage(0x20) },
      ];
      const metas = [
        { path: "/x", meta: { size: 2 * PAGE_SIZE, mode: 0o100644, ctime: 900, mtime: 1000 } as FileMeta },
        { path: "/y", meta: { size: PAGE_SIZE, mode: 0o100644, ctime: 900, mtime: 1000 } as FileMeta },
      ];
      await callClient(clientWorker, "syncAll", [pages, metas]);

      // Verify /x pages
      const xPages = toPageArray(
        await callClient(clientWorker, "readPages", ["/x", [0, 1]]),
      );
      expect(xPages[0]![0]).toBe(0x10);
      expect(xPages[1]![0]).toBe(0x11);

      // Verify /y page
      const yPage = toUint8Array(
        await callClient(clientWorker, "readPage", ["/y", 0]),
      );
      expect(yPage[0]).toBe(0x20);

      // Verify metas
      const xMeta = (await callClient(clientWorker, "readMeta", ["/x"])) as FileMeta;
      expect(xMeta.size).toBe(2 * PAGE_SIZE);
      const yMeta = (await callClient(clientWorker, "readMeta", ["/y"])) as FileMeta;
      expect(yMeta.size).toBe(PAGE_SIZE);
    });
  });

  // -----------------------------------------------------------------
  // Listing & batch query chunking (maxBatchFiles = 32)
  // -----------------------------------------------------------------

  describe("listing and batch query chunking (maxBatchFiles=32)", () => {
    it("listFiles with 10 files (no pagination)", async () => {
      // Seed the backend with 10 files (each needs a page so listFiles includes them)
      for (let i = 0; i < 10; i++) {
        await callClient(clientWorker, "writeMeta", [
          `/list_${i}`,
          { size: 0, mode: 0o100644, ctime: 0, mtime: 0 },
        ]);
      }
      const files = (await callClient(clientWorker, "listFiles", [])) as string[];
      expect(files).toHaveLength(10);
      for (let i = 0; i < 10; i++) {
        expect(files).toContain(`/list_${i}`);
      }
    });

    it("@fast listFiles with 50 files (paginated into [32, 18])", async () => {
      for (let i = 0; i < 50; i++) {
        await callClient(clientWorker, "writeMeta", [
          `/many_${String(i).padStart(3, "0")}`,
          { size: 0, mode: 0o100644, ctime: 0, mtime: 0 },
        ]);
      }
      const files = (await callClient(clientWorker, "listFiles", [])) as string[];
      expect(files).toHaveLength(50);
      for (let i = 0; i < 50; i++) {
        expect(files).toContain(`/many_${String(i).padStart(3, "0")}`);
      }
    });

    it("countPagesBatch across chunk boundary", async () => {
      // Create 40 files with different page counts
      for (let i = 0; i < 40; i++) {
        const pageCount = (i % 3) + 1; // 1, 2, or 3 pages
        for (let p = 0; p < pageCount; p++) {
          await callClient(clientWorker, "writePage", [
            `/cnt_${i}`,
            p,
            fillPage(i),
          ]);
        }
      }

      const paths = Array.from({ length: 40 }, (_, i) => `/cnt_${i}`);
      const counts = (await callClient(clientWorker, "countPagesBatch", [paths])) as number[];
      expect(counts).toHaveLength(40);
      for (let i = 0; i < 40; i++) {
        expect(counts[i]).toBe((i % 3) + 1);
      }
    });

    it("maxPageIndexBatch across chunk boundary", async () => {
      // Create 35 files with different max page indices
      for (let i = 0; i < 35; i++) {
        const maxIdx = i % 5; // 0, 1, 2, 3, 4
        await callClient(clientWorker, "writePage", [
          `/idx_${i}`,
          maxIdx,
          fillPage(0),
        ]);
      }

      const paths = Array.from({ length: 35 }, (_, i) => `/idx_${i}`);
      const maxIndices = (await callClient(
        clientWorker,
        "maxPageIndexBatch",
        [paths],
      )) as number[];
      expect(maxIndices).toHaveLength(35);
      for (let i = 0; i < 35; i++) {
        expect(maxIndices[i]).toBe(i % 5);
      }
    });
  });

  // -----------------------------------------------------------------
  // deleteFiles chunking (uses maxBatchMetas = 16)
  // -----------------------------------------------------------------

  describe("deleteFiles chunking (maxBatchMetas=16)", () => {
    it("deleteFiles with 20 paths (chunked into [16, 4])", async () => {
      // Write pages for 20 files
      for (let i = 0; i < 20; i++) {
        await callClient(clientWorker, "writePage", [
          `/delf_${i}`,
          0,
          fillPage(i),
        ]);
      }

      // Verify they exist
      for (let i = 0; i < 20; i++) {
        const page = await callClient(clientWorker, "readPage", [
          `/delf_${i}`,
          0,
        ]);
        expect(page).not.toBeNull();
      }

      // Delete all 20 files (chunked)
      const paths = Array.from({ length: 20 }, (_, i) => `/delf_${i}`);
      await callClient(clientWorker, "deleteFiles", [paths]);

      // Verify all deleted
      for (let i = 0; i < 20; i++) {
        const page = await callClient(clientWorker, "readPage", [
          `/delf_${i}`,
          0,
        ]);
        expect(page).toBeNull();
      }
    });
  });

  // -----------------------------------------------------------------
  // deleteAll chunking (uses maxBatchMetas = 16)
  // -----------------------------------------------------------------

  describe("deleteAll chunking (maxBatchMetas=16)", () => {
    it("@fast deleteAll with 20 paths removes pages and metadata (chunked into [16, 4])", async () => {
      // Write pages and metadata for 20 files
      for (let i = 0; i < 20; i++) {
        await callClient(clientWorker, "writePage", [
          `/da_${i}`,
          0,
          fillPage(i),
        ]);
        await callClient(clientWorker, "writeMeta", [
          `/da_${i}`,
          { size: PAGE_SIZE, mode: 0o100644, ctime: 1000, mtime: 2000 } as FileMeta,
        ]);
      }

      // Verify pages and metadata exist
      for (let i = 0; i < 20; i++) {
        const page = await callClient(clientWorker, "readPage", [`/da_${i}`, 0]);
        expect(page).not.toBeNull();
        const meta = await callClient(clientWorker, "readMeta", [`/da_${i}`]);
        expect(meta).not.toBeNull();
      }

      // deleteAll 20 paths — exceeds maxBatchMetas=16, so it chunks
      const paths = Array.from({ length: 20 }, (_, i) => `/da_${i}`);
      await callClient(clientWorker, "deleteAll", [paths]);

      // Verify all pages deleted
      for (let i = 0; i < 20; i++) {
        const page = await callClient(clientWorker, "readPage", [`/da_${i}`, 0]);
        expect(page).toBeNull();
      }

      // Verify all metadata deleted
      const metas = (await callClient(clientWorker, "readMetas", [paths])) as Array<FileMeta | null>;
      for (const meta of metas) {
        expect(meta).toBeNull();
      }
    });

    it("deleteAll with paths at exact chunk boundary (16 paths)", async () => {
      for (let i = 0; i < 16; i++) {
        await callClient(clientWorker, "writePage", [
          `/dab_${i}`,
          0,
          fillPage(0x80 + i),
        ]);
        await callClient(clientWorker, "writeMeta", [
          `/dab_${i}`,
          { size: PAGE_SIZE, mode: 0o100644, ctime: 1000, mtime: 2000 } as FileMeta,
        ]);
      }

      const paths = Array.from({ length: 16 }, (_, i) => `/dab_${i}`);
      await callClient(clientWorker, "deleteAll", [paths]);

      for (let i = 0; i < 16; i++) {
        const page = await callClient(clientWorker, "readPage", [`/dab_${i}`, 0]);
        expect(page).toBeNull();
      }
      const metas = (await callClient(clientWorker, "readMetas", [paths])) as Array<FileMeta | null>;
      for (const meta of metas) {
        expect(meta).toBeNull();
      }
    });

    it("deleteAll with empty array is a no-op", async () => {
      await callClient(clientWorker, "writePage", ["/keep", 0, fillPage(0xff)]);
      await callClient(clientWorker, "writeMeta", [
        "/keep",
        { size: PAGE_SIZE, mode: 0o100644, ctime: 1000, mtime: 2000 } as FileMeta,
      ]);

      await callClient(clientWorker, "deleteAll", [[]]);

      const page = await callClient(clientWorker, "readPage", ["/keep", 0]);
      expect(page).not.toBeNull();
      const meta = await callClient(clientWorker, "readMeta", ["/keep"]);
      expect(meta).not.toBeNull();
    });

    it("deleteAll removes multi-page files across chunk boundary", async () => {
      // Write 20 files with 2 pages each
      for (let i = 0; i < 20; i++) {
        await callClient(clientWorker, "writePages", [
          [
            { path: `/dam_${i}`, pageIndex: 0, data: fillPage(i) },
            { path: `/dam_${i}`, pageIndex: 1, data: fillPage(i + 100) },
          ],
        ]);
        await callClient(clientWorker, "writeMeta", [
          `/dam_${i}`,
          { size: PAGE_SIZE * 2, mode: 0o100644, ctime: 1000, mtime: 2000 } as FileMeta,
        ]);
      }

      const paths = Array.from({ length: 20 }, (_, i) => `/dam_${i}`);
      await callClient(clientWorker, "deleteAll", [paths]);

      // Both pages of every file should be gone
      for (let i = 0; i < 20; i++) {
        expect(await callClient(clientWorker, "readPage", [`/dam_${i}`, 0])).toBeNull();
        expect(await callClient(clientWorker, "readPage", [`/dam_${i}`, 1])).toBeNull();
      }
      const metas = (await callClient(clientWorker, "readMetas", [paths])) as Array<FileMeta | null>;
      for (const meta of metas) {
        expect(meta).toBeNull();
      }
    });

    it("deleteAll only deletes specified paths — others survive", async () => {
      // Write 25 files
      for (let i = 0; i < 25; i++) {
        await callClient(clientWorker, "writePage", [
          `/das_${i}`,
          0,
          fillPage(i),
        ]);
        await callClient(clientWorker, "writeMeta", [
          `/das_${i}`,
          { size: PAGE_SIZE, mode: 0o100644, ctime: 1000, mtime: 3000 + i } as FileMeta,
        ]);
      }

      // Delete only the first 20 (chunked) — leave 5 survivors
      const toDelete = Array.from({ length: 20 }, (_, i) => `/das_${i}`);
      await callClient(clientWorker, "deleteAll", [toDelete]);

      // Deleted files are gone
      for (let i = 0; i < 20; i++) {
        expect(await callClient(clientWorker, "readPage", [`/das_${i}`, 0])).toBeNull();
      }

      // Survivors are intact
      for (let i = 20; i < 25; i++) {
        const page = toUint8Array(
          await callClient(clientWorker, "readPage", [`/das_${i}`, 0]),
        );
        expect(page[0]).toBe(i);
        const meta = (await callClient(clientWorker, "readMeta", [`/das_${i}`])) as FileMeta;
        expect(meta.mtime).toBe(3000 + i);
      }
    });
  });

  // -----------------------------------------------------------------
  // Mixed operations across chunk boundaries
  // -----------------------------------------------------------------

  describe("mixed operations across chunk boundaries", () => {
    it("write + read + overwrite + read cycle with chunked batches", async () => {
      // Write 4 pages
      const initial = Array.from({ length: 4 }, (_, i) => ({
        path: "/cycle",
        pageIndex: i,
        data: fillPage(0x01 + i),
      }));
      await callClient(clientWorker, "writePages", [initial]);

      // Read all 4
      let result = toPageArray(
        await callClient(clientWorker, "readPages", ["/cycle", [0, 1, 2, 3]]),
      );
      for (let i = 0; i < 4; i++) {
        expect(result[i]![0]).toBe(0x01 + i);
      }

      // Overwrite with new data
      const overwrite = Array.from({ length: 4 }, (_, i) => ({
        path: "/cycle",
        pageIndex: i,
        data: fillPage(0xf0 + i),
      }));
      await callClient(clientWorker, "writePages", [overwrite]);

      // Read again — should see new data
      result = toPageArray(
        await callClient(clientWorker, "readPages", ["/cycle", [0, 1, 2, 3]]),
      );
      for (let i = 0; i < 4; i++) {
        expect(result[i]![0]).toBe(0xf0 + i);
      }
    });

    it("@fast interleaved page writes and meta writes across chunk boundaries", async () => {
      // Write pages for 3 files (chunked)
      const pages = [
        { path: "/f1", pageIndex: 0, data: fillPage(0xa1) },
        { path: "/f2", pageIndex: 0, data: fillPage(0xa2) },
        { path: "/f3", pageIndex: 0, data: fillPage(0xa3) },
      ];
      await callClient(clientWorker, "writePages", [pages]);

      // Write 20 metas including these files + extras (chunked into [16, 4])
      const metas = Array.from({ length: 20 }, (_, i) => ({
        path: i < 3 ? `/f${i + 1}` : `/extra_${i}`,
        meta: {
          size: i < 3 ? PAGE_SIZE : 0,
          mode: 0o100644,
          ctime: 1000 + i,
          mtime: 2000 + i,
        } as FileMeta,
      }));
      await callClient(clientWorker, "writeMetas", [metas]);

      // Verify pages
      for (let i = 0; i < 3; i++) {
        const page = toUint8Array(
          await callClient(clientWorker, "readPage", [`/f${i + 1}`, 0]),
        );
        expect(page[0]).toBe(0xa1 + i);
      }

      // Verify all 20 metas
      const paths = metas.map((m) => m.path);
      const readMetas = (await callClient(clientWorker, "readMetas", [paths])) as Array<FileMeta | null>;
      for (let i = 0; i < 20; i++) {
        expect(readMetas[i]!.mtime).toBe(2000 + i);
      }
    });
  });
});
