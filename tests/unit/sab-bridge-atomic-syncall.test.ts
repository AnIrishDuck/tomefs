/**
 * Tests for the APPEND+COMMIT atomic syncAll protocol.
 *
 * When syncAll data exceeds the SAB buffer, the client uses a multi-round
 * APPEND+COMMIT protocol: chunk pages and metas into SYNC_ALL_APPEND calls
 * that the worker accumulates, then SYNC_ALL_COMMIT triggers a single
 * backend.syncAll() — preserving IDB single-transaction atomicity.
 *
 * These tests verify that the chunked path calls backend.syncAll() exactly
 * once (not separate writePages + writeMetas), and that all data arrives.
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
import type { StorageBackend } from "../../src/storage-backend.js";
import { CONTROL_BYTES } from "../../src/sab-protocol.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const WORKER_SRC = join(__dirname, "sab-bridge-timeout-worker.ts");
const WORKER_BUNDLE = join(__dirname, ".sab-bridge-timeout-worker.bundle.mjs");

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

function toUint8Array(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) return value;
  if (value && typeof value === "object" && "data" in value) {
    return new Uint8Array((value as { data: number[] }).data);
  }
  if (Array.isArray(value)) return new Uint8Array(value);
  throw new Error(`Cannot convert to Uint8Array: ${typeof value}`);
}

function fillPage(value: number): Uint8Array {
  const buf = new Uint8Array(PAGE_SIZE);
  buf.fill(value);
  return buf;
}

/**
 * Backend wrapper that records which write methods were called and delegates
 * to a real MemoryBackend. Used to verify that the APPEND+COMMIT protocol
 * routes all data through a single syncAll call instead of separate
 * writePages + writeMetas calls.
 */
class RecordingBackend implements StorageBackend {
  readonly inner = new MemoryBackend();
  readonly calls: Array<{
    method: string;
    pageCount?: number;
    metaCount?: number;
    paths?: string[];
  }> = [];

  clearCalls(): void {
    this.calls.length = 0;
  }

  async readPage(path: string, pageIndex: number) {
    return this.inner.readPage(path, pageIndex);
  }
  async readPages(path: string, pageIndices: number[]) {
    return this.inner.readPages(path, pageIndices);
  }
  async writePage(path: string, pageIndex: number, data: Uint8Array) {
    this.calls.push({ method: "writePage" });
    return this.inner.writePage(path, pageIndex, data);
  }
  async writePages(pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>) {
    this.calls.push({ method: "writePages", pageCount: pages.length });
    return this.inner.writePages(pages);
  }
  async deleteFile(path: string) { return this.inner.deleteFile(path); }
  async deleteFiles(paths: string[]) { return this.inner.deleteFiles(paths); }
  async deletePagesFrom(path: string, fromPageIndex: number) {
    return this.inner.deletePagesFrom(path, fromPageIndex);
  }
  async renameFile(oldPath: string, newPath: string) {
    return this.inner.renameFile(oldPath, newPath);
  }
  async countPages(path: string) { return this.inner.countPages(path); }
  async countPagesBatch(paths: string[]) { return this.inner.countPagesBatch(paths); }
  async maxPageIndex(path: string) { return this.inner.maxPageIndex(path); }
  async maxPageIndexBatch(paths: string[]) { return this.inner.maxPageIndexBatch(paths); }
  async readMeta(path: string) { return this.inner.readMeta(path); }
  async writeMeta(path: string, meta: FileMeta) {
    this.calls.push({ method: "writeMeta" });
    return this.inner.writeMeta(path, meta);
  }
  async writeMetas(entries: Array<{ path: string; meta: FileMeta }>) {
    this.calls.push({ method: "writeMetas", metaCount: entries.length });
    return this.inner.writeMetas(entries);
  }
  async deleteMeta(path: string) { return this.inner.deleteMeta(path); }
  async readMetas(paths: string[]) { return this.inner.readMetas(paths); }
  async deleteMetas(paths: string[]) { return this.inner.deleteMetas(paths); }
  async listFiles() { return this.inner.listFiles(); }
  async syncAll(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
    metas: Array<{ path: string; meta: FileMeta }>,
  ) {
    this.calls.push({
      method: "syncAll",
      pageCount: pages.length,
      metaCount: metas.length,
    });
    return this.inner.syncAll(pages, metas);
  }
  async deleteAll(paths: string[]) {
    this.calls.push({ method: "deleteAll", paths });
    return this.inner.deleteAll(paths);
  }
}

/**
 * Small buffer: CONTROL_BYTES + 12 KB data region.
 * maxBatchPages=1, maxBatchMetas=16 — forces chunking on small batches.
 */
const SMALL_BUFFER_SIZE = CONTROL_BYTES + 12288;

describe("SAB bridge: atomic APPEND+COMMIT syncAll", () => {
  let backend: RecordingBackend;
  let sabWorker: SabWorker;
  let clientWorker: Worker;
  let sab: SharedArrayBuffer;

  beforeAll(async () => {
    await buildWorkerBundle();
  });

  beforeEach(async () => {
    backend = new RecordingBackend();
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

  it("@fast syncAll fast path calls backend.syncAll once", async () => {
    const pages = [{ path: "/f", pageIndex: 0, data: fillPage(0xaa) }];
    const metas = [
      { path: "/f", meta: { size: PAGE_SIZE, mode: 0o100644, ctime: 1, mtime: 2 } as FileMeta },
    ];

    backend.clearCalls();
    await callClient(clientWorker, "syncAll", [pages, metas]);

    const syncCalls = backend.calls.filter((c) => c.method === "syncAll");
    expect(syncCalls).toHaveLength(1);
    expect(syncCalls[0].pageCount).toBe(1);
    expect(syncCalls[0].metaCount).toBe(1);

    const writeCalls = backend.calls.filter(
      (c) => c.method === "writePages" || c.method === "writeMetas",
    );
    expect(writeCalls).toHaveLength(0);
  });

  it("@fast chunked syncAll with 5 pages calls backend.syncAll once (not writePages + writeMetas)", async () => {
    const pages = Array.from({ length: 5 }, (_, i) => ({
      path: "/multi",
      pageIndex: i,
      data: fillPage(0xcc + i),
    }));
    const metas = [
      {
        path: "/multi",
        meta: { size: 5 * PAGE_SIZE, mode: 0o100644, ctime: 1, mtime: 2 } as FileMeta,
      },
    ];

    backend.clearCalls();
    await callClient(clientWorker, "syncAll", [pages, metas]);

    const syncCalls = backend.calls.filter((c) => c.method === "syncAll");
    expect(syncCalls).toHaveLength(1);
    expect(syncCalls[0].pageCount).toBe(5);
    expect(syncCalls[0].metaCount).toBe(1);

    const writeCalls = backend.calls.filter(
      (c) => c.method === "writePages" || c.method === "writeMetas",
    );
    expect(writeCalls).toHaveLength(0);

    // Verify data integrity
    const page0 = await backend.inner.readPage("/multi", 0);
    expect(page0![0]).toBe(0xcc);
    const page4 = await backend.inner.readPage("/multi", 4);
    expect(page4![0]).toBe((0xcc + 4) & 0xff);
    const meta = await backend.inner.readMeta("/multi");
    expect(meta!.size).toBe(5 * PAGE_SIZE);
  });

  it("@fast chunked syncAll with many metas calls backend.syncAll once", async () => {
    const pages = [{ path: "/file0", pageIndex: 0, data: fillPage(0xdd) }];
    const metas = Array.from({ length: 20 }, (_, i) => ({
      path: `/file${i}`,
      meta: { size: i === 0 ? PAGE_SIZE : 0, mode: 0o100644, ctime: 500 + i, mtime: 600 + i } as FileMeta,
    }));

    backend.clearCalls();
    await callClient(clientWorker, "syncAll", [pages, metas]);

    const syncCalls = backend.calls.filter((c) => c.method === "syncAll");
    expect(syncCalls).toHaveLength(1);
    expect(syncCalls[0].pageCount).toBe(1);
    expect(syncCalls[0].metaCount).toBe(20);

    const writeCalls = backend.calls.filter(
      (c) => c.method === "writePages" || c.method === "writeMetas",
    );
    expect(writeCalls).toHaveLength(0);
  });

  it("combined pages + metas overflow uses APPEND+COMMIT (not writePages + writeMetas)", async () => {
    const longPath = (i: number) =>
      `/deeply/nested/directory/structure/that/inflates/json/size` +
      `_padding_to_make_this_much_longer_abcdefghijklmnopqrstuvwxyz_0123456789_abcdefghijklmnopqrstuvwxyz_extra_padding_here` +
      `/entry_${String(i).padStart(3, "0")}`;

    const pages = [{ path: longPath(0), pageIndex: 0, data: fillPage(0xee) }];
    const metas = Array.from({ length: 16 }, (_, i) => ({
      path: longPath(i),
      meta: { size: i === 0 ? PAGE_SIZE : 0, mode: 0o100644, ctime: 1100 + i, mtime: 1200 + i } as FileMeta,
    }));

    backend.clearCalls();
    await callClient(clientWorker, "syncAll", [pages, metas]);

    const syncCalls = backend.calls.filter((c) => c.method === "syncAll");
    expect(syncCalls).toHaveLength(1);
    expect(syncCalls[0].pageCount).toBe(1);
    expect(syncCalls[0].metaCount).toBe(16);

    const writeCalls = backend.calls.filter(
      (c) => c.method === "writePages" || c.method === "writeMetas",
    );
    expect(writeCalls).toHaveLength(0);
  });

  it("pages-only chunked syncAll (no metas)", async () => {
    const pages = Array.from({ length: 3 }, (_, i) => ({
      path: "/ponly",
      pageIndex: i,
      data: fillPage(0x10 + i),
    }));

    backend.clearCalls();
    await callClient(clientWorker, "syncAll", [pages, []]);

    const syncCalls = backend.calls.filter((c) => c.method === "syncAll");
    expect(syncCalls).toHaveLength(1);
    expect(syncCalls[0].pageCount).toBe(3);
    expect(syncCalls[0].metaCount).toBe(0);
  });

  it("metas-only chunked syncAll (no pages)", async () => {
    const metas = Array.from({ length: 20 }, (_, i) => ({
      path: `/dir_${i}`,
      meta: { size: 0, mode: 0o040755, ctime: 700, mtime: 800 } as FileMeta,
    }));

    backend.clearCalls();
    await callClient(clientWorker, "syncAll", [[], metas]);

    const syncCalls = backend.calls.filter((c) => c.method === "syncAll");
    expect(syncCalls).toHaveLength(1);
    expect(syncCalls[0].pageCount).toBe(0);
    expect(syncCalls[0].metaCount).toBe(20);

    // Verify all metas arrived
    for (let i = 0; i < 20; i++) {
      const m = await backend.inner.readMeta(`/dir_${i}`);
      expect(m).not.toBeNull();
      expect(m!.mode).toBe(0o040755);
    }
  });

  it("multi-file pages + metas all arrive in single syncAll", async () => {
    const pages = [
      { path: "/x", pageIndex: 0, data: fillPage(0x10) },
      { path: "/x", pageIndex: 1, data: fillPage(0x11) },
      { path: "/y", pageIndex: 0, data: fillPage(0x20) },
    ];
    const metas = [
      { path: "/x", meta: { size: 2 * PAGE_SIZE, mode: 0o100644, ctime: 1, mtime: 2 } as FileMeta },
      { path: "/y", meta: { size: PAGE_SIZE, mode: 0o100644, ctime: 3, mtime: 4 } as FileMeta },
    ];

    backend.clearCalls();
    await callClient(clientWorker, "syncAll", [pages, metas]);

    const syncCalls = backend.calls.filter((c) => c.method === "syncAll");
    expect(syncCalls).toHaveLength(1);
    expect(syncCalls[0].pageCount).toBe(3);
    expect(syncCalls[0].metaCount).toBe(2);
  });

  it("page data integrity after chunked APPEND+COMMIT", async () => {
    const pages = Array.from({ length: 5 }, (_, i) => {
      const page = new Uint8Array(PAGE_SIZE);
      for (let j = 0; j < PAGE_SIZE; j++) {
        page[j] = (i * 37 + j) & 0xff;
      }
      return { path: `/integrity`, pageIndex: i, data: page };
    });
    const metas = [
      { path: "/integrity", meta: { size: 5 * PAGE_SIZE, mode: 0o100644, ctime: 0, mtime: 0 } as FileMeta },
    ];

    await callClient(clientWorker, "syncAll", [pages, metas]);

    for (let i = 0; i < 5; i++) {
      const result = await callClient(clientWorker, "readPage", ["/integrity", i]);
      const buf = toUint8Array(result);
      expect(buf.length).toBe(PAGE_SIZE);
      for (let j = 0; j < PAGE_SIZE; j++) {
        if (buf[j] !== ((i * 37 + j) & 0xff)) {
          throw new Error(
            `Byte mismatch: page ${i}, offset ${j}: expected ${(i * 37 + j) & 0xff}, got ${buf[j]}`,
          );
        }
      }
    }
  });

  it("consecutive chunked syncAll calls do not leak pending state", async () => {
    // First syncAll: 3 pages, 1 meta
    const pages1 = Array.from({ length: 3 }, (_, i) => ({
      path: "/first",
      pageIndex: i,
      data: fillPage(0xa0 + i),
    }));
    const metas1 = [
      { path: "/first", meta: { size: 3 * PAGE_SIZE, mode: 0o100644, ctime: 1, mtime: 1 } as FileMeta },
    ];

    backend.clearCalls();
    await callClient(clientWorker, "syncAll", [pages1, metas1]);

    let syncCalls = backend.calls.filter((c) => c.method === "syncAll");
    expect(syncCalls).toHaveLength(1);
    expect(syncCalls[0].pageCount).toBe(3);

    // Second syncAll: 2 pages, 2 metas — should NOT include pages from first call
    const pages2 = Array.from({ length: 2 }, (_, i) => ({
      path: "/second",
      pageIndex: i,
      data: fillPage(0xb0 + i),
    }));
    const metas2 = [
      { path: "/second", meta: { size: 2 * PAGE_SIZE, mode: 0o100644, ctime: 2, mtime: 2 } as FileMeta },
      { path: "/second2", meta: { size: 0, mode: 0o100644, ctime: 3, mtime: 3 } as FileMeta },
    ];

    backend.clearCalls();
    await callClient(clientWorker, "syncAll", [pages2, metas2]);

    syncCalls = backend.calls.filter((c) => c.method === "syncAll");
    expect(syncCalls).toHaveLength(1);
    expect(syncCalls[0].pageCount).toBe(2);
    expect(syncCalls[0].metaCount).toBe(2);
  });
});
