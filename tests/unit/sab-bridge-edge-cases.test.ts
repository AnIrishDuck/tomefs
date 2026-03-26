/**
 * Edge case tests for the SAB+Atomics bridge.
 *
 * Covers scenarios not tested in sab-bridge.test.ts:
 * - Buffer overflow detection (message exceeds SharedArrayBuffer capacity)
 * - Timeout recovery (storage worker unresponsive)
 * - Backend error propagation (storage backend throws)
 * - Large batch operations near buffer limits
 * - Protocol recovery after errors (bridge remains usable)
 */
import { describe, it, expect, beforeEach, afterEach, beforeAll } from "vitest";
import { Worker } from "node:worker_threads";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { MemoryBackend } from "../../src/memory-backend.js";
import { SabWorker } from "../../src/sab-worker.js";
import { SabClient } from "../../src/sab-client.js";
import { PAGE_SIZE } from "../../src/types.js";
import type { StorageBackend } from "../../src/storage-backend.js";
import type { FileMeta } from "../../src/types.js";
import {
  encodeMessage,
  decodeMessage,
  CONTROL_BYTES,
  JSON_REGION_OFFSET,
  DEFAULT_BUFFER_SIZE,
} from "../../src/sab-protocol.js";

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

/** Convert structured-clone result back to Uint8Array. */
function toUint8Array(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) return value;
  if (value && typeof value === "object" && "data" in value) {
    return new Uint8Array((value as { data: number[] }).data);
  }
  if (Array.isArray(value)) return new Uint8Array(value);
  throw new Error(`Cannot convert to Uint8Array: ${typeof value}`);
}

/**
 * A storage backend that can be configured to fail on specific operations.
 * Not a mock — it wraps a real MemoryBackend and injects failures.
 */
class FailingBackend implements StorageBackend {
  private inner = new MemoryBackend();
  failOn: string | null = null;
  failMessage = "Injected backend failure";

  private maybeThrow(method: string): void {
    if (this.failOn === method) {
      throw new Error(this.failMessage);
    }
  }

  async readPage(path: string, pageIndex: number): Promise<Uint8Array | null> {
    this.maybeThrow("readPage");
    return this.inner.readPage(path, pageIndex);
  }

  async readPages(path: string, pageIndices: number[]): Promise<Array<Uint8Array | null>> {
    this.maybeThrow("readPages");
    return this.inner.readPages(path, pageIndices);
  }

  async writePage(path: string, pageIndex: number, data: Uint8Array): Promise<void> {
    this.maybeThrow("writePage");
    return this.inner.writePage(path, pageIndex, data);
  }

  async writePages(pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>): Promise<void> {
    this.maybeThrow("writePages");
    return this.inner.writePages(pages);
  }

  async deleteFile(path: string): Promise<void> {
    this.maybeThrow("deleteFile");
    return this.inner.deleteFile(path);
  }

  async deletePagesFrom(path: string, fromPageIndex: number): Promise<void> {
    this.maybeThrow("deletePagesFrom");
    return this.inner.deletePagesFrom(path, fromPageIndex);
  }

  async renameFile(oldPath: string, newPath: string): Promise<void> {
    this.maybeThrow("renameFile");
    return this.inner.renameFile(oldPath, newPath);
  }

  async readMeta(path: string): Promise<FileMeta | null> {
    this.maybeThrow("readMeta");
    return this.inner.readMeta(path);
  }

  async writeMeta(path: string, meta: FileMeta): Promise<void> {
    this.maybeThrow("writeMeta");
    return this.inner.writeMeta(path, meta);
  }

  async writeMetas(entries: Array<{ path: string; meta: FileMeta }>): Promise<void> {
    this.maybeThrow("writeMetas");
    return this.inner.writeMetas(entries);
  }

  async deleteMeta(path: string): Promise<void> {
    this.maybeThrow("deleteMeta");
    return this.inner.deleteMeta(path);
  }

  async deleteMetas(paths: string[]): Promise<void> {
    this.maybeThrow("deleteMetas");
    return this.inner.deleteMetas(paths);
  }

  async listFiles(): Promise<string[]> {
    this.maybeThrow("listFiles");
    return this.inner.listFiles();
  }
}

/**
 * A storage backend where readPage takes a configurable delay.
 * Used to test timeout behavior.
 */
class SlowBackend implements StorageBackend {
  private inner = new MemoryBackend();
  readDelay = 0;

  async readPage(path: string, pageIndex: number): Promise<Uint8Array | null> {
    if (this.readDelay > 0) {
      await new Promise((r) => setTimeout(r, this.readDelay));
    }
    return this.inner.readPage(path, pageIndex);
  }

  async readPages(path: string, pageIndices: number[]): Promise<Array<Uint8Array | null>> {
    if (this.readDelay > 0) {
      await new Promise((r) => setTimeout(r, this.readDelay));
    }
    return this.inner.readPages(path, pageIndices);
  }

  async writePage(path: string, pageIndex: number, data: Uint8Array): Promise<void> {
    return this.inner.writePage(path, pageIndex, data);
  }

  async writePages(pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>): Promise<void> {
    return this.inner.writePages(pages);
  }

  async deleteFile(path: string): Promise<void> {
    return this.inner.deleteFile(path);
  }

  async deletePagesFrom(path: string, fromPageIndex: number): Promise<void> {
    return this.inner.deletePagesFrom(path, fromPageIndex);
  }

  async renameFile(oldPath: string, newPath: string): Promise<void> {
    return this.inner.renameFile(oldPath, newPath);
  }

  async readMeta(path: string): Promise<FileMeta | null> {
    return this.inner.readMeta(path);
  }

  async writeMeta(path: string, meta: FileMeta): Promise<void> {
    return this.inner.writeMeta(path, meta);
  }

  async writeMetas(entries: Array<{ path: string; meta: FileMeta }>): Promise<void> {
    return this.inner.writeMetas(entries);
  }

  async deleteMeta(path: string): Promise<void> {
    return this.inner.deleteMeta(path);
  }

  async deleteMetas(paths: string[]): Promise<void> {
    return this.inner.deleteMetas(paths);
  }

  async listFiles(): Promise<string[]> {
    return this.inner.listFiles();
  }
}

describe("SAB bridge: buffer overflow", () => {
  it("@fast encodeMessage throws when message exceeds buffer", () => {
    // Create a tiny buffer (just control region + 100 bytes)
    const smallBuf = new ArrayBuffer(CONTROL_BYTES + 100);
    const dataView = new DataView(smallBuf);
    const uint8View = new Uint8Array(smallBuf);

    // Try to encode a message with binary data that exceeds the buffer
    const largeChunk = new Uint8Array(200);
    expect(() => {
      encodeMessage(dataView, uint8View, { test: true }, [largeChunk]);
    }).toThrow(/SAB buffer overflow/);
  });

  it("encodeMessage throws for large writePages batch exceeding default buffer", () => {
    // Default buffer is ~1MB. 150 pages × 8KB = 1.2MB — should overflow.
    const buf = new ArrayBuffer(DEFAULT_BUFFER_SIZE);
    const dataView = new DataView(buf);
    const uint8View = new Uint8Array(buf);

    const chunks: Uint8Array[] = [];
    const meta: Array<{ path: string; pageIndex: number; dataLen: number }> = [];
    for (let i = 0; i < 150; i++) {
      chunks.push(new Uint8Array(PAGE_SIZE));
      meta.push({ path: "/big", pageIndex: i, dataLen: PAGE_SIZE });
    }

    expect(() => {
      encodeMessage(dataView, uint8View, { pages: meta }, chunks);
    }).toThrow(/SAB buffer overflow/);
  });

  it("encodeMessage succeeds for batch that fits in buffer", () => {
    // 64 pages × 8KB = 512KB — fits in 1MB buffer
    const buf = new ArrayBuffer(DEFAULT_BUFFER_SIZE);
    const dataView = new DataView(buf);
    const uint8View = new Uint8Array(buf);

    const chunks: Uint8Array[] = [];
    const meta: Array<{ path: string; pageIndex: number; dataLen: number }> = [];
    for (let i = 0; i < 64; i++) {
      chunks.push(new Uint8Array(PAGE_SIZE));
      meta.push({ path: "/ok", pageIndex: i, dataLen: PAGE_SIZE });
    }

    const len = encodeMessage(dataView, uint8View, { pages: meta }, chunks);
    expect(len).toBeGreaterThan(0);
  });

  it("encodeMessage overflow error includes helpful size information", () => {
    const smallBuf = new ArrayBuffer(CONTROL_BYTES + 50);
    const dataView = new DataView(smallBuf);
    const uint8View = new Uint8Array(smallBuf);

    try {
      encodeMessage(dataView, uint8View, { data: "x".repeat(100) });
      expect.unreachable("should have thrown");
    } catch (err: unknown) {
      const msg = (err as Error).message;
      expect(msg).toContain("requires");
      expect(msg).toContain("bytes");
      expect(msg).toContain("buffer is");
    }
  });

  it("encodeMessage with JSON-only payload near buffer limit", () => {
    // A very large JSON object with no binary data
    const buf = new ArrayBuffer(CONTROL_BYTES + 500);
    const dataView = new DataView(buf);
    const uint8View = new Uint8Array(buf);

    // JSON that fits
    const smallJson = { files: Array.from({ length: 10 }, (_, i) => `/f${i}`) };
    const len = encodeMessage(dataView, uint8View, smallJson);
    expect(len).toBeGreaterThan(0);

    // JSON that doesn't fit
    const largeJson = { files: Array.from({ length: 100 }, (_, i) => `/file-with-long-path-${i}`) };
    expect(() => {
      encodeMessage(dataView, uint8View, largeJson);
    }).toThrow(/SAB buffer overflow/);
  });
});

describe("SAB bridge: buffer overflow through client writePages", () => {
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

  it("client writePages with oversized batch throws buffer overflow error", async () => {
    // Try to encode 150 pages through the worker — should fail with overflow
    await expect(
      callClient(clientWorker, "encodeOverflow", [150]),
    ).rejects.toThrow(/SAB buffer overflow/);
  });

  it("client writePages within limits succeeds through bridge", async () => {
    // Write 10 pages — well within 1MB buffer
    const pages = Array.from({ length: 10 }, (_, i) => ({
      path: "/batchfile",
      pageIndex: i,
      data: new Uint8Array(PAGE_SIZE),
    }));
    // Fill each page with a recognizable byte
    pages.forEach((p, i) => p.data.fill(i & 0xff));

    await callClient(clientWorker, "writePages", [pages]);

    // Verify through the bridge
    for (let i = 0; i < 10; i++) {
      const result = await callClient(clientWorker, "readPage", ["/batchfile", i]);
      const buf = toUint8Array(result);
      expect(buf[0]).toBe(i & 0xff);
    }
  });
});

describe("SAB bridge: timeout", () => {
  let slowBackend: SlowBackend;
  let sabWorker: SabWorker;
  let clientWorker: Worker;
  let sab: SharedArrayBuffer;

  beforeAll(async () => {
    await buildWorkerBundle();
  });

  beforeEach(async () => {
    slowBackend = new SlowBackend();
    sab = SabClient.createBuffer();
    sabWorker = new SabWorker(sab, slowBackend);
    sabWorker.start();

    // Client with 200ms timeout
    clientWorker = new Worker(WORKER_BUNDLE, {
      workerData: { sab, timeout: 200 },
    });
    await waitReady(clientWorker);
  });

  afterEach(async () => {
    sabWorker.stop();
    await clientWorker.terminate();
  });

  it("@fast client throws on timeout when worker is slow", async () => {
    // Make backend take 2 seconds — well beyond 200ms timeout
    slowBackend.readDelay = 2000;

    await expect(
      callClient(clientWorker, "readPage", ["/test", 0]),
    ).rejects.toThrow(/SAB bridge timeout/);
  });

  it("bridge recovers after timeout — subsequent fast calls succeed", async () => {
    // First call: slow, times out
    slowBackend.readDelay = 2000;
    await expect(
      callClient(clientWorker, "readPage", ["/slow", 0]),
    ).rejects.toThrow(/SAB bridge timeout/);

    // Wait for the slow backend operation to complete so the worker settles
    await new Promise((r) => setTimeout(r, 2500));

    // Now make it fast again
    slowBackend.readDelay = 0;

    // Write and read should work
    const data = new Uint8Array(PAGE_SIZE);
    data[0] = 0x42;
    await callClient(clientWorker, "writePage", ["/recovery", 0, data]);
    const result = await callClient(clientWorker, "readPage", ["/recovery", 0]);
    const buf = toUint8Array(result);
    expect(buf[0]).toBe(0x42);
  }, 10000);

  it("fast operations succeed within timeout", async () => {
    // No delay — should complete well within 200ms
    slowBackend.readDelay = 0;

    const data = new Uint8Array(PAGE_SIZE);
    data[0] = 0xaa;
    await callClient(clientWorker, "writePage", ["/fast", 0, data]);
    const result = await callClient(clientWorker, "readPage", ["/fast", 0]);
    expect(toUint8Array(result)[0]).toBe(0xaa);
  });
});

describe("SAB bridge: backend error propagation", () => {
  let failingBackend: FailingBackend;
  let sabWorker: SabWorker;
  let clientWorker: Worker;
  let sab: SharedArrayBuffer;

  beforeAll(async () => {
    await buildWorkerBundle();
  });

  beforeEach(async () => {
    failingBackend = new FailingBackend();
    sab = SabClient.createBuffer();
    sabWorker = new SabWorker(sab, failingBackend);
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

  it("@fast readPage error propagates through bridge", async () => {
    failingBackend.failOn = "readPage";
    failingBackend.failMessage = "IDB read failed";

    await expect(
      callClient(clientWorker, "readPage", ["/test", 0]),
    ).rejects.toThrow(/IDB read failed/);
  });

  it("writePage error propagates through bridge", async () => {
    failingBackend.failOn = "writePage";
    failingBackend.failMessage = "IDB write quota exceeded";

    const data = new Uint8Array(PAGE_SIZE);
    await expect(
      callClient(clientWorker, "writePage", ["/test", 0, data]),
    ).rejects.toThrow(/IDB write quota exceeded/);
  });

  it("writePages batch error propagates through bridge", async () => {
    failingBackend.failOn = "writePages";
    failingBackend.failMessage = "IDB batch transaction failed";

    const pages = [{ path: "/test", pageIndex: 0, data: new Uint8Array(PAGE_SIZE) }];
    await expect(
      callClient(clientWorker, "writePages", [pages]),
    ).rejects.toThrow(/IDB batch transaction failed/);
  });

  it("listFiles error propagates through bridge", async () => {
    failingBackend.failOn = "listFiles";
    failingBackend.failMessage = "IDB cursor failed";

    await expect(
      callClient(clientWorker, "listFiles", []),
    ).rejects.toThrow(/IDB cursor failed/);
  });

  it("readMeta error propagates through bridge", async () => {
    failingBackend.failOn = "readMeta";

    await expect(
      callClient(clientWorker, "readMeta", ["/test"]),
    ).rejects.toThrow(/Injected backend failure/);
  });

  it("writeMeta error propagates through bridge", async () => {
    failingBackend.failOn = "writeMeta";

    const meta = { size: 0, mode: 0o644, ctime: 0, mtime: 0 };
    await expect(
      callClient(clientWorker, "writeMeta", ["/test", meta]),
    ).rejects.toThrow(/Injected backend failure/);
  });

  it("bridge recovers after backend error — next call succeeds", async () => {
    // First: fail a read
    failingBackend.failOn = "readPage";
    await expect(
      callClient(clientWorker, "readPage", ["/test", 0]),
    ).rejects.toThrow();

    // Clear the failure
    failingBackend.failOn = null;

    // Write + read should now work
    const data = new Uint8Array(PAGE_SIZE);
    data[0] = 0xcc;
    await callClient(clientWorker, "writePage", ["/test", 0, data]);

    const result = await callClient(clientWorker, "readPage", ["/test", 0]);
    const buf = toUint8Array(result);
    expect(buf[0]).toBe(0xcc);
  });

  it("multiple consecutive errors don't corrupt bridge state", async () => {
    failingBackend.failOn = "readPage";

    // 5 consecutive errors
    for (let i = 0; i < 5; i++) {
      await expect(
        callClient(clientWorker, "readPage", ["/test", i]),
      ).rejects.toThrow();
    }

    // Clear failure and verify bridge still works
    failingBackend.failOn = null;
    const meta = { size: 100, mode: 0o644, ctime: 1000, mtime: 2000 };
    await callClient(clientWorker, "writeMeta", ["/recovered", meta]);

    const result = await callClient(clientWorker, "readMeta", ["/recovered"]);
    expect(result).toEqual(meta);
  });

  it("error on one operation type doesn't affect other operation types", async () => {
    // readPage fails, but writeMeta should work
    failingBackend.failOn = "readPage";

    await expect(
      callClient(clientWorker, "readPage", ["/test", 0]),
    ).rejects.toThrow();

    // writeMeta should succeed since only readPage fails
    const meta = { size: 0, mode: 0o644, ctime: 0, mtime: 0 };
    await callClient(clientWorker, "writeMeta", ["/test", meta]);

    const result = await callClient(clientWorker, "readMeta", ["/test"]);
    expect(result).toEqual(meta);
  });
});

describe("SAB bridge: large batch operations", () => {
  let backend: MemoryBackend;
  let sabWorker: SabWorker;
  let clientWorker: Worker;
  let sab: SharedArrayBuffer;

  beforeAll(async () => {
    await buildWorkerBundle();
  });

  beforeEach(async () => {
    backend = new MemoryBackend();
    // Use a larger buffer for batch tests: 4MB
    sab = SabClient.createBuffer(CONTROL_BYTES + 4 * 1024 * 1024);
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

  it("writePages batch of 100 pages (800KB) through bridge", async () => {
    const pages = Array.from({ length: 100 }, (_, i) => {
      const data = new Uint8Array(PAGE_SIZE);
      data[0] = i & 0xff;
      data[PAGE_SIZE - 1] = (i * 7) & 0xff;
      return { path: "/bigbatch", pageIndex: i, data };
    });

    await callClient(clientWorker, "writePages", [pages]);

    // Verify a sample
    for (const idx of [0, 25, 50, 75, 99]) {
      const result = await callClient(clientWorker, "readPage", ["/bigbatch", idx]);
      const buf = toUint8Array(result);
      expect(buf[0]).toBe(idx & 0xff);
      expect(buf[PAGE_SIZE - 1]).toBe((idx * 7) & 0xff);
    }
  });

  it("writePages batch of 200 pages with custom buffer succeeds", async () => {
    const pages = Array.from({ length: 200 }, (_, i) => {
      const data = new Uint8Array(PAGE_SIZE);
      data[0] = i & 0xff;
      return { path: "/huge", pageIndex: i, data };
    });

    await callClient(clientWorker, "writePages", [pages]);

    // Spot check
    const first = await callClient(clientWorker, "readPage", ["/huge", 0]);
    expect(toUint8Array(first)[0]).toBe(0);

    const last = await callClient(clientWorker, "readPage", ["/huge", 199]);
    expect(toUint8Array(last)[0]).toBe(199 & 0xff);
  });

  it("100 sequential write+read cycles don't leak or corrupt", async () => {
    for (let i = 0; i < 100; i++) {
      const data = new Uint8Array(PAGE_SIZE);
      data[0] = i & 0xff;
      data[1] = (i >> 8) & 0xff;
      await callClient(clientWorker, "writePage", [`/seq${i}`, 0, data]);
    }

    // Read all back
    for (let i = 0; i < 100; i++) {
      const result = await callClient(clientWorker, "readPage", [`/seq${i}`, 0]);
      const buf = toUint8Array(result);
      expect(buf[0]).toBe(i & 0xff);
      expect(buf[1]).toBe((i >> 8) & 0xff);
    }
  });
});

describe("SAB bridge: protocol edge cases", () => {
  it("decodeMessage with zero-length binary section", () => {
    const buf = new ArrayBuffer(CONTROL_BYTES + 200);
    const dataView = new DataView(buf);
    const uint8View = new Uint8Array(buf);

    const json = { test: true, value: 42 };
    const totalLen = encodeMessage(dataView, uint8View, json);

    const decoded = decodeMessage(dataView, uint8View, totalLen);
    expect(decoded.json).toEqual(json);
    expect(decoded.binary.length).toBe(0);
  });

  it("encodeMessage + decodeMessage round-trip with binary data", () => {
    const buf = new ArrayBuffer(CONTROL_BYTES + 1024);
    const dataView = new DataView(buf);
    const uint8View = new Uint8Array(buf);

    const json = { found: true, extra: "data" };
    const binary = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]);
    const totalLen = encodeMessage(dataView, uint8View, json, [binary]);

    const decoded = decodeMessage(dataView, uint8View, totalLen);
    expect(decoded.json).toEqual(json);
    expect(Array.from(decoded.binary)).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
  });

  it("encodeMessage + decodeMessage with multiple binary chunks", () => {
    const buf = new ArrayBuffer(CONTROL_BYTES + 1024);
    const dataView = new DataView(buf);
    const uint8View = new Uint8Array(buf);

    const chunk1 = new Uint8Array([0xaa, 0xbb]);
    const chunk2 = new Uint8Array([0xcc, 0xdd, 0xee]);
    const json = { chunks: 2 };
    const totalLen = encodeMessage(dataView, uint8View, json, [chunk1, chunk2]);

    const decoded = decodeMessage(dataView, uint8View, totalLen);
    expect(decoded.json).toEqual(json);
    // Binary chunks are concatenated
    expect(Array.from(decoded.binary)).toEqual([0xaa, 0xbb, 0xcc, 0xdd, 0xee]);
  });

  it("encodeMessage with empty JSON and no binary", () => {
    const buf = new ArrayBuffer(CONTROL_BYTES + 100);
    const dataView = new DataView(buf);
    const uint8View = new Uint8Array(buf);

    const totalLen = encodeMessage(dataView, uint8View, {});
    const decoded = decodeMessage(dataView, uint8View, totalLen);
    expect(decoded.json).toEqual({});
    expect(decoded.binary.length).toBe(0);
  });

  it("encodeMessage with special characters in JSON", () => {
    const buf = new ArrayBuffer(CONTROL_BYTES + 1024);
    const dataView = new DataView(buf);
    const uint8View = new Uint8Array(buf);

    const json = {
      path: "/files/日本語/файл.txt",
      emoji: "📁",
      newlines: "line1\nline2\ttab",
      nullChar: "a\0b",
    };
    const totalLen = encodeMessage(dataView, uint8View, json);
    const decoded = decodeMessage(dataView, uint8View, totalLen);
    expect(decoded.json).toEqual(json);
  });

  it("encodeMessage exactly fills buffer (boundary case)", () => {
    // Calculate exact buffer size needed for a known payload
    const json = { x: 1 };
    const jsonStr = JSON.stringify(json);
    const jsonBytes = new TextEncoder().encode(jsonStr);
    const binaryData = new Uint8Array(100);
    const exactSize = CONTROL_BYTES + 4 + jsonBytes.length + binaryData.length;

    const buf = new ArrayBuffer(exactSize);
    const dataView = new DataView(buf);
    const uint8View = new Uint8Array(buf);

    // Should succeed — exactly fits
    const len = encodeMessage(dataView, uint8View, json, [binaryData]);
    expect(len).toBeGreaterThan(0);
  });

  it("encodeMessage fails when one byte over buffer", () => {
    const json = { x: 1 };
    const jsonStr = JSON.stringify(json);
    const jsonBytes = new TextEncoder().encode(jsonStr);
    const binaryData = new Uint8Array(100);
    const exactSize = CONTROL_BYTES + 4 + jsonBytes.length + binaryData.length;

    // One byte too small
    const buf = new ArrayBuffer(exactSize - 1);
    const dataView = new DataView(buf);
    const uint8View = new Uint8Array(buf);

    expect(() => {
      encodeMessage(dataView, uint8View, json, [binaryData]);
    }).toThrow(/SAB buffer overflow/);
  });
});

describe("SAB bridge: decodeMessage validation", () => {
  it("@fast rejects totalLen < 4 (minimum for JSON length prefix)", () => {
    const buf = new ArrayBuffer(CONTROL_BYTES + 200);
    const dataView = new DataView(buf);
    const uint8View = new Uint8Array(buf);

    expect(() => decodeMessage(dataView, uint8View, 0)).toThrow(
      /SAB decode error.*totalLen 0/,
    );
    expect(() => decodeMessage(dataView, uint8View, 3)).toThrow(
      /SAB decode error.*totalLen 3/,
    );
  });

  it("@fast rejects totalLen exceeding buffer capacity", () => {
    const buf = new ArrayBuffer(CONTROL_BYTES + 100);
    const dataView = new DataView(buf);
    const uint8View = new Uint8Array(buf);

    expect(() => decodeMessage(dataView, uint8View, 200)).toThrow(
      /SAB decode error.*totalLen 200/,
    );
  });

  it("@fast rejects jsonLen exceeding totalLen", () => {
    const buf = new ArrayBuffer(CONTROL_BYTES + 200);
    const dataView = new DataView(buf);
    const uint8View = new Uint8Array(buf);

    // Write a valid totalLen but corrupt jsonLen to be larger
    const totalLen = 20;
    dataView.setUint32(JSON_REGION_OFFSET, 9999, true); // jsonLen = 9999

    expect(() => decodeMessage(dataView, uint8View, totalLen)).toThrow(
      /SAB decode error.*jsonLen 9999/,
    );
  });

  it("accepts valid messages at exact boundary", () => {
    const buf = new ArrayBuffer(CONTROL_BYTES + 200);
    const dataView = new DataView(buf);
    const uint8View = new Uint8Array(buf);

    // Encode a real message, then decode with exact totalLen
    const totalLen = encodeMessage(dataView, uint8View, { ok: true });
    const decoded = decodeMessage(dataView, uint8View, totalLen);
    expect(decoded.json).toEqual({ ok: true });
  });

  it("rejects negative totalLen", () => {
    const buf = new ArrayBuffer(CONTROL_BYTES + 200);
    const dataView = new DataView(buf);
    const uint8View = new Uint8Array(buf);

    expect(() => decodeMessage(dataView, uint8View, -1)).toThrow(
      /SAB decode error/,
    );
  });
});
