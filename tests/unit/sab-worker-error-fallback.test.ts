/**
 * Tests for SabWorker error encoding fallback.
 *
 * Verifies that handleRequest always signals STATUS_ERROR to the client,
 * even when encoding the error message itself fails (e.g., buffer too
 * small for the error string). Without the fallback, the client would
 * block forever on Atomics.wait().
 */
import { describe, it, expect, afterEach } from "vitest";
import { SabWorker } from "../../src/sab-worker.js";
import { MemoryBackend } from "../../src/memory-backend.js";
import type { StorageBackend } from "../../src/storage-backend.js";
import {
  CONTROL_BYTES,
  STATUS_REQUEST,
  STATUS_ERROR,
  SLOT_STATUS,
  SLOT_OPCODE,
  SLOT_DATA_LEN,
  OpCode,
  encodeMessage,
  decodeMessage,
} from "../../src/sab-protocol.js";

/**
 * A backend that delegates to MemoryBackend but throws on readPage
 * with a configurable error message.
 */
class ThrowingBackend extends MemoryBackend {
  throwMessage: string | null = null;

  override async readPage(
    _path: string,
    _pageIndex: number,
  ): Promise<Uint8Array | null> {
    if (this.throwMessage) throw new Error(this.throwMessage);
    return super.readPage(_path, _pageIndex);
  }
}

function waitForStatus(
  controlView: Int32Array,
  notStatus: number,
  timeoutMs = 5000,
): Promise<number> {
  return new Promise((resolve, reject) => {
    const deadline = Date.now() + timeoutMs;
    const check = () => {
      const status = Atomics.load(controlView, SLOT_STATUS);
      if (status !== notStatus) return resolve(status);
      if (Date.now() > deadline) return reject(new Error("timed out"));
      setTimeout(check, 1);
    };
    check();
  });
}

describe("SabWorker error encoding fallback @fast", () => {
  let worker: SabWorker | null = null;

  afterEach(() => {
    worker?.stop();
    worker = null;
  });

  it("signals STATUS_ERROR when error message exceeds buffer capacity", async () => {
    // Buffer: big enough for a short READ_PAGE request, too small for a
    // 10 000-char error message. 120 bytes = 12 control + 108 data region.
    const bufSize = 120;
    const sab = new SharedArrayBuffer(bufSize);
    const controlView = new Int32Array(sab, 0, 3);
    const dataView = new DataView(sab);
    const uint8View = new Uint8Array(sab);

    const backend = new ThrowingBackend();
    backend.throwMessage = "x".repeat(10_000);

    worker = new SabWorker(sab, backend as unknown as StorageBackend);

    const reqLen = encodeMessage(dataView, uint8View, {
      path: "/a",
      pageIndex: 0,
    });
    Atomics.store(controlView, SLOT_DATA_LEN, reqLen);
    Atomics.store(controlView, SLOT_OPCODE, OpCode.READ_PAGE as number);
    Atomics.store(controlView, SLOT_STATUS, STATUS_REQUEST);

    const startPromise = worker.start();
    const finalStatus = await waitForStatus(controlView, STATUS_REQUEST);

    expect(finalStatus).toBe(STATUS_ERROR);

    // The fallback should have encoded { error: "internal error" }
    const respLen = Atomics.load(controlView, SLOT_DATA_LEN);
    expect(respLen).toBeGreaterThan(0);
    const { json } = decodeMessage(dataView, uint8View, respLen);
    expect((json as { error: string }).error).toBe("internal error");

    worker.stop();
    await startPromise;
  });

  it("signals STATUS_ERROR with normal-sized error on normal buffer", async () => {
    // Standard-sized buffer — the primary error encoding should succeed.
    const bufSize = CONTROL_BYTES + 1024;
    const sab = new SharedArrayBuffer(bufSize);
    const controlView = new Int32Array(sab, 0, 3);
    const dataView = new DataView(sab);
    const uint8View = new Uint8Array(sab);

    const backend = new ThrowingBackend();
    backend.throwMessage = "disk full";

    worker = new SabWorker(sab, backend as unknown as StorageBackend);

    const reqLen = encodeMessage(dataView, uint8View, {
      path: "/a",
      pageIndex: 0,
    });
    Atomics.store(controlView, SLOT_DATA_LEN, reqLen);
    Atomics.store(controlView, SLOT_OPCODE, OpCode.READ_PAGE as number);
    Atomics.store(controlView, SLOT_STATUS, STATUS_REQUEST);

    const startPromise = worker.start();
    const finalStatus = await waitForStatus(controlView, STATUS_REQUEST);

    expect(finalStatus).toBe(STATUS_ERROR);

    const respLen = Atomics.load(controlView, SLOT_DATA_LEN);
    const { json } = decodeMessage(dataView, uint8View, respLen);
    expect((json as { error: string }).error).toBe("disk full");

    worker.stop();
    await startPromise;
  });

  it("falls back to zero-length data when buffer is extremely small", async () => {
    // Buffer so small that even { error: "internal error" } won't fit.
    // Control: 12 bytes, data region: 20 bytes — too small for any JSON.
    const bufSize = 32;
    const sab = new SharedArrayBuffer(bufSize);
    const controlView = new Int32Array(sab, 0, 3);
    const dataView = new DataView(sab);
    const uint8View = new Uint8Array(sab);

    const backend = new ThrowingBackend();
    backend.throwMessage = "fail";

    worker = new SabWorker(sab, backend as unknown as StorageBackend);

    // The request won't fit either, so dispatch will fail at decodeMessage.
    // We still set STATUS_REQUEST to trigger handleRequest.
    Atomics.store(controlView, SLOT_DATA_LEN, 0);
    Atomics.store(controlView, SLOT_OPCODE, OpCode.READ_PAGE as number);
    Atomics.store(controlView, SLOT_STATUS, STATUS_REQUEST);

    const startPromise = worker.start();
    const finalStatus = await waitForStatus(controlView, STATUS_REQUEST);

    // Even with a decode failure and tiny buffer, STATUS_ERROR must be set
    expect(finalStatus).toBe(STATUS_ERROR);

    worker.stop();
    await startPromise;
  });
});
