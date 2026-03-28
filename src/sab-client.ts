/**
 * SAB+Atomics client — the synchronous side of the bridge.
 *
 * Runs in the PGlite/Emscripten worker thread. Sends requests to the storage
 * worker via SharedArrayBuffer and blocks with Atomics.wait() until the
 * response is ready.
 *
 * This provides a synchronous StorageBackend-compatible interface that the
 * Emscripten FS layer can call from its synchronous C-style callbacks.
 */

import { PAGE_SIZE } from "./types.js";
import type { FileMeta } from "./types.js";
import type { SyncStorageBackend } from "./sync-storage-backend.js";
import {
  STATUS_IDLE,
  STATUS_REQUEST,
  STATUS_RESPONSE,
  STATUS_ERROR,
  OpCode,
  SLOT_STATUS,
  SLOT_OPCODE,
  SLOT_DATA_LEN,
  CONTROL_BYTES,
  DEFAULT_BUFFER_SIZE,
  encodeMessage,
  decodeMessage,
} from "./sab-protocol.js";

/**
 * SAB+Atomics client that implements SyncStorageBackend.
 *
 * This is the synchronous side of the bridge: it runs in the PGlite/Emscripten
 * worker thread, sends requests to the storage worker via SharedArrayBuffer,
 * and blocks with Atomics.wait() until the response is ready.
 *
 * Can be passed directly as the `backend` option to `createTomeFS`.
 */
export interface SabClientOptions {
  /** Timeout in milliseconds for each SAB bridge call. 0 = no timeout (default). */
  timeout?: number;
}

export class SabClient implements SyncStorageBackend {
  private readonly sab: SharedArrayBuffer;
  private readonly controlView: Int32Array;
  private readonly dataView: DataView;
  private readonly uint8View: Uint8Array;
  private readonly timeout: number;

  /**
   * Maximum pages per batch call. Computed from buffer size to prevent
   * overflow when flushAll/flushFile sends large dirty page batches.
   *
   * Each page in a writePages request needs PAGE_SIZE bytes of binary data
   * plus ~256 bytes of JSON metadata. Each page in a readPages response
   * needs PAGE_SIZE bytes plus JSON overhead for the sizes array.
   * We use a conservative estimate and leave 4 KB headroom for the
   * enclosing JSON wrapper.
   */
  private readonly maxBatchPages: number;

  constructor(sab: SharedArrayBuffer, options?: SabClientOptions) {
    this.sab = sab;
    this.controlView = new Int32Array(sab, 0, 3);
    this.dataView = new DataView(sab);
    this.uint8View = new Uint8Array(sab);
    this.timeout = options?.timeout ?? 0;

    const dataRegionSize = sab.byteLength - CONTROL_BYTES;
    this.maxBatchPages = Math.max(
      1,
      Math.floor((dataRegionSize - 4096) / (PAGE_SIZE + 256)),
    );
  }

  /** Create a SharedArrayBuffer for use with this client and a SabWorker. */
  static createBuffer(size: number = DEFAULT_BUFFER_SIZE): SharedArrayBuffer {
    return new SharedArrayBuffer(size);
  }

  readPage(path: string, pageIndex: number): Uint8Array | null {
    const { json, binary } = this.call(OpCode.READ_PAGE, { path, pageIndex });
    const result = json as { found: boolean };
    if (!result.found) return null;
    return new Uint8Array(binary);
  }

  readPages(path: string, pageIndices: number[]): Array<Uint8Array | null> {
    if (pageIndices.length === 0) return [];

    // If the batch fits in a single call, use the fast path.
    // Otherwise chunk to avoid overflowing the SAB response buffer
    // (the worker encodes all page data into the shared buffer).
    if (pageIndices.length <= this.maxBatchPages) {
      return this.readPagesChunk(path, pageIndices);
    }

    const results: Array<Uint8Array | null> = [];
    for (let i = 0; i < pageIndices.length; i += this.maxBatchPages) {
      const chunk = pageIndices.slice(i, i + this.maxBatchPages);
      results.push(...this.readPagesChunk(path, chunk));
    }
    return results;
  }

  private readPagesChunk(
    path: string,
    pageIndices: number[],
  ): Array<Uint8Array | null> {
    const { json, binary } = this.call(OpCode.READ_PAGES, {
      path,
      pageIndices,
    });
    const result = json as { sizes: number[] };
    if (!Array.isArray(result.sizes) || result.sizes.length !== pageIndices.length) {
      throw new Error(
        `SAB bridge readPages: expected ${pageIndices.length} size entries, got ${
          Array.isArray(result.sizes) ? result.sizes.length : "non-array"
        }`,
      );
    }
    const pages: Array<Uint8Array | null> = [];
    let offset = 0;
    for (const size of result.sizes) {
      if (size < 0) {
        pages.push(null);
      } else {
        if (offset + size > binary.length) {
          throw new Error(
            `SAB bridge readPages: binary data underflow at offset ${offset} + size ${size} > ${binary.length}`,
          );
        }
        pages.push(new Uint8Array(binary.slice(offset, offset + size)));
        offset += size;
      }
    }
    return pages;
  }

  writePage(path: string, pageIndex: number, data: Uint8Array): void {
    this.call(OpCode.WRITE_PAGE, { path, pageIndex, dataLen: data.length }, [
      data,
    ]);
  }

  writePages(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
  ): void {
    if (pages.length === 0) return;

    // If the batch fits in a single call, use the fast path.
    // Otherwise chunk to avoid overflowing the SAB request buffer.
    if (pages.length <= this.maxBatchPages) {
      this.writePagesChunk(pages);
      return;
    }

    for (let i = 0; i < pages.length; i += this.maxBatchPages) {
      this.writePagesChunk(pages.slice(i, i + this.maxBatchPages));
    }
  }

  private writePagesChunk(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
  ): void {
    const meta = pages.map((p) => ({
      path: p.path,
      pageIndex: p.pageIndex,
      dataLen: p.data.length,
    }));
    const chunks = pages.map((p) => p.data);
    this.call(OpCode.WRITE_PAGES, { pages: meta }, chunks);
  }

  deleteFile(path: string): void {
    this.call(OpCode.DELETE_FILE, { path });
  }

  deletePagesFrom(path: string, fromPageIndex: number): void {
    this.call(OpCode.DELETE_PAGES_FROM, { path, fromPageIndex });
  }

  renameFile(oldPath: string, newPath: string): void {
    this.call(OpCode.RENAME_FILE, { oldPath, newPath });
  }

  countPages(path: string): number {
    const { json } = this.call(OpCode.COUNT_PAGES, { path });
    return (json as { count: number }).count;
  }

  readMeta(path: string): FileMeta | null {
    const { json } = this.call(OpCode.READ_META, { path });
    const result = json as { meta: FileMeta | null };
    return result.meta;
  }

  writeMeta(path: string, meta: FileMeta): void {
    this.call(OpCode.WRITE_META, { path, meta });
  }

  writeMetas(entries: Array<{ path: string; meta: FileMeta }>): void {
    if (entries.length === 0) return;
    this.call(OpCode.WRITE_METAS, { entries });
  }

  deleteMeta(path: string): void {
    this.call(OpCode.DELETE_META, { path });
  }

  deleteMetas(paths: string[]): void {
    if (paths.length === 0) return;
    this.call(OpCode.DELETE_METAS, { paths });
  }

  listFiles(): string[] {
    const { json } = this.call(OpCode.LIST_FILES, {});
    return (json as { files: string[] }).files;
  }

  /**
   * Send a request and block until the response arrives.
   */
  private call(
    opcode: OpCode,
    params: unknown,
    binaryChunks?: Uint8Array[],
  ): { json: unknown; binary: Uint8Array } {
    // Encode request into the data region
    const dataLen = encodeMessage(
      this.dataView,
      this.uint8View,
      params,
      binaryChunks,
    );

    // Set opcode and data length
    Atomics.store(this.controlView, SLOT_OPCODE, opcode);
    Atomics.store(this.controlView, SLOT_DATA_LEN, dataLen);

    // Signal request ready (wake the worker)
    Atomics.store(this.controlView, SLOT_STATUS, STATUS_REQUEST);
    Atomics.notify(this.controlView, SLOT_STATUS);

    // Block until response arrives
    const waitTimeout = this.timeout > 0 ? this.timeout : undefined;
    let status: number;
    do {
      const result = Atomics.wait(
        this.controlView,
        SLOT_STATUS,
        STATUS_REQUEST,
        waitTimeout,
      );
      if (result === "timed-out") {
        // Reset to idle so the bridge can recover
        Atomics.store(this.controlView, SLOT_STATUS, STATUS_IDLE);
        throw new Error(
          `SAB bridge timeout: storage worker did not respond within ${this.timeout}ms`,
        );
      }
      status = Atomics.load(this.controlView, SLOT_STATUS);
    } while (status === STATUS_REQUEST);

    if (status !== STATUS_RESPONSE && status !== STATUS_ERROR) {
      // Reset to idle so the bridge can recover
      Atomics.store(this.controlView, SLOT_STATUS, STATUS_IDLE);
      throw new Error(
        `SAB bridge unexpected status: ${status} (expected ${STATUS_RESPONSE} or ${STATUS_ERROR})`,
      );
    }

    if (status === STATUS_ERROR) {
      const responseLen = Atomics.load(this.controlView, SLOT_DATA_LEN);
      const { json } = decodeMessage(this.dataView, this.uint8View, responseLen);
      const errMsg = (json as { error: string }).error;

      // Reset to idle for next call
      Atomics.store(this.controlView, SLOT_STATUS, STATUS_IDLE);

      throw new Error(`SAB bridge error: ${errMsg}`);
    }

    // Read response
    const responseLen = Atomics.load(this.controlView, SLOT_DATA_LEN);
    const result = decodeMessage(this.dataView, this.uint8View, responseLen);

    // Reset to idle for next call
    Atomics.store(this.controlView, SLOT_STATUS, STATUS_IDLE);

    return result;
  }
}
