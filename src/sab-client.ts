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

  constructor(sab: SharedArrayBuffer, options?: SabClientOptions) {
    this.sab = sab;
    this.controlView = new Int32Array(sab, 0, 3);
    this.dataView = new DataView(sab);
    this.uint8View = new Uint8Array(sab);
    this.timeout = options?.timeout ?? 0;
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
    const { json, binary } = this.call(OpCode.READ_PAGES, {
      path,
      pageIndices,
    });
    const result = json as { sizes: number[] };
    const pages: Array<Uint8Array | null> = [];
    let offset = 0;
    for (const size of result.sizes) {
      if (size < 0) {
        pages.push(null);
      } else {
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

  readMeta(path: string): FileMeta | null {
    const { json } = this.call(OpCode.READ_META, { path });
    const result = json as { meta: FileMeta | null };
    return result.meta;
  }

  writeMeta(path: string, meta: FileMeta): void {
    this.call(OpCode.WRITE_META, { path, meta });
  }

  deleteMeta(path: string): void {
    this.call(OpCode.DELETE_META, { path });
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
