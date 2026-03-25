/**
 * SAB+Atomics worker — the asynchronous side of the bridge.
 *
 * Runs in the storage worker thread (or main thread). Uses Atomics.waitAsync()
 * to avoid blocking the event loop, allowing async StorageBackend operations
 * to proceed between polls.
 *
 * In production, this typically runs in a dedicated Web Worker that handles
 * async IDB operations. In tests, it can run on the main thread alongside
 * vitest since waitAsync is non-blocking.
 */

import type { StorageBackend } from "./storage-backend.js";
import type { FileMeta } from "./types.js";
import {
  STATUS_IDLE,
  STATUS_REQUEST,
  STATUS_RESPONSE,
  STATUS_ERROR,
  OpCode,
  SLOT_STATUS,
  SLOT_OPCODE,
  SLOT_DATA_LEN,
  encodeMessage,
  decodeMessage,
} from "./sab-protocol.js";

export class SabWorker {
  private readonly sab: SharedArrayBuffer;
  private readonly backend: StorageBackend;
  private readonly controlView: Int32Array;
  private readonly dataView: DataView;
  private readonly uint8View: Uint8Array;
  private running = false;

  constructor(sab: SharedArrayBuffer, backend: StorageBackend) {
    this.sab = sab;
    this.backend = backend;
    this.controlView = new Int32Array(sab, 0, 3);
    this.dataView = new DataView(sab);
    this.uint8View = new Uint8Array(sab);
  }

  /**
   * Start the request processing loop using non-blocking waitAsync.
   * Returns a promise that resolves when stop() is called.
   */
  async start(): Promise<void> {
    this.running = true;

    while (this.running) {
      // Non-blocking wait: yields to the event loop while waiting
      const status = Atomics.load(this.controlView, SLOT_STATUS);

      if (status === STATUS_REQUEST) {
        await this.handleRequest();
        continue;
      }

      if (!this.running) break;

      // Use Atomics.waitAsync for non-blocking wait on the event loop
      const waitResult = Atomics.waitAsync(
        this.controlView,
        SLOT_STATUS,
        status,
      );

      if (waitResult.async) {
        // Wait for either a status change or timeout
        await Promise.race([
          waitResult.value,
          new Promise<void>((resolve) => {
            const check = () => {
              if (!this.running) {
                resolve();
                return;
              }
              const s = Atomics.load(this.controlView, SLOT_STATUS);
              if (s !== status) {
                resolve();
                return;
              }
              setTimeout(check, 10);
            };
            setTimeout(check, 10);
          }),
        ]);
      }
      // If not async, the value already changed — loop back to check it
    }
  }

  /** Stop the processing loop. */
  stop(): void {
    this.running = false;
    // Change status to wake up any waitAsync
    Atomics.notify(this.controlView, SLOT_STATUS);
  }

  private async handleRequest(): Promise<void> {
    const opcode = Atomics.load(
      this.controlView,
      SLOT_OPCODE,
    ) as unknown as OpCode;
    const dataLen = Atomics.load(this.controlView, SLOT_DATA_LEN);

    try {
      const { json: params, binary } = decodeMessage(
        this.dataView,
        this.uint8View,
        dataLen,
      );

      await this.dispatch(opcode, params as Record<string, unknown>, binary);

      // Signal response ready
      Atomics.store(this.controlView, SLOT_STATUS, STATUS_RESPONSE);
      Atomics.notify(this.controlView, SLOT_STATUS);
    } catch (err: unknown) {
      const errMsg =
        err instanceof Error ? err.message : "Unknown bridge error";
      const errLen = encodeMessage(this.dataView, this.uint8View, {
        error: errMsg,
      });
      Atomics.store(this.controlView, SLOT_DATA_LEN, errLen);
      Atomics.store(this.controlView, SLOT_STATUS, STATUS_ERROR);
      Atomics.notify(this.controlView, SLOT_STATUS);
    }
  }

  /**
   * Dispatch a request to the appropriate StorageBackend method.
   * Writes the response directly into the shared buffer.
   */
  private async dispatch(
    opcode: OpCode,
    params: Record<string, unknown>,
    binary: Uint8Array,
  ): Promise<void> {
    switch (opcode) {
      case OpCode.READ_PAGE: {
        const data = await this.backend.readPage(
          params.path as string,
          params.pageIndex as number,
        );
        if (data) {
          const respLen = encodeMessage(
            this.dataView,
            this.uint8View,
            { found: true },
            [data],
          );
          Atomics.store(this.controlView, SLOT_DATA_LEN, respLen);
        } else {
          const respLen = encodeMessage(this.dataView, this.uint8View, {
            found: false,
          });
          Atomics.store(this.controlView, SLOT_DATA_LEN, respLen);
        }
        break;
      }

      case OpCode.READ_PAGES: {
        const path = params.path as string;
        const pageIndices = params.pageIndices as number[];
        const pages = await this.backend.readPages(path, pageIndices);
        const sizes: number[] = [];
        const chunks: Uint8Array[] = [];
        for (const page of pages) {
          if (page) {
            sizes.push(page.length);
            chunks.push(page);
          } else {
            sizes.push(-1);
          }
        }
        const respLen = encodeMessage(
          this.dataView,
          this.uint8View,
          { sizes },
          chunks,
        );
        Atomics.store(this.controlView, SLOT_DATA_LEN, respLen);
        break;
      }

      case OpCode.WRITE_PAGE: {
        const dataLen = params.dataLen as number;
        const pageData = new Uint8Array(binary.buffer, binary.byteOffset, dataLen);
        await this.backend.writePage(
          params.path as string,
          params.pageIndex as number,
          pageData,
        );
        const respLen = encodeMessage(this.dataView, this.uint8View, {
          ok: true,
        });
        Atomics.store(this.controlView, SLOT_DATA_LEN, respLen);
        break;
      }

      case OpCode.WRITE_PAGES: {
        const pageMeta = params.pages as Array<{
          path: string;
          pageIndex: number;
          dataLen: number;
        }>;
        let offset = 0;
        const pages = pageMeta.map((pm) => {
          const data = binary.slice(offset, offset + pm.dataLen);
          offset += pm.dataLen;
          return { path: pm.path, pageIndex: pm.pageIndex, data };
        });
        await this.backend.writePages(pages);
        const respLen = encodeMessage(this.dataView, this.uint8View, {
          ok: true,
        });
        Atomics.store(this.controlView, SLOT_DATA_LEN, respLen);
        break;
      }

      case OpCode.DELETE_FILE: {
        await this.backend.deleteFile(params.path as string);
        const respLen = encodeMessage(this.dataView, this.uint8View, {
          ok: true,
        });
        Atomics.store(this.controlView, SLOT_DATA_LEN, respLen);
        break;
      }

      case OpCode.DELETE_PAGES_FROM: {
        await this.backend.deletePagesFrom(
          params.path as string,
          params.fromPageIndex as number,
        );
        const respLen = encodeMessage(this.dataView, this.uint8View, {
          ok: true,
        });
        Atomics.store(this.controlView, SLOT_DATA_LEN, respLen);
        break;
      }

      case OpCode.READ_META: {
        const meta = await this.backend.readMeta(params.path as string);
        const respLen = encodeMessage(this.dataView, this.uint8View, { meta });
        Atomics.store(this.controlView, SLOT_DATA_LEN, respLen);
        break;
      }

      case OpCode.WRITE_META: {
        await this.backend.writeMeta(
          params.path as string,
          params.meta as FileMeta,
        );
        const respLen = encodeMessage(this.dataView, this.uint8View, {
          ok: true,
        });
        Atomics.store(this.controlView, SLOT_DATA_LEN, respLen);
        break;
      }

      case OpCode.DELETE_META: {
        await this.backend.deleteMeta(params.path as string);
        const respLen = encodeMessage(this.dataView, this.uint8View, {
          ok: true,
        });
        Atomics.store(this.controlView, SLOT_DATA_LEN, respLen);
        break;
      }

      case OpCode.LIST_FILES: {
        const files = await this.backend.listFiles();
        const respLen = encodeMessage(this.dataView, this.uint8View, { files });
        Atomics.store(this.controlView, SLOT_DATA_LEN, respLen);
        break;
      }

      default:
        throw new Error(`Unknown opcode: ${opcode}`);
    }
  }
}
