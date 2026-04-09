/**
 * SharedArrayBuffer + Atomics protocol for bridging synchronous Emscripten FS
 * operations to asynchronous StorageBackend calls.
 *
 * Layout of the SharedArrayBuffer control region (Int32Array view):
 *   [0] status   — 0 = idle, 1 = request pending, 2 = response ready, -1 = error
 *   [1] opcode   — which StorageBackend method to call
 *   [2] dataLen  — length of serialized request/response data in the data region
 *
 * The data region follows the control region and carries serialized JSON
 * for request parameters and response payloads. Page data (Uint8Array) is
 * passed in a separate data section after the JSON to avoid base64 overhead.
 */

/** Status values in the control word (slot 0). */
export const STATUS_IDLE = 0;
export const STATUS_REQUEST = 1;
export const STATUS_RESPONSE = 2;
export const STATUS_ERROR = -1;

/** Opcodes for StorageBackend methods. */
export const OpCode = {
  READ_PAGE: 1,
  WRITE_PAGE: 2,
  WRITE_PAGES: 3,
  DELETE_FILE: 4,
  DELETE_PAGES_FROM: 5,
  READ_META: 6,
  WRITE_META: 7,
  DELETE_META: 8,
  LIST_FILES: 9,
  READ_PAGES: 10,
  RENAME_FILE: 11,
  WRITE_METAS: 12,
  DELETE_METAS: 13,
  COUNT_PAGES: 14,
  READ_METAS: 15,
  DELETE_FILES: 16,
  MAX_PAGE_INDEX: 17,
  LIST_FILES_RANGE: 18,
  COUNT_PAGES_BATCH: 19,
  MAX_PAGE_INDEX_BATCH: 20,
  SYNC_ALL: 21,
} as const;

export type OpCode = (typeof OpCode)[keyof typeof OpCode];

/** Byte offsets in the SharedArrayBuffer. */
export const CONTROL_BYTES = 12; // 3 x Int32
export const JSON_REGION_OFFSET = CONTROL_BYTES;

/**
 * Default buffer size: 12 bytes control + 1MB data region.
 * The data region must be large enough for the largest page batch operation.
 * A single page is 8KB; a batch of 128 pages is ~1MB.
 */
export const DEFAULT_BUFFER_SIZE = CONTROL_BYTES + 1024 * 1024;

/** Indices into the Int32Array control view. */
export const SLOT_STATUS = 0;
export const SLOT_OPCODE = 1;
export const SLOT_DATA_LEN = 2;

/** Reusable encoder/decoder to avoid allocation on every SAB call. */
const encoder = new TextEncoder();
const decoder = new TextDecoder();

/**
 * Encode a request into the shared buffer.
 * Returns the total bytes written to the data region.
 *
 * Format: [jsonLen: 4 bytes LE][json bytes][binary data bytes]
 *
 * Throws if the encoded message exceeds the buffer capacity.
 */
export function encodeMessage(
  dataView: DataView,
  uint8View: Uint8Array,
  json: unknown,
  binaryChunks?: Uint8Array[],
): number {
  const jsonStr = JSON.stringify(json);

  // Write JSON directly into the SAB data region using encodeInto(),
  // skipping the intermediate Uint8Array allocation that encode() makes.
  // Leave 4 bytes at the start for the JSON length prefix.
  const jsonWriteStart = JSON_REGION_OFFSET + 4;
  const jsonTarget = uint8View.subarray(jsonWriteStart);
  const { read, written: jsonLen } = encoder.encodeInto(jsonStr, jsonTarget);

  // If encodeInto couldn't write all characters, the buffer is too small
  if (read !== jsonStr.length) {
    const bufferSize = uint8View.byteLength;
    throw new Error(
      `SAB buffer overflow: JSON requires more than ${bufferSize - jsonWriteStart} bytes but buffer is ${bufferSize} bytes. ` +
        `Increase buffer size or reduce batch size.`,
    );
  }

  // Check total size including binary chunks
  let totalNeeded = JSON_REGION_OFFSET + 4 + jsonLen;
  if (binaryChunks) {
    for (const chunk of binaryChunks) {
      totalNeeded += chunk.length;
    }
  }

  const bufferSize = uint8View.byteLength;
  if (totalNeeded > bufferSize) {
    throw new Error(
      `SAB buffer overflow: message requires ${totalNeeded} bytes but buffer is ${bufferSize} bytes. ` +
        `Increase buffer size or reduce batch size.`,
    );
  }

  // Write JSON length prefix (4 bytes LE)
  dataView.setUint32(JSON_REGION_OFFSET, jsonLen, true);

  let offset = JSON_REGION_OFFSET + 4 + jsonLen;

  // Write binary chunks sequentially
  if (binaryChunks) {
    for (const chunk of binaryChunks) {
      uint8View.set(chunk, offset);
      offset += chunk.length;
    }
  }

  return offset - JSON_REGION_OFFSET;
}

/**
 * Decode a message from the shared buffer.
 * Returns the parsed JSON and any remaining binary data.
 *
 * Validates that lengths are within buffer bounds to prevent
 * reading garbage data from a corrupt or stale response.
 */
export function decodeMessage(
  dataView: DataView,
  uint8View: Uint8Array,
  totalLen: number,
): { json: unknown; binary: Uint8Array } {
  const bufferSize = uint8View.byteLength;
  const maxDataLen = bufferSize - JSON_REGION_OFFSET;

  if (totalLen < 4 || totalLen > maxDataLen) {
    throw new Error(
      `SAB decode error: totalLen ${totalLen} out of range [4, ${maxDataLen}]`,
    );
  }

  const jsonLen = dataView.getUint32(JSON_REGION_OFFSET, true);
  if (jsonLen > totalLen - 4) {
    throw new Error(
      `SAB decode error: jsonLen ${jsonLen} exceeds totalLen ${totalLen} (max ${totalLen - 4})`,
    );
  }

  // Use subarray() instead of slice() — avoids copying the JSON bytes.
  // The view is consumed immediately by TextDecoder and not retained.
  const jsonBytes = uint8View.subarray(
    JSON_REGION_OFFSET + 4,
    JSON_REGION_OFFSET + 4 + jsonLen,
  );
  let json: unknown;
  try {
    json = JSON.parse(decoder.decode(jsonBytes));
  } catch (err) {
    throw new Error(
      `SAB decode error: invalid JSON in response (${(err as Error).message})`,
    );
  }

  const binaryStart = JSON_REGION_OFFSET + 4 + jsonLen;
  const binaryEnd = JSON_REGION_OFFSET + totalLen;
  // Return a subarray (view) instead of a slice (copy) — callers that need
  // the data to outlive the current SAB call must copy it themselves. Both
  // SabClient (readPage, readPagesChunk) and SabWorker (WRITE_PAGE,
  // WRITE_PAGES, SYNC_ALL) already copy before the buffer is reused,
  // so this eliminates an unnecessary intermediate allocation that could
  // be up to 1 MB per readPages batch.
  const binary = uint8View.subarray(binaryStart, binaryEnd);

  return { json, binary };
}
