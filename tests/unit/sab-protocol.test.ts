/**
 * Unit tests for the SAB protocol encoding/decoding layer.
 *
 * Tests encodeMessage/decodeMessage in isolation — the serialization boundary
 * between SabClient and SabWorker. These are pure functions operating on
 * SharedArrayBuffer views, so no workers or backends are needed.
 */
import { describe, it, expect, beforeEach } from "vitest";
import {
  encodeMessage,
  decodeMessage,
  CONTROL_BYTES,
  JSON_REGION_OFFSET,
  DEFAULT_BUFFER_SIZE,
} from "../../src/sab-protocol.js";

/** Create buffer views matching the SAB protocol layout. */
function createViews(size = DEFAULT_BUFFER_SIZE) {
  const buffer = new ArrayBuffer(size);
  const dataView = new DataView(buffer);
  const uint8View = new Uint8Array(buffer);
  return { buffer, dataView, uint8View };
}

describe("sab-protocol", () => {
  describe("encodeMessage + decodeMessage round-trip", () => {
    it("round-trips a simple JSON object with no binary", () => {
      const { dataView, uint8View } = createViews();
      const json = { path: "/test/file.txt", pageIndex: 42 };

      const totalLen = encodeMessage(dataView, uint8View, json);
      const decoded = decodeMessage(dataView, uint8View, totalLen);

      expect(decoded.json).toEqual(json);
      expect(decoded.binary.length).toBe(0);
    });

    it("round-trips JSON with a single binary chunk", () => {
      const { dataView, uint8View } = createViews();
      const json = { op: "WRITE_PAGE", dataLen: 8192 };
      const chunk = new Uint8Array(8192);
      chunk.fill(0xab);

      const totalLen = encodeMessage(dataView, uint8View, json, [chunk]);
      const decoded = decodeMessage(dataView, uint8View, totalLen);

      expect(decoded.json).toEqual(json);
      expect(decoded.binary.length).toBe(8192);
      expect(decoded.binary[0]).toBe(0xab);
      expect(decoded.binary[8191]).toBe(0xab);
    });

    it("round-trips JSON with multiple binary chunks", () => {
      const { dataView, uint8View } = createViews();
      const json = { op: "WRITE_PAGES", count: 3 };
      const chunks = [
        new Uint8Array([1, 2, 3]),
        new Uint8Array([4, 5]),
        new Uint8Array([6]),
      ];

      const totalLen = encodeMessage(dataView, uint8View, json, chunks);
      const decoded = decodeMessage(dataView, uint8View, totalLen);

      expect(decoded.json).toEqual(json);
      expect(decoded.binary).toEqual(new Uint8Array([1, 2, 3, 4, 5, 6]));
    });

    it("round-trips an empty JSON object", () => {
      const { dataView, uint8View } = createViews();

      const totalLen = encodeMessage(dataView, uint8View, {});
      const decoded = decodeMessage(dataView, uint8View, totalLen);

      expect(decoded.json).toEqual({});
      expect(decoded.binary.length).toBe(0);
    });

    it("round-trips null JSON value", () => {
      const { dataView, uint8View } = createViews();

      const totalLen = encodeMessage(dataView, uint8View, null);
      const decoded = decodeMessage(dataView, uint8View, totalLen);

      expect(decoded.json).toBeNull();
    });

    it("round-trips JSON array", () => {
      const { dataView, uint8View } = createViews();
      const json = ["/file1", "/file2", "/file3"];

      const totalLen = encodeMessage(dataView, uint8View, json);
      const decoded = decodeMessage(dataView, uint8View, totalLen);

      expect(decoded.json).toEqual(json);
    });

    it("round-trips JSON with unicode strings", () => {
      const { dataView, uint8View } = createViews();
      const json = { path: "/tmp/日本語/文件.txt", emoji: "🗂️" };

      const totalLen = encodeMessage(dataView, uint8View, json);
      const decoded = decodeMessage(dataView, uint8View, totalLen);

      expect(decoded.json).toEqual(json);
    });

    it("round-trips zero-length binary chunks", () => {
      const { dataView, uint8View } = createViews();
      const json = { op: "test" };
      const chunks = [new Uint8Array(0), new Uint8Array(0)];

      const totalLen = encodeMessage(dataView, uint8View, json, chunks);
      const decoded = decodeMessage(dataView, uint8View, totalLen);

      expect(decoded.json).toEqual(json);
      expect(decoded.binary.length).toBe(0);
    });

    it("round-trips an empty binary chunks array", () => {
      const { dataView, uint8View } = createViews();
      const json = { op: "test" };

      const totalLen = encodeMessage(dataView, uint8View, json, []);
      const decoded = decodeMessage(dataView, uint8View, totalLen);

      expect(decoded.json).toEqual(json);
      expect(decoded.binary.length).toBe(0);
    });
  });

  describe("encodeMessage", () => {
    it("returns the total data region length", () => {
      const { dataView, uint8View } = createViews();
      const json = { a: 1 };

      const totalLen = encodeMessage(dataView, uint8View, json);

      // totalLen = 4 (jsonLen prefix) + JSON bytes
      const jsonBytes = new TextEncoder().encode(JSON.stringify(json));
      expect(totalLen).toBe(4 + jsonBytes.length);
    });

    it("includes binary chunk sizes in total length", () => {
      const { dataView, uint8View } = createViews();
      const json = { a: 1 };
      const chunk = new Uint8Array(100);

      const totalLen = encodeMessage(dataView, uint8View, json, [chunk]);

      const jsonBytes = new TextEncoder().encode(JSON.stringify(json));
      expect(totalLen).toBe(4 + jsonBytes.length + 100);
    });

    it("writes JSON length as little-endian uint32 at JSON_REGION_OFFSET", () => {
      const { dataView, uint8View } = createViews();
      const json = { test: "value" };

      encodeMessage(dataView, uint8View, json);

      const jsonBytes = new TextEncoder().encode(JSON.stringify(json));
      const writtenLen = dataView.getUint32(JSON_REGION_OFFSET, true);
      expect(writtenLen).toBe(jsonBytes.length);
    });

    it("throws on buffer overflow", () => {
      // Create a tiny buffer: control region + minimal data space
      const { dataView, uint8View } = createViews(CONTROL_BYTES + 20);
      const json = { longKey: "x".repeat(100) };

      expect(() => encodeMessage(dataView, uint8View, json)).toThrow(
        /SAB buffer overflow/,
      );
    });

    it("throws on buffer overflow from binary chunks", () => {
      const { dataView, uint8View } = createViews(CONTROL_BYTES + 50);
      const json = {};
      const chunk = new Uint8Array(100);

      expect(() => encodeMessage(dataView, uint8View, json, [chunk])).toThrow(
        /SAB buffer overflow/,
      );
    });

    it("succeeds when message exactly fills the buffer", () => {
      // Compute exact buffer size needed for a known payload
      const json = { x: 1 };
      const jsonBytes = new TextEncoder().encode(JSON.stringify(json));
      const chunkSize = 10;
      const exactSize = CONTROL_BYTES + 4 + jsonBytes.length + chunkSize;

      const { dataView, uint8View } = createViews(exactSize);
      const chunk = new Uint8Array(chunkSize);

      // Should not throw
      const totalLen = encodeMessage(dataView, uint8View, json, [chunk]);
      expect(totalLen).toBe(4 + jsonBytes.length + chunkSize);
    });
  });

  describe("decodeMessage", () => {
    it("throws when totalLen is less than 4", () => {
      const { dataView, uint8View } = createViews();

      expect(() => decodeMessage(dataView, uint8View, 3)).toThrow(
        /totalLen 3 out of range/,
      );
      expect(() => decodeMessage(dataView, uint8View, 0)).toThrow(
        /totalLen 0 out of range/,
      );
    });

    it("throws when totalLen is negative", () => {
      const { dataView, uint8View } = createViews();

      expect(() => decodeMessage(dataView, uint8View, -1)).toThrow(
        /totalLen -1 out of range/,
      );
    });

    it("throws when totalLen exceeds buffer capacity", () => {
      const size = CONTROL_BYTES + 100;
      const { dataView, uint8View } = createViews(size);

      expect(() => decodeMessage(dataView, uint8View, 101)).toThrow(
        /totalLen 101 out of range/,
      );
    });

    it("throws when jsonLen exceeds totalLen", () => {
      const { dataView, uint8View } = createViews();

      // Write a jsonLen that is too large
      dataView.setUint32(JSON_REGION_OFFSET, 9999, true);
      // Write minimal valid-looking data
      expect(() => decodeMessage(dataView, uint8View, 10)).toThrow(
        /jsonLen 9999 exceeds totalLen/,
      );
    });

    it("throws on invalid JSON payload", () => {
      const { dataView, uint8View } = createViews();

      // Write a jsonLen and then invalid JSON bytes
      const badJson = new TextEncoder().encode("{not valid json!!");
      dataView.setUint32(JSON_REGION_OFFSET, badJson.length, true);
      uint8View.set(badJson, JSON_REGION_OFFSET + 4);

      const totalLen = 4 + badJson.length;
      expect(() => decodeMessage(dataView, uint8View, totalLen)).toThrow(
        /invalid JSON in response/,
      );
    });

    it("correctly separates JSON and binary regions", () => {
      const { dataView, uint8View } = createViews();

      // Manually encode: JSON '{"a":1}' + 3 binary bytes
      const jsonStr = '{"a":1}';
      const jsonBytes = new TextEncoder().encode(jsonStr);
      const binaryData = new Uint8Array([0xff, 0xfe, 0xfd]);

      dataView.setUint32(JSON_REGION_OFFSET, jsonBytes.length, true);
      uint8View.set(jsonBytes, JSON_REGION_OFFSET + 4);
      uint8View.set(binaryData, JSON_REGION_OFFSET + 4 + jsonBytes.length);

      const totalLen = 4 + jsonBytes.length + binaryData.length;
      const decoded = decodeMessage(dataView, uint8View, totalLen);

      expect(decoded.json).toEqual({ a: 1 });
      expect(decoded.binary).toEqual(new Uint8Array([0xff, 0xfe, 0xfd]));
    });

    it("returns empty binary when jsonLen equals totalLen - 4", () => {
      const { dataView, uint8View } = createViews();

      const jsonStr = '{"x":"y"}';
      const jsonBytes = new TextEncoder().encode(jsonStr);
      dataView.setUint32(JSON_REGION_OFFSET, jsonBytes.length, true);
      uint8View.set(jsonBytes, JSON_REGION_OFFSET + 4);

      const totalLen = 4 + jsonBytes.length; // no binary region
      const decoded = decodeMessage(dataView, uint8View, totalLen);

      expect(decoded.json).toEqual({ x: "y" });
      expect(decoded.binary.length).toBe(0);
    });

    it("accepts totalLen exactly at buffer capacity", () => {
      const size = CONTROL_BYTES + 50;
      const { dataView, uint8View } = createViews(size);

      const json = {};
      const jsonBytes = new TextEncoder().encode(JSON.stringify(json));
      dataView.setUint32(JSON_REGION_OFFSET, jsonBytes.length, true);
      uint8View.set(jsonBytes, JSON_REGION_OFFSET + 4);

      // totalLen = maxDataLen = 50
      const decoded = decodeMessage(dataView, uint8View, 50);
      expect(decoded.json).toEqual({});
      // binary region is 50 - 4 - jsonBytes.length bytes
      expect(decoded.binary.length).toBe(50 - 4 - jsonBytes.length);
    });
  });

  describe("protocol constants", () => {
    it("CONTROL_BYTES is 12 (3 x Int32)", () => {
      expect(CONTROL_BYTES).toBe(12);
    });

    it("JSON_REGION_OFFSET equals CONTROL_BYTES", () => {
      expect(JSON_REGION_OFFSET).toBe(CONTROL_BYTES);
    });

    it("DEFAULT_BUFFER_SIZE is control + 1MB", () => {
      expect(DEFAULT_BUFFER_SIZE).toBe(CONTROL_BYTES + 1024 * 1024);
    });
  });

  describe("data isolation", () => {
    it("decodeMessage returns copies, not views into the buffer", () => {
      const { dataView, uint8View } = createViews();
      const json = { path: "/test" };
      const chunk = new Uint8Array([1, 2, 3]);

      const totalLen = encodeMessage(dataView, uint8View, json, [chunk]);
      const decoded = decodeMessage(dataView, uint8View, totalLen);

      // Mutate the original buffer
      uint8View.fill(0);

      // Decoded binary should be unaffected (it's a slice copy)
      expect(decoded.binary).toEqual(new Uint8Array([1, 2, 3]));
    });

    it("successive encodes overwrite previous data", () => {
      const { dataView, uint8View } = createViews();

      // First encode
      encodeMessage(dataView, uint8View, { first: true }, [
        new Uint8Array([0xaa, 0xbb]),
      ]);

      // Second encode with different data
      const totalLen = encodeMessage(dataView, uint8View, { second: true }, [
        new Uint8Array([0xcc]),
      ]);
      const decoded = decodeMessage(dataView, uint8View, totalLen);

      expect(decoded.json).toEqual({ second: true });
      expect(decoded.binary).toEqual(new Uint8Array([0xcc]));
    });
  });

  describe("large payloads", () => {
    it("handles a full 8KB page as binary", () => {
      const { dataView, uint8View } = createViews();
      const json = { op: "WRITE_PAGE", path: "/db/base/16384", pageIndex: 0 };
      const page = new Uint8Array(8192);
      // Fill with a recognizable pattern
      for (let i = 0; i < page.length; i++) {
        page[i] = i & 0xff;
      }

      const totalLen = encodeMessage(dataView, uint8View, json, [page]);
      const decoded = decodeMessage(dataView, uint8View, totalLen);

      expect(decoded.json).toEqual(json);
      expect(decoded.binary.length).toBe(8192);
      for (let i = 0; i < 8192; i++) {
        if (decoded.binary[i] !== (i & 0xff)) {
          throw new Error(`Mismatch at byte ${i}`);
        }
      }
    });

    it("handles batch of multiple 8KB pages", () => {
      const { dataView, uint8View } = createViews();
      const pageCount = 10;
      const json = { op: "WRITE_PAGES", count: pageCount };
      const chunks: Uint8Array[] = [];
      for (let p = 0; p < pageCount; p++) {
        const page = new Uint8Array(8192);
        page.fill(p & 0xff);
        chunks.push(page);
      }

      const totalLen = encodeMessage(dataView, uint8View, json, chunks);
      const decoded = decodeMessage(dataView, uint8View, totalLen);

      expect(decoded.json).toEqual(json);
      expect(decoded.binary.length).toBe(8192 * pageCount);

      // Verify each page's fill pattern
      for (let p = 0; p < pageCount; p++) {
        const offset = p * 8192;
        expect(decoded.binary[offset]).toBe(p & 0xff);
        expect(decoded.binary[offset + 8191]).toBe(p & 0xff);
      }
    });
  });
});
