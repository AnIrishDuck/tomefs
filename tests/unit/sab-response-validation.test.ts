/**
 * Tests for SAB bridge response validation helpers.
 *
 * The SabClient validates batch response structure to catch protocol bugs
 * or buffer corruption early with clear error messages, instead of letting
 * malformed responses propagate as subtle data corruption.
 */
import { describe, it, expect } from "vitest";
import {
  validateBatchArray,
  validateListFilesResponse,
} from "../../src/sab-client.js";

describe("validateBatchArray", () => {
  it("@fast returns the array when field exists and length matches", () => {
    const result = validateBatchArray<number>(
      { counts: [1, 2, 3] },
      "counts",
      3,
      "testMethod",
    );
    expect(result).toEqual([1, 2, 3]);
  });

  it("@fast returns empty array when expected length is 0", () => {
    const result = validateBatchArray<number>(
      { counts: [] },
      "counts",
      0,
      "testMethod",
    );
    expect(result).toEqual([]);
  });

  it("returns array with null elements", () => {
    const result = validateBatchArray<string | null>(
      { metas: [null, "a", null] },
      "metas",
      3,
      "readMetas",
    );
    expect(result).toEqual([null, "a", null]);
  });

  it("@fast throws when field is missing", () => {
    expect(() =>
      validateBatchArray<number>({}, "counts", 3, "countPagesBatch"),
    ).toThrow(/SAB bridge countPagesBatch: expected 3 counts entries, got undefined/);
  });

  it("@fast throws when field is not an array", () => {
    expect(() =>
      validateBatchArray<number>(
        { counts: "not-an-array" },
        "counts",
        3,
        "countPagesBatch",
      ),
    ).toThrow(/SAB bridge countPagesBatch: expected 3 counts entries, got string/);
  });

  it("throws when field is a number", () => {
    expect(() =>
      validateBatchArray<number>(
        { counts: 42 },
        "counts",
        1,
        "countPagesBatch",
      ),
    ).toThrow(/SAB bridge countPagesBatch: expected 1 counts entries, got number/);
  });

  it("throws when field is null", () => {
    expect(() =>
      validateBatchArray<number>(
        { counts: null },
        "counts",
        1,
        "countPagesBatch",
      ),
    ).toThrow(/SAB bridge countPagesBatch: expected 1 counts entries, got object/);
  });

  it("@fast throws when array length doesn't match expected", () => {
    expect(() =>
      validateBatchArray<number>(
        { counts: [1, 2] },
        "counts",
        3,
        "countPagesBatch",
      ),
    ).toThrow(/SAB bridge countPagesBatch: expected 3 counts entries, got 2/);
  });

  it("throws when array is too long", () => {
    expect(() =>
      validateBatchArray<number>(
        { maxIndices: [1, 2, 3, 4] },
        "maxIndices",
        2,
        "maxPageIndexBatch",
      ),
    ).toThrow(/SAB bridge maxPageIndexBatch: expected 2 maxIndices entries, got 4/);
  });

  it("throws when json is null", () => {
    expect(() =>
      validateBatchArray<number>(null, "counts", 1, "countPagesBatch"),
    ).toThrow();
  });

  it("includes method name in error message", () => {
    expect(() =>
      validateBatchArray<number>(
        { sizes: [1] },
        "sizes",
        2,
        "readPages",
      ),
    ).toThrow(/SAB bridge readPages/);
  });

  it("includes field name in error message", () => {
    expect(() =>
      validateBatchArray<number>(
        { wrong: [1, 2] },
        "maxIndices",
        2,
        "maxPageIndexBatch",
      ),
    ).toThrow(/maxIndices entries/);
  });
});

describe("validateListFilesResponse", () => {
  it("@fast returns the response when structure is valid", () => {
    const result = validateListFilesResponse(
      { files: ["/a", "/b"], total: 5 },
      "listFiles",
    );
    expect(result.files).toEqual(["/a", "/b"]);
    expect(result.total).toBe(5);
  });

  it("@fast returns empty files array", () => {
    const result = validateListFilesResponse(
      { files: [], total: 0 },
      "listFiles",
    );
    expect(result.files).toEqual([]);
    expect(result.total).toBe(0);
  });

  it("@fast throws when files field is missing", () => {
    expect(() =>
      validateListFilesResponse({ total: 5 }, "listFiles"),
    ).toThrow(/SAB bridge listFiles: expected files array, got undefined/);
  });

  it("@fast throws when files field is not an array", () => {
    expect(() =>
      validateListFilesResponse(
        { files: "not-array", total: 1 },
        "listFiles",
      ),
    ).toThrow(/SAB bridge listFiles: expected files array, got string/);
  });

  it("throws when total field is missing", () => {
    expect(() =>
      validateListFilesResponse({ files: [] }, "listFiles"),
    ).toThrow(/SAB bridge listFiles: expected total number, got undefined/);
  });

  it("throws when total field is not a number", () => {
    expect(() =>
      validateListFilesResponse(
        { files: [], total: "five" },
        "listFiles",
      ),
    ).toThrow(/SAB bridge listFiles: expected total number, got string/);
  });

  it("throws when total is null", () => {
    expect(() =>
      validateListFilesResponse(
        { files: [], total: null },
        "listFiles",
      ),
    ).toThrow(/SAB bridge listFiles: expected total number, got object/);
  });

  it("throws when json is null", () => {
    expect(() =>
      validateListFilesResponse(null, "listFiles"),
    ).toThrow();
  });

  it("includes method name in error messages", () => {
    expect(() =>
      validateListFilesResponse({ total: 0 }, "myMethod"),
    ).toThrow(/SAB bridge myMethod/);
  });
});
