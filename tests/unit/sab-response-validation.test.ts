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
  validateSingletonField,
  validateMetaResponse,
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

describe("validateSingletonField", () => {
  it("@fast returns the value when field exists and type matches (number)", () => {
    const result = validateSingletonField<number>(
      { count: 42 },
      "count",
      "number",
      "countPages",
    );
    expect(result).toBe(42);
  });

  it("@fast returns the value when field exists and type matches (boolean)", () => {
    const result = validateSingletonField<boolean>(
      { found: true },
      "found",
      "boolean",
      "readPage",
    );
    expect(result).toBe(true);
  });

  it("returns false for boolean fields", () => {
    const result = validateSingletonField<boolean>(
      { found: false },
      "found",
      "boolean",
      "readPage",
    );
    expect(result).toBe(false);
  });

  it("returns zero for number fields", () => {
    const result = validateSingletonField<number>(
      { count: 0 },
      "count",
      "number",
      "countPages",
    );
    expect(result).toBe(0);
  });

  it("returns negative numbers for number fields", () => {
    const result = validateSingletonField<number>(
      { maxIdx: -1 },
      "maxIdx",
      "number",
      "maxPageIndex",
    );
    expect(result).toBe(-1);
  });

  it("@fast throws when field is missing", () => {
    expect(() =>
      validateSingletonField<number>({}, "count", "number", "countPages"),
    ).toThrow(/SAB bridge countPages: response missing 'count' field/);
  });

  it("@fast throws when field has wrong type (string instead of number)", () => {
    expect(() =>
      validateSingletonField<number>(
        { count: "forty-two" },
        "count",
        "number",
        "countPages",
      ),
    ).toThrow(/SAB bridge countPages: expected count to be number, got string/);
  });

  it("throws when field has wrong type (number instead of boolean)", () => {
    expect(() =>
      validateSingletonField<boolean>(
        { found: 1 },
        "found",
        "boolean",
        "readPage",
      ),
    ).toThrow(/SAB bridge readPage: expected found to be boolean, got number/);
  });

  it("throws when field is undefined (explicitly set)", () => {
    expect(() =>
      validateSingletonField<number>(
        { count: undefined },
        "count",
        "number",
        "countPages",
      ),
    ).toThrow(/SAB bridge countPages: expected count to be number, got undefined/);
  });

  it("throws when field is null and expected type is number", () => {
    expect(() =>
      validateSingletonField<number>(
        { count: null },
        "count",
        "number",
        "countPages",
      ),
    ).toThrow(/SAB bridge countPages: expected count to be number, got null/);
  });

  it("throws when field is null and expected type is boolean", () => {
    expect(() =>
      validateSingletonField<boolean>(
        { found: null },
        "found",
        "boolean",
        "readPage",
      ),
    ).toThrow(/SAB bridge readPage: expected found to be boolean, got null/);
  });

  it("allows null when expected type is object", () => {
    const result = validateSingletonField<object | null>(
      { data: null },
      "data",
      "object",
      "getData",
    );
    expect(result).toBeNull();
  });

  it("throws when json is null", () => {
    expect(() =>
      validateSingletonField<number>(null, "count", "number", "countPages"),
    ).toThrow();
  });

  it("throws when field is an array but expected number", () => {
    expect(() =>
      validateSingletonField<number>(
        { count: [1, 2, 3] },
        "count",
        "number",
        "countPages",
      ),
    ).toThrow(/SAB bridge countPages: expected count to be number, got object/);
  });

  it("includes method name in error message", () => {
    expect(() =>
      validateSingletonField<number>({}, "removed", "number", "cleanupOrphanedPages"),
    ).toThrow(/SAB bridge cleanupOrphanedPages/);
  });

  it("includes field name in error message", () => {
    expect(() =>
      validateSingletonField<number>({}, "maxIdx", "number", "maxPageIndex"),
    ).toThrow(/'maxIdx' field/);
  });
});

describe("validateMetaResponse", () => {
  it("@fast returns null when meta is null", () => {
    const result = validateMetaResponse({ meta: null }, "readMeta");
    expect(result).toBeNull();
  });

  it("@fast returns the meta object when present", () => {
    const meta = {
      mode: 0o100644,
      size: 1024,
      atime: 1000,
      mtime: 2000,
      ctime: 3000,
    };
    const result = validateMetaResponse({ meta }, "readMeta");
    expect(result).toEqual(meta);
  });

  it("returns meta with optional fields", () => {
    const meta = {
      mode: 0o120777,
      size: 5,
      atime: 100,
      mtime: 200,
      ctime: 300,
      link: "/target",
    };
    const result = validateMetaResponse({ meta }, "readMeta");
    expect(result).toEqual(meta);
  });

  it("@fast throws when meta field is missing", () => {
    expect(() => validateMetaResponse({}, "readMeta")).toThrow(
      /SAB bridge readMeta: response missing 'meta' field/,
    );
  });

  it("@fast throws when meta is a string", () => {
    expect(() =>
      validateMetaResponse({ meta: "not-an-object" }, "readMeta"),
    ).toThrow(/SAB bridge readMeta: expected meta to be object or null, got string/);
  });

  it("throws when meta is a number", () => {
    expect(() =>
      validateMetaResponse({ meta: 42 }, "readMeta"),
    ).toThrow(/SAB bridge readMeta: expected meta to be object or null, got number/);
  });

  it("throws when meta is a boolean", () => {
    expect(() =>
      validateMetaResponse({ meta: true }, "readMeta"),
    ).toThrow(/SAB bridge readMeta: expected meta to be object or null, got boolean/);
  });

  it("throws when meta is undefined (explicitly set)", () => {
    expect(() =>
      validateMetaResponse({ meta: undefined }, "readMeta"),
    ).toThrow(/SAB bridge readMeta: expected meta to be object or null, got undefined/);
  });

  it("throws when json is null", () => {
    expect(() => validateMetaResponse(null, "readMeta")).toThrow();
  });

  it("includes method name in error messages", () => {
    expect(() => validateMetaResponse({}, "myMethod")).toThrow(
      /SAB bridge myMethod/,
    );
  });
});
