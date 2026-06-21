/**
 * Tests for SAB worker request parameter validation.
 *
 * The SabWorker validates incoming request parameters to catch protocol bugs
 * or buffer corruption early with clear error messages, instead of letting
 * malformed requests propagate to the backend as undefined/NaN values.
 *
 * Mirrors tests/unit/sab-response-validation.test.ts which covers the
 * client-side response validation (PR #342).
 */
import { describe, it, expect } from "vitest";
import {
  validateStringParam,
  validateNumberParam,
  validateArrayParam,
  validateObjectParam,
} from "../../src/sab-worker.js";

describe("validateStringParam", () => {
  it("@fast returns the string when field exists and is a string", () => {
    expect(validateStringParam({ path: "/foo/bar" }, "path", "READ_PAGE")).toBe(
      "/foo/bar",
    );
  });

  it("@fast returns empty string", () => {
    expect(validateStringParam({ path: "" }, "path", "READ_PAGE")).toBe("");
  });

  it("@fast throws when field is missing", () => {
    expect(() => validateStringParam({}, "path", "READ_PAGE")).toThrow(
      /READ_PAGE: expected path to be string, got undefined/,
    );
  });

  it("@fast throws when field is a number", () => {
    expect(() =>
      validateStringParam({ path: 42 }, "path", "WRITE_META"),
    ).toThrow(/WRITE_META: expected path to be string, got number/);
  });

  it("throws when field is null", () => {
    expect(() =>
      validateStringParam({ path: null }, "path", "DELETE_FILE"),
    ).toThrow(/DELETE_FILE: expected path to be string, got null/);
  });

  it("throws when field is boolean", () => {
    expect(() =>
      validateStringParam({ path: true }, "path", "READ_META"),
    ).toThrow(/READ_META: expected path to be string, got boolean/);
  });

  it("throws when field is an array", () => {
    expect(() =>
      validateStringParam({ path: ["a", "b"] }, "path", "COUNT_PAGES"),
    ).toThrow(/COUNT_PAGES: expected path to be string, got object/);
  });

  it("throws when field is an object", () => {
    expect(() =>
      validateStringParam({ path: { x: 1 } }, "path", "RENAME_FILE"),
    ).toThrow(/RENAME_FILE: expected path to be string, got object/);
  });

  it("includes op name in error", () => {
    expect(() =>
      validateStringParam({}, "oldPath", "RENAME_FILE"),
    ).toThrow(/RENAME_FILE/);
  });

  it("includes field name in error", () => {
    expect(() =>
      validateStringParam({}, "newPath", "RENAME_FILE"),
    ).toThrow(/newPath/);
  });
});

describe("validateNumberParam", () => {
  it("@fast returns the number when field exists and is a number", () => {
    expect(
      validateNumberParam({ pageIndex: 5 }, "pageIndex", "READ_PAGE"),
    ).toBe(5);
  });

  it("@fast returns zero", () => {
    expect(
      validateNumberParam({ pageIndex: 0 }, "pageIndex", "READ_PAGE"),
    ).toBe(0);
  });

  it("returns negative numbers", () => {
    expect(
      validateNumberParam({ maxIdx: -1 }, "maxIdx", "MAX_PAGE_INDEX"),
    ).toBe(-1);
  });

  it("@fast throws when field is missing", () => {
    expect(() =>
      validateNumberParam({}, "pageIndex", "WRITE_PAGE"),
    ).toThrow(/WRITE_PAGE: expected pageIndex to be number, got undefined/);
  });

  it("@fast throws when field is a string", () => {
    expect(() =>
      validateNumberParam({ pageIndex: "5" }, "pageIndex", "READ_PAGE"),
    ).toThrow(/READ_PAGE: expected pageIndex to be number, got string/);
  });

  it("throws when field is null", () => {
    expect(() =>
      validateNumberParam({ offset: null }, "offset", "LIST_FILES_RANGE"),
    ).toThrow(/LIST_FILES_RANGE: expected offset to be number, got null/);
  });

  it("throws when field is boolean", () => {
    expect(() =>
      validateNumberParam({ limit: false }, "limit", "LIST_FILES_RANGE"),
    ).toThrow(/LIST_FILES_RANGE: expected limit to be number, got boolean/);
  });

  it("throws when field is an array", () => {
    expect(() =>
      validateNumberParam({ dataLen: [100] }, "dataLen", "WRITE_PAGE"),
    ).toThrow(/WRITE_PAGE: expected dataLen to be number, got object/);
  });

  it("includes op name in error", () => {
    expect(() =>
      validateNumberParam({}, "fromPageIndex", "DELETE_PAGES_FROM"),
    ).toThrow(/DELETE_PAGES_FROM/);
  });

  it("includes field name in error", () => {
    expect(() =>
      validateNumberParam({}, "fromPageIndex", "DELETE_PAGES_FROM"),
    ).toThrow(/fromPageIndex/);
  });
});

describe("validateArrayParam", () => {
  it("@fast returns the array when field exists and is an array", () => {
    expect(
      validateArrayParam({ paths: ["/a", "/b"] }, "paths", "DELETE_FILES"),
    ).toEqual(["/a", "/b"]);
  });

  it("@fast returns empty array", () => {
    expect(
      validateArrayParam({ paths: [] }, "paths", "DELETE_METAS"),
    ).toEqual([]);
  });

  it("returns array with mixed types", () => {
    expect(
      validateArrayParam(
        { pageIndices: [0, 1, 2] },
        "pageIndices",
        "READ_PAGES",
      ),
    ).toEqual([0, 1, 2]);
  });

  it("@fast throws when field is missing", () => {
    expect(() =>
      validateArrayParam({}, "paths", "DELETE_ALL"),
    ).toThrow(/DELETE_ALL: expected paths to be array, got undefined/);
  });

  it("@fast throws when field is a string", () => {
    expect(() =>
      validateArrayParam({ paths: "/a" }, "paths", "DELETE_FILES"),
    ).toThrow(/DELETE_FILES: expected paths to be array, got string/);
  });

  it("throws when field is null", () => {
    expect(() =>
      validateArrayParam({ entries: null }, "entries", "WRITE_METAS"),
    ).toThrow(/WRITE_METAS: expected entries to be array, got null/);
  });

  it("throws when field is a number", () => {
    expect(() =>
      validateArrayParam({ pageIndices: 42 }, "pageIndices", "READ_PAGES"),
    ).toThrow(/READ_PAGES: expected pageIndices to be array, got number/);
  });

  it("throws when field is an object", () => {
    expect(() =>
      validateArrayParam(
        { pages: { path: "/a" } },
        "pages",
        "WRITE_PAGES",
      ),
    ).toThrow(/WRITE_PAGES: expected pages to be array, got object/);
  });

  it("throws when field is boolean", () => {
    expect(() =>
      validateArrayParam({ metas: true }, "metas", "SYNC_ALL"),
    ).toThrow(/SYNC_ALL: expected metas to be array, got boolean/);
  });

  it("includes op name in error", () => {
    expect(() =>
      validateArrayParam({}, "entries", "WRITE_METAS"),
    ).toThrow(/WRITE_METAS/);
  });

  it("includes field name in error", () => {
    expect(() =>
      validateArrayParam({}, "pageIndices", "READ_PAGES"),
    ).toThrow(/pageIndices/);
  });
});

describe("validateObjectParam", () => {
  it("@fast returns the object when field exists and is an object", () => {
    const meta = { size: 100, mode: 0o644, ctime: 1000, mtime: 2000 };
    expect(
      validateObjectParam({ meta }, "meta", "WRITE_META"),
    ).toEqual(meta);
  });

  it("@fast returns empty object", () => {
    expect(
      validateObjectParam({ meta: {} }, "meta", "WRITE_META"),
    ).toEqual({});
  });

  it("@fast throws when field is missing", () => {
    expect(() =>
      validateObjectParam({}, "meta", "WRITE_META"),
    ).toThrow(/WRITE_META: expected meta to be object, got undefined/);
  });

  it("@fast throws when field is null", () => {
    expect(() =>
      validateObjectParam({ meta: null }, "meta", "WRITE_META"),
    ).toThrow(/WRITE_META: expected meta to be object, got null/);
  });

  it("throws when field is a string", () => {
    expect(() =>
      validateObjectParam({ meta: "not-meta" }, "meta", "WRITE_META"),
    ).toThrow(/WRITE_META: expected meta to be object, got string/);
  });

  it("throws when field is a number", () => {
    expect(() =>
      validateObjectParam({ meta: 42 }, "meta", "WRITE_META"),
    ).toThrow(/WRITE_META: expected meta to be object, got number/);
  });

  it("throws when field is an array", () => {
    expect(() =>
      validateObjectParam({ meta: [1, 2] }, "meta", "WRITE_META"),
    ).toThrow(/WRITE_META: expected meta to be object, got array/);
  });

  it("throws when field is boolean", () => {
    expect(() =>
      validateObjectParam({ meta: true }, "meta", "WRITE_META"),
    ).toThrow(/WRITE_META: expected meta to be object, got boolean/);
  });

  it("includes op name in error", () => {
    expect(() =>
      validateObjectParam({}, "meta", "WRITE_META"),
    ).toThrow(/WRITE_META/);
  });

  it("includes field name in error", () => {
    expect(() =>
      validateObjectParam({ wrong: {} }, "meta", "WRITE_META"),
    ).toThrow(/meta/);
  });
});
