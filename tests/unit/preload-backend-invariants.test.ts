/**
 * Unit tests for PreloadBackend.assertInvariants() violation detection.
 *
 * The fuzz tests (backend-invariants.test.ts) rely on assertInvariants() to
 * detect corruption in the 8+ concurrent data structures PreloadBackend
 * maintains (pages, meta, filePageKeys, filePageIndices, fileMaxIdx,
 * dirtyPages, dirtyMeta, deletedFiles, deletedMeta, truncations). If the
 * invariant checker has a bug — e.g., misses a certain type of violation —
 * fuzz tests could pass even when the backend is internally corrupted.
 *
 * These tests intentionally inject each type of invariant violation and
 * verify that assertInvariants() detects it with a descriptive error
 * message. Each test starts with a valid backend state, corrupts one
 * specific aspect, and checks detection.
 *
 * Invariants validated:
 *   1. Every key in pages must appear in exactly one filePageKeys set
 *   2. filePageIndices consistent with filePageKeys
 *   3. fileMaxIdx correct for each file
 *   4. Every dirty page key must exist in pages
 *   5. Every dirty meta path must exist in meta
 *   6. Deleted files should not have non-dirty pages
 *   7. Deleted meta paths should not exist in meta
 *   8. Truncation points: pages beyond truncation are only valid if dirty
 */

import { describe, it, expect, beforeEach } from "vitest";
import { MemoryBackend } from "../../src/memory-backend.js";
import { PreloadBackend } from "../../src/preload-backend.js";
import { PAGE_SIZE, pageKeyStr } from "../../src/types.js";
import type { FileMeta } from "../../src/types.js";

function filledPage(value: number): Uint8Array {
  const buf = new Uint8Array(PAGE_SIZE);
  buf.fill(value);
  return buf;
}

function makeMeta(size: number): FileMeta {
  return { size, mode: 0o100644, ctime: 1000, mtime: 1000 };
}

describe("PreloadBackend.assertInvariants() violation detection @fast", () => {
  let remote: MemoryBackend;
  let backend: PreloadBackend;

  beforeEach(async () => {
    remote = new MemoryBackend();
    backend = new PreloadBackend(remote);
    await backend.init();
  });

  function internals(): any {
    return backend as any;
  }

  function setupValidState() {
    backend.writeMeta("/fileA", makeMeta(PAGE_SIZE * 2));
    backend.writePage("/fileA", 0, filledPage(0xaa));
    backend.writePage("/fileA", 1, filledPage(0xbb));
    backend.writeMeta("/fileB", makeMeta(PAGE_SIZE));
    backend.writePage("/fileB", 0, filledPage(0xcc));
  }

  it("valid state passes without error", () => {
    setupValidState();
    expect(() => backend.assertInvariants()).not.toThrow();
  });

  it("empty backend passes", () => {
    expect(() => backend.assertInvariants()).not.toThrow();
  });

  // ---------------------------------------------------------------
  // Invariant 1: Every key in pages ↔ filePageKeys bidirectional
  // ---------------------------------------------------------------

  it("detects page key not tracked in filePageKeys", () => {
    setupValidState();
    const orphanKey = pageKeyStr("/orphan", 0);
    internals().pages.set(orphanKey, filledPage(0xff));
    expect(() => backend.assertInvariants()).toThrow(
      /not tracked in filePageKeys/,
    );
  });

  it("detects filePageKeys referencing key not in pages", () => {
    setupValidState();
    const keys = internals().filePageKeys.get("/fileA");
    const phantomKey = pageKeyStr("/fileA", 99);
    keys.add(phantomKey);
    expect(() => backend.assertInvariants()).toThrow(
      /not in pages/,
    );
  });

  it("detects empty filePageKeys set (should be deleted)", () => {
    setupValidState();
    internals().filePageKeys.set("/empty", new Set());
    expect(() => backend.assertInvariants()).toThrow(
      /is empty \(should be deleted\)/,
    );
  });

  it("detects key appearing under multiple paths in filePageKeys", () => {
    setupValidState();
    const key = pageKeyStr("/fileA", 0);
    let bKeys = internals().filePageKeys.get("/fileB");
    if (!bKeys) {
      bKeys = new Set();
      internals().filePageKeys.set("/fileB", bKeys);
    }
    bKeys.add(key);
    expect(() => backend.assertInvariants()).toThrow(
      /appears under multiple paths/,
    );
  });

  it("detects filePageKeys key with wrong path prefix", () => {
    setupValidState();
    const wrongKey = pageKeyStr("/wrong", 0);
    internals().pages.set(wrongKey, filledPage(0xdd));
    const keys = internals().filePageKeys.get("/fileA");
    keys.add(wrongKey);
    expect(() => backend.assertInvariants()).toThrow(
      /contains key with path=/,
    );
  });

  // ---------------------------------------------------------------
  // Invariant 2: filePageIndices consistent with filePageKeys
  // ---------------------------------------------------------------

  it("detects empty filePageIndices (should be deleted)", () => {
    setupValidState();
    internals().filePageIndices.set("/ghost", new Map());
    expect(() => backend.assertInvariants()).toThrow(
      /filePageIndices.*is empty \(should be deleted\)/,
    );
  });

  it("detects filePageIndices path not in filePageKeys", () => {
    setupValidState();
    const indices = new Map<number, string>();
    indices.set(0, pageKeyStr("/phantom", 0));
    internals().filePageIndices.set("/phantom", indices);
    expect(() => backend.assertInvariants()).toThrow(
      /filePageIndices has path.*not in filePageKeys/,
    );
  });

  it("detects filePageIndices size mismatch with filePageKeys", () => {
    setupValidState();
    const indices = internals().filePageIndices.get("/fileA") as Map<number, string>;
    indices.set(99, pageKeyStr("/fileA", 99));
    expect(() => backend.assertInvariants()).toThrow(
      /size.*!== filePageKeys/,
    );
  });

  it("detects filePageIndices key not in filePageKeys", () => {
    setupValidState();
    const indices = internals().filePageIndices.get("/fileA") as Map<number, string>;
    const keys = internals().filePageKeys.get("/fileA") as Set<string>;
    const wrongKey = pageKeyStr("/fileA", 77);
    indices.set(1, wrongKey);
    keys.delete(pageKeyStr("/fileA", 1));
    keys.add(wrongKey);
    internals().pages.delete(pageKeyStr("/fileA", 1));
    internals().pages.set(wrongKey, filledPage(0xee));
    expect(() => backend.assertInvariants()).toThrow(
      /but expected/,
    );
  });

  it("detects filePageKeys path not in filePageIndices", () => {
    setupValidState();
    internals().filePageIndices.delete("/fileB");
    expect(() => backend.assertInvariants()).toThrow(
      /filePageKeys has path.*not in filePageIndices/,
    );
  });

  // ---------------------------------------------------------------
  // Invariant 3: fileMaxIdx correct for each file
  // ---------------------------------------------------------------

  it("detects fileMaxIdx too high", () => {
    setupValidState();
    internals().fileMaxIdx.set("/fileA", 10);
    expect(() => backend.assertInvariants()).toThrow(
      /fileMaxIdx.*= 10 but actual max is/,
    );
  });

  it("detects fileMaxIdx too low", () => {
    setupValidState();
    internals().fileMaxIdx.set("/fileA", 0);
    expect(() => backend.assertInvariants()).toThrow(
      /fileMaxIdx.*= 0 but actual max is 1/,
    );
  });

  it("detects fileMaxIdx with no pages", () => {
    setupValidState();
    internals().filePageIndices.set("/gone", new Map());
    internals().fileMaxIdx.set("/gone", 5);
    expect(() => backend.assertInvariants()).toThrow(
      /fileMaxIdx.*but no pages exist/,
    );
  });

  it("detects filePageIndices path not in fileMaxIdx", () => {
    setupValidState();
    internals().fileMaxIdx.delete("/fileA");
    expect(() => backend.assertInvariants()).toThrow(
      /filePageIndices has path.*not in fileMaxIdx/,
    );
  });

  // ---------------------------------------------------------------
  // Invariant 4: Every dirty page key must exist in pages
  // ---------------------------------------------------------------

  it("detects dirty page key not in pages", () => {
    setupValidState();
    const ghostKey = pageKeyStr("/gone", 0);
    internals().dirtyPages.add(ghostKey);
    expect(() => backend.assertInvariants()).toThrow(
      /dirtyPages contains.*not in pages/,
    );
  });

  it("detects dirty page key after page removal", () => {
    setupValidState();
    const key = pageKeyStr("/fileA", 0);
    internals().pages.delete(key);
    expect(() => backend.assertInvariants()).toThrow(
      /dirtyPages contains.*not in pages/,
    );
  });

  // ---------------------------------------------------------------
  // Invariant 5: Every dirty meta path must exist in meta
  // ---------------------------------------------------------------

  it("detects dirty meta path not in meta", () => {
    setupValidState();
    internals().dirtyMeta.add("/vanished");
    expect(() => backend.assertInvariants()).toThrow(
      /dirtyMeta contains.*not in meta/,
    );
  });

  it("detects dirty meta after meta removal", () => {
    setupValidState();
    internals().meta.delete("/fileA");
    expect(() => backend.assertInvariants()).toThrow(
      /dirtyMeta contains.*not in meta/,
    );
  });

  // ---------------------------------------------------------------
  // Invariant 6: Deleted files should not have non-dirty pages
  // ---------------------------------------------------------------

  it("detects non-dirty page in deleted file", () => {
    setupValidState();
    internals().deletedFiles.add("/fileB");
    internals().dirtyPages.delete(pageKeyStr("/fileB", 0));
    expect(() => backend.assertInvariants()).toThrow(
      /deletedFiles contains.*which has non-dirty page/,
    );
  });

  it("allows dirty pages in deleted file (delete-then-recreate)", () => {
    setupValidState();
    internals().deletedFiles.add("/fileB");
    expect(() => backend.assertInvariants()).not.toThrow();
  });

  // ---------------------------------------------------------------
  // Invariant 7: Deleted meta paths should not exist in meta
  // ---------------------------------------------------------------

  it("detects deleted meta path still in meta", () => {
    setupValidState();
    internals().deletedMeta.add("/fileA");
    expect(() => backend.assertInvariants()).toThrow(
      /deletedMeta contains.*which still exists in meta/,
    );
  });

  it("allows deleted meta path not in meta (normal delete)", () => {
    setupValidState();
    internals().deletedMeta.add("/gone");
    expect(() => backend.assertInvariants()).not.toThrow();
  });

  // ---------------------------------------------------------------
  // Invariant 8: Truncation - pages beyond point must be dirty
  // ---------------------------------------------------------------

  it("detects non-dirty page beyond truncation point", () => {
    setupValidState();
    internals().truncations.set("/fileA", 1);
    internals().dirtyPages.delete(pageKeyStr("/fileA", 1));
    expect(() => backend.assertInvariants()).toThrow(
      /truncations.*but non-dirty page 1 still exists/,
    );
  });

  it("allows dirty page beyond truncation point", () => {
    setupValidState();
    internals().truncations.set("/fileA", 1);
    expect(() => backend.assertInvariants()).not.toThrow();
  });

  it("allows truncation with no pages beyond point", () => {
    setupValidState();
    internals().truncations.set("/fileB", 5);
    expect(() => backend.assertInvariants()).not.toThrow();
  });

  // ---------------------------------------------------------------
  // Compound violations — multiple invariants violated at once
  // ---------------------------------------------------------------

  it("detects multiple simultaneous violations", () => {
    setupValidState();
    internals().dirtyPages.add(pageKeyStr("/gone", 0));
    internals().dirtyMeta.add("/vanished");
    internals().deletedMeta.add("/fileA");
    expect(() => backend.assertInvariants()).toThrow(
      /PreloadBackend invariant violations \(3\)/,
    );
  });

  // ---------------------------------------------------------------
  // Invariant survival after legitimate operations
  // ---------------------------------------------------------------

  it("survives after flush clears dirty state", async () => {
    setupValidState();
    expect(() => backend.assertInvariants()).not.toThrow();
    await backend.flush();
    expect(() => backend.assertInvariants()).not.toThrow();
  });

  it("survives after deleteFile", () => {
    setupValidState();
    backend.deleteFile("/fileA");
    expect(() => backend.assertInvariants()).not.toThrow();
  });

  it("survives after renameFile", () => {
    setupValidState();
    backend.renameFile("/fileA", "/fileC");
    expect(() => backend.assertInvariants()).not.toThrow();
  });

  it("survives after deletePagesFrom", () => {
    setupValidState();
    backend.deletePagesFrom("/fileA", 1);
    expect(() => backend.assertInvariants()).not.toThrow();
  });

  it("survives after rename-overwrite (delete-then-recreate)", () => {
    setupValidState();
    backend.renameFile("/fileA", "/fileB");
    expect(() => backend.assertInvariants()).not.toThrow();
  });

  it("survives after write to new file", () => {
    setupValidState();
    backend.writeMeta("/new", makeMeta(PAGE_SIZE));
    backend.writePage("/new", 0, filledPage(0xdd));
    expect(() => backend.assertInvariants()).not.toThrow();
  });

  it("survives after deleteAll", () => {
    setupValidState();
    backend.deleteAll(["/fileA", "/fileB"]);
    expect(() => backend.assertInvariants()).not.toThrow();
  });

  it("survives after cleanupOrphanedPages", () => {
    setupValidState();
    internals().meta.delete("/fileB");
    internals().dirtyMeta.delete("/fileB");
    backend.cleanupOrphanedPages();
    expect(() => backend.assertInvariants()).not.toThrow();
  });
});
