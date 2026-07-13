/**
 * Unit tests for SyncMemoryBackend.assertInvariants() violation detection.
 *
 * The fuzz tests (differential, continuous-invariants, backend-invariants,
 * page-cache, persistence, dirty-shutdown, etc.) call backend.assertInvariants()
 * after every operation sequence to detect index corruption. If the invariant
 * checker itself has a bug — e.g., misses a certain type of violation — fuzz
 * tests could pass even when the backend is internally corrupted.
 *
 * These tests intentionally inject each type of invariant violation and
 * verify that assertInvariants() detects it with a descriptive error
 * message. Each test starts with a valid backend state, corrupts one
 * specific internal data structure, and checks detection.
 *
 * MemoryBackend (async) delegates assertInvariants() to its inner
 * SyncMemoryBackend, so testing the sync variant covers both.
 *
 * Invariants validated:
 *   1. pages ↔ filePageKeys bidirectional consistency
 *      - Every page key tracked in filePageKeys exists in pages
 *      - Every page key in pages is tracked in some filePageKeys set
 *      - No empty filePageKeys sets (should be deleted)
 *      - Key path prefix matches the filePageKeys path
 *      - No key appears under multiple paths
 *   2. filePageIndices consistent with filePageKeys
 *      - No empty filePageIndices maps (should be deleted)
 *      - Every filePageIndices path exists in filePageKeys
 *      - Size matches between filePageIndices and filePageKeys per path
 *      - Each index entry's key exists in the corresponding filePageKeys set
 *      - Each index entry's key matches expected pageKeyStr
 *      - Every filePageKeys path exists in filePageIndices
 *   3. fileMaxIdx correct for each file
 *      - Cached max matches actual max page index
 *      - No fileMaxIdx entries without corresponding pages
 *      - Every filePageIndices path exists in fileMaxIdx
 */

import { describe, it, expect, beforeEach } from "vitest";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { PAGE_SIZE, pageKeyStr } from "../../src/types.js";

function filledPage(value: number): Uint8Array {
  const buf = new Uint8Array(PAGE_SIZE);
  buf.fill(value);
  return buf;
}

describe("SyncMemoryBackend.assertInvariants() violation detection @fast", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  function internals(): any {
    return backend as any;
  }

  function setupValidState() {
    backend.writePage("/fileA", 0, filledPage(0xaa));
    backend.writePage("/fileA", 1, filledPage(0xbb));
    backend.writePage("/fileA", 3, filledPage(0xdd));
    backend.writePage("/fileB", 0, filledPage(0x11));
    backend.writePage("/fileB", 2, filledPage(0x33));
  }

  it("valid state passes without error", () => {
    setupValidState();
    expect(() => backend.assertInvariants()).not.toThrow();
  });

  it("empty backend passes without error", () => {
    expect(() => backend.assertInvariants()).not.toThrow();
  });

  // ---------------------------------------------------------------
  // Invariant 1: pages ↔ filePageKeys bidirectional consistency
  // ---------------------------------------------------------------

  describe("pages ↔ filePageKeys consistency", () => {
    it("detects page key in pages but not tracked in filePageKeys", () => {
      setupValidState();
      const key = pageKeyStr("/orphan", 0);
      internals().pages.set(key, filledPage(0xff));

      expect(() => backend.assertInvariants()).toThrow(
        /pages contains.*not tracked in filePageKeys/,
      );
    });

    it("detects filePageKeys entry pointing to nonexistent page", () => {
      setupValidState();
      const ghostKey = pageKeyStr("/fileA", 99);
      internals().filePageKeys.get("/fileA").add(ghostKey);

      expect(() => backend.assertInvariants()).toThrow(
        /filePageKeys.*contains.*not in pages/,
      );
    });

    it("detects empty filePageKeys set (should be deleted)", () => {
      setupValidState();
      internals().filePageKeys.set("/ghost", new Set());

      expect(() => backend.assertInvariants()).toThrow(
        /filePageKeys.*is empty.*should be deleted/,
      );
    });

    it("detects key path prefix mismatch in filePageKeys", () => {
      setupValidState();
      const wrongKey = pageKeyStr("/fileB", 0);
      internals().filePageKeys.get("/fileA").add(wrongKey);

      expect(() => backend.assertInvariants()).toThrow(
        /filePageKeys.*contains key with path=/,
      );
    });

    it("detects key appearing under multiple paths in filePageKeys", () => {
      setupValidState();
      const sharedKey = pageKeyStr("/fileA", 0);
      internals().filePageKeys.get("/fileB").add(sharedKey);

      expect(() => backend.assertInvariants()).toThrow(
        /key.*appears under multiple paths/,
      );
    });

    it("detects page removed from pages but still tracked in filePageKeys", () => {
      setupValidState();
      const key = pageKeyStr("/fileA", 0);
      internals().pages.delete(key);

      expect(() => backend.assertInvariants()).toThrow(
        /filePageKeys.*contains.*not in pages/,
      );
    });
  });

  // ---------------------------------------------------------------
  // Invariant 2: filePageIndices consistent with filePageKeys
  // ---------------------------------------------------------------

  describe("filePageIndices ↔ filePageKeys consistency", () => {
    it("detects empty filePageIndices map (should be deleted)", () => {
      setupValidState();
      internals().filePageIndices.set("/ghost", new Map());

      expect(() => backend.assertInvariants()).toThrow(
        /filePageIndices.*is empty.*should be deleted/,
      );
    });

    it("detects filePageIndices path not in filePageKeys", () => {
      setupValidState();
      const fakeIndices = new Map<number, string>();
      fakeIndices.set(0, pageKeyStr("/phantom", 0));
      internals().filePageIndices.set("/phantom", fakeIndices);

      expect(() => backend.assertInvariants()).toThrow(
        /filePageIndices has path.*not in filePageKeys/,
      );
    });

    it("detects size mismatch between filePageIndices and filePageKeys", () => {
      setupValidState();
      const extraKey = pageKeyStr("/fileA", 99);
      internals().filePageIndices.get("/fileA").set(99, extraKey);

      expect(() => backend.assertInvariants()).toThrow(
        /filePageIndices.*size.*!== filePageKeys.*size/,
      );
    });

    it("detects filePageIndices entry key not in filePageKeys", () => {
      setupValidState();
      const indices = internals().filePageIndices.get("/fileA");
      const wrongKey = pageKeyStr("/fileA", 77);
      indices.set(0, wrongKey);

      expect(() => backend.assertInvariants()).toThrow(
        /filePageIndices.*not in filePageKeys/,
      );
    });

    it("detects filePageIndices key not matching expected pageKeyStr", () => {
      setupValidState();
      const indices = internals().filePageIndices.get("/fileA");
      const realKey = pageKeyStr("/fileA", 0);
      indices.set(0, realKey + "_corrupted");
      internals().filePageKeys.get("/fileA").add(realKey + "_corrupted");
      internals().pages.set(realKey + "_corrupted", filledPage(0));

      expect(() => backend.assertInvariants()).toThrow(
        /filePageIndices.*but expected/,
      );
    });

    it("detects filePageKeys path not in filePageIndices", () => {
      setupValidState();
      internals().filePageIndices.delete("/fileB");

      expect(() => backend.assertInvariants()).toThrow(
        /filePageKeys has path.*not in filePageIndices/,
      );
    });
  });

  // ---------------------------------------------------------------
  // Invariant 3: fileMaxIdx correct for each file
  // ---------------------------------------------------------------

  describe("fileMaxIdx consistency", () => {
    it("detects fileMaxIdx too high (cached > actual)", () => {
      setupValidState();
      internals().fileMaxIdx.set("/fileA", 100);

      expect(() => backend.assertInvariants()).toThrow(
        /fileMaxIdx.*but actual max is/,
      );
    });

    it("detects fileMaxIdx too low (cached < actual)", () => {
      setupValidState();
      internals().fileMaxIdx.set("/fileA", 0);

      expect(() => backend.assertInvariants()).toThrow(
        /fileMaxIdx.*= 0 but actual max is/,
      );
    });

    it("detects fileMaxIdx entry with no corresponding pages", () => {
      setupValidState();
      internals().fileMaxIdx.set("/nonexistent", 5);

      expect(() => backend.assertInvariants()).toThrow(
        /fileMaxIdx.*but no pages exist/,
      );
    });

    it("detects filePageIndices path missing from fileMaxIdx", () => {
      setupValidState();
      internals().fileMaxIdx.delete("/fileB");

      expect(() => backend.assertInvariants()).toThrow(
        /filePageIndices has path.*not in fileMaxIdx/,
      );
    });

    it("detects fileMaxIdx stale after highest page deleted from indices", () => {
      setupValidState();
      // /fileA has pages 0, 1, 3. Remove page 3 from indices/keys/pages but
      // leave fileMaxIdx at 3 (stale).
      const key3 = pageKeyStr("/fileA", 3);
      internals().pages.delete(key3);
      internals().filePageKeys.get("/fileA").delete(key3);
      internals().filePageIndices.get("/fileA").delete(3);

      expect(() => backend.assertInvariants()).toThrow(
        /fileMaxIdx.*= 3 but actual max is 1/,
      );
    });
  });

  // ---------------------------------------------------------------
  // Compound violations (multiple simultaneous)
  // ---------------------------------------------------------------

  describe("compound violations", () => {
    it("detects multiple violations at once", () => {
      setupValidState();
      // Inject 3 violations simultaneously
      internals().fileMaxIdx.set("/fileA", 100);
      internals().filePageKeys.set("/ghost", new Set());
      const orphanKey = pageKeyStr("/orphan", 0);
      internals().pages.set(orphanKey, filledPage(0));

      expect(() => backend.assertInvariants()).toThrow(
        /invariant violations \((\d+)\)/,
      );
      try {
        backend.assertInvariants();
      } catch (e: any) {
        const match = e.message.match(/invariant violations \((\d+)\)/);
        expect(Number(match[1])).toBeGreaterThanOrEqual(3);
      }
    });
  });

  // ---------------------------------------------------------------
  // Invariant survival after legitimate operations
  // ---------------------------------------------------------------

  describe("invariants hold after legitimate operations", () => {
    it("after writePage", () => {
      backend.writePage("/f", 0, filledPage(1));
      backend.writePage("/f", 5, filledPage(2));
      backend.writePage("/g", 0, filledPage(3));
      expect(() => backend.assertInvariants()).not.toThrow();
    });

    it("after writePages batch", () => {
      backend.writePages([
        { path: "/a", pageIndex: 0, data: filledPage(1) },
        { path: "/a", pageIndex: 3, data: filledPage(2) },
        { path: "/b", pageIndex: 1, data: filledPage(3) },
        { path: "/c", pageIndex: 0, data: filledPage(4) },
      ]);
      expect(() => backend.assertInvariants()).not.toThrow();
    });

    it("after deleteFile", () => {
      setupValidState();
      backend.deleteFile("/fileA");
      expect(() => backend.assertInvariants()).not.toThrow();
    });

    it("after deleteFiles", () => {
      setupValidState();
      backend.deleteFiles(["/fileA", "/fileB"]);
      expect(() => backend.assertInvariants()).not.toThrow();
    });

    it("after deletePagesFrom", () => {
      setupValidState();
      backend.deletePagesFrom("/fileA", 1);
      expect(() => backend.assertInvariants()).not.toThrow();
      expect(backend.maxPageIndex("/fileA")).toBe(0);
    });

    it("after deletePagesFrom removes all pages", () => {
      setupValidState();
      backend.deletePagesFrom("/fileA", 0);
      expect(() => backend.assertInvariants()).not.toThrow();
      expect(backend.countPages("/fileA")).toBe(0);
    });

    it("after renameFile", () => {
      setupValidState();
      backend.renameFile("/fileA", "/fileC");
      expect(() => backend.assertInvariants()).not.toThrow();
      expect(backend.countPages("/fileA")).toBe(0);
      expect(backend.countPages("/fileC")).toBe(3);
      expect(backend.maxPageIndex("/fileC")).toBe(3);
    });

    it("after renameFile overwrites existing", () => {
      setupValidState();
      backend.renameFile("/fileA", "/fileB");
      expect(() => backend.assertInvariants()).not.toThrow();
      expect(backend.countPages("/fileA")).toBe(0);
      expect(backend.countPages("/fileB")).toBe(3);
    });

    it("after renameFile self-rename", () => {
      setupValidState();
      backend.renameFile("/fileA", "/fileA");
      expect(() => backend.assertInvariants()).not.toThrow();
      expect(backend.countPages("/fileA")).toBe(3);
    });

    it("after syncAll", () => {
      backend.syncAll(
        [
          { path: "/s1", pageIndex: 0, data: filledPage(1) },
          { path: "/s1", pageIndex: 1, data: filledPage(2) },
          { path: "/s2", pageIndex: 0, data: filledPage(3) },
        ],
        [
          { path: "/s1", meta: { size: PAGE_SIZE * 2, mode: 0o100644, ctime: 0, mtime: 0 } },
          { path: "/s2", meta: { size: PAGE_SIZE, mode: 0o100644, ctime: 0, mtime: 0 } },
        ],
      );
      expect(() => backend.assertInvariants()).not.toThrow();
    });

    it("after deleteAll", () => {
      setupValidState();
      backend.writeMeta("/fileA", { size: 0, mode: 0o100644, ctime: 0, mtime: 0 });
      backend.writeMeta("/fileB", { size: 0, mode: 0o100644, ctime: 0, mtime: 0 });
      backend.deleteAll(["/fileA", "/fileB"]);
      expect(() => backend.assertInvariants()).not.toThrow();
      expect(backend.countPages("/fileA")).toBe(0);
      expect(backend.countPages("/fileB")).toBe(0);
    });

    it("after cleanupOrphanedPages", () => {
      setupValidState();
      backend.writeMeta("/fileA", { size: 0, mode: 0o100644, ctime: 0, mtime: 0 });
      // /fileB has pages but no metadata — should be cleaned up
      const removed = backend.cleanupOrphanedPages();
      expect(removed).toBe(1);
      expect(() => backend.assertInvariants()).not.toThrow();
      expect(backend.countPages("/fileB")).toBe(0);
    });

    it("after overwriting existing page", () => {
      setupValidState();
      backend.writePage("/fileA", 0, filledPage(0xff));
      expect(() => backend.assertInvariants()).not.toThrow();
      expect(backend.readPage("/fileA", 0)![0]).toBe(0xff);
    });

    it("after complex lifecycle: write → rename → delete partial → write more", () => {
      backend.writePage("/x", 0, filledPage(1));
      backend.writePage("/x", 1, filledPage(2));
      backend.writePage("/x", 2, filledPage(3));
      expect(() => backend.assertInvariants()).not.toThrow();

      backend.renameFile("/x", "/y");
      expect(() => backend.assertInvariants()).not.toThrow();

      backend.deletePagesFrom("/y", 2);
      expect(() => backend.assertInvariants()).not.toThrow();

      backend.writePage("/y", 5, filledPage(6));
      expect(() => backend.assertInvariants()).not.toThrow();
      expect(backend.maxPageIndex("/y")).toBe(5);
      expect(backend.countPages("/y")).toBe(3);

      backend.writePage("/z", 0, filledPage(7));
      backend.renameFile("/z", "/y");
      expect(() => backend.assertInvariants()).not.toThrow();
      expect(backend.countPages("/y")).toBe(1);
      expect(backend.maxPageIndex("/y")).toBe(0);
    });
  });
});
