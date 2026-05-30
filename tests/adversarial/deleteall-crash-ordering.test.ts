/**
 * Adversarial tests: deleteAll must delete metadata before pages.
 *
 * OPFS backends can't atomically delete pages and metadata (unlike IDB which
 * uses a single multi-store transaction). If a crash occurs mid-deleteAll,
 * the ordering determines the failure mode:
 *
 * - Metadata first (correct): orphaned pages remain, invisible to listFiles().
 *   Cleaned up safely by cleanupOrphanedPages() on next mount.
 *
 * - Pages first (wrong): ghost metadata entries appear in listFiles() with no
 *   backing data. restoreTree creates files that read as zeros — silent data
 *   corruption.
 *
 * These tests verify the crash-safety property by simulating a partial
 * deleteAll (only metadata deleted, crash before page deletion) and
 * confirming the resulting state is safe: deleted files are invisible to
 * listFiles(), surviving files are intact.
 *
 * Ethos §9: "target the seams: metadata updates after flush"
 */

import { describe, it, expect } from "vitest";
import "fake-indexeddb/auto";
import { MemoryBackend } from "../../src/memory-backend.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { IdbBackend } from "../../src/idb-backend.js";
import { OpfsBackend } from "../../src/opfs-backend.js";
import { OpfsSahBackend } from "../../src/opfs-sah-backend.js";
import { PAGE_SIZE } from "../../src/types.js";
import type { FileMeta } from "../../src/types.js";
import { createFakeOpfsRoot } from "../harness/fake-opfs.js";

function filledPage(value: number): Uint8Array {
  const page = new Uint8Array(PAGE_SIZE);
  page.fill(value);
  return page;
}

const meta: FileMeta = {
  size: 8192,
  mode: 0o100644,
  ctime: 1000,
  mtime: 2000,
};

describe("deleteAll crash ordering", () => {
  describe("SyncMemoryBackend @fast", () => {
    it("partial deleteAll (metadata deleted, pages survive) leaves deleted files invisible", () => {
      const backend = new SyncMemoryBackend();

      backend.writePage("/keep", 0, filledPage(0x01));
      backend.writeMeta("/keep", meta);
      backend.writePage("/remove", 0, filledPage(0xaa));
      backend.writePage("/remove", 1, filledPage(0xbb));
      backend.writeMeta("/remove", meta);

      // Simulate the first phase of metadata-first deleteAll:
      // metadata is deleted, but crash prevents page deletion.
      backend.deleteMetas(["/remove"]);

      // listFiles must NOT return the removed path — this is the key
      // safety property. Ghost metadata entries would cause restoreTree
      // to create files with no data.
      expect(backend.listFiles()).toEqual(["/keep"]);

      // Pages are orphaned but invisible to the filesystem.
      // They waste space but don't corrupt state.
      expect(backend.readPage("/remove", 0)).not.toBeNull();

      // Kept file is completely untouched.
      expect(backend.readPage("/keep", 0)).toEqual(filledPage(0x01));
      expect(backend.readMeta("/keep")).toEqual(meta);
    });

    it("complete deleteAll leaves no traces", () => {
      const backend = new SyncMemoryBackend();

      backend.writePage("/f", 0, filledPage(0x42));
      backend.writeMeta("/f", meta);

      backend.deleteAll(["/f"]);

      expect(backend.listFiles()).toEqual([]);
      expect(backend.readPage("/f", 0)).toBeNull();
      expect(backend.readMeta("/f")).toBeNull();
    });

    it("partial deleteAll with multiple files leaves correct state", () => {
      const backend = new SyncMemoryBackend();

      backend.writePage("/a", 0, filledPage(0x01));
      backend.writeMeta("/a", meta);
      backend.writePage("/b", 0, filledPage(0x02));
      backend.writePage("/b", 1, filledPage(0x03));
      backend.writeMeta("/b", meta);
      backend.writePage("/c", 0, filledPage(0x04));
      backend.writeMeta("/c", meta);

      // Simulate crash after metadata deletion for /a and /b
      backend.deleteMetas(["/a", "/b"]);

      // Only /c remains visible
      expect(backend.listFiles().sort()).toEqual(["/c"]);

      // /c data intact
      expect(backend.readPage("/c", 0)).toEqual(filledPage(0x04));
      expect(backend.readMeta("/c")).toEqual(meta);
    });

    it("reverse order (pages first) would create ghost metadata — contrast test", () => {
      const backend = new SyncMemoryBackend();

      backend.writePage("/f", 0, filledPage(0xaa));
      backend.writeMeta("/f", meta);

      // Simulate the WRONG order: pages deleted first, metadata survives.
      // This is what the old code did — demonstrating why it's dangerous.
      backend.deleteFiles(["/f"]);

      // Ghost metadata: file appears in listFiles but has no data.
      // restoreTree would create a file node that reads as zeros.
      expect(backend.listFiles()).toContain("/f");
      expect(backend.readMeta("/f")).not.toBeNull();
      expect(backend.readPage("/f", 0)).toBeNull();
    });
  });

  describe("MemoryBackend (async) @fast", () => {
    it("partial deleteAll (metadata deleted, pages survive) leaves deleted files invisible", async () => {
      const backend = new MemoryBackend();

      await backend.writePage("/keep", 0, filledPage(0x01));
      await backend.writeMeta("/keep", meta);
      await backend.writePage("/remove", 0, filledPage(0xaa));
      await backend.writePage("/remove", 1, filledPage(0xbb));
      await backend.writeMeta("/remove", meta);

      // Simulate first phase of metadata-first deleteAll
      await backend.deleteMetas(["/remove"]);

      expect(await backend.listFiles()).toEqual(["/keep"]);
      expect(await backend.readPage("/remove", 0)).not.toBeNull();
      expect(await backend.readPage("/keep", 0)).toEqual(filledPage(0x01));
      expect(await backend.readMeta("/keep")).toEqual(meta);
    });
  });

  describe("IdbBackend @fast", () => {
    let backend: IdbBackend;
    let dbCounter = 0;

    it("cleanupOrphanedPages recovers from partial deleteAll", async () => {
      backend = new IdbBackend({ dbName: `deleteall-crash-${dbCounter++}` });

      await backend.writePage("/keep", 0, filledPage(0x01));
      await backend.writeMeta("/keep", meta);
      await backend.writePage("/remove", 0, filledPage(0xaa));
      await backend.writeMeta("/remove", meta);

      // Simulate metadata-first crash: metadata deleted, pages survive
      await backend.deleteMetas(["/remove"]);

      expect(await backend.listFiles()).toEqual(["/keep"]);
      expect(await backend.readPage("/remove", 0)).not.toBeNull();

      // IDB's cleanupOrphanedPages finds the orphaned pages
      const cleaned = await backend.cleanupOrphanedPages();
      expect(cleaned).toBe(1);

      expect(await backend.readPage("/remove", 0)).toBeNull();
      expect(await backend.readPage("/keep", 0)).toEqual(filledPage(0x01));

      await backend.destroy();
    });
  });

  describe("OpfsBackend @fast", () => {
    it("cleanupOrphanedPages recovers from partial deleteAll", async () => {
      const root = createFakeOpfsRoot();
      const backend = new OpfsBackend({ root: root as any });

      await backend.writePage("/keep", 0, filledPage(0x01));
      await backend.writeMeta("/keep", meta);
      await backend.writePage("/remove", 0, filledPage(0xaa));
      await backend.writeMeta("/remove", meta);

      // Simulate metadata-first crash
      await backend.deleteMetas(["/remove"]);

      expect(await backend.listFiles()).toEqual(["/keep"]);

      const cleaned = await backend.cleanupOrphanedPages();
      expect(cleaned).toBe(1);

      expect(await backend.readPage("/remove", 0)).toBeNull();
      expect(await backend.readPage("/keep", 0)).toEqual(filledPage(0x01));

      await backend.destroy();
    });
  });

  describe("OpfsSahBackend @fast", () => {
    it("cleanupOrphanedPages recovers from partial deleteAll", async () => {
      const root = createFakeOpfsRoot();
      const backend = new OpfsSahBackend({ root: root as any });

      await backend.writePage("/keep", 0, filledPage(0x01));
      await backend.writeMeta("/keep", meta);
      await backend.writePage("/remove", 0, filledPage(0xaa));
      await backend.writeMeta("/remove", meta);

      // Simulate metadata-first crash
      await backend.deleteMetas(["/remove"]);

      expect(await backend.listFiles()).toEqual(["/keep"]);

      const cleaned = await backend.cleanupOrphanedPages();
      expect(cleaned).toBe(1);

      expect(await backend.readPage("/remove", 0)).toBeNull();
      expect(await backend.readPage("/keep", 0)).toEqual(filledPage(0x01));

      await backend.destroy();
    });
  });
});
