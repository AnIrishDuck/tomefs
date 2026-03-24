/**
 * Unit tests for MemoryBackend.
 *
 * Validates the StorageBackend interface contract using the in-memory
 * implementation. These tests also serve as the specification for any
 * future backend (IDB, OPFS).
 */
import { describe, it, expect, beforeEach } from "vitest";
import { MemoryBackend } from "../../src/memory-backend.js";
import { PAGE_SIZE } from "../../src/types.js";

describe("MemoryBackend", () => {
  let backend: MemoryBackend;

  beforeEach(() => {
    backend = new MemoryBackend();
  });

  describe("page operations", () => {
    it("@fast returns null for non-existent page", async () => {
      const page = await backend.readPage("/test", 0);
      expect(page).toBeNull();
    });

    it("@fast writes and reads a page", async () => {
      const data = new Uint8Array(PAGE_SIZE);
      data[0] = 0xde;
      data[1] = 0xad;
      data[PAGE_SIZE - 1] = 0xff;

      await backend.writePage("/test", 0, data);
      const read = await backend.readPage("/test", 0);

      expect(read).not.toBeNull();
      expect(read![0]).toBe(0xde);
      expect(read![1]).toBe(0xad);
      expect(read![PAGE_SIZE - 1]).toBe(0xff);
    });

    it("returns a copy, not a reference", async () => {
      const data = new Uint8Array(PAGE_SIZE);
      data[0] = 42;
      await backend.writePage("/test", 0, data);

      const read1 = await backend.readPage("/test", 0);
      read1![0] = 99;

      const read2 = await backend.readPage("/test", 0);
      expect(read2![0]).toBe(42);
    });

    it("stores pages independently by path and index", async () => {
      const data1 = new Uint8Array(PAGE_SIZE);
      data1[0] = 1;
      const data2 = new Uint8Array(PAGE_SIZE);
      data2[0] = 2;
      const data3 = new Uint8Array(PAGE_SIZE);
      data3[0] = 3;

      await backend.writePage("/a", 0, data1);
      await backend.writePage("/a", 1, data2);
      await backend.writePage("/b", 0, data3);

      expect((await backend.readPage("/a", 0))![0]).toBe(1);
      expect((await backend.readPage("/a", 1))![0]).toBe(2);
      expect((await backend.readPage("/b", 0))![0]).toBe(3);
    });

    it("overwrites existing page data", async () => {
      const data1 = new Uint8Array(PAGE_SIZE);
      data1[0] = 1;
      await backend.writePage("/test", 0, data1);

      const data2 = new Uint8Array(PAGE_SIZE);
      data2[0] = 2;
      await backend.writePage("/test", 0, data2);

      expect((await backend.readPage("/test", 0))![0]).toBe(2);
    });
  });

  describe("batch write", () => {
    it("writes multiple pages atomically", async () => {
      const pages = [
        { path: "/a", pageIndex: 0, data: new Uint8Array(PAGE_SIZE) },
        { path: "/a", pageIndex: 1, data: new Uint8Array(PAGE_SIZE) },
        { path: "/b", pageIndex: 0, data: new Uint8Array(PAGE_SIZE) },
      ];
      pages[0].data[0] = 10;
      pages[1].data[0] = 20;
      pages[2].data[0] = 30;

      await backend.writePages(pages);

      expect((await backend.readPage("/a", 0))![0]).toBe(10);
      expect((await backend.readPage("/a", 1))![0]).toBe(20);
      expect((await backend.readPage("/b", 0))![0]).toBe(30);
    });
  });

  describe("deleteFile", () => {
    it("removes all pages for a file", async () => {
      await backend.writePage("/a", 0, new Uint8Array(PAGE_SIZE));
      await backend.writePage("/a", 1, new Uint8Array(PAGE_SIZE));
      await backend.writePage("/b", 0, new Uint8Array(PAGE_SIZE));

      await backend.deleteFile("/a");

      expect(await backend.readPage("/a", 0)).toBeNull();
      expect(await backend.readPage("/a", 1)).toBeNull();
      expect(await backend.readPage("/b", 0)).not.toBeNull();
    });
  });

  describe("deletePagesFrom", () => {
    it("removes pages at and beyond the given index", async () => {
      await backend.writePage("/a", 0, new Uint8Array(PAGE_SIZE));
      await backend.writePage("/a", 1, new Uint8Array(PAGE_SIZE));
      await backend.writePage("/a", 2, new Uint8Array(PAGE_SIZE));
      await backend.writePage("/a", 3, new Uint8Array(PAGE_SIZE));

      await backend.deletePagesFrom("/a", 2);

      expect(await backend.readPage("/a", 0)).not.toBeNull();
      expect(await backend.readPage("/a", 1)).not.toBeNull();
      expect(await backend.readPage("/a", 2)).toBeNull();
      expect(await backend.readPage("/a", 3)).toBeNull();
    });
  });

  describe("metadata operations", () => {
    it("@fast returns null for non-existent metadata", async () => {
      expect(await backend.readMeta("/test")).toBeNull();
    });

    it("writes and reads metadata", async () => {
      const meta = { size: 1024, mode: 0o644, ctime: 1000, mtime: 2000 };
      await backend.writeMeta("/test", meta);

      const read = await backend.readMeta("/test");
      expect(read).toEqual(meta);
    });

    it("returns a copy of metadata", async () => {
      const meta = { size: 1024, mode: 0o644, ctime: 1000, mtime: 2000 };
      await backend.writeMeta("/test", meta);

      const read = await backend.readMeta("/test");
      read!.size = 9999;

      const read2 = await backend.readMeta("/test");
      expect(read2!.size).toBe(1024);
    });

    it("deleteMeta removes metadata", async () => {
      await backend.writeMeta("/test", {
        size: 0,
        mode: 0o644,
        ctime: 0,
        mtime: 0,
      });
      await backend.deleteMeta("/test");
      expect(await backend.readMeta("/test")).toBeNull();
    });
  });

  describe("listFiles", () => {
    it("returns empty array when no files exist", async () => {
      expect(await backend.listFiles()).toEqual([]);
    });

    it("lists all files with metadata", async () => {
      const meta = { size: 0, mode: 0o644, ctime: 0, mtime: 0 };
      await backend.writeMeta("/a", meta);
      await backend.writeMeta("/b", meta);
      await backend.writeMeta("/c/d", meta);

      const files = await backend.listFiles();
      expect(files.sort()).toEqual(["/a", "/b", "/c/d"]);
    });

    it("does not list deleted files", async () => {
      const meta = { size: 0, mode: 0o644, ctime: 0, mtime: 0 };
      await backend.writeMeta("/a", meta);
      await backend.writeMeta("/b", meta);
      await backend.deleteMeta("/a");

      expect(await backend.listFiles()).toEqual(["/b"]);
    });
  });
});
