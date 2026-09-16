/**
 * Unit tests for PreloadBackend.close().
 *
 * Validates that close() properly:
 * - Flushes dirty state before releasing resources
 * - Clears all in-memory state
 * - Closes the underlying remote backend if it supports it
 * - Allows re-initialization via init()
 * - Is idempotent (safe to call multiple times)
 * - Throws on use-after-close without re-init
 */
import { describe, it, expect } from "vitest";
import { MemoryBackend } from "../../src/memory-backend.js";
import { PreloadBackend } from "../../src/preload-backend.js";
import { PAGE_SIZE } from "../../src/types.js";
import type { StorageBackend } from "../../src/storage-backend.js";
import type { FileMeta } from "../../src/types.js";

function filledPage(value: number): Uint8Array {
  const page = new Uint8Array(PAGE_SIZE);
  page.fill(value);
  return page;
}

const meta: FileMeta = { size: PAGE_SIZE, mode: 0o100644, ctime: 1000, mtime: 2000 };

/**
 * MemoryBackend wrapper that tracks close() calls and exposes close state.
 */
class ClosableBackend implements StorageBackend {
  private inner = new MemoryBackend();
  closeCount = 0;
  closed = false;

  close(): void {
    this.closeCount++;
    this.closed = true;
  }

  async readPage(path: string, pageIndex: number) {
    return this.inner.readPage(path, pageIndex);
  }
  async readPages(path: string, pageIndices: number[]) {
    return this.inner.readPages(path, pageIndices);
  }
  async readPageBatch(entries: Array<{ path: string; pageIndex: number }>) {
    return this.inner.readPageBatch(entries);
  }
  async writePage(path: string, pageIndex: number, data: Uint8Array) {
    return this.inner.writePage(path, pageIndex, data);
  }
  async writePages(pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>) {
    return this.inner.writePages(pages);
  }
  async deleteFile(path: string) {
    return this.inner.deleteFile(path);
  }
  async deleteFiles(paths: string[]) {
    return this.inner.deleteFiles(paths);
  }
  async deletePagesFrom(path: string, fromPageIndex: number) {
    return this.inner.deletePagesFrom(path, fromPageIndex);
  }
  async renameFile(oldPath: string, newPath: string) {
    return this.inner.renameFile(oldPath, newPath);
  }
  async countPages(path: string) {
    return this.inner.countPages(path);
  }
  async countPagesBatch(paths: string[]) {
    return this.inner.countPagesBatch(paths);
  }
  async maxPageIndex(path: string) {
    return this.inner.maxPageIndex(path);
  }
  async maxPageIndexBatch(paths: string[]) {
    return this.inner.maxPageIndexBatch(paths);
  }
  async readMeta(path: string) {
    return this.inner.readMeta(path);
  }
  async readMetas(paths: string[]) {
    return this.inner.readMetas(paths);
  }
  async writeMeta(path: string, meta: FileMeta) {
    return this.inner.writeMeta(path, meta);
  }
  async writeMetas(entries: Array<{ path: string; meta: FileMeta }>) {
    return this.inner.writeMetas(entries);
  }
  async deleteMeta(path: string) {
    return this.inner.deleteMeta(path);
  }
  async deleteMetas(paths: string[]) {
    return this.inner.deleteMetas(paths);
  }
  async listFiles() {
    return this.inner.listFiles();
  }
  async syncAll(
    pages: Array<{ path: string; pageIndex: number; data: Uint8Array }>,
    metas: Array<{ path: string; meta: FileMeta }>,
  ) {
    return this.inner.syncAll(pages, metas);
  }
  async deleteAll(paths: string[]) {
    return this.inner.deleteAll(paths);
  }
  async cleanupOrphanedPages() {
    return this.inner.cleanupOrphanedPages();
  }
}

describe("PreloadBackend.close()", () => {
  it("flushes dirty state to remote before closing @fast", async () => {
    const remote = new MemoryBackend();
    const backend = new PreloadBackend(remote);
    await backend.init();

    backend.writePage("/file", 0, filledPage(0xaa));
    backend.writeMeta("/file", meta);
    expect(backend.isDirty).toBe(true);

    await backend.close();

    // Dirty state should have been flushed to the remote
    const page = await remote.readPage("/file", 0);
    expect(page).not.toBeNull();
    expect(page![0]).toBe(0xaa);

    const remoteMeta = await remote.readMeta("/file");
    expect(remoteMeta).not.toBeNull();
    expect(remoteMeta!.size).toBe(PAGE_SIZE);
  });

  it("clears all in-memory state @fast", async () => {
    const remote = new MemoryBackend();
    const backend = new PreloadBackend(remote);
    await backend.init();

    backend.writePage("/file", 0, filledPage(0xbb));
    backend.writeMeta("/file", meta);

    await backend.close();

    // After close, operations should throw (not initialized)
    expect(() => backend.readPage("/file", 0)).toThrow(
      "PreloadBackend.init() must be called before use",
    );
  });

  it("closes the underlying remote backend @fast", async () => {
    const remote = new ClosableBackend();
    const backend = new PreloadBackend(remote);
    await backend.init();

    await backend.close();

    expect(remote.closeCount).toBe(1);
    expect(remote.closed).toBe(true);
  });

  it("is idempotent @fast", async () => {
    const remote = new ClosableBackend();
    const backend = new PreloadBackend(remote);
    await backend.init();

    await backend.close();
    await backend.close();
    await backend.close();

    // close() should only call remote.close() once (first call)
    expect(remote.closeCount).toBe(1);
  });

  it("allows re-initialization after close @fast", async () => {
    const remote = new MemoryBackend();
    const backend = new PreloadBackend(remote);
    await backend.init();

    // Write and flush so data is in the remote
    backend.writePage("/file", 0, filledPage(0xcc));
    backend.writeMeta("/file", meta);
    await backend.flush();

    await backend.close();

    // Re-init should reload from remote
    await backend.init();
    const page = backend.readPage("/file", 0);
    expect(page).not.toBeNull();
    expect(page![0]).toBe(0xcc);

    const m = backend.readMeta("/file");
    expect(m).not.toBeNull();
    expect(m!.size).toBe(PAGE_SIZE);
  });

  it("handles close with no dirty state @fast", async () => {
    const remote = new MemoryBackend();
    const backend = new PreloadBackend(remote);
    await backend.init();

    // No writes — close should succeed without flushing
    await backend.close();

    expect(() => backend.readPage("/file", 0)).toThrow(
      "PreloadBackend.init() must be called before use",
    );
  });

  it("flushes pending deletes before closing", async () => {
    const remote = new MemoryBackend();

    // Pre-populate remote with a file
    await remote.writePage("/file", 0, filledPage(0xdd));
    await remote.writeMeta("/file", meta);

    const backend = new PreloadBackend(remote);
    await backend.init();

    // Delete the file locally
    backend.deleteFile("/file");
    backend.deleteMeta("/file");

    await backend.close();

    // Remote should reflect the delete
    const page = await remote.readPage("/file", 0);
    expect(page).toBeNull();
    const m = await remote.readMeta("/file");
    expect(m).toBeNull();
  });

  it("flushes pending truncations before closing", async () => {
    const remote = new MemoryBackend();

    // Pre-populate remote with a multi-page file
    await remote.writePage("/file", 0, filledPage(0x11));
    await remote.writePage("/file", 1, filledPage(0x22));
    await remote.writePage("/file", 2, filledPage(0x33));
    await remote.writeMeta("/file", { ...meta, size: PAGE_SIZE * 3 });

    const backend = new PreloadBackend(remote);
    await backend.init();

    // Truncate to 1 page
    backend.deletePagesFrom("/file", 1);
    backend.writeMeta("/file", { ...meta, size: PAGE_SIZE });

    await backend.close();

    // Remote should have only page 0
    const p0 = await remote.readPage("/file", 0);
    expect(p0).not.toBeNull();
    expect(p0![0]).toBe(0x11);

    const p1 = await remote.readPage("/file", 1);
    expect(p1).toBeNull();
    const p2 = await remote.readPage("/file", 2);
    expect(p2).toBeNull();
  });

  it("works with remote that has no close method @fast", async () => {
    const remote = new MemoryBackend();
    // MemoryBackend doesn't have close() — should not throw
    const backend = new PreloadBackend(remote);
    await backend.init();

    backend.writePage("/file", 0, filledPage(0xee));
    backend.writeMeta("/file", meta);

    await backend.close();

    // Data should still be flushed to remote
    const page = await remote.readPage("/file", 0);
    expect(page).not.toBeNull();
    expect(page![0]).toBe(0xee);
  });

  it("passes invariant checks after close and re-init", async () => {
    const remote = new MemoryBackend();
    const backend = new PreloadBackend(remote);
    await backend.init();

    // Write some data, flush, close, re-init
    backend.writePage("/a", 0, filledPage(0x01));
    backend.writeMeta("/a", meta);
    backend.writePage("/b", 0, filledPage(0x02));
    backend.writePage("/b", 1, filledPage(0x03));
    backend.writeMeta("/b", { ...meta, size: PAGE_SIZE * 2 });
    await backend.flush();

    await backend.close();
    await backend.init();

    // Invariants should hold after re-init
    backend.assertInvariants();

    // Verify all data is accessible
    expect(backend.listFiles()).toEqual(
      expect.arrayContaining(["/a", "/b"]),
    );
    expect(backend.readPage("/a", 0)![0]).toBe(0x01);
    expect(backend.readPage("/b", 0)![0]).toBe(0x02);
    expect(backend.readPage("/b", 1)![0]).toBe(0x03);
  });
});
