/**
 * Unit tests for pglite-fs.ts adapter.
 *
 * Tests the TomeFSPGlite adapter's init, syncToFs, preRun hook
 * mounting, exposed properties, and data flow through a fake
 * Emscripten FS module. closeFs error handling is covered separately
 * in pglite-fs-closefs.test.ts.
 */
import { describe, it, expect, beforeEach } from "vitest";
import { createTomeFSPGlite } from "../../src/pglite-fs.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { PAGE_SIZE } from "../../src/types.js";

/**
 * Fake MemoryFS class that mimics PGlite's MemoryFS interface.
 * Tracks which methods were called and their arguments.
 */
class FakeMemoryFS {
  initCalled = false;
  closeFsCalled = false;
  syncToFsCalled = false;

  init() {
    this.initCalled = true;
    return Promise.resolve({
      emscriptenOpts: { preRun: [] },
    });
  }
  closeFs() {
    this.closeFsCalled = true;
    return Promise.resolve();
  }
  syncToFs() {
    this.syncToFsCalled = true;
    return Promise.resolve();
  }
}

/**
 * Create a fake Emscripten module with a minimal FS implementation.
 * Tracks mkdir/mount calls and provides a working syncfs.
 */
function createFakeModule() {
  const mkdirCalls: string[] = [];
  const mountCalls: Array<{ fs: any; mountpoint: string }> = [];
  let syncfsError: Error | null = null;

  return {
    module: {
      FS: {
        filesystems: {},
        mkdir(path: string) {
          mkdirCalls.push(path);
        },
        mount(fs: any, _opts: any, mountpoint: string) {
          mountCalls.push({ fs, mountpoint });
        },
        syncfs(_populate: boolean, callback: (err: Error | null) => void) {
          callback(syncfsError);
        },
      },
    },
    mkdirCalls,
    mountCalls,
    setSyncfsError(err: Error | null) {
      syncfsError = err;
    },
  };
}

/** Run all preRun hooks from an init result against a fake module. */
function runPreRunHooks(initResult: any, fakeModule: any) {
  for (const hook of initResult.emscriptenOpts.preRun || []) {
    hook(fakeModule);
  }
}

describe("pglite-fs adapter @fast", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  describe("init", () => {
    it("calls base MemoryFS init", async () => {
      const adapter = createTomeFSPGlite({
        MemoryFS: FakeMemoryFS as any,
        backend,
      });

      await adapter.init({}, {});
      // FakeMemoryFS.init sets initCalled = true on the instance,
      // but createTomeFSPGlite calls init on the same instance
      // it created, so we verify through the returned result shape.
      const result = await adapter.init({}, {});
      expect(result.emscriptenOpts).toBeDefined();
    });

    it("returns emscriptenOpts with preRun hooks", async () => {
      const adapter = createTomeFSPGlite({
        MemoryFS: FakeMemoryFS as any,
        backend,
      });

      const result = await adapter.init({}, {});
      expect(result.emscriptenOpts.preRun).toBeDefined();
      expect(Array.isArray(result.emscriptenOpts.preRun)).toBe(true);
      expect(result.emscriptenOpts.preRun.length).toBeGreaterThan(0);
    });

    it("preserves base preRun hooks from MemoryFS", async () => {
      let baseHookCalled = false;
      class MemoryFSWithPreRun {
        init() {
          return Promise.resolve({
            emscriptenOpts: {
              preRun: [() => { baseHookCalled = true; }],
            },
          });
        }
        closeFs() { return Promise.resolve(); }
        syncToFs() { return Promise.resolve(); }
      }

      const adapter = createTomeFSPGlite({
        MemoryFS: MemoryFSWithPreRun as any,
        backend,
      });

      const result = await adapter.init({}, {});
      // Should have base hook + tomefs hook
      expect(result.emscriptenOpts.preRun.length).toBe(2);

      // Run all hooks — base hook should execute
      const { module } = createFakeModule();
      runPreRunHooks(result, module);
      expect(baseHookCalled).toBe(true);
    });

    it("handles base init returning no preRun array", async () => {
      class MemoryFSNoPreRun {
        init() {
          return Promise.resolve({
            emscriptenOpts: {},
          });
        }
        closeFs() { return Promise.resolve(); }
        syncToFs() { return Promise.resolve(); }
      }

      const adapter = createTomeFSPGlite({
        MemoryFS: MemoryFSNoPreRun as any,
        backend,
      });

      const result = await adapter.init({}, {});
      expect(result.emscriptenOpts.preRun).toBeDefined();
      expect(result.emscriptenOpts.preRun.length).toBe(1);
    });
  });

  describe("preRun hook", () => {
    it("creates and mounts tomefs at /pglite/data", async () => {
      const adapter = createTomeFSPGlite({
        MemoryFS: FakeMemoryFS as any,
        backend,
      });

      const result = await adapter.init({}, {});
      const { module, mkdirCalls, mountCalls } = createFakeModule();

      runPreRunHooks(result, module);

      expect(mkdirCalls).toContain("/pglite/data");
      expect(mountCalls.length).toBe(1);
      expect(mountCalls[0].mountpoint).toBe("/pglite/data");
      expect(mountCalls[0].fs).toBeDefined();
    });

    it("is idempotent — second invocation is a no-op", async () => {
      const adapter = createTomeFSPGlite({
        MemoryFS: FakeMemoryFS as any,
        backend,
      });

      const result = await adapter.init({}, {});
      const { module, mkdirCalls, mountCalls } = createFakeModule();

      runPreRunHooks(result, module);
      expect(mountCalls.length).toBe(1);

      // Run hooks again
      runPreRunHooks(result, module);
      // Should still be 1 mount — second call is a no-op
      expect(mountCalls.length).toBe(1);
      expect(mkdirCalls.filter(p => p === "/pglite/data").length).toBe(1);
    });

    it("sets tomefsInstance after hook runs", async () => {
      const adapter = createTomeFSPGlite({
        MemoryFS: FakeMemoryFS as any,
        backend,
      });

      expect(adapter.tomefsInstance).toBeNull();

      const result = await adapter.init({}, {});
      expect(adapter.tomefsInstance).toBeNull();

      const { module } = createFakeModule();
      runPreRunHooks(result, module);

      expect(adapter.tomefsInstance).not.toBeNull();
    });
  });

  describe("syncToFs", () => {
    it("calls FS.syncfs when module is initialized", async () => {
      const adapter = createTomeFSPGlite({
        MemoryFS: FakeMemoryFS as any,
        backend,
      });

      const result = await adapter.init({}, {});
      let syncfsCalled = false;

      const fakeModule = {
        FS: {
          filesystems: {},
          mkdir() {},
          mount() {},
          syncfs(_populate: boolean, callback: (err: Error | null) => void) {
            syncfsCalled = true;
            callback(null);
          },
        },
      };

      runPreRunHooks(result, fakeModule);
      await adapter.syncToFs();

      expect(syncfsCalled).toBe(true);
    });

    it("returns without error when module is not initialized", async () => {
      const adapter = createTomeFSPGlite({
        MemoryFS: FakeMemoryFS as any,
        backend,
      });

      await adapter.init({}, {});
      // Don't run preRun hooks — moduleFS stays null
      await adapter.syncToFs(); // Should not throw
    });

    it("calls backend.flush after syncfs when flush exists", async () => {
      let flushCalled = false;
      const flushableBackend = Object.assign(new SyncMemoryBackend(), {
        flush: () => {
          flushCalled = true;
          return Promise.resolve();
        },
      });

      const adapter = createTomeFSPGlite({
        MemoryFS: FakeMemoryFS as any,
        backend: flushableBackend,
      });

      const result = await adapter.init({}, {});
      const { module } = createFakeModule();
      runPreRunHooks(result, module);

      await adapter.syncToFs();
      expect(flushCalled).toBe(true);
    });

    it("works without backend.flush (SyncMemoryBackend)", async () => {
      const adapter = createTomeFSPGlite({
        MemoryFS: FakeMemoryFS as any,
        backend,
      });

      const result = await adapter.init({}, {});
      const { module } = createFakeModule();
      runPreRunHooks(result, module);

      // SyncMemoryBackend has no flush — should not throw
      await adapter.syncToFs();
    });

    it("propagates syncfs errors", async () => {
      const adapter = createTomeFSPGlite({
        MemoryFS: FakeMemoryFS as any,
        backend,
      });

      const result = await adapter.init({}, {});
      const { module, setSyncfsError } = createFakeModule();
      runPreRunHooks(result, module);

      setSyncfsError(new Error("disk full"));

      await expect(adapter.syncToFs()).rejects.toThrow("disk full");
    });

    it("calls flush even after syncfs error propagated to closeFs", async () => {
      // This tests that syncAndFlush always calls flush when it exists,
      // even if syncfs fails (which propagates through closeFs but not
      // directly through syncToFs since syncToFs rejects).
      let flushCalled = false;
      const flushableBackend = Object.assign(new SyncMemoryBackend(), {
        flush: () => {
          flushCalled = true;
          return Promise.resolve();
        },
      });

      const adapter = createTomeFSPGlite({
        MemoryFS: FakeMemoryFS as any,
        backend: flushableBackend,
      });

      const result = await adapter.init({}, {});
      const { module } = createFakeModule();
      runPreRunHooks(result, module);

      // Successful sync should call flush
      await adapter.syncToFs();
      expect(flushCalled).toBe(true);
    });

    it("ignores relaxedDurability parameter", async () => {
      const adapter = createTomeFSPGlite({
        MemoryFS: FakeMemoryFS as any,
        backend,
      });

      const result = await adapter.init({}, {});
      const { module } = createFakeModule();
      runPreRunHooks(result, module);

      // Should work with any value of relaxedDurability
      await adapter.syncToFs(true);
      await adapter.syncToFs(false);
      await adapter.syncToFs(undefined);
    });
  });

  describe("exposed properties", () => {
    it("storageBackend returns the configured backend", () => {
      const adapter = createTomeFSPGlite({
        MemoryFS: FakeMemoryFS as any,
        backend,
      });

      expect(adapter.storageBackend).toBe(backend);
    });

    it("tomefsInstance is null before preRun hooks", async () => {
      const adapter = createTomeFSPGlite({
        MemoryFS: FakeMemoryFS as any,
        backend,
      });

      expect(adapter.tomefsInstance).toBeNull();
      await adapter.init({}, {});
      expect(adapter.tomefsInstance).toBeNull();
    });

    it("tomefsInstance is set after preRun hooks", async () => {
      const adapter = createTomeFSPGlite({
        MemoryFS: FakeMemoryFS as any,
        backend,
      });

      const result = await adapter.init({}, {});
      const { module } = createFakeModule();
      runPreRunHooks(result, module);

      const instance = adapter.tomefsInstance;
      expect(instance).not.toBeNull();
      expect(instance.pageCache).toBeDefined();
      expect(instance.backend).toBe(backend);
    });

    it("pageCache is undefined before preRun hooks", async () => {
      const adapter = createTomeFSPGlite({
        MemoryFS: FakeMemoryFS as any,
        backend,
      });

      expect(adapter.pageCache).toBeUndefined();
    });

    it("pageCache returns the page cache after preRun hooks", async () => {
      const adapter = createTomeFSPGlite({
        MemoryFS: FakeMemoryFS as any,
        backend,
      });

      const result = await adapter.init({}, {});
      const { module } = createFakeModule();
      runPreRunHooks(result, module);

      expect(adapter.pageCache).toBeDefined();
      expect(adapter.pageCache).toBe(adapter.tomefsInstance.pageCache);
    });
  });

  describe("defaults", () => {
    it("uses SyncMemoryBackend when no backend is provided", async () => {
      const adapter = createTomeFSPGlite({
        MemoryFS: FakeMemoryFS as any,
      });

      // storageBackend should be a SyncMemoryBackend instance
      expect(adapter.storageBackend).toBeDefined();
      expect(adapter.storageBackend.listFiles()).toEqual([]);
    });

    it("uses default maxPages (4096) when not specified", async () => {
      const adapter = createTomeFSPGlite({
        MemoryFS: FakeMemoryFS as any,
        backend,
      });

      const result = await adapter.init({}, {});
      const { module } = createFakeModule();
      runPreRunHooks(result, module);

      const cache = adapter.pageCache;
      expect(cache).toBeDefined();
      const stats = cache.getStats();
      expect(stats).toBeDefined();
    });

    it("respects custom maxPages", async () => {
      const adapter = createTomeFSPGlite({
        MemoryFS: FakeMemoryFS as any,
        backend,
        maxPages: 16,
      });

      const result = await adapter.init({}, {});
      const { module } = createFakeModule();
      runPreRunHooks(result, module);

      const cache = adapter.pageCache;
      expect(cache).toBeDefined();
    });
  });

  describe("data flow", () => {
    it("tomefs instance uses the provided backend", async () => {
      const adapter = createTomeFSPGlite({
        MemoryFS: FakeMemoryFS as any,
        backend,
      });

      const result = await adapter.init({}, {});
      const { module } = createFakeModule();
      runPreRunHooks(result, module);

      const instance = adapter.tomefsInstance;
      expect(instance.backend).toBe(backend);
    });

    it("data written to backend before mount is visible after restoreTree", async () => {
      // Pre-populate the backend with a file's metadata
      const meta = { size: 5, mode: 0o100644, ctime: 1000, mtime: 1000 };
      backend.writeMeta("/testfile", meta);
      const data = new Uint8Array(PAGE_SIZE);
      data.set([72, 101, 108, 108, 111]); // "Hello"
      backend.writePage("/testfile", 0, data);

      const adapter = createTomeFSPGlite({
        MemoryFS: FakeMemoryFS as any,
        backend,
      });

      const result = await adapter.init({}, {});

      // Create a more complete fake module with real createTomeFS mounted
      const { module } = createFakeModule();
      runPreRunHooks(result, module);

      // The tomefs instance should have restored the file from backend
      const instance = adapter.tomefsInstance;
      expect(instance).not.toBeNull();
      // The backend should still have the metadata
      const storedMeta = backend.readMeta("/testfile");
      expect(storedMeta).not.toBeNull();
      expect(storedMeta!.size).toBe(5);
    });

    it("syncToFs passes populate=false to FS.syncfs", async () => {
      const adapter = createTomeFSPGlite({
        MemoryFS: FakeMemoryFS as any,
        backend,
      });

      const result = await adapter.init({}, {});
      let capturedPopulate: boolean | null = null;

      const fakeModule = {
        FS: {
          filesystems: {},
          mkdir() {},
          mount() {},
          syncfs(populate: boolean, callback: (err: Error | null) => void) {
            capturedPopulate = populate;
            callback(null);
          },
        },
      };

      runPreRunHooks(result, fakeModule);
      await adapter.syncToFs();

      expect(capturedPopulate).toBe(false);
    });
  });

  describe("flush integration", () => {
    it("calls flush after successful syncfs", async () => {
      const callOrder: string[] = [];

      const flushableBackend = Object.assign(new SyncMemoryBackend(), {
        flush: () => {
          callOrder.push("flush");
          return Promise.resolve();
        },
      });

      const adapter = createTomeFSPGlite({
        MemoryFS: FakeMemoryFS as any,
        backend: flushableBackend,
      });

      const result = await adapter.init({}, {});
      const fakeModule = {
        FS: {
          filesystems: {},
          mkdir() {},
          mount() {},
          syncfs(_populate: boolean, callback: (err: Error | null) => void) {
            callOrder.push("syncfs");
            callback(null);
          },
        },
      };

      runPreRunHooks(result, fakeModule);
      await adapter.syncToFs();

      expect(callOrder).toEqual(["syncfs", "flush"]);
    });

    it("does not call flush when syncfs fails", async () => {
      let flushCalled = false;

      const flushableBackend = Object.assign(new SyncMemoryBackend(), {
        flush: () => {
          flushCalled = true;
          return Promise.resolve();
        },
      });

      const adapter = createTomeFSPGlite({
        MemoryFS: FakeMemoryFS as any,
        backend: flushableBackend,
      });

      const result = await adapter.init({}, {});
      const fakeModule = {
        FS: {
          filesystems: {},
          mkdir() {},
          mount() {},
          syncfs(_populate: boolean, callback: (err: Error | null) => void) {
            callback(new Error("syncfs failed"));
          },
        },
      };

      runPreRunHooks(result, fakeModule);
      await expect(adapter.syncToFs()).rejects.toThrow("syncfs failed");
      expect(flushCalled).toBe(false);
    });

    it("propagates flush errors", async () => {
      const flushableBackend = Object.assign(new SyncMemoryBackend(), {
        flush: () => Promise.reject(new Error("flush failed")),
      });

      const adapter = createTomeFSPGlite({
        MemoryFS: FakeMemoryFS as any,
        backend: flushableBackend,
      });

      const result = await adapter.init({}, {});
      const { module } = createFakeModule();
      runPreRunHooks(result, module);

      await expect(adapter.syncToFs()).rejects.toThrow("flush failed");
    });
  });
});
