/**
 * Unit tests for the PGlite filesystem adapter (pglite-fs.ts).
 *
 * Tests the createTomeFSPGlite adapter's lifecycle, preRun idempotency,
 * syncToFs/closeFs behavior, and exposed internals — without requiring
 * a real PGlite instance. Uses a fake MemoryFS that simulates PGlite's
 * Filesystem contract.
 */

import { describe, it, expect, beforeEach } from "vitest";
import { createTomeFSPGlite } from "../../src/pglite-fs.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";

// ---------------------------------------------------------------------------
// Fake MemoryFS — simulates PGlite's MemoryFS contract
// ---------------------------------------------------------------------------

/**
 * Tracks calls made during the MemoryFS / Emscripten lifecycle so tests
 * can assert on ordering, arguments, and idempotency.
 */
interface FakeCall {
  method: string;
  args?: any[];
}

function createFakeMemoryFS() {
  const calls: FakeCall[] = [];
  let syncfsCallback: ((err: Error | null) => void) | null = null;
  let syncfsError: Error | null = null;

  /** Simulated Emscripten FS object passed to preRun hooks via mod.FS. */
  const fakeEmscriptenFS = {
    mkdir(path: string) {
      calls.push({ method: "FS.mkdir", args: [path] });
    },
    mount(fs: any, _opts: any, mountPoint: string) {
      calls.push({ method: "FS.mount", args: [mountPoint] });
    },
    syncfs(populate: boolean, cb: (err: Error | null) => void) {
      calls.push({ method: "FS.syncfs", args: [populate] });
      if (syncfsError) {
        cb(syncfsError);
      } else {
        cb(null);
      }
    },
    filesystems: {} as Record<string, any>,
  };

  /** Simulated Emscripten Module passed to preRun hooks. */
  const fakeMod = { FS: fakeEmscriptenFS };

  class FakeMemoryFS {
    closeFsCalled = false;

    async init(_pg: any, _emscriptenOptions: any) {
      calls.push({ method: "init" });
      return {
        emscriptenOpts: {
          preRun: [],
        },
      };
    }

    async closeFs() {
      calls.push({ method: "closeFs" });
      this.closeFsCalled = true;
    }
  }

  return {
    FakeMemoryFS,
    calls,
    fakeMod,
    fakeEmscriptenFS,
    setSyncfsError(err: Error | null) {
      syncfsError = err;
    },
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("createTomeFSPGlite @fast", () => {
  let fake: ReturnType<typeof createFakeMemoryFS>;
  let adapter: any;
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    fake = createFakeMemoryFS();
    backend = new SyncMemoryBackend();
    adapter = createTomeFSPGlite({
      MemoryFS: fake.FakeMemoryFS as any,
      backend,
      maxPages: 16,
    });
  });

  describe("construction", () => {
    it("returns an adapter with init, syncToFs, and closeFs methods", () => {
      expect(typeof adapter.init).toBe("function");
      expect(typeof adapter.syncToFs).toBe("function");
      expect(typeof adapter.closeFs).toBe("function");
    });

    it("uses the provided backend", () => {
      expect(adapter.storageBackend).toBe(backend);
    });

    it("defaults to SyncMemoryBackend when no backend is provided", () => {
      const defaultAdapter = createTomeFSPGlite({
        MemoryFS: fake.FakeMemoryFS as any,
      });
      expect(defaultAdapter.storageBackend).toBeDefined();
    });

    it("has null tomefsInstance before init", () => {
      expect(adapter.tomefsInstance).toBeNull();
    });

    it("has null pageCache before init", () => {
      expect(adapter.pageCache).toBeUndefined();
    });
  });

  describe("init lifecycle", () => {
    it("calls parent init and returns emscriptenOpts with preRun hook", async () => {
      const result = await adapter.init({}, {});
      expect(result.emscriptenOpts.preRun).toBeDefined();
      expect(result.emscriptenOpts.preRun.length).toBeGreaterThan(0);
      expect(fake.calls.some((c) => c.method === "init")).toBe(true);
    });

    it("preRun hook creates tomefs, mkdirs, and mounts", async () => {
      const result = await adapter.init({}, {});
      const hook = result.emscriptenOpts.preRun[result.emscriptenOpts.preRun.length - 1];
      hook(fake.fakeMod);

      expect(fake.calls.some((c) => c.method === "FS.mkdir" && c.args![0] === "/pglite/data")).toBe(true);
      expect(fake.calls.some((c) => c.method === "FS.mount" && c.args![0] === "/pglite/data")).toBe(true);
    });

    it("preRun hook is idempotent — second call is a no-op", async () => {
      const result = await adapter.init({}, {});
      const hook = result.emscriptenOpts.preRun[result.emscriptenOpts.preRun.length - 1];

      hook(fake.fakeMod);
      const callsAfterFirst = fake.calls.length;

      hook(fake.fakeMod);
      // No new FS.mkdir or FS.mount calls
      expect(fake.calls.length).toBe(callsAfterFirst);
    });

    it("exposes tomefsInstance after preRun executes", async () => {
      const result = await adapter.init({}, {});
      const hook = result.emscriptenOpts.preRun[result.emscriptenOpts.preRun.length - 1];
      hook(fake.fakeMod);

      expect(adapter.tomefsInstance).toBeDefined();
      expect(adapter.tomefsInstance).not.toBeNull();
    });

    it("exposes pageCache after preRun executes", async () => {
      const result = await adapter.init({}, {});
      const hook = result.emscriptenOpts.preRun[result.emscriptenOpts.preRun.length - 1];
      hook(fake.fakeMod);

      expect(adapter.pageCache).toBeDefined();
    });

    it("preserves parent preRun hooks", async () => {
      // Override FakeMemoryFS to return existing preRun hooks
      const existingHook = () => {};
      const CustomFS = class {
        async init() {
          return {
            emscriptenOpts: {
              preRun: [existingHook],
            },
          };
        }
        async closeFs() {}
      };

      const customAdapter = createTomeFSPGlite({
        MemoryFS: CustomFS as any,
        backend,
      });

      const result = await customAdapter.init({}, {});
      expect(result.emscriptenOpts.preRun[0]).toBe(existingHook);
      expect(result.emscriptenOpts.preRun.length).toBe(2);
    });

    it("handles parent init returning no preRun array", async () => {
      const CustomFS = class {
        async init() {
          return {
            emscriptenOpts: {},
          };
        }
        async closeFs() {}
      };

      const customAdapter = createTomeFSPGlite({
        MemoryFS: CustomFS as any,
        backend,
      });

      const result = await customAdapter.init({}, {});
      // Should still have the tomefs preRun hook
      expect(result.emscriptenOpts.preRun.length).toBe(1);
    });
  });

  describe("syncToFs", () => {
    it("is a no-op before preRun executes (no moduleFS)", async () => {
      // syncToFs before init — should not throw
      await expect(adapter.syncToFs()).resolves.toBeUndefined();
    });

    it("calls FS.syncfs after preRun has executed", async () => {
      const result = await adapter.init({}, {});
      const hook = result.emscriptenOpts.preRun[result.emscriptenOpts.preRun.length - 1];
      hook(fake.fakeMod);

      await adapter.syncToFs();
      expect(fake.calls.some((c) => c.method === "FS.syncfs" && c.args![0] === false)).toBe(true);
    });

    it("propagates FS.syncfs errors as rejections", async () => {
      const result = await adapter.init({}, {});
      const hook = result.emscriptenOpts.preRun[result.emscriptenOpts.preRun.length - 1];
      hook(fake.fakeMod);

      fake.setSyncfsError(new Error("sync failed"));
      await expect(adapter.syncToFs()).rejects.toThrow("sync failed");
    });
  });

  describe("closeFs", () => {
    it("flushes via syncfs then calls parent closeFs", async () => {
      const result = await adapter.init({}, {});
      const hook = result.emscriptenOpts.preRun[result.emscriptenOpts.preRun.length - 1];
      hook(fake.fakeMod);

      await adapter.closeFs();

      // Verify ordering: syncfs must come before closeFs
      const syncIdx = fake.calls.findIndex((c) => c.method === "FS.syncfs");
      const closeIdx = fake.calls.findIndex((c) => c.method === "closeFs");
      expect(syncIdx).toBeGreaterThan(-1);
      expect(closeIdx).toBeGreaterThan(-1);
      expect(syncIdx).toBeLessThan(closeIdx);
    });

    it("calls parent closeFs even when moduleFS is null (before mount)", async () => {
      // closeFs before init — should still call parent
      await adapter.closeFs();
      expect(fake.calls.some((c) => c.method === "closeFs")).toBe(true);
    });

    it("propagates syncfs errors during close", async () => {
      const result = await adapter.init({}, {});
      const hook = result.emscriptenOpts.preRun[result.emscriptenOpts.preRun.length - 1];
      hook(fake.fakeMod);

      fake.setSyncfsError(new Error("close sync failed"));
      await expect(adapter.closeFs()).rejects.toThrow("close sync failed");
    });
  });

  describe("exposed properties", () => {
    it("storageBackend returns the backend passed at construction", () => {
      expect(adapter.storageBackend).toBe(backend);
    });

    it("tomefsInstance transitions from null to defined after preRun", async () => {
      expect(adapter.tomefsInstance).toBeNull();

      const result = await adapter.init({}, {});
      const hook = result.emscriptenOpts.preRun[result.emscriptenOpts.preRun.length - 1];

      // Still null before hook runs
      expect(adapter.tomefsInstance).toBeNull();

      hook(fake.fakeMod);
      expect(adapter.tomefsInstance).not.toBeNull();
    });

    it("pageCache is accessible and has expected methods after mount", async () => {
      const result = await adapter.init({}, {});
      const hook = result.emscriptenOpts.preRun[result.emscriptenOpts.preRun.length - 1];
      hook(fake.fakeMod);

      const cache = adapter.pageCache;
      expect(cache).toBeDefined();
      expect(typeof cache.getPage).toBe("function");
      expect(typeof cache.read).toBe("function");
      expect(typeof cache.write).toBe("function");
      expect(typeof cache.flushFile).toBe("function");
    });
  });

  describe("backend integration", () => {
    it("page cache writes are visible through getPage", async () => {
      const result = await adapter.init({}, {});
      const hook = result.emscriptenOpts.preRun[result.emscriptenOpts.preRun.length - 1];
      hook(fake.fakeMod);

      const pageCache = adapter.pageCache;

      // Write through the page cache using the write method
      const testData = new Uint8Array(100).fill(0x42);
      pageCache.write("test-file", testData, 0, 100, 0);

      // Verify page is in cache via getPage
      const cached = pageCache.getPage("test-file", 0);
      expect(cached).toBeDefined();
      expect(cached.data[0]).toBe(0x42);
    });

    it("respects maxPages option — evicted dirty pages flush to backend", async () => {
      const result = await adapter.init({}, {});
      const hook = result.emscriptenOpts.preRun[result.emscriptenOpts.preRun.length - 1];
      hook(fake.fakeMod);

      const cache = adapter.pageCache;
      // The cache was created with maxPages=16
      // Write 20 pages to force eviction
      for (let i = 0; i < 20; i++) {
        const data = new Uint8Array(8192).fill(i);
        cache.write("file", data, 0, 8192, i * 8192);
      }

      // Recent pages should be in cache
      const recent = cache.getPage("file", 19);
      expect(recent).toBeDefined();

      // Evicted dirty pages should have been flushed to the backend
      const evicted = backend.readPage("file", 0);
      expect(evicted).not.toBeNull();
      expect(evicted![0]).toBe(0);
    });
  });
});
