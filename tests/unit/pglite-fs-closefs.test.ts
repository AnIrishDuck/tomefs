/**
 * Unit tests for pglite-fs.ts closeFs error handling.
 *
 * Verifies that closeFs always calls the base MemoryFS closeFs even
 * when syncfs fails (e.g., IDB quota exceeded). Before the fix,
 * a syncfs failure caused closeFs to reject without calling the base
 * cleanup, leaking resources.
 */
import { describe, it, expect } from "vitest";
import { createTomeFSPGlite } from "../../src/pglite-fs.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";

describe("pglite-fs closeFs error handling @fast", () => {
  it("closeFs calls base cleanup even when syncfs fails", async () => {
    // Create a fake MemoryFS class that tracks closeFs calls
    let baseCloseFsCalled = false;
    class FakeMemoryFS {
      init() {
        return Promise.resolve({
          emscriptenOpts: { preRun: [] },
        });
      }
      closeFs() {
        baseCloseFsCalled = true;
        return Promise.resolve();
      }
      syncToFs() {
        return Promise.resolve();
      }
    }

    const backend = new SyncMemoryBackend();
    const adapter = createTomeFSPGlite({
      MemoryFS: FakeMemoryFS as any,
      backend,
      maxPages: 64,
    });

    // Initialize the adapter (this calls FakeMemoryFS.init + stores preRun hooks)
    const initResult = await adapter.init({}, {});

    // Simulate Emscripten module init — run preRun hooks to set moduleFS.
    // Create a fake Emscripten module with a FS that has syncfs that always errors.
    const fakeModule = {
      FS: {
        filesystems: {},
        mkdir: () => {},
        mount: () => {},
        syncfs: (_populate: boolean, callback: (err: Error | null) => void) => {
          callback(new Error("simulated IDB quota exceeded"));
        },
      },
    };

    // Run the preRun hooks (this sets moduleFS inside the adapter closure)
    for (const hook of initResult.emscriptenOpts.preRun) {
      hook(fakeModule);
    }

    // Now closeFs should:
    // 1. Call syncfs → error via callback
    // 2. Still call base closeFs
    // 3. Re-throw the sync error
    let caught: Error | null = null;
    try {
      await adapter.closeFs();
    } catch (e) {
      caught = e as Error;
    }

    expect(caught).not.toBeNull();
    expect(caught!.message).toBe("simulated IDB quota exceeded");
    expect(baseCloseFsCalled).toBe(true);
  });

  it("closeFs succeeds normally when syncfs succeeds @fast", async () => {
    let baseCloseFsCalled = false;
    class FakeMemoryFS {
      init() {
        return Promise.resolve({
          emscriptenOpts: { preRun: [] },
        });
      }
      closeFs() {
        baseCloseFsCalled = true;
        return Promise.resolve();
      }
      syncToFs() {
        return Promise.resolve();
      }
    }

    const backend = new SyncMemoryBackend();
    const adapter = createTomeFSPGlite({
      MemoryFS: FakeMemoryFS as any,
      backend,
      maxPages: 64,
    });

    const initResult = await adapter.init({}, {});

    const fakeModule = {
      FS: {
        filesystems: {},
        mkdir: () => {},
        mount: () => {},
        syncfs: (_populate: boolean, callback: (err: Error | null) => void) => {
          callback(null);
        },
      },
    };

    for (const hook of initResult.emscriptenOpts.preRun) {
      hook(fakeModule);
    }

    await adapter.closeFs();

    expect(baseCloseFsCalled).toBe(true);
  });

  it("closeFs without module init skips syncfs and calls base @fast", async () => {
    let baseCloseFsCalled = false;
    class FakeMemoryFS {
      init() {
        return Promise.resolve({
          emscriptenOpts: { preRun: [] },
        });
      }
      closeFs() {
        baseCloseFsCalled = true;
        return Promise.resolve();
      }
      syncToFs() {
        return Promise.resolve();
      }
    }

    const backend = new SyncMemoryBackend();
    const adapter = createTomeFSPGlite({
      MemoryFS: FakeMemoryFS as any,
      backend,
      maxPages: 64,
    });

    // Don't run preRun hooks — moduleFS stays null
    await adapter.init({}, {});
    await adapter.closeFs();

    expect(baseCloseFsCalled).toBe(true);
  });
});
