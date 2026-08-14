/**
 * Barrel export validation tests.
 *
 * Verifies that the three package entry points (index, worker, pglite)
 * correctly re-export their public APIs. If a re-export is accidentally
 * removed or renamed, these tests catch it before consumers hit a
 * missing-export error at import time.
 */
import { describe, it, expect } from "vitest";

describe("main entry point (src/index.ts) @fast", () => {
  it("exports all public classes and functions", async () => {
    const mod = await import("../../src/index.js");
    expect(mod.PageCache).toBeDefined();
    expect(mod.MemoryBackend).toBeDefined();
    expect(mod.IdbBackend).toBeDefined();
    expect(mod.OpfsBackend).toBeDefined();
    expect(mod.OpfsSahBackend).toBeDefined();
    expect(mod.SyncPageCache).toBeDefined();
    expect(mod.SyncMemoryBackend).toBeDefined();
    expect(mod.createTomeFS).toBeDefined();
    expect(mod.PreloadBackend).toBeDefined();
    expect(mod.SabClient).toBeDefined();
    expect(mod.SabWorker).toBeDefined();
    expect(mod.opcodeName).toBeDefined();
  });

  it("exports all public constants", async () => {
    const mod = await import("../../src/index.js");
    expect(mod.PAGE_SIZE).toBe(8192);
    expect(mod.DEFAULT_MAX_PAGES).toBe(4096);
    expect(typeof mod.pageKeyStr).toBe("function");
  });
});

describe("worker entry point (src/worker.ts) @fast", () => {
  it("exports all worker-side classes", async () => {
    const mod = await import("../../src/worker.js");
    expect(mod.SabWorker).toBeDefined();
    expect(mod.IdbBackend).toBeDefined();
    expect(mod.OpfsBackend).toBeDefined();
    expect(mod.OpfsSahBackend).toBeDefined();
  });

  it("exports PAGE_SIZE constant", async () => {
    const mod = await import("../../src/worker.js");
    expect(mod.PAGE_SIZE).toBe(8192);
  });
});

describe("pglite entry point (src/pglite.ts) @fast", () => {
  it("exports createTomeFSPGlite", async () => {
    const mod = await import("../../src/pglite.js");
    expect(typeof mod.createTomeFSPGlite).toBe("function");
  });
});
