# Fix: PGlite PreloadBackend WASM Memory Access Out of Bounds

## Problem

PR #379 ("Add close() to PreloadBackend and StorageBackend interfaces") fails CI on the "Full test suite (tomefs)" job (Node 22) with:

```
FAIL tests/pglite/preload-backend.test.ts > PGlite + PreloadBackend auto-flush (ethos §10) > syncToFs alone persists data to remote backend @fast

RuntimeError: memory access out of bounds
  at null.<anonymous> wasm:/wasm/000a5fc6:1:59858
  ...
  at Object.callMain node_modules/@electric-sql/pglite/release/initdb.js:9:107273
```

The crash occurs during PGlite's `initdb` WASM initialization — not during any close() logic.

## Root Cause Analysis

**Not a logic bug in the PR.** The PR:
- Adds `close()` to `PreloadBackend` (src/preload-backend.ts)
- Adds optional `close?()` to `StorageBackend` and `SyncStorageBackend` interfaces
- Adds `tests/unit/preload-backend-close.test.ts` (10 new tests)
- Does NOT modify `pglite-fs.ts` or the failing test

**Evidence this is a flaky WASM memory issue:**
1. The failing test (`syncToFs alone persists data to remote backend`) is unchanged between main and the PR
2. All 5 recent CI runs on `main` pass (30/30 jobs success)
3. The crash is in PGlite's `initdb.js` WASM code, not in tomefs code
4. The crash type (`memory access out of bounds`) is a known class of WASM OOM failure
5. The same test passes in the MEMFS step of the same CI run
6. Local test run on main (this session) passes 978+ tests with zero failures
7. The CI runner's vitest process for the full tomefs suite consumes ~2.9GB RAM; additional test files push memory higher

**Likely trigger:** The PR adds 308 lines of new tests that create/init/close PreloadBackend instances. When vitest runs all tests in a single process, accumulated WASM memory from earlier PGlite test instances (which each allocate WASM linear memory for PostgreSQL) may push total memory past a threshold. PGlite's `initdb` allocates a new WASM memory instance that fails to get the needed contiguous address space.

## Fix Options

### Option A: Isolate PGlite tests with `vitest.config` pooling (Recommended)

Configure the PGlite test files to run in isolated worker threads so each PGlite instance gets its own memory space. In `vitest.config.ts`:

```typescript
test: {
  poolOptions: {
    forks: {
      // PGlite tests get isolated processes to avoid WASM memory pressure
      execArgv: ['--max-old-space-size=4096'],
    }
  },
  // Or use fileParallelism + isolate per-file
  isolate: true,
}
```

Or add a `vitest.workspace.ts` entry that runs `tests/pglite/**` in forked mode while other tests use the default thread pool.

### Option B: Add retry to the flaky test

Mark the test with `retry(2)` since the WASM OOM is non-deterministic:

```typescript
it("syncToFs alone persists data to remote backend @fast", { retry: 2 }, async () => {
```

This is a band-aid — the underlying memory pressure remains.

### Option C: Force garbage collection between PGlite test files

Add a setup file for `tests/pglite/` that calls `global.gc?.()` between test files (requires `--expose-gc` Node flag):

```typescript
// tests/pglite/setup.ts
afterAll(() => { global.gc?.(); });
```

And in `vitest.config.ts`:
```typescript
test: {
  setupFiles: ['tests/pglite/setup.ts'],
}
```

### Option D: Split PGlite tests into separate vitest invocations in CI

Update `.github/workflows/ci.yml` to run PGlite tests in a separate step:

```yaml
- name: Full test suite (tomefs) — non-PGlite
  run: TOMEFS_BACKEND=tomefs npx vitest run --exclude 'tests/pglite/**'

- name: Full test suite (tomefs) — PGlite
  run: TOMEFS_BACKEND=tomefs npx vitest run tests/pglite/
```

## Recommendation

**Option A** is the cleanest fix — it addresses the root cause (WASM memory accumulation in a single process) without changing test logic or CI structure. The PGlite tests are already slower by nature (~3-4s each due to PostgreSQL initialization), so the overhead of process isolation is negligible relative to their runtime.

If Option A proves complex to configure, **Option D** is the simplest pragmatic fix with the same effect (separate processes for PGlite tests).

## Verification

1. Run `TOMEFS_BACKEND=tomefs npm test` on the PR branch 3-5 times to confirm the fix eliminates the flaky failure
2. Ensure memory usage during the PGlite test phase stays bounded (< 2GB per isolated process)
3. Confirm CI passes consistently on re-run

## Additional Context

- The test that fails creates TWO PGlite instances (session 1 and session 2 within a single `it()` block), doubling the WASM memory pressure within that test
- The crash happens during session 2's `initdb` — the first PGlite instance's WASM memory may still be live when the second allocates
- PGlite's WASM linear memory is non-shrinkable (WebAssembly limitation) — once allocated, it stays until the process exits
