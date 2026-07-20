# Flaky PGlite Timeout: jsonb-stress.test.ts

## Observed Failure

**Test:** `tests/pglite/jsonb-stress.test.ts` > "GIN index on JSONB with containment queries" > `cache=medium (64 pages) @fast`

**Error:** `Test timed out in 30000ms.`

**When:** During the full `npm test` run, which executes 195 test files
(4774 tests) concurrently via vitest's thread pool.

## Root Cause: Resource Contention, Not a Code Bug

The test passes reliably in isolation (~2.5s) and passes when all 40
scenarios in the same file run sequentially (~97s total). The timeout
only occurred during the full suite run because:

1. **Memory pressure.** The full suite runs 195 test files across
   multiple threads. Each PGlite test spins up a WASM Postgres instance
   (~200-300 MB RSS). With thread pooling, several PGlite instances can
   be active simultaneously, pushing total memory past 2 GB.

2. **CPU saturation.** The vitest process sustains 260-280% CPU during
   PGlite stress tests. When multiple PGlite files run in parallel
   threads, GIN index operations (which do heavy page I/O through
   tomefs) compete for CPU, inflating wall-clock time past the 30s
   timeout.

3. **GC pressure.** WASM Postgres allocates and frees large buffers
   during GIN index scans. Under memory pressure, V8 GC pauses
   contribute to the slowdown.

The `medium` cache size (64 pages) was the specific victim because of
scheduling — it happened to land in a window when other heavy PGlite
tests were also running. `tiny` and `small` passed because they ran in
an earlier, less contested window.

## Evidence

- The same test passes 100% in isolation (re-ran full file: 40/40 pass).
- CI (GitHub Actions) passes consistently — last 10 runs all green. CI
  runs on `ubuntu-latest` with more resources (7 GB RAM, 2 cores
  dedicated).
- All other PGlite stress test files pass (23/24 files, 750+ PGlite
  tests), confirming no regression in the filesystem layer.
- Non-PGlite tests: 3891/3891 pass across all three backends (default,
  tomefs, preload) including 4-page-cache stress mode.

## Fix Options

### Option A: Increase timeout for PGlite tests (recommended)

Add a longer timeout for PGlite stress tests to accommodate resource
contention during full-suite runs.

**File:** `tests/pglite/jsonb-stress.test.ts` (and potentially other
PGlite test files)

```ts
// At the top of each describeScenario:
it(`cache=${size} (${pages} pages) @fast`, { timeout: 60_000 }, async () => {
```

Or globally in vitest config for PGlite tests:

```ts
// vitest.config.ts
export default defineConfig({
  test: {
    globals: true,
    testTimeout: 30000,
    include: ["tests/**/*.test.ts"],
    pool: "threads",
    // Override timeout for heavy PGlite tests
    typecheck: { tsconfig: "./tsconfig.json" },
  },
});
```

A simpler approach: a vitest workspace or project-level override:

```ts
// vitest.config.ts
export default defineConfig({
  test: {
    globals: true,
    testTimeout: 30000,
    include: ["tests/**/*.test.ts"],
    pool: "threads",
    poolOptions: {
      threads: {
        // Limit concurrency for PGlite tests
        maxThreads: 2,
      },
    },
  },
});
```

**Tradeoff:** Increasing timeout hides legitimate regressions that cause
real slowdowns. A 60s timeout still catches infinite loops but tolerates
2x resource contention.

### Option B: Limit PGlite test concurrency (better long-term)

Reduce thread count during PGlite tests or run them sequentially. PGlite
tests are inherently heavyweight — each WASM Postgres instance needs
~200 MB. Running fewer in parallel keeps memory and CPU within bounds.

**Approach:** Use vitest's `sequence` or `poolMatchGlobs` to isolate
PGlite tests:

```ts
// vitest.config.ts
export default defineConfig({
  test: {
    globals: true,
    testTimeout: 30000,
    include: ["tests/**/*.test.ts"],
    pool: "threads",
    poolOptions: {
      threads: {
        // Lower thread count for memory-bound tests
        maxThreads: Math.max(2, (os.cpus().length || 4) - 2),
      },
    },
  },
});
```

Or split PGlite tests into a separate vitest project that runs with
fewer threads.

**Tradeoff:** Slower total suite time (PGlite tests already dominate at
~30 min). But more reliable.

### Option C: Increase timeout only for GIN-related scenarios

The GIN index tests are the most I/O-intensive because they require
metapage reads, entry tree traversals, posting list scans, heap fetches,
and TOAST decompression — all hitting the page cache under pressure.

Add per-test timeouts only for the known heavy scenarios:

```ts
it(`cache=${size} (${pages} pages) @fast`, { timeout: 60_000 }, async () => {
```

**Files to update:**
- `tests/pglite/jsonb-stress.test.ts` — GIN index scenarios
- `tests/pglite/fts-gin-stress.test.ts` — GIN index + full-text search
- `tests/pglite/index-stress.test.ts` — B-tree + hash index stress

## Recommendation

**Option A** (per-test timeout increase to 60s) is the simplest fix that
addresses the immediate issue without changing test architecture. Apply
it to the GIN-related test scenarios first, then monitor CI for further
flakes.

If flakes recur across more PGlite test files, escalate to **Option B**
(concurrency limits).

## Full Test Results Summary

| Backend | Test Group | Files | Tests | Pass | Fail | Skip |
|---------|-----------|-------|-------|------|------|------|
| default | Full suite | 195 | 4774 | 4751 | 1* | 22 |
| default | Non-PGlite | 169 | 3891 | 3891 | 0 | 22 |
| tomefs | Conformance | 35 | 517 | 513 | 0 | 4 |
| tomefs (4pg) | Conformance | 35 | 517 | 513 | 0 | 4 |
| preload | Conformance | 35 | 517 | 513 | 0 | 4 |
| default | jsonb-stress (rerun) | 1 | 40 | 40 | 0 | 0 |

*The single failure is a timeout flake, not a correctness bug.
