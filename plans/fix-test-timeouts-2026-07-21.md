# Fix Test Timeout Regressions (2026-07-21)

## Summary

Full test suite run on 2026-07-21: **15 tests failed** (across 11 test files) | 4737 passed | 22 skipped (4774 total). Duration: 36 minutes. All failures are timeouts — no data corruption or assertion failures. CI (GitHub Actions) passes on the same commit (`8a5d29a`), so these are environment-sensitive performance issues caused by PGlite-Postgres interaction at mid-size caches combined with CPU/memory exhaustion during parallel test execution (4 CPU cores, 15 GB RAM).

## Failing Tests

### 1. PGlite UPSERT — Scenario 12: multi-column UNIQUE constraint UPSERT
- **File**: `tests/pglite/upsert-trigger-stress.test.ts:836`
- **Failures**: `cache=small (16 pages)` — 72649ms; `cache=medium (64 pages)` — 34850ms
- **Passes**: `cache=tiny (4 pages)`, `cache=large (4096 pages)`
- **Timeout**: 30000ms (vitest `testTimeout`)
- **Workload**: 120 individual UPSERT queries (10 rounds × 3 sources × 4 metrics), each awaited separately. Multi-column UNIQUE constraint forces index lookups + potential heap updates per query.

### 2. PGlite DDL — DO block procedural loop under cache pressure
- **File**: `tests/pglite/table-rewrite-ddl.test.ts:583`
- **Failure**: `cache=medium (64 pages)` — 31372ms
- **Passes**: `cache=tiny (4 pages)`, `cache=small (16 pages)`, `cache=large (4096 pages)`
- **Timeout**: 30000ms

### 3. PGlite Schema — Schema evolution: DROP COLUMN
- **File**: `tests/pglite/schema-evolution.test.ts`
- **Failure**: `cache=small (16 pages)` — 30738ms
- **Passes**: `cache=tiny (4 pages)`, `cache=medium (64 pages)`, `cache=large (4096 pages)`
- **Timeout**: 30000ms

### 4. Adversarial — FD write-read cycle with 1-page cache
- **File**: `tests/adversarial/fd-extreme-cache-pressure.test.ts:665`
- **Failure**: 64722ms, "Hook timed out in 10000ms"
- **Nature**: Unlike the PGlite tests, this is a trivially simple test (write 3 pages, read 3 pages). The extreme duration suggests it was starved by concurrent test execution or hit an edge case in Emscripten Module initialization under resource pressure.

### 5. PGlite JSONB — Mixed JSONB sizes with interleaved queries
- **File**: `tests/pglite/jsonb-stress.test.ts:994`
- **Failure**: `cache=tiny (4 pages)` — 102265ms (over 100s!)
- **Passes**: small (17166ms), medium (9158ms), large (7430ms)
- **Timeout**: 30000ms
- **Note**: This test file took 323901ms (5.4 min) total for 40 tests

### 6. PGlite Index — REINDEX rebuilds index from scratch
- **File**: `tests/pglite/index-stress.test.ts`
- **Failures**: `cache=medium (64 pages)` — 41377ms; `cache=large (4096 pages)` — 52354ms
- **Passes**: tiny (5348ms), small (5339ms)
- **Note**: The cache=large failure is especially notable — 4096 pages (32 MB) should hold the entire working set. This confirms resource exhaustion, not cache pressure.

### 7. PGlite Index — Range scan via index with large result
- **File**: `tests/pglite/index-stress.test.ts`
- **Failure**: `cache=tiny (4 pages)` — 31377ms
- **Note**: This test file took 357302ms (6 min) total for 48 tests

### 8. PGlite Join — Multiple concurrent cursors on different tables
- **File**: `tests/pglite/join-stress.test.ts`
- **Failures**: `cache=tiny (4 pages)` — 55352ms; `cache=small (16 pages)` — 37929ms
- **Note**: Cursor operations require holding pages from multiple tables simultaneously; tiny/small caches force constant cross-table thrashing

### 9. PGlite Cache Pressure — Large text data (TOAST)
- **File**: `tests/pglite/cache-pressure.test.ts`
- **Failure**: `cache=large (4096 pages)` — 50695ms
- **Passes**: tiny (5333ms), small (5729ms), medium (5645ms)
- **Note**: Another cache=large failure — pure resource exhaustion

### 10. Scribe-Data — Search indexing: incremental indexing
- **File**: `tests/scribe-data/search-indexing.test.ts:129`
- **Failure**: 58034ms
- **Note**: App-level workload test; file took 109246ms total for 9 tests

### 11. Scribe-Data — Burst note creation
- **File**: `tests/scribe-data/write-patterns.test.ts:38`
- **Failure**: `cache=small (16 pages)` — 36013ms

### 12. PGlite Basic — creates a table, inserts, and selects
- **File**: `tests/pglite/basic.test.ts`
- **Failure**: 34018ms
- **Note**: Even the basic PGlite integration test timed out, confirming system-level resource exhaustion during parallel execution

### Near-misses (passed but borderline)
- "CTE: recursive CTE tree traversal under cache pressure > cache=small (16 pages)" — 29604ms (limit 30000ms)
- "CTAS from populated table > cache=tiny (4 pages)" — 16168ms (unusually slow for tiny cache)
- "Mixed JSONB sizes with interleaved queries > cache=small (16 pages)" — 17166ms (relatively slow)

## Root Cause Analysis

### Two distinct failure categories

**Category A: PGlite mid-cache thrashing (tests 1-3)**

The PGlite tests parameterize cache sizes: tiny=4, small=16, medium=64, large=4096 pages. The failures cluster at **small (16) and medium (64)** where Postgres's working set partially fits but thrashes heavily. This creates a worst-case for the LRU eviction:

- **tiny (4 pages)**: Working set doesn't fit at all. Every operation evicts. But the thrashing is *uniform* — there's no "almost fits" pathology, and the eviction cost is predictable. Paradoxically, this can be *faster* than mid-size caches for certain workloads.

- **small/medium (16-64 pages)**: Working set *partially* fits. Index pages and heap pages compete for cache slots. Each UPSERT needs: (1) btree index traversal, (2) heap page read for conflict detection, (3) heap page write for the update, (4) potential HOT chain follow. With 16-64 pages, the index pages evict heap pages between steps 1→3, causing re-reads from the backend.

- **large (4096 pages)**: Working set fits entirely. No eviction pressure. Baseline performance.

Deep analysis confirmed that **no O(n^2) algorithms exist** in sync-page-cache.ts, tomefs.ts, or the backend. The total tomefs overhead for 2400 page operations is only ~15-25ms. The 72-second runtime for 120 UPSERTs (~600ms/query vs ~1-10ms on MEMFS) means the bottleneck is in **Postgres internals interacting with the tomefs eviction pattern**.

**Why tiny passes but small/medium fail — the resonance hypothesis**:

- **cache=tiny (4 pages)**: Pure pass-through. Every Postgres I/O goes to the SyncMemoryBackend. Postgres's own buffer pool (shared_buffers) is the effective cache. The "disk" has uniform O(1) latency regardless of tomefs state. Paradoxically fast because Postgres's clock-sweep replacement works correctly with uniform backend latency.

- **cache=small (16 pages)**: The cache holds ~ONE working set component (e.g., btree index pages) but not all simultaneously. This creates cross-file thrashing: index pages evict catalog pages, catalog pages evict WAL pages, WAL pages evict heap pages. Critically, Postgres's fsync (one per commit, 120 total) flushes dirty WAL pages via `collectDirtyPagesForFile`. When WAL pages compete with data/index pages for cache slots, Postgres may need to re-read pages it already wrote, potentially triggering additional Postgres-internal checkpoint or WAL segment switches.

- **cache=medium (64 pages)**: Better than small (more pages survive), but still insufficient for the full cross-file working set. Some workloads (UPSERT with multi-column UNIQUE) require simultaneous access to heap, index, WAL, pg_xact, and catalog pages.

- **cache=large (4096 pages)**: Everything fits. No eviction, minimal backend I/O. Baseline performance.

**Supporting evidence**: The per-node page table population at `tomefs.ts:300-304` is an additional inefficiency — after cold-path reads, `getPage()` calls for each page in range can cause cascading evictions at small cache sizes — but this overhead is in the microsecond range and cannot explain the 60-100x slowdown.

**Category B: `beforeEach` hook timeout from WASM module pressure (test 4)**

The 1-page cache FD test took 64722ms with "Hook timed out in 10000ms". Investigation confirmed:

- The failing test is the **last test** (test 20 of 20) in the describe block
- The `beforeEach` hook at line 83 creates a new Emscripten WASM module (2-page cache) for every test — by test 20, there are ~22 un-GC'd WASM module instances
- The test body creates a *separate* FS with `createTestFS(1)` (the `beforeEach` FS is never used)
- vitest's `hookTimeout` defaults to **10000ms** (not configurable in the project's vitest.config.ts), while `testTimeout` is 30000ms
- Under parallel test execution (`pool: "threads"`), WASM module instantiation pressure across test files causes the `beforeEach` call to exceed the 10s hook timeout
- The page cache logic with maxPages=1 has **no deadlock or infinite loop** — the test body would complete in ~300-500ms if the hook succeeded
- The test passes in isolation (~312-510ms)

**Category C: CPU/memory exhaustion during parallel PGlite execution (tests 6-7)**

The REINDEX failure at `cache=large (4096 pages)` is conclusive evidence: with 32 MB of cache (the entire working set fits), cache eviction is irrelevant. The 52354ms runtime is purely from CPU/memory contention during parallel test execution.

The test files `jsonb-stress.test.ts` and `index-stress.test.ts` each take 5-6 minutes. When these run in parallel via vitest's thread pool alongside other PGlite files, each test file creates multiple Emscripten WASM modules (one per test case) competing for 4 CPU cores and 15 GB RAM.

### Not a regression in tomefs source code

CI passes all these tests on the same commit. The failures are timing-dependent:
- PGlite stress tests run 4-7 seconds per cache config × 4 configs × many scenarios
- Parallel test execution with `pool: "threads"` creates CPU contention
- Tests marked `@fast` share the timeout with non-fast tests (30s), but `@fast` only controls which tests `test:fast` runs
- Individual PGlite test files (jsonb-stress, index-stress, upsert-trigger-stress) take 5-6 minutes each; running 3-4 in parallel saturates 4 CPU cores

## Fix Plan

### Option A: Increase timeouts for known-heavy tests (minimal, targeted)

Add per-test or per-file timeout overrides for PGlite stress tests:

```typescript
// In vitest.config.ts or per-test:
it(`cache=${size} (${pages} pages) @fast`, async () => {
  // ...
}, 60_000); // 60s for PGlite stress tests
```

**Files to change:**
- `tests/pglite/upsert-trigger-stress.test.ts` — Scenario 12 (add 60s timeout)
- `tests/pglite/table-rewrite-ddl.test.ts` — DO block procedural loop (add 60s timeout)
- `tests/pglite/schema-evolution.test.ts` — DROP COLUMN (add 60s timeout)
- `tests/pglite/jsonb-stress.test.ts` — Mixed JSONB sizes (add 120s timeout for tiny cache)
- `tests/pglite/index-stress.test.ts` — REINDEX + Range scan (add 60s timeout)
- `tests/adversarial/fd-extreme-cache-pressure.test.ts` — 1-page cache test (add 120s timeout)

**Pros**: Simple, no behavior changes, doesn't mask real bugs.
**Cons**: Doesn't address the underlying performance issue. Tests that take 70s are still slow.

### Option B: Reduce PGlite workload for `@fast`-tagged tests (preferred)

The test at `upsert-trigger-stress.test.ts:852` runs 10 rounds of 12 queries (120 total). Reducing to 5 rounds (60 queries) would halve the workload while still exercising the UPSERT + cache pressure interaction:

```typescript
const rounds = 10; // → 5 for @fast-tagged variants
```

Similarly, reduce row counts in schema-evolution and table-rewrite-ddl tests by ~50% for small/medium cache configs.

**Files to change:**
- `tests/pglite/upsert-trigger-stress.test.ts:851` — reduce `rounds` from 10 to 5
- `tests/pglite/table-rewrite-ddl.test.ts` — reduce DO block loop iterations
- `tests/pglite/schema-evolution.test.ts` — reduce row count for DROP COLUMN test

**Pros**: Tests still exercise the important code paths. Faster CI. More robust timing.
**Cons**: Reduced coverage depth.

### Option C: Fix per-node page table re-population cascade (perf improvement)

In `tomefs.ts:295-305`, after a multi-page read via `pageCache.read()`, the code re-populates the per-node page table by calling `getPage()` for each page in range. With small caches, these `getPage()` calls evict pages that were just loaded, creating wasteful cascading evictions.

Fix: only populate the page table entry for the *last* page in the range (which is guaranteed to still be in cache), or limit population to pages that `pageCache.has()`:

```typescript
// After multi-page cold path read
for (let p = firstPage; p <= lastPage; p++) {
  const existing = node._pages[p];
  if (!existing || existing.evicted) {
    if (pageCache.has(node.storagePath, p)) {
      node._pages[p] = pageCache.getPage(node.storagePath, p);
    }
  }
}
```

Same fix needed in `writePages()` at `tomefs.ts:425-433`.

**Files to change:**
- `src/tomefs.ts:295-305` — guard page table population with `pageCache.has()`
- `src/tomefs.ts:425-433` — same fix for write path

**Pros**: Eliminates wasteful cascading evictions. Meaningful perf improvement for small caches. Makes small-cache behavior closer to tiny-cache.
**Cons**: Requires testing. The `has()` check adds one Map lookup per page but saves a `getPage()` + eviction cycle when the page isn't cached.

### Option D: Fix 1-page cache test hook timeout

Root cause: the `beforeEach` hook at line 83 creates a WASM module for every test, and the 10s default `hookTimeout` is exceeded when running as the 20th test in the describe block under parallel execution pressure. The 1-page cache test doesn't even use the `beforeEach`-created FS.

Three possible fixes, in order of preference:

1. **Set `hookTimeout` in vitest.config.ts** to match `testTimeout`:
   ```typescript
   hookTimeout: 30000,
   ```
   This is the simplest fix and benefits all test files.

2. **Move the 1-page cache test to a separate describe block** that doesn't inherit the `beforeEach`:
   ```typescript
   describe("1-page cache edge cases", () => {
     it("@fast FD write-read cycle with 1-page cache", async () => {
       const { FS: FS1 } = await createTestFS(1);
       // ... test body ...
     });
   });
   ```

3. **Move the 1-page cache test earlier in the file** (before the remount tests at lines 483/510/537 that create additional WASM modules) to reduce cumulative memory pressure.

**Files to change:**
- `vitest.config.ts` — add `hookTimeout: 30000` (option 1)
- OR `tests/adversarial/fd-extreme-cache-pressure.test.ts` — restructure (options 2/3)

### Option E: Reduce PGlite test parallelism (addresses Category C)

The REINDEX failure at cache=large (4096 pages, 32 MB cache) proves CPU/memory exhaustion, not cache algorithm issues. With `pool: "threads"` and 4 CPU cores, 3-4 heavy PGlite test files running simultaneously each create WASM modules and PGlite instances that compete for resources.

Vitest supports per-file concurrency configuration via `poolOptions.threads.maxThreads` or by separating PGlite tests into a separate project with lower parallelism:

```typescript
// vitest.config.ts
export default defineConfig({
  test: {
    globals: true,
    testTimeout: 30000,
    hookTimeout: 30000,
    include: ["tests/**/*.test.ts"],
    pool: "threads",
    poolOptions: {
      threads: {
        // Limit to 2 threads to reduce PGlite WASM module contention
        maxThreads: 2,
      },
    },
  },
});
```

Or use vitest workspace to run PGlite tests with lower parallelism:

```typescript
// vitest.workspace.ts
export default [
  {
    test: {
      include: ["tests/pglite/**/*.test.ts"],
      pool: "threads",
      poolOptions: { threads: { maxThreads: 2 } },
    },
  },
  {
    test: {
      include: ["tests/!(pglite)/**/*.test.ts"],
      pool: "threads",
    },
  },
];
```

**Pros**: Addresses the root cause (resource exhaustion). No code changes. No timeout increases.
**Cons**: Slower total test time due to reduced parallelism.

## Recommended Approach

Combine **Option B** + **Option C** + **Option D** + **Option E**:

1. **Reduce PGlite workload** for the 3 failing PGlite tests — quick fix, immediately improves timing robustness
2. **Fix per-node page table cascade** — real perf improvement that benefits all small-cache users
3. **Fix 1-page test isolation** — eliminates the double Emscripten Module creation

### Implementation Order

1. Option D first (set `hookTimeout: 30000` in vitest.config.ts) — immediate fix for the FD test
2. Option C (fix per-node page table cascade in `tomefs.ts`) — real perf improvement for small caches
3. Run tests to verify improvement
4. Option E if PGlite tests still fail (reduce thread count for PGlite tests)
5. Option B as last resort (reduce workload in individual tests)

## Key Files

| File | Role |
|------|------|
| `src/tomefs.ts:295-305, 425-433` | Per-node page table population after cold-path reads/writes |
| `src/sync-page-cache.ts:908-942` | LRU eviction logic (ensureCapacity, evictOne) |
| `tests/pglite/upsert-trigger-stress.test.ts:836-877` | Scenario 12 test |
| `tests/pglite/table-rewrite-ddl.test.ts` | DO block procedural loop test |
| `tests/pglite/schema-evolution.test.ts` | DROP COLUMN test |
| `tests/adversarial/fd-extreme-cache-pressure.test.ts:665` | 1-page cache FD test |
| `tests/pglite/jsonb-stress.test.ts:994` | Mixed JSONB sizes test (102s on tiny cache) |
| `tests/pglite/index-stress.test.ts` | REINDEX + Range scan tests (fails at large cache too) |
| `tests/pglite/join-stress.test.ts` | Concurrent cursor tests |
| `tests/pglite/harness.ts:14-18` | Cache config definitions |
| `vitest.config.ts` | Global 30s test timeout, missing hookTimeout |
