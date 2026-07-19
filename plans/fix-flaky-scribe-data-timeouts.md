# Fix flaky scribe-data test timeouts under CPU contention

## Problem

The `tests/scribe-data/write-patterns.test.ts` and `tests/scribe-data/sync-paging.test.ts` tests intermittently time out (30s default) when run alongside other test suites on machines with limited CPU cores. The failures are non-deterministic: different tests fail on each run, and all pass reliably when run in isolation.

### Evidence

Full test matrix results (2026-07-19):

| Config | Suite | Result |
|--------|-------|--------|
| Default (MEMFS) | conformance (508 tests) | PASS |
| Default (MEMFS) | unit (1693 tests) | PASS |
| Default (MEMFS) | adversarial (872 tests) | PASS |
| TOMEFS_BACKEND=tomefs | conformance (513 tests) | PASS |
| TOMEFS_BACKEND=tomefs | adversarial (885 tests) | PASS |
| TOMEFS_BACKEND=tomefs | fuzz/workload/scribe (713 tests) | PASS |
| TOMEFS_BACKEND=preload | conformance (513 tests) | PASS |
| TOMEFS_BACKEND=preload | adversarial (885 tests) | PASS |
| TOMEFS_BACKEND=preload | workload/scribe (145 tests) | PASS (re-run, failed first under contention) |
| 4-page cache | conformance (513 tests) | PASS |
| 4-page cache | adversarial (885 tests) | PASS |
| 4-page cache | fuzz/integration (615 tests) | PASS |
| 4-page cache | workload/scribe (145 tests) | **3 FAIL** (timeouts) then **0 FAIL** in isolation |

Failing tests under contention (non-deterministic — different tests fail each run):
- `write-patterns.test.ts` > "Burst note creation" > cache=tiny (4 pages) — 30s timeout
- `write-patterns.test.ts` > "Version chain" > cache=large (4096 pages) — 30s timeout
- `sync-paging.test.ts` > "single page — all blobs fit in one fetch" — 30s timeout

In isolation, these tests complete in 3-6s. Under CPU contention (10 vitest processes on 4 cores), individual PGlite WASM operations slow down enough to exceed the 30s timeout.

CI on main is green — CI runs test suites sequentially, not in parallel, so contention doesn't occur.

### Root cause

The global `testTimeout: 30000` in `vitest.config.ts` (line 6) is adequate for isolated runs but too tight for PGlite+WASM workloads under CPU contention. The scribe-data tests are the most sensitive because they:

1. Run full PGlite instances (WASM) with complex SQL schemas (5 tables + indexes + triggers)
2. Execute many sequential SQL operations (100 INSERT loops, 20-version chains)
3. Layer cache eviction pressure (4-page max) on top of the WASM overhead
4. Each cache miss triggers a synchronous backend read/write cycle

## Proposed fix

Increase the test timeout for the scribe-data test files specifically, rather than globally. This keeps the global 30s timeout as a safety net for other tests while giving the PGlite workload tests room to breathe under contention.

### Option A: Per-file timeout (recommended)

Add a `testTimeout` override in each scribe-data test file:

```ts
// tests/scribe-data/write-patterns.test.ts
import { describe, it, expect, afterEach } from "vitest";
// ... existing imports ...

// PGlite+WASM tests need more headroom under CPU contention
const TEST_TIMEOUT = 60_000;

// Then use it in each it() call:
it(`cache=${size} (${pages} pages)`, async () => {
  // ...
}, TEST_TIMEOUT);
```

Files to change:
- `tests/scribe-data/write-patterns.test.ts` — all `it()` calls (20 tests)
- `tests/scribe-data/sync-paging.test.ts` — all `it()` calls (16 tests)

### Option B: Vitest project-level override

Use vitest's `test.testTimeout` per-file via inline config:

```ts
// At the top of write-patterns.test.ts:
import { describe, it, expect, afterEach, vi } from "vitest";
vi.setConfig({ testTimeout: 60_000 });
```

This is cleaner but less explicit about which tests need the override.

### Option C: Increase global timeout

Change `vitest.config.ts` line 6 from `testTimeout: 30000` to `testTimeout: 60000`. Simplest change but reduces the timeout's value as a regression detector for other test suites.

## Recommendation

**Option A** is preferred. The 30s global timeout is correct for the vast majority of tests — only the PGlite WASM workload tests need more headroom. Making the override explicit per-test-file documents why and keeps the safety net for everything else.

60s is sufficient: the slowest individual test seen in isolation was ~18s (`search-indexing > paginated search — cache=tiny`), and under heavy contention the PGlite tests typically take 10-15s. A 4x safety margin is adequate.

## Scope

- No code changes to `src/` — this is a test infrastructure issue only.
- No logic bugs or assertion failures were found across the full test matrix.
- CI is unaffected (tests run sequentially there).
