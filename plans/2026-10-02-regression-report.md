# Regression Report: 2026-10-02

## Summary

**No regressions found.** All 4752 tests pass on `main` (commit `3323bfc`).

## Test Run Details

- **Date**: 2026-10-02
- **Branch**: `main` at `3323bfc` ("Add regression report for 2026-10-01: all 4752 tests passing")
- **Duration**: ~27 minutes wall time (1629s), 4747s total test time across threads
- **Result**: 193 test files passed, 2 skipped | 4752 tests passed, 22 skipped | 0 failures

## What Was Tested

Full `npm test` (vitest run) covering all test categories:
- Conformance tests (22 files): POSIX FS operations, stat, links, rename, truncate, etc.
- Unit tests: page cache, SAB bridge, error handling, persistence, backends
- Integration tests: PreloadBackend, SAB+Atomics bridge
- Adversarial tests: cache eviction, page boundaries, dirty tracking, crash recovery, mmap pressure, rename edge cases
- Fuzz tests: differential testing, incremental syncfs, preload flush roundtrip, backend invariants
- Workload tests: persistence scenarios, workload simulation
- PGlite tests: basic SQL, CTE/window functions, cursor stress, dirty shutdown, join stress, JSONB, index stress, cache pressure, persistence, schema evolution, TOAST, vacuum, sequences, savepoints, bulk load, temp tables, upsert/trigger
- Scribe-data tests: write patterns, sync paging, search indexing
- BadFS validation

## Skipped Tests

- 2 test files skipped (as expected):
  - `allocate-mmap.test.ts` — allocate+mmap combination not yet implemented (13 tests)
  - 9 additional individual test skips across other files
- Total: 22 individual tests skipped, consistent with previous runs

## Observations

- Test suite took ~27 minutes wall time (up from ~10 minutes in the 2026-10-01 report). The difference is likely due to environment resource constraints in this cloud session vs. previous runs — all tests passed within their 30s timeouts.
- Slowest test files were PGlite integration tests: `cursor-stress.test.ts` (174s), `dirty-shutdown.test.ts` (133s), `cache-pressure-persistence.test.ts`, `write-patterns.test.ts` (95s).

## Action Required

None. No regressions to fix.
