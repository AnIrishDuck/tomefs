# Regression Report: 2026-10-03

## Summary

**No regressions found.** All 4752 tests pass on `main` (commit `3d329f8`).

## Test Run Details

- **Date**: 2026-10-03
- **Branch**: `main` at `3d329f8` ("Add regression report for 2026-10-02: all 4752 tests passing")
- **Duration**: ~19 minutes wall time (1141s), 3317s total test time across threads
- **Result**: 193 test files passed, 2 skipped | 4752 tests passed, 22 skipped | 0 failures

## What Was Tested

Full `npm test` (vitest run) covering all test categories:
- Conformance tests (35 files): POSIX FS operations, stat, links, rename, truncate, etc.
- Unit tests (29 files): page cache, SAB bridge, error handling, persistence, backends
- Integration tests (2 files): PreloadBackend, SAB+Atomics bridge
- Adversarial tests (82 files): cache eviction, page boundaries, dirty tracking, crash recovery, mmap pressure, rename edge cases
- Fuzz tests (17 files): differential testing, incremental syncfs, preload flush roundtrip, backend invariants
- Workload tests (2 files): persistence scenarios, workload simulation
- PGlite tests (24 files): basic SQL, CTE/window functions, cursor stress, dirty shutdown, join stress, JSONB, index stress, cache pressure, persistence, schema evolution, TOAST, vacuum, sequences, savepoints, bulk load, temp tables, upsert/trigger
- Scribe-data tests (3 files): write patterns, sync paging, search indexing
- BadFS validation (1 file)

## Skipped Tests

- 2 test files skipped (as expected):
  - `allocate-mmap.test.ts` — allocate+mmap combination not yet implemented
  - `enametoolong.test.ts` — platform-specific name length checks
- Total: 22 individual tests skipped, consistent with all previous runs

## Observations

- Runtime improved to ~19 minutes from ~27 minutes in the 2026-10-02 report, suggesting previous slowness was environment-specific.
- Slowest test files remain PGlite integration tests: `savepoint-stress.test.ts` (144s), `cursor-stress.test.ts`, `write-patterns.test.ts` (66s).
- Test count (4752 passed, 22 skipped) has been stable since at least 2026-08-30 with no changes to the test suite.

## Action Required

None. No regressions to fix.
