# Regression Report: 2026-10-07

## Summary

**No regressions found.** All 4752 tests pass on `main` (commit `0be4993`).

## Test Run Details

- **Date**: 2026-10-07
- **Branch**: `main` at `0be4993` ("Add regression report for 2026-10-05: all 4752 tests passing")
- **Duration**: ~29.5 minutes wall time (1770s), 5163s total test time
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
  - `benchmark/` — excluded from standard test run
- 22 individual tests skipped (stable set, matching prior runs)

## Comparison to Previous Run

| Metric | 2026-10-05 | 2026-10-07 |
|--------|-----------|-----------|
| Test files passed | 193 | 193 |
| Tests passed | 4752 | 4752 |
| Tests skipped | 22 | 22 |
| Failures | 0 | 0 |
| Wall time | ~22.8min | ~29.5min |

Wall time increased due to cloud environment variability; no behavioral changes.

## Conclusion

No regressions. No code changes since last report. Suite remains fully green.
