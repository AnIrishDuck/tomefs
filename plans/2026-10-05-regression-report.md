# Regression Report: 2026-10-05

## Summary

**No regressions found.** All 4752 tests pass on `main` (commit `fd4a5b5`).

## Test Run Details

- **Date**: 2026-10-05
- **Branch**: `main` at `fd4a5b5` ("Add regression report for 2026-10-04: all 4752 tests passing")
- **Duration**: ~22.8 minutes wall time (1368s), 3980s total test time
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
  - `enametoolong.test.ts` — ENAMETOOLONG not enforced in tomefs (known limitation)
- 22 individual tests skipped across other files (expected: mkdir with trailing slash, etc.)

## Regression Status

No regressions detected. This is the 5th consecutive clean run (2026-10-01 through 2026-10-05). The codebase remains stable with no code changes since the test infrastructure was established.
