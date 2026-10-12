# Regression Report: 2026-10-01

## Summary

**No regressions found.** All 4752 tests pass on `main` (commit `dc4d21c`).

## Test Run Details

- **Date**: 2026-10-01
- **Branch**: `main` at `dc4d21c` ("Add test run report for 2026-09-16: all 4752 tests passing")
- **Node vitest**: v3.2.4
- **Duration**: ~10 minutes (full suite)
- **Result**: 56 test files, 4752 tests, 0 failures

## What Was Tested

Full `npm test` (vitest run) covering:
- Conformance tests (22 files): POSIX FS operations, stat, links, rename, truncate, etc.
- Unit tests: page cache, SAB bridge, error handling, persistence
- Integration tests: PreloadBackend, SAB+Atomics bridge
- Adversarial tests: cache eviction, page boundaries, dirty tracking, crash recovery
- Fuzz tests: differential testing, incremental syncfs, preload flush roundtrip
- Workload tests: journal/WAL, large objects, TOAST, vacuum, small tables
- PGlite tests: basic SQL, binary data, concurrent queries, persistence, savepoints, transactions, schema migration, text search, hash indexes, cache pressure
- Scribe-data tests: real workloads, sync paging, search indexing
- BadFS validation

## Observations

- Known Emscripten stderr warnings (`mmapAlloc called but emscripten_builtin_memalign native symbol not exported`) appear during fuzz tests — these are harmless and expected when the Emscripten module's mmap path is exercised without the native memalign export.
- 13 tests in `allocate-mmap.test.ts` are skipped (expected — allocate+mmap combination not yet implemented).

## CI Confirmation

Latest CI run on `main` (run #906, 2026-09-16) also shows all 4752 tests passing. No CI failures on any recent PR branches either (runs #907-917 all green).

## Action Required

None. No regressions to fix.
