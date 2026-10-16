# Regression Report: 2026-10-16

## Summary

**No regressions found.** All 4752 tests pass on `main` (commit `1e373c7`).

## Test Run Details

- **Date**: 2026-10-16
- **Branch**: `main` at `1e373c7` ("Add regression report for 2026-10-15: all 4752 tests passing")
- **Duration**: ~33.9 minutes wall time (2033s), 5930s total test time
- **Result**: 193 test files passed, 2 skipped | 4752 tests passed, 22 skipped | 0 failures

## What Was Tested

Full `npm test` (vitest run) covering all test categories:
- Conformance tests (35 files): POSIX FS operations, stat, links, rename, truncate, etc.
- Unit tests (29 files): page cache, SAB bridge, error handling, persistence, backends
- Integration tests (2 files): PreloadBackend, SAB+Atomics bridge
- Adversarial tests (82 files): cache eviction, page boundaries, dirty tracking, crash recovery, mmap pressure, rename edge cases
- Fuzz tests (15 files): randomized differential testing, PreloadBackend flush roundtrips
- Workload tests (12 files): simulated PGlite access patterns
- PGlite tests (9 files): SQL-level integration with multiple cache sizes
- Scribe-data tests (3 files): app-level workload scenarios
- BadFS tests (4 files): defect injection validation
- Benchmark tests (2 files): performance benchmarks

## Skipped Tests

Same 2 files / 22 tests as prior runs:
- `enametoolong.test.ts` (8 tests): Emscripten does not enforce NAME_MAX
- `mkdir.test.ts` (1 test): known skip for an Emscripten-specific edge case
- `allocate-mmap.test.ts` (13 tests): mmap not available in test environment

## Trend

Stable — 4752/4752 passing continuously since at least 2026-10-01 (16 consecutive reports). No fix plan needed.
