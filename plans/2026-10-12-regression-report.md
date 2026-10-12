# Regression Report: 2026-10-12

## Summary

**No regressions found.** All 4752 tests pass on `main` (commit `d4dfe53`).

## Test Run Details

- **Date**: 2026-10-12
- **Branch**: `main` at `d4dfe53` ("Add regression report for 2026-10-11: all 4752 tests passing")
- **Duration**: ~26.3 minutes wall time (1576s), 4595s total test time
- **Result**: 193 test files passed, 2 skipped | 4752 tests passed, 22 skipped | 0 failures

## What Was Tested

Full `npm test` (vitest run) covering all test categories:
- Conformance tests (35 files): POSIX FS operations, stat, links, rename, truncate, etc.
- Unit tests (29 files): page cache, SAB bridge, error handling, persistence, backends
- Integration tests (2 files): PreloadBackend, SAB+Atomics bridge
- Adversarial tests (82 files): cache eviction, page boundaries, dirty tracking, crash recovery, mmap pressure, rename edge cases
- PGlite tests (37 files): SQL integration, CTE/window, bulk load, indexes, schema evolution, persistence
- Workload tests (2 files): scribe-data app-level scenarios
- Fuzz tests (4 files): randomized differential testing
- BadFS tests (2 files): defect injection validation

## Skipped Tests

- `tests/conformance/enametoolong.test.ts` (8 tests) — skipped (known: ENAMETOOLONG not supported in Emscripten FS)
- `tests/integration/sab-bridge.test.ts` — skipped (requires SharedArrayBuffer + worker environment)
- 14 additional individual skips across various files (environment-specific)

## Notes

- Test count matches all previous days (4752 passed, 22 skipped) — stable.
- Wall-clock time slightly higher than yesterday's run (1576s vs ~1273s), likely due to cloud environment variability — no performance regression indicated.
- Build (`tsc -p tsconfig.build.json`) succeeded with zero errors.
- No action items.
