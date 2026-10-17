# Regression Report: 2026-10-17

## Summary

**No regressions found.** All 4752 tests pass on `main` (commit `e1ee8b4`).

## Test Run Details

- **Date**: 2026-10-17
- **Branch**: `main` at `e1ee8b4` ("Add regression report for 2026-10-16: all 4752 tests passing")
- **Result**: 193 test files passed, 2 skipped | 4752 tests passed, 22 skipped | 0 failures

## What Was Tested

Tests were run per category in parallel (6 vitest processes):

| Category | Files Passed | Files Skipped | Tests Passed | Tests Skipped | Duration |
|---|---|---|---|---|---|
| Conformance | 34 | 1 | 508 | 9 | 19s |
| Unit | 29 | 0 | 1693 | 0 | 40s |
| Adversarial | 81 | 1 | 872 | 13 | 82s |
| Fuzz | 17 | 0 | 568 | 0 | 133s |
| Integration/Workload/BadFS/Scribe-data | 8 | 0 | 250 | 0 | 142s |
| PGlite | 24 | 0 | 861 | 0 | 1046s |
| **Total** | **193** | **2** | **4752** | **22** | |

## Skipped Tests

Same 2 files / 22 tests as prior runs:
- `tests/conformance/enametoolong.test.ts` (8 tests) — ENAMETOOLONG not supported in Emscripten WASM
- `tests/adversarial/allocate-mmap.test.ts` (13 tests) — mmap not supported in Emscripten WASM
- 1 test in `tests/conformance/mkdir.test.ts` — known skip

## Comparison to Prior Run

Identical to 2026-10-16: same test count (4752), same skip count (22), same pass rate (100%). No code changes since last report.

## Notes

- PGlite stress tests continue to be the heaviest workload (~17 min wall time, 9 test files spawning full PGlite instances at 4 cache sizes each).
- Emscripten `mmapAlloc` stderr warnings during fuzz tests are benign (known Emscripten limitation, not test failures).
- No new test files added since last report.
