# Regression Report: 2026-10-18

## Summary

**No regressions found.** All 4752 tests pass on `main` (commit `bb76f99`).

## Test Run Details

- **Date**: 2026-10-18
- **Branch**: `main` at `bb76f99` ("Add regression report for 2026-10-17: all 4752 tests passing")
- **Result**: 193 test files passed, 2 skipped | 4752 tests passed, 22 skipped | 0 failures
- **Duration**: 1285.92s wall time (3740.81s test execution across threads)

## What Was Tested

Full sequential `npm test` run (vitest with thread pool):

| Metric | Value |
|---|---|
| Test Files Passed | 193 |
| Test Files Skipped | 2 |
| Tests Passed | 4752 |
| Tests Skipped | 22 |
| Failures | 0 |

## Skipped Tests

Same 2 files / 22 tests as prior runs:
- `tests/conformance/enametoolong.test.ts` (8 tests) — ENAMETOOLONG not supported in Emscripten WASM
- `tests/adversarial/allocate-mmap.test.ts` (13 tests) — mmap not supported in Emscripten WASM
- 1 test in `tests/conformance/mkdir.test.ts` — known skip

## Comparison to Prior Run

Identical to 2026-10-17: same test count (4752), same skip count (22), same pass rate (100%). No code changes since last report.

## Notes

- Full sequential run took ~21 minutes wall time; prior parallelized runs completed in ~17 minutes.
- Emscripten `mmapAlloc` stderr warnings during fuzz tests are benign (known Emscripten limitation, not test failures).
- No new test files or code changes since the previous report.
