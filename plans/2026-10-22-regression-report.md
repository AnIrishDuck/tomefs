# Regression Report: 2026-10-22

## Summary

**No regressions found.** All 4752 tests pass on `main` (commit `fa32fa7`).

## Test Run Details

- **Date**: 2026-10-22
- **Branch**: `main` at `fa32fa7` ("Add regression report for 2026-10-21: all 4752 tests passing")
- **Result**: 193 test files passed, 2 skipped | 4752 tests passed, 22 skipped | 0 failures
- **Duration**: 1316.40s wall time (3829.72s test execution across threads)

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
- `tests/adversarial/allocate-mmap.test.ts` (13 tests + 1 skip in mkdir) — mmap not supported in Emscripten WASM

## Action Items

None. No regressions detected. Codebase is stable.
