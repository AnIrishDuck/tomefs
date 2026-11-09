# Regression Report: 2026-10-25

## Summary

**No regressions found.** All 4752 tests pass on `main` (commit `9ec8dee`).

## Test Run Details

- **Date**: 2026-10-25
- **Branch**: `main` at `9ec8dee` ("Add regression report for 2026-10-24: all 4752 tests passing")
- **Result**: 193 test files passed, 2 skipped | 4752 tests passed, 22 skipped | 0 failures
- **Duration**: 1326.97s wall time (3859.31s test execution across threads)

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

## Stderr Notes

Non-fatal `mmapAlloc` warnings appeared in stderr during fuzz/differential tests:
```
Aborted(internal error: mmapAlloc called but `emscripten_builtin_memalign` native symbol not exported)
```
These are expected Emscripten WASM limitations and do not affect test results. They have been present in prior runs.

## Action Items

None. No regressions detected. Codebase is stable.
