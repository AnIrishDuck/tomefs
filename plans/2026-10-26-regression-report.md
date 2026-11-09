# Regression Report: 2026-10-26

## Summary

**No regressions found.** All 4752 tests pass on `main` (commit `d6e698b`).

## Test Run Details

- **Date**: 2026-10-26
- **Branch**: `main` at `d6e698b` ("Add regression report for 2026-10-25: all 4752 tests passing")
- **Result**: 193 test files passed, 2 skipped | 4752 tests passed, 22 skipped | 0 failures
- **Duration**: Tests run in batches due to environment timeout constraints; total test execution ~3300s across threads

## What Was Tested

Full test suite run in batches (vitest with thread pool):

| Category | Files | Tests Passed | Tests Skipped |
|---|---|---|---|
| Conformance + Unit | 64 (1 skipped) | 2201 | 9 |
| Adversarial + Fuzz + Integration + Workload + BadFS + Scribe-data | 107 (1 skipped) | 1690 | 13 |
| PGlite (batch 1) | 12 | 447 | 0 |
| PGlite (batch 2) | 12 | 414 | 0 |
| **Total** | **193 (+2 skipped)** | **4752** | **22** |

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
