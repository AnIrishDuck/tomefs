# Regression Report: 2026-11-19

## Summary

**No regressions found.** All 4752 tests pass on `main` (commit `b595884`).

## Test Run Details

- **Date**: 2026-11-19
- **Branch**: `main` at `b595884` ("Add regression report for 2026-11-18: all 4752 tests passing")
- **Result**: 193 test files passed, 2 skipped | 4752 tests passed, 22 skipped | 0 failures
- **Duration**: ~1150s wall clock (~3339s test execution across threads)

## What Was Tested

Full test suite (`npm test`) run as a single invocation via vitest thread pool, plus parallel verification by test category:

| Category | Files | Tests | Result |
|---|---|---|---|
| Conformance (POSIX) | 34 passed, 1 skipped | 508 passed, 9 skipped | All passed |
| Unit (page cache, backends, SAB bridge) | 29 passed | 1693 passed | All passed |
| Adversarial (edge cases) | 81 passed, 1 skipped | 872 passed, 13 skipped | All passed |
| Fuzz (differential, roundtrip, invariants) | 17 passed | 568 passed | All passed |
| Integration (tomefs + SAB + backend, preload) | 2 passed | 47 passed | All passed |
| Workload (PGlite access patterns) | -- | -- | All passed |
| Scribe-data (app-level scenarios) | -- | -- | All passed |
| BadFS (defect injection) | -- | -- | All passed |
| PGlite (SQL-level integration) | -- | -- | All passed |

## Skipped Tests

22 skipped tests across 2 skipped files — same as previous reports:
- `enametoolong.test.ts` (8 tests) — platform-dependent, not applicable in Emscripten environment
- `allocate-mmap.test.ts` (13 tests) — environment constraints
- 1 test in `mkdir.test.ts` — sticky bit not applicable

## Comparison to Previous Run

| Metric | 2026-11-18 | 2026-11-19 | Delta |
|---|---|---|---|
| Test files passed | 193 | 193 | 0 |
| Tests passed | 4752 | 4752 | 0 |
| Tests skipped | 22 | 22 | 0 |
| Failures | 0 | 0 | 0 |
| Wall clock (s) | ~1160 | ~1150 | -10s (noise) |

## Notes

- No code changes since previous report — only regression report commits.
- Non-fatal `mmapAlloc` warnings appear in test output (Emscripten `emscripten_builtin_memalign` symbol not exported) but do not affect test results. These are a known Emscripten configuration limitation and have been present in all prior runs.

## Action Required

None. No regressions detected.
