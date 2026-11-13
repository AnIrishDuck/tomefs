# Regression Report: 2026-11-13

## Summary

**No regressions found.** All 4752 tests pass on `main` (commit `bc5132a`).

## Test Run Details

- **Date**: 2026-11-13
- **Branch**: `main` at `bc5132a` ("Add regression report for 2026-11-09: all 4752 tests passing")
- **Result**: 193 test files passed, 2 skipped | 4752 tests passed, 22 skipped | 0 failures
- **Duration**: ~1250s wall clock (~3630s test execution across threads)

## What Was Tested

Full test suite (`npm test`) run as a single invocation via vitest thread pool:

| Category | Result |
|---|---|
| Conformance (POSIX) | All passed |
| Unit (page cache, backends, SAB bridge) | All passed |
| Adversarial (edge cases) | All passed |
| Fuzz (differential, preload flush roundtrip, preload FS roundtrip) | All passed |
| Integration (tomefs + SAB + backend, preload) | All passed |
| Workload (PGlite access patterns) | All passed |
| Scribe-data (app-level scenarios) | All passed |
| BadFS (defect injection) | All passed |
| PGlite (SQL-level integration) | All passed |

## Skipped Tests

22 skipped tests across 2 skipped files — same as previous reports. These are known platform-dependent skips (e.g., mkdir sticky bit tests not applicable in Emscripten environment).

## Comparison to Previous Run

| Metric | 2026-11-09 | 2026-11-13 | Delta |
|---|---|---|---|
| Test files passed | 193 | 193 | 0 |
| Tests passed | 4752 | 4752 | 0 |
| Tests skipped | 22 | 22 | 0 |
| Failures | 0 | 0 | 0 |
| Wall clock (s) | ~1243 | ~1250 | +7s (noise) |

## Notes

- No code changes since previous report — only regression report commits.
- Non-fatal `mmapAlloc` warnings appear in test output (Emscripten `emscripten_builtin_memalign` symbol not exported) but do not affect test results. These are a known Emscripten configuration limitation and have been present in all prior runs.

## Action Required

None. No regressions detected.
