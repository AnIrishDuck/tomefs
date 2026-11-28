# Regression Report: 2026-11-28

## Summary

**No regressions found.** All 4,752 tests pass on `main` (commit `df4f872`). The full suite completed in a single run with zero failures.

## Test Run Details

- **Date**: 2026-11-28
- **Branch**: `main` at `df4f872` ("Add regression reports for 2026-11-24 through 2026-11-27: all 4752 tests passing")
- **Result**: 4,752 tests passed, 22 skipped | 0 failures
- **Duration**: 982s (~16 minutes)
- **Test files**: 193 passed, 2 skipped (195 total)

## What Was Tested

| Category | Files | Tests Passed | Tests Skipped | Failures |
|---|---|---|---|---|
| Conformance (POSIX) | 34 passed, 1 skipped | 508 | 9 | 0 |
| Unit (page cache, backends, SAB bridge) | 29 | 1,693 | 0 | 0 |
| Adversarial (edge cases) | 81 passed, 1 skipped | 872 | 13 | 0 |
| Fuzz (differential, roundtrip, etc.) | 17 | 568 | 0 | 0 |
| Integration + Workload + Scribe-data + BadFS | 8 | 250 | 0 | 0 |
| PGlite (SQL-level integration) | 24 | 861 | 0 | 0 |

## Skipped Tests

22 skipped tests across 2 skipped files — same as all previous reports:
- `enametoolong.test.ts` (8 tests) — only runs when `TOMEFS_BACKEND=tomefs` is set (CI small-cache stress stage)
- `allocate-mmap.test.ts` (13 tests) — only runs when `TOMEFS_BACKEND=tomefs` is set (CI small-cache stress stage)
- 1 test in `mkdir.test.ts` — known platform skip

## Comparison to Previous Run

| Metric | 2026-11-27 | 2026-11-28 | Delta |
|---|---|---|---|
| Tests passed | 4,752 | 4,752 | 0 |
| Tests skipped | 22 | 22 | 0 |
| Failures | 0 | 0 | 0 |
| Test files | 193 passed | 193 passed | 0 |
| Duration | 1,039s | 982s | -57s (faster) |

## Notes

- No code changes since previous report — only regression report commits.
- Non-fatal `mmapAlloc` stderr warnings continue in fuzz tests (Emscripten `emscripten_builtin_memalign` symbol not exported). These do not affect results.
- Full suite completed in one run (~16 minutes), including all 24 PGlite test files.
- Run was ~57 seconds faster than 2026-11-27 run, within normal variance.

## Action Required

None. No regressions detected.
