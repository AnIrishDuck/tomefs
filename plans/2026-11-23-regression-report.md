# Regression Report: 2026-11-23

## Summary

**No regressions found.** All 4,752 tests pass on `main` (commit `1478ec5`). The full suite completed in a single run with zero failures.

## Test Run Details

- **Date**: 2026-11-23
- **Branch**: `main` at `1478ec5` ("Add regression report for 2026-11-22: all tests passing, no regressions")
- **Result**: 4,752 tests passed, 22 skipped | 0 failures
- **Duration**: 1,138s (~19 minutes)
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
- `enametoolong.test.ts` (8 tests) — platform-dependent, name length limit not enforced in this environment
- `allocate-mmap.test.ts` (13 tests) — mmap allocation not available in this environment
- 1 test in `mkdir.test.ts` — known platform skip

## Comparison to Previous Run

| Metric | 2026-11-22 | 2026-11-23 | Delta |
|---|---|---|---|
| Tests passed | ~4,306+ (partial) | 4,752 | Full run |
| Tests skipped | 22 | 22 | 0 |
| Failures | 0 | 0 | 0 |
| Test files | 193 passed | 193 passed | 0 |

**Note**: The 2026-11-22 run did not complete the full PGlite suite due to timeout. Today's run completed all 195 test files in a single invocation.

## Notes

- No code changes since previous report — only regression report commits.
- Non-fatal `mmapAlloc` stderr warnings continue in fuzz tests (Emscripten `emscripten_builtin_memalign` symbol not exported). These do not affect results.
- Full suite completed in one run (~19 minutes), including all 24 PGlite test files.

## Action Required

None. No regressions detected.
