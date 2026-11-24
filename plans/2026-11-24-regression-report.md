# Regression Report: 2026-11-24

## Summary

**No regressions found.** All 4,752 tests pass on `main` (commit `83a3ab7`). The full suite completed in a single run with zero failures.

## Test Run Details

- **Date**: 2026-11-24
- **Branch**: `main` at `83a3ab7` ("Add regression report for 2026-11-23: all 4752 tests passing")
- **Result**: 4,752 tests passed, 22 skipped | 0 failures
- **Duration**: 1,042s (~17 minutes)
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

| Metric | 2026-11-23 | 2026-11-24 | Delta |
|---|---|---|---|
| Tests passed | 4,752 | 4,752 | 0 |
| Tests skipped | 22 | 22 | 0 |
| Failures | 0 | 0 | 0 |
| Test files | 193 passed | 193 passed | 0 |
| Duration | 1,138s | 1,042s | -96s (faster) |

## Notes

- No code changes since previous report — only regression report commits.
- Non-fatal `mmapAlloc` stderr warnings continue in fuzz tests (Emscripten `emscripten_builtin_memalign` symbol not exported). These do not affect results.
- Full suite completed in one run (~17 minutes), including all 24 PGlite test files.
- Run was ~96 seconds faster than yesterday, within normal variance.

## Action Required

None. No regressions detected.
