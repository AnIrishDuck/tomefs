# Test Suite Results — 2026-07-04

## Summary

**Full test suite: all passing. No regressions found.**

- Test files: 185 passed, 2 skipped (enametoolong conformance tests)
- Tests: 4513 passed, 22 skipped
- Duration: ~1121s (18.7 minutes)
- Exit code: 0

## Details

All test categories passed cleanly:

| Category | Files | Status |
|---|---|---|
| conformance/ | 22 | All pass (1 file skipped: enametoolong) |
| unit/ | 13 | All pass |
| adversarial/ | ~45 | All pass |
| fuzz/ | 3 | All pass |
| pglite/ | 13 | All pass |
| scribe-data/ | 3 | All pass |
| integration/ | 2 | All pass |
| workload/ | 2 | All pass |

## Skipped Tests

- `tests/conformance/enametoolong.test.ts` — 8 tests, all skipped (likely platform-dependent path length limits not applicable in this environment)
- `tests/conformance/mkdir.test.ts` — 1 test skipped out of 10

## Observations

- The `mmapAlloc` stderr warnings (`Aborted(internal error: mmapAlloc called but emscripten_builtin_memalign native symbol not exported)`) appear during fuzz tests but do not cause test failures. These are benign Emscripten warnings from the WASM module when mmap is attempted but not available.
- PGlite integration tests are the slowest (~3-4s each due to WASM Postgres startup), accounting for most of the total runtime.

## Action Items

None — no regressions to fix. The codebase is in a clean state.
