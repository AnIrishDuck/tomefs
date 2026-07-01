# Test Regression Audit — 2026-07-01

## Result: No regressions found

Full test suite passes cleanly on the current `main` branch (`d3d325f`).

## Test Run Summary

| Metric | Value |
|--------|-------|
| Test files | 185 passed, 2 skipped |
| Individual tests | 4513 passed, 22 skipped |
| Duration | ~27 minutes (1594s wall, 4650s test time) |
| Exit code | 0 |

### Skipped tests (intentional)

Two test files are gated on `TOMEFS_BACKEND=tomefs` and skip in default mode:

- `tests/adversarial/allocate-mmap.test.ts` (13 tests) — tomefs-specific allocate/mmap edge cases
- `tests/conformance/enametoolong.test.ts` (8 tests) — ENAMETOOLONG enforcement (tomefs only, not MEMFS)
- 1 test in `tests/conformance/mkdir.test.ts` also skipped

All three pass when run with `TOMEFS_BACKEND=tomefs`.

### CI status

All recent GitHub Actions runs on `main` and feature branches show `conclusion: success`.

## Observations (not regressions)

1. **PGlite tests dominate runtime.** The 20+ pglite test files each take 3-4 minutes (each runs 40-54 subtests at ~4-5s each). They account for ~80% of total test time. The `npm run test:fast` smoke suite exists to skip these in development.

2. **Emscripten mmap stderr noise.** Every fuzz test that exercises mmap produces `Aborted(internal error: mmapAlloc called but emscripten_builtin_memalign native symbol not exported)` on stderr. This is expected — tomefs provides its own mmap implementation that doesn't need the native symbol, and the error comes from MEMFS's mmap path during differential testing.

## No fix plan needed

Since no regressions were found, there is nothing to fix. This document records the audit for future reference.
