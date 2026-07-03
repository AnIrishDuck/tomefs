# Test Suite Status — 2026-07-03

## Summary

All non-fast tests pass. No regressions detected across any backend configuration.

## Test Runs Executed

| Configuration | Files | Tests | Skipped | Result |
|---|---|---|---|---|
| Default (MEMFS) | 185 (2 skipped) | 4513 | 22 | PASS |
| `TOMEFS_BACKEND=tomefs` | all | all | — | PASS (exit 0) |
| `TOMEFS_BACKEND=preload` | all | all | — | PASS (exit 0) |
| `TOMEFS_BACKEND=tomefs TOMEFS_MAX_PAGES=4` | 137 | 2096 | 4 | PASS |
| Conformance only (`tomefs`) | 34 | 496 | 4 | PASS |

## CI Status

Last 10 GitHub Actions runs on `main` and feature branches: all `success`.

## Skipped Tests

The following tests are consistently skipped (not regressions):
- `tests/conformance/enametoolong.test.ts` — 8 tests skipped under MEMFS (MEMFS doesn't enforce ENAMETOOLONG)
- `tests/conformance/readdir.test.ts` — 2 tests skipped under tomefs (readdir ordering differences)
- `tests/conformance/mkdir.test.ts` — 1 test skipped under MEMFS

These are known, intentional skips based on backend capabilities.

## Observations

- Stderr noise from `mmapAlloc` warnings is expected — Emscripten WASM module emits these during mmap operations in the fuzz tests. Not a regression.
- PGlite integration tests (basic, savepoint-stress, partition-matview-stress, foreign-key) all pass across all cache sizes.
- Fuzz tests (differential, preload-flush-roundtrip, backend-invariants, syncfs-roundtrip) all pass.

## Action Items

None. The test suite is clean.
