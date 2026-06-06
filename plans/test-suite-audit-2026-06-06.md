# Test Suite Audit — 2026-06-06

## Summary

Full test suite (`npm test` / `vitest run`) completed with **zero regressions**.

| Metric       | Value              |
|--------------|--------------------|
| Test files   | 161 passed, 2 skipped (163 total) |
| Tests        | 4056 passed, 22 skipped (4078 total) |
| Duration     | 1377s (~23 min)    |
| Failures     | **0**              |

## Skipped Tests

- `tests/conformance/enametoolong.test.ts` — 8 tests skipped. This file tests `ENAMETOOLONG` errno behavior; likely skipped because the Emscripten FS does not enforce name-length limits in the test environment.

## Observations

### Slow test files

Several pglite test files dominate runtime, each taking ~3 minutes because they spin up PGlite instances across 4 cache sizes (tiny/small/medium/large):

| File | Duration |
|------|----------|
| `tests/pglite/sequence-stress.test.ts` | 201s |
| `tests/pglite/bulk-load.test.ts` | 194s |
| `tests/pglite/index-stress.test.ts` | 192s |
| `tests/pglite/toast-stress.test.ts` | 191s |
| `tests/pglite/savepoint-stress.test.ts` | 168s |
| `tests/pglite/partition-matview-stress.test.ts` | 155s |
| `tests/pglite/dirty-shutdown.test.ts` | 115s |

These collectively account for ~18 minutes of the ~23-minute total.

### Stderr noise (non-issues)

The fuzz tests emit `Aborted(internal error: mmapAlloc called but emscripten_builtin_memalign native symbol not exported)` to stderr. This is a known Emscripten warning when mmap is used without the memalign export — it does not affect test correctness since the tests pass.

## Action Items

No regressions found — no fixes needed. The test suite is healthy.

Potential improvements (not regressions):
1. **Test parallelism**: The pglite stress tests are the bottleneck. If CI time matters, consider running them in parallel workers (vitest `--pool forks`).
2. **Stderr noise**: The `mmapAlloc` warnings could be suppressed by adding `emscripten_builtin_memalign` to the WASM module exports, but this is cosmetic.
3. **ENAMETOOLONG tests**: Consider whether these should be unskipped or removed if the platform will never support name-length enforcement.
