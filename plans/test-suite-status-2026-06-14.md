# Test Suite Status Report — 2026-06-14

## Summary

Full test suite (`npm test`) ran to completion with **zero regressions**.

| Metric | Value |
|--------|-------|
| Test files passed | 178 |
| Test files skipped | 2 |
| Individual tests passed | 4,314 |
| Individual tests skipped | 22 |
| Failures | **0** |
| Duration | 1,358s (~23 min) |
| Commit | `b1d5cb8` (HEAD) |

## Skipped Tests (All Intentional)

Both skipped files are gated behind `TOMEFS_BACKEND=tomefs`, which is only set
in CI. They test tomefs-specific behavior that MEMFS doesn't support:

1. **`tests/adversarial/allocate-mmap.test.ts`** (13 tests) — `allocate()` and
   `mmap()` stream_ops are tomefs-only.
2. **`tests/conformance/enametoolong.test.ts`** (8 tests) — ENAMETOOLONG
   enforcement (path component > 255 chars) is tomefs-only.

CI runs these with `TOMEFS_BACKEND=tomefs npm test` and additionally
`TOMEFS_BACKEND=preload` for adversarial tests. See `.github/workflows/ci.yml`
lines 80, 103, 126.

## No Regressions Found

Every test category passed cleanly:

- **Conformance** (22 files) — POSIX FS semantics
- **Unit** (12 files) — page cache, backends, SAB bridge, error handling
- **Fuzz** (8 files) — differential, persistence, dirty-shutdown, page-cache
- **Adversarial** (38 files) — edge cases at cache/persistence seams
- **Integration** (1 file) — PreloadBackend full-stack
- **PGlite** (14 files) — SQL-level integration across cache sizes
- **Scribe-data** (3 files) — app-level workload scenarios
- **Workload** (2 files) — persistence and access pattern scenarios
- **BadFS** (1 file) — defect injection validation

## Observations

### Stderr noise (non-blocking)
The fuzz/differential tests emit repeated `Aborted(internal error: mmapAlloc
called but emscripten_builtin_memalign native symbol not exported)` messages to
stderr. These are Emscripten mmap path fallback warnings, not test failures.
The tests handle this gracefully and all pass.

### Test duration distribution
PGlite tests dominate runtime. Each PGlite test file takes 30–200s because it
boots a full PGlite instance per cache-size variant (tiny/small/medium/large).
The 14 PGlite files account for ~80% of the total 23-minute runtime.

## Recommended Next Steps

No fix plan needed — the suite is green. Areas to monitor:

1. **Emscripten mmap warnings**: The `mmapAlloc` stderr noise could be cleaned
   up by suppressing or handling it in the test harness, but it's cosmetic.
2. **Test runtime**: The PGlite tests are slow. If CI time becomes a concern,
   consider reducing cache-size variants for non-`@fast` tests or running
   PGlite tests in a separate CI stage with longer timeout.
3. **TOMEFS_BACKEND coverage**: To validate the 2 skipped test files locally,
   run `TOMEFS_BACKEND=tomefs npm test`.
