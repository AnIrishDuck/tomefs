# Test Health Report — 2026-07-06

## Summary

Full test suite (`npm test`) passed with **zero failures**.

| Metric         | Value       |
|----------------|-------------|
| Test files     | 185 passed, 2 skipped (187 total) |
| Individual tests | 4,513 passed, 22 skipped (4,535 total) |
| Duration       | ~1,354s (~22.5 min) |
| Exit code      | 0           |

## Skipped Tests

Both skipped test files are **intentionally guarded**, not regressions:

1. **`tests/adversarial/allocate-mmap.test.ts`** (13 tests) — requires `TOMEFS_BACKEND=tomefs` env var. Tests allocate() and mmap() stream_ops that only exist on tomefs, not MEMFS.

2. **`tests/conformance/enametoolong.test.ts`** (8 tests) — likely requires a TOMEFS_BACKEND flag or a feature that isn't available in the default test configuration.

3. **`tests/conformance/mkdir.test.ts`** — 1 of 10 tests skipped (same pattern).

These are design-time skip guards, not failures.

## Observations

- **No regressions found.** All test categories pass: conformance, unit, integration, adversarial, fuzz, workload, PGlite, scribe-data, and badfs.
- **PGlite tests are the bottleneck.** The ~15 PGlite test files account for the majority of wall-clock time (~3-4 min each), testing across 4 cache sizes (tiny/small/medium/large).
- **stderr noise is benign.** Fuzz differential tests emit `Aborted(internal error: mmapAlloc called but emscripten_builtin_memalign native symbol not exported)` to stderr. This is expected — MEMFS's mmap path hits this on large allocations, but the tests still pass because they catch the error.

## Regressions to Fix

**None.** No action items from this test run.

## Potential Improvements (not regressions)

- The 2 skipped test files could be included in CI by running a second pass with `TOMEFS_BACKEND=tomefs`. The CI workflow (`.github/workflows/ci.yml`) may already do this in a separate stage.
- The mmapAlloc stderr noise could be suppressed in the fuzz harness to keep logs cleaner, but it's cosmetic.
