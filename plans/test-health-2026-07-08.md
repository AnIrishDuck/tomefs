# Test Health Report — 2026-07-08

## Summary

Full test suite (`npm test`) ran to completion with **zero failures**.

| Metric | Value |
|--------|-------|
| Test files | 187 passed, 2 skipped (189 total) |
| Individual tests | 4571 passed, 22 skipped (4593 total) |
| Duration | 1378s (~23 minutes) |
| Exit code | 0 |

## Skipped Tests (Expected)

Both skipped files require `TOMEFS_BACKEND=tomefs` and skip intentionally when running against MEMFS:

1. **`tests/adversarial/allocate-mmap.test.ts`** (13 tests) — mmap/allocate tests specific to tomefs, not applicable to MEMFS.
2. **`tests/conformance/enametoolong.test.ts`** (8 tests) — ENAMETOOLONG enforcement tests, tomefs-only since MEMFS doesn't enforce NAME_MAX.

## No Regressions Found

All test categories passed:
- **Conformance** (22 files): POSIX FS behavior against Emscripten WASM module
- **Unit** (24 files): Page cache, backends, SAB bridge, protocol
- **Adversarial** (77 files): Edge cases targeting page cache seams, crash recovery, rename, fsync
- **Fuzz** (16 files): Differential, persistence, dirty-shutdown, backend invariants
- **Integration** (2 files): Full-stack tomefs + SAB + backend
- **PGlite** (20 files): SQL-level integration across cache sizes (tiny/small/medium/large)
- **Workload** (2 files): Simulated PGlite access patterns
- **Scribe-data** (3 files): App-level workload scenarios
- **BadFS** (1 file): Defect injection validation

## Observations

### Performance Bottleneck: PGlite Tests

PGlite test files dominate wall-clock time. Each file runs 4 cache-size variants (tiny=4, small=16, medium=64, large=4096 pages) with each test taking 3-5 seconds for PGlite init + SQL execution. Individual file times:

| File | Duration |
|------|----------|
| `join-stress.test.ts` | 203s |
| `bulk-load.test.ts` | 202s |
| `fk-cascade-stress.test.ts` | 176s |
| `cursor-stress.test.ts` | 157s |
| `partition-matview-stress.test.ts` | 151s |
| `dirty-shutdown.test.ts` | 110s |

The non-PGlite tests complete in under 3 minutes; PGlite tests account for ~20 minutes of the ~23 minute total.

### Stderr Noise (Non-Errors)

Differential fuzz tests produce expected `mmapAlloc` abort messages on stderr — these are from MEMFS's mmap codepath, not from tomefs, and do not indicate failures.

## Action Items

No fixes needed — the suite is green. If test speed becomes a concern, the PGlite init overhead per test case is the primary target.
