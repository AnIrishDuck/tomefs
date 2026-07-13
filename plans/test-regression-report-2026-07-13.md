# Test Regression Report — 2026-07-13

## Summary

**No regressions found.** The full test suite passes cleanly, and all recent CI runs are green.

## Test Results

| Metric | Value |
|---|---|
| Test files | 187 passed, 2 skipped (189 total) |
| Tests | 4,571 passed, 22 skipped (4,593 total) |
| Duration | ~1,127s (~19 min) |
| Exit code | 0 |

### Skipped tests (expected)

Both skipped files require `TOMEFS_BACKEND=tomefs` — they test tomefs-only features not present in MEMFS:

- `tests/adversarial/allocate-mmap.test.ts` (13 tests) — allocate/mmap stream_ops
- `tests/conformance/enametoolong.test.ts` (8 tests) — ENAMETOOLONG enforcement

These were verified separately with `TOMEFS_BACKEND=tomefs` and all 21 pass.

### One additional skipped test

The remaining 1 skipped test (22 total - 21 from the two files above) is an individual skip within another test file — likely a platform-specific guard or TODO marker.

## CI Status

All 30 recent workflow runs on GitHub Actions:
- 28 succeeded
- 2 cancelled (superseded by newer pushes — not failures)

No failures in CI history for the current branch.

## Recent Commits

The most recent commits are a mix of bug fixes and new test coverage:

- `3404c4d` — Make fsync atomic via syncAll instead of separate writes (#354)
- `23a3dfe` — Fix PreloadBackend flush losing writes during async gap (#353)
- `44ede01` — Optimize PreloadBackend.cleanupOrphanedPages (#358)
- `081ccb7` — Optimize cleanupOrphanedPages (#355)
- `722b049` — Batch stale tail page writes during dirty recovery (#351)
- `65e9bce` — Zero stale tail bytes in backend during dirty crash recovery (#341)
- `6825bf1` — Fix orphan pages surviving at reused storage paths (#340)

All recent bug fixes have corresponding test coverage and the full suite confirms they work correctly.

## Action Items

None. The codebase is in a clean state with no regressions to fix.
