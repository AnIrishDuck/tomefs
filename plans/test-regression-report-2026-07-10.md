# Test Regression Report — 2026-07-10

## Summary

**No regressions found.** The full test suite passes cleanly on HEAD (`3404c4d`).

## Test Results

| Metric | Value |
|--------|-------|
| Test files | 187 passed, 2 skipped (189 total) |
| Tests | 4,571 passed, 22 skipped (4,593 total) |
| Failures | 0 |
| Duration | ~19 minutes (1,141s) |

### Skipped Tests (expected)

All 22 skipped tests require `TOMEFS_BACKEND=tomefs` (they test tomefs-specific behavior not present in MEMFS):

- `tests/adversarial/allocate-mmap.test.ts` — 13 tests (allocate + mmap stream_ops)
- `tests/conformance/enametoolong.test.ts` — 8 tests (ENAMETOOLONG enforcement)
- `tests/conformance/mkdir.test.ts` — 1 test (mkdir ENAMETOOLONG variant)

## CI Status

All 10 most recent CI runs on GitHub are green, including runs on `main` and feature branches. The latest merged PRs:

- `#354` — Make fsync atomic via syncAll instead of separate writes
- `#353` — Fix PreloadBackend flush losing writes during async gap
- `#358` — Optimize PreloadBackend.cleanupOrphanedPages
- `#355` — Optimize cleanupOrphanedPages from O(total_pages) to O(unique_paths)
- `#351` — Batch stale tail page writes during dirty recovery

## HEAD vs Local main

HEAD is detached at `3404c4d` (tip of remote main). Local `main` branch is at `d3d325f` (9 commits behind). The test run was against the latest code.

## Environment

- Node.js via vitest v3.2.4
- Linux 6.18.5, single-threaded (no SharedArrayBuffer — SAB bridge tests use Worker threads)
- PGlite tests (~24 files) dominated runtime at ~3s per test case

## Next Steps

No action required — the test suite is healthy. To run the tomefs-specific skipped tests, use:

```bash
TOMEFS_BACKEND=tomefs npm test
```
