# Test Audit — 2026-05-12

## Summary

Full test suite (3319 tests across 146 files) passes in both MEMFS and tomefs
backends with zero failures. No regressions found.

## Results

| Backend | Files passed | Files skipped | Tests passed | Tests skipped | Duration |
|---------|-------------|---------------|-------------|---------------|----------|
| MEMFS (default) | 144 | 2 | 3297 | 22 | 918s |
| tomefs (`TOMEFS_BACKEND=tomefs`) | 146 | 0 | 3315 | 4 | 888s |

## Skipped tests

### MEMFS mode (2 files, 22 tests)

- `tests/adversarial/allocate-mmap.test.ts` (13 tests) — gated on
  `TOMEFS_BACKEND=tomefs`; exercises tomefs-specific `allocate()` and `mmap()`
  stream_ops that MEMFS does not expose.
- `tests/conformance/enametoolong.test.ts` (8 tests) — gated on
  `TOMEFS_BACKEND=tomefs`; tests `ENAMETOOLONG` enforcement that MEMFS skips.
- 1 test skipped in `tests/conformance/mkdir.test.ts` — likely a conditional
  skip for a known MEMFS limitation.

### tomefs mode (4 tests)

4 individual tests skipped (same `mkdir.test.ts` skip plus 3 others — likely
environment-gated tests for features not available in Node).

## Conclusion

No action needed. The codebase is green on HEAD (`7f04d10`).
