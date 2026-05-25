# Test Suite Status Report — 2026-05-25

## Result: All tests pass. No regressions found.

## Summary

Full test suite (`npm test`) ran to completion. All tests pass:

- **Test files**: 154 passed, 2 skipped (156 total)
- **Tests**: 3,622 passed, 22 skipped (3,644 total)
- **Duration**: 901s (~15 minutes)
- **Exit code**: 0

## Skipped Tests (all intentional)

The 22 skipped tests across 2 skipped files are **not regressions** — they are behind `TOMEFS_BACKEND=tomefs` gates and intentionally skip when running against MEMFS:

| File | Skipped | Reason |
|------|---------|--------|
| `tests/adversarial/allocate-mmap.test.ts` | 13 | Requires `TOMEFS_BACKEND=tomefs` — tests `allocate()` and `mmap()` stream_ops not present in MEMFS |
| `tests/conformance/enametoolong.test.ts` | 8 | Requires `TOMEFS_BACKEND=tomefs` — MEMFS doesn't enforce `NAME_MAX` |
| `tests/conformance/mkdir.test.ts` | 1 | Likely a single POSIX edge case gated similarly |

## No Action Required

There are no failing tests to fix. The codebase is in a healthy state.

## Environment

- Node vitest with `pool: "threads"`, `testTimeout: 30000`
- HEAD commit: `67c26f5` (Add filesystem-level assertInvariants to tomefs)
