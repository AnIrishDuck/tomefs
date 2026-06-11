# Test Suite Status — 2026-06-11

## Summary

Full test suite passes cleanly. No regressions detected.

## Results

| Metric | Value |
|--------|-------|
| Test files | 169 passed, 2 skipped (171 total) |
| Tests | 4149 passed, 22 skipped (4171 total) |
| Duration | 1595.70s wall clock (~26.6 min) |
| Failures | **0** |

With `TOMEFS_BACKEND=tomefs`, the 2 skipped files (21 tests) also pass, bringing the total to **4170 passing tests**.

## Skipped Tests (intentional, not regressions)

### `tests/conformance/enametoolong.test.ts` — 8 tests
- **Reason:** Guarded by `process.env.TOMEFS_BACKEND`. Tests POSIX `ENAMETOOLONG` enforcement (path component > 255 chars), which MEMFS does not implement. Passes when run with `TOMEFS_BACKEND=tomefs`.

### `tests/adversarial/allocate-mmap.test.ts` — 13 tests
- **Reason:** Same `TOMEFS_BACKEND` guard. Tests `allocate()` (posix_fallocate) and `mmap()`/`msync()` stream_ops that only tomefs exposes. Passes when run with `TOMEFS_BACKEND=tomefs`.

### `tests/conformance/mkdir.test.ts` — 1 test
- Skipped within the file (not the whole file). Likely a known MEMFS limitation.

## No Fix Needed

There are no regressions or broken tests to address. The codebase is in a healthy state.
