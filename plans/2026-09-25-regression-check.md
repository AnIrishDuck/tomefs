# Regression Check — 2026-09-25

## Summary

Full test suite ran with **zero regressions**. No fix plan needed.

## Results

| Metric        | Value              |
|---------------|--------------------|
| Test files    | 193 passed, 2 skipped |
| Tests         | 4752 passed, 22 skipped, 0 failed |
| Duration      | 1829s (~30 min)    |
| Exit code     | 0                  |

## Skipped tests

- `tests/conformance/enametoolong.test.ts` (8 tests) — intentionally skipped (known platform limitation)
- 14 additional individual skips across other files (e.g., mkdir edge cases)

## Categories exercised

- Conformance (POSIX FS operations): all passing
- Unit (page cache, backends, SAB bridge): all passing
- Integration (tomefs + SAB + backend): all passing
- Adversarial (page cache seams, rename, truncate, crash): all passing
- Fuzz (differential randomized): all passing
- Workload (simulated PGlite patterns): all passing
- PGlite (SQL-level integration, TOAST, partitions, savepoints, DDL rewrites): all passing
- Scribe-data (app-level workload scenarios): all passing
- BadFS (defect injection validation): all passing

## Conclusion

No action required. The codebase is healthy as of this check.
