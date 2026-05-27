# Test Suite Audit — 2026-05-27

## Result

**All tests pass. No regressions found.**

## Default run (`npm test`)

- Test Files: 154 passed, 2 skipped (156 total)
- Tests: 3622 passed, 22 skipped (3644 total)
- Duration: ~1305s (~22 minutes)
- Exit code: 0

## TOMEFS_BACKEND=tomefs run (skipped tests only)

- Test Files: 3 passed (3)
- Tests: 31 passed (31)
- Duration: ~1.2s
- Exit code: 0

## Skipped tests (all expected)

The 22 skipped tests are gated behind `TOMEFS_BACKEND=tomefs` and all pass
when that env var is set:

| File | Skipped | Reason |
|------|---------|--------|
| `tests/adversarial/allocate-mmap.test.ts` | 13 | tomefs-only `allocate()` and `mmap()` stream_ops |
| `tests/conformance/enametoolong.test.ts` | 8 | tomefs-only ENAMETOOLONG enforcement (MEMFS doesn't enforce) |
| `tests/conformance/mkdir.test.ts` | 1 | tomefs-only mkdir ENAMETOOLONG case |

## HEAD at time of audit

```
67c26f5 Add filesystem-level assertInvariants to tomefs (#303)
```

## Recent commits reviewed for regression risk

| Commit | Description | Risk |
|--------|-------------|------|
| `67c26f5` | Add `assertInvariants()` to tomefs | Low — additive, no logic changes |
| `679005a` | Optimize page cache write paths (2-page specialization, full-page skip) | Medium — touches hot write path |
| `f9d2ea4` | Optimize read hot paths (full-page skip-subarray, 2-page specialization) | Medium — touches hot read path |
| `91e882a` | Add OPFS SAH backend | Low — new backend, no existing code changed |
| `e3f970b` | Optimize memory backend write batching | Low — backend internals |

All passed without issue.

## Conclusion

No action items. The codebase is clean at HEAD.
