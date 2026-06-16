# Test Regression Audit — 2026-06-16

## Summary

**No regressions found.** The full test suite passes locally and CI has been green for 11 consecutive runs (since 2026-06-12).

## Local Test Results

| Metric | Value |
|--------|-------|
| Test files | 178 passed, 2 skipped (tomefs-only, by design) |
| Tests | 4,314 passed, 22 skipped |
| Duration | 1,564s (~26 min) |
| Failures | **0** |

The 22 skipped tests are guarded by `TOMEFS_BACKEND=tomefs` — they test tomefs-specific features (allocate/mmap stream_ops, ENAMETOOLONG enforcement) that don't apply to the default MEMFS harness. All 22 pass when run with `TOMEFS_BACKEND=tomefs`.

## CI Status

Last 30 runs: 28 passed, 2 failed (both resolved). The most recent 11 runs are all green.

### Historical CI Failure: Run #27424214224 (2026-06-12)

**Commit:** `27f86fd` — "Add fsync stream_op for per-file durability (#335)"
**Failed job:** "Full test suite (Node 20)" — the "Full test suite (tomefs)" step
**Root cause:** The `restoreTree` function was unconditionally setting `_metaDirty=false` after crash recovery, so corrected file sizes were never persisted. This caused repeated recovery on every mount, and under certain timing/ordering conditions on Node 20 (but not Node 22), triggered a test failure.
**Fix:** `b1d5cb8` — "Persist corrected metadata after restoreTree size recovery (#339)" marks size-corrected nodes as dirty so the correction is persisted on the next `syncfs`.

The other CI failure (run on branch `claude/adoring-ride-m1hwhe`) was a TS6133 unused parameter error, fixed in its follow-up commit.

## Remaining Risk Areas

While there are no active regressions, the audit identified one area worth monitoring:

### 1. restoreTree recovery path is lightly fuzz-tested

The `restoreTree` bug (fixed in #339) was a subtle interaction: crash recovery corrected file sizes but didn't persist the correction, creating a loop of re-recovery on every mount. The fix added 9 targeted adversarial tests, but the recovery path isn't yet covered by the differential fuzz suite (`tests/fuzz/differential.test.ts`), which only compares FS operations against a reference implementation — it doesn't exercise mount/remount cycles.

**Recommendation:** Add fuzz seeds that include mount→write→crash→remount→verify cycles to `differential.test.ts`, particularly with tiny caches where the recovery code is most sensitive to ordering. This would catch regressions like #339 earlier.

**Files involved:**
- `src/tomefs.ts:1326-1387` — `restoreTree` recovery logic
- `tests/adversarial/restore-metadata-persistence.test.ts` — existing targeted tests
- `tests/fuzz/differential.test.ts` — fuzz suite to extend

### 2. Node 20 vs Node 22 behavioral difference

The #335 CI failure only manifested on Node 20, not Node 22. The root cause was a real bug (not a flake), but the Node-version-specific manifestation suggests the two runtimes have subtly different IDB/async scheduling behavior. This hasn't caused further issues since the fix, but it's worth noting that:
- The full suite only runs on Node 20 and 22 in CI
- Node 20 reaches EOL 2026-04-30 (already past) — consider dropping it from CI

**Files involved:**
- `.github/workflows/ci.yml` — CI matrix configuration

## No Action Required

The codebase is in a healthy state. The single real bug found in recent CI (#339) has already been fixed and tested. The recommendations above are preventive hardening, not urgent fixes.
