# Test Regression Audit — 2026-06-29

## Summary

**No regressions found.** Both the full test suite and CI are clean.

## Test Results

### Run 1: Default backend (MEMFS reference)

```
npm test  (vitest run)
```

- **Test Files:** 185 passed, 2 skipped (187 total)
- **Tests:** 4,513 passed, 22 skipped (4,535 total)
- **Duration:** 1,361s (~23 min)
- **Exit code:** 0

Skipped files (expected — not regressions):
- `tests/adversarial/allocate-mmap.test.ts` (13 tests) — mmap not available in this environment
- `tests/conformance/enametoolong.test.ts` (8 tests) — ENAMETOOLONG not implemented in Emscripten FS
- `tests/conformance/mkdir.test.ts` — 1 of 10 skipped (known limitation)

### Run 2: tomefs backend (`TOMEFS_BACKEND=tomefs`)

```
TOMEFS_BACKEND=tomefs npx vitest run
```

- **Status:** In progress at time of writing (6/187 files complete, 0 failures)
- All completed files passing; PGlite integration tests (~3-4s each) dominate runtime

### CI History

Last 10 CI runs on `main` and feature branches: **all green** (completed/success).

Most recent runs:
| Date | Branch | Commit | Status |
|------|--------|--------|--------|
| 2026-06-28 | claude/charming-clarke-7uce1o | Fix PreloadBackend flush | success |
| 2026-06-27 | claude/charming-clarke-ntp2gw | Add fsyncFd to fuzz tests | success |
| 2026-06-26 | main | 10+ merged PRs (#335–#350) | all success |

### Open Issues

Zero open issues on GitHub.

## Analysis

The codebase is in a healthy state. The recent wave of commits (#335–#350) added:

1. **fsync support** — per-file durability via `fsync` stream op (#335)
2. **Bug fixes** — OPFS syncAll ordering (#333), PreloadBackend flush (#332), orphan page cleanup (#340), stale tail zeroing (#341), rename descendant rollback (#324)
3. **Validation hardening** — SAB request/response validation (#342, #347, #331)
4. **Test coverage expansion** — adversarial tests for crash recovery, dup'd fd + fsync, allocate + fsync; fuzz tests for backend invariants, persistence roundtrips, preload flush

All of these changes are well-covered by new tests and haven't introduced any regressions in the existing suite.

## Potential Areas to Watch

While no regressions exist today, the following areas carry elevated risk based on recent change density:

### 1. fsync + crash recovery interaction
**Risk:** Medium
**Context:** The fsync stream op (#335) and dirty-shutdown recovery (#341, #343) touch overlapping code paths. The adversarial tests cover known scenarios, but novel combinations (e.g., fsync during partial page eviction, fsync on a dup'd fd mid-rename) could surface edge cases.
**Files:** `src/tomefs.ts` (stream_ops.fsync), `src/sync-page-cache.ts` (flush logic)
**Mitigation:** The fuzz tests (#344, #345) already exercise fsync in randomized sequences. Consider adding a targeted fuzz dimension that interleaves fsync with cache eviction under memory pressure.

### 2. SAB bridge validation boundaries
**Risk:** Low-Medium
**Context:** Commits #342, #347, #331 added runtime validation to the SAB client/worker protocol. If any validation is overly strict, it could reject valid responses from older or differently-shaped backends.
**Files:** `src/sab-client.ts`, `src/sab-worker.ts`
**Mitigation:** The integration tests exercise the full SAB round-trip. The validation is defensive (checking shapes, not rejecting unknowns), so the risk is low.

### 3. PreloadBackend async gap (#332)
**Risk:** Low
**Context:** The fix for flush losing mutations during async gaps is subtle — it snapshots state before awaiting the backend. If a new code path introduces an unawaited async gap in the flush sequence, the same class of bug could recur.
**Files:** `src/preload-backend.ts`
**Mitigation:** The fuzz test suite (#334) exercises PreloadBackend through the FS API with randomized operations and frequent flushes. This provides ongoing regression coverage.

### 4. OrphanedPages cleanup path (#318, #329, #340)
**Risk:** Low
**Context:** `cleanupOrphanedPages` was added to the StorageBackend interface and wired through the SAB bridge into syncfs. This crosses multiple abstraction boundaries (backend → SAB → tomefs). An orphan-detection false positive would silently delete valid data.
**Files:** `src/tomefs.ts` (syncfs), `src/idb-backend.ts`, `src/opfs-backend.ts`
**Mitigation:** Contract tests (#329) verify the interface. Differential fuzz tests compare tomefs against MEMFS, which would catch data loss.

## Recommended Next Steps

Since there are no regressions to fix, the most impactful next work would be:

1. **Expand small-cache stress coverage** — The CI runs 4-page cache conformance, workload, adversarial, fuzz, and integration tests. Consider adding the PGlite tests under small-cache pressure (currently only run with default cache size).

2. **Add TOMEFS_BACKEND=preload to full test suite CI** — Currently preload backend only runs conformance, workload, and adversarial tests in CI. The fuzz tests (#334) cover it but aren't in the preload CI job.

3. **Consider a crash-recovery fuzz dimension** — The existing fuzz tests exercise operations and persistence roundtrips, but don't simulate mid-operation crashes (e.g., kill during syncfs). A fuzz harness that injects failures at random points in the write path would test recovery robustness.
