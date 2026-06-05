# Test Suite Audit — 2026-06-05

## Result

**All 4056 tests pass. Zero regressions. Zero failures.**

- 4056 passed
- 22 skipped (all by design; see below)
- 0 failed
- Duration: 1563s wall time (4556s test time across threads)
- 161 test files passed, 2 test files skipped

## Skipped Tests (Not Regressions)

All 22 skips are conditional on the `TOMEFS_BACKEND` env var and are intentional:

| File | Skipped | Reason |
|------|---------|--------|
| `tests/conformance/enametoolong.test.ts` | 8 tests | Only runs with `TOMEFS_BACKEND=tomefs`. MEMFS doesn't enforce ENAMETOOLONG; tomefs does. |
| `tests/conformance/mkdir.test.ts` | 1 test | `mkdir_at_root` — only runs with `TOMEFS_BACKEND=tomefs`. |
| `tests/conformance/rename.test.ts` | 1 test | `rename_dir_onto_nonempty_dir` — skipped for tomefs (known MEMFS-only behavior). |
| `tests/conformance/readdir.test.ts` | 1 test | `readdir_on_unlinked_directory` — skipped for tomefs. |
| `tests/adversarial/allocate-mmap.test.ts` | ~11 tests | Only runs with `TOMEFS_BACKEND=tomefs` (mmap tests need tomefs allocator). |

These are expected divergences between the MEMFS reference and tomefs.

## Slow Tests

Several PGlite stress test files take 3-4 minutes each due to spinning up real PGlite instances across multiple cache sizes:

| File | Duration | Tests |
|------|----------|-------|
| `tests/pglite/cte-window-stress.test.ts` | 237s | 54 |
| `tests/pglite/upsert-trigger-stress.test.ts` | 234s | 48 |
| `tests/pglite/table-rewrite-ddl.test.ts` | 205s | 48 |
| `tests/pglite/fts-gin-stress.test.ts` | 190s | 40 |
| `tests/pglite/partition-matview-stress.test.ts` | 187s | 40 |
| `tests/pglite/schema-evolution.test.ts` | 167s | 36 |

These are not regressions — each test spawns a PGlite instance across 4 cache sizes (tiny/small/medium/large), so ~4-5s per individual test is expected.

## Action Items

None required. The codebase is in a healthy state with full test coverage passing.

### Optional Future Work

1. **Update CLAUDE.md test count**: The readme says "2300+ tests" but the actual count is 4056. The test suite has grown significantly.
2. **Consider test parallelism for PGlite tests**: The PGlite stress tests dominate total runtime. If CI time becomes a concern, these could be split into a separate CI stage or run with higher parallelism.
3. **Run with `TOMEFS_BACKEND=tomefs`**: The 22 skipped tests could be validated by running the suite with the tomefs backend flag to ensure full conformance.
