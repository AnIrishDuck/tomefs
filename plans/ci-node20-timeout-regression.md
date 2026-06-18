# CI Regression: Full test suite timeout on Node 20

## Problem

The most recent CI run on `main` (run `27424214224`, commit `27f86fd` — "Add fsync stream_op for per-file durability #335") has one failing job: **Full test suite (Node 20)**. The `Full test suite (tomefs)` step ran for ~52 minutes before the process died, while the identical step on Node 22 completed in ~14 minutes. All other CI jobs passed (build, type-check, smoke tests, small-cache stress, Node 22 full suite, PreloadBackend conformance).

This is NOT a test assertion failure. All tests that complete pass. The issue is the test process itself dying or timing out during the Node 20 tomefs run.

## Root cause analysis

The full test suite (`npm test` with `TOMEFS_BACKEND=tomefs`) runs ~2600 tests across 156 files using vitest's `pool: "threads"` (default parallelism). The PGlite tests are the bottleneck:

1. **Each PGlite test instantiates a full Postgres WASM binary** (~1-2s per test, ~800MB RSS per worker)
2. **Vitest's thread pool spawns many test files in parallel**, each loading PGlite independently
3. **The threaded pool shares memory across workers**, amplifying memory pressure
4. **PGlite tests are parameterized across 4 cache sizes** (tiny/small/medium/large), multiplying the count

On Node 22, V8 improvements handle this memory/scheduling pressure. On Node 20, the workers compete for memory and CPU, causing extreme slowdowns where vitest produces no output for minutes at a time while workers thrash.

### Evidence

- **Local reproduction (Node 22):** Running the full suite serially takes ~10 min total. With the default thread pool, vitest stalls for 1-3 minutes between output bursts while PGlite workers initialize. The non-PGlite tests (154 files, 3453 tests) complete in ~3 minutes with `pool=forks, maxForks=2`.
- **CI timing:** Node 20 MEMFS run completed in 14 min; Node 20 tomefs run died after 52 min. Node 22 tomefs run completed in 14 min.
- **No test failures:** Across all local runs, 0 test assertions failed. All 3453 non-PGlite tests pass. All PGlite tests that complete also pass.
- **Memory:** PGlite test workers reach 1-3 GB RSS. Multiple workers running in the threaded pool can exceed CI runner memory (7 GB on `ubuntu-latest`).

## Proposed fix

### Option A: Limit parallelism for CI (recommended)

Add `--pool=forks --poolOptions.forks.maxForks=2` to the full-tests CI step, or configure vitest to limit concurrency when running PGlite tests. This prevents the memory explosion from too many simultaneous PGlite instances.

**In `ci.yml`:**
```yaml
- name: Full test suite (tomefs)
  run: TOMEFS_BACKEND=tomefs npx vitest run --pool=forks --poolOptions.forks.maxForks=2
```

Trade-off: Slightly slower but deterministic. The forked pool isolates memory per worker and maxForks=2 keeps peak memory under control.

### Option B: Split PGlite tests into a separate CI job

Move PGlite tests (`tests/pglite/**`) to a dedicated job with `maxForks=1` or sequential execution, while the main full-tests job excludes them. This gives PGlite tests more headroom and lets the main suite run fast.

### Option C: Add a timeout-minutes to the full-tests job

```yaml
full-tests:
  timeout-minutes: 30
```

This is a band-aid — it caps the damage but doesn't fix the root cause. The job will still fail on Node 20 if memory pressure is high.

### Option D: Drop Node 20 from the full test matrix

Run only smoke tests on Node 20, full suite on Node 22 only. Node 20 reaches EOL in April 2026 (already past for this project's timeline). This simplifies CI but reduces coverage.

## Recommendation

**Option A** is the simplest fix with no downside. The full test suite already passes on both Node versions when parallelism is controlled. Pair it with Option C as a safety net.

If CI time budget is a concern, Option B gives the best balance: fast main suite + reliable PGlite tests.

## Additional note: PR #340 branch failure

The other CI failure (run `27425762333`, branch `claude/adoring-ride-m1hwhe`) is a TypeScript error in `tests/adversarial/orphan-page-on-create.test.ts:44` — unused variable `tomefs`. This file doesn't exist on `main`, so it's a branch-only issue unrelated to the main regression.

## Test results summary (current main)

| Suite | Files | Tests | Result |
|-------|-------|-------|--------|
| Non-PGlite (conformance, adversarial, fuzz, unit, integration, workload, scribe-data, badfs) | 154 | 3453 passed, 22 skipped | PASS |
| PGlite | 24 | 861 passed | PASS |
| Type-check | — | — | PASS |
| Build | — | — | PASS |

Skipped tests are expected: `enametoolong.test.ts` (8) and `allocate-mmap.test.ts` (13) require `TOMEFS_BACKEND=tomefs` and `TOMEFS_BACKEND=tomefs` respectively.
