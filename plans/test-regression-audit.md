# Test Regression Audit — 2026-06-01

## Summary

**No regressions found.** Both MEMFS-mode and tomefs-mode full test suites pass cleanly.

## Test Runs

### Run 1: Default mode (MEMFS)

```
npm test   # vitest run, no TOMEFS_BACKEND
```

| Metric       | Result                  |
|-------------|------------------------|
| Test Files  | 161 passed, 2 skipped  |
| Tests       | 4056 passed, 22 skipped|
| Failures    | 0                      |
| Duration    | 1575s (~26 min)        |

### Run 2: tomefs mode (`TOMEFS_BACKEND=tomefs`)

Kicked off after Run 1 completed. Covers the same 163 test files but exercises
the tomefs FS implementation instead of MEMFS. The 22 tests that skip in
MEMFS mode (gated on `TOMEFS_BACKEND`) become active in this mode.

```
TOMEFS_BACKEND=tomefs npm test
```

| Metric       | Result                  |
|-------------|------------------------|
| Test Files  | 163 passed, 0 skipped  |
| Tests       | 4074 passed, 4 skipped |
| Failures    | 0                      |
| Duration    | 1529s (~25.5 min)      |

All 163 test files ran (none skipped). The 22 MEMFS-skipped tests now execute
and pass. Only 4 tests skip in this mode (preload-backend-specific).

### Run 3: Previously-skipped tests only

```
TOMEFS_BACKEND=tomefs npx vitest run \
  tests/adversarial/allocate-mmap.test.ts \
  tests/conformance/enametoolong.test.ts \
  tests/conformance/mkdir.test.ts
```

| Metric | Result       |
|--------|-------------|
| Tests  | 31 passed   |
| Failures | 0         |

## Skipped Tests (22 total, all intentional)

| File | Skipped | Reason |
|------|---------|--------|
| `tests/adversarial/allocate-mmap.test.ts` | 13 | Requires `TOMEFS_BACKEND=tomefs` — tests allocate() and mmap() stream_ops only available in tomefs |
| `tests/conformance/enametoolong.test.ts` | 8 | Requires `TOMEFS_BACKEND=tomefs` — MEMFS doesn't enforce ENAMETOOLONG |
| `tests/conformance/mkdir.test.ts` | 1 | One test gated on `TOMEFS_BACKEND` (WASMFS-only upstream behavior) |

All 22 skipped tests pass when `TOMEFS_BACKEND=tomefs` is set. This is by
design — these tests exercise tomefs-specific behavior that MEMFS doesn't
implement.

## Observations

1. **No code changes needed.** The codebase is in a clean state.

2. **Build is clean.** `npm run build` (tsc) succeeds with no errors or
   warnings.

3. **Test performance.** The full suite takes ~26 minutes. The slowest
   categories are:
   - PGlite stress tests (~3-5 min per file, 48 tests each at 4 cache levels)
   - Fuzz differential tests (~2-3 min per file, many seeds)
   - Conformance/adversarial tests are fast (<2s per file)

4. **CI coverage is comprehensive.** The CI pipeline (`.github/workflows/ci.yml`)
   runs five stages:
   - Build + type-check (Node 20, 22)
   - Smoke tests (MEMFS, tomefs, preload — `@fast` only)
   - Full suite (MEMFS, tomefs)
   - PreloadBackend conformance (conformance, workload, adversarial)
   - Small-cache stress (tomefs, 4-page cache — conformance, workload,
     adversarial, fuzz, integration)

5. **Emscripten mmap warnings are expected.** The output contains repeated
   `Aborted(internal error: mmapAlloc called but emscripten_builtin_memalign
   native symbol not exported)` messages — these are the expected MEMFS
   behavior that tomefs's mmap implementation works around. The tests that
   produce these messages still pass.

## Action Items

None. The test suite is green across all configurations.
