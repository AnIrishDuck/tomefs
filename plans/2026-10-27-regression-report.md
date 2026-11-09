# Regression Report: 2026-10-27

## Summary

**No regressions found.** All 4752 tests pass on `main` (commit `2845c10`).

## Test Run Details

- **Date**: 2026-10-27
- **Branch**: `main` at `2845c10` ("Add regression report for 2026-10-26: all 4752 tests passing")
- **Result**: 193 test files passed, 2 skipped | 4752 tests passed, 22 skipped | 0 failures
- **Duration**: ~1186s wall clock (~3447s test execution across threads)

## What Was Tested

Full test suite (`npm test`) run as a single invocation via vitest thread pool:

| Category | Result |
|---|---|
| Conformance (POSIX) | All passed |
| Unit (page cache, backends, SAB bridge) | All passed |
| Adversarial (edge cases) | All passed |
| Fuzz (differential, preload flush roundtrip) | All passed |
| Integration (tomefs + SAB + backend, preload) | All passed |
| Workload (PGlite access patterns) | All passed |
| Scribe-data (app-level scenarios) | All passed |
| BadFS (defect injection) | All passed |
| PGlite (SQL-level integration) | All passed |

## Skipped Tests

Same 2 files / 22 tests as prior runs:
- `tests/conformance/enametoolong.test.ts` (8 tests) — ENAMETOOLONG not supported in Emscripten WASM
- `tests/adversarial/allocate-mmap.test.ts` (13 tests + 1 skip in mkdir) — mmap not supported in Emscripten WASM

## Action Items

None. No regressions detected. Codebase is stable.
