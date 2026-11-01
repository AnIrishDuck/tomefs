# Regression Report: 2026-11-01

## Summary

**No regressions found.** All 4752 tests pass on `main` (commit `f76a420`).

## Test Run Details

- **Date**: 2026-11-01
- **Branch**: `main` at `f76a420` ("Add regression report for 2026-10-27: all 4752 tests passing")
- **Result**: 193 test files passed, 2 skipped | 4752 tests passed, 22 skipped | 0 failures
- **Duration**: ~1271s wall clock (~3692s test execution across threads)

## What Was Tested

Full test suite (`npm test`) run as a single invocation via vitest thread pool:

| Category | Result |
|---|---|
| Conformance (POSIX) | All passed |
| Unit tests | All passed |
| Integration tests | All passed |
| Adversarial tests | All passed |
| Fuzz tests | All passed |
| Workload tests | All passed |
| PGlite integration | All passed |
| Scribe-data workloads | All passed |
| BadFS validation | All passed |

## Notes

- Consistent with the previous 27 consecutive clean runs (since 2026-10-01).
- No source code changes since the last regression report.
- No fix plan needed — no regressions to address.
