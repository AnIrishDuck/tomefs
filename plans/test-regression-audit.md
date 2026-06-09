# Test Regression Audit — 2026-06-09

## Summary

Full test suite (`vitest run`) passes cleanly: **4056 passed, 0 failed, 22 skipped** across 163 test files (161 run, 2 skipped). No regressions detected.

## Run Details

- Duration: 1375s (~23 min)
- Environment: Linux, Node.js, no `TOMEFS_BACKEND` env var set
- Commit: `b428b56` (HEAD — "Add filesystem-level getStats/resetStats to tomefs")

## Skipped Tests (expected)

| File | Count | Reason |
|------|-------|--------|
| `tests/adversarial/allocate-mmap.test.ts` | 13 | Requires `TOMEFS_BACKEND` env var |
| `tests/conformance/enametoolong.test.ts` | 8 | Requires `TOMEFS_BACKEND` env var |
| `tests/conformance/mkdir.test.ts` | 1 | Single skip within the file |

All three are gated on `TOMEFS_BACKEND=tomefs` and skip intentionally in the default MEMFS test path.

## Stderr Warnings (non-failures)

112 occurrences of:
```
Aborted(internal error: mmapAlloc called but `emscripten_builtin_memalign` native symbol not exported)
```

These appear exclusively in `tests/fuzz/differential.test.ts` stderr output. They fire when a fuzz seed triggers `mmap()` on the Emscripten module, which lacks the `emscripten_builtin_memalign` export. The tests pass despite the abort — the test harness catches the error and the fuzz driver continues to the next operation.

### Potential improvement

The mmap fallback path in the Emscripten FS harness could suppress or handle this abort more gracefully. Two options:

1. **Patch the test WASM module** to export `emscripten_builtin_memalign` (a stub returning null would suffice since tomefs implements mmap in JS).
2. **Filter the abort in the test harness** — the `Aborted()` call currently writes to stderr but doesn't crash the process because the fuzz runner's error boundary catches it. Adding a stderr filter would reduce noise without masking real issues.

Neither is urgent since the tests pass correctly, but option 1 would make the fuzz output cleaner and avoid false-alarm noise in CI logs.

## Conclusion

No action needed — the test suite is fully green. The skipped tests and stderr warnings are all known/expected behavior.
