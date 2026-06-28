# Test Suite Health Report — 2026-06-28

## Status: No Regressions

Full test suite (4513 tests across 185 files) passes. CI history confirms
the last 10 runs on `main` are all green. Two test files are skipped
(expected). 22 individual tests skipped (expected — e.g., `mkdir` edge case).

## Issue: mmapAlloc stderr noise in fuzz tests

### Symptom

Fuzz/differential tests emit dozens of stderr warnings:

```
Aborted(internal error: mmapAlloc called but `emscripten_builtin_memalign` native symbol not exported)
```

Tests still pass — the fuzz harness catches the `RuntimeError` and falls
back to `FS.read()`/`FS.write()`. But the noise obscures real stderr output
and could mask future genuine warnings.

### Root Cause

The WASM test harness (`tests/harness/emscripten_fs.mjs`) was compiled
without exporting `emscripten_builtin_memalign`. The `mmapAlloc` function
(line 1268) is a stub that calls `abort()`. When MEMFS's `stream_ops.mmap`
(line 1543) invokes `mmapAlloc(length)` for `MAP_PRIVATE` copies, it
triggers the abort, which prints to stderr before throwing.

The fuzz generator produces `mmapRead`/`mmapWrite` operations
(`tests/fuzz/differential.test.ts`, lines ~509-533). These operations call
`stream.stream_ops.mmap()` on the MEMFS side, hitting the stub. The
try/catch at lines ~846-879 catches the abort and falls back.

tomefs's own `mmap` (`src/tomefs.ts:1072-1084`) returns a fresh
`Uint8Array` copy from pages — no `mmapAlloc` needed. The differential
comparison remains valid since both sides use semantically equivalent
fallback operations.

### Does this mask real bugs?

No. The fallback path exercises the same read/write semantics. tomefs's
mmap path is independently tested in `tests/adversarial/mmap-*.test.ts`
and `tests/adversarial/msync*.test.ts`.

### Fix Options

**Option A (recommended): Recompile the WASM harness** — Add
`_emscripten_builtin_memalign` and `_free` to `EXPORTED_FUNCTIONS` in
`scripts/build-harness.sh`. This makes MEMFS mmap actually work, giving a
stronger differential comparison (the MEMFS side would exercise real mmap
rather than falling back to read/write).

Requires emsdk. Verify with:
```bash
# In build-harness.sh, add to EXPORTED_FUNCTIONS:
# "_emscripten_builtin_memalign", "_free"
# Then rebuild:
./scripts/build-harness.sh
npm test  # verify mmap operations no longer fall back
```

**Option B (quick fix): Suppress stderr around MEMFS mmap calls** —
In `tests/fuzz/differential.test.ts`, temporarily redirect
`Module.printErr` / `console.error` around the MEMFS mmap call. No build
changes needed.

**Option C (not recommended): Skip mmap ops on MEMFS** — Remove mmap
weight from the fuzz generator. Loses differential coverage of tomefs's
mmap path.

### Recommendation

Start with Option B to eliminate the noise immediately. Then pursue Option A
when emsdk is available, since it upgrades the differential test from
"mmap falls back to read/write" to "mmap vs mmap" — a strictly stronger
comparison.

## Observation: Slow PGlite test files

Not a regression — these are inherently slow due to PGlite startup overhead
(~4s per parameterized variant). But worth noting for CI optimization:

| File | Duration | Tests |
|------|----------|-------|
| `pglite/join-stress.test.ts` | 210s | 48 |
| `pglite/cursor-stress.test.ts` | 175s | 40 |
| `pglite/dirty-shutdown.test.ts` | 132s | 26 |
| `pglite/preload-backend.test.ts` | 75s | 14 |
| `pglite/temp-table-lifecycle.test.ts` | 46s | 9 |

Total PGlite wall time: ~638s (~42% of the 1530s total run). The existing
`@fast` tag system already lets `npm run test:fast` skip the slow variants.
No action needed unless CI time becomes a bottleneck.
