# Plan: Suppress mmapAlloc stderr noise in differential fuzz tests

## Problem

The full test suite (3622 tests, all passing) emits 112 lines of stderr noise:

```
Aborted(internal error: mmapAlloc called but `emscripten_builtin_memalign` native symbol not exported)
```

These come from MEMFS's mmap implementation when used as the reference in differential fuzz tests. MEMFS calls `mmapAlloc`, which requires `emscripten_builtin_memalign` — a native symbol not exported in the test harness WASM module (`emscripten_fs.wasm`). tomefs's own mmap implementation (`src/tomefs.ts:1033-1046`) avoids this by returning a fresh `Uint8Array` copy directly.

The fuzz tests already handle this gracefully: `tests/fuzz/differential.test.ts:843-854` wraps the MEMFS mmap call in try/catch and falls back to positional read/write. The Emscripten runtime, however, prints the `Aborted(...)` message to stderr before throwing.

## Impact

- **No correctness issue.** All tests pass. The error is caught and handled.
- **Developer experience.** 112 lines of alarming "Aborted" messages in test output create noise and could mask real issues.
- **CI signal.** stderr noise doesn't fail CI but makes logs harder to read.

## Root cause

In `tests/harness/emscripten-fs.ts`, the WASM module is loaded without exporting `emscripten_builtin_memalign`. Emscripten's MEMFS `mmap` calls `mmapAlloc`, which calls `abort()` when the symbol is missing. The `abort()` function in the Emscripten runtime prints to stderr before throwing.

## Options

### Option A: Suppress stderr in MEMFS mmap fallback paths (recommended)

In `tests/fuzz/differential.test.ts`, temporarily redirect or suppress `console.error`/`process.stderr` around the MEMFS mmap calls in the `mmapRead` and `mmapWrite` cases (lines 843-854 and 863-871). This is scoped narrowly and doesn't affect test correctness.

```typescript
case "mmapRead": {
  const stream = fdStreams?.get(op.fdId);
  if (!stream) return { error: "no-fd" };
  try {
    const origStderr = process.stderr.write;
    process.stderr.write = () => true; // suppress Aborted() noise
    try {
      const mmapResult = stream.stream_ops.mmap(stream, op.length, op.position, 0, 0);
      // ... existing logic
    } finally {
      process.stderr.write = origStderr;
    }
  } catch {
    // ... existing fallback
  }
}
```

**Pros:** Minimal change, targeted suppression, no WASM rebuild needed.
**Cons:** Slightly ugly stderr redirection pattern.

### Option B: Export `emscripten_builtin_memalign` in the test WASM module

Modify the Emscripten build configuration for `emscripten_fs.wasm` to export `emscripten_builtin_memalign`. This would let MEMFS mmap work natively.

**Pros:** Eliminates root cause; MEMFS mmap would work in tests.
**Cons:** Requires rebuilding the WASM module; may have side effects on memory management in the test environment; the WASM module build process isn't documented in CLAUDE.md.

### Option C: Skip mmap ops in MEMFS-side differential testing

Only run mmap/msync ops against tomefs in the differential fuzz tests, since MEMFS can't do them anyway and the fallback is already semantically equivalent to read/write.

**Pros:** Cleanest separation of concerns.
**Cons:** Loses the differential comparison for mmap (though it's already degraded to read/write comparison).

### Option D: Do nothing

The noise is cosmetic. All tests pass. The existing try/catch handles it correctly.

**Pros:** Zero effort.
**Cons:** 112 lines of "Aborted" in every test run.

## Files to modify

| File | Change |
|------|--------|
| `tests/fuzz/differential.test.ts` | Suppress stderr around MEMFS mmap calls (Option A) or skip mmap for MEMFS side (Option C) |
| `tests/fuzz/idb-differential.test.ts` | Same pattern if mmap ops exist there |
| `tests/fuzz/opfs-differential.test.ts` | Same pattern if mmap ops exist there |

## Skipped tests (not regressions)

The 2 skipped test files and 22 skipped individual tests are intentional:

1. **`tests/adversarial/allocate-mmap.test.ts`** (13 tests skipped): Gated on `TOMEFS_BACKEND=tomefs` — only runs when testing the tomefs implementation, not MEMFS.
2. **`tests/conformance/enametoolong.test.ts`** (8 tests skipped): Emscripten doesn't enforce `NAME_MAX` / `PATH_MAX` — documented platform limitation.
3. **`tests/conformance/mkdir.test.ts`** (1 test skipped): Likely a specific edge case not supported by the test environment.

## Recommendation

Option A is the pragmatic choice: suppress stderr in the 2 narrow code paths where MEMFS mmap is known to print noise. This keeps the differential comparison active while silencing the 112 spurious lines. If a WASM module rebuild becomes convenient later, Option B can be pursued as a follow-up.
