# Test Regression Report — 2026-06-27

## Summary

**No test failures.** Both backends pass the full suite cleanly.

| Backend | Files | Passed | Skipped | Duration |
|---------|-------|--------|---------|----------|
| MEMFS (default) | 185 passed, 2 skipped | 4513 | 22 | 1536s |
| tomefs (`TOMEFS_BACKEND=tomefs`) | 187 passed | 4531 | 4 | 1498s |

## Findings

### 1. mmapAlloc stderr noise (98 warnings)

**Severity:** Low — cosmetic, no functional impact.

During the MEMFS fuzz test run, `tests/fuzz/differential.test.ts` emits 98 lines
of stderr:

```
Aborted(internal error: mmapAlloc called but `emscripten_builtin_memalign` native symbol not exported)
```

**Root cause:** The Emscripten test harness (`tests/harness/emscripten_fs.mjs`)
does not export the `emscripten_builtin_memalign` native symbol. When fuzz tests
exercise `mmap` operations against MEMFS, the underlying `stream_ops.mmap` call
aborts. The fuzz test code (`differential.test.ts:843-881`) already catches this
and falls back to positional `FS.read`/`FS.write`, so tests pass.

**Fix options:**
1. **Suppress at test level** — redirect stderr for mmap-related fuzz seeds, or
   skip mmap ops when running against MEMFS. Simplest, but hides a real
   limitation.
2. **Export the symbol from the harness** — rebuild the Emscripten test WASM
   module with `-sEXPORTED_FUNCTIONS=['_emscripten_builtin_memalign',...]`.
   This would let MEMFS mmap actually work in tests, improving differential
   coverage. Requires touching `scripts/build-harness.sh`.
3. **Do nothing** — the warnings are harmless stderr. Tests pass. CI passes.

**Recommendation:** Option 2 is the best long-term fix since it also improves
mmap differential test coverage for MEMFS. Option 3 is acceptable if the
harness build is painful to change.

### 2. Intentionally skipped tests

All skips are gated by environment variable and represent backend-specific
behavior differences — not missing functionality.

#### MEMFS-only skips (22 tests, active when `TOMEFS_BACKEND` is set):

| File | Count | Reason |
|------|-------|--------|
| `allocate-mmap.test.ts` | 13 | Tests tomefs-specific `allocate()` and mmap; MEMFS lacks these |
| `enametoolong.test.ts` | 8 | ENAMETOOLONG (NAME_MAX=255) enforced by tomefs, not MEMFS |
| `mkdir.test.ts` | 1 | Same ENAMETOOLONG reason |

#### tomefs-only skips (4 tests, active when `TOMEFS_BACKEND` is set):

| File | Count | Reason |
|------|-------|--------|
| `rename.test.ts` | 2 | Root rename produces EXDEV under tomefs mount semantics |
| `readdir.test.ts` | 2 | Root/dev listings differ under tomefs mount point |

### 3. No action needed

All tests pass. All skips are intentional. CI configuration
(`.github/workflows/ci.yml`) already runs both MEMFS and tomefs backends,
plus PreloadBackend and small-cache stress variants.

## Plan: Address mmapAlloc stderr noise

If deemed worth fixing, here is the implementation plan:

### Step 1: Update harness build script

**File:** `scripts/build-harness.sh`

Add `_emscripten_builtin_memalign` to the exported functions list when compiling
the test harness WASM module. This is the Emscripten internal allocator needed
by `mmapAlloc`.

### Step 2: Verify MEMFS mmap works in harness

After rebuilding, confirm that `tests/fuzz/differential.test.ts` no longer emits
the abort messages on stderr, and that the mmap code path in the fuzz tests
actually exercises real MEMFS mmap (not the fallback).

### Step 3: Remove the try-catch fallback (optional)

**File:** `tests/fuzz/differential.test.ts` (lines 843-881)

If MEMFS mmap now works in the harness, the try-catch fallback for mmap
operations is no longer needed for differential correctness. However, keeping it
is harmless and provides defense-in-depth if the harness is rebuilt without the
symbol in the future.

### Estimated effort

Small — the build script change is a one-liner, plus a harness rebuild and test
verification. No production code changes needed.
