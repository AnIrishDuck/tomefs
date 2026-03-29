# Fix: Duplicate `readMetas` key in backend-contract test

## Status
No test regressions — all 1562 tests pass (22 skipped). One code quality issue found.

## Issue
**File:** `tests/unit/backend-contract.test.ts`, lines 53 and 56

The `wrapSync()` helper that creates an async `StorageBackend` adapter from a
`SyncMemoryBackend` defines `readMetas` twice in the same object literal:

```ts
readMetas: async (ps) => sync.readMetas(ps),   // line 53
writeMeta: async (p, m) => sync.writeMeta(p, m),
writeMetas: async (e) => sync.writeMetas(e),
readMetas: async (ps) => sync.readMetas(ps),   // line 56 — DUPLICATE
```

Vite/esbuild emits a warning for this at test startup:
```
warning: Duplicate key "readMetas" in object literal
```

### Impact
- **Functional:** None currently — both definitions are identical, and the
  second silently overwrites the first per JS semantics.
- **Maintenance risk:** If someone edits only one copy, the other is silently
  ignored, leading to confusing test failures.
- **CI noise:** The warning clutters test output.

## Fix
Delete line 56 (the duplicate `readMetas`). The first definition on line 53 is
correct and sufficient.

```diff
     readMetas: async (ps) => sync.readMetas(ps),
     writeMeta: async (p, m) => sync.writeMeta(p, m),
     writeMetas: async (e) => sync.writeMetas(e),
-    readMetas: async (ps) => sync.readMetas(ps),
     deleteMeta: async (p) => sync.deleteMeta(p),
```

## Verification
Run `npm test` and confirm the Vite duplicate-key warning is gone and all tests
still pass.
