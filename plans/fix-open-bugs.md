# Plan: Fix Three Latent Bugs on Main

## Status

The full test suite (2069 tests, all configurations) passes on main. All CI
checks are green. However, three open PRs identify real bugs in the current
codebase that are not covered by existing tests. Each PR adds both the fix and
the regression test that exposes it.

## Bug 1: Rename Moves Pages Before Writing Metadata (Data Loss on Crash)

**Severity**: Critical — permanent data loss on crash during rename
**PR**: #179 (`claude/fix-rename-crash-safety-ordering`)
**File**: `src/tomefs.ts` lines 422-438, 584-646

### What's wrong

`rename()` calls `pageCache.renameFile(old, new)` on line 424 BEFORE
`backend.writeMeta(new, ...)` on line 431. A crash (tab close, OOM kill, power
loss) between these two operations leaves pages at `newPath` with no metadata —
permanently unreachable and leaked storage.

The same ordering problem exists in `renameDescendantPaths()` (line 590):
`pageCache.renameFile()` is called inline during the tree walk, before the
batched `backend.writeMetas()` call on line 642-643. A crash during directory
rename can lose child files.

Note: `unlink()` already uses the correct metadata-first ordering for the same
reason (see comment at `tomefs.ts:504-509`), but `rename` was not updated.

### Fix (from PR #179)

1. **File rename**: move `backend.writeMeta(new)` before
   `pageCache.renameFile()`. Worst case on crash: stale duplicate at old path,
   cleaned up by orphan cleanup on next syncfs.
2. **Directory rename**: defer `backend.deleteMeta(oldDirPath)` until after all
   descendant processing. In `renameDescendantPaths`, collect page renames into
   a deferred list, execute `backend.writeMetas()` first, then do page renames,
   then `backend.deleteMetas()`.
3. **5 new adversarial tests** in `rename-crash-mid-operation.test.ts` using a
   `CrashAfterNOps` fake backend to simulate crashes at every operation
   boundary.

## Bug 2: `allocate()` Doesn't Mark Metadata Dirty (Size Lost After Incremental Syncfs)

**Severity**: High — file size lost after crash following allocate + syncfs
**PR**: #178 (`claude/fix-tiny-cache-smoke-and-writev-conformance`)
**File**: `src/tomefs.ts` lines 761-766

### What's wrong

`allocate()` (which implements `posix_fallocate`) calls `resizeFileStorage()`
but never calls `markMetaDirty()`. After a prior `syncfs` clears dirty flags,
a subsequent `allocate()` extends the file but the incremental syncfs path
skips persisting the new size (it's not in `dirtyMetaNodes`). After crash +
remount, `restoreTree` recovers a page-boundary-rounded size instead of the
exact allocated size.

This matters because Postgres frequently extends WAL and heap files via
`posix_fallocate`.

### Fix (from PR #178)

```typescript
allocate(stream: any, offset: number, length: number) {
  const node = stream.node;
  const oldSize = node.usedBytes;
  resizeFileStorage(node, Math.max(oldSize, offset + length));
  if (node.usedBytes !== oldSize) {
    markMetaDirty(node);
  }
},
```

1 regression test verifying non-page-aligned size survives allocate + syncfs +
remount. PR also removes `@fast` tag from the tiny-cache PGlite persistence
test that consistently times out.

## Bug 3: Metadata Dirty Flags Cleared Before `syncAll()` Succeeds (Data Loss on Failed Sync)

**Severity**: High — silent data loss when backend write fails
**PR**: #175 (`fix-syncfs-dirty-flag-loss`)
**File**: `src/tomefs.ts` line 1163

### What's wrong

In the incremental syncfs path, `node._metaDirty = false` is set on line 1163
BEFORE `backend.syncAll()` is called on line 1172. If `syncAll` throws (IDB
quota exceeded, network error, tab close mid-write), metadata dirty flags are
already cleared. The next syncfs won't retry the metadata writes.

Note: `pageCache.commitDirtyPages()` (line 1173) already uses two-phase
commit correctly (added in PR #174, merged). But the metadata dirty flags were
not included in this pattern — they're cleared eagerly in the loop before the
`syncAll` call.

### Fix (from PR #175)

Move `node._metaDirty = false` to AFTER `backend.syncAll()` succeeds:

```diff
-              node._metaDirty = false;
             }
             backend.syncAll(dirtyPages, metaBatch);
             pageCache.commitDirtyPages(dirtyPages);
             needsCleanMarker = false;
+            for (const node of dirtyMetaNodes) {
+              node._metaDirty = false;
+            }
             dirtyMetaNodes.clear();
```

4 new adversarial tests in `syncfs-write-failure.test.ts` covering both
incremental and full tree walk paths, including the critical scenario: dirty
pages surviving cache eviction after a failed syncfs + retry.

## Recommended Fix Order

1. **Bug 3** (metadata dirty flags) — smallest diff, most isolated. Only moves
   one line and adds a loop. No interactions with the other two fixes.
2. **Bug 2** (allocate markMetaDirty) — 3-line fix in `allocate()`. Fully
   independent of the other two.
3. **Bug 1** (rename crash safety) — largest change, touches rename and
   renameDescendantPaths. Should go last since it restructures operation
   ordering in two functions.

All three fixes are independent and could also be applied in parallel, but
sequential merging makes review easier.

## Verification

For each fix, run:
```bash
npm run build
npx tsc --noEmit
npm test
TOMEFS_BACKEND=tomefs npm test
TOMEFS_BACKEND=tomefs TOMEFS_MAX_PAGES=4 npx vitest run tests/conformance/ tests/adversarial/ tests/fuzz/ tests/integration/
```
