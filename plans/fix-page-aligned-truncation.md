# Fix: Sparse page loss on page-aligned truncation

## Problem

Local `main` (c65e7ab) is behind `origin/main` (7137a89) by 11 commits, which
include three bug fixes and eight new test files. The critical regression:

**File size shrinks after syncfs + remount when truncation lands on a page
boundary.**

### Root cause

In `src/tomefs.ts` `resizeFileStorage()`, when truncating to a page-aligned
size (`newSize % PAGE_SIZE === 0`), `zeroTailAfterTruncate` is a no-op because
`tailOffset = 0`. This means the last page before the truncation point is never
loaded into the page cache.

If that page is **sparse** (never materialized — e.g., from a prior `ftruncate`
extend), it doesn't exist in the backend either. On the next `restoreTree`, the
file extent is determined by `maxPageIndex` found in the backend. Since the last
expected page was never written, `restoreTree` sees a smaller extent and the
file silently loses data.

### Reproduction

```
1. Write 1 page (page 0) to /tome/file
2. ftruncate to 3 pages (pages 1, 2 are sparse zeros)
3. ftruncate to 2 pages (page-aligned, page 1 remains sparse)
4. syncfs → backend only has page 0 (page 1 was never dirty)
5. unmount + remount → restoreTree sees maxPageIndex=0 → file size = 8192
6. Expected: 16384
```

Confirmed locally: test fails with `expected 8192 to be 16384`.

## Fix (from commit 50ba1c0 on origin/main)

After the truncation logic in `resizeFileStorage`, materialize a sentinel for
the last page when:
- `neededPages > 0` (file is not empty)
- `newSize % PAGE_SIZE === 0` (page-aligned truncation)
- The last page doesn't exist in the cache

```typescript
// In resizeFileStorage(), after invalidatePagesFrom:
if (neededPages > 0 && newSize % PAGE_SIZE === 0) {
  const lastIdx = neededPages - 1;
  if (!pageCache.has(path, lastIdx)) {
    pageCache.markPageDirty(path, lastIdx);
  }
}
```

This forces the page into the cache (reading from backend if it exists, or
creating a zero-filled page) and marks it dirty so it gets flushed during
syncfs. `restoreTree` then sees the correct `maxPageIndex`.

## Additional fixes on origin/main

### #272: Stale per-node page table entries (performance bug)

**Location:** `src/tomefs.ts` lines 255-261 (read) and 366-372 (write), the
multi-page cold path population loops.

**Bug:** After multi-page I/O reloads pages via the cold path, the population
loop uses `if (!node._pages[p])` — which doesn't detect stale entries (evicted
CachedPage objects are truthy). Stale references persist, preventing the warm
path from activating on subsequent multi-page reads/writes.

**Impact:** Performance only — forces repeated cold path usage under cache
pressure with multi-page I/O patterns.

**Fix:** Change to `const existing = node._pages[p]; if (!existing || existing.evicted)`.

### #275: Clean marker ordering (crash safety)

**Location:** `src/tomefs.ts` syncfs implementation.

**Bug:** The clean-shutdown marker is written in the same `syncAll` batch as
dirty pages. If the process crashes between `syncAll` and the subsequent
`deleteAll` (orphan cleanup), the marker is present but orphans remain. Next
mount trusts the marker and skips orphan cleanup.

**Impact:** Phantom files persist after crash between sync and cleanup.

**Fix:** Write the marker AFTER `deleteAll` completes, not in the `syncAll`
batch.

## Applying the fix

Pull origin/main to bring in all three fixes:

```bash
git pull origin main
```

Or cherry-pick the specific fix commits:

```bash
git cherry-pick 6b361aa  # #272: stale page table
git cherry-pick 50ba1c0  # #278: page-aligned truncation (CRITICAL)
git cherry-pick 85fc1d7  # #275: clean marker ordering
```

## Test verification

After applying, run:

```bash
TOMEFS_BACKEND=tomefs TOMEFS_MAX_PAGES=4 npx vitest run tests/adversarial/stale-page-table.test.ts
TOMEFS_BACKEND=tomefs TOMEFS_MAX_PAGES=4 npx vitest run tests/adversarial/
TOMEFS_BACKEND=tomefs npx vitest run
```

The existing test suite on local main does NOT catch these bugs — the regression
tests were added in the same commits as the fixes. Full CI on origin/main
passes all 4 stages.
