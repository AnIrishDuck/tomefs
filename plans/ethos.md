# Ethos: tomefs

## What We're Building

A page-cached Emscripten filesystem that replaces IDBFS for PGlite. Today,
PGlite loads the entire database into memory at startup via IDBFS. tomefs stores
file data as 8 KB pages in IndexedDB, loads pages on demand into a bounded LRU
cache, and flushes dirty pages back. Only the working set lives in memory.

The end state: mount tomefs in PGlite instead of IDBFS. Postgres doesn't know
the difference — it's just an Emscripten FS. But startup is fast, memory is
bounded, and databases can grow beyond RAM.

## Where Things Stand

tomefs lives in its own repo (`anirishduck/tomefs`). The page cache core
(LRU + memory backend) was originally built inside `tributary/tome/` and has
been extracted here. The tributary repo will eventually consume tomefs as a
dependency.

**Done:**
- `PageCache` class: LRU eviction, dirty tracking, configurable max pages
- `MemoryBackend`: in-memory `StorageBackend` for testing
- `StorageBackend` interface: sync API that backends implement
- Unit tests for cache and memory backend

**Not done (in rough priority order):**
1. Conformance test suite (see below — this is next)
2. Emscripten FS implementation (`PageCacheFS`)
3. IDB backend + SAB+Atomics sync bridge
4. PGlite integration + IDBFS migration
5. OPFS backend (future)

## Priorities

### 1. Tests Before Implementation

We write the conformance test suite *before* any filesystem implementation code.
Tests define correctness. They run against Emscripten's MEMFS first (the
known-good reference), then against our FS as we build it.

### 2. Real POSIX Semantics, Not Toy Coverage

The conformance tests come from Emscripten's own test suite — ~20 C test files
covering ~165 assertions across core I/O, metadata, directories, rename/unlink,
links, truncate, and edge cases. These were battle-tested against CPython's test
suite, which uncovered 14 real POSIX compliance bugs in Emscripten's FS. We port
these to TypeScript calling the Emscripten JS FS API.

The test plan (`plans/conformance-test-plan.md`) has the full inventory, port
order, and source mapping.

### 3. BadFS Validation

After the test suite passes against MEMFS, we build a `BadFS` wrapper that
injects specific defects (off-by-one reads, missing mtime updates, broken
symlink resolution, etc). Each defect must cause at least one test to fail. This
proves the tests actually catch bugs, not just exercise happy paths.

### 4. No Mocks

From tributary's conventions: **we never use mocks**. We use fakes (like
`MemoryBackend`) that implement the real interface and can be substituted for
integration testing.

### 5. Standard Emscripten FS — Not a PGlite Abstraction

tomefs implements the standard Emscripten filesystem interface (`node_ops`,
`stream_ops`). It registers via `FS.filesystems.PageCacheFS = PageCacheFS` and
mounts like any other FS. PGlite doesn't need to know about it beyond the mount
call.

## How to Pick the Next Task

Look at the repo. Look at what exists. Work down this list:

1. **If there are no conformance tests yet** → start porting them. Follow the
   batch order in `plans/conformance-test-plan.md` (Batch 1: Core I/O first).
   Port C test logic to TypeScript/vitest calling Emscripten's JS FS API. Tests
   must pass against MEMFS.

2. **If conformance tests exist but are incomplete** → continue porting the next
   batch. Check which batches are done by looking at test files in `tests/`.

3. **If all 6 batches of conformance tests pass against MEMFS** → build the
   BadFS wrapper and validate that each injected defect is caught.

4. **If BadFS validation is done** → start the Emscripten FS implementation
   (`PageCacheFS`). Wire it up to the existing `PageCache` + `MemoryBackend`.
   Run the conformance tests against it. Fix failures until green.

5. **If PageCacheFS passes conformance tests with MemoryBackend** → build the
   IDB backend and SAB+Atomics sync bridge. See `plans/page-cache-fs-plan.md`
   for architecture.

6. **If IDB backend works** → integrate with PGlite in the tributary repo.
   Replace IDBFS mount with tomefs mount. Run PGlite's existing tests.

## Technical Decisions

- **Language**: TypeScript. No Rust needed.
- **Page size**: 8 KB, matching Postgres internals.
- **Test framework**: vitest.
- **Sync bridge**: SharedArrayBuffer + Atomics (requires COOP/COEP headers).
- **Fallback**: Environments without SAB degrade to full MEMFS load with a
  warning.
- **Storage**: IDB with compound keys `[path, pageIndex]` for efficient range
  queries. Two object stores: `file_meta` and `pages`.

## Conventions

- Keep it simple. Don't abstract until you need to.
- Port order matters — foundational operations first (open/read/write/close),
  then metadata, then complex operations (rename, links).
- Each test file maps to a specific upstream Emscripten C test file. Note the
  source in a comment at the top.
- Commit working increments. Don't batch up large changes.
