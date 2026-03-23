# Page-Cached Filesystem for PGLite

## Context

PGLite's `idb://` backend uses Emscripten's IDBFS, which is a **snapshot-sync layer**, not a live filesystem. The flow:

1. **Startup**: `FS.syncfs(true)` loads ALL files from IndexedDB into MEMFS (in-memory)
2. **Runtime**: All Postgres read/write happens against MEMFS (synchronous, fast)
3. **After each query**: `FS.syncfs(false)` flushes changed files back to IndexedDB

This means the entire database must fit in memory. As databases grow, this becomes untenable — slow startups, unbounded memory usage.

**Goal**: Replace IDBFS with a custom Emscripten filesystem that stores file data as pages in IndexedDB, loads pages on demand into a bounded LRU cache, and writes dirty pages back. Only the working set lives in memory.

## Prior Art Evaluation

| Project | What it does | Why it doesn't solve our problem |
|---------|-------------|----------------------------------|
| **IDBFS** | Snapshot-sync: loads all files into MEMFS, flushes back | Unbounded memory — this IS the problem |
| **PGLite OPFS AHP** | OPFS sync access handles with pre-opened handle pool | Postgres needs 300+ open files; Safari caps at 252 handles. Multiplexing handles is too complex |
| **absurd-sql** | SAB+Atomics bridge for synchronous IDB access from SQLite WASM | SQLite-specific, but the sync bridge pattern is directly relevant |
| **wa-sqlite IDBBatchAtomicVFS** | IDB-backed VFS with batch atomic writes | SQLite VFS, not Emscripten FS. But page-level IDB storage pattern is relevant |
| **WasmFS** | Next-gen Emscripten FS with pluggable backends | No IDB page-cache backend exists |
| **BrowserFS** | Node-like FS for browsers | No page caching, no Emscripten FS integration |

**Conclusion**: No existing solution provides a page-cached Emscripten filesystem over IDB. Must be built custom. However, absurd-sql and wa-sqlite provide proven patterns for the sync/async bridge and page-level IDB storage.

## Key Decisions

### 1. This is a standard Emscripten custom filesystem

Not a PGLite-specific abstraction. The Emscripten filesystem API is well-documented:
- `mount(mount)` — initialize
- `node_ops` — lookup, create, mkdir, rmdir, unlink, rename, readdir, etc.
- `stream_ops` — open, close, read, write, llseek, etc.

Register with `FS.filesystems.PageCacheFS = PageCacheFS` and mount. PGLite uses this like any other Emscripten FS.

### 2. IDB as the storage backend (with pages, not whole files)

IDB stores:
- **`file_meta` store**: keyed by path → `{ size, mode, ctime, mtime }`
- **`pages` store**: keyed by `[path, pageIndex]` → `Uint8Array` (8 KB)

Compound keys allow efficient range queries (all pages for a file, for deletion/prefetch).

### 3. Sync/async bridge: SAB+Atomics

**The core problem**: Emscripten FS operations (read, write, etc.) are synchronous C-style calls. IndexedDB is async.

**PGLite is explicitly NOT built with Asyncify** — this is a deliberate design choice to keep the WASM binary small (~3MB gzipped) and performant. From the docs: "PGlite is a fully synchronous WASM build of Postgres and unable to call async APIs while handling a query." Asyncify is not an option.

**Therefore: SAB+Atomics is the sync bridge.** This is the same approach used by absurd-sql (battle-tested for SQLite). The pattern:

1. PGLite runs in a **Web Worker** (not main thread — `Atomics.wait()` would block the UI)
2. A **Storage Worker** handles async IDB operations
3. Communication via SharedArrayBuffer:
   - PGLite worker writes request to SAB, calls `Atomics.wait()`
   - Storage worker reads request, does async IDB op, writes result to SAB, calls `Atomics.notify()`
   - PGLite worker wakes up with the result

**Requires COOP/COEP headers** (`Cross-Origin-Opener-Policy: same-origin`, `Cross-Origin-Embedder-Policy: require-corp`). This is a hard requirement. The spike must evaluate whether these headers can be deployed without breaking OAuth flows, iframes, etc.

**Fallback for environments without SAB**: Degrade to current IDBFS behavior (full MEMFS load) with a console warning. This maintains backward compatibility.

### 4. Language: TypeScript

LRU cache + IDB operations + Emscripten FS glue. No Rust needed.

### 5. Page size: 8 KB (matching Postgres)

Postgres uses 8 KB pages internally. 1:1 alignment eliminates partial-page handling.

## Architecture

```
┌─────────────────────────────────────────┐
│            Postgres (WASM)              │
│         (compiled C code)               │
├─────────────────────────────────────────┤
│       Emscripten FS layer               │
│  ┌───────────────────────────────────┐  │
│  │     PageCacheFS (our code)        │  │
│  │  ┌─────────────┐ ┌─────────────┐ │  │
│  │  │  LRU Page   │ │  File Meta  │ │  │
│  │  │   Cache     │ │   Cache     │ │  │
│  │  │ (bounded)   │ │             │ │  │
│  │  └──────┬──────┘ └──────┬──────┘ │  │
│  │         │               │        │  │
│  │    ┌────┴───────────────┴────┐   │  │
│  │    │   IDB Backend           │   │  │
│  │    │  (pages + file_meta)    │   │  │
│  │    │  [SAB + Atomics]        │   │  │
│  │    └─────────────────────────┘   │  │
│  └───────────────────────────────────┘  │
└─────────────────────────────────────────┘
```

### Read path (e.g., Postgres reads 8 KB from a table file)

1. `stream_ops.read(stream, buffer, offset, length, position)` called by Emscripten
2. Calculate page index: `pageIndex = Math.floor(position / PAGE_SIZE)`
3. Check LRU page cache → **hit**: copy from cache to buffer, done
4. **Miss**: load page from IDB (async call, bridged via SAB+Atomics to storage worker)
5. Insert page into LRU cache (evict oldest if at capacity; flush if dirty)
6. Copy from cache to buffer

### Write path

1. `stream_ops.write(stream, buffer, offset, length, position)` called
2. Calculate affected pages
3. For partial-page writes: read existing page first (cache or IDB), modify, mark dirty
4. For full-page writes: write directly to cache, mark dirty
5. Dirty pages flushed to IDB on: eviction, fsync, periodic timer, close

### Directory/metadata operations

- `node_ops.lookup`, `readdir`, `mkdir`, etc. use the `file_meta` IDB store
- Metadata is also cached in memory (small, bounded by file count not data size)

## Spike Plan (Phase 0)

Validate the critical unknowns before building the full system.

### Spike Questions

1. **Can COOP/COEP headers be deployed?**
   - SAB+Atomics requires these headers — this is a hard requirement
   - Test whether adding them breaks OAuth popups, iframes, third-party embeds
   - If they can't be deployed, the entire approach needs rethinking (perhaps OPFS-only, or contributing an IDB page-cache backend to WasmFS upstream)

2. **What subset of Emscripten FS operations does Postgres actually call?**
   - Instrument a minimal FS that logs all calls
   - Run PGLite through initialization + basic SQL
   - Catalog the required operations (likely: read, write, open, close, llseek, stat, mkdir, readdir, unlink, rename, truncate)

3. **SAB+Atomics bridge: end-to-end with PGLite in a Worker**
   - PGLite runs in Worker A with custom FS
   - Custom FS sends IDB requests to Worker B via SAB
   - Worker B does async IDB ops, signals back via `Atomics.notify()`
   - Verify this works for the full PGLite lifecycle

4. **IDB page-level performance**
   - Write 8 KB pages to IDB with compound keys
   - Measure read latency for random page access
   - Measure batch write throughput
   - Compare to current whole-file IDBFS sync performance

### Spike Deliverable

A working proof-of-concept that:
- Runs PGLite in a Web Worker with a custom Emscripten FS
- Uses SAB+Atomics to bridge sync FS calls to async IDB reads/writes
- Stores data as 8 KB pages in IndexedDB
- Loads pages on demand (not all at startup)
- Successfully runs: CREATE TABLE → INSERT → SELECT → close → reopen → SELECT

## Full Implementation Phases (post-spike)

### Phase 1: Page Cache Core
- `PageCache` class: LRU eviction, dirty tracking, configurable max pages
- `MemoryBackend` for testing
- Unit tests (pure TypeScript, no browser APIs)

### Phase 2: IDB Backend
- `IdbBackend`: two object stores (`file_meta`, `pages`)
- Compound keys `[path, pageIndex]` for efficient range queries
- Batch writes in single IDB transactions
- Test with `fake-indexeddb`

### Phase 3: Emscripten FS Implementation
- `PageCacheFS` implementing Emscripten's filesystem interface
- Wire up page cache + IDB backend via SAB+Atomics bridge
- PGLite must run in a Web Worker (already supported via `PGliteWorker`)
- Integration test with real PGLite SQL

### Phase 4: Integration
- Update `persistence.ts` to use page-cache FS instead of `idb://`
- Migration: convert existing IDBFS blobs to paged format
- End-to-end browser testing
- Performance benchmarks vs current IDBFS approach

### Phase 5 (Future): OPFS Backend
- Alternative backend using OPFS for storage instead of IDB
- Same page cache, different storage layer

## New Package Structure

```
tome/
  src/
    index.ts                  # Public API
    page-cache.ts             # LRU cache with dirty tracking
    page-cache-fs.ts          # Emscripten FS implementation
    storage-backend.ts        # Backend interface
    idb-backend.ts            # IndexedDB backend (pages + metadata)
    types.ts
  tests/
    page-cache.test.ts
    idb-backend.test.ts
    integration.test.ts
  package.json
  tsconfig.json
  vitest.config.ts
```

## Risks

| Risk | Mitigation |
|------|------------|
| SAB+Atomics needs COOP/COEP headers (hard requirement) | Evaluate header impact in spike; if impossible, need fundamentally different approach (OPFS-only, or WasmFS upstream contribution) |
| IDB page-read latency too high (one IDB read per cache miss) | Prefetch neighboring pages on miss; tune cache size; batch reads |
| Emscripten FS interface has undocumented edge cases | Start with logged/instrumented FS; add operations as Postgres exercises them |
| Migration from existing IDBFS format | One-time async migration before PGLite init; split blobs into pages |

## Verification

1. **Spike**: Custom FS + PGLite runs basic SQL with page-level IDB storage
2. **Unit tests**: LRU behavior, IDB backend (fake-indexeddb), bridge protocol
3. **Integration tests**: Full SQL lifecycle, survive close+reopen, data integrity
4. **Memory bound test**: Small cache (64 pages), force evictions, verify correctness
5. **Performance**: Compare startup time and query latency vs current IDBFS
