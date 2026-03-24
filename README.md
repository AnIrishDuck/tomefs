# tomefs

A bounded, page-cached filesystem layer for [PGlite](https://electric-sql.com/product/pglite) and Emscripten.

## Problem

PGlite's `idb://` backend uses Emscripten's IDBFS, a snapshot-sync layer that loads **all** files from IndexedDB into memory at startup. As databases grow, this means slow startups and unbounded memory usage — the entire database must fit in RAM.

## Solution

tomefs replaces IDBFS with a custom Emscripten filesystem that:

- Stores file data as **8 KB pages** in IndexedDB (matching Postgres's internal page size)
- Loads pages **on demand** into a **bounded LRU cache** — only the working set lives in memory
- Tracks **dirty pages** and flushes them back to storage in batches
- Supports **pluggable storage backends** (memory, IndexedDB, OPFS)

## Architecture

```
Postgres (WASM)
  │
  ▼
Emscripten FS layer
  │
  ▼
tomefs
  ├── LRU Page Cache (bounded, in-memory)
  ├── File Metadata Cache
  └── Storage Backend
      └── IndexedDB (persistent, page-level)
```

### Sync Bridge

Emscripten FS operations are synchronous (C-style), but IndexedDB is async. tomefs bridges this gap using **SharedArrayBuffer + Atomics**:

1. PGlite runs in a Web Worker
2. A Storage Worker handles async IDB operations
3. The PGlite worker writes requests to a SharedArrayBuffer and calls `Atomics.wait()`
4. The Storage Worker performs the IDB operation and signals back via `Atomics.notify()`

This requires `Cross-Origin-Opener-Policy: same-origin` and `Cross-Origin-Embedder-Policy: require-corp` headers.

## Key Features

- **Bounded memory**: Configurable max pages in cache (default 4096 = 32 MB)
- **LRU eviction**: Least-recently-used pages are evicted when cache is full; dirty pages are flushed before eviction
- **Dirty tracking**: Only modified pages are written back to storage
- **Batch flush**: All dirty pages for a file (or globally) can be flushed in a single operation
- **Page-aligned I/O**: 8 KB pages match Postgres internals, eliminating partial-page overhead
- **Pluggable backends**: `StorageBackend` interface allows swapping storage (memory for testing, IDB for production, OPFS for future)

## Status

**Early development.** See [plans/](plans/) for the architecture plan, implementation phases, and design rationale.

### Roadmap

| Phase | Description | Status |
|-------|-------------|--------|
| 0 | Conformance test suite (Batches 1-6) | Done |
| 0.5 | BadFS validation | Done |
| 1 | Page cache core + memory backend | Done |
| 1.5 | tomefs Emscripten FS + page cache integration | Done |
| 1.6 | Workload scenario tests | Done |
| 1.7 | Persistence (Batch 6) tests | Done |
| 2 | IndexedDB backend with compound key storage | Done |
| 3 | SAB+Atomics sync bridge | Done |
| 4 | PGlite integration + migration from IDBFS | Planned |
| 5 | OPFS backend (alternative to IDB) | Future |

## License

See [LICENSE](LICENSE) for details.
