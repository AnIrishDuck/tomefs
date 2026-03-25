# CLAUDE.md

## Project

tomefs is a bounded, page-cached Emscripten filesystem for PGlite. It replaces IDBFS so that only the working set lives in memory, with LRU eviction and dirty-page flushing to IndexedDB or OPFS.

## Build & Test

```bash
npm install
npm run build          # TypeScript compilation (tsconfig.build.json)
npm test               # Full test suite (755+ tests, ~90s)
npm run test:fast      # Smoke tests tagged @fast (~15s)
npm run bench          # Performance benchmarks
```

CI runs three stages: build/type-check, smoke tests, full suite (see `.github/workflows/ci.yml`).

The `TOMEFS_BACKEND=tomefs` env var switches conformance tests to run against tomefs instead of MEMFS.

## Architecture

```
src/tomefs.ts          — Emscripten FS implementation (node_ops, stream_ops)
src/sync-page-cache.ts — Bounded LRU page cache with dirty tracking
src/page-cache.ts      — Async page cache variant
src/idb-backend.ts     — IndexedDB storage backend
src/opfs-backend.ts    — OPFS storage backend
src/preload-backend.ts — Graceful degradation without SharedArrayBuffer
src/sab-client.ts      — SAB+Atomics sync bridge (worker side)
src/sab-worker.ts      — SAB+Atomics sync bridge (storage worker side)
src/sab-protocol.ts    — Shared protocol constants
src/pglite-fs.ts       — PGlite adapter (extends MemoryFS)
src/pglite.ts          — Public re-export for tomefs/pglite entry point
src/worker.ts          — Public re-export for tomefs/worker entry point
src/index.ts           — Main public API
src/types.ts           — PAGE_SIZE (8192), FileMeta, PageKey, constants
```

Three package entry points: `.` (main), `./worker`, `./pglite`.

## Test Structure

```
tests/conformance/     — POSIX conformance (22 files, ported from Emscripten C tests)
tests/unit/            — Component-level tests (page cache, backends, SAB bridge)
tests/integration/     — Full-stack integration (tomefs + SAB + backend)
tests/adversarial/     — Edge cases targeting page cache seams
tests/fuzz/            — Randomized differential fuzz testing
tests/workload/        — Simulated PGlite access patterns
tests/pglite/          — PGlite SQL-level integration tests
tests/scribe-data/     — App-level workload scenarios
tests/badfs/           — BadFS validation (defect injection)
tests/benchmark/       — Performance benchmarks
tests/harness/         — Shared test infrastructure (Emscripten FS loader, BadFS, fake-opfs)
```

Test harness: `tests/harness/emscripten-fs.ts` provides `createFS()` which returns `{ FS, E, O }` — the Emscripten FS API, errno constants, and open flags. All conformance tests use this. The harness loads a real compiled Emscripten WASM module (`emscripten_fs.wasm`).

## Conventions

- **No mocks.** Use fakes that implement real interfaces (e.g., `SyncMemoryBackend`, `MemoryBackend`).
- **Tests before implementation.** Conformance tests define correctness independently of the implementation.
- **Strict TypeScript.** `"strict": true` in both tsconfig files.
- **Page size is 8192 bytes** (matches Postgres internal page size).
- **Commit working increments.** Don't batch large changes.
- Emscripten FS API is untyped upstream — `any` usage in `tomefs.ts` and `pglite-fs.ts` is intentional for the Emscripten interop layer.

## Key Interfaces

- `SyncStorageBackend` — synchronous storage (used by tomefs directly)
- `StorageBackend` — async storage (used in worker context)
- Both define: `readPage`, `writePage`, `readPages`, `writePages`, `deleteFile`, `listFiles`, `getMetadata`, `putMetadata`, `deleteMetadata`, `listMetadata`

## Design Decisions

- The sync bridge (SAB+Atomics) is needed because Emscripten FS operations are synchronous but IndexedDB is async. PGlite runs in a Web Worker; a separate Storage Worker handles async IDB operations.
- `PreloadBackend` provides graceful degradation without SharedArrayBuffer by eagerly loading all pages at init time.
- `SyncMemoryBackend` is the default for testing and for PGlite when persistence isn't needed.
