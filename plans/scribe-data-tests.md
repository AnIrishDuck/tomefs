# Plan: Scribe-Data Integration Tests for tomefs

## Context

The [tributary](https://github.com/AnIrishDuck/tributary) project uses tomefs as its filesystem layer for PGlite. The `scribe-data` module (`apps/scribe/scribe-data/`) is a markdown document editor that stores versioned notes, collections, and search indexes via `TributaryStream` (synced writes) and `TributaryLocal` (local-only indexing). All writes flow through a `Server` interface that stores encrypted blobs with hash-chaining and sequence numbers.

Currently, tributary's scribe-data tests live in tributary and depend on `tributary-client`, `PGlite`, `nacl` crypto, etc. **This plan adds tests to tomefs** that exercise the filesystem layer under realistic scribe-data workloads — particularly the paging/sync patterns that stress the page cache. We use a **fake server** (no real network) to drive deterministic scenarios.

## Why This Matters

tomefs's existing tests verify POSIX correctness and page-cache mechanics in isolation. But the real workload is PGlite running SQL driven by tributary sync. Specific risks:

1. **Sync paging**: tributary syncs blobs in batches (e.g., `max=2`). Each batch triggers SQL replayed against PGlite, which hits tomefs. Page cache eviction during mid-batch SQL replay could corrupt state.
2. **Prefetch overlap**: tributary fires prefetch requests concurrently with DB transactions. The filesystem sees interleaved read/write patterns.
3. **Multi-stream contention**: scribe supports multiple libraries (streams), each with its own schema. Concurrent sync of multiple streams creates cross-file page cache pressure.
4. **Index rebuilds**: local search indexing scans and rewrites index tables after sync. This is a write-heavy burst that follows a read-heavy sync.

## Architecture

```
tests/scribe-data/
├── fake-tributary.ts        # Fake Server + TributaryStream-like driver
├── harness.ts               # Creates PGlite-on-tomefs, wires up fakes
├── sync-paging.test.ts      # Sync batch/paging scenarios
├── write-patterns.test.ts   # Note CRUD write patterns
├── search-indexing.test.ts  # Search index rebuild under cache pressure
├── multi-stream.test.ts     # Multiple concurrent streams
└── prefetch.test.ts         # Prefetch + DB transaction interleaving
```

### Fake Tributary Server (`fake-tributary.ts`)

A minimal fake that implements the `Server` interface from tributary-client:

- **In-memory blob store**: `Map<string, Blob[]>` keyed by pubkey
- **`storeBlob()`**: validates sequence number ordering, stores blob
- **`getBlobsArrow()`**: returns blobs after `startSequence`, limited by `max` — this is the paging primitive
- **`getAllBlobMetadata()`**: metadata-only variant with same paging
- **`getLatestBlobMetadata()`**: returns most recent blob
- **Disconnect/reconnect**: toggle to simulate network failures
- **`setMaxBlobsPerSync()`**: artificially limit page size per pubkey

This is modeled directly on tributary's `FakeServer` and `TestFakeServer`. The key difference: we don't need real crypto (nacl signing, encryption). We can store plaintext SQL as blob data and skip signature verification. This keeps the fake simple and focused on the paging/sync behavior that stresses tomefs.

### Test Harness (`harness.ts`)

```typescript
interface ScribeTestHarness {
  // PGlite instance backed by tomefs (not MEMFS)
  pglite: PGliteInterface
  // The underlying tomefs page cache (for inspection/pressure tuning)
  cache: SyncPageCache
  // Fake server for blob storage
  server: FakeServer
  // Helper to create a stream and run schema migrations
  createStream(name: string): Promise<TestStream>
  // Helper to sync a stream with paging
  syncStream(stream: TestStream, max: number): Promise<SyncStatus>
  // Cleanup
  destroy(): Promise<void>
}

// Cache pressure configs (same pattern as workload/scenarios.test.ts)
type CachePressure = 'tiny' | 'small' | 'medium' | 'large'
```

Each test gets a fresh `ScribeTestHarness`. The harness wires PGlite to use tomefs with `SyncMemoryBackend` (same as existing workload tests), so no IDB is needed.

## Test Scenarios

### 1. Sync Paging (`sync-paging.test.ts`)

These are the highest priority. They test that paged sync correctly replays SQL against PGlite-on-tomefs without data loss or corruption.

#### Scenario matrix:

| Scenario | Blobs | Page size (`max`) | Cache pressure | What it tests |
|---|---|---|---|---|
| **Single page** | 5 | 100 | medium | Baseline: all blobs in one fetch |
| **Exact fit** | 6 | 3 | medium | Two pages, no remainder |
| **Remainder** | 7 | 3 | medium | Two full pages + partial third |
| **One-at-a-time** | 5 | 1 | medium | `max=1` edge case (no infinite loop) |
| **Tiny cache + large sync** | 50 | 5 | tiny (4 pages) | Heavy eviction during SQL replay |
| **Small cache + large sync** | 50 | 5 | small (16 pages) | Moderate eviction |
| **Large batch, small page** | 100 | 2 | medium | Many sync iterations |
| **Empty server** | 0 | 10 | medium | No-op sync |
| **Already synced** | 10 | 10 | medium | Re-sync returns complete immediately |

#### Test pattern:

```typescript
it('syncs 50 notes in pages of 5 under tiny cache', async () => {
  const h = await createHarness('tiny')
  const stream = await h.createStream('test')

  // Write 50 notes (each becomes a blob on the fake server)
  for (let i = 0; i < 50; i++) {
    await stream.exec(`INSERT INTO block (...) VALUES (...)`)
  }

  // Create a second "reader" stream on the same harness
  const reader = await h.createReaderStream('test')

  // Sync in pages of 5
  let synced = false
  let iterations = 0
  while (!synced) {
    const status = await h.syncStream(reader, 5)
    synced = status.complete()
    iterations++
    expect(iterations).toBeLessThan(20) // safety bound
  }

  // Verify all data arrived
  const result = await reader.query('SELECT COUNT(*) as count FROM block')
  expect(result.rows[0].count).toBe(50)
})
```

#### Additional paging edge cases:

- **Disconnect mid-sync**: sync 2 pages, disconnect server, verify error, reconnect, resume from correct offset
- **New blobs arrive during sync**: writer adds blobs between reader's sync pages; verify reader eventually gets all data
- **Hash chain verification**: verify blob sequence integrity across page boundaries
- **Sync index persistence**: after partial sync + "restart" (new stream instance on same PGlite), verify sync resumes from last committed index, not from 0

### 2. Write Patterns (`write-patterns.test.ts`)

These test the filesystem under scribe-data's actual SQL patterns: note creation, versioning, collection hierarchy, and moves.

#### Scenarios:

- **Burst note creation**: create 100 notes in rapid succession, verify all persist through page cache
- **Version chain**: create a note, then 20 versions of it; verify version history query returns all 20 in order
- **Collection hierarchy**: create 5 levels of nested collections, create notes at each level, query breadcrumbs
- **Move operations**: create notes, move between collections, verify slug paths update correctly
- **Mixed reads and writes**: interleave note creation with queries (simulating UI that reads while writing)

Run each scenario at multiple cache pressures (tiny, small, medium, large) using the same `describeScenario` matrix pattern from `tests/workload/scenarios.test.ts`.

### 3. Search Indexing (`search-indexing.test.ts`)

Search indexing is a write-heavy burst that follows sync. It scans `block` + `authoritative_version`, extracts text, and writes to `block_search_index` with tsvector computation.

#### Scenarios:

- **Index after sync**: sync 50 notes, then run `indexSearchVectors()` in batches of 10; verify all notes get indexed
- **Index under tiny cache**: same as above but with 4-page cache — forces heavy eviction during the scan+write pattern
- **Incremental indexing**: sync 20 notes and index, then sync 30 more and index again; verify only new notes are processed
- **Search after index**: after indexing, run `searchNotes()` queries and verify results are correct
- **Search pagination**: create 50 notes matching a query, search with `limit=10, offset=0`, then `offset=10`, etc.; verify no duplicates or gaps

### 4. Multi-Stream (`multi-stream.test.ts`)

Scribe supports multiple libraries, each backed by a separate tributary stream with its own PG schema.

#### Scenarios:

- **Two streams, sequential sync**: create stream A with 20 notes, stream B with 20 notes; sync A fully, then sync B fully; verify both have correct data
- **Two streams, interleaved sync**: sync A page 1, sync B page 1, sync A page 2, sync B page 2, ...; verify no cross-contamination
- **Two streams, tiny cache**: both streams syncing under 4-page cache; verify eviction doesn't corrupt either stream's data
- **Stream isolation**: write to stream A, verify stream B cannot see A's data (schema isolation)

### 5. Prefetch Patterns (`prefetch.test.ts`)

Tributary's sync fires a prefetch for the next batch before the current batch's DB transaction completes. This creates overlapping I/O patterns on tomefs.

#### Scenarios:

- **Prefetch reuse**: sync page 1 (triggers prefetch of page 2), sync page 2 (should reuse prefetch); verify only N+1 server calls for N pages
- **Prefetch invalidation on local write**: sync page 1, local write advances sync index, next sync should discard stale prefetch
- **Prefetch with cache pressure**: under tiny cache, prefetch arrives while DB transaction is writing; verify no corruption
- **Stale prefetch (new blobs)**: sync page 1 (prefetch fires for page 2), writer adds more blobs, sync page 2 uses stale prefetch with old totalCount; verify subsequent syncs pick up remaining blobs

## Implementation Order

1. **`fake-tributary.ts`** — the fake server. ~150 LOC. No external deps beyond what tomefs already has.
2. **`harness.ts`** — the test harness wiring PGlite-on-tomefs to the fake server. ~100 LOC. Depends on PGlite being available as a dev dependency.
3. **`sync-paging.test.ts`** — highest value, most paging scenarios. ~300 LOC.
4. **`write-patterns.test.ts`** — exercises real SQL patterns. ~200 LOC.
5. **`search-indexing.test.ts`** — write-heavy indexing workload. ~200 LOC.
6. **`multi-stream.test.ts`** — cross-stream isolation under cache pressure. ~200 LOC.
7. **`prefetch.test.ts`** — overlapping I/O patterns. ~200 LOC.

## Dependencies

The key question: **do we need PGlite as a dependency?**

- **Option A (recommended)**: Add `@electric-sql/pglite` as a devDependency. This lets us run real SQL and test the actual filesystem access patterns PGlite generates. The tests are then true integration tests.
- **Option B**: Simulate PGlite's I/O patterns with direct filesystem calls (open/read/write at specific offsets). This avoids the PGlite dep but loses fidelity — we'd be guessing at PGlite's actual access patterns.

**Recommendation**: Option A. tomefs exists to serve PGlite; testing without PGlite is testing in a vacuum. The PGlite dep is dev-only and doesn't affect the published package.

Additional dev dependencies needed:
- `@electric-sql/pglite` — the database
- `tweetnacl` — only if we want real crypto in the fake server (can skip with plaintext blobs)
- `uuid` — for generating test UUIDs (or use `crypto.randomUUID()`)

## Notes

- All tests use vitest (matching existing tomefs convention)
- No mocks — only fakes (matching both tomefs and tributary convention)
- `@fast` tags on a representative subset for development iteration
- Cache pressure matrix matches existing `tests/workload/` conventions
- Each test file is self-contained with its own `beforeEach` setup
