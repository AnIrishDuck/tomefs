# Workload Scenario Test Plan

## Goal

Verify that tomefs works end-to-end under realistic usage patterns, not just individual POSIX operations. Conformance tests prove each operation is correct in isolation; workload scenarios prove the filesystem holds together under sustained, interleaved access patterns that match how Postgres actually uses a filesystem.

These tests should catch interaction bugs that no single conformance test covers: cache thrashing under write-heavy loads, page eviction during sequential scans, dirty flush ordering on concurrent streams, metadata consistency after mixed read/write/truncate sequences.

## When to Build

After tomefs passes the conformance suite with MemoryBackend (ethos step 5). The tomefs Emscripten FS must be mountable and functional before these scenarios can run.

## Test Infrastructure

### Harness

Each scenario mounts tomefs (backed by SyncMemoryBackend) and runs a sequence of FS operations. The harness should:

- Mount tomefs with a **configurable cache size** (small caches force eviction, exposing cache bugs)
- Provide helpers to create realistic file structures (WAL files, table files, temp files)
- Track and assert on cache statistics (hits, misses, evictions, dirty flushes) where useful
- Support running the same scenario against MEMFS as a reference (differential testing)

### Running

Workload scenarios should be in `tests/workload/`. They run with `vitest` like everything else. They may be slower than conformance tests — that's fine. Tag a fast subset with `@fast` for CI.

## Scenarios

### Scenario 1: Sequential Table Scan

Simulates Postgres reading an entire table file sequentially.

**Pattern**: Create a large file (e.g., 256 KB = 32 pages). Read it front-to-back in 8 KB blocks. Verify every byte.

**What it catches**: Sequential read correctness across many pages, page cache population under sequential access, correct behavior when cache is smaller than the file (eviction during scan).

**Variants**:
- Cache larger than file (no eviction — baseline correctness)
- Cache smaller than file (forced eviction during scan)
- Cache size = 1 page (extreme eviction pressure)

### Scenario 2: Write-Heavy Append (WAL Simulation)

Simulates Postgres appending to a WAL (write-ahead log) file.

**Pattern**: Open a file in append mode. Write 100+ small records (64-512 bytes each). After all writes, read back the entire file and verify contents.

**What it catches**: O_APPEND correctness under sustained use, partial-page writes that accumulate across many operations, dirty page flush correctness when many pages are modified.

**Variants**:
- Small cache (dirty pages must be flushed mid-stream)
- Interspersed reads between writes (read-your-writes consistency)

### Scenario 3: Mixed Read/Write on Multiple Files

Simulates Postgres accessing multiple table/index files concurrently.

**Pattern**: Create 5-10 files. Interleave reads and writes across all files — write to file A, read from file B, write to file C, read from file A, etc. Verify all files have correct contents at the end.

**What it catches**: Cross-file cache coherence, eviction of one file's pages while another file is being written, dirty flush ordering when multiple files compete for cache space.

**Variants**:
- More files than cache pages (guaranteed cross-file eviction)
- Same file opened by multiple streams (shared node, independent positions)

### Scenario 4: Truncate Under Load

Simulates Postgres vacuuming (truncating a table file to reclaim space).

**Pattern**: Write a large file (multi-page). Read it to populate the cache. Truncate to a smaller size. Write new data at the end. Read back and verify: old data before truncation point is preserved, truncated region is gone, new data after truncation is correct.

**What it catches**: Cache invalidation after truncate — stale pages beyond the new size must not be served. Dirty pages beyond truncation point must be discarded, not flushed.

**Variants**:
- Truncate to zero, then rebuild
- Truncate while other streams have the file open
- Repeated truncate/grow cycles

### Scenario 5: Create/Delete Churn (Temp Files)

Simulates Postgres creating and deleting temporary files during query execution.

**Pattern**: In a loop, create a temp file, write data, read it back, verify, then unlink. Repeat 50+ times. Verify no resource leaks (cache entries, metadata) accumulate.

**What it catches**: Cleanup after unlink (pages removed from cache and backend), metadata cache not growing unboundedly, file ID recycling doesn't cause stale data.

### Scenario 6: Directory Operations Under File Churn

Simulates Postgres managing its data directory structure.

**Pattern**: Create a directory tree (`base/`, `base/16384/`, etc.). Create files inside directories. Rename files between directories. Delete files and directories. At each step, verify readdir returns correct entries.

**What it catches**: Metadata consistency during directory mutations, readdir correctness after rename/unlink, ENOTEMPTY enforcement after file creation.

### Scenario 7: Large Sequential Write Then Random Read

Simulates initial data load followed by random-access queries.

**Pattern**: Write a large file (512 KB+) sequentially. Then read 50+ random 8 KB-aligned blocks. Verify each block's content matches what was written.

**What it catches**: Random-access reads after sequential population, cache eviction and re-fetch correctness for previously-evicted pages, no data corruption when pages cycle through the cache multiple times.

**Variants**:
- Cache much smaller than file (most reads are cache misses)
- Re-read the same pages multiple times (cache hit path)

### Scenario 8: Rename Atomicity

Simulates Postgres's safe-write pattern (write to temp file, rename over original).

**Pattern**: Create file `data`. Write to `data.tmp`. Rename `data.tmp` to `data` (overwriting). Read `data` and verify it has the new contents. Repeat multiple times.

**What it catches**: Rename correctly replaces cached pages for the old path, no stale data served from cache after rename, page cache keys updated atomically.

## Cache-Pressure Matrix

Each scenario above should be run at multiple cache sizes to force different code paths:

| Cache Config | Pages | Memory | Purpose |
|-------------|-------|--------|---------|
| Tiny | 4 | 32 KB | Maximum eviction pressure, every operation forces eviction |
| Small | 16 | 128 KB | Moderate eviction, realistic for memory-constrained environments |
| Medium | 64 | 512 KB | Working set partially fits, some eviction |
| Large | 4096 | 32 MB | Default — working set fits, should match MEMFS performance |

The tiny and small configs are the most valuable for finding bugs. Large is a baseline sanity check.

## Success Criteria

- All scenarios pass against tomefs with SyncMemoryBackend at all cache sizes
- Same scenarios pass against MEMFS (differential correctness)
- No data corruption detected across any scenario
- Cache statistics are reasonable (no unexplained misses in large-cache configs)
- A `@fast` subset exists for CI (pick 1 variant of 3-4 key scenarios)

## Relationship to Other Tests

| Test Type | What It Proves | When It Runs |
|-----------|---------------|-------------|
| Conformance (Batches 1-5) | Individual POSIX operations are correct | Always |
| BadFS validation | Conformance tests catch specific defect classes | Always |
| **Workload scenarios** | **Operations compose correctly under realistic patterns** | **After tomefs exists** |
| Adversarial differential | Page cache seams don't corrupt data | After workload scenarios |
