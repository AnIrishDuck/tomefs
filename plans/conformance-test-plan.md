# Conformance Test Plan

## Goal

Before writing any filesystem implementation code, build an exhaustive conformance test suite that:

1. **Defines correctness** — every POSIX/Emscripten FS behavior our implementation must satisfy
2. **Validates against a known-good FS** — all tests pass against Emscripten's IDBFS/MEMFS
3. **Catches real bugs** — a deliberately broken "BadFS" with subtle defects must fail specific tests

This is Phase 1. No implementation code until the test suite is complete and validated.

## Architecture

```
test/
  conformance/
    runner.ts              # Test harness: runs suites against any FS backend
    backends/
      memfs-adapter.ts     # Adapter wrapping Emscripten MEMFS
      idbfs-adapter.ts     # Adapter wrapping Emscripten IDBFS (fake-indexeddb)
      badfs.ts             # Deliberately broken FS for validation
    suites/
      basic-io.test.ts     # open, read, write, close
      seek-truncate.test.ts # lseek, ftruncate
      stat.test.ts         # stat, fstat, lstat, timestamps
      directories.test.ts  # mkdir, rmdir, readdir, lookup
      rename.test.ts       # rename (files, dirs, overwrite semantics)
      unlink.test.ts       # unlink, ENOENT, EISDIR
      symlinks.test.ts     # symlink, readlink, path resolution through symlinks
      permissions.test.ts  # mode bits, EACCES
      errno.test.ts        # correct errno for every error case
      edge-cases.test.ts   # CPython-discovered bugs, boundary conditions
      large-io.test.ts     # multi-page writes, page-boundary spanning
      concurrent-fds.test.ts # multiple open file descriptors to same file
      persistence.test.ts  # write → sync → reload → verify
```

### Test Backend Interface

Every conformance test operates against a `TestFS` abstraction:

```typescript
interface TestFS {
  // Setup / teardown
  mount(): Promise<void>;
  unmount(): Promise<void>;

  // Core POSIX-like operations (mirrors Emscripten FS JS API)
  open(path: string, flags: number, mode?: number): number; // returns fd
  close(fd: number): void;
  read(fd: number, buffer: Uint8Array, offset: number, length: number): number;
  write(fd: number, buffer: Uint8Array, offset: number, length: number): number;
  llseek(fd: number, offset: number, whence: number): number;

  stat(path: string): StatResult;
  fstat(fd: number): StatResult;
  lstat(path: string): StatResult;

  mkdir(path: string, mode?: number): void;
  rmdir(path: string): void;
  readdir(path: string): string[];

  unlink(path: string): void;
  rename(oldPath: string, newPath: string): void;
  truncate(path: string, length: number): void;
  ftruncate(fd: number, length: number): void;

  symlink(target: string, linkPath: string): void;
  readlink(path: string): string;

  chmod(path: string, mode: number): void;

  // Persistence (for round-trip tests)
  sync?(): Promise<void>;
}
```

### BadFS Design

`BadFS` wraps a correct FS and injects specific, subtle bugs. Each bug is toggled by name so tests can assert which suite catches which defect.

```typescript
class BadFS implements TestFS {
  constructor(private inner: TestFS, private defect: string) {}
  // ...delegates to inner, with targeted corruption
}
```

Planned defects:

| Defect ID | What it does | Which suite should catch it |
|-----------|-------------|---------------------------|
| `off-by-one-read` | Returns `length - 1` bytes on read | `basic-io` |
| `no-mtime-update` | Doesn't update mtime on write | `stat` |
| `rename-no-overwrite` | Rename fails silently when target exists | `rename` |
| `truncate-no-shrink` | ftruncate doesn't actually reduce file size | `seek-truncate` |
| `readdir-missing-dot` | readdir omits `.` and `..` entries | `directories` |
| `unlink-leaves-pages` | unlink removes metadata but not file data (leaked pages) | `unlink` |
| `symlink-no-resolve` | open() doesn't follow symlinks | `symlinks` |
| `wrong-errno-enoent` | Returns EACCES instead of ENOENT for missing files | `errno` |
| `seek-end-off-by-one` | SEEK_END is off by one byte | `seek-truncate` |
| `partial-page-corrupt` | Writes spanning page boundary corrupt last byte | `large-io` |
| `stale-fd-after-unlink` | Already-open fd returns error after unlink | `concurrent-fds` |
| `persistence-drop-page` | Drops the last page on sync | `persistence` |

## Test Suites — Detailed Specification

### Suite 1: `basic-io.test.ts` — Core Read/Write/Close

**Source**: Emscripten `test_unistd_read`, `test_unistd_write`, `test_files`

| # | Test | Expected behavior |
|---|------|-------------------|
| 1 | Create file, write bytes, read back | Exact byte-for-byte match |
| 2 | Write empty file, read returns 0 bytes | Length 0, no error |
| 3 | Read from empty file | Returns 0 (EOF), no error |
| 4 | Write at offset 0 then offset 100 (gap) | Gap filled with zero bytes |
| 5 | Multiple sequential writes accumulate | File contains concatenated data |
| 6 | Read with O_RDONLY, then attempt write | Write returns EBADF |
| 7 | Write with O_WRONLY, then attempt read | Read returns EBADF |
| 8 | O_CREAT creates file if not exists | File exists after open |
| 9 | O_CREAT on existing file opens it | No error, no truncation |
| 10 | O_TRUNC truncates existing file to 0 | stat.size === 0 after open |
| 11 | O_APPEND writes always go to end | Concurrent appends don't overwrite |
| 12 | O_EXCL + O_CREAT on existing file | Returns EEXIST |
| 13 | Write > PAGE_SIZE (8192) bytes | All bytes survive round-trip |
| 14 | Write exactly PAGE_SIZE bytes | All bytes survive round-trip |
| 15 | Close fd, then attempt read/write | Returns EBADF |
| 16 | Double close same fd | Returns EBADF on second close |

### Suite 2: `seek-truncate.test.ts` — Seek and Truncate

**Source**: Emscripten `test_unistd_truncate`, POSIX lseek semantics

| # | Test | Expected |
|---|------|----------|
| 1 | SEEK_SET to position 0 | Returns 0 |
| 2 | SEEK_SET to position N | Returns N |
| 3 | SEEK_CUR from current position | Returns current + offset |
| 4 | SEEK_END on file of size S | Returns S + offset |
| 5 | SEEK_SET past end of file, then write | Gap zero-filled |
| 6 | SEEK_SET to negative offset | Returns EINVAL |
| 7 | ftruncate to smaller size | stat.size shrinks, data beyond lost |
| 8 | ftruncate to larger size | stat.size grows, new bytes are zero |
| 9 | ftruncate to same size | No change, no error |
| 10 | ftruncate to 0 | File becomes empty |
| 11 | truncate by path | Same semantics as ftruncate |
| 12 | ftruncate on read-only fd | Returns EINVAL or EBADF |
| 13 | Seek after truncate reflects new size | SEEK_END uses new size |
| 14 | ftruncate mid-page (not page-aligned) | Partial page handled correctly |
| 15 | ftruncate across page boundary | Pages beyond boundary removed |

### Suite 3: `stat.test.ts` — File Metadata

**Source**: Emscripten `test_unistd_stat`, CPython POSIX compliance fixes

| # | Test | Expected |
|---|------|----------|
| 1 | stat on regular file | S_IFREG set, correct size |
| 2 | stat on directory | S_IFDIR set |
| 3 | stat on symlink (should follow) | Returns target's stat |
| 4 | lstat on symlink (should not follow) | Returns symlink's own stat |
| 5 | fstat on open fd | Matches stat on same path |
| 6 | stat on nonexistent path | ENOENT |
| 7 | stat on path component not a dir | ENOTDIR (e.g. `/file/child`) |
| 8 | size updates after write | stat.size reflects written bytes |
| 9 | size updates after truncate | stat.size reflects new length |
| 10 | mtime updates after write | mtime > previous mtime |
| 11 | ctime updates after chmod | ctime > previous ctime |
| 12 | mode bits match what was set | chmod then stat round-trips |
| 13 | stat distinguishes atime/mtime/ctime | Three distinct timestamps |
| 14 | fstat on fd with no path (unlinked file) | Still works, returns valid stat |
| 15 | stat on `/` (root) | S_IFDIR, no error |

### Suite 4: `directories.test.ts` — Directory Operations

**Source**: Emscripten `test_fs_readdir`, CPython `mkdir("a/b/..")` fix

| # | Test | Expected |
|---|------|----------|
| 1 | mkdir creates directory | stat confirms S_IFDIR |
| 2 | mkdir nested path (parent exists) | Success |
| 3 | mkdir when parent doesn't exist | ENOENT |
| 4 | mkdir on existing directory | EEXIST |
| 5 | mkdir on existing file | EEXIST |
| 6 | mkdir with trailing slash | Same as without (or EISDIR per CPython fix) |
| 7 | rmdir on empty directory | Success, gone |
| 8 | rmdir on non-empty directory | ENOTEMPTY |
| 9 | rmdir on file | ENOTDIR |
| 10 | rmdir on nonexistent path | ENOENT |
| 11 | readdir lists `.` and `..` | Both present |
| 12 | readdir lists created files | All created entries appear |
| 13 | readdir after unlink | Removed entry absent |
| 14 | readdir on file (not dir) | ENOTDIR |
| 15 | readdir on nonexistent path | ENOENT |
| 16 | lookup resolves nested path | `/a/b/c` works when all components exist |
| 17 | lookup with `..` in path | `/a/b/../c` resolves correctly |
| 18 | `mkdir("a/b/..")` where `a` exists | Returns EEXIST (not create `a` again) |

### Suite 5: `rename.test.ts` — Rename Semantics

**Source**: Emscripten memfs/nodefs rename fixes, CPython rename-over-existing fix

| # | Test | Expected |
|---|------|----------|
| 1 | Rename file to new name | Old gone, new has same content |
| 2 | Rename file over existing file | Target replaced atomically |
| 3 | Rename preserves file content | Byte-for-byte identical |
| 4 | Rename preserves open fd | fd still readable after rename |
| 5 | Rename directory | All children accessible under new path |
| 6 | Rename to same path | No-op, no error |
| 7 | Rename nonexistent source | ENOENT |
| 8 | Rename file to existing directory | EISDIR |
| 9 | Rename directory to existing file | ENOTDIR |
| 10 | Rename to path where parent doesn't exist | ENOENT |
| 11 | Rename dir over non-empty dir | ENOTEMPTY |
| 12 | stat after rename shows updated path | stat(old) fails, stat(new) succeeds |
| 13 | Rename updates mtime of parent dirs | Both old and new parent mtime change |

### Suite 6: `unlink.test.ts` — File Removal

**Source**: POSIX unlink semantics

| # | Test | Expected |
|---|------|----------|
| 1 | unlink removes file | stat returns ENOENT |
| 2 | unlink nonexistent file | ENOENT |
| 3 | unlink directory | EISDIR (use rmdir instead) |
| 4 | unlink file with open fd | fd still usable until closed |
| 5 | After unlink + close, data gone | Cannot reopen |
| 6 | readdir after unlink | Entry absent |
| 7 | unlink then create same name | New file, not old data |
| 8 | Parent dir mtime updates after unlink | mtime changes |

### Suite 7: `symlinks.test.ts` — Symbolic Links

**Source**: CPython symlink path resolution fixes

| # | Test | Expected |
|---|------|----------|
| 1 | Create symlink, readlink | Returns target path |
| 2 | open through symlink | Reads target file content |
| 3 | write through symlink | Modifies target file |
| 4 | stat follows symlink | Returns target's stat |
| 5 | lstat does not follow | Returns symlink node's stat |
| 6 | Symlink to nonexistent target | symlink succeeds (dangling link) |
| 7 | open dangling symlink | ENOENT |
| 8 | Nested symlinks (link → link → file) | Resolves full chain |
| 9 | Symlink loop (a → b → a) | ELOOP |
| 10 | unlink symlink | Removes link, not target |
| 11 | rename symlink | Link moves, target unchanged |
| 12 | readdir shows symlink | Listed in parent directory |

### Suite 8: `permissions.test.ts` — Mode Bits

| # | Test | Expected |
|---|------|----------|
| 1 | chmod sets mode | stat reflects new mode |
| 2 | Default file mode | 0o666 (or as specified at creation) |
| 3 | Default directory mode | 0o777 (or as specified) |
| 4 | Mode preserved across rename | Same mode after rename |
| 5 | Mode preserved across persistence | Same mode after sync+reload |

### Suite 9: `errno.test.ts` — Error Code Correctness

**Source**: CPython emscripten POSIX compliance tracker (python/cpython#127146)

| # | Test | Expected errno |
|---|------|---------------|
| 1 | open nonexistent file without O_CREAT | ENOENT |
| 2 | open directory for writing | EISDIR |
| 3 | read/write on closed fd | EBADF |
| 4 | mkdir where component is a file | ENOTDIR |
| 5 | rmdir on file | ENOTDIR |
| 6 | unlink on directory | EISDIR |
| 7 | rename src doesn't exist | ENOENT |
| 8 | truncate nonexistent file | ENOENT |
| 9 | open with O_CREAT + O_EXCL on existing | EEXIST |
| 10 | readlink on non-symlink | EINVAL |
| 11 | rmdir on non-empty dir | ENOTEMPTY |
| 12 | create file with path ending in `/` | EISDIR |
| 13 | SEEK_SET to negative offset | EINVAL |
| 14 | write to O_RDONLY fd | EBADF |
| 15 | read from O_WRONLY fd | EBADF |
| 16 | open path with empty component (`//a`) | Handled (not crash) |
| 17 | stat with ENOTDIR in path prefix | ENOTDIR |

### Suite 10: `edge-cases.test.ts` — CPython-Discovered Bugs & Boundaries

**Source**: python/cpython#127146, Emscripten issue tracker

| # | Test | Description |
|---|------|-------------|
| 1 | Write exactly 0 bytes | No error, no size change |
| 2 | Read with length 0 | Returns 0, no error |
| 3 | File with special chars in name | `a b`, `a\nb`, unicode |
| 4 | Very long filename (255 chars) | Succeeds |
| 5 | Filename too long (> PATH_MAX) | ENAMETOOLONG |
| 6 | Write at position > current size | Gap zero-filled |
| 7 | Many files in one directory (1000+) | readdir returns all |
| 8 | Deep nesting (20 levels) | Paths resolve correctly |
| 9 | fstatfs works | Returns non-error result |
| 10 | readdir on `/proc/self/fd` equivalent | If supported, returns open fds |
| 11 | rename file on top of itself | No-op, data preserved |
| 12 | Open same file twice, write via one fd, read via other | Sees written data |
| 13 | Close and reopen preserves data | Data survives |

### Suite 11: `large-io.test.ts` — Multi-Page & Boundary Writes

**Source**: Critical for page-cached FS correctness

| # | Test | Description |
|---|------|-------------|
| 1 | Write 1 byte at page boundary - 1 | Stays in single page |
| 2 | Write 1 byte at page boundary | Starts new page |
| 3 | Write spanning exactly 2 pages | Both pages correct |
| 4 | Write spanning 3+ pages | All pages correct |
| 5 | Write exactly N * PAGE_SIZE bytes | All pages, no partial |
| 6 | Write N * PAGE_SIZE + 1 bytes | Last page has 1 byte |
| 7 | Read spanning page boundary | Seamless data, no gap |
| 8 | Partial page overwrite | Only changed bytes differ |
| 9 | Write 1 MB of data, read back | All bytes correct |
| 10 | Alternating small writes across pages | No corruption at boundaries |
| 11 | Truncate to mid-page, then write at end | Partial page zeroed correctly |
| 12 | Sequential writes filling exact pages | No off-by-one at boundaries |

### Suite 12: `concurrent-fds.test.ts` — Multiple File Descriptors

| # | Test | Description |
|---|------|-------------|
| 1 | Two fds to same file, independent seek positions | Each has own position |
| 2 | Write via fd1, read via fd2 | fd2 sees fd1's write |
| 3 | O_APPEND fd + normal fd | Append always goes to end |
| 4 | Close one fd, other still works | No interference |
| 5 | Unlink file with open fd | fd still readable |
| 6 | Unlink then close, file gone | Data cleaned up after last close |
| 7 | Open 100 fds to different files | All work, no fd exhaustion |
| 8 | dup-like behavior (if supported) | Shared seek position |

### Suite 13: `persistence.test.ts` — Durability Round-Trips

**Source**: PGlite `dumpDataDir()` / reload patterns

| # | Test | Description |
|---|------|-------------|
| 1 | Write → sync → unmount → remount → read | Data survives |
| 2 | Create directory tree → sync → reload | Tree intact |
| 3 | Rename → sync → reload | New name persisted |
| 4 | Unlink → sync → reload | File gone |
| 5 | Truncate → sync → reload | Size persisted |
| 6 | Mode bits → sync → reload | Permissions preserved |
| 7 | Large file (multi-page) → sync → reload | All pages intact |
| 8 | Many files (100+) → sync → reload | All present |
| 9 | Timestamps → sync → reload | mtime/ctime preserved |
| 10 | Rapid write-sync cycles | No data loss |

## Test Infrastructure

### Running Tests

```bash
# Run all conformance suites against MEMFS (reference)
npm test -- --suite conformance --backend memfs

# Run against IDBFS (via fake-indexeddb)
npm test -- --suite conformance --backend idbfs

# Run against BadFS variants (expect failures)
npm test -- --suite conformance --backend badfs

# Run against tomefs (our implementation, once it exists)
npm test -- --suite conformance --backend tomefs
```

### Vitest Parameterized Pattern

```typescript
// conformance/runner.ts
import { describe, it, expect } from 'vitest';

type BackendFactory = () => Promise<TestFS>;

export function runConformanceSuite(
  name: string,
  createBackend: BackendFactory,
  tests: (fs: () => TestFS) => void
) {
  describe(`[${name}]`, () => {
    let fs: TestFS;
    beforeEach(async () => { fs = await createBackend(); await fs.mount(); });
    afterEach(async () => { await fs.unmount(); });
    tests(() => fs);
  });
}

// Usage in each suite file:
export function basicIOTests(fs: () => TestFS) {
  it('write then read round-trips', () => { ... });
  // ...
}

// Top-level runner:
for (const [name, factory] of backends) {
  runConformanceSuite(name, factory, basicIOTests);
  runConformanceSuite(name, factory, seekTruncateTests);
  // ...
}
```

### BadFS Validation Matrix

Each BadFS defect must cause **at least one** specific test to fail. We validate this as a meta-test:

```typescript
describe('BadFS validation', () => {
  for (const [defectId, expectedFailingSuites] of badfsDefects) {
    it(`defect "${defectId}" is caught by ${expectedFailingSuites}`, async () => {
      const badfs = new BadFS(memfs, defectId);
      const results = await runAllSuites(badfs);
      for (const suite of expectedFailingSuites) {
        expect(results[suite].failures).toBeGreaterThan(0);
      }
    });
  }
});
```

## Test Sources & References

### Primary Sources to Port From

1. **Emscripten test suite** (`emscripten-core/emscripten`)
   - `test/test_core.py` → `test_files`, `test_unistd_*` patterns
   - `test/test_other.py` → `test_fs_*` JS-level FS API tests
   - `test/test_core.py` → `with_all_fs` decorator pattern (backend-agnostic tests)
   - `test/wasmfs/` → `wasmfs_getdents.c`, `wasmfs_jsfile.c` (backend contract tests)

2. **CPython Emscripten compliance** (`python/cpython#127146`)
   - Symlink path resolution edge cases
   - `readdir` on `/proc/self/fd`
   - `fstat` on descriptors with no name
   - Rename over existing file (memfs + nodefs)
   - atime/mtime/ctime distinction
   - EISDIR for paths ending in `/`
   - `mkdir("a/b/..")` returning EEXIST
   - `fstatfs` actually working

3. **WasmFS backend interface** (`emscripten-core/emscripten/system/lib/wasmfs/`)
   - Abstract Backend class (defines required operations)
   - MemoryBackend, NodeBackend, OpfsBackend as reference implementations

4. **PGlite persistence tests** (`packages/pglite/tests/`)
   - `dumpDataDir()` / reload round-trip pattern
   - `fs` constructor option for custom FS injection

5. **PostgreSQL regression suite** (`src/test/regress/sql/`)
   - `copy.sql` — large sequential writes
   - `largeobject.sql` — blob I/O patterns
   - `temp.sql` — temp file creation/deletion
   - `create_index.sql` — write-heavy index builds
   - (These are Layer 3/4 integration tests, not part of initial conformance suite)

### What We Explicitly Do NOT Test (Yet)

- WasmFS C-level backend interface (Layer 2 — deferred until we decide on WasmFS vs JS FS)
- Full PostgreSQL regression SQL (Layer 3 — requires PGlite integration)
- SAB+Atomics bridge correctness (Phase 3 implementation concern)
- Performance benchmarks (separate from correctness)

## Implementation Order

1. **Define `TestFS` interface** and MEMFS adapter
2. **Write Suite 1 (`basic-io`)** — validate against MEMFS
3. **Write BadFS with `off-by-one-read`** — validate Suite 1 catches it
4. **Proceed through Suites 2–13**, adding BadFS defects in parallel
5. **Add IDBFS adapter** (via `fake-indexeddb`) — run all suites
6. **Meta-test**: validate every BadFS defect is caught
7. **Freeze test suite** — this is the contract our implementation must satisfy

## Success Criteria

- All 13 suites pass against MEMFS
- All 13 suites pass against IDBFS (via fake-indexeddb)
- Every BadFS defect is caught by its designated suite
- No test requires implementation-specific knowledge (pure POSIX/Emscripten FS semantics)
- Test suite runs in < 30 seconds via `vitest`
