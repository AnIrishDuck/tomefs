# Conformance Test Plan

## Goal

Before writing any filesystem implementation code, build an exhaustive conformance test suite that:

1. **Defines correctness** — every POSIX/Emscripten FS behavior our implementation must satisfy
2. **Validates against a known-good FS** — all tests pass against Emscripten's MEMFS
3. **Catches real bugs** — a deliberately broken "BadFS" with subtle defects must fail specific tests

This is Phase 1. No implementation code until the test suite is complete and validated.

## Source Inventory: What Exists Upstream

### Emscripten Test Suite (Primary Source)

The Emscripten repo (`emscripten-core/emscripten`) contains **~18 self-contained C test files** that exercise exactly the POSIX FS operations we need. These are assertion-based, use only standard POSIX headers, and need minimal adaptation.

#### Tier 1 — Drop-in portable (zero emscripten dependencies)

| File | Operations tested | ~LOC |
|------|------------------|------|
| `test/unistd/close.c` | fsync, close, EBADF | 30 |
| `test/unistd/unlink.c` | unlink, rmdir, mkdir, symlink, chmod, access, ENOENT/EISDIR/ENOTDIR/ENOTEMPTY/EACCES | 130 |
| `test/unistd/truncate.c` | ftruncate grow/shrink, truncate by path, readonly, O_TRUNC, negative size | 100 |
| `test/unistd/links.c` | symlink, readlink, ELOOP, relative/absolute symlink path resolution | 160 |
| `test/stdio/test_rename.c` | rename files/dirs, overwrite, ancestors, EISDIR/ENOTDIR/ENOTEMPTY/EACCES/EINVAL, empty paths | 180 |
| `test/dirent/test_readdir.c` | opendir, readdir, `.`/`..`, rewinddir, telldir/seekdir, scandir, ENOENT/EACCES/ENOTDIR | 140 |
| `test/test_files.c` | fopen/fclose/fread/fwrite/fseek, tmpfile, mkstemp, O_TRUNC | 130 |
| `test/fs/test_fs_rename_on_existing.c` | rename file over existing file | 25 |
| `test/fs/test_fs_mkdir_dotdot.c` | `mkdir("a/b/..")` → EEXIST, `mkdir("a/b/.")` → EEXIST | 20 |
| `test/fs/test_fs_symlink_resolution.c` | create file/dir through `symlink/../path` | 30 |
| `test/fs/test_fs_enotdir.c` | ENOTDIR when mkdir under file, EISDIR for path ending in `/` | 15 |
| `test/fs/test_stat_unnamed_file_descriptor.c` | fstat/fchmod/ftruncate on fd after unlink | 20 |
| `test/fs/test_writev.c` | pwritev scattered I/O | 20 |

#### Tier 2 — Easily adaptable (strip `#ifdef WASMFS` guards)

| File | Operations tested | ~LOC | Adaptation needed |
|------|------------------|------|-------------------|
| `test/wasmfs/wasmfs_seek.c` | read, write, lseek (SEEK_SET/CUR/END), pread, pwrite | 120 | Remove WASMFS comment, strip guards |
| `test/wasmfs/wasmfs_stat.c` | fstat files/dirs, stat, lstat, inode persistence, mtime after write, utime | 170 | Strip `#ifdef WASMFS` on block count assertions |
| `test/wasmfs/wasmfs_mkdir.c` | mkdir mode bits, ENOENT/EEXIST/ENOTDIR/ENAMETOOLONG | 80 | Strip WASMFS guard on ENAMETOOLONG |
| `test/wasmfs/wasmfs_create.c` | open+write+read, O_EXCL+O_CREAT, O_DIRECTORY, zero-length r/w | 80 | Strip WASMFS guard on ENAMETOOLONG |
| `test/wasmfs/wasmfs_open_append.c` | O_APPEND (write always to end, even after seek) | 50 | None |
| `test/wasmfs/wasmfs_getdents.c` | getdents, scandir, EBADF/EINVAL/ENOTDIR, directory seek | 100 | Remove one `EM_ASM` JS readdir block |
| `test/wasmfs/wasmfs_dup.c` | dup/dup2, shared seek position between duped fds, EBADF | 100 | None |

#### Tier 3 — Needs rewrite of setup (EM_ASM for file creation)

| File | Operations tested | Issue |
|------|------------------|-------|
| `test/unistd/access.c` | access/faccessat, chmod, fchmod, lchmod, rename | Setup uses EM_ASM; rewrite to use C APIs |
| `test/fs/test_fs_js_api.c` | FS.open, FS.readFile, FS.writeFile, FS.rename, FS.readlink, FS.read, FS.rmdir, FS.close, FS.truncate, FS.utime, FS.mkdirTree (~300 LOC) | Entirely EM_JS — port test logic to our harness |

#### Tier 4 — Not portable (browser/IDBFS-specific)

| File | Why not portable |
|------|-----------------|
| `test/fs/test_idbfs_sync.c` | Two-phase browser test with IDBFS mount, FS.syncfs, REPORT_RESULT |
| `test/fs/test_idbfs_fsync.c` | Same two-phase IDBFS pattern |
| `test/fs/test_idbfs_autopersist.c` | IDBFS auto-persistence, multi-phase |
| `test/wasmfs/wasmfs_opfs.c` | Requires OPFS browser API |
| `test/fs/test_nodefs_*.c` | Requires Node.js filesystem |

**Total portable LOC: ~1,740** across Tiers 1-2 (20 files), covering ~165 distinct assertions.

### CPython #127146 — Edge Case Checklist (No portable tests)

The CPython Emscripten compliance effort (python/cpython#127146) is **bug reports + skip-list management**, not standalone test files. The edge cases they found are already covered by the Emscripten test files above:

- `mkdir("a/b/..")` → EEXIST *(covered by `test_fs_mkdir_dotdot.c`)*
- rename file over existing file *(covered by `test_fs_rename_on_existing.c`)*
- symlink path resolution through intermediate components *(covered by `test_fs_symlink_resolution.c`)*
- fstat on fd with no name (unlinked file) *(covered by `test_stat_unnamed_file_descriptor.c`)*
- EISDIR for path ending in `/` *(covered by `test_fs_enotdir.c`)*
- atime/mtime/ctime distinction *(covered by `wasmfs_stat.c`)*
- File modification time precision (nanoseconds through 53-bit JS Number) *(noted, not critical for us)*

**Value**: Serves as a validation checklist, not a source of tests. All relevant Emscripten fixes landed in Emscripten 4.0.2.

### emscripten-core/posixtestsuite — Not Useful

Focuses on pthreads, signals, semaphores, IPC. **No filesystem tests.** 185 interface directories, none for `open`/`read`/`write`/`stat`/`mkdir` etc.

### PGlite Tests — SQL-level Only

All existing PGlite tests (`packages/pglite/tests/`) exercise the SQL query level. **Zero FS-level tests exist.** Key findings:

- `dump.test.js` — dumpDataDir/loadDataDir round-trips (SQL-level)
- `targets/runtimes/base.js` — parameterized backend tests (SQL-level)
- `BaseFilesystem` abstract class in `base.ts` — defines the interface we'd implement
- `fs` constructor option — clean injection point for custom FS

Useful for **Layer 3/4** integration tests (run Postgres SQL against our FS), but nothing to port for FS conformance.

## Port Strategy

### Format Decision: C or TypeScript?

**Option A — Port to TypeScript/vitest (calling Emscripten JS FS API)**
- Translate C test logic to TS calling `FS.open`, `FS.read`, `FS.write`, `FS.stat`, `FS.mkdir`, etc.
- Same vitest infrastructure as existing tome tests
- Fast iteration, easy to run, easy to debug
- ~1,740 LOC C → ~1,200 LOC TS (mechanical translation)
- Can run against MEMFS natively (it's just JS)

**Option B — Keep C files, compile with emcc**
- Keep C files verbatim, compile with `emcc` mounted against our FS
- Most faithful to real Postgres usage (C → WASM → FS calls)
- Requires Emscripten toolchain in CI
- Harder to debug, slower iteration

**Option C — Both (phased)**
- Start with TS tests for fast iteration (Phase 1)
- Add C-compiled integration tests later (Phase 3, when FS implementation exists)

### Recommended: Option A first, Option C eventually

The C test logic is simple assertion-based code. The translation to TS is mechanical:

```c
// C (emscripten)
int fd = open("/test", O_RDWR | O_CREAT, 0777);
assert(fd >= 0);
write(fd, "hello", 5);
lseek(fd, 0, SEEK_SET);
char buf[10];
int n = read(fd, buf, 10);
assert(n == 5);
close(fd);
```

```typescript
// TS (our test)
const fd = FS.open('/test', FS.O_RDWR | FS.O_CREAT, 0o777);
expect(fd).toBeGreaterThanOrEqual(0);
FS.write(fd, new Uint8Array([104, 101, 108, 108, 111]), 0, 5);
FS.llseek(fd, 0, 0 /* SEEK_SET */);
const buf = new Uint8Array(10);
const n = FS.read(fd, buf, 0, 10);
expect(n).toBe(5);
FS.close(fd);
```

## Port Order

Port in dependency order, starting with the simplest and most foundational:

### Batch 1: Core I/O
1. `wasmfs_create.c` — open, write, read, close, O_CREAT, O_EXCL, O_DIRECTORY
2. `wasmfs_seek.c` — lseek, pread, pwrite
3. `wasmfs_open_append.c` — O_APPEND semantics
4. `test_files.c` — fopen/fclose/fread/fwrite/fseek, O_TRUNC

### Batch 2: Metadata & Directories
5. `wasmfs_stat.c` — fstat, stat, lstat, mtime updates, utime
6. `wasmfs_mkdir.c` — mkdir, mode bits, ENOENT/EEXIST/ENOTDIR
7. `test_readdir.c` — opendir, readdir, `.`/`..`, rewinddir, seekdir, scandir
8. `wasmfs_getdents.c` — getdents, directory entry types

### Batch 3: Rename & Unlink
9. `test_rename.c` — rename files/dirs, all error cases
10. `test_fs_rename_on_existing.c` — rename over existing file
11. `unlink.c` — unlink, rmdir, all error cases

### Batch 4: Links & FD semantics
12. `links.c` — symlink, readlink, ELOOP, path resolution
13. `test_fs_symlink_resolution.c` — create through symlink/../path
14. `wasmfs_dup.c` — dup/dup2, shared seek position
15. `test_stat_unnamed_file_descriptor.c` — fstat on unlinked fd

### Batch 5: Truncate & Edge Cases
16. `truncate.c` — ftruncate, truncate, readonly, negative size
17. `test_fs_mkdir_dotdot.c` — mkdir with `..` and `.`
18. `test_fs_enotdir.c` — ENOTDIR, EISDIR for trailing `/`
19. `test_writev.c` — pwritev

### Batch 6: Persistence (custom, no upstream source)
20. Write → sync → unmount → remount → read round-trips
21. Large file (multi-page) round-trips
22. Directory tree persistence

## BadFS Validation

After all suites are ported and passing against MEMFS, create a `BadFS` wrapper that injects specific defects. Each defect must cause at least one test to fail.

| Defect ID | What it does | Which batch should catch it |
|-----------|-------------|---------------------------|
| `off-by-one-read` | Returns `length - 1` bytes on read | Batch 1 |
| `no-mtime-update` | Doesn't update mtime on write | Batch 2 |
| `rename-no-overwrite` | Rename fails silently when target exists | Batch 3 |
| `truncate-no-shrink` | ftruncate doesn't reduce file size | Batch 5 |
| `readdir-missing-dot` | readdir omits `.` and `..` | Batch 2 |
| `symlink-no-resolve` | open() doesn't follow symlinks | Batch 4 |
| `wrong-errno-enoent` | Returns EACCES instead of ENOENT | Batch 3 |
| `seek-end-off-by-one` | SEEK_END off by one byte | Batch 1 |
| `partial-page-corrupt` | Writes spanning page boundary corrupt last byte | Batch 6 |
| `persistence-drop-page` | Drops the last page on sync | Batch 6 |

## Success Criteria

- All ported tests pass against MEMFS
- Every BadFS defect is caught by its designated batch
- No test requires implementation-specific knowledge (pure POSIX/Emscripten FS semantics)
- A `@fast` tagged subset of core smoke tests exists for quick iteration
- The full suite prioritizes completeness over speed
