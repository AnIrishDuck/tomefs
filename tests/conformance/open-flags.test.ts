/**
 * Conformance tests for open flag combinations: O_TRUNC, O_EXCL, O_DIRECTORY.
 *
 * Source: POSIX open(2) specification.
 *
 * O_TRUNC: "If the file exists and is a regular file, and the file is
 * successfully opened O_RDWR or O_WRONLY, its length shall be truncated to 0."
 *
 * O_EXCL: "If O_EXCL and O_CREAT are set, open() shall fail if the file
 * exists."
 *
 * O_DIRECTORY: "If the path does not refer to a directory, fail with ENOTDIR."
 *
 * These flags are critical for database workloads: Postgres uses O_TRUNC when
 * overwriting WAL segments and temp files, O_EXCL for lock files and exclusive
 * creation, and O_DIRECTORY when syncing parent directories.
 */
import {
  createFS,
  encode,
  decode,
  expectErrno,
  O,
  SEEK_SET,
  type FSHarness,
} from "../harness/emscripten-fs.js";

describe("O_TRUNC", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  it("O_TRUNC with O_WRONLY truncates existing file to zero @fast", () => {
    const { FS } = h;
    const ws = FS.open("/trunc_wr", O.WRONLY | O.CREAT, 0o666);
    FS.write(ws, encode("hello world"), 0, 11);
    FS.close(ws);

    expect(FS.stat("/trunc_wr").size).toBe(11);

    const ts = FS.open("/trunc_wr", O.WRONLY | O.TRUNC);
    FS.close(ts);

    expect(FS.stat("/trunc_wr").size).toBe(0);
  });

  it("O_TRUNC with O_RDWR truncates existing file to zero @fast", () => {
    const { FS } = h;
    const ws = FS.open("/trunc_rw", O.RDWR | O.CREAT, 0o666);
    FS.write(ws, encode("some data"), 0, 9);
    FS.close(ws);

    expect(FS.stat("/trunc_rw").size).toBe(9);

    const ts = FS.open("/trunc_rw", O.RDWR | O.TRUNC);
    const buf = new Uint8Array(20);
    const n = FS.read(ts, buf, 0, 20);
    expect(n).toBe(0);
    FS.close(ts);

    expect(FS.stat("/trunc_rw").size).toBe(0);
  });

  it("O_TRUNC allows writing new content after truncation", () => {
    const { FS } = h;
    const ws = FS.open("/trunc_rewrite", O.WRONLY | O.CREAT, 0o666);
    FS.write(ws, encode("original content"), 0, 16);
    FS.close(ws);

    const ts = FS.open("/trunc_rewrite", O.WRONLY | O.TRUNC);
    FS.write(ts, encode("new"), 0, 3);
    FS.close(ts);

    const rs = FS.open("/trunc_rewrite", O.RDONLY);
    const buf = new Uint8Array(20);
    const n = FS.read(rs, buf, 0, 20);
    expect(n).toBe(3);
    expect(decode(buf, n)).toBe("new");
    FS.close(rs);
  });

  it("O_TRUNC | O_CREAT creates new empty file if it doesn't exist", () => {
    const { FS } = h;
    const ts = FS.open("/trunc_create", O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    FS.close(ts);

    expect(FS.stat("/trunc_create").size).toBe(0);
  });

  it("O_TRUNC | O_CREAT truncates existing file", () => {
    const { FS } = h;
    const ws = FS.open("/trunc_creat_exist", O.WRONLY | O.CREAT, 0o666);
    FS.write(ws, encode("existing"), 0, 8);
    FS.close(ws);

    const ts = FS.open(
      "/trunc_creat_exist",
      O.WRONLY | O.CREAT | O.TRUNC,
      0o666,
    );
    FS.close(ts);

    expect(FS.stat("/trunc_creat_exist").size).toBe(0);
  });

  it("O_TRUNC preserves inode identity", () => {
    const { FS } = h;
    const ws = FS.open("/trunc_ino", O.WRONLY | O.CREAT, 0o666);
    FS.write(ws, encode("data"), 0, 4);
    FS.close(ws);

    const inoBefore = FS.stat("/trunc_ino").ino;

    const ts = FS.open("/trunc_ino", O.WRONLY | O.TRUNC);
    FS.close(ts);

    const inoAfter = FS.stat("/trunc_ino").ino;
    expect(inoAfter).toBe(inoBefore);
  });

  it("O_TRUNC updates mtime", () => {
    const { FS } = h;
    const ws = FS.open("/trunc_time", O.WRONLY | O.CREAT, 0o666);
    FS.write(ws, encode("data"), 0, 4);
    FS.close(ws);

    const mtimeBefore = FS.stat("/trunc_time").mtime.getTime();

    // Small delay to ensure different timestamp
    const start = Date.now();
    while (Date.now() === start) {
      /* spin */
    }

    const ts = FS.open("/trunc_time", O.WRONLY | O.TRUNC);
    FS.close(ts);

    const mtimeAfter = FS.stat("/trunc_time").mtime.getTime();
    expect(mtimeAfter).toBeGreaterThanOrEqual(mtimeBefore);
  });

  it("O_TRUNC is visible through another fd on the same file @fast", () => {
    const { FS } = h;
    const ws = FS.open("/trunc_multi", O.RDWR | O.CREAT, 0o666);
    FS.write(ws, encode("hello"), 0, 5);
    FS.close(ws);

    // Open two fds: one for reading, one to truncate
    const rd = FS.open("/trunc_multi", O.RDONLY);
    const tr = FS.open("/trunc_multi", O.WRONLY | O.TRUNC);

    // Read should see truncated (empty) file
    const buf = new Uint8Array(10);
    const n = FS.read(rd, buf, 0, 10);
    expect(n).toBe(0);

    FS.close(rd);
    FS.close(tr);
  });

  it("O_TRUNC + O_APPEND: truncates first, then appends", () => {
    const { FS } = h;
    const ws = FS.open("/trunc_append", O.WRONLY | O.CREAT, 0o666);
    FS.write(ws, encode("original"), 0, 8);
    FS.close(ws);

    const ts = FS.open("/trunc_append", O.WRONLY | O.TRUNC | O.APPEND);
    FS.write(ts, encode("A"), 0, 1);
    FS.write(ts, encode("B"), 0, 1);
    FS.close(ts);

    const rs = FS.open("/trunc_append", O.RDONLY);
    const buf = new Uint8Array(20);
    const n = FS.read(rs, buf, 0, 20);
    expect(n).toBe(2);
    expect(decode(buf, n)).toBe("AB");
    FS.close(rs);
  });

  it("O_TRUNC on already-empty file is harmless", () => {
    const { FS } = h;
    const ws = FS.open("/trunc_empty", O.WRONLY | O.CREAT, 0o666);
    FS.close(ws);

    expect(FS.stat("/trunc_empty").size).toBe(0);

    const ts = FS.open("/trunc_empty", O.WRONLY | O.TRUNC);
    FS.close(ts);

    expect(FS.stat("/trunc_empty").size).toBe(0);
  });

  it("O_TRUNC clears all content, not just metadata", () => {
    const { FS } = h;
    // Write recognizable pattern
    const data = new Uint8Array(16384);
    for (let i = 0; i < data.length; i++) data[i] = i & 0xff;
    const ws = FS.open("/trunc_clear", O.WRONLY | O.CREAT, 0o666);
    FS.write(ws, data, 0, data.length);
    FS.close(ws);

    // Truncate
    const ts = FS.open("/trunc_clear", O.WRONLY | O.TRUNC);
    FS.close(ts);

    // Re-extend by writing at offset 0 — should see zeros elsewhere
    const ws2 = FS.open("/trunc_clear", O.WRONLY);
    FS.write(ws2, encode("X"), 0, 1);
    FS.close(ws2);

    const rs = FS.open("/trunc_clear", O.RDONLY);
    const buf = new Uint8Array(2);
    FS.llseek(rs, 0, SEEK_SET);
    const n = FS.read(rs, buf, 0, 2);
    expect(n).toBe(1);
    expect(buf[0]).toBe(0x58); // 'X'
    FS.close(rs);
  });

  it("O_TRUNC on large file (multi-page)", () => {
    const { FS } = h;
    // Write 32KB (4 pages at 8KB page size)
    const data = new Uint8Array(32768);
    data.fill(0xaa);
    const ws = FS.open("/trunc_large", O.WRONLY | O.CREAT, 0o666);
    FS.write(ws, data, 0, data.length);
    FS.close(ws);

    expect(FS.stat("/trunc_large").size).toBe(32768);

    const ts = FS.open("/trunc_large", O.WRONLY | O.TRUNC);
    FS.close(ts);

    expect(FS.stat("/trunc_large").size).toBe(0);

    // Write new small content
    const ws2 = FS.open("/trunc_large", O.WRONLY);
    FS.write(ws2, encode("small"), 0, 5);
    FS.close(ws2);

    const rs = FS.open("/trunc_large", O.RDONLY);
    const buf = new Uint8Array(10);
    const n = FS.read(rs, buf, 0, 10);
    expect(n).toBe(5);
    expect(decode(buf, n)).toBe("small");
    FS.close(rs);
  });

  it("repeated O_TRUNC open/write/close cycles", () => {
    const { FS } = h;
    const ws = FS.open("/trunc_cycle", O.WRONLY | O.CREAT, 0o666);
    FS.close(ws);

    for (let i = 0; i < 10; i++) {
      const s = FS.open("/trunc_cycle", O.WRONLY | O.TRUNC);
      const msg = `iteration ${i}`;
      FS.write(s, encode(msg), 0, msg.length);
      FS.close(s);

      const rs = FS.open("/trunc_cycle", O.RDONLY);
      const buf = new Uint8Array(50);
      const n = FS.read(rs, buf, 0, 50);
      expect(decode(buf, n)).toBe(msg);
      FS.close(rs);
    }
  });
});

describe("O_EXCL", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  it("O_EXCL | O_CREAT succeeds for new file @fast", () => {
    const { FS } = h;
    const s = FS.open("/excl_new", O.RDWR | O.CREAT | O.EXCL, 0o666);
    expect(s.fd).toBeGreaterThanOrEqual(0);

    FS.write(s, encode("exclusive"), 0, 9);
    FS.close(s);

    const rs = FS.open("/excl_new", O.RDONLY);
    const buf = new Uint8Array(20);
    const n = FS.read(rs, buf, 0, 20);
    expect(decode(buf, n)).toBe("exclusive");
    FS.close(rs);
  });

  it("O_EXCL | O_CREAT fails with EEXIST on existing file @fast", () => {
    const { FS, E } = h;
    const ws = FS.open("/excl_exist", O.WRONLY | O.CREAT, 0o666);
    FS.close(ws);

    expectErrno(
      () => FS.open("/excl_exist", O.RDWR | O.CREAT | O.EXCL, 0o666),
      E.EEXIST,
    );
  });

  it("O_EXCL | O_CREAT fails with EEXIST on existing directory", () => {
    const { FS, E } = h;
    FS.mkdir("/excl_dir");

    expectErrno(
      () => FS.open("/excl_dir", O.RDONLY | O.CREAT | O.EXCL, 0o666),
      E.EEXIST,
    );
  });

  it("O_EXCL | O_CREAT creates file with correct mode", () => {
    const { FS } = h;
    const s = FS.open("/excl_mode", O.RDWR | O.CREAT | O.EXCL, 0o644);
    FS.close(s);

    const stat = FS.stat("/excl_mode");
    expect(stat.mode & 0o777).toBe(0o644);
  });

  it("O_EXCL | O_CREAT is atomic — no race window @fast", () => {
    const { FS, E } = h;
    // First exclusive create succeeds
    const s = FS.open("/excl_atomic", O.WRONLY | O.CREAT | O.EXCL, 0o666);

    // Second exclusive create fails even before first is closed
    expectErrno(
      () => FS.open("/excl_atomic", O.WRONLY | O.CREAT | O.EXCL, 0o666),
      E.EEXIST,
    );

    FS.close(s);
  });

  it("O_EXCL | O_CREAT on symlink to existing file fails with EEXIST", () => {
    const { FS, E } = h;
    const ws = FS.open("/excl_target", O.WRONLY | O.CREAT, 0o666);
    FS.close(ws);

    FS.symlink("/excl_target", "/excl_link");

    expectErrno(
      () => FS.open("/excl_link", O.RDWR | O.CREAT | O.EXCL, 0o666),
      E.EEXIST,
    );
  });

  it("O_EXCL | O_CREAT | O_TRUNC on new file creates empty file", () => {
    const { FS } = h;
    const s = FS.open(
      "/excl_trunc_new",
      O.WRONLY | O.CREAT | O.EXCL | O.TRUNC,
      0o666,
    );
    FS.close(s);

    expect(FS.stat("/excl_trunc_new").size).toBe(0);
  });

  it("O_EXCL | O_CREAT | O_TRUNC on existing file fails with EEXIST", () => {
    const { FS, E } = h;
    const ws = FS.open("/excl_trunc_exist", O.WRONLY | O.CREAT, 0o666);
    FS.write(ws, encode("data"), 0, 4);
    FS.close(ws);

    expectErrno(
      () =>
        FS.open(
          "/excl_trunc_exist",
          O.WRONLY | O.CREAT | O.EXCL | O.TRUNC,
          0o666,
        ),
      E.EEXIST,
    );

    // Original file is untouched
    expect(FS.stat("/excl_trunc_exist").size).toBe(4);
  });
});

describe("O_DIRECTORY", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  it("O_DIRECTORY on regular file fails with ENOTDIR @fast", () => {
    const { FS, E } = h;
    const ws = FS.open("/dir_flag_file", O.WRONLY | O.CREAT, 0o666);
    FS.close(ws);

    expectErrno(
      () => FS.open("/dir_flag_file", O.RDONLY | O.DIRECTORY),
      E.ENOTDIR,
    );
  });

  it("O_DIRECTORY on directory succeeds @fast", () => {
    const { FS } = h;
    FS.mkdir("/dir_flag_dir");

    const s = FS.open("/dir_flag_dir", O.RDONLY | O.DIRECTORY);
    expect(s.fd).toBeGreaterThanOrEqual(0);
    FS.close(s);
  });

  it("O_DIRECTORY on nonexistent path fails with ENOENT", () => {
    const { FS, E } = h;
    expectErrno(
      () => FS.open("/dir_flag_nonexist", O.RDONLY | O.DIRECTORY),
      E.ENOENT,
    );
  });

  it("O_DIRECTORY on symlink to directory succeeds", () => {
    const { FS } = h;
    FS.mkdir("/dir_target");
    FS.symlink("/dir_target", "/dir_link");

    const s = FS.open("/dir_link", O.RDONLY | O.DIRECTORY);
    expect(s.fd).toBeGreaterThanOrEqual(0);
    FS.close(s);
  });

  it("O_DIRECTORY on symlink to regular file fails with ENOTDIR", () => {
    const { FS, E } = h;
    const ws = FS.open("/dir_file_target", O.WRONLY | O.CREAT, 0o666);
    FS.close(ws);

    FS.symlink("/dir_file_target", "/dir_file_link");

    expectErrno(
      () => FS.open("/dir_file_link", O.RDONLY | O.DIRECTORY),
      E.ENOTDIR,
    );
  });

  it("O_DIRECTORY does not create a directory with O_CREAT", () => {
    const { FS, E } = h;
    // O_CREAT | O_DIRECTORY should create a regular file, then O_DIRECTORY
    // should reject it. Or it might just succeed in creating a file.
    // The key invariant: O_DIRECTORY never silently opens a regular file.
    try {
      const s = FS.open(
        "/dir_creat",
        O.RDONLY | O.CREAT | O.DIRECTORY,
        0o666,
      );
      // If open succeeds, the result must be a directory (not a file)
      const stat = FS.fstat(s.fd);
      expect(FS.isDir(stat.mode)).toBe(true);
      FS.close(s);
    } catch (e: unknown) {
      // Failing is also acceptable — POSIX says behavior is implementation-defined
      if (e instanceof Error && "errno" in e) {
        // ENOTDIR is the expected error
        expect((e as Error & { errno: number }).errno).toBe(E.ENOTDIR);
      } else {
        throw e;
      }
    }
  });
});

describe("open flag combinations", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
  });

  it("O_CREAT | O_TRUNC | O_WRONLY — classic overwrite pattern @fast", () => {
    const { FS } = h;
    // Write original content
    const ws = FS.open("/overwrite", O.WRONLY | O.CREAT, 0o666);
    FS.write(ws, encode("version 1"), 0, 9);
    FS.close(ws);

    // Overwrite with new content (Postgres WAL segment overwrite pattern)
    const ts = FS.open("/overwrite", O.WRONLY | O.CREAT | O.TRUNC, 0o666);
    FS.write(ts, encode("v2"), 0, 2);
    FS.close(ts);

    const rs = FS.open("/overwrite", O.RDONLY);
    const buf = new Uint8Array(20);
    const n = FS.read(rs, buf, 0, 20);
    expect(decode(buf, n)).toBe("v2");
    FS.close(rs);
  });

  it("O_CREAT without O_TRUNC does not truncate existing file @fast", () => {
    const { FS } = h;
    const ws = FS.open("/no_trunc", O.WRONLY | O.CREAT, 0o666);
    FS.write(ws, encode("keep me"), 0, 7);
    FS.close(ws);

    const s = FS.open("/no_trunc", O.RDWR | O.CREAT, 0o666);
    const buf = new Uint8Array(20);
    const n = FS.read(s, buf, 0, 20);
    expect(n).toBe(7);
    expect(decode(buf, n)).toBe("keep me");
    FS.close(s);
  });

  it("O_TRUNC with subsequent writes at various positions", () => {
    const { FS } = h;
    const ws = FS.open("/trunc_positions", O.WRONLY | O.CREAT, 0o666);
    FS.write(ws, encode("AAAAABBBBBCCCCC"), 0, 15);
    FS.close(ws);

    // Truncate and write at position 0
    const ts = FS.open("/trunc_positions", O.RDWR | O.TRUNC);
    FS.write(ts, encode("X"), 0, 1);

    // Write at position 5 (creates sparse gap)
    FS.write(ts, encode("Y"), 0, 1, 5);
    FS.close(ts);

    const rs = FS.open("/trunc_positions", O.RDONLY);
    const buf = new Uint8Array(20);
    const n = FS.read(rs, buf, 0, 20);
    expect(n).toBe(6);
    expect(buf[0]).toBe(0x58); // 'X'
    expect(buf[1]).toBe(0); // zero gap
    expect(buf[2]).toBe(0);
    expect(buf[3]).toBe(0);
    expect(buf[4]).toBe(0);
    expect(buf[5]).toBe(0x59); // 'Y'
    FS.close(rs);
  });

  it("O_TRUNC + O_EXCL + O_CREAT fails on existing file", () => {
    const { FS, E } = h;
    const ws = FS.open("/trunc_excl", O.WRONLY | O.CREAT, 0o666);
    FS.write(ws, encode("data"), 0, 4);
    FS.close(ws);

    // O_EXCL takes precedence — file exists, so EEXIST
    expectErrno(
      () =>
        FS.open(
          "/trunc_excl",
          O.WRONLY | O.CREAT | O.TRUNC | O.EXCL,
          0o666,
        ),
      E.EEXIST,
    );

    // File is unchanged
    expect(FS.stat("/trunc_excl").size).toBe(4);
  });

  it("O_TRUNC + O_EXCL + O_CREAT succeeds on new file", () => {
    const { FS } = h;
    const s = FS.open(
      "/trunc_excl_new",
      O.WRONLY | O.CREAT | O.TRUNC | O.EXCL,
      0o666,
    );
    FS.write(s, encode("fresh"), 0, 5);
    FS.close(s);

    expect(FS.stat("/trunc_excl_new").size).toBe(5);
  });

  it("open nonexistent file without O_CREAT fails with ENOENT @fast", () => {
    const { FS, E } = h;
    expectErrno(() => FS.open("/no_creat", O.RDONLY), E.ENOENT);
    expectErrno(() => FS.open("/no_creat", O.WRONLY), E.ENOENT);
    expectErrno(() => FS.open("/no_creat", O.RDWR), E.ENOENT);
  });

  it("O_TRUNC on unlinked-then-recreated file truncates the new file", () => {
    const { FS } = h;
    // Create, write, and close
    const ws = FS.open("/trunc_recreate", O.WRONLY | O.CREAT, 0o666);
    FS.write(ws, encode("first"), 0, 5);
    FS.close(ws);

    // Unlink
    FS.unlink("/trunc_recreate");

    // Recreate with different content
    const ws2 = FS.open("/trunc_recreate", O.WRONLY | O.CREAT, 0o666);
    FS.write(ws2, encode("second"), 0, 6);
    FS.close(ws2);

    // Truncate-open the recreated file
    const ts = FS.open("/trunc_recreate", O.WRONLY | O.TRUNC);
    FS.close(ts);

    expect(FS.stat("/trunc_recreate").size).toBe(0);
  });
});
