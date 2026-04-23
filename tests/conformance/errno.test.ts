/**
 * Comprehensive errno conformance tests.
 *
 * Source: POSIX.1-2017 system interface specifications and SUSv4 error
 * definitions. Systematically verifies that every filesystem operation
 * returns the correct errno for each applicable error condition.
 *
 * Critical for database safety: Postgres checks specific error codes
 * (ENOENT, EEXIST, EISDIR, ENOTDIR, ENOSPC, etc.) to handle failures
 * correctly. Wrong error codes can cause silent data corruption or
 * unnecessary crashes.
 *
 * Ethos §2 (real POSIX semantics), §8 (additional conformance sources)
 */
import {
  createFS,
  encode,
  expectErrno,
  O,
  SEEK_SET,
  SEEK_CUR,
  SEEK_END,
  type FSHarness,
} from "../harness/emscripten-fs.js";

describe("errno conformance", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createFS();
    const { FS } = h;
    FS.mkdir("/dir", 0o777);
    FS.writeFile("/dir/child", "child-data");
    FS.writeFile("/file", "file-data");
    FS.symlink("/file", "/link");
    FS.symlink("/nonexistent-target", "/dangling");
  });

  // =================================================================
  // stat / lstat / fstat
  // =================================================================

  describe("stat errors", () => {
    it("stat non-existent path returns ENOENT @fast", () => {
      const { FS, E } = h;
      expectErrno(() => FS.stat("/nonexistent"), E.ENOENT);
    });

    it("lstat non-existent path returns ENOENT @fast", () => {
      const { FS, E } = h;
      expectErrno(() => FS.lstat("/nonexistent"), E.ENOENT);
    });

    it("stat with ENOTDIR component returns ENOTDIR", () => {
      const { FS, E } = h;
      expectErrno(() => FS.stat("/file/sub"), E.ENOTDIR);
    });

    it("lstat with ENOTDIR component returns ENOTDIR", () => {
      const { FS, E } = h;
      expectErrno(() => FS.lstat("/file/sub"), E.ENOTDIR);
    });

    it("fstat with invalid fd returns EBADF @fast", () => {
      const { FS, E } = h;
      expectErrno(() => FS.fstat(999), E.EBADF);
    });

    it("stat dangling symlink returns ENOENT", () => {
      const { FS, E } = h;
      expectErrno(() => FS.stat("/dangling"), E.ENOENT);
    });

    it("lstat dangling symlink succeeds (returns link metadata)", () => {
      const { FS } = h;
      const st = FS.lstat("/dangling");
      expect(FS.isLink(st.mode)).toBe(true);
    });

    it("stat non-existent nested path returns ENOENT", () => {
      const { FS, E } = h;
      expectErrno(() => FS.stat("/dir/nonexistent"), E.ENOENT);
    });

    it("stat deeply non-existent path returns ENOENT", () => {
      const { FS, E } = h;
      expectErrno(() => FS.stat("/no/such/deep/path"), E.ENOENT);
    });
  });

  // =================================================================
  // open
  // =================================================================

  describe("open errors", () => {
    it("open non-existent without O_CREAT returns ENOENT @fast", () => {
      const { FS, E } = h;
      expectErrno(() => FS.open("/nonexistent", O.RDONLY), E.ENOENT);
    });

    it("open with O_CREAT|O_EXCL on existing file returns EEXIST @fast", () => {
      const { FS, E } = h;
      expectErrno(
        () => FS.open("/file", O.RDWR | O.CREAT | O.EXCL, 0o666),
        E.EEXIST,
      );
    });

    it("open directory with O_WRONLY returns EISDIR @fast", () => {
      const { FS, E } = h;
      expectErrno(() => FS.open("/dir", O.WRONLY), E.EISDIR);
    });

    it("open directory with O_RDWR returns EISDIR", () => {
      const { FS, E } = h;
      expectErrno(() => FS.open("/dir", O.RDWR), E.EISDIR);
    });

    it("open with ENOTDIR component returns ENOTDIR", () => {
      const { FS, E } = h;
      expectErrno(() => FS.open("/file/sub", O.RDONLY), E.ENOTDIR);
    });

    it("open O_CREAT with ENOTDIR component returns ENOTDIR", () => {
      const { FS, E } = h;
      expectErrno(
        () => FS.open("/file/newfile", O.RDWR | O.CREAT, 0o666),
        E.ENOTDIR,
      );
    });

    it("open O_DIRECTORY on regular file returns ENOTDIR", () => {
      const { FS, E } = h;
      expectErrno(() => FS.open("/file", O.DIRECTORY), E.ENOTDIR);
    });

    it("open O_DIRECTORY on directory succeeds", () => {
      const { FS } = h;
      const stream = FS.open("/dir", O.DIRECTORY);
      expect(stream.fd).toBeGreaterThanOrEqual(0);
      FS.close(stream);
    });

    it("open O_NOFOLLOW on symlink returns ELOOP", () => {
      const { FS, E } = h;
      expectErrno(() => FS.open("/link", O.NOFOLLOW), E.ELOOP);
    });

    it("open dangling symlink without O_CREAT returns ENOENT", () => {
      const { FS, E } = h;
      expectErrno(() => FS.open("/dangling", O.RDONLY), E.ENOENT);
    });

    it("open non-existent parent with O_CREAT returns ENOENT", () => {
      const { FS, E } = h;
      expectErrno(
        () => FS.open("/nosuchdir/file", O.RDWR | O.CREAT, 0o666),
        E.ENOENT,
      );
    });
  });

  // =================================================================
  // read / write on invalid fds
  // =================================================================

  describe("read/write fd errors", () => {
    it("write to read-only fd returns EBADF @fast", () => {
      const { FS, E } = h;
      const stream = FS.open("/file", O.RDONLY);
      const buf = new Uint8Array([65, 66, 67]);
      expectErrno(() => FS.write(stream, buf, 0, 3), E.EBADF);
      FS.close(stream);
    });

    it("read from write-only fd returns EBADF @fast", () => {
      const { FS, E } = h;
      const stream = FS.open("/file", O.WRONLY);
      const buf = new Uint8Array(10);
      expectErrno(() => FS.read(stream, buf, 0, 10), E.EBADF);
      FS.close(stream);
    });

    it("read past EOF returns 0 bytes, not an error", () => {
      const { FS } = h;
      const stream = FS.open("/file", O.RDONLY);
      const size = FS.stat("/file").size;
      FS.llseek(stream, size, SEEK_SET);
      const buf = new Uint8Array(100);
      const n = FS.read(stream, buf, 0, 100);
      expect(n).toBe(0);
      FS.close(stream);
    });
  });

  // =================================================================
  // llseek
  // =================================================================

  describe("llseek errors", () => {
    it("seek to negative absolute position returns EINVAL @fast", () => {
      const { FS, E } = h;
      const stream = FS.open("/file", O.RDONLY);
      expectErrno(() => FS.llseek(stream, -1, SEEK_SET), E.EINVAL);
      FS.close(stream);
    });

    it("SEEK_CUR that results in negative position returns EINVAL", () => {
      const { FS, E } = h;
      const stream = FS.open("/file", O.RDONLY);
      expectErrno(() => FS.llseek(stream, -1, SEEK_CUR), E.EINVAL);
      FS.close(stream);
    });

    it("SEEK_END that results in negative position returns EINVAL", () => {
      const { FS, E } = h;
      const stream = FS.open("/file", O.RDONLY);
      const size = FS.stat("/file").size;
      expectErrno(
        () => FS.llseek(stream, -(size + 1), SEEK_END),
        E.EINVAL,
      );
      FS.close(stream);
    });

    it("seek past EOF is valid (creates sparse region)", () => {
      const { FS } = h;
      const stream = FS.open("/file", O.RDWR);
      const pos = FS.llseek(stream, 100000, SEEK_SET);
      expect(pos).toBe(100000);
      FS.close(stream);
    });
  });

  // =================================================================
  // truncate / ftruncate
  // =================================================================

  describe("truncate errors", () => {
    it("truncate non-existent path returns ENOENT @fast", () => {
      const { FS, E } = h;
      expectErrno(() => FS.truncate("/nonexistent", 0), E.ENOENT);
    });

    it("truncate directory returns EISDIR @fast", () => {
      const { FS, E } = h;
      expectErrno(() => FS.truncate("/dir", 0), E.EISDIR);
    });

    it("truncate with ENOTDIR component returns ENOTDIR", () => {
      const { FS, E } = h;
      expectErrno(() => FS.truncate("/file/sub", 0), E.ENOTDIR);
    });

    it("ftruncate with invalid fd returns EBADF @fast", () => {
      const { FS, E } = h;
      expectErrno(() => FS.ftruncate(999, 0), E.EBADF);
    });

    it("ftruncate to negative size returns EINVAL", () => {
      const { FS, E } = h;
      const stream = FS.open("/file", O.RDWR);
      expectErrno(() => FS.ftruncate(stream.fd, -1), E.EINVAL);
      FS.close(stream);
    });

    it("truncate to 0 clears file contents", () => {
      const { FS } = h;
      FS.truncate("/file", 0);
      expect(FS.stat("/file").size).toBe(0);
    });

    it("ftruncate to 0 clears file contents", () => {
      const { FS } = h;
      const stream = FS.open("/file", O.RDWR);
      FS.ftruncate(stream.fd, 0);
      expect(FS.fstat(stream.fd).size).toBe(0);
      FS.close(stream);
    });
  });

  // =================================================================
  // mkdir
  // =================================================================

  describe("mkdir errors", () => {
    it("mkdir existing directory returns EEXIST @fast", () => {
      const { FS, E } = h;
      expectErrno(() => FS.mkdir("/dir", 0o777), E.EEXIST);
    });

    it("mkdir at existing file path returns EEXIST", () => {
      const { FS, E } = h;
      expectErrno(() => FS.mkdir("/file", 0o777), E.EEXIST);
    });

    it("mkdir with non-existent parent returns ENOENT @fast", () => {
      const { FS, E } = h;
      expectErrno(() => FS.mkdir("/nonexistent/sub", 0o777), E.ENOENT);
    });

    it("mkdir with file component in path returns ENOTDIR", () => {
      const { FS, E } = h;
      expectErrno(() => FS.mkdir("/file/sub", 0o777), E.ENOTDIR);
    });

    it("mkdir with deeply non-existent path returns ENOENT", () => {
      const { FS, E } = h;
      expectErrno(() => FS.mkdir("/a/b/c/d", 0o777), E.ENOENT);
    });
  });

  // =================================================================
  // rmdir
  // =================================================================

  describe("rmdir errors", () => {
    it("rmdir non-existent path returns ENOENT @fast", () => {
      const { FS, E } = h;
      expectErrno(() => FS.rmdir("/nonexistent"), E.ENOENT);
    });

    it("rmdir non-empty directory returns ENOTEMPTY @fast", () => {
      const { FS, E } = h;
      expectErrno(() => FS.rmdir("/dir"), E.ENOTEMPTY);
    });

    it("rmdir a regular file returns ENOTDIR", () => {
      const { FS, E } = h;
      expectErrno(() => FS.rmdir("/file"), E.ENOTDIR);
    });

    it("rmdir with ENOTDIR component returns ENOTDIR", () => {
      const { FS, E } = h;
      expectErrno(() => FS.rmdir("/file/sub"), E.ENOTDIR);
    });

    it("rmdir empty directory succeeds", () => {
      const { FS, E } = h;
      FS.mkdir("/empty", 0o777);
      FS.rmdir("/empty");
      expectErrno(() => FS.stat("/empty"), E.ENOENT);
    });
  });

  // =================================================================
  // unlink
  // =================================================================

  describe("unlink errors", () => {
    it("unlink non-existent path returns ENOENT @fast", () => {
      const { FS, E } = h;
      expectErrno(() => FS.unlink("/nonexistent"), E.ENOENT);
    });

    it("unlink directory returns EISDIR @fast", () => {
      const { FS, E } = h;
      expectErrno(() => FS.unlink("/dir"), E.EISDIR);
    });

    it("unlink with ENOTDIR component returns ENOTDIR", () => {
      const { FS, E } = h;
      expectErrno(() => FS.unlink("/file/sub"), E.ENOTDIR);
    });

    it("unlink non-existent file in existing dir returns ENOENT", () => {
      const { FS, E } = h;
      expectErrno(() => FS.unlink("/dir/nonexistent"), E.ENOENT);
    });

    it("unlink removes file and stat returns ENOENT", () => {
      const { FS, E } = h;
      FS.writeFile("/unlinkme", "temp");
      FS.unlink("/unlinkme");
      expectErrno(() => FS.stat("/unlinkme"), E.ENOENT);
    });

    it("unlink symlink removes link, not target", () => {
      const { FS, E } = h;
      FS.unlink("/link");
      expectErrno(() => FS.lstat("/link"), E.ENOENT);
      const st = FS.stat("/file");
      expect(FS.isFile(st.mode)).toBe(true);
    });
  });

  // =================================================================
  // rename
  // =================================================================

  describe("rename errors", () => {
    it("rename non-existent source returns ENOENT @fast", () => {
      const { FS, E } = h;
      expectErrno(() => FS.rename("/nonexistent", "/dest"), E.ENOENT);
    });

    it("rename with ENOTDIR source component returns ENOTDIR", () => {
      const { FS, E } = h;
      expectErrno(() => FS.rename("/file/sub", "/dest"), E.ENOTDIR);
    });

    it("rename with ENOTDIR dest component returns ENOTDIR", () => {
      const { FS, E } = h;
      FS.writeFile("/src", "source");
      expectErrno(() => FS.rename("/src", "/file/dest"), E.ENOTDIR);
    });

    it("rename file onto directory returns EISDIR @fast", () => {
      const { FS, E } = h;
      expectErrno(() => FS.rename("/file", "/dir"), E.EISDIR);
    });

    it("rename directory onto file returns ENOTDIR", () => {
      const { FS, E } = h;
      FS.mkdir("/empty", 0o777);
      expectErrno(() => FS.rename("/empty", "/file"), E.ENOTDIR);
    });

    it("rename directory onto non-empty directory returns ENOTEMPTY", () => {
      const { FS, E } = h;
      FS.mkdir("/empty2", 0o777);
      expectErrno(() => FS.rename("/empty2", "/dir"), E.ENOTEMPTY);
    });

    it("rename directory into own descendant returns EINVAL", () => {
      const { FS, E } = h;
      FS.mkdir("/dir/sub", 0o777);
      expectErrno(() => FS.rename("/dir", "/dir/sub/moved"), E.EINVAL);
    });

    it("rename file to self is a no-op (no error)", () => {
      const { FS } = h;
      FS.rename("/file", "/file");
      expect(FS.stat("/file").size).toBeGreaterThan(0);
    });

    it("rename file to new name succeeds", () => {
      const { FS, E } = h;
      FS.rename("/file", "/renamed");
      expectErrno(() => FS.stat("/file"), E.ENOENT);
      expect(FS.stat("/renamed").size).toBeGreaterThan(0);
    });

    it("rename onto existing file replaces it", () => {
      const { FS } = h;
      FS.writeFile("/src", "source-data");
      FS.writeFile("/dst", "dest-data");
      FS.rename("/src", "/dst");
      const content = FS.readFile("/dst", { encoding: "utf8" });
      expect(content).toBe("source-data");
    });
  });

  // =================================================================
  // chmod / fchmod
  // =================================================================

  describe("chmod errors", () => {
    it("chmod non-existent path returns ENOENT @fast", () => {
      const { FS, E } = h;
      expectErrno(() => FS.chmod("/nonexistent", 0o777), E.ENOENT);
    });

    it("chmod with ENOTDIR component returns ENOTDIR", () => {
      const { FS, E } = h;
      expectErrno(() => FS.chmod("/file/sub", 0o777), E.ENOTDIR);
    });

    it("fchmod with invalid fd returns EBADF @fast", () => {
      const { FS, E } = h;
      expectErrno(() => FS.fchmod(999, 0o777), E.EBADF);
    });

    it("chmod changes mode bits", () => {
      const { FS } = h;
      FS.chmod("/file", 0o444);
      const st = FS.stat("/file");
      expect(st.mode & 0o777).toBe(0o444);
    });

    it("fchmod changes mode bits via fd", () => {
      const { FS } = h;
      const stream = FS.open("/file", O.RDONLY);
      FS.fchmod(stream.fd, 0o555);
      const st = FS.fstat(stream.fd);
      expect(st.mode & 0o777).toBe(0o555);
      FS.close(stream);
    });
  });

  // =================================================================
  // utime
  // =================================================================

  describe("utime errors", () => {
    it("utime non-existent path returns ENOENT @fast", () => {
      const { FS, E } = h;
      expectErrno(() => FS.utime("/nonexistent", 1000, 2000), E.ENOENT);
    });

    it("utime with ENOTDIR component returns ENOTDIR", () => {
      const { FS, E } = h;
      expectErrno(() => FS.utime("/file/sub", 1000, 2000), E.ENOTDIR);
    });

    it("utime updates timestamps", () => {
      const { FS } = h;
      FS.utime("/file", 5000, 6000);
      const st = FS.stat("/file");
      expect(st.atime.getTime()).toBe(5000);
      expect(st.mtime.getTime()).toBe(6000);
    });
  });

  // =================================================================
  // readdir
  // =================================================================

  describe("readdir errors", () => {
    it("readdir non-existent path returns ENOENT @fast", () => {
      const { FS, E } = h;
      expectErrno(() => FS.readdir("/nonexistent"), E.ENOENT);
    });

    it("readdir on regular file returns ENOTDIR @fast", () => {
      const { FS, E } = h;
      expectErrno(() => FS.readdir("/file"), E.ENOTDIR);
    });

    it("readdir with ENOTDIR component returns ENOTDIR", () => {
      const { FS, E } = h;
      expectErrno(() => FS.readdir("/file/sub"), E.ENOTDIR);
    });

    it("readdir on empty directory returns ['.', '..']", () => {
      const { FS } = h;
      FS.mkdir("/empty", 0o777);
      const entries = FS.readdir("/empty");
      expect(entries).toContain(".");
      expect(entries).toContain("..");
      expect(entries.length).toBe(2);
    });
  });

  // =================================================================
  // symlink / readlink
  // =================================================================

  describe("symlink/readlink errors", () => {
    it("readlink on regular file returns EINVAL @fast", () => {
      const { FS, E } = h;
      expectErrno(() => FS.readlink("/file"), E.EINVAL);
    });

    it("readlink on directory returns EINVAL", () => {
      const { FS, E } = h;
      expectErrno(() => FS.readlink("/dir"), E.EINVAL);
    });

    it("readlink non-existent returns ENOENT", () => {
      const { FS, E } = h;
      expectErrno(() => FS.readlink("/nonexistent"), E.ENOENT);
    });

    it("symlink with empty path returns ENOENT", () => {
      const { FS, E } = h;
      expectErrno(() => FS.symlink("/file", ""), E.ENOENT);
    });

    it("symlink with ENOTDIR component returns ENOTDIR", () => {
      const { FS, E } = h;
      expectErrno(() => FS.symlink("/file", "/file/lnk"), E.ENOTDIR);
    });

    it("readlink succeeds on valid symlink", () => {
      const { FS } = h;
      const target = FS.readlink("/link");
      expect(target).toBe("/file");
    });

    it("symlink to already-existing name returns EEXIST", () => {
      const { FS, E } = h;
      expectErrno(() => FS.symlink("/file", "/link"), E.EEXIST);
    });
  });

  // =================================================================
  // Cross-operation errno consistency
  // =================================================================

  describe("cross-operation errno consistency", () => {
    it("operations on paths through dangling symlink return ENOENT", () => {
      const { FS, E } = h;
      expectErrno(() => FS.stat("/dangling"), E.ENOENT);
      expectErrno(() => FS.open("/dangling", O.RDONLY), E.ENOENT);
      expectErrno(() => FS.truncate("/dangling", 0), E.ENOENT);
      expectErrno(() => FS.chmod("/dangling", 0o777), E.ENOENT);
    });

    it("operations on ENOTDIR paths all return ENOTDIR", () => {
      const { FS, E } = h;
      expectErrno(() => FS.stat("/file/sub"), E.ENOTDIR);
      expectErrno(() => FS.open("/file/sub", O.RDONLY), E.ENOTDIR);
      expectErrno(() => FS.mkdir("/file/sub", 0o777), E.ENOTDIR);
      expectErrno(() => FS.readdir("/file/sub"), E.ENOTDIR);
      expectErrno(() => FS.unlink("/file/sub"), E.ENOTDIR);
      expectErrno(() => FS.rmdir("/file/sub"), E.ENOTDIR);
      expectErrno(() => FS.truncate("/file/sub", 0), E.ENOTDIR);
      expectErrno(() => FS.chmod("/file/sub", 0o777), E.ENOTDIR);
      expectErrno(() => FS.utime("/file/sub", 1000, 2000), E.ENOTDIR);
      expectErrno(() => FS.symlink("/target", "/file/sub"), E.ENOTDIR);
    });

    it("fd operations after close return EBADF", () => {
      const { FS, E } = h;
      const stream = FS.open("/file", O.RDWR);
      const fd = stream.fd;
      FS.close(stream);
      expectErrno(() => FS.fstat(fd), E.EBADF);
      expectErrno(() => FS.ftruncate(fd, 0), E.EBADF);
      expectErrno(() => FS.fchmod(fd, 0o777), E.EBADF);
    });

    it("double unlink returns ENOENT", () => {
      const { FS, E } = h;
      FS.writeFile("/temp", "data");
      FS.unlink("/temp");
      expectErrno(() => FS.unlink("/temp"), E.ENOENT);
    });

    it("double rmdir returns ENOENT", () => {
      const { FS, E } = h;
      FS.mkdir("/tempdir", 0o777);
      FS.rmdir("/tempdir");
      expectErrno(() => FS.rmdir("/tempdir"), E.ENOENT);
    });

    it("create file then mkdir at same path returns EEXIST", () => {
      const { FS, E } = h;
      FS.writeFile("/name", "data");
      expectErrno(() => FS.mkdir("/name", 0o777), E.EEXIST);
    });

    it("create dir then create file at same path with O_EXCL returns EEXIST", () => {
      const { FS, E } = h;
      FS.mkdir("/name2", 0o777);
      expectErrno(
        () => FS.open("/name2", O.RDWR | O.CREAT | O.EXCL, 0o666),
        E.EEXIST,
      );
    });
  });

  // =================================================================
  // Multi-step error recovery
  // =================================================================

  describe("error recovery", () => {
    it("failed operation does not corrupt existing file", () => {
      const { FS, E } = h;
      const original = FS.readFile("/file", { encoding: "utf8" });

      // Try invalid operations
      expectErrno(
        () => FS.open("/file", O.RDWR | O.CREAT | O.EXCL, 0o666),
        E.EEXIST,
      );
      expectErrno(() => FS.mkdir("/file", 0o777), E.EEXIST);

      // File should be unchanged
      const after = FS.readFile("/file", { encoding: "utf8" });
      expect(after).toBe(original);
    });

    it("failed mkdir does not leave partial state", () => {
      const { FS, E } = h;
      expectErrno(() => FS.mkdir("/file/sub", 0o777), E.ENOTDIR);
      expectErrno(() => FS.stat("/file/sub"), E.ENOTDIR);
    });

    it("failed rename does not move source", () => {
      const { FS, E } = h;
      FS.writeFile("/src", "important");
      expectErrno(() => FS.rename("/src", "/file/dest"), E.ENOTDIR);
      const content = FS.readFile("/src", { encoding: "utf8" });
      expect(content).toBe("important");
    });

    it("failed rename does not destroy target", () => {
      const { FS, E } = h;
      FS.mkdir("/target", 0o777);
      FS.writeFile("/target/keep", "preserved");
      FS.mkdir("/moved", 0o777);
      expectErrno(() => FS.rename("/moved", "/target"), E.ENOTEMPTY);
      const content = FS.readFile("/target/keep", { encoding: "utf8" });
      expect(content).toBe("preserved");
    });
  });
});
