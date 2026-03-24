/**
 * BadFS: Wrapper FSes that inject specific defects into a real MEMFS.
 *
 * Each defect creates a proxy around the real FS that subtly breaks one
 * specific behavior. The conformance test suite should catch each defect —
 * if it doesn't, the test lacks discriminating power for that class of bug.
 *
 * See plans/conformance-test-plan.md for the full defect inventory.
 */

import {
  createFS,
  type EmscriptenFS,
  type EmscriptenStream,
  type EmscriptenStat,
  type FSHarness,
} from "./emscripten-fs.js";

export type DefectId =
  | "off-by-one-read"
  | "no-mtime-update"
  | "rename-no-overwrite"
  | "readdir-missing-dot"
  | "symlink-no-resolve"
  | "wrong-errno-enoent"
  | "seek-end-off-by-one";

/**
 * Create an FS harness with a specific defect injected.
 * The returned FS behaves like MEMFS except for the injected bug.
 */
export async function createBadFS(defect: DefectId): Promise<FSHarness> {
  const harness = await createFS();
  const wrappedFS = applyDefect(harness.FS, harness, defect);
  return { FS: wrappedFS, E: harness.E };
}

function applyDefect(
  fs: EmscriptenFS,
  harness: FSHarness,
  defect: DefectId,
): EmscriptenFS {
  switch (defect) {
    case "off-by-one-read":
      return offByOneRead(fs);
    case "no-mtime-update":
      return noMtimeUpdate(fs);
    case "rename-no-overwrite":
      return renameNoOverwrite(fs, harness);
    case "readdir-missing-dot":
      return readdirMissingDot(fs);
    case "symlink-no-resolve":
      return symlinkNoResolve(fs, harness);
    case "wrong-errno-enoent":
      return wrongErrnoEnoent(fs, harness);
    case "seek-end-off-by-one":
      return seekEndOffByOne(fs);
  }
}

/**
 * off-by-one-read: Returns length - 1 bytes on read (when reading > 0 bytes).
 * Should be caught by Batch 1 (Core I/O) tests.
 */
function offByOneRead(fs: EmscriptenFS): EmscriptenFS {
  const origRead = fs.read.bind(fs);
  return new Proxy(fs, {
    get(target, prop) {
      if (prop === "read") {
        return (
          stream: EmscriptenStream,
          buffer: ArrayBufferView,
          offset: number,
          length: number,
          position?: number,
        ) => {
          const n = origRead(stream, buffer, offset, length, position);
          // Return one fewer byte than actually read (when > 0)
          return n > 0 ? n - 1 : n;
        };
      }
      return Reflect.get(target, prop);
    },
  });
}

/**
 * no-mtime-update: Freezes mtime so that writes don't appear to update it.
 * Records each file's mtime at creation/open time, and returns that frozen
 * value from stat/fstat instead of the real (updated) one.
 * Should be caught by Batch 2 (Metadata) tests.
 */
function noMtimeUpdate(fs: EmscriptenFS): EmscriptenFS {
  // Track frozen mtimes per inode (id)
  const frozenMtime = new Map<number, Date>();

  return new Proxy(fs, {
    get(target, prop) {
      if (prop === "open") {
        return (path: string, flags: number | string, mode?: number) => {
          const stream = fs.open(path, flags, mode);
          // Freeze the mtime at open time if we haven't already
          if (!frozenMtime.has(stream.node.id)) {
            const stat = fs.fstat(stream.fd);
            frozenMtime.set(stream.node.id, new Date(stat.mtime.getTime()));
          }
          return stream;
        };
      }
      if (prop === "stat") {
        return (path: string) => {
          const stat = fs.stat(path);
          const frozen = frozenMtime.get(stat.ino);
          if (frozen) {
            return { ...stat, mtime: frozen };
          }
          return stat;
        };
      }
      if (prop === "fstat") {
        return (fd: number) => {
          const stat = fs.fstat(fd);
          const frozen = frozenMtime.get(stat.ino);
          if (frozen) {
            return { ...stat, mtime: frozen };
          }
          return stat;
        };
      }
      return Reflect.get(target, prop);
    },
  });
}

/**
 * rename-no-overwrite: When renaming to an existing target, silently keeps
 * the old target instead of overwriting it.
 * Should be caught by Batch 3 (Rename) tests.
 */
function renameNoOverwrite(fs: EmscriptenFS, harness: FSHarness): EmscriptenFS {
  return new Proxy(fs, {
    get(target, prop) {
      if (prop === "rename") {
        return (oldPath: string, newPath: string) => {
          // Check if target exists
          let targetExists = false;
          try {
            fs.stat(newPath);
            targetExists = true;
          } catch {
            // doesn't exist
          }

          if (targetExists) {
            // Silently remove oldPath but DON'T overwrite newPath
            // This simulates a rename that "forgets" to replace the target
            try {
              const stat = fs.lstat(oldPath);
              if (fs.isDir(stat.mode)) {
                // For directories, just don't do anything when target exists
                // (this is subtly wrong — real rename should replace empty target dir)
                return;
              }
              fs.unlink(oldPath);
            } catch {
              // pass through errors
              fs.rename(oldPath, newPath);
            }
            return;
          }
          // No target — normal rename
          fs.rename(oldPath, newPath);
        };
      }
      return Reflect.get(target, prop);
    },
  });
}

/**
 * readdir-missing-dot: readdir omits "." and ".." entries.
 * Should be caught by Batch 2 (Metadata & Directories) tests.
 */
function readdirMissingDot(fs: EmscriptenFS): EmscriptenFS {
  return new Proxy(fs, {
    get(target, prop) {
      if (prop === "readdir") {
        return (path: string) => {
          const entries = fs.readdir(path);
          return entries.filter((e: string) => e !== "." && e !== "..");
        };
      }
      return Reflect.get(target, prop);
    },
  });
}

/**
 * symlink-no-resolve: symlink creates a link but readlink returns a wrong
 * (nonexistent) target, so following the symlink always fails.
 * This simulates a FS that creates symlinks but doesn't resolve them properly.
 * Should be caught by Batch 4 (Links) tests.
 */
function symlinkNoResolve(fs: EmscriptenFS, harness: FSHarness): EmscriptenFS {
  // Track which paths are symlinks we created
  const symlinkTargets = new Map<string, string>();

  return new Proxy(fs, {
    get(target, prop) {
      if (prop === "symlink") {
        return (linkTarget: string, linkpath: string) => {
          // Store the real target for readlink
          symlinkTargets.set(linkpath, linkTarget);
          // Create symlink pointing to a nonexistent path
          fs.symlink(linkTarget + ".__broken__", linkpath);
        };
      }
      if (prop === "readlink") {
        return (path: string) => {
          // Return the "real" target so readlink looks correct,
          // but the actual symlink on disk points somewhere broken
          const realTarget = symlinkTargets.get(path);
          if (realTarget !== undefined) {
            return realTarget;
          }
          return fs.readlink(path);
        };
      }
      return Reflect.get(target, prop);
    },
  });
}

/**
 * wrong-errno-enoent: Returns EACCES instead of ENOENT when a path doesn't exist.
 * Should be caught by Batch 3 (Rename & Unlink) tests — and potentially others.
 */
function wrongErrnoEnoent(fs: EmscriptenFS, harness: FSHarness): EmscriptenFS {
  return new Proxy(fs, {
    get(target, prop) {
      // Wrap all methods that can throw ErrnoError
      const val = Reflect.get(target, prop);
      if (typeof val === "function") {
        return (...args: unknown[]) => {
          try {
            return val.apply(target, args);
          } catch (e: unknown) {
            if (
              e &&
              typeof e === "object" &&
              "errno" in e &&
              (e as { errno: number }).errno === harness.E.ENOENT
            ) {
              // Replace ENOENT with EACCES
              (e as { errno: number }).errno = harness.E.EACCES;
            }
            throw e;
          }
        };
      }
      return val;
    },
  });
}

/**
 * seek-end-off-by-one: SEEK_END is off by one byte (returns size instead of size-1
 * as the last byte position, effectively adding an extra byte).
 * Should be caught by Batch 1 (Core I/O) tests.
 */
function seekEndOffByOne(fs: EmscriptenFS): EmscriptenFS {
  const SEEK_END = 2;
  return new Proxy(fs, {
    get(target, prop) {
      if (prop === "llseek") {
        return (stream: EmscriptenStream, offset: number, whence: number) => {
          if (whence === SEEK_END) {
            // Off by one: add 1 to the offset when seeking from end
            return fs.llseek(stream, offset + 1, whence);
          }
          return fs.llseek(stream, offset, whence);
        };
      }
      return Reflect.get(target, prop);
    },
  });
}
