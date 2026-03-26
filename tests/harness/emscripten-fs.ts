/**
 * Test harness that provides access to Emscripten's MEMFS via the FS API.
 *
 * Usage in tests:
 *   const { FS, E, O } = await createFS();
 *   const stream = FS.open('/test', O.RDWR | O.CREAT, 0o777);
 *   FS.write(stream, encode("hello"), 0, 5);
 *   FS.close(stream);
 */

import { fileURLToPath } from "url";
import { dirname, join } from "path";

const __dirname = dirname(fileURLToPath(import.meta.url));

// Types for the Emscripten FS API subset we use in conformance tests.

export interface EmscriptenStream {
  fd: number;
  node: EmscriptenFSNode;
  position: number;
  flags: number;
  stream_ops: Record<string, Function>;
}

export interface EmscriptenFSNode {
  id: number;
  name: string;
  mode: number;
  node_ops: Record<string, Function>;
  stream_ops: Record<string, Function>;
  timestamp: number;
  contents?: Uint8Array | Record<string, EmscriptenFSNode>;
}

export interface EmscriptenFS {
  // File operations (flags are numeric — use O.* constants)
  open(path: string, flags: number | string, mode?: number): EmscriptenStream;
  close(stream: EmscriptenStream): void;
  read(
    stream: EmscriptenStream,
    buffer: ArrayBufferView,
    offset: number,
    length: number,
    position?: number,
  ): number;
  write(
    stream: EmscriptenStream,
    buffer: ArrayBufferView,
    offset: number,
    length: number,
    position?: number,
  ): number;
  llseek(stream: EmscriptenStream, offset: number, whence: number): number;

  // Metadata
  stat(path: string): EmscriptenStat;
  fstat(fd: number): EmscriptenStat;
  lstat(path: string): EmscriptenStat;
  chmod(path: string, mode: number): void;
  fchmod(fd: number, mode: number): void;
  utime(path: string, atime: number, mtime: number): void;
  truncate(path: string, len: number): void;
  ftruncate(fd: number, len: number): void;

  // Directory operations
  mkdir(path: string, mode?: number): void;
  rmdir(path: string): void;
  readdir(path: string): string[];

  // Link operations
  unlink(path: string): void;
  rename(oldPath: string, newPath: string): void;
  symlink(target: string, linkpath: string): void;
  readlink(path: string): string;

  // Utilities
  writeFile(
    path: string,
    data: string | ArrayBufferView,
    opts?: { flags?: string },
  ): void;
  readFile(path: string, opts?: { encoding?: string }): Uint8Array | string;
  isFile(mode: number): boolean;
  isDir(mode: number): boolean;
  isLink(mode: number): boolean;

  // Stream/node helpers
  getStream(fd: number): EmscriptenStream | null;
  dupStream(stream: EmscriptenStream, fd?: number): EmscriptenStream;
  mknod(path: string, mode: number, dev: number): void;
  cwd(): string;
  chdir(path: string): void;

  // ErrnoError constructor (for instanceof checks)
  ErrnoError: new (errno: number) => Error & { errno: number };
}

export interface EmscriptenStat {
  dev: number;
  ino: number;
  mode: number;
  nlink: number;
  uid: number;
  gid: number;
  rdev: number;
  size: number;
  atime: Date;
  mtime: Date;
  ctime: Date;
  blksize: number;
  blocks: number;
}

export interface ErrNoCodes {
  ENOENT: number;
  EEXIST: number;
  ENOTDIR: number;
  EISDIR: number;
  EBADF: number;
  EINVAL: number;
  EACCES: number;
  EPERM: number;
  ENOTEMPTY: number;
  ELOOP: number;
  ENAMETOOLONG: number;
  [key: string]: number;
}

/**
 * Open flag constants (Linux/WASM values from fcntl.h).
 * Use these instead of FS.O_* which don't exist on the JS FS object.
 */
export const O = {
  RDONLY: 0,
  WRONLY: 1,
  RDWR: 2,
  CREAT: 64,
  EXCL: 128,
  TRUNC: 512,
  APPEND: 1024,
  DIRECTORY: 65536,
  NOFOLLOW: 131072,
} as const;

/** lseek whence constants */
export const SEEK_SET = 0;
export const SEEK_CUR = 1;
export const SEEK_END = 2;

/** POSIX mode constants */
export const S_IFMT = 0o170000;
export const S_IFREG = 0o100000;
export const S_IFDIR = 0o040000;
export const S_IFLNK = 0o120000;
export const S_IRWXU = 0o700;
export const S_IRUSR = 0o400;
export const S_IWUSR = 0o200;
export const S_IXUSR = 0o100;
export const S_IRWXG = 0o070;
export const S_IRWXO = 0o007;
export const S_IRWXUGO = 0o777;

export interface FSHarness {
  FS: EmscriptenFS;
  E: ErrNoCodes;
}

/**
 * The mount point where tomefs is mounted when running in tomefs mode.
 * All absolute test paths (e.g. '/test') are transparently rewritten
 * to '/tome/test' by the FS wrapper.
 */
const TOME_MOUNT = "/tome";

/** System paths that should never be rewritten to the tomefs mount. */
function isSystemPath(p: string): boolean {
  return (
    p.startsWith("/dev") ||
    p.startsWith("/proc") ||
    p.startsWith("/tmp")
  );
}

/** Rewrite an absolute path to live under the tomefs mount. */
function rewritePath(p: string): string {
  if (!p.startsWith("/")) return p;
  if (p.startsWith(TOME_MOUNT + "/") || p === TOME_MOUNT) return p;
  if (isSystemPath(p)) return p;
  if (p === "/") return TOME_MOUNT;
  return TOME_MOUNT + p;
}

/** Strip the tomefs mount prefix from a path. */
function unrewritePath(p: string): string {
  if (p.startsWith(TOME_MOUNT + "/")) return p.slice(TOME_MOUNT.length);
  if (p === TOME_MOUNT) return "/";
  return p;
}

/**
 * Create a path-rewriting wrapper around an Emscripten FS.
 * Allows conformance tests to use absolute paths like '/test'
 * while transparently operating under the tomefs mount point.
 */
function createPathRewritingFS(realFS: any): EmscriptenFS {
  return {
    open(path: string, flags: number | string, mode?: number) {
      return realFS.open(rewritePath(path), flags, mode);
    },
    close(stream: EmscriptenStream) {
      return realFS.close(stream);
    },
    read(stream: any, buffer: any, offset: number, length: number, position?: number) {
      return realFS.read(stream, buffer, offset, length, position);
    },
    write(stream: any, buffer: any, offset: number, length: number, position?: number) {
      return realFS.write(stream, buffer, offset, length, position);
    },
    llseek(stream: any, offset: number, whence: number) {
      return realFS.llseek(stream, offset, whence);
    },
    stat(path: string) {
      return realFS.stat(rewritePath(path));
    },
    fstat(fd: number) {
      return realFS.fstat(fd);
    },
    lstat(path: string) {
      const result = realFS.lstat(rewritePath(path));
      if (realFS.isLink(result.mode) && result.size > 0) {
        try {
          const target = realFS.readlink(rewritePath(path));
          result.size = unrewritePath(target).length;
        } catch (_e) {
          // If readlink fails, leave size as-is
        }
      }
      return result;
    },
    chmod(path: string, mode: number) {
      return realFS.chmod(rewritePath(path), mode);
    },
    fchmod(fd: number, mode: number) {
      return realFS.fchmod(fd, mode);
    },
    utime(path: string, atime: number, mtime: number) {
      return realFS.utime(rewritePath(path), atime, mtime);
    },
    truncate(path: string, len: number) {
      return realFS.truncate(rewritePath(path), len);
    },
    ftruncate(fd: number, len: number) {
      return realFS.ftruncate(fd, len);
    },
    mkdir(path: string, mode?: number) {
      return realFS.mkdir(rewritePath(path), mode);
    },
    rmdir(path: string) {
      return realFS.rmdir(rewritePath(path));
    },
    readdir(path: string) {
      return realFS.readdir(rewritePath(path));
    },
    unlink(path: string) {
      return realFS.unlink(rewritePath(path));
    },
    rename(oldPath: string, newPath: string) {
      return realFS.rename(rewritePath(oldPath), rewritePath(newPath));
    },
    symlink(target: string, linkpath: string) {
      const rTarget = target.startsWith("/") ? rewritePath(target) : target;
      return realFS.symlink(rTarget, rewritePath(linkpath));
    },
    readlink(path: string) {
      const target = realFS.readlink(rewritePath(path));
      return unrewritePath(target);
    },
    writeFile(path: string, data: string | ArrayBufferView, opts?: { flags?: string }) {
      return realFS.writeFile(rewritePath(path), data, opts);
    },
    readFile(path: string, opts?: { encoding?: string }) {
      return realFS.readFile(rewritePath(path), opts);
    },
    isFile(mode: number) {
      return realFS.isFile(mode);
    },
    isDir(mode: number) {
      return realFS.isDir(mode);
    },
    isLink(mode: number) {
      return realFS.isLink(mode);
    },
    getStream(fd: number) {
      return realFS.getStream(fd);
    },
    dupStream(stream: any, fd?: number) {
      return realFS.dupStream(stream, fd);
    },
    mknod(path: string, mode: number, dev: number) {
      return realFS.mknod(rewritePath(path), mode, dev);
    },
    cwd() {
      return unrewritePath(realFS.cwd());
    },
    chdir(path: string) {
      return realFS.chdir(rewritePath(path));
    },
    ErrnoError: realFS.ErrnoError,
  } as EmscriptenFS;
}

/**
 * Create a fresh Emscripten module instance with the configured filesystem.
 *
 * By default uses MEMFS. Set TOMEFS_BACKEND=tomefs to test against tomefs.
 * Each call returns an isolated FS — tests don't share state.
 */
export async function createFS(): Promise<FSHarness> {
  const { default: createModule } = await import(
    join(__dirname, "emscripten_fs.mjs")
  );

  const Module = await createModule();
  const rawFS = Module.FS;
  const E = Module.ERRNO_CODES as ErrNoCodes;

  const backendName = process.env.TOMEFS_BACKEND;
  if (backendName === "tomefs") {
    const { createTomeFS } = await import("../../src/tomefs.js");
    const maxPages = process.env.TOMEFS_MAX_PAGES
      ? parseInt(process.env.TOMEFS_MAX_PAGES, 10)
      : undefined;
    const tomefs = createTomeFS(rawFS, maxPages ? { maxPages } : undefined);

    rawFS.mkdir(TOME_MOUNT);
    rawFS.mount(tomefs, {}, TOME_MOUNT);

    return {
      FS: createPathRewritingFS(rawFS),
      E,
    };
  }

  return {
    FS: rawFS as EmscriptenFS,
    E,
  };
}

/** Encode a string to Uint8Array */
export function encode(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

/** Decode a Uint8Array (or subarray) to string */
export function decode(buf: Uint8Array, length?: number): string {
  return new TextDecoder().decode(
    length !== undefined ? buf.subarray(0, length) : buf,
  );
}

/**
 * Assert that an FS operation throws with a specific errno code.
 * Emscripten FS throws ErrnoError objects with an `errno` property
 * whose value comes from ERRNO_CODES (not standard POSIX numbers).
 */
export function expectErrno(
  fn: () => unknown,
  expectedErrno: number,
  message?: string,
): void {
  try {
    fn();
    throw new Error(
      message ||
        `Expected ErrnoError with code ${expectedErrno}, but no error was thrown`,
    );
  } catch (e: unknown) {
    if (e instanceof Error && "errno" in e) {
      const errnoErr = e as Error & { errno: number };
      expect(errnoErr.errno).toBe(expectedErrno);
    } else {
      throw e;
    }
  }
}
