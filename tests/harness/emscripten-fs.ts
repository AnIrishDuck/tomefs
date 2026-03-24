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
 * Create a fresh Emscripten module instance with MEMFS mounted.
 * Each call returns an isolated FS — tests don't share state.
 */
export async function createFS(): Promise<FSHarness> {
  const { default: createModule } = await import(
    join(__dirname, "emscripten_fs.mjs")
  );

  const Module = await createModule();

  return {
    FS: Module.FS as EmscriptenFS,
    E: Module.ERRNO_CODES as ErrNoCodes,
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
