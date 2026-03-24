/**
 * Workload scenario test harness.
 *
 * Provides helpers to mount tomefs with configurable cache sizes,
 * run the same scenario against both MEMFS and tomefs for differential
 * testing, and generate deterministic test data.
 */

import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { createTomeFS } from "../../src/tomefs.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { PAGE_SIZE } from "../../src/types.js";
import type { EmscriptenFS, EmscriptenStream, FSHarness } from "../harness/emscripten-fs.js";
import { O, SEEK_SET, SEEK_END } from "../harness/emscripten-fs.js";

export { O, SEEK_SET, SEEK_END, PAGE_SIZE };
export type { EmscriptenFS, EmscriptenStream };

const __dirname = dirname(fileURLToPath(import.meta.url));

/** Cache size configurations for the pressure matrix. */
export const CACHE_CONFIGS = {
  tiny: 4,      // 32 KB — maximum eviction pressure
  small: 16,    // 128 KB — moderate eviction
  medium: 64,   // 512 KB — working set partially fits
  large: 4096,  // 32 MB — working set fits, baseline
} as const;

export type CacheSize = keyof typeof CACHE_CONFIGS;

/** The mount point for tomefs inside the Emscripten FS. */
const TOME_MOUNT = "/tome";

export interface WorkloadHarness {
  FS: EmscriptenFS;
  /** Cache size in pages (only meaningful for tomefs). */
  cachePages: number;
}

/**
 * Create a fresh tomefs-backed Emscripten FS with a specific cache size.
 */
export async function createTomeFSHarness(
  cacheSize: CacheSize | number,
): Promise<WorkloadHarness> {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  const Module = await createModule();
  const rawFS = Module.FS;

  const maxPages =
    typeof cacheSize === "number" ? cacheSize : CACHE_CONFIGS[cacheSize];

  const backend = new SyncMemoryBackend();
  const tomefs = createTomeFS(rawFS, { backend, maxPages });

  rawFS.mkdir(TOME_MOUNT);
  rawFS.mount(tomefs, {}, TOME_MOUNT);

  // Return a path-rewriting wrapper so tests use absolute paths like '/test'
  const FS = createPathRewritingFS(rawFS);

  return { FS, cachePages: maxPages };
}

/**
 * Create a MEMFS-backed harness (for differential comparison).
 */
export async function createMemFSHarness(): Promise<WorkloadHarness> {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  const Module = await createModule();
  return { FS: Module.FS as EmscriptenFS, cachePages: Infinity };
}

/** Encode string to Uint8Array. */
export function encode(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

/** Decode Uint8Array to string. */
export function decode(buf: Uint8Array, length?: number): string {
  return new TextDecoder().decode(
    length !== undefined ? buf.subarray(0, length) : buf,
  );
}

/**
 * Generate deterministic data for a given page index.
 * Each page is filled with a repeating pattern based on the index,
 * making verification straightforward.
 */
export function generatePageData(pageIndex: number, size = PAGE_SIZE): Uint8Array {
  const data = new Uint8Array(size);
  for (let i = 0; i < size; i++) {
    // Mix page index and byte offset for a unique-per-page pattern
    data[i] = ((pageIndex * 251 + i * 31 + 17) & 0xff);
  }
  return data;
}

/**
 * Write `totalBytes` of deterministic data to a file.
 * Uses PAGE_SIZE-aligned writes for efficiency.
 */
export function writeFileData(
  FS: EmscriptenFS,
  path: string,
  totalBytes: number,
): void {
  const stream = FS.open(path, O.WRONLY | O.CREAT | O.TRUNC, 0o666);
  let written = 0;
  let pageIdx = 0;
  while (written < totalBytes) {
    const chunkSize = Math.min(PAGE_SIZE, totalBytes - written);
    const data = generatePageData(pageIdx, chunkSize);
    FS.write(stream, data, 0, chunkSize);
    written += chunkSize;
    pageIdx++;
  }
  FS.close(stream);
}

/**
 * Verify that a file contains the expected deterministic data.
 * Returns true if all data matches.
 */
export function verifyFileData(
  FS: EmscriptenFS,
  path: string,
  totalBytes: number,
): void {
  const stream = FS.open(path, O.RDONLY);
  const buf = new Uint8Array(PAGE_SIZE);
  let position = 0;
  let pageIdx = 0;
  while (position < totalBytes) {
    const chunkSize = Math.min(PAGE_SIZE, totalBytes - position);
    const expected = generatePageData(pageIdx, chunkSize);
    const n = FS.read(stream, buf, 0, chunkSize);
    expect(n).toBe(chunkSize);
    for (let i = 0; i < chunkSize; i++) {
      if (buf[i] !== expected[i]) {
        FS.close(stream);
        throw new Error(
          `Data mismatch at byte ${position + i} (page ${pageIdx}, offset ${i}): ` +
          `expected ${expected[i]}, got ${buf[i]}`,
        );
      }
    }
    position += chunkSize;
    pageIdx++;
  }
  FS.close(stream);
}

// --- Path rewriting (same approach as conformance harness) ---

function isSystemPath(p: string): boolean {
  return p.startsWith("/dev") || p.startsWith("/proc") || p.startsWith("/tmp");
}

function rewritePath(p: string): string {
  if (!p.startsWith("/")) return p;
  if (p.startsWith(TOME_MOUNT + "/") || p === TOME_MOUNT) return p;
  if (isSystemPath(p)) return p;
  if (p === "/") return TOME_MOUNT;
  return TOME_MOUNT + p;
}

function unrewritePath(p: string): string {
  if (p.startsWith(TOME_MOUNT + "/")) return p.slice(TOME_MOUNT.length);
  if (p === TOME_MOUNT) return "/";
  return p;
}

function createPathRewritingFS(realFS: any): EmscriptenFS {
  return {
    open(path: string, flags: number | string, mode?: number) {
      return realFS.open(rewritePath(path), flags, mode);
    },
    close(stream: EmscriptenStream) { return realFS.close(stream); },
    read(stream: any, buffer: any, offset: number, length: number, position?: number) {
      return realFS.read(stream, buffer, offset, length, position);
    },
    write(stream: any, buffer: any, offset: number, length: number, position?: number) {
      return realFS.write(stream, buffer, offset, length, position);
    },
    llseek(stream: any, offset: number, whence: number) {
      return realFS.llseek(stream, offset, whence);
    },
    stat(path: string) { return realFS.stat(rewritePath(path)); },
    fstat(fd: number) { return realFS.fstat(fd); },
    lstat(path: string) { return realFS.lstat(rewritePath(path)); },
    chmod(path: string, mode: number) { return realFS.chmod(rewritePath(path), mode); },
    fchmod(fd: number, mode: number) { return realFS.fchmod(fd, mode); },
    utime(path: string, atime: number, mtime: number) {
      return realFS.utime(rewritePath(path), atime, mtime);
    },
    truncate(path: string, len: number) { return realFS.truncate(rewritePath(path), len); },
    ftruncate(fd: number, len: number) { return realFS.ftruncate(fd, len); },
    mkdir(path: string, mode?: number) { return realFS.mkdir(rewritePath(path), mode); },
    rmdir(path: string) { return realFS.rmdir(rewritePath(path)); },
    readdir(path: string) { return realFS.readdir(rewritePath(path)); },
    unlink(path: string) { return realFS.unlink(rewritePath(path)); },
    rename(oldPath: string, newPath: string) {
      return realFS.rename(rewritePath(oldPath), rewritePath(newPath));
    },
    symlink(target: string, linkpath: string) {
      const rTarget = target.startsWith("/") ? rewritePath(target) : target;
      return realFS.symlink(rTarget, rewritePath(linkpath));
    },
    readlink(path: string) { return unrewritePath(realFS.readlink(rewritePath(path))); },
    writeFile(path: string, data: string | ArrayBufferView, opts?: { flags?: string }) {
      return realFS.writeFile(rewritePath(path), data, opts);
    },
    readFile(path: string, opts?: { encoding?: string }) {
      return realFS.readFile(rewritePath(path), opts);
    },
    isFile(mode: number) { return realFS.isFile(mode); },
    isDir(mode: number) { return realFS.isDir(mode); },
    isLink(mode: number) { return realFS.isLink(mode); },
    getStream(fd: number) { return realFS.getStream(fd); },
    dupStream(stream: any, fd?: number) { return realFS.dupStream(stream, fd); },
    mknod(path: string, mode: number, dev: number) {
      return realFS.mknod(rewritePath(path), mode, dev);
    },
    cwd() { return unrewritePath(realFS.cwd()); },
    chdir(path: string) { return realFS.chdir(rewritePath(path)); },
    ErrnoError: realFS.ErrnoError,
  } as EmscriptenFS;
}
