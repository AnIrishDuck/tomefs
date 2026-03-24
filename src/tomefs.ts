/**
 * tomefs — A page-cached Emscripten filesystem.
 *
 * Implements the standard Emscripten custom filesystem interface (mount,
 * createNode, node_ops, stream_ops). File data is stored as PAGE_SIZE-byte
 * pages in a bounded LRU page cache backed by a SyncStorageBackend.
 *
 * This is the core value proposition: bounded memory via LRU eviction,
 * with dirty pages flushed to the backend before eviction. Only the
 * working set lives in memory.
 *
 * Usage:
 *   const backend = new SyncMemoryBackend();
 *   const tomefs = createTomeFS(Module.FS, { backend, maxPages: 4096 });
 *   FS.mkdir('/data');
 *   FS.mount(tomefs, {}, '/data');
 */

import { PAGE_SIZE } from "./types.js";
import { SyncPageCache } from "./sync-page-cache.js";
import { SyncMemoryBackend } from "./sync-memory-backend.js";
import type { SyncStorageBackend } from "./sync-storage-backend.js";

/** Options for creating a tomefs instance. */
export interface TomeFSOptions {
  /** Storage backend. Defaults to SyncMemoryBackend. */
  backend?: SyncStorageBackend;
  /** Maximum pages in the LRU cache. Default: 4096 (32 MB). */
  maxPages?: number;
}

/** Monotonically increasing path counter for unique file identifiers. */
let nextPathId = 0;

/**
 * Compute a unique storage path for a node by walking up to the mount root.
 */
function nodePath(node: any): string {
  const parts: string[] = [];
  let n = node;
  while (n.parent && n.parent !== n) {
    parts.unshift(n.name);
    n = n.parent;
  }
  // Prefix with mount id to avoid collisions across mounts
  return "/" + parts.join("/");
}

/**
 * Create a tomefs filesystem instance bound to an Emscripten FS module.
 *
 * Usage:
 *   const tomefs = createTomeFS(Module.FS);
 *   FS.mkdir('/data');
 *   FS.mount(tomefs, {}, '/data');
 */
export function createTomeFS(FS: any, options?: TomeFSOptions): any {
  const backend = options?.backend ?? new SyncMemoryBackend();
  const maxPages = options?.maxPages ?? 4096;
  const pageCache = new SyncPageCache(backend, maxPages);

  // ---------------------------------------------------------------
  // Page-cached file I/O
  // ---------------------------------------------------------------

  /** Read bytes from a node's file data via the page cache. */
  function readPages(
    node: any,
    buffer: Uint8Array,
    offset: number,
    length: number,
    position: number,
  ): number {
    const size: number = node.usedBytes;
    if (position >= size) return 0;
    const toRead = Math.min(length, size - position);
    if (toRead === 0) return 0;

    return pageCache.read(
      node.storagePath,
      buffer,
      offset,
      toRead,
      position,
      size,
    );
  }

  /** Write bytes to a node's file data via the page cache. */
  function writePages(
    node: any,
    buffer: Uint8Array,
    offset: number,
    length: number,
    position: number,
  ): number {
    if (length === 0) return 0;

    const result = pageCache.write(
      node.storagePath,
      buffer,
      offset,
      length,
      position,
      node.usedBytes,
    );

    node.usedBytes = result.newFileSize;
    return result.bytesWritten;
  }

  /** Resize a file's storage (truncate or extend). */
  function resizeFileStorage(node: any, newSize: number): void {
    if (node.usedBytes === newSize) return;

    const path = node.storagePath;

    if (newSize === 0) {
      pageCache.deleteFile(path);
      node.usedBytes = 0;
      return;
    }

    if (newSize < node.usedBytes) {
      // Shrink: zero the tail of the last surviving page, then invalidate beyond
      const neededPages = Math.ceil(newSize / PAGE_SIZE);
      pageCache.zeroTailAfterTruncate(path, newSize);
      pageCache.invalidatePagesFrom(path, neededPages);
      backend.deletePagesFrom(path, neededPages);
    }
    // Growing: pages are allocated on demand when written/read via getPage

    node.usedBytes = newSize;
  }

  // ---------------------------------------------------------------
  // Node operations
  // ---------------------------------------------------------------

  function getattr(node: any) {
    let size: number;
    if (FS.isFile(node.mode)) {
      size = node.usedBytes;
    } else if (FS.isDir(node.mode)) {
      size = 4096;
    } else if (FS.isLink(node.mode)) {
      size = node.link.length;
    } else {
      size = 0;
    }
    return {
      dev: 1,
      ino: node.id,
      mode: node.mode,
      nlink: 1,
      uid: 0,
      gid: 0,
      rdev: node.rdev,
      size,
      atime: new Date(node.atime),
      mtime: new Date(node.mtime),
      ctime: new Date(node.ctime),
      blksize: 4096,
      blocks: Math.ceil(size / 4096),
    };
  }

  function setattr(node: any, attr: any) {
    for (const key of ["mode", "atime", "mtime", "ctime"] as const) {
      if (attr[key] != null) {
        node[key] = attr[key];
      }
    }
    if (attr.size !== undefined) {
      resizeFileStorage(node, attr.size);
    }
  }

  function lookup(parent: any, name: string) {
    if (!parent.contents || !(name in parent.contents)) {
      throw new FS.ErrnoError(44); // ENOENT
    }
    return parent.contents[name];
  }

  function mknod(parent: any, name: string, mode: number, dev: number) {
    return TOMEFS.createNode(parent, name, mode, dev);
  }

  function rename(old_node: any, new_dir: any, new_name: string) {
    let new_node: any;
    try {
      new_node = FS.lookupNode(new_dir, new_name);
    } catch (_e) {
      // Target doesn't exist — that's fine
    }

    if (new_node) {
      if (FS.isDir(old_node.mode)) {
        for (const _i in new_node.contents) {
          throw new FS.ErrnoError(55); // ENOTEMPTY
        }
      }
      // Clean up target's storage if it's a file
      if (FS.isFile(new_node.mode)) {
        pageCache.deleteFile(new_node.storagePath);
      }
      FS.hashRemoveNode(new_node);
    }

    // Update storage path for file nodes
    if (FS.isFile(old_node.mode)) {
      const oldStoragePath = old_node.storagePath;
      // Compute new storage path
      const newStoragePath = computeStoragePath(new_dir, new_name);
      pageCache.renameFile(oldStoragePath, newStoragePath);
      old_node.storagePath = newStoragePath;
    }

    // Rewire the node tree
    delete old_node.parent.contents[old_node.name];
    new_dir.contents[new_name] = old_node;
    old_node.name = new_name;
    const now = Date.now();
    new_dir.ctime = new_dir.mtime = old_node.parent.ctime =
      old_node.parent.mtime = now;
  }

  function unlink(parent: any, name: string) {
    const node = parent.contents[name];
    if (node && FS.isFile(node.mode)) {
      node.unlinked = true;
      // Defer page deletion: if open fds exist, data must remain readable.
      // Pages are cleaned up when the last fd is closed (see stream_ops.close).
      if (node.openCount === 0) {
        pageCache.deleteFile(node.storagePath);
      }
    }
    delete parent.contents[name];
    parent.ctime = parent.mtime = Date.now();
  }

  function rmdir(parent: any, name: string) {
    const node = FS.lookupNode(parent, name);
    for (const _i in node.contents) {
      throw new FS.ErrnoError(55); // ENOTEMPTY
    }
    delete parent.contents[name];
    parent.ctime = parent.mtime = Date.now();
  }

  /** Compute a storage path for a node given its parent and name. */
  function computeStoragePath(parent: any, name: string): string {
    const parts: string[] = [name];
    let n = parent;
    while (n.parent && n.parent !== n) {
      parts.unshift(n.name);
      n = n.parent;
    }
    return "/" + parts.join("/");
  }

  /** Directory node_ops. */
  const dir_node_ops = {
    getattr,
    setattr,
    lookup,
    mknod,
    rename,
    unlink,
    rmdir,
    readdir(node: any) {
      return [".", "..", ...Object.keys(node.contents)];
    },
    symlink(parent: any, newname: string, oldpath: string) {
      const node = TOMEFS.createNode(parent, newname, 0o777 | 0o120000, 0);
      node.link = oldpath;
      return node;
    },
  };

  /** File node_ops. */
  const file_node_ops = {
    getattr,
    setattr,
  };

  /** Symlink node_ops. */
  const link_node_ops = {
    getattr,
    setattr,
    readlink(node: any) {
      if (!FS.isLink(node.mode)) {
        throw new FS.ErrnoError(28); // EINVAL
      }
      return node.link;
    },
  };

  // ---------------------------------------------------------------
  // Stream operations
  // ---------------------------------------------------------------

  const stream_ops = {
    read(
      stream: any,
      buffer: Uint8Array,
      offset: number,
      length: number,
      position: number,
    ): number {
      return readPages(stream.node, buffer, offset, length, position);
    },

    write(
      stream: any,
      buffer: Uint8Array,
      offset: number,
      length: number,
      position: number,
      _canOwn?: boolean,
    ): number {
      if (!length) return 0;
      const node = stream.node;
      node.mtime = node.ctime = Date.now();
      return writePages(node, buffer, offset, length, position);
    },

    llseek(stream: any, offset: number, whence: number): number {
      let position = offset;
      if (whence === 1) {
        // SEEK_CUR
        position += stream.position;
      } else if (whence === 2) {
        // SEEK_END
        if (FS.isFile(stream.node.mode)) {
          position += stream.node.usedBytes;
        }
      }
      if (position < 0) {
        throw new FS.ErrnoError(28); // EINVAL
      }
      return position;
    },

    open(stream: any) {
      if (FS.isFile(stream.node.mode)) {
        stream.node.openCount++;
      }
    },

    close(stream: any) {
      const node = stream.node;
      if (FS.isFile(node.mode)) {
        node.openCount--;
        if (node.unlinked && node.openCount === 0) {
          // Last fd closed on an unlinked file — clean up pages
          pageCache.deleteFile(node.storagePath);
        } else {
          pageCache.flushFile(node.storagePath);
        }
      }
    },

    allocate(stream: any, offset: number, length: number) {
      resizeFileStorage(
        stream.node,
        Math.max(stream.node.usedBytes, offset + length),
      );
    },

    mmap(
      stream: any,
      length: number,
      position: number,
      _prot: number,
      _flags: number,
    ) {
      if (!FS.isFile(stream.node.mode)) {
        throw new FS.ErrnoError(43); // ENODEV
      }
      const buf = new Uint8Array(length);
      readPages(stream.node, buf, 0, length, position);
      return { ptr: buf, allocated: true };
    },

    msync(
      stream: any,
      buffer: Uint8Array,
      offset: number,
      length: number,
      _mmapFlags: number,
    ) {
      writePages(stream.node, buffer, offset, length, 0);
      return 0;
    },
  };

  const dir_stream_ops = {
    llseek: stream_ops.llseek,
  };

  // ---------------------------------------------------------------
  // Filesystem object
  // ---------------------------------------------------------------

  const TOMEFS = {
    /** Expose the page cache for testing and diagnostics. */
    pageCache,

    mount(_mount: any) {
      return TOMEFS.createNode(null, "/", 0o40000 | 0o777, 0);
    },

    createNode(parent: any, name: string, mode: number, dev: number) {
      if (FS.isBlkdev(mode) || FS.isFIFO(mode)) {
        throw new FS.ErrnoError(63); // EPERM
      }

      const node = FS.createNode(parent, name, mode, dev);

      if (FS.isDir(node.mode)) {
        node.node_ops = dir_node_ops;
        node.stream_ops = dir_stream_ops;
        node.contents = {};
      } else if (FS.isFile(node.mode)) {
        node.node_ops = file_node_ops;
        node.stream_ops = stream_ops;
        node.usedBytes = 0;
        node.openCount = 0;
        node.unlinked = false;
        // Assign a unique storage path for page cache keying
        node.storagePath = parent
          ? computeStoragePath(parent, name)
          : `/__root_${nextPathId++}`;
      } else if (FS.isLink(node.mode)) {
        node.node_ops = link_node_ops;
        node.stream_ops = {};
      }

      node.atime = node.mtime = node.ctime = Date.now();

      if (parent) {
        parent.contents[name] = node;
        parent.atime = parent.mtime = parent.ctime = node.atime;
      }

      return node;
    },
  };

  return TOMEFS;
}
