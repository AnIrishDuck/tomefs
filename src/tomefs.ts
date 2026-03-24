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
import type { FileMeta } from "./types.js";
import { SyncPageCache } from "./sync-page-cache.js";
import { SyncMemoryBackend } from "./sync-memory-backend.js";
import type { SyncStorageBackend } from "./sync-storage-backend.js";

/** Mode bit constants for type checking when FS object isn't available. */
const S_IFMT = 0o170000;
const S_IFDIR = 0o040000;
const S_IFREG = 0o100000;
const S_IFLNK = 0o120000;

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
        backend.deleteMeta(new_node.storagePath);
      }
      FS.hashRemoveNode(new_node);
    }

    // Compute old storage path before rewiring
    const oldStoragePath = FS.isFile(old_node.mode)
      ? old_node.storagePath
      : computeStoragePath(old_node.parent, old_node.name);

    // Update storage path for file nodes
    if (FS.isFile(old_node.mode)) {
      const newStoragePath = computeStoragePath(new_dir, new_name);
      pageCache.renameFile(oldStoragePath, newStoragePath);
      old_node.storagePath = newStoragePath;
      // Update metadata key
      backend.deleteMeta(oldStoragePath);
    } else if (FS.isDir(old_node.mode)) {
      // Delete old directory metadata
      backend.deleteMeta(oldStoragePath);
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
        backend.deleteMeta(node.storagePath);
      }
    } else if (node && FS.isLink(node.mode)) {
      const sp = computeStoragePath(parent, name);
      backend.deleteMeta(sp);
    }
    delete parent.contents[name];
    parent.ctime = parent.mtime = Date.now();
  }

  function rmdir(parent: any, name: string) {
    const node = FS.lookupNode(parent, name);
    for (const _i in node.contents) {
      throw new FS.ErrnoError(55); // ENOTEMPTY
    }
    const sp = computeStoragePath(parent, name);
    backend.deleteMeta(sp);
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
          // Last fd closed on an unlinked file — clean up pages + metadata
          pageCache.deleteFile(node.storagePath);
          backend.deleteMeta(node.storagePath);
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

  // ---------------------------------------------------------------
  // Persistence: save/restore directory tree to/from backend metadata
  // ---------------------------------------------------------------

  /**
   * Walk the in-memory node tree and persist all file/directory/symlink
   * metadata to the storage backend. File page data is flushed via the
   * page cache. Call this before unmount to ensure durability.
   */
  function persistTree(node: any, path: string): void {
    if (FS.isFile(node.mode)) {
      pageCache.flushFile(node.storagePath);
      backend.writeMeta(path, {
        size: node.usedBytes,
        mode: node.mode,
        ctime: node.ctime,
        mtime: node.mtime,
        atime: node.atime,
      });
    } else if (FS.isLink(node.mode)) {
      backend.writeMeta(path, {
        size: 0,
        mode: node.mode,
        ctime: node.ctime,
        mtime: node.mtime,
        atime: node.atime,
        link: node.link,
      });
    } else if (FS.isDir(node.mode)) {
      // Persist directory metadata (skip root — it's recreated on mount)
      if (path !== "/") {
        backend.writeMeta(path, {
          size: 0,
          mode: node.mode,
          ctime: node.ctime,
          mtime: node.mtime,
          atime: node.atime,
        });
      }
      // Recurse into children
      for (const name of Object.keys(node.contents)) {
        const childPath = path === "/" ? `/${name}` : `${path}/${name}`;
        persistTree(node.contents[name], childPath);
      }
    }
  }

  /**
   * Restore the directory tree from backend metadata.
   * Creates directories, files, and symlinks from stored metadata.
   * File page data remains in the backend and is loaded on demand.
   */
  function restoreTree(root: any): void {
    const paths = backend.listFiles();
    if (paths.length === 0) return;

    // Sort paths by depth so parents are created before children
    paths.sort((a, b) => {
      const da = a.split("/").length;
      const db = b.split("/").length;
      return da - db || a.localeCompare(b);
    });

    for (const path of paths) {
      const meta = backend.readMeta(path);
      if (!meta) continue;

      // Split path into parent + name
      const lastSlash = path.lastIndexOf("/");
      const parentPath = path.substring(0, lastSlash) || "/";
      const name = path.substring(lastSlash + 1);
      if (!name) continue; // skip root

      // Find parent node by walking from root
      const parent = lookupByPath(root, parentPath);
      if (!parent) continue;

      const typeMode = meta.mode & S_IFMT;
      if (typeMode === S_IFDIR) {
        const node = TOMEFS.createNode(parent, name, meta.mode, 0);
        node.atime = meta.atime ?? meta.mtime;
        node.mtime = meta.mtime;
        node.ctime = meta.ctime;
      } else if (typeMode === S_IFREG) {
        const node = TOMEFS.createNode(parent, name, meta.mode, 0);
        node.usedBytes = meta.size;
        node.atime = meta.atime ?? meta.mtime;
        node.mtime = meta.mtime;
        node.ctime = meta.ctime;
      } else if (typeMode === S_IFLNK) {
        const node = TOMEFS.createNode(parent, name, meta.mode, 0);
        node.link = meta.link ?? "";
        node.atime = meta.atime ?? meta.mtime;
        node.mtime = meta.mtime;
        node.ctime = meta.ctime;
      }
    }
  }

  /** Walk from root to find a node at the given internal path. */
  function lookupByPath(root: any, path: string): any {
    if (path === "/" || path === "") return root;
    const parts = path.split("/").filter(Boolean);
    let node = root;
    for (const part of parts) {
      if (!node.contents || !(part in node.contents)) return null;
      node = node.contents[part];
    }
    return node;
  }

  const TOMEFS = {
    /** Expose the page cache for testing and diagnostics. */
    pageCache,

    /** Expose the backend for testing. */
    backend,

    mount(_mount: any) {
      const root = TOMEFS.createNode(null, "/", 0o40000 | 0o777, 0);
      // Restore directory tree from backend if data exists
      restoreTree(root);
      return root;
    },

    /**
     * Emscripten syncfs hook. Called by FS.syncfs().
     * When populate=false: flush all dirty data + persist metadata.
     * When populate=true: no-op (tree is restored on mount).
     */
    syncfs(mount: any, populate: boolean, callback: (err?: Error | null) => void) {
      try {
        if (!populate) {
          pageCache.flushAll();
          // Clear existing metadata and re-persist the full tree
          for (const path of backend.listFiles()) {
            backend.deleteMeta(path);
          }
          persistTree(mount.root, "/");
        }
        callback(null);
      } catch (err) {
        callback(err as Error);
      }
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
