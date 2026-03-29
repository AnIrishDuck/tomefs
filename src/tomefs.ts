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

/** POSIX NAME_MAX: maximum bytes in a single path component. */
const NAME_MAX = 255;

/** Emscripten errno for ENAMETOOLONG. */
const ENAMETOOLONG = 37;

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

  /**
   * Registry of all live tomefs file nodes.
   *
   * When PGlite's MemoryFS preloads a database image, Emscripten may
   * reinitialize its nameTable (hash table) during module startup. This
   * causes nodes created before the reinitialization to become invisible
   * to hash-based lookups, even though they remain reachable through
   * parent.contents chains. Additionally, path resolution can route
   * through MEMFS parent nodes instead of the tomefs mount tree, creating
   * "detached" file nodes that have tomefs stream_ops but don't appear
   * in mount.root's subtree.
   *
   * This registry ensures syncfs can find ALL tomefs file nodes regardless
   * of their position in the Emscripten node graph.
   */
  const allFileNodes = new Set<any>();

  // True when the backend may contain stale metadata not in the live tree.
  // Set on mount (crash recovery may have left orphans) and cleared after
  // a successful orphan cleanup pass in syncfs. Operations that directly
  // modify backend metadata (rename, unlink-with-open-fds) re-set this
  // flag since a crash between their backend writes could leave orphans.
  let needsOrphanCleanup = true;

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
    } else {
      // Growing: materialize only the LAST new page so it's flushed during
      // syncfs. restoreTree distinguishes sparse files from crash-truncated
      // files by probing the last expected page — if it exists in the
      // backend, restoreTree trusts metadata.size. Intermediate pages don't
      // need materializing: they read as zeros on demand (correct for
      // allocate/extend semantics) and are never written to the backend
      // unless the caller actually writes data to them.
      //
      // This reduces allocate cost from O(pages) to O(1) in cache
      // operations and avoids cache thrashing when the allocation exceeds
      // cache capacity (e.g., Postgres pre-allocating a 256 MB WAL segment
      // with a 32 MB page cache).
      const firstNewPage = node.usedBytes > 0 ? Math.ceil(node.usedBytes / PAGE_SIZE) : 0;
      const lastPageIdx = Math.ceil(newSize / PAGE_SIZE) - 1;
      if (lastPageIdx >= firstNewPage) {
        pageCache.markPageDirty(path, lastPageIdx);
      }
    }

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
    if (new_name.length > NAME_MAX) {
      throw new FS.ErrnoError(ENAMETOOLONG);
    }
    // Rename writes metadata directly to the backend (for both source
    // and target paths). A crash between these writes could leave
    // orphaned metadata, so flag that orphan cleanup is needed.
    needsOrphanCleanup = true;

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
        const targetStoragePath = new_node.storagePath;
        if (new_node.openCount > 0) {
          // Target has open fds — preserve its pages under a unique path
          // so open fds can still read the old data (POSIX unlink semantics).
          // The source is about to take over this storagePath.
          const tempPath = `/__deleted_${nextPathId++}`;
          // Write marker metadata BEFORE renaming pages. This ensures
          // crash safety: if the process dies after page rename but before
          // marker write, orphan cleanup can still discover the pages via
          // listFiles() (which returns paths with metadata). Without this
          // ordering, pages at tempPath would be permanently leaked.
          backend.writeMeta(tempPath, {
            size: new_node.usedBytes,
            mode: new_node.mode,
            ctime: new_node.ctime,
            mtime: new_node.mtime,
            atime: new_node.atime,
          });
          pageCache.renameFile(targetStoragePath, tempPath);
          new_node.storagePath = tempPath;
          new_node.unlinked = true;
        } else {
          pageCache.deleteFile(targetStoragePath);
        }
        backend.deleteMeta(targetStoragePath);
        // Keep node tracked if it has open fds — syncfs needs to
        // preserve its /__deleted_* path until the last fd closes.
        if (new_node.openCount === 0) {
          allFileNodes.delete(new_node);
        }
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
      // Move metadata to new path. Construct from node state (no backend
      // read needed) — the node tree is the source of truth. This also
      // provides crash safety for files not yet synced (metadata gets
      // written at the new path immediately).
      backend.writeMeta(newStoragePath, {
        size: old_node.usedBytes,
        mode: old_node.mode,
        ctime: old_node.ctime,
        mtime: old_node.mtime,
        atime: old_node.atime,
      });
      backend.deleteMeta(oldStoragePath);
    } else if (FS.isDir(old_node.mode)) {
      // Move directory metadata to the new path before recursing into
      // children. Construct from node state (no backend read needed) —
      // the node tree is the source of truth for current metadata.
      const newDirPath = computeStoragePath(new_dir, new_name);
      backend.writeMeta(newDirPath, {
        size: 0,
        mode: old_node.mode,
        ctime: old_node.ctime,
        mtime: old_node.mtime,
        atime: old_node.atime,
      });
      backend.deleteMeta(oldStoragePath);
      // Recursively update storagePaths for all file descendants.
      // Without this, pages remain keyed by old paths in the cache/backend,
      // causing data loss on syncfs → remount when metadata is persisted
      // under the new tree-computed paths but pages are under old paths.
      renameDescendantPaths(old_node, oldStoragePath, newDirPath);
    } else if (FS.isLink(old_node.mode)) {
      // Move symlink metadata eagerly for crash safety — same rationale
      // as file and directory renames. Without this, a crash between the
      // rename and the next syncfs would lose the rename: metadata stays
      // at the old path while the node tree has it at the new path.
      const newLinkPath = computeStoragePath(new_dir, new_name);
      backend.writeMeta(newLinkPath, {
        size: 0,
        mode: old_node.mode,
        ctime: old_node.ctime,
        mtime: old_node.mtime,
        atime: old_node.atime,
        link: old_node.link,
      });
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
    // Unlink modifies backend metadata directly (deletes or writes
    // /__deleted_* markers). A crash could leave orphaned entries.
    needsOrphanCleanup = true;
    const node = parent.contents[name];
    if (node && FS.isFile(node.mode)) {
      node.unlinked = true;
      if (node.openCount === 0) {
        pageCache.deleteFile(node.storagePath);
        backend.deleteMeta(node.storagePath);
      } else {
        // Open fds exist — move pages to a unique temporary path so the
        // original storagePath is free for reuse by new files or renames.
        // Without this, a new file at the same path would share page cache
        // entries with the unlinked node, causing data corruption.
        const originalPath = node.storagePath;
        const tempPath = `/__deleted_${nextPathId++}`;
        // Write marker metadata BEFORE renaming pages. This ensures that
        // if the process crashes after the page rename but before the
        // marker write, the /__deleted_* pages are still discoverable
        // via listFiles() for orphan cleanup. Without this ordering, a
        // crash between renameFile and writeMeta leaves pages at tempPath
        // with no metadata — permanently leaked since listFiles() only
        // returns paths with metadata.
        backend.writeMeta(tempPath, {
          size: node.usedBytes,
          mode: node.mode,
          ctime: node.ctime,
          mtime: node.mtime,
          atime: node.atime,
        });
        pageCache.renameFile(originalPath, tempPath);
        node.storagePath = tempPath;
        backend.deleteMeta(originalPath);
      }
      // Only remove from tracking if no open fds — syncfs needs to
      // preserve /__deleted_* paths for nodes with live fds.
      if (node.openCount === 0) {
        allFileNodes.delete(node);
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
    // rmdir deletes directory metadata from backend directly.
    needsOrphanCleanup = true;
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

  /**
   * Recursively rename storage paths for all file descendants of a directory.
   * Called during directory rename to keep storagePaths consistent with the
   * new tree structure, preventing data loss on persist → restore cycles.
   *
   * Collects all metadata operations during the tree walk, then executes
   * them in two batched backend calls (writeMetas + deleteMetas) instead
   * of O(3n) individual calls. Through the SAB bridge, this reduces
   * directory rename metadata round-trips from O(3n) to O(2).
   *
   * Metadata is constructed from in-memory node state rather than read
   * from the backend. The node tree is the source of truth for current
   * state; this also provides crash safety for files that haven't been
   * synced yet (their metadata gets written at the new path immediately).
   */
  function renameDescendantPaths(
    dirNode: any,
    oldDirPath: string,
    newDirPath: string,
  ): void {
    const metaWrites: Array<{ path: string; meta: FileMeta }> = [];
    const metaDeletes: string[] = [];

    function collect(node: any, oldBase: string, newBase: string): void {
      for (const childName of Object.keys(node.contents)) {
        const child = node.contents[childName];
        if (FS.isFile(child.mode)) {
          const oldPath = child.storagePath;
          const newPath = newBase + oldPath.substring(oldBase.length);
          pageCache.renameFile(oldPath, newPath);
          child.storagePath = newPath;
          metaWrites.push({
            path: newPath,
            meta: {
              size: child.usedBytes,
              mode: child.mode,
              ctime: child.ctime,
              mtime: child.mtime,
              atime: child.atime,
            },
          });
          metaDeletes.push(oldPath);
        } else if (FS.isLink(child.mode)) {
          const oldPath = oldBase + "/" + childName;
          const newPath = newBase + "/" + childName;
          metaWrites.push({
            path: newPath,
            meta: {
              size: 0,
              mode: child.mode,
              ctime: child.ctime,
              mtime: child.mtime,
              atime: child.atime,
              link: child.link,
            },
          });
          metaDeletes.push(oldPath);
        } else if (FS.isDir(child.mode)) {
          const oldChildPath = oldBase + "/" + childName;
          const newChildPath = newBase + "/" + childName;
          metaWrites.push({
            path: newChildPath,
            meta: {
              size: 0,
              mode: child.mode,
              ctime: child.ctime,
              mtime: child.mtime,
              atime: child.atime,
            },
          });
          metaDeletes.push(oldChildPath);
          collect(child, oldChildPath, newChildPath);
        }
      }
    }

    collect(dirNode, oldDirPath, newDirPath);

    // Batch-write all metadata at new paths, then batch-delete old paths.
    // Two backend calls instead of O(3n) individual calls.
    if (metaWrites.length > 0) {
      backend.writeMetas(metaWrites);
    }
    if (metaDeletes.length > 0) {
      backend.deleteMetas(metaDeletes);
    }
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

    dup(stream: any) {
      if (FS.isFile(stream.node.mode)) {
        stream.node.openCount++;
      }
    },

    close(stream: any) {
      const node = stream.node;
      if (FS.isFile(node.mode)) {
        node.openCount--;
        if (node.unlinked && node.openCount === 0) {
          // Last fd closed on an unlinked file — clean up pages + marker metadata
          pageCache.deleteFile(node.storagePath);
          backend.deleteMeta(node.storagePath); // removes /__deleted_* marker
          allFileNodes.delete(node);
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
      // offset is the file position (matching MEMFS: write(stream, buffer, 0, length, offset))
      writePages(stream.node, buffer, 0, length, offset);
      return 0;
    },
  };

  const dir_stream_ops = {
    llseek: stream_ops.llseek,
  };

  // ---------------------------------------------------------------
  // Persistence: save/restore directory tree to/from backend metadata
  // ---------------------------------------------------------------

  /**
   * Compute the mount-relative storage path for a node.
   *
   * For nodes in the mount root's tree, this walks up to the mount root
   * (where parent === node). For "detached" nodes parented in MEMFS,
   * the path walks through the mount point directory; we strip the mount
   * prefix so storage paths are always mount-relative.
   */
  function nodeStoragePath(node: any, mountPrefix: string): string {
    const full = nodePath(node);
    if (mountPrefix && full.startsWith(mountPrefix + "/")) {
      return full.substring(mountPrefix.length);
    }
    return full;
  }

  /**
   * Walk the in-memory node tree and persist all file/directory/symlink
   * metadata to the storage backend. File page data is flushed via the
   * page cache. Call this before unmount to ensure durability.
   */
  function persistTree(
    node: any,
    path: string,
    currentPaths: Set<string>,
    metaBatch: Array<{ path: string; meta: FileMeta }>,
  ): Set<any> {
    const visited = new Set<any>();

    function walk(node: any, path: string): void {
      visited.add(node);
      if (FS.isFile(node.mode)) {
        currentPaths.add(path);
        // Page data is already flushed by flushAll() before persistTree() is called.
        metaBatch.push({
          path,
          meta: {
            size: node.usedBytes,
            mode: node.mode,
            ctime: node.ctime,
            mtime: node.mtime,
            atime: node.atime,
          },
        });
      } else if (FS.isLink(node.mode)) {
        currentPaths.add(path);
        metaBatch.push({
          path,
          meta: {
            size: 0,
            mode: node.mode,
            ctime: node.ctime,
            mtime: node.mtime,
            atime: node.atime,
            link: node.link,
          },
        });
      } else if (FS.isDir(node.mode)) {
        // Persist directory metadata (skip root — it's recreated on mount)
        if (path !== "/") {
          currentPaths.add(path);
          metaBatch.push({
            path,
            meta: {
              size: 0,
              mode: node.mode,
              ctime: node.ctime,
              mtime: node.mtime,
              atime: node.atime,
            },
          });
        }
        // Recurse into children
        for (const name of Object.keys(node.contents)) {
          const childPath = path === "/" ? `/${name}` : `${path}/${name}`;
          walk(node.contents[name], childPath);
        }
      }
    }

    walk(node, path);
    return visited;
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

    // Filter out /__deleted_* marker entries — these are orphaned pages from
    // files that had open fds when the process crashed. They'll be
    // cleaned up by the first syncfs call (orphan cleanup pass).
    const livePaths = paths.filter((p) => !p.startsWith("/__deleted_"));

    // Batch-read all metadata in a single backend call to reduce SAB bridge
    // round-trips from O(n) to O(1) during mount/restore.
    const allMeta = backend.readMetas(livePaths);

    for (let i = 0; i < livePaths.length; i++) {
      const path = livePaths[i];
      const meta = allMeta[i];
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
        // Use metadata size, but verify against actual backend pages.
        // Pages may extend beyond meta.size if writes occurred after the
        // last metadata sync (e.g., Postgres shutdown writes during close),
        // or may be fewer if a truncation wasn't synced before a crash.
        // A single countPages call replaces the previous O(log n)
        // readPage-based binary search probing — no data transfer needed,
        // just a count query on the backend's page store.
        const storagePath = computeStoragePath(parent, name);
        const actualPageCount = backend.countPages(storagePath);
        const pagesFromMeta = meta.size > 0 ? Math.ceil(meta.size / PAGE_SIZE) : 0;

        let fileSize: number;
        if (actualPageCount === 0) {
          fileSize = 0;
        } else if (actualPageCount === pagesFromMeta) {
          // Page count matches metadata — trust meta.size for sub-page precision
          fileSize = meta.size;
        } else if (actualPageCount < pagesFromMeta) {
          // Fewer pages than expected. Two possible causes:
          // (a) Sparse file with zero-filled gaps — the last page exists but
          //     intermediate pages were never written. Trust meta.size.
          // (b) Crash after truncation deleted tail pages but before metadata
          //     was updated. The last expected page is missing. Fall back to
          //     probing for the true extent.
          // Distinguish by probing the last page implied by meta.size.
          const lastPageIndex = pagesFromMeta - 1;
          const lastPage = backend.readPage(storagePath, lastPageIndex);
          if (lastPage !== null) {
            // Last page exists — sparse file with gaps, trust metadata
            fileSize = meta.size;
          } else {
            // Last page missing — crash recovery. Use maxPageIndex to
            // find the true highest page index rather than assuming
            // pages are contiguous from index 0 (countPages only counts
            // stored pages, not their maximum index — non-contiguous
            // pages from allocate + crash would be lost otherwise).
            const highIdx = backend.maxPageIndex(storagePath);
            fileSize = highIdx >= 0 ? (highIdx + 1) * PAGE_SIZE : 0;
          }
        } else {
          // More pages than metadata expects: crash occurred between page
          // writes and metadata sync. Use maxPageIndex to find the true
          // highest page index — pages may be non-contiguous if
          // allocate/seek-past-end created sparse pages that were evicted
          // before the crash. countPages only counts stored pages, not
          // their maximum index.
          const highIdx = backend.maxPageIndex(storagePath);
          fileSize = highIdx >= 0 ? (highIdx + 1) * PAGE_SIZE : 0;
        }
        node.usedBytes = fileSize;
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

  // ---------------------------------------------------------------
  // Filesystem object
  // ---------------------------------------------------------------

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
          // Persist the full tree first, then clean up stale entries.
          // This ordering is critical for crash safety with IDB: if the
          // process is killed mid-sync (e.g., tab close), current metadata
          // is already written. Worst case is stale orphan entries that get
          // cleaned up on the next sync — never metadata loss.
          const currentPaths = new Set<string>();
          const metaBatch: Array<{ path: string; meta: FileMeta }> = [];
          const visited = persistTree(mount.root, "/", currentPaths, metaBatch);

          // Persist "detached" file nodes — nodes created via tomefs's
          // createNode but parented in MEMFS's directory tree instead of
          // the tomefs mount root. This happens with PGlite's MemoryFS
          // preloading, where Emscripten's path resolution routes through
          // MEMFS parent nodes. These nodes have tomefs stream_ops and
          // storagePaths, so reads/writes go through the page cache
          // correctly, but they don't appear in mount.root's subtree.
          const mountPrefix = mount.mountpoint || "";
          for (const node of allFileNodes) {
            if (visited.has(node)) continue;
            if (node.unlinked) continue;
            const path = nodeStoragePath(node, mountPrefix);
            currentPaths.add(path);
            // Always persist metadata for detached nodes — not just on first
            // sync. Without this, metadata (size, timestamps, mode) becomes
            // stale after the first sync if the file is modified between cycles.
            // flushAll() already flushed dirty pages, but detached nodes
            // may not have been flushed if their storagePath differs.
            pageCache.flushFile(node.storagePath);
            metaBatch.push({
              path,
              meta: {
                size: node.usedBytes,
                mode: node.mode,
                ctime: node.ctime,
                mtime: node.mtime,
                atime: node.atime,
              },
            });
          }

          // Also persist parent directories for detached nodes
          // so restoreTree can recreate the full tree structure.
          // Use currentPaths (in-memory set) instead of backend.readMeta()
          // to check for already-collected directories. This avoids O(depth)
          // synchronous backend reads per detached node — each of which is
          // a SAB bridge round-trip in production.
          for (const node of allFileNodes) {
            if (visited.has(node)) continue;
            if (node.unlinked) continue;
            // Walk up from node's parent to the mount boundary,
            // persisting any directories not already collected.
            let dir = node.parent;
            while (dir && dir.parent && dir.parent !== dir) {
              const dirPath = nodeStoragePath(dir, mountPrefix);
              if (currentPaths.has(dirPath)) break; // already collected
              currentPaths.add(dirPath);
              if (FS.isDir(dir.mode)) {
                metaBatch.push({
                  path: dirPath,
                  meta: {
                    size: 0,
                    mode: dir.mode,
                    ctime: dir.ctime,
                    mtime: dir.mtime,
                    atime: dir.atime,
                  },
                });
              }
              dir = dir.parent;
            }
          }

          // Preserve /__deleted_* paths for unlinked nodes that still have
          // open fds. These have marker metadata in the backend; without
          // adding them to currentPaths, orphan cleanup would delete their
          // pages while fds are still reading them.
          for (const node of allFileNodes) {
            if (node.unlinked && node.openCount > 0) {
              currentPaths.add(node.storagePath);
            }
          }

          // Batch-write all collected metadata in a single backend call.
          // Through the SAB bridge, this reduces O(n) round-trips to O(1).
          backend.writeMetas(metaBatch);

          // Delete metadata and orphaned page data for paths no longer in the tree.
          // Page data can become orphaned if the process crashes between an unlink
          // (which deletes pages) and the next syncfs (which updates metadata).
          // On restart, restoreTree recreates the file from stale metadata, but
          // the pages may already be gone — or they may still exist if the crash
          // happened before backend.deleteFile completed.  Either way, clean up both.
          //
          // Skip this scan when no orphans are possible — avoids a full
          // backend.listFiles() round-trip on every sync (the common case).
          if (needsOrphanCleanup) {
            const orphanPaths: string[] = [];
            for (const path of backend.listFiles()) {
              if (!currentPaths.has(path)) {
                orphanPaths.push(path);
              }
            }
            if (orphanPaths.length > 0) {
              backend.deleteFiles(orphanPaths);
              backend.deleteMetas(orphanPaths);
            }
            needsOrphanCleanup = false;
          }
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
      if (name.length > NAME_MAX) {
        throw new FS.ErrnoError(ENAMETOOLONG);
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
        // Track this file node for persistence
        allFileNodes.add(node);
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
