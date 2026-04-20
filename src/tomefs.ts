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

  // Mount prefix for normalizing storage paths. Set in mount() and used by
  // computeStoragePath to strip the mount prefix from paths that traverse
  // MEMFS parent nodes (detached nodes). Without this, detached nodes get
  // storagePaths like "/data/base/1/1234" instead of "/base/1/1234",
  // causing page data and metadata to be stored under different paths.
  let mountPrefix = "";

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
  //
  // On mount, this is set to true by default. If the backend has a clean-
  // shutdown marker (written at the end of a successful syncfs), we know
  // the backend is consistent and can skip the first full tree walk.
  let needsOrphanCleanup = true;

  /** Backend key for the clean-shutdown marker. */
  const CLEAN_MARKER_PATH = "/__tomefs_clean";

  /** True when the clean marker needs to be written to the backend.
   *  Set when the marker is consumed during mount (restoreTree) and
   *  cleared after a successful syncfs writes it back. */
  let needsCleanMarker = false;

  /**
   * Set of nodes with dirty metadata (not yet persisted to the backend).
   *
   * Enables O(dirty) syncfs instead of O(tree): when no orphan cleanup is
   * needed, syncfs iterates only this set instead of walking the entire
   * node tree via persistTree. For PGlite workloads
   * where syncToFs is called after every query, this eliminates the
   * dominant overhead — repeated full tree walks on a large directory
   * structure with hundreds of Postgres catalog/data/WAL files.
   */
  const dirtyMetaNodes = new Set<any>();

  /** Mark a node's metadata as dirty and add it to the dirty set. */
  function markMetaDirty(node: any): void {
    if (!node._metaDirty) {
      node._metaDirty = true;
      dirtyMetaNodes.add(node);
    }
  }

  // ---------------------------------------------------------------
  // Page-cached file I/O
  // ---------------------------------------------------------------

  /**
   * Read bytes from a node's file data via the page cache.
   *
   * Includes a per-node page table optimization: each file node maintains
   * a sparse array of CachedPage references indexed by page number. For
   * pages already in the table, this provides O(1) direct access — no
   * string key construction, no Map lookup, no LRU reordering. The
   * CachedPage.evicted flag ensures stale references are lazily detected
   * and cleaned up.
   */
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

    // Single-page fast path with per-node page table (avoids pageCache overhead)
    const firstPage = (position / PAGE_SIZE) | 0;
    const pageOffset = position - firstPage * PAGE_SIZE;
    if (pageOffset + toRead <= PAGE_SIZE) {
      let page = node._pages?.[firstPage];
      if (page && page.evicted) {
        node._pages[firstPage] = undefined;
        page = undefined;
      }
      if (!page) {
        page = pageCache.getPage(node.storagePath, firstPage);
        if (!node._pages) node._pages = [];
        node._pages[firstPage] = page;
      }
      buffer.set(
        page.data.subarray(pageOffset, pageOffset + toRead),
        offset,
      );
      return toRead;
    }

    // Multi-page fast path: check per-node page table for all pages.
    // If every page is cached and not evicted, copy directly without
    // key construction, Map lookups, or LRU reordering overhead.
    const lastPage = ((position + toRead - 1) / PAGE_SIZE) | 0;
    const pages = node._pages;
    if (pages) {
      let allCached = true;
      for (let p = firstPage; p <= lastPage; p++) {
        const pg = pages[p];
        if (!pg || pg.evicted) {
          allCached = false;
          break;
        }
      }
      if (allCached) {
        let bytesRead = 0;
        let pos = position;
        while (bytesRead < toRead) {
          const pi = (pos / PAGE_SIZE) | 0;
          const po = pos - pi * PAGE_SIZE;
          const n = Math.min(PAGE_SIZE - po, toRead - bytesRead);
          buffer.set(pages[pi].data.subarray(po, po + n), offset + bytesRead);
          bytesRead += n;
          pos += n;
        }
        return bytesRead;
      }
    }

    // Multi-page cold path: delegate to page cache (handles batch loading
    // of missing pages via backend.readPages in a single SAB round-trip)
    const bytesRead = pageCache.read(
      node.storagePath,
      buffer,
      offset,
      toRead,
      position,
      size,
    );

    // Populate per-node page table from pages now in cache, so subsequent
    // reads at the same positions use the fast path above.
    if (!node._pages) node._pages = [];
    for (let p = firstPage; p <= lastPage; p++) {
      if (!node._pages[p]) {
        node._pages[p] = pageCache.getPage(node.storagePath, p);
      }
    }
    return bytesRead;
  }

  /**
   * Write bytes to a node's file data via the page cache.
   *
   * Single-page writes use a per-node page table for O(1) page lookup,
   * while maintaining dirty tracking through the cache's dirtyKeys index.
   */
  function writePages(
    node: any,
    buffer: Uint8Array,
    offset: number,
    length: number,
    position: number,
  ): number {
    if (length === 0) return 0;

    // Single-page fast path with per-node page table
    const firstPage = (position / PAGE_SIZE) | 0;
    const pageOffset = position - firstPage * PAGE_SIZE;
    if (pageOffset + length <= PAGE_SIZE) {
      let page = node._pages?.[firstPage];
      if (page && page.evicted) {
        node._pages[firstPage] = undefined;
        page = undefined;
      }
      if (!page) {
        // Skip backend read for pages beyond the current file extent —
        // they don't exist in the backend, so readPage would be a wasted
        // SAB bridge round-trip returning null.
        const firstNewPage = node.usedBytes > 0
          ? Math.ceil(node.usedBytes / PAGE_SIZE) : 0;
        // Skip backend read when the entire page will be overwritten —
        // every byte is about to be replaced, so reading the old data
        // is a wasted SAB bridge round-trip.
        const needsRead = firstPage < firstNewPage
          && !(pageOffset === 0 && length >= PAGE_SIZE);
        page = needsRead
          ? pageCache.getPage(node.storagePath, firstPage)
          : pageCache.getPageNoRead(node.storagePath, firstPage);
        if (!node._pages) node._pages = [];
        node._pages[firstPage] = page;
      }
      page.data.set(
        buffer.subarray(offset, offset + length),
        pageOffset,
      );
      if (!page.dirty) {
        page.dirty = true;
        pageCache.addDirtyKey(page.key, node.storagePath);
      }
      node.usedBytes = Math.max(node.usedBytes, position + length);
      return length;
    }

    // Multi-page warm path: check per-node page table for all pages.
    // If every page is cached and not evicted, write directly without
    // key construction, Map lookups, or LRU reordering overhead.
    const lastPage = ((position + length - 1) / PAGE_SIZE) | 0;
    const pages = node._pages;
    if (pages) {
      let allCached = true;
      for (let p = firstPage; p <= lastPage; p++) {
        const pg = pages[p];
        if (!pg || pg.evicted) {
          allCached = false;
          break;
        }
      }
      if (allCached) {
        let bytesWritten = 0;
        let pos = position;
        while (bytesWritten < length) {
          const pi = (pos / PAGE_SIZE) | 0;
          const po = pos - pi * PAGE_SIZE;
          const n = Math.min(PAGE_SIZE - po, length - bytesWritten);
          const page = pages[pi];
          page.data.set(
            buffer.subarray(offset + bytesWritten, offset + bytesWritten + n),
            po,
          );
          if (!page.dirty) {
            page.dirty = true;
            pageCache.addDirtyKey(page.key, node.storagePath);
          }
          bytesWritten += n;
          pos += n;
        }
        node.usedBytes = Math.max(node.usedBytes, position + length);
        return length;
      }
    }

    // Multi-page cold path: delegate to page cache (handles batch loading,
    // skip-read for new/overwritten pages, and eviction)
    const result = pageCache.write(
      node.storagePath,
      buffer,
      offset,
      length,
      position,
      node.usedBytes,
    );

    // Populate per-node page table from pages now in cache, so subsequent
    // writes at the same positions use the fast path above.
    if (!node._pages) node._pages = [];
    for (let p = firstPage; p <= lastPage; p++) {
      if (!node._pages[p]) {
        node._pages[p] = pageCache.getPage(node.storagePath, p);
      }
    }

    node.usedBytes = result.newFileSize;
    return result.bytesWritten;
  }

  /** Resize a file's storage (truncate or extend). */
  function resizeFileStorage(node: any, newSize: number): void {
    if (node.usedBytes === newSize) return;

    const path = node.storagePath;

    if (newSize === 0) {
      // Reset per-node page table — all pages are being deleted.
      node._pages = undefined;
      pageCache.deleteFile(path);
      node.usedBytes = 0;
      return;
    }

    if (newSize < node.usedBytes) {
      // Shrink: reset per-node page table — truncation invalidates pages
      // beyond the new size, and zeroTailAfterTruncate may reload the
      // last surviving page (replacing the cached CachedPage reference).
      node._pages = undefined;
      // Zero the tail of the last surviving page, then invalidate beyond
      const neededPages = Math.ceil(newSize / PAGE_SIZE);
      pageCache.zeroTailAfterTruncate(path, newSize);
      pageCache.invalidatePagesFrom(path, neededPages);
      backend.deletePagesFrom(path, neededPages);
    } else {
      // Growing: materialize only the LAST new page so it's flushed during
      // syncfs. restoreTree uses maxPageIndex to determine file extent — if
      // the highest stored page matches the last expected page, restoreTree
      // trusts metadata.size. Materializing the sentinel ensures maxPageIndex
      // reflects the true file extent after a crash. Intermediate pages don't
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
    if (attr.mode != null) node.mode = attr.mode;
    if (attr.atime != null) node.atime = attr.atime;
    if (attr.mtime != null) node.mtime = attr.mtime;
    if (attr.ctime != null) node.ctime = attr.ctime;
    if (attr.size !== undefined) resizeFileStorage(node, attr.size);
    markMetaDirty(node);
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
          new_node._pages = undefined;
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
      // Remove target from dirty tracking. Without this, the incremental
      // syncfs path would re-persist the target's metadata at its old
      // storage path — re-creating metadata that was just deleted or
      // overwritten, producing a ghost entry in the backend.
      if (new_node._metaDirty) {
        new_node._metaDirty = false;
        dirtyMetaNodes.delete(new_node);
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
      // Write metadata at the new path BEFORE moving pages for crash safety.
      // If the process dies after the page move (renameFile) but before
      // the metadata write, pages are at newPath with no metadata —
      // permanently unreachable and leaked. Writing metadata first ensures
      // data is never lost: worst case is a stale duplicate entry at the
      // old path that orphan cleanup removes on the next syncfs.
      // This matches the ordering used in unlink() for the same reason.
      backend.writeMeta(newStoragePath, {
        size: old_node.usedBytes,
        mode: old_node.mode,
        ctime: old_node.ctime,
        mtime: old_node.mtime,
        atime: old_node.atime,
      });
      pageCache.renameFile(oldStoragePath, newStoragePath);
      old_node.storagePath = newStoragePath;
      old_node._pages = undefined;
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
      // Recursively update storagePaths for all file descendants.
      // Without this, pages remain keyed by old paths in the cache/backend,
      // causing data loss on syncfs → remount when metadata is persisted
      // under the new tree-computed paths but pages are under old paths.
      renameDescendantPaths(old_node, oldStoragePath, newDirPath);
      // Delete old directory metadata LAST — after all descendant metadata
      // is written at new paths and pages are moved. During a crash, the
      // old directory must still exist so restoreTree can find it as a
      // parent for children whose metadata hasn't been moved yet.
      backend.deleteMeta(oldStoragePath);
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
    const old_parent = old_node.parent;
    delete old_parent.contents[old_node.name];
    new_dir.contents[new_name] = old_node;
    old_node.name = new_name;
    const now = Date.now();
    new_dir.ctime = new_dir.mtime = now;
    markMetaDirty(new_dir);
    old_parent.ctime = old_parent.mtime = now;
    markMetaDirty(old_parent);
  }

  function unlink(parent: any, name: string) {
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
        node._pages = undefined;
        backend.deleteMeta(originalPath);
      }
      // Only remove from tracking if no open fds — syncfs needs to
      // preserve /__deleted_* paths for nodes with live fds.
      if (node.openCount === 0) {
        allFileNodes.delete(node);
        // Remove from dirty tracking. The 'unlinked && openCount === 0'
        // check in incremental syncfs already skips this node, but
        // cleaning it up avoids scanning a dead node on every sync.
        if (node._metaDirty) {
          node._metaDirty = false;
          dirtyMetaNodes.delete(node);
        }
      }
    } else if (node && FS.isLink(node.mode)) {
      const sp = computeStoragePath(parent, name);
      backend.deleteMeta(sp);
      // Remove from dirty tracking. Without this, the incremental syncfs
      // path would re-persist the symlink's metadata — re-creating an
      // entry that was just deleted from the backend.
      if (node._metaDirty) {
        node._metaDirty = false;
        dirtyMetaNodes.delete(node);
      }
    }
    delete parent.contents[name];
    parent.ctime = parent.mtime = Date.now();
    markMetaDirty(parent);
  }

  function rmdir(parent: any, name: string) {
    const node = FS.lookupNode(parent, name);
    for (const _i in node.contents) {
      throw new FS.ErrnoError(55); // ENOTEMPTY
    }
    const sp = computeStoragePath(parent, name);
    backend.deleteMeta(sp);
    delete parent.contents[name];
    // Remove from dirty tracking. Without this, the incremental syncfs
    // path would re-persist the directory's metadata — re-creating an
    // entry that was just deleted from the backend.
    if (node._metaDirty) {
      node._metaDirty = false;
      dirtyMetaNodes.delete(node);
    }
    parent.ctime = parent.mtime = Date.now();
    markMetaDirty(parent);
  }

  /**
   * Compute a mount-relative storage path for a node given its parent and name.
   *
   * For nodes in the tomefs mount tree, the parent chain terminates at the
   * mount root (where parent === node), producing a mount-relative path like
   * "/base/1/1234". For "detached" nodes whose parent chain goes through
   * MEMFS, the walk reaches the MEMFS root, producing an absolute path like
   * "/data/base/1/1234" (where "/data" is the mount point). We strip the
   * mount prefix to normalize both cases to mount-relative paths, ensuring
   * page cache keys and metadata paths are always consistent.
   */
  function computeStoragePath(parent: any, name: string): string {
    const parts: string[] = [name];
    let n = parent;
    while (n.parent && n.parent !== n) {
      parts.unshift(n.name);
      n = n.parent;
    }
    const raw = "/" + parts.join("/");
    if (mountPrefix && raw.startsWith(mountPrefix + "/")) {
      return raw.substring(mountPrefix.length);
    }
    return raw;
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
    const pageRenames: Array<{ child: any; oldPath: string; newPath: string }> = [];
    const visitedNodes: any[] = [];

    function collect(node: any, oldBase: string, newBase: string): void {
      for (const childName of Object.keys(node.contents)) {
        const child = node.contents[childName];
        visitedNodes.push(child);
        if (FS.isFile(child.mode)) {
          const oldPath = child.storagePath;
          const newPath = newBase + oldPath.substring(oldBase.length);
          // Defer page rename — collect for batch execution after metadata
          // is written. This ensures metadata at the new path exists before
          // pages are moved, preventing data loss on mid-operation crashes.
          pageRenames.push({ child, oldPath, newPath });
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

    // Write metadata at new paths FIRST for crash safety: if the process
    // dies after a page rename but before its metadata write, pages at
    // the new path are unreachable (no metadata) and permanently leaked.
    // Writing all metadata before any page renames ensures data is never
    // lost — worst case is stale duplicates cleaned up by orphan cleanup.
    if (metaWrites.length > 0) {
      backend.writeMetas(metaWrites);
    }

    // Now move pages — metadata at new paths already exists.
    for (const { child, oldPath, newPath } of pageRenames) {
      pageCache.renameFile(oldPath, newPath);
      child.storagePath = newPath;
      child._pages = undefined;
    }

    // Delete old metadata last.
    if (metaDeletes.length > 0) {
      backend.deleteMetas(metaDeletes);
    }

    // Clear dirty flags — metadata was just written from live node state.
    // Without this, the next incremental syncfs re-persists all descendants.
    for (const node of visitedNodes) {
      if (node._metaDirty) {
        node._metaDirty = false;
        dirtyMetaNodes.delete(node);
      }
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
      markMetaDirty(node);
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
          if (node._metaDirty) {
            node._metaDirty = false;
            dirtyMetaNodes.delete(node);
          }
        }
        // Dirty pages remain in the cache and are flushed by syncfs or
        // eviction. POSIX close() does not guarantee persistence — that
        // is fsync's job. Deferring flush eliminates O(dirty) backend
        // writes on every close, matching MEMFS behavior.
      }
    },

    allocate(stream: any, offset: number, length: number) {
      const node = stream.node;
      const oldSize = node.usedBytes;
      resizeFileStorage(node, Math.max(oldSize, offset + length));
      if (node.usedBytes !== oldSize) {
        node.mtime = node.ctime = Date.now();
        markMetaDirty(node);
      }
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
      const node = stream.node;
      node.mtime = node.ctime = Date.now();
      markMetaDirty(node);
      writePages(node, buffer, 0, length, offset);
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
        // Only write metadata if it changed since last sync (or first sync).
        // Page data is collected via collectDirtyPages() and written
        // atomically with metadata via syncAll() after persistTree().
        if (node._metaDirty) {
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
      } else if (FS.isLink(node.mode)) {
        currentPaths.add(path);
        if (node._metaDirty) {
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
        }
      } else if (FS.isDir(node.mode)) {
        // Persist directory metadata (skip root — it's recreated on mount)
        if (path !== "/") {
          currentPaths.add(path);
          if (node._metaDirty) {
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

    // Check for clean-shutdown marker. If present AND no /__deleted_* orphans
    // exist, the backend is consistent — no orphan cleanup needed on first syncfs.
    // /__deleted_* entries are created by rename/unlink during normal operation
    // and cleaned up by orphan cleanup. If they survive to mount time, a crash
    // occurred between the operation and the next syncfs — the backend is dirty
    // regardless of the marker.
    const hasCleanMarker = paths.includes(CLEAN_MARKER_PATH);
    const hasOrphans = paths.some((p) => p.startsWith("/__deleted_"));
    if (hasCleanMarker && !hasOrphans) {
      needsOrphanCleanup = false;
    }
    if (hasCleanMarker) {
      needsCleanMarker = true; // Re-write marker on next syncfs
      backend.deleteMeta(CLEAN_MARKER_PATH);
    }

    // Filter out internal marker entries — /__deleted_* are orphaned pages
    // from files that had open fds when the process crashed, and
    // /__tomefs_clean is the shutdown marker we just consumed.
    const livePaths = paths.filter(
      (p) => !p.startsWith("/__deleted_") && p !== CLEAN_MARKER_PATH,
    );

    // Batch-read all metadata in a single backend call to reduce SAB bridge
    // round-trips from O(n) to O(1) during mount/restore.
    const allMeta = backend.readMetas(livePaths);

    // First pass: create directories and symlinks, collect file entries.
    // Files need maxPageIndex verification, which we batch in a single call
    // to reduce SAB bridge round-trips from O(n) to O(1).
    interface FileEntry {
      path: string;
      meta: FileMeta;
      parent: any;
      name: string;
      storagePath: string;
    }
    const fileEntries: FileEntry[] = [];

    // Build a path-to-node map during restoration so parent lookups are O(1)
    // instead of O(depth). Since paths are sorted by depth, parents are always
    // created before children.
    const nodeByPath = new Map<string, any>();
    nodeByPath.set("/", root);

    for (let i = 0; i < livePaths.length; i++) {
      const path = livePaths[i];
      const meta = allMeta[i];
      if (!meta) continue;

      // Split path into parent + name
      const lastSlash = path.lastIndexOf("/");
      const parentPath = path.substring(0, lastSlash) || "/";
      const name = path.substring(lastSlash + 1);
      if (!name) continue; // skip root

      // O(1) parent lookup via map instead of O(depth) tree walk
      const parent = nodeByPath.get(parentPath);
      if (!parent) continue;

      const typeMode = meta.mode & S_IFMT;
      if (typeMode === S_IFDIR) {
        const node = TOMEFS.createNode(parent, name, meta.mode, 0);
        node.atime = meta.atime ?? meta.mtime;
        node.mtime = meta.mtime;
        node.ctime = meta.ctime;
        // Metadata already in backend — no need to re-write on next sync.
        node._metaDirty = false;
        // Parent was marked dirty by createNode's timestamp update, but
        // restoreTree processes parents before children (depth-sorted), so
        // parent._metaDirty will be cleared when its own entry is processed.
        // Register directory in map so children can find it in O(1)
        nodeByPath.set(path, node);
      } else if (typeMode === S_IFREG) {
        const storagePath = computeStoragePath(parent, name);
        fileEntries.push({ path, meta, parent, name, storagePath });
      } else if (typeMode === S_IFLNK) {
        const node = TOMEFS.createNode(parent, name, meta.mode, 0);
        node.link = meta.link ?? "";
        node.atime = meta.atime ?? meta.mtime;
        node.mtime = meta.mtime;
        node.ctime = meta.ctime;
        node._metaDirty = false;
      }
    }

    // Batch maxPageIndex for all files in a single backend call.
    // This single call replaces the previous two-call approach
    // (countPagesBatch + maxPageIndexBatch), halving SAB bridge
    // round-trips during mount from 2 to 1.
    //
    // maxPageIndex alone encodes all the information we need:
    // - highIdx == -1: no pages exist → empty file
    // - highIdx == lastPageIndex: extent matches → trust meta.size
    // - highIdx > lastPageIndex: file extended past metadata → recover
    // - highIdx < lastPageIndex: crash truncation → adjust size
    if (fileEntries.length > 0) {
      const storagePaths = fileEntries.map((e) => e.storagePath);
      const allMaxIndices = backend.maxPageIndexBatch(storagePaths);

      for (let i = 0; i < fileEntries.length; i++) {
        const { meta, parent, name } = fileEntries[i];
        const pagesFromMeta = meta.size > 0 ? Math.ceil(meta.size / PAGE_SIZE) : 0;
        const highIdx = allMaxIndices[i];
        const lastPageIndex = pagesFromMeta - 1;

        let fileSize: number;
        if (highIdx < 0) {
          // No pages at all — empty file or fully truncated.
          fileSize = 0;
        } else if (highIdx > lastPageIndex) {
          // Pages exist beyond what metadata expects — file was extended
          // after last metadata sync (crash recovery or sparse+extension).
          fileSize = (highIdx + 1) * PAGE_SIZE;
        } else if (highIdx === lastPageIndex) {
          // Highest page matches last expected — common case or sparse file
          // with correct extent. Trust metadata for sub-page precision.
          fileSize = meta.size;
        } else {
          // Highest page is below last expected — pages were lost from the
          // end (crash truncation). Adjust size to actual extent.
          fileSize = (highIdx + 1) * PAGE_SIZE;
        }

        const node = TOMEFS.createNode(parent, name, meta.mode, 0);
        node.usedBytes = fileSize;
        node.atime = meta.atime ?? meta.mtime;
        node.mtime = meta.mtime;
        node.ctime = meta.ctime;
        node._metaDirty = false;
      }
    }

    // createNode marks parent directories dirty (timestamp updates), but
    // all metadata was just restored from the backend — nothing needs
    // re-writing. Clear dirty flags on only the affected nodes via the
    // dirty set — O(dirty) instead of O(tree).
    for (const node of dirtyMetaNodes) {
      node._metaDirty = false;
    }
    dirtyMetaNodes.clear();
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
      mountPrefix = _mount.mountpoint || "";
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
          // Fast path: nothing to sync — no dirty pages, no dirty metadata,
          // and no orphan cleanup needed. This is the common case when
          // PGlite calls syncToFs after read-only queries. Avoids the
          // O(tree-size) persistTree walk that otherwise dominates syncfs
          // cost for databases with hundreds of files.
          if (
            dirtyMetaNodes.size === 0 &&
            pageCache.dirtyCount === 0 &&
            !needsOrphanCleanup
          ) {
            // Re-write clean marker if it was consumed during mount.
            // This ensures a clean exit writes the marker even if no
            // data was modified (pure read-only session).
            if (needsCleanMarker) {
              backend.writeMeta(CLEAN_MARKER_PATH, {
                size: 0, mode: 0, ctime: Date.now(), mtime: Date.now(),
              });
              needsCleanMarker = false;
            }
            callback(null);
            return;
          }

          if (!needsOrphanCleanup) {
            // Incremental path: only persist metadata for dirty nodes.
            // O(dirty) instead of O(tree-size). This is the common case
            // for steady-state PGlite workloads where most queries only
            // modify a few files (WAL + heap + index) out of hundreds.
            //
            // Collect dirty pages without flushing, then combine with
            // dirty metadata into a single backend.syncAll() call. This
            // halves SAB bridge round-trips (2→1) and for IDB backends
            // writes pages + metadata in a single atomic transaction.
            const dirtyPages = pageCache.collectDirtyPages();
            const metaBatch: Array<{ path: string; meta: FileMeta }> = [];
            const mountPrefix = mount.mountpoint || "";

            for (const node of dirtyMetaNodes) {
              // Skip fully cleaned-up unlinked nodes
              if (node.unlinked && node.openCount === 0) continue;

              let path: string;
              if (FS.isFile(node.mode)) {
                path = node.storagePath;
              } else {
                path = nodeStoragePath(node, mountPrefix);
              }

              if (FS.isFile(node.mode)) {
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
            }

            // Include clean-shutdown marker in the same batch so it's
            // atomically committed with the data (for IDB backends).
            metaBatch.push({
              path: CLEAN_MARKER_PATH,
              meta: { size: 0, mode: 0, ctime: Date.now(), mtime: Date.now() },
            });
            backend.syncAll(dirtyPages, metaBatch);

            // Two-phase commit: clear dirty flags only after the backend
            // write succeeds. If syncAll throws, dirty flags are preserved
            // so the next syncfs retries instead of silently losing data.
            pageCache.commitDirtyPages(dirtyPages);
            needsCleanMarker = false;
            for (const node of dirtyMetaNodes) {
              node._metaDirty = false;
            }
            dirtyMetaNodes.clear();
          } else {
            // Full tree walk path: needed when orphan cleanup is required
            // (after rename/unlink operations). Builds currentPaths set to
            // detect stale backend entries.
            //
            // Collect dirty pages first, then build the metadata batch,
            // then write both atomically via syncAll. Orphan cleanup
            // follows as a separate operation.
            //
            // This ordering is critical for crash safety with IDB: if the
            // process is killed mid-sync (e.g., tab close), current data
            // is already written. Worst case is stale orphan entries that
            // get cleaned up on the next sync — never data loss.
            const dirtyPages = pageCache.collectDirtyPages();
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
              // Only persist metadata when dirty — avoids redundant backend
              // writes for detached nodes that haven't changed since last sync.
              if (node._metaDirty) {
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

            // Write dirty pages + metadata atomically via syncAll.
            // For IDB, this is a single multi-store transaction.
            // Include clean-shutdown marker in the same batch.
            metaBatch.push({
              path: CLEAN_MARKER_PATH,
              meta: { size: 0, mode: 0, ctime: Date.now(), mtime: Date.now() },
            });
            backend.syncAll(dirtyPages, metaBatch);
            pageCache.commitDirtyPages(dirtyPages);
            needsCleanMarker = false;

            // Clear dirty flags on all nodes whose metadata was persisted.
            // Uses the dirty set instead of walking the full tree — O(dirty)
            // instead of O(tree) + O(files).
            for (const node of dirtyMetaNodes) {
              node._metaDirty = false;
            }
            dirtyMetaNodes.clear();

            // Delete metadata and orphaned page data for paths no longer in the tree.
            // Exclude the clean-shutdown marker — it's internal bookkeeping, not
            // an orphan. It will be re-written after this cleanup completes.
            const orphanPaths: string[] = [];
            for (const path of backend.listFiles()) {
              if (!currentPaths.has(path) && path !== CLEAN_MARKER_PATH) {
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
        // Per-node page table: sparse array of CachedPage references
        // indexed by page number. Provides O(1) direct page access,
        // bypassing string key construction and Map lookup in the cache.
        // Stale entries (evicted pages) are detected via CachedPage.evicted.
        node._pages = undefined;
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
      markMetaDirty(node);

      if (parent) {
        parent.contents[name] = node;
        parent.atime = parent.mtime = parent.ctime = node.atime;
        markMetaDirty(parent);
      }

      return node;
    },
  };

  return TOMEFS;
}
