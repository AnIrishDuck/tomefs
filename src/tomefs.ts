/**
 * tomefs — A page-structured Emscripten filesystem.
 *
 * Implements the standard Emscripten custom filesystem interface (mount,
 * createNode, node_ops, stream_ops). File data is stored as PAGE_SIZE-byte
 * pages, matching Postgres's internal page size for 1:1 alignment.
 *
 * This module exports a factory function that creates a tomefs instance
 * bound to a specific Emscripten FS module. The instance can then be
 * mounted via FS.mount(tomefs, opts, '/mountpoint').
 *
 * For now, pages are stored in memory (like MEMFS but page-structured).
 * This validates the Emscripten FS interface against the conformance tests.
 * Later, the page store will be backed by PageCache + StorageBackend for
 * persistent, bounded-memory operation.
 */

import { PAGE_SIZE } from "./types.js";

/** Options for creating a tomefs instance. */
export interface TomeFSOptions {
  /** Maximum pages to keep in memory. 0 = unlimited. Default: 0 (unlimited). */
  maxPages?: number;
}

/**
 * Create a tomefs filesystem instance bound to an Emscripten FS module.
 *
 * Usage:
 *   const tomefs = createTomeFS(Module.FS);
 *   FS.mkdir('/data');
 *   FS.mount(tomefs, {}, '/data');
 */
export function createTomeFS(FS: any, _options?: TomeFSOptions): any {
  // ---------------------------------------------------------------
  // Page-structured file storage
  // ---------------------------------------------------------------

  /** Read bytes from a node's page array. */
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

    const pages: Uint8Array[] = node.pages;
    let bytesRead = 0;
    let pos = position;

    while (bytesRead < toRead) {
      const pageIndex = Math.floor(pos / PAGE_SIZE);
      const pageOffset = pos % PAGE_SIZE;
      const chunk = Math.min(PAGE_SIZE - pageOffset, toRead - bytesRead);

      const page = pages[pageIndex];
      if (page) {
        buffer.set(
          page.subarray(pageOffset, pageOffset + chunk),
          offset + bytesRead,
        );
      } else {
        // Sparse: page doesn't exist yet, read as zeros
        buffer.fill(0, offset + bytesRead, offset + bytesRead + chunk);
      }

      bytesRead += chunk;
      pos += chunk;
    }

    return bytesRead;
  }

  /** Write bytes into a node's page array. */
  function writePages(
    node: any,
    buffer: Uint8Array,
    offset: number,
    length: number,
    position: number,
  ): number {
    if (length === 0) return 0;

    const pages: Uint8Array[] = node.pages;
    let bytesWritten = 0;
    let pos = position;

    while (bytesWritten < length) {
      const pageIndex = Math.floor(pos / PAGE_SIZE);
      const pageOffset = pos % PAGE_SIZE;
      const chunk = Math.min(PAGE_SIZE - pageOffset, length - bytesWritten);

      // Ensure page exists
      while (pages.length <= pageIndex) {
        pages.push(new Uint8Array(PAGE_SIZE));
      }
      if (!pages[pageIndex]) {
        pages[pageIndex] = new Uint8Array(PAGE_SIZE);
      }

      pages[pageIndex].set(
        buffer.subarray(offset + bytesWritten, offset + bytesWritten + chunk),
        pageOffset,
      );

      bytesWritten += chunk;
      pos += chunk;
    }

    node.usedBytes = Math.max(node.usedBytes, position + length);
    return bytesWritten;
  }

  /** Resize a file's storage (truncate or extend). */
  function resizeFileStorage(node: any, newSize: number): void {
    if (node.usedBytes === newSize) return;

    const pages: Uint8Array[] = node.pages;

    if (newSize === 0) {
      node.pages = [];
      node.usedBytes = 0;
      return;
    }

    const neededPages = Math.ceil(newSize / PAGE_SIZE);

    if (newSize < node.usedBytes) {
      // Shrink: drop excess pages
      pages.length = neededPages;
      // Zero the tail of the last page beyond newSize
      const tailOffset = newSize % PAGE_SIZE;
      if (tailOffset > 0 && pages[neededPages - 1]) {
        pages[neededPages - 1].fill(0, tailOffset);
      }
    } else {
      // Grow: allocate new zero-filled pages as needed
      while (pages.length < neededPages) {
        pages.push(new Uint8Array(PAGE_SIZE));
      }
    }

    node.usedBytes = newSize;
  }

  // ---------------------------------------------------------------
  // Node operations
  // ---------------------------------------------------------------

  // Shared node operations (getattr, setattr, mknod)
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
    // Check if target exists
    let new_node: any;
    try {
      new_node = FS.lookupNode(new_dir, new_name);
    } catch (_e) {
      // Target doesn't exist — that's fine
    }

    if (new_node) {
      if (FS.isDir(old_node.mode)) {
        // Overwriting a directory: it must be empty
        for (const _i in new_node.contents) {
          throw new FS.ErrnoError(55); // ENOTEMPTY
        }
      }
      FS.hashRemoveNode(new_node);
    }

    // Rewire
    delete old_node.parent.contents[old_node.name];
    new_dir.contents[new_name] = old_node;
    old_node.name = new_name;
    const now = Date.now();
    new_dir.ctime = new_dir.mtime = old_node.parent.ctime =
      old_node.parent.mtime = now;
  }

  function unlink(parent: any, name: string) {
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

  /** Directory node_ops — includes readdir, symlink, readlink. */
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
      const node = TOMEFS.createNode(
        parent,
        newname,
        0o777 | 0o120000,
        0,
      );
      node.link = oldpath;
      return node;
    },
  };

  /** File node_ops — no readdir/symlink. */
  const file_node_ops = {
    getattr,
    setattr,
  };

  /** Symlink node_ops — includes readlink. */
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

    open(_stream: any) {
      // No-op — O_TRUNC and O_APPEND are handled by the FS layer
    },

    close(_stream: any) {
      // No-op for in-memory storage
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
      // Read data into a temporary buffer for mmap emulation
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

  // Dir streams only need llseek
  const dir_stream_ops = {
    llseek: stream_ops.llseek,
  };

  // ---------------------------------------------------------------
  // Filesystem object
  // ---------------------------------------------------------------

  const TOMEFS = {
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
        node.pages = [];
        node.usedBytes = 0;
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
