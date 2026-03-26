/**
 * Fake OPFS (Origin Private File System) implementation for Node.js testing.
 *
 * Provides in-memory implementations of FileSystemDirectoryHandle and
 * FileSystemFileHandle that match the browser OPFS API surface used by
 * OpfsBackend. This is a fake (not a mock) per project conventions.
 */

/** In-memory blob supporting arrayBuffer(), text(), slice(), and size. */
class FakeBlob {
  private _data: Uint8Array;

  constructor(data: Uint8Array) {
    this._data = new Uint8Array(data);
  }

  get size(): number {
    return this._data.byteLength;
  }

  async arrayBuffer(): Promise<ArrayBuffer> {
    return this._data.buffer.slice(
      this._data.byteOffset,
      this._data.byteOffset + this._data.byteLength,
    ) as ArrayBuffer;
  }

  async text(): Promise<string> {
    return new TextDecoder().decode(this._data);
  }

  slice(start?: number, end?: number): FakeBlob {
    return new FakeBlob(this._data.slice(start, end));
  }
}

/** In-memory file with name, size, and blob-like read methods. */
class FakeFile extends FakeBlob {
  readonly name: string;
  readonly lastModified: number;

  constructor(name: string, data: Uint8Array) {
    super(data);
    this.name = name;
    this.lastModified = Date.now();
  }
}

/** Writable stream that collects writes and commits on close. */
class FakeWritableFileStream {
  private chunks: Uint8Array[] = [];
  private committed = false;
  private readonly onClose: (data: Uint8Array) => void;

  constructor(onClose: (data: Uint8Array) => void) {
    this.onClose = onClose;
  }

  async write(data: BufferSource | string): Promise<void> {
    if (this.committed) {
      throw new DOMException("Stream is closed", "InvalidStateError");
    }
    if (typeof data === "string") {
      this.chunks.push(new TextEncoder().encode(data));
    } else if (data instanceof ArrayBuffer) {
      this.chunks.push(new Uint8Array(data));
    } else if (ArrayBuffer.isView(data)) {
      this.chunks.push(
        new Uint8Array(data.buffer, data.byteOffset, data.byteLength),
      );
    }
  }

  async close(): Promise<void> {
    if (this.committed) return;
    this.committed = true;

    // Concatenate all chunks
    const totalLen = this.chunks.reduce((s, c) => s + c.byteLength, 0);
    const result = new Uint8Array(totalLen);
    let offset = 0;
    for (const chunk of this.chunks) {
      result.set(chunk, offset);
      offset += chunk.byteLength;
    }
    this.onClose(result);
  }
}

/** Fake FileSystemFileHandle backed by in-memory data. */
class FakeFileHandle {
  readonly kind = "file" as const;
  readonly name: string;
  /** The stored file data. Null means the handle exists but has no data yet. */
  data: Uint8Array;

  constructor(name: string, data?: Uint8Array) {
    this.name = name;
    this.data = data ?? new Uint8Array(0);
  }

  async getFile(): Promise<FakeFile> {
    return new FakeFile(this.name, this.data);
  }

  async createWritable(): Promise<FakeWritableFileStream> {
    return new FakeWritableFileStream((committed) => {
      this.data = new Uint8Array(committed);
    });
  }
}

/** Fake FileSystemDirectoryHandle backed by in-memory Maps. */
class FakeDirectoryHandle {
  readonly kind = "directory" as const;
  readonly name: string;
  private dirs = new Map<string, FakeDirectoryHandle>();
  private files = new Map<string, FakeFileHandle>();

  constructor(name: string) {
    this.name = name;
  }

  async getDirectoryHandle(
    name: string,
    options?: { create?: boolean },
  ): Promise<FakeDirectoryHandle> {
    let dir = this.dirs.get(name);
    if (!dir) {
      if (!options?.create) {
        throw new DOMException(
          `Directory "${name}" not found`,
          "NotFoundError",
        );
      }
      dir = new FakeDirectoryHandle(name);
      this.dirs.set(name, dir);
    }
    return dir;
  }

  async getFileHandle(
    name: string,
    options?: { create?: boolean },
  ): Promise<FakeFileHandle> {
    let file = this.files.get(name);
    if (!file) {
      if (!options?.create) {
        throw new DOMException(`File "${name}" not found`, "NotFoundError");
      }
      file = new FakeFileHandle(name);
      this.files.set(name, file);
    }
    return file;
  }

  async removeEntry(
    name: string,
    options?: { recursive?: boolean },
  ): Promise<void> {
    if (this.files.has(name)) {
      this.files.delete(name);
      return;
    }
    if (this.dirs.has(name)) {
      const dir = this.dirs.get(name)!;
      if (!options?.recursive && (dir.dirs.size > 0 || dir.files.size > 0)) {
        throw new DOMException(
          `Directory "${name}" is not empty`,
          "InvalidModificationError",
        );
      }
      this.dirs.delete(name);
      return;
    }
    throw new DOMException(`Entry "${name}" not found`, "NotFoundError");
  }

  async *keys(): AsyncIterableIterator<string> {
    for (const name of this.dirs.keys()) {
      yield name;
    }
    for (const name of this.files.keys()) {
      yield name;
    }
  }

  async *values(): AsyncIterableIterator<FakeDirectoryHandle | FakeFileHandle> {
    for (const dir of this.dirs.values()) {
      yield dir;
    }
    for (const file of this.files.values()) {
      yield file;
    }
  }

  async *entries(): AsyncIterableIterator<
    [string, FakeDirectoryHandle | FakeFileHandle]
  > {
    for (const [name, dir] of this.dirs.entries()) {
      yield [name, dir];
    }
    for (const [name, file] of this.files.entries()) {
      yield [name, file];
    }
  }
}

/**
 * Create a fresh fake OPFS root directory handle.
 *
 * Usage:
 *   const root = createFakeOpfsRoot();
 *   const backend = new OpfsBackend({ root: root as any });
 */
export function createFakeOpfsRoot(): FakeDirectoryHandle {
  return new FakeDirectoryHandle("");
}
