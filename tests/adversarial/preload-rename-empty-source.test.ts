/**
 * Adversarial tests for PreloadBackend rename with empty source files.
 *
 * The PreloadBackend.renameFile method has a critical ordering constraint:
 * destination pages must be cleared BEFORE checking if the source has pages
 * to move. Without this, renaming an empty file over a file with data would
 * leave orphan pages at the destination.
 *
 * This is documented in the code comment at the destination-clearing block
 * but had no focused test coverage.
 *
 * Ethos §9: "Write tests designed to break tomefs specifically"
 * Ethos §10: "Graceful degradation without SharedArrayBuffer"
 */

import { describe, it, expect } from "vitest";
import { MemoryBackend } from "../../src/memory-backend.js";
import { PreloadBackend } from "../../src/preload-backend.js";
import { PAGE_SIZE } from "../../src/types.js";

function filledPage(v: number): Uint8Array {
  const d = new Uint8Array(PAGE_SIZE);
  d.fill(v);
  return d;
}

describe("PreloadBackend: rename empty source over destination with data", () => {
  it("clears destination pages when source has zero pages @fast", async () => {
    const remote = new MemoryBackend();
    await remote.writePage("/dest", 0, filledPage(0xdd));
    await remote.writePage("/dest", 1, filledPage(0xee));
    await remote.writeMeta("/dest", {
      size: 2 * PAGE_SIZE,
      mode: 0o100644,
      ctime: 1000,
      mtime: 1000,
      atime: 1000,
    });
    // Source has metadata but NO pages (e.g. an empty file)
    await remote.writeMeta("/src", {
      size: 0,
      mode: 0o100644,
      ctime: 2000,
      mtime: 2000,
      atime: 2000,
    });

    const backend = new PreloadBackend(remote);
    await backend.init();

    backend.renameFile("/src", "/dest");

    // Destination pages must be gone
    expect(backend.readPage("/dest", 0)).toBeNull();
    expect(backend.readPage("/dest", 1)).toBeNull();
    // Source pages also gone (there were none)
    expect(backend.readPage("/src", 0)).toBeNull();

    // After flush, remote should be clean too
    await backend.flush();
    expect(await remote.readPage("/dest", 0)).toBeNull();
    expect(await remote.readPage("/dest", 1)).toBeNull();
    expect(await remote.readPage("/src", 0)).toBeNull();
  });

  it("clears destination pages when source was never in the backend @fast", async () => {
    const remote = new MemoryBackend();
    await remote.writePage("/dest", 0, filledPage(0xaa));
    await remote.writeMeta("/dest", {
      size: PAGE_SIZE,
      mode: 0o100644,
      ctime: 1000,
      mtime: 1000,
      atime: 1000,
    });

    const backend = new PreloadBackend(remote);
    await backend.init();

    // Create a new file locally with no pages, then rename over destination
    backend.writeMeta("/new", {
      size: 0,
      mode: 0o100644,
      ctime: 3000,
      mtime: 3000,
      atime: 3000,
    });

    backend.renameFile("/new", "/dest");

    expect(backend.readPage("/dest", 0)).toBeNull();

    await backend.flush();
    expect(await remote.readPage("/dest", 0)).toBeNull();
  });

  it("clears multi-page destination when source has zero pages", async () => {
    const remote = new MemoryBackend();
    for (let i = 0; i < 5; i++) {
      await remote.writePage("/big", i, filledPage(0x10 + i));
    }
    await remote.writeMeta("/big", {
      size: 5 * PAGE_SIZE,
      mode: 0o100644,
      ctime: 1000,
      mtime: 1000,
      atime: 1000,
    });
    await remote.writeMeta("/empty", {
      size: 0,
      mode: 0o100644,
      ctime: 2000,
      mtime: 2000,
      atime: 2000,
    });

    const backend = new PreloadBackend(remote);
    await backend.init();

    backend.renameFile("/empty", "/big");

    for (let i = 0; i < 5; i++) {
      expect(backend.readPage("/big", i)).toBeNull();
    }
    expect(backend.countPages("/big")).toBe(0);

    await backend.flush();

    for (let i = 0; i < 5; i++) {
      expect(await remote.readPage("/big", i)).toBeNull();
    }
  });

  it("sequential rename-empty-over-data then write-at-destination", async () => {
    const remote = new MemoryBackend();
    await remote.writePage("/old", 0, filledPage(0xff));
    await remote.writeMeta("/old", {
      size: PAGE_SIZE,
      mode: 0o100644,
      ctime: 1000,
      mtime: 1000,
      atime: 1000,
    });
    await remote.writeMeta("/empty", {
      size: 0,
      mode: 0o100644,
      ctime: 2000,
      mtime: 2000,
      atime: 2000,
    });

    const backend = new PreloadBackend(remote);
    await backend.init();

    // Rename empty file over destination (clears old data)
    backend.renameFile("/empty", "/old");
    expect(backend.readPage("/old", 0)).toBeNull();

    // Now write new data at the same path
    const newData = filledPage(0xab);
    backend.writePage("/old", 0, newData);
    expect(backend.readPage("/old", 0)).toEqual(newData);

    // Flush and verify — the delete-then-recreate flush path must handle this
    await backend.flush();
    expect(await remote.readPage("/old", 0)).toEqual(newData);
  });

  it("double rename: empty over A, then A over B", async () => {
    const remote = new MemoryBackend();
    await remote.writePage("/a", 0, filledPage(0xaa));
    await remote.writeMeta("/a", {
      size: PAGE_SIZE,
      mode: 0o100644,
      ctime: 1000,
      mtime: 1000,
      atime: 1000,
    });
    await remote.writePage("/b", 0, filledPage(0xbb));
    await remote.writeMeta("/b", {
      size: PAGE_SIZE,
      mode: 0o100644,
      ctime: 1000,
      mtime: 1000,
      atime: 1000,
    });
    await remote.writeMeta("/empty", {
      size: 0,
      mode: 0o100644,
      ctime: 2000,
      mtime: 2000,
      atime: 2000,
    });

    const backend = new PreloadBackend(remote);
    await backend.init();

    // Rename empty over /a (clears /a's data)
    backend.renameFile("/empty", "/a");
    expect(backend.readPage("/a", 0)).toBeNull();

    // Rename (now-empty) /a over /b (clears /b's data)
    backend.renameFile("/a", "/b");
    expect(backend.readPage("/b", 0)).toBeNull();
    expect(backend.readPage("/a", 0)).toBeNull();

    await backend.flush();
    expect(await remote.readPage("/a", 0)).toBeNull();
    expect(await remote.readPage("/b", 0)).toBeNull();
  });

  it("rename empty over destination preserves dirty tracking for other files", async () => {
    const remote = new MemoryBackend();
    await remote.writePage("/dest", 0, filledPage(0xdd));
    await remote.writeMeta("/dest", {
      size: PAGE_SIZE,
      mode: 0o100644,
      ctime: 1000,
      mtime: 1000,
      atime: 1000,
    });
    await remote.writeMeta("/empty", {
      size: 0,
      mode: 0o100644,
      ctime: 2000,
      mtime: 2000,
      atime: 2000,
    });

    const backend = new PreloadBackend(remote);
    await backend.init();

    // Write to an unrelated file
    const otherData = filledPage(0xcc);
    backend.writePage("/other", 0, otherData);
    backend.writeMeta("/other", {
      size: PAGE_SIZE,
      mode: 0o100644,
      ctime: 3000,
      mtime: 3000,
      atime: 3000,
    });

    // Rename empty over destination
    backend.renameFile("/empty", "/dest");

    // The unrelated file's dirty state must survive
    expect(backend.isDirty).toBe(true);

    await backend.flush();

    expect(await remote.readPage("/other", 0)).toEqual(otherData);
    expect(await remote.readPage("/dest", 0)).toBeNull();
  });
});
