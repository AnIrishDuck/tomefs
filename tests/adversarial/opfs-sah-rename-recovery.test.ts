/**
 * Adversarial tests: OpfsSahBackend.renameFile error recovery.
 *
 * When the write to the new file fails during rename (e.g., disk full,
 * I/O error), the backend must:
 *   1. Clean up the partial new file
 *   2. Preserve the old file intact
 *   3. Propagate the original error
 *
 * Without error recovery, a write failure leaves a partial new file in
 * the pages directory — an orphan that wastes space and could confuse
 * restoreTree if metadata happens to reference that path.
 *
 * Uses a FailingSahDirectoryHandle fake that wraps the standard fake
 * OPFS and injects write errors for targeted paths.
 *
 * Ethos §9: "Write tests designed to break tomefs specifically — target
 * the seams"
 */

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { OpfsSahBackend } from "../../src/opfs-sah-backend.js";
import { PAGE_SIZE } from "../../src/types.js";
import { createFakeOpfsRoot } from "../harness/fake-opfs.js";

function filledPage(value: number): Uint8Array {
  const page = new Uint8Array(PAGE_SIZE);
  page.fill(value);
  return page;
}

const HEX_TABLE: string[] = new Array(256);
for (let i = 0; i < 256; i++) {
  HEX_TABLE[i] = i.toString(16).padStart(2, "0");
}

function encodePath(path: string): string {
  const bytes = new TextEncoder().encode(path);
  const parts = new Array<string>(bytes.length);
  for (let i = 0; i < bytes.length; i++) {
    parts[i] = HEX_TABLE[bytes[i]];
  }
  return parts.join("");
}

/**
 * Wraps a fake OPFS root and intercepts getFileHandle on the pages
 * directory to inject sync access handle write failures for targeted
 * encoded path names.
 */
function createFailingRoot(
  fakeRoot: any,
  failOnWritePath: string | null,
): { root: any; setFailPath: (p: string | null) => void } {
  let failPath = failOnWritePath;

  const setFailPath = (p: string | null) => {
    failPath = p;
  };

  const wrapDirectoryHandle = (dir: any, isPagesDir: boolean): any => {
    return new Proxy(dir, {
      get(target: any, prop: string | symbol) {
        if (prop === "getDirectoryHandle") {
          return async (name: string, options?: any) => {
            const result = await target.getDirectoryHandle(name, options);
            const isPages = name === "pages";
            return wrapDirectoryHandle(result, isPages);
          };
        }

        if (isPagesDir && prop === "getFileHandle") {
          return async (name: string, options?: any) => {
            const handle = await target.getFileHandle(name, options);
            if (failPath && name === failPath && options?.create) {
              return wrapFileHandleToFail(handle);
            }
            return handle;
          };
        }

        const val = target[prop];
        if (typeof val === "function") return val.bind(target);
        return val;
      },
    });
  };

  const wrapFileHandleToFail = (handle: any): any => {
    return new Proxy(handle, {
      get(target: any, prop: string | symbol) {
        if (prop === "createSyncAccessHandle") {
          return async () => {
            const sah = await target.createSyncAccessHandle();
            return wrapSahToFail(sah);
          };
        }
        const val = target[prop];
        if (typeof val === "function") return val.bind(target);
        return val;
      },
    });
  };

  const wrapSahToFail = (sah: any): any => {
    return new Proxy(sah, {
      get(target: any, prop: string | symbol) {
        if (prop === "write") {
          return () => {
            throw new Error("Simulated write failure");
          };
        }
        const val = target[prop];
        if (typeof val === "function") return val.bind(target);
        return val;
      },
    });
  };

  return {
    root: wrapDirectoryHandle(fakeRoot, false),
    setFailPath,
  };
}

describe("OpfsSahBackend renameFile error recovery", () => {
  let backend: OpfsSahBackend;
  let setFailPath: (p: string | null) => void;

  beforeEach(() => {
    const fakeRoot = createFakeOpfsRoot();
    const failing = createFailingRoot(fakeRoot, null);
    setFailPath = failing.setFailPath;
    backend = new OpfsSahBackend({ root: failing.root });
  });

  afterEach(async () => {
    await backend.destroy();
  });

  it("preserves old file when write to new file fails @fast", async () => {
    const data = filledPage(0xaa);
    await backend.writePage("/old", 0, data);
    await backend.writePage("/old", 1, filledPage(0xbb));

    setFailPath(encodePath("/new"));

    await expect(backend.renameFile("/old", "/new")).rejects.toThrow(
      "Simulated write failure",
    );

    expect(await backend.readPage("/old", 0)).toEqual(data);
    expect(await backend.readPage("/old", 1)).toEqual(filledPage(0xbb));
  });

  it("does not leave partial new file on write failure @fast", async () => {
    await backend.writePage("/src", 0, filledPage(0x11));

    setFailPath(encodePath("/dst"));

    await expect(backend.renameFile("/src", "/dst")).rejects.toThrow(
      "Simulated write failure",
    );

    expect(await backend.readPage("/dst", 0)).toBeNull();
  });

  it("propagates the original write error @fast", async () => {
    await backend.writePage("/a", 0, filledPage(0x01));

    setFailPath(encodePath("/b"));

    const err = await backend.renameFile("/a", "/b").catch((e: Error) => e);
    expect(err).toBeInstanceOf(Error);
    expect((err as Error).message).toBe("Simulated write failure");
  });

  it("rename succeeds when no failure is injected @fast", async () => {
    await backend.writePage("/old", 0, filledPage(0xcc));

    await backend.renameFile("/old", "/new");

    expect(await backend.readPage("/old", 0)).toBeNull();
    expect(await backend.readPage("/new", 0)).toEqual(filledPage(0xcc));
  });

  it("can rename again after a failed attempt @fast", async () => {
    const data = filledPage(0xdd);
    await backend.writePage("/src", 0, data);

    setFailPath(encodePath("/dst"));
    await expect(backend.renameFile("/src", "/dst")).rejects.toThrow();

    expect(await backend.readPage("/src", 0)).toEqual(data);
    expect(await backend.readPage("/dst", 0)).toBeNull();

    setFailPath(null);
    await backend.renameFile("/src", "/dst");

    expect(await backend.readPage("/src", 0)).toBeNull();
    expect(await backend.readPage("/dst", 0)).toEqual(data);
  });

  it("preserves multi-page file on write failure @fast", async () => {
    const pages = [filledPage(0x10), filledPage(0x20), filledPage(0x30)];
    for (let i = 0; i < pages.length; i++) {
      await backend.writePage("/multi", i, pages[i]);
    }

    setFailPath(encodePath("/target"));
    await expect(backend.renameFile("/multi", "/target")).rejects.toThrow();

    for (let i = 0; i < pages.length; i++) {
      expect(await backend.readPage("/multi", i)).toEqual(pages[i]);
    }
    expect(await backend.readPage("/target", 0)).toBeNull();
  });

  it("handles write failure when destination previously existed @fast", async () => {
    await backend.writePage("/existing", 0, filledPage(0xee));
    await backend.writePage("/src", 0, filledPage(0xff));

    setFailPath(encodePath("/existing"));
    await expect(backend.renameFile("/src", "/existing")).rejects.toThrow();

    expect(await backend.readPage("/src", 0)).toEqual(filledPage(0xff));
  });

  it("empty file rename succeeds even when write failures are armed @fast", async () => {
    const encoded = encodePath("/empty");
    await backend.writePage("/empty", 0, filledPage(0x01));
    await backend.deletePagesFrom("/empty", 0);

    setFailPath(encoded);

    await backend.renameFile("/empty", "/empty-renamed");
  });
});
