/**
 * Adversarial tests: Rename lookupNode error propagation.
 *
 * When rename checks whether the target path exists, it calls
 * FS.lookupNode(new_dir, new_name). If this throws ENOENT (target
 * doesn't exist), that's expected and rename proceeds normally.
 * But if it throws any OTHER error (EACCES, ENOTDIR, or an internal
 * error), rename must propagate it — not silently swallow it.
 *
 * A catch-all at this point would mask filesystem corruption, permission
 * errors, or internal bugs, causing rename to silently proceed as if the
 * target doesn't exist when it actually failed for a different reason.
 *
 * Ethos §9: "Write tests designed to break tomefs specifically"
 */
import { describe, it, expect, beforeEach } from "vitest";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { createTomeFS } from "../../src/tomefs.js";
import { PAGE_SIZE } from "../../src/types.js";
import { fileURLToPath } from "url";
import { dirname, join } from "path";

const __dirname = dirname(fileURLToPath(import.meta.url));

const O = {
  RDONLY: 0,
  WRONLY: 1,
  RDWR: 2,
  CREAT: 64,
  TRUNC: 512,
} as const;

const MOUNT = "/tome";
const EACCES = 2;

function encode(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

async function mountTome(backend: SyncMemoryBackend) {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  const Module = await createModule();
  const FS = Module.FS;
  const tomefs = createTomeFS(FS, { backend });
  FS.mkdir(MOUNT);
  FS.mount(tomefs, {}, MOUNT);
  return { FS, tomefs, Module };
}

describe("adversarial: rename lookupNode error propagation (tomefs)", () => {
  let backend: SyncMemoryBackend;

  beforeEach(() => {
    backend = new SyncMemoryBackend();
  });

  it("propagates non-ENOENT errors from target lookup @fast", async () => {
    const { FS } = await mountTome(backend);

    const data = encode("source data");
    const s = FS.open(`${MOUNT}/src`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    FS.close(s);

    const mountNode = FS.lookupPath(MOUNT).node;
    const originalLookup = mountNode.node_ops.lookup;

    mountNode.node_ops.lookup = (parent: any, name: string) => {
      if (name === "dst") throw new FS.ErrnoError(EACCES);
      return originalLookup(parent, name);
    };

    try {
      expect(() => FS.rename(`${MOUNT}/src`, `${MOUNT}/dst`)).toThrow();
      expect(FS.stat(`${MOUNT}/src`).size).toBe(data.length);
    } finally {
      mountNode.node_ops.lookup = originalLookup;
    }
  });

  it("still proceeds normally when target throws ENOENT", async () => {
    const { FS } = await mountTome(backend);

    const data = encode("should rename fine");
    const s = FS.open(`${MOUNT}/src`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    FS.close(s);

    FS.rename(`${MOUNT}/src`, `${MOUNT}/dst`);

    expect(FS.stat(`${MOUNT}/dst`).size).toBe(data.length);
    expect(() => FS.stat(`${MOUNT}/src`)).toThrow();
  });

  it("propagates non-ENOENT with open fd on source @fast", async () => {
    const { FS } = await mountTome(backend);

    const data = encode("open fd test");
    const s = FS.open(`${MOUNT}/src`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);

    const mountNode = FS.lookupPath(MOUNT).node;
    const originalLookup = mountNode.node_ops.lookup;

    mountNode.node_ops.lookup = (parent: any, name: string) => {
      if (name === "dst") throw new FS.ErrnoError(EACCES);
      return originalLookup(parent, name);
    };

    try {
      expect(() => FS.rename(`${MOUNT}/src`, `${MOUNT}/dst`)).toThrow();
      expect(FS.stat(`${MOUNT}/src`).size).toBe(data.length);

      const buf = new Uint8Array(data.length);
      FS.llseek(s, 0, 0);
      FS.read(s, buf, 0, data.length);
      expect(buf).toEqual(data);
    } finally {
      mountNode.node_ops.lookup = originalLookup;
      FS.close(s);
    }
  });

  it("does not corrupt backend state when target lookup fails", async () => {
    const { FS, tomefs } = await mountTome(backend);

    const data = new Uint8Array(PAGE_SIZE + 100);
    data.fill(0xab);
    const s = FS.open(`${MOUNT}/src`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, data, 0, data.length);
    FS.close(s);

    tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
      if (err) throw err;
    });

    const metaBefore = backend.readMeta("/src");
    expect(metaBefore).not.toBeNull();

    const mountNode = FS.lookupPath(MOUNT).node;
    const originalLookup = mountNode.node_ops.lookup;

    mountNode.node_ops.lookup = (parent: any, name: string) => {
      if (name === "dst") throw new FS.ErrnoError(EACCES);
      return originalLookup(parent, name);
    };

    try {
      expect(() => FS.rename(`${MOUNT}/src`, `${MOUNT}/dst`)).toThrow();
    } finally {
      mountNode.node_ops.lookup = originalLookup;
    }

    const metaAfter = backend.readMeta("/src");
    expect(metaAfter).not.toBeNull();
    expect(metaAfter!.size).toBe(metaBefore!.size);
    expect(backend.readMeta("/dst")).toBeNull();
  });

  it("propagates errors for directory rename target lookup", async () => {
    const { FS } = await mountTome(backend);

    FS.mkdir(`${MOUNT}/srcdir`);
    const s = FS.open(`${MOUNT}/srcdir/file`, O.RDWR | O.CREAT, 0o666);
    FS.write(s, encode("child"), 0, 5);
    FS.close(s);

    const mountNode = FS.lookupPath(MOUNT).node;
    const originalLookup = mountNode.node_ops.lookup;

    mountNode.node_ops.lookup = (parent: any, name: string) => {
      if (name === "dstdir") throw new FS.ErrnoError(EACCES);
      return originalLookup(parent, name);
    };

    try {
      expect(() => FS.rename(`${MOUNT}/srcdir`, `${MOUNT}/dstdir`)).toThrow();
      expect(FS.readdir(`${MOUNT}/srcdir`)).toContain("file");
    } finally {
      mountNode.node_ops.lookup = originalLookup;
    }
  });
});
