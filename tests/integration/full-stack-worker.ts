/**
 * Worker script for full-stack integration tests.
 *
 * Runs in a Worker thread because Atomics.wait() cannot be called from the
 * main thread. Creates a SabClient as the SyncStorageBackend, wires it into
 * createTomeFS, and executes FS operations requested by the test harness.
 *
 * This file is bundled by esbuild before being spawned as a Worker.
 */
import { parentPort, workerData } from "node:worker_threads";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { SabClient } from "../../src/sab-client.js";
import { createTomeFS } from "../../src/tomefs.js";
import { PAGE_SIZE } from "../../src/types.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

const sab: SharedArrayBuffer = workerData.sab;
const client = new SabClient(sab);

// We need to load the Emscripten module, mount tomefs, and run operations.
let FS: any;
let tomefs: any;

const MOUNT = "/data";

async function init(): Promise<void> {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  const Module = await createModule();
  FS = Module.FS;

  tomefs = createTomeFS(FS, { backend: client, maxPages: workerData.maxPages ?? 64 });
  FS.mkdir(MOUNT);
  FS.mount(tomefs, {}, MOUNT);
}

/** Rewrite path to mount point. */
function mp(path: string): string {
  if (path.startsWith("/")) return MOUNT + path;
  return path;
}

/** Execute a test command and return the result. */
function exec(cmd: string, args: any[]): any {
  switch (cmd) {
    case "writeFile": {
      const [path, content] = args;
      const data = new TextEncoder().encode(content);
      const stream = FS.open(mp(path), 64 | 1 | 512, 0o666); // O_CREAT | O_WRONLY | O_TRUNC
      FS.write(stream, data, 0, data.length, 0);
      FS.close(stream);
      return null;
    }
    case "readFile": {
      const [path] = args;
      const stat = FS.stat(mp(path));
      const stream = FS.open(mp(path), 0, 0); // O_RDONLY
      const buf = new Uint8Array(stat.size);
      FS.read(stream, buf, 0, stat.size, 0);
      FS.close(stream);
      return new TextDecoder().decode(buf);
    }
    case "writeBytes": {
      const [path, bytes, position] = args;
      const data = new Uint8Array(bytes);
      const flags = position !== undefined ? (64 | 2) : (64 | 1 | 512); // O_CREAT|O_RDWR or O_CREAT|O_WRONLY|O_TRUNC
      const stream = FS.open(mp(path), flags, 0o666);
      FS.write(stream, data, 0, data.length, position ?? 0);
      FS.close(stream);
      return null;
    }
    case "readBytes": {
      const [path, offset, length] = args;
      const stream = FS.open(mp(path), 0, 0); // O_RDONLY
      const buf = new Uint8Array(length);
      const n = FS.read(stream, buf, 0, length, offset ?? 0);
      FS.close(stream);
      return Array.from(buf.subarray(0, n));
    }
    case "stat": {
      const [path] = args;
      const s = FS.stat(mp(path));
      return { size: s.size, mode: s.mode };
    }
    case "mkdir": {
      const [path] = args;
      FS.mkdir(mp(path));
      return null;
    }
    case "readdir": {
      const [path] = args;
      return FS.readdir(mp(path));
    }
    case "unlink": {
      const [path] = args;
      FS.unlink(mp(path));
      return null;
    }
    case "rename": {
      const [oldPath, newPath] = args;
      FS.rename(mp(oldPath), mp(newPath));
      return null;
    }
    case "truncate": {
      const [path, len] = args;
      FS.truncate(mp(path), len);
      return null;
    }
    case "syncfs": {
      return new Promise<void>((resolve, reject) => {
        FS.syncfs(false, (err: Error | null) => {
          if (err) reject(err);
          else resolve();
        });
      });
    }
    case "writeMultiPage": {
      // Write a file that spans multiple pages
      const [path, numPages, pattern] = args;
      const totalSize = numPages * PAGE_SIZE;
      const data = new Uint8Array(totalSize);
      for (let i = 0; i < totalSize; i++) {
        data[i] = (i + (pattern ?? 0)) & 0xff;
      }
      const stream = FS.open(mp(path), 64 | 1 | 512, 0o666);
      FS.write(stream, data, 0, totalSize, 0);
      FS.close(stream);
      return null;
    }
    case "verifyMultiPage": {
      // Read and verify a multi-page file
      const [path, numPages, pattern] = args;
      const totalSize = numPages * PAGE_SIZE;
      const stream = FS.open(mp(path), 0, 0);
      const buf = new Uint8Array(totalSize);
      const n = FS.read(stream, buf, 0, totalSize, 0);
      FS.close(stream);
      if (n !== totalSize) return { ok: false, error: `read ${n} bytes, expected ${totalSize}` };
      for (let i = 0; i < totalSize; i++) {
        const expected = (i + (pattern ?? 0)) & 0xff;
        if (buf[i] !== expected) {
          return { ok: false, error: `byte ${i}: got ${buf[i]}, expected ${expected}` };
        }
      }
      return { ok: true };
    }
    case "cacheStats": {
      return {
        size: tomefs.pageCache.size,
        capacity: tomefs.pageCache.capacity,
        dirtyCount: tomefs.pageCache.dirtyCount,
      };
    }
    default:
      throw new Error(`Unknown command: ${cmd}`);
  }
}

// Message handler
parentPort!.on("message", async (msg: { cmd: string; args: any[]; id: number }) => {
  try {
    const result = await exec(msg.cmd, msg.args);
    parentPort!.postMessage({ id: msg.id, result });
  } catch (err: unknown) {
    const errMsg = err instanceof Error ? err.message : String(err);
    parentPort!.postMessage({ id: msg.id, error: errMsg });
  }
});

// Initialize and signal ready
init().then(() => {
  parentPort!.postMessage({ ready: true });
}).catch((err) => {
  parentPort!.postMessage({ error: `Init failed: ${err.message}` });
});
