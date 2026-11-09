import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { createTomeFS } from "../../src/tomefs.js";
import type { SyncStorageBackend } from "../../src/sync-storage-backend.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

export const MOUNT = "/tome";

export async function mountTome(
  backend: SyncStorageBackend,
  maxPages?: number,
) {
  const { default: createModule } = await import(
    join(__dirname, "emscripten_fs.mjs")
  );
  const Module = await createModule();
  const FS = Module.FS;
  const tomefs = createTomeFS(FS, { backend, maxPages });
  FS.mkdir(MOUNT);
  FS.mount(tomefs, {}, MOUNT);
  return { FS, tomefs, Module };
}

export function syncfs(FS: any, tomefs: any, populate = false) {
  tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, populate, (err: any) => {
    if (err) throw err;
  });
}

export function trySyncfs(FS: any, tomefs: any): Error | null {
  let error: Error | null = null;
  tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
    error = err;
  });
  return error;
}

export function syncAndUnmount(FS: any, tomefs: any) {
  syncfs(FS, tomefs);
  FS.unmount(MOUNT);
}
