/**
 * Worker entry point for the SAB storage worker.
 *
 * In production, the storage worker runs in a dedicated Web Worker and handles
 * async IDB operations. The PGlite/Emscripten worker communicates with it via
 * SharedArrayBuffer + Atomics (see SabClient).
 *
 * Usage (in your worker script):
 *   import { SabWorker, IdbBackend } from 'tomefs/worker';
 *
 *   self.onmessage = async (e) => {
 *     const { sab } = e.data;
 *     const backend = new IdbBackend({ dbName: 'mydb' });
 *     const worker = new SabWorker(sab, backend);
 *     await worker.start();
 *   };
 */

export { SabWorker } from "./sab-worker.js";
export { IdbBackend } from "./idb-backend.js";
export type { IdbBackendOptions } from "./idb-backend.js";
export { OpfsBackend } from "./opfs-backend.js";
export type { OpfsBackendOptions } from "./opfs-backend.js";
export type { StorageBackend } from "./storage-backend.js";
export type { FileMeta } from "./types.js";
export { PAGE_SIZE } from "./types.js";
