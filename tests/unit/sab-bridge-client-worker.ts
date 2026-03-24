/**
 * Worker script that runs the SabClient (synchronous side) in a Worker thread.
 *
 * Atomics.wait() cannot be called from the main thread in browsers, and this
 * test validates the real cross-thread SAB communication pattern.
 *
 * This file is bundled by esbuild before being spawned as a Worker.
 */
import { parentPort, workerData } from "node:worker_threads";
import { SabClient } from "../../src/sab-client.js";

const sab: SharedArrayBuffer = workerData.sab;
const client = new SabClient(sab);

parentPort!.on("message", (msg: { cmd: string; args: unknown[]; id: number }) => {
  try {
    const method = (client as Record<string, Function>)[msg.cmd];
    if (!method) throw new Error(`Unknown method: ${msg.cmd}`);
    const result = method.apply(client, msg.args);
    parentPort!.postMessage({ id: msg.id, result });
  } catch (err: unknown) {
    const errMsg = err instanceof Error ? err.message : String(err);
    parentPort!.postMessage({ id: msg.id, error: errMsg });
  }
});

// Signal ready
parentPort!.postMessage({ ready: true });
