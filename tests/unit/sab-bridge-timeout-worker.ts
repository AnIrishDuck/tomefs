/**
 * Worker script that runs the SabClient with configurable timeout.
 *
 * Used by sab-bridge-edge-cases.test.ts to test timeout, buffer overflow,
 * and error recovery scenarios.
 */
import { parentPort, workerData } from "node:worker_threads";
import { SabClient } from "../../src/sab-client.js";
import { PAGE_SIZE } from "../../src/types.js";
import {
  encodeMessage,
  CONTROL_BYTES,
  JSON_REGION_OFFSET,
} from "../../src/sab-protocol.js";

const sab: SharedArrayBuffer = workerData.sab;
const timeout: number = workerData.timeout ?? 0;
const client = new SabClient(sab, { timeout });

parentPort!.on("message", (msg: { cmd: string; args: unknown[]; id: number }) => {
  try {
    if (msg.cmd === "encodeOverflow") {
      // Test encodeMessage overflow detection directly in the worker
      // (since it uses the same shared buffer views)
      const dataView = new DataView(sab);
      const uint8View = new Uint8Array(sab);
      const [numPages] = msg.args as [number];
      const chunks: Uint8Array[] = [];
      const meta: Array<{ path: string; pageIndex: number; dataLen: number }> = [];
      for (let i = 0; i < numPages; i++) {
        const page = new Uint8Array(PAGE_SIZE);
        page[0] = i & 0xff;
        chunks.push(page);
        meta.push({ path: "/overflow", pageIndex: i, dataLen: PAGE_SIZE });
      }
      // This should throw for large batches
      encodeMessage(dataView, uint8View, { pages: meta }, chunks);
      parentPort!.postMessage({ id: msg.id, result: "ok" });
      return;
    }

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
