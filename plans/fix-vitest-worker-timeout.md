# Fix: vitest worker timeout on `onTaskUpdate`

## Problem

`npm test` exits with code 1 despite all 2308 tests passing. The error:

```
Error: [vitest-worker]: Timeout calling "onTaskUpdate"
 at Object.onTimeoutError node_modules/vitest/dist/chunks/rpc.-pEldfrD.js:53:10
```

This is vitest's internal birpc (bidirectional RPC) timing out after 60 seconds
when the worker process tries to report results back to the main process.

## Root Cause

`SabWorker.start()` returns a `Promise<void>` that resolves when `stop()` is
called. Multiple test files call `sabWorker.start()` without capturing or
awaiting the returned promise. When `stop()` is called in `afterEach`, it sets
`this.running = false` and resolves the internal stop promise, which should
cause the `start()` promise to settle — but the promise is never awaited by the
test, leaving it dangling on the microtask queue.

This prevents the Node.js worker thread from cleanly exiting after all tests
complete. Vitest's worker then can't respond to the main process's
`onTaskUpdate` RPC within the 60-second birpc timeout, triggering the unhandled
error.

### How `SabWorker.start()` works (src/sab-worker.ts:56-88)

```typescript
async start(): Promise<void> {
  this.running = true;
  this.stopPromise = new Promise<void>((resolve) => {
    this.stopResolve = resolve;
  });
  while (this.running) {
    // ...Atomics.waitAsync + Promise.race([waitResult.value, this.stopPromise])
  }
}
```

The `while (this.running)` loop exits when `stop()` sets `running = false`, but
the promise returned by `start()` is never consumed by tests that fire-and-forget it.

## Affected Files

All four files follow the same pattern — `sabWorker.start()` with no `await`:

| File | Line | Pattern |
|------|------|---------|
| `tests/integration/full-stack.test.ts` | 86 | `sabWorker.start();` in beforeEach |
| `tests/unit/sab-bridge.test.ts` | 86 | `sabWorker.start();` in beforeEach |
| `tests/unit/sab-bridge-chunking.test.ts` | 122 | `sabWorker.start();` in beforeEach |
| `tests/unit/sab-bridge-edge-cases.test.ts` | 401, 456, 527, 666, 799, 1020, 1165 | `sabWorker.start();` in individual tests |

One instance in `sab-bridge-edge-cases.test.ts` (line 1497) correctly captures
the promise: `const workerPromise = sabWorker.start();`.

## Fix

In each affected location, capture the promise from `start()` and await it
after calling `stop()`:

### For `beforeEach`/`afterEach` pattern (full-stack, sab-bridge, sab-bridge-chunking):

```typescript
let workerPromise: Promise<void>;

beforeEach(async () => {
  // ...
  workerPromise = sabWorker.start();
  // ...
});

afterEach(async () => {
  sabWorker.stop();
  await workerPromise;        // <-- ensure start() promise settles
  await clientWorker.terminate();
});
```

### For inline test pattern (sab-bridge-edge-cases):

```typescript
const workerPromise = sabWorker.start();
// ... test body ...
sabWorker.stop();
await workerPromise;           // <-- ensure start() promise settles
```

## Verification

After the fix, `npm test` should:
1. Still pass all 2308 tests
2. Exit with code 0 (no unhandled errors)
3. Complete faster (workers exit cleanly without waiting for timeout)

## Risk

Low. The fix only adds `await` calls on promises that already resolve once
`stop()` is called. No behavioral change to the SabWorker itself or any test
logic. The existing test at line 1497 of `sab-bridge-edge-cases.test.ts`
already uses this correct pattern.
