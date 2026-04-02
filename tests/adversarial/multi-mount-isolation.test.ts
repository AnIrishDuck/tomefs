/**
 * Adversarial test: Multi-mount path isolation.
 *
 * Validates that two independent tomefs mounts on the same Emscripten FS
 * do not share state. Previously, `nextPathId` was module-global, meaning
 * unlinked-file markers (/__deleted_*) and root paths (/__root_*) could
 * collide across mounts.
 */
import { describe, it, expect } from "vitest";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { createTomeFS } from "../../src/tomefs.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { O, encode, decode } from "../harness/emscripten-fs.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

async function loadEmscriptenFS() {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  return (await createModule()).FS;
}

describe("adversarial: multi-mount isolation", () => {
  it("two tomefs mounts on the same FS have independent storage", async () => {
    const rawFS = await loadEmscriptenFS();

    const backendA = new SyncMemoryBackend();
    const backendB = new SyncMemoryBackend();
    const tomeA = createTomeFS(rawFS, { backend: backendA, maxPages: 32 });
    const tomeB = createTomeFS(rawFS, { backend: backendB, maxPages: 32 });

    rawFS.mkdir("/mountA");
    rawFS.mkdir("/mountB");
    rawFS.mount(tomeA, {}, "/mountA");
    rawFS.mount(tomeB, {}, "/mountB");

    // Write different data to the same relative path on each mount
    rawFS.writeFile("/mountA/test.txt", "data from mount A");
    rawFS.writeFile("/mountB/test.txt", "data from mount B");

    // Each mount should see its own data
    const dataA = rawFS.readFile("/mountA/test.txt", { encoding: "utf8" });
    const dataB = rawFS.readFile("/mountB/test.txt", { encoding: "utf8" });
    expect(dataA).toBe("data from mount A");
    expect(dataB).toBe("data from mount B");

    // Flush both mounts to their backends
    await new Promise<void>((resolve, reject) => {
      rawFS.syncfs(false, (err: Error | null) => (err ? reject(err) : resolve()));
    });

    // Backend storage should be independent — each has its own /test.txt
    const metaA = backendA.readMeta("/test.txt");
    const metaB = backendB.readMeta("/test.txt");
    expect(metaA).not.toBeNull();
    expect(metaB).not.toBeNull();
    expect(metaA!.size).toBe(17); // "data from mount A"
    expect(metaB!.size).toBe(17); // "data from mount B"
  });

  it("unlink markers do not collide across mounts", async () => {
    const rawFS = await loadEmscriptenFS();

    const backendA = new SyncMemoryBackend();
    const backendB = new SyncMemoryBackend();
    const tomeA = createTomeFS(rawFS, { backend: backendA, maxPages: 32 });
    const tomeB = createTomeFS(rawFS, { backend: backendB, maxPages: 32 });

    rawFS.mkdir("/mntA");
    rawFS.mkdir("/mntB");
    rawFS.mount(tomeA, {}, "/mntA");
    rawFS.mount(tomeB, {}, "/mntB");

    // Create files, open them, then unlink while open (creates /__deleted_* markers)
    rawFS.writeFile("/mntA/ephemeral.txt", "aaa");
    rawFS.writeFile("/mntB/ephemeral.txt", "bbb");

    const streamA = rawFS.open("/mntA/ephemeral.txt", O.RDONLY);
    const streamB = rawFS.open("/mntB/ephemeral.txt", O.RDONLY);

    rawFS.unlink("/mntA/ephemeral.txt");
    rawFS.unlink("/mntB/ephemeral.txt");

    // Both files should still be readable through their open fds
    const bufA = new Uint8Array(3);
    const bufB = new Uint8Array(3);
    rawFS.read(streamA, bufA, 0, 3);
    rawFS.read(streamB, bufB, 0, 3);

    expect(decode(bufA, 3)).toBe("aaa");
    expect(decode(bufB, 3)).toBe("bbb");

    rawFS.close(streamA);
    rawFS.close(streamB);
  });

  it("syncfs on one mount does not affect the other", async () => {
    const rawFS = await loadEmscriptenFS();

    const backendA = new SyncMemoryBackend();
    const backendB = new SyncMemoryBackend();
    const tomeA = createTomeFS(rawFS, { backend: backendA, maxPages: 32 });
    const tomeB = createTomeFS(rawFS, { backend: backendB, maxPages: 32 });

    rawFS.mkdir("/sA");
    rawFS.mkdir("/sB");
    rawFS.mount(tomeA, {}, "/sA");
    rawFS.mount(tomeB, {}, "/sB");

    // Write data to both mounts
    rawFS.writeFile("/sA/file.dat", encode("mount-a-content"));
    rawFS.writeFile("/sB/file.dat", encode("mount-b-content"));

    // Sync only mount A
    await new Promise<void>((resolve, reject) => {
      rawFS.syncfs(false, (err: Error | null) => (err ? reject(err) : resolve()));
    });

    // Both should still be readable
    const a = rawFS.readFile("/sA/file.dat", { encoding: "utf8" });
    const b = rawFS.readFile("/sB/file.dat", { encoding: "utf8" });
    expect(a).toBe("mount-a-content");
    expect(b).toBe("mount-b-content");
  });
});
