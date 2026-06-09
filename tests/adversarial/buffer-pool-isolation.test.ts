import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { describe, it, expect, beforeAll, afterEach } from "vitest";
import { createTomeFS } from "../../src/tomefs.js";
import { SyncMemoryBackend } from "../../src/sync-memory-backend.js";
import { PAGE_SIZE } from "../../src/types.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

const O = {
  RDONLY: 0,
  WRONLY: 1,
  RDWR: 2,
  CREAT: 64,
  TRUNC: 512,
} as const;

const MOUNT = "/tome";

let FS: any;
let mounted = false;

beforeAll(async () => {
  const { default: createModule } = await import(
    join(__dirname, "../harness/emscripten_fs.mjs")
  );
  const Module = await createModule();
  FS = Module.FS;
  FS.mkdir(MOUNT);
});

function mountTome(backend: SyncMemoryBackend, maxPages = 4) {
  const tomefs = createTomeFS(FS, { backend, maxPages });
  FS.mount(tomefs, {}, MOUNT);
  mounted = true;
  return tomefs;
}

function unmountTome() {
  if (mounted) {
    FS.unmount(MOUNT);
    mounted = false;
  }
}

afterEach(() => {
  unmountTome();
});

function syncfs(tomefs: any): void {
  tomefs.syncfs(FS.lookupPath(MOUNT).node.mount, false, (err: any) => {
    if (err) throw err;
  });
}

describe("adversarial: buffer pool data isolation", () => {
  it("evicted page data does not leak into new file via buffer pool @fast", async () => {
    const backend = new SyncMemoryBackend();
    const tomefs = mountTome(backend, 4);

    const poison = new Uint8Array(PAGE_SIZE);
    poison.fill(0xff);
    const sA = FS.open(`${MOUNT}/fileA`, O.RDWR | O.CREAT, 0o666);
    for (let p = 0; p < 4; p++) {
      FS.write(sA, poison, 0, PAGE_SIZE, p * PAGE_SIZE);
    }
    FS.close(sA);
    syncfs(tomefs);

    const fillerB = new Uint8Array(PAGE_SIZE);
    fillerB.fill(0xaa);
    const sB = FS.open(`${MOUNT}/fileB`, O.RDWR | O.CREAT, 0o666);
    for (let p = 0; p < 4; p++) {
      FS.write(sB, fillerB, 0, PAGE_SIZE, p * PAGE_SIZE);
    }
    FS.close(sB);

    const sC = FS.open(`${MOUNT}/fileC`, O.RDWR | O.CREAT, 0o666);
    FS.ftruncate(sC.fd, PAGE_SIZE * 2);

    const readBuf = new Uint8Array(PAGE_SIZE * 2);
    FS.read(sC, readBuf, 0, PAGE_SIZE * 2, 0);
    FS.close(sC);

    for (let i = 0; i < readBuf.length; i++) {
      if (readBuf[i] !== 0) {
        throw new Error(
          `Data leak at byte ${i}: expected 0x00, got 0x${readBuf[i].toString(16).padStart(2, "0")} ` +
            `(likely stale data from evicted page buffer pool)`,
        );
      }
    }
  });

  it("buffer reuse after eviction cycle does not corrupt new page writes @fast", async () => {
    const backend = new SyncMemoryBackend();
    const tomefs = mountTome(backend, 2);

    const sA = FS.open(`${MOUNT}/a`, O.RDWR | O.CREAT, 0o666);
    const patA0 = new Uint8Array(PAGE_SIZE);
    patA0.fill(0xde);
    const patA1 = new Uint8Array(PAGE_SIZE);
    patA1.fill(0xad);
    FS.write(sA, patA0, 0, PAGE_SIZE, 0);
    FS.write(sA, patA1, 0, PAGE_SIZE, PAGE_SIZE);
    FS.close(sA);
    syncfs(tomefs);

    const sB = FS.open(`${MOUNT}/b`, O.RDWR | O.CREAT, 0o666);
    const patB0 = new Uint8Array(PAGE_SIZE);
    patB0.fill(0x11);
    const patB1 = new Uint8Array(PAGE_SIZE);
    patB1.fill(0x22);
    FS.write(sB, patB0, 0, PAGE_SIZE, 0);
    FS.write(sB, patB1, 0, PAGE_SIZE, PAGE_SIZE);
    FS.close(sB);

    const rA = FS.open(`${MOUNT}/a`, O.RDONLY);
    const readBuf0 = new Uint8Array(PAGE_SIZE);
    FS.read(rA, readBuf0, 0, PAGE_SIZE, 0);
    expect(readBuf0).toEqual(patA0);

    const readBuf1 = new Uint8Array(PAGE_SIZE);
    FS.read(rA, readBuf1, 0, PAGE_SIZE, PAGE_SIZE);
    expect(readBuf1).toEqual(patA1);
    FS.close(rA);
  });

  it("getPageNoRead returns zeroed buffer from pool after eviction @fast", async () => {
    const backend = new SyncMemoryBackend();
    const tomefs = mountTome(backend, 2);

    const poison = new Uint8Array(PAGE_SIZE);
    poison.fill(0xbe);
    const sA = FS.open(`${MOUNT}/poison`, O.RDWR | O.CREAT, 0o666);
    FS.write(sA, poison, 0, PAGE_SIZE, 0);
    FS.write(sA, poison, 0, PAGE_SIZE, PAGE_SIZE);
    FS.close(sA);
    syncfs(tomefs);

    const evict = new Uint8Array(PAGE_SIZE);
    evict.fill(0x01);
    const sE = FS.open(`${MOUNT}/evict`, O.RDWR | O.CREAT, 0o666);
    FS.write(sE, evict, 0, PAGE_SIZE, 0);
    FS.write(sE, evict, 0, PAGE_SIZE, PAGE_SIZE);
    FS.close(sE);

    const sNew = FS.open(`${MOUNT}/newfile`, O.RDWR | O.CREAT, 0o666);
    const smallWrite = new Uint8Array(100);
    smallWrite.fill(0x42);
    FS.write(sNew, smallWrite, 0, 100, 0);

    const fullPage = new Uint8Array(PAGE_SIZE);
    FS.read(sNew, fullPage, 0, PAGE_SIZE, 0);
    FS.close(sNew);

    for (let i = 0; i < 100; i++) {
      expect(fullPage[i]).toBe(0x42);
    }
    for (let i = 100; i < PAGE_SIZE; i++) {
      if (fullPage[i] !== 0) {
        throw new Error(
          `Buffer pool leak at byte ${i}: expected 0x00, got 0x${fullPage[i].toString(16).padStart(2, "0")}`,
        );
      }
    }
  });

  it("multiple eviction-reuse cycles maintain isolation @fast", async () => {
    const backend = new SyncMemoryBackend();
    const tomefs = mountTome(backend, 2);

    for (let cycle = 0; cycle < 10; cycle++) {
      const marker = (cycle * 0x17 + 0x30) & 0xff;
      const path = `${MOUNT}/cycle_${cycle}`;

      const poison = new Uint8Array(PAGE_SIZE);
      poison.fill(marker);
      const sW = FS.open(path, O.RDWR | O.CREAT, 0o666);
      FS.write(sW, poison, 0, PAGE_SIZE, 0);
      FS.write(sW, poison, 0, PAGE_SIZE, PAGE_SIZE);
      FS.close(sW);
      syncfs(tomefs);

      const evictBuf = new Uint8Array(PAGE_SIZE);
      evictBuf.fill(0x00);
      const sE = FS.open(
        `${MOUNT}/evict_${cycle}`,
        O.RDWR | O.CREAT,
        0o666,
      );
      FS.write(sE, evictBuf, 0, PAGE_SIZE, 0);
      FS.write(sE, evictBuf, 0, PAGE_SIZE, PAGE_SIZE);
      FS.close(sE);

      const newPath = `${MOUNT}/new_${cycle}`;
      const sN = FS.open(newPath, O.RDWR | O.CREAT, 0o666);
      FS.ftruncate(sN.fd, PAGE_SIZE);
      const readBuf = new Uint8Array(PAGE_SIZE);
      FS.read(sN, readBuf, 0, PAGE_SIZE, 0);
      FS.close(sN);

      for (let i = 0; i < PAGE_SIZE; i++) {
        if (readBuf[i] !== 0) {
          throw new Error(
            `Cycle ${cycle}: buffer pool leak at byte ${i}: ` +
              `expected 0x00, got 0x${readBuf[i].toString(16).padStart(2, "0")} ` +
              `(poison marker was 0x${marker.toString(16).padStart(2, "0")})`,
          );
        }
      }
    }
  });

  it("sparse file extension via write beyond EOF uses clean buffers @fast", async () => {
    const backend = new SyncMemoryBackend();
    const tomefs = mountTome(backend, 2);

    const poison = new Uint8Array(PAGE_SIZE);
    poison.fill(0xcc);
    const sP = FS.open(`${MOUNT}/poison`, O.RDWR | O.CREAT, 0o666);
    FS.write(sP, poison, 0, PAGE_SIZE, 0);
    FS.write(sP, poison, 0, PAGE_SIZE, PAGE_SIZE);
    FS.close(sP);
    syncfs(tomefs);

    const sE = FS.open(`${MOUNT}/evict`, O.RDWR | O.CREAT, 0o666);
    const zero = new Uint8Array(PAGE_SIZE);
    FS.write(sE, zero, 0, PAGE_SIZE, 0);
    FS.write(sE, zero, 0, PAGE_SIZE, PAGE_SIZE);
    FS.close(sE);

    const sNew = FS.open(`${MOUNT}/sparse`, O.RDWR | O.CREAT, 0o666);
    const data = new Uint8Array(100);
    data.fill(0x99);
    FS.write(sNew, data, 0, 100, 2 * PAGE_SIZE);

    const hole = new Uint8Array(2 * PAGE_SIZE);
    FS.read(sNew, hole, 0, 2 * PAGE_SIZE, 0);
    FS.close(sNew);

    for (let i = 0; i < hole.length; i++) {
      if (hole[i] !== 0) {
        throw new Error(
          `Sparse hole leak at byte ${i}: expected 0x00, got 0x${hole[i].toString(16).padStart(2, "0")}`,
        );
      }
    }
  });

  it("buffer pool isolation survives persistence round-trip @fast", async () => {
    const backend = new SyncMemoryBackend();

    // Session 1: fill backend with poison data, sync
    {
      const tomefs = mountTome(backend, 4);
      const poison = new Uint8Array(PAGE_SIZE);
      poison.fill(0xee);
      const s = FS.open(`${MOUNT}/persistent`, O.RDWR | O.CREAT, 0o666);
      for (let p = 0; p < 4; p++) {
        FS.write(s, poison, 0, PAGE_SIZE, p * PAGE_SIZE);
      }
      FS.close(s);
      syncfs(tomefs);
      unmountTome();
    }

    // Session 2: remount, evict persistent file's pages, create new file
    {
      mountTome(backend, 4);

      const sP = FS.open(`${MOUNT}/persistent`, O.RDONLY);
      const dummy = new Uint8Array(PAGE_SIZE * 4);
      FS.read(sP, dummy, 0, PAGE_SIZE * 4, 0);
      FS.close(sP);

      const evictBuf = new Uint8Array(PAGE_SIZE);
      evictBuf.fill(0x01);
      const sE = FS.open(`${MOUNT}/evictor`, O.RDWR | O.CREAT, 0o666);
      for (let p = 0; p < 4; p++) {
        FS.write(sE, evictBuf, 0, PAGE_SIZE, p * PAGE_SIZE);
      }
      FS.close(sE);

      const sNew = FS.open(`${MOUNT}/clean`, O.RDWR | O.CREAT, 0o666);
      FS.ftruncate(sNew.fd, PAGE_SIZE * 2);
      const readBuf = new Uint8Array(PAGE_SIZE * 2);
      FS.read(sNew, readBuf, 0, PAGE_SIZE * 2, 0);
      FS.close(sNew);

      for (let i = 0; i < readBuf.length; i++) {
        if (readBuf[i] !== 0) {
          throw new Error(
            `Post-remount buffer pool leak at byte ${i}: ` +
              `expected 0x00, got 0x${readBuf[i].toString(16).padStart(2, "0")}`,
          );
        }
      }
    }
  });
});
