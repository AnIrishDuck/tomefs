/**
 * BadFS Validation: Verifies that each injected defect is caught by the
 * conformance test suite, proving each test has real discriminating power.
 *
 * For each defect, we run the same test logic as the conformance suite but
 * against a BadFS wrapper. The defect should cause specific tests to fail
 * while other tests continue to pass.
 *
 * See plans/conformance-test-plan.md § BadFS Validation.
 */
import { createBadFS, type DefectId } from "../harness/bad-fs.js";
import {
  encode,
  decode,
  expectErrno,
  O,
  SEEK_SET,
  SEEK_CUR,
  SEEK_END,
  S_IFMT,
  S_IFREG,
  S_IFDIR,
  S_IFLNK,
  S_IRWXUGO,
  type FSHarness,
} from "../harness/emscripten-fs.js";

// Helper: run a test function and return whether it passed or threw
async function doesPass(fn: () => void | Promise<void>): Promise<boolean> {
  try {
    await fn();
    return true;
  } catch {
    return false;
  }
}

describe("BadFS: off-by-one-read", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createBadFS("off-by-one-read");
  });

  it("FAILS: read returns wrong byte count", () => {
    const { FS } = h;
    const stream = FS.open("/test", O.RDWR | O.CREAT, 0o777);
    const msg = encode("Test\n");
    FS.write(stream, msg, 0, msg.length);
    const stream2 = FS.open("/test", O.RDWR);
    const buf = new Uint8Array(100);
    const n = FS.read(stream2, buf, 0, buf.length);
    // This SHOULD be 5 but the defect returns 4
    expect(n).not.toBe(5);
    expect(n).toBe(4);
    FS.close(stream);
    FS.close(stream2);
  });

  it("PASSES: writes still work correctly", () => {
    const { FS } = h;
    const stream = FS.open("/test", O.RDWR | O.CREAT, 0o777);
    const msg = encode("hello");
    const n = FS.write(stream, msg, 0, msg.length);
    expect(n).toBe(5);
    FS.close(stream);
  });

  it("PASSES: stat still works correctly", () => {
    const { FS } = h;
    FS.writeFile("/test", "hello");
    const stat = FS.stat("/test");
    expect(stat.mode & S_IFMT).toBe(S_IFREG);
  });

  it("PASSES: mkdir still works correctly", () => {
    const { FS } = h;
    FS.mkdir("/testdir", 0o777);
    const stat = FS.stat("/testdir");
    expect(stat.mode & S_IFMT).toBe(S_IFDIR);
  });
});

describe("BadFS: no-mtime-update", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createBadFS("no-mtime-update");
  });

  it("FAILS: mtime does not change after write", async () => {
    const { FS } = h;
    const stream = FS.open("/testfile", O.RDWR | O.CREAT, 0o777);

    const statBefore = FS.stat("/testfile");
    const mtimeBefore = statBefore.mtime.getTime();

    // Wait a tiny bit to ensure timestamp would differ
    await new Promise((r) => setTimeout(r, 10));

    // Write data
    FS.write(stream, encode("data"), 0, 4);

    const statAfter = FS.stat("/testfile");
    const mtimeAfter = statAfter.mtime.getTime();

    // The defect: mtime should have changed but didn't
    expect(mtimeAfter).toBe(mtimeBefore);
    FS.close(stream);
  });

  it("PASSES: read still works", () => {
    const { FS } = h;
    FS.writeFile("/test", "hello");
    const data = FS.readFile("/test", {}) as Uint8Array;
    expect(decode(data)).toBe("hello");
  });

  it("PASSES: mkdir still works", () => {
    const { FS } = h;
    FS.mkdir("/testdir", 0o777);
    const entries = FS.readdir("/testdir");
    expect(entries).toContain(".");
    expect(entries).toContain("..");
  });
});

describe("BadFS: rename-no-overwrite", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createBadFS("rename-no-overwrite");
  });

  it("FAILS: rename over existing file keeps old contents", () => {
    const { FS } = h;
    // Create file "a" with "abc"
    const s1 = FS.open("/a", O.WRONLY | O.CREAT | O.EXCL, 0o666);
    FS.write(s1, encode("abc"), 0, 3);
    FS.close(s1);

    // Create file "b" with "xyz"
    const s2 = FS.open("/b", O.WRONLY | O.CREAT | O.EXCL, 0o666);
    FS.write(s2, encode("xyz"), 0, 3);
    FS.close(s2);

    // rename("a", "b") — defect: keeps b's old contents
    FS.rename("/a", "/b");

    const data = FS.readFile("/b", {}) as Uint8Array;
    // Should be "abc" (from /a) but defect keeps "xyz" (old /b)
    expect(decode(data)).toBe("xyz");
    expect(decode(data)).not.toBe("abc");
  });

  it("PASSES: rename to non-existing target works normally", () => {
    const { FS } = h;
    const s = FS.open("/src", O.WRONLY | O.CREAT, 0o666);
    FS.write(s, encode("hello"), 0, 5);
    FS.close(s);

    FS.rename("/src", "/dst");

    const data = FS.readFile("/dst", {}) as Uint8Array;
    expect(decode(data)).toBe("hello");
    expectErrno(() => FS.stat("/src"), h.E.ENOENT);
  });

  it("PASSES: basic I/O unaffected", () => {
    const { FS } = h;
    const stream = FS.open("/test", O.RDWR | O.CREAT, 0o777);
    FS.write(stream, encode("test"), 0, 4);
    FS.llseek(stream, 0, SEEK_SET);
    const buf = new Uint8Array(10);
    const n = FS.read(stream, buf, 0, 10);
    expect(n).toBe(4);
    expect(decode(buf, n)).toBe("test");
    FS.close(stream);
  });
});

describe("BadFS: readdir-missing-dot", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createBadFS("readdir-missing-dot");
  });

  it("FAILS: readdir omits . and .. entries", () => {
    const { FS } = h;
    FS.mkdir("/testdir", 0o777);
    const entries = FS.readdir("/testdir");
    expect(entries).not.toContain(".");
    expect(entries).not.toContain("..");
    expect(entries.length).toBe(0);
  });

  it("PASSES: regular file entries still appear", () => {
    const { FS } = h;
    FS.mkdir("/testdir", 0o777);
    const s = FS.open("/testdir/file.txt", O.WRONLY | O.CREAT, 0o666);
    FS.close(s);
    const entries = FS.readdir("/testdir");
    expect(entries).toContain("file.txt");
  });

  it("PASSES: file I/O unaffected", () => {
    const { FS } = h;
    FS.writeFile("/test", "hello");
    const data = FS.readFile("/test", {}) as Uint8Array;
    expect(decode(data)).toBe("hello");
  });

  it("PASSES: stat still works", () => {
    const { FS } = h;
    FS.mkdir("/testdir", 0o777);
    const stat = FS.stat("/testdir");
    expect(stat.mode & S_IFMT).toBe(S_IFDIR);
  });
});

describe("BadFS: symlink-no-resolve", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createBadFS("symlink-no-resolve");
  });

  it("FAILS: open through symlink fails because target is broken", () => {
    const { FS } = h;
    FS.writeFile("/target", "hello");
    FS.symlink("/target", "/link");

    // The defect: symlink points to broken path, so open through it fails
    expect(() => {
      FS.open("/link", O.RDONLY);
    }).toThrow();
  });

  it("FAILS: stat through symlink fails because target is broken", () => {
    const { FS } = h;
    FS.writeFile("/target", "hello");
    FS.symlink("/target", "/link");

    // stat follows symlink, which points to broken path
    expect(() => {
      FS.stat("/link");
    }).toThrow();
  });

  it("PASSES: readlink returns the expected target (looks correct)", () => {
    const { FS } = h;
    FS.writeFile("/target", "hello");
    FS.symlink("/target", "/link");
    // readlink is intercepted to return the "correct" value
    expect(FS.readlink("/link")).toBe("/target");
  });

  it("PASSES: non-symlink operations work normally", () => {
    const { FS } = h;
    FS.writeFile("/test", "hello");
    const stream = FS.open("/test", O.RDONLY);
    const buf = new Uint8Array(10);
    const n = FS.read(stream, buf, 0, 10);
    expect(n).toBe(5);
    expect(decode(buf, n)).toBe("hello");
    FS.close(stream);
  });

  it("PASSES: lstat still returns link info", () => {
    const { FS } = h;
    FS.writeFile("/target", "hello");
    FS.symlink("/target", "/link");
    const lstat = FS.lstat("/link");
    expect(lstat.mode & S_IFMT).toBe(S_IFLNK);
  });
});

describe("BadFS: wrong-errno-enoent", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createBadFS("wrong-errno-enoent");
  });

  it("FAILS: stat on non-existent path throws EACCES instead of ENOENT", () => {
    const { FS, E } = h;
    try {
      FS.stat("/nonexistent");
      throw new Error("should have thrown");
    } catch (e: unknown) {
      const err = e as { errno: number };
      // Defect: EACCES instead of ENOENT
      expect(err.errno).toBe(E.EACCES);
      expect(err.errno).not.toBe(E.ENOENT);
    }
  });

  it("FAILS: unlink non-existent throws EACCES instead of ENOENT", () => {
    const { FS, E } = h;
    try {
      FS.unlink("/nonexistent");
      throw new Error("should have thrown");
    } catch (e: unknown) {
      const err = e as { errno: number };
      expect(err.errno).toBe(E.EACCES);
    }
  });

  it("PASSES: operations on existing files work normally", () => {
    const { FS } = h;
    FS.writeFile("/test", "hello");
    const stat = FS.stat("/test");
    expect(stat.mode & S_IFMT).toBe(S_IFREG);
    expect(stat.size).toBe(5);
  });

  it("PASSES: mkdir creates directories", () => {
    const { FS } = h;
    FS.mkdir("/testdir", 0o777);
    const stat = FS.stat("/testdir");
    expect(stat.mode & S_IFMT).toBe(S_IFDIR);
  });
});

describe("BadFS: seek-end-off-by-one", () => {
  let h: FSHarness;

  beforeEach(async () => {
    h = await createBadFS("seek-end-off-by-one");
  });

  it("FAILS: SEEK_END positions one byte too far", () => {
    const { FS } = h;
    const stream = FS.open("/test", O.RDWR | O.CREAT, 0o777);
    FS.write(stream, encode("1234567890"), 0, 10);

    // SEEK_END with offset 0 should position at byte 10 (end of file)
    const pos = FS.llseek(stream, 0, SEEK_END);
    // Defect: returns 11 instead of 10
    expect(pos).toBe(11);
    expect(pos).not.toBe(10);
    FS.close(stream);
  });

  it("FAILS: SEEK_END with negative offset is off by one", () => {
    const { FS } = h;
    const stream = FS.open("/test", O.RDWR | O.CREAT, 0o777);
    FS.write(stream, encode("1234567890"), 0, 10);

    // SEEK_END(-5) should position at byte 5
    const pos = FS.llseek(stream, -5, SEEK_END);
    // Defect: returns 6 instead of 5
    expect(pos).toBe(6);
    expect(pos).not.toBe(5);
    FS.close(stream);
  });

  it("PASSES: SEEK_SET still works correctly", () => {
    const { FS } = h;
    const stream = FS.open("/test", O.RDWR | O.CREAT, 0o777);
    FS.write(stream, encode("1234567890"), 0, 10);
    const pos = FS.llseek(stream, 3, SEEK_SET);
    expect(pos).toBe(3);
    FS.close(stream);
  });

  it("PASSES: SEEK_CUR still works correctly", () => {
    const { FS } = h;
    const stream = FS.open("/test", O.RDWR | O.CREAT, 0o777);
    FS.write(stream, encode("1234567890"), 0, 10);
    FS.llseek(stream, 3, SEEK_SET);
    const pos = FS.llseek(stream, 2, SEEK_CUR);
    expect(pos).toBe(5);
    FS.close(stream);
  });

  it("PASSES: basic read/write unaffected", () => {
    const { FS } = h;
    FS.writeFile("/test", "hello");
    const data = FS.readFile("/test", {}) as Uint8Array;
    expect(decode(data)).toBe("hello");
  });
});

/**
 * Cross-validation: Run representative conformance tests against each BadFS
 * to verify the defect is caught by its designated batch while passing others.
 *
 * This is the core validation — each defect must:
 * 1. FAIL at least one test from its designated batch
 * 2. PASS at least some tests from other batches
 */
describe("BadFS cross-validation", () => {
  // Batch 1 representative: read back written data
  async function batch1ReadBack(h: FSHarness): Promise<void> {
    const { FS } = h;
    const stream = FS.open("/test", O.RDWR | O.CREAT, 0o777);
    FS.write(stream, encode("Test\n"), 0, 5);
    const stream2 = FS.open("/test", O.RDWR);
    const buf = new Uint8Array(100);
    const n = FS.read(stream2, buf, 0, buf.length);
    expect(n).toBe(5);
    expect(decode(buf, n)).toBe("Test\n");
    FS.close(stream);
    FS.close(stream2);
  }

  // Batch 1 representative: SEEK_END positioning
  async function batch1SeekEnd(h: FSHarness): Promise<void> {
    const { FS } = h;
    const stream = FS.open("/file", O.RDWR | O.CREAT);
    FS.write(stream, encode("1234567890"), 0, 10);
    const pos = FS.llseek(stream, 0, SEEK_END);
    expect(pos).toBe(10);
    FS.close(stream);
  }

  // Batch 2 representative: mtime updates after write
  async function batch2Mtime(h: FSHarness): Promise<void> {
    const { FS } = h;
    const stream = FS.open("/testfile", O.RDWR | O.CREAT, 0o777);
    const statBefore = FS.stat("/testfile");
    await new Promise((r) => setTimeout(r, 10));
    FS.write(stream, encode("data"), 0, 4);
    const statAfter = FS.stat("/testfile");
    expect(statAfter.mtime.getTime()).toBeGreaterThan(
      statBefore.mtime.getTime(),
    );
    FS.close(stream);
  }

  // Batch 2 representative: readdir includes . and ..
  async function batch2Readdir(h: FSHarness): Promise<void> {
    const { FS } = h;
    FS.mkdir("/testdir", 0o777);
    const entries = FS.readdir("/testdir");
    expect(entries).toContain(".");
    expect(entries).toContain("..");
  }

  // Batch 3 representative: rename over existing file
  async function batch3RenameOverwrite(h: FSHarness): Promise<void> {
    const { FS } = h;
    const s1 = FS.open("/a", O.WRONLY | O.CREAT | O.EXCL, 0o666);
    FS.write(s1, encode("abc"), 0, 3);
    FS.close(s1);
    const s2 = FS.open("/b", O.WRONLY | O.CREAT | O.EXCL, 0o666);
    FS.write(s2, encode("xyz"), 0, 3);
    FS.close(s2);
    FS.rename("/a", "/b");
    const data = FS.readFile("/b", {}) as Uint8Array;
    expect(decode(data)).toBe("abc");
  }

  // Batch 3 representative: unlink ENOENT
  async function batch3UnlinkEnoent(h: FSHarness): Promise<void> {
    const { FS, E } = h;
    expectErrno(() => FS.unlink("/nonexistent"), E.ENOENT);
  }

  // Batch 4 representative: open through symlink
  async function batch4SymlinkOpen(h: FSHarness): Promise<void> {
    const { FS } = h;
    FS.writeFile("/target", "hello");
    FS.symlink("/target", "/link");
    const stream = FS.open("/link", O.RDONLY);
    const buf = new Uint8Array(10);
    const n = FS.read(stream, buf, 0, 10);
    expect(n).toBe(5);
    expect(decode(buf, n)).toBe("hello");
    FS.close(stream);
  }

  // Each row: [defect, tests-that-should-fail, tests-that-should-pass]
  const defectMatrix: Array<{
    defect: DefectId;
    shouldFail: Array<{ name: string; fn: (h: FSHarness) => Promise<void> }>;
    shouldPass: Array<{ name: string; fn: (h: FSHarness) => Promise<void> }>;
  }> = [
    {
      defect: "off-by-one-read",
      shouldFail: [{ name: "batch1:readBack", fn: batch1ReadBack }],
      shouldPass: [
        { name: "batch2:readdir", fn: batch2Readdir },
        { name: "batch3:renameOverwrite", fn: batch3RenameOverwrite },
      ],
    },
    {
      defect: "no-mtime-update",
      shouldFail: [{ name: "batch2:mtime", fn: batch2Mtime }],
      shouldPass: [
        { name: "batch1:readBack", fn: batch1ReadBack },
        { name: "batch2:readdir", fn: batch2Readdir },
      ],
    },
    {
      defect: "rename-no-overwrite",
      shouldFail: [
        { name: "batch3:renameOverwrite", fn: batch3RenameOverwrite },
      ],
      shouldPass: [
        { name: "batch1:readBack", fn: batch1ReadBack },
        { name: "batch2:readdir", fn: batch2Readdir },
      ],
    },
    {
      defect: "readdir-missing-dot",
      shouldFail: [{ name: "batch2:readdir", fn: batch2Readdir }],
      shouldPass: [
        { name: "batch1:readBack", fn: batch1ReadBack },
        { name: "batch3:renameOverwrite", fn: batch3RenameOverwrite },
      ],
    },
    {
      defect: "symlink-no-resolve",
      shouldFail: [{ name: "batch4:symlinkOpen", fn: batch4SymlinkOpen }],
      shouldPass: [
        { name: "batch1:readBack", fn: batch1ReadBack },
        { name: "batch2:readdir", fn: batch2Readdir },
      ],
    },
    {
      defect: "wrong-errno-enoent",
      shouldFail: [{ name: "batch3:unlinkEnoent", fn: batch3UnlinkEnoent }],
      shouldPass: [
        { name: "batch1:readBack", fn: batch1ReadBack },
        { name: "batch3:renameOverwrite", fn: batch3RenameOverwrite },
      ],
    },
    {
      defect: "seek-end-off-by-one",
      shouldFail: [{ name: "batch1:seekEnd", fn: batch1SeekEnd }],
      shouldPass: [
        { name: "batch1:readBack", fn: batch1ReadBack },
        { name: "batch2:readdir", fn: batch2Readdir },
      ],
    },
  ];

  for (const { defect, shouldFail, shouldPass } of defectMatrix) {
    describe(`${defect}`, () => {
      let h: FSHarness;
      beforeEach(async () => {
        h = await createBadFS(defect);
      });

      for (const { name, fn } of shouldFail) {
        it(`catches defect: ${name} FAILS`, async () => {
          const passed = await doesPass(() => fn(h));
          expect(passed).toBe(false);
        });
      }

      for (const { name, fn } of shouldPass) {
        it(`non-targeted: ${name} still PASSES`, async () => {
          await fn(h);
        });
      }
    });
  }
});
