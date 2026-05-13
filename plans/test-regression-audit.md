# Test Regression Audit — 2026-05-13

## Result: No regressions found

Full test suite (`npx vitest run`) passed cleanly.

| Metric        | Value                        |
|---------------|------------------------------|
| Test files    | 144 passed, 2 skipped (146)  |
| Tests         | 3297 passed, 22 skipped (0 failed) |
| Duration      | ~1302s (tests: ~3764s wall)  |

## Skipped tests (all intentional)

The 22 skipped tests across 3 files are guarded by `TOMEFS_BACKEND === "tomefs"` and only activate when running the tomefs-specific conformance variant. They are not regressions:

- **`tests/adversarial/allocate-mmap.test.ts`** (13 skipped) — tomefs-only mmap allocation tests
- **`tests/conformance/enametoolong.test.ts`** (8 skipped) — tomefs-only ENAMETOOLONG behavior
- **`tests/conformance/mkdir.test.ts`** (1 skipped) — WASMFS-specific mkdir edge case

To run these, set `TOMEFS_BACKEND=tomefs` as documented in CLAUDE.md.

## CLAUDE.md note

CLAUDE.md references "2300+ tests" but the suite now contains 3319 (3297 run + 22 skipped). The count in CLAUDE.md is stale but not a functional issue.

## Action items

None — no fixes required. If desired, update the test count in CLAUDE.md from "2300+" to "3300+".
