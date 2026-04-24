import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globals: true,
    testTimeout: 30000,
    include: ["tests/**/*.test.ts"],
    pool: "forks",
    poolOptions: {
      forks: {
        maxForks: 4,
      },
    },
  },
});
