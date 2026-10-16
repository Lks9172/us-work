import { fileURLToPath } from 'node:url';

import { defineConfig } from 'vitest/config';

export default defineConfig({
  resolve: {
    alias: {
      '@usall-backend-assignment/alarms': fileURLToPath(
        new URL('./packages/alarms/src/index.ts', import.meta.url),
      ),
      '@usall-backend-assignment/platform': fileURLToPath(
        new URL('./packages/platform/src/index.ts', import.meta.url),
      ),
    },
  },
  test: {
    environment: 'node',
    // The integration files share one PostgreSQL/Redis service while several of them create
    // 10k-100k fixtures and assert latency budgets. Run files serially so those measurements are
    // not distorted by unrelated specs; concurrency exercised inside each file remains intact.
    fileParallelism: false,
    include: ['tests/integration/**/*.integration-spec.ts', 'tests/e2e/**/*.e2e-spec.ts'],
    passWithNoTests: false,
    testTimeout: 30_000,
  },
});
