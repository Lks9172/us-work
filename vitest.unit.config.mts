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
    include: ['tests/scaffold/**/*.spec.ts', 'tests/unit/**/*.spec.ts'],
    passWithNoTests: false,
  },
});
