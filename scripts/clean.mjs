import { rm } from 'node:fs/promises';

const generatedDirectories = [
  'apps/api/dist',
  'apps/worker/dist',
  'packages/alarms/dist',
  'packages/platform/dist',
  'coverage',
];

await Promise.all(
  generatedDirectories.map((directory) => rm(directory, { force: true, recursive: true })),
);
