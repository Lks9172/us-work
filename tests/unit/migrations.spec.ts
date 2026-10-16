import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { afterEach, describe, expect, it } from 'vitest';

import {
  discoverMigrationFiles,
  migrationChecksum,
} from '../../packages/alarms/src/infrastructure/persistence/migrations';

const temporaryDirectories: string[] = [];

afterEach(async () => {
  await Promise.all(
    temporaryDirectories.splice(0).map((directory) => rm(directory, { recursive: true })),
  );
});

async function createTemporaryDirectory(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), 'usall-migrations-'));
  temporaryDirectories.push(directory);
  return directory;
}

describe('migration discovery', () => {
  it('sorts versioned SQL files and computes immutable checksums', async () => {
    const directory = await createTemporaryDirectory();
    await Promise.all([
      writeFile(join(directory, '0002_second.sql'), 'SELECT 2;\n'),
      writeFile(join(directory, '0001_first.sql'), 'SELECT 1;\n'),
      writeFile(join(directory, 'README.md'), 'ignored'),
    ]);

    const migrations = await discoverMigrationFiles(directory);

    expect(migrations.map(({ version }) => version)).toEqual(['0001_first.sql', '0002_second.sql']);
    expect(migrations[0]?.checksum).toBe(migrationChecksum('SELECT 1;\n'));
  });

  it('rejects an empty migration directory', async () => {
    const directory = await createTemporaryDirectory();

    await expect(discoverMigrationFiles(directory)).rejects.toThrow(
      'No versioned SQL migrations found',
    );
  });

  it('rejects duplicate numeric versions', async () => {
    const directory = await createTemporaryDirectory();
    await Promise.all([
      writeFile(join(directory, '0001_first.sql'), 'SELECT 1;\n'),
      writeFile(join(directory, '0001_second.sql'), 'SELECT 2;\n'),
    ]);

    await expect(discoverMigrationFiles(directory)).rejects.toThrow(
      'Duplicate migration version: 0001',
    );
  });
});
