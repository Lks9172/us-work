import { createHash } from 'node:crypto';
import { readdir, readFile } from 'node:fs/promises';
import { resolve } from 'node:path';

import { Client } from 'pg';

const MIGRATION_FILE_PATTERN = /^\d{4}_[a-z0-9_]+\.sql$/;
const MIGRATION_LOCK_NAMESPACE = 811_771;
const MIGRATION_LOCK_KEY = 1;

export interface MigrationFile {
  readonly version: string;
  readonly path: string;
  readonly sql: string;
  readonly checksum: string;
}

export interface RunMigrationsOptions {
  readonly databaseUrl: string;
  readonly migrationsDirectory?: string;
}

export function migrationChecksum(sql: string): string {
  return createHash('sha256').update(sql).digest('hex');
}

export async function discoverMigrationFiles(directory: string): Promise<readonly MigrationFile[]> {
  const entries = await readdir(directory, { withFileTypes: true });
  const fileNames = entries
    .filter((entry) => entry.isFile() && MIGRATION_FILE_PATTERN.test(entry.name))
    .map((entry) => entry.name)
    .sort();

  if (fileNames.length === 0) {
    throw new Error(`No versioned SQL migrations found in ${directory}`);
  }

  const duplicatedVersions = fileNames
    .map((fileName) => fileName.slice(0, 4))
    .filter((version, index, versions) => versions.indexOf(version) !== index);

  if (duplicatedVersions.length > 0) {
    throw new Error(`Duplicate migration version: ${duplicatedVersions[0]}`);
  }

  return Promise.all(
    fileNames.map(async (fileName) => {
      const path = resolve(directory, fileName);
      const sql = await readFile(path, 'utf8');

      return {
        version: fileName,
        path,
        sql,
        checksum: migrationChecksum(sql),
      } satisfies MigrationFile;
    }),
  );
}

export async function runMigrations(options: RunMigrationsOptions): Promise<readonly string[]> {
  if (options.databaseUrl.trim().length === 0) {
    throw new Error('DATABASE_URL is required to run migrations');
  }

  const migrationsDirectory = resolve(
    options.migrationsDirectory ??
      process.env.MIGRATIONS_DIR ??
      resolve(process.cwd(), 'migrations'),
  );
  const migrations = await discoverMigrationFiles(migrationsDirectory);
  const client = new Client({
    connectionString: options.databaseUrl,
    application_name: 'usall-migrate',
  });
  const appliedVersions: string[] = [];

  await client.connect();

  try {
    await client.query('SELECT pg_advisory_lock($1, $2)', [
      MIGRATION_LOCK_NAMESPACE,
      MIGRATION_LOCK_KEY,
    ]);
    await client.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        version text PRIMARY KEY,
        checksum text NOT NULL,
        applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
      )
    `);

    for (const migration of migrations) {
      const existing = await client.query<{ checksum: string }>(
        'SELECT checksum FROM schema_migrations WHERE version = $1',
        [migration.version],
      );

      if (existing.rowCount === 1) {
        if (existing.rows[0]?.checksum !== migration.checksum) {
          throw new Error(`Applied migration checksum changed: ${migration.version}`);
        }

        continue;
      }

      await client.query('BEGIN');

      try {
        await client.query(migration.sql);
        await client.query('INSERT INTO schema_migrations(version, checksum) VALUES ($1, $2)', [
          migration.version,
          migration.checksum,
        ]);
        await client.query('COMMIT');
        appliedVersions.push(migration.version);
      } catch (error) {
        await client.query('ROLLBACK');
        throw error;
      }
    }
  } finally {
    await client
      .query('SELECT pg_advisory_unlock($1, $2)', [MIGRATION_LOCK_NAMESPACE, MIGRATION_LOCK_KEY])
      .catch(() => undefined);
    await client.end();
  }

  return appliedVersions;
}
