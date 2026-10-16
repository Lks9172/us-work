import { randomUUID } from 'node:crypto';

import { Client } from 'pg';

export interface TestDatabase {
  readonly databaseUrl: string;
  cleanup(): Promise<void>;
}

export async function createTestDatabase(baseDatabaseUrl: string): Promise<TestDatabase> {
  const schema = `test_${randomUUID().replaceAll('-', '')}`;
  const admin = new Client({ connectionString: baseDatabaseUrl });
  await admin.connect();
  await admin.query(`CREATE SCHEMA "${schema}"`);
  await admin.end();

  const scopedUrl = new URL(baseDatabaseUrl);
  scopedUrl.searchParams.set('options', `-csearch_path=${schema}`);

  return {
    databaseUrl: scopedUrl.toString(),
    async cleanup(): Promise<void> {
      const cleanupClient = new Client({ connectionString: baseDatabaseUrl });
      await cleanupClient.connect();
      try {
        await cleanupClient.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
      } finally {
        await cleanupClient.end();
      }
    },
  };
}
