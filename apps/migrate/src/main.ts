import { runMigrations } from '@usall-backend-assignment/alarms';
import { safeErrorMetadata } from '@usall-backend-assignment/platform';

import { MigrationLifecycleEventSink } from './migration-lifecycle-events';

const lifecycleEvents = new MigrationLifecycleEventSink();

async function main(): Promise<void> {
  const databaseUrl = process.env.DATABASE_URL;

  if (databaseUrl === undefined || databaseUrl.trim().length === 0) {
    throw new Error('DATABASE_URL is required');
  }

  const applied = await runMigrations({ databaseUrl });
  lifecycleEvents.recordCompleted(applied);
}

void main().catch((error: unknown) => {
  lifecycleEvents.recordFailed(safeErrorMetadata(error));
  process.exitCode = 1;
});
