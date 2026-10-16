import { Module } from '@nestjs/common';
import {
  DispatchRuntime,
  ExpansionRuntime,
  HISTORY_VISIBILITY_PROFILES,
  type HistoryVisibilityProfile,
  MaintenanceRuntime,
  ReconciliationRuntime,
} from '@usall-backend-assignment/alarms';
import { requireEnvironmentUrl } from '@usall-backend-assignment/platform';

import { DISPATCH_RUNTIME, DispatchWorkerRunner } from './dispatch-worker.runner';

import { EXPANSION_RUNTIME, ExpansionWorkerRunner } from './expansion-worker.runner';
import { MAINTENANCE_RUNTIME, MaintenanceWorkerRunner } from './maintenance-worker.runner';
import { RECONCILIATION_RUNTIME, ReconciliationWorkerRunner } from './reconciliation-worker.runner';

function providerHistoryVisibilityProfile(): HistoryVisibilityProfile {
  const configured = process.env.PROVIDER_HISTORY_VISIBILITY ?? 'OPAQUE';
  if (!HISTORY_VISIBILITY_PROFILES.includes(configured as HistoryVisibilityProfile)) {
    throw new Error(
      `PROVIDER_HISTORY_VISIBILITY must be one of ${HISTORY_VISIBILITY_PROFILES.join(', ')}`,
    );
  }
  return configured as HistoryVisibilityProfile;
}

export function authoritativeEmptyGraceMs(): number {
  const configured = process.env.PROVIDER_HISTORY_AUTHORITATIVE_EMPTY_GRACE_MS ?? '30000';
  if (!/^\d+$/.test(configured)) {
    throw new Error(
      'PROVIDER_HISTORY_AUTHORITATIVE_EMPTY_GRACE_MS must be a PostgreSQL integer from 0 through 2147483647',
    );
  }
  const parsed = Number(configured);
  if (!Number.isSafeInteger(parsed) || parsed > 2_147_483_647) {
    throw new Error(
      'PROVIDER_HISTORY_AUTHORITATIVE_EMPTY_GRACE_MS must be a PostgreSQL integer from 0 through 2147483647',
    );
  }
  return parsed;
}

@Module({
  providers: [
    {
      provide: DISPATCH_RUNTIME,
      useFactory: () => {
        const databaseUrl = requireEnvironmentUrl(process.env.DATABASE_URL, 'DATABASE_URL', [
          'postgres:',
          'postgresql:',
        ]);
        const redisUrl = requireEnvironmentUrl(process.env.REDIS_URL, 'REDIS_URL', [
          'redis:',
          'rediss:',
        ]);
        const providerBaseUrl = requireEnvironmentUrl(
          process.env.PROVIDER_BASE_URL,
          'PROVIDER_BASE_URL',
          ['http:', 'https:'],
        );

        return new DispatchRuntime({ databaseUrl, redisUrl, providerBaseUrl });
      },
    },
    {
      provide: EXPANSION_RUNTIME,
      useFactory: () => {
        const databaseUrl = requireEnvironmentUrl(process.env.DATABASE_URL, 'DATABASE_URL', [
          'postgres:',
          'postgresql:',
        ]);
        const providerBaseUrl = requireEnvironmentUrl(
          process.env.PROVIDER_BASE_URL,
          'PROVIDER_BASE_URL',
          ['http:', 'https:'],
        );

        return new ExpansionRuntime({ databaseUrl, providerBaseUrl });
      },
    },
    {
      provide: MAINTENANCE_RUNTIME,
      useFactory: () => {
        const databaseUrl = requireEnvironmentUrl(process.env.DATABASE_URL, 'DATABASE_URL', [
          'postgres:',
          'postgresql:',
        ]);
        return new MaintenanceRuntime({ databaseUrl });
      },
    },
    {
      provide: RECONCILIATION_RUNTIME,
      useFactory: () => {
        const databaseUrl = requireEnvironmentUrl(process.env.DATABASE_URL, 'DATABASE_URL', [
          'postgres:',
          'postgresql:',
        ]);
        const providerBaseUrl = requireEnvironmentUrl(
          process.env.PROVIDER_BASE_URL,
          'PROVIDER_BASE_URL',
          ['http:', 'https:'],
        );
        return new ReconciliationRuntime({
          databaseUrl,
          providerBaseUrl,
          historyVisibilityProfile: providerHistoryVisibilityProfile(),
          authoritativeEmptyGraceMs: authoritativeEmptyGraceMs(),
        });
      },
    },
    ExpansionWorkerRunner,
    DispatchWorkerRunner,
    MaintenanceWorkerRunner,
    ReconciliationWorkerRunner,
  ],
})
export class WorkerModule {}
