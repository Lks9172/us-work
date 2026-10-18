import { Pool } from 'pg';

import { ReconciliationProcessor } from '../../application/reconciliation';
import { HttpProviderMessageHistory } from '../provider/http-provider-message-history';
import {
  type HistoryVisibilityProfile,
  PostgresReconciliationRepository,
} from './postgres-reconciliation-repository';
import { attachPostgresPoolErrorHandler } from './postgres-pool-observability';

export interface ReconciliationRuntimeOptions {
  readonly databaseUrl: string;
  readonly providerBaseUrl: string;
  readonly historyVisibilityProfile?: HistoryVisibilityProfile;
  readonly authoritativeEmptyGraceMs?: number;
  readonly maxFastChecks?: number;
  readonly maxFastElapsedMs?: number;
}

export class ReconciliationRuntime {
  private readonly pool: Pool;
  public readonly processor: ReconciliationProcessor;

  public constructor(options: ReconciliationRuntimeOptions) {
    this.pool = new Pool({
      connectionString: options.databaseUrl,
      application_name: 'usall-reconciliation-worker',
      connectionTimeoutMillis: 5_000,
      idleTimeoutMillis: 30_000,
      query_timeout: 5_000,
      statement_timeout: 5_000,
      lock_timeout: 5_000,
      max: 10,
    });
    attachPostgresPoolErrorHandler(this.pool, { component: 'reconciliation-worker' });
    this.processor = new ReconciliationProcessor(
      new PostgresReconciliationRepository(this.pool, {
        ...(options.historyVisibilityProfile === undefined
          ? {}
          : { historyVisibilityProfile: options.historyVisibilityProfile }),
        ...(options.authoritativeEmptyGraceMs === undefined
          ? {}
          : { authoritativeEmptyGraceMs: options.authoritativeEmptyGraceMs }),
        ...(options.maxFastChecks === undefined ? {} : { maxFastChecks: options.maxFastChecks }),
        ...(options.maxFastElapsedMs === undefined
          ? {}
          : { maxFastElapsedMs: options.maxFastElapsedMs }),
      }),
      new HttpProviderMessageHistory({ baseUrl: options.providerBaseUrl }),
    );
  }

  public close(): Promise<void> {
    return this.pool.end();
  }
}
