import { Pool } from 'pg';

import { MaintenanceProcessor } from '../../application/maintenance';
import { JsonMaintenanceEventSink } from '../observability/json-maintenance-event-sink';
import { nodeMonotonicNow } from '../runtime/node-application-runtime';
import { PostgresMaintenanceRepository } from './postgres-maintenance-repository';
import { attachPostgresPoolErrorHandler } from './postgres-pool-observability';

export interface MaintenanceRuntimeOptions {
  readonly databaseUrl: string;
}

export class MaintenanceRuntime {
  private readonly pool: Pool;
  private readonly repository: PostgresMaintenanceRepository;
  public readonly processor: MaintenanceProcessor;

  public constructor(options: MaintenanceRuntimeOptions) {
    this.pool = new Pool({
      connectionString: options.databaseUrl,
      application_name: 'usall-maintenance-worker',
      max: 4,
      connectionTimeoutMillis: 5_000,
      query_timeout: 5_000,
      statement_timeout: 5_000,
      lock_timeout: 5_000,
    });
    attachPostgresPoolErrorHandler(this.pool, { component: 'maintenance-worker' });
    this.repository = new PostgresMaintenanceRepository(this.pool);
    this.processor = new MaintenanceProcessor(this.repository, {
      eventSink: new JsonMaintenanceEventSink(),
      monotonicNow: nodeMonotonicNow,
    });
  }

  public async close(): Promise<void> {
    await this.processor.stopObservability();
    await this.repository.releaseCounterConsistencyLeadership();
    await this.pool.end();
  }
}
