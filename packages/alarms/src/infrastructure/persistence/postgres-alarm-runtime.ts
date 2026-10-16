import { Pool } from 'pg';

import { AlarmApplicationService } from '../../application/alarm-service';
import { nodeGenerateId } from '../runtime/node-application-runtime';
import { PostgresAlarmRepository } from './postgres-alarm-repository';
import { attachPostgresPoolErrorHandler } from './postgres-pool-observability';

export interface PostgresAlarmRuntimeOptions {
  readonly databaseUrl: string;
  readonly cursorSigningKey: string;
}

export class PostgresAlarmRuntime {
  public readonly repository: PostgresAlarmRepository;
  public readonly service: AlarmApplicationService;
  private readonly pool: Pool;

  public constructor(options: PostgresAlarmRuntimeOptions) {
    if (options.databaseUrl.trim().length === 0) {
      throw new Error('DATABASE_URL is required');
    }

    this.pool = new Pool({
      connectionString: options.databaseUrl,
      application_name: 'usall-api',
      connectionTimeoutMillis: 3_000,
      idleTimeoutMillis: 30_000,
      max: 20,
    });
    attachPostgresPoolErrorHandler(this.pool, { component: 'alarm-api' });
    this.repository = new PostgresAlarmRepository(this.pool);
    this.service = new AlarmApplicationService(
      this.repository,
      options.cursorSigningKey,
      nodeGenerateId,
    );
  }

  public async ready(): Promise<void> {
    await this.pool.query('SELECT 1');
  }

  public close(): Promise<void> {
    return this.pool.end();
  }
}
