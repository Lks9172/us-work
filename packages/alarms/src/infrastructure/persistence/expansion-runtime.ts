import { Pool } from 'pg';

import { ExpansionProcessor } from '../../application/expansion';
import { JsonExpansionEventSink } from '../observability/json-expansion-event-sink';
import { HttpUserDirectory } from '../provider/http-user-directory';
import { nodeExpansionSleep, nodeRandom } from '../runtime/node-application-runtime';
import { PostgresExpansionRepository } from './postgres-expansion-repository';
import { attachPostgresPoolErrorHandler } from './postgres-pool-observability';

export interface ExpansionRuntimeOptions {
  readonly databaseUrl: string;
  readonly providerBaseUrl: string;
}

export class ExpansionRuntime {
  private readonly pool: Pool;
  public readonly processor: ExpansionProcessor;

  public constructor(options: ExpansionRuntimeOptions) {
    this.pool = new Pool({
      connectionString: options.databaseUrl,
      application_name: 'usall-expansion-worker',
      connectionTimeoutMillis: 5_000,
      idleTimeoutMillis: 30_000,
      query_timeout: 30_000,
      statement_timeout: 30_000,
      lock_timeout: 5_000,
      max: 4,
    });
    attachPostgresPoolErrorHandler(this.pool, { component: 'expansion-worker' });
    this.processor = new ExpansionProcessor(
      new PostgresExpansionRepository(this.pool, new JsonExpansionEventSink()),
      new HttpUserDirectory({ baseUrl: options.providerBaseUrl }),
      { sleep: nodeExpansionSleep, random: nodeRandom },
    );
  }

  public close(): Promise<void> {
    return this.pool.end();
  }
}
