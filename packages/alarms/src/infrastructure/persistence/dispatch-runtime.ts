import { Pool } from 'pg';

import { DispatchProcessor } from '../../application/dispatch';
import { RedisPacedRateLimiter } from '../limiter/redis-paced-rate-limiter';
import { JsonDispatchEventSink } from '../observability/json-dispatch-event-sink';
import { NodeProviderMessageSender } from '../provider/node-provider-message-sender';
import {
  NodeDispatchHeartbeatScheduler,
  nodeMonotonicNow,
  nodeWaitUntil,
} from '../runtime/node-application-runtime';
import { PostgresDispatchRepository } from './postgres-dispatch-repository';
import { attachPostgresPoolErrorHandler } from './postgres-pool-observability';

export interface DispatchRuntimeOptions {
  readonly databaseUrl: string;
  readonly redisUrl: string;
  readonly providerBaseUrl: string;
}

export class DispatchRuntime {
  private readonly pool: Pool;
  private readonly limiter: RedisPacedRateLimiter;
  private readonly sender: NodeProviderMessageSender;
  public readonly processor: DispatchProcessor;

  public constructor(options: DispatchRuntimeOptions) {
    this.pool = new Pool({
      connectionString: options.databaseUrl,
      application_name: 'usall-dispatch-worker',
      max: 12,
      connectionTimeoutMillis: 5_000,
      query_timeout: 5_000,
      statement_timeout: 5_000,
      lock_timeout: 5_000,
    });
    attachPostgresPoolErrorHandler(this.pool, { component: 'dispatch-worker' });
    this.limiter = new RedisPacedRateLimiter({ redisUrl: options.redisUrl, pool: this.pool });
    this.sender = new NodeProviderMessageSender({ baseUrl: options.providerBaseUrl });
    this.processor = new DispatchProcessor(
      new PostgresDispatchRepository(this.pool),
      this.limiter,
      this.sender,
      {
        monotonicNow: nodeMonotonicNow,
        waitUntil: nodeWaitUntil,
        heartbeatScheduler: new NodeDispatchHeartbeatScheduler(),
        eventSink: new JsonDispatchEventSink(),
      },
    );
  }

  public async close(): Promise<void> {
    await this.limiter.close();
    this.sender.close();
    await this.pool.end();
  }
}
