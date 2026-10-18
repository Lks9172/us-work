import { EventEmitter } from 'node:events';
import type { ClientRequest } from 'node:http';

import type { Pool } from 'pg';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type { DispatchProcessor } from '../../packages/alarms/src/application/dispatch';
import type { RedisPacedRateLimiter } from '../../packages/alarms/src/infrastructure/limiter/redis-paced-rate-limiter';
import { DispatchRuntime } from '../../packages/alarms/src/infrastructure/persistence/dispatch-runtime';
import { ExpansionRuntime } from '../../packages/alarms/src/infrastructure/persistence/expansion-runtime';
import { PostgresAlarmRuntime } from '../../packages/alarms/src/infrastructure/persistence/postgres-alarm-runtime';
import { ReconciliationRuntime } from '../../packages/alarms/src/infrastructure/persistence/reconciliation-runtime';
import { NodeProviderMessageSender } from '../../packages/alarms/src/infrastructure/provider/node-provider-message-sender';

const providerRequest = {
  alarmId: '11111111-1111-4111-8111-111111111111',
  recipientId: 'user-1',
  body: 'body',
  clientRef: '22222222-2222-4222-8222-222222222222',
};

afterEach(() => {
  vi.useRealTimers();
});

describe('bounded production dispatch configuration', () => {
  it('OPS-14 uses a real 35 second default provider response deadline', async () => {
    vi.useFakeTimers();
    const request = Object.assign(new EventEmitter(), {
      destroy: vi.fn(),
      end: vi.fn(),
    }) as unknown as ClientRequest;
    const sender = new NodeProviderMessageSender({
      baseUrl: 'http://127.0.0.1:1',
      requestFactory: () => request,
    });
    let settled = false;
    const outcome = sender.send(providerRequest).then((result) => {
      settled = true;
      return result;
    });

    await vi.advanceTimersByTimeAsync(34_999);
    expect(settled).toBe(false);
    expect(request.destroy).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(1);
    await expect(outcome).resolves.toEqual({
      kind: 'AMBIGUOUS',
      code: 'PROVIDER_TIMEOUT',
    });
    expect(request.destroy).toHaveBeenCalledOnce();
    request.emit('close');
    sender.close();
  });

  it('OPS-14 binds the production pool, lease, heartbeat, limiter, and socket caps', async () => {
    const runtime = new DispatchRuntime({
      databaseUrl: 'postgresql://usall:usall@127.0.0.1:1/usall',
      redisUrl: 'redis://127.0.0.1:1',
      providerBaseUrl: 'http://127.0.0.1:1',
    });
    const internals = runtime as unknown as {
      readonly pool: Pool & { readonly options: Record<string, unknown> };
      readonly processor: DispatchProcessor & {
        readonly leaseDurationMs: number;
        readonly heartbeatIntervalMs: number;
      };
      readonly limiter: RedisPacedRateLimiter & {
        readonly intervalMs: number;
        readonly leadMs: number;
        readonly horizonMs: number;
      };
      readonly sender: NodeProviderMessageSender & {
        readonly responseTimeoutMs: number;
        readonly httpAgent: { readonly maxSockets: number };
        readonly httpsAgent: { readonly maxSockets: number };
      };
    };

    try {
      expect(internals.pool.options).toMatchObject({
        max: 12,
        connectionTimeoutMillis: 5_000,
        query_timeout: 5_000,
        statement_timeout: 5_000,
        lock_timeout: 5_000,
      });
      expect(internals.processor.leaseDurationMs).toBe(60_000);
      expect(internals.processor.heartbeatIntervalMs).toBe(10_000);
      expect(internals.limiter.intervalMs).toBe(25);
      expect(internals.limiter.leadMs).toBe(50);
      expect(internals.limiter.horizonMs).toBe(100);
      expect(internals.sender.responseTimeoutMs).toBe(35_000);
      expect(internals.sender.httpAgent.maxSockets).toBe(64);
      expect(internals.sender.httpsAgent.maxSockets).toBe(64);
    } finally {
      await runtime.close();
    }
  });

  it('OPS-15 bounds PostgreSQL waits by workload without cutting off 100k publication', async () => {
    const alarm = new PostgresAlarmRuntime({
      databaseUrl: 'postgresql://usall:usall@127.0.0.1:1/usall',
      cursorSigningKey: 'runtime-capacity-contract-signing-key',
    });
    const expansion = new ExpansionRuntime({
      databaseUrl: 'postgresql://usall:usall@127.0.0.1:1/usall',
      providerBaseUrl: 'http://127.0.0.1:1',
    });
    const reconciliation = new ReconciliationRuntime({
      databaseUrl: 'postgresql://usall:usall@127.0.0.1:1/usall',
      providerBaseUrl: 'http://127.0.0.1:1',
    });
    const optionsOf = (runtime: object): Record<string, unknown> =>
      (
        runtime as unknown as {
          readonly pool: Pool & { readonly options: Record<string, unknown> };
        }
      ).pool.options;

    try {
      expect(optionsOf(alarm)).toMatchObject({
        max: 20,
        connectionTimeoutMillis: 3_000,
        query_timeout: 5_000,
        statement_timeout: 5_000,
        lock_timeout: 5_000,
      });
      expect(optionsOf(reconciliation)).toMatchObject({
        max: 10,
        connectionTimeoutMillis: 5_000,
        query_timeout: 5_000,
        statement_timeout: 5_000,
        lock_timeout: 5_000,
      });
      expect(optionsOf(expansion)).toMatchObject({
        max: 4,
        connectionTimeoutMillis: 5_000,
        query_timeout: 30_000,
        statement_timeout: 30_000,
        lock_timeout: 5_000,
      });
    } finally {
      await Promise.all([alarm.close(), expansion.close(), reconciliation.close()]);
    }
  });
});
