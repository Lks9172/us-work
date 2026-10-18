import { EventEmitter } from 'node:events';

import type { Pool, PoolClient } from 'pg';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { PersistenceUnavailableError } from '../../packages/alarms/src/application/alarm-errors';
import { PostgresAlarmRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-alarm-repository';
import { PostgresExpansionRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-expansion-repository';
import { PostgresMaintenanceRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-maintenance-repository';
import { PostgresReconciliationRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-reconciliation-repository';

interface FakeClient extends EventEmitter {
  readonly query: ReturnType<typeof vi.fn>;
  readonly release: ReturnType<typeof vi.fn>;
}

afterEach(() => {
  vi.useRealTimers();
});

function clientWithQuery(query: FakeClient['query']): FakeClient {
  return Object.assign(new EventEmitter(), {
    query,
    release: vi.fn(),
  });
}

function poolWithClient(client: FakeClient): Pool {
  return {
    connect: vi.fn().mockResolvedValue(client as unknown as PoolClient),
  } as unknown as Pool;
}

const repositoryCases = [
  {
    name: 'alarm',
    invoke: (pool: Pool) =>
      new PostgresAlarmRepository(pool).create({
        id: '11111111-1111-4111-8111-111111111111',
        type: 'BULK',
        title: 'title',
        body: 'body',
        recipientIds: [],
      }),
  },
  {
    name: 'expansion',
    invoke: (pool: Pool) => new PostgresExpansionRepository(pool).claimNext(30_000),
  },
  {
    name: 'reconciliation',
    invoke: (pool: Pool) => new PostgresReconciliationRepository(pool).claimNext(15_000),
  },
  {
    name: 'maintenance',
    invoke: (pool: Pool) => new PostgresMaintenanceRepository(pool).sweepCanceledDeliveries(500),
  },
] as const;

describe.each(repositoryCases)('$name repository transaction disposal', ({ invoke }) => {
  it.each([false, true])(
    'destroys a timed-out transaction client instead of returning it to the pool (rollbackFails=%s)',
    async (rollbackFails) => {
      const timeout = Object.assign(new Error('query read timeout'), { code: 'QUERY_TIMEOUT' });
      const client = clientWithQuery(
        vi.fn(async (text: string) => {
          if (text === 'BEGIN') return { rows: [], rowCount: null };
          if (text === 'ROLLBACK') {
            if (rollbackFails) throw new Error('connection remained blackholed');
            return { rows: [], rowCount: null };
          }
          throw timeout;
        }),
      );

      await expect(invoke(poolWithClient(client))).rejects.toBeInstanceOf(
        PersistenceUnavailableError,
      );

      expect(client.query).toHaveBeenCalledWith('ROLLBACK');
      expect(client.release).toHaveBeenCalledTimes(1);
      expect(client.release).toHaveBeenCalledWith(true);
      expect(client.listenerCount('error')).toBe(0);
    },
  );
});

describe('bounded PostgreSQL transaction cleanup', () => {
  it('stops waiting for a blackholed ROLLBACK after five seconds and destroys the client', async () => {
    vi.useFakeTimers();
    const never = new Promise<never>(() => undefined);
    const timeout = Object.assign(new Error('query read timeout'), { code: 'QUERY_TIMEOUT' });
    const client = clientWithQuery(
      vi.fn(async (text: string) => {
        if (text === 'BEGIN') return { rows: [], rowCount: null };
        if (text === 'ROLLBACK') return never;
        throw timeout;
      }),
    );
    const outcome = new PostgresExpansionRepository(poolWithClient(client)).claimNext(30_000).then(
      () => ({ kind: 'resolved' as const }),
      (error: unknown) => ({ error, kind: 'rejected' as const }),
    );
    let settled = false;
    void outcome.then(() => {
      settled = true;
    });

    await vi.advanceTimersByTimeAsync(0);
    expect(client.query).toHaveBeenCalledWith('ROLLBACK');
    await vi.advanceTimersByTimeAsync(4_999);
    expect(settled).toBe(false);

    await vi.advanceTimersByTimeAsync(1);
    await expect(outcome).resolves.toMatchObject({
      error: expect.any(PersistenceUnavailableError),
      kind: 'rejected',
    });
    expect(client.release).toHaveBeenCalledOnce();
    expect(client.release).toHaveBeenCalledWith(true);
  });
});
