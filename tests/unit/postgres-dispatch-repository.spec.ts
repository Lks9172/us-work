import { EventEmitter } from 'node:events';

import type { Pool, PoolClient } from 'pg';
import { describe, expect, it, vi } from 'vitest';

import { PersistenceUnavailableError } from '../../packages/alarms/src/application/alarm-errors';
import { PostgresDispatchRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-dispatch-repository';

interface FakeClient extends EventEmitter {
  readonly query: ReturnType<typeof vi.fn>;
  readonly release: ReturnType<typeof vi.fn>;
}

function clientWithQuery(query: FakeClient['query']): FakeClient {
  return Object.assign(new EventEmitter(), {
    query,
    release: vi.fn(),
  });
}

function repositoryWithClient(client: FakeClient): PostgresDispatchRepository {
  const pool = {
    connect: vi.fn().mockResolvedValue(client as unknown as PoolClient),
  } as unknown as Pool;
  return new PostgresDispatchRepository(pool);
}

describe('PostgresDispatchRepository transaction disposal', () => {
  it('returns a client to the pool only after BEGIN, work, and COMMIT all succeed', async () => {
    const client = clientWithQuery(
      vi.fn(async (text: string) => {
        if (text === 'BEGIN' || text === 'COMMIT') return { rows: [], rowCount: null };
        return { rows: [], rowCount: 0 };
      }),
    );
    const repository = repositoryWithClient(client);

    await expect(repository.claimNext(0, 60_000)).resolves.toBeNull();

    expect(client.release).toHaveBeenCalledTimes(1);
    expect(client.release).toHaveBeenCalledWith();
    expect(client.listenerCount('error')).toBe(0);
  });

  it.each([false, true])(
    'destroys an uncertain client after a transactional query error (rollbackFails=%s)',
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
      const repository = repositoryWithClient(client);

      await expect(repository.claimNext(0, 60_000)).rejects.toBeInstanceOf(
        PersistenceUnavailableError,
      );

      expect(client.query).toHaveBeenCalledWith('ROLLBACK');
      expect(client.release).toHaveBeenCalledTimes(1);
      expect(client.release).toHaveBeenCalledWith(true);
      expect(client.listenerCount('error')).toBe(0);
    },
  );
});
