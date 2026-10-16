import type { Pool, PoolClient } from 'pg';
import { describe, expect, it, vi } from 'vitest';

import { PostgresMaintenanceRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-maintenance-repository';

function clientWith(
  query: (sql: string, parameters?: readonly unknown[]) => Promise<{ readonly rows: unknown[] }>,
): PoolClient {
  return {
    query: vi.fn(query),
    on: vi.fn(),
    removeListener: vi.fn(),
    release: vi.fn(),
  } as unknown as PoolClient;
}

describe('PostgresMaintenanceRepository operational observation', () => {
  it('OBS-02 maps the leader-owned DB snapshot and bounds its window', async () => {
    const queries: Array<{ readonly sql: string; readonly parameters?: readonly unknown[] }> = [];
    const client = clientWith(async (sql, parameters) => {
      queries.push({ sql, parameters });
      if (sql.includes('pg_try_advisory_lock')) {
        return { rows: [{ acquired: true }] };
      }
      return {
        rows: [
          {
            observed_at: new Date('2026-10-15T00:00:00.000Z'),
            queue_depth: 12,
            due_queue_depth: 7,
            oldest_due_age_ms: 2_500,
            sent: 80,
            failed: 3,
            retry_wait: 4,
            unknown: 2,
            unresolved: 1,
            active_send_leases: 5,
            provider_attempts_window: 50,
            provider_accepted_window: 47,
            provider_rate_limited_window: 2,
          },
        ],
      };
    });
    const pool = { connect: vi.fn(async () => client) } as unknown as Pool;
    const repository = new PostgresMaintenanceRepository(pool);

    await expect(repository.readOperationalSnapshot(99_999)).resolves.toEqual({
      observedAt: '2026-10-15T00:00:00.000Z',
      windowSeconds: 3_600,
      queueDepth: 12,
      dueQueueDepth: 7,
      oldestDueAgeMs: 2_500,
      sent: 80,
      failed: 3,
      retryWait: 4,
      unknown: 2,
      unresolved: 1,
      activeSendLeases: 5,
      providerAttemptsWindow: 50,
      providerAcceptedWindow: 47,
      providerRateLimitedWindow: 2,
      providerThroughputPerSecond: 47 / 3_600,
    });
    expect(queries).toHaveLength(2);
    expect(queries[1]?.parameters).toEqual([3_600]);
    expect(queries[1]?.sql).toContain('COALESCE(sum(a.sent), 0)::bigint');
    expect(queries[1]?.sql).toContain("WHERE d.state IN ('READY', 'RETRY_WAIT')");
    expect(queries[1]?.sql).toContain("WHERE state = 'UNKNOWN'");
    expect(queries[1]?.sql).toContain("WHERE state = 'RECONCILING'");
    expect(queries[1]?.sql).toContain('a.http_status = 429');
    expect(queries[1]?.sql).not.toContain("d.state = 'SENT'");
    expect(queries[1]?.sql).not.toContain("'FAILED_PERMANENT', 'FAILED_RETRY_EXHAUSTED'");
  });

  it('OBS-02 returns no duplicate global snapshot when advisory leadership is held elsewhere', async () => {
    const client = clientWith(async () => ({ rows: [{ acquired: false }] }));
    const pool = { connect: vi.fn(async () => client) } as unknown as Pool;
    const repository = new PostgresMaintenanceRepository(pool);

    await expect(repository.readOperationalSnapshot(60)).resolves.toBeNull();
    expect(client.release).toHaveBeenCalledOnce();
    expect(client.query).toHaveBeenCalledOnce();
  });
});
