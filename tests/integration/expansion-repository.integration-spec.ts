import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';

import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { AlarmApplicationService } from '../../packages/alarms/src/application/alarm-service';
import {
  ExpansionProcessor,
  type UserDirectory,
} from '../../packages/alarms/src/application/expansion';
import { runMigrations } from '../../packages/alarms/src/infrastructure/persistence/migrations';
import { PostgresAlarmRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-alarm-repository';
import { PostgresExpansionRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-expansion-repository';
import { PostgresMaintenanceRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-maintenance-repository';
import { HttpUserDirectory } from '../../packages/alarms/src/infrastructure/provider/http-user-directory';
import { createTestDatabase, type TestDatabase } from './postgres-test-schema';

const databaseUrl = process.env.TEST_DATABASE_URL;
const describeWithDatabase = databaseUrl === undefined ? describe.skip : describe;

describeWithDatabase('PostgresExpansionRepository', () => {
  let testDatabase: TestDatabase;
  let pool: Pool;
  let alarms: AlarmApplicationService;
  let expansions: PostgresExpansionRepository;
  let maintenance: PostgresMaintenanceRepository;

  beforeAll(async () => {
    if (databaseUrl === undefined) {
      throw new Error('TEST_DATABASE_URL is required');
    }

    testDatabase = await createTestDatabase(databaseUrl);
    await runMigrations({
      databaseUrl: testDatabase.databaseUrl,
      migrationsDirectory: resolve(process.cwd(), 'migrations'),
    });
    pool = new Pool({ connectionString: testDatabase.databaseUrl, max: 10 });
    alarms = new AlarmApplicationService(
      new PostgresAlarmRepository(pool),
      'integration-cursor-key',
      randomUUID,
    );
    expansions = new PostgresExpansionRepository(pool);
    maintenance = new PostgresMaintenanceRepository(pool);
  });

  afterAll(async () => {
    await pool?.end();
    await testDatabase?.cleanup();
  });

  async function startBulk(): Promise<string> {
    const created = await alarms.create({
      type: 'BULK',
      title: `bulk-${randomUUID()}`,
      body: 'body',
    });
    await alarms.start(created.id);
    return created.id;
  }

  it('stages pages behind the audience barrier and atomically opens the delivery queue', async () => {
    const alarmId = await startBulk();
    const lease = await expansions.claimNext(30_000);
    expect(lease?.alarmId).toBe(alarmId);
    if (lease === null) {
      throw new Error('Expected an expansion lease');
    }

    await expect(
      expansions.savePage(lease, { recipientIds: [], nextCursor: 'cursor-a' }, 30_000),
    ).resolves.toBe('CONTINUE');
    await expect(
      expansions.savePage(
        lease,
        { recipientIds: ['user-1', 'user-2', 'user-1'], nextCursor: 'cursor-b' },
        30_000,
      ),
    ).resolves.toBe('CONTINUE');

    const beforeFinalPage = await pool.query<{ count: number }>(
      'SELECT count(*)::integer AS count FROM deliveries WHERE alarm_id = $1',
      [alarmId],
    );
    expect(beforeFinalPage.rows[0]?.count).toBe(0);

    await expect(
      expansions.savePage(lease, { recipientIds: ['user-2', 'user-3'], nextCursor: null }, 30_000),
    ).resolves.toBe('COMPLETED');

    const alarm = await alarms.findById(alarmId);
    const deliveries = await pool.query<{
      recipient_id: string;
      state: string;
      priority: number;
    }>(
      `
        SELECT recipient_id, state, priority
        FROM deliveries
        WHERE alarm_id = $1
        ORDER BY recipient_id
      `,
      [alarmId],
    );
    const job = await pool.query<{
      state: string;
      cleanup_state: string;
      materialized_count: number;
    }>(
      'SELECT state, cleanup_state, materialized_count FROM alarm_expansion_jobs WHERE alarm_id = $1',
      [alarmId],
    );

    expect(alarm).toMatchObject({
      status: 'SENDING',
      materializedCount: 3,
      total: 3,
      active: 3,
    });
    expect(deliveries.rows).toEqual([
      { recipient_id: 'user-1', state: 'READY', priority: 10 },
      { recipient_id: 'user-2', state: 'READY', priority: 10 },
      { recipient_id: 'user-3', state: 'READY', priority: 10 },
    ]);
    expect(job.rows[0]).toEqual({
      state: 'COMPLETED',
      cleanup_state: 'PENDING',
      materialized_count: 3,
    });
  });

  it('SND-08/USR-11 rolls back page staging and checkpoint together, then retries the same lease', async () => {
    const alarmId = await startBulk();
    const lease = await expansions.claimNext(30_000);
    if (lease === null) {
      throw new Error('Expected a page checkpoint lease');
    }

    await pool.query(`
      CREATE FUNCTION reject_expansion_checkpoint() RETURNS trigger
      LANGUAGE plpgsql AS $$
      BEGIN
        RAISE EXCEPTION 'injected expansion checkpoint failure';
      END
      $$
    `);
    await pool.query(`
      CREATE TRIGGER reject_expansion_checkpoint_trigger
      BEFORE UPDATE OF next_cursor ON alarm_expansion_jobs
      FOR EACH ROW EXECUTE FUNCTION reject_expansion_checkpoint()
    `);
    try {
      await expect(
        expansions.savePage(
          lease,
          { recipientIds: ['rollback-a', 'rollback-b'], nextCursor: 'checkpoint-a' },
          30_000,
        ),
      ).rejects.toThrow();
    } finally {
      await pool.query('DROP TRIGGER reject_expansion_checkpoint_trigger ON alarm_expansion_jobs');
      await pool.query('DROP FUNCTION reject_expansion_checkpoint()');
    }

    const rolledBack = await pool.query<{
      alarm_materialized_count: number;
      cursor_count: number;
      job_materialized_count: number;
      lease_generation: number;
      lease_token: string;
      next_cursor: string | null;
      recipient_count: number;
      run_generation: string;
    }>(
      `
        SELECT
          a.materialized_count AS alarm_materialized_count,
          j.materialized_count AS job_materialized_count,
          j.run_generation,
          j.lease_generation,
          j.lease_token,
          j.next_cursor,
          (
            SELECT count(*)::integer
            FROM alarm_expansion_recipients r
            WHERE r.alarm_id = j.alarm_id AND r.run_generation = j.run_generation
          ) AS recipient_count,
          (
            SELECT count(*)::integer
            FROM alarm_expansion_seen_cursors c
            WHERE c.alarm_id = j.alarm_id AND c.run_generation = j.run_generation
          ) AS cursor_count
        FROM alarms a
        JOIN alarm_expansion_jobs j ON j.alarm_id = a.id
        WHERE a.id = $1
      `,
      [alarmId],
    );
    expect(rolledBack.rows[0]).toEqual({
      alarm_materialized_count: 0,
      cursor_count: 1,
      job_materialized_count: 0,
      lease_generation: lease.leaseGeneration,
      lease_token: lease.leaseToken,
      next_cursor: null,
      recipient_count: 0,
      run_generation: lease.runGeneration,
    });

    await expect(
      expansions.savePage(
        lease,
        { recipientIds: ['rollback-a', 'rollback-b'], nextCursor: 'checkpoint-a' },
        30_000,
      ),
    ).resolves.toBe('CONTINUE');
    const checkpoint = await pool.query<{
      alarm_materialized_count: number;
      cursor_count: number;
      job_materialized_count: number;
      lease_generation: number;
      lease_token: string;
      next_cursor: string;
      recipient_count: number;
      run_generation: string;
    }>(
      `
        SELECT
          a.materialized_count AS alarm_materialized_count,
          j.materialized_count AS job_materialized_count,
          j.run_generation,
          j.lease_generation,
          j.lease_token,
          j.next_cursor,
          (
            SELECT count(*)::integer
            FROM alarm_expansion_recipients r
            WHERE r.alarm_id = j.alarm_id AND r.run_generation = j.run_generation
          ) AS recipient_count,
          (
            SELECT count(*)::integer
            FROM alarm_expansion_seen_cursors c
            WHERE c.alarm_id = j.alarm_id AND c.run_generation = j.run_generation
          ) AS cursor_count
        FROM alarms a
        JOIN alarm_expansion_jobs j ON j.alarm_id = a.id
        WHERE a.id = $1
      `,
      [alarmId],
    );
    expect(checkpoint.rows[0]).toEqual({
      alarm_materialized_count: 2,
      cursor_count: 2,
      job_materialized_count: 2,
      lease_generation: lease.leaseGeneration,
      lease_token: lease.leaseToken,
      next_cursor: 'checkpoint-a',
      recipient_count: 2,
      run_generation: lease.runGeneration,
    });

    await expect(
      expansions.savePage(lease, { recipientIds: [], nextCursor: null }, 30_000),
    ).resolves.toBe('COMPLETED');
    const published = await pool.query<{ count: number }>(
      'SELECT count(*)::integer AS count FROM deliveries WHERE alarm_id = $1',
      [alarmId],
    );
    expect(published.rows[0]?.count).toBe(2);
  });

  it('detects a cursor cycle and terminates without creating dispatchable rows', async () => {
    const alarmId = await startBulk();
    const lease = await expansions.claimNext(30_000);
    if (lease === null) {
      throw new Error('Expected an expansion lease');
    }

    await expansions.savePage(
      lease,
      { recipientIds: ['user-1'], nextCursor: 'cursor-cycle' },
      30_000,
    );
    await expect(
      expansions.savePage(lease, { recipientIds: ['user-2'], nextCursor: 'cursor-cycle' }, 30_000),
    ).resolves.toBe('COMPLETED');

    const alarm = await alarms.findById(alarmId);
    const deliveries = await pool.query<{ count: number }>(
      'SELECT count(*)::integer AS count FROM deliveries WHERE alarm_id = $1',
      [alarmId],
    );
    expect(alarm).toMatchObject({
      status: 'FAILED',
      failureCode: 'EXPANSION_FAILED',
      total: 0,
    });
    expect(deliveries.rows[0]?.count).toBe(0);
  });

  it('lets cancellation win at the next fenced page commit', async () => {
    const alarmId = await startBulk();
    const lease = await expansions.claimNext(30_000);
    if (lease === null) {
      throw new Error('Expected an expansion lease');
    }
    await alarms.cancel(alarmId);

    await expect(
      expansions.savePage(lease, { recipientIds: ['user-1'], nextCursor: null }, 30_000),
    ).resolves.toBe('CANCELED');

    const alarm = await alarms.findById(alarmId);
    expect(alarm).toMatchObject({ status: 'CANCELED', total: 0, active: 0 });
    const deliveries = await pool.query<{ count: number }>(
      'SELECT count(*)::integer AS count FROM deliveries WHERE alarm_id = $1',
      [alarmId],
    );
    expect(deliveries.rows[0]?.count).toBe(0);
  });

  it('CAN-11 rolls back a killed cancel finalizer and lets one takeover finalize plus durable cleanup', async () => {
    const alarmId = await startBulk();
    const lease = await expansions.claimNext(30_000);
    if (lease === null) {
      throw new Error('Expected CAN-11 expansion lease');
    }
    await expect(
      expansions.savePage(
        lease,
        { recipientIds: ['can11-staged-user'], nextCursor: 'can11-next' },
        30_000,
      ),
    ).resolves.toBe('CONTINUE');
    await alarms.cancel(alarmId);

    await pool.query(`
      CREATE FUNCTION reject_cancel_finalizer_job_commit() RETURNS trigger
      LANGUAGE plpgsql AS $$
      BEGIN
        RAISE EXCEPTION 'injected cancel finalizer crash before commit';
      END
      $$
    `);
    await pool.query(`
      CREATE TRIGGER reject_cancel_finalizer_job_commit_trigger
      BEFORE UPDATE OF state ON alarm_expansion_jobs
      FOR EACH ROW
      WHEN (OLD.state = 'RUNNING' AND NEW.state = 'CANCELED')
      EXECUTE FUNCTION reject_cancel_finalizer_job_commit()
    `);
    try {
      await expect(
        expansions.savePage(lease, { recipientIds: ['must-roll-back'], nextCursor: null }, 30_000),
      ).rejects.toThrow();
    } finally {
      await pool.query(
        'DROP TRIGGER reject_cancel_finalizer_job_commit_trigger ON alarm_expansion_jobs',
      );
      await pool.query('DROP FUNCTION reject_cancel_finalizer_job_commit()');
    }

    const rolledBack = await pool.query<{
      alarm_status: string;
      audience_finalized_at: Date | null;
      total: number | null;
      job_state: string;
      cleanup_state: string;
      lease_generation: number;
      lease_token: string;
      staged_count: number;
      delivery_count: number;
    }>(
      `
        SELECT
          a.status AS alarm_status,
          a.audience_finalized_at,
          a.total,
          j.state AS job_state,
          j.cleanup_state,
          j.lease_generation,
          j.lease_token,
          (
            SELECT count(*)::integer
            FROM alarm_expansion_recipients r
            WHERE r.alarm_id = j.alarm_id AND r.run_generation = j.run_generation
          ) AS staged_count,
          (SELECT count(*)::integer FROM deliveries d WHERE d.alarm_id = a.id)
            AS delivery_count
        FROM alarms a
        JOIN alarm_expansion_jobs j ON j.alarm_id = a.id
        WHERE a.id = $1
      `,
      [alarmId],
    );
    expect(rolledBack.rows[0]).toEqual({
      alarm_status: 'CANCEL_REQUESTED',
      audience_finalized_at: null,
      total: null,
      job_state: 'RUNNING',
      cleanup_state: 'NOT_REQUIRED',
      lease_generation: lease.leaseGeneration,
      lease_token: lease.leaseToken,
      staged_count: 1,
      delivery_count: 0,
    });

    await pool.query(
      `
        UPDATE alarm_expansion_jobs
        SET lease_until = clock_timestamp() - interval '1 second'
        WHERE alarm_id = $1
      `,
      [alarmId],
    );
    const takeoverResults = await Promise.all([
      expansions.claimNext(30_000),
      new PostgresExpansionRepository(pool).claimNext(30_000),
    ]);
    expect(takeoverResults).toEqual([null, null]);

    const finalized = await pool.query<{
      alarm_status: string;
      audience_finalized_at: Date;
      expansion_completed_at: Date | null;
      finished_at: Date;
      total: number;
      sent: number;
      failed: number;
      canceled: number;
      active: number;
      unresolved: number;
      job_state: string;
      cleanup_state: string;
      cleanup_run_generation: string;
      lease_token: string | null;
      staged_count: number;
      delivery_count: number;
    }>(
      `
        SELECT
          a.status AS alarm_status,
          a.audience_finalized_at,
          a.expansion_completed_at,
          a.finished_at,
          a.total,
          a.sent,
          a.failed,
          a.canceled,
          a.active,
          a.unresolved,
          j.state AS job_state,
          j.cleanup_state,
          j.cleanup_run_generation,
          j.lease_token,
          (
            SELECT count(*)::integer
            FROM alarm_expansion_recipients r
            WHERE r.alarm_id = j.alarm_id AND r.run_generation = j.run_generation
          ) AS staged_count,
          (SELECT count(*)::integer FROM deliveries d WHERE d.alarm_id = a.id)
            AS delivery_count
        FROM alarms a
        JOIN alarm_expansion_jobs j ON j.alarm_id = a.id
        WHERE a.id = $1
      `,
      [alarmId],
    );
    expect(finalized.rows[0]).toMatchObject({
      alarm_status: 'CANCELED',
      expansion_completed_at: null,
      total: 0,
      sent: 0,
      failed: 0,
      canceled: 0,
      active: 0,
      unresolved: 0,
      job_state: 'CANCELED',
      cleanup_state: 'PENDING',
      cleanup_run_generation: lease.runGeneration,
      lease_token: null,
      staged_count: 1,
      delivery_count: 0,
    });
    expect(finalized.rows[0]?.audience_finalized_at).toBeInstanceOf(Date);
    expect(finalized.rows[0]?.finished_at).toBeInstanceOf(Date);

    await expect(maintenance.cleanupExpansionBatch(1, 30_000)).resolves.toBe(true);
    for (let iteration = 0; iteration < 10; iteration += 1) {
      if (!(await maintenance.cleanupExpansionBatch(1, 30_000))) {
        break;
      }
    }
    const cleaned = await pool.query<{
      cleanup_state: string;
      staged_count: number;
      cursor_count: number;
    }>(
      `
        SELECT
          j.cleanup_state,
          (
            SELECT count(*)::integer
            FROM alarm_expansion_recipients r
            WHERE r.alarm_id = j.alarm_id AND r.run_generation = j.run_generation
          ) AS staged_count,
          (
            SELECT count(*)::integer
            FROM alarm_expansion_seen_cursors c
            WHERE c.alarm_id = j.alarm_id AND c.run_generation = j.run_generation
          ) AS cursor_count
        FROM alarm_expansion_jobs j
        WHERE j.alarm_id = $1
      `,
      [alarmId],
    );
    expect(cleaned.rows[0]).toEqual({
      cleanup_state: 'COMPLETED',
      staged_count: 0,
      cursor_count: 0,
    });
  });

  it('preserves the run while fencing writes from an expired lease owner', async () => {
    const alarmId = await startBulk();
    const first = await expansions.claimNext(30_000);
    if (first === null) {
      throw new Error('Expected the first expansion lease');
    }
    await pool.query(
      `UPDATE alarm_expansion_jobs SET lease_until = clock_timestamp() - interval '1 second' WHERE alarm_id = $1`,
      [alarmId],
    );

    const takeover = await expansions.claimNext(30_000);
    if (takeover === null) {
      throw new Error('Expected a takeover expansion lease');
    }

    expect(takeover.runGeneration).toBe(first.runGeneration);
    expect(takeover.leaseGeneration).toBe(first.leaseGeneration + 1);
    await expect(
      expansions.savePage(first, { recipientIds: ['stale-user'], nextCursor: null }, 30_000),
    ).resolves.toBe('STALE');
    await expect(
      expansions.savePage(takeover, { recipientIds: ['current-user'], nextCursor: null }, 30_000),
    ).resolves.toBe('COMPLETED');

    const recipients = await pool.query<{ recipient_id: string }>(
      'SELECT recipient_id FROM deliveries WHERE alarm_id = $1',
      [alarmId],
    );
    expect(recipients.rows).toEqual([{ recipient_id: 'current-user' }]);
  });

  it('USR-15/PERF-04A atomically retries 10k final publication after a statement error', async () => {
    const recipientCount = 10_000;
    const alarmId = await startBulk();
    const lease = await expansions.claimNext(30_000);
    if (lease === null) {
      throw new Error('Expected a publication lease');
    }

    await pool.query(
      `
        INSERT INTO alarm_expansion_recipients(alarm_id, run_generation, recipient_id)
        SELECT $1, $2, 'scale-user-' || lpad(n::text, 5, '0')
        FROM generate_series(1, $3::integer) AS generated(n)
      `,
      [alarmId, lease.runGeneration, recipientCount],
    );
    await pool.query(
      `
        UPDATE alarms
        SET materialized_count = $2
        WHERE id = $1
      `,
      [alarmId, recipientCount],
    );
    await pool.query(
      `
        UPDATE alarm_expansion_jobs
        SET next_cursor = 'scale-checkpoint', materialized_count = $2
        WHERE alarm_id = $1
      `,
      [alarmId, recipientCount],
    );
    await pool.query(
      `
        INSERT INTO deliveries(
          id, alarm_id, recipient_id, client_ref, state, priority, alarm_created_at
        )
        SELECT
          gen_random_uuid(), id, 'scale-user-00001', gen_random_uuid(), 'READY', 10, created_at
        FROM alarms
        WHERE id = $1
      `,
      [alarmId],
    );

    await expect(
      expansions.savePage(lease, { recipientIds: [], nextCursor: null }, 30_000),
    ).rejects.toThrow();

    const rolledBack = await pool.query<{
      active: number;
      alarm_materialized_count: number;
      alarm_status: string;
      audience_finalized_at: Date | null;
      cleanup_run_generation: string | null;
      cleanup_state: string;
      delivery_count: number;
      expansion_completed_at: Date | null;
      job_materialized_count: number;
      job_state: string;
      lease_active: boolean;
      lease_generation: number;
      lease_token: string;
      next_cursor: string;
      run_generation: string;
      staging_count: number;
      total: number | null;
    }>(
      `
        SELECT
          a.status AS alarm_status,
          a.materialized_count AS alarm_materialized_count,
          a.audience_finalized_at,
          a.expansion_completed_at,
          a.total,
          a.active,
          j.state AS job_state,
          j.run_generation,
          j.next_cursor,
          j.materialized_count AS job_materialized_count,
          j.lease_generation,
          j.lease_token,
          j.lease_until IS NOT NULL AS lease_active,
          j.cleanup_state,
          j.cleanup_run_generation,
          (
            SELECT count(*)::integer
            FROM alarm_expansion_recipients r
            WHERE r.alarm_id = a.id AND r.run_generation = j.run_generation
          ) AS staging_count,
          (SELECT count(*)::integer FROM deliveries d WHERE d.alarm_id = a.id) AS delivery_count
        FROM alarms a
        JOIN alarm_expansion_jobs j ON j.alarm_id = a.id
        WHERE a.id = $1
      `,
      [alarmId],
    );
    expect(rolledBack.rows[0]).toEqual({
      active: 0,
      alarm_materialized_count: recipientCount,
      alarm_status: 'EXPANDING',
      audience_finalized_at: null,
      cleanup_run_generation: null,
      cleanup_state: 'NOT_REQUIRED',
      delivery_count: 1,
      expansion_completed_at: null,
      job_materialized_count: recipientCount,
      job_state: 'RUNNING',
      lease_active: true,
      lease_generation: lease.leaseGeneration,
      lease_token: lease.leaseToken,
      next_cursor: 'scale-checkpoint',
      run_generation: lease.runGeneration,
      staging_count: recipientCount,
      total: null,
    });

    await pool.query('DELETE FROM deliveries WHERE alarm_id = $1', [alarmId]);
    await expect(
      expansions.savePage(lease, { recipientIds: [], nextCursor: null }, 30_000),
    ).resolves.toBe('COMPLETED');

    const published = await pool.query<{
      active: number;
      alarm_materialized_count: number;
      alarm_status: string;
      audience_finalized: boolean;
      cleanup_run_generation: string;
      cleanup_state: string;
      delivery_count: number;
      distinct_client_refs: number;
      distinct_recipients: number;
      expansion_completed: boolean;
      job_materialized_count: number;
      job_state: string;
      lease_released: boolean;
      ready_count: number;
      total: number;
    }>(
      `
        SELECT
          a.status AS alarm_status,
          a.materialized_count AS alarm_materialized_count,
          a.audience_finalized_at IS NOT NULL AS audience_finalized,
          a.expansion_completed_at IS NOT NULL AS expansion_completed,
          a.total,
          a.active,
          j.state AS job_state,
          j.materialized_count AS job_materialized_count,
          j.lease_token IS NULL AND j.lease_until IS NULL AS lease_released,
          j.cleanup_state,
          j.cleanup_run_generation,
          count(d.id)::integer AS delivery_count,
          count(d.id) FILTER (WHERE d.state = 'READY')::integer AS ready_count,
          count(DISTINCT d.recipient_id)::integer AS distinct_recipients,
          count(DISTINCT d.client_ref)::integer AS distinct_client_refs
        FROM alarms a
        JOIN alarm_expansion_jobs j ON j.alarm_id = a.id
        LEFT JOIN deliveries d ON d.alarm_id = a.id
        WHERE a.id = $1
        GROUP BY a.id, j.alarm_id
      `,
      [alarmId],
    );
    expect(published.rows[0]).toEqual({
      active: recipientCount,
      alarm_materialized_count: recipientCount,
      alarm_status: 'SENDING',
      audience_finalized: true,
      cleanup_run_generation: lease.runGeneration,
      cleanup_state: 'PENDING',
      delivery_count: recipientCount,
      distinct_client_refs: recipientCount,
      distinct_recipients: recipientCount,
      expansion_completed: true,
      job_materialized_count: recipientCount,
      job_state: 'COMPLETED',
      lease_released: true,
      ready_count: recipientCount,
      total: recipientCount,
    });
  }, 60_000);

  it('USR-01 expands exactly 2,001 users across three opaque-cursor pages behind the barrier', async () => {
    const alarmId = await startBulk();
    const firstCursor = 'opaque+/=:page-2';
    const secondCursor = 'opaque:/+=:page-3';
    const recipientIds = Array.from(
      { length: 2_001 },
      (_, index) => `usr-01-${String(index + 1).padStart(4, '0')}`,
    );
    const pages = [
      { recipientIds: recipientIds.slice(0, 1_000), nextCursor: firstCursor },
      { recipientIds: recipientIds.slice(1_000, 2_000), nextCursor: secondCursor },
      { recipientIds: recipientIds.slice(2_000), nextCursor: null },
    ];
    const calls: { readonly cursor: string | null; readonly limit: number }[] = [];
    let deliveryCountBeforeFinalization: number | undefined;
    const users: UserDirectory = {
      getPage: async (cursor, limit) => {
        calls.push({ cursor, limit });
        const page = pages[calls.length - 1];
        if (page === undefined) {
          throw new Error('Unexpected fourth users page request');
        }
        if (calls.length === 3) {
          const beforeFinalization = await pool.query<{ count: number }>(
            'SELECT count(*)::integer AS count FROM deliveries WHERE alarm_id = $1',
            [alarmId],
          );
          deliveryCountBeforeFinalization = beforeFinalization.rows[0]?.count;
        }
        return page;
      },
    };
    const processor = new ExpansionProcessor(expansions, users, {
      sleep: async () => undefined,
      random: () => 0,
    });

    await expect(processor.runOnce()).resolves.toBe(true);

    expect(calls).toEqual([
      { cursor: null, limit: 1_000 },
      { cursor: firstCursor, limit: 1_000 },
      { cursor: secondCursor, limit: 1_000 },
    ]);
    expect(deliveryCountBeforeFinalization).toBe(0);
    const published = await pool.query<{
      delivery_count: number;
      distinct_recipients: number;
      job_materialized_count: number;
      job_state: string;
    }>(
      `
        SELECT
          count(d.id)::integer AS delivery_count,
          count(DISTINCT d.recipient_id)::integer AS distinct_recipients,
          j.state AS job_state,
          j.materialized_count AS job_materialized_count
        FROM alarm_expansion_jobs j
        LEFT JOIN deliveries d ON d.alarm_id = j.alarm_id
        WHERE j.alarm_id = $1
        GROUP BY j.alarm_id
      `,
      [alarmId],
    );
    expect(published.rows[0]).toEqual({
      delivery_count: 2_001,
      distinct_recipients: 2_001,
      job_materialized_count: 2_001,
      job_state: 'COMPLETED',
    });
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'SENDING',
      materializedCount: 2_001,
      total: 2_001,
      active: 2_001,
    });
  });

  it.each(['INVALID_CURSOR', 'INVALID_REQUEST'] as const)(
    'USR-14 finalizes a required 400 %s envelope without retry, reset, delivery, or POST',
    async (code) => {
      const alarmId = await startBulk();
      const initialJob = await pool.query<{ run_generation: string }>(
        'SELECT run_generation FROM alarm_expansion_jobs WHERE alarm_id = $1',
        [alarmId],
      );
      const runGeneration = initialJob.rows[0]?.run_generation;
      if (runGeneration === undefined) {
        throw new Error('Expected an expansion job before processing');
      }

      const requests: { readonly method: string; readonly url: string }[] = [];
      let sleepCalls = 0;
      let randomCalls = 0;
      const users = new HttpUserDirectory({
        baseUrl: 'http://provider.test:4000',
        fetch: async (input, init) => {
          requests.push({ method: init?.method ?? 'GET', url: String(input) });
          return new Response(
            JSON.stringify({ code, message: `${code} provider detail must remain upstream` }),
            { status: 400, headers: { 'content-type': 'application/json' } },
          );
        },
      });
      const processor = new ExpansionProcessor(expansions, users, {
        maxFetchAttempts: 5,
        sleep: async () => {
          sleepCalls += 1;
        },
        random: () => {
          randomCalls += 1;
          return 0;
        },
      });

      await expect(processor.runOnce()).resolves.toBe(true);

      expect(requests).toHaveLength(1);
      expect(requests[0]?.method).toBe('GET');
      expect(new URL(requests[0]?.url ?? '').searchParams.get('limit')).toBe('1000');
      expect(requests.filter(({ method }) => method === 'POST')).toHaveLength(0);
      expect(sleepCalls).toBe(0);
      expect(randomCalls).toBe(0);

      const finalized = await pool.query<{
        active: number;
        alarm_materialized_count: number;
        alarm_status: string;
        audience_finalized: boolean;
        cleanup_run_generation: string;
        cleanup_state: string;
        delivery_count: number;
        expansion_completed_at: Date | null;
        failure_code: string;
        finished: boolean;
        job_materialized_count: number;
        job_state: string;
        lease_generation: number;
        lease_released: boolean;
        next_cursor: string | null;
        run_generation: string;
        staging_count: number;
        total: number;
      }>(
        `
          SELECT
            a.status AS alarm_status,
            a.failure_code,
            a.audience_finalized_at IS NOT NULL AS audience_finalized,
            a.expansion_completed_at,
            a.total,
            a.active,
            a.materialized_count AS alarm_materialized_count,
            a.finished_at IS NOT NULL AS finished,
            j.state AS job_state,
            j.run_generation,
            j.next_cursor,
            j.materialized_count AS job_materialized_count,
            j.lease_generation,
            j.lease_token IS NULL AND j.lease_until IS NULL AS lease_released,
            j.cleanup_state,
            j.cleanup_run_generation,
            (
              SELECT count(*)::integer
              FROM alarm_expansion_recipients r
              WHERE r.alarm_id = a.id AND r.run_generation = j.run_generation
            ) AS staging_count,
            (SELECT count(*)::integer FROM deliveries d WHERE d.alarm_id = a.id) AS delivery_count
          FROM alarms a
          JOIN alarm_expansion_jobs j ON j.alarm_id = a.id
          WHERE a.id = $1
        `,
        [alarmId],
      );
      expect(finalized.rows[0]).toEqual({
        active: 0,
        alarm_materialized_count: 0,
        alarm_status: 'FAILED',
        audience_finalized: true,
        cleanup_run_generation: runGeneration,
        cleanup_state: 'PENDING',
        delivery_count: 0,
        expansion_completed_at: null,
        failure_code: 'EXPANSION_FAILED',
        finished: true,
        job_materialized_count: 0,
        job_state: 'FAILED',
        lease_generation: 1,
        lease_released: true,
        next_cursor: null,
        run_generation: runGeneration,
        staging_count: 0,
        total: 0,
      });
    },
  );

  it('runs the complete multi-page use case against PostgreSQL', async () => {
    const alarmId = await startBulk();
    const pages = [
      { recipientIds: ['user-a'], nextCursor: 'next' },
      { recipientIds: ['user-b'], nextCursor: null },
    ];
    const users: UserDirectory = {
      getPage: async () => {
        const page = pages.shift();
        if (page === undefined) {
          throw new Error('Unexpected provider request');
        }
        return page;
      },
    };
    const processor = new ExpansionProcessor(expansions, users, {
      sleep: async () => undefined,
      random: () => 0,
    });

    await expect(processor.runOnce()).resolves.toBe(true);
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'SENDING',
      total: 2,
    });
  });
});
