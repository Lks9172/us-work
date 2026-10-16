import 'reflect-metadata';

import { AsyncLocalStorage } from 'node:async_hooks';
import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';

import { FastifyAdapter, type NestFastifyApplication } from '@nestjs/platform-fastify';
import { Test } from '@nestjs/testing';
import { Pool } from 'pg';
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

import { ApiModule } from '../../apps/api/src/api.module';
import { API_FASTIFY_OPTIONS } from '../../apps/api/src/http-server-config';
import { runMigrations } from '../../packages/alarms/src/infrastructure/persistence/migrations';
import { PostgresAlarmRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-alarm-repository';
import { PostgresDispatchRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-dispatch-repository';
import { PostgresExpansionRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-expansion-repository';
import { PostgresMaintenanceRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-maintenance-repository';
import { createTestDatabase, type TestDatabase } from './postgres-test-schema';

const databaseUrl = process.env.TEST_DATABASE_URL;
const describeWithDatabase = databaseUrl === undefined ? describe.skip : describe.sequential;

interface ExplainNode {
  readonly 'Actual Rows'?: number;
  readonly 'Execution Time'?: number;
  readonly 'Index Name'?: string;
  readonly 'Shared Hit Blocks'?: number;
  readonly 'Temp Read Blocks'?: number;
  readonly 'Temp Written Blocks'?: number;
  readonly Plans?: readonly ExplainNode[];
}

interface ExplainDocument {
  readonly Plan: ExplainNode;
  readonly 'Execution Time': number;
}

function collectIndexNames(node: ExplainNode): string[] {
  return [
    ...(node['Index Name'] === undefined ? [] : [node['Index Name']]),
    ...(node.Plans ?? []).flatMap(collectIndexNames),
  ];
}

function sumPlanMetric(
  node: ExplainNode,
  metric: 'Temp Read Blocks' | 'Temp Written Blocks',
): number {
  return (
    (node[metric] ?? 0) +
    (node.Plans ?? []).reduce((sum, child) => sum + sumPlanMetric(child, metric), 0)
  );
}

function percentile95(samples: readonly number[]): number {
  const sorted = [...samples].sort((left, right) => left - right);
  const index = Math.max(0, Math.ceil(sorted.length * 0.95) - 1);
  const value = sorted[index];
  if (value === undefined) {
    throw new Error('Cannot calculate a percentile without samples');
  }
  return value;
}

function auditMetric(name: string, value: unknown): void {
  if (process.env.PERF_AUDIT_LOG === '1') {
    console.info(`PERF_AUDIT ${JSON.stringify({ name, value })}`);
  }
}

describeWithDatabase('PostgreSQL 100k and database-clock performance contracts', () => {
  let application: NestFastifyApplication;
  let testDatabase: TestDatabase;
  let pool: Pool;
  let alarms: PostgresAlarmRepository;
  let dispatch: PostgresDispatchRepository;
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
    process.env.DATABASE_URL = testDatabase.databaseUrl;
    process.env.CURSOR_SIGNING_KEY = 'database-performance-e2e-cursor-key';

    pool = new Pool({ connectionString: testDatabase.databaseUrl, max: 48 });
    alarms = new PostgresAlarmRepository(pool);
    dispatch = new PostgresDispatchRepository(pool);
    expansions = new PostgresExpansionRepository(pool);
    maintenance = new PostgresMaintenanceRepository(pool);

    const module = await Test.createTestingModule({ imports: [ApiModule] }).compile();
    application = module.createNestApplication<NestFastifyApplication>(
      new FastifyAdapter(API_FASTIFY_OPTIONS),
    );
    await application.init();
    await application.getHttpAdapter().getInstance().ready();
  });

  beforeEach(async () => {
    await pool.query('TRUNCATE alarms CASCADE');
  });

  afterAll(async () => {
    vi.restoreAllMocks();
    await application?.close();
    await pool?.end();
    await testDatabase?.cleanup();
    delete process.env.DATABASE_URL;
    delete process.env.CURSOR_SIGNING_KEY;
  });

  async function startBulkWithStaging(recipientCount: number): Promise<{
    readonly alarmId: string;
    readonly lease: NonNullable<Awaited<ReturnType<PostgresExpansionRepository['claimNext']>>>;
  }> {
    const alarmId = randomUUID();
    await alarms.create({
      id: alarmId,
      type: 'BULK',
      title: '100k publication fixture',
      body: 'body',
      recipientIds: [],
    });
    await alarms.start(alarmId);
    const lease = await expansions.claimNext(300_000);
    if (lease === null || lease.alarmId !== alarmId) {
      throw new Error('Expected the 100k publication lease');
    }

    await pool.query(
      `
        INSERT INTO alarm_expansion_recipients(alarm_id, run_generation, recipient_id)
        SELECT $1, $2, 'perf-user-' || lpad(n::text, 6, '0')
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
        SET next_cursor = 'perf-final-page', materialized_count = $2
        WHERE alarm_id = $1
      `,
      [alarmId, recipientCount],
    );

    return { alarmId, lease };
  }

  async function expectUnpublished(alarmId: string, recipientCount: number): Promise<void> {
    const result = await pool.query<{
      readonly active: number;
      readonly audience_finalized_at: Date | null;
      readonly cleanup_run_generation: string | null;
      readonly cleanup_state: string;
      readonly delivery_count: number;
      readonly expansion_completed_at: Date | null;
      readonly job_state: string;
      readonly lease_active: boolean;
      readonly staging_count: number;
      readonly status: string;
      readonly total: number | null;
    }>(
      `
        SELECT
          a.status,
          a.audience_finalized_at,
          a.expansion_completed_at,
          a.total,
          a.active,
          j.state AS job_state,
          j.cleanup_state,
          j.cleanup_run_generation,
          j.lease_token IS NOT NULL AND j.lease_until IS NOT NULL AS lease_active,
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

    expect(result.rows[0]).toEqual({
      active: 0,
      audience_finalized_at: null,
      cleanup_run_generation: null,
      cleanup_state: 'NOT_REQUIRED',
      delivery_count: 0,
      expansion_completed_at: null,
      job_state: 'RUNNING',
      lease_active: true,
      staging_count: recipientCount,
      status: 'EXPANDING',
      total: null,
    });
  }

  async function waitForFinalizationFailpoint(): Promise<void> {
    for (let attempt = 0; attempt < 200; attempt += 1) {
      const activity = await pool.query<{ readonly reached: boolean }>(`
        SELECT EXISTS (
          SELECT 1
          FROM pg_stat_activity
          WHERE datname = current_database()
            AND pid <> pg_backend_pid()
            AND wait_event = 'PgSleep'
            AND query LIKE '%INSERT INTO deliveries%'
        ) AS reached
      `);
      if (activity.rows[0]?.reached === true) {
        return;
      }
      await new Promise((resolveDelay) => setTimeout(resolveDelay, 5));
    }
    throw new Error('Timed out waiting for the finalization transaction failpoint');
  }

  async function measureRowLockWait(query: string, parameter: string): Promise<number> {
    const client = await pool.connect();
    const startedAt = performance.now();
    try {
      await client.query('BEGIN');
      await client.query(query, [parameter]);
      await client.query('ROLLBACK');
      return performance.now() - startedAt;
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  it('PERF-04A/B rolls back three 100k publication failures, then publishes exactly once within the declared budget', async () => {
    const recipientCount = 100_000;
    const { alarmId, lease } = await startBulkWithStaging(recipientCount);

    const otherAlarmId = randomUUID();
    await alarms.create({
      id: otherAlarmId,
      type: 'EMERGENCY',
      title: 'independent alarm lock probe',
      body: 'body',
      recipientIds: ['independent-recipient'],
    });
    await alarms.start(otherAlarmId);

    await pool.query(`
      CREATE FUNCTION perf04a_reject_alarm_finalize() RETURNS trigger
      LANGUAGE plpgsql AS $$
      BEGIN
        PERFORM pg_sleep(0.75);
        RAISE EXCEPTION 'injected transaction failure after delivery insert';
      END
      $$
    `);
    await pool.query(`
      CREATE TRIGGER perf04a_reject_alarm_finalize_trigger
      BEFORE UPDATE OF audience_finalized_at ON alarms
      FOR EACH ROW
      WHEN (OLD.audience_finalized_at IS NULL AND NEW.audience_finalized_at IS NOT NULL)
      EXECUTE FUNCTION perf04a_reject_alarm_finalize()
    `);
    // Observe the expected rejection immediately. Under a saturated parallel run the injected
    // 750 ms failure can settle before the lock/read probes below finish; attaching the handler
    // only afterwards makes Node report a transient unhandled rejection even though the test
    // eventually asserts it.
    const failedFinalization = expansions
      .savePage(lease, { recipientIds: [], nextCursor: null }, 300_000)
      .then(
        () => ({ outcome: 'resolved' as const }),
        (error: unknown) => ({ error, outcome: 'rejected' as const }),
      );
    await waitForFinalizationFailpoint();
    const alarmLockWait = measureRowLockWait(
      'SELECT id FROM alarms WHERE id = $1 FOR UPDATE',
      alarmId,
    );
    const jobLockWait = measureRowLockWait(
      'SELECT alarm_id FROM alarm_expansion_jobs WHERE alarm_id = $1 FOR UPDATE',
      alarmId,
    );
    const detailStartedAt = performance.now();
    const detailResponse = await application.inject({
      method: 'GET',
      url: `/v1/alarms/${alarmId}`,
    });
    const detailMs = performance.now() - detailStartedAt;
    expect(detailResponse.statusCode).toBe(200);
    expect(detailResponse.json<{ readonly status: string }>().status).toBe('EXPANDING');
    expect(detailMs).toBeLessThan(500);
    const independentClaimStartedAt = performance.now();
    const independentClaim = await dispatch.claimNext(0, 30_000);
    const independentClaimMs = performance.now() - independentClaimStartedAt;
    expect(independentClaim?.alarmId).toBe(otherAlarmId);
    expect(independentClaimMs).toBeLessThan(500);
    await expect(failedFinalization).resolves.toMatchObject({
      error: { code: 'PERSISTENCE_UNAVAILABLE' },
      outcome: 'rejected',
    });
    const [alarmLockWaitMs, jobLockWaitMs] = await Promise.all([alarmLockWait, jobLockWait]);
    expect(alarmLockWaitMs).toBeGreaterThan(200);
    expect(alarmLockWaitMs).toBeLessThan(2_000);
    expect(jobLockWaitMs).toBeGreaterThan(200);
    expect(jobLockWaitMs).toBeLessThan(2_000);
    await pool.query('DROP TRIGGER perf04a_reject_alarm_finalize_trigger ON alarms');
    await pool.query('DROP FUNCTION perf04a_reject_alarm_finalize()');
    await expectUnpublished(alarmId, recipientCount);

    await pool.query(`
      CREATE FUNCTION perf04a_slow_delivery_insert() RETURNS trigger
      LANGUAGE plpgsql AS $$
      BEGIN
        PERFORM pg_sleep(0.001);
        RETURN NEW;
      END
      $$
    `);
    await pool.query(`
      CREATE TRIGGER perf04a_slow_delivery_insert_trigger
      BEFORE INSERT ON deliveries
      FOR EACH ROW EXECUTE FUNCTION perf04a_slow_delivery_insert()
    `);
    const timeoutUrl = new URL(testDatabase.databaseUrl);
    const existingOptions = timeoutUrl.searchParams.get('options') ?? '';
    timeoutUrl.searchParams.set('options', `${existingOptions} -cstatement_timeout=25`.trim());
    const timeoutPool = new Pool({ connectionString: timeoutUrl.toString(), max: 1 });
    try {
      await expect(
        new PostgresExpansionRepository(timeoutPool).savePage(
          lease,
          { recipientIds: [], nextCursor: null },
          300_000,
        ),
      ).rejects.toMatchObject({ code: 'PERSISTENCE_UNAVAILABLE' });
    } finally {
      await timeoutPool.end();
      await pool.query('DROP TRIGGER perf04a_slow_delivery_insert_trigger ON deliveries');
      await pool.query('DROP FUNCTION perf04a_slow_delivery_insert()');
    }
    await expectUnpublished(alarmId, recipientCount);

    await pool.query(
      `
        INSERT INTO deliveries(
          id, alarm_id, recipient_id, client_ref, state, priority, alarm_created_at
        )
        SELECT
          gen_random_uuid(), id, 'perf-user-000001', gen_random_uuid(), 'READY', 10, created_at
        FROM alarms
        WHERE id = $1
      `,
      [alarmId],
    );
    await expect(
      expansions.savePage(lease, { recipientIds: [], nextCursor: null }, 300_000),
    ).rejects.toMatchObject({ code: 'PERSISTENCE_UNAVAILABLE' });
    const conflictRollback = await pool.query<{ readonly count: number }>(
      'SELECT count(*)::integer AS count FROM deliveries WHERE alarm_id = $1',
      [alarmId],
    );
    expect(conflictRollback.rows[0]?.count).toBe(1);
    await pool.query('DELETE FROM deliveries WHERE alarm_id = $1', [alarmId]);
    await expectUnpublished(alarmId, recipientCount);

    const explainClient = await pool.connect();
    let publicationPlan: ExplainDocument | undefined;
    try {
      await explainClient.query('BEGIN');
      const explained = await explainClient.query<{
        readonly 'QUERY PLAN': readonly ExplainDocument[];
      }>(
        `
          EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON)
          WITH inserted AS (
            INSERT INTO deliveries(
              id, alarm_id, recipient_id, client_ref, state, priority, alarm_created_at
            )
            SELECT
              gen_random_uuid(), a.id, r.recipient_id, gen_random_uuid(),
              'READY', 10, a.created_at
            FROM alarms a
            JOIN alarm_expansion_recipients r
              ON r.alarm_id = a.id AND r.run_generation = $2
            WHERE a.id = $1
            RETURNING 1
          ), audience AS (
            SELECT count(*)::integer AS total
            FROM alarm_expansion_recipients
            WHERE alarm_id = $1 AND run_generation = $2
          )
          UPDATE alarms a
          SET
            status = CASE WHEN audience.total = 0 THEN 'COMPLETED' ELSE 'SENDING' END,
            failure_code = NULL,
            audience_finalized_at = clock_timestamp(),
            expansion_completed_at = clock_timestamp(),
            total = audience.total,
            active = audience.total,
            finished_at = CASE WHEN audience.total = 0 THEN clock_timestamp() ELSE NULL END,
            updated_at = clock_timestamp(),
            version = version + 1
          FROM audience
          WHERE a.id = $1
          RETURNING audience.total
        `,
        [alarmId, lease.runGeneration],
      );
      publicationPlan = explained.rows[0]?.['QUERY PLAN'][0];
      await explainClient.query('ROLLBACK');
    } catch (error) {
      await explainClient.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      explainClient.release();
    }
    if (publicationPlan === undefined) {
      throw new Error('Expected the PERF-04B publication query plan');
    }
    expect(publicationPlan.Plan['Actual Rows']).toBe(1);
    expect(publicationPlan['Execution Time']).toBeLessThan(30_000);
    expect(sumPlanMetric(publicationPlan.Plan, 'Temp Read Blocks')).toBe(0);
    expect(sumPlanMetric(publicationPlan.Plan, 'Temp Written Blocks')).toBe(0);

    const walBefore = await pool.query<{ readonly lsn: string }>(
      'SELECT pg_current_wal_lsn()::text AS lsn',
    );
    const publicationStartedAt = performance.now();
    await expect(
      expansions.savePage(lease, { recipientIds: [], nextCursor: null }, 300_000),
    ).resolves.toBe('COMPLETED');
    const publicationMs = performance.now() - publicationStartedAt;
    const wal = await pool.query<{ readonly bytes: string }>(
      `SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), $1::pg_lsn)::text AS bytes`,
      [walBefore.rows[0]?.lsn],
    );
    const walBytes = Number(wal.rows[0]?.bytes);

    const published = await pool.query<{
      readonly active: number;
      readonly audience_finalized: boolean;
      readonly cleanup_state: string;
      readonly delivery_count: number;
      readonly distinct_client_refs: number;
      readonly distinct_recipients: number;
      readonly expansion_completed: boolean;
      readonly job_state: string;
      readonly lease_released: boolean;
      readonly status: string;
      readonly total: number;
    }>(
      `
        SELECT
          a.status,
          a.audience_finalized_at IS NOT NULL AS audience_finalized,
          a.expansion_completed_at IS NOT NULL AS expansion_completed,
          a.total,
          a.active,
          j.state AS job_state,
          j.cleanup_state,
          j.lease_token IS NULL AND j.lease_until IS NULL AS lease_released,
          count(d.id)::integer AS delivery_count,
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
      audience_finalized: true,
      cleanup_state: 'PENDING',
      delivery_count: recipientCount,
      distinct_client_refs: recipientCount,
      distinct_recipients: recipientCount,
      expansion_completed: true,
      job_state: 'COMPLETED',
      lease_released: true,
      status: 'SENDING',
      total: recipientCount,
    });
    expect(publicationMs).toBeLessThan(30_000);
    expect(walBytes).toBeGreaterThan(0);
    expect(walBytes).toBeLessThan(256 * 1024 * 1024);
    auditMetric('PERF-04B', {
      alarmLockWaitMs,
      detailDuringPublicationMs: detailMs,
      independentAlarmClaimMs: independentClaimMs,
      jobLockWaitMs,
      publicationExecutionPlan: publicationPlan,
      publicationMs,
      walBytes,
    });
  }, 120_000);

  it('DB-09/PERF-01 paginates every failure once and audits the 100k aggregate plus index plan', async () => {
    const alarmId = randomUUID();
    await pool.query(
      `
        INSERT INTO alarms(
          id, type, title, body, status, audience_finalized_at, total,
          sent, failed, canceled, active, unresolved, finished_at
        )
        VALUES (
          $1, 'BULK', 'db-09 fixture', 'body', 'ATTENTION_REQUIRED', clock_timestamp(), 100000,
          50000, 30000, 10000, 0, 10000, clock_timestamp()
        )
      `,
      [alarmId],
    );
    await pool.query(
      `
        INSERT INTO deliveries(
          id, alarm_id, recipient_id, client_ref, state, priority, alarm_created_at
        )
        SELECT
          gen_random_uuid(),
          a.id,
          'db09-user-' || lpad(n::text, 6, '0'),
          gen_random_uuid(),
          CASE
            WHEN n <= 50000 THEN 'SENT'
            WHEN n <= 70000 THEN 'FAILED_PERMANENT'
            WHEN n <= 80000 THEN 'FAILED_RETRY_EXHAUSTED'
            WHEN n <= 90000 THEN 'CANCELED'
            ELSE 'NEEDS_REVIEW'
          END,
          10,
          a.created_at
        FROM alarms a
        CROSS JOIN generate_series(1, 100000) AS generated(n)
        WHERE a.id = $1
      `,
      [alarmId],
    );
    await pool.query('ANALYZE alarms');
    await pool.query('ANALYZE deliveries');

    const aggregateStartedAt = performance.now();
    const aggregate = await pool.query<{ readonly count: number; readonly state: string }>(
      `
        SELECT state, count(*)::integer AS count
        FROM deliveries
        WHERE alarm_id = $1
        GROUP BY state
        ORDER BY state
      `,
      [alarmId],
    );
    const aggregateMs = performance.now() - aggregateStartedAt;
    expect(aggregate.rows).toEqual([
      { count: 10_000, state: 'CANCELED' },
      { count: 20_000, state: 'FAILED_PERMANENT' },
      { count: 10_000, state: 'FAILED_RETRY_EXHAUSTED' },
      { count: 10_000, state: 'NEEDS_REVIEW' },
      { count: 50_000, state: 'SENT' },
    ]);
    expect(aggregateMs).toBeLessThan(5_000);
    const aggregatePlanResult = await pool.query<{
      readonly 'QUERY PLAN': readonly ExplainDocument[];
    }>(
      `
        EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
        SELECT state, count(*)::integer AS count
        FROM deliveries
        WHERE alarm_id = $1
        GROUP BY state
        ORDER BY state
      `,
      [alarmId],
    );
    const aggregatePlan = aggregatePlanResult.rows[0]?.['QUERY PLAN'][0];
    if (aggregatePlan === undefined) {
      throw new Error('Expected the PERF-01 aggregate query plan');
    }
    expect(aggregatePlan['Execution Time']).toBeLessThan(5_000);

    const seen = new Set<string>();
    let cursor = '00000000-0000-0000-0000-000000000000';
    const failurePaginationStartedAt = performance.now();
    for (;;) {
      const page = await pool.query<{ readonly id: string }>(
        `
          SELECT id
          FROM deliveries
          WHERE alarm_id = $1
            AND state IN ('FAILED_PERMANENT', 'FAILED_RETRY_EXHAUSTED')
            AND id > $2::uuid
          ORDER BY id
          LIMIT 1000
        `,
        [alarmId, cursor],
      );
      if (page.rows.length === 0) {
        break;
      }
      for (const row of page.rows) {
        expect(seen.has(row.id)).toBe(false);
        seen.add(row.id);
      }
      cursor = page.rows.at(-1)?.id ?? cursor;
    }
    const failurePaginationMs = performance.now() - failurePaginationStartedAt;
    expect(seen.size).toBe(30_000);
    expect(failurePaginationMs).toBeLessThan(5_000);

    const explained = await pool.query<{ readonly 'QUERY PLAN': readonly ExplainDocument[] }>(
      `
        EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
        SELECT id
        FROM deliveries
        WHERE alarm_id = $1
          AND state IN ('FAILED_PERMANENT', 'FAILED_RETRY_EXHAUSTED')
          AND id > $2::uuid
        ORDER BY id
        LIMIT 1000
      `,
      [alarmId, '00000000-0000-0000-0000-000000000000'],
    );
    const plan = explained.rows[0]?.['QUERY PLAN'][0];
    if (plan === undefined) {
      throw new Error('Expected the DB-09 JSON query plan');
    }
    expect(collectIndexNames(plan.Plan)).toContain('deliveries_alarm_page_idx');
    expect(plan.Plan['Actual Rows']).toBe(1_000);
    expect(plan.Plan['Shared Hit Blocks']).toBeLessThan(10_000);
    expect(plan['Execution Time']).toBeLessThan(5_000);

    await pool.query(
      `
        WITH selected AS (
          SELECT id, row_number() OVER (ORDER BY id) AS ordinal
          FROM deliveries
          WHERE alarm_id = $1 AND state = 'SENT'
          ORDER BY id
          LIMIT 3
        )
        UPDATE deliveries d
        SET
          state = CASE selected.ordinal
            WHEN 1 THEN 'READY'
            WHEN 2 THEN 'UNKNOWN'
            ELSE 'CLAIMED'
          END,
          next_action_at = clock_timestamp() - interval '1 second',
          lease_generation = CASE WHEN selected.ordinal = 3 THEN 1 ELSE 0 END,
          lease_token = CASE WHEN selected.ordinal = 3 THEN gen_random_uuid() ELSE NULL END,
          lease_until = CASE
            WHEN selected.ordinal = 3 THEN clock_timestamp() - interval '1 second'
            ELSE NULL
          END
        FROM selected
        WHERE d.id = selected.id
      `,
      [alarmId],
    );
    await pool.query(
      `
        UPDATE alarms
        SET status = 'SENDING', sent = sent - 3, active = active + 3
        WHERE id = $1
      `,
      [alarmId],
    );
    await pool.query('ANALYZE deliveries');

    const dispatchPlanResult = await pool.query<{
      readonly 'QUERY PLAN': readonly ExplainDocument[];
    }>(
      `
        EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
        SELECT d.id
        FROM deliveries d
        JOIN alarms a ON a.id = d.alarm_id
        WHERE d.state IN ('READY', 'RETRY_WAIT')
          AND d.next_action_at <= clock_timestamp()
          AND d.priority >= 0
          AND a.audience_finalized_at IS NOT NULL
          AND a.status = 'SENDING'
          AND a.cancel_requested_at IS NULL
        ORDER BY
          d.priority DESC,
          d.next_action_at ASC,
          d.alarm_created_at ASC,
          d.id ASC
        FOR UPDATE OF d SKIP LOCKED
        LIMIT 1
      `,
    );
    const dispatchPlan = dispatchPlanResult.rows[0]?.['QUERY PLAN'][0];
    if (dispatchPlan === undefined) {
      throw new Error('Expected the PERF-01 dispatch query plan');
    }
    expect(collectIndexNames(dispatchPlan.Plan)).toContain('deliveries_dispatch_claim_idx');
    expect(dispatchPlan.Plan['Shared Hit Blocks']).toBeLessThan(100);
    expect(dispatchPlan['Execution Time']).toBeLessThan(250);

    const reconciliationPlanResult = await pool.query<{
      readonly 'QUERY PLAN': readonly ExplainDocument[];
    }>(`
      EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
      SELECT d.id, d.state
      FROM deliveries d
      WHERE (
        d.state = 'UNKNOWN' AND d.next_action_at <= clock_timestamp()
      ) OR (
        d.state = 'RECONCILING' AND d.reconcile_until <= clock_timestamp()
      ) OR (
        d.state = 'NEEDS_REVIEW'
        AND d.next_review_at <= clock_timestamp()
        AND d.review_until > clock_timestamp()
        AND (d.reconcile_until IS NULL OR d.reconcile_until <= clock_timestamp())
      )
      ORDER BY
        CASE WHEN d.state IN ('UNKNOWN', 'RECONCILING') THEN 0 ELSE 1 END,
        COALESCE(d.next_review_at, d.next_action_at),
        d.id
      FOR UPDATE SKIP LOCKED
      LIMIT 1
    `);
    const reconciliationPlan = reconciliationPlanResult.rows[0]?.['QUERY PLAN'][0];
    if (reconciliationPlan === undefined) {
      throw new Error('Expected the PERF-01 reconciliation query plan');
    }
    expect(collectIndexNames(reconciliationPlan.Plan)).toContain('deliveries_reconcile_claim_idx');
    expect(reconciliationPlan['Execution Time']).toBeLessThan(250);

    const expiredLeasePlanResult = await pool.query<{
      readonly 'QUERY PLAN': readonly ExplainDocument[];
    }>(`
      EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
      SELECT id, alarm_id
      FROM deliveries
      WHERE state IN ('CLAIMED', 'IN_FLIGHT')
        AND lease_until <= clock_timestamp()
      ORDER BY lease_until, id
      LIMIT 1
    `);
    const expiredLeasePlan = expiredLeasePlanResult.rows[0]?.['QUERY PLAN'][0];
    if (expiredLeasePlan === undefined) {
      throw new Error('Expected the PERF-01 expired-lease query plan');
    }
    expect(collectIndexNames(expiredLeasePlan.Plan)).toContain('deliveries_send_lease_idx');
    expect(expiredLeasePlan.Plan['Shared Hit Blocks']).toBeLessThan(100);
    expect(expiredLeasePlan['Execution Time']).toBeLessThan(250);

    auditMetric('DB-09/PERF-01', {
      aggregateMs,
      aggregatePlan,
      dispatchPlan,
      expiredLeasePlan,
      failurePagePlan: plan,
      failurePaginationMs,
      reconciliationPlan,
    });
  }, 60_000);

  it('DB-10 ignores +/-10 second process-clock skew for due work and lease expiry', async () => {
    const alarmId = randomUUID();
    const deliveryId = randomUUID();
    await pool.query(
      `
        INSERT INTO alarms(
          id, type, title, body, status, audience_finalized_at, total, active
        )
        VALUES ($1, 'EMERGENCY', 'db-10 fixture', 'body', 'SENDING', clock_timestamp(), 1, 1)
      `,
      [alarmId],
    );
    await pool.query(
      `
        INSERT INTO deliveries(
          id, alarm_id, recipient_id, client_ref, state, priority,
          next_action_at, alarm_created_at
        )
        SELECT
          $2, id, 'db10-recipient', gen_random_uuid(), 'READY', 100,
          clock_timestamp() + interval '250 milliseconds', created_at
        FROM alarms
        WHERE id = $1
      `,
      [alarmId, deliveryId],
    );

    const realNow = Date.now.bind(Date);
    const dateNow = vi.spyOn(Date, 'now');
    try {
      dateNow.mockImplementation(() => realNow() + 10_000);
      await expect(dispatch.claimNext(0, 250)).resolves.toBeNull();

      await pool.query('SELECT pg_sleep(0.3)');
      dateNow.mockImplementation(() => realNow() - 10_000);
      const dueClaim = await dispatch.claimNext(0, 250);
      expect(dueClaim?.deliveryId).toBe(deliveryId);

      dateNow.mockImplementation(() => realNow() + 10_000);
      await expect(maintenance.reapExpiredSendLease()).resolves.toBeNull();

      await pool.query('SELECT pg_sleep(0.3)');
      dateNow.mockImplementation(() => realNow() - 10_000);
      await expect(maintenance.reapExpiredSendLease()).resolves.not.toBeNull();
    } finally {
      dateNow.mockRestore();
    }

    const processClockOffset = new AsyncLocalStorage<number>();
    const contenderClock = vi
      .spyOn(Date, 'now')
      .mockImplementation(() => realNow() + (processClockOffset.getStore() ?? 0));
    let contenders: Awaited<ReturnType<PostgresDispatchRepository['claimNext']>>[];
    try {
      contenders = await Promise.all(
        [-10_000, 0, 10_000].map((offset) =>
          processClockOffset.run(offset, () =>
            new PostgresDispatchRepository(pool).claimNext(0, 30_000),
          ),
        ),
      );
    } finally {
      contenderClock.mockRestore();
    }
    expect(contenders.filter((claim) => claim !== null)).toHaveLength(1);
    expect(contenders.find((claim) => claim !== null)?.deliveryId).toBe(deliveryId);

    const persisted = await pool.query<{
      readonly lease_generation: number;
      readonly state: string;
    }>('SELECT state, lease_generation FROM deliveries WHERE id = $1', [deliveryId]);
    expect(persisted.rows[0]).toEqual({ lease_generation: 2, state: 'CLAIMED' });
    auditMetric('DB-10', { contenderCount: contenders.length, winningClaims: 1 });
  });

  it('CAN-10 keeps cancel API p95 below 500ms with 100k deliveries and 2,000 STARTED attempts', async () => {
    const alarmId = randomUUID();
    await pool.query(
      `
        INSERT INTO alarms(
          id, type, title, body, status, audience_finalized_at, total, active
        )
        VALUES ($1, 'BULK', 'can-10 fixture', 'body', 'SENDING', clock_timestamp(), 100000, 100000)
      `,
      [alarmId],
    );
    await pool.query(
      `
        INSERT INTO deliveries(
          id, alarm_id, recipient_id, client_ref, state, priority, alarm_created_at
        )
        SELECT
          gen_random_uuid(),
          a.id,
          'can10-user-' || lpad(n::text, 6, '0'),
          gen_random_uuid(),
          'READY',
          10,
          a.created_at
        FROM alarms a
        CROSS JOIN generate_series(1, 100000) AS generated(n)
        WHERE a.id = $1
      `,
      [alarmId],
    );
    await pool.query(
      `
        WITH selected AS (
          SELECT id, row_number() OVER (ORDER BY id) AS attempt_no
          FROM deliveries
          WHERE alarm_id = $1
          ORDER BY id
          LIMIT 2000
        ), inserted AS (
          INSERT INTO delivery_attempts(
            id, delivery_id, attempt_no, state, lease_generation,
            limiter_reservation_id, request_phase, adapter_committed_at
          )
          SELECT
            gen_random_uuid(), id, 1, 'STARTED', 1,
            gen_random_uuid(), 'COMMITTED', clock_timestamp()
          FROM selected
          RETURNING id, delivery_id
        )
        UPDATE deliveries d
        SET
          state = 'IN_FLIGHT',
          attempt_no = 1,
          post_budget_count = 1,
          lease_generation = 1,
          lease_token = gen_random_uuid(),
          lease_until = clock_timestamp() + interval '10 minutes',
          current_attempt_id = i.id,
          updated_at = clock_timestamp()
        FROM inserted i
        WHERE d.id = i.delivery_id
      `,
      [alarmId],
    );
    const initial = await pool.query<{ readonly started: number }>(
      `
        SELECT count(*)::integer AS started
        FROM delivery_attempts da
        JOIN deliveries d ON d.id = da.delivery_id
        WHERE d.alarm_id = $1 AND da.state = 'STARTED'
      `,
      [alarmId],
    );
    expect(initial.rows[0]?.started).toBe(2_000);
    await pool.query('ANALYZE deliveries');

    const cancelLatencies: number[] = [];
    for (let sample = 0; sample < 20; sample += 1) {
      if (sample > 0) {
        await pool.query(
          `
            UPDATE alarms
            SET status = 'SENDING', cancel_requested_at = NULL
            WHERE id = $1 AND status = 'CANCEL_REQUESTED'
          `,
          [alarmId],
        );
      }
      const startedAt = performance.now();
      const response = await application.inject({
        method: 'POST',
        url: `/v1/alarms/${alarmId}/cancel`,
      });
      cancelLatencies.push(performance.now() - startedAt);
      expect(response.statusCode).toBe(202);
    }
    expect(percentile95(cancelLatencies)).toBeLessThan(500);
    auditMetric('CAN-10', {
      maxMs: Math.max(...cancelLatencies),
      minMs: Math.min(...cancelLatencies),
      p95Ms: percentile95(cancelLatencies),
      samples: cancelLatencies.length,
    });

    await pool.query(
      `
        UPDATE alarms
        SET status = 'SENDING', cancel_requested_at = NULL
        WHERE id = $1 AND status = 'CANCEL_REQUESTED'
      `,
      [alarmId],
    );
    let releaseBarrier: (() => void) | undefined;
    const barrier = new Promise<void>((resolveBarrier) => {
      releaseBarrier = resolveBarrier;
    });
    const startContenders = Array.from({ length: 64 }, async () => {
      await barrier;
      const claim = await new PostgresDispatchRepository(pool).claimNext(0, 30_000);
      if (claim === null) {
        return null;
      }
      return new PostgresDispatchRepository(pool).startAttempt(claim, {
        reservationId: randomUUID(),
        scheduledMonotonicMs: performance.now(),
      });
    });
    const racingCancel = (async () => {
      await barrier;
      return application.inject({
        method: 'POST',
        url: `/v1/alarms/${alarmId}/cancel`,
      });
    })();
    releaseBarrier?.();

    const [startResults, cancelResponse] = await Promise.all([
      Promise.all(startContenders),
      racingCancel,
    ]);
    expect(startResults).toHaveLength(64);
    expect(cancelResponse.statusCode).toBe(202);

    const result = await pool.query<{
      readonly active: number;
      readonly cancel_requested_at: Date;
      readonly canceled: number;
      readonly failed: number;
      readonly late_started: number;
      readonly sent: number;
      readonly status: string;
      readonly total: number;
      readonly unresolved: number;
    }>(
      `
        SELECT
          a.status,
          a.cancel_requested_at,
          a.total,
          a.sent,
          a.failed,
          a.canceled,
          a.active,
          a.unresolved,
          (
            SELECT count(*)::integer
            FROM delivery_attempts da
            JOIN deliveries d ON d.id = da.delivery_id
            WHERE d.alarm_id = a.id
              AND da.started_at > a.cancel_requested_at
          ) AS late_started
        FROM alarms a
        WHERE a.id = $1
      `,
      [alarmId],
    );
    const row = result.rows[0];
    expect(row?.status).toBe('CANCEL_REQUESTED');
    expect(row?.cancel_requested_at).toBeInstanceOf(Date);
    expect(row?.late_started).toBe(0);
    expect(
      (row?.sent ?? 0) +
        (row?.failed ?? 0) +
        (row?.canceled ?? 0) +
        (row?.active ?? 0) +
        (row?.unresolved ?? 0),
    ).toBe(row?.total);
  }, 90_000);
});
