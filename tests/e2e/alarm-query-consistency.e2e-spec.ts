import 'reflect-metadata';

import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';

import { FastifyAdapter, type NestFastifyApplication } from '@nestjs/platform-fastify';
import { Test } from '@nestjs/testing';
import { Pool, type PoolClient } from 'pg';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import { ApiModule } from '../../apps/api/src/api.module';
import { API_FASTIFY_OPTIONS } from '../../apps/api/src/http-server-config';
import type { DispatchAttempt } from '../../packages/alarms/src/application/dispatch';
import { runMigrations } from '../../packages/alarms/src/infrastructure/persistence/migrations';
import { PostgresDispatchRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-dispatch-repository';
import { PostgresMaintenanceRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-maintenance-repository';
import { createTestDatabase, type TestDatabase } from '../integration/postgres-test-schema';

const databaseUrl = process.env.TEST_DATABASE_URL;
const describeWithDatabase = databaseUrl === undefined ? describe.skip : describe;

interface IncidentCodeCountResponse {
  readonly code: string;
  readonly count: number;
}

interface AlarmDetailResponse {
  readonly id: string;
  readonly status: string;
  readonly total: number | null;
  readonly sent: number;
  readonly failed: number;
  readonly canceled: number;
  readonly active: number;
  readonly unresolved: number;
  readonly duplicateCount: number;
  readonly incidentCount: number;
  readonly incidentSummary: {
    readonly count: number;
    readonly codes: readonly IncidentCodeCountResponse[];
    readonly firstDetectedAt: string | null;
    readonly lastDetectedAt: string | null;
  };
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolveDelay) => setTimeout(resolveDelay, milliseconds));
}

async function within<T>(operation: Promise<T>, milliseconds: number): Promise<T> {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  const deadline = new Promise<never>((_, reject) => {
    timeout = setTimeout(
      () => reject(new Error(`Operation did not complete within ${milliseconds}ms`)),
      milliseconds,
    );
  });

  try {
    return await Promise.race([operation, deadline]);
  } finally {
    clearTimeout(timeout);
  }
}

function expectCounterInvariant(snapshot: AlarmDetailResponse): void {
  expect(snapshot.total).not.toBeNull();
  const total = snapshot.total;
  if (total === null) {
    throw new Error('Expected a finalized audience');
  }

  const resultCounts = [
    snapshot.sent,
    snapshot.failed,
    snapshot.canceled,
    snapshot.active,
    snapshot.unresolved,
  ];
  for (const count of resultCounts) {
    expect(Number.isInteger(count)).toBe(true);
    expect(count).toBeGreaterThanOrEqual(0);
    expect(count).toBeLessThanOrEqual(total);
  }
  expect(resultCounts.reduce((sum, count) => sum + count, 0)).toBe(total);
}

describeWithDatabase('Alarm query transaction consistency', () => {
  let application: NestFastifyApplication;
  let testDatabase: TestDatabase;
  let pool: Pool;
  let dispatch: PostgresDispatchRepository;
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
    process.env.CURSOR_SIGNING_KEY = 'query-consistency-e2e-cursor-key';

    pool = new Pool({ connectionString: testDatabase.databaseUrl, max: 24 });
    dispatch = new PostgresDispatchRepository(pool);
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
    await application?.close();
    await pool?.end();
    await testDatabase?.cleanup();
    delete process.env.DATABASE_URL;
    delete process.env.CURSOR_SIGNING_KEY;
  });

  async function createStartedEmergency(recipientCount: number): Promise<string> {
    const createResponse = await application.inject({
      method: 'POST',
      url: '/v1/alarms',
      payload: {
        type: 'EMERGENCY',
        title: `query-consistency-${randomUUID()}`,
        body: 'body',
        recipientIds: Array.from(
          { length: recipientCount },
          (_, index) => `query-user-${index}-${randomUUID()}`,
        ),
      },
    });
    expect(createResponse.statusCode).toBe(201);
    const alarmId = createResponse.json<{ id: string }>().id;

    const startResponse = await application.inject({
      method: 'POST',
      url: `/v1/alarms/${alarmId}/send`,
    });
    expect(startResponse.statusCode).toBe(202);

    return alarmId;
  }

  async function detail(alarmId: string): Promise<AlarmDetailResponse> {
    const response = await application.inject({
      method: 'GET',
      url: `/v1/alarms/${alarmId}`,
    });
    expect(response.statusCode).toBe(200);
    return response.json<AlarmDetailResponse>();
  }

  async function startCommittedAttempts(
    alarmId: string,
    count: number,
  ): Promise<DispatchAttempt[]> {
    const attempts: DispatchAttempt[] = [];
    for (let index = 0; index < count; index += 1) {
      const claim = await dispatch.claimNext(0, 60_000);
      if (claim === null || claim.alarmId !== alarmId) {
        throw new Error(`Expected dispatch claim ${index + 1} for ${alarmId}`);
      }
      const attempt = await dispatch.startAttempt(claim, {
        reservationId: randomUUID(),
        scheduledMonotonicMs: performance.now(),
      });
      if (attempt === null || !(await dispatch.commitAttempt(attempt))) {
        throw new Error(`Expected committed dispatch attempt ${index + 1}`);
      }
      attempts.push(attempt);
    }
    return attempts;
  }

  async function waitForAdvisoryWait(firstKey: number, secondKey: number): Promise<void> {
    const deadline = Date.now() + 3_000;
    while (Date.now() < deadline) {
      const waiting = await pool.query<{ waiting: boolean }>(
        `
          SELECT EXISTS (
            SELECT 1
            FROM pg_locks
            WHERE locktype = 'advisory'
              AND classid = $1::oid
              AND objid = $2::oid
              AND granted = false
          ) AS waiting
        `,
        [firstKey, secondKey],
      );
      if (waiting.rows[0]?.waiting === true) {
        return;
      }
      await delay(10);
    }
    throw new Error('Timed out waiting for the projection transaction barrier');
  }

  async function releaseAdvisoryLock(
    client: PoolClient,
    firstKey: number,
    secondKey: number,
  ): Promise<void> {
    await client.query('SELECT pg_advisory_unlock($1, $2)', [firstKey, secondKey]);
    client.release();
  }

  it('QRY-07 returns only nonnegative total-preserving snapshots during dispatch', async () => {
    const recipientCount = 12;
    const alarmId = await createStartedEmergency(recipientCount);
    const attempts = await startCommittedAttempts(alarmId, recipientCount);

    const rowLocker = await pool.connect();
    await rowLocker.query('BEGIN');
    await rowLocker.query('SELECT id FROM alarms WHERE id = $1 FOR UPDATE', [alarmId]);
    try {
      const whileWorkerOwnsRow = await within(detail(alarmId), 2_000);
      expectCounterInvariant(whileWorkerOwnsRow);
      expect(whileWorkerOwnsRow).toMatchObject({ status: 'SENDING', active: recipientCount });
    } finally {
      await rowLocker.query('ROLLBACK');
      rowLocker.release();
    }

    const snapshots: AlarmDetailResponse[] = [];
    for (const [index, attempt] of attempts.entries()) {
      const finish = dispatch.finish(
        attempt,
        index % 2 === 0
          ? { kind: 'ACCEPTED', messageId: `query-message-${index}` }
          : { kind: 'PERMANENT_FAILURE', code: 'RECIPIENT_BLOCKED', httpStatus: 400 },
      );
      const reads = Array.from({ length: 6 }, () => detail(alarmId));
      const [, ...during] = await Promise.all([finish, ...reads]);
      snapshots.push(...during, await detail(alarmId));
    }

    for (const snapshot of snapshots) {
      expectCounterInvariant(snapshot);
      expect(snapshot.status).toSatisfy((status: string) =>
        ['SENDING', 'PARTIALLY_FAILED'].includes(status),
      );
    }
    expect(snapshots.at(-1)).toMatchObject({
      status: 'PARTIALLY_FAILED',
      total: recipientCount,
      sent: recipientCount / 2,
      failed: recipientCount / 2,
      active: 0,
    });
  });

  it('QRY-11 exposes the last delivery and stored Alarm counters in one commit', async () => {
    const alarmId = await createStartedEmergency(1);
    const [attempt] = await startCommittedAttempts(alarmId, 1);
    if (attempt === undefined) {
      throw new Error('Expected the final delivery attempt');
    }

    const firstKey = 91_337;
    const secondKey = 7_011;
    await pool.query(`
      CREATE FUNCTION block_last_alarm_projection() RETURNS trigger
      LANGUAGE plpgsql AS $$
      BEGIN
        IF NEW.active IS DISTINCT FROM OLD.active THEN
          PERFORM pg_advisory_xact_lock(${firstKey}, ${secondKey});
        END IF;
        RETURN NEW;
      END
      $$
    `);
    await pool.query(`
      CREATE TRIGGER block_last_alarm_projection_trigger
      BEFORE UPDATE ON alarms
      FOR EACH ROW EXECUTE FUNCTION block_last_alarm_projection()
    `);

    const blocker = await pool.connect();
    await blocker.query('SELECT pg_advisory_lock($1, $2)', [firstKey, secondKey]);
    const finishing = dispatch.finish(attempt, {
      kind: 'ACCEPTED',
      messageId: 'last-delivery-message',
    });
    try {
      await waitForAdvisoryWait(firstKey, secondKey);
      const during = await Promise.all(Array.from({ length: 5 }, () => detail(alarmId)));
      expect(during).toEqual(
        Array.from({ length: 5 }, () =>
          expect.objectContaining({
            status: 'SENDING',
            total: 1,
            sent: 0,
            active: 1,
          }),
        ),
      );
      for (const snapshot of during) {
        expectCounterInvariant(snapshot);
      }
    } finally {
      await releaseAdvisoryLock(blocker, firstKey, secondKey);
      await finishing;
      await pool.query('DROP TRIGGER block_last_alarm_projection_trigger ON alarms');
      await pool.query('DROP FUNCTION block_last_alarm_projection()');
    }

    const after = await detail(alarmId);
    expectCounterInvariant(after);
    expect(after).toMatchObject({ status: 'COMPLETED', total: 1, sent: 1, active: 0 });
    const stored = await pool.query<{
      delivery_state: string;
      alarm_status: string;
      total: number;
      sent: number;
      active: number;
    }>(
      `
        SELECT
          d.state AS delivery_state,
          a.status AS alarm_status,
          a.total,
          a.sent,
          a.active
        FROM deliveries d
        JOIN alarms a ON a.id = d.alarm_id
        WHERE d.id = $1
      `,
      [attempt.deliveryId],
    );
    expect(stored.rows[0]).toEqual({
      delivery_state: 'SENT',
      alarm_status: 'COMPLETED',
      total: 1,
      sent: 1,
      active: 0,
    });
  });

  it('QRY-11/QRY-13 keeps incident summary atomic, sorted, and idempotent', async () => {
    const alarmId = await createStartedEmergency(2);
    const [failedAttempt, sentAttempt] = await startCommittedAttempts(alarmId, 2);
    if (failedAttempt === undefined || sentAttempt === undefined) {
      throw new Error('Expected two terminal evidence attempts');
    }
    await dispatch.finish(failedAttempt, {
      kind: 'PERMANENT_FAILURE',
      code: 'RECIPIENT_BLOCKED',
      httpStatus: 400,
    });
    await dispatch.finish(sentAttempt, { kind: 'ACCEPTED', messageId: 'original-message' });

    const zeroIncident = await detail(alarmId);
    expect(zeroIncident.incidentCount).toBe(0);
    expect(zeroIncident.incidentSummary).toEqual({
      count: 0,
      codes: [],
      firstDetectedAt: null,
      lastDetectedAt: null,
    });

    const lateFailedResult = { kind: 'ACCEPTED', messageId: 'late-after-failure' } as const;
    await dispatch.finish(failedAttempt, lateFailedResult);
    await dispatch.finish(failedAttempt, lateFailedResult);
    await expect(
      pool.query<{ count: number }>(
        `
          SELECT count(*)::integer AS count
          FROM delivery_evidence
          WHERE delivery_id = $1 AND consumed_at IS NULL
        `,
        [failedAttempt.deliveryId],
      ),
    ).resolves.toMatchObject({ rows: [{ count: 1 }] });

    const firstKey = 91_337;
    const secondKey = 7_013;
    await pool.query(`
      CREATE FUNCTION block_incident_projection() RETURNS trigger
      LANGUAGE plpgsql AS $$
      BEGIN
        IF NEW.incident_count IS DISTINCT FROM OLD.incident_count THEN
          PERFORM pg_advisory_xact_lock(${firstKey}, ${secondKey});
        END IF;
        RETURN NEW;
      END
      $$
    `);
    await pool.query(`
      CREATE TRIGGER block_incident_projection_trigger
      BEFORE UPDATE ON alarms
      FOR EACH ROW EXECUTE FUNCTION block_incident_projection()
    `);

    const blocker = await pool.connect();
    await blocker.query('SELECT pg_advisory_lock($1, $2)', [firstKey, secondKey]);
    const adopting = maintenance.adoptUnconsumedEvidence();
    let adopted = false;
    try {
      await waitForAdvisoryWait(firstKey, secondKey);
      const during = await detail(alarmId);
      expect(during.incidentCount).toBe(0);
      expect(during.incidentSummary).toEqual(zeroIncident.incidentSummary);
      const deliveryDuring = await pool.query<{
        post_terminal_incident_code: string | null;
      }>('SELECT post_terminal_incident_code FROM deliveries WHERE id = $1', [
        failedAttempt.deliveryId,
      ]);
      expect(deliveryDuring.rows[0]?.post_terminal_incident_code).toBeNull();
    } finally {
      await releaseAdvisoryLock(blocker, firstKey, secondKey);
      adopted = await adopting;
      await pool.query('DROP TRIGGER block_incident_projection_trigger ON alarms');
      await pool.query('DROP FUNCTION block_incident_projection()');
    }
    expect(adopted).toBe(true);

    const afterFirstIncident = await detail(alarmId);
    expect(afterFirstIncident.incidentSummary).toMatchObject({
      count: 1,
      codes: [{ code: 'LATE_POSITIVE_AFTER_FAILED', count: 1 }],
    });

    const lateDuplicateResult = { kind: 'ACCEPTED', messageId: 'late-duplicate' } as const;
    await dispatch.finish(sentAttempt, lateDuplicateResult);
    await dispatch.finish(sentAttempt, lateDuplicateResult);
    await expect(maintenance.adoptUnconsumedEvidence()).resolves.toBe(true);

    const projected = await detail(alarmId);
    expectCounterInvariant(projected);
    expect(projected).toMatchObject({
      status: 'ATTENTION_REQUIRED',
      total: 2,
      sent: 1,
      failed: 1,
      active: 0,
      duplicateCount: 1,
      incidentCount: 2,
      incidentSummary: {
        count: 2,
        codes: [
          { code: 'LATE_DISTINCT_DUPLICATE', count: 1 },
          { code: 'LATE_POSITIVE_AFTER_FAILED', count: 1 },
        ],
      },
    });
    expect(projected.incidentSummary.codes.reduce((sum, entry) => sum + entry.count, 0)).toBe(
      projected.incidentCount,
    );
    expect(projected.incidentSummary.firstDetectedAt).not.toBeNull();
    expect(projected.incidentSummary.lastDetectedAt).not.toBeNull();
    expect(new Date(projected.incidentSummary.firstDetectedAt ?? 0).getTime()).toBeLessThanOrEqual(
      new Date(projected.incidentSummary.lastDetectedAt ?? 0).getTime(),
    );

    const storedProjection = await pool.query<{
      incident_count: number;
      incident_counts: Record<string, number>;
      incident_first_detected_at: Date;
      incident_last_detected_at: Date;
    }>(
      `
        SELECT
          incident_count,
          incident_counts,
          incident_first_detected_at,
          incident_last_detected_at
        FROM alarms
        WHERE id = $1
      `,
      [alarmId],
    );
    expect(storedProjection.rows[0]).toMatchObject({
      incident_count: projected.incidentCount,
      incident_counts: {
        LATE_DISTINCT_DUPLICATE: 1,
        LATE_POSITIVE_AFTER_FAILED: 1,
      },
    });
    expect(storedProjection.rows[0]?.incident_first_detected_at.toISOString()).toBe(
      projected.incidentSummary.firstDetectedAt,
    );
    expect(storedProjection.rows[0]?.incident_last_detected_at.toISOString()).toBe(
      projected.incidentSummary.lastDetectedAt,
    );

    await dispatch.finish(failedAttempt, lateFailedResult);
    await dispatch.finish(sentAttempt, lateDuplicateResult);
    await expect(maintenance.adoptUnconsumedEvidence()).resolves.toBe(false);
    const afterRepeatedEvidence = await detail(alarmId);
    expect(afterRepeatedEvidence.incidentCount).toBe(projected.incidentCount);
    expect(afterRepeatedEvidence.duplicateCount).toBe(projected.duplicateCount);
    expect(afterRepeatedEvidence.incidentSummary).toEqual(projected.incidentSummary);

    const deliveryTableLocker = await pool.connect();
    await deliveryTableLocker.query('BEGIN');
    await deliveryTableLocker.query('LOCK TABLE deliveries IN ACCESS EXCLUSIVE MODE');
    try {
      const withoutDeliveryAggregate = await within(detail(alarmId), 2_000);
      expect(withoutDeliveryAggregate.incidentSummary).toEqual(projected.incidentSummary);
    } finally {
      await deliveryTableLocker.query('ROLLBACK');
      deliveryTableLocker.release();
    }
  });
});
