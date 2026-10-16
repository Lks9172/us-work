import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';

import { Pool } from 'pg';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import { AlarmApplicationService } from '../../packages/alarms/src/application/alarm-service';
import type {
  DispatchAttempt,
  DispatchClaim,
  ProviderSendResult,
} from '../../packages/alarms/src/application/dispatch';
import { runMigrations } from '../../packages/alarms/src/infrastructure/persistence/migrations';
import { PostgresAlarmRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-alarm-repository';
import { PostgresDispatchRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-dispatch-repository';
import { PostgresMaintenanceRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-maintenance-repository';
import { PostgresReconciliationRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-reconciliation-repository';
import { createTestDatabase, type TestDatabase } from './postgres-test-schema';

const databaseUrl = process.env.TEST_DATABASE_URL;
const describeWithDatabase = databaseUrl === undefined ? describe.skip : describe;

const DELIVERY_STATES = [
  'READY',
  'CLAIMED',
  'IN_FLIGHT',
  'RETRY_WAIT',
  'UNKNOWN',
  'RECONCILING',
  'NEEDS_REVIEW',
  'SENT',
  'FAILED_PERMANENT',
  'FAILED_RETRY_EXHAUSTED',
  'CANCELED',
] as const;

interface EmergencyFixture {
  readonly alarmId: string;
  readonly recipientId: string;
  readonly body: string;
}

interface FifthAttemptCase {
  readonly label: string;
  readonly result: ProviderSendResult;
  readonly deliveryState: string;
  readonly deliveryResultCode: string;
  readonly attemptResultCode: string;
  readonly httpStatus: number | null;
  readonly alarmStatus: string;
  readonly sent: number;
  readonly failed: number;
  readonly active: number;
}

const fifthAttemptCases: readonly FifthAttemptCase[] = [
  {
    label: '500',
    result: { kind: 'TRANSIENT_FAILURE', code: 'INTERNAL_ERROR', httpStatus: 500 },
    deliveryState: 'FAILED_RETRY_EXHAUSTED',
    deliveryResultCode: 'POST_BUDGET_EXHAUSTED',
    attemptResultCode: 'INTERNAL_ERROR',
    httpStatus: 500,
    alarmStatus: 'FAILED',
    sent: 0,
    failed: 1,
    active: 0,
  },
  {
    label: '503',
    result: { kind: 'TRANSIENT_FAILURE', code: 'SERVICE_UNAVAILABLE', httpStatus: 503 },
    deliveryState: 'FAILED_RETRY_EXHAUSTED',
    deliveryResultCode: 'POST_BUDGET_EXHAUSTED',
    attemptResultCode: 'SERVICE_UNAVAILABLE',
    httpStatus: 503,
    alarmStatus: 'FAILED',
    sent: 0,
    failed: 1,
    active: 0,
  },
  {
    label: '202',
    result: { kind: 'ACCEPTED', messageId: 'fifth-attempt-accepted' },
    deliveryState: 'SENT',
    deliveryResultCode: 'ACCEPTED',
    attemptResultCode: 'ACCEPTED',
    httpStatus: 202,
    alarmStatus: 'COMPLETED',
    sent: 1,
    failed: 0,
    active: 0,
  },
  {
    label: 'known 400',
    result: { kind: 'PERMANENT_FAILURE', code: 'RECIPIENT_BLOCKED', httpStatus: 400 },
    deliveryState: 'FAILED_PERMANENT',
    deliveryResultCode: 'RECIPIENT_BLOCKED',
    attemptResultCode: 'RECIPIENT_BLOCKED',
    httpStatus: 400,
    alarmStatus: 'FAILED',
    sent: 0,
    failed: 1,
    active: 0,
  },
  {
    label: 'unknown 400',
    result: { kind: 'PERMANENT_FAILURE', code: 'UNRECOGNIZED_PROVIDER_4XX', httpStatus: 400 },
    deliveryState: 'FAILED_PERMANENT',
    deliveryResultCode: 'UNRECOGNIZED_PROVIDER_4XX',
    attemptResultCode: 'UNRECOGNIZED_PROVIDER_4XX',
    httpStatus: 400,
    alarmStatus: 'FAILED',
    sent: 0,
    failed: 1,
    active: 0,
  },
  {
    label: 'timeout',
    result: { kind: 'AMBIGUOUS', code: 'PROVIDER_TIMEOUT' },
    deliveryState: 'UNKNOWN',
    deliveryResultCode: 'PROVIDER_TIMEOUT',
    attemptResultCode: 'PROVIDER_TIMEOUT',
    httpStatus: null,
    alarmStatus: 'SENDING',
    sent: 0,
    failed: 0,
    active: 1,
  },
];

describeWithDatabase('retry and crash repository matrix', () => {
  let testDatabase: TestDatabase;
  let pool: Pool;
  let alarms: AlarmApplicationService;
  let dispatch: PostgresDispatchRepository;
  let maintenance: PostgresMaintenanceRepository;
  let reconciliation: PostgresReconciliationRepository;
  let strongReconciliation: PostgresReconciliationRepository;
  let opaqueDeadlineReconciliation: PostgresReconciliationRepository;

  beforeAll(async () => {
    if (databaseUrl === undefined) {
      throw new Error('TEST_DATABASE_URL is required');
    }
    testDatabase = await createTestDatabase(databaseUrl);
    await runMigrations({
      databaseUrl: testDatabase.databaseUrl,
      migrationsDirectory: resolve(process.cwd(), 'migrations'),
    });
    pool = new Pool({ connectionString: testDatabase.databaseUrl, max: 12 });
    alarms = new AlarmApplicationService(
      new PostgresAlarmRepository(pool),
      'retry-crash-matrix-cursor-key',
      randomUUID,
    );
    dispatch = new PostgresDispatchRepository(pool);
    maintenance = new PostgresMaintenanceRepository(pool);
    reconciliation = new PostgresReconciliationRepository(pool);
    strongReconciliation = new PostgresReconciliationRepository(pool, {
      historyVisibilityProfile: 'AUTHORITATIVE_EMPTY_AFTER_GRACE',
      authoritativeEmptyGraceMs: 0,
      maxFastElapsedMs: 60_000,
    });
    opaqueDeadlineReconciliation = new PostgresReconciliationRepository(pool, {
      historyVisibilityProfile: 'OPAQUE',
      maxFastChecks: 1,
      maxFastElapsedMs: 0,
    });
  });

  beforeEach(async () => {
    await pool.query('TRUNCATE alarms CASCADE');
  });

  afterAll(async () => {
    await pool?.end();
    await testDatabase?.cleanup();
  });

  async function emergency(label: string): Promise<EmergencyFixture> {
    const recipientId = `${label}-recipient-${randomUUID()}`;
    const body = `${label}-body-${randomUUID()}`;
    const alarm = await alarms.create({
      type: 'EMERGENCY',
      title: `${label}-${randomUUID()}`,
      body,
      recipientIds: [recipientId],
    });
    await alarms.start(alarm.id);
    // These matrix fixtures are intentionally immediately claimable. Place their due time safely
    // in the PostgreSQL past so a small host/VM clock correction cannot turn an equality-at-now
    // fixture into an unrelated null claim.
    await pool.query(
      `UPDATE deliveries SET next_action_at = clock_timestamp() - interval '1 second' WHERE alarm_id = $1`,
      [alarm.id],
    );
    return { alarmId: alarm.id, recipientId, body };
  }

  async function claim(fixture: EmergencyFixture): Promise<DispatchClaim> {
    const claimed = await dispatch.claimNext(0, 60_000);
    if (claimed === null || claimed.alarmId !== fixture.alarmId) {
      const diagnostic = await pool.query<{
        alarm_status: string;
        audience_finalized: boolean;
        cancel_requested: boolean;
        delivery_state: string;
        due: boolean;
      }>(
        `
          SELECT
            a.status AS alarm_status,
            a.audience_finalized_at IS NOT NULL AS audience_finalized,
            a.cancel_requested_at IS NOT NULL AS cancel_requested,
            d.state AS delivery_state,
            d.next_action_at <= clock_timestamp() AS due
          FROM alarms a
          JOIN deliveries d ON d.alarm_id = a.id
          WHERE a.id = $1
        `,
        [fixture.alarmId],
      );
      throw new Error(
        `Expected claim for ${fixture.alarmId}; received ${claimed?.alarmId ?? 'null'}; ` +
          `snapshot=${JSON.stringify(diagnostic.rows)}`,
      );
    }
    return claimed;
  }

  async function start(claimed: DispatchClaim): Promise<DispatchAttempt> {
    const attempt = await dispatch.startAttempt(claimed, {
      reservationId: randomUUID(),
      scheduledMonotonicMs: performance.now(),
    });
    if (attempt === null) {
      throw new Error(`Expected STARTED attempt for ${claimed.deliveryId}`);
    }
    return attempt;
  }

  async function startCommitted(claimed: DispatchClaim): Promise<DispatchAttempt> {
    const attempt = await start(claimed);
    if (!(await dispatch.commitAttempt(attempt))) {
      throw new Error(`Expected COMMITTED attempt for ${claimed.deliveryId}`);
    }
    return attempt;
  }

  async function makeDue(deliveryId: string): Promise<void> {
    await pool.query(
      `UPDATE deliveries SET next_action_at = clock_timestamp() - interval '1 millisecond' WHERE id = $1`,
      [deliveryId],
    );
  }

  async function expireSendLease(deliveryId: string): Promise<void> {
    await pool.query(
      `UPDATE deliveries SET lease_until = clock_timestamp() - interval '1 second' WHERE id = $1`,
      [deliveryId],
    );
  }

  async function makeUnknownDue(deliveryId: string): Promise<void> {
    await pool.query(
      `
        UPDATE deliveries
        SET next_action_at = clock_timestamp() - interval '1 second',
            first_unknown_at = clock_timestamp() - interval '1 second'
        WHERE id = $1
      `,
      [deliveryId],
    );
  }

  async function recoverCommittedCrashToUnknown(deliveryId: string): Promise<void> {
    await expireSendLease(deliveryId);
    await expect(maintenance.reapExpiredSendLease()).resolves.not.toBeNull();
    await makeUnknownDue(deliveryId);
  }

  it.each(fifthAttemptCases)(
    '[RET-04] resolves the fifth STARTED result boundary for $label',
    async (testCase) => {
      const fixture = await emergency(`ret04-${testCase.label.replaceAll(' ', '-')}`);
      const claimed = await claim(fixture);
      await pool.query(
        'UPDATE deliveries SET attempt_no = 4, post_budget_count = 4 WHERE id = $1',
        [claimed.deliveryId],
      );
      const attempt = await startCommitted(claimed);
      expect(attempt.attemptNo).toBe(5);

      await dispatch.finish(attempt, testCase.result);

      const stored = await pool.query<{
        delivery_state: string;
        delivery_result_code: string;
        post_budget_count: number;
        attempt_state: string;
        request_phase: string;
        attempt_result_code: string;
        http_status: number | null;
      }>(
        `
          SELECT
            d.state AS delivery_state,
            d.result_code AS delivery_result_code,
            d.post_budget_count,
            da.state AS attempt_state,
            da.request_phase,
            da.result_code AS attempt_result_code,
            da.http_status
          FROM deliveries d
          JOIN delivery_attempts da ON da.id = $2
          WHERE d.id = $1
        `,
        [claimed.deliveryId, attempt.attemptId],
      );
      expect(stored.rows[0]).toEqual({
        delivery_state: testCase.deliveryState,
        delivery_result_code: testCase.deliveryResultCode,
        post_budget_count: 5,
        attempt_state: 'FINISHED',
        request_phase: 'FINISHED',
        attempt_result_code: testCase.attemptResultCode,
        http_status: testCase.httpStatus,
      });
      await expect(alarms.findById(fixture.alarmId)).resolves.toMatchObject({
        status: testCase.alarmStatus,
        sent: testCase.sent,
        failed: testCase.failed,
        active: testCase.active,
      });
      await expect(dispatch.claimNext(0, 60_000)).resolves.toBeNull();
    },
  );

  it('[RET-05] preserves request identity while only attempt identity advances', async () => {
    const fixture = await emergency('ret05');
    const claims: DispatchClaim[] = [];
    const attempts: DispatchAttempt[] = [];
    const results: readonly ProviderSendResult[] = [
      { kind: 'TRANSIENT_FAILURE', code: 'INTERNAL_ERROR', httpStatus: 500 },
      { kind: 'TRANSIENT_FAILURE', code: 'SERVICE_UNAVAILABLE', httpStatus: 503 },
      { kind: 'ACCEPTED', messageId: 'ret05-accepted' },
    ];

    for (const [index, result] of results.entries()) {
      const claimed = await claim(fixture);
      const attempt = await startCommitted(claimed);
      claims.push(claimed);
      attempts.push(attempt);
      await dispatch.finish(attempt, result);
      if (index < results.length - 1) {
        await makeDue(claimed.deliveryId);
      }
    }

    const first = claims[0];
    if (first === undefined) {
      throw new Error('Expected retry identity baseline');
    }
    expect(
      claims.map(({ alarmId, deliveryId, recipientId, alarmBody, clientRef }) => ({
        alarmId,
        deliveryId,
        recipientId,
        alarmBody,
        clientRef,
      })),
    ).toEqual(
      Array.from({ length: 3 }, () => ({
        alarmId: first.alarmId,
        deliveryId: first.deliveryId,
        recipientId: first.recipientId,
        alarmBody: first.alarmBody,
        clientRef: first.clientRef,
      })),
    );
    expect(attempts.map(({ attemptNo }) => attemptNo)).toEqual([1, 2, 3]);
    expect(new Set(attempts.map(({ attemptId }) => attemptId)).size).toBe(3);
    expect(new Set(attempts.map(({ reservationId }) => reservationId)).size).toBe(3);

    const stored = await pool.query<{
      attempt_no: number;
      http_status: number;
      state: string;
      budget_consumed: boolean;
    }>(
      `
        SELECT attempt_no, http_status, state, budget_consumed
        FROM delivery_attempts
        WHERE delivery_id = $1
        ORDER BY attempt_no
      `,
      [first.deliveryId],
    );
    expect(stored.rows).toEqual([
      { attempt_no: 1, http_status: 500, state: 'FINISHED', budget_consumed: true },
      { attempt_no: 2, http_status: 503, state: 'FINISHED', budget_consumed: true },
      { attempt_no: 3, http_status: 202, state: 'FINISHED', budget_consumed: true },
    ]);
    await expect(alarms.findById(fixture.alarmId)).resolves.toMatchObject({
      status: 'COMPLETED',
      sent: 1,
      failed: 0,
      active: 0,
    });
  });

  it('[RET-07][RET-25] refunds 429 budget and creates a new due attempt through the shared path', async () => {
    const fixture = await emergency('ret07-ret25');
    const firstClaim = await claim(fixture);
    const firstAttempt = await startCommitted(firstClaim);
    const before429 = await pool.query<{ now: Date }>('SELECT clock_timestamp() AS now');

    await dispatch.finish(firstAttempt, {
      kind: 'RATE_LIMITED',
      code: 'RATE_LIMITED',
      httpStatus: 429,
      retryAfterMs: 2_000,
    });

    const retry = await pool.query<{
      state: string;
      post_budget_count: number;
      rate_limit_count: number;
      first_rate_limited_at: Date;
      next_action_at: Date;
      attempt_budget_consumed: boolean;
    }>(
      `
        SELECT
          d.state,
          d.post_budget_count,
          d.rate_limit_count,
          d.first_rate_limited_at,
          d.next_action_at,
          da.budget_consumed AS attempt_budget_consumed
        FROM deliveries d
        JOIN delivery_attempts da ON da.id = $2
        WHERE d.id = $1
      `,
      [firstClaim.deliveryId, firstAttempt.attemptId],
    );
    expect(retry.rows[0]).toMatchObject({
      state: 'RETRY_WAIT',
      post_budget_count: 0,
      rate_limit_count: 1,
      attempt_budget_consumed: false,
    });
    expect(retry.rows[0]?.first_rate_limited_at).toBeInstanceOf(Date);
    expect(retry.rows[0]?.next_action_at.getTime() ?? 0).toBeGreaterThanOrEqual(
      (before429.rows[0]?.now.getTime() ?? Number.POSITIVE_INFINITY) + 2_000,
    );
    await expect(dispatch.claimNext(0, 60_000)).resolves.toBeNull();

    await makeDue(firstClaim.deliveryId);
    const secondClaim = await claim(fixture);
    expect(secondClaim).toMatchObject({
      alarmId: firstClaim.alarmId,
      deliveryId: firstClaim.deliveryId,
      recipientId: firstClaim.recipientId,
      alarmBody: firstClaim.alarmBody,
      clientRef: firstClaim.clientRef,
    });
    const secondAttempt = await startCommitted(secondClaim);
    expect(secondAttempt.attemptNo).toBe(2);
    expect(secondAttempt.reservationId).not.toBe(firstAttempt.reservationId);
    await dispatch.finish(secondAttempt, { kind: 'ACCEPTED', messageId: 'ret25-accepted' });

    const terminal = await pool.query<{
      state: string;
      post_budget_count: number;
      rate_limit_count: number;
      accepted_at: Date;
      provider_sent_at: Date | null;
      result_code: string;
    }>(
      `
        SELECT
          state, post_budget_count, rate_limit_count, accepted_at, provider_sent_at, result_code
        FROM deliveries
        WHERE id = $1
      `,
      [firstClaim.deliveryId],
    );
    expect(terminal.rows[0]).toMatchObject({
      state: 'SENT',
      post_budget_count: 1,
      rate_limit_count: 1,
      provider_sent_at: null,
      result_code: 'ACCEPTED',
    });
    expect(terminal.rows[0]?.accepted_at).toBeInstanceOf(Date);
    await expect(alarms.findById(fixture.alarmId)).resolves.toMatchObject({
      status: 'COMPLETED',
      sent: 1,
      active: 0,
    });
  });

  it.each([
    ['SENT', { kind: 'ACCEPTED', messageId: 'terminal-stable-message' } as const, 'COMPLETED'],
    [
      'FAILED_PERMANENT',
      { kind: 'PERMANENT_FAILURE', code: 'RECIPIENT_BLOCKED', httpStatus: 400 } as const,
      'FAILED',
    ],
  ])(
    '[RET-18][CR-08] keeps terminal %s immutable under stale owner calls',
    async (_state, result, status) => {
      const fixture = await emergency(`terminal-${_state.toLowerCase()}`);
      const claimed = await claim(fixture);
      const attempt = await startCommitted(claimed);
      await dispatch.finish(attempt, result);

      const before = await pool.query<{
        delivery_state: string;
        delivery_result_code: string;
        delivery_updated_at: Date;
        attempt_count: number;
        alarm_status: string;
        alarm_active: number;
        alarm_sent: number;
        alarm_failed: number;
        alarm_finished_at: Date;
        alarm_version: string;
      }>(
        `
        SELECT
          d.state AS delivery_state,
          d.result_code AS delivery_result_code,
          d.updated_at AS delivery_updated_at,
          (SELECT count(*)::integer FROM delivery_attempts WHERE delivery_id = d.id)
            AS attempt_count,
          a.status AS alarm_status,
          a.active AS alarm_active,
          a.sent AS alarm_sent,
          a.failed AS alarm_failed,
          a.finished_at AS alarm_finished_at,
          a.version::text AS alarm_version
        FROM deliveries d
        JOIN alarms a ON a.id = d.alarm_id
        WHERE d.id = $1
      `,
        [claimed.deliveryId],
      );
      expect(before.rows[0]?.alarm_status).toBe(status);

      await expect(
        dispatch.startAttempt(claimed, {
          reservationId: randomUUID(),
          scheduledMonotonicMs: performance.now(),
        }),
      ).resolves.toBeNull();
      await expect(dispatch.commitAttempt(attempt)).resolves.toBe(false);
      await expect(dispatch.heartbeat(attempt, 60_000)).resolves.toBe(false);
      await dispatch.finishNotSent(attempt);
      await dispatch.finish(attempt, result);
      await expect(dispatch.claimNext(0, 60_000)).resolves.toBeNull();

      const after = await pool.query<(typeof before.rows)[number]>(
        `
        SELECT
          d.state AS delivery_state,
          d.result_code AS delivery_result_code,
          d.updated_at AS delivery_updated_at,
          (SELECT count(*)::integer FROM delivery_attempts WHERE delivery_id = d.id)
            AS attempt_count,
          a.status AS alarm_status,
          a.active AS alarm_active,
          a.sent AS alarm_sent,
          a.failed AS alarm_failed,
          a.finished_at AS alarm_finished_at,
          a.version::text AS alarm_version
        FROM deliveries d
        JOIN alarms a ON a.id = d.alarm_id
        WHERE d.id = $1
      `,
        [claimed.deliveryId],
      );
      expect(after.rows[0]).toEqual(before.rows[0]);
    },
  );

  it('[CR-02] fences an expired pre-STARTED claim and lets a new owner send once', async () => {
    const fixture = await emergency('cr02');
    const staleClaim = await claim(fixture);
    await expireSendLease(staleClaim.deliveryId);

    await expect(maintenance.reapExpiredSendLease()).resolves.not.toBeNull();
    await expect(
      dispatch.startAttempt(staleClaim, {
        reservationId: randomUUID(),
        scheduledMonotonicMs: performance.now(),
      }),
    ).resolves.toBeNull();

    const replacement = await claim(fixture);
    expect(replacement.deliveryId).toBe(staleClaim.deliveryId);
    expect(replacement.leaseGeneration).toBe(staleClaim.leaseGeneration + 1);
    expect(replacement.leaseToken).not.toBe(staleClaim.leaseToken);
    await expect(
      dispatch.startAttempt(staleClaim, {
        reservationId: randomUUID(),
        scheduledMonotonicMs: performance.now(),
      }),
    ).resolves.toBeNull();

    const replacementAttempt = await startCommitted(replacement);
    expect(replacementAttempt.attemptNo).toBe(1);
    await dispatch.finish(replacementAttempt, { kind: 'ACCEPTED', messageId: 'cr02-accepted' });
    const stored = await pool.query<{ attempts: number; evidence: number }>(
      `
        SELECT
          (SELECT count(*)::integer FROM delivery_attempts WHERE delivery_id = $1) AS attempts,
          (SELECT count(*)::integer FROM delivery_evidence WHERE delivery_id = $1) AS evidence
      `,
      [staleClaim.deliveryId],
    );
    expect(stored.rows[0]).toEqual({ attempts: 1, evidence: 1 });
    await expect(alarms.findById(fixture.alarmId)).resolves.toMatchObject({
      status: 'COMPLETED',
      sent: 1,
      active: 0,
    });
  });

  it('[CR-03] refunds an expired PREPARED attempt and fences its stale owner', async () => {
    const fixture = await emergency('cr03');
    const firstClaim = await claim(fixture);
    const staleAttempt = await start(firstClaim);
    await expireSendLease(firstClaim.deliveryId);

    await expect(maintenance.reapExpiredSendLease()).resolves.not.toBeNull();
    await expect(dispatch.commitAttempt(staleAttempt)).resolves.toBe(false);
    await expect(dispatch.heartbeat(staleAttempt, 60_000)).resolves.toBe(false);
    await dispatch.finish(staleAttempt, {
      kind: 'TRANSIENT_FAILURE',
      code: 'STALE_OWNER_RESULT',
      httpStatus: 503,
    });

    const replacement = await claim(fixture);
    expect(replacement.leaseGeneration).toBe(firstClaim.leaseGeneration + 1);
    expect(replacement.leaseToken).not.toBe(firstClaim.leaseToken);
    const replacementAttempt = await startCommitted(replacement);
    expect(replacementAttempt.attemptNo).toBe(2);
    await dispatch.finish(replacementAttempt, { kind: 'ACCEPTED', messageId: 'cr03-accepted' });

    const stored = await pool.query<{
      attempt_no: number;
      state: string;
      request_phase: string;
      budget_consumed: boolean;
      result_code: string;
    }>(
      `
        SELECT attempt_no, state, request_phase, budget_consumed, result_code
        FROM delivery_attempts
        WHERE delivery_id = $1
        ORDER BY attempt_no
      `,
      [firstClaim.deliveryId],
    );
    expect(stored.rows).toEqual([
      {
        attempt_no: 1,
        state: 'FINISHED_NOT_SENT',
        request_phase: 'FINISHED_NOT_SENT',
        budget_consumed: false,
        result_code: 'WORKER_EXITED_BEFORE_PROVIDER_COMMIT',
      },
      {
        attempt_no: 2,
        state: 'FINISHED',
        request_phase: 'FINISHED',
        budget_consumed: true,
        result_code: 'ACCEPTED',
      },
    ]);
    const delivery = await pool.query<{ state: string; post_budget_count: number }>(
      'SELECT state, post_budget_count FROM deliveries WHERE id = $1',
      [firstClaim.deliveryId],
    );
    expect(delivery.rows[0]).toEqual({ state: 'SENT', post_budget_count: 1 });
  });

  it('moves an expired COMMITTED attempt to UNKNOWN and fences a late owner result', async () => {
    const fixture = await emergency('committed-expiry');
    const claimed = await claim(fixture);
    const staleAttempt = await startCommitted(claimed);
    await expireSendLease(claimed.deliveryId);

    await expect(maintenance.reapExpiredSendLease()).resolves.not.toBeNull();
    await expect(dispatch.heartbeat(staleAttempt, 60_000)).resolves.toBe(false);
    await expect(dispatch.commitAttempt(staleAttempt)).resolves.toBe(false);
    await dispatch.finish(staleAttempt, {
      kind: 'ACCEPTED',
      messageId: 'late-after-committed-expiry',
    });

    const stored = await pool.query<{
      delivery_state: string;
      post_budget_count: number;
      current_attempt_id: string;
      attempt_state: string;
      request_phase: string;
      evidence_count: number;
      unconsumed_evidence_count: number;
    }>(
      `
        SELECT
          d.state AS delivery_state,
          d.post_budget_count,
          d.current_attempt_id,
          da.state AS attempt_state,
          da.request_phase,
          count(e.id)::integer AS evidence_count,
          count(e.id) FILTER (WHERE e.consumed_at IS NULL)::integer AS unconsumed_evidence_count
        FROM deliveries d
        JOIN delivery_attempts da ON da.id = d.current_attempt_id
        LEFT JOIN delivery_evidence e ON e.delivery_id = d.id
        WHERE d.id = $1
        GROUP BY d.id, da.id
      `,
      [claimed.deliveryId],
    );
    expect(stored.rows[0]).toEqual({
      delivery_state: 'UNKNOWN',
      post_budget_count: 1,
      current_attempt_id: staleAttempt.attemptId,
      attempt_state: 'STARTED',
      request_phase: 'COMMITTED',
      evidence_count: 1,
      unconsumed_evidence_count: 1,
    });
    await expect(dispatch.claimNext(0, 60_000)).resolves.toBeNull();
    await expect(alarms.findById(fixture.alarmId)).resolves.toMatchObject({
      status: 'SENDING',
      active: 1,
      sent: 0,
      failed: 0,
    });
  });

  it('[SND-07] rejects a provider result before the durable COMMITTED fence', async () => {
    const fixture = await emergency('snd07-prepared-result');
    const claimed = await claim(fixture);
    const prepared = await start(claimed);

    await expect(
      dispatch.finish(prepared, { kind: 'ACCEPTED', messageId: 'must-not-be-adopted' }),
    ).rejects.toThrow();

    const stored = await pool.query<{
      delivery_state: string;
      current_attempt_id: string;
      attempt_state: string;
      request_phase: string;
      evidence_count: number;
      sent: number;
      active: number;
    }>(
      `
        SELECT
          d.state AS delivery_state,
          d.current_attempt_id,
          da.state AS attempt_state,
          da.request_phase,
          (SELECT count(*)::integer FROM delivery_evidence WHERE delivery_id = d.id)
            AS evidence_count,
          a.sent,
          a.active
        FROM deliveries d
        JOIN delivery_attempts da ON da.id = d.current_attempt_id
        JOIN alarms a ON a.id = d.alarm_id
        WHERE d.id = $1
      `,
      [claimed.deliveryId],
    );
    expect(stored.rows[0]).toEqual({
      delivery_state: 'IN_FLIGHT',
      current_attempt_id: prepared.attemptId,
      attempt_state: 'STARTED',
      request_phase: 'PREPARED',
      evidence_count: 0,
      sent: 0,
      active: 1,
    });

    await dispatch.finishNotSent(prepared);
    await expect(dispatch.hasEligible(0)).resolves.toBe(true);
  });

  it('[SND-07] rejects forbidden dispatch events across every Delivery state', async () => {
    for (const state of DELIVERY_STATES.filter(
      (candidate) => !['READY', 'RETRY_WAIT'].includes(candidate),
    )) {
      await pool.query('TRUNCATE alarms CASCADE');
      const fixture = await emergency(`snd07-claim-${state.toLowerCase()}`);
      const delivery = await pool.query<{ id: string }>(
        'SELECT id FROM deliveries WHERE alarm_id = $1',
        [fixture.alarmId],
      );
      const deliveryId = delivery.rows[0]?.id;
      if (deliveryId === undefined) {
        throw new Error(`Missing SND-07 claim fixture for ${state}`);
      }
      await pool.query(
        `UPDATE deliveries SET state = $2, next_action_at = clock_timestamp() WHERE id = $1`,
        [deliveryId, state],
      );
      await expect(dispatch.hasEligible(0)).resolves.toBe(false);
      await expect(dispatch.claimNext(0, 60_000)).resolves.toBeNull();
      const unchanged = await pool.query<{ state: string; attempts: number }>(
        `
          SELECT
            state,
            (SELECT count(*)::integer FROM delivery_attempts WHERE delivery_id = deliveries.id)
              AS attempts
          FROM deliveries
          WHERE id = $1
        `,
        [deliveryId],
      );
      expect(unchanged.rows[0]).toEqual({ state, attempts: 0 });
    }

    for (const state of DELIVERY_STATES.filter((candidate) => candidate !== 'CLAIMED')) {
      await pool.query('TRUNCATE alarms CASCADE');
      const fixture = await emergency(`snd07-start-${state.toLowerCase()}`);
      const claimed = await claim(fixture);
      await pool.query('UPDATE deliveries SET state = $2 WHERE id = $1', [
        claimed.deliveryId,
        state,
      ]);
      await expect(
        dispatch.startAttempt(claimed, {
          reservationId: randomUUID(),
          scheduledMonotonicMs: performance.now(),
        }),
      ).resolves.toBeNull();
      const unchanged = await pool.query<{ state: string; attempts: number }>(
        `
          SELECT
            state,
            (SELECT count(*)::integer FROM delivery_attempts WHERE delivery_id = deliveries.id)
              AS attempts
          FROM deliveries
          WHERE id = $1
        `,
        [claimed.deliveryId],
      );
      expect(unchanged.rows[0]).toEqual({ state, attempts: 0 });
    }

    for (const state of DELIVERY_STATES.filter((candidate) => candidate !== 'IN_FLIGHT')) {
      await pool.query('TRUNCATE alarms CASCADE');
      const fixture = await emergency(`snd07-commit-${state.toLowerCase()}`);
      const claimed = await claim(fixture);
      const prepared = await start(claimed);
      await pool.query('UPDATE deliveries SET state = $2 WHERE id = $1', [
        claimed.deliveryId,
        state,
      ]);
      await expect(dispatch.commitAttempt(prepared)).resolves.toBe(false);
      await expect(dispatch.heartbeat(prepared, 60_000)).resolves.toBe(false);
      await dispatch.finishNotSent(prepared);
      await dispatch.finish(prepared, {
        kind: 'TRANSIENT_FAILURE',
        code: 'SERVICE_UNAVAILABLE',
        httpStatus: 503,
      });
      const unchanged = await pool.query<{
        delivery_state: string;
        attempt_state: string;
        request_phase: string;
        evidence_count: number;
        sent: number;
        failed: number;
        canceled: number;
        active: number;
      }>(
        `
          SELECT
            d.state AS delivery_state,
            da.state AS attempt_state,
            da.request_phase,
            (SELECT count(*)::integer FROM delivery_evidence WHERE delivery_id = d.id)
              AS evidence_count,
            a.sent,
            a.failed,
            a.canceled,
            a.active
          FROM deliveries d
          JOIN delivery_attempts da ON da.id = d.current_attempt_id
          JOIN alarms a ON a.id = d.alarm_id
          WHERE d.id = $1
        `,
        [claimed.deliveryId],
      );
      expect(unchanged.rows[0]).toEqual({
        delivery_state: state,
        attempt_state: 'STARTED',
        request_phase: 'PREPARED',
        evidence_count: 0,
        sent: 0,
        failed: 0,
        canceled: 0,
        active: 1,
      });
    }

    for (const state of DELIVERY_STATES.filter((candidate) => candidate !== 'IN_FLIGHT')) {
      await pool.query('TRUNCATE alarms CASCADE');
      const fixture = await emergency(`snd07-result-${state.toLowerCase()}`);
      const claimed = await claim(fixture);
      const committed = await startCommitted(claimed);
      await pool.query('UPDATE deliveries SET state = $2 WHERE id = $1', [
        claimed.deliveryId,
        state,
      ]);
      await dispatch.finish(committed, {
        kind: 'TRANSIENT_FAILURE',
        code: 'SERVICE_UNAVAILABLE',
        httpStatus: 503,
      });
      const unchanged = await pool.query<{
        delivery_state: string;
        attempt_state: string;
        request_phase: string;
        evidence_count: number;
        sent: number;
        failed: number;
        canceled: number;
        active: number;
      }>(
        `
          SELECT
            d.state AS delivery_state,
            da.state AS attempt_state,
            da.request_phase,
            (SELECT count(*)::integer FROM delivery_evidence WHERE delivery_id = d.id)
              AS evidence_count,
            a.sent,
            a.failed,
            a.canceled,
            a.active
          FROM deliveries d
          JOIN delivery_attempts da ON da.id = d.current_attempt_id
          JOIN alarms a ON a.id = d.alarm_id
          WHERE d.id = $1
        `,
        [claimed.deliveryId],
      );
      expect(unchanged.rows[0]).toEqual({
        delivery_state: state,
        attempt_state: 'STARTED',
        request_phase: 'COMMITTED',
        evidence_count: 0,
        sent: 0,
        failed: 0,
        canceled: 0,
        active: 1,
      });
    }
  });

  it('[CR-01] rolls back the real claim transaction when its backend dies before commit', async () => {
    const fixture = await emergency('cr01-before-claim-commit');
    const advisoryKey = 741_852;
    const applicationName = `cr01-${randomUUID()}`;
    const blocker = await pool.connect();
    const crashingPool = new Pool({
      connectionString: testDatabase.databaseUrl,
      application_name: applicationName,
      max: 1,
    });
    const crashingDispatch = new PostgresDispatchRepository(crashingPool);
    let crashingClaim: Promise<DispatchClaim | null> | undefined;

    await blocker.query('SELECT pg_advisory_lock($1)', [advisoryKey]);
    await pool.query(`
      CREATE FUNCTION block_claim_before_commit() RETURNS trigger
      LANGUAGE plpgsql AS $$
      BEGIN
        PERFORM pg_advisory_xact_lock(${advisoryKey});
        RETURN NEW;
      END
      $$
    `);
    await pool.query(`
      CREATE TRIGGER block_claim_before_commit_trigger
      AFTER UPDATE OF state ON deliveries
      FOR EACH ROW
      WHEN (OLD.state IN ('READY', 'RETRY_WAIT') AND NEW.state = 'CLAIMED')
      EXECUTE FUNCTION block_claim_before_commit()
    `);

    try {
      crashingClaim = crashingDispatch.claimNext(0, 60_000);
      // Own the expected rejection before terminating the backend. Otherwise the backend can
      // fail the claim between pg_terminate_backend() and the later assertion, which Node/Vitest
      // correctly reports as an unhandled rejection even though the assertion eventually awaits it.
      const crashingClaimRejection = expect(crashingClaim).rejects.toThrow();
      let backendPid: number | undefined;
      for (let iteration = 0; iteration < 200; iteration += 1) {
        const activity = await pool.query<{ pid: number }>(
          `
            SELECT pid
            FROM pg_stat_activity
            WHERE application_name = $1 AND wait_event_type = 'Lock'
          `,
          [applicationName],
        );
        backendPid = activity.rows[0]?.pid;
        if (backendPid !== undefined) {
          break;
        }
        await new Promise((resolveDelay) => setTimeout(resolveDelay, 5));
      }
      if (backendPid === undefined) {
        throw new Error('The CR-01 claim never reached the pre-commit barrier');
      }

      const terminated = await pool.query<{ terminated: boolean }>(
        'SELECT pg_terminate_backend($1) AS terminated',
        [backendPid],
      );
      expect(terminated.rows[0]?.terminated).toBe(true);
      await crashingClaimRejection;
    } finally {
      await blocker.query('SELECT pg_advisory_unlock($1)', [advisoryKey]);
      blocker.release();
      await crashingPool.end();
      await pool.query('DROP TRIGGER IF EXISTS block_claim_before_commit_trigger ON deliveries');
      await pool.query('DROP FUNCTION IF EXISTS block_claim_before_commit()');
    }

    const rolledBack = await pool.query<{
      state: string;
      lease_generation: number;
      lease_token: string | null;
      attempt_count: number;
    }>(
      `
        SELECT
          d.state,
          d.lease_generation,
          d.lease_token,
          (SELECT count(*)::integer FROM delivery_attempts WHERE delivery_id = d.id)
            AS attempt_count
        FROM deliveries d
        WHERE d.alarm_id = $1
      `,
      [fixture.alarmId],
    );
    expect(rolledBack.rows[0]).toEqual({
      state: 'READY',
      lease_generation: 0,
      lease_token: null,
      attempt_count: 0,
    });

    const replacement = await claim(fixture);
    const attempt = await startCommitted(replacement);
    await dispatch.finish(attempt, { kind: 'ACCEPTED', messageId: 'cr01-single-post' });
    const terminal = await pool.query<{
      state: string;
      attempts: number;
      evidence: number;
    }>(
      `
        SELECT
          d.state,
          (SELECT count(*)::integer FROM delivery_attempts WHERE delivery_id = d.id) AS attempts,
          (SELECT count(*)::integer FROM delivery_evidence WHERE delivery_id = d.id) AS evidence
        FROM deliveries d
        WHERE d.id = $1
      `,
      [replacement.deliveryId],
    );
    expect(terminal.rows[0]).toEqual({ state: 'SENT', attempts: 1, evidence: 1 });
  });

  it('[CR-04] recovers a provider side effect after response loss and a reconciliation commit crash', async () => {
    const fixture = await emergency('cr04-side-effect-before-response');
    const claimed = await claim(fixture);
    const attempt = await startCommitted(claimed);
    await recoverCommittedCrashToUnknown(claimed.deliveryId);

    const firstClaim = await reconciliation.claimNext(15_000);
    if (firstClaim === null || firstClaim.deliveryId !== claimed.deliveryId) {
      throw new Error('Expected CR-04 reconciliation claim');
    }

    await pool.query(`
      CREATE FUNCTION reject_history_reconcile_commit() RETURNS trigger
      LANGUAGE plpgsql AS $$
      BEGIN
        RAISE EXCEPTION 'injected history reconcile commit crash';
      END
      $$
    `);
    await pool.query(`
      CREATE TRIGGER reject_history_reconcile_commit_trigger
      BEFORE UPDATE OF state ON deliveries
      FOR EACH ROW
      WHEN (OLD.state = 'RECONCILING' AND NEW.state = 'SENT')
      EXECUTE FUNCTION reject_history_reconcile_commit()
    `);
    try {
      await expect(
        reconciliation.record(firstClaim, {
          kind: 'OBSERVATION',
          entries: [
            {
              messageId: 'cr04-provider-message',
              recipientId: fixture.recipientId,
              sentAt: '2026-10-15T00:00:00.000Z',
            },
          ],
        }),
      ).rejects.toThrow();
    } finally {
      await pool.query('DROP TRIGGER reject_history_reconcile_commit_trigger ON deliveries');
      await pool.query('DROP FUNCTION reject_history_reconcile_commit()');
    }

    const rolledBack = await pool.query<{
      state: string;
      evidence_count: number;
      attempt_state: string;
    }>(
      `
        SELECT
          d.state,
          (SELECT count(*)::integer FROM delivery_evidence WHERE delivery_id = d.id)
            AS evidence_count,
          da.state AS attempt_state
        FROM deliveries d
        JOIN delivery_attempts da ON da.id = d.current_attempt_id
        WHERE d.id = $1
      `,
      [claimed.deliveryId],
    );
    expect(rolledBack.rows[0]).toEqual({
      state: 'RECONCILING',
      evidence_count: 0,
      attempt_state: 'STARTED',
    });

    await pool.query(
      `UPDATE deliveries SET reconcile_until = clock_timestamp() - interval '1 second' WHERE id = $1`,
      [claimed.deliveryId],
    );
    const takeover = await reconciliation.claimNext(15_000);
    if (takeover === null || takeover.deliveryId !== claimed.deliveryId) {
      throw new Error('Expected CR-04 reconciliation takeover');
    }
    expect(takeover.reconcileGeneration).toBe(firstClaim.reconcileGeneration + 1);
    await reconciliation.record(takeover, {
      kind: 'OBSERVATION',
      entries: [
        {
          messageId: 'cr04-provider-message',
          recipientId: fixture.recipientId,
          sentAt: '2026-10-15T00:00:00.000Z',
        },
      ],
    });

    const terminal = await pool.query<{
      state: string;
      attempts: number;
      unfinished_attempts: number;
      evidence: number;
      unconsumed_evidence: number;
    }>(
      `
        SELECT
          d.state,
          (SELECT count(*)::integer FROM delivery_attempts WHERE delivery_id = d.id) AS attempts,
          (
            SELECT count(*)::integer
            FROM delivery_attempts
            WHERE delivery_id = d.id AND state = 'STARTED'
          ) AS unfinished_attempts,
          (SELECT count(*)::integer FROM delivery_evidence WHERE delivery_id = d.id) AS evidence,
          (
            SELECT count(*)::integer
            FROM delivery_evidence
            WHERE delivery_id = d.id AND consumed_at IS NULL
          ) AS unconsumed_evidence
        FROM deliveries d
        WHERE d.id = $1
      `,
      [claimed.deliveryId],
    );
    expect(terminal.rows[0]).toEqual({
      state: 'SENT',
      attempts: 1,
      unfinished_attempts: 0,
      evidence: 1,
      unconsumed_evidence: 0,
    });
    await expect(dispatch.hasEligible(0)).resolves.toBe(false);
    await expect(alarms.findById(fixture.alarmId)).resolves.toMatchObject({
      status: 'COMPLETED',
      sent: 1,
      active: 0,
      duplicateCount: 0,
      unresolved: 0,
    });
    expect(attempt.attemptNo).toBe(1);
  });

  it('[CR-05] recovers a received 202 after its result transaction rolls back', async () => {
    const fixture = await emergency('cr05-response-before-state-commit');
    const claimed = await claim(fixture);
    const attempt = await startCommitted(claimed);

    await pool.query(`
      CREATE FUNCTION reject_direct_accepted_commit() RETURNS trigger
      LANGUAGE plpgsql AS $$
      BEGIN
        RAISE EXCEPTION 'injected direct accepted commit crash';
      END
      $$
    `);
    await pool.query(`
      CREATE TRIGGER reject_direct_accepted_commit_trigger
      BEFORE UPDATE OF state ON deliveries
      FOR EACH ROW
      WHEN (OLD.state = 'IN_FLIGHT' AND NEW.state = 'SENT')
      EXECUTE FUNCTION reject_direct_accepted_commit()
    `);
    try {
      await expect(
        dispatch.finish(attempt, { kind: 'ACCEPTED', messageId: 'cr05-provider-message' }),
      ).rejects.toThrow();
    } finally {
      await pool.query('DROP TRIGGER reject_direct_accepted_commit_trigger ON deliveries');
      await pool.query('DROP FUNCTION reject_direct_accepted_commit()');
    }

    const beforeRecovery = await pool.query<{
      delivery_state: string;
      attempt_state: string;
      request_phase: string;
      evidence_count: number;
    }>(
      `
        SELECT
          d.state AS delivery_state,
          da.state AS attempt_state,
          da.request_phase,
          (SELECT count(*)::integer FROM delivery_evidence WHERE delivery_id = d.id)
            AS evidence_count
        FROM deliveries d
        JOIN delivery_attempts da ON da.id = d.current_attempt_id
        WHERE d.id = $1
      `,
      [claimed.deliveryId],
    );
    expect(beforeRecovery.rows[0]).toEqual({
      delivery_state: 'IN_FLIGHT',
      attempt_state: 'STARTED',
      request_phase: 'COMMITTED',
      evidence_count: 0,
    });

    await recoverCommittedCrashToUnknown(claimed.deliveryId);
    const reconcileClaim = await reconciliation.claimNext(15_000);
    if (reconcileClaim === null || reconcileClaim.deliveryId !== claimed.deliveryId) {
      throw new Error('Expected CR-05 reconciliation claim');
    }
    await reconciliation.record(reconcileClaim, {
      kind: 'OBSERVATION',
      entries: [
        {
          messageId: 'cr05-provider-message',
          recipientId: fixture.recipientId,
          sentAt: '2026-10-15T00:00:00.000Z',
        },
      ],
    });

    const terminal = await pool.query<{ state: string; attempts: number; evidence: number }>(
      `
        SELECT
          d.state,
          (SELECT count(*)::integer FROM delivery_attempts WHERE delivery_id = d.id) AS attempts,
          (SELECT count(*)::integer FROM delivery_evidence WHERE delivery_id = d.id) AS evidence
        FROM deliveries d
        WHERE d.id = $1
      `,
      [claimed.deliveryId],
    );
    expect(terminal.rows[0]).toEqual({ state: 'SENT', attempts: 1, evidence: 1 });
    await expect(dispatch.hasEligible(0)).resolves.toBe(false);
  });

  it.each([
    {
      id: 'CR-06',
      label: 'lost 503',
      firstResult: {
        kind: 'TRANSIENT_FAILURE',
        code: 'SERVICE_UNAVAILABLE',
        httpStatus: 503,
      } as const,
      replacementResult: { kind: 'ACCEPTED', messageId: 'cr06-retry-accepted' } as const,
      terminalState: 'SENT',
      alarmStatus: 'COMPLETED',
      sent: 1,
      failed: 0,
    },
    {
      id: 'CR-07',
      label: 'lost blocked 400',
      firstResult: {
        kind: 'PERMANENT_FAILURE',
        code: 'RECIPIENT_BLOCKED',
        httpStatus: 400,
      } as const,
      replacementResult: {
        kind: 'PERMANENT_FAILURE',
        code: 'RECIPIENT_BLOCKED',
        httpStatus: 400,
      } as const,
      terminalState: 'FAILED_PERMANENT',
      alarmStatus: 'FAILED',
      sent: 0,
      failed: 1,
    },
  ])(
    '[$id] retries $label only after a strong authoritative-empty observation',
    async (testCase) => {
      const fixture = await emergency(`${testCase.id.toLowerCase()}-strong`);
      const claimed = await claim(fixture);
      const attempt = await startCommitted(claimed);

      await pool.query(`
        CREATE FUNCTION reject_observed_result_commit() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          RAISE EXCEPTION 'injected observed result commit crash';
        END
        $$
      `);
      await pool.query(`
        CREATE TRIGGER reject_observed_result_commit_trigger
        BEFORE UPDATE OF state ON deliveries
        FOR EACH ROW
        WHEN (OLD.state = 'IN_FLIGHT')
        EXECUTE FUNCTION reject_observed_result_commit()
      `);
      try {
        await expect(dispatch.finish(attempt, testCase.firstResult)).rejects.toThrow();
      } finally {
        await pool.query('DROP TRIGGER reject_observed_result_commit_trigger ON deliveries');
        await pool.query('DROP FUNCTION reject_observed_result_commit()');
      }

      await recoverCommittedCrashToUnknown(claimed.deliveryId);
      const historyClaim = await strongReconciliation.claimNext(15_000);
      if (historyClaim === null || historyClaim.deliveryId !== claimed.deliveryId) {
        throw new Error(`Expected ${testCase.id} strong reconciliation claim`);
      }
      await strongReconciliation.record(historyClaim, { kind: 'OBSERVATION', entries: [] });

      const retryReady = await pool.query<{
        state: string;
        post_budget_count: number;
        attempts: number;
      }>(
        `
          SELECT
            d.state,
            d.post_budget_count,
            (SELECT count(*)::integer FROM delivery_attempts WHERE delivery_id = d.id) AS attempts
          FROM deliveries d
          WHERE d.id = $1
        `,
        [claimed.deliveryId],
      );
      expect(retryReady.rows[0]).toEqual({
        state: 'RETRY_WAIT',
        post_budget_count: 1,
        attempts: 1,
      });

      const replacement = await claim(fixture);
      const replacementAttempt = await startCommitted(replacement);
      expect(replacementAttempt.attemptNo).toBe(2);
      await dispatch.finish(replacementAttempt, testCase.replacementResult);

      const terminal = await pool.query<{
        state: string;
        post_budget_count: number;
        attempts: number;
        unfinished_attempts: number;
      }>(
        `
          SELECT
            d.state,
            d.post_budget_count,
            (SELECT count(*)::integer FROM delivery_attempts WHERE delivery_id = d.id) AS attempts,
            (
              SELECT count(*)::integer
              FROM delivery_attempts
              WHERE delivery_id = d.id AND state = 'STARTED'
            ) AS unfinished_attempts
          FROM deliveries d
          WHERE d.id = $1
        `,
        [claimed.deliveryId],
      );
      expect(terminal.rows[0]).toEqual({
        state: testCase.terminalState,
        post_budget_count: 2,
        attempts: 2,
        unfinished_attempts: 0,
      });
      await expect(alarms.findById(fixture.alarmId)).resolves.toMatchObject({
        status: testCase.alarmStatus,
        sent: testCase.sent,
        failed: testCase.failed,
        active: 0,
        unresolved: 0,
        duplicateCount: 0,
      });
    },
  );

  it.each([
    {
      id: 'CR-06',
      result: {
        kind: 'TRANSIENT_FAILURE',
        code: 'SERVICE_UNAVAILABLE',
        httpStatus: 503,
      } as const,
    },
    {
      id: 'CR-07',
      result: {
        kind: 'PERMANENT_FAILURE',
        code: 'RECIPIENT_BLOCKED',
        httpStatus: 400,
      } as const,
    },
  ])('[$id] keeps a lost response unresolved at the opaque history deadline', async (testCase) => {
    const fixture = await emergency(`${testCase.id.toLowerCase()}-opaque`);
    const claimed = await claim(fixture);
    const attempt = await startCommitted(claimed);

    await pool.query(`
        CREATE FUNCTION reject_opaque_result_commit() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          RAISE EXCEPTION 'injected opaque result commit crash';
        END
        $$
      `);
    await pool.query(`
        CREATE TRIGGER reject_opaque_result_commit_trigger
        BEFORE UPDATE OF state ON deliveries
        FOR EACH ROW
        WHEN (OLD.state = 'IN_FLIGHT')
        EXECUTE FUNCTION reject_opaque_result_commit()
      `);
    try {
      await expect(dispatch.finish(attempt, testCase.result)).rejects.toThrow();
    } finally {
      await pool.query('DROP TRIGGER reject_opaque_result_commit_trigger ON deliveries');
      await pool.query('DROP FUNCTION reject_opaque_result_commit()');
    }

    await recoverCommittedCrashToUnknown(claimed.deliveryId);
    const historyClaim = await opaqueDeadlineReconciliation.claimNext(15_000);
    if (historyClaim === null || historyClaim.deliveryId !== claimed.deliveryId) {
      throw new Error(`Expected ${testCase.id} opaque reconciliation claim`);
    }
    await opaqueDeadlineReconciliation.record(historyClaim, {
      kind: 'OBSERVATION',
      entries: [],
    });

    const unresolved = await pool.query<{
      state: string;
      post_budget_count: number;
      attempts: number;
      unfinished_attempts: number;
    }>(
      `
          SELECT
            d.state,
            d.post_budget_count,
            (SELECT count(*)::integer FROM delivery_attempts WHERE delivery_id = d.id) AS attempts,
            (
              SELECT count(*)::integer
              FROM delivery_attempts
              WHERE delivery_id = d.id AND state = 'STARTED'
            ) AS unfinished_attempts
          FROM deliveries d
          WHERE d.id = $1
        `,
      [claimed.deliveryId],
    );
    expect(unresolved.rows[0]).toEqual({
      state: 'NEEDS_REVIEW',
      post_budget_count: 1,
      attempts: 1,
      unfinished_attempts: 0,
    });
    await expect(dispatch.hasEligible(0)).resolves.toBe(false);
    await expect(alarms.findById(fixture.alarmId)).resolves.toMatchObject({
      status: 'ATTENTION_REQUIRED',
      sent: 0,
      failed: 0,
      active: 0,
      unresolved: 1,
    });
  });
});
