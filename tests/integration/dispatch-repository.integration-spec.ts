import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';

import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { AlarmApplicationService } from '../../packages/alarms/src/application/alarm-service';
import type {
  DispatchClaim,
  PacedPermit,
  ProviderSendResult,
} from '../../packages/alarms/src/application/dispatch';
import { runMigrations } from '../../packages/alarms/src/infrastructure/persistence/migrations';
import { PostgresAlarmRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-alarm-repository';
import { PostgresDispatchRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-dispatch-repository';
import { PostgresExpansionRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-expansion-repository';
import { PostgresMaintenanceRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-maintenance-repository';
import { createTestDatabase, type TestDatabase } from './postgres-test-schema';

const databaseUrl = process.env.TEST_DATABASE_URL;
const describeWithDatabase = databaseUrl === undefined ? describe.skip : describe;

describeWithDatabase('PostgresDispatchRepository', () => {
  let testDatabase: TestDatabase;
  let pool: Pool;
  let alarms: AlarmApplicationService;
  let dispatch: PostgresDispatchRepository;
  let expansion: PostgresExpansionRepository;

  beforeAll(async () => {
    if (databaseUrl === undefined) {
      throw new Error('TEST_DATABASE_URL is required');
    }
    testDatabase = await createTestDatabase(databaseUrl);
    await runMigrations({
      databaseUrl: testDatabase.databaseUrl,
      migrationsDirectory: resolve(process.cwd(), 'migrations'),
    });
    pool = new Pool({ connectionString: testDatabase.databaseUrl, max: 24 });
    alarms = new AlarmApplicationService(
      new PostgresAlarmRepository(pool),
      'dispatch-integration-cursor-key',
      randomUUID,
    );
    dispatch = new PostgresDispatchRepository(pool);
    expansion = new PostgresExpansionRepository(pool);
  });

  afterAll(async () => {
    await pool?.end();
    await testDatabase?.cleanup();
  });

  function permit(): PacedPermit {
    return { reservationId: randomUUID(), scheduledMonotonicMs: performance.now() };
  }

  async function emergency(recipientIds: string[]): Promise<string> {
    const alarm = await alarms.create({
      type: 'EMERGENCY',
      title: `emergency-${randomUUID()}`,
      body: 'urgent body',
      recipientIds,
    });
    await alarms.start(alarm.id);
    return alarm.id;
  }

  async function bulkRecipients(recipientIds: readonly string[]): Promise<string> {
    const alarm = await alarms.create({
      type: 'BULK',
      title: `bulk-${randomUUID()}`,
      body: 'bulk body',
    });
    await alarms.start(alarm.id);
    const lease = await expansion.claimNext(30_000);
    if (lease === null || lease.alarmId !== alarm.id) {
      throw new Error('Expected the new BULK expansion lease');
    }

    for (let offset = 0; offset < recipientIds.length; offset += 1_000) {
      const page = recipientIds.slice(offset, offset + 1_000);
      const nextCursor =
        offset + page.length < recipientIds.length
          ? `fixture:${alarm.id}:${offset + page.length}`
          : null;
      const outcome = await expansion.savePage(lease, { recipientIds: page, nextCursor }, 30_000);
      expect(outcome).toBe(nextCursor === null ? 'COMPLETED' : 'CONTINUE');
    }

    return alarm.id;
  }

  function bulk(recipientId: string): Promise<string> {
    return bulkRecipients([recipientId]);
  }

  async function startClaim(claim: DispatchClaim) {
    const attempt = await dispatch.startAttempt(claim, permit());
    if (attempt === null) {
      throw new Error('Expected dispatch STARTED');
    }
    if (!(await dispatch.commitAttempt(attempt))) {
      throw new Error('Expected provider commit fence');
    }
    return attempt;
  }

  it('claims emergency before an older bulk delivery and completes both aggregates', async () => {
    const bulkAlarmId = await bulk(`bulk-user-${randomUUID()}`);
    const emergencyAlarmId = await emergency([`emergency-user-${randomUUID()}`]);

    const firstClaim = await dispatch.claimNext(0, 60_000);
    expect(firstClaim?.alarmId).toBe(emergencyAlarmId);
    if (firstClaim === null) {
      throw new Error('Expected emergency claim');
    }
    const emergencyAttempt = await startClaim(firstClaim);
    await dispatch.finish(emergencyAttempt, { kind: 'ACCEPTED', messageId: 'message-emergency' });

    const secondClaim = await dispatch.claimNext(0, 60_000);
    expect(secondClaim?.alarmId).toBe(bulkAlarmId);
    if (secondClaim === null) {
      throw new Error('Expected bulk claim');
    }
    const bulkAttempt = await startClaim(secondClaim);
    await dispatch.finish(bulkAttempt, { kind: 'ACCEPTED', messageId: 'message-bulk' });

    await expect(alarms.findById(emergencyAlarmId)).resolves.toMatchObject({
      status: 'COMPLETED',
      sent: 1,
      active: 0,
    });
    await expect(alarms.findById(bulkAlarmId)).resolves.toMatchObject({
      status: 'COMPLETED',
      sent: 1,
      active: 0,
    });
  });

  it('uses stable emergency ties before bulk retry and new bulk priorities', async () => {
    const olderEmergencyId = await emergency([
      `stable-emergency-a-${randomUUID()}`,
      `stable-emergency-b-${randomUUID()}`,
    ]);
    const retryEmergencyId = await emergency([`retry-emergency-${randomUUID()}`]);
    const retryBulkId = await bulk(`retry-bulk-${randomUUID()}`);
    const newBulkId = await bulk(`new-bulk-${randomUUID()}`);
    const sameDueAt = '2026-01-01T00:00:00.000Z';

    await pool.query(
      `
        UPDATE deliveries
        SET
          next_action_at = $5::timestamptz,
          alarm_created_at = CASE alarm_id
            WHEN $1::uuid THEN '2026-01-01T00:00:01.000Z'::timestamptz
            WHEN $2::uuid THEN '2026-01-01T00:00:02.000Z'::timestamptz
            WHEN $3::uuid THEN '2026-01-01T00:00:03.000Z'::timestamptz
            WHEN $4::uuid THEN '2026-01-01T00:00:04.000Z'::timestamptz
          END,
          state = CASE
            WHEN alarm_id IN ($2::uuid, $3::uuid) THEN 'RETRY_WAIT'
            ELSE 'READY'
          END,
          priority = CASE
            WHEN alarm_id IN ($1::uuid, $2::uuid) THEN 100
            WHEN alarm_id = $3::uuid THEN 20
            ELSE 10
          END
        WHERE alarm_id IN ($1::uuid, $2::uuid, $3::uuid, $4::uuid)
      `,
      [olderEmergencyId, retryEmergencyId, retryBulkId, newBulkId, sameDueAt],
    );

    const expected = await pool.query<{ alarm_id: string; id: string }>(
      `
        SELECT alarm_id, id
        FROM deliveries
        WHERE alarm_id = ANY($1::uuid[])
        ORDER BY priority DESC, next_action_at ASC, alarm_created_at ASC, id ASC
      `,
      [[olderEmergencyId, retryEmergencyId, retryBulkId, newBulkId]],
    );
    expect(expected.rows.map(({ alarm_id }) => alarm_id)).toEqual([
      olderEmergencyId,
      olderEmergencyId,
      retryEmergencyId,
      retryBulkId,
      newBulkId,
    ]);

    const claims: DispatchClaim[] = [];
    for (let index = 0; index < expected.rows.length; index += 1) {
      const claim = await dispatch.claimNext(0, 300_000);
      if (claim === null) {
        throw new Error(`Expected priority claim ${index + 1}`);
      }
      claims.push(claim);
    }

    expect(claims.map(({ deliveryId }) => deliveryId)).toEqual(expected.rows.map(({ id }) => id));
    expect(claims.map(({ alarmId }) => alarmId)).toEqual(
      expected.rows.map(({ alarm_id }) => alarm_id),
    );
  });

  it('serializes cancel before STARTED and creates no provider attempt', async () => {
    const alarmId = await emergency([`cancel-user-${randomUUID()}`]);
    const claim = await dispatch.claimNext(0, 60_000);
    if (claim === null || claim.alarmId !== alarmId) {
      throw new Error('Expected cancel test claim');
    }

    await alarms.cancel(alarmId);
    await expect(dispatch.startAttempt(claim, permit())).resolves.toBeNull();

    const attempts = await pool.query<{ count: number }>(
      `
        SELECT count(*)::integer AS count
        FROM delivery_attempts da
        JOIN deliveries d ON d.id = da.delivery_id
        WHERE d.alarm_id = $1
      `,
      [alarmId],
    );
    expect(attempts.rows[0]?.count).toBe(0);
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'CANCELED',
      canceled: 1,
      active: 0,
    });
  });

  it('blocks provider commit when cancel wins after STARTED', async () => {
    const alarmId = await emergency([`cancel-after-start-${randomUUID()}`]);
    const claim = await dispatch.claimNext(0, 60_000);
    if (claim === null || claim.alarmId !== alarmId) {
      throw new Error('Expected post-STARTED cancel claim');
    }
    const attempt = await dispatch.startAttempt(claim, permit());
    if (attempt === null) {
      throw new Error('Expected post-STARTED cancel attempt');
    }

    await alarms.cancel(alarmId);
    await expect(dispatch.commitAttempt(attempt)).resolves.toBe(false);
    await dispatch.finishNotSent(attempt);

    const stored = await pool.query<{
      delivery_state: string;
      attempt_state: string;
      request_phase: string;
      adapter_committed_at: Date | null;
    }>(
      `
        SELECT
          d.state AS delivery_state,
          da.state AS attempt_state,
          da.request_phase,
          da.adapter_committed_at
        FROM deliveries d
        JOIN delivery_attempts da ON da.delivery_id = d.id
        WHERE d.alarm_id = $1
      `,
      [alarmId],
    );
    expect(stored.rows[0]).toEqual({
      delivery_state: 'CANCELED',
      attempt_state: 'FINISHED_NOT_SENT',
      request_phase: 'FINISHED_NOT_SENT',
      adapter_committed_at: null,
    });
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'CANCELED',
      canceled: 1,
      active: 0,
    });
  });

  it('[RED-04] fences an old prepared attempt when recovery advances the durable barrier', async () => {
    const alarmId = await emergency([`old-reservation-${randomUUID()}`]);
    const claim = await dispatch.claimNext(0, 60_000);
    if (claim === null || claim.alarmId !== alarmId) {
      throw new Error('Expected old-reservation claim');
    }
    const oldPermit = permit();
    const attempt = await dispatch.startAttempt(claim, oldPermit);
    if (attempt === null) {
      throw new Error('Expected prepared old-reservation attempt');
    }

    try {
      await pool.query(`
        UPDATE dispatch_control
        SET
          resume_not_before = clock_timestamp() + interval '1.2 seconds',
          version = version + 1,
          updated_at = clock_timestamp()
        WHERE control_key = 'default-provider'
      `);

      await expect(dispatch.commitAttempt(attempt)).resolves.toBe(false);
      await dispatch.finishNotSent(attempt);

      const stored = await pool.query<{
        delivery_state: string;
        attempt_state: string;
        request_phase: string;
        post_budget_count: number;
        budget_consumed: boolean;
        adapter_committed_at: Date | null;
        limiter_reservation_id: string;
      }>(
        `
          SELECT
            d.state AS delivery_state,
            da.state AS attempt_state,
            da.request_phase,
            d.post_budget_count,
            da.budget_consumed,
            da.adapter_committed_at,
            da.limiter_reservation_id
          FROM deliveries d
          JOIN delivery_attempts da ON da.delivery_id = d.id
          WHERE d.id = $1
          ORDER BY da.created_at DESC
          LIMIT 1
        `,
        [claim.deliveryId],
      );
      expect(stored.rows[0]).toEqual({
        delivery_state: 'READY',
        attempt_state: 'FINISHED_NOT_SENT',
        request_phase: 'FINISHED_NOT_SENT',
        post_budget_count: 0,
        budget_consumed: false,
        adapter_committed_at: null,
        limiter_reservation_id: oldPermit.reservationId,
      });
    } finally {
      await alarms.cancel(alarmId).catch(() => undefined);
      await pool.query(`
        UPDATE dispatch_control
        SET resume_not_before = '-infinity'::timestamptz, updated_at = clock_timestamp()
        WHERE control_key = 'default-provider'
      `);
    }
  });

  it.each<{
    readonly label: string;
    readonly result: ProviderSendResult;
    readonly deliveryState: string;
    readonly counts: { readonly sent: number; readonly failed: number; readonly canceled: number };
  }>([
    {
      label: 'accepted request',
      result: { kind: 'ACCEPTED', messageId: 'accepted-before-cancel' },
      deliveryState: 'SENT',
      counts: { sent: 1, failed: 0, canceled: 0 },
    },
    {
      label: 'permanent provider rejection',
      result: { kind: 'PERMANENT_FAILURE', code: 'RECIPIENT_BLOCKED', httpStatus: 400 },
      deliveryState: 'FAILED_PERMANENT',
      counts: { sent: 0, failed: 1, canceled: 0 },
    },
    {
      label: 'known-unsent rate limit',
      result: {
        kind: 'RATE_LIMITED',
        code: 'RATE_LIMITED',
        httpStatus: 429,
        retryAfterMs: 2_000,
      },
      deliveryState: 'CANCELED',
      counts: { sent: 0, failed: 0, canceled: 1 },
    },
    {
      label: 'known-unsent transient failure',
      result: {
        kind: 'TRANSIENT_FAILURE',
        code: 'SERVICE_UNAVAILABLE',
        httpStatus: 503,
      },
      deliveryState: 'CANCELED',
      counts: { sent: 0, failed: 0, canceled: 1 },
    },
    {
      label: 'proven pre-request construction failure',
      result: {
        kind: 'DEFINITELY_NOT_SENT',
        code: 'PROVIDER_REQUEST_CREATION_ERROR',
      },
      deliveryState: 'CANCELED',
      counts: { sent: 0, failed: 0, canceled: 1 },
    },
  ])('settles a committed $label without retry when cancel follows', async (testCase) => {
    const alarmId = await emergency([`cancel-committed-${randomUUID()}`]);
    const claim = await dispatch.claimNext(0, 60_000);
    if (claim === null || claim.alarmId !== alarmId) {
      throw new Error('Expected committed cancel claim');
    }
    const attempt = await startClaim(claim);

    await alarms.cancel(alarmId);
    await dispatch.finish(attempt, testCase.result);

    const delivery = await pool.query<{ state: string }>(
      'SELECT state FROM deliveries WHERE id = $1',
      [claim.deliveryId],
    );
    expect(delivery.rows[0]?.state).toBe(testCase.deliveryState);
    if (testCase.result.kind === 'DEFINITELY_NOT_SENT') {
      await expect(
        pool.query(
          `
            SELECT
              d.post_budget_count,
              d.rate_limit_count,
              da.state AS attempt_state,
              da.request_phase,
              da.transport_class,
              da.budget_consumed
            FROM deliveries d
            JOIN delivery_attempts da ON da.id = $2
            WHERE d.id = $1
          `,
          [claim.deliveryId, attempt.attemptId],
        ),
      ).resolves.toMatchObject({
        rows: [
          {
            post_budget_count: 0,
            rate_limit_count: 0,
            attempt_state: 'FINISHED_NOT_SENT',
            request_phase: 'FINISHED_NOT_SENT',
            transport_class: 'DEFINITELY_NOT_SENT',
            budget_consumed: false,
          },
        ],
      });
    }
    await expect(dispatch.hasEligible(0)).resolves.toBe(false);
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'CANCELED',
      ...testCase.counts,
      active: 0,
    });
  });

  it('keeps an ambiguous committed request UNKNOWN and never blind-retries it', async () => {
    const alarmId = await emergency([`unknown-user-${randomUUID()}`]);
    const claim = await dispatch.claimNext(0, 60_000);
    if (claim === null || claim.alarmId !== alarmId) {
      throw new Error('Expected unknown test claim');
    }
    const attempt = await startClaim(claim);
    await dispatch.finish(attempt, { kind: 'AMBIGUOUS', code: 'PROVIDER_TIMEOUT' });

    const delivery = await pool.query<{
      state: string;
      post_budget_count: number;
      current_attempt_id: string | null;
    }>('SELECT state, post_budget_count, current_attempt_id FROM deliveries WHERE alarm_id = $1', [
      alarmId,
    ]);
    expect(delivery.rows[0]).toEqual({
      state: 'UNKNOWN',
      post_budget_count: 1,
      current_attempt_id: attempt.attemptId,
    });
    await expect(dispatch.hasEligible(0)).resolves.toBe(false);
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'SENDING',
      active: 1,
    });
  });

  it('[RET-17] rolls back a proven not-sent fifth reservation and audits a bounded retry', async () => {
    const priority = 32_750;
    const alarmId = await emergency([`definitely-not-sent-${randomUUID()}`]);
    await pool.query(
      `
        UPDATE deliveries
        SET priority = $2, post_budget_count = 4
        WHERE alarm_id = $1
      `,
      [alarmId, priority],
    );
    const claim = await dispatch.claimNext(priority, 60_000);
    if (claim === null || claim.alarmId !== alarmId) {
      throw new Error('Expected definite-not-sent boundary claim');
    }
    const attempt = await startClaim(claim);
    const deterministicDispatch = new PostgresDispatchRepository(pool, { random: () => 0.5 });

    await deterministicDispatch.finish(attempt, {
      kind: 'DEFINITELY_NOT_SENT',
      code: 'PROVIDER_REQUEST_CREATION_ERROR',
    });

    const state = await pool.query<{
      delivery_state: string;
      result_code: string;
      post_budget_count: number;
      rate_limit_count: number;
      current_attempt_id: string | null;
      attempt_state: string;
      request_phase: string;
      transport_class: string;
      budget_consumed: boolean;
      http_status: number | null;
      response_received: boolean;
      bounded_backoff: boolean;
    }>(
      `
        SELECT
          d.state AS delivery_state,
          d.result_code,
          d.post_budget_count,
          d.rate_limit_count,
          d.current_attempt_id,
          da.state AS attempt_state,
          da.request_phase,
          da.transport_class,
          da.budget_consumed,
          da.http_status,
          da.response_received_at IS NOT NULL AS response_received,
          d.next_action_at >= da.finished_at
            AND d.next_action_at <= da.finished_at + interval '1 second' AS bounded_backoff
        FROM deliveries d
        JOIN delivery_attempts da ON da.id = $2
        WHERE d.id = $1
      `,
      [claim.deliveryId, attempt.attemptId],
    );
    expect(state.rows[0]).toEqual({
      delivery_state: 'RETRY_WAIT',
      result_code: 'PROVIDER_REQUEST_CREATION_ERROR',
      post_budget_count: 4,
      rate_limit_count: 0,
      current_attempt_id: null,
      attempt_state: 'FINISHED_NOT_SENT',
      request_phase: 'FINISHED_NOT_SENT',
      transport_class: 'DEFINITELY_NOT_SENT',
      budget_consumed: false,
      http_status: null,
      response_received: false,
      bounded_backoff: true,
    });
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'SENDING',
      active: 1,
      failed: 0,
    });
    await alarms.cancel(alarmId);
  });

  it('SND-07 forbids stale dispatch events from reopening UNKNOWN or CANCELED work', async () => {
    const unknownAlarmId = await emergency([`unknown-transition-${randomUUID()}`]);
    const unknownClaim = await dispatch.claimNext(0, 60_000);
    if (unknownClaim === null || unknownClaim.alarmId !== unknownAlarmId) {
      throw new Error('Expected UNKNOWN transition claim');
    }
    const unknownAttempt = await startClaim(unknownClaim);
    await dispatch.finish(unknownAttempt, { kind: 'AMBIGUOUS', code: 'PROVIDER_TIMEOUT' });

    await expect(dispatch.startAttempt(unknownClaim, permit())).resolves.toBeNull();
    await expect(dispatch.commitAttempt(unknownAttempt)).resolves.toBe(false);
    await expect(dispatch.heartbeat(unknownAttempt, 60_000)).resolves.toBe(false);
    await dispatch.finishNotSent(unknownAttempt);
    await dispatch.finish(unknownAttempt, {
      kind: 'ACCEPTED',
      messageId: 'late-positive-must-be-adopted-as-evidence',
    });

    const unknown = await pool.query<{
      state: string;
      result_code: string;
      attempt_state: string;
      unconsumed_evidence: number;
      alarm_status: string;
      active: number;
      sent: number;
    }>(
      `
        SELECT
          d.state,
          d.result_code,
          da.state AS attempt_state,
          count(e.id) FILTER (WHERE e.consumed_at IS NULL)::integer AS unconsumed_evidence,
          a.status AS alarm_status,
          a.active,
          a.sent
        FROM deliveries d
        JOIN alarms a ON a.id = d.alarm_id
        JOIN delivery_attempts da ON da.id = d.current_attempt_id
        LEFT JOIN delivery_evidence e ON e.delivery_id = d.id
        WHERE d.id = $1
        GROUP BY d.id, da.id, a.id
      `,
      [unknownClaim.deliveryId],
    );
    expect(unknown.rows[0]).toEqual({
      state: 'UNKNOWN',
      result_code: 'PROVIDER_TIMEOUT',
      attempt_state: 'FINISHED',
      unconsumed_evidence: 1,
      alarm_status: 'SENDING',
      active: 1,
      sent: 0,
    });

    const canceledAlarmId = await emergency([`canceled-transition-${randomUUID()}`]);
    const canceledClaim = await dispatch.claimNext(0, 60_000);
    if (canceledClaim === null || canceledClaim.alarmId !== canceledAlarmId) {
      throw new Error('Expected CANCELED transition claim');
    }
    const canceledAttempt = await dispatch.startAttempt(canceledClaim, permit());
    if (canceledAttempt === null) {
      throw new Error('Expected prepared CANCELED transition attempt');
    }
    await alarms.cancel(canceledAlarmId);
    await expect(dispatch.commitAttempt(canceledAttempt)).resolves.toBe(false);
    await dispatch.finishNotSent(canceledAttempt);

    await expect(dispatch.startAttempt(canceledClaim, permit())).resolves.toBeNull();
    await expect(dispatch.commitAttempt(canceledAttempt)).resolves.toBe(false);
    await expect(dispatch.heartbeat(canceledAttempt, 60_000)).resolves.toBe(false);
    await dispatch.finishNotSent(canceledAttempt);

    const canceled = await pool.query<{
      state: string;
      result_code: string;
      attempt_state: string;
      alarm_status: string;
      active: number;
      canceled: number;
    }>(
      `
        SELECT
          d.state,
          d.result_code,
          da.state AS attempt_state,
          a.status AS alarm_status,
          a.active,
          a.canceled
        FROM deliveries d
        JOIN alarms a ON a.id = d.alarm_id
        JOIN delivery_attempts da ON da.delivery_id = d.id
        WHERE d.id = $1
      `,
      [canceledClaim.deliveryId],
    );
    expect(canceled.rows[0]).toEqual({
      state: 'CANCELED',
      result_code: 'CANCELED_BEFORE_PROVIDER_COMMIT',
      attempt_state: 'FINISHED_NOT_SENT',
      alarm_status: 'CANCELED',
      active: 0,
      canceled: 1,
    });
  });

  it('heartbeats only the current committed attempt and fences stale owners', async () => {
    const alarmId = await emergency([`heartbeat-user-${randomUUID()}`]);
    const claim = await dispatch.claimNext(0, 60_000);
    if (claim === null || claim.alarmId !== alarmId) {
      throw new Error('Expected heartbeat test claim');
    }
    const attempt = await dispatch.startAttempt(claim, permit());
    if (attempt === null) {
      throw new Error('Expected heartbeat test attempt');
    }

    await expect(dispatch.heartbeat(attempt, 60_000)).resolves.toBe(false);
    await expect(dispatch.commitAttempt(attempt)).resolves.toBe(true);

    await pool.query(
      `
        UPDATE deliveries
        SET lease_until = clock_timestamp() + interval '100 milliseconds'
        WHERE id = $1
      `,
      [claim.deliveryId],
    );
    await expect(dispatch.heartbeat(attempt, 60_000)).resolves.toBe(true);

    const extended = await pool.query<{ remaining_ms: number }>(
      `
        SELECT extract(epoch FROM (lease_until - clock_timestamp())) * 1000 AS remaining_ms
        FROM deliveries
        WHERE id = $1
      `,
      [claim.deliveryId],
    );
    expect(Number(extended.rows[0]?.remaining_ms)).toBeGreaterThan(55_000);

    await expect(
      dispatch.heartbeat(
        {
          ...attempt,
          leaseToken: randomUUID(),
        },
        60_000,
      ),
    ).resolves.toBe(false);
    await expect(
      dispatch.heartbeat(
        {
          ...attempt,
          leaseGeneration: attempt.leaseGeneration + 1,
        },
        60_000,
      ),
    ).resolves.toBe(false);

    await dispatch.finish(attempt, { kind: 'ACCEPTED', messageId: 'heartbeat-message' });
    await expect(dispatch.heartbeat(attempt, 60_000)).resolves.toBe(false);
  });

  it('exhausts the 5xx POST budget exactly once and updates failure counters atomically', async () => {
    const alarmId = await emergency([`failure-user-${randomUUID()}`]);
    const claim = await dispatch.claimNext(0, 60_000);
    if (claim === null || claim.alarmId !== alarmId) {
      throw new Error('Expected failure test claim');
    }
    const attempt = await startClaim(claim);
    await pool.query('UPDATE deliveries SET post_budget_count = 5 WHERE id = $1', [
      claim.deliveryId,
    ]);
    await dispatch.finish(attempt, {
      kind: 'TRANSIENT_FAILURE',
      code: 'SERVICE_UNAVAILABLE',
      httpStatus: 503,
    });

    const delivery = await pool.query<{ state: string; result_code: string }>(
      'SELECT state, result_code FROM deliveries WHERE id = $1',
      [claim.deliveryId],
    );
    expect(delivery.rows[0]).toEqual({
      state: 'FAILED_RETRY_EXHAUSTED',
      result_code: 'POST_BUDGET_EXHAUSTED',
    });
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'FAILED',
      failed: 1,
      active: 0,
    });
  });

  it('uses a separate 429 budget and the PostgreSQL clock for its elapsed deadline', async () => {
    const retryAlarmId = await emergency([`rate-retry-${randomUUID()}`]);
    const retryClaim = await dispatch.claimNext(0, 60_000);
    if (retryClaim === null || retryClaim.alarmId !== retryAlarmId) {
      throw new Error('Expected rate-limit retry claim');
    }
    const retryAttempt = await startClaim(retryClaim);
    await dispatch.finish(retryAttempt, {
      kind: 'RATE_LIMITED',
      code: 'RATE_LIMITED',
      httpStatus: 429,
      retryAfterMs: 2_000,
    });
    const retryDelivery = await pool.query<{
      state: string;
      post_budget_count: number;
      rate_limit_count: number;
      priority: number;
    }>(
      `
        SELECT state, post_budget_count, rate_limit_count, priority
        FROM deliveries
        WHERE id = $1
      `,
      [retryClaim.deliveryId],
    );
    expect(retryDelivery.rows[0]).toEqual({
      state: 'RETRY_WAIT',
      post_budget_count: 0,
      rate_limit_count: 1,
      priority: 100,
    });
    await alarms.cancel(retryAlarmId);

    const expiredAlarmId = await emergency([`rate-expired-${randomUUID()}`]);
    const expiredClaim = await dispatch.claimNext(0, 60_000);
    if (expiredClaim === null || expiredClaim.alarmId !== expiredAlarmId) {
      throw new Error('Expected elapsed rate-limit claim');
    }
    const expiredAttempt = await startClaim(expiredClaim);
    await pool.query(
      `
        UPDATE deliveries
        SET first_rate_limited_at = clock_timestamp() - interval '6 minutes'
        WHERE id = $1
      `,
      [expiredClaim.deliveryId],
    );
    await dispatch.finish(expiredAttempt, {
      kind: 'RATE_LIMITED',
      code: 'RATE_LIMITED',
      httpStatus: 429,
      retryAfterMs: 2_000,
    });
    const expiredDelivery = await pool.query<{
      state: string;
      result_code: string;
      post_budget_count: number;
      rate_limit_count: number;
    }>(
      `
        SELECT state, result_code, post_budget_count, rate_limit_count
        FROM deliveries
        WHERE id = $1
      `,
      [expiredClaim.deliveryId],
    );
    expect(expiredDelivery.rows[0]).toEqual({
      state: 'FAILED_RETRY_EXHAUSTED',
      result_code: 'RATE_LIMIT_EXHAUSTED',
      post_budget_count: 0,
      rate_limit_count: 1,
    });
  });

  it('claims 10,000 deliveries exactly once across 20 concurrent claimer loops', async () => {
    const recipients = Array.from(
      { length: 10_000 },
      (_, index) => `db05-${String(index).padStart(5, '0')}-${randomUUID()}`,
    );
    const alarmId = await bulkRecipients(recipients);

    const claimsByWorker = await Promise.all(
      Array.from({ length: 20 }, async () => {
        const deliveryIds: string[] = [];
        while (true) {
          const claim = await dispatch.claimNext(0, 300_000);
          if (claim === null) {
            return deliveryIds;
          }
          if (claim.alarmId !== alarmId) {
            throw new Error(`Claimer selected an unexpected Alarm: ${claim.alarmId}`);
          }
          deliveryIds.push(claim.deliveryId);
        }
      }),
    );
    const allClaimed = claimsByWorker.flat();

    expect(allClaimed).toHaveLength(10_000);
    expect(new Set(allClaimed).size).toBe(10_000);
    const stored = await pool.query<{
      claimed: number;
      distinct_lease_tokens: number;
    }>(
      `
          SELECT
            count(*) FILTER (WHERE state = 'CLAIMED')::integer AS claimed,
            count(DISTINCT lease_token)::integer AS distinct_lease_tokens
          FROM deliveries
          WHERE alarm_id = $1
        `,
      [alarmId],
    );
    expect(stored.rows[0]).toEqual({
      claimed: 10_000,
      distinct_lease_tokens: 10_000,
    });
  }, 120_000);

  it('skips 100 locked head rows and returns the other 900 within 500ms', async () => {
    const recipients = Array.from(
      { length: 1_000 },
      (_, index) => `db06-${String(index).padStart(4, '0')}-${randomUUID()}`,
    );
    const alarmId = await bulkRecipients(recipients);
    const blocker = await pool.connect();
    const contender = await pool.connect();

    try {
      await blocker.query('BEGIN');
      const locked = await blocker.query<{ id: string }>(
        `
          SELECT d.id
          FROM deliveries d
          JOIN alarms a ON a.id = d.alarm_id
          WHERE d.alarm_id = $1
            AND d.state = 'READY'
            AND a.status = 'SENDING'
          ORDER BY
            d.priority DESC,
            d.next_action_at ASC,
            d.alarm_created_at ASC,
            d.id ASC
          FOR UPDATE OF d
          LIMIT 100
        `,
        [alarmId],
      );
      expect(locked.rowCount).toBe(100);

      await contender.query('BEGIN');
      const startedAt = performance.now();
      const skipped = await contender.query<{ id: string }>(
        `
          SELECT d.id
          FROM deliveries d
          JOIN alarms a ON a.id = d.alarm_id
          WHERE d.alarm_id = $1
            AND d.state = 'READY'
            AND d.next_action_at <= clock_timestamp()
            AND a.audience_finalized_at IS NOT NULL
            AND a.status = 'SENDING'
            AND a.cancel_requested_at IS NULL
          ORDER BY
            d.priority DESC,
            d.next_action_at ASC,
            d.alarm_created_at ASC,
            d.id ASC
          FOR UPDATE OF d SKIP LOCKED
          LIMIT 1_000
        `,
        [alarmId],
      );
      const elapsedMs = performance.now() - startedAt;

      const lockedIds = new Set(locked.rows.map(({ id }) => id));
      const skippedIds = new Set(skipped.rows.map(({ id }) => id));
      expect(skipped.rowCount).toBe(900);
      expect(skippedIds.size).toBe(900);
      expect([...skippedIds].some((id) => lockedIds.has(id))).toBe(false);
      expect(new Set([...lockedIds, ...skippedIds]).size).toBe(1_000);
      expect(elapsedMs).toBeLessThan(500);
    } finally {
      await contender.query('ROLLBACK').catch(() => undefined);
      contender.release();
      await blocker.query('ROLLBACK').catch(() => undefined);
      blocker.release();
    }
  });

  it('DB-07 claims only a due nonterminal row from mixed terminal, future, and leased rows', async () => {
    const alarmId = await emergency(
      Array.from({ length: 4 }, (_, index) => `db07-${index}-${randomUUID()}`),
    );
    const rows = await pool.query<{ id: string }>(
      'SELECT id FROM deliveries WHERE alarm_id = $1 ORDER BY id',
      [alarmId],
    );
    const [eligible, terminal, future, leased] = rows.rows;
    if (
      eligible === undefined ||
      terminal === undefined ||
      future === undefined ||
      leased === undefined
    ) {
      throw new Error('Expected four DB-07 deliveries');
    }

    await pool.query(
      `
        UPDATE deliveries
        SET
          state = CASE id
            WHEN $2::uuid THEN 'SENT'
            WHEN $3::uuid THEN 'RETRY_WAIT'
            WHEN $4::uuid THEN 'CLAIMED'
            ELSE 'READY'
          END,
          priority = 32767,
          next_action_at = CASE
            WHEN id = $3::uuid THEN clock_timestamp() + interval '1 hour'
            ELSE clock_timestamp() - interval '1 second'
          END,
          lease_token = CASE WHEN id = $4::uuid THEN $5::uuid ELSE NULL END,
          lease_until = CASE
            WHEN id = $4::uuid THEN clock_timestamp() + interval '1 hour'
            ELSE NULL
          END,
          updated_at = clock_timestamp()
        WHERE alarm_id = $1
      `,
      [alarmId, terminal.id, future.id, leased.id, randomUUID()],
    );

    const claim = await dispatch.claimNext(32767, 60_000);
    expect(claim?.deliveryId).toBe(eligible.id);
    await expect(dispatch.claimNext(32767, 60_000)).resolves.toBeNull();

    const states = await pool.query<{ id: string; state: string }>(
      'SELECT id, state FROM deliveries WHERE alarm_id = $1 ORDER BY id',
      [alarmId],
    );
    expect(new Map(states.rows.map(({ id, state }) => [id, state]))).toEqual(
      new Map([
        [eligible.id, 'CLAIMED'],
        [terminal.id, 'SENT'],
        [future.id, 'RETRY_WAIT'],
        [leased.id, 'CLAIMED'],
      ]),
    );
  });

  it('DB-08 elects one result transition and serializes cancel against terminal completion', async () => {
    const retryRaceAlarmId = await emergency([`db08-retry-${randomUUID()}`]);
    const retryRacePriority = 32_760;
    await pool.query('UPDATE deliveries SET priority = $2 WHERE alarm_id = $1', [
      retryRaceAlarmId,
      retryRacePriority,
    ]);
    const retryRaceClaim = await dispatch.claimNext(retryRacePriority, 60_000);
    if (retryRaceClaim === null || retryRaceClaim.alarmId !== retryRaceAlarmId) {
      throw new Error('Expected DB-08 retry race claim');
    }
    const retryRaceAttempt = await startClaim(retryRaceClaim);

    const serializedPool = new Pool({ connectionString: testDatabase.databaseUrl, max: 1 });
    const serializedDispatch = new PostgresDispatchRepository(serializedPool);
    try {
      await Promise.all([
        serializedDispatch.finish(retryRaceAttempt, {
          kind: 'TRANSIENT_FAILURE',
          code: 'SERVICE_UNAVAILABLE',
          httpStatus: 503,
        }),
        serializedDispatch.finish(retryRaceAttempt, {
          kind: 'ACCEPTED',
          messageId: 'db08-terminal-winner',
        }),
      ]);
    } finally {
      await serializedPool.end();
    }

    const retryRace = await pool.query<{
      delivery_state: string;
      delivery_result: string;
      provider_message_id: string;
      attempt_state: string;
      attempt_result: string;
      sent: number;
      active: number;
      status: string;
      evidence_count: number;
      unconsumed_evidence: number;
      evidence_decision: string;
    }>(
      `
        SELECT
          d.state AS delivery_state,
          d.result_code AS delivery_result,
          d.provider_message_id,
          da.state AS attempt_state,
          da.result_code AS attempt_result,
          a.sent,
          a.active,
          a.status,
          count(e.id)::integer AS evidence_count,
          count(e.id) FILTER (WHERE e.consumed_at IS NULL)::integer AS unconsumed_evidence,
          min(e.decision) AS evidence_decision
        FROM deliveries d
        JOIN alarms a ON a.id = d.alarm_id
        JOIN delivery_attempts da ON da.id = $2
        LEFT JOIN delivery_evidence e ON e.delivery_id = d.id
        WHERE d.id = $1
        GROUP BY d.id, da.id, a.id
      `,
      [retryRaceClaim.deliveryId, retryRaceAttempt.attemptId],
    );
    expect(retryRace.rows[0]).toEqual({
      delivery_state: 'SENT',
      delivery_result: 'LATE_RESPONSE_ACCEPTED',
      provider_message_id: 'db08-terminal-winner',
      attempt_state: 'FINISHED',
      attempt_result: 'SERVICE_UNAVAILABLE',
      sent: 1,
      active: 0,
      status: 'COMPLETED',
      evidence_count: 1,
      unconsumed_evidence: 0,
      evidence_decision: 'LATE_ACCEPTED_SUPERSEDES_RETRY',
    });
    await expect(dispatch.hasEligible(retryRacePriority)).resolves.toBe(false);
    await expect(dispatch.claimNext(retryRacePriority, 60_000)).resolves.toBeNull();

    const cancelRaceAlarmId = await emergency([`db08-cancel-${randomUUID()}`]);
    const cancelRaceClaim = await dispatch.claimNext(0, 60_000);
    if (cancelRaceClaim === null || cancelRaceClaim.alarmId !== cancelRaceAlarmId) {
      throw new Error('Expected DB-08 cancel race claim');
    }
    const cancelRaceAttempt = await startClaim(cancelRaceClaim);
    const [cancelResult, finishResult] = await Promise.allSettled([
      alarms.cancel(cancelRaceAlarmId),
      dispatch.finish(cancelRaceAttempt, {
        kind: 'ACCEPTED',
        messageId: 'db08-committed-terminal',
      }),
    ]);
    expect(finishResult.status).toBe('fulfilled');
    expect(['fulfilled', 'rejected']).toContain(cancelResult.status);

    const cancelRace = await pool.query<{
      delivery_state: string;
      attempt_state: string;
      sent: number;
      failed: number;
      canceled: number;
      active: number;
      status: string;
    }>(
      `
        SELECT
          d.state AS delivery_state,
          da.state AS attempt_state,
          a.sent,
          a.failed,
          a.canceled,
          a.active,
          a.status
        FROM deliveries d
        JOIN alarms a ON a.id = d.alarm_id
        JOIN delivery_attempts da ON da.id = $2
        WHERE d.id = $1
      `,
      [cancelRaceClaim.deliveryId, cancelRaceAttempt.attemptId],
    );
    expect(cancelRace.rows[0]).toEqual({
      delivery_state: 'SENT',
      attempt_state: 'FINISHED',
      sent: 1,
      failed: 0,
      canceled: 0,
      active: 0,
      status: cancelResult.status === 'fulfilled' ? 'CANCELED' : 'COMPLETED',
    });
  });

  it('DB-08 stops a reclaimed PREPARED retry when the prior attempt reports late acceptance', async () => {
    const priority = 32_759;
    const alarmId = await emergency([`db08-prepared-retry-${randomUUID()}`]);
    await pool.query('UPDATE deliveries SET priority = $2 WHERE alarm_id = $1', [
      alarmId,
      priority,
    ]);
    const firstClaim = await dispatch.claimNext(priority, 60_000);
    if (firstClaim === null || firstClaim.alarmId !== alarmId) {
      throw new Error('Expected the first DB-08 PREPARED claim');
    }
    const firstAttempt = await startClaim(firstClaim);
    await dispatch.finish(firstAttempt, {
      kind: 'TRANSIENT_FAILURE',
      code: 'SERVICE_UNAVAILABLE',
      httpStatus: 503,
    });
    await pool.query(
      `
        UPDATE deliveries
        SET priority = $2, next_action_at = clock_timestamp() - interval '1 second'
        WHERE id = $1
      `,
      [firstClaim.deliveryId, priority],
    );

    const retryClaim = await dispatch.claimNext(priority, 60_000);
    if (retryClaim === null || retryClaim.deliveryId !== firstClaim.deliveryId) {
      throw new Error('Expected the reclaimed DB-08 PREPARED retry');
    }
    const retryAttempt = await dispatch.startAttempt(retryClaim, permit());
    if (retryAttempt === null) {
      throw new Error('Expected the DB-08 PREPARED retry attempt');
    }

    await dispatch.finish(firstAttempt, {
      kind: 'ACCEPTED',
      messageId: 'db08-late-before-retry-commit',
    });
    await expect(dispatch.commitAttempt(retryAttempt)).resolves.toBe(false);
    await dispatch.finishNotSent(retryAttempt);

    const state = await pool.query<{
      delivery_state: string;
      delivery_result: string;
      provider_message_id: string;
      post_budget_count: number;
      current_attempt_id: string | null;
      sent: number;
      active: number;
      status: string;
      unfinished_attempts: number;
      unconsumed_evidence: number;
    }>(
      `
        SELECT
          d.state AS delivery_state,
          d.result_code AS delivery_result,
          d.provider_message_id,
          d.post_budget_count,
          d.current_attempt_id,
          a.sent,
          a.active,
          a.status,
          count(DISTINCT da.id) FILTER (WHERE da.state = 'STARTED')::integer
            AS unfinished_attempts,
          count(DISTINCT e.id) FILTER (WHERE e.consumed_at IS NULL)::integer
            AS unconsumed_evidence
        FROM deliveries d
        JOIN alarms a ON a.id = d.alarm_id
        JOIN delivery_attempts da ON da.delivery_id = d.id
        LEFT JOIN delivery_evidence e ON e.delivery_id = d.id
        WHERE d.id = $1
        GROUP BY d.id, a.id
      `,
      [firstClaim.deliveryId],
    );
    expect(state.rows[0]).toEqual({
      delivery_state: 'SENT',
      delivery_result: 'LATE_RESPONSE_ACCEPTED',
      provider_message_id: 'db08-late-before-retry-commit',
      post_budget_count: 1,
      current_attempt_id: null,
      sent: 1,
      active: 0,
      status: 'COMPLETED',
      unfinished_attempts: 0,
      unconsumed_evidence: 0,
    });
    const attempts = await pool.query<{
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
    expect(attempts.rows).toEqual([
      {
        attempt_no: 1,
        state: 'FINISHED',
        request_phase: 'FINISHED',
        budget_consumed: true,
        result_code: 'SERVICE_UNAVAILABLE',
      },
      {
        attempt_no: 2,
        state: 'FINISHED_NOT_SENT',
        request_phase: 'FINISHED_NOT_SENT',
        budget_consumed: false,
        result_code: 'SUPERSEDED_BEFORE_PROVIDER_COMMIT',
      },
    ]);
    await expect(dispatch.claimNext(priority, 60_000)).resolves.toBeNull();
  });

  it('DB-08 closes a reclaimed COMMITTED retry and audits its stale completion as terminal evidence', async () => {
    const priority = 32_758;
    const alarmId = await emergency([`db08-committed-retry-${randomUUID()}`]);
    await pool.query('UPDATE deliveries SET priority = $2 WHERE alarm_id = $1', [
      alarmId,
      priority,
    ]);
    const firstClaim = await dispatch.claimNext(priority, 60_000);
    if (firstClaim === null || firstClaim.alarmId !== alarmId) {
      throw new Error('Expected the first DB-08 COMMITTED claim');
    }
    const firstAttempt = await startClaim(firstClaim);
    await dispatch.finish(firstAttempt, {
      kind: 'TRANSIENT_FAILURE',
      code: 'SERVICE_UNAVAILABLE',
      httpStatus: 503,
    });
    await pool.query(
      `
        UPDATE deliveries
        SET priority = $2, next_action_at = clock_timestamp() - interval '1 second'
        WHERE id = $1
      `,
      [firstClaim.deliveryId, priority],
    );

    const retryClaim = await dispatch.claimNext(priority, 60_000);
    if (retryClaim === null || retryClaim.deliveryId !== firstClaim.deliveryId) {
      throw new Error('Expected the reclaimed DB-08 COMMITTED retry');
    }
    const retryAttempt = await startClaim(retryClaim);

    await dispatch.finish(firstAttempt, {
      kind: 'ACCEPTED',
      messageId: 'db08-first-attempt-late-accepted',
    });
    const beforeRetryResponse = await pool.query<{
      delivery_state: string;
      current_attempt_id: string | null;
      attempt_state: string;
      attempt_result: string;
      transport_class: string;
      unfinished_attempts: number;
      unconsumed_evidence: number;
    }>(
      `
        SELECT
          d.state AS delivery_state,
          d.current_attempt_id,
          da.state AS attempt_state,
          da.result_code AS attempt_result,
          da.transport_class,
          count(*) FILTER (WHERE all_attempts.state = 'STARTED')::integer AS unfinished_attempts,
          count(DISTINCT e.id) FILTER (WHERE e.consumed_at IS NULL)::integer
            AS unconsumed_evidence
        FROM deliveries d
        JOIN delivery_attempts da ON da.id = $2
        JOIN delivery_attempts all_attempts ON all_attempts.delivery_id = d.id
        LEFT JOIN delivery_evidence e ON e.delivery_id = d.id
        WHERE d.id = $1
        GROUP BY d.id, da.id
      `,
      [firstClaim.deliveryId, retryAttempt.attemptId],
    );
    expect(beforeRetryResponse.rows[0]).toEqual({
      delivery_state: 'SENT',
      current_attempt_id: null,
      attempt_state: 'FINISHED',
      attempt_result: 'SUPERSEDED_COMMITTED_BY_LATE_ACCEPTED',
      transport_class: 'SUPERSEDED_COMMITTED_OUTCOME_UNOBSERVED',
      unfinished_attempts: 0,
      unconsumed_evidence: 0,
    });
    await expect(dispatch.claimNext(priority, 60_000)).resolves.toBeNull();
    const beforeIncident = await alarms.findById(alarmId);
    expect(beforeIncident).toMatchObject({
      status: 'COMPLETED',
      sent: 1,
      active: 0,
      duplicateCount: 0,
      incidentCount: 0,
    });

    await dispatch.finish(retryAttempt, {
      kind: 'ACCEPTED',
      messageId: 'db08-second-attempt-distinct-accepted',
    });
    const maintenance = new PostgresMaintenanceRepository(pool);
    await expect(maintenance.adoptUnconsumedEvidence()).resolves.toBe(true);

    const retryAudit = await pool.query<{
      state: string;
      request_phase: string;
      result_code: string;
      provider_message_id: string;
      transport_class: string;
      response_received: boolean;
    }>(
      `
        SELECT
          state,
          request_phase,
          result_code,
          provider_message_id,
          transport_class,
          response_received_at IS NOT NULL AS response_received
        FROM delivery_attempts
        WHERE id = $1
      `,
      [retryAttempt.attemptId],
    );
    expect(retryAudit.rows[0]).toEqual({
      state: 'FINISHED',
      request_phase: 'FINISHED',
      result_code: 'ACCEPTED',
      provider_message_id: 'db08-second-attempt-distinct-accepted',
      transport_class: 'SUPERSEDED_ACCEPTED',
      response_received: true,
    });
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'ATTENTION_REQUIRED',
      sent: 1,
      active: 0,
      duplicateCount: 1,
      incidentCount: 1,
      incidentSummary: {
        count: 1,
        codes: [{ code: 'LATE_DISTINCT_DUPLICATE', count: 1 }],
      },
      finishedAt: beforeIncident?.finishedAt,
    });
  });

  it('[RET-17][DB-08] refunds a superseded COMMITTED retry that later proves not sent', async () => {
    const priority = 32_757;
    const alarmId = await emergency([`db08-committed-dnf-${randomUUID()}`]);
    await pool.query('UPDATE deliveries SET priority = $2 WHERE alarm_id = $1', [
      alarmId,
      priority,
    ]);
    const firstClaim = await dispatch.claimNext(priority, 60_000);
    if (firstClaim === null || firstClaim.alarmId !== alarmId) {
      throw new Error('Expected first superseded definite-not-sent claim');
    }
    const firstAttempt = await startClaim(firstClaim);
    await dispatch.finish(firstAttempt, {
      kind: 'TRANSIENT_FAILURE',
      code: 'SERVICE_UNAVAILABLE',
      httpStatus: 503,
    });
    await pool.query(
      `
        UPDATE deliveries
        SET priority = $2, next_action_at = clock_timestamp() - interval '1 second'
        WHERE id = $1
      `,
      [firstClaim.deliveryId, priority],
    );

    const retryClaim = await dispatch.claimNext(priority, 60_000);
    if (retryClaim === null || retryClaim.deliveryId !== firstClaim.deliveryId) {
      throw new Error('Expected superseded definite-not-sent retry claim');
    }
    const retryAttempt = await startClaim(retryClaim);
    await dispatch.finish(firstAttempt, {
      kind: 'ACCEPTED',
      messageId: 'db08-dnf-first-attempt-late-accepted',
    });

    await dispatch.finish(retryAttempt, {
      kind: 'DEFINITELY_NOT_SENT',
      code: 'PROVIDER_REQUEST_CREATION_ERROR',
    });

    await expect(
      pool.query(
        `
          SELECT
            d.state AS delivery_state,
            d.result_code AS delivery_result,
            d.post_budget_count,
            d.rate_limit_count,
            d.current_attempt_id,
            da.state AS attempt_state,
            da.request_phase,
            da.result_code AS attempt_result,
            da.transport_class,
            da.budget_consumed,
            da.response_received_at IS NOT NULL AS response_received,
            a.status AS alarm_status,
            a.sent,
            a.active,
            a.incident_count
          FROM deliveries d
          JOIN alarms a ON a.id = d.alarm_id
          JOIN delivery_attempts da ON da.id = $2
          WHERE d.id = $1
        `,
        [firstClaim.deliveryId, retryAttempt.attemptId],
      ),
    ).resolves.toMatchObject({
      rows: [
        {
          delivery_state: 'SENT',
          delivery_result: 'LATE_RESPONSE_ACCEPTED',
          post_budget_count: 1,
          rate_limit_count: 0,
          current_attempt_id: null,
          attempt_state: 'FINISHED_NOT_SENT',
          request_phase: 'FINISHED_NOT_SENT',
          attempt_result: 'PROVIDER_REQUEST_CREATION_ERROR',
          transport_class: 'DEFINITELY_NOT_SENT',
          budget_consumed: false,
          response_received: false,
          alarm_status: 'COMPLETED',
          sent: 1,
          active: 0,
          incident_count: 0,
        },
      ],
    });
    await expect(dispatch.claimNext(priority, 60_000)).resolves.toBeNull();
  });
});
