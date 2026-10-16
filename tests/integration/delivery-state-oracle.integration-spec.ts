import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';

import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { AlarmApplicationService } from '../../packages/alarms/src/application/alarm-service';
import type { ProviderSendResult } from '../../packages/alarms/src/application/dispatch';
import { runMigrations } from '../../packages/alarms/src/infrastructure/persistence/migrations';
import { PostgresAlarmRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-alarm-repository';
import { PostgresDispatchRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-dispatch-repository';
import { PostgresMaintenanceRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-maintenance-repository';
import { PostgresReconciliationRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-reconciliation-repository';
import {
  DELIVERY_STATES,
  type DeliveryState,
  decideDeliveryTransitionOracle,
  deriveAlarmStatusOracle,
} from '../support/delivery-transition-model';
import { createTestDatabase, type TestDatabase } from './postgres-test-schema';

const databaseUrl = process.env.TEST_DATABASE_URL;
const describeWithDatabase = databaseUrl === undefined ? describe.skip : describe;

interface DeliveryRow {
  readonly state: DeliveryState;
  readonly attempt_no: number;
  readonly post_budget_count: number;
  readonly rate_limit_count: number;
  readonly client_ref: string;
  readonly recipient_id: string;
}

interface AlarmRow {
  readonly status: string;
  readonly sent: number;
  readonly failed: number;
  readonly canceled: number;
  readonly active: number;
  readonly unresolved: number;
  readonly duplicate_count: number;
  readonly incident_count: number;
}

describeWithDatabase('Delivery state reference oracle against production repositories', () => {
  let testDatabase: TestDatabase;
  let pool: Pool;
  let alarms: AlarmApplicationService;
  let dispatch: PostgresDispatchRepository;
  let maintenance: PostgresMaintenanceRepository;
  let reconciliation: PostgresReconciliationRepository;

  beforeAll(async () => {
    if (databaseUrl === undefined) {
      throw new Error('TEST_DATABASE_URL is required');
    }
    testDatabase = await createTestDatabase(databaseUrl);
    await runMigrations({
      databaseUrl: testDatabase.databaseUrl,
      migrationsDirectory: resolve(process.cwd(), 'migrations'),
    });
    pool = new Pool({ connectionString: testDatabase.databaseUrl, max: 16 });
    alarms = new AlarmApplicationService(
      new PostgresAlarmRepository(pool),
      'delivery-state-oracle-cursor-key',
      randomUUID,
    );
    dispatch = new PostgresDispatchRepository(pool, { random: () => 0 });
    maintenance = new PostgresMaintenanceRepository(pool);
    reconciliation = new PostgresReconciliationRepository(pool);
  });

  afterAll(async () => {
    await pool?.end();
    await testDatabase?.cleanup();
  });

  async function emergency(label: string): Promise<string> {
    const alarm = await alarms.create({
      type: 'EMERGENCY',
      title: label,
      body: 'state oracle body',
      recipientIds: [`${label}-${randomUUID()}`],
    });
    await alarms.start(alarm.id);
    return alarm.id;
  }

  async function deliveryRow(deliveryId: string): Promise<DeliveryRow> {
    const result = await pool.query<DeliveryRow>(
      `
        SELECT
          state, attempt_no, post_budget_count, rate_limit_count,
          client_ref::text, recipient_id
        FROM deliveries
        WHERE id = $1
      `,
      [deliveryId],
    );
    const row = result.rows[0];
    if (row === undefined) {
      throw new Error(`Delivery disappeared: ${deliveryId}`);
    }
    return row;
  }

  async function alarmRow(alarmId: string): Promise<AlarmRow> {
    const result = await pool.query<AlarmRow>(
      `
        SELECT
          status, sent, failed, canceled, active, unresolved,
          duplicate_count, incident_count
        FROM alarms
        WHERE id = $1
      `,
      [alarmId],
    );
    const row = result.rows[0];
    if (row === undefined) {
      throw new Error(`Alarm disappeared: ${alarmId}`);
    }
    return row;
  }

  async function committedAttempt(label: string, preBudget = 0, preRateLimit = 0) {
    const alarmId = await emergency(label);
    if (preBudget > 0 || preRateLimit > 0) {
      await pool.query(
        `
          UPDATE deliveries
          SET post_budget_count = $2, rate_limit_count = $3
          WHERE alarm_id = $1
        `,
        [alarmId, preBudget, preRateLimit],
      );
    }
    const claim = await dispatch.claimNext(0, 60_000);
    if (claim === null || claim.alarmId !== alarmId) {
      throw new Error(`Expected ${label} claim`);
    }
    expect((await deliveryRow(claim.deliveryId)).state).toBe(
      decideDeliveryTransitionOracle('READY', 'CLAIM_DUE').to,
    );

    const beforeStart = await deliveryRow(claim.deliveryId);
    const attempt = await dispatch.startAttempt(claim, {
      reservationId: randomUUID(),
      scheduledMonotonicMs: performance.now(),
    });
    if (attempt === null) {
      throw new Error(`Expected ${label} attempt`);
    }
    const started = decideDeliveryTransitionOracle('CLAIMED', 'START_ATTEMPT');
    const afterStart = await deliveryRow(claim.deliveryId);
    expect(afterStart).toMatchObject({
      state: started.to,
      attempt_no: beforeStart.attempt_no + started.attemptNoDelta,
      post_budget_count: beforeStart.post_budget_count + started.postBudgetDelta,
      client_ref: beforeStart.client_ref,
      recipient_id: beforeStart.recipient_id,
    });
    await expect(dispatch.commitAttempt(attempt)).resolves.toBe(true);
    expect((await deliveryRow(claim.deliveryId)).state).toBe(
      decideDeliveryTransitionOracle('IN_FLIGHT', 'COMMIT_PROVIDER').to,
    );
    return { alarmId, attempt, beforeResult: afterStart };
  }

  it.each([
    {
      name: 'accepted',
      event: 'PROVIDER_ACCEPTED',
      result: { kind: 'ACCEPTED', messageId: 'oracle-accepted' },
      preBudget: 0,
      preRateLimit: 0,
    },
    {
      name: 'permanent failure',
      event: 'PROVIDER_PERMANENT_FAILURE',
      result: { kind: 'PERMANENT_FAILURE', code: 'RECIPIENT_BLOCKED', httpStatus: 400 },
      preBudget: 0,
      preRateLimit: 0,
    },
    {
      name: 'rate limited retry',
      event: 'PROVIDER_RATE_LIMITED_RETRY',
      result: {
        kind: 'RATE_LIMITED',
        code: 'RATE_LIMITED',
        httpStatus: 429,
        retryAfterMs: 60_000,
      },
      preBudget: 0,
      preRateLimit: 0,
    },
    {
      name: 'rate limit exhausted',
      event: 'PROVIDER_RATE_LIMITED_EXHAUSTED',
      result: {
        kind: 'RATE_LIMITED',
        code: 'RATE_LIMITED',
        httpStatus: 429,
        retryAfterMs: 1,
      },
      preBudget: 0,
      preRateLimit: 19,
    },
    {
      name: 'definitely not sent at fifth POST-budget boundary',
      event: 'PROVIDER_DEFINITELY_NOT_SENT_RETRY',
      result: {
        kind: 'DEFINITELY_NOT_SENT',
        code: 'PROVIDER_REQUEST_CREATION_ERROR',
      },
      preBudget: 4,
      preRateLimit: 0,
    },
    {
      name: 'transient retry',
      event: 'PROVIDER_TRANSIENT_RETRY',
      result: { kind: 'TRANSIENT_FAILURE', code: 'PROVIDER_503', httpStatus: 503 },
      preBudget: 0,
      preRateLimit: 0,
    },
    {
      name: 'transient exhausted',
      event: 'PROVIDER_TRANSIENT_EXHAUSTED',
      result: { kind: 'TRANSIENT_FAILURE', code: 'PROVIDER_500', httpStatus: 500 },
      preBudget: 4,
      preRateLimit: 0,
    },
    {
      name: 'ambiguous',
      event: 'PROVIDER_AMBIGUOUS',
      result: { kind: 'AMBIGUOUS', code: 'PROVIDER_TIMEOUT' },
      preBudget: 0,
      preRateLimit: 0,
    },
  ] as const)(
    'SND-07 cross-checks production dispatch result: $name',
    async ({ name, event, result, preBudget, preRateLimit }) => {
      const { alarmId, attempt, beforeResult } = await committedAttempt(
        `oracle-${name}`,
        preBudget,
        preRateLimit,
      );
      const expected = decideDeliveryTransitionOracle('IN_FLIGHT', event);

      await dispatch.finish(attempt, result as ProviderSendResult);

      const delivery = await deliveryRow(attempt.deliveryId);
      expect(delivery).toMatchObject({
        state: expected.to,
        attempt_no: beforeResult.attempt_no,
        post_budget_count: beforeResult.post_budget_count + expected.postBudgetDelta,
        rate_limit_count: beforeResult.rate_limit_count + expected.rateLimitDelta,
        client_ref: beforeResult.client_ref,
        recipient_id: beforeResult.recipient_id,
      });
      const aggregate = await alarmRow(alarmId);
      expect(aggregate).toMatchObject({
        sent: expected.counterDelta.sent,
        failed: expected.counterDelta.failed,
        canceled: expected.counterDelta.canceled,
        active: 1 + expected.counterDelta.active,
        unresolved: expected.counterDelta.unresolved,
        duplicate_count: expected.counterDelta.duplicate,
        incident_count: expected.counterDelta.incident,
      });
      expect(aggregate.status).toBe(
        deriveAlarmStatusOracle({
          audienceFinalized: true,
          currentStatus: 'SENDING',
          cancelRequested: false,
          total: 1,
          sent: aggregate.sent,
          failed: aggregate.failed,
          canceled: aggregate.canceled,
          active: aggregate.active,
          unresolved: aggregate.unresolved,
          duplicateCount: aggregate.duplicate_count,
          incidentCount: aggregate.incident_count,
        }),
      );

      if (result.kind === 'DEFINITELY_NOT_SENT') {
        await expect(
          pool.query(
            `
              SELECT state, request_phase, transport_class, budget_consumed
              FROM delivery_attempts
              WHERE id = $1
            `,
            [attempt.attemptId],
          ),
        ).resolves.toMatchObject({
          rows: [
            {
              state: 'FINISHED_NOT_SENT',
              request_phase: 'FINISHED_NOT_SENT',
              transport_class: 'DEFINITELY_NOT_SENT',
              budget_consumed: false,
            },
          ],
        });
      }

      if (delivery.state === 'RETRY_WAIT' || delivery.state === 'UNKNOWN') {
        await pool.query(
          `UPDATE deliveries SET next_action_at = clock_timestamp() + interval '1 day' WHERE id = $1`,
          [attempt.deliveryId],
        );
      }
    },
  );

  it.each([
    { name: 'normal release', cancel: false, event: 'FINISH_NOT_SENT_RETRY' },
    { name: 'cancel release', cancel: true, event: 'FINISH_NOT_SENT_CANCELED' },
  ] as const)(
    'SND-07 cross-checks production PREPARED finish-not-sent: $name',
    async ({ name, cancel, event }) => {
      const alarmId = await emergency(`oracle-${name}`);
      const claim = await dispatch.claimNext(0, 60_000);
      if (claim === null || claim.alarmId !== alarmId) {
        throw new Error(`Expected ${name} claim`);
      }
      const attempt = await dispatch.startAttempt(claim, {
        reservationId: randomUUID(),
        scheduledMonotonicMs: performance.now(),
      });
      if (attempt === null) {
        throw new Error(`Expected ${name} attempt`);
      }
      if (cancel) {
        await alarms.cancel(alarmId);
      }
      const expected = decideDeliveryTransitionOracle('IN_FLIGHT', event);

      await dispatch.finishNotSent(attempt);

      const delivery = await deliveryRow(claim.deliveryId);
      expect(delivery).toMatchObject({
        state: expected.to,
        attempt_no: 1,
        post_budget_count: 1 + expected.postBudgetDelta,
      });
      const attemptAudit = await pool.query<{ state: string; request_phase: string }>(
        'SELECT state, request_phase FROM delivery_attempts WHERE id = $1',
        [attempt.attemptId],
      );
      expect(attemptAudit.rows[0]).toEqual({
        state: expected.attemptEffect,
        request_phase: expected.attemptEffect,
      });
      if (delivery.state === 'READY') {
        await pool.query(
          `UPDATE deliveries SET next_action_at = clock_timestamp() + interval '1 day' WHERE id = $1`,
          [attempt.deliveryId],
        );
      }
    },
  );

  async function insertFixture(
    state: DeliveryState,
    options: {
      readonly status?: string;
      readonly cancelRequested?: boolean;
      readonly sent?: number;
      readonly failed?: number;
      readonly canceled?: number;
      readonly active?: number;
      readonly unresolved?: number;
      readonly providerMessageId?: string | null;
      readonly dueReview?: boolean;
      readonly expiredReconcile?: boolean;
    } = {},
  ) {
    const alarmId = randomUUID();
    const deliveryId = randomUUID();
    const recipientId = `fixture-${randomUUID()}`;
    const sent = options.sent ?? 0;
    const failed = options.failed ?? 0;
    const canceled = options.canceled ?? 0;
    const active = options.active ?? 1;
    const unresolved = options.unresolved ?? 0;
    const total = sent + failed + canceled + active + unresolved;
    const cancelRequested = options.cancelRequested === true;
    await pool.query(
      `
        INSERT INTO alarms(
          id, type, title, body, status, cancel_requested_at,
          audience_finalized_at, finished_at, total,
          sent, failed, canceled, active, unresolved
        )
        VALUES (
          $1, 'EMERGENCY', 'fixture', 'body', $2,
          CASE WHEN $3::boolean THEN clock_timestamp() ELSE NULL END,
          clock_timestamp(),
          CASE WHEN $4::integer = 0 THEN clock_timestamp() ELSE NULL END,
          $5, $6, $7, $8, $4, $9
        )
      `,
      [
        alarmId,
        options.status ?? (cancelRequested ? 'CANCEL_REQUESTED' : 'SENDING'),
        cancelRequested,
        active,
        total,
        sent,
        failed,
        canceled,
        unresolved,
      ],
    );
    await pool.query(
      `
        INSERT INTO deliveries(
          id, alarm_id, recipient_id, client_ref, state, priority, alarm_created_at,
          provider_message_id, next_action_at, next_review_at, review_until,
          reconcile_generation, reconcile_token, reconcile_until
        )
        VALUES (
          $1, $2, $3, $4, $5, 100, clock_timestamp(), $6,
          clock_timestamp(),
          CASE WHEN $7::boolean THEN clock_timestamp() ELSE NULL END,
          CASE WHEN $7::boolean THEN clock_timestamp() + interval '1 day' ELSE NULL END,
          CASE WHEN $8::boolean THEN 1 ELSE 0 END,
          CASE WHEN $8::boolean THEN $9::uuid ELSE NULL END,
          CASE WHEN $8::boolean THEN clock_timestamp() - interval '1 second' ELSE NULL END
        )
      `,
      [
        deliveryId,
        alarmId,
        recipientId,
        randomUUID(),
        state,
        options.providerMessageId ?? null,
        options.dueReview === true,
        options.expiredReconcile === true,
        randomUUID(),
      ],
    );
    return { alarmId, deliveryId, recipientId };
  }

  it.each(DELIVERY_STATES)(
    'SND-07 cross-checks cancel sweeper eligibility from %s',
    async (state) => {
      const fixture = await insertFixture(state, {
        status: 'CANCEL_REQUESTED',
        cancelRequested: true,
      });
      const allowed = ['READY', 'RETRY_WAIT', 'CLAIMED'].includes(state);

      await expect(maintenance.sweepCanceledDeliveries(1)).resolves.toBe(allowed ? 1 : 0);

      const actual = await deliveryRow(fixture.deliveryId);
      if (allowed) {
        expect(actual.state).toBe(decideDeliveryTransitionOracle(state, 'CANCEL_SWEEP').to);
      } else {
        expect(actual.state).toBe(state);
      }
    },
  );

  it.each(DELIVERY_STATES)(
    'SND-07 cross-checks reconciliation claim eligibility from %s',
    async (state) => {
      await pool.query(`
        UPDATE deliveries
        SET
          next_action_at = clock_timestamp() + interval '1 day',
          next_review_at = CASE
            WHEN next_review_at IS NULL THEN NULL
            ELSE clock_timestamp() + interval '1 day'
          END,
          reconcile_until = CASE
            WHEN reconcile_token IS NULL THEN NULL
            ELSE clock_timestamp() + interval '1 day'
          END
        WHERE state IN ('UNKNOWN', 'RECONCILING', 'NEEDS_REVIEW')
      `);
      const fixture = await insertFixture(state, {
        status: state === 'NEEDS_REVIEW' ? 'ATTENTION_REQUIRED' : 'SENDING',
        active: state === 'NEEDS_REVIEW' ? 0 : 1,
        unresolved: state === 'NEEDS_REVIEW' ? 1 : 0,
        dueReview: state === 'NEEDS_REVIEW',
        expiredReconcile: state === 'RECONCILING',
      });
      const allowed = ['UNKNOWN', 'RECONCILING', 'NEEDS_REVIEW'].includes(state);

      const claim = await reconciliation.claimNext(15_000);
      if (!allowed) {
        expect(claim).toBeNull();
        expect((await deliveryRow(fixture.deliveryId)).state).toBe(state);
        return;
      }
      expect(claim?.deliveryId).toBe(fixture.deliveryId);
      expect((await deliveryRow(fixture.deliveryId)).state).toBe(
        decideDeliveryTransitionOracle(state, 'RECONCILE_CLAIM').to,
      );
    },
  );

  it.each([
    { name: 'expired claim release', phase: 'CLAIMED', cancel: false, event: 'RELEASE_CLAIM' },
    {
      name: 'expired canceled claim release',
      phase: 'CLAIMED',
      cancel: true,
      event: 'RELEASE_CLAIM_CANCELED',
    },
    {
      name: 'expired prepared attempt release',
      phase: 'PREPARED',
      cancel: false,
      event: 'FINISH_NOT_SENT_RETRY',
    },
    {
      name: 'expired canceled prepared attempt release',
      phase: 'PREPARED',
      cancel: true,
      event: 'FINISH_NOT_SENT_CANCELED',
    },
    {
      name: 'expired committed attempt uncertainty',
      phase: 'COMMITTED',
      cancel: false,
      event: 'LEASE_EXPIRED_COMMITTED',
    },
  ] as const)(
    'SND-07 cross-checks production send-lease transition: $name',
    async ({ name, phase, cancel, event }) => {
      await pool.query('TRUNCATE alarms CASCADE');
      const alarmId = await emergency(`oracle-${name}`);
      const claim = await dispatch.claimNext(0, 60_000);
      if (claim === null || claim.alarmId !== alarmId) {
        throw new Error(`Expected ${name} claim`);
      }

      let attemptId: string | null = null;
      if (phase !== 'CLAIMED') {
        const attempt = await dispatch.startAttempt(claim, {
          reservationId: randomUUID(),
          scheduledMonotonicMs: performance.now(),
        });
        if (attempt === null) {
          throw new Error(`Expected ${name} attempt`);
        }
        attemptId = attempt.attemptId;
        if (phase === 'COMMITTED') {
          await expect(dispatch.commitAttempt(attempt)).resolves.toBe(true);
        }
      }
      if (cancel) {
        await alarms.cancel(alarmId);
      }

      const beforeDelivery = await deliveryRow(claim.deliveryId);
      const beforeAlarm = await alarmRow(alarmId);
      await pool.query(
        `UPDATE deliveries SET lease_until = clock_timestamp() - interval '1 second' WHERE id = $1`,
        [claim.deliveryId],
      );
      const expected = decideDeliveryTransitionOracle(
        phase === 'CLAIMED' ? 'CLAIMED' : 'IN_FLIGHT',
        event,
      );

      await expect(maintenance.reapExpiredSendLease()).resolves.not.toBeNull();

      expect(await deliveryRow(claim.deliveryId)).toMatchObject({
        state: expected.to,
        attempt_no: beforeDelivery.attempt_no,
        post_budget_count: beforeDelivery.post_budget_count + expected.postBudgetDelta,
        rate_limit_count: beforeDelivery.rate_limit_count + expected.rateLimitDelta,
        client_ref: beforeDelivery.client_ref,
        recipient_id: beforeDelivery.recipient_id,
      });
      expect(await alarmRow(alarmId)).toMatchObject({
        sent: beforeAlarm.sent + expected.counterDelta.sent,
        failed: beforeAlarm.failed + expected.counterDelta.failed,
        canceled: beforeAlarm.canceled + expected.counterDelta.canceled,
        active: beforeAlarm.active + expected.counterDelta.active,
        unresolved: beforeAlarm.unresolved + expected.counterDelta.unresolved,
        duplicate_count: beforeAlarm.duplicate_count + expected.counterDelta.duplicate,
        incident_count: beforeAlarm.incident_count + expected.counterDelta.incident,
      });

      if (attemptId !== null) {
        const audit = await pool.query<{ state: string; request_phase: string }>(
          'SELECT state, request_phase FROM delivery_attempts WHERE id = $1',
          [attemptId],
        );
        expect(audit.rows[0]).toEqual(
          phase === 'PREPARED'
            ? { state: 'FINISHED_NOT_SENT', request_phase: 'FINISHED_NOT_SENT' }
            : { state: 'STARTED', request_phase: 'COMMITTED' },
        );
      }
    },
  );

  it('SND-07 keeps every non-send-lease source state invariant under the production reaper', async () => {
    for (const state of DELIVERY_STATES.filter(
      (candidate) => !['CLAIMED', 'IN_FLIGHT'].includes(candidate),
    )) {
      await pool.query('TRUNCATE alarms CASCADE');
      const terminalOptions =
        state === 'SENT'
          ? { status: 'COMPLETED', sent: 1, active: 0 }
          : state === 'FAILED_PERMANENT' || state === 'FAILED_RETRY_EXHAUSTED'
            ? { status: 'FAILED', failed: 1, active: 0 }
            : state === 'CANCELED'
              ? {
                  status: 'CANCELED',
                  cancelRequested: true,
                  canceled: 1,
                  active: 0,
                }
              : state === 'NEEDS_REVIEW'
                ? { status: 'ATTENTION_REQUIRED', unresolved: 1, active: 0 }
                : {};
      const fixture = await insertFixture(state, terminalOptions);
      await pool.query(
        `
          UPDATE deliveries
          SET
            lease_generation = 17,
            lease_token = $2,
            lease_until = clock_timestamp() - interval '1 second'
          WHERE id = $1
        `,
        [fixture.deliveryId, randomUUID()],
      );
      const snapshot = async () =>
        (
          await pool.query(
            `
              SELECT
                d.state, d.attempt_no, d.post_budget_count, d.rate_limit_count,
                d.current_attempt_id::text, d.lease_generation, d.lease_token::text,
                d.lease_until::text, d.result_code,
                a.status, a.sent, a.failed, a.canceled, a.active, a.unresolved,
                a.duplicate_count, a.incident_count, a.version::text
              FROM deliveries d
              JOIN alarms a ON a.id = d.alarm_id
              WHERE d.id = $1
            `,
            [fixture.deliveryId],
          )
        ).rows[0];
      const before = await snapshot();

      await expect(maintenance.reapExpiredSendLease()).resolves.toBeNull();

      expect(await snapshot(), state).toEqual(before);
    }
  });

  it('SND-07 fences a stale reconciliation result across every non-owned source state', async () => {
    for (const state of DELIVERY_STATES) {
      await pool.query('TRUNCATE alarms CASCADE');
      const fixture = await insertFixture('UNKNOWN');
      const claim = await reconciliation.claimNext(15_000);
      if (claim === null || claim.deliveryId !== fixture.deliveryId) {
        throw new Error(`Expected reconciliation fixture for ${state}`);
      }
      await pool.query('UPDATE deliveries SET state = $2 WHERE id = $1', [
        fixture.deliveryId,
        state,
      ]);
      const snapshot = async () =>
        (
          await pool.query<{
            state: DeliveryState;
            reconcile_count: number;
            reconcile_generation: number;
            reconcile_token: string | null;
            result_code: string | null;
            sent: number;
            failed: number;
            canceled: number;
            active: number;
            unresolved: number;
            duplicate_count: number;
            incident_count: number;
            version: string;
          }>(
            `
              SELECT
                d.state, d.reconcile_count, d.reconcile_generation,
                d.reconcile_token::text, d.result_code,
                a.sent, a.failed, a.canceled, a.active, a.unresolved,
                a.duplicate_count, a.incident_count, a.version::text
              FROM deliveries d
              JOIN alarms a ON a.id = d.alarm_id
              WHERE d.id = $1
            `,
            [fixture.deliveryId],
          )
        ).rows[0];
      const before = await snapshot();

      await reconciliation.record(claim, { kind: 'UNAVAILABLE', code: 'STATE_FENCE_TEST' });

      const after = await snapshot();
      if (state !== 'RECONCILING') {
        expect(after, state).toEqual(before);
        continue;
      }
      const expected = decideDeliveryTransitionOracle('RECONCILING', 'RECONCILE_NO_MATCH_RETRY');
      expect(after).toMatchObject({
        state: expected.to,
        reconcile_count: (before?.reconcile_count ?? 0) + 1,
        reconcile_generation: before?.reconcile_generation,
        reconcile_token: null,
        result_code: 'STATE_FENCE_TEST',
        sent: (before?.sent ?? 0) + expected.counterDelta.sent,
        failed: (before?.failed ?? 0) + expected.counterDelta.failed,
        canceled: (before?.canceled ?? 0) + expected.counterDelta.canceled,
        active: (before?.active ?? 0) + expected.counterDelta.active,
        unresolved: (before?.unresolved ?? 0) + expected.counterDelta.unresolved,
        duplicate_count: (before?.duplicate_count ?? 0) + expected.counterDelta.duplicate,
        incident_count: (before?.incident_count ?? 0) + expected.counterDelta.incident,
        version: before?.version,
      });
    }
  });

  it('U-SM-03 cross-checks every true-terminal state against compatible and anomalous evidence', async () => {
    const cases = [
      {
        state: 'SENT',
        status: 'COMPLETED',
        sent: 1,
        failed: 0,
        canceled: 0,
        cancelRequested: false,
        providerMessageId: 'terminal-original',
        anomalyEvent: 'TERMINAL_DISTINCT_DUPLICATE_FIRST',
        anomalyMessageId: 'terminal-distinct',
      },
      {
        state: 'FAILED_PERMANENT',
        status: 'FAILED',
        sent: 0,
        failed: 1,
        canceled: 0,
        cancelRequested: false,
        providerMessageId: null,
        anomalyEvent: 'TERMINAL_MATCHING_POSITIVE_FIRST',
        anomalyMessageId: 'terminal-positive-permanent',
      },
      {
        state: 'FAILED_RETRY_EXHAUSTED',
        status: 'FAILED',
        sent: 0,
        failed: 1,
        canceled: 0,
        cancelRequested: false,
        providerMessageId: null,
        anomalyEvent: 'TERMINAL_MATCHING_POSITIVE_FIRST',
        anomalyMessageId: 'terminal-positive-exhausted',
      },
      {
        state: 'CANCELED',
        status: 'CANCELED',
        sent: 0,
        failed: 0,
        canceled: 1,
        cancelRequested: true,
        providerMessageId: null,
        anomalyEvent: 'TERMINAL_MATCHING_POSITIVE_FIRST',
        anomalyMessageId: 'terminal-positive-canceled',
      },
    ] as const;

    for (const entry of cases) {
      const fixture = await insertFixture(entry.state, {
        status: entry.status,
        cancelRequested: entry.cancelRequested,
        sent: entry.sent,
        failed: entry.failed,
        canceled: entry.canceled,
        active: 0,
        providerMessageId: entry.providerMessageId,
      });
      await pool.query(
        `
          INSERT INTO delivery_evidence(
            id, delivery_id, source, observation_key, payload_hash, entries, entry_count
          )
          VALUES ($1, $2, 'HISTORY', $3, $4, '[]'::jsonb, 0)
        `,
        [randomUUID(), fixture.deliveryId, randomUUID(), randomUUID()],
      );
      await expect(maintenance.adoptUnconsumedEvidence()).resolves.toBe(true);
      const compatible = decideDeliveryTransitionOracle(
        entry.state,
        'TERMINAL_COMPATIBLE_EVIDENCE',
      );
      expect((await deliveryRow(fixture.deliveryId)).state).toBe(compatible.to);
      expect(await alarmRow(fixture.alarmId)).toMatchObject({
        sent: entry.sent,
        failed: entry.failed,
        canceled: entry.canceled,
        incident_count: 0,
      });

      await pool.query(
        `
          INSERT INTO delivery_evidence(
            id, delivery_id, source, observation_key, payload_hash, entries, entry_count
          )
          VALUES ($1, $2, 'HISTORY', $3, $4, $5::jsonb, 1)
        `,
        [
          randomUUID(),
          fixture.deliveryId,
          randomUUID(),
          randomUUID(),
          JSON.stringify([{ messageId: entry.anomalyMessageId, recipientId: fixture.recipientId }]),
        ],
      );
      await expect(maintenance.adoptUnconsumedEvidence()).resolves.toBe(true);
      const anomaly = decideDeliveryTransitionOracle(entry.state, entry.anomalyEvent);
      expect((await deliveryRow(fixture.deliveryId)).state).toBe(anomaly.to);
      const afterAnomaly = await alarmRow(fixture.alarmId);
      expect(afterAnomaly).toMatchObject({
        sent: entry.sent,
        failed: entry.failed,
        canceled: entry.canceled,
        duplicate_count: anomaly.counterDelta.duplicate,
        incident_count: anomaly.counterDelta.incident,
        status: 'ATTENTION_REQUIRED',
      });

      await pool.query(
        `
          INSERT INTO delivery_evidence(
            id, delivery_id, source, observation_key, payload_hash, entries, entry_count
          )
          VALUES ($1, $2, 'HISTORY', $3, $4, $5::jsonb, 1)
        `,
        [
          randomUUID(),
          fixture.deliveryId,
          randomUUID(),
          randomUUID(),
          JSON.stringify([
            { messageId: `${entry.anomalyMessageId}-repeat`, recipientId: fixture.recipientId },
          ]),
        ],
      );
      await expect(maintenance.adoptUnconsumedEvidence()).resolves.toBe(true);
      const repeated = decideDeliveryTransitionOracle(entry.state, 'TERMINAL_ANOMALY_REPEAT');
      expect(await alarmRow(fixture.alarmId)).toMatchObject({
        sent: entry.sent,
        failed: entry.failed,
        canceled: entry.canceled,
        duplicate_count: anomaly.counterDelta.duplicate + repeated.counterDelta.duplicate,
        incident_count: anomaly.counterDelta.incident + repeated.counterDelta.incident,
      });
    }
  });

  it('U-SM-03 cross-checks ATTENTION_REQUIRED correction only on positive review evidence', async () => {
    const fixture = await insertFixture('NEEDS_REVIEW', {
      status: 'ATTENTION_REQUIRED',
      active: 0,
      unresolved: 1,
      dueReview: true,
    });
    const claim = await reconciliation.claimNext(15_000);
    if (claim === null || claim.deliveryId !== fixture.deliveryId) {
      throw new Error('Expected review claim');
    }
    const expected = decideDeliveryTransitionOracle('NEEDS_REVIEW', 'REVIEW_MATCH_ONE');
    await reconciliation.record(claim, {
      kind: 'OBSERVATION',
      entries: [
        {
          messageId: 'review-positive',
          recipientId: fixture.recipientId,
          sentAt: '2026-10-15T00:00:00.000Z',
        },
      ],
    });

    expect((await deliveryRow(fixture.deliveryId)).state).toBe(expected.to);
    const aggregate = await alarmRow(fixture.alarmId);
    expect(aggregate).toMatchObject({
      status: 'COMPLETED',
      sent: expected.counterDelta.sent,
      active: 0,
      unresolved: 1 + expected.counterDelta.unresolved,
    });
  });
});
