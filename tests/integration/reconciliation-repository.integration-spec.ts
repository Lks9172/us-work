import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';

import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { AlarmApplicationService } from '../../packages/alarms/src/application/alarm-service';
import { runMigrations } from '../../packages/alarms/src/infrastructure/persistence/migrations';
import { PostgresAlarmRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-alarm-repository';
import { PostgresDispatchRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-dispatch-repository';
import { PostgresMaintenanceRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-maintenance-repository';
import { PostgresReconciliationRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-reconciliation-repository';
import { createTestDatabase, type TestDatabase } from './postgres-test-schema';

const databaseUrl = process.env.TEST_DATABASE_URL;
const describeWithDatabase = databaseUrl === undefined ? describe.skip : describe;

describeWithDatabase('PostgresReconciliationRepository', () => {
  let testDatabase: TestDatabase;
  let pool: Pool;
  let alarms: AlarmApplicationService;
  let dispatch: PostgresDispatchRepository;
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
    pool = new Pool({ connectionString: testDatabase.databaseUrl, max: 12 });
    alarms = new AlarmApplicationService(
      new PostgresAlarmRepository(pool),
      'reconciliation-integration-cursor-key',
      randomUUID,
    );
    dispatch = new PostgresDispatchRepository(pool);
    reconciliation = new PostgresReconciliationRepository(pool);
  });

  afterAll(async () => {
    await pool?.end();
    await testDatabase?.cleanup();
  });

  async function createUnknown(): Promise<{
    readonly alarmId: string;
    readonly deliveryId: string;
    readonly recipientId: string;
  }> {
    const recipientId = `reconcile-user-${randomUUID()}`;
    const alarm = await alarms.create({
      type: 'EMERGENCY',
      title: `reconcile-${randomUUID()}`,
      body: 'body',
      recipientIds: [recipientId],
    });
    await alarms.start(alarm.id);
    const claim = await dispatch.claimNext(0, 60_000);
    if (claim === null || claim.alarmId !== alarm.id) {
      throw new Error('Expected dispatch claim for reconciliation');
    }
    const attempt = await dispatch.startAttempt(claim, {
      reservationId: randomUUID(),
      scheduledMonotonicMs: performance.now(),
    });
    if (attempt === null) {
      throw new Error('Expected dispatch attempt for reconciliation');
    }
    if (!(await dispatch.commitAttempt(attempt))) {
      throw new Error('Expected committed dispatch attempt for reconciliation');
    }
    await dispatch.finish(attempt, { kind: 'AMBIGUOUS', code: 'PROVIDER_TIMEOUT' });
    await pool.query('UPDATE deliveries SET next_action_at = clock_timestamp() WHERE id = $1', [
      claim.deliveryId,
    ]);
    return { alarmId: alarm.id, deliveryId: claim.deliveryId, recipientId };
  }

  async function exposeAuthoritativeRetryBeforeStalePositive(): Promise<{
    readonly strong: PostgresReconciliationRepository;
    readonly stale: NonNullable<Awaited<ReturnType<PostgresReconciliationRepository['claimNext']>>>;
    readonly target: Awaited<ReturnType<typeof createUnknown>>;
  }> {
    const strong = new PostgresReconciliationRepository(pool, {
      historyVisibilityProfile: 'AUTHORITATIVE_EMPTY_AFTER_GRACE',
      authoritativeEmptyGraceMs: 0,
    });
    const target = await createUnknown();
    const stale = await strong.claimNext(15_000);
    if (stale === null || stale.deliveryId !== target.deliveryId) {
      throw new Error('Expected stale strong-visibility reconciliation claim');
    }
    await pool.query(
      `UPDATE deliveries SET reconcile_until = clock_timestamp() - interval '1 second' WHERE id = $1`,
      [target.deliveryId],
    );
    const current = await strong.claimNext(15_000);
    if (current === null || current.deliveryId !== target.deliveryId) {
      throw new Error('Expected current strong-visibility reconciliation claim');
    }
    await strong.record(current, { kind: 'OBSERVATION', entries: [] });
    await expect(
      pool.query('SELECT state, current_attempt_id FROM deliveries WHERE id = $1', [
        target.deliveryId,
      ]),
    ).resolves.toMatchObject({
      rows: [{ state: 'RETRY_WAIT', current_attempt_id: null }],
    });
    return { strong, stale, target };
  }

  async function recordStalePositive(
    strong: PostgresReconciliationRepository,
    stale: NonNullable<Awaited<ReturnType<PostgresReconciliationRepository['claimNext']>>>,
    recipientId: string,
    messageId: string,
  ): Promise<void> {
    await strong.record(stale, {
      kind: 'OBSERVATION',
      entries: [{ messageId, recipientId, sentAt: '2026-10-15T00:00:00.000Z' }],
    });
  }

  it('treats empty opaque history as non-authoritative and bounds UNKNOWN', async () => {
    const target = await createUnknown();
    const first = await reconciliation.claimNext(15_000);
    if (first === null || first.deliveryId !== target.deliveryId) {
      throw new Error('Expected first reconciliation claim');
    }
    await reconciliation.record(first, { kind: 'OBSERVATION', entries: [] });

    const firstState = await pool.query<{ state: string; reconcile_count: number }>(
      'SELECT state, reconcile_count FROM deliveries WHERE id = $1',
      [target.deliveryId],
    );
    expect(firstState.rows[0]).toEqual({ state: 'UNKNOWN', reconcile_count: 1 });

    await pool.query(
      `
        UPDATE deliveries
        SET reconcile_count = 11, next_action_at = clock_timestamp()
        WHERE id = $1
      `,
      [target.deliveryId],
    );
    const last = await reconciliation.claimNext(15_000);
    if (last === null || last.deliveryId !== target.deliveryId) {
      throw new Error('Expected final fast reconciliation claim');
    }
    await reconciliation.record(last, { kind: 'OBSERVATION', entries: [] });

    const alarm = await alarms.findById(target.alarmId);
    const delivery = await pool.query<{
      state: string;
      reconcile_count: number;
      current_attempt_id: string | null;
      attempt_state: string;
      request_phase: string;
    }>(
      `
        SELECT
          d.state,
          d.reconcile_count,
          d.current_attempt_id,
          da.state AS attempt_state,
          da.request_phase
        FROM deliveries d
        JOIN delivery_attempts da ON da.delivery_id = d.id
        WHERE d.id = $1
      `,
      [target.deliveryId],
    );
    expect(delivery.rows[0]).toEqual({
      state: 'NEEDS_REVIEW',
      reconcile_count: 12,
      current_attempt_id: null,
      attempt_state: 'FINISHED',
      request_phase: 'FINISHED',
    });
    expect(alarm).toMatchObject({
      status: 'ATTENTION_REQUIRED',
      active: 0,
      unresolved: 1,
    });
  });

  it('corrects NEEDS_REVIEW only after positive history evidence', async () => {
    const target = await createUnknown();
    await pool.query(
      `
        UPDATE deliveries
        SET
          state = 'NEEDS_REVIEW',
          reconcile_count = 12,
          next_review_at = clock_timestamp(),
          review_until = clock_timestamp() + interval '24 hours',
          next_action_at = clock_timestamp()
        WHERE id = $1;
      `,
      [target.deliveryId],
    );
    await pool.query(
      `
        UPDATE alarms
        SET active = 0, unresolved = 1, status = 'ATTENTION_REQUIRED'
        WHERE id = $1
      `,
      [target.alarmId],
    );

    const claim = await reconciliation.claimNext(15_000);
    if (claim === null || claim.deliveryId !== target.deliveryId) {
      throw new Error('Expected review claim');
    }
    await reconciliation.record(claim, {
      kind: 'OBSERVATION',
      entries: [
        {
          messageId: 'history-message-1',
          recipientId: target.recipientId,
          sentAt: '2026-10-15T00:00:00.000Z',
        },
      ],
    });

    await expect(alarms.findById(target.alarmId)).resolves.toMatchObject({
      status: 'COMPLETED',
      sent: 1,
      unresolved: 0,
    });
    const delivery = await pool.query<{
      state: string;
      provider_sent_at: Date;
      corrected_at: Date;
      attempt_state: string;
      request_phase: string;
    }>(
      `
        SELECT
          d.state,
          d.provider_sent_at,
          d.corrected_at,
          da.state AS attempt_state,
          da.request_phase
        FROM deliveries d
        JOIN delivery_attempts da ON da.delivery_id = d.id
        WHERE d.id = $1
      `,
      [target.deliveryId],
    );
    expect(delivery.rows[0]?.state).toBe('SENT');
    expect(delivery.rows[0]?.provider_sent_at.toISOString()).toBe('2026-10-15T00:00:00.000Z');
    expect(delivery.rows[0]?.corrected_at).toBeInstanceOf(Date);
    expect(delivery.rows[0]).toMatchObject({
      attempt_state: 'FINISHED',
      request_phase: 'FINISHED',
    });
  });

  it('runs GET-only review only when due and stops automatic review at retention', async () => {
    const target = await createUnknown();
    await pool.query(
      `
        UPDATE deliveries
        SET
          state = 'NEEDS_REVIEW',
          reconcile_count = 12,
          current_attempt_id = NULL,
          next_review_at = clock_timestamp() + interval '15 minutes',
          review_until = clock_timestamp() + interval '24 hours'
        WHERE id = $1
      `,
      [target.deliveryId],
    );
    await pool.query(
      `
        UPDATE alarms
        SET active = 0, unresolved = 1, status = 'ATTENTION_REQUIRED'
        WHERE id = $1
      `,
      [target.alarmId],
    );

    await expect(reconciliation.claimNext(15_000)).resolves.toBeNull();

    await pool.query('UPDATE deliveries SET next_review_at = clock_timestamp() WHERE id = $1', [
      target.deliveryId,
    ]);
    const due = await reconciliation.claimNext(15_000);
    if (
      due === null ||
      due.deliveryId !== target.deliveryId ||
      due.sourceState !== 'NEEDS_REVIEW'
    ) {
      throw new Error('Expected a due GET-only review claim');
    }
    await reconciliation.record(due, { kind: 'OBSERVATION', entries: [] });

    const rescheduled = await pool.query<{
      state: string;
      next_delay_seconds: number;
      retention_seconds: number;
    }>(
      `
        SELECT
          state,
          extract(epoch FROM (next_review_at - clock_timestamp())) AS next_delay_seconds,
          extract(epoch FROM (review_until - clock_timestamp())) AS retention_seconds
        FROM deliveries
        WHERE id = $1
      `,
      [target.deliveryId],
    );
    expect(rescheduled.rows[0]?.state).toBe('NEEDS_REVIEW');
    expect(Number(rescheduled.rows[0]?.next_delay_seconds)).toBeGreaterThan(14 * 60);
    expect(Number(rescheduled.rows[0]?.retention_seconds)).toBeGreaterThan(23 * 60 * 60);

    await pool.query(
      `
        UPDATE deliveries
        SET
          next_review_at = clock_timestamp(),
          review_until = clock_timestamp() - interval '1 millisecond'
        WHERE id = $1
      `,
      [target.deliveryId],
    );
    await expect(reconciliation.claimNext(15_000)).resolves.toBeNull();
    await expect(alarms.findById(target.alarmId)).resolves.toMatchObject({
      status: 'ATTENTION_REQUIRED',
      active: 0,
      unresolved: 1,
    });
  });

  it('surfaces distinct duplicate sends instead of hiding them as success', async () => {
    const target = await createUnknown();
    const claim = await reconciliation.claimNext(15_000);
    if (claim === null || claim.deliveryId !== target.deliveryId) {
      throw new Error('Expected duplicate reconciliation claim');
    }
    await reconciliation.record(claim, {
      kind: 'OBSERVATION',
      entries: [
        { messageId: 'duplicate-a', recipientId: target.recipientId },
        { messageId: 'duplicate-b', recipientId: target.recipientId },
      ],
    });

    await expect(alarms.findById(target.alarmId)).resolves.toMatchObject({
      status: 'ATTENTION_REQUIRED',
      sent: 1,
      duplicateCount: 1,
      active: 0,
    });
  });

  it('AGG-16 keeps the aggregate active-first until a duplicate-marked delivery is the last active row', async () => {
    const recipients = [`agg16-unknown-${randomUUID()}`, `agg16-active-${randomUUID()}`];
    const alarm = await alarms.create({
      type: 'EMERGENCY',
      title: `agg16-${randomUUID()}`,
      body: 'body',
      recipientIds: recipients,
    });
    await alarms.start(alarm.id);

    const ambiguousClaim = await dispatch.claimNext(0, 60_000);
    if (ambiguousClaim === null || ambiguousClaim.alarmId !== alarm.id) {
      throw new Error('Expected AGG-16 ambiguous claim');
    }
    const ambiguousAttempt = await dispatch.startAttempt(ambiguousClaim, {
      reservationId: randomUUID(),
      scheduledMonotonicMs: performance.now(),
    });
    if (ambiguousAttempt === null || !(await dispatch.commitAttempt(ambiguousAttempt))) {
      throw new Error('Expected AGG-16 committed attempt');
    }
    await dispatch.finish(ambiguousAttempt, { kind: 'AMBIGUOUS', code: 'PROVIDER_TIMEOUT' });
    await pool.query('UPDATE deliveries SET next_action_at = clock_timestamp() WHERE id = $1', [
      ambiguousClaim.deliveryId,
    ]);

    const reconciliationClaim = await reconciliation.claimNext(15_000);
    if (
      reconciliationClaim === null ||
      reconciliationClaim.deliveryId !== ambiguousClaim.deliveryId
    ) {
      throw new Error('Expected AGG-16 reconciliation claim');
    }
    await reconciliation.record(reconciliationClaim, {
      kind: 'OBSERVATION',
      entries: [
        { messageId: 'agg16-duplicate-a', recipientId: ambiguousClaim.recipientId },
        { messageId: 'agg16-duplicate-b', recipientId: ambiguousClaim.recipientId },
      ],
    });

    await expect(alarms.findById(alarm.id)).resolves.toMatchObject({
      status: 'SENDING',
      sent: 1,
      active: 1,
      duplicateCount: 1,
      incidentCount: 0,
    });
    const during = await pool.query<{ state: string; duplicate_detected: boolean }>(
      'SELECT state, duplicate_detected FROM deliveries WHERE id = $1',
      [ambiguousClaim.deliveryId],
    );
    expect(during.rows[0]).toEqual({ state: 'SENT', duplicate_detected: true });

    const remainingClaim = await dispatch.claimNext(0, 60_000);
    if (remainingClaim === null || remainingClaim.alarmId !== alarm.id) {
      throw new Error('Expected AGG-16 remaining active claim');
    }
    const remainingAttempt = await dispatch.startAttempt(remainingClaim, {
      reservationId: randomUUID(),
      scheduledMonotonicMs: performance.now(),
    });
    if (remainingAttempt === null || !(await dispatch.commitAttempt(remainingAttempt))) {
      throw new Error('Expected AGG-16 remaining committed attempt');
    }
    await dispatch.finish(remainingAttempt, {
      kind: 'ACCEPTED',
      messageId: 'agg16-final-message',
    });

    await expect(alarms.findById(alarm.id)).resolves.toMatchObject({
      status: 'ATTENTION_REQUIRED',
      sent: 2,
      active: 0,
      duplicateCount: 1,
      incidentCount: 0,
    });
  });

  it('keeps a provider conflict sticky when a later observation only matches', async () => {
    const target = await createUnknown();
    const conflictingClaim = await reconciliation.claimNext(15_000);
    if (conflictingClaim === null || conflictingClaim.deliveryId !== target.deliveryId) {
      throw new Error('Expected conflicting reconciliation claim');
    }
    await reconciliation.record(conflictingClaim, {
      kind: 'OBSERVATION',
      entries: [{ messageId: 'wrong-recipient-message', recipientId: 'another-recipient' }],
    });

    await pool.query('UPDATE deliveries SET next_review_at = clock_timestamp() WHERE id = $1', [
      target.deliveryId,
    ]);
    const matchingClaim = await reconciliation.claimNext(15_000);
    if (matchingClaim === null || matchingClaim.deliveryId !== target.deliveryId) {
      throw new Error('Expected matching review claim');
    }
    await reconciliation.record(matchingClaim, {
      kind: 'OBSERVATION',
      entries: [{ messageId: 'matching-message', recipientId: target.recipientId }],
    });

    await expect(alarms.findById(target.alarmId)).resolves.toMatchObject({
      status: 'ATTENTION_REQUIRED',
      active: 0,
      unresolved: 1,
      sent: 0,
    });
    const delivery = await pool.query<{
      state: string;
      blocking_conflict_code: string;
      provider_message_id: string | null;
    }>(
      `
        SELECT state, blocking_conflict_code, provider_message_id
        FROM deliveries
        WHERE id = $1
      `,
      [target.deliveryId],
    );
    expect(delivery.rows[0]).toEqual({
      state: 'NEEDS_REVIEW',
      blocking_conflict_code: 'PROVIDER_RECIPIENT_MISMATCH',
      provider_message_id: null,
    });
  });

  it('stores a stale lookup observation and lets the current fence adopt it', async () => {
    const target = await createUnknown();
    const stale = await reconciliation.claimNext(15_000);
    if (stale === null || stale.deliveryId !== target.deliveryId) {
      throw new Error('Expected stale reconciliation claim');
    }
    await pool.query(
      `UPDATE deliveries SET reconcile_until = clock_timestamp() - interval '1 second' WHERE id = $1`,
      [target.deliveryId],
    );
    const current = await reconciliation.claimNext(15_000);
    if (current === null || current.deliveryId !== target.deliveryId) {
      throw new Error('Expected takeover reconciliation claim');
    }

    await reconciliation.record(stale, {
      kind: 'OBSERVATION',
      entries: [{ messageId: 'late-positive', recipientId: target.recipientId }],
    });
    const unconsumed = await pool.query<{ count: number }>(
      'SELECT count(*)::integer AS count FROM delivery_evidence WHERE delivery_id = $1 AND consumed_at IS NULL',
      [target.deliveryId],
    );
    expect(unconsumed.rows[0]?.count).toBe(1);

    await reconciliation.record(current, { kind: 'UNAVAILABLE', code: 'LOOKUP_TIMEOUT' });
    await expect(alarms.findById(target.alarmId)).resolves.toMatchObject({
      status: 'COMPLETED',
      sent: 1,
      active: 0,
    });
  });

  it('adopts stale positive history after authoritative empty exposed RETRY_WAIT', async () => {
    const { strong, stale, target } = await exposeAuthoritativeRetryBeforeStalePositive();

    await recordStalePositive(strong, stale, target.recipientId, 'reverse-order-positive');

    await expect(alarms.findById(target.alarmId)).resolves.toMatchObject({
      status: 'COMPLETED',
      sent: 1,
      active: 0,
      unresolved: 0,
    });
    const state = await pool.query<{
      state: string;
      provider_message_id: string;
      post_budget_count: number;
      current_attempt_id: string | null;
      lease_token: string | null;
      reconcile_token: string | null;
      unconsumed_evidence: number;
    }>(
      `
        SELECT
          d.state,
          d.provider_message_id,
          d.post_budget_count,
          d.current_attempt_id,
          d.lease_token,
          d.reconcile_token,
          count(e.id) FILTER (WHERE e.consumed_at IS NULL)::integer AS unconsumed_evidence
        FROM deliveries d
        LEFT JOIN delivery_evidence e ON e.delivery_id = d.id
        WHERE d.id = $1
        GROUP BY d.id
      `,
      [target.deliveryId],
    );
    expect(state.rows[0]).toEqual({
      state: 'SENT',
      provider_message_id: 'reverse-order-positive',
      post_budget_count: 1,
      current_attempt_id: null,
      lease_token: null,
      reconcile_token: null,
      unconsumed_evidence: 0,
    });
    await expect(dispatch.hasEligible(0)).resolves.toBe(false);
    await expect(dispatch.claimNext(0, 60_000)).resolves.toBeNull();
  });

  it('adopts stale positive history after a retry was CLAIMED and fences startAttempt', async () => {
    const { strong, stale, target } = await exposeAuthoritativeRetryBeforeStalePositive();
    const retryClaim = await dispatch.claimNext(0, 60_000);
    if (retryClaim === null || retryClaim.deliveryId !== target.deliveryId) {
      throw new Error('Expected the authoritative-empty retry claim');
    }

    await recordStalePositive(strong, stale, target.recipientId, 'claimed-late-positive');

    await expect(
      dispatch.startAttempt(retryClaim, {
        reservationId: randomUUID(),
        scheduledMonotonicMs: performance.now(),
      }),
    ).resolves.toBeNull();
    await expect(alarms.findById(target.alarmId)).resolves.toMatchObject({
      status: 'COMPLETED',
      sent: 1,
      active: 0,
    });
    await expect(
      pool.query(
        `
          SELECT
            d.state,
            d.post_budget_count,
            d.current_attempt_id,
            d.lease_token,
            (SELECT count(*)::integer FROM delivery_attempts WHERE delivery_id = d.id) AS attempts,
            (
              SELECT count(*)::integer
              FROM delivery_evidence
              WHERE delivery_id = d.id AND consumed_at IS NULL
            ) AS unconsumed_evidence
          FROM deliveries d
          WHERE d.id = $1
        `,
        [target.deliveryId],
      ),
    ).resolves.toMatchObject({
      rows: [
        {
          state: 'SENT',
          post_budget_count: 1,
          current_attempt_id: null,
          lease_token: null,
          attempts: 1,
          unconsumed_evidence: 0,
        },
      ],
    });
    await expect(dispatch.claimNext(0, 60_000)).resolves.toBeNull();
  });

  it('adopts stale positive history over PREPARED retry and refunds its POST budget', async () => {
    const { strong, stale, target } = await exposeAuthoritativeRetryBeforeStalePositive();
    const retryClaim = await dispatch.claimNext(0, 60_000);
    if (retryClaim === null || retryClaim.deliveryId !== target.deliveryId) {
      throw new Error('Expected the authoritative-empty PREPARED retry claim');
    }
    const retryAttempt = await dispatch.startAttempt(retryClaim, {
      reservationId: randomUUID(),
      scheduledMonotonicMs: performance.now(),
    });
    if (retryAttempt === null) {
      throw new Error('Expected the authoritative-empty PREPARED retry attempt');
    }

    await recordStalePositive(strong, stale, target.recipientId, 'prepared-late-positive');

    await expect(dispatch.commitAttempt(retryAttempt)).resolves.toBe(false);
    await dispatch.finishNotSent(retryAttempt);
    await expect(alarms.findById(target.alarmId)).resolves.toMatchObject({
      status: 'COMPLETED',
      sent: 1,
      active: 0,
    });
    const delivery = await pool.query<{
      state: string;
      post_budget_count: number;
      current_attempt_id: string | null;
      lease_token: string | null;
      unconsumed_evidence: number;
    }>(
      `
        SELECT
          d.state,
          d.post_budget_count,
          d.current_attempt_id,
          d.lease_token,
          (
            SELECT count(*)::integer
            FROM delivery_evidence
            WHERE delivery_id = d.id AND consumed_at IS NULL
          ) AS unconsumed_evidence
        FROM deliveries d
        WHERE d.id = $1
      `,
      [target.deliveryId],
    );
    expect(delivery.rows[0]).toEqual({
      state: 'SENT',
      post_budget_count: 1,
      current_attempt_id: null,
      lease_token: null,
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
      [target.deliveryId],
    );
    expect(attempts.rows[1]).toEqual({
      attempt_no: 2,
      state: 'FINISHED_NOT_SENT',
      request_phase: 'FINISHED_NOT_SENT',
      budget_consumed: false,
      result_code: 'SUPERSEDED_BEFORE_PROVIDER_COMMIT',
    });
    await expect(dispatch.claimNext(0, 60_000)).resolves.toBeNull();
  });

  it('leaves stale positive history pending after COMMITTED and surfaces a distinct acceptance', async () => {
    const { strong, stale, target } = await exposeAuthoritativeRetryBeforeStalePositive();
    const retryClaim = await dispatch.claimNext(0, 60_000);
    if (retryClaim === null || retryClaim.deliveryId !== target.deliveryId) {
      throw new Error('Expected the authoritative-empty COMMITTED retry claim');
    }
    const retryAttempt = await dispatch.startAttempt(retryClaim, {
      reservationId: randomUUID(),
      scheduledMonotonicMs: performance.now(),
    });
    if (retryAttempt === null || !(await dispatch.commitAttempt(retryAttempt))) {
      throw new Error('Expected the authoritative-empty COMMITTED retry attempt');
    }

    await recordStalePositive(strong, stale, target.recipientId, 'committed-history-positive');

    await expect(
      pool.query(
        `
          SELECT
            d.state AS delivery_state,
            da.state AS attempt_state,
            da.request_phase,
            a.active,
            a.sent,
            (
              SELECT count(*)::integer
              FROM delivery_evidence
              WHERE delivery_id = d.id AND consumed_at IS NULL
            ) AS unconsumed_evidence
          FROM deliveries d
          JOIN alarms a ON a.id = d.alarm_id
          JOIN delivery_attempts da ON da.id = d.current_attempt_id
          WHERE d.id = $1
        `,
        [target.deliveryId],
      ),
    ).resolves.toMatchObject({
      rows: [
        {
          delivery_state: 'IN_FLIGHT',
          attempt_state: 'STARTED',
          request_phase: 'COMMITTED',
          active: 1,
          sent: 0,
          unconsumed_evidence: 1,
        },
      ],
    });

    await dispatch.finish(retryAttempt, {
      kind: 'ACCEPTED',
      messageId: 'committed-retry-accepted',
    });
    const maintenance = new PostgresMaintenanceRepository(pool);
    await expect(maintenance.adoptUnconsumedEvidence()).resolves.toBe(true);

    await expect(alarms.findById(target.alarmId)).resolves.toMatchObject({
      status: 'ATTENTION_REQUIRED',
      active: 0,
      sent: 1,
      duplicateCount: 1,
      incidentCount: 1,
    });
    await expect(
      pool.query(
        `
          SELECT
            state,
            provider_message_id,
            duplicate_detected,
            post_terminal_incident_code,
            (
              SELECT count(*)::integer
              FROM delivery_evidence
              WHERE delivery_id = deliveries.id AND consumed_at IS NULL
            ) AS unconsumed_evidence
          FROM deliveries
          WHERE id = $1
        `,
        [target.deliveryId],
      ),
    ).resolves.toMatchObject({
      rows: [
        {
          state: 'SENT',
          provider_message_id: 'committed-retry-accepted',
          duplicate_detected: true,
          post_terminal_incident_code: 'LATE_DISTINCT_DUPLICATE',
          unconsumed_evidence: 0,
        },
      ],
    });
  });

  it('defers a COMMITTED transient result with stale history to reconciliation before retry', async () => {
    const { strong, stale, target } = await exposeAuthoritativeRetryBeforeStalePositive();
    const retryClaim = await dispatch.claimNext(0, 60_000);
    if (retryClaim === null || retryClaim.deliveryId !== target.deliveryId) {
      throw new Error('Expected the transient COMMITTED retry claim');
    }
    const retryAttempt = await dispatch.startAttempt(retryClaim, {
      reservationId: randomUUID(),
      scheduledMonotonicMs: performance.now(),
    });
    if (retryAttempt === null || !(await dispatch.commitAttempt(retryAttempt))) {
      throw new Error('Expected the transient COMMITTED retry attempt');
    }

    await recordStalePositive(strong, stale, target.recipientId, 'committed-transient-history');
    await dispatch.finish(retryAttempt, {
      kind: 'TRANSIENT_FAILURE',
      code: 'SERVICE_UNAVAILABLE',
      httpStatus: 503,
    });

    await expect(
      pool.query(
        `
          SELECT state, result_code, post_budget_count, current_attempt_id
          FROM deliveries
          WHERE id = $1
        `,
        [target.deliveryId],
      ),
    ).resolves.toMatchObject({
      rows: [
        {
          state: 'UNKNOWN',
          result_code: 'HISTORY_EVIDENCE_PENDING_AFTER_COMMITTED_RETRY',
          post_budget_count: 2,
          current_attempt_id: retryAttempt.attemptId,
        },
      ],
    });
    await expect(dispatch.hasEligible(0)).resolves.toBe(false);
    await expect(dispatch.claimNext(0, 60_000)).resolves.toBeNull();

    const reconciliationClaim = await strong.claimNext(15_000);
    if (reconciliationClaim === null || reconciliationClaim.deliveryId !== target.deliveryId) {
      throw new Error('Expected history-first reconciliation after the transient result');
    }
    await strong.record(reconciliationClaim, { kind: 'UNAVAILABLE', code: 'HISTORY_HTTP_503' });

    await expect(alarms.findById(target.alarmId)).resolves.toMatchObject({
      status: 'COMPLETED',
      active: 0,
      sent: 1,
    });
    await expect(
      pool.query(
        `
          SELECT
            state,
            provider_message_id,
            post_budget_count,
            (
              SELECT count(*)::integer
              FROM delivery_evidence
              WHERE delivery_id = deliveries.id AND consumed_at IS NULL
            ) AS unconsumed_evidence
          FROM deliveries
          WHERE id = $1
        `,
        [target.deliveryId],
      ),
    ).resolves.toMatchObject({
      rows: [
        {
          state: 'SENT',
          provider_message_id: 'committed-transient-history',
          post_budget_count: 2,
          unconsumed_evidence: 0,
        },
      ],
    });
  });

  it('refunds a COMMITTED 429 while stale history is reconciled before retry', async () => {
    const { strong, stale, target } = await exposeAuthoritativeRetryBeforeStalePositive();
    const retryClaim = await dispatch.claimNext(0, 60_000);
    if (retryClaim === null || retryClaim.deliveryId !== target.deliveryId) {
      throw new Error('Expected the rate-limited COMMITTED retry claim');
    }
    const retryAttempt = await dispatch.startAttempt(retryClaim, {
      reservationId: randomUUID(),
      scheduledMonotonicMs: performance.now(),
    });
    if (retryAttempt === null || !(await dispatch.commitAttempt(retryAttempt))) {
      throw new Error('Expected the rate-limited COMMITTED retry attempt');
    }

    await recordStalePositive(strong, stale, target.recipientId, 'committed-429-history');
    await dispatch.finish(retryAttempt, {
      kind: 'RATE_LIMITED',
      code: 'PROVIDER_RATE_LIMITED',
      httpStatus: 429,
      retryAfterMs: 1_000,
    });

    await expect(
      pool.query(
        `
          SELECT
            d.state,
            d.post_budget_count,
            d.rate_limit_count,
            da.state AS attempt_state,
            da.request_phase,
            da.budget_consumed
          FROM deliveries d
          JOIN delivery_attempts da ON da.id = d.current_attempt_id
          WHERE d.id = $1
        `,
        [target.deliveryId],
      ),
    ).resolves.toMatchObject({
      rows: [
        {
          state: 'UNKNOWN',
          post_budget_count: 1,
          rate_limit_count: 1,
          attempt_state: 'FINISHED',
          request_phase: 'FINISHED',
          budget_consumed: false,
        },
      ],
    });
    await expect(dispatch.claimNext(0, 60_000)).resolves.toBeNull();

    const reconciliationClaim = await strong.claimNext(15_000);
    if (reconciliationClaim === null || reconciliationClaim.deliveryId !== target.deliveryId) {
      throw new Error('Expected history-first reconciliation after the 429 result');
    }
    await strong.record(reconciliationClaim, { kind: 'UNAVAILABLE', code: 'HISTORY_HTTP_503' });

    await expect(alarms.findById(target.alarmId)).resolves.toMatchObject({
      status: 'COMPLETED',
      active: 0,
      sent: 1,
    });
    await expect(
      pool.query(
        'SELECT state, post_budget_count, rate_limit_count FROM deliveries WHERE id = $1',
        [target.deliveryId],
      ),
    ).resolves.toMatchObject({
      rows: [{ state: 'SENT', post_budget_count: 1, rate_limit_count: 1 }],
    });
  });

  it('[RET-17] refunds definitely-not-sent while pending positive history wins before retry', async () => {
    const { strong, stale, target } = await exposeAuthoritativeRetryBeforeStalePositive();
    const retryClaim = await dispatch.claimNext(0, 60_000);
    if (retryClaim === null || retryClaim.deliveryId !== target.deliveryId) {
      throw new Error('Expected definitely-not-sent COMMITTED retry claim');
    }
    const retryAttempt = await dispatch.startAttempt(retryClaim, {
      reservationId: randomUUID(),
      scheduledMonotonicMs: performance.now(),
    });
    if (retryAttempt === null || !(await dispatch.commitAttempt(retryAttempt))) {
      throw new Error('Expected definitely-not-sent COMMITTED retry attempt');
    }

    await recordStalePositive(strong, stale, target.recipientId, 'committed-dnf-history');
    await dispatch.finish(retryAttempt, {
      kind: 'DEFINITELY_NOT_SENT',
      code: 'PROVIDER_REQUEST_CREATION_ERROR',
    });

    await expect(
      pool.query(
        `
          SELECT
            d.state,
            d.post_budget_count,
            d.rate_limit_count,
            d.current_attempt_id,
            da.state AS attempt_state,
            da.request_phase,
            da.transport_class,
            da.budget_consumed
          FROM deliveries d
          JOIN delivery_attempts da ON da.id = d.current_attempt_id
          WHERE d.id = $1
        `,
        [target.deliveryId],
      ),
    ).resolves.toMatchObject({
      rows: [
        {
          state: 'UNKNOWN',
          post_budget_count: 1,
          rate_limit_count: 0,
          current_attempt_id: retryAttempt.attemptId,
          attempt_state: 'FINISHED_NOT_SENT',
          request_phase: 'FINISHED_NOT_SENT',
          transport_class: 'DEFINITELY_NOT_SENT',
          budget_consumed: false,
        },
      ],
    });
    await expect(dispatch.claimNext(0, 60_000)).resolves.toBeNull();

    const reconciliationClaim = await strong.claimNext(15_000);
    if (reconciliationClaim === null || reconciliationClaim.deliveryId !== target.deliveryId) {
      throw new Error('Expected history-first reconciliation after definite-not-sent');
    }
    await strong.record(reconciliationClaim, { kind: 'UNAVAILABLE', code: 'HISTORY_HTTP_503' });

    await expect(alarms.findById(target.alarmId)).resolves.toMatchObject({
      status: 'COMPLETED',
      active: 0,
      sent: 1,
    });
    await expect(
      pool.query(
        `
          SELECT state, provider_message_id, post_budget_count, rate_limit_count
          FROM deliveries
          WHERE id = $1
        `,
        [target.deliveryId],
      ),
    ).resolves.toMatchObject({
      rows: [
        {
          state: 'SENT',
          provider_message_id: 'committed-dnf-history',
          post_budget_count: 1,
          rate_limit_count: 0,
        },
      ],
    });
  });
});
