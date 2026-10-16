import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';

import { Pool } from 'pg';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import { AlarmStateConflictError } from '../../packages/alarms/src/application/alarm-errors';
import { AlarmApplicationService } from '../../packages/alarms/src/application/alarm-service';
import type {
  DispatchAttempt,
  DispatchClaim,
  ProviderSendResult,
} from '../../packages/alarms/src/application/dispatch';
import { runMigrations } from '../../packages/alarms/src/infrastructure/persistence/migrations';
import { PostgresAlarmRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-alarm-repository';
import { PostgresDispatchRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-dispatch-repository';
import { PostgresExpansionRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-expansion-repository';
import { PostgresMaintenanceRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-maintenance-repository';
import { PostgresReconciliationRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-reconciliation-repository';
import { createTestDatabase, type TestDatabase } from './postgres-test-schema';

const databaseUrl = process.env.TEST_DATABASE_URL;
const describeWithDatabase = databaseUrl === undefined ? describe.skip : describe;

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolveDelay) => setTimeout(resolveDelay, milliseconds));
}

function releaseGate(): { readonly wait: Promise<void>; release(): void } {
  let release = (): void => undefined;
  const wait = new Promise<void>((resolveGate) => {
    release = resolveGate;
  });
  return { wait, release };
}

describeWithDatabase('cancel concurrency and phase matrix', () => {
  let testDatabase: TestDatabase;
  let pool: Pool;
  let alarms: AlarmApplicationService;
  let dispatch: PostgresDispatchRepository;
  let expansion: PostgresExpansionRepository;
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
    pool = new Pool({ connectionString: testDatabase.databaseUrl, max: 32 });
    alarms = new AlarmApplicationService(
      new PostgresAlarmRepository(pool),
      'cancel-phase-integration-cursor-key',
      randomUUID,
    );
    dispatch = new PostgresDispatchRepository(pool);
    expansion = new PostgresExpansionRepository(pool);
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

  async function createEmergency(recipientCount: number): Promise<string> {
    const alarm = await alarms.create({
      type: 'EMERGENCY',
      title: `cancel-phase-${randomUUID()}`,
      body: 'body',
      recipientIds: Array.from(
        { length: recipientCount },
        (_, index) => `cancel-user-${index}-${randomUUID()}`,
      ),
    });
    return alarm.id;
  }

  async function startEmergency(recipientCount: number): Promise<string> {
    const alarmId = await createEmergency(recipientCount);
    await alarms.start(alarmId);
    return alarmId;
  }

  async function startBulk(recipientCount: number): Promise<string> {
    const alarm = await alarms.create({
      type: 'BULK',
      title: `cancel-phase-bulk-${randomUUID()}`,
      body: 'body',
    });
    await alarms.start(alarm.id);
    const lease = await expansion.claimNext(30_000);
    if (lease === null || lease.alarmId !== alarm.id) {
      throw new Error(`Expected an expansion lease for ${alarm.id}`);
    }
    const outcome = await expansion.savePage(
      lease,
      {
        recipientIds: Array.from(
          { length: recipientCount },
          (_, index) => `cancel-bulk-user-${index}-${randomUUID()}`,
        ),
        nextCursor: null,
      },
      30_000,
    );
    expect(outcome).toBe('COMPLETED');
    return alarm.id;
  }

  async function claimFor(alarmId: string): Promise<DispatchClaim> {
    const claim = await dispatch.claimNext(0, 60_000);
    if (claim === null || claim.alarmId !== alarmId) {
      throw new Error(`Expected a dispatch claim for ${alarmId}`);
    }
    return claim;
  }

  async function startAttempt(claim: DispatchClaim): Promise<DispatchAttempt> {
    const attempt = await dispatch.startAttempt(claim, {
      reservationId: randomUUID(),
      scheduledMonotonicMs: performance.now(),
    });
    if (attempt === null) {
      throw new Error(`Expected a dispatch attempt for ${claim.deliveryId}`);
    }
    return attempt;
  }

  async function committedAttempt(alarmId: string): Promise<DispatchAttempt> {
    const attempt = await startAttempt(await claimFor(alarmId));
    if (!(await dispatch.commitAttempt(attempt))) {
      throw new Error(`Expected a committed dispatch attempt for ${alarmId}`);
    }
    return attempt;
  }

  async function observeVersion(alarmId: string): Promise<number> {
    const result = await pool.query<{ version: number }>(
      'SELECT version FROM alarms WHERE id = $1',
      [alarmId],
    );
    const version = result.rows[0]?.version;
    if (version === undefined) {
      throw new Error(`Missing Alarm version for ${alarmId}`);
    }
    return Number(version);
  }

  it('EMG-08 cancels only the target emergency while another emergency and bulk keep progressing', async () => {
    const targetEmergencyId = await startEmergency(2);
    const otherEmergencyId = await startEmergency(1);
    const bulkId = await startBulk(1);
    const otherEmergencyVersion = await observeVersion(otherEmergencyId);
    const bulkVersion = await observeVersion(bulkId);

    await alarms.cancel(targetEmergencyId);
    expect(await maintenance.sweepCanceledDeliveries(100)).toBe(2);

    await expect(alarms.findById(targetEmergencyId)).resolves.toMatchObject({
      status: 'CANCELED',
      total: 2,
      canceled: 2,
      active: 0,
    });
    await expect(alarms.findById(otherEmergencyId)).resolves.toMatchObject({
      status: 'SENDING',
      total: 1,
      sent: 0,
      canceled: 0,
      active: 1,
    });
    await expect(alarms.findById(bulkId)).resolves.toMatchObject({
      status: 'SENDING',
      total: 1,
      sent: 0,
      canceled: 0,
      active: 1,
    });
    expect(await observeVersion(otherEmergencyId)).toBe(otherEmergencyVersion);
    expect(await observeVersion(bulkId)).toBe(bulkVersion);

    const otherEmergencyAttempt = await committedAttempt(otherEmergencyId);
    await dispatch.finish(otherEmergencyAttempt, {
      kind: 'ACCEPTED',
      messageId: 'emg-08-other-emergency',
    });
    const bulkAttempt = await committedAttempt(bulkId);
    await dispatch.finish(bulkAttempt, { kind: 'ACCEPTED', messageId: 'emg-08-bulk' });

    await expect(alarms.findById(otherEmergencyId)).resolves.toMatchObject({
      status: 'COMPLETED',
      total: 1,
      sent: 1,
      canceled: 0,
      active: 0,
    });
    await expect(alarms.findById(bulkId)).resolves.toMatchObject({
      status: 'COMPLETED',
      total: 1,
      sent: 1,
      canceled: 0,
      active: 0,
    });
    await expect(alarms.findById(targetEmergencyId)).resolves.toMatchObject({
      status: 'CANCELED',
      sent: 0,
      canceled: 2,
      active: 0,
    });
  });

  it('CAN-07 makes 20 concurrent and repeated cancels one idempotent event', async () => {
    const alarmId = await startEmergency(6);
    const beforeCancelVersion = await observeVersion(alarmId);

    const concurrent = await Promise.all(Array.from({ length: 20 }, () => alarms.cancel(alarmId)));
    expect(concurrent).toHaveLength(20);
    expect(concurrent.every(({ status }) => status === 'CANCEL_REQUESTED')).toBe(true);
    expect(await observeVersion(alarmId)).toBe(beforeCancelVersion + 1);

    expect(await maintenance.sweepCanceledDeliveries(2)).toBe(2);
    expect(await maintenance.sweepCanceledDeliveries(2)).toBe(2);
    expect(await maintenance.sweepCanceledDeliveries(2)).toBe(2);
    expect(await maintenance.sweepCanceledDeliveries(2)).toBe(0);
    const terminal = await alarms.findById(alarmId);
    expect(terminal).toMatchObject({
      status: 'CANCELED',
      total: 6,
      canceled: 6,
      active: 0,
    });

    const terminalVersion = await observeVersion(alarmId);
    const repeated = await Promise.all(Array.from({ length: 20 }, () => alarms.cancel(alarmId)));
    expect(repeated.every(({ status }) => status === 'CANCELED')).toBe(true);
    expect(await observeVersion(alarmId)).toBe(terminalVersion);
    expect(await maintenance.sweepCanceledDeliveries(100)).toBe(0);
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      canceled: 6,
      active: 0,
    });
  });

  it('CAN-08 admits only the two legal CREATED start/cancel race outcomes', async () => {
    const observed = new Set<string>();

    for (let iteration = 0; iteration < 20; iteration += 1) {
      const alarmId = await createEmergency(1);
      const gate = releaseGate();
      const starting = gate.wait.then(() => alarms.start(alarmId));
      const canceling = gate.wait.then(() => alarms.cancel(alarmId));
      gate.release();
      const [startResult, cancelResult] = await Promise.allSettled([starting, canceling]);

      expect(cancelResult.status).toBe('fulfilled');
      if (cancelResult.status !== 'fulfilled') {
        throw cancelResult.reason;
      }

      if (startResult.status === 'rejected') {
        expect(startResult.reason).toBeInstanceOf(AlarmStateConflictError);
        expect(cancelResult.value.status).toBe('CANCELED');
        await expect(alarms.findById(alarmId)).resolves.toMatchObject({
          status: 'CANCELED',
          total: 0,
          canceled: 0,
          active: 0,
        });
        observed.add('CANCEL_CREATED');
      } else {
        expect(startResult.value.status).toBe('SENDING');
        expect(cancelResult.value.status).toBe('CANCEL_REQUESTED');
        await expect(alarms.findById(alarmId)).resolves.toMatchObject({
          status: 'CANCEL_REQUESTED',
          total: 1,
          active: 1,
        });
        expect(await maintenance.sweepCanceledDeliveries(10)).toBe(1);
        await expect(alarms.findById(alarmId)).resolves.toMatchObject({
          status: 'CANCELED',
          canceled: 1,
          active: 0,
        });
        observed.add('START_THEN_CANCEL');
      }
    }

    expect(
      [...observed].every((outcome) => ['CANCEL_CREATED', 'START_THEN_CANCEL'].includes(outcome)),
    ).toBe(true);
  });

  it('CAN-03/CAN-09 preserves SENT and sweeps READY plus CLAIMED-without-STARTED once', async () => {
    const alarmId = await startEmergency(3);
    const sentAttempt = await committedAttempt(alarmId);
    await dispatch.finish(sentAttempt, { kind: 'ACCEPTED', messageId: 'already-sent' });
    const staleClaim = await claimFor(alarmId);

    await alarms.cancel(alarmId);
    expect(await maintenance.sweepCanceledDeliveries(10)).toBe(2);
    expect(await maintenance.sweepCanceledDeliveries(10)).toBe(0);
    await expect(
      dispatch.startAttempt(staleClaim, {
        reservationId: randomUUID(),
        scheduledMonotonicMs: performance.now(),
      }),
    ).resolves.toBeNull();

    const states = await pool.query<{ state: string; count: number }>(
      `
        SELECT state, count(*)::integer AS count
        FROM deliveries
        WHERE alarm_id = $1
        GROUP BY state
        ORDER BY state
      `,
      [alarmId],
    );
    expect(states.rows).toEqual([
      { state: 'CANCELED', count: 2 },
      { state: 'SENT', count: 1 },
    ]);
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'CANCELED',
      total: 3,
      sent: 1,
      canceled: 2,
      active: 0,
    });
  });

  it('CAN-04/CAN-16 serializes cancel against STARTED creation without double terminal work', async () => {
    for (let iteration = 0; iteration < 12; iteration += 1) {
      const alarmId = await startEmergency(1);
      const claim = await claimFor(alarmId);
      const gate = releaseGate();
      const starting = gate.wait.then(() =>
        dispatch.startAttempt(claim, {
          reservationId: randomUUID(),
          scheduledMonotonicMs: performance.now(),
        }),
      );
      const canceling = gate.wait.then(() => alarms.cancel(alarmId));
      gate.release();
      const [attempt, canceled] = await Promise.all([starting, canceling]);
      expect(canceled.status).toBe('CANCEL_REQUESTED');

      if (attempt === null) {
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
      } else {
        await expect(dispatch.commitAttempt(attempt)).resolves.toBe(false);
        await dispatch.finishNotSent(attempt);
        await dispatch.finishNotSent(attempt);
      }

      await expect(alarms.findById(alarmId)).resolves.toMatchObject({
        status: 'CANCELED',
        total: 1,
        canceled: 1,
        active: 0,
      });
    }
  });

  it('CAN-16 rejects COMMITTED after deterministic PREPARED cancellation', async () => {
    const alarmId = await startEmergency(1);
    const attempt = await startAttempt(await claimFor(alarmId));

    await alarms.cancel(alarmId);
    await expect(dispatch.commitAttempt(attempt)).resolves.toBe(false);
    await expect(dispatch.heartbeat(attempt, 60_000)).resolves.toBe(false);
    await dispatch.finishNotSent(attempt);
    await dispatch.finishNotSent(attempt);

    const stored = await pool.query<{
      delivery_state: string;
      attempt_state: string;
      request_phase: string;
      adapter_committed_at: Date | null;
      evidence_count: number;
    }>(
      `
        SELECT
          d.state AS delivery_state,
          da.state AS attempt_state,
          da.request_phase,
          da.adapter_committed_at,
          (
            SELECT count(*)::integer
            FROM delivery_evidence e
            WHERE e.delivery_id = d.id
          ) AS evidence_count
        FROM deliveries d
        JOIN delivery_attempts da ON da.delivery_id = d.id
        WHERE d.id = $1
      `,
      [attempt.deliveryId],
    );
    expect(stored.rows[0]).toEqual({
      delivery_state: 'CANCELED',
      attempt_state: 'FINISHED_NOT_SENT',
      request_phase: 'FINISHED_NOT_SENT',
      adapter_committed_at: null,
      evidence_count: 0,
    });
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'CANCELED',
      canceled: 1,
      active: 0,
    });
  });

  it.each<{
    readonly label: string;
    readonly result: ProviderSendResult;
    readonly deliveryState: string;
    readonly sent: number;
    readonly failed: number;
    readonly canceled: number;
  }>([
    {
      label: '202 ACCEPTED',
      result: { kind: 'ACCEPTED', messageId: 'accepted-after-cancel' },
      deliveryState: 'SENT',
      sent: 1,
      failed: 0,
      canceled: 0,
    },
    {
      label: 'known 400',
      result: { kind: 'PERMANENT_FAILURE', code: 'RECIPIENT_BLOCKED', httpStatus: 400 },
      deliveryState: 'FAILED_PERMANENT',
      sent: 0,
      failed: 1,
      canceled: 0,
    },
    {
      label: '429 definitely not sent',
      result: {
        kind: 'RATE_LIMITED',
        code: 'RATE_LIMITED',
        httpStatus: 429,
        retryAfterMs: 2_000,
      },
      deliveryState: 'CANCELED',
      sent: 0,
      failed: 0,
      canceled: 1,
    },
    {
      label: '500 definitely not sent',
      result: { kind: 'TRANSIENT_FAILURE', code: 'INTERNAL_ERROR', httpStatus: 500 },
      deliveryState: 'CANCELED',
      sent: 0,
      failed: 0,
      canceled: 1,
    },
    {
      label: '503 definitely not sent',
      result: { kind: 'TRANSIENT_FAILURE', code: 'SERVICE_UNAVAILABLE', httpStatus: 503 },
      deliveryState: 'CANCELED',
      sent: 0,
      failed: 0,
      canceled: 1,
    },
  ])('CAN-14 settles committed $label under cancellation', async (testCase) => {
    const alarmId = await startEmergency(1);
    const attempt = await committedAttempt(alarmId);
    await alarms.cancel(alarmId);
    await dispatch.finish(attempt, testCase.result);

    const delivery = await pool.query<{ state: string; attempt_state: string }>(
      `
        SELECT d.state, da.state AS attempt_state
        FROM deliveries d
        JOIN delivery_attempts da ON da.id = $2
        WHERE d.id = $1
      `,
      [attempt.deliveryId, attempt.attemptId],
    );
    expect(delivery.rows[0]).toEqual({
      state: testCase.deliveryState,
      attempt_state: 'FINISHED',
    });
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'CANCELED',
      total: 1,
      sent: testCase.sent,
      failed: testCase.failed,
      canceled: testCase.canceled,
      active: 0,
    });
    await expect(dispatch.hasEligible(0)).resolves.toBe(false);
  });

  it('CAN-12/CAN-14 leaves ambiguous committed work UNKNOWN until positive lookup', async () => {
    const alarmId = await startEmergency(1);
    const attempt = await committedAttempt(alarmId);
    await alarms.cancel(alarmId);
    await dispatch.finish(attempt, { kind: 'AMBIGUOUS', code: 'PROVIDER_TIMEOUT' });

    expect(await maintenance.sweepCanceledDeliveries(100)).toBe(0);
    const unknown = await pool.query<{ state: string; request_phase: string }>(
      `
        SELECT d.state, da.request_phase
        FROM deliveries d
        JOIN delivery_attempts da ON da.id = d.current_attempt_id
        WHERE d.id = $1
      `,
      [attempt.deliveryId],
    );
    expect(unknown.rows[0]).toEqual({ state: 'UNKNOWN', request_phase: 'FINISHED' });
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'CANCEL_REQUESTED',
      active: 1,
      canceled: 0,
    });

    await delay(1_100);
    const claim = await reconciliation.claimNext(15_000);
    if (claim === null || claim.deliveryId !== attempt.deliveryId) {
      throw new Error('Expected reconciliation claim for canceled UNKNOWN delivery');
    }
    await reconciliation.record(claim, {
      kind: 'OBSERVATION',
      entries: [
        {
          messageId: 'history-confirmed-after-cancel',
          recipientId: attempt.recipientId,
          sentAt: '2026-10-15T00:00:00.000Z',
        },
      ],
    });

    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'CANCELED',
      sent: 1,
      canceled: 0,
      active: 0,
    });
    const reconciled = await pool.query<{ state: string; attempt_state: string }>(
      `
        SELECT d.state, da.state AS attempt_state
        FROM deliveries d
        JOIN delivery_attempts da ON da.delivery_id = d.id
        WHERE d.id = $1
      `,
      [attempt.deliveryId],
    );
    expect(reconciled.rows[0]).toEqual({ state: 'SENT', attempt_state: 'FINISHED' });
  });

  it('CAN-05/CAN-14 settles canceled ambiguity only when empty history is explicitly authoritative', async () => {
    const alarmId = await startEmergency(1);
    const attempt = await committedAttempt(alarmId);
    await alarms.cancel(alarmId);
    await dispatch.finish(attempt, { kind: 'AMBIGUOUS', code: 'PROVIDER_TIMEOUT' });
    await pool.query(
      `
        UPDATE deliveries
        SET next_action_at = clock_timestamp(),
            first_unknown_at = clock_timestamp() - interval '1 second'
        WHERE id = $1
      `,
      [attempt.deliveryId],
    );

    const claim = await strongReconciliation.claimNext(15_000);
    if (claim === null || claim.deliveryId !== attempt.deliveryId) {
      throw new Error('Expected authoritative-empty cancellation reconciliation claim');
    }
    await strongReconciliation.record(claim, { kind: 'OBSERVATION', entries: [] });

    const stored = await pool.query<{
      delivery_state: string;
      result_code: string;
      post_budget_count: number;
      attempt_count: number;
      unfinished_attempts: number;
      evidence_count: number;
      unconsumed_evidence: number;
    }>(
      `
        SELECT
          d.state AS delivery_state,
          d.result_code,
          d.post_budget_count,
          (SELECT count(*)::integer FROM delivery_attempts WHERE delivery_id = d.id)
            AS attempt_count,
          (
            SELECT count(*)::integer
            FROM delivery_attempts
            WHERE delivery_id = d.id AND state = 'STARTED'
          ) AS unfinished_attempts,
          (SELECT count(*)::integer FROM delivery_evidence WHERE delivery_id = d.id)
            AS evidence_count,
          (
            SELECT count(*)::integer
            FROM delivery_evidence
            WHERE delivery_id = d.id AND consumed_at IS NULL
          ) AS unconsumed_evidence
        FROM deliveries d
        WHERE d.id = $1
      `,
      [attempt.deliveryId],
    );
    expect(stored.rows[0]).toEqual({
      delivery_state: 'CANCELED',
      result_code: 'HISTORY_AUTHORITATIVE_EMPTY_CANCELED',
      post_budget_count: 1,
      attempt_count: 1,
      unfinished_attempts: 0,
      evidence_count: 1,
      unconsumed_evidence: 0,
    });
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'CANCELED',
      sent: 0,
      failed: 0,
      canceled: 1,
      active: 0,
      unresolved: 0,
    });
    await expect(dispatch.hasEligible(0)).resolves.toBe(false);
  });

  it('CAN-05/CAN-14 preserves canceled ambiguity at the opaque deadline without a new POST', async () => {
    const alarmId = await startEmergency(1);
    const attempt = await committedAttempt(alarmId);
    await alarms.cancel(alarmId);
    await dispatch.finish(attempt, { kind: 'AMBIGUOUS', code: 'PROVIDER_TIMEOUT' });
    await pool.query(
      `
        UPDATE deliveries
        SET next_action_at = clock_timestamp(),
            first_unknown_at = clock_timestamp() - interval '1 second'
        WHERE id = $1
      `,
      [attempt.deliveryId],
    );

    const claim = await opaqueDeadlineReconciliation.claimNext(15_000);
    if (claim === null || claim.deliveryId !== attempt.deliveryId) {
      throw new Error('Expected opaque-deadline cancellation reconciliation claim');
    }
    await opaqueDeadlineReconciliation.record(claim, { kind: 'OBSERVATION', entries: [] });

    const stored = await pool.query<{
      delivery_state: string;
      post_budget_count: number;
      attempts: number;
      unfinished_attempts: number;
    }>(
      `
        SELECT
          d.state AS delivery_state,
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
      [attempt.deliveryId],
    );
    expect(stored.rows[0]).toEqual({
      delivery_state: 'NEEDS_REVIEW',
      post_budget_count: 1,
      attempts: 1,
      unfinished_attempts: 0,
    });
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'ATTENTION_REQUIRED',
      sent: 0,
      failed: 0,
      canceled: 0,
      active: 0,
      unresolved: 1,
    });
    await expect(dispatch.hasEligible(0)).resolves.toBe(false);
  });

  it('CAN-15 rolls back a failed batch and resumes each eligible row exactly once', async () => {
    const alarmId = await startEmergency(8);
    const retryAttempt = await committedAttempt(alarmId);
    await dispatch.finish(retryAttempt, {
      kind: 'TRANSIENT_FAILURE',
      code: 'SERVICE_UNAVAILABLE',
      httpStatus: 503,
    });
    const staleClaim = await claimFor(alarmId);
    await alarms.cancel(alarmId);

    await pool.query(`
      CREATE FUNCTION reject_cancel_counter_update() RETURNS trigger
      LANGUAGE plpgsql AS $$
      BEGIN
        IF NEW.canceled > OLD.canceled THEN
          RAISE EXCEPTION 'injected cancel counter failure';
        END IF;
        RETURN NEW;
      END
      $$
    `);
    await pool.query(`
      CREATE TRIGGER reject_cancel_counter_update_trigger
      BEFORE UPDATE ON alarms
      FOR EACH ROW EXECUTE FUNCTION reject_cancel_counter_update()
    `);
    try {
      await expect(maintenance.sweepCanceledDeliveries(3)).rejects.toMatchObject({
        code: 'PERSISTENCE_UNAVAILABLE',
      });
    } finally {
      await pool.query('DROP TRIGGER reject_cancel_counter_update_trigger ON alarms');
      await pool.query('DROP FUNCTION reject_cancel_counter_update()');
    }

    const rolledBack = await pool.query<{ state: string; count: number }>(
      `
        SELECT state, count(*)::integer AS count
        FROM deliveries
        WHERE alarm_id = $1
        GROUP BY state
        ORDER BY state
      `,
      [alarmId],
    );
    expect(rolledBack.rows).toEqual([
      { state: 'CLAIMED', count: 1 },
      { state: 'READY', count: 6 },
      { state: 'RETRY_WAIT', count: 1 },
    ]);
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'CANCEL_REQUESTED',
      total: 8,
      canceled: 0,
      active: 8,
    });

    const firstWorkerAfterRestart = new PostgresMaintenanceRepository(pool);
    expect(await firstWorkerAfterRestart.sweepCanceledDeliveries(3)).toBe(3);
    const secondWorkerAfterRestart = new PostgresMaintenanceRepository(pool);
    expect(await secondWorkerAfterRestart.sweepCanceledDeliveries(3)).toBe(3);
    expect(await secondWorkerAfterRestart.sweepCanceledDeliveries(3)).toBe(2);
    expect(await secondWorkerAfterRestart.sweepCanceledDeliveries(3)).toBe(0);
    await expect(
      Promise.all(
        Array.from({ length: 10 }, () =>
          new PostgresMaintenanceRepository(pool).sweepCanceledDeliveries(3),
        ),
      ),
    ).resolves.toEqual(Array.from({ length: 10 }, () => 0));
    await expect(
      dispatch.startAttempt(staleClaim, {
        reservationId: randomUUID(),
        scheduledMonotonicMs: performance.now(),
      }),
    ).resolves.toBeNull();
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'CANCELED',
      total: 8,
      canceled: 8,
      active: 0,
    });
  });

  it('CAN-09 stores a stale post-cancel result as evidence without rewriting business state', async () => {
    const alarmId = await startEmergency(1);
    const attempt = await committedAttempt(alarmId);
    await alarms.cancel(alarmId);
    await dispatch.finish(attempt, {
      kind: 'RATE_LIMITED',
      code: 'RATE_LIMITED',
      httpStatus: 429,
      retryAfterMs: 2_000,
    });
    const beforeLateResult = await alarms.findById(alarmId);
    expect(beforeLateResult).toMatchObject({
      status: 'CANCELED',
      sent: 0,
      canceled: 1,
      active: 0,
      incidentCount: 0,
    });

    const lateResult = { kind: 'ACCEPTED', messageId: 'stale-worker-late-accept' } as const;
    await dispatch.finish(attempt, lateResult);
    await dispatch.finish(attempt, lateResult);

    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'CANCELED',
      sent: 0,
      canceled: 1,
      active: 0,
      incidentCount: 0,
    });
    const evidence = await pool.query<{ unconsumed: number; delivery_state: string }>(
      `
        SELECT
          count(*) FILTER (WHERE e.consumed_at IS NULL)::integer AS unconsumed,
          min(d.state) AS delivery_state
        FROM deliveries d
        LEFT JOIN delivery_evidence e ON e.delivery_id = d.id
        WHERE d.id = $1
        GROUP BY d.id
      `,
      [attempt.deliveryId],
    );
    expect(evidence.rows[0]).toEqual({ unconsumed: 1, delivery_state: 'CANCELED' });

    await expect(maintenance.adoptUnconsumedEvidence()).resolves.toBe(true);
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'ATTENTION_REQUIRED',
      sent: 0,
      canceled: 1,
      active: 0,
      incidentCount: 1,
      incidentSummary: {
        codes: [{ code: 'LATE_POSITIVE_AFTER_CANCELED', count: 1 }],
      },
    });
    const terminal = await pool.query<{ state: string; incident: string }>(
      `
        SELECT state, post_terminal_incident_code AS incident
        FROM deliveries
        WHERE id = $1
      `,
      [attempt.deliveryId],
    );
    expect(terminal.rows[0]).toEqual({
      state: 'CANCELED',
      incident: 'LATE_POSITIVE_AFTER_CANCELED',
    });
  });
});
