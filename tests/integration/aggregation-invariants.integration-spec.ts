import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';

import { Pool } from 'pg';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import { AlarmApplicationService } from '../../packages/alarms/src/application/alarm-service';
import type {
  DispatchAttempt,
  ProviderSendResult,
} from '../../packages/alarms/src/application/dispatch';
import { MaintenanceProcessor } from '../../packages/alarms/src/application/maintenance';
import { runMigrations } from '../../packages/alarms/src/infrastructure/persistence/migrations';
import { PostgresAlarmRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-alarm-repository';
import { PostgresDispatchRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-dispatch-repository';
import { PostgresMaintenanceRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-maintenance-repository';
import { PostgresReconciliationRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-reconciliation-repository';
import { createTestDatabase, type TestDatabase } from './postgres-test-schema';

const databaseUrl = process.env.TEST_DATABASE_URL;
const describeWithDatabase = databaseUrl === undefined ? describe.skip : describe;

interface StoredAlarmProjection {
  readonly status: string;
  readonly total: number;
  readonly sent: number;
  readonly failed: number;
  readonly canceled: number;
  readonly active: number;
  readonly unresolved: number;
  readonly duplicate_count: number;
  readonly incident_count: number;
  readonly incident_counts: Record<string, number>;
  readonly incident_first_detected_at: Date | null;
  readonly incident_last_detected_at: Date | null;
  readonly finished_at: Date | null;
}

interface DeliveryProjection {
  readonly state: string;
  readonly duplicate_detected: boolean;
  readonly post_terminal_incident_code: string | null;
}

describeWithDatabase('Alarm aggregate invariants', () => {
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
    pool = new Pool({ connectionString: testDatabase.databaseUrl, max: 24 });
    alarms = new AlarmApplicationService(
      new PostgresAlarmRepository(pool),
      'aggregate-integration-cursor-key',
      randomUUID,
    );
    dispatch = new PostgresDispatchRepository(pool);
    maintenance = new PostgresMaintenanceRepository(pool);
    reconciliation = new PostgresReconciliationRepository(pool);
  });

  beforeEach(async () => {
    await pool.query('TRUNCATE alarms CASCADE');
  });

  afterAll(async () => {
    await maintenance?.releaseCounterConsistencyLeadership();
    await pool?.end();
    await testDatabase?.cleanup();
  });

  async function startedEmergency(recipientCount: number, label: string): Promise<string> {
    const alarm = await alarms.create({
      type: 'EMERGENCY',
      title: `${label}-${randomUUID()}`,
      body: 'body',
      recipientIds: Array.from(
        { length: recipientCount },
        (_, index) => `${label}-${index}-${randomUUID()}`,
      ),
    });
    await alarms.start(alarm.id);
    return alarm.id;
  }

  async function committedAttempts(alarmId: string, count: number): Promise<DispatchAttempt[]> {
    const attempts: DispatchAttempt[] = [];
    for (let index = 0; index < count; index += 1) {
      const claim = await dispatch.claimNext(0, 60_000);
      if (claim === null || claim.alarmId !== alarmId) {
        throw new Error(`Expected aggregate dispatch claim ${index + 1}`);
      }
      const attempt = await dispatch.startAttempt(claim, {
        reservationId: randomUUID(),
        scheduledMonotonicMs: performance.now(),
      });
      if (attempt === null || !(await dispatch.commitAttempt(attempt))) {
        throw new Error(`Expected aggregate committed attempt ${index + 1}`);
      }
      attempts.push(attempt);
    }
    return attempts;
  }

  async function insertHistoryEvidence(
    deliveryId: string,
    entries: readonly {
      readonly messageId: string;
      readonly recipientId: string;
      readonly sentAt?: string;
    }[],
  ): Promise<void> {
    await pool.query(
      `
        INSERT INTO delivery_evidence(
          id, delivery_id, source, observation_key, payload_hash, entries, entry_count
        )
        VALUES ($1, $2, 'HISTORY', $3, $4, $5::jsonb, $6)
      `,
      [
        randomUUID(),
        deliveryId,
        randomUUID(),
        randomUUID(),
        JSON.stringify(entries),
        entries.length,
      ],
    );
  }

  async function expectStoredProjectionMatchesDeliveries(
    alarmId: string,
  ): Promise<StoredAlarmProjection> {
    const alarmResult = await pool.query<StoredAlarmProjection>(
      `
        SELECT
          status, total, sent, failed, canceled, active, unresolved,
          duplicate_count, incident_count, incident_counts,
          incident_first_detected_at, incident_last_detected_at, finished_at
        FROM alarms
        WHERE id = $1
      `,
      [alarmId],
    );
    const alarm = alarmResult.rows[0];
    if (alarm === undefined) {
      throw new Error(`Expected stored Alarm projection ${alarmId}`);
    }
    const deliveryResult = await pool.query<DeliveryProjection>(
      `
        SELECT state, duplicate_detected, post_terminal_incident_code
        FROM deliveries
        WHERE alarm_id = $1
      `,
      [alarmId],
    );
    const observed = {
      total: deliveryResult.rows.length,
      sent: deliveryResult.rows.filter(({ state }) => state === 'SENT').length,
      failed: deliveryResult.rows.filter(({ state }) =>
        ['FAILED_PERMANENT', 'FAILED_RETRY_EXHAUSTED'].includes(state),
      ).length,
      canceled: deliveryResult.rows.filter(({ state }) => state === 'CANCELED').length,
      unresolved: deliveryResult.rows.filter(({ state }) => state === 'NEEDS_REVIEW').length,
      duplicateCount: deliveryResult.rows.filter(({ duplicate_detected }) => duplicate_detected)
        .length,
      incidentCount: deliveryResult.rows.filter(
        ({ post_terminal_incident_code }) => post_terminal_incident_code !== null,
      ).length,
    };
    const observedTerminal =
      observed.sent + observed.failed + observed.canceled + observed.unresolved;
    const active = observed.total - observedTerminal;

    expect({
      total: alarm.total,
      sent: alarm.sent,
      failed: alarm.failed,
      canceled: alarm.canceled,
      active: alarm.active,
      unresolved: alarm.unresolved,
      duplicateCount: alarm.duplicate_count,
      incidentCount: alarm.incident_count,
    }).toEqual({ ...observed, active });
    expect(alarm.sent + alarm.failed + alarm.canceled + alarm.active + alarm.unresolved).toBe(
      alarm.total,
    );
    expect(Object.values(alarm.incident_counts).reduce((sum, count) => sum + count, 0)).toBe(
      alarm.incident_count,
    );
    if (alarm.incident_count === 0) {
      expect(alarm.incident_first_detected_at).toBeNull();
      expect(alarm.incident_last_detected_at).toBeNull();
    } else {
      expect(alarm.incident_first_detected_at).toBeInstanceOf(Date);
      expect(alarm.incident_last_detected_at).toBeInstanceOf(Date);
      expect(alarm.incident_first_detected_at?.getTime()).toBeLessThanOrEqual(
        alarm.incident_last_detected_at?.getTime() ?? 0,
      );
    }
    return alarm;
  }

  it('AGG-16 keeps CANCEL_REQUESTED active-first while adopting a canonical duplicate union', async () => {
    const alarmId = await startedEmergency(2, 'agg16-cancel');
    const [ambiguousAttempt, remainingAttempt] = await committedAttempts(alarmId, 2);
    if (ambiguousAttempt === undefined || remainingAttempt === undefined) {
      throw new Error('Expected AGG-16 attempts');
    }
    await dispatch.finish(ambiguousAttempt, { kind: 'AMBIGUOUS', code: 'PROVIDER_TIMEOUT' });
    await pool.query('UPDATE deliveries SET next_action_at = clock_timestamp() WHERE id = $1', [
      ambiguousAttempt.deliveryId,
    ]);
    await alarms.cancel(alarmId);

    const claim = await reconciliation.claimNext(15_000);
    if (claim === null || claim.deliveryId !== ambiguousAttempt.deliveryId) {
      throw new Error('Expected AGG-16 reconciliation claim');
    }
    await reconciliation.record(claim, {
      kind: 'OBSERVATION',
      entries: [
        { messageId: 'agg16-cancel-a', recipientId: ambiguousAttempt.recipientId },
        { messageId: 'agg16-cancel-b', recipientId: ambiguousAttempt.recipientId },
      ],
    });

    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'CANCEL_REQUESTED',
      sent: 1,
      active: 1,
      duplicateCount: 1,
      incidentCount: 0,
      finishedAt: null,
    });
    await expectStoredProjectionMatchesDeliveries(alarmId);

    await dispatch.finish(remainingAttempt, {
      kind: 'ACCEPTED',
      messageId: 'agg16-cancel-final',
    });
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'ATTENTION_REQUIRED',
      sent: 2,
      active: 0,
      duplicateCount: 1,
      incidentCount: 0,
    });
    await expectStoredProjectionMatchesDeliveries(alarmId);
  });

  it('AGG-18 preserves PARTIALLY_FAILED business truth for a late evidence conflict under concurrent scanners', async () => {
    const alarmId = await startedEmergency(2, 'agg18-conflict');
    const [sentAttempt, failedAttempt] = await committedAttempts(alarmId, 2);
    if (sentAttempt === undefined || failedAttempt === undefined) {
      throw new Error('Expected AGG-18 attempts');
    }
    await dispatch.finish(sentAttempt, { kind: 'ACCEPTED', messageId: 'agg18-original' });
    await dispatch.finish(failedAttempt, {
      kind: 'PERMANENT_FAILURE',
      code: 'RECIPIENT_BLOCKED',
      httpStatus: 400,
    });
    const before = await alarms.findById(alarmId);
    expect(before).toMatchObject({
      status: 'PARTIALLY_FAILED',
      sent: 1,
      failed: 1,
      active: 0,
      incidentCount: 0,
    });
    const attemptsBefore = await pool.query<{ count: number }>(
      `
        SELECT count(*)::integer AS count
        FROM delivery_attempts da
        JOIN deliveries d ON d.id = da.delivery_id
        WHERE d.alarm_id = $1
      `,
      [alarmId],
    );

    await insertHistoryEvidence(sentAttempt.deliveryId, [
      {
        messageId: 'agg18-conflicting-time',
        recipientId: sentAttempt.recipientId,
        sentAt: '2026-10-15T00:00:00.000Z',
      },
      {
        messageId: 'agg18-conflicting-time',
        recipientId: sentAttempt.recipientId,
        sentAt: '2026-10-15T00:00:01.000Z',
      },
    ]);
    const scannerResults = await Promise.all(
      Array.from({ length: 12 }, () =>
        new PostgresMaintenanceRepository(pool).adoptUnconsumedEvidence(),
      ),
    );
    expect(scannerResults.filter(Boolean)).toHaveLength(1);

    const after = await alarms.findById(alarmId);
    expect(after).toMatchObject({
      status: 'ATTENTION_REQUIRED',
      total: 2,
      sent: 1,
      failed: 1,
      canceled: 0,
      active: 0,
      duplicateCount: 0,
      incidentCount: 1,
      incidentSummary: {
        count: 1,
        codes: [{ code: 'LATE_PROVIDER_EVIDENCE_CONFLICT', count: 1 }],
      },
      finishedAt: before?.finishedAt,
    });
    const firstProjection = await expectStoredProjectionMatchesDeliveries(alarmId);

    await insertHistoryEvidence(sentAttempt.deliveryId, [
      {
        messageId: 'agg18-repeat-conflict',
        recipientId: sentAttempt.recipientId,
        sentAt: '2026-10-15T00:01:00.000Z',
      },
      {
        messageId: 'agg18-repeat-conflict',
        recipientId: sentAttempt.recipientId,
        sentAt: '2026-10-15T00:01:01.000Z',
      },
    ]);
    const repeatedScannerResults = await Promise.all(
      Array.from({ length: 8 }, () =>
        new PostgresMaintenanceRepository(pool).adoptUnconsumedEvidence(),
      ),
    );
    expect(repeatedScannerResults.filter(Boolean)).toHaveLength(1);

    const repeated = await alarms.findById(alarmId);
    expect(repeated).toMatchObject({
      status: 'ATTENTION_REQUIRED',
      sent: 1,
      failed: 1,
      active: 0,
      duplicateCount: 0,
      incidentCount: 1,
      incidentSummary: after?.incidentSummary,
      finishedAt: before?.finishedAt,
    });
    const attemptsAfter = await pool.query<{ count: number; unfinished: number }>(
      `
        SELECT
          count(*)::integer AS count,
          (count(*) FILTER (WHERE da.state = 'STARTED'))::integer AS unfinished
        FROM delivery_attempts da
        JOIN deliveries d ON d.id = da.delivery_id
        WHERE d.alarm_id = $1
      `,
      [alarmId],
    );
    expect(attemptsAfter.rows[0]).toEqual({
      count: attemptsBefore.rows[0]?.count,
      unfinished: 0,
    });
    await expect(dispatch.claimNext(0, 60_000)).resolves.toBeNull();
    expect(firstProjection.incident_first_detected_at?.toISOString()).toBe(
      repeated?.incidentSummary.firstDetectedAt,
    );
    expect(firstProjection.incident_last_detected_at?.toISOString()).toBe(
      repeated?.incidentSummary.lastDetectedAt,
    );
    await expectStoredProjectionMatchesDeliveries(alarmId);
  });

  it('AGG-19 applies the no-anomaly last-result and cancellation truth table sequentially and concurrently', async () => {
    const cases: readonly {
      readonly label: string;
      readonly concurrent: boolean;
      readonly cancel: boolean;
      readonly results: readonly ProviderSendResult[];
      readonly expected: {
        readonly status: string;
        readonly sent: number;
        readonly failed: number;
      };
    }[] = [
      {
        label: 'all-success-sequential',
        concurrent: false,
        cancel: false,
        results: [
          { kind: 'ACCEPTED', messageId: 'agg19-success-a' },
          { kind: 'ACCEPTED', messageId: 'agg19-success-b' },
        ],
        expected: { status: 'COMPLETED', sent: 2, failed: 0 },
      },
      {
        label: 'mixed-concurrent',
        concurrent: true,
        cancel: false,
        results: [
          { kind: 'ACCEPTED', messageId: 'agg19-mixed-a' },
          { kind: 'PERMANENT_FAILURE', code: 'RECIPIENT_BLOCKED', httpStatus: 400 },
        ],
        expected: { status: 'PARTIALLY_FAILED', sent: 1, failed: 1 },
      },
      {
        label: 'all-failed-sequential',
        concurrent: false,
        cancel: false,
        results: [
          { kind: 'PERMANENT_FAILURE', code: 'RECIPIENT_BLOCKED', httpStatus: 400 },
          { kind: 'PERMANENT_FAILURE', code: 'RECIPIENT_BLOCKED', httpStatus: 400 },
        ],
        expected: { status: 'FAILED', sent: 0, failed: 2 },
      },
      {
        label: 'cancel-mixed-concurrent',
        concurrent: true,
        cancel: true,
        results: [
          { kind: 'ACCEPTED', messageId: 'agg19-cancel-a' },
          { kind: 'PERMANENT_FAILURE', code: 'RECIPIENT_BLOCKED', httpStatus: 400 },
        ],
        expected: { status: 'CANCELED', sent: 1, failed: 1 },
      },
    ];

    for (const scenario of cases) {
      const alarmId = await startedEmergency(2, scenario.label);
      const attempts = await committedAttempts(alarmId, 2);
      if (scenario.cancel) {
        await alarms.cancel(alarmId);
        await expect(alarms.findById(alarmId)).resolves.toMatchObject({
          status: 'CANCEL_REQUESTED',
          active: 2,
          finishedAt: null,
        });
      }
      const finishes = attempts.map((attempt, index) =>
        dispatch.finish(attempt, scenario.results[index] as ProviderSendResult),
      );
      if (scenario.concurrent) {
        await Promise.all(finishes);
      } else {
        for (const finish of finishes) {
          await finish;
        }
      }

      const final = await alarms.findById(alarmId);
      expect(final).toMatchObject({
        ...scenario.expected,
        total: 2,
        canceled: 0,
        active: 0,
        unresolved: 0,
        duplicateCount: 0,
        incidentCount: 0,
      });
      expect(final?.finishedAt).not.toBeNull();
      await expectStoredProjectionMatchesDeliveries(alarmId);

      await Promise.all(
        attempts.map((attempt, index) =>
          dispatch.finish(attempt, scenario.results[index] as ProviderSendResult),
        ),
      );
      await expect(alarms.findById(alarmId)).resolves.toMatchObject({
        ...scenario.expected,
        total: 2,
        active: 0,
        finishedAt: final?.finishedAt,
      });
      await expectStoredProjectionMatchesDeliveries(alarmId);
    }
  });

  it('AGG-19 keeps unresolved and incident anomalies visible until concurrent last results settle to attention', async () => {
    const unresolvedAlarmId = await startedEmergency(2, 'agg19-unresolved');
    const [unknownAttempt, unresolvedLastAttempt] = await committedAttempts(unresolvedAlarmId, 2);
    if (unknownAttempt === undefined || unresolvedLastAttempt === undefined) {
      throw new Error('Expected AGG-19 unresolved attempts');
    }
    await dispatch.finish(unknownAttempt, { kind: 'AMBIGUOUS', code: 'PROVIDER_TIMEOUT' });
    await pool.query(
      `
        UPDATE deliveries
        SET reconcile_count = 11, next_action_at = clock_timestamp()
        WHERE id = $1
      `,
      [unknownAttempt.deliveryId],
    );
    const unresolvedClaim = await reconciliation.claimNext(15_000);
    if (unresolvedClaim === null || unresolvedClaim.deliveryId !== unknownAttempt.deliveryId) {
      throw new Error('Expected AGG-19 unresolved reconciliation claim');
    }
    await reconciliation.record(unresolvedClaim, { kind: 'OBSERVATION', entries: [] });
    await expect(alarms.findById(unresolvedAlarmId)).resolves.toMatchObject({
      status: 'SENDING',
      active: 1,
      unresolved: 1,
      finishedAt: null,
    });
    await dispatch.finish(unresolvedLastAttempt, {
      kind: 'PERMANENT_FAILURE',
      code: 'RECIPIENT_BLOCKED',
      httpStatus: 400,
    });
    await expect(alarms.findById(unresolvedAlarmId)).resolves.toMatchObject({
      status: 'ATTENTION_REQUIRED',
      failed: 1,
      active: 0,
      unresolved: 1,
    });
    await expectStoredProjectionMatchesDeliveries(unresolvedAlarmId);

    const incidentAlarmId = await startedEmergency(3, 'agg19-incident');
    const [incidentAttempt, acceptedLastAttempt, failedLastAttempt] = await committedAttempts(
      incidentAlarmId,
      3,
    );
    if (
      incidentAttempt === undefined ||
      acceptedLastAttempt === undefined ||
      failedLastAttempt === undefined
    ) {
      throw new Error('Expected AGG-19 incident attempts');
    }
    await dispatch.finish(incidentAttempt, {
      kind: 'PERMANENT_FAILURE',
      code: 'RECIPIENT_BLOCKED',
      httpStatus: 400,
    });
    await dispatch.finish(incidentAttempt, {
      kind: 'ACCEPTED',
      messageId: 'agg19-late-positive',
    });
    await expect(maintenance.adoptUnconsumedEvidence()).resolves.toBe(true);
    await expect(alarms.findById(incidentAlarmId)).resolves.toMatchObject({
      status: 'SENDING',
      failed: 1,
      active: 2,
      incidentCount: 1,
      finishedAt: null,
    });
    await alarms.cancel(incidentAlarmId);
    await expect(alarms.findById(incidentAlarmId)).resolves.toMatchObject({
      status: 'CANCEL_REQUESTED',
      active: 2,
      incidentCount: 1,
      finishedAt: null,
    });

    await Promise.all([
      dispatch.finish(acceptedLastAttempt, {
        kind: 'ACCEPTED',
        messageId: 'agg19-concurrent-success',
      }),
      dispatch.finish(failedLastAttempt, {
        kind: 'PERMANENT_FAILURE',
        code: 'RECIPIENT_BLOCKED',
        httpStatus: 400,
      }),
    ]);
    await expect(alarms.findById(incidentAlarmId)).resolves.toMatchObject({
      status: 'ATTENTION_REQUIRED',
      total: 3,
      sent: 1,
      failed: 2,
      canceled: 0,
      active: 0,
      unresolved: 0,
      duplicateCount: 0,
      incidentCount: 1,
      incidentSummary: {
        count: 1,
        codes: [{ code: 'LATE_POSITIVE_AFTER_FAILED', count: 1 }],
      },
    });
    await expectStoredProjectionMatchesDeliveries(incidentAlarmId);
  });

  it('AGG-11 emits a read-only drift alert and leaves stored counters/query truth untouched', async () => {
    const alarmId = await startedEmergency(2, 'agg11-drift');
    await pool.query(
      `
        UPDATE alarms
        SET sent = 1, active = 1, updated_at = clock_timestamp()
        WHERE id = $1
      `,
      [alarmId],
    );

    await expect(maintenance.findCounterInconsistencies(10)).resolves.toEqual([
      {
        alarmId,
        stored: {
          total: 2,
          sent: 1,
          failed: 0,
          canceled: 0,
          active: 1,
          unresolved: 0,
          duplicateCount: 0,
          incidentCount: 0,
        },
        observed: {
          total: 2,
          sent: 0,
          failed: 0,
          canceled: 0,
          active: 2,
          unresolved: 0,
          duplicateCount: 0,
          incidentCount: 0,
        },
      },
    ]);
    const competingWorker = new PostgresMaintenanceRepository(pool);
    await expect(competingWorker.findCounterInconsistencies(10)).resolves.toEqual([]);
    const reports: unknown[] = [];
    const processor = new MaintenanceProcessor(maintenance, {
      consistencyCheckEveryIdleRuns: 1,
      monotonicNow: () => performance.now(),
      eventSink: {
        recordCounterInconsistency: (issue) => reports.push(issue),
        recordOperationalSnapshot: () => undefined,
        recordSendLeaseRecovery: () => undefined,
      },
    });
    await expect(processor.runOnce()).resolves.toBe(false);
    await expect(processor.runOnce()).resolves.toBe(false);
    expect(reports).toHaveLength(1);

    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'SENDING',
      total: 2,
      sent: 1,
      active: 1,
    });
    const after = await pool.query<{
      sent: number;
      active: number;
      delivery_sent: number;
      delivery_active: number;
    }>(
      `
        SELECT
          a.sent,
          a.active,
          (count(d.id) FILTER (WHERE d.state = 'SENT'))::integer AS delivery_sent,
          (
            count(d.id) FILTER (
              WHERE d.state NOT IN (
                'SENT', 'FAILED_PERMANENT', 'FAILED_RETRY_EXHAUSTED',
                'CANCELED', 'NEEDS_REVIEW'
              )
            )
          )::integer AS delivery_active
        FROM alarms a
        LEFT JOIN deliveries d ON d.alarm_id = a.id
        WHERE a.id = $1
        GROUP BY a.id
      `,
      [alarmId],
    );
    expect(after.rows[0]).toEqual({
      sent: 1,
      active: 1,
      delivery_sent: 0,
      delivery_active: 2,
    });

    await maintenance.releaseCounterConsistencyLeadership();
    await expect(competingWorker.findCounterInconsistencies(10)).resolves.toHaveLength(1);
    await competingWorker.releaseCounterConsistencyLeadership();
  });

  it('AGG-11 advances a cyclic cursor so inconsistencies beyond one batch are not starved', async () => {
    const alarmIds: string[] = [];
    for (let index = 0; index < 3; index += 1) {
      const alarmId = await startedEmergency(1, `agg11-cursor-${index}`);
      alarmIds.push(alarmId);
      await pool.query(
        `
          UPDATE alarms
          SET
            sent = 1,
            active = 0,
            updated_at = clock_timestamp() + ($2::integer * interval '1 millisecond')
          WHERE id = $1
        `,
        [alarmId, index],
      );
    }

    const firstBatch = await maintenance.findCounterInconsistencies(2);
    const secondBatch = await maintenance.findCounterInconsistencies(2);

    expect(firstBatch).toHaveLength(2);
    expect(secondBatch).toHaveLength(2);
    expect(new Set([...firstBatch, ...secondBatch].map(({ alarmId }) => alarmId))).toEqual(
      new Set(alarmIds),
    );
    await maintenance.releaseCounterConsistencyLeadership();
  });
});
