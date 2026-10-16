import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';

import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { AlarmApplicationService } from '../../packages/alarms/src/application/alarm-service';
import {
  type DispatchLifecycleEvent,
  DispatchProcessor,
  type DispatchRepository,
  type ProviderSendResult,
} from '../../packages/alarms/src/application/dispatch';
import { runMigrations } from '../../packages/alarms/src/infrastructure/persistence/migrations';
import { PostgresAlarmRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-alarm-repository';
import { PostgresDispatchRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-dispatch-repository';
import { PostgresMaintenanceRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-maintenance-repository';
import { PostgresReconciliationRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-reconciliation-repository';
import { NodeDispatchHeartbeatScheduler } from '../../packages/alarms/src/infrastructure/runtime/node-application-runtime';
import { createTestDatabase, type TestDatabase } from './postgres-test-schema';

const databaseUrl = process.env.TEST_DATABASE_URL;
const describeWithDatabase = databaseUrl === undefined ? describe.skip : describe;

function delay(milliseconds: number): Promise<void> {
  return new Promise((settle) => setTimeout(settle, milliseconds));
}

describeWithDatabase('worker lease and poison isolation', () => {
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
      'worker-lease-integration-cursor-key',
      randomUUID,
    );
    dispatch = new PostgresDispatchRepository(pool);
    maintenance = new PostgresMaintenanceRepository(pool);
    reconciliation = new PostgresReconciliationRepository(pool);
  });

  afterAll(async () => {
    await maintenance?.releaseCounterConsistencyLeadership();
    await pool?.end();
    await testDatabase?.cleanup();
  });

  async function emergency(count: number): Promise<string> {
    const alarm = await alarms.create({
      type: 'EMERGENCY',
      title: `worker-lease-${randomUUID()}`,
      body: 'body',
      recipientIds: Array.from(
        { length: count },
        (_, index) => `recipient-${index}-${randomUUID()}`,
      ),
    });
    await alarms.start(alarm.id);
    // Every worker scenario below expects this fixture to be immediately eligible. Avoid making
    // that precondition depend on equality with a host/VM wall-clock sample.
    await pool.query(
      `UPDATE deliveries SET next_action_at = clock_timestamp() - interval '1 second' WHERE alarm_id = $1`,
      [alarm.id],
    );
    return alarm.id;
  }

  function processor(
    sender: () => Promise<ProviderSendResult>,
    repository: DispatchRepository = dispatch,
    events: DispatchLifecycleEvent[] = [],
    leaseDurationMs = 60_000,
    heartbeatIntervalMs = 10_000,
  ): DispatchProcessor {
    return new DispatchProcessor(
      repository,
      {
        reserve: async () => ({
          reservationId: randomUUID(),
          scheduledMonotonicMs: performance.now(),
        }),
      },
      { send: sender },
      {
        leaseDurationMs,
        heartbeatIntervalMs,
        allowedLateMs: 10_000,
        monotonicNow: () => performance.now(),
        waitUntil: async () => undefined,
        heartbeatScheduler: new NodeDispatchHeartbeatScheduler(),
        eventSink: { record: (event) => events.push(event) },
      },
    );
  }

  it('LF-05 keeps a send longer than its initial lease owned with heartbeat p99 below L/2', async () => {
    const alarmId = await emergency(1);
    const heartbeatTimes: number[] = [];
    const measuredRepository: DispatchRepository = {
      hasEligible: (minimumPriority) => dispatch.hasEligible(minimumPriority),
      claimNext: (minimumPriority, leaseDurationMs) =>
        dispatch.claimNext(minimumPriority, leaseDurationMs),
      startAttempt: (claim, permit) => dispatch.startAttempt(claim, permit),
      commitAttempt: (attempt) => dispatch.commitAttempt(attempt),
      heartbeat: (attempt, leaseDurationMs) => {
        heartbeatTimes.push(performance.now());
        return dispatch.heartbeat(attempt, leaseDurationMs);
      },
      finishNotSent: (attempt) => dispatch.finishNotSent(attempt),
      finish: (attempt, result) => dispatch.finish(attempt, result),
    };
    const worker = processor(
      async () => {
        await delay(1_300);
        return { kind: 'ACCEPTED', messageId: 'lf05-slow-accepted' };
      },
      measuredRepository,
      [],
      900,
      100,
    );

    const started = await worker.beginOnce(0);
    if (started === null) {
      throw new Error('Expected LF-05 dispatch');
    }
    let finished = false;
    const reaped: Array<Awaited<ReturnType<typeof maintenance.reapExpiredSendLease>>> = [];
    const reaper = (async () => {
      while (!finished) {
        await delay(100);
        reaped.push(await maintenance.reapExpiredSendLease());
      }
    })();
    await started.completion.finally(() => {
      finished = true;
    });
    await reaper;

    expect(reaped.every((event) => event === null)).toBe(true);
    expect(heartbeatTimes.length).toBeGreaterThanOrEqual(10);
    const gaps = heartbeatTimes
      .slice(1)
      .map((timestamp, index) => timestamp - heartbeatTimes[index]);
    const ordered = [...gaps].sort((left, right) => left - right);
    const p99 = ordered[Math.ceil(ordered.length * 0.99) - 1];
    expect(p99).toBeLessThan(450);
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'COMPLETED',
      sent: 1,
      active: 0,
    });
  });

  it('LF-08 revalidates a stale candidate after its unlocked scan instead of reclaiming it', async () => {
    const alarmId = await emergency(1);
    const claim = await dispatch.claimNext(0, 60_000);
    if (claim === null || claim.alarmId !== alarmId) {
      throw new Error('Expected LF-08 claim');
    }
    await pool.query(
      "UPDATE deliveries SET lease_until = clock_timestamp() - interval '1 second' WHERE id = $1",
      [claim.deliveryId],
    );

    const blocker = await pool.connect();
    const reaperPool = new Pool({
      connectionString: testDatabase.databaseUrl,
      application_name: 'lf08-stale-candidate-reaper',
      max: 2,
    });
    const isolatedMaintenance = new PostgresMaintenanceRepository(reaperPool);
    try {
      await blocker.query('BEGIN');
      await blocker.query('SELECT id FROM alarms WHERE id = $1 FOR UPDATE', [alarmId]);
      const reaping = isolatedMaintenance.reapExpiredSendLease();

      const deadline = performance.now() + 5_000;
      while (true) {
        const waiting = await pool.query<{ waiting: boolean }>(
          `
            SELECT EXISTS (
              SELECT 1
              FROM pg_stat_activity
              WHERE application_name = 'lf08-stale-candidate-reaper'
                AND wait_event_type = 'Lock'
                AND query LIKE '%FROM alarms WHERE id%'
            ) AS waiting
          `,
        );
        if (waiting.rows[0]?.waiting === true) {
          break;
        }
        if (performance.now() >= deadline) {
          throw new Error('Timed out waiting for LF-08 Alarm lock');
        }
        await delay(10);
      }

      await pool.query(
        `
          UPDATE deliveries
          SET state = 'READY', lease_token = NULL, lease_until = NULL,
              updated_at = clock_timestamp()
          WHERE id = $1
        `,
        [claim.deliveryId],
      );
      await blocker.query('COMMIT');
      await expect(reaping).resolves.toBeNull();
    } finally {
      await blocker.query('ROLLBACK').catch(() => undefined);
      blocker.release();
      await isolatedMaintenance.releaseCounterConsistencyLeadership();
      await reaperPool.end();
    }

    const stored = await pool.query<{ state: string; lease_generation: number }>(
      'SELECT state, lease_generation FROM deliveries WHERE id = $1',
      [claim.deliveryId],
    );
    expect(stored.rows[0]).toEqual({ state: 'READY', lease_generation: 1 });
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({ active: 1, sent: 0 });

    const recovered = await processor(async () => ({
      kind: 'ACCEPTED',
      messageId: 'lf08-stale-recovered',
    })).beginOnce(0);
    await recovered?.completion;
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'COMPLETED',
      sent: 1,
      active: 0,
    });
  });

  it('LF-08 serializes expired-lease reaping against a provider result without double delta', async () => {
    const alarmId = await emergency(1);
    const claim = await dispatch.claimNext(0, 60_000);
    if (claim === null || claim.alarmId !== alarmId) {
      throw new Error('Expected LF-08 result-race claim');
    }
    const attempt = await dispatch.startAttempt(claim, {
      reservationId: randomUUID(),
      scheduledMonotonicMs: performance.now(),
    });
    if (attempt === null || !(await dispatch.commitAttempt(attempt))) {
      throw new Error('Expected LF-08 committed attempt');
    }
    await pool.query(
      "UPDATE deliveries SET lease_until = clock_timestamp() - interval '1 second' WHERE id = $1",
      [claim.deliveryId],
    );

    await Promise.all([
      maintenance.reapExpiredSendLease(),
      dispatch.finish(attempt, { kind: 'ACCEPTED', messageId: 'lf08-racing-result' }),
    ]);
    const afterRace = await pool.query<{ state: string }>(
      'SELECT state FROM deliveries WHERE id = $1',
      [claim.deliveryId],
    );
    if (afterRace.rows[0]?.state === 'UNKNOWN') {
      const reconcileClaim = await reconciliation.claimNext(15_000);
      if (reconcileClaim === null || reconcileClaim.deliveryId !== claim.deliveryId) {
        throw new Error('Expected LF-08 reconciliation takeover');
      }
      await reconciliation.record(reconcileClaim, {
        kind: 'UNAVAILABLE',
        code: 'LOOKUP_RACE_INJECTED',
      });
    }

    const proof = await pool.query<{
      state: string;
      attempts: number;
      unfinished_attempts: number;
      unconsumed_evidence: number;
    }>(
      `
        SELECT
          d.state,
          (SELECT count(*)::integer FROM delivery_attempts WHERE delivery_id = d.id) AS attempts,
          (
            SELECT count(*)::integer FROM delivery_attempts
            WHERE delivery_id = d.id AND state = 'STARTED'
          ) AS unfinished_attempts,
          (
            SELECT count(*)::integer FROM delivery_evidence
            WHERE delivery_id = d.id AND consumed_at IS NULL
          ) AS unconsumed_evidence
        FROM deliveries d
        WHERE d.id = $1
      `,
      [claim.deliveryId],
    );
    expect(proof.rows[0]).toEqual({
      state: 'SENT',
      attempts: 1,
      unfinished_attempts: 0,
      unconsumed_evidence: 0,
    });
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'COMPLETED',
      total: 1,
      sent: 1,
      active: 0,
      duplicateCount: 0,
    });
  });

  it('WRK-08 isolates permanent and ambiguous poison deliveries so the next work is not blocked', async () => {
    const permanentAlarmId = await emergency(2);
    const permanentEvents: DispatchLifecycleEvent[] = [];
    const permanentResults: ProviderSendResult[] = [
      { kind: 'PERMANENT_FAILURE', code: 'UNKNOWN_PROVIDER_4XX', httpStatus: 400 },
      { kind: 'ACCEPTED', messageId: 'wrk08-after-permanent' },
    ];
    const permanentWorker = processor(
      async () => {
        const result = permanentResults.shift();
        if (result === undefined) throw new Error('Unexpected WRK-08 permanent retry');
        return result;
      },
      dispatch,
      permanentEvents,
    );
    for (let index = 0; index < 2; index += 1) {
      const started = await permanentWorker.beginOnce(0);
      if (started === null) {
        const snapshot = await pool.query(
          `
            SELECT d.state, d.next_action_at <= clock_timestamp() AS due, a.status
            FROM deliveries d
            JOIN alarms a ON a.id = d.alarm_id
            WHERE d.alarm_id = $1
            ORDER BY d.id
          `,
          [permanentAlarmId],
        );
        throw new Error(
          `Expected WRK-08 permanent work ${index + 1}; snapshot=${JSON.stringify(snapshot.rows)}`,
        );
      }
      await started.completion;
    }
    expect(permanentResults).toHaveLength(0);
    expect(permanentEvents).toContainEqual(
      expect.objectContaining({
        eventName: 'ATTEMPT_FINISHED',
        outcome: 'PERMANENT_FAILURE',
        providerCode: 'UNKNOWN_PROVIDER_4XX',
        httpStatus: 400,
      }),
    );
    const permanent = await pool.query<{ state: string; result_code: string }>(
      'SELECT state, result_code FROM deliveries WHERE alarm_id = $1 ORDER BY state',
      [permanentAlarmId],
    );
    expect(permanent.rows).toEqual([
      { state: 'FAILED_PERMANENT', result_code: 'UNKNOWN_PROVIDER_4XX' },
      { state: 'SENT', result_code: 'ACCEPTED' },
    ]);

    const ambiguousAlarmId = await emergency(2);
    const ambiguousResults: ProviderSendResult[] = [
      { kind: 'AMBIGUOUS', code: 'MALFORMED_PROVIDER_RESPONSE' },
      { kind: 'ACCEPTED', messageId: 'wrk08-after-ambiguous' },
    ];
    const ambiguousWorker = processor(async () => {
      const result = ambiguousResults.shift();
      if (result === undefined) throw new Error('Unexpected WRK-08 ambiguous retry');
      return result;
    });
    for (let index = 0; index < 2; index += 1) {
      const started = await ambiguousWorker.beginOnce(0);
      if (started === null) {
        const snapshot = await pool.query(
          `
            SELECT d.state, d.next_action_at <= clock_timestamp() AS due, a.status
            FROM deliveries d
            JOIN alarms a ON a.id = d.alarm_id
            WHERE d.alarm_id = $1
            ORDER BY d.id
          `,
          [ambiguousAlarmId],
        );
        throw new Error(
          `Expected WRK-08 ambiguous work ${index + 1}; snapshot=${JSON.stringify(snapshot.rows)}`,
        );
      }
      await started.completion;
    }
    expect(ambiguousResults).toHaveLength(0);

    const unknown = await pool.query<{ id: string }>(
      "SELECT id FROM deliveries WHERE alarm_id = $1 AND state = 'UNKNOWN'",
      [ambiguousAlarmId],
    );
    const unknownId = unknown.rows[0]?.id;
    if (unknownId === undefined) throw new Error('Expected WRK-08 UNKNOWN poison delivery');
    await pool.query(
      `
        UPDATE deliveries
        SET reconcile_count = 11, next_action_at = clock_timestamp() - interval '1 second'
        WHERE id = $1
      `,
      [unknownId],
    );
    const reconcileClaim = await reconciliation.claimNext(15_000);
    if (reconcileClaim === null || reconcileClaim.deliveryId !== unknownId) {
      throw new Error('Expected WRK-08 poison reconciliation');
    }
    await reconciliation.record(reconcileClaim, {
      kind: 'UNAVAILABLE',
      code: 'MALFORMED_HISTORY_RESPONSE',
    });

    const ambiguous = await pool.query<{ state: string; result_code: string }>(
      'SELECT state, result_code FROM deliveries WHERE alarm_id = $1 ORDER BY state',
      [ambiguousAlarmId],
    );
    expect(ambiguous.rows).toEqual([
      { state: 'NEEDS_REVIEW', result_code: 'MALFORMED_HISTORY_RESPONSE' },
      { state: 'SENT', result_code: 'ACCEPTED' },
    ]);
    await expect(alarms.findById(ambiguousAlarmId)).resolves.toMatchObject({
      status: 'ATTENTION_REQUIRED',
      sent: 1,
      active: 0,
      unresolved: 1,
    });
  });
});
