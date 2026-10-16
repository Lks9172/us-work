import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';

import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { AlarmApplicationService } from '../../packages/alarms/src/application/alarm-service';
import { runMigrations } from '../../packages/alarms/src/infrastructure/persistence/migrations';
import { PostgresAlarmRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-alarm-repository';
import { PostgresDispatchRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-dispatch-repository';
import { PostgresExpansionRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-expansion-repository';
import {
  OPERATIONAL_SNAPSHOT_SQL,
  PostgresMaintenanceRepository,
} from '../../packages/alarms/src/infrastructure/persistence/postgres-maintenance-repository';
import { createTestDatabase, type TestDatabase } from './postgres-test-schema';

const databaseUrl = process.env.TEST_DATABASE_URL;
const describeWithDatabase = databaseUrl === undefined ? describe.skip : describe;

describeWithDatabase('PostgresMaintenanceRepository', () => {
  let testDatabase: TestDatabase;
  let pool: Pool;
  let alarms: AlarmApplicationService;
  let dispatch: PostgresDispatchRepository;
  let expansion: PostgresExpansionRepository;
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
    pool = new Pool({ connectionString: testDatabase.databaseUrl, max: 12 });
    alarms = new AlarmApplicationService(
      new PostgresAlarmRepository(pool),
      'maintenance-integration-cursor-key',
      randomUUID,
    );
    dispatch = new PostgresDispatchRepository(pool);
    expansion = new PostgresExpansionRepository(pool);
    maintenance = new PostgresMaintenanceRepository(pool);
  });

  afterAll(async () => {
    await maintenance?.releaseCounterConsistencyLeadership();
    await pool?.end();
    await testDatabase?.cleanup();
  });

  async function emergency(size: number): Promise<string> {
    const alarm = await alarms.create({
      type: 'EMERGENCY',
      title: `maintenance-${randomUUID()}`,
      body: 'body',
      recipientIds: Array.from({ length: size }, () => `user-${randomUUID()}`),
    });
    await alarms.start(alarm.id);
    return alarm.id;
  }

  async function committedAttempt(alarmId: string) {
    const claim = await dispatch.claimNext(0, 60_000);
    if (claim === null || claim.alarmId !== alarmId) {
      throw new Error('Expected a committed-attempt claim');
    }
    const attempt = await dispatch.startAttempt(claim, {
      reservationId: randomUUID(),
      scheduledMonotonicMs: performance.now(),
    });
    if (attempt === null || !(await dispatch.commitAttempt(attempt))) {
      throw new Error('Expected a committed durable attempt');
    }
    return attempt;
  }

  async function finalizedBulk(
    recipientCount: number,
    label: string,
  ): Promise<{ readonly alarmId: string; readonly runGeneration: string }> {
    const alarm = await alarms.create({
      type: 'BULK',
      title: `${label}-${randomUUID()}`,
      body: 'body',
    });
    await alarms.start(alarm.id);
    const lease = await expansion.claimNext(30_000);
    if (lease === null || lease.alarmId !== alarm.id) {
      throw new Error(`Expected ${label} expansion lease`);
    }
    await expansion.savePage(
      lease,
      {
        recipientIds: Array.from({ length: recipientCount }, (_, index) => `${label}-${index + 1}`),
        nextCursor: `${label}-cursor`,
      },
      30_000,
    );
    await expansion.savePage(lease, { recipientIds: [], nextCursor: null }, 30_000);
    return { alarmId: alarm.id, runGeneration: lease.runGeneration };
  }

  it('cancels a 100k-shaped audience in bounded resumable batches', async () => {
    const alarmId = await emergency(5);
    await alarms.cancel(alarmId);

    await expect(maintenance.sweepCanceledDeliveries(2)).resolves.toBe(2);
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'CANCEL_REQUESTED',
      canceled: 2,
      active: 3,
    });
    await expect(maintenance.sweepCanceledDeliveries(2)).resolves.toBe(2);
    await expect(maintenance.sweepCanceledDeliveries(2)).resolves.toBe(1);
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'CANCELED',
      canceled: 5,
      active: 0,
    });
  });

  it('OBS-02 reads one leader-owned DB snapshot with queue, outcome, and provider-window gauges', async () => {
    const alarmId = await emergency(2);
    const competingWorker = new PostgresMaintenanceRepository(pool);
    try {
      const attempt = await committedAttempt(alarmId);
      await dispatch.finish(attempt, { kind: 'ACCEPTED', messageId: 'snapshot-accepted' });

      const snapshot = await maintenance.readOperationalSnapshot(60);
      expect(snapshot).toMatchObject({
        windowSeconds: 60,
        queueDepth: expect.any(Number),
        dueQueueDepth: expect.any(Number),
        sent: expect.any(Number),
        failed: expect.any(Number),
        retryWait: expect.any(Number),
        unknown: expect.any(Number),
        unresolved: expect.any(Number),
        activeSendLeases: expect.any(Number),
        providerAttemptsWindow: expect.any(Number),
        providerAcceptedWindow: expect.any(Number),
        providerRateLimitedWindow: expect.any(Number),
        providerThroughputPerSecond: expect.any(Number),
      });
      if (snapshot === null) {
        throw new Error('Expected this repository to own snapshot leadership');
      }
      expect(snapshot.sent).toBeGreaterThanOrEqual(1);
      expect(snapshot.queueDepth).toBeGreaterThanOrEqual(1);
      expect(snapshot.dueQueueDepth).toBeGreaterThanOrEqual(1);
      expect(snapshot.providerAttemptsWindow).toBeGreaterThanOrEqual(1);
      expect(snapshot.providerAcceptedWindow).toBeGreaterThanOrEqual(1);
      expect(snapshot.providerThroughputPerSecond).toBe(
        snapshot.providerAcceptedWindow / snapshot.windowSeconds,
      );
      await expect(competingWorker.readOperationalSnapshot(60)).resolves.toBeNull();
    } finally {
      await maintenance.releaseCounterConsistencyLeadership();
      await competingWorker.releaseCounterConsistencyLeadership();
      await alarms.cancel(alarmId);
      while ((await maintenance.sweepCanceledDeliveries(500)) > 0) {
        // Drain the fixture through the production cancellation path.
      }
    }
  });

  it('OBS-02 reports every mixed-state gauge exactly from projections and selective rows', async () => {
    if (databaseUrl === undefined) {
      throw new Error('TEST_DATABASE_URL is required');
    }
    const isolatedDatabase = await createTestDatabase(databaseUrl);
    await runMigrations({
      databaseUrl: isolatedDatabase.databaseUrl,
      migrationsDirectory: resolve(process.cwd(), 'migrations'),
    });
    const isolatedPool = new Pool({ connectionString: isolatedDatabase.databaseUrl, max: 4 });
    const repository = new PostgresMaintenanceRepository(isolatedPool);
    const alarmId = randomUUID();
    const deliveryIds = Array.from({ length: 10 }, () => randomUUID());
    const clientRefs = Array.from({ length: 10 }, () => randomUUID());
    const leaseToken = randomUUID();
    const reconcileToken = randomUUID();
    try {
      await isolatedPool.query(
        `
          INSERT INTO alarms(
            id, type, title, body, status, audience_finalized_at, expansion_completed_at,
            materialized_count, total, sent, failed, active, unresolved
          ) VALUES (
            $1, 'BULK', 'snapshot-fixture', 'body', 'SENDING',
            clock_timestamp(), clock_timestamp(), 10, 10, 1, 1, 7, 1
          )
        `,
        [alarmId],
      );
      await isolatedPool.query(
        `
          INSERT INTO deliveries(
            id, alarm_id, recipient_id, client_ref, state, priority,
            next_action_at, alarm_created_at, lease_generation, lease_token, lease_until,
            reconcile_generation, reconcile_token, reconcile_until, attempt_no
          )
          SELECT
            fixture.id,
            $1::uuid,
            'snapshot-recipient-' || fixture.ordinality,
            fixture.client_ref,
            fixture.state,
            CASE WHEN fixture.ordinality <= 2 THEN 100 ELSE 0 END,
            CASE fixture.ordinality
              WHEN 1 THEN clock_timestamp() - interval '5 minutes'
              WHEN 2 THEN clock_timestamp() + interval '5 minutes'
              WHEN 3 THEN clock_timestamp() - interval '2 minutes'
              WHEN 4 THEN clock_timestamp() + interval '2 minutes'
              ELSE clock_timestamp()
            END,
            clock_timestamp(),
            CASE WHEN fixture.state = 'CLAIMED' THEN 1 ELSE 0 END,
            CASE WHEN fixture.state = 'CLAIMED' THEN $4::uuid ELSE NULL END,
            CASE WHEN fixture.state = 'CLAIMED'
              THEN clock_timestamp() + interval '1 minute' ELSE NULL END,
            CASE WHEN fixture.state = 'RECONCILING' THEN 1 ELSE 0 END,
            CASE WHEN fixture.state = 'RECONCILING' THEN $5::uuid ELSE NULL END,
            CASE WHEN fixture.state = 'RECONCILING'
              THEN clock_timestamp() + interval '1 minute' ELSE NULL END,
            CASE WHEN fixture.ordinality IN (9, 10) THEN 1 ELSE 0 END
          FROM unnest(
            $2::uuid[],
            $3::uuid[],
            ARRAY[
              'READY', 'READY', 'RETRY_WAIT', 'RETRY_WAIT', 'UNKNOWN',
              'RECONCILING', 'NEEDS_REVIEW', 'CLAIMED', 'SENT', 'FAILED_PERMANENT'
            ]::text[]
          ) WITH ORDINALITY AS fixture(id, client_ref, state, ordinality)
        `,
        [alarmId, deliveryIds, clientRefs, leaseToken, reconcileToken],
      );
      await isolatedPool.query(
        `
          INSERT INTO delivery_attempts(
            id, delivery_id, attempt_no, state, lease_generation,
            limiter_reservation_id, request_phase, http_status, result_code,
            started_at, finished_at
          ) VALUES
            ($1, $2, 1, 'FINISHED', 0, $3, 'FINISHED', 202, 'ACCEPTED',
              clock_timestamp(), clock_timestamp()),
            ($4, $5, 1, 'FINISHED', 0, $6, 'FINISHED', 429, 'RATE_LIMITED',
              clock_timestamp(), clock_timestamp())
        `,
        [randomUUID(), deliveryIds[8], randomUUID(), randomUUID(), deliveryIds[9], randomUUID()],
      );

      const snapshot = await repository.readOperationalSnapshot(60);
      expect(snapshot).toEqual({
        observedAt: expect.any(String),
        windowSeconds: 60,
        queueDepth: 4,
        dueQueueDepth: 2,
        oldestDueAgeMs: expect.any(Number),
        sent: 1,
        failed: 1,
        retryWait: 2,
        unknown: 2,
        unresolved: 1,
        activeSendLeases: 1,
        providerAttemptsWindow: 2,
        providerAcceptedWindow: 1,
        providerRateLimitedWindow: 1,
        providerThroughputPerSecond: 1 / 60,
      });
      expect(snapshot?.oldestDueAgeMs).toBeGreaterThanOrEqual(300_000);
      expect(snapshot?.oldestDueAgeMs).toBeLessThan(310_000);
    } finally {
      await repository.releaseCounterConsistencyLeadership();
      await isolatedPool.end();
      await isolatedDatabase.cleanup();
    }
  });

  it('OBS-02 keeps the 100k terminal population off Delivery scans and uses the recent-attempt index', async () => {
    if (databaseUrl === undefined) {
      throw new Error('TEST_DATABASE_URL is required');
    }
    const isolatedDatabase = await createTestDatabase(databaseUrl);
    await runMigrations({
      databaseUrl: isolatedDatabase.databaseUrl,
      migrationsDirectory: resolve(process.cwd(), 'migrations'),
    });
    const isolatedPool = new Pool({ connectionString: isolatedDatabase.databaseUrl, max: 2 });
    const alarmId = randomUUID();
    try {
      await isolatedPool.query(
        `
          INSERT INTO alarms(
            id, type, title, body, status, audience_finalized_at, expansion_completed_at,
            finished_at, materialized_count, total, sent, active
          ) VALUES (
            $1, 'BULK', 'snapshot-plan', 'body', 'COMPLETED', clock_timestamp(),
            clock_timestamp(), clock_timestamp(), 100000, 100000, 100000, 0
          )
        `,
        [alarmId],
      );
      await isolatedPool.query(
        `
          INSERT INTO deliveries(
            id, alarm_id, recipient_id, client_ref, state, priority,
            alarm_created_at, attempt_no
          )
          SELECT
            ('00000000-0000-4000-8000-' || lpad(to_hex(g), 12, '0'))::uuid,
            $1::uuid,
            'snapshot-plan-' || g,
            ('10000000-0000-4000-8000-' || lpad(to_hex(g), 12, '0'))::uuid,
            'SENT', 0, clock_timestamp(), 1
          FROM generate_series(1, 100000) AS g
        `,
        [alarmId],
      );
      await isolatedPool.query(
        `
          INSERT INTO delivery_attempts(
            id, delivery_id, attempt_no, state, lease_generation,
            limiter_reservation_id, request_phase, http_status, result_code,
            started_at, finished_at
          )
          SELECT
            ('20000000-0000-4000-8000-' || lpad(to_hex(g), 12, '0'))::uuid,
            ('00000000-0000-4000-8000-' || lpad(to_hex(g), 12, '0'))::uuid,
            1,
            'FINISHED',
            0,
            ('30000000-0000-4000-8000-' || lpad(to_hex(g), 12, '0'))::uuid,
            'FINISHED', 202, 'ACCEPTED',
            CASE WHEN g <= 100 THEN clock_timestamp() - interval '1 second'
              ELSE clock_timestamp() - interval '1 day' END,
            CASE WHEN g <= 100 THEN clock_timestamp() - interval '1 second'
              ELSE clock_timestamp() - interval '1 day' END
          FROM generate_series(1, 10000) AS g
        `,
      );
      await isolatedPool.query('VACUUM (ANALYZE) deliveries');
      await isolatedPool.query('VACUUM (ANALYZE) delivery_attempts');
      await isolatedPool.query('ANALYZE alarms');

      const explained = await isolatedPool.query<{ readonly 'QUERY PLAN': unknown }>(
        `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ${OPERATIONAL_SNAPSHOT_SQL}`,
        [60],
      );
      const document = explained.rows[0]?.['QUERY PLAN'];
      if (!Array.isArray(document) || document[0] === null || typeof document[0] !== 'object') {
        throw new Error('Expected PostgreSQL JSON execution plan');
      }
      const root = document[0] as Record<string, unknown>;
      const nodes: Array<Record<string, unknown>> = [];
      const visit = (candidate: unknown): void => {
        if (candidate === null || typeof candidate !== 'object' || Array.isArray(candidate)) return;
        const node = candidate as Record<string, unknown>;
        nodes.push(node);
        const children = node.Plans;
        if (Array.isArray(children)) children.forEach(visit);
      };
      visit(root.Plan);

      expect(
        nodes.some(
          (node) =>
            node['Index Name'] === 'delivery_attempts_operational_finished_idx' &&
            String(node['Node Type']).includes('Index'),
        ),
      ).toBe(true);
      expect(
        nodes.some(
          (node) => node['Relation Name'] === 'deliveries' && node['Node Type'] === 'Seq Scan',
        ),
      ).toBe(false);
      expect(Number(root['Execution Time'])).toBeLessThan(2_000);
      const plan = root.Plan as Record<string, unknown>;
      expect(Number(plan['Shared Hit Blocks']) + Number(plan['Shared Read Blocks'])).toBeLessThan(
        25_000,
      );
    } finally {
      await isolatedPool.end();
      await isolatedDatabase.cleanup();
    }
  });

  it('returns an expired pre-STARTED claim to READY without changing counters', async () => {
    const alarmId = await emergency(1);
    const claim = await dispatch.claimNext(0, 60_000);
    if (claim === null || claim.alarmId !== alarmId) {
      throw new Error('Expected the maintenance claim');
    }
    await pool.query(
      `UPDATE deliveries SET lease_until = clock_timestamp() - interval '1 second' WHERE id = $1`,
      [claim.deliveryId],
    );

    await expect(maintenance.reapExpiredSendLease()).resolves.toMatchObject({
      reason: 'CLAIM_EXPIRED_REQUEUED',
      resultingState: 'READY',
    });
    const delivery = await pool.query<{ state: string }>(
      'SELECT state FROM deliveries WHERE id = $1',
      [claim.deliveryId],
    );
    expect(delivery.rows[0]?.state).toBe('READY');
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({ active: 1, sent: 0 });

    const reclaimed = await dispatch.claimNext(0, 60_000);
    if (reclaimed === null || reclaimed.deliveryId !== claim.deliveryId) {
      throw new Error('Expected the released delivery to be claimable');
    }
    const attempt = await dispatch.startAttempt(reclaimed, {
      reservationId: randomUUID(),
      scheduledMonotonicMs: performance.now(),
    });
    if (attempt === null) {
      throw new Error('Expected a replacement attempt');
    }
    if (!(await dispatch.commitAttempt(attempt))) {
      throw new Error('Expected the replacement provider commit fence');
    }
    await dispatch.finish(attempt, { kind: 'ACCEPTED', messageId: 'reaped-claim-success' });
  });

  it('keeps cancellation attention-required when the last active claim expires', async () => {
    const alarmId = await emergency(2);
    const claim = await dispatch.claimNext(0, 60_000);
    if (claim === null || claim.alarmId !== alarmId) {
      throw new Error('Expected the expiring cancellation claim');
    }
    await pool.query(
      `
        UPDATE deliveries
        SET state = 'NEEDS_REVIEW', next_review_at = clock_timestamp() + interval '1 hour'
        WHERE alarm_id = $1 AND id <> $2
      `,
      [alarmId, claim.deliveryId],
    );
    await pool.query(
      `
        UPDATE alarms
        SET active = 1, unresolved = 1
        WHERE id = $1
      `,
      [alarmId],
    );
    await alarms.cancel(alarmId);
    await pool.query(
      `UPDATE deliveries SET lease_until = clock_timestamp() - interval '1 second' WHERE id = $1`,
      [claim.deliveryId],
    );

    await expect(maintenance.reapExpiredSendLease()).resolves.toMatchObject({
      reason: 'CLAIM_EXPIRED_CANCELED',
      resultingState: 'CANCELED',
    });
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'ATTENTION_REQUIRED',
      active: 0,
      canceled: 1,
      unresolved: 1,
    });
  });

  it('requeues an expired PREPARED attempt because provider commit never happened', async () => {
    const alarmId = await emergency(1);
    const claim = await dispatch.claimNext(0, 60_000);
    if (claim === null || claim.alarmId !== alarmId) {
      throw new Error('Expected the in-flight maintenance claim');
    }
    const attempt = await dispatch.startAttempt(claim, {
      reservationId: randomUUID(),
      scheduledMonotonicMs: performance.now(),
    });
    if (attempt === null) {
      throw new Error('Expected a durable STARTED attempt');
    }
    await pool.query(
      `UPDATE deliveries SET lease_until = clock_timestamp() - interval '1 second' WHERE id = $1`,
      [claim.deliveryId],
    );

    await expect(maintenance.reapExpiredSendLease()).resolves.toMatchObject({
      reason: 'PREPARED_EXPIRED_REQUEUED',
      resultingState: 'READY',
    });
    const state = await pool.query<{
      delivery_state: string;
      attempt_state: string;
      request_phase: string;
      current_attempt_id: string | null;
      post_budget_count: number;
    }>(
      `
        SELECT
          d.state AS delivery_state,
          da.state AS attempt_state,
          da.request_phase,
          d.current_attempt_id,
          d.post_budget_count
        FROM deliveries d
        JOIN delivery_attempts da ON da.id = $2
        WHERE d.id = $1
      `,
      [claim.deliveryId, attempt.attemptId],
    );
    expect(state.rows[0]).toEqual({
      delivery_state: 'READY',
      attempt_state: 'FINISHED_NOT_SENT',
      request_phase: 'FINISHED_NOT_SENT',
      current_attempt_id: null,
      post_budget_count: 0,
    });
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'SENDING',
      active: 1,
    });

    const recovered = await dispatch.claimNext(0, 60_000);
    if (recovered === null || recovered.deliveryId !== claim.deliveryId) {
      throw new Error('Expected the PREPARED delivery to be reclaimable');
    }
    const recoveredAttempt = await dispatch.startAttempt(recovered, {
      reservationId: randomUUID(),
      scheduledMonotonicMs: performance.now(),
    });
    if (recoveredAttempt === null || !(await dispatch.commitAttempt(recoveredAttempt))) {
      throw new Error('Expected the recovered provider commit fence');
    }
    await dispatch.finish(recoveredAttempt, {
      kind: 'ACCEPTED',
      messageId: 'recovered-pre-provider-commit',
    });
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'COMPLETED',
      sent: 1,
      active: 0,
    });
  });

  it('moves an expired COMMITTED attempt to UNKNOWN instead of blind retry', async () => {
    const alarmId = await emergency(1);
    const claim = await dispatch.claimNext(0, 60_000);
    if (claim === null || claim.alarmId !== alarmId) {
      throw new Error('Expected the committed maintenance claim');
    }
    const attempt = await dispatch.startAttempt(claim, {
      reservationId: randomUUID(),
      scheduledMonotonicMs: performance.now(),
    });
    if (attempt === null || !(await dispatch.commitAttempt(attempt))) {
      throw new Error('Expected a committed durable attempt');
    }
    await pool.query(
      `UPDATE deliveries SET lease_until = clock_timestamp() - interval '1 second' WHERE id = $1`,
      [claim.deliveryId],
    );

    await expect(maintenance.reapExpiredSendLease()).resolves.toMatchObject({
      reason: 'COMMITTED_EXPIRED_TO_UNKNOWN',
      resultingState: 'UNKNOWN',
    });
    const state = await pool.query<{
      delivery_state: string;
      attempt_state: string;
      request_phase: string;
      current_attempt_id: string;
    }>(
      `
        SELECT
          d.state AS delivery_state,
          da.state AS attempt_state,
          da.request_phase,
          d.current_attempt_id
        FROM deliveries d
        JOIN delivery_attempts da ON da.id = d.current_attempt_id
        WHERE d.id = $1
      `,
      [claim.deliveryId],
    );
    expect(state.rows[0]).toEqual({
      delivery_state: 'UNKNOWN',
      attempt_state: 'STARTED',
      request_phase: 'COMMITTED',
      current_attempt_id: attempt.attemptId,
    });
  });

  it('preserves terminal business results while surfacing late contradictory evidence', async () => {
    const alarmId = await emergency(1);
    const claim = await dispatch.claimNext(0, 60_000);
    if (claim === null || claim.alarmId !== alarmId) {
      throw new Error('Expected terminal evidence claim');
    }
    const attempt = await dispatch.startAttempt(claim, {
      reservationId: randomUUID(),
      scheduledMonotonicMs: performance.now(),
    });
    if (attempt === null) {
      throw new Error('Expected terminal evidence attempt');
    }
    if (!(await dispatch.commitAttempt(attempt))) {
      throw new Error('Expected terminal evidence provider commit fence');
    }
    await dispatch.finish(attempt, {
      kind: 'PERMANENT_FAILURE',
      code: 'RECIPIENT_BLOCKED',
      httpStatus: 400,
    });
    await dispatch.finish(attempt, { kind: 'ACCEPTED', messageId: 'late-provider-message' });

    await expect(maintenance.adoptUnconsumedEvidence()).resolves.toBe(true);
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'ATTENTION_REQUIRED',
      failed: 1,
      sent: 0,
      incidentCount: 1,
      incidentSummary: {
        count: 1,
        codes: [{ code: 'LATE_POSITIVE_AFTER_FAILED', count: 1 }],
      },
    });
    const delivery = await pool.query<{
      state: string;
      post_terminal_incident_code: string;
    }>('SELECT state, post_terminal_incident_code FROM deliveries WHERE id = $1', [
      claim.deliveryId,
    ]);
    expect(delivery.rows[0]).toEqual({
      state: 'FAILED_PERMANENT',
      post_terminal_incident_code: 'LATE_POSITIVE_AFTER_FAILED',
    });
  });

  it('AGG-18 adopts terminal duplicate and conflict incidents once under concurrent scanners', async () => {
    const sentAlarmId = await emergency(1);
    const sentAttempt = await committedAttempt(sentAlarmId);
    await dispatch.finish(sentAttempt, {
      kind: 'ACCEPTED',
      messageId: 'agg18-original-message',
    });
    const sentBeforeIncident = await alarms.findById(sentAlarmId);
    if (sentBeforeIncident === null) {
      throw new Error('Expected AGG-18 sent Alarm');
    }

    await Promise.all(
      Array.from({ length: 8 }, () =>
        dispatch.finish(sentAttempt, {
          kind: 'ACCEPTED',
          messageId: 'agg18-distinct-late-message',
        }),
      ),
    );
    const duplicateAdoptions = await Promise.all(
      Array.from({ length: 8 }, () =>
        new PostgresMaintenanceRepository(pool).adoptUnconsumedEvidence(),
      ),
    );
    expect(duplicateAdoptions.filter(Boolean)).toHaveLength(1);

    const sentAfterIncident = await alarms.findById(sentAlarmId);
    expect(sentAfterIncident).toMatchObject({
      status: 'ATTENTION_REQUIRED',
      total: 1,
      sent: 1,
      failed: 0,
      canceled: 0,
      active: 0,
      duplicateCount: 1,
      incidentCount: 1,
      incidentSummary: {
        count: 1,
        codes: [{ code: 'LATE_DISTINCT_DUPLICATE', count: 1 }],
      },
      finishedAt: sentBeforeIncident.finishedAt,
    });
    const sentDelivery = await pool.query<{
      state: string;
      provider_message_id: string;
      duplicate_detected: boolean;
      post_terminal_incident_code: string;
      evidence_count: number;
      unconsumed_count: number;
    }>(
      `
        SELECT
          d.state,
          d.provider_message_id,
          d.duplicate_detected,
          d.post_terminal_incident_code,
          count(e.id)::integer AS evidence_count,
          count(e.id) FILTER (WHERE e.consumed_at IS NULL)::integer AS unconsumed_count
        FROM deliveries d
        LEFT JOIN delivery_evidence e ON e.delivery_id = d.id
        WHERE d.id = $1
        GROUP BY d.id
      `,
      [sentAttempt.deliveryId],
    );
    expect(sentDelivery.rows[0]).toEqual({
      state: 'SENT',
      provider_message_id: 'agg18-original-message',
      duplicate_detected: true,
      post_terminal_incident_code: 'LATE_DISTINCT_DUPLICATE',
      evidence_count: 2,
      unconsumed_count: 0,
    });

    await dispatch.finish(sentAttempt, {
      kind: 'ACCEPTED',
      messageId: 'agg18-third-distinct-message',
    });
    await expect(maintenance.adoptUnconsumedEvidence()).resolves.toBe(true);
    const sentAfterRepeat = await alarms.findById(sentAlarmId);
    expect(sentAfterRepeat).toMatchObject({
      status: 'ATTENTION_REQUIRED',
      sent: 1,
      active: 0,
      duplicateCount: 1,
      incidentCount: 1,
      incidentSummary: {
        count: 1,
        codes: [{ code: 'LATE_DISTINCT_DUPLICATE', count: 1 }],
        firstDetectedAt: sentAfterIncident?.incidentSummary.firstDetectedAt,
        lastDetectedAt: sentAfterIncident?.incidentSummary.lastDetectedAt,
      },
      finishedAt: sentBeforeIncident.finishedAt,
    });

    const canceledAlarmId = await emergency(1);
    await alarms.cancel(canceledAlarmId);
    await expect(maintenance.sweepCanceledDeliveries(10)).resolves.toBe(1);
    const canceledBeforeIncident = await alarms.findById(canceledAlarmId);
    if (canceledBeforeIncident === null) {
      throw new Error('Expected AGG-18 canceled Alarm');
    }
    const canceledDelivery = await pool.query<{ id: string; recipient_id: string }>(
      'SELECT id, recipient_id FROM deliveries WHERE alarm_id = $1',
      [canceledAlarmId],
    );
    const canceledRow = canceledDelivery.rows[0];
    if (canceledRow === undefined) {
      throw new Error('Expected AGG-18 canceled Delivery');
    }
    await pool.query(
      `
        INSERT INTO delivery_evidence(
          id, delivery_id, source, observation_key, payload_hash, entries, entry_count
        )
        VALUES ($1, $2, 'HISTORY', $3, $4, $5::jsonb, 1)
      `,
      [
        randomUUID(),
        canceledRow.id,
        randomUUID(),
        randomUUID(),
        JSON.stringify([
          {
            messageId: 'agg18-wrong-recipient-message',
            recipientId: `${canceledRow.recipient_id}-wrong`,
          },
        ]),
      ],
    );

    const conflictAdoptions = await Promise.all(
      Array.from({ length: 8 }, () =>
        new PostgresMaintenanceRepository(pool).adoptUnconsumedEvidence(),
      ),
    );
    expect(conflictAdoptions.filter(Boolean)).toHaveLength(1);
    await expect(
      Promise.all(
        Array.from({ length: 4 }, () =>
          new PostgresMaintenanceRepository(pool).adoptUnconsumedEvidence(),
        ),
      ),
    ).resolves.toEqual([false, false, false, false]);

    await expect(alarms.findById(canceledAlarmId)).resolves.toMatchObject({
      status: 'ATTENTION_REQUIRED',
      total: 1,
      sent: 0,
      failed: 0,
      canceled: 1,
      active: 0,
      duplicateCount: 0,
      incidentCount: 1,
      incidentSummary: {
        count: 1,
        codes: [{ code: 'LATE_PROVIDER_RECIPIENT_MISMATCH', count: 1 }],
      },
      finishedAt: canceledBeforeIncident.finishedAt,
    });
    const canceledTerminal = await pool.query<{
      state: string;
      duplicate_detected: boolean;
      post_terminal_incident_code: string;
      unconsumed_count: number;
    }>(
      `
        SELECT
          d.state,
          d.duplicate_detected,
          d.post_terminal_incident_code,
          count(e.id) FILTER (WHERE e.consumed_at IS NULL)::integer AS unconsumed_count
        FROM deliveries d
        LEFT JOIN delivery_evidence e ON e.delivery_id = d.id
        WHERE d.id = $1
        GROUP BY d.id
      `,
      [canceledRow.id],
    );
    expect(canceledTerminal.rows[0]).toEqual({
      state: 'CANCELED',
      duplicate_detected: false,
      post_terminal_incident_code: 'LATE_PROVIDER_RECIPIENT_MISMATCH',
      unconsumed_count: 0,
    });
  });

  it('cleans only the finalized stable expansion run in bounded batches', async () => {
    const alarm = await alarms.create({
      type: 'BULK',
      title: `cleanup-${randomUUID()}`,
      body: 'body',
    });
    await alarms.start(alarm.id);
    const lease = await expansion.claimNext(30_000);
    if (lease === null || lease.alarmId !== alarm.id) {
      throw new Error('Expected cleanup expansion lease');
    }
    await expansion.savePage(
      lease,
      { recipientIds: ['cleanup-a', 'cleanup-b'], nextCursor: 'cleanup-cursor' },
      30_000,
    );
    await expansion.savePage(lease, { recipientIds: ['cleanup-c'], nextCursor: null }, 30_000);

    await expect(maintenance.cleanupExpansionBatch(1, 30_000)).resolves.toBe(true);
    await expect(maintenance.cleanupExpansionBatch(1, 30_000)).resolves.toBe(true);
    await expect(maintenance.cleanupExpansionBatch(1, 30_000)).resolves.toBe(true);

    const job = await pool.query<{ cleanup_state: string }>(
      'SELECT cleanup_state FROM alarm_expansion_jobs WHERE alarm_id = $1',
      [alarm.id],
    );
    const staging = await pool.query<{ count: number }>(
      `
        SELECT (
          (SELECT count(*) FROM alarm_expansion_recipients WHERE alarm_id = $1) +
          (SELECT count(*) FROM alarm_expansion_seen_cursors WHERE alarm_id = $1)
        )::integer AS count
      `,
      [alarm.id],
    );
    expect(job.rows[0]?.cleanup_state).toBe('COMPLETED');
    expect(staging.rows[0]?.count).toBe(0);
  });

  it('durably takes over cleanup after a failed batch and fences the stale owner', async () => {
    const alarm = await alarms.create({
      type: 'BULK',
      title: `cleanup-takeover-${randomUUID()}`,
      body: 'body',
    });
    await alarms.start(alarm.id);
    const expansionLease = await expansion.claimNext(30_000);
    if (expansionLease === null || expansionLease.alarmId !== alarm.id) {
      throw new Error('Expected cleanup takeover expansion lease');
    }
    await expansion.savePage(
      expansionLease,
      { recipientIds: ['takeover-a', 'takeover-b'], nextCursor: 'takeover-cursor' },
      30_000,
    );
    await expansion.savePage(
      expansionLease,
      { recipientIds: ['takeover-c'], nextCursor: null },
      30_000,
    );

    await pool.query(`
      CREATE FUNCTION reject_cleanup_delete() RETURNS trigger
      LANGUAGE plpgsql AS $$
      BEGIN
        RAISE EXCEPTION 'injected cleanup delete failure';
      END
      $$
    `);
    await pool.query(`
      CREATE TRIGGER reject_cleanup_delete_trigger
      BEFORE DELETE ON alarm_expansion_recipients
      FOR EACH ROW EXECUTE FUNCTION reject_cleanup_delete()
    `);

    await expect(maintenance.cleanupExpansionBatch(1, 30_000)).rejects.toThrow();
    const failedOwner = await pool.query<{
      cleanup_state: string;
      cleanup_lease_generation: number;
      cleanup_lease_token: string;
      staging_count: number;
    }>(
      `
        SELECT
          j.cleanup_state,
          j.cleanup_lease_generation,
          j.cleanup_lease_token,
          (
            SELECT count(*)::integer
            FROM alarm_expansion_recipients r
            WHERE r.alarm_id = j.alarm_id
          ) AS staging_count
        FROM alarm_expansion_jobs j
        WHERE j.alarm_id = $1
      `,
      [alarm.id],
    );
    expect(failedOwner.rows[0]).toMatchObject({
      cleanup_state: 'RUNNING',
      cleanup_lease_generation: 1,
      staging_count: 3,
    });

    await pool.query('DROP TRIGGER reject_cleanup_delete_trigger ON alarm_expansion_recipients');
    await pool.query('DROP FUNCTION reject_cleanup_delete()');
    await pool.query(
      `
        UPDATE alarm_expansion_jobs
        SET cleanup_lease_until = clock_timestamp() - interval '1 second'
        WHERE alarm_id = $1
      `,
      [alarm.id],
    );
    await expect(maintenance.cleanupExpansionBatch(1, 30_000)).resolves.toBe(true);

    const staleCompletion = await pool.query(
      `
        UPDATE alarm_expansion_jobs
        SET cleanup_state = 'COMPLETED'
        WHERE alarm_id = $1
          AND cleanup_lease_generation = $2
          AND cleanup_lease_token = $3
      `,
      [
        alarm.id,
        failedOwner.rows[0]?.cleanup_lease_generation,
        failedOwner.rows[0]?.cleanup_lease_token,
      ],
    );
    expect(staleCompletion.rowCount).toBe(0);

    await expect(maintenance.cleanupExpansionBatch(10, 30_000)).resolves.toBe(true);
    const completed = await pool.query<{
      cleanup_state: string;
      cleanup_lease_generation: number;
      remaining: number;
      deliveries: number;
    }>(
      `
        SELECT
          j.cleanup_state,
          j.cleanup_lease_generation,
          (
            (SELECT count(*) FROM alarm_expansion_recipients WHERE alarm_id = j.alarm_id) +
            (SELECT count(*) FROM alarm_expansion_seen_cursors WHERE alarm_id = j.alarm_id)
          )::integer AS remaining,
          (SELECT count(*)::integer FROM deliveries WHERE alarm_id = j.alarm_id) AS deliveries
        FROM alarm_expansion_jobs j
        WHERE j.alarm_id = $1
      `,
      [alarm.id],
    );
    expect(completed.rows[0]).toEqual({
      cleanup_state: 'COMPLETED',
      cleanup_lease_generation: 3,
      remaining: 0,
      deliveries: 3,
    });
  });

  it.each([
    { position: 'first', recipientCount: 3, successfulBatches: 0 },
    { position: 'middle', recipientCount: 3, successfulBatches: 1 },
    { position: 'last', recipientCount: 2, successfulBatches: 1 },
  ])(
    'USR-17/PERF-04C recovers a $position cleanup delete failure through durable takeover',
    async ({ position, recipientCount, successfulBatches }) => {
      const fixture = await finalizedBulk(recipientCount, `position-${position}`);
      for (let index = 0; index < successfulBatches; index += 1) {
        await expect(maintenance.cleanupExpansionBatch(1, 30_000)).resolves.toBe(true);
      }

      await pool.query(`
        CREATE FUNCTION reject_positioned_cleanup_delete() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          RAISE EXCEPTION 'injected positioned cleanup delete failure';
        END
        $$
      `);
      await pool.query(`
        CREATE TRIGGER reject_positioned_cleanup_delete_trigger
        BEFORE DELETE ON alarm_expansion_recipients
        FOR EACH ROW EXECUTE FUNCTION reject_positioned_cleanup_delete()
      `);
      try {
        await expect(maintenance.cleanupExpansionBatch(1, 30_000)).rejects.toThrow();
      } finally {
        await pool.query(
          'DROP TRIGGER reject_positioned_cleanup_delete_trigger ON alarm_expansion_recipients',
        );
        await pool.query('DROP FUNCTION reject_positioned_cleanup_delete()');
      }

      const failedOwner = await pool.query<{
        alarm_status: string;
        cleanup_lease_active: boolean;
        cleanup_lease_generation: number;
        cleanup_lease_token: string;
        cleanup_run_generation: string;
        cleanup_state: string;
        cursor_count: number;
        delivery_count: number;
        job_state: string;
        recipient_count: number;
      }>(
        `
          SELECT
            a.status AS alarm_status,
            j.state AS job_state,
            j.cleanup_state,
            j.cleanup_run_generation,
            j.cleanup_lease_generation,
            j.cleanup_lease_token,
            j.cleanup_lease_until IS NOT NULL AS cleanup_lease_active,
            (
              SELECT count(*)::integer
              FROM alarm_expansion_recipients r
              WHERE r.alarm_id = j.alarm_id
                AND r.run_generation = j.cleanup_run_generation
            ) AS recipient_count,
            (
              SELECT count(*)::integer
              FROM alarm_expansion_seen_cursors c
              WHERE c.alarm_id = j.alarm_id
                AND c.run_generation = j.cleanup_run_generation
            ) AS cursor_count,
            (SELECT count(*)::integer FROM deliveries d WHERE d.alarm_id = j.alarm_id)
              AS delivery_count
          FROM alarm_expansion_jobs j
          JOIN alarms a ON a.id = j.alarm_id
          WHERE j.alarm_id = $1
        `,
        [fixture.alarmId],
      );
      expect(failedOwner.rows[0]).toEqual({
        alarm_status: 'SENDING',
        cleanup_lease_active: true,
        cleanup_lease_generation: successfulBatches + 1,
        cleanup_lease_token: expect.any(String),
        cleanup_run_generation: fixture.runGeneration,
        cleanup_state: 'RUNNING',
        cursor_count: 2 - successfulBatches,
        delivery_count: recipientCount,
        job_state: 'COMPLETED',
        recipient_count: recipientCount - successfulBatches,
      });

      await expect(maintenance.cleanupExpansionBatch(10, 30_000)).resolves.toBe(false);
      await pool.query(
        `
          UPDATE alarm_expansion_jobs
          SET cleanup_lease_until = clock_timestamp() - interval '1 second'
          WHERE alarm_id = $1
        `,
        [fixture.alarmId],
      );
      await expect(maintenance.cleanupExpansionBatch(10, 30_000)).resolves.toBe(true);

      const staleCompletion = await pool.query(
        `
          UPDATE alarm_expansion_jobs
          SET cleanup_state = 'COMPLETED'
          WHERE alarm_id = $1
            AND cleanup_lease_generation = $2
            AND cleanup_lease_token = $3
        `,
        [
          fixture.alarmId,
          failedOwner.rows[0]?.cleanup_lease_generation,
          failedOwner.rows[0]?.cleanup_lease_token,
        ],
      );
      expect(staleCompletion.rowCount).toBe(0);

      const completed = await pool.query<{
        alarm_status: string;
        cleanup_completed: boolean;
        cleanup_lease_generation: number;
        cleanup_lease_released: boolean;
        cleanup_run_generation: string;
        cleanup_state: string;
        delivery_count: number;
        remaining: number;
      }>(
        `
          SELECT
            a.status AS alarm_status,
            j.cleanup_state,
            j.cleanup_run_generation,
            j.cleanup_lease_generation,
            j.cleanup_lease_token IS NULL AND j.cleanup_lease_until IS NULL
              AS cleanup_lease_released,
            j.cleanup_completed_at IS NOT NULL AS cleanup_completed,
            (
              (SELECT count(*) FROM alarm_expansion_recipients WHERE alarm_id = j.alarm_id) +
              (SELECT count(*) FROM alarm_expansion_seen_cursors WHERE alarm_id = j.alarm_id)
            )::integer AS remaining,
            (SELECT count(*)::integer FROM deliveries WHERE alarm_id = j.alarm_id)
              AS delivery_count
          FROM alarm_expansion_jobs j
          JOIN alarms a ON a.id = j.alarm_id
          WHERE j.alarm_id = $1
        `,
        [fixture.alarmId],
      );
      expect(completed.rows[0]).toEqual({
        alarm_status: 'SENDING',
        cleanup_completed: true,
        cleanup_lease_generation: successfulBatches + 2,
        cleanup_lease_released: true,
        cleanup_run_generation: fixture.runGeneration,
        cleanup_state: 'COMPLETED',
        delivery_count: recipientCount,
        remaining: 0,
      });
    },
  );

  it('USR-17/PERF-04C rolls back the last deletes when completion persistence fails', async () => {
    const fixture = await finalizedBulk(2, 'completion-boundary');
    await pool.query(`
      CREATE FUNCTION reject_cleanup_completion() RETURNS trigger
      LANGUAGE plpgsql AS $$
      BEGIN
        IF NEW.cleanup_state = 'COMPLETED' THEN
          RAISE EXCEPTION 'injected cleanup completion failure';
        END IF;
        RETURN NEW;
      END
      $$
    `);
    await pool.query(`
      CREATE TRIGGER reject_cleanup_completion_trigger
      BEFORE UPDATE OF cleanup_state ON alarm_expansion_jobs
      FOR EACH ROW EXECUTE FUNCTION reject_cleanup_completion()
    `);
    try {
      await expect(maintenance.cleanupExpansionBatch(10, 30_000)).rejects.toThrow();
    } finally {
      await pool.query('DROP TRIGGER reject_cleanup_completion_trigger ON alarm_expansion_jobs');
      await pool.query('DROP FUNCTION reject_cleanup_completion()');
    }

    const failedCompletion = await pool.query<{
      alarm_status: string;
      cleanup_lease_generation: number;
      cleanup_lease_token: string;
      cleanup_run_generation: string;
      cleanup_state: string;
      cursor_count: number;
      delivery_count: number;
      recipient_count: number;
    }>(
      `
        SELECT
          a.status AS alarm_status,
          j.cleanup_state,
          j.cleanup_run_generation,
          j.cleanup_lease_generation,
          j.cleanup_lease_token,
          (
            SELECT count(*)::integer FROM alarm_expansion_recipients
            WHERE alarm_id = j.alarm_id AND run_generation = j.cleanup_run_generation
          ) AS recipient_count,
          (
            SELECT count(*)::integer FROM alarm_expansion_seen_cursors
            WHERE alarm_id = j.alarm_id AND run_generation = j.cleanup_run_generation
          ) AS cursor_count,
          (SELECT count(*)::integer FROM deliveries WHERE alarm_id = j.alarm_id)
            AS delivery_count
        FROM alarm_expansion_jobs j
        JOIN alarms a ON a.id = j.alarm_id
        WHERE j.alarm_id = $1
      `,
      [fixture.alarmId],
    );
    expect(failedCompletion.rows[0]).toEqual({
      alarm_status: 'SENDING',
      cleanup_lease_generation: 1,
      cleanup_lease_token: expect.any(String),
      cleanup_run_generation: fixture.runGeneration,
      cleanup_state: 'RUNNING',
      cursor_count: 2,
      delivery_count: 2,
      recipient_count: 2,
    });

    await expect(maintenance.cleanupExpansionBatch(10, 30_000)).resolves.toBe(false);
    await pool.query(
      `
        UPDATE alarm_expansion_jobs
        SET cleanup_lease_until = clock_timestamp() - interval '1 second'
        WHERE alarm_id = $1
      `,
      [fixture.alarmId],
    );
    await expect(maintenance.cleanupExpansionBatch(10, 30_000)).resolves.toBe(true);

    const completed = await pool.query<{
      cleanup_completed: boolean;
      cleanup_lease_generation: number;
      cleanup_lease_released: boolean;
      cleanup_state: string;
      delivery_count: number;
      remaining: number;
    }>(
      `
        SELECT
          j.cleanup_state,
          j.cleanup_lease_generation,
          j.cleanup_lease_token IS NULL AND j.cleanup_lease_until IS NULL
            AS cleanup_lease_released,
          j.cleanup_completed_at IS NOT NULL AS cleanup_completed,
          (
            (SELECT count(*) FROM alarm_expansion_recipients WHERE alarm_id = j.alarm_id) +
            (SELECT count(*) FROM alarm_expansion_seen_cursors WHERE alarm_id = j.alarm_id)
          )::integer AS remaining,
          (SELECT count(*)::integer FROM deliveries WHERE alarm_id = j.alarm_id)
            AS delivery_count
        FROM alarm_expansion_jobs j
        WHERE j.alarm_id = $1
      `,
      [fixture.alarmId],
    );
    expect(completed.rows[0]).toEqual({
      cleanup_completed: true,
      cleanup_lease_generation: 2,
      cleanup_lease_released: true,
      cleanup_state: 'COMPLETED',
      delivery_count: 2,
      remaining: 0,
    });
  });
});
