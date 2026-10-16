import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';

import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { AlarmApplicationService } from '../../packages/alarms/src/application/alarm-service';
import type {
  DispatchAttempt,
  DispatchClaim,
  PacedPermit,
} from '../../packages/alarms/src/application/dispatch';
import { DispatchProcessor } from '../../packages/alarms/src/application/dispatch';
import { ReconciliationProcessor } from '../../packages/alarms/src/application/reconciliation';
import { runMigrations } from '../../packages/alarms/src/infrastructure/persistence/migrations';
import { PostgresAlarmRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-alarm-repository';
import { PostgresDispatchRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-dispatch-repository';
import { PostgresExpansionRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-expansion-repository';
import { PostgresMaintenanceRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-maintenance-repository';
import { PostgresReconciliationRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-reconciliation-repository';
import { createTestDatabase, type TestDatabase } from './postgres-test-schema';

const databaseUrl = process.env.TEST_DATABASE_URL;
const describeWithDatabase = databaseUrl === undefined ? describe.skip : describe;

interface UnknownFixture {
  readonly alarmId: string;
  readonly deliveryId: string;
  readonly recipientId: string;
}

describeWithDatabase('exact retry and reconciliation boundaries', () => {
  let testDatabase: TestDatabase;
  let pool: Pool;
  let alarms: AlarmApplicationService;
  let dispatch: PostgresDispatchRepository;
  let expansion: PostgresExpansionRepository;
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
    pool = new Pool({ connectionString: testDatabase.databaseUrl, max: 32 });
    alarms = new AlarmApplicationService(
      new PostgresAlarmRepository(pool),
      'retry-reconciliation-exact-cursor-key',
      randomUUID,
    );
    dispatch = new PostgresDispatchRepository(pool);
    expansion = new PostgresExpansionRepository(pool);
    maintenance = new PostgresMaintenanceRepository(pool);
    reconciliation = new PostgresReconciliationRepository(pool);
  });

  afterAll(async () => {
    await pool?.end();
    await testDatabase?.cleanup();
  });

  function permit(): PacedPermit {
    return { reservationId: randomUUID(), scheduledMonotonicMs: performance.now() };
  }

  async function emergency(recipientIds: readonly string[]): Promise<string> {
    const alarm = await alarms.create({
      type: 'EMERGENCY',
      title: `retry-exact-${randomUUID()}`,
      body: 'retry exact body',
      recipientIds,
    });
    await alarms.start(alarm.id);
    return alarm.id;
  }

  async function bulkRecipients(recipientIds: readonly string[]): Promise<string> {
    const alarm = await alarms.create({
      type: 'BULK',
      title: `retry-exact-bulk-${randomUUID()}`,
      body: 'retry exact bulk body',
    });
    await alarms.start(alarm.id);
    const lease = await expansion.claimNext(30_000);
    if (lease === null || lease.alarmId !== alarm.id) {
      throw new Error('Expected exact-test BULK expansion lease');
    }
    for (let offset = 0; offset < recipientIds.length; offset += 1_000) {
      const page = recipientIds.slice(offset, offset + 1_000);
      const nextCursor = offset + page.length < recipientIds.length ? `next:${offset}` : null;
      const result = await expansion.savePage(lease, { recipientIds: page, nextCursor }, 30_000);
      expect(result).toBe(nextCursor === null ? 'COMPLETED' : 'CONTINUE');
    }
    return alarm.id;
  }

  async function claimFor(
    repository: PostgresDispatchRepository,
    alarmId: string,
  ): Promise<DispatchClaim> {
    const claim = await repository.claimNext(0, 60_000);
    if (claim === null || claim.alarmId !== alarmId) {
      throw new Error(`Expected dispatch claim for ${alarmId}`);
    }
    return claim;
  }

  async function startCommitted(
    repository: PostgresDispatchRepository,
    claim: DispatchClaim,
  ): Promise<DispatchAttempt> {
    const attempt = await repository.startAttempt(claim, permit());
    if (attempt === null || !(await repository.commitAttempt(attempt))) {
      throw new Error(`Expected committed attempt for ${claim.deliveryId}`);
    }
    return attempt;
  }

  async function makeDue(deliveryId: string): Promise<void> {
    await pool.query(
      "UPDATE deliveries SET next_action_at = clock_timestamp() - interval '1 millisecond' WHERE id = $1",
      [deliveryId],
    );
  }

  async function cancelAndSweep(alarmId: string): Promise<void> {
    await alarms.cancel(alarmId);
    while (await maintenance.sweepCanceledDeliveries(1_000)) {
      // Drain the bounded production cancellation path.
    }
  }

  async function createUnknown(label: string): Promise<UnknownFixture> {
    const recipientId = `${label}-${randomUUID()}`;
    const alarmId = await emergency([recipientId]);
    const claim = await claimFor(dispatch, alarmId);
    const attempt = await startCommitted(dispatch, claim);
    await dispatch.finish(attempt, { kind: 'AMBIGUOUS', code: 'PROVIDER_TIMEOUT' });
    await makeDue(claim.deliveryId);
    return { alarmId, deliveryId: claim.deliveryId, recipientId };
  }

  it('[RET-06] applies deterministic full jitter at the first and last retry boundaries without a sixth POST', async () => {
    const samples = [0, 0.999_999_999];
    const exactDispatch = new PostgresDispatchRepository(pool, {
      random: () => samples.shift() ?? 0,
    });
    const alarmId = await emergency([`ret06-${randomUUID()}`]);
    const firstClaim = await claimFor(exactDispatch, alarmId);
    const firstAttempt = await startCommitted(exactDispatch, firstClaim);
    await exactDispatch.finish(firstAttempt, {
      kind: 'TRANSIENT_FAILURE',
      code: 'INTERNAL_ERROR',
      httpStatus: 500,
    });
    const firstDelay = await pool.query<{ delay_ms: string }>(
      `
        SELECT extract(epoch FROM (next_action_at - updated_at)) * 1000 AS delay_ms
        FROM deliveries
        WHERE id = $1
      `,
      [firstClaim.deliveryId],
    );
    expect(Math.abs(Number(firstDelay.rows[0]?.delay_ms))).toBeLessThan(2);

    await pool.query(
      `
        UPDATE deliveries
        SET attempt_no = 3, post_budget_count = 3,
            next_action_at = clock_timestamp() - interval '1 millisecond'
        WHERE id = $1
      `,
      [firstClaim.deliveryId],
    );
    const fourthClaim = await claimFor(exactDispatch, alarmId);
    const fourthAttempt = await startCommitted(exactDispatch, fourthClaim);
    expect(fourthAttempt.attemptNo).toBe(4);
    await exactDispatch.finish(fourthAttempt, {
      kind: 'TRANSIENT_FAILURE',
      code: 'SERVICE_UNAVAILABLE',
      httpStatus: 503,
    });
    const fourthDelay = await pool.query<{ delay_ms: string }>(
      `
        SELECT extract(epoch FROM (next_action_at - updated_at)) * 1000 AS delay_ms
        FROM deliveries
        WHERE id = $1
      `,
      [firstClaim.deliveryId],
    );
    expect(Number(fourthDelay.rows[0]?.delay_ms)).toBeGreaterThanOrEqual(1_597);
    expect(Number(fourthDelay.rows[0]?.delay_ms)).toBeLessThanOrEqual(1_600);

    await makeDue(firstClaim.deliveryId);
    const fifthClaim = await claimFor(exactDispatch, alarmId);
    const fifthAttempt = await startCommitted(exactDispatch, fifthClaim);
    expect(fifthAttempt.attemptNo).toBe(5);
    await exactDispatch.finish(fifthAttempt, {
      kind: 'TRANSIENT_FAILURE',
      code: 'SERVICE_UNAVAILABLE',
      httpStatus: 503,
    });

    await expect(exactDispatch.claimNext(0, 60_000)).resolves.toBeNull();
    const terminal = await pool.query<{
      state: string;
      result_code: string;
      post_budget_count: number;
      attempts: number;
    }>(
      `
        SELECT
          d.state,
          d.result_code,
          d.post_budget_count,
          (SELECT count(*)::integer FROM delivery_attempts WHERE delivery_id = d.id) AS attempts
        FROM deliveries d
        WHERE d.id = $1
      `,
      [firstClaim.deliveryId],
    );
    expect(terminal.rows[0]).toEqual({
      state: 'FAILED_RETRY_EXHAUSTED',
      result_code: 'POST_BUDGET_EXHAUSTED',
      post_budget_count: 5,
      attempts: 3,
    });
  });

  it('[RET-09] enforces the 429 count and PostgreSQL-time window at the boundary', async () => {
    const exactDispatch = new PostgresDispatchRepository(pool, { random: () => 0 });
    const countAlarmId = await emergency([`ret09-count-${randomUUID()}`]);
    const countClaim = await claimFor(exactDispatch, countAlarmId);
    await pool.query('UPDATE deliveries SET rate_limit_count = 18 WHERE id = $1', [
      countClaim.deliveryId,
    ]);
    const nineteenth = await startCommitted(exactDispatch, countClaim);
    await exactDispatch.finish(nineteenth, {
      kind: 'RATE_LIMITED',
      code: 'RATE_LIMITED',
      httpStatus: 429,
      retryAfterMs: 100,
    });
    await expect(
      pool.query(
        'SELECT state, rate_limit_count, post_budget_count FROM deliveries WHERE id = $1',
        [countClaim.deliveryId],
      ),
    ).resolves.toMatchObject({
      rows: [{ state: 'RETRY_WAIT', rate_limit_count: 19, post_budget_count: 0 }],
    });

    await makeDue(countClaim.deliveryId);
    const twentiethClaim = await claimFor(exactDispatch, countAlarmId);
    const twentieth = await startCommitted(exactDispatch, twentiethClaim);
    await exactDispatch.finish(twentieth, {
      kind: 'RATE_LIMITED',
      code: 'RATE_LIMITED',
      httpStatus: 429,
      retryAfterMs: 100,
    });
    await expect(
      pool.query('SELECT state, result_code, rate_limit_count FROM deliveries WHERE id = $1', [
        countClaim.deliveryId,
      ]),
    ).resolves.toMatchObject({
      rows: [
        {
          state: 'FAILED_RETRY_EXHAUSTED',
          result_code: 'RATE_LIMIT_EXHAUSTED',
          rate_limit_count: 20,
        },
      ],
    });
    await expect(exactDispatch.claimNext(0, 60_000)).resolves.toBeNull();

    const beforeAlarmId = await emergency([`ret09-before-${randomUUID()}`]);
    const beforeClaim = await claimFor(exactDispatch, beforeAlarmId);
    const beforeAttempt = await startCommitted(exactDispatch, beforeClaim);
    await pool.query(
      `
        UPDATE deliveries
        SET first_rate_limited_at = clock_timestamp() - interval '5 minutes' + interval '2 seconds'
        WHERE id = $1
      `,
      [beforeClaim.deliveryId],
    );
    await exactDispatch.finish(beforeAttempt, {
      kind: 'RATE_LIMITED',
      code: 'RATE_LIMITED',
      httpStatus: 429,
      retryAfterMs: 100,
    });
    await expect(
      pool.query('SELECT state FROM deliveries WHERE id = $1', [beforeClaim.deliveryId]),
    ).resolves.toMatchObject({ rows: [{ state: 'RETRY_WAIT' }] });
    await cancelAndSweep(beforeAlarmId);

    const exactAlarmId = await emergency([`ret09-exact-${randomUUID()}`]);
    const exactClaim = await claimFor(exactDispatch, exactAlarmId);
    const exactAttempt = await startCommitted(exactDispatch, exactClaim);
    await pool.query(
      `
        UPDATE deliveries
        SET first_rate_limited_at = clock_timestamp() - interval '5 minutes'
        WHERE id = $1
      `,
      [exactClaim.deliveryId],
    );
    await exactDispatch.finish(exactAttempt, {
      kind: 'RATE_LIMITED',
      code: 'RATE_LIMITED',
      httpStatus: 429,
      retryAfterMs: 100,
    });
    await expect(
      pool.query('SELECT state, result_code FROM deliveries WHERE id = $1', [
        exactClaim.deliveryId,
      ]),
    ).resolves.toMatchObject({
      rows: [{ state: 'FAILED_RETRY_EXHAUSTED', result_code: 'RATE_LIMIT_EXHAUSTED' }],
    });
  });

  // This intentionally performs 256 serial claim/commit pairs. Parallel 100k plan/publication
  // integration files can saturate the shared PostgreSQL container, so keep the assertion strict
  // but not the default small-test wall-clock budget; timing is not the behaviour under test here.
  it('[RET-09] disperses hundreds of simultaneous 429 retries without consuming the 5xx budget', async () => {
    let sample = 0;
    const exactDispatch = new PostgresDispatchRepository(pool, {
      random: () => (sample++ % 256) / 256,
    });
    const recipients = Array.from(
      { length: 256 },
      (_, index) => `ret09-wave-${String(index).padStart(3, '0')}-${randomUUID()}`,
    );
    const alarmId = await bulkRecipients(recipients);
    const attempts: DispatchAttempt[] = [];
    for (let index = 0; index < recipients.length; index += 1) {
      const claim = await claimFor(exactDispatch, alarmId);
      attempts.push(await startCommitted(exactDispatch, claim));
    }

    await Promise.all(
      attempts.map((attempt) =>
        exactDispatch.finish(attempt, {
          kind: 'RATE_LIMITED',
          code: 'RATE_LIMITED',
          httpStatus: 429,
          retryAfterMs: 1_000,
        }),
      ),
    );

    const spread = await pool.query<{
      count: number;
      min_delay_ms: string;
      max_delay_ms: string;
      distinct_delays: number;
      wrong_budget: number;
    }>(
      `
          SELECT
            count(*)::integer AS count,
            min(extract(epoch FROM (next_action_at - updated_at)) * 1000) AS min_delay_ms,
            max(extract(epoch FROM (next_action_at - updated_at)) * 1000) AS max_delay_ms,
            count(DISTINCT round(extract(epoch FROM (next_action_at - updated_at)) * 1000))::integer
              AS distinct_delays,
            count(*) FILTER (WHERE post_budget_count <> 0 OR rate_limit_count <> 1)::integer
              AS wrong_budget
          FROM deliveries
          WHERE alarm_id = $1
        `,
      [alarmId],
    );
    expect(spread.rows[0]?.count).toBe(256);
    expect(Number(spread.rows[0]?.min_delay_ms)).toBeGreaterThanOrEqual(998);
    expect(Number(spread.rows[0]?.max_delay_ms)).toBeLessThan(2_001);
    expect(spread.rows[0]?.distinct_delays).toBeGreaterThan(200);
    expect(spread.rows[0]?.wrong_budget).toBe(0);
    await cancelAndSweep(alarmId);
  }, 120_000);

  it('[RET-12][RET-15] gives positive evidence priority at the exact elapsed deadline', async () => {
    const target = await createUnknown('ret12-positive-deadline');
    await pool.query(
      `
        UPDATE deliveries
        SET first_unknown_at = clock_timestamp() - interval '60 seconds',
            next_action_at = clock_timestamp()
        WHERE id = $1
      `,
      [target.deliveryId],
    );
    const claim = await reconciliation.claimNext(15_000);
    if (claim === null || claim.deliveryId !== target.deliveryId) {
      throw new Error('Expected exact-deadline positive reconciliation claim');
    }
    await reconciliation.record(claim, {
      kind: 'OBSERVATION',
      entries: [
        {
          messageId: 'ret12-exact-positive',
          recipientId: target.recipientId,
          sentAt: '2026-10-15T00:00:00.000Z',
        },
      ],
    });

    await expect(alarms.findById(target.alarmId)).resolves.toMatchObject({
      status: 'COMPLETED',
      active: 0,
      unresolved: 0,
      sent: 1,
    });
    const stored = await pool.query<{
      state: string;
      provider_message_id: string;
      attempts: number;
    }>(
      `
        SELECT
          d.state,
          d.provider_message_id,
          (SELECT count(*)::integer FROM delivery_attempts WHERE delivery_id = d.id) AS attempts
        FROM deliveries d
        WHERE d.id = $1
      `,
      [target.deliveryId],
    );
    expect(stored.rows[0]).toEqual({
      state: 'SENT',
      provider_message_id: 'ret12-exact-positive',
      attempts: 1,
    });
    await expect(dispatch.hasEligible(0)).resolves.toBe(false);
  });

  it('[RET-17] keeps a rejected adapter UNKNOWN, reconciles by GET, and never re-POSTs', async () => {
    const recipientId = `ret17-adapter-rejected-${randomUUID()}`;
    const alarmId = await emergency([recipientId]);
    const claim = await claimFor(dispatch, alarmId);
    const attempt = await startCommitted(dispatch, claim);
    await dispatch.finish(attempt, {
      kind: 'AMBIGUOUS',
      code: 'PROVIDER_ADAPTER_REJECTED',
    });

    await expect(
      pool.query(
        `
          SELECT
            d.state,
            d.post_budget_count,
            (SELECT count(*)::integer FROM delivery_attempts WHERE delivery_id = d.id) AS attempts
          FROM deliveries d
          WHERE d.id = $1
        `,
        [claim.deliveryId],
      ),
    ).resolves.toMatchObject({
      rows: [{ state: 'UNKNOWN', post_budget_count: 1, attempts: 1 }],
    });
    await expect(dispatch.hasEligible(0)).resolves.toBe(false);
    await expect(dispatch.claimNext(0, 60_000)).resolves.toBeNull();

    await makeDue(claim.deliveryId);
    const historyClaim = await reconciliation.claimNext(15_000);
    if (historyClaim === null || historyClaim.deliveryId !== claim.deliveryId) {
      throw new Error('Expected RET-17 history-only reconciliation claim');
    }
    await reconciliation.record(historyClaim, {
      kind: 'OBSERVATION',
      entries: [{ messageId: 'ret17-history-message', recipientId }],
    });
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'COMPLETED',
      sent: 1,
      active: 0,
    });
    await expect(
      pool.query(
        `
          SELECT
            d.state,
            (SELECT count(*)::integer FROM delivery_attempts WHERE delivery_id = d.id) AS attempts
          FROM deliveries d
          WHERE d.id = $1
        `,
        [claim.deliveryId],
      ),
    ).resolves.toMatchObject({ rows: [{ state: 'SENT', attempts: 1 }] });
    await expect(dispatch.claimNext(0, 60_000)).resolves.toBeNull();
  });

  it.each([
    { label: 'timeout', result: { kind: 'UNAVAILABLE', code: 'HISTORY_TRANSPORT_ERROR' } as const },
    { label: '503', result: { kind: 'UNAVAILABLE', code: 'HISTORY_HTTP_503' } as const },
    {
      label: 'malformed body',
      result: { kind: 'UNAVAILABLE', code: 'MALFORMED_HISTORY_RESPONSE' } as const,
    },
    { label: 'opaque empty', result: { kind: 'OBSERVATION', entries: [] } as const },
  ])(
    '[RET-15][RET-22] keeps $label UNKNOWN before the budget and converges once at the deadline',
    async ({ label, result }) => {
      const exactReconciliation = new PostgresReconciliationRepository(pool, {
        maxFastChecks: 2,
        maxFastElapsedMs: 60_000,
      });
      const target = await createUnknown(`ret15-${label.replaceAll(' ', '-')}`);
      const first = await exactReconciliation.claimNext(15_000);
      if (first === null || first.deliveryId !== target.deliveryId) {
        throw new Error(`Expected first ${label} reconciliation claim`);
      }
      await exactReconciliation.record(first, result);
      const beforeDeadline = await pool.query<{
        state: string;
        reconcile_count: number;
        attempts: number;
      }>(
        `
          SELECT
            d.state,
            d.reconcile_count,
            (SELECT count(*)::integer FROM delivery_attempts WHERE delivery_id = d.id) AS attempts
          FROM deliveries d
          WHERE d.id = $1
        `,
        [target.deliveryId],
      );
      expect(beforeDeadline.rows[0]).toEqual({ state: 'UNKNOWN', reconcile_count: 1, attempts: 1 });
      await expect(dispatch.hasEligible(0)).resolves.toBe(false);

      await makeDue(target.deliveryId);
      const deadline = await exactReconciliation.claimNext(15_000);
      if (deadline === null || deadline.deliveryId !== target.deliveryId) {
        throw new Error(`Expected deadline ${label} reconciliation claim`);
      }
      await exactReconciliation.record(deadline, result);
      const terminal = await pool.query<{
        state: string;
        reconcile_count: number;
        attempts: number;
        unconsumed_evidence: number;
      }>(
        `
          SELECT
            d.state,
            d.reconcile_count,
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
      );
      expect(terminal.rows[0]).toEqual({
        state: 'NEEDS_REVIEW',
        reconcile_count: 2,
        attempts: 1,
        unconsumed_evidence: 0,
      });
      await expect(alarms.findById(target.alarmId)).resolves.toMatchObject({
        status: 'ATTENTION_REQUIRED',
        active: 0,
        unresolved: 1,
        sent: 0,
      });
      await expect(dispatch.hasEligible(0)).resolves.toBe(false);
    },
  );

  it('[RET-22] recovers an expired reconcile lease, fences the killed owner, and counts one lookup result', async () => {
    const target = await createUnknown('ret22-reconcile-kill');
    const stale = await reconciliation.claimNext(15_000);
    if (stale === null || stale.deliveryId !== target.deliveryId) {
      throw new Error('Expected stale RET-22 reconciliation claim');
    }
    await pool.query(
      "UPDATE deliveries SET reconcile_until = clock_timestamp() - interval '1 millisecond' WHERE id = $1",
      [target.deliveryId],
    );
    const current = await reconciliation.claimNext(15_000);
    if (current === null || current.deliveryId !== target.deliveryId) {
      throw new Error('Expected RET-22 takeover claim');
    }
    expect(current.reconcileGeneration).toBe(stale.reconcileGeneration + 1);
    expect(current.reconcileToken).not.toBe(stale.reconcileToken);

    await reconciliation.record(stale, { kind: 'UNAVAILABLE', code: 'KILLED_DURING_LOOKUP' });
    const afterStale = await pool.query<{
      state: string;
      reconcile_count: number;
      reconcile_generation: number;
      reconcile_token: string;
    }>(
      'SELECT state, reconcile_count, reconcile_generation, reconcile_token FROM deliveries WHERE id = $1',
      [target.deliveryId],
    );
    expect(afterStale.rows[0]).toEqual({
      state: 'RECONCILING',
      reconcile_count: 0,
      reconcile_generation: current.reconcileGeneration,
      reconcile_token: current.reconcileToken,
    });

    await reconciliation.record(current, { kind: 'UNAVAILABLE', code: 'HISTORY_HTTP_503' });
    const recovered = await pool.query<{
      state: string;
      reconcile_count: number;
      current_attempt_id: string;
      attempts: number;
    }>(
      `
        SELECT
          d.state,
          d.reconcile_count,
          d.current_attempt_id,
          (SELECT count(*)::integer FROM delivery_attempts WHERE delivery_id = d.id) AS attempts
        FROM deliveries d
        WHERE d.id = $1
      `,
      [target.deliveryId],
    );
    expect(recovered.rows[0]).toMatchObject({
      state: 'UNKNOWN',
      reconcile_count: 1,
      attempts: 1,
    });
    expect(recovered.rows[0]?.current_attempt_id).not.toBeNull();
    await pool.query(
      "UPDATE deliveries SET next_action_at = clock_timestamp() + interval '1 hour' WHERE id = $1",
      [target.deliveryId],
    );
  });

  it('[RET-23] recovers review claim/GET/observation kill points without changing counters twice', async () => {
    const target = await createUnknown('ret23-review-kill');
    await pool.query(
      `
        UPDATE deliveries
        SET
          state = 'NEEDS_REVIEW',
          reconcile_count = 12,
          current_attempt_id = NULL,
          next_review_at = clock_timestamp(),
          review_until = clock_timestamp() + interval '24 hours'
        WHERE id = $1
      `,
      [target.deliveryId],
    );
    await pool.query(
      `
        UPDATE alarms
        SET
          active = 0,
          unresolved = 1,
          status = 'ATTENTION_REQUIRED',
          finished_at = clock_timestamp()
        WHERE id = $1
      `,
      [target.alarmId],
    );
    const before = await alarms.findById(target.alarmId);
    const stale = await reconciliation.claimNext(15_000);
    if (stale === null || stale.deliveryId !== target.deliveryId) {
      throw new Error('Expected stale RET-23 review claim');
    }
    await pool.query(
      "UPDATE deliveries SET reconcile_until = clock_timestamp() - interval '1 millisecond' WHERE id = $1",
      [target.deliveryId],
    );
    const current = await reconciliation.claimNext(15_000);
    if (current === null || current.deliveryId !== target.deliveryId) {
      throw new Error('Expected RET-23 review takeover');
    }

    await reconciliation.record(stale, {
      kind: 'OBSERVATION',
      entries: [
        {
          messageId: 'ret23-late-positive',
          recipientId: target.recipientId,
          sentAt: '2026-10-15T00:00:00.000Z',
        },
      ],
    });
    const between = await pool.query<{
      state: string;
      unconsumed: number;
      reconcile_generation: number;
      reconcile_token: string;
    }>(
      `
        SELECT
          d.state,
          count(e.id) FILTER (WHERE e.consumed_at IS NULL)::integer AS unconsumed,
          d.reconcile_generation,
          d.reconcile_token
        FROM deliveries d
        LEFT JOIN delivery_evidence e ON e.delivery_id = d.id
        WHERE d.id = $1
        GROUP BY d.id
      `,
      [target.deliveryId],
    );
    expect(between.rows[0]).toEqual({
      state: 'NEEDS_REVIEW',
      unconsumed: 1,
      reconcile_generation: current.reconcileGeneration,
      reconcile_token: current.reconcileToken,
    });
    await expect(alarms.findById(target.alarmId)).resolves.toMatchObject({
      active: 0,
      unresolved: 1,
      sent: 0,
    });

    await reconciliation.record(current, { kind: 'UNAVAILABLE', code: 'WORKER_RESTARTED' });
    const corrected = await alarms.findById(target.alarmId);
    expect(corrected).toMatchObject({
      status: 'COMPLETED',
      active: 0,
      unresolved: 0,
      sent: 1,
    });
    expect(corrected.finishedAt).toEqual(before.finishedAt);
    const delivery = await pool.query<{
      state: string;
      corrected_at: Date;
      attempts: number;
      unconsumed: number;
    }>(
      `
        SELECT
          d.state,
          d.corrected_at,
          (SELECT count(*)::integer FROM delivery_attempts WHERE delivery_id = d.id) AS attempts,
          (
            SELECT count(*)::integer
            FROM delivery_evidence
            WHERE delivery_id = d.id AND consumed_at IS NULL
          ) AS unconsumed
        FROM deliveries d
        WHERE d.id = $1
      `,
      [target.deliveryId],
    );
    expect(delivery.rows[0]).toMatchObject({ state: 'SENT', attempts: 1, unconsumed: 0 });
    expect(delivery.rows[0]?.corrected_at).toBeInstanceOf(Date);
    await expect(dispatch.hasEligible(0)).resolves.toBe(false);
  });

  it('[EO-01 negative proof] exposes two provider side effects when an authoritative-empty capability lies', async () => {
    const providerHistory: Array<{
      messageId: string;
      recipientId: string;
      sentAt: string;
    }> = [];
    let sendCount = 0;
    const processor = new DispatchProcessor(
      dispatch,
      {
        reserve: async () => ({
          reservationId: randomUUID(),
          scheduledMonotonicMs: performance.now(),
        }),
      },
      {
        send: async ({ recipientId }) => {
          sendCount += 1;
          const messageId = `eo01-provider-message-${sendCount}`;
          providerHistory.push({
            messageId,
            recipientId,
            sentAt: `2026-10-15T00:00:0${sendCount}.000Z`,
          });
          return sendCount === 1
            ? { kind: 'AMBIGUOUS' as const, code: 'RESPONSE_LOST_AFTER_SIDE_EFFECT' }
            : { kind: 'ACCEPTED' as const, messageId };
        },
      },
      {
        monotonicNow: () => performance.now(),
        waitUntil: async () => undefined,
        heartbeatScheduler: {
          start: () => ({ stop: async () => undefined }),
        },
        allowedLateMs: 1_000,
      },
    );
    const strong = new PostgresReconciliationRepository(pool, {
      historyVisibilityProfile: 'AUTHORITATIVE_EMPTY_AFTER_GRACE',
      authoritativeEmptyGraceMs: 5_000,
    });
    const recipientId = `eo01-recipient-${randomUUID()}`;
    const alarmId = await emergency([recipientId]);

    const firstDispatch = await processor.beginOnce(0);
    if (firstDispatch === null) {
      throw new Error('Expected EO-01 first provider dispatch');
    }
    await firstDispatch.completion;
    const target = await pool.query<{ id: string }>(
      'SELECT id FROM deliveries WHERE alarm_id = $1',
      [alarmId],
    );
    const deliveryId = target.rows[0]?.id;
    if (deliveryId === undefined) {
      throw new Error('Expected EO-01 Delivery');
    }
    await pool.query(
      `
        UPDATE deliveries
        SET
          first_unknown_at = clock_timestamp() - interval '10 seconds',
          next_action_at = clock_timestamp() - interval '1 millisecond'
        WHERE id = $1
      `,
      [deliveryId],
    );

    // Keep one lookup in flight while a second owner observes a false authoritative empty. This
    // models a provider that advertised a strong visibility capability but hid the first side
    // effect past its grace period.
    const staleLookup = await strong.claimNext(15_000);
    if (staleLookup === null || staleLookup.deliveryId !== deliveryId) {
      throw new Error('Expected EO-01 stale lookup owner');
    }
    await pool.query(
      "UPDATE deliveries SET reconcile_until = clock_timestamp() - interval '1 millisecond' WHERE id = $1",
      [deliveryId],
    );
    const emptyLookup = await strong.claimNext(15_000);
    if (emptyLookup === null || emptyLookup.deliveryId !== deliveryId) {
      throw new Error('Expected EO-01 current lookup owner');
    }
    await strong.record(emptyLookup, { kind: 'OBSERVATION', entries: [] });

    const secondDispatch = await processor.beginOnce(0);
    if (secondDispatch === null) {
      throw new Error('Expected EO-01 unsafe retry dispatch');
    }
    await secondDispatch.completion;
    expect(providerHistory.map(({ messageId }) => messageId)).toEqual([
      'eo01-provider-message-1',
      'eo01-provider-message-2',
    ]);

    // The late first lookup now sees both real provider records. Its stale owner cannot rewrite
    // SENT directly, so the append-only evidence scanner raises the durable duplicate incident.
    await strong.record(staleLookup, { kind: 'OBSERVATION', entries: providerHistory });
    await expect(maintenance.adoptUnconsumedEvidence()).resolves.toBe(true);

    const proof = await pool.query<{
      attempts: number;
      duplicate_detected: boolean;
      post_budget_count: number;
      post_terminal_incident_code: string;
      provider_message_id: string;
      state: string;
      unconsumed_evidence: number;
    }>(
      `
        SELECT
          d.state,
          d.provider_message_id,
          d.post_budget_count,
          d.duplicate_detected,
          d.post_terminal_incident_code,
          (SELECT count(*)::integer FROM delivery_attempts WHERE delivery_id = d.id) AS attempts,
          (
            SELECT count(*)::integer
            FROM delivery_evidence
            WHERE delivery_id = d.id AND consumed_at IS NULL
          ) AS unconsumed_evidence
        FROM deliveries d
        WHERE d.id = $1
      `,
      [deliveryId],
    );
    expect(proof.rows[0]).toEqual({
      attempts: 2,
      duplicate_detected: true,
      post_budget_count: 2,
      post_terminal_incident_code: 'LATE_DISTINCT_DUPLICATE',
      provider_message_id: 'eo01-provider-message-2',
      state: 'SENT',
      unconsumed_evidence: 0,
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
    });
    await expect(dispatch.hasEligible(0)).resolves.toBe(false);
  });

  it('[EO-02] never re-POSTs a hidden side effect from opaque empty history', async () => {
    const target = await createUnknown('eo02-hidden-side-effect');
    await pool.query(
      `
        UPDATE deliveries
        SET
          reconcile_count = 11,
          first_unknown_at = clock_timestamp() - interval '65 seconds',
          next_action_at = clock_timestamp()
        WHERE id = $1
      `,
      [target.deliveryId],
    );
    const claim = await reconciliation.claimNext(15_000);
    if (claim === null || claim.deliveryId !== target.deliveryId) {
      throw new Error('Expected EO-02 hidden-side-effect lookup');
    }
    await reconciliation.record(claim, { kind: 'OBSERVATION', entries: [] });

    const proof = await pool.query<{
      state: string;
      post_budget_count: number;
      attempts: number;
      unconsumed_evidence: number;
    }>(
      `
        SELECT
          d.state,
          d.post_budget_count,
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
    );
    expect(proof.rows[0]).toEqual({
      state: 'NEEDS_REVIEW',
      post_budget_count: 1,
      attempts: 1,
      unconsumed_evidence: 0,
    });
    await expect(dispatch.hasEligible(0)).resolves.toBe(false);
    await expect(dispatch.claimNext(0, 60_000)).resolves.toBeNull();
    await expect(alarms.findById(target.alarmId)).resolves.toMatchObject({
      status: 'ATTENTION_REQUIRED',
      active: 0,
      unresolved: 1,
      sent: 0,
      failed: 0,
    });
  });

  it('[RET-19] permits exactly the fifth POST after strong authoritative-empty evidence at budget four', async () => {
    const strong = new PostgresReconciliationRepository(pool, {
      historyVisibilityProfile: 'AUTHORITATIVE_EMPTY_AFTER_GRACE',
      authoritativeEmptyGraceMs: 0,
    });
    const target = await createUnknown('ret19-budget-four');
    await pool.query(
      `
        UPDATE deliveries
        SET attempt_no = 4, post_budget_count = 4, next_action_at = clock_timestamp()
        WHERE id = $1
      `,
      [target.deliveryId],
    );
    const reconcileClaim = await strong.claimNext(15_000);
    if (reconcileClaim === null || reconcileClaim.deliveryId !== target.deliveryId) {
      throw new Error('Expected RET-19 budget-four reconciliation claim');
    }
    await strong.record(reconcileClaim, { kind: 'OBSERVATION', entries: [] });

    await expect(
      pool.query(
        'SELECT state, post_budget_count, current_attempt_id FROM deliveries WHERE id = $1',
        [target.deliveryId],
      ),
    ).resolves.toMatchObject({
      rows: [{ state: 'RETRY_WAIT', post_budget_count: 4, current_attempt_id: null }],
    });

    const fifthClaim = await claimFor(dispatch, target.alarmId);
    const fifthAttempt = await startCommitted(dispatch, fifthClaim);
    expect(fifthAttempt.attemptNo).toBe(5);
    await dispatch.finish(fifthAttempt, { kind: 'ACCEPTED', messageId: 'ret19-fifth-accepted' });

    await expect(dispatch.claimNext(0, 60_000)).resolves.toBeNull();
    await expect(
      pool.query(
        `
          SELECT
            d.state,
            d.post_budget_count,
            (SELECT count(*)::integer FROM delivery_attempts WHERE delivery_id = d.id) AS attempts
          FROM deliveries d
          WHERE d.id = $1
        `,
        [target.deliveryId],
      ),
    ).resolves.toMatchObject({
      rows: [{ state: 'SENT', post_budget_count: 5, attempts: 2 }],
    });
  });

  it('[RET-19] fails at budget five and lets cancellation win under strong visibility', async () => {
    const strong = new PostgresReconciliationRepository(pool, {
      historyVisibilityProfile: 'AUTHORITATIVE_EMPTY_AFTER_GRACE',
      authoritativeEmptyGraceMs: 0,
    });

    const exhausted = await createUnknown('ret19-budget-five');
    await pool.query(
      `
        UPDATE deliveries
        SET attempt_no = 5, post_budget_count = 5, next_action_at = clock_timestamp()
        WHERE id = $1
      `,
      [exhausted.deliveryId],
    );
    const exhaustedClaim = await strong.claimNext(15_000);
    if (exhaustedClaim === null || exhaustedClaim.deliveryId !== exhausted.deliveryId) {
      throw new Error('Expected RET-19 budget-five reconciliation claim');
    }
    await strong.record(exhaustedClaim, { kind: 'OBSERVATION', entries: [] });
    await expect(
      pool.query('SELECT state, result_code, post_budget_count FROM deliveries WHERE id = $1', [
        exhausted.deliveryId,
      ]),
    ).resolves.toMatchObject({
      rows: [
        {
          state: 'FAILED_RETRY_EXHAUSTED',
          result_code: 'POST_BUDGET_EXHAUSTED_AFTER_RECONCILE_EMPTY',
          post_budget_count: 5,
        },
      ],
    });
    await expect(alarms.findById(exhausted.alarmId)).resolves.toMatchObject({
      status: 'FAILED',
      active: 0,
      failed: 1,
    });

    const canceled = await createUnknown('ret19-canceled');
    await pool.query(
      `
        UPDATE deliveries
        SET attempt_no = 5, post_budget_count = 5, next_action_at = clock_timestamp()
        WHERE id = $1
      `,
      [canceled.deliveryId],
    );
    await alarms.cancel(canceled.alarmId);
    const canceledClaim = await strong.claimNext(15_000);
    if (canceledClaim === null || canceledClaim.deliveryId !== canceled.deliveryId) {
      throw new Error('Expected RET-19 canceled reconciliation claim');
    }
    await strong.record(canceledClaim, { kind: 'OBSERVATION', entries: [] });
    await expect(
      pool.query('SELECT state, result_code, post_budget_count FROM deliveries WHERE id = $1', [
        canceled.deliveryId,
      ]),
    ).resolves.toMatchObject({
      rows: [
        {
          state: 'CANCELED',
          result_code: 'HISTORY_AUTHORITATIVE_EMPTY_CANCELED',
          post_budget_count: 5,
        },
      ],
    });
    await expect(alarms.findById(canceled.alarmId)).resolves.toMatchObject({
      status: 'CANCELED',
      active: 0,
      canceled: 1,
      failed: 0,
    });
    await expect(dispatch.claimNext(0, 60_000)).resolves.toBeNull();
  });

  it('[RET-19] never opens the authoritative-empty retry path in the default opaque profile', async () => {
    const opaqueDeadline = new PostgresReconciliationRepository(pool, {
      maxFastChecks: 1,
      maxFastElapsedMs: 60_000,
    });
    const target = await createUnknown('ret19-opaque');
    await pool.query(
      `
        UPDATE deliveries
        SET attempt_no = 4, post_budget_count = 4, next_action_at = clock_timestamp()
        WHERE id = $1
      `,
      [target.deliveryId],
    );
    const claim = await opaqueDeadline.claimNext(15_000);
    if (claim === null || claim.deliveryId !== target.deliveryId) {
      throw new Error('Expected RET-19 opaque reconciliation claim');
    }
    await opaqueDeadline.record(claim, { kind: 'OBSERVATION', entries: [] });

    await expect(
      pool.query('SELECT state, post_budget_count FROM deliveries WHERE id = $1', [
        target.deliveryId,
      ]),
    ).resolves.toMatchObject({
      rows: [{ state: 'NEEDS_REVIEW', post_budget_count: 4 }],
    });
    await expect(dispatch.hasEligible(0)).resolves.toBe(false);
    await expect(dispatch.claimNext(0, 60_000)).resolves.toBeNull();
  });

  it('[RET-20] drains 1,000 UNKNOWN lookups at concurrency eight while normal POST/readiness progress', async () => {
    const backlogRecipients = Array.from(
      { length: 1_000 },
      (_, index) => `ret20-${String(index).padStart(4, '0')}-${randomUUID()}`,
    );
    const backlogAlarmId = await bulkRecipients(backlogRecipients);
    await pool.query(
      `
        UPDATE deliveries
        SET
          state = 'UNKNOWN',
          result_code = 'RET20_FIXTURE_AMBIGUOUS',
          first_unknown_at = clock_timestamp(),
          next_action_at = clock_timestamp()
        WHERE alarm_id = $1
      `,
      [backlogAlarmId],
    );

    const reviewTarget = await createUnknown('ret20-review-priority');
    await pool.query(
      `
        UPDATE deliveries
        SET
          state = 'NEEDS_REVIEW',
          reconcile_count = 12,
          current_attempt_id = NULL,
          next_review_at = clock_timestamp(),
          review_until = clock_timestamp() + interval '24 hours'
        WHERE id = $1
      `,
      [reviewTarget.deliveryId],
    );
    await pool.query(
      `
        UPDATE alarms
        SET active = 0, unresolved = 1, status = 'ATTENTION_REQUIRED',
            finished_at = clock_timestamp()
        WHERE id = $1
      `,
      [reviewTarget.alarmId],
    );

    const recipientByRef = new Map<string, string>();
    const rows = await pool.query<{ client_ref: string; recipient_id: string }>(
      `
        SELECT client_ref, recipient_id
        FROM deliveries
        WHERE alarm_id IN ($1, $2)
      `,
      [backlogAlarmId, reviewTarget.alarmId],
    );
    for (const row of rows.rows) {
      recipientByRef.set(row.client_ref, row.recipient_id);
    }
    const reviewRef = rows.rows.find(
      ({ recipient_id }) => recipient_id === reviewTarget.recipientId,
    )?.client_ref;
    if (reviewRef === undefined) {
      throw new Error('Expected RET-20 review clientRef');
    }

    let activeLookups = 0;
    let maximumLookups = 0;
    let lookupCount = 0;
    const lookupOrder: string[] = [];
    let releaseFirstWave: (() => void) | undefined;
    const firstWave = new Promise<void>((resolve) => {
      releaseFirstWave = resolve;
    });
    const processor = new ReconciliationProcessor(reconciliation, {
      lookup: async (clientRef) => {
        activeLookups += 1;
        maximumLookups = Math.max(maximumLookups, activeLookups);
        lookupCount += 1;
        lookupOrder.push(clientRef);
        try {
          if (lookupCount <= 8) {
            await firstWave;
          } else {
            await new Promise<void>((resolve) => setTimeout(resolve, 0));
          }
          const recipientId = recipientByRef.get(clientRef);
          if (recipientId === undefined) {
            throw new Error(`Missing RET-20 recipient for ${clientRef}`);
          }
          return {
            kind: 'OBSERVATION' as const,
            entries: [{ messageId: `ret20-message-${clientRef}`, recipientId }],
          };
        } finally {
          activeLookups -= 1;
        }
      },
    });

    const initial = Array.from({ length: 8 }, () => processor.runOnce());
    const concurrencyDeadline = performance.now() + 5_000;
    while (maximumLookups < 8) {
      if (performance.now() >= concurrencyDeadline) {
        throw new Error('Timed out waiting for eight RET-20 lookups');
      }
      await new Promise<void>((resolve) => setTimeout(resolve, 1));
    }

    const normalAlarmId = await emergency([`ret20-normal-post-${randomUUID()}`]);
    const normalClaim = await claimFor(dispatch, normalAlarmId);
    const normalAttempt = await startCommitted(dispatch, normalClaim);
    await dispatch.finish(normalAttempt, { kind: 'ACCEPTED', messageId: 'ret20-normal-accepted' });
    await expect(alarms.findById(normalAlarmId)).resolves.toMatchObject({
      status: 'COMPLETED',
      sent: 1,
    });
    await expect(pool.query('SELECT 1 AS ready')).resolves.toMatchObject({ rows: [{ ready: 1 }] });

    releaseFirstWave?.();
    await Promise.all(initial);
    await Promise.all(
      Array.from({ length: 8 }, async () => {
        while (await processor.runOnce()) {
          // The production repository owns all claim/fence/state transitions.
        }
      }),
    );

    expect(maximumLookups).toBe(8);
    expect(lookupCount).toBe(1_001);
    expect(lookupOrder.indexOf(reviewRef)).toBe(1_000);
    const backlog = await pool.query<{
      total: number;
      sent: number;
      attempts: number;
      unconsumed: number;
    }>(
      `
        SELECT
          count(*)::integer AS total,
          count(*) FILTER (WHERE state = 'SENT')::integer AS sent,
          (
            SELECT count(*)::integer
            FROM delivery_attempts da
            JOIN deliveries attempted ON attempted.id = da.delivery_id
            WHERE attempted.alarm_id = $1
          ) AS attempts,
          (
            SELECT count(*)::integer
            FROM delivery_evidence e
            JOIN deliveries evidenced ON evidenced.id = e.delivery_id
            WHERE evidenced.alarm_id = $1 AND e.consumed_at IS NULL
          ) AS unconsumed
        FROM deliveries
        WHERE alarm_id = $1
      `,
      [backlogAlarmId],
    );
    expect(backlog.rows[0]).toEqual({ total: 1_000, sent: 1_000, attempts: 0, unconsumed: 0 });
    await expect(alarms.findById(backlogAlarmId)).resolves.toMatchObject({
      status: 'COMPLETED',
      total: 1_000,
      sent: 1_000,
      active: 0,
    });
    await expect(alarms.findById(reviewTarget.alarmId)).resolves.toMatchObject({
      status: 'COMPLETED',
      sent: 1,
      unresolved: 0,
    });
  }, 120_000);
});
