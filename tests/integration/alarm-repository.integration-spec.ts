import { resolve } from 'node:path';

import { Pool } from 'pg';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import { AlarmStateConflictError } from '../../packages/alarms/src/application/alarm-errors';
import { runMigrations } from '../../packages/alarms/src/infrastructure/persistence/migrations';
import { PostgresAlarmRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-alarm-repository';
import { PostgresDispatchRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-dispatch-repository';
import { createTestDatabase, type TestDatabase } from './postgres-test-schema';

const baseDatabaseUrl = process.env.TEST_DATABASE_URL;
const describeWithDatabase = baseDatabaseUrl === undefined ? describe.skip : describe;

describeWithDatabase('PostgresAlarmRepository', () => {
  let testDatabase: TestDatabase;
  let pool: Pool;
  let repository: PostgresAlarmRepository;

  beforeAll(async () => {
    if (baseDatabaseUrl === undefined) {
      throw new Error('TEST_DATABASE_URL is required');
    }

    testDatabase = await createTestDatabase(baseDatabaseUrl);
    await runMigrations({
      databaseUrl: testDatabase.databaseUrl,
      migrationsDirectory: resolve(process.cwd(), 'migrations'),
    });
    pool = new Pool({ connectionString: testDatabase.databaseUrl, max: 24 });
    repository = new PostgresAlarmRepository(pool);
  });

  beforeEach(async () => {
    await pool.query('TRUNCATE alarms CASCADE');
  });

  afterAll(async () => {
    await pool?.end();
    await testDatabase?.cleanup();
  });

  it('creates an emergency snapshot without normalization', async () => {
    const alarmId = '11111111-1111-4111-8111-111111111111';
    const created = await repository.create({
      id: alarmId,
      type: 'EMERGENCY',
      title: 'urgent',
      body: 'body',
      recipientIds: ['A', 'a', '\u00e9', 'e\u0301'],
    });
    const recipients = await pool.query<{ recipient_id: string }>(
      'SELECT recipient_id FROM emergency_recipients WHERE alarm_id = $1 ORDER BY ordinal',
      [alarmId],
    );

    expect(created.status).toBe('CREATED');
    expect(recipients.rows.map(({ recipient_id }) => recipient_id)).toEqual([
      'A',
      'a',
      '\u00e9',
      'e\u0301',
    ]);
  });

  it('serializes concurrent emergency starts and creates each delivery once', async () => {
    const alarmId = '22222222-2222-4222-8222-222222222222';
    await repository.create({
      id: alarmId,
      type: 'EMERGENCY',
      title: 'urgent',
      body: 'body',
      recipientIds: ['u1', 'u2', 'u3'],
    });

    const results = await Promise.all([
      repository.start(alarmId),
      repository.start(alarmId),
      repository.start(alarmId),
    ]);
    const count = await pool.query<{ count: string }>(
      'SELECT count(*) FROM deliveries WHERE alarm_id = $1',
      [alarmId],
    );

    expect(results.every(({ status }) => status === 'SENDING')).toBe(true);
    expect(results.every(({ total }) => total === 3)).toBe(true);
    expect(count.rows[0]?.count).toBe('3');
  });

  it('creates the BULK expansion job and first-page sentinel atomically', async () => {
    const alarmId = '23232323-2323-4232-8232-232323232323';
    await repository.create({
      id: alarmId,
      type: 'BULK',
      title: 'bulk',
      body: 'body',
      recipientIds: [],
    });

    await expect(repository.start(alarmId)).resolves.toMatchObject({ status: 'QUEUED' });
    const persisted = await pool.query<{ jobs: number; cursors: number }>(
      `
        SELECT
          (SELECT count(*)::integer FROM alarm_expansion_jobs WHERE alarm_id = $1) AS jobs,
          (SELECT count(*)::integer FROM alarm_expansion_seen_cursors WHERE alarm_id = $1) AS cursors
      `,
      [alarmId],
    );

    expect(persisted.rows[0]).toEqual({ jobs: 1, cursors: 1 });
  });

  it('SND-05 serializes 20 concurrent BULK starts into one stable expansion run', async () => {
    const alarmId = '24242424-2424-4242-8242-242424242424';
    await repository.create({
      id: alarmId,
      type: 'BULK',
      title: 'bulk concurrency',
      body: 'body',
      recipientIds: [],
    });

    let releaseBarrier: (() => void) | undefined;
    const barrier = new Promise<void>((resolveBarrier) => {
      releaseBarrier = resolveBarrier;
    });
    const starts = Array.from({ length: 20 }, async () => {
      await barrier;
      return repository.start(alarmId);
    });

    releaseBarrier?.();
    const results = await Promise.all(starts);

    expect(results).toHaveLength(20);
    expect(results.every(({ id, status }) => id === alarmId && status === 'QUEUED')).toBe(true);

    const persisted = await pool.query<{
      status: string;
      version: string;
      jobs: number;
      distinct_runs: number;
      cursor_sentinels: number;
      staged_recipients: number;
      deliveries: number;
    }>(
      `
        SELECT
          a.status,
          a.version::text,
          (SELECT count(*)::integer FROM alarm_expansion_jobs WHERE alarm_id = a.id) AS jobs,
          (
            SELECT count(DISTINCT run_generation)::integer
            FROM alarm_expansion_jobs
            WHERE alarm_id = a.id
          ) AS distinct_runs,
          (
            SELECT count(*)::integer
            FROM alarm_expansion_seen_cursors
            WHERE alarm_id = a.id
          ) AS cursor_sentinels,
          (
            SELECT count(*)::integer
            FROM alarm_expansion_recipients
            WHERE alarm_id = a.id
          ) AS staged_recipients,
          (SELECT count(*)::integer FROM deliveries WHERE alarm_id = a.id) AS deliveries
        FROM alarms a
        WHERE a.id = $1
      `,
      [alarmId],
    );

    expect(persisted.rows[0]).toEqual({
      status: 'QUEUED',
      version: '1',
      jobs: 1,
      distinct_runs: 1,
      cursor_sentinels: 1,
      staged_recipients: 0,
      deliveries: 0,
    });
  });

  it('DB-11 uses READ COMMITTED with explicit lock order and a losing generation CAS', async () => {
    const alarmId = '25252525-2525-4252-8252-252525252525';
    await repository.create({
      id: alarmId,
      type: 'EMERGENCY',
      title: 'isolation fixture',
      body: 'body',
      recipientIds: ['db-11-user'],
    });
    await repository.start(alarmId);

    const fixture = await pool.query<{
      delivery_id: string;
      alarm_version: string;
      lease_generation: number;
    }>(
      `
        SELECT
          d.id AS delivery_id,
          a.version::text AS alarm_version,
          d.lease_generation
        FROM alarms a
        JOIN deliveries d ON d.alarm_id = a.id
        WHERE a.id = $1
      `,
      [alarmId],
    );
    const row = fixture.rows[0];
    if (row === undefined) {
      throw new Error('Expected DB-11 delivery fixture');
    }

    const winner = await pool.connect();
    const contender = await pool.connect();
    let contenderWork: Promise<number> | undefined;

    try {
      await winner.query('BEGIN');
      await contender.query('BEGIN');
      const [winnerIsolation, contenderIsolation] = await Promise.all([
        winner.query<{ transaction_isolation: string }>('SHOW transaction_isolation'),
        contender.query<{ transaction_isolation: string }>('SHOW transaction_isolation'),
      ]);
      expect(winnerIsolation.rows[0]?.transaction_isolation).toBe('read committed');
      expect(contenderIsolation.rows[0]?.transaction_isolation).toBe('read committed');

      await winner.query('SELECT id FROM alarms WHERE id = $1 FOR UPDATE', [alarmId]);
      await winner.query('SELECT id FROM deliveries WHERE id = $1 FOR UPDATE', [row.delivery_id]);

      const contenderPid = await contender.query<{ pid: number }>('SELECT pg_backend_pid() AS pid');
      const pid = contenderPid.rows[0]?.pid;
      if (pid === undefined) {
        throw new Error('Expected contender backend pid');
      }
      contenderWork = (async () => {
        await contender.query('SELECT id FROM alarms WHERE id = $1 FOR UPDATE', [alarmId]);
        await contender.query('SELECT id FROM deliveries WHERE id = $1 FOR UPDATE', [
          row.delivery_id,
        ]);
        const lost = await contender.query(
          `
            UPDATE deliveries
            SET lease_generation = lease_generation + 1
            WHERE id = $1
              AND state = 'READY'
              AND lease_generation = $2
          `,
          [row.delivery_id, row.lease_generation],
        );
        await contender.query('COMMIT');
        return lost.rowCount ?? 0;
      })();

      let observedBlockedLock = false;
      for (let attempt = 0; attempt < 100; attempt += 1) {
        const activity = await pool.query<{ blocked: boolean }>(
          `
            SELECT wait_event_type = 'Lock' AS blocked
            FROM pg_stat_activity
            WHERE pid = $1
          `,
          [pid],
        );
        if (activity.rows[0]?.blocked === true) {
          observedBlockedLock = true;
          break;
        }
        await new Promise<void>((resolveDelay) => setTimeout(resolveDelay, 10));
      }
      expect(observedBlockedLock).toBe(true);

      const winnerDelivery = await winner.query(
        `
          UPDATE deliveries
          SET
            state = 'CLAIMED',
            lease_generation = lease_generation + 1,
            lease_token = $3,
            lease_until = clock_timestamp() + interval '1 minute'
          WHERE id = $1
            AND state = 'READY'
            AND lease_generation = $2
        `,
        [row.delivery_id, row.lease_generation, '26262626-2626-4262-8262-262626262626'],
      );
      const winnerAlarm = await winner.query(
        `
          UPDATE alarms
          SET version = version + 1
          WHERE id = $1 AND version = $2
        `,
        [alarmId, row.alarm_version],
      );
      expect(winnerDelivery.rowCount).toBe(1);
      expect(winnerAlarm.rowCount).toBe(1);
      await winner.query('COMMIT');

      await expect(contenderWork).resolves.toBe(0);

      const final = await pool.query<{
        state: string;
        lease_generation: number;
        alarm_version: string;
      }>(
        `
          SELECT
            d.state,
            d.lease_generation,
            a.version::text AS alarm_version
          FROM deliveries d
          JOIN alarms a ON a.id = d.alarm_id
          WHERE d.id = $1
        `,
        [row.delivery_id],
      );
      expect(final.rows[0]).toEqual({
        state: 'CLAIMED',
        lease_generation: row.lease_generation + 1,
        alarm_version: String(Number(row.alarm_version) + 1),
      });
    } finally {
      await winner.query('ROLLBACK').catch(() => undefined);
      if (contenderWork !== undefined) {
        await contenderWork.catch(() => undefined);
      }
      await contender.query('ROLLBACK').catch(() => undefined);
      winner.release();
      contender.release();
    }
  });

  it('DB-11 fences concurrent production startAttempt calls on fresh connections', async () => {
    const alarmId = '27272727-2727-4272-8272-272727272727';
    await repository.create({
      id: alarmId,
      type: 'EMERGENCY',
      title: 'production isolation race',
      body: 'body',
      recipientIds: ['db-11-production-user'],
    });
    await repository.start(alarmId);

    const dispatch = new PostgresDispatchRepository(pool);
    const claim = await dispatch.claimNext(0, 60_000);
    if (claim === null) {
      throw new Error('Expected DB-11 production claim');
    }

    const advisoryKey = 727_272;
    const blocker = await pool.connect();
    const winnerPool = new Pool({
      connectionString: testDatabase.databaseUrl,
      application_name: 'db11-production-winner',
      max: 1,
    });
    const contenderPool = new Pool({
      connectionString: testDatabase.databaseUrl,
      application_name: 'db11-production-contender',
      max: 1,
    });
    const winner = new PostgresDispatchRepository(winnerPool);
    const contender = new PostgresDispatchRepository(contenderPool);
    let winnerWork: ReturnType<PostgresDispatchRepository['startAttempt']> | undefined;
    let contenderWork: ReturnType<PostgresDispatchRepository['startAttempt']> | undefined;

    await blocker.query('SELECT pg_advisory_lock($1)', [advisoryKey]);
    await pool.query(`
      CREATE FUNCTION block_production_attempt_before_commit() RETURNS trigger
      LANGUAGE plpgsql AS $$
      BEGIN
        PERFORM pg_advisory_xact_lock(${advisoryKey});
        RETURN NEW;
      END
      $$
    `);
    await pool.query(`
      CREATE TRIGGER block_production_attempt_before_commit_trigger
      AFTER INSERT ON delivery_attempts
      FOR EACH ROW EXECUTE FUNCTION block_production_attempt_before_commit()
    `);

    try {
      const isolations = await Promise.all([
        winnerPool.query<{ transaction_isolation: string }>('SHOW transaction_isolation'),
        contenderPool.query<{ transaction_isolation: string }>('SHOW transaction_isolation'),
      ]);
      expect(isolations.map(({ rows }) => rows[0]?.transaction_isolation)).toEqual([
        'read committed',
        'read committed',
      ]);

      winnerWork = winner.startAttempt(claim, {
        reservationId: '28282828-2828-4282-8282-282828282828',
        scheduledMonotonicMs: 0,
      });
      let winnerPid: number | undefined;
      for (let iteration = 0; iteration < 200; iteration += 1) {
        const activity = await pool.query<{ pid: number }>(
          `
            SELECT pid
            FROM pg_stat_activity
            WHERE application_name = 'db11-production-winner'
              AND wait_event_type = 'Lock'
          `,
        );
        winnerPid = activity.rows[0]?.pid;
        if (winnerPid !== undefined) {
          break;
        }
        await new Promise<void>((resolveDelay) => setTimeout(resolveDelay, 5));
      }
      if (winnerPid === undefined) {
        throw new Error('Production winner did not reach the pre-commit barrier');
      }

      contenderWork = contender.startAttempt(claim, {
        reservationId: '29292929-2929-4292-8292-292929292929',
        scheduledMonotonicMs: 0,
      });
      let contenderBlocked = false;
      for (let iteration = 0; iteration < 200; iteration += 1) {
        const activity = await pool.query<{ blocked: boolean }>(
          `
            SELECT wait_event_type = 'Lock' AS blocked
            FROM pg_stat_activity
            WHERE application_name = 'db11-production-contender'
          `,
        );
        if (activity.rows[0]?.blocked === true) {
          contenderBlocked = true;
          break;
        }
        await new Promise<void>((resolveDelay) => setTimeout(resolveDelay, 5));
      }
      expect(contenderBlocked).toBe(true);

      await blocker.query('SELECT pg_advisory_unlock($1)', [advisoryKey]);
      const [winnerAttempt, contenderAttempt] = await Promise.all([winnerWork, contenderWork]);
      expect(winnerAttempt).not.toBeNull();
      expect(contenderAttempt).toBeNull();

      const stored = await pool.query<{
        attempt_count: number;
        attempt_no: number;
        delivery_state: string;
        lease_generation: number;
        post_budget_count: number;
        request_phase: string;
      }>(
        `
          SELECT
            d.state AS delivery_state,
            d.attempt_no,
            d.post_budget_count,
            d.lease_generation,
            count(da.id)::integer AS attempt_count,
            min(da.request_phase) AS request_phase
          FROM deliveries d
          LEFT JOIN delivery_attempts da ON da.delivery_id = d.id
          WHERE d.id = $1
          GROUP BY d.id
        `,
        [claim.deliveryId],
      );
      expect(stored.rows[0]).toEqual({
        delivery_state: 'IN_FLIGHT',
        attempt_no: 1,
        post_budget_count: 1,
        lease_generation: claim.leaseGeneration,
        attempt_count: 1,
        request_phase: 'PREPARED',
      });
    } finally {
      await blocker.query('SELECT pg_advisory_unlock($1)', [advisoryKey]).catch(() => undefined);
      blocker.release();
      if (winnerWork !== undefined) {
        await winnerWork.catch(() => undefined);
      }
      if (contenderWork !== undefined) {
        await contenderWork.catch(() => undefined);
      }
      await winnerPool.end();
      await contenderPool.end();
      await pool.query(
        'DROP TRIGGER IF EXISTS block_production_attempt_before_commit_trigger ON delivery_attempts',
      );
      await pool.query('DROP FUNCTION IF EXISTS block_production_attempt_before_commit()');
    }
  });

  it('linearizes a CREATED start/cancel race into one legal path', async () => {
    const alarmId = '33333333-3333-4333-8333-333333333333';
    await repository.create({
      id: alarmId,
      type: 'BULK',
      title: 'bulk',
      body: 'body',
      recipientIds: [],
    });

    const outcomes = await Promise.allSettled([
      repository.start(alarmId),
      repository.cancel(alarmId),
    ]);
    const persisted = await repository.findById(alarmId);
    const jobs = await pool.query<{ count: string }>(
      'SELECT count(*) FROM alarm_expansion_jobs WHERE alarm_id = $1',
      [alarmId],
    );

    expect(['CANCELED', 'CANCEL_REQUESTED']).toContain(persisted?.status);
    expect(['0', '1']).toContain(jobs.rows[0]?.count);
    expect(outcomes.filter(({ status }) => status === 'rejected')).toHaveLength(
      persisted?.status === 'CANCELED' ? 1 : 0,
    );
    for (const outcome of outcomes) {
      if (outcome.status === 'rejected') {
        expect(outcome.reason).toBeInstanceOf(AlarmStateConflictError);
      }
    }
  });

  it('preserves the exact database timestamp key across same-millisecond pages', async () => {
    const fixtures = [
      ['44444444-4444-4444-8444-444444444441', '2026-10-15T00:00:00.123900Z'],
      ['44444444-4444-4444-8444-444444444442', '2026-10-15T00:00:00.123500Z'],
      ['44444444-4444-4444-8444-444444444443', '2026-10-15T00:00:00.123100Z'],
    ] as const;
    for (const [id, createdAt] of fixtures) {
      await repository.create({
        id,
        type: 'BULK',
        title: id,
        body: 'body',
        recipientIds: [],
      });
      await pool.query('UPDATE alarms SET created_at = $2::timestamptz WHERE id = $1', [
        id,
        createdAt,
      ]);
    }

    const seen: string[] = [];
    let cursor = null;
    for (;;) {
      const page = await repository.findPage({ limit: 1, cursor, status: null, type: null });
      seen.push(...page.alarms.map(({ id }) => id));
      expect(page.alarms[0]?.createdAt).toBe('2026-10-15T00:00:00.123Z');
      if (!page.hasMore) {
        expect(page.nextCursorPosition).toBeNull();
        break;
      }
      expect(page.nextCursorPosition?.createdAt).toMatch(/\.123[159]00Z$/);
      cursor = page.nextCursorPosition;
    }

    expect(seen).toEqual(fixtures.map(([id]) => id));
    expect(new Set(seen).size).toBe(fixtures.length);
  });
});
