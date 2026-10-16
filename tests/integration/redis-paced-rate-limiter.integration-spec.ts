import { randomUUID } from 'node:crypto';
import { createConnection, createServer, type Socket } from 'node:net';
import { resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';

import { Pool } from 'pg';
import { createClient } from 'redis';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

import {
  RateLimiterUnavailableError,
  RedisPacedRateLimiter,
  type RedisPacedRateLimiterOptions,
} from '../../packages/alarms/src/infrastructure/limiter/redis-paced-rate-limiter';
import { runMigrations } from '../../packages/alarms/src/infrastructure/persistence/migrations';
import { createTestDatabase, type TestDatabase } from './postgres-test-schema';

const LIMITER_KEY_PREFIX = `usall:test:${randomUUID()}`;
const LIMITER_KEY = `${LIMITER_KEY_PREFIX}:limiter:{default-provider}`;
const LIMITER_AUDIT_KEY = `${LIMITER_KEY_PREFIX}:limiter:audit:{default-provider}`;
const UUID_V4_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const COLD_LOCK_NAMESPACE = 811_772;
const COLD_LOCK_KEY = 1;

const databaseUrl = process.env.TEST_DATABASE_URL;
const redisUrl = process.env.TEST_REDIS_URL;
const describeWithDependencies =
  databaseUrl === undefined || redisUrl === undefined ? describe.skip : describe;

interface StartedReservation {
  readonly reservationId: string;
  readonly scheduledMonotonicMs: number;
  readonly startedMonotonicMs: number;
  readonly startedEpochMs: number;
}

function maximumRollingCount(timestamps: readonly number[], windowMs: number): number {
  const ordered = [...timestamps].sort((left, right) => left - right);
  let left = 0;
  let maximum = 0;
  for (let right = 0; right < ordered.length; right += 1) {
    while ((ordered[right] ?? 0) - (ordered[left] ?? 0) >= windowMs) {
      left += 1;
    }
    maximum = Math.max(maximum, right - left + 1);
  }
  return maximum;
}

function redisRunId(info: string): string {
  const match = /^run_id:([^\r\n]+)$/m.exec(info);
  if (match?.[1] === undefined) {
    throw new Error('Redis INFO server did not contain run_id');
  }
  return match[1];
}

describeWithDependencies('RedisPacedRateLimiter', () => {
  let testDatabase: TestDatabase;
  let pool: Pool;
  let redis: ReturnType<typeof createClient>;
  let openLimiters: RedisPacedRateLimiter[] = [];

  beforeAll(async () => {
    if (databaseUrl === undefined || redisUrl === undefined) {
      throw new Error('TEST_DATABASE_URL and TEST_REDIS_URL are required');
    }
    testDatabase = await createTestDatabase(databaseUrl);
    await runMigrations({
      databaseUrl: testDatabase.databaseUrl,
      migrationsDirectory: resolve(process.cwd(), 'migrations'),
    });
    pool = new Pool({ connectionString: testDatabase.databaseUrl });
    redis = createClient({ url: redisUrl });
    redis.on('error', () => undefined);
    await redis.connect();
  });

  beforeEach(async () => {
    await redis.del(LIMITER_KEY);
    await redis.del(LIMITER_AUDIT_KEY);
    await pool.query(`
      UPDATE dispatch_control
      SET resume_not_before = '-infinity'::timestamptz, version = 0
      WHERE control_key = 'default-provider'
    `);
  });

  afterEach(async () => {
    const limiters = openLimiters;
    openLimiters = [];
    await Promise.all(limiters.map((limiter) => limiter.close()));
  });

  afterAll(async () => {
    if (redis?.isOpen) {
      await redis.close();
    }
    await pool?.end();
    await testDatabase?.cleanup();
  });

  function limiter(
    options: Omit<RedisPacedRateLimiterOptions, 'redisUrl' | 'pool'> = {},
  ): RedisPacedRateLimiter {
    if (redisUrl === undefined) {
      throw new Error('TEST_REDIS_URL is required');
    }
    const created = new RedisPacedRateLimiter({
      redisUrl,
      pool,
      keyPrefix: LIMITER_KEY_PREFIX,
      ...options,
    });
    openLimiters.push(created);
    return created;
  }

  async function reserveAndStart(
    target: RedisPacedRateLimiter,
    timeoutMs = 5_000,
    retryDelayMs = 2,
  ): Promise<StartedReservation> {
    const deadline = performance.now() + timeoutMs;
    while (performance.now() < deadline) {
      const permit = await target.reserve();
      if (permit === null) {
        await delay(retryDelayMs);
        continue;
      }

      while (performance.now() < permit.scheduledMonotonicMs) {
        await delay(Math.max(1, permit.scheduledMonotonicMs - performance.now()));
      }
      return {
        reservationId: permit.reservationId,
        scheduledMonotonicMs: permit.scheduledMonotonicMs,
        startedMonotonicMs: performance.now(),
        startedEpochMs: Date.now(),
      };
    }
    throw new Error('Timed out waiting for a paced reservation');
  }

  it('[RL-05] shares exact paced slots and a rolling 50/s budget across three clients', async () => {
    const clients = Array.from({ length: 3 }, () =>
      limiter({ intervalMs: 25, leadMs: 50, horizonMs: 100, coldBarrierMs: 0 }),
    );
    await expect(clients[0]?.reserve()).resolves.toBeNull();

    const startsByClient = await Promise.all(
      clients.map(async (client) => {
        const starts: StartedReservation[] = [];
        for (let index = 0; index < 20; index += 1) {
          starts.push(await reserveAndStart(client));
        }
        return starts;
      }),
    );
    const starts = startsByClient.flat();

    expect(startsByClient.map((entries) => entries.length)).toEqual([20, 20, 20]);
    expect(starts).toHaveLength(60);
    expect(new Set(starts.map(({ reservationId }) => reservationId)).size).toBe(60);
    expect(starts.every(({ reservationId }) => UUID_V4_PATTERN.test(reservationId))).toBe(true);
    expect(
      starts.every(
        ({ scheduledMonotonicMs, startedMonotonicMs }) =>
          startedMonotonicMs + 0.5 >= scheduledMonotonicMs,
      ),
    ).toBe(true);

    const audit = await redis.zRangeWithScores(LIMITER_AUDIT_KEY, 0, -1);
    const scheduledByReservation = new Map(
      audit.map(({ value, score }) => [value, score] as const),
    );
    const exactRedisSlots = starts
      .map(({ reservationId }) => scheduledByReservation.get(reservationId))
      .filter((score): score is number => score !== undefined)
      .sort((left, right) => left - right);
    expect(exactRedisSlots).toHaveLength(60);
    for (let index = 1; index < exactRedisSlots.length; index += 1) {
      expect(
        (exactRedisSlots[index] ?? 0) - (exactRedisSlots[index - 1] ?? 0),
      ).toBeGreaterThanOrEqual(25);
    }

    const actualStarts = starts
      .map(({ startedMonotonicMs }) => startedMonotonicMs)
      .sort((left, right) => left - right);
    for (let index = 0; index + 50 < actualStarts.length; index += 1) {
      expect((actualStarts[index + 50] ?? 0) - (actualStarts[index] ?? 0)).toBeGreaterThanOrEqual(
        1_000,
      );
    }

    const currentRunId = redisRunId(await redis.info('server'));
    await redis.hSet(LIMITER_KEY, {
      run_id: currentRunId,
      next_slot_ms: String(Date.now() + 10_000),
    });
    const auditSizeBeforeDenial = await redis.zCard(LIMITER_AUDIT_KEY);
    await expect(Promise.all(clients.map((client) => client.reserve()))).resolves.toEqual([
      null,
      null,
      null,
    ]);
    await expect(redis.zCard(LIMITER_AUDIT_KEY)).resolves.toBe(auditSizeBeforeDenial);
  });

  it('[RL-06] keeps an adversarial second-boundary release within every rolling 1 second budget', async () => {
    const clients = Array.from({ length: 5 }, () =>
      limiter({ intervalMs: 25, leadMs: 0, horizonMs: 100, coldBarrierMs: 0 }),
    );
    await expect(clients[0]?.reserve()).resolves.toBeNull();

    let nextBoundaryMs = 0;
    let releasedAtMs = 0;
    for (let alignmentAttempt = 0; alignmentAttempt < 3; alignmentAttempt += 1) {
      nextBoundaryMs = Math.ceil((Date.now() + 1_000) / 1_000) * 1_000;
      // Give the event loop a material wake-up margin while keeping the Redis schedule itself
      // adversarial: its 25 ms slots must cross the exact wall-clock second boundary.
      await redis.hSet(LIMITER_KEY, 'next_slot_ms', String(nextBoundaryMs - 500));
      await delay(Math.max(0, nextBoundaryMs - 600 - Date.now()));
      releasedAtMs = Date.now();
      if (releasedAtMs < nextBoundaryMs && nextBoundaryMs - releasedAtMs <= 650) break;
    }

    const starts = await Promise.all(
      Array.from({ length: 100 }, (_unused, index) => {
        const client = clients[index % clients.length];
        if (client === undefined) {
          throw new Error('Expected an RL-06 limiter client');
        }
        return reserveAndStart(client, 8_000, 10);
      }),
    );
    const ingressTimestamps = starts.map(({ startedEpochMs }) => startedEpochMs);
    const fixedWindowCounts = new Map<number, number>();
    for (const timestamp of ingressTimestamps) {
      const second = Math.floor(timestamp / 1_000);
      fixedWindowCounts.set(second, (fixedWindowCounts.get(second) ?? 0) + 1);
    }

    expect(releasedAtMs).toBeLessThan(nextBoundaryMs);
    expect(nextBoundaryMs - releasedAtMs).toBeLessThanOrEqual(650);
    expect(starts).toHaveLength(100);
    expect(Math.max(...ingressTimestamps)).toBeGreaterThanOrEqual(nextBoundaryMs);
    expect(maximumRollingCount(ingressTimestamps, 1_000)).toBeLessThanOrEqual(50);
    expect(Math.max(...fixedWindowCounts.values())).toBeLessThanOrEqual(50);

    const audit = await redis.zRangeWithScores(LIMITER_AUDIT_KEY, 0, -1);
    expect(audit).toHaveLength(100);
    const scheduledTimestamps = audit.map(({ score }) => score);
    expect(Math.min(...scheduledTimestamps)).toBeLessThan(nextBoundaryMs);
    expect(Math.max(...scheduledTimestamps)).toBeGreaterThanOrEqual(nextBoundaryMs);
    expect(maximumRollingCount(scheduledTimestamps, 1_000)).toBeLessThanOrEqual(50);
  });

  it('[RL-09] remains paced when callers have -10/0/+10 second wall-clock skew', async () => {
    const offsetsMs = [-10_000, 0, 10_000] as const;
    const clients = offsetsMs.map(() =>
      limiter({ intervalMs: 25, leadMs: 5, horizonMs: 100, coldBarrierMs: 0 }),
    );
    await expect(clients[0]?.reserve()).resolves.toBeNull();

    const actualDateNow = Date.now.bind(Date);
    const dateNow = vi.spyOn(Date, 'now');
    const starts: StartedReservation[] = [];
    try {
      for (let index = 0; index < 60; index += 1) {
        const clientIndex = index % clients.length;
        const client = clients[clientIndex];
        const offsetMs = offsetsMs[clientIndex];
        if (client === undefined || offsetMs === undefined) {
          throw new Error('Expected an RL-09 limiter client and clock offset');
        }
        dateNow.mockImplementation(() => actualDateNow() + offsetMs);
        starts.push(await reserveAndStart(client));
      }
    } finally {
      dateNow.mockRestore();
    }

    expect(
      maximumRollingCount(
        starts.map(({ startedMonotonicMs }) => startedMonotonicMs),
        1_000,
      ),
    ).toBeLessThanOrEqual(50);
    const audit = await redis.zRangeWithScores(LIMITER_AUDIT_KEY, 0, -1);
    expect(audit).toHaveLength(60);
    const scheduledSlots = audit.map(({ score }) => score).sort((left, right) => left - right);
    for (let index = 1; index < scheduledSlots.length; index += 1) {
      expect(
        (scheduledSlots[index] ?? 0) - (scheduledSlots[index - 1] ?? 0),
      ).toBeGreaterThanOrEqual(25);
    }
    expect(maximumRollingCount(scheduledSlots, 1_000)).toBeLessThanOrEqual(50);
  });

  it('[RED-02] flushes into a durable cold barrier and does not resume ahead of it', async () => {
    const warm = limiter({ intervalMs: 25, leadMs: 5, horizonMs: 100, coldBarrierMs: 0 });
    await expect(warm.reserve()).resolves.toBeNull();
    const beforeFlush = [
      await reserveAndStart(warm),
      await reserveAndStart(warm),
      await reserveAndStart(warm),
    ];
    const versionBeforeFlush = await pool.query<{ version: string }>(`
      SELECT version::text
      FROM dispatch_control
      WHERE control_key = 'default-provider'
    `);

    await redis.del(LIMITER_KEY);
    await redis.del(LIMITER_AUDIT_KEY);
    const recovery = limiter({ intervalMs: 25, leadMs: 5, horizonMs: 100, coldBarrierMs: 1_200 });
    await expect(recovery.reserve()).resolves.toBeNull();
    await expect(recovery.reserve()).resolves.toBeNull();

    const barrier = await pool.query<{
      resume_not_before: Date;
      remaining_ms: number;
      version: string;
    }>(`
      SELECT
        resume_not_before,
        (EXTRACT(EPOCH FROM (resume_not_before - clock_timestamp())) * 1000)::double precision
          AS remaining_ms,
        version::text
      FROM dispatch_control
      WHERE control_key = 'default-provider'
    `);
    const state = await redis.hGetAll(LIMITER_KEY);
    expect(barrier.rows[0]?.remaining_ms ?? 0).toBeGreaterThan(1_000);
    expect(Number(barrier.rows[0]?.version)).toBe(Number(versionBeforeFlush.rows[0]?.version) + 1);
    expect(Number(state.next_slot_ms)).toBeGreaterThanOrEqual(
      barrier.rows[0]?.resume_not_before.getTime() ?? Number.POSITIVE_INFINITY,
    );

    const afterFlush = await reserveAndStart(recovery, 3_000);
    const lastBeforeFlush = Math.max(
      ...beforeFlush.map(({ startedMonotonicMs }) => startedMonotonicMs),
    );
    expect(afterFlush.startedMonotonicMs - lastBeforeFlush).toBeGreaterThanOrEqual(1_000);
    const observedBarrier = await pool.query<{ open: boolean; remaining_ms: number }>(`
      SELECT
        resume_not_before <= clock_timestamp() AS open,
        GREATEST(
          0,
          (EXTRACT(EPOCH FROM (resume_not_before - clock_timestamp())) * 1000)::double precision
        ) AS remaining_ms
      FROM dispatch_control
      WHERE control_key = 'default-provider'
    `);
    const remainingMs = observedBarrier.rows[0]?.remaining_ms ?? Number.POSITIVE_INFINITY;
    // Redis TIME is translated to a local monotonic deadline, while the durable fence uses
    // PostgreSQL clock_timestamp(). Under a heavily parallel test run the timer can wake a few
    // milliseconds before a second DB observation crosses the boundary. A material early resume
    // is still a failure; the production dispatch commit path independently rechecks this fence.
    expect(remainingMs).toBeLessThanOrEqual(50);
    if (observedBarrier.rows[0]?.open !== true) {
      await delay(Math.ceil(remainingMs) + 10);
    }
    const openAfterBoundary = await pool.query<{ open: boolean }>(`
      SELECT resume_not_before <= clock_timestamp() AS open
      FROM dispatch_control
      WHERE control_key = 'default-provider'
    `);
    expect(openAfterBoundary.rows[0]?.open).toBe(true);
  });

  it('[RED-03] does not refund late unused slots or burst when work resumes after a pause', async () => {
    const target = limiter({ intervalMs: 25, leadMs: 5, horizonMs: 100, coldBarrierMs: 0 });
    await expect(target.reserve()).resolves.toBeNull();

    const unusedPermits = await Promise.all([target.reserve(), target.reserve(), target.reserve()]);
    expect(unusedPermits.every((permit) => permit !== null)).toBe(true);
    const latestUnusedSlot = Math.max(
      ...unusedPermits.map((permit) => permit?.scheduledMonotonicMs ?? Number.NEGATIVE_INFINITY),
    );
    await delay(Math.max(0, latestUnusedSlot + 25 - performance.now()));

    const resumedStarts: StartedReservation[] = [];
    for (let index = 0; index < 20; index += 1) {
      resumedStarts.push(await reserveAndStart(target));
    }

    expect(
      Math.min(...resumedStarts.map(({ scheduledMonotonicMs }) => scheduledMonotonicMs)),
    ).toBeGreaterThanOrEqual(latestUnusedSlot + 24);
    expect(
      maximumRollingCount(
        resumedStarts.map(({ startedMonotonicMs }) => startedMonotonicMs),
        1_000,
      ),
    ).toBeLessThanOrEqual(50);
    await expect(redis.zCard(LIMITER_AUDIT_KEY)).resolves.toBe(23);
  });

  it('[RED-01] aborts an in-progress Redis connect without leaving shutdown work pending', async () => {
    const blackhole = createServer();
    const sockets = new Set<Socket>();
    let resolveConnection: (() => void) | undefined;
    const connectionAccepted = new Promise<void>((resolve) => {
      resolveConnection = resolve;
    });
    blackhole.on('connection', (socket) => {
      sockets.add(socket);
      socket.once('close', () => sockets.delete(socket));
      resolveConnection?.();
    });
    await new Promise<void>((resolveListen, rejectListen) => {
      blackhole.once('error', rejectListen);
      blackhole.listen(0, '127.0.0.1', resolveListen);
    });
    const address = blackhole.address();
    if (address === null || typeof address === 'string') {
      throw new Error('Expected a TCP address for the Redis blackhole');
    }
    const target = new RedisPacedRateLimiter({
      redisUrl: `redis://127.0.0.1:${address.port}`,
      pool,
      keyPrefix: `${LIMITER_KEY_PREFIX}:blackhole`,
    });
    openLimiters.push(target);
    const shutdown = new AbortController();

    try {
      const pending = target.reserve(shutdown.signal);
      await Promise.race([
        connectionAccepted,
        delay(1_000).then(() => {
          throw new Error('Redis blackhole did not accept the connection');
        }),
      ]);
      const abortedAt = performance.now();
      shutdown.abort();
      await expect(
        Promise.race([pending, delay(1_000).then(() => 'abort-timeout' as const)]),
      ).resolves.toBeNull();
      expect(performance.now() - abortedAt).toBeLessThan(500);
    } finally {
      const serverClosed = new Promise<void>((resolveClose) =>
        blackhole.close(() => resolveClose()),
      );
      for (const socket of sockets) socket.destroy();
      await serverClosed;
      await target.close();
    }
  });

  it.each([
    ['missing run_id', 'missing-run-id'],
    ['missing next_slot_ms', 'missing-next-slot'],
    ['mismatched run_id', 'mismatched-run-id'],
    ['malformed next_slot_ms', 'malformed-next-slot'],
  ] as const)(
    '[RED-07] treats %s as COLD and repairs it behind the durable barrier',
    async (_label, corruption) => {
      const currentRunId = redisRunId(await redis.info('server'));
      await redis.hSet(LIMITER_KEY, {
        run_id: currentRunId,
        next_slot_ms: String(Date.now()),
      });
      if (corruption === 'missing-run-id') {
        await redis.hDel(LIMITER_KEY, 'run_id');
      } else if (corruption === 'missing-next-slot') {
        await redis.hDel(LIMITER_KEY, 'next_slot_ms');
      } else if (corruption === 'mismatched-run-id') {
        await redis.hSet(LIMITER_KEY, 'run_id', 'stale-redis-run-id');
      } else {
        await redis.hSet(LIMITER_KEY, 'next_slot_ms', 'not-a-timestamp');
      }

      const recovery = limiter({
        intervalMs: 25,
        leadMs: 5,
        horizonMs: 100,
        coldBarrierMs: 1_200,
      });
      await expect(recovery.reserve()).resolves.toBeNull();

      const repaired = await redis.hGetAll(LIMITER_KEY);
      const barrier = await pool.query<{
        barrier_span_ms: number;
        resume_not_before: Date;
        version: string;
      }>(`
        SELECT
          resume_not_before,
          (EXTRACT(EPOCH FROM (resume_not_before - updated_at)) * 1000)::double precision
            AS barrier_span_ms,
          version::text
        FROM dispatch_control
        WHERE control_key = 'default-provider'
      `);
      expect(repaired.run_id).toBe(currentRunId);
      expect(Number.isSafeInteger(Number(repaired.next_slot_ms))).toBe(true);
      expect(barrier.rows[0]?.barrier_span_ms ?? 0).toBeGreaterThanOrEqual(1_190);
      expect(barrier.rows[0]?.version).toBe('1');
      expect(Number(repaired.next_slot_ms) + 50).toBeGreaterThanOrEqual(
        barrier.rows[0]?.resume_not_before.getTime() ?? Number.POSITIVE_INFINITY,
      );

      // Under parallel host load the observer may resume after the 1.2 second fence has opened.
      // A permit is then valid, but its scheduled instant must still remain behind the durable
      // recovery boundary.
      const reservationAfterRepair = await recovery.reserve();
      if (reservationAfterRepair !== null) {
        const scheduledEpochMs =
          Date.now() + (reservationAfterRepair.scheduledMonotonicMs - performance.now());
        expect(scheduledEpochMs + 50).toBeGreaterThanOrEqual(
          barrier.rows[0]?.resume_not_before.getTime() ?? Number.POSITIVE_INFINITY,
        );
      }
      await expect(redis.zCard(LIMITER_AUDIT_KEY)).resolves.toBe(
        reservationAfterRepair === null ? 0 : 1,
      );
    },
  );

  it('[RED-07] never treats expiration of the diagnostic audit key as lost authority', async () => {
    const target = limiter({ intervalMs: 25, leadMs: 5, horizonMs: 100, coldBarrierMs: 0 });
    await expect(target.reserve()).resolves.toBeNull();
    const first = await target.reserve();
    expect(first).not.toBeNull();
    await expect(redis.zCard(LIMITER_AUDIT_KEY)).resolves.toBe(1);

    const versionBeforeAuditExpiry = await pool.query<{ version: string }>(`
      SELECT version::text
      FROM dispatch_control
      WHERE control_key = 'default-provider'
    `);
    await redis.del(LIMITER_AUDIT_KEY);
    const afterAuditExpiry = await target.reserve();
    expect(afterAuditExpiry).not.toBeNull();
    expect(afterAuditExpiry?.reservationId).not.toBe(first?.reservationId);
    await expect(redis.zCard(LIMITER_AUDIT_KEY)).resolves.toBe(1);
    const versionAfterAuditExpiry = await pool.query<{ version: string }>(`
      SELECT version::text
      FROM dispatch_control
      WHERE control_key = 'default-provider'
    `);
    expect(versionAfterAuditExpiry.rows[0]?.version).toBe(
      versionBeforeAuditExpiry.rows[0]?.version,
    );
  });

  it('[RED-07] fails closed when the durable dispatch barrier row is missing', async () => {
    await pool.query("DELETE FROM dispatch_control WHERE control_key = 'default-provider'");
    const target = limiter({ intervalMs: 25, leadMs: 5, horizonMs: 100, coldBarrierMs: 1_200 });

    try {
      await expect(target.reserve()).rejects.toBeInstanceOf(RateLimiterUnavailableError);
      await expect(redis.exists(LIMITER_KEY)).resolves.toBe(0);
      await expect(redis.zCard(LIMITER_AUDIT_KEY)).resolves.toBe(0);
    } finally {
      await pool.query(
        "INSERT INTO dispatch_control(control_key) VALUES ('default-provider') ON CONFLICT DO NOTHING",
      );
    }
  });

  it('[RED-04] recovers the advisory lock and re-extends the barrier after a leader dies', async () => {
    const crashedLeader = await pool.connect();
    let crashedLeaderReleased = false;
    try {
      await crashedLeader.query('SELECT pg_advisory_lock($1, $2)', [
        COLD_LOCK_NAMESPACE,
        COLD_LOCK_KEY,
      ]);
      await crashedLeader.query(`
        UPDATE dispatch_control
        SET
          resume_not_before = clock_timestamp() + interval '200 milliseconds',
          updated_at = clock_timestamp(),
          version = version + 1
        WHERE control_key = 'default-provider'
      `);

      const recovery = limiter({
        intervalMs: 25,
        leadMs: 5,
        horizonMs: 100,
        coldBarrierMs: 200,
      });
      const pendingRecovery = recovery.reserve();
      const stateWhileLeaderLives = await Promise.race([
        pendingRecovery.then(() => 'completed'),
        delay(100).then(() => 'blocked'),
      ]);
      expect(stateWhileLeaderLives).toBe('blocked');

      crashedLeader.release(true);
      crashedLeaderReleased = true;
      await expect(pendingRecovery).resolves.toBeNull();

      const recoveredState = await redis.hGetAll(LIMITER_KEY);
      expect(recoveredState.run_id).toBe(redisRunId(await redis.info('server')));
      expect(Number.isSafeInteger(Number(recoveredState.next_slot_ms))).toBe(true);
      const recoveredBarrier = await pool.query<{
        barrier_span_ms: number;
        resume_not_before: Date;
        version: string;
      }>(`
        SELECT
          resume_not_before,
          (EXTRACT(EPOCH FROM (resume_not_before - updated_at)) * 1000)::double precision
            AS barrier_span_ms,
          version::text
        FROM dispatch_control
        WHERE control_key = 'default-provider'
      `);
      expect(recoveredBarrier.rows[0]?.version).toBe('2');
      expect(recoveredBarrier.rows[0]?.barrier_span_ms ?? 0).toBeGreaterThanOrEqual(190);

      // A saturated parallel run can pause this worker beyond the intentionally short 200 ms
      // barrier. In that case a permit is legal, so verify its scheduled instant rather than
      // requiring this observer to run while the barrier is still closed.
      const reservationAfterRecovery = await recovery.reserve();
      if (reservationAfterRecovery !== null) {
        const scheduledEpochMs =
          Date.now() + (reservationAfterRecovery.scheduledMonotonicMs - performance.now());
        expect(scheduledEpochMs + 50).toBeGreaterThanOrEqual(
          recoveredBarrier.rows[0]?.resume_not_before.getTime() ?? Number.POSITIVE_INFINITY,
        );
      }

      await reserveAndStart(recovery, 2_000);
      const open = await pool.query<{ open: boolean }>(`
        SELECT resume_not_before <= clock_timestamp() AS open
        FROM dispatch_control
        WHERE control_key = 'default-provider'
      `);
      expect(open.rows[0]?.open).toBe(true);
    } finally {
      if (!crashedLeaderReleased) {
        crashedLeader.release(true);
      }
    }
  });

  it('[RED-04] aborts a COLD follower waiting on the advisory lock', async () => {
    const leader = await pool.connect();
    try {
      await leader.query('SELECT pg_advisory_lock($1, $2)', [COLD_LOCK_NAMESPACE, COLD_LOCK_KEY]);
      const follower = limiter({
        intervalMs: 25,
        leadMs: 5,
        horizonMs: 100,
        coldBarrierMs: 1_200,
      });
      const shutdown = new AbortController();
      const pending = follower.reserve(shutdown.signal);

      await delay(100);
      const abortedAt = performance.now();
      shutdown.abort();
      await expect(pending).resolves.toBeNull();
      expect(performance.now() - abortedAt).toBeLessThan(500);
      await expect(redis.exists(LIMITER_KEY)).resolves.toBe(0);
      const barrier = await pool.query<{ version: string }>(
        "SELECT version::text FROM dispatch_control WHERE control_key = 'default-provider'",
      );
      expect(barrier.rows[0]?.version).toBe('0');
    } finally {
      await leader.query('SELECT pg_advisory_unlock($1, $2)', [COLD_LOCK_NAMESPACE, COLD_LOCK_KEY]);
      leader.release();
    }
  });

  it('[RED-04] bounds shutdown when an established PostgreSQL socket stops responding', async () => {
    const upstreamUrl = new URL(testDatabase.databaseUrl);
    const upstreamPort = Number(upstreamUrl.port || 5432);
    const proxy = createServer();
    const sockets = new Set<Socket>();
    let discardUpstreamResponses = false;
    let resolveBlackholedQuery: (() => void) | undefined;
    const blackholedQuery = new Promise<void>((resolveQuery) => {
      resolveBlackholedQuery = resolveQuery;
    });

    proxy.on('connection', (downstream) => {
      const upstream = createConnection({
        host: upstreamUrl.hostname,
        port: upstreamPort,
      });
      sockets.add(downstream);
      sockets.add(upstream);
      const forget = (socket: Socket): void => {
        sockets.delete(socket);
      };
      downstream.once('close', () => {
        forget(downstream);
        upstream.destroy();
      });
      upstream.once('close', () => {
        forget(upstream);
        downstream.destroy();
      });
      downstream.on('data', (chunk) => {
        if (discardUpstreamResponses) {
          resolveBlackholedQuery?.();
        }
        if (!upstream.destroyed) {
          upstream.write(chunk);
        }
      });
      upstream.on('data', (chunk) => {
        if (!discardUpstreamResponses && !downstream.destroyed) {
          downstream.write(chunk);
        }
      });
    });
    await new Promise<void>((resolveListen, rejectListen) => {
      proxy.once('error', rejectListen);
      proxy.listen(0, '127.0.0.1', resolveListen);
    });
    const address = proxy.address();
    if (address === null || typeof address === 'string') {
      throw new Error('Expected a TCP address for the PostgreSQL blackhole proxy');
    }

    const proxyUrl = new URL(testDatabase.databaseUrl);
    proxyUrl.hostname = '127.0.0.1';
    proxyUrl.port = String(address.port);
    const blackholePool = new Pool({
      connectionString: proxyUrl.toString(),
      max: 1,
      connectionTimeoutMillis: 500,
      query_timeout: 250,
      statement_timeout: 1_000,
    });
    const target = new RedisPacedRateLimiter({
      redisUrl: redisUrl ?? '',
      pool: blackholePool,
      keyPrefix: `${LIMITER_KEY_PREFIX}:postgres-blackhole`,
      coldBarrierMs: 1_200,
    });

    try {
      await blackholePool.query('SELECT 1');
      discardUpstreamResponses = true;
      const shutdown = new AbortController();
      const pending = target.reserve(shutdown.signal);
      await Promise.race([
        blackholedQuery,
        delay(1_000).then(() => {
          throw new Error('PostgreSQL blackhole did not observe the COLD-lock query');
        }),
      ]);

      const abortedAt = performance.now();
      shutdown.abort();
      await expect(
        Promise.race([pending, delay(1_000).then(() => 'abort-timeout' as const)]),
      ).resolves.toBeNull();
      expect(performance.now() - abortedAt).toBeLessThan(750);

      const poolClosed = blackholePool.end();
      await expect(
        Promise.race([poolClosed, delay(1_000).then(() => 'pool-close-timeout' as const)]),
      ).resolves.toBeUndefined();
    } finally {
      await target.close();
      await blackholePool.end().catch(() => undefined);
      const proxyClosed = new Promise<void>((resolveClose) => proxy.close(() => resolveClose()));
      for (const socket of sockets) socket.destroy();
      await proxyClosed;
    }
  });
});
