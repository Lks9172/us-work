import { randomUUID } from 'node:crypto';

import type { Pool, PoolClient } from 'pg';
import { createClient, type RedisClientType } from 'redis';

import { abortableDelay } from '@usall-backend-assignment/platform';

import type { PacedPermit, PacedRateLimiter } from '../../application/dispatch';
import { attachPostgresClientErrorHandler } from '../persistence/postgres-pool-observability';

const DEFAULT_KEY_PREFIX = 'usall';
const AUDIT_TTL_MS = 10 * 60 * 1_000;
const COLD_LOCK_NAMESPACE = 811_772;
const COLD_LOCK_KEY = 1;
const COLD_LOCK_RETRY_MS = 50;

const RESERVE_SCRIPT = `
local state = redis.call('HMGET', KEYS[1], 'run_id', 'next_slot_ms')
local next_slot_ms = tonumber(state[2])
if (not state[1]) or (not next_slot_ms) or next_slot_ms <= 0 or state[1] ~= ARGV[1] then
  return {'COLD'}
end

local now = redis.call('TIME')
local now_ms = now[1] * 1000 + math.floor(now[2] / 1000)
local scheduled_ms = math.max(next_slot_ms, now_ms + tonumber(ARGV[3]))
if scheduled_ms - now_ms > tonumber(ARGV[4]) then
  return {'DENIED', tostring(now_ms), tostring(scheduled_ms)}
end

redis.call(
  'HSET', KEYS[1],
  'next_slot_ms', tostring(scheduled_ms + tonumber(ARGV[2])),
  'last_reservation_id', ARGV[5]
)
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', tostring(now_ms - tonumber(ARGV[6])))
redis.call('ZADD', KEYS[2], tostring(scheduled_ms), ARGV[5])
redis.call('PEXPIRE', KEYS[2], ARGV[6])
return {'GRANTED', tostring(now_ms), tostring(scheduled_ms), ARGV[5]}
`;

const INITIALIZE_SCRIPT = `
local now = redis.call('TIME')
local now_ms = now[1] * 1000 + math.floor(now[2] / 1000)
redis.call(
  'HSET', KEYS[1],
  'run_id', ARGV[1],
  'next_slot_ms', tostring(now_ms + tonumber(ARGV[2]))
)
return tostring(now_ms)
`;

export class RateLimiterUnavailableError extends Error {
  public constructor(options?: ErrorOptions) {
    super('Shared rate limiter is unavailable', options);
    this.name = 'RateLimiterUnavailableError';
  }
}

export interface RedisPacedRateLimiterOptions {
  readonly redisUrl: string;
  readonly pool: Pool;
  readonly keyPrefix?: string;
  readonly intervalMs?: number;
  readonly leadMs?: number;
  readonly horizonMs?: number;
  readonly coldBarrierMs?: number;
}

function runId(info: string): string {
  const match = /^run_id:([^\r\n]+)$/m.exec(info);
  if (match?.[1] === undefined || match[1].length === 0) {
    throw new Error('Redis INFO server did not contain run_id');
  }
  return match[1];
}

function arrayReply(value: unknown): string[] {
  if (!Array.isArray(value) || !value.every((entry) => typeof entry === 'string')) {
    throw new Error('Redis limiter returned an invalid reply');
  }
  return value;
}

function hasCompleteState(state: Record<string, string>, expectedRunId: string): boolean {
  const nextSlotMs = Number(state.next_slot_ms);
  return state.run_id === expectedRunId && Number.isSafeInteger(nextSlotMs) && nextSlotMs > 0;
}

function isAborted(signal?: AbortSignal): boolean {
  return signal?.aborted ?? false;
}

export class RedisPacedRateLimiter implements PacedRateLimiter {
  private readonly client: RedisClientType;
  private readonly intervalMs: number;
  private readonly leadMs: number;
  private readonly horizonMs: number;
  private readonly coldBarrierMs: number;
  private readonly limiterKey: string;
  private readonly limiterAuditKey: string;
  private connectPromise: Promise<unknown> | null = null;

  public constructor(private readonly options: RedisPacedRateLimiterOptions) {
    this.client = createClient({ url: options.redisUrl });
    this.client.on('error', () => undefined);
    this.intervalMs = options.intervalMs ?? 25;
    this.leadMs = options.leadMs ?? 50;
    this.horizonMs = options.horizonMs ?? 100;
    this.coldBarrierMs = options.coldBarrierMs ?? 1_200;
    const keyPrefix = options.keyPrefix ?? DEFAULT_KEY_PREFIX;
    if (!/^[A-Za-z0-9:_-]{1,128}$/.test(keyPrefix)) {
      throw new Error(
        'Redis limiter keyPrefix must contain only ASCII letters, digits, :, _, or -',
      );
    }
    this.limiterKey = `${keyPrefix}:limiter:{default-provider}`;
    this.limiterAuditKey = `${keyPrefix}:limiter:audit:{default-provider}`;
  }

  public async reserve(signal?: AbortSignal): Promise<PacedPermit | null> {
    if (isAborted(signal)) {
      return null;
    }

    let abortDestroyTimer: ReturnType<typeof setInterval> | undefined;
    const destroyOnAbort = (): void => {
      const destroy = (): void => {
        try {
          // Shutdown owns this long-lived signal. Destroying the client rejects both an unfinished
          // handshake and any command that has already reached a connected but unavailable socket.
          this.client.destroy();
        } catch {
          // The command-level AbortSignal still settles the operation if the socket already closed.
        }
      };
      destroy();
      // A TCP accept can race just before node-redis stores its socket. Retry until connect settles
      // so that the newly stored socket cannot escape the first destroy call.
      abortDestroyTimer = setInterval(destroy, 10);
      abortDestroyTimer.unref();
    };
    signal?.addEventListener('abort', destroyOnAbort, { once: true });
    try {
      await this.connect(signal);
      if (isAborted(signal)) {
        return null;
      }

      const client = signal === undefined ? this.client : this.client.withAbortSignal(signal);
      const currentRunId = runId(await client.info('server'));
      const reservationId = randomUUID();
      const raw = await client.eval(RESERVE_SCRIPT, {
        keys: [this.limiterKey, this.limiterAuditKey],
        arguments: [
          currentRunId,
          String(this.intervalMs),
          String(this.leadMs),
          String(this.horizonMs),
          reservationId,
          String(AUDIT_TTL_MS),
        ],
      });
      const reply = arrayReply(raw);

      if (reply[0] === 'COLD') {
        await this.initializeColdState(currentRunId, signal);
        return null;
      }
      if (reply[0] === 'DENIED') {
        return null;
      }
      if (
        reply[0] !== 'GRANTED' ||
        reply[1] === undefined ||
        reply[2] === undefined ||
        reply[3] !== reservationId
      ) {
        throw new Error('Redis limiter returned an unknown result');
      }

      const redisNowMs = Number(reply[1]);
      const scheduledAtMs = Number(reply[2]);
      if (!Number.isFinite(redisNowMs) || !Number.isFinite(scheduledAtMs)) {
        throw new Error('Redis limiter returned invalid timestamps');
      }

      return {
        reservationId,
        scheduledMonotonicMs: performance.now() + (scheduledAtMs - redisNowMs),
      };
    } catch (error) {
      if (isAborted(signal)) {
        return null;
      }
      if (error instanceof RateLimiterUnavailableError) {
        throw error;
      }
      throw new RateLimiterUnavailableError({ cause: error });
    } finally {
      if (abortDestroyTimer !== undefined) {
        clearInterval(abortDestroyTimer);
      }
      signal?.removeEventListener('abort', destroyOnAbort);
    }
  }

  public async close(): Promise<void> {
    if (this.client.isOpen) {
      await this.client.close();
    }
  }

  private async connect(signal?: AbortSignal): Promise<void> {
    if (isAborted(signal)) {
      return;
    }
    if (this.client.isOpen) {
      return;
    }

    this.connectPromise ??= this.client.connect().finally(() => {
      this.connectPromise = null;
    });
    await this.connectPromise;
  }

  private async initializeColdState(observedRunId: string, signal?: AbortSignal): Promise<void> {
    const client = await this.options.pool.connect();
    const detachErrorHandler = attachPostgresClientErrorHandler(client, {
      component: 'redis-limiter-barrier',
    });
    let lockAcquired = false;
    let reusable = false;
    try {
      lockAcquired = await this.acquireColdLock(client, signal);
      if (!lockAcquired || isAborted(signal)) {
        return;
      }

      const redis = signal === undefined ? this.client : this.client.withAbortSignal(signal);
      const lockedRunId = runId(await redis.info('server'));
      const state = await redis.hGetAll(this.limiterKey);
      if (hasCompleteState(state, lockedRunId)) {
        reusable = true;
        return;
      }

      await this.advanceDispatchBarrier(client);
      if (isAborted(signal)) {
        return;
      }
      const verifiedRunId = runId(await redis.info('server'));
      if (verifiedRunId !== lockedRunId || lockedRunId !== observedRunId) {
        reusable = true;
        return;
      }

      await redis.eval(INITIALIZE_SCRIPT, {
        keys: [this.limiterKey],
        arguments: [lockedRunId, String(this.coldBarrierMs)],
      });
      reusable = true;
    } finally {
      if (reusable) {
        if (lockAcquired) {
          const unlocked = await client
            .query<{ unlocked: boolean }>('SELECT pg_advisory_unlock($1, $2) AS unlocked', [
              COLD_LOCK_NAMESPACE,
              COLD_LOCK_KEY,
            ])
            .then((result) => result.rows[0]?.unlocked === true)
            .catch(() => false);
          if (!unlocked) {
            reusable = false;
          }
        }
      }
      detachErrorHandler();
      client.release(!reusable);
    }
  }

  private async acquireColdLock(client: PoolClient, signal?: AbortSignal): Promise<boolean> {
    while (!isAborted(signal)) {
      const result = await client.query<{ acquired: boolean }>(
        'SELECT pg_try_advisory_lock($1, $2) AS acquired',
        [COLD_LOCK_NAMESPACE, COLD_LOCK_KEY],
      );
      if (result.rows[0]?.acquired === true) {
        return true;
      }
      await abortableDelay(COLD_LOCK_RETRY_MS, signal);
    }
    return false;
  }

  private async advanceDispatchBarrier(client: PoolClient): Promise<void> {
    const advanced = await client.query(
      `
        UPDATE dispatch_control
        SET
          resume_not_before = GREATEST(
            resume_not_before,
            clock_timestamp() + ($1::integer * interval '1 millisecond')
          ),
          updated_at = clock_timestamp(),
          version = version + 1
        WHERE control_key = 'default-provider'
      `,
      [this.coldBarrierMs],
    );
    if (advanced.rowCount !== 1) {
      throw new Error('Dispatch control barrier is missing');
    }
  }
}
