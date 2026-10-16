import { describe, expect, it, vi } from 'vitest';

import {
  type DispatchAttempt,
  type DispatchClaim,
  type DispatchHeartbeatScheduler,
  type DispatchLifecycleEvent,
  DispatchProcessor,
  type DispatchRepository,
  type PacedPermit,
  type PacedRateLimiter,
  type ProviderMessageSender,
  type ProviderSendResult,
} from '../../packages/alarms/src/application/dispatch';

const claim: DispatchClaim = {
  alarmId: '11111111-1111-4111-8111-111111111111',
  alarmType: 'EMERGENCY',
  alarmBody: 'body',
  deliveryId: '22222222-2222-4222-8222-222222222222',
  recipientId: 'user-1',
  clientRef: '33333333-3333-4333-8333-333333333333',
  leaseGeneration: 1,
  leaseToken: '44444444-4444-4444-8444-444444444444',
};

const attempt: DispatchAttempt = {
  ...claim,
  attemptId: '55555555-5555-4555-8555-555555555555',
  attemptNo: 1,
  reservationId: '66666666-6666-4666-8666-666666666666',
};

const heartbeatScheduler: DispatchHeartbeatScheduler = {
  start: (intervalMs, heartbeat) => {
    let stopped = false;
    let inFlight = Promise.resolve();
    const timer = setInterval(() => {
      if (stopped) return;
      inFlight = inFlight.then(async () => {
        if (!stopped && !(await heartbeat())) {
          stopped = true;
          clearInterval(timer);
        }
      });
    }, intervalMs);
    return {
      stop: async () => {
        stopped = true;
        clearInterval(timer);
        await inFlight;
      },
    };
  },
};

const runtimeOptions = {
  monotonicNow: (): number => 0,
  waitUntil: async (): Promise<void> => undefined,
  heartbeatScheduler,
};

class FakeDispatchRepository implements DispatchRepository {
  public eligible = true;
  public claimed: DispatchClaim | null = claim;
  public started: DispatchAttempt | null = attempt;
  public readonly calls: string[] = [];
  public readonly minimumPriorities: number[] = [];
  public readonly claimMinimumPriorities: number[] = [];
  public finished: ProviderSendResult | null = null;

  public async hasEligible(minimumPriority: number): Promise<boolean> {
    this.calls.push('eligible');
    this.minimumPriorities.push(minimumPriority);
    return this.eligible;
  }

  public async claimNext(minimumPriority: number): Promise<DispatchClaim | null> {
    this.calls.push('claim');
    this.claimMinimumPriorities.push(minimumPriority);
    return this.claimed;
  }

  public async startAttempt(): Promise<DispatchAttempt | null> {
    this.calls.push('start');
    return this.started;
  }

  public async commitAttempt(): Promise<boolean> {
    this.calls.push('commit');
    return true;
  }

  public async heartbeat(): Promise<boolean> {
    this.calls.push('heartbeat');
    return true;
  }

  public async finishNotSent(): Promise<void> {
    this.calls.push('not-sent');
  }

  public async finish(_attempt: DispatchAttempt, result: ProviderSendResult): Promise<void> {
    this.calls.push('finish');
    this.finished = result;
  }
}

describe('DispatchProcessor', () => {
  it('LF-05 rejects a heartbeat cadence that can outlive one third of the send lease', () => {
    const repository = new FakeDispatchRepository();
    const limiter: PacedRateLimiter = { reserve: async () => null };
    const sender: ProviderMessageSender = {
      send: async () => ({ kind: 'ACCEPTED', messageId: 'unused' }),
    };

    expect(
      () =>
        new DispatchProcessor(repository, limiter, sender, {
          ...runtimeOptions,
          leaseDurationMs: 60_000,
          heartbeatIntervalMs: 20_001,
        }),
    ).toThrow('heartbeatIntervalMs must be between 1 and leaseDurationMs / 3');
    expect(
      () =>
        new DispatchProcessor(repository, limiter, sender, {
          ...runtimeOptions,
          leaseDurationMs: 60_000,
          heartbeatIntervalMs: 20_000,
        }),
    ).not.toThrow();
  });

  it('checks work before reserving and persists STARTED before provider commit', async () => {
    const calls: string[] = [];
    const events: DispatchLifecycleEvent[] = [];
    const repository = new FakeDispatchRepository();
    const limiter: PacedRateLimiter = {
      reserve: async (): Promise<PacedPermit> => {
        calls.push('permit');
        return { reservationId: attempt.reservationId, scheduledMonotonicMs: 100 };
      },
    };
    const sender: ProviderMessageSender = {
      send: async (request) => {
        calls.push(`send:${request.clientRef}`);
        return { kind: 'ACCEPTED', messageId: 'message-1' };
      },
    };
    const monotonicTimes = [100, 100, 1_000, 1_037];
    const processor = new DispatchProcessor(repository, limiter, sender, {
      ...runtimeOptions,
      monotonicNow: () => monotonicTimes.shift() ?? 1_037,
      waitUntil: async () => undefined,
      eventSink: { record: (event) => events.push(event) },
    });

    const started = await processor.beginOnce(100);
    await started?.completion;

    expect(repository.calls.slice(0, 2)).toEqual(['eligible', 'claim']);
    expect(calls).toEqual(['permit', `send:${attempt.clientRef}`]);
    expect(repository.calls).toEqual(['eligible', 'claim', 'start', 'commit', 'finish']);
    expect(repository.finished).toEqual({ kind: 'ACCEPTED', messageId: 'message-1' });
    expect(events).toEqual([
      expect.objectContaining({
        eventName: 'ATTEMPT_STARTED',
        traceId: attempt.attemptId,
        deliveryId: attempt.deliveryId,
        clientRef: attempt.clientRef,
        leaseGeneration: attempt.leaseGeneration,
      }),
      expect.objectContaining({
        eventName: 'ATTEMPT_FINISHED',
        outcome: 'ACCEPTED',
        providerCode: 'ACCEPTED',
        providerDurationMs: 37,
        providerDurationSource: 'MONOTONIC_SENDER_BOUNDARY',
      }),
    ]);
  });

  it('PRI-06 checks an empty reserved emergency lane before consuming a shared permit', async () => {
    const repository = new FakeDispatchRepository();
    repository.eligible = false;
    let reservations = 0;
    const processor = new DispatchProcessor(
      repository,
      {
        reserve: async () => {
          reservations += 1;
          return null;
        },
      },
      { send: async () => ({ kind: 'ACCEPTED', messageId: 'unused' }) },
      runtimeOptions,
    );

    await expect(processor.beginOnce(100)).resolves.toBeNull();
    expect(reservations).toBe(0);
    expect(repository.calls).toEqual(['eligible']);
    expect(repository.minimumPriorities).toEqual([100]);
  });

  it('prefetches only a permit and re-evaluates priority before the DB claim', async () => {
    const repository = new FakeDispatchRepository();
    const processor = new DispatchProcessor(
      repository,
      {
        reserve: async () => ({
          reservationId: attempt.reservationId,
          scheduledMonotonicMs: 100,
        }),
      },
      { send: async () => ({ kind: 'ACCEPTED', messageId: 'message-1' }) },
      { ...runtimeOptions, monotonicNow: () => 100 },
    );

    const permit = await processor.preparePermit(100);
    expect(permit).not.toBeNull();
    expect(repository.calls).toEqual(['eligible']);
    expect(repository.minimumPriorities).toEqual([100]);

    const started = await processor.beginWithPermit(permit as PacedPermit, 0);
    await started?.completion;

    expect(repository.claimMinimumPriorities).toEqual([0]);
    expect(repository.calls).toEqual(['eligible', 'claim', 'start', 'commit', 'finish']);
  });

  it('does not reserve when shutdown arrives while the eligible-work probe is pending', async () => {
    const repository = new FakeDispatchRepository();
    let releaseEligible: (() => void) | undefined;
    let eligibleProbeStarted: (() => void) | undefined;
    const probeStarted = new Promise<void>((resolve) => {
      eligibleProbeStarted = resolve;
    });
    repository.hasEligible = async () => {
      repository.calls.push('eligible');
      eligibleProbeStarted?.();
      await new Promise<void>((resolve) => {
        releaseEligible = resolve;
      });
      return true;
    };
    let reservations = 0;
    const processor = new DispatchProcessor(
      repository,
      {
        reserve: async () => {
          reservations += 1;
          return { reservationId: attempt.reservationId, scheduledMonotonicMs: 100 };
        },
      },
      { send: async () => ({ kind: 'ACCEPTED', messageId: 'unused' }) },
      runtimeOptions,
    );
    const shutdown = new AbortController();

    const preparation = processor.preparePermit(0, shutdown.signal);
    await probeStarted;
    shutdown.abort();
    releaseEligible?.();

    await expect(preparation).resolves.toBeNull();
    expect(reservations).toBe(0);
    expect(repository.calls).toEqual(['eligible']);
  });

  it('drops a prefetched permit past allowedLate before claim or STARTED', async () => {
    const repository = new FakeDispatchRepository();
    let sends = 0;
    const processor = new DispatchProcessor(
      repository,
      { reserve: async () => null },
      {
        send: async () => {
          sends += 1;
          return { kind: 'ACCEPTED', messageId: 'must-not-send' };
        },
      },
      {
        ...runtimeOptions,
        monotonicNow: () => 106,
        allowedLateMs: 5,
      },
    );

    await expect(
      processor.beginWithPermit(
        { reservationId: attempt.reservationId, scheduledMonotonicMs: 100 },
        0,
      ),
    ).resolves.toBeNull();
    expect(repository.calls).toEqual([]);
    expect(sends).toBe(0);
  });

  it('allows the exact late boundary before claim and rechecks after STARTED', async () => {
    const repository = new FakeDispatchRepository();
    let now = 105;
    repository.startAttempt = async () => {
      repository.calls.push('start');
      now = 106;
      return attempt;
    };
    let sends = 0;
    const processor = new DispatchProcessor(
      repository,
      { reserve: async () => null },
      {
        send: async () => {
          sends += 1;
          return { kind: 'ACCEPTED', messageId: 'must-not-send' };
        },
      },
      {
        ...runtimeOptions,
        monotonicNow: () => now,
        allowedLateMs: 5,
      },
    );

    await expect(
      processor.beginWithPermit(
        { reservationId: attempt.reservationId, scheduledMonotonicMs: 100 },
        0,
      ),
    ).resolves.toBeNull();
    expect(repository.calls).toEqual(['claim', 'start', 'not-sent']);
    expect(sends).toBe(0);
  });

  it('commits and sends at the exact second late-fence boundary', async () => {
    const repository = new FakeDispatchRepository();
    let monotonicReads = 0;
    let sends = 0;
    const processor = new DispatchProcessor(
      repository,
      { reserve: async () => null },
      {
        send: async () => {
          sends += 1;
          return { kind: 'ACCEPTED', messageId: 'message-1' };
        },
      },
      {
        ...runtimeOptions,
        monotonicNow: () => {
          monotonicReads += 1;
          return monotonicReads === 1 ? 100 : 105;
        },
        allowedLateMs: 5,
      },
    );

    const started = await processor.beginWithPermit(
      { reservationId: attempt.reservationId, scheduledMonotonicMs: 100 },
      0,
    );
    await started?.completion;

    expect(repository.calls).toEqual(['claim', 'start', 'commit', 'finish']);
    expect(sends).toBe(1);
  });

  it('finishes not-sent when shutdown aborts a wait before the commit fence', async () => {
    const repository = new FakeDispatchRepository();
    let waitStarted: (() => void) | undefined;
    const waiting = new Promise<void>((resolve) => {
      waitStarted = resolve;
    });
    let sends = 0;
    const processor = new DispatchProcessor(
      repository,
      {
        reserve: async () => ({
          reservationId: attempt.reservationId,
          scheduledMonotonicMs: 100,
        }),
      },
      {
        send: async () => {
          sends += 1;
          return { kind: 'ACCEPTED', messageId: 'must-not-send' };
        },
      },
      {
        ...runtimeOptions,
        monotonicNow: () => 100,
        waitUntil: async (_target, signal) => {
          waitStarted?.();
          if (signal?.aborted === true) return;
          await new Promise<void>((resolve) => {
            signal?.addEventListener('abort', () => resolve(), { once: true });
          });
        },
      },
    );
    const shutdown = new AbortController();

    const admission = processor.beginOnce(0, shutdown.signal);
    await waiting;
    shutdown.abort();

    await expect(admission).resolves.toBeNull();
    expect(repository.calls).toEqual(['eligible', 'claim', 'start', 'not-sent']);
    expect(sends).toBe(0);
  });

  it('PRI-06 bounds a stale eligible-hint permit to one and does not self-saturate on empty polls', async () => {
    const repository = new FakeDispatchRepository();
    repository.claimed = null;
    let reservations = 0;
    const processor = new DispatchProcessor(
      repository,
      {
        reserve: async () => {
          reservations += 1;
          return {
            reservationId: attempt.reservationId,
            scheduledMonotonicMs: 100,
          };
        },
      },
      { send: async () => ({ kind: 'ACCEPTED', messageId: 'unused' }) },
      runtimeOptions,
    );

    await expect(processor.beginOnce(100)).resolves.toBeNull();
    repository.eligible = false;
    for (let poll = 0; poll < 40; poll += 1) {
      await expect(processor.beginOnce(100)).resolves.toBeNull();
    }

    expect(reservations).toBe(1);
    expect(repository.calls.filter((call) => call === 'claim')).toHaveLength(1);
    expect(repository.calls.filter((call) => call === 'eligible')).toHaveLength(41);
  });

  it('does not let an observability sink failure change dispatch state', async () => {
    const repository = new FakeDispatchRepository();
    const processor = new DispatchProcessor(
      repository,
      {
        reserve: async () => ({
          reservationId: attempt.reservationId,
          scheduledMonotonicMs: 100,
        }),
      },
      { send: async () => ({ kind: 'ACCEPTED', messageId: 'message-1' }) },
      {
        ...runtimeOptions,
        monotonicNow: () => 100,
        waitUntil: async () => undefined,
        eventSink: {
          record: () => {
            throw new Error('stdout unavailable');
          },
        },
      },
    );

    const started = await processor.beginOnce(0);
    await expect(started?.completion).resolves.toBeUndefined();
    expect(repository.calls).toEqual(['eligible', 'claim', 'start', 'commit', 'finish']);
  });

  it('[RET-17] converts a rejected provider adapter promise to UNKNOWN-safe evidence', async () => {
    const repository = new FakeDispatchRepository();
    const events: DispatchLifecycleEvent[] = [];
    const rawError = 'adapter rejected with phone=010-1234-5678';
    const processor = new DispatchProcessor(
      repository,
      {
        reserve: async () => ({
          reservationId: attempt.reservationId,
          scheduledMonotonicMs: 100,
        }),
      },
      {
        send: async () => {
          throw new Error(rawError);
        },
      },
      {
        ...runtimeOptions,
        monotonicNow: () => 100,
        eventSink: { record: (event) => events.push(event) },
      },
    );

    const started = await processor.beginOnce(0);
    await expect(started?.completion).resolves.toBeUndefined();
    expect(repository.finished).toEqual({
      kind: 'AMBIGUOUS',
      code: 'PROVIDER_ADAPTER_REJECTED',
    });
    expect(JSON.stringify(events)).not.toContain(rawError);
    expect(events.at(-1)).toEqual(
      expect.objectContaining({
        eventName: 'ATTEMPT_FINISHED',
        outcome: 'AMBIGUOUS',
        providerCode: 'PROVIDER_ADAPTER_REJECTED',
      }),
    );
  });

  it('[RET-17] records a proven pre-request failure as an explicit not-sent lifecycle event', async () => {
    const repository = new FakeDispatchRepository();
    const events: DispatchLifecycleEvent[] = [];
    const processor = new DispatchProcessor(
      repository,
      {
        reserve: async () => ({
          reservationId: attempt.reservationId,
          scheduledMonotonicMs: 100,
        }),
      },
      {
        send: async () => ({
          kind: 'DEFINITELY_NOT_SENT',
          code: 'PROVIDER_REQUEST_CREATION_ERROR',
        }),
      },
      {
        ...runtimeOptions,
        monotonicNow: () => 100,
        eventSink: { record: (event) => events.push(event) },
      },
    );

    const started = await processor.beginOnce(0);
    await expect(started?.completion).resolves.toBeUndefined();
    expect(repository.finished).toEqual({
      kind: 'DEFINITELY_NOT_SENT',
      code: 'PROVIDER_REQUEST_CREATION_ERROR',
    });
    expect(events.at(-1)).toEqual(
      expect.objectContaining({
        eventName: 'ATTEMPT_NOT_SENT',
        outcome: 'DEFINITELY_NOT_SENT',
        providerCode: 'PROVIDER_REQUEST_CREATION_ERROR',
      }),
    );
  });

  it('finishes the durable attempt as not-sent when its paced slot is late', async () => {
    const repository = new FakeDispatchRepository();
    const events: DispatchLifecycleEvent[] = [];
    let sends = 0;
    let monotonicReads = 0;
    const processor = new DispatchProcessor(
      repository,
      {
        reserve: async () => ({
          reservationId: attempt.reservationId,
          scheduledMonotonicMs: 100,
        }),
      },
      {
        send: async () => {
          sends += 1;
          return { kind: 'ACCEPTED', messageId: 'unused' };
        },
      },
      {
        ...runtimeOptions,
        monotonicNow: () => {
          monotonicReads += 1;
          return monotonicReads === 1 ? 100 : 106;
        },
        waitUntil: async () => undefined,
        allowedLateMs: 5,
        eventSink: { record: (event) => events.push(event) },
      },
    );

    await expect(processor.beginOnce(0)).resolves.toBeNull();
    expect(sends).toBe(0);
    expect(repository.calls).toEqual(['eligible', 'claim', 'start', 'not-sent']);
    expect(events.at(-1)).toEqual(
      expect.objectContaining({ eventName: 'ATTEMPT_NOT_SENT', outcome: 'NOT_SENT' }),
    );
  });

  it('does not call the provider when the durable commit fence is rejected', async () => {
    const repository = new FakeDispatchRepository();
    repository.commitAttempt = async () => {
      repository.calls.push('commit');
      return false;
    };
    let sends = 0;
    const processor = new DispatchProcessor(
      repository,
      {
        reserve: async () => ({
          reservationId: attempt.reservationId,
          scheduledMonotonicMs: 100,
        }),
      },
      {
        send: async () => {
          sends += 1;
          return { kind: 'ACCEPTED', messageId: 'unused' };
        },
      },
      { ...runtimeOptions, monotonicNow: () => 100 },
    );

    await expect(processor.beginOnce(0)).resolves.toBeNull();
    expect(sends).toBe(0);
    expect(repository.calls).toEqual(['eligible', 'claim', 'start', 'commit', 'not-sent']);
  });

  it.each(['claim', 'start', 'commit'] as const)(
    'fails closed without a provider call when the %s database phase fails',
    async (failurePhase) => {
      const repository = new FakeDispatchRepository();
      if (failurePhase === 'claim') {
        repository.claimNext = async () => {
          repository.calls.push('claim');
          throw new Error('database unavailable during claim');
        };
      } else if (failurePhase === 'start') {
        repository.startAttempt = async () => {
          repository.calls.push('start');
          throw new Error('database unavailable during start');
        };
      } else {
        repository.commitAttempt = async () => {
          repository.calls.push('commit');
          throw new Error('database unavailable during provider commit fence');
        };
      }
      let sends = 0;
      const processor = new DispatchProcessor(
        repository,
        {
          reserve: async () => ({
            reservationId: attempt.reservationId,
            scheduledMonotonicMs: 100,
          }),
        },
        {
          send: async () => {
            sends += 1;
            return { kind: 'ACCEPTED', messageId: 'must-not-send' };
          },
        },
        { ...runtimeOptions, monotonicNow: () => 100 },
      );

      await expect(processor.beginOnce(0)).rejects.toThrow('database unavailable');
      expect(sends).toBe(0);
    },
  );

  it('does not resend when the result commit fails after a provider acceptance', async () => {
    const repository = new FakeDispatchRepository();
    repository.finish = async () => {
      repository.calls.push('finish');
      throw new Error('database unavailable during result commit');
    };
    let sends = 0;
    const processor = new DispatchProcessor(
      repository,
      {
        reserve: async () => ({
          reservationId: attempt.reservationId,
          scheduledMonotonicMs: 100,
        }),
      },
      {
        send: async () => {
          sends += 1;
          return { kind: 'ACCEPTED', messageId: 'accepted-before-db-outage' };
        },
      },
      { ...runtimeOptions, monotonicNow: () => 100 },
    );

    const started = await processor.beginOnce(0);
    await expect(started?.completion).rejects.toThrow('database unavailable during result commit');
    expect(sends).toBe(1);
    expect(repository.calls).toEqual(['eligible', 'claim', 'start', 'commit', 'finish']);
  });

  it('LF-05 keeps a 30-second committed request leased with p99 cadence below half a lease', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    try {
      const repository = new FakeDispatchRepository();
      const heartbeatTimes: number[] = [];
      repository.heartbeat = async () => {
        repository.calls.push('heartbeat');
        heartbeatTimes.push(Date.now());
        return true;
      };
      let resolveSend: ((result: ProviderSendResult) => void) | undefined;
      const processor = new DispatchProcessor(
        repository,
        {
          reserve: async () => ({
            reservationId: attempt.reservationId,
            scheduledMonotonicMs: 100,
          }),
        },
        {
          send: () =>
            new Promise((resolve) => {
              resolveSend = resolve;
            }),
        },
        {
          ...runtimeOptions,
          monotonicNow: () => 100,
          waitUntil: async () => undefined,
          leaseDurationMs: 60_000,
          heartbeatIntervalMs: 10_000,
        },
      );

      const started = await processor.beginOnce(0);
      await vi.advanceTimersByTimeAsync(30_000);
      expect(repository.calls.filter((call) => call === 'heartbeat')).toHaveLength(3);
      expect(heartbeatTimes).toEqual([10_000, 20_000, 30_000]);
      const heartbeatDelays = heartbeatTimes.map((timestamp, index) =>
        index === 0 ? timestamp : timestamp - (heartbeatTimes[index - 1] ?? 0),
      );
      const p99Index = Math.ceil(heartbeatDelays.length * 0.99) - 1;
      expect([...heartbeatDelays].sort((left, right) => left - right)[p99Index]).toBeLessThan(
        30_000,
      );

      resolveSend?.({ kind: 'ACCEPTED', messageId: 'slow-message' });
      await started?.completion;
      const heartbeatCount = repository.calls.filter((call) => call === 'heartbeat').length;
      await vi.advanceTimersByTimeAsync(30_000);
      expect(repository.calls.filter((call) => call === 'heartbeat')).toHaveLength(heartbeatCount);
      expect(repository.calls.at(-1)).toBe('finish');
    } finally {
      vi.useRealTimers();
    }
  });

  it('drains a committed provider request instead of forwarding the admission abort', async () => {
    const repository = new FakeDispatchRepository();
    const shutdown = new AbortController();
    let providerSignal: AbortSignal | undefined;
    let resolveSend: ((result: ProviderSendResult) => void) | undefined;
    const processor = new DispatchProcessor(
      repository,
      {
        reserve: async () => ({
          reservationId: attempt.reservationId,
          scheduledMonotonicMs: 100,
        }),
      },
      {
        send: (_request, signal) => {
          providerSignal = signal;
          return new Promise((resolve) => {
            resolveSend = resolve;
          });
        },
      },
      { ...runtimeOptions, monotonicNow: () => 100 },
    );

    const started = await processor.beginOnce(0, shutdown.signal);
    shutdown.abort();
    await new Promise<void>((resolve) => setImmediate(resolve));

    expect(providerSignal).toBeUndefined();
    expect(repository.calls).toEqual(['eligible', 'claim', 'start', 'commit']);
    resolveSend?.({ kind: 'ACCEPTED', messageId: 'drained-message' });
    await expect(started?.completion).resolves.toBeUndefined();
    expect(repository.calls.at(-1)).toBe('finish');
  });
});
