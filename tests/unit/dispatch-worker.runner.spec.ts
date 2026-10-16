import 'reflect-metadata';

import { Logger } from '@nestjs/common';
import { describe, expect, it, vi } from 'vitest';

import { DispatchWorkerRunner } from '../../apps/worker/src/dispatch-worker.runner';
import {
  type DispatchAttempt,
  type DispatchClaim,
  DispatchProcessor,
  type DispatchRepository,
  type PacedPermit,
  type StartedDispatch,
} from '../../packages/alarms/src/application/dispatch';
import type { DispatchRuntime } from '../../packages/alarms/src/infrastructure/persistence/dispatch-runtime';
import { PROCESS_INSTANCE_ID } from '../../packages/platform/src/process-identity';

interface Deferred {
  readonly promise: Promise<void>;
  readonly resolve: () => void;
}

interface RejectableDeferred extends Deferred {
  readonly reject: (error: unknown) => void;
}

type BeginOnce = (minimumPriority: number, signal?: AbortSignal) => Promise<StartedDispatch | null>;

function deferred(): Deferred {
  let resolve: (() => void) | undefined;
  const promise = new Promise<void>((settle) => {
    resolve = settle;
  });
  return {
    promise,
    resolve: () => {
      resolve?.();
    },
  };
}

function rejectableDeferred(): RejectableDeferred {
  let resolve: (() => void) | undefined;
  let reject: ((error: unknown) => void) | undefined;
  const promise = new Promise<void>((settle, fail) => {
    resolve = settle;
    reject = fail;
  });
  return {
    promise,
    resolve: () => {
      resolve?.();
    },
    reject: (error) => {
      reject?.(error);
    },
  };
}

async function within<T>(promise: Promise<T>, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => reject(new Error(`Timed out waiting for ${label}`)), 1_000);
  });
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    if (timer !== undefined) {
      clearTimeout(timer);
    }
  }
}

function createRunner(beginOnce: BeginOnce, close: () => Promise<void>): DispatchWorkerRunner {
  let sequence = 0;
  const prepared = new Map<string, StartedDispatch>();
  return new DispatchWorkerRunner({
    processor: {
      preparePermit: async (minimumPriority: number, signal?: AbortSignal) => {
        const started = await beginOnce(minimumPriority, signal);
        if (started === null) return null;
        sequence += 1;
        const permit = {
          reservationId: `00000000-0000-4000-8000-${String(sequence).padStart(12, '0')}`,
          scheduledMonotonicMs: 0,
        };
        prepared.set(permit.reservationId, started);
        return permit;
      },
      beginWithPermit: async (permit: PacedPermit) => {
        const started = prepared.get(permit.reservationId) ?? null;
        prepared.delete(permit.reservationId);
        return started;
      },
    },
    close,
  } as unknown as DispatchRuntime);
}

function createSplitRunner(
  processor: {
    preparePermit: (minimumPriority: number, signal?: AbortSignal) => Promise<PacedPermit | null>;
    beginWithPermit: (
      permit: PacedPermit,
      minimumPriority: number,
      signal?: AbortSignal,
    ) => Promise<StartedDispatch | null>;
  },
  close: () => Promise<void>,
): DispatchWorkerRunner {
  return new DispatchWorkerRunner({ processor, close } as unknown as DispatchRuntime);
}

describe('DispatchWorkerRunner', () => {
  it('PRI-02 admits emergency work through two reserved slots while 62 bulk sends remain slow', async () => {
    const completions = Array.from({ length: 63 }, deferred);
    const emergencyAdmitted = deferred();
    const priorities: number[] = [];
    let generalAdmissions = 0;
    let emergencyAdmissions = 0;
    const beginOnce = vi.fn<BeginOnce>(async (minimumPriority, signal) => {
      priorities.push(minimumPriority);
      if (signal?.aborted === true) {
        return null;
      }
      if (minimumPriority === 0 && generalAdmissions < 62) {
        const completion = completions[generalAdmissions]?.promise;
        generalAdmissions += 1;
        return completion === undefined ? null : { priority: 10, completion };
      }
      if (minimumPriority === 100 && emergencyAdmissions === 0) {
        emergencyAdmissions += 1;
        emergencyAdmitted.resolve();
        return { priority: 100, completion: completions[62]?.promise ?? Promise.resolve() };
      }
      return null;
    });
    const close = vi.fn(async () => undefined);
    const runner = createRunner(beginOnce, close);

    runner.onApplicationBootstrap();
    try {
      await within(emergencyAdmitted.promise, 'reserved emergency admission');

      expect(generalAdmissions).toBe(62);
      expect(emergencyAdmissions).toBe(1);
      expect(priorities.slice(0, 63)).toEqual([...Array<number>(62).fill(0), 100]);
    } finally {
      const stopping = runner.onApplicationShutdown();
      for (const completion of completions) {
        completion.resolve();
      }
      await stopping;
    }

    expect(close).toHaveBeenCalledOnce();
  });

  it('PRI-06 probes the empty emergency lane without starving a freed bulk slot', async () => {
    const completions = Array.from({ length: 63 }, deferred);
    const reservedProbe = deferred();
    const bulkReplacement = deferred();
    const priorities: number[] = [];
    let generalAdmissions = 0;
    const beginOnce = vi.fn<BeginOnce>(async (minimumPriority, signal) => {
      priorities.push(minimumPriority);
      if (signal?.aborted === true) {
        return null;
      }
      if (minimumPriority === 100) {
        reservedProbe.resolve();
        return null;
      }

      const index = generalAdmissions;
      generalAdmissions += 1;
      if (index === 62) {
        bulkReplacement.resolve();
      }
      return { priority: 10, completion: completions[index]?.promise ?? Promise.resolve() };
    });
    const close = vi.fn(async () => undefined);
    const runner = createRunner(beginOnce, close);

    runner.onApplicationBootstrap();
    try {
      await within(reservedProbe.promise, 'empty reserved-lane probe');
      expect(priorities.slice(0, 63)).toEqual([...Array<number>(62).fill(0), 100]);

      completions[0]?.resolve();
      await within(bulkReplacement.promise, 'bulk replacement admission');

      expect(generalAdmissions).toBe(63);
      expect(priorities).toContain(100);
      expect(priorities[62]).toBe(100);
      expect(priorities.slice(63)).toContain(0);
    } finally {
      const stopping = runner.onApplicationShutdown();
      for (const completion of completions) {
        completion.resolve();
      }
      await stopping;
    }

    expect(close).toHaveBeenCalledOnce();
  });

  it('PRI-06 detects emergency work on the next 50 ms reserved-lane poll in one runner', async () => {
    vi.useFakeTimers();
    try {
      const bulkCompletions = Array.from({ length: 62 }, deferred);
      const emergencyCompletion = deferred();
      const firstEmptyReservedProbe = deferred();
      const emergencyAdmitted = deferred();
      const priorities: number[] = [];
      const reservedProbeTimes: number[] = [];
      let generalAdmissions = 0;
      let reservedProbes = 0;
      let emergencyAvailable = false;
      const beginOnce = vi.fn<BeginOnce>(async (minimumPriority, signal) => {
        priorities.push(minimumPriority);
        if (signal?.aborted === true) {
          return null;
        }
        if (minimumPriority === 0 && generalAdmissions < 62) {
          const completion = bulkCompletions[generalAdmissions]?.promise;
          generalAdmissions += 1;
          return completion === undefined ? null : { priority: 10, completion };
        }
        if (minimumPriority === 100) {
          reservedProbes += 1;
          reservedProbeTimes.push(Date.now());
          if (!emergencyAvailable) {
            firstEmptyReservedProbe.resolve();
            return null;
          }
          emergencyAvailable = false;
          emergencyAdmitted.resolve();
          return { priority: 100, completion: emergencyCompletion.promise };
        }
        return null;
      });
      const close = vi.fn(async () => undefined);
      const runner = createRunner(beginOnce, close);

      runner.onApplicationBootstrap();
      let stopping: Promise<void> | undefined;
      try {
        await firstEmptyReservedProbe.promise;
        expect(priorities.slice(0, 63)).toEqual([...Array<number>(62).fill(0), 100]);
        await vi.advanceTimersByTimeAsync(0);
        const probesBeforeEmergency = reservedProbes;
        const lastProbeBeforeEmergency = reservedProbeTimes.at(-1) ?? 0;

        emergencyAvailable = true;
        await vi.advanceTimersByTimeAsync(49);
        expect(reservedProbes).toBe(probesBeforeEmergency);

        await vi.advanceTimersByTimeAsync(1);
        await emergencyAdmitted.promise;
        expect(reservedProbes).toBeGreaterThan(probesBeforeEmergency);
        expect((reservedProbeTimes.at(-1) ?? 0) - lastProbeBeforeEmergency).toBe(50);
        expect(priorities.at(-1)).toBe(100);
      } finally {
        stopping = runner.onApplicationShutdown();
        for (const completion of bulkCompletions) {
          completion.resolve();
        }
        emergencyCompletion.resolve();
        await stopping;
      }

      expect(close).toHaveBeenCalledOnce();
    } finally {
      vi.useRealTimers();
    }
  });

  it('RL-11 spends no permits while idle for 2 seconds and admits new work on the next 50 ms poll', async () => {
    vi.useFakeTimers();
    try {
      const claim: DispatchClaim = {
        alarmId: '11111111-1111-4111-8111-111111111111',
        alarmType: 'BULK',
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
      const completed = deferred();
      let workAvailable = false;
      let claimed = false;
      let reservations = 0;
      let finished = false;
      const repository: DispatchRepository = {
        hasEligible: async () => workAvailable && !claimed,
        claimNext: async () => {
          if (!workAvailable || claimed) return null;
          claimed = true;
          return claim;
        },
        startAttempt: async () => attempt,
        commitAttempt: async () => true,
        heartbeat: async () => true,
        finishNotSent: async () => undefined,
        finish: async () => {
          finished = true;
          workAvailable = false;
          completed.resolve();
        },
      };
      const processor = new DispatchProcessor(
        repository,
        {
          reserve: async () => {
            reservations += 1;
            return { reservationId: attempt.reservationId, scheduledMonotonicMs: 0 };
          },
        },
        { send: async () => ({ kind: 'ACCEPTED', messageId: 'message-1' }) },
        {
          monotonicNow: () => 0,
          waitUntil: async () => undefined,
          heartbeatScheduler: {
            start: () => ({ stop: async () => undefined }),
          },
        },
      );
      const close = vi.fn(async () => undefined);
      const runner = new DispatchWorkerRunner({ processor, close } as unknown as DispatchRuntime);

      runner.onApplicationBootstrap();
      let stopping: Promise<void> | undefined;
      try {
        await vi.advanceTimersByTimeAsync(2_000);
        expect(reservations).toBe(0);
        expect(finished).toBe(false);

        const availableAt = Date.now();
        workAvailable = true;
        await vi.advanceTimersByTimeAsync(49);
        expect(reservations).toBe(0);

        await vi.advanceTimersByTimeAsync(1);
        await completed.promise;
        expect(Date.now() - availableAt).toBe(50);
        expect(reservations).toBe(1);
        expect(finished).toBe(true);
      } finally {
        stopping = runner.onApplicationShutdown();
        await stopping;
      }

      expect(close).toHaveBeenCalledOnce();
    } finally {
      vi.useRealTimers();
    }
  });

  it('LF-06 keeps at most one unresolved admission pipeline while provider completions overlap', async () => {
    const admissionGates = Array.from({ length: 3 }, deferred);
    const admissionStarted = Array.from({ length: 3 }, deferred);
    const completions = Array.from({ length: 3 }, deferred);
    let calls = 0;
    let activeAdmissions = 0;
    let maximumActiveAdmissions = 0;
    const beginOnce = vi.fn<BeginOnce>(async (_minimumPriority, signal) => {
      if (signal?.aborted === true) {
        return null;
      }
      const index = calls;
      calls += 1;
      if (index >= admissionGates.length) {
        return null;
      }
      activeAdmissions += 1;
      maximumActiveAdmissions = Math.max(maximumActiveAdmissions, activeAdmissions);
      admissionStarted[index]?.resolve();
      await admissionGates[index]?.promise;
      activeAdmissions -= 1;
      return { priority: 10, completion: completions[index]?.promise ?? Promise.resolve() };
    });
    const close = vi.fn(async () => undefined);
    const runner = createRunner(beginOnce, close);

    runner.onApplicationBootstrap();
    let stopping: Promise<void> | undefined;
    try {
      await within(admissionStarted[0]?.promise ?? Promise.resolve(), 'first admission pipeline');
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(calls).toBe(1);
      expect(activeAdmissions).toBe(1);

      admissionGates[0]?.resolve();
      await within(admissionStarted[1]?.promise ?? Promise.resolve(), 'second admission pipeline');
      expect(calls).toBe(2);
      expect(activeAdmissions).toBe(1);

      admissionGates[1]?.resolve();
      await within(admissionStarted[2]?.promise ?? Promise.resolve(), 'third admission pipeline');
      expect(calls).toBe(3);
      expect(activeAdmissions).toBe(1);
      expect(maximumActiveAdmissions).toBe(1);
    } finally {
      for (const gate of admissionGates) {
        gate.resolve();
      }
      stopping = runner.onApplicationShutdown();
      for (const completion of completions) {
        completion.resolve();
      }
      await stopping;
    }

    expect(close).toHaveBeenCalledOnce();
  });

  it('LF-06 overlaps one permit prefetch with exactly one active DB admission', async () => {
    const admissionGates = [deferred(), deferred()];
    const admissionStarted = [deferred(), deferred()];
    const completions = [deferred(), deferred()];
    const secondPermitPrepared = deferred();
    const permits: PacedPermit[] = [
      { reservationId: '11111111-1111-4111-8111-111111111111', scheduledMonotonicMs: 100 },
      { reservationId: '22222222-2222-4222-8222-222222222222', scheduledMonotonicMs: 200 },
    ];
    let preparations = 0;
    let admissionSequence = 0;
    let activeAdmissions = 0;
    let maximumActiveAdmissions = 0;
    const processor = {
      preparePermit: async (): Promise<PacedPermit | null> => {
        const index = preparations;
        preparations += 1;
        if (index === 1) secondPermitPrepared.resolve();
        return permits[index] ?? null;
      },
      beginWithPermit: async (): Promise<StartedDispatch | null> => {
        const index = admissionSequence;
        admissionSequence += 1;
        activeAdmissions += 1;
        maximumActiveAdmissions = Math.max(maximumActiveAdmissions, activeAdmissions);
        admissionStarted[index]?.resolve();
        await admissionGates[index]?.promise;
        activeAdmissions -= 1;
        return {
          priority: 10,
          completion: completions[index]?.promise ?? Promise.resolve(),
        };
      },
    };
    const close = vi.fn(async () => undefined);
    const runner = createSplitRunner(processor, close);

    runner.onApplicationBootstrap();
    let stopping: Promise<void> | undefined;
    try {
      await within(admissionStarted[0]?.promise ?? Promise.resolve(), 'first active admission');
      await within(secondPermitPrepared.promise, 'single pending permit');
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(preparations).toBe(2);
      expect(activeAdmissions).toBe(1);

      admissionGates[0]?.resolve();
      await within(admissionStarted[1]?.promise ?? Promise.resolve(), 'second active admission');
      expect(preparations).toBe(3);
      expect(activeAdmissions).toBe(1);
      expect(maximumActiveAdmissions).toBe(1);
    } finally {
      stopping = runner.onApplicationShutdown();
      for (const gate of admissionGates) gate.resolve();
      for (const completion of completions) completion.resolve();
      await stopping;
    }

    expect(close).toHaveBeenCalledOnce();
  });

  it('quarantines a speculative permit for 50 ms after a stale claim returns null', async () => {
    vi.useFakeTimers();
    try {
      const activatedReservationIds: string[] = [];
      let preparations = 0;
      const processor = {
        preparePermit: async (
          _minimumPriority: number,
          signal?: AbortSignal,
        ): Promise<PacedPermit | null> => {
          if (signal?.aborted === true) return null;
          preparations += 1;
          return {
            reservationId: `00000000-0000-4000-8000-${String(preparations).padStart(12, '0')}`,
            scheduledMonotonicMs: preparations,
          };
        },
        beginWithPermit: async (permit: PacedPermit): Promise<StartedDispatch | null> => {
          activatedReservationIds.push(permit.reservationId);
          return null;
        },
      };
      const close = vi.fn(async () => undefined);
      const runner = createSplitRunner(processor, close);

      runner.onApplicationBootstrap();
      await vi.advanceTimersByTimeAsync(0);
      expect(preparations).toBe(2);
      expect(activatedReservationIds).toEqual(['00000000-0000-4000-8000-000000000001']);

      await vi.advanceTimersByTimeAsync(49);
      expect(preparations).toBe(2);
      expect(activatedReservationIds).toHaveLength(1);

      await vi.advanceTimersByTimeAsync(1);
      expect(activatedReservationIds.at(-1)).toBe('00000000-0000-4000-8000-000000000003');
      expect(activatedReservationIds).not.toContain('00000000-0000-4000-8000-000000000002');

      const stopping = runner.onApplicationShutdown();
      await stopping;
      expect(close).toHaveBeenCalledOnce();
    } finally {
      vi.useRealTimers();
    }
  });

  it('caps real split provider completions plus the active admission at 64', async () => {
    const completions = Array.from({ length: 65 }, deferred);
    const capacityReached = deferred();
    const replacementActivated = deferred();
    const preparationPriorities: number[] = [];
    const activationPriorities: number[] = [];
    let preparations = 0;
    let activations = 0;
    let providerInFlight = 0;
    let maximumProviderInFlight = 0;
    const processor = {
      preparePermit: async (
        minimumPriority: number,
        signal?: AbortSignal,
      ): Promise<PacedPermit | null> => {
        if (signal?.aborted === true) return null;
        preparations += 1;
        preparationPriorities.push(minimumPriority);
        return {
          reservationId: `00000000-0000-4000-8000-${String(preparations).padStart(12, '0')}`,
          scheduledMonotonicMs: preparations,
        };
      },
      beginWithPermit: async (
        _permit: PacedPermit,
        minimumPriority: number,
        signal?: AbortSignal,
      ): Promise<StartedDispatch | null> => {
        if (signal?.aborted === true) return null;
        const index = activations;
        activations += 1;
        activationPriorities.push(minimumPriority);
        providerInFlight += 1;
        maximumProviderInFlight = Math.max(maximumProviderInFlight, providerInFlight);
        if (activations === 64) capacityReached.resolve();
        if (activations === 65) replacementActivated.resolve();
        return {
          priority: minimumPriority === 100 ? 100 : 10,
          completion: (completions[index]?.promise ?? Promise.resolve()).finally(() => {
            providerInFlight -= 1;
          }),
        };
      },
    };
    const close = vi.fn(async () => undefined);
    const runner = createSplitRunner(processor, close);

    runner.onApplicationBootstrap();
    let stopping: Promise<void> | undefined;
    try {
      await within(capacityReached.promise, '64th split activation');
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(preparations).toBe(64);
      expect(activations).toBe(64);
      expect(providerInFlight).toBe(64);
      expect(maximumProviderInFlight).toBe(64);
      expect(preparationPriorities.slice(0, 62)).toEqual(Array<number>(62).fill(0));
      expect(preparationPriorities.slice(62, 64)).toEqual([100, 100]);
      expect(activationPriorities.slice(0, 62)).toEqual(Array<number>(62).fill(0));
      expect(activationPriorities.slice(62, 64)).toEqual([100, 100]);

      completions[0]?.resolve();
      await within(replacementActivated.promise, 'replacement after capacity frees');
      expect(preparations).toBe(65);
      expect(activations).toBe(65);
      expect(maximumProviderInFlight).toBe(64);
    } finally {
      stopping = runner.onApplicationShutdown();
      for (const completion of completions) completion.resolve();
      await stopping;
    }

    expect(close).toHaveBeenCalledOnce();
  });

  it('re-evaluates priority when a prefetched reserved-lane permit is activated', async () => {
    const completions = Array.from({ length: 62 }, deferred);
    const sixtySecondAdmissionGate = deferred();
    const reservedPermitPrepared = deferred();
    const reservedPermitActivated = deferred();
    const preparedPriorities = new Map<string, number>();
    let permitSequence = 0;
    let activationSequence = 0;
    let reservedPermitId: string | undefined;
    let reservedActivationPriority: number | undefined;
    const processor = {
      preparePermit: async (minimumPriority: number): Promise<PacedPermit | null> => {
        permitSequence += 1;
        if (permitSequence > 63) return null;
        const permit = {
          reservationId: `00000000-0000-4000-8000-${String(permitSequence).padStart(12, '0')}`,
          scheduledMonotonicMs: permitSequence,
        };
        preparedPriorities.set(permit.reservationId, minimumPriority);
        if (permitSequence === 63) {
          reservedPermitId = permit.reservationId;
          reservedPermitPrepared.resolve();
        }
        return permit;
      },
      beginWithPermit: async (
        permit: PacedPermit,
        minimumPriority: number,
      ): Promise<StartedDispatch | null> => {
        const index = activationSequence;
        activationSequence += 1;
        if (index === 61) await sixtySecondAdmissionGate.promise;
        if (permit.reservationId === reservedPermitId) {
          reservedActivationPriority = minimumPriority;
          reservedPermitActivated.resolve();
          return null;
        }
        return { priority: 10, completion: completions[index]?.promise ?? Promise.resolve() };
      },
    };
    const close = vi.fn(async () => undefined);
    const runner = createSplitRunner(processor, close);

    runner.onApplicationBootstrap();
    let stopping: Promise<void> | undefined;
    try {
      await within(reservedPermitPrepared.promise, 'reserved-lane permit prefetch');
      expect(preparedPriorities.get(reservedPermitId as string)).toBe(100);

      completions[0]?.resolve();
      sixtySecondAdmissionGate.resolve();
      await within(reservedPermitActivated.promise, 'prefetched permit activation');
      expect(reservedActivationPriority).toBe(0);
    } finally {
      stopping = runner.onApplicationShutdown();
      sixtySecondAdmissionGate.resolve();
      for (const completion of completions) completion.resolve();
      await stopping;
    }

    expect(close).toHaveBeenCalledOnce();
  });

  it('WRK-04 aborts new admissions, drains in-flight completion, then closes runtime', async () => {
    const completion = deferred();
    const secondAdmission = deferred();
    const lifecycle: string[] = [];
    let observedSignal: AbortSignal | undefined;
    let admissions = 0;
    const beginOnce = vi.fn<BeginOnce>(async (_minimumPriority, signal) => {
      admissions += 1;
      observedSignal = signal;
      if (admissions === 1) {
        return {
          priority: 10,
          completion: completion.promise.then(() => {
            lifecycle.push('completion');
          }),
        };
      }
      secondAdmission.resolve();
      return null;
    });
    const close = vi.fn(async () => {
      lifecycle.push('close');
    });
    const runner = createRunner(beginOnce, close);
    let stopping: Promise<void> | undefined;

    runner.onApplicationBootstrap();
    try {
      await within(secondAdmission.promise, 'runner idle poll after the in-flight admission');
      const callsBeforeStop = beginOnce.mock.calls.length;
      let stopped = false;
      stopping = runner.onApplicationShutdown().then(() => {
        stopped = true;
      });

      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(observedSignal?.aborted).toBe(true);
      expect(beginOnce).toHaveBeenCalledTimes(callsBeforeStop);
      expect(stopped).toBe(false);
      expect(close).not.toHaveBeenCalled();

      completion.resolve();
      await stopping;

      expect(stopped).toBe(true);
      expect(lifecycle).toEqual(['completion', 'close']);
      expect(close).toHaveBeenCalledOnce();
    } finally {
      completion.resolve();
      if (stopping === undefined) {
        await runner.onApplicationShutdown();
      } else {
        await stopping;
      }
    }
  });

  it('WRK-04 discards a prefetched permit on shutdown and waits for the active admission', async () => {
    const activeAdmissionGate = deferred();
    const activeAdmissionStarted = deferred();
    const pendingPermitPrepared = deferred();
    const lifecycle: string[] = [];
    let preparations = 0;
    let activations = 0;
    let observedSignal: AbortSignal | undefined;
    const processor = {
      preparePermit: async (
        _minimumPriority: number,
        signal?: AbortSignal,
      ): Promise<PacedPermit | null> => {
        preparations += 1;
        observedSignal = signal;
        const permit = {
          reservationId: `00000000-0000-4000-8000-${String(preparations).padStart(12, '0')}`,
          scheduledMonotonicMs: preparations,
        };
        if (preparations === 2) pendingPermitPrepared.resolve();
        return permit;
      },
      beginWithPermit: async (): Promise<StartedDispatch | null> => {
        activations += 1;
        activeAdmissionStarted.resolve();
        await activeAdmissionGate.promise;
        lifecycle.push('admission');
        return null;
      },
    };
    const close = vi.fn(async () => {
      lifecycle.push('close');
    });
    const runner = createSplitRunner(processor, close);

    runner.onApplicationBootstrap();
    await within(activeAdmissionStarted.promise, 'active admission');
    await within(pendingPermitPrepared.promise, 'prefetched permit');
    let stopped = false;
    const stopping = runner.onApplicationShutdown().then(() => {
      stopped = true;
    });

    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(observedSignal?.aborted).toBe(true);
    expect(stopped).toBe(false);
    expect(activations).toBe(1);
    expect(preparations).toBe(2);
    expect(close).not.toHaveBeenCalled();

    activeAdmissionGate.resolve();
    await stopping;

    expect(activations).toBe(1);
    expect(lifecycle).toEqual(['admission', 'close']);
    expect(close).toHaveBeenCalledOnce();
  });

  it('ERR-08 isolates an unexpected admission exception and continues with the next delivery', async () => {
    vi.useFakeTimers();
    const loggerError = vi.spyOn(Logger.prototype, 'error').mockImplementation(() => undefined);
    const sensitiveFailure = 'phone=010-1234-5678 body=must-not-leak';
    try {
      const completion = deferred();
      const nextDeliveryAdmitted = deferred();
      let admissions = 0;
      const beginOnce = vi.fn<BeginOnce>(async (_minimumPriority, signal) => {
        admissions += 1;
        if (signal?.aborted === true) {
          return null;
        }
        if (admissions === 1) {
          throw new Error(sensitiveFailure);
        }
        if (admissions === 2) {
          nextDeliveryAdmitted.resolve();
          return { priority: 10, completion: completion.promise };
        }
        return null;
      });
      const close = vi.fn(async () => undefined);
      const runner = createRunner(beginOnce, close);

      runner.onApplicationBootstrap();
      let stopping: Promise<void> | undefined;
      try {
        await vi.advanceTimersByTimeAsync(249);
        expect(admissions).toBe(1);

        await vi.advanceTimersByTimeAsync(1);
        await nextDeliveryAdmitted.promise;
        expect(admissions).toBeGreaterThanOrEqual(2);
      } finally {
        stopping = runner.onApplicationShutdown();
        completion.resolve();
        await stopping;
      }

      expect(close).toHaveBeenCalledOnce();
      expect(loggerError).toHaveBeenCalledWith({
        eventName: 'DISPATCH_ADMISSION_FAILED',
        processInstanceId: PROCESS_INSTANCE_ID,
        workerId: PROCESS_INSTANCE_ID,
        errorName: 'Error',
      });
      expect(JSON.stringify(loggerError.mock.calls)).not.toContain(sensitiveFailure);
    } finally {
      loggerError.mockRestore();
      vi.useRealTimers();
    }
  });

  it('WRK-06 fails closed for an exact 10-second DB outage, then recovers with bounded exponential backoff', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const loggerError = vi.spyOn(Logger.prototype, 'error').mockImplementation(() => undefined);
    try {
      const completion = deferred();
      const recovered = deferred();
      const resetRetry = deferred();
      const outageAttempts: number[] = [];
      let successfulAdmissions = 0;
      let injectedResetFailure = false;
      let resetFailureAt: number | null = null;
      let resetRetryAt: number | null = null;
      let permitSequence = 0;
      const processor = {
        preparePermit: vi.fn(
          async (_minimumPriority: number, signal?: AbortSignal): Promise<PacedPermit | null> => {
            if (signal?.aborted === true) {
              return null;
            }
            const now = Date.now();
            if (now < 10_000) {
              outageAttempts.push(now);
              throw new Error('database unavailable');
            }
            if (successfulAdmissions > 0 && !injectedResetFailure) {
              injectedResetFailure = true;
              resetFailureAt = now;
              throw new Error('database unavailable after recovery');
            }
            if (injectedResetFailure) {
              resetRetryAt = now;
              resetRetry.resolve();
              return null;
            }
            permitSequence += 1;
            return {
              reservationId: `00000000-0000-4000-8000-${String(permitSequence).padStart(12, '0')}`,
              scheduledMonotonicMs: now,
            };
          },
        ),
        beginWithPermit: vi.fn(
          async (
            _permit: PacedPermit,
            _minimumPriority: number,
            signal?: AbortSignal,
          ): Promise<StartedDispatch | null> => {
            if (signal?.aborted === true) {
              return null;
            }
            successfulAdmissions += 1;
            recovered.resolve();
            return { priority: 10, completion: completion.promise };
          },
        ),
      };
      const close = vi.fn(async () => undefined);
      const runner = createSplitRunner(processor, close);

      runner.onApplicationBootstrap();
      let stopping: Promise<void> | undefined;
      try {
        await vi.advanceTimersByTimeAsync(12_749);
        expect(successfulAdmissions).toBe(0);
        expect(outageAttempts).toEqual([0, 250, 750, 1_750, 3_750, 7_750]);

        await vi.advanceTimersByTimeAsync(1);
        expect(successfulAdmissions).toBe(1);
        await recovered.promise;
        expect(resetFailureAt).toBe(12_750);

        await vi.advanceTimersByTimeAsync(249);
        expect(resetRetryAt).toBeNull();
        await vi.advanceTimersByTimeAsync(1);
        await resetRetry.promise;
        expect(resetRetryAt).toBe(13_000);
      } finally {
        stopping = runner.onApplicationShutdown();
        completion.resolve();
        await stopping;
      }

      expect(close).toHaveBeenCalledOnce();
    } finally {
      loggerError.mockRestore();
      vi.useRealTimers();
    }
  });

  it('resets prior backoff after a fulfilled null activation before a new isolated failure', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const loggerError = vi.spyOn(Logger.prototype, 'error').mockImplementation(() => undefined);
    try {
      const preparationTimes: number[] = [];
      const retryObserved = deferred();
      let preparations = 0;
      let activations = 0;
      const processor = {
        preparePermit: async (): Promise<PacedPermit | null> => {
          preparations += 1;
          preparationTimes.push(Date.now());
          if (preparations <= 2 || preparations === 5) {
            throw new Error('database unavailable');
          }
          if (preparations === 6) {
            retryObserved.resolve();
            return null;
          }
          return {
            reservationId: `00000000-0000-4000-8000-${String(preparations).padStart(12, '0')}`,
            scheduledMonotonicMs: Date.now(),
          };
        },
        beginWithPermit: async (): Promise<StartedDispatch | null> => {
          activations += 1;
          return null;
        },
      };
      const close = vi.fn(async () => undefined);
      const runner = createSplitRunner(processor, close);

      runner.onApplicationBootstrap();
      await vi.advanceTimersByTimeAsync(750);
      expect(preparationTimes).toEqual([0, 250, 750, 750]);
      expect(activations).toBe(1);

      await vi.advanceTimersByTimeAsync(49);
      expect(preparations).toBe(4);
      await vi.advanceTimersByTimeAsync(1);
      expect(preparationTimes.at(-1)).toBe(800);

      await vi.advanceTimersByTimeAsync(249);
      expect(preparations).toBe(5);
      await vi.advanceTimersByTimeAsync(1);
      await retryObserved.promise;
      expect(preparationTimes.at(-1)).toBe(1_050);

      await runner.onApplicationShutdown();
      expect(close).toHaveBeenCalledOnce();
    } finally {
      loggerError.mockRestore();
      vi.useRealTimers();
    }
  });

  it('ERR-08 observes an unexpected started completion rejection and continues with the next delivery', async () => {
    const failedCompletion = rejectableDeferred();
    const firstDeliveryAdmitted = deferred();
    const idleProbeCompleted = deferred();
    const nextDeliveryAdmitted = deferred();
    const nextCompletion = deferred();
    const sensitiveFailure = 'recipient=private-user phone=010-9999-9999';
    const completionFailure = new Error(sensitiveFailure);
    const loggerError = vi.spyOn(Logger.prototype, 'error').mockImplementation(() => undefined);
    let admissions = 0;
    const beginOnce = vi.fn<BeginOnce>(async (_minimumPriority, signal) => {
      if (signal?.aborted === true) {
        return null;
      }
      admissions += 1;
      if (admissions === 1) {
        firstDeliveryAdmitted.resolve();
        return { priority: 10, completion: failedCompletion.promise };
      }
      if (admissions === 2) {
        idleProbeCompleted.resolve();
        return null;
      }
      if (admissions === 3) {
        nextDeliveryAdmitted.resolve();
        return { priority: 10, completion: nextCompletion.promise };
      }
      return null;
    });
    const close = vi.fn(async () => undefined);
    const runner = createRunner(beginOnce, close);

    runner.onApplicationBootstrap();
    let stopping: Promise<void> | undefined;
    try {
      await within(firstDeliveryAdmitted.promise, 'first delivery admission');
      await within(idleProbeCompleted.promise, 'idle probe after first delivery admission');

      failedCompletion.reject(completionFailure);
      await vi.waitFor(() => {
        expect(loggerError).toHaveBeenCalledWith({
          eventName: 'DISPATCH_RESULT_COMMIT_FAILED',
          processInstanceId: PROCESS_INSTANCE_ID,
          workerId: PROCESS_INSTANCE_ID,
          errorName: 'Error',
        });
      });
      expect(JSON.stringify(loggerError.mock.calls)).not.toContain(sensitiveFailure);

      await within(nextDeliveryAdmitted.promise, 'next delivery after completion rejection');
      expect(admissions).toBeGreaterThanOrEqual(3);
    } finally {
      stopping = runner.onApplicationShutdown();
      failedCompletion.resolve();
      nextCompletion.resolve();
      await stopping;
      loggerError.mockRestore();
    }

    expect(close).toHaveBeenCalledOnce();
  });

  it('observes a started completion rejection before slow speculative preparation settles', async () => {
    const slowPreparationGate = deferred();
    const slowPreparationStarted = deferred();
    const sensitiveFailure = 'recipient=private-user body=must-not-leak';
    const loggerError = vi.spyOn(Logger.prototype, 'error').mockImplementation(() => undefined);
    let preparations = 0;
    const processor = {
      preparePermit: async (): Promise<PacedPermit | null> => {
        preparations += 1;
        if (preparations === 1) {
          return {
            reservationId: '11111111-1111-4111-8111-111111111111',
            scheduledMonotonicMs: 1,
          };
        }
        if (preparations === 2) {
          slowPreparationStarted.resolve();
          await slowPreparationGate.promise;
        }
        return null;
      },
      beginWithPermit: async (): Promise<StartedDispatch | null> => ({
        priority: 10,
        completion: Promise.reject(new Error(sensitiveFailure)),
      }),
    };
    const close = vi.fn(async () => undefined);
    const runner = createSplitRunner(processor, close);

    runner.onApplicationBootstrap();
    try {
      await within(slowPreparationStarted.promise, 'slow speculative preparation');
      await vi.waitFor(() => {
        expect(loggerError).toHaveBeenCalledWith({
          eventName: 'DISPATCH_RESULT_COMMIT_FAILED',
          processInstanceId: PROCESS_INSTANCE_ID,
          workerId: PROCESS_INSTANCE_ID,
          errorName: 'Error',
        });
      });
      expect(JSON.stringify(loggerError.mock.calls)).not.toContain(sensitiveFailure);
    } finally {
      slowPreparationGate.resolve();
      await runner.onApplicationShutdown();
      loggerError.mockRestore();
    }

    expect(close).toHaveBeenCalledOnce();
  });
});
