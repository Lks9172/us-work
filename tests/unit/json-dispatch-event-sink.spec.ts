import { hostname } from 'node:os';

import { describe, expect, it, vi } from 'vitest';

import {
  type DispatchAttempt,
  type DispatchClaim,
  DispatchProcessor,
  type DispatchRepository,
  type ProviderSendResult,
} from '../../packages/alarms/src/application/dispatch';
import { JsonDispatchEventSink } from '../../packages/alarms/src/infrastructure/observability/json-dispatch-event-sink';

const lifecycleEvent = {
  eventName: 'ATTEMPT_FINISHED' as const,
  traceId: 'attempt-1',
  alarmId: 'alarm-1',
  deliveryId: 'delivery-1',
  attemptId: 'attempt-1',
  clientRef: 'client-ref-1',
  attemptNo: 2,
  leaseGeneration: 3,
  reservationId: 'reservation-1',
  outcome: 'RATE_LIMITED' as const,
  providerCode: 'RATE_LIMITED',
  httpStatus: 429,
};

const sensitiveClaim: DispatchClaim = {
  alarmId: '11111111-1111-4111-8111-111111111111',
  alarmType: 'BULK',
  alarmBody: 'secret-body phone=010-1234-5678',
  deliveryId: '22222222-2222-4222-8222-222222222222',
  recipientId: 'private-recipient-010-9999-9999',
  clientRef: '33333333-3333-4333-8333-333333333333',
  leaseGeneration: 7,
  leaseToken: '44444444-4444-4444-8444-444444444444',
};

const sensitiveAttempt: DispatchAttempt = {
  ...sensitiveClaim,
  attemptId: '55555555-5555-4555-8555-555555555555',
  attemptNo: 3,
  reservationId: '66666666-6666-4666-8666-666666666666',
};

async function captureProductionEvents(
  result: ProviderSendResult,
  commitAttempt = true,
): Promise<{ readonly lines: readonly string[]; readonly sends: number }> {
  const lines: string[] = [];
  let sends = 0;
  const repository: DispatchRepository = {
    hasEligible: async () => true,
    claimNext: async () => sensitiveClaim,
    startAttempt: async () => sensitiveAttempt,
    commitAttempt: async () => commitAttempt,
    heartbeat: async () => true,
    finishNotSent: async () => undefined,
    finish: async () => undefined,
  };
  const processor = new DispatchProcessor(
    repository,
    {
      reserve: async () => ({
        reservationId: sensitiveAttempt.reservationId,
        scheduledMonotonicMs: 100,
      }),
    },
    {
      send: async () => {
        sends += 1;
        return result;
      },
    },
    {
      monotonicNow: () => 100,
      waitUntil: async () => undefined,
      heartbeatScheduler: { start: () => ({ stop: async () => undefined }) },
      eventSink: new JsonDispatchEventSink({
        workerId: 'worker-observability-matrix',
        now: () => new Date('2026-10-15T00:00:00.000Z'),
        write: (line) => lines.push(line),
      }),
    },
  );

  const started = await processor.beginOnce(0);
  await started?.completion;
  return { lines, sends };
}

function recordDefaultWorkerId(Sink: typeof JsonDispatchEventSink): string {
  const lines: string[] = [];
  new Sink({ write: (line) => lines.push(line) }).record(lifecycleEvent);
  const workerId: unknown = (JSON.parse(lines[0] ?? '') as Record<string, unknown>).workerId;
  if (typeof workerId !== 'string') {
    throw new Error('Dispatch log did not contain a workerId');
  }
  return workerId;
}

describe('JsonDispatchEventSink', () => {
  it('writes one correlation-rich JSON record without recipient content', () => {
    const lines: string[] = [];
    const sink = new JsonDispatchEventSink({
      workerId: 'worker-test',
      now: () => new Date('2026-10-15T00:00:00.000Z'),
      write: (line) => lines.push(line),
    });

    sink.record(lifecycleEvent);

    expect(lines).toHaveLength(1);
    expect(JSON.parse(lines[0] ?? '')).toEqual({
      timestamp: '2026-10-15T00:00:00.000Z',
      level: 'info',
      component: 'dispatch-worker',
      processInstanceId: 'worker-test',
      workerId: 'worker-test',
      eventName: 'ATTEMPT_FINISHED',
      traceId: 'attempt-1',
      alarmId: 'alarm-1',
      deliveryId: 'delivery-1',
      attemptId: 'attempt-1',
      clientRef: 'client-ref-1',
      attemptNo: 2,
      leaseGeneration: 3,
      reservationId: 'reservation-1',
      outcome: 'RATE_LIMITED',
      providerCode: 'RATE_LIMITED',
      httpStatus: 429,
    });
    expect(lines[0]).not.toContain('recipientId');
    expect(lines[0]).not.toContain('alarmBody');
  });

  it('MWK-11 keeps one hostname+bootUUID per process and rotates it on process restart', async () => {
    vi.resetModules();
    const firstBoot = await import(
      '../../packages/alarms/src/infrastructure/observability/json-dispatch-event-sink'
    );
    const firstWorkerId = recordDefaultWorkerId(firstBoot.JsonDispatchEventSink);
    const sameProcessWorkerId = recordDefaultWorkerId(firstBoot.JsonDispatchEventSink);

    expect(sameProcessWorkerId).toBe(firstWorkerId);
    const prefix = `${hostname()}:`;
    expect(firstWorkerId.startsWith(prefix)).toBe(true);
    expect(firstWorkerId.slice(prefix.length)).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
    );

    vi.resetModules();
    const restartedBoot = await import(
      '../../packages/alarms/src/infrastructure/observability/json-dispatch-event-sink'
    );
    const restartedWorkerId = recordDefaultWorkerId(restartedBoot.JsonDispatchEventSink);

    expect(restartedWorkerId).not.toBe(firstWorkerId);
    expect(restartedWorkerId.startsWith(prefix)).toBe(true);
  });

  it.each([
    { label: 'success', result: { kind: 'ACCEPTED', messageId: 'message-1' } },
    {
      label: '429',
      result: {
        kind: 'RATE_LIMITED',
        code: 'RATE_LIMITED',
        httpStatus: 429,
        retryAfterMs: 1_000,
      },
    },
    {
      label: '500',
      result: { kind: 'TRANSIENT_FAILURE', code: 'INTERNAL_ERROR', httpStatus: 500 },
    },
    {
      label: '503',
      result: {
        kind: 'TRANSIENT_FAILURE',
        code: 'SERVICE_UNAVAILABLE',
        httpStatus: 503,
      },
    },
    {
      label: 'timeout',
      result: { kind: 'AMBIGUOUS', code: 'PROVIDER_TIMEOUT' },
    },
  ] satisfies ReadonlyArray<{ readonly label: string; readonly result: ProviderSendResult }>)(
    'OBS-01/MCK-16 records bounded PII-free production events for $label',
    async ({ result }) => {
      const { lines, sends } = await captureProductionEvents(result);

      expect(sends).toBe(1);
      expect(lines).toHaveLength(2);
      for (const line of lines) {
        expect(Buffer.byteLength(line, 'utf8')).toBeLessThan(2_048);
        expect(JSON.parse(line)).toMatchObject({
          workerId: 'worker-observability-matrix',
          traceId: sensitiveAttempt.attemptId,
          alarmId: sensitiveAttempt.alarmId,
          deliveryId: sensitiveAttempt.deliveryId,
          attemptId: sensitiveAttempt.attemptId,
          clientRef: sensitiveAttempt.clientRef,
          attemptNo: sensitiveAttempt.attemptNo,
          leaseGeneration: sensitiveAttempt.leaseGeneration,
          reservationId: sensitiveAttempt.reservationId,
        });
        expect(line).not.toContain(sensitiveClaim.recipientId);
        expect(line).not.toContain(sensitiveClaim.alarmBody);
        expect(line).not.toContain('010-1234-5678');
        expect(line).not.toContain('010-9999-9999');
        expect(line).not.toContain('message-1');
      }
      expect(JSON.parse(lines[1] ?? '')).toMatchObject({
        eventName: 'ATTEMPT_FINISHED',
        outcome: result.kind,
        providerDurationMs: 0,
        providerDurationSource: 'MONOTONIC_SENDER_BOUNDARY',
      });
    },
  );

  it('OBS-01 records the cancel commit fence as correlated NOT_SENT without a provider call', async () => {
    const { lines, sends } = await captureProductionEvents(
      { kind: 'ACCEPTED', messageId: 'must-not-send' },
      false,
    );

    expect(sends).toBe(0);
    expect(lines).toHaveLength(2);
    expect(JSON.parse(lines[1] ?? '')).toMatchObject({
      eventName: 'ATTEMPT_NOT_SENT',
      traceId: sensitiveAttempt.attemptId,
      alarmId: sensitiveAttempt.alarmId,
      deliveryId: sensitiveAttempt.deliveryId,
      clientRef: sensitiveAttempt.clientRef,
      attemptNo: sensitiveAttempt.attemptNo,
      leaseGeneration: sensitiveAttempt.leaseGeneration,
      reservationId: sensitiveAttempt.reservationId,
      outcome: 'NOT_SENT',
    });
    expect(lines.join('')).not.toContain('must-not-send');
    expect(lines.join('')).not.toContain(sensitiveClaim.recipientId);
    expect(lines.join('')).not.toContain(sensitiveClaim.alarmBody);
  });
});
