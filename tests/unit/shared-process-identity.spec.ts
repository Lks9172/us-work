import { EventEmitter } from 'node:events';

import { describe, expect, it } from 'vitest';

import { WorkerLifecycleEventSink } from '../../apps/worker/src/worker-lifecycle-events';
import { JsonDispatchEventSink } from '../../packages/alarms/src/infrastructure/observability/json-dispatch-event-sink';
import { JsonExpansionEventSink } from '../../packages/alarms/src/infrastructure/observability/json-expansion-event-sink';
import { JsonMaintenanceEventSink } from '../../packages/alarms/src/infrastructure/observability/json-maintenance-event-sink';
import { attachPostgresPoolErrorHandler } from '../../packages/alarms/src/infrastructure/persistence/postgres-pool-observability';
import { PROCESS_INSTANCE_ID } from '../../packages/platform/src/process-identity';

describe('worker process identity', () => {
  it('uses one boot identity for lifecycle, dispatch, expansion, maintenance and reconciliation logs', () => {
    const lines: string[] = [];
    const write = (line: string): void => lines.push(line);

    new WorkerLifecycleEventSink({ write }).record({ eventName: 'WORKER_STARTED' });
    new JsonDispatchEventSink({ write }).record({
      eventName: 'ATTEMPT_STARTED',
      traceId: 'attempt-id',
      alarmId: 'alarm-id',
      deliveryId: 'delivery-id',
      attemptId: 'attempt-id',
      clientRef: 'client-ref',
      attemptNo: 1,
      leaseGeneration: 1,
      reservationId: 'reservation-id',
    });
    new JsonExpansionEventSink({ write }).recordDuplicateRecipients({
      alarmId: 'alarm-id',
      runGeneration: 'run-generation',
      leaseGeneration: 1,
      receivedCount: 2,
      insertedCount: 1,
      duplicateCount: 1,
    });
    new JsonMaintenanceEventSink({ write }).recordOperationalSnapshot({
      observedAt: '2026-10-15T00:00:00.000Z',
      windowSeconds: 60,
      queueDepth: 1,
      dueQueueDepth: 1,
      oldestDueAgeMs: 10,
      sent: 1,
      failed: 0,
      retryWait: 0,
      unknown: 0,
      unresolved: 0,
      activeSendLeases: 0,
      providerAttemptsWindow: 1,
      providerAcceptedWindow: 1,
      providerRateLimitedWindow: 0,
      providerThroughputPerSecond: 1 / 60,
    });
    const reconciliationPool = new EventEmitter();
    attachPostgresPoolErrorHandler(reconciliationPool, {
      component: 'reconciliation-worker',
      write,
    });
    reconciliationPool.emit('error', new Error('history database unavailable'));

    const records = lines.map((line) => JSON.parse(line) as Record<string, unknown>);
    expect(records).toHaveLength(5);
    expect(records.map((record) => record.processInstanceId)).toEqual(
      Array.from({ length: 5 }, () => PROCESS_INSTANCE_ID),
    );
    expect(records.slice(0, 4).map((record) => record.workerId)).toEqual(
      Array.from({ length: 4 }, () => PROCESS_INSTANCE_ID),
    );
    expect(records.at(-1)).toMatchObject({
      component: 'reconciliation-worker',
      processInstanceId: PROCESS_INSTANCE_ID,
      eventName: 'POSTGRES_IDLE_CLIENT_ERROR',
    });
  });
});
