import { describe, expect, it } from 'vitest';

import {
  type AlarmCounterInconsistency,
  MaintenanceProcessor,
  type MaintenanceRepository,
  type OperationalSnapshot,
  type SendLeaseRecoveryEvent,
} from '../../packages/alarms/src/application/maintenance';
import { JsonMaintenanceEventSink } from '../../packages/alarms/src/infrastructure/observability/json-maintenance-event-sink';

const ISSUE: AlarmCounterInconsistency = {
  alarmId: '11111111-1111-4111-8111-111111111111',
  stored: {
    total: 2,
    sent: 1,
    failed: 0,
    canceled: 0,
    active: 1,
    unresolved: 0,
    duplicateCount: 0,
    incidentCount: 0,
  },
  observed: {
    total: 2,
    sent: 0,
    failed: 0,
    canceled: 0,
    active: 2,
    unresolved: 0,
    duplicateCount: 0,
    incidentCount: 0,
  },
};

const SNAPSHOT: OperationalSnapshot = {
  observedAt: '2026-10-15T00:00:00.000Z',
  windowSeconds: 60,
  queueDepth: 12,
  dueQueueDepth: 7,
  oldestDueAgeMs: 2_500,
  sent: 80,
  failed: 3,
  retryWait: 4,
  unknown: 2,
  unresolved: 1,
  activeSendLeases: 5,
  providerAttemptsWindow: 50,
  providerAcceptedWindow: 47,
  providerRateLimitedWindow: 2,
  providerThroughputPerSecond: 47 / 60,
};

const RECOVERY: SendLeaseRecoveryEvent = {
  alarmId: '11111111-1111-4111-8111-111111111111',
  deliveryId: '22222222-2222-4222-8222-222222222222',
  attemptId: '33333333-3333-4333-8333-333333333333',
  leaseGeneration: 4,
  previousState: 'IN_FLIGHT',
  requestPhase: 'COMMITTED',
  reason: 'COMMITTED_EXPIRED_TO_UNKNOWN',
  resultingState: 'UNKNOWN',
};

class FakeMaintenanceRepository implements MaintenanceRepository {
  public issues: readonly AlarmCounterInconsistency[] = [ISSUE];
  public consistencyChecks = 0;
  public snapshot: OperationalSnapshot | null = SNAPSHOT;
  public snapshotReads = 0;
  public recoveredLease: SendLeaseRecoveryEvent | null = null;
  public canceled = 0;

  public async sweepCanceledDeliveries(): Promise<number> {
    return this.canceled;
  }

  public async reapExpiredSendLease(): Promise<SendLeaseRecoveryEvent | null> {
    return this.recoveredLease;
  }

  public async adoptUnconsumedEvidence(): Promise<boolean> {
    return false;
  }

  public async cleanupExpansionBatch(): Promise<boolean> {
    return false;
  }

  public async findCounterInconsistencies(): Promise<readonly AlarmCounterInconsistency[]> {
    this.consistencyChecks += 1;
    return this.issues;
  }

  public async readOperationalSnapshot(): Promise<OperationalSnapshot | null> {
    this.snapshotReads += 1;
    return this.snapshot;
  }
}

const noopSink = {
  recordCounterInconsistency: () => undefined,
  recordOperationalSnapshot: () => undefined,
  recordSendLeaseRecovery: () => undefined,
};

describe('MaintenanceProcessor', () => {
  it('reports each observed counter-drift fingerprint once without treating it as repair work', async () => {
    const repository = new FakeMaintenanceRepository();
    const reported: AlarmCounterInconsistency[] = [];
    const processor = new MaintenanceProcessor(repository, {
      consistencyCheckEveryIdleRuns: 1,
      monotonicNow: () => 0,
      eventSink: {
        ...noopSink,
        recordCounterInconsistency: (issue) => reported.push(issue),
      },
    });

    await expect(processor.runOnce()).resolves.toBe(false);
    await processor.drainObservability();
    await expect(processor.runOnce()).resolves.toBe(false);
    await processor.drainObservability();
    expect(reported).toEqual([ISSUE]);

    repository.issues = [];
    await expect(processor.runOnce()).resolves.toBe(false);
    await processor.drainObservability();
    repository.issues = [ISSUE];
    await expect(processor.runOnce()).resolves.toBe(false);
    await processor.stopObservability();
    expect(reported).toEqual([ISSUE, ISSUE]);
  });

  it('emits one leader snapshot on a monotonic bounded cadence even while work stays busy', async () => {
    const repository = new FakeMaintenanceRepository();
    repository.canceled = 1;
    let now = 0;
    const snapshots: OperationalSnapshot[] = [];
    const processor = new MaintenanceProcessor(repository, {
      monotonicNow: () => now,
      snapshotIntervalMs: 1_000,
      eventSink: {
        ...noopSink,
        recordOperationalSnapshot: (snapshot) => snapshots.push(snapshot),
      },
    });

    await expect(processor.runOnce()).resolves.toBe(true);
    now = 999;
    await expect(processor.runOnce()).resolves.toBe(true);
    now = 1_000;
    await expect(processor.runOnce()).resolves.toBe(true);
    await processor.stopObservability();

    expect(repository.snapshotReads).toBe(2);
    expect(snapshots).toEqual([SNAPSHOT, SNAPSHOT]);
  });

  it('never awaits a slow observation before durable sweep work and coalesces overlapping ticks', async () => {
    const repository = new FakeMaintenanceRepository();
    repository.canceled = 1;
    let now = 0;
    let resolveFirst: ((snapshot: OperationalSnapshot | null) => void) | undefined;
    let concurrentReads = 0;
    let maximumConcurrentReads = 0;
    repository.readOperationalSnapshot = async () => {
      repository.snapshotReads += 1;
      concurrentReads += 1;
      maximumConcurrentReads = Math.max(maximumConcurrentReads, concurrentReads);
      if (repository.snapshotReads === 1) {
        const result = await new Promise<OperationalSnapshot | null>((resolve) => {
          resolveFirst = resolve;
        });
        concurrentReads -= 1;
        return result;
      }
      concurrentReads -= 1;
      return SNAPSHOT;
    };
    const processor = new MaintenanceProcessor(repository, {
      monotonicNow: () => now,
      snapshotIntervalMs: 1_000,
    });

    await expect(processor.runOnce()).resolves.toBe(true);
    now = 1_000;
    await expect(processor.runOnce()).resolves.toBe(true);
    now = 2_000;
    await expect(processor.runOnce()).resolves.toBe(true);
    expect(repository.snapshotReads).toBe(1);

    resolveFirst?.(SNAPSHOT);
    await processor.drainObservability();
    expect(repository.snapshotReads).toBe(2);
    expect(maximumConcurrentReads).toBe(1);
    await processor.stopObservability();
  });

  it('does not let a slow consistency observation delay a later lease sweep', async () => {
    const repository = new FakeMaintenanceRepository();
    repository.snapshot = null;
    let releaseConsistency: (() => void) | undefined;
    repository.findCounterInconsistencies = async () => {
      repository.consistencyChecks += 1;
      await new Promise<void>((resolve) => {
        releaseConsistency = resolve;
      });
      return [];
    };
    const processor = new MaintenanceProcessor(repository, {
      consistencyCheckEveryIdleRuns: 1,
      monotonicNow: () => 0,
    });

    await expect(processor.runOnce()).resolves.toBe(false);
    repository.recoveredLease = RECOVERY;
    await expect(processor.runOnce()).resolves.toBe(true);
    releaseConsistency?.();
    await processor.stopObservability();
  });

  it('keeps maintenance work progressing when snapshot observation fails', async () => {
    const repository = new FakeMaintenanceRepository();
    repository.canceled = 1;
    repository.readOperationalSnapshot = async () => {
      throw new Error('metrics query unavailable');
    };
    const processor = new MaintenanceProcessor(repository, { monotonicNow: () => 0 });

    await expect(processor.runOnce()).resolves.toBe(true);
  });

  it('publishes a lease recovery only after the repository transaction result is committed', async () => {
    const repository = new FakeMaintenanceRepository();
    const order: string[] = [];
    repository.snapshot = null;
    repository.reapExpiredSendLease = async () => {
      order.push('repository-committed');
      return RECOVERY;
    };
    const processor = new MaintenanceProcessor(repository, {
      monotonicNow: () => 0,
      eventSink: {
        ...noopSink,
        recordSendLeaseRecovery: (event) => order.push(`event:${event.reason}`),
      },
    });

    await expect(processor.runOnce()).resolves.toBe(true);
    expect(order).toEqual(['repository-committed', 'event:COMMITTED_EXPIRED_TO_UNKNOWN']);
  });

  it('bounds the source-row scan to one of every 300 idle iterations', async () => {
    const repository = new FakeMaintenanceRepository();
    const processor = new MaintenanceProcessor(repository, {
      consistencyCheckEveryIdleRuns: 300,
      monotonicNow: () => 0,
    });

    for (let iteration = 0; iteration < 300; iteration += 1) {
      await expect(processor.runOnce()).resolves.toBe(false);
    }
    await processor.drainObservability();
    expect(repository.consistencyChecks).toBe(1);

    await expect(processor.runOnce()).resolves.toBe(false);
    await processor.stopObservability();
    expect(repository.consistencyChecks).toBe(2);
  });

  it('writes a bounded structured counter inconsistency alert without recipient content', () => {
    const output: string[] = [];
    const sink = new JsonMaintenanceEventSink({
      workerId: 'maintenance-test-worker',
      now: () => new Date('2026-10-15T00:00:00.000Z'),
      write: (line) => output.push(line),
    });

    sink.recordCounterInconsistency(ISSUE);

    expect(output).toHaveLength(1);
    expect(JSON.parse(output[0] ?? '')).toEqual({
      timestamp: '2026-10-15T00:00:00.000Z',
      level: 'error',
      component: 'maintenance-worker',
      processInstanceId: 'maintenance-test-worker',
      workerId: 'maintenance-test-worker',
      eventName: 'ALARM_COUNTER_INCONSISTENCY',
      ...ISSUE,
    });
    expect(output[0]).not.toContain('recipient');
    expect(output[0]).not.toContain('body');
  });

  it('writes bounded PII-free operational snapshot and recovery records', () => {
    const output: string[] = [];
    const sink = new JsonMaintenanceEventSink({
      workerId: 'maintenance-test-worker',
      now: () => new Date('2026-10-15T00:00:00.000Z'),
      write: (line) => output.push(line),
    });

    sink.recordOperationalSnapshot(SNAPSHOT);
    sink.recordSendLeaseRecovery(RECOVERY);

    expect(JSON.parse(output[0] ?? '')).toMatchObject({
      eventName: 'OPERATIONAL_SNAPSHOT',
      workerId: 'maintenance-test-worker',
      queueDepth: 12,
      providerRateLimitedWindow: 2,
    });
    expect(JSON.parse(output[1] ?? '')).toMatchObject({
      eventName: 'SEND_LEASE_RECOVERED',
      reason: 'COMMITTED_EXPIRED_TO_UNKNOWN',
      resultingState: 'UNKNOWN',
    });
    for (const line of output) {
      expect(Buffer.byteLength(line, 'utf8')).toBeLessThan(2_048);
      expect(line).not.toContain('recipient');
      expect(line).not.toContain('body');
      expect(line).not.toContain('leaseToken');
    }
  });
});
