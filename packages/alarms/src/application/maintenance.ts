export interface AlarmCounterValues {
  readonly total: number;
  readonly sent: number;
  readonly failed: number;
  readonly canceled: number;
  readonly active: number;
  readonly unresolved: number;
  readonly duplicateCount: number;
  readonly incidentCount: number;
}

export interface AlarmCounterInconsistency {
  readonly alarmId: string;
  readonly stored: AlarmCounterValues;
  readonly observed: AlarmCounterValues;
}

export interface OperationalSnapshot {
  readonly observedAt: string;
  readonly windowSeconds: number;
  readonly queueDepth: number;
  readonly dueQueueDepth: number;
  readonly oldestDueAgeMs: number | null;
  readonly sent: number;
  readonly failed: number;
  readonly retryWait: number;
  readonly unknown: number;
  readonly unresolved: number;
  readonly activeSendLeases: number;
  readonly providerAttemptsWindow: number;
  readonly providerAcceptedWindow: number;
  readonly providerRateLimitedWindow: number;
  readonly providerThroughputPerSecond: number;
}

export type SendLeaseRecoveryReason =
  | 'CLAIM_EXPIRED_REQUEUED'
  | 'CLAIM_EXPIRED_CANCELED'
  | 'PREPARED_EXPIRED_REQUEUED'
  | 'PREPARED_EXPIRED_CANCELED'
  | 'COMMITTED_EXPIRED_TO_UNKNOWN'
  | 'IN_FLIGHT_ATTEMPT_MISSING_TO_UNKNOWN';

export interface SendLeaseRecoveryEvent {
  readonly alarmId: string;
  readonly deliveryId: string;
  readonly attemptId: string | null;
  readonly leaseGeneration: number;
  readonly previousState: 'CLAIMED' | 'IN_FLIGHT';
  readonly requestPhase: 'PREPARED' | 'COMMITTED' | 'FINISHED_NOT_SENT' | 'FINISHED' | null;
  readonly reason: SendLeaseRecoveryReason;
  readonly resultingState: 'READY' | 'CANCELED' | 'UNKNOWN';
}

export interface MaintenanceRepository {
  sweepCanceledDeliveries(batchSize: number): Promise<number>;
  reapExpiredSendLease(): Promise<SendLeaseRecoveryEvent | null>;
  adoptUnconsumedEvidence(): Promise<boolean>;
  cleanupExpansionBatch(batchSize: number, leaseDurationMs: number): Promise<boolean>;
  findCounterInconsistencies(limit: number): Promise<readonly AlarmCounterInconsistency[]>;
  readOperationalSnapshot(windowSeconds: number): Promise<OperationalSnapshot | null>;
}

export interface MaintenanceEventSink {
  recordCounterInconsistency(issue: AlarmCounterInconsistency): void;
  recordOperationalSnapshot(snapshot: OperationalSnapshot): void;
  recordSendLeaseRecovery(event: SendLeaseRecoveryEvent): void;
}

export interface MaintenanceProcessorOptions {
  readonly cancelBatchSize?: number;
  readonly cleanupBatchSize?: number;
  readonly cleanupLeaseDurationMs?: number;
  readonly consistencyBatchSize?: number;
  readonly consistencyCheckEveryIdleRuns?: number;
  readonly snapshotIntervalMs?: number;
  readonly snapshotWindowSeconds?: number;
  readonly monotonicNow: () => number;
  readonly eventSink?: MaintenanceEventSink;
}

const NOOP_EVENT_SINK: MaintenanceEventSink = {
  recordCounterInconsistency: () => undefined,
  recordOperationalSnapshot: () => undefined,
  recordSendLeaseRecovery: () => undefined,
};

export class MaintenanceProcessor {
  private readonly cancelBatchSize: number;
  private readonly cleanupBatchSize: number;
  private readonly cleanupLeaseDurationMs: number;
  private readonly consistencyBatchSize: number;
  private readonly consistencyCheckEveryIdleRuns: number;
  private readonly snapshotIntervalMs: number;
  private readonly snapshotWindowSeconds: number;
  private readonly monotonicNow: () => number;
  private readonly eventSink: MaintenanceEventSink;
  private nextSnapshotAtMs = Number.NEGATIVE_INFINITY;
  private idleRunsUntilConsistencyCheck = 0;
  private reportedInconsistencies = new Map<string, string>();
  private snapshotRequested = false;
  private consistencyCheckRequested = false;
  private readonly pendingRecoveryEvents: SendLeaseRecoveryEvent[] = [];
  private observationTask: Promise<void> | null = null;
  private observationsStopping = false;

  public constructor(
    private readonly repository: MaintenanceRepository,
    options: MaintenanceProcessorOptions,
  ) {
    this.cancelBatchSize = options.cancelBatchSize ?? 500;
    this.cleanupBatchSize = options.cleanupBatchSize ?? 1_000;
    this.cleanupLeaseDurationMs = options.cleanupLeaseDurationMs ?? 30_000;
    this.consistencyBatchSize = options.consistencyBatchSize ?? 100;
    this.consistencyCheckEveryIdleRuns = Math.max(1, options.consistencyCheckEveryIdleRuns ?? 300);
    this.snapshotIntervalMs = Math.max(1_000, options.snapshotIntervalMs ?? 60_000);
    this.snapshotWindowSeconds = Math.max(1, options.snapshotWindowSeconds ?? 60);
    this.monotonicNow = options.monotonicNow;
    this.eventSink = options.eventSink ?? NOOP_EVENT_SINK;
  }

  public async runOnce(): Promise<boolean> {
    this.requestOperationalSnapshotIfDue();

    const canceled = await this.repository.sweepCanceledDeliveries(this.cancelBatchSize);
    if (canceled > 0) {
      return true;
    }
    const recoveredLease = await this.repository.reapExpiredSendLease();
    if (recoveredLease !== null) {
      this.pendingRecoveryEvents.push(recoveredLease);
      this.startObservationTask();
      return true;
    }
    if (await this.repository.adoptUnconsumedEvidence()) {
      return true;
    }
    if (
      await this.repository.cleanupExpansionBatch(
        this.cleanupBatchSize,
        this.cleanupLeaseDurationMs,
      )
    ) {
      return true;
    }

    if (this.idleRunsUntilConsistencyCheck > 0) {
      this.idleRunsUntilConsistencyCheck -= 1;
      return false;
    }
    this.idleRunsUntilConsistencyCheck = this.consistencyCheckEveryIdleRuns - 1;
    this.consistencyCheckRequested = true;
    this.startObservationTask();
    return false;
  }

  /**
   * Wait for the currently requested diagnostic work without disabling future cadence ticks.
   * This is intended for deterministic tests and explicit runtime drains, never the durable loop.
   */
  public async drainObservability(): Promise<void> {
    while (this.observationTask !== null) {
      await this.observationTask;
    }
  }

  /**
   * Stop accepting new diagnostic work and wait for the one bounded in-flight observation.
   * Durable maintenance never awaits this path; the runtime calls it only while shutting down,
   * before releasing the repository's advisory-leader connection and pool.
   */
  public async stopObservability(): Promise<void> {
    this.observationsStopping = true;
    await this.drainObservability();
  }

  private requestOperationalSnapshotIfDue(): void {
    if (this.observationsStopping) {
      return;
    }
    const now = this.monotonicNow();
    if (now < this.nextSnapshotAtMs) {
      return;
    }
    // Advance before I/O so a failed sink/query cannot create a busy retry loop.
    this.nextSnapshotAtMs = now + this.snapshotIntervalMs;
    this.snapshotRequested = true;
    this.startObservationTask();
  }

  private startObservationTask(): void {
    if (
      this.observationsStopping ||
      this.observationTask !== null ||
      (!this.snapshotRequested &&
        !this.consistencyCheckRequested &&
        this.pendingRecoveryEvents.length === 0)
    ) {
      return;
    }

    const task = Promise.resolve()
      .then(() => this.runRequestedObservations())
      .catch(() => {
        // Observability is diagnostic only; a repository or sink failure must not reject a worker
        // loop promise or mutate the durable state machine.
      });
    this.observationTask = task;
    void task.then(() => {
      if (this.observationTask === task) {
        this.observationTask = null;
      }
      this.startObservationTask();
    });
  }

  private async runRequestedObservations(): Promise<void> {
    while (
      this.snapshotRequested ||
      this.consistencyCheckRequested ||
      this.pendingRecoveryEvents.length > 0
    ) {
      const snapshotRequested = this.snapshotRequested;
      const consistencyCheckRequested = this.consistencyCheckRequested;
      const recoveryEvents = this.pendingRecoveryEvents.splice(0);
      this.snapshotRequested = false;
      this.consistencyCheckRequested = false;

      for (const event of recoveryEvents) {
        try {
          this.eventSink.recordSendLeaseRecovery(event);
        } catch {
          // A logging failure cannot roll back the already committed recovery.
        }
      }
      if (snapshotRequested) {
        await this.emitOperationalSnapshot();
      }
      if (consistencyCheckRequested) {
        await this.reportCounterInconsistencies().catch(() => {
          // The next idle cadence retries after a bounded delay.
        });
      }
    }
  }

  private async emitOperationalSnapshot(): Promise<void> {
    try {
      const snapshot = await this.repository.readOperationalSnapshot(this.snapshotWindowSeconds);
      if (snapshot !== null) {
        this.eventSink.recordOperationalSnapshot(snapshot);
      }
    } catch {
      // Metrics are diagnostic only; state-machine work must keep making progress.
    }
  }

  private async reportCounterInconsistencies(): Promise<void> {
    const issues = await this.repository.findCounterInconsistencies(this.consistencyBatchSize);
    const currentFingerprints = new Map<string, string>();
    for (const issue of issues) {
      const fingerprint = JSON.stringify({ stored: issue.stored, observed: issue.observed });
      currentFingerprints.set(issue.alarmId, fingerprint);
      if (this.reportedInconsistencies.get(issue.alarmId) === fingerprint) {
        continue;
      }
      try {
        this.eventSink.recordCounterInconsistency(issue);
      } catch {
        // Observability must never mutate or interrupt the durable state machine.
      }
    }
    this.reportedInconsistencies = currentFingerprints;
  }
}
