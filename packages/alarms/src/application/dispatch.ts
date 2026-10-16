import type { AlarmType } from '../domain/alarm';

export interface PacedPermit {
  readonly reservationId: string;
  readonly scheduledMonotonicMs: number;
}

export interface PacedRateLimiter {
  reserve(signal?: AbortSignal): Promise<PacedPermit | null>;
}

export interface DispatchClaim {
  readonly alarmId: string;
  readonly alarmType: AlarmType;
  readonly alarmBody: string;
  readonly deliveryId: string;
  readonly recipientId: string;
  readonly clientRef: string;
  readonly leaseGeneration: number;
  readonly leaseToken: string;
}

export interface DispatchAttempt extends DispatchClaim {
  readonly attemptId: string;
  readonly attemptNo: number;
  readonly reservationId: string;
}

export type ProviderSendResult =
  | { readonly kind: 'ACCEPTED'; readonly messageId: string }
  | { readonly kind: 'DEFINITELY_NOT_SENT'; readonly code: string }
  | { readonly kind: 'PERMANENT_FAILURE'; readonly code: string; readonly httpStatus: 400 }
  | {
      readonly kind: 'RATE_LIMITED';
      readonly code: string;
      readonly httpStatus: 429;
      readonly retryAfterMs: number;
    }
  | {
      readonly kind: 'TRANSIENT_FAILURE';
      readonly code: string;
      readonly httpStatus: 500 | 503;
    }
  | { readonly kind: 'AMBIGUOUS'; readonly code: string };

export interface ProviderMessageSender {
  send(
    request: {
      readonly alarmId: string;
      readonly recipientId: string;
      readonly body: string;
      readonly clientRef: string;
    },
    signal?: AbortSignal,
  ): Promise<ProviderSendResult>;
}

export interface DispatchRepository {
  hasEligible(minimumPriority: number): Promise<boolean>;
  claimNext(minimumPriority: number, leaseDurationMs: number): Promise<DispatchClaim | null>;
  startAttempt(claim: DispatchClaim, permit: PacedPermit): Promise<DispatchAttempt | null>;
  commitAttempt(attempt: DispatchAttempt): Promise<boolean>;
  heartbeat(attempt: DispatchAttempt, leaseDurationMs: number): Promise<boolean>;
  finishNotSent(attempt: DispatchAttempt): Promise<void>;
  finish(attempt: DispatchAttempt, result: ProviderSendResult): Promise<void>;
}

export interface DispatchLifecycleEvent {
  readonly eventName: 'ATTEMPT_STARTED' | 'ATTEMPT_NOT_SENT' | 'ATTEMPT_FINISHED';
  readonly traceId: string;
  readonly alarmId: string;
  readonly deliveryId: string;
  readonly attemptId: string;
  readonly clientRef: string;
  readonly attemptNo: number;
  readonly leaseGeneration: number;
  readonly reservationId: string;
  readonly outcome?: ProviderSendResult['kind'] | 'NOT_SENT';
  readonly providerCode?: string;
  readonly httpStatus?: number;
  readonly providerDurationMs?: number;
  readonly providerDurationSource?: 'MONOTONIC_SENDER_BOUNDARY';
}

export interface DispatchEventSink {
  record(event: DispatchLifecycleEvent): void;
}

export interface DispatchHeartbeatHandle {
  stop(): Promise<void>;
}

export interface DispatchHeartbeatScheduler {
  start(intervalMs: number, heartbeat: () => Promise<boolean>): DispatchHeartbeatHandle;
}

export interface DispatchProcessorOptions {
  readonly leaseDurationMs?: number;
  readonly heartbeatIntervalMs?: number;
  readonly allowedLateMs?: number;
  readonly monotonicNow: () => number;
  readonly waitUntil: (targetMonotonicMs: number, signal?: AbortSignal) => Promise<void>;
  readonly heartbeatScheduler: DispatchHeartbeatScheduler;
  readonly eventSink?: DispatchEventSink;
}

const NOOP_EVENT_SINK: DispatchEventSink = { record: () => undefined };

function isAborted(signal?: AbortSignal): boolean {
  return signal?.aborted ?? false;
}

export interface StartedDispatch {
  readonly priority: number;
  readonly completion: Promise<void>;
}

export class DispatchProcessor {
  private readonly leaseDurationMs: number;
  private readonly heartbeatIntervalMs: number;
  private readonly allowedLateMs: number;
  private readonly monotonicNow: () => number;
  private readonly waitUntil: (targetMonotonicMs: number, signal?: AbortSignal) => Promise<void>;
  private readonly heartbeatScheduler: DispatchHeartbeatScheduler;
  private readonly eventSink: DispatchEventSink;

  public constructor(
    private readonly repository: DispatchRepository,
    private readonly limiter: PacedRateLimiter,
    private readonly sender: ProviderMessageSender,
    options: DispatchProcessorOptions,
  ) {
    this.leaseDurationMs = options.leaseDurationMs ?? 60_000;
    this.heartbeatIntervalMs = options.heartbeatIntervalMs ?? 10_000;
    this.allowedLateMs = options.allowedLateMs ?? 5;
    this.monotonicNow = options.monotonicNow;
    this.waitUntil = options.waitUntil;
    this.heartbeatScheduler = options.heartbeatScheduler;
    this.eventSink = options.eventSink ?? NOOP_EVENT_SINK;

    if (!Number.isSafeInteger(this.leaseDurationMs) || this.leaseDurationMs < 3) {
      throw new RangeError('leaseDurationMs must be a safe integer of at least 3 milliseconds');
    }
    if (
      !Number.isSafeInteger(this.heartbeatIntervalMs) ||
      this.heartbeatIntervalMs < 1 ||
      this.heartbeatIntervalMs > Math.floor(this.leaseDurationMs / 3)
    ) {
      throw new RangeError('heartbeatIntervalMs must be between 1 and leaseDurationMs / 3');
    }
  }

  private startHeartbeat(attempt: DispatchAttempt): () => Promise<void> {
    const handle = this.heartbeatScheduler.start(this.heartbeatIntervalMs, () =>
      this.repository.heartbeat(attempt, this.leaseDurationMs),
    );
    return () => handle.stop();
  }

  private record(
    attempt: DispatchAttempt,
    details: Pick<DispatchLifecycleEvent, 'eventName'> &
      Partial<
        Pick<
          DispatchLifecycleEvent,
          | 'outcome'
          | 'providerCode'
          | 'httpStatus'
          | 'providerDurationMs'
          | 'providerDurationSource'
        >
      >,
  ): void {
    try {
      this.eventSink.record({
        ...details,
        traceId: attempt.attemptId,
        alarmId: attempt.alarmId,
        deliveryId: attempt.deliveryId,
        attemptId: attempt.attemptId,
        clientRef: attempt.clientRef,
        attemptNo: attempt.attemptNo,
        leaseGeneration: attempt.leaseGeneration,
        reservationId: attempt.reservationId,
      });
    } catch {
      // Observability must never change the durable delivery state machine.
    }
  }

  public async beginOnce(
    minimumPriority: number,
    signal?: AbortSignal,
  ): Promise<StartedDispatch | null> {
    const permit = await this.preparePermit(minimumPriority, signal);
    if (permit === null) {
      return null;
    }

    return this.beginWithPermit(permit, minimumPriority, signal);
  }

  public async preparePermit(
    minimumPriority: number,
    signal?: AbortSignal,
  ): Promise<PacedPermit | null> {
    if (isAborted(signal)) {
      return null;
    }

    if (!(await this.repository.hasEligible(minimumPriority)) || isAborted(signal)) {
      return null;
    }

    return this.limiter.reserve(signal);
  }

  public async beginWithPermit(
    permit: PacedPermit,
    minimumPriority: number,
    signal?: AbortSignal,
  ): Promise<StartedDispatch | null> {
    if (
      isAborted(signal) ||
      this.monotonicNow() > permit.scheduledMonotonicMs + this.allowedLateMs
    ) {
      return null;
    }

    const claim = await this.repository.claimNext(minimumPriority, this.leaseDurationMs);
    if (claim === null) {
      return null;
    }

    const attempt = await this.repository.startAttempt(claim, permit);
    if (attempt === null) {
      return null;
    }
    this.record(attempt, { eventName: 'ATTEMPT_STARTED' });

    await this.waitUntil(permit.scheduledMonotonicMs, signal);
    if (
      isAborted(signal) ||
      this.monotonicNow() > permit.scheduledMonotonicMs + this.allowedLateMs
    ) {
      await this.repository.finishNotSent(attempt);
      this.record(attempt, { eventName: 'ATTEMPT_NOT_SENT', outcome: 'NOT_SENT' });
      return null;
    }

    if (!(await this.repository.commitAttempt(attempt))) {
      await this.repository.finishNotSent(attempt);
      this.record(attempt, { eventName: 'ATTEMPT_NOT_SENT', outcome: 'NOT_SENT' });
      return null;
    }

    const stopHeartbeat = this.startHeartbeat(attempt);
    const completion = Promise.resolve()
      .then(async () => {
        let result: ProviderSendResult;
        const providerStartedAtMs = this.monotonicNow();
        let providerDurationMs = 0;
        try {
          result = await this.sender.send({
            alarmId: attempt.alarmId,
            recipientId: attempt.recipientId,
            body: attempt.alarmBody,
            clientRef: attempt.clientRef,
          });
        } catch {
          result = { kind: 'AMBIGUOUS', code: 'PROVIDER_ADAPTER_REJECTED' };
        } finally {
          const elapsed = this.monotonicNow() - providerStartedAtMs;
          providerDurationMs = Number.isFinite(elapsed) ? Math.max(0, elapsed) : 0;
        }
        await this.repository.finish(attempt, result);
        this.record(attempt, {
          eventName:
            result.kind === 'DEFINITELY_NOT_SENT' ? 'ATTEMPT_NOT_SENT' : 'ATTEMPT_FINISHED',
          outcome: result.kind,
          providerCode: result.kind === 'ACCEPTED' ? 'ACCEPTED' : result.code,
          providerDurationMs,
          providerDurationSource: 'MONOTONIC_SENDER_BOUNDARY',
          ...('httpStatus' in result ? { httpStatus: result.httpStatus } : {}),
        });
      })
      .finally(stopHeartbeat);

    return {
      priority: attempt.alarmType === 'EMERGENCY' ? 100 : 10,
      completion,
    };
  }
}
