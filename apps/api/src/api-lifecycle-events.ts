import {
  PROCESS_INSTANCE_ID,
  type SafeErrorMetadata,
  safeErrorMetadata,
} from '@usall-backend-assignment/platform';

export type ApiShutdownSignal = 'SIGINT' | 'SIGTERM';

type ApiLifecycleEvent =
  | { readonly eventName: 'API_STARTED'; readonly port: number }
  | {
      readonly eventName: 'API_READINESS_CHANGED';
      readonly dependency: 'postgresql';
      readonly readiness: 'READY';
    }
  | {
      readonly eventName: 'API_READINESS_CHANGED';
      readonly dependency: 'postgresql';
      readonly readiness: 'NOT_READY';
      readonly errorName: string;
      readonly errorCode?: string;
    }
  | { readonly eventName: 'API_SHUTDOWN_REQUESTED'; readonly signal: ApiShutdownSignal }
  | { readonly eventName: 'API_STOPPED'; readonly shutdownDurationMs: number }
  | {
      readonly eventName: 'API_STARTUP_FAILED' | 'API_SHUTDOWN_FAILED';
      readonly errorName: string;
      readonly errorCode?: string;
    };

export interface ApiLifecycleEventSinkOptions {
  readonly instanceId?: string;
  readonly now?: () => Date;
  readonly write?: (line: string) => void;
}

export class ApiLifecycleEventSink {
  private readonly instanceId: string;
  private readonly now: () => Date;
  private readonly write: (line: string) => void;

  public constructor(options: ApiLifecycleEventSinkOptions = {}) {
    this.instanceId = options.instanceId ?? PROCESS_INSTANCE_ID;
    this.now = options.now ?? (() => new Date());
    this.write = options.write ?? ((line) => process.stdout.write(line));
  }

  public record(event: ApiLifecycleEvent): void {
    try {
      this.write(
        `${JSON.stringify({
          timestamp: this.now().toISOString(),
          level:
            event.eventName.endsWith('_FAILED') ||
            (event.eventName === 'API_READINESS_CHANGED' && event.readiness === 'NOT_READY')
              ? 'error'
              : 'info',
          component: 'api',
          processInstanceId: this.instanceId,
          instanceId: this.instanceId,
          ...event,
        })}\n`,
      );
    } catch {
      // A logging failure must not affect HTTP readiness or process lifecycle.
    }
  }

  public recordFailure(
    eventName: 'API_STARTUP_FAILED' | 'API_SHUTDOWN_FAILED',
    metadata: SafeErrorMetadata,
  ): void {
    this.record({ eventName, ...metadata });
  }

  public recordNotReady(error: unknown): void {
    this.record({
      eventName: 'API_READINESS_CHANGED',
      dependency: 'postgresql',
      readiness: 'NOT_READY',
      ...safeErrorMetadata(error),
    });
  }
}

export const API_LIFECYCLE_EVENTS = new ApiLifecycleEventSink();
