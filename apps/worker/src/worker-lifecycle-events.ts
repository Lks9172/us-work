import { PROCESS_INSTANCE_ID, type SafeErrorMetadata } from '@usall-backend-assignment/platform';

export type WorkerShutdownSignal = 'SIGINT' | 'SIGTERM';

type WorkerLifecycleEvent =
  | { readonly eventName: 'WORKER_STARTED' }
  | { readonly eventName: 'WORKER_SHUTDOWN_REQUESTED'; readonly signal: WorkerShutdownSignal }
  | { readonly eventName: 'WORKER_STOPPED'; readonly shutdownDurationMs: number }
  | {
      readonly eventName: 'WORKER_STARTUP_FAILED' | 'WORKER_SHUTDOWN_FAILED';
      readonly errorName: string;
      readonly errorCode?: string;
    };

export interface WorkerLifecycleEventSinkOptions {
  readonly workerId?: string;
  readonly now?: () => Date;
  readonly write?: (line: string) => void;
}

export class WorkerLifecycleEventSink {
  private readonly workerId: string;
  private readonly now: () => Date;
  private readonly write: (line: string) => void;

  public constructor(options: WorkerLifecycleEventSinkOptions = {}) {
    this.workerId = options.workerId ?? PROCESS_INSTANCE_ID;
    this.now = options.now ?? (() => new Date());
    this.write = options.write ?? ((line) => process.stdout.write(line));
  }

  public record(event: WorkerLifecycleEvent): void {
    try {
      this.write(
        `${JSON.stringify({
          timestamp: this.now().toISOString(),
          level: event.eventName.endsWith('_FAILED') ? 'error' : 'info',
          component: 'worker-bootstrap',
          processInstanceId: this.workerId,
          workerId: this.workerId,
          ...event,
        })}\n`,
      );
    } catch {
      // A logging failure must not affect process lifecycle.
    }
  }

  public recordFailure(
    eventName: 'WORKER_STARTUP_FAILED' | 'WORKER_SHUTDOWN_FAILED',
    metadata: SafeErrorMetadata,
  ): void {
    this.record({ eventName, ...metadata });
  }
}
