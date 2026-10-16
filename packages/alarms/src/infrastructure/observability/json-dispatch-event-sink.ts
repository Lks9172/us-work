import { PROCESS_INSTANCE_ID } from '@usall-backend-assignment/platform';

import type { DispatchEventSink, DispatchLifecycleEvent } from '../../application/dispatch';

export interface JsonDispatchEventSinkOptions {
  readonly workerId?: string;
  readonly now?: () => Date;
  readonly write?: (line: string) => void;
}

export class JsonDispatchEventSink implements DispatchEventSink {
  private readonly workerId: string;
  private readonly now: () => Date;
  private readonly write: (line: string) => void;

  public constructor(options: JsonDispatchEventSinkOptions = {}) {
    this.workerId = options.workerId ?? PROCESS_INSTANCE_ID;
    this.now = options.now ?? (() => new Date());
    this.write = options.write ?? ((line) => process.stdout.write(line));
  }

  public record(event: DispatchLifecycleEvent): void {
    this.write(
      `${JSON.stringify({
        timestamp: this.now().toISOString(),
        level: 'info',
        component: 'dispatch-worker',
        processInstanceId: this.workerId,
        workerId: this.workerId,
        ...event,
      })}\n`,
    );
  }
}
