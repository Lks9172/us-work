import { PROCESS_INSTANCE_ID } from '@usall-backend-assignment/platform';

import type {
  ExpansionDuplicateRecipientEvent,
  ExpansionEventSink,
} from '../../application/expansion';

export interface JsonExpansionEventSinkOptions {
  readonly workerId?: string;
  readonly now?: () => Date;
  readonly write?: (line: string) => void;
}

export class JsonExpansionEventSink implements ExpansionEventSink {
  private readonly workerId: string;
  private readonly now: () => Date;
  private readonly write: (line: string) => void;

  public constructor(options: JsonExpansionEventSinkOptions = {}) {
    this.workerId = options.workerId ?? PROCESS_INSTANCE_ID;
    this.now = options.now ?? (() => new Date());
    this.write = options.write ?? ((line) => process.stdout.write(line));
  }

  public recordDuplicateRecipients(event: ExpansionDuplicateRecipientEvent): void {
    this.write(
      `${JSON.stringify({
        timestamp: this.now().toISOString(),
        level: 'warn',
        component: 'expansion-worker',
        processInstanceId: this.workerId,
        workerId: this.workerId,
        eventName: 'EXPANSION_DUPLICATE_RECIPIENTS',
        ...event,
      })}\n`,
    );
  }
}
