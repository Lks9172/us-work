import { PROCESS_INSTANCE_ID } from '@usall-backend-assignment/platform';

import type {
  AlarmCounterInconsistency,
  MaintenanceEventSink,
  OperationalSnapshot,
  SendLeaseRecoveryEvent,
} from '../../application/maintenance';

export interface JsonMaintenanceEventSinkOptions {
  readonly workerId?: string;
  readonly now?: () => Date;
  readonly write?: (line: string) => void;
}

export class JsonMaintenanceEventSink implements MaintenanceEventSink {
  private readonly workerId: string;
  private readonly now: () => Date;
  private readonly write: (line: string) => void;

  public constructor(options: JsonMaintenanceEventSinkOptions = {}) {
    this.workerId = options.workerId ?? PROCESS_INSTANCE_ID;
    this.now = options.now ?? (() => new Date());
    this.write = options.write ?? ((line) => process.stdout.write(line));
  }

  public recordCounterInconsistency(issue: AlarmCounterInconsistency): void {
    this.write(
      `${JSON.stringify({
        timestamp: this.now().toISOString(),
        level: 'error',
        component: 'maintenance-worker',
        processInstanceId: this.workerId,
        workerId: this.workerId,
        eventName: 'ALARM_COUNTER_INCONSISTENCY',
        ...issue,
      })}\n`,
    );
  }

  public recordOperationalSnapshot(snapshot: OperationalSnapshot): void {
    this.write(
      `${JSON.stringify({
        timestamp: this.now().toISOString(),
        level: 'info',
        component: 'maintenance-worker',
        processInstanceId: this.workerId,
        workerId: this.workerId,
        eventName: 'OPERATIONAL_SNAPSHOT',
        ...snapshot,
      })}\n`,
    );
  }

  public recordSendLeaseRecovery(event: SendLeaseRecoveryEvent): void {
    this.write(
      `${JSON.stringify({
        timestamp: this.now().toISOString(),
        level: 'warn',
        component: 'maintenance-worker',
        processInstanceId: this.workerId,
        workerId: this.workerId,
        eventName: 'SEND_LEASE_RECOVERED',
        ...event,
      })}\n`,
    );
  }
}
