import type { SafeErrorMetadata } from '@usall-backend-assignment/platform';

export interface MigrationLifecycleEventSinkOptions {
  readonly now?: () => Date;
  readonly write?: (line: string) => void;
}

export class MigrationLifecycleEventSink {
  private readonly now: () => Date;
  private readonly write: (line: string) => void;

  public constructor(options: MigrationLifecycleEventSinkOptions = {}) {
    this.now = options.now ?? (() => new Date());
    this.write = options.write ?? ((line) => process.stdout.write(line));
  }

  public recordCompleted(appliedMigrations: readonly string[]): void {
    this.record({
      eventName: 'MIGRATION_COMPLETED',
      appliedCount: appliedMigrations.length,
      schemaAlreadyCurrent: appliedMigrations.length === 0,
    });
  }

  public recordFailed(metadata: SafeErrorMetadata): void {
    this.record({ eventName: 'MIGRATION_FAILED', ...metadata });
  }

  private record(event: Record<string, unknown>): void {
    try {
      this.write(
        `${JSON.stringify({
          timestamp: this.now().toISOString(),
          level: event.eventName === 'MIGRATION_FAILED' ? 'error' : 'info',
          component: 'migration',
          ...event,
        })}\n`,
      );
    } catch {
      // Migration success/failure must not be changed by logging availability.
    }
  }
}
