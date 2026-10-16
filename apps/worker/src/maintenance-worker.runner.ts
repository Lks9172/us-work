import {
  Inject,
  Injectable,
  Logger,
  type OnApplicationBootstrap,
  type OnApplicationShutdown,
} from '@nestjs/common';
import { MaintenanceRuntime } from '@usall-backend-assignment/alarms';
import {
  abortableDelay,
  boundedExponentialBackoffMs,
  PROCESS_INSTANCE_ID,
  safeErrorMetadata,
} from '@usall-backend-assignment/platform';

export const MAINTENANCE_RUNTIME = Symbol('MAINTENANCE_RUNTIME');

@Injectable()
export class MaintenanceWorkerRunner implements OnApplicationBootstrap, OnApplicationShutdown {
  private readonly logger = new Logger(MaintenanceWorkerRunner.name);
  private readonly shutdown = new AbortController();
  private loopPromise: Promise<void> | null = null;

  public constructor(
    @Inject(MAINTENANCE_RUNTIME)
    private readonly runtime: MaintenanceRuntime,
  ) {}

  public onApplicationBootstrap(): void {
    this.loopPromise = this.runLoop();
  }

  public async onApplicationShutdown(): Promise<void> {
    this.shutdown.abort();
    await this.loopPromise;
    await this.runtime.close();
  }

  private async runLoop(): Promise<void> {
    let consecutiveFailures = 0;
    while (!this.shutdown.signal.aborted) {
      try {
        const processed = await this.runtime.processor.runOnce();
        consecutiveFailures = 0;
        if (!processed) {
          await abortableDelay(200, this.shutdown.signal);
        }
      } catch (error) {
        consecutiveFailures += 1;
        this.logger.error({
          eventName: 'MAINTENANCE_ITERATION_FAILED',
          processInstanceId: PROCESS_INSTANCE_ID,
          workerId: PROCESS_INSTANCE_ID,
          ...safeErrorMetadata(error),
        });
        await abortableDelay(
          boundedExponentialBackoffMs(consecutiveFailures),
          this.shutdown.signal,
        );
      }
    }
  }
}
