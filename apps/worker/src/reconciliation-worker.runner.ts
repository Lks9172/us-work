import {
  Inject,
  Injectable,
  Logger,
  type OnApplicationBootstrap,
  type OnApplicationShutdown,
} from '@nestjs/common';
import { ReconciliationRuntime } from '@usall-backend-assignment/alarms';
import {
  abortableDelay,
  boundedExponentialBackoffMs,
  PROCESS_INSTANCE_ID,
  safeErrorMetadata,
} from '@usall-backend-assignment/platform';

export const RECONCILIATION_RUNTIME = Symbol('RECONCILIATION_RUNTIME');
const MAX_CONCURRENCY = 8;

@Injectable()
export class ReconciliationWorkerRunner implements OnApplicationBootstrap, OnApplicationShutdown {
  private readonly logger = new Logger(ReconciliationWorkerRunner.name);
  private readonly shutdown = new AbortController();
  private loopPromise: Promise<void> | null = null;

  public constructor(
    @Inject(RECONCILIATION_RUNTIME)
    private readonly runtime: ReconciliationRuntime,
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
      let iterationFailed = false;
      const results = await Promise.all(
        Array.from({ length: MAX_CONCURRENCY }, async () => {
          try {
            return await this.runtime.processor.runOnce(this.shutdown.signal);
          } catch (error) {
            iterationFailed = true;
            this.logger.error({
              eventName: 'RECONCILIATION_ITERATION_FAILED',
              processInstanceId: PROCESS_INSTANCE_ID,
              workerId: PROCESS_INSTANCE_ID,
              ...safeErrorMetadata(error),
            });
            return false;
          }
        }),
      );

      if (results.some(Boolean)) {
        consecutiveFailures = 0;
      } else if (iterationFailed) {
        consecutiveFailures += 1;
        await abortableDelay(
          boundedExponentialBackoffMs(consecutiveFailures),
          this.shutdown.signal,
        );
      } else {
        consecutiveFailures = 0;
        await abortableDelay(200, this.shutdown.signal);
      }
    }
  }
}
