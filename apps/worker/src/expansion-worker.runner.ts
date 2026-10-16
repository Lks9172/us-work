import {
  Inject,
  Injectable,
  Logger,
  type OnApplicationBootstrap,
  type OnApplicationShutdown,
} from '@nestjs/common';
import { ExpansionAbortedError, ExpansionRuntime } from '@usall-backend-assignment/alarms';
import {
  abortableDelay,
  boundedExponentialBackoffMs,
  PROCESS_INSTANCE_ID,
  safeErrorMetadata,
} from '@usall-backend-assignment/platform';

export const EXPANSION_RUNTIME = Symbol('EXPANSION_RUNTIME');

@Injectable()
export class ExpansionWorkerRunner implements OnApplicationBootstrap, OnApplicationShutdown {
  private readonly logger = new Logger(ExpansionWorkerRunner.name);
  private readonly shutdown = new AbortController();
  private loopPromise: Promise<void> | null = null;

  public constructor(
    @Inject(EXPANSION_RUNTIME)
    private readonly runtime: ExpansionRuntime,
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
        const processed = await this.runtime.processor.runOnce(this.shutdown.signal);
        consecutiveFailures = 0;
        if (!processed) {
          await abortableDelay(250, this.shutdown.signal);
        }
      } catch (error) {
        if (error instanceof ExpansionAbortedError && this.shutdown.signal.aborted) {
          return;
        }

        consecutiveFailures += 1;
        this.logger.error({
          eventName: 'EXPANSION_ITERATION_FAILED',
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
