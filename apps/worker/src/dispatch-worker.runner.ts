import {
  Inject,
  Injectable,
  Logger,
  type OnApplicationBootstrap,
  type OnApplicationShutdown,
} from '@nestjs/common';
import {
  DispatchRuntime,
  type PacedPermit,
  type StartedDispatch,
} from '@usall-backend-assignment/alarms';
import {
  abortableDelay,
  boundedExponentialBackoffMs,
  PROCESS_INSTANCE_ID,
  safeErrorMetadata,
} from '@usall-backend-assignment/platform';

export const DISPATCH_RUNTIME = Symbol('DISPATCH_RUNTIME');

const MAX_IN_FLIGHT = 64;
const GENERAL_CAPACITY = 62;

@Injectable()
export class DispatchWorkerRunner implements OnApplicationBootstrap, OnApplicationShutdown {
  private readonly logger = new Logger(DispatchWorkerRunner.name);
  private readonly shutdown = new AbortController();
  private readonly inFlight = new Set<Promise<void>>();
  private loopPromise: Promise<void> | null = null;

  public constructor(
    @Inject(DISPATCH_RUNTIME)
    private readonly runtime: DispatchRuntime,
  ) {}

  public onApplicationBootstrap(): void {
    this.loopPromise = this.runLoop();
  }

  public async onApplicationShutdown(): Promise<void> {
    this.shutdown.abort();
    await this.loopPromise;
    await Promise.allSettled(this.inFlight);
    await this.runtime.close();
  }

  private priorityFor(occupiedCapacity: number): number {
    return occupiedCapacity >= GENERAL_CAPACITY ? 100 : 0;
  }

  private track(started: StartedDispatch): void {
    const tracked = started.completion
      .catch((error: unknown) => {
        this.logger.error({
          eventName: 'DISPATCH_RESULT_COMMIT_FAILED',
          processInstanceId: PROCESS_INSTANCE_ID,
          workerId: PROCESS_INSTANCE_ID,
          ...safeErrorMetadata(error),
        });
      })
      .finally(() => {
        this.inFlight.delete(tracked);
      });
    this.inFlight.add(tracked);
  }

  private recordAdmissionFailure(error: unknown): void {
    this.logger.error({
      eventName: 'DISPATCH_ADMISSION_FAILED',
      processInstanceId: PROCESS_INSTANCE_ID,
      workerId: PROCESS_INSTANCE_ID,
      ...safeErrorMetadata(error),
    });
  }

  private async runLoop(): Promise<void> {
    let consecutiveFailures = 0;
    let preparedPermit: PacedPermit | null = null;
    while (!this.shutdown.signal.aborted) {
      if (preparedPermit === null && this.inFlight.size >= MAX_IN_FLIGHT) {
        await Promise.race(this.inFlight);
        continue;
      }

      if (preparedPermit === null) {
        try {
          preparedPermit = await this.runtime.processor.preparePermit(
            this.priorityFor(this.inFlight.size),
            this.shutdown.signal,
          );
          if (preparedPermit === null) {
            consecutiveFailures = 0;
            await abortableDelay(50, this.shutdown.signal);
          }
        } catch (error) {
          consecutiveFailures += 1;
          this.recordAdmissionFailure(error);
          await abortableDelay(
            boundedExponentialBackoffMs(consecutiveFailures),
            this.shutdown.signal,
          );
        }
        continue;
      }

      if (this.inFlight.size >= MAX_IN_FLIGHT) {
        // A permit is never refunded. If capacity changed before activation, discard it rather
        // than retaining an increasingly late permit or exceeding the provider in-flight cap.
        preparedPermit = null;
        await Promise.race(this.inFlight);
        continue;
      }

      const permit = preparedPermit;
      preparedPermit = null;
      const occupiedAtActivation = this.inFlight.size;
      const activation = this.runtime.processor.beginWithPermit(
        permit,
        this.priorityFor(occupiedAtActivation),
        this.shutdown.signal,
      );
      const projectedOccupied = occupiedAtActivation + 1;
      const nextPreparation =
        projectedOccupied < MAX_IN_FLIGHT
          ? this.runtime.processor.preparePermit(
              this.priorityFor(projectedOccupied),
              this.shutdown.signal,
            )
          : Promise.resolve(null);
      const activationOutcome = activation.then(
        (started) => {
          // Attach the completion observer as soon as activation settles. Waiting for a slow
          // speculative reservation here could otherwise leave a rejection temporarily unhandled.
          if (started !== null) {
            this.track(started);
          }
          return { failed: false as const, started };
        },
        (error: unknown) => {
          this.recordAdmissionFailure(error);
          return { failed: true as const, started: null };
        },
      );
      const preparationOutcome = nextPreparation.then(
        (permit) => ({ failed: false as const, permit }),
        (error: unknown) => {
          this.recordAdmissionFailure(error);
          return { failed: true as const, permit: null };
        },
      );
      const [settledActivation, settledPreparation] = await Promise.all([
        activationOutcome,
        preparationOutcome,
      ]);

      if (!this.shutdown.signal.aborted) {
        preparedPermit = settledPreparation.permit;
      }

      // A fulfilled admission, including a safe null caused by a claim race or late slot, proves
      // the DB path answered. A concurrent preparation error is therefore the first new failure.
      if (!settledActivation.failed) {
        consecutiveFailures = 0;
      }

      const failed = settledActivation.failed || settledPreparation.failed;
      if (failed) {
        consecutiveFailures += 1;
        await abortableDelay(
          boundedExponentialBackoffMs(consecutiveFailures),
          this.shutdown.signal,
        );
      } else {
        if (settledActivation.started === null) {
          // A stale eligibility observation may lose its claim race. The already-issued
          // speculative permit is intentionally not refunded, but it must not seed an unbounded
          // permit-only chain with no durable STARTED work.
          preparedPermit = null;
          await abortableDelay(50, this.shutdown.signal);
        }
      }
    }
  }
}
