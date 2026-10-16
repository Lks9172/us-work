import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import {
  NodeProcessShutdownCoordinator,
  safeErrorMetadata,
} from '@usall-backend-assignment/platform';

import { WorkerModule } from './worker.module';
import { WorkerLifecycleEventSink, type WorkerShutdownSignal } from './worker-lifecycle-events';

const lifecycleEvents = new WorkerLifecycleEventSink();

export async function bootstrap(): Promise<void> {
  let keepAliveTimer: ReturnType<typeof setInterval> | null = null;
  const shutdownCoordinator = new NodeProcessShutdownCoordinator({
    onShutdownRequested: (signal: WorkerShutdownSignal) => {
      lifecycleEvents.record({ eventName: 'WORKER_SHUTDOWN_REQUESTED', signal });
      if (keepAliveTimer !== null) {
        clearInterval(keepAliveTimer);
        keepAliveTimer = null;
      }
    },
    onShutdownCompleted: (shutdownDurationMs) => {
      lifecycleEvents.record({ eventName: 'WORKER_STOPPED', shutdownDurationMs });
    },
    onShutdownFailed: (error) => {
      lifecycleEvents.recordFailure('WORKER_SHUTDOWN_FAILED', safeErrorMetadata(error));
      process.exitCode = 1;
    },
  });

  try {
    const application = await NestFactory.createApplicationContext(WorkerModule);
    shutdownCoordinator.setCloseHandler(() => application.close());

    if (shutdownCoordinator.shutdownRequested) {
      await shutdownCoordinator.waitForShutdown();
      return;
    }

    keepAliveTimer = setInterval(() => undefined, 60_000);
    lifecycleEvents.record({ eventName: 'WORKER_STARTED' });
  } catch (error: unknown) {
    if (keepAliveTimer !== null) {
      clearInterval(keepAliveTimer);
    }
    shutdownCoordinator.dispose();
    throw error;
  }
}

void bootstrap().catch((error: unknown) => {
  lifecycleEvents.recordFailure('WORKER_STARTUP_FAILED', safeErrorMetadata(error));
  process.exitCode = 1;
});
