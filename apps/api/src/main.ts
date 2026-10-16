import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { FastifyAdapter, type NestFastifyApplication } from '@nestjs/platform-fastify';
import {
  NodeProcessShutdownCoordinator,
  parseTcpPort,
  safeErrorMetadata,
} from '@usall-backend-assignment/platform';

import { ApiModule } from './api.module';
import { API_LIFECYCLE_EVENTS, type ApiShutdownSignal } from './api-lifecycle-events';
import { API_FASTIFY_OPTIONS } from './http-server-config';

async function bootstrap(): Promise<void> {
  const shutdownCoordinator = new NodeProcessShutdownCoordinator({
    onShutdownRequested: (signal: ApiShutdownSignal) => {
      API_LIFECYCLE_EVENTS.record({ eventName: 'API_SHUTDOWN_REQUESTED', signal });
    },
    onShutdownCompleted: (shutdownDurationMs) => {
      API_LIFECYCLE_EVENTS.record({ eventName: 'API_STOPPED', shutdownDurationMs });
    },
    onShutdownFailed: (error) => {
      API_LIFECYCLE_EVENTS.recordFailure('API_SHUTDOWN_FAILED', safeErrorMetadata(error));
      process.exitCode = 1;
    },
  });

  try {
    const port = parseTcpPort(process.env.PORT, 3_000);
    const application = await NestFactory.create<NestFastifyApplication>(
      ApiModule,
      new FastifyAdapter(API_FASTIFY_OPTIONS),
    );
    shutdownCoordinator.setCloseHandler(() => application.close());

    if (shutdownCoordinator.shutdownRequested) {
      await shutdownCoordinator.waitForShutdown();
      return;
    }

    try {
      await application.listen(port, '0.0.0.0');
    } catch (error: unknown) {
      if (shutdownCoordinator.shutdownRequested) {
        await shutdownCoordinator.waitForShutdown();
        return;
      }
      throw error;
    }

    if (shutdownCoordinator.shutdownRequested) {
      await shutdownCoordinator.waitForShutdown();
      return;
    }
    API_LIFECYCLE_EVENTS.record({ eventName: 'API_STARTED', port });
  } catch (error: unknown) {
    shutdownCoordinator.dispose();
    throw error;
  }
}

void bootstrap().catch((error: unknown) => {
  API_LIFECYCLE_EVENTS.recordFailure('API_STARTUP_FAILED', safeErrorMetadata(error));
  process.exitCode = 1;
});
