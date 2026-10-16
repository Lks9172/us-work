import { Inject, Module, type OnApplicationShutdown } from '@nestjs/common';
import { APP_FILTER } from '@nestjs/core';
import {
  ALARM_APPLICATION_SERVICE,
  AlarmsController,
  PostgresAlarmRuntime,
  ProblemExceptionFilter,
} from '@usall-backend-assignment/alarms';
import {
  requireEnvironmentString,
  requireEnvironmentUrl,
} from '@usall-backend-assignment/platform';

import { DocsController } from './docs.controller';
import { API_LIFECYCLE_EVENTS, ApiLifecycleEventSink } from './api-lifecycle-events';
import { HealthController } from './health.controller';

@Module({
  controllers: [AlarmsController, HealthController, DocsController],
  providers: [
    { provide: ApiLifecycleEventSink, useValue: API_LIFECYCLE_EVENTS },
    {
      provide: PostgresAlarmRuntime,
      useFactory: () => {
        const databaseUrl = requireEnvironmentUrl(process.env.DATABASE_URL, 'DATABASE_URL', [
          'postgres:',
          'postgresql:',
        ]);
        const cursorSigningKey = requireEnvironmentString(
          process.env.CURSOR_SIGNING_KEY,
          'CURSOR_SIGNING_KEY',
        );

        return new PostgresAlarmRuntime({ databaseUrl, cursorSigningKey });
      },
    },
    {
      provide: ALARM_APPLICATION_SERVICE,
      inject: [PostgresAlarmRuntime],
      useFactory: (runtime: PostgresAlarmRuntime) => runtime.service,
    },
    { provide: APP_FILTER, useClass: ProblemExceptionFilter },
  ],
})
export class ApiModule implements OnApplicationShutdown {
  public constructor(
    @Inject(PostgresAlarmRuntime)
    private readonly runtime: PostgresAlarmRuntime,
  ) {}

  public onApplicationShutdown(): Promise<void> {
    return this.runtime.close();
  }
}
