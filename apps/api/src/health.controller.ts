import { Controller, Get, ServiceUnavailableException } from '@nestjs/common';
import { PostgresAlarmRuntime } from '@usall-backend-assignment/alarms';

import { ApiLifecycleEventSink } from './api-lifecycle-events';

@Controller('health')
export class HealthController {
  private readiness: 'UNKNOWN' | 'READY' | 'NOT_READY' = 'UNKNOWN';

  public constructor(
    private readonly runtime: PostgresAlarmRuntime,
    private readonly lifecycleEvents: ApiLifecycleEventSink,
  ) {}

  @Get('live')
  public live(): { readonly ok: true } {
    return { ok: true };
  }

  @Get('ready')
  public async ready(): Promise<{ readonly ok: true }> {
    try {
      await this.runtime.ready();
      if (this.readiness !== 'READY') {
        this.readiness = 'READY';
        this.lifecycleEvents.record({
          eventName: 'API_READINESS_CHANGED',
          dependency: 'postgresql',
          readiness: 'READY',
        });
      }
      return { ok: true };
    } catch (error) {
      if (this.readiness !== 'NOT_READY') {
        this.readiness = 'NOT_READY';
        this.lifecycleEvents.recordNotReady(error);
      }
      throw new ServiceUnavailableException('Database is not ready');
    }
  }
}
