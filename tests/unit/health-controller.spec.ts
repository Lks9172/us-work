import 'reflect-metadata';

import { ServiceUnavailableException } from '@nestjs/common';
import { describe, expect, it, vi } from 'vitest';

import { ApiLifecycleEventSink } from '../../apps/api/src/api-lifecycle-events';
import { HealthController } from '../../apps/api/src/health.controller';
import { PostgresAlarmRuntime } from '../../packages/alarms/src/infrastructure/persistence/postgres-alarm-runtime';

function controllerWith(ready: () => Promise<void>, output: string[] = []): HealthController {
  return new HealthController(
    { ready } as unknown as PostgresAlarmRuntime,
    new ApiLifecycleEventSink({
      instanceId: 'api-health-test',
      now: () => new Date('2026-10-15T00:00:00.000Z'),
      write: (line) => output.push(line),
    }),
  );
}

describe('API health dependency boundary', () => {
  it('OPS-10 keeps liveness independent from PostgreSQL readiness', () => {
    const ready = vi.fn(async () => {
      throw new Error('database unavailable');
    });
    const controller = controllerWith(ready);

    expect(controller.live()).toEqual({ ok: true });
    expect(ready).not.toHaveBeenCalled();
  });

  it('OPS-10 makes readiness probe only the API PostgreSQL runtime', async () => {
    const ready = vi.fn(async () => undefined);
    const controller = controllerWith(ready);

    await expect(controller.ready()).resolves.toEqual({ ok: true });
    expect(ready).toHaveBeenCalledOnce();
    expect(Reflect.getMetadata('design:paramtypes', HealthController)).toEqual([
      PostgresAlarmRuntime,
      ApiLifecycleEventSink,
    ]);
  });

  it('OPS-10 maps a PostgreSQL readiness failure to 503 without changing liveness', async () => {
    const ready = vi.fn(async () => {
      throw new Error('database unavailable');
    });
    const controller = controllerWith(ready);

    const failure = await controller.ready().catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(ServiceUnavailableException);
    expect((failure as ServiceUnavailableException).getStatus()).toBe(503);
    expect(controller.live()).toEqual({ ok: true });
    expect(ready).toHaveBeenCalledOnce();
  });

  it('OBS-04 logs readiness transitions once and excludes database error messages', async () => {
    let available = false;
    const output: string[] = [];
    const controller = controllerWith(async () => {
      if (!available) {
        throw new Error('postgresql://secret-user:secret-password@db/private-recipient');
      }
    }, output);

    await expect(controller.ready()).rejects.toBeInstanceOf(ServiceUnavailableException);
    await expect(controller.ready()).rejects.toBeInstanceOf(ServiceUnavailableException);
    available = true;
    await expect(controller.ready()).resolves.toEqual({ ok: true });
    await expect(controller.ready()).resolves.toEqual({ ok: true });

    expect(output).toHaveLength(2);
    expect(JSON.parse(output[0] ?? '')).toMatchObject({
      eventName: 'API_READINESS_CHANGED',
      dependency: 'postgresql',
      readiness: 'NOT_READY',
      errorName: 'Error',
    });
    expect(JSON.parse(output[1] ?? '')).toMatchObject({
      eventName: 'API_READINESS_CHANGED',
      dependency: 'postgresql',
      readiness: 'READY',
    });
    expect(output.join('')).not.toContain('secret-password');
    expect(output.join('')).not.toContain('private-recipient');
  });
});
