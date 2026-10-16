import { EventEmitter } from 'node:events';

import { describe, expect, it } from 'vitest';

import {
  attachPostgresClientErrorHandler,
  attachPostgresPoolErrorHandler,
} from '../../packages/alarms/src/infrastructure/persistence/postgres-pool-observability';

describe('attachPostgresPoolErrorHandler', () => {
  it('handles idle connection errors without serializing connection details', () => {
    const emitter = new EventEmitter();
    const lines: string[] = [];
    attachPostgresPoolErrorHandler(emitter, {
      component: 'test-pool',
      processInstanceId: 'process-test',
      now: () => new Date('2026-10-15T00:00:00.000Z'),
      write: (line) => lines.push(line),
    });
    const error = Object.assign(new Error('password=must-not-leak host=database'), {
      code: '57P01',
    });

    expect(() => emitter.emit('error', error)).not.toThrow();
    expect(JSON.parse(lines[0] ?? '')).toEqual({
      timestamp: '2026-10-15T00:00:00.000Z',
      level: 'error',
      component: 'test-pool',
      processInstanceId: 'process-test',
      eventName: 'POSTGRES_IDLE_CLIENT_ERROR',
      errorName: 'Error',
      errorCode: '57P01',
    });
    expect(lines[0]).not.toContain('must-not-leak');
    expect(lines[0]).not.toContain('database');
  });

  it('handles active transaction client errors until detached', () => {
    const emitter = new EventEmitter();
    const lines: string[] = [];
    const detach = attachPostgresClientErrorHandler(emitter, {
      component: 'test-client',
      processInstanceId: 'process-test',
      now: () => new Date('2026-10-15T00:00:00.000Z'),
      write: (line) => lines.push(line),
    });

    expect(() => emitter.emit('error', new Error('connection lost'))).not.toThrow();
    expect(JSON.parse(lines[0] ?? '')).toMatchObject({
      component: 'test-client',
      processInstanceId: 'process-test',
      eventName: 'POSTGRES_ACTIVE_CLIENT_ERROR',
    });
    detach();
    expect(emitter.listenerCount('error')).toBe(0);
  });
});
