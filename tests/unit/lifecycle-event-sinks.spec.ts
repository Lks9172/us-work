import { describe, expect, it } from 'vitest';

import { ApiLifecycleEventSink } from '../../apps/api/src/api-lifecycle-events';
import { MigrationLifecycleEventSink } from '../../apps/migrate/src/migration-lifecycle-events';
import { WorkerLifecycleEventSink } from '../../apps/worker/src/worker-lifecycle-events';

const NOW = () => new Date('2026-10-15T00:00:00.000Z');

describe('structured process lifecycle events', () => {
  it('OBS-04 emits bounded worker startup and shutdown JSON without error messages', () => {
    const output: string[] = [];
    const sink = new WorkerLifecycleEventSink({
      workerId: 'worker-lifecycle-test',
      now: NOW,
      write: (line) => output.push(line),
    });

    sink.record({ eventName: 'WORKER_STARTED' });
    sink.record({ eventName: 'WORKER_SHUTDOWN_REQUESTED', signal: 'SIGTERM' });
    sink.record({ eventName: 'WORKER_STOPPED', shutdownDurationMs: 12.5 });
    sink.recordFailure('WORKER_STARTUP_FAILED', { errorName: 'Error' });

    expect(output.slice(0, 3).map((line) => JSON.parse(line).eventName)).toEqual([
      'WORKER_STARTED',
      'WORKER_SHUTDOWN_REQUESTED',
      'WORKER_STOPPED',
    ]);
    expect(output.join('')).not.toContain('private-recipient');
    expect(output.join('')).not.toContain('secret-body');
    for (const line of output) {
      expect(Buffer.byteLength(line, 'utf8')).toBeLessThan(1_024);
    }
  });

  it('OBS-04 emits API lifecycle JSON and swallows writer failures', () => {
    const output: string[] = [];
    const sink = new ApiLifecycleEventSink({
      instanceId: 'api-lifecycle-test',
      now: NOW,
      write: (line) => output.push(line),
    });

    sink.record({ eventName: 'API_STARTED', port: 3_000 });
    sink.recordNotReady(new Error('DATABASE_URL=postgresql://secret@db'));
    sink.record({ eventName: 'API_SHUTDOWN_REQUESTED', signal: 'SIGINT' });
    sink.record({ eventName: 'API_STOPPED', shutdownDurationMs: 8 });

    expect(output.map((line) => JSON.parse(line).eventName)).toEqual([
      'API_STARTED',
      'API_READINESS_CHANGED',
      'API_SHUTDOWN_REQUESTED',
      'API_STOPPED',
    ]);
    expect(output.join('')).not.toContain('postgresql://');
    const unavailableSink = new ApiLifecycleEventSink({
      write: () => {
        throw new Error('collector unavailable');
      },
    });
    expect(() => unavailableSink.record({ eventName: 'API_STARTED', port: 3_000 })).not.toThrow();
  });

  it('OBS-04 reports migration count/state without migration names or error content', () => {
    const output: string[] = [];
    const sink = new MigrationLifecycleEventSink({ now: NOW, write: (line) => output.push(line) });

    sink.recordCompleted(['0001_secret_customer_name.sql', '0002_private_recipient.sql']);
    sink.recordFailed({ errorName: 'Error' });

    expect(JSON.parse(output[0] ?? '')).toMatchObject({
      eventName: 'MIGRATION_COMPLETED',
      appliedCount: 2,
      schemaAlreadyCurrent: false,
    });
    expect(JSON.parse(output[1] ?? '')).toMatchObject({
      eventName: 'MIGRATION_FAILED',
      errorName: 'Error',
    });
    expect(output.join('')).not.toContain('0001_secret');
    expect(output.join('')).not.toContain('password');
  });
});
