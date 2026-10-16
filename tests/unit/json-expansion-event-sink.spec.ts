import { describe, expect, it } from 'vitest';

import { JsonExpansionEventSink } from '../../packages/alarms/src/infrastructure/observability/json-expansion-event-sink';

describe('JsonExpansionEventSink', () => {
  it('USR-06 emits only bounded aggregate duplicate counts after a committed page', () => {
    const lines: string[] = [];
    const sink = new JsonExpansionEventSink({
      workerId: 'expansion-test-worker',
      now: () => new Date('2026-10-15T00:00:00.000Z'),
      write: (line) => lines.push(line),
    });

    sink.recordDuplicateRecipients({
      alarmId: '11111111-1111-4111-8111-111111111111',
      runGeneration: '22222222-2222-4222-8222-222222222222',
      leaseGeneration: 3,
      receivedCount: 1_000,
      insertedCount: 998,
      duplicateCount: 2,
    });

    expect(lines).toHaveLength(1);
    expect(JSON.parse(lines[0] ?? '')).toEqual({
      timestamp: '2026-10-15T00:00:00.000Z',
      level: 'warn',
      component: 'expansion-worker',
      processInstanceId: 'expansion-test-worker',
      workerId: 'expansion-test-worker',
      eventName: 'EXPANSION_DUPLICATE_RECIPIENTS',
      alarmId: '11111111-1111-4111-8111-111111111111',
      runGeneration: '22222222-2222-4222-8222-222222222222',
      leaseGeneration: 3,
      receivedCount: 1_000,
      insertedCount: 998,
      duplicateCount: 2,
    });
    expect(lines[0]).not.toContain('recipientId');
    expect(lines[0]).not.toContain('cursor');
    expect(lines[0]).not.toContain('phone');
    expect(lines[0]).not.toContain('name');
  });
});
