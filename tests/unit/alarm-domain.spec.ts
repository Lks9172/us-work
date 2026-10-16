import { describe, expect, it } from 'vitest';

import {
  ALARM_STATUSES,
  type AlarmStatus,
  type AlarmType,
  AlarmValidationError,
  validateCreateAlarm,
} from '../../packages/alarms/src/domain/alarm';
import {
  type CancelDecision,
  decideCancel,
  decideStart,
  type StartDecision,
} from '../../packages/alarms/src/domain/alarm-policy';

const startDecisionByStatus = {
  CREATED: { BULK: 'INITIALIZE_BULK', EMERGENCY: 'INITIALIZE_EMERGENCY' },
  QUEUED: { BULK: 'NOOP', EMERGENCY: 'NOOP' },
  EXPANDING: { BULK: 'NOOP', EMERGENCY: 'NOOP' },
  SENDING: { BULK: 'NOOP', EMERGENCY: 'NOOP' },
  CANCEL_REQUESTED: { BULK: 'CONFLICT', EMERGENCY: 'CONFLICT' },
  CANCELED: { BULK: 'CONFLICT', EMERGENCY: 'CONFLICT' },
  COMPLETED: { BULK: 'CONFLICT', EMERGENCY: 'CONFLICT' },
  PARTIALLY_FAILED: { BULK: 'CONFLICT', EMERGENCY: 'CONFLICT' },
  FAILED: { BULK: 'CONFLICT', EMERGENCY: 'CONFLICT' },
  ATTENTION_REQUIRED: { BULK: 'CONFLICT', EMERGENCY: 'CONFLICT' },
} satisfies Record<AlarmStatus, Record<AlarmType, StartDecision>>;

const cancelDecisionByStatus = {
  CREATED: 'CANCEL_CREATED',
  QUEUED: 'REQUEST_CANCEL',
  EXPANDING: 'REQUEST_CANCEL',
  SENDING: 'REQUEST_CANCEL',
  CANCEL_REQUESTED: 'NOOP',
  CANCELED: 'NOOP',
  COMPLETED: 'CONFLICT',
  PARTIALLY_FAILED: 'CONFLICT',
  FAILED: 'CONFLICT',
  ATTENTION_REQUIRED: 'CONFLICT',
} satisfies Record<AlarmStatus, CancelDecision>;

describe('alarm creation rules', () => {
  it('accepts BULK without recipients and trims only the title', () => {
    expect(validateCreateAlarm({ type: 'BULK', title: '  Notice  ', body: ' body ' })).toEqual({
      type: 'BULK',
      title: 'Notice',
      body: ' body ',
      recipientIds: [],
    });
  });

  it('preserves exact opaque emergency recipient IDs', () => {
    const composed = '\u00e9';
    const decomposed = 'e\u0301';

    expect(
      validateCreateAlarm({
        type: 'EMERGENCY',
        title: 'Payment',
        body: 'Approved',
        recipientIds: ['A', 'a', composed, decomposed],
      }).recipientIds,
    ).toEqual(['A', 'a', composed, decomposed]);
  });

  it.each([
    {
      name: 'BULK recipients',
      draft: { type: 'BULK', title: 't', body: 'b', recipientIds: ['user-1'] },
      code: 'FORBIDDEN_FIELD',
    },
    {
      name: 'duplicate emergency recipient',
      draft: { type: 'EMERGENCY', title: 't', body: 'b', recipientIds: ['same', 'same'] },
      code: 'DUPLICATE_RECIPIENT',
    },
    {
      name: 'recipient edge whitespace',
      draft: { type: 'EMERGENCY', title: 't', body: 'b', recipientIds: [' user-1'] },
      code: 'INVALID_RECIPIENT',
    },
    {
      name: 'empty emergency recipients',
      draft: { type: 'EMERGENCY', title: 't', body: 'b', recipientIds: [] },
      code: 'REQUIRED_FIELD',
    },
  ])('rejects $name atomically', ({ draft, code }) => {
    expect.assertions(1);

    try {
      validateCreateAlarm(draft);
    } catch (error) {
      expect(error).toMatchObject<Partial<AlarmValidationError>>({
        issues: expect.arrayContaining([expect.objectContaining({ code })]),
      });
    }
  });

  it('counts Unicode code points instead of UTF-16 code units', () => {
    expect(() =>
      validateCreateAlarm({ type: 'BULK', title: '😀'.repeat(200), body: 'ok' }),
    ).not.toThrow();
    expect(() =>
      validateCreateAlarm({ type: 'BULK', title: '😀'.repeat(201), body: 'ok' }),
    ).toThrow(AlarmValidationError);
  });
});

describe('alarm command policy', () => {
  it.each(ALARM_STATUSES)('maps %s start for both alarm types', (status) => {
    expect(decideStart('BULK', status)).toBe(startDecisionByStatus[status].BULK);
    expect(decideStart('EMERGENCY', status)).toBe(startDecisionByStatus[status].EMERGENCY);
  });

  it.each(ALARM_STATUSES)('maps %s cancel without reopening terminal work', (status) => {
    expect(decideCancel(status)).toBe(cancelDecisionByStatus[status]);
  });
});
