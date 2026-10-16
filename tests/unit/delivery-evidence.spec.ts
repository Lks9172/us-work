import { describe, expect, it } from 'vitest';

import {
  evaluateEvidence,
  type ProviderEvidenceEntry,
} from '../../packages/alarms/src/domain/delivery-evidence';

describe('evaluateEvidence', () => {
  it.each([
    {
      name: 'zero matching messages',
      entries: [],
      expected: { kind: 'MATCH', distinctMessageIds: [], providerSentAt: null },
    },
    {
      name: 'one response-only message',
      entries: [{ messageId: 'message-1', recipientId: 'user-1' }],
      expected: { kind: 'MATCH', distinctMessageIds: ['message-1'], providerSentAt: null },
    },
    {
      name: 'two distinct messages',
      entries: [
        { messageId: 'message-2', recipientId: 'user-1', sentAt: '2026-01-01T00:00:02Z' },
        { messageId: 'message-1', recipientId: 'user-1', sentAt: '2026-01-01T00:00:01Z' },
      ],
      expected: {
        kind: 'MATCH',
        distinctMessageIds: ['message-1', 'message-2'],
        providerSentAt: '2026-01-01T00:00:01Z',
      },
    },
    {
      name: 'three distinct messages in the 2+ bucket',
      entries: [
        { messageId: 'message-3', recipientId: 'user-1' },
        { messageId: 'message-1', recipientId: 'user-1' },
        { messageId: 'message-2', recipientId: 'user-1' },
      ],
      expected: {
        kind: 'MATCH',
        distinctMessageIds: ['message-1', 'message-2', 'message-3'],
        providerSentAt: null,
      },
    },
  ] satisfies ReadonlyArray<{
    name: string;
    entries: readonly ProviderEvidenceEntry[];
    expected: ReturnType<typeof evaluateEvidence>;
  }>)('classifies $name by distinct provider message ID', ({ entries, expected }) => {
    expect(evaluateEvidence(entries, 'user-1')).toEqual(expected);
  });

  it('unions RESPONSE and HISTORY repeats independently of observation order', () => {
    const response: ProviderEvidenceEntry = {
      messageId: 'message-1',
      recipientId: 'user-1',
    };
    const history: ProviderEvidenceEntry = {
      messageId: 'message-1',
      recipientId: 'user-1',
      sentAt: '2026-01-01T00:00:01Z',
    };
    const duplicate: ProviderEvidenceEntry = {
      messageId: 'message-2',
      recipientId: 'user-1',
      sentAt: '2026-01-01T00:00:02Z',
    };
    const expected = {
      kind: 'MATCH',
      distinctMessageIds: ['message-1', 'message-2'],
      providerSentAt: '2026-01-01T00:00:01Z',
    };

    expect(evaluateEvidence([response, history, duplicate], 'user-1')).toEqual(expected);
    expect(evaluateEvidence([duplicate, history, response], 'user-1')).toEqual(expected);
    expect(evaluateEvidence([history, response, duplicate, history], 'user-1')).toEqual(expected);
  });

  it('keeps response-only evidence timestamp-free until history supplies provider time', () => {
    const response: ProviderEvidenceEntry = {
      messageId: 'message-1',
      recipientId: 'user-1',
    };
    const history: ProviderEvidenceEntry = {
      messageId: 'message-1',
      recipientId: 'user-1',
      sentAt: '2026-01-01T00:00:01Z',
    };

    expect(evaluateEvidence([response], 'user-1')).toMatchObject({ providerSentAt: null });
    expect(evaluateEvidence([response, history], 'user-1')).toMatchObject({
      providerSentAt: history.sentAt,
    });
    expect(evaluateEvidence([history, response], 'user-1')).toMatchObject({
      providerSentAt: history.sentAt,
    });
  });

  it.each([
    {
      entries: [
        { messageId: 'message-1', recipientId: 'user-1', sentAt: '2026-01-01T00:00:00Z' },
        { messageId: 'message-1', recipientId: 'user-1', sentAt: '2026-01-01T00:00:01Z' },
        { messageId: 'message-2', recipientId: 'other-user' },
      ],
    },
    {
      entries: [
        { messageId: 'message-2', recipientId: 'other-user' },
        { messageId: 'message-1', recipientId: 'user-1', sentAt: '2026-01-01T00:00:01Z' },
        { messageId: 'message-1', recipientId: 'user-1', sentAt: '2026-01-01T00:00:00Z' },
      ],
    },
  ] satisfies ReadonlyArray<{ entries: readonly ProviderEvidenceEntry[] }>)(
    'gives recipient mismatch precedence over positive, duplicate, and conflict evidence',
    ({ entries }) => {
      expect(evaluateEvidence(entries, 'user-1')).toEqual({ kind: 'RECIPIENT_MISMATCH' });
    },
  );

  it.each([
    {
      entries: [
        { messageId: 'message-1', recipientId: 'user-1', sentAt: '2026-01-01T00:00:00Z' },
        { messageId: 'message-1', recipientId: 'user-1', sentAt: '2026-01-01T00:00:01Z' },
        { messageId: 'message-2', recipientId: 'user-1' },
      ],
    },
    {
      entries: [
        { messageId: 'message-2', recipientId: 'user-1' },
        { messageId: 'message-1', recipientId: 'user-1', sentAt: '2026-01-01T00:00:01Z' },
        { messageId: 'message-1', recipientId: 'user-1', sentAt: '2026-01-01T00:00:00Z' },
      ],
    },
  ] satisfies ReadonlyArray<{ entries: readonly ProviderEvidenceEntry[] }>)(
    'gives same-message conflict precedence over matching and distinct evidence',
    ({ entries }) => {
      expect(evaluateEvidence(entries, 'user-1')).toEqual({ kind: 'EVIDENCE_CONFLICT' });
    },
  );

  it('does not turn repeated identical observations into a distinct duplicate', () => {
    const repeated: ProviderEvidenceEntry = {
      messageId: 'message-1',
      recipientId: 'user-1',
      sentAt: '2026-01-01T00:00:00Z',
    };

    expect(evaluateEvidence([repeated, repeated, repeated], 'user-1')).toEqual({
      kind: 'MATCH',
      distinctMessageIds: ['message-1'],
      providerSentAt: repeated.sentAt,
    });
  });
});
