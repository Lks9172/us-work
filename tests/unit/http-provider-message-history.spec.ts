import { describe, expect, it, vi } from 'vitest';

import { HttpProviderMessageHistory } from '../../packages/alarms/src/infrastructure/provider/http-provider-message-history';

const validHistoryEntry = {
  messageId: 'message-1',
  recipientId: 'u_000001',
  sentAt: '2026-10-15T00:00:00.000Z',
};

const malformedHistoryBodies: readonly { readonly label: string; readonly body: string }[] = [
  { label: 'invalid JSON', body: 'not-json' },
  { label: 'null top-level body', body: 'null' },
  { label: 'array top-level body', body: '[]' },
  { label: 'missing messages', body: '{}' },
  { label: 'null messages', body: JSON.stringify({ messages: null }) },
  { label: 'non-array messages', body: JSON.stringify({ messages: {} }) },
  { label: 'null message entry', body: JSON.stringify({ messages: [null] }) },
  { label: 'array message entry', body: JSON.stringify({ messages: [[]] }) },
  { label: 'string message entry', body: JSON.stringify({ messages: ['message'] }) },
  {
    label: 'missing messageId',
    body: JSON.stringify({
      messages: [{ recipientId: validHistoryEntry.recipientId, sentAt: validHistoryEntry.sentAt }],
    }),
  },
  {
    label: 'null messageId',
    body: JSON.stringify({ messages: [{ ...validHistoryEntry, messageId: null }] }),
  },
  {
    label: 'numeric messageId',
    body: JSON.stringify({ messages: [{ ...validHistoryEntry, messageId: 1 }] }),
  },
  {
    label: 'empty messageId',
    body: JSON.stringify({ messages: [{ ...validHistoryEntry, messageId: '' }] }),
  },
  {
    label: 'missing recipientId',
    body: JSON.stringify({
      messages: [{ messageId: validHistoryEntry.messageId, sentAt: validHistoryEntry.sentAt }],
    }),
  },
  {
    label: 'null recipientId',
    body: JSON.stringify({ messages: [{ ...validHistoryEntry, recipientId: null }] }),
  },
  {
    label: 'numeric recipientId',
    body: JSON.stringify({ messages: [{ ...validHistoryEntry, recipientId: 1 }] }),
  },
  {
    label: 'empty recipientId',
    body: JSON.stringify({ messages: [{ ...validHistoryEntry, recipientId: '' }] }),
  },
  {
    label: 'missing sentAt',
    body: JSON.stringify({
      messages: [
        {
          messageId: validHistoryEntry.messageId,
          recipientId: validHistoryEntry.recipientId,
        },
      ],
    }),
  },
  {
    label: 'null sentAt',
    body: JSON.stringify({ messages: [{ ...validHistoryEntry, sentAt: null }] }),
  },
  {
    label: 'numeric sentAt',
    body: JSON.stringify({ messages: [{ ...validHistoryEntry, sentAt: 1 }] }),
  },
  ...[
    '',
    'not-a-date',
    '2026-10-15',
    '10/15/2026 09:00:00',
    '2026-02-30T00:00:00Z',
    '2026-10-15T00:00:00+24:00',
  ].map((sentAt) => ({
    label: `invalid RFC 3339 sentAt ${JSON.stringify(sentAt)}`,
    body: JSON.stringify({ messages: [{ ...validHistoryEntry, sentAt }] }),
  })),
];

describe('HttpProviderMessageHistory', () => {
  it('encodes the exact clientRef and validates a complete history observation', async () => {
    const fetch = vi.fn(async (input: string | URL | Request) => {
      const url = new URL(String(input));
      expect(url.pathname).toBe('/v1/messages');
      expect(url.searchParams.get('clientRef')).toBe('ref/with +=:symbols');
      expect(url.search).toBe('?clientRef=ref%2Fwith+%2B%3D%3Asymbols');

      return new Response(
        JSON.stringify({
          messages: [
            {
              ...validHistoryEntry,
              providerExtension: 'ignored',
            },
          ],
          providerMetadata: { version: 2 },
        }),
        { status: 200 },
      );
    });
    const history = new HttpProviderMessageHistory({ baseUrl: 'http://provider:4000', fetch });

    await expect(history.lookup('ref/with +=:symbols')).resolves.toEqual({
      kind: 'OBSERVATION',
      entries: [validHistoryEntry],
    });
  });

  it.each(malformedHistoryBodies)(
    'does not treat $label as delivery evidence',
    async ({ body }) => {
      const history = new HttpProviderMessageHistory({
        baseUrl: 'http://provider:4000',
        fetch: async () => new Response(body, { status: 200 }),
      });

      await expect(history.lookup('client-ref')).resolves.toEqual({
        kind: 'UNAVAILABLE',
        code: 'MALFORMED_HISTORY_RESPONSE',
      });
    },
  );

  it('preserves a normal empty messages array as an empty observation', async () => {
    const history = new HttpProviderMessageHistory({
      baseUrl: 'http://provider:4000',
      fetch: async () => new Response(JSON.stringify({ messages: [] }), { status: 200 }),
    });

    await expect(history.lookup('client-ref')).resolves.toEqual({
      kind: 'OBSERVATION',
      entries: [],
    });
  });

  it.each(['2026-10-15T00:00:00Z', '2026-10-15T09:00:00.123+09:00'])(
    'accepts RFC 3339 provider timestamps: %s',
    async (sentAt) => {
      const history = new HttpProviderMessageHistory({
        baseUrl: 'http://provider:4000',
        fetch: async () =>
          new Response(
            JSON.stringify({
              messages: [{ messageId: 'm1', recipientId: 'u1', sentAt }],
            }),
            { status: 200 },
          ),
      });

      await expect(history.lookup('client-ref')).resolves.toEqual({
        kind: 'OBSERVATION',
        entries: [{ messageId: 'm1', recipientId: 'u1', sentAt }],
      });
    },
  );

  it('classifies HTTP and transport failures without fabricating an empty observation', async () => {
    const unavailable = new HttpProviderMessageHistory({
      baseUrl: 'http://provider:4000',
      fetch: async () => new Response('{"code":"SERVICE_UNAVAILABLE"}', { status: 503 }),
    });
    const transport = new HttpProviderMessageHistory({
      baseUrl: 'http://provider:4000',
      fetch: async () => {
        throw new Error('connection reset');
      },
    });

    await expect(unavailable.lookup('client-ref')).resolves.toEqual({
      kind: 'UNAVAILABLE',
      code: 'HISTORY_HTTP_503',
    });
    await expect(transport.lookup('client-ref')).resolves.toEqual({
      kind: 'UNAVAILABLE',
      code: 'HISTORY_TRANSPORT_ERROR',
    });
  });

  it('cancels non-success bodies and stops an unbounded success stream at 512 KiB', async () => {
    let unavailableCanceled = false;
    const unavailable = new HttpProviderMessageHistory({
      baseUrl: 'http://provider:4000',
      fetch: async () =>
        new Response(
          new ReadableStream({
            start(controller) {
              controller.enqueue(new Uint8Array([1]));
            },
            cancel() {
              unavailableCanceled = true;
            },
          }),
          { status: 503 },
        ),
    });
    await expect(unavailable.lookup('client-ref')).resolves.toMatchObject({
      kind: 'UNAVAILABLE',
      code: 'HISTORY_HTTP_503',
    });
    expect(unavailableCanceled).toBe(true);

    let pulls = 0;
    let oversizedCanceled = false;
    const oversized = new HttpProviderMessageHistory({
      baseUrl: 'http://provider:4000',
      fetch: async () =>
        new Response(
          new ReadableStream({
            pull(controller) {
              pulls += 1;
              controller.enqueue(new Uint8Array(128 * 1024));
            },
            cancel() {
              oversizedCanceled = true;
            },
          }),
          { status: 200 },
        ),
    });
    await expect(oversized.lookup('client-ref')).resolves.toEqual({
      kind: 'UNAVAILABLE',
      code: 'HISTORY_RESPONSE_TOO_LARGE',
    });
    expect(oversizedCanceled).toBe(true);
    expect(pulls).toBeLessThanOrEqual(6);
  });
});
