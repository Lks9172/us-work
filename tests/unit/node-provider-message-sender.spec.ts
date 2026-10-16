import { EventEmitter } from 'node:events';
import { type ClientRequest, createServer, type Server } from 'node:http';

import { afterEach, describe, expect, it, vi } from 'vitest';

import { NodeProviderMessageSender } from '../../packages/alarms/src/infrastructure/provider/node-provider-message-sender';

const servers: Server[] = [];

afterEach(async () => {
  await Promise.all(
    servers.splice(0).map(
      (server) =>
        new Promise<void>((resolve, reject) => {
          server.close((error) => (error === undefined ? resolve() : reject(error)));
        }),
    ),
  );
});

async function listen(
  handler: Parameters<typeof createServer>[0],
): Promise<{ readonly server: Server; readonly baseUrl: string }> {
  const server = createServer(handler);
  servers.push(server);
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  if (address === null || typeof address === 'string') {
    throw new Error('Test server did not expose a TCP address');
  }
  return { server, baseUrl: `http://127.0.0.1:${address.port}` };
}

const request = {
  alarmId: 'alarm-1',
  recipientId: 'user-1',
  body: 'message body',
  clientRef: 'client-ref-1',
};

function expectNoAcceptanceEvidence(result: unknown): void {
  expect(result).not.toHaveProperty('messageId');
  expect(result).not.toHaveProperty('entries');
}

describe('NodeProviderMessageSender', () => {
  it('[RET-17] proves a synchronous pre-request construction error was not sent', async () => {
    const rawError = 'request factory leaked phone=010-1234-5678';
    const sender = new NodeProviderMessageSender({
      baseUrl: 'http://127.0.0.1:1',
      requestFactory: () => {
        throw new Error(rawError);
      },
    });

    const result = await sender.send(request);
    expect(result).toEqual({
      kind: 'DEFINITELY_NOT_SENT',
      code: 'PROVIDER_REQUEST_CREATION_ERROR',
    });
    expect(JSON.stringify(result)).not.toContain(rawError);
    sender.close();
  });

  it('[RET-17] keeps a synchronous request.end error ambiguous after request construction', async () => {
    const requestObject = Object.assign(new EventEmitter(), {
      destroy: vi.fn(),
      end: vi.fn(() => {
        throw new Error('request.end failed after construction');
      }),
    }) as unknown as ClientRequest;
    const sender = new NodeProviderMessageSender({
      baseUrl: 'http://127.0.0.1:1',
      requestFactory: () => requestObject,
    });

    const resultPromise = sender.send(request);
    requestObject.emit('close');
    await expect(resultPromise).resolves.toEqual({
      kind: 'AMBIGUOUS',
      code: 'PROVIDER_COMMIT_ERROR',
    });
    sender.close();
  });

  it('sends the exact provider payload and accepts a valid 202', async () => {
    let receivedBody = '';
    const { baseUrl } = await listen((incoming, response) => {
      incoming.setEncoding('utf8');
      incoming.on('data', (chunk: string) => {
        receivedBody += chunk;
      });
      incoming.on('end', () => {
        response.writeHead(202, { 'content-type': 'application/json' });
        response.end(JSON.stringify({ messageId: 'provider-message-1' }));
      });
    });
    const sender = new NodeProviderMessageSender({ baseUrl });

    await expect(sender.send(request)).resolves.toEqual({
      kind: 'ACCEPTED',
      messageId: 'provider-message-1',
    });
    expect(JSON.parse(receivedBody)).toEqual(request);
    sender.close();
  });

  it('normalizes unknown 400 codes to a permanent non-retry result', async () => {
    const { baseUrl } = await listen((_incoming, response) => {
      response.writeHead(400, { 'content-type': 'application/json' });
      response.end(JSON.stringify({ code: 'NEW_UNKNOWN_CODE', message: 'no' }));
    });
    const sender = new NodeProviderMessageSender({ baseUrl });

    const result = await sender.send(request);
    expect(result).toEqual({
      kind: 'PERMANENT_FAILURE',
      code: 'UNKNOWN_PROVIDER_4XX',
      httpStatus: 400,
    });
    expectNoAcceptanceEvidence(result);
    sender.close();
  });

  it.each(['INVALID_REQUEST', 'UNKNOWN_RECIPIENT', 'RECIPIENT_BLOCKED'])(
    'preserves the known permanent provider code %s',
    async (code) => {
      const { baseUrl } = await listen((_incoming, response) => {
        response.writeHead(400, { 'content-type': 'application/json' });
        response.end(JSON.stringify({ code, message: 'permanent' }));
      });
      const sender = new NodeProviderMessageSender({ baseUrl });

      const result = await sender.send(request);
      expect(result).toEqual({
        kind: 'PERMANENT_FAILURE',
        code,
        httpStatus: 400,
      });
      expectNoAcceptanceEvidence(result);
      sender.close();
    },
  );

  it('does not trust a permanent provider code when the required error message is missing', async () => {
    const { baseUrl } = await listen((_incoming, response) => {
      response.writeHead(400, { 'content-type': 'application/json' });
      response.end(JSON.stringify({ code: 'RECIPIENT_BLOCKED' }));
    });
    const sender = new NodeProviderMessageSender({ baseUrl });

    await expect(sender.send(request)).resolves.toEqual({
      kind: 'PERMANENT_FAILURE',
      code: 'UNKNOWN_PROVIDER_4XX',
      httpStatus: 400,
    });
    sender.close();
  });

  it.each([
    { status: 500 as const, code: 'INTERNAL_ERROR' },
    { status: 503 as const, code: 'SERVICE_UNAVAILABLE' },
  ])('classifies a known-unsent $status as transient', async ({ status, code }) => {
    const { baseUrl } = await listen((_incoming, response) => {
      response.writeHead(status, { 'content-type': 'application/json' });
      response.end(JSON.stringify({ code, message: 'retry later' }));
    });
    const sender = new NodeProviderMessageSender({ baseUrl });

    const result = await sender.send(request);
    expect(result).toEqual({
      kind: 'TRANSIENT_FAILURE',
      code,
      httpStatus: status,
    });
    expectNoAcceptanceEvidence(result);
    sender.close();
  });

  it('bounds an oversized provider error without exposing its raw message', async () => {
    const rawProviderMessage = `phone=010-1234-5678:${'x'.repeat(70 * 1024)}`;
    const { baseUrl } = await listen((_incoming, response) => {
      response.writeHead(400, { 'content-type': 'application/json' });
      response.end(JSON.stringify({ code: 'RECIPIENT_BLOCKED', message: rawProviderMessage }));
    });
    const sender = new NodeProviderMessageSender({ baseUrl });

    const result = await sender.send(request);
    expect(result).toMatchObject({ kind: 'AMBIGUOUS' });
    expect(JSON.stringify(result)).not.toContain('010-1234-5678');
    expectNoAcceptanceEvidence(result);
    sender.close();
  });

  it.each([429, 500, 503] as const)(
    'keeps a malformed %s error envelope ambiguous',
    async (status) => {
      const { baseUrl } = await listen((_incoming, response) => {
        response.writeHead(status, {
          'content-type': 'application/json',
          ...(status === 429 ? { 'retry-after': '1' } : {}),
        });
        response.end(JSON.stringify({ code: status === 429 ? 'RATE_LIMITED' : 'INTERNAL_ERROR' }));
      });
      const sender = new NodeProviderMessageSender({ baseUrl });

      await expect(sender.send(request)).resolves.toEqual({
        kind: 'AMBIGUOUS',
        code: 'UNEXPECTED_PROVIDER_RESPONSE',
      });
      sender.close();
    },
  );

  it.each([
    { status: 500 as const, code: 'SERVICE_UNAVAILABLE' },
    { status: 503 as const, code: 'INTERNAL_ERROR' },
  ])('keeps a mismatched $status/$code provider contract ambiguous', async ({ status, code }) => {
    const { baseUrl } = await listen((_incoming, response) => {
      response.writeHead(status, { 'content-type': 'application/json' });
      response.end(JSON.stringify({ code, message: 'mismatched provider response' }));
    });
    const sender = new NodeProviderMessageSender({ baseUrl });

    await expect(sender.send(request)).resolves.toEqual({
      kind: 'AMBIGUOUS',
      code: 'UNEXPECTED_PROVIDER_RESPONSE',
    });
    sender.close();
  });

  it.each(['{}', '{"messageId":""}', 'not-json'])(
    'keeps a malformed 202 ambiguous instead of confirming SENT: %s',
    async (body) => {
      const { baseUrl } = await listen((_incoming, response) => {
        response.writeHead(202, { 'content-type': 'application/json' });
        response.end(body);
      });
      const sender = new NodeProviderMessageSender({ baseUrl });

      await expect(sender.send(request)).resolves.toEqual({
        kind: 'AMBIGUOUS',
        code: 'MALFORMED_PROVIDER_202',
      });
      sender.close();
    },
  );

  it('[RET-10][RET-17] treats timeout after request.end as ambiguous and never as safe retry', async () => {
    const { baseUrl } = await listen(() => undefined);
    const sender = new NodeProviderMessageSender({ baseUrl, responseTimeoutMs: 10 });

    await expect(sender.send(request)).resolves.toMatchObject({
      kind: 'AMBIGUOUS',
      code: 'PROVIDER_TIMEOUT',
    });
    sender.close();
  });

  it('[RET-17] keeps a connection reset before response headers ambiguous', async () => {
    const { baseUrl } = await listen((incoming) => {
      incoming.socket.destroy();
    });
    const sender = new NodeProviderMessageSender({ baseUrl, responseTimeoutMs: 250 });

    await expect(sender.send(request)).resolves.toEqual({
      kind: 'AMBIGUOUS',
      code: 'PROVIDER_TRANSPORT_AMBIGUOUS',
    });
    sender.close();
  });

  it('[RET-17] keeps a premature EOF after response headers ambiguous', async () => {
    const { baseUrl } = await listen((_incoming, response) => {
      response.writeHead(202, {
        'content-type': 'application/json',
        'content-length': '64',
      });
      response.write('{"messageId":"partial');
      setTimeout(() => response.destroy(), 5);
    });
    const sender = new NodeProviderMessageSender({ baseUrl, responseTimeoutMs: 250 });

    await expect(sender.send(request)).resolves.toEqual({
      kind: 'AMBIGUOUS',
      code: 'PROVIDER_RESPONSE_ERROR',
    });
    sender.close();
  });

  it('[RET-10] accepts a slow valid 202 that completes before the response timeout', async () => {
    const { baseUrl } = await listen((_incoming, response) => {
      setTimeout(() => {
        response.writeHead(202, { 'content-type': 'application/json' });
        response.end(JSON.stringify({ messageId: 'slow-provider-message' }));
      }, 25);
    });
    const sender = new NodeProviderMessageSender({ baseUrl, responseTimeoutMs: 250 });

    await expect(sender.send(request)).resolves.toEqual({
      kind: 'ACCEPTED',
      messageId: 'slow-provider-message',
    });
    sender.close();
  });

  it.each([
    { header: '2', expected: 2_000 },
    { header: '0', expected: 100 },
    { header: '0.5', expected: 1_000 },
    { header: '-1', expected: 1_000 },
    { header: '999999999999999999', expected: 1_000 },
    { header: 'invalid', expected: 1_000 },
  ])('[RET-08] parses bounded integer-second Retry-After $header', async ({ header, expected }) => {
    const { baseUrl } = await listen((_incoming, response) => {
      response.writeHead(429, {
        'content-type': 'application/json',
        'retry-after': header,
      });
      response.end(JSON.stringify({ code: 'RATE_LIMITED', message: 'slow down' }));
    });
    const sender = new NodeProviderMessageSender({ baseUrl });

    const result = await sender.send(request);
    expect(result).toEqual({
      kind: 'RATE_LIMITED',
      code: 'RATE_LIMITED',
      httpStatus: 429,
      retryAfterMs: expected,
    });
    expectNoAcceptanceEvidence(result);
    sender.close();
  });

  it('[RET-08] uses the bounded fallback when Retry-After is missing', async () => {
    const { baseUrl } = await listen((_incoming, response) => {
      response.writeHead(429, { 'content-type': 'application/json' });
      response.end(JSON.stringify({ code: 'RATE_LIMITED', message: 'slow down' }));
    });
    const sender = new NodeProviderMessageSender({ baseUrl });

    await expect(sender.send(request)).resolves.toEqual({
      kind: 'RATE_LIMITED',
      code: 'RATE_LIMITED',
      httpStatus: 429,
      retryAfterMs: 1_000,
    });
    sender.close();
  });

  it.each([
    {
      name: 'future IMF-fixdate',
      header: (): string => new Date(Date.now() + 5_000).toUTCString(),
      assertDelay: (milliseconds: number): void => {
        expect(milliseconds).toBeGreaterThanOrEqual(3_500);
        expect(milliseconds).toBeLessThanOrEqual(5_000);
      },
    },
    {
      name: 'past IMF-fixdate',
      header: (): string => new Date(Date.now() - 5_000).toUTCString(),
      assertDelay: (milliseconds: number): void => expect(milliseconds).toBe(100),
    },
    {
      name: 'invalid HTTP-date',
      header: (): string => 'Sun, 99 Not 0000 99:99:99 GMT',
      assertDelay: (milliseconds: number): void => expect(milliseconds).toBe(1_000),
    },
  ])('[RET-08] handles defensive Retry-After $name', async ({ header, assertDelay }) => {
    const { baseUrl } = await listen((_incoming, response) => {
      response.writeHead(429, {
        'content-type': 'application/json',
        'retry-after': header(),
      });
      response.end(JSON.stringify({ code: 'RATE_LIMITED', message: 'slow down' }));
    });
    const sender = new NodeProviderMessageSender({ baseUrl });

    const result = await sender.send(request);
    expect(result).toMatchObject({ kind: 'RATE_LIMITED', code: 'RATE_LIMITED', httpStatus: 429 });
    if (result.kind !== 'RATE_LIMITED') {
      throw new Error('Expected a rate-limited result');
    }
    assertDelay(result.retryAfterMs);
    sender.close();
  });
});
