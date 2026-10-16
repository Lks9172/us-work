import { describe, expect, it } from 'vitest';

import {
  UserDirectoryProtocolError,
  UserDirectoryTransientError,
} from '../../packages/alarms/src/application/expansion';
import { HttpUserDirectory } from '../../packages/alarms/src/infrastructure/provider/http-user-directory';

const validUser = {
  id: 'user-1',
  name: 'Alice',
  phone: '010-0000-0000',
};

const malformedPageCases: readonly {
  readonly label: string;
  readonly payload: unknown;
  readonly limit?: number;
}[] = [
  { label: 'null top-level body', payload: null },
  { label: 'array top-level body', payload: [] },
  { label: 'missing users', payload: { nextCursor: null } },
  { label: 'null users', payload: { users: null, nextCursor: null } },
  { label: 'non-array users', payload: { users: {}, nextCursor: null } },
  {
    label: 'users longer than the requested limit',
    payload: { users: [validUser, { ...validUser, id: 'user-2' }], nextCursor: null },
    limit: 1,
  },
  { label: 'missing nextCursor', payload: { users: [validUser] } },
  { label: 'numeric nextCursor', payload: { users: [validUser], nextCursor: 1 } },
  { label: 'array nextCursor', payload: { users: [validUser], nextCursor: [] } },
  { label: 'empty nextCursor', payload: { users: [validUser], nextCursor: '' } },
  { label: 'null user item', payload: { users: [null], nextCursor: null } },
  { label: 'array user item', payload: { users: [[]], nextCursor: null } },
  {
    label: 'missing user id',
    payload: { users: [{ name: validUser.name, phone: validUser.phone }], nextCursor: null },
  },
  {
    label: 'null user id',
    payload: { users: [{ ...validUser, id: null }], nextCursor: null },
  },
  {
    label: 'numeric user id',
    payload: { users: [{ ...validUser, id: 1 }], nextCursor: null },
  },
  {
    label: 'empty user id',
    payload: { users: [{ ...validUser, id: '' }], nextCursor: null },
  },
  {
    label: 'non-canonical whitespace in user id',
    payload: { users: [{ ...validUser, id: ' user-1' }], nextCursor: null },
  },
  {
    label: 'overlong user id',
    payload: { users: [{ ...validUser, id: 'u'.repeat(129) }], nextCursor: null },
  },
  {
    label: 'missing user name',
    payload: { users: [{ id: validUser.id, phone: validUser.phone }], nextCursor: null },
  },
  {
    label: 'null user name',
    payload: { users: [{ ...validUser, name: null }], nextCursor: null },
  },
  {
    label: 'numeric user name',
    payload: { users: [{ ...validUser, name: 1 }], nextCursor: null },
  },
  {
    label: 'missing user phone',
    payload: { users: [{ id: validUser.id, name: validUser.name }], nextCursor: null },
  },
  {
    label: 'null user phone',
    payload: { users: [{ ...validUser, phone: null }], nextCursor: null },
  },
  {
    label: 'numeric user phone',
    payload: { users: [{ ...validUser, phone: 1 }], nextCursor: null },
  },
];

describe('HttpUserDirectory', () => {
  it('calls the provider wire contract and projects only opaque recipient IDs', async () => {
    let requestedUrl = '';
    const directory = new HttpUserDirectory({
      baseUrl: 'http://provider.test:4000',
      fetch: async (input) => {
        requestedUrl = String(input);
        return new Response(
          JSON.stringify({
            users: [
              { id: 'User-A', name: 'Alice', phone: '010-0000-0000' },
              { id: 'user-a', name: 'Bob', phone: '010-0000-0001' },
            ],
            nextCursor: 'opaque+/=:next',
          }),
          { status: 200, headers: { 'content-type': 'application/json' } },
        );
      },
    });

    await expect(directory.getPage('cursor+/=:segment', 1_000)).resolves.toEqual({
      recipientIds: ['User-A', 'user-a'],
      nextCursor: 'opaque+/=:next',
    });
    const url = new URL(requestedUrl);
    expect(url.pathname).toBe('/v1/users');
    expect(url.searchParams.get('cursor')).toBe('cursor+/=:segment');
    expect(url.searchParams.get('limit')).toBe('1000');
    expect(url.search).toBe('?limit=1000&cursor=cursor%2B%2F%3D%3Asegment');
  });

  it.each(malformedPageCases)(
    'rejects $label as a permanent protocol failure',
    async ({ payload, limit = 100 }) => {
      const directory = new HttpUserDirectory({
        baseUrl: 'http://provider.test:4000',
        fetch: async () => new Response(JSON.stringify(payload), { status: 200 }),
      });

      await expect(directory.getPage(null, limit)).rejects.toBeInstanceOf(
        UserDirectoryProtocolError,
      );
    },
  );

  it('ignores unknown response fields while preserving required contract fields', async () => {
    const directory = new HttpUserDirectory({
      baseUrl: 'http://provider.test:4000',
      fetch: async () =>
        new Response(
          JSON.stringify({
            users: [{ ...validUser, providerExtension: { version: 2 } }],
            nextCursor: null,
            providerMetadata: 'ignored',
          }),
          { status: 200 },
        ),
    });

    await expect(directory.getPage(null, 100)).resolves.toEqual({
      recipientIds: ['user-1'],
      nextCursor: null,
    });
  });

  it('classifies 429/5xx as transient and honors bounded Retry-After', async () => {
    const directory = new HttpUserDirectory({
      baseUrl: 'http://provider.test:4000',
      fetch: async () =>
        new Response(JSON.stringify({ code: 'RATE_LIMITED' }), {
          status: 429,
          headers: { 'retry-after': '2' },
        }),
    });

    await expect(directory.getPage(null, 100)).rejects.toMatchObject({
      constructor: UserDirectoryTransientError,
      retryAfterMs: 2_000,
    });
  });

  it.each(['INVALID_CURSOR', 'INVALID_REQUEST'] as const)(
    'preserves the safe %s code from a valid 400 error envelope',
    async (code) => {
      const directory = new HttpUserDirectory({
        baseUrl: 'http://provider.test:4000',
        fetch: async () =>
          new Response(JSON.stringify({ code, message: 'provider detail must not be exposed' }), {
            status: 400,
          }),
      });

      await expect(directory.getPage(null, 100)).rejects.toMatchObject({
        constructor: UserDirectoryProtocolError,
        code,
        httpStatus: 400,
        message: 'Users request failed with HTTP 400',
      });
    },
  );

  it.each([
    { label: 'unknown code', body: { code: 'UNEXPECTED', message: 'detail' } },
    { label: 'missing message', body: { code: 'INVALID_CURSOR' } },
    { label: 'malformed body', body: [] },
  ])('uses a stable safe code for a $label 4xx response', async ({ body }) => {
    const directory = new HttpUserDirectory({
      baseUrl: 'http://provider.test:4000',
      fetch: async () => new Response(JSON.stringify(body), { status: 400 }),
    });

    await expect(directory.getPage(null, 100)).rejects.toMatchObject({
      constructor: UserDirectoryProtocolError,
      code: 'UNKNOWN_USER_DIRECTORY_4XX',
      httpStatus: 400,
    });
  });

  it('cancels transient bodies and bounds streaming success responses before buffering them', async () => {
    let transientCanceled = false;
    const transient = new HttpUserDirectory({
      baseUrl: 'http://provider.test:4000',
      fetch: async () =>
        new Response(
          new ReadableStream({
            start(controller) {
              controller.enqueue(new Uint8Array([1]));
            },
            cancel() {
              transientCanceled = true;
            },
          }),
          { status: 503 },
        ),
    });
    await expect(transient.getPage(null, 100)).rejects.toBeInstanceOf(UserDirectoryTransientError);
    expect(transientCanceled).toBe(true);

    let pulls = 0;
    let oversizedCanceled = false;
    const oversized = new HttpUserDirectory({
      baseUrl: 'http://provider.test:4000',
      fetch: async () =>
        new Response(
          new ReadableStream({
            pull(controller) {
              pulls += 1;
              controller.enqueue(new Uint8Array(512 * 1024));
            },
            cancel() {
              oversizedCanceled = true;
            },
          }),
          { status: 200 },
        ),
    });
    await expect(oversized.getPage(null, 100)).rejects.toMatchObject({
      constructor: UserDirectoryProtocolError,
      message: 'Users response exceeds the size limit',
    });
    expect(oversizedCanceled).toBe(true);
    expect(pulls).toBeLessThanOrEqual(6);
  });
});
