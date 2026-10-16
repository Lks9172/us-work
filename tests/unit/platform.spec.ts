import { getEventListeners } from 'node:events';

import { describe, expect, it } from 'vitest';

import {
  abortableDelay,
  boundedExponentialBackoffMs,
  parseTcpPort,
  requireEnvironmentString,
  requireEnvironmentUrl,
  safeErrorMetadata,
} from '../../packages/platform/src';

describe('abortableDelay', () => {
  it('removes its abort listener after the timer completes', async () => {
    const controller = new AbortController();

    await abortableDelay(1, controller.signal);

    expect(getEventListeners(controller.signal, 'abort')).toHaveLength(0);
  });

  it('removes its abort listener when interrupted', async () => {
    const controller = new AbortController();
    const waiting = abortableDelay(60_000, controller.signal);

    expect(getEventListeners(controller.signal, 'abort')).toHaveLength(1);
    controller.abort();
    await waiting;

    expect(getEventListeners(controller.signal, 'abort')).toHaveLength(0);
  });
});

describe('boundedExponentialBackoffMs', () => {
  it('grows exponentially, caps, and restarts from the base after the caller resets its streak', () => {
    expect(
      [1, 2, 3, 4, 5, 6, 7].map((failureCount) =>
        boundedExponentialBackoffMs(failureCount, 250, 5_000),
      ),
    ).toEqual([250, 500, 1_000, 2_000, 4_000, 5_000, 5_000]);
    expect(boundedExponentialBackoffMs(1, 250, 5_000)).toBe(250);
  });

  it.each([
    () => boundedExponentialBackoffMs(0),
    () => boundedExponentialBackoffMs(1, 0),
    () => boundedExponentialBackoffMs(1, 500, 499),
  ])('rejects invalid retry configuration', (calculate) => {
    expect(calculate).toThrow(RangeError);
  });
});

describe('safeErrorMetadata', () => {
  it('keeps only allow-listed name/code metadata without reading sensitive Error fields', () => {
    let sensitiveFieldReads = 0;
    const error = Object.assign(new Error('phone=010-1234-5678 body=must-not-leak'), {
      name: 'PersistenceUnavailableError',
      code: '57P01',
    });
    for (const property of ['message', 'stack', 'cause'] as const) {
      Object.defineProperty(error, property, {
        configurable: true,
        get: () => {
          sensitiveFieldReads += 1;
          throw new Error(`${property}=phone=010-1234-5678`);
        },
      });
    }

    expect(safeErrorMetadata(error)).toEqual({
      errorName: 'Error',
      errorCode: '57P01',
    });
    expect(sensitiveFieldReads).toBe(0);
  });

  it('drops untrusted names, phone-like codes, and non-Error thrown values', () => {
    const error = Object.assign(new Error('password=must-not-leak'), {
      name: 'Alice01012345678Error',
      code: 'USER_01012345678',
    });

    expect(safeErrorMetadata(error)).toEqual({ errorName: 'Error' });
    expect(safeErrorMetadata('body=must-not-leak')).toEqual({ errorName: 'UnknownError' });
    expect(JSON.stringify(safeErrorMetadata(error))).not.toContain('01012345678');
  });

  it('is total for revoked proxies without inspecting their target', () => {
    const revocable = Proxy.revocable(new Error('body=must-not-leak'), {});
    revocable.revoke();

    expect(safeErrorMetadata(revocable.proxy)).toEqual({ errorName: 'UnknownError' });
  });
});

describe('runtime configuration', () => {
  it('accepts only nonblank required values and allowed URL protocols', () => {
    expect(requireEnvironmentString('value', 'NAME')).toBe('value');
    expect(() => requireEnvironmentString('   ', 'NAME')).toThrow('NAME is required');
    expect(
      requireEnvironmentUrl('postgresql://db:5432/app', 'DATABASE_URL', [
        'postgres:',
        'postgresql:',
      ]),
    ).toBe('postgresql://db:5432/app');
    expect(() =>
      requireEnvironmentUrl('http://db:5432/app', 'DATABASE_URL', ['postgres:', 'postgresql:']),
    ).toThrow('DATABASE_URL must use postgres: or postgresql: with a host');
    expect(() => requireEnvironmentUrl('not a url', 'REDIS_URL', ['redis:'])).toThrow(
      'REDIS_URL must be a valid URL',
    );
  });

  it.each([
    { value: undefined, expected: 3_000 },
    { value: '1', expected: 1 },
    { value: '65535', expected: 65_535 },
  ])('parses a bounded TCP port', ({ value, expected }) => {
    expect(parseTcpPort(value, 3_000)).toBe(expected);
  });

  it.each(['', '0', '65536', '-1', '3000x', '3.5'])('rejects an invalid TCP port: %s', (value) => {
    expect(() => parseTcpPort(value, 3_000)).toThrow('PORT must be an integer between 1 and 65535');
  });
});
