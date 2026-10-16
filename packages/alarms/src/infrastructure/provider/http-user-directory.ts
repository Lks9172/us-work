import {
  ExpansionAbortedError,
  type UserDirectory,
  type UserDirectoryPage,
  UserDirectoryProtocolError,
  UserDirectoryTransientError,
} from '../../application/expansion';
import {
  cancelResponseBody,
  readBoundedResponseText,
  ResponseBodyTooLargeError,
} from './bounded-response-body';

const MAX_RESPONSE_BYTES = 2 * 1024 * 1024;
const KNOWN_REQUEST_ERROR_CODES = new Set(['INVALID_CURSOR', 'INVALID_REQUEST']);

type FetchFunction = (input: string | URL | Request, init?: RequestInit) => Promise<Response>;

export interface HttpUserDirectoryOptions {
  readonly baseUrl: string;
  readonly timeoutMs?: number;
  readonly fetch?: FetchFunction;
}

function parseRetryAfter(value: string | null): number | null {
  if (value === null) {
    return null;
  }

  const seconds = Number(value);
  if (Number.isFinite(seconds) && seconds >= 0) {
    return Math.min(Math.ceil(seconds * 1_000), 60_000);
  }

  const date = Date.parse(value);
  return Number.isNaN(date) ? null : Math.max(0, Math.min(date - Date.now(), 60_000));
}

function parsePage(payload: unknown, expectedLimit: number): UserDirectoryPage {
  if (payload === null || typeof payload !== 'object' || Array.isArray(payload)) {
    throw new UserDirectoryProtocolError('Users response must be an object');
  }

  const record = payload as Record<string, unknown>;
  if (!Array.isArray(record.users) || record.users.length > expectedLimit) {
    throw new UserDirectoryProtocolError('Users response contains an invalid users array');
  }
  if (record.nextCursor !== null && typeof record.nextCursor !== 'string') {
    throw new UserDirectoryProtocolError('Users response contains an invalid nextCursor');
  }
  if (record.nextCursor === '') {
    throw new UserDirectoryProtocolError('Users response nextCursor must not be empty');
  }

  const recipientIds = record.users.map((candidate, index) => {
    if (candidate === null || typeof candidate !== 'object' || Array.isArray(candidate)) {
      throw new UserDirectoryProtocolError(`users[${index}] must be an object`);
    }

    const user = candidate as Record<string, unknown>;
    if (
      typeof user.id !== 'string' ||
      [...user.id].length < 1 ||
      [...user.id].length > 128 ||
      user.id.trim() !== user.id ||
      typeof user.name !== 'string' ||
      typeof user.phone !== 'string'
    ) {
      throw new UserDirectoryProtocolError(`users[${index}] violates the provider contract`);
    }

    return user.id;
  });

  return { recipientIds, nextCursor: record.nextCursor as string | null };
}

function requestErrorCode(payload: unknown): string {
  if (payload === null || typeof payload !== 'object' || Array.isArray(payload)) {
    return 'UNKNOWN_USER_DIRECTORY_4XX';
  }

  const error = payload as Record<string, unknown>;
  return typeof error.code === 'string' &&
    typeof error.message === 'string' &&
    KNOWN_REQUEST_ERROR_CODES.has(error.code)
    ? error.code
    : 'UNKNOWN_USER_DIRECTORY_4XX';
}

export class HttpUserDirectory implements UserDirectory {
  private readonly baseUrl: URL;
  private readonly timeoutMs: number;
  private readonly fetch: FetchFunction;

  public constructor(options: HttpUserDirectoryOptions) {
    this.baseUrl = new URL(options.baseUrl);
    this.timeoutMs = options.timeoutMs ?? 5_000;
    this.fetch = options.fetch ?? globalThis.fetch;
  }

  public async getPage(
    cursor: string | null,
    limit: number,
    signal?: AbortSignal,
  ): Promise<UserDirectoryPage> {
    const url = new URL('/v1/users', this.baseUrl);
    url.searchParams.set('limit', String(limit));
    if (cursor !== null) {
      url.searchParams.set('cursor', cursor);
    }

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.timeoutMs);
    const abort = (): void => controller.abort();
    signal?.addEventListener('abort', abort, { once: true });

    try {
      const response = await this.fetch(url, {
        method: 'GET',
        headers: { accept: 'application/json' },
        signal: controller.signal,
      });
      if (response.status === 429 || response.status >= 500) {
        await cancelResponseBody(response);
        throw new UserDirectoryTransientError(
          `Users request failed with HTTP ${response.status}`,
          parseRetryAfter(response.headers.get('retry-after')),
        );
      }

      const text = await readBoundedResponseText(response, MAX_RESPONSE_BYTES).catch((error) => {
        if (error instanceof ResponseBodyTooLargeError) {
          throw new UserDirectoryProtocolError('Users response exceeds the size limit');
        }
        throw error;
      });

      let payload: unknown;
      try {
        payload = text.length === 0 ? null : JSON.parse(text);
      } catch (error) {
        throw new UserDirectoryProtocolError('Users response is not valid JSON', { cause: error });
      }

      if (response.status === 200) {
        return parsePage(payload, limit);
      }

      throw new UserDirectoryProtocolError(`Users request failed with HTTP ${response.status}`, {
        code: requestErrorCode(payload),
        httpStatus: response.status,
      });
    } catch (error) {
      if (
        error instanceof UserDirectoryProtocolError ||
        error instanceof UserDirectoryTransientError
      ) {
        throw error;
      }
      if (signal?.aborted === true) {
        throw new ExpansionAbortedError();
      }

      throw new UserDirectoryTransientError('Users request failed before a response', null, {
        cause: error,
      });
    } finally {
      clearTimeout(timeout);
      signal?.removeEventListener('abort', abort);
    }
  }
}
