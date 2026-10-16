import type { HistoryLookupResult, ProviderMessageHistory } from '../../application/reconciliation';
import type { ProviderEvidenceEntry } from '../../domain/delivery-evidence';
import {
  cancelResponseBody,
  readBoundedResponseText,
  ResponseBodyTooLargeError,
} from './bounded-response-body';

const MAX_RESPONSE_BYTES = 512 * 1024;

type FetchFunction = (input: string | URL | Request, init?: RequestInit) => Promise<Response>;

const RFC_3339_DATE_TIME =
  /^(\d{4})-(\d{2})-(\d{2})T([01]\d|2[0-3]):([0-5]\d):([0-5]\d)(?:\.\d+)?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)$/;

export interface HttpProviderMessageHistoryOptions {
  readonly baseUrl: string;
  readonly timeoutMs?: number;
  readonly fetch?: FetchFunction;
}

function isRfc3339DateTime(value: string): boolean {
  const match = RFC_3339_DATE_TIME.exec(value);
  if (match === null) {
    return false;
  }
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const daysInMonth = new Date(Date.UTC(year, month, 0)).getUTCDate();
  return (
    month >= 1 && month <= 12 && day >= 1 && day <= daysInMonth && !Number.isNaN(Date.parse(value))
  );
}

function parseEntries(payload: unknown): readonly ProviderEvidenceEntry[] {
  if (payload === null || typeof payload !== 'object' || Array.isArray(payload)) {
    throw new Error('History response must be an object');
  }
  const messages = (payload as Record<string, unknown>).messages;
  if (!Array.isArray(messages)) {
    throw new Error('History response must contain messages');
  }

  return messages.map((candidate, index) => {
    if (candidate === null || typeof candidate !== 'object' || Array.isArray(candidate)) {
      throw new Error(`messages[${index}] must be an object`);
    }
    const record = candidate as Record<string, unknown>;
    if (
      typeof record.messageId !== 'string' ||
      record.messageId.length === 0 ||
      typeof record.recipientId !== 'string' ||
      record.recipientId.length === 0 ||
      typeof record.sentAt !== 'string' ||
      !isRfc3339DateTime(record.sentAt)
    ) {
      throw new Error(`messages[${index}] violates the provider history contract`);
    }
    return {
      messageId: record.messageId,
      recipientId: record.recipientId,
      sentAt: record.sentAt,
    };
  });
}

export class HttpProviderMessageHistory implements ProviderMessageHistory {
  private readonly baseUrl: URL;
  private readonly timeoutMs: number;
  private readonly fetch: FetchFunction;

  public constructor(options: HttpProviderMessageHistoryOptions) {
    this.baseUrl = new URL(options.baseUrl);
    this.timeoutMs = options.timeoutMs ?? 5_000;
    this.fetch = options.fetch ?? globalThis.fetch;
  }

  public async lookup(clientRef: string, signal?: AbortSignal): Promise<HistoryLookupResult> {
    const url = new URL('/v1/messages', this.baseUrl);
    url.searchParams.set('clientRef', clientRef);
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
      if (response.status !== 200) {
        await cancelResponseBody(response);
        return { kind: 'UNAVAILABLE', code: `HISTORY_HTTP_${response.status}` };
      }
      const text = await readBoundedResponseText(response, MAX_RESPONSE_BYTES);

      try {
        return { kind: 'OBSERVATION', entries: parseEntries(JSON.parse(text)) };
      } catch {
        return { kind: 'UNAVAILABLE', code: 'MALFORMED_HISTORY_RESPONSE' };
      }
    } catch (error) {
      if (error instanceof ResponseBodyTooLargeError) {
        return { kind: 'UNAVAILABLE', code: 'HISTORY_RESPONSE_TOO_LARGE' };
      }
      return { kind: 'UNAVAILABLE', code: 'HISTORY_TRANSPORT_ERROR' };
    } finally {
      clearTimeout(timeout);
      signal?.removeEventListener('abort', abort);
    }
  }
}
