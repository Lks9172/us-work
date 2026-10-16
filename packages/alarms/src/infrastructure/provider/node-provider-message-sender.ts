import {
  type ClientRequest,
  Agent as HttpAgent,
  request as httpRequest,
  type IncomingMessage,
  type RequestOptions,
} from 'node:http';
import { Agent as HttpsAgent, request as httpsRequest } from 'node:https';

import type { ProviderMessageSender, ProviderSendResult } from '../../application/dispatch';

const MAX_RESPONSE_BYTES = 64 * 1024;
const KNOWN_PERMANENT_CODES = new Set([
  'INVALID_REQUEST',
  'UNKNOWN_RECIPIENT',
  'RECIPIENT_BLOCKED',
]);
const IMF_FIXDATE =
  /^(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun), \d{2} (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{4} \d{2}:\d{2}:\d{2} GMT$/;

export interface NodeProviderMessageSenderOptions {
  readonly baseUrl: string;
  readonly responseTimeoutMs?: number;
  /** Test seam for the synchronous request-construction boundary. */
  readonly requestFactory?: NodeProviderRequestFactory;
}

export type NodeProviderRequestFactory = (
  endpoint: URL,
  options: RequestOptions,
  onResponse: (response: IncomingMessage) => void,
) => ClientRequest;

function retryAfterMilliseconds(value: string | string[] | undefined): number {
  const first = Array.isArray(value) ? value[0] : value;
  if (first === undefined) {
    return 1_000;
  }

  if (/^\d+$/.test(first)) {
    const seconds = Number(first);
    if (Number.isSafeInteger(seconds)) {
      return Math.max(100, Math.min(seconds * 1_000, 60_000));
    }
  }

  if (!IMF_FIXDATE.test(first)) {
    return 1_000;
  }

  const date = Date.parse(first);
  return Number.isNaN(date) ? 1_000 : Math.max(100, Math.min(date - Date.now(), 60_000));
}

function parseObject(text: string): Record<string, unknown> | null {
  try {
    const parsed: unknown = JSON.parse(text);
    return parsed !== null && typeof parsed === 'object' && !Array.isArray(parsed)
      ? (parsed as Record<string, unknown>)
      : null;
  } catch {
    return null;
  }
}

function parseErrorCode(payload: Record<string, unknown> | null): string | null {
  return typeof payload?.code === 'string' && typeof payload.message === 'string'
    ? payload.code
    : null;
}

export class NodeProviderMessageSender implements ProviderMessageSender {
  private readonly endpoint: URL;
  private readonly responseTimeoutMs: number;
  private readonly requestFactory: NodeProviderRequestFactory | undefined;
  private readonly httpAgent = new HttpAgent({ keepAlive: true, maxSockets: 64 });
  private readonly httpsAgent = new HttpsAgent({ keepAlive: true, maxSockets: 64 });

  public constructor(options: NodeProviderMessageSenderOptions) {
    this.endpoint = new URL('/v1/messages', options.baseUrl);
    this.responseTimeoutMs = options.responseTimeoutMs ?? 35_000;
    this.requestFactory = options.requestFactory;
  }

  public send(
    message: {
      readonly alarmId: string;
      readonly recipientId: string;
      readonly body: string;
      readonly clientRef: string;
    },
    signal?: AbortSignal,
  ): Promise<ProviderSendResult> {
    const body = Buffer.from(JSON.stringify(message), 'utf8');
    const transport = this.endpoint.protocol === 'https:' ? httpsRequest : httpRequest;
    const agent = this.endpoint.protocol === 'https:' ? this.httpsAgent : this.httpAgent;

    return new Promise((resolve) => {
      let settled = false;
      const settle = (result: ProviderSendResult): void => {
        if (!settled) {
          settled = true;
          resolve(result);
        }
      };

      const onResponse = (response: IncomingMessage): void => {
        const chunks: Buffer[] = [];
        let size = 0;

        response.on('data', (chunk: Buffer) => {
          size += chunk.byteLength;
          if (size > MAX_RESPONSE_BYTES) {
            response.destroy(new Error('Provider response exceeded the size limit'));
            return;
          }
          chunks.push(chunk);
        });
        response.on('error', () => {
          settle({ kind: 'AMBIGUOUS', code: 'PROVIDER_RESPONSE_ERROR' });
        });
        response.on('end', () => {
          if (size > MAX_RESPONSE_BYTES) {
            settle({ kind: 'AMBIGUOUS', code: 'PROVIDER_RESPONSE_TOO_LARGE' });
            return;
          }

          const payload = parseObject(Buffer.concat(chunks).toString('utf8'));
          const errorCode = parseErrorCode(payload);
          if (response.statusCode === 202) {
            const messageId = payload?.messageId;
            settle(
              typeof messageId === 'string' && messageId.length > 0
                ? { kind: 'ACCEPTED', messageId }
                : { kind: 'AMBIGUOUS', code: 'MALFORMED_PROVIDER_202' },
            );
            return;
          }
          if (response.statusCode === 400) {
            const code =
              errorCode !== null && KNOWN_PERMANENT_CODES.has(errorCode)
                ? errorCode
                : 'UNKNOWN_PROVIDER_4XX';
            settle({ kind: 'PERMANENT_FAILURE', code, httpStatus: 400 });
            return;
          }
          if (response.statusCode === 429 && errorCode === 'RATE_LIMITED') {
            settle({
              kind: 'RATE_LIMITED',
              code: 'RATE_LIMITED',
              httpStatus: 429,
              retryAfterMs: retryAfterMilliseconds(response.headers['retry-after']),
            });
            return;
          }
          if (response.statusCode === 500 && errorCode === 'INTERNAL_ERROR') {
            settle({
              kind: 'TRANSIENT_FAILURE',
              code: errorCode,
              httpStatus: 500,
            });
            return;
          }
          if (response.statusCode === 503 && errorCode === 'SERVICE_UNAVAILABLE') {
            settle({
              kind: 'TRANSIENT_FAILURE',
              code: errorCode,
              httpStatus: 503,
            });
            return;
          }

          settle({ kind: 'AMBIGUOUS', code: 'UNEXPECTED_PROVIDER_RESPONSE' });
        });
      };
      let request: ClientRequest;
      try {
        request = (this.requestFactory ?? transport)(
          this.endpoint,
          {
            method: 'POST',
            agent,
            headers: {
              accept: 'application/json',
              'content-type': 'application/json',
              'content-length': body.byteLength,
            },
          },
          onResponse,
        );
      } catch {
        // No ClientRequest exists and request.end has not been called. This is the only
        // production adapter boundary where we can prove that the request body was not handed
        // to Node's HTTP transport. All errors after a request object is returned stay ambiguous.
        settle({ kind: 'DEFINITELY_NOT_SENT', code: 'PROVIDER_REQUEST_CREATION_ERROR' });
        return;
      }

      const timeout = setTimeout(() => {
        request.destroy(new Error('Provider response timeout'));
        settle({ kind: 'AMBIGUOUS', code: 'PROVIDER_TIMEOUT' });
      }, this.responseTimeoutMs);
      timeout.unref();

      const abort = (): void => {
        request.destroy(new Error('Dispatch shutdown'));
        settle({ kind: 'AMBIGUOUS', code: 'PROVIDER_ABORTED' });
      };
      signal?.addEventListener('abort', abort, { once: true });

      request.once('close', () => {
        clearTimeout(timeout);
        signal?.removeEventListener('abort', abort);
      });
      request.once('error', () => {
        settle({ kind: 'AMBIGUOUS', code: 'PROVIDER_TRANSPORT_AMBIGUOUS' });
      });

      try {
        request.end(body);
      } catch {
        settle({ kind: 'AMBIGUOUS', code: 'PROVIDER_COMMIT_ERROR' });
      }
    });
  }

  public close(): void {
    this.httpAgent.destroy();
    this.httpsAgent.destroy();
  }
}
