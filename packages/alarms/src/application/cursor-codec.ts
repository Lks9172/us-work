import { createHmac, timingSafeEqual } from 'node:crypto';

import { InvalidCursorError } from './alarm-errors';
import type { AlarmCursorPosition } from './alarm-repository';

interface CursorPayload {
  readonly v: 1;
  readonly createdAt: string;
  readonly id: string;
}

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export class AlarmCursorCodec {
  public constructor(private readonly signingKey: string) {
    if (signingKey.length < 16) {
      throw new Error('Cursor signing key must contain at least 16 characters');
    }
  }

  public encode(position: AlarmCursorPosition): string {
    const body = Buffer.from(
      JSON.stringify({
        v: 1,
        createdAt: position.createdAt,
        id: position.id,
      } satisfies CursorPayload),
    ).toString('base64url');
    const signature = this.sign(body);

    return `${body}.${signature}`;
  }

  public decode(cursor: string): AlarmCursorPosition {
    const [body, signature, extra] = cursor.split('.');
    if (body === undefined || signature === undefined || extra !== undefined) {
      throw new InvalidCursorError();
    }

    const expected = Buffer.from(this.sign(body));
    const actual = Buffer.from(signature);
    if (actual.length !== expected.length || !timingSafeEqual(actual, expected)) {
      throw new InvalidCursorError();
    }

    try {
      const parsed = JSON.parse(
        Buffer.from(body, 'base64url').toString('utf8'),
      ) as Partial<CursorPayload>;
      if (
        parsed.v !== 1 ||
        typeof parsed.createdAt !== 'string' ||
        Number.isNaN(Date.parse(parsed.createdAt)) ||
        typeof parsed.id !== 'string' ||
        !UUID_PATTERN.test(parsed.id)
      ) {
        throw new InvalidCursorError();
      }

      return { createdAt: parsed.createdAt, id: parsed.id };
    } catch (error) {
      if (error instanceof InvalidCursorError) {
        throw error;
      }

      throw new InvalidCursorError();
    }
  }

  private sign(body: string): string {
    return createHmac('sha256', this.signingKey).update(body).digest('base64url');
  }
}
