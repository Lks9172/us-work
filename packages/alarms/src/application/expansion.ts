export interface ExpansionLease {
  readonly alarmId: string;
  readonly runGeneration: string;
  readonly leaseGeneration: number;
  readonly leaseToken: string;
  readonly nextCursor: string | null;
}

export interface UserDirectoryPage {
  readonly recipientIds: readonly string[];
  readonly nextCursor: string | null;
}

export type ExpansionPageOutcome = 'CONTINUE' | 'COMPLETED' | 'CANCELED' | 'STALE';

export interface ExpansionDuplicateRecipientEvent {
  readonly alarmId: string;
  readonly runGeneration: string;
  readonly leaseGeneration: number;
  readonly receivedCount: number;
  readonly insertedCount: number;
  readonly duplicateCount: number;
}

export interface ExpansionEventSink {
  recordDuplicateRecipients(event: ExpansionDuplicateRecipientEvent): void;
}

export interface ExpansionRepository {
  claimNext(leaseDurationMs: number): Promise<ExpansionLease | null>;
  savePage(
    lease: ExpansionLease,
    page: UserDirectoryPage,
    leaseDurationMs: number,
  ): Promise<ExpansionPageOutcome>;
  fail(lease: ExpansionLease): Promise<'FAILED' | 'CANCELED' | 'STALE'>;
}

export interface UserDirectory {
  getPage(cursor: string | null, limit: number, signal?: AbortSignal): Promise<UserDirectoryPage>;
}

export class UserDirectoryTransientError extends Error {
  public constructor(
    message: string,
    public readonly retryAfterMs: number | null = null,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = 'UserDirectoryTransientError';
  }
}

export class UserDirectoryProtocolError extends Error {
  public readonly code: string;
  public readonly httpStatus: number | null;

  public constructor(
    message: string,
    options?: ErrorOptions & { readonly code?: string; readonly httpStatus?: number },
  ) {
    super(message, options);
    this.name = 'UserDirectoryProtocolError';
    this.code = options?.code ?? 'USER_DIRECTORY_PROTOCOL_ERROR';
    this.httpStatus = options?.httpStatus ?? null;
  }
}

export class ExpansionAbortedError extends Error {
  public constructor() {
    super('Expansion was aborted');
    this.name = 'ExpansionAbortedError';
  }
}

export interface ExpansionProcessorOptions {
  readonly pageSize?: number;
  readonly leaseDurationMs?: number;
  readonly maxFetchAttempts?: number;
  readonly retryBaseMs?: number;
  readonly retryCapMs?: number;
  readonly sleep: (milliseconds: number, signal?: AbortSignal) => Promise<void>;
  readonly random: () => number;
}

export class ExpansionProcessor {
  private readonly pageSize: number;
  private readonly leaseDurationMs: number;
  private readonly maxFetchAttempts: number;
  private readonly retryBaseMs: number;
  private readonly retryCapMs: number;
  private readonly sleep: (milliseconds: number, signal?: AbortSignal) => Promise<void>;
  private readonly random: () => number;

  public constructor(
    private readonly repository: ExpansionRepository,
    private readonly users: UserDirectory,
    options: ExpansionProcessorOptions,
  ) {
    this.pageSize = options.pageSize ?? 1_000;
    this.leaseDurationMs = options.leaseDurationMs ?? 30_000;
    this.maxFetchAttempts = options.maxFetchAttempts ?? 5;
    this.retryBaseMs = options.retryBaseMs ?? 100;
    this.retryCapMs = options.retryCapMs ?? 5_000;
    this.sleep = options.sleep;
    this.random = options.random;

    if (!Number.isInteger(this.pageSize) || this.pageSize < 1 || this.pageSize > 1_000) {
      throw new RangeError('pageSize must be an integer from 1 to 1000');
    }
    if (!Number.isInteger(this.maxFetchAttempts) || this.maxFetchAttempts < 1) {
      throw new RangeError('maxFetchAttempts must be a positive integer');
    }
  }

  public async runOnce(signal?: AbortSignal): Promise<boolean> {
    const lease = await this.repository.claimNext(this.leaseDurationMs);
    if (lease === null) {
      return false;
    }

    let cursor = lease.nextCursor;

    try {
      while (true) {
        if (signal?.aborted === true) {
          throw new ExpansionAbortedError();
        }

        const page = await this.fetchWithRetry(cursor, signal);
        const outcome = await this.repository.savePage(lease, page, this.leaseDurationMs);

        if (outcome !== 'CONTINUE') {
          return true;
        }

        cursor = page.nextCursor;
      }
    } catch (error) {
      if (error instanceof ExpansionAbortedError) {
        throw error;
      }

      if (
        error instanceof UserDirectoryProtocolError ||
        error instanceof UserDirectoryTransientError
      ) {
        await this.repository.fail(lease);
        return true;
      }

      throw error;
    }
  }

  private async fetchWithRetry(
    cursor: string | null,
    signal?: AbortSignal,
  ): Promise<UserDirectoryPage> {
    for (let attempt = 1; attempt <= this.maxFetchAttempts; attempt += 1) {
      try {
        return await this.users.getPage(cursor, this.pageSize, signal);
      } catch (error) {
        if (error instanceof ExpansionAbortedError || signal?.aborted === true) {
          throw new ExpansionAbortedError();
        }
        if (!(error instanceof UserDirectoryTransientError)) {
          throw error;
        }
        if (attempt === this.maxFetchAttempts) {
          throw error;
        }

        const exponentialCap = Math.min(this.retryCapMs, this.retryBaseMs * 2 ** (attempt - 1));
        const jitter = Math.floor(this.random() * exponentialCap);
        await this.sleep(Math.max(jitter, error.retryAfterMs ?? 0), signal);
      }
    }

    throw new Error('Unreachable expansion retry state');
  }
}
