import { describe, expect, it } from 'vitest';

import {
  ExpansionAbortedError,
  type ExpansionLease,
  type ExpansionPageOutcome,
  ExpansionProcessor,
  type ExpansionRepository,
  type UserDirectory,
  type UserDirectoryPage,
  UserDirectoryProtocolError,
  UserDirectoryTransientError,
} from '../../packages/alarms/src/application/expansion';

const lease: ExpansionLease = {
  alarmId: '11111111-1111-4111-8111-111111111111',
  runGeneration: '22222222-2222-4222-8222-222222222222',
  leaseGeneration: 1,
  leaseToken: '33333333-3333-4333-8333-333333333333',
  nextCursor: null,
};

const testRuntime = {
  sleep: async (): Promise<void> => undefined,
  random: (): number => 0,
};

class FakeExpansionRepository implements ExpansionRepository {
  public claimed: ExpansionLease | null = lease;
  public readonly claimLeaseDurations: number[] = [];
  public readonly pages: UserDirectoryPage[] = [];
  public readonly savedLeases: ExpansionLease[] = [];
  public readonly saveLeaseDurations: number[] = [];
  public readonly failedLeases: ExpansionLease[] = [];
  public outcomes: ExpansionPageOutcome[] = ['COMPLETED'];
  public failOutcome: 'FAILED' | 'CANCELED' | 'STALE' = 'FAILED';

  public async claimNext(leaseDurationMs: number): Promise<ExpansionLease | null> {
    this.claimLeaseDurations.push(leaseDurationMs);
    return this.claimed;
  }

  public async savePage(
    claimedLease: ExpansionLease,
    page: UserDirectoryPage,
    leaseDurationMs: number,
  ): Promise<ExpansionPageOutcome> {
    this.savedLeases.push(claimedLease);
    this.pages.push(page);
    this.saveLeaseDurations.push(leaseDurationMs);
    return this.outcomes.shift() ?? 'COMPLETED';
  }

  public async fail(claimedLease: ExpansionLease): Promise<'FAILED' | 'CANCELED' | 'STALE'> {
    this.failedLeases.push(claimedLease);
    return this.failOutcome;
  }
}

class PageSequence implements UserDirectory {
  public readonly requestedCursors: Array<string | null> = [];
  public readonly requestedLimits: number[] = [];

  public constructor(private readonly results: Array<UserDirectoryPage | Error>) {}

  public async getPage(cursor: string | null, limit: number): Promise<UserDirectoryPage> {
    this.requestedCursors.push(cursor);
    this.requestedLimits.push(limit);
    const result = this.results.shift();
    if (result instanceof Error) {
      throw result;
    }
    if (result === undefined) {
      throw new Error('Missing fake page');
    }
    return result;
  }
}

describe('ExpansionProcessor', () => {
  it('returns idle without touching the user directory when no lease can be claimed', async () => {
    const repository = new FakeExpansionRepository();
    repository.claimed = null;
    const users = new PageSequence([{ recipientIds: ['must-not-be-read'], nextCursor: null }]);
    const processor = new ExpansionProcessor(repository, users, testRuntime);

    await expect(processor.runOnce()).resolves.toBe(false);
    expect(repository.claimLeaseDurations).toEqual([30_000]);
    expect(users.requestedCursors).toEqual([]);
    expect(repository.pages).toEqual([]);
    expect(repository.failedLeases).toEqual([]);
  });

  it('continues across an empty non-terminal page and stops only on a null cursor', async () => {
    const repository = new FakeExpansionRepository();
    repository.outcomes = ['CONTINUE', 'COMPLETED'];
    const users = new PageSequence([
      { recipientIds: [], nextCursor: 'next-page' },
      { recipientIds: ['user-1'], nextCursor: null },
    ]);
    const processor = new ExpansionProcessor(repository, users, testRuntime);

    await expect(processor.runOnce()).resolves.toBe(true);
    expect(users.requestedCursors).toEqual([null, 'next-page']);
    expect(users.requestedLimits).toEqual([1_000, 1_000]);
    expect(repository.pages).toHaveLength(2);
    expect(repository.savedLeases).toEqual([lease, lease]);
    expect(repository.saveLeaseDurations).toEqual([30_000, 30_000]);
  });

  it.each([
    {
      name: 'non-empty success',
      page: { recipientIds: ['user-1'], nextCursor: null },
      outcome: 'COMPLETED',
    },
    {
      name: 'zero-user success',
      page: { recipientIds: [], nextCursor: null },
      outcome: 'COMPLETED',
    },
    {
      name: 'cancellation finalizer',
      page: { recipientIds: ['must-not-publish'], nextCursor: null },
      outcome: 'CANCELED',
    },
    {
      name: 'stale owner after lease takeover',
      page: { recipientIds: ['stale-owner'], nextCursor: null },
      outcome: 'STALE',
    },
  ] satisfies ReadonlyArray<{
    name: string;
    page: UserDirectoryPage;
    outcome: ExpansionPageOutcome;
  }>)('treats repository $name as a terminal expansion event', async ({ page, outcome }) => {
    const repository = new FakeExpansionRepository();
    repository.outcomes = [outcome];
    const users = new PageSequence([page, { recipientIds: ['must-not-read'], nextCursor: null }]);
    const processor = new ExpansionProcessor(repository, users, testRuntime);

    await expect(processor.runOnce()).resolves.toBe(true);
    expect(users.requestedCursors).toEqual([lease.nextCursor]);
    expect(repository.pages).toEqual([page]);
    expect(repository.savedLeases).toEqual([lease]);
    expect(repository.failedLeases).toEqual([]);
  });

  it('retries transient fetch failures with bounded jitter without failing the job', async () => {
    const repository = new FakeExpansionRepository();
    const users = new PageSequence([
      new UserDirectoryTransientError('temporary'),
      new UserDirectoryTransientError('backpressure', 75),
      { recipientIds: ['user-1'], nextCursor: null },
    ]);
    const delays: number[] = [];
    const processor = new ExpansionProcessor(repository, users, {
      ...testRuntime,
      random: () => 0.5,
      sleep: async (milliseconds) => {
        delays.push(milliseconds);
      },
    });

    await processor.runOnce();

    expect(delays).toEqual([50, 100]);
    expect(repository.failedLeases).toEqual([]);
    expect(repository.pages).toHaveLength(1);
  });

  it.each([
    {
      name: 'protocol failure wins',
      errorSequence: [new UserDirectoryProtocolError('invalid cursor')],
      failOutcome: 'FAILED',
      maxFetchAttempts: 5,
    },
    {
      name: 'cancellation wins during failure finalization',
      errorSequence: [new UserDirectoryProtocolError('invalid cursor')],
      failOutcome: 'CANCELED',
      maxFetchAttempts: 5,
    },
    {
      name: 'lease takeover wins during failure finalization',
      errorSequence: [new UserDirectoryProtocolError('invalid cursor')],
      failOutcome: 'STALE',
      maxFetchAttempts: 5,
    },
    {
      name: 'transient retry budget is exhausted',
      errorSequence: [new UserDirectoryTransientError('1'), new UserDirectoryTransientError('2')],
      failOutcome: 'FAILED',
      maxFetchAttempts: 2,
    },
  ] satisfies ReadonlyArray<{
    name: string;
    errorSequence: Error[];
    failOutcome: 'FAILED' | 'CANCELED' | 'STALE';
    maxFetchAttempts: number;
  }>)(
    'delegates $name to the fenced repository finalizer exactly once',
    async ({ errorSequence, failOutcome, maxFetchAttempts }) => {
      const repository = new FakeExpansionRepository();
      repository.failOutcome = failOutcome;
      const processor = new ExpansionProcessor(repository, new PageSequence(errorSequence), {
        ...testRuntime,
        maxFetchAttempts,
        sleep: async () => undefined,
      });

      await expect(processor.runOnce()).resolves.toBe(true);
      expect(repository.failedLeases).toEqual([lease]);
      expect(repository.pages).toEqual([]);
    },
  );

  it('leaves the durable lease for takeover when shutdown aborts the fetch', async () => {
    const repository = new FakeExpansionRepository();
    const users: UserDirectory = {
      getPage: async () => {
        throw new ExpansionAbortedError();
      },
    };
    const processor = new ExpansionProcessor(repository, users, testRuntime);

    await expect(processor.runOnce()).rejects.toBeInstanceOf(ExpansionAbortedError);
    expect(repository.failedLeases).toEqual([]);
  });
});
