import { randomUUID } from 'node:crypto';

import { describe, expect, it } from 'vitest';

import { InvalidCursorError } from '../../packages/alarms/src/application/alarm-errors';
import type {
  AlarmPageSlice,
  AlarmRepository,
  FindAlarmPageQuery,
  PersistAlarmInput,
} from '../../packages/alarms/src/application/alarm-repository';
import { AlarmApplicationService } from '../../packages/alarms/src/application/alarm-service';
import type { AlarmSnapshot } from '../../packages/alarms/src/domain/alarm';

const cursorKey = 'test-cursor-signing-key-at-least-16';

function alarm(overrides: Partial<AlarmSnapshot> = {}): AlarmSnapshot {
  return {
    id: randomUUID(),
    type: 'BULK',
    title: 'title',
    body: 'body',
    status: 'CREATED',
    failureCode: null,
    materializedCount: 0,
    total: null,
    sent: 0,
    failed: 0,
    canceled: 0,
    active: 0,
    unresolved: 0,
    duplicateCount: 0,
    incidentCount: 0,
    incidentSummary: { count: 0, codes: [], firstDetectedAt: null, lastDetectedAt: null },
    audienceFinalizedAt: null,
    expansionCompletedAt: null,
    finishedAt: null,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    ...overrides,
  };
}

class FakeAlarmRepository implements AlarmRepository {
  public readonly persisted: PersistAlarmInput[] = [];
  public page: AlarmPageSlice = { alarms: [], hasMore: false, nextCursorPosition: null };
  public lastPageQuery: FindAlarmPageQuery | null = null;

  public async create(input: PersistAlarmInput): Promise<AlarmSnapshot> {
    this.persisted.push(input);
    return alarm({ ...input, status: 'CREATED' });
  }

  public async findById(): Promise<AlarmSnapshot | null> {
    return null;
  }

  public async findPage(query: FindAlarmPageQuery): Promise<AlarmPageSlice> {
    this.lastPageQuery = query;
    return this.page;
  }

  public async start(): Promise<AlarmSnapshot> {
    return alarm({ status: 'SENDING' });
  }

  public async cancel(): Promise<AlarmSnapshot> {
    return alarm({ status: 'CANCELED', total: 0, audienceFinalizedAt: new Date().toISOString() });
  }
}

describe('AlarmApplicationService', () => {
  it('validates before persisting and assigns a UUID', async () => {
    const repository = new FakeAlarmRepository();
    const service = new AlarmApplicationService(repository, cursorKey, randomUUID);

    const created = await service.create({ type: 'BULK', title: ' hello ', body: 'world' });

    expect(created.title).toBe('hello');
    expect(repository.persisted).toHaveLength(1);
    expect(repository.persisted[0]?.id).toMatch(/^[0-9a-f-]{36}$/);
  });

  it('returns a signed cursor and rejects tampering before repository access', async () => {
    const repository = new FakeAlarmRepository();
    const service = new AlarmApplicationService(repository, cursorKey, randomUUID);
    repository.page = {
      alarms: [alarm({ createdAt: '2026-10-15T00:00:00.000Z' })],
      hasMore: true,
      nextCursorPosition: {
        createdAt: '2026-10-15T00:00:00.000123Z',
        id: '11111111-1111-4111-8111-111111111111',
      },
    };

    const firstPage = await service.list({ limit: 1 });
    expect(firstPage.nextCursor).not.toBeNull();

    await service.list({ limit: 1, cursor: firstPage.nextCursor ?? undefined });
    expect(repository.lastPageQuery?.cursor).toEqual({
      createdAt: '2026-10-15T00:00:00.000123Z',
      id: '11111111-1111-4111-8111-111111111111',
    });

    const previousQuery = repository.lastPageQuery;
    await expect(service.list({ cursor: `${firstPage.nextCursor}x` })).rejects.toBeInstanceOf(
      InvalidCursorError,
    );
    expect(repository.lastPageQuery).toBe(previousQuery);
  });

  it.each([0, 101, 1.5])('rejects invalid page limit %s', async (limit) => {
    const service = new AlarmApplicationService(new FakeAlarmRepository(), cursorKey, randomUUID);

    await expect(service.list({ limit })).rejects.toThrow('limit must be an integer');
  });
});
