import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';

import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { AlarmApplicationService } from '../../packages/alarms/src/application/alarm-service';
import {
  type ExpansionDuplicateRecipientEvent,
  ExpansionProcessor,
  type UserDirectory,
  type UserDirectoryPage,
  UserDirectoryTransientError,
} from '../../packages/alarms/src/application/expansion';
import { runMigrations } from '../../packages/alarms/src/infrastructure/persistence/migrations';
import { PostgresAlarmRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-alarm-repository';
import { PostgresExpansionRepository } from '../../packages/alarms/src/infrastructure/persistence/postgres-expansion-repository';
import { createTestDatabase, type TestDatabase } from './postgres-test-schema';

const databaseUrl = process.env.TEST_DATABASE_URL;
const describeWithDatabase = databaseUrl === undefined ? describe.skip : describe;

type DirectoryResult = UserDirectoryPage | UserDirectoryTransientError;

class ScriptedUserDirectory implements UserDirectory {
  public readonly cursors: Array<string | null> = [];
  public readonly limits: number[] = [];

  public constructor(private readonly results: DirectoryResult[]) {}

  public async getPage(cursor: string | null, limit: number): Promise<UserDirectoryPage> {
    this.cursors.push(cursor);
    this.limits.push(limit);
    const result = this.results.shift();
    if (result instanceof UserDirectoryTransientError) throw result;
    if (result === undefined) throw new Error('Scripted users result exhausted');
    return result;
  }
}

describeWithDatabase('exact user expansion behavior', () => {
  let testDatabase: TestDatabase;
  let pool: Pool;
  let alarms: AlarmApplicationService;

  beforeAll(async () => {
    if (databaseUrl === undefined) throw new Error('TEST_DATABASE_URL is required');
    testDatabase = await createTestDatabase(databaseUrl);
    await runMigrations({
      databaseUrl: testDatabase.databaseUrl,
      migrationsDirectory: resolve(process.cwd(), 'migrations'),
    });
    pool = new Pool({ connectionString: testDatabase.databaseUrl, max: 6 });
    alarms = new AlarmApplicationService(
      new PostgresAlarmRepository(pool),
      'user-expansion-exact-cursor-key',
      randomUUID,
    );
  });

  afterAll(async () => {
    await pool?.end();
    await testDatabase?.cleanup();
  });

  async function startBulk(): Promise<string> {
    const created = await alarms.create({ type: 'BULK', title: 'bulk', body: 'body' });
    await alarms.start(created.id);
    return created.id;
  }

  it('USR-04 resumes from the committed cursor after transient failures without duplicates', async () => {
    const alarmId = await startBulk();
    const directory = new ScriptedUserDirectory([
      { recipientIds: ['user-a', 'user-b'], nextCursor: 'cursor-2' },
      new UserDirectoryTransientError('temporary-500'),
      new UserDirectoryTransientError('temporary-timeout', 75),
      { recipientIds: ['user-b', 'user-c'], nextCursor: null },
    ]);
    const delays: number[] = [];
    const processor = new ExpansionProcessor(new PostgresExpansionRepository(pool), directory, {
      random: () => 0.5,
      sleep: async (milliseconds) => {
        delays.push(milliseconds);
      },
    });

    await expect(processor.runOnce()).resolves.toBe(true);

    expect(directory.cursors).toEqual([null, 'cursor-2', 'cursor-2', 'cursor-2']);
    expect(directory.limits).toEqual([1_000, 1_000, 1_000, 1_000]);
    expect(delays).toEqual([50, 100]);
    await expect(
      pool.query<{ recipient_id: string }>(
        'SELECT recipient_id FROM deliveries WHERE alarm_id = $1 ORDER BY recipient_id',
        [alarmId],
      ),
    ).resolves.toMatchObject({
      rows: [{ recipient_id: 'user-a' }, { recipient_id: 'user-b' }, { recipient_id: 'user-c' }],
    });
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'SENDING',
      materializedCount: 3,
      total: 3,
      active: 3,
    });
  });

  it('USR-04 exhausts exactly five fetch attempts and atomically leaves EXPANDING', async () => {
    const alarmId = await startBulk();
    const directory = new ScriptedUserDirectory(
      Array.from(
        { length: 5 },
        (_, index) => new UserDirectoryTransientError(`transient-${index + 1}`),
      ),
    );
    const delays: number[] = [];
    const processor = new ExpansionProcessor(new PostgresExpansionRepository(pool), directory, {
      random: () => 0,
      sleep: async (milliseconds) => {
        delays.push(milliseconds);
      },
    });

    await expect(processor.runOnce()).resolves.toBe(true);

    expect(directory.cursors).toEqual([null, null, null, null, null]);
    expect(delays).toEqual([0, 0, 0, 0]);
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      status: 'FAILED',
      failureCode: 'EXPANSION_FAILED',
      materializedCount: 0,
      total: 0,
      active: 0,
      audienceFinalizedAt: expect.any(String),
      expansionCompletedAt: null,
      finishedAt: expect.any(String),
    });
    await expect(
      pool.query<{
        cleanup_state: string;
        delivery_count: number;
        lease_released: boolean;
        state: string;
      }>(
        `
          SELECT
            j.state,
            j.cleanup_state,
            j.lease_token IS NULL AND j.lease_until IS NULL AS lease_released,
            (SELECT count(*)::integer FROM deliveries d WHERE d.alarm_id = j.alarm_id)
              AS delivery_count
          FROM alarm_expansion_jobs j
          WHERE j.alarm_id = $1
        `,
        [alarmId],
      ),
    ).resolves.toMatchObject({
      rows: [
        {
          state: 'FAILED',
          cleanup_state: 'PENDING',
          lease_released: true,
          delivery_count: 0,
        },
      ],
    });
  });

  it('USR-06/USR-07 deduplicates same and adjacent pages and emits aggregate anomalies', async () => {
    const alarmId = await startBulk();
    const anomalies: ExpansionDuplicateRecipientEvent[] = [];
    const repository = new PostgresExpansionRepository(pool, {
      recordDuplicateRecipients: (event) => anomalies.push(event),
    });
    const directory = new ScriptedUserDirectory([
      { recipientIds: ['stable-a', 'stable-a', 'stable-b'], nextCursor: 'mutable-page-2' },
      { recipientIds: ['stable-b', 'new-c'], nextCursor: null },
    ]);
    const processor = new ExpansionProcessor(repository, directory, {
      random: () => 0,
      sleep: async () => undefined,
    });

    await expect(processor.runOnce()).resolves.toBe(true);

    expect(anomalies).toEqual([
      expect.objectContaining({ receivedCount: 3, insertedCount: 2, duplicateCount: 1 }),
      expect.objectContaining({ receivedCount: 2, insertedCount: 1, duplicateCount: 1 }),
    ]);
    expect(new Set(anomalies.map((event) => event.alarmId))).toEqual(new Set([alarmId]));
    expect(new Set(anomalies.map((event) => event.runGeneration)).size).toBe(1);
    expect(new Set(anomalies.map((event) => event.leaseGeneration))).toEqual(new Set([1]));
    await expect(
      pool.query<{ recipient_id: string }>(
        'SELECT recipient_id FROM deliveries WHERE alarm_id = $1 ORDER BY recipient_id',
        [alarmId],
      ),
    ).resolves.toMatchObject({
      rows: [{ recipient_id: 'new-c' }, { recipient_id: 'stable-a' }, { recipient_id: 'stable-b' }],
    });
    await expect(alarms.findById(alarmId)).resolves.toMatchObject({
      materializedCount: 3,
      total: 3,
      active: 3,
    });
  });
});
