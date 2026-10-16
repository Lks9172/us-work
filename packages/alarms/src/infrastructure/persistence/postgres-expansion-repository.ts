import { randomUUID } from 'node:crypto';

import type { Pool, PoolClient } from 'pg';

import { PersistenceUnavailableError } from '../../application/alarm-errors';
import type {
  ExpansionDuplicateRecipientEvent,
  ExpansionEventSink,
  ExpansionLease,
  ExpansionPageOutcome,
  ExpansionRepository,
  UserDirectoryPage,
} from '../../application/expansion';
import { expansionCursorHash } from './expansion-cursor-hash';
import { attachPostgresClientErrorHandler } from './postgres-pool-observability';

interface ClaimCandidateRow {
  readonly alarm_id: string;
  readonly alarm_status: 'QUEUED' | 'EXPANDING' | 'CANCEL_REQUESTED';
}

interface ExpansionJobRow {
  readonly alarm_id: string;
  readonly state: 'READY' | 'RUNNING' | 'COMPLETED' | 'FAILED' | 'CANCELED';
  readonly run_generation: string;
  readonly next_cursor: string | null;
  readonly lease_generation: number;
  readonly lease_token: string | null;
}

interface LockedAlarmRow {
  readonly id: string;
  readonly status: string;
}

const NOOP_EVENT_SINK: ExpansionEventSink = {
  recordDuplicateRecipients: () => undefined,
};

export class PostgresExpansionRepository implements ExpansionRepository {
  public constructor(
    private readonly pool: Pool,
    private readonly eventSink: ExpansionEventSink = NOOP_EVENT_SINK,
  ) {}

  public claimNext(leaseDurationMs: number): Promise<ExpansionLease | null> {
    return this.transaction(async (client) => {
      const candidateResult = await client.query<ClaimCandidateRow>(`
        SELECT a.id AS alarm_id, a.status AS alarm_status
        FROM alarms a
        WHERE a.type = 'BULK'
          AND a.status IN ('QUEUED', 'EXPANDING', 'CANCEL_REQUESTED')
          AND EXISTS (
            SELECT 1
            FROM alarm_expansion_jobs j
            WHERE j.alarm_id = a.id
              AND (
                j.state = 'READY'
                OR (j.state = 'RUNNING' AND j.lease_until <= clock_timestamp())
              )
          )
        ORDER BY
          CASE WHEN a.status = 'CANCEL_REQUESTED' THEN 0 ELSE 1 END,
          a.created_at,
          a.id
        FOR UPDATE OF a SKIP LOCKED
        LIMIT 1
      `);
      const candidate = candidateResult.rows[0];
      if (candidate === undefined) {
        return null;
      }

      const job = await this.lockJob(client, candidate.alarm_id);
      const claimable =
        job.state === 'READY' ||
        (job.state === 'RUNNING' &&
          (await this.hasExpiredLease(client, candidate.alarm_id, job.lease_generation)));
      if (!claimable) {
        return null;
      }

      if (candidate.alarm_status === 'CANCEL_REQUESTED') {
        await this.finalizeCancellation(client, candidate.alarm_id, job.run_generation);
        return null;
      }

      const leaseToken = randomUUID();
      const claimed = await client.query<ExpansionJobRow>(
        `
          UPDATE alarm_expansion_jobs
          SET
            state = 'RUNNING',
            lease_generation = lease_generation + 1,
            lease_token = $2,
            lease_until = clock_timestamp() + ($3::integer * interval '1 millisecond'),
            updated_at = clock_timestamp()
          WHERE alarm_id = $1
          RETURNING *
        `,
        [candidate.alarm_id, leaseToken, leaseDurationMs],
      );
      await client.query(
        `
          UPDATE alarms
          SET status = 'EXPANDING', updated_at = clock_timestamp(), version = version + 1
          WHERE id = $1 AND status IN ('QUEUED', 'EXPANDING')
        `,
        [candidate.alarm_id],
      );

      const row = claimed.rows[0];
      if (row === undefined) {
        throw new Error('Expansion claim returned no row');
      }

      return {
        alarmId: row.alarm_id,
        runGeneration: row.run_generation,
        leaseGeneration: row.lease_generation,
        leaseToken,
        nextCursor: row.next_cursor,
      };
    });
  }

  public async savePage(
    lease: ExpansionLease,
    page: UserDirectoryPage,
    leaseDurationMs: number,
  ): Promise<ExpansionPageOutcome> {
    let duplicateEvent: ExpansionDuplicateRecipientEvent | null = null;
    const outcome = await this.transaction(async (client) => {
      const alarm = await this.lockAlarm(client, lease.alarmId);
      const job = await this.lockJob(client, lease.alarmId);

      if (!this.owns(job, lease)) {
        return 'STALE';
      }
      if (alarm.status === 'CANCEL_REQUESTED') {
        await this.finalizeCancellation(client, lease.alarmId, lease.runGeneration);
        return 'CANCELED';
      }
      if (alarm.status !== 'EXPANDING') {
        return 'STALE';
      }

      if (page.nextCursor !== null) {
        const seen = await client.query(
          `
            INSERT INTO alarm_expansion_seen_cursors(
              alarm_id, run_generation, cursor_hash
            )
            VALUES ($1, $2, $3)
            ON CONFLICT DO NOTHING
            RETURNING cursor_hash
          `,
          [lease.alarmId, lease.runGeneration, expansionCursorHash(page.nextCursor)],
        );
        if (seen.rowCount !== 1) {
          await this.finalizeFailure(client, lease.alarmId, lease.runGeneration);
          return 'COMPLETED';
        }
      }

      let insertedCount = 0;
      if (page.recipientIds.length > 0) {
        const inserted = await client.query(
          `
            INSERT INTO alarm_expansion_recipients(
              alarm_id, run_generation, recipient_id
            )
            SELECT $1, $2, recipient_id
            FROM unnest($3::text[]) AS recipient_id
            ON CONFLICT DO NOTHING
            RETURNING recipient_id
          `,
          [lease.alarmId, lease.runGeneration, page.recipientIds],
        );
        insertedCount = inserted.rowCount ?? 0;
        const duplicateCount = page.recipientIds.length - insertedCount;
        if (duplicateCount > 0) {
          duplicateEvent = {
            alarmId: lease.alarmId,
            runGeneration: lease.runGeneration,
            leaseGeneration: lease.leaseGeneration,
            receivedCount: page.recipientIds.length,
            insertedCount,
            duplicateCount,
          };
        }
      }

      await client.query(
        `
          UPDATE alarms
          SET
            materialized_count = materialized_count + $2,
            updated_at = clock_timestamp(),
            version = version + 1
          WHERE id = $1
        `,
        [lease.alarmId, insertedCount],
      );

      if (page.nextCursor !== null) {
        await client.query(
          `
            UPDATE alarm_expansion_jobs
            SET
              next_cursor = $5,
              materialized_count = materialized_count + $6,
              lease_until = clock_timestamp() + ($7::integer * interval '1 millisecond'),
              updated_at = clock_timestamp()
            WHERE alarm_id = $1
              AND run_generation = $2
              AND lease_generation = $3
              AND lease_token = $4
          `,
          [
            lease.alarmId,
            lease.runGeneration,
            lease.leaseGeneration,
            lease.leaseToken,
            page.nextCursor,
            insertedCount,
            leaseDurationMs,
          ],
        );
        return 'CONTINUE';
      }

      await client.query(
        `
          UPDATE alarm_expansion_jobs
          SET materialized_count = materialized_count + $2, updated_at = clock_timestamp()
          WHERE alarm_id = $1
        `,
        [lease.alarmId, insertedCount],
      );
      await this.finalizeSuccess(client, lease.alarmId, lease.runGeneration);
      return 'COMPLETED';
    });

    if (duplicateEvent !== null) {
      try {
        this.eventSink.recordDuplicateRecipients(duplicateEvent);
      } catch {
        // Observability must never change the committed expansion state.
      }
    }
    return outcome;
  }

  public fail(lease: ExpansionLease): Promise<'FAILED' | 'CANCELED' | 'STALE'> {
    return this.transaction(async (client) => {
      const alarm = await this.lockAlarm(client, lease.alarmId);
      const job = await this.lockJob(client, lease.alarmId);
      if (!this.owns(job, lease)) {
        return 'STALE';
      }
      if (alarm.status === 'CANCEL_REQUESTED') {
        await this.finalizeCancellation(client, lease.alarmId, lease.runGeneration);
        return 'CANCELED';
      }
      if (alarm.status !== 'EXPANDING') {
        return 'STALE';
      }

      await this.finalizeFailure(client, lease.alarmId, lease.runGeneration);
      return 'FAILED';
    });
  }

  private async finalizeSuccess(
    client: PoolClient,
    alarmId: string,
    runGeneration: string,
  ): Promise<void> {
    const finalized = await client.query<{ total: number }>(
      `
        WITH inserted AS (
          INSERT INTO deliveries(
            id, alarm_id, recipient_id, client_ref, state, priority, alarm_created_at
          )
          SELECT
            gen_random_uuid(), a.id, r.recipient_id, gen_random_uuid(),
            'READY', 10, a.created_at
          FROM alarms a
          JOIN alarm_expansion_recipients r
            ON r.alarm_id = a.id AND r.run_generation = $2
          WHERE a.id = $1
          RETURNING 1
        ), audience AS (
          SELECT count(*)::integer AS total
          FROM alarm_expansion_recipients
          WHERE alarm_id = $1 AND run_generation = $2
        )
        UPDATE alarms a
        SET
          status = CASE WHEN audience.total = 0 THEN 'COMPLETED' ELSE 'SENDING' END,
          failure_code = NULL,
          audience_finalized_at = clock_timestamp(),
          expansion_completed_at = clock_timestamp(),
          total = audience.total,
          active = audience.total,
          finished_at = CASE WHEN audience.total = 0 THEN clock_timestamp() ELSE NULL END,
          updated_at = clock_timestamp(),
          version = version + 1
        FROM audience
        WHERE a.id = $1
        RETURNING audience.total
      `,
      [alarmId, runGeneration],
    );
    if (finalized.rowCount !== 1) {
      throw new Error('Expansion success finalization did not update the Alarm');
    }

    await client.query(
      `
        UPDATE alarm_expansion_jobs
        SET
          state = 'COMPLETED',
          next_cursor = NULL,
          lease_token = NULL,
          lease_until = NULL,
          cleanup_state = 'PENDING',
          cleanup_run_generation = $2,
          updated_at = clock_timestamp()
        WHERE alarm_id = $1 AND run_generation = $2
      `,
      [alarmId, runGeneration],
    );
    await client.query("SELECT pg_notify('alarm_work', $1)", [alarmId]);
  }

  private async finalizeFailure(
    client: PoolClient,
    alarmId: string,
    runGeneration: string,
  ): Promise<void> {
    await client.query(
      `
        UPDATE alarms
        SET
          status = 'FAILED',
          failure_code = 'EXPANSION_FAILED',
          audience_finalized_at = clock_timestamp(),
          total = 0,
          finished_at = clock_timestamp(),
          updated_at = clock_timestamp(),
          version = version + 1
        WHERE id = $1
      `,
      [alarmId],
    );
    await client.query(
      `
        UPDATE alarm_expansion_jobs
        SET
          state = 'FAILED',
          lease_token = NULL,
          lease_until = NULL,
          cleanup_state = 'PENDING',
          cleanup_run_generation = $2,
          updated_at = clock_timestamp()
        WHERE alarm_id = $1 AND run_generation = $2
      `,
      [alarmId, runGeneration],
    );
  }

  private async finalizeCancellation(
    client: PoolClient,
    alarmId: string,
    runGeneration: string,
  ): Promise<void> {
    await client.query(
      `
        UPDATE alarms
        SET
          status = 'CANCELED',
          failure_code = NULL,
          audience_finalized_at = clock_timestamp(),
          total = 0,
          finished_at = clock_timestamp(),
          updated_at = clock_timestamp(),
          version = version + 1
        WHERE id = $1 AND status = 'CANCEL_REQUESTED'
      `,
      [alarmId],
    );
    await client.query(
      `
        UPDATE alarm_expansion_jobs
        SET
          state = 'CANCELED',
          lease_token = NULL,
          lease_until = NULL,
          cleanup_state = 'PENDING',
          cleanup_run_generation = $2,
          updated_at = clock_timestamp()
        WHERE alarm_id = $1 AND run_generation = $2
      `,
      [alarmId, runGeneration],
    );
  }

  private async lockAlarm(client: PoolClient, alarmId: string): Promise<LockedAlarmRow> {
    const result = await client.query<LockedAlarmRow>(
      'SELECT id, status FROM alarms WHERE id = $1 FOR UPDATE',
      [alarmId],
    );
    const row = result.rows[0];
    if (row === undefined) {
      throw new Error(`Expansion Alarm disappeared: ${alarmId}`);
    }
    return row;
  }

  private async lockJob(client: PoolClient, alarmId: string): Promise<ExpansionJobRow> {
    const result = await client.query<ExpansionJobRow>(
      'SELECT * FROM alarm_expansion_jobs WHERE alarm_id = $1 FOR UPDATE',
      [alarmId],
    );
    const row = result.rows[0];
    if (row === undefined) {
      throw new Error(`Expansion job disappeared: ${alarmId}`);
    }
    return row;
  }

  private owns(job: ExpansionJobRow, lease: ExpansionLease): boolean {
    return (
      job.state === 'RUNNING' &&
      job.run_generation === lease.runGeneration &&
      job.lease_generation === lease.leaseGeneration &&
      job.lease_token === lease.leaseToken
    );
  }

  private async hasExpiredLease(
    client: PoolClient,
    alarmId: string,
    leaseGeneration: number,
  ): Promise<boolean> {
    const result = await client.query<{ expired: boolean }>(
      `
        SELECT lease_until <= clock_timestamp() AS expired
        FROM alarm_expansion_jobs
        WHERE alarm_id = $1 AND lease_generation = $2
      `,
      [alarmId, leaseGeneration],
    );
    return result.rows[0]?.expired === true;
  }

  private async transaction<T>(work: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.pool.connect().catch((error: unknown) => {
      throw new PersistenceUnavailableError(error);
    });
    const detachErrorHandler = attachPostgresClientErrorHandler(client, {
      component: 'expansion-repository',
    });

    try {
      await client.query('BEGIN');
      const result = await work(client);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      if (error instanceof PersistenceUnavailableError) {
        throw error;
      }
      throw new PersistenceUnavailableError(error);
    } finally {
      client.release();
      detachErrorHandler();
    }
  }
}
