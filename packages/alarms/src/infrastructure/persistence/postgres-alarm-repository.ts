import { randomUUID } from 'node:crypto';

import type { Pool, PoolClient } from 'pg';

import {
  AlarmNotFoundError,
  AlarmStateConflictError,
  PersistenceUnavailableError,
} from '../../application/alarm-errors';
import type {
  AlarmPageSlice,
  AlarmRepository,
  FindAlarmPageQuery,
  PersistAlarmInput,
} from '../../application/alarm-repository';
import {
  ALARM_STATUSES,
  ALARM_TYPES,
  type AlarmSnapshot,
  type AlarmStatus,
  type AlarmType,
  INCIDENT_CODES,
  type IncidentCode,
} from '../../domain/alarm';
import { decideCancel, decideStart } from '../../domain/alarm-policy';
import { FIRST_EXPANSION_PAGE_HASH } from './expansion-cursor-hash';
import { attachPostgresClientErrorHandler } from './postgres-pool-observability';

interface AlarmRow {
  readonly id: string;
  readonly type: AlarmType;
  readonly title: string;
  readonly body: string;
  readonly status: AlarmStatus;
  readonly failure_code: string | null;
  readonly materialized_count: number;
  readonly total: number | null;
  readonly sent: number;
  readonly failed: number;
  readonly canceled: number;
  readonly active: number;
  readonly unresolved: number;
  readonly duplicate_count: number;
  readonly incident_count: number;
  readonly incident_counts: Record<string, unknown>;
  readonly incident_first_detected_at: Date | null;
  readonly incident_last_detected_at: Date | null;
  readonly audience_finalized_at: Date | null;
  readonly expansion_completed_at: Date | null;
  readonly finished_at: Date | null;
  readonly created_at: Date;
  readonly updated_at: Date;
}

interface AlarmPageRow extends AlarmRow {
  readonly cursor_created_at: string;
}

function assertAlarmType(value: string): asserts value is AlarmType {
  if (!ALARM_TYPES.includes(value as AlarmType)) {
    throw new Error(`Unexpected persisted Alarm type: ${value}`);
  }
}

function assertAlarmStatus(value: string): asserts value is AlarmStatus {
  if (!ALARM_STATUSES.includes(value as AlarmStatus)) {
    throw new Error(`Unexpected persisted Alarm status: ${value}`);
  }
}

function toIso(value: Date | null): string | null {
  return value?.toISOString() ?? null;
}

function mapAlarm(row: AlarmRow): AlarmSnapshot {
  assertAlarmType(row.type);
  assertAlarmStatus(row.status);

  const codes = Object.entries(row.incident_counts)
    .map(([code, count]) => {
      if (
        !INCIDENT_CODES.includes(code as IncidentCode) ||
        typeof count !== 'number' ||
        !Number.isInteger(count) ||
        count <= 0
      ) {
        throw new Error(`Unexpected persisted incident count: ${code}`);
      }

      return { code: code as IncidentCode, count: count as number };
    })
    .sort((left, right) => left.code.localeCompare(right.code));
  const codeCount = codes.reduce((sum, entry) => sum + entry.count, 0);
  if (codeCount !== row.incident_count) {
    throw new Error('Persisted incident code counts do not match incident_count');
  }

  return {
    id: row.id,
    type: row.type,
    title: row.title,
    body: row.body,
    status: row.status,
    failureCode: row.failure_code,
    materializedCount: row.materialized_count,
    total: row.total,
    sent: row.sent,
    failed: row.failed,
    canceled: row.canceled,
    active: row.active,
    unresolved: row.unresolved,
    duplicateCount: row.duplicate_count,
    incidentCount: row.incident_count,
    incidentSummary: {
      count: row.incident_count,
      codes,
      firstDetectedAt: toIso(row.incident_first_detected_at),
      lastDetectedAt: toIso(row.incident_last_detected_at),
    },
    audienceFinalizedAt: toIso(row.audience_finalized_at),
    expansionCompletedAt: toIso(row.expansion_completed_at),
    finishedAt: toIso(row.finished_at),
    createdAt: row.created_at.toISOString(),
    updatedAt: row.updated_at.toISOString(),
  };
}

export class PostgresAlarmRepository implements AlarmRepository {
  public constructor(private readonly pool: Pool) {}

  public async create(input: PersistAlarmInput): Promise<AlarmSnapshot> {
    return this.transaction(async (client) => {
      const inserted = await client.query<AlarmRow>(
        `
          INSERT INTO alarms(id, type, title, body, status)
          VALUES ($1, $2, $3, $4, 'CREATED')
          RETURNING *
        `,
        [input.id, input.type, input.title, input.body],
      );

      if (input.type === 'EMERGENCY') {
        const values: unknown[] = [];
        const placeholders = input.recipientIds.map((recipientId, index) => {
          values.push(input.id, recipientId, index + 1);
          const offset = index * 3;
          return `($${offset + 1}, $${offset + 2}, $${offset + 3})`;
        });

        await client.query(
          `
            INSERT INTO emergency_recipients(alarm_id, recipient_id, ordinal)
            VALUES ${placeholders.join(', ')}
          `,
          values,
        );
      }

      const row = inserted.rows[0];
      if (row === undefined) {
        throw new Error('Alarm insert returned no row');
      }

      return mapAlarm(row);
    });
  }

  public async findById(alarmId: string): Promise<AlarmSnapshot | null> {
    try {
      const result = await this.pool.query<AlarmRow>('SELECT * FROM alarms WHERE id = $1', [
        alarmId,
      ]);
      const row = result.rows[0];

      return row === undefined ? null : mapAlarm(row);
    } catch (error) {
      throw new PersistenceUnavailableError(error);
    }
  }

  public async findPage(query: FindAlarmPageQuery): Promise<AlarmPageSlice> {
    try {
      const parameters: unknown[] = [];
      const predicates: string[] = [];

      if (query.status !== null) {
        parameters.push(query.status);
        predicates.push(`status = $${parameters.length}`);
      }
      if (query.type !== null) {
        parameters.push(query.type);
        predicates.push(`type = $${parameters.length}`);
      }
      if (query.cursor !== null) {
        parameters.push(query.cursor.createdAt, query.cursor.id);
        predicates.push(
          `(created_at, id) < ($${parameters.length - 1}::timestamptz, $${parameters.length}::uuid)`,
        );
      }

      parameters.push(query.limit + 1);
      const where = predicates.length === 0 ? '' : `WHERE ${predicates.join(' AND ')}`;
      const result = await this.pool.query<AlarmPageRow>(
        `
          SELECT
            *,
            to_char(
              created_at AT TIME ZONE 'UTC',
              'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
            ) AS cursor_created_at
          FROM alarms
          ${where}
          ORDER BY created_at DESC, id DESC
          LIMIT $${parameters.length}
        `,
        parameters,
      );
      const visibleRows = result.rows.slice(0, query.limit);
      const hasMore = result.rows.length > query.limit;
      const lastVisibleRow = visibleRows.at(-1);

      return {
        alarms: visibleRows.map(mapAlarm),
        hasMore,
        nextCursorPosition:
          hasMore && lastVisibleRow !== undefined
            ? { createdAt: lastVisibleRow.cursor_created_at, id: lastVisibleRow.id }
            : null,
      };
    } catch (error) {
      throw new PersistenceUnavailableError(error);
    }
  }

  public async start(alarmId: string): Promise<AlarmSnapshot> {
    return this.transaction(async (client) => {
      const current = await this.lockAlarm(client, alarmId);
      const decision = decideStart(current.type, current.status);

      if (decision === 'NOOP') {
        return mapAlarm(current);
      }
      if (decision === 'CONFLICT') {
        throw new AlarmStateConflictError(alarmId, 'START', current.status);
      }

      if (decision === 'INITIALIZE_BULK') {
        const runGeneration = randomUUID();
        await client.query(
          `
            INSERT INTO alarm_expansion_jobs(alarm_id, state, run_generation)
            VALUES ($1, 'READY', $2)
          `,
          [alarmId, runGeneration],
        );
        await client.query(
          `
            INSERT INTO alarm_expansion_seen_cursors(
              alarm_id, run_generation, cursor_hash
            )
            VALUES ($1, $2, $3)
          `,
          [alarmId, runGeneration, FIRST_EXPANSION_PAGE_HASH],
        );
        const updated = await client.query<AlarmRow>(
          `
            UPDATE alarms
            SET status = 'QUEUED', updated_at = clock_timestamp(), version = version + 1
            WHERE id = $1
            RETURNING *
          `,
          [alarmId],
        );
        await client.query("SELECT pg_notify('alarm_work', $1)", [alarmId]);

        return this.requireRow(updated.rows[0], 'BULK start update');
      }

      const updated = await client.query<AlarmRow>(
        `
          WITH inserted AS (
            INSERT INTO deliveries(
              id, alarm_id, recipient_id, client_ref, state, priority, alarm_created_at
            )
            SELECT
              gen_random_uuid(), a.id, er.recipient_id, gen_random_uuid(), 'READY', 100, a.created_at
            FROM alarms a
            JOIN emergency_recipients er ON er.alarm_id = a.id
            WHERE a.id = $1
            ORDER BY er.ordinal
            RETURNING 1
          ), total AS (
            SELECT count(*)::integer AS value FROM inserted
          )
          UPDATE alarms a
          SET
            status = CASE WHEN total.value = 0 THEN 'COMPLETED' ELSE 'SENDING' END,
            audience_finalized_at = clock_timestamp(),
            total = total.value,
            active = total.value,
            finished_at = CASE WHEN total.value = 0 THEN clock_timestamp() ELSE NULL END,
            updated_at = clock_timestamp(),
            version = version + 1
          FROM total
          WHERE a.id = $1
          RETURNING a.*
        `,
        [alarmId],
      );
      await client.query("SELECT pg_notify('alarm_work', $1)", [alarmId]);

      return this.requireRow(updated.rows[0], 'EMERGENCY start update');
    });
  }

  public async cancel(alarmId: string): Promise<AlarmSnapshot> {
    return this.transaction(async (client) => {
      const current = await this.lockAlarm(client, alarmId);
      const decision = decideCancel(current.status);

      if (decision === 'NOOP') {
        return mapAlarm(current);
      }
      if (decision === 'CONFLICT') {
        throw new AlarmStateConflictError(alarmId, 'CANCEL', current.status);
      }

      if (decision === 'CANCEL_CREATED') {
        const updated = await client.query<AlarmRow>(
          `
            UPDATE alarms
            SET
              status = 'CANCELED',
              cancel_requested_at = clock_timestamp(),
              audience_finalized_at = clock_timestamp(),
              total = 0,
              finished_at = clock_timestamp(),
              updated_at = clock_timestamp(),
              version = version + 1
            WHERE id = $1
            RETURNING *
          `,
          [alarmId],
        );

        return this.requireRow(updated.rows[0], 'CREATED cancel update');
      }

      const updated = await client.query<AlarmRow>(
        `
          UPDATE alarms
          SET
            status = 'CANCEL_REQUESTED',
            cancel_requested_at = COALESCE(cancel_requested_at, clock_timestamp()),
            updated_at = clock_timestamp(),
            version = version + 1
          WHERE id = $1
          RETURNING *
        `,
        [alarmId],
      );
      await client.query("SELECT pg_notify('alarm_work', $1)", [alarmId]);

      return this.requireRow(updated.rows[0], 'cancel request update');
    });
  }

  private async lockAlarm(client: PoolClient, alarmId: string): Promise<AlarmRow> {
    const result = await client.query<AlarmRow>('SELECT * FROM alarms WHERE id = $1 FOR UPDATE', [
      alarmId,
    ]);
    const row = result.rows[0];
    if (row === undefined) {
      throw new AlarmNotFoundError(alarmId);
    }
    assertAlarmType(row.type);
    assertAlarmStatus(row.status);

    return row;
  }

  private requireRow(row: AlarmRow | undefined, operation: string): AlarmSnapshot {
    if (row === undefined) {
      throw new Error(`${operation} returned no row`);
    }

    return mapAlarm(row);
  }

  private async transaction<T>(work: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.pool.connect().catch((error: unknown) => {
      throw new PersistenceUnavailableError(error);
    });
    const detachErrorHandler = attachPostgresClientErrorHandler(client, {
      component: 'alarm-repository',
    });

    try {
      await client.query('BEGIN');
      const result = await work(client);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      if (
        error instanceof AlarmNotFoundError ||
        error instanceof AlarmStateConflictError ||
        error instanceof PersistenceUnavailableError
      ) {
        throw error;
      }

      throw new PersistenceUnavailableError(error);
    } finally {
      client.release();
      detachErrorHandler();
    }
  }
}
