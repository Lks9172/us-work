import { randomUUID } from 'node:crypto';

import type { Pool, PoolClient } from 'pg';

import { PersistenceUnavailableError } from '../../application/alarm-errors';
import type {
  AlarmCounterInconsistency,
  MaintenanceRepository,
  OperationalSnapshot,
  SendLeaseRecoveryEvent,
} from '../../application/maintenance';
import { evaluateEvidence, type ProviderEvidenceEntry } from '../../domain/delivery-evidence';
import { attachPostgresClientErrorHandler } from './postgres-pool-observability';
import { rollbackPostgresTransaction } from './postgres-transaction-cleanup';

interface MaintenanceAlarmRow {
  readonly id: string;
}

interface ExpiredDeliveryCandidate {
  readonly id: string;
  readonly alarm_id: string;
}

interface LockedDelivery {
  readonly id: string;
  readonly state: string;
  readonly current_attempt_id: string | null;
  readonly lease_until: Date | null;
  readonly lease_expired: boolean;
  readonly lease_generation: number;
}

interface LockedAttempt {
  readonly id: string;
  readonly request_phase: 'PREPARED' | 'COMMITTED' | 'FINISHED_NOT_SENT' | 'FINISHED';
}

interface CleanupJob {
  readonly alarm_id: string;
  readonly cleanup_state: string;
  readonly cleanup_run_generation: string | null;
  readonly cleanup_lease_generation: number;
  readonly cleanup_lease_token: string | null;
}

interface CleanupLease {
  readonly alarmId: string;
  readonly runGeneration: string;
  readonly leaseGeneration: number;
  readonly leaseToken: string;
}

interface TerminalEvidenceCandidate {
  readonly delivery_id: string;
  readonly alarm_id: string;
}

interface TerminalDelivery {
  readonly id: string;
  readonly recipient_id: string;
  readonly state: string;
  readonly provider_message_id: string | null;
  readonly duplicate_detected: boolean;
  readonly post_terminal_incident_code: string | null;
}

interface StoredEvidence {
  readonly id: string;
  readonly entries: unknown;
}

interface CounterInconsistencyRow {
  readonly alarm_id: string;
  readonly alarm_updated_at: Date;
  readonly stored_total: number;
  readonly stored_sent: number;
  readonly stored_failed: number;
  readonly stored_canceled: number;
  readonly stored_active: number;
  readonly stored_unresolved: number;
  readonly stored_duplicate_count: number;
  readonly stored_incident_count: number;
  readonly observed_total: number;
  readonly observed_sent: number;
  readonly observed_failed: number;
  readonly observed_canceled: number;
  readonly observed_active: number;
  readonly observed_unresolved: number;
  readonly observed_duplicate_count: number;
  readonly observed_incident_count: number;
}

interface CounterConsistencyLeadership {
  readonly client: PoolClient;
  readonly detachErrorHandler: () => void;
}

interface CounterConsistencyCursor {
  readonly updatedAt: Date;
  readonly alarmId: string;
}

interface OperationalSnapshotRow {
  readonly observed_at: Date;
  readonly queue_depth: number | string;
  readonly due_queue_depth: number | string;
  readonly oldest_due_age_ms: number | string | null;
  readonly sent: number | string;
  readonly failed: number | string;
  readonly retry_wait: number | string;
  readonly unknown: number | string;
  readonly unresolved: number | string;
  readonly active_send_leases: number | string;
  readonly provider_attempts_window: number | string;
  readonly provider_accepted_window: number | string;
  readonly provider_rate_limited_window: number | string;
}

// Signed int32 ASCII namespaces: "USAL" / "CNTR".
const COUNTER_CONSISTENCY_ADVISORY_LOCK = [1_431_519_564, 1_129_206_866] as const;

export const OPERATIONAL_SNAPSHOT_SQL = `
  WITH observed_clock AS (
    SELECT clock_timestamp() AS observed_at
  ),
  alarm_metrics AS (
    SELECT
      COALESCE(sum(a.sent), 0)::bigint AS sent,
      COALESCE(sum(a.failed), 0)::bigint AS failed,
      COALESCE(sum(a.unresolved), 0)::bigint AS unresolved
    FROM alarms a
    WHERE a.audience_finalized_at IS NOT NULL
  ),
  queue_metrics AS (
    SELECT
      count(*)::bigint AS queue_depth,
      (count(*) FILTER (WHERE d.next_action_at <= c.observed_at))::bigint
        AS due_queue_depth,
      CASE
        WHEN min(d.next_action_at) FILTER (
          WHERE d.next_action_at <= c.observed_at
        ) IS NULL THEN NULL
        ELSE GREATEST(
          0,
          floor(
            extract(
              epoch FROM (
                c.observed_at - min(d.next_action_at) FILTER (
                  WHERE d.next_action_at <= c.observed_at
                )
              )
            )
            * 1000
          )
        )::bigint
      END AS oldest_due_age_ms,
      (count(*) FILTER (WHERE d.state = 'RETRY_WAIT'))::bigint AS retry_wait
    FROM deliveries d
    CROSS JOIN observed_clock c
    WHERE d.state IN ('READY', 'RETRY_WAIT')
    GROUP BY c.observed_at
  ),
  unknown_metrics AS (
    SELECT (
      (SELECT count(*) FROM deliveries WHERE state = 'UNKNOWN')
      + (SELECT count(*) FROM deliveries WHERE state = 'RECONCILING')
    )::bigint AS unknown
  ),
  active_send_metrics AS (
    SELECT count(*)::bigint AS active_send_leases
    FROM deliveries
    WHERE state IN ('CLAIMED', 'IN_FLIGHT')
  ),
  provider_metrics AS (
    SELECT
      count(*)::bigint AS provider_attempts_window,
      (count(*) FILTER (WHERE a.result_code = 'ACCEPTED'))::bigint
        AS provider_accepted_window,
      (count(*) FILTER (WHERE a.http_status = 429))::bigint
        AS provider_rate_limited_window
    FROM delivery_attempts a
    CROSS JOIN observed_clock c
    WHERE a.request_phase = 'FINISHED'
      AND a.finished_at >= c.observed_at - ($1::integer * interval '1 second')
      AND a.finished_at <= c.observed_at
  )
  SELECT
    c.observed_at,
    COALESCE(q.queue_depth, 0)::bigint AS queue_depth,
    COALESCE(q.due_queue_depth, 0)::bigint AS due_queue_depth,
    q.oldest_due_age_ms,
    a.sent,
    a.failed,
    COALESCE(q.retry_wait, 0)::bigint AS retry_wait,
    u.unknown,
    a.unresolved,
    s.active_send_leases,
    p.provider_attempts_window,
    p.provider_accepted_window,
    p.provider_rate_limited_window
  FROM observed_clock c
  CROSS JOIN alarm_metrics a
  LEFT JOIN queue_metrics q ON true
  CROSS JOIN unknown_metrics u
  CROSS JOIN active_send_metrics s
  CROSS JOIN provider_metrics p
`;

function parseEvidenceEntries(value: unknown): ProviderEvidenceEntry[] {
  if (!Array.isArray(value)) {
    throw new Error('Persisted evidence entries are not an array');
  }
  return value.map((candidate) => {
    if (candidate === null || typeof candidate !== 'object' || Array.isArray(candidate)) {
      throw new Error('Persisted evidence entry is not an object');
    }
    const record = candidate as Record<string, unknown>;
    if (typeof record.messageId !== 'string' || typeof record.recipientId !== 'string') {
      throw new Error('Persisted evidence entry is malformed');
    }
    return {
      messageId: record.messageId,
      recipientId: record.recipientId,
      ...(typeof record.sentAt === 'string' ? { sentAt: record.sentAt } : {}),
    };
  });
}

function parseSafeCount(value: number | string, field: string): number {
  const parsed = typeof value === 'number' ? value : Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new Error(`Operational snapshot ${field} is not a non-negative safe integer`);
  }
  return parsed;
}

export class PostgresMaintenanceRepository implements MaintenanceRepository {
  private counterConsistencyLeadership: CounterConsistencyLeadership | null = null;
  private counterConsistencyLeadershipAttempt: Promise<CounterConsistencyLeadership | null> | null =
    null;
  private counterConsistencyCursor: CounterConsistencyCursor | null = null;

  public constructor(private readonly pool: Pool) {}

  public sweepCanceledDeliveries(batchSize: number): Promise<number> {
    return this.transaction(async (client) => {
      const alarmResult = await client.query<MaintenanceAlarmRow>(`
        SELECT a.id
        FROM alarms a
        WHERE a.status = 'CANCEL_REQUESTED'
          AND a.audience_finalized_at IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM deliveries d
            WHERE d.alarm_id = a.id
              AND (
                d.state IN ('READY', 'RETRY_WAIT')
                OR (d.state = 'CLAIMED' AND d.current_attempt_id IS NULL)
              )
          )
        ORDER BY a.cancel_requested_at, a.id
        FOR UPDATE OF a SKIP LOCKED
        LIMIT 1
      `);
      const alarm = alarmResult.rows[0];
      if (alarm === undefined) {
        return 0;
      }

      const canceled = await client.query<{ id: string }>(
        `
          WITH candidates AS (
            SELECT d.id
            FROM deliveries d
            WHERE d.alarm_id = $1
              AND (
                d.state IN ('READY', 'RETRY_WAIT')
                OR (d.state = 'CLAIMED' AND d.current_attempt_id IS NULL)
              )
            ORDER BY d.id
            FOR UPDATE SKIP LOCKED
            LIMIT $2
          )
          UPDATE deliveries d
          SET
            state = 'CANCELED',
            result_code = 'ALARM_CANCELED',
            current_attempt_id = NULL,
            lease_token = NULL,
            lease_until = NULL,
            updated_at = clock_timestamp()
          FROM candidates c
          WHERE d.id = c.id
          RETURNING d.id
        `,
        [alarm.id, batchSize],
      );
      const count = canceled.rowCount ?? 0;
      if (count === 0) {
        return 0;
      }

      await client.query(
        `
          UPDATE alarms
          SET
            canceled = canceled + $2,
            active = active - $2,
            status = CASE
              WHEN active - $2 > 0 THEN 'CANCEL_REQUESTED'
              WHEN unresolved > 0 OR duplicate_count > 0 OR incident_count > 0
                THEN 'ATTENTION_REQUIRED'
              ELSE 'CANCELED'
            END,
            finished_at = CASE
              WHEN active - $2 = 0 THEN COALESCE(finished_at, clock_timestamp())
              ELSE finished_at
            END,
            updated_at = clock_timestamp(),
            version = version + 1
          WHERE id = $1 AND active >= $2
        `,
        [alarm.id, count],
      );
      return count;
    });
  }

  public async reapExpiredSendLease(): Promise<SendLeaseRecoveryEvent | null> {
    const candidate = await this.pool
      .query<ExpiredDeliveryCandidate>(`
        SELECT id, alarm_id
        FROM deliveries
        WHERE state IN ('CLAIMED', 'IN_FLIGHT')
          AND lease_until <= clock_timestamp()
        ORDER BY lease_until, id
        LIMIT 1
      `)
      .catch((error: unknown) => {
        throw new PersistenceUnavailableError(error);
      });
    const row = candidate.rows[0];
    if (row === undefined) {
      return null;
    }

    return this.transaction(async (client) => {
      const alarm = await client.query<{ id: string; status: string }>(
        'SELECT id, status FROM alarms WHERE id = $1 FOR UPDATE',
        [row.alarm_id],
      );
      const deliveryResult = await client.query<LockedDelivery>(
        `
          SELECT
            id, state, current_attempt_id, lease_until, lease_generation,
            lease_until <= clock_timestamp() AS lease_expired
          FROM deliveries
          WHERE id = $1
          FOR UPDATE
        `,
        [row.id],
      );
      const delivery = deliveryResult.rows[0];
      if (
        alarm.rows[0] === undefined ||
        delivery === undefined ||
        delivery.lease_until === null ||
        !delivery.lease_expired ||
        !['CLAIMED', 'IN_FLIGHT'].includes(delivery.state)
      ) {
        return null;
      }

      if (delivery.state === 'CLAIMED' && delivery.current_attempt_id === null) {
        const canceled = alarm.rows[0].status === 'CANCEL_REQUESTED';
        if (canceled) {
          await client.query(
            `
              UPDATE deliveries
              SET
                state = 'CANCELED', result_code = 'CANCELED_AFTER_CLAIM_EXPIRY',
                lease_token = NULL, lease_until = NULL, updated_at = clock_timestamp()
              WHERE id = $1
            `,
            [delivery.id],
          );
          await client.query(
            `
              UPDATE alarms
              SET
                active = active - 1,
                canceled = canceled + 1,
                status = CASE
                  WHEN active - 1 > 0 THEN 'CANCEL_REQUESTED'
                  WHEN unresolved > 0 OR duplicate_count > 0 OR incident_count > 0
                    THEN 'ATTENTION_REQUIRED'
                  ELSE 'CANCELED'
                END,
                finished_at = CASE
                  WHEN active - 1 = 0 THEN COALESCE(finished_at, clock_timestamp())
                  ELSE finished_at
                END,
                updated_at = clock_timestamp(),
                version = version + 1
              WHERE id = $1 AND active > 0
            `,
            [row.alarm_id],
          );
        } else {
          await client.query(
            `
              UPDATE deliveries
              SET
                state = 'READY', lease_token = NULL, lease_until = NULL,
                updated_at = clock_timestamp()
              WHERE id = $1
            `,
            [delivery.id],
          );
          await client.query("SELECT pg_notify('alarm_work', $1)", [row.alarm_id]);
        }
        return {
          alarmId: row.alarm_id,
          deliveryId: delivery.id,
          attemptId: null,
          leaseGeneration: delivery.lease_generation,
          previousState: 'CLAIMED',
          requestPhase: null,
          reason: canceled ? 'CLAIM_EXPIRED_CANCELED' : 'CLAIM_EXPIRED_REQUEUED',
          resultingState: canceled ? 'CANCELED' : 'READY',
        };
      }

      if (delivery.state === 'IN_FLIGHT' && delivery.current_attempt_id !== null) {
        const attemptResult = await client.query<LockedAttempt>(
          `
            SELECT id, request_phase
            FROM delivery_attempts
            WHERE id = $1 AND delivery_id = $2
            FOR UPDATE
          `,
          [delivery.current_attempt_id, delivery.id],
        );
        const attempt = attemptResult.rows[0];

        if (attempt?.request_phase === 'PREPARED') {
          await client.query(
            `
              UPDATE delivery_attempts
              SET
                state = 'FINISHED_NOT_SENT',
                request_phase = 'FINISHED_NOT_SENT',
                budget_consumed = false,
                result_code = 'WORKER_EXITED_BEFORE_PROVIDER_COMMIT',
                finished_at = clock_timestamp()
              WHERE id = $1
                AND state = 'STARTED'
                AND request_phase = 'PREPARED'
            `,
            [attempt.id],
          );

          const canceled = alarm.rows[0].status === 'CANCEL_REQUESTED';
          if (canceled) {
            await client.query(
              `
                UPDATE deliveries
                SET
                  state = 'CANCELED',
                  result_code = 'CANCELED_AFTER_PRE_COMMIT_WORKER_EXIT',
                  post_budget_count = post_budget_count - 1,
                  current_attempt_id = NULL,
                  lease_token = NULL,
                  lease_until = NULL,
                  updated_at = clock_timestamp()
                WHERE id = $1 AND post_budget_count > 0
              `,
              [delivery.id],
            );
            await client.query(
              `
                UPDATE alarms
                SET
                  active = active - 1,
                  canceled = canceled + 1,
                  status = CASE
                    WHEN active - 1 > 0 THEN 'CANCEL_REQUESTED'
                    WHEN unresolved > 0 OR duplicate_count > 0 OR incident_count > 0
                      THEN 'ATTENTION_REQUIRED'
                    ELSE 'CANCELED'
                  END,
                  finished_at = CASE
                    WHEN active - 1 = 0 THEN COALESCE(finished_at, clock_timestamp())
                    ELSE finished_at
                  END,
                  updated_at = clock_timestamp(),
                  version = version + 1
                WHERE id = $1 AND active > 0
              `,
              [row.alarm_id],
            );
          } else {
            await client.query(
              `
                UPDATE deliveries
                SET
                  state = 'READY',
                  result_code = 'RECOVERED_PRE_PROVIDER_COMMIT',
                  post_budget_count = post_budget_count - 1,
                  current_attempt_id = NULL,
                  lease_token = NULL,
                  lease_until = NULL,
                  updated_at = clock_timestamp()
                WHERE id = $1 AND post_budget_count > 0
              `,
              [delivery.id],
            );
            await client.query("SELECT pg_notify('alarm_work', $1)", [row.alarm_id]);
          }
          return {
            alarmId: row.alarm_id,
            deliveryId: delivery.id,
            attemptId: attempt.id,
            leaseGeneration: delivery.lease_generation,
            previousState: 'IN_FLIGHT',
            requestPhase: attempt.request_phase,
            reason: canceled ? 'PREPARED_EXPIRED_CANCELED' : 'PREPARED_EXPIRED_REQUEUED',
            resultingState: canceled ? 'CANCELED' : 'READY',
          };
        }

        await client.query(
          `
            UPDATE deliveries
            SET
              state = 'UNKNOWN',
              result_code = 'SEND_LEASE_EXPIRED',
              first_unknown_at = COALESCE(first_unknown_at, clock_timestamp()),
              next_action_at = clock_timestamp(),
              lease_token = NULL,
              lease_until = NULL,
              updated_at = clock_timestamp()
            WHERE id = $1
          `,
          [delivery.id],
        );
        return {
          alarmId: row.alarm_id,
          deliveryId: delivery.id,
          attemptId: delivery.current_attempt_id,
          leaseGeneration: delivery.lease_generation,
          previousState: 'IN_FLIGHT',
          requestPhase: attempt?.request_phase ?? null,
          reason:
            attempt === undefined
              ? 'IN_FLIGHT_ATTEMPT_MISSING_TO_UNKNOWN'
              : 'COMMITTED_EXPIRED_TO_UNKNOWN',
          resultingState: 'UNKNOWN',
        };
      }

      return null;
    });
  }

  public async adoptUnconsumedEvidence(): Promise<boolean> {
    const candidate = await this.pool
      .query<TerminalEvidenceCandidate>(`
        SELECT d.id AS delivery_id, d.alarm_id
        FROM delivery_evidence e
        JOIN deliveries d ON d.id = e.delivery_id
        WHERE e.consumed_at IS NULL
          AND d.state IN ('SENT', 'FAILED_PERMANENT', 'FAILED_RETRY_EXHAUSTED', 'CANCELED')
        ORDER BY e.observed_at, e.id
        LIMIT 1
      `)
      .catch((error: unknown) => {
        throw new PersistenceUnavailableError(error);
      });
    const target = candidate.rows[0];
    if (target === undefined) {
      return false;
    }

    return this.transaction(async (client) => {
      await client.query('SELECT id FROM alarms WHERE id = $1 FOR UPDATE', [target.alarm_id]);
      const deliveryResult = await client.query<TerminalDelivery>(
        `
          SELECT
            id, recipient_id, state, provider_message_id,
            duplicate_detected, post_terminal_incident_code
          FROM deliveries
          WHERE id = $1
          FOR UPDATE
        `,
        [target.delivery_id],
      );
      const delivery = deliveryResult.rows[0];
      if (
        delivery === undefined ||
        !['SENT', 'FAILED_PERMANENT', 'FAILED_RETRY_EXHAUSTED', 'CANCELED'].includes(delivery.state)
      ) {
        return false;
      }

      const evidence = await client.query<StoredEvidence>(
        `
          SELECT id, entries
          FROM delivery_evidence
          WHERE delivery_id = $1 AND consumed_at IS NULL
          ORDER BY observed_at, id
          FOR UPDATE
        `,
        [delivery.id],
      );
      if (evidence.rowCount === 0) {
        return false;
      }

      const entries = evidence.rows.flatMap(({ entries: stored }) => parseEvidenceEntries(stored));
      if (delivery.provider_message_id !== null) {
        entries.push({
          messageId: delivery.provider_message_id,
          recipientId: delivery.recipient_id,
        });
      }
      const decision = evaluateEvidence(entries, delivery.recipient_id);
      let incidentCode: string | null = null;
      if (decision.kind === 'RECIPIENT_MISMATCH') {
        incidentCode = 'LATE_PROVIDER_RECIPIENT_MISMATCH';
      } else if (decision.kind === 'EVIDENCE_CONFLICT') {
        incidentCode = 'LATE_PROVIDER_EVIDENCE_CONFLICT';
      } else if (decision.distinctMessageIds.length >= 2 && delivery.state === 'SENT') {
        incidentCode = 'LATE_DISTINCT_DUPLICATE';
      } else if (
        decision.distinctMessageIds.length >= 1 &&
        ['FAILED_PERMANENT', 'FAILED_RETRY_EXHAUSTED'].includes(delivery.state)
      ) {
        incidentCode = 'LATE_POSITIVE_AFTER_FAILED';
      } else if (decision.distinctMessageIds.length >= 1 && delivery.state === 'CANCELED') {
        incidentCode = 'LATE_POSITIVE_AFTER_CANCELED';
      }

      const adoptIncident = incidentCode !== null && delivery.post_terminal_incident_code === null;
      const adoptDuplicate =
        incidentCode === 'LATE_DISTINCT_DUPLICATE' && !delivery.duplicate_detected;
      if (adoptIncident) {
        await client.query(
          `
            UPDATE deliveries
            SET
              post_terminal_incident_code = $2,
              post_terminal_incident_at = clock_timestamp(),
              duplicate_detected = duplicate_detected OR $3,
              updated_at = clock_timestamp()
            WHERE id = $1 AND post_terminal_incident_code IS NULL
          `,
          [delivery.id, incidentCode, adoptDuplicate],
        );
        await client.query(
          `
            UPDATE alarms
            SET
              incident_count = incident_count + 1,
              incident_counts = jsonb_set(
                incident_counts,
                ARRAY[$2],
                to_jsonb(COALESCE((incident_counts ->> $2)::integer, 0) + 1),
                true
              ),
              incident_first_detected_at = COALESCE(
                incident_first_detected_at,
                clock_timestamp()
              ),
              incident_last_detected_at = clock_timestamp(),
              duplicate_count = duplicate_count + CASE WHEN $3 THEN 1 ELSE 0 END,
              status = CASE WHEN active = 0 THEN 'ATTENTION_REQUIRED' ELSE status END,
              updated_at = clock_timestamp(),
              version = version + 1
            WHERE id = $1
          `,
          [target.alarm_id, incidentCode, adoptDuplicate],
        );
      }

      await client.query(
        `
          UPDATE delivery_evidence
          SET decision = $2, consumed_at = clock_timestamp()
          WHERE id = ANY($1::uuid[]) AND consumed_at IS NULL
        `,
        [evidence.rows.map(({ id }) => id), incidentCode ?? 'TERMINAL_COMPATIBLE_EVIDENCE'],
      );
      return true;
    });
  }

  public async readOperationalSnapshot(windowSeconds: number): Promise<OperationalSnapshot | null> {
    const leadership = await this.acquireCounterConsistencyLeadership();
    if (leadership === null) {
      return null;
    }

    const boundedWindowSeconds = Math.min(3_600, Math.max(1, Math.trunc(windowSeconds)));
    try {
      const result = await leadership.client.query<OperationalSnapshotRow>(
        OPERATIONAL_SNAPSHOT_SQL,
        [boundedWindowSeconds],
      );
      const row = result.rows[0];
      if (row === undefined) {
        throw new Error('Operational snapshot query returned no row');
      }
      const providerAcceptedWindow = parseSafeCount(
        row.provider_accepted_window,
        'providerAcceptedWindow',
      );
      return {
        observedAt: row.observed_at.toISOString(),
        windowSeconds: boundedWindowSeconds,
        queueDepth: parseSafeCount(row.queue_depth, 'queueDepth'),
        dueQueueDepth: parseSafeCount(row.due_queue_depth, 'dueQueueDepth'),
        oldestDueAgeMs:
          row.oldest_due_age_ms === null
            ? null
            : parseSafeCount(row.oldest_due_age_ms, 'oldestDueAgeMs'),
        sent: parseSafeCount(row.sent, 'sent'),
        failed: parseSafeCount(row.failed, 'failed'),
        retryWait: parseSafeCount(row.retry_wait, 'retryWait'),
        unknown: parseSafeCount(row.unknown, 'unknown'),
        unresolved: parseSafeCount(row.unresolved, 'unresolved'),
        activeSendLeases: parseSafeCount(row.active_send_leases, 'activeSendLeases'),
        providerAttemptsWindow: parseSafeCount(
          row.provider_attempts_window,
          'providerAttemptsWindow',
        ),
        providerAcceptedWindow,
        providerRateLimitedWindow: parseSafeCount(
          row.provider_rate_limited_window,
          'providerRateLimitedWindow',
        ),
        providerThroughputPerSecond: providerAcceptedWindow / boundedWindowSeconds,
      };
    } catch (error) {
      this.discardCounterConsistencyLeadership();
      throw new PersistenceUnavailableError(error);
    }
  }

  public async findCounterInconsistencies(
    limit: number,
  ): Promise<readonly AlarmCounterInconsistency[]> {
    const leadership = await this.acquireCounterConsistencyLeadership();
    if (leadership === null) {
      return [];
    }

    try {
      const result = await leadership.client.query<CounterInconsistencyRow>(
        `
          WITH observed AS (
            SELECT
              alarm_id,
              count(*)::integer AS total,
              (count(*) FILTER (WHERE state = 'SENT'))::integer AS sent,
              (
                count(*) FILTER (
                  WHERE state IN ('FAILED_PERMANENT', 'FAILED_RETRY_EXHAUSTED')
                )
              )::integer AS failed,
              (count(*) FILTER (WHERE state = 'CANCELED'))::integer AS canceled,
              (
                count(*) FILTER (
                  WHERE state NOT IN (
                    'SENT', 'FAILED_PERMANENT', 'FAILED_RETRY_EXHAUSTED',
                    'CANCELED', 'NEEDS_REVIEW'
                  )
                )
              )::integer AS active,
              (count(*) FILTER (WHERE state = 'NEEDS_REVIEW'))::integer AS unresolved,
              (count(*) FILTER (WHERE duplicate_detected))::integer AS duplicate_count,
              (
                count(*) FILTER (WHERE post_terminal_incident_code IS NOT NULL)
              )::integer AS incident_count
            FROM deliveries
            GROUP BY alarm_id
          )
          SELECT
            a.id AS alarm_id,
            a.updated_at AS alarm_updated_at,
            a.total AS stored_total,
            a.sent AS stored_sent,
            a.failed AS stored_failed,
            a.canceled AS stored_canceled,
            a.active AS stored_active,
            a.unresolved AS stored_unresolved,
            a.duplicate_count AS stored_duplicate_count,
            a.incident_count AS stored_incident_count,
            COALESCE(o.total, 0)::integer AS observed_total,
            COALESCE(o.sent, 0)::integer AS observed_sent,
            COALESCE(o.failed, 0)::integer AS observed_failed,
            COALESCE(o.canceled, 0)::integer AS observed_canceled,
            COALESCE(o.active, 0)::integer AS observed_active,
            COALESCE(o.unresolved, 0)::integer AS observed_unresolved,
            COALESCE(o.duplicate_count, 0)::integer AS observed_duplicate_count,
            COALESCE(o.incident_count, 0)::integer AS observed_incident_count
          FROM alarms a
          LEFT JOIN observed o ON o.alarm_id = a.id
          WHERE a.audience_finalized_at IS NOT NULL
            AND (
              a.total IS DISTINCT FROM COALESCE(o.total, 0)
              OR a.sent IS DISTINCT FROM COALESCE(o.sent, 0)
              OR a.failed IS DISTINCT FROM COALESCE(o.failed, 0)
              OR a.canceled IS DISTINCT FROM COALESCE(o.canceled, 0)
              OR a.active IS DISTINCT FROM COALESCE(o.active, 0)
              OR a.unresolved IS DISTINCT FROM COALESCE(o.unresolved, 0)
              OR a.duplicate_count IS DISTINCT FROM COALESCE(o.duplicate_count, 0)
              OR a.incident_count IS DISTINCT FROM COALESCE(o.incident_count, 0)
            )
          ORDER BY
            CASE
              WHEN $2::timestamptz IS NULL THEN 0
              WHEN (a.updated_at, a.id) > ($2::timestamptz, $3::uuid) THEN 0
              ELSE 1
            END,
            a.updated_at,
            a.id
          LIMIT $1
        `,
        [
          Math.max(1, limit),
          this.counterConsistencyCursor?.updatedAt ?? null,
          this.counterConsistencyCursor?.alarmId ?? null,
        ],
      );

      const last = result.rows.at(-1);
      if (last !== undefined) {
        this.counterConsistencyCursor = {
          updatedAt: last.alarm_updated_at,
          alarmId: last.alarm_id,
        };
      }

      return result.rows.map((row) => ({
        alarmId: row.alarm_id,
        stored: {
          total: row.stored_total,
          sent: row.stored_sent,
          failed: row.stored_failed,
          canceled: row.stored_canceled,
          active: row.stored_active,
          unresolved: row.stored_unresolved,
          duplicateCount: row.stored_duplicate_count,
          incidentCount: row.stored_incident_count,
        },
        observed: {
          total: row.observed_total,
          sent: row.observed_sent,
          failed: row.observed_failed,
          canceled: row.observed_canceled,
          active: row.observed_active,
          unresolved: row.observed_unresolved,
          duplicateCount: row.observed_duplicate_count,
          incidentCount: row.observed_incident_count,
        },
      }));
    } catch (error) {
      this.discardCounterConsistencyLeadership();
      throw new PersistenceUnavailableError(error);
    }
  }

  public async releaseCounterConsistencyLeadership(): Promise<void> {
    const leadership = this.counterConsistencyLeadership;
    this.counterConsistencyLeadership = null;
    this.counterConsistencyCursor = null;
    if (leadership === null) {
      return;
    }

    let reusable = true;
    try {
      await leadership.client.query('SELECT pg_advisory_unlock($1, $2)', [
        ...COUNTER_CONSISTENCY_ADVISORY_LOCK,
      ]);
    } catch {
      reusable = false;
    } finally {
      leadership.detachErrorHandler();
      leadership.client.release(!reusable);
    }
  }

  public async cleanupExpansionBatch(batchSize: number, leaseDurationMs: number): Promise<boolean> {
    const lease = await this.claimExpansionCleanup(leaseDurationMs);
    if (lease === null) {
      return false;
    }
    return this.runExpansionCleanupBatch(lease, batchSize);
  }

  private claimExpansionCleanup(leaseDurationMs: number): Promise<CleanupLease | null> {
    return this.transaction(async (client) => {
      const alarmResult = await client.query<MaintenanceAlarmRow>(`
        SELECT a.id
        FROM alarms a
        WHERE EXISTS (
          SELECT 1
          FROM alarm_expansion_jobs j
          WHERE j.alarm_id = a.id
            AND j.state IN ('COMPLETED', 'FAILED', 'CANCELED')
            AND (
              j.cleanup_state = 'PENDING'
              OR (
                j.cleanup_state = 'RUNNING'
                AND j.cleanup_lease_until <= clock_timestamp()
              )
            )
        )
        ORDER BY a.created_at, a.id
        FOR UPDATE OF a SKIP LOCKED
        LIMIT 1
      `);
      const alarm = alarmResult.rows[0];
      if (alarm === undefined) {
        return null;
      }

      const jobResult = await client.query<CleanupJob>(
        'SELECT * FROM alarm_expansion_jobs WHERE alarm_id = $1 FOR UPDATE',
        [alarm.id],
      );
      const job = jobResult.rows[0];
      if (job?.cleanup_run_generation === null || job?.cleanup_run_generation === undefined) {
        throw new Error('Expansion cleanup is missing its stable run generation');
      }

      const cleanupToken = randomUUID();
      const claimed = await client.query<{
        cleanup_run_generation: string;
        cleanup_lease_generation: number;
        cleanup_lease_token: string;
      }>(
        `
          UPDATE alarm_expansion_jobs
          SET
            cleanup_state = 'RUNNING',
            cleanup_lease_generation = cleanup_lease_generation + 1,
            cleanup_lease_token = $2,
            cleanup_lease_until = clock_timestamp() + ($3::integer * interval '1 millisecond'),
            updated_at = clock_timestamp()
          WHERE alarm_id = $1
          RETURNING
            cleanup_run_generation,
            cleanup_lease_generation,
            cleanup_lease_token
        `,
        [alarm.id, cleanupToken, leaseDurationMs],
      );
      const row = claimed.rows[0];
      if (row === undefined) {
        throw new Error('Expansion cleanup claim returned no row');
      }
      return {
        alarmId: alarm.id,
        runGeneration: row.cleanup_run_generation,
        leaseGeneration: row.cleanup_lease_generation,
        leaseToken: row.cleanup_lease_token,
      };
    });
  }

  private runExpansionCleanupBatch(lease: CleanupLease, batchSize: number): Promise<boolean> {
    return this.transaction(async (client) => {
      await client.query('SELECT id FROM alarms WHERE id = $1 FOR UPDATE', [lease.alarmId]);
      const jobResult = await client.query<CleanupJob>(
        'SELECT * FROM alarm_expansion_jobs WHERE alarm_id = $1 FOR UPDATE',
        [lease.alarmId],
      );
      const job = jobResult.rows[0];
      if (
        job === undefined ||
        job.cleanup_state !== 'RUNNING' ||
        job.cleanup_run_generation !== lease.runGeneration ||
        job.cleanup_lease_generation !== lease.leaseGeneration ||
        job.cleanup_lease_token !== lease.leaseToken
      ) {
        return false;
      }

      await client.query(
        `
          WITH candidates AS (
            SELECT ctid
            FROM alarm_expansion_recipients
            WHERE alarm_id = $1 AND run_generation = $2
            ORDER BY recipient_id
            LIMIT $3
            FOR UPDATE SKIP LOCKED
          )
          DELETE FROM alarm_expansion_recipients r
          USING candidates c
          WHERE r.ctid = c.ctid
        `,
        [lease.alarmId, lease.runGeneration, batchSize],
      );
      await client.query(
        `
          WITH candidates AS (
            SELECT ctid
            FROM alarm_expansion_seen_cursors
            WHERE alarm_id = $1 AND run_generation = $2
            ORDER BY cursor_hash
            LIMIT $3
            FOR UPDATE SKIP LOCKED
          )
          DELETE FROM alarm_expansion_seen_cursors c
          USING candidates selected
          WHERE c.ctid = selected.ctid
        `,
        [lease.alarmId, lease.runGeneration, batchSize],
      );

      const remaining = await client.query<{ remaining: boolean }>(
        `
          SELECT
            EXISTS (
              SELECT 1 FROM alarm_expansion_recipients
              WHERE alarm_id = $1 AND run_generation = $2
            ) OR EXISTS (
              SELECT 1 FROM alarm_expansion_seen_cursors
              WHERE alarm_id = $1 AND run_generation = $2
            ) AS remaining
        `,
        [lease.alarmId, lease.runGeneration],
      );
      const complete = remaining.rows[0]?.remaining !== true;
      await client.query(
        `
          UPDATE alarm_expansion_jobs
          SET
            cleanup_state = $2,
            cleanup_lease_token = NULL,
            cleanup_lease_until = NULL,
            cleanup_completed_at = CASE WHEN $2 = 'COMPLETED' THEN clock_timestamp() ELSE NULL END,
            updated_at = clock_timestamp()
          WHERE alarm_id = $1
            AND cleanup_run_generation = $4
            AND cleanup_lease_generation = $5
            AND cleanup_lease_token = $3
        `,
        [
          lease.alarmId,
          complete ? 'COMPLETED' : 'PENDING',
          lease.leaseToken,
          lease.runGeneration,
          lease.leaseGeneration,
        ],
      );
      return true;
    });
  }

  private async acquireCounterConsistencyLeadership(): Promise<CounterConsistencyLeadership | null> {
    if (this.counterConsistencyLeadership !== null) {
      return this.counterConsistencyLeadership;
    }
    if (this.counterConsistencyLeadershipAttempt !== null) {
      return this.counterConsistencyLeadershipAttempt;
    }

    this.counterConsistencyLeadershipAttempt = this.tryAcquireCounterConsistencyLeadership();
    try {
      return await this.counterConsistencyLeadershipAttempt;
    } finally {
      this.counterConsistencyLeadershipAttempt = null;
    }
  }

  private async tryAcquireCounterConsistencyLeadership(): Promise<CounterConsistencyLeadership | null> {
    const client = await this.pool.connect().catch((error: unknown) => {
      throw new PersistenceUnavailableError(error);
    });
    const detachErrorHandler = attachPostgresClientErrorHandler(client, {
      component: 'counter-consistency-leader',
    });
    try {
      const result = await client.query<{ acquired: boolean }>(
        'SELECT pg_try_advisory_lock($1, $2) AS acquired',
        [...COUNTER_CONSISTENCY_ADVISORY_LOCK],
      );
      if (result.rows[0]?.acquired !== true) {
        detachErrorHandler();
        client.release();
        return null;
      }

      const leadership = { client, detachErrorHandler };
      this.counterConsistencyLeadership = leadership;
      return leadership;
    } catch (error) {
      detachErrorHandler();
      client.release(true);
      if (error instanceof PersistenceUnavailableError) {
        throw error;
      }
      throw new PersistenceUnavailableError(error);
    }
  }

  private discardCounterConsistencyLeadership(): void {
    const leadership = this.counterConsistencyLeadership;
    this.counterConsistencyLeadership = null;
    this.counterConsistencyCursor = null;
    if (leadership === null) {
      return;
    }
    leadership.detachErrorHandler();
    leadership.client.release(true);
  }

  private async transaction<T>(work: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.pool.connect().catch((error: unknown) => {
      throw new PersistenceUnavailableError(error);
    });
    const detachErrorHandler = attachPostgresClientErrorHandler(client, {
      component: 'maintenance-repository',
    });
    let reusable = false;
    try {
      await client.query('BEGIN');
      const result = await work(client);
      await client.query('COMMIT');
      reusable = true;
      return result;
    } catch (error) {
      await rollbackPostgresTransaction(client);
      if (error instanceof PersistenceUnavailableError) {
        throw error;
      }
      throw new PersistenceUnavailableError(error);
    } finally {
      detachErrorHandler();
      if (reusable) {
        client.release();
      } else {
        client.release(true);
      }
    }
  }
}
