import { createHash, randomUUID } from 'node:crypto';

import type { Pool, PoolClient } from 'pg';

import { PersistenceUnavailableError } from '../../application/alarm-errors';
import type {
  DispatchAttempt,
  DispatchClaim,
  DispatchRepository,
  PacedPermit,
  ProviderSendResult,
} from '../../application/dispatch';
import type { AlarmType } from '../../domain/alarm';
import { attachPostgresClientErrorHandler } from './postgres-pool-observability';

interface ClaimRow {
  readonly alarm_id: string;
  readonly alarm_type: AlarmType;
  readonly alarm_body: string;
  readonly delivery_id: string;
  readonly recipient_id: string;
  readonly client_ref: string;
  readonly lease_generation: number;
  readonly lease_token: string;
}

interface LockedAlarmRow {
  readonly id: string;
  readonly type: AlarmType;
  readonly status: string;
  readonly cancel_requested_at: Date | null;
}

interface LockedDeliveryRow {
  readonly id: string;
  readonly alarm_id: string;
  readonly recipient_id: string;
  readonly client_ref: string;
  readonly state: string;
  readonly attempt_no: number;
  readonly post_budget_count: number;
  readonly rate_limit_count: number;
  readonly first_rate_limited_at: Date | null;
  readonly rate_limit_window_expired: boolean;
  readonly lease_generation: number;
  readonly lease_token: string | null;
  readonly current_attempt_id: string | null;
}

interface LockedAttemptRow {
  readonly id: string;
  readonly state: 'STARTED' | 'FINISHED_NOT_SENT' | 'FINISHED';
  readonly request_phase: 'PREPARED' | 'COMMITTED' | 'FINISHED_NOT_SENT' | 'FINISHED';
  readonly lease_generation: number;
}

function evidenceFor(result: ProviderSendResult, recipientId: string): readonly object[] {
  return result.kind === 'ACCEPTED' ? [{ messageId: result.messageId, recipientId }] : [];
}

export interface PostgresDispatchRepositoryOptions {
  readonly random?: () => number;
  readonly retryBaseMs?: number;
  readonly retryCapMs?: number;
  readonly rateLimitJitterCapMs?: number;
}

export function fullJitterDelay(
  random: () => number,
  attemptCount: number,
  baseMs = 200,
  capMs = 30_000,
): number {
  const sample = random();
  if (!Number.isFinite(sample) || sample < 0 || sample >= 1) {
    throw new RangeError('random must return a finite number in [0, 1)');
  }
  const exponentialCap = Math.min(capMs, baseMs * 2 ** Math.max(0, attemptCount - 1));
  return Math.floor(sample * exponentialCap);
}

export class PostgresDispatchRepository implements DispatchRepository {
  private readonly random: () => number;
  private readonly retryBaseMs: number;
  private readonly retryCapMs: number;
  private readonly rateLimitJitterCapMs: number;

  public constructor(
    private readonly pool: Pool,
    options: PostgresDispatchRepositoryOptions = {},
  ) {
    this.random = options.random ?? Math.random;
    this.retryBaseMs = options.retryBaseMs ?? 200;
    this.retryCapMs = options.retryCapMs ?? 30_000;
    this.rateLimitJitterCapMs = options.rateLimitJitterCapMs ?? 1_000;

    for (const [name, value] of [
      ['retryBaseMs', this.retryBaseMs],
      ['retryCapMs', this.retryCapMs],
      ['rateLimitJitterCapMs', this.rateLimitJitterCapMs],
    ] as const) {
      if (!Number.isSafeInteger(value) || value < 1) {
        throw new RangeError(`${name} must be a positive safe integer`);
      }
    }
    if (this.retryBaseMs > this.retryCapMs) {
      throw new RangeError('retryBaseMs must not exceed retryCapMs');
    }
  }

  public async hasEligible(minimumPriority: number): Promise<boolean> {
    try {
      const result = await this.pool.query<{ eligible: boolean }>(
        `
          SELECT EXISTS (
            SELECT 1
            FROM deliveries d
            JOIN alarms a ON a.id = d.alarm_id
            WHERE d.state IN ('READY', 'RETRY_WAIT')
              AND d.next_action_at <= clock_timestamp()
              AND d.priority >= $1
              AND a.audience_finalized_at IS NOT NULL
              AND a.status = 'SENDING'
              AND a.cancel_requested_at IS NULL
          ) AS eligible
        `,
        [minimumPriority],
      );
      return result.rows[0]?.eligible === true;
    } catch (error) {
      throw new PersistenceUnavailableError(error);
    }
  }

  public claimNext(
    minimumPriority: number,
    leaseDurationMs: number,
  ): Promise<DispatchClaim | null> {
    return this.transaction(async (client) => {
      const leaseToken = randomUUID();
      const result = await client.query<ClaimRow>(
        `
          WITH candidate AS (
            SELECT d.id
            FROM deliveries d
            JOIN alarms a ON a.id = d.alarm_id
            WHERE d.state IN ('READY', 'RETRY_WAIT')
              AND d.next_action_at <= clock_timestamp()
              AND d.priority >= $1
              AND a.audience_finalized_at IS NOT NULL
              AND a.status = 'SENDING'
              AND a.cancel_requested_at IS NULL
            ORDER BY
              d.priority DESC,
              d.next_action_at ASC,
              d.alarm_created_at ASC,
              d.id ASC
            FOR UPDATE OF d SKIP LOCKED
            LIMIT 1
          )
          UPDATE deliveries d
          SET
            state = 'CLAIMED',
            lease_generation = lease_generation + 1,
            lease_token = $2,
            lease_until = clock_timestamp() + ($3::integer * interval '1 millisecond'),
            updated_at = clock_timestamp()
          FROM candidate c, alarms a
          WHERE d.id = c.id AND a.id = d.alarm_id
          RETURNING
            a.id AS alarm_id,
            a.type AS alarm_type,
            a.body AS alarm_body,
            d.id AS delivery_id,
            d.recipient_id,
            d.client_ref,
            d.lease_generation,
            d.lease_token
        `,
        [minimumPriority, leaseToken, leaseDurationMs],
      );
      const row = result.rows[0];
      if (row === undefined) {
        return null;
      }

      return {
        alarmId: row.alarm_id,
        alarmType: row.alarm_type,
        alarmBody: row.alarm_body,
        deliveryId: row.delivery_id,
        recipientId: row.recipient_id,
        clientRef: row.client_ref,
        leaseGeneration: row.lease_generation,
        leaseToken: row.lease_token,
      };
    });
  }

  public startAttempt(claim: DispatchClaim, permit: PacedPermit): Promise<DispatchAttempt | null> {
    return this.transaction(async (client) => {
      const alarm = await this.lockAlarm(client, claim.alarmId);
      const delivery = await this.lockDelivery(client, claim.deliveryId);
      if (!this.ownsClaim(delivery, claim) || delivery.state !== 'CLAIMED') {
        return null;
      }

      const barrier = await client.query<{ open: boolean }>(`
        SELECT resume_not_before <= clock_timestamp() AS open
        FROM dispatch_control
        WHERE control_key = 'default-provider'
      `);
      if (alarm.status !== 'SENDING' || barrier.rows[0]?.open !== true) {
        await this.releaseClaim(client, alarm, delivery);
        return null;
      }

      const attemptId = randomUUID();
      const attemptNo = delivery.attempt_no + 1;
      await client.query(
        `
          INSERT INTO delivery_attempts(
            id, delivery_id, attempt_no, state, lease_generation,
            limiter_reservation_id, budget_consumed, request_phase
          )
          VALUES ($1, $2, $3, 'STARTED', $4, $5, true, 'PREPARED')
        `,
        [attemptId, delivery.id, attemptNo, claim.leaseGeneration, permit.reservationId],
      );
      const updated = await client.query(
        `
          UPDATE deliveries
          SET
            state = 'IN_FLIGHT',
            attempt_no = $2,
            post_budget_count = post_budget_count + 1,
            current_attempt_id = $3,
            updated_at = clock_timestamp()
          WHERE id = $1
            AND state = 'CLAIMED'
            AND lease_generation = $4
            AND lease_token = $5
        `,
        [delivery.id, attemptNo, attemptId, claim.leaseGeneration, claim.leaseToken],
      );
      if (updated.rowCount !== 1) {
        throw new Error('Dispatch STARTED fence changed while locked');
      }

      return {
        ...claim,
        attemptId,
        attemptNo,
        reservationId: permit.reservationId,
      };
    });
  }

  public commitAttempt(attempt: DispatchAttempt): Promise<boolean> {
    return this.transaction(async (client) => {
      const alarm = await this.lockAlarm(client, attempt.alarmId);
      const delivery = await this.lockDelivery(client, attempt.deliveryId);
      if (
        !this.ownsAttempt(delivery, attempt) ||
        delivery.state !== 'IN_FLIGHT' ||
        alarm.status !== 'SENDING' ||
        alarm.cancel_requested_at !== null
      ) {
        return false;
      }

      const barrier = await client.query<{ open: boolean }>(`
        SELECT resume_not_before <= clock_timestamp() AS open
        FROM dispatch_control
        WHERE control_key = 'default-provider'
      `);
      if (barrier.rows[0]?.open !== true) {
        return false;
      }

      const committed = await client.query(
        `
          UPDATE delivery_attempts
          SET
            request_phase = 'COMMITTED',
            adapter_committed_at = clock_timestamp()
          WHERE id = $1
            AND delivery_id = $2
            AND state = 'STARTED'
            AND request_phase = 'PREPARED'
            AND lease_generation = $3
        `,
        [attempt.attemptId, attempt.deliveryId, attempt.leaseGeneration],
      );
      return committed.rowCount === 1;
    });
  }

  public async heartbeat(attempt: DispatchAttempt, leaseDurationMs: number): Promise<boolean> {
    const result = await this.pool
      .query(
        `
          UPDATE deliveries d
          SET
            lease_until = clock_timestamp() + ($2::integer * interval '1 millisecond'),
            updated_at = clock_timestamp()
          WHERE d.id = $1
            AND d.state = 'IN_FLIGHT'
            AND d.current_attempt_id = $3
            AND d.lease_generation = $4
            AND d.lease_token = $5
            AND EXISTS (
              SELECT 1
              FROM delivery_attempts da
              WHERE da.id = d.current_attempt_id
                AND da.delivery_id = d.id
                AND da.state = 'STARTED'
                AND da.request_phase = 'COMMITTED'
                AND da.lease_generation = d.lease_generation
            )
        `,
        [
          attempt.deliveryId,
          leaseDurationMs,
          attempt.attemptId,
          attempt.leaseGeneration,
          attempt.leaseToken,
        ],
      )
      .catch((error: unknown) => {
        throw new PersistenceUnavailableError(error);
      });
    return result.rowCount === 1;
  }

  public finishNotSent(attempt: DispatchAttempt): Promise<void> {
    return this.transaction(async (client) => {
      const alarm = await this.lockAlarm(client, attempt.alarmId);
      const delivery = await this.lockDelivery(client, attempt.deliveryId);
      if (!this.ownsAttempt(delivery, attempt)) {
        return;
      }

      await client.query(
        `
          UPDATE delivery_attempts
          SET
            state = 'FINISHED_NOT_SENT',
            request_phase = 'FINISHED_NOT_SENT',
            budget_consumed = false,
            result_code = 'SLOT_LATE_OR_ABORTED',
            finished_at = clock_timestamp()
          WHERE id = $1 AND state = 'STARTED'
        `,
        [attempt.attemptId],
      );

      if (alarm.status === 'CANCEL_REQUESTED') {
        await this.updateDeliveryState(client, delivery.id, 'CANCELED', {
          resultCode: 'CANCELED_BEFORE_PROVIDER_COMMIT',
          decrementBudget: true,
          keepCurrentAttempt: false,
        });
        await this.updateAlarmTerminalCounter(client, alarm.id, 0, 0, 1);
        return;
      }

      await this.updateDeliveryState(client, delivery.id, 'READY', {
        resultCode: null,
        decrementBudget: true,
        keepCurrentAttempt: false,
      });
      await client.query("SELECT pg_notify('alarm_work', $1)", [alarm.id]);
    });
  }

  public finish(attempt: DispatchAttempt, result: ProviderSendResult): Promise<void> {
    return this.transaction(async (client) => {
      const alarm = await this.lockAlarm(client, attempt.alarmId);
      const delivery = await this.lockDelivery(client, attempt.deliveryId);
      const ownsAttempt = this.ownsAttempt(delivery, attempt);
      const responseObservationKey = await this.appendResponseEvidence(
        client,
        attempt,
        result,
        ownsAttempt,
      );

      if (!ownsAttempt) {
        let adoptedLateAccepted = false;
        if (result.kind === 'ACCEPTED' && responseObservationKey !== null) {
          adoptedLateAccepted = await this.adoptLateAcceptedRetryEvidence(
            client,
            alarm,
            delivery,
            attempt.attemptId,
            result.messageId,
            responseObservationKey,
          );
        }
        if (!adoptedLateAccepted) {
          await this.recordSupersededCommittedResponse(client, attempt, result);
        }
        return;
      }

      if (!(await this.finishCurrentAttemptAudit(client, attempt, result))) {
        throw new Error('Dispatch result attempt fence changed while locked');
      }

      if (
        result.kind !== 'ACCEPTED' &&
        (await this.hasPendingPositiveHistoryEvidence(client, delivery.id))
      ) {
        await this.deferCommittedResultToReconciliation(client, delivery.id, result);
        return;
      }

      if (result.kind === 'ACCEPTED') {
        await this.updateDeliveryState(client, delivery.id, 'SENT', {
          resultCode: 'ACCEPTED',
          providerMessageId: result.messageId,
          accepted: true,
          keepCurrentAttempt: false,
        });
        await this.updateAlarmTerminalCounter(client, alarm.id, 1, 0, 0);
        return;
      }

      if (result.kind === 'PERMANENT_FAILURE') {
        await this.updateDeliveryState(client, delivery.id, 'FAILED_PERMANENT', {
          resultCode: result.code,
          keepCurrentAttempt: false,
        });
        await this.updateAlarmTerminalCounter(client, alarm.id, 0, 1, 0);
        return;
      }

      if (result.kind === 'AMBIGUOUS') {
        await client.query(
          `
            UPDATE deliveries
            SET
              state = 'UNKNOWN',
              result_code = $2,
              first_unknown_at = COALESCE(first_unknown_at, clock_timestamp()),
              next_action_at = clock_timestamp() + interval '1 second',
              lease_token = NULL,
              lease_until = NULL,
              updated_at = clock_timestamp()
            WHERE id = $1
          `,
          [delivery.id, result.code],
        );
        return;
      }

      if (alarm.status === 'CANCEL_REQUESTED') {
        await this.updateDeliveryState(client, delivery.id, 'CANCELED', {
          resultCode: 'CANCELED_AFTER_DEFINITE_UNSENT',
          decrementBudget: result.kind === 'RATE_LIMITED' || result.kind === 'DEFINITELY_NOT_SENT',
          keepCurrentAttempt: false,
        });
        await this.updateAlarmTerminalCounter(client, alarm.id, 0, 0, 1);
        return;
      }

      if (result.kind === 'DEFINITELY_NOT_SENT') {
        await this.scheduleRetry(client, alarm, delivery, {
          delayMs: fullJitterDelay(
            this.random,
            delivery.attempt_no,
            this.retryBaseMs,
            this.retryCapMs,
          ),
          resultCode: result.code,
          decrementBudget: true,
          incrementRateLimit: false,
        });
        return;
      }

      if (result.kind === 'RATE_LIMITED') {
        const rateLimitExhausted =
          delivery.rate_limit_count + 1 >= 20 || delivery.rate_limit_window_expired;
        if (rateLimitExhausted) {
          await this.updateDeliveryState(client, delivery.id, 'FAILED_RETRY_EXHAUSTED', {
            resultCode: 'RATE_LIMIT_EXHAUSTED',
            decrementBudget: true,
            incrementRateLimit: true,
            keepCurrentAttempt: false,
          });
          await this.updateAlarmTerminalCounter(client, alarm.id, 0, 1, 0);
          return;
        }

        await this.scheduleRetry(client, alarm, delivery, {
          delayMs:
            result.retryAfterMs +
            fullJitterDelay(this.random, 1, this.rateLimitJitterCapMs, this.rateLimitJitterCapMs),
          resultCode: result.code,
          decrementBudget: true,
          incrementRateLimit: true,
        });
        return;
      }

      if (delivery.post_budget_count >= 5) {
        await this.updateDeliveryState(client, delivery.id, 'FAILED_RETRY_EXHAUSTED', {
          resultCode: 'POST_BUDGET_EXHAUSTED',
          keepCurrentAttempt: false,
        });
        await this.updateAlarmTerminalCounter(client, alarm.id, 0, 1, 0);
        return;
      }

      await this.scheduleRetry(client, alarm, delivery, {
        delayMs: fullJitterDelay(
          this.random,
          delivery.post_budget_count,
          this.retryBaseMs,
          this.retryCapMs,
        ),
        resultCode: result.code,
        decrementBudget: false,
        incrementRateLimit: false,
      });
    });
  }

  private async scheduleRetry(
    client: PoolClient,
    alarm: LockedAlarmRow,
    delivery: LockedDeliveryRow,
    options: {
      readonly delayMs: number;
      readonly resultCode: string;
      readonly decrementBudget: boolean;
      readonly incrementRateLimit: boolean;
    },
  ): Promise<void> {
    await client.query(
      `
        UPDATE deliveries
        SET
          state = 'RETRY_WAIT',
          priority = $2,
          next_action_at = clock_timestamp() + ($3::integer * interval '1 millisecond'),
          result_code = $4,
          post_budget_count = post_budget_count - $5,
          rate_limit_count = rate_limit_count + $6,
          first_rate_limited_at = CASE
            WHEN $6 = 1 THEN COALESCE(first_rate_limited_at, clock_timestamp())
            ELSE first_rate_limited_at
          END,
          current_attempt_id = NULL,
          lease_token = NULL,
          lease_until = NULL,
          updated_at = clock_timestamp()
        WHERE id = $1
      `,
      [
        delivery.id,
        alarm.type === 'EMERGENCY' ? 100 : 20,
        Math.max(0, options.delayMs),
        options.resultCode,
        options.decrementBudget ? 1 : 0,
        options.incrementRateLimit ? 1 : 0,
      ],
    );
    await client.query("SELECT pg_notify('alarm_work', $1)", [alarm.id]);
  }

  private async appendResponseEvidence(
    client: PoolClient,
    attempt: DispatchAttempt,
    result: ProviderSendResult,
    consumedByCurrentAttempt: boolean,
  ): Promise<string | null> {
    const entries = evidenceFor(result, attempt.recipientId);
    if (entries.length === 0) {
      return null;
    }

    const canonical = JSON.stringify(entries);
    const payloadHash = createHash('sha256').update(canonical).digest('hex');
    const observationKey = createHash('sha256')
      .update(`${attempt.deliveryId}|RESPONSE|${attempt.attemptId}|${payloadHash}`)
      .digest('hex');
    await client.query(
      `
        INSERT INTO delivery_evidence(
          id, delivery_id, attempt_id, source, observation_key,
          payload_hash, entries, entry_count, decision, consumed_at
        )
        VALUES (
          $1, $2, $3, 'RESPONSE', $4, $5, $6::jsonb, $7,
          CASE WHEN $8 THEN 'DIRECT_ACCEPTED' ELSE NULL END,
          CASE WHEN $8 THEN clock_timestamp() ELSE NULL END
        )
        ON CONFLICT (delivery_id, observation_key) DO NOTHING
      `,
      [
        randomUUID(),
        attempt.deliveryId,
        attempt.attemptId,
        observationKey,
        payloadHash,
        canonical,
        entries.length,
        consumedByCurrentAttempt,
      ],
    );
    return observationKey;
  }

  private async adoptLateAcceptedRetryEvidence(
    client: PoolClient,
    alarm: LockedAlarmRow,
    delivery: LockedDeliveryRow,
    sourceAttemptId: string,
    providerMessageId: string,
    observationKey: string,
  ): Promise<boolean> {
    // A definite-unsent result can expose a retry before a concurrent 202 acquires this row lock.
    // Leaving that positive evidence for an asynchronous scanner would reopen a duplicate-POST window.
    const dispatchableWithoutAttempt =
      ['READY', 'RETRY_WAIT', 'CLAIMED'].includes(delivery.state) &&
      delivery.current_attempt_id === null;
    const supersededInFlight =
      delivery.state === 'IN_FLIGHT' &&
      delivery.current_attempt_id !== null &&
      delivery.current_attempt_id !== sourceAttemptId;
    if (!dispatchableWithoutAttempt && !supersededInFlight) {
      return false;
    }

    let refundedBudget = 0;
    if (supersededInFlight) {
      const currentAttempt = await this.lockAttempt(
        client,
        delivery.id,
        delivery.current_attempt_id as string,
      );
      if (currentAttempt.lease_generation !== delivery.lease_generation) {
        throw new Error('Current retry attempt generation changed while Delivery was locked');
      }

      if (currentAttempt.state !== 'STARTED') {
        throw new Error('Current retry attempt was already closed while Delivery was IN_FLIGHT');
      }
      if (currentAttempt.request_phase === 'PREPARED') {
        const closed = await client.query(
          `
            UPDATE delivery_attempts
            SET
              state = 'FINISHED_NOT_SENT',
              request_phase = 'FINISHED_NOT_SENT',
              budget_consumed = false,
              result_code = 'SUPERSEDED_BEFORE_PROVIDER_COMMIT',
              finished_at = clock_timestamp()
            WHERE id = $1
              AND delivery_id = $2
              AND state = 'STARTED'
              AND request_phase = 'PREPARED'
              AND lease_generation = $3
          `,
          [currentAttempt.id, delivery.id, delivery.lease_generation],
        );
        if (closed.rowCount !== 1) {
          throw new Error('Prepared retry attempt fence changed while locked');
        }
        refundedBudget = 1;
      } else if (currentAttempt.request_phase === 'COMMITTED') {
        const closed = await client.query(
          `
            UPDATE delivery_attempts
            SET
              state = 'FINISHED',
              request_phase = 'FINISHED',
              result_code = 'SUPERSEDED_COMMITTED_BY_LATE_ACCEPTED',
              transport_class = 'SUPERSEDED_COMMITTED_OUTCOME_UNOBSERVED',
              finished_at = clock_timestamp()
            WHERE id = $1
              AND delivery_id = $2
              AND state = 'STARTED'
              AND request_phase = 'COMMITTED'
              AND lease_generation = $3
          `,
          [currentAttempt.id, delivery.id, delivery.lease_generation],
        );
        if (closed.rowCount !== 1) {
          throw new Error('Committed retry attempt fence changed while locked');
        }
      } else {
        throw new Error('Current retry attempt has an invalid active request phase');
      }
    }

    const adopted = await client.query(
      `
        UPDATE deliveries
        SET
          state = 'SENT',
          result_code = 'LATE_RESPONSE_ACCEPTED',
          provider_message_id = $2,
          accepted_at = COALESCE(accepted_at, clock_timestamp()),
          post_budget_count = post_budget_count - $6,
          current_attempt_id = NULL,
          lease_token = NULL,
          lease_until = NULL,
          updated_at = clock_timestamp()
        WHERE id = $1
          AND state = $3
          AND lease_generation = $4
          AND current_attempt_id IS NOT DISTINCT FROM $5::uuid
          AND post_budget_count >= $6
      `,
      [
        delivery.id,
        providerMessageId,
        delivery.state,
        delivery.lease_generation,
        delivery.current_attempt_id,
        refundedBudget,
      ],
    );
    if (adopted.rowCount !== 1) {
      throw new Error('Late accepted retry fence changed while locked');
    }

    const consumed = await client.query(
      `
        UPDATE delivery_evidence
        SET
          decision = 'LATE_ACCEPTED_SUPERSEDES_RETRY',
          consumed_at = clock_timestamp()
        WHERE delivery_id = $1
          AND observation_key = $2
          AND consumed_at IS NULL
      `,
      [delivery.id, observationKey],
    );
    if (consumed.rowCount !== 1) {
      throw new Error('Late accepted retry evidence was not consumable');
    }

    await this.updateAlarmTerminalCounter(client, alarm.id, 1, 0, 0);
    return true;
  }

  private async finishCurrentAttemptAudit(
    client: PoolClient,
    attempt: DispatchAttempt,
    result: ProviderSendResult,
  ): Promise<boolean> {
    const finished = await client.query(
      `
        UPDATE delivery_attempts
        SET
          state = CASE WHEN $6::boolean THEN 'FINISHED_NOT_SENT' ELSE 'FINISHED' END,
          request_phase = CASE WHEN $6::boolean THEN 'FINISHED_NOT_SENT' ELSE 'FINISHED' END,
          http_status = $2,
          result_code = $3,
          provider_message_id = $4,
          response_received_at = CASE
            WHEN $6::boolean THEN NULL
            ELSE clock_timestamp()
          END,
          transport_class = $5,
          budget_consumed = CASE WHEN $7::boolean THEN false ELSE budget_consumed END,
          finished_at = clock_timestamp()
        WHERE id = $1
          AND delivery_id = $8
          AND state = 'STARTED'
          AND request_phase = 'COMMITTED'
          AND lease_generation = $9
      `,
      [
        attempt.attemptId,
        result.kind === 'ACCEPTED' ? 202 : 'httpStatus' in result ? result.httpStatus : null,
        result.kind === 'ACCEPTED' ? 'ACCEPTED' : result.code,
        result.kind === 'ACCEPTED' ? result.messageId : null,
        result.kind,
        result.kind === 'DEFINITELY_NOT_SENT',
        result.kind === 'RATE_LIMITED' || result.kind === 'DEFINITELY_NOT_SENT',
        attempt.deliveryId,
        attempt.leaseGeneration,
      ],
    );
    return finished.rowCount === 1;
  }

  private async hasPendingPositiveHistoryEvidence(
    client: PoolClient,
    deliveryId: string,
  ): Promise<boolean> {
    const result = await client.query<{ exists: boolean }>(
      `
        SELECT EXISTS (
          SELECT 1
          FROM delivery_evidence
          WHERE delivery_id = $1
            AND source = 'HISTORY'
            AND consumed_at IS NULL
            AND entry_count > 0
        ) AS exists
      `,
      [deliveryId],
    );
    return result.rows[0]?.exists === true;
  }

  private async deferCommittedResultToReconciliation(
    client: PoolClient,
    deliveryId: string,
    result: Exclude<ProviderSendResult, { readonly kind: 'ACCEPTED' }>,
  ): Promise<void> {
    const refundPostBudget =
      result.kind === 'RATE_LIMITED' || result.kind === 'DEFINITELY_NOT_SENT';
    const incrementRateLimit = result.kind === 'RATE_LIMITED';
    const deferred = await client.query(
      `
        UPDATE deliveries
        SET
          state = 'UNKNOWN',
          result_code = 'HISTORY_EVIDENCE_PENDING_AFTER_COMMITTED_RETRY',
          first_unknown_at = COALESCE(first_unknown_at, clock_timestamp()),
          next_action_at = clock_timestamp(),
          post_budget_count = post_budget_count - $2,
          rate_limit_count = rate_limit_count + $3,
          first_rate_limited_at = CASE
            WHEN $3 = 1 THEN COALESCE(first_rate_limited_at, clock_timestamp())
            ELSE first_rate_limited_at
          END,
          lease_token = NULL,
          lease_until = NULL,
          updated_at = clock_timestamp()
        WHERE id = $1
          AND state = 'IN_FLIGHT'
          AND post_budget_count >= $2
      `,
      [deliveryId, refundPostBudget ? 1 : 0, incrementRateLimit ? 1 : 0],
    );
    if (deferred.rowCount !== 1) {
      throw new Error('Committed result reconciliation deferral fence changed while locked');
    }
  }

  private async recordSupersededCommittedResponse(
    client: PoolClient,
    attempt: DispatchAttempt,
    result: ProviderSendResult,
  ): Promise<boolean> {
    const recorded = await client.query(
      `
        UPDATE delivery_attempts
        SET
          state = CASE WHEN $6::boolean THEN 'FINISHED_NOT_SENT' ELSE state END,
          request_phase = CASE WHEN $6::boolean THEN 'FINISHED_NOT_SENT' ELSE request_phase END,
          http_status = $2,
          result_code = $3,
          provider_message_id = $4,
          response_received_at = CASE
            WHEN $6::boolean THEN NULL
            ELSE clock_timestamp()
          END,
          transport_class = CASE
            WHEN $6::boolean THEN 'DEFINITELY_NOT_SENT'
            ELSE 'SUPERSEDED_' || $5::text
          END,
          budget_consumed = CASE WHEN $7::boolean THEN false ELSE budget_consumed END
        WHERE id = $1
          AND delivery_id = $8
          AND state = 'FINISHED'
          AND request_phase = 'FINISHED'
          AND result_code = 'SUPERSEDED_COMMITTED_BY_LATE_ACCEPTED'
          AND transport_class = 'SUPERSEDED_COMMITTED_OUTCOME_UNOBSERVED'
          AND lease_generation = $9
      `,
      [
        attempt.attemptId,
        result.kind === 'ACCEPTED' ? 202 : 'httpStatus' in result ? result.httpStatus : null,
        result.kind === 'ACCEPTED' ? 'ACCEPTED' : result.code,
        result.kind === 'ACCEPTED' ? result.messageId : null,
        result.kind,
        result.kind === 'DEFINITELY_NOT_SENT',
        result.kind === 'RATE_LIMITED' || result.kind === 'DEFINITELY_NOT_SENT',
        attempt.deliveryId,
        attempt.leaseGeneration,
      ],
    );
    if (
      recorded.rowCount === 1 &&
      (result.kind === 'RATE_LIMITED' || result.kind === 'DEFINITELY_NOT_SENT')
    ) {
      const incrementRateLimit = result.kind === 'RATE_LIMITED' ? 1 : 0;
      await client.query(
        `
          UPDATE deliveries
          SET
            post_budget_count = post_budget_count - 1,
            rate_limit_count = rate_limit_count + $2,
            first_rate_limited_at = CASE
              WHEN $2 = 1 THEN COALESCE(first_rate_limited_at, clock_timestamp())
              ELSE first_rate_limited_at
            END,
            updated_at = clock_timestamp()
          WHERE id = $1
            AND state = 'SENT'
            AND result_code = 'LATE_RESPONSE_ACCEPTED'
            AND post_budget_count > 0
        `,
        [attempt.deliveryId, incrementRateLimit],
      );
    }
    return recorded.rowCount === 1;
  }

  private async releaseClaim(
    client: PoolClient,
    alarm: LockedAlarmRow,
    delivery: LockedDeliveryRow,
  ): Promise<void> {
    if (alarm.status === 'CANCEL_REQUESTED') {
      await this.updateDeliveryState(client, delivery.id, 'CANCELED', {
        resultCode: 'CANCELED_BEFORE_START',
        keepCurrentAttempt: false,
      });
      await this.updateAlarmTerminalCounter(client, alarm.id, 0, 0, 1);
      return;
    }

    await this.updateDeliveryState(client, delivery.id, 'READY', {
      resultCode: null,
      keepCurrentAttempt: false,
    });
  }

  private async updateDeliveryState(
    client: PoolClient,
    deliveryId: string,
    state: string,
    options: {
      readonly resultCode: string | null;
      readonly providerMessageId?: string;
      readonly accepted?: boolean;
      readonly decrementBudget?: boolean;
      readonly incrementRateLimit?: boolean;
      readonly keepCurrentAttempt: boolean;
    },
  ): Promise<void> {
    await client.query(
      `
        UPDATE deliveries
        SET
          state = $2,
          result_code = $3,
          provider_message_id = COALESCE($4, provider_message_id),
          accepted_at = CASE WHEN $5 THEN clock_timestamp() ELSE accepted_at END,
          post_budget_count = post_budget_count - $6,
          rate_limit_count = rate_limit_count + $7,
          first_rate_limited_at = CASE
            WHEN $7 = 1 THEN COALESCE(first_rate_limited_at, clock_timestamp())
            ELSE first_rate_limited_at
          END,
          current_attempt_id = CASE WHEN $8 THEN current_attempt_id ELSE NULL END,
          lease_token = NULL,
          lease_until = NULL,
          updated_at = clock_timestamp()
        WHERE id = $1
      `,
      [
        deliveryId,
        state,
        options.resultCode,
        options.providerMessageId ?? null,
        options.accepted ?? false,
        options.decrementBudget ? 1 : 0,
        options.incrementRateLimit ? 1 : 0,
        options.keepCurrentAttempt,
      ],
    );
  }

  private async updateAlarmTerminalCounter(
    client: PoolClient,
    alarmId: string,
    sentDelta: number,
    failedDelta: number,
    canceledDelta: number,
  ): Promise<void> {
    await client.query(
      `
        UPDATE alarms
        SET
          sent = sent + $2,
          failed = failed + $3,
          canceled = canceled + $4,
          active = active - 1,
          status = CASE
            WHEN active - 1 > 0 THEN
              CASE WHEN cancel_requested_at IS NULL THEN 'SENDING' ELSE 'CANCEL_REQUESTED' END
            WHEN unresolved > 0 OR duplicate_count > 0 OR incident_count > 0 THEN
              'ATTENTION_REQUIRED'
            WHEN cancel_requested_at IS NOT NULL THEN 'CANCELED'
            WHEN failed + $3 = 0 THEN 'COMPLETED'
            WHEN sent + $2 > 0 AND failed + $3 > 0 THEN 'PARTIALLY_FAILED'
            ELSE 'FAILED'
          END,
          finished_at = CASE
            WHEN active - 1 = 0 THEN COALESCE(finished_at, clock_timestamp())
            ELSE finished_at
          END,
          updated_at = clock_timestamp(),
          version = version + 1
        WHERE id = $1 AND active > 0
      `,
      [alarmId, sentDelta, failedDelta, canceledDelta],
    );
  }

  private async lockAlarm(client: PoolClient, alarmId: string): Promise<LockedAlarmRow> {
    const result = await client.query<LockedAlarmRow>(
      'SELECT id, type, status, cancel_requested_at FROM alarms WHERE id = $1 FOR UPDATE',
      [alarmId],
    );
    const row = result.rows[0];
    if (row === undefined) {
      throw new Error(`Dispatch Alarm disappeared: ${alarmId}`);
    }
    return row;
  }

  private async lockDelivery(client: PoolClient, deliveryId: string): Promise<LockedDeliveryRow> {
    const result = await client.query<LockedDeliveryRow>(
      `
        SELECT
          *,
          first_rate_limited_at IS NOT NULL
            AND first_rate_limited_at <= clock_timestamp() - interval '5 minutes'
            AS rate_limit_window_expired
        FROM deliveries
        WHERE id = $1
        FOR UPDATE
      `,
      [deliveryId],
    );
    const row = result.rows[0];
    if (row === undefined) {
      throw new Error(`Dispatch Delivery disappeared: ${deliveryId}`);
    }
    return row;
  }

  private async lockAttempt(
    client: PoolClient,
    deliveryId: string,
    attemptId: string,
  ): Promise<LockedAttemptRow> {
    const result = await client.query<LockedAttemptRow>(
      `
        SELECT id, state, request_phase, lease_generation
        FROM delivery_attempts
        WHERE delivery_id = $1 AND id = $2
        FOR UPDATE
      `,
      [deliveryId, attemptId],
    );
    const row = result.rows[0];
    if (row === undefined) {
      throw new Error(`Dispatch attempt disappeared: ${attemptId}`);
    }
    return row;
  }

  private ownsClaim(delivery: LockedDeliveryRow, claim: DispatchClaim): boolean {
    return (
      delivery.id === claim.deliveryId &&
      delivery.alarm_id === claim.alarmId &&
      delivery.lease_generation === claim.leaseGeneration &&
      delivery.lease_token === claim.leaseToken
    );
  }

  private ownsAttempt(delivery: LockedDeliveryRow, attempt: DispatchAttempt): boolean {
    return (
      this.ownsClaim(delivery, attempt) &&
      delivery.state === 'IN_FLIGHT' &&
      delivery.current_attempt_id === attempt.attemptId
    );
  }

  private async transaction<T>(work: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.pool.connect().catch((error: unknown) => {
      throw new PersistenceUnavailableError(error);
    });
    const detachErrorHandler = attachPostgresClientErrorHandler(client, {
      component: 'dispatch-repository',
    });
    let reusable = false;
    try {
      await client.query('BEGIN');
      const result = await work(client);
      await client.query('COMMIT');
      reusable = true;
      return result;
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      if (error instanceof PersistenceUnavailableError) {
        throw error;
      }
      throw new PersistenceUnavailableError(error);
    } finally {
      detachErrorHandler();
      if (reusable) {
        client.release();
      } else {
        // A query timeout or connection error can leave transaction outcome/protocol state
        // uncertain even when a best-effort ROLLBACK also appears to settle. Never return that
        // socket to the dispatch pool; dropping the session also releases all session locks.
        client.release(true);
      }
    }
  }
}
