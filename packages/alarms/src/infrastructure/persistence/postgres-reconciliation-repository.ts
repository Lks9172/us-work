import { createHash, randomUUID } from 'node:crypto';

import type { Pool, PoolClient } from 'pg';

import { PersistenceUnavailableError } from '../../application/alarm-errors';
import type {
  HistoryLookupResult,
  ReconciliationClaim,
  ReconciliationRepository,
} from '../../application/reconciliation';
import { evaluateEvidence, type ProviderEvidenceEntry } from '../../domain/delivery-evidence';
import { attachPostgresClientErrorHandler } from './postgres-pool-observability';

export const HISTORY_VISIBILITY_PROFILES = ['OPAQUE', 'AUTHORITATIVE_EMPTY_AFTER_GRACE'] as const;
export type HistoryVisibilityProfile = (typeof HISTORY_VISIBILITY_PROFILES)[number];

export interface PostgresReconciliationRepositoryOptions {
  /**
   * OPAQUE is the production-safe default. The authoritative profile may only be selected when
   * the provider contract explicitly guarantees that an empty lookup after the configured grace
   * proves the request produced no side effect.
   */
  readonly historyVisibilityProfile?: HistoryVisibilityProfile;
  readonly authoritativeEmptyGraceMs?: number;
  readonly maxFastChecks?: number;
  readonly maxFastElapsedMs?: number;
}

interface ClaimRow {
  readonly alarm_id: string;
  readonly delivery_id: string;
  readonly recipient_id: string;
  readonly client_ref: string;
  readonly source_state: 'UNKNOWN' | 'NEEDS_REVIEW';
  readonly reconcile_generation: number;
  readonly reconcile_token: string;
}

interface LockedDeliveryRow {
  readonly id: string;
  readonly alarm_id: string;
  readonly recipient_id: string;
  readonly state: string;
  readonly blocking_conflict_code: string | null;
  readonly post_budget_count: number;
  readonly reconcile_count: number;
  readonly reconcile_generation: number;
  readonly reconcile_token: string | null;
  readonly current_attempt_id: string | null;
  readonly lease_generation: number;
  readonly lease_token: string | null;
  readonly authoritative_empty_eligible: boolean;
  readonly fast_deadline_reached: boolean;
}

interface LockedDispatchAttemptRow {
  readonly id: string;
  readonly state: 'STARTED' | 'FINISHED_NOT_SENT' | 'FINISHED';
  readonly request_phase: 'PREPARED' | 'COMMITTED' | 'FINISHED_NOT_SENT' | 'FINISHED';
  readonly lease_generation: number;
}

interface LockedAlarmRow {
  readonly id: string;
  readonly type: 'BULK' | 'EMERGENCY';
  readonly cancel_requested_at: Date | null;
}

interface EvidenceRow {
  readonly id: string;
  readonly entries: unknown;
}

function canonicalEntries(
  entries: readonly ProviderEvidenceEntry[],
): readonly ProviderEvidenceEntry[] {
  return [...entries].sort((left, right) => {
    const idOrder = left.messageId.localeCompare(right.messageId);
    if (idOrder !== 0) {
      return idOrder;
    }
    const recipientOrder = left.recipientId.localeCompare(right.recipientId);
    if (recipientOrder !== 0) {
      return recipientOrder;
    }
    return (left.sentAt ?? '').localeCompare(right.sentAt ?? '');
  });
}

function parseStoredEntries(value: unknown): ProviderEvidenceEntry[] {
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

export class PostgresReconciliationRepository implements ReconciliationRepository {
  private readonly historyVisibilityProfile: HistoryVisibilityProfile;
  private readonly authoritativeEmptyGraceMs: number;
  private readonly maxFastChecks: number;
  private readonly maxFastElapsedMs: number;

  public constructor(
    private readonly pool: Pool,
    options: PostgresReconciliationRepositoryOptions = {},
  ) {
    this.historyVisibilityProfile = options.historyVisibilityProfile ?? 'OPAQUE';
    this.authoritativeEmptyGraceMs = options.authoritativeEmptyGraceMs ?? 30_000;
    this.maxFastChecks = options.maxFastChecks ?? 12;
    this.maxFastElapsedMs = options.maxFastElapsedMs ?? 60_000;
    if (!HISTORY_VISIBILITY_PROFILES.includes(this.historyVisibilityProfile)) {
      throw new Error('Unsupported provider history visibility profile');
    }
    if (
      !Number.isSafeInteger(this.authoritativeEmptyGraceMs) ||
      this.authoritativeEmptyGraceMs < 0
    ) {
      throw new Error('authoritativeEmptyGraceMs must be a non-negative safe integer');
    }
    if (!Number.isSafeInteger(this.maxFastChecks) || this.maxFastChecks < 1) {
      throw new Error('maxFastChecks must be a positive safe integer');
    }
    if (!Number.isSafeInteger(this.maxFastElapsedMs) || this.maxFastElapsedMs < 0) {
      throw new Error('maxFastElapsedMs must be a non-negative safe integer');
    }
    if (
      this.historyVisibilityProfile === 'AUTHORITATIVE_EMPTY_AFTER_GRACE' &&
      this.authoritativeEmptyGraceMs > this.maxFastElapsedMs
    ) {
      throw new Error('authoritative empty grace must not exceed the fast reconciliation deadline');
    }
  }

  public claimNext(leaseDurationMs: number): Promise<ReconciliationClaim | null> {
    return this.transaction(async (client) => {
      const token = randomUUID();
      const result = await client.query<ClaimRow>(
        `
          WITH candidate AS (
            SELECT d.id, d.state
            FROM deliveries d
            WHERE (
              d.state = 'UNKNOWN' AND d.next_action_at <= clock_timestamp()
            ) OR (
              d.state = 'RECONCILING' AND d.reconcile_until <= clock_timestamp()
            ) OR (
              d.state = 'NEEDS_REVIEW'
              AND d.next_review_at <= clock_timestamp()
              AND d.review_until > clock_timestamp()
              AND (d.reconcile_until IS NULL OR d.reconcile_until <= clock_timestamp())
            )
            ORDER BY
              CASE WHEN d.state IN ('UNKNOWN', 'RECONCILING') THEN 0 ELSE 1 END,
              COALESCE(d.next_review_at, d.next_action_at),
              d.id
            FOR UPDATE SKIP LOCKED
            LIMIT 1
          )
          UPDATE deliveries d
          SET
            state = CASE WHEN c.state = 'NEEDS_REVIEW' THEN 'NEEDS_REVIEW' ELSE 'RECONCILING' END,
            reconcile_generation = reconcile_generation + 1,
            reconcile_token = $1,
            reconcile_until = clock_timestamp() + ($2::integer * interval '1 millisecond'),
            updated_at = clock_timestamp()
          FROM candidate c
          WHERE d.id = c.id
          RETURNING
            d.alarm_id,
            d.id AS delivery_id,
            d.recipient_id,
            d.client_ref,
            CASE WHEN c.state = 'NEEDS_REVIEW' THEN 'NEEDS_REVIEW' ELSE 'UNKNOWN' END AS source_state,
            d.reconcile_generation,
            d.reconcile_token
        `,
        [token, leaseDurationMs],
      );
      const row = result.rows[0];
      if (row === undefined) {
        return null;
      }
      return {
        alarmId: row.alarm_id,
        deliveryId: row.delivery_id,
        recipientId: row.recipient_id,
        clientRef: row.client_ref,
        sourceState: row.source_state,
        reconcileGeneration: row.reconcile_generation,
        reconcileToken: row.reconcile_token,
      };
    });
  }

  public record(claim: ReconciliationClaim, result: HistoryLookupResult): Promise<void> {
    return this.transaction(async (client) => {
      const alarmResult = await client.query<LockedAlarmRow>(
        'SELECT id, type, cancel_requested_at FROM alarms WHERE id = $1 FOR UPDATE',
        [claim.alarmId],
      );
      const alarm = alarmResult.rows[0];
      if (alarm === undefined) {
        throw new Error(`Reconciliation Alarm disappeared: ${claim.alarmId}`);
      }
      const delivery = await this.lockDelivery(client, claim.deliveryId);

      if (result.kind === 'OBSERVATION') {
        await this.appendObservation(client, claim, result.entries);
      }

      const evidence = await client.query<EvidenceRow>(
        `
          SELECT id, entries
          FROM delivery_evidence
          WHERE delivery_id = $1 AND consumed_at IS NULL
          ORDER BY observed_at, id
          FOR UPDATE
        `,
        [claim.deliveryId],
      );
      const entries = evidence.rows.flatMap(({ entries: stored }) => parseStoredEntries(stored));
      const decision = evaluateEvidence(entries, delivery.recipient_id);

      if (!this.owns(delivery, claim)) {
        await this.adoptLatePositiveEvidence(client, alarm, delivery, evidence.rows, decision);
        return;
      }

      if (delivery.blocking_conflict_code !== null) {
        await this.finishReconciledAttempt(client, delivery, delivery.blocking_conflict_code);
        await client.query(
          `
            UPDATE deliveries
            SET
              state = 'NEEDS_REVIEW',
              reconcile_count = reconcile_count + 1,
              result_code = blocking_conflict_code,
              next_review_at = clock_timestamp() + interval '15 minutes',
              review_until = COALESCE(
                review_until,
                clock_timestamp() + interval '24 hours'
              ),
              reconcile_token = NULL,
              reconcile_until = NULL,
              current_attempt_id = NULL,
              updated_at = clock_timestamp()
            WHERE id = $1
          `,
          [claim.deliveryId],
        );
        if (claim.sourceState === 'UNKNOWN') {
          await this.updateAlarm(client, claim.alarmId, {
            activeDelta: -1,
            unresolvedDelta: 1,
          });
        }
        await this.consume(client, evidence.rows, 'BLOCKING_CONFLICT_STICKY');
        return;
      }

      if (decision.kind !== 'MATCH') {
        const conflictCode =
          decision.kind === 'RECIPIENT_MISMATCH'
            ? 'PROVIDER_RECIPIENT_MISMATCH'
            : 'PROVIDER_EVIDENCE_CONFLICT';
        await this.finishReconciledAttempt(client, delivery, conflictCode);
        await client.query(
          `
            UPDATE deliveries
            SET
              state = 'NEEDS_REVIEW',
              blocking_conflict_code = COALESCE(blocking_conflict_code, $2),
              reconcile_count = reconcile_count + 1,
              next_review_at = clock_timestamp() + interval '15 minutes',
              review_until = COALESCE(
                review_until,
                clock_timestamp() + interval '24 hours'
              ),
              reconcile_token = NULL,
              reconcile_until = NULL,
              current_attempt_id = NULL,
              updated_at = clock_timestamp()
            WHERE id = $1
          `,
          [claim.deliveryId, conflictCode],
        );
        if (claim.sourceState === 'UNKNOWN') {
          await this.updateAlarm(client, claim.alarmId, {
            activeDelta: -1,
            unresolvedDelta: 1,
          });
        }
        await this.consume(client, evidence.rows, conflictCode);
        return;
      }

      if (decision.distinctMessageIds.length > 0) {
        const duplicateDelta = decision.distinctMessageIds.length >= 2 ? 1 : 0;
        const reconciliationCode =
          duplicateDelta === 1 ? 'HISTORY_DUPLICATE_CONFIRMED' : 'HISTORY_CONFIRMED';
        await this.finishReconciledAttempt(
          client,
          delivery,
          reconciliationCode,
          decision.distinctMessageIds[0],
        );
        await client.query(
          `
            UPDATE deliveries
            SET
              state = 'SENT',
              provider_message_id = $2,
              provider_sent_at = COALESCE($3::timestamptz, provider_sent_at),
              accepted_at = COALESCE(accepted_at, clock_timestamp()),
              duplicate_detected = duplicate_detected OR $4,
              corrected_at = CASE
                WHEN $5 = 'NEEDS_REVIEW' THEN clock_timestamp()
                ELSE corrected_at
              END,
              result_code = 'HISTORY_CONFIRMED',
              current_attempt_id = NULL,
              reconcile_token = NULL,
              reconcile_until = NULL,
              next_review_at = NULL,
              review_until = NULL,
              updated_at = clock_timestamp()
            WHERE id = $1
          `,
          [
            claim.deliveryId,
            decision.distinctMessageIds[0],
            decision.providerSentAt,
            duplicateDelta === 1,
            claim.sourceState,
          ],
        );
        await this.updateAlarm(client, claim.alarmId, {
          activeDelta: claim.sourceState === 'UNKNOWN' ? -1 : 0,
          unresolvedDelta: claim.sourceState === 'NEEDS_REVIEW' ? -1 : 0,
          sentDelta: 1,
          duplicateDelta,
        });
        await this.consume(
          client,
          evidence.rows,
          duplicateDelta === 1 ? 'HISTORY_DUPLICATE_CONFIRMED' : 'HISTORY_SENT_CONFIRMED',
        );
        return;
      }

      if (
        claim.sourceState === 'UNKNOWN' &&
        result.kind === 'OBSERVATION' &&
        this.historyVisibilityProfile === 'AUTHORITATIVE_EMPTY_AFTER_GRACE' &&
        delivery.authoritative_empty_eligible
      ) {
        await this.settleAuthoritativeEmpty(client, alarm, delivery, evidence.rows);
        return;
      }

      const nextCount = delivery.reconcile_count + 1;
      if (
        claim.sourceState === 'UNKNOWN' &&
        (nextCount >= this.maxFastChecks || delivery.fast_deadline_reached)
      ) {
        const resultCode =
          result.kind === 'UNAVAILABLE' ? result.code : 'HISTORY_EMPTY_NON_AUTHORITATIVE';
        await this.finishReconciledAttempt(client, delivery, resultCode);
        await client.query(
          `
            UPDATE deliveries
            SET
              state = 'NEEDS_REVIEW',
              reconcile_count = $2,
              result_code = $3,
              next_review_at = clock_timestamp() + interval '15 minutes',
              review_until = COALESCE(
                review_until,
                clock_timestamp() + interval '24 hours'
              ),
              reconcile_token = NULL,
              reconcile_until = NULL,
              current_attempt_id = NULL,
              updated_at = clock_timestamp()
            WHERE id = $1
          `,
          [claim.deliveryId, nextCount, resultCode],
        );
        await this.updateAlarm(client, claim.alarmId, {
          activeDelta: -1,
          unresolvedDelta: 1,
        });
      } else if (claim.sourceState === 'UNKNOWN') {
        const delaySeconds = Math.min(60, 2 ** Math.min(nextCount, 6));
        await client.query(
          `
            UPDATE deliveries
            SET
              state = 'UNKNOWN',
              reconcile_count = $2,
              result_code = $3,
              next_action_at = clock_timestamp() + ($4::integer * interval '1 second'),
              reconcile_token = NULL,
              reconcile_until = NULL,
              updated_at = clock_timestamp()
            WHERE id = $1
          `,
          [
            claim.deliveryId,
            nextCount,
            result.kind === 'UNAVAILABLE' ? result.code : 'HISTORY_EMPTY_NON_AUTHORITATIVE',
            delaySeconds,
          ],
        );
      } else {
        await client.query(
          `
            UPDATE deliveries
            SET
              state = 'NEEDS_REVIEW',
              reconcile_count = $2,
              result_code = $3,
              next_review_at = clock_timestamp() + interval '15 minutes',
              reconcile_token = NULL,
              reconcile_until = NULL,
              updated_at = clock_timestamp()
            WHERE id = $1
          `,
          [
            claim.deliveryId,
            nextCount,
            result.kind === 'UNAVAILABLE' ? result.code : 'HISTORY_EMPTY_NON_AUTHORITATIVE',
          ],
        );
      }
      await this.consume(client, evidence.rows, 'HISTORY_NO_MATCH');
    });
  }

  private async appendObservation(
    client: PoolClient,
    claim: ReconciliationClaim,
    entries: readonly ProviderEvidenceEntry[],
  ): Promise<void> {
    const canonical = JSON.stringify(canonicalEntries(entries));
    const payloadHash = createHash('sha256').update(canonical).digest('hex');
    const observationKey = createHash('sha256')
      .update(`${claim.deliveryId}|HISTORY|${claim.clientRef}|${payloadHash}`)
      .digest('hex');
    await client.query(
      `
        INSERT INTO delivery_evidence(
          id, delivery_id, source, observation_key, payload_hash, entries, entry_count
        )
        VALUES ($1, $2, 'HISTORY', $3, $4, $5::jsonb, $6)
        ON CONFLICT (delivery_id, observation_key) DO NOTHING
      `,
      [randomUUID(), claim.deliveryId, observationKey, payloadHash, canonical, entries.length],
    );
  }

  private async consume(
    client: PoolClient,
    evidence: readonly EvidenceRow[],
    decision: string,
  ): Promise<void> {
    if (evidence.length === 0) {
      return;
    }
    await client.query(
      `
        UPDATE delivery_evidence
        SET decision = $2, consumed_at = clock_timestamp()
        WHERE id = ANY($1::uuid[]) AND consumed_at IS NULL
      `,
      [evidence.map(({ id }) => id), decision],
    );
  }

  private async finishReconciledAttempt(
    client: PoolClient,
    delivery: LockedDeliveryRow,
    resultCode: string,
    providerMessageId?: string,
  ): Promise<void> {
    if (delivery.current_attempt_id === null) {
      return;
    }

    await client.query(
      `
        UPDATE delivery_attempts
        SET
          state = 'FINISHED',
          request_phase = 'FINISHED',
          result_code = $2,
          provider_message_id = COALESCE($3, provider_message_id),
          transport_class = 'HISTORY_RECONCILED',
          finished_at = clock_timestamp()
        WHERE id = $1 AND state = 'STARTED'
      `,
      [delivery.current_attempt_id, resultCode, providerMessageId ?? null],
    );
  }

  private async adoptLatePositiveEvidence(
    client: PoolClient,
    alarm: LockedAlarmRow,
    delivery: LockedDeliveryRow,
    evidence: readonly EvidenceRow[],
    decision: ReturnType<typeof evaluateEvidence>,
  ): Promise<boolean> {
    if (
      delivery.alarm_id !== alarm.id ||
      delivery.blocking_conflict_code !== null ||
      decision.kind !== 'MATCH' ||
      decision.distinctMessageIds.length === 0
    ) {
      return false;
    }

    const dispatchingStates = ['READY', 'CLAIMED', 'IN_FLIGHT', 'RETRY_WAIT'];
    if (!dispatchingStates.includes(delivery.state)) {
      return false;
    }

    let refundedBudget = 0;
    if (delivery.state === 'IN_FLIGHT') {
      if (delivery.current_attempt_id === null) {
        throw new Error('IN_FLIGHT Delivery has no current attempt');
      }
      const attempt = await this.lockDispatchAttempt(
        client,
        delivery.id,
        delivery.current_attempt_id,
      );
      if (attempt.lease_generation !== delivery.lease_generation) {
        throw new Error('Current retry attempt generation changed while Delivery was locked');
      }
      if (attempt.state !== 'STARTED') {
        throw new Error('Current retry attempt was already closed while Delivery was IN_FLIGHT');
      }

      if (attempt.request_phase === 'COMMITTED') {
        // commitAttempt(true) is the provider point-of-no-return. The processor may already be
        // entering sender.send(), so closing the attempt here would hide a possible second send.
        // Keep the evidence unconsumed and let the current response/reconciliation settle it.
        return false;
      }
      if (attempt.request_phase === 'PREPARED') {
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
          [attempt.id, delivery.id, delivery.lease_generation],
        );
        if (closed.rowCount !== 1) {
          throw new Error('Prepared retry attempt fence changed while locked');
        }
        refundedBudget = 1;
      } else {
        throw new Error('Current retry attempt has an invalid active request phase');
      }
    } else if (delivery.current_attempt_id !== null) {
      throw new Error(`${delivery.state} Delivery unexpectedly retained a current attempt`);
    }

    const duplicateDelta = decision.distinctMessageIds.length >= 2 ? 1 : 0;
    const adopted = await client.query(
      `
        UPDATE deliveries
        SET
          state = 'SENT',
          provider_message_id = $2,
          provider_sent_at = COALESCE($3::timestamptz, provider_sent_at),
          accepted_at = COALESCE(accepted_at, clock_timestamp()),
          duplicate_detected = duplicate_detected OR $4,
          result_code = 'HISTORY_CONFIRMED',
          post_budget_count = post_budget_count - $5,
          current_attempt_id = NULL,
          reconcile_token = NULL,
          reconcile_until = NULL,
          next_review_at = NULL,
          review_until = NULL,
          lease_token = NULL,
          lease_until = NULL,
          updated_at = clock_timestamp()
        WHERE id = $1
          AND state = $6
          AND lease_generation = $7
          AND lease_token IS NOT DISTINCT FROM $8::uuid
          AND reconcile_generation = $9
          AND reconcile_token IS NOT DISTINCT FROM $10::uuid
          AND current_attempt_id IS NOT DISTINCT FROM $11::uuid
          AND post_budget_count >= $5
      `,
      [
        delivery.id,
        decision.distinctMessageIds[0],
        decision.providerSentAt,
        duplicateDelta === 1,
        refundedBudget,
        delivery.state,
        delivery.lease_generation,
        delivery.lease_token,
        delivery.reconcile_generation,
        delivery.reconcile_token,
        delivery.current_attempt_id,
      ],
    );
    if (adopted.rowCount !== 1) {
      throw new Error('Late history evidence fence changed while Delivery was locked');
    }

    await this.updateAlarm(client, alarm.id, {
      activeDelta: -1,
      sentDelta: 1,
      duplicateDelta,
    });
    await this.consume(
      client,
      evidence,
      duplicateDelta === 1 ? 'HISTORY_DUPLICATE_CONFIRMED' : 'HISTORY_SENT_CONFIRMED',
    );
    return true;
  }

  private async settleAuthoritativeEmpty(
    client: PoolClient,
    alarm: LockedAlarmRow,
    delivery: LockedDeliveryRow,
    evidence: readonly EvidenceRow[],
  ): Promise<void> {
    if (alarm.cancel_requested_at !== null) {
      await this.finishReconciledAttempt(client, delivery, 'HISTORY_AUTHORITATIVE_EMPTY_CANCELED');
      await client.query(
        `
          UPDATE deliveries
          SET
            state = 'CANCELED',
            result_code = 'HISTORY_AUTHORITATIVE_EMPTY_CANCELED',
            current_attempt_id = NULL,
            reconcile_token = NULL,
            reconcile_until = NULL,
            lease_token = NULL,
            lease_until = NULL,
            updated_at = clock_timestamp()
          WHERE id = $1
        `,
        [delivery.id],
      );
      await this.updateAlarm(client, alarm.id, {
        activeDelta: -1,
        canceledDelta: 1,
      });
      await this.consume(client, evidence, 'HISTORY_AUTHORITATIVE_EMPTY_CANCELED');
      return;
    }

    if (delivery.post_budget_count < 5) {
      await this.finishReconciledAttempt(client, delivery, 'HISTORY_AUTHORITATIVE_EMPTY_RETRY');
      await client.query(
        `
          UPDATE deliveries
          SET
            state = 'RETRY_WAIT',
            priority = $2,
            result_code = 'HISTORY_AUTHORITATIVE_EMPTY_RETRY',
            next_action_at = clock_timestamp(),
            current_attempt_id = NULL,
            reconcile_token = NULL,
            reconcile_until = NULL,
            lease_token = NULL,
            lease_until = NULL,
            updated_at = clock_timestamp()
          WHERE id = $1
        `,
        [delivery.id, alarm.type === 'EMERGENCY' ? 100 : 20],
      );
      await this.consume(client, evidence, 'HISTORY_AUTHORITATIVE_EMPTY_RETRY');
      await client.query("SELECT pg_notify('alarm_work', $1)", [alarm.id]);
      return;
    }

    await this.finishReconciledAttempt(
      client,
      delivery,
      'POST_BUDGET_EXHAUSTED_AFTER_RECONCILE_EMPTY',
    );
    await client.query(
      `
        UPDATE deliveries
        SET
          state = 'FAILED_RETRY_EXHAUSTED',
          result_code = 'POST_BUDGET_EXHAUSTED_AFTER_RECONCILE_EMPTY',
          current_attempt_id = NULL,
          reconcile_token = NULL,
          reconcile_until = NULL,
          lease_token = NULL,
          lease_until = NULL,
          updated_at = clock_timestamp()
        WHERE id = $1
      `,
      [delivery.id],
    );
    await this.updateAlarm(client, alarm.id, {
      activeDelta: -1,
      failedDelta: 1,
    });
    await this.consume(client, evidence, 'POST_BUDGET_EXHAUSTED_AFTER_RECONCILE_EMPTY');
  }

  private async updateAlarm(
    client: PoolClient,
    alarmId: string,
    deltas: {
      readonly activeDelta?: number;
      readonly unresolvedDelta?: number;
      readonly sentDelta?: number;
      readonly failedDelta?: number;
      readonly canceledDelta?: number;
      readonly duplicateDelta?: number;
    },
  ): Promise<void> {
    const activeDelta = deltas.activeDelta ?? 0;
    const unresolvedDelta = deltas.unresolvedDelta ?? 0;
    const sentDelta = deltas.sentDelta ?? 0;
    const failedDelta = deltas.failedDelta ?? 0;
    const canceledDelta = deltas.canceledDelta ?? 0;
    const duplicateDelta = deltas.duplicateDelta ?? 0;
    await client.query(
      `
        UPDATE alarms
        SET
          active = active + $2,
          unresolved = unresolved + $3,
          sent = sent + $4,
          failed = failed + $5,
          canceled = canceled + $6,
          duplicate_count = duplicate_count + $7,
          status = CASE
            WHEN active + $2 > 0 THEN
              CASE WHEN cancel_requested_at IS NULL THEN 'SENDING' ELSE 'CANCEL_REQUESTED' END
            WHEN unresolved + $3 > 0
              OR duplicate_count + $7 > 0
              OR incident_count > 0
              THEN 'ATTENTION_REQUIRED'
            WHEN cancel_requested_at IS NOT NULL THEN 'CANCELED'
            WHEN failed + $5 = 0 THEN 'COMPLETED'
            WHEN sent + $4 > 0 AND failed + $5 > 0 THEN 'PARTIALLY_FAILED'
            ELSE 'FAILED'
          END,
          finished_at = CASE
            WHEN active + $2 = 0 THEN COALESCE(finished_at, clock_timestamp())
            ELSE finished_at
          END,
          updated_at = clock_timestamp(),
          version = version + 1
        WHERE id = $1
      `,
      [
        alarmId,
        activeDelta,
        unresolvedDelta,
        sentDelta,
        failedDelta,
        canceledDelta,
        duplicateDelta,
      ],
    );
  }

  private async lockDelivery(client: PoolClient, deliveryId: string): Promise<LockedDeliveryRow> {
    const result = await client.query<LockedDeliveryRow>(
      `
        SELECT
          *,
          first_unknown_at IS NOT NULL
            AND first_unknown_at <=
              clock_timestamp() - ($2::integer * interval '1 millisecond')
            AS authoritative_empty_eligible,
          first_unknown_at IS NOT NULL
            AND first_unknown_at <=
              clock_timestamp() - ($3::integer * interval '1 millisecond')
            AS fast_deadline_reached
        FROM deliveries
        WHERE id = $1
        FOR UPDATE
      `,
      [deliveryId, this.authoritativeEmptyGraceMs, this.maxFastElapsedMs],
    );
    const row = result.rows[0];
    if (row === undefined) {
      throw new Error(`Reconciliation Delivery disappeared: ${deliveryId}`);
    }
    return row;
  }

  private async lockDispatchAttempt(
    client: PoolClient,
    deliveryId: string,
    attemptId: string,
  ): Promise<LockedDispatchAttemptRow> {
    const result = await client.query<LockedDispatchAttemptRow>(
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

  private owns(delivery: LockedDeliveryRow, claim: ReconciliationClaim): boolean {
    const expectedState = claim.sourceState === 'UNKNOWN' ? 'RECONCILING' : 'NEEDS_REVIEW';
    return (
      delivery.alarm_id === claim.alarmId &&
      delivery.state === expectedState &&
      delivery.reconcile_generation === claim.reconcileGeneration &&
      delivery.reconcile_token === claim.reconcileToken
    );
  }

  private async transaction<T>(work: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.pool.connect().catch((error: unknown) => {
      throw new PersistenceUnavailableError(error);
    });
    const detachErrorHandler = attachPostgresClientErrorHandler(client, {
      component: 'reconciliation-repository',
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
