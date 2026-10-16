import type { AlarmStatus } from '../../packages/alarms/src/domain/alarm';

export const DELIVERY_STATES = [
  'READY',
  'CLAIMED',
  'IN_FLIGHT',
  'RETRY_WAIT',
  'UNKNOWN',
  'RECONCILING',
  'NEEDS_REVIEW',
  'SENT',
  'FAILED_PERMANENT',
  'FAILED_RETRY_EXHAUSTED',
  'CANCELED',
] as const;

export type DeliveryState = (typeof DELIVERY_STATES)[number];

export const TRUE_TERMINAL_DELIVERY_STATES = [
  'SENT',
  'FAILED_PERMANENT',
  'FAILED_RETRY_EXHAUSTED',
  'CANCELED',
] as const satisfies readonly DeliveryState[];

export type TrueTerminalDeliveryState = (typeof TRUE_TERMINAL_DELIVERY_STATES)[number];

/**
 * Guard-resolved events from the authoritative transition table.
 *
 * A caller must resolve external guards (cancel ordering, request phase, evidence cardinality,
 * budget and deadline) before asking for a decision. Keeping those outcomes explicit prevents a
 * boolean combination from silently selecting a less safe edge.
 */
export const DELIVERY_EVENTS = [
  'CLAIM_DUE',
  'CANCEL_SWEEP',
  'RELEASE_CLAIM',
  'RELEASE_CLAIM_CANCELED',
  'START_ATTEMPT',
  'COMMIT_PROVIDER',
  'FINISH_NOT_SENT_RETRY',
  'FINISH_NOT_SENT_CANCELED',
  'PROVIDER_ACCEPTED',
  'PROVIDER_PERMANENT_FAILURE',
  'PROVIDER_RATE_LIMITED_RETRY',
  'PROVIDER_RATE_LIMITED_EXHAUSTED',
  'PROVIDER_RATE_LIMITED_CANCELED',
  'PROVIDER_DEFINITELY_NOT_SENT_RETRY',
  'PROVIDER_DEFINITELY_NOT_SENT_CANCELED',
  'PROVIDER_TRANSIENT_RETRY',
  'PROVIDER_TRANSIENT_EXHAUSTED',
  'PROVIDER_TRANSIENT_CANCELED',
  'PROVIDER_AMBIGUOUS',
  'LEASE_EXPIRED_COMMITTED',
  'RECONCILE_CLAIM',
  'RECONCILE_MATCH_ONE',
  'RECONCILE_MATCH_DUPLICATE',
  'RECONCILE_BLOCKING_CONFLICT',
  'RECONCILE_NO_MATCH_RETRY',
  'RECONCILE_NO_MATCH_DEADLINE',
  'RECONCILE_AUTHORITATIVE_EMPTY_RETRY',
  'RECONCILE_AUTHORITATIVE_EMPTY_EXHAUSTED',
  'RECONCILE_AUTHORITATIVE_EMPTY_CANCELED',
  'REVIEW_MATCH_ONE',
  'REVIEW_MATCH_DUPLICATE',
  'REVIEW_BLOCKING_CONFLICT',
  'REVIEW_NO_MATCH',
  'TERMINAL_COMPATIBLE_EVIDENCE',
  'TERMINAL_RECIPIENT_CONFLICT_FIRST',
  'TERMINAL_EVIDENCE_CONFLICT_FIRST',
  'TERMINAL_MATCHING_POSITIVE_FIRST',
  'TERMINAL_DISTINCT_DUPLICATE_FIRST',
  'TERMINAL_ANOMALY_REPEAT',
  'STALE_RESULT',
] as const;

export type DeliveryEvent = (typeof DELIVERY_EVENTS)[number];

export interface DeliveryCounterDelta {
  readonly active: number;
  readonly sent: number;
  readonly failed: number;
  readonly canceled: number;
  readonly unresolved: number;
  readonly duplicate: number;
  readonly incident: number;
}

export type AttemptEffect = 'NONE' | 'STARTED' | 'COMMITTED' | 'FINISHED' | 'FINISHED_NOT_SENT';

export type TransitionAuditEffect =
  | 'NONE'
  | 'ATTEMPT_STARTED'
  | 'ATTEMPT_FINISHED'
  | 'ATTEMPT_NOT_SENT'
  | 'EVIDENCE_DECIDED'
  | 'INCIDENT_RECORDED';

export interface DeliveryTransitionDecision {
  readonly effect: 'APPLIED' | 'IGNORED_TERMINAL';
  readonly to: DeliveryState;
  readonly counterDelta: DeliveryCounterDelta;
  readonly attemptEffect: AttemptEffect;
  readonly attemptNoDelta: 0 | 1;
  readonly postBudgetDelta: -1 | 0 | 1;
  readonly rateLimitDelta: 0 | 1;
  readonly auditEffect: TransitionAuditEffect;
  readonly correctedAt: 'UNCHANGED' | 'SET_DATABASE_NOW';
}

export class DeliveryTransitionError extends Error {
  public readonly code = 'INVALID_DELIVERY_TRANSITION';

  public constructor(
    public readonly from: DeliveryState,
    public readonly event: DeliveryEvent,
  ) {
    super(`Delivery transition ${from} + ${event} is not allowed`);
    this.name = 'DeliveryTransitionError';
  }
}

const ZERO_COUNTER_DELTA: DeliveryCounterDelta = Object.freeze({
  active: 0,
  sent: 0,
  failed: 0,
  canceled: 0,
  unresolved: 0,
  duplicate: 0,
  incident: 0,
});

interface TransitionOptions {
  readonly counterDelta?: Partial<DeliveryCounterDelta>;
  readonly attemptEffect?: AttemptEffect;
  readonly attemptNoDelta?: 0 | 1;
  readonly postBudgetDelta?: -1 | 0 | 1;
  readonly rateLimitDelta?: 0 | 1;
  readonly auditEffect?: TransitionAuditEffect;
  readonly correctedAt?: 'UNCHANGED' | 'SET_DATABASE_NOW';
}

function decision(to: DeliveryState, options: TransitionOptions = {}): DeliveryTransitionDecision {
  return {
    effect: 'APPLIED',
    to,
    counterDelta: { ...ZERO_COUNTER_DELTA, ...options.counterDelta },
    attemptEffect: options.attemptEffect ?? 'NONE',
    attemptNoDelta: options.attemptNoDelta ?? 0,
    postBudgetDelta: options.postBudgetDelta ?? 0,
    rateLimitDelta: options.rateLimitDelta ?? 0,
    auditEffect: options.auditEffect ?? 'NONE',
    correctedAt: options.correctedAt ?? 'UNCHANGED',
  };
}

function terminalNoop(state: TrueTerminalDeliveryState): DeliveryTransitionDecision {
  return {
    ...decision(state),
    effect: 'IGNORED_TERMINAL',
  };
}

const TRANSITIONS: Readonly<
  Record<DeliveryState, Partial<Record<DeliveryEvent, DeliveryTransitionDecision>>>
> = {
  READY: {
    CLAIM_DUE: decision('CLAIMED'),
    CANCEL_SWEEP: decision('CANCELED', {
      counterDelta: { active: -1, canceled: 1 },
    }),
  },
  RETRY_WAIT: {
    CLAIM_DUE: decision('CLAIMED'),
    CANCEL_SWEEP: decision('CANCELED', {
      counterDelta: { active: -1, canceled: 1 },
    }),
  },
  CLAIMED: {
    CANCEL_SWEEP: decision('CANCELED', {
      counterDelta: { active: -1, canceled: 1 },
    }),
    RELEASE_CLAIM: decision('READY'),
    RELEASE_CLAIM_CANCELED: decision('CANCELED', {
      counterDelta: { active: -1, canceled: 1 },
    }),
    START_ATTEMPT: decision('IN_FLIGHT', {
      attemptEffect: 'STARTED',
      attemptNoDelta: 1,
      postBudgetDelta: 1,
      auditEffect: 'ATTEMPT_STARTED',
    }),
  },
  IN_FLIGHT: {
    COMMIT_PROVIDER: decision('IN_FLIGHT', { attemptEffect: 'COMMITTED' }),
    FINISH_NOT_SENT_RETRY: decision('READY', {
      attemptEffect: 'FINISHED_NOT_SENT',
      postBudgetDelta: -1,
      auditEffect: 'ATTEMPT_NOT_SENT',
    }),
    FINISH_NOT_SENT_CANCELED: decision('CANCELED', {
      counterDelta: { active: -1, canceled: 1 },
      attemptEffect: 'FINISHED_NOT_SENT',
      postBudgetDelta: -1,
      auditEffect: 'ATTEMPT_NOT_SENT',
    }),
    PROVIDER_ACCEPTED: decision('SENT', {
      counterDelta: { active: -1, sent: 1 },
      attemptEffect: 'FINISHED',
      auditEffect: 'ATTEMPT_FINISHED',
    }),
    PROVIDER_PERMANENT_FAILURE: decision('FAILED_PERMANENT', {
      counterDelta: { active: -1, failed: 1 },
      attemptEffect: 'FINISHED',
      auditEffect: 'ATTEMPT_FINISHED',
    }),
    PROVIDER_RATE_LIMITED_RETRY: decision('RETRY_WAIT', {
      attemptEffect: 'FINISHED',
      postBudgetDelta: -1,
      rateLimitDelta: 1,
      auditEffect: 'ATTEMPT_FINISHED',
    }),
    PROVIDER_RATE_LIMITED_EXHAUSTED: decision('FAILED_RETRY_EXHAUSTED', {
      counterDelta: { active: -1, failed: 1 },
      attemptEffect: 'FINISHED',
      postBudgetDelta: -1,
      rateLimitDelta: 1,
      auditEffect: 'ATTEMPT_FINISHED',
    }),
    PROVIDER_RATE_LIMITED_CANCELED: decision('CANCELED', {
      counterDelta: { active: -1, canceled: 1 },
      attemptEffect: 'FINISHED',
      postBudgetDelta: -1,
      auditEffect: 'ATTEMPT_FINISHED',
    }),
    PROVIDER_DEFINITELY_NOT_SENT_RETRY: decision('RETRY_WAIT', {
      attemptEffect: 'FINISHED_NOT_SENT',
      postBudgetDelta: -1,
      auditEffect: 'ATTEMPT_NOT_SENT',
    }),
    PROVIDER_DEFINITELY_NOT_SENT_CANCELED: decision('CANCELED', {
      counterDelta: { active: -1, canceled: 1 },
      attemptEffect: 'FINISHED_NOT_SENT',
      postBudgetDelta: -1,
      auditEffect: 'ATTEMPT_NOT_SENT',
    }),
    PROVIDER_TRANSIENT_RETRY: decision('RETRY_WAIT', {
      attemptEffect: 'FINISHED',
      auditEffect: 'ATTEMPT_FINISHED',
    }),
    PROVIDER_TRANSIENT_EXHAUSTED: decision('FAILED_RETRY_EXHAUSTED', {
      counterDelta: { active: -1, failed: 1 },
      attemptEffect: 'FINISHED',
      auditEffect: 'ATTEMPT_FINISHED',
    }),
    PROVIDER_TRANSIENT_CANCELED: decision('CANCELED', {
      counterDelta: { active: -1, canceled: 1 },
      attemptEffect: 'FINISHED',
      auditEffect: 'ATTEMPT_FINISHED',
    }),
    PROVIDER_AMBIGUOUS: decision('UNKNOWN', {
      attemptEffect: 'FINISHED',
      auditEffect: 'ATTEMPT_FINISHED',
    }),
    LEASE_EXPIRED_COMMITTED: decision('UNKNOWN'),
  },
  UNKNOWN: {
    RECONCILE_CLAIM: decision('RECONCILING'),
  },
  RECONCILING: {
    RECONCILE_CLAIM: decision('RECONCILING'),
    RECONCILE_MATCH_ONE: decision('SENT', {
      counterDelta: { active: -1, sent: 1 },
      attemptEffect: 'FINISHED',
      auditEffect: 'EVIDENCE_DECIDED',
    }),
    RECONCILE_MATCH_DUPLICATE: decision('SENT', {
      counterDelta: { active: -1, sent: 1, duplicate: 1 },
      attemptEffect: 'FINISHED',
      auditEffect: 'EVIDENCE_DECIDED',
    }),
    RECONCILE_BLOCKING_CONFLICT: decision('NEEDS_REVIEW', {
      counterDelta: { active: -1, unresolved: 1 },
      attemptEffect: 'FINISHED',
      auditEffect: 'EVIDENCE_DECIDED',
    }),
    RECONCILE_NO_MATCH_RETRY: decision('UNKNOWN', { auditEffect: 'EVIDENCE_DECIDED' }),
    RECONCILE_NO_MATCH_DEADLINE: decision('NEEDS_REVIEW', {
      counterDelta: { active: -1, unresolved: 1 },
      attemptEffect: 'FINISHED',
      auditEffect: 'EVIDENCE_DECIDED',
    }),
    RECONCILE_AUTHORITATIVE_EMPTY_RETRY: decision('RETRY_WAIT', {
      attemptEffect: 'FINISHED',
      auditEffect: 'EVIDENCE_DECIDED',
    }),
    RECONCILE_AUTHORITATIVE_EMPTY_EXHAUSTED: decision('FAILED_RETRY_EXHAUSTED', {
      counterDelta: { active: -1, failed: 1 },
      attemptEffect: 'FINISHED',
      auditEffect: 'EVIDENCE_DECIDED',
    }),
    RECONCILE_AUTHORITATIVE_EMPTY_CANCELED: decision('CANCELED', {
      counterDelta: { active: -1, canceled: 1 },
      attemptEffect: 'FINISHED',
      auditEffect: 'EVIDENCE_DECIDED',
    }),
  },
  NEEDS_REVIEW: {
    RECONCILE_CLAIM: decision('NEEDS_REVIEW'),
    REVIEW_MATCH_ONE: decision('SENT', {
      counterDelta: { sent: 1, unresolved: -1 },
      auditEffect: 'EVIDENCE_DECIDED',
      correctedAt: 'SET_DATABASE_NOW',
    }),
    REVIEW_MATCH_DUPLICATE: decision('SENT', {
      counterDelta: { sent: 1, unresolved: -1, duplicate: 1 },
      auditEffect: 'EVIDENCE_DECIDED',
      correctedAt: 'SET_DATABASE_NOW',
    }),
    REVIEW_BLOCKING_CONFLICT: decision('NEEDS_REVIEW', {
      auditEffect: 'EVIDENCE_DECIDED',
    }),
    REVIEW_NO_MATCH: decision('NEEDS_REVIEW', { auditEffect: 'EVIDENCE_DECIDED' }),
  },
  SENT: {},
  FAILED_PERMANENT: {},
  FAILED_RETRY_EXHAUSTED: {},
  CANCELED: {},
};

function isTrueTerminal(state: DeliveryState): state is TrueTerminalDeliveryState {
  return (TRUE_TERMINAL_DELIVERY_STATES as readonly DeliveryState[]).includes(state);
}

function decideTerminalEvidence(
  state: TrueTerminalDeliveryState,
  event: DeliveryEvent,
): DeliveryTransitionDecision | null {
  if (
    event === 'TERMINAL_COMPATIBLE_EVIDENCE' ||
    event === 'TERMINAL_ANOMALY_REPEAT' ||
    event === 'STALE_RESULT'
  ) {
    return terminalNoop(state);
  }

  if (
    event === 'TERMINAL_RECIPIENT_CONFLICT_FIRST' ||
    event === 'TERMINAL_EVIDENCE_CONFLICT_FIRST'
  ) {
    return decision(state, {
      counterDelta: { incident: 1 },
      auditEffect: 'INCIDENT_RECORDED',
    });
  }

  if (event === 'TERMINAL_MATCHING_POSITIVE_FIRST') {
    if (state === 'SENT') {
      return terminalNoop(state);
    }
    return decision(state, {
      counterDelta: { incident: 1 },
      auditEffect: 'INCIDENT_RECORDED',
    });
  }

  if (event === 'TERMINAL_DISTINCT_DUPLICATE_FIRST') {
    return decision(state, {
      counterDelta: {
        incident: 1,
        duplicate: state === 'SENT' ? 1 : 0,
      },
      auditEffect: 'INCIDENT_RECORDED',
    });
  }

  return null;
}

/**
 * Returns the guard-resolved transition or rejects an impossible non-terminal edge.
 * True-terminal rows deliberately absorb stale worker events: only validated, first-seen evidence
 * can add the monotonic incident projection, and no event can reopen the business result.
 */
export function decideDeliveryTransitionOracle(
  from: DeliveryState,
  event: DeliveryEvent,
): DeliveryTransitionDecision {
  if (isTrueTerminal(from)) {
    return decideTerminalEvidence(from, event) ?? terminalNoop(from);
  }

  const resolved = TRANSITIONS[from][event];
  if (resolved === undefined) {
    throw new DeliveryTransitionError(from, event);
  }
  return resolved;
}
export interface AlarmDeliveryAggregate {
  readonly audienceFinalized: boolean;
  readonly currentStatus: AlarmStatus;
  readonly cancelRequested: boolean;
  readonly total: number | null;
  readonly sent: number;
  readonly failed: number;
  readonly canceled: number;
  readonly active: number;
  readonly unresolved: number;
  readonly duplicateCount: number;
  readonly incidentCount: number;
}

function assertNonnegativeSafeInteger(name: string, value: number): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new RangeError(`${name} must be a non-negative safe integer`);
  }
}

/** Reference oracle for the SQL projection CASE expressions. */
export function deriveAlarmStatusOracle(input: AlarmDeliveryAggregate): AlarmStatus {
  const counters = {
    sent: input.sent,
    failed: input.failed,
    canceled: input.canceled,
    active: input.active,
    unresolved: input.unresolved,
    duplicateCount: input.duplicateCount,
    incidentCount: input.incidentCount,
  };
  for (const [name, value] of Object.entries(counters)) {
    assertNonnegativeSafeInteger(name, value);
  }

  if (!input.audienceFinalized) {
    if (
      input.total !== null ||
      input.sent !== 0 ||
      input.failed !== 0 ||
      input.canceled !== 0 ||
      input.active !== 0 ||
      input.unresolved !== 0
    ) {
      throw new RangeError('unfinalized audience cannot have Delivery result counters');
    }
    if (!['QUEUED', 'EXPANDING', 'CANCEL_REQUESTED'].includes(input.currentStatus)) {
      throw new RangeError('unfinalized audience must remain in an expansion lifecycle status');
    }
    return input.currentStatus;
  }

  if (input.total === null) {
    throw new RangeError('finalized audience must have total');
  }
  assertNonnegativeSafeInteger('total', input.total);
  if (
    input.sent + input.failed + input.canceled + input.active + input.unresolved !==
    input.total
  ) {
    throw new RangeError('Delivery result counters must sum to total');
  }
  if (input.canceled > 0 && !input.cancelRequested) {
    throw new RangeError('canceled deliveries require a cancellation request');
  }
  if (input.duplicateCount > input.sent) {
    throw new RangeError('duplicate count cannot exceed sent deliveries');
  }

  if (input.active > 0) {
    return input.cancelRequested ? 'CANCEL_REQUESTED' : 'SENDING';
  }
  if (input.unresolved > 0 || input.duplicateCount > 0 || input.incidentCount > 0) {
    return 'ATTENTION_REQUIRED';
  }
  if (input.cancelRequested) {
    return 'CANCELED';
  }
  if (input.failed === 0) {
    return 'COMPLETED';
  }
  if (input.sent > 0) {
    return 'PARTIALLY_FAILED';
  }
  return 'FAILED';
}
