import { describe, expect, it } from 'vitest';

import {
  type AlarmDeliveryAggregate,
  DELIVERY_EVENTS,
  DELIVERY_STATES,
  type DeliveryEvent,
  type DeliveryState,
  DeliveryTransitionError,
  decideDeliveryTransitionOracle,
  deriveAlarmStatusOracle,
  TRUE_TERMINAL_DELIVERY_STATES,
} from '../support/delivery-transition-model';

interface AllowedTransition {
  readonly from: DeliveryState;
  readonly event: DeliveryEvent;
  readonly to: DeliveryState;
  readonly active?: number;
  readonly sent?: number;
  readonly failed?: number;
  readonly canceled?: number;
  readonly unresolved?: number;
  readonly duplicate?: number;
  readonly incident?: number;
  readonly attempt?: ReturnType<typeof decideDeliveryTransitionOracle>['attemptEffect'];
  readonly attemptNo?: 0 | 1;
  readonly postBudget?: -1 | 0 | 1;
  readonly rateLimit?: 0 | 1;
  readonly audit?: ReturnType<typeof decideDeliveryTransitionOracle>['auditEffect'];
  readonly corrected?: ReturnType<typeof decideDeliveryTransitionOracle>['correctedAt'];
}

const ALLOWED_NON_TERMINAL_TRANSITIONS = [
  { from: 'READY', event: 'CLAIM_DUE', to: 'CLAIMED' },
  { from: 'READY', event: 'CANCEL_SWEEP', to: 'CANCELED', active: -1, canceled: 1 },
  { from: 'RETRY_WAIT', event: 'CLAIM_DUE', to: 'CLAIMED' },
  {
    from: 'RETRY_WAIT',
    event: 'CANCEL_SWEEP',
    to: 'CANCELED',
    active: -1,
    canceled: 1,
  },
  { from: 'CLAIMED', event: 'CANCEL_SWEEP', to: 'CANCELED', active: -1, canceled: 1 },
  { from: 'CLAIMED', event: 'RELEASE_CLAIM', to: 'READY' },
  {
    from: 'CLAIMED',
    event: 'RELEASE_CLAIM_CANCELED',
    to: 'CANCELED',
    active: -1,
    canceled: 1,
  },
  {
    from: 'CLAIMED',
    event: 'START_ATTEMPT',
    to: 'IN_FLIGHT',
    attempt: 'STARTED',
    attemptNo: 1,
    postBudget: 1,
    audit: 'ATTEMPT_STARTED',
  },
  { from: 'IN_FLIGHT', event: 'COMMIT_PROVIDER', to: 'IN_FLIGHT', attempt: 'COMMITTED' },
  {
    from: 'IN_FLIGHT',
    event: 'FINISH_NOT_SENT_RETRY',
    to: 'READY',
    attempt: 'FINISHED_NOT_SENT',
    postBudget: -1,
    audit: 'ATTEMPT_NOT_SENT',
  },
  {
    from: 'IN_FLIGHT',
    event: 'FINISH_NOT_SENT_CANCELED',
    to: 'CANCELED',
    active: -1,
    canceled: 1,
    attempt: 'FINISHED_NOT_SENT',
    postBudget: -1,
    audit: 'ATTEMPT_NOT_SENT',
  },
  {
    from: 'IN_FLIGHT',
    event: 'PROVIDER_ACCEPTED',
    to: 'SENT',
    active: -1,
    sent: 1,
    attempt: 'FINISHED',
    audit: 'ATTEMPT_FINISHED',
  },
  {
    from: 'IN_FLIGHT',
    event: 'PROVIDER_PERMANENT_FAILURE',
    to: 'FAILED_PERMANENT',
    active: -1,
    failed: 1,
    attempt: 'FINISHED',
    audit: 'ATTEMPT_FINISHED',
  },
  {
    from: 'IN_FLIGHT',
    event: 'PROVIDER_RATE_LIMITED_RETRY',
    to: 'RETRY_WAIT',
    attempt: 'FINISHED',
    postBudget: -1,
    rateLimit: 1,
    audit: 'ATTEMPT_FINISHED',
  },
  {
    from: 'IN_FLIGHT',
    event: 'PROVIDER_RATE_LIMITED_EXHAUSTED',
    to: 'FAILED_RETRY_EXHAUSTED',
    active: -1,
    failed: 1,
    attempt: 'FINISHED',
    postBudget: -1,
    rateLimit: 1,
    audit: 'ATTEMPT_FINISHED',
  },
  {
    from: 'IN_FLIGHT',
    event: 'PROVIDER_RATE_LIMITED_CANCELED',
    to: 'CANCELED',
    active: -1,
    canceled: 1,
    attempt: 'FINISHED',
    postBudget: -1,
    audit: 'ATTEMPT_FINISHED',
  },
  {
    from: 'IN_FLIGHT',
    event: 'PROVIDER_DEFINITELY_NOT_SENT_RETRY',
    to: 'RETRY_WAIT',
    attempt: 'FINISHED_NOT_SENT',
    postBudget: -1,
    audit: 'ATTEMPT_NOT_SENT',
  },
  {
    from: 'IN_FLIGHT',
    event: 'PROVIDER_DEFINITELY_NOT_SENT_CANCELED',
    to: 'CANCELED',
    active: -1,
    canceled: 1,
    attempt: 'FINISHED_NOT_SENT',
    postBudget: -1,
    audit: 'ATTEMPT_NOT_SENT',
  },
  {
    from: 'IN_FLIGHT',
    event: 'PROVIDER_TRANSIENT_RETRY',
    to: 'RETRY_WAIT',
    attempt: 'FINISHED',
    audit: 'ATTEMPT_FINISHED',
  },
  {
    from: 'IN_FLIGHT',
    event: 'PROVIDER_TRANSIENT_EXHAUSTED',
    to: 'FAILED_RETRY_EXHAUSTED',
    active: -1,
    failed: 1,
    attempt: 'FINISHED',
    audit: 'ATTEMPT_FINISHED',
  },
  {
    from: 'IN_FLIGHT',
    event: 'PROVIDER_TRANSIENT_CANCELED',
    to: 'CANCELED',
    active: -1,
    canceled: 1,
    attempt: 'FINISHED',
    audit: 'ATTEMPT_FINISHED',
  },
  {
    from: 'IN_FLIGHT',
    event: 'PROVIDER_AMBIGUOUS',
    to: 'UNKNOWN',
    attempt: 'FINISHED',
    audit: 'ATTEMPT_FINISHED',
  },
  { from: 'IN_FLIGHT', event: 'LEASE_EXPIRED_COMMITTED', to: 'UNKNOWN' },
  { from: 'UNKNOWN', event: 'RECONCILE_CLAIM', to: 'RECONCILING' },
  { from: 'RECONCILING', event: 'RECONCILE_CLAIM', to: 'RECONCILING' },
  {
    from: 'RECONCILING',
    event: 'RECONCILE_MATCH_ONE',
    to: 'SENT',
    active: -1,
    sent: 1,
    attempt: 'FINISHED',
    audit: 'EVIDENCE_DECIDED',
  },
  {
    from: 'RECONCILING',
    event: 'RECONCILE_MATCH_DUPLICATE',
    to: 'SENT',
    active: -1,
    sent: 1,
    duplicate: 1,
    attempt: 'FINISHED',
    audit: 'EVIDENCE_DECIDED',
  },
  {
    from: 'RECONCILING',
    event: 'RECONCILE_BLOCKING_CONFLICT',
    to: 'NEEDS_REVIEW',
    active: -1,
    unresolved: 1,
    attempt: 'FINISHED',
    audit: 'EVIDENCE_DECIDED',
  },
  {
    from: 'RECONCILING',
    event: 'RECONCILE_NO_MATCH_RETRY',
    to: 'UNKNOWN',
    audit: 'EVIDENCE_DECIDED',
  },
  {
    from: 'RECONCILING',
    event: 'RECONCILE_NO_MATCH_DEADLINE',
    to: 'NEEDS_REVIEW',
    active: -1,
    unresolved: 1,
    attempt: 'FINISHED',
    audit: 'EVIDENCE_DECIDED',
  },
  {
    from: 'RECONCILING',
    event: 'RECONCILE_AUTHORITATIVE_EMPTY_RETRY',
    to: 'RETRY_WAIT',
    attempt: 'FINISHED',
    audit: 'EVIDENCE_DECIDED',
  },
  {
    from: 'RECONCILING',
    event: 'RECONCILE_AUTHORITATIVE_EMPTY_EXHAUSTED',
    to: 'FAILED_RETRY_EXHAUSTED',
    active: -1,
    failed: 1,
    attempt: 'FINISHED',
    audit: 'EVIDENCE_DECIDED',
  },
  {
    from: 'RECONCILING',
    event: 'RECONCILE_AUTHORITATIVE_EMPTY_CANCELED',
    to: 'CANCELED',
    active: -1,
    canceled: 1,
    attempt: 'FINISHED',
    audit: 'EVIDENCE_DECIDED',
  },
  { from: 'NEEDS_REVIEW', event: 'RECONCILE_CLAIM', to: 'NEEDS_REVIEW' },
  {
    from: 'NEEDS_REVIEW',
    event: 'REVIEW_MATCH_ONE',
    to: 'SENT',
    sent: 1,
    unresolved: -1,
    audit: 'EVIDENCE_DECIDED',
    corrected: 'SET_DATABASE_NOW',
  },
  {
    from: 'NEEDS_REVIEW',
    event: 'REVIEW_MATCH_DUPLICATE',
    to: 'SENT',
    sent: 1,
    unresolved: -1,
    duplicate: 1,
    audit: 'EVIDENCE_DECIDED',
    corrected: 'SET_DATABASE_NOW',
  },
  {
    from: 'NEEDS_REVIEW',
    event: 'REVIEW_BLOCKING_CONFLICT',
    to: 'NEEDS_REVIEW',
    audit: 'EVIDENCE_DECIDED',
  },
  {
    from: 'NEEDS_REVIEW',
    event: 'REVIEW_NO_MATCH',
    to: 'NEEDS_REVIEW',
    audit: 'EVIDENCE_DECIDED',
  },
] as const satisfies readonly AllowedTransition[];

const ALLOWED_BY_PAIR = new Map(
  ALLOWED_NON_TERMINAL_TRANSITIONS.map((entry) => [`${entry.from}:${entry.event}`, entry]),
);

function expectedDelta(row: AllowedTransition) {
  return {
    active: row.active ?? 0,
    sent: row.sent ?? 0,
    failed: row.failed ?? 0,
    canceled: row.canceled ?? 0,
    unresolved: row.unresolved ?? 0,
    duplicate: row.duplicate ?? 0,
    incident: row.incident ?? 0,
  };
}

describe('Delivery transition policy', () => {
  it.each(ALLOWED_NON_TERMINAL_TRANSITIONS)(
    'U-SM-01 applies $from + $event -> $to with exact durable effects',
    (row) => {
      const result = decideDeliveryTransitionOracle(row.from, row.event);

      expect(result).toEqual({
        effect: 'APPLIED',
        to: row.to,
        counterDelta: expectedDelta(row),
        attemptEffect: row.attempt ?? 'NONE',
        attemptNoDelta: row.attemptNo ?? 0,
        postBudgetDelta: row.postBudget ?? 0,
        rateLimitDelta: row.rateLimit ?? 0,
        auditEffect: row.audit ?? 'NONE',
        correctedAt: row.corrected ?? 'UNCHANGED',
      });
    },
  );

  it('U-SM-02 rejects every unspecified non-terminal state/event pair without mutation', () => {
    const nonTerminalStates = DELIVERY_STATES.filter(
      (state) => !(TRUE_TERMINAL_DELIVERY_STATES as readonly DeliveryState[]).includes(state),
    );

    expect(ALLOWED_BY_PAIR.size).toBe(ALLOWED_NON_TERMINAL_TRANSITIONS.length);
    let forbiddenPairCount = 0;
    for (const from of nonTerminalStates) {
      for (const event of DELIVERY_EVENTS) {
        if (ALLOWED_BY_PAIR.has(`${from}:${event}`)) {
          continue;
        }
        forbiddenPairCount += 1;
        const before = Object.freeze({
          state: from,
          attemptNo: 7,
          postBudget: 3,
          clientRef: '018f0000-0000-7000-8000-000000000001',
        });

        expect(() => decideDeliveryTransitionOracle(from, event)).toThrowError(
          new DeliveryTransitionError(from, event),
        );
        expect(before).toEqual({
          state: from,
          attemptNo: 7,
          postBudget: 3,
          clientRef: '018f0000-0000-7000-8000-000000000001',
        });
      }
    }
    expect(ALLOWED_BY_PAIR.size + forbiddenPairCount).toBe(
      nonTerminalStates.length * DELIVERY_EVENTS.length,
    );
  });

  it('U-SM-03 absorbs every operational event in all true-terminal states', () => {
    const evidenceEvents = new Set<DeliveryEvent>([
      'TERMINAL_COMPATIBLE_EVIDENCE',
      'TERMINAL_RECIPIENT_CONFLICT_FIRST',
      'TERMINAL_EVIDENCE_CONFLICT_FIRST',
      'TERMINAL_MATCHING_POSITIVE_FIRST',
      'TERMINAL_DISTINCT_DUPLICATE_FIRST',
      'TERMINAL_ANOMALY_REPEAT',
    ]);

    for (const state of TRUE_TERMINAL_DELIVERY_STATES) {
      for (const event of DELIVERY_EVENTS) {
        const result = decideDeliveryTransitionOracle(state, event);
        expect(result.to, `${state} + ${event}`).toBe(state);
        expect(result.counterDelta.active, `${state} + ${event}`).toBe(0);
        expect(result.counterDelta.sent, `${state} + ${event}`).toBe(0);
        expect(result.counterDelta.failed, `${state} + ${event}`).toBe(0);
        expect(result.counterDelta.canceled, `${state} + ${event}`).toBe(0);
        expect(result.counterDelta.unresolved, `${state} + ${event}`).toBe(0);
        if (!evidenceEvents.has(event)) {
          expect(result.effect, `${state} + ${event}`).toBe('IGNORED_TERMINAL');
          expect(result.counterDelta.duplicate, `${state} + ${event}`).toBe(0);
          expect(result.counterDelta.incident, `${state} + ${event}`).toBe(0);
        }
      }
    }
  });

  it('U-SM-03 keeps compatible/repeated evidence idempotent and adopts only the first anomaly', () => {
    for (const state of TRUE_TERMINAL_DELIVERY_STATES) {
      expect(decideDeliveryTransitionOracle(state, 'TERMINAL_COMPATIBLE_EVIDENCE')).toMatchObject({
        effect: 'IGNORED_TERMINAL',
        to: state,
        counterDelta: { duplicate: 0, incident: 0 },
      });
      expect(decideDeliveryTransitionOracle(state, 'TERMINAL_ANOMALY_REPEAT')).toMatchObject({
        effect: 'IGNORED_TERMINAL',
        to: state,
        counterDelta: { duplicate: 0, incident: 0 },
      });
      expect(
        decideDeliveryTransitionOracle(state, 'TERMINAL_RECIPIENT_CONFLICT_FIRST'),
      ).toMatchObject({
        effect: 'APPLIED',
        to: state,
        counterDelta: { duplicate: 0, incident: 1 },
        auditEffect: 'INCIDENT_RECORDED',
      });
    }

    expect(
      decideDeliveryTransitionOracle('SENT', 'TERMINAL_MATCHING_POSITIVE_FIRST'),
    ).toMatchObject({
      effect: 'IGNORED_TERMINAL',
      counterDelta: { duplicate: 0, incident: 0 },
    });
    expect(
      decideDeliveryTransitionOracle('SENT', 'TERMINAL_DISTINCT_DUPLICATE_FIRST'),
    ).toMatchObject({
      to: 'SENT',
      counterDelta: { duplicate: 1, incident: 1 },
    });
    for (const state of ['FAILED_PERMANENT', 'FAILED_RETRY_EXHAUSTED', 'CANCELED'] as const) {
      expect(
        decideDeliveryTransitionOracle(state, 'TERMINAL_MATCHING_POSITIVE_FIRST'),
      ).toMatchObject({
        to: state,
        counterDelta: { duplicate: 0, incident: 1 },
      });
      expect(
        decideDeliveryTransitionOracle(state, 'TERMINAL_DISTINCT_DUPLICATE_FIRST'),
      ).toMatchObject({
        to: state,
        counterDelta: { duplicate: 0, incident: 1 },
      });
    }
  });

  it('U-SM-03 allows NEEDS_REVIEW correction only from validated positive evidence', () => {
    expect(decideDeliveryTransitionOracle('NEEDS_REVIEW', 'REVIEW_MATCH_ONE')).toMatchObject({
      to: 'SENT',
      counterDelta: { sent: 1, unresolved: -1, duplicate: 0 },
      correctedAt: 'SET_DATABASE_NOW',
    });
    expect(decideDeliveryTransitionOracle('NEEDS_REVIEW', 'REVIEW_MATCH_DUPLICATE')).toMatchObject({
      to: 'SENT',
      counterDelta: { sent: 1, unresolved: -1, duplicate: 1 },
      correctedAt: 'SET_DATABASE_NOW',
    });
    for (const event of ['REVIEW_NO_MATCH', 'REVIEW_BLOCKING_CONFLICT'] as const) {
      expect(decideDeliveryTransitionOracle('NEEDS_REVIEW', event)).toMatchObject({
        to: 'NEEDS_REVIEW',
        counterDelta: {
          active: 0,
          sent: 0,
          failed: 0,
          canceled: 0,
          unresolved: 0,
          duplicate: 0,
          incident: 0,
        },
        correctedAt: 'UNCHANGED',
      });
    }
    for (const event of ['CLAIM_DUE', 'CANCEL_SWEEP', 'PROVIDER_ACCEPTED'] as const) {
      expect(() => decideDeliveryTransitionOracle('NEEDS_REVIEW', event)).toThrow(
        DeliveryTransitionError,
      );
    }
  });
});

function aggregate(overrides: Partial<AlarmDeliveryAggregate> = {}): AlarmDeliveryAggregate {
  return {
    audienceFinalized: true,
    currentStatus: 'SENDING',
    cancelRequested: false,
    total: 1,
    sent: 0,
    failed: 0,
    canceled: 0,
    active: 1,
    unresolved: 0,
    duplicateCount: 0,
    incidentCount: 0,
    ...overrides,
  };
}

describe('Alarm Delivery aggregate projection', () => {
  it.each([
    {
      name: 'unfinalized queued audience',
      input: aggregate({
        audienceFinalized: false,
        currentStatus: 'QUEUED',
        total: null,
        active: 0,
      }),
      expected: 'QUEUED',
    },
    {
      name: 'unfinalized expansion',
      input: aggregate({
        audienceFinalized: false,
        currentStatus: 'EXPANDING',
        total: null,
        active: 0,
      }),
      expected: 'EXPANDING',
    },
    {
      name: 'unfinalized cancellation',
      input: aggregate({
        audienceFinalized: false,
        currentStatus: 'CANCEL_REQUESTED',
        cancelRequested: true,
        total: null,
        active: 0,
      }),
      expected: 'CANCEL_REQUESTED',
    },
    {
      name: 'active clean work',
      input: aggregate(),
      expected: 'SENDING',
    },
    {
      name: 'active work outranks unresolved and incidents',
      input: aggregate({ total: 2, unresolved: 1, incidentCount: 2 }),
      expected: 'SENDING',
    },
    {
      name: 'active cancellation outranks anomalies',
      input: aggregate({
        cancelRequested: true,
        total: 3,
        sent: 1,
        unresolved: 1,
        duplicateCount: 1,
        incidentCount: 1,
      }),
      expected: 'CANCEL_REQUESTED',
    },
    {
      name: 'unresolved result needs attention',
      input: aggregate({ active: 0, unresolved: 1 }),
      expected: 'ATTENTION_REQUIRED',
    },
    {
      name: 'duplicate result needs attention',
      input: aggregate({ active: 0, sent: 1, duplicateCount: 1 }),
      expected: 'ATTENTION_REQUIRED',
    },
    {
      name: 'post-terminal incident needs attention',
      input: aggregate({ active: 0, sent: 1, incidentCount: 1 }),
      expected: 'ATTENTION_REQUIRED',
    },
    {
      name: 'clean cancellation',
      input: aggregate({
        cancelRequested: true,
        active: 0,
        canceled: 1,
      }),
      expected: 'CANCELED',
    },
    {
      name: 'zero audience success',
      input: aggregate({ total: 0, active: 0 }),
      expected: 'COMPLETED',
    },
    {
      name: 'all sent',
      input: aggregate({ active: 0, sent: 1 }),
      expected: 'COMPLETED',
    },
    {
      name: 'mixed sent and failed',
      input: aggregate({ total: 2, active: 0, sent: 1, failed: 1 }),
      expected: 'PARTIALLY_FAILED',
    },
    {
      name: 'all failed',
      input: aggregate({ active: 0, failed: 1 }),
      expected: 'FAILED',
    },
  ] as const)('U-AGG-01 applies priority for $name', ({ input, expected }) => {
    expect(deriveAlarmStatusOracle(input)).toBe(expected);
  });

  it.each([
    aggregate({ active: -1 }),
    aggregate({ total: 2 }),
    aggregate({ audienceFinalized: true, total: null }),
    aggregate({
      audienceFinalized: false,
      currentStatus: 'QUEUED',
      total: null,
      sent: 1,
      active: 0,
    }),
    aggregate({
      audienceFinalized: false,
      currentStatus: 'SENDING',
      total: null,
      active: 0,
    }),
    aggregate({ active: 0, canceled: 1, cancelRequested: false }),
    aggregate({ active: 0, sent: 1, duplicateCount: 2 }),
  ])('U-AGG-01 rejects impossible or negative projection %#', (input) => {
    expect(() => deriveAlarmStatusOracle(input)).toThrow(RangeError);
  });
});

interface PropertyModel {
  state: DeliveryState;
  attemptNo: number;
  postBudget: number;
  rateLimitCount: number;
  clientRef: string;
  recipientId: string;
  runGeneration: string;
  cancelRequested: boolean;
  incidentRecorded: boolean;
  total: number;
  active: number;
  sent: number;
  failed: number;
  canceled: number;
  unresolved: number;
  duplicateCount: number;
  incidentCount: number;
}

function seededRandom(seed: number): () => number {
  let value = seed >>> 0;
  return () => {
    value ^= value << 13;
    value ^= value >>> 17;
    value ^= value << 5;
    return (value >>> 0) / 0x1_0000_0000;
  };
}

function allowedEventsFor(state: DeliveryState): readonly DeliveryEvent[] {
  if ((TRUE_TERMINAL_DELIVERY_STATES as readonly DeliveryState[]).includes(state)) {
    return DELIVERY_EVENTS;
  }
  return ALLOWED_NON_TERMINAL_TRANSITIONS.filter((row) => row.from === state).map(
    (row) => row.event,
  );
}

function propertyAggregate(model: PropertyModel): AlarmDeliveryAggregate {
  return aggregate({
    currentStatus: 'SENDING',
    cancelRequested: model.cancelRequested,
    total: model.total,
    sent: model.sent,
    failed: model.failed,
    canceled: model.canceled,
    active: model.active,
    unresolved: model.unresolved,
    duplicateCount: model.duplicateCount,
    incidentCount: model.incidentCount,
  });
}

function propertyInvariant(condition: boolean, message: string): asserts condition {
  if (!condition) {
    throw new Error(message);
  }
}

describe('deterministic Delivery sequence properties', () => {
  it('U-PROP-01 preserves identity, monotonicity, terminality, idempotence and counters', () => {
    const seeds = Array.from({ length: 128 }, (_, index) => 0x9e37_79b9 ^ (index + 1));

    for (const seed of seeds) {
      const random = seededRandom(seed);
      const model: PropertyModel = {
        state: 'READY',
        attemptNo: 0,
        postBudget: 0,
        rateLimitCount: 0,
        clientRef: `client-ref-seed-${seed}`,
        recipientId: `Opaque-é-e\u0301-${seed}`,
        runGeneration: `run-generation-${seed}`,
        cancelRequested: false,
        incidentRecorded: false,
        total: 1,
        active: 1,
        sent: 0,
        failed: 0,
        canceled: 0,
        unresolved: 0,
        duplicateCount: 0,
        incidentCount: 0,
      };
      const immutableIdentity = {
        clientRef: model.clientRef,
        recipientId: model.recipientId,
        runGeneration: model.runGeneration,
      };
      let priorAttemptNo = model.attemptNo;
      let terminalState: DeliveryState | null = null;

      for (let step = 0; step < 512; step += 1) {
        const allowed = allowedEventsFor(model.state);
        const chooseAllowed = allowed.length > 0 && random() < 0.72;
        let event = chooseAllowed
          ? allowed[Math.floor(random() * allowed.length)]
          : DELIVERY_EVENTS[Math.floor(random() * DELIVERY_EVENTS.length)];
        if (event === undefined) {
          throw new Error(`U-PROP-01 seed=${seed} step=${step}: generator selected no event`);
        }
        if (
          model.incidentRecorded &&
          [
            'TERMINAL_RECIPIENT_CONFLICT_FIRST',
            'TERMINAL_EVIDENCE_CONFLICT_FIRST',
            'TERMINAL_MATCHING_POSITIVE_FIRST',
            'TERMINAL_DISTINCT_DUPLICATE_FIRST',
          ].includes(event)
        ) {
          event = 'TERMINAL_ANOMALY_REPEAT';
        }

        const before = structuredClone(model);
        try {
          const result = decideDeliveryTransitionOracle(model.state, event);
          model.state = result.to;
          model.attemptNo += result.attemptNoDelta;
          model.postBudget += result.postBudgetDelta;
          model.rateLimitCount += result.rateLimitDelta;
          model.active += result.counterDelta.active;
          model.sent += result.counterDelta.sent;
          model.failed += result.counterDelta.failed;
          model.canceled += result.counterDelta.canceled;
          model.unresolved += result.counterDelta.unresolved;
          model.duplicateCount += result.counterDelta.duplicate;
          model.incidentCount += result.counterDelta.incident;
          if (result.counterDelta.incident === 1) {
            model.incidentRecorded = true;
          }
          if (result.counterDelta.canceled === 1) {
            model.cancelRequested = true;
          }

          if (terminalState !== null) {
            propertyInvariant(
              model.state === terminalState,
              `seed=${seed} step=${step}: ${event} reopened terminal ${terminalState}`,
            );
          }
          if (
            (TRUE_TERMINAL_DELIVERY_STATES as readonly DeliveryState[]).includes(model.state) &&
            terminalState === null
          ) {
            terminalState = model.state;
          }
        } catch (error) {
          if (!(error instanceof DeliveryTransitionError)) {
            throw error;
          }
          propertyInvariant(
            JSON.stringify(model) === JSON.stringify(before),
            `seed=${seed} step=${step}: rejected ${event} mutated the model`,
          );
        }

        propertyInvariant(
          model.attemptNo >= priorAttemptNo,
          `seed=${seed} step=${step}: attempt number decreased`,
        );
        priorAttemptNo = model.attemptNo;
        propertyInvariant(
          model.postBudget >= 0,
          `seed=${seed} step=${step}: post budget became negative`,
        );
        propertyInvariant(
          model.rateLimitCount >= 0,
          `seed=${seed} step=${step}: rate limit count became negative`,
        );
        for (const count of [
          model.total,
          model.active,
          model.sent,
          model.failed,
          model.canceled,
          model.unresolved,
          model.duplicateCount,
          model.incidentCount,
        ]) {
          propertyInvariant(count >= 0, `seed=${seed} step=${step}: counter became negative`);
        }
        propertyInvariant(
          model.sent + model.failed + model.canceled + model.active + model.unresolved ===
            model.total,
          `seed=${seed} step=${step}: result counters do not sum to total`,
        );
        propertyInvariant(
          model.clientRef === immutableIdentity.clientRef &&
            model.recipientId === immutableIdentity.recipientId &&
            model.runGeneration === immutableIdentity.runGeneration,
          `seed=${seed} step=${step}: stable identity changed`,
        );
        try {
          deriveAlarmStatusOracle(propertyAggregate(model));
        } catch (error) {
          throw new Error(`seed=${seed} step=${step}: aggregate projection rejected the model`, {
            cause: error,
          });
        }
      }
    }
  }, 15_000);
});
