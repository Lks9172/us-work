# Alarm delivery design

## 1. Scope and design drivers

This system creates, starts, cancels, and reads two alarm types:

- `BULK`: all users, approximately 100,000 recipients discovered through a cursor API.
- `EMERGENCY`: 1 to 100 exact recipient IDs supplied by the caller.

Both types use the provider's shared limit of 50 message POSTs per second. Emergency work must
overtake queued bulk work even while bulk delivery is active. API commands, three worker processes,
lease recovery, and provider callbacks can concurrently affect the same Alarm.

The design therefore prioritizes these invariants:

1. No Delivery can be claimed before its complete local audience is published.
2. A stale lease owner cannot change current state.
3. A cancellation committed before `STARTED` prevents that attempt from starting.
4. A provider request with an ambiguous outcome is never blindly posted again.
5. All workers share one fail-closed admission schedule.
6. Alarm counters and each Delivery transition commit atomically.

Provider-side exactly-once cannot be guaranteed because the provider neither deduplicates
`clientRef` nor defines a bounded history-visibility delay. The implementation exposes unresolved
outcomes instead of concealing that limitation.

## 2. Runtime architecture

```text
client
  |
  v
API (Nest/Fastify) ---- PostgreSQL <---- migrate (one shot)
                            ^
                            |
                  worker x3 composition root
                    |       |        |
             expansion   dispatch   maintenance/reconciliation
                    |       |        |
                    +---- provider --+
                            ^
                            |
                         Redis
                    shared paced slots
```

The API only handles bounded database transactions. It does not enumerate users or submit provider
messages. Worker processes are controller-free Nest application contexts. Each worker can run every
background role; database predicates and fenced leases distribute work safely.

PostgreSQL is the durable source of truth. Redis only coordinates provider admission and is never
the authoritative work queue. The provider is accessed through narrow HTTP adapters behind
application ports.

## 3. Module boundaries

```text
presentation ──> application ──> domain
infrastructure ────────────────> application/domain
apps ─────────────────────────> composition only
platform ─────────────────────> business-neutral helpers only
```

- Domain owns values, validation, command policy, and evidence evaluation without Nest, SQL, Redis,
  or HTTP imports.
- Application owns use cases and outbound interfaces.
- Infrastructure implements PostgreSQL repositories, provider clients, and the Redis limiter.
- Presentation parses HTTP input and maps failures to problem details.
- API/worker/migrate own lifecycle and dependency assembly only.

Dependency Cruiser makes the important reverse dependencies build failures.

## 4. Data model

### Alarm

The aggregate-facing row stores type, content, lifecycle, audience barriers, result counters, and a
monotonic incident summary. `total` is null before audience finalization. Afterwards:

```text
sent + failed + canceled + active + unresolved = total
```

`duplicateCount` and `incidentCount` are anomaly projections and are deliberately outside that
partition. `finishedAt` is written once when no active delivery remains.

### Delivery and attempt

A Delivery is unique by `(alarm_id, recipient_id)` and owns an immutable, unique `client_ref`.
Attempts are append-only audit rows with the durable point at which the application authorized a
provider request to enter the adapter. `COMMITTED` records that authorization/intent; it is not
proof that `request.end(body)` ran or that any byte reached a socket. A Delivery holds current
claim/reconciliation leases and the current attempt pointer.

The important Delivery progression is:

```text
READY/RETRY_WAIT -> CLAIMED -> IN_FLIGHT -> SENT
                                 |          FAILED_*
                                 |          RETRY_WAIT
                                 +--------> UNKNOWN -> RECONCILING -> SENT/NEEDS_REVIEW
```

`SENT`, permanent/exhausted failure, and `CANCELED` are true business-result terminals. Later
contradictory evidence does not rewrite the result. It sets a sticky incident projection and makes
the Alarm `ATTENTION_REQUIRED` once active work is zero.

### Expansion staging

`alarm_expansion_jobs` separates stable `run_generation` from takeover
`lease_generation/token/until`. Staging recipients and domain-separated hashes of observed cursors
are keyed by the stable run. A first-page sentinel makes the initial traversal position explicit.

Success, failure, and cancellation all record durable cleanup work. Cleanup has its own generation,
token, and lease, and only deletes rows for the recorded stable run in bounded batches.

### Evidence

Each complete provider response/history observation is one append-only evidence row. Entries are
canonicalized and unioned by provider `messageId`. A recipient mismatch or incompatible evidence
has precedence over a match; two or more distinct matching IDs are a duplicate signal.

One narrow race is resolved synchronously: if a definitely-unsent result has already exposed the
same attempt as `READY`/`RETRY_WAIT` (or it has just been reclaimed as `CLAIMED`) when that attempt's
concurrent `202` arrives, the Alarm and Delivery row locks adopt the positive evidence and close the
Delivery as `SENT` in the evidence transaction. The first finalized attempt result remains in the
audit row, while the consumed evidence records why it superseded redispatch. If a new retry is
already `IN_FLIGHT`, a `PREPARED` attempt is closed as proven-not-sent with its budget refunded; a
`COMMITTED` attempt is closed with an explicit outcome-unobserved audit marker. Its later response
can fill that audit row, and any positive observation remains unconsumed for the terminal evidence
scanner to classify as compatible or duplicate. `UNKNOWN`, `NEEDS_REVIEW`, and pre-existing true
terminals retain their reconciler/scanner paths.

## 5. Command and worker flows

### 5.1 Create and start

Create validates all fields before inserting anything. Emergency recipient IDs are opaque strings:
allowed values are not trimmed, case-folded, normalized, or re-encoded.

Emergency start locks the Alarm and publishes its snapshotted recipients as priority-100 Delivery
rows in one transaction. Bulk start locks the Alarm and atomically creates the stable expansion job,
first-page sentinel, and `QUEUED` state.

Duplicate start while work is active returns the current resource. A terminal Alarm cannot be
restarted.

### 5.2 Bulk expansion and publication barrier

The expander claims a current lease, requests at most 1,000 users, and commits each page's staging
upserts, seen-cursor hash, next cursor, count delta, and lease extension together. An empty page only
ends traversal when `nextCursor` is null.

The final page transaction:

1. locks the Alarm and job and verifies stable run plus current lease fence;
2. inserts the complete run into Delivery with one set-based statement;
3. sets total/active, `audienceFinalizedAt`, and `expansionCompletedAt`;
4. marks the job complete and cleanup pending;
5. makes work visible to dispatch.

Before that commit, there are no Delivery rows to claim. Failure/cancellation finalizes a zero-sized
audience and never publishes staging recipients.

### 5.3 Priority dispatch and shared limit

Dispatch follows this order:

1. use a cheap non-locking eligible hint;
2. obtain one shared paced permit and, while activating it, prepare at most one next raw permit;
3. re-evaluate the 62-general/64-total capacity and priority, then claim one globally
   highest-priority due Delivery with `FOR UPDATE OF d SKIP LOCKED`;
4. lock the Alarm and Delivery, recheck cancel plus the durable Redis-restart barrier, and commit an
   attempt as `STARTED/PREPARED`;
5. wait until the reserved monotonic time, lock Alarm and Delivery again, recheck cancel/barrier/fence,
   durably mark the attempt `COMMITTED`, and invoke the HTTP adapter; the adapter separately proves
   whether it constructed a request object before attempting `request.end(body)`;
6. store attempt, Delivery, and Alarm counter results in one transaction.

Claiming after the permit prevents bulk work from sitting in a limiter wait queue ahead of a later
emergency. The local pool allows 64 in-flight requests but reserves the final two admissions for
priority 100. Each worker has at most one active DB admission and one raw prepared permit, so at most
two reservations are not yet provider-COMMITTED. The raw permit owns no Delivery or priority;
activation re-evaluates both, and an expired or shutdown permit is discarded before claim. Already
STARTED HTTP requests are not preempted.

The Redis Lua script uses Redis TIME, one authority hash containing `run_id` and `next_slot_ms`, a
caller UUID, 25ms slots, 50ms lead, and 100ms horizon. A separate same-slot ZSET keeps at most ten
minutes of reservation diagnostics; expiration or deletion of that ZSET never changes limiter
authority. Missing/partial authority state is COLD, never a free bucket. A COLD leader takes a
PostgreSQL advisory lock, advances a durable 1.2-second `resume_not_before`, verifies the Redis run
again, then initializes the hash. Failures deny new sends.

### 5.4 Result policy

- `202`: `SENT`; store database `acceptedAt` and response `messageId`.
- `400`: permanent failure; never retry.
- `429`: use `Retry-After`, with a separate 20-attempt/five-minute budget.
- `500/503`: definitely not sent; retry with bounded full jitter and total POST budget five.
- synchronous request-construction failure before a `ClientRequest` exists:
  `DEFINITELY_NOT_SENT`; close the attempt as `FINISHED_NOT_SENT`, roll back the provisional POST
  budget, and retry with bounded full jitter. This safe local-negative branch is evaluated before
  the fifth-POST exhaustion check. Pending positive history evidence still takes precedence.
- timeout/reset/unknown response: `UNKNOWN`; do not POST again.

`request.end(body)` synchronous throws, asynchronous request errors, resets, timeouts, aborts, and
response errors remain `AMBIGUOUS`. A connect/socket event is not used to infer remote non-receipt.
`DEFINITELY_NOT_SENT` has no HTTP status or provider-response timestamp; `finishedAt` records the
local conclusion. If cancellation is already requested, the same budget rollback settles the
Delivery to `CANCELED` instead of exposing a retry.

If a concurrent `202` for the same attempt is serialized after a `500/503` retry transition, the
positive observation closes any still-dispatchable row before it can remain eligible for another
claim. This is a result-race correction, not a general rewrite of true terminal states.

For `UNKNOWN`, history lookup uses its own fenced lease and up to eight concurrent GETs. The official
provider's empty history is non-authoritative, so the default `OPAQUE` profile never treats zero rows
as proof that no request was sent. Twelve fast observations or the 60-second fast deadline moves the
Delivery to `NEEDS_REVIEW`; a 15-minute GET-only review can still correct it to `SENT` during the
24-hour review retention window. After that window automatic GET stops while the unresolved state
remains visible. Positive or conflicting evidence is evaluated before count/time deadlines. No
opaque review path creates a new POST.

An explicit `AUTHORITATIVE_EMPTY_AFTER_GRACE` capability profile is also implemented for a future
provider contract that guarantees a bounded visibility delay. It is opt-in through
`PROVIDER_HISTORY_VISIBILITY` and validates a non-negative grace configured by
`PROVIDER_HISTORY_AUTHORITATIVE_EMPTY_GRACE_MS`; Compose deliberately defaults to `OPAQUE`. Only in
that profile, after the grace, a complete empty observation proves no side effect: cancellation
settles to `CANCELED`, otherwise a remaining POST budget permits `RETRY_WAIT`, and an exhausted budget
settles to `FAILED_RETRY_EXHAUSTED`. The ambiguous attempt's consumed POST budget is never refunded.

### 5.5 Cancellation and recovery

Cancel takes the Alarm row lock also used by STARTED creation and the provider-commit fence. If
cancel commits before the provider fence, a PREPARED attempt becomes `FINISHED_NOT_SENT` and is not
submitted. If `COMMITTED` wins first, cancellation accepts that the request may already be submitted
and later settles its actual result.

The API only records the cancellation flag. A bounded sweeper cancels READY/RETRY_WAIT and claims
without STARTED, updating counters in the same transaction. It leaves IN_FLIGHT/UNKNOWN work for
result/reconciliation.

An expired CLAIMED lease without STARTED returns to READY (or CANCELED). An expired IN_FLIGHT lease
whose current attempt is still `PREPARED` is proven pre-provider, so it becomes FINISHED_NOT_SENT and
returns to READY (or CANCELED) without consuming POST budget. Only an expired `COMMITTED` attempt
becomes UNKNOWN; it must not return to READY because the provider may have accepted it.

PostgreSQL can emit connection errors on both an idle pool client and a checked-out transaction
client. Runtime pools install a sanitized handler for the former, and every repository transaction
installs a scoped handler for the latter and removes it after release. This prevents an unhandled
EventEmitter error from terminating the worker. It does not convert a failed query into success:
the repository operation still fails, the transaction rolls back, and durable leases plus fencing
determine safe recovery after connectivity returns.

### 5.6 Dispatch observability

Every durable attempt emits newline-delimited JSON after `STARTED`, after a proven local no-send, and
after the provider result transaction commits. Records carry a process-unique worker ID plus
`alarmId`, `deliveryId`, `attemptId`, `clientRef`, attempt number, lease generation, reservation ID,
and outcome. A provider-final event also carries the non-negative monotonic duration measured only
around `sender.send`, plus `providerDurationSource=MONOTONIC_SENDER_BOUNDARY`; it is not inferred from
wall-clock database timestamps. The attempt ID is the trace ID for this asynchronous dispatch span. Recipient IDs,
message content, names, phone numbers, and raw provider bodies are never logged. A log sink failure is
isolated from the state machine and cannot prevent or duplicate a send.

The maintenance application port also exposes a DB-derived operational snapshot and a committed
send-lease recovery event. The PostgreSQL adapter uses the existing session advisory leader so only
one of N workers emits the global snapshot. The processor schedules it from a monotonic clock at a
60-second cadence even while normal maintenance work stays busy and advances the next deadline
before I/O. A single non-overlapping observation task is fire-and-forget from the durable loop, so a
slow snapshot or consistency query cannot delay cancellation sweep or expired-lease recovery; only
runtime shutdown drains it before releasing the leader connection and pool. Query/sink failures are
contained. One statement reads terminal totals from Alarm's same-transaction projection, scans only
selective non-terminal Delivery indexes for queue/reconciliation/lease gauges, and uses a partial
covering `delivery_attempts(finished_at)` index for last-window attempt/accepted/429 counts. Recovery
events are emitted only after the recovery transaction returns from COMMIT and identify
CLAIMED/PREPARED/COMMITTED-or-missing-attempt handling without a recipient or lease token.

Every worker-side structured record also carries the same platform-owned, boot-unique
`processInstanceId`. Lifecycle, dispatch, expansion, maintenance, reconciliation failure, and
PostgreSQL client records therefore correlate to one process; the identifier rotates only when the
process restarts and contains no Alarm or recipient data.

## 6. Alarm aggregation precedence

After audience finalization, Alarm status is derived with this precedence:

1. `active > 0`: `SENDING` or `CANCEL_REQUESTED`;
2. unresolved, duplicate, or incident remains: `ATTENTION_REQUIRED`;
3. cancel requested: `CANCELED`;
4. no failure: `COMPLETED`;
5. sent and failed both nonzero: `PARTIALLY_FAILED`;
6. otherwise: `FAILED`.

This active-first ordering prevents a late anomaly on one Delivery from making an Alarm look finished
while other messages are still being sent.

## 7. Design decisions and alternatives

Only decisions with meaningful alternatives are compared here. Provider-defined wire fields and
routine file placement do not get artificial candidate lists.

The `D` number is local to this public narrative. `ADR-xx` is the canonical identifier used by
`docs/requirements-traceability.json`; the two must not be compared by number alone. The following
crosswalk also makes every candidate's requirement fit explicit. “Misses” means a material gap for
the listed requirement or problem, not that the candidate is universally invalid. Cross-cutting
`R-CHOICE-01` and `R-AI-01` apply to all 25 decisions and are not repeated in every row;
`R-TEST-01` applies to runtime ADR-01 through ADR-24.

| Public decision / canonical ID | Requirement or problem | Candidate fit: what meets and misses | Adopted conclusion |
| --- | --- | --- | --- |
| D1 / ADR-01 | `R-STACK-01`, `R-WORKER-01`, `R-COMPOSE-01`: separate API/worker without three-day contract drift | Separate repositories meet release isolation but miss the bounded implementation/contract-sharing cost. One HTTP process meets simplicity but misses process separation. One modular codebase with separate bootstraps meets both required separation and shared policy. | Modular monolith, separate API/worker processes. |
| D2 / ADR-02 | `R-USERS-02`, `R-WORKER-01`, `R-CONC-01`, `O-MWK-01`: atomically create work and business state | RabbitMQ/SQS meet queue routing but miss atomic DB enqueue without an outbox. Redis Streams meet consumer distribution but create a second business-state authority. PostgreSQL meets transactional claim/cancel/fencing at this scale, with polling/bloat cost. | PostgreSQL durable rows with `SKIP LOCKED`. |
| D3 / ADR-04 | `R-USERS-01`, `R-USERS-02`, `R-CONC-01`: publish no partial 100,000-user audience | Per-page Delivery meets first-send latency but misses atomic audience publication. In-memory collection avoids partial publication but misses bounded memory/restart safety. Durable staging plus final set-based publication meets all three correctness constraints. | Stage pages, publish once. |
| D4 / ADR-06 | `O-PRIO-01`, `R-RATE-01`, `O-MWK-01`, `R-CONC-01`: urgent overtaking plus 38–40/s efficiency | Strict bound-one meets the safety bound but misses measured efficiency. Claim-first batches meet throughput but miss urgent overtaking and enlarge kill recovery. Separate limiters miss the shared 50/s budget. One active admission plus one rowless raw permit meets priority and utilization, with bounded wasted permits/starvation risk. | Exact bound-two permit-only pipeline. |
| D5 / ADR-07 | `R-RATE-01`, `R-PROD-01`, `O-MWK-01`: all processes below a rolling 50 POST/s | Per-worker buckets miss aggregate coordination. Fixed windows/token buckets share state but can miss the no-boundary-burst interpretation. A PostgreSQL hot row meets atomicity but competes with business transactions. Redis paced slots meet global clock, atomicity, and no burst, but sending stops when Redis is uncertain. | Redis TIME paced slots at 40/s. |
| D6 / ADR-12 | `R-CONC-01`, `R-USERS-02`: auditable lock/CAS/set-based SQL | A full ORM meets CRUD productivity but misses visibility for most correctness-critical SQL. ORM plus raw queries meets Nest integration but creates two mapping models. `pg` meets explicit lock/fence/plan inspection, at the cost of boilerplate. | `pg` repositories and immutable SQL migrations. |
| D7 / ADR-09 | `R-AMB-01`, `R-DEDUP-01`, `R-MSG-01`, `R-PROD-01`: do not hide ambiguous side effects | Immediate retry meets availability but misses duplicate safety. Permanent failure meets no-repost safety but misses recovery of true non-delivery. Stable `clientRef` plus history-first reconciliation meets evidence-based recovery, while honestly leaving opaque empty history unresolved. | `UNKNOWN` → reconciliation → `NEEDS_REVIEW` when unresolved. |
| D8 / ADR-11 | `R-USERS-02`, `R-DATA-01`, `R-CONC-01`: O(1), transactionally consistent detail reads | Query-time counts meet single-source simplicity but miss bounded 100,000-row reads. Async projection meets fast reads but misses strong consistency. Same-transaction counters meet both, with one Alarm-row contention point. | Stored counters plus read-only drift detection. |
| D9 / ADR-15 | `R-DATA-01`, `R-CONC-01`, `R-DEDUP-01`: expose late contradictory evidence without rewriting history | Rewriting terminal results meets “latest observation” but misses audit monotonicity. Log-only handling preserves results but misses API-visible risk. A sticky incident projection meets both preservation and visibility, with a more complex status model. | Preserve result; add monotonic incident projection. |
| D10 / ADR-17 | `R-PROD-01`, `R-README-01`, `R-CONF-01`: PII-safe multi-worker operations | Free-form logs meet zero setup but miss schema/PII/correlation guarantees. A mandatory metrics/tracing stack meets aggregation but adds a new topology and failure contract. Typed JSON events plus one advisory-lock snapshot leader meet testable signals without an availability dependency, but not alert delivery or distributed traces. | Typed NDJSON events and leader-owned snapshots. |
| D11 / ADR-16 | `R-PROD-01`, `R-CONC-01`: survive DB connection events without false success | No listener delegates recovery to restart and misses process continuity. A Pool-only listener misses checked-out transaction clients. Pool plus scoped client listeners meet EventEmitter safety while preserving query rejection/rollback. | Handle both event sources; never swallow query failure. |
| D12 / ADR-18 | `R-CONC-01`, `R-AMB-01`, `R-DEDUP-01`: late positive versus redispatch race | Letting retry win meets progress but misses duplicate minimization. Append-and-scan preserves evidence but misses the pre-scan re-POST window. Adoption under the dispatch lock meets the linearization need; an already committed duplicate remains irreversible and is recorded. | Consume positive evidence at the Alarm→Delivery→Attempt lock. |
| D13 / ADR-19 | `R-OAS-01`, `R-API-01`, `R-ERROR-01`: one API-contract authority | A repository file meets submission but misses runtime discovery. Runtime decorator generation meets discovery but creates a second source. Serving the validated checked-in bytes meets both, with CDN-only UI assets as a deployment cost. | Serve canonical `openapi.json` directly. |
| D14 / ADR-20 | `R-PROD-01`, `R-CONC-01`: avoid outage retry storms without slow recovery | Immediate/fixed-short retry meets fast recovery but misses bounded dependency pressure. Unbounded exponential meets pressure control but misses bounded recovery latency. Abortable 250ms–5s exponential meets both bounds, though replicas may still synchronize. | Shared abortable bounded exponential backoff. |
| D15 / ADR-21 | `R-PROD-01`, `R-RUN-01`, `R-COMPOSE-01`: distinguish API control-plane readiness from worker-path health | All-dependency readiness gives one signal but unnecessarily removes a usable API during Redis/provider failure. Process-only readiness misses DB command capability. Direct-dependency readiness meets API truth while worker fail-closed signals cover its own path. | API readiness checks PostgreSQL; startup still gates the full topology. |
| D16 / ADR-13 | `R-MOCK-01`, `R-MSG-01`, `R-RATE-01`, `R-AMB-01`: prove the retry-safe send boundary | Treating every adapter error as ambiguous meets duplicate safety but unnecessarily strands provable pre-request failures. Inferring non-delivery from socket errors misses the no-blind-retry guarantee. Only classifying synchronous request-construction failure as definitely not sent meets safety and limited liveness. | Narrow Node HTTP adapter phase contract. |
| D17 / ADR-03 | `R-DATA-01`, `R-CONC-01`, `R-AMB-01`: separate aggregate, logical result, and actual attempts | Alarm-only state meets aggregate display but misses recipient outcomes. Overwriting the latest result on Delivery meets recipient status but misses attempt/crash evidence. Alarm+Delivery+Attempt meets all three responsibilities, with more transitions. | Three-level state model. |
| D18 / ADR-05 | `O-EMG-01`, `R-DATA-01`: exact 1–100 recipients while preserving the start command | An Alarm JSON array meets storage simplicity but misses relational uniqueness/traceability. Create-time Delivery meets exact recipients but makes them dispatchable before start semantics. Snapshot rows materialized at start meet both identity and command boundaries. | Create-time exact snapshot, start-time Delivery publication. |
| D19 / ADR-08 | `R-CONC-01`, `R-PROD-01`, `R-SLOW-01`, `O-MWK-01`: reject stale writes without holding locks across HTTP | Holding a row lock across HTTP meets serialization but misses connection/lock availability. Owner strings plus heartbeat meet basic ownership but miss ABA/late-result fencing. Lease generation+random token+CAS meets short transactions and stale rejection, at reaper/heartbeat complexity. | Fenced leases with generation and token. |
| D20 / ADR-10 | `R-API-01`, `R-CONC-01`: define an honest cancel/send race boundary | Aborting sockets then declaring canceled meets user immediacy but cannot prove no side effect. Rejecting cancellation after first send meets simplicity but misses useful cancellation. Alarm-lock serialization meets “no new start/commit after cancel wins” while admitting prior committed work may finish. | Best-effort, linearly fenced cancellation. |
| D21 / ADR-14 | `R-TRANSIENT-01`, `R-SLOW-01`, `R-PERM-01`, `R-RATE-01`, `R-AMB-01`: bounded policy by failure meaning | One short timeout/max-attempt count meets simplicity but misses valid delayed-202 and conflates 429/5xx/ambiguity. Unlimited retry misses termination and provider protection. Separate timeout, lease, 5xx, 429, and reconciliation budgets meet the distinct semantics, with configuration/measurement cost. | 35s/60s timing relation and error-specific budgets. |

Canonical ADR-22 through ADR-24 are repository-wide decisions rather than additional delivery
algorithms: pre-bootstrap signal preservation, one-shot checksummed migration ownership, and the
private/public artifact boundary. Their candidate fit is summarized after D21. The earlier scaffold
adoption decision is independently recorded in
[ADR-0001](../adr/0001-adopt-validated-repository-scaffold.md).

### D1. One codebase, separate API and worker processes

- Candidates: separate services/repositories; one HTTP process with background timers; one modular
  codebase with separate bootstraps.
- Chosen: one modular codebase/image, separate API and controller-free worker bootstraps.
- Rationale: it satisfies failure/resource separation while avoiding duplicated contracts and build
  setup for a small bounded context.
- Cost: both processes deploy from the same release and the worker contains roles with different
  resource profiles.
- Revisit when: teams or scaling profiles need independent releases.

### D2. PostgreSQL durable queue instead of a message broker

- Candidates: PostgreSQL rows; RabbitMQ/SQS; Redis Streams.
- Chosen: PostgreSQL rows with partial indexes and `SKIP LOCKED`.
- Rationale: command state and work are created in one transaction, avoiding a DB/broker dual-write
  and keeping recovery evidence queryable.
- Cost: polling and queue traffic share the primary database; broker-native routing/DLQ features are
  absent.
- Revisit when: queue load threatens OLTP, cross-service fan-out appears, or independent retention is
  required. Adopt a transactional outbox before adding a broker.

### D3. Staged bulk audience publication

- Candidates: create Delivery per page; collect all users in memory; stage pages then publish once.
- Chosen: durable staging plus set-based final publication.
- Rationale: page-at-a-time Delivery would expose a partial audience, while memory collection makes
  API/worker crash recovery and 100,000-user memory boundedness worse.
- Cost: additional tables, cleanup, and one larger final transaction.
- Revisit when: the provider offers a versioned audience snapshot or streaming batch-send primitive.

### D4. Strict priority with permit-only pipelining

- Candidates: (A) serialize one reservation through claim/STARTED/COMMITTED, keeping a strict bound of
  one but leaving admission-cycle gaps; (B) overlap one active DB admission with one raw next permit,
  keeping DB admission at one and the total pre-COMMITTED reservation bound at two; or use separate
  queues/limiters and claim-first batching.
- Chosen: candidate B with one JIT priority claim, activation-time priority re-evaluation, and two
  emergency-only local slots. The strict bound-one candidate measured 36.712--37.470/s with three
  workers against the 38--40/s efficiency target because the 50ms lead left slot gaps.
- Rationale: separate limiters can violate the shared 50/s budget; claim-first can park bulk work in
  front of urgent work. Permit-only pipelining fills the global 25ms schedule without preclaiming a
  row or weakening the rolling limit.
- Cost: a crash or stale eligibility race can waste up to two non-refundable reservations per worker,
  and sustained emergency load can starve bulk. A null claim discards the speculative permit and
  applies a 50ms quarantine to avoid permit-only self-saturation.
- Revisit when: product defines a maximum emergency share or bulk deadline; add quota/aging then.

### D5. Redis paced slots

- Candidates: per-worker local limit; shared fixed-window/token bucket; a PostgreSQL hot scheduling
  row; Redis paced slots.
- Chosen: Redis server-time paced slots at 40/s with burst zero.
- Rationale: local limits do not coordinate replicas and boundary bursts from fixed windows can exceed
  a rolling provider limit. A PostgreSQL hot row is atomic but competes with business transactions at
  the 25ms scheduling cadence. Redis pacing gives a shared clock, explicit no-burst scheduling, and
  headroom.
- Cost: Redis availability becomes part of sending availability and cold recovery is cross-store.
- Revisit when: the provider offers centralized quotas/admission or accepts a documented burst.

### D6. Direct SQL repositories and versioned migrations

- Candidates: full ORM; ORM/query builder for routine mapping plus raw SQL for critical paths; `pg`
  with explicit SQL throughout the bounded context.
- Chosen: `pg` and immutable migrations.
- Rationale: the hybrid option keeps framework convenience but creates two mapping and transaction
  models because most correctness-critical paths still need raw SQL. Direct `pg` keeps lock targets,
  transaction boundaries, `SKIP LOCKED`, partial indexes, and fencing predicates visible and testable.
- Cost: more mapping code and manual schema evolution.
- Revisit when: routine CRUD dominates and the selected abstraction preserves explicit escape hatches.

### D7. History-first ambiguous outcome handling

- Candidates: retry immediately; fail permanently; reconcile by stable `clientRef`; allow retry
  after an explicitly contracted authoritative-empty grace.
- Chosen: durable UNKNOWN plus GET-only reconciliation, defaulting to opaque empty history. The
  authoritative-empty branch is a named opt-in capability rather than an inference from a zero-row
  response.
- Rationale: immediate retry can duplicate a sent notification; permanent failure can silently omit an
  unsent notification. History is the only available evidence channel, and capability-gating keeps a
  stronger future provider contract usable without weakening the official provider's safe default.
- Cost: non-authoritative empty history may leave NEEDS_REVIEW indefinitely after the bounded
  automatic-review window ends. A wrongly configured authoritative profile could create a duplicate,
  so deployments must not select it without the provider guarantee.
- Revisit when: provider supplies idempotent POST or a bounded authoritative lookup contract.

### D8. Stored counters in the command transaction

- Candidates: query-time count over Delivery; asynchronous projection/outbox; same-transaction Alarm
  counters.
- Chosen: counter deltas and Delivery state commit together.
- Rationale: detail lookup remains O(1) and cannot observe a Delivery result without its matching
  aggregate snapshot.
- Consistency guard: one pass is attempted per 300 idle maintenance iterations (about 60 seconds at
  the current 200 ms idle delay). A session-scoped PostgreSQL advisory lock elects one worker for
  the cluster-wide source-row scan; connection loss releases leadership and a later pass can take
  over. The leader compares the stored projection with source Delivery states read-only, emits a
  deduplicated structured alert on drift, and never silently substitutes or repairs the public query
  result. Repair would require a separate locked, versioned, audited command.
- Cost: every terminal transition also locks the Alarm, creating bounded per-Alarm contention; the
  detector adds one dedicated pooled connection on the elected worker and an infrequent idle-time
  aggregate scan used for corruption detection rather than the read path.
- Revisit when: one Alarm's write rate exceeds the row-lock budget; shard counters or introduce an
  outbox with explicit eventual consistency.

### D9. Preserve terminal business results and project incidents

- Candidates: overwrite terminal status from late evidence; ignore late evidence; retain result and
  add monotonic incident projection.
- Chosen: retain result/counters/first finish time, expose an incident and duplicate summary.
- Rationale: overwriting destroys the original operational fact; ignoring evidence hides risk.
- Cost: status and incident dimensions are more complex.
- Revisit when: an operator-led correction workflow defines a new audited business event.

### D10. Structured worker events on standard output

- Candidates: human-oriented framework logs; newline-delimited JSON on standard output; a mandatory
  tracing/metrics SDK and collector.
- Chosen: narrow application event ports with infrastructure JSON sinks on standard output. Dispatch
  emits attempt lifecycle and exact sender-boundary latency sample events; maintenance emits one
  leader-owned operational snapshot plus committed recovery-reason events; expansion emits aggregate
  counts when a committed page contains duplicate recipient IDs. API, worker, and migrate composition
  roots emit bounded startup/readiness/shutdown/migration events. None includes a recipient ID, cursor,
  message body, provider raw payload, error message, name, phone number, or connection string.
- Rationale: container runtimes can collect it without adding a second availability dependency, and
  the explicit event shape makes correlation and PII exclusion testable.
- Cost: attempt-level logs are high-volume. The repository deliberately has no second HTTP server or
  pull-style `/metrics`; logs require a collector for histograms, alerts, and cross-service traces.
- Revisit when: an OpenTelemetry collector is part of the deployment contract; keep the application
  port and replace or fan out the sink with sampled traces and metrics.

### D11. Contain PostgreSQL connection events without hiding query failure

- Candidates: allow driver error events to terminate the process; handle only idle pool errors;
  handle pool errors and attach a scoped listener to each checked-out transaction client.
- Chosen: handle both idle and active-client events, while preserving ordinary query rejection and
  transaction rollback.
- Rationale: a database restart can emit `error` from either object. Handling only the pool still
  leaves an active transaction able to crash a worker, while treating the event as a successful
  operation would violate state-machine safety.
- Cost: every transaction has a small listener lifecycle and connection loss still reduces
  availability until PostgreSQL recovers.
- Revisit when: the database driver guarantees one centralized, non-fatal error channel with the
  same checked-out-client semantics.

### D12. Resolve same-attempt retry/positive races at the dispatch lock

- Candidates: let the retry win and ignore the positive; append evidence for a later scanner; adopt
  the positive while the row is still dispatchable.
- Chosen: append the observation, then atomically consume it and transition a
  `READY`/`RETRY_WAIT`/`CLAIMED` or redispatch `IN_FLIGHT` Delivery to `SENT` under the existing
  Alarm-to-Delivery-to-Attempt lock order.
- Rationale: ignoring a verified positive can duplicate a notification, while asynchronous adoption
  leaves a claim/re-POST window. A new `PREPARED` attempt is safe to abort; a new `COMMITTED` attempt
  may already be duplicated, so it is audit-closed explicitly and a later positive response is sent
  through the terminal incident scanner rather than rewriting the business result.
- Cost: one attempt audit row can retain its first definite-unsent response while the Delivery is
  corrected by a separately named positive evidence decision. A superseded committed retry can be
  audit-closed before its response, using an explicit outcome-unobserved marker that a later response
  may replace.
- Revisit when: the provider offers authoritative idempotency or a single immutable result per
  client reference.

### D13. Serve the checked-in OpenAPI contract

- Candidates: keep only a repository file; generate a second document from decorators at runtime;
  serve the checked-in canonical document with a thin Swagger UI.
- Chosen: `/docs/openapi.json` returns the same checked-in `openapi.json` that contract tests validate,
  and `/docs` is a thin Swagger UI that reads that endpoint.
- Rationale: generating a second representation would create a new drift direction. Serving the
  validated artifact makes the browser, tests, and repository review use one contract.
- Cost: the lightweight UI loads pinned-major Swagger assets from a CDN and therefore needs browser
  network access; JSON remains available offline. The endpoints share the API's exposure boundary.
- Revisit when: the service is externally deployed; self-host the assets with CSP and put both
  endpoints behind gateway authorization or disable them at the gateway.

### D14. Bound worker dependency-outage retries

- Candidates: retry each loop immediately; wait a fixed interval; use unbounded exponential delay;
  use bounded exponential delay and reset it after a successful repository iteration.
- Chosen: every worker role starts at 250 ms, doubles consecutive-failure delay up to 5 seconds, and
  resets to the base after the next successful iteration. Shutdown can abort every wait.
- Rationale: an immediate or fixed tight loop can amplify a PostgreSQL outage, while an unbounded
  delay makes recovery unnecessarily slow after the dependency returns. One shared neutral helper
  keeps dispatch, expansion, maintenance, and reconciliation behavior consistent without moving an
  Alarm concern into the platform package.
- Cost: the deterministic delay can align retries across the current three replicas. At this scale
  the cap bounds both retry pressure and recovery delay, but it is not a general anti-herd policy.
- Revisit when: replica counts or measured reconnect pressure grow; add injected per-process jitter
  while retaining the same cap and deterministic test seam.

### D15. Keep API readiness scoped to direct dependencies

- Candidates: make readiness depend on PostgreSQL, Redis, and the provider; expose process-only
  readiness; or let each process report only dependencies required to serve its own traffic.
- Chosen: Compose gates the initial API/worker start on migration plus PostgreSQL, Redis, and Mock
  health. After startup, API liveness reports process survival and API readiness probes PostgreSQL,
  its direct request-path dependency. Redis and provider failures instead stop worker admission and
  provider dispatch fail-closed; they do not make a healthy read/command API falsely unready.
- Rationale: coupling API readiness to worker-only dependencies would remove a usable status/control
  plane during a delivery-path outage and could amplify the incident through unnecessary API
  restarts. The startup gate still prevents traffic before the complete local topology is usable.
- Cost: one endpoint is not a whole-system health summary. Operations must combine API readiness
  with worker structured events and provider-ingress evidence when diagnosing dispatch.
- Revisit when: a dedicated worker-health/aggregate service-health endpoint is added with an
  explicit consumer and availability contract.

### D16. Admit one explicit pre-request not-sent result without inferring socket delivery

- Candidates: classify every adapter-side exception as ambiguous; infer non-delivery from selected
  socket/connect errors; or expose a distinct result only for the synchronous boundary where HTTP
  request construction throws before returning a `ClientRequest` and before `request.end` is called.
- Chosen: the application port includes `DEFINITELY_NOT_SENT`, and the Node adapter emits it only
  for that narrow synchronous construction boundary. `request.end` throws and every asynchronous
  transport/timeout/reset/abort path remain `AMBIGUOUS`.
- Rationale: treating a provable local pre-request failure as ambiguous can strand a notification in
  review even though ingress is impossible. Treating socket events as proof of non-delivery risks a
  blind duplicate. The narrow result restores liveness without weakening the at-most-once safety
  choice at uncertain network boundaries.
- Cost: `COMMITTED` means durable authorization/intent rather than proof of body handoff, so the
  attempt audit needs the later `FINISHED_NOT_SENT` phase and an explicit budget rollback. Adapter
  implementations must not emit this result without an equally strong pre-handoff proof.
- Revisit when: the provider accepts an idempotency key or the runtime exposes a stronger, documented
  atomic handoff acknowledgement; broader retry rules then require a separate contract decision.

### D17. Keep Alarm, Delivery, and Attempt as separate state responsibilities

- Candidates and fit: Alarm-only state satisfies aggregate display but cannot represent recipient
  outcomes or retry evidence. A Delivery row whose latest HTTP result is overwritten satisfies a
  recipient summary but cannot distinguish a stale response from the current attempt. Separate
  Alarm, Delivery, and append-only Attempt records satisfy aggregate, logical-result, and physical-
  attempt responsibilities.
- Chosen: the three-level model, with current attempt authority fenced on Delivery.
- Rationale: cancellation, timeout, retry, worker death, and late evidence must be explainable without
  rewriting the fact that a previous provider attempt existed.
- Cost: more states and transactional guards; every legal transition needs an invariant test and a
  repository-level atomic implementation.
- Revisit when: the provider supplies an idempotent write plus a linearizable, immutable outcome that
  makes local attempt evidence redundant.

### D18. Snapshot emergency recipients at create and publish them at start

- Candidates and fit: storing one JSON array satisfies compact persistence but weakens database
  uniqueness and recipient-level audit. Creating Deliveries during create satisfies exact identity
  but violates the command boundary by creating dispatchable work before start. A normalized exact
  snapshot followed by start-time set publication satisfies both constraints.
- Chosen: validate and snapshot 1–100 opaque IDs in the create transaction; materialize Deliveries
  with the audience barrier and counters in the start transaction.
- Rationale: recipient intent survives a restart, while start remains the only operation that creates
  work eligible for dispatch. No 100,000-user directory scan is needed.
- Cost: one small table and a materialization step; unknown IDs are classified by the provider rather
  than prevalidated through the all-users endpoint.
- Revisit when: emergency audiences become large enough to require their own paged ingestion or the
  product changes to create-and-send semantics.

### D19. Fence ownership with lease generation and random token

- Candidates and fit: holding the database row lock across provider HTTP satisfies serialization but
  fails connection-pool, cancellation-latency, and multi-worker availability needs. A worker ID plus
  heartbeat satisfies basic liveness but cannot robustly reject an ABA owner or a late result after
  takeover. A lease with monotonically increasing generation, random token, and current-attempt CAS
  satisfies short transactions and stale-write rejection.
- Chosen: fenced leases for dispatch, expansion, reconciliation, and cleanup; no database row lock is
  held during external HTTP.
- Rationale: a killed worker may resume or deliver a late response after another worker has recovered
  the row. Generation and token make that old authority fail a zero-row CAS instead of overwriting
  current state.
- Cost: heartbeat, expiry recovery, and explicit stale-result evidence paths are required.
- Revisit when: a selected durable queue provides equivalent generation fencing and the database no
  longer owns the business-state transition.

### D20. Serialize cancellation against STARTED and COMMITTED fences

- Candidates and fit: force-aborting sockets and immediately declaring `CANCELED` satisfies a fast UI
  response but cannot prove the provider saw no request. Rejecting cancel after any send has started
  satisfies simple accounting but fails the required useful cancellation behavior. Serializing the
  cancel flag, attempt start, and provider commit under the Alarm-first lock order satisfies a precise
  best-effort boundary.
- Chosen: after cancel commits, no new attempt may start and no still-`PREPARED` attempt may become
  provider-`COMMITTED`; work that committed earlier is settled from response or history evidence.
- Rationale: this is the strongest guarantee available without a provider revoke API, and it does not
  mislabel a possibly delivered message as canceled.
- Cost: counters can still move after the cancel command because pre-existing committed work is being
  truthfully settled; cancellation finishes through bounded sweeps.
- Revisit when: the provider exposes a revoke operation with a documented side-effect linearization
  point.

### D21. Separate timeout, lease, 5xx, 429, and ambiguity budgets

- Candidates and fit: one short timeout and one `maxAttempts` value satisfy implementation simplicity
  but reject the valid 30-second delayed response and erase the distinction between provider
  backpressure, known non-delivery, and unknown delivery. Unlimited retry satisfies optimistic
  liveness but fails termination and provider protection. Error-specific bounded policies satisfy the
  supplied fault meanings.
- Chosen: 35-second response timeout; 60-second send lease with 10-second heartbeat and 5-second
  reaper cadence; five total ordinary POST attempts for known-unsent 5xx; a separate 429 count/time
  budget; and GET-only reconciliation for ambiguous outcomes.
- Rationale: each budget consumes only evidence that has the corresponding meaning. In particular,
  an ambiguous outcome never becomes a 5xx-style repost just because time passed.
- Cost: configuration relationships must be checked at startup and recalibrated from real latency and
  Retry-After observations.
- Revisit when: provider SLA, idempotency, or authoritative history contracts change, or measured
  latency makes the current inequalities invalid.

### Repository-wide canonical decisions outside the D series

These decisions use canonical IDs because they are referenced by the requirements manifest, but
they are not given new `D` numbers: they govern all process entrypoints or artifact delivery rather
than the alarm dispatch algorithm.

| Canonical decision | Requirement/problem | Candidates: meets and misses | Adopted, cost, and revisit condition |
| --- | --- | --- | --- |
| ADR-22: pre-bootstrap signal preservation | `R-PROD-01`, `R-COMPOSE-01`, `R-WORKER-01`; do not lose SIGTERM during the first async bootstrap | Registering after app creation meets steady-state shutdown but misses an early signal. Per-entrypoint handlers meet local control but invite drift and duplicate close. One coordinator registered before the first await meets signal preservation and one-close semantics. | Preserve/coalesce the first signal, attach the app close action later, and drain once. Hard kill still cannot drain. Revisit if the runtime guarantees pre-bootstrap handoff and a bounded drain deadline. |
| ADR-23: one migration owner | `R-RUN-01`, `R-COMPOSE-01`, `R-PROD-01`; avoid schema races during bare Compose boot | API/worker bootstrap DDL meets no-extra-service simplicity but misses single ownership under scale. External-only migration meets production separation but misses one-command local acceptance. A checksummed one-shot service meets both single ownership and bare startup. | Gate API/worker on the migrate service. Its failure intentionally blocks traffic. Revisit when the deployment platform supplies an equivalent verified pre-deploy job and rollout gate. |
| ADR-24: private/public artifact boundary | `R-CONF-01`, `R-SUBMIT-01`, `R-AI-01`, `R-README-01`; retain rationale without publishing protected source material | Deleting all private evidence meets public hygiene but misses local auditability. Publishing it meets traceability but violates confidentiality. Ignored local evidence plus self-contained sanitized public artifacts meets both, with a required reference/secret/image-context scan. | Keep the two artifact classes separate. A clean clone lacks private notes by design. Revisit only with explicit redistribution permission or an access-controlled documentation vault. |

### Public P0 decision verification index

The candidate comparison above explains why each decision was made. This index closes the reverse
link from every canonical decision to scenario IDs contained in the public P0 traceability manifest;
expected results remain in executable tests and the manifest evidence groups rather than being
duplicated here. Additional P1/P2 strengthening scenarios are intentionally excluded from this
clean-clone index.

| Canonical decision | Public P0 verification IDs |
| --- | --- |
| ADR-0001 repository scaffold | `STACK-01`, `ARC-02`, `OPS-02`, `DOC-14`, `DOC-17`, `SUB-01~03` |
| ADR-01 process/module boundary | `STACK-01`, `ARC-02`, `OPS-04`, `OPS-08`, `MWK-01` |
| ADR-02 PostgreSQL queue | `DB-01~02`, `DB-04~08`, `DB-11`, `LF-01~08`, `MWK-02~04`, `MWK-06~13`, `PERF-01` |
| ADR-03 three-level state | `U-SM-01~04`, `CR-01~08`, `AGG-01~10`, `AGG-12~19` |
| ADR-04 bulk barrier | `USR-01~02`, `USR-04~05`, `USR-08~09`, `USR-11~17`, `SND-11`, `AGG-15`, `CAN-11`, `MWK-03`, `PERF-01`, `PERF-04A`, `PERF-04C` |
| ADR-05 emergency snapshot | `CRT-08~11`, `SND-02`, `EMG-01~02` |
| ADR-06 priority admission | `EMG-03~10`, `PRI-01~03`, `PRI-05~06`, `XFT-01` |
| ADR-07 Redis paced limiter | `RL-01~06`, `RL-08`, `RL-11`, `RED-01~02`, `RED-04`, `RED-06~07`, `XFT-01` |
| ADR-08 lease/fencing | `LF-01~08`, `CR-01~08`, `MWK-03~04`, `MWK-06~13` |
| ADR-09 reconciliation | `RET-10~15`, `RET-20~23`, `LF-07`, `EO-01~03`, `CR-04~07` |
| ADR-10 cancellation | `CAN-01~16`, `XFT-01` |
| ADR-11 stored counters | `U-AGG-01`, `AGG-01~10`, `AGG-12~19`, `MWK-09` |
| ADR-12 database access | `DB-01~02`, `DB-04~08`, `DB-11`, `OPS-09`, `OPS-16` |
| ADR-13 HTTP send phase | `RET-17`, `RL-01~06`, `RL-08`, `RL-11`, `MCK-11`, `MCK-24~25` |
| ADR-14 timing/retry | `RET-02~05`, `RET-07~08`, `RET-10~18`, `RET-20~21`, `LF-05` |
| ADR-15 terminal incident projection | `AGG-16~19`, `LF-04`, `RET-14`, `RET-21`, `RET-23` |
| ADR-16 PostgreSQL connection events | `DB-12`, `OPS-15` |
| ADR-17 PII-safe observability | `OBS-01` |
| ADR-18 retry/positive race | `RET-10`, `RET-17`, `RET-20`, `RET-23`, `AGG-10`, `AGG-17~19` |
| ADR-19 OpenAPI authority | `DOC-01~06` |
| ADR-20 dependency backoff | `ERR-08`, `OPS-03`, `OPS-15~18` |
| ADR-21 direct-dependency readiness | `OPS-03` |
| ADR-22 startup signal lifecycle | `OPS-03` |
| ADR-23 one-shot migration owner | `OPS-02`, `OPS-09`, `OPS-16` |
| ADR-24 public/private artifact boundary | `DOC-14`, `DOC-17`, `SUB-01~07` |

## 8. Verification strategy

- Unit/contract: input boundaries, command policy, evidence precedence, retry/admission behavior,
  provider parsing, OpenAPI route/schema coverage, layer boundaries, and a deterministic test-only
  reference table for every guard-resolved Delivery state/event pair.
- PostgreSQL integration: full schema dictionary inspection, previous-version data-preserving
  upgrade, concurrent migrator serialization, transactional migration rollback/recovery, concurrent
  claims, lease fencing, publication barrier, cancellation, reaping, reconciliation, terminal
  evidence, cleanup, and table-driven comparison of repository outcomes with the reference table.
  The lock-order check also races two production `startAttempt` calls on fresh READ COMMITTED
  connections behind a deliberate pre-commit barrier and requires one attempt/budget winner.
- Redis integration: shared pacing, COLD detection, restart barrier, unique reservations.
- HTTP E2E: create/list/detail/start/cancel, status codes, signed pagination, problem details,
  documented request examples, Swagger UI, and the served canonical OpenAPI JSON.
- Compose black box: base one-command boot, migration success/failure startup gating, three workers,
  priority overtaking, shared rate limiting, and representative worker-kill recovery. Timestamp-audit
  controlled-provider evidence and supplied-provider standard API/history evidence stay separate;
  the current-revision supplied-provider in-flight kill gate remains `partial` in the README matrix.

Performance results are environment-dependent. The implementation fixes bounded algorithms and
indexes; it does not present an unmeasured completion-time number as a guarantee.

The reference table is test code, not a parallel production decision source. Runtime authority
remains the fenced PostgreSQL repository transactions. An oracle-only row therefore proves the
conceptual invariant/property layer; claims about a persisted edge additionally require a production
repository integration case.

Crash-boundary integration uses real PostgreSQL rollback semantics: backend termination before claim
commit, and transaction-aborting triggers before page/finalizer/result/reconciliation commits. These
deterministic cases prove the persisted recovery edges, but they are kept distinct from the broader
Compose matrix that kills an operating-system worker at every network and evidence-insert point.

### 8.1 Measured 100,000-recipient database boundary

The set-based publication choice in D3 is also exercised against an exact 100,000-row stable run.
Three faults are injected independently: a failure after the Delivery insert but before the Alarm
update, a statement timeout during the insert, and a real recipient uniqueness conflict. Each fault
rolls the whole transaction back; the same fenced expansion lease can then publish exactly one
complete audience.

In one local WSL2/PostgreSQL 18.1 sample, the production repository transaction took 6.718 seconds,
generated 84.84 MiB of WAL, and used no temporary blocks. An equivalent
`EXPLAIN (ANALYZE, BUFFERS, WAL)` took 5.899 seconds. Same-Alarm and expansion-job writers waited on
the deliberate 750 ms fault barrier as designed, while an MVCC detail request and another Alarm's
dispatch claim completed in about 30 ms. The 100,000-row aggregate, failure keyset page, dispatch,
reconciliation, and expired-lease plans also stay within their declared regression budgets and use
the intended selective indexes; the full aggregate intentionally uses a sequential scan.

These figures validate the current choice against the declared local budget; they are not a
portable production SLO. The candidate to split publication into externally visible page commits is
still rejected because it exposes a partial audience. Revisit D3 with a partition-swap or other
database-native publication barrier only if measurements in the deployment environment exceed its
transaction, WAL, lock-wait, or API-latency budget.

### 8.2 Measured 100,000-recipient runtime/resource boundary

A separately recorded pre-final-review Node 24 Compose profile exercised the page traversal rather
than pre-seeding the staging table. It killed the only worker after the 43,000th committed recipient,
observed matching staging/materialized counts and zero Deliveries, and then allowed the same durable
run to resume.
The users source saw all 100 expected page offsets plus one safe replay of the boundary offset.

During traversal and the final set-based publication, 319 Alarm-detail requests measured p95
6.098ms and p99 8.360ms. API V8 heap moved from 25.49MiB to a 34.04MiB peak; the three worker peaks
were 38.18–44.34MiB, and application-name connection counts remained below the configured pool
ceilings. These bounded local observations support the page/staging choice in D3; they do not set a
portable heap or latency SLO.

The profile then held a cleanup delete page with a test-only trigger and killed its owner while the
durable cleanup lease was `RUNNING`. Publication remained visible as exactly 100,000 Deliveries; the
interrupted delete rolled back, and lease takeover reached cleanup `COMPLETED` with zero staging
rows. After scaling to three workers, one 20-second sample completed 306/239/247 sends with maximum
in-flight 120 and ingress rolling maximum 41. Cancel returned in 5.654ms and bounded sweeps reached
active zero in 20.072 seconds. PostgreSQL deadlocks were zero; injected lock waiters and the final
dead-tuple estimate were retained in the audit rather than hidden.

The worker JSON events, API Alarm ID, and controlled-provider ingress provide a common correlation
key, but this is not distributed tracing. Production now emits a leader-owned DB snapshot for queue
age/throughput/state counts, exact per-attempt monotonic provider-duration samples, and committed
lease-recovery reasons. A collector can derive histograms/counters from these records, but no
production metric backend, pull endpoint, alert delivery, or worker health endpoint is claimed.
Repeated V8-inspector attachment coincided with many late
reservations in a diagnostic rerun; the audit cannot prove that instrumentation was the sole cause,
so that intrusive rerun is retained as a diagnostic miss and is not used as a throughput gate.
