# Tests

The repository has two executable suites.

## Requirements traceability gate

```bash
pnpm test:traceability
```

The public manifest at `docs/requirements-traceability.json` inventories all 260 canonical P0
scenarios (218 source-required and 42 project-promoted). The gate validates independent design,
implementation, and verification states; existing implementation paths; executable metadata for
automated IDs; complete non-automated owner/evidence/blocker fields; and orphan-free test coverage.
Runtime evidence records its source provenance instead of treating every older snapshot as current.
The 100,000-recipient soak matches the current source fingerprint; exact multi-worker evidence is
identified separately because PostgreSQL wait/disposal code changed after that run. External
submission work and the known official-Mock dependency deviation retain distinct statuses.
The manifest contains no private local evidence paths.
When the private internal catalog exists, the gate also performs a strict ID/priority/count
cross-check and prints `catalogSource=internal-catalog`. A real public clone intentionally has no
`docs/internal` tree; there the same command prints `catalogSource=manifest-only` and still enforces
the manifest's counts, uniqueness, public paths/evidence, requirement assignments, and executable
test-source references.

## Unit and contract suite

```bash
pnpm test:unit
```

This suite covers Alarm validation and policy, application services, expansion/dispatch policies,
provider adapters, evidence evaluation, migration discovery, OpenAPI shape and executable examples,
Compose contracts, API/worker separation, deterministic seeded retry bounds, PII-safe correlated
worker events, leader-owned operational snapshot mapping/cadence, monotonic provider duration,
readiness/lifecycle JSON, and the business-neutral worker delay. A test-only, guard-resolved Delivery reference
oracle enumerates every conceptual state/event pair and drives reproducible seeded event sequences;
it is deliberately not imported by production code or presented as a second runtime state machine.

Latest current-tree pinned-runtime regression: Node 24.19.0 `pnpm verify` passed all format, lint,
architecture-boundary, typecheck, documentation, build, traceability, and unit/contract gates. The
unit/contract portion contains 32 files and 368 tests.

## PostgreSQL, Redis, and HTTP integration suite

```bash
docker compose -p usall-integration \
  -f docker-compose.yml -f docker-compose.test.yml \
  up -d postgres redis

TEST_DATABASE_URL=postgresql://usall:usall_local@127.0.0.1:55432/usall_assignment \
TEST_REDIS_URL=redis://127.0.0.1:56379 \
pnpm test:integration
```

Integration files run serially because they share one PostgreSQL/Redis service and several files
create 10,000-100,000-row fixtures with explicit latency budgets. This isolates each measured
contract from unrelated suite load without relaxing its threshold; concurrency scenarios inside a
file still use their real competing clients, transactions, and requests.

Every PostgreSQL spec creates an isolated schema and drops it afterwards. The suite exercises fresh
migrations, repositories, concurrent claims, audience publication, cancellation, retry exhaustion,
lease recovery, reconciliation, active-first aggregate truth tables, terminal late evidence,
read-only counter-drift alerts, exact user-page retry/deduplication, cleanup, Redis pacing/cold
recovery, exact second-boundary pacing, caller wall-clock skew of -10/0/+10 seconds, and the HTTP API.
The retry suite also separates the two exactly-once boundary claims: an EO-01 negative proof makes a
provider with a falsely advertised authoritative-empty capability produce two distinct external
message IDs and verifies the durable duplicate incident, while EO-02 keeps the default opaque
profile on the no-blind-retry path.
The HTTP checks execute every documented create example and verify that
`/docs/openapi.json` serves the checked-in contract and `/docs` exposes its Swagger UI entrypoint.
The Delivery state-oracle integration profile separately cross-checks production repository results
for dispatch outcomes, pre-commit release, all persisted-state cancel/reconciliation eligibility,
all true-terminal evidence classes, and positive NEEDS_REVIEW correction. Oracle-only coverage is
not treated as proof of an unexecuted SQL edge.
Two fresh READ COMMITTED connections also race the same production `startAttempt` claim behind a
pre-commit barrier; exactly one creates an attempt and consumes budget. Crash-edge tests terminate a
real PostgreSQL backend before claim commit and inject transaction rollback before expansion,
dispatch-result, and reconciliation commits. Full operating-system worker-kill phase matrices remain
separate Compose evidence rather than being inferred from these repository tests.
The migration contract inventories every domain column, CHECK/FK/PK/UNIQUE constraint, and index,
then verifies two-owner serialization, a data-preserving `0001` to `0002` upgrade, sequential
application through the current `0003` index migration, transactional rollback of a broken file,
and clean recovery on the same database.

`database-performance.integration-spec.ts` adds the exact 100,000-row database gate. It injects
three publication failures and proves full rollback plus same-lease retry, records
`EXPLAIN (ANALYZE, BUFFERS, WAL)` for publication and selective worker paths, traverses all 30,000
failed rows without a cursor gap or duplicate, proves due and lease decisions ignore concurrent
process-clock offsets of -10/0/+10 seconds, and measures the real cancel API with 100,000 Deliveries
and 2,000 durable STARTED attempts. Set `PERF_AUDIT_LOG=1` for local plan and timing diagnostics;
measured values are environment evidence, not portable production guarantees.

The current-source 100,000-recipient runtime gate traversed real paged users, held the durable queue
with zero workers, scaled to three workers, and issued 20 worker kill/restarts. It completed in
42m43s with `100000/100000` sent. PostgreSQL, provider ingress, and provider history contained the
same exact 100,000 recipients; duplicate, repost, unresolved, unfinished, deadlock, 429, and rolling
one-second violations were all zero. The ingress window peaked at 45 requests. This rate proof used
a controlled Mock that records authoritative receipt timestamps because the supplied Mock does not
expose them. The source fingerprint (`a2233a89...ce0b`, 168 files, 1,520,108 bytes) matched the
executed tree. Clean throughput of 37.65/s and permit-to-STARTED p99 of 108.033ms missed separate
local diagnostic targets of 38/s and 40ms; neither is an assignment limit or provider-cap violation.

The exact multi-worker matrix separately passed scale out/in at worker counts 1, 2, 3, and 5 with
rolling maxima 41, 42, 41, and 43 and no violation. Official-Mock in-flight `SIGKILL` recovery and a
saturated BULK plus EMERGENCY-100 priority/kill scenario also passed with no priority violation. This
matrix predates only the later PostgreSQL query-wait and failed-client disposal hardening; dispatch
priority, admission, fencing, and limiter anchors are unchanged, while the current-fingerprint soak
exercised the new PostgreSQL path.

The integration/E2E audit covers 17 files and 224 unique tests on the pinned Node 24.19 runtime. An
initial aggregate run passed 220/224 while a separately running Compose maintenance leader held the
same advisory lock. After isolating the intended PostgreSQL/Redis test services, both affected specs
passed 22/22. This records verification of all 224 tests without misreporting it as one uninterrupted
aggregate run.
The high-contention RET-09 boundary case has an explicit 120-second test wall-clock allowance so a
parallel 100,000-row planning fixture cannot abort it; its assertions and product retry/rate budgets
are unchanged.

The supplied provider is covered separately by local Compose black-box acceptance checks because its
failure distribution and 30-second delayed responses would make the fast deterministic suite flaky.
Those black-box checks treat provider ingress timestamps and durable adapter-commit timestamps as
separate rate-limit evidence. They also distinguish the Compose initial dependency-health gate from
runtime API readiness: `/health/ready` is PostgreSQL-scoped, while Redis/provider outages must stop
worker dispatch fail-closed without pretending that those worker-only dependencies are API
dependencies.
