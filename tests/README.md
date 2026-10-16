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
Recorded runtime snapshots remain `partial` until they are rerun on the settled current tree, while
external submission work and the known official-Mock dependency deviation have distinct statuses.
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

Latest current-tree host regression: 31 files and 358 tests passed on Node 22.22.2. The earlier
22-file/306-test tree passed in the pinned Node 24.19 build image; the current observability and
public-clone traceability delta still needs the final pinned-image gate and does not claim that
evidence early.

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

The separate local 100,000-recipient runtime gate traverses the real users pages and combines a
committed-checkpoint worker kill, exact publication, cleanup-transaction kill/takeover, V8
heap/PostgreSQL pool/API-latency sampling, live scale from one to three workers, and bounded cancel
convergence. A current exact process-kill matrix additionally stops dedicated workers with `SIGKILL`
before and after the first, middle, and last cleanup batch commits, and before and after a cancel
batch commit. It verifies transaction rollback versus durable commit, cleanup-lease takeover, and
exact 100,000-row terminal counters without inferring those facts from exception-only failpoints.
The completed reference sample preserved 43,000 staged recipients and zero Deliveries
at the checkpoint kill, published exactly 100,000, cleaned staging to zero after takeover, observed
API p99 8.360ms and worker heap peaks below 45MiB, and finished cancel with no active or STARTED
work. A repeated V8-inspector diagnostic coincided with many misses of the 5ms permit-lateness
boundary, without proving instrumentation was the sole cause, so runtime throughput numbers remain
local diagnostic evidence rather than a CI or production SLO.

Final current-tree integration/E2E regression: 17 files and 224 tests passed twice on the Node 22
host. The settled current tree does not claim a pinned Node 24.19 integration result until that
separate final gate completes.
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
