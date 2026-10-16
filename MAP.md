# Repository map

| Path | Responsibility | Status |
| --- | --- | --- |
| `apps/api` | NestJS/Fastify HTTP composition and health | Implemented |
| `apps/migrate` | One-shot checksummed SQL migration process | Implemented |
| `apps/worker` | Controller-free expansion, dispatch, maintenance, reconciliation loops | Implemented |
| `packages/alarms/src/domain` | Alarm validation/policy and provider evidence decisions | Implemented |
| `packages/alarms/src/application` | API use cases and background-process ports/policies | Implemented |
| `packages/alarms/src/infrastructure/persistence` | PostgreSQL repositories, leases, queue, recovery | Implemented |
| `packages/alarms/src/infrastructure/provider` | Users, message POST, and history HTTP adapters | Implemented |
| `packages/alarms/src/infrastructure/limiter` | Redis server-time paced global limiter | Implemented |
| `packages/alarms/src/presentation` | HTTP controller, validation boundary, problem details | Implemented |
| `packages/platform` | Business-neutral abortable worker delay | Implemented |
| `migrations` | Immutable versioned Alarm/Delivery runtime schema | Implemented |
| `tests/unit`, `tests/scaffold` | Domain, application, adapter, contract and architecture tests | Implemented |
| `tests/integration`, `tests/e2e` | PostgreSQL, Redis, concurrency, recovery and HTTP tests | Implemented |
| `docs/architecture` | Public architecture, trade-offs, and repository boundaries | Active |
| `docs/adr` | Repository-level architectural decisions | Active |

## Dependency direction

```text
presentation --> application --> domain
infrastructure --> application --> domain
apps ---------> composition only
platform -----> no alarms dependency
```

`.dependency-cruiser.cjs` prevents domain/application from importing infrastructure or Nest/DB/Redis
adapters. Alarm-aware persistence, rate-limit and provider behavior stays in the Alarm bounded
context rather than becoming generic platform code.

## Runtime topology

```text
client -> api -> PostgreSQL

worker x3 -> PostgreSQL durable work + fencing
          -> Redis shared paced permits
          -> provider users/messages/history APIs

migrate -> PostgreSQL DDL before api/worker startup
```

There is no Kafka/RabbitMQ service. PostgreSQL is the durable queue; Redis is only the shared provider
admission coordinator. See [alarm delivery design](docs/architecture/alarm-delivery-design.md) for
the flow and decision rationale.
