# US Alliance Backend Assignment

NestJS로 구현한 대량·긴급 알림 발송 백엔드입니다. API 요청과 실제 발송을 분리하고,
PostgreSQL을 durable queue이자 상태의 원장으로 사용합니다. 기본 Compose 구성은 `api`, worker 3개,
PostgreSQL, Redis, migration, 제공 Mock(`ghcr.io/us-all/backend-assignment-api:1.2`)을 한 번에 실행합니다.

- `BULK`: Mock users API의 전체 사용자 약 100,000명을 조회해 발송
- `EMERGENCY`: 생성 시 지정한 1~100명에게 발송
- 원문에서 선택 기능인 긴급 우선순위, 전역 처리량 제한, 다중 worker, worker 종료 복구까지 모두 구현 범위에 포함

## 실행

### Docker Compose로 설치·실행

필수 도구는 Docker와 Docker Compose입니다. 저장소 루트에서 다음 한 번으로 image build/pull,
PostgreSQL migration, API와 worker 기동까지 수행합니다.

```bash
docker compose up
```

기본 Compose에도 worker replica가 3개로 선언되어 있습니다. 평가 명령을 명시적으로 실행해도 같은
구성으로 동작합니다.

```bash
docker compose up --scale worker=3
```

기동 확인과 API 문서는 다음 주소에서 볼 수 있습니다.

```bash
curl http://127.0.0.1:3000/health/live
curl http://127.0.0.1:3000/health/ready
curl http://127.0.0.1:3000/docs/openapi.json
```

- Swagger UI: <http://127.0.0.1:3000/docs>
- OpenAPI 3.0.3 JSON: <http://127.0.0.1:3000/docs/openapi.json>
- 종료 및 volume 정리: `docker compose down -v`

API는 기본적으로 host의 `127.0.0.1:3000`에만 노출됩니다. 포트와 로컬 자격 증명은
`.env.example`을 복사한 `.env`에서 변경할 수 있습니다.

### 로컬 의존성 설치와 테스트

로컬 검증 기준은 Node.js `24.19.0`, pnpm `11.22.0`입니다.

```bash
corepack enable
corepack prepare pnpm@11.22.0 --activate
pnpm install --frozen-lockfile
pnpm verify
```

`pnpm verify`는 format, lint, architecture boundary, typecheck, unit/contract, 요구사항 추적성,
문서 정합성, build를 순서대로 검사합니다.

PostgreSQL·Redis·HTTP integration/E2E suite는 격리된 test Compose를 사용합니다.

```bash
docker compose -p usall-integration \
  -f docker-compose.yml -f docker-compose.test.yml \
  up -d postgres redis

TEST_DATABASE_URL=postgresql://usall:usall_local@127.0.0.1:55432/usall_assignment \
TEST_REDIS_URL=redis://127.0.0.1:56379 \
pnpm test:integration

docker compose -p usall-integration \
  -f docker-compose.yml -f docker-compose.test.yml \
  down -v
```

세부 테스트 범위와 실행 증거는 [tests/README.md](tests/README.md)에 정리했습니다.

## API 예시

### 기본 흐름

대량 알림을 생성합니다.

```bash
curl -i -X POST http://127.0.0.1:3000/v1/alarms \
  -H 'Content-Type: application/json' \
  -d '{"type":"BULK","title":"서비스 점검","body":"02시에 점검을 시작합니다."}'
```

긴급 알림은 정확한 수신자 ID 1~100개를 함께 저장합니다.

```bash
curl -i -X POST http://127.0.0.1:3000/v1/alarms \
  -H 'Content-Type: application/json' \
  -d '{"type":"EMERGENCY","title":"결제 승인","body":"지금 결제를 승인해 주세요.","recipientIds":["u_000001","u_000002"]}'
```

생성 직후 응답은 `201 Created`이며 `Location`과 현재 Alarm 전체 표현을 반환합니다.

```http
HTTP/1.1 201 Created
Location: /v1/alarms/4ba646af-219d-4a92-aa75-2cfcb19874ef
Content-Type: application/json
```

```json
{
  "id": "4ba646af-219d-4a92-aa75-2cfcb19874ef",
  "type": "BULK",
  "title": "서비스 점검",
  "body": "02시에 점검을 시작합니다.",
  "status": "CREATED",
  "failureCode": null,
  "materializedCount": 0,
  "total": null,
  "sent": 0,
  "failed": 0,
  "canceled": 0,
  "active": 0,
  "unresolved": 0,
  "duplicateCount": 0,
  "incidentCount": 0,
  "incidentSummary": {
    "count": 0,
    "codes": [],
    "firstDetectedAt": null,
    "lastDetectedAt": null
  },
  "audienceFinalizedAt": null,
  "expansionCompletedAt": null,
  "finishedAt": null,
  "createdAt": "2026-10-18T00:00:00.000Z",
  "updatedAt": "2026-10-18T00:00:00.000Z"
}
```

응답의 `id`로 발송을 시작하고 상태를 조회하거나 취소합니다.

```bash
ALARM_ID=4ba646af-219d-4a92-aa75-2cfcb19874ef

curl -i -X POST "http://127.0.0.1:3000/v1/alarms/$ALARM_ID/send"
curl -i "http://127.0.0.1:3000/v1/alarms/$ALARM_ID"
curl -i -X POST "http://127.0.0.1:3000/v1/alarms/$ALARM_ID/cancel"
curl -i 'http://127.0.0.1:3000/v1/alarms?type=BULK&limit=50'
```

발송 시작과 취소는 body 없이 요청하며 `202 Accepted`, `Location`, 현재 Alarm 전체 표현을
반환합니다. 여기서 `202`는 DB에 비동기 명령이 안전하게 접수됐다는 뜻이지 외부 발송 완료를
뜻하지 않습니다.

### Endpoint 계약

| Endpoint | 성공 | 주요 오류 |
| --- | --- | --- |
| `POST /v1/alarms` | `201` + `Location` + Alarm | `400`, `413`, `415`, `422`, `500`, `503` |
| `GET /v1/alarms` | `200` + `{ alarms, nextCursor }` | `400`, `422`, `500`, `503` |
| `GET /v1/alarms/{alarmId}` | `200` + Alarm | `400`, `404`, `500`, `503` |
| `POST /v1/alarms/{alarmId}/send` | `202` + `Location` + Alarm | `400`, `404`, `409`, `500`, `503` |
| `POST /v1/alarms/{alarmId}/cancel` | `202` + `Location` + Alarm | `400`, `404`, `409`, `500`, `503` |
| `GET /health/live` | `200 {"ok":true}` | `500` |
| `GET /health/ready` | PostgreSQL 정상 시 `200 {"ok":true}` | `500`, `503` |
| `GET /docs`, `GET /docs/openapi.json` | `200` | `500` |

목록은 `type`, `status`, `limit`, signed opaque `cursor`를 선택적으로 받는 keyset pagination입니다.
생성 요청의 base media type은 `application/json`이어야 하며 `charset` 같은 parameter는 허용합니다.
title은 trim 후 1~200 Unicode code point, body는 보존된 값 기준 1~5000자이면서 공백만으로 구성될
수 없습니다. 요청 body 한도는 128KiB입니다. `EMERGENCY.recipientIds`는 정규화하지 않는 opaque
ID 1~100개이고, 각 값은 1~128자·중복 없음·양끝 공백 없음 조건을 만족해야 합니다. `BULK`에는
`recipientIds`를 보낼 수 없습니다.

공개 enum은 코드, DB constraint, OpenAPI와 contract test에서 동일성을 검사합니다.

- `AlarmType`: `BULK`, `EMERGENCY`
- `AlarmStatus`: `CREATED`, `QUEUED`, `EXPANDING`, `SENDING`, `CANCEL_REQUESTED`, `CANCELED`,
  `COMPLETED`, `PARTIALLY_FAILED`, `FAILED`, `ATTENTION_REQUIRED`

오류는 일관된 `application/problem+json`으로 반환합니다. `traceId`는 형식을 약속하지 않는 요청
상관키입니다.

```http
HTTP/1.1 422 Unprocessable Entity
Content-Type: application/problem+json
```

```json
{
  "type": "about:blank",
  "title": "Unprocessable Entity",
  "status": 422,
  "code": "INVALID_ALARM",
  "detail": "Alarm input is invalid",
  "instance": "/v1/alarms",
  "traceId": "req-1",
  "errors": [
    {
      "field": "recipientIds[1]",
      "code": "DUPLICATE_RECIPIENT",
      "message": "recipient IDs must be unique without normalization"
    }
  ]
}
```

start는 진행 중이면 현재 자원을 멱등 반환하지만 terminal 상태에서는 `409`입니다. cancel은
`CANCEL_REQUESTED`와 `CANCELED`에서 멱등이며, `CANCELED`를 제외한 terminal 상태에서는 `409`입니다.
응답 표현은 JSON으로 고정해 별도 `Accept` 협상을 하지 않습니다. 현재 Nest/Fastify route 계약상
unknown path와 미등록 method는 모두 `404`이며 `405`를 별도로 만들지 않습니다. required/nullable을
포함한 최종 계약은 [openapi.json](openapi.json)이 기준입니다.

## 구현 코멘트

### API 설계

- 생성은 새 resource를 만들므로 `201`, 발송·취소는 durable 비동기 명령 접수이므로 `202`를
  사용했습니다. command API가 provider 응답을 기다리지 않아 100,000명 발송 시간과 분리됩니다.
- 공개 상태 전이는 `CREATED -> QUEUED -> EXPANDING/SENDING -> terminal`이며 취소 중에는
  `CANCEL_REQUESTED`를 노출합니다. terminal은 `CANCELED`, `COMPLETED`, `PARTIALLY_FAILED`, `FAILED`,
  `ATTENTION_REQUIRED`입니다.
- API command와 worker는 같은 Alarm row lock 및 고정 lock 순서를 사용합니다. start/cancel의
  선형화 지점은 PostgreSQL transaction commit이고, 조회는 저장된 집계값만 읽습니다.

### 구조

```text
client -> api (NestJS/Fastify) -----> PostgreSQL <----- worker x3
                                          ^              |   |
                                          |              |   +-> provider Mock
                                     migrate             +-----> Redis limiter
```

API와 worker는 같은 domain/application package를 사용하지만 composition root와 process는
분리했습니다. API는 bounded DB transaction만 수행하고, users 순회·외부 발송·정합성 복구는 worker가
담당합니다. PostgreSQL이 durable source of truth이고 Redis는 발송 허가 시각 조정에만 사용합니다.

### 핵심 동작

- `BULK`는 users API를 최대 1,000명씩 cursor 순회해 staging에 적재합니다. 마지막 page transaction이
  전체 Delivery를 set-based publish하고 audience barrier를 세우므로 중간 실패 시 일부 수신자만
  먼저 발송되지 않습니다.
- `EMERGENCY`는 생성 transaction에서 정확한 수신자 집합을 snapshot하고 발송 시작 시 Delivery를
  만듭니다.
- Delivery는 `FOR UPDATE SKIP LOCKED`와 lease generation/token fencing으로 여러 worker에 분배됩니다.
  stale worker는 lease를 되찾은 최신 owner의 상태를 덮어쓸 수 없습니다.
- 외부 시도는 `PREPARED`와 HTTP handoff 직전의 `COMMITTED`를 구분합니다. 미발송이 증명된 실패만
  재시도하고, 전송 여부가 불명확하면 blind retry하지 않고 provider history reconciliation으로
  이동합니다.
- cancel과 durable `STARTED`/provider `COMMITTED` fence는 Alarm row lock으로 직렬화합니다. cancel
  commit 뒤에는 새 provider commit을 거절하고, 이미 commit된 요청만 응답이나 history로 정산합니다.

### 데이터 처리와 성능 고려

- Alarm에는 API 조회용 count와 audience barrier를, Delivery에는 수신자별 현재 상태를, Attempt에는
  외부 요청의 durable 경계를 저장합니다. 상태와 count는 같은 transaction에서 갱신됩니다.
- 상세 조회는 100,000개 Delivery를 매번 집계하지 않고 Alarm의 stored counter를 읽어 O(1)로
  처리합니다. 목록은 signed keyset cursor를 사용합니다.
- publication은 100,000행을 한 건씩 만들지 않고 set-based SQL로 수행합니다. cancellation과 cleanup은
  lease를 가진 bounded batch로 처리해 긴 transaction과 memory 증가를 피합니다.
- PostgreSQL wait는 workload별로 제한했습니다. 일반 API·dispatch·maintenance·reconciliation query는
  5초, 100,000명 set-based publication이 있는 expansion query는 30초, lock wait는 모두 5초입니다.
  실패한 transaction client는 pool에 반환하지 않고, best-effort rollback도 최대 5초 뒤 종료합니다.

### 관측 신호와 장애 대응

- attempt/Alarm/Delivery/worker/lease 상관키와 provider duration을 구조화 JSON log로 남깁니다.
  수신자 ID, 알림 본문, provider raw payload는 기록하지 않습니다.
- maintenance leader가 queue/state/provider-window snapshot과 counter drift를 점검합니다. worker가
  종료되면 만료 lease를 다른 worker가 회수하고 generation/token으로 이전 owner의 늦은 commit을
  차단합니다.
- Redis 또는 provider가 사용할 수 없으면 dispatch는 fail-closed합니다. API readiness는 API의 직접
  runtime dependency인 PostgreSQL 상태를 나타냅니다.

### 설계 해석과 한계

- provider에 idempotency key 계약이 없고 history visibility도 bounded하지 않으므로 네트워크 단절
  이후의 외부 exactly-once를 일반적으로 증명할 수 없습니다. 이 경우 중복 가능성을 숨기지 않고
  `UNKNOWN`, 이후 `ATTENTION_REQUIRED`와 incident count로 노출합니다.
- 취소는 이미 provider에 commit된 HTTP 요청을 회수하지 못합니다. 긴급 우선순위도 이미 전송 중인
  HTTP 요청을 선점하지 않고 다음 eligible claim부터 적용합니다.
- users API에 point-in-time snapshot token이 없어서 BULK 대상은 순회 중 upstream 변경의 영향을 받을
  수 있습니다. 로컬 publication 자체는 완전한 집합이 확정된 뒤에만 열립니다.
- 지속적인 긴급 유입은 대량 알림을 지연시킬 수 있습니다. 요구사항의 strict priority를 택한 결과이며,
  운영 SLO가 정해지면 aging이나 quota를 추가할 수 있습니다.

## 선택 구현 사항

원문에서 선택 사항이지만 이번 구현에서는 모두 필수 범위로 승격했습니다.

| 항목 | 구현 방식 | 보장과 경계 |
| --- | --- | --- |
| 긴급 알림 | 생성 시 exact recipient snapshot, 최대 100명 | BULK와 같은 start/cancel/status model 사용 |
| 긴급 우선 처리 | 긴급 신규·재시도 `100` > 대량 재시도 `20` > 대량 신규 `10`; worker당 64 slot 중 2개는 긴급용 | 다음 eligible claim부터 strict priority; 이미 제출한 HTTP는 선점하지 않음 |
| 공유 처리량 제한 | 모든 worker·알림 종류·재시도가 Redis TIME 기반 25ms paced slot을 공유, 목표 40/s | provider 상한 50/s에 headroom; Redis 장애 시 발송 중단 |
| worker 수평 확장 | PostgreSQL `SKIP LOCKED`, fenced lease, 공통 Redis limiter | `docker compose up --scale worker=3` 이상에서 중복 claim 방지 |
| worker 종료 복구 | lease 만료 후 generation/token을 바꿔 takeover; ambiguous commit은 reconciliation | provider 요청 전이면 안전 재시도, 불명확 결과는 blind retry하지 않음 |

## 검증 결과

최종 작업 트리에서 확인한 결과입니다.

| 범위 | 결과 |
| --- | --- |
| Node `24.19.0` 전체 정적·unit/contract gate | `pnpm verify` PASS, 32 files / 368 tests |
| PostgreSQL·Redis·HTTP integration/E2E | 17 files / 224 unique tests 확인. 외부에 떠 있던 maintenance leader lock이 간섭한 4건은 격리 후 영향 spec 22/22 PASS |
| 새 volume의 bare Compose | `docker compose up`으로 migration, API, worker 3개, 공식 Mock `1.2` 정상 기동 |
| 공식 Mock 단건 acceptance | EMERGENCY 1명: `201 -> 202 -> COMPLETED`, sent 1 |
| 현재 source의 BULK 100,000명 soak | 42분 43초, `100000/100000 COMPLETED`, DB/provider/history exact set 일치, 중복·누락·미확정·429·rolling-1s 위반 0, rolling 최대 45, worker kill/restart 20회 |
| 다중 worker/우선순위/kill matrix | worker 1·2·3·5 scale out/in과 official Mock SIGKILL, 포화 BULK 중 EMERGENCY 100명 우선 처리 PASS; 이후 변경은 PostgreSQL wait/disposal 경로에 한정 |

100,000명 soak의 ingress 수치는 정확한 수신 timestamp를 기록하는 controlled Mock에서 측정했습니다.
공식 Mock `1.2`가 제공하지 않는 timestamp를 공식 Mock 결과처럼 주장하지 않습니다. 이 실행에서 과제
요구인 50/s 이하와 무누락·무중복은 충족했습니다. 별도 내부 진단 목표인 clean throughput 38/s와
permit-to-STARTED p99 40ms는 각각 37.65/s, 108.033ms로 미달했지만 과제의 hard requirement나 provider
상한 위반은 아닙니다. 실행 조건과 더 자세한 수치는 [tests/README.md](tests/README.md)를 참고하세요.

## 주요 기술 선택 요약

| 해결할 문제 | 검토 후보 | 채택과 근거 | 미채택 이유·재검토 조건 |
| --- | --- | --- | --- |
| durable 작업 분배 | Kafka/RabbitMQ, Redis queue, PostgreSQL queue | PostgreSQL Delivery + `SKIP LOCKED`: 상태 전이·취소·claim을 같은 transaction으로 선형화 | 전용 broker는 높은 처리량/stream replay에 유리하지만 이번 50/s에서 DB와 이중 원장 문제가 큼. 규모가 크게 늘면 재검토 |
| 전역 50/s 제한 | worker별 limiter, DB advisory lock, Redis paced schedule | Redis TIME 기반 단일 paced schedule: worker 수·시계 편차와 무관하게 공유 | worker별 제한은 scale 시 합산 초과, DB는 25ms hot coordination 부담. Redis Cluster 도입 시 단일 key 배치 재검토 |
| 100,000명 audience 확정 | page마다 즉시 Delivery, memory 일괄 적재, DB staging | cursor staging 후 마지막 transaction에서 set-based publish | 즉시 공개는 부분 발송, memory 적재는 crash/heap 위험. upstream snapshot API가 생기면 경계 단순화 가능 |
| 불명확한 provider 결과 | 즉시 retry, 무조건 실패, durable attempt+history reconciliation | `PREPARED/COMMITTED/UNKNOWN`과 history-first reconciliation | 즉시 retry는 중복 위험, 무조건 실패는 실제 성공 누락. provider idempotency 계약이 생기면 단순화 가능 |
| 긴급 우선순위 | 별도 queue/limiter, DB priority만, strict shared admission | 같은 Delivery queue와 limiter에서 priority claim, 긴급 reserved slot | 별도 limiter는 50/s 공유 위반. starvation 방지 SLO가 생기면 aging/quota 추가 |
| 조회 성능과 정합성 | 매 조회 `COUNT`, 비동기 cache, stored counter | 상태 전이와 같은 transaction에서 Alarm counter 갱신 | 실시간 COUNT는 100,000행 조회 비용, cache는 stale 가능. analytics 요구가 생기면 별도 read model 고려 |
| API/worker 배치 | 단일 process, 완전 별도 codebase, 단일 codebase+별도 process | domain/application을 공유하고 `api`/`worker` composition root와 container를 분리 | 단일 process는 장애·확장 격리 부족, 별도 codebase는 규칙 drift 위험 |
| DB 접근 계층 | ORM 자동 migration, query builder, 직접 `pg`+immutable migration | concurrency SQL·lock·timeout을 명시적으로 통제하고 checksum migration 사용 | CRUD 비중이 커지면 type-safe query builder를 재검토 |

세부 후보 비교와 결정 근거는 [알림 발송 설계](docs/architecture/alarm-delivery-design.md)의 D1~D24에,
초기 scaffold 채택 근거는 [ADR-0001](docs/adr/0001-adopt-validated-repository-scaffold.md)에 있습니다.

## 구현 회고와 후속 작업

### 고민했고 되돌린 결정

- provider raw error message를 로그에 남기려던 방향은 PII·대용량 payload 유입 가능성 때문에
  allowlist code와 상관키만 기록하도록 바꿨습니다.
- retry scanner가 먼저 판단하고 늦은 positive history를 뒤에서 적용하는 구조는 같은 Delivery를
  재발송할 race가 있어, dispatch lock 안에서 positive evidence를 먼저 채택하도록 바꿨습니다.
- 매 idle loop마다 100,000행 counter 정합성을 검사하는 방식은 DB 부하가 커서, 약 60초 주기의
  advisory-lock leader 점검으로 변경했습니다.
- PostgreSQL query timeout을 전부 같은 값으로 두는 안은 실제 100,000명 publication이 5초를 넘을 수
  있어 폐기했습니다. API/dispatch는 5초, expansion은 30초로 workload별 예산을 분리했습니다.

### 시간이 더 있다면

- OpenTelemetry collector, metric dashboard, SLO alert와 operator repair command를 추가합니다.
- Swagger UI asset을 self-host하고 운영 gateway 인증·CSP 정책까지 배포 구성으로 검증합니다.
- provider가 idempotency key와 authoritative history SLA를 제공하면 외부 exactly-once 경계를 강화합니다.
- users API가 snapshot token을 제공하면 BULK audience의 point-in-time 일관성을 보장합니다.

## 상세 문서

- [OpenAPI 3.0 계약](openapi.json)
- [알림 발송 설계와 상세 선택 근거](docs/architecture/alarm-delivery-design.md)
- [테스트 범위와 실행 증거](tests/README.md)
- [요구사항 추적성 manifest](docs/requirements-traceability.json)
- [저장소 구조 지도](MAP.md)
- [Scaffold 채택 ADR](docs/adr/0001-adopt-validated-repository-scaffold.md)
