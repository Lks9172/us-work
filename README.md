# US Alliance Backend Assignment

대량 알림과 긴급 알림을 비동기로 발송하는 NestJS 백엔드입니다. 대량 알림은 약 100,000명의
사용자를 cursor pagination으로 확장하고, 긴급 알림은 지정한 1~100명을 먼저 처리합니다. API와
worker는 별도 프로세스이며 기본 Compose 구성은 worker 3개를 실행합니다.

원문에서 선택 기능으로 분류된 긴급 알림, 우선 처리, worker 3개, 처리 중 worker 종료 복구도 이
구현에서는 모두 필수 범위로 다룹니다.

## 핵심 동작

- `BULK`: users API를 최대 1,000명씩 순회해 staging에 저장한 뒤, 마지막 page transaction에서만
  Delivery를 일괄 공개합니다. 중간 실패나 worker takeover 중에는 일부 수신자만 발송되지 않습니다.
- `EMERGENCY`: create 시 exact opaque recipient ID를 snapshot하고 start 시 Delivery를 즉시 만듭니다.
- 우선순위: 긴급 신규·재시도 `100` > 대량 재시도 `20` > 대량 신규 `10`입니다. 이미 외부 API에
  제출한 요청을 취소하지는 않지만, 다음 permit 이후 eligible claim부터 긴급 알림이 앞섭니다.
- 처리량: 모든 worker와 모든 알림이 Redis TIME 기반 paced limiter 하나를 공유합니다. 25ms 간격,
  burst 0, 목표 40 reservations/s로 외부 상한 50 POST/s에 headroom을 둡니다. DB fence와 network
  scheduling 뒤 실제 도착은 별도 timestamp-audit Mock의 rolling window로 검증합니다. worker마다
  DB claim/STARTED admission은 1개만 수행하고 그와 겹쳐 row/priority를 소유하지 않는 다음 raw
  permit 1개만 준비합니다. 따라서 provider-COMMITTED 전 reservation 합계는 최대 2/worker입니다.
- durable queue: 별도 메시지 브로커 없이 PostgreSQL Delivery row와 `FOR UPDATE ... SKIP LOCKED`를
  사용합니다. Redis는 queue가 아니라 전역 발송 속도 조정에만 사용합니다.
- 복구: claim/expansion/reconciliation/cleanup에 generation+token lease fencing을 적용합니다.
  durable attempt는 `PREPARED`와 실제 HTTP handoff 직전의 `COMMITTED`를 구분합니다. worker가
  `PREPARED`에서 종료되면 provider 요청 전임이 증명되므로 다른 worker가 안전하게 재할당하고,
  `COMMITTED`는 HTTP adapter 호출 권한/의도이지 `request.end` 실행 증거는 아닙니다. request 객체가
  만들어지기 전 동기 construction 실패만 `DEFINITELY_NOT_SENT`로 audit하고 잠정 POST budget을
  환급해 재시도합니다. `request.end` throw와 그 이후 외부 요청 여부가 불명확한 오류는 재발송하지
  않고 `UNKNOWN`으로 전환해 provider history를 조회합니다.
  PostgreSQL pool의 idle 연결과 transaction 중인 client 오류는 모두 처리해 process-level
  `EventEmitter` crash를 막고, 실패한 query는 기존 lease/fencing 복구 경로로 넘깁니다.
- 취소: Alarm row lock에서 cancel과 durable `STARTED` 및 provider `COMMITTED` fence를 직렬화합니다.
  cancel commit 뒤 provider commit은 거절되고 `FINISHED_NOT_SENT`로 정리합니다. 먼저 COMMITTED된
  요청만 실제 응답 또는 history로 정산합니다.
- 관측: 각 durable attempt의 시작·미발송·결과 저장 뒤 JSON 로그를 남깁니다. provider 호출이
  끝난 event에는 monotonic sender-boundary `providerDurationMs`도 포함합니다. maintenance leader는
  60초마다 DB queue/state/provider-window snapshot을 남기고, lease 회수 원인을 commit 뒤 별도 event로
  남깁니다. 로그에는 worker와 Alarm/Delivery/attempt/clientRef/lease 상관키만 포함하고
  수신자·알림 본문·provider raw payload는 포함하지 않습니다.

상세 구조, 후보 비교와 선택 근거는
[알림 발송 설계](docs/architecture/alarm-delivery-design.md), 레포 도입 근거는
[ADR-0001](docs/adr/0001-adopt-validated-repository-scaffold.md)에 있습니다. API 계약은
[OpenAPI 3.0](openapi.json)이 기준입니다.

## 실행

필요 도구는 Docker Compose입니다. 로컬 Node 실행 시에는 Node.js `24.19.0`, pnpm `11.22.0`을
사용합니다.

```bash
docker compose up
```

첫 실행도 이 명령 하나로 image build/pull, PostgreSQL migration, API/worker 기동까지 수행합니다.
API는 기본적으로 `http://127.0.0.1:3000`에만 노출됩니다. 포트나 로컬 자격 증명은 `.env.example`을
복사한 `.env`로 바꿀 수 있습니다.

```bash
curl http://127.0.0.1:3000/health/live
curl http://127.0.0.1:3000/health/ready
curl http://127.0.0.1:3000/docs/openapi.json
docker compose ps
```

대화형 Swagger UI는 `http://127.0.0.1:3000/docs`, 같은 화면이 읽는 canonical 계약은
`/docs/openapi.json`에서 제공합니다. UI asset은 browser가 `swagger-ui-dist@5` CDN에서 읽으므로
offline 환경에서도 JSON 계약은 계속 사용할 수 있지만 UI 렌더링에는 network가 필요합니다. 기본
Compose는 API 자체를 localhost에만 bind합니다. 외부 배포에서는 이 문서 endpoint도 다른 API와
동일하게 gateway 인증·접근 제어 뒤에 두거나 gateway에서 차단해야 합니다.

worker 수를 명시적으로 검증하려면 다음처럼 실행합니다.

```bash
docker compose up --scale worker=3
```

API와 worker는 `restart: unless-stopped`이며, migration은 checksum과 PostgreSQL advisory lock으로
보호되는 one-shot service입니다. API/worker는 runtime에 DDL을 실행하지 않습니다.
worker는 종료 시 신규 admission을 멈추고 이미 provider commit fence를 통과한 요청을 최대
35초 응답 timeout까지 정산하며, Compose는 이를 위해 45초 종료 유예를 둡니다.

## API 예시

대량 알림 생성:

```bash
curl -i -X POST http://127.0.0.1:3000/v1/alarms \
  -H 'content-type: application/json' \
  -d '{"type":"BULK","title":"서비스 안내","body":"02시에 점검을 시작합니다."}'
```

긴급 알림 생성:

```bash
curl -i -X POST http://127.0.0.1:3000/v1/alarms \
  -H 'content-type: application/json' \
  -d '{"type":"EMERGENCY","title":"결제 승인","body":"지금 결제를 승인해 주세요.","recipientIds":["u_000001","u_000002"]}'
```

응답의 `id`로 발송 시작, 조회, 취소를 요청합니다.

```bash
ALARM_ID=4ba646af-219d-4a92-aa75-2cfcb19874ef
curl -X POST "http://127.0.0.1:3000/v1/alarms/$ALARM_ID/send"
curl "http://127.0.0.1:3000/v1/alarms/$ALARM_ID"
curl -X POST "http://127.0.0.1:3000/v1/alarms/$ALARM_ID/cancel"
curl 'http://127.0.0.1:3000/v1/alarms?type=BULK&limit=50'
```

생성은 `201`, start/cancel은 비동기 접수 의미의 `202`입니다. 잘못된 syntax는 `400`, JSON이
아닌 생성 요청은 `415`, 의미상 validation 실패는 `422`, 현재 상태에서 불가능한 명령은 `409`이며
오류는 일관된 `application/problem+json`으로 반환합니다. 목록 cursor는 signed opaque keyset
cursor입니다.

공개 API enum은 다음과 같으며 코드, DB constraint, OpenAPI와 contract test로 동일성을 검사합니다.

- `AlarmType`: `BULK`, `EMERGENCY`
- `AlarmStatus`: `CREATED`, `QUEUED`, `EXPANDING`, `SENDING`, `CANCEL_REQUESTED`, `CANCELED`,
  `COMPLETED`, `PARTIALLY_FAILED`, `FAILED`, `ATTENTION_REQUIRED`

응답 representation은 JSON으로 고정하며 별도 `Accept` 협상을 도입하지 않았습니다. 따라서
`Accept: application/xml`도 정상 요청이면 JSON으로 응답하고 `406`을 만들지 않습니다. Nest/Fastify의
현재 route 계약에 따라 unknown path와 미등록 method는 모두 `404` problem으로 반환하며 `405`를
별도로 만들지 않습니다. 반면 생성 요청의 `Content-Type`은 정확히 `application/json`이어야 하며
그 밖의 media type은 `415` problem입니다.

대표 Alarm 응답은 다음 필드를 포함합니다.

```json
{
  "id": "4ba646af-219d-4a92-aa75-2cfcb19874ef",
  "type": "EMERGENCY",
  "title": "결제 승인",
  "body": "지금 결제를 승인해 주세요.",
  "status": "SENDING",
  "failureCode": null,
  "materializedCount": 0,
  "total": 2,
  "sent": 1,
  "failed": 0,
  "canceled": 0,
  "active": 1,
  "unresolved": 0,
  "duplicateCount": 0,
  "incidentCount": 0,
  "incidentSummary": {
    "count": 0,
    "codes": [],
    "firstDetectedAt": null,
    "lastDetectedAt": null
  },
  "audienceFinalizedAt": "2026-10-15T00:00:00.000Z",
  "expansionCompletedAt": null,
  "finishedAt": null,
  "createdAt": "2026-10-15T00:00:00.000Z",
  "updatedAt": "2026-10-15T00:00:01.000Z"
}
```

`audienceFinalizedAt`은 local audience가 완전히 확정됐다는 발송 barrier입니다.
`expansionCompletedAt`은 BULK users traversal 성공만 의미하므로 EMERGENCY에서는 `null`입니다.
`active`에는 아직 결과가 확정되지 않은 `UNKNOWN`도 포함되고, 장기 미확정은 `unresolved`로 이동해
`ATTENTION_REQUIRED`가 됩니다.

### Endpoint별 요청·응답·오류 계약

아래 표의 `Alarm`은 바로 위의 전체 JSON 표현을, `Problem`은 아래의 공통 오류 JSON을 뜻합니다.
body가 없는 `POST`도 `content-length: 0`인 빈 요청이며, 성공 응답은 모두 JSON입니다. 실제 schema와
nullable/required 조건의 최종 기준은 [OpenAPI 3.0](openapi.json)입니다.

| Endpoint | 요청 예시 | 성공 응답 예시 | 가능한 오류 |
| --- | --- | --- | --- |
| `GET /docs` | `curl -i http://127.0.0.1:3000/docs` | `200 text/html`; Swagger UI HTML | `500 Problem` |
| `GET /docs/openapi.json` | `curl -i http://127.0.0.1:3000/docs/openapi.json` | `200 application/json`; `{"openapi":"3.0.3",...}` | `500 Problem` |
| `POST /v1/alarms` | 위 BULK 또는 EMERGENCY JSON, `Content-Type: application/json` | `201`, `Location: /v1/alarms/{id}`, 생성 직후 `Alarm` | malformed JSON `400`; body 초과 `413`; 비JSON `415`; field/enum/길이/중복 `422`; `500`; DB 사용 불가 `503` |
| `GET /v1/alarms` | `?type=BULK&status=SENDING&limit=50&cursor={signed-opaque}`; 모두 선택 | `200 {"alarms":[<Alarm>],"nextCursor":null}` | syntax/위변조 cursor `400`; query 의미·범위 오류 `422`; `500`; DB 사용 불가 `503` |
| `GET /v1/alarms/{alarmId}` | path의 `alarmId`는 UUID | `200 Alarm` | UUID syntax `400`; 없음 `404`; `500`; DB 사용 불가 `503` |
| `POST /v1/alarms/{alarmId}/send` | 빈 body; 생성된 Alarm ID | `202 Alarm`; 이미 진행 중인 start도 같은 자원의 현재 표현 | UUID syntax `400`; 없음 `404`; terminal 등 시작 불가 상태 `409`; `500`; DB 사용 불가 `503` |
| `POST /v1/alarms/{alarmId}/cancel` | 빈 body; 생성된 Alarm ID | `202 Alarm`; 이미 `CANCEL_REQUESTED/CANCELED`면 멱등 접수 | UUID syntax `400`; 없음 `404`; 이미 완료되어 취소 불가 `409`; `500`; DB 사용 불가 `503` |
| `GET /health/live` | `curl -i http://127.0.0.1:3000/health/live` | `200 {"ok":true}` | 예기치 않은 handler 실패 `500 Problem` |
| `GET /health/ready` | `curl -i http://127.0.0.1:3000/health/ready` | PostgreSQL 사용 가능 시 `200 {"ok":true}` | 예기치 않은 handler 실패 `500`; PostgreSQL 사용 불가 `503 Problem` |

대표 validation 오류는 다음과 같습니다. 모든 오류 status의 content type과 필수 envelope는 같고,
`errors`만 field 오류가 있을 때 선택적으로 포함됩니다. `traceId`는 요청마다 생성된 상관키입니다.

```http
HTTP/1.1 422 Unprocessable Entity
content-type: application/problem+json

{
  "type": "about:blank",
  "title": "Unprocessable Entity",
  "status": 422,
  "code": "INVALID_ALARM",
  "detail": "Alarm input is invalid",
  "instance": "/v1/alarms",
  "traceId": "01JAA2M4YJ8QWJ8Y4C9H4M6X2T",
  "errors": [{"field":"recipientIds[1]","code":"DUPLICATE_RECIPIENT","message":"recipient IDs must be unique without normalization"}]
}
```

상태 충돌은 같은 envelope의 `409/ALARM_STATE_CONFLICT`, 없는 자원은
`404/ALARM_NOT_FOUND`, 지원하지 않는 생성 media type은 `415/UNSUPPORTED_MEDIA_TYPE`입니다.
unknown path와 미등록 method는 현재 route 계약대로 `404` Problem입니다. 생성·start·cancel은 한
DB transaction이 commit된 표현만 반환하며, `202`는 외부 provider 발송 완료가 아니라 durable
비동기 명령이 접수됐다는 뜻입니다.

## 상태·동시성·장애 경계

공개 Alarm 상태는 다음 흐름을 갖습니다. 화살표 사이의 외부 HTTP가 끝날 때까지 API request를
붙잡아 두지 않으며, terminal은 `CANCELED`, `COMPLETED`, `PARTIALLY_FAILED`, `FAILED`,
`ATTENTION_REQUIRED`입니다.

```text
                                  +---- expansion 오류/budget 소진 ----> FAILED
CREATED -- BULK send(202) --> QUEUED --> EXPANDING -- audience 원자 공개 --> SENDING
   |                                                                      |
   +-- EMERGENCY send(202) -- recipient snapshot 원자 공개 ---------------+

QUEUED / EXPANDING / SENDING -- cancel(202) --> CANCEL_REQUESTED --> CANCELED
SENDING -- active=0 --> COMPLETED | PARTIALLY_FAILED | FAILED | ATTENTION_REQUIRED
```

- BULK는 모든 page를 `alarm_expansion_recipients` staging에 저장하고 마지막 page transaction에서
  Delivery 전체, `audienceFinalizedAt`, `total/active`, `SENDING`을 함께 공개합니다. 그 전에는
  Delivery가 0개라 일부 audience만 발송될 수 없습니다. users API가 upstream point-in-time snapshot을
  보장하지 않으므로 이 barrier는 **우리 DB에 materialize한 집합**에 대한 보장입니다.
- EMERGENCY는 create transaction에서 1~100명의 opaque ID를 snapshot하고 start transaction에서
  Delivery와 audience counter를 함께 공개합니다. 존재 여부를 확인하기 위한 100,000명 scan은 하지
  않고 provider의 `UNKNOWN_RECIPIENT`로 영구 실패를 판정합니다.
- `CANCEL_REQUESTED`는 접수 직후의 비terminal 상태입니다. bounded sweeper가 아직 provider commit을
  통과하지 않은 Delivery를 `CANCELED`로 닫고, 이미 commit된 요청은 응답/history로 정산한 뒤
  Alarm terminal을 결정합니다. 따라서 cancel `202` 뒤에 일부 `sent`가 증가할 수 있습니다.

### 선형화 지점과 transaction 경계

| 동작 | 같은 PostgreSQL transaction에서 확정하는 것 | transaction 밖에서 하는 것 |
| --- | --- | --- |
| create | Alarm, EMERGENCY recipient snapshot | provider/users 호출 없음 |
| BULK start | Alarm 상태와 stable expansion run/job | `202` 뒤 users page GET |
| EMERGENCY start | Alarm lock, snapshot→Delivery, audience barrier/counter | `202` 뒤 provider POST |
| expansion page | Alarm→job fence 확인, recipient staging, cursor checkpoint | users GET과 retry sleep |
| expansion final page | staging→Delivery set-based publish, audience barrier/counter, cleanup `PENDING` | bounded staging cleanup |
| dispatch admission | Delivery claim 후 공통 `Alarm→Delivery` lock 순서로 cancel/barrier 재확인, durable attempt `STARTED/PREPARED` | Redis 예약 시각까지 대기 |
| provider handoff fence | `Alarm→Delivery→Attempt` lock에서 `PREPARED→COMMITTED`; cancel이 먼저 commit되면 거절 | 실제 socket `request.end`, 최대 35초 응답 대기 |
| provider 결과 | current generation attempt 종료, Delivery 상태, Alarm counter/terminal CAS | 없음; stale 결과는 evidence로만 append |
| reconcile | token/generation을 가진 Delivery claim 또는 evidence append; 채택 때 Alarm→Delivery와 evidence decision/counter를 원자 반영 | provider history GET, worker당 최대 8개 |
| cancel | Alarm row에서 cancel 시각·상태를 STARTED/COMMITTED와 직렬화 | 최대 500개씩 Delivery sweep |
| cleanup | stable run과 cleanup generation/token을 확인한 bounded 삭제 | 다음 batch poll |

외부 users/provider HTTP 중 DB row lock을 유지하지 않습니다. 여러 경로가 같은 row를 만날 때는
`Alarm→Delivery→Attempt` 순서를 사용하고, expansion/cleanup은 `Alarm→job→bounded staging` 순서를
사용합니다. claim, send, expansion, reconciliation, cleanup은 각각 lease generation과 무작위 token을
함께 검사합니다. lease가 만료돼 새 owner가 generation을 올리면 이전 owner의 heartbeat/result/page는
CAS 0행 또는 evidence-only가 되어 새 상태를 덮지 못합니다. 기본 send lease/heartbeat는
`60초/10초`, expansion·cleanup lease는 `30초`, reconciliation lease는 `15초`입니다.

### 외부 결과, retry와 timeout

| 관측 결과 | durable 판정 | 다음 동작과 상한 |
| --- | --- | --- |
| 유효한 `202 {messageId}` | `SENT`; DB 관측 시각을 `acceptedAt`에 기록 | 재POST 금지 |
| 모든 provider `400` | 알려진 code 또는 `UNKNOWN_PROVIDER_4XX`로 `FAILED_PERMANENT` | 재POST 금지 |
| `429 RATE_LIMITED` | `Retry-After` 우선 `RETRY_WAIT` | 일반 POST budget과 별도인 최대 20회/첫 429부터 5분 |
| `500 INTERNAL_ERROR`, `503 SERVICE_UNAVAILABLE` | `RETRY_WAIT`, 마지막 budget이면 `FAILED_RETRY_EXHAUSTED` | 최초 포함 일반 POST 최대 5회, full-jitter backoff |
| request 객체 생성 전 동기 실패 | `DEFINITELY_NOT_SENT`, attempt `FINISHED_NOT_SENT` | 잠정 POST budget 환급 후 bounded retry |
| request 객체 생성 이후 throw/timeout/EOF/abort/malformed·예상 밖 응답 | `UNKNOWN` | blind 재POST 금지, stable `clientRef`로 history-first reconciliation |
| matching history 1건 | `SENT`; 검증된 history `sentAt`을 `providerSentAt`에 기록 | 재POST 금지 |
| history 충돌·recipient 불일치·distinct messageId 2개 이상 | `NEEDS_REVIEW` 또는 `SENT`+incident/duplicate 표시 | 자동 재POST 금지 |
| opaque empty history가 빠른 조회 12회/60초 안에 끝내 양성 증거를 주지 않음 | `NEEDS_REVIEW`, Alarm `ATTENTION_REQUIRED` | 15분 간격 GET-only review를 24시간 수행; POST 금지 |

provider response timeout은 공식 Mock의 정상 30초 delayed-202보다 긴 35초입니다. 429 fallback은
1초이며 정수 초/HTTP-date를 100ms~60초로 clamp합니다. 5xx/definitely-not-sent retry 시각은 bounded
full jitter로 분산합니다. 모든 종류와 retry는 하나의 Redis TIME 기반 25ms paced slot을 공유하므로
목표는 40 POST/s이고 burst credit은 없습니다. limiter/Redis 상태를 확신할 수 없으면 신규 POST를
fail-closed하며 우회하지 않습니다.

공식 send `202`에는 `sentAt`이 없습니다. 따라서 `acceptedAt`은 provider가 202를 반환했거나 양성
history가 채택됐음을 우리 DB clock으로 기록한 시각이지 실제 발송 시각이 아닙니다.
`providerSentAt`은 history의 RFC 3339 `sentAt`을 검증한 경우에만 채웁니다. DB의 `COMMITTED`와 실제
socket handoff는 서로 다른 시스템의 원자 연산이 아니고 provider는 같은 `clientRef`를 deduplicate하지
않으므로, crash가 그 사이에 발생하고 history까지 비어 있으면 외부 exactly-once를 일반적으로 증명할
수 없습니다. 이 구현이 보장하는 것은 내부 fence/원자 counter, 확인된 side effect의 재POST 금지,
불명확한 건의 명시적 `NEEDS_REVIEW/ATTENTION_REQUIRED` 보존입니다. duplicate=0과 omission=0은
ambiguous window가 없었거나 provider의 idempotency/authoritative history capability가 별도로 검증된
profile에서만 조건부로 주장합니다.

## 공식 Mock 계약과 ambiguity 해석

기본 Compose는 과제 제공 image `ghcr.io/us-all/backend-assignment-api:1.2`를 tag 변경이나 동작
환경변수 override 없이 사용합니다. 애플리케이션이 의존하는 정확한 wire contract는 다음과 같습니다.

| API | 요청 | 성공 응답 | 오류·해석 |
| --- | --- | --- | --- |
| `GET /health` | body 없음 | `200 {"ok":true}` | Compose healthcheck가 사용 |
| `GET /v1/users?cursor&limit` | `cursor` 선택, `limit` 1~1000/default 100 | `200 {"users":[{"id":"...","name":"...","phone":"..."}],"nextCursor":"..."}`; 마지막은 `null` | 잘못된 limit `400 INVALID_REQUEST`, cursor `400 INVALID_CURSOR`; app은 opaque `id`만 저장하고 name/phone은 폐기 |
| `POST /v1/messages` | required non-empty `alarmId`, `recipientId`, `body`; provider상 선택인 `clientRef`도 app은 항상 전송 | `202 {"messageId":"..."}` | `400 INVALID_REQUEST/UNKNOWN_RECIPIENT/RECIPIENT_BLOCKED`; `429 RATE_LIMITED`+초 단위 `Retry-After`; `500 INTERNAL_ERROR`; `503 SERVICE_UNAVAILABLE` |
| `GET /v1/messages?clientRef=...` | non-empty `clientRef` 필수 | `200 {"messages":[{"messageId":"...","recipientId":"...","sentAt":"RFC3339"}]}` | 누락/빈 값 `400 INVALID_REQUEST`; 같은 clientRef의 모든 관측 항목을 반환 |

오류 body는 `{"code":"...","message":"..."}`이며 raw `message`는 저장·로그하지 않고 code만
allowlist하거나 `UNKNOWN_PROVIDER_4XX`로 축약합니다. users 응답이 빈 배열이어도 non-null
`nextCursor`면 순회를 계속하고 `nextCursor:null`만 종료 신호로 사용합니다. 같은 `clientRef` POST도
Mock은 새 발송을 만들 수 있으므로 이 값은 lookup correlation key이지 provider idempotency key가
아닙니다.

| 환경변수 | 공식 기본값 | 환경변수 | 공식 기본값 |
| --- | ---: | --- | ---: |
| `PORT` | `4000` | `USER_COUNT` | `100000` |
| `RATE_LIMIT` | `50` | `ERROR_RATE` | `0.05` |
| `TIMEOUT_RATE` | `0.02` | `TIMEOUT_MS` | `30000` |
| `SLOW_RATE` | `0.05` | `SLOW_MS` | `3000` |
| `BLOCKED_PERCENT` | `1` | `SEED` | `42` |

`ERROR_RATE`의 500/503과 blocked/429는 side effect가 없는 **known-unsent** 결과라 bounded retry 또는
영구 실패가 안전합니다. 반면 `TIMEOUT_RATE` cohort는 발송 side effect를 먼저 만들고 정상적으로
30초 뒤 202를 반환합니다. app의 response timeout은 이를 정상 수용하도록 35초이고, 그 뒤 timeout,
응답 body/transport 오류, COMMITTED 뒤 crash는 side effect 여부를 모르는 **ambiguous** 결과입니다.
이 둘을 같은 retry 분기로 합치지 않습니다.

send `202`에는 `sentAt`이 없고 history item에만 required `sentAt`이 있습니다. 특히
`{"messages":[]}`는 현재 관측에 항목이 없다는 뜻일 뿐, 공식 image가 visibility upper bound나
authoritative negative를 약속하지 않으므로 “발송되지 않았다”는 증거가 아닙니다. 기본
`PROVIDER_HISTORY_VISIBILITY=OPAQUE`에서 empty history로 재POST하지 않고 결국 review 상태를
보존합니다. `AUTHORITATIVE_EMPTY_AFTER_GRACE`는 그런 강한 계약을 별도 provider가 제공하고 검증할
때만 선택하는 비공식 profile이며, 공식 Mock에는 적용하지 않습니다.

공식 image 기본 `RATE_LIMIT=50`을 60개 동시 요청으로 직접 찌른 별도 clean 관측에서는 51~52개
202가 허용된 dependency deviation이 있어 Mock 자체의 exact boundary 검증은 통과로 주장하지
않습니다. 이는 애플리케이션의 보수적 40/s limiter와 별도 timestamp-audit Mock에서 검증하는 실제
ingress rolling 상한의 합격을 대체하거나 무효화하지 않으며, 두 결과를 서로 다른 증거로 기록합니다.

## 개발과 테스트

```bash
corepack enable
corepack prepare pnpm@11.22.0 --activate
pnpm install --frozen-lockfile
pnpm verify
docker compose config --quiet
```

`pnpm verify`는 format, lint, 계층 의존성, TypeScript, unit/contract test, 문서 검사, production
build를 실행합니다.

PostgreSQL/Redis integration 및 HTTP E2E test는 별도 로컬 포트를 여는 test override로 실행합니다.

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

테스트는 입력 경계, 상태 정책, API 계약, migration, concurrent `SKIP LOCKED` claim, stale lease
fencing, expansion publication barrier/cursor cycle/cancel, shared Redis pacing/cold restart barrier,
retry budget, cancellation sweeper, ambiguous-send reconciliation, late evidence와 cleanup을 다룹니다.

### 실행 확인 범위

2026-10-15 최종 current tree는 WSL Node.js 24.13.0 host에서 unit/contract/scaffold
24 files/327 tests와 PostgreSQL·Redis·HTTP integration/E2E 17 files/209 tests가 통과했습니다.
integration 전체는 crash rejection observer 보정 뒤 2회 연속 통과했고, 같은 제품 source의 고정
Node.js 24.19.0 production image build도 통과했습니다. format, lint, 104 modules·282 dependencies
계층 검사, TypeScript, traceability(29 requirements/260 canonical P0 tests), 공개 문서 8개 검사도
통과했습니다. 이 자동 suite green은 아래 canonical
catalog의 PARTIAL/NOT_RUN/DEPENDENCY_DEVIATION까지 통과했다는 뜻은 아닙니다.

앞선 전체 병렬 integration/E2E 실행에서는 고부하 `RET-09` case가 기본 30초 test timeout을 넘어
후속 suite가 연쇄 실패했습니다. 해당 test에만 60초 timeout을 명시했고 제품 retry/timeout budget과
합격 assertion은 바꾸지 않았으며, 최종 current-tree 전체 재실행은 통과했습니다. 같은 image의 10,000명
hard-kill 회귀에서는 worker를 차례로 3회 kill/restart한 뒤 258.325초에
`COMPLETED 10,000/10,000`, duplicate/unresolved/incident 0으로 수렴했습니다. DB에는
Delivery/unique recipient/unique clientRef가 각각 10,000개였고 10,029개
attempt는 `FINISHED_NOT_SENT=29`, 미종료 0이었습니다. 저장 evidence 10,000개는 모두 소비됐고,
provider history 10,000개 전수 대조에서도 누락·중복·recipient/messageId 위반이 0이었습니다.
15초 동안 응답 중인 committed 요청에 정상 종료를 걸었을 때도 15.331초 drain 후
`COMPLETED`로 마감했습니다.

직전 Node 24 runtime snapshot의 3-worker 100,000명 scale/priority/cancel 실행에서는 audience와
약 8.2초에 정확히 100,000명으로 확정했습니다. BULK 진행 중 EMERGENCY100이 먼저 완료됐고 그
window의 BULK ingress는 이미 발급된 허용 상한 3개 중 1개뿐이었으며, 합산 rolling 최대 42건과
위반 0을 기록했습니다. 이후 BULK 취소는 `SENT=289/CANCELED=99,711`,
active/unresolved/duplicate/incident 0으로 수렴했습니다. DB의 Delivery/unique recipient/unique
clientRef는 각각 100,000개, attempts 322개 중 미종료 0과 `FINISHED_NOT_SENT=33`이었고 cleanup 뒤
staging recipient/cursor도 0이었습니다. 이는 해당 snapshot의 100,000명
materialization·priority·cancel 증거이며, 100,000건을 끝까지 모두 발송한 full-send 증거로 표현하지
않습니다. 이후 permit-only prefetch와 bounded shutdown이 제품 dispatch 경로를 바꿨으므로 이
100,000명 결과는 current-tree release 증거로 재사용하지 않으며, 최종 제출 전 100k re-soak이
필요합니다.

변경 후 clean 3-worker RL-07은 10초 warm-up+60초 고정 window에서 2,323 ingress/59.976초,
즉 `38.732/s`를 기록했습니다. ingress와 adapter commit의 rolling 1초 최대는 모두 41건이고 limiter
기인 429와 rolling 위반은 0건이었습니다. worker당 active DB admission 1개와 row를 소유하지 않는
raw prepared permit 1개만 허용해 provider-COMMITTED 전 reservation 합계를 최대 2로 고정하며,
exact kill test에서 그 2개를 포착한 상태의 durable attempt와 provider POST가 모두 0임을 확인했습니다.

같은 pre-final-review Node 24 runtime snapshot의 별도 100,000명 resource/crash 표본에서는
43,000번째 page checkpoint 뒤 worker를 hard-kill했을 때 staging/materialized 43,000, Delivery 0을
보존했고, 재기동 뒤 같은 run이 offset
100개를 끝까지 방문했습니다. 경계 page replay는 1회였고 unique staging이 흡수했습니다. API 상세
319회는 p95 6.098ms/p99 8.360ms였으며 API V8 heap은 25.49MiB에서 34.04MiB peak, worker peak는
38.18~44.34MiB였습니다. 정확히 100,000개 Delivery를 공개한 뒤 cleanup `RUNNING` 중 다시 kill해도
terminal 결과는 유지됐고 lease takeover 후 staging 0으로 정리됐습니다. 이어진 한 20초 3-worker
표본은 완료 306/239/247, max in-flight 120, ingress rolling 최대 41/위반 0이었고, cancel은 5.654ms에
접수되어 20.072초 뒤 `SENT=1,049/CANCELED=98,951`, active/미종료 attempt 0으로 수렴했습니다.
이는 WSL2/Docker Desktop/PostgreSQL 18.6의 local 표본이지 portable latency·heap·처리량 SLO가 아닙니다.

이전 코드 단계에서 실행한 official Mock 기본 설정 100,000명 run은 정확히 100,000개 Delivery를
publish하고 `SENT=99,001`, `FAILED_PERMANENT=999`, `429=0`, duplicate/unresolved/incident 0으로
종료했습니다. 별도 100,000명 cancel run은 `SENT=455`, `FAILED=3`, `CANCELED=99,542`, active 0으로
수렴했습니다. 이 두 장기 결과는 직전 controlled runtime snapshot의 증거와도 구분합니다.

결정적 profile에서 worker 3대 중 2대를 BULK 처리 중 hard-kill한 이전 run은
`COMPLETED 2,000/2,000`으로 수렴했습니다. 종료 직전 `PREPARED` 2건은 provider 미요청이
증명되어 안전하게 재처리되었고, `COMMITTED` 3건은 history로 확인한 뒤 마감되어
미종료 attempt·`429`·중복·미해결이 모두 0건이었습니다. 추가로 Redis flush와 전면 중단,
PostgreSQL restart와 전면 중단, 실제 provider 429 재시도, worker 0→3 복구, API 중단 중
worker 계속 처리를 확인했습니다. 같은 runtime snapshot의 별도 ingress timestamp 제어 Mock에서는
동시 긴급 알림 3개가 300/300으로 끝날 때 rolling 1초 최대 41건이었고, 10,000명 BULK 도중 긴급 100명을
넣은 실행은 긴급 window에서 신규 BULK 요청 0, rolling 최대 43건이었습니다. 긴급 완료 뒤 BULK를
취소해 `SENT=305/CANCELED=9,695`로 수렴했습니다. mixed 429/500/503/slow 100명은 115 attempts로
완료했고 rolling 최대 41건이었습니다. 세 실행 모두 rolling 위반과 limiter 기인 429는 0건입니다.

이전 코드 단계에서 official Mock 기본 100,000명에 worker hard-kill/restart를 20회 반복한 약
49분 35초 run은
`SENT=98,997`, `FAILED=999`, `NEEDS_REVIEW=4`, active/duplicate/incident 0으로
`ATTENTION_REQUIRED`에 정확히 수렴했습니다. DB attempt 107,111건은 모두 종료됐고 429는
0건이었습니다. 100,000개 clientRef의 provider history를 전수 대조해 SENT는 정확히 history 1건이며
recipient/messageId가 일치하고, 실패와 NEEDS_REVIEW는 history 0건이며, 불명확 attempt 뒤 blind
retry가 없음을 확인했습니다. opaque history에서는 이 결과가 합격이며 외부 exactly-once 성공으로
표현하지 않습니다.

다만 DB의 `COMMITTED`와 실제 socket handoff는 하나의
원자 연산이 아니므로, history가 빈 최악의 crash window는 blind resend 대신
`NEEDS_REVIEW/ATTENTION_REQUIRED`로 보존합니다. 이는 장애 phase 전체 조합이나 장시간 반복
성능 matrix 전체, 공개 저장소 anonymous clone과 실제 제출까지 끝났다는 뜻은 아닙니다.

### 요구사항·기능·검증 매트릭스

`required`는 과제 원문 필수, `project-required`는 원문 선택 기능이지만 이 구현에서 필수로 승격한
범위, `future-optional`은 의도적으로 채택하지 않은 후속 범위입니다. 원문 사실과 이 프로젝트의
채택 결정을 섞지 않기 위해 source priority를 별도 열에 둡니다. `project-required`도 제출 gate에서는
`required`와 똑같이 구현·검증 완료돼야 합니다.

Implementation은 현재 source의 `unimplemented`/`partial`/`implemented`, Verification은 현재 source에
대한 `not-run`/`partial`/`passed`/`failed`를 뜻합니다. `passed`는 자동 test, current-tree 정적 검사,
또는 명시된 runtime evidence가 acceptance 전체를 충족할 때만 씁니다. 과거 snapshot, 설계 문서,
서로 따로 통과한 두 시나리오를 결합 acceptance의 통과로 재사용하지 않습니다. 실행 중인 exact/soak는
끝나기 전까지 `partial` 또는 `not-run`이고, dependency deviation은 애플리케이션 합격과 분리합니다.

| 범위 | Source priority | Implementation | Verification |
|---|---|---|---|
| BULK 생성·start·목록·상세·기본 cancel | required | implemented | passed |
| 100,000명 cursor pagination, staging barrier, 정확한 audience 공개 | required | implemented | partial — page/barrier 자동 검증은 통과, current revision official 100k full-send/re-soak는 미완료 |
| 공용 외부 API 예산, 모든 종류·재시도 합산 rolling 1초 50건 이하 | required | implemented | partial — limiter/controlled ingress core는 통과, current revision exact soak 재검증은 미완료 |
| 202/400/429/500/503/slow/timeout 분류, bounded retry와 history-first reconciliation의 결정적 core | required | implemented | passed |
| 실제 35초 delayed-202와 reconcile/review 각 지점 OS-worker kill 조합 | required | implemented | partial — repository failpoint는 통과, exact runtime 조합은 미실행 |
| API/worker 동시 접근, Alarm→Delivery lock 순서, lease generation/token fencing의 결정적 race | required | implemented | passed |
| expansion cancel finalizer와 crash/reconcile의 catalog 전 지점 OS-worker kill 조합 | required | implemented | partial — transaction rollback/takeover core는 통과 |
| EMERGENCY 지정 수신자 1~100명 | project-required | implemented | passed |
| BULK 처리 중 EMERGENCY strict priority와 reserved HTTP capacity | project-required | implemented | partial — 결정적 priority/capacity test는 통과, current exact saturation/kill 결합 run은 미완료 |
| worker 3개 분산 처리, live 1↔3 scale와 순차 hard-kill 회수 | project-required | implemented | partial — 자동 claim/fence test와 과거 runtime은 통과, current exact matrix는 미완료 |
| 각 `N={1,2,3,5}` clean run마다 scale-in/out을 결합한 exact matrix | project-required | implemented | partial — 과거 N별 run과 recorded 1↔3 증거는 분리 보존 |
| current revision의 official Mock worker=3 in-flight SIGKILL black-box gate | project-required | implemented | partial — 과거 코드 증거만 보존, current rerun 미실행 |
| BULK 포화 중 EMERGENCY commit 직후 일반-slot worker SIGKILL 교차 조합 | project-required | implemented | partial — priority와 kill은 각각 통과, current exact 결합은 미실행 |
| 모든 endpoint 요청/응답/error와 상태·transaction·retry·timeout·cancel·rate·lease/fencing·exactly-once 한계 문서 | required | implemented | passed — README/OpenAPI/current source를 current-tree 정적 대조 |
| 공식 Mock image/route/error/default/empty-history ambiguity 문서 | required | implemented | passed — 문서 계약은 current-tree 대조; direct 50/s probe의 dependency deviation은 별도 보존 |
| source priority와 implementation/verification 분리, 미검증·미구현 공개 | required | implemented | passed — 이 matrix의 structural contract를 current-tree 검사 |
| 승격 기능 최종 제출 gate(SUB-06) | project-required | implemented | partial — 위 project-required exact runtime 행이 전부 `passed`가 되기 전에는 닫지 않음 |
| 수신자별 delivery 공개 조회 endpoint | future-optional | unimplemented | not-run |
| create `Idempotency-Key` | future-optional | unimplemented | not-run |
| 업무 event/outbox consumer | future-optional | unimplemented | not-run |
| 공개 gateway 인증·RBAC·TLS | future-optional | unimplemented | not-run |
| Public repository anonymous clone과 실제 제출 채널 확인 | required | unimplemented | not-run — 공식 과제 시작·제출 전 |

## 구조

```text
apps/
  api/                     # Nest/Fastify HTTP composition root
  migrate/                 # versioned SQL one-shot migration
  worker/                  # controller-less background composition root
packages/
  alarms/src/
    domain/                # framework/DB/HTTP를 모르는 불변조건
    application/           # use case와 outbound port
    infrastructure/        # PostgreSQL, Redis, provider adapter
    presentation/          # controller와 problem mapping
  platform/                # 비즈니스 중립 공통 런타임 유틸리티
migrations/                # immutable versioned SQL
tests/                     # unit, contract, integration, E2E
```

의존 방향은 `presentation/infrastructure -> application -> domain`입니다. API와 worker는 조립만
담당합니다. `.dependency-cruiser.cjs`가 domain/application의 역방향 의존을 CI에서 막습니다.

## 데이터 처리와 성능 고려

- API 요청은 100,000명 배열을 만들거나 발송 완료를 기다리지 않습니다. BULK start는 durable job만
  등록하고 `202`를 반환합니다.
- users pagination은 page 단위 transaction과 unique staging key를 사용합니다. cursor는 raw log에
  남기지 않고 SHA-256 hash로 cycle을 탐지합니다.
- audience 확정 전 Delivery는 0개입니다. 마지막 page에서 set-based `INSERT ... SELECT`와 total/active
  갱신을 함께 commit해 partial publication을 막습니다.
- 목록은 `(created_at, id)` keyset pagination, dispatch는 partial priority index와 한 건 JIT claim을
  사용합니다. HTTP 호출 중 DB row lock을 유지하지 않습니다.
- Alarm 상세 조회는 100,000 Delivery를 매번 집계하지 않고 Delivery 전이 transaction에서 함께
  갱신한 stored counters를 읽습니다. maintenance worker의 read-only consistency detector가 실제
  Delivery 행과 다른 projection을 구조화 alert로 노출하되 조회 값을 몰래 대체하거나 자동 repair하지
  않습니다. detector는 유휴 loop 300회당 한 번(현재 200ms idle delay에서 약 60초)만 시도하고,
  PostgreSQL session advisory lock을 잡은 worker 한 대만 전체 집계를 수행합니다.
- cancel과 cleanup은 bounded batch로 진행해 하나의 100,000-row 장기 transaction을 만들지 않습니다.
- worker당 HTTP 동시성은 64이고 마지막 2 slot은 긴급 알림 전용입니다. permit을 먼저 예약한 뒤
  global-priority row 한 건을 claim해 limiter wait queue에서 priority가 역전되지 않게 합니다. active
  DB admission 1개와 raw next permit 1개만 겹치며, raw permit activation 직전에 priority와 62/64
  capacity를 다시 계산합니다. 늦거나 shutdown된 raw permit은 claim 없이 폐기합니다.

## 설계 해석과 한계

- users API에는 version/snapshot token이 없으므로 BULK 결과는 pagination 동안 얻은 local materialized
  set입니다. upstream의 강한 시작 시점 snapshot을 보장한다고 주장하지 않습니다.
- provider는 같은 `clientRef`의 중복 POST를 제거하지 않으며 history visibility 상한도 제공하지
  않습니다. 따라서 process crash의 가장 좁은 ambiguous window에서 외부 exactly-once를 수학적으로
  보장할 수 없습니다. 이 구현은 stable `clientRef`, durable STARTED, lookup-first, blind retry 금지로
  중복 위험을 우선 차단하고, 끝내 확인되지 않으면 `NEEDS_REVIEW/ATTENTION_REQUIRED`로 노출합니다.
  `NEEDS_REVIEW`는 24시간 동안 15분 간격의 GET-only review로 늦은 positive history를
  교정하며, 보존 창 만료 후에는 자동 GET을 멈추고 미확정 상태를 그대로 노출합니다.
  Compose의 `PROVIDER_HISTORY_VISIBILITY` 기본값은 반드시 `OPAQUE`입니다. 별도 provider가 설정한
  grace 뒤 빈 history를 확정 미발송으로 보장하는 계약을 제공할 때만
  `AUTHORITATIVE_EMPTY_AFTER_GRACE`를 선택할 수 있으며, 공식 Mock에는 이 강한 profile을 적용하지
  않습니다.
- 긴급 우선은 이미 시작한 외부 요청의 preemption이 아니라 다음 eligible claim부터의 strict priority입니다.
  긴급 요청이 무한히 들어오면 BULK starvation이 가능하며, fairness가 필요해지면 aging/quota 정책을
  별도 요구로 도입해야 합니다.
- 과제 app 인증은 의도적으로 범위 밖이며 API는 localhost에만 bind됩니다. 실제 외부 공개 전에는
  gateway 인증/인가, TLS, secret manager와 감사 정책이 필요합니다.
- Redis 또는 DB를 관측상 사용할 수 없으면 신규 발송은 fail-closed입니다. Redis cold start 때는
  PostgreSQL에 1.2초 resume barrier를 먼저 기록한 후 limiter를 재초기화합니다.
- Compose의 **초기 기동**은 migration과 PostgreSQL/Redis/Mock health를 모두 gate해 준비 전
  API·worker traffic을 막습니다. 기동 후 `/health/ready`는 API가 직접 사용하는 PostgreSQL만
  확인합니다. 따라서 Redis/provider 장애 때 API readiness가 200을 유지하는 것은 정상이며,
  worker가 신규 permit/POST를 fail-closed하는지로 발송 경로의 안전성을 판정합니다.
- PostgreSQL 연결 오류 로그에는 component/event/error code만 남기며 접속 문자열, host, 자격 증명,
  driver error message는 남기지 않습니다. 연결 오류를 삼켜 성공으로 간주하지 않고 query failure와
  transaction rollback은 그대로 호출자에게 전파합니다.

## 관측 신호와 장애 대응

dispatch worker는 durable attempt가 시작·미발송 확정·결과 확정될 때 한 줄 JSON을 표준 출력에
기록합니다. `alarmId`, `deliveryId`, `attemptId=traceId`, `clientRef`, `attemptNo`, `workerId`,
`leaseGeneration`, `reservationId`로 API 응답의 Alarm부터 provider history까지 추적할 수 있습니다.
provider 호출을 수행한 최종 event의 `providerDurationMs`는 wall clock이 아니라 호출 직전/settle 직후의
monotonic clock 차이이며 `providerDurationSource=MONOTONIC_SENDER_BOUNDARY`로 경계를 명시합니다.
expansion 중 같은 page 또는 이전 page와 중복된 수신자를 발견하면 recipient 값 대신
`receivedCount/insertedCount/duplicateCount`만 경고로 남깁니다. 수신자 ID·이름·전화번호·본문·provider
raw payload는 두 이벤트 모두에 포함하지 않습니다.

maintenance worker 중 PostgreSQL advisory lock을 가진 한 process만 최초 실행과 이후 60초 cadence에
`OPERATIONAL_SNAPSHOT`을 기록합니다. 한 DB statement에서 queue/due depth, oldest due age, SENT/failed/
RETRY_WAIT/UNKNOWN+RECONCILING/NEEDS_REVIEW, active send lease, 최근 60초 provider attempt/accepted/429와
accepted throughput을 읽습니다. terminal 결과는 100,000 Delivery를 다시 훑지 않고 Alarm stored
projection에서 읽고, non-terminal gauge는 상태별 partial index, 최근 attempt는 `finished_at` partial
covering index로 제한합니다. 이 관측 task는 durable sweep/reaper와 겹치지 않게 한 개만 실행하되 worker
loop에서는 await하지 않고, shutdown에서만 drain합니다. N개 worker가 같은 전역 gauge를 N번 내보내지
않으며 query/sink 실패는 발송 상태 머신을 중단하지 않습니다. `SEND_LEASE_RECOVERED`는 회수 transaction commit 뒤
PREPARED requeue/cancel과 COMMITTED-or-missing-attempt UNKNOWN 원인을 구분합니다. 이 event들을
collector에서 count/histogram으로 집계할 수 있지만, 현재 `/metrics` endpoint·metric backend·분산 trace
collector·alert delivery는 제공하지 않습니다.

같은 worker process의 lifecycle, dispatch, expansion, maintenance, reconciliation/DB 오류 record에는
동일한 boot-unique `processInstanceId`가 들어갑니다. 기존 `workerId`도 같은 값을 유지하므로 component가
달라도 한 process의 시작부터 종료까지 연결할 수 있고, 재시작하면 새 값으로 바뀝니다.

API는 PostgreSQL readiness가 `READY↔NOT_READY`로 바뀔 때만, API/worker는 startup·signal shutdown·stop/
failure 때, migrate는 성공 건수 또는 실패의 allow-list error metadata를 JSON으로 남깁니다. error
message·stack·접속 문자열·migration 파일명은 기록하지 않습니다.

| 감지 신호 | 즉시 동작 | 복구 확인 |
| --- | --- | --- |
| `/health/ready` 503 또는 `POSTGRES_*_CLIENT_ERROR` | 신규 변경을 중단하고 PostgreSQL 연결·migration 상태를 먼저 복구한다. query 실패를 성공으로 간주하거나 lease를 수동 삭제하지 않는다. | readiness 200, migration version/checksum, expired lease의 fenced takeover를 확인한다. |
| Redis 연결 실패·cold recovery | limiter를 우회하지 않는다. 신규 provider POST는 fail-closed로 유지한 채 Redis를 복구한다. | PostgreSQL resume barrier 이후 paced reservation과 rolling 1초 상한을 확인한다. |
| provider 429 증가 | worker 수나 발송률을 올리지 않고 `Retry-After`, limiter audit, 실제 ingress를 대조한다. | 429 전용 budget 안에서 분산 재시도되고 합산 rolling 상한이 회복됐는지 확인한다. |
| provider timeout/history unavailable | ambiguous attempt를 5xx와 같은 미발송으로 바꾸거나 blind retry하지 않는다. 필요하면 worker를 중단해 durable 상태를 보존한다. | stable `clientRef` history로 정산되거나 `NEEDS_REVIEW`로 명시적으로 남는지 확인한다. |
| `SEND_LEASE_RECOVERED` 증가 | `reason`, request phase, resulting state를 확인하고 worker 종료·DB stall 시각과 대조한다. COMMITTED를 READY로 수동 변경하지 않는다. | PREPARED는 새 fenced attempt로 진행되고 COMMITTED/missing attempt는 history-first로 수렴하는지 확인한다. |
| `ALARM_COUNTER_INCONSISTENCY` | 자동 repair하지 않고 해당 Alarm의 Delivery source row와 incident projection을 조사한다. | 잠금·버전·감사를 포함한 별도 교정 명령 전까지 공개 projection이 임의로 바뀌지 않았는지 확인한다. |

## 주요 기술 선택 요약

| 결정 | 검토 후보 | 선택과 근거 | 비용/재검토 조건 |
| --- | --- | --- | --- |
| 작업 전달 | PostgreSQL queue, RabbitMQ/SQS, Redis Streams | 이미 필요한 PostgreSQL의 transaction·`SKIP LOCKED`로 API 상태와 work 생성의 dual-write를 제거 | 독립 broker 수준의 격리·처리량이 필요하면 outbox+broker 재검토 |
| 전역 속도 | local limiter, fixed/token bucket, Redis paced slot | worker 전체가 한 Redis server time과 Lua 상태를 공유하고 burst 0으로 50/s 아래를 보수적으로 shape | Redis 운영비와 fail-closed 가용성 비용 |
| BULK audience | page별 즉시 Delivery, 메모리 수집, staging 후 publish | staging 후 마지막 set-based publish로 부분 발송과 heap 폭증을 동시에 방지 | 마지막 transaction의 WAL/lock 비용은 부하 측정 대상 |
| ambiguous send | 즉시 retry, 영구 실패, history reconciliation | 즉시 retry의 중복과 영구 실패의 누락 중 history-first가 provider 계약에서 가장 보수적 | provider가 idempotency key를 보장하면 단순화 가능 |
| 집계 | 조회 때 COUNT, event/outbox projection, 같은 transaction stored counters | 결과 전이와 같은 DB transaction에서 O(1) 상세 조회와 일관성을 확보 | 별도 분석 consumer가 생기면 outbox projection 재검토 |

상세한 후보·근거·trade-off·재검토 조건은 설계 문서에서 이어집니다. 형식상 대안이 없는 HTTP
field 이름이나 provider 고정 계약에는 억지 후보 비교를 만들지 않았습니다.

## 구현 회고와 후속 작업

- 검증 초기에는 upstream 오류 message를 진단 목적으로 보존하는 선택을 검토했지만, 큰 payload와
  전화번호가 그대로 유입되는 반례 때문에 allowlist code만 전달하고 raw message는 adapter 경계에서
  폐기하도록 바꿨습니다.
- late positive와 이미 예약된 retry가 겹치는 회귀에서 `RETRY_WAIT`가 재claim될 수 있음을 발견해,
  Alarm→Delivery→Attempt lock 안에서 positive를 우선 채택하고 새 PREPARED/COMMITTED attempt를 각각
  명시적으로 정리하는 방식으로 수정했습니다.
- counter consistency scan을 매 idle poll에 실행하는 안은 100,000 Delivery에서 불필요한 DB 부하를
  만들었으므로 약 60초 cadence와 PostgreSQL advisory leader 한 대만 실행하는 구조로 축소했습니다.

시간과 운영 계약이 더 주어지면 우선할 항목은 현재 JSON signal을 수집할 OpenTelemetry collector와
histogram/alert rule, operator-led incident 교정 명령, CDN이 아닌 자체 호스팅 Swagger asset과 CSP입니다. provider가
idempotent POST 또는 authoritative history visibility를 공식 보장할 때에만 현재 ambiguity 정책을
단순화합니다. 공개 저장소 anonymous clone과 실제 제출 채널 검증은 과제 시작 뒤 별도 수행합니다.
