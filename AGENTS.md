# Repository instructions

## Read first

1. `README.md`
2. `MAP.md`
3. `WORKFLOW.md`
4. 로컬 private planning material이 있으면 변경 대상의 requirement/design/test 절

## Scope and truth

- 원문에서 선택 기능인 항목도 이 프로젝트에서는 모두 필수다.
- 현재 저장소에는 핵심 API/worker/runtime이 구현되어 있다. 문서의 상태 표시는 실제 코드와 실행
  증거로 재검증하고, 설계됐거나 계획된 항목을 자동으로 구현 완료로 간주하지 않는다.
- 요구사항, 설계, 테스트 추적성이 충돌하면 먼저 문서를 함께 정합화한다.
- 새 설계 결정은 실제 후보가 있을 때 후보, 선택, 근거, trade-off를 ADR이나 설계문서에 남긴다.
  형식적인 후보를 억지로 만들지 않는다.

## Architecture boundaries

- `alarms/domain`은 프레임워크, DB, Redis, HTTP를 모르는 순수 계층으로 유지한다.
- `alarms/application`은 domain과 port에만 의존하고 adapter에 의존하지 않는다.
- PostgreSQL repository, Provider client, Redis limiter처럼 알람 의미를 아는 adapter는
  `alarms/infrastructure`에 둔다.
- `platform`에는 여러 앱이 공유하는 비즈니스 중립 기능만 둔다.
- 메시지 브로커를 기본 전제로 추가하지 않는다. 현재 기준은 PostgreSQL durable queue다.

## Change discipline

- 비밀값, `.env`, 원본 과제 자료, 개인 참고 문서를 커밋하지 않는다.
- 비공개 source-derived 작업 자료는 Git, Public CI, Docker image에 포함하지 않는다.
- 기능 변경에는 해당 요구사항의 정상·경계·실패·동시성 테스트를 함께 추가한다.
- 의존성을 추가하기 전에 실제 사용처와 더 작은 대안을 확인한다.
- 생성물과 대용량 의존성은 Git에 넣지 않는다.
- 완료 전 `pnpm verify`와 `docker compose config --quiet`를 실행한다.
