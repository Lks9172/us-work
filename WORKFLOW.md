# Development workflow

## 1. Establish the contract

When local private planning materials are available, trace the source requirement through its
detailed design and every matching test scenario before changing code. Source features labelled
optional are mandatory for this project. Never publish private source-derived planning materials.

## 2. Record a real decision when needed

If implementation requires a meaningful choice, document the context, viable candidates, selected
candidate, evidence-based rationale, rejected trade-offs, and reversibility. Do not manufacture
alternatives for routine code placement.

## 3. Implement from the inside out

1. Domain invariant and unit test
2. Application port/use case and unit test
3. Infrastructure or presentation adapter and integration test
4. API/Worker composition
5. Concurrency, recovery, and end-to-end scenarios where applicable

Business-aware adapters stay in `packages/alarms/src/infrastructure`; only neutral shared mechanics
belong in `packages/platform`.

## 4. Verify

```bash
pnpm format
pnpm verify
docker compose config --quiet
```

When runtime dependencies are implemented, run the relevant Compose-backed integration and
end-to-end suites as well. Never describe an unimplemented or unexecuted suite as passing.

## 5. Review the delta

Check `git diff --check`, `git status --short`, generated files, accidental secrets, dependency
direction, and documentation/test traceability before committing.
