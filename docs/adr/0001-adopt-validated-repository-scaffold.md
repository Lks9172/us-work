# ADR-0001: Adopt only validated repository-scaffold patterns

- Status: Accepted
- Date: 2026-10-14

## Context

The assignment needs an independent local implementation repository. Previously validated NestJS
scaffold patterns are available for workspace layout, runtime bootstrapping, compilation, lint,
tests, architecture checks, and concise repository documentation. The source application's domain
and delivery architecture solve a different problem and must not be inherited.

The repository must be useful immediately without pretending that assignment behavior has already
been implemented.

## Requirement and problem mapping

| Requirement ID | Problem this decision addresses |
| --- | --- |
| `R-STACK-01`, `R-WORKER-01`, `R-COMPOSE-01` | Establish TypeScript/Nest API and worker bootstraps with enforceable process and module boundaries. |
| `R-RUN-01` | Make the independent repository buildable and runnable from its own lockfile and production image path. |
| `R-CHOICE-01`, `R-AI-01` | Reuse only explainable patterns whose constraints match this assignment. |
| `R-CONF-01`, `R-SUBMIT-01` | Keep protected source material, unrelated history, and local-only evidence out of the public artifact. |

## Candidates

### A. Copy an existing starter repository wholesale

Meets the short-term speed goal and inherits already working build plumbing. It does not meet
independence, minimal-scope, explainability, or public-artifact hygiene: it can import unrelated
domain behavior, persistence and messaging choices, background processes, agent tooling, Git
history, and dependencies. Deleting the excess afterward makes accidental coupling harder to audit.

### B. Reproduce only reviewed scaffold patterns

Create an independent repository and reproduce only the reviewed version pins, pnpm workspace
structure, Nest/Fastify and application-context bootstraps, strict TypeScript/SWC, Biome, Vitest,
dependency-cruiser, CI, and repository guidance. Keep private assignment-derived design documents
as an ignored local baseline, while leaving domain directories empty.

This meets independent history, bounded reuse, API/worker process structure, enforceable dependency
direction, and public/private separation. Its cost is deliberate review and manual reproduction of
the small accepted set instead of a one-command clone.

### C. Start from a generated blank Nest project

This meets independence and avoids inherited domain assumptions. It does not meet the timebox as
well as B because it discards already validated multi-process workspace and boundary-checking
conventions; the same non-product plumbing would need to be reconstructed and reviewed before
feature work.

## Decision

Choose candidate B.

It reuses the parts whose constraints match this assignment and excludes the parts whose business
or operational assumptions do not. The selected patterns are independently represented and
verified by this repository's own lockfile, configuration, architecture checks, and tests. An
independent Git repository prevents accidental history or worktree coupling.

The alarm package starts with explicit Clean Architecture boundaries. Alarm-aware PostgreSQL,
Provider, and Redis adapters will live below `packages/alarms/src/infrastructure`; they are not
generic platform services merely because more than one process may call them.

No broker is added. The accepted software design uses PostgreSQL as the future durable job source
and Redis for future global rate limiting. Importing broker/outbox code would add a second delivery
model without a requirement-based benefit.

## Consequences

- Feature implementation begins from a small, buildable API/Worker workspace.
- Unrelated source code, repository history, and local working changes are not imported.
- Private assignment-derived working documents stay local and are excluded from Git, CI checkouts,
  Docker build contexts, and runtime images.
- At scaffold-adoption time, the PostgreSQL schema, Redis scripts, provider client, APIs, workers,
  and specification evidence still required implementation. Their current implementation and
  verification status is maintained in the README instead of being frozen in this historical ADR.
- Some configuration follows the selected scaffold patterns by design, but this repository owns
  its lockfile, Git history, package names, architecture rules, and future decisions.
- A future broker or ORM adoption requires a new decision based on demonstrated need rather than
  starter-repository precedent.

## Verification

- Canonical scenario IDs: `STACK-01`, `ARC-01~03`, `OPS-02`, `DOC-14`, `DOC-17`, `SUB-01~03`.
- Evidence note: production bootstraps, dependency-boundary inspection, clean Compose startup, and
  public reference/secret/image-context scans are distinct evidence layers. Passing a scaffold test
  does not prove notification behavior has been implemented.

## Revisit conditions

Revisit only if the repository ceases to be an independent submission, the organization mandates a
different maintained starter, or measured maintenance cost shows that an adopted toolchain choice
is riskier than replacing it. A broker, ORM, or copied domain still requires its own requirement-
based decision; this scaffold ADR never authorizes one implicitly.
