# Repository layout and boundaries

This repository adopts only reviewed workspace and verification patterns without inheriting an
unrelated application's business domains or infrastructure choices.

`apps/api` and `apps/worker` are composition roots. Alarm behavior belongs to the `alarms` bounded
context, split into domain, application, infrastructure, and presentation layers. The `platform`
package is deliberately narrow: a component does not become platform code merely because API and
Worker both use it.

Provider response mapping, durable-job claiming, repository queries, and rate-limit semantics all
encode alarm workflow knowledge. They therefore remain inside `alarms/infrastructure`, behind
application ports. This prevents generic utilities from becoming an unowned business layer.

The implementation now follows these boundaries across the Alarm API, PostgreSQL queue and
repositories, Redis limiter, provider adapters, and controller-free worker roles. Directory README
files describe placement only; runtime behavior is documented in
`docs/architecture/alarm-delivery-design.md` and verified by executable tests.

Scaled workers must derive a unique default identity from their runtime hostname. Do not hard-code
a shared `WORKER_ID` in Compose; an explicit override is only safe when the deployer guarantees a
different value per replica.
