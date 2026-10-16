# Contributing

Keep each change tied to one or more requirement and test identifiers from the specification set.
Prefer small commits that preserve a runnable repository.

Before opening a pull request:

1. Update the design or ADR when a real architectural decision changed.
2. Add tests at the lowest useful level, plus integration/concurrency coverage where required.
3. Run `pnpm format`, `pnpm verify`, and `docker compose config --quiet`.
4. Confirm that no secret, personal source document, build output, or unrelated source code is staged.
