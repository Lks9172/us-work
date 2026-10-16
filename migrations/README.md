# Migrations

Versioned PostgreSQL migrations are applied only by the one-shot `migrate` process. API and Worker
processes never run DDL at startup.

Rules:

- file names use `NNNN_snake_case.sql` and versions are never reused;
- an applied file is immutable because its SHA-256 checksum is stored in `schema_migrations`;
- each file and its metadata insert run in one transaction;
- a PostgreSQL advisory lock serializes concurrent migration processes;
- changing an applied migration requires a new forward migration.

`0003_operational_snapshot_indexes.sql` is an additive, transactional forward migration. Its
partial covering index keeps recent provider-attempt observation on `finished_at` and the included
result/status columns; in-flight attempt rows are deliberately absent from that index.

## Rollback and compatibility

Each migration and its `schema_migrations` insert are one PostgreSQL transaction. A failed file
therefore leaves neither partial objects nor an applied-version record, and the same database can be
started again after the faulty, still-unapplied file is corrected. An already applied migration is
never automatically reversed: recovery uses a new forward migration or a separately verified
backup restore.

The assignment Compose performs a gated stop/start rollout: API and workers start only after the
one-shot migrator succeeds, so mixed application/schema versions are outside this local topology.
A rolling production deployment would use additive expand/migrate/deploy/contract migrations and
retain old columns until every old process has drained.
