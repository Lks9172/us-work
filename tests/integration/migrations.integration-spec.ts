import { randomUUID } from 'node:crypto';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

import { Client } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { runMigrations } from '../../packages/alarms/src/infrastructure/persistence/migrations';
import { createTestDatabase, type TestDatabase } from './postgres-test-schema';

const databaseUrl = process.env.TEST_DATABASE_URL;
const describeWithDatabase = databaseUrl === undefined ? describe.skip : describe;

const EXPECTED_SCHEMA_COLUMNS = {
  alarm_expansion_jobs: [
    'alarm_id',
    'state',
    'run_generation',
    'next_cursor',
    'materialized_count',
    'lease_generation',
    'lease_token',
    'lease_until',
    'cleanup_state',
    'cleanup_run_generation',
    'cleanup_lease_generation',
    'cleanup_lease_token',
    'cleanup_lease_until',
    'cleanup_completed_at',
    'created_at',
    'updated_at',
    'attempt_count',
    'next_action_at',
    'last_error',
  ],
  alarm_expansion_recipients: ['alarm_id', 'run_generation', 'recipient_id', 'created_at'],
  alarm_expansion_seen_cursors: ['alarm_id', 'run_generation', 'cursor_hash', 'created_at'],
  alarms: [
    'id',
    'type',
    'title',
    'body',
    'status',
    'failure_code',
    'cancel_requested_at',
    'audience_finalized_at',
    'expansion_completed_at',
    'finished_at',
    'materialized_count',
    'total',
    'sent',
    'failed',
    'canceled',
    'active',
    'unresolved',
    'duplicate_count',
    'incident_count',
    'incident_counts',
    'incident_first_detected_at',
    'incident_last_detected_at',
    'version',
    'created_at',
    'updated_at',
  ],
  deliveries: [
    'id',
    'alarm_id',
    'recipient_id',
    'client_ref',
    'state',
    'priority',
    'next_action_at',
    'alarm_created_at',
    'attempt_no',
    'lease_generation',
    'lease_token',
    'lease_until',
    'current_attempt_id',
    'reconcile_generation',
    'reconcile_token',
    'reconcile_until',
    'accepted_at',
    'provider_sent_at',
    'blocking_conflict_code',
    'duplicate_detected',
    'post_terminal_incident_code',
    'post_terminal_incident_at',
    'corrected_at',
    'created_at',
    'updated_at',
    'post_budget_count',
    'rate_limit_count',
    'first_rate_limited_at',
    'reconcile_count',
    'first_unknown_at',
    'next_review_at',
    'review_until',
    'result_code',
    'provider_message_id',
  ],
  delivery_attempts: [
    'id',
    'delivery_id',
    'attempt_no',
    'state',
    'lease_generation',
    'limiter_reservation_id',
    'budget_consumed',
    'request_phase',
    'http_status',
    'result_code',
    'started_at',
    'finished_at',
    'created_at',
    'adapter_committed_at',
    'response_received_at',
    'provider_message_id',
    'transport_class',
  ],
  delivery_evidence: [
    'id',
    'delivery_id',
    'attempt_id',
    'source',
    'observation_key',
    'payload_hash',
    'entries',
    'entry_count',
    'decision',
    'observed_at',
    'consumed_at',
    'created_at',
  ],
  dispatch_control: ['control_key', 'resume_not_before', 'updated_at', 'version'],
  emergency_recipients: ['alarm_id', 'recipient_id', 'ordinal', 'created_at'],
  schema_migrations: ['version', 'checksum', 'applied_at'],
} as const;

const EXPECTED_CONSTRAINT_KEYS = [
  'alarm_expansion_jobs|c|alarm_expansion_jobs_attempt_count_check',
  'alarm_expansion_jobs|c|alarm_expansion_jobs_cleanup_lease_generation_check',
  'alarm_expansion_jobs|c|alarm_expansion_jobs_cleanup_state_check',
  'alarm_expansion_jobs|c|alarm_expansion_jobs_lease_generation_check',
  'alarm_expansion_jobs|c|alarm_expansion_jobs_materialized_count_check',
  'alarm_expansion_jobs|c|alarm_expansion_jobs_state_check',
  'alarm_expansion_jobs|c|cleanup_lease_shape_check',
  'alarm_expansion_jobs|c|expansion_lease_shape_check',
  'alarm_expansion_jobs|f|alarm_expansion_jobs_alarm_id_fkey',
  'alarm_expansion_jobs|p|alarm_expansion_jobs_pkey',
  'alarm_expansion_jobs|u|alarm_expansion_jobs_alarm_id_run_generation_key',
  'alarm_expansion_recipients|c|alarm_expansion_recipients_recipient_id_check',
  'alarm_expansion_recipients|f|alarm_expansion_recipients_alarm_id_run_generation_fkey',
  'alarm_expansion_recipients|p|alarm_expansion_recipients_pkey',
  'alarm_expansion_seen_cursors|f|alarm_expansion_seen_cursors_alarm_id_run_generation_fkey',
  'alarm_expansion_seen_cursors|p|alarm_expansion_seen_cursors_pkey',
  'alarms|c|alarms_active_check',
  'alarms|c|alarms_audience_counters_check',
  'alarms|c|alarms_body_check',
  'alarms|c|alarms_canceled_check',
  'alarms|c|alarms_duplicate_count_check',
  'alarms|c|alarms_duplicate_not_above_sent_check',
  'alarms|c|alarms_failed_check',
  'alarms|c|alarms_incident_count_check',
  'alarms|c|alarms_incident_counts_check',
  'alarms|c|alarms_incident_window_check',
  'alarms|c|alarms_materialized_count_check',
  'alarms|c|alarms_sent_check',
  'alarms|c|alarms_status_check',
  'alarms|c|alarms_title_check',
  'alarms|c|alarms_total_check',
  'alarms|c|alarms_type_check',
  'alarms|c|alarms_unresolved_check',
  'alarms|c|alarms_version_check',
  'alarms|p|alarms_pkey',
  'deliveries|c|deliveries_attempt_no_check',
  'deliveries|c|deliveries_lease_generation_check',
  'deliveries|c|deliveries_post_budget_count_check',
  'deliveries|c|deliveries_post_terminal_incident_code_check',
  'deliveries|c|deliveries_rate_limit_count_check',
  'deliveries|c|deliveries_recipient_id_check',
  'deliveries|c|deliveries_reconcile_count_check',
  'deliveries|c|deliveries_reconcile_generation_check',
  'deliveries|c|deliveries_state_check',
  'deliveries|c|delivery_incident_shape_check',
  'deliveries|c|delivery_reconcile_lease_shape_check',
  'deliveries|c|delivery_send_lease_shape_check',
  'deliveries|f|deliveries_alarm_id_fkey',
  'deliveries|f|deliveries_current_attempt_fk',
  'deliveries|p|deliveries_pkey',
  'deliveries|u|deliveries_alarm_id_recipient_id_key',
  'deliveries|u|deliveries_client_ref_key',
  'deliveries|u|deliveries_id_current_attempt_id_key',
  'delivery_attempts|c|delivery_attempt_finished_shape_check',
  'delivery_attempts|c|delivery_attempts_attempt_no_check',
  'delivery_attempts|c|delivery_attempts_lease_generation_check',
  'delivery_attempts|c|delivery_attempts_request_phase_check',
  'delivery_attempts|c|delivery_attempts_state_check',
  'delivery_attempts|f|delivery_attempts_delivery_id_fkey',
  'delivery_attempts|p|delivery_attempts_pkey',
  'delivery_attempts|u|delivery_attempts_delivery_id_attempt_no_key',
  'delivery_attempts|u|delivery_attempts_delivery_id_id_key',
  'delivery_attempts|u|delivery_attempts_limiter_reservation_id_key',
  'delivery_evidence|c|delivery_evidence_entries_check',
  'delivery_evidence|c|delivery_evidence_entry_count_check',
  'delivery_evidence|c|delivery_evidence_source_check',
  'delivery_evidence|f|delivery_evidence_delivery_id_attempt_id_fkey',
  'delivery_evidence|f|delivery_evidence_delivery_id_fkey',
  'delivery_evidence|p|delivery_evidence_pkey',
  'delivery_evidence|u|delivery_evidence_delivery_id_observation_key_key',
  'dispatch_control|c|dispatch_control_version_check',
  'dispatch_control|p|dispatch_control_pkey',
  'emergency_recipients|c|emergency_recipients_ordinal_check',
  'emergency_recipients|c|emergency_recipients_recipient_id_check',
  'emergency_recipients|f|emergency_recipients_alarm_id_fkey',
  'emergency_recipients|p|emergency_recipients_pkey',
  'emergency_recipients|u|emergency_recipients_alarm_id_ordinal_key',
  'schema_migrations|p|schema_migrations_pkey',
] as const;

const EXPECTED_INDEX_NAMES = [
  'alarm_expansion_jobs_alarm_id_run_generation_key',
  'alarm_expansion_jobs_pkey',
  'alarm_expansion_recipients_pkey',
  'alarm_expansion_seen_cursors_pkey',
  'alarms_list_idx',
  'alarms_pkey',
  'alarms_status_list_idx',
  'deliveries_alarm_id_recipient_id_key',
  'deliveries_alarm_page_idx',
  'deliveries_client_ref_key',
  'deliveries_dispatch_claim_idx',
  'deliveries_id_current_attempt_id_key',
  'deliveries_pkey',
  'deliveries_reconcile_claim_idx',
  'deliveries_reconcile_lease_idx',
  'deliveries_review_claim_idx',
  'deliveries_send_lease_idx',
  'delivery_attempts_delivery_id_attempt_no_key',
  'delivery_attempts_delivery_id_id_key',
  'delivery_attempts_limiter_reservation_id_key',
  'delivery_attempts_operational_finished_idx',
  'delivery_attempts_pkey',
  'delivery_evidence_delivery_id_observation_key_key',
  'delivery_evidence_pkey',
  'delivery_evidence_unconsumed_idx',
  'dispatch_control_pkey',
  'emergency_recipients_alarm_id_ordinal_key',
  'emergency_recipients_pkey',
  'expansion_jobs_claim_idx',
  'expansion_jobs_cleanup_claim_idx',
  'schema_migrations_pkey',
] as const;

describeWithDatabase('PostgreSQL migrations', () => {
  let testDatabase: TestDatabase;

  beforeAll(async () => {
    if (databaseUrl === undefined) {
      throw new Error('TEST_DATABASE_URL is required');
    }
    testDatabase = await createTestDatabase(databaseUrl);
  });

  afterAll(async () => {
    await testDatabase?.cleanup();
  });

  it('applies once, is rerunnable, and installs concurrency constraints', async () => {
    const migrationsDirectory = resolve(process.cwd(), 'migrations');
    const firstRun = await runMigrations({
      databaseUrl: testDatabase.databaseUrl,
      migrationsDirectory,
    });
    const secondRun = await runMigrations({
      databaseUrl: testDatabase.databaseUrl,
      migrationsDirectory,
    });
    const client = new Client({ connectionString: testDatabase.databaseUrl });
    await client.connect();

    try {
      const tables = await client.query<{ table_name: string }>(`
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = current_schema()
          AND table_name IN ('alarms', 'deliveries', 'delivery_attempts', 'delivery_evidence')
        ORDER BY table_name
      `);
      const constraints = await client.query<{ constraint_name: string }>(`
        SELECT constraint_name
        FROM information_schema.table_constraints
        WHERE table_schema = current_schema()
          AND constraint_name IN (
            'deliveries_alarm_id_recipient_id_key',
            'deliveries_client_ref_key',
            'deliveries_current_attempt_fk'
          )
        ORDER BY constraint_name
      `);

      expect(firstRun).toEqual([
        '0001_alarm_delivery_core.sql',
        '0002_delivery_runtime_guards.sql',
        '0003_operational_snapshot_indexes.sql',
      ]);
      expect(secondRun).toEqual([]);
      expect(tables.rows.map(({ table_name }) => table_name)).toEqual([
        'alarms',
        'deliveries',
        'delivery_attempts',
        'delivery_evidence',
      ]);
      expect(constraints.rowCount).toBe(3);
    } finally {
      await client.end();
    }
  });

  it('installs the complete documented column, constraint, and index contract', async () => {
    const migrationsDirectory = resolve(process.cwd(), 'migrations');
    await runMigrations({
      databaseUrl: testDatabase.databaseUrl,
      migrationsDirectory,
    });
    const client = new Client({ connectionString: testDatabase.databaseUrl });
    await client.connect();

    try {
      const columns = await client.query<{ table_name: string; column_names: string[] }>(`
        SELECT
          table_name,
          array_agg(column_name::text ORDER BY ordinal_position) AS column_names
        FROM information_schema.columns
        WHERE table_schema = current_schema()
        GROUP BY table_name
        ORDER BY table_name
      `);
      const constraints = await client.query<{
        table_name: string;
        constraint_name: string;
        constraint_type: string;
        definition: string;
      }>(`
        SELECT
          c.conrelid::regclass::text AS table_name,
          c.conname AS constraint_name,
          c.contype::text AS constraint_type,
          pg_get_constraintdef(c.oid) AS definition
        FROM pg_constraint c
        JOIN pg_namespace n ON n.oid = c.connamespace
        WHERE n.nspname = current_schema()
          AND c.contype IN ('c', 'f', 'p', 'u')
        ORDER BY table_name, constraint_type, constraint_name
      `);
      const indexes = await client.query<{
        index_name: string;
        index_definition: string;
      }>(`
        SELECT indexname AS index_name, indexdef AS index_definition
        FROM pg_indexes
        WHERE schemaname = current_schema()
        ORDER BY indexname
      `);

      expect(
        Object.fromEntries(columns.rows.map((row) => [row.table_name, row.column_names])),
      ).toEqual(EXPECTED_SCHEMA_COLUMNS);
      expect(
        constraints.rows.map(
          (row) => `${row.table_name}|${row.constraint_type}|${row.constraint_name}`,
        ),
      ).toEqual(EXPECTED_CONSTRAINT_KEYS);
      expect(indexes.rows.map((row) => row.index_name)).toEqual(EXPECTED_INDEX_NAMES);

      const definitions = Object.fromEntries(
        constraints.rows.map((row) => [row.constraint_name, row.definition]),
      );
      expect(definitions).toMatchObject({
        alarm_expansion_jobs_alarm_id_run_generation_key: 'UNIQUE (alarm_id, run_generation)',
        alarm_expansion_recipients_alarm_id_run_generation_fkey:
          'FOREIGN KEY (alarm_id, run_generation) REFERENCES alarm_expansion_jobs(alarm_id, run_generation) ON DELETE RESTRICT',
        alarm_expansion_seen_cursors_alarm_id_run_generation_fkey:
          'FOREIGN KEY (alarm_id, run_generation) REFERENCES alarm_expansion_jobs(alarm_id, run_generation) ON DELETE RESTRICT',
        deliveries_alarm_id_recipient_id_key: 'UNIQUE (alarm_id, recipient_id)',
        deliveries_client_ref_key: 'UNIQUE (client_ref)',
        deliveries_current_attempt_fk:
          'FOREIGN KEY (id, current_attempt_id) REFERENCES delivery_attempts(delivery_id, id) DEFERRABLE INITIALLY DEFERRED',
        delivery_attempts_delivery_id_attempt_no_key: 'UNIQUE (delivery_id, attempt_no)',
        delivery_attempts_delivery_id_id_key: 'UNIQUE (delivery_id, id)',
        delivery_evidence_delivery_id_attempt_id_fkey:
          'FOREIGN KEY (delivery_id, attempt_id) REFERENCES delivery_attempts(delivery_id, id) DEFERRABLE INITIALLY DEFERRED',
        emergency_recipients_alarm_id_ordinal_key: 'UNIQUE (alarm_id, ordinal)',
        emergency_recipients_pkey: 'PRIMARY KEY (alarm_id, recipient_id)',
      });
      expect(definitions.alarms_audience_counters_check).toContain(
        'total = ((((sent + failed) + canceled) + active) + unresolved)',
      );
      expect(definitions.alarms_incident_window_check).toContain("incident_counts = '{}'::jsonb");
      expect(definitions.alarms_incident_window_check).toContain(
        'incident_first_detected_at <= incident_last_detected_at',
      );
      expect(definitions.alarms_duplicate_not_above_sent_check).toBe(
        'CHECK ((duplicate_count <= sent))',
      );
      expect(definitions.expansion_lease_shape_check).toContain(
        '(lease_token IS NULL) AND (lease_until IS NULL)',
      );
      expect(definitions.cleanup_lease_shape_check).toContain(
        '(cleanup_lease_token IS NOT NULL) AND (cleanup_lease_until IS NOT NULL)',
      );
      expect(definitions.delivery_send_lease_shape_check).toContain(
        '(lease_token IS NOT NULL) AND (lease_until IS NOT NULL)',
      );
      expect(definitions.delivery_reconcile_lease_shape_check).toContain(
        '(reconcile_token IS NOT NULL) AND (reconcile_until IS NOT NULL)',
      );
      expect(definitions.delivery_incident_shape_check).toContain(
        '(post_terminal_incident_code IS NOT NULL) AND (post_terminal_incident_at IS NOT NULL)',
      );
      expect(definitions.delivery_attempt_finished_shape_check).toContain(
        "ARRAY['FINISHED_NOT_SENT'::text, 'FINISHED'::text]",
      );

      const indexDefinitions = Object.fromEntries(
        indexes.rows.map((row) => [row.index_name, row.index_definition]),
      );
      expect(indexDefinitions.deliveries_dispatch_claim_idx).toContain(
        '(priority DESC, next_action_at, alarm_created_at, id)',
      );
      expect(indexDefinitions.deliveries_dispatch_claim_idx).toContain(
        "WHERE (state = ANY (ARRAY['READY'::text, 'RETRY_WAIT'::text]))",
      );
      expect(indexDefinitions.expansion_jobs_claim_idx).toContain('(state, lease_until, alarm_id)');
      expect(indexDefinitions.expansion_jobs_cleanup_claim_idx).toContain(
        '(cleanup_state, cleanup_lease_until, alarm_id)',
      );
      expect(indexDefinitions.delivery_evidence_unconsumed_idx).toContain(
        'WHERE (consumed_at IS NULL)',
      );
      expect(indexDefinitions.delivery_attempts_operational_finished_idx).toContain(
        '(finished_at DESC) INCLUDE (result_code, http_status)',
      );
      expect(indexDefinitions.delivery_attempts_operational_finished_idx).toContain(
        "WHERE (request_phase = 'FINISHED'::text)",
      );
    } finally {
      await client.end();
    }
  });

  it('serializes two migration owners on a fresh database', async () => {
    if (databaseUrl === undefined) {
      throw new Error('TEST_DATABASE_URL is required');
    }
    const concurrentDatabase = await createTestDatabase(databaseUrl);
    const migrationsDirectory = resolve(process.cwd(), 'migrations');

    try {
      const results = await Promise.all([
        runMigrations({
          databaseUrl: concurrentDatabase.databaseUrl,
          migrationsDirectory,
        }),
        runMigrations({
          databaseUrl: concurrentDatabase.databaseUrl,
          migrationsDirectory,
        }),
      ]);

      expect(results.map((result) => result.length).sort((left, right) => left - right)).toEqual([
        0, 3,
      ]);
      expect(results.flat().sort()).toEqual([
        '0001_alarm_delivery_core.sql',
        '0002_delivery_runtime_guards.sql',
        '0003_operational_snapshot_indexes.sql',
      ]);

      const client = new Client({ connectionString: concurrentDatabase.databaseUrl });
      await client.connect();
      try {
        const applied = await client.query<{ version: string; copies: number }>(
          `
            SELECT version, count(*)::integer AS copies
            FROM schema_migrations
            GROUP BY version
            ORDER BY version
          `,
        );
        expect(applied.rows).toEqual([
          { version: '0001_alarm_delivery_core.sql', copies: 1 },
          { version: '0002_delivery_runtime_guards.sql', copies: 1 },
          { version: '0003_operational_snapshot_indexes.sql', copies: 1 },
        ]);
      } finally {
        await client.end();
      }
    } finally {
      await concurrentDatabase.cleanup();
    }
  });

  it('upgrades the previous schema without data loss and is rerunnable afterwards', async () => {
    if (databaseUrl === undefined) {
      throw new Error('TEST_DATABASE_URL is required');
    }
    const upgradeDatabase = await createTestDatabase(databaseUrl);
    const legacyMigrationsDirectory = await mkdtemp(join(tmpdir(), 'usall-legacy-migration-'));
    const migrationsDirectory = resolve(process.cwd(), 'migrations');
    const alarmId = randomUUID();
    const deliveryId = randomUUID();
    const clientRef = randomUUID();

    try {
      await writeFile(
        join(legacyMigrationsDirectory, '0001_alarm_delivery_core.sql'),
        await readFile(join(migrationsDirectory, '0001_alarm_delivery_core.sql'), 'utf8'),
        'utf8',
      );
      expect(
        await runMigrations({
          databaseUrl: upgradeDatabase.databaseUrl,
          migrationsDirectory: legacyMigrationsDirectory,
        }),
      ).toEqual(['0001_alarm_delivery_core.sql']);

      const client = new Client({ connectionString: upgradeDatabase.databaseUrl });
      await client.connect();
      try {
        await client.query(
          `
            INSERT INTO alarms(
              id, type, title, body, status, audience_finalized_at,
              materialized_count, total, active
            ) VALUES ($1, 'BULK', 'legacy title', 'legacy body', 'SENDING', clock_timestamp(), 1, 1, 1)
          `,
          [alarmId],
        );
        await client.query(
          `
            INSERT INTO deliveries(
              id, alarm_id, recipient_id, client_ref, state, priority, alarm_created_at
            ) VALUES ($1, $2, 'legacy-recipient', $3, 'READY', 10, clock_timestamp())
          `,
          [deliveryId, alarmId, clientRef],
        );
      } finally {
        await client.end();
      }

      expect(
        await runMigrations({
          databaseUrl: upgradeDatabase.databaseUrl,
          migrationsDirectory,
        }),
      ).toEqual(['0002_delivery_runtime_guards.sql', '0003_operational_snapshot_indexes.sql']);
      expect(
        await runMigrations({
          databaseUrl: upgradeDatabase.databaseUrl,
          migrationsDirectory,
        }),
      ).toEqual([]);

      const verifyClient = new Client({ connectionString: upgradeDatabase.databaseUrl });
      await verifyClient.connect();
      try {
        const preserved = await verifyClient.query<{
          alarm_id: string;
          title: string;
          delivery_id: string;
          recipient_id: string;
          post_budget_count: number;
          rate_limit_count: number;
          reconcile_count: number;
        }>(
          `
            SELECT
              a.id::text AS alarm_id,
              a.title,
              d.id::text AS delivery_id,
              d.recipient_id,
              d.post_budget_count,
              d.rate_limit_count,
              d.reconcile_count
            FROM alarms a
            JOIN deliveries d ON d.alarm_id = a.id
            WHERE a.id = $1
          `,
          [alarmId],
        );
        const applied = await verifyClient.query<{ version: string }>(
          'SELECT version FROM schema_migrations ORDER BY version',
        );

        expect(preserved.rows).toEqual([
          {
            alarm_id: alarmId,
            title: 'legacy title',
            delivery_id: deliveryId,
            recipient_id: 'legacy-recipient',
            post_budget_count: 0,
            rate_limit_count: 0,
            reconcile_count: 0,
          },
        ]);
        expect(applied.rows).toEqual([
          { version: '0001_alarm_delivery_core.sql' },
          { version: '0002_delivery_runtime_guards.sql' },
          { version: '0003_operational_snapshot_indexes.sql' },
        ]);
      } finally {
        await verifyClient.end();
      }
    } finally {
      await upgradeDatabase.cleanup();
      await rm(legacyMigrationsDirectory, { recursive: true, force: true });
    }
  });

  it('rolls back a broken migration, then recovers cleanly with the valid set', async () => {
    if (databaseUrl === undefined) {
      throw new Error('TEST_DATABASE_URL is required');
    }
    const brokenDatabase = await createTestDatabase(databaseUrl);
    const brokenMigrationsDirectory = await mkdtemp(join(tmpdir(), 'usall-broken-migration-'));

    try {
      await writeFile(
        join(brokenMigrationsDirectory, '0001_broken.sql'),
        `
          CREATE TABLE must_rollback(id integer PRIMARY KEY);
          INSERT INTO missing_table(id) VALUES (1);
        `,
        'utf8',
      );

      await expect(
        runMigrations({
          databaseUrl: brokenDatabase.databaseUrl,
          migrationsDirectory: brokenMigrationsDirectory,
        }),
      ).rejects.toThrow();

      const client = new Client({ connectionString: brokenDatabase.databaseUrl });
      await client.connect();
      try {
        const state = await client.query<{
          leaked_table: string | null;
          applied_count: number;
        }>(`
          SELECT
            to_regclass('must_rollback')::text AS leaked_table,
            (SELECT count(*)::integer FROM schema_migrations) AS applied_count
        `);
        expect(state.rows[0]).toEqual({ leaked_table: null, applied_count: 0 });
      } finally {
        await client.end();
      }

      const migrationsDirectory = resolve(process.cwd(), 'migrations');
      expect(
        await runMigrations({
          databaseUrl: brokenDatabase.databaseUrl,
          migrationsDirectory,
        }),
      ).toEqual([
        '0001_alarm_delivery_core.sql',
        '0002_delivery_runtime_guards.sql',
        '0003_operational_snapshot_indexes.sql',
      ]);
      expect(
        await runMigrations({
          databaseUrl: brokenDatabase.databaseUrl,
          migrationsDirectory,
        }),
      ).toEqual([]);

      const recoveryClient = new Client({ connectionString: brokenDatabase.databaseUrl });
      await recoveryClient.connect();
      try {
        const recovered = await recoveryClient.query<{
          leaked_table: string | null;
          alarms_table: string | null;
          deliveries_table: string | null;
          applied_count: number;
        }>(`
          SELECT
            to_regclass('must_rollback')::text AS leaked_table,
            to_regclass('alarms')::text AS alarms_table,
            to_regclass('deliveries')::text AS deliveries_table,
            (SELECT count(*)::integer FROM schema_migrations) AS applied_count
        `);
        expect(recovered.rows[0]).toEqual({
          leaked_table: null,
          alarms_table: 'alarms',
          deliveries_table: 'deliveries',
          applied_count: 3,
        });
      } finally {
        await recoveryClient.end();
      }
    } finally {
      await brokenDatabase.cleanup();
      await rm(brokenMigrationsDirectory, { recursive: true, force: true });
    }
  });
});
