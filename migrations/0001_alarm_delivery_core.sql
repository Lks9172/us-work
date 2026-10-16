CREATE TABLE alarms (
  id uuid PRIMARY KEY,
  type text NOT NULL CHECK (type IN ('BULK', 'EMERGENCY')),
  title text NOT NULL CHECK (char_length(title) BETWEEN 1 AND 200),
  body text NOT NULL CHECK (char_length(body) BETWEEN 1 AND 5000),
  status text NOT NULL CHECK (
    status IN (
      'CREATED', 'QUEUED', 'EXPANDING', 'SENDING', 'CANCEL_REQUESTED', 'CANCELED',
      'COMPLETED', 'PARTIALLY_FAILED', 'FAILED', 'ATTENTION_REQUIRED'
    )
  ),
  failure_code text,
  cancel_requested_at timestamptz,
  audience_finalized_at timestamptz,
  expansion_completed_at timestamptz,
  finished_at timestamptz,
  materialized_count integer NOT NULL DEFAULT 0 CHECK (materialized_count >= 0),
  total integer CHECK (total >= 0),
  sent integer NOT NULL DEFAULT 0 CHECK (sent >= 0),
  failed integer NOT NULL DEFAULT 0 CHECK (failed >= 0),
  canceled integer NOT NULL DEFAULT 0 CHECK (canceled >= 0),
  active integer NOT NULL DEFAULT 0 CHECK (active >= 0),
  unresolved integer NOT NULL DEFAULT 0 CHECK (unresolved >= 0),
  duplicate_count integer NOT NULL DEFAULT 0 CHECK (duplicate_count >= 0),
  incident_count integer NOT NULL DEFAULT 0 CHECK (incident_count >= 0),
  incident_counts jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(incident_counts) = 'object'),
  incident_first_detected_at timestamptz,
  incident_last_detected_at timestamptz,
  version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT alarms_audience_counters_check CHECK (
    (
      audience_finalized_at IS NULL
      AND total IS NULL
      AND sent = 0 AND failed = 0 AND canceled = 0 AND active = 0 AND unresolved = 0
    )
    OR (
      audience_finalized_at IS NOT NULL
      AND total IS NOT NULL
      AND total = sent + failed + canceled + active + unresolved
    )
  ),
  CONSTRAINT alarms_incident_window_check CHECK (
    (
      incident_count = 0
      AND incident_counts = '{}'::jsonb
      AND incident_first_detected_at IS NULL
      AND incident_last_detected_at IS NULL
    )
    OR (
      incident_count > 0
      AND incident_first_detected_at IS NOT NULL
      AND incident_last_detected_at IS NOT NULL
      AND incident_first_detected_at <= incident_last_detected_at
    )
  ),
  CONSTRAINT alarms_duplicate_not_above_sent_check CHECK (duplicate_count <= sent)
);

CREATE INDEX alarms_list_idx ON alarms (created_at DESC, id DESC);
CREATE INDEX alarms_status_list_idx ON alarms (status, created_at DESC, id DESC);

CREATE TABLE emergency_recipients (
  alarm_id uuid NOT NULL REFERENCES alarms(id) ON DELETE RESTRICT,
  recipient_id text NOT NULL CHECK (
    char_length(recipient_id) BETWEEN 1 AND 128
    AND recipient_id = btrim(recipient_id)
  ),
  ordinal smallint NOT NULL CHECK (ordinal BETWEEN 1 AND 100),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (alarm_id, recipient_id),
  UNIQUE (alarm_id, ordinal)
);

CREATE TABLE alarm_expansion_jobs (
  alarm_id uuid PRIMARY KEY REFERENCES alarms(id) ON DELETE RESTRICT,
  state text NOT NULL CHECK (state IN ('READY', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELED')),
  run_generation uuid NOT NULL,
  next_cursor text,
  materialized_count integer NOT NULL DEFAULT 0 CHECK (materialized_count >= 0),
  lease_generation integer NOT NULL DEFAULT 0 CHECK (lease_generation >= 0),
  lease_token uuid,
  lease_until timestamptz,
  cleanup_state text NOT NULL DEFAULT 'NOT_REQUIRED' CHECK (
    cleanup_state IN ('NOT_REQUIRED', 'PENDING', 'RUNNING', 'COMPLETED')
  ),
  cleanup_run_generation uuid,
  cleanup_lease_generation integer NOT NULL DEFAULT 0 CHECK (cleanup_lease_generation >= 0),
  cleanup_lease_token uuid,
  cleanup_lease_until timestamptz,
  cleanup_completed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (alarm_id, run_generation),
  CONSTRAINT expansion_lease_shape_check CHECK (
    (lease_token IS NULL AND lease_until IS NULL)
    OR (lease_token IS NOT NULL AND lease_until IS NOT NULL)
  ),
  CONSTRAINT cleanup_lease_shape_check CHECK (
    (cleanup_lease_token IS NULL AND cleanup_lease_until IS NULL)
    OR (cleanup_lease_token IS NOT NULL AND cleanup_lease_until IS NOT NULL)
  )
);

CREATE INDEX expansion_jobs_claim_idx
  ON alarm_expansion_jobs (state, lease_until, alarm_id)
  WHERE state IN ('READY', 'RUNNING');
CREATE INDEX expansion_jobs_cleanup_claim_idx
  ON alarm_expansion_jobs (cleanup_state, cleanup_lease_until, alarm_id)
  WHERE cleanup_state IN ('PENDING', 'RUNNING');

CREATE TABLE alarm_expansion_recipients (
  alarm_id uuid NOT NULL,
  run_generation uuid NOT NULL,
  recipient_id text NOT NULL CHECK (char_length(recipient_id) BETWEEN 1 AND 128),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (alarm_id, run_generation, recipient_id),
  FOREIGN KEY (alarm_id, run_generation)
    REFERENCES alarm_expansion_jobs(alarm_id, run_generation)
    ON DELETE RESTRICT
);

CREATE TABLE alarm_expansion_seen_cursors (
  alarm_id uuid NOT NULL,
  run_generation uuid NOT NULL,
  cursor_hash text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (alarm_id, run_generation, cursor_hash),
  FOREIGN KEY (alarm_id, run_generation)
    REFERENCES alarm_expansion_jobs(alarm_id, run_generation)
    ON DELETE RESTRICT
);

CREATE TABLE deliveries (
  id uuid PRIMARY KEY,
  alarm_id uuid NOT NULL REFERENCES alarms(id) ON DELETE RESTRICT,
  recipient_id text NOT NULL CHECK (char_length(recipient_id) BETWEEN 1 AND 128),
  client_ref uuid NOT NULL UNIQUE,
  state text NOT NULL CHECK (
    state IN (
      'READY', 'CLAIMED', 'IN_FLIGHT', 'RETRY_WAIT', 'UNKNOWN', 'RECONCILING',
      'NEEDS_REVIEW', 'SENT', 'FAILED_PERMANENT', 'FAILED_RETRY_EXHAUSTED', 'CANCELED'
    )
  ),
  priority smallint NOT NULL,
  next_action_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  alarm_created_at timestamptz NOT NULL,
  attempt_no integer NOT NULL DEFAULT 0 CHECK (attempt_no >= 0),
  lease_generation integer NOT NULL DEFAULT 0 CHECK (lease_generation >= 0),
  lease_token uuid,
  lease_until timestamptz,
  current_attempt_id uuid,
  reconcile_generation integer NOT NULL DEFAULT 0 CHECK (reconcile_generation >= 0),
  reconcile_token uuid,
  reconcile_until timestamptz,
  accepted_at timestamptz,
  provider_sent_at timestamptz,
  blocking_conflict_code text,
  duplicate_detected boolean NOT NULL DEFAULT false,
  post_terminal_incident_code text CHECK (
    post_terminal_incident_code IS NULL
    OR post_terminal_incident_code IN (
      'LATE_PROVIDER_RECIPIENT_MISMATCH', 'LATE_PROVIDER_EVIDENCE_CONFLICT',
      'LATE_DISTINCT_DUPLICATE', 'LATE_POSITIVE_AFTER_FAILED', 'LATE_POSITIVE_AFTER_CANCELED'
    )
  ),
  post_terminal_incident_at timestamptz,
  corrected_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (alarm_id, recipient_id),
  UNIQUE (id, current_attempt_id),
  CONSTRAINT delivery_send_lease_shape_check CHECK (
    (lease_token IS NULL AND lease_until IS NULL)
    OR (lease_token IS NOT NULL AND lease_until IS NOT NULL)
  ),
  CONSTRAINT delivery_reconcile_lease_shape_check CHECK (
    (reconcile_token IS NULL AND reconcile_until IS NULL)
    OR (reconcile_token IS NOT NULL AND reconcile_until IS NOT NULL)
  ),
  CONSTRAINT delivery_incident_shape_check CHECK (
    (post_terminal_incident_code IS NULL AND post_terminal_incident_at IS NULL)
    OR (post_terminal_incident_code IS NOT NULL AND post_terminal_incident_at IS NOT NULL)
  )
);

CREATE INDEX deliveries_dispatch_claim_idx
  ON deliveries (priority DESC, next_action_at ASC, alarm_created_at ASC, id ASC)
  WHERE state IN ('READY', 'RETRY_WAIT');
CREATE INDEX deliveries_send_lease_idx
  ON deliveries (lease_until, id)
  WHERE state IN ('CLAIMED', 'IN_FLIGHT');
CREATE INDEX deliveries_reconcile_claim_idx
  ON deliveries (next_action_at, id)
  WHERE state IN ('UNKNOWN', 'NEEDS_REVIEW');
CREATE INDEX deliveries_reconcile_lease_idx
  ON deliveries (reconcile_until, id)
  WHERE state = 'RECONCILING';
CREATE INDEX deliveries_alarm_page_idx ON deliveries (alarm_id, id);

CREATE TABLE delivery_attempts (
  id uuid PRIMARY KEY,
  delivery_id uuid NOT NULL REFERENCES deliveries(id) ON DELETE RESTRICT,
  attempt_no integer NOT NULL CHECK (attempt_no > 0),
  state text NOT NULL CHECK (state IN ('STARTED', 'FINISHED')),
  lease_generation integer NOT NULL CHECK (lease_generation >= 0),
  limiter_reservation_id uuid NOT NULL UNIQUE,
  budget_consumed boolean NOT NULL DEFAULT true,
  request_phase text NOT NULL DEFAULT 'PREPARED' CHECK (
    request_phase IN ('PREPARED', 'COMMITTED', 'FINISHED_NOT_SENT', 'FINISHED')
  ),
  http_status integer,
  result_code text,
  started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  finished_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (delivery_id, attempt_no),
  UNIQUE (delivery_id, id),
  CONSTRAINT delivery_attempt_finished_shape_check CHECK (
    (state = 'STARTED' AND finished_at IS NULL)
    OR (state = 'FINISHED' AND finished_at IS NOT NULL)
  )
);

ALTER TABLE deliveries
  ADD CONSTRAINT deliveries_current_attempt_fk
  FOREIGN KEY (id, current_attempt_id)
  REFERENCES delivery_attempts(delivery_id, id)
  DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE delivery_evidence (
  id uuid PRIMARY KEY,
  delivery_id uuid NOT NULL REFERENCES deliveries(id) ON DELETE RESTRICT,
  attempt_id uuid,
  source text NOT NULL CHECK (source IN ('RESPONSE', 'HISTORY')),
  observation_key text NOT NULL,
  payload_hash text NOT NULL,
  entries jsonb NOT NULL CHECK (jsonb_typeof(entries) = 'array'),
  entry_count integer NOT NULL CHECK (entry_count >= 0),
  decision text,
  observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  consumed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (delivery_id, observation_key),
  FOREIGN KEY (delivery_id, attempt_id)
    REFERENCES delivery_attempts(delivery_id, id)
    DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX delivery_evidence_unconsumed_idx
  ON delivery_evidence (observed_at, id)
  WHERE consumed_at IS NULL;

CREATE TABLE dispatch_control (
  control_key text PRIMARY KEY,
  resume_not_before timestamptz NOT NULL DEFAULT '-infinity'::timestamptz,
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  version bigint NOT NULL DEFAULT 0 CHECK (version >= 0)
);

INSERT INTO dispatch_control(control_key) VALUES ('default-provider');
