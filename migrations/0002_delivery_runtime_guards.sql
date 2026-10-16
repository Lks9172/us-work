ALTER TABLE alarm_expansion_jobs
  ADD COLUMN attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  ADD COLUMN next_action_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  ADD COLUMN last_error text;

ALTER TABLE deliveries
  ADD COLUMN post_budget_count integer NOT NULL DEFAULT 0 CHECK (post_budget_count >= 0),
  ADD COLUMN rate_limit_count integer NOT NULL DEFAULT 0 CHECK (rate_limit_count >= 0),
  ADD COLUMN first_rate_limited_at timestamptz,
  ADD COLUMN reconcile_count integer NOT NULL DEFAULT 0 CHECK (reconcile_count >= 0),
  ADD COLUMN first_unknown_at timestamptz,
  ADD COLUMN next_review_at timestamptz,
  ADD COLUMN review_until timestamptz,
  ADD COLUMN result_code text,
  ADD COLUMN provider_message_id text;

CREATE INDEX deliveries_review_claim_idx
  ON deliveries (next_review_at, id)
  WHERE state = 'NEEDS_REVIEW';

ALTER TABLE delivery_attempts
  DROP CONSTRAINT delivery_attempts_state_check,
  DROP CONSTRAINT delivery_attempt_finished_shape_check,
  ADD COLUMN adapter_committed_at timestamptz,
  ADD COLUMN response_received_at timestamptz,
  ADD COLUMN provider_message_id text,
  ADD COLUMN transport_class text,
  ADD CONSTRAINT delivery_attempts_state_check CHECK (
    state IN ('STARTED', 'FINISHED_NOT_SENT', 'FINISHED')
  ),
  ADD CONSTRAINT delivery_attempt_finished_shape_check CHECK (
    (state = 'STARTED' AND finished_at IS NULL)
    OR (state IN ('FINISHED_NOT_SENT', 'FINISHED') AND finished_at IS NOT NULL)
  );
