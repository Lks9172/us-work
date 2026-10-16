-- Operational snapshots inspect only recently completed provider attempts.  Keeping result/status
-- in the leaf tuple lets PostgreSQL satisfy the one-minute aggregate without visiting the full
-- attempt heap, while the partial predicate avoids indexing PREPARED/COMMITTED in-flight rows.
CREATE INDEX delivery_attempts_operational_finished_idx
  ON delivery_attempts (finished_at DESC)
  INCLUDE (result_code, http_status)
  WHERE request_phase = 'FINISHED';
