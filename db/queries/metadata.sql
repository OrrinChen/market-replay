-- name: CreateDataset :one
INSERT INTO datasets (id, name, description)
VALUES ($1, $2, $3)
RETURNING id, name, description, created_at;

-- name: ListDatasets :many
SELECT id, name, description, created_at
FROM datasets
ORDER BY created_at ASC;

-- name: GetDataset :one
SELECT id, name, description, created_at
FROM datasets
WHERE id = $1;

-- name: CreateEventFile :one
INSERT INTO event_files (id, dataset_id, path, format, symbol, bytes)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING id, dataset_id, path, format, symbol, bytes, created_at;

-- name: GetEventFile :one
SELECT id, dataset_id, path, format, symbol, bytes, created_at
FROM event_files
WHERE id = $1;

-- name: CreateReplayJob :one
INSERT INTO replay_jobs (id, dataset_id, event_file_id, idempotency_key, symbol, speed, status)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING id, dataset_id, event_file_id, COALESCE(idempotency_key, '') AS idempotency_key,
    symbol, speed, status, attempts, last_error, checkpoint_line, created_at,
    started_at, completed_at, canceled_at;

-- name: GetReplayJob :one
SELECT id, dataset_id, event_file_id, COALESCE(idempotency_key, '') AS idempotency_key,
    symbol, speed, status, attempts, last_error, checkpoint_line, created_at,
    started_at, completed_at, canceled_at
FROM replay_jobs
WHERE id = $1;

-- name: GetReplayJobByIdempotencyKey :one
SELECT id, dataset_id, event_file_id, COALESCE(idempotency_key, '') AS idempotency_key,
    symbol, speed, status, attempts, last_error, checkpoint_line, created_at,
    started_at, completed_at, canceled_at
FROM replay_jobs
WHERE idempotency_key = $1;

-- name: ListReplayJobs :many
SELECT id, dataset_id, event_file_id, COALESCE(idempotency_key, '') AS idempotency_key,
    symbol, speed, status, attempts, last_error, checkpoint_line, created_at,
    started_at, completed_at, canceled_at
FROM replay_jobs
ORDER BY created_at ASC;

-- name: UpdateReplayJobStatus :one
UPDATE replay_jobs
SET status = $2,
    last_error = $3,
    attempts = attempts + 1,
    started_at = CASE WHEN $2 = 'running' THEN $4 ELSE started_at END,
    completed_at = CASE WHEN $2 IN ('completed', 'failed', 'dlq') THEN $4 ELSE completed_at END,
    canceled_at = CASE WHEN $2 = 'canceled' THEN $4 ELSE canceled_at END
WHERE id = $1
RETURNING id, dataset_id, event_file_id, COALESCE(idempotency_key, '') AS idempotency_key,
    symbol, speed, status, attempts, last_error, checkpoint_line, created_at,
    started_at, completed_at, canceled_at;

-- name: UpdateReplayCheckpoint :execrows
UPDATE replay_jobs
SET checkpoint_line = GREATEST(checkpoint_line, $2)
WHERE id = $1;

-- name: CompleteReplayJobStatus :execrows
UPDATE replay_jobs
SET status = 'completed', completed_at = $2
WHERE id = $1;

-- name: CreateReplayMetric :one
INSERT INTO replay_metrics (
    id, job_id, rows, events, malformed_events, sequence_gaps, duration_ns,
    rows_per_second, events_per_second, p95_latency_ns, peak_alloc_bytes,
    allocs_per_event, created_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
RETURNING id, job_id, rows, events, malformed_events, sequence_gaps, duration_ns,
    rows_per_second, events_per_second, p95_latency_ns, peak_alloc_bytes,
    allocs_per_event, created_at;

-- name: ListReplayMetrics :many
SELECT id, job_id, rows, events, malformed_events, sequence_gaps, duration_ns,
    rows_per_second, events_per_second, p95_latency_ns, peak_alloc_bytes,
    allocs_per_event, created_at
FROM replay_metrics
WHERE job_id = $1
ORDER BY created_at ASC;

-- name: CreateValidationError :one
INSERT INTO validation_errors (id, job_id, line, symbol, type, message, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING id, job_id, line, symbol, type, message, created_at;

-- name: ListValidationErrors :many
SELECT id, job_id, line, symbol, type, message, created_at
FROM validation_errors
WHERE job_id = $1
ORDER BY line ASC, created_at ASC;

-- name: CancelReplayJob :one
UPDATE replay_jobs
SET status = 'canceled', canceled_at = $2
WHERE id = $1
RETURNING id, dataset_id, event_file_id, COALESCE(idempotency_key, '') AS idempotency_key,
    symbol, speed, status, attempts, last_error, checkpoint_line, created_at,
    started_at, completed_at, canceled_at;
