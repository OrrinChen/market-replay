package store

import "context"

func EnsurePostgresSchema(ctx context.Context, pool postgresPool) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS datasets (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			description TEXT NOT NULL DEFAULT '',
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE IF NOT EXISTS event_files (
			id TEXT PRIMARY KEY,
			dataset_id TEXT NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
			path TEXT NOT NULL,
			format TEXT NOT NULL,
			symbol TEXT NOT NULL DEFAULT '',
			bytes BIGINT NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			CHECK (bytes >= 0)
		)`,
		`CREATE TABLE IF NOT EXISTS replay_jobs (
			id TEXT PRIMARY KEY,
			dataset_id TEXT NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
			event_file_id TEXT NOT NULL REFERENCES event_files(id) ON DELETE CASCADE,
			idempotency_key TEXT UNIQUE,
			symbol TEXT NOT NULL DEFAULT '',
			speed TEXT NOT NULL,
			status TEXT NOT NULL,
			attempts INTEGER NOT NULL DEFAULT 0,
			last_error TEXT NOT NULL DEFAULT '',
			checkpoint_line BIGINT NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			started_at TIMESTAMPTZ,
			completed_at TIMESTAMPTZ,
			canceled_at TIMESTAMPTZ,
			CHECK (attempts >= 0),
			CHECK (checkpoint_line >= 0)
		)`,
		`CREATE TABLE IF NOT EXISTS replay_metrics (
			id TEXT PRIMARY KEY,
			job_id TEXT NOT NULL REFERENCES replay_jobs(id) ON DELETE CASCADE,
			rows BIGINT NOT NULL DEFAULT 0,
			events BIGINT NOT NULL DEFAULT 0,
			malformed_events BIGINT NOT NULL DEFAULT 0,
			sequence_gaps BIGINT NOT NULL DEFAULT 0,
			duration_ns BIGINT NOT NULL DEFAULT 0,
			rows_per_second DOUBLE PRECISION NOT NULL DEFAULT 0,
			events_per_second DOUBLE PRECISION NOT NULL DEFAULT 0,
			p95_latency_ns BIGINT NOT NULL DEFAULT 0,
			peak_alloc_bytes BIGINT NOT NULL DEFAULT 0,
			allocs_per_event DOUBLE PRECISION NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE IF NOT EXISTS validation_errors (
			id TEXT PRIMARY KEY,
			job_id TEXT NOT NULL REFERENCES replay_jobs(id) ON DELETE CASCADE,
			line BIGINT NOT NULL DEFAULT 0,
			symbol TEXT NOT NULL DEFAULT '',
			type TEXT NOT NULL,
			message TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_event_files_dataset_id ON event_files(dataset_id)`,
		`CREATE INDEX IF NOT EXISTS idx_replay_jobs_dataset_id ON replay_jobs(dataset_id)`,
		`CREATE INDEX IF NOT EXISTS idx_replay_jobs_id ON replay_jobs(id)`,
		`CREATE INDEX IF NOT EXISTS idx_replay_jobs_idempotency_key ON replay_jobs(idempotency_key) WHERE idempotency_key IS NOT NULL`,
		`CREATE INDEX IF NOT EXISTS idx_replay_metrics_job_id ON replay_metrics(job_id)`,
		`CREATE INDEX IF NOT EXISTS idx_validation_errors_job_id ON validation_errors(job_id)`,
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
