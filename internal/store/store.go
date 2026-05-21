package store

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
)

type Dataset struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

type EventFile struct {
	ID        string    `json:"id"`
	DatasetID string    `json:"dataset_id"`
	Path      string    `json:"path"`
	Format    string    `json:"format"`
	Symbol    string    `json:"symbol,omitempty"`
	Bytes     int64     `json:"bytes"`
	SHA256    string    `json:"sha256,omitempty"`
	Rows      int64     `json:"rows"`
	CreatedAt time.Time `json:"created_at"`
}

type JobStatus string

const (
	JobStatusQueued    JobStatus = "queued"
	JobStatusRunning   JobStatus = "running"
	JobStatusCompleted JobStatus = "completed"
	JobStatusFailed    JobStatus = "failed"
	JobStatusCanceled  JobStatus = "canceled"
	JobStatusDLQ       JobStatus = "dlq"
)

type ReplayJob struct {
	ID             string         `json:"id"`
	DatasetID      string         `json:"dataset_id"`
	EventFileID    string         `json:"event_file_id"`
	IdempotencyKey string         `json:"idempotency_key"`
	Symbol         string         `json:"symbol,omitempty"`
	Speed          string         `json:"speed"`
	Status         JobStatus      `json:"status"`
	Attempts       int            `json:"attempts"`
	LastError      string         `json:"last_error,omitempty"`
	CheckpointLine int64          `json:"checkpoint_line"`
	CreatedAt      time.Time      `json:"created_at"`
	StartedAt      *time.Time     `json:"started_at,omitempty"`
	CompletedAt    *time.Time     `json:"completed_at,omitempty"`
	CanceledAt     *time.Time     `json:"canceled_at,omitempty"`
	Manifest       ReplayManifest `json:"manifest"`
}

type ValidationError struct {
	ID        string    `json:"id"`
	JobID     string    `json:"job_id"`
	Line      int64     `json:"line"`
	Symbol    string    `json:"symbol,omitempty"`
	Type      string    `json:"type"`
	Message   string    `json:"message"`
	CreatedAt time.Time `json:"created_at"`
}

type ReplayMetric struct {
	ID              string        `json:"id"`
	JobID           string        `json:"job_id"`
	Rows            int64         `json:"rows"`
	Events          int64         `json:"events"`
	MalformedEvents int64         `json:"malformed_events"`
	SequenceGaps    int64         `json:"sequence_gaps"`
	Duration        time.Duration `json:"duration"`
	RowsPerSecond   float64       `json:"rows_per_second"`
	EventsPerSecond float64       `json:"events_per_second"`
	P95Latency      time.Duration `json:"p95_latency"`
	PeakAllocBytes  uint64        `json:"peak_alloc_bytes"`
	AllocsPerEvent  float64       `json:"allocs_per_event"`
	CreatedAt       time.Time     `json:"created_at"`
}

type ReplayManifest struct {
	InputPath       string        `json:"input_path,omitempty"`
	InputFormat     string        `json:"input_format,omitempty"`
	InputSymbol     string        `json:"input_symbol,omitempty"`
	InputFileSHA256 string        `json:"input_file_sha256,omitempty"`
	InputBytes      int64         `json:"input_bytes"`
	InputRows       int64         `json:"input_rows"`
	AppVersion      string        `json:"app_version,omitempty"`
	StartedAt       time.Time     `json:"started_at,omitempty"`
	CompletedAt     time.Time     `json:"completed_at,omitempty"`
	Duration        time.Duration `json:"duration_ns"`
	CheckpointLine  int64         `json:"checkpoint_line"`
	ResumeFromLine  int64         `json:"resume_from_line"`
	ErrorCount      int64         `json:"error_count"`
	MalformedEvents int64         `json:"malformed_events"`
	SequenceGaps    int64         `json:"sequence_gaps"`
}

type CreateDatasetParams struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

type CreateEventFileParams struct {
	DatasetID string `json:"dataset_id"`
	Path      string `json:"path"`
	Format    string `json:"format"`
	Symbol    string `json:"symbol"`
	Bytes     int64  `json:"bytes"`
	SHA256    string `json:"sha256"`
	Rows      int64  `json:"rows"`
}

type CreateReplayJobParams struct {
	DatasetID      string `json:"dataset_id"`
	EventFileID    string `json:"event_file_id"`
	IdempotencyKey string `json:"idempotency_key"`
	Symbol         string `json:"symbol"`
	Speed          string `json:"speed"`
}

type CompleteReplayJobParams struct {
	Metric ReplayMetric
	Errors []ValidationError
}

type UpdateEventFileStatsParams struct {
	SHA256 string
	Rows   int64
	Bytes  int64
}

type Repository interface {
	CreateDataset(ctx context.Context, params CreateDatasetParams) (Dataset, error)
	ListDatasets(ctx context.Context) ([]Dataset, error)
	GetDataset(ctx context.Context, id string) (Dataset, error)
	CreateEventFile(ctx context.Context, params CreateEventFileParams) (EventFile, error)
	GetEventFile(ctx context.Context, id string) (EventFile, error)
	ListEventFiles(ctx context.Context, datasetID string) ([]EventFile, error)
	UpdateEventFileStats(ctx context.Context, id string, params UpdateEventFileStatsParams) (EventFile, error)
	CreateReplayJob(ctx context.Context, params CreateReplayJobParams) (ReplayJob, error)
	GetReplayJob(ctx context.Context, id string) (ReplayJob, error)
	GetReplayJobByIdempotencyKey(ctx context.Context, key string) (ReplayJob, error)
	ListReplayJobs(ctx context.Context) ([]ReplayJob, error)
	UpdateReplayJobStatus(ctx context.Context, id string, status JobStatus, lastError string) (ReplayJob, error)
	UpdateReplayCheckpoint(ctx context.Context, id string, checkpointLine int64) error
	UpdateReplayManifest(ctx context.Context, id string, manifest ReplayManifest) (ReplayJob, error)
	CompleteReplayJob(ctx context.Context, jobID string, params CompleteReplayJobParams) (ReplayJob, error)
	ListReplayMetrics(ctx context.Context, jobID string) ([]ReplayMetric, error)
	ListValidationErrors(ctx context.Context, jobID string) ([]ValidationError, error)
	CancelReplayJob(ctx context.Context, jobID string) (ReplayJob, error)
}
