package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	storedb "github.com/orynwilder/market-replay-service/internal/store/db"
)

type PostgresRepository struct {
	pool    postgresPool
	queries *storedb.Queries
}

type postgresPool interface {
	storedb.DBTX
	Begin(ctx context.Context) (pgx.Tx, error)
}

func NewPostgresRepository(pool postgresPool) *PostgresRepository {
	return &PostgresRepository{pool: pool, queries: storedb.New(pool)}
}

func (r *PostgresRepository) CreateDataset(ctx context.Context, params CreateDatasetParams) (Dataset, error) {
	dataset, err := r.queries.CreateDataset(ctx, storedb.CreateDatasetParams{
		ID:          uuid.NewString(),
		Name:        params.Name,
		Description: params.Description,
	})
	if err != nil {
		return Dataset{}, mapPostgresError(err)
	}
	return convertDataset(dataset), nil
}

func (r *PostgresRepository) ListDatasets(ctx context.Context) ([]Dataset, error) {
	rows, err := r.queries.ListDatasets(ctx)
	if err != nil {
		return nil, mapPostgresError(err)
	}
	datasets := make([]Dataset, 0, len(rows))
	for _, row := range rows {
		datasets = append(datasets, convertDataset(row))
	}
	return datasets, nil
}

func (r *PostgresRepository) GetDataset(ctx context.Context, id string) (Dataset, error) {
	dataset, err := r.queries.GetDataset(ctx, id)
	if err != nil {
		return Dataset{}, mapPostgresError(err)
	}
	return convertDataset(dataset), nil
}

func (r *PostgresRepository) CreateEventFile(ctx context.Context, params CreateEventFileParams) (EventFile, error) {
	file, err := r.queries.CreateEventFile(ctx, storedb.CreateEventFileParams{
		ID:        uuid.NewString(),
		DatasetID: params.DatasetID,
		Path:      params.Path,
		Format:    params.Format,
		Symbol:    params.Symbol,
		Bytes:     params.Bytes,
		Sha256:    params.SHA256,
		Rows:      params.Rows,
	})
	if err != nil {
		return EventFile{}, mapPostgresError(err)
	}
	return convertEventFile(file), nil
}

func (r *PostgresRepository) GetEventFile(ctx context.Context, id string) (EventFile, error) {
	file, err := r.queries.GetEventFile(ctx, id)
	if err != nil {
		return EventFile{}, mapPostgresError(err)
	}
	return convertEventFile(file), nil
}

func (r *PostgresRepository) ListEventFiles(ctx context.Context, datasetID string) ([]EventFile, error) {
	if _, err := r.GetDataset(ctx, datasetID); err != nil {
		return nil, err
	}
	rows, err := r.queries.ListEventFiles(ctx, datasetID)
	if err != nil {
		return nil, mapPostgresError(err)
	}
	files := make([]EventFile, 0, len(rows))
	for _, row := range rows {
		files = append(files, convertEventFile(row))
	}
	return files, nil
}

func (r *PostgresRepository) UpdateEventFileStats(ctx context.Context, id string, params UpdateEventFileStatsParams) (EventFile, error) {
	file, err := r.queries.UpdateEventFileStats(ctx, storedb.UpdateEventFileStatsParams{
		ID:     id,
		Sha256: params.SHA256,
		Rows:   params.Rows,
		Bytes:  params.Bytes,
	})
	if err != nil {
		return EventFile{}, mapPostgresError(err)
	}
	return convertEventFile(file), nil
}

func (r *PostgresRepository) CreateReplayJob(ctx context.Context, params CreateReplayJobParams) (ReplayJob, error) {
	if params.IdempotencyKey != "" {
		if existing, err := r.GetReplayJobByIdempotencyKey(ctx, params.IdempotencyKey); err == nil {
			return existing, ErrAlreadyExists
		} else if !errors.Is(err, ErrNotFound) {
			return ReplayJob{}, err
		}
	}

	job, err := r.queries.CreateReplayJob(ctx, storedb.CreateReplayJobParams{
		ID:             uuid.NewString(),
		DatasetID:      params.DatasetID,
		EventFileID:    params.EventFileID,
		IdempotencyKey: textParam(params.IdempotencyKey),
		Symbol:         params.Symbol,
		Speed:          params.Speed,
		Status:         string(JobStatusQueued),
	})
	if err != nil {
		if isUniqueViolation(err) && params.IdempotencyKey != "" {
			existing, getErr := r.GetReplayJobByIdempotencyKey(ctx, params.IdempotencyKey)
			if getErr != nil {
				return ReplayJob{}, getErr
			}
			return existing, ErrAlreadyExists
		}
		return ReplayJob{}, mapPostgresError(err)
	}
	return convertReplayJob(
		job.ID, job.DatasetID, job.EventFileID, job.IdempotencyKey, job.Symbol, job.Speed, job.Status,
		job.Attempts, job.LastError, job.CheckpointLine, job.CreatedAt, job.StartedAt, job.CompletedAt, job.CanceledAt,
		job.Manifest,
	), nil
}

func (r *PostgresRepository) GetReplayJob(ctx context.Context, id string) (ReplayJob, error) {
	job, err := r.queries.GetReplayJob(ctx, id)
	if err != nil {
		return ReplayJob{}, mapPostgresError(err)
	}
	return convertReplayJob(
		job.ID, job.DatasetID, job.EventFileID, job.IdempotencyKey, job.Symbol, job.Speed, job.Status,
		job.Attempts, job.LastError, job.CheckpointLine, job.CreatedAt, job.StartedAt, job.CompletedAt, job.CanceledAt,
		job.Manifest,
	), nil
}

func (r *PostgresRepository) GetReplayJobByIdempotencyKey(ctx context.Context, key string) (ReplayJob, error) {
	job, err := r.queries.GetReplayJobByIdempotencyKey(ctx, textParam(key))
	if err != nil {
		return ReplayJob{}, mapPostgresError(err)
	}
	return convertReplayJob(
		job.ID, job.DatasetID, job.EventFileID, job.IdempotencyKey, job.Symbol, job.Speed, job.Status,
		job.Attempts, job.LastError, job.CheckpointLine, job.CreatedAt, job.StartedAt, job.CompletedAt, job.CanceledAt,
		job.Manifest,
	), nil
}

func (r *PostgresRepository) ListReplayJobs(ctx context.Context) ([]ReplayJob, error) {
	rows, err := r.queries.ListReplayJobs(ctx)
	if err != nil {
		return nil, mapPostgresError(err)
	}
	jobs := make([]ReplayJob, 0, len(rows))
	for _, row := range rows {
		jobs = append(jobs, convertReplayJob(
			row.ID, row.DatasetID, row.EventFileID, row.IdempotencyKey, row.Symbol, row.Speed, row.Status,
			row.Attempts, row.LastError, row.CheckpointLine, row.CreatedAt, row.StartedAt, row.CompletedAt, row.CanceledAt,
			row.Manifest,
		))
	}
	return jobs, nil
}

func (r *PostgresRepository) UpdateReplayJobStatus(ctx context.Context, id string, status JobStatus, lastError string) (ReplayJob, error) {
	job, err := r.queries.UpdateReplayJobStatus(ctx, storedb.UpdateReplayJobStatusParams{
		ID:        id,
		Status:    string(status),
		LastError: lastError,
		StartedAt: timestampParam(time.Now().UTC()),
	})
	if err != nil {
		return ReplayJob{}, mapPostgresError(err)
	}
	return convertReplayJob(
		job.ID, job.DatasetID, job.EventFileID, job.IdempotencyKey, job.Symbol, job.Speed, job.Status,
		job.Attempts, job.LastError, job.CheckpointLine, job.CreatedAt, job.StartedAt, job.CompletedAt, job.CanceledAt,
		job.Manifest,
	), nil
}

func (r *PostgresRepository) UpdateReplayCheckpoint(ctx context.Context, id string, checkpointLine int64) error {
	affected, err := r.queries.UpdateReplayCheckpoint(ctx, storedb.UpdateReplayCheckpointParams{
		ID:             id,
		CheckpointLine: checkpointLine,
	})
	if err != nil {
		return mapPostgresError(err)
	}
	if affected == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *PostgresRepository) UpdateReplayManifest(ctx context.Context, id string, manifest ReplayManifest) (ReplayJob, error) {
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return ReplayJob{}, err
	}
	job, err := r.queries.UpdateReplayManifest(ctx, storedb.UpdateReplayManifestParams{
		ID:       id,
		Manifest: encoded,
	})
	if err != nil {
		return ReplayJob{}, mapPostgresError(err)
	}
	return convertReplayJob(
		job.ID, job.DatasetID, job.EventFileID, job.IdempotencyKey, job.Symbol, job.Speed, job.Status,
		job.Attempts, job.LastError, job.CheckpointLine, job.CreatedAt, job.StartedAt, job.CompletedAt, job.CanceledAt,
		job.Manifest,
	), nil
}

func (r *PostgresRepository) CompleteReplayJob(ctx context.Context, jobID string, params CompleteReplayJobParams) (ReplayJob, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return ReplayJob{}, mapPostgresError(err)
	}
	defer tx.Rollback(ctx)

	q := r.queries.WithTx(tx)
	now := time.Now().UTC()
	affected, err := q.CompleteReplayJobStatus(ctx, storedb.CompleteReplayJobStatusParams{
		ID:          jobID,
		CompletedAt: timestampParam(now),
	})
	if err != nil {
		return ReplayJob{}, mapPostgresError(err)
	}
	if affected == 0 {
		return ReplayJob{}, ErrNotFound
	}

	metric := params.Metric
	if metric.ID == "" {
		metric.ID = uuid.NewString()
	}
	if metric.CreatedAt.IsZero() {
		metric.CreatedAt = now
	}
	if _, err := q.CreateReplayMetric(ctx, storedb.CreateReplayMetricParams{
		ID:              metric.ID,
		JobID:           jobID,
		Rows:            metric.Rows,
		Events:          metric.Events,
		MalformedEvents: metric.MalformedEvents,
		SequenceGaps:    metric.SequenceGaps,
		DurationNs:      metric.Duration.Nanoseconds(),
		RowsPerSecond:   metric.RowsPerSecond,
		EventsPerSecond: metric.EventsPerSecond,
		P95LatencyNs:    metric.P95Latency.Nanoseconds(),
		PeakAllocBytes:  int64(metric.PeakAllocBytes),
		AllocsPerEvent:  metric.AllocsPerEvent,
		CreatedAt:       timestampParam(metric.CreatedAt),
	}); err != nil {
		return ReplayJob{}, mapPostgresError(err)
	}

	for _, validationError := range params.Errors {
		if validationError.ID == "" {
			validationError.ID = uuid.NewString()
		}
		if validationError.CreatedAt.IsZero() {
			validationError.CreatedAt = now
		}
		if _, err := q.CreateValidationError(ctx, storedb.CreateValidationErrorParams{
			ID:        validationError.ID,
			JobID:     jobID,
			Line:      validationError.Line,
			Symbol:    validationError.Symbol,
			Type:      validationError.Type,
			Message:   validationError.Message,
			CreatedAt: timestampParam(validationError.CreatedAt),
		}); err != nil {
			return ReplayJob{}, mapPostgresError(err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return ReplayJob{}, mapPostgresError(err)
	}
	return r.GetReplayJob(ctx, jobID)
}

func (r *PostgresRepository) ListReplayMetrics(ctx context.Context, jobID string) ([]ReplayMetric, error) {
	rows, err := r.queries.ListReplayMetrics(ctx, jobID)
	if err != nil {
		return nil, mapPostgresError(err)
	}
	metrics := make([]ReplayMetric, 0, len(rows))
	for _, row := range rows {
		metrics = append(metrics, convertReplayMetric(row))
	}
	return metrics, nil
}

func (r *PostgresRepository) ListValidationErrors(ctx context.Context, jobID string) ([]ValidationError, error) {
	rows, err := r.queries.ListValidationErrors(ctx, jobID)
	if err != nil {
		return nil, mapPostgresError(err)
	}
	validationErrors := make([]ValidationError, 0, len(rows))
	for _, row := range rows {
		validationErrors = append(validationErrors, convertValidationError(row))
	}
	return validationErrors, nil
}

func (r *PostgresRepository) CancelReplayJob(ctx context.Context, jobID string) (ReplayJob, error) {
	job, err := r.queries.CancelReplayJob(ctx, storedb.CancelReplayJobParams{
		ID:         jobID,
		CanceledAt: timestampParam(time.Now().UTC()),
	})
	if err != nil {
		return ReplayJob{}, mapPostgresError(err)
	}
	return convertReplayJob(
		job.ID, job.DatasetID, job.EventFileID, job.IdempotencyKey, job.Symbol, job.Speed, job.Status,
		job.Attempts, job.LastError, job.CheckpointLine, job.CreatedAt, job.StartedAt, job.CompletedAt, job.CanceledAt,
		job.Manifest,
	), nil
}

func convertDataset(row storedb.Dataset) Dataset {
	return Dataset{
		ID:          row.ID,
		Name:        row.Name,
		Description: row.Description,
		CreatedAt:   timeFromPG(row.CreatedAt),
	}
}

func convertEventFile(row storedb.EventFile) EventFile {
	return EventFile{
		ID:        row.ID,
		DatasetID: row.DatasetID,
		Path:      row.Path,
		Format:    row.Format,
		Symbol:    row.Symbol,
		Bytes:     row.Bytes,
		SHA256:    row.Sha256,
		Rows:      row.Rows,
		CreatedAt: timeFromPG(row.CreatedAt),
	}
}

func convertReplayJob(
	id string,
	datasetID string,
	eventFileID string,
	idempotencyKey string,
	symbol string,
	speed string,
	status string,
	attempts int32,
	lastError string,
	checkpointLine int64,
	createdAt pgtype.Timestamptz,
	startedAt pgtype.Timestamptz,
	completedAt pgtype.Timestamptz,
	canceledAt pgtype.Timestamptz,
	manifestJSON []byte,
) ReplayJob {
	return ReplayJob{
		ID:             id,
		DatasetID:      datasetID,
		EventFileID:    eventFileID,
		IdempotencyKey: idempotencyKey,
		Symbol:         symbol,
		Speed:          speed,
		Status:         JobStatus(status),
		Attempts:       int(attempts),
		LastError:      lastError,
		CheckpointLine: checkpointLine,
		CreatedAt:      timeFromPG(createdAt),
		StartedAt:      timePtrFromPG(startedAt),
		CompletedAt:    timePtrFromPG(completedAt),
		CanceledAt:     timePtrFromPG(canceledAt),
		Manifest:       replayManifestFromJSON(manifestJSON),
	}
}

func convertReplayMetric(row storedb.ReplayMetric) ReplayMetric {
	return ReplayMetric{
		ID:              row.ID,
		JobID:           row.JobID,
		Rows:            row.Rows,
		Events:          row.Events,
		MalformedEvents: row.MalformedEvents,
		SequenceGaps:    row.SequenceGaps,
		Duration:        time.Duration(row.DurationNs),
		RowsPerSecond:   row.RowsPerSecond,
		EventsPerSecond: row.EventsPerSecond,
		P95Latency:      time.Duration(row.P95LatencyNs),
		PeakAllocBytes:  uint64(row.PeakAllocBytes),
		AllocsPerEvent:  row.AllocsPerEvent,
		CreatedAt:       timeFromPG(row.CreatedAt),
	}
}

func replayManifestFromJSON(data []byte) ReplayManifest {
	if len(data) == 0 {
		return ReplayManifest{}
	}
	var manifest ReplayManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return ReplayManifest{}
	}
	return manifest
}

func convertValidationError(row storedb.ValidationError) ValidationError {
	return ValidationError{
		ID:        row.ID,
		JobID:     row.JobID,
		Line:      row.Line,
		Symbol:    row.Symbol,
		Type:      row.Type,
		Message:   row.Message,
		CreatedAt: timeFromPG(row.CreatedAt),
	}
}

func textParam(value string) pgtype.Text {
	if value == "" {
		return pgtype.Text{}
	}
	return pgtype.Text{String: value, Valid: true}
}

func timestampParam(value time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: value.UTC(), Valid: true}
}

func timeFromPG(value pgtype.Timestamptz) time.Time {
	if !value.Valid {
		return time.Time{}
	}
	return value.Time.UTC()
}

func timePtrFromPG(value pgtype.Timestamptz) *time.Time {
	if !value.Valid {
		return nil
	}
	ts := value.Time.UTC()
	return &ts
}

func mapPostgresError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23503":
			return ErrNotFound
		case "23505":
			return ErrAlreadyExists
		}
	}
	return err
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
