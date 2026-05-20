package store

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

type MemoryRepository struct {
	mu             sync.RWMutex
	datasets       map[string]Dataset
	eventFiles     map[string]EventFile
	jobs           map[string]ReplayJob
	idempotencyMap map[string]string
	metrics        map[string][]ReplayMetric
	errors         map[string][]ValidationError
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		datasets:       make(map[string]Dataset),
		eventFiles:     make(map[string]EventFile),
		jobs:           make(map[string]ReplayJob),
		idempotencyMap: make(map[string]string),
		metrics:        make(map[string][]ReplayMetric),
		errors:         make(map[string][]ValidationError),
	}
}

func (r *MemoryRepository) CreateDataset(_ context.Context, params CreateDatasetParams) (Dataset, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now().UTC()
	dataset := Dataset{ID: uuid.NewString(), Name: params.Name, Description: params.Description, CreatedAt: now}
	r.datasets[dataset.ID] = dataset
	return dataset, nil
}

func (r *MemoryRepository) ListDatasets(_ context.Context) ([]Dataset, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	items := make([]Dataset, 0, len(r.datasets))
	for _, dataset := range r.datasets {
		items = append(items, dataset)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].CreatedAt.Before(items[j].CreatedAt) })
	return items, nil
}

func (r *MemoryRepository) GetDataset(_ context.Context, id string) (Dataset, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	dataset, ok := r.datasets[id]
	if !ok {
		return Dataset{}, ErrNotFound
	}
	return dataset, nil
}

func (r *MemoryRepository) CreateEventFile(_ context.Context, params CreateEventFileParams) (EventFile, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.datasets[params.DatasetID]; !ok {
		return EventFile{}, ErrNotFound
	}
	file := EventFile{
		ID:        uuid.NewString(),
		DatasetID: params.DatasetID,
		Path:      params.Path,
		Format:    params.Format,
		Symbol:    params.Symbol,
		Bytes:     params.Bytes,
		CreatedAt: time.Now().UTC(),
	}
	r.eventFiles[file.ID] = file
	return file, nil
}

func (r *MemoryRepository) GetEventFile(_ context.Context, id string) (EventFile, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	file, ok := r.eventFiles[id]
	if !ok {
		return EventFile{}, ErrNotFound
	}
	return file, nil
}

func (r *MemoryRepository) CreateReplayJob(_ context.Context, params CreateReplayJobParams) (ReplayJob, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.datasets[params.DatasetID]; !ok {
		return ReplayJob{}, ErrNotFound
	}
	if _, ok := r.eventFiles[params.EventFileID]; !ok {
		return ReplayJob{}, ErrNotFound
	}
	if params.IdempotencyKey != "" {
		if id, ok := r.idempotencyMap[params.IdempotencyKey]; ok {
			return r.jobs[id], ErrAlreadyExists
		}
	}

	job := ReplayJob{
		ID:             uuid.NewString(),
		DatasetID:      params.DatasetID,
		EventFileID:    params.EventFileID,
		IdempotencyKey: params.IdempotencyKey,
		Symbol:         params.Symbol,
		Speed:          params.Speed,
		Status:         JobStatusQueued,
		CreatedAt:      time.Now().UTC(),
	}
	r.jobs[job.ID] = job
	if params.IdempotencyKey != "" {
		r.idempotencyMap[params.IdempotencyKey] = job.ID
	}
	return job, nil
}

func (r *MemoryRepository) GetReplayJob(_ context.Context, id string) (ReplayJob, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	job, ok := r.jobs[id]
	if !ok {
		return ReplayJob{}, ErrNotFound
	}
	return job, nil
}

func (r *MemoryRepository) GetReplayJobByIdempotencyKey(_ context.Context, key string) (ReplayJob, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	id, ok := r.idempotencyMap[key]
	if !ok {
		return ReplayJob{}, ErrNotFound
	}
	return r.jobs[id], nil
}

func (r *MemoryRepository) ListReplayJobs(_ context.Context) ([]ReplayJob, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	items := make([]ReplayJob, 0, len(r.jobs))
	for _, job := range r.jobs {
		items = append(items, job)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].CreatedAt.Before(items[j].CreatedAt) })
	return items, nil
}

func (r *MemoryRepository) UpdateReplayJobStatus(_ context.Context, id string, status JobStatus, lastError string) (ReplayJob, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	job, ok := r.jobs[id]
	if !ok {
		return ReplayJob{}, ErrNotFound
	}
	now := time.Now().UTC()
	job.Status = status
	job.LastError = lastError
	job.Attempts++
	switch status {
	case JobStatusRunning:
		job.StartedAt = &now
	case JobStatusCompleted, JobStatusFailed, JobStatusDLQ:
		job.CompletedAt = &now
	case JobStatusCanceled:
		job.CanceledAt = &now
	}
	r.jobs[id] = job
	return job, nil
}

func (r *MemoryRepository) UpdateReplayCheckpoint(_ context.Context, id string, checkpointLine int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	job, ok := r.jobs[id]
	if !ok {
		return ErrNotFound
	}
	if checkpointLine > job.CheckpointLine {
		job.CheckpointLine = checkpointLine
		r.jobs[id] = job
	}
	return nil
}

func (r *MemoryRepository) CompleteReplayJob(_ context.Context, jobID string, params CompleteReplayJobParams) (ReplayJob, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	job, ok := r.jobs[jobID]
	if !ok {
		return ReplayJob{}, ErrNotFound
	}
	now := time.Now().UTC()
	job.Status = JobStatusCompleted
	job.CompletedAt = &now
	r.jobs[jobID] = job

	metric := params.Metric
	if metric.ID == "" {
		metric.ID = uuid.NewString()
	}
	metric.JobID = jobID
	if metric.CreatedAt.IsZero() {
		metric.CreatedAt = now
	}
	r.metrics[jobID] = append(r.metrics[jobID], metric)

	for _, validationError := range params.Errors {
		if validationError.ID == "" {
			validationError.ID = uuid.NewString()
		}
		validationError.JobID = jobID
		if validationError.CreatedAt.IsZero() {
			validationError.CreatedAt = now
		}
		r.errors[jobID] = append(r.errors[jobID], validationError)
	}
	return job, nil
}

func (r *MemoryRepository) ListReplayMetrics(_ context.Context, jobID string) ([]ReplayMetric, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	items := append([]ReplayMetric(nil), r.metrics[jobID]...)
	return items, nil
}

func (r *MemoryRepository) ListValidationErrors(_ context.Context, jobID string) ([]ValidationError, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	items := append([]ValidationError(nil), r.errors[jobID]...)
	return items, nil
}

func (r *MemoryRepository) CancelReplayJob(_ context.Context, jobID string) (ReplayJob, error) {
	return r.UpdateReplayJobStatus(context.Background(), jobID, JobStatusCanceled, "")
}
