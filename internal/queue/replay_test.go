package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/orynwilder/market-replay-service/internal/store"
)

type fakeAsynqClient struct {
	calls int
	task  *asynq.Task
	opts  []asynq.Option
	err   error
}

func (c *fakeAsynqClient) EnqueueContext(_ context.Context, task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	c.calls++
	c.task = task
	c.opts = append([]asynq.Option(nil), opts...)
	if c.err != nil {
		return nil, c.err
	}
	return &asynq.TaskInfo{ID: "task-id"}, nil
}

func TestReplayPayloadRoundTrip(t *testing.T) {
	payload, err := EncodeReplayPayload(ReplayPayload{JobID: "job-123"})
	if err != nil {
		t.Fatalf("EncodeReplayPayload returned error: %v", err)
	}

	got, err := DecodeReplayPayload(payload)
	if err != nil {
		t.Fatalf("DecodeReplayPayload returned error: %v", err)
	}
	if got.JobID != "job-123" {
		t.Fatalf("decoded job id = %q, want job-123", got.JobID)
	}
}

func TestDecodeReplayPayloadRejectsMissingJobID(t *testing.T) {
	_, err := DecodeReplayPayload([]byte(`{"job_id":""}`))
	if err == nil {
		t.Fatal("DecodeReplayPayload returned nil error, want missing job id error")
	}
}

func TestSubmitReplayEnqueuesCreatedJobWithIdempotencyOptions(t *testing.T) {
	ctx := context.Background()
	repo := store.NewMemoryRepository()
	dataset, _ := repo.CreateDataset(ctx, store.CreateDatasetParams{Name: "fixture"})
	file, _ := repo.CreateEventFile(ctx, store.CreateEventFileParams{DatasetID: dataset.ID, Path: "../../testdata/btcusdt_depth.jsonl", Format: "jsonl"})
	client := &fakeAsynqClient{}
	enqueuer := NewReplayEnqueuer(repo, client, ReplayEnqueueConfig{
		Queue:          "replay",
		MaxRetry:       5,
		Timeout:        2 * time.Minute,
		UniqueTTL:      time.Hour,
		TaskIDPrefix:   "replay-job:",
		RetryBackoff:   3 * time.Second,
		DeadLetterName: "asynq-archive",
	})

	job, info, err := enqueuer.SubmitReplay(ctx, store.CreateReplayJobParams{
		DatasetID:      dataset.ID,
		EventFileID:    file.ID,
		IdempotencyKey: "same-input",
		Speed:          "max",
	})
	if err != nil {
		t.Fatalf("SubmitReplay returned error: %v", err)
	}
	if info == nil || info.ID != "task-id" {
		t.Fatalf("task info = %#v, want task-id", info)
	}
	if client.calls != 1 {
		t.Fatalf("enqueue calls = %d, want 1", client.calls)
	}
	if client.task.Type() != TypeReplayJob {
		t.Fatalf("task type = %q, want %q", client.task.Type(), TypeReplayJob)
	}
	decoded, err := DecodeReplayPayload(client.task.Payload())
	if err != nil {
		t.Fatalf("DecodeReplayPayload returned error: %v", err)
	}
	if decoded.JobID != job.ID {
		t.Fatalf("payload job id = %q, want %q", decoded.JobID, job.ID)
	}
	if !hasOption(client.opts, asynq.TaskIDOpt, "replay-job:same-input") {
		t.Fatalf("enqueue options missing TaskID replay-job:same-input: %#v", client.opts)
	}
	if !hasOption(client.opts, asynq.UniqueOpt, time.Hour) {
		t.Fatalf("enqueue options missing Unique 1h: %#v", client.opts)
	}
}

func TestSubmitReplayReturnsExistingJobWithoutDuplicateEnqueue(t *testing.T) {
	ctx := context.Background()
	repo := store.NewMemoryRepository()
	dataset, _ := repo.CreateDataset(ctx, store.CreateDatasetParams{Name: "fixture"})
	file, _ := repo.CreateEventFile(ctx, store.CreateEventFileParams{DatasetID: dataset.ID, Path: "../../testdata/btcusdt_depth.jsonl", Format: "jsonl"})
	params := store.CreateReplayJobParams{
		DatasetID:      dataset.ID,
		EventFileID:    file.ID,
		IdempotencyKey: "same-input",
		Speed:          "max",
	}
	first, _ := repo.CreateReplayJob(ctx, params)
	client := &fakeAsynqClient{}
	enqueuer := NewReplayEnqueuer(repo, client, ReplayEnqueueConfig{})

	got, info, err := enqueuer.SubmitReplay(ctx, params)
	if err != nil {
		t.Fatalf("SubmitReplay returned error: %v", err)
	}
	if got.ID != first.ID {
		t.Fatalf("duplicate job id = %q, want %q", got.ID, first.ID)
	}
	if info != nil {
		t.Fatalf("task info = %#v, want nil for duplicate idempotency key", info)
	}
	if client.calls != 0 {
		t.Fatalf("enqueue calls = %d, want 0", client.calls)
	}
}

func TestSubmitReplayTreatsAsynqDuplicateAsIdempotentSuccess(t *testing.T) {
	ctx := context.Background()
	repo := store.NewMemoryRepository()
	dataset, _ := repo.CreateDataset(ctx, store.CreateDatasetParams{Name: "fixture"})
	file, _ := repo.CreateEventFile(ctx, store.CreateEventFileParams{DatasetID: dataset.ID, Path: "../../testdata/btcusdt_depth.jsonl", Format: "jsonl"})
	client := &fakeAsynqClient{err: asynq.ErrTaskIDConflict}
	enqueuer := NewReplayEnqueuer(repo, client, ReplayEnqueueConfig{})

	job, info, err := enqueuer.SubmitReplay(ctx, store.CreateReplayJobParams{
		DatasetID:      dataset.ID,
		EventFileID:    file.ID,
		IdempotencyKey: "same-input",
		Speed:          "max",
	})
	if err != nil {
		t.Fatalf("SubmitReplay returned error: %v", err)
	}
	if job.ID == "" {
		t.Fatal("SubmitReplay returned empty job id")
	}
	if info != nil {
		t.Fatalf("task info = %#v, want nil for asynq duplicate", info)
	}
}

func TestSubmitReplayReturnsNonDuplicateEnqueueError(t *testing.T) {
	ctx := context.Background()
	repo := store.NewMemoryRepository()
	dataset, _ := repo.CreateDataset(ctx, store.CreateDatasetParams{Name: "fixture"})
	file, _ := repo.CreateEventFile(ctx, store.CreateEventFileParams{DatasetID: dataset.ID, Path: "../../testdata/btcusdt_depth.jsonl", Format: "jsonl"})
	wantErr := errors.New("redis unavailable")
	client := &fakeAsynqClient{err: wantErr}
	enqueuer := NewReplayEnqueuer(repo, client, ReplayEnqueueConfig{})

	_, _, err := enqueuer.SubmitReplay(ctx, store.CreateReplayJobParams{
		DatasetID:      dataset.ID,
		EventFileID:    file.ID,
		IdempotencyKey: "same-input",
		Speed:          "max",
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("SubmitReplay error = %v, want %v", err, wantErr)
	}
}

func hasOption(opts []asynq.Option, typ asynq.OptionType, value any) bool {
	for _, opt := range opts {
		if opt.Type() == typ && opt.Value() == value {
			return true
		}
	}
	return false
}
