package store

import (
	"context"
	"errors"
	"testing"
)

func TestMemoryRepositoryRejectsDuplicateIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepository()
	dataset, err := repo.CreateDataset(ctx, CreateDatasetParams{Name: "fixture"})
	if err != nil {
		t.Fatalf("CreateDataset returned error: %v", err)
	}
	file, err := repo.CreateEventFile(ctx, CreateEventFileParams{DatasetID: dataset.ID, Path: "testdata/btcusdt_depth.jsonl", Format: "jsonl"})
	if err != nil {
		t.Fatalf("CreateEventFile returned error: %v", err)
	}

	params := CreateReplayJobParams{
		DatasetID:      dataset.ID,
		EventFileID:    file.ID,
		IdempotencyKey: "same-job",
		Speed:          "max",
	}
	first, err := repo.CreateReplayJob(ctx, params)
	if err != nil {
		t.Fatalf("first CreateReplayJob returned error: %v", err)
	}
	second, err := repo.CreateReplayJob(ctx, params)
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("second CreateReplayJob error = %v, want ErrAlreadyExists", err)
	}
	if second.ID != first.ID {
		t.Fatalf("duplicate job id = %s, want original %s", second.ID, first.ID)
	}
}

func TestMemoryRepositoryStoresMetricsAndValidationErrors(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepository()
	dataset, _ := repo.CreateDataset(ctx, CreateDatasetParams{Name: "fixture"})
	file, _ := repo.CreateEventFile(ctx, CreateEventFileParams{DatasetID: dataset.ID, Path: "testdata/sequence_gap.jsonl", Format: "jsonl"})
	job, _ := repo.CreateReplayJob(ctx, CreateReplayJobParams{DatasetID: dataset.ID, EventFileID: file.ID, Speed: "max"})

	_, err := repo.CompleteReplayJob(ctx, job.ID, CompleteReplayJobParams{
		Metric: ReplayMetric{Rows: 4, Events: 4, SequenceGaps: 1},
		Errors: []ValidationError{{
			Line:    2,
			Type:    "sequence_gap",
			Message: "gap",
		}},
	})
	if err != nil {
		t.Fatalf("CompleteReplayJob returned error: %v", err)
	}

	metrics, err := repo.ListReplayMetrics(ctx, job.ID)
	if err != nil {
		t.Fatalf("ListReplayMetrics returned error: %v", err)
	}
	if len(metrics) != 1 || metrics[0].SequenceGaps != 1 {
		t.Fatalf("metrics = %#v, want one gap metric", metrics)
	}
	errs, err := repo.ListValidationErrors(ctx, job.ID)
	if err != nil {
		t.Fatalf("ListValidationErrors returned error: %v", err)
	}
	if len(errs) != 1 || errs[0].Line != 2 {
		t.Fatalf("validation errors = %#v, want line 2 error", errs)
	}
}
