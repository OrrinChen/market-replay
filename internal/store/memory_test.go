package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
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

func TestMemoryRepositoryBuildsLineageAndQualityReport(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryRepository()
	dataset, _ := repo.CreateDataset(ctx, CreateDatasetParams{Name: "governance-fixture"})
	file, _ := repo.CreateEventFile(ctx, CreateEventFileParams{
		DatasetID: dataset.ID,
		Path:      "testdata/sequence_gap.jsonl",
		Format:    "jsonl",
		Symbol:    "BTCUSDT",
		Bytes:     4096,
		SHA256:    strings.Repeat("a", 64),
		Rows:      4,
	})
	job, _ := repo.CreateReplayJob(ctx, CreateReplayJobParams{
		DatasetID:      dataset.ID,
		EventFileID:    file.ID,
		IdempotencyKey: "governance-report",
		Symbol:         "BTCUSDT",
		Speed:          "max",
	})
	manifest := ReplayManifest{
		InputFileSHA256: strings.Repeat("a", 64),
		InputRows:       4,
		InputBytes:      4096,
		AppVersion:      "test",
		CheckpointLine:  4,
		ResumeFromLine:  1,
		ErrorCount:      1,
		SequenceGaps:    1,
		Duration:        25 * time.Millisecond,
	}
	if _, err := repo.UpdateReplayManifest(ctx, job.ID, manifest); err != nil {
		t.Fatalf("UpdateReplayManifest returned error: %v", err)
	}
	_, err := repo.CompleteReplayJob(ctx, job.ID, CompleteReplayJobParams{
		Metric: ReplayMetric{Rows: 4, Events: 3, SequenceGaps: 1, Duration: 25 * time.Millisecond},
		Errors: []ValidationError{{
			Line:    2,
			Symbol:  "BTCUSDT",
			Type:    "sequence_gap",
			Message: "gap",
		}},
	})
	if err != nil {
		t.Fatalf("CompleteReplayJob returned error: %v", err)
	}

	lineage, err := BuildDatasetLineage(ctx, repo, dataset.ID)
	if err != nil {
		t.Fatalf("BuildDatasetLineage returned error: %v", err)
	}
	if lineage.Dataset.ID != dataset.ID || len(lineage.EventFiles) != 1 || len(lineage.EventFiles[0].Jobs) != 1 {
		t.Fatalf("lineage = %#v, want dataset -> one file -> one job", lineage)
	}
	if lineage.EventFiles[0].Jobs[0].ErrorCount != 1 || lineage.EventFiles[0].Jobs[0].MetricCount != 1 {
		t.Fatalf("lineage job = %#v, want error and metric counts", lineage.EventFiles[0].Jobs[0])
	}

	report, err := BuildReplayQualityReport(ctx, repo, job.ID)
	if err != nil {
		t.Fatalf("BuildReplayQualityReport returned error: %v", err)
	}
	if report.Dataset.ID != dataset.ID || report.EventFile.SHA256 != strings.Repeat("a", 64) {
		t.Fatalf("report linkage = %#v, want dataset and event file hash", report)
	}
	if report.Job.Manifest.InputRows != 4 || report.Job.Manifest.ResumeFromLine != 1 {
		t.Fatalf("report manifest = %#v, want input rows and resume line", report.Job.Manifest)
	}
	if len(report.ErrorSummary) != 1 || report.ErrorSummary[0].Type != "sequence_gap" || report.ErrorSummary[0].Count != 1 {
		t.Fatalf("error summary = %#v, want one sequence_gap", report.ErrorSummary)
	}
}
