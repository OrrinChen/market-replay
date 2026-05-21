//go:build integration

package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresRepositoryIntegrationSchemaAndReplayJobLifecycle(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("DATABASE_URL is not set; skipping Postgres integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	adminPool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect postgres admin pool: %v", err)
	}
	defer adminPool.Close()

	schema := fmt.Sprintf("mrs_it_%d", time.Now().UnixNano())
	if _, err := adminPool.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatalf("create isolated schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = adminPool.Exec(cleanupCtx, `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
	})

	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse DATABASE_URL: %v", err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect postgres test pool: %v", err)
	}
	defer pool.Close()

	if err := EnsurePostgresSchema(ctx, pool); err != nil {
		t.Fatalf("EnsurePostgresSchema returned error: %v", err)
	}
	for _, table := range []string{"datasets", "event_files", "replay_jobs", "replay_metrics", "validation_errors"} {
		var exists bool
		if err := pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, table).Scan(&exists); err != nil {
			t.Fatalf("check table %s: %v", table, err)
		}
		if !exists {
			t.Fatalf("table %s was not created in integration schema", table)
		}
	}

	repo := NewPostgresRepository(pool)
	dataset, err := repo.CreateDataset(ctx, CreateDatasetParams{
		Name:        "integration-fixture",
		Description: "Postgres repository integration test",
	})
	if err != nil {
		t.Fatalf("CreateDataset returned error: %v", err)
	}
	datasets, err := repo.ListDatasets(ctx)
	if err != nil {
		t.Fatalf("ListDatasets returned error: %v", err)
	}
	if len(datasets) != 1 || datasets[0].ID != dataset.ID {
		t.Fatalf("ListDatasets = %#v, want created dataset", datasets)
	}
	gotDataset, err := repo.GetDataset(ctx, dataset.ID)
	if err != nil {
		t.Fatalf("GetDataset returned error: %v", err)
	}
	if gotDataset.Name != dataset.Name {
		t.Fatalf("GetDataset name = %q, want %q", gotDataset.Name, dataset.Name)
	}

	file, err := repo.CreateEventFile(ctx, CreateEventFileParams{
		DatasetID: dataset.ID,
		Path:      "testdata/btcusdt_depth.jsonl",
		Format:    "jsonl",
		Symbol:    "BTCUSDT",
		Bytes:     128,
		SHA256:    strings.Repeat("c", 64),
		Rows:      2,
	})
	if err != nil {
		t.Fatalf("CreateEventFile returned error: %v", err)
	}
	if gotFile, err := repo.GetEventFile(ctx, file.ID); err != nil {
		t.Fatalf("GetEventFile returned error: %v", err)
	} else if gotFile.SHA256 != strings.Repeat("c", 64) || gotFile.Rows != 2 {
		t.Fatalf("GetEventFile = %#v, want governance file stats", gotFile)
	}
	files, err := repo.ListEventFiles(ctx, dataset.ID)
	if err != nil {
		t.Fatalf("ListEventFiles returned error: %v", err)
	}
	if len(files) != 1 || files[0].ID != file.ID {
		t.Fatalf("ListEventFiles = %#v, want created file", files)
	}

	params := CreateReplayJobParams{
		DatasetID:      dataset.ID,
		EventFileID:    file.ID,
		IdempotencyKey: "postgres-integration-idempotency-key",
		Symbol:         "BTCUSDT",
		Speed:          "max",
	}
	job, err := repo.CreateReplayJob(ctx, params)
	if err != nil {
		t.Fatalf("CreateReplayJob returned error: %v", err)
	}
	duplicate, err := repo.CreateReplayJob(ctx, params)
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("duplicate CreateReplayJob error = %v, want ErrAlreadyExists", err)
	}
	if duplicate.ID != job.ID {
		t.Fatalf("duplicate job id = %s, want original %s", duplicate.ID, job.ID)
	}
	byKey, err := repo.GetReplayJobByIdempotencyKey(ctx, params.IdempotencyKey)
	if err != nil {
		t.Fatalf("GetReplayJobByIdempotencyKey returned error: %v", err)
	}
	if byKey.ID != job.ID {
		t.Fatalf("idempotency lookup id = %s, want %s", byKey.ID, job.ID)
	}
	jobs, err := repo.ListReplayJobs(ctx)
	if err != nil {
		t.Fatalf("ListReplayJobs returned error: %v", err)
	}
	if len(jobs) != 1 || jobs[0].ID != job.ID {
		t.Fatalf("ListReplayJobs = %#v, want created job only", jobs)
	}
	if _, err := repo.UpdateEventFileStats(ctx, file.ID, UpdateEventFileStatsParams{
		SHA256: strings.Repeat("d", 64),
		Rows:   4,
		Bytes:  256,
	}); err != nil {
		t.Fatalf("UpdateEventFileStats returned error: %v", err)
	}
	manifest := ReplayManifest{
		InputFileSHA256: strings.Repeat("d", 64),
		InputRows:       4,
		InputBytes:      256,
		AppVersion:      "integration-test",
		CheckpointLine:  4,
		ErrorCount:      1,
		SequenceGaps:    1,
	}
	if updatedJob, err := repo.UpdateReplayManifest(ctx, job.ID, manifest); err != nil {
		t.Fatalf("UpdateReplayManifest returned error: %v", err)
	} else if updatedJob.Manifest.InputRows != 4 || updatedJob.Manifest.AppVersion != "integration-test" {
		t.Fatalf("UpdateReplayManifest job = %#v, want manifest fields", updatedJob)
	}

	completed, err := repo.CompleteReplayJob(ctx, job.ID, CompleteReplayJobParams{
		Metric: ReplayMetric{
			Rows:            2,
			Events:          2,
			SequenceGaps:    1,
			Duration:        25 * time.Millisecond,
			RowsPerSecond:   80,
			EventsPerSecond: 80,
			P95Latency:      time.Millisecond,
			PeakAllocBytes:  1024,
			AllocsPerEvent:  2,
		},
		Errors: []ValidationError{{
			Line:    2,
			Symbol:  "BTCUSDT",
			Type:    "sequence_gap",
			Message: "integration gap",
		}},
	})
	if err != nil {
		t.Fatalf("CompleteReplayJob returned error: %v", err)
	}
	if completed.Status != JobStatusCompleted || completed.CompletedAt == nil {
		t.Fatalf("completed job = %#v, want completed status and timestamp", completed)
	}
	metrics, err := repo.ListReplayMetrics(ctx, job.ID)
	if err != nil {
		t.Fatalf("ListReplayMetrics returned error: %v", err)
	}
	if len(metrics) != 1 || metrics[0].SequenceGaps != 1 {
		t.Fatalf("metrics = %#v, want one sequence gap metric", metrics)
	}
	validationErrors, err := repo.ListValidationErrors(ctx, job.ID)
	if err != nil {
		t.Fatalf("ListValidationErrors returned error: %v", err)
	}
	if len(validationErrors) != 1 || validationErrors[0].Line != 2 {
		t.Fatalf("validation errors = %#v, want one line 2 error", validationErrors)
	}
	lineage, err := BuildDatasetLineage(ctx, repo, dataset.ID)
	if err != nil {
		t.Fatalf("BuildDatasetLineage returned error: %v", err)
	}
	if len(lineage.EventFiles) != 1 || len(lineage.EventFiles[0].Jobs) != 1 || lineage.EventFiles[0].Jobs[0].ErrorCount != 1 {
		t.Fatalf("lineage = %#v, want file, job, and error count", lineage)
	}
	report, err := BuildReplayQualityReport(ctx, repo, job.ID)
	if err != nil {
		t.Fatalf("BuildReplayQualityReport returned error: %v", err)
	}
	if report.Job.Manifest.InputFileSHA256 != strings.Repeat("d", 64) || len(report.ErrorSummary) != 1 {
		t.Fatalf("report = %#v, want manifest and error summary", report)
	}
}
