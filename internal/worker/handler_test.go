package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/hibiken/asynq"
	"github.com/orynwilder/market-replay-service/internal/observability"
	jobqueue "github.com/orynwilder/market-replay-service/internal/queue"
	"github.com/orynwilder/market-replay-service/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"go.uber.org/zap/zaptest"
)

func TestReplayHandlerCompletesJobFromMemoryRepositoryFixture(t *testing.T) {
	ctx := context.Background()
	repo := store.NewMemoryRepository()
	dataset, _ := repo.CreateDataset(ctx, store.CreateDatasetParams{Name: "fixture"})
	file, _ := repo.CreateEventFile(ctx, store.CreateEventFileParams{
		DatasetID: dataset.ID,
		Path:      "../../testdata/sequence_gap.jsonl",
		Format:    "jsonl",
		Symbol:    "BTCUSDT",
	})
	job, _ := repo.CreateReplayJob(ctx, store.CreateReplayJobParams{
		DatasetID:   dataset.ID,
		EventFileID: file.ID,
		Symbol:      "BTCUSDT",
		Speed:       "max",
	})
	payload, _ := jobqueue.EncodeReplayPayload(jobqueue.ReplayPayload{JobID: job.ID})
	handler := NewReplayHandler(repo, zaptest.NewLogger(t), ReplayHandlerConfig{})

	if err := handler.ProcessTask(ctx, asynq.NewTask(jobqueue.TypeReplayJob, payload)); err != nil {
		t.Fatalf("ProcessTask returned error: %v", err)
	}

	got, err := repo.GetReplayJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("GetReplayJob returned error: %v", err)
	}
	if got.Status != store.JobStatusCompleted {
		t.Fatalf("job status = %q, want completed", got.Status)
	}
	if got.CheckpointLine == 0 {
		t.Fatal("checkpoint line = 0, want final row checkpoint")
	}
	if got.Manifest.InputFileSHA256 == "" || got.Manifest.InputRows != got.CheckpointLine {
		t.Fatalf("manifest = %#v, want input hash and final row count", got.Manifest)
	}
	updatedFile, err := repo.GetEventFile(ctx, file.ID)
	if err != nil {
		t.Fatalf("GetEventFile returned error: %v", err)
	}
	if updatedFile.SHA256 == "" || updatedFile.Rows != got.CheckpointLine || updatedFile.Bytes == 0 {
		t.Fatalf("event file stats = %#v, want hash, rows, and bytes", updatedFile)
	}
	metrics, err := repo.ListReplayMetrics(ctx, job.ID)
	if err != nil {
		t.Fatalf("ListReplayMetrics returned error: %v", err)
	}
	if len(metrics) != 1 || metrics[0].Rows == 0 || metrics[0].SequenceGaps == 0 {
		t.Fatalf("metrics = %#v, want one metric with rows and sequence gap", metrics)
	}
	errs, err := repo.ListValidationErrors(ctx, job.ID)
	if err != nil {
		t.Fatalf("ListValidationErrors returned error: %v", err)
	}
	if len(errs) == 0 || errs[0].Type != "sequence_gap" {
		t.Fatalf("validation errors = %#v, want sequence_gap", errs)
	}
}

func TestReplayHandlerRecordsReplayAndSequenceGapMetrics(t *testing.T) {
	ctx := context.Background()
	repo := store.NewMemoryRepository()
	registry := prometheus.NewRegistry()
	metrics, err := observability.Register(registry)
	if err != nil {
		t.Fatalf("register metrics: %v", err)
	}
	dataset, _ := repo.CreateDataset(ctx, store.CreateDatasetParams{Name: "fixture"})
	file, _ := repo.CreateEventFile(ctx, store.CreateEventFileParams{
		DatasetID: dataset.ID,
		Path:      "../../testdata/sequence_gap.jsonl",
		Format:    "jsonl",
		Symbol:    "BTCUSDT",
	})
	job, _ := repo.CreateReplayJob(ctx, store.CreateReplayJobParams{
		DatasetID:   dataset.ID,
		EventFileID: file.ID,
		Symbol:      "BTCUSDT",
		Speed:       "max",
	})
	payload, _ := jobqueue.EncodeReplayPayload(jobqueue.ReplayPayload{JobID: job.ID})
	handler := NewReplayHandler(repo, zaptest.NewLogger(t), ReplayHandlerConfig{Metrics: metrics})

	if err := handler.ProcessTask(ctx, asynq.NewTask(jobqueue.TypeReplayJob, payload)); err != nil {
		t.Fatalf("ProcessTask returned error: %v", err)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}

	assertLabeledMetricValue(t, families, "replay_events_total", map[string]string{
		"dataset": "fixture",
		"symbol":  "BTCUSDT",
		"status":  string(store.JobStatusCompleted),
	}, 4)
	assertLabeledMetricValue(t, families, "replay_sequence_gaps_total", map[string]string{
		"dataset": "fixture",
		"symbol":  "BTCUSDT",
	}, 1)
}

func TestReplayHandlerResumesAfterCheckpointWithPrimedValidationState(t *testing.T) {
	ctx := context.Background()
	repo := store.NewMemoryRepository()
	dataset, _ := repo.CreateDataset(ctx, store.CreateDatasetParams{Name: "fixture"})
	file, _ := repo.CreateEventFile(ctx, store.CreateEventFileParams{
		DatasetID: dataset.ID,
		Path:      "../../testdata/sequence_gap.jsonl",
		Format:    "jsonl",
		Symbol:    "BTCUSDT",
	})
	job, _ := repo.CreateReplayJob(ctx, store.CreateReplayJobParams{
		DatasetID:   dataset.ID,
		EventFileID: file.ID,
		Symbol:      "BTCUSDT",
		Speed:       "max",
	})
	if err := repo.UpdateReplayCheckpoint(ctx, job.ID, 1); err != nil {
		t.Fatalf("UpdateReplayCheckpoint returned error: %v", err)
	}
	payload, _ := jobqueue.EncodeReplayPayload(jobqueue.ReplayPayload{JobID: job.ID})
	handler := NewReplayHandler(repo, zaptest.NewLogger(t), ReplayHandlerConfig{})

	if err := handler.ProcessTask(ctx, asynq.NewTask(jobqueue.TypeReplayJob, payload)); err != nil {
		t.Fatalf("ProcessTask returned error: %v", err)
	}
	got, err := repo.GetReplayJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("GetReplayJob returned error: %v", err)
	}
	if got.CheckpointLine != 4 {
		t.Fatalf("checkpoint line = %d, want final line 4", got.CheckpointLine)
	}
	metrics, err := repo.ListReplayMetrics(ctx, job.ID)
	if err != nil {
		t.Fatalf("ListReplayMetrics returned error: %v", err)
	}
	if len(metrics) != 1 {
		t.Fatalf("metrics length = %d, want 1", len(metrics))
	}
	if metrics[0].Rows != 4 {
		t.Fatalf("metric rows = %d, want final line 4", metrics[0].Rows)
	}
	if metrics[0].Events != 3 {
		t.Fatalf("metric events = %d, want resumed events after checkpoint", metrics[0].Events)
	}
	errs, err := repo.ListValidationErrors(ctx, job.ID)
	if err != nil {
		t.Fatalf("ListValidationErrors returned error: %v", err)
	}
	if len(errs) != 1 || errs[0].Line != 2 || errs[0].Type != "sequence_gap" {
		t.Fatalf("validation errors = %#v, want sequence gap at resumed line 2", errs)
	}
}

func TestReplayHandlerDoesNotDuplicateCheckpointedValidationFailures(t *testing.T) {
	ctx := context.Background()
	repo := store.NewMemoryRepository()
	dataset, _ := repo.CreateDataset(ctx, store.CreateDatasetParams{Name: "fixture"})
	file, _ := repo.CreateEventFile(ctx, store.CreateEventFileParams{
		DatasetID: dataset.ID,
		Path:      "../../testdata/sequence_gap.jsonl",
		Format:    "jsonl",
		Symbol:    "BTCUSDT",
	})
	job, _ := repo.CreateReplayJob(ctx, store.CreateReplayJobParams{
		DatasetID:   dataset.ID,
		EventFileID: file.ID,
		Symbol:      "BTCUSDT",
		Speed:       "max",
	})
	if err := repo.UpdateReplayCheckpoint(ctx, job.ID, 2); err != nil {
		t.Fatalf("UpdateReplayCheckpoint returned error: %v", err)
	}
	payload, _ := jobqueue.EncodeReplayPayload(jobqueue.ReplayPayload{JobID: job.ID})
	handler := NewReplayHandler(repo, zaptest.NewLogger(t), ReplayHandlerConfig{})

	if err := handler.ProcessTask(ctx, asynq.NewTask(jobqueue.TypeReplayJob, payload)); err != nil {
		t.Fatalf("ProcessTask returned error: %v", err)
	}
	metrics, err := repo.ListReplayMetrics(ctx, job.ID)
	if err != nil {
		t.Fatalf("ListReplayMetrics returned error: %v", err)
	}
	if len(metrics) != 1 || metrics[0].Events != 2 || metrics[0].SequenceGaps != 0 {
		t.Fatalf("metrics = %#v, want two resumed events and zero new gaps", metrics)
	}
	errs, err := repo.ListValidationErrors(ctx, job.ID)
	if err != nil {
		t.Fatalf("ListValidationErrors returned error: %v", err)
	}
	if len(errs) != 0 {
		t.Fatalf("validation errors = %#v, want no duplicate checkpointed failures", errs)
	}
}

func TestReplayHandlerSkipsCanceledJobBeforeExecution(t *testing.T) {
	ctx := context.Background()
	repo := store.NewMemoryRepository()
	dataset, _ := repo.CreateDataset(ctx, store.CreateDatasetParams{Name: "fixture"})
	file, _ := repo.CreateEventFile(ctx, store.CreateEventFileParams{
		DatasetID: dataset.ID,
		Path:      "../../testdata/btcusdt_depth.jsonl",
		Format:    "jsonl",
	})
	job, _ := repo.CreateReplayJob(ctx, store.CreateReplayJobParams{
		DatasetID:   dataset.ID,
		EventFileID: file.ID,
		Speed:       "max",
	})
	if _, err := repo.CancelReplayJob(ctx, job.ID); err != nil {
		t.Fatalf("CancelReplayJob returned error: %v", err)
	}
	payload, _ := jobqueue.EncodeReplayPayload(jobqueue.ReplayPayload{JobID: job.ID})
	handler := NewReplayHandler(repo, zaptest.NewLogger(t), ReplayHandlerConfig{})

	if err := handler.ProcessTask(ctx, asynq.NewTask(jobqueue.TypeReplayJob, payload)); err != nil {
		t.Fatalf("ProcessTask returned error: %v", err)
	}
	metrics, err := repo.ListReplayMetrics(ctx, job.ID)
	if err != nil {
		t.Fatalf("ListReplayMetrics returned error: %v", err)
	}
	if len(metrics) != 0 {
		t.Fatalf("metrics = %#v, want none for canceled job", metrics)
	}
}

func TestReplayHandlerMarksMissingFileFailed(t *testing.T) {
	ctx := context.Background()
	repo := store.NewMemoryRepository()
	dataset, _ := repo.CreateDataset(ctx, store.CreateDatasetParams{Name: "fixture"})
	file, _ := repo.CreateEventFile(ctx, store.CreateEventFileParams{
		DatasetID: dataset.ID,
		Path:      "../../testdata/does-not-exist.jsonl",
		Format:    "jsonl",
	})
	job, _ := repo.CreateReplayJob(ctx, store.CreateReplayJobParams{
		DatasetID:   dataset.ID,
		EventFileID: file.ID,
		Speed:       "max",
	})
	payload, _ := jobqueue.EncodeReplayPayload(jobqueue.ReplayPayload{JobID: job.ID})
	handler := NewReplayHandler(repo, zaptest.NewLogger(t), ReplayHandlerConfig{})

	err := handler.ProcessTask(ctx, asynq.NewTask(jobqueue.TypeReplayJob, payload))
	if err == nil {
		t.Fatal("ProcessTask returned nil error, want missing file error")
	}
	got, getErr := repo.GetReplayJob(ctx, job.ID)
	if getErr != nil {
		t.Fatalf("GetReplayJob returned error: %v", getErr)
	}
	if got.Status != store.JobStatusFailed {
		t.Fatalf("job status = %q, want failed", got.Status)
	}
	if got.LastError == "" {
		t.Fatal("LastError is empty, want failure message")
	}
}

func TestReplayHandlerInvalidPayloadSkipsRetry(t *testing.T) {
	handler := NewReplayHandler(store.NewMemoryRepository(), zaptest.NewLogger(t), ReplayHandlerConfig{})
	err := handler.ProcessTask(context.Background(), asynq.NewTask(jobqueue.TypeReplayJob, []byte(`{}`)))
	if !errors.Is(err, asynq.SkipRetry) {
		t.Fatalf("ProcessTask error = %v, want asynq.SkipRetry", err)
	}
}

func assertLabeledMetricValue(t *testing.T, families []*dto.MetricFamily, name string, labels map[string]string, want float64) {
	t.Helper()
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if !metricHasLabels(metric, labels) {
				continue
			}
			if metric.Counter != nil && metric.Counter.GetValue() == want {
				return
			}
			if metric.Gauge != nil && metric.Gauge.GetValue() == want {
				return
			}
		}
		t.Fatalf("metric %s did not contain labels %#v with value %v", name, labels, want)
	}
	t.Fatalf("metric %s was not gathered", name)
}

func metricHasLabels(metric *dto.Metric, want map[string]string) bool {
	got := make(map[string]string, len(metric.GetLabel()))
	for _, label := range metric.GetLabel() {
		got[label.GetName()] = label.GetValue()
	}
	for name, value := range want {
		if got[name] != value {
			return false
		}
	}
	return true
}
