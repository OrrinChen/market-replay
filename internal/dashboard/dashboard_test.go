package dashboard

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/orynwilder/market-replay-service/internal/store"
)

func TestDashboardRendersCorePagesFromMemoryStore(t *testing.T) {
	ctx := context.Background()
	repo := store.NewMemoryRepository()
	job := seedDashboardRepository(t, ctx, repo)

	handler := New(repo)
	tests := []struct {
		path string
		want []string
	}{
		{path: "/dashboard/datasets", want: []string{"Datasets", "BTC depth fixture", "BTCUSDT"}},
		{path: "/dashboard/jobs", want: []string{"Replay Jobs", job.ID, "completed"}},
		{path: "/dashboard/jobs/" + job.ID, want: []string{"Job Detail", "checkpoint", "sequence_gap"}},
		{path: "/dashboard/validation-errors", want: []string{"Validation Errors", "sequence_gap", "gap between update ids"}},
		{path: "/dashboard/metrics", want: []string{"Metrics Summary", "rows/sec", "events/sec", "1000.00"}},
		{path: "/dashboard/benchmark", want: []string{"Benchmark Report", "p95 latency", "allocs/event"}},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
			}
			body := rec.Body.String()
			for _, want := range tt.want {
				if !strings.Contains(body, want) {
					t.Fatalf("body missing %q:\n%s", want, body)
				}
			}
		})
	}
}

func TestDashboardReturnsNotFoundForMissingJob(t *testing.T) {
	handler := New(store.NewMemoryRepository())
	req := httptest.NewRequest(http.MethodGet, "/dashboard/jobs/missing", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func seedDashboardRepository(t *testing.T, ctx context.Context, repo *store.MemoryRepository) store.ReplayJob {
	t.Helper()

	dataset, err := repo.CreateDataset(ctx, store.CreateDatasetParams{
		Name:        "BTC depth fixture",
		Description: "Small deterministic replay fixture",
	})
	if err != nil {
		t.Fatalf("CreateDataset returned error: %v", err)
	}
	file, err := repo.CreateEventFile(ctx, store.CreateEventFileParams{
		DatasetID: dataset.ID,
		Path:      "testdata/btcusdt_depth.jsonl",
		Format:    "jsonl",
		Symbol:    "BTCUSDT",
		Bytes:     4096,
	})
	if err != nil {
		t.Fatalf("CreateEventFile returned error: %v", err)
	}
	job, err := repo.CreateReplayJob(ctx, store.CreateReplayJobParams{
		DatasetID:      dataset.ID,
		EventFileID:    file.ID,
		IdempotencyKey: "dashboard-test",
		Symbol:         "BTCUSDT",
		Speed:          "max",
	})
	if err != nil {
		t.Fatalf("CreateReplayJob returned error: %v", err)
	}
	if err := repo.UpdateReplayCheckpoint(ctx, job.ID, 128); err != nil {
		t.Fatalf("UpdateReplayCheckpoint returned error: %v", err)
	}
	completed, err := repo.CompleteReplayJob(ctx, job.ID, store.CompleteReplayJobParams{
		Metric: store.ReplayMetric{
			Rows:            100,
			Events:          100,
			MalformedEvents: 0,
			SequenceGaps:    1,
			Duration:        100 * time.Millisecond,
			RowsPerSecond:   1000,
			EventsPerSecond: 1000,
			P95Latency:      2 * time.Millisecond,
			PeakAllocBytes:  4 * 1024 * 1024,
			AllocsPerEvent:  3.5,
		},
		Errors: []store.ValidationError{{
			Line:    42,
			Symbol:  "BTCUSDT",
			Type:    "sequence_gap",
			Message: "gap between update ids",
		}},
	})
	if err != nil {
		t.Fatalf("CompleteReplayJob returned error: %v", err)
	}
	return completed
}
