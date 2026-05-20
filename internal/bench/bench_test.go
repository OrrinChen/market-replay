package bench

import (
	"path/filepath"
	"testing"

	"github.com/orynwilder/market-replay-service/internal/parser"
)

func TestFileReportsBenchmarkMetricFields(t *testing.T) {
	metric, err := File(filepath.Join("..", "..", "testdata", "btcusdt_depth.jsonl"), parser.FormatJSONL, "")
	if err != nil {
		t.Fatalf("File returned error: %v", err)
	}

	if metric.Rows != 4 || metric.Events != 4 {
		t.Fatalf("rows/events = %d/%d, want 4/4", metric.Rows, metric.Events)
	}
	if metric.RowsPerSecond <= 0 || metric.EventsPerSecond <= 0 {
		t.Fatalf("throughput metrics should be positive: %#v", metric)
	}
	if metric.WorkloadFilePath == "" {
		t.Fatal("WorkloadFilePath was empty")
	}
}
