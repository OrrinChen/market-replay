package observability

import (
	"testing"
	"time"

	"github.com/orynwilder/market-replay-service/internal/event"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestRegisterMetricsExposesPhase4Collectors(t *testing.T) {
	registry := prometheus.NewRegistry()

	if _, err := Register(registry); err != nil {
		t.Fatalf("register metrics: %v", err)
	}

	want := map[string]bool{
		"replay_events_total":              false,
		"replay_events_per_second":         false,
		"replay_validation_failures_total": false,
		"replay_sequence_gaps_total":       false,
		"replay_job_duration_seconds":      false,
		"replay_job_queue_lag_seconds":     false,
		"worker_active_count":              false,
		"worker_retry_total":               false,
		"worker_dlq_total":                 false,
		"api_request_duration_seconds":     false,
	}

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, family := range families {
		if _, ok := want[family.GetName()]; ok {
			want[family.GetName()] = true
		}
	}
	for name, seen := range want {
		if !seen {
			t.Fatalf("metric %s was not registered", name)
		}
	}
}

func TestRecordReplayBenchmarkUpdatesCountersGaugesAndHistograms(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := Register(registry)
	if err != nil {
		t.Fatalf("register metrics: %v", err)
	}

	metrics.RecordReplayBenchmark(ReplayLabels{
		Dataset: "fixture-a",
		Symbol:  "BTCUSDT",
		Status:  "completed",
	}, event.ReplayMetric{
		Events:          7,
		MalformedEvents: 2,
		SequenceGaps:    1,
		Duration:        250 * time.Millisecond,
		EventsPerSecond: 28,
	})

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}

	assertMetricValue(t, families, "replay_events_total", 7)
	assertMetricValue(t, families, "replay_events_per_second", 28)
	assertMetricValue(t, families, "replay_validation_failures_total", 2)
	assertMetricValue(t, families, "replay_sequence_gaps_total", 1)
	assertHistogramCount(t, families, "replay_job_duration_seconds", 1)
}

func assertMetricValue(t *testing.T, families []*dto.MetricFamily, name string, want float64) {
	t.Helper()
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if metric.Counter != nil && metric.Counter.GetValue() == want {
				return
			}
			if metric.Gauge != nil && metric.Gauge.GetValue() == want {
				return
			}
		}
		t.Fatalf("metric %s did not contain value %v", name, want)
	}
	t.Fatalf("metric %s was not gathered", name)
}

func assertHistogramCount(t *testing.T, families []*dto.MetricFamily, name string, want uint64) {
	t.Helper()
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if metric.Histogram != nil && metric.Histogram.GetSampleCount() == want {
				return
			}
		}
		t.Fatalf("histogram %s did not contain count %d", name, want)
	}
	t.Fatalf("histogram %s was not gathered", name)
}
