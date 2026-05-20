package observability

import (
	"time"

	"github.com/orynwilder/market-replay-service/internal/event"
	"github.com/prometheus/client_golang/prometheus"
)

const unknownLabel = "unknown"

type ReplayLabels struct {
	Dataset string
	Symbol  string
	Status  string
}

type Metrics struct {
	replayEvents          *prometheus.CounterVec
	replayEventsPerSecond *prometheus.GaugeVec
	validationFailures    *prometheus.CounterVec
	sequenceGaps          *prometheus.CounterVec
	jobDuration           *prometheus.HistogramVec
	jobQueueLag           *prometheus.HistogramVec
	workerActive          *prometheus.GaugeVec
	workerRetry           *prometheus.CounterVec
	workerDLQ             *prometheus.CounterVec
	apiRequestDuration    *prometheus.HistogramVec
}

func Register(registry *prometheus.Registry) (*Metrics, error) {
	metrics := NewMetrics()
	collectors := []prometheus.Collector{
		metrics.replayEvents,
		metrics.replayEventsPerSecond,
		metrics.validationFailures,
		metrics.sequenceGaps,
		metrics.jobDuration,
		metrics.jobQueueLag,
		metrics.workerActive,
		metrics.workerRetry,
		metrics.workerDLQ,
		metrics.apiRequestDuration,
	}
	for _, collector := range collectors {
		if err := registry.Register(collector); err != nil {
			return nil, err
		}
	}
	return metrics, nil
}

func MustRegister(registry *prometheus.Registry) *Metrics {
	metrics, err := Register(registry)
	if err != nil {
		panic(err)
	}
	return metrics
}

func NewMetrics() *Metrics {
	metrics := &Metrics{
		replayEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "replay_events_total",
			Help: "Total replay events processed.",
		}, []string{"dataset", "symbol", "status"}),
		replayEventsPerSecond: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "replay_events_per_second",
			Help: "Last observed replay event throughput.",
		}, []string{"dataset", "symbol"}),
		validationFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "replay_validation_failures_total",
			Help: "Total replay validation failures by type.",
		}, []string{"dataset", "symbol", "type"}),
		sequenceGaps: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "replay_sequence_gaps_total",
			Help: "Total replay sequence gaps detected.",
		}, []string{"dataset", "symbol"}),
		jobDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "replay_job_duration_seconds",
			Help:    "Replay job duration in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"dataset", "symbol", "status"}),
		jobQueueLag: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "replay_job_queue_lag_seconds",
			Help:    "Replay job queue lag in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"queue"}),
		workerActive: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "worker_active_count",
			Help: "Current active worker count.",
		}, []string{"worker"}),
		workerRetry: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "worker_retry_total",
			Help: "Total worker retry attempts.",
		}, []string{"queue", "worker"}),
		workerDLQ: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "worker_dlq_total",
			Help: "Total worker dead-letter queue events.",
		}, []string{"queue", "reason"}),
		apiRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "api_request_duration_seconds",
			Help:    "API request latency in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "route", "status"}),
	}
	metrics.initMetricFamilies()
	return metrics
}

func (m *Metrics) initMetricFamilies() {
	m.replayEvents.WithLabelValues(unknownLabel, unknownLabel, unknownLabel).Add(0)
	m.replayEventsPerSecond.WithLabelValues(unknownLabel, unknownLabel).Set(0)
	m.validationFailures.WithLabelValues(unknownLabel, unknownLabel, unknownLabel).Add(0)
	m.sequenceGaps.WithLabelValues(unknownLabel, unknownLabel).Add(0)
	m.jobDuration.WithLabelValues(unknownLabel, unknownLabel, unknownLabel)
	m.jobQueueLag.WithLabelValues(unknownLabel)
	m.workerActive.WithLabelValues(unknownLabel).Set(0)
	m.workerRetry.WithLabelValues(unknownLabel, unknownLabel).Add(0)
	m.workerDLQ.WithLabelValues(unknownLabel, unknownLabel).Add(0)
	m.apiRequestDuration.WithLabelValues(unknownLabel, unknownLabel, unknownLabel)
}

func (m *Metrics) RecordReplayBenchmark(labels ReplayLabels, metric event.ReplayMetric) {
	replayLabels := normalizeReplayLabels(labels)
	m.replayEvents.WithLabelValues(replayLabels.Dataset, replayLabels.Symbol, replayLabels.Status).Add(float64(metric.Events))
	m.replayEventsPerSecond.WithLabelValues(replayLabels.Dataset, replayLabels.Symbol).Set(metric.EventsPerSecond)
	if metric.MalformedEvents > 0 {
		m.validationFailures.WithLabelValues(replayLabels.Dataset, replayLabels.Symbol, "malformed").Add(float64(metric.MalformedEvents))
	}
	if metric.SequenceGaps > 0 {
		m.validationFailures.WithLabelValues(replayLabels.Dataset, replayLabels.Symbol, "sequence_gap").Add(float64(metric.SequenceGaps))
		m.sequenceGaps.WithLabelValues(replayLabels.Dataset, replayLabels.Symbol).Add(float64(metric.SequenceGaps))
	}
	if metric.Duration > 0 {
		m.jobDuration.WithLabelValues(replayLabels.Dataset, replayLabels.Symbol, replayLabels.Status).Observe(metric.Duration.Seconds())
	}
}

func (m *Metrics) RecordValidationFailure(dataset, symbol, failureType string, count int64) {
	if count <= 0 {
		return
	}
	m.validationFailures.WithLabelValues(labelOrUnknown(dataset), labelOrUnknown(symbol), labelOrUnknown(failureType)).Add(float64(count))
}

func (m *Metrics) RecordSequenceGap(dataset, symbol string, count int64) {
	if count <= 0 {
		return
	}
	m.sequenceGaps.WithLabelValues(labelOrUnknown(dataset), labelOrUnknown(symbol)).Add(float64(count))
}

func (m *Metrics) RecordJobQueueLag(queue string, lag time.Duration) {
	if lag < 0 {
		return
	}
	m.jobQueueLag.WithLabelValues(labelOrUnknown(queue)).Observe(lag.Seconds())
}

func (m *Metrics) SetActiveWorkers(worker string, count float64) {
	m.workerActive.WithLabelValues(labelOrUnknown(worker)).Set(count)
}

func (m *Metrics) RecordWorkerRetry(queue, worker string) {
	m.workerRetry.WithLabelValues(labelOrUnknown(queue), labelOrUnknown(worker)).Inc()
}

func (m *Metrics) RecordWorkerDLQ(queue, reason string) {
	m.workerDLQ.WithLabelValues(labelOrUnknown(queue), labelOrUnknown(reason)).Inc()
}

func normalizeReplayLabels(labels ReplayLabels) ReplayLabels {
	return ReplayLabels{
		Dataset: labelOrUnknown(labels.Dataset),
		Symbol:  labelOrUnknown(labels.Symbol),
		Status:  labelOrUnknown(labels.Status),
	}
}

func labelOrUnknown(value string) string {
	if value == "" {
		return unknownLabel
	}
	return value
}
