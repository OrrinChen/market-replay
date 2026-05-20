package bench

import (
	"io"
	"runtime"
	"sort"
	"time"

	"github.com/orynwilder/market-replay-service/internal/event"
	"github.com/orynwilder/market-replay-service/internal/parser"
	"github.com/orynwilder/market-replay-service/internal/validate"
)

func File(path string, format parser.Format, symbol string) (event.ReplayMetric, error) {
	stream, err := parser.Open(path, format)
	if err != nil {
		return event.ReplayMetric{}, err
	}
	defer stream.Close()

	metric, err := Stream(stream, symbol)
	if err != nil {
		return event.ReplayMetric{}, err
	}
	metric.WorkloadFilePath = path
	return metric, nil
}

func Stream(stream *parser.Stream, symbol string) (event.ReplayMetric, error) {
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	peakAlloc := before.Alloc

	started := time.Now()
	validator := validate.NewValidator(validate.Options{Symbol: symbol})
	latencies := make([]time.Duration, 0, 4096)
	rows := int64(0)
	for {
		rowStarted := time.Now()
		record, err := stream.Next()
		if err == io.EOF {
			result := validator.Result()
			duration := time.Since(started)
			if duration <= 0 {
				duration = time.Nanosecond
			}

			var after runtime.MemStats
			runtime.ReadMemStats(&after)
			if after.Alloc > peakAlloc {
				peakAlloc = after.Alloc
			}

			events := result.Events
			allocsPerEvent := 0.0
			if events > 0 && after.Mallocs >= before.Mallocs {
				allocsPerEvent = float64(after.Mallocs-before.Mallocs) / float64(events)
			}

			return event.ReplayMetric{
				Rows:            result.Rows,
				Events:          events,
				MalformedEvents: result.MalformedEvents,
				SequenceGaps:    result.SequenceGaps,
				Duration:        duration,
				RowsPerSecond:   float64(result.Rows) / duration.Seconds(),
				EventsPerSecond: float64(events) / duration.Seconds(),
				P95Latency:      percentile(latencies, 0.95),
				PeakAllocBytes:  peakAlloc,
				AllocsPerEvent:  allocsPerEvent,
				ProcessedAt:     time.Now().UTC(),
			}, nil
		}
		if err != nil {
			return event.ReplayMetric{}, err
		}

		validator.Process(record)
		rows++
		if record.Err == nil && (symbol == "" || record.Event.Symbol == symbol) && len(latencies) < cap(latencies) {
			latencies = append(latencies, time.Since(rowStarted))
		}
		if rows%2048 == 0 {
			var current runtime.MemStats
			runtime.ReadMemStats(&current)
			if current.Alloc > peakAlloc {
				peakAlloc = current.Alloc
			}
		}
	}
}

func percentile(values []time.Duration, pct float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	cp := append([]time.Duration(nil), values...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	index := int(float64(len(cp)-1) * pct)
	return cp[index]
}
