package replay

import (
	"fmt"
	"io"
	"time"

	"github.com/orynwilder/market-replay-service/internal/event"
	"github.com/orynwilder/market-replay-service/internal/parser"
	"github.com/orynwilder/market-replay-service/internal/validate"
)

type SpeedMode string

const (
	SpeedMax SpeedMode = "max"
	Speed1x  SpeedMode = "1x"
	Speed10x SpeedMode = "10x"
)

type Summary struct {
	Rows            int64         `json:"rows"`
	Events          int64         `json:"events"`
	MalformedEvents int64         `json:"malformed_events"`
	SequenceGaps    int64         `json:"sequence_gaps"`
	Duration        time.Duration `json:"duration"`
}

type Runner struct {
	Sleep func(time.Duration)
}

func ParseSpeed(value string) (SpeedMode, error) {
	switch SpeedMode(value) {
	case SpeedMax, Speed1x, Speed10x:
		return SpeedMode(value), nil
	default:
		return "", fmt.Errorf("unsupported replay speed %q", value)
	}
}

func File(path string, format parser.Format, symbol string, speed SpeedMode) (Summary, error) {
	stream, err := parser.Open(path, format)
	if err != nil {
		return Summary{}, err
	}
	defer stream.Close()
	return NewRunner().Stream(stream, symbol, speed)
}

func NewRunner() Runner {
	return Runner{Sleep: time.Sleep}
}

func (r Runner) Stream(stream *parser.Stream, symbol string, speed SpeedMode) (Summary, error) {
	started := time.Now()
	validator := validate.NewValidator(validate.Options{Symbol: symbol})
	var previousEventTime int64

	for {
		record, err := stream.Next()
		if err == io.EOF {
			result := validator.Result()
			return Summary{
				Rows:            result.Rows,
				Events:          result.Events,
				MalformedEvents: result.MalformedEvents,
				SequenceGaps:    result.SequenceGaps,
				Duration:        time.Since(started),
			}, nil
		}
		if err != nil {
			return Summary{}, err
		}

		validator.Process(record)
		if record.Err != nil || (symbol != "" && record.Event.Symbol != symbol) {
			continue
		}
		r.sleepForEvent(record.Event, speed, &previousEventTime)
	}
}

func (r Runner) sleepForEvent(ev event.MarketEvent, speed SpeedMode, previousEventTime *int64) {
	eventTime := ev.EventTimeMillis()
	if speed == SpeedMax || eventTime <= 0 {
		*previousEventTime = eventTime
		return
	}
	if *previousEventTime == 0 {
		*previousEventTime = eventTime
		return
	}

	delta := eventTime - *previousEventTime
	*previousEventTime = eventTime
	if delta <= 0 {
		return
	}

	sleep := time.Duration(delta) * time.Millisecond
	if speed == Speed10x {
		sleep /= 10
	}
	if sleep > 0 && r.Sleep != nil {
		r.Sleep(sleep)
	}
}
