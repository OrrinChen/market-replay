package validate

import (
	"io"
	"time"

	"github.com/orynwilder/market-replay-service/internal/event"
	"github.com/orynwilder/market-replay-service/internal/parser"
)

type Options struct {
	Symbol string
}

type Validator struct {
	opts       Options
	result     event.ValidationResult
	lastDepth  map[string]int64
	lastTrade  map[string]int64
	lastTimeMS map[string]int64
}

func NewValidator(opts Options) *Validator {
	return &Validator{
		opts: opts,
		result: event.ValidationResult{
			SymbolEventCounts: make(map[string]int64),
			StartedAt:         time.Now().UTC(),
		},
		lastDepth:  make(map[string]int64),
		lastTrade:  make(map[string]int64),
		lastTimeMS: make(map[string]int64),
	}
}

func File(path string, format parser.Format, opts Options) (event.ValidationResult, error) {
	stream, err := parser.Open(path, format)
	if err != nil {
		return event.ValidationResult{}, err
	}
	defer stream.Close()
	return Stream(stream, opts)
}

func Reader(r io.Reader, format parser.Format, opts Options) (event.ValidationResult, error) {
	stream, err := parser.NewStream(r, format)
	if err != nil {
		return event.ValidationResult{}, err
	}
	return Stream(stream, opts)
}

func Stream(stream *parser.Stream, opts Options) (event.ValidationResult, error) {
	validator := NewValidator(opts)
	for {
		record, err := stream.Next()
		if err == io.EOF {
			return validator.Result(), nil
		}
		if err != nil {
			return event.ValidationResult{}, err
		}
		validator.Process(record)
	}
}

func (v *Validator) Process(record parser.Record) {
	v.result.Rows++
	if record.Err != nil {
		v.result.MalformedEvents++
		v.addFailure(record.Line, "", "malformed", record.Err.Error())
		return
	}

	ev := record.Event
	if v.opts.Symbol != "" && ev.Symbol != v.opts.Symbol {
		return
	}

	v.result.Events++
	v.result.SymbolEventCounts[ev.Symbol]++
	v.checkTime(ev)

	switch ev.Type {
	case event.EventTypeDepth:
		v.checkDepthSequence(ev)
	case event.EventTypeAggTrade:
		v.checkTradeSequence(ev)
	}
}

func (v *Validator) Prime(record parser.Record) {
	if record.Err != nil {
		return
	}

	ev := record.Event
	if v.opts.Symbol != "" && ev.Symbol != v.opts.Symbol {
		return
	}

	eventTime := ev.EventTimeMillis()
	if eventTime > v.lastTimeMS[ev.Symbol] {
		v.lastTimeMS[ev.Symbol] = eventTime
	}
	switch ev.Type {
	case event.EventTypeDepth:
		if ev.FinalUpdateID > v.lastDepth[ev.Symbol] {
			v.lastDepth[ev.Symbol] = ev.FinalUpdateID
		}
	case event.EventTypeAggTrade:
		if ev.TradeID > v.lastTrade[ev.Symbol] {
			v.lastTrade[ev.Symbol] = ev.TradeID
		}
	}
}

func (v *Validator) Result() event.ValidationResult {
	v.result.CompletedAt = time.Now().UTC()
	return v.result
}

func (v *Validator) checkTime(ev event.MarketEvent) {
	eventTime := ev.EventTimeMillis()
	last, ok := v.lastTimeMS[ev.Symbol]
	if ok && eventTime < last {
		v.result.OrderingFailures++
		v.addFailure(ev.Line, ev.Symbol, "ordering", "event_time moved backwards for symbol")
	}
	if !ok || eventTime > last {
		v.lastTimeMS[ev.Symbol] = eventTime
	}
}

func (v *Validator) checkDepthSequence(ev event.MarketEvent) {
	last, ok := v.lastDepth[ev.Symbol]
	if ok && ev.FirstUpdateID != last+1 {
		v.result.SequenceGaps++
		v.addFailure(ev.Line, ev.Symbol, "sequence_gap", "depth first_update_id does not continue after prior final_update_id")
	}
	if ok && ev.FinalUpdateID <= last {
		v.result.OrderingFailures++
		v.addFailure(ev.Line, ev.Symbol, "ordering", "depth final_update_id did not increase")
	}
	if !ok || ev.FinalUpdateID > last {
		v.lastDepth[ev.Symbol] = ev.FinalUpdateID
	}
}

func (v *Validator) checkTradeSequence(ev event.MarketEvent) {
	last, ok := v.lastTrade[ev.Symbol]
	if ok && ev.TradeID != last+1 {
		v.result.SequenceGaps++
		v.addFailure(ev.Line, ev.Symbol, "sequence_gap", "trade_id is not contiguous")
	}
	if ok && ev.TradeID <= last {
		v.result.OrderingFailures++
		v.addFailure(ev.Line, ev.Symbol, "ordering", "trade_id did not increase")
	}
	if !ok || ev.TradeID > last {
		v.lastTrade[ev.Symbol] = ev.TradeID
	}
}

func (v *Validator) addFailure(line int64, symbol, failureType, message string) {
	v.result.Failures = append(v.result.Failures, event.ValidationFailure{
		Line:    line,
		Symbol:  symbol,
		Type:    failureType,
		Message: message,
	})
}
