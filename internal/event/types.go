package event

import (
	"encoding/json"
	"fmt"
	"time"
)

type EventType string

const (
	EventTypeDepth    EventType = "depth"
	EventTypeAggTrade EventType = "aggTrade"
)

type PriceLevel struct {
	Price    string `json:"price"`
	Quantity string `json:"quantity"`
}

func (p *PriceLevel) UnmarshalJSON(data []byte) error {
	var pair []string
	if err := json.Unmarshal(data, &pair); err == nil {
		if len(pair) != 2 {
			return fmt.Errorf("price level array must contain price and quantity")
		}
		p.Price = pair[0]
		p.Quantity = pair[1]
		return nil
	}

	var obj struct {
		Price    string `json:"price"`
		Quantity string `json:"quantity"`
	}
	if err := json.Unmarshal(data, &obj); err != nil {
		return err
	}
	if obj.Price == "" || obj.Quantity == "" {
		return fmt.Errorf("price level requires price and quantity")
	}
	p.Price = obj.Price
	p.Quantity = obj.Quantity
	return nil
}

type MarketEvent struct {
	Type          EventType    `json:"event_type"`
	Symbol        string       `json:"symbol"`
	EventTimeMS   int64        `json:"event_time,omitempty"`
	TradeTimeMS   int64        `json:"trade_time,omitempty"`
	FirstUpdateID int64        `json:"first_update_id,omitempty"`
	FinalUpdateID int64        `json:"final_update_id,omitempty"`
	TradeID       int64        `json:"trade_id,omitempty"`
	Price         string       `json:"price,omitempty"`
	Quantity      string       `json:"quantity,omitempty"`
	IsBuyerMaker  *bool        `json:"is_buyer_maker,omitempty"`
	Bids          []PriceLevel `json:"bids,omitempty"`
	Asks          []PriceLevel `json:"asks,omitempty"`
	Line          int64        `json:"line,omitempty"`
}

func (e MarketEvent) EventTime() time.Time {
	if e.EventTimeMS > 0 {
		return time.UnixMilli(e.EventTimeMS).UTC()
	}
	return time.UnixMilli(e.TradeTimeMS).UTC()
}

func (e MarketEvent) EventTimeMillis() int64 {
	if e.EventTimeMS > 0 {
		return e.EventTimeMS
	}
	return e.TradeTimeMS
}

type ReplayStatus string

const (
	ReplayStatusPending   ReplayStatus = "pending"
	ReplayStatusRunning   ReplayStatus = "running"
	ReplayStatusCompleted ReplayStatus = "completed"
	ReplayStatusFailed    ReplayStatus = "failed"
)

type ReplayJob struct {
	ID          string       `json:"id"`
	DatasetID   string       `json:"dataset_id"`
	Symbol      string       `json:"symbol"`
	FilePath    string       `json:"file_path"`
	Speed       string       `json:"speed"`
	Status      ReplayStatus `json:"status"`
	SubmittedAt time.Time    `json:"submitted_at"`
	StartedAt   *time.Time   `json:"started_at,omitempty"`
	CompletedAt *time.Time   `json:"completed_at,omitempty"`
}

type ValidationFailure struct {
	Line    int64  `json:"line"`
	Symbol  string `json:"symbol,omitempty"`
	Type    string `json:"type"`
	Message string `json:"message"`
}

type ValidationResult struct {
	Rows              int64               `json:"rows"`
	Events            int64               `json:"events"`
	MalformedEvents   int64               `json:"malformed_events"`
	SequenceGaps      int64               `json:"sequence_gaps"`
	OrderingFailures  int64               `json:"ordering_failures"`
	SymbolEventCounts map[string]int64    `json:"symbol_event_counts"`
	Failures          []ValidationFailure `json:"failures"`
	StartedAt         time.Time           `json:"started_at"`
	CompletedAt       time.Time           `json:"completed_at"`
}

type ReplayMetric struct {
	Rows             int64         `json:"rows"`
	Events           int64         `json:"events"`
	MalformedEvents  int64         `json:"malformed_events"`
	SequenceGaps     int64         `json:"sequence_gaps"`
	Duration         time.Duration `json:"duration"`
	RowsPerSecond    float64       `json:"rows_per_second"`
	EventsPerSecond  float64       `json:"events_per_second"`
	P95Latency       time.Duration `json:"p95_latency"`
	PeakAllocBytes   uint64        `json:"peak_alloc_bytes"`
	AllocsPerEvent   float64       `json:"allocs_per_event"`
	ProcessedAt      time.Time     `json:"processed_at"`
	WorkloadFilePath string        `json:"workload_file_path"`
}
