package parser

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/orynwilder/market-replay-service/internal/event"
)

type jsonEvent struct {
	Type          event.EventType    `json:"event_type"`
	LegacyType    event.EventType    `json:"type"`
	Symbol        string             `json:"symbol"`
	EventTimeMS   int64              `json:"event_time"`
	TradeTimeMS   int64              `json:"trade_time"`
	FirstUpdateID *int64             `json:"first_update_id,omitempty"`
	FinalUpdateID *int64             `json:"final_update_id,omitempty"`
	LegacySeq     *int64             `json:"sequence,omitempty"`
	LegacyPrevSeq *int64             `json:"prev_sequence,omitempty"`
	TradeID       *int64             `json:"trade_id,omitempty"`
	Price         string             `json:"price,omitempty"`
	Quantity      string             `json:"quantity,omitempty"`
	IsBuyerMaker  *bool              `json:"is_buyer_maker,omitempty"`
	Bids          []event.PriceLevel `json:"bids,omitempty"`
	Asks          []event.PriceLevel `json:"asks,omitempty"`
}

func ParseJSONLine(line []byte, lineNo int64) (event.MarketEvent, error) {
	if len(strings.TrimSpace(string(line))) == 0 {
		return event.MarketEvent{}, fmt.Errorf("empty line")
	}

	var raw jsonEvent
	if err := json.Unmarshal(line, &raw); err != nil {
		return event.MarketEvent{}, fmt.Errorf("decode json event: %w", err)
	}
	if raw.Type == "" {
		raw.Type = raw.LegacyType
	}
	raw.Type = normalizeEventType(string(raw.Type))

	ev := event.MarketEvent{
		Type:         raw.Type,
		Symbol:       strings.ToUpper(strings.TrimSpace(raw.Symbol)),
		EventTimeMS:  raw.EventTimeMS,
		TradeTimeMS:  raw.TradeTimeMS,
		Price:        raw.Price,
		Quantity:     raw.Quantity,
		IsBuyerMaker: raw.IsBuyerMaker,
		Bids:         raw.Bids,
		Asks:         raw.Asks,
		Line:         lineNo,
	}
	if raw.FirstUpdateID != nil {
		ev.FirstUpdateID = *raw.FirstUpdateID
	}
	if raw.FinalUpdateID != nil {
		ev.FinalUpdateID = *raw.FinalUpdateID
	}
	if raw.LegacyPrevSeq != nil {
		ev.FirstUpdateID = *raw.LegacyPrevSeq + 1
	}
	if raw.LegacySeq != nil {
		ev.FinalUpdateID = *raw.LegacySeq
	}
	if raw.TradeID != nil {
		ev.TradeID = *raw.TradeID
	}

	if err := validateEventShape(ev, raw.FirstUpdateID != nil || raw.LegacyPrevSeq != nil, raw.FinalUpdateID != nil || raw.LegacySeq != nil, raw.TradeID != nil); err != nil {
		return event.MarketEvent{}, err
	}
	return ev, nil
}

func validateEventShape(ev event.MarketEvent, hasSequence, hasPrevSequence, hasTradeID bool) error {
	if ev.Type != event.EventTypeDepth && ev.Type != event.EventTypeAggTrade {
		return fmt.Errorf("unknown event type %q", ev.Type)
	}
	if ev.Symbol == "" {
		return fmt.Errorf("missing symbol")
	}

	switch ev.Type {
	case event.EventTypeDepth:
		if ev.EventTimeMS <= 0 {
			return fmt.Errorf("event_time must be positive unix milliseconds")
		}
		if !hasSequence || !hasPrevSequence {
			return fmt.Errorf("depth event requires first_update_id and final_update_id")
		}
		if ev.FinalUpdateID < ev.FirstUpdateID {
			return fmt.Errorf("final_update_id must be greater than or equal to first_update_id")
		}
		if len(ev.Bids) == 0 && len(ev.Asks) == 0 {
			return fmt.Errorf("depth event requires at least one bid or ask level")
		}
	case event.EventTypeAggTrade:
		if ev.TradeTimeMS <= 0 {
			return fmt.Errorf("trade_time must be positive unix milliseconds")
		}
		if !hasTradeID {
			return fmt.Errorf("trade event requires trade_id")
		}
		if ev.TradeID <= 0 {
			return fmt.Errorf("trade_id must be positive")
		}
		if ev.Price == "" || ev.Quantity == "" {
			return fmt.Errorf("trade event requires price and quantity")
		}
		if ev.IsBuyerMaker == nil {
			return fmt.Errorf("trade event requires is_buyer_maker")
		}
	}
	return nil
}
