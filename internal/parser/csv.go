package parser

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/orynwilder/market-replay-service/internal/event"
)

func parseCSV(fields []string, header map[string]int, lineNo int64) (event.MarketEvent, error) {
	rawType := strings.ToLower(csvValue(fields, header, "type"))
	if rawType == "" {
		rawType = csvValue(fields, header, "event_type")
	}
	eventType := normalizeEventType(rawType)
	ev := event.MarketEvent{
		Type:     eventType,
		Symbol:   strings.ToUpper(csvValue(fields, header, "symbol")),
		Price:    csvValue(fields, header, "price"),
		Quantity: csvValue(fields, header, "quantity"),
		Line:     lineNo,
	}

	eventTimeKey := "event_time"
	if csvValue(fields, header, eventTimeKey) == "" {
		eventTimeKey = "trade_time"
	}
	eventTime, err := csvInt(fields, header, eventTimeKey)
	if err != nil {
		return event.MarketEvent{}, err
	}

	switch ev.Type {
	case event.EventTypeDepth:
		ev.EventTimeMS = eventTime
		finalKey := "final_update_id"
		if csvValue(fields, header, finalKey) == "" {
			finalKey = "sequence"
		}
		seq, err := csvInt(fields, header, finalKey)
		if err != nil {
			return event.MarketEvent{}, err
		}
		firstKey := "first_update_id"
		if csvValue(fields, header, firstKey) == "" {
			firstKey = "prev_sequence"
		}
		firstSeq, err := csvInt(fields, header, firstKey)
		if err != nil {
			return event.MarketEvent{}, err
		}
		if firstKey == "prev_sequence" {
			firstSeq++
		}
		ev.FirstUpdateID = firstSeq
		ev.FinalUpdateID = seq
		ev.Bids = []event.PriceLevel{{Price: ev.Price, Quantity: ev.Quantity}}
	case event.EventTypeAggTrade:
		ev.TradeTimeMS = eventTime
		tradeID, err := csvInt(fields, header, "trade_id")
		if err != nil {
			return event.MarketEvent{}, err
		}
		ev.TradeID = tradeID
		isBuyerMaker, err := strconv.ParseBool(csvValue(fields, header, "is_buyer_maker"))
		if err != nil {
			return event.MarketEvent{}, fmt.Errorf("parse is_buyer_maker: %w", err)
		}
		ev.IsBuyerMaker = &isBuyerMaker
	default:
		return event.MarketEvent{}, fmt.Errorf("unknown event type %q", ev.Type)
	}

	if err := validateEventShape(ev, ev.FirstUpdateID != 0, ev.Type == event.EventTypeDepth, ev.TradeID != 0); err != nil {
		return event.MarketEvent{}, err
	}
	return ev, nil
}

func normalizeEventType(raw string) event.EventType {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "depth":
		return event.EventTypeDepth
	case "aggtrade", "trade":
		return event.EventTypeAggTrade
	default:
		return event.EventType(raw)
	}
}
