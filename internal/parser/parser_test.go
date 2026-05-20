package parser

import (
	"io"
	"strings"
	"testing"

	"github.com/orynwilder/market-replay-service/internal/event"
)

func TestParseJSONLineDepthEvent(t *testing.T) {
	line := []byte(`{"event_type":"depth","symbol":"btcusdt","event_time":1710000000000,"first_update_id":1001,"final_update_id":1002,"bids":[["68250.10","0.500"]],"asks":[["68251.00","0.250"]]}`)

	ev, err := ParseJSONLine(line, 7)
	if err != nil {
		t.Fatalf("ParseJSONLine returned error: %v", err)
	}

	if ev.Type != event.EventTypeDepth {
		t.Fatalf("event type = %q, want %q", ev.Type, event.EventTypeDepth)
	}
	if ev.Symbol != "BTCUSDT" {
		t.Fatalf("symbol = %q, want BTCUSDT", ev.Symbol)
	}
	if ev.FirstUpdateID != 1001 || ev.FinalUpdateID != 1002 {
		t.Fatalf("update ids = %d/%d, want 1001/1002", ev.FirstUpdateID, ev.FinalUpdateID)
	}
	if len(ev.Bids) != 1 || ev.Bids[0].Price != "68250.10" {
		t.Fatalf("bids not decoded correctly: %#v", ev.Bids)
	}
	if ev.Line != 7 {
		t.Fatalf("line = %d, want 7", ev.Line)
	}
}

func TestParseJSONLineRejectsMalformedDepth(t *testing.T) {
	line := []byte(`{"event_type":"depth","symbol":"BTCUSDT","event_time":1710000000000,"first_update_id":1002,"final_update_id":1001,"bids":[["1","1"]]}`)

	if _, err := ParseJSONLine(line, 1); err == nil {
		t.Fatal("ParseJSONLine succeeded for final_update_id below first_update_id")
	}
}

func TestCSVStreamParsesAggTradeRows(t *testing.T) {
	input := strings.NewReader("event_type,symbol,trade_id,price,quantity,trade_time,is_buyer_maker\naggTrade,SOLUSDT,9001,148.1200,12.50,1710000001000,true\n")

	stream, err := NewStream(input, FormatCSV)
	if err != nil {
		t.Fatalf("NewStream returned error: %v", err)
	}

	record, err := stream.Next()
	if err != nil {
		t.Fatalf("Next returned error: %v", err)
	}
	if record.Err != nil {
		t.Fatalf("record error: %v", record.Err)
	}
	if record.Event.Type != event.EventTypeAggTrade {
		t.Fatalf("event type = %q, want %q", record.Event.Type, event.EventTypeAggTrade)
	}
	if record.Event.TradeID != 9001 || record.Event.TradeTimeMS != 1710000001000 {
		t.Fatalf("trade fields not decoded: %#v", record.Event)
	}
	if record.Event.IsBuyerMaker == nil || !*record.Event.IsBuyerMaker {
		t.Fatalf("is_buyer_maker not decoded: %#v", record.Event.IsBuyerMaker)
	}

	if _, err := stream.Next(); err != io.EOF {
		t.Fatalf("second Next error = %v, want EOF", err)
	}
}
