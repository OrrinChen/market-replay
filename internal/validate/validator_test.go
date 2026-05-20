package validate

import (
	"path/filepath"
	"testing"

	"github.com/orynwilder/market-replay-service/internal/parser"
)

func fixturePath(name string) string {
	return filepath.Join("..", "..", "testdata", name)
}

func TestValidDepthFixtureHasNoFailures(t *testing.T) {
	result, err := File(fixturePath("btcusdt_depth.jsonl"), parser.FormatJSONL, Options{})
	if err != nil {
		t.Fatalf("File returned error: %v", err)
	}

	if result.Rows != 4 || result.Events != 4 {
		t.Fatalf("rows/events = %d/%d, want 4/4", result.Rows, result.Events)
	}
	if result.MalformedEvents != 0 || result.SequenceGaps != 0 || result.OrderingFailures != 0 {
		t.Fatalf("unexpected validation failures: %#v", result)
	}
	if result.SymbolEventCounts["BTCUSDT"] != 4 {
		t.Fatalf("BTCUSDT count = %d, want 4", result.SymbolEventCounts["BTCUSDT"])
	}
}

func TestSequenceGapFixtureDetectsExactlyOneGap(t *testing.T) {
	result, err := File(fixturePath("sequence_gap.jsonl"), parser.FormatJSONL, Options{})
	if err != nil {
		t.Fatalf("File returned error: %v", err)
	}

	if result.Rows != 4 || result.Events != 4 {
		t.Fatalf("rows/events = %d/%d, want 4/4", result.Rows, result.Events)
	}
	if result.MalformedEvents != 0 {
		t.Fatalf("malformed = %d, want 0", result.MalformedEvents)
	}
	if result.SequenceGaps != 1 {
		t.Fatalf("sequence gaps = %d, want 1; failures=%#v", result.SequenceGaps, result.Failures)
	}
}

func TestMalformedFixtureCountsBadRowsWithoutStopping(t *testing.T) {
	result, err := File(fixturePath("malformed.jsonl"), parser.FormatJSONL, Options{})
	if err != nil {
		t.Fatalf("File returned error: %v", err)
	}

	if result.Rows != 5 {
		t.Fatalf("rows = %d, want 5", result.Rows)
	}
	if result.Events != 1 {
		t.Fatalf("events = %d, want 1", result.Events)
	}
	if result.MalformedEvents != 4 {
		t.Fatalf("malformed = %d, want 4; failures=%#v", result.MalformedEvents, result.Failures)
	}
}

func TestCSVTradeFixtureIsValid(t *testing.T) {
	result, err := File(fixturePath("solusdt_aggtrade.csv"), parser.FormatCSV, Options{})
	if err != nil {
		t.Fatalf("File returned error: %v", err)
	}

	if result.Rows != 5 || result.Events != 5 {
		t.Fatalf("rows/events = %d/%d, want 5/5", result.Rows, result.Events)
	}
	if result.MalformedEvents != 0 || result.SequenceGaps != 0 || result.OrderingFailures != 0 {
		t.Fatalf("unexpected validation failures: %#v", result)
	}
}
