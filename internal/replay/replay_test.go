package replay

import (
	"strings"
	"testing"
	"time"

	"github.com/orynwilder/market-replay-service/internal/parser"
)

func TestRunnerApplies10xSpeedControl(t *testing.T) {
	input := strings.NewReader(strings.Join([]string{
		`{"event_type":"depth","symbol":"BTCUSDT","event_time":1710000000000,"first_update_id":1,"final_update_id":2,"bids":[["1","1"]],"asks":[["2","1"]]}`,
		`{"event_type":"depth","symbol":"BTCUSDT","event_time":1710000000100,"first_update_id":3,"final_update_id":4,"bids":[["1","1"]],"asks":[["2","1"]]}`,
		`{"event_type":"depth","symbol":"BTCUSDT","event_time":1710000000200,"first_update_id":5,"final_update_id":6,"bids":[["1","1"]],"asks":[["2","1"]]}`,
	}, "\n"))
	stream, err := parser.NewStream(input, parser.FormatJSONL)
	if err != nil {
		t.Fatalf("NewStream returned error: %v", err)
	}

	var slept time.Duration
	runner := Runner{Sleep: func(d time.Duration) { slept += d }}
	summary, err := runner.Stream(stream, "BTCUSDT", Speed10x)
	if err != nil {
		t.Fatalf("Stream returned error: %v", err)
	}

	if summary.Events != 3 || summary.SequenceGaps != 0 || summary.MalformedEvents != 0 {
		t.Fatalf("unexpected summary: %#v", summary)
	}
	if slept != 20*time.Millisecond {
		t.Fatalf("slept = %s, want 20ms", slept)
	}
}

func TestParseSpeedRejectsUnsupportedValue(t *testing.T) {
	if _, err := ParseSpeed("100x"); err == nil {
		t.Fatal("ParseSpeed accepted unsupported value")
	}
}
