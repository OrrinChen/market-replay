package kafkastream

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/orynwilder/market-replay-service/internal/parser"
)

type fakeProducer struct {
	messages []Message
	err      error
}

func (p *fakeProducer) Produce(ctx context.Context, msg Message) error {
	if p.err != nil {
		return p.err
	}
	p.messages = append(p.messages, msg)
	return nil
}

type fakeConsumer struct {
	messages []Message
	index    int
}

func (c *fakeConsumer) Poll(ctx context.Context) (Message, error) {
	if c.index >= len(c.messages) {
		return Message{}, ErrNoMessage
	}
	msg := c.messages[c.index]
	c.index++
	return msg, nil
}

func TestRawTopicForEventType(t *testing.T) {
	tests := []struct {
		name      string
		eventType string
		want      string
	}{
		{name: "depth", eventType: "depth", want: TopicMarketDepthRaw},
		{name: "agg trade", eventType: "aggTrade", want: TopicMarketTradeRaw},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := RawTopicForEventType(tt.eventType)
			if err != nil {
				t.Fatalf("RawTopicForEventType returned error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("topic = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPublishFileUsesSymbolAsPartitionKey(t *testing.T) {
	ctx := context.Background()
	producer := &fakeProducer{}
	input := strings.NewReader(`{"event_type":"depth","symbol":"btcusdt","event_time":1710000000000,"first_update_id":1,"final_update_id":2,"bids":[["65000.00","1.2"]]}` + "\n")

	count, err := Publish(ctx, input, parser.FormatJSONL, producer)
	if err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
	if len(producer.messages) != 1 {
		t.Fatalf("produced %d messages, want 1", len(producer.messages))
	}
	msg := producer.messages[0]
	if msg.Topic != TopicMarketDepthRaw {
		t.Fatalf("topic = %q, want %q", msg.Topic, TopicMarketDepthRaw)
	}
	if string(msg.Key) != "BTCUSDT" {
		t.Fatalf("key = %q, want BTCUSDT", string(msg.Key))
	}
}

func TestValidatorRoutesPoisonRecordsToErrors(t *testing.T) {
	ctx := context.Background()
	consumer := &fakeConsumer{
		messages: []Message{
			{Topic: TopicMarketDepthRaw, Key: []byte("BTCUSDT"), Value: []byte(`{"event_type":"depth","symbol":"BTCUSDT","event_time":1710000000000,"first_update_id":1,"final_update_id":2,"bids":[["65000.00","1.2"]]}`)},
			{Topic: TopicMarketDepthRaw, Key: []byte("BTCUSDT"), Value: []byte(`{"event_type":"depth","symbol":"BTCUSDT","event_time":1710000000001,"first_update_id":4,"final_update_id":5,"bids":[["65001.00","1.0"]]}`)},
			{Topic: TopicMarketTradeRaw, Key: []byte("ETHUSDT"), Value: []byte(`{"event_type":"aggTrade","symbol":`)},
		},
	}
	producer := &fakeProducer{}
	validator := NewValidator(consumer, producer)

	err := validator.Run(ctx)
	if !errors.Is(err, ErrNoMessage) {
		t.Fatalf("Run error = %v, want ErrNoMessage", err)
	}

	var validated, poisoned int
	for _, msg := range producer.messages {
		switch msg.Topic {
		case TopicMarketReplayValidated:
			validated++
			if string(msg.Key) == "" {
				t.Fatalf("validated message has empty partition key")
			}
		case TopicMarketReplayErrors:
			poisoned++
			if string(msg.Key) == "" {
				t.Fatalf("poison message has empty partition key")
			}
			var envelope ErrorEnvelope
			if err := json.Unmarshal(msg.Value, &envelope); err != nil {
				t.Fatalf("decode error envelope: %v", err)
			}
			if envelope.Error == "" {
				t.Fatalf("error envelope missing error text")
			}
		}
	}
	if validated != 1 {
		t.Fatalf("validated messages = %d, want 1", validated)
	}
	if poisoned != 2 {
		t.Fatalf("poison messages = %d, want 2", poisoned)
	}
}

func TestValidatorEmitsLagMetricHook(t *testing.T) {
	ctx := context.Background()
	consumer := &fakeConsumer{
		messages: []Message{
			{Topic: TopicMarketTradeRaw, Key: []byte("ETHUSDT"), Value: []byte(`{"event_type":"aggTrade","symbol":"ETHUSDT","trade_time":1710000000000,"trade_id":10,"price":"3200.00","quantity":"0.5","is_buyer_maker":false}`), Partition: 2, Lag: 42},
		},
	}
	var observedGroup, observedTopic string
	var observedPartition int32
	var observedLag int64
	validator := NewValidatorWithOptions(consumer, &fakeProducer{}, ValidatorOptions{
		Group: GroupReplayValidator,
		LagObserver: func(group, topic string, partition int32, lag int64) {
			observedGroup = group
			observedTopic = topic
			observedPartition = partition
			observedLag = lag
		},
	})

	err := validator.Run(ctx)
	if !errors.Is(err, ErrNoMessage) {
		t.Fatalf("Run error = %v, want ErrNoMessage", err)
	}
	if observedGroup != GroupReplayValidator || observedTopic != TopicMarketTradeRaw || observedPartition != 2 || observedLag != 42 {
		t.Fatalf("observed lag = (%q, %q, %d, %d), want (%q, %q, 2, 42)", observedGroup, observedTopic, observedPartition, observedLag, GroupReplayValidator, TopicMarketTradeRaw)
	}
}
