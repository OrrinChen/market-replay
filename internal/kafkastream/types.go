package kafkastream

import (
	"context"
	"errors"
	"fmt"

	"github.com/orynwilder/market-replay-service/internal/event"
)

const (
	TopicMarketDepthRaw        = "market.depth.raw"
	TopicMarketTradeRaw        = "market.trade.raw"
	TopicMarketReplayValidated = "market.replay.validated"
	TopicMarketReplayErrors    = "market.replay.errors"
)

const (
	GroupReplayValidator = "replay-validator"
	GroupMetricsWriter   = "metrics-writer"
	GroupGapDetector     = "gap-detector"
)

var ErrNoMessage = errors.New("no kafka message available")

type Message struct {
	Topic     string
	Key       []byte
	Value     []byte
	Partition int32
	Offset    int64
	Lag       int64
}

type TopicSpec struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
}

type Producer interface {
	Produce(context.Context, Message) error
}

type Consumer interface {
	Poll(context.Context) (Message, error)
}

type LagObserver func(group, topic string, partition int32, lag int64)

func RawTopicForEventType(eventType string) (string, error) {
	switch event.EventType(eventType) {
	case event.EventTypeDepth:
		return TopicMarketDepthRaw, nil
	case event.EventTypeAggTrade:
		return TopicMarketTradeRaw, nil
	default:
		return "", fmt.Errorf("unsupported market event type %q", eventType)
	}
}
