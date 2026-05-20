package kafkastream

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/orynwilder/market-replay-service/internal/parser"
)

func PublishFile(ctx context.Context, path string, format parser.Format, producer Producer) (int64, error) {
	return PublishFileWithTopics(ctx, path, format, producer, TopicMarketDepthRaw, TopicMarketTradeRaw)
}

func PublishFileWithTopics(ctx context.Context, path string, format parser.Format, producer Producer, depthTopic string, tradeTopic string) (int64, error) {
	stream, err := parser.Open(path, format)
	if err != nil {
		return 0, err
	}
	defer stream.Close()
	return PublishStreamWithTopics(ctx, stream, producer, depthTopic, tradeTopic)
}

func Publish(ctx context.Context, r io.Reader, format parser.Format, producer Producer) (int64, error) {
	stream, err := parser.NewStream(r, format)
	if err != nil {
		return 0, err
	}
	return PublishStream(ctx, stream, producer)
}

func PublishStream(ctx context.Context, stream *parser.Stream, producer Producer) (int64, error) {
	return PublishStreamWithTopics(ctx, stream, producer, TopicMarketDepthRaw, TopicMarketTradeRaw)
}

func PublishStreamWithTopics(ctx context.Context, stream *parser.Stream, producer Producer, depthTopic string, tradeTopic string) (int64, error) {
	var produced int64
	for {
		record, err := stream.Next()
		if err == io.EOF {
			return produced, nil
		}
		if err != nil {
			return produced, err
		}
		if record.Err != nil {
			return produced, fmt.Errorf("line %d: %w", record.Line, record.Err)
		}

		topic, err := rawTopicForEventType(string(record.Event.Type), depthTopic, tradeTopic)
		if err != nil {
			return produced, fmt.Errorf("line %d: %w", record.Line, err)
		}
		value, err := json.Marshal(record.Event)
		if err != nil {
			return produced, fmt.Errorf("line %d: marshal event: %w", record.Line, err)
		}
		msg := Message{
			Topic: topic,
			Key:   []byte(record.Event.Symbol),
			Value: value,
		}
		if err := producer.Produce(ctx, msg); err != nil {
			return produced, fmt.Errorf("line %d: produce: %w", record.Line, err)
		}
		produced++
	}
}

func rawTopicForEventType(eventType string, depthTopic string, tradeTopic string) (string, error) {
	topic, err := RawTopicForEventType(eventType)
	if err != nil {
		return "", err
	}
	switch topic {
	case TopicMarketDepthRaw:
		return depthTopic, nil
	case TopicMarketTradeRaw:
		return tradeTopic, nil
	default:
		return topic, nil
	}
}
