//go:build !kafka

package kafkastream

import (
	"context"
	"fmt"
)

type KgoProducer struct{}

func NewKgoProducer(brokers []string) (*KgoProducer, error) {
	return nil, fmt.Errorf("kafka support requires building with -tags kafka")
}

func (p *KgoProducer) Close() {}

func (p *KgoProducer) Produce(ctx context.Context, msg Message) error {
	return fmt.Errorf("kafka support requires building with -tags kafka")
}

func EnsureTopics(ctx context.Context, brokers []string, specs []TopicSpec) error {
	return fmt.Errorf("kafka support requires building with -tags kafka")
}

type KgoConsumer struct{}

func NewKgoConsumer(brokers []string, group string, topics []string) (*KgoConsumer, error) {
	return nil, fmt.Errorf("kafka support requires building with -tags kafka")
}

func (c *KgoConsumer) Close() {}

func (c *KgoConsumer) Poll(ctx context.Context) (Message, error) {
	return Message{}, fmt.Errorf("kafka support requires building with -tags kafka")
}
