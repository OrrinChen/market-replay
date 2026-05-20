//go:build kafka

package kafkastream

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
)

type KgoProducer struct {
	client *kgo.Client
}

func NewKgoProducer(brokers []string) (*KgoProducer, error) {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, err
	}
	return &KgoProducer{client: client}, nil
}

func EnsureTopics(ctx context.Context, brokers []string, specs []TopicSpec) error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.MaxVersions(kversion.V2_4_0()),
	)
	if err != nil {
		return err
	}
	defer client.Close()

	req := kmsg.NewPtrCreateTopicsRequest()
	for _, spec := range specs {
		if spec.Name == "" {
			continue
		}
		partitions := spec.Partitions
		if partitions <= 0 {
			partitions = 1
		}
		replication := spec.ReplicationFactor
		if replication <= 0 {
			replication = 1
		}
		topic := kmsg.NewCreateTopicsRequestTopic()
		topic.Topic = spec.Name
		topic.NumPartitions = partitions
		topic.ReplicationFactor = replication
		req.Topics = append(req.Topics, topic)
	}
	if len(req.Topics) == 0 {
		return nil
	}
	res, err := req.RequestWith(ctx, client)
	if err != nil {
		return err
	}
	for _, topic := range res.Topics {
		if err := kerr.ErrorForCode(topic.ErrorCode); err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("create topic %q: %w", topic.Topic, err)
		}
	}
	for _, spec := range specs {
		if spec.Name == "" {
			continue
		}
		if err := waitTopicMetadata(ctx, client, spec.Name); err != nil {
			return err
		}
	}
	return nil
}

func waitTopicMetadata(ctx context.Context, client *kgo.Client, topic string) error {
	for {
		req := kmsg.NewPtrMetadataRequest()
		reqTopic := kmsg.NewMetadataRequestTopic()
		reqTopic.Topic = kmsg.StringPtr(topic)
		req.Topics = append(req.Topics, reqTopic)
		res, err := req.RequestWith(ctx, client)
		if err == nil && len(res.Topics) == 1 && kerr.ErrorForCode(res.Topics[0].ErrorCode) == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for topic %q metadata: %w", topic, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (p *KgoProducer) Close() {
	p.client.Close()
}

func (p *KgoProducer) Produce(ctx context.Context, msg Message) error {
	done := make(chan error, 1)
	p.client.Produce(ctx, &kgo.Record{
		Topic: msg.Topic,
		Key:   msg.Key,
		Value: msg.Value,
	}, func(_ *kgo.Record, err error) {
		done <- err
	})
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

type KgoConsumer struct {
	client  *kgo.Client
	pending []Message
}

func NewKgoConsumer(brokers []string, group string, topics []string) (*KgoConsumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
	)
	if err != nil {
		return nil, err
	}
	return &KgoConsumer{client: client}, nil
}

func (c *KgoConsumer) Close() {
	c.client.Close()
}

func (c *KgoConsumer) Poll(ctx context.Context) (Message, error) {
	if len(c.pending) > 0 {
		msg := c.pending[0]
		c.pending = c.pending[1:]
		return msg, nil
	}

	fetches := c.client.PollFetches(ctx)
	if err := fetches.Err(); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return Message{}, err
		}
		return Message{}, fmt.Errorf("poll kafka fetches: %w", err)
	}
	fetches.EachRecord(func(record *kgo.Record) {
		c.pending = append(c.pending, Message{
			Topic:     record.Topic,
			Key:       append([]byte(nil), record.Key...),
			Value:     append([]byte(nil), record.Value...),
			Partition: record.Partition,
			Offset:    record.Offset,
			Lag:       -1,
		})
	})
	if len(c.pending) == 0 {
		return Message{}, ErrNoMessage
	}
	msg := c.pending[0]
	c.pending = c.pending[1:]
	return msg, nil
}
