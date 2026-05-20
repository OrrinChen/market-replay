//go:build integration && kafka

package kafkastream

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
)

func TestKgoProducerIntegrationPublishesRawMarketMessage(t *testing.T) {
	brokers := splitEnvCSV(os.Getenv("KAFKA_BROKERS"))
	if len(brokers) == 0 {
		t.Skip("KAFKA_BROKERS is not set; skipping Kafka/Redpanda integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	createTopicForIntegrationTest(t, ctx, brokers, TopicMarketDepthRaw)

	producer, err := NewKgoProducer(brokers)
	if err != nil {
		t.Fatalf("NewKgoProducer returned error: %v", err)
	}
	defer producer.Close()

	err = producer.Produce(ctx, Message{
		Topic: TopicMarketDepthRaw,
		Key:   []byte("BTCUSDT"),
		Value: []byte(`{"event_type":"depth","symbol":"BTCUSDT","event_time":1710000000000,"first_update_id":1,"final_update_id":2,"bids":[["65000.00","1.2"]]}`),
	})
	if err != nil {
		t.Fatalf("Produce returned error: %v", err)
	}
}

func createTopicForIntegrationTest(t *testing.T, ctx context.Context, brokers []string, topic string) {
	t.Helper()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.MaxVersions(kversion.V2_4_0()),
	)
	if err != nil {
		t.Fatalf("create admin client: %v", err)
	}
	defer client.Close()

	req := kmsg.NewPtrCreateTopicsRequest()
	reqTopic := kmsg.NewCreateTopicsRequestTopic()
	reqTopic.Topic = topic
	reqTopic.NumPartitions = 1
	reqTopic.ReplicationFactor = 1
	req.Topics = append(req.Topics, reqTopic)

	res, err := req.RequestWith(ctx, client)
	if err != nil {
		t.Fatalf("create topic request: %v", err)
	}
	if len(res.Topics) != 1 {
		t.Fatalf("create topic response count = %d, want 1", len(res.Topics))
	}
	if err := kerr.ErrorForCode(res.Topics[0].ErrorCode); err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		t.Fatalf("create topic %q: %v", topic, err)
	}

	for {
		meta := kmsg.NewPtrMetadataRequest()
		metaTopic := kmsg.NewMetadataRequestTopic()
		metaTopic.Topic = kmsg.StringPtr(topic)
		meta.Topics = append(meta.Topics, metaTopic)

		res, err := meta.RequestWith(ctx, client)
		if err == nil && len(res.Topics) == 1 && kerr.ErrorForCode(res.Topics[0].ErrorCode) == nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for topic %q metadata: %v", topic, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func splitEnvCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := parts[:0]
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
