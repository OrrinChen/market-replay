package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"time"

	"github.com/orynwilder/market-replay-service/internal/kafkastream"
	"github.com/orynwilder/market-replay-service/internal/parser"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "kafka-replay: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		return fmt.Errorf("usage: kafka-replay <produce|validate> [flags]")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	switch os.Args[1] {
	case "produce":
		return runProduce(ctx, os.Args[2:])
	case "validate":
		return runValidate(ctx, os.Args[2:])
	case "bench":
		return runBench(ctx, os.Args[2:])
	default:
		return fmt.Errorf("unknown command %q", os.Args[1])
	}
}

func runProduce(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("produce", flag.ContinueOnError)
	brokers := fs.String("brokers", "localhost:9092", "comma-separated Kafka broker addresses")
	filePath := fs.String("file", "", "historical market event file")
	format := fs.String("format", string(parser.FormatAuto), "input format: auto, jsonl, csv")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *filePath == "" {
		return fmt.Errorf("-file is required")
	}

	producer, err := kafkastream.NewKgoProducer(splitCSV(*brokers))
	if err != nil {
		return err
	}
	defer producer.Close()

	count, err := kafkastream.PublishFile(ctx, *filePath, parser.Format(*format), producer)
	if err != nil {
		return err
	}
	fmt.Printf("produced %d raw market events\n", count)
	return nil
}

func runValidate(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	brokers := fs.String("brokers", "localhost:9092", "comma-separated Kafka broker addresses")
	group := fs.String("group", kafkastream.GroupReplayValidator, "Kafka consumer group")
	if err := fs.Parse(args); err != nil {
		return err
	}

	brokerList := splitCSV(*brokers)
	consumer, err := kafkastream.NewKgoConsumer(brokerList, *group, []string{
		kafkastream.TopicMarketDepthRaw,
		kafkastream.TopicMarketTradeRaw,
	})
	if err != nil {
		return err
	}
	defer consumer.Close()

	producer, err := kafkastream.NewKgoProducer(brokerList)
	if err != nil {
		return err
	}
	defer producer.Close()

	validator := kafkastream.NewValidatorWithOptions(consumer, producer, kafkastream.ValidatorOptions{
		Group: *group,
	})
	return validator.Run(ctx)
}

type benchResult struct {
	File                         string        `json:"file"`
	DepthTopic                   string        `json:"depth_topic"`
	TradeTopic                   string        `json:"trade_topic"`
	Group                        string        `json:"group"`
	ProducedEvents               int64         `json:"produced_events"`
	ConsumedEvents               int64         `json:"consumed_events"`
	ProduceDuration              time.Duration `json:"produce_duration"`
	ConsumeDuration              time.Duration `json:"consume_duration"`
	EndToEndDuration             time.Duration `json:"end_to_end_duration"`
	ProducerEventsPerSecond      float64       `json:"producer_events_per_second"`
	ConsumerEventsPerSecond      float64       `json:"consumer_events_per_second"`
	ObservedConsumerLagP95Events int64         `json:"observed_consumer_lag_p95_events"`
}

func runBench(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	brokers := fs.String("brokers", "localhost:9092", "comma-separated Kafka broker addresses")
	filePath := fs.String("file", "", "historical market event file")
	format := fs.String("format", string(parser.FormatAuto), "input format: auto, jsonl, csv")
	topicPrefix := fs.String("topic-prefix", fmt.Sprintf("market.bench.%d", time.Now().UnixNano()), "topic prefix for isolated benchmark topics")
	timeout := fs.Duration("timeout", 2*time.Minute, "produce+consume timeout")
	jsonOut := fs.Bool("json", false, "emit raw JSON result instead of Markdown table")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *filePath == "" {
		return fmt.Errorf("-file is required")
	}

	resolved, err := parser.ResolveFormat(*filePath, parser.Format(*format))
	if err != nil {
		return err
	}
	brokerList := splitCSV(*brokers)
	depthTopic := *topicPrefix + ".depth.raw"
	tradeTopic := *topicPrefix + ".trade.raw"
	group := *topicPrefix + ".replay-validator"

	runCtx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	if err := kafkastream.EnsureTopics(runCtx, brokerList, []kafkastream.TopicSpec{
		{Name: depthTopic, Partitions: 1, ReplicationFactor: 1},
		{Name: tradeTopic, Partitions: 1, ReplicationFactor: 1},
	}); err != nil {
		return err
	}

	producer, err := kafkastream.NewKgoProducer(brokerList)
	if err != nil {
		return err
	}
	defer producer.Close()

	started := time.Now()
	produceStarted := time.Now()
	produced, err := kafkastream.PublishFileWithTopics(runCtx, *filePath, resolved, producer, depthTopic, tradeTopic)
	if err != nil {
		return err
	}
	produceDuration := time.Since(produceStarted)

	consumer, err := kafkastream.NewKgoConsumer(brokerList, group, []string{depthTopic, tradeTopic})
	if err != nil {
		return err
	}
	defer consumer.Close()

	consumeStarted := time.Now()
	consumed := int64(0)
	lagSamples := make([]int64, 0, produced)
	for consumed < produced {
		msg, err := consumer.Poll(runCtx)
		if err != nil {
			if err == kafkastream.ErrNoMessage {
				continue
			}
			return err
		}
		if msg.Topic == depthTopic || msg.Topic == tradeTopic {
			consumed++
			lagSamples = append(lagSamples, produced-consumed)
		}
	}
	consumeDuration := time.Since(consumeStarted)
	result := benchResult{
		File:                         *filePath,
		DepthTopic:                   depthTopic,
		TradeTopic:                   tradeTopic,
		Group:                        group,
		ProducedEvents:               produced,
		ConsumedEvents:               consumed,
		ProduceDuration:              produceDuration,
		ConsumeDuration:              consumeDuration,
		EndToEndDuration:             time.Since(started),
		ProducerEventsPerSecond:      perSecond(produced, produceDuration),
		ConsumerEventsPerSecond:      perSecond(consumed, consumeDuration),
		ObservedConsumerLagP95Events: percentileInt64(lagSamples, 0.95),
	}
	if *jsonOut {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(result)
	}
	printBenchResult(result)
	return nil
}

func printBenchResult(row benchResult) {
	fmt.Println("| file | produced | consumed | produce events/sec | consume events/sec | e2e duration | observed lag p95 events |")
	fmt.Println("|---|---:|---:|---:|---:|---:|---:|")
	fmt.Printf("| %s | %d | %d | %.2f | %.2f | %s | %d |\n",
		row.File,
		row.ProducedEvents,
		row.ConsumedEvents,
		row.ProducerEventsPerSecond,
		row.ConsumerEventsPerSecond,
		row.EndToEndDuration.Round(time.Millisecond),
		row.ObservedConsumerLagP95Events,
	)
}

func perSecond(count int64, duration time.Duration) float64 {
	if duration <= 0 {
		return 0
	}
	return float64(count) / duration.Seconds()
}

func percentileInt64(values []int64, pct float64) int64 {
	if len(values) == 0 {
		return 0
	}
	cp := append([]int64(nil), values...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	index := int(float64(len(cp)-1) * pct)
	return cp[index]
}

func splitCSV(value string) []string {
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
