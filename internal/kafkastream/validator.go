package kafkastream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/orynwilder/market-replay-service/internal/parser"
	"github.com/orynwilder/market-replay-service/internal/validate"
)

type ErrorEnvelope struct {
	SourceTopic string `json:"source_topic"`
	Symbol      string `json:"symbol,omitempty"`
	Partition   int32  `json:"partition"`
	Offset      int64  `json:"offset"`
	Error       string `json:"error"`
	Payload     []byte `json:"payload,omitempty"`
	ProducedAt  string `json:"produced_at"`
}

type ValidatorOptions struct {
	Group       string
	LagObserver LagObserver
}

type Validator struct {
	consumer ProducerConsumer
	producer Producer
	state    *validate.Validator
	opts     ValidatorOptions
}

type ProducerConsumer interface {
	Consumer
}

func NewValidator(consumer Consumer, producer Producer) *Validator {
	return NewValidatorWithOptions(consumer, producer, ValidatorOptions{})
}

func NewValidatorWithOptions(consumer Consumer, producer Producer, opts ValidatorOptions) *Validator {
	if opts.Group == "" {
		opts.Group = GroupReplayValidator
	}
	return &Validator{
		consumer: consumer,
		producer: producer,
		state:    validate.NewValidator(validate.Options{}),
		opts:     opts,
	}
}

func (v *Validator) Run(ctx context.Context) error {
	for {
		msg, err := v.consumer.Poll(ctx)
		if err != nil {
			return err
		}
		if v.opts.LagObserver != nil && msg.Lag >= 0 {
			v.opts.LagObserver(v.opts.Group, msg.Topic, msg.Partition, msg.Lag)
		}
		if err := v.Process(ctx, msg); err != nil {
			return err
		}
	}
}

func (v *Validator) Process(ctx context.Context, msg Message) error {
	ev, err := parser.ParseJSONLine(msg.Value, msg.Offset+1)
	if err != nil {
		return v.routeError(ctx, msg, "", err)
	}
	expectedTopic, err := RawTopicForEventType(string(ev.Type))
	if err != nil {
		return v.routeError(ctx, msg, ev.Symbol, err)
	}
	if msg.Topic != "" && msg.Topic != expectedTopic {
		return v.routeError(ctx, msg, ev.Symbol, fmt.Errorf("event type %q arrived on %q, want %q", ev.Type, msg.Topic, expectedTopic))
	}

	before := len(v.state.Result().Failures)
	v.state.Process(parser.Record{Line: msg.Offset + 1, Event: ev, Raw: msg.Value})
	afterResult := v.state.Result()
	if len(afterResult.Failures) > before {
		return v.routeError(ctx, msg, ev.Symbol, errors.New(afterResult.Failures[len(afterResult.Failures)-1].Message))
	}

	value, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	return v.producer.Produce(ctx, Message{
		Topic: TopicMarketReplayValidated,
		Key:   []byte(ev.Symbol),
		Value: value,
	})
}

func (v *Validator) routeError(ctx context.Context, msg Message, symbol string, cause error) error {
	if symbol == "" {
		symbol = string(msg.Key)
	}
	envelope := ErrorEnvelope{
		SourceTopic: msg.Topic,
		Symbol:      symbol,
		Partition:   msg.Partition,
		Offset:      msg.Offset,
		Error:       cause.Error(),
		Payload:     msg.Value,
		ProducedAt:  time.Now().UTC().Format(time.RFC3339Nano),
	}
	value, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	return v.producer.Produce(ctx, Message{
		Topic: TopicMarketReplayErrors,
		Key:   []byte(symbol),
		Value: value,
	})
}
