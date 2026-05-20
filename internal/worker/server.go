package worker

import (
	"context"
	"errors"
	"time"

	"github.com/hibiken/asynq"
	"github.com/orynwilder/market-replay-service/internal/observability"
	jobqueue "github.com/orynwilder/market-replay-service/internal/queue"
	"github.com/orynwilder/market-replay-service/internal/store"
	"go.uber.org/zap"
)

type ServerConfig struct {
	Redis          asynq.RedisConnOpt
	Queue          string
	Concurrency    int
	MaxRetry       int
	Timeout        time.Duration
	RetryBackoff   time.Duration
	DeadLetterName string
	Metrics        *observability.Metrics
}

func NewServer(repo store.Repository, logger *zap.Logger, cfg ServerConfig) *asynq.Server {
	if logger == nil {
		logger = zap.NewNop()
	}
	queueName := cfg.Queue
	if queueName == "" {
		queueName = jobqueue.DefaultQueue
	}
	backoff := cfg.RetryBackoff
	if backoff == 0 {
		backoff = 15 * time.Second
	}
	return asynq.NewServer(cfg.Redis, asynq.Config{
		Concurrency: cfg.Concurrency,
		Queues: map[string]int{
			queueName: 1,
		},
		RetryDelayFunc: func(n int, _ error, _ *asynq.Task) time.Duration {
			delay := backoff * (1 << max(0, n-1))
			if delay > 5*time.Minute {
				return 5 * time.Minute
			}
			return delay
		},
		Logger: zapAsynqLogger{logger: logger},
		ErrorHandler: asynq.ErrorHandlerFunc(func(ctx context.Context, task *asynq.Task, err error) {
			retried, _ := asynq.GetRetryCount(ctx)
			maxRetry, _ := asynq.GetMaxRetry(ctx)
			if cfg.Metrics != nil && retried < maxRetry && !errors.Is(err, asynq.SkipRetry) {
				cfg.Metrics.RecordWorkerRetry(queueName, task.Type())
			}
			logger.Error("asynq replay task error",
				zap.String("task_type", task.Type()),
				zap.Int("retry_count", retried),
				zap.Int("max_retry", maxRetry),
				zap.String("dead_letter", cfg.DeadLetterName),
				zap.Error(err),
			)
		}),
	})
}

func NewServeMux(repo store.Repository, logger *zap.Logger, cfg ReplayHandlerConfig) *asynq.ServeMux {
	mux := asynq.NewServeMux()
	handler := NewReplayHandler(repo, logger, cfg)
	mux.Handle(jobqueue.TypeReplayJob, handler)
	return mux
}

type zapAsynqLogger struct {
	logger *zap.Logger
}

func (l zapAsynqLogger) Debug(args ...interface{}) { l.log().Sugar().Debug(args...) }
func (l zapAsynqLogger) Info(args ...interface{})  { l.log().Sugar().Info(args...) }
func (l zapAsynqLogger) Warn(args ...interface{})  { l.log().Sugar().Warn(args...) }
func (l zapAsynqLogger) Error(args ...interface{}) { l.log().Sugar().Error(args...) }
func (l zapAsynqLogger) Fatal(args ...interface{}) { l.log().Sugar().Fatal(args...) }

func (l zapAsynqLogger) log() *zap.Logger {
	if l.logger == nil {
		return zap.NewNop()
	}
	return l.logger
}
