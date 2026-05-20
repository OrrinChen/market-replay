package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/orynwilder/market-replay-service/internal/observability"
	"github.com/orynwilder/market-replay-service/internal/queue"
	"github.com/orynwilder/market-replay-service/internal/store"
	"github.com/orynwilder/market-replay-service/internal/worker"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

func main() {
	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintln(os.Stderr, "init logger:", err)
		os.Exit(1)
	}
	defer logger.Sync()

	if err := run(logger); err != nil {
		logger.Fatal("worker exited", zap.Error(err))
	}
}

func run(logger *zap.Logger) error {
	ctx := context.Background()
	redisAddr := env("REDIS_ADDR", "127.0.0.1:6379")
	queueName := env("ASYNQ_QUEUE", queue.DefaultQueue)
	maxRetry := envInt("ASYNQ_MAX_RETRY", 3)
	timeout := envDuration("ASYNQ_TIMEOUT", 30*time.Minute)
	backoff := envDuration("ASYNQ_RETRY_BACKOFF", 15*time.Second)
	deadLetter := env("ASYNQ_DEAD_LETTER", "asynq-archive")
	metricsAddr := os.Getenv("WORKER_METRICS_ADDR")
	concurrency := envInt("WORKER_CONCURRENCY", 1)

	// This binary is wired repository-first: Redis/Asynq is only the job control plane.
	repo, cleanup, err := openRepository(ctx, logger)
	if err != nil {
		return err
	}
	defer cleanup()

	var metrics *observability.Metrics
	if metricsAddr != "" {
		registry := prometheus.NewRegistry()
		metrics = observability.MustRegister(registry)
		metricsServer, err := startMetricsEndpoint(logger, metricsAddr, registry)
		if err != nil {
			return err
		}
		defer metricsServer.Close()
	}

	redis := asynq.RedisClientOpt{Addr: redisAddr}
	server := worker.NewServer(repo, logger, worker.ServerConfig{
		Redis:          redis,
		Queue:          queueName,
		Concurrency:    concurrency,
		MaxRetry:       maxRetry,
		Timeout:        timeout,
		RetryBackoff:   backoff,
		DeadLetterName: deadLetter,
		Metrics:        metrics,
	})
	mux := worker.NewServeMux(repo, logger, worker.ReplayHandlerConfig{
		DeadLetterName: deadLetter,
		Queue:          queueName,
		Metrics:        metrics,
	})
	logger.Info("starting replay worker",
		zap.String("redis_addr", redisAddr),
		zap.String("queue", queueName),
		zap.Int("max_retry", maxRetry),
		zap.Duration("timeout", timeout),
		zap.Duration("retry_backoff", backoff),
		zap.String("dead_letter", deadLetter),
		zap.String("metrics_addr", metricsAddr),
	)
	return server.Run(mux)
}

func startMetricsEndpoint(logger *zap.Logger, addr string, registry *prometheus.Registry) (*http.Server, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen worker metrics endpoint: %w", err)
	}
	mux := http.NewServeMux()
	observability.RegisterMetricsHandler(mux, "/metrics", registry)
	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		logger.Info("worker metrics listening", zap.String("addr", addr))
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			logger.Error("worker metrics server exited", zap.Error(err))
		}
	}()
	return server, nil
}

func openRepository(ctx context.Context, logger *zap.Logger) (store.Repository, func(), error) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		logger.Warn("worker using memory repository; set DATABASE_URL to use Postgres as source of truth")
		return store.NewMemoryRepository(), func() {}, nil
	}
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, nil, fmt.Errorf("open postgres repository: %w", err)
	}
	if err := store.EnsurePostgresSchema(ctx, pool); err != nil {
		pool.Close()
		return nil, nil, fmt.Errorf("ensure postgres schema: %w", err)
	}
	return store.NewPostgresRepository(pool), pool.Close, nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}
