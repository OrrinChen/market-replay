package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/orynwilder/market-replay-service/internal/api"
	"github.com/orynwilder/market-replay-service/internal/dashboard"
	"github.com/orynwilder/market-replay-service/internal/observability"
	"github.com/orynwilder/market-replay-service/internal/queue"
	"github.com/orynwilder/market-replay-service/internal/store"
	"github.com/prometheus/client_golang/prometheus"
)

func main() {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}
	addr := os.Getenv("MARKET_REPLAY_HTTP_ADDR")
	if addr == "" {
		port := os.Getenv("PORT")
		if port == "" {
			port = "8080"
		}
		addr = ":" + port
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		log.Fatalf("create postgres pool: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("ping postgres: %v", err)
	}
	if err := store.EnsurePostgresSchema(ctx, pool); err != nil {
		log.Fatalf("ensure postgres schema: %v", err)
	}

	repo := store.NewPostgresRepository(pool)
	registry := prometheus.NewRegistry()
	metrics := observability.MustRegister(registry)
	apiOptions := api.Options{
		Middleware: []gin.HandlerFunc{metrics.GinLatencyMiddleware()},
	}
	if redisAddr := os.Getenv("REDIS_ADDR"); redisAddr != "" {
		client := queue.NewAsynqClient(asynq.RedisClientOpt{Addr: redisAddr})
		defer client.Close()
		enqueuer := queue.NewReplayEnqueuer(repo, client, queue.ReplayEnqueueConfig{})
		apiOptions.SubmitReplayJob = func(ctx context.Context, params store.CreateReplayJobParams) (store.ReplayJob, error) {
			job, _, err := enqueuer.SubmitReplay(ctx, params)
			return job, err
		}
	}

	handler := http.NewServeMux()
	apiHandler := api.NewRouterWithOptions(repo, apiOptions)
	handler.Handle("/", apiHandler)
	handler.Handle("/dashboard", dashboard.New(repo))
	handler.Handle("/dashboard/", dashboard.New(repo))
	observability.RegisterMetricsHandler(handler, "/metrics", registry)
	observability.RegisterPprof(handler, "/debug/pprof")
	if strings.EqualFold(os.Getenv("MARKET_REPLAY_SERVE_OPENAPI"), "true") {
		handler.Handle("/openapi.yaml", http.FileServer(http.Dir("docs")))
	}

	server := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("market replay API listening on %s", addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("serve API: %v", err)
	}
}
