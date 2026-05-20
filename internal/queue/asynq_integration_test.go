//go:build integration

package queue

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/orynwilder/market-replay-service/internal/store"
)

func TestAsynqIntegrationDuplicateReplayEnqueueIsIdempotent(t *testing.T) {
	redisAddr := strings.TrimSpace(os.Getenv("REDIS_ADDR"))
	if redisAddr == "" {
		t.Skip("REDIS_ADDR is not set; skipping Redis/Asynq integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	redis := asynq.RedisClientOpt{Addr: redisAddr}
	client := NewAsynqClient(redis)
	defer client.Close()

	queueName := "replay-integration-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	inspector := asynq.NewInspector(redis)
	defer inspector.Close()
	t.Cleanup(func() {
		_, _ = inspector.DeleteAllPendingTasks(queueName)
		_ = inspector.DeleteQueue(queueName, true)
	})

	repo := store.NewMemoryRepository()
	dataset, err := repo.CreateDataset(ctx, store.CreateDatasetParams{Name: "redis-integration"})
	if err != nil {
		t.Fatalf("CreateDataset returned error: %v", err)
	}
	file, err := repo.CreateEventFile(ctx, store.CreateEventFileParams{
		DatasetID: dataset.ID,
		Path:      "testdata/btcusdt_depth.jsonl",
		Format:    "jsonl",
		Symbol:    "BTCUSDT",
	})
	if err != nil {
		t.Fatalf("CreateEventFile returned error: %v", err)
	}
	job, err := repo.CreateReplayJob(ctx, store.CreateReplayJobParams{
		DatasetID:      dataset.ID,
		EventFileID:    file.ID,
		IdempotencyKey: "redis-integration-" + queueName,
		Symbol:         "BTCUSDT",
		Speed:          "max",
	})
	if err != nil {
		t.Fatalf("CreateReplayJob returned error: %v", err)
	}

	enqueuer := NewReplayEnqueuer(repo, client, ReplayEnqueueConfig{
		Queue:        queueName,
		MaxRetry:     1,
		Timeout:      time.Minute,
		UniqueTTL:    time.Minute,
		TaskIDPrefix: "replay-job:",
	})
	first, err := enqueuer.EnqueueReplayJob(ctx, job)
	if err != nil {
		t.Fatalf("first EnqueueReplayJob returned error: %v", err)
	}
	if first == nil || first.ID != "replay-job:"+job.IdempotencyKey {
		t.Fatalf("first task info = %#v, want deterministic task id", first)
	}

	second, err := enqueuer.EnqueueReplayJob(ctx, job)
	if !errors.Is(err, asynq.ErrTaskIDConflict) && !errors.Is(err, asynq.ErrDuplicateTask) {
		t.Fatalf("second EnqueueReplayJob error = %v, want Asynq duplicate conflict", err)
	}
	if second != nil {
		t.Fatalf("second task info = %#v, want nil on duplicate enqueue", second)
	}

	pending, err := inspector.ListPendingTasks(queueName)
	if err != nil {
		t.Fatalf("ListPendingTasks returned error: %v", err)
	}
	if len(pending) != 1 || pending[0].ID != first.ID {
		t.Fatalf("pending tasks = %#v, want exactly the first task", pending)
	}
}
