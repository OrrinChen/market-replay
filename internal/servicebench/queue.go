package servicebench

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/hibiken/asynq"
	"github.com/orynwilder/market-replay-service/internal/queue"
)

const (
	DefaultQueueJobs = 1000
)

type QueueOptions struct {
	RedisAddr string
	Jobs      int
	QueueName string
}

type QueueResult struct {
	RedisAddr               string        `json:"redis_addr"`
	QueueName               string        `json:"queue_name"`
	Jobs                    int           `json:"jobs"`
	EnqueueDuration         time.Duration `json:"enqueue_duration"`
	JobsPerMinute           float64       `json:"jobs_per_minute"`
	EnqueueP50              time.Duration `json:"enqueue_p50"`
	EnqueueP95              time.Duration `json:"enqueue_p95"`
	DuplicateRejected       bool          `json:"duplicate_rejected"`
	DuplicateRejectionCount int           `json:"duplicate_rejection_count"`
}

func RunQueue(ctx context.Context, opts QueueOptions) (QueueResult, error) {
	if opts.RedisAddr == "" {
		return QueueResult{}, fmt.Errorf("redis address is required")
	}
	if opts.Jobs <= 0 {
		opts.Jobs = DefaultQueueJobs
	}
	if opts.QueueName == "" {
		opts.QueueName = fmt.Sprintf("bench-replay-%d", time.Now().UnixNano())
	}

	redis := asynq.RedisClientOpt{Addr: opts.RedisAddr}
	client := asynq.NewClient(redis)
	defer client.Close()
	inspector := asynq.NewInspector(redis)
	defer inspector.Close()
	defer inspector.DeleteQueue(opts.QueueName, true)

	latencies := make([]time.Duration, 0, opts.Jobs)
	started := time.Now()
	for i := 0; i < opts.Jobs; i++ {
		task, err := replayTask(fmt.Sprintf("queue-bench-job-%08d", i))
		if err != nil {
			return QueueResult{}, err
		}
		taskStarted := time.Now()
		if _, err := client.EnqueueContext(ctx, task,
			asynq.Queue(opts.QueueName),
			asynq.MaxRetry(3),
			asynq.Timeout(30*time.Minute),
			asynq.TaskID(fmt.Sprintf("queue-bench-task-%08d", i)),
			asynq.Unique(time.Hour),
		); err != nil {
			return QueueResult{}, err
		}
		latencies = append(latencies, time.Since(taskStarted))
	}
	duration := time.Since(started)

	duplicateRejected := 0
	duplicateTask, err := replayTask("queue-bench-duplicate")
	if err != nil {
		return QueueResult{}, err
	}
	duplicateOpts := []asynq.Option{
		asynq.Queue(opts.QueueName),
		asynq.TaskID("queue-bench-duplicate-task"),
		asynq.Unique(time.Hour),
	}
	if _, err := client.EnqueueContext(ctx, duplicateTask, duplicateOpts...); err != nil {
		return QueueResult{}, err
	}
	if _, err := client.EnqueueContext(ctx, duplicateTask, duplicateOpts...); errors.Is(err, asynq.ErrDuplicateTask) || errors.Is(err, asynq.ErrTaskIDConflict) {
		duplicateRejected = 1
	} else if err != nil {
		return QueueResult{}, err
	}

	return QueueResult{
		RedisAddr:               opts.RedisAddr,
		QueueName:               opts.QueueName,
		Jobs:                    opts.Jobs,
		EnqueueDuration:         duration,
		JobsPerMinute:           float64(opts.Jobs) / duration.Minutes(),
		EnqueueP50:              percentileQueueDuration(latencies, 0.50),
		EnqueueP95:              percentileQueueDuration(latencies, 0.95),
		DuplicateRejected:       duplicateRejected == 1,
		DuplicateRejectionCount: duplicateRejected,
	}, nil
}

func replayTask(jobID string) (*asynq.Task, error) {
	payload, err := queue.EncodeReplayPayload(queue.ReplayPayload{JobID: jobID})
	if err != nil {
		return nil, err
	}
	return asynq.NewTask(queue.TypeReplayJob, payload), nil
}

func percentileQueueDuration(values []time.Duration, pct float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	cp := append([]time.Duration(nil), values...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	index := int(float64(len(cp)-1) * pct)
	return cp[index]
}
