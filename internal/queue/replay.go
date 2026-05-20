package queue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hibiken/asynq"
	"github.com/orynwilder/market-replay-service/internal/store"
)

const (
	TypeReplayJob = "replay:run"
	DefaultQueue  = "replay"
)

type ReplayPayload struct {
	JobID string `json:"job_id"`
}

type ReplayEnqueueConfig struct {
	Queue          string
	MaxRetry       int
	Timeout        time.Duration
	UniqueTTL      time.Duration
	TaskIDPrefix   string
	RetryBackoff   time.Duration
	DeadLetterName string
}

type Client interface {
	EnqueueContext(ctx context.Context, task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error)
}

type ReplayEnqueuer struct {
	repo   store.Repository
	client Client
	cfg    ReplayEnqueueConfig
}

func NewAsynqClient(redis asynq.RedisConnOpt) *asynq.Client {
	return asynq.NewClient(redis)
}

func NewReplayEnqueuer(repo store.Repository, client Client, cfg ReplayEnqueueConfig) *ReplayEnqueuer {
	cfg = cfg.withDefaults()
	return &ReplayEnqueuer{repo: repo, client: client, cfg: cfg}
}

func EncodeReplayPayload(payload ReplayPayload) ([]byte, error) {
	if strings.TrimSpace(payload.JobID) == "" {
		return nil, fmt.Errorf("replay payload requires job_id")
	}
	return json.Marshal(payload)
}

func DecodeReplayPayload(data []byte) (ReplayPayload, error) {
	var payload ReplayPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return ReplayPayload{}, fmt.Errorf("decode replay payload: %w", err)
	}
	if strings.TrimSpace(payload.JobID) == "" {
		return ReplayPayload{}, fmt.Errorf("replay payload requires job_id")
	}
	return payload, nil
}

func (e *ReplayEnqueuer) SubmitReplay(ctx context.Context, params store.CreateReplayJobParams) (store.ReplayJob, *asynq.TaskInfo, error) {
	job, err := e.repo.CreateReplayJob(ctx, params)
	if err != nil {
		if errors.Is(err, store.ErrAlreadyExists) && params.IdempotencyKey != "" {
			existing, getErr := e.repo.GetReplayJobByIdempotencyKey(ctx, params.IdempotencyKey)
			if getErr != nil {
				return store.ReplayJob{}, nil, getErr
			}
			return existing, nil, nil
		}
		return store.ReplayJob{}, nil, err
	}

	info, err := e.EnqueueReplayJob(ctx, job)
	if err != nil {
		if errors.Is(err, asynq.ErrDuplicateTask) || errors.Is(err, asynq.ErrTaskIDConflict) {
			return job, nil, nil
		}
		return store.ReplayJob{}, nil, err
	}
	return job, info, nil
}

func (e *ReplayEnqueuer) EnqueueReplayJob(ctx context.Context, job store.ReplayJob) (*asynq.TaskInfo, error) {
	payload, err := EncodeReplayPayload(ReplayPayload{JobID: job.ID})
	if err != nil {
		return nil, err
	}
	task := asynq.NewTask(TypeReplayJob, payload)
	return e.client.EnqueueContext(ctx, task, e.OptionsForJob(job)...)
}

func (e *ReplayEnqueuer) OptionsForJob(job store.ReplayJob) []asynq.Option {
	opts := []asynq.Option{
		asynq.Queue(e.cfg.Queue),
		asynq.MaxRetry(e.cfg.MaxRetry),
		asynq.Timeout(e.cfg.Timeout),
	}
	taskID := e.taskID(job)
	if taskID != "" {
		opts = append(opts, asynq.TaskID(taskID))
	}
	if job.IdempotencyKey != "" && e.cfg.UniqueTTL >= time.Second {
		opts = append(opts, asynq.Unique(e.cfg.UniqueTTL))
	}
	return opts
}

func (e *ReplayEnqueuer) Config() ReplayEnqueueConfig {
	return e.cfg
}

func (e *ReplayEnqueuer) taskID(job store.ReplayJob) string {
	if job.IdempotencyKey == "" {
		return e.cfg.TaskIDPrefix + job.ID
	}
	key := job.IdempotencyKey
	if len(key) > 96 {
		sum := sha256.Sum256([]byte(key))
		key = hex.EncodeToString(sum[:])
	}
	return e.cfg.TaskIDPrefix + key
}

func (c ReplayEnqueueConfig) withDefaults() ReplayEnqueueConfig {
	if c.Queue == "" {
		c.Queue = DefaultQueue
	}
	if c.MaxRetry == 0 {
		c.MaxRetry = 3
	}
	if c.Timeout == 0 {
		c.Timeout = 30 * time.Minute
	}
	if c.UniqueTTL == 0 {
		c.UniqueTTL = 24 * time.Hour
	}
	if c.TaskIDPrefix == "" {
		c.TaskIDPrefix = "replay-job:"
	}
	if c.RetryBackoff == 0 {
		c.RetryBackoff = 15 * time.Second
	}
	if c.DeadLetterName == "" {
		// Asynq's Redis-side dead letter set is the archive; the store mirrors terminal failures as status=dlq.
		c.DeadLetterName = "asynq-archive"
	}
	return c
}
