package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/hibiken/asynq"
	"github.com/orynwilder/market-replay-service/internal/event"
	"github.com/orynwilder/market-replay-service/internal/observability"
	"github.com/orynwilder/market-replay-service/internal/parser"
	jobqueue "github.com/orynwilder/market-replay-service/internal/queue"
	"github.com/orynwilder/market-replay-service/internal/replay"
	"github.com/orynwilder/market-replay-service/internal/store"
	"github.com/orynwilder/market-replay-service/internal/validate"
	"go.uber.org/zap"
)

type ReplayHandlerConfig struct {
	DeadLetterName  string
	CheckpointEvery int64
	Queue           string
	Metrics         *observability.Metrics
}

type ReplayHandler struct {
	repo          store.Repository
	logger        *zap.Logger
	cfg           ReplayHandlerConfig
	activeWorkers atomic.Int64
}

func NewReplayHandler(repo store.Repository, logger *zap.Logger, cfg ReplayHandlerConfig) *ReplayHandler {
	if logger == nil {
		logger = zap.NewNop()
	}
	if cfg.DeadLetterName == "" {
		// Asynq archives exhausted tasks in Redis; store.JobStatusDLQ mirrors that control-plane terminal state.
		cfg.DeadLetterName = "asynq-archive"
	}
	if cfg.CheckpointEvery <= 0 {
		cfg.CheckpointEvery = 256
	}
	if cfg.Queue == "" {
		cfg.Queue = jobqueue.DefaultQueue
	}
	return &ReplayHandler{repo: repo, logger: logger, cfg: cfg}
}

func (h *ReplayHandler) ProcessTask(ctx context.Context, task *asynq.Task) error {
	if task.Type() != jobqueue.TypeReplayJob {
		return fmt.Errorf("%w: unsupported task type %q", asynq.SkipRetry, task.Type())
	}

	payload, err := jobqueue.DecodeReplayPayload(task.Payload())
	if err != nil {
		h.logger.Warn("dropping invalid replay payload", zap.Error(err))
		return fmt.Errorf("%w: %v", asynq.SkipRetry, err)
	}

	job, err := h.repo.GetReplayJob(ctx, payload.JobID)
	if err != nil {
		h.logger.Warn("dropping replay task for missing job", zap.String("job_id", payload.JobID), zap.Error(err))
		return fmt.Errorf("%w: %v", asynq.SkipRetry, err)
	}
	if job.Status == store.JobStatusCanceled {
		h.logger.Info("replay job canceled before execution", zap.String("job_id", job.ID))
		return nil
	}
	if job.Status == store.JobStatusCompleted || job.Status == store.JobStatusDLQ {
		h.logger.Info("replay job already terminal", zap.String("job_id", job.ID), zap.String("status", string(job.Status)))
		return nil
	}
	if h.cfg.Metrics != nil && !job.CreatedAt.IsZero() {
		h.cfg.Metrics.RecordJobQueueLag(h.cfg.Queue, time.Since(job.CreatedAt))
	}

	if _, err := h.repo.UpdateReplayJobStatus(ctx, job.ID, store.JobStatusRunning, ""); err != nil {
		return err
	}
	h.addActiveWorker(1)
	defer h.addActiveWorker(-1)
	h.logger.Info("replay job started", zap.String("job_id", job.ID), zap.String("event_file_id", job.EventFileID))

	if err := h.run(ctx, job); err != nil {
		return h.failJob(ctx, job.ID, err)
	}
	h.logger.Info("replay job completed", zap.String("job_id", job.ID))
	return nil
}

func (h *ReplayHandler) run(ctx context.Context, job store.ReplayJob) error {
	latest, err := h.repo.GetReplayJob(ctx, job.ID)
	if err != nil {
		return err
	}
	if latest.Status == store.JobStatusCanceled {
		return nil
	}

	file, err := h.repo.GetEventFile(ctx, job.EventFileID)
	if err != nil {
		return err
	}
	format, err := parser.ResolveFormat(file.Path, parser.Format(file.Format))
	if err != nil {
		return err
	}
	symbol := job.Symbol
	if symbol == "" {
		symbol = file.Symbol
	}

	metric, failures, err := h.processFile(ctx, latest, file.Path, format, symbol)
	if err != nil {
		return err
	}
	if err := h.repo.UpdateReplayCheckpoint(ctx, job.ID, metric.Rows); err != nil {
		return err
	}
	_, err = h.repo.CompleteReplayJob(ctx, job.ID, store.CompleteReplayJobParams{
		Metric: metric,
		Errors: validationErrors(job.ID, failures),
	})
	if err != nil {
		return err
	}
	h.recordReplayMetrics(ctx, job, metric, symbol, store.JobStatusCompleted)
	return nil
}

func (h *ReplayHandler) addActiveWorker(delta int64) {
	if h.cfg.Metrics == nil {
		return
	}
	count := h.activeWorkers.Add(delta)
	h.cfg.Metrics.SetActiveWorkers(jobqueue.TypeReplayJob, float64(count))
}

func (h *ReplayHandler) recordReplayMetrics(ctx context.Context, job store.ReplayJob, metric store.ReplayMetric, symbol string, status store.JobStatus) {
	if h.cfg.Metrics == nil {
		return
	}
	datasetLabel := job.DatasetID
	if dataset, err := h.repo.GetDataset(ctx, job.DatasetID); err == nil && dataset.Name != "" {
		datasetLabel = dataset.Name
	}
	h.cfg.Metrics.RecordReplayBenchmark(observability.ReplayLabels{
		Dataset: datasetLabel,
		Symbol:  symbol,
		Status:  string(status),
	}, event.ReplayMetric{
		Rows:            metric.Rows,
		Events:          metric.Events,
		MalformedEvents: metric.MalformedEvents,
		SequenceGaps:    metric.SequenceGaps,
		Duration:        metric.Duration,
		RowsPerSecond:   metric.RowsPerSecond,
		EventsPerSecond: metric.EventsPerSecond,
		P95Latency:      metric.P95Latency,
		PeakAllocBytes:  metric.PeakAllocBytes,
		AllocsPerEvent:  metric.AllocsPerEvent,
		ProcessedAt:     metric.CreatedAt,
	})
}

func (h *ReplayHandler) processFile(ctx context.Context, job store.ReplayJob, path string, format parser.Format, symbol string) (store.ReplayMetric, []event.ValidationFailure, error) {
	stream, err := parser.Open(path, format)
	if err != nil {
		return store.ReplayMetric{}, nil, err
	}
	defer stream.Close()

	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	peakAlloc := before.Alloc
	started := time.Now()
	validator := validate.NewValidator(validate.Options{Symbol: symbol})
	lastLine := int64(0)
	for {
		if err := ctx.Err(); err != nil {
			return store.ReplayMetric{}, nil, err
		}
		record, err := stream.Next()
		if err == io.EOF {
			result := validator.Result()
			rows := result.Rows
			if lastLine > rows {
				rows = lastLine
			}
			duration := time.Since(started)
			if duration <= 0 {
				duration = time.Nanosecond
			}
			var after runtime.MemStats
			runtime.ReadMemStats(&after)
			if after.Alloc > peakAlloc {
				peakAlloc = after.Alloc
			}
			allocsPerEvent := 0.0
			if result.Events > 0 && after.Mallocs >= before.Mallocs {
				allocsPerEvent = float64(after.Mallocs-before.Mallocs) / float64(result.Events)
			}
			return store.ReplayMetric{
				Rows:            rows,
				Events:          result.Events,
				MalformedEvents: result.MalformedEvents,
				SequenceGaps:    result.SequenceGaps,
				Duration:        duration,
				RowsPerSecond:   float64(rows) / duration.Seconds(),
				EventsPerSecond: float64(result.Events) / duration.Seconds(),
				PeakAllocBytes:  peakAlloc,
				AllocsPerEvent:  allocsPerEvent,
				CreatedAt:       time.Now().UTC(),
			}, result.Failures, nil
		}
		if err != nil {
			return store.ReplayMetric{}, nil, err
		}
		lastLine = record.Line
		if record.Line <= job.CheckpointLine {
			validator.Prime(record)
			continue
		}
		validator.Process(record)
		if record.Line > job.CheckpointLine && record.Line%h.cfg.CheckpointEvery == 0 {
			if err := h.repo.UpdateReplayCheckpoint(ctx, job.ID, record.Line); err != nil {
				return store.ReplayMetric{}, nil, err
			}
			current, err := h.repo.GetReplayJob(ctx, job.ID)
			if err != nil {
				return store.ReplayMetric{}, nil, err
			}
			if current.Status == store.JobStatusCanceled {
				return store.ReplayMetric{}, nil, context.Canceled
			}
			var currentMem runtime.MemStats
			runtime.ReadMemStats(&currentMem)
			if currentMem.Alloc > peakAlloc {
				peakAlloc = currentMem.Alloc
			}
		}
	}
}

func processFile(path string, format parser.Format, symbol, speedValue string) (store.ReplayMetric, []event.ValidationFailure, error) {
	if speedValue == "" {
		speedValue = string(replay.SpeedMax)
	}
	speed, err := replay.ParseSpeed(speedValue)
	if err != nil {
		return store.ReplayMetric{}, nil, err
	}

	validation, err := validate.File(path, format, validate.Options{Symbol: symbol})
	if err != nil {
		return store.ReplayMetric{}, nil, err
	}

	if speed == replay.SpeedMax {
		handler := NewReplayHandler(store.NewMemoryRepository(), nil, ReplayHandlerConfig{CheckpointEvery: 1<<62 - 1})
		metric, failures, err := handler.processFile(context.Background(), store.ReplayJob{ID: "local"}, path, format, symbol)
		if err != nil {
			return store.ReplayMetric{}, nil, err
		}
		return metric, failures, nil
	}

	started := time.Now()
	summary, err := replay.File(path, format, symbol, speed)
	if err != nil {
		return store.ReplayMetric{}, nil, err
	}
	duration := summary.Duration
	if duration <= 0 {
		duration = time.Since(started)
	}
	if duration <= 0 {
		duration = time.Nanosecond
	}
	return store.ReplayMetric{
		Rows:            summary.Rows,
		Events:          summary.Events,
		MalformedEvents: summary.MalformedEvents,
		SequenceGaps:    summary.SequenceGaps,
		Duration:        duration,
		RowsPerSecond:   float64(summary.Rows) / duration.Seconds(),
		EventsPerSecond: float64(summary.Events) / duration.Seconds(),
		CreatedAt:       time.Now().UTC(),
	}, validation.Failures, nil
}

func storeMetric(metric event.ReplayMetric) store.ReplayMetric {
	return store.ReplayMetric{
		Rows:            metric.Rows,
		Events:          metric.Events,
		MalformedEvents: metric.MalformedEvents,
		SequenceGaps:    metric.SequenceGaps,
		Duration:        metric.Duration,
		RowsPerSecond:   metric.RowsPerSecond,
		EventsPerSecond: metric.EventsPerSecond,
		P95Latency:      metric.P95Latency,
		PeakAllocBytes:  metric.PeakAllocBytes,
		AllocsPerEvent:  metric.AllocsPerEvent,
		CreatedAt:       metric.ProcessedAt,
	}
}

func validationErrors(jobID string, failures []event.ValidationFailure) []store.ValidationError {
	errs := make([]store.ValidationError, 0, len(failures))
	for _, failure := range failures {
		errs = append(errs, store.ValidationError{
			JobID:   jobID,
			Line:    failure.Line,
			Symbol:  failure.Symbol,
			Type:    failure.Type,
			Message: failure.Message,
		})
	}
	return errs
}

func (h *ReplayHandler) failJob(ctx context.Context, jobID string, cause error) error {
	status := store.JobStatusFailed
	retried, retriedOK := asynq.GetRetryCount(ctx)
	maxRetry, maxRetryOK := asynq.GetMaxRetry(ctx)
	if retriedOK && maxRetryOK && retried >= maxRetry {
		status = store.JobStatusDLQ
	}
	if _, err := h.repo.UpdateReplayJobStatus(ctx, jobID, status, cause.Error()); err != nil {
		h.logger.Error("failed to persist replay job failure", zap.String("job_id", jobID), zap.Error(err))
		return err
	}
	h.logger.Error("replay job failed",
		zap.String("job_id", jobID),
		zap.String("status", string(status)),
		zap.String("dead_letter", h.cfg.DeadLetterName),
		zap.Error(cause),
	)
	if status == store.JobStatusDLQ {
		if h.cfg.Metrics != nil {
			h.cfg.Metrics.RecordWorkerDLQ(h.cfg.Queue, "retry_exhausted")
		}
		return fmt.Errorf("%w: %v", asynq.SkipRetry, cause)
	}
	if errors.Is(cause, context.Canceled) {
		return fmt.Errorf("%w: %v", asynq.SkipRetry, cause)
	}
	return cause
}
