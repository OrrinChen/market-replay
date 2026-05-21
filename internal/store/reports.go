package store

import (
	"context"
	"sort"
	"time"
)

type DatasetLineage struct {
	Dataset     Dataset            `json:"dataset"`
	EventFiles  []EventFileLineage `json:"event_files"`
	GeneratedAt time.Time          `json:"generated_at"`
}

type EventFileLineage struct {
	EventFile EventFile          `json:"event_file"`
	Jobs      []ReplayJobLineage `json:"jobs"`
}

type ReplayJobLineage struct {
	Job         ReplayJob `json:"job"`
	MetricCount int       `json:"metric_count"`
	ErrorCount  int       `json:"error_count"`
	ErrorTypes  []string  `json:"error_types,omitempty"`
}

type ReplayQualityReport struct {
	Dataset      Dataset                  `json:"dataset"`
	EventFile    EventFile                `json:"event_file"`
	Job          ReplayJob                `json:"job"`
	Metrics      []ReplayMetric           `json:"metrics"`
	Errors       []ValidationError        `json:"errors"`
	ErrorSummary []ValidationErrorSummary `json:"error_summary"`
	GeneratedAt  time.Time                `json:"generated_at"`
}

type ValidationErrorSummary struct {
	Type        string    `json:"type"`
	Symbol      string    `json:"symbol,omitempty"`
	DatasetID   string    `json:"dataset_id,omitempty"`
	DatasetName string    `json:"dataset_name,omitempty"`
	EventFileID string    `json:"event_file_id,omitempty"`
	FilePath    string    `json:"file_path,omitempty"`
	Day         string    `json:"day,omitempty"`
	Count       int64     `json:"count"`
	FirstLine   int64     `json:"first_line"`
	LastLine    int64     `json:"last_line"`
	FirstSeenAt time.Time `json:"first_seen_at,omitempty"`
	LastSeenAt  time.Time `json:"last_seen_at,omitempty"`
}

func BuildDatasetLineage(ctx context.Context, repo Repository, datasetID string) (DatasetLineage, error) {
	dataset, err := repo.GetDataset(ctx, datasetID)
	if err != nil {
		return DatasetLineage{}, err
	}
	files, err := repo.ListEventFiles(ctx, datasetID)
	if err != nil {
		return DatasetLineage{}, err
	}
	jobs, err := repo.ListReplayJobs(ctx)
	if err != nil {
		return DatasetLineage{}, err
	}
	byFile := make(map[string][]ReplayJob, len(files))
	for _, job := range jobs {
		if job.DatasetID == datasetID {
			byFile[job.EventFileID] = append(byFile[job.EventFileID], job)
		}
	}

	lineage := DatasetLineage{
		Dataset:     dataset,
		EventFiles:  make([]EventFileLineage, 0, len(files)),
		GeneratedAt: time.Now().UTC(),
	}
	for _, file := range files {
		row := EventFileLineage{EventFile: file}
		for _, job := range byFile[file.ID] {
			metrics, err := repo.ListReplayMetrics(ctx, job.ID)
			if err != nil {
				return DatasetLineage{}, err
			}
			errs, err := repo.ListValidationErrors(ctx, job.ID)
			if err != nil {
				return DatasetLineage{}, err
			}
			row.Jobs = append(row.Jobs, ReplayJobLineage{
				Job:         job,
				MetricCount: len(metrics),
				ErrorCount:  len(errs),
				ErrorTypes:  validationErrorTypes(errs),
			})
		}
		sort.Slice(row.Jobs, func(i, j int) bool {
			return row.Jobs[i].Job.CreatedAt.Before(row.Jobs[j].Job.CreatedAt)
		})
		lineage.EventFiles = append(lineage.EventFiles, row)
	}
	sort.Slice(lineage.EventFiles, func(i, j int) bool {
		return lineage.EventFiles[i].EventFile.CreatedAt.Before(lineage.EventFiles[j].EventFile.CreatedAt)
	})
	return lineage, nil
}

func validationErrorTypes(errs []ValidationError) []string {
	seen := make(map[string]struct{})
	types := make([]string, 0)
	for _, err := range errs {
		if err.Type == "" {
			continue
		}
		if _, ok := seen[err.Type]; ok {
			continue
		}
		seen[err.Type] = struct{}{}
		types = append(types, err.Type)
	}
	sort.Strings(types)
	return types
}

func BuildReplayQualityReport(ctx context.Context, repo Repository, jobID string) (ReplayQualityReport, error) {
	job, err := repo.GetReplayJob(ctx, jobID)
	if err != nil {
		return ReplayQualityReport{}, err
	}
	dataset, err := repo.GetDataset(ctx, job.DatasetID)
	if err != nil {
		return ReplayQualityReport{}, err
	}
	file, err := repo.GetEventFile(ctx, job.EventFileID)
	if err != nil {
		return ReplayQualityReport{}, err
	}
	metrics, err := repo.ListReplayMetrics(ctx, job.ID)
	if err != nil {
		return ReplayQualityReport{}, err
	}
	errs, err := repo.ListValidationErrors(ctx, job.ID)
	if err != nil {
		return ReplayQualityReport{}, err
	}
	return ReplayQualityReport{
		Dataset:      dataset,
		EventFile:    file,
		Job:          job,
		Metrics:      metrics,
		Errors:       errs,
		ErrorSummary: summarizeValidationErrors(dataset, file, errs),
		GeneratedAt:  time.Now().UTC(),
	}, nil
}

func BuildValidationErrorSummary(ctx context.Context, repo Repository) ([]ValidationErrorSummary, error) {
	jobs, err := repo.ListReplayJobs(ctx)
	if err != nil {
		return nil, err
	}
	summaries := make(map[string]ValidationErrorSummary)
	for _, job := range jobs {
		dataset, err := repo.GetDataset(ctx, job.DatasetID)
		if err != nil {
			return nil, err
		}
		file, err := repo.GetEventFile(ctx, job.EventFileID)
		if err != nil {
			return nil, err
		}
		errs, err := repo.ListValidationErrors(ctx, job.ID)
		if err != nil {
			return nil, err
		}
		for _, item := range summarizeValidationErrors(dataset, file, errs) {
			key := item.Type + "\x00" + item.Symbol + "\x00" + item.EventFileID + "\x00" + item.Day
			existing := summaries[key]
			if existing.Count == 0 {
				summaries[key] = item
				continue
			}
			existing.Count += item.Count
			if item.FirstLine < existing.FirstLine || existing.FirstLine == 0 {
				existing.FirstLine = item.FirstLine
			}
			if item.LastLine > existing.LastLine {
				existing.LastLine = item.LastLine
			}
			if existing.FirstSeenAt.IsZero() || item.FirstSeenAt.Before(existing.FirstSeenAt) {
				existing.FirstSeenAt = item.FirstSeenAt
			}
			if item.LastSeenAt.After(existing.LastSeenAt) {
				existing.LastSeenAt = item.LastSeenAt
			}
			summaries[key] = existing
		}
	}
	rows := make([]ValidationErrorSummary, 0, len(summaries))
	for _, summary := range summaries {
		rows = append(rows, summary)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Count != rows[j].Count {
			return rows[i].Count > rows[j].Count
		}
		if rows[i].Type != rows[j].Type {
			return rows[i].Type < rows[j].Type
		}
		if rows[i].FilePath != rows[j].FilePath {
			return rows[i].FilePath < rows[j].FilePath
		}
		return rows[i].Symbol < rows[j].Symbol
	})
	return rows, nil
}

func summarizeValidationErrors(dataset Dataset, file EventFile, errs []ValidationError) []ValidationErrorSummary {
	summaries := make(map[string]ValidationErrorSummary)
	for _, validationError := range errs {
		day := ""
		if !validationError.CreatedAt.IsZero() {
			day = validationError.CreatedAt.UTC().Format("2006-01-02")
		}
		key := validationError.Type + "\x00" + validationError.Symbol + "\x00" + day
		summary := summaries[key]
		if summary.Count == 0 {
			summary = ValidationErrorSummary{
				Type:        validationError.Type,
				Symbol:      validationError.Symbol,
				DatasetID:   dataset.ID,
				DatasetName: dataset.Name,
				EventFileID: file.ID,
				FilePath:    file.Path,
				Day:         day,
				FirstLine:   validationError.Line,
				LastLine:    validationError.Line,
				FirstSeenAt: validationError.CreatedAt,
				LastSeenAt:  validationError.CreatedAt,
			}
		}
		summary.Count++
		if validationError.Line < summary.FirstLine || summary.FirstLine == 0 {
			summary.FirstLine = validationError.Line
		}
		if validationError.Line > summary.LastLine {
			summary.LastLine = validationError.Line
		}
		if summary.FirstSeenAt.IsZero() || validationError.CreatedAt.Before(summary.FirstSeenAt) {
			summary.FirstSeenAt = validationError.CreatedAt
		}
		if validationError.CreatedAt.After(summary.LastSeenAt) {
			summary.LastSeenAt = validationError.CreatedAt
		}
		summaries[key] = summary
	}
	rows := make([]ValidationErrorSummary, 0, len(summaries))
	for _, summary := range summaries {
		rows = append(rows, summary)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Count != rows[j].Count {
			return rows[i].Count > rows[j].Count
		}
		if rows[i].Type != rows[j].Type {
			return rows[i].Type < rows[j].Type
		}
		return rows[i].Symbol < rows[j].Symbol
	})
	return rows
}
