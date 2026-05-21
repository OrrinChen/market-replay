package dashboard

import (
	"context"
	"errors"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/orynwilder/market-replay-service/internal/store"
)

type Handler struct {
	repo      store.Repository
	templates *template.Template
}

type pageData struct {
	Title        string
	Datasets     []datasetRow
	Jobs         []jobRow
	Job          jobDetail
	Errors       []validationErrorRow
	ErrorSummary []store.ValidationErrorSummary
	Metrics      []metricRow
	Summary      metricsSummary
	Lineage      store.DatasetLineage
	GeneratedAt  time.Time
}

type datasetRow struct {
	Dataset store.Dataset
	Files   []store.EventFile
}

type jobRow struct {
	Job     store.ReplayJob
	Dataset string
	File    string
}

type jobDetail struct {
	Job     store.ReplayJob
	Dataset store.Dataset
	File    store.EventFile
	Metrics []store.ReplayMetric
	Errors  []store.ValidationError
}

type validationErrorRow struct {
	Error store.ValidationError
	Job   store.ReplayJob
}

type metricRow struct {
	Metric store.ReplayMetric
	Job    store.ReplayJob
}

type metricsSummary struct {
	TotalJobs        int
	CompletedJobs    int
	FailedJobs       int
	TotalRows        int64
	TotalEvents      int64
	MalformedEvents  int64
	SequenceGaps     int64
	RowsPerSecond    float64
	EventsPerSecond  float64
	PeakAllocMB      float64
	AverageP95Millis float64
}

func New(repo store.Repository) http.Handler {
	return &Handler{
		repo:      repo,
		templates: template.Must(parseTemplates()),
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	path := strings.TrimSuffix(r.URL.Path, "/")
	switch {
	case path == "" || path == "/dashboard":
		http.Redirect(w, r, "/dashboard/jobs", http.StatusFound)
	case path == "/dashboard/datasets":
		h.renderDatasets(w, r)
	case strings.HasPrefix(path, "/dashboard/datasets/") && strings.HasSuffix(path, "/lineage"):
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/dashboard/datasets/"), "/lineage")
		h.renderDatasetLineage(w, r, id)
	case path == "/dashboard/jobs":
		h.renderJobs(w, r)
	case strings.HasPrefix(path, "/dashboard/jobs/"):
		h.renderJobDetail(w, r, strings.TrimPrefix(path, "/dashboard/jobs/"))
	case path == "/dashboard/validation-errors":
		h.renderValidationErrors(w, r)
	case path == "/dashboard/metrics":
		h.renderMetrics(w, r, "metrics.html", "Metrics Summary")
	case path == "/dashboard/benchmark":
		h.renderMetrics(w, r, "benchmark.html", "Benchmark Report")
	default:
		http.NotFound(w, r)
	}
}

func (h *Handler) renderDatasets(w http.ResponseWriter, r *http.Request) {
	datasets, err := h.repo.ListDatasets(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	rows := make([]datasetRow, 0, len(datasets))
	for _, dataset := range datasets {
		files, err := h.repo.ListEventFiles(r.Context(), dataset.ID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		rows = append(rows, datasetRow{Dataset: dataset, Files: files})
	}
	h.render(w, "datasets.html", pageData{Title: "Datasets", Datasets: rows, GeneratedAt: time.Now().UTC()})
}

func (h *Handler) renderDatasetLineage(w http.ResponseWriter, r *http.Request, id string) {
	lineage, err := store.BuildDatasetLineage(r.Context(), h.repo, id)
	if errors.Is(err, store.ErrNotFound) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.render(w, "dataset_lineage.html", pageData{
		Title:       "Dataset Lineage",
		Lineage:     lineage,
		GeneratedAt: time.Now().UTC(),
	})
}

func (h *Handler) renderJobs(w http.ResponseWriter, r *http.Request) {
	_, rows, err := h.loadJobRows(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.render(w, "jobs.html", pageData{Title: "Replay Jobs", Jobs: rows, GeneratedAt: time.Now().UTC()})
}

func (h *Handler) renderJobDetail(w http.ResponseWriter, r *http.Request, id string) {
	job, err := h.repo.GetReplayJob(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	dataset, _ := h.repo.GetDataset(r.Context(), job.DatasetID)
	file, _ := h.repo.GetEventFile(r.Context(), job.EventFileID)
	metrics, err := h.repo.ListReplayMetrics(r.Context(), job.ID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	validationErrors, err := h.repo.ListValidationErrors(r.Context(), job.ID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.render(w, "job_detail.html", pageData{
		Title:       "Job Detail",
		Job:         jobDetail{Job: job, Dataset: dataset, File: file, Metrics: metrics, Errors: validationErrors},
		GeneratedAt: time.Now().UTC(),
	})
}

func (h *Handler) renderValidationErrors(w http.ResponseWriter, r *http.Request) {
	jobs, err := h.repo.ListReplayJobs(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	rows := make([]validationErrorRow, 0)
	for _, job := range jobs {
		errs, err := h.repo.ListValidationErrors(r.Context(), job.ID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		for _, validationError := range errs {
			rows = append(rows, validationErrorRow{Error: validationError, Job: job})
		}
	}
	summary, err := store.BuildValidationErrorSummary(r.Context(), h.repo)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.render(w, "validation_errors.html", pageData{
		Title:        "Validation Errors",
		Errors:       rows,
		ErrorSummary: summary,
		GeneratedAt:  time.Now().UTC(),
	})
}

func (h *Handler) renderMetrics(w http.ResponseWriter, r *http.Request, templateName string, title string) {
	jobs, metricRows, err := h.loadMetrics(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.render(w, templateName, pageData{
		Title:       title,
		Metrics:     metricRows,
		Summary:     summarizeMetrics(jobs, metricRows),
		GeneratedAt: time.Now().UTC(),
	})
}

func (h *Handler) loadDatasetsAndJobs(ctx context.Context) ([]store.Dataset, []store.ReplayJob, error) {
	datasets, err := h.repo.ListDatasets(ctx)
	if err != nil {
		return nil, nil, err
	}
	jobs, err := h.repo.ListReplayJobs(ctx)
	if err != nil {
		return nil, nil, err
	}
	return datasets, jobs, nil
}

func (h *Handler) loadJobRows(ctx context.Context) ([]store.ReplayJob, []jobRow, error) {
	jobs, err := h.repo.ListReplayJobs(ctx)
	if err != nil {
		return nil, nil, err
	}
	rows := make([]jobRow, 0, len(jobs))
	for _, job := range jobs {
		row := jobRow{Job: job}
		if dataset, err := h.repo.GetDataset(ctx, job.DatasetID); err == nil {
			row.Dataset = dataset.Name
		}
		if file, err := h.repo.GetEventFile(ctx, job.EventFileID); err == nil {
			row.File = file.Path
		}
		rows = append(rows, row)
	}
	return jobs, rows, nil
}

func (h *Handler) loadMetrics(ctx context.Context) ([]store.ReplayJob, []metricRow, error) {
	jobs, err := h.repo.ListReplayJobs(ctx)
	if err != nil {
		return nil, nil, err
	}
	rows := make([]metricRow, 0)
	for _, job := range jobs {
		metrics, err := h.repo.ListReplayMetrics(ctx, job.ID)
		if err != nil {
			return nil, nil, err
		}
		for _, metric := range metrics {
			rows = append(rows, metricRow{Metric: metric, Job: job})
		}
	}
	return jobs, rows, nil
}

func summarizeMetrics(jobs []store.ReplayJob, rows []metricRow) metricsSummary {
	var summary metricsSummary
	summary.TotalJobs = len(jobs)
	for _, job := range jobs {
		switch job.Status {
		case store.JobStatusCompleted:
			summary.CompletedJobs++
		case store.JobStatusFailed, store.JobStatusDLQ:
			summary.FailedJobs++
		}
	}
	for _, row := range rows {
		metric := row.Metric
		summary.TotalRows += metric.Rows
		summary.TotalEvents += metric.Events
		summary.MalformedEvents += metric.MalformedEvents
		summary.SequenceGaps += metric.SequenceGaps
		summary.RowsPerSecond += metric.RowsPerSecond
		summary.EventsPerSecond += metric.EventsPerSecond
		if allocMB := float64(metric.PeakAllocBytes) / (1024 * 1024); allocMB > summary.PeakAllocMB {
			summary.PeakAllocMB = allocMB
		}
		summary.AverageP95Millis += float64(metric.P95Latency.Microseconds()) / 1000
	}
	if len(rows) > 0 {
		summary.RowsPerSecond = summary.RowsPerSecond / float64(len(rows))
		summary.EventsPerSecond = summary.EventsPerSecond / float64(len(rows))
		summary.AverageP95Millis = summary.AverageP95Millis / float64(len(rows))
	}
	return summary
}

func (h *Handler) render(w http.ResponseWriter, name string, data pageData) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := h.templates.ExecuteTemplate(w, name, data); err != nil {
		http.Error(w, fmt.Sprintf("render dashboard: %v", err), http.StatusInternalServerError)
	}
}

func parseTemplates() (*template.Template, error) {
	root, err := findRepoRoot()
	if err != nil {
		return nil, err
	}
	templateFS := os.DirFS(filepath.Join(root, "web", "templates"))
	return parseTemplatesFS(templateFS)
}

func parseTemplatesFS(templateFS fs.FS) (*template.Template, error) {
	funcs := template.FuncMap{
		"bytesMB": func(bytes uint64) float64 { return float64(bytes) / (1024 * 1024) },
		"millis":  func(duration time.Duration) float64 { return float64(duration.Microseconds()) / 1000 },
	}
	return template.New("dashboard").Funcs(funcs).ParseFS(templateFS, "*.html")
}

func findRepoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "web", "templates")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", fmt.Errorf("web/templates directory not found")
}
