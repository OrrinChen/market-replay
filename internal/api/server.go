package api

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/orynwilder/market-replay-service/internal/store"
)

type errorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

type Server struct {
	repo         store.Repository
	submitReplay func(context.Context, store.CreateReplayJobParams) (store.ReplayJob, error)
}

func NewRouter(repo store.Repository) http.Handler {
	return NewRouterWithOptions(repo, Options{})
}

type Options struct {
	Middleware      []gin.HandlerFunc
	SubmitReplayJob func(context.Context, store.CreateReplayJobParams) (store.ReplayJob, error)
}

func NewRouterWithOptions(repo store.Repository, opts Options) http.Handler {
	gin.SetMode(gin.ReleaseMode)
	server := &Server{repo: repo, submitReplay: opts.SubmitReplayJob}
	router := gin.New()
	router.Use(gin.Recovery())
	for _, middleware := range opts.Middleware {
		router.Use(middleware)
	}

	router.GET("/healthz", server.healthz)
	router.POST("/datasets", server.createDataset)
	router.GET("/datasets", server.listDatasets)
	router.POST("/event-files", server.createEventFile)
	router.POST("/replay-jobs", server.createReplayJob)
	router.GET("/replay-jobs/:id", server.getReplayJob)
	router.GET("/replay-jobs/:id/metrics", server.listReplayMetrics)
	router.GET("/replay-jobs/:id/errors", server.listValidationErrors)
	router.POST("/replay-jobs/:id/cancel", server.cancelReplayJob)

	return router
}

func (s *Server) healthz(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (s *Server) createDataset(c *gin.Context) {
	var req store.CreateDatasetParams
	if !bindJSON(c, &req) {
		return
	}
	req.Name = strings.TrimSpace(req.Name)
	req.Description = strings.TrimSpace(req.Description)
	if req.Name == "" {
		writeError(c, http.StatusBadRequest, "invalid_request", "name is required")
		return
	}

	dataset, err := s.repo.CreateDataset(c.Request.Context(), req)
	if err != nil {
		writeStoreError(c, err)
		return
	}
	c.JSON(http.StatusCreated, dataset)
}

func (s *Server) listDatasets(c *gin.Context) {
	datasets, err := s.repo.ListDatasets(c.Request.Context())
	if err != nil {
		writeStoreError(c, err)
		return
	}
	c.JSON(http.StatusOK, datasets)
}

func (s *Server) createEventFile(c *gin.Context) {
	var req store.CreateEventFileParams
	if !bindJSON(c, &req) {
		return
	}
	req.DatasetID = strings.TrimSpace(req.DatasetID)
	req.Path = strings.TrimSpace(req.Path)
	req.Format = strings.TrimSpace(req.Format)
	req.Symbol = strings.TrimSpace(req.Symbol)
	if req.DatasetID == "" {
		writeError(c, http.StatusBadRequest, "invalid_request", "dataset_id is required")
		return
	}
	if req.Path == "" {
		writeError(c, http.StatusBadRequest, "invalid_request", "path is required")
		return
	}
	if req.Format == "" {
		writeError(c, http.StatusBadRequest, "invalid_request", "format is required")
		return
	}
	if req.Bytes < 0 {
		writeError(c, http.StatusBadRequest, "invalid_request", "bytes must be greater than or equal to zero")
		return
	}

	file, err := s.repo.CreateEventFile(c.Request.Context(), req)
	if err != nil {
		writeStoreError(c, err)
		return
	}
	c.JSON(http.StatusCreated, file)
}

func (s *Server) createReplayJob(c *gin.Context) {
	var req store.CreateReplayJobParams
	if !bindJSON(c, &req) {
		return
	}
	req.DatasetID = strings.TrimSpace(req.DatasetID)
	req.EventFileID = strings.TrimSpace(req.EventFileID)
	req.IdempotencyKey = strings.TrimSpace(req.IdempotencyKey)
	req.Symbol = strings.TrimSpace(req.Symbol)
	req.Speed = strings.TrimSpace(req.Speed)
	if req.DatasetID == "" {
		writeError(c, http.StatusBadRequest, "invalid_request", "dataset_id is required")
		return
	}
	if req.EventFileID == "" {
		writeError(c, http.StatusBadRequest, "invalid_request", "event_file_id is required")
		return
	}
	if req.Speed == "" {
		writeError(c, http.StatusBadRequest, "invalid_request", "speed is required")
		return
	}

	submit := s.repo.CreateReplayJob
	if s.submitReplay != nil {
		submit = s.submitReplay
	}
	job, err := submit(c.Request.Context(), req)
	if errors.Is(err, store.ErrAlreadyExists) {
		c.JSON(http.StatusOK, job)
		return
	}
	if err != nil {
		writeStoreError(c, err)
		return
	}
	c.JSON(http.StatusCreated, job)
}

func (s *Server) getReplayJob(c *gin.Context) {
	job, err := s.repo.GetReplayJob(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeStoreError(c, err)
		return
	}
	c.JSON(http.StatusOK, job)
}

func (s *Server) listReplayMetrics(c *gin.Context) {
	if _, ok := s.ensureJobExists(c); !ok {
		return
	}
	metrics, err := s.repo.ListReplayMetrics(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeStoreError(c, err)
		return
	}
	c.JSON(http.StatusOK, metrics)
}

func (s *Server) listValidationErrors(c *gin.Context) {
	if _, ok := s.ensureJobExists(c); !ok {
		return
	}
	errs, err := s.repo.ListValidationErrors(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeStoreError(c, err)
		return
	}
	c.JSON(http.StatusOK, errs)
}

func (s *Server) cancelReplayJob(c *gin.Context) {
	job, err := s.repo.CancelReplayJob(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeStoreError(c, err)
		return
	}
	c.JSON(http.StatusOK, job)
}

func (s *Server) ensureJobExists(c *gin.Context) (store.ReplayJob, bool) {
	job, err := s.repo.GetReplayJob(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeStoreError(c, err)
		return store.ReplayJob{}, false
	}
	return job, true
}

func bindJSON(c *gin.Context, target any) bool {
	if err := c.ShouldBindJSON(target); err != nil {
		writeError(c, http.StatusBadRequest, "invalid_json", "request body must be valid JSON")
		return false
	}
	return true
}

func writeStoreError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(c, http.StatusNotFound, "not_found", "requested resource was not found")
	case errors.Is(err, store.ErrAlreadyExists):
		writeError(c, http.StatusConflict, "already_exists", "resource already exists")
	default:
		writeError(c, http.StatusInternalServerError, "internal_error", "internal server error")
	}
}

func writeError(c *gin.Context, status int, code, message string) {
	c.JSON(status, errorResponse{Error: code, Message: message})
}
