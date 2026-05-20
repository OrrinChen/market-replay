package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/orynwilder/market-replay-service/internal/api"
	"github.com/orynwilder/market-replay-service/internal/store"
)

func TestAPIHappyPathWithMemoryRepository(t *testing.T) {
	repo := store.NewMemoryRepository()
	router := api.NewRouter(repo)

	health := doJSON(t, router, http.MethodGet, "/healthz", nil)
	if health.Code != http.StatusOK {
		t.Fatalf("GET /healthz status = %d, want %d", health.Code, http.StatusOK)
	}

	createDataset := doJSON(t, router, http.MethodPost, "/datasets", map[string]string{
		"name":        "binance-depth",
		"description": "depth fixture",
	})
	if createDataset.Code != http.StatusCreated {
		t.Fatalf("POST /datasets status = %d body = %s, want %d", createDataset.Code, createDataset.Body.String(), http.StatusCreated)
	}
	var dataset store.Dataset
	decodeJSON(t, createDataset, &dataset)
	if dataset.ID == "" || dataset.Name != "binance-depth" {
		t.Fatalf("dataset = %#v, want generated id and provided name", dataset)
	}

	listDatasets := doJSON(t, router, http.MethodGet, "/datasets", nil)
	if listDatasets.Code != http.StatusOK {
		t.Fatalf("GET /datasets status = %d body = %s, want %d", listDatasets.Code, listDatasets.Body.String(), http.StatusOK)
	}
	var datasets []store.Dataset
	decodeJSON(t, listDatasets, &datasets)
	if len(datasets) != 1 || datasets[0].ID != dataset.ID {
		t.Fatalf("datasets = %#v, want created dataset", datasets)
	}

	createFile := doJSON(t, router, http.MethodPost, "/event-files", map[string]any{
		"dataset_id": dataset.ID,
		"path":       "testdata/btcusdt_depth.jsonl",
		"format":     "jsonl",
		"symbol":     "BTCUSDT",
		"bytes":      1024,
	})
	if createFile.Code != http.StatusCreated {
		t.Fatalf("POST /event-files status = %d body = %s, want %d", createFile.Code, createFile.Body.String(), http.StatusCreated)
	}
	var file store.EventFile
	decodeJSON(t, createFile, &file)
	if file.ID == "" || file.DatasetID != dataset.ID {
		t.Fatalf("event file = %#v, want generated id and dataset linkage", file)
	}

	createJob := doJSON(t, router, http.MethodPost, "/replay-jobs", map[string]string{
		"dataset_id":      dataset.ID,
		"event_file_id":   file.ID,
		"idempotency_key": "same-job",
		"symbol":          "BTCUSDT",
		"speed":           "max",
	})
	if createJob.Code != http.StatusCreated {
		t.Fatalf("POST /replay-jobs status = %d body = %s, want %d", createJob.Code, createJob.Body.String(), http.StatusCreated)
	}
	var job store.ReplayJob
	decodeJSON(t, createJob, &job)
	if job.ID == "" || job.Status != store.JobStatusQueued {
		t.Fatalf("job = %#v, want generated queued job", job)
	}

	getJob := doJSON(t, router, http.MethodGet, "/replay-jobs/"+job.ID, nil)
	if getJob.Code != http.StatusOK {
		t.Fatalf("GET /replay-jobs/:id status = %d body = %s, want %d", getJob.Code, getJob.Body.String(), http.StatusOK)
	}
	var fetched store.ReplayJob
	decodeJSON(t, getJob, &fetched)
	if fetched.ID != job.ID {
		t.Fatalf("fetched job id = %q, want %q", fetched.ID, job.ID)
	}

	metrics := doJSON(t, router, http.MethodGet, "/replay-jobs/"+job.ID+"/metrics", nil)
	if metrics.Code != http.StatusOK {
		t.Fatalf("GET metrics status = %d body = %s, want %d", metrics.Code, metrics.Body.String(), http.StatusOK)
	}
	var metricItems []store.ReplayMetric
	decodeJSON(t, metrics, &metricItems)
	if len(metricItems) != 0 {
		t.Fatalf("metrics = %#v, want empty slice for new job", metricItems)
	}

	errorsResp := doJSON(t, router, http.MethodGet, "/replay-jobs/"+job.ID+"/errors", nil)
	if errorsResp.Code != http.StatusOK {
		t.Fatalf("GET errors status = %d body = %s, want %d", errorsResp.Code, errorsResp.Body.String(), http.StatusOK)
	}
	var errorItems []store.ValidationError
	decodeJSON(t, errorsResp, &errorItems)
	if len(errorItems) != 0 {
		t.Fatalf("validation errors = %#v, want empty slice for new job", errorItems)
	}

	cancel := doJSON(t, router, http.MethodPost, "/replay-jobs/"+job.ID+"/cancel", nil)
	if cancel.Code != http.StatusOK {
		t.Fatalf("POST cancel status = %d body = %s, want %d", cancel.Code, cancel.Body.String(), http.StatusOK)
	}
	var canceled store.ReplayJob
	decodeJSON(t, cancel, &canceled)
	if canceled.Status != store.JobStatusCanceled {
		t.Fatalf("canceled status = %q, want %q", canceled.Status, store.JobStatusCanceled)
	}
}

func TestAPIDuplicateReplayJobIdempotencyReturnsOriginalJob(t *testing.T) {
	repo := store.NewMemoryRepository()
	router := api.NewRouter(repo)

	dataset := createDataset(t, router)
	file := createEventFile(t, router, dataset.ID)
	body := map[string]string{
		"dataset_id":      dataset.ID,
		"event_file_id":   file.ID,
		"idempotency_key": "same-job",
		"speed":           "max",
	}

	firstResp := doJSON(t, router, http.MethodPost, "/replay-jobs", body)
	if firstResp.Code != http.StatusCreated {
		t.Fatalf("first POST /replay-jobs status = %d body = %s, want %d", firstResp.Code, firstResp.Body.String(), http.StatusCreated)
	}
	var first store.ReplayJob
	decodeJSON(t, firstResp, &first)

	secondResp := doJSON(t, router, http.MethodPost, "/replay-jobs", body)
	if secondResp.Code != http.StatusOK {
		t.Fatalf("duplicate POST /replay-jobs status = %d body = %s, want %d", secondResp.Code, secondResp.Body.String(), http.StatusOK)
	}
	var second store.ReplayJob
	decodeJSON(t, secondResp, &second)
	if second.ID != first.ID {
		t.Fatalf("duplicate job id = %q, want original %q", second.ID, first.ID)
	}
}

func TestAPIValidationErrorsAreClearJSON(t *testing.T) {
	router := api.NewRouter(store.NewMemoryRepository())

	resp := doJSON(t, router, http.MethodPost, "/datasets", map[string]string{"description": "missing name"})
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("POST /datasets status = %d body = %s, want %d", resp.Code, resp.Body.String(), http.StatusBadRequest)
	}
	var payload map[string]any
	decodeJSON(t, resp, &payload)
	if payload["error"] == "" || payload["message"] == "" {
		t.Fatalf("error payload = %#v, want error and message fields", payload)
	}
}

func createDataset(t *testing.T, router http.Handler) store.Dataset {
	t.Helper()
	resp := doJSON(t, router, http.MethodPost, "/datasets", map[string]string{"name": "fixture"})
	if resp.Code != http.StatusCreated {
		t.Fatalf("POST /datasets status = %d body = %s, want %d", resp.Code, resp.Body.String(), http.StatusCreated)
	}
	var dataset store.Dataset
	decodeJSON(t, resp, &dataset)
	return dataset
}

func createEventFile(t *testing.T, router http.Handler, datasetID string) store.EventFile {
	t.Helper()
	resp := doJSON(t, router, http.MethodPost, "/event-files", map[string]string{
		"dataset_id": datasetID,
		"path":       "testdata/btcusdt_depth.jsonl",
		"format":     "jsonl",
	})
	if resp.Code != http.StatusCreated {
		t.Fatalf("POST /event-files status = %d body = %s, want %d", resp.Code, resp.Body.String(), http.StatusCreated)
	}
	var file store.EventFile
	decodeJSON(t, resp, &file)
	return file
}

func doJSON(t *testing.T, handler http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Reader
	if body == nil {
		reader = bytes.NewReader(nil)
	} else {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
		reader = bytes.NewReader(encoded)
	}
	req := httptest.NewRequest(method, path, reader)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	return resp
}

func decodeJSON(t *testing.T, resp *httptest.ResponseRecorder, target any) {
	t.Helper()
	if err := json.Unmarshal(resp.Body.Bytes(), target); err != nil {
		t.Fatalf("decode response %q: %v", resp.Body.String(), err)
	}
}
