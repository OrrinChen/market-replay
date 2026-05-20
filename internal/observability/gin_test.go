package observability

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestGinLatencyMiddlewareRecordsRouteStatusAndDuration(t *testing.T) {
	gin.SetMode(gin.TestMode)
	registry := prometheus.NewRegistry()
	metrics, err := Register(registry)
	if err != nil {
		t.Fatalf("register metrics: %v", err)
	}

	router := gin.New()
	router.Use(metrics.GinLatencyMiddleware())
	router.GET("/jobs/:id", func(c *gin.Context) {
		c.String(http.StatusAccepted, "queued")
	})

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/jobs/abc", nil)
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusAccepted)
	}

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	metric := findMetricWithLabels(t, families, "api_request_duration_seconds", map[string]string{
		"method": "GET",
		"route":  "/jobs/:id",
		"status": "202",
	})
	if metric.Histogram == nil {
		t.Fatalf("api_request_duration_seconds metric was not a histogram")
	}
	if metric.Histogram.GetSampleCount() != 1 {
		t.Fatalf("sample count = %d, want 1", metric.Histogram.GetSampleCount())
	}
}

func findMetricWithLabels(t *testing.T, families []*dto.MetricFamily, name string, labels map[string]string) *dto.Metric {
	t.Helper()
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if hasLabels(metric, labels) {
				return metric
			}
		}
	}
	t.Fatalf("metric %s with labels %#v was not gathered", name, labels)
	return nil
}

func hasLabels(metric *dto.Metric, labels map[string]string) bool {
	seen := make(map[string]string, len(metric.GetLabel()))
	for _, label := range metric.GetLabel() {
		seen[label.GetName()] = label.GetValue()
	}
	for name, want := range labels {
		if seen[name] != want {
			return false
		}
	}
	return true
}
