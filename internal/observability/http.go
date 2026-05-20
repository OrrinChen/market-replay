package observability

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func HandlerFor(registry *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
}

func RegisterMetricsHandler(mux *http.ServeMux, pattern string, registry *prometheus.Registry) {
	if pattern == "" {
		pattern = "/metrics"
	}
	mux.Handle(pattern, HandlerFor(registry))
}
