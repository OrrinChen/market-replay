package observability

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

func (m *Metrics) GinLatencyMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		started := time.Now()
		c.Next()

		route := c.FullPath()
		if route == "" {
			route = c.Request.URL.Path
		}
		m.apiRequestDuration.WithLabelValues(
			c.Request.Method,
			route,
			strconv.Itoa(c.Writer.Status()),
		).Observe(time.Since(started).Seconds())
	}
}

func GinLatencyMiddleware(metrics *Metrics) gin.HandlerFunc {
	return metrics.GinLatencyMiddleware()
}
