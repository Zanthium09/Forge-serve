// Package metrics holds the gateway's Prometheus instruments.
// Package-level promauto vars: simple, and there is exactly one gateway
// process — no need for injected registries yet.
package metrics

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	requestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "forge_requests_total",
		Help: "Chat completion requests by HTTP status code.",
	}, []string{"code"})

	// Buckets 50ms..~200s: TTFT on CPU spans stub-fast to queued-slow.
	ttftSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "forge_ttft_seconds",
		Help:    "Time to first token.",
		Buckets: prometheus.ExponentialBuckets(0.05, 2, 12),
	})

	tokensPerSecond = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "forge_tokens_per_second",
		Help: "Completion tokens/sec of the most recently finished request.",
	})
)

func IncRequest(code int)          { requestsTotal.WithLabelValues(strconv.Itoa(code)).Inc() }
func ObserveTTFT(d time.Duration)  { ttftSeconds.Observe(d.Seconds()) }
func SetTokensPerSecond(v float64) { tokensPerSecond.Set(v) }
