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

	workerQueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "forge_worker_queue_depth",
		Help: "Requests waiting for a batch slot on the worker.",
	})

	batchOccupancy = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "forge_batch_occupancy",
		Help: "Sequences currently decoding across healthy workers.",
	})

	healthyWorkers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "forge_healthy_workers",
		Help: "Workers currently passing health checks.",
	})

	totalWorkers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "forge_workers_total",
		Help: "Workers configured in the fleet.",
	})

	failoversTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "forge_failovers_total",
		Help: "Requests retried on another worker after an upstream failure.",
	})

	rateLimitedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "forge_rate_limited_total",
		Help: "Requests rejected by the rate limiter.",
	})

	limiterFallbackTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "forge_limiter_local_fallback_total",
		Help: "Rate-limit decisions served by the local fallback (Redis down).",
	})

	budgetSkippedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "forge_budget_check_skipped_total",
		Help: "Token-budget checks skipped because Redis was unavailable (ADR-009).",
	})
)

func IncRequest(code int)          { requestsTotal.WithLabelValues(strconv.Itoa(code)).Inc() }
func ObserveTTFT(d time.Duration)  { ttftSeconds.Observe(d.Seconds()) }
func SetTokensPerSecond(v float64) { tokensPerSecond.Set(v) }

// SetFleet records aggregate fleet state (polled, not pushed — workers
// stay ignorant of Prometheus).
func SetFleet(queue, active, healthy, total int) {
	workerQueueDepth.Set(float64(queue))
	batchOccupancy.Set(float64(active))
	healthyWorkers.Set(float64(healthy))
	totalWorkers.Set(float64(total))
}

func IncFailover()        { failoversTotal.Inc() }
func IncRateLimited()     { rateLimitedTotal.Inc() }
func IncLimiterFallback() { limiterFallbackTotal.Inc() }
func IncBudgetSkipped()   { budgetSkippedTotal.Inc() }
