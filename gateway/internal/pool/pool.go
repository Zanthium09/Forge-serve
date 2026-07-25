// Package pool tracks the worker fleet: liveness and routing.
//
// Health is PULLED (the gateway polls each worker's /stats every 2s)
// rather than pushed by worker heartbeats - see ADR-004. The same poll
// feeds both the health decision and the capacity gauges, so there is one
// signal path instead of two.
//
// Routing is least-outstanding-tokens: pick the healthy worker with the
// fewest tokens still owed to clients. Round-robin would ignore that a
// worker can be mid-way through several 500-token generations while
// another just finished; outstanding tokens is a direct estimate of
// remaining work, which is what actually predicts queueing delay.
package pool

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Zanthium09/forge-serve/gateway/internal/worker"
)

// unhealthyAfter is the consecutive-miss threshold. Three misses at a 2s
// interval means a dead worker is ejected within ~6s, while a single
// blocked poll (a busy worker, a GC pause) does not eject anyone.
const unhealthyAfter = 3

var ErrNoHealthyWorker = errors.New("no healthy worker available")

// Worker is one inference worker plus the gateway's view of it.
type Worker struct {
	URL    string
	Client *worker.Client

	// outstanding is the estimated number of tokens this worker still owes
	// to in-flight clients. Atomic: read by every routing decision.
	outstanding atomic.Int64

	mu         sync.Mutex
	healthy    bool
	misses     int
	queueDepth int
	activeSeqs int
}

func (w *Worker) Healthy() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.healthy
}

func (w *Worker) Outstanding() int64 { return w.outstanding.Load() }

// AddOutstanding reserves a request's whole token budget up front, so a
// worker that was just handed a large request is immediately seen as
// loaded rather than after the tokens have already been generated.
func (w *Worker) AddOutstanding(maxTokens int) { w.outstanding.Add(int64(maxTokens)) }

// TokenDelivered retires one reserved token.
func (w *Worker) TokenDelivered() { w.outstanding.Add(-1) }

// ReleaseOutstanding returns the unused remainder when a request ends
// early (EOS, cancel, or error). Without this, short requests would leave
// phantom load on a worker forever.
func (w *Worker) ReleaseOutstanding(remaining int) {
	if remaining > 0 {
		w.outstanding.Add(-int64(remaining))
	}
}

type Pool struct {
	workers []*Worker
}

func New(urls []string) *Pool {
	p := &Pool{}
	for _, u := range urls {
		p.workers = append(p.workers, &Worker{URL: u, Client: worker.New(u)})
	}
	return p
}

func (p *Pool) Size() int { return len(p.workers) }

// MarkHealthyForTest seeds health without waiting for a poll cycle.
// Exported for tests in other packages that need a usable pool.
func (p *Pool) MarkHealthyForTest() {
	for _, w := range p.workers {
		w.mu.Lock()
		w.healthy = true
		w.mu.Unlock()
	}
}

// Pick returns the healthy worker with the fewest outstanding tokens,
// skipping `exclude` (used by failover to avoid retrying on the worker
// that just failed).
func (p *Pool) Pick(exclude *Worker) (*Worker, error) {
	var best *Worker
	for _, w := range p.workers {
		if w == exclude || !w.Healthy() {
			continue
		}
		if best == nil || w.Outstanding() < best.Outstanding() {
			best = w
		}
	}
	if best == nil {
		return nil, ErrNoHealthyWorker
	}
	return best, nil
}

// Snapshot aggregates fleet state for metrics. Unhealthy workers are
// excluded so capacity gauges describe usable capacity.
type Snapshot struct {
	HealthyWorkers int
	TotalWorkers   int
	QueueDepth     int
	ActiveSeqs     int
}

func (p *Pool) Snapshot() Snapshot {
	s := Snapshot{TotalWorkers: len(p.workers)}
	for _, w := range p.workers {
		w.mu.Lock()
		if w.healthy {
			s.HealthyWorkers++
			s.QueueDepth += w.queueDepth
			s.ActiveSeqs += w.activeSeqs
		}
		w.mu.Unlock()
	}
	return s
}

// Start runs the health/stats poll loop until ctx is cancelled.
func (p *Pool) Start(ctx context.Context, interval time.Duration) {
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		p.pollOnce(ctx) // don't wait a full interval to learn the fleet state
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				p.pollOnce(ctx)
			}
		}
	}()
}

func (p *Pool) pollOnce(ctx context.Context) {
	var wg sync.WaitGroup
	for _, w := range p.workers {
		wg.Add(1)
		go func(w *Worker) {
			defer wg.Done()
			// Timeout well under the poll interval: a hung worker must not
			// stall the next round of health checks.
			cctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
			defer cancel()
			st, err := w.Client.Stats(cctx)
			w.observe(st, err)
		}(w)
	}
	wg.Wait()
}

func (w *Worker) observe(st worker.StatsResponse, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err != nil {
		w.misses++
		if w.healthy && w.misses >= unhealthyAfter {
			w.healthy = false
			slog.Warn("worker marked unhealthy", "url", w.URL, "misses", w.misses)
		}
		return
	}
	if !w.healthy {
		slog.Info("worker healthy", "url", w.URL)
	}
	w.healthy = true
	w.misses = 0
	w.queueDepth = st.QueueDepth
	w.activeSeqs = st.ActiveSeqs
}
