package pool

import (
	"context"
	"time"

	"github.com/Zanthium09/forge-serve/gateway/internal/backend"
	"github.com/Zanthium09/forge-serve/gateway/internal/config"
)

// Model is one client-facing model and the pool that serves it.
type Model struct {
	Name             string
	Upstream         string // name sent to the engine
	Restricted       bool
	DefaultMaxTokens int    // routing estimate when the client sends none
	Fallback         string // "" = none (ADR-014)
	Pool             *Pool
}

// Fleet is every model's pool, in config order.
type Fleet struct {
	byName map[string]*Model
	order  []*Model
}

func NewFleet(c *config.Config) (*Fleet, error) {
	f := &Fleet{byName: map[string]*Model{}}
	for _, mc := range c.Models {
		var ws []*Worker
		for _, bc := range mc.Backends {
			b, err := backend.New(bc.Kind, bc.URL)
			if err != nil {
				return nil, err
			}
			ws = append(ws, NewWorker(bc.URL, b, bc.MaxWaiting, bc.MaxInflight, bc.SupportsPriority))
		}
		m := &Model{Name: mc.Name, Upstream: mc.Upstream(), Restricted: mc.Restricted,
			DefaultMaxTokens: mc.DefaultMaxTokens, Fallback: mc.Fallback, Pool: New(ws)}
		f.byName[m.Name] = m
		f.order = append(f.order, m)
	}
	return f, nil
}

func (f *Fleet) Model(name string) (*Model, bool) {
	m, ok := f.byName[name]
	return m, ok
}

func (f *Fleet) Names() []string {
	out := make([]string, len(f.order))
	for i, m := range f.order {
		out[i] = m.Name
	}
	return out
}

// Start polls every pool until ctx ends.
func (f *Fleet) Start(ctx context.Context, interval time.Duration) {
	for _, m := range f.order {
		m.Pool.Start(ctx, interval)
	}
}

// Snapshot sums fleet state across all pools for metrics.
func (f *Fleet) Snapshot() Snapshot {
	var s Snapshot
	for _, m := range f.order {
		ps := m.Pool.Snapshot()
		s.HealthyWorkers += ps.HealthyWorkers
		s.TotalWorkers += ps.TotalWorkers
		s.QueueDepth += ps.QueueDepth
		s.ActiveSeqs += ps.ActiveSeqs
	}
	return s
}

func (f *Fleet) AnyHealthy() bool { return f.Snapshot().HealthyWorkers > 0 }

// MarkHealthyForTest seeds health in every pool without a poll cycle.
func (f *Fleet) MarkHealthyForTest() {
	for _, m := range f.order {
		m.Pool.MarkHealthyForTest()
	}
}
