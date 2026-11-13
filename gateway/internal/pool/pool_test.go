package pool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/Zanthium09/forge-serve/gateway/internal/backend"
	"github.com/Zanthium09/forge-serve/gateway/internal/config"
)

// fakeBackend reports a configurable load, or an error while down.
type fakeBackend struct {
	down atomic.Bool
	load backend.Load
}

func (f *fakeBackend) Forward(context.Context, *backend.ProxyRequest, backend.Sink) (backend.Result, error) {
	return backend.Result{}, nil
}

func (f *fakeBackend) Load(context.Context) (backend.Load, error) {
	if f.down.Load() {
		return backend.Load{}, errors.New("down")
	}
	return f.load, nil
}

func mkPool(n, maxWaiting, maxInflight int) *Pool {
	ws := make([]*Worker, n)
	for i := range ws {
		ws[i] = NewWorker(string(rune('a'+i)), &fakeBackend{}, maxWaiting, maxInflight, false)
	}
	return New(ws)
}

func TestPickReturnsErrorWhenNoWorkers(t *testing.T) {
	p := New(nil)
	if _, err := p.Pick(nil); !errors.Is(err, ErrNoHealthyWorker) {
		t.Fatalf("err = %v, want ErrNoHealthyWorker", err)
	}
}

func TestPickPrefersLeastOutstandingTokens(t *testing.T) {
	p := mkPool(2, 0, 64)
	a, b := p.workers[0], p.workers[1]
	a.healthy, b.healthy = true, true
	a.AddOutstanding(500)
	b.AddOutstanding(10)
	if got, err := p.Pick(nil); err != nil || got != b {
		t.Fatalf("picked %v err %v, want the worker with fewer outstanding tokens", got, err)
	}
}

func TestPickSkipsUnhealthyWorkers(t *testing.T) {
	p := mkPool(2, 0, 64)
	a, b := p.workers[0], p.workers[1]
	a.healthy, b.healthy = false, true
	b.AddOutstanding(999)
	if got, err := p.Pick(nil); err != nil || got != b {
		t.Fatalf("picked %v err %v", got, err)
	}
}

func TestPickExcludesGivenWorker(t *testing.T) {
	p := mkPool(2, 0, 64)
	a, b := p.workers[0], p.workers[1]
	a.healthy, b.healthy = true, true
	if got, err := p.Pick(a); err != nil || got != b {
		t.Fatalf("picked %v err %v, want the non-excluded worker", got, err)
	}
}

func TestPickErrorsWhenOnlyWorkerIsExcluded(t *testing.T) {
	p := mkPool(1, 0, 64)
	p.workers[0].healthy = true
	if _, err := p.Pick(p.workers[0]); !errors.Is(err, ErrNoHealthyWorker) {
		t.Fatal("expected ErrNoHealthyWorker when the only worker is excluded")
	}
}

func TestOutstandingTokensAccounting(t *testing.T) {
	w := mkPool(1, 0, 64).workers[0]
	w.AddOutstanding(100)
	w.TokenDelivered()
	w.TokenDelivered()
	if got := w.Outstanding(); got != 98 {
		t.Fatalf("outstanding = %d, want 98", got)
	}
	// Finishing early must return the whole remaining reservation.
	w.ReleaseOutstanding(98)
	if got := w.Outstanding(); got != 0 {
		t.Fatalf("outstanding = %d, want 0 after release", got)
	}
}

func TestHealthMarksUnhealthyAfterThreeMissesAndRecovers(t *testing.T) {
	fb := &fakeBackend{}
	p := New([]*Worker{NewWorker("a", fb, 0, 64, false)})
	w := p.workers[0]

	p.pollOnce(context.Background())
	if !w.Healthy() {
		t.Fatal("worker should be healthy after a successful poll")
	}
	fb.down.Store(true)
	for i := 1; i <= 2; i++ {
		p.pollOnce(context.Background())
		if !w.Healthy() {
			t.Fatalf("%d misses must not eject a worker", i)
		}
	}
	p.pollOnce(context.Background())
	if w.Healthy() {
		t.Fatal("worker should be unhealthy after three consecutive misses")
	}
	fb.down.Store(false)
	p.pollOnce(context.Background())
	if !w.Healthy() {
		t.Fatal("worker should recover on the first successful poll")
	}
}

func TestSnapshotAggregatesForMetrics(t *testing.T) {
	p := mkPool(2, 0, 64)
	p.workers[0].healthy = true
	p.workers[0].load = backend.Load{Waiting: 3, Running: 5, Known: true}
	p.workers[1].healthy = false
	p.workers[1].load = backend.Load{Waiting: 100, Running: 100, Known: true}

	s := p.Snapshot()
	if s.HealthyWorkers != 1 || s.QueueDepth != 3 || s.ActiveSeqs != 5 {
		t.Fatalf("snapshot = %+v, want only the healthy worker counted", s)
	}
}

func TestPickPoolFullWhenAllAtMaxInflight(t *testing.T) {
	p := mkPool(2, 0, 1)
	a, b := p.workers[0], p.workers[1]
	a.healthy, b.healthy = true, true
	a.Acquire()
	b.Acquire()
	if _, err := p.Pick(nil); !errors.Is(err, ErrPoolFull) {
		t.Fatalf("err = %v, want ErrPoolFull", err)
	}
	a.Release()
	if got, err := p.Pick(nil); err != nil || got != a {
		t.Fatalf("picked %v err %v after release", got, err)
	}
}

func TestPickPoolFullWhenEngineQueueFull(t *testing.T) {
	p := mkPool(1, 4, 64)
	w := p.workers[0]
	w.observe(backend.Load{Waiting: 4, Known: true}, nil)
	if _, err := p.Pick(nil); !errors.Is(err, ErrPoolFull) {
		t.Fatalf("err = %v, want ErrPoolFull at max_waiting", err)
	}
	w.observe(backend.Load{Waiting: 400, Known: false}, nil)
	if _, err := p.Pick(nil); err != nil {
		t.Fatalf("unknown queue must be ignored: %v", err)
	}
}

func TestPickNoHealthyIsDistinctFromFull(t *testing.T) {
	p := mkPool(2, 0, 64)
	if _, err := p.Pick(nil); !errors.Is(err, ErrNoHealthyWorker) || errors.Is(err, ErrPoolFull) {
		t.Fatalf("err = %v, want ErrNoHealthyWorker only", err)
	}
}

func TestFleetBuildsPoolsFromConfig(t *testing.T) {
	c := &config.Config{Models: []config.Model{
		{Name: "big", Fallback: "small", DefaultMaxTokens: 256, Backends: []config.Backend{
			{URL: "http://g1", Kind: "vllm", MaxInflight: 64},
			{URL: "http://g2", Kind: "vllm", MaxInflight: 64, SupportsPriority: true},
		}},
		{Name: "small", UpstreamModel: "qwen:0.5b", DefaultMaxTokens: 128, Backends: []config.Backend{
			{URL: "http://o1", Kind: "ollama", MaxInflight: 64},
		}},
	}}
	f, err := NewFleet(c)
	if err != nil {
		t.Fatal(err)
	}
	if n := f.Names(); len(n) != 2 || n[0] != "big" || n[1] != "small" {
		t.Fatalf("names = %v", n)
	}
	big, ok := f.Model("big")
	if !ok || big.Pool.Size() != 2 || big.Fallback != "small" || !big.Pool.workers[1].SupportsPriority() {
		t.Fatalf("big = %+v", big)
	}
	small, _ := f.Model("small")
	if small.Upstream != "qwen:0.5b" || small.DefaultMaxTokens != 128 {
		t.Fatalf("small = %+v", small)
	}
	if _, ok := f.Model("x"); ok {
		t.Fatal("unknown model must not resolve")
	}
}
