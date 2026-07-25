package pool

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// statsServer is a worker whose /stats can be switched on and off, so a
// test can simulate a worker dying and recovering.
func statsServer(t *testing.T, up *atomic.Bool) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !up.Load() {
			http.Error(w, "down", http.StatusInternalServerError)
			return
		}
		_, _ = fmt.Fprint(w, `{"queue_depth":0,"active_seqs":0,"max_batch":16}`)
	}))
}

func TestPickReturnsErrorWhenNoWorkers(t *testing.T) {
	p := New(nil)
	if _, err := p.Pick(nil); !errors.Is(err, ErrNoHealthyWorker) {
		t.Fatalf("err = %v, want ErrNoHealthyWorker", err)
	}
}

func TestPickPrefersLeastOutstandingTokens(t *testing.T) {
	p := New([]string{"http://a", "http://b"})
	a, b := p.workers[0], p.workers[1]
	a.healthy, b.healthy = true, true

	a.AddOutstanding(500)
	b.AddOutstanding(10)

	got, err := p.Pick(nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != b {
		t.Fatalf("picked %s, want the worker with fewer outstanding tokens", got.URL)
	}
}

func TestPickSkipsUnhealthyWorkers(t *testing.T) {
	p := New([]string{"http://a", "http://b"})
	a, b := p.workers[0], p.workers[1]
	a.healthy, b.healthy = false, true
	a.AddOutstanding(0) // idle but dead
	b.AddOutstanding(999)

	got, err := p.Pick(nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != b {
		t.Fatalf("picked unhealthy worker %s", got.URL)
	}
}

func TestPickExcludesGivenWorker(t *testing.T) {
	p := New([]string{"http://a", "http://b"})
	a, b := p.workers[0], p.workers[1]
	a.healthy, b.healthy = true, true

	got, err := p.Pick(a)
	if err != nil {
		t.Fatal(err)
	}
	if got != b {
		t.Fatalf("picked %s, want the non-excluded worker", got.URL)
	}
}

func TestPickErrorsWhenOnlyWorkerIsExcluded(t *testing.T) {
	p := New([]string{"http://a"})
	p.workers[0].healthy = true
	if _, err := p.Pick(p.workers[0]); !errors.Is(err, ErrNoHealthyWorker) {
		t.Fatal("expected ErrNoHealthyWorker when the only worker is excluded")
	}
}

func TestOutstandingTokensAccounting(t *testing.T) {
	p := New([]string{"http://a"})
	w := p.workers[0]

	w.AddOutstanding(100)
	if got := w.Outstanding(); got != 100 {
		t.Fatalf("outstanding = %d, want 100", got)
	}
	w.TokenDelivered()
	w.TokenDelivered()
	if got := w.Outstanding(); got != 98 {
		t.Fatalf("outstanding = %d, want 98", got)
	}
	// Finishing early (EOS at 2 of 100) must return the whole reservation.
	w.ReleaseOutstanding(98)
	if got := w.Outstanding(); got != 0 {
		t.Fatalf("outstanding = %d, want 0 after release", got)
	}
}

func TestHealthMarksUnhealthyAfterThreeMissesAndRecovers(t *testing.T) {
	up := &atomic.Bool{}
	up.Store(true)
	srv := statsServer(t, up)
	defer srv.Close()

	p := New([]string{srv.URL})
	w := p.workers[0]

	// One good poll makes it healthy.
	p.pollOnce(context.Background())
	if !w.Healthy() {
		t.Fatal("worker should be healthy after a successful poll")
	}

	up.Store(false)
	p.pollOnce(context.Background())
	if !w.Healthy() {
		t.Fatal("one miss must not eject a worker (transient blips happen)")
	}
	p.pollOnce(context.Background())
	if !w.Healthy() {
		t.Fatal("two misses must not eject a worker")
	}
	p.pollOnce(context.Background())
	if w.Healthy() {
		t.Fatal("worker should be unhealthy after three consecutive misses")
	}

	// A single good poll brings it straight back.
	up.Store(true)
	p.pollOnce(context.Background())
	if !w.Healthy() {
		t.Fatal("worker should recover on the first successful poll")
	}
}

func TestSnapshotAggregatesForMetrics(t *testing.T) {
	p := New([]string{"http://a", "http://b"})
	p.workers[0].healthy = true
	p.workers[0].queueDepth, p.workers[0].activeSeqs = 3, 5
	p.workers[1].healthy = false
	p.workers[1].queueDepth, p.workers[1].activeSeqs = 100, 100

	s := p.Snapshot()
	if s.HealthyWorkers != 1 {
		t.Fatalf("healthy = %d, want 1", s.HealthyWorkers)
	}
	// Unhealthy workers must not pollute capacity signals.
	if s.QueueDepth != 3 || s.ActiveSeqs != 5 {
		t.Fatalf("snapshot = %+v, want only the healthy worker counted", s)
	}
}
