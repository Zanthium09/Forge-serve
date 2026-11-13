package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Zanthium09/forge-serve/gateway/internal/auth"
	"github.com/Zanthium09/forge-serve/gateway/internal/backend/backendtest"
	"github.com/Zanthium09/forge-serve/gateway/internal/budget"
	"github.com/Zanthium09/forge-serve/gateway/internal/config"
	"github.com/Zanthium09/forge-serve/gateway/internal/pool"
	"github.com/Zanthium09/forge-serve/gateway/internal/ratelimit"
)

// recStore authenticates every key as one fixed person.
type recStore struct{ rec auth.KeyRecord }

func (s recStore) LookupKeyForAuth(context.Context, string) (auth.KeyRecord, error) { return s.rec, nil }

func person() auth.KeyRecord {
	return auth.KeyRecord{KeyID: 11, UserID: 5, TeamID: 2, Role: "member", Active: true,
		RPMBurst: 1000, RPMRefillPerSec: 1000}
}

// recorder captures usage events synchronously.
type recorder struct {
	mu  sync.Mutex
	evs []UsageEvent
}

func (r *recorder) RecordUsage(ev UsageEvent) {
	r.mu.Lock()
	r.evs = append(r.evs, ev)
	r.mu.Unlock()
}

func (r *recorder) events() []UsageEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]UsageEvent(nil), r.evs...)
}

// mapCounter is an in-memory budget counter that records Adds.
type mapCounter struct {
	mu   sync.Mutex
	vals map[string]int64
	adds int
}

func (m *mapCounter) Get(_ context.Context, k string) (int64, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.vals[k]
	return v, ok, nil
}
func (m *mapCounter) SetNX(_ context.Context, k string, v int64, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.vals[k]; !ok {
		m.vals[k] = v
	}
	return nil
}
func (m *mapCounter) AddIfExists(_ context.Context, k string, n int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.adds++
	if _, ok := m.vals[k]; ok {
		m.vals[k] += n
	}
	return nil
}

type usedSource int64

func (u usedSource) TokensUsed(context.Context, string, int64, time.Time) (int64, error) {
	return int64(u), nil
}

type env struct {
	srv     *httptest.Server
	rec     *recorder
	counter *mapCounter
	fleet   *pool.Fleet
}

func identityEnv(t *testing.T, rec auth.KeyRecord, used int64, models ...config.Model) env {
	t.Helper()
	f, err := pool.NewFleet(&config.Config{Models: models})
	if err != nil {
		t.Fatal(err)
	}
	f.MarkHealthyForTest()
	e := env{rec: &recorder{}, counter: &mapCounter{vals: map[string]int64{}}, fleet: f}
	mux := NewMux(Deps{
		Fleet: f, Auth: auth.New(recStore{rec}, time.Minute, 0, nil),
		Limiter: ratelimit.New(nil), TPM: ratelimit.NewTPM(nil),
		Budgets: budget.New(e.counter, usedSource(used)),
		Record:  e.rec, MaxBodyBytes: 1 << 20,
	})
	e.srv = httptest.NewServer(mux)
	t.Cleanup(e.srv.Close)
	return e
}

func restricted(m config.Model) config.Model { m.Restricted = true; return m }

func TestRestrictedModelNeedsGrant(t *testing.T) {
	f := &backendtest.Fake{}
	e := identityEnv(t, person(), 0, restricted(model("big", 64, f.Server(t))))
	if resp := post(t, e.srv.URL+"/v1/chat/completions", `{"model":"big","messages":[]}`); resp.StatusCode != 403 || errCode(t, resp) != "model_not_allowed" {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	granted := person()
	granted.Grants = []string{"big"}
	e = identityEnv(t, granted, 0, restricted(model("big", 64, f.Server(t))))
	if resp := post(t, e.srv.URL+"/v1/chat/completions", `{"model":"big","messages":[]}`); resp.StatusCode != 200 {
		t.Fatalf("granted status = %d", resp.StatusCode)
	}
}

func TestModelsFilteredByGrants(t *testing.T) {
	e := identityEnv(t, person(), 0,
		restricted(model("big", 64, (&backendtest.Fake{}).Server(t))), model("small", 64, (&backendtest.Fake{}).Server(t)))
	req, _ := http.NewRequest(http.MethodGet, e.srv.URL+"/v1/models", nil)
	req.Header.Set("Authorization", "Bearer sk-test")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(b), `"big"`) || !strings.Contains(string(b), `"small"`) {
		t.Fatalf("models = %s", b)
	}
}

func TestFallbackRespectsGrants(t *testing.T) {
	bigF, smallF := slowFake(), &backendtest.Fake{}
	big := model("big", 1, bigF.Server(t))
	big.Fallback = "small"
	e := identityEnv(t, person(), 0, big, restricted(model("small", 64, smallF.Server(t))))
	defer hold(t, e.srv.URL, "big", bigF)()
	resp := post(t, e.srv.URL+"/v1/chat/completions", `{"model":"big","messages":[]}`)
	if resp.StatusCode != 429 || smallF.Requests.Load() != 0 {
		t.Fatalf("status = %d small calls = %d", resp.StatusCode, smallF.Requests.Load())
	}
}

func TestPriorityOnlyToSupportingBackend(t *testing.T) {
	pf, nf := &backendtest.Fake{}, &backendtest.Fake{}
	p := model("p", 64, pf.Server(t))
	p.Backends[0].SupportsPriority = true
	four := 4
	who := person()
	who.Priority = &four
	e := identityEnv(t, who, 0, p, model("n", 64, nf.Server(t)))

	post(t, e.srv.URL+"/v1/chat/completions", `{"model":"p","messages":[],"priority":-100}`)
	post(t, e.srv.URL+"/v1/chat/completions", `{"model":"n","messages":[],"priority":-100}`)
	var pb, nb map[string]json.RawMessage
	_ = json.Unmarshal(pf.LastBody(), &pb)
	_ = json.Unmarshal(nf.LastBody(), &nb)
	if string(pb["priority"]) != "4" {
		t.Fatalf("supporting backend priority = %s", pb["priority"])
	}
	if _, ok := nb["priority"]; ok {
		t.Fatalf("client priority leaked to non-supporting backend: %s", nf.LastBody())
	}
}

func TestTPMBlocksAfterBigRequest(t *testing.T) {
	f := &backendtest.Fake{Body: `{"choices":[],"usage":{"prompt_tokens":50,"completion_tokens":50}}`}
	who := person()
	who.TokensPerMinute = 60
	e := identityEnv(t, who, 0, model("a", 64, f.Server(t)))
	if resp := post(t, e.srv.URL+"/v1/chat/completions", `{"messages":[]}`); resp.StatusCode != 200 {
		t.Fatalf("first status = %d", resp.StatusCode)
	}
	resp := post(t, e.srv.URL+"/v1/chat/completions", `{"messages":[]}`)
	if resp.StatusCode != 429 || resp.Header.Get("Retry-After") == "" || errCode(t, resp) != "tokens_per_minute" {
		t.Fatalf("second status = %d", resp.StatusCode)
	}
}

func TestBudgetExceededIs429InsufficientQuota(t *testing.T) {
	f := &backendtest.Fake{}
	who := person()
	who.DailyBudget = 10
	e := identityEnv(t, who, 10, model("a", 64, f.Server(t)))
	resp := post(t, e.srv.URL+"/v1/chat/completions", `{"messages":[]}`)
	var body struct {
		Error struct{ Type, Code string } `json:"error"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if resp.StatusCode != 429 || body.Error.Type != "insufficient_quota" || body.Error.Code != "budget_exceeded" || f.Requests.Load() != 0 {
		t.Fatalf("status = %d body = %+v", resp.StatusCode, body)
	}
}

func TestUsageRecordedAgainstServedModel(t *testing.T) {
	bigF, smallF := slowFake(), &backendtest.Fake{}
	big := model("big", 1, bigF.Server(t))
	big.Fallback = "small"
	e := identityEnv(t, person(), 0, big, model("small", 64, smallF.Server(t)))
	release := hold(t, e.srv.URL, "big", bigF)
	resp := post(t, e.srv.URL+"/v1/chat/completions", `{"model":"big","messages":[]}`)
	release()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var got *UsageEvent
	for _, ev := range e.rec.events() {
		if ev.Model == "small" {
			ev := ev
			got = &ev
		}
	}
	if got == nil || got.RequestedModel != "big" || got.UserID != 5 || got.TeamID != 2 || got.KeyID != 11 ||
		got.Endpoint != "chat" || got.PromptTokens != 5 || got.CompletionTokens != 2 {
		t.Fatalf("event = %+v (all: %+v)", got, e.rec.events())
	}
}

// Review Focus 2: a client that leaves mid-stream is charged for what it got.
func TestClientDisconnectRecordsEstimatedUsage(t *testing.T) {
	f := slowFake()
	e := identityEnv(t, person(), 0, model("a", 64, f.Server(t)))
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, e.srv.URL+"/v1/chat/completions",
		strings.NewReader(`{"stream":true,"messages":[]}`))
	req.Header.Set("Authorization", "Bearer sk-test")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resp.Body.Read(make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	cancel()
	_ = resp.Body.Close()

	deadline := time.Now().Add(time.Second)
	for len(e.rec.events()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	evs := e.rec.events()
	if len(evs) != 1 || !evs[0].Estimated || evs[0].CompletionTokens < 1 {
		t.Fatalf("events = %+v", evs)
	}
}

// Review Focus 1: an engine-rejected request costs the user nothing.
func TestBackend4xxChargesNothing(t *testing.T) {
	f := &backendtest.Fake{Status: 400, Body: `{"error":{"message":"bad"}}`}
	who := person()
	who.DailyBudget = 1000
	who.TokensPerMinute = 1000
	e := identityEnv(t, who, 0, model("a", 64, f.Server(t)))
	if resp := post(t, e.srv.URL+"/v1/chat/completions", `{"messages":[]}`); resp.StatusCode != 400 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if len(e.rec.events()) != 0 || e.counter.adds != 0 {
		t.Fatalf("charged: events = %+v adds = %d", e.rec.events(), e.counter.adds)
	}
}

// The real server always mounts the admin API; it must coexist with the
// playground route (a "GET /" catch-all panicked at boot against it).
func TestMuxWithAdminAndPlayground(t *testing.T) {
	adminHit := false
	admin := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { adminHit = true })
	rec := person()
	rec.Role = "admin"
	f, _ := pool.NewFleet(&config.Config{})
	srv := httptest.NewServer(NewMux(Deps{Fleet: f, Auth: auth.New(recStore{rec}, time.Minute, 0, nil),
		Limiter: ratelimit.New(nil), Admin: admin}))
	defer srv.Close()
	req, _ := http.NewRequest("POST", srv.URL+"/admin/v1/users", nil)
	req.Header.Set("Authorization", "Bearer sk-forge-x")
	if resp, err := http.DefaultClient.Do(req); err != nil || !adminHit {
		t.Fatalf("admin route not reached: %v", err)
	} else {
		_ = resp.Body.Close()
	}
	resp, err := http.Get(srv.URL + "/")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("playground: %v %v", resp, err)
	}
	_ = resp.Body.Close()
	resp, _ = http.Get(srv.URL + "/nope")
	if resp.StatusCode != 404 {
		t.Fatalf("unknown path status = %d", resp.StatusCode)
	}
	_ = resp.Body.Close()
}
