package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/Zanthium09/forge-serve/gateway/internal/auth"
	"github.com/Zanthium09/forge-serve/gateway/internal/backend/backendtest"
	"github.com/Zanthium09/forge-serve/gateway/internal/config"
	"github.com/Zanthium09/forge-serve/gateway/internal/pool"
)

// model builds a config model served by the given OpenAI-compatible fakes.
func model(name string, maxInflight int, servers ...*httptest.Server) config.Model {
	m := config.Model{Name: name, DefaultMaxTokens: 256}
	for _, s := range servers {
		m.Backends = append(m.Backends, config.Backend{URL: s.URL, Kind: "openai", MaxInflight: maxInflight})
	}
	return m
}

func fleetMux(t *testing.T, ready func(context.Context) error, models ...config.Model) (http.Handler, *pool.Fleet) {
	t.Helper()
	f, err := pool.NewFleet(&config.Config{Models: models})
	if err != nil {
		t.Fatal(err)
	}
	f.MarkHealthyForTest()
	return NewMux(Deps{Fleet: f, Auth: auth.New(allowAllStore{}, time.Minute, 0, nil),
		MaxBodyBytes: 1 << 20, Ready: ready}), f
}

func post(t *testing.T, url, body string, hdr ...string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer sk-test")
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func errCode(t *testing.T, resp *http.Response) string {
	t.Helper()
	var e struct {
		Error struct{ Code string } `json:"error"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&e)
	return e.Error.Code
}

// hold starts a slow streaming request and returns once the backend has it,
// so the backend's in-flight slot is occupied. cancel releases it.
func hold(t *testing.T, srvURL, model string, f *backendtest.Fake) (cancel func()) {
	t.Helper()
	ctx, cancelFn := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srvURL+"/v1/chat/completions",
		strings.NewReader(`{"model":"`+model+`","stream":true,"messages":[]}`))
	req.Header.Set("Authorization", "Bearer sk-test")
	before := f.Requests.Load()
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	}()
	deadline := time.Now().Add(2 * time.Second)
	for f.Requests.Load() == before {
		if time.Now().After(deadline) {
			t.Fatal("held request never reached the backend")
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cancelFn
}

func slowFake() *backendtest.Fake {
	chunks := make([]string, 50)
	for i := range chunks {
		chunks[i] = backendtest.ChatChunk("x")
	}
	return &backendtest.Fake{Chunks: chunks, Delay: 50 * time.Millisecond}
}

func TestProxyForwardsUnknownFields(t *testing.T) {
	f := &backendtest.Fake{}
	m := model("a", 64, f.Server(t))
	m.UpstreamModel = "up/a"
	mux, _ := fleetMux(t, nil, m)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp := post(t, srv.URL+"/v1/chat/completions", `{"model":"a","messages":[],"top_p":0.3}`)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var got map[string]json.RawMessage
	_ = json.Unmarshal(f.LastBody(), &got)
	if string(got["top_p"]) != "0.3" || string(got["model"]) != `"up/a"` {
		t.Fatalf("backend saw %s", f.LastBody())
	}
}

func TestProxyRoutesByModel(t *testing.T) {
	fa, fb := &backendtest.Fake{}, &backendtest.Fake{}
	mux, _ := fleetMux(t, nil, model("a", 64, fa.Server(t)), model("b", 64, fb.Server(t)))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	post(t, srv.URL+"/v1/chat/completions", `{"model":"b","messages":[]}`)
	if fa.Requests.Load() != 0 || fb.Requests.Load() != 1 {
		t.Fatalf("a=%d b=%d", fa.Requests.Load(), fb.Requests.Load())
	}
}

func TestProxyUnknownModel404(t *testing.T) {
	mux, _ := fleetMux(t, nil, model("a", 64, (&backendtest.Fake{}).Server(t)))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp := post(t, srv.URL+"/v1/chat/completions", `{"model":"zzz","messages":[]}`)
	if resp.StatusCode != 404 || errCode(t, resp) != "model_not_found" {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestProxyMissingModelWithSeveralConfigured400(t *testing.T) {
	mux, _ := fleetMux(t, nil, model("a", 64, (&backendtest.Fake{}).Server(t)), model("b", 64, (&backendtest.Fake{}).Server(t)))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp := post(t, srv.URL+"/v1/chat/completions", `{"messages":[]}`)
	if resp.StatusCode != 400 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestProxyBodyTooLarge413(t *testing.T) {
	f := &backendtest.Fake{}
	mux, _ := fleetMux(t, nil, model("a", 64, f.Server(t)))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	big := `{"messages":[],"pad":"` + strings.Repeat("x", 2<<20) + `"}`
	resp := post(t, srv.URL+"/v1/chat/completions", big)
	if resp.StatusCode != 413 || f.Requests.Load() != 0 {
		t.Fatalf("status = %d backend calls = %d", resp.StatusCode, f.Requests.Load())
	}
}

func TestProxyBackend4xxPassedThroughNoRetry(t *testing.T) {
	bad := &backendtest.Fake{Status: 400, Body: `{"error":{"message":"ctx too long"}}`}
	ok := &backendtest.Fake{}
	mux, _ := fleetMux(t, nil, model("a", 64, bad.Server(t), ok.Server(t)))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp := post(t, srv.URL+"/v1/chat/completions", `{"model":"a","stream":true,"messages":[]}`)
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 400 || !strings.Contains(string(body), "ctx too long") || ok.Requests.Load() != 0 {
		t.Fatalf("status = %d body = %s ok calls = %d", resp.StatusCode, body, ok.Requests.Load())
	}
}

func TestProxyFallbackWhenPoolFull(t *testing.T) {
	bigF, smallF := slowFake(), &backendtest.Fake{}
	big := model("big", 1, bigF.Server(t))
	big.Fallback = "small"
	mux, _ := fleetMux(t, nil, big, model("small", 64, smallF.Server(t)))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	defer hold(t, srv.URL, "big", bigF)()

	resp := post(t, srv.URL+"/v1/chat/completions", `{"model":"big","messages":[]}`)
	if resp.StatusCode != 200 || resp.Header.Get("X-Forge-Model-Fallback") != "small" || smallF.Requests.Load() != 1 {
		t.Fatalf("status = %d header = %q", resp.StatusCode, resp.Header.Get("X-Forge-Model-Fallback"))
	}
}

func TestProxyNoFallbackHeaderGives429(t *testing.T) {
	bigF, smallF := slowFake(), &backendtest.Fake{}
	big := model("big", 1, bigF.Server(t))
	big.Fallback = "small"
	mux, _ := fleetMux(t, nil, big, model("small", 64, smallF.Server(t)))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	defer hold(t, srv.URL, "big", bigF)()

	resp := post(t, srv.URL+"/v1/chat/completions", `{"model":"big","messages":[]}`, "X-Forge-No-Fallback", "1")
	if resp.StatusCode != 429 || resp.Header.Get("Retry-After") != "1" || smallF.Requests.Load() != 0 {
		t.Fatalf("status = %d retry-after = %q", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
}

func TestProxyFallbackIsOneHop(t *testing.T) {
	bigF, smallF, tinyF := slowFake(), slowFake(), &backendtest.Fake{}
	big := model("big", 1, bigF.Server(t))
	big.Fallback = "small"
	small := model("small", 1, smallF.Server(t))
	small.Fallback = "tiny"
	mux, _ := fleetMux(t, nil, big, small, model("tiny", 64, tinyF.Server(t)))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	defer hold(t, srv.URL, "big", bigF)()
	defer hold(t, srv.URL, "small", smallF)()

	resp := post(t, srv.URL+"/v1/chat/completions", `{"model":"big","messages":[]}`)
	if resp.StatusCode != 429 || tinyF.Requests.Load() != 0 {
		t.Fatalf("status = %d tiny calls = %d", resp.StatusCode, tinyF.Requests.Load())
	}
}

func TestProxyEmbeddingsOnForgeIs400(t *testing.T) {
	m := config.Model{Name: "m", DefaultMaxTokens: 256,
		Backends: []config.Backend{{URL: "http://unused.invalid", Kind: "forge", MaxInflight: 64}}}
	mux, _ := fleetMux(t, nil, m)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp := post(t, srv.URL+"/v1/embeddings", `{"model":"m","input":"a"}`)
	if resp.StatusCode != 400 || errCode(t, resp) != "unsupported_endpoint" {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestModelsListsConfiguredModels(t *testing.T) {
	mux, _ := fleetMux(t, nil, model("a", 64, (&backendtest.Fake{}).Server(t)), model("b", 64, (&backendtest.Fake{}).Server(t)))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/models", nil)
	req.Header.Set("Authorization", "Bearer sk-test")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var list struct {
		Object string
		Data   []struct{ ID string }
	}
	_ = json.NewDecoder(resp.Body).Decode(&list)
	if list.Object != "list" || len(list.Data) != 2 || list.Data[0].ID != "a" || list.Data[1].ID != "b" {
		t.Fatalf("list = %+v", list)
	}
}

func TestReadyz(t *testing.T) {
	healthy, _ := fleetMux(t, func(context.Context) error { return nil }, model("a", 64, (&backendtest.Fake{}).Server(t)))
	rec := httptest.NewRecorder()
	healthy.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != 200 {
		t.Fatalf("ready = %d", rec.Code)
	}
	broken, _ := fleetMux(t, func(context.Context) error { return errors.New("db down") }, model("a", 64, (&backendtest.Fake{}).Server(t)))
	rec = httptest.NewRecorder()
	broken.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != 503 {
		t.Fatalf("db down ready = %d", rec.Code)
	}
}

func TestProxySetsRequestID(t *testing.T) {
	mux, _ := fleetMux(t, nil, model("a", 64, (&backendtest.Fake{}).Server(t)))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp := post(t, srv.URL+"/v1/chat/completions", `{"messages":[]}`)
	if !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(resp.Header.Get("X-Request-Id")) {
		t.Fatalf("X-Request-Id = %q", resp.Header.Get("X-Request-Id"))
	}
}

func TestDisconnectReleasesInflight(t *testing.T) {
	f := slowFake()
	other := &backendtest.Fake{}
	mux, fleet := fleetMux(t, nil, model("a", 64, f.Server(t), other.Server(t)))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/chat/completions",
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

	m, _ := fleet.Model("a")
	w := m.Pool.Workers()[0]
	deadline := time.Now().Add(500 * time.Millisecond)
	for w.Inflight() != 0 || w.Outstanding() != 0 || !f.Cancelled.Load() {
		if time.Now().After(deadline) {
			t.Fatalf("inflight = %d outstanding = %d cancelled = %v",
				w.Inflight(), w.Outstanding(), f.Cancelled.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if other.Requests.Load() != 0 {
		t.Fatal("a disconnect must not be retried on another backend")
	}
}
