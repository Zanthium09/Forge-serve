package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/Zanthium09/forge-serve/gateway/internal/auth"
	"github.com/Zanthium09/forge-serve/gateway/internal/backend"
	"github.com/Zanthium09/forge-serve/gateway/internal/budget"
	"github.com/Zanthium09/forge-serve/gateway/internal/metrics"
	"github.com/Zanthium09/forge-serve/gateway/internal/pool"
	"github.com/Zanthium09/forge-serve/gateway/internal/ratelimit"
)

// proxyHandler is the request path (spec §6.1): parse, route by model,
// admit (or fall back), forward the body, relay, retry once if nothing
// reached the client yet (ADR-005).
type proxyHandler struct {
	fleet   *pool.Fleet
	maxBody int64
	record  UsageRecorder
	tpm     *ratelimit.TPMLimiter // nil = disabled
	budgets *budget.Budgets       // nil = disabled
}

func (h *proxyHandler) serve(ep backend.Endpoint) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h.handle(w, r, ep)
	}
}

func newRequestID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (h *proxyHandler) handle(w http.ResponseWriter, r *http.Request, ep backend.Endpoint) {
	start := time.Now()
	w.Header().Set("X-Request-Id", newRequestID())

	body, err := readBody(w, r, h.maxBody)
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			reject(w, http.StatusRequestEntityTooLarge, "invalid_request_error", "body_too_large", "request body too large")
			return
		}
		reject(w, http.StatusBadRequest, "invalid_request_error", "invalid_request", "could not read body")
		return
	}
	p, err := parseRequest(ep, body)
	if err != nil {
		reject(w, http.StatusBadRequest, "invalid_request_error", "invalid_request", err.Error())
		return
	}
	name, err := resolveModel(p.Model, h.fleet.Names())
	if err != nil {
		reject(w, http.StatusBadRequest, "invalid_request_error", "invalid_request", err.Error())
		return
	}
	requested, ok := h.fleet.Model(name)
	if !ok {
		reject(w, http.StatusNotFound, "invalid_request_error", "model_not_found", "model "+name+" does not exist")
		return
	}
	id, authed := auth.FromContext(r.Context())
	if authed && !id.CanUse(requested.Name, requested.Restricted) {
		reject(w, http.StatusForbidden, "permission_error", "model_not_allowed", "you may not use model "+name)
		return
	}

	served, wk, err := h.admit(requested, r, id)
	if err != nil {
		if errors.Is(err, pool.ErrPoolFull) {
			writeOverloaded(w)
		} else {
			writeNoCapacity(w)
		}
		return
	}
	if served != requested {
		w.Header().Set("X-Forge-Model-Fallback", served.Name)
	}

	sink := &httpSink{w: w, start: start}
	res, err := h.attempt(r, p, served, wk, sink, id)
	if err != nil && !sink.started && r.Context().Err() == nil && retryable(err) {
		slog.Warn("backend failed before first token, retrying elsewhere", "backend", wk.URL, "err", err)
		metrics.IncFailover()
		if alt, perr := served.Pool.Pick(wk); perr == nil {
			res, err = h.attempt(r, p, served, alt, sink, id)
		}
	}

	// Charge what the client actually received: the engine's own count on
	// success; delivered chunks (estimated) when the stream broke or the
	// client left. Nothing when no content reached the client.
	switch {
	case err == nil:
		h.charge(r, id, authed, p, requested, served, res.Usage)
	case sink.started:
		h.charge(r, id, authed, p, requested, served,
			backend.Usage{CompletionTokens: res.Sent, Estimated: true})
	}

	if r.Context().Err() != nil {
		slog.Info("client disconnected", "content_chunks_sent", res.Sent)
		return
	}
	var se *backend.StatusError
	switch {
	case err == nil:
		metrics.IncRequest(http.StatusOK)
		if el := time.Since(start).Seconds(); el > 0 && res.Usage.CompletionTokens > 0 {
			metrics.SetTokensPerSecond(float64(res.Usage.CompletionTokens) / el)
		}
	case sink.started:
		// Mid-stream failure. No migration (ADR-005): a clean,
		// machine-readable error event instead of a silent truncation.
		metrics.IncRequest(http.StatusBadGateway)
		slog.Error("backend stream failed mid-response", "err", err, "content_chunks_sent", res.Sent)
		_ = sink.Write([]byte("data: {\"error\":{\"message\":\"upstream failure\",\"type\":\"worker_lost\"}}\n\n"))
	case errors.As(err, &se):
		// The engine rejected the request itself: relay it untouched.
		metrics.IncRequest(se.Code)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(se.Code)
		_, _ = w.Write(se.Body)
	case errors.Is(err, backend.ErrUnsupported):
		reject(w, http.StatusBadRequest, "invalid_request_error", "unsupported_endpoint",
			"this model's backend does not support this endpoint")
	case errors.Is(err, backend.ErrOverloaded):
		writeOverloaded(w)
	default:
		writeNoCapacity(w)
	}
}

// admit picks a backend for the model, or for its fallback when the
// model's pool is full or down (ADR-014): admission time only, one hop,
// and only if the client did not opt out.
func (h *proxyHandler) admit(m *pool.Model, r *http.Request, id auth.Identity) (*pool.Model, *pool.Worker, error) {
	wk, err := m.Pool.Pick(nil)
	if err == nil {
		return m, wk, nil
	}
	if m.Fallback == "" || r.Header.Get("X-Forge-No-Fallback") == "1" {
		return nil, nil, err
	}
	fb, ok := h.fleet.Model(m.Fallback)
	if !ok || !id.CanUse(fb.Name, fb.Restricted) {
		return nil, nil, err
	}
	fwk, ferr := fb.Pool.Pick(nil)
	if ferr != nil {
		return nil, nil, err // report the requested model's condition
	}
	return fb, fwk, nil
}

// attempt runs one request against one backend and keeps the backend's
// in-flight count and outstanding-token estimate honest however it ends.
func (h *proxyHandler) attempt(r *http.Request, p *parsedRequest, m *pool.Model, wk *pool.Worker, sink *httpSink, id auth.Identity) (backend.Result, error) {
	reserve := p.MaxTokens
	if reserve == 0 {
		reserve = m.DefaultMaxTokens
	}
	wk.Acquire()
	wk.AddOutstanding(reserve)
	defer func() {
		wk.ReleaseOutstanding(reserve)
		wk.Release()
	}()

	// Priority goes only to engines that schedule by it (ADR-013); the
	// client's own value is always stripped by build.
	var prio *int
	if wk.SupportsPriority() {
		prio = &id.Priority
	}
	body, injected, err := p.build(m.Upstream, prio)
	if err != nil {
		return backend.Result{}, err
	}
	return wk.Backend.Forward(r.Context(), &backend.ProxyRequest{
		Endpoint: p.Endpoint, Body: body, Stream: p.Stream, StripUsageChunk: injected,
	}, sink)
}

// retryable: a failure the next backend might not have. A 4xx means the
// request itself is wrong; overload and unsupported are answered directly.
func retryable(err error) bool {
	var se *backend.StatusError
	if errors.As(err, &se) && se.Code < 500 {
		return false
	}
	return !errors.Is(err, backend.ErrOverloaded) && !errors.Is(err, backend.ErrUnsupported)
}

// charge debits the caller's TPM bucket and budgets and queues the usage
// row. Fire and forget: billing must never delay a response. It uses a
// detached context because the client may already be gone.
func (h *proxyHandler) charge(r *http.Request, id auth.Identity, authed bool, p *parsedRequest,
	requested, served *pool.Model, u backend.Usage) {
	tokens := u.PromptTokens + u.CompletionTokens
	if !authed || tokens == 0 {
		return
	}
	ctx := context.WithoutCancel(r.Context())
	now := time.Now()
	if h.tpm != nil {
		h.tpm.Debit(ctx, id.UserID, tokens, id.TokensPerMinute, now)
	}
	if h.budgets != nil {
		h.budgets.Add(ctx, budgetLimits(id), int64(tokens), now)
	}
	if h.record == nil {
		return
	}
	ev := UsageEvent{
		UserID: id.UserID, TeamID: id.TeamID, KeyID: id.KeyID,
		Model: served.Name, Endpoint: string(p.Endpoint),
		PromptTokens: u.PromptTokens, CompletionTokens: u.CompletionTokens, Estimated: u.Estimated,
	}
	if served != requested {
		ev.RequestedModel = requested.Name
	}
	h.record.RecordUsage(ev)
}

// httpSink writes to the client. Headers are committed only on Start, so
// until the first content chunk the handler can still pick a status code.
type httpSink struct {
	w       http.ResponseWriter
	start   time.Time
	started bool
}

func (s *httpSink) Start(contentType string) {
	if s.started {
		return
	}
	s.started = true
	s.w.Header().Set("Content-Type", contentType)
	if contentType == "text/event-stream" {
		s.w.Header().Set("Cache-Control", "no-cache")
	}
	s.w.WriteHeader(http.StatusOK)
	metrics.ObserveTTFT(time.Since(s.start))
}

func (s *httpSink) Write(p []byte) error {
	if _, err := s.w.Write(p); err != nil {
		return err // client gone
	}
	if f, ok := s.w.(http.Flusher); ok {
		f.Flush()
	}
	return nil
}

func reject(w http.ResponseWriter, status int, typ, code, msg string) {
	metrics.IncRequest(status)
	writeError(w, status, typ, code, msg)
}

// writeOverloaded: Retry-After tells clients how long to back off, which is
// the point of failing fast instead of queueing indefinitely.
func writeOverloaded(w http.ResponseWriter) {
	w.Header().Set("Retry-After", "1")
	reject(w, http.StatusTooManyRequests, "server_error", "overloaded", "server overloaded, retry")
}

func writeNoCapacity(w http.ResponseWriter) {
	w.Header().Set("Retry-After", "5")
	reject(w, http.StatusServiceUnavailable, "server_error", "no_healthy_backend", "no healthy backend available")
}
