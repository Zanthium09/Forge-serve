package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/Zanthium09/forge-serve/gateway/internal/auth"
	"github.com/Zanthium09/forge-serve/gateway/internal/metrics"
	"github.com/Zanthium09/forge-serve/gateway/internal/pool"
	"github.com/Zanthium09/forge-serve/gateway/internal/worker"
)

// SSE chunk types mirror OpenAI's chat.completion.chunk schema.
type chunk struct {
	ID      string   `json:"id"`
	Object  string   `json:"object"`
	Created int64    `json:"created"`
	Model   string   `json:"model"`
	Choices []choice `json:"choices"`
}

type choice struct {
	Index        int     `json:"index"`
	Delta        delta   `json:"delta"`
	FinishReason *string `json:"finish_reason"`
}

type delta struct {
	Content string `json:"content,omitempty"`
}

const (
	defaultTemperature = 0.7
	defaultMaxTokens   = 256
)

type chatHandler struct {
	pool   *pool.Pool
	record UsageRecorder
}

// reportUsage queues token accounting for the authenticated caller. Fire
// and forget: billing data must never delay a response.
func (h *chatHandler) reportUsage(r *http.Request, model string, prompt, completion int) {
	if h.record == nil || completion == 0 {
		return
	}
	id, ok := auth.FromContext(r.Context())
	if !ok {
		return
	}
	h.record.RecordUsage(UsageEvent{
		TenantID: id.TenantID, KeyHash: id.KeyHash, Model: model,
		PromptTokens: prompt, CompletionTokens: completion,
	})
}

// writeOverloaded answers a rejected-at-admission request. Retry-After
// tells clients how long to back off, which is the whole point of failing
// fast instead of queueing indefinitely.
func writeOverloaded(w http.ResponseWriter) {
	w.Header().Set("Retry-After", "1")
	metrics.IncRequest(http.StatusTooManyRequests)
	http.Error(w, `{"error":{"message":"server overloaded, retry"}}`,
		http.StatusTooManyRequests)
}

func writeNoCapacity(w http.ResponseWriter) {
	w.Header().Set("Retry-After", "5")
	metrics.IncRequest(http.StatusServiceUnavailable)
	http.Error(w, `{"error":{"message":"no healthy worker available"}}`,
		http.StatusServiceUnavailable)
}

func (h *chatHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var req ChatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		metrics.IncRequest(http.StatusBadRequest)
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := req.Validate(); err != nil {
		metrics.IncRequest(http.StatusBadRequest)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	wreq := worker.Request{
		Temperature: defaultTemperature,
		MaxTokens:   defaultMaxTokens,
	}
	if req.Temperature != nil {
		wreq.Temperature = *req.Temperature
	}
	if req.MaxTokens != nil {
		wreq.MaxTokens = *req.MaxTokens
	}
	for _, m := range req.Messages {
		wreq.Messages = append(wreq.Messages, worker.Message{Role: m.Role, Content: m.Content})
	}

	id := fmt.Sprintf("chatcmpl-%d", time.Now().UnixNano())
	if req.Stream {
		h.stream(w, r, id, req.Model, wreq, start)
	} else {
		h.aggregate(w, r, id, req.Model, wreq, start)
	}
}

// dispatch runs one attempt against one worker, keeping that worker's
// outstanding-token estimate honest however the attempt ends. onEvent
// returns false to stop reading early.
//
// It reports how many tokens reached the client, because that is what
// decides whether a failure is retryable: a request that has emitted
// nothing can be safely restarted elsewhere, one that has already streamed
// tokens cannot (ADR-005).
func dispatch(r *http.Request, wk *pool.Worker, wreq worker.Request,
	onEvent func(worker.Event) bool) (sent int, err error) {
	wk.AddOutstanding(wreq.MaxTokens)
	defer func() { wk.ReleaseOutstanding(wreq.MaxTokens - sent) }()

	err = wk.Client.Generate(r.Context(), wreq, func(ev worker.Event) bool {
		if !ev.Done {
			sent++
			wk.TokenDelivered()
		}
		return onEvent(ev)
	})
	return sent, err
}

// stream relays worker tokens as SSE. Client disconnect cancels
// r.Context(), which aborts the worker request - generation stops end to
// end. A worker failing before the first token is retried once elsewhere.
func (h *chatHandler) stream(w http.ResponseWriter, r *http.Request, id, model string, wreq worker.Request, start time.Time) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	total := 0
	var usage worker.Event
	emit := func(ev worker.Event) bool {
		if ev.Done {
			usage = ev
		}
		if total == 0 && !ev.Done {
			// Headers are deferred until the first real event: an admission
			// rejection or a failover must still be able to set a status
			// code, which is impossible once any byte has been written.
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "no-cache")
			metrics.ObserveTTFT(time.Since(start))
		}
		c := chunk{ID: id, Object: "chat.completion.chunk",
			Created: time.Now().Unix(), Model: model}
		if ev.Done {
			fr := ev.FinishReason
			c.Choices = []choice{{FinishReason: &fr}}
		} else {
			c.Choices = []choice{{Delta: delta{Content: ev.Token}}}
			total++
		}
		b, merr := json.Marshal(c)
		if merr != nil {
			return false
		}
		if _, werr := fmt.Fprintf(w, "data: %s\n\n", b); werr != nil {
			return false // client gone
		}
		flusher.Flush()
		return true
	}

	wk, err := h.pool.Pick(nil)
	if err != nil {
		writeNoCapacity(w)
		return
	}
	sent, err := dispatch(r, wk, wreq, emit)

	// Retry once on another worker, but only if nothing reached the client
	// yet. Restarting a half-streamed response would duplicate text.
	if err != nil && sent == 0 && r.Context().Err() == nil &&
		!errors.Is(err, worker.ErrOverloaded) {
		slog.Warn("worker failed before first token, retrying elsewhere",
			"worker", wk.URL, "err", err)
		metrics.IncFailover()
		if alt, perr := h.pool.Pick(wk); perr == nil {
			wk = alt
			sent, err = dispatch(r, wk, wreq, emit)
		}
	}

	if r.Context().Err() != nil {
		slog.Info("client disconnected, stream cancelled", "tokens_sent", total)
		return
	}
	if errors.Is(err, worker.ErrOverloaded) {
		writeOverloaded(w)
		return
	}
	if err != nil {
		if total == 0 {
			// Still nothing written: a real status code is possible.
			writeNoCapacity(w)
			return
		}
		// Mid-stream failure. No migration (ADR-005): the client gets a
		// clean, machine-readable error event instead of a silent truncation.
		metrics.IncRequest(http.StatusBadGateway)
		slog.Error("worker stream failed mid-response", "err", err, "tokens_sent", total)
		_, _ = fmt.Fprintf(w, "data: {\"error\": {\"message\": \"upstream failure\", \"type\": \"worker_lost\"}}\n\n")
		flusher.Flush()
		return
	}
	if el := time.Since(start).Seconds(); el > 0 && total > 0 {
		metrics.SetTokensPerSecond(float64(total) / el)
	}
	metrics.IncRequest(http.StatusOK)
	h.reportUsage(r, model, usage.PromptTokens, usage.CompletionTokens)
	_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
	flusher.Flush()
}

// aggregate collects the full worker stream, then answers with one JSON
// body. Non-stream responses buffer everything anyway, so a mid-response
// worker failure is always retryable here.
func (h *chatHandler) aggregate(w http.ResponseWriter, r *http.Request, id, model string, wreq worker.Request, start time.Time) {
	attempt := func(wk *pool.Worker) (string, worker.Event, int, error) {
		var sb strings.Builder
		var final worker.Event
		first := true
		sent, err := dispatch(r, wk, wreq, func(ev worker.Event) bool {
			if ev.Done {
				final = ev
			} else {
				if first {
					metrics.ObserveTTFT(time.Since(start))
					first = false
				}
				sb.WriteString(ev.Token)
			}
			return true
		})
		return sb.String(), final, sent, err
	}

	wk, err := h.pool.Pick(nil)
	if err != nil {
		writeNoCapacity(w)
		return
	}
	text, final, _, err := attempt(wk)

	if err != nil && r.Context().Err() == nil && !errors.Is(err, worker.ErrOverloaded) {
		slog.Warn("worker failed, retrying elsewhere", "worker", wk.URL, "err", err)
		metrics.IncFailover()
		if alt, perr := h.pool.Pick(wk); perr == nil {
			text, final, _, err = attempt(alt)
		}
	}
	if errors.Is(err, worker.ErrOverloaded) {
		writeOverloaded(w)
		return
	}
	if err != nil {
		metrics.IncRequest(http.StatusBadGateway)
		http.Error(w, "upstream failure", http.StatusBadGateway)
		return
	}

	if el := time.Since(start).Seconds(); el > 0 && final.CompletionTokens > 0 {
		metrics.SetTokensPerSecond(float64(final.CompletionTokens) / el)
	}
	metrics.IncRequest(http.StatusOK)
	h.reportUsage(r, model, final.PromptTokens, final.CompletionTokens)
	resp := ChatResponse{
		ID: id, Object: "chat.completion", Created: time.Now().Unix(), Model: model,
		Choices: []RespChoice{{
			Message:      ChatMessage{Role: "assistant", Content: text},
			FinishReason: final.FinishReason,
		}},
		Usage: Usage{
			PromptTokens:     final.PromptTokens,
			CompletionTokens: final.CompletionTokens,
			TotalTokens:      final.PromptTokens + final.CompletionTokens,
		},
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
