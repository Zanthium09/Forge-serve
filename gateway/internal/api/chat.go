package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

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
	worker *worker.Client
}

func (h *chatHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req ChatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := req.Validate(); err != nil {
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
		h.stream(w, r, id, req.Model, wreq)
	} else {
		h.aggregate(w, r, id, req.Model, wreq)
	}
}

// stream relays worker tokens as SSE. Client disconnect cancels r.Context(),
// which aborts the worker request — generation stops end to end.
func (h *chatHandler) stream(w http.ResponseWriter, r *http.Request, id, model string, wreq worker.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")

	sent := 0
	err := h.worker.Generate(r.Context(), wreq, func(ev worker.Event) bool {
		c := chunk{ID: id, Object: "chat.completion.chunk",
			Created: time.Now().Unix(), Model: model}
		if ev.Done {
			fr := ev.FinishReason
			c.Choices = []choice{{FinishReason: &fr}}
		} else {
			c.Choices = []choice{{Delta: delta{Content: ev.Token}}}
			sent++
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
	})
	if err != nil {
		// Headers are already sent; an SSE error event is all we can do.
		slog.Error("worker stream failed", "err", err, "tokens_sent", sent)
		fmt.Fprintf(w, "data: {\"error\": {\"message\": \"upstream failure\"}}\n\n")
		flusher.Flush()
		return
	}
	if r.Context().Err() != nil {
		slog.Info("client disconnected, stream cancelled", "tokens_sent", sent)
		return
	}
	fmt.Fprint(w, "data: [DONE]\n\n")
	flusher.Flush()
}

// aggregate collects the full worker stream, then answers with one JSON body.
func (h *chatHandler) aggregate(w http.ResponseWriter, r *http.Request, id, model string, wreq worker.Request) {
	var sb strings.Builder
	var final worker.Event
	err := h.worker.Generate(r.Context(), wreq, func(ev worker.Event) bool {
		if ev.Done {
			final = ev
		} else {
			sb.WriteString(ev.Token)
		}
		return true
	})
	if err != nil {
		http.Error(w, "upstream failure", http.StatusBadGateway)
		return
	}
	resp := ChatResponse{
		ID: id, Object: "chat.completion", Created: time.Now().Unix(), Model: model,
		Choices: []RespChoice{{
			Message:      ChatMessage{Role: "assistant", Content: sb.String()},
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
