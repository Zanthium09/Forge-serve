package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// Minimal mirror of OpenAI's chat.completion.chunk schema — only the fields
// clients need to render a stream. Full schema arrives in Phase 1.
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

var loremWords = strings.Fields(
	"lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod " +
		"tempor incididunt ut labore et dolore magna aliqua")

// handleChatCompletions is the Phase 0 stub: it proves the SSE pipe and the
// cancellation path before any model exists. The select-on-context shape here
// is exactly what the Phase 1 proxy and Phase 2 batch eviction will reuse.
func handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")

	// 50ms/token = 20 tok/s per blueprint Phase 0 spec.
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for i, word := range loremWords {
		select {
		case <-r.Context().Done():
			// Client went away: stop generating immediately. In Phase 2 this
			// same signal evicts the sequence from a live batch.
			slog.Info("client disconnected, stream cancelled", "tokens_sent", i)
			return
		case <-ticker.C:
		}
		c := chunk{
			ID:      "chatcmpl-stub",
			Object:  "chat.completion.chunk",
			Created: time.Now().Unix(),
			Model:   "stub",
			Choices: []choice{{Delta: delta{Content: word + " "}}},
		}
		b, err := json.Marshal(c)
		if err != nil {
			slog.Error("marshal chunk", "err", err)
			return
		}
		fmt.Fprintf(w, "data: %s\n\n", b)
		flusher.Flush()
	}
	fmt.Fprint(w, "data: [DONE]\n\n")
	flusher.Flush()
}
