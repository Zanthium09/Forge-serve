// Package backendtest provides a fake OpenAI-compatible engine for tests.
// It can stream, stall, die before or after its first token, omit usage,
// and serve vLLM-style /metrics, which covers every failure mode the
// gateway's retry and accounting rules depend on.
package backendtest

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type Fake struct {
	Chunks    []string      // SSE payloads without "data: "; [DONE] is appended
	Delay     time.Duration // before each chunk
	DieAfter  int           // 0 = never; n = drop the connection after n chunks
	Status    int           // non-zero: reply with this status and Body
	Body      string        // non-stream / error reply body
	OmitUsage bool          // never send a usage chunk
	Metrics   string        // served at GET /metrics

	Requests  atomic.Int64
	Cancelled atomic.Bool // the request context was cancelled mid-stream

	mu       sync.Mutex
	lastBody []byte
	lastPath string
}

// DefaultBody is the non-stream reply when Body is empty.
const DefaultBody = `{"id":"x","object":"chat.completion","model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"Hello world"},"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7}}`

func ChatChunk(content string) string {
	b, _ := json.Marshal(content)
	return `{"choices":[{"index":0,"delta":{"content":` + string(b) + `}}]}`
}

func RoleChunk() string {
	return `{"choices":[{"index":0,"delta":{"role":"assistant"}}]}`
}

func UsageChunk(prompt, completion int) string {
	return fmt.Sprintf(`{"choices":[],"usage":{"prompt_tokens":%d,"completion_tokens":%d,"total_tokens":%d}}`,
		prompt, completion, prompt+completion)
}

func (f *Fake) LastBody() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastBody
}

func (f *Fake) LastPath() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastPath
}

// Server starts the fake; it is closed when the test ends.
func (f *Fake) Server(t *testing.T) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(s.Close)
	return s
}

func (f *Fake) serve(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/metrics":
		_, _ = io.WriteString(w, f.Metrics)
		return
	case r.Method == http.MethodGet && r.URL.Path == "/v1/models":
		_, _ = io.WriteString(w, `{"object":"list","data":[]}`)
		return
	}
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	f.lastBody, f.lastPath = body, r.URL.Path
	f.mu.Unlock()
	f.Requests.Add(1)

	if f.Status != 0 {
		w.WriteHeader(f.Status)
		_, _ = io.WriteString(w, f.Body)
		return
	}
	var req struct {
		Stream        bool `json:"stream"`
		StreamOptions struct {
			IncludeUsage bool `json:"include_usage"`
		} `json:"stream_options"`
	}
	_ = json.Unmarshal(body, &req)
	if !req.Stream {
		b := f.Body
		if b == "" {
			b = DefaultBody
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, b)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	fl := w.(http.Flusher)
	content := 0
	for i, c := range f.Chunks {
		if f.DieAfter > 0 && i == f.DieAfter {
			drop(w)
			return
		}
		select {
		case <-r.Context().Done():
			f.Cancelled.Store(true)
			return
		case <-time.After(f.Delay):
		}
		if c != RoleChunk() {
			content++
		}
		_, _ = fmt.Fprintf(w, "data: %s\n\n", c)
		fl.Flush()
	}
	if req.StreamOptions.IncludeUsage && !f.OmitUsage {
		_, _ = fmt.Fprintf(w, "data: %s\n\n", UsageChunk(5, content))
	}
	_, _ = io.WriteString(w, "data: [DONE]\n\n")
	fl.Flush()
}

// drop closes the connection without finishing the chunked body, which the
// client sees as an unexpected EOF, like a killed engine.
func drop(w http.ResponseWriter) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		panic("backendtest: hijack unsupported")
	}
	conn, _, err := hj.Hijack()
	if err == nil {
		_ = conn.Close()
	}
}

// IsDone reports whether an SSE event is the terminal marker.
func IsDone(event string) bool { return strings.TrimSpace(event) == "data: [DONE]" }
