package api

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Zanthium09/forge-serve/gateway/internal/auth"
	"github.com/Zanthium09/forge-serve/gateway/internal/worker"
)

// allowAllStore accepts any key — auth behavior itself is covered in
// package auth; these tests focus on the chat handler.
type allowAllStore struct{}

func (allowAllStore) TenantIDByKeyHash(_ context.Context, _ string) (int64, error) {
	return 1, nil
}

// postJSON sends an authed POST and returns the response.
func postJSON(t *testing.T, url, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer sk-test")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// fakeWorker speaks the NDJSON protocol with a configurable per-line delay.
func fakeWorker(t *testing.T, delay time.Duration, lines []string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f := w.(http.Flusher)
		w.Header().Set("Content-Type", "application/x-ndjson")
		for _, l := range lines {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(delay):
			}
			fmt.Fprintln(w, l)
			f.Flush()
		}
	}))
}

var happyLines = []string{
	`{"token": "Hello", "done": false}`,
	`{"token": " world", "done": false}`,
	`{"done": true, "prompt_tokens": 5, "completion_tokens": 2, "finish_reason": "stop"}`,
}

func newTestMux(t *testing.T, workerURL string) http.Handler {
	t.Helper()
	return NewMux(worker.New(workerURL), auth.New(allowAllStore{}, time.Minute))
}

func TestChatStreamRelaysSSE(t *testing.T) {
	fw := fakeWorker(t, 0, happyLines)
	defer fw.Close()
	srv := httptest.NewServer(newTestMux(t, fw.URL))
	defer srv.Close()

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q", ct)
	}

	var contents []string
	sawDone := false
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		payload := strings.TrimPrefix(line, "data: ")
		if payload == "[DONE]" {
			sawDone = true
			break
		}
		var c chunk
		if err := json.Unmarshal([]byte(payload), &c); err != nil {
			t.Fatalf("bad chunk %q: %v", payload, err)
		}
		if s := c.Choices[0].Delta.Content; s != "" {
			contents = append(contents, s)
		}
	}
	got := strings.Join(contents, "")
	if got != "Hello world" || !sawDone {
		t.Fatalf("got %q sawDone=%v", got, sawDone)
	}
}

func TestChatNonStreamAggregates(t *testing.T) {
	fw := fakeWorker(t, 0, happyLines)
	defer fw.Close()
	srv := httptest.NewServer(newTestMux(t, fw.URL))
	defer srv.Close()

	resp := postJSON(t, srv.URL+"/v1/chat/completions",
		`{"messages":[{"role":"user","content":"hi"}]}`)
	defer resp.Body.Close()

	var cr ChatResponse
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
		t.Fatal(err)
	}
	if cr.Object != "chat.completion" {
		t.Fatalf("object = %q", cr.Object)
	}
	if cr.Choices[0].Message.Content != "Hello world" {
		t.Fatalf("content = %q", cr.Choices[0].Message.Content)
	}
	if cr.Usage.TotalTokens != 7 {
		t.Fatalf("usage = %+v", cr.Usage)
	}
}

func TestChatRejectsInvalidRequest(t *testing.T) {
	srv := httptest.NewServer(newTestMux(t, "http://unused.invalid"))
	defer srv.Close()

	resp := postJSON(t, srv.URL+"/v1/chat/completions", `{"messages":[]}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestChatClientDisconnectCancelsHandlerAndWorker(t *testing.T) {
	// Slow fake worker: 50ms per line, so the client can bail mid-stream.
	fw := fakeWorker(t, 50*time.Millisecond, happyLines)
	defer fw.Close()

	handlerDone := make(chan struct{})
	mux := newTestMux(t, fw.URL)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
		close(handlerDone)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		srv.URL+"/v1/chat/completions",
		strings.NewReader(`{"stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Authorization", "Bearer sk-test")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if _, err := resp.Body.Read(make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	cancel()

	select {
	case <-handlerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not exit after client disconnect")
	}
}

func TestHealthzStillWorks(t *testing.T) {
	srv := httptest.NewServer(newTestMux(t, "http://unused.invalid"))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}
